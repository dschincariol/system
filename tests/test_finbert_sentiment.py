from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _fake_probabilities(texts, model_name=None):
    out = []
    for text in texts or []:
        lower = str(text or "").lower()
        if any(word in lower for word in ("beat", "gain", "surge", "rally")):
            out.append({"positive": 0.82, "negative": 0.08, "neutral": 0.10})
        elif any(word in lower for word in ("miss", "drop", "loss", "cut")):
            out.append({"positive": 0.07, "negative": 0.84, "neutral": 0.09})
        else:
            out.append({"positive": 0.12, "negative": 0.11, "neutral": 0.77})
    return out


class FinbertSentimentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "USE_FINBERT_SENTIMENT",
                "FINBERT_MODEL_NAME",
                "FINBERT_BATCH_SIZE",
                "FINBERT_MAX_TEXT_LEN",
                "FINBERT_USE_PERSISTED_ENRICHMENT",
                "FINBERT_LIVE_INFERENCE_ENABLED",
                "USE_SYMBOL_SNAPSHOT_FEATURES",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "finbert_sentiment.db")
        os.environ["FINBERT_MODEL_NAME"] = "ProsusAI/finbert"
        os.environ["FINBERT_BATCH_SIZE"] = "4"
        os.environ["FINBERT_MAX_TEXT_LEN"] = "256"
        os.environ["FINBERT_USE_PERSISTED_ENRICHMENT"] = "1"
        os.environ["FINBERT_LIVE_INFERENCE_ENABLED"] = "0"

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _init_storage(self):
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()
        return storage

    def test_score_financial_text_output_structure(self) -> None:
        os.environ["USE_FINBERT_SENTIMENT"] = "1"
        (finbert,) = _reload_modules("engine.data.finbert_sentiment")
        fake_bundle = {"model_name": "ProsusAI/finbert", "model_version": "unit-test"}

        with (
            patch.object(finbert, "load_finbert_model", return_value=fake_bundle),
            patch.object(finbert, "_probabilities_for_texts", side_effect=_fake_probabilities),
        ):
            out = finbert.score_financial_text("AAPL gains after earnings beat and strong guidance.")

        self.assertEqual(str(out["label"]), "positive")
        self.assertGreater(float(out["score"]), 0.0)
        self.assertGreater(float(out["confidence"]), 0.0)
        self.assertGreater(float(out["pos"]), float(out["neg"]))
        self.assertEqual(str(out["model_name"]), "ProsusAI/finbert")
        self.assertEqual(str(out["model_version"]), "unit-test")
        self.assertTrue(bool(out["payload_json"]["text_hash"]))
        self.assertGreater(int(out["payload_json"]["text_len"]), 0)

    def test_score_event_rows_is_deterministic_and_handles_missing_symbol(self) -> None:
        os.environ["USE_FINBERT_SENTIMENT"] = "1"
        (finbert,) = _reload_modules("engine.data.finbert_sentiment")
        fake_bundle = {"model_name": "ProsusAI/finbert", "model_version": "unit-test"}
        rows = [
            {
                "event_id": 101,
                "symbol": "AAPL",
                "title": "AAPL shares rally after earnings beat",
                "body": "Revenue gains and strong margin expansion.",
                "ts_ms": 1_710_000_000_000,
            },
            {
                "event_id": 102,
                "title": "Macro outlook remains mixed",
                "body": "Markets wait for the next CPI print.",
                "ts_ms": 1_710_000_060_000,
            },
        ]

        with (
            patch.object(finbert, "load_finbert_model", return_value=fake_bundle),
            patch.object(finbert, "_probabilities_for_texts", side_effect=_fake_probabilities),
        ):
            first = finbert.score_event_rows(rows)
            second = finbert.score_event_rows(rows)

        self.assertEqual(first, second)
        self.assertEqual(str(first[0]["label"]), "positive")
        self.assertIsNone(first[1]["symbol"])
        self.assertEqual(str(first[1]["label"]), "neutral")

    def test_missing_text_short_circuits_model_load(self) -> None:
        os.environ["USE_FINBERT_SENTIMENT"] = "1"
        (finbert,) = _reload_modules("engine.data.finbert_sentiment")

        with patch.object(finbert, "load_finbert_model", side_effect=AssertionError("unexpected_model_load")):
            out = finbert.summarize_document_sentiment("", metadata={"event_id": 7, "symbol": "AAPL", "ts_ms": 123})

        self.assertEqual(str(out["label"]), "missing")
        self.assertEqual(float(out["score"]), 0.0)
        self.assertEqual(float(out["confidence"]), 0.0)
        self.assertTrue(bool(out["payload_json"]["missing_text"]))

    @pytest.mark.requires_postgres
    def test_persistence_write_and_read_round_trip(self) -> None:
        storage = self._init_storage()
        row = {
            "event_id": 201,
            "source_identifier": "rss:aapl:201",
            "symbol": "AAPL",
            "ts_ms": 1_710_000_000_000,
            "label": "positive",
            "score": 0.74,
            "confidence": 0.82,
            "pos": 0.82,
            "neg": 0.08,
            "neu": 0.10,
            "model_name": "ProsusAI/finbert",
            "model_version": "unit-test",
            "payload_json": {"text_hash": "abc123", "truncated": False},
        }
        missing_symbol_row = dict(row)
        missing_symbol_row.update({"event_id": 202, "source_identifier": "rss:macro:202", "symbol": None, "label": "neutral"})

        storage.put_finbert_sentiment_enrichment(row)
        storage.put_finbert_sentiment_enrichment(missing_symbol_row)

        event_row = storage.load_finbert_sentiment_enrichment_for_event(
            event_id=201,
            model_name="ProsusAI/finbert",
        )
        latest_row = storage.load_latest_finbert_sentiment_enrichment(
            symbol="AAPL",
            ts_ms=1_710_000_000_000,
            model_name="ProsusAI/finbert",
        )
        self.assertIsNotNone(event_row)
        self.assertIsNotNone(latest_row)
        assert event_row is not None
        assert latest_row is not None
        self.assertEqual(str(event_row["label"]), "positive")
        self.assertEqual(str(latest_row["symbol"]), "AAPL")
        self.assertEqual(float(latest_row["score"]), 0.74)
        self.assertEqual(dict(event_row["payload_json"])["text_hash"], "abc123")

        con = storage.connect(readonly=True)
        try:
            rows = con.execute(
                "SELECT COUNT(*) FROM news_event_features WHERE finbert_label IS NOT NULL",
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(int(rows[0] or 0), 2)

    @pytest.mark.requires_postgres
    def test_feature_registry_integration_uses_persisted_sentiment(self) -> None:
        os.environ["USE_FINBERT_SENTIMENT"] = "1"
        os.environ["USE_SYMBOL_SNAPSHOT_FEATURES"] = "1"
        storage = self._init_storage()
        con = storage.connect(readonly=False)
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS price_quotes (
                  ts_ms INTEGER NOT NULL,
                  symbol TEXT NOT NULL,
                  last REAL,
                  bid REAL,
                  ask REAL,
                  spread REAL,
                  volume REAL
                )
                """
            )
            con.commit()
        finally:
            con.close()
        _, feature_registry = _reload_modules(
            "engine.data.finbert_sentiment",
            "engine.strategy.feature_registry",
        )

        event = {
            "event_id": 301,
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "AAPL shares rally after earnings beat",
            "body": "Revenue gains and strong guidance.",
        }
        storage.put_finbert_sentiment_enrichment(
            {
                "event_id": 301,
                "source_identifier": "rss:aapl:301",
                "symbol": "AAPL",
                "ts_ms": int(event["ts_ms"]),
                "label": "positive",
                "score": 0.74,
                "confidence": 0.82,
                "pos": 0.82,
                "neg": 0.08,
                "neu": 0.10,
                "model_name": "ProsusAI/finbert",
                "model_version": "unit-test",
                "payload_json": {"text_hash": "persisted"},
            }
        )

        feature_ids = [
            "base.source_credibility",
            "sentiment.finbert.label",
            "sentiment.finbert.score",
            "sentiment.finbert.pos",
            "sentiment.finbert.neg",
            "sentiment.finbert.neu",
            "sentiment.finbert.confidence",
        ]
        with patch.object(feature_registry, "_schedule_feature_store_write", return_value=None):
            snapshot = feature_registry.build_feature_snapshot(
                event=event,
                symbol="AAPL",
                feature_ids=list(feature_ids),
            )

        default_ids = feature_registry.default_feature_ids()
        self.assertTrue(any(fid.startswith("sentiment.finbert.") for fid in default_ids))
        self.assertEqual(float(snapshot["sentiment.finbert.label"]), 1.0)
        self.assertEqual(float(snapshot["sentiment.finbert.score"]), 0.74)
        self.assertEqual(float(snapshot["sentiment.finbert.pos"]), 0.82)
        self.assertEqual(float(snapshot["sentiment.finbert.neg"]), 0.08)
        self.assertEqual(float(snapshot["sentiment.finbert.neu"]), 0.10)
        self.assertEqual(float(snapshot["sentiment.finbert.confidence"]), 0.82)
        self.assertIn("finbert", feature_registry.feature_set_tag_from_ids(feature_ids))

    @pytest.mark.requires_postgres
    def test_disabled_path_startup_imports_without_finbert_model_load(self) -> None:
        os.environ["USE_FINBERT_SENTIMENT"] = "0"
        os.environ["USE_SYMBOL_SNAPSHOT_FEATURES"] = "1"
        self._init_storage()
        finbert, ingest_now, process_events_enriched, feature_registry = _reload_modules(
            "engine.data.finbert_sentiment",
            "engine.data.jobs.ingest_now",
            "engine.data.jobs.process_events_enriched",
            "engine.strategy.feature_registry",
        )

        event = {
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "AAPL scheduled earnings",
            "body": "Quarterly update",
        }
        with (
            patch.object(finbert, "load_finbert_model", side_effect=AssertionError("unexpected_finbert_model_load")),
            patch.object(feature_registry, "_schedule_feature_store_write", return_value=None),
        ):
            snapshot = feature_registry.build_feature_snapshot(
                event=event,
                symbol="AAPL",
                feature_ids=["base.source_credibility", "base.scheduled_flag"],
            )

        self.assertTrue(callable(getattr(ingest_now, "main", None)))
        self.assertTrue(callable(getattr(process_events_enriched, "main", None)))
        self.assertGreater(float(snapshot["base.source_credibility"]), 0.0)
        self.assertEqual(float(snapshot["base.scheduled_flag"]), 1.0)

    @pytest.mark.requires_postgres
    def test_disabled_path_remains_backward_compatible(self) -> None:
        os.environ["USE_FINBERT_SENTIMENT"] = "0"
        os.environ["USE_SYMBOL_SNAPSHOT_FEATURES"] = "1"
        self._init_storage()
        _, feature_registry = _reload_modules(
            "engine.data.finbert_sentiment",
            "engine.strategy.feature_registry",
        )

        default_ids = feature_registry.default_feature_ids()
        self.assertFalse(any(fid.startswith("sentiment.finbert.") for fid in default_ids))
        self.assertEqual(
            feature_registry.resolve_feature_ids(
                model_spec={"feature_ids": ["sentiment.finbert.score", "base.source_credibility"]}
            ),
            ["base.source_credibility"],
        )

        event = {
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "AAPL scheduled earnings",
            "body": "Quarterly update",
        }
        with patch.object(feature_registry, "_schedule_feature_store_write", return_value=None):
            snapshot = feature_registry.build_feature_snapshot(
                event=event,
                symbol="AAPL",
                feature_ids=["base.source_credibility", "base.scheduled_flag"],
            )

        self.assertGreater(float(snapshot["base.source_credibility"]), 0.0)
        self.assertEqual(float(snapshot["base.scheduled_flag"]), 1.0)


if __name__ == "__main__":
    unittest.main()
