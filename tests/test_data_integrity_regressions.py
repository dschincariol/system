"""Targeted regressions for data integrity and observability fixes."""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _DummyConnection:
    def close(self) -> None:
        return None


class _FakeManager:
    def __init__(self) -> None:
        self.job_status_calls: list[dict] = []

    def load_rss_sources(self):
        return [{"name": "feed", "url": "https://example.test/rss"}]

    def record_job_status(self, *args, **kwargs) -> None:
        self.job_status_calls.append({"args": args, "kwargs": kwargs})


class DataIntegrityRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "integrity.db")
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as exc:
            sys.stderr.write(f"[test_data_integrity_regressions] teardown_failed: {type(exc).__name__}: {exc}\n")
        self.tmp.cleanup()

    def test_price_router_clamps_ancient_timestamps_and_drops_nonfinite_volume(self) -> None:
        (price_router,) = _reload_modules("engine.runtime.price_router")
        received_ts_ms = 1_760_000_000_000

        row = price_router._normalize_event_strict(
            {
                "symbol": "AAPL",
                "provider": "unit",
                "timestamp": int(received_ts_ms - (10 * 86400 * 1000)),
                "last": -1.0,
                "bid": 100.0,
                "ask": 101.0,
                "volume": "nan",
            },
            received_ts_ms,
        )

        self.assertIsNotNone(row)
        self.assertEqual(int(row["timestamp"]), int(received_ts_ms))
        self.assertAlmostEqual(float(row["last"]), 100.5, places=6)
        self.assertIsNone(row["volume"])

        rejected = price_router._normalize_event_strict(
            {
                "symbol": "AAPL",
                "provider": "unit",
                "timestamp": int(received_ts_ms),
                "last": -5.0,
            },
            received_ts_ms,
        )
        self.assertIsNone(rejected)

    def test_event_normalization_marks_invalid_json(self) -> None:
        (event_normalization,) = _reload_modules("engine.data.event_normalization")

        self.assertEqual(event_normalization._json_loads(None), {})
        self.assertEqual(event_normalization._json_loads(""), {})
        self.assertEqual(event_normalization._json_loads("{bad"), {"_parse_error": True})
        self.assertEqual(event_normalization._json_loads("[]"), {"_parse_error": True})

    def test_default_symbols_resolves_relative_sec_cache_from_repo_root(self) -> None:
        repo_tmp = tempfile.TemporaryDirectory(dir=str(REPO_ROOT), ignore_cleanup_errors=True)
        other_cwd = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        prev_cwd = os.getcwd()
        prev_env = os.environ.get("SEC_TICKER_MAP_CACHE")
        try:
            cache_path = Path(repo_tmp.name) / "sec_company_tickers_exchange.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "fields": ["ticker", "exchange"],
                        "data": [["AAPL", "NASDAQ"], ["MSFT", "NASDAQ"]],
                    }
                ),
                encoding="utf-8",
            )
            os.environ["SEC_TICKER_MAP_CACHE"] = str(cache_path.relative_to(REPO_ROOT))
            os.chdir(other_cwd.name)

            (default_symbols,) = _reload_modules("engine.data.default_symbols")
            symbols = default_symbols.load_sec_seed_symbols(top_n=2)
        finally:
            os.chdir(prev_cwd)
            if prev_env is None:
                os.environ.pop("SEC_TICKER_MAP_CACHE", None)
            else:
                os.environ["SEC_TICKER_MAP_CACHE"] = prev_env
            repo_tmp.cleanup()
            other_cwd.cleanup()

        self.assertEqual(symbols, ["AAPL", "MSFT"])

    def test_ingest_now_records_dropped_enrichment_count(self) -> None:
        (ingest_now,) = _reload_modules("engine.data.jobs.ingest_now")
        fake_manager = _FakeManager()
        status_calls: list[dict] = []

        def _build_enriched(_con, item, allowed_symbols=None):
            del allowed_symbols
            if str(item.get("id")) == "drop":
                return []
            return [
                {
                    "event": {
                        "ts_ms": 1_760_000_000_000,
                        "source": "rss:feed",
                        "title": "kept",
                        "body": "body",
                        "url": "https://example.test/article",
                        "event_key": "evt:keep",
                        "meta_json": "{}",
                        "symbol": "AAPL",
                    },
                    "feature": {"symbol": "AAPL"},
                }
            ]

        def _record_pipeline_status(*args, **kwargs):
            del args
            status_calls.append(dict(kwargs))
            return {"ok": kwargs.get("ok"), "meta": dict(kwargs.get("meta") or {})}

        with patch.object(ingest_now, "get_manager", return_value=fake_manager):
            with patch.object(ingest_now, "connect", return_value=_DummyConnection()):
                with patch("engine.data.universe.get_active_symbols", return_value=[]):
                    with patch.object(ingest_now, "ingest_rss_sources", return_value=([{"id": "drop"}, {"id": "keep"}], [])):
                        with patch.object(ingest_now, "build_enriched_news_records", side_effect=_build_enriched):
                            with patch.object(ingest_now, "run_write_txn", side_effect=lambda fn, **_kwargs: fn(None)):
                                with patch.object(ingest_now, "put_normalized_event", return_value=1):
                                    with patch.object(ingest_now, "put_news_event_feature", return_value=None):
                                        with patch.object(ingest_now, "refresh_news_symbol_features", return_value=None):
                                            with patch.object(ingest_now, "record_pipeline_status", side_effect=_record_pipeline_status):
                                                with patch.object(ingest_now, "put_job_heartbeat", return_value=None):
                                                    with patch.object(ingest_now, "USE_FINBERT_SENTIMENT", False):
                                                        ingest_now._run_once()

        self.assertTrue(status_calls)
        self.assertEqual(int(status_calls[-1]["meta"]["dropped_enrichment"]), 1)
        self.assertTrue(fake_manager.job_status_calls)
        self.assertEqual(
            int(fake_manager.job_status_calls[-1]["kwargs"]["meta"]["dropped_enrichment"]),
            1,
        )

    def test_options_poll_alerts_when_no_fresh_symbols_succeed(self) -> None:
        os.environ["TRADIER_API_TOKEN"] = "token"
        os.environ["OPTIONS_CRITICAL_SYMBOLS"] = "SPY"
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("down", kind="server_error"),
            ):
                with patch("engine.data.options_poll.emit_alert", return_value=None) as alert_mock:
                    result = options_poll._run_once(["tradier"])

        self.assertEqual(int(result["meta"]["symbols_attempted"]), 1)
        self.assertEqual(int(result["meta"]["symbols_succeeded"]), 0)
        self.assertEqual(int(result["meta"]["symbols_failed"]), 1)
        self.assertFalse(bool(result["pipeline_ok"]))
        alert_mock.assert_called_once()

    def test_predictor_knn_returns_fallback_reason_when_cache_is_empty(self) -> None:
        (predictor,) = _reload_modules("engine.strategy.predictor")

        with patch.object(predictor, "_load_labeled_event_vectors_cached", return_value=([], {}, None, [], [])):
            score, weight, explain = predictor._knn_raw(
                np.asarray([0.0], dtype=np.float32),
                "AAPL",
                300,
                5,
            )

        self.assertEqual(float(score), 0.0)
        self.assertEqual(float(weight), 0.0)
        self.assertEqual(dict(explain or {}).get("fallback_reason"), "no_cached_labeled_events")

    def test_embed_regressor_split_reserves_gap_when_dataset_is_large_enough(self) -> None:
        prev_split = os.environ.get("EMBED_TRAIN_SPLIT")
        try:
            os.environ["EMBED_TRAIN_SPLIT"] = "0.8"
            (embed_regressor,) = _reload_modules("engine.strategy.embed_regressor")
            train_end, eval_start, gap_n = embed_regressor._compute_train_eval_split(100)
        finally:
            if prev_split is None:
                os.environ.pop("EMBED_TRAIN_SPLIT", None)
            else:
                os.environ["EMBED_TRAIN_SPLIT"] = prev_split

        self.assertEqual(int(train_end), 78)
        self.assertEqual(int(eval_start), 80)
        self.assertEqual(int(gap_n), 2)


if __name__ == "__main__":
    unittest.main()
