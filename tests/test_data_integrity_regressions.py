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

    def test_price_raw_event_key_ignores_mutable_floats(self) -> None:
        price_event_keys, price_router = _reload_modules(
            "engine.data.price_event_keys",
            "engine.runtime.price_router",
        )
        received_ts_ms = 1_760_000_000_000
        base = {
            "symbol": "SPY",
            "provider": "polygon_ws",
            "source": "polygon_ws",
            "timestamp": received_ts_ms,
            "event_type": "T",
            "trade_id": "trade-1",
            "sequence_number": "42",
            "exchange": "N",
            "last": 500.25,
            "bid": 500.2,
            "ask": 500.3,
            "volume": 1000,
        }
        changed_values = dict(base, last=501.75, bid=501.7, ask=501.8, volume=2000)

        row_a = price_router._normalize_event_strict(dict(base), received_ts_ms)
        row_b = price_router._normalize_event_strict(dict(changed_values), received_ts_ms)

        self.assertIsNotNone(row_a)
        self.assertIsNotNone(row_b)
        self.assertEqual(row_a["event_key"], row_b["event_key"])
        self.assertTrue(str(row_a["event_key"]).startswith(price_event_keys.PRICE_RAW_EVENT_KEY_VERSION))
        self.assertNotIn("500.25", str(row_a["event_key"]))
        self.assertNotIn("1000", str(row_a["event_key"]))

    def test_price_router_raw_retry_does_not_duplicate_sqlite_rows(self) -> None:
        with patch.dict(os.environ, {"PRICE_ROUTER_SQLITE_RAW_ENABLED": "1"}, clear=False):
            storage, price_router = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.price_router",
            )
            storage.init_db()
            event_ts_ms = price_router.now_ms()
            event = {
                "symbol": "SPY",
                "provider": "polygon_ws",
                "source": "polygon_ws",
                "timestamp": event_ts_ms,
                "event_type": "T",
                "trade_id": "retry-trade-1",
                "sequence_number": "77",
                "exchange": "N",
                "last": 500.25,
                "volume": 1000,
            }

            def publish_once(payload):
                price_router._LAST_EVENT_KEY_BY_STREAM.clear()
                price_router._LAST_EVENT_TS_BY_STREAM.clear()
                with patch.object(price_router, "publish_event"):
                    with patch.object(price_router, "record_component_health"):
                        with patch.object(price_router, "emit_counter"):
                            with patch.object(price_router, "enqueue_price_persistence", return_value=True):
                                with patch.object(price_router, "register_after_commit", lambda _db, _callback: None):
                                    return price_router.publish_price_events(
                                        [payload],
                                        write_prices=False,
                                        write_quotes=False,
                                        write_raw=True,
                                        emit_telemetry=False,
                                    )

            first = publish_once(dict(event))
            second = publish_once(dict(event, last=501.0, volume=2000))

            self.assertEqual(int(first.get("raw") or 0), 1)
            self.assertEqual(int(second.get("raw") or 0), 1)
            con = storage.connect_ro_direct()
            try:
                row = con.execute(
                    """
                    SELECT COUNT(*), COUNT(DISTINCT event_key), MAX(last)
                    FROM price_quotes_raw
                    WHERE symbol='SPY' AND provider='polygon_ws'
                    """
                ).fetchone()
            finally:
                con.close()
            self.assertEqual((int(row[0]), int(row[1])), (1, 1))
            self.assertAlmostEqual(float(row[2]), 501.0, places=6)

    def test_price_router_cutover_reuses_raw_key_without_duplicate_buffer_flush(self) -> None:
        storage, price_router = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.price_router",
        )
        storage.init_db()
        event_ts_ms = price_router.now_ms()
        event = {
            "symbol": "QQQ",
            "provider": "polygon_ws",
            "source": "polygon_ws",
            "timestamp": event_ts_ms,
            "event_type": "Q",
            "sequence_number": "88",
            "bid_exchange": "Q",
            "ask_exchange": "Q",
            "bid": 430.10,
            "ask": 430.20,
        }

        with patch.dict(os.environ, {"PRICE_ROUTER_SQLITE_RAW_ENABLED": "1"}, clear=False):
            (price_router,) = _reload_modules("engine.runtime.price_router")
            price_router._LAST_EVENT_KEY_BY_STREAM.clear()
            price_router._LAST_EVENT_TS_BY_STREAM.clear()
            with patch.object(price_router, "publish_event"):
                with patch.object(price_router, "record_component_health"):
                    with patch.object(price_router, "emit_counter"):
                        with patch.object(price_router, "enqueue_price_persistence", return_value=True):
                            with patch.object(price_router, "register_after_commit", lambda _db, _callback: None):
                                price_router.publish_price_events(
                                    [dict(event)],
                                    write_prices=False,
                                    write_quotes=False,
                                    write_raw=True,
                                    emit_telemetry=False,
                                )

        con = storage.connect_ro_direct()
        try:
            direct_row = con.execute(
                """
                SELECT event_key, ts_ms
                FROM price_quotes_raw
                WHERE symbol='QQQ' AND provider='polygon_ws'
                """
            ).fetchone()
        finally:
            con.close()

        captured_pg_rows = []
        captured_buffer_rows = []
        with patch.dict(
            os.environ,
            {"PRICE_ROUTER_SQLITE_WRITE_ENABLED": "0", "ASYNC_PRICE_WRITER_ENABLED": "1"},
            clear=False,
        ):
            price_router, telemetry_append_buffer = _reload_modules(
                "engine.runtime.price_router",
                "engine.runtime.telemetry_append_buffer",
            )
            price_router._LAST_EVENT_KEY_BY_STREAM.clear()
            price_router._LAST_EVENT_TS_BY_STREAM.clear()
            with patch.object(price_router, "publish_event"):
                with patch.object(price_router, "record_component_health"):
                    with patch.object(price_router, "emit_counter"):
                        with patch.object(
                            price_router,
                            "enqueue_price_persistence",
                            side_effect=lambda **kwargs: captured_pg_rows.extend(kwargs.get("raw") or ()) or True,
                        ):
                            with patch.object(
                                price_router,
                                "enqueue_price_quotes_raw_rows",
                                side_effect=lambda rows: captured_buffer_rows.extend(tuple(rows or ())) or True,
                            ):
                                price_router.publish_price_events(
                                    [dict(event, bid=431.10, ask=431.20)],
                                    write_prices=False,
                                    write_quotes=False,
                                    write_raw=True,
                                    emit_telemetry=False,
                                )

            self.assertEqual(len(captured_pg_rows), 1)
            self.assertEqual(len(captured_buffer_rows), 1)
            self.assertEqual(captured_pg_rows[0]["event_key"], direct_row[0])
            self.assertEqual(int(captured_pg_rows[0]["ts_ms"]), int(direct_row[1]))
            telemetry_append_buffer._write_rows(
                "price_quotes_raw",
                tuple(captured_buffer_rows),
                attempts=1,
                timeout_s=0.5,
                busy_timeout_ms=500,
            )

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT event_key), MAX(bid), MAX(ask)
                FROM price_quotes_raw
                WHERE symbol='QQQ' AND provider='polygon_ws'
                """
            ).fetchone()
        finally:
            con.close()
        self.assertEqual((int(row[0]), int(row[1])), (1, 1))
        self.assertAlmostEqual(float(row[2]), 431.10, places=6)
        self.assertAlmostEqual(float(row[3]), 431.20, places=6)

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
