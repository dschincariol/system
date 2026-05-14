from __future__ import annotations

import asyncio
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _FakeAsyncpgConnection:
    def __init__(self, row):
        self.row = row
        self.executed: list[str] = []
        self.queries: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql, *params):
        self.executed.append(str(sql))
        return "OK"

    async def fetchrow(self, sql, *params):
        self.queries.append((str(sql), tuple(params)))
        return self.row

    async def close(self):
        return None


class StrategyFeatureStoreTests(unittest.TestCase):
    def test_prepare_row_sanitizes_non_finite_values(self) -> None:
        from engine.strategy.feature_store import FeatureStore, FeatureStoreConfig

        store = FeatureStore(
            FeatureStoreConfig(
                enabled=False,
                dsn="",
                schema_name="public",
                batch_size=8,
                flush_interval_s=0.1,
                queue_maxsize=8,
                enqueue_timeout_s=0.1,
                retry_attempts=1,
                retry_base_s=0.01,
                retry_max_s=0.01,
                connect_timeout_s=0.1,
                command_timeout_s=1.0,
                application_name="unit-test",
            )
        )

        row = store._prepare_row(
            "aapl",
            1_710_000_000_000,
            {
                "good": 1.25,
                "nan_value": float("nan"),
                "inf_value": float("inf"),
                "nested": {"x": float("-inf")},
            },
            3,
        )

        self.assertIsNotNone(row)
        self.assertEqual(row.symbol, "AAPL")
        self.assertEqual(row.feature_version, 3)
        self.assertIn('"nan_value":0.0', row.features_json)
        self.assertIn('"inf_value":0.0', row.features_json)
        self.assertIn('"nested":{"x":0.0}', row.features_json)

    def test_build_feature_snapshot_degrades_open_when_feature_store_enqueue_fails(self) -> None:
        from engine.strategy import feature_registry

        event = {
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "AAPL earnings scheduled",
            "body": "Quarterly update",
        }

        with patch("engine.strategy.feature_store.enqueue_feature_write", side_effect=RuntimeError("boom")):
            snap = feature_registry.build_feature_snapshot(
                event=event,
                symbol="AAPL",
                feature_ids=[
                    "base.source_credibility",
                    "base.normalized_text_len",
                    "base.scheduled_flag",
                ],
            )

        self.assertIn("base.source_credibility", snap)
        self.assertIn("base.normalized_text_len", snap)
        self.assertIn("base.scheduled_flag", snap)
        self.assertGreater(float(snap["base.source_credibility"]), 0.0)
        self.assertEqual(float(snap["base.scheduled_flag"]), 1.0)

    def test_build_feature_snapshot_reads_from_store_when_enabled(self) -> None:
        from engine.strategy import feature_registry

        event = {
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "Ignored by read-through",
            "body": "Ignored by read-through",
        }
        fake_store = types.SimpleNamespace(
            get_features_blocking=lambda *args, **kwargs: {
                "symbol": "AAPL",
                "timestamp": 1_710_000_000_000,
                "version": 1,
                "features": {
                    "base.source_credibility": 9.0,
                    "base.normalized_text_len": 7.0,
                    "base.scheduled_flag": 5.0,
                },
            }
        )

        with patch.object(feature_registry, "FEATURE_STORE_READS_ENABLED", True):
            with patch("engine.strategy.feature_store.get_feature_store", return_value=fake_store):
                snap = feature_registry.build_feature_snapshot(
                    event=event,
                    symbol="AAPL",
                    feature_ids=[
                        "base.source_credibility",
                        "base.normalized_text_len",
                        "base.scheduled_flag",
                    ],
                )

        self.assertEqual(float(snap["base.source_credibility"]), 9.0)
        self.assertEqual(float(snap["base.normalized_text_len"]), 7.0)
        self.assertEqual(float(snap["base.scheduled_flag"]), 5.0)

    def test_build_feature_snapshot_skips_store_reads_when_disabled(self) -> None:
        from engine.strategy import feature_registry

        event = {
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "AAPL earnings scheduled",
            "body": "Quarterly update",
        }

        with patch.object(feature_registry, "FEATURE_STORE_READS_ENABLED", False):
            with patch.object(feature_registry, "_schedule_feature_store_write", return_value=None):
                with patch("engine.strategy.feature_store.get_feature_store", side_effect=AssertionError("unexpected_read")):
                    snap = feature_registry.build_feature_snapshot(
                        event=event,
                        symbol="AAPL",
                        feature_ids=[
                            "base.source_credibility",
                            "base.normalized_text_len",
                            "base.scheduled_flag",
                        ],
                    )

        self.assertGreater(float(snap["base.source_credibility"]), 0.0)
        self.assertEqual(float(snap["base.scheduled_flag"]), 1.0)

    def test_get_features_returns_latest_version_row(self) -> None:
        from engine.strategy.feature_store import FeatureStore, FeatureStoreConfig
        import engine.strategy.feature_store as feature_store_module

        config = FeatureStoreConfig(
            enabled=True,
            dsn="postgres://unit-test",
            schema_name="public",
            batch_size=8,
            flush_interval_s=0.1,
            queue_maxsize=8,
            enqueue_timeout_s=0.1,
            retry_attempts=1,
            retry_base_s=0.01,
            retry_max_s=0.01,
            connect_timeout_s=0.1,
            command_timeout_s=1.0,
            application_name="unit-test",
        )
        row = {
            "symbol": "AAPL",
            "time": datetime(2026, 4, 11, 14, 30, tzinfo=timezone.utc),
            "feature_version": 2,
            "features": {"f1": 1.0, "bad": float("nan")},
        }
        fake_conn = _FakeAsyncpgConnection(row)
        fake_asyncpg = types.SimpleNamespace(connect=AsyncMock(return_value=fake_conn))
        store = FeatureStore(config)

        with patch.object(feature_store_module, "asyncpg", fake_asyncpg):
            payload = asyncio.run(store.get_features("aapl", 1_776_000_000_000))

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["symbol"], "AAPL")
        self.assertEqual(payload["version"], 2)
        self.assertEqual(float(payload["features"]["f1"]), 1.0)
        self.assertEqual(float(payload["features"]["bad"]), 0.0)
        self.assertTrue(any('feature_version DESC' in query for query, _params in fake_conn.queries))

    def test_storage_init_timeseries_storage_includes_feature_store_startup(self) -> None:
        import engine.runtime.storage as storage

        fake_timescale = types.SimpleNamespace(
            init_timescale_client=lambda: {"ok": True, "enabled": True, "started": True},
            shutdown_timescale_client=lambda timeout_s=None: {"ok": True, "enabled": True},
            get_timescale_snapshot=lambda: {"ok": True, "enabled": True},
        )
        fake_feature_store = types.SimpleNamespace(
            init_feature_store=lambda: {"ok": True, "enabled": True, "schema_ready": True},
            close_feature_store=lambda timeout_s=None: None,
            get_feature_store_snapshot=lambda: {"ok": True, "enabled": True, "schema_ready": True},
        )

        with patch.object(storage, "_load_timescale_module", return_value=fake_timescale):
            with patch.object(storage, "_load_feature_store_module", return_value=fake_feature_store):
                snapshot = storage.init_timeseries_storage()

        self.assertTrue(bool(snapshot["ok"]))
        self.assertIn("feature_store", snapshot)
        self.assertTrue(bool(snapshot["feature_store"]["schema_ready"]))

    def test_storage_feature_store_loader_imports_and_caches_runtime_module(self) -> None:
        import engine.runtime.storage as storage

        storage._FEATURE_STORE_MODULE = None
        module = storage._load_feature_store_module()

        self.assertIsNotNone(module)
        self.assertTrue(hasattr(module, "init_feature_store"))
        self.assertTrue(hasattr(module, "get_feature_store_snapshot"))
        self.assertIs(storage._load_feature_store_module(), module)


if __name__ == "__main__":
    unittest.main()
