"""Regression tests for the hybrid price feature store."""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class FeatureStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "FEATURE_STORE_TTL_S",
                "PRICE_CACHE_TTL_S",
                "PRICE_CACHE_MAX_POINTS",
                "TS_STORAGE_BACKEND",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "feature_store.db")
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["FEATURE_STORE_TTL_S"] = "0.05"
        os.environ["PRICE_CACHE_TTL_S"] = "3600"
        os.environ["PRICE_CACHE_MAX_POINTS"] = "512"
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

    def tearDown(self) -> None:
        try:
            storage, price_cache, feature_store = _reload_modules(
                "engine.runtime.storage",
                "engine.data.price_cache",
                "engine.data.feature_store",
            )
            price_cache.clear_price_cache()
            feature_store.clear_feature_cache()
            storage.close_pooled_connections()
        except Exception as exc:
            sys.stderr.write(f"[test_feature_store] teardown_failed: {type(exc).__name__}: {exc}\n")
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            storage, price_cache, feature_store = _reload_modules(
                "engine.runtime.storage",
                "engine.data.price_cache",
                "engine.data.feature_store",
            )
            price_cache.clear_price_cache()
            feature_store.clear_feature_cache()
            storage.close_pooled_connections()
        except Exception as exc:
            sys.stderr.write(f"[test_feature_store] restore_failed: {type(exc).__name__}: {exc}\n")
        self.tmp.cleanup()

    def test_compute_features_is_deterministic_for_same_snapshot(self) -> None:
        price_cache, feature_store = _reload_modules(
            "engine.data.price_cache",
            "engine.data.feature_store",
        )

        base_ts_ms = int(time.time() * 1000) - (30 * 60 * 1000)
        rows = [
            {
                "ts_ms": int(base_ts_ms + (idx * 60_000)),
                "price": float(100.0 + idx),
                "volume": float(1_000 + (idx * 25)),
            }
            for idx in range(25)
        ]
        snapshot = price_cache.snapshot_from_rows("AAPL", rows)

        first = feature_store.compute_features("AAPL", snapshot)
        second = feature_store.compute_features("AAPL", snapshot)

        self.assertEqual(first, second)
        self.assertEqual(first["symbol"], "AAPL")
        self.assertEqual(first["ts_ms"], rows[-1]["ts_ms"])
        self.assertIn("rolling_return_5m", first["features"])
        self.assertIn("volatility_20", first["features"])
        self.assertIn("momentum_5m", first["features"])
        self.assertIn("volume_rel_20", first["features"])
        self.assertGreater(first["features"]["volume_last"], 0.0)

    def test_get_features_expires_cache_and_recovers_latest_snapshot_from_db(self) -> None:
        storage, price_cache, feature_store, price_router = _reload_modules(
            "engine.runtime.storage",
            "engine.data.price_cache",
            "engine.data.feature_store",
            "engine.runtime.price_router",
        )

        base_ts_ms = int(time.time() * 1000) - (25 * 60 * 1000)
        rows = [
            {
                "ts_ms": int(base_ts_ms + (idx * 60_000)),
                "price": float(150.0 + idx),
                "volume": float(2_000 + (idx * 10)),
            }
            for idx in range(25)
        ]
        snapshot = price_cache.snapshot_from_rows("MSFT", rows)
        stored = feature_store.store_features("MSFT", feature_store.compute_features("MSFT", snapshot))

        newer = json.loads(json.dumps(stored))
        newer["ts_ms"] = int(stored["ts_ms"]) + 60_000
        newer["features"]["rolling_return_5m"] = 9.99

        def _write_newer(db):
            db.execute(
                """
                INSERT OR REPLACE INTO market_features(ts_ms, symbol, v, features_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    int(newer["ts_ms"]),
                    "MSFT",
                    int(newer.get("schema_version") or 1),
                    json.dumps(newer, separators=(",", ":"), sort_keys=True),
                ),
            )

        storage.run_write_txn(_write_newer, table="market_features", operation="test_write_newer_snapshot")

        time.sleep(0.08)
        recovered = feature_store.get_features("MSFT")

        self.assertEqual(int(recovered["ts_ms"]), int(newer["ts_ms"]))
        self.assertEqual(float(recovered["features"]["rolling_return_5m"]), 9.99)

    def test_offline_asof_features_match_live_feature_logic(self) -> None:
        storage, price_cache, feature_store, price_router = _reload_modules(
            "engine.runtime.storage",
            "engine.data.price_cache",
            "engine.data.feature_store",
            "engine.runtime.price_router",
        )

        base_ts_ms = int(time.time() * 1000) - (40 * 60 * 1000)
        rows = [
            {
                "symbol": "AMD",
                "ts_ms": int(base_ts_ms + (idx * 60_000)),
                "price": float(90.0 + (idx * 0.5)),
                "volume": float(3_000 + (idx * 50)),
                "source": "unit_test",
            }
            for idx in range(30)
        ]

        for row in rows:
            price_router.publish_price_event(
                {
                    "symbol": str(row["symbol"]),
                    "timestamp": int(row["ts_ms"]),
                    "last": float(row["price"]),
                    "volume": float(row["volume"]),
                    "provider": str(row["source"]),
                    "source": str(row["source"]),
                    "latency_ms": 1,
                },
                emit_telemetry=False,
            )

        target_ts_ms = int(rows[19]["ts_ms"])
        live_like_snapshot = price_cache.snapshot_from_rows(
            "AMD",
            [row for row in rows if int(row["ts_ms"]) <= int(target_ts_ms)],
        )
        live_like = feature_store.compute_features("AMD", live_like_snapshot)
        con = storage.connect(readonly=True)
        try:
            offline = feature_store.get_features_asof("AMD", int(target_ts_ms), con=con, persist=False)
        finally:
            con.close()

        self.assertEqual(live_like, offline)

    def test_price_router_hook_refreshes_price_cache_and_feature_store(self) -> None:
        storage, price_cache, feature_store, price_router = _reload_modules(
            "engine.runtime.storage",
            "engine.data.price_cache",
            "engine.data.feature_store",
            "engine.runtime.price_router",
        )

        base_ts_ms = int(time.time() * 1000) - (30 * 60 * 1000)
        for idx in range(25):
            price_router.publish_price_event(
                {
                    "symbol": "NVDA",
                    "timestamp": int(base_ts_ms + (idx * 60_000)),
                    "last": float(200.0 + (idx * 0.75)),
                    "volume": float(5_000 + (idx * 100)),
                    "provider": "unit_test",
                    "source": "unit_test",
                    "latency_ms": 1,
                },
                emit_telemetry=False,
            )

        cached_snapshot = price_cache.get_symbol_snapshot("NVDA", allow_db_recovery=False)
        feature_snapshot = feature_store.get_features("NVDA")

        self.assertEqual(int(cached_snapshot.asof_ts_ms), int(base_ts_ms + (24 * 60_000)))
        self.assertEqual(int(feature_snapshot["ts_ms"]), int(base_ts_ms + (24 * 60_000)))
        self.assertGreater(feature_snapshot["features"]["volatility_20"], 0.0)
        self.assertGreater(feature_snapshot["features"]["volume_rel_20"], 0.0)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM market_features WHERE symbol = ?",
                ("NVDA",),
            ).fetchone()
        finally:
            con.close()

        self.assertGreater(int(row[0] or 0), 0)

    def test_sqlite_disabled_get_features_ignores_market_feature_rows(self) -> None:
        with patch.dict(os.environ, {"FEATURE_STORE_SQLITE_WRITE_ENABLED": "0"}, clear=False):
            storage, price_cache, feature_store = _reload_modules(
                "engine.runtime.storage",
                "engine.data.price_cache",
                "engine.data.feature_store",
            )
            storage.init_db()

            base_ts_ms = int(time.time() * 1000) - (25 * 60 * 1000)
            rows = [
                {
                    "symbol": "MSFT",
                    "ts_ms": int(base_ts_ms + (idx * 60_000)),
                    "price": float(150.0 + idx),
                    "volume": float(2_000 + (idx * 10)),
                    "source": "unit_test",
                }
                for idx in range(25)
            ]
            price_cache.record_price_rows(rows)
            expected = feature_store.compute_features(
                "MSFT",
                price_cache.snapshot_from_rows("MSFT", rows),
            )

            sqlite_only = json.loads(json.dumps(expected))
            sqlite_only["ts_ms"] = int(expected["ts_ms"]) + 60_000
            sqlite_only["features"]["rolling_return_5m"] = 9.99
            sqlite_only["vector"][feature_store.FEATURE_NAMES.index("rolling_return_5m")] = 9.99

            def _write_sqlite_only(db):
                db.execute(
                    """
                    INSERT OR REPLACE INTO market_features(ts_ms, symbol, v, features_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        int(sqlite_only["ts_ms"]),
                        "MSFT",
                        int(sqlite_only.get("schema_version") or 1),
                        json.dumps(sqlite_only, separators=(",", ":"), sort_keys=True),
                    ),
                )

            storage.run_write_txn(
                _write_sqlite_only,
                table="market_features",
                operation="test_sqlite_disabled_market_feature_row",
            )
            feature_store.clear_feature_cache("MSFT")

            resolved = feature_store.get_features("MSFT")
            storage_snapshot = storage.get_timeseries_storage_snapshot()

        self.assertEqual(int(resolved["ts_ms"]), int(expected["ts_ms"]))
        self.assertNotEqual(float(resolved["features"]["rolling_return_5m"]), 9.99)
        self.assertEqual(
            str((storage_snapshot.get("market_feature_store") or {}).get("write_mode") or ""),
            "memory",
        )
        self.assertFalse(
            bool((storage_snapshot.get("market_feature_store") or {}).get("sqlite_write_enabled"))
        )
        self.assertFalse(
            bool((storage_snapshot.get("market_feature_store") or {}).get("sqlite_read_fallback_enabled"))
        )

    def test_sqlite_disabled_get_features_asof_recomputes_from_runtime_snapshot(self) -> None:
        with patch.dict(os.environ, {"FEATURE_STORE_SQLITE_WRITE_ENABLED": "0"}, clear=False):
            storage, price_cache, feature_store = _reload_modules(
                "engine.runtime.storage",
                "engine.data.price_cache",
                "engine.data.feature_store",
            )
            storage.init_db()

            base_ts_ms = int(time.time() * 1000) - (40 * 60 * 1000)
            rows = [
                {
                    "ts_ms": int(base_ts_ms + (idx * 60_000)),
                    "price": float(90.0 + (idx * 0.5)),
                    "volume": float(3_000 + (idx * 50)),
                }
                for idx in range(30)
            ]
            target_ts_ms = int(rows[19]["ts_ms"])
            live_like_snapshot = price_cache.snapshot_from_rows(
                "AMD",
                [row for row in rows if int(row["ts_ms"]) <= int(target_ts_ms)],
            )
            expected = feature_store.compute_features("AMD", live_like_snapshot)

            sqlite_only = json.loads(json.dumps(expected))
            sqlite_only["features"]["rolling_return_5m"] = 9.99
            sqlite_only["vector"][feature_store.FEATURE_NAMES.index("rolling_return_5m")] = 9.99

            def _write_sqlite_only(db):
                db.execute(
                    """
                    INSERT OR REPLACE INTO market_features(ts_ms, symbol, v, features_json)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        int(sqlite_only["ts_ms"]),
                        "AMD",
                        int(sqlite_only.get("schema_version") or 1),
                        json.dumps(sqlite_only, separators=(",", ":"), sort_keys=True),
                    ),
                )

            storage.run_write_txn(
                _write_sqlite_only,
                table="market_features",
                operation="test_sqlite_disabled_market_feature_asof_row",
            )

            resolved = feature_store.get_features_asof(
                "AMD",
                int(target_ts_ms),
                price_cache=live_like_snapshot,
                persist=False,
            )

        self.assertEqual(resolved, expected)

    def test_validate_feature_snapshot_flags_missing_required_fields(self) -> None:
        (feature_store,) = _reload_modules("engine.data.feature_store")
        now_ms = int(time.time() * 1000)
        missing_feature = str(feature_store.FEATURE_NAMES[0])
        snapshot = {
            "symbol": "AAPL",
            "ts_ms": int(now_ms),
            "schema_version": int(getattr(feature_store, "FEATURE_SCHEMA_VERSION", 1)),
            "feature_set_tag": str(feature_store.FEATURE_SET_TAG),
            "feature_names": list(feature_store.FEATURE_NAMES),
            "vector": [float(idx + 1) for idx in range(len(feature_store.FEATURE_NAMES) - 1)],
            "point_count": 64,
            "source_timestamps": {"price_history_last_ts_ms": int(now_ms)},
            "features": {
                str(name): float(idx + 1)
                for idx, name in enumerate(feature_store.FEATURE_NAMES[1:], start=1)
            },
        }

        validation = feature_store.validate_feature_snapshot(snapshot, now_ms=now_ms)

        self.assertFalse(bool(validation["ok"]))
        self.assertEqual(str(validation["detail"]), "feature_required_fields_missing")
        self.assertIn(missing_feature, list(validation["missing_required_features"]))

    def test_get_live_features_records_feature_validation_state_for_empty_snapshot(self) -> None:
        feature_store, data_quality = _reload_modules(
            "engine.data.feature_store",
            "engine.runtime.data_quality",
        )

        snapshot = feature_store.get_live_features("AAPL")
        validation_state = data_quality.get_feature_validation_snapshot()

        self.assertEqual(str(snapshot["symbol"]), "AAPL")
        self.assertFalse(bool(validation_state.get("ok")))
        self.assertEqual(str(validation_state.get("symbol") or ""), "AAPL")
        self.assertIn("feature_snapshot_stale", list(validation_state.get("reason_codes") or []))
        self.assertGreaterEqual(int(validation_state.get("invalid_count_total") or 0), 1)
