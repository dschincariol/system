from __future__ import annotations

import importlib
import importlib.util
import os
import sys
import tempfile
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


class TsfreshFeatureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "USE_TSFRESH_FEATURES",
                "TSFRESH_WINDOW_S",
                "TSFRESH_FC_PROFILE",
                "TSFRESH_MAX_FEATURES",
                "TSFRESH_N_JOBS",
                "TSFRESH_MAX_N_JOBS",
                "TSFRESH_SNAPSHOT_SYMBOL_LIMIT",
                "TSFRESH_SNAPSHOT_MAX_SYMBOLS",
                "TSFRESH_SNAPSHOT_BATCH_SIZE",
                "TSFRESH_SNAPSHOT_MAX_BATCH_SIZE",
                "TSFRESH_USE_PERSISTED_SNAPSHOTS",
                "TSFRESH_LIVE_COMPUTE_ENABLED",
                "TSFRESH_SNAPSHOT_BUCKET_SEC",
                "MODEL_FEATURE_SNAPSHOT_BUCKET_SEC",
                "USE_SYMBOL_SNAPSHOT_FEATURES",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "tsfresh_features.db")
        os.environ["TSFRESH_WINDOW_S"] = "3600"
        os.environ["TSFRESH_FC_PROFILE"] = "minimal"
        os.environ["TSFRESH_MAX_FEATURES"] = "6"
        os.environ["TSFRESH_N_JOBS"] = "0"
        os.environ["TSFRESH_MAX_N_JOBS"] = "1"
        os.environ["TSFRESH_SNAPSHOT_SYMBOL_LIMIT"] = "10"
        os.environ["TSFRESH_SNAPSHOT_MAX_SYMBOLS"] = "10"
        os.environ["TSFRESH_SNAPSHOT_BATCH_SIZE"] = "4"
        os.environ["TSFRESH_SNAPSHOT_MAX_BATCH_SIZE"] = "4"
        os.environ["TSFRESH_USE_PERSISTED_SNAPSHOTS"] = "1"
        os.environ["TSFRESH_LIVE_COMPUTE_ENABLED"] = "0"
        os.environ["TSFRESH_SNAPSHOT_BUCKET_SEC"] = "60"
        os.environ["MODEL_FEATURE_SNAPSHOT_BUCKET_SEC"] = "60"

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

    def _insert_price_series(self, storage, *, symbol: str, start_ts_ms: int, values: list[float]) -> None:
        con = storage.connect_rw_direct()
        try:
            for idx, value in enumerate(values):
                ts_ms = int(start_ts_ms + (idx * 60_000))
                con.execute(
                    """
                    INSERT OR REPLACE INTO prices(ts_ms, symbol, price, px, source)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (int(ts_ms), str(symbol), float(value), float(value), "unit_test"),
                )
            con.commit()
        finally:
            con.close()

    def test_feature_ids_are_deterministic(self) -> None:
        first, = _reload_modules("engine.strategy.tsfresh_features")
        second, = _reload_modules("engine.strategy.tsfresh_features")

        self.assertEqual(first.get_tsfresh_feature_ids(), second.get_tsfresh_feature_ids())
        self.assertEqual(first.get_default_tsfresh_feature_ids(), second.get_default_tsfresh_feature_ids())
        self.assertTrue(first.get_default_tsfresh_feature_ids())
        self.assertEqual(
            len(first.get_tsfresh_feature_ids()),
            len(set(first.get_tsfresh_feature_ids())),
        )
        self.assertTrue(all(fid.startswith("tsfresh.") for fid in first.get_tsfresh_feature_ids()))

    def test_tsfresh_parallelism_is_configurable_and_bounded(self) -> None:
        os.environ["TSFRESH_N_JOBS"] = "8"
        os.environ["TSFRESH_MAX_N_JOBS"] = "3"
        tsfresh_features, = _reload_modules("engine.strategy.tsfresh_features")

        self.assertEqual(tsfresh_features.TSFRESH_N_JOBS, 3)

        from engine.strategy.discovery.tsfresh_discoverer import TsfreshDiscoverer

        discoverer = TsfreshDiscoverer(n_jobs=9)
        self.assertEqual(discoverer.n_jobs, 3)

    def test_tsfresh_snapshot_materialization_bounds_symbols_and_batches(self) -> None:
        os.environ["TSFRESH_SNAPSHOT_SYMBOL_LIMIT"] = "2"
        os.environ["TSFRESH_SNAPSHOT_MAX_SYMBOLS"] = "2"
        os.environ["TSFRESH_SNAPSHOT_BATCH_SIZE"] = "1"
        os.environ["TSFRESH_SNAPSHOT_MAX_BATCH_SIZE"] = "1"
        tsfresh_features, = _reload_modules("engine.strategy.tsfresh_features")
        built_symbols: list[str] = []
        stored_batch_sizes: list[int] = []

        def fake_build(*, symbol, ts_ms, window_s, con=None):
            built_symbols.append(str(symbol))
            return {"symbol": str(symbol), "ts": int(ts_ms), "window_s": int(window_s), "features": {}}

        def fake_store(snapshots, *, con=None):
            rows = list(snapshots or [])
            stored_batch_sizes.append(len(rows))
            return len(rows)

        with (
            patch.object(tsfresh_features, "build_tsfresh_feature_snapshot", side_effect=fake_build),
            patch.object(tsfresh_features, "store_tsfresh_feature_snapshots", side_effect=fake_store),
        ):
            summary = tsfresh_features.materialize_tsfresh_feature_snapshots(
                symbols=["aapl", "msft", "AAPL", "nvda"],
                ts_ms=1_710_000_000_123,
                window_s=3600,
            )

        self.assertEqual(built_symbols, ["AAPL", "MSFT"])
        self.assertEqual(stored_batch_sizes, [1, 1])
        self.assertEqual(int(summary["symbols"]), 2)
        self.assertEqual(int(summary["snapshots"]), 2)
        self.assertEqual(int(summary["symbol_limit"]), 2)
        self.assertEqual(int(summary["batch_size"]), 1)

    @unittest.skipUnless(importlib.util.find_spec("tsfresh") is not None, "tsfresh not installed")
    def test_extracts_features_from_synthetic_series(self) -> None:
        storage = self._init_storage()
        tsfresh_features, = _reload_modules("engine.strategy.tsfresh_features")
        start_ts_ms = 1_710_000_000_000 - (31 * 60_000)
        values = [100.0 + (idx * 0.75) + ((idx % 5) * 0.1) for idx in range(32)]
        self._insert_price_series(storage, symbol="AAPL", start_ts_ms=start_ts_ms, values=values)

        window_df = tsfresh_features.build_tsfresh_window("AAPL", 1_710_000_000_000, 3600)
        features = tsfresh_features.compute_tsfresh_features(window_df)

        self.assertEqual(int(len(window_df.index)), 32)
        self.assertEqual(float(features["tsfresh.length"]), 32.0)
        self.assertGreater(float(features["tsfresh.maximum"]), float(features["tsfresh.minimum"]))
        self.assertGreater(float(features["tsfresh.variance"]), 0.0)
        self.assertGreater(float(features["tsfresh.mean_abs_change"]), 0.0)

    def test_snapshot_write_and_read_round_trip(self) -> None:
        storage = self._init_storage()
        tsfresh_features, = _reload_modules("engine.strategy.tsfresh_features")
        snapshot = {
            "symbol": "AAPL",
            "ts": 1_710_000_000_000,
            "window_s": 3600,
            "features": {
                "tsfresh.abs_energy": 12.5,
                "tsfresh.mean": 101.25,
            },
        }

        wrote = tsfresh_features.store_tsfresh_feature_snapshots([snapshot])
        loaded = tsfresh_features.load_tsfresh_feature_snapshot(
            symbol="AAPL",
            ts_ms=1_710_000_000_000,
            window_s=3600,
        )

        self.assertEqual(wrote, 1)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertEqual(str(loaded["symbol"]), "AAPL")
        self.assertEqual(int(loaded["ts"]), 1_710_000_000_000)
        self.assertEqual(int(loaded["window_s"]), 3600)
        self.assertEqual(float(loaded["features"]["tsfresh.abs_energy"]), 12.5)
        self.assertEqual(float(loaded["features"]["tsfresh.mean"]), 101.25)
        self.assertIn("tsfresh.variance", loaded["features"])

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM tsfresh_feature_snapshots WHERE symbol = ?",
                ("AAPL",),
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 1)

    def test_train_serve_parity_uses_persisted_tsfresh_snapshot_when_enabled(self) -> None:
        os.environ["USE_TSFRESH_FEATURES"] = "1"
        os.environ["USE_SYMBOL_SNAPSHOT_FEATURES"] = "0"
        self._init_storage()
        tsfresh_features, feature_registry, feature_expansion = _reload_modules(
            "engine.strategy.tsfresh_features",
            "engine.strategy.feature_registry",
            "engine.strategy.feature_expansion",
        )

        event = {
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "AAPL scheduled earnings",
            "body": "Quarterly report",
        }
        tsfresh_ids = list(tsfresh_features.get_default_tsfresh_feature_ids())
        persisted = {fid: float(idx + 1) for idx, fid in enumerate(tsfresh_ids)}
        tsfresh_features.store_tsfresh_feature_snapshots(
            [
                {
                    "symbol": "AAPL",
                    "ts": int(event["ts_ms"]),
                    "window_s": int(tsfresh_features.TSFRESH_WINDOW_S),
                    "features": dict(persisted),
                }
            ]
        )

        default_ids = feature_registry.default_feature_ids()
        feature_ids = [
            "base.source_credibility",
            "base.normalized_text_len",
        ] + list(tsfresh_ids)
        with patch.object(feature_registry, "_schedule_feature_store_write", return_value=None):
            snapshot = feature_registry.build_feature_snapshot(
                event=event,
                symbol="AAPL",
                feature_ids=list(feature_ids),
            )
            vector = feature_expansion.build_feature_vector(
                event=event,
                symbol="AAPL",
                feature_ids=list(feature_ids),
            )

        self.assertTrue(all(fid in default_ids for fid in tsfresh_ids))
        self.assertEqual(
            feature_registry.resolve_feature_ids(model_spec={"feature_ids": list(feature_ids)}),
            list(feature_ids),
        )
        self.assertIn("tsfresh", feature_expansion.feature_set_tag(list(feature_ids)))
        for fid, expected in persisted.items():
            self.assertEqual(float(snapshot[fid]), float(expected))
            self.assertEqual(float(vector[feature_ids.index(fid)]), float(expected))

    def test_disabled_path_remains_backward_compatible(self) -> None:
        os.environ["USE_TSFRESH_FEATURES"] = "0"
        os.environ["USE_SYMBOL_SNAPSHOT_FEATURES"] = "0"
        self._init_storage()
        feature_registry, feature_expansion = _reload_modules(
            "engine.strategy.feature_registry",
            "engine.strategy.feature_expansion",
        )

        default_ids = feature_registry.default_feature_ids()
        self.assertFalse(any(fid.startswith("tsfresh.") for fid in default_ids))
        self.assertNotIn("tsfresh", feature_expansion.feature_set_tag(list(default_ids)))

        event = {
            "ts_ms": 1_710_000_000_000,
            "ref_ts_ms": 1_710_000_000_000,
            "source": "rss:reuters",
            "title": "AAPL scheduled earnings",
            "body": "Quarterly report",
        }
        with (
            patch.object(feature_registry, "_schedule_feature_store_write", return_value=None),
            patch.object(feature_registry, "resolve_tsfresh_features", side_effect=AssertionError("unexpected_tsfresh_load")),
        ):
            snapshot = feature_registry.build_feature_snapshot(
                event=event,
                symbol="AAPL",
                feature_ids=[
                    "base.source_credibility",
                    "base.normalized_text_len",
                    "base.scheduled_flag",
                ],
            )

        self.assertIn("base.source_credibility", snapshot)
        self.assertGreater(float(snapshot["base.source_credibility"]), 0.0)
        self.assertEqual(float(snapshot["base.scheduled_flag"]), 1.0)


if __name__ == "__main__":
    unittest.main()
