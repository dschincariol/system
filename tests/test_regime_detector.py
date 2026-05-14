from __future__ import annotations

import importlib
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


class RegimeDetectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "regime_detector.db")
        self.storage, self.engine_regime, self.public_regime = _reload_modules(
            "engine.runtime.storage",
            "engine.regime_detector",
            "regime_detector",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            self.public_regime.shutdown_regime_detector(timeout_s=1.0)
        except Exception:
            pass
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_submit_refresh_computes_and_persists_regime_asynchronously(self) -> None:
        snapshot = {
            "symbol": "AMD",
            "ts_ms": 1_700_000_000_000,
            "features": {
                "volatility_20": 0.045,
                "volatility_60": 0.020,
                "atr_pct_14": 0.025,
                "momentum_1h": -0.006,
                "momentum_1d": -0.012,
                "rolling_return_1d": -0.010,
                "trend_strength_20": 1.4,
                "volume_rel_20": 0.45,
                "dollar_volume_rel_20": 0.50,
                "volume_nonzero_share_20": 0.55,
                "dollar_volume_last": 500_000.0,
            },
        }
        original = self.engine_regime.classify_regime_snapshot

        def _slow_classify(*args, **kwargs):
            time.sleep(0.2)
            return original(*args, **kwargs)

        with patch.object(self.engine_regime, "classify_regime_snapshot", side_effect=_slow_classify):
            started = time.perf_counter()
            queued = self.public_regime.submit_regime_refresh_nowait(
                "AMD",
                feature_snapshot=snapshot,
                ts_ms=int(snapshot["ts_ms"]),
                source="unit_test",
            )
            elapsed_s = time.perf_counter() - started
            self.assertTrue(bool(queued))
            self.assertLess(elapsed_s, 0.1)
            self.assertTrue(bool(self.public_regime.flush_regime_detector(3.0)))

        con = self.storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT time, symbol, volatility_regime, trend_regime, liquidity_regime
                FROM regime_state
                WHERE symbol='AMD'
                ORDER BY time DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(int(row[0]), int(snapshot["ts_ms"]))
        self.assertEqual(str(row[1]), "AMD")
        self.assertEqual(tuple(str(value) for value in row[2:]), ("high", "bearish", "thin"))


if __name__ == "__main__":
    unittest.main()
