from __future__ import annotations

import importlib
import math
import os
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_con() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(symbol TEXT, ts_ms INTEGER, px REAL, price REAL)")
    con.execute("CREATE TABLE execution_fills(fill_ts_ms INTEGER, liquidity TEXT)")
    con.execute("CREATE TABLE execution_capital_efficiency(ts_ms INTEGER, drawdown_contrib REAL)")
    con.execute(
        """
        CREATE TABLE factor_features(
          feature_id TEXT,
          asof_ts INTEGER,
          effective_ts INTEGER,
          value REAL,
          meta_json TEXT
        )
        """
    )
    return con


class FxRegimeLayerTests(unittest.TestCase):
    def tearDown(self) -> None:
        os.environ.pop("USE_FX_REGIME", None)

    def test_fx_regime_keys_merge_into_macro_and_flatten(self) -> None:
        canary = "CANARY-" + uuid.uuid4().hex
        os.environ["FX04_SECRET_SHAPED_VALUE"] = canary
        os.environ["USE_FX_REGIME"] = "1"
        self.addCleanup(os.environ.pop, "FX04_SECRET_SHAPED_VALUE", None)
        regime_stack = importlib.reload(importlib.import_module("engine.strategy.regime_stack"))
        con = _make_con()
        try:
            for fid, value in (
                ("fx.dxy_level_z", 2.5),
                ("fx.dxy_ret_5d", -0.4),
                ("fx.carry_to_vol", 1.5),
            ):
                con.execute(
                    "INSERT INTO factor_features(feature_id, asof_ts, effective_ts, value, meta_json) VALUES (?,?,?,?,?)",
                    (fid, 1_000, 1_000, value, "{}"),
                )
            vec = regime_stack.compute_regime_vector(symbol="EURUSD", ts_ms=2_000, con=con, include_hmm=False)
            macro = vec["macro"]
            for key in ("fx_usd_strength_z", "fx_usd_strength_dir", "fx_carry_pressure"):
                self.assertIn(key, macro)
                self.assertTrue(math.isfinite(float(macro[key])))
            self.assertLessEqual(abs(float(macro["fx_usd_strength_z"])), 10.0)
            self.assertLessEqual(abs(float(macro["fx_usd_strength_dir"])), 1.0)
            self.assertGreaterEqual(float(macro["fx_carry_pressure"]), 0.0)
            self.assertLessEqual(float(macro["fx_carry_pressure"]), 1.0)

            flat = regime_stack._flatten_regime_vector(vec)
            self.assertIn("macro.fx_usd_strength_z", flat)
            compat = regime_stack.regime_compatibility({"macro": {"fx_carry_pressure": 1.0}}, vec)
            self.assertTrue(0.0 <= compat <= 1.0)
            self.assertNotIn(canary, repr(vec))
        finally:
            con.close()

    def test_flag_off_is_noop_for_equity_vector_shape(self) -> None:
        os.environ["USE_FX_REGIME"] = "0"
        regime_stack = importlib.reload(importlib.import_module("engine.strategy.regime_stack"))
        con = _make_con()
        try:
            vec = regime_stack.compute_regime_vector(symbol="SPY", ts_ms=2_000, con=con, include_hmm=False)
            self.assertNotIn("fx_usd_strength_z", vec["macro"])
            self.assertNotIn("fx_carry_pressure", vec["macro"])
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
