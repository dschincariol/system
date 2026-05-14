from __future__ import annotations

import importlib
import os
import sys
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


class AlmgrenChrissCostTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "ALMGREN_CHRISS_ENABLED",
                "ALMGREN_CHRISS_TEMP_COEF",
                "ALMGREN_CHRISS_PERM_COEF",
                "ALMGREN_CHRISS_RISK_AVERSION",
                "PORTFOLIO_BACKTEST_USE_EXEC_COSTS",
            )
        }

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass

    def test_disabled_path_returns_zero_costs(self) -> None:
        os.environ["ALMGREN_CHRISS_ENABLED"] = "0"
        (almgren_chriss,) = _reload_modules("engine.execution.almgren_chriss")

        result = almgren_chriss.estimate_almgren_chriss_costs(
            symbol="AAPL",
            qty=500,
            px=100.0,
            side="BUY",
            liquidity_snapshot={"adv_participation": 0.05, "intraday_vol_bps": 12.0},
        )

        self.assertFalse(result["enabled"])
        self.assertFalse(result["ok"])
        self.assertEqual(float(result["execution_cost_bps"]), 0.0)
        self.assertEqual(float(result["temporary_impact_bps"]), 0.0)
        self.assertEqual(float(result["permanent_impact_bps"]), 0.0)

    def test_enabled_path_estimates_temp_and_perm_impact(self) -> None:
        os.environ["ALMGREN_CHRISS_ENABLED"] = "1"
        os.environ["ALMGREN_CHRISS_TEMP_COEF"] = "0.20"
        os.environ["ALMGREN_CHRISS_PERM_COEF"] = "0.05"
        os.environ["ALMGREN_CHRISS_RISK_AVERSION"] = "0.0"
        (almgren_chriss,) = _reload_modules("engine.execution.almgren_chriss")

        with patch(
            "engine.execution.almgren_chriss.get_execution_liquidity_snapshot",
            return_value={
                "rolling_adv": 2_000_000.0,
                "adv_participation": 0.04,
                "live_participation_rate": 0.01,
                "intraday_vol_bps": 20.0,
                "true_spread_bps": 4.0,
                "interval_mult": 1.0,
            },
        ):
            result = almgren_chriss.estimate_almgren_chriss_costs(
                symbol="AAPL",
                qty=1_000,
                px=100.0,
                side="BUY",
                ts_ms=1_710_000_000_000,
            )

        self.assertTrue(result["enabled"])
        self.assertTrue(result["ok"])
        self.assertGreater(float(result["temporary_impact_bps"]), 0.0)
        self.assertGreater(float(result["permanent_impact_bps"]), 0.0)
        self.assertAlmostEqual(float(result["execution_cost_bps"]), 0.82, places=2)

    def test_backtest_transition_costs_remain_disabled_by_default(self) -> None:
        os.environ["PORTFOLIO_BACKTEST_USE_EXEC_COSTS"] = "0"
        os.environ["ALMGREN_CHRISS_ENABLED"] = "1"
        _, _, portfolio_backtest = _reload_modules(
            "engine.execution.almgren_chriss",
            "engine.strategy.portfolio",
            "engine.strategy.portfolio_backtest",
        )

        result = portfolio_backtest._estimate_transition_trade_costs(
            None,
            [{"symbol": "AAPL", "side": "LONG", "weight": 0.0}],
            [{"symbol": "AAPL", "side": "LONG", "weight": 0.25}],
            equity=100_000.0,
            ts_ms=1_710_000_000_000,
        )

        self.assertFalse(result["enabled"])
        self.assertEqual(float(result["exec_cost"]), 0.0)
        self.assertEqual(float(result["slippage"]), 0.0)
        self.assertEqual(float(result["fees"]), 0.0)
        self.assertEqual(list(result["trade_costs"]), [])


if __name__ == "__main__":
    unittest.main()
