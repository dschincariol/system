from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class OptionAssetClassBudgetTest(unittest.TestCase):
    ENV_KEYS = (
        "PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON",
        "PORTFOLIO_RISK_BIND_EQUITY_BUDGET",
        "PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS",
    )

    def setUp(self) -> None:
        self.env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}

    def tearDown(self) -> None:
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _reload_engine(self, **env: str):
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
        for key, value in env.items():
            os.environ[key] = str(value)

        import engine.risk.portfolio_risk_engine as portfolio_risk_engine

        return importlib.reload(portfolio_risk_engine)

    def test_option_budget_default_is_conservative(self) -> None:
        engine = self._reload_engine()
        budgets = engine.ASSET_CLASS_BUDGETS

        self.assertIn("OPTION", budgets)
        self.assertEqual(budgets["OPTION"], 0.20)
        self.assertLessEqual(budgets["OPTION"], budgets["EQUITY"])
        self.assertLess(budgets["OPTION"], budgets["UNKNOWN"])

    def test_option_book_is_scaled_to_option_cap(self) -> None:
        engine = self._reload_engine()
        desired = {
            "OPT_A": {"weight": 0.30, "side": "LONG", "reason": {}},
            "OPT_B": {"weight": 0.20, "side": "LONG", "reason": {}},
        }
        info = {"asset_class_by_symbol": {"OPT_A": "OPTION", "OPT_B": "OPTION"}}

        out = engine._apply_asset_class_budgets(desired, info)

        post_gross = sum(abs(float(row["weight"])) for row in out.values())
        self.assertLessEqual(post_gross, engine.ASSET_CLASS_BUDGETS["OPTION"] + 1e-9)
        self.assertIn("OPTION", info["asset_class_budgets_hit"])
        self.assertAlmostEqual(info["asset_class_budgets_hit"]["OPTION"]["gross_pre"], 0.50)
        self.assertAlmostEqual(info["asset_class_budgets_hit"]["OPTION"]["cap"], 0.20)
        self.assertAlmostEqual(out["OPT_A"]["weight"], 0.12)
        self.assertAlmostEqual(out["OPT_B"]["weight"], 0.08)

    def test_non_option_budgets_and_equity_path_remain_unchanged(self) -> None:
        engine = self._reload_engine(PORTFOLIO_RISK_BIND_EQUITY_BUDGET="0")
        budgets = engine.ASSET_CLASS_BUDGETS

        self.assertEqual(budgets["EQUITY"], 1.00)
        self.assertEqual(budgets["CRYPTO"], 0.35)
        self.assertEqual(budgets["COMMODITY"], 0.50)
        self.assertEqual(budgets["FX"], 0.50)
        self.assertEqual(budgets["RATES"], 0.60)
        self.assertEqual(budgets["UNKNOWN"], 0.40)
        if "FUTURES" in budgets:
            self.assertEqual(budgets["FUTURES"], 0.40)

        desired = {"SPY": {"weight": 0.90, "side": "LONG", "reason": {}}}
        info = {"asset_class_by_symbol": {"SPY": "EQUITY"}}

        out = engine._apply_asset_class_budgets(desired, info)

        self.assertAlmostEqual(out["SPY"]["weight"], 0.90)
        self.assertNotIn("asset_class_budgets_hit", info)

    def test_option_budget_json_override_wins(self) -> None:
        engine = self._reload_engine(PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON='{"OPTION":0.05}')

        self.assertEqual(engine.ASSET_CLASS_BUDGETS["OPTION"], 0.05)


if __name__ == "__main__":
    unittest.main()
