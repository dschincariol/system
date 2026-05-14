from __future__ import annotations

import copy
import importlib
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


class PortfolioHRPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "PORTFOLIO_ALLOCATION_MODE",
                "PORTFOLIO_CORR_OPT",
                "PORTFOLIO_CORR_PRUNE",
                "PORTFOLIO_GROSS_CAP",
                "PORTFOLIO_RISK_MAX_GROSS",
                "PORTFOLIO_RISK_MAX_NET",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "portfolio_hrp.db")

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
        self.tmp.cleanup()

    def test_required_hrp_functions_produce_normalized_weights(self) -> None:
        (portfolio,) = _reload_modules("engine.strategy.portfolio")
        returns = [
            [0.02, 0.01, -0.01, 0.015, 0.005],
            [0.018, 0.011, -0.008, 0.013, 0.004],
            [-0.01, 0.004, 0.006, -0.003, 0.002],
        ]
        corr_matrix = portfolio.compute_correlation_matrix(returns)
        linkage_matrix = portfolio.hierarchical_clustering(corr_matrix)
        order = portfolio.quasi_diagonalization(linkage_matrix)
        cov_matrix, _meta = portfolio._build_covariance_matrix_from_returns(returns, corr_matrix)
        weights = portfolio.recursive_bisection(order, cov_matrix)

        self.assertEqual(len(corr_matrix), 3)
        self.assertEqual(len(linkage_matrix), 2)
        self.assertEqual(sorted(order), [0, 1, 2])
        self.assertAlmostEqual(sum(weights), 1.0, places=6)
        self.assertTrue(all(float(weight) >= 0.0 for weight in weights))

    def test_default_allocation_mode_preserves_existing_corr_opt_preference(self) -> None:
        os.environ.pop("PORTFOLIO_ALLOCATION_MODE", None)
        os.environ["PORTFOLIO_CORR_OPT"] = "1"
        os.environ["PORTFOLIO_CORR_PRUNE"] = "1"
        _, _, _, portfolio = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.runtime.risk_state",
            "engine.strategy.portfolio",
        )

        self.assertEqual(portfolio._resolve_allocation_mode(), "corr_opt")

    def test_existing_mode_alias_preserves_current_default_behavior(self) -> None:
        os.environ["PORTFOLIO_ALLOCATION_MODE"] = "existing_mode"
        os.environ["PORTFOLIO_CORR_OPT"] = "1"
        os.environ["PORTFOLIO_CORR_PRUNE"] = "1"
        (portfolio,) = _reload_modules("engine.strategy.portfolio")

        self.assertEqual(portfolio._resolve_allocation_mode(), "corr_opt")

    def test_hrp_allocation_is_stable_and_sums_to_one(self) -> None:
        os.environ["PORTFOLIO_GROSS_CAP"] = "1.0"
        (portfolio,) = _reload_modules("engine.strategy.portfolio")
        desired = {
            "AAPL": {"symbol": "AAPL", "side": "LONG", "weight": 0.40, "weight_cap": 0.40, "reason": {}},
            "MSFT": {"symbol": "MSFT", "side": "LONG", "weight": 0.35, "weight_cap": 0.35, "reason": {}},
            "TLT": {"symbol": "TLT", "side": "LONG", "weight": 0.25, "weight_cap": 0.25, "reason": {}},
        }
        returns = {
            "AAPL": [0.020, 0.015, -0.010, 0.012, 0.009, -0.004],
            "MSFT": [0.018, 0.014, -0.009, 0.011, 0.008, -0.003],
            "TLT": [-0.006, 0.004, 0.003, -0.002, 0.002, 0.001],
        }

        with patch.object(
            portfolio,
            "_load_hrp_return_series",
            side_effect=lambda _con, symbol, lookback=0: list(returns[str(symbol)]),
        ):
            first = portfolio._apply_hrp_allocation(None, copy.deepcopy(desired), gross_cap=1.0, lookback=64)
            second = portfolio._apply_hrp_allocation(None, copy.deepcopy(desired), gross_cap=1.0, lookback=64)

        first_total = sum(float(row["weight"]) for row in first.values())
        second_total = sum(float(row["weight"]) for row in second.values())
        self.assertAlmostEqual(first_total, 1.0, places=6)
        self.assertAlmostEqual(second_total, 1.0, places=6)
        for symbol in desired:
            self.assertAlmostEqual(float(first[symbol]["weight"]), float(second[symbol]["weight"]), places=9)
            self.assertEqual(str(first[symbol]["reason"]["allocation_mode"]), "hrp")

    def test_hrp_handles_missing_covariance_data_without_losing_budget(self) -> None:
        (portfolio,) = _reload_modules("engine.strategy.portfolio")
        desired = {
            "AAPL": {"symbol": "AAPL", "side": "LONG", "weight": 0.60, "weight_cap": 0.60, "reason": {}},
            "MSFT": {"symbol": "MSFT", "side": "LONG", "weight": 0.40, "weight_cap": 0.40, "reason": {}},
        }

        with patch.object(portfolio, "_load_hrp_return_series", return_value=[]):
            result = portfolio._apply_hrp_allocation(None, copy.deepcopy(desired), gross_cap=1.0, lookback=64)

        total = sum(float(row["weight"]) for row in result.values())
        self.assertAlmostEqual(total, 1.0, places=6)
        self.assertAlmostEqual(float(result["AAPL"]["weight"]), 0.60, places=6)
        self.assertAlmostEqual(float(result["MSFT"]["weight"]), 0.40, places=6)
        self.assertEqual(str(result["AAPL"]["reason"]["hrp_covariance_source"]), "diagonal_fallback")

    def test_hrp_mode_falls_back_to_existing_allocation_mode(self) -> None:
        os.environ["PORTFOLIO_ALLOCATION_MODE"] = "hrp"
        os.environ["PORTFOLIO_CORR_OPT"] = "1"
        os.environ["PORTFOLIO_CORR_PRUNE"] = "0"
        (portfolio,) = _reload_modules("engine.strategy.portfolio")
        desired = {
            "AAPL": {"symbol": "AAPL", "side": "LONG", "weight": 0.30, "reason": {}},
            "MSFT": {"symbol": "MSFT", "side": "LONG", "weight": 0.20, "reason": {}},
        }
        sentinel = copy.deepcopy(desired)

        with patch.object(portfolio, "_apply_hrp_allocation", side_effect=RuntimeError("hrp failed")), patch.object(
            portfolio,
            "_apply_existing_allocation_mode",
            return_value=sentinel,
        ) as mocked_existing:
            result = portfolio._apply_allocation_mode(None, copy.deepcopy(desired))

        mocked_existing.assert_called_once()
        self.assertEqual(str(result["AAPL"]["reason"]["allocation_mode_requested"]), "hrp")
        self.assertEqual(str(result["AAPL"]["reason"]["allocation_mode_fallback"]), "corr_opt")
        self.assertIn("hrp failed", str(result["AAPL"]["reason"]["hrp_error"]))

    def test_hrp_output_remains_compatible_with_existing_risk_caps(self) -> None:
        os.environ["PORTFOLIO_GROSS_CAP"] = "1.30"
        os.environ["PORTFOLIO_RISK_MAX_GROSS"] = "0.80"
        os.environ["PORTFOLIO_RISK_MAX_NET"] = "0.25"
        portfolio, risk_engine = _reload_modules(
            "engine.strategy.portfolio",
            "engine.risk.portfolio_risk_engine",
        )
        desired = {
            "AAPL": {"symbol": "AAPL", "side": "LONG", "weight": 0.70, "weight_cap": 0.70, "reason": {}},
            "MSFT": {"symbol": "MSFT", "side": "LONG", "weight": 0.30, "weight_cap": 0.30, "reason": {}},
            "TSLA": {"symbol": "TSLA", "side": "SHORT", "weight": 0.30, "weight_cap": 0.30, "reason": {}},
        }
        returns = {
            "AAPL": [0.020, 0.015, -0.010, 0.012, 0.009, -0.004],
            "MSFT": [0.018, 0.014, -0.009, 0.011, 0.008, -0.003],
            "TSLA": [-0.012, 0.008, -0.010, 0.006, -0.004, 0.002],
        }

        with patch.object(
            portfolio,
            "_load_hrp_return_series",
            side_effect=lambda _con, symbol, lookback=0: list(returns[str(symbol)]),
        ):
            hrp_desired = portfolio._apply_hrp_allocation(None, copy.deepcopy(desired), gross_cap=1.30, lookback=64)

        capped = risk_engine._apply_portfolio_caps(copy.deepcopy(hrp_desired), info={})
        snapshot = risk_engine._exposure_snapshot(capped)

        self.assertLessEqual(float(snapshot["gross"]), float(risk_engine.MAX_GROSS) + 1e-9)
        self.assertLessEqual(abs(float(snapshot["net"])), float(risk_engine.MAX_NET) + 1e-9)


if __name__ == "__main__":
    unittest.main()
