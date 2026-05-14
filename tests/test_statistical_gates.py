from __future__ import annotations

import importlib
import math
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


def _returns_with_t_stat(target_t: float, n_obs: int = 60) -> list[float]:
    if n_obs < 2 or (n_obs % 2) != 0:
        raise ValueError("n_obs_must_be_even_and_ge_2")
    mean = float(target_t) / math.sqrt(float(n_obs - 1))
    half = int(n_obs // 2)
    return ([float(mean + 1.0)] * half) + ([float(mean - 1.0)] * half)


class StatisticalGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.statistical_gates, self.promotion_guard = _reload_modules(
            "engine.strategy.statistical_gates",
            "engine.strategy.promotion_guard",
        )

    def test_bootstrap_performance_distribution_is_deterministic_with_seed(self) -> None:
        models_returns = {
            "best": _returns_with_t_stat(4.0, n_obs=40),
            "middle": _returns_with_t_stat(1.0, n_obs=40),
            "worst": _returns_with_t_stat(-0.5, n_obs=40),
        }

        first = self.statistical_gates.bootstrap_performance_distribution(
            models_returns,
            bootstrap_samples=64,
            seed=17,
            min_models=3,
            min_observations=20,
        )
        second = self.statistical_gates.bootstrap_performance_distribution(
            models_returns,
            bootstrap_samples=64,
            seed=17,
            min_models=3,
            min_observations=20,
        )

        self.assertTrue(bool(first.get("applied")))
        self.assertEqual(first.get("distribution"), second.get("distribution"))
        self.assertEqual(int(first.get("model_count") or 0), 3)
        self.assertEqual(int(first.get("bootstrap_samples") or 0), 64)
        self.assertEqual(str(first.get("best_model_name") or ""), "best")

    def test_spa_test_identifies_strong_best_model(self) -> None:
        models_returns = {
            "best": _returns_with_t_stat(5.5, n_obs=60),
            "middle": _returns_with_t_stat(1.0, n_obs=60),
            "worst": _returns_with_t_stat(-0.25, n_obs=60),
        }

        result = self.statistical_gates.spa_test(
            models_returns,
            alpha=0.05,
            bootstrap_samples=256,
            seed=23,
            min_models=3,
            min_observations=50,
        )

        self.assertTrue(bool(result.get("applied")))
        self.assertEqual(str(result.get("status") or ""), "evaluated")
        self.assertEqual(str(result.get("best_model_name") or ""), "best")
        self.assertTrue(bool(result.get("passed")))
        self.assertLess(float(result.get("p_value") or 1.0), 0.05)
        self.assertGreater(float(result.get("best_t_statistic") or 0.0), 5.0)

    def test_white_reality_check_degrades_gracefully_when_not_enough_models(self) -> None:
        result = self.statistical_gates.white_reality_check(
            {"best": _returns_with_t_stat(4.0, n_obs=40)},
            bootstrap_samples=64,
            seed=11,
            min_models=3,
            min_observations=20,
        )

        self.assertFalse(bool(result.get("applied")))
        self.assertEqual(str(result.get("status") or ""), "insufficient_models")
        self.assertTrue(bool(result.get("passed")))

    def test_promotion_guard_applies_spa_only_when_enough_models_exist(self) -> None:
        models_returns = {
            "best": _returns_with_t_stat(5.0, n_obs=60),
            "middle": _returns_with_t_stat(1.0, n_obs=60),
            "worst": _returns_with_t_stat(-0.5, n_obs=60),
        }
        config = {
            "enabled": True,
            "min_t_stat": 3.0,
            "min_deflated_sharpe": 0.0,
            "min_observations": 50,
            "fdr_alpha": 0.05,
            "spa_test_enabled": True,
            "spa_min_models": 3,
            "spa_bootstrap_samples": 256,
            "spa_seed": 31,
        }

        with patch.object(
            self.promotion_guard,
            "evaluate_cpcv_promotion_gate",
            return_value=(True, {"enabled": False, "applied": False, "status": "disabled", "passed": True}),
        ):
            passed, diagnostics = self.promotion_guard.evaluate_statistical_promotion_gate(
                model_name="best",
                candidate_version="best-v1",
                returns=models_returns["best"],
                n_competing_trials=3,
                models_returns=models_returns,
                config=config,
                persist=False,
            )

        self.assertTrue(bool(passed))
        self.assertTrue(bool((diagnostics.get("spa_test") or {}).get("applied")))
        self.assertEqual(str((diagnostics.get("spa_test") or {}).get("best_model_name") or ""), "best")
        self.assertTrue(bool((diagnostics.get("white_reality_check") or {}).get("applied")))
        self.assertEqual(int((diagnostics.get("multiple_testing") or {}).get("candidate_models_considered") or 0), 3)

    def test_promotion_guard_does_not_block_when_spa_has_insufficient_models(self) -> None:
        models_returns = {
            "best": _returns_with_t_stat(5.0, n_obs=60),
            "middle": _returns_with_t_stat(1.0, n_obs=60),
        }
        config = {
            "enabled": True,
            "min_t_stat": 3.0,
            "min_deflated_sharpe": 0.0,
            "min_observations": 50,
            "fdr_alpha": 0.05,
            "spa_test_enabled": True,
            "spa_min_models": 3,
            "spa_bootstrap_samples": 128,
            "spa_seed": 37,
        }

        with patch.object(
            self.promotion_guard,
            "evaluate_cpcv_promotion_gate",
            return_value=(True, {"enabled": False, "applied": False, "status": "disabled", "passed": True}),
        ):
            passed, diagnostics = self.promotion_guard.evaluate_statistical_promotion_gate(
                model_name="best",
                candidate_version="best-v1",
                returns=models_returns["best"],
                n_competing_trials=2,
                models_returns=models_returns,
                config=config,
                persist=False,
            )

        self.assertTrue(bool(passed))
        self.assertFalse(bool((diagnostics.get("spa_test") or {}).get("applied")))
        self.assertEqual(str((diagnostics.get("spa_test") or {}).get("status") or ""), "insufficient_models")
        self.assertTrue(bool(diagnostics.get("spa_pass")))


if __name__ == "__main__":
    unittest.main()
