from __future__ import annotations

import copy
import importlib
import json
import os
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class AlphaShrinkageTests(unittest.TestCase):
    def _config(self):
        from engine.strategy.alpha_shrinkage import AlphaShrinkageConfig

        return AlphaShrinkageConfig(
            enabled=True,
            prior_strength=24.0,
            missing_prior_strength=48.0,
            min_prior_observations=3.0,
            fallback_mean=0.0,
            allow_upsizing=False,
            prior_levels=("sector", "global"),
        )

    def test_low_sample_alpha_shrinks_more_than_high_evidence_alpha(self) -> None:
        from engine.strategy.alpha_shrinkage import AlphaObservation, shrink_alpha_estimates

        cfg = self._config()
        observations = [
            AlphaObservation(key="low", symbol="LOW", raw_estimate=0.10, n_obs=2, sector="tech"),
            AlphaObservation(key="high", symbol="HIGH", raw_estimate=0.10, n_obs=200, sector="tech"),
            AlphaObservation(key="peer", symbol="PEER", raw_estimate=0.02, n_obs=200, sector="tech"),
        ]

        result = shrink_alpha_estimates(observations, config=cfg)

        self.assertLess(float(result["low"]["own_weight"]), float(result["high"]["own_weight"]))
        self.assertGreater(float(result["low"]["shrinkage_abs"]), float(result["high"]["shrinkage_abs"]))
        self.assertLess(float(result["low"]["effective_estimate"]), 0.10)
        self.assertLess(float(result["high"]["effective_estimate"]), 0.10)

    def test_missing_priors_fall_back_to_neutral_conservatively(self) -> None:
        from engine.strategy.alpha_shrinkage import AlphaObservation, shrink_alpha_estimates

        cfg = self._config()
        result = shrink_alpha_estimates(
            [AlphaObservation(key="solo", symbol="SOLO", raw_estimate=0.12, n_obs=2)],
            config=cfg,
        )
        solo = result["solo"]

        self.assertTrue(bool(solo["conservative_fallback"]))
        self.assertEqual(str(solo["prior_source"]), "conservative_missing_prior")
        self.assertAlmostEqual(float(solo["prior_mean"]), 0.0, places=8)
        self.assertLess(float(solo["effective_estimate"]), 0.01)
        self.assertLess(float(solo["size_multiplier"]), 0.10)

    def test_hierarchy_uses_model_family_prior_when_sector_is_missing(self) -> None:
        from engine.strategy.alpha_shrinkage import AlphaObservation, AlphaShrinkageConfig, shrink_alpha_estimates

        cfg = AlphaShrinkageConfig(
            enabled=True,
            prior_strength=24.0,
            missing_prior_strength=48.0,
            min_prior_observations=3.0,
            prior_levels=("sector", "model_family", "global"),
        )
        result = shrink_alpha_estimates(
            [
                AlphaObservation(key="thin", symbol="THIN", raw_estimate=0.08, n_obs=2, model_family="ridge"),
                AlphaObservation(key="peer", symbol="PEER", raw_estimate=0.01, n_obs=100, model_family="ridge"),
            ],
            config=cfg,
        )

        self.assertEqual(str(result["thin"]["prior_level"]), "model_family")
        self.assertEqual(str(result["thin"]["prior_key"]), "ridge")
        self.assertLess(float(result["thin"]["effective_estimate"]), 0.08)

    def test_live_mode_forces_shrinkage_enabled_even_if_env_disables_it(self) -> None:
        import engine.strategy.alpha_shrinkage as alpha_shrinkage

        backup = {
            "ENGINE_MODE": os.environ.get("ENGINE_MODE"),
            "EXECUTION_MODE": os.environ.get("EXECUTION_MODE"),
            "ALPHA_SHRINKAGE_ENABLED": os.environ.get("ALPHA_SHRINKAGE_ENABLED"),
        }
        os.environ["ENGINE_MODE"] = "live"
        os.environ["EXECUTION_MODE"] = "live"
        os.environ["ALPHA_SHRINKAGE_ENABLED"] = "0"
        try:
            cfg = alpha_shrinkage.config_from_env()
            self.assertTrue(bool(cfg.enabled))
        finally:
            for key, value in backup.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_portfolio_overlay_reduces_weight_and_feeds_adjusted_return_to_optimizer(self) -> None:
        from engine.strategy.alpha_shrinkage import apply_alpha_shrinkage_to_desired

        env_backup = {
            "PORTFOLIO_ALLOC_OPT": os.environ.get("PORTFOLIO_ALLOC_OPT"),
            "PORTFOLIO_USE_EXEC_REGIME": os.environ.get("PORTFOLIO_USE_EXEC_REGIME"),
        }
        os.environ["PORTFOLIO_ALLOC_OPT"] = "1"
        os.environ["PORTFOLIO_USE_EXEC_REGIME"] = "0"
        try:
            portfolio = importlib.reload(importlib.import_module("engine.strategy.portfolio"))

            desired = {
                "model:AAPL": self._target("AAPL", expected_ret_net=0.12, n_obs=2, sector="tech"),
                "model:MSFT": self._target("MSFT", expected_ret_net=0.12, n_obs=200, sector="tech"),
                "model:NVDA": self._target("NVDA", expected_ret_net=0.02, n_obs=200, sector="tech"),
            }

            shrunk, diagnostics = apply_alpha_shrinkage_to_desired(
                None,
                copy.deepcopy(desired),
                now_ms=1_700_000_000_000,
                config=self._config(),
            )

            low_reason = shrunk["model:AAPL"]["reason"]["alpha_shrinkage"]
            high_reason = shrunk["model:MSFT"]["reason"]["alpha_shrinkage"]
            self.assertTrue(bool(diagnostics["applied"]))
            self.assertLess(float(low_reason["size_multiplier"]), float(high_reason["size_multiplier"]))
            self.assertLess(float(shrunk["model:AAPL"]["weight"]), float(shrunk["model:MSFT"]["weight"]))
            self.assertLess(float(shrunk["model:AAPL"]["adjusted_expected_ret_net"]), 0.12)

            explain = json.loads(shrunk["model:AAPL"]["explain_json"])
            self.assertAlmostEqual(
                float(explain["tradability"]["expected_ret_net"]),
                float(shrunk["model:AAPL"]["adjusted_expected_ret_net"]),
                places=8,
            )

            optimized = portfolio._optimize_capital_allocation(None, copy.deepcopy(shrunk))
            self.assertLess(
                float(optimized["model:AAPL"]["reason"]["alloc_util"]),
                float(optimized["model:MSFT"]["reason"]["alloc_util"]),
            )
        finally:
            for key, value in env_backup.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            importlib.reload(importlib.import_module("engine.strategy.portfolio"))

    def _target(self, symbol: str, *, expected_ret_net: float, n_obs: int, sector: str) -> dict:
        return {
            "symbol": symbol,
            "side": "LONG",
            "weight": 0.20,
            "horizon_s": 3600,
            "reason": {
                "confidence": 0.80,
                "expected_z": 1.5,
                "alpha_n_obs": int(n_obs),
                "sector": sector,
            },
            "explain_json": json.dumps(
                {
                    "tradability": {"expected_ret_net": float(expected_ret_net), "expected_dd": 0.05},
                    "model_intent": {
                        "expected_ret_net": float(expected_ret_net),
                        "confidence": 0.80,
                        "model_family": "ridge",
                    },
                    "sector": sector,
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        }


if __name__ == "__main__":
    unittest.main()
