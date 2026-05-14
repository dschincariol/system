from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from capital_allocator import CapitalAllocator


class CapitalAllocatorTests(unittest.TestCase):
    def test_higher_confidence_receives_higher_allocation(self) -> None:
        allocator = CapitalAllocator(max_model_allocation=0.70)
        result = allocator.allocate(
            [
                {
                    "model_name": "high_conf",
                    "score": 0.8,
                    "capital_score": 0.6,
                    "net_pnl": 40.0,
                    "performance_score": 0.7,
                    "effective_stability_score": 0.8,
                    "raw_weight": 1.0,
                    "avg_confidence": 0.9,
                    "max_drawdown": 15.0,
                },
                {
                    "model_name": "low_conf",
                    "score": 0.8,
                    "capital_score": 0.6,
                    "net_pnl": 40.0,
                    "performance_score": 0.7,
                    "effective_stability_score": 0.8,
                    "raw_weight": 1.0,
                    "avg_confidence": 0.3,
                    "max_drawdown": 15.0,
                },
            ],
            {
                "ensemble_confidence": 0.7,
                "models": {
                    "high_conf": 0.9,
                    "low_conf": 0.3,
                },
            },
            {
                "strategy": "proportional",
                "max_model_allocation": 0.70,
                "models": {
                    "high_conf": {"max_drawdown": 15.0, "effective_stability_score": 0.8},
                    "low_conf": {"max_drawdown": 15.0, "effective_stability_score": 0.8},
                },
            },
        )

        allocations = dict(result.get("allocations") or {})
        self.assertGreater(float(allocations["high_conf"]), float(allocations["low_conf"]))
        self.assertAlmostEqual(sum(float(v) for v in allocations.values()), 1.0, places=6)

    def test_underperforming_model_is_penalized(self) -> None:
        allocator = CapitalAllocator(max_model_allocation=0.70)
        result = allocator.allocate(
            [
                {
                    "model_name": "healthy",
                    "score": 0.85,
                    "capital_score": 0.7,
                    "net_pnl": 90.0,
                    "performance_score": 0.8,
                    "effective_stability_score": 0.85,
                    "raw_weight": 1.0,
                    "avg_confidence": 0.7,
                    "max_drawdown": 12.0,
                },
                {
                    "model_name": "fragile",
                    "score": 0.88,
                    "capital_score": -0.4,
                    "net_pnl": -60.0,
                    "performance_score": 0.45,
                    "effective_stability_score": 0.35,
                    "raw_weight": 1.0,
                    "avg_confidence": 0.7,
                    "max_drawdown": 180.0,
                },
            ],
            {
                "ensemble_confidence": 0.7,
                "models": {
                    "healthy": 0.7,
                    "fragile": 0.7,
                },
            },
            {
                "strategy": "proportional",
                "max_model_allocation": 0.70,
                "models": {
                    "healthy": {
                        "max_drawdown": 12.0,
                        "effective_stability_score": 0.85,
                        "model_risk_limit_multiplier": 1.0,
                    },
                    "fragile": {
                        "max_drawdown": 180.0,
                        "effective_stability_score": 0.35,
                        "model_risk_limit_multiplier": 0.45,
                    },
                },
            },
        )

        allocations = dict(result.get("allocations") or {})
        details = dict(result.get("details") or {})
        self.assertGreater(float(allocations["healthy"]), float(allocations["fragile"]))
        self.assertLess(
            float((details.get("fragile") or {}).get("underperformance_penalty") or 1.0),
            float((details.get("healthy") or {}).get("underperformance_penalty") or 1.0),
        )

    def test_risk_cap_limits_dominant_model(self) -> None:
        allocator = CapitalAllocator(max_model_allocation=0.55)
        result = allocator.allocate(
            [
                {
                    "model_name": "dominant",
                    "score": 1.6,
                    "capital_score": 1.0,
                    "net_pnl": 180.0,
                    "performance_score": 0.95,
                    "effective_stability_score": 0.9,
                    "raw_weight": 10.0,
                    "avg_confidence": 0.95,
                    "max_drawdown": 10.0,
                },
                {
                    "model_name": "backup",
                    "score": 0.2,
                    "capital_score": 0.1,
                    "net_pnl": 10.0,
                    "performance_score": 0.55,
                    "effective_stability_score": 0.7,
                    "raw_weight": 0.5,
                    "avg_confidence": 0.55,
                    "max_drawdown": 15.0,
                },
            ],
            {
                "ensemble_confidence": 0.85,
                "models": {
                    "dominant": 0.95,
                    "backup": 0.55,
                },
            },
            {
                "strategy": "winner_take_most",
                "max_model_allocation": 0.55,
                "models": {
                    "dominant": {"max_drawdown": 10.0, "effective_stability_score": 0.9},
                    "backup": {"max_drawdown": 15.0, "effective_stability_score": 0.7},
                },
            },
        )

        allocations = dict(result.get("allocations") or {})
        self.assertLessEqual(float(allocations["dominant"]), 0.55 + 1e-6)
        self.assertAlmostEqual(sum(float(v) for v in allocations.values()), 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
