from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class EnsembleEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine_ensemble, self.public_ensemble = _reload_modules(
            "engine.ensemble_engine",
            "ensemble_engine",
        )

    def test_estimate_model_weight_blends_historical_and_recent_signals(self) -> None:
        strong = {
            "performance_metrics": {"accuracy": 0.88},
            "metadata": {"recent_performance": 0.81},
        }
        weak = {
            "performance_metrics": {"accuracy": 0.54},
            "metadata": {"recent_performance": 0.33},
        }

        strong_weight = self.public_ensemble.estimate_model_weight(strong)
        weak_weight = self.public_ensemble.estimate_model_weight(weak)

        self.assertGreater(float(strong_weight), float(weak_weight))
        self.assertAlmostEqual(float(strong_weight), 0.852, places=6)
        self.assertAlmostEqual(float(weak_weight), 0.456, places=6)

    def test_combine_predictions_weighted_average_returns_expected_result(self) -> None:
        result = self.public_ensemble.combine_predictions(
            [
                {
                    "model_name": "alpha",
                    "prediction": 1.0,
                    "confidence": 0.90,
                    "performance_metrics": {"accuracy": 0.80},
                    "metadata": {"recent_performance": 0.70},
                },
                {
                    "model_name": "beta",
                    "prediction": -0.5,
                    "confidence": 0.60,
                    "performance_metrics": {"accuracy": 0.50},
                    "metadata": {"recent_performance": 0.30},
                },
            ],
            method="weighted_average",
        )

        self.assertEqual(str(result["method"]), "weighted_average")
        self.assertEqual(int(result["ensemble_size"]), 2)
        self.assertAlmostEqual(float(result["final_prediction"]), 0.4661016949, places=6)
        self.assertAlmostEqual(float(result["aggregated_confidence"]), 0.6164406779, places=6)

    def test_combine_predictions_voting_respects_weighted_direction(self) -> None:
        result = self.public_ensemble.combine_predictions(
            [
                {
                    "model_name": "alpha",
                    "prediction": 0.4,
                    "confidence": 0.82,
                    "performance_metrics": {"accuracy": 0.88},
                    "metadata": {"recent_performance": 0.78},
                },
                {
                    "model_name": "beta",
                    "prediction": 0.3,
                    "confidence": 0.74,
                    "performance_metrics": {"accuracy": 0.75},
                    "metadata": {"recent_performance": 0.70},
                },
                {
                    "model_name": "gamma",
                    "prediction": -1.2,
                    "confidence": 0.63,
                    "performance_metrics": {"accuracy": 0.45},
                    "metadata": {"recent_performance": 0.28},
                },
            ],
            method="voting",
        )

        self.assertEqual(str(result["method"]), "voting")
        self.assertEqual(int(result["ensemble_size"]), 3)
        self.assertGreater(float(result["final_prediction"]), 0.0)
        self.assertGreater(float(result["aggregated_confidence"]), 0.0)
        self.assertLessEqual(float(result["aggregated_confidence"]), 1.0)


if __name__ == "__main__":
    unittest.main()
