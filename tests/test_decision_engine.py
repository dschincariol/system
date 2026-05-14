from __future__ import annotations

import importlib
import os
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


class DecisionEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.prev_env = {
            "DECISION_ENGINE_ENABLED": os.environ.get("DECISION_ENGINE_ENABLED"),
            "DECISION_MIN_CONFIDENCE": os.environ.get("DECISION_MIN_CONFIDENCE"),
            "DECISION_MIN_ABS_PREDICTION": os.environ.get("DECISION_MIN_ABS_PREDICTION"),
            "DECISION_MAX_OPEN_POSITIONS": os.environ.get("DECISION_MAX_OPEN_POSITIONS"),
            "DECISION_MAX_MARKET_STRESS": os.environ.get("DECISION_MAX_MARKET_STRESS"),
        }
        os.environ["DECISION_ENGINE_ENABLED"] = "1"
        os.environ["DECISION_MIN_CONFIDENCE"] = "0.70"
        os.environ["DECISION_MIN_ABS_PREDICTION"] = "0.80"
        os.environ["DECISION_MAX_OPEN_POSITIONS"] = "2"
        os.environ["DECISION_MAX_MARKET_STRESS"] = "0.75"
        self.engine_module, self.public_module = _reload_modules(
            "engine.decision_engine",
            "decision_engine",
        )

    def tearDown(self) -> None:
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[str(key)] = str(value)

    def test_should_execute_blocks_low_confidence_signal(self) -> None:
        allowed = self.public_module.should_execute(
            1.25,
            0.55,
            {"action": "OPEN", "from_side": "FLAT", "to_side": "LONG", "target_weight": 0.15},
        )
        self.assertFalse(bool(allowed))

    def test_evaluate_blocks_when_open_position_limit_is_reached(self) -> None:
        decision = self.public_module.evaluate_decision(
            1.10,
            0.92,
            {
                "action": "OPEN",
                "from_side": "FLAT",
                "to_side": "LONG",
                "target_weight": 0.10,
                "open_positions": 2,
                "symbol_open_positions": 0,
            },
        )
        self.assertFalse(bool(decision.get("execute")))
        self.assertIn("open_position_limit", list(decision.get("reasons") or []))

    def test_evaluate_passes_through_non_risk_increasing_actions(self) -> None:
        decision = self.public_module.evaluate_decision(
            0.10,
            0.10,
            {
                "action": "CLOSE",
                "from_side": "LONG",
                "to_side": "FLAT",
                "current_weight": 0.20,
                "target_weight": 0.0,
            },
        )
        self.assertTrue(bool(decision.get("execute")))
        self.assertEqual(str(decision.get("reason")), "pass_through_non_risk_increasing")

    def test_evaluate_blocks_market_stress_breach(self) -> None:
        decision = self.engine_module.DEFAULT_ENGINE.evaluate(
            prediction=1.40,
            confidence=0.88,
            risk={
                "action": "OPEN",
                "from_side": "FLAT",
                "to_side": "LONG",
                "target_weight": 0.12,
                "market_stress": 0.90,
            },
        )
        self.assertFalse(bool(decision.get("execute")))
        self.assertIn("market_stress_above_limit", list(decision.get("reasons") or []))


if __name__ == "__main__":
    unittest.main()
