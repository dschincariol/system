"""Public shim for the execution decision gate."""

from engine.decision_engine import (
    DEFAULT_ENGINE,
    DecisionEngine,
    evaluate_decision,
    should_execute,
)

__all__ = [
    "DecisionEngine",
    "DEFAULT_ENGINE",
    "evaluate_decision",
    "should_execute",
]
