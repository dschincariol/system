"""Public shim for the ensemble prediction engine."""

from engine.ensemble_engine import (
    DEFAULT_ENGINE,
    EnsembleEngine,
    combine_predictions,
    estimate_model_weight,
    resolve_historical_accuracy,
    resolve_recent_performance,
)

__all__ = [
    "EnsembleEngine",
    "DEFAULT_ENGINE",
    "combine_predictions",
    "estimate_model_weight",
    "resolve_historical_accuracy",
    "resolve_recent_performance",
]
