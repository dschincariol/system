"""Public shim for prediction feedback and model metrics."""

from engine.metrics_engine import (
    DEFAULT_ENGINE,
    MetricsEngine,
    compute_model_performance_stats,
    get_model_performance_stats,
    init_metrics_db,
    list_prediction_feedback,
    refresh_feedback_loop,
    refresh_prediction_feedback,
)

__all__ = [
    "MetricsEngine",
    "DEFAULT_ENGINE",
    "init_metrics_db",
    "refresh_prediction_feedback",
    "compute_model_performance_stats",
    "refresh_feedback_loop",
    "list_prediction_feedback",
    "get_model_performance_stats",
]
