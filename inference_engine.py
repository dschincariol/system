"""Public shim for the real-time inference engine."""

from engine.inference_engine import (
    DEFAULT_ENGINE,
    InferenceEngine,
    batch_predict,
    batch_predict_async,
    predict,
    predict_async,
)

__all__ = [
    "InferenceEngine",
    "DEFAULT_ENGINE",
    "predict",
    "predict_async",
    "batch_predict",
    "batch_predict_async",
]
