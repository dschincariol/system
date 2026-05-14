"""Public shim for model catalog and prediction-tracking registry helpers."""

from engine.model_registry import (
    DEFAULT_MODEL_REGISTRY,
    ModelRegistry,
    get_best_model,
    get_model_spec,
    get_stage_latest,
    list_models,
    load_model,
    register_model,
)

__all__ = [
    "ModelRegistry",
    "DEFAULT_MODEL_REGISTRY",
    "register_model",
    "load_model",
    "list_models",
    "get_best_model",
    "get_stage_latest",
    "get_model_spec",
]
