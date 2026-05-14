"""Stacked ensemble utilities for production prediction blending."""

from engine.strategy.ensemble.blender import (
    BlendResult,
    EnsembleBlender,
    ensemble_mode,
    load_latest_weights,
    persist_weights,
)
from engine.strategy.ensemble.oos_store import (
    ensure_schema as ensure_oos_schema,
    read_oos_predictions,
    upsert_oos_prediction,
    upsert_oos_predictions,
)
from engine.strategy.ensemble.ridge_meta import RidgeStackEnsemble

__all__ = [
    "BlendResult",
    "EnsembleBlender",
    "RidgeStackEnsemble",
    "ensemble_mode",
    "ensure_oos_schema",
    "load_latest_weights",
    "persist_weights",
    "read_oos_predictions",
    "upsert_oos_prediction",
    "upsert_oos_predictions",
]
