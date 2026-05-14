"""Public shim for the ensemble decision layer."""

from engine.ensemble_model import DEFAULT_MODEL, EnsembleModel, combine

__all__ = [
    "EnsembleModel",
    "DEFAULT_MODEL",
    "combine",
]
