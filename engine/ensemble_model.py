"""DB-aware ensemble decision layer."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence

from engine.ensemble_engine import DEFAULT_METHOD, combine_predictions


class EnsembleModel:
    """Combine registered model predictions into a single decision output.

    The initial strategy is weighted averaging. The object keeps a meta-model
    hook so the caller contract can stay stable when a learned combiner is
    introduced later.
    """

    def __init__(
        self,
        *,
        default_method: str = DEFAULT_METHOD,
        meta_model: Any | None = None,
    ) -> None:
        self.default_method = str(default_method or DEFAULT_METHOD).strip() or DEFAULT_METHOD
        self.meta_model = meta_model

    def combine(
        self,
        predictions: Sequence[Mapping[str, Any]],
        *,
        method: str | None = None,
    ) -> Dict[str, Any]:
        resolved_method = str(method or self.default_method or DEFAULT_METHOD).strip().lower() or DEFAULT_METHOD
        fallback_reason = None
        if resolved_method == "meta_model":
            if self.meta_model is None:
                fallback_reason = "meta_model_unconfigured"
                resolved_method = "weighted_average"
            else:
                fallback_reason = "meta_model_not_implemented"
                resolved_method = "weighted_average"

        combined = combine_predictions(predictions, method=resolved_method)
        if fallback_reason:
            combined["fallback_reason"] = str(fallback_reason)
        combined["combiner"] = "ensemble_model"
        return combined


DEFAULT_MODEL = EnsembleModel()


def combine(
    predictions: Sequence[Mapping[str, Any]],
    *,
    method: str | None = None,
) -> Dict[str, Any]:
    """Combine prediction rows with the shared default ensemble model instance."""
    return DEFAULT_MODEL.combine(predictions, method=method)


__all__ = [
    "EnsembleModel",
    "DEFAULT_MODEL",
    "combine",
]
