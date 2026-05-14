"""Gradient-boosted model wrapper compatible with LightGBM/XGBoost-style predictors."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np

from engine.strategy.models.base_model import BaseModel, _clip01, _safe_float

_CONFIDENCE_KEYS = (
    "confidence",
    "quality_score",
    "directional_acc",
    "directional_accuracy",
    "accuracy",
    "win_rate",
    "auc",
    "f1",
    "r2",
)
_ERROR_KEYS = ("rmse", "mae", "mse", "loss")


def _metric_confidence(metrics: Mapping[str, Any], default: float) -> float:
    for key in _CONFIDENCE_KEYS:
        if key in metrics:
            return _clip01(metrics.get(key), default=default)
    for key in _ERROR_KEYS:
        if key in metrics:
            return _clip01(1.0 / (1.0 + max(0.0, _safe_float(metrics.get(key), 0.0))), default=default)
    return float(default)


class GBMModel(BaseModel):
    """Wrapper for LightGBM/XGBoost/native regressor artifacts with a shared interface."""

    supports_online_update = False

    def __init__(
        self,
        model: Any,
        *,
        model_name: str = "gbm_model",
        feature_ids: Sequence[Any] | None = None,
        feature_set_tag: str | None = None,
        backend: str | None = None,
        default_confidence: float = 0.6,
        training_metrics: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self.model = model
        self.backend = str(backend or self._infer_backend(model)).strip() or "gbm"
        self.training_metrics = dict(training_metrics or {})
        merged_metadata = dict(metadata or {})
        merged_metadata.setdefault("model_backend", str(self.backend))
        super().__init__(
            model_name=model_name,
            model_kind="gbm",
            feature_ids=feature_ids,
            feature_set_tag=feature_set_tag,
            default_confidence=default_confidence,
            metadata=merged_metadata,
        )

    @staticmethod
    def _infer_backend(model: Any) -> str:
        module_name = str(getattr(type(model), "__module__", "") or "").lower()
        class_name = str(getattr(type(model), "__name__", "") or "").lower()
        if "xgboost" in module_name or class_name.startswith("xgb"):
            return "xgboost"
        if "lightgbm" in module_name:
            return "lightgbm"
        if "gradientboost" in class_name:
            return "sklearn_gbm"
        return "gbm"

    def _predict_vector(self, vector: np.ndarray, *, context: Mapping[str, Any] | None = None) -> float:
        del context
        matrix = vector.reshape(1, -1).astype(np.float32, copy=False)

        if self.backend == "xgboost" and hasattr(self.model, "inplace_predict"):
            raw = self.model.inplace_predict(matrix)
            return _safe_float(np.asarray(raw, dtype=float).reshape(-1)[0], 0.0)

        if self.backend == "xgboost" and str(getattr(type(self.model), "__name__", "")).lower() == "booster":
            import xgboost as xgb  # pragma: no cover - optional dependency path

            raw = self.model.predict(xgb.DMatrix(matrix, feature_names=(self.feature_ids or None)))
            return _safe_float(np.asarray(raw, dtype=float).reshape(-1)[0], 0.0)

        raw = self.model.predict(matrix)
        return _safe_float(np.asarray(raw, dtype=float).reshape(-1)[0], 0.0)

    def _predict_confidence(
        self,
        vector: np.ndarray,
        *,
        prediction: float,
        context: Mapping[str, Any] | None = None,
    ) -> float:
        del prediction, context
        if hasattr(self.model, "predict_proba") and callable(getattr(self.model, "predict_proba")):
            try:
                matrix = vector.reshape(1, -1).astype(np.float32, copy=False)
                proba = np.asarray(self.model.predict_proba(matrix), dtype=float)
                if proba.ndim >= 2 and int(proba.shape[0]) > 0:
                    return _clip01(np.max(proba[0]), default=self.default_confidence)
            except Exception:
                pass  # no-op-guard: allow confidence fallback to training metrics
        return _metric_confidence(self.training_metrics, self.default_confidence)

    def registration_metadata(self) -> dict[str, Any]:
        metadata = super().registration_metadata()
        metadata.setdefault("model_backend", str(self.backend))
        return metadata


__all__ = [
    "GBMModel",
]
