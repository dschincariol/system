"""Online SGD-based regression model with incremental updates."""

from __future__ import annotations

import math
import os
from typing import Any, Mapping, Sequence

import numpy as np
from sklearn.linear_model import SGDRegressor
from sklearn.preprocessing import StandardScaler

from engine.strategy.models.base_model import BaseModel, _clip01, _safe_float

ONLINE_MODEL_MAX_ABS_PREDICTION = max(
    1.0,
    float(os.environ.get("ONLINE_MODEL_MAX_ABS_PREDICTION", os.environ.get("INFERENCE_MAX_ABS_PREDICTION", "20.0"))),
)
_LIVE_BROKERS = {"alpaca", "ibkr", "interactive_brokers"}
_SAFE_BROKERS = {"", "unknown", "sim", "paper", "sandbox", "test", "mock"}


def _clip_prediction(value: Any) -> float:
    bounded = _safe_float(value, 0.0)
    limit = float(ONLINE_MODEL_MAX_ABS_PREDICTION)
    return float(max(-limit, min(limit, bounded)))


def _live_context_active() -> bool:
    modes = {
        str(os.environ.get("ENGINE_MODE", "") or "").strip().lower(),
        str(os.environ.get("EXECUTION_MODE", "") or "").strip().lower(),
    }
    if "live" in modes:
        return True
    broker = str(os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or os.environ.get("LIVE_BROKER") or "").strip().lower()
    if broker in _LIVE_BROKERS:
        return True
    return bool(broker and broker not in _SAFE_BROKERS)


class OnlineModel(BaseModel):
    """Incremental regressor that updates in-place via ``partial_fit``."""

    supports_online_update = True

    def __init__(
        self,
        model: SGDRegressor | None = None,
        *,
        model_name: str = "online_model",
        feature_ids: Sequence[Any] | None = None,
        feature_set_tag: str | None = None,
        default_confidence: float = 0.5,
        metadata: Mapping[str, Any] | None = None,
        scaler: StandardScaler | None = None,
        confidence_half_life_updates: int = 20,
        error_ema_alpha: float = 0.2,
        n_updates: int = 0,
        error_ema: float = 1.0,
    ) -> None:
        self.model = model or SGDRegressor(
            loss="squared_error",
            penalty="l2",
            alpha=0.0001,
            learning_rate="invscaling",
            eta0=0.01,
            power_t=0.25,
            average=True,
            random_state=42,
        )
        self.scaler = scaler or StandardScaler()
        self.confidence_half_life_updates = max(1, int(confidence_half_life_updates))
        self.error_ema_alpha = float(max(0.01, min(1.0, error_ema_alpha)))
        self.n_updates = max(0, int(n_updates))
        self.error_ema = max(0.0, _safe_float(error_ema, 1.0))
        self._scaler_fitted = bool(self.n_updates > 0)
        self._model_fitted = bool(self.n_updates > 0)
        merged_metadata = dict(metadata or {})
        merged_metadata.setdefault("model_backend", "sklearn_sgd")
        super().__init__(
            model_name=model_name,
            model_kind="online",
            feature_ids=feature_ids,
            feature_set_tag=feature_set_tag,
            default_confidence=default_confidence,
            metadata=merged_metadata,
        )

    def _transform(self, vector: np.ndarray) -> np.ndarray:
        matrix = vector.reshape(1, -1).astype(np.float32, copy=False)
        if not self._scaler_fitted:
            return matrix
        return self.scaler.transform(matrix).astype(np.float32, copy=False)

    def _predict_vector(self, vector: np.ndarray, *, context: Mapping[str, Any] | None = None) -> float:
        del context
        if not self._model_fitted:
            if _live_context_active():
                raise RuntimeError("live_online_model_unfitted_dummy_prediction")
            return 0.0
        matrix = self._transform(vector)
        raw = self.model.predict(matrix)
        return _clip_prediction(np.asarray(raw, dtype=float).reshape(-1)[0])

    def _predict_confidence(
        self,
        vector: np.ndarray,
        *,
        prediction: float,
        context: Mapping[str, Any] | None = None,
    ) -> float:
        del vector, prediction, context
        update_factor = 1.0 - math.exp(-float(self.n_updates) / float(self.confidence_half_life_updates))
        error_factor = 1.0 / (1.0 + max(0.0, float(self.error_ema)))
        confidence = (0.2 * float(self.default_confidence)) + (0.8 * update_factor * error_factor)
        if not self._model_fitted:
            confidence *= 0.5
        return _clip01(confidence, default=self.default_confidence)

    def update(self, features: Any, outcome: Any) -> dict[str, float]:
        vector, context = self._coerce_vector(features)
        target = _safe_float(outcome, 0.0)
        prior_prediction = self._predict_vector(vector, context=context) if self._model_fitted else 0.0

        raw_matrix = vector.reshape(1, -1).astype(np.float32, copy=False)
        self.scaler.partial_fit(raw_matrix)
        self._scaler_fitted = True
        matrix = self.scaler.transform(raw_matrix).astype(np.float32, copy=False)
        self.model.partial_fit(matrix, np.asarray([target], dtype=np.float32))
        self._model_fitted = True

        self.n_updates += 1
        abs_error = abs(float(target) - float(prior_prediction))
        if int(self.n_updates) <= 1:
            self.error_ema = float(abs_error)
        else:
            self.error_ema = ((1.0 - self.error_ema_alpha) * float(self.error_ema)) + (
                self.error_ema_alpha * float(abs_error)
            )

        prediction = self._predict_vector(vector, context=context)
        confidence = self._predict_confidence(vector, prediction=prediction, context=context)
        return {
            "prediction": float(prediction),
            "confidence": float(confidence),
        }

    def registration_metadata(self) -> dict[str, Any]:
        metadata = super().registration_metadata()
        metadata.setdefault("model_backend", "sklearn_sgd")
        metadata["online_updates"] = int(self.n_updates)
        metadata["online_error_ema"] = float(self.error_ema)
        return metadata


__all__ = [
    "OnlineModel",
]
