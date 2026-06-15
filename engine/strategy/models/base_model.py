"""Shared model interface for batch and online inference artifacts."""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
from engine.artifacts.serialization import dump_pickle_artifact, dumps_pickle_artifact
from engine.artifacts.store import LocalArtifactStore


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _clip01(value: Any, default: float = 0.5) -> float:
    return float(max(0.0, min(1.0, _safe_float(value, default))))


def _normalize_feature_ids(feature_ids: Sequence[Any] | None) -> list[str]:
    return [str(feature_id).strip() for feature_id in (feature_ids or []) if str(feature_id).strip()]


def _feature_store_defaults() -> tuple[list[str], str]:
    try:
        from engine.data import feature_store

        return (
            list(getattr(feature_store, "FEATURE_NAMES", ()) or ()),
            str(getattr(feature_store, "FEATURE_SET_TAG", "") or ""),
        )
    except Exception:
        return [], ""


class BaseModel(ABC):
    """Minimal serving contract shared by ensemble-capable model artifacts."""

    supports_online_update = False

    def __init__(
        self,
        *,
        model_name: str | None = None,
        model_kind: str = "base",
        feature_ids: Sequence[Any] | None = None,
        feature_set_tag: str | None = None,
        default_confidence: float = 0.5,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        default_feature_ids, default_feature_set_tag = _feature_store_defaults()
        resolved_feature_ids = _normalize_feature_ids(feature_ids) or list(default_feature_ids)
        resolved_feature_set_tag = str(feature_set_tag or default_feature_set_tag).strip() or default_feature_set_tag
        self.model_name = str(model_name or self.__class__.__name__).strip() or self.__class__.__name__
        self.model_kind = str(model_kind or "base").strip() or "base"
        self.feature_ids = list(resolved_feature_ids)
        self.feature_set_tag = str(resolved_feature_set_tag)
        self.default_confidence = _clip01(default_confidence, default=0.5)
        self.metadata = dict(metadata or {})

    def _maybe_update_feature_contract(
        self,
        *,
        feature_ids: Sequence[Any] | None = None,
        feature_set_tag: Any = None,
    ) -> None:
        resolved_feature_ids = _normalize_feature_ids(feature_ids)
        if resolved_feature_ids and not self.feature_ids:
            self.feature_ids = list(resolved_feature_ids)
        resolved_feature_set_tag = str(feature_set_tag or "").strip()
        if resolved_feature_set_tag and not self.feature_set_tag:
            self.feature_set_tag = str(resolved_feature_set_tag)

    def _get_feature_store_snapshot(self, symbol: str) -> Mapping[str, Any]:
        from engine.data import feature_store

        snapshot = feature_store.get_features(str(symbol).upper())
        if not isinstance(snapshot, Mapping):
            raise TypeError("feature_store_snapshot_invalid")
        self._maybe_update_feature_contract(
            feature_ids=snapshot.get("feature_names"),
            feature_set_tag=snapshot.get("feature_set_tag"),
        )
        return snapshot

    def _coerce_vector(self, features: Any) -> tuple[np.ndarray, dict[str, Any]]:
        if isinstance(features, str):
            return self._coerce_vector(self._get_feature_store_snapshot(features))

        if isinstance(features, Mapping):
            context = dict(features)
            symbol = str(context.get("symbol") or "").strip().upper()
            feature_names = context.get("feature_names")
            feature_set_tag = context.get("feature_set_tag")
            self._maybe_update_feature_contract(feature_ids=feature_names, feature_set_tag=feature_set_tag)

            if symbol and not context.get("vector") and not context.get("features"):
                direct_feature_ids = self.feature_ids or _normalize_feature_ids(feature_names)
                if direct_feature_ids and any(feature_id in context for feature_id in direct_feature_ids):
                    vector = np.asarray(
                        [_safe_float(context.get(feature_id), 0.0) for feature_id in direct_feature_ids],
                        dtype=np.float32,
                    )
                    return vector, context
                return self._coerce_vector(self._get_feature_store_snapshot(symbol))

            raw_vector = context.get("vector")
            if raw_vector is not None:
                return self._coerce_vector(raw_vector)

            feature_map = context.get("features")
            if isinstance(feature_map, Mapping):
                resolved_feature_ids = self.feature_ids or _normalize_feature_ids(feature_names) or list(feature_map.keys())
                self._maybe_update_feature_contract(feature_ids=resolved_feature_ids, feature_set_tag=feature_set_tag)
                vector = np.asarray(
                    [_safe_float(feature_map.get(feature_id), 0.0) for feature_id in resolved_feature_ids],
                    dtype=np.float32,
                )
                return vector, context

            direct_feature_ids = self.feature_ids or _normalize_feature_ids(feature_names)
            if direct_feature_ids and any(feature_id in context for feature_id in direct_feature_ids):
                vector = np.asarray(
                    [_safe_float(context.get(feature_id), 0.0) for feature_id in direct_feature_ids],
                    dtype=np.float32,
                )
                return vector, context

        if isinstance(features, np.ndarray):
            array = np.asarray(features, dtype=np.float32)
        elif isinstance(features, Sequence) and not isinstance(features, (str, bytes, bytearray)):
            array = np.asarray(list(features), dtype=np.float32)
        else:
            raise TypeError(f"unsupported_feature_payload:{type(features).__name__}")

        if array.ndim == 2:
            if int(array.shape[0]) != 1:
                raise ValueError("batch_features_not_supported")
            array = array.reshape(-1)
        elif array.ndim != 1:
            raise ValueError("feature_vector_invalid_shape")

        if not self.feature_ids and int(array.size) > 0:
            self.feature_ids = [f"f_{idx}" for idx in range(int(array.size))]
        if self.feature_ids and int(array.size) != int(len(self.feature_ids)):
            raise ValueError(f"feature_count_mismatch:{int(array.size)}:{int(len(self.feature_ids))}")

        return array.astype(np.float32, copy=False), {}

    @abstractmethod
    def _predict_vector(self, vector: np.ndarray, *, context: Mapping[str, Any] | None = None) -> float:
        raise NotImplementedError

    def _predict_confidence(
        self,
        vector: np.ndarray,
        *,
        prediction: float,
        context: Mapping[str, Any] | None = None,
    ) -> float:
        return float(self.default_confidence)

    def predict(self, features: Any) -> dict[str, float]:
        vector, context = self._coerce_vector(features)
        prediction = _safe_float(self._predict_vector(vector, context=context), 0.0)
        confidence = _clip01(
            self._predict_confidence(vector, prediction=prediction, context=context),
            default=self.default_confidence,
        )
        return {
            "prediction": float(prediction),
            "confidence": float(confidence),
        }

    def predict_with_confidence(self, features: Any) -> dict[str, float]:
        return self.predict(features)

    def update(self, features: Any, outcome: Any) -> dict[str, float] | None:
        del features, outcome
        return None

    def save_artifact(self, artifact_uri: str | Path) -> Path:
        artifact_path = Path(artifact_uri).expanduser()
        return dump_pickle_artifact(
            self,
            artifact_path,
            prefer_joblib=artifact_path.suffix.lower() == ".joblib",
        )

    def registration_metadata(self) -> dict[str, Any]:
        metadata = dict(self.metadata)
        metadata.setdefault("feature_ids", list(self.feature_ids))
        metadata.setdefault("feature_set_tag", str(self.feature_set_tag))
        metadata.setdefault("model_interface", "BaseModel")
        metadata.setdefault("model_class", f"{self.__class__.__module__}.{self.__class__.__name__}")
        metadata.setdefault("supports_online_update", bool(self.supports_online_update))
        return metadata

    def register(
        self,
        *,
        symbol: str,
        version: str,
        artifact_uri: str | Path,
        model_name: str | None = None,
        performance_metrics: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
        training_data_window: Mapping[str, Any] | None = None,
        is_active: bool = True,
        status: str = "registered",
    ) -> dict[str, Any] | None:
        from engine.model_registry import register_model

        merged_metadata = self.registration_metadata()
        merged_metadata.update(dict(metadata or {}))
        intrinsic_metrics = dict(getattr(self, "training_metrics", {}) or {})
        merged_metrics = dict(intrinsic_metrics)
        merged_metrics.update(dict(performance_metrics or {}))
        if "confidence" not in merged_metrics and float(self.default_confidence) > 0.0:
            merged_metrics["confidence"] = float(self.default_confidence)
        model_name_s = str(model_name or self.model_name)
        symbol_s = str(symbol).upper()
        artifact_uri_text = str(artifact_uri or "").strip()
        prefer_joblib = Path(artifact_uri_text).suffix.lower() == ".joblib"
        register_artifact_uri = artifact_uri_text
        if artifact_uri_text and not artifact_uri_text.startswith("model:") and "://" not in artifact_uri_text:
            artifact_path = self.save_artifact(artifact_uri_text)
            register_artifact_uri = str(artifact_path)
        else:
            alias = (
                artifact_uri_text
                if artifact_uri_text.startswith("model:")
                else f"model:{model_name_s}:{symbol_s}:current"
            )
            payload = dumps_pickle_artifact(self, prefer_joblib=prefer_joblib)
            ref = LocalArtifactStore().put(
                payload,
                content_type=("application/vnd.joblib" if prefer_joblib else "application/x-python-pickle"),
                kind="model",
                alias=alias,
                metadata={
                    **merged_metadata,
                    "model_name": model_name_s,
                    "symbol": symbol_s,
                    "version": str(version),
                },
            )
            register_artifact_uri = str(alias)
            merged_metadata["artifact_manifest"] = {
                "alias": str(alias),
                "sha256": ref.sha256,
                "size_bytes": int(ref.size),
                "content_type": ref.content_type,
                "kind": ref.kind,
            }
        return register_model(
            symbol=symbol_s,
            model_name=model_name_s,
            model_kind=str(self.model_kind),
            version=str(version),
            artifact_uri=str(register_artifact_uri),
            metadata=merged_metadata,
            performance_metrics=merged_metrics,
            training_data_window=(dict(training_data_window or {}) if training_data_window is not None else None),
            is_active=bool(is_active),
            status=str(status or "registered"),
        )


__all__ = [
    "BaseModel",
]
