"""First-class LightGBM tabular regressor family.

The family intentionally resolves its feature order through
``feature_registry.expected_columns`` during both training and serving so the
model artifact remains bound to the same schema contract used by live feature
construction.
"""

from __future__ import annotations
import inspect
import logging

import json
import math
import os
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from engine.artifacts.serialization import (
    dump_pickle_artifact,
    dumps_pickle_artifact,
    load_pickle_artifact,
    loads_pickle_artifact,
)
from engine.artifacts.store import LocalArtifactStore
from engine.model_registry import register_model, register_model_family
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db
from engine.runtime.workload_profiles import assert_offline_work_allowed, model_family_n_jobs
from engine.strategy import feature_registry
from engine.strategy.feature_registry import build_feature_snapshot, feature_set_tag_from_ids
from engine.strategy.era_boost import (
    era_boost_config_from_env,
    era_labels_for,
    era_score_table,
    score_std,
    validation_degraded,
    worst_half_eras,
)
from engine.strategy.model_lifecycle import (
    load_lifecycle_plan,
    record_version_performance,
    register_model_version,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.strategy.ood import build_ood_profile, score_ood, summarize_ood_profile

FAMILY = "lgbm_regressor"
DEFAULT_MODEL_NAME = FAMILY
DEFAULT_MODEL_KIND = "lightgbm"
DEFAULT_MIN_SAMPLES = int(os.environ.get("LGBM_MIN_SAMPLES", "20"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("LGBM_LOOKBACK_DAYS", "365"))
DEFAULT_HORIZON_S = int(os.environ.get("LGBM_HORIZON_S", os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600")))
LOG = logging.getLogger(__name__)


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_lgbm_models",
            inference_entrypoint="engine.strategy.models.lgbm_regressor.LGBMRegressorModel",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


_register_family()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _expected_columns(
    feature_ids: Sequence[Any] | None = None,
    *,
    model_name: str = FAMILY,
    model_spec: Mapping[str, Any] | None = None,
) -> list[str]:
    fn = getattr(feature_registry, "expected_columns", None)
    if isinstance(feature_ids, (set, frozenset)):
        raw_ids = sorted(feature_ids, key=lambda item: str(item or "").strip())
    elif isinstance(feature_ids, Mapping):
        raw_ids = sorted(feature_ids.keys(), key=lambda item: str(item or "").strip())
    else:
        raw_ids = list(feature_ids or [])
    ids = [str(item).strip() for item in raw_ids if str(item or "").strip()]
    spec = dict(model_spec or {})
    if ids and "feature_ids" not in spec:
        spec["feature_ids"] = list(ids)
    if callable(fn):
        try:
            return [
                str(item).strip()
                for item in fn(ids or None, model_name=str(model_name), model_spec=spec or None)
                if str(item or "").strip()
            ]
        except TypeError:
            try:
                return [str(item).strip() for item in fn(str(model_name)) if str(item or "").strip()]
            except TypeError:
                return [str(item).strip() for item in fn() if str(item or "").strip()]
    return feature_registry.resolve_feature_ids(
        ids or None,
        model_name=str(model_name),
        model_spec=spec or None,
    )


def _feature_nan_alert_pct() -> float:
    return max(0.0, min(100.0, _safe_float(os.environ.get("FEATURE_NAN_ALERT_PCT"), 20.0)))


def _winsor_lower_pct() -> float:
    return max(0.0, min(100.0, _safe_float(os.environ.get("FEATURE_WINSOR_LOWER_PCT"), 0.5)))


def _winsor_upper_pct() -> float:
    return max(0.0, min(100.0, _safe_float(os.environ.get("FEATURE_WINSOR_UPPER_PCT"), 99.5)))


def _feature_schema(feature_ids: Sequence[Any], preprocessing: Mapping[str, Any] | None = None) -> dict[str, Any]:
    columns = [str(item).strip() for item in list(feature_ids or []) if str(item or "").strip()]
    schema = {
        "feature_ids": list(columns),
        "feature_set_tag": str(feature_set_tag_from_ids(list(columns))),
        "feature_count": int(len(columns)),
    }
    if isinstance(preprocessing, Mapping) and preprocessing:
        schema["preprocessing"] = dict(preprocessing)
    return schema


def _coerce_raw_feature_value(value: Any, default: float = float("nan")) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _feature_imputation_accounting(matrix: np.ndarray, columns: Sequence[str]) -> dict[str, Any]:
    arr = np.asarray(matrix, dtype=np.float32)
    cols = [str(col) for col in list(columns or [])]
    rows = int(arr.shape[0]) if arr.ndim == 2 else 0
    features: dict[str, dict[str, float | int]] = {}
    total = 0
    if arr.ndim == 2:
        mask = ~np.isfinite(arr)
        for idx, col in enumerate(cols):
            count = int(np.sum(mask[:, idx])) if idx < int(mask.shape[1]) else 0
            total += count
            features[str(col)] = {
                "nan_count": int(count),
                "nan_pct": float((100.0 * count / rows) if rows > 0 else 0.0),
            }
    return {
        "rows": int(rows),
        "feature_count": int(len(cols)),
        "total_nan_count": int(total),
        "features": features,
    }


def _emit_feature_nan_accounting(
    accounting: Mapping[str, Any],
    *,
    phase: str,
    model_name: str,
) -> None:
    rows = int(accounting.get("rows") or 0)
    total = int(accounting.get("total_nan_count") or 0)
    feature_stats = dict(accounting.get("features") or {})
    LOG.info(
        "feature_nan_accounting phase=%s model_name=%s rows=%d total_nan_count=%d",
        str(phase),
        str(model_name or ""),
        int(rows),
        int(total),
    )
    alert_pct = _feature_nan_alert_pct()
    breached = {
        str(fid): dict(stats)
        for fid, stats in feature_stats.items()
        if _safe_float((stats or {}).get("nan_pct"), 0.0) > float(alert_pct)
    }
    if not breached:
        return
    getattr(LOG, "warning")(
        "feature_nan_alert phase=%s model_name=%s alert_pct=%s breached_features=%s",
        str(phase),
        str(model_name or ""),
        float(alert_pct),
        ",".join(sorted(breached)),
    )
    log_failure(
        LOG,
        event="feature_nan_alert",
        code="FEATURE_NAN_ALERT",
        message="Feature imputation rate exceeded FEATURE_NAN_ALERT_PCT.",
        error=RuntimeError("feature_nan_alert"),
        level=logging.WARNING,
        component="engine.strategy.models.lgbm_regressor",
        extra={
            "phase": str(phase),
            "model_name": str(model_name or ""),
            "alert_pct": float(alert_pct),
            "breached_features": breached,
            "rows": int(rows),
            "total_nan_count": int(total),
        },
        persist=True,
    )


def _compute_winsor_bounds(matrix: np.ndarray, columns: Sequence[str]) -> dict[str, dict[str, float]]:
    arr = np.asarray(matrix, dtype=np.float32)
    lower_pct = _winsor_lower_pct()
    upper_pct = _winsor_upper_pct()
    if upper_pct < lower_pct:
        lower_pct, upper_pct = upper_pct, lower_pct
    bounds: dict[str, dict[str, float]] = {}
    for idx, col in enumerate([str(item) for item in list(columns or [])]):
        vals = arr[:, idx] if arr.ndim == 2 and idx < int(arr.shape[1]) else np.asarray([], dtype=np.float32)
        finite = vals[np.isfinite(vals)]
        if int(finite.size) <= 0:
            lo = hi = 0.0
        else:
            lo, hi = np.percentile(finite.astype(np.float64), [lower_pct, upper_pct])
            if not math.isfinite(float(lo)) or not math.isfinite(float(hi)):
                lo = float(np.min(finite))
                hi = float(np.max(finite))
            if float(lo) > float(hi):
                lo, hi = hi, lo
        bounds[str(col)] = {"lower": float(lo), "upper": float(hi)}
    return bounds


def _build_feature_preprocessing(matrix: np.ndarray, columns: Sequence[str]) -> dict[str, Any]:
    lower_pct = _winsor_lower_pct()
    upper_pct = _winsor_upper_pct()
    if upper_pct < lower_pct:
        lower_pct, upper_pct = upper_pct, lower_pct
    return {
        "imputation": {
            "strategy": "non_finite_to_zero",
            "nan_alert_pct": float(_feature_nan_alert_pct()),
        },
        "winsorization": {
            "enabled": True,
            "lower_pct": float(lower_pct),
            "upper_pct": float(upper_pct),
            "bounds": _compute_winsor_bounds(matrix, columns),
        },
    }


def _preprocess_feature_matrix(
    matrix: np.ndarray,
    columns: Sequence[str],
    *,
    feature_schema: Mapping[str, Any] | None = None,
    phase: str = "serve",
    model_name: str = "",
    fit_preprocessing: bool = False,
) -> tuple[np.ndarray, dict[str, Any], dict[str, Any]]:
    cols = [str(col) for col in list(columns or [])]
    arr = np.asarray(matrix, dtype=np.float32)
    if arr.ndim != 2:
        raise ValueError("feature_matrix_invalid_shape")
    if int(arr.shape[1]) != int(len(cols)):
        raise ValueError(f"feature_count_mismatch:{int(arr.shape[1])}:{int(len(cols))}")

    accounting = _feature_imputation_accounting(arr, cols)
    _emit_feature_nan_accounting(accounting, phase=str(phase), model_name=str(model_name or ""))

    preprocessing: dict[str, Any]
    if fit_preprocessing:
        preprocessing = _build_feature_preprocessing(arr, cols)
    else:
        schema = dict(feature_schema or {})
        preprocessing = dict(schema.get("preprocessing") or {}) if isinstance(schema.get("preprocessing"), Mapping) else {}

    winsor = dict(preprocessing.get("winsorization") or {}) if isinstance(preprocessing.get("winsorization"), Mapping) else {}
    bounds = dict(winsor.get("bounds") or {}) if isinstance(winsor.get("bounds"), Mapping) else {}
    if bool(winsor.get("enabled", bool(bounds))) and bounds:
        clipped = arr.astype(np.float32, copy=True)
        for idx, col in enumerate(cols):
            bound = bounds.get(col)
            if not isinstance(bound, Mapping):
                continue
            lo = _safe_float(bound.get("lower"), float("nan"))
            hi = _safe_float(bound.get("upper"), float("nan"))
            if not (math.isfinite(lo) and math.isfinite(hi)):
                continue
            if lo > hi:
                lo, hi = hi, lo
            finite_mask = np.isfinite(clipped[:, idx])
            clipped[finite_mask, idx] = np.clip(clipped[finite_mask, idx], float(lo), float(hi))
        arr = clipped

    arr = np.nan_to_num(arr.astype(np.float32, copy=False), nan=0.0, posinf=0.0, neginf=0.0)
    return arr, preprocessing, accounting


def _persisted_feature_schema(loaded: Any) -> dict[str, Any]:
    schema = getattr(loaded, "persisted_feature_schema", None)
    if not isinstance(schema, Mapping):
        metrics = getattr(loaded, "training_metrics", None)
        if isinstance(metrics, Mapping):
            schema = metrics.get("feature_schema")
    if isinstance(schema, Mapping):
        return dict(schema)
    return {}


def _emit_feature_schema_load_failure(
    *,
    loaded: Any,
    artifact_schema: Mapping[str, Any],
    current_schema: Mapping[str, Any],
    reason: str,
    error: BaseException,
) -> None:
    artifact_tag = str(artifact_schema.get("feature_set_tag") or "").strip()
    current_tag = str(current_schema.get("feature_set_tag") or "").strip()
    extra = {
        "model_name": str(getattr(loaded, "model_name", "") or ""),
        "family": str(getattr(loaded, "family", FAMILY) or FAMILY),
        "artifact_feature_set_tag": str(artifact_tag),
        "current_feature_set_tag": str(current_tag),
        "artifact_feature_ids": list(artifact_schema.get("feature_ids") or []),
        "current_feature_ids": list(current_schema.get("feature_ids") or []),
        "reason": str(reason),
    }
    LOG.error(
        "feature_schema_load_validation_failed model_name=%s family=%s artifact_feature_set_tag=%s current_feature_set_tag=%s reason=%s",
        extra["model_name"],
        extra["family"],
        artifact_tag or "<missing>",
        current_tag or "<missing>",
        str(reason),
    )
    log_failure(
        LOG,
        event="feature_schema_load_validation_failed",
        code="FEATURE_SCHEMA_LOAD_VALIDATION_FAILED",
        message=str(error),
        error=error,
        level=logging.ERROR,
        component="engine.strategy.models.lgbm_regressor",
        extra=extra,
        persist=True,
    )
    try:
        from engine.runtime.alerts import emit_runtime_alert

        emit_runtime_alert(
            event_title="Model feature schema load validation failed",
            symbol="SYSTEM",
            severity="ERROR",
            rule_id="FEATURE_SCHEMA_LOAD_VALIDATION_FAILED",
            explain=extra,
            detail={"error": str(error)},
            source="model_load_validation",
            dedupe_scope=f"{extra['family']}:{extra['model_name']}:{reason}:{artifact_tag}:{current_tag}",
        )
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)


def _assert_loaded_feature_schema_current(loaded: Any) -> None:
    loaded_feature_ids = [
        str(item).strip()
        for item in list(getattr(loaded, "feature_ids", []) or [])
        if str(item or "").strip()
    ]
    current = _expected_columns(
        loaded_feature_ids,
        model_name=getattr(loaded, "model_name", DEFAULT_MODEL_NAME),
        model_spec={"feature_ids": loaded_feature_ids},
    )
    current_schema = _feature_schema(current)
    artifact_schema = _persisted_feature_schema(loaded)
    if not artifact_schema:
        artifact_schema = {"feature_ids": list(loaded_feature_ids), "feature_set_tag": ""}
    artifact_ids = [
        str(item).strip()
        for item in list(artifact_schema.get("feature_ids") or loaded_feature_ids)
        if str(item or "").strip()
    ]
    artifact_tag = str(artifact_schema.get("feature_set_tag") or "").strip()
    current_tag = str(current_schema.get("feature_set_tag") or "").strip()

    error: ValueError | None = None
    reason = ""
    if not artifact_tag:
        reason = "missing_feature_set_tag"
        error = ValueError(
            "feature_schema_drift: artifact_feature_set_tag=<missing> "
            f"current_feature_set_tag={current_tag or '<missing>'}"
        )
    elif artifact_ids != loaded_feature_ids:
        reason = "artifact_column_list_mismatch"
        error = ValueError(
            "feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={artifact_ids} loaded_columns={loaded_feature_ids}"
        )
    elif current != loaded_feature_ids:
        reason = "registry_column_list_mismatch"
        error = ValueError(
            "feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={loaded_feature_ids} current_columns={current}"
        )
    elif artifact_tag != current_tag:
        reason = "feature_set_tag_mismatch"
        error = ValueError(
            "feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={loaded_feature_ids} current_columns={current}"
        )
    if error is not None:
        _emit_feature_schema_load_failure(
            loaded=loaded,
            artifact_schema={**dict(artifact_schema), "feature_ids": list(artifact_ids)},
            current_schema=current_schema,
            reason=reason,
            error=error,
        )
        raise error


def _current_model_artifact_alias(family: str, model_name: str, symbol: str = "*") -> str:
    return f"model:{str(family)}:{str(model_name)}:{str(symbol or '*').upper()}:current"


def _load_previous_feature_schema(family: str, model_name: str) -> dict[str, Any]:
    alias = _current_model_artifact_alias(str(family), str(model_name), "*")
    try:
        ref = LocalArtifactStore(ensure_schema=False).resolve(alias)
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)
        return {}
    if ref is None:
        return {}
    metadata = dict(getattr(ref, "metadata", {}) or {})
    schema = metadata.get("feature_schema")
    if isinstance(schema, Mapping):
        return dict(schema)
    return {}


def _bumped_training_version_id(
    *,
    family: str,
    model_name: str,
    cfg: Mapping[str, Any],
    previous_schema: Mapping[str, Any],
) -> str:
    explicit = str(cfg.get("training_version_id") or cfg.get("model_version") or "").strip()
    previous = str(previous_schema.get("training_version_id") or previous_schema.get("model_version") or "").strip()
    if explicit and explicit != previous:
        return explicit
    return version_from_ts(str(model_name), int(time.time() * 1000), prefix=str(family))


def _resolve_retrain_schema_guard(
    *,
    family: str,
    model_name: str,
    feature_ids: Sequence[Any],
    cfg: Mapping[str, Any],
    schema_builder: Any = _feature_schema,
) -> dict[str, Any]:
    current_schema = dict(schema_builder(feature_ids))
    current_tag = str(current_schema.get("feature_set_tag") or "").strip()
    previous_schema = _load_previous_feature_schema(str(family), str(model_name))
    previous_tag = str(previous_schema.get("feature_set_tag") or "").strip()
    if not previous_tag or previous_tag == current_tag:
        return {
            "feature_schema": dict(current_schema),
            "feature_set_tag": str(current_tag),
            "training_version_id": str(cfg.get("training_version_id") or cfg.get("model_version") or "").strip(),
            "feature_schema_changed": False,
        }

    training_version_id = _bumped_training_version_id(
        family=str(family),
        model_name=str(model_name),
        cfg=cfg,
        previous_schema=previous_schema,
    )
    LOG.info(
        "feature_schema_changed model_name=%s family=%s previous_feature_set_tag=%s new_feature_set_tag=%s training_version_id=%s",
        str(model_name),
        str(family),
        str(previous_tag),
        str(current_tag),
        str(training_version_id),
    )
    if str(os.environ.get("TS_ALLOW_SCHEMA_CHANGE", "") or "").strip() != "1":
        raise RuntimeError(
            "feature_schema_change_requires_ack:"
            f"model_name={str(model_name)}:"
            f"previous={str(previous_tag)}:"
            f"current={str(current_tag)}"
        )
    return {
        "feature_schema": dict(current_schema),
        "feature_set_tag": str(current_tag),
        "previous_feature_set_tag": str(previous_tag),
        "training_version_id": str(training_version_id),
        "model_version": str(training_version_id),
        "feature_schema_changed": True,
    }


def _coerce_feature_map(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        nested = row.get("features")
        if isinstance(nested, Mapping):
            return dict(nested)
        return dict(row)
    return {}


def _matrix_from_features(
    features: Any,
    columns: Sequence[str],
    *,
    feature_schema: Mapping[str, Any] | None = None,
    phase: str = "serve",
    model_name: str = "",
    fit_preprocessing: bool = False,
    return_metadata: bool = False,
) -> np.ndarray | tuple[np.ndarray, dict[str, Any], dict[str, Any]]:
    cols = [str(col) for col in list(columns or [])]
    if not cols:
        raise ValueError("feature_columns_required")

    if isinstance(features, np.ndarray):
        arr = np.asarray(features, dtype=np.float32)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        if arr.ndim != 2:
            raise ValueError("feature_matrix_invalid_shape")
        if int(arr.shape[1]) != int(len(cols)):
            raise ValueError(f"feature_count_mismatch:{int(arr.shape[1])}:{int(len(cols))}")
        result = _preprocess_feature_matrix(
            arr,
            cols,
            feature_schema=feature_schema,
            phase=phase,
            model_name=model_name,
            fit_preprocessing=fit_preprocessing,
        )
        return result if return_metadata else result[0]

    if hasattr(features, "loc") and hasattr(features, "columns"):
        arr = features.loc[:, cols].to_numpy(dtype=np.float32)
        result = _preprocess_feature_matrix(
            arr,
            cols,
            feature_schema=feature_schema,
            phase=phase,
            model_name=model_name,
            fit_preprocessing=fit_preprocessing,
        )
        return result if return_metadata else result[0]

    if isinstance(features, Mapping):
        feature_map = _coerce_feature_map(features)
        values = [feature_map.get(col, float("nan")) for col in cols]
        if any(isinstance(value, (list, tuple, np.ndarray)) for value in values):
            columns_values = [np.asarray(value, dtype=np.float32).reshape(-1) for value in values]
            row_count = max(int(value.shape[0]) for value in columns_values)
            matrix = np.full((row_count, len(cols)), np.nan, dtype=np.float32)
            for idx, value in enumerate(columns_values):
                if int(value.shape[0]) == 1 and row_count > 1:
                    matrix[:, idx] = float(value[0])
                elif int(value.shape[0]) == row_count:
                    matrix[:, idx] = value
                else:
                    raise ValueError("feature_column_length_mismatch")
            result = _preprocess_feature_matrix(
                matrix,
                cols,
                feature_schema=feature_schema,
                phase=phase,
                model_name=model_name,
                fit_preprocessing=fit_preprocessing,
            )
            return result if return_metadata else result[0]
        matrix = np.asarray([[_coerce_raw_feature_value(feature_map.get(col)) for col in cols]], dtype=np.float32)
        result = _preprocess_feature_matrix(
            matrix,
            cols,
            feature_schema=feature_schema,
            phase=phase,
            model_name=model_name,
            fit_preprocessing=fit_preprocessing,
        )
        return result if return_metadata else result[0]

    if isinstance(features, Sequence) and not isinstance(features, (str, bytes, bytearray)):
        rows = list(features)
        if rows and all(isinstance(row, Mapping) for row in rows):
            matrix = [
                [_coerce_raw_feature_value(_coerce_feature_map(row).get(col)) for col in cols]
                for row in rows
            ]
            result = _preprocess_feature_matrix(
                np.asarray(matrix, dtype=np.float32),
                cols,
                feature_schema=feature_schema,
                phase=phase,
                model_name=model_name,
                fit_preprocessing=fit_preprocessing,
            )
            return result if return_metadata else result[0]
        arr = np.asarray(rows, dtype=np.float32)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        if int(arr.shape[1]) != int(len(cols)):
            raise ValueError(f"feature_count_mismatch:{int(arr.shape[1])}:{int(len(cols))}")
        result = _preprocess_feature_matrix(
            arr,
            cols,
            feature_schema=feature_schema,
            phase=phase,
            model_name=model_name,
            fit_preprocessing=fit_preprocessing,
        )
        return result if return_metadata else result[0]

    raise TypeError(f"unsupported_feature_payload:{type(features).__name__}")


def _dump_joblib_to_bytes(value: Any) -> bytes:
    return dumps_pickle_artifact(value, prefer_joblib=True)


def _load_joblib_from_bytes(payload: bytes) -> Any:
    return loads_pickle_artifact(payload, prefer_joblib=True)


def _artifact_payload_from_alias(alias: str, sha256: str = "") -> bytes:
    store = LocalArtifactStore()
    ref = store.resolve(alias) if str(alias or "").strip() else None
    if ref is None and str(sha256 or "").strip():
        from datetime import datetime, timezone

        from engine.artifacts.refs import ArtifactRef

        ref = ArtifactRef(
            sha256=str(sha256).strip(),
            size=0,
            content_type="application/vnd.joblib",
            kind="model",
            created_ts=datetime.now(timezone.utc),
            metadata={},
        )
    return store.get_bytes(ref) if ref is not None else b""


def _fit_eval_metrics(model: Any, X: np.ndarray, y: np.ndarray) -> dict[str, float]:
    if int(y.shape[0]) <= 1:
        return {"rmse": 0.0, "directional_acc": 0.0}
    preds = np.asarray(model.predict(X), dtype=np.float32).reshape(-1)
    err = np.asarray(y, dtype=np.float32).reshape(-1) - preds
    rmse = float(np.sqrt(np.mean(err * err)))
    directional = float(np.mean(np.sign(y) == np.sign(preds)))
    return {"rmse": float(rmse), "directional_acc": float(directional)}


def _as_weight_array(sample_weight: Any, n_rows: int) -> np.ndarray:
    if sample_weight is None:
        return np.ones(int(n_rows), dtype=np.float32)
    arr = np.asarray(sample_weight, dtype=np.float32).reshape(-1)
    if int(arr.shape[0]) != int(n_rows):
        raise ValueError("lgbm_sample_weight_row_count_mismatch")
    arr = np.where(np.isfinite(arr), arr, 1.0).astype(np.float32, copy=False)
    return np.maximum(arr, 0.0).astype(np.float32, copy=False)


def _mse_loss(model: Any, X: np.ndarray | None, y: np.ndarray | None) -> float | None:
    if X is None or y is None or int(np.asarray(y).reshape(-1).shape[0]) <= 0:
        return None
    pred = np.asarray(model.predict(X), dtype=np.float64).reshape(-1)
    truth = np.asarray(y, dtype=np.float64).reshape(-1)
    n = min(int(pred.size), int(truth.size))
    if n <= 0:
        return None
    err = truth[:n] - pred[:n]
    return float(np.mean(err * err))


def _fit_lgbm_continuation(
    *,
    model_factory: Any,
    X: np.ndarray,
    y: np.ndarray,
    columns: Sequence[str],
    sample_weight: np.ndarray,
    init_model: Any,
) -> Any:
    estimator = model_factory()
    estimator.fit(
        X,
        y,
        sample_weight=np.asarray(sample_weight, dtype=np.float32).reshape(-1),
        feature_name=list(columns),
        init_model=init_model,
    )
    return estimator


def _apply_regression_era_boost(
    *,
    model_factory: Any,
    initial_model: Any,
    X_train: np.ndarray,
    y_train: np.ndarray,
    columns: Sequence[str],
    base_sample_weight: np.ndarray,
    train_timestamps: Any = None,
    train_era_labels: Any = None,
    validation_matrix: np.ndarray | None = None,
    validation_target: np.ndarray | None = None,
) -> tuple[Any, dict[str, Any]]:
    cfg = era_boost_config_from_env()
    labels, label_diag = era_labels_for(
        n_obs=int(y_train.shape[0]),
        timestamps=train_timestamps,
        era_labels=train_era_labels,
    )
    payload: dict[str, Any] = {
        "enabled": bool(cfg.get("enabled")),
        "applied": False,
        "config": dict(cfg),
        "label_diagnostics": dict(label_diag),
    }
    if not bool(cfg.get("enabled")):
        return initial_model, payload
    if not bool(label_diag.get("applied")) or len(labels) < int(y_train.shape[0]):
        payload["status"] = "missing_training_eras"
        return initial_model, payload

    score_kind = str(cfg.get("score_kind") or "neg_mse")
    initial_pred = np.asarray(initial_model.predict(X_train), dtype=np.float64).reshape(-1)
    before_table = era_score_table(y_train, initial_pred, labels, score_kind=score_kind)
    payload["before"] = {
        "era_scores": before_table,
        "era_score_std": float(score_std(before_table)),
    }
    if len(before_table) < 2:
        payload["status"] = "insufficient_eras"
        return initial_model, payload

    current = initial_model
    current_val_loss = _mse_loss(current, validation_matrix, validation_target)
    iterations: list[dict[str, Any]] = []
    base_weights = _as_weight_array(base_sample_weight, int(y_train.shape[0]))
    multiplier = float(cfg.get("weight_multiplier") or 2.0)

    for iteration in range(int(cfg.get("iters") or 1)):
        train_pred = np.asarray(current.predict(X_train), dtype=np.float64).reshape(-1)
        table = era_score_table(y_train, train_pred, labels, score_kind=score_kind)
        worst = set(worst_half_eras(table))
        if not worst:
            iterations.append({"iteration": int(iteration + 1), "status": "no_worst_eras"})
            break
        weights = base_weights.copy()
        for idx, label in enumerate(labels[: int(weights.shape[0])]):
            if str(label) in worst:
                weights[idx] = float(weights[idx]) * float(multiplier)
        candidate = _fit_lgbm_continuation(
            model_factory=model_factory,
            X=X_train,
            y=y_train,
            columns=columns,
            sample_weight=weights,
            init_model=current.booster_,
        )
        candidate_val_loss = _mse_loss(candidate, validation_matrix, validation_target)
        degraded = (
            current_val_loss is not None
            and candidate_val_loss is not None
            and validation_degraded(
                prior_loss=float(current_val_loss),
                candidate_loss=float(candidate_val_loss),
                max_degrade=float(cfg.get("max_degrade") or 0.0),
            )
        )
        iteration_payload = {
            "iteration": int(iteration + 1),
            "worst_eras": sorted(worst),
            "weight_source_eras": sorted({str(label) for label in labels}),
            "weighted_rows": int(sum(1 for label in labels if str(label) in worst)),
            "validation_loss_before": (None if current_val_loss is None else float(current_val_loss)),
            "validation_loss_after": (None if candidate_val_loss is None else float(candidate_val_loss)),
            "accepted": bool(not degraded),
        }
        iterations.append(iteration_payload)
        if degraded:
            payload["status"] = "stopped_validation_degrade"
            break
        current = candidate
        current_val_loss = candidate_val_loss

    final_pred = np.asarray(current.predict(X_train), dtype=np.float64).reshape(-1)
    after_table = era_score_table(y_train, final_pred, labels, score_kind=score_kind)
    payload.update(
        {
            "applied": True,
            "status": str(payload.get("status") or "completed"),
            "iterations": iterations,
            "after": {
                "era_scores": after_table,
                "era_score_std": float(score_std(after_table)),
            },
            "validation_rows": int(0 if validation_target is None else np.asarray(validation_target).reshape(-1).shape[0]),
        }
    )
    return current, payload


class LGBMRegressorModel:
    """LightGBM regressor with schema-bound feature vectorization."""

    family = FAMILY
    model_kind = DEFAULT_MODEL_KIND

    def __init__(
        self,
        *,
        model_name: str = DEFAULT_MODEL_NAME,
        feature_ids: Sequence[Any] | None = None,
        hyperparams: Mapping[str, Any] | None = None,
        model: Any = None,
        training_metrics: Mapping[str, Any] | None = None,
    ) -> None:
        self.model_name = str(model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
        self.feature_ids = _expected_columns(feature_ids, model_name=self.model_name)
        self.hyperparams = self._default_hyperparams()
        self.hyperparams.update(dict(hyperparams or {}))
        self.model = model
        self.training_metrics = dict(training_metrics or {})
        self.ood_profile = dict(getattr(self, "ood_profile", {}) or {})
        metrics_schema = self.training_metrics.get("feature_schema") if isinstance(self.training_metrics, Mapping) else None
        self.feature_preprocessing = (
            dict((metrics_schema or {}).get("preprocessing") or {})
            if isinstance(metrics_schema, Mapping)
            else {}
        )
        self.persisted_feature_schema = dict(self.feature_schema)

    @staticmethod
    def _default_hyperparams() -> dict[str, Any]:
        return {
            "objective": "regression",
            "num_leaves": 31,
            "learning_rate": 0.05,
            "n_estimators": 100,
            "min_child_samples": 2,
            "random_state": 42,
            "n_jobs": model_family_n_jobs("LGBM_N_JOBS"),
            "verbosity": -1,
            "deterministic": True,
            "force_col_wise": True,
        }

    @property
    def feature_schema(self) -> dict[str, Any]:
        return _feature_schema(self.feature_ids, preprocessing=getattr(self, "feature_preprocessing", {}))

    def _new_estimator(self) -> Any:
        try:
            import lightgbm as lgb
        except ImportError as exc:  # pragma: no cover - dependency is declared
            raise RuntimeError("lightgbm_not_installed") from exc
        return lgb.LGBMRegressor(**dict(self.hyperparams))

    def fit(
        self,
        X: Any,
        y: Any,
        sample_weight: Any = None,
        *,
        era_timestamps: Any = None,
        era_labels: Any = None,
        validation_data: tuple[Any, Any] | None = None,
        validation_timestamps: Any = None,
        validation_era_labels: Any = None,
    ) -> "LGBMRegressorModel":
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr, preprocessing, _accounting = _matrix_from_features(
            X,
            columns,
            phase="train",
            model_name=self.model_name,
            fit_preprocessing=True,
            return_metadata=True,
        )
        y_arr = np.asarray(y, dtype=np.float32).reshape(-1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("lgbm_row_count_mismatch")
        base_sample_weight = _as_weight_array(sample_weight, int(y_arr.shape[0]))
        self.feature_ids = list(columns)
        self.feature_preprocessing = dict(preprocessing or {})
        model = self._new_estimator()
        fit_kwargs: dict[str, Any] = {"feature_name": list(columns)}
        if sample_weight is not None:
            fit_kwargs["sample_weight"] = base_sample_weight
        model.fit(X_arr, y_arr, **fit_kwargs)

        validation_matrix = None
        validation_target = None
        if validation_data is not None:
            val_X, val_y = validation_data
            validation_matrix = _matrix_from_features(
                val_X,
                columns,
                feature_schema=_feature_schema(columns, preprocessing=dict(preprocessing or {})),
                phase="serve",
                model_name=self.model_name,
            )
            validation_target = np.asarray(val_y, dtype=np.float32).reshape(-1)
            if int(validation_matrix.shape[0]) != int(validation_target.shape[0]):
                raise ValueError("lgbm_validation_row_count_mismatch")

        era_cfg = era_boost_config_from_env()
        if bool(era_cfg.get("enabled")):
            boost_hyperparams = dict(self.hyperparams)
            boost_hyperparams["n_estimators"] = int(era_cfg.get("rounds") or 20)

            def _factory() -> Any:
                try:
                    import lightgbm as lgb
                except ImportError as exc:  # pragma: no cover - dependency is declared
                    raise RuntimeError("lightgbm_not_installed") from exc
                return lgb.LGBMRegressor(**boost_hyperparams)

            model, era_payload = _apply_regression_era_boost(
                model_factory=_factory,
                initial_model=model,
                X_train=X_arr,
                y_train=y_arr,
                columns=columns,
                base_sample_weight=base_sample_weight,
                train_timestamps=era_timestamps,
                train_era_labels=era_labels,
                validation_matrix=validation_matrix,
                validation_target=validation_target,
            )
        else:
            era_payload = {"enabled": False, "applied": False, "config": dict(era_cfg)}
        self.model = model
        self.ood_profile = build_ood_profile(X_arr, columns)
        self.training_metrics = {
            "n_train": int(y_arr.shape[0]),
            "model_family": str(self.family),
            "model_kind": str(self.model_kind),
            "feature_schema": self.feature_schema,
            "ood_profile_summary": summarize_ood_profile(self.ood_profile),
            **_fit_eval_metrics(model, X_arr, y_arr),
        }
        if bool(era_payload.get("enabled")):
            # Validation labels are intentionally not persisted in the weight
            # source payload; they are only used for the stop/rollback guard.
            era_payload["validation_label_diagnostics"] = era_labels_for(
                n_obs=(0 if validation_target is None else int(validation_target.shape[0])),
                timestamps=validation_timestamps,
                era_labels=validation_era_labels,
            )[1]
            self.training_metrics["era_boost"] = dict(era_payload)
        self.persisted_feature_schema = dict(self.feature_schema)
        return self

    def predict(self, X: Any) -> np.ndarray:
        if self.model is None:
            raise RuntimeError("lgbm_model_not_fitted")
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr = _matrix_from_features(
            X,
            columns,
            feature_schema=getattr(self, "persisted_feature_schema", None) or self.feature_schema,
            phase="serve",
            model_name=self.model_name,
        )
        raw = self.model.predict(X_arr)
        return np.asarray(raw, dtype=np.float32).reshape(-1)

    def predict_one(self, features: Mapping[str, Any]) -> float:
        return float(self.predict(features)[0])

    def score_ood(self, features: Any) -> dict[str, Any]:
        return score_ood(getattr(self, "ood_profile", None), features)

    def save(self, path: str | Path) -> Path:
        target = Path(path)
        if not target.suffix:
            target = target / "model.joblib"
        return dump_pickle_artifact(self, target, prefer_joblib=True)

    @classmethod
    def load(cls, path: str | Path) -> "LGBMRegressorModel":
        loaded = load_pickle_artifact(path, prefer_joblib=True)
        if not isinstance(loaded, cls):
            raise TypeError("invalid_lgbm_regressor_artifact")
        _assert_loaded_feature_schema_current(loaded)
        return loaded

    def to_bytes(self) -> bytes:
        return _dump_joblib_to_bytes(self)

    @classmethod
    def from_bytes(cls, payload: bytes) -> "LGBMRegressorModel":
        loaded = _load_joblib_from_bytes(payload)
        if not isinstance(loaded, cls):
            raise TypeError("invalid_lgbm_regressor_payload")
        _assert_loaded_feature_schema_current(loaded)
        return loaded


def train_lgbm_regressor(
    X: Any,
    y: Any,
    *,
    feature_ids: Sequence[Any] | None = None,
    sample_weight: Any = None,
    hyperparams: Mapping[str, Any] | None = None,
    model_name: str = DEFAULT_MODEL_NAME,
    era_timestamps: Any = None,
    era_labels: Any = None,
    validation_data: tuple[Any, Any] | None = None,
    validation_timestamps: Any = None,
    validation_era_labels: Any = None,
) -> LGBMRegressorModel:
    return LGBMRegressorModel(
        model_name=str(model_name or DEFAULT_MODEL_NAME),
        feature_ids=feature_ids,
        hyperparams=hyperparams,
    ).fit(
        X,
        y,
        sample_weight=sample_weight,
        era_timestamps=era_timestamps,
        era_labels=era_labels,
        validation_data=validation_data,
        validation_timestamps=validation_timestamps,
        validation_era_labels=validation_era_labels,
    )


def evaluate_lgbm_regressor(model: LGBMRegressorModel, X: Any, y: Any) -> dict[str, Any]:
    """Evaluate a schema-bound LightGBM regressor on held-out rows."""

    y_arr = np.asarray(y, dtype=np.float32).reshape(-1)
    preds = np.asarray(model.predict(X), dtype=np.float32).reshape(-1)
    if int(preds.shape[0]) != int(y_arr.shape[0]):
        raise ValueError("lgbm_eval_row_count_mismatch")
    if int(y_arr.shape[0]) <= 0:
        return {"rmse": 0.0, "mae": 0.0, "n_eval": 0}
    err = y_arr - preds
    return {
        "rmse": float(np.sqrt(np.mean(err * err))),
        "mae": float(np.mean(np.abs(err))),
        "n_eval": int(y_arr.shape[0]),
    }


def continue_lgbm_regressor(
    model: LGBMRegressorModel,
    X: Any,
    y: Any,
    *,
    num_boost_round: int = 25,
    sample_weight: Any = None,
) -> LGBMRegressorModel:
    """Continue a persisted LightGBM booster on a recent window.

    PatchTST and other non-LightGBM families are intentionally out of scope for
    this incremental path; they continue to use full retrains.
    """

    if model.model is None or not hasattr(model.model, "booster_"):
        raise RuntimeError("lgbm_incremental_requires_fitted_booster")
    columns = _expected_columns(model.feature_ids, model_name=model.model_name, model_spec=model.feature_schema)
    X_arr = _matrix_from_features(
        X,
        columns,
        feature_schema=getattr(model, "persisted_feature_schema", None) or model.feature_schema,
        phase="serve",
        model_name=model.model_name,
    )
    y_arr = np.asarray(y, dtype=np.float32).reshape(-1)
    if int(X_arr.shape[0]) != int(y_arr.shape[0]):
        raise ValueError("lgbm_incremental_row_count_mismatch")

    hyperparams = dict(model.hyperparams or {})
    hyperparams["n_estimators"] = max(1, int(num_boost_round or 25))
    refreshed = LGBMRegressorModel(
        model_name=str(model.model_name),
        feature_ids=list(columns),
        hyperparams=hyperparams,
    )
    refreshed.feature_preprocessing = dict(getattr(model, "feature_preprocessing", {}) or {})
    refreshed.persisted_feature_schema = dict(getattr(model, "persisted_feature_schema", None) or model.feature_schema)
    estimator = refreshed._new_estimator()
    fit_kwargs: dict[str, Any] = {
        "feature_name": list(columns),
        "init_model": model.model.booster_,
    }
    if sample_weight is not None:
        fit_kwargs["sample_weight"] = np.asarray(sample_weight, dtype=np.float32).reshape(-1)
    estimator.fit(X_arr, y_arr, **fit_kwargs)
    refreshed.model = estimator
    refreshed.ood_profile = build_ood_profile(X_arr, columns)
    refreshed.training_metrics = {
        **dict(model.training_metrics or {}),
        "incremental_refresh": True,
        "n_refresh": int(y_arr.shape[0]),
        "num_boost_round": int(hyperparams["n_estimators"]),
        "model_family": str(refreshed.family),
        "model_kind": str(refreshed.model_kind),
        "feature_schema": refreshed.feature_schema,
        "ood_profile_summary": summarize_ood_profile(refreshed.ood_profile),
        **_fit_eval_metrics(estimator, X_arr, y_arr),
    }
    return refreshed


def persist_model_artifact(
    model: LGBMRegressorModel,
    *,
    family: str = FAMILY,
    symbol: str = "*",
    version: str,
) -> dict[str, Any]:
    alias = f"model:{family}:{str(model.model_name)}:{str(symbol or '*').upper()}:current"
    payload = model.to_bytes()
    ref = LocalArtifactStore().put(
        payload,
        content_type="application/vnd.joblib",
        kind="model",
        alias=alias,
        metadata={
            "model_name": str(model.model_name),
            "family": str(family),
            "symbol": str(symbol or "*").upper(),
            "version": str(version),
            "feature_schema": dict(model.feature_schema),
            "ood_profile_summary": summarize_ood_profile(getattr(model, "ood_profile", None)),
        },
    )
    return {
        "alias": str(alias),
        "sha256": str(ref.sha256),
        "size_bytes": int(ref.size),
        "content_type": str(ref.content_type),
    }


def register_shadow_model(
    model: LGBMRegressorModel,
    *,
    symbol: str = "*",
    version: str | None = None,
    family: str = FAMILY,
    model_kind: str = DEFAULT_MODEL_KIND,
    performance_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    version_s = str(version or version_from_ts(str(model.model_name), int(time.time() * 1000), prefix=str(family)))
    manifest = persist_model_artifact(model, family=str(family), symbol=str(symbol), version=version_s)
    metrics = {
        **dict(model.training_metrics or {}),
        **dict(performance_metrics or {}),
        "model_name": str(model.model_name),
        "model_version": str(version_s),
        "model_family": str(family),
        "model_kind": str(model_kind),
        "feature_ids": list(model.feature_ids),
        "feature_set_tag": str(model.feature_schema.get("feature_set_tag") or ""),
        "feature_schema": dict(model.feature_schema),
        "artifact_alias": str(manifest.get("alias") or ""),
        "artifact_sha256": str(manifest.get("sha256") or ""),
    }
    model_ts_ms = int(time.time() * 1000)
    register_model(
        model_name=str(model.model_name),
        model_kind=str(model_kind),
        model_ts_ms=int(model_ts_ms),
        stage="shadow",
        metrics=dict(metrics),
        regime="global",
    )
    register_model_version(
        model_name=str(model.model_name),
        model_version=str(version_s),
        model_kind=str(model_kind),
        stage="shadow",
        status="trained",
        live_ready=False,
        training_job_name=f"train_{family}_models",
        train_scope={
            "symbol": str(symbol or "*").upper(),
            "feature_ids": list(model.feature_ids),
            "feature_schema": dict(model.feature_schema),
        },
        meta=dict(metrics),
    )
    catalog_symbol = str(symbol or "*").upper()
    if catalog_symbol != "*":
        register_model(
            symbol=catalog_symbol,
            model_name=str(model.model_name),
            model_kind=str(model_kind),
            version=str(version_s),
            status="shadow",
            is_active=False,
            metadata={"artifact_manifest": dict(manifest), **dict(metrics)},
            performance_metrics=dict(metrics),
            artifact_uri=str(manifest.get("alias") or ""),
        )
    return {"version": version_s, "stage": "shadow", "artifact_manifest": manifest, "metrics": metrics}


def load_model_from_artifact(alias: str = "", sha256: str = "", path: str | Path | None = None) -> LGBMRegressorModel:
    if path is not None and str(path).strip():
        return LGBMRegressorModel.load(Path(path))
    payload = _artifact_payload_from_alias(str(alias or ""), str(sha256 or ""))
    if not payload:
        raise FileNotFoundError("lgbm_artifact_not_found")
    return LGBMRegressorModel.from_bytes(payload)


def _resolve_training_config(family: str, plan: Mapping[str, Any] | None = None) -> dict[str, Any]:
    from engine.strategy.model_config import get_model_config, load_model_configs

    plan_dict = dict(plan or {})
    model_name = str(plan_dict.get("model_name") or "").strip()
    cfg = get_model_config(model_name, family=family) if model_name else {}
    if not cfg:
        configs = load_model_configs(family=family, include_disabled=True)
        cfg = dict(configs[0]) if configs else {"family": family, "model_name": family}
    model_name = str(model_name or cfg.get("model_name") or family).strip() or family
    feature_ids = _expected_columns(cfg.get("feature_ids"), model_name=model_name, model_spec=cfg)
    schema_guard = _resolve_retrain_schema_guard(
        family=str(family),
        model_name=str(model_name),
        feature_ids=list(feature_ids),
        cfg=cfg,
    )
    horizons = [int(h) for h in list(cfg.get("horizons_s") or cfg.get("horizons") or [DEFAULT_HORIZON_S]) if int(h) > 0]
    return {
        **cfg,
        **schema_guard,
        "family": str(family),
        "model_name": str(model_name),
        "feature_ids": list(feature_ids),
        "horizon_s": int(cfg.get("horizon_s") or (horizons[0] if horizons else DEFAULT_HORIZON_S)),
        "horizons_s": list(horizons or [DEFAULT_HORIZON_S]),
        "symbol_universe": list(cfg.get("symbol_universe") or cfg.get("symbols") or ["*"]),
        "training_window_days": int(cfg.get("training_window_days") or cfg.get("lookback_days") or DEFAULT_LOOKBACK_DAYS),
        "hyperparams": dict(cfg.get("hyperparams") or {}),
    }


def _load_training_rows(
    *,
    cutoff_ms: int,
    horizon_s: int,
    symbols: Sequence[str],
    feature_ids: Sequence[str],
    include_metadata: bool = False,
) -> tuple[list[dict[str, float]], list[float]] | tuple[list[dict[str, float]], list[float], list[dict[str, int | str]]]:
    symbol_filter = {str(s).upper().strip() for s in list(symbols or []) if str(s or "").strip() and str(s).strip() != "*"}
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT l.symbol, l.horizon_s, COALESCE(le.net_z, l.impact_z) AS impact_z,
                   e.ts_ms, e.title, e.body, e.source
            FROM labels l
            JOIN events e ON e.id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            WHERE e.ts_ms >= ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            ORDER BY e.ts_ms ASC, l.event_id ASC, l.symbol ASC
            """,
            (int(cutoff_ms),),
        ).fetchall()
    finally:
        con.close()
    X_rows: list[dict[str, float]] = []
    y_rows: list[float] = []
    meta_rows: list[dict[str, int | str]] = []
    for symbol, row_horizon_s, impact_z, ts_ms, title, body, source in rows or []:
        sym = str(symbol or "").upper().strip()
        if not sym or (symbol_filter and sym not in symbol_filter):
            continue
        if int(row_horizon_s or 0) != int(horizon_s):
            continue
        event = {
            "ts_ms": int(ts_ms or 0),
            "title": str(title or ""),
            "body": str(body or ""),
            "source": str(source or ""),
        }
        snapshot = build_feature_snapshot(event=event, symbol=sym, feature_ids=list(feature_ids))
        X_rows.append({feature_id: _coerce_raw_feature_value(dict(snapshot).get(feature_id)) for feature_id in feature_ids})
        y_rows.append(_safe_float(impact_z, 0.0))
        meta_rows.append({"symbol": str(sym), "ts": int(ts_ms or 0), "horizon": int(horizon_s)})
    if include_metadata:
        return X_rows, y_rows, meta_rows
    return X_rows, y_rows


def run_tabular_training_job(
    *,
    family: str,
    model_cls: type[LGBMRegressorModel],
    model_kind: str,
    version_prefix: str,
) -> int:
    try:
        assert_offline_work_allowed(job_name=f"train_{family}_models")
    except RuntimeError as exc:
        print(f"[workload_profile] {exc}")
        return 3
    init_db()
    plan = load_lifecycle_plan(str(family))
    cfg = _resolve_training_config(str(family), plan)
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS) * 86_400_000
    feature_ids = list(cfg.get("feature_ids") or [])
    try:
        from engine.data.universe_pit import resolve_training_window_universe

        con_universe = connect(readonly=True)
        try:
            pit_universe = resolve_training_window_universe(
                con_universe,
                configured_symbols=list(cfg.get("symbol_universe") or ["*"]),
                lookback_days=int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS),
                as_of_ts_ms=int(now_ms),
            )
        finally:
            con_universe.close()
        if list(pit_universe.get("symbols") or []):
            cfg["symbol_universe"] = list(pit_universe.get("symbols") or [])
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    loaded_rows = _load_training_rows(
        cutoff_ms=int(cutoff_ms),
        horizon_s=int(cfg.get("horizon_s") or DEFAULT_HORIZON_S),
        symbols=list(cfg.get("symbol_universe") or ["*"]),
        feature_ids=list(feature_ids),
        include_metadata=True,
    )
    X_rows, y_rows, meta_rows = loaded_rows
    min_samples = int(os.environ.get(f"{family.upper()}_MIN_SAMPLES", str(DEFAULT_MIN_SAMPLES)))
    if len(y_rows) < max(2, min_samples):
        print(f"{family}: insufficient_samples n={len(y_rows)} min_required={max(2, min_samples)}")
        return 0
    split = min(max(1, int(len(y_rows) * 0.8)), int(len(y_rows) - 1))
    X_train = X_rows[:split]
    y_train = y_rows[:split]
    X_eval = X_rows[split:]
    y_eval = y_rows[split:]
    meta_eval = meta_rows[split:]

    model = model_cls(
        model_name=str(cfg.get("model_name") or family),
        feature_ids=list(feature_ids),
        hyperparams=dict(cfg.get("hyperparams") or {}),
    )
    fit_kwargs = {
        "era_timestamps": [int((meta or {}).get("ts") or 0) for meta in meta_rows[:split]],
        "validation_data": (X_eval, y_eval),
        "validation_timestamps": [int((meta or {}).get("ts") or 0) for meta in meta_eval],
    }
    try:
        signature = inspect.signature(model.fit)
        parameters = dict(signature.parameters)
        if not any(param.kind == inspect.Parameter.VAR_KEYWORD for param in parameters.values()):
            fit_kwargs = {key: value for key, value in fit_kwargs.items() if key in parameters}
    # system-audit: ignore[silent_except] signature probing is optional compatibility filtering.
    except (TypeError, ValueError):
        pass
    model.fit(X_train, y_train, **fit_kwargs)
    try:
        eval_pred = model.predict(X_eval)
        oos_run_id = str(uuid.uuid4())
        upsert_oos_predictions(
            [
                {
                    "symbol": str(meta.get("symbol") or "*"),
                    "horizon": int(meta.get("horizon") or cfg.get("horizon_s") or DEFAULT_HORIZON_S),
                    "family": str(family),
                    "ts": int(meta.get("ts") or 0),
                    "run_id": str(oos_run_id),
                    "prediction": float(eval_pred[idx]),
                    "target": float(y_eval[idx]),
                }
                for idx, meta in enumerate(meta_eval)
            ]
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    version = str(
        plan.get("model_version")
        or cfg.get("training_version_id")
        or version_from_ts(str(model.model_name), now_ms, prefix=str(version_prefix))
    )
    result = register_shadow_model(
        model,
        symbol="*",
        version=str(version),
        family=str(family),
        model_kind=str(model_kind),
    )
    metrics = dict(result.get("metrics") or {})
    record_version_performance(
        model_name=str(model.model_name),
        model_version=str(version),
        metric_scope="training",
        metrics={
            "avg_rmse": float(metrics.get("rmse") or 0.0),
            "avg_directional_acc": float(metrics.get("directional_acc") or 0.0),
            "quality_score": float(max(0.0, min(1.0, _safe_float(metrics.get("directional_acc"), 0.0)))),
            "trained_models": 1,
        },
        sample_n=int(len(y_rows)),
        meta={"job_name": f"train_{family}_models"},
    )
    update_model_version_status(
        str(model.model_name),
        str(version),
        stage="shadow",
        status="trained",
        live_ready=False,
        meta_patch={"training_completed_ts_ms": int(time.time() * 1000)},
    )
    print(json.dumps({"ok": True, "family": str(family), "version": str(version), "stage": "shadow"}))
    return 0


def main() -> int:
    return run_tabular_training_job(
        family=FAMILY,
        model_cls=LGBMRegressorModel,
        model_kind=DEFAULT_MODEL_KIND,
        version_prefix="lgbm",
    )


__all__ = [
    "FAMILY",
    "LGBMRegressorModel",
    "continue_lgbm_regressor",
    "evaluate_lgbm_regressor",
    "load_model_from_artifact",
    "main",
    "persist_model_artifact",
    "register_shadow_model",
    "run_tabular_training_job",
    "train_lgbm_regressor",
]


if __name__ == "__main__":
    raise SystemExit(main())
