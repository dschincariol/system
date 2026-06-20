"""Real-time symbol inference backed by feature snapshots and the model catalog.

Flow:
  online_feature_runtime -> model_registry -> prediction -> validation/timescale storage
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import pickle
import threading
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Mapping, Sequence

import numpy as np

try:
    import joblib
except Exception:  # pragma: no cover - optional dependency transitively provided by sklearn
    joblib = None  # type: ignore[assignment]

try:
    import torch
except Exception:  # pragma: no cover - optional dependency at runtime
    torch = None  # type: ignore[assignment]

from engine.ensemble_model import EnsembleModel
from engine.ensemble_engine import estimate_model_weight
from engine.model_registry import DEFAULT_MODEL_REGISTRY
from engine.prediction_logger import DEFAULT_PREDICTION_LOGGER
from engine.regime_detector import normalize_regime_state, regime_signature, resolve_regime_snapshot
from engine.runtime.artifact_store import artifact_cache_key, resolve_artifact_read_path
from engine.runtime.data_quality import record_model_input_validation, record_scoring_pipeline
from engine.runtime.db_guard import resolve_db_path
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.inference_runtime import (
    get_best_online_model_record,
    get_online_feature_contract,
    list_online_model_records,
    load_online_model_record,
    read_online_feature_snapshot,
    validate_online_feature_snapshot,
)
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_timing
from engine.runtime.observability import backoff_delay_s, record_component_health
from engine.strategy.model_config import get_model_config
from engine.strategy.uncertainty_sizing import ensemble_epistemic_uncertainty
from engine.strategy.validation import store_prediction

LOG = get_logger("engine.inference_engine")
_WARNED_NONFATAL_KEYS: set[str] = set()
_ARTIFACT_CACHE_LOCK = threading.RLock()
_ARTIFACT_CACHE: dict[str, tuple[int, Any]] = {}
_ARTIFACT_CACHE_DB_PATH = ""

DEFAULT_TIMEOUT_S = max(0.05, float(os.environ.get("INFERENCE_TIMEOUT_S", "1.0")))
DEFAULT_BATCH_CONCURRENCY = max(1, int(os.environ.get("INFERENCE_BATCH_CONCURRENCY", "8")))
DEFAULT_HORIZON_S = max(1, int(os.environ.get("INFERENCE_DEFAULT_HORIZON_S", "300")))
DEFAULT_ENSEMBLE_ENABLED = str(os.environ.get("INFERENCE_ENSEMBLE_ENABLED", "0") or "0").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DEFAULT_ENSEMBLE_METHOD = (
    str(os.environ.get("INFERENCE_ENSEMBLE_METHOD", "weighted_average") or "weighted_average").strip()
    or "weighted_average"
)
DEFAULT_ENSEMBLE_MODEL_LIMIT = max(2, int(os.environ.get("INFERENCE_ENSEMBLE_MODEL_LIMIT", "5")))
SAFE_PREDICTION = float(os.environ.get("INFERENCE_SAFE_PREDICTION", "0.0") or 0.0)
SAFE_CONFIDENCE = max(0.0, min(1.0, float(os.environ.get("INFERENCE_SAFE_CONFIDENCE", "0.0") or 0.0)))
ARTIFACT_LOAD_RETRY_ATTEMPTS = max(1, int(os.environ.get("INFERENCE_ARTIFACT_LOAD_RETRY_ATTEMPTS", "2")))
ARTIFACT_LOAD_RETRY_BASE_S = max(0.0, float(os.environ.get("INFERENCE_ARTIFACT_LOAD_RETRY_BASE_S", "0.05")))
ARTIFACT_LOAD_RETRY_MAX_S = max(0.0, float(os.environ.get("INFERENCE_ARTIFACT_LOAD_RETRY_MAX_S", "0.5")))
PERSIST_RETRY_ATTEMPTS = max(1, int(os.environ.get("INFERENCE_PERSIST_RETRY_ATTEMPTS", "2")))
PERSIST_RETRY_BASE_S = max(0.0, float(os.environ.get("INFERENCE_PERSIST_RETRY_BASE_S", "0.05")))
PERSIST_RETRY_MAX_S = max(0.0, float(os.environ.get("INFERENCE_PERSIST_RETRY_MAX_S", "0.5")))
ENSEMBLE_PARALLEL_WORKERS = max(2, int(os.environ.get("INFERENCE_ENSEMBLE_PARALLEL_WORKERS", "4")))
MAX_ABS_PREDICTION = max(1.0, float(os.environ.get("INFERENCE_MAX_ABS_PREDICTION", "100.0")))


def _resolve_ensemble_parallel_workers() -> int:
    raw_value = os.environ.get("INFERENCE_ENSEMBLE_PARALLEL_WORKERS")
    if raw_value is None:
        return int(ENSEMBLE_PARALLEL_WORKERS)
    try:
        return max(2, int(raw_value))
    except Exception:
        return int(ENSEMBLE_PARALLEL_WORKERS)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.inference_engine",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _current_db_path_key() -> str:
    try:
        return str(resolve_db_path())
    except Exception:
        return ""


def _reset_artifact_cache_if_db_changed() -> None:
    global _ARTIFACT_CACHE_DB_PATH, _ARTIFACT_CACHE
    current_db_path = _current_db_path_key()
    with _ARTIFACT_CACHE_LOCK:
        if str(_ARTIFACT_CACHE_DB_PATH or "") == str(current_db_path or ""):
            return
        _ARTIFACT_CACHE = {}
        _ARTIFACT_CACHE_DB_PATH = str(current_db_path or "")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _clip_confidence(value: Any, default: float = SAFE_CONFIDENCE) -> float:
    return float(max(0.0, min(1.0, _safe_float(value, default))))


def _first_scalar(value: Any) -> float:
    if isinstance(value, np.ndarray):
        if value.size <= 0:
            raise ValueError("empty_prediction_array")
        return float(np.asarray(value, dtype=float).reshape(-1)[0])
    if isinstance(value, (list, tuple)):
        if not value:
            raise ValueError("empty_prediction_sequence")
        return _first_scalar(value[0])
    return float(value)


def _pick_metric_confidence(record: Mapping[str, Any], prediction: float) -> float:
    metrics = dict(record.get("performance_metrics") or {})
    for key in ("confidence", "quality_score", "directional_acc", "directional_accuracy", "accuracy", "win_rate", "auc", "f1"):
        if key in metrics:
            return _clip_confidence(metrics.get(key), default=SAFE_CONFIDENCE)

    for key in ("rmse", "mae", "mse", "loss"):
        if key in metrics:
            return _clip_confidence(1.0 / (1.0 + max(0.0, _safe_float(metrics.get(key), 0.0))), default=SAFE_CONFIDENCE)

    metric_value = record.get("selection_metric_value")
    if metric_value is not None:
        higher_is_better = bool(record.get("selection_metric_higher_is_better", True))
        numeric = _safe_float(metric_value, 0.0)
        if higher_is_better:
            return _clip_confidence(numeric, default=SAFE_CONFIDENCE)
        return _clip_confidence(1.0 / (1.0 + max(0.0, numeric)), default=SAFE_CONFIDENCE)

    return _clip_confidence(abs(_safe_float(prediction, 0.0)) / (1.0 + abs(_safe_float(prediction, 0.0))), default=SAFE_CONFIDENCE)


def _resolve_horizon_s(record: Mapping[str, Any], requested_horizon_s: int | None) -> int:
    if requested_horizon_s is not None and int(requested_horizon_s) > 0:
        return int(requested_horizon_s)

    metadata = dict(record.get("metadata") or {})
    config = get_model_config(str(record.get("model_name") or ""))

    for source in (
        metadata.get("horizon_s"),
        config.get("horizon_s"),
    ):
        resolved = _safe_int(source, 0)
        if resolved > 0:
            return int(resolved)

    for source in (
        metadata.get("horizons_s"),
        config.get("horizons_s"),
    ):
        if isinstance(source, list):
            for raw in source:
                resolved = _safe_int(raw, 0)
                if resolved > 0:
                    return int(resolved)

    return int(DEFAULT_HORIZON_S)


def _resolve_feature_ids(record: Mapping[str, Any]) -> list[str]:
    metadata = dict(record.get("metadata") or {})
    config = get_model_config(str(record.get("model_name") or ""))
    for source in (
        metadata.get("feature_ids"),
        dict(metadata.get("feature_schema") or {}).get("feature_ids"),
        config.get("feature_ids"),
    ):
        if isinstance(source, list):
            feature_ids = [str(item or "").strip() for item in source if str(item or "").strip()]
            if feature_ids:
                return feature_ids
    contract = dict(get_online_feature_contract() or {})
    return [
        str(item).strip()
        for item in list(contract.get("feature_names") or [])
        if str(item).strip()
    ]


def _feature_set_tag(record: Mapping[str, Any], feature_snapshot: Mapping[str, Any]) -> str:
    metadata = dict(record.get("metadata") or {})
    config = get_model_config(str(record.get("model_name") or ""))
    for value in (
        metadata.get("feature_set_tag"),
        dict(metadata.get("feature_schema") or {}).get("feature_set_tag"),
        config.get("feature_set_tag"),
        feature_snapshot.get("feature_set_tag"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _tracking_model_version(output: Mapping[str, Any], record: Mapping[str, Any] | None = None) -> str:
    for value in (
        output.get("model_version"),
        (record or {}).get("version"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    if str(output.get("model_kind") or "").strip() == "ensemble":
        text = str(output.get("ensemble_method") or "").strip()
        if text:
            return text
    return "unversioned"


def _attach_regime_fields(
    output: Dict[str, Any],
    regime: Mapping[str, Any] | None,
    *,
    symbol: str,
    ts_ms: int,
) -> Dict[str, Any]:
    state = normalize_regime_state(regime, symbol=symbol, ts_ms=ts_ms)
    source_time_ms = int(state["time"])
    output["regime"] = {
        "time": int(ts_ms),
        "symbol": str(state["symbol"]),
        "volatility_regime": str(state["volatility_regime"]),
        "trend_regime": str(state["trend_regime"]),
        "liquidity_regime": str(state["liquidity_regime"]),
    }
    output["regime_source_time_ms"] = int(source_time_ms)
    output["regime_time_ms"] = int(ts_ms)
    output["volatility_regime"] = str(state["volatility_regime"])
    output["trend_regime"] = str(state["trend_regime"])
    output["liquidity_regime"] = str(state["liquidity_regime"])
    output["regime_key"] = regime_signature(output["regime"])
    return output


def _track_prediction_output(
    output: Mapping[str, Any],
    *,
    record: Mapping[str, Any] | None = None,
    feature_snapshot: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    if bool(output.get("safe_output")):
        return
    symbol = _normalize_symbol(output.get("symbol"))
    model_name = str(output.get("model_name") or (record or {}).get("model_name") or "").strip()
    if not symbol or not model_name:
        return
    model_version = _tracking_model_version(output, record=record)
    features_version = str(
        output.get("feature_set_tag")
        or _feature_set_tag(record or {}, feature_snapshot or {})
        or "unknown"
    ).strip() or "unknown"
    tracking_metadata = dict((record or {}).get("metadata") or {})
    tracking_metadata.update(dict(metadata or {}))
    tracking_metadata["model_name"] = str(model_name)
    tracking_metadata["model_version"] = str(model_version)
    tracking_metadata["model_id"] = str(output.get("model_id") or tracking_metadata.get("model_id") or model_name)
    tracking_metadata["model_kind"] = str(output.get("model_kind") or tracking_metadata.get("model_kind") or "")
    tracking_metadata["symbol"] = str(symbol)
    tracking_metadata["horizon_s"] = int(output.get("horizon_s") or tracking_metadata.get("horizon_s") or 0)
    tracking_metadata["features_version"] = str(features_version)
    if feature_snapshot:
        tracking_metadata["feature_ts_ms"] = int(feature_snapshot.get("ts_ms") or 0)
    regime_state = normalize_regime_state(
        output.get("regime"),
        symbol=symbol,
        ts_ms=int(output.get("regime_time_ms") or output.get("feature_ts_ms") or output.get("ts_ms") or _now_ms()),
    )
    tracking_metadata["regime"] = {
        "time": int(regime_state["time"]),
        "symbol": str(regime_state["symbol"]),
        "volatility_regime": str(regime_state["volatility_regime"]),
        "trend_regime": str(regime_state["trend_regime"]),
        "liquidity_regime": str(regime_state["liquidity_regime"]),
    }
    if output.get("regime_source_time_ms") is not None:
        tracking_metadata["regime_source_time_ms"] = int(output.get("regime_source_time_ms") or 0)
    tracking_metadata["regime_key"] = regime_signature(regime_state)
    try:
        DEFAULT_MODEL_REGISTRY.register_model(
            name=str(model_name),
            version=str(model_version),
            metadata=tracking_metadata,
        )
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_MODEL_TRACKING_REGISTER_FAILED",
            exc,
            once_key=None,
            model_name=str(model_name),
            model_version=str(model_version),
            symbol=str(symbol),
        )
    try:
        DEFAULT_PREDICTION_LOGGER.log_prediction_nowait(
            model_name=str(model_name),
            model_version=str(model_version),
            symbol=str(symbol),
            timestamp=int(output.get("ts_ms") or _now_ms()),
            prediction=float(output.get("prediction") or SAFE_PREDICTION),
            confidence=float(output.get("confidence") or SAFE_CONFIDENCE),
            features_version=str(features_version),
            model_id=str(output.get("model_id") or tracking_metadata.get("model_id") or model_name),
            horizon_s=int(output.get("horizon_s") or tracking_metadata.get("horizon_s") or 0),
            tracking_source="inference_engine_output",
            metadata=dict(tracking_metadata),
        )
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_PREDICTION_TRACKING_FAILED",
            exc,
            once_key=None,
            model_name=str(model_name),
            model_version=str(model_version),
            symbol=str(symbol),
        )


def _project_feature_vector(feature_snapshot: Mapping[str, Any], record: Mapping[str, Any]) -> tuple[np.ndarray, list[str], float]:
    feature_ids = _resolve_feature_ids(record)
    feature_map = dict(feature_snapshot.get("features") or {})
    if not feature_map:
        raw_vector = list(feature_snapshot.get("vector") or [])
        if raw_vector:
            snapshot_feature_names = [
                str(item or "").strip()
                for item in list(feature_snapshot.get("feature_names") or [])
                if str(item or "").strip()
            ]
            resolved_names = snapshot_feature_names or feature_ids
            vector = np.asarray([_safe_float(value, 0.0) for value in raw_vector], dtype=np.float32)
            return vector, list(resolved_names[: len(raw_vector)]), 1.0
        return np.asarray([], dtype=np.float32), feature_ids, 0.0

    vector_values = [_safe_float(feature_map.get(feature_id), 0.0) for feature_id in feature_ids]
    covered = sum(1 for feature_id in feature_ids if feature_id in feature_map)
    coverage = float(covered / max(1, len(feature_ids)))
    return np.asarray(vector_values, dtype=np.float32), feature_ids, coverage


def _expected_feature_set_tag(record: Mapping[str, Any], feature_snapshot: Mapping[str, Any]) -> str:
    tag = str(_feature_set_tag(record, feature_snapshot) or "").strip()
    return tag


def _validate_model_input_payload(
    *,
    symbol: str,
    record: Mapping[str, Any],
    feature_snapshot: Mapping[str, Any],
    vector: np.ndarray,
    feature_ids: Sequence[str],
    coverage: float,
) -> Dict[str, Any]:
    feature_map = dict(feature_snapshot.get("features") or {})
    model_name = str(record.get("model_name") or "").strip()
    model_version = str(record.get("version") or "").strip()
    feature_ts_ms = int(feature_snapshot.get("ts_ms") or 0)
    snapshot_tag = str(feature_snapshot.get("feature_set_tag") or "").strip()
    expected_tag = _expected_feature_set_tag(record, feature_snapshot)
    missing_feature_ids = [
        str(feature_id)
        for feature_id in list(feature_ids or [])
        if str(feature_id or "").strip() and str(feature_id) not in feature_map
    ]
    shape_valid = int(vector.size) == int(len(feature_ids or [])) and int(vector.size) > 0
    vector_invalid = bool(vector.size <= 0) or bool(not np.all(np.isfinite(vector)))
    schema_mismatch = bool(expected_tag and snapshot_tag and expected_tag != snapshot_tag)
    stale = bool(validate_online_feature_snapshot(feature_snapshot).get("stale"))
    reason_codes: list[str] = []
    if stale:
        reason_codes.append("feature_snapshot_stale")
    if missing_feature_ids:
        reason_codes.append("model_input_missing_features")
    if float(coverage) < 1.0:
        reason_codes.append("model_input_feature_coverage_incomplete")
    if not shape_valid:
        reason_codes.append("model_input_shape_invalid")
    if vector_invalid:
        reason_codes.append("model_input_values_invalid")
    if schema_mismatch:
        reason_codes.append("model_input_schema_mismatch")

    ok = len(reason_codes) == 0
    detail = "ok" if ok else str(reason_codes[0])
    return {
        "ok": bool(ok),
        "status": ("ok" if ok else ("stale" if stale else "invalid")),
        "detail": str(detail),
        "symbol": str(symbol),
        "validated_ts_ms": int(_now_ms()),
        "model_name": str(model_name),
        "model_version": str(model_version),
        "model_kind": str(record.get("model_kind") or ""),
        "feature_ts_ms": int(feature_ts_ms),
        "feature_set_tag": str(snapshot_tag),
        "expected_feature_count": int(len(feature_ids or [])),
        "actual_feature_count": int(vector.size),
        "feature_coverage": float(coverage),
        "missing_feature_ids": list(missing_feature_ids),
        "schema_mismatch": bool(schema_mismatch),
        "shape_valid": bool(shape_valid),
        "stale": bool(stale),
        "reason_codes": list(reason_codes),
    }


def _validate_prediction_output(
    output: Mapping[str, Any],
    *,
    record: Mapping[str, Any],
) -> Dict[str, Any]:
    prediction = _safe_float(output.get("prediction"), float("nan"))
    confidence = _safe_float(output.get("confidence"), float("nan"))
    reason_codes: list[str] = []
    if not math.isfinite(prediction):
        reason_codes.append("prediction_not_finite")
    elif abs(float(prediction)) > float(MAX_ABS_PREDICTION):
        reason_codes.append("prediction_out_of_range")
    if not math.isfinite(confidence):
        reason_codes.append("confidence_not_finite")
    elif float(confidence) < 0.0 or float(confidence) > 1.0:
        reason_codes.append("confidence_out_of_range")
    ok = len(reason_codes) == 0
    return {
        "ok": bool(ok),
        "detail": ("ok" if ok else str(reason_codes[0])),
        "reason_codes": list(reason_codes),
        "model_name": str(record.get("model_name") or ""),
        "model_version": str(record.get("version") or ""),
    }


def _invalid_input_delta_from_reason(reason: Any) -> int:
    normalized = str(reason or "").strip()
    if not normalized:
        return 0
    if normalized == "feature_store_empty":
        return 1
    if normalized.startswith("feature_"):
        return 1
    if normalized.startswith("model_input_"):
        return 1
    if normalized.startswith("artifact_feature_"):
        return 1
    return 0


def _build_scoring_pipeline_payload(
    result: Mapping[str, Any],
    *,
    symbol: str,
    requested_model_name: str | None = None,
) -> Dict[str, Any]:
    safe_output = bool(result.get("safe_output"))
    status = str(result.get("status") or ("safe_default" if safe_output else "ok")).strip() or "unknown"
    fallback_reason = str(result.get("fallback_reason") or "").strip()
    model_name = str(result.get("model_name") or requested_model_name or "").strip()
    detail = str(fallback_reason or ("ok" if (not safe_output and status == "ok") else status))
    reason_codes = [
        str(code)
        for code in [fallback_reason or None, None if status == "ok" else status]
        if str(code or "").strip()
    ]
    return {
        "ok": bool((not safe_output) and status == "ok"),
        "status": str(status),
        "detail": str(detail),
        "symbol": str(symbol),
        "attempt_ts_ms": int(result.get("ts_ms") or _now_ms()),
        "model_name": str(model_name),
        "model_version": str(result.get("model_version") or ""),
        "model_kind": str(result.get("model_kind") or ""),
        "model_loaded": bool(result.get("model_loaded")),
        "prediction": float(_safe_float(result.get("prediction"), SAFE_PREDICTION)),
        "confidence": float(_safe_float(result.get("confidence"), SAFE_CONFIDENCE)),
        "feature_ts_ms": int(result.get("feature_ts_ms") or 0),
        "prediction_ts_ms": int(result.get("ts_ms") or 0),
        "safe_output": bool(safe_output),
        "fallback_reason": str(fallback_reason),
        "config_variant": str(result.get("config_variant") or model_name),
        "reason_codes": list(reason_codes),
        "invalid_input_delta": int(_invalid_input_delta_from_reason(fallback_reason)),
    }


def _resolve_model_record(
    symbol: str,
    *,
    model_name: str | None = None,
    version: str | None = None,
) -> dict[str, Any] | None:
    symbol_key = _normalize_symbol(symbol)
    requested_name = str(model_name or "").strip() or None
    requested_version = str(version or "").strip() or None

    if requested_name and requested_version:
        rec = load_online_model_record(
            symbol_key,
            model_name=requested_name,
            version=requested_version,
            allow_db_refresh=True,
        )
        return dict(rec) if isinstance(rec, dict) else None

    if requested_name:
        rec = load_online_model_record(
            symbol_key,
            model_name=requested_name,
            active_only=True,
            allow_db_refresh=True,
        ) or load_online_model_record(
            symbol_key,
            model_name=requested_name,
            allow_db_refresh=True,
        )
        if rec is not None:
            return dict(rec)

    rec = load_online_model_record(symbol_key, active_only=True, allow_db_refresh=True)
    if rec is not None:
        return dict(rec)

    rec = get_best_online_model_record(symbol_key, model_name=requested_name, allow_db_refresh=True)
    return dict(rec) if isinstance(rec, dict) else None


def _record_lookup_key(record: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(record.get("model_name") or "").strip(),
        str(record.get("version") or "").strip(),
    )


def _record_weight_hint(
    record: Mapping[str, Any],
    *,
    symbol: str,
    regime: Mapping[str, Any] | None = None,
) -> float:
    try:
        payload = dict(record)
        payload["symbol"] = str(symbol)
        payload["regime"] = normalize_regime_state(regime, symbol=symbol)
        return float(estimate_model_weight(payload))
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_MODEL_WEIGHT_HINT_FAILED",
            exc,
            once_key=(
                f"inference_model_weight_hint_failed:{symbol}:"
                f"{record.get('model_name')}:{record.get('version')}"
            ),
            symbol=str(symbol),
            model_name=str(record.get("model_name") or ""),
            version=str(record.get("version") or ""),
        )
        return 0.5


def _resolve_model_records(
    symbol: str,
    *,
    model_name: str | None = None,
    version: str | None = None,
    ensemble_enabled: bool = DEFAULT_ENSEMBLE_ENABLED,
    max_candidates: int = DEFAULT_ENSEMBLE_MODEL_LIMIT,
    current_regime: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return []

    effective_limit = max(1, int(max_candidates or DEFAULT_ENSEMBLE_MODEL_LIMIT))
    if version:
        record = _resolve_model_record(symbol_key, model_name=model_name, version=version)
        return [record] if record else []

    if not ensemble_enabled:
        record = _resolve_model_record(symbol_key, model_name=model_name, version=version)
        return [record] if record else []

    if model_name:
        raw_records = list_online_model_records(
            symbol_key,
            model_name=model_name,
            limit=max(10, effective_limit * 4),
            allow_db_refresh=True,
        )
    else:
        raw_records = list_online_model_records(
            symbol_key,
            active_only=True,
            limit=max(10, effective_limit * 4),
            allow_db_refresh=True,
        )

    candidates = [
        dict(record)
        for record in (raw_records or [])
        if isinstance(record, dict) and str(record.get("artifact_uri") or "").strip()
    ]

    if len(candidates) <= 1:
        record = _resolve_model_record(symbol_key, model_name=model_name, version=version)
        return [record] if record else []

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for record in candidates:
        lookup_key = _record_lookup_key(record)
        if lookup_key in seen:
            continue
        seen.add(lookup_key)
        deduped.append(record)

    deduped.sort(
        key=lambda record: (
            _record_weight_hint(record, symbol=symbol_key, regime=current_regime),
            1 if bool(record.get("is_active")) else 0,
            int(record.get("updated_ts_ms") or 0),
            int(record.get("created_ts_ms") or 0),
        ),
        reverse=True,
    )
    return deduped[:effective_limit]


def _load_artifact_from_path(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".joblib" and joblib is not None:
        return joblib.load(path)
    if suffix in {".pickle", ".pkl"}:
        with path.open("rb") as handle:
            return pickle.load(handle)
    if suffix in {".pt", ".pth"} and torch is not None:
        try:
            return torch.load(path, map_location="cpu", weights_only=False)
        except TypeError:
            return torch.load(path, map_location="cpu")
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))

    if joblib is not None:
        try:
            return joblib.load(path)
        except Exception as exc:
            _warn_nonfatal(
                "INFERENCE_ARTIFACT_JOBLIB_LOAD_FAILED",
                exc,
                once_key=f"inference_artifact_joblib_load_failed:{path}",
                artifact_path=str(path),
            )
    with path.open("rb") as handle:
        return pickle.load(handle)


def _load_model_artifact(record: Mapping[str, Any]) -> Any:
    _reset_artifact_cache_if_db_changed()
    manifest, artifact_path = resolve_artifact_read_path(record)
    log_artifact_uri = str(manifest.get("artifact_uri") or artifact_path)
    cache_key = artifact_cache_key(record, manifest=manifest)
    mtime_ns = int(artifact_path.stat().st_mtime_ns)
    cached_fallback = None
    artifact = None
    with _ARTIFACT_CACHE_LOCK:
        cached = _ARTIFACT_CACHE.get(cache_key)
        if cached is not None and int(cached[0]) == int(mtime_ns):
            return cached[1]
        if cached is not None:
            cached_fallback = cached[1]

    last_error: BaseException | None = None
    for attempt in range(1, int(ARTIFACT_LOAD_RETRY_ATTEMPTS) + 1):
        try:
            artifact = _load_artifact_from_path(artifact_path)
            if attempt > 1:
                log_event(
                    LOG,
                    logging.INFO,
                    "inference_artifact_load_recovered",
                    component="engine.inference_engine",
                    extra={
                        "artifact_uri": log_artifact_uri,
                        "attempt": int(attempt),
                        "model_name": str(record.get("model_name") or ""),
                        "model_version": str(record.get("version") or ""),
                    },
                )
            break
        except Exception as exc:
            last_error = exc
            retryable = attempt < int(ARTIFACT_LOAD_RETRY_ATTEMPTS)
            log_event(
                LOG,
                logging.WARNING if retryable else logging.ERROR,
                "inference_artifact_load_failed",
                component="engine.inference_engine",
                extra={
                    "artifact_uri": log_artifact_uri,
                    "attempt": int(attempt),
                    "retryable": bool(retryable),
                    "error": f"{type(exc).__name__}:{exc}",
                    "model_name": str(record.get("model_name") or ""),
                    "model_version": str(record.get("version") or ""),
                },
            )
            if retryable:
                time.sleep(
                    backoff_delay_s(
                        attempt,
                        base_s=float(ARTIFACT_LOAD_RETRY_BASE_S),
                        max_s=float(ARTIFACT_LOAD_RETRY_MAX_S),
                    )
                )
                continue
            artifact = None
            break

    if artifact is None and cached_fallback is not None:
        log_event(
            LOG,
            logging.WARNING,
            "inference_artifact_cache_fallback",
            component="engine.inference_engine",
            extra={
                "artifact_uri": log_artifact_uri,
                "model_name": str(record.get("model_name") or ""),
                "model_version": str(record.get("version") or ""),
                "reason": (f"{type(last_error).__name__}:{last_error}" if last_error is not None else "load_failed"),
            },
        )
        return cached_fallback

    if artifact is None:
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"model_artifact_load_failed:{artifact_path}")

    with _ARTIFACT_CACHE_LOCK:
        _ARTIFACT_CACHE[cache_key] = (int(mtime_ns), artifact)
    return artifact


def _predict_from_mapping_artifact(artifact: Mapping[str, Any], vector: np.ndarray) -> tuple[float, float | None]:
    if any(key in artifact for key in ("prediction", "predicted_z", "score", "value")):
        for key in ("prediction", "predicted_z", "score", "value"):
            if key in artifact:
                return _safe_float(artifact.get(key), 0.0), artifact.get("confidence")

    weights = artifact.get("weights") or artifact.get("coefficients") or artifact.get("coef")
    if isinstance(weights, (list, tuple, np.ndarray)):
        coef = np.asarray(weights, dtype=np.float32).reshape(-1)
        if coef.size != vector.size:
            raise ValueError(f"artifact_feature_mismatch:{coef.size}:{vector.size}")
        bias = _safe_float(artifact.get("bias", artifact.get("intercept", 0.0)), 0.0)
        prediction = float(np.dot(vector.astype(np.float32, copy=False), coef) + bias)
        return prediction, artifact.get("confidence")

    raise TypeError("unsupported_mapping_model_artifact")


def _parse_prediction_payload(result: Any, *, record: Mapping[str, Any]) -> Dict[str, Any]:
    prediction: float | None = None
    confidence: float | None = None
    meta: Dict[str, Any] = {}

    if isinstance(result, Mapping):
        for key in ("prediction", "predicted_z", "value", "score"):
            if key in result:
                prediction = _safe_float(result.get(key), 0.0)
                break
        for key in ("confidence", "confidence_score", "probability"):
            if key in result:
                confidence = _clip_confidence(result.get(key), default=SAFE_CONFIDENCE)
                break
        for key in (
            "uncertainty",
            "predictive_uncertainty",
            "epistemic_uncertainty",
            "aleatoric_uncertainty",
            "mc_dropout_samples",
            "uncertainty_ts_ms",
            "prediction_vector",
            "epistemic_uncertainty_vector",
            "aleatoric_uncertainty_vector",
            "uncertainty_detail",
        ):
            if key in result:
                meta[key] = result.get(key)
    elif isinstance(result, (tuple, list)):
        if len(result) >= 2:
            prediction = _safe_float(_first_scalar(result[0]), 0.0)
            confidence = _clip_confidence(_first_scalar(result[1]), default=SAFE_CONFIDENCE)
            if len(result) >= 3 and isinstance(result[2], Mapping):
                meta.update(dict(result[2]))
        elif len(result) == 1:
            prediction = _safe_float(_first_scalar(result[0]), 0.0)
    else:
        prediction = _safe_float(_first_scalar(result), 0.0)

    if prediction is None:
        raise ValueError("prediction_unavailable")
    if confidence is None:
        confidence = _pick_metric_confidence(record, prediction)
    meta["prediction"] = float(prediction)
    meta["confidence"] = float(confidence)
    return meta


def _parse_prediction_result(result: Any, *, record: Mapping[str, Any]) -> tuple[float, float]:
    payload = _parse_prediction_payload(result, record=record)
    return float(payload.get("prediction") or 0.0), float(payload.get("confidence") or SAFE_CONFIDENCE)


def _predict_with_artifact_detail(artifact: Any, vector: np.ndarray, *, record: Mapping[str, Any]) -> Dict[str, Any]:
    expected_features = _safe_int(getattr(artifact, "n_features_in_", 0), 0)
    if expected_features > 0 and int(expected_features) != int(vector.size):
        raise ValueError(f"artifact_feature_count_mismatch:{expected_features}:{vector.size}")

    if isinstance(artifact, Mapping):
        prediction, confidence = _predict_from_mapping_artifact(artifact, vector)
        if confidence is None:
            confidence = _pick_metric_confidence(record, prediction)
        return {
            "prediction": float(prediction),
            "confidence": float(_clip_confidence(confidence, default=SAFE_CONFIDENCE)),
        }

    matrix = vector.reshape(1, -1).astype(np.float32, copy=False)

    if hasattr(artifact, "predict_with_uncertainty") and callable(getattr(artifact, "predict_with_uncertainty")):
        return _parse_prediction_payload(artifact.predict_with_uncertainty(matrix), record=record)

    if hasattr(artifact, "predict_with_confidence") and callable(getattr(artifact, "predict_with_confidence")):
        return _parse_prediction_payload(artifact.predict_with_confidence(matrix), record=record)

    if hasattr(artifact, "predict_proba") and callable(getattr(artifact, "predict_proba")):
        predicted = artifact.predict(matrix)
        proba = np.asarray(artifact.predict_proba(matrix), dtype=float)
        confidence = None
        if proba.ndim >= 2 and proba.shape[0] > 0:
            confidence = float(np.max(proba[0]))
        payload = {
            "prediction": _first_scalar(predicted),
            "confidence": confidence,
        }
        return _parse_prediction_payload(payload, record=record)

    if hasattr(artifact, "decision_function") and callable(getattr(artifact, "decision_function")):
        decision = _safe_float(_first_scalar(artifact.decision_function(matrix)), 0.0)
        predicted = decision
        if hasattr(artifact, "predict") and callable(getattr(artifact, "predict")):
            try:
                predicted = _first_scalar(artifact.predict(matrix))
            except Exception:
                predicted = decision
        confidence = 1.0 / (1.0 + math.exp(-abs(decision)))
        return _parse_prediction_payload({"prediction": predicted, "confidence": confidence}, record=record)

    if hasattr(artifact, "predict") and callable(getattr(artifact, "predict")):
        return _parse_prediction_payload(artifact.predict(matrix), record=record)

    if callable(artifact):
        try:
            return _parse_prediction_payload(artifact(matrix), record=record)
        except TypeError:
            return _parse_prediction_payload(artifact(vector), record=record)

    raise TypeError(f"unsupported_model_artifact:{type(artifact).__name__}")


def _predict_with_artifact(artifact: Any, vector: np.ndarray, *, record: Mapping[str, Any]) -> tuple[float, float]:
    payload = _predict_with_artifact_detail(artifact, vector, record=record)
    return float(payload.get("prediction") or 0.0), float(payload.get("confidence") or SAFE_CONFIDENCE)


def _safe_output(
    *,
    symbol: str,
    horizon_s: int,
    reason: str,
    timed_out: bool = False,
    feature_snapshot: Mapping[str, Any] | None = None,
    record: Mapping[str, Any] | None = None,
    requested_model_name: str | None = None,
    regime: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    record_dict = dict(record or {})
    feature_snapshot = dict(feature_snapshot or {})
    resolved_model_name = str(
        record_dict.get("model_name")
        or requested_model_name
        or "safe_default"
    ).strip() or "safe_default"
    metadata = dict(record_dict.get("metadata") or {})
    output = {
        "symbol": _normalize_symbol(symbol),
        "prediction": float(SAFE_PREDICTION),
        "confidence": float(SAFE_CONFIDENCE),
        "prediction_strength": float(abs(SAFE_PREDICTION) * SAFE_CONFIDENCE),
        "horizon_s": int(horizon_s),
        "model_name": str(resolved_model_name),
        "model_id": str(metadata.get("model_id") or resolved_model_name),
        "model_version": (str(record_dict.get("version") or "").strip() or None),
        "model_kind": (str(record_dict.get("model_kind") or "").strip() or None),
        "feature_ts_ms": int(feature_snapshot.get("ts_ms") or 0),
        "feature_set_tag": str(feature_snapshot.get("feature_set_tag") or ""),
        "ts_ms": _now_ms(),
        "timed_out": bool(timed_out),
        "safe_output": True,
        "fallback_reason": str(reason),
        "status": "safe_default",
        "model_loaded": False,
        "config_variant": str(record_dict.get("model_name") or requested_model_name or ""),
        "ensemble_output": _build_ensemble_decision_output(
            final_prediction=float(SAFE_PREDICTION),
            aggregated_confidence=float(SAFE_CONFIDENCE),
            method=str(DEFAULT_ENSEMBLE_METHOD),
            members=[],
            total_weight=0.0,
            fallback=True,
            fallback_reason=str(reason),
        ),
    }
    return _attach_regime_fields(
        output,
        regime,
        symbol=_normalize_symbol(symbol),
        ts_ms=int(feature_snapshot.get("ts_ms") or _now_ms()),
    )


def _build_ensemble_decision_output(
    *,
    final_prediction: float,
    aggregated_confidence: float,
    method: str,
    members: Sequence[Mapping[str, Any]],
    total_weight: float,
    fallback: bool,
    fallback_reason: str | None = None,
) -> Dict[str, Any]:
    return {
        "final_prediction": float(final_prediction),
        "aggregated_confidence": float(aggregated_confidence),
        "method": str(method or DEFAULT_ENSEMBLE_METHOD),
        "ensemble_size": int(len(members or [])),
        "total_weight": float(total_weight),
        "members": [dict(member) for member in (members or [])],
        "fallback": bool(fallback),
        "fallback_reason": (str(fallback_reason) if fallback_reason else None),
    }


def _component_vector_from_members(members: Sequence[Mapping[str, Any]]) -> Dict[str, Any] | None:
    components: Dict[str, Any] = {}
    weights: Dict[str, float] = {}
    for idx, member in enumerate(members or []):
        key = str(
            member.get("model_family")
            or member.get("family")
            or member.get("model_name")
            or member.get("model_id")
            or f"member_{idx}"
        ).strip()
        if not key:
            key = f"member_{idx}"
        try:
            weight = float(member.get("weight") or 0.0)
        except Exception:
            weight = 0.0
        weights[key] = float(weight)
        components[key] = {
            "prediction": member.get("prediction"),
            "confidence": member.get("confidence"),
            "weight": float(weight),
            "model_name": member.get("model_name"),
            "model_id": member.get("model_id"),
            "model_version": member.get("model_version"),
            "model_kind": member.get("model_kind"),
            "weight_source": member.get("weight_source"),
        }
    if not components:
        return None
    return {
        "components": components,
        "weights": weights,
        "source": "inference_ensemble",
    }


def _component_vector_from_output(output: Mapping[str, Any]) -> Dict[str, Any] | None:
    explicit = output.get("component_vector")
    if isinstance(explicit, Mapping):
        return dict(explicit)
    members = output.get("ensemble_members")
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes, bytearray)):
        ensemble_output = output.get("ensemble_output")
        if isinstance(ensemble_output, Mapping):
            members = ensemble_output.get("members")
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes, bytearray)):
        return None
    if str(output.get("model_kind") or "").strip().lower() != "ensemble" and not output.get("ensemble_members"):
        return None
    return _component_vector_from_members([member for member in members if isinstance(member, Mapping)])


def _ensemble_model_version(method: str, members: Sequence[Mapping[str, Any]]) -> str | None:
    parts = []
    for member in members or []:
        name = str(member.get("model_name") or member.get("model_id") or "").strip()
        version = str(member.get("model_version") or member.get("version") or "").strip()
        if name or version:
            parts.append(f"{name}@{version}")
    if not parts:
        return None
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"ensemble:{str(method or DEFAULT_ENSEMBLE_METHOD)}:{digest}"


def _persist_prediction_output(output: Mapping[str, Any]) -> None:
    symbol = _normalize_symbol(output.get("symbol"))
    if not symbol:
        return
    regime_state = normalize_regime_state(
        output.get("regime"),
        symbol=symbol,
        ts_ms=int(output.get("regime_time_ms") or output.get("feature_ts_ms") or output.get("ts_ms") or _now_ms()),
    )
    try:
        event_id = int(output.get("event_id") or -int(time.time_ns()))
    except Exception:
        event_id = -int(time.time_ns())
    last_error: BaseException | None = None
    for attempt in range(1, int(PERSIST_RETRY_ATTEMPTS) + 1):
        try:
            store_prediction(
                event_id,
                symbol,
                int(output.get("horizon_s") or DEFAULT_HORIZON_S),
                float(output.get("prediction") or SAFE_PREDICTION),
                float(output.get("confidence") or SAFE_CONFIDENCE),
                confidence_raw=float(output.get("confidence") or SAFE_CONFIDENCE),
                prediction_strength=float(output.get("prediction_strength") or 0.0),
                model_name=str(output.get("model_name") or "safe_default"),
                model_id=str(output.get("model_id") or output.get("model_name") or "safe_default"),
                model_version=(str(output.get("model_version")) if output.get("model_version") is not None else None),
                features_version=str(output.get("feature_set_tag") or "unknown"),
                tracking_source="inference_engine",
                tracking_metadata={
                    "feature_ts_ms": int(output.get("feature_ts_ms") or 0),
                    "regime": dict(regime_state),
                    "regime_key": regime_signature(regime_state),
                },
                regime=regime_state,
            )
            try:
                from engine.strategy.decision_log import log_decision

                feature_payload = {
                    "feature_ids": list(output.get("feature_ids") or []),
                    "feature_set_tag": str(output.get("feature_set_tag") or "unknown"),
                    "feature_ts_ms": int(output.get("feature_ts_ms") or 0),
                    "feature_coverage": float(output.get("feature_coverage") or 0.0),
                }
                log_decision(
                    event_id=event_id,
                    symbol=symbol,
                    horizon_s=int(output.get("horizon_s") or DEFAULT_HORIZON_S),
                    predicted_z=float(output.get("prediction") or SAFE_PREDICTION),
                    confidence=float(output.get("confidence") or SAFE_CONFIDENCE),
                    model_name=str(output.get("model_name") or "safe_default"),
                    model_kind=(str(output.get("model_kind")) if output.get("model_kind") is not None else None),
                    model_version=(
                        str(output.get("model_version")) if output.get("model_version") is not None else None
                    ),
                    feature_set_tag=str(output.get("feature_set_tag") or "unknown"),
                    features_json=feature_payload,
                    explain_json=dict(output),
                    extra_json={
                        "tracking_source": "inference_engine",
                        "regime": dict(regime_state),
                        "regime_key": regime_signature(regime_state),
                    },
                    component_vector=_component_vector_from_output(output),
                    ts_ms=int(output.get("ts_ms") or _now_ms()),
                )
            except Exception as exc:
                _warn_nonfatal(
                    "INFERENCE_DECISION_LOG_FAILED",
                    exc,
                    once_key=None,
                    symbol=str(symbol),
                    model_name=str(output.get("model_name") or ""),
                )
            return
        except Exception as exc:
            last_error = exc
            if attempt >= int(PERSIST_RETRY_ATTEMPTS):
                break
            _warn_nonfatal(
                "INFERENCE_PERSIST_RETRY",
                exc,
                once_key=None,
                symbol=str(symbol),
                attempt=int(attempt),
                model_name=str(output.get("model_name") or ""),
            )
            time.sleep(
                backoff_delay_s(
                    attempt,
                    base_s=float(PERSIST_RETRY_BASE_S),
                    max_s=float(PERSIST_RETRY_MAX_S),
                )
            )
    if last_error is not None:
        raise last_error


def _build_prediction_output(
    *,
    symbol: str,
    record: Mapping[str, Any],
    feature_snapshot: Mapping[str, Any],
    prediction: float,
    confidence: float,
    horizon_s: int,
    feature_ids: Sequence[str],
    coverage: float,
    requested_model_name: str | None = None,
    regime: Mapping[str, Any] | None = None,
    prediction_meta: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    metadata = dict(record.get("metadata") or {})
    resolved_model_name = str(record.get("model_name") or requested_model_name or "safe_default")
    meta = dict(prediction_meta or {})
    output = {
        "symbol": str(symbol),
        "prediction": float(prediction),
        "confidence": float(confidence),
        "prediction_strength": float(abs(prediction) * confidence),
        "horizon_s": int(horizon_s),
        "model_name": str(resolved_model_name),
        "model_id": str(metadata.get("model_id") or resolved_model_name),
        "model_version": (str(record.get("version") or "").strip() or None),
        "model_kind": (str(record.get("model_kind") or "").strip() or None),
        "feature_ts_ms": int(feature_snapshot.get("ts_ms") or 0),
        "feature_set_tag": str(_feature_set_tag(record, feature_snapshot)),
        "feature_ids": list(feature_ids),
        "feature_coverage": float(coverage),
        "ts_ms": _now_ms(),
        "timed_out": False,
        "safe_output": False,
        "fallback_reason": None,
        "status": "ok",
        "model_loaded": True,
        "config_variant": str(resolved_model_name),
    }
    for key in (
        "uncertainty",
        "predictive_uncertainty",
        "epistemic_uncertainty",
        "aleatoric_uncertainty",
        "mc_dropout_samples",
        "prediction_vector",
        "epistemic_uncertainty_vector",
        "aleatoric_uncertainty_vector",
        "uncertainty_detail",
    ):
        if key in meta:
            output[key] = meta.get(key)
    output["uncertainty_ts_ms"] = int(meta.get("uncertainty_ts_ms") or output["ts_ms"])
    output["ensemble_output"] = _build_ensemble_decision_output(
        final_prediction=float(prediction),
        aggregated_confidence=float(confidence),
        method="single_model_fallback",
        members=[
            {
                "model_name": str(resolved_model_name),
                "model_version": output.get("model_version"),
                "model_id": str(output.get("model_id") or resolved_model_name),
                "model_kind": output.get("model_kind"),
                "prediction": float(prediction),
                "confidence": float(confidence),
                "weight": 1.0,
                "weight_source": "single_model",
                "feature_coverage": float(coverage),
                "horizon_s": int(horizon_s),
            }
        ],
        total_weight=1.0,
        fallback=True,
        fallback_reason="single_model_available",
    )
    return _attach_regime_fields(
        output,
        regime,
        symbol=str(symbol),
        ts_ms=int(feature_snapshot.get("ts_ms") or output.get("ts_ms") or _now_ms()),
    )


def _predict_record_output(
    symbol: str,
    *,
    record: Mapping[str, Any],
    feature_snapshot: Mapping[str, Any],
    preloaded_artifact: Any | None = None,
    requested_model_name: str | None = None,
    horizon_s: int | None = None,
    regime: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    resolved_horizon_s = _resolve_horizon_s(record, horizon_s)
    vector, feature_ids, coverage = _project_feature_vector(feature_snapshot, record)
    input_validation = _validate_model_input_payload(
        symbol=str(symbol),
        record=record,
        feature_snapshot=feature_snapshot,
        vector=vector,
        feature_ids=feature_ids,
        coverage=coverage,
    )
    record_model_input_validation(input_validation)
    if not bool(input_validation.get("ok")):
        raise ValueError(str(input_validation.get("detail") or "model_input_invalid"))
    artifact = preloaded_artifact if preloaded_artifact is not None else _load_model_artifact(record)
    prediction_payload = _predict_with_artifact_detail(artifact, vector, record=record)
    prediction = float(prediction_payload.get("prediction") or 0.0)
    confidence = float(prediction_payload.get("confidence") or SAFE_CONFIDENCE)
    adjusted_confidence = _clip_confidence(
        float(confidence) * float(max(0.0, min(1.0, coverage))),
        default=SAFE_CONFIDENCE,
    )
    prediction_payload["confidence"] = float(adjusted_confidence)
    output = _build_prediction_output(
        symbol=symbol,
        record=record,
        feature_snapshot=feature_snapshot,
        prediction=float(prediction),
        confidence=float(adjusted_confidence),
        horizon_s=int(resolved_horizon_s),
        feature_ids=list(feature_ids),
        coverage=float(coverage),
        requested_model_name=requested_model_name,
        regime=regime,
        prediction_meta=prediction_payload,
    )
    prediction_validation = _validate_prediction_output(output, record=record)
    if not bool(prediction_validation.get("ok")):
        raise ValueError(str(prediction_validation.get("detail") or "prediction_invalid"))
    _track_prediction_output(output, record=record, feature_snapshot=feature_snapshot)
    return output


def _build_ensemble_output(
    symbol: str,
    *,
    member_records: Sequence[Mapping[str, Any]],
    member_outputs: Sequence[Mapping[str, Any]],
    feature_snapshot: Mapping[str, Any],
    method: str,
    horizon_s: int | None = None,
    regime: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    ensemble_inputs = []
    normalized_regime = normalize_regime_state(
        regime,
        symbol=symbol,
        ts_ms=int(feature_snapshot.get("ts_ms") or _now_ms()),
    )
    for record, output in zip(member_records, member_outputs):
        ensemble_inputs.append(
            {
                "symbol": str(symbol),
                "model_id": output.get("model_id"),
                "model_name": output.get("model_name"),
                "model_version": output.get("model_version"),
                "prediction": output.get("prediction"),
                "confidence": output.get("confidence"),
                "performance_metrics": dict(record.get("performance_metrics") or {}),
                "metadata": dict(record.get("metadata") or {}),
                "selection_metric_name": record.get("selection_metric_name"),
                "selection_metric_value": record.get("selection_metric_value"),
                "selection_metric_higher_is_better": record.get("selection_metric_higher_is_better"),
                "regime": dict(normalized_regime),
            }
        )

    combined = EnsembleModel(default_method=str(method or DEFAULT_ENSEMBLE_METHOD)).combine(
        ensemble_inputs,
        method=method,
    )
    combined_members = list(combined.get("members") or [])
    total_weight = float(combined.get("total_weight") or 0.0)
    weighted_coverage = 0.0
    if total_weight > 0.0:
        weighted_coverage = sum(
            float(member.get("weight") or 0.0) * float(output.get("feature_coverage") or 0.0)
            for member, output in zip(combined_members, member_outputs)
        ) / total_weight

    ensemble_members = []
    for member, output in zip(combined_members, member_outputs):
        payload = dict(member)
        payload["model_id"] = str(output.get("model_id") or payload.get("model_name") or "")
        payload["model_kind"] = output.get("model_kind")
        payload["feature_coverage"] = float(output.get("feature_coverage") or 0.0)
        payload["horizon_s"] = int(output.get("horizon_s") or 0)
        payload["regime_key"] = str(output.get("regime_key") or regime_signature(normalized_regime))
        ensemble_members.append(payload)

    epistemic = ensemble_epistemic_uncertainty({"ensemble_members": ensemble_members})
    resolved_horizon_s = int(horizon_s or member_outputs[0].get("horizon_s") or DEFAULT_HORIZON_S)
    ensemble_method = str(combined.get("method") or method or DEFAULT_ENSEMBLE_METHOD)
    feature_ids = list(member_outputs[0].get("feature_ids") or [])
    feature_set_tag = str(feature_snapshot.get("feature_set_tag") or member_outputs[0].get("feature_set_tag") or "")
    final_prediction = float(combined.get("final_prediction") or 0.0)
    aggregated_confidence = _clip_confidence(combined.get("aggregated_confidence"), default=SAFE_CONFIDENCE)
    component_vector = _component_vector_from_members(ensemble_members)
    model_version = _ensemble_model_version(ensemble_method, ensemble_members)
    output = {
        "symbol": str(symbol),
        "prediction": float(final_prediction),
        "confidence": float(aggregated_confidence),
        "prediction_strength": float(abs(final_prediction) * aggregated_confidence),
        "horizon_s": int(resolved_horizon_s),
        "model_name": f"ensemble_{ensemble_method}",
        "model_id": f"ensemble:{ensemble_method}:{symbol}:{len(ensemble_members)}",
        "model_version": model_version,
        "model_kind": "ensemble",
        "feature_ts_ms": int(feature_snapshot.get("ts_ms") or 0),
        "feature_set_tag": str(feature_set_tag),
        "feature_ids": feature_ids,
        "feature_coverage": float(weighted_coverage),
        "ts_ms": _now_ms(),
        "timed_out": False,
        "safe_output": False,
        "fallback_reason": None,
        "status": "ok",
        "ensemble_method": str(ensemble_method),
        "ensemble_size": int(len(ensemble_members)),
        "ensemble_members": ensemble_members,
        "component_vector": component_vector,
        "uncertainty_ts_ms": _now_ms(),
    }
    if bool(epistemic.get("available")):
        output["epistemic_uncertainty"] = float(epistemic.get("epistemic_uncertainty") or 0.0)
        output["ensemble_epistemic_uncertainty"] = float(epistemic.get("epistemic_uncertainty") or 0.0)
        output["uncertainty_detail"] = dict(epistemic)
    output["ensemble_output"] = _build_ensemble_decision_output(
        final_prediction=float(final_prediction),
        aggregated_confidence=float(aggregated_confidence),
        method=str(ensemble_method),
        members=ensemble_members,
        total_weight=float(total_weight),
        fallback=False,
        fallback_reason=(
            str(combined.get("fallback_reason"))
            if combined.get("fallback_reason") is not None
            else None
        ),
    )
    if bool(epistemic.get("available")):
        output["ensemble_output"]["epistemic_uncertainty"] = float(epistemic.get("epistemic_uncertainty") or 0.0)
        output["ensemble_output"]["uncertainty_detail"] = dict(epistemic)
    _track_prediction_output(
        output,
        feature_snapshot=feature_snapshot,
        metadata={
            "ensemble_method": str(ensemble_method),
            "ensemble_size": int(len(ensemble_members)),
            "regime": dict(normalized_regime),
            "regime_key": regime_signature(normalized_regime),
            "ensemble_members": [
                {
                    "model_name": str(member.get("model_name") or ""),
                    "model_version": str(member.get("model_version") or ""),
                    "weight": float(member.get("weight") or 0.0),
                }
                for member in ensemble_members
            ],
        },
    )
    return _attach_regime_fields(
        output,
        normalized_regime,
        symbol=str(symbol),
        ts_ms=int(feature_snapshot.get("ts_ms") or output.get("ts_ms") or _now_ms()),
    )


def _predict_records_parallel(
    symbol: str,
    *,
    records: Sequence[Mapping[str, Any]],
    feature_snapshot: Mapping[str, Any],
    requested_model_name: str | None = None,
    horizon_s: int | None = None,
    regime: Mapping[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not records:
        return [], []

    if len(records) == 1:
        output = _predict_record_output(
            symbol,
            record=records[0],
            feature_snapshot=feature_snapshot,
            requested_model_name=requested_model_name,
            horizon_s=horizon_s,
            regime=regime,
        )
        return [dict(records[0])], [dict(output)]

    prepared_records: list[tuple[int, dict[str, Any], Any]] = []
    for idx, raw_record in enumerate(records):
        record = dict(raw_record)
        try:
            artifact = _load_model_artifact(record)
        except Exception as exc:
            _warn_nonfatal(
                "INFERENCE_ENSEMBLE_MEMBER_FAILED",
                exc,
                once_key=f"inference_ensemble_member_failed:{symbol}:{record.get('model_name')}:{record.get('version')}",
                symbol=str(symbol),
                model_name=str(record.get("model_name") or ""),
                version=str(record.get("version") or ""),
            )
            continue
        prepared_records.append((idx, record, artifact))

    if not prepared_records:
        return [], []

    ordered_pairs: list[tuple[dict[str, Any], dict[str, Any]] | None] = [None] * len(records)
    max_workers = max(1, min(int(len(prepared_records)), int(_resolve_ensemble_parallel_workers())))
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="inference-ensemble",
    ) as executor:
        futures = {
            executor.submit(
                _predict_record_output,
                symbol,
                record=record,
                feature_snapshot=feature_snapshot,
                preloaded_artifact=artifact,
                requested_model_name=requested_model_name,
                horizon_s=horizon_s,
                regime=regime,
            ): (idx, record)
            for idx, record, artifact in prepared_records
        }
        for future in concurrent.futures.as_completed(futures):
            idx, record = futures[future]
            try:
                output = future.result()
            except Exception as exc:
                _warn_nonfatal(
                    "INFERENCE_ENSEMBLE_MEMBER_FAILED",
                    exc,
                    once_key=f"inference_ensemble_member_failed:{symbol}:{record.get('model_name')}:{record.get('version')}",
                    symbol=str(symbol),
                    model_name=str(record.get("model_name") or ""),
                    version=str(record.get("version") or ""),
                )
                continue
            ordered_pairs[idx] = (dict(record), dict(output))

    successful_records: list[dict[str, Any]] = []
    successful_outputs: list[dict[str, Any]] = []
    for pair in ordered_pairs:
        if pair is None:
            continue
        record, output = pair
        successful_records.append(record)
        successful_outputs.append(output)
    return successful_records, successful_outputs


def _predict_blocking(
    symbol: str,
    *,
    model_name: str | None = None,
    version: str | None = None,
    horizon_s: int | None = None,
    ensemble_enabled: bool = DEFAULT_ENSEMBLE_ENABLED,
    ensemble_method: str | None = None,
) -> Dict[str, Any]:
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return _safe_output(symbol="", horizon_s=int(horizon_s or DEFAULT_HORIZON_S), reason="missing_symbol")

    feature_snapshot = read_online_feature_snapshot(symbol_key)
    if int(feature_snapshot.get("ts_ms") or 0) <= 0:
        record_model_input_validation(
            {
                "ok": False,
                "status": "empty",
                "detail": "feature_store_empty",
                "symbol": str(symbol_key),
                "feature_ts_ms": int(feature_snapshot.get("ts_ms") or 0),
                "feature_set_tag": str(feature_snapshot.get("feature_set_tag") or ""),
                "shape_valid": False,
                "reason_codes": ["feature_store_empty"],
            }
        )
        return _safe_output(
            symbol=symbol_key,
            horizon_s=int(horizon_s or DEFAULT_HORIZON_S),
            reason="feature_store_empty",
            feature_snapshot=feature_snapshot,
            requested_model_name=model_name,
        )
    feature_validation = validate_online_feature_snapshot(feature_snapshot)
    if not bool(feature_validation.get("ok")):
        record_model_input_validation(
            {
                "ok": False,
                "status": str(feature_validation.get("status") or "invalid"),
                "detail": str(feature_validation.get("detail") or "feature_snapshot_invalid"),
                "symbol": str(symbol_key),
                "feature_ts_ms": int(feature_validation.get("feature_ts_ms") or 0),
                "feature_set_tag": str(feature_validation.get("feature_set_tag") or ""),
                "shape_valid": False,
                "stale": bool(feature_validation.get("stale")),
                "missing_feature_ids": list(feature_validation.get("missing_required_features") or []),
                "reason_codes": list(feature_validation.get("reason_codes") or []),
            }
        )
        return _safe_output(
            symbol=symbol_key,
            horizon_s=int(horizon_s or DEFAULT_HORIZON_S),
            reason=str(feature_validation.get("detail") or "feature_snapshot_invalid"),
            feature_snapshot=feature_snapshot,
            requested_model_name=model_name,
        )
    current_regime = resolve_regime_snapshot(
        symbol_key,
        feature_snapshot=feature_snapshot,
        target_time_ms=int(feature_snapshot.get("ts_ms") or 0),
        enqueue_refresh=True,
        allow_inline_fallback=False,
        source="inference_engine",
    )

    records = _resolve_model_records(
        symbol_key,
        model_name=model_name,
        version=version,
        ensemble_enabled=bool(ensemble_enabled),
        max_candidates=DEFAULT_ENSEMBLE_MODEL_LIMIT,
        current_regime=current_regime,
    )
    resolved_horizon_s = _resolve_horizon_s(records[0] if records else {}, horizon_s)
    if not records:
        return _safe_output(
            symbol=symbol_key,
            horizon_s=int(resolved_horizon_s),
            reason="model_registry_miss",
            feature_snapshot=feature_snapshot,
            requested_model_name=model_name,
            regime=current_regime,
        )

    if len(records) == 1:
        record = records[0]
        try:
            return _predict_record_output(
                symbol_key,
                record=record,
                feature_snapshot=feature_snapshot,
                requested_model_name=model_name,
                horizon_s=resolved_horizon_s,
                regime=current_regime,
            )
        except Exception as exc:
            _warn_nonfatal(
                "INFERENCE_PREDICT_FAILED",
                exc,
                once_key=f"inference_predict_failed:{symbol_key}:{record.get('model_name')}:{record.get('version')}",
                symbol=str(symbol_key),
                model_name=str(record.get("model_name") or ""),
                version=str(record.get("version") or ""),
            )
            if (model_name is None or not str(model_name).strip()) and version is None and not bool(ensemble_enabled):
                alternate_records = [
                    dict(candidate)
                    for candidate in _resolve_model_records(
                        symbol_key,
                        model_name=None,
                        version=None,
                        ensemble_enabled=True,
                        max_candidates=DEFAULT_ENSEMBLE_MODEL_LIMIT,
                        current_regime=current_regime,
                    )
                    if _record_lookup_key(candidate) != _record_lookup_key(record)
                ]
                for alternate_record in alternate_records:
                    try:
                        output = _predict_record_output(
                            symbol_key,
                            record=alternate_record,
                            feature_snapshot=feature_snapshot,
                            requested_model_name=model_name,
                            horizon_s=resolved_horizon_s,
                            regime=current_regime,
                        )
                    except Exception as alternate_exc:
                        _warn_nonfatal(
                            "INFERENCE_ALTERNATE_PREDICT_FAILED",
                            alternate_exc,
                            once_key=(
                                f"inference_alternate_predict_failed:{symbol_key}:"
                                f"{alternate_record.get('model_name')}:{alternate_record.get('version')}"
                            ),
                            symbol=str(symbol_key),
                            model_name=str(alternate_record.get("model_name") or ""),
                            version=str(alternate_record.get("version") or ""),
                            failed_primary_model_name=str(record.get("model_name") or ""),
                            failed_primary_version=str(record.get("version") or ""),
                        )
                        continue
                    ensemble_output = dict(output.get("ensemble_output") or {})
                    if ensemble_output:
                        ensemble_output["fallback"] = True
                        ensemble_output["fallback_reason"] = "primary_candidate_failed"
                        ensemble_output["attempted_size"] = int(len(alternate_records) + 1)
                        ensemble_output["recovered_from_model_name"] = str(record.get("model_name") or "")
                        ensemble_output["recovered_from_model_version"] = str(record.get("version") or "")
                        output["ensemble_output"] = ensemble_output
                    return output
            return _safe_output(
                symbol=symbol_key,
                horizon_s=int(resolved_horizon_s),
                reason=f"{type(exc).__name__}:{exc}",
                feature_snapshot=feature_snapshot,
                record=record,
                requested_model_name=model_name,
                regime=current_regime,
            )

    successful_records, successful_outputs = _predict_records_parallel(
        symbol_key,
        records=records,
        feature_snapshot=feature_snapshot,
        requested_model_name=model_name,
        horizon_s=resolved_horizon_s,
        regime=current_regime,
    )

    if not successful_outputs:
        return _safe_output(
            symbol=symbol_key,
            horizon_s=int(resolved_horizon_s),
            reason="ensemble_members_failed",
            feature_snapshot=feature_snapshot,
            record=records[0],
            requested_model_name=model_name,
            regime=current_regime,
        )

    if len(successful_outputs) == 1:
        output = dict(successful_outputs[0])
        ensemble_output = dict(output.get("ensemble_output") or {})
        if ensemble_output:
            ensemble_output["fallback"] = True
            ensemble_output["fallback_reason"] = "ensemble_degraded_to_single_member"
            ensemble_output["attempted_size"] = int(len(records))
            output["ensemble_output"] = ensemble_output
        return output

    try:
        return _build_ensemble_output(
            symbol_key,
            member_records=successful_records,
            member_outputs=successful_outputs,
            feature_snapshot=feature_snapshot,
            method=str(ensemble_method or DEFAULT_ENSEMBLE_METHOD),
            horizon_s=resolved_horizon_s,
            regime=current_regime,
        )
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_ENSEMBLE_COMBINE_FAILED",
            exc,
            once_key=f"inference_ensemble_combine_failed:{symbol_key}",
            symbol=str(symbol_key),
            ensemble_method=str(ensemble_method or DEFAULT_ENSEMBLE_METHOD),
            member_count=int(len(successful_outputs)),
        )
        return _attach_regime_fields(
            dict(successful_outputs[0]),
            current_regime,
            symbol=str(symbol_key),
            ts_ms=int(feature_snapshot.get("ts_ms") or _now_ms()),
        )


def _run_async_blocking(factory: Callable[[], Awaitable[Any]]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(factory())

    result: dict[str, Any] = {}
    error: dict[str, BaseException] = {}
    done = threading.Event()

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(factory())
        except BaseException as exc:  # pragma: no cover - sync wrapper only used under active event loop
            error["error"] = exc
        finally:
            done.set()

    thread = threading.Thread(target=_runner, name="inference-engine-sync", daemon=True)
    thread.start()
    done.wait()
    if "error" in error:
        raise error["error"]
    return result.get("value")


class InferenceEngine:
    """Resolve model artifacts, run predictions, and degrade safely on failure."""

    def __init__(
        self,
        *,
        default_timeout_s: float = DEFAULT_TIMEOUT_S,
        batch_concurrency: int = DEFAULT_BATCH_CONCURRENCY,
        default_horizon_s: int = DEFAULT_HORIZON_S,
        ensemble_enabled: bool = DEFAULT_ENSEMBLE_ENABLED,
        ensemble_method: str = DEFAULT_ENSEMBLE_METHOD,
        persist_predictions: bool = True,
    ) -> None:
        self.default_timeout_s = max(0.05, float(default_timeout_s))
        self.batch_concurrency = max(1, int(batch_concurrency))
        self.default_horizon_s = max(1, int(default_horizon_s))
        self.ensemble_enabled = bool(ensemble_enabled)
        self.ensemble_method = str(ensemble_method or DEFAULT_ENSEMBLE_METHOD).strip() or DEFAULT_ENSEMBLE_METHOD
        self.persist_predictions = bool(persist_predictions)

    async def predict_async(
        self,
        symbol: str,
        *,
        model_name: str | None = None,
        version: str | None = None,
        horizon_s: int | None = None,
        timeout_s: float | None = None,
        ensemble_enabled: bool | None = None,
        ensemble_method: str | None = None,
        persist: bool | None = None,
    ) -> Dict[str, Any]:
        """Predict one symbol asynchronously and optionally persist the output."""
        symbol_key = _normalize_symbol(symbol)
        effective_timeout_s = max(0.05, float(timeout_s if timeout_s is not None else self.default_timeout_s))
        effective_horizon_s = int(horizon_s or self.default_horizon_s)
        explicit_ensemble_method = str(ensemble_method or "").strip()
        effective_ensemble_enabled = self.ensemble_enabled if ensemble_enabled is None else bool(ensemble_enabled)
        if (
            ensemble_enabled is None
            and explicit_ensemble_method
            and not str(model_name or "").strip()
            and not str(version or "").strip()
        ):
            effective_ensemble_enabled = True
        effective_ensemble_method = str(explicit_ensemble_method or self.ensemble_method or DEFAULT_ENSEMBLE_METHOD).strip()
        effective_ensemble_method = effective_ensemble_method or DEFAULT_ENSEMBLE_METHOD
        should_persist = self.persist_predictions if persist is None else bool(persist)
        started = time.perf_counter()

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    _predict_blocking,
                    symbol_key,
                    model_name=model_name,
                    version=version,
                    horizon_s=effective_horizon_s,
                    ensemble_enabled=effective_ensemble_enabled,
                    ensemble_method=effective_ensemble_method,
                ),
                timeout=effective_timeout_s,
                )
        except asyncio.TimeoutError:
            result = _safe_output(
                symbol=symbol_key,
                horizon_s=int(effective_horizon_s),
                reason=f"timeout:{effective_timeout_s:.3f}s",
                timed_out=True,
                requested_model_name=model_name,
            )
        except Exception as exc:  # pragma: no cover - _predict_blocking already degrades safely
            _warn_nonfatal(
                "INFERENCE_ASYNC_EXECUTION_FAILED",
                exc,
                once_key=f"inference_async_execution_failed:{symbol_key}",
                symbol=str(symbol_key),
            )
            result = _safe_output(
                symbol=symbol_key,
                horizon_s=int(effective_horizon_s),
                reason=f"{type(exc).__name__}:{exc}",
                requested_model_name=model_name,
            )

        if should_persist:
            try:
                await asyncio.to_thread(_persist_prediction_output, result)
            except Exception as exc:
                _warn_nonfatal(
                    "INFERENCE_PERSIST_FAILED",
                    exc,
                    once_key=f"inference_persist_failed:{symbol_key}",
                    symbol=str(symbol_key),
                    model_name=str(result.get("model_name") or ""),
                )
        latency_ms = int(round((time.perf_counter() - started) * 1000.0))
        safe_output = bool(result.get("safe_output"))
        status = str(result.get("status") or ("safe_default" if safe_output else "ok"))
        fallback_reason = str(result.get("fallback_reason") or "")
        emit_counter(
            "inference_requests",
            1,
            component="engine.inference_engine",
            symbol=str(symbol_key),
            extra_tags={
                "status": str(status),
                "safe_output": int(safe_output),
                "model_name": str(result.get("model_name") or model_name or ""),
            },
        )
        emit_timing(
            "inference_latency_ms",
            int(latency_ms),
            component="engine.inference_engine",
            symbol=str(symbol_key),
            extra_tags={
                "status": str(status),
                "safe_output": int(safe_output),
            },
        )
        record_component_health(
            "inference",
            ok=(not safe_output and status == "ok"),
            status=str(status),
            detail=str(fallback_reason or "ok"),
            latency_ms=float(latency_ms),
            extra={
                "symbol": str(symbol_key),
                "model_name": str(result.get("model_name") or model_name or ""),
                "model_version": result.get("model_version"),
                "safe_output": bool(safe_output),
                "timed_out": bool(result.get("timed_out")),
                "last_prediction_ts_ms": int(result.get("ts_ms") or _now_ms()),
            },
        )
        try:
            record_scoring_pipeline(
                _build_scoring_pipeline_payload(
                    result,
                    symbol=str(symbol_key),
                    requested_model_name=model_name,
                )
            )
        except Exception as exc:
            _warn_nonfatal(
                "INFERENCE_SCORING_PIPELINE_HEALTH_FAILED",
                exc,
                once_key=f"inference_scoring_pipeline_health_failed:{symbol_key}",
                symbol=str(symbol_key),
                model_name=str(result.get("model_name") or model_name or ""),
            )
        log_event(
            LOG,
            logging.INFO if (not safe_output and status == "ok") else logging.WARNING,
            "inference_completed",
            component="engine.inference_engine",
            extra={
                "symbol": str(symbol_key),
                "status": str(status),
                "safe_output": bool(safe_output),
                "timed_out": bool(result.get("timed_out")),
                "fallback_reason": str(fallback_reason or ""),
                "latency_ms": int(latency_ms),
                "model_name": str(result.get("model_name") or model_name or ""),
                "model_version": result.get("model_version"),
                "ensemble_size": int(result.get("ensemble_size") or 0),
            },
        )
        return dict(result)

    async def batch_predict_async(
        self,
        symbols: Sequence[str],
        *,
        model_name: str | None = None,
        version: str | None = None,
        horizon_s: int | None = None,
        timeout_s: float | None = None,
        ensemble_enabled: bool | None = None,
        ensemble_method: str | None = None,
        persist: bool | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Predict a batch of symbols concurrently with bounded fanout."""
        normalized_symbols = [_normalize_symbol(symbol) for symbol in symbols or [] if _normalize_symbol(symbol)]
        if not normalized_symbols:
            return {}

        semaphore = asyncio.Semaphore(self.batch_concurrency)

        async def _predict_one(symbol_key: str) -> tuple[str, Dict[str, Any]]:
            async with semaphore:
                return symbol_key, await self.predict_async(
                    symbol_key,
                    model_name=model_name,
                    version=version,
                    horizon_s=horizon_s,
                    timeout_s=timeout_s,
                    ensemble_enabled=ensemble_enabled,
                    ensemble_method=ensemble_method,
                    persist=persist,
                )

        pairs = await asyncio.gather(*(_predict_one(symbol_key) for symbol_key in normalized_symbols))
        return {str(symbol_key): dict(payload) for symbol_key, payload in pairs}

    def predict(
        self,
        symbol: str,
        *,
        model_name: str | None = None,
        version: str | None = None,
        horizon_s: int | None = None,
        timeout_s: float | None = None,
        ensemble_enabled: bool | None = None,
        ensemble_method: str | None = None,
        persist: bool | None = None,
    ) -> Dict[str, Any]:
        """Synchronously predict one symbol using the async engine under the hood."""
        return dict(
            _run_async_blocking(
                lambda: self.predict_async(
                    symbol,
                    model_name=model_name,
                    version=version,
                    horizon_s=horizon_s,
                    timeout_s=timeout_s,
                    ensemble_enabled=ensemble_enabled,
                    ensemble_method=ensemble_method,
                    persist=persist,
                )
            )
        )

    def batch_predict(
        self,
        symbols: Sequence[str],
        *,
        model_name: str | None = None,
        version: str | None = None,
        horizon_s: int | None = None,
        timeout_s: float | None = None,
        ensemble_enabled: bool | None = None,
        ensemble_method: str | None = None,
        persist: bool | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Synchronously predict multiple symbols with the configured batch fanout."""
        return dict(
            _run_async_blocking(
                lambda: self.batch_predict_async(
                    symbols,
                    model_name=model_name,
                    version=version,
                    horizon_s=horizon_s,
                    timeout_s=timeout_s,
                    ensemble_enabled=ensemble_enabled,
                    ensemble_method=ensemble_method,
                    persist=persist,
                )
            )
        )


DEFAULT_ENGINE = InferenceEngine()


def predict(
    symbol: str,
    *,
    model_name: str | None = None,
    version: str | None = None,
    horizon_s: int | None = None,
    timeout_s: float | None = None,
    ensemble_enabled: bool | None = None,
    ensemble_method: str | None = None,
    persist: bool | None = None,
) -> Dict[str, Any]:
    """Synchronously predict one symbol with the default inference engine."""
    return DEFAULT_ENGINE.predict(
        symbol,
        model_name=model_name,
        version=version,
        horizon_s=horizon_s,
        timeout_s=timeout_s,
        ensemble_enabled=ensemble_enabled,
        ensemble_method=ensemble_method,
        persist=persist,
    )


async def predict_async(
    symbol: str,
    *,
    model_name: str | None = None,
    version: str | None = None,
    horizon_s: int | None = None,
    timeout_s: float | None = None,
    ensemble_enabled: bool | None = None,
    ensemble_method: str | None = None,
    persist: bool | None = None,
) -> Dict[str, Any]:
    """Asynchronously predict one symbol with the default inference engine."""
    return await DEFAULT_ENGINE.predict_async(
        symbol,
        model_name=model_name,
        version=version,
        horizon_s=horizon_s,
        timeout_s=timeout_s,
        ensemble_enabled=ensemble_enabled,
        ensemble_method=ensemble_method,
        persist=persist,
    )


def batch_predict(
    symbols: Sequence[str],
    *,
    model_name: str | None = None,
    version: str | None = None,
    horizon_s: int | None = None,
    timeout_s: float | None = None,
    ensemble_enabled: bool | None = None,
    ensemble_method: str | None = None,
    persist: bool | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Synchronously predict multiple symbols with the default inference engine."""
    return DEFAULT_ENGINE.batch_predict(
        symbols,
        model_name=model_name,
        version=version,
        horizon_s=horizon_s,
        timeout_s=timeout_s,
        ensemble_enabled=ensemble_enabled,
        ensemble_method=ensemble_method,
        persist=persist,
    )


async def batch_predict_async(
    symbols: Sequence[str],
    *,
    model_name: str | None = None,
    version: str | None = None,
    horizon_s: int | None = None,
    timeout_s: float | None = None,
    ensemble_enabled: bool | None = None,
    ensemble_method: str | None = None,
    persist: bool | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Asynchronously predict multiple symbols with the default inference engine."""
    return await DEFAULT_ENGINE.batch_predict_async(
        symbols,
        model_name=model_name,
        version=version,
        horizon_s=horizon_s,
        timeout_s=timeout_s,
        ensemble_enabled=ensemble_enabled,
        ensemble_method=ensemble_method,
        persist=persist,
    )


__all__ = [
    "InferenceEngine",
    "DEFAULT_ENGINE",
    "predict",
    "predict_async",
    "batch_predict",
    "batch_predict_async",
]
