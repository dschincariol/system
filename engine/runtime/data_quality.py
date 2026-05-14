from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Mapping, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.observability import record_component_health
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime.ingestion_status import get_pipeline_status

LOG = get_logger("engine.runtime.data_quality")
_WARNED_NONFATAL_KEYS: set[str] = set()
_STATE_PREFIX = "data_quality::"

FEATURE_VALIDATION_MAX_AGE_S = max(
    1.0,
    float(
        os.environ.get(
            "FEATURE_VALIDATION_MAX_AGE_S",
            os.environ.get("HEALTH_RUNTIME_PRICE_CACHE_MAX_AGE_S", "120"),
        )
    ),
)
MODEL_INPUT_VALIDATION_MAX_AGE_S = max(
    1.0,
    float(
        os.environ.get(
            "MODEL_INPUT_VALIDATION_MAX_AGE_S",
            os.environ.get("FEATURE_VALIDATION_MAX_AGE_S", str(FEATURE_VALIDATION_MAX_AGE_S)),
        )
    ),
)
SCORING_PIPELINE_MAX_AGE_S = max(
    1.0,
    float(
        os.environ.get(
            "SCORING_PIPELINE_MAX_AGE_S",
            os.environ.get("HEALTH_PREDICTIONS_MAX_AGE_S", "600"),
        )
    ),
)
INGESTION_ACTIVE_MAX_AGE_S = max(
    1.0,
    float(os.environ.get("DATA_GATE_INGESTION_ACTIVE_MAX_AGE_S", "90")),
)
INGESTION_NOT_STALE_MAX_AGE_S = max(
    1.0,
    float(
        os.environ.get(
            "DATA_GATE_INGESTION_NOT_STALE_MAX_AGE_S",
            os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120"),
        )
    ),
)


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
        component="engine.runtime.data_quality",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _state_key(name: str) -> str:
    return f"{_STATE_PREFIX}{str(name or '').strip().lower()}"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_json_dict(raw: Any) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception as exc:
        _warn_nonfatal(
            "DATA_QUALITY_JSON_PARSE_FAILED",
            exc,
            once_key=f"data_quality_json_parse_failed:{text[:80]}",
        )
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _load_state(name: str) -> Dict[str, Any]:
    return _safe_json_dict(meta_get(_state_key(name), ""))


def _write_state(name: str, payload: Mapping[str, Any]) -> Dict[str, Any]:
    state = dict(payload or {})
    meta_set(
        _state_key(name),
        json.dumps(state, separators=(",", ":"), sort_keys=True),
        best_effort=True,
    )
    return state


def _reason_codes(payload: Mapping[str, Any]) -> list[str]:
    values = payload.get("reason_codes")
    if not isinstance(values, list):
        return []
    return [
        str(value or "").strip()
        for value in values
        if str(value or "").strip()
    ]


def _normalize_status(payload: Mapping[str, Any], *, ok: bool, default_failure_status: str) -> str:
    status = str(payload.get("status") or "").strip().lower()
    if status:
        return status
    return "ok" if ok else str(default_failure_status)


def record_feature_validation(payload: Mapping[str, Any]) -> Dict[str, Any]:
    previous = _load_state("feature_validation")
    now_ms = _now_ms()
    ok = bool(payload.get("ok"))
    state = {
        "updated_ts_ms": int(now_ms),
        "validated_ts_ms": int(payload.get("validated_ts_ms") or now_ms),
        "last_success_ts_ms": (
            int(payload.get("validated_ts_ms") or now_ms)
            if ok
            else int(previous.get("last_success_ts_ms") or 0)
        ),
        "last_failure_ts_ms": (
            int(previous.get("last_failure_ts_ms") or 0)
            if ok
            else int(payload.get("validated_ts_ms") or now_ms)
        ),
        "ok": bool(ok),
        "status": _normalize_status(payload, ok=ok, default_failure_status="invalid"),
        "detail": str(payload.get("detail") or ""),
        "symbol": str(payload.get("symbol") or "").upper().strip(),
        "feature_ts_ms": int(payload.get("feature_ts_ms") or 0),
        "feature_set_tag": str(payload.get("feature_set_tag") or ""),
        "schema_version": int(payload.get("schema_version") or 0),
        "point_count": int(payload.get("point_count") or 0),
        "feature_count": int(payload.get("feature_count") or 0),
        "vector_size": int(payload.get("vector_size") or 0),
        "stale": bool(payload.get("stale")),
        "age_ms": int(payload.get("age_ms") or 0),
        "missing_required_features": [
            str(value)
            for value in list(payload.get("missing_required_features") or [])
            if str(value or "").strip()
        ],
        "invalid_feature_ids": [
            str(value)
            for value in list(payload.get("invalid_feature_ids") or [])
            if str(value or "").strip()
        ],
        "reason_codes": _reason_codes(payload),
        "invalid_count_total": int(previous.get("invalid_count_total") or 0) + (0 if ok else 1),
        "success_count_total": int(previous.get("success_count_total") or 0) + (1 if ok else 0),
    }
    saved = _write_state("feature_validation", state)
    record_component_health(
        "feature_engine",
        ok=bool(ok),
        status=str(saved.get("status") or ("ok" if ok else "invalid")),
        detail=str(saved.get("detail") or ""),
        observed_ts_ms=int(saved.get("validated_ts_ms") or now_ms),
        extra={
            "symbol": str(saved.get("symbol") or ""),
            "feature_ts_ms": int(saved.get("feature_ts_ms") or 0),
            "missing_required_features": list(saved.get("missing_required_features") or []),
            "invalid_feature_ids": list(saved.get("invalid_feature_ids") or []),
            "stale": bool(saved.get("stale")),
            "last_success_ts_ms": int(saved.get("last_success_ts_ms") or 0),
        },
    )
    return saved


def get_feature_validation_snapshot() -> Dict[str, Any]:
    return _load_state("feature_validation")


def record_model_input_validation(payload: Mapping[str, Any]) -> Dict[str, Any]:
    previous = _load_state("model_input_validation")
    now_ms = _now_ms()
    ok = bool(payload.get("ok"))
    state = {
        "updated_ts_ms": int(now_ms),
        "validated_ts_ms": int(payload.get("validated_ts_ms") or now_ms),
        "last_success_ts_ms": (
            int(payload.get("validated_ts_ms") or now_ms)
            if ok
            else int(previous.get("last_success_ts_ms") or 0)
        ),
        "last_failure_ts_ms": (
            int(previous.get("last_failure_ts_ms") or 0)
            if ok
            else int(payload.get("validated_ts_ms") or now_ms)
        ),
        "ok": bool(ok),
        "status": _normalize_status(payload, ok=ok, default_failure_status="invalid"),
        "detail": str(payload.get("detail") or ""),
        "symbol": str(payload.get("symbol") or "").upper().strip(),
        "model_name": str(payload.get("model_name") or ""),
        "model_version": str(payload.get("model_version") or ""),
        "model_kind": str(payload.get("model_kind") or ""),
        "feature_ts_ms": int(payload.get("feature_ts_ms") or 0),
        "feature_set_tag": str(payload.get("feature_set_tag") or ""),
        "expected_feature_count": int(payload.get("expected_feature_count") or 0),
        "actual_feature_count": int(payload.get("actual_feature_count") or 0),
        "feature_coverage": float(payload.get("feature_coverage") or 0.0),
        "missing_feature_ids": [
            str(value)
            for value in list(payload.get("missing_feature_ids") or [])
            if str(value or "").strip()
        ],
        "schema_mismatch": bool(payload.get("schema_mismatch")),
        "shape_valid": bool(payload.get("shape_valid", ok)),
        "stale": bool(payload.get("stale")),
        "reason_codes": _reason_codes(payload),
        "invalid_count_total": int(previous.get("invalid_count_total") or 0) + (0 if ok else 1),
        "success_count_total": int(previous.get("success_count_total") or 0) + (1 if ok else 0),
    }
    saved = _write_state("model_input_validation", state)
    record_component_health(
        "model_inputs",
        ok=bool(ok),
        status=str(saved.get("status") or ("ok" if ok else "invalid")),
        detail=str(saved.get("detail") or ""),
        observed_ts_ms=int(saved.get("validated_ts_ms") or now_ms),
        extra={
            "symbol": str(saved.get("symbol") or ""),
            "model_name": str(saved.get("model_name") or ""),
            "model_version": str(saved.get("model_version") or ""),
            "feature_ts_ms": int(saved.get("feature_ts_ms") or 0),
            "feature_coverage": float(saved.get("feature_coverage") or 0.0),
            "missing_feature_ids": list(saved.get("missing_feature_ids") or []),
            "schema_mismatch": bool(saved.get("schema_mismatch")),
            "shape_valid": bool(saved.get("shape_valid")),
        },
    )
    return saved


def get_model_input_validation_snapshot() -> Dict[str, Any]:
    return _load_state("model_input_validation")


def record_scoring_pipeline(payload: Mapping[str, Any]) -> Dict[str, Any]:
    previous = _load_state("scoring_pipeline")
    now_ms = _now_ms()
    ok = bool(payload.get("ok"))
    safe_output = bool(payload.get("safe_output"))
    state = {
        "updated_ts_ms": int(now_ms),
        "attempt_ts_ms": int(payload.get("attempt_ts_ms") or now_ms),
        "last_success_ts_ms": (
            int(payload.get("attempt_ts_ms") or now_ms)
            if ok
            else int(previous.get("last_success_ts_ms") or 0)
        ),
        "last_failure_ts_ms": (
            int(previous.get("last_failure_ts_ms") or 0)
            if ok
            else int(payload.get("attempt_ts_ms") or now_ms)
        ),
        "ok": bool(ok),
        "status": _normalize_status(payload, ok=ok, default_failure_status="failed"),
        "detail": str(payload.get("detail") or ""),
        "symbol": str(payload.get("symbol") or "").upper().strip(),
        "model_name": str(payload.get("model_name") or ""),
        "model_version": str(payload.get("model_version") or ""),
        "model_kind": str(payload.get("model_kind") or ""),
        "model_loaded": bool(payload.get("model_loaded")),
        "prediction": _safe_float(payload.get("prediction"), 0.0),
        "confidence": _safe_float(payload.get("confidence"), 0.0),
        "feature_ts_ms": int(payload.get("feature_ts_ms") or 0),
        "prediction_ts_ms": int(payload.get("prediction_ts_ms") or 0),
        "safe_output": bool(safe_output),
        "fallback_reason": str(payload.get("fallback_reason") or ""),
        "config_variant": str(payload.get("config_variant") or ""),
        "reason_codes": _reason_codes(payload),
        "invalid_input_count_total": int(previous.get("invalid_input_count_total") or 0) + int(
            payload.get("invalid_input_delta") or 0
        ),
        "fallback_count_total": int(previous.get("fallback_count_total") or 0) + (1 if safe_output else 0),
        "success_count_total": int(previous.get("success_count_total") or 0) + (1 if ok else 0),
    }
    saved = _write_state("scoring_pipeline", state)
    record_component_health(
        "scoring_pipeline",
        ok=bool(ok),
        status=str(saved.get("status") or ("ok" if ok else "failed")),
        detail=str(saved.get("detail") or ""),
        observed_ts_ms=int(saved.get("attempt_ts_ms") or now_ms),
        extra={
            "symbol": str(saved.get("symbol") or ""),
            "model_name": str(saved.get("model_name") or ""),
            "model_version": str(saved.get("model_version") or ""),
            "model_loaded": bool(saved.get("model_loaded")),
            "safe_output": bool(saved.get("safe_output")),
            "fallback_reason": str(saved.get("fallback_reason") or ""),
            "last_success_ts_ms": int(saved.get("last_success_ts_ms") or 0),
        },
    )
    return saved


def get_scoring_pipeline_snapshot() -> Dict[str, Any]:
    return _load_state("scoring_pipeline")


def _gate_payload(
    name: str,
    *,
    passed: bool,
    criteria: str,
    detail: str,
    reason_codes: list[str] | None = None,
    **extra: Any,
) -> Dict[str, Any]:
    return {
        "name": str(name),
        "ok": bool(passed),
        "criteria": str(criteria),
        "detail": str(detail),
        "reason_codes": [
            str(code)
            for code in list(reason_codes or [])
            if str(code or "").strip()
        ],
        **dict(extra or {}),
    }


def build_data_pipeline_gate_snapshot(
    *,
    now_ms: Optional[int] = None,
    ingestion_runtime: Optional[Mapping[str, Any]] = None,
    ingestion_freshness: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    current_ts_ms = int(now_ms or _now_ms())
    poll_prices = dict(get_pipeline_status("poll_prices") or {})
    stream_polygon = dict(get_pipeline_status("stream_prices_polygon_ws") or {})
    stream_ibkr = dict(get_pipeline_status("stream_prices_ibkr") or {})
    critical_pipelines = [
        ("poll_prices", poll_prices),
        ("stream_prices_polygon_ws", stream_polygon),
        ("stream_prices_ibkr", stream_ibkr),
    ]

    freshest_activity_ts_ms = max(
        [
            _safe_int(row.get("last_success_ts_ms") or row.get("last_ingested_ts_ms") or row.get("updated_ts_ms"), 0)
            for _name, row in critical_pipelines
        ]
        + [_safe_int((ingestion_runtime or {}).get("last_publish_ts_ms"), 0)]
    )
    activity_age_s = (
        None
        if freshest_activity_ts_ms <= 0
        else round(max(0, current_ts_ms - freshest_activity_ts_ms) / 1000.0, 1)
    )
    active_pipeline_names = [
        name
        for name, row in critical_pipelines
        if bool(row.get("ok"))
        and _safe_int(row.get("last_success_ts_ms") or row.get("last_ingested_ts_ms") or row.get("updated_ts_ms"), 0) > 0
        and (current_ts_ms - _safe_int(row.get("last_success_ts_ms") or row.get("last_ingested_ts_ms") or row.get("updated_ts_ms"), 0))
        <= int(INGESTION_ACTIVE_MAX_AGE_S * 1000.0)
    ]
    ingestion_running = bool((ingestion_runtime or {}).get("running"))
    ingestion_active = bool(ingestion_running or active_pipeline_names)

    freshness_payload = dict(ingestion_freshness or {})
    critical_ingestion_ok = bool(freshness_payload.get("critical_ok")) if freshness_payload else (
        freshest_activity_ts_ms > 0
        and (current_ts_ms - freshest_activity_ts_ms) <= int(INGESTION_NOT_STALE_MAX_AGE_S * 1000.0)
    )
    stale_critical_sources = [
        str(value)
        for value in list(freshness_payload.get("stale_critical_sources") or [])
        if str(value or "").strip()
    ]

    feature_state = get_feature_validation_snapshot()
    feature_age_s = None
    feature_validated_ts_ms = _safe_int(feature_state.get("validated_ts_ms"), 0)
    if feature_validated_ts_ms > 0:
        feature_age_s = round(max(0, current_ts_ms - feature_validated_ts_ms) / 1000.0, 1)
    feature_gate_ok = bool(feature_state.get("ok")) and feature_validated_ts_ms > 0 and (
        (current_ts_ms - feature_validated_ts_ms) <= int(FEATURE_VALIDATION_MAX_AGE_S * 1000.0)
    )

    model_input_state = get_model_input_validation_snapshot()
    model_input_age_s = None
    model_input_validated_ts_ms = _safe_int(model_input_state.get("validated_ts_ms"), 0)
    if model_input_validated_ts_ms > 0:
        model_input_age_s = round(max(0, current_ts_ms - model_input_validated_ts_ms) / 1000.0, 1)
    model_input_gate_ok = bool(model_input_state.get("ok")) and model_input_validated_ts_ms > 0 and (
        (current_ts_ms - model_input_validated_ts_ms) <= int(MODEL_INPUT_VALIDATION_MAX_AGE_S * 1000.0)
    )

    scoring_state = get_scoring_pipeline_snapshot()
    scoring_success_ts_ms = _safe_int(scoring_state.get("last_success_ts_ms"), 0)
    scoring_age_s = None
    if scoring_success_ts_ms > 0:
        scoring_age_s = round(max(0, current_ts_ms - scoring_success_ts_ms) / 1000.0, 1)
    scoring_gate_ok = (
        bool(scoring_state.get("ok"))
        and bool(scoring_state.get("model_loaded"))
        and not bool(scoring_state.get("safe_output"))
        and scoring_success_ts_ms > 0
        and (current_ts_ms - scoring_success_ts_ms) <= int(SCORING_PIPELINE_MAX_AGE_S * 1000.0)
    )

    gates = {
        "ingestion_active": _gate_payload(
            "ingestion_active",
            passed=bool(ingestion_active),
            criteria=(
                f"Pass when ingestion is running or a critical market-data pipeline reports a successful update "
                f"within {round(INGESTION_ACTIVE_MAX_AGE_S, 1)}s."
            ),
            detail=(
                "ok"
                if ingestion_active
                else "no_fresh_critical_ingestion_pipeline"
            ),
            reason_codes=[] if ingestion_active else ["ingestion_inactive"],
            running=bool(ingestion_running),
            active_pipelines=list(active_pipeline_names),
            freshest_activity_ts_ms=(int(freshest_activity_ts_ms) if freshest_activity_ts_ms > 0 else None),
            freshest_activity_age_s=activity_age_s,
        ),
        "ingestion_not_stale": _gate_payload(
            "ingestion_not_stale",
            passed=bool(critical_ingestion_ok),
            criteria=(
                f"Pass when critical ingestion sources have not breached the freshness window of "
                f"{round(INGESTION_NOT_STALE_MAX_AGE_S, 1)}s."
            ),
            detail=("ok" if critical_ingestion_ok else "critical_ingestion_source_stale"),
            reason_codes=(
                list(freshness_payload.get("runtime_reason_codes") or [])
                or ["critical_ingestion_source_stale"]
            ),
            stale_critical_sources=list(stale_critical_sources),
        ),
        "critical_features_valid": _gate_payload(
            "critical_features_valid",
            passed=bool(feature_gate_ok),
            criteria=(
                f"Pass when the latest live feature validation succeeded and is not older than "
                f"{round(FEATURE_VALIDATION_MAX_AGE_S, 1)}s."
            ),
            detail=(str(feature_state.get("detail") or "ok") if feature_state else "feature_validation_missing"),
            reason_codes=(
                list(feature_state.get("reason_codes") or [])
                or (["feature_validation_missing"] if not feature_state else [])
            ),
            validated_ts_ms=(int(feature_validated_ts_ms) if feature_validated_ts_ms > 0 else None),
            age_s=feature_age_s,
            symbol=str(feature_state.get("symbol") or ""),
            missing_required_features=list(feature_state.get("missing_required_features") or []),
            invalid_feature_ids=list(feature_state.get("invalid_feature_ids") or []),
        ),
        "model_inputs_valid": _gate_payload(
            "model_inputs_valid",
            passed=bool(model_input_gate_ok),
            criteria=(
                f"Pass when the latest model-input validation succeeded and is not older than "
                f"{round(MODEL_INPUT_VALIDATION_MAX_AGE_S, 1)}s."
            ),
            detail=(
                str(model_input_state.get("detail") or "ok")
                if model_input_state
                else "model_input_validation_missing"
            ),
            reason_codes=(
                list(model_input_state.get("reason_codes") or [])
                or (["model_input_validation_missing"] if not model_input_state else [])
            ),
            validated_ts_ms=(int(model_input_validated_ts_ms) if model_input_validated_ts_ms > 0 else None),
            age_s=model_input_age_s,
            symbol=str(model_input_state.get("symbol") or ""),
            model_name=str(model_input_state.get("model_name") or ""),
            missing_feature_ids=list(model_input_state.get("missing_feature_ids") or []),
            feature_coverage=float(model_input_state.get("feature_coverage") or 0.0),
        ),
        "scoring_pipeline_operational": _gate_payload(
            "scoring_pipeline_operational",
            passed=bool(scoring_gate_ok),
            criteria=(
                f"Pass when scoring produced a non-fallback output from a loaded model within "
                f"{round(SCORING_PIPELINE_MAX_AGE_S, 1)}s."
            ),
            detail=(
                str(scoring_state.get("detail") or "ok")
                if scoring_state
                else "scoring_pipeline_unreported"
            ),
            reason_codes=(
                list(scoring_state.get("reason_codes") or [])
                or (["scoring_pipeline_unreported"] if not scoring_state else [])
            ),
            last_success_ts_ms=(int(scoring_success_ts_ms) if scoring_success_ts_ms > 0 else None),
            age_s=scoring_age_s,
            symbol=str(scoring_state.get("symbol") or ""),
            model_name=str(scoring_state.get("model_name") or ""),
            model_loaded=bool(scoring_state.get("model_loaded")),
            safe_output=bool(scoring_state.get("safe_output")),
            fallback_reason=str(scoring_state.get("fallback_reason") or ""),
        ),
    }

    failed = [name for name, gate in gates.items() if not bool(gate.get("ok"))]
    return {
        "ok": len(failed) == 0,
        "updated_ts_ms": int(current_ts_ms),
        "gates": gates,
        "failed_gates": failed,
    }
