"""
FILE: lifecycle_state.py

Runtime subsystem module for `lifecycle_state`.
"""

import logging
import os
import time
from typing import Any, Dict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime.state_cache import cache_invalidate_namespace
from engine.runtime.logging import get_logger
from engine.runtime.event_log import record_lifecycle_event
from engine.runtime.metrics import emit_counter, emit_gauge
from engine.runtime.tracing import trace_event

_logger = get_logger("lifecycle")


BOOTING = "BOOTING"
SCHEMA_REPAIR = "SCHEMA_REPAIR"
WARMING_UP = "WARMING_UP"
LIVE = "LIVE"
DEGRADED = "DEGRADED"
KILL_SWITCH = "KILL_SWITCH"
SHUTTING_DOWN = "SHUTTING_DOWN"

_ALLOWED = {
    BOOTING,
    SCHEMA_REPAIR,
    WARMING_UP,
    LIVE,
    DEGRADED,
    KILL_SWITCH,
    SHUTTING_DOWN,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_state(state: Any) -> str:
    s = str(state or "").strip().upper()
    if s == "WARMING":
        s = WARMING_UP
    elif s == "KILL":
        s = KILL_SWITCH
    elif s in ("SHUTDOWN", "SHUTTING"):
        s = SHUTTING_DOWN
    if s not in _ALLOWED:
        s = DEGRADED
    return s


_WARMUP_TIMEOUT_DETAILS = {
    "warmup_timeout_awaiting_first_price_tick",
}


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        _logger,
        event="lifecycle_state_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.lifecycle_state",
        extra=extra or None,
        persist=False,
    )


def get_state() -> Dict[str, Any]:
    state = _normalize_state(meta_get("lifecycle_state", BOOTING))
    detail = str(meta_get("lifecycle_detail", "") or "")
    first_price_ts_ms = str(meta_get("first_price_ts_ms", "") or "").strip()
    previous_state = str(meta_get("lifecycle_prev_state", "") or "").strip()
    last_clean_shutdown_ts_ms = str(meta_get("last_clean_shutdown_ts_ms", "") or "").strip()
    last_crash_shutdown_ts_ms = str(meta_get("last_crash_shutdown_ts_ms", "") or "").strip()
    last_crash_reason = str(meta_get("last_crash_reason", "") or "").strip()
    dashboard_bound_ts_ms = str(meta_get("dashboard_bound_ts_ms", "") or "").strip()
    dashboard_bound_detail = str(meta_get("dashboard_bound_detail", "") or "").strip()
    warmup_started_ts_ms = str(meta_get("warmup_started_ts_ms", "") or "").strip()
    warmup_timeout_ts_ms = str(meta_get("warmup_timeout_ts_ms", "") or "").strip()
    updated_ts_ms_raw = str(meta_get("lifecycle_updated_ts_ms", "0") or "0").strip()

    try:
        updated_ts_ms = int(updated_ts_ms_raw or 0)
    except Exception:
        updated_ts_ms = 0

    out = {
        "state": str(state),
        "previous_state": str(previous_state),
        "detail": str(detail),
        "first_price_ts_ms": str(first_price_ts_ms),
        "last_clean_shutdown_ts_ms": str(last_clean_shutdown_ts_ms),
        "last_crash_shutdown_ts_ms": str(last_crash_shutdown_ts_ms),
        "last_crash_reason": str(last_crash_reason),
        "dashboard_bound_ts_ms": str(dashboard_bound_ts_ms),
        "dashboard_bound_detail": str(dashboard_bound_detail),
        "warmup_started_ts_ms": str(warmup_started_ts_ms),
        "warmup_timeout_ts_ms": str(warmup_timeout_ts_ms),
        "updated_ts_ms": int(updated_ts_ms),
    }

    return out


def set_state(state: Any, detail: str = "") -> Dict[str, Any]:
    prev = _normalize_state(meta_get("lifecycle_state", BOOTING))
    norm = _normalize_state(state)
    detail_s = str(detail or "")
    prev_detail = str(meta_get("lifecycle_detail", "") or "")
    now_ms = _now_ms()
    first_price_seen = str(meta_get("first_price_ts_ms", "") or "").strip()

    # WARMING_UP is only valid before the first confirmed market-data tick.
    # After that point, late bootstrap / child-start callbacks must not drag the
    # global lifecycle back out of LIVE or DEGRADED.
    if norm == WARMING_UP and first_price_seen:
        return get_state()
    if norm == WARMING_UP and prev == DEGRADED and prev_detail in _WARMUP_TIMEOUT_DETAILS:
        return get_state()

    # Lifecycle monitoring polls continuously. When the state/detail pair has
    # not changed, avoid rewriting runtime_meta and appending duplicate event
    # rows on every pass.
    if prev == norm and prev_detail == detail_s:
        return get_state()

    meta_set("lifecycle_prev_state", str(prev), best_effort=True)
    meta_set("lifecycle_state", str(norm), best_effort=True)
    meta_set("lifecycle_detail", detail_s, best_effort=True)
    meta_set("lifecycle_updated_ts_ms", str(int(now_ms)), best_effort=True)

    if norm == WARMING_UP:
        existing_started = str(meta_get("warmup_started_ts_ms", "") or "").strip()
        if prev != WARMING_UP or not existing_started:
            meta_set("warmup_started_ts_ms", str(int(now_ms)), best_effort=True)
        timeout_s = max(1, int(os.environ.get("WARMUP_TIMEOUT_S", "120")))
        meta_set("warmup_timeout_ts_ms", str(int(now_ms + (timeout_s * 1000))), best_effort=True)
    elif norm in (LIVE, DEGRADED, SHUTTING_DOWN, KILL_SWITCH):
        meta_set("warmup_timeout_ts_ms", "", best_effort=True)
        if norm != DEGRADED:
            meta_set("warmup_started_ts_ms", "", best_effort=True)

    try:
        _logger.info(
            "lifecycle_transition",
            extra={
                "event": "lifecycle_transition",
                "extra_json": {
                    "prev_state": str(prev),
                    "new_state": str(norm),
                    "detail": detail_s,
                    "ts_ms": int(now_ms),
                },
            },
        )
        emit_counter(
            "supervisor_state",
            1,
            component="engine.runtime.lifecycle_state",
            extra_tags={"state": str(norm), "previous_state": str(prev)},
        )
        emit_gauge(
            "job_health",
            1.0 if str(norm) in (LIVE, WARMING_UP) else 0.0,
            component="engine.runtime.lifecycle_state",
            extra_tags={"metric_scope": "lifecycle_state", "state": str(norm)},
        )
        trace_event(
            "supervisor_state",
            component="engine.runtime.lifecycle_state",
            entity_type="lifecycle",
            entity_id="runtime",
            payload={
                "prev_state": str(prev),
                "new_state": str(norm),
                "detail": detail_s,
                "ts_ms": int(now_ms),
            },
        )
    except Exception as e:
        _warn_nonfatal(
            "LIFECYCLE_STATE_SIDE_EFFECTS_FAILED",
            e,
            state=str(norm),
            previous_state=str(prev),
            detail=detail_s,
        )

    try:
        record_lifecycle_event(
            event_type="lifecycle_state_change",
            state=str(norm),
            detail=detail_s,
            actor="system",
            ts_ms=int(now_ms),
            con=None,
        )
    except Exception as e:
        _warn_nonfatal(
            "LIFECYCLE_STATE_EVENT_RECORD_FAILED",
            e,
            state=str(norm),
            previous_state=str(prev),
            detail=detail_s,
        )

    out = {
        "state": str(norm),
        "previous_state": str(prev),
        "detail": detail_s,
        "first_price_ts_ms": str(meta_get("first_price_ts_ms", "") or ""),
        "last_clean_shutdown_ts_ms": str(meta_get("last_clean_shutdown_ts_ms", "") or ""),
        "last_crash_shutdown_ts_ms": str(meta_get("last_crash_shutdown_ts_ms", "") or ""),
        "last_crash_reason": str(meta_get("last_crash_reason", "") or ""),
        "dashboard_bound_ts_ms": str(meta_get("dashboard_bound_ts_ms", "") or ""),
        "dashboard_bound_detail": str(meta_get("dashboard_bound_detail", "") or ""),
        "warmup_started_ts_ms": str(meta_get("warmup_started_ts_ms", "") or ""),
        "warmup_timeout_ts_ms": str(meta_get("warmup_timeout_ts_ms", "") or ""),
        "updated_ts_ms": int(now_ms),
    }

    cache_invalidate_namespace("lifecycle_state")
    return out


def mark_dashboard_bound(detail: str = "") -> None:
    ts_ms = _now_ms()
    meta_set("dashboard_bound_ts_ms", str(ts_ms), best_effort=True)
    meta_set("dashboard_bound_detail", str(detail or "")[:2000], best_effort=True)
    cache_invalidate_namespace("lifecycle_state")


def mark_clean_shutdown() -> None:
    ts_ms = _now_ms()
    meta_set("last_clean_shutdown_ts_ms", str(ts_ms), best_effort=True)
    set_state(SHUTTING_DOWN, "clean_shutdown")


def mark_crash_shutdown(reason: str = "") -> None:
    ts_ms = _now_ms()
    msg = str(reason or "").strip()
    meta_set("last_crash_shutdown_ts_ms", str(ts_ms), best_effort=True)
    meta_set("last_crash_reason", msg[:2000], best_effort=True)
    set_state(DEGRADED, msg or "crash_shutdown")
