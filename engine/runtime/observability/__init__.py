"""Shared runtime observability helpers."""

from __future__ import annotations

import copy
import logging
import os
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Tuple

from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_gauge

LOG = get_logger("runtime.observability")

_COMPONENT_HEALTH_LOCK = threading.RLock()
_COMPONENT_HEALTH: Dict[str, Dict[str, Any]] = {}
_ROLLING_RATE_LOCK = threading.RLock()
_ROLLING_RATES: Dict[Tuple[str, str, Tuple[Tuple[str, str], ...]], Deque[int]] = {}

DEFAULT_HEALTH_TTL_MS = int(float(os.environ.get("OBS_COMPONENT_HEALTH_TTL_S", "900")) * 1000.0)
DEFAULT_RATE_WINDOW = max(5, int(os.environ.get("OBS_RATE_WINDOW", "50")))


def now_ms() -> int:
    return int(time.time() * 1000)


def backoff_delay_s(attempt: int, *, base_s: float, max_s: float) -> float:
    attempt_i = max(1, int(attempt or 1))
    delay_s = float(max(0.0, float(base_s))) * float(2 ** (attempt_i - 1))
    return float(min(max(0.0, float(max_s)), delay_s))


def _normalize_tags(extra_tags: Optional[Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, value in dict(extra_tags or {}).items():
        if value is None:
            continue
        out[str(key)] = str(value)
    return out


def _health_payload(
    component: str,
    *,
    ok: bool,
    status: str,
    detail: str,
    observed_ts_ms: int,
    latency_ms: Optional[float],
    extra: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "component": str(component),
        "ok": bool(ok),
        "status": str(status or ("ok" if ok else "error")),
        "detail": str(detail or ""),
        "updated_ts_ms": int(observed_ts_ms),
    }
    if latency_ms is not None:
        payload["latency_ms"] = float(latency_ms)
    if extra:
        payload.update(dict(extra))
    return payload


def _annotate_health_age(payload: Dict[str, Any], *, ttl_ms: int) -> Dict[str, Any]:
    out = copy.deepcopy(dict(payload or {}))
    updated_ts_ms = int(out.get("updated_ts_ms") or 0)
    age_ms = max(0, now_ms() - updated_ts_ms) if updated_ts_ms > 0 else 10**12
    out["age_s"] = round(age_ms / 1000.0, 1) if age_ms < 10**12 else None
    out["stale"] = bool(updated_ts_ms <= 0 or age_ms > int(ttl_ms))
    if out["stale"]:
        out["ok"] = False
        status = str(out.get("status") or "").strip().lower()
        if status in ("", "ok", "healthy"):
            out["status"] = "stale"
        if not str(out.get("detail") or "").strip():
            out["detail"] = "component_health_stale"
    return out


def record_component_health(
    component: str,
    *,
    ok: bool,
    status: str = "",
    detail: str = "",
    observed_ts_ms: Optional[int] = None,
    latency_ms: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None,
    emit_log_on_change: bool = True,
) -> Dict[str, Any]:
    component_name = str(component or "").strip().lower() or "unknown"
    observed = int(observed_ts_ms or now_ms())
    payload = _health_payload(
        component_name,
        ok=bool(ok),
        status=(status or ("ok" if ok else "error")),
        detail=str(detail or ""),
        observed_ts_ms=observed,
        latency_ms=latency_ms,
        extra=extra,
    )

    with _COMPONENT_HEALTH_LOCK:
        previous = copy.deepcopy(_COMPONENT_HEALTH.get(component_name) or {})
        _COMPONENT_HEALTH[component_name] = dict(payload)

    emit_gauge(
        "component_health_ok",
        1.0 if bool(ok) else 0.0,
        component="engine.runtime.observability",
        extra_tags={
            "observed_component": component_name,
            "status": str(payload.get("status") or ""),
        },
    )

    changed = (
        not previous
        or bool(previous.get("ok")) != bool(payload.get("ok"))
        or str(previous.get("status") or "") != str(payload.get("status") or "")
        or str(previous.get("detail") or "") != str(payload.get("detail") or "")
    )
    if emit_log_on_change and changed:
        log_event(
            LOG,
            logging.INFO if bool(ok) else logging.WARNING,
            "component_health_updated",
            component="engine.runtime.observability",
            extra=payload,
        )
    return dict(payload)


def get_component_health_snapshot(
    component: Optional[str] = None,
    *,
    ttl_ms: Optional[int] = None,
) -> Dict[str, Any]:
    effective_ttl_ms = max(1, int(ttl_ms or DEFAULT_HEALTH_TTL_MS))
    with _COMPONENT_HEALTH_LOCK:
        if component is not None:
            component_name = str(component or "").strip().lower() or "unknown"
            payload = copy.deepcopy(_COMPONENT_HEALTH.get(component_name) or {})
            if not payload:
                return {
                    "component": component_name,
                    "ok": False,
                    "status": "unknown",
                    "detail": "component_health_unreported",
                    "updated_ts_ms": None,
                    "age_s": None,
                    "stale": True,
                }
            return _annotate_health_age(payload, ttl_ms=effective_ttl_ms)

        snapshot = copy.deepcopy(_COMPONENT_HEALTH)
    return {
        component_name: _annotate_health_age(payload, ttl_ms=effective_ttl_ms)
        for component_name, payload in snapshot.items()
    }


def record_rolling_rate(
    metric: str,
    *,
    success: bool,
    component: str,
    extra_tags: Optional[Dict[str, Any]] = None,
    window_size: Optional[int] = None,
) -> float:
    metric_name = str(metric or "").strip()
    component_name = str(component or "").strip() or "unknown"
    tags = _normalize_tags(extra_tags)
    effective_window = max(5, int(window_size or DEFAULT_RATE_WINDOW))
    key = (
        metric_name,
        component_name,
        tuple(sorted((str(key), str(value)) for key, value in tags.items())),
    )

    with _ROLLING_RATE_LOCK:
        window = _ROLLING_RATES.get(key)
        if window is None or int(window.maxlen or 0) != int(effective_window):
            window = deque(maxlen=effective_window)
            _ROLLING_RATES[key] = window
        window.append(1 if bool(success) else 0)
        sample_size = len(window)
        rate = float(sum(window) / float(sample_size)) if sample_size > 0 else 0.0

    emit_counter(
        f"{metric_name}_observations",
        1,
        component=component_name,
        extra_tags={
            **tags,
            "outcome": ("success" if bool(success) else "failure"),
        },
    )
    emit_gauge(
        metric_name,
        float(rate),
        component=component_name,
        extra_tags={
            **tags,
            "sample_size": int(sample_size),
        },
    )
    return float(rate)


__all__ = [
    "backoff_delay_s",
    "get_component_health_snapshot",
    "now_ms",
    "record_component_health",
    "record_rolling_rate",
]
