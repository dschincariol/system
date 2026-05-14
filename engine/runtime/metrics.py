"""
FILE: metrics.py

Runtime subsystem module for `metrics`.
"""

from __future__ import annotations

import importlib
import logging
import os
import threading
import time
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("runtime.metrics")
_GAUGE_MIN_EMIT_INTERVAL_MS = max(
    0,
    int(float(os.environ.get("RUNTIME_GAUGE_MIN_EMIT_INTERVAL_S", "5.0")) * 1000.0),
)
_LAST_GAUGE_EMISSION_LOCK = threading.Lock()
_LAST_GAUGE_EMISSION: Dict[tuple[str, str, tuple[tuple[str, str], ...]], tuple[str, int]] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="runtime_metrics_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.metrics",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _metrics_store():
    return importlib.import_module("engine.runtime.metrics_store")


def _metric_scope_key() -> str:
    try:
        from engine.runtime.db_guard import resolve_db_path

        return str(resolve_db_path())
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_METRICS_SCOPE_KEY_RESOLVE_FAILED",
            e,
            once_key="runtime_metrics_scope_key_resolve_failed",
        )
        return ""


def _metric_tags(**kwargs: Any) -> Dict[str, str]:
    # Tags are normalized to strings here so the metrics store schema stays
    # simple and callers can pass ints/bools without special handling.
    out: Dict[str, str] = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        out[str(key)] = str(value)
    return out


def emit_metric(
    metric: str,
    value: Any,
    *,
    metric_type: str = "gauge",
    service: str = "trading-engine",
    component: Optional[str] = None,
    job: Optional[str] = None,
    symbol: Optional[str] = None,
    strategy: Optional[str] = None,
    provider: Optional[str] = None,
    broker: Optional[str] = None,
    trace_id: Optional[str] = None,
    span_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
    extra_tags: Optional[Dict[str, Any]] = None,
) -> None:
    # This is the single public entrypoint for runtime metric emission; counter,
    # gauge, and timing helpers all funnel through it.
    tags = _metric_tags(
        metric_type=metric_type,
        service=service,
        component=component,
        job=job,
        symbol=symbol,
        strategy=strategy,
        provider=provider,
        broker=broker,
        trace_id=trace_id,
        span_id=span_id,
    )
    for k, v in dict(extra_tags or {}).items():
        if v is not None:
            tags[str(k)] = str(v)

    metric_name = str(metric)
    emitted_ts_ms = int(ts_ms) if ts_ms is not None else _now_ms()
    if metric_type == "gauge" and _GAUGE_MIN_EMIT_INTERVAL_MS > 0:
        gauge_value = str(value)
        try:
            with _LAST_GAUGE_EMISSION_LOCK:
                gauge_key = (_metric_scope_key(), metric_name, tuple(sorted(tags.items())))
                previous = _LAST_GAUGE_EMISSION.get(gauge_key)
                if (
                    previous is not None
                    and previous[0] == gauge_value
                    and (emitted_ts_ms - int(previous[1])) < _GAUGE_MIN_EMIT_INTERVAL_MS
                ):
                    return
                _LAST_GAUGE_EMISSION[gauge_key] = (gauge_value, int(emitted_ts_ms))
        except Exception as e:
            _warn_nonfatal(
                "RUNTIME_METRICS_GAUGE_THROTTLE_FAILED",
                e,
                once_key=f"gauge_throttle:{metric_name}",
                metric=metric_name,
            )

    try:
        _metrics_store().write_runtime_metric(
            metric_name,
            value_num=value,
            tags=tags,
            ts_ms=int(emitted_ts_ms),
        )
    except Exception as e:
        _warn_nonfatal("RUNTIME_METRICS_EMIT_FAILED", e, once_key=f"emit_metric:{metric_name}", metric=metric_name)
        # Metrics are observability only; they must never take down callers.
        return


def emit_counter(metric: str, value: Any = 1, **kwargs: Any) -> None:
    emit_metric(metric, value, metric_type="counter", **kwargs)


def emit_gauge(metric: str, value: Any, **kwargs: Any) -> None:
    emit_metric(metric, value, metric_type="gauge", **kwargs)


def emit_timing(metric: str, latency_ms: Any, **kwargs: Any) -> None:
    emit_metric(metric, latency_ms, metric_type="timing_ms", **kwargs)


def emit_snapshot(metrics: Dict[str, Any], *, tags: Optional[Dict[str, Any]] = None, ts_ms: Optional[int] = None) -> None:
    # Snapshots are coarse multi-metric dumps, separate from the row-per-metric
    # stream written by emit_metric().
    try:
        _metrics_store().write_runtime_snapshot(
            {
                "ts_ms": int(ts_ms or _now_ms()),
                "metrics": dict(metrics or {}),
                "tags": dict(tags or {}),
            }
        )
    except Exception as e:
        _warn_nonfatal("RUNTIME_METRICS_SNAPSHOT_FAILED", e, once_key="emit_snapshot")
        return
