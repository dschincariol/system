"""
FILE: tracing.py

Runtime subsystem module for `tracing`.
"""

from __future__ import annotations

import contextlib
import contextvars
import logging
import time
import uuid
from typing import Any, Dict, Generator, Optional

from engine.runtime.event_log import append_event
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import bind_log_context, get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_timing

_TRACE_ID: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("trace_id", default=None)
_SPAN_ID: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("span_id", default=None)
_PARENT_SPAN_ID: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("parent_span_id", default=None)
LOG = get_logger("engine.runtime.tracing")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _new_id() -> str:
    return uuid.uuid4().hex


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="tracing_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.tracing",
        extra=extra or None,
        persist=False,
    )


def current_trace_context() -> Dict[str, Optional[str]]:
    return {
        "trace_id": _TRACE_ID.get(),
        "span_id": _SPAN_ID.get(),
        "parent_span_id": _PARENT_SPAN_ID.get(),
    }


def ensure_trace_context(*, trace_id: Optional[str] = None, span_id: Optional[str] = None) -> Dict[str, str]:
    # ContextVars let deep runtime code attach to the active trace without
    # threading IDs through every call signature.
    trace = str(trace_id or _TRACE_ID.get() or _new_id())
    span = str(span_id or _SPAN_ID.get() or _new_id())
    _TRACE_ID.set(trace)
    _SPAN_ID.set(span)
    bind_log_context(trace_id=trace, span_id=span)
    return {"trace_id": trace, "span_id": span}


def trace_event(
    event: str,
    *,
    component: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    job: Optional[str] = None,
    symbol: Optional[str] = None,
    strategy: Optional[str] = None,
    provider: Optional[str] = None,
    broker: Optional[str] = None,
    ts_ms: Optional[int] = None,
    con: Any = None,
) -> Dict[str, str]:
    # Every emitted event carries trace identifiers so logs, event rows, and
    # metrics can be stitched back together later.
    ctx = ensure_trace_context()
    payload_out = dict(payload or {})
    payload_out.setdefault("trace_id", ctx["trace_id"])
    payload_out.setdefault("span_id", ctx["span_id"])

    if job is not None:
        payload_out.setdefault("job", str(job))
    if symbol is not None:
        payload_out.setdefault("symbol", str(symbol))
    if strategy is not None:
        payload_out.setdefault("strategy", str(strategy))
    if provider is not None:
        payload_out.setdefault("provider", str(provider))
    if broker is not None:
        payload_out.setdefault("broker", str(broker))

    try:
        append_event(
            event_type=str(event),
            event_source=str(component),
            entity_type=(str(entity_type) if entity_type else None),
            entity_id=(str(entity_id) if entity_id else None),
            correlation_id=str(ctx["trace_id"]),
            payload=payload_out,
            ts_ms=(int(ts_ms) if ts_ms is not None else _now_ms()),
            con=con,
        )
    except Exception as e:
        _warn_nonfatal(
            "TRACING_APPEND_EVENT_FAILED",
            e,
            trace_id=str(ctx["trace_id"]),
            span_id=str(ctx["span_id"]),
            event=str(event),
            component=str(component),
        )

    try:
        log_event(
            get_logger(component),
            20,
            event,
            component=component,
            extra=payload_out,
        )
    except Exception as e:
        _warn_nonfatal(
            "TRACING_LOG_EVENT_FAILED",
            e,
            trace_id=str(ctx["trace_id"]),
            span_id=str(ctx["span_id"]),
            event=str(event),
            component=str(component),
        )

    return ctx


@contextlib.contextmanager
def trace_block(
    event: str,
    *,
    component: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    job: Optional[str] = None,
    symbol: Optional[str] = None,
    strategy: Optional[str] = None,
    provider: Optional[str] = None,
    broker: Optional[str] = None,
) -> Generator[Dict[str, Optional[str]], None, None]:
    # trace_block creates a nested span and emits symmetric start/ok/error
    # events around one unit of work.
    parent_trace = _TRACE_ID.get()
    parent_span = _SPAN_ID.get()

    trace = str(parent_trace or _new_id())
    span = _new_id()

    token_trace = _TRACE_ID.set(trace)
    token_parent = _PARENT_SPAN_ID.set(parent_span)
    token_span = _SPAN_ID.set(span)

    bind_log_context(trace_id=trace, span_id=span)
    started_ms = _now_ms()

    trace_event(
        f"{event}.start",
        component=component,
        entity_type=entity_type,
        entity_id=entity_id,
        payload=dict(payload or {}),
        job=job,
        symbol=symbol,
        strategy=strategy,
        provider=provider,
        broker=broker,
        ts_ms=started_ms,
    )

    try:
        yield {"trace_id": trace, "span_id": span, "parent_span_id": parent_span}

        dur_ms = _now_ms() - started_ms
        emit_timing(
            f"{event}.latency_ms",
            dur_ms,
            component=component,
            job=job,
            symbol=symbol,
            strategy=strategy,
            provider=provider,
            broker=broker,
            trace_id=trace,
            span_id=span,
        )
        emit_counter(
            f"{event}.ok",
            1,
            component=component,
            job=job,
            symbol=symbol,
            strategy=strategy,
            provider=provider,
            broker=broker,
            trace_id=trace,
            span_id=span,
        )
        trace_event(
            f"{event}.ok",
            component=component,
            entity_type=entity_type,
            entity_id=entity_id,
            payload={**dict(payload or {}), "latency_ms": int(dur_ms), "trace_id": trace, "span_id": span},
            job=job,
            symbol=symbol,
            strategy=strategy,
            provider=provider,
            broker=broker,
        )
    except Exception as exc:
        dur_ms = _now_ms() - started_ms
        emit_timing(
            f"{event}.latency_ms",
            dur_ms,
            component=component,
            job=job,
            symbol=symbol,
            strategy=strategy,
            provider=provider,
            broker=broker,
            trace_id=trace,
            span_id=span,
        )
        emit_counter(
            f"{event}.error",
            1,
            component=component,
            job=job,
            symbol=symbol,
            strategy=strategy,
            provider=provider,
            broker=broker,
            trace_id=trace,
            span_id=span,
        )
        trace_event(
            f"{event}.error",
            component=component,
            entity_type=entity_type,
            entity_id=entity_id,
            payload={**dict(payload or {}), "latency_ms": int(dur_ms), "error": str(exc), "trace_id": trace, "span_id": span},
            job=job,
            symbol=symbol,
            strategy=strategy,
            provider=provider,
            broker=broker,
        )
        raise
    finally:
        _SPAN_ID.reset(token_span)
        _PARENT_SPAN_ID.reset(token_parent)
        _TRACE_ID.reset(token_trace)
        bind_log_context(trace_id=_TRACE_ID.get(), span_id=_SPAN_ID.get())
