"""Event-driven runtime bridge for live inference and execution shaping."""

from __future__ import annotations

import concurrent.futures
import logging
import os
import threading
import time
from typing import Any, Dict

from engine.execution.broker_router import apply_new_portfolio_orders_router
from engine.execution.execution_mode import get_execution_mode
from engine.execution.execution_policy_engine import apply_execution_policy
from engine.inference_engine import predict
from engine.runtime.event_bus import publish_event, subscribe_event, unsubscribe_event
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.observability import record_component_health
from engine.runtime.config_schema import get_runtime_safety_context

LOG = get_logger("runtime.event_runtime")
_LOCK = threading.RLock()
_STARTED = False
_EXECUTOR: concurrent.futures.ThreadPoolExecutor | None = None
_LAST_SUBMITTED_BY_SYMBOL: Dict[str, int] = {}
_LAST_PRICE_BY_SYMBOL: Dict[str, float] = {}
_LAST_EXECUTED_SIGNAL_TS_BY_SYMBOL: Dict[str, int] = {}
_SUBSCRIPTIONS = (
    ("price_tick", "_on_price_tick"),
    ("model_prediction", "_on_model_prediction"),
    ("strategy_signal", "_on_strategy_signal"),
    ("execution_decision", "_on_execution_decision"),
)

EVENT_RUNTIME_ENABLED = str(os.environ.get("EVENT_RUNTIME_ENABLED", "0") or "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
EVENT_RUNTIME_EXECUTE_REQUESTED = str(os.environ.get("EVENT_RUNTIME_EXECUTE_ENABLED", "0") or "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN = str(
    os.environ.get("EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN", "0") or "0"
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
EVENT_RUNTIME_HORIZON_S = max(1, int(os.environ.get("EVENT_RUNTIME_HORIZON_S", "300")))
EVENT_RUNTIME_TIMEOUT_S = max(0.05, float(os.environ.get("EVENT_RUNTIME_TIMEOUT_S", "1.0")))
EVENT_RUNTIME_DEBOUNCE_MS = max(0, int(os.environ.get("EVENT_RUNTIME_DEBOUNCE_MS", "250")))
EVENT_RUNTIME_SIGNAL_MIN_ABS_PREDICTION = float(os.environ.get("EVENT_RUNTIME_SIGNAL_MIN_ABS_PREDICTION", "0.75"))
EVENT_RUNTIME_SIGNAL_MIN_CONFIDENCE = float(os.environ.get("EVENT_RUNTIME_SIGNAL_MIN_CONFIDENCE", "0.45"))
EVENT_RUNTIME_EXEC_QTY = max(0.0, float(os.environ.get("EVENT_RUNTIME_EXEC_QTY", "1.0")))
EVENT_RUNTIME_EXECUTOR_WORKERS = max(1, int(os.environ.get("EVENT_RUNTIME_EXECUTOR_WORKERS", "4")))
EVENT_RUNTIME_ALPHA_TTL_MS = max(1, int(os.environ.get("EVENT_RUNTIME_ALPHA_TTL_MS", str(5 * 60 * 1000))))
EVENT_RUNTIME_ALPHA_HALF_LIFE_MS = max(1, int(os.environ.get("EVENT_RUNTIME_ALPHA_HALF_LIFE_MS", str(90 * 1000))))

_RUNTIME_SAFETY = dict(get_runtime_safety_context() or {})
_EVENT_RUNTIME_LIVE_LIKE_MODE = bool(_RUNTIME_SAFETY.get("live_like_mode"))
_EVENT_RUNTIME_STRICT_RUNTIME = bool(_RUNTIME_SAFETY.get("strict_runtime"))
_EVENT_RUNTIME_EXPLICIT_DEV_ENV = bool(_RUNTIME_SAFETY.get("explicit_dev_env"))


def _event_runtime_execute_block_reason() -> str:
    if not EVENT_RUNTIME_EXECUTE_REQUESTED:
        return "event_runtime_direct_execution_not_requested"
    if not EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN:
        return "event_runtime_direct_execution_requires_unsafe_opt_in"
    if _EVENT_RUNTIME_LIVE_LIKE_MODE:
        return "event_runtime_direct_execution_live_like_blocked"
    if _EVENT_RUNTIME_STRICT_RUNTIME:
        return "event_runtime_direct_execution_strict_runtime_blocked"
    if not _EVENT_RUNTIME_EXPLICIT_DEV_ENV:
        return "event_runtime_direct_execution_requires_explicit_dev_env"
    return ""


EVENT_RUNTIME_EXECUTE_BLOCK_REASON = _event_runtime_execute_block_reason()
EVENT_RUNTIME_EXECUTE_ENABLED = bool(
    EVENT_RUNTIME_EXECUTE_REQUESTED and not EVENT_RUNTIME_EXECUTE_BLOCK_REASON
)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.event_runtime",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _extract_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _event_executor() -> concurrent.futures.ThreadPoolExecutor:
    global _EXECUTOR
    with _LOCK:
        if _EXECUTOR is None:
            _EXECUTOR = concurrent.futures.ThreadPoolExecutor(
                max_workers=int(EVENT_RUNTIME_EXECUTOR_WORKERS),
                thread_name_prefix="event_runtime",
            )
        return _EXECUTOR


def _submit(fn, *args: Any) -> None:
    try:
        _event_executor().submit(fn, *args)
    except Exception as exc:
        _warn_nonfatal("EVENT_RUNTIME_SUBMIT_FAILED", exc)


def _on_price_tick(event: Dict[str, Any]) -> None:
    if not EVENT_RUNTIME_ENABLED:
        return
    payload = _extract_payload(event)
    symbol = str(payload.get("symbol") or "").strip().upper()
    if not symbol:
        return
    ts_ms = _safe_int(payload.get("ts_ms") or event.get("ts_ms") or _now_ms())
    price = _safe_float(payload.get("price"), 0.0)
    if price > 0.0:
        with _LOCK:
            _LAST_PRICE_BY_SYMBOL[symbol] = float(price)

    with _LOCK:
        last_ts_ms = int(_LAST_SUBMITTED_BY_SYMBOL.get(symbol) or 0)
        if ts_ms > 0 and last_ts_ms > 0 and (ts_ms - last_ts_ms) < int(EVENT_RUNTIME_DEBOUNCE_MS):
            return
        _LAST_SUBMITTED_BY_SYMBOL[symbol] = int(ts_ms)

    _submit(_run_prediction, symbol, dict(payload), int(ts_ms))


def _run_prediction(symbol: str, payload: Dict[str, Any], ts_ms: int) -> None:
    try:
        output = dict(
            predict(
                str(symbol),
                horizon_s=int(EVENT_RUNTIME_HORIZON_S),
                timeout_s=float(EVENT_RUNTIME_TIMEOUT_S),
                persist=True,
            )
        )
        prediction_payload = {
            "symbol": str(symbol),
            "price": float(_safe_float(payload.get("price"), 0.0)),
            "signal_ts_ms": int(ts_ms),
            "price_ts_ms": int(_safe_int(payload.get("ts_ms") or ts_ms, ts_ms)),
            "provider": str(payload.get("provider") or ""),
            "source": str(payload.get("source") or payload.get("provider") or ""),
            **output,
        }
        publish_event("model_prediction", prediction_payload)
    except Exception as exc:
        _warn_nonfatal("EVENT_RUNTIME_PREDICTION_FAILED", exc, symbol=str(symbol))


def _on_model_prediction(event: Dict[str, Any]) -> None:
    if not EVENT_RUNTIME_ENABLED:
        return
    payload = _extract_payload(event)
    _submit(_publish_strategy_signal, dict(payload), int(event.get("ts_ms") or _now_ms()))


def _publish_strategy_signal(payload: Dict[str, Any], ts_ms: int) -> None:
    symbol = str(payload.get("symbol") or "").strip().upper()
    prediction = _safe_float(payload.get("prediction"), 0.0)
    confidence = max(0.0, min(1.0, _safe_float(payload.get("confidence"), 0.0)))
    if (
        not symbol
        or bool(payload.get("safe_output"))
        or abs(float(prediction)) < float(EVENT_RUNTIME_SIGNAL_MIN_ABS_PREDICTION)
        or float(confidence) < float(EVENT_RUNTIME_SIGNAL_MIN_CONFIDENCE)
    ):
        return

    signal = "buy" if prediction > 0.0 else "sell"
    strategy_signal = {
        "source_alert_id": None,
        "event_title": "event_runtime_model_signal",
        "symbol": str(symbol),
        "horizon_s": int(payload.get("horizon_s") or EVENT_RUNTIME_HORIZON_S),
        "expected_z": float(prediction),
        "confidence": float(confidence),
        "signal": str(signal),
        "side": str(signal),
        "model_name": str(payload.get("model_name") or "safe_default"),
        "model_id": str(payload.get("model_id") or payload.get("model_name") or "safe_default"),
        "model_version": payload.get("model_version"),
        "regime": str(payload.get("regime") or "runtime"),
        "market_regime": str(payload.get("market_regime") or "runtime"),
        "explain": {
            "prediction_source": "event_runtime",
            "feature_ts_ms": int(payload.get("feature_ts_ms") or 0),
            "feature_set_tag": str(payload.get("feature_set_tag") or ""),
            "feature_ids": list(payload.get("feature_ids") or []),
            "feature_coverage": float(_safe_float(payload.get("feature_coverage"), 0.0)),
            "model_kind": payload.get("model_kind"),
            "price_ts_ms": int(payload.get("price_ts_ms") or ts_ms),
            "provider": str(payload.get("provider") or ""),
        },
        "ts_ms": int(ts_ms or _now_ms()),
    }
    publish_event("strategy_signal", strategy_signal)


def _on_strategy_signal(event: Dict[str, Any]) -> None:
    if not EVENT_RUNTIME_ENABLED:
        return
    payload = _extract_payload(event)
    _submit(_run_execution_decision, dict(payload), int(event.get("ts_ms") or _now_ms()))


def _run_execution_decision(payload: Dict[str, Any], event_ts_ms: int) -> None:
    explain = dict(payload.get("explain") or {})
    if str(payload.get("event_title") or "").strip() != "event_runtime_model_signal" and str(explain.get("prediction_source") or "").strip() != "event_runtime":
        return
    symbol = str(payload.get("symbol") or "").strip().upper()
    signal = str(payload.get("signal") or payload.get("side") or "").strip().lower()
    confidence = max(0.0, min(1.0, _safe_float(payload.get("confidence"), 0.0)))
    expected_z = _safe_float(payload.get("expected_z"), 0.0)
    if not symbol or signal not in {"buy", "sell"}:
        return

    with _LOCK:
        ref_price = float(_LAST_PRICE_BY_SYMBOL.get(symbol) or 0.0)

    order_side = "BUY" if signal == "buy" else "SELL"
    order = {
        "symbol": str(symbol),
        "side": str(order_side),
        "qty": float(max(EVENT_RUNTIME_EXEC_QTY * max(confidence, 0.25), 0.0)),
        "confidence": float(confidence),
        "expected_z": float(expected_z),
        "signal_ts_ms": int(payload.get("ts_ms") or event_ts_ms or _now_ms()),
        "ts_ms": int(payload.get("ts_ms") or event_ts_ms or _now_ms()),
        "alpha_ttl_ms": int(EVENT_RUNTIME_ALPHA_TTL_MS),
        "alpha_half_life_ms": int(EVENT_RUNTIME_ALPHA_HALF_LIFE_MS),
        "source_alert_id": payload.get("source_alert_id"),
        "model_id": str(payload.get("model_id") or payload.get("model_name") or "safe_default"),
        "model_name": str(payload.get("model_name") or "safe_default"),
        "ref_price": (float(ref_price) if ref_price > 0.0 else None),
    }

    try:
        shaped = list(
            apply_execution_policy(
                [order],
                actor="event_runtime",
                mode=str(get_execution_mode() or "unknown"),
                broker=str(os.environ.get("BROKER_NAME") or os.environ.get("BROKER") or "sim"),
                default_signal_ts_ms=int(order["signal_ts_ms"]),
            )
        )
        publish_event(
            "execution_decision",
            {
                "symbol": str(symbol),
                "signal": str(signal),
                "expected_z": float(expected_z),
                "confidence": float(confidence),
                "model_id": str(order.get("model_id") or ""),
                "orders": list(shaped),
                "ts_ms": int(_now_ms()),
                "source": "event_runtime",
            },
        )
    except Exception as exc:
        _warn_nonfatal("EVENT_RUNTIME_EXECUTION_DECISION_FAILED", exc, symbol=str(symbol))
        record_component_health(
            "execution",
            ok=False,
            status="error",
            detail=f"{type(exc).__name__}:{exc}",
            observed_ts_ms=int(_now_ms()),
            extra={"symbol": str(symbol), "signal": str(signal)},
        )


def _on_execution_decision(event: Dict[str, Any]) -> None:
    if not EVENT_RUNTIME_ENABLED:
        return
    if not EVENT_RUNTIME_EXECUTE_ENABLED:
        if EVENT_RUNTIME_EXECUTE_REQUESTED:
            record_component_health(
                "execution",
                ok=False,
                status="blocked",
                detail=str(EVENT_RUNTIME_EXECUTE_BLOCK_REASON or "event_runtime_direct_execution_blocked"),
                observed_ts_ms=int(_now_ms()),
                extra={
                    "source": "event_runtime",
                    "execute_requested": True,
                    "unsafe_direct_opt_in": bool(EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN),
                    "live_like_mode": bool(_EVENT_RUNTIME_LIVE_LIKE_MODE),
                    "strict_runtime": bool(_EVENT_RUNTIME_STRICT_RUNTIME),
                    "explicit_dev_env": bool(_EVENT_RUNTIME_EXPLICIT_DEV_ENV),
                },
            )
        return
    payload = _extract_payload(event)
    _submit(_run_execution_submission, dict(payload), int(event.get("ts_ms") or _now_ms()))


def _run_execution_submission(payload: Dict[str, Any], event_ts_ms: int) -> None:
    symbol = str(payload.get("symbol") or "").strip().upper()
    orders = [dict(order) for order in (payload.get("orders") or []) if isinstance(order, dict)]
    if not symbol or not orders:
        record_component_health(
            "execution",
            ok=False,
            status="suppressed",
            detail="execution_policy_suppressed",
            observed_ts_ms=int(_now_ms()),
            extra={"symbol": str(symbol), "orders": int(len(orders))},
        )
        return

    signal_ts_ms = max(
        int(payload.get("ts_ms") or event_ts_ms or 0),
        max((int(order.get("signal_ts_ms") or 0) for order in orders), default=0),
    )
    with _LOCK:
        last_executed_ts_ms = int(_LAST_EXECUTED_SIGNAL_TS_BY_SYMBOL.get(symbol) or 0)
        if signal_ts_ms > 0 and signal_ts_ms <= last_executed_ts_ms:
            return

    try:
        result = dict(
            apply_new_portfolio_orders_router(
                dry_run=False,
                override_orders=orders,
                override_order_id=None,
                override_ts_ms=int(signal_ts_ms or _now_ms()),
            )
            or {}
        )
        publish_event(
            "execution_result",
            {
                "symbol": str(symbol),
                "orders": list(orders),
                "result": dict(result),
                "ts_ms": int(_now_ms()),
                "source": "event_runtime",
            },
        )
        if bool(result.get("ok")) and signal_ts_ms > 0:
            with _LOCK:
                _LAST_EXECUTED_SIGNAL_TS_BY_SYMBOL[symbol] = int(signal_ts_ms)
        record_component_health(
            "execution",
            ok=bool(result.get("ok")),
            status=str(result.get("status") or ("ok" if result.get("ok") else "failed")),
            detail=str(result.get("broker") or result.get("status") or "execution_submitted"),
            observed_ts_ms=int(_now_ms()),
            extra={
                "symbol": str(symbol),
                "orders": int(len(orders)),
                "broker": str(result.get("broker") or ""),
            },
        )
    except Exception as exc:
        _warn_nonfatal("EVENT_RUNTIME_EXECUTION_SUBMISSION_FAILED", exc, symbol=str(symbol))
        record_component_health(
            "execution",
            ok=False,
            status="error",
            detail=f"{type(exc).__name__}:{exc}",
            observed_ts_ms=int(_now_ms()),
            extra={"symbol": str(symbol), "orders": int(len(orders))},
        )


def start_event_runtime() -> Dict[str, Any]:
    """Start the event-driven runtime bridge and subscribe its handlers."""
    global _STARTED

    with _LOCK:
        if _STARTED:
            return {
                "ok": True,
                "started": False,
                "enabled": bool(EVENT_RUNTIME_ENABLED),
                "execute_enabled": bool(EVENT_RUNTIME_EXECUTE_ENABLED),
                "execute_requested": bool(EVENT_RUNTIME_EXECUTE_REQUESTED),
                "execute_block_reason": str(EVENT_RUNTIME_EXECUTE_BLOCK_REASON or ""),
                "workers": int(EVENT_RUNTIME_EXECUTOR_WORKERS),
            }
        if not EVENT_RUNTIME_ENABLED:
            return {
                "ok": True,
                "started": False,
                "enabled": False,
                "execute_enabled": bool(EVENT_RUNTIME_EXECUTE_ENABLED),
                "execute_requested": bool(EVENT_RUNTIME_EXECUTE_REQUESTED),
                "execute_block_reason": str(EVENT_RUNTIME_EXECUTE_BLOCK_REASON or ""),
                "workers": int(EVENT_RUNTIME_EXECUTOR_WORKERS),
            }
        _STARTED = True

    subscribe_event("price_tick", _on_price_tick)
    subscribe_event("model_prediction", _on_model_prediction)
    subscribe_event("strategy_signal", _on_strategy_signal)
    subscribe_event("execution_decision", _on_execution_decision)
    _event_executor()
    if EVENT_RUNTIME_EXECUTE_REQUESTED and not EVENT_RUNTIME_EXECUTE_ENABLED:
        record_component_health(
            "execution",
            ok=False,
            status="blocked",
            detail=str(EVENT_RUNTIME_EXECUTE_BLOCK_REASON or "event_runtime_direct_execution_blocked"),
            observed_ts_ms=int(_now_ms()),
            extra={
                "source": "event_runtime",
                "execute_requested": True,
                "unsafe_direct_opt_in": bool(EVENT_RUNTIME_EXECUTE_UNSAFE_DIRECT_OPT_IN),
                "live_like_mode": bool(_EVENT_RUNTIME_LIVE_LIKE_MODE),
                "strict_runtime": bool(_EVENT_RUNTIME_STRICT_RUNTIME),
                "explicit_dev_env": bool(_EVENT_RUNTIME_EXPLICIT_DEV_ENV),
            },
        )
    return {
        "ok": True,
        "started": True,
        "enabled": bool(EVENT_RUNTIME_ENABLED),
        "execute_enabled": bool(EVENT_RUNTIME_EXECUTE_ENABLED),
        "execute_requested": bool(EVENT_RUNTIME_EXECUTE_REQUESTED),
        "execute_block_reason": str(EVENT_RUNTIME_EXECUTE_BLOCK_REASON or ""),
        "workers": int(EVENT_RUNTIME_EXECUTOR_WORKERS),
    }


def stop_event_runtime(timeout_s: float = 2.0) -> Dict[str, Any]:
    """Stop the event runtime, unsubscribe handlers, and tear down workers."""
    global _STARTED, _EXECUTOR
    with _LOCK:
        started = bool(_STARTED)
        _STARTED = False
        executor = _EXECUTOR
        _EXECUTOR = None
        _LAST_SUBMITTED_BY_SYMBOL.clear()
        _LAST_PRICE_BY_SYMBOL.clear()
        _LAST_EXECUTED_SIGNAL_TS_BY_SYMBOL.clear()
    if started:
        for event_type, handler_name in _SUBSCRIPTIONS:
            handler = globals().get(str(handler_name))
            if callable(handler):
                try:
                    unsubscribe_event(str(event_type), handler)
                except Exception as exc:
                    _warn_nonfatal("EVENT_RUNTIME_UNSUBSCRIBE_FAILED", exc, event_type=str(event_type))
    if executor is not None:
        try:
            executor.shutdown(wait=True, cancel_futures=True)
        except Exception as exc:
            _warn_nonfatal("EVENT_RUNTIME_EXECUTOR_SHUTDOWN_FAILED", exc)
    record_component_health(
        "execution",
        ok=True,
        status="stopped",
        detail="event_runtime_stopped",
        observed_ts_ms=int(_now_ms()),
        extra={"source": "event_runtime"},
    )
    return {
        "ok": True,
        "started": False,
        "enabled": bool(EVENT_RUNTIME_ENABLED),
        "execute_enabled": bool(EVENT_RUNTIME_EXECUTE_ENABLED),
        "execute_requested": bool(EVENT_RUNTIME_EXECUTE_REQUESTED),
        "execute_block_reason": str(EVENT_RUNTIME_EXECUTE_BLOCK_REASON or ""),
        "workers": int(EVENT_RUNTIME_EXECUTOR_WORKERS),
    }


__all__ = ["start_event_runtime", "stop_event_runtime"]
