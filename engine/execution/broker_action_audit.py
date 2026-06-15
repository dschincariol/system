"""Durable audit helper for broker side-effect attempts."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from engine.execution.order_command_boundary import record_order_event
from engine.runtime.failure_diagnostics import log_failure


LOG = logging.getLogger("engine.execution.broker_action_audit")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.broker_action_audit",
        extra=extra or None,
        persist=False,
    )


def record_broker_action_audit(
    *,
    broker: str,
    action: str,
    status: str,
    symbol: Optional[str] = None,
    qty: Optional[float] = None,
    client_order_id: Optional[str] = None,
    broker_order_id: Optional[str] = None,
    portfolio_orders_id: Optional[int] = None,
    mode: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Append an order-boundary event for a broker side effect.

    Callers use this as a pre-submit barrier: a failed audit write should block
    the broker action rather than allowing unaudited side effects.
    """

    event_ts_ms = int(ts_ms if ts_ms is not None else _now_ms())
    broker_name = str(broker or "").strip().lower() or "unknown"
    action_name = str(action or "").strip().lower() or "broker_action"
    client_id = str(client_order_id or "").strip()
    broker_id = str(broker_order_id or "").strip()
    correlation_id = client_id or broker_id or (
        str(portfolio_orders_id) if portfolio_orders_id is not None else None
    )
    payload_norm: Dict[str, Any] = {
        "broker": broker_name,
        "action": action_name,
        "status": str(status or "").strip().lower() or "unknown",
        "symbol": (str(symbol or "").strip().upper() or None),
        "qty": (float(qty) if qty is not None else None),
        "client_order_id": client_id or None,
        "broker_order_id": broker_id or None,
        "portfolio_orders_id": (int(portfolio_orders_id) if portfolio_orders_id is not None else None),
    }
    payload_norm.update(dict(payload or {}))

    try:
        event_id = record_order_event(
            ts_ms=int(event_ts_ms),
            event_type=f"broker_{action_name}",
            mode=str(mode or ""),
            broker=broker_name,
            status=str(status or "").strip().lower() or "unknown",
            payload=payload_norm,
            batch_id=(int(portfolio_orders_id) if portfolio_orders_id is not None else None),
            correlation_id=(str(correlation_id) if correlation_id else None),
        )
        return {"ok": True, "event_id": event_id, "ts_ms": int(event_ts_ms)}
    except Exception as exc:
        _warn_nonfatal(
            "BROKER_ACTION_AUDIT_WRITE_FAILED",
            exc,
            broker=broker_name,
            action=action_name,
            status=str(status or ""),
            symbol=str(symbol or ""),
            client_order_id=client_id,
            broker_order_id=broker_id,
        )
        return {
            "ok": False,
            "status": "broker_action_audit_failed",
            "broker": broker_name,
            "action": action_name,
            "error": f"{type(exc).__name__}: {exc}",
        }
