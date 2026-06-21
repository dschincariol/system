"""
FILE: broker_alpaca_rest.py

Execution subsystem module for `broker_alpaca_rest`.
"""

"""
Alpaca Trading API v2 (REST) adapter.

Env:
  ALPACA_BASE_URL=https://paper-api.alpaca.markets
  ALPACA_KEY_ID=...
  ALPACA_SECRET_KEY=...

Optional execution knobs:
  ALPACA_ORDER_TIF=day
  ALPACA_ORDER_TYPE=market
  ALPACA_MAX_ORDERS_PER_PASS=25
  ALPACA_SLEEP_BETWEEN_ORDERS_S=0.25

Limit microstructure knobs:
  ALPACA_LIMIT_OFFSET_BPS_PASSIVE=5.0
  ALPACA_LIMIT_OFFSET_BPS_NEUTRAL=2.0
  ALPACA_LIMIT_OFFSET_BPS_AGGRESSIVE=0.5
"""

import json
import logging
import os
import hashlib
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from engine.data._credentials import get_data_credential
from engine.execution.broker_failover_policy import terminal_broker_failure
from engine.execution.broker_fill_utils import parse_broker_timestamp_ms
from engine.execution.kill_switch_reactivity import wait_with_kill_interrupt
from engine.execution.options_readiness import live_options_order_block
from engine.execution.execution_ledger import init_execution_ledger, log_submit, log_fill
from engine.execution.broker_submission_recovery import (
    record_submission_unrecorded,
    unrecorded_submission_gate,
)
from engine.strategy.alpha_lifecycle_engine import apply_alpha_lifecycle
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.live_execution_control import (
    disabled_live_execution_gate,
    live_execution_disabled,
    prelive_reconcile_policy_gate,
)
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.storage import connect, run_write_txn
from engine.execution.kill_switch import execution_allowed
from engine.runtime.risk_state import get_state, set_state
from engine.execution.deployable_capital import compute_deployable_equity
from engine.execution.order_idempotency import (
    claim_order_submission_durable,
    mark_order_submission_submitted_durable,
    mark_order_submission_unknown_durable,
)
from engine.execution.broker_action_audit import record_broker_action_audit

try:
    from engine.runtime.gates import execution_gate_snapshot  # type: ignore
except Exception:
    execution_gate_snapshot = None  # type: ignore

try:
    from engine.cache.wrappers.kill_switch import read_kill_switch as kill_switch_snapshot  # type: ignore
except Exception:
    kill_switch_snapshot = None  # type: ignore

try:
    from engine.cache.wrappers.execution_mode import read_execution_mode as get_execution_mode  # type: ignore
except Exception:
    get_execution_mode = None  # type: ignore

try:
    from engine.execution.position_reconcile import pre_live_position_reconcile as _prelive_reconcile
except Exception:
    _prelive_reconcile = None  # type: ignore

try:
    import websocket  # type: ignore
except Exception as _WEBSOCKET_IMPORT_ERROR:
    websocket = None  # type: ignore
else:
    _WEBSOCKET_IMPORT_ERROR = None


BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()
KEY_ID = os.environ.get("ALPACA_KEY_ID", "").strip()
SECRET = os.environ.get("ALPACA_SECRET_KEY", "").strip()
STREAM_URL = os.environ.get("ALPACA_STREAM_URL", "").strip()

ORDER_TIF = os.environ.get("ALPACA_ORDER_TIF", "day").strip()
ORDER_TYPE = os.environ.get("ALPACA_ORDER_TYPE", "market").strip()
MAX_ORDERS_PER_PASS = int(os.environ.get("ALPACA_MAX_ORDERS_PER_PASS", "25"))
SLEEP_BETWEEN_ORDERS_S = float(os.environ.get("ALPACA_SLEEP_BETWEEN_ORDERS_S", "0.25"))
TRADE_UPDATES_WS_ENABLED = os.environ.get("ALPACA_TRADE_UPDATES_WS_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
TRADE_UPDATES_PING_INTERVAL_S = float(os.environ.get("ALPACA_TRADE_UPDATES_PING_INTERVAL_S", "20"))
TRADE_UPDATES_PING_TIMEOUT_S = float(os.environ.get("ALPACA_TRADE_UPDATES_PING_TIMEOUT_S", "10"))
TRADE_UPDATES_RECONNECT_BASE_S = float(os.environ.get("ALPACA_TRADE_UPDATES_RECONNECT_BASE_S", "1.0"))
TRADE_UPDATES_RECONNECT_MAX_S = float(os.environ.get("ALPACA_TRADE_UPDATES_RECONNECT_MAX_S", "30.0"))
TRADE_UPDATES_GAP_LOOKBACK_S = int(os.environ.get("ALPACA_TRADE_UPDATES_GAP_LOOKBACK_S", "3600"))

LIM_OFF_BPS_PASSIVE = float(os.environ.get("ALPACA_LIMIT_OFFSET_BPS_PASSIVE", "5.0"))
LIM_OFF_BPS_NEUTRAL = float(os.environ.get("ALPACA_LIMIT_OFFSET_BPS_NEUTRAL", "2.0"))
LIM_OFF_BPS_AGGR = float(os.environ.get("ALPACA_LIMIT_OFFSET_BPS_AGGRESSIVE", "0.5"))

EXEC_TOTAL_EXPOSURE_CAP = float(
    os.environ.get(
        "EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP",
        os.environ.get("PORTFOLIO_RISK_MAX_GROSS", os.environ.get("PORTFOLIO_GROSS_CAP", "1.00")),
    )
)
EXEC_SYMBOL_CONCENTRATION_CAP = float(
    os.environ.get(
        "EXEC_PORTFOLIO_SYMBOL_CONCENTRATION_CAP",
        os.environ.get("PORTFOLIO_RISK_MAX_SYMBOL_GROSS", os.environ.get("KILL_SWITCH_CONCENTRATION_MAX_SINGLE", "0.35")),
    )
)

EXEC_DIRECTION_CONCENTRATION_CAP = float(
    os.environ.get(
        "EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP",
        os.environ.get("PORTFOLIO_RISK_MAX_NET", "0.60"),
    )
)
LOG = get_logger("engine.execution.broker_alpaca_rest")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _alpaca_key_id() -> str:
    return str(get_data_credential("ALPACA_KEY_ID") or KEY_ID or "").strip()


def _alpaca_secret_key() -> str:
    return str(get_data_credential("ALPACA_SECRET_KEY") or SECRET or "").strip()


class AlpacaCredentialError(RuntimeError):
    """Raised when required Alpaca credentials are absent."""


class AlpacaAuthenticationError(RuntimeError):
    """Raised when Alpaca rejects configured credentials."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _alpaca_live_endpoint_required() -> bool:
    return (
        str(os.environ.get("ENGINE_MODE", "") or "").strip().lower() == "live"
        or str(os.environ.get("EXECUTION_MODE", "") or "").strip().lower() == "live"
        or str(os.environ.get("ENV", "") or "").strip().lower() in {"prod", "production"}
    )


def alpaca_credentials_status(*, require_live_endpoint: Optional[bool] = None) -> Dict[str, Any]:
    key_id = _alpaca_key_id()
    secret = _alpaca_secret_key()
    base_url = str(os.environ.get("ALPACA_BASE_URL", BASE_URL) or "").strip()
    require_live = _alpaca_live_endpoint_required() if require_live_endpoint is None else bool(require_live_endpoint)
    missing = []
    invalid = []
    if not key_id:
        missing.append("ALPACA_KEY_ID")
    if not secret:
        missing.append("ALPACA_SECRET_KEY")
    if require_live and "paper-api.alpaca.markets" in base_url.lower():
        invalid.append("ALPACA_BASE_URL")
    status = "configured"
    if missing:
        status = "missing_credentials"
    elif invalid:
        status = "alpaca_paper_endpoint_for_live"
    return {
        "ok": not missing and not invalid,
        "broker": "alpaca",
        "status": status,
        "required_live_endpoint": bool(require_live),
        "configured_fields": {
            "ALPACA_BASE_URL": bool(base_url),
            "ALPACA_KEY_ID": bool(key_id),
            "ALPACA_SECRET_KEY": bool(secret),
        },
        "base_url": base_url,
        "missing": missing,
        "invalid": invalid,
    }


def _alpaca_terminal_failure(
    *,
    status: str,
    failure_kind: str,
    detail: str = "",
    error: Any = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return terminal_broker_failure(
        broker="alpaca",
        status=status,
        failure_kind=failure_kind,
        detail=detail,
        error=error,
        extra=extra,
    )


def _alpaca_credentials_block() -> Optional[Dict[str, Any]]:
    state = alpaca_credentials_status(require_live_endpoint=_alpaca_live_endpoint_required())
    if bool(state.get("ok")):
        return None
    status = str(state.get("status") or "missing_credentials")
    return _alpaca_terminal_failure(
        status=status,
        failure_kind="configuration" if status == "alpaca_paper_endpoint_for_live" else "credential",
        detail=status,
        extra={"credentials": state},
    )


def _alpaca_exception_failure(exc: BaseException) -> Optional[Dict[str, Any]]:
    if isinstance(exc, AlpacaCredentialError):
        return _alpaca_terminal_failure(
            status="missing_credentials",
            failure_kind="credential",
            detail="alpaca_credentials_missing",
            error=str(exc),
        )
    if isinstance(exc, AlpacaAuthenticationError):
        return _alpaca_terminal_failure(
            status="auth_failed",
            failure_kind="auth",
            detail="alpaca_auth_failed",
            error=str(exc),
            extra={"http_status": getattr(exc, "status_code", None)},
        )
    return None


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
        component="engine.execution.broker_alpaca_rest",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ALPACA_REST_SAFE_INT_FAILED",
            e,
            once_key=f"safe_int:{type(value).__name__}:{str(value)[:64]}",
            value_type=type(value).__name__,
        )
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ALPACA_REST_SAFE_FLOAT_FAILED",
            e,
            once_key=f"safe_float:{type(value).__name__}:{str(value)[:64]}",
            value_type=type(value).__name__,
        )
        return float(default)


def _is_multi_slice_override(orders: Optional[List[Dict[str, Any]]]) -> bool:
    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        has_index = order.get("slice_index") not in (None, "") or order.get("adaptive_slice_index") not in (None, "")
        slice_count = _safe_int(order.get("slice_count") or order.get("adaptive_slice_count"), 0)
        if bool(has_index) and int(slice_count) > 1:
            return True
    return False


def _alpaca_stream_url() -> str:
    if STREAM_URL:
        return str(STREAM_URL)
    base = str(BASE_URL or "").strip().lower()
    if "paper-api.alpaca.markets" in base:
        return "wss://paper-api.alpaca.markets/stream"
    if "api.alpaca.markets" in base:
        return "wss://api.alpaca.markets/stream"
    return str(BASE_URL or "https://paper-api.alpaca.markets").rstrip("/").replace("https://", "wss://").replace("http://", "ws://") + "/stream"


def _decode_ws_payload(message: Any) -> List[Dict[str, Any]]:
    if isinstance(message, (bytes, bytearray)):
        message = bytes(message).decode("utf-8")
    if isinstance(message, str):
        payload = json.loads(message or "{}")
    else:
        payload = message
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        return [dict(payload)]
    return []


def _alpaca_side_sign(side: Any) -> float:
    side_s = str(side or "").strip().lower()
    if side_s in {"sell", "sell_short", "short"}:
        return -1.0
    return 1.0


def _signed_alpaca_qty(qty: Any, side: Any) -> float:
    return float(abs(_safe_float(qty, 0.0))) * float(_alpaca_side_sign(side))


def _alpaca_event_ts_ms(update: Dict[str, Any], order: Dict[str, Any]) -> int:
    for value in (
        update.get("timestamp"),
        update.get("time"),
        update.get("updated_at"),
        order.get("updated_at"),
        order.get("filled_at"),
        order.get("canceled_at"),
        order.get("created_at"),
    ):
        if value not in (None, ""):
            return parse_broker_timestamp_ms(value, default_ms=int(time.time() * 1000))
    return int(time.time() * 1000)


def _alpaca_terminal_status(event: str, order: Dict[str, Any]) -> str:
    raw = str(order.get("status") or event or "").strip().lower()
    aliases = {
        "cancelled": "canceled",
        "partial_fill": "partially_filled",
        "fill": "filled",
    }
    return aliases.get(raw, raw or "unknown")


def _merge_order_extra(existing_json: Any, *, event_id: str, event: str, source: str, payload: Dict[str, Any]) -> str:
    try:
        existing = json.loads(existing_json or "{}")
        if not isinstance(existing, dict):
            existing = {}
    except Exception:
        existing = {}
    events = list(existing.get("alpaca_trade_update_events") or [])
    if event_id and event_id not in events:
        events.append(str(event_id))
    existing["alpaca_trade_update_events"] = events[-50:]
    existing["last_alpaca_trade_update"] = {
        "event_id": str(event_id or ""),
        "event": str(event or ""),
        "source": str(source or ""),
        "payload": dict(payload or {}),
    }
    return json.dumps(existing, separators=(",", ":"), sort_keys=True, default=str)


def _existing_fill_abs_qty(con, client_order_id: str) -> float:
    row = con.execute(
        """
        SELECT COALESCE(SUM(ABS(fill_qty)), 0.0)
        FROM execution_fills
        WHERE client_order_id=?
        """,
        (str(client_order_id),),
    ).fetchone()
    return float((row or [0.0])[0] or 0.0)


def apply_alpaca_trade_update(update: Dict[str, Any], *, source: str = "websocket", received_ts_ms: Optional[int] = None) -> Dict[str, Any]:
    """Apply one Alpaca ``trade_updates`` payload through the execution ledger.

    The WebSocket and REST poller both flow through this function. Fill writes
    are idempotent and delta-aware, so a poll after a WebSocket event recovers
    gaps without double-applying already observed partial fills.
    """
    payload = dict(update or {})
    if str(payload.get("stream") or "") == "trade_updates" and isinstance(payload.get("data"), dict):
        payload = dict(payload.get("data") or {})
    order = dict(payload.get("order") or payload)
    event = str(payload.get("event") or order.get("status") or "").strip().lower()
    broker_order_id = str(order.get("id") or payload.get("order_id") or payload.get("id") or "").strip()
    client_order_id = str(order.get("client_order_id") or payload.get("client_order_id") or broker_order_id or "").strip()
    symbol = str(order.get("symbol") or payload.get("symbol") or "").strip().upper()
    side = order.get("side") or payload.get("side")
    event_ts_ms = _alpaca_event_ts_ms(payload, order)
    received_ms = int(received_ts_ms or time.time() * 1000)
    detection_latency_ms = max(0, int(received_ms - int(event_ts_ms)))
    event_id = str(
        payload.get("execution_id")
        or payload.get("event_id")
        or payload.get("id")
        or f"{broker_order_id}:{event}:{event_ts_ms}:{payload.get('qty') or order.get('filled_qty') or ''}"
    ).strip()
    status = _alpaca_terminal_status(event, order)

    if not client_order_id:
        return {"ok": False, "status": "missing_client_order_id", "event": event}

    init_execution_ledger()

    def _write(conw) -> Dict[str, Any]:
        existing = conw.execute(
            """
            SELECT client_order_id, extra_json
            FROM execution_orders
            WHERE client_order_id=?
               OR (? <> '' AND broker_order_id=?)
            ORDER BY submit_ts_ms DESC
            LIMIT 1
            """,
            (str(client_order_id), str(broker_order_id), str(broker_order_id)),
        ).fetchone()
        if existing and existing[0]:
            client_id = str(existing[0])
            extra_json = existing[1]
        else:
            client_id = str(client_order_id)
            extra_json = None

        if existing:
            conw.execute(
                """
                UPDATE execution_orders
                SET status=?,
                    broker_order_id=COALESCE(NULLIF(?, ''), broker_order_id),
                    extra_json=?
                WHERE client_order_id=?
                """,
                (
                    str(status or "unknown"),
                    str(broker_order_id),
                    _merge_order_extra(
                        extra_json,
                        event_id=str(event_id),
                        event=str(event),
                        source=str(source),
                        payload={**payload, "fill_detection_latency_ms": int(detection_latency_ms)},
                    ),
                    str(client_id),
                ),
            )

        fill_events = {"fill", "partial_fill", "filled", "partially_filled"}
        if event not in fill_events and status not in fill_events:
            return {"ok": True, "status": "order_state_updated", "event": event, "client_order_id": str(client_id)}

        cumulative_abs = _safe_float(order.get("filled_qty"), 0.0)
        existing_abs = _existing_fill_abs_qty(conw, str(client_id))
        event_qty_abs = _safe_float(payload.get("qty"), 0.0)
        if event_qty_abs <= 0.0:
            event_qty_abs = max(0.0, float(cumulative_abs) - float(existing_abs))
        elif cumulative_abs > 0.0:
            event_qty_abs = min(float(event_qty_abs), max(0.0, float(cumulative_abs) - float(existing_abs)))

        if event_qty_abs <= 1e-12:
            return {
                "ok": True,
                "status": "duplicate_or_no_delta",
                "event": event,
                "client_order_id": str(client_id),
                "existing_abs_qty": float(existing_abs),
                "cumulative_abs_qty": float(cumulative_abs),
            }

        fill_px = _safe_float(payload.get("price") or order.get("filled_avg_price") or order.get("limit_price"), 0.0)
        if fill_px <= 0.0:
            return {"ok": False, "status": "missing_fill_price", "event": event, "client_order_id": str(client_id)}

        fill_id = str(event_id or f"{broker_order_id}:{source}:{cumulative_abs}:{event_ts_ms}")
        log_fill(
            client_order_id=str(client_id),
            fill_id=str(fill_id),
            broker="alpaca",
            symbol=str(symbol or order.get("symbol") or ""),
            qty=_signed_alpaca_qty(float(event_qty_abs), side),
            fill_px=float(fill_px),
            fill_ts_ms=int(event_ts_ms),
            fees=None,
            extra={
                **dict(payload or {}),
                "broker_order_id": str(broker_order_id),
                "source": str(source),
                "event": str(event),
                "event_id": str(event_id),
                "order_status": str(status),
                "cumulative_filled_qty": float(cumulative_abs),
                "existing_abs_qty_before_event": float(existing_abs),
                "fill_detection_latency_ms": int(detection_latency_ms),
                "liquidity": str(order.get("order_class") or ""),
            },
            con=conw,
        )
        return {
            "ok": True,
            "status": "fill_logged",
            "event": event,
            "client_order_id": str(client_id),
            "fill_id": str(fill_id),
            "fill_detection_latency_ms": int(detection_latency_ms),
        }

    result = run_write_txn(
        _write,
        table="execution_fills",
        operation="apply_alpaca_trade_update",
        context={"client_order_id": str(client_order_id), "event_id": str(event_id), "source": str(source)},
    )
    if bool((result or {}).get("ok")):
        emit_counter(
            "alpaca_trade_update_event",
            1,
            component="engine.execution.broker_alpaca_rest",
            broker="alpaca",
            symbol=(str(symbol) if symbol else None),
            extra_tags={"event": str(event or ""), "source": str(source or "")},
        )
        if str((result or {}).get("status") or "") == "fill_logged":
            emit_timing(
                "fill_detection_latency_ms",
                int(detection_latency_ms),
                component="engine.execution.broker_alpaca_rest",
                broker="alpaca",
                symbol=(str(symbol) if symbol else None),
                extra_tags={"source": str(source or "")},
            )
    return dict(result or {"ok": False, "status": "unknown"})


# ============================================================
# HTTP Helpers
# ============================================================

def _headers() -> Dict[str, str]:
    key_id = _alpaca_key_id()
    secret = _alpaca_secret_key()
    return {
        "APCA-API-KEY-ID": key_id,
        "APCA-API-SECRET-KEY": secret,
        "Content-Type": "application/json",
    }


def _req(method: str, path: str, payload: Optional[dict] = None, timeout_s: Optional[float] = None) -> Any:
    # Transport errors are allowed to raise here; callers decide whether a
    # given Alpaca failure is retryable, degradable, or execution-blocking.
    if not _alpaca_key_id() or not _alpaca_secret_key():
        raise AlpacaCredentialError("alpaca credentials missing")
    url = BASE_URL.rstrip("/") + path
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    r = urllib.request.Request(url, data=data, headers=_headers(), method=method.upper())
    try:
        timeout = max(0.1, min(20.0, float(timeout_s if timeout_s is not None else 20.0)))
    except Exception:
        timeout = 20.0
    try:
        with urllib.request.urlopen(r, timeout=float(timeout)) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        if int(getattr(exc, "code", 0) or 0) in (401, 403):
            raise AlpacaAuthenticationError(
                f"alpaca_auth_failed:{int(getattr(exc, 'code', 0) or 0)}",
                status_code=int(getattr(exc, "code", 0) or 0),
            ) from exc
        raise


def _real_trading_gate() -> Dict[str, Any]:
    if live_execution_disabled():
        return disabled_live_execution_gate(source="engine.execution.broker_alpaca_rest")
    if execution_gate_snapshot is None or kill_switch_snapshot is None or get_execution_mode is None:
        return {
            "ok": False,
            "reason": "execution_gate_providers_missing",
            "real_trading_allowed": False,
            "allowed": False,
        }
    return execution_gate_snapshot(
        get_execution_mode_fn=get_execution_mode,
        kill_switches=(kill_switch_snapshot() or {}),
    )


def _prelive_reconcile_or_block(broker: str = "alpaca") -> Optional[Dict[str, Any]]:
    policy_block = prelive_reconcile_policy_gate(
        source="engine.execution.broker_alpaca_rest",
        engine_mode="live",
        broker=str(broker),
        audit_override=True,
    )
    if policy_block is not None:
        return policy_block
    unrecorded_block = unrecorded_submission_gate(broker=str(broker), connect_fn=connect)
    if unrecorded_block is not None:
        return unrecorded_block
    if os.environ.get("EXECUTION_PRELIVE_RECONCILE", "1") != "1":
        return None
    if not callable(_prelive_reconcile):
        return {
            "ok": False,
            "status": "prelive_reconcile_unavailable",
            "broker": str(broker),
            "fatal_reconcile": True,
        }
    try:
        gate = _prelive_reconcile(broker=str(broker)) or {}
    except Exception as exc:
        _warn_nonfatal(
            "BROKER_ALPACA_PRELIVE_RECONCILE_FAILED",
            exc,
            once_key="prelive_reconcile",
            broker=str(broker),
        )
        return {
            "ok": False,
            "status": "prelive_reconcile_exception",
            "broker": str(broker),
            "fatal_reconcile": True,
            "error": str(exc),
        }
    if bool(gate.get("ok", False)):
        return None
    return {
        "ok": False,
        "status": str(gate.get("status") or "prelive_reconcile_block"),
        "broker": str(broker),
        "fatal_reconcile": True,
        "reconcile": dict(gate or {}),
    }


# ============================================================
# Account / Positions
# ============================================================

def get_account(timeout_s: Optional[float] = None) -> Dict[str, Any]:
    return _req("GET", "/v2/account", timeout_s=timeout_s)


def get_positions(timeout_s: Optional[float] = None) -> List[Dict[str, Any]]:
    res = _req("GET", "/v2/positions", timeout_s=timeout_s)
    return list(res or [])


def get_order(order_id: str, timeout_s: Optional[float] = None) -> Dict[str, Any]:
    return _req("GET", f"/v2/orders/{str(order_id)}", timeout_s=timeout_s)


def _alpaca_cancel_remaining_qty(order: Dict[str, Any]) -> float:
    qty = _safe_float((order or {}).get("qty"), 0.0)
    filled_qty = _safe_float((order or {}).get("filled_qty"), 0.0)
    remaining_abs = max(0.0, abs(float(qty)) - abs(float(filled_qty)))
    if remaining_abs <= 1e-9:
        return 0.0
    return _signed_alpaca_qty(float(remaining_abs), (order or {}).get("side"))


def _alpaca_cancel_status(order: Dict[str, Any]) -> str:
    return _alpaca_terminal_status(str((order or {}).get("status") or ""), dict(order or {}))


def cancel_order(order_id: str, timeout_s: Optional[float] = None) -> Dict[str, Any]:
    verify_timeout_s = float(
        timeout_s
        if timeout_s is not None
        else os.environ.get("ALPACA_CANCEL_VERIFY_TIMEOUT_S", "5.0")
    )
    poll_interval_s = float(os.environ.get("ALPACA_CANCEL_VERIFY_POLL_INTERVAL_S", "0.25"))
    audit = record_broker_action_audit(
        broker="alpaca",
        action="order_cancel_attempt",
        status="attempted",
        broker_order_id=str(order_id),
        payload={"order_id": str(order_id), "timeout_s": float(verify_timeout_s)},
    )
    if not bool(audit.get("ok")):
        return {"ok": False, **audit}
    cancel_response = _req("DELETE", f"/v2/orders/{str(order_id)}", timeout_s=verify_timeout_s)
    deadline = time.monotonic() + max(0.0, float(verify_timeout_s))
    last_order: Dict[str, Any] = dict(cancel_response or {}) if isinstance(cancel_response, dict) else {}
    last_error = ""
    while True:
        try:
            last_order = dict(
                get_order(
                    str(order_id),
                    timeout_s=min(float(verify_timeout_s), max(0.1, float(deadline - time.monotonic()))),
                )
                or {}
            )
            last_error = ""
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            last_order = {}

        status = _alpaca_cancel_status(last_order) if last_order else ""
        remaining_qty = _alpaca_cancel_remaining_qty(last_order) if last_order else 0.0
        if status in {"canceled", "cancelled", "api_cancelled"}:
            return {
                "ok": True,
                "broker": "alpaca",
                "status": "cancel_verified",
                "order_id": str(order_id),
                "broker_status": str(status),
                "cancel_requested": True,
                "cancel_verified": True,
                "terminal_cancel_verified": True,
                "zero_remaining_verified": abs(float(remaining_qty)) <= 1e-9,
                "remaining_qty": float(remaining_qty),
                "order": last_order,
                "cancel_response": cancel_response,
            }
        if last_order and abs(float(remaining_qty)) <= 1e-9:
            return {
                "ok": True,
                "broker": "alpaca",
                "status": "zero_remaining_verified",
                "order_id": str(order_id),
                "broker_status": str(status),
                "cancel_requested": True,
                "cancel_verified": True,
                "zero_remaining_verified": True,
                "remaining_qty": 0.0,
                "order": last_order,
                "cancel_response": cancel_response,
            }

        if time.monotonic() >= deadline:
            return {
                "ok": False,
                "broker": "alpaca",
                "status": "cancel_not_verified",
                "order_id": str(order_id),
                "broker_status": str(status),
                "cancel_requested": True,
                "cancel_verified": False,
                "terminal_cancel_verified": False,
                "zero_remaining_verified": False,
                "remaining_qty": float(remaining_qty),
                "order": last_order,
                "cancel_response": cancel_response,
                "last_error": str(last_error),
                "timeout_s": float(verify_timeout_s),
            }

        wait_s = min(max(0.01, float(poll_interval_s)), max(0.01, float(deadline - time.monotonic())))
        wait_with_kill_interrupt(wait_s)


def replace_limit_order(
    *,
    order_id: str,
    symbol: str,
    qty: float,
    limit_price: float,
    client_oid: Optional[str] = None,
) -> Dict[str, Any]:
    gate = _real_trading_gate()
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "alpaca"}
    reconcile_block = _prelive_reconcile_or_block("alpaca")
    if reconcile_block is not None:
        return reconcile_block
    credentials_block = _alpaca_credentials_block()
    if credentials_block is not None:
        return credentials_block
    audit = record_broker_action_audit(
        broker="alpaca",
        action="order_replace_attempt",
        status="attempted",
        symbol=str(symbol),
        qty=float(qty),
        client_order_id=str(client_oid or ""),
        broker_order_id=str(order_id),
        payload={"order_type": "LIMIT", "limit_price": float(limit_price)},
    )
    if not bool(audit.get("ok")):
        return {"ok": False, **audit}
    raw = _req("PATCH", f"/v2/orders/{str(order_id)}", {"limit_price": str(float(limit_price))})
    order = dict(raw or {}) if isinstance(raw, dict) else {}
    return {
        **order,
        "ok": True,
        "broker": "alpaca",
        "status": "native_replace_verified",
        "broker_status": str(order.get("status") or ""),
        "replace_verified": True,
        "broker_order_id": str(order.get("id") or order_id),
        "order_id": str(order.get("id") or order_id),
        "order": order,
    }


def list_orders(
    status: str = "all",
    limit: int = 500,
    after_ts_ms: Optional[int] = None,
    timeout_s: Optional[float] = None,
) -> List[Dict[str, Any]]:
    parts = [f"status={status}", "direction=asc", f"limit={int(limit)}"]
    if after_ts_ms is not None:
        dt = datetime.fromtimestamp(float(after_ts_ms) / 1000.0, tz=timezone.utc)
        after = dt.isoformat().replace("+00:00", "Z")
        parts.append(f"after={after}")
    path = "/v2/orders?" + "&".join(parts)
    res = _req("GET", path, timeout_s=timeout_s)
    return list(res or [])


def list_open_orders(limit: int = 500, timeout_s: Optional[float] = None) -> List[Dict[str, Any]]:
    return list_orders(status="open", limit=int(limit), timeout_s=timeout_s)


def _shutdown_client_order_id(command_id: str, symbol: str, qty: float) -> str:
    raw = f"alpaca:shutdown_flatten:{command_id}:{symbol}:{float(qty):.12g}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"alp_flat_{digest}"


def _position_qty(position: Dict[str, Any]) -> float:
    return _safe_float((position or {}).get("qty"), 0.0)


def cancel_open_orders(timeout_s: float = 10.0, command_id: Optional[str] = None) -> Dict[str, Any]:
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    try:
        orders = list_open_orders(timeout_s=max(0.1, float(deadline - time.monotonic())))
    except Exception as exc:
        _warn_nonfatal(
            "ALPACA_SHUTDOWN_OPEN_ORDERS_LIST_FAILED",
            exc,
            once_key="shutdown_open_orders_list",
            command_id=str(command_id or ""),
        )
        return {
            "ok": False,
            "broker": "alpaca",
            "status": "open_orders_list_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }

    results: List[Dict[str, Any]] = []
    failed_n = 0
    cancelled_n = 0
    audit = record_broker_action_audit(
        broker="alpaca",
        action="cancel_open_orders_attempt",
        status="attempted",
        mode=str(os.environ.get("ENGINE_MODE", "")),
        payload={
            "command_id": str(command_id or ""),
            "open_order_count": int(len(orders or [])),
            "shutdown_cancel": True,
        },
    )
    if not bool(audit.get("ok")):
        return {
            "ok": False,
            "broker": "alpaca",
            "status": "broker_action_audit_failed",
            "command_id": str(command_id or ""),
            "audit": audit,
        }
    for order in list(orders or []):
        oid = str((order or {}).get("id") or (order or {}).get("order_id") or "").strip()
        if not oid:
            failed_n += 1
            results.append({"ok": False, "status": "missing_order_id", "order": dict(order or {})})
            continue
        if time.monotonic() >= deadline:
            failed_n += 1
            results.append({"ok": False, "status": "cancel_timeout_before_order", "order_id": oid})
            break
        try:
            result = cancel_order(oid, timeout_s=max(0.1, float(deadline - time.monotonic())))
        except Exception as exc:
            _warn_nonfatal(
                "ALPACA_SHUTDOWN_CANCEL_ORDER_FAILED",
                exc,
                once_key=f"shutdown_cancel_order:{oid}",
                command_id=str(command_id or ""),
                order_id=oid,
            )
            result = {
                "ok": False,
                "broker": "alpaca",
                "status": "cancel_exception",
                "order_id": oid,
                "error": f"{type(exc).__name__}: {exc}",
            }
        if bool(result.get("ok")):
            cancelled_n += 1
        else:
            failed_n += 1
        results.append(dict(result or {}))

    return {
        "ok": failed_n == 0,
        "broker": "alpaca",
        "status": "cancel_open_orders_complete" if failed_n == 0 else "cancel_open_orders_incomplete",
        "command_id": str(command_id or ""),
        "open_order_count": int(len(orders or [])),
        "cancelled_n": int(cancelled_n),
        "failed_n": int(failed_n),
        "results": results,
    }


def flatten_positions(
    *,
    timeout_s: float = 10.0,
    command_id: str,
    max_abs_qty_per_symbol: float,
    max_total_abs_qty: float,
) -> Dict[str, Any]:
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    try:
        positions = get_positions(timeout_s=max(0.1, float(deadline - time.monotonic())))
    except Exception as exc:
        _warn_nonfatal(
            "ALPACA_SHUTDOWN_POSITIONS_FETCH_FAILED",
            exc,
            once_key="shutdown_positions_fetch",
            command_id=str(command_id or ""),
        )
        return {
            "ok": False,
            "broker": "alpaca",
            "status": "positions_fetch_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }

    flatten_rows: List[Dict[str, Any]] = []
    total_abs_qty = 0.0
    for position in list(positions or []):
        symbol = str((position or {}).get("symbol") or "").strip().upper()
        qty = _position_qty(dict(position or {}))
        if not symbol or abs(float(qty)) <= 1e-9:
            continue
        total_abs_qty += abs(float(qty))
        if abs(float(qty)) > float(max_abs_qty_per_symbol):
            return {
                "ok": False,
                "broker": "alpaca",
                "status": "flatten_symbol_qty_limit_exceeded",
                "symbol": symbol,
                "qty": float(qty),
                "max_abs_qty_per_symbol": float(max_abs_qty_per_symbol),
            }
        flatten_rows.append({"symbol": symbol, "position_qty": float(qty), "flatten_qty": -float(qty)})

    if total_abs_qty > float(max_total_abs_qty):
        return {
            "ok": False,
            "broker": "alpaca",
            "status": "flatten_total_qty_limit_exceeded",
            "total_abs_qty": float(total_abs_qty),
            "max_total_abs_qty": float(max_total_abs_qty),
        }

    submitted: List[Dict[str, Any]] = []
    failed_n = 0
    for row in flatten_rows:
        if time.monotonic() >= deadline:
            failed_n += 1
            submitted.append({"ok": False, "status": "flatten_timeout_before_submit", **row})
            break
        symbol = str(row["symbol"])
        qty = float(row["flatten_qty"])
        client_oid = _shutdown_client_order_id(str(command_id), symbol, qty)
        audit = record_broker_action_audit(
            broker="alpaca",
            action="position_flatten_attempt",
            status="attempted",
            symbol=symbol,
            qty=float(qty),
            client_order_id=client_oid,
            payload={
                "command_id": str(command_id),
                "position_qty": float(row["position_qty"]),
                "order_type": "MARKET",
                "shutdown_flatten": True,
            },
        )
        if not bool(audit.get("ok")):
            failed_n += 1
            submitted.append({"ok": False, "status": "broker_action_audit_failed", **row, "audit": audit})
            continue
        try:
            response = _submit_market_order(symbol, qty, client_oid, timeout_s=max(0.1, float(deadline - time.monotonic())))
            submitted.append(
                {
                    "ok": True,
                    "status": "flatten_submitted",
                    **row,
                    "client_order_id": client_oid,
                    "broker_order_id": str((response or {}).get("id") or ""),
                    "response": dict(response or {}) if isinstance(response, dict) else response,
                }
            )
        except Exception as exc:
            failed_n += 1
            submitted.append(
                {
                    "ok": False,
                    "status": "flatten_submit_exception",
                    **row,
                    "client_order_id": client_oid,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    return {
        "ok": failed_n == 0,
        "broker": "alpaca",
        "status": "flatten_positions_submitted" if failed_n == 0 else "flatten_positions_incomplete",
        "command_id": str(command_id),
        "position_count": int(len(flatten_rows)),
        "submitted_n": int(sum(1 for item in submitted if bool(item.get("ok")))),
        "failed_n": int(failed_n),
        "total_abs_qty": float(total_abs_qty),
        "results": submitted,
    }


def list_orders_after(after_ts_ms: int, status: str = "all", limit: int = 500) -> List[Dict[str, Any]]:
    return list_orders(status=status, limit=int(limit), after_ts_ms=int(after_ts_ms))


# ============================================================
# Intent Loader
# ============================================================

def _latest_order_row(con) -> Optional[Tuple[int, int, list]]:
    from engine.strategy.portfolio_execution_intents import load_latest_execution_intents
    # The adapter consumes canonical execution intents rather than reconstructing
    # orders from older tables, so broker behavior tracks the current pipeline.
    b = load_latest_execution_intents(con)
    orders = list(b.get("intents") or [])
    if not orders:
        return None
    bid = b.get("batch_id")
    bts = b.get("batch_ts_ms")
    try:
        bid_i = int(bid) if bid is not None else None
    except Exception:
        bid_i = None
    try:
        bts_i = int(bts) if bts is not None else int(time.time() * 1000)
    except Exception:
        bts_i = int(time.time() * 1000)
    return bid_i if bid_i is not None else 0, bts_i, orders


# ============================================================
# Pricing Helpers
# ============================================================

def _price_at_or_before(con, symbol: str, ts_ms: int) -> Optional[float]:
    # Use point-in-time prices so paper/live audit math does not peek forward.
    try:
        r = con.execute(
            """
            SELECT price
            FROM prices
            WHERE symbol=? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
        if not r:
            return None
        return float(r[0])
    except Exception as e:
        _warn_nonfatal(
            "ALPACA_LAST_PRICE_LOOKUP_FAILED",
            e,
            once_key="last_price_lookup",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return None


def _alpaca_pos_map(positions: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for p in positions or []:
        try:
            sym = str(p.get("symbol") or "").upper().strip()
            qty = float(p.get("qty") or 0.0)
            if sym:
                out[sym] = qty
        except Exception as e:
            _warn_nonfatal(
                "ALPACA_POSITION_PARSE_FAILED",
                e,
                once_key="position_parse",
                position=str(p)[:200],
            )
            continue
    return out


def _load_latest_prices(con) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for sql in (
        """
        SELECT symbol, price
        FROM prices
        WHERE ts_ms IN (SELECT MAX(ts_ms) FROM prices GROUP BY symbol)
        """,
        """
        SELECT symbol, px
        FROM prices
        WHERE ts_ms IN (SELECT MAX(ts_ms) FROM prices GROUP BY symbol)
        """,
    ):
        try:
            rows = con.execute(sql).fetchall() or []
        except Exception:
            rows = []
        for sym, px in rows:
            sym_u = str(sym or "").upper().strip()
            if not sym_u:
                continue
            try:
                px_f = float(px or 0.0)
            except Exception:
                px_f = 0.0
            if px_f > 0.0:
                out[sym_u] = px_f
        if out:
            break
    return out


def _book_exposure_notional(positions: Dict[str, float], prices: Dict[str, float]) -> Tuple[float, float]:
    gross = 0.0
    net = 0.0
    for sym, qty in (positions or {}).items():
        px = float(prices.get(str(sym or "").upper().strip()) or 0.0)
        if px <= 0.0:
            continue
        signed = float(qty or 0.0) * float(px)
        gross += abs(float(signed))
        net += float(signed)
    return float(gross), float(net)


def _max_scale_for_metric(metric_fn, cap: float) -> float:
    eps = 1e-9
    cap_f = float(cap)
    current = float(metric_fn(0.0))
    projected = float(metric_fn(1.0))

    if projected <= cap_f + eps:
        return 1.0
    if projected <= current + eps:
        return 1.0
    if current >= cap_f - eps:
        return 0.0

    lo = 0.0
    hi = 1.0
    for _ in range(48):
        mid = (lo + hi) / 2.0
        if float(metric_fn(mid)) <= cap_f + eps:
            lo = mid
        else:
            hi = mid
    return float(max(0.0, min(1.0, lo)))


def _apply_execution_risk_caps(
    *,
    positions: Dict[str, float],
    prices: Dict[str, float],
    symbol: str,
    current_qty: float,
    delta_qty: float,
    px: float,
    equity: float,
) -> Tuple[float, Dict[str, Any]]:
    sym = str(symbol or "").upper().strip()
    px_f = float(px or 0.0)
    eq_f = float(equity or 0.0)
    delta_f = float(delta_qty or 0.0)
    cur_qty_f = float(current_qty or 0.0)

    if (not sym) or px_f <= 0.0 or eq_f <= 0.0 or abs(delta_f) <= 1e-9:
        return delta_f, {"applied": False, "scale": 1.0}

    prices_local = dict(prices or {})
    prices_local[sym] = float(px_f)

    gross_cur, net_cur = _book_exposure_notional(positions or {}, prices_local)
    cur_sym_notional = float(cur_qty_f) * float(px_f)
    delta_notional = float(delta_f) * float(px_f)
    other_gross = max(0.0, float(gross_cur) - abs(float(cur_sym_notional)))

    total_cap = max(0.0, float(EXEC_TOTAL_EXPOSURE_CAP)) * float(eq_f)
    symbol_cap = max(0.0, float(EXEC_SYMBOL_CONCENTRATION_CAP)) * float(eq_f)
    direction_cap = max(0.0, float(EXEC_DIRECTION_CONCENTRATION_CAP)) * float(eq_f)

    total_scale = _max_scale_for_metric(
        lambda s: float(other_gross) + abs(float(cur_sym_notional) + (float(s) * float(delta_notional))),
        total_cap,
    )
    symbol_scale = _max_scale_for_metric(
        lambda s: abs(float(cur_sym_notional) + (float(s) * float(delta_notional))),
        symbol_cap,
    )
    direction_scale = _max_scale_for_metric(
        lambda s: abs(float(net_cur) + (float(s) * float(delta_notional))),
        direction_cap,
    )

    scale = max(0.0, min(1.0, float(total_scale), float(symbol_scale), float(direction_scale)))
    scaled_delta = float(delta_f) * float(scale)
    projected_sym_notional = float(cur_sym_notional) + float(scaled_delta) * float(px_f)
    projected_total_gross = float(other_gross) + abs(float(projected_sym_notional))
    projected_net = float(net_cur) + (float(scaled_delta) * float(px_f))

    audit = {
        "applied": True,
        "scale": float(scale),
        "scaled": bool(scale < 0.999999),
        "caps": {
            "total_exposure_cap": float(total_cap),
            "symbol_concentration_cap": float(symbol_cap),
            "direction_concentration_cap": float(direction_cap),
        },
        "factors": {
            "total_exposure": float(total_scale),
            "symbol_concentration": float(symbol_scale),
            "direction_concentration": float(direction_scale),
        },
        "pre": {
            "gross_notional": float(gross_cur),
            "net_notional": float(net_cur),
            "symbol_notional": float(cur_sym_notional),
            "delta_notional": float(delta_notional),
        },
        "post": {
            "gross_notional": float(projected_total_gross),
            "net_notional": float(projected_net),
            "symbol_notional": float(projected_sym_notional),
            "delta_notional": float(scaled_delta * float(px_f)),
        },
    }
    return float(scaled_delta), audit


# ============================================================
# Order Submission
# ============================================================

def _submit_market_order(symbol: str, qty: float, client_oid: str, timeout_s: Optional[float] = None) -> Dict[str, Any]:
    side = "buy" if qty > 0 else "sell"
    payload = {
        "symbol": symbol,
        "qty": str(abs(qty)),
        "side": side,
        "type": "market",
        "time_in_force": ORDER_TIF,
        "client_order_id": client_oid,
    }
    return _req("POST", "/v2/orders", payload, timeout_s=timeout_s)


def _submit_limit_order(
    symbol: str,
    qty: float,
    limit_price: float,
    client_oid: str,
    timeout_s: Optional[float] = None,
) -> Dict[str, Any]:
    side = "buy" if qty > 0 else "sell"
    payload = {
        "symbol": symbol,
        "qty": str(abs(qty)),
        "side": side,
        "type": "limit",
        "time_in_force": ORDER_TIF,
        "limit_price": str(float(limit_price)),
        "client_order_id": client_oid,
    }
    return _req("POST", "/v2/orders", payload, timeout_s=timeout_s)


def submit_limit_order(symbol: str, qty: float, limit_price: float, client_oid: str) -> Dict[str, Any]:
    gate = _real_trading_gate()
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "alpaca"}
    reconcile_block = _prelive_reconcile_or_block("alpaca")
    if reconcile_block is not None:
        return reconcile_block
    credentials_block = _alpaca_credentials_block()
    if credentials_block is not None:
        return credentials_block
    audit = record_broker_action_audit(
        broker="alpaca",
        action="order_submit_attempt",
        status="attempted",
        symbol=str(symbol),
        qty=float(qty),
        client_order_id=str(client_oid),
        payload={"order_type": "LIMIT", "limit_price": float(limit_price)},
    )
    if not bool(audit.get("ok")):
        return {"ok": False, **audit}
    try:
        return _submit_limit_order(symbol, qty, limit_price, client_oid)
    except (AlpacaCredentialError, AlpacaAuthenticationError) as exc:
        failure = _alpaca_exception_failure(exc)
        if failure is not None:
            _warn_nonfatal(
                "ALPACA_ORDER_SUBMIT_TERMINAL_CREDENTIAL_FAILURE",
                exc,
                once_key=f"order_submit_limit:{failure.get('status')}",
                status=str(failure.get("status") or ""),
                symbol=str(symbol),
            )
            return failure
        raise


def submit_market_order(symbol: str, qty: float, client_oid: str) -> Dict[str, Any]:
    gate = _real_trading_gate()
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "alpaca"}
    reconcile_block = _prelive_reconcile_or_block("alpaca")
    if reconcile_block is not None:
        return reconcile_block
    credentials_block = _alpaca_credentials_block()
    if credentials_block is not None:
        return credentials_block
    audit = record_broker_action_audit(
        broker="alpaca",
        action="order_submit_attempt",
        status="attempted",
        symbol=str(symbol),
        qty=float(qty),
        client_order_id=str(client_oid),
        payload={"order_type": "MARKET"},
    )
    if not bool(audit.get("ok")):
        return {"ok": False, **audit}
    try:
        return _submit_market_order(symbol, qty, client_oid)
    except (AlpacaCredentialError, AlpacaAuthenticationError) as exc:
        failure = _alpaca_exception_failure(exc)
        if failure is not None:
            _warn_nonfatal(
                "ALPACA_ORDER_SUBMIT_TERMINAL_CREDENTIAL_FAILURE",
                exc,
                once_key=f"order_submit_market:{failure.get('status')}",
                status=str(failure.get("status") or ""),
                symbol=str(symbol),
            )
            return failure
        raise


def _limit_from_px(px: float, qty: float, aggressiveness: str) -> float:
    a = str(aggressiveness or "").upper().strip()
    if a == "PASSIVE":
        off = LIM_OFF_BPS_PASSIVE
    elif a == "NEUTRAL":
        off = LIM_OFF_BPS_NEUTRAL
    else:
        off = LIM_OFF_BPS_AGGR

    if qty > 0:
        return px * (1.0 - off / 10000.0)
    return px * (1.0 + off / 10000.0)


# ============================================================
# Core Execution
# ============================================================

def apply_latest_portfolio_orders_live(
    dry_run: bool = False,
    override_orders: Optional[List[Dict[str, Any]]] = None,
    override_order_id: Optional[int] = None,
    override_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:

    if not bool(dry_run):
        gate = _real_trading_gate()
        if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
            return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "alpaca"}

    credentials_block = _alpaca_credentials_block()
    if credentials_block is not None:
        return credentials_block

    if not bool(dry_run):
        reconcile_block = _prelive_reconcile_or_block("alpaca")
        if reconcile_block is not None:
            return reconcile_block

    con = connect()
    try:

        if override_orders is not None:
            order_id = (int(override_order_id) if override_order_id is not None else None)
            ts_ms = int(override_ts_ms) if override_ts_ms is not None else int(time.time() * 1000)
            orders = list(override_orders or [])
        else:
            latest = _latest_order_row(con)
            if not latest:
                return {"ok": True, "status": "no_orders", "broker": "alpaca"}
            order_id, ts_ms, orders = latest

        # ALE integration
        try:
            orders_ale, ale_meta = apply_alpha_lifecycle(
                con=con,
                portfolio_orders_id=order_id,
                portfolio_ts_ms=int(ts_ms),
                orders=list(orders or []),
            )
        except Exception:
            orders_ale, ale_meta = list(orders or []), {"ok": False, "error": "ale_failed"}
        multi_slice_override = bool(override_orders is not None and _is_multi_slice_override(orders_ale))

        if not bool(dry_run):
            options_block = live_options_order_block(
                orders_ale,
                broker="alpaca",
                dry_run=False,
                engine_mode=os.environ.get("ENGINE_MODE", ""),
                execution_mode=os.environ.get("EXECUTION_MODE", ""),
            )
            if options_block is not None:
                return options_block

        # idempotency
        if order_id is not None and not bool(multi_slice_override):
            last_applied = get_state("alpaca_last_portfolio_orders_id", "0")
            try:
                if int(last_applied) >= int(order_id):
                    return {"ok": True, "status": "already_applied", "broker": "alpaca"}
            except Exception as e:
                _warn_nonfatal(
                    "BROKER_ALPACA_LAST_APPLIED_PARSE_FAILED",
                    e,
                    once_key="alpaca_last_applied_parse",
                    order_id=order_id,
                    last_applied=last_applied,
                )

        allow0, _, _ = execution_allowed(con=con, symbol=None, regime=None)
        if not allow0:
            return {"ok": False, "status": "blocked_kill_switch_global"}

        try:
            acct = get_account()
        except (AlpacaCredentialError, AlpacaAuthenticationError) as exc:
            failure = _alpaca_exception_failure(exc)
            if failure is not None:
                _warn_nonfatal(
                    "ALPACA_ACCOUNT_TERMINAL_CREDENTIAL_FAILURE",
                    exc,
                    once_key=f"account_read:{failure.get('status')}",
                    status=str(failure.get("status") or ""),
                )
                return failure
            raise
        eq = float(acct.get("equity") or 0.0)
        bp = float(acct.get("buying_power") or 0.0)
        cash = float(acct.get("cash") or 0.0)

        eq = float(
            compute_deployable_equity(
                {"equity": float(eq), "buying_power": float(bp), "cash": float(cash)},
                default_equity=float(eq),
            )
            or 0.0
        )
        if eq <= 0:
            return {"ok": False, "status": "nonpositive_equity"}

        pos = _alpaca_pos_map(get_positions())
        latest_prices = _load_latest_prices(con)

        if dry_run:
            return {
                "ok": True,
                "status": "dry_run_preview",
                "orders": orders_ale,
                "positions": pos,
                "ale": ale_meta,
            }

        submitted = []
        n = 0

        for o in orders_ale[: int(MAX_ORDERS_PER_PASS)]:

            symbol = str(o.get("symbol") or "").strip().upper()
            if not symbol:
                continue

            allow_sym, _, _ = execution_allowed(con=con, symbol=symbol, regime=None)
            if not allow_sym:
                continue

            to_side = str(o.get("to_side") or "FLAT").upper().strip()
            to_w = float(o.get("to_weight") or 0.0)

            px = _price_at_or_before(con, symbol, int(ts_ms))
            if px is None or px <= 0:
                continue

            cur_qty = float(pos.get(symbol, 0.0))
            raw_qty = _safe_float(o.get("qty"), 0.0)
            if abs(float(raw_qty)) > 0.0:
                delta = float(raw_qty)
            else:
                target_qty = (to_w * eq) / px
                if to_side == "SHORT":
                    target_qty = -abs(target_qty)
                elif to_side == "LONG":
                    target_qty = abs(target_qty)
                else:
                    target_qty = 0.0
                delta = float(target_qty - cur_qty)
            if abs(delta) < 1e-6:
                continue

            delta, risk_cap_audit = _apply_execution_risk_caps(
                positions=pos,
                prices=latest_prices,
                symbol=symbol,
                current_qty=cur_qty,
                delta_qty=delta,
                px=float(px),
                equity=float(eq),
            )
            if abs(delta) < 1e-6:
                pos[symbol] = float(cur_qty)
                continue

            order_type = str(o.get("order_type") or ORDER_TYPE).upper().strip()
            aggressiveness = str(o.get("aggressiveness") or "").upper().strip()
            order_meta = dict(o or {})
            order_meta["portfolio_risk_caps"] = dict(risk_cap_audit or {})

            audit = record_broker_action_audit(
                broker="alpaca",
                action="order_submit_attempt",
                status="attempted",
                symbol=str(symbol),
                qty=float(delta),
                portfolio_orders_id=(int(order_id) if order_id is not None else None),
                mode=str(order_meta.get("execution_mode") or ""),
                payload={
                    "order_type": str(order_type or "MARKET"),
                    "aggressiveness": str(aggressiveness or ""),
                    "source_order_id": o.get("source_order_id"),
                    "source_alert_id": o.get("source_alert_id"),
                },
            )
            if not bool(audit.get("ok")):
                return {
                    "ok": False,
                    "status": "broker_action_audit_failed",
                    "broker": "alpaca",
                    "stop_failover": True,
                    "detail": "pre_submit_audit_failed",
                    "symbol": str(symbol),
                    "submitted_n": int(n),
                    "audit": dict(audit or {}),
                }

            guard = claim_order_submission_durable(
                broker="alpaca",
                portfolio_orders_id=order_id,
                portfolio_ts_ms=int(ts_ms),
                order=o,
                connect_fn=connect,
            )
            if not bool(guard.get("ok")):
                return {
                    "ok": False,
                    "status": str(guard.get("status") or "order_idempotency_claim_failed"),
                    "broker": "alpaca",
                    "stop_failover": True,
                    "detail": "order_idempotency_claim_failed",
                    "order_uid": str(guard.get("order_uid") or ""),
                    "client_order_id": str(guard.get("client_order_id") or ""),
                    "symbol": str(symbol),
                    "submitted_n": int(n),
                }
            if bool(guard.get("duplicate")):
                continue

            order_uid = str(guard.get("order_uid") or "")
            client_oid = str(guard.get("client_order_id") or "")
            limit_px = None

            try:
                if order_type == "LIMIT":
                    limit_px = _limit_from_px(px, delta, aggressiveness)
                    policy_offset_bps = float(order_meta.get("entry_limit_offset_bps") or 0.0)
                    if policy_offset_bps > 0.0:
                        if float(delta) > 0:
                            limit_px = float(limit_px) + ((float(policy_offset_bps) / 10000.0) * float(px))
                        else:
                            limit_px = float(limit_px) - ((float(policy_offset_bps) / 10000.0) * float(px))
                    res = _submit_limit_order(symbol, delta, limit_px, client_oid)
                else:
                    res = _submit_market_order(symbol, delta, client_oid)
            except (AlpacaCredentialError, AlpacaAuthenticationError) as e:
                failure = _alpaca_exception_failure(e)
                if failure is not None:
                    failure.update(
                        {
                            "order_uid": str(order_uid),
                            "client_order_id": str(client_oid),
                            "symbol": str(symbol),
                            "submitted_n": int(n),
                        }
                    )
                    _warn_nonfatal(
                        "ALPACA_PORTFOLIO_SUBMIT_TERMINAL_CREDENTIAL_FAILURE",
                        e,
                        once_key=f"portfolio_submit:{failure.get('status')}",
                        status=str(failure.get("status") or ""),
                        symbol=str(symbol),
                    )
                    return failure
                raise
            except Exception as e:
                try:
                    mark_order_submission_unknown_durable(
                        order_uid=order_uid,
                        last_error=str(e),
                        connect_fn=connect,
                    )
                except Exception as mark_err:
                    _warn_nonfatal(
                        "BROKER_ALPACA_MARK_ORDER_SUBMISSION_UNKNOWN_FAILED",
                        mark_err,
                        once_key="alpaca_mark_order_submission_unknown",
                        order_uid=str(order_uid),
                        client_order_id=str(client_oid),
                        symbol=str(symbol),
                    )
                return {
                    "ok": False,
                    "status": "submit_inflight_unknown",
                    "broker": "alpaca",
                    "stop_failover": True,
                    "detail": "broker_submit_ambiguous",
                    "order_uid": str(order_uid),
                    "client_order_id": str(client_oid),
                    "symbol": str(symbol),
                    "error": str(e),
                    "submitted_n": int(n),
                }

            broker_order_id = str((res or {}).get("id") or "")
            source_alert_id = (
                _safe_int(o.get("source_alert_id"))
                if isinstance(o, dict) and o.get("source_alert_id") is not None
                else None
            )
            submit_ts_ms = int(time.time() * 1000)
            try:
                log_submit(
                    client_order_id=client_oid,
                    broker="alpaca",
                    symbol=symbol,
                    qty=delta,
                    submit_ts_ms=int(submit_ts_ms),
                    ref_px=float(px),
                    broker_order_id=broker_order_id,
                    portfolio_orders_id=order_id,
                    source_alert_id=source_alert_id,
                    extra={**dict(order_meta or {}), "order_uid": str(order_uid), "idempotency_status": "submitted"},
                    order_uid=str(order_uid),
                    idempotency_status="submitted",
                )
            except Exception as e:
                _warn_nonfatal(
                    "BROKER_ALPACA_LOG_SUBMIT_FAILED",
                    e,
                    once_key="alpaca_log_submit",
                    symbol=str(symbol),
                    client_order_id=str(client_oid),
                    order_uid=str(order_uid),
                )
                return record_submission_unrecorded(
                    con=con,
                    broker="alpaca",
                    symbol=str(symbol),
                    qty=float(delta),
                    order_uid=str(order_uid),
                    client_order_id=str(client_oid),
                    broker_order_id=broker_order_id,
                    submit_ts_ms=int(submit_ts_ms),
                    portfolio_orders_id=(int(order_id) if order_id is not None else None),
                    portfolio_ts_ms=int(ts_ms),
                    source_order_id=(
                        _safe_int(o.get("source_order_id"))
                        if isinstance(o, dict) and o.get("source_order_id") is not None
                        else None
                    ),
                    source_alert_id=source_alert_id,
                    payload={**dict(order_meta or {}), "order_uid": str(order_uid), "idempotency_status": "submission_unrecorded"},
                    error=e,
                    stage="log_submit",
                    submitted_n=int(n),
                    durable_idempotency=True,
                    connect_fn=connect,
                )

            try:
                mark_order_submission_submitted_durable(
                    order_uid=str(order_uid),
                    client_order_id=str(client_oid),
                    broker_order_id=broker_order_id,
                    submit_ts_ms=int(submit_ts_ms),
                    connect_fn=connect,
                )
            except Exception as e:
                _warn_nonfatal(
                    "BROKER_ALPACA_MARK_ORDER_SUBMISSION_SUBMITTED_FAILED",
                    e,
                    once_key="alpaca_mark_order_submission_submitted",
                    symbol=str(symbol),
                    client_order_id=str(client_oid),
                    order_uid=str(order_uid),
                )
                return record_submission_unrecorded(
                    con=con,
                    broker="alpaca",
                    symbol=str(symbol),
                    qty=float(delta),
                    order_uid=str(order_uid),
                    client_order_id=str(client_oid),
                    broker_order_id=broker_order_id,
                    submit_ts_ms=int(submit_ts_ms),
                    portfolio_orders_id=(int(order_id) if order_id is not None else None),
                    portfolio_ts_ms=int(ts_ms),
                    source_order_id=(
                        _safe_int(o.get("source_order_id"))
                        if isinstance(o, dict) and o.get("source_order_id") is not None
                        else None
                    ),
                    source_alert_id=source_alert_id,
                    payload={**dict(order_meta or {}), "order_uid": str(order_uid), "idempotency_status": "submission_unrecorded"},
                    error=e,
                    stage="mark_order_submission_submitted",
                    submitted_n=int(n),
                    durable_idempotency=True,
                    connect_fn=connect,
                )

            try:
                if (
                    order_type == "LIMIT"
                    and limit_px is not None
                    and str(aggressiveness or "").upper().strip() == "PASSIVE"
                    and bool(o.get("cancel_replace") or False)
                    and int(o.get("max_reprice_attempts") or 0) > 0
                ):
                    from engine.execution.execution_microstructure import record_open_order

                    record_open_order(
                        broker="alpaca",
                        symbol=symbol,
                        qty=float(delta),
                        order_type=str(order_type),
                        aggressiveness=str(aggressiveness),
                        limit_px=float(limit_px),
                        client_order_id=str(client_oid),
                        broker_order_id=broker_order_id,
                        max_attempts=int(o.get("max_reprice_attempts") or 0),
                        portfolio_orders_id=order_id,
                        source_alert_id=source_alert_id,
                        meta={
                            "escalation_enabled": True,
                            "escalation_timeout_s": float(
                                o.get("escalation_timeout_s")
                                or o.get("epe_reprice_interval_s")
                                or os.environ.get("EPE_REPRICE_INTERVAL_S", "60")
                            ),
                            "escalation_path": ["PASSIVE", "NEUTRAL", "AGGRESSIVE", "MARKET"],
                            "original_order": dict(o),
                            "portfolio_risk_caps": dict(risk_cap_audit or {}),
                        },
                    )
            except Exception as e:
                _warn_nonfatal(
                    "BROKER_ALPACA_RECORD_OPEN_ORDER_FAILED",
                    e,
                    once_key="alpaca_record_open_order",
                    symbol=str(symbol),
                    client_order_id=str(client_oid),
                    order_uid=str(order_uid),
                )

            pos[symbol] = float(cur_qty) + float(delta)
            submitted.append({"symbol": symbol, "delta_qty": delta})
            n += 1
            wait_ok, wait_reason, wait_meta = wait_with_kill_interrupt(
                delay_s=max(0.0, float(SLEEP_BETWEEN_ORDERS_S)),
                con=con,
                symbol=str(symbol),
                model_id=str(order_meta.get("model_id") or ""),
                broker="alpaca",
                component="engine.execution.broker_alpaca_rest",
                stage="adapter_order_sleep",
            )
            if not bool(wait_ok):
                return {
                    "ok": False,
                    "status": "blocked_kill_switch_mid_slice",
                    "broker": "alpaca",
                    "reason": str(wait_reason or "kill_switch_block"),
                    "kill_meta": dict(wait_meta or {}),
                    "submitted_n": int(n),
                }

        if order_id is not None and not bool(multi_slice_override):
            set_state("alpaca_last_portfolio_orders_id", str(int(order_id)))

        return {
            "ok": True,
            "broker": "alpaca",
            "submitted_n": n,
            "parent_cursor_deferred": bool(multi_slice_override),
        }

    finally:
        con.close()


# ============================================================
# Poll Fills
# ============================================================

def poll_and_log_fills(after_ts_ms: int) -> Dict[str, Any]:
    n = 0
    orders = list_orders_after(after_ts_ms=int(after_ts_ms))
    for o in orders:
        try:
            cid = str(o.get("client_order_id") or "").strip()
            if not cid:
                continue

            filled_avg = o.get("filled_avg_price")
            if not filled_avg:
                continue
            result = apply_alpaca_trade_update(
                {"event": str(o.get("status") or "fill"), "order": dict(o or {})},
                source="poll",
                received_ts_ms=int(time.time() * 1000),
            )
            if str((result or {}).get("status") or "") == "fill_logged":
                n += 1
        except Exception as e:
            _warn_nonfatal(
                "ALPACA_FILL_LOG_FAILED",
                e,
                once_key=f"fill_log:{cid or 'unknown'}",
                client_order_id=str(cid or ""),
                order_id=str(o.get("id") or ""),
            )
            continue

    return {"ok": True, "fills_logged": n}


def run_trade_updates_stream_daemon(stop_event: Any = None) -> None:
    """Run the Alpaca ``trade_updates`` WebSocket with REST gap recovery."""
    if not TRADE_UPDATES_WS_ENABLED:
        LOG.info("alpaca_trade_updates_ws_disabled")
        return
    if websocket is None:
        raise RuntimeError(f"websocket-client unavailable: {_WEBSOCKET_IMPORT_ERROR}")
    key_id = _alpaca_key_id()
    secret = _alpaca_secret_key()
    if not key_id or not secret:
        log_failure(
            LOG,
            event="alpaca_trade_updates_ws_missing_credentials",
            code="ALPACA_TRADE_UPDATES_WS_MISSING_CREDENTIALS",
            message="Alpaca trade update stream skipped because credentials are missing.",
            level=logging.WARNING,
            component="engine.execution.broker_alpaca_rest",
            persist=False,
        )
        return

    backoff_s = max(0.1, float(TRADE_UPDATES_RECONNECT_BASE_S))
    last_event_ts_ms = int(time.time() * 1000) - int(TRADE_UPDATES_GAP_LOOKBACK_S * 1000)

    def _should_stop() -> bool:
        return bool(stop_event is not None and callable(getattr(stop_event, "is_set", None)) and stop_event.is_set())

    while not _should_stop():
        connected_at_ms = int(time.time() * 1000)
        last_message_ms = connected_at_ms

        def _on_open(ws) -> None:
            ws.send(json.dumps({"action": "auth", "key": key_id, "secret": secret}))
            ws.send(json.dumps({"action": "listen", "data": {"streams": ["trade_updates"]}}))
            emit_counter("alpaca_trade_updates_ws_connect", 1, component="engine.execution.broker_alpaca_rest", broker="alpaca")

        def _on_message(_ws, message) -> None:
            nonlocal last_event_ts_ms, last_message_ms
            received_ms = int(time.time() * 1000)
            last_message_ms = int(received_ms)
            for payload in _decode_ws_payload(message):
                stream = str(payload.get("stream") or "")
                if stream and stream != "trade_updates":
                    continue
                result = apply_alpaca_trade_update(payload, source="websocket", received_ts_ms=int(received_ms))
                if bool((result or {}).get("ok")):
                    last_event_ts_ms = max(last_event_ts_ms, int(received_ms))

        def _on_error(_ws, error) -> None:
            _warn_nonfatal(
                "ALPACA_TRADE_UPDATES_WS_ERROR",
                error if isinstance(error, BaseException) else RuntimeError(str(error)),
                once_key="trade_updates_ws_error",
            )

        def _on_close(_ws, _status_code, _msg) -> None:
            age_ms = max(0, int(time.time() * 1000) - int(last_message_ms))
            emit_gauge(
                "alpaca_trade_updates_ws_heartbeat_age_ms",
                int(age_ms),
                component="engine.execution.broker_alpaca_rest",
                broker="alpaca",
            )

        ws_app = websocket.WebSocketApp(
            _alpaca_stream_url(),
            on_open=_on_open,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
        )
        try:
            ws_app.run_forever(
                ping_interval=max(1, int(TRADE_UPDATES_PING_INTERVAL_S)),
                ping_timeout=max(1, int(TRADE_UPDATES_PING_TIMEOUT_S)),
            )
        finally:
            try:
                after_ms = max(
                    0,
                    int(min(last_event_ts_ms, connected_at_ms) - int(TRADE_UPDATES_GAP_LOOKBACK_S * 1000)),
                )
                poll_and_log_fills(after_ts_ms=int(after_ms))
            except Exception as e:
                _warn_nonfatal(
                    "ALPACA_TRADE_UPDATES_GAP_RECOVERY_FAILED",
                    e,
                    once_key="trade_updates_gap_recovery",
                    after_ts_ms=int(last_event_ts_ms),
                )

        if _should_stop():
            break
        sleep_s = min(float(TRADE_UPDATES_RECONNECT_MAX_S), float(backoff_s))
        if stop_event is not None and callable(getattr(stop_event, "wait", None)):
            stop_event.wait(timeout=float(sleep_s))
        else:
            time.sleep(float(sleep_s))
        backoff_s = min(float(TRADE_UPDATES_RECONNECT_MAX_S), max(0.1, float(backoff_s) * 2.0))
