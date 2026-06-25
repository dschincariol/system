"""Risk-gated terminal order-entry handlers for BUY, SELL, and FLATTEN intents.

The terminal never bypasses backend execution policy. These routes only write
portfolio-order intents after the same runtime execution barrier reports that
trading is currently allowed.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any, Dict

from engine.execution.mode_safety import env_execution_mode_snapshot, resolve_effective_execution_mode
from engine.runtime.live_execution_control import (
    DISABLE_LIVE_EXECUTION_REASON,
    disabled_live_execution_gate,
    live_execution_disabled,
    prelive_reconcile_policy_gate,
)
from engine.runtime.storage import connect, run_write_txn
from engine.runtime.gates import execution_gate_snapshot
from engine.runtime.state_cache import cache_invalidate_namespace
from engine.terminal.api.api_terminal import _table_exists
from engine.terminal.api.price_reference import latest_terminal_price

try:
    from engine.cache.wrappers.execution_mode import read_execution_mode as _get_execution_mode
except Exception:
    _get_execution_mode = None  # type: ignore

try:
    from engine.cache.wrappers.kill_switch import read_kill_switch as _kill_switch_snapshot
except Exception:
    _kill_switch_snapshot = None  # type: ignore


ROUTE_SPECS_TERMINAL_ORDERS = [
    ("POST", "/api/terminal/order",   "api_post_terminal_order"),
    ("POST", "/api/terminal/flatten", "api_post_terminal_flatten"),
]


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    details = ", ".join(f"{k}={v}" for k, v in (extra or {}).items())
    suffix = f" ({details})" if details else ""
    sys.stderr.write(f"[engine.terminal.api.api_terminal_orders] {code}: {type(error).__name__}: {error}{suffix}\n")
    sys.stderr.flush()


def _json_body(handler) -> Dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length") or 0)
        raw = handler.rfile.read(length) if length > 0 else b"{}"
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_ORDERS_JSON_BODY_FAILED", e)
        return {}


def _request_body(parsed_or_handler: Any = None, body: Any = None) -> Dict[str, Any]:
    if isinstance(body, dict):
        return dict(body)
    if isinstance(parsed_or_handler, dict) and any(
        key in parsed_or_handler for key in ("symbol", "side", "qty")
    ):
        return dict(parsed_or_handler)
    if hasattr(parsed_or_handler, "headers") and hasattr(parsed_or_handler, "rfile"):
        return _json_body(parsed_or_handler)
    return {}


def _positive_qty(value: Any) -> float:
    try:
        qty = float(value or 0.0)
    except Exception:
        return 0.0
    return qty if qty > 0.0 else 0.0


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(os.environ.get(name, default))
    except Exception:
        value = float(default)
    return max(float(minimum), float(value))


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(float(os.environ.get(name, default)))
    except Exception:
        value = int(default)
    return max(int(minimum), int(value))


def _truthy_confirmation_value(value: Any) -> bool:
    if isinstance(value, bool):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "ack", "confirmed"}


def _effective_positive_limit(global_value: float, symbol_value: float | None = None) -> float:
    global_limit = max(0.0, float(global_value or 0.0))
    symbol_limit = max(0.0, float(symbol_value or 0.0))
    if symbol_limit > 0.0:
        return min(global_limit, symbol_limit) if global_limit > 0.0 else symbol_limit
    return global_limit


def _execution_gate_for_terminal_order() -> Dict[str, Any]:
    if _get_execution_mode is None or _kill_switch_snapshot is None:
        return {
            "ok": False,
            "reason": "execution_gate_providers_missing",
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "real_trading_allowed": False,
            "allowed": False,
        }
    return execution_gate_snapshot(
        get_execution_mode_fn=_get_execution_mode,
        kill_switches=(_kill_switch_snapshot() or {}),
    )


def _table_columns(con, table: str) -> set[str]:
    try:
        return {
            str(row[1])
            for row in (con.execute(f"PRAGMA table_info({table})").fetchall() or [])
            if row and len(row) > 1 and row[1]
        }
    except Exception:
        return set()


def _latest_price(con, symbol: str) -> Dict[str, Any]:
    return latest_terminal_price(
        con,
        symbol,
        table_exists_fn=_table_exists,
        warn_fn=_warn_nonfatal,
    )


def _symbol_caps(symbol: str) -> Dict[str, float]:
    caps: Dict[str, float] = {}
    raw = str(os.environ.get("TERMINAL_SYMBOL_CAPS_JSON") or "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            item = (parsed or {}).get(str(symbol or "").strip().upper())
            if isinstance(item, dict):
                if item.get("max_qty") is not None:
                    caps["max_qty"] = _positive_qty(item.get("max_qty"))
                if item.get("max_notional") is not None:
                    caps["max_notional"] = _positive_qty(item.get("max_notional"))
                if item.get("confirm_qty") is not None:
                    caps["confirm_qty"] = _positive_qty(item.get("confirm_qty"))
                if item.get("confirm_notional") is not None:
                    caps["confirm_notional"] = _positive_qty(item.get("confirm_notional"))
                if item.get("flatten_confirm_qty") is not None:
                    caps["flatten_confirm_qty"] = _positive_qty(item.get("flatten_confirm_qty"))
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_SYMBOL_CAPS_PARSE_FAILED", e)
    return caps


def _recent_duplicate_intent(con, symbol: str, side: str, qty: float, now_ms: int, *, action: str | None = None) -> bool:
    window_ms = _env_int("TERMINAL_DUPLICATE_WINDOW_MS", 5000, minimum=0)
    if window_ms <= 0 or not _table_exists(con, "portfolio_orders"):
        return False
    expected_actions = {str(side or "").strip().upper()}
    if action:
        expected_actions.add(str(action or "").strip().upper())
    try:
        rows = con.execute(
            """
            SELECT ts_ms, action, explain_json
              FROM portfolio_orders
             WHERE UPPER(symbol)=?
               AND ts_ms >= ?
             ORDER BY ts_ms DESC
             LIMIT 25
            """,
            (str(symbol or "").strip().upper(), int(now_ms) - int(window_ms)),
        ).fetchall() or []
        for row in rows:
            row_action = str(row[1] or "").strip().upper()
            if row_action not in expected_actions:
                continue
            try:
                explain = json.loads(str(row[2] or "{}"))
                terminal = explain.get("terminal_order") if isinstance(explain, dict) else {}
                existing_qty = float((terminal or {}).get("qty") or 0.0)
            except Exception:
                existing_qty = 0.0
            if abs(float(existing_qty) - float(qty)) <= 1e-9:
                return True
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_DUPLICATE_CHECK_FAILED", e, symbol=symbol, side=side)
    return False


def _ensure_terminal_rejection_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS terminal_intent_rejections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT,
          qty REAL,
          reason_code TEXT NOT NULL,
          reason TEXT NOT NULL,
          source TEXT NOT NULL,
          detail_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_terminal_intent_rejections_symbol_ts ON terminal_intent_rejections(symbol, ts_ms DESC)"
    )


def _record_terminal_rejection(
    *,
    symbol: str,
    side: str,
    qty: float,
    reason_code: str,
    reason: str,
    detail: Dict[str, Any] | None = None,
) -> None:
    try:
        ts_ms = int(time.time() * 1000)

        def _write(con):
            _ensure_terminal_rejection_schema(con)
            con.execute(
                """
                INSERT INTO terminal_intent_rejections
                (ts_ms, symbol, side, qty, reason_code, reason, source, detail_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts_ms,
                    str(symbol or "").strip().upper() or "UNKNOWN",
                    str(side or "").strip().upper(),
                    float(qty or 0.0),
                    str(reason_code or "rejected"),
                    str(reason or "Terminal request rejected."),
                    "terminal",
                    json.dumps(detail or {}, separators=(",", ":"), sort_keys=True),
                ),
            )

        run_write_txn(_write)
        cache_invalidate_namespace("portfolio_orders")
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_REJECTION_RECORD_FAILED", e, symbol=symbol, side=side, reason_code=reason_code)


def _threshold_confirmed(body: Dict[str, Any] | None, token: str) -> bool:
    if not isinstance(body, dict):
        return False
    actual = str(body.get("threshold_confirmation") or body.get("threshold_confirm") or "").strip()
    ack_value = body.get("threshold_consequence_ack")
    if ack_value is None:
        ack_value = body.get("threshold_ack")
    return actual == str(token or "").strip() and _truthy_confirmation_value(ack_value)


def _threshold_confirmation_required(
    *,
    symbol: str,
    side: str,
    qty: float,
    reason_code: str,
    reason: str,
    required_token: str,
    detail: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    detail_payload = dict(detail or {})
    detail_payload["required_confirm"] = str(required_token)
    _record_terminal_rejection(
        symbol=symbol,
        side=side,
        qty=qty,
        reason_code=reason_code,
        reason=reason,
        detail=detail_payload,
    )
    return _refusal_payload(
        error="confirmation_required",
        reason_code=reason_code,
        message=reason,
        http_status=422,
        required_confirm=str(required_token),
        required_confirmation=str(required_token),
        detail=detail_payload,
    )


def _refusal_payload(
    *,
    error: str,
    reason_code: str,
    message: str,
    http_status: int,
    **fields: Any,
) -> Dict[str, Any]:
    meta = fields.pop("meta", {})
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("status", int(http_status))
    meta.setdefault("reason_code", str(reason_code))
    payload: Dict[str, Any] = {
        "ok": False,
        "error": str(error),
        "reason_code": str(reason_code),
        "message": str(message),
        "reason": str(message),
        "http_status": int(http_status),
        "meta": meta,
    }
    payload.update(fields)
    return payload


def _rejected(symbol: str, side: str, qty: float, reason_code: str, reason: str, detail: Dict[str, Any] | None = None) -> Dict[str, Any]:
    _record_terminal_rejection(
        symbol=symbol,
        side=side,
        qty=qty,
        reason_code=reason_code,
        reason=reason,
        detail=detail,
    )
    return _refusal_payload(
        error="pre_trade_rejected",
        reason_code=str(reason_code),
        message=str(reason),
        http_status=409,
        detail=dict(detail or {}),
    )


def _pre_trade_controls(
    con,
    *,
    symbol: str,
    side: str,
    qty: float,
    body: Dict[str, Any] | None = None,
    intent: str = "order",
    flatten_position_qty: float | None = None,
) -> Dict[str, Any]:
    max_qty = _env_float("TERMINAL_MAX_QTY", 10_000.0, minimum=0.0)
    max_notional = _env_float("TERMINAL_MAX_NOTIONAL", 1_000_000.0, minimum=0.0)
    price_max_age_ms = _env_int("TERMINAL_PRICE_MAX_AGE_MS", 60_000, minimum=0)
    caps = _symbol_caps(symbol)
    max_qty = _effective_positive_limit(max_qty, caps.get("max_qty"))
    max_notional = _effective_positive_limit(max_notional, caps.get("max_notional"))
    confirm_qty = _effective_positive_limit(_env_float("TERMINAL_CONFIRM_QTY", 0.0, minimum=0.0), caps.get("confirm_qty"))
    confirm_notional = _effective_positive_limit(
        _env_float("TERMINAL_CONFIRM_NOTIONAL", 0.0, minimum=0.0),
        caps.get("confirm_notional"),
    )
    flatten_confirm_qty = _effective_positive_limit(
        _env_float("TERMINAL_FLATTEN_CONFIRM_POSITION_QTY", 0.0, minimum=0.0),
        caps.get("flatten_confirm_qty"),
    )
    if max_qty > 0 and qty > max_qty:
        return _rejected(symbol, side, qty, "max_qty_exceeded", f"Quantity {qty:g} exceeds the configured limit {max_qty:g}.", {"max_qty": max_qty})
    price = _latest_price(con, symbol)
    if not bool(price.get("ok")):
        return _rejected(symbol, side, qty, "missing_price", "No fresh price is available for this symbol.", price)
    age_ms = price.get("age_ms")
    if price_max_age_ms > 0 and (age_ms is None or float(age_ms) > price_max_age_ms):
        return _rejected(symbol, side, qty, "stale_price", "The latest price is stale; refresh market data before ordering.", {**price, "max_age_ms": price_max_age_ms})
    notional = abs(float(qty) * float(price.get("price") or 0.0))
    if max_notional > 0 and notional > max_notional:
        return _rejected(symbol, side, qty, "max_notional_exceeded", f"Estimated notional ${notional:,.2f} exceeds the configured limit ${max_notional:,.2f}.", {"notional": notional, "max_notional": max_notional, **price})
    if confirm_notional > 0 and notional > confirm_notional and not _threshold_confirmed(body, "HIGH_NOTIONAL"):
        return _threshold_confirmation_required(
            symbol=symbol,
            side=side,
            qty=qty,
            reason_code="threshold_notional_confirmation_required",
            reason=f"Estimated notional ${notional:,.2f} requires typed threshold confirmation.",
            required_token="HIGH_NOTIONAL",
            detail={"notional": notional, "confirm_notional": confirm_notional, **price},
        )
    if confirm_qty > 0 and qty > confirm_qty and not _threshold_confirmed(body, "HIGH_QTY"):
        return _threshold_confirmation_required(
            symbol=symbol,
            side=side,
            qty=qty,
            reason_code="threshold_quantity_confirmation_required",
            reason=f"Quantity {qty:g} requires typed threshold confirmation.",
            required_token="HIGH_QTY",
            detail={"qty": qty, "confirm_qty": confirm_qty, **price},
        )
    position_qty = abs(float(flatten_position_qty if flatten_position_qty is not None else qty))
    if (
        str(intent or "").strip().lower() == "flatten"
        and flatten_confirm_qty > 0
        and position_qty > flatten_confirm_qty
        and not _threshold_confirmed(body, "POSITION_LIMIT")
    ):
        return _threshold_confirmation_required(
            symbol=symbol,
            side=side,
            qty=qty,
            reason_code="threshold_position_confirmation_required",
            reason="Flatten size requires typed position-limit confirmation.",
            required_token="POSITION_LIMIT",
            detail={"position_qty": position_qty, "confirm_position_qty": flatten_confirm_qty, **price},
        )
    now_ms = int(time.time() * 1000)
    dedupe_action = "FLATTEN" if str(intent or "").strip().lower() == "flatten" else None
    if _recent_duplicate_intent(con, symbol, side, qty, now_ms, action=dedupe_action):
        return _rejected(symbol, side, qty, "duplicate_recent_order", "A matching terminal intent was recorded moments ago.", {"duplicate_window_ms": _env_int("TERMINAL_DUPLICATE_WINDOW_MS", 5000, minimum=0)})
    return {"ok": True, "price": price, "estimated_notional": notional, "max_qty": max_qty, "max_notional": max_notional}


def _terminal_source_alert_id(body: Dict[str, Any], ts_ms: int) -> int:
    try:
        candidate = int(body.get("source_alert_id") or 0)
    except Exception:
        candidate = 0
    return int(candidate if candidate > 0 else int(ts_ms))


def _terminal_explain(
    symbol: str,
    side: str,
    qty: float,
    *,
    flatten: bool = False,
    source_alert_id: int | None = None,
) -> str:
    signed_qty = float(qty) if str(side).upper() == "BUY" else -float(qty)
    payload = {
        "source": "terminal",
        "terminal_order": {
            "sizing": "quantity",
            "symbol": str(symbol),
            "side": str(side).upper(),
            "qty": float(qty),
            "signed_qty": float(signed_qty),
            "flatten": bool(flatten),
        },
    }
    if source_alert_id is not None:
        payload["terminal_order"]["source_alert_id"] = int(source_alert_id)
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _terminal_order_target_side(side: str) -> str:
    return "LONG" if str(side or "").strip().upper() == "BUY" else "SHORT"


def _position_side(qty: float) -> str:
    if float(qty or 0.0) > 0.0:
        return "LONG"
    if float(qty or 0.0) < 0.0:
        return "SHORT"
    return "FLAT"


def _effective_execution_mode_for_terminal_order() -> Dict[str, Any]:
    mode_state: Any = None
    if callable(_get_execution_mode):
        try:
            mode_state = _get_execution_mode()
        except Exception as e:
            _warn_nonfatal(
                "API_TERMINAL_ORDERS_EXECUTION_MODE_LOAD_FAILED",
                e,
            )
    resolved = resolve_effective_execution_mode(mode_state, environ=os.environ)
    if isinstance(resolved, dict):
        return dict(resolved)
    return {
        "ok": False,
        "mode": "safe",
        "armed": None,
        "reason": "execution_mode_resolution_invalid",
        "source": "terminal_order:invalid",
    }


def _disabled_live_execution_response(*, allow_paper_simulation: bool = False) -> Dict[str, Any] | None:
    if not live_execution_disabled():
        return None
    resolved_mode = _effective_execution_mode_for_terminal_order()
    env_mode = env_execution_mode_snapshot(os.environ)
    env_inputs = dict(env_mode.get("inputs") or {})
    env_mentions_live = any(
        str((item or {}).get("mode") or "").strip().lower() == "live"
        for item in env_inputs.values()
        if isinstance(item, dict)
    )
    env_is_explicit_paper = (
        bool(env_mode.get("explicit"))
        and str(env_mode.get("mode") or "").strip().lower() == "paper"
        and not env_mentions_live
        and not env_mode.get("invalid")
    )
    if allow_paper_simulation and str(resolved_mode.get("mode") or "").strip().lower() == "paper" and env_is_explicit_paper:
        gate = _execution_gate_for_terminal_order()
        if (
            str(gate.get("mode") or "").strip().lower() == "paper"
            and _terminal_intent_allowed(gate)
            and not bool(gate.get("real_trading_allowed", False))
        ):
            return None
    gate = disabled_live_execution_gate(
        source="engine.terminal.api.api_terminal_orders",
        mode=str(resolved_mode.get("mode") or "safe"),
        armed=resolved_mode.get("armed"),
        extra={
            "applied_policy": DISABLE_LIVE_EXECUTION_REASON,
            "execution_mode_resolution": resolved_mode,
        },
    )
    reason_code = str(gate.get("reason") or gate.get("status") or "live_execution_disabled")
    return {
        **_refusal_payload(
            error="execution_blocked",
            reason_code=reason_code,
            message="Terminal order entry is blocked by the live-execution safety gate.",
            http_status=403,
        ),
        "gate": gate,
    }


def _prelive_reconcile_policy_response() -> Dict[str, Any] | None:
    policy_block = prelive_reconcile_policy_gate(
        source="engine.terminal.api.api_terminal_orders",
        broker="terminal",
        audit_override=True,
    )
    if policy_block is None:
        return None
    reason_code = str(policy_block.get("status") or policy_block.get("reason") or "prelive_reconcile_blocked")
    return {
        **_refusal_payload(
            error="execution_blocked",
            reason_code=reason_code,
            message="Terminal order entry is blocked until pre-live reconciliation is enabled and passing.",
            http_status=403,
        ),
        "gate": policy_block,
    }


def _terminal_intent_allowed(gate: Dict[str, Any]) -> bool:
    if bool(gate.get("real_trading_allowed", False)):
        return True
    mode = str(gate.get("mode") or "").strip().lower()
    return bool(
        mode == "paper"
        and gate.get("allow_execution_pipeline", False)
        and gate.get("allow_simulation", False)
    )


def api_post_terminal_order(_parsed=None, body=None, _ctx=None):
    """Validate and record a terminal order intent.

    Parameters
    ----------
    _parsed : urllib.parse.ParseResult | BaseHTTPRequestHandler | dict, optional
        Parsed request path from the shared dispatcher, or a legacy direct HTTP
        handler in older tests/callers.
    body : dict, optional
        JSON body already consumed by the shared HTTP dispatcher.
    _ctx : Any, optional
        Unused route context accepted for signature consistency.

    Returns
    -------
    dict[str, Any]
        Success payload containing the normalized symbol, side, and quantity,
        or an error payload when the request is invalid, the execution gate is
        closed, or the required storage table is unavailable.

    Notes
    -----
    The handler never submits directly to a broker. It writes a
    `portfolio_orders` intent only after `execution_gate_snapshot()` reports
    that real trading is currently allowed.
    """

    body = _request_body(_parsed, body)

    symbol = str(body.get("symbol") or "").strip().upper()
    side = str(body.get("side") or "").strip().upper()
    qty = _positive_qty(body.get("qty"))

    if not symbol or qty <= 0 or side not in ("BUY", "SELL"):
        return _refusal_payload(
            error="invalid_order",
            reason_code="invalid_terminal_order",
            message="Terminal order requires a symbol, BUY or SELL side, and a positive quantity.",
            http_status=400,
        )

    disabled = _disabled_live_execution_response(allow_paper_simulation=True)
    if disabled is not None:
        return disabled
    prelive_policy = _prelive_reconcile_policy_response()
    if prelive_policy is not None:
        return prelive_policy

    # Terminal order entry is intentionally just an intent write behind the same
    # execution gate as the rest of the system. The route does not bypass policy.
    gate = _execution_gate_for_terminal_order()
    if not _terminal_intent_allowed(gate):
        reason_code = str(gate.get("reason") or gate.get("status") or "execution_gate_blocked")
        return {
            **_refusal_payload(
                error="execution_blocked",
                reason_code=reason_code,
                message="Terminal order entry is blocked by the execution gate.",
                http_status=403,
            ),
            "gate": gate,
        }

    con = connect(readonly=True)
    try:
        if not _table_exists(con, "portfolio_orders"):
            return _refusal_payload(
                error="portfolio_orders_missing",
                reason_code="portfolio_orders_missing",
                message="Terminal order entry cannot record intents because portfolio_orders is unavailable.",
                http_status=503,
            )
        pre_trade = _pre_trade_controls(con, symbol=symbol, side=side, qty=qty, body=body)
        if not bool(pre_trade.get("ok")):
            return pre_trade
    finally:
        con.close()

    ts = int(time.time() * 1000)
    source_alert_id = _terminal_source_alert_id(body, ts)

    try:
        def _write(con):
            # The terminal writes into `portfolio_orders`, letting the existing
            # execution pipeline pick up and route the request consistently.
            con.execute(
                """
                INSERT INTO portfolio_orders (ts_ms, model_id, symbol, action, from_side, to_side,
                                              from_weight, to_weight, delta_weight, source_alert_id, explain_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    "baseline",
                    symbol,
                    side,
                    "FLAT",
                    _terminal_order_target_side(side),
                    0.0,
                    0.0,
                    0.0,
                    int(source_alert_id),
                    _terminal_explain(symbol, side, qty, source_alert_id=source_alert_id),
                ),
            )

        run_write_txn(_write)
        cache_invalidate_namespace("portfolio_orders")
        cache_invalidate_namespace("portfolio_snapshot")
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_ORDER_WRITE_FAILED", e, symbol=str(symbol), side=str(side), qty=qty)
        return _refusal_payload(
            error="internal_server_error",
            reason_code="terminal_order_write_failed",
            message="Terminal order intent could not be recorded.",
            http_status=500,
            detail=type(e).__name__,
        )

    return {
        "ok": True,
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "estimated_notional": pre_trade.get("estimated_notional"),
        "price": pre_trade.get("price"),
    }


def api_post_terminal_flatten(_parsed=None, body=None, _ctx=None):
    """Validate and record a terminal flatten intent for one symbol.

    Parameters
    ----------
    _parsed : urllib.parse.ParseResult | BaseHTTPRequestHandler | dict, optional
        Parsed request path from the shared dispatcher, or a legacy direct HTTP
        handler in older tests/callers.
    body : dict, optional
        JSON body already consumed by the shared HTTP dispatcher.
    _ctx : Any, optional
        Unused route context accepted for signature consistency.

    Returns
    -------
    dict[str, Any]
        Success payload containing the flatten quantity, a benign no-position
        message, or an error payload when the request is invalid, the execution
        gate is closed, or required storage tables are unavailable.

    Notes
    -----
    Flatten requests are translated into `portfolio_orders` intents so the
    normal execution pipeline, audit trail, and downstream routing remain the
    source of truth.
    """

    body = _request_body(_parsed, body)
    symbol = str(body.get("symbol") or "").strip().upper()
    if not symbol:
        return _refusal_payload(
            error="missing_symbol",
            reason_code="missing_symbol",
            message="Terminal flatten requires a symbol.",
            http_status=400,
        )

    disabled = _disabled_live_execution_response()
    if disabled is not None:
        return disabled
    prelive_policy = _prelive_reconcile_policy_response()
    if prelive_policy is not None:
        return prelive_policy

    gate = _execution_gate_for_terminal_order()
    if not _terminal_intent_allowed(gate):
        reason_code = str(gate.get("reason") or gate.get("status") or "execution_gate_blocked")
        return {
            **_refusal_payload(
                error="execution_blocked",
                reason_code=reason_code,
                message="Terminal flatten is blocked by the execution gate.",
                http_status=403,
            ),
            "gate": gate,
        }

    con = connect(readonly=True)
    try:
        if not _table_exists(con, "broker_positions"):
            return _refusal_payload(
                error="broker_positions_missing",
                reason_code="broker_positions_missing",
                message="Terminal flatten cannot inspect broker positions because broker_positions is unavailable.",
                http_status=503,
            )
        if not _table_exists(con, "portfolio_orders"):
            return _refusal_payload(
                error="portfolio_orders_missing",
                reason_code="portfolio_orders_missing",
                message="Terminal flatten cannot record intents because portfolio_orders is unavailable.",
                http_status=503,
            )

        row = con.execute(
            "SELECT qty FROM broker_positions WHERE symbol=? LIMIT 1",
            (symbol,),
        ).fetchone()
    finally:
        con.close()

    if not row:
        return {"ok": True, "message": "no_position"}

    qty = float(row[0] or 0.0)
    if qty == 0:
        return {"ok": True, "message": "already_flat"}

    side = "SELL" if qty > 0 else "BUY"
    con = connect(readonly=True)
    try:
        pre_trade = _pre_trade_controls(
            con,
            symbol=symbol,
            side=side,
            qty=abs(qty),
            body=body,
            intent="flatten",
            flatten_position_qty=qty,
        )
        if not bool(pre_trade.get("ok")):
            return pre_trade
    finally:
        con.close()
    ts = int(time.time() * 1000)
    source_alert_id = _terminal_source_alert_id(body, ts)

    try:
        def _write(con):
            # Flatten is encoded as another portfolio-order intent instead of
            # directly mutating positions, preserving the normal audit trail.
            con.execute(
                """
                INSERT INTO portfolio_orders (ts_ms, model_id, symbol, action, from_side, to_side,
                                              from_weight, to_weight, delta_weight, source_alert_id, explain_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    "baseline",
                    symbol,
                    "FLATTEN",
                    _position_side(qty),
                    "FLAT",
                    0.0,
                    0.0,
                    0.0,
                    int(source_alert_id),
                    _terminal_explain(symbol, side, abs(qty), flatten=True, source_alert_id=source_alert_id),
                ),
            )

        run_write_txn(_write)
        cache_invalidate_namespace("portfolio_orders")
        cache_invalidate_namespace("portfolio_snapshot")
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_FLATTEN_WRITE_FAILED", e, symbol=str(symbol), flatten_qty=abs(qty))
        return _refusal_payload(
            error="internal_server_error",
            reason_code="terminal_flatten_write_failed",
            message="Terminal flatten intent could not be recorded.",
            http_status=500,
            detail=type(e).__name__,
        )

    return {
        "ok": True,
        "symbol": symbol,
        "flatten_qty": abs(qty),
        "estimated_notional": pre_trade.get("estimated_notional"),
        "price": pre_trade.get("price"),
    }
