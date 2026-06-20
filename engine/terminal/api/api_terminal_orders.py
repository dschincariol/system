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

from engine.runtime.live_execution_control import (
    disabled_live_execution_gate,
    live_execution_disabled,
    prelive_reconcile_policy_gate,
)
from engine.runtime.storage import connect, run_write_txn
from engine.runtime.gates import execution_gate_snapshot
from engine.runtime.state_cache import cache_invalidate_namespace
from engine.terminal.api.api_terminal import _table_exists

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
    symbol = str(symbol or "").strip().upper()
    now_ms = int(time.time() * 1000)
    if _table_exists(con, "prices"):
        cols = _table_columns(con, "prices")
        price_col = "price" if "price" in cols else ("close" if "close" in cols else "")
        ts_col = "ts_ms" if "ts_ms" in cols else ("timestamp_ms" if "timestamp_ms" in cols else "")
        if price_col and ts_col and "symbol" in cols:
            try:
                row = con.execute(
                    f"""
                    SELECT {price_col}, {ts_col}
                      FROM prices
                     WHERE UPPER(symbol)=?
                     ORDER BY {ts_col} DESC
                     LIMIT 1
                    """,
                    (symbol,),
                ).fetchone()
                if row and row[0] is not None:
                    ts_ms = int(row[1] or 0)
                    return {
                        "ok": True,
                        "price": float(row[0]),
                        "ts_ms": ts_ms,
                        "age_ms": max(0, now_ms - ts_ms) if ts_ms > 0 else None,
                        "source": "prices",
                    }
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_PRICE_READ_FAILED", e, symbol=symbol)
    if _table_exists(con, "broker_positions"):
        cols = _table_columns(con, "broker_positions")
        price_col = "market_px" if "market_px" in cols else ("avg_px" if "avg_px" in cols else "")
        ts_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "")
        if price_col and ts_col and "symbol" in cols:
            try:
                row = con.execute(
                    f"""
                    SELECT {price_col}, {ts_col}
                      FROM broker_positions
                     WHERE UPPER(symbol)=?
                     ORDER BY {ts_col} DESC
                     LIMIT 1
                    """,
                    (symbol,),
                ).fetchone()
                if row and row[0] is not None:
                    ts_ms = int(row[1] or 0)
                    return {
                        "ok": True,
                        "price": float(row[0]),
                        "ts_ms": ts_ms,
                        "age_ms": max(0, now_ms - ts_ms) if ts_ms > 0 else None,
                        "source": "broker_positions",
                    }
            except Exception as e:
                _warn_nonfatal("API_TERMINAL_POSITION_PRICE_READ_FAILED", e, symbol=symbol)
    return {"ok": False, "error": "missing_price", "source": "none"}


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
        except Exception as e:
            _warn_nonfatal("API_TERMINAL_SYMBOL_CAPS_PARSE_FAILED", e)
    return caps


def _recent_duplicate_intent(con, symbol: str, side: str, qty: float, now_ms: int) -> bool:
    window_ms = _env_int("TERMINAL_DUPLICATE_WINDOW_MS", 5000, minimum=0)
    if window_ms <= 0 or not _table_exists(con, "portfolio_orders"):
        return False
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
            action = str(row[1] or "").strip().upper()
            if action != str(side or "").strip().upper():
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


def _rejected(symbol: str, side: str, qty: float, reason_code: str, reason: str, detail: Dict[str, Any] | None = None) -> Dict[str, Any]:
    _record_terminal_rejection(
        symbol=symbol,
        side=side,
        qty=qty,
        reason_code=reason_code,
        reason=reason,
        detail=detail,
    )
    return {
        "ok": False,
        "error": "pre_trade_rejected",
        "reason_code": str(reason_code),
        "reason": str(reason),
        "detail": dict(detail or {}),
    }


def _pre_trade_controls(con, *, symbol: str, side: str, qty: float) -> Dict[str, Any]:
    max_qty = _env_float("TERMINAL_MAX_QTY", 10_000.0, minimum=0.0)
    max_notional = _env_float("TERMINAL_MAX_NOTIONAL", 1_000_000.0, minimum=0.0)
    price_max_age_ms = _env_int("TERMINAL_PRICE_MAX_AGE_MS", 60_000, minimum=0)
    caps = _symbol_caps(symbol)
    if caps.get("max_qty", 0.0) > 0:
        max_qty = min(max_qty, float(caps["max_qty"])) if max_qty > 0 else float(caps["max_qty"])
    if caps.get("max_notional", 0.0) > 0:
        max_notional = min(max_notional, float(caps["max_notional"])) if max_notional > 0 else float(caps["max_notional"])
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
    now_ms = int(time.time() * 1000)
    if _recent_duplicate_intent(con, symbol, side, qty, now_ms):
        return _rejected(symbol, side, qty, "duplicate_recent_order", "A matching terminal intent was recorded moments ago.", {"duplicate_window_ms": _env_int("TERMINAL_DUPLICATE_WINDOW_MS", 5000, minimum=0)})
    return {"ok": True, "price": price, "estimated_notional": notional, "max_qty": max_qty, "max_notional": max_notional}


def _terminal_explain(symbol: str, side: str, qty: float, *, flatten: bool = False) -> str:
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
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _disabled_live_execution_response() -> Dict[str, Any] | None:
    if not live_execution_disabled():
        return None
    return {
        "ok": False,
        "error": "execution_blocked",
        "gate": disabled_live_execution_gate(source="engine.terminal.api.api_terminal_orders"),
    }


def _prelive_reconcile_policy_response() -> Dict[str, Any] | None:
    policy_block = prelive_reconcile_policy_gate(
        source="engine.terminal.api.api_terminal_orders",
        broker="terminal",
        audit_override=True,
    )
    if policy_block is None:
        return None
    return {"ok": False, "error": "execution_blocked", "gate": policy_block}


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
        return {"ok": False, "error": "invalid_order"}

    disabled = _disabled_live_execution_response()
    if disabled is not None:
        return disabled
    prelive_policy = _prelive_reconcile_policy_response()
    if prelive_policy is not None:
        return prelive_policy

    # Terminal order entry is intentionally just an intent write behind the same
    # execution gate as the rest of the system. The route does not bypass policy.
    gate = _execution_gate_for_terminal_order()
    if not _terminal_intent_allowed(gate):
        return {"ok": False, "error": "execution_blocked", "gate": gate}

    con = connect(readonly=True)
    try:
        if not _table_exists(con, "portfolio_orders"):
            return {"ok": False, "error": "portfolio_orders_missing"}
        pre_trade = _pre_trade_controls(con, symbol=symbol, side=side, qty=qty)
        if not bool(pre_trade.get("ok")):
            return pre_trade
    finally:
        con.close()

    ts = int(time.time() * 1000)

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
                    None,
                    side,
                    0.0,
                    0.0,
                    0.0,
                    None,
                    _terminal_explain(symbol, side, qty),
                ),
            )

        run_write_txn(_write)
        cache_invalidate_namespace("portfolio_orders")
        cache_invalidate_namespace("portfolio_snapshot")
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_ORDER_WRITE_FAILED", e, symbol=str(symbol), side=str(side), qty=qty)
        return {"ok": False, "error": str(e)}

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
        return {"ok": False, "error": "missing_symbol"}

    disabled = _disabled_live_execution_response()
    if disabled is not None:
        return disabled
    prelive_policy = _prelive_reconcile_policy_response()
    if prelive_policy is not None:
        return prelive_policy

    gate = _execution_gate_for_terminal_order()
    if not _terminal_intent_allowed(gate):
        return {"ok": False, "error": "execution_blocked", "gate": gate}

    con = connect(readonly=True)
    try:
        if not _table_exists(con, "broker_positions"):
            return {"ok": False, "error": "broker_positions_missing"}
        if not _table_exists(con, "portfolio_orders"):
            return {"ok": False, "error": "portfolio_orders_missing"}

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
        pre_trade = _pre_trade_controls(con, symbol=symbol, side=side, qty=abs(qty))
        if not bool(pre_trade.get("ok")):
            return pre_trade
    finally:
        con.close()
    ts = int(time.time() * 1000)

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
                    None,
                    side,
                    0.0,
                    0.0,
                    0.0,
                    None,
                    _terminal_explain(symbol, side, abs(qty), flatten=True),
                ),
            )

        run_write_txn(_write)
        cache_invalidate_namespace("portfolio_orders")
        cache_invalidate_namespace("portfolio_snapshot")
    except Exception as e:
        _warn_nonfatal("API_TERMINAL_FLATTEN_WRITE_FAILED", e, symbol=str(symbol), flatten_qty=abs(qty))
        return {"ok": False, "error": str(e)}

    return {
        "ok": True,
        "symbol": symbol,
        "flatten_qty": abs(qty),
        "estimated_notional": pre_trade.get("estimated_notional"),
        "price": pre_trade.get("price"),
    }
