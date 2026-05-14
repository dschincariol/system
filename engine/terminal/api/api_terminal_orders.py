"""Risk-gated terminal order-entry handlers for BUY, SELL, and FLATTEN intents.

The terminal never bypasses backend execution policy. These routes only write
portfolio-order intents after the same runtime execution barrier reports that
trading is currently allowed.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict

from engine.runtime.storage import connect, run_write_txn
from engine.runtime.gates import execution_gate_snapshot
from engine.runtime.state_cache import cache_invalidate_namespace
from engine.terminal.api.api_terminal import _table_exists


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

    # Terminal order entry is intentionally just an intent write behind the same
    # execution gate as the rest of the system. The route does not bypass policy.
    gate = execution_gate_snapshot()
    if not gate.get("real_trading_allowed", False):
        return {"ok": False, "error": "execution_blocked", "gate": gate}

    con = connect(readonly=True)
    try:
        if not _table_exists(con, "portfolio_orders"):
            return {"ok": False, "error": "portfolio_orders_missing"}
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

    return {"ok": True, "symbol": symbol, "side": side, "qty": qty}


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

    gate = execution_gate_snapshot()
    if not gate.get("real_trading_allowed", False):
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

    return {"ok": True, "symbol": symbol, "flatten_qty": abs(qty)}
