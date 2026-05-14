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
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from engine.execution.broker_fill_utils import parse_broker_timestamp_ms
from engine.execution.execution_ledger import log_submit, log_fill
from engine.strategy.alpha_lifecycle_engine import apply_alpha_lifecycle
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.execution.kill_switch import execution_allowed
from engine.runtime.risk_state import get_state, set_state
from engine.execution.deployable_capital import compute_deployable_equity
from engine.execution.order_idempotency import (
    claim_order_submission,
    mark_order_submission_submitted,
    mark_order_submission_unknown,
)
from engine.cache.wrappers.kill_switch import read_kill_switch as kill_switch_snapshot
from engine.cache.wrappers.execution_mode import read_execution_mode as get_execution_mode

execution_gate_snapshot = None


BASE_URL = os.environ.get("ALPACA_BASE_URL", "https://paper-api.alpaca.markets").strip()
KEY_ID = os.environ.get("ALPACA_KEY_ID", "").strip()
SECRET = os.environ.get("ALPACA_SECRET_KEY", "").strip()

ORDER_TIF = os.environ.get("ALPACA_ORDER_TIF", "day").strip()
ORDER_TYPE = os.environ.get("ALPACA_ORDER_TYPE", "market").strip()
MAX_ORDERS_PER_PASS = int(os.environ.get("ALPACA_MAX_ORDERS_PER_PASS", "25"))
SLEEP_BETWEEN_ORDERS_S = float(os.environ.get("ALPACA_SLEEP_BETWEEN_ORDERS_S", "0.25"))

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


# ============================================================
# HTTP Helpers
# ============================================================

def _headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": KEY_ID,
        "APCA-API-SECRET-KEY": SECRET,
        "Content-Type": "application/json",
    }


def _req(method: str, path: str, payload: Optional[dict] = None) -> Any:
    # Transport errors are allowed to raise here; callers decide whether a
    # given Alpaca failure is retryable, degradable, or execution-blocking.
    if not KEY_ID or not SECRET:
        raise RuntimeError("alpaca credentials missing")
    url = BASE_URL.rstrip("/") + path
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    r = urllib.request.Request(url, data=data, headers=_headers(), method=method.upper())
    with urllib.request.urlopen(r, timeout=20) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _real_trading_gate() -> Dict[str, Any]:
    if execution_gate_snapshot is None:
        return {
            "ok": False,
            "reason": "execution_gate_provider_missing",
            "real_trading_allowed": False,
            "allowed": False,
        }
    return execution_gate_snapshot(
        get_execution_mode_fn=get_execution_mode,
        kill_switches=(kill_switch_snapshot() or {}),
    )


# ============================================================
# Account / Positions
# ============================================================

def get_account() -> Dict[str, Any]:
    return _req("GET", "/v2/account")


def get_positions() -> List[Dict[str, Any]]:
    res = _req("GET", "/v2/positions")
    return list(res or [])


def get_order(order_id: str) -> Dict[str, Any]:
    return _req("GET", f"/v2/orders/{str(order_id)}")


def cancel_order(order_id: str) -> Dict[str, Any]:
    return _req("DELETE", f"/v2/orders/{str(order_id)}")


def list_orders(status: str = "all", limit: int = 500, after_ts_ms: Optional[int] = None) -> List[Dict[str, Any]]:
    parts = [f"status={status}", "direction=asc", f"limit={int(limit)}"]
    if after_ts_ms is not None:
        dt = datetime.fromtimestamp(float(after_ts_ms) / 1000.0, tz=timezone.utc)
        after = dt.isoformat().replace("+00:00", "Z")
        parts.append(f"after={after}")
    path = "/v2/orders?" + "&".join(parts)
    res = _req("GET", path)
    return list(res or [])


def list_open_orders(limit: int = 500) -> List[Dict[str, Any]]:
    return list_orders(status="open", limit=int(limit))


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

def _submit_market_order(symbol: str, qty: float, client_oid: str) -> Dict[str, Any]:
    side = "buy" if qty > 0 else "sell"
    payload = {
        "symbol": symbol,
        "qty": str(abs(qty)),
        "side": side,
        "type": "market",
        "time_in_force": ORDER_TIF,
        "client_order_id": client_oid,
    }
    return _req("POST", "/v2/orders", payload)


def _submit_limit_order(symbol: str, qty: float, limit_price: float, client_oid: str) -> Dict[str, Any]:
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
    return _req("POST", "/v2/orders", payload)


def submit_limit_order(symbol: str, qty: float, limit_price: float, client_oid: str) -> Dict[str, Any]:
    gate = _real_trading_gate()
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "alpaca"}
    return _submit_limit_order(symbol, qty, limit_price, client_oid)


def submit_market_order(symbol: str, qty: float, client_oid: str) -> Dict[str, Any]:
    gate = _real_trading_gate()
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "alpaca"}
    return _submit_market_order(symbol, qty, client_oid)


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

    if not KEY_ID or not SECRET:
        return {"ok": False, "status": "missing_credentials"}

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

        # idempotency
        if order_id is not None:
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

        acct = get_account()
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

            guard = claim_order_submission(
                con=con,
                broker="alpaca",
                portfolio_orders_id=order_id,
                portfolio_ts_ms=int(ts_ms),
                order=o,
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
            except Exception as e:
                try:
                    mark_order_submission_unknown(
                        con=con,
                        order_uid=order_uid,
                        last_error=str(e),
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

            try:
                broker_order_id = str((res or {}).get("id") or "")
                source_alert_id = (
                    _safe_int(o.get("source_alert_id"))
                    if isinstance(o, dict) and o.get("source_alert_id") is not None
                    else None
                )
                log_submit(
                    client_order_id=client_oid,
                    broker="alpaca",
                    symbol=symbol,
                    qty=delta,
                    submit_ts_ms=int(time.time() * 1000),
                    ref_px=float(px),
                    broker_order_id=broker_order_id,
                    portfolio_orders_id=order_id,
                    source_alert_id=source_alert_id,
                    extra={**dict(order_meta or {}), "order_uid": str(order_uid), "idempotency_status": "submitted"},
                    order_uid=str(order_uid),
                    idempotency_status="submitted",
                )
                mark_order_submission_submitted(
                    con=con,
                    order_uid=str(order_uid),
                    client_order_id=str(client_oid),
                    broker_order_id=broker_order_id,
                    submit_ts_ms=int(time.time() * 1000),
                )

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
                    "BROKER_ALPACA_LOG_SUBMIT_FAILED",
                    e,
                    once_key="alpaca_log_submit",
                    symbol=str(symbol),
                    client_order_id=str(client_oid),
                    order_uid=str(order_uid),
                )

            pos[symbol] = float(cur_qty) + float(delta)
            submitted.append({"symbol": symbol, "delta_qty": delta})
            n += 1
            time.sleep(max(0.0, SLEEP_BETWEEN_ORDERS_S))

        if order_id is not None:
            set_state("alpaca_last_portfolio_orders_id", str(int(order_id)))

        return {"ok": True, "broker": "alpaca", "submitted_n": n}

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

            filled_qty = float(o.get("filled_qty") or 0.0)
            filled_avg = o.get("filled_avg_price")
            if not filled_avg:
                continue

            fill_ts_ms = parse_broker_timestamp_ms(
                o.get("updated_at") or o.get("filled_at") or o.get("created_at"),
                default_ms=int(time.time() * 1000),
            )
            log_fill(
                client_order_id=cid,
                fill_id=str(o.get("id") or o.get("client_order_id") or ""),
                broker="alpaca",
                symbol=str(o.get("symbol") or ""),
                qty=filled_qty,
                fill_px=float(filled_avg),
                fill_ts_ms=fill_ts_ms,
                fees=None,
                extra={
                    **dict(o or {}),
                    "liquidity": str(o.get("order_class") or ""),
                },
            )
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
