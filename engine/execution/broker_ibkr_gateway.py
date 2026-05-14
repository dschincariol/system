"""
FILE: broker_ibkr_gateway.py

Execution subsystem module for `broker_ibkr_gateway`.
"""

"""
IBKR (Interactive Brokers) IB Gateway / TWS adapter using ibapi.

Institutional version:
- True delta reconciliation execution
- ALE integration
- Execution risk compatible
- Execution analytics compatible
- Position reconciliation support
- Live execution stream daemon

Supports:
- apply_latest_portfolio_orders_live()
- poll_and_log_fills()
- get_positions_snapshot()
- get_positions_live()
- run_execution_stream_daemon()

Env:
  IBKR_HOST
  IBKR_PORT
  IBKR_CLIENT_ID
  IBKR_ORDER_TIF
  IBKR_MAX_ORDERS_PER_PASS
  IBKR_SLEEP_BETWEEN_ORDERS_S
  IBKR_EQUITY_USD
"""

import os
import time
import json
import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

from engine.execution.broker_fill_utils import parse_broker_timestamp_ms
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect
from engine.execution.kill_switch import execution_allowed
from engine.runtime.risk_state import get_state, set_state
from engine.execution.execution_ledger import log_submit, log_fill
from engine.strategy.alpha_lifecycle_engine import apply_alpha_lifecycle
from engine.execution.deployable_capital import compute_deployable_equity_from_env
from engine.execution.execution_analytics_engine import (
    get_slippage_feedback,
    get_execution_degradation_snapshot,
)
from engine.execution.order_idempotency import (
    claim_order_submission,
    mark_order_submission_submitted,
    mark_order_submission_unknown,
)
from engine.execution.execution_liquidity_model import (
    attach_liquidity_context,
    get_execution_liquidity_snapshot,
)

# Execution gate providers are fail-closed here to keep adapter import from
# blocking when runtime registry services are unavailable.
_execution_gate_snapshot = None  # type: ignore
_ALLOWED_JOBS = {}  # type: ignore

try:
    from engine.cache.wrappers.kill_switch import read_kill_switch as _kill_switch_snapshot  # type: ignore
except Exception:
    _kill_switch_snapshot = None  # type: ignore

try:
    from engine.cache.wrappers.execution_mode import read_execution_mode as _get_execution_mode  # type: ignore
except Exception:
    _get_execution_mode = None  # type: ignore


LOG = logging.getLogger("engine.execution.broker_ibkr_gateway")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.broker_ibkr_gateway",
        extra=extra or {},
        include_health=False,
        persist=False,
    )


def _safe_f(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_IBKR_GATEWAY_SAFE_FLOAT_FAILED",
            e,
            once_key=f"safe_float:{type(value).__name__}:{str(value)[:64]}",
            value_type=type(value).__name__,
        )
        return float(default)


def _safe_i(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_IBKR_GATEWAY_SAFE_INT_FAILED",
            e,
            once_key=f"safe_int:{type(value).__name__}:{str(value)[:64]}",
            value_type=type(value).__name__,
        )
        return int(default)


def _set_order_total_quantity(order: Any, qty: float) -> None:
    # ibapi's runtime object accepts numeric assignment here, but its Python
    # surface is not typed precisely enough for static analysis.
    setattr(order, "totalQuantity", abs(float(qty)))


def _consume_next_order_id(app: Any) -> int:
    next_order_id = getattr(app, "_next_order_id", None)
    if next_order_id is None:
        raise RuntimeError("IBKR: next order id unavailable")
    oid = _safe_i(next_order_id)
    setattr(app, "_next_order_id", int(oid) + 1)
    return int(oid)


def _real_trading_gate() -> Dict[str, Any]:
    if _execution_gate_snapshot is None or _kill_switch_snapshot is None or _get_execution_mode is None:
        return {
            "ok": False,
            "reason": "execution_gate_providers_missing",
            "real_trading_allowed": False,
            "allowed": False,
        }
    return _execution_gate_snapshot(
        get_execution_mode_fn=_get_execution_mode,
        kill_switches=(_kill_switch_snapshot() or {}),
    )


IBKR_HOST = os.environ.get("IBKR_HOST", "127.0.0.1").strip()
IBKR_PORT = int(os.environ.get("IBKR_PORT", "7497"))
IBKR_CLIENT_ID = int(os.environ.get("IBKR_CLIENT_ID", "42"))

ORDER_TIF = os.environ.get("IBKR_ORDER_TIF", "DAY").strip().upper()
MAX_ORDERS_PER_PASS = int(os.environ.get("IBKR_MAX_ORDERS_PER_PASS", "25"))
SLEEP_BETWEEN_ORDERS_S = float(os.environ.get("IBKR_SLEEP_BETWEEN_ORDERS_S", "0.25"))
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


# ============================================================
# Load Latest Intent Batch
# ============================================================

def _latest_order_row(con) -> Optional[Tuple[int, int, list]]:
    from engine.strategy.portfolio_execution_intents import load_latest_execution_intents
    # Shared intent loading keeps IBKR and Alpaca adapters aligned on the same
    # upstream order semantics.
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
# Market Price Lookup
# ============================================================

def _price_at_or_before(con, symbol: str, ts_ms: int) -> Optional[float]:
    try:
        r = con.execute(
            """
            SELECT px
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
            "BROKER_IBKR_GATEWAY_PRICE_LOOKUP_FAILED",
            e,
            once_key=f"price_lookup:{symbol}:{int(ts_ms)}",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return None


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
# IB API Client Wrapper
# ============================================================

def _connect_ib():
    # Connection setup is intentionally strict in supervised/prod mode so a
    # partially configured IBKR deployment fails early and visibly.
    strict_runtime = (
        str(os.environ.get("ENGINE_SUPERVISED", "")).strip().lower() in ("1", "true", "yes", "y", "on")
        or str(os.environ.get("ENV", "")).strip().lower() in ("prod", "production")
    )
    if strict_runtime:
        missing = [
            name
            for name in ("IBKR_HOST", "IBKR_PORT", "IBKR_CLIENT_ID")
            if not str(os.environ.get(name, "") or "").strip()
        ]
        if missing:
            raise RuntimeError("missing_required_ibkr_env:" + ",".join(missing))

    from ibapi.client import EClient
    from ibapi.wrapper import EWrapper

    class App(EWrapper, EClient):
        def __init__(self):
            EClient.__init__(self, self)
            self._next_order_id = None
            self._next_order_evt = threading.Event()

            self._exec = []
            self._exec_lock = threading.Lock()
            self._exec_evt = threading.Event()

            self._err = []
            self._err_lock = threading.Lock()

            self._pos = []
            self._pos_lock = threading.Lock()
            self._pos_end_evt = threading.Event()

            self._open_orders = []
            self._open_orders_lock = threading.Lock()
            self._open_orders_end_evt = threading.Event()

        def nextValidId(self, orderId: int):
            self._next_order_id = int(orderId)
            self._next_order_evt.set()

        def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
            with self._err_lock:
                self._err.append(
                    {"reqId": reqId, "code": errorCode, "msg": errorString}
                )

        def execDetails(self, reqId, contract, execution):
            # Stream execution callbacks into an in-memory buffer; callers pull
            # from this snapshot after bounded waits rather than blocking forever.
            rec = {
                "symbol": getattr(contract, "symbol", None),
                "orderId": getattr(execution, "orderId", None),
                "permId": getattr(execution, "permId", None),
                "execId": getattr(execution, "execId", None),
                "time": getattr(execution, "time", None),
                "shares": getattr(execution, "shares", None),
                "price": getattr(execution, "price", None),
                "side": getattr(execution, "side", None),
            }
            with self._exec_lock:
                self._exec.append(rec)
                self._exec_evt.set()

        def position(self, account, contract, position, avgCost):
            try:
                sym = getattr(contract, "symbol", None)
                if sym:
                    with self._pos_lock:
                        self._pos.append(
                            {
                                "symbol": str(sym).upper().strip(),
                                "qty": float(position or 0.0),
                                "avg_cost": float(avgCost or 0.0),
                            }
                        )
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_POSITION_CALLBACK_FAILED", e, once_key="position_callback")

        def positionEnd(self):
            self._pos_end_evt.set()

        def openOrder(self, orderId, contract, order, orderState):
            try:
                rec = {
                    "orderId": int(orderId),
                    "permId": getattr(order, "permId", None),
                    "symbol": getattr(contract, "symbol", None),
                    "action": getattr(order, "action", None),
                    "orderType": getattr(order, "orderType", None),
                    "totalQuantity": getattr(order, "totalQuantity", None),
                    "lmtPrice": getattr(order, "lmtPrice", None),
                    "status": getattr(orderState, "status", None),
                }
                with self._open_orders_lock:
                    self._open_orders.append(rec)
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_OPEN_ORDER_CALLBACK_FAILED", e, once_key="open_order_callback")

        def orderStatus(
            self,
            orderId,
            status,
            filled,
            remaining,
            avgFillPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice=0.0,
        ):
            try:
                oid = int(orderId)
            except Exception:
                oid = orderId
            try:
                with self._open_orders_lock:
                    for rec in self._open_orders:
                        if rec.get("orderId") == oid:
                            rec["status"] = status
                            rec["filled"] = filled
                            rec["remaining"] = remaining
                            rec["avgFillPrice"] = avgFillPrice
                            rec["permId"] = permId
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_ORDER_STATUS_CALLBACK_FAILED", e, once_key="order_status_callback")

        def openOrderEnd(self):
            self._open_orders_end_evt.set()

    app = App()
    app.connect(IBKR_HOST, IBKR_PORT, clientId=IBKR_CLIENT_ID)

    t = threading.Thread(target=app.run, daemon=True)
    t.start()

    if not app._next_order_evt.wait(timeout=10):
        raise RuntimeError("IBKR: nextValidId not received")

    return app


def _mk_stock_contract(symbol: str):
    from ibapi.contract import Contract
    c = Contract()
    c.symbol = str(symbol).upper().strip()
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def _mk_market_order(qty: float):
    from ibapi.order import Order
    o = Order()
    o.orderType = "MKT"
    _set_order_total_quantity(o, qty)
    o.action = "BUY" if float(qty) > 0 else "SELL"
    o.tif = ORDER_TIF
    return o


def _mk_limit_order(qty: float, limit_px: float):
    from ibapi.order import Order
    o = Order()
    o.orderType = "LMT"
    _set_order_total_quantity(o, qty)
    o.action = "BUY" if float(qty) > 0 else "SELL"
    o.tif = ORDER_TIF
    o.lmtPrice = float(limit_px)
    return o


def _adaptive_aggressiveness(symbol: str, qty: float, px: float, order_meta: Dict[str, Any]) -> Tuple[str, str, float, float]:
    order_type = str(order_meta.get("order_type") or "").upper().strip()
    aggressiveness = str(order_meta.get("aggressiveness") or "").upper().strip()
    policy_locked = False
    try:
        policy_locked = bool(int(order_meta.get("execution_policy_locked") or 0))
    except Exception:
        policy_locked = bool(order_meta.get("execution_policy_locked"))

    order_meta.update(
        attach_liquidity_context(
            order_meta=order_meta,
            symbol=str(symbol),
            qty=float(qty),
            px=float(px),
            ts_ms=int(order_meta.get("submit_ts_ms") or time.time() * 1000),
        )
    )

    liq = get_execution_liquidity_snapshot(
        symbol=str(symbol),
        qty=float(qty),
        px=float(px),
        ts_ms=int(order_meta.get("submit_ts_ms") or time.time() * 1000),
    )

    spread_bps = float(
        order_meta.get("true_spread_bps")
        or order_meta.get("spread_bps")
        or liq.get("true_spread_bps")
        or 0.0
    )
    if spread_bps <= 0.0:
        spread_bps = float(os.environ.get("IBKR_SPREAD_PROXY_BPS", "8.0"))

    if not order_type:
        order_type = "MARKET"
    if not aggressiveness:
        aggressiveness = "AGGRESSIVE"

    notional = abs(float(qty) * float(px))
    passive_notional_cap = float(os.environ.get("IBKR_PASSIVE_NOTIONAL_CAP_USD", "25000"))

    if (not policy_locked) and spread_bps >= float(os.environ.get("IBKR_PASSIVE_SPREAD_BPS", "8.0")) and notional <= passive_notional_cap:
        order_type = "LIMIT"
        aggressiveness = "PASSIVE"

    if (not policy_locked) and spread_bps <= float(os.environ.get("IBKR_FORCE_AGGRESSIVE_SPREAD_BPS", "2.0")):
        aggressiveness = "AGGRESSIVE"

    try:
        con = connect(readonly=True)
        try:
            fb = get_slippage_feedback(con, broker="ibkr")
            degrade = get_execution_degradation_snapshot(con, lookback_n=500)
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_CONNECTION_CLOSE_FAILED", e, once_key="adaptive_aggressiveness_close", scope="adaptive_aggressiveness")
    except Exception as e:
        _warn_nonfatal("BROKER_IBKR_GATEWAY_EXECUTION_ANALYTICS_READ_FAILED", e, once_key="adaptive_aggressiveness_analytics")
        fb = {}
        degrade = {}

    key = f"{str(order_type).upper().strip()}|{str(aggressiveness).upper().strip()}"
    cfg = fb.get(key) or {}
    limit_offset_bps = float(cfg.get("limit_offset_bps") or 0.0)
    extra_slip_bps = float(cfg.get("extra_slip_bps") or 0.0)

    mean_slippage = float((degrade or {}).get("mean_slippage") or 0.0)
    p95_latency = float((degrade or {}).get("p95_latency") or 0.0)

    submit_ts_ms = int(order_meta.get("submit_ts_ms") or time.time() * 1000)
    hour = int((int(submit_ts_ms) // 3600000) % 24)
    tod_bucket = (
        "open" if hour < 10
        else "midday" if hour < 15
        else "close"
    )

    aggressiveness_bias = float(liq.get("aggressiveness_bias") or 0.0)
    if (not policy_locked) and aggressiveness_bias >= 0.60:
        aggressiveness = "AGGRESSIVE"
    elif (not policy_locked) and aggressiveness_bias >= 0.20 and str(aggressiveness) == "PASSIVE":
        aggressiveness = "NORMAL"
    elif (not policy_locked) and aggressiveness_bias <= -0.10 and order_type == "LIMIT":
        aggressiveness = "PASSIVE"

    if (not policy_locked) and float(extra_slip_bps) >= float(os.environ.get("IBKR_FEEDBACK_PASSIVE_MAX_BPS", "6.0")):
        order_type = "LIMIT"
        aggressiveness = "PASSIVE"

    if (not policy_locked) and float(extra_slip_bps) >= float(os.environ.get("IBKR_FEEDBACK_NORMALIZE_BPS", "12.0")):
        aggressiveness = "NORMAL"

    if (not policy_locked) and float(mean_slippage) >= float(os.environ.get("IBKR_DEGRADE_PASSIVE_SLIP_BPS", "10.0")):
        order_type = "LIMIT"
        aggressiveness = "PASSIVE"

    if (not policy_locked) and float(p95_latency) >= float(os.environ.get("IBKR_DEGRADE_AGGRESSIVE_LAT_MS", "15000")):
        aggressiveness = "AGGRESSIVE"

    if (not policy_locked) and str(tod_bucket) == "open" and float(spread_bps) >= float(os.environ.get("IBKR_OPEN_PASSIVE_SPREAD_BPS", "4.0")):
        order_type = "LIMIT"
        aggressiveness = "PASSIVE"

    if (not policy_locked) and float(liq.get("adv_participation") or 0.0) >= float(os.environ.get("IBKR_FORCE_LIMIT_ADV_PART", "0.10")):
        order_type = "LIMIT"

    order_meta["feedback_limit_offset_bps"] = float(limit_offset_bps)
    order_meta["feedback_extra_slip_bps"] = float(extra_slip_bps)
    order_meta["execution_degradation_mean_slippage"] = float(mean_slippage)
    order_meta["execution_degradation_p95_latency"] = float(p95_latency)
    order_meta["tod_bucket"] = str(tod_bucket)
    order_meta["spread_bps"] = float(spread_bps)
    order_meta["true_spread_bps"] = float(spread_bps)
    order_meta["liquidity_limit_offset_bps"] = float(liq.get("limit_offset_bps") or 0.0)
    order_meta["rolling_adv"] = float(liq.get("rolling_adv") or 0.0)
    order_meta["adv_participation"] = float(liq.get("adv_participation") or 0.0)
    order_meta["intraday_vol_bps"] = float(liq.get("intraday_vol_bps") or 0.0)
    order_meta["spread_regime"] = str(liq.get("spread_regime") or "")
    order_meta["execution_policy_locked"] = 1 if bool(policy_locked) else 0

    return str(order_type), str(aggressiveness), float(spread_bps), float(notional)


def _limit_from_px(px: float, qty: float, aggressiveness: str, spread_bps: float = 0.0) -> float:
    a = str(aggressiveness or "").upper().strip()
    half_spread_px = (float(spread_bps) / 10000.0) * float(px) / 2.0

    if float(qty) > 0:
        if a == "PASSIVE":
            return max(0.01, float(px) - float(half_spread_px))
        if a == "NORMAL":
            return max(0.01, float(px) + (float(half_spread_px) * 0.25))
        return max(0.01, float(px) + float(half_spread_px))
    else:
        if a == "PASSIVE":
            return max(0.01, float(px) + float(half_spread_px))
        if a == "NORMAL":
            return max(0.01, float(px) - (float(half_spread_px) * 0.25))
        return max(0.01, float(px) - float(half_spread_px))


# ============================================================
# TRUE DELTA RECONCILIATION EXECUTION
# ============================================================

def apply_latest_portfolio_orders_live(
    dry_run: bool = False,
    override_orders: Optional[List[Dict[str, Any]]] = None,
    override_order_id: Optional[int] = None,
    override_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:

    # HARD EXECUTION GATE (fail-closed)
    if not bool(dry_run):
        gate = _real_trading_gate()
        if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
            return {"ok": False, "status": "real_trading_blocked", "broker": "ibkr", "gate": gate}

    con = connect()
    try:
        if override_orders is not None:
            order_id = override_order_id
            ts_ms = override_ts_ms or int(time.time() * 1000)
            orders = list(override_orders or [])
        else:
            latest = _latest_order_row(con)
            if not latest:
                return {"ok": True, "status": "no_orders", "broker": "ibkr"}
            order_id, ts_ms, orders = latest

        # ALE
        orders_ale, _ = apply_alpha_lifecycle(
            con=con,
            portfolio_orders_id=order_id,
            portfolio_ts_ms=int(ts_ms),
            orders=orders,
        )

        last_applied = get_state("ibkr_last_portfolio_orders_id", "0")
        if order_id is not None:
            try:
                if int(last_applied) >= int(order_id):
                    return {"ok": True, "status": "already_applied", "broker": "ibkr"}
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_LAST_APPLIED_PARSE_FAILED", e, once_key="last_applied_parse")

        allow0, _, _ = execution_allowed(con=con, symbol=None, regime=None)
        if not allow0:
            return {"ok": False, "status": "blocked_kill_switch", "broker": "ibkr"}

        eq = float(compute_deployable_equity_from_env("IBKR", default_equity=0.0) or 0.0)
        if eq <= 0:
            return {"ok": False, "status": "missing_equity", "broker": "ibkr"}

        if dry_run:
            return {"ok": True, "status": "dry_run_preview", "orders": orders_ale}

        # ----------------------------------------
        # Pull LIVE POSITIONS
        # ----------------------------------------
        live_positions = {p["symbol"]: p["qty"] for p in get_positions_live()}
        latest_prices = _load_latest_prices(con)

        app = _connect_ib()
        try:
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

                current_qty = float(live_positions.get(symbol, 0.0))
                raw_qty = _safe_f(o.get("qty"), 0.0)
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
                    delta = float(target_qty - current_qty)

                if abs(delta) < 1e-6:
                    continue

                delta, risk_cap_audit = _apply_execution_risk_caps(
                    positions=live_positions,
                    prices=latest_prices,
                    symbol=symbol,
                    current_qty=current_qty,
                    delta_qty=delta,
                    px=float(px),
                    equity=float(eq),
                )
                if abs(delta) < 1e-6:
                    live_positions[symbol] = float(current_qty)
                    continue

                order_meta = dict(o or {})
                order_meta["portfolio_risk_caps"] = dict(risk_cap_audit or {})
                order_type, aggressiveness, spread_bps, est_notional = _adaptive_aggressiveness(
                    symbol=symbol,
                    qty=float(delta),
                    px=float(px),
                    order_meta=order_meta,
                )

                order_meta["order_type"] = str(order_type)
                order_meta["aggressiveness"] = str(aggressiveness)
                order_meta["spread_bps"] = float(spread_bps)
                order_meta["true_spread_bps"] = float(spread_bps)
                order_meta["estimated_notional"] = float(est_notional)
                order_meta["passive_flag"] = 1 if str(aggressiveness).upper().strip() == "PASSIVE" else 0
                order_meta["aggressive_flag"] = 1 if str(aggressiveness).upper().strip() == "AGGRESSIVE" else 0
                order_meta["arrival_mid_px"] = float(order_meta.get("mid_px") or px)
                order_meta["submit_hour_bucket"] = (
                    "open" if int((int(ts_ms) // 3600000) % 24) < 10
                    else "midday" if int((int(ts_ms) // 3600000) % 24) < 15
                    else "close"
                )

                contract = _mk_stock_contract(symbol)
                limit_px = None
                if str(order_type).upper().strip() == "LIMIT":
                    liq_offset_bps = float(order_meta.get("liquidity_limit_offset_bps") or 0.0)
                    policy_offset_bps = float(order_meta.get("entry_limit_offset_bps") or 0.0)
                    liq_offset_bps = max(float(liq_offset_bps), float(policy_offset_bps))
                    limit_px = _limit_from_px(float(px), float(delta), str(aggressiveness), float(spread_bps))
                    if liq_offset_bps > 0.0:
                        if float(delta) > 0:
                            limit_px = float(limit_px) + ((float(liq_offset_bps) / 10000.0) * float(px))
                        else:
                            limit_px = float(limit_px) - ((float(liq_offset_bps) / 10000.0) * float(px))
                    order_meta["limit_px"] = float(limit_px)
                    order = _mk_limit_order(delta, limit_px)
                else:
                    order = _mk_market_order(delta)

                guard = claim_order_submission(
                    con=con,
                    broker="ibkr",
                    portfolio_orders_id=order_id,
                    portfolio_ts_ms=int(ts_ms),
                    order=o,
                )
                if not bool(guard.get("ok")):
                    return {
                        "ok": False,
                        "status": str(guard.get("status") or "order_idempotency_claim_failed"),
                        "broker": "ibkr",
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
                client_order_id = str(guard.get("client_order_id") or "")

                oid = _consume_next_order_id(app)

                source_alert_id = (
                    _safe_i(o.get("source_alert_id"))
                    if isinstance(o, dict) and o.get("source_alert_id") is not None
                    else None
                )
                bid_px = _safe_f(order_meta.get("bid_px")) if order_meta.get("bid_px") is not None else None
                ask_px = _safe_f(order_meta.get("ask_px")) if order_meta.get("ask_px") is not None else None

                try:
                    app.placeOrder(oid, contract, order)
                except Exception as e:
                    try:
                        mark_order_submission_unknown(
                            con=con,
                            order_uid=order_uid,
                            last_error=str(e),
                        )
                    except Exception as mark_err:
                        _warn_nonfatal("BROKER_IBKR_GATEWAY_MARK_UNKNOWN_FAILED", mark_err, once_key="mark_submission_unknown", order_uid=str(order_uid), broker_order_id=str(oid))
                    return {
                        "ok": False,
                        "status": "submit_inflight_unknown",
                        "broker": "ibkr",
                        "stop_failover": True,
                        "detail": "broker_submit_ambiguous",
                        "order_uid": str(order_uid),
                        "client_order_id": str(client_order_id),
                        "broker_order_id": str(oid),
                        "symbol": str(symbol),
                        "error": str(e),
                        "submitted_n": int(n),
                    }

                submit_ts_ms = int(time.time() * 1000)

                order_meta["order_uid"] = str(order_uid)
                order_meta["idempotency_status"] = "submitted"

                log_submit(
                    client_order_id=str(client_order_id),
                    broker="ibkr",
                    symbol=symbol,
                    qty=float(delta),
                    submit_ts_ms=int(submit_ts_ms),
                    ref_px=float(px),
                    broker_order_id=str(oid),
                    portfolio_orders_id=order_id,
                    source_alert_id=source_alert_id,
                    extra=order_meta,
                    expected_px=float(px),
                    mid_px=float(px),
                    bid_px=bid_px,
                    ask_px=ask_px,
                    spread_bps=float(spread_bps),
                    order_uid=str(order_uid),
                    idempotency_status="submitted",
                )

                mark_order_submission_submitted(
                    con=con,
                    order_uid=str(order_uid),
                    client_order_id=str(client_order_id),
                    broker_order_id=str(oid),
                    submit_ts_ms=int(submit_ts_ms),
                )

                if str(order_type).upper().strip() == "LIMIT":
                    try:
                        from engine.execution.execution_microstructure import record_open_order
                        record_open_order(
                            broker="ibkr",
                            symbol=symbol,
                            qty=float(delta),
                            order_type=str(order_type),
                            aggressiveness=str(aggressiveness),
                            limit_px=(float(limit_px) if limit_px is not None else None),
                            client_order_id=str(client_order_id),
                            broker_order_id=str(oid),
                            max_attempts=int(os.environ.get("IBKR_LIMIT_RETRY_MAX_ATTEMPTS", "3")),
                            portfolio_orders_id=int(order_id) if order_id is not None else None,
                            source_alert_id=source_alert_id,
                            meta={
                                **dict(order_meta or {}),
                                "escalation_timeout_s": float(os.environ.get("IBKR_LIMIT_RETRY_TIMEOUT_S", "45")),
                                "broker_submit_ts_ms": int(submit_ts_ms),
                                "broker": "ibkr",
                                "ack_timeout_ms": int(float(os.environ.get("IBKR_ACK_TIMEOUT_S", "15")) * 1000.0),
                                "client_order_id": str(client_order_id),
                            },
                        )
                    except Exception as e:
                        _warn_nonfatal("BROKER_IBKR_GATEWAY_RECORD_OPEN_ORDER_FAILED", e, once_key="record_open_order", client_order_id=str(client_order_id), broker_order_id=str(oid), symbol=str(symbol))

                submitted.append(
                    {
                        "symbol": symbol,
                        "delta_qty": delta,
                        "order_type": str(order_type),
                        "aggressiveness": str(aggressiveness),
                        "spread_bps": float(spread_bps),
                    }
                )
                live_positions[symbol] = float(current_qty) + float(delta)
                n += 1
                time.sleep(max(0.0, float(SLEEP_BETWEEN_ORDERS_S)))

            if order_id is not None:
                set_state("ibkr_last_portfolio_orders_id", str(int(order_id)))

            return {"ok": True, "status": "applied", "submitted_n": n, "broker": "ibkr"}
        finally:
            try:
                app.disconnect()
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_DISCONNECT_FAILED", e, once_key="apply_latest_portfolio_orders_live_disconnect")

    finally:
        con.close()


# ============================================================
# Positions
# ============================================================

def get_positions_snapshot(timeout_s: float = 10.0) -> Dict[str, float]:
    try:
        rows = get_positions_live(timeout_s)
        return {r["symbol"]: r["qty"] for r in rows}
    except Exception as e:
        _warn_nonfatal(
            "BROKER_IBKR_GATEWAY_POSITIONS_SNAPSHOT_FAILED",
            e,
            once_key="positions_snapshot",
            timeout_s=float(timeout_s),
        )
        return {}


def get_positions_live(timeout_s: float = 8.0) -> List[Dict[str, Any]]:
    app = _connect_ib()
    try:
        with app._pos_lock:
            app._pos = []
        app._pos_end_evt.clear()

        app.reqPositions()
        ok = app._pos_end_evt.wait(timeout=float(timeout_s))
        app.cancelPositions()

        if not ok:
            raise RuntimeError("IBKR: positions timeout")

        with app._pos_lock:
            return list(app._pos or [])
    finally:
        app.disconnect()


def list_open_orders_live(timeout_s: float = 8.0) -> List[Dict[str, Any]]:
    app = _connect_ib()
    try:
        with app._open_orders_lock:
            app._open_orders = []
        app._open_orders_end_evt.clear()

        app.reqOpenOrders()
        ok = app._open_orders_end_evt.wait(timeout=float(timeout_s))

        if not ok:
            raise RuntimeError("IBKR: open orders timeout")

        with app._open_orders_lock:
            return list(app._open_orders or [])
    finally:
        app.disconnect()


def ping_broker_connection(timeout_s: float = 8.0, retries: int = 2) -> Dict[str, Any]:
    started_ms = int(time.time() * 1000)
    last_error = None
    max_retries = max(1, int(retries or 1))

    for attempt in range(1, max_retries + 1):
        app = None
        try:
            app = _connect_ib()
            latency_ms = int(time.time() * 1000) - int(started_ms)
            err_rows = []
            try:
                with app._err_lock:
                    err_rows = list(app._err or [])
            except Exception:
                err_rows = []

            return {
                "ok": True,
                "broker": "ibkr",
                "state": "connected",
                "latency_ms": int(latency_ms),
                "attempt": int(attempt),
                "errors": err_rows[:5],
            }
        except Exception as e:
            last_error = str(e)
            try:
                time.sleep(min(1.0, max(0.1, float(timeout_s) / 8.0)))
            except Exception as sleep_err:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_RETRY_SLEEP_FAILED", sleep_err, once_key="ping_broker_connection_sleep")
        finally:
            try:
                if app is not None:
                    app.disconnect()
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_DISCONNECT_FAILED", e, once_key="ping_broker_connection_disconnect")

    return {
        "ok": False,
        "broker": "ibkr",
        "state": "reconnect_failed",
        "latency_ms": int(time.time() * 1000) - int(started_ms),
        "attempt": int(max_retries),
        "error": str(last_error or "ibkr_connect_failed"),
    }


def get_order(order_id: str, timeout_s: float = 8.0) -> Dict[str, Any]:
    oid = str(order_id or "").strip()
    if not oid:
        return {}
    for rec in list_open_orders_live(timeout_s=timeout_s):
        try:
            if str(rec.get("orderId") or "") == oid:
                return dict(rec)
        except Exception as e:
            _warn_nonfatal(
                "BROKER_IBKR_GATEWAY_OPEN_ORDER_MATCH_FAILED",
                e,
                once_key=f"open_order_match:{oid}",
                order_id=str(oid),
            )
            continue
    return {}


def cancel_order(order_id: str, timeout_s: float = 8.0) -> Dict[str, Any]:
    oid = int(order_id)
    app = _connect_ib()
    try:
        app.cancelOrder(int(oid))
        time.sleep(min(max(float(timeout_s), 0.1), 2.0))
        return {"ok": True, "orderId": int(oid)}
    finally:
        try:
            app.disconnect()
        except Exception as e:
            _warn_nonfatal("BROKER_IBKR_GATEWAY_DISCONNECT_FAILED", e, once_key="cancel_order_disconnect")


def submit_limit_order(symbol: str, qty: float, limit_price: float, client_oid: str) -> Dict[str, Any]:
    gate = _real_trading_gate()
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "ibkr"}
    app = _connect_ib()
    try:
        oid = _consume_next_order_id(app)
        contract = _mk_stock_contract(symbol)
        order = _mk_limit_order(qty, limit_price)
        app.placeOrder(oid, contract, order)
        return {"id": str(oid), "client_order_id": str(client_oid), "orderType": "LMT", "limit_price": float(limit_price)}
    finally:
        try:
            app.disconnect()
        except Exception as e:
            _warn_nonfatal("BROKER_IBKR_GATEWAY_DISCONNECT_FAILED", e, once_key="submit_limit_order_disconnect")


def submit_market_order(symbol: str, qty: float, client_oid: str) -> Dict[str, Any]:
    gate = _real_trading_gate()
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": "ibkr"}
    app = _connect_ib()
    try:
        oid = _consume_next_order_id(app)
        contract = _mk_stock_contract(symbol)
        order = _mk_market_order(qty)
        app.placeOrder(oid, contract, order)
        return {"id": str(oid), "client_order_id": str(client_oid), "orderType": "MKT"}
    finally:
        try:
            app.disconnect()
        except Exception as e:
            _warn_nonfatal("BROKER_IBKR_GATEWAY_DISCONNECT_FAILED", e, once_key="submit_market_order_disconnect")


def list_recent_executions_live(after_ts_ms: int, timeout_s: float = 8.0) -> List[Dict[str, Any]]:
    """
    Used by crash recovery replay to fetch broker executions after a timestamp.
    """
    app = _connect_ib()
    try:
        from ibapi.execution import ExecutionFilter

        with app._exec_lock:
            app._exec = []

        app._exec_evt.clear()

        filt = ExecutionFilter()

        if int(after_ts_ms or 0) > 0:
            try:
                from datetime import datetime
                filt.time = datetime.utcfromtimestamp(after_ts_ms / 1000.0).strftime("%Y%m%d %H:%M:%S")
            except Exception as e:
                _warn_nonfatal("BROKER_IBKR_GATEWAY_EXECUTION_FILTER_TIME_FAILED", e, once_key="execution_filter_time", after_ts_ms=int(after_ts_ms or 0))

        app.reqExecutions(0, filt)
        app._exec_evt.wait(timeout=float(timeout_s))

        with app._exec_lock:
            return list(app._exec or [])

    finally:
        app.disconnect()


# ============================================================
# Poll Stub
# ============================================================

def poll_and_log_fills(after_ts_ms: int) -> Dict[str, Any]:
    app = _connect_ib()
    con = connect()

    from ibapi.execution import ExecutionFilter
    filt = ExecutionFilter()

    try:
        app._exec_evt.clear()
        app.reqExecutions(0, filt)
        app._exec_evt.wait(timeout=5.0)

        with app._exec_lock:
            rows = list(app._exec or [])
            app._exec = []

        n = 0
        for r in rows:
            try:
                broker_order_id = str(r.get("orderId") or "").strip()
                client_order_id = broker_order_id
                submit_ts_ms = None
                expected_px = None
                mid_px = None
                spread_bps = None
                extra_submit_json = None

                if broker_order_id:
                    row = con.execute(
                        """
                        SELECT client_order_id, submit_ts_ms, expected_px, mid_px, spread_bps, extra_json
                        FROM execution_orders
                        WHERE broker_order_id=?
                        ORDER BY submit_ts_ms DESC
                        LIMIT 1
                        """,
                        (broker_order_id,),
                    ).fetchone()
                    if row:
                        if row[0]:
                            client_order_id = str(row[0])
                        submit_ts_ms = int(row[1]) if row[1] is not None else None
                        expected_px = float(row[2]) if row[2] is not None else None
                        mid_px = float(row[3]) if row[3] is not None else None
                        spread_bps = float(row[4]) if row[4] is not None else None
                        extra_submit_json = row[5]

                fill_ts_ms = parse_broker_timestamp_ms(
                    r.get("time"),
                    default_ms=int(time.time() * 1000),
                )
                if fill_ts_ms < int(after_ts_ms):
                    continue

                fill_latency_ms = None
                if submit_ts_ms is not None:
                    fill_latency_ms = max(0, int(fill_ts_ms) - int(submit_ts_ms))

                submit_extra = {}
                try:
                    submit_extra = json.loads(extra_submit_json or "{}")
                    if not isinstance(submit_extra, dict):
                        submit_extra = {}
                except Exception:
                    submit_extra = {}

                log_fill(
                    client_order_id=client_order_id,
                    fill_id=str(r.get("execId") or r.get("permId") or broker_order_id or ""),
                    broker="ibkr",
                    symbol=str(r.get("symbol")),
                    qty=float(r.get("shares") or 0.0),
                    fill_px=float(r.get("price") or 0.0),
                    fill_ts_ms=fill_ts_ms,
                    extra={
                        **submit_extra,
                        **dict(r or {}),
                        "broker_order_id": broker_order_id,
                        "submit_ts_ms": submit_ts_ms,
                        "expected_px": expected_px,
                        "mid_px": mid_px,
                        "spread_bps": spread_bps,
                        "fill_latency_ms": fill_latency_ms,
                    },
                )
                n += 1
            except Exception as e:
                _warn_nonfatal(
                    "BROKER_IBKR_GATEWAY_FILL_LOG_FAILED",
                    e,
                    once_key=f"fill_log:{broker_order_id}",
                    broker_order_id=str(broker_order_id),
                )
                continue

        return {"ok": True, "fills_logged": int(n), "status": "ibkr_polled"}

    finally:
        try:
            app.disconnect()
        except Exception as e:
            _warn_nonfatal("BROKER_IBKR_GATEWAY_DISCONNECT_FAILED", e, once_key="poll_and_log_fills_disconnect")
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("BROKER_IBKR_GATEWAY_CONNECTION_CLOSE_FAILED", e, once_key="poll_and_log_fills_close", scope="poll_and_log_fills")


# ============================================================
# Execution Stream Daemon
# ============================================================

def run_execution_stream_daemon(poll_sleep_s: float = 1.0) -> None:
    app = _connect_ib()

    from ibapi.execution import ExecutionFilter
    filt = ExecutionFilter()

    try:
        while True:
            con = connect(readonly=True)
            try:
                app._exec_evt.clear()
                app.reqExecutions(0, filt)
                app._exec_evt.wait(timeout=5.0)

                with app._exec_lock:
                    rows = list(app._exec or [])
                    app._exec = []

                for r in rows:
                    try:
                        broker_order_id = str(r.get("orderId") or "").strip()
                        client_order_id = broker_order_id

                        if broker_order_id:
                            row = con.execute(
                                """
                                SELECT client_order_id
                                FROM execution_orders
                                WHERE broker_order_id=?
                                ORDER BY submit_ts_ms DESC
                                LIMIT 1
                                """,
                                (broker_order_id,),
                            ).fetchone()
                            if row and row[0]:
                                client_order_id = str(row[0])

                        fill_ts_ms = parse_broker_timestamp_ms(
                            r.get("time"),
                            default_ms=int(time.time() * 1000),
                        )

                        submit_ts_ms = None
                        expected_px = None
                        mid_px = None
                        spread_bps = None
                        extra_submit_json = None

                        if broker_order_id:
                            row2 = con.execute(
                                """
                                SELECT submit_ts_ms, expected_px, mid_px, spread_bps, extra_json
                                FROM execution_orders
                                WHERE broker_order_id=?
                                ORDER BY submit_ts_ms DESC
                                LIMIT 1
                                """,
                                (broker_order_id,),
                            ).fetchone()
                            if row2:
                                submit_ts_ms = int(row2[0]) if row2[0] is not None else None
                                expected_px = float(row2[1]) if row2[1] is not None else None
                                mid_px = float(row2[2]) if row2[2] is not None else None
                                spread_bps = float(row2[3]) if row2[3] is not None else None
                                extra_submit_json = row2[4]

                        fill_latency_ms = None
                        if submit_ts_ms is not None:
                            fill_latency_ms = max(0, int(fill_ts_ms) - int(submit_ts_ms))

                        submit_extra = {}
                        try:
                            submit_extra = json.loads(extra_submit_json or "{}")
                            if not isinstance(submit_extra, dict):
                                submit_extra = {}
                        except Exception:
                            submit_extra = {}

                        log_fill(
                            client_order_id=client_order_id,
                            fill_id=str(r.get("execId") or r.get("permId") or broker_order_id or ""),
                            broker="ibkr",
                            symbol=str(r.get("symbol")),
                            qty=float(r.get("shares") or 0.0),
                            fill_px=float(r.get("price") or 0.0),
                            fill_ts_ms=fill_ts_ms,
                            extra={
                                **submit_extra,
                                **dict(r or {}),
                                "broker_order_id": broker_order_id,
                                "submit_ts_ms": submit_ts_ms,
                                "expected_px": expected_px,
                                "mid_px": mid_px,
                                "spread_bps": spread_bps,
                                "fill_latency_ms": fill_latency_ms,
                            },
                        )
                    except Exception as e:
                        _warn_nonfatal(
                            "BROKER_IBKR_GATEWAY_STREAM_FILL_LOG_FAILED",
                            e,
                            once_key=f"stream_fill_log:{broker_order_id}",
                            broker_order_id=str(broker_order_id),
                        )
                        continue
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn_nonfatal("BROKER_IBKR_GATEWAY_CONNECTION_CLOSE_FAILED", e, once_key="execution_stream_close", scope="run_execution_stream_daemon")

            time.sleep(max(0.1, float(poll_sleep_s)))

    finally:
        try:
            app.disconnect()
        except Exception as e:
            _warn_nonfatal("BROKER_IBKR_GATEWAY_DISCONNECT_FAILED", e, once_key="execution_stream_disconnect")
