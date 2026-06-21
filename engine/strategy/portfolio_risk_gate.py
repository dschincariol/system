"""
FILE: portfolio_risk_gate.py

Portfolio-level hard risk gate. It clamps desired targets for net exposure,
turnover, and drawdown-driven restrictions before orders are emitted.
"""

import os
import json
import logging
import math
from typing import Any, Dict, Tuple, List, Optional
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import table_exists

from engine.strategy.drawdown_state import evaluate_current_drawdown
from engine.data.weather_features import get_weather_feature_snapshot
from engine.data.asset_map import asset_class_for_symbol

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.portfolio_risk_gate",
        extra=extra or None,
        include_health=False,
        persist=False,
    )

USE = os.environ.get("PORTFOLIO_USE_RISK_GATE", "1") == "1"

MAX_NET = float(os.environ.get("PORTFOLIO_MAX_NET_EXPOSURE", "0.60"))
MAX_TURNOVER = float(os.environ.get("PORTFOLIO_MAX_TURNOVER", "0.60"))

DD_ADD_BLOCK = float(os.environ.get("PORTFOLIO_DD_ADD_BLOCK", "0.08"))
DD_GROSS_MULT = float(os.environ.get("PORTFOLIO_DD_GROSS_MULT", "0.70"))

GROSS_CAP = float(os.environ.get("PORTFOLIO_GROSS_CAP", "1.00"))

# ------            -- ------------------------------------------------------
# Hard Sleeve Caps (asset-class sleeves)
# ------            -- ------------------------------------------------------
USE_SLEEVE_CAPS = os.environ.get("PORTFOLIO_USE_SLEEVE_CAPS", "1") == "1"

# JSON maps: {"EQUITY":0.60,"CRYPTO":0.20,"FX":0.10,"RATES":0.10,"COMMODITY":0.10}
SLEEVE_MAX_GROSS_JSON = os.environ.get("PORTFOLIO_SLEEVE_MAX_GROSS_JSON", "").strip()
SLEEVE_MAX_NET_JSON = os.environ.get("PORTFOLIO_SLEEVE_MAX_NET_JSON", "").strip()

SLEEVE_DEFAULT_MAX_GROSS = float(os.environ.get("PORTFOLIO_SLEEVE_DEFAULT_MAX_GROSS", "1.00"))
SLEEVE_DEFAULT_MAX_NET = float(os.environ.get("PORTFOLIO_SLEEVE_DEFAULT_MAX_NET", "1.00"))


def _load_json_map(raw: str) -> Dict[str, float]:
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        if isinstance(d, dict):
            out = {}
            for k, v in d.items():
                kk = str(k or "").upper().strip()
                if not kk:
                    continue
                try:
                    out[kk] = float(v)
                except Exception as e:
                    _warn_nonfatal(
                        "PORTFOLIO_RISK_GATE_JSON_MAP_VALUE_PARSE_FAILED",
                        e,
                        once_key=f"json_map_value:{kk}",
                        key=str(kk),
                    )
                    continue
            return out
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_JSON_MAP_LOAD_FAILED",
            e,
            once_key="json_map_load",
        )
        return {}
    return {}


_SLEEVE_MAX_GROSS = _load_json_map(SLEEVE_MAX_GROSS_JSON)
_SLEEVE_MAX_NET = _load_json_map(SLEEVE_MAX_NET_JSON)


def _sleeve(sym: str) -> str:
    try:
        return str(asset_class_for_symbol(sym) or "UNKNOWN").upper().strip() or "UNKNOWN"
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_SLEEVE_CLASSIFY_FAILED",
            e,
            once_key=f"sleeve:{sym}",
            symbol=str(sym),
        )
        return "UNKNOWN"


def _sleeve_gross(out: Dict[str, Dict[str, Any]], sleeve_name: str) -> float:
    g = 0.0
    sn = str(sleeve_name or "").upper().strip()
    for s, tgt in (out or {}).items():
        if _sleeve(s) != sn:
            continue
        try:
            g += abs(float(tgt.get("weight", 0.0) or 0.0))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_SLEEVE_GROSS_WEIGHT_FAILED", e, once_key=f"sleeve_gross:{s}", symbol=str(s))
    return float(g)


def _sleeve_net(out: Dict[str, Dict[str, Any]], sleeve_name: str) -> float:
    n = 0.0
    sn = str(sleeve_name or "").upper().strip()
    for s, tgt in (out or {}).items():
        if _sleeve(s) != sn:
            continue
        try:
            side = str(tgt.get("side", "FLAT")).upper()
            w = float(tgt.get("weight", 0.0) or 0.0)
            if side == "SHORT":
                n -= abs(w)
            elif side == "LONG":
                n += abs(w)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_SLEEVE_NET_WEIGHT_FAILED", e, once_key=f"sleeve_net:{s}", symbol=str(s))
    return float(n)


def _apply_sleeve_caps(out: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> None:
    if not USE_SLEEVE_CAPS:
        return

    sleeves = set()
    for s in (out or {}).keys():
        sleeves.add(_sleeve(s))

    applied = {}
    for sn in sorted(list(sleeves)):
        mg = float(_SLEEVE_MAX_GROSS.get(sn, SLEEVE_DEFAULT_MAX_GROSS))
        mn = float(_SLEEVE_MAX_NET.get(sn, SLEEVE_DEFAULT_MAX_NET))

        # gross cap
        g = _sleeve_gross(out, sn)
        if mg > 0.0 and g > mg + 1e-12 and g > 1e-12:
            sc = float(mg) / float(g)
            for s, tgt in (out or {}).items():
                if _sleeve(s) != sn:
                    continue
                try:
                    tgt["weight"] = float(tgt.get("weight", 0.0) or 0.0) * float(sc)
                    tgt.setdefault("reason", {})
                    tgt["reason"].setdefault("risk_gate", {})
                    tgt["reason"]["risk_gate"]["sleeve_gross_scale"] = float(sc)
                    tgt["reason"]["risk_gate"]["sleeve"] = str(sn)
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_RISK_GATE_APPLY_SLEEVE_GROSS_SCALE_FAILED", e, once_key=f"apply_sleeve_gross:{s}", symbol=str(s), sleeve=str(sn))
            applied.setdefault(sn, {})
            applied[sn]["gross_cap"] = float(mg)
            applied[sn]["gross_pre"] = float(g)
            applied[sn]["gross_scale"] = float(sc)

        # net cap (scale only overweight side)
        n = _sleeve_net(out, sn)
        if mn > 0.0 and abs(float(n)) > mn + 1e-12:
            side_to_scale = "LONG" if n > 0 else "SHORT"
            denom = 0.0
            for s, tgt in (out or {}).items():
                if _sleeve(s) != sn:
                    continue
                side = str(tgt.get("side", "FLAT")).upper()
                if side == side_to_scale:
                    denom += abs(float(tgt.get("weight", 0.0) or 0.0))
            if denom > 1e-12:
                # reduce overweight side by excess
                target_sum = float(denom) - (abs(float(n)) - float(mn))
                sc = max(0.0, float(target_sum) / float(denom))
                for s, tgt in (out or {}).items():
                    if _sleeve(s) != sn:
                        continue
                    side = str(tgt.get("side", "FLAT")).upper()
                    if side == side_to_scale:
                        try:
                            tgt["weight"] = float(tgt.get("weight", 0.0) or 0.0) * float(sc)
                            tgt.setdefault("reason", {})
                            tgt["reason"].setdefault("risk_gate", {})
                            tgt["reason"]["risk_gate"]["sleeve_net_scale"] = float(sc)
                            tgt["reason"]["risk_gate"]["sleeve_net_side"] = str(side_to_scale)
                            tgt["reason"]["risk_gate"]["sleeve"] = str(sn)
                        except Exception as e:
                            _warn_nonfatal("PORTFOLIO_RISK_GATE_APPLY_SLEEVE_NET_SCALE_FAILED", e, once_key=f"apply_sleeve_net:{s}", symbol=str(s), sleeve=str(sn))
                applied.setdefault(sn, {})
                applied[sn]["net_cap"] = float(mn)
                applied[sn]["net_pre"] = float(n)
                applied[sn]["net_scale_side"] = str(side_to_scale)
                applied[sn]["net_scale"] = float(sc)

    if applied:
        info["sleeve_caps"] = applied

# ------            -- ------------------------------------------------------
# Optional: weather-aware portfolio clamps (read-only)
# ------            -- ------------------------------------------------------
USE_WX_RISK = os.environ.get("PORTFOLIO_USE_WEATHER_RISK", "1") == "1"

# If storm_risk >= threshold, block any increase in gross exposure
WX_STORM_ADD_BLOCK = float(os.environ.get("PORTFOLIO_WX_STORM_ADD_BLOCK", "0.60"))

# If storm_risk >= threshold, apply additional gross cap multiplier
WX_STORM_GROSS_MULT = float(os.environ.get("PORTFOLIO_WX_STORM_GROSS_MULT", "0.85"))

# Only evaluate top-N symbols by abs(target weight) to bound DB queries
WX_MAX_SYMBOLS = int(os.environ.get("PORTFOLIO_WX_MAX_SYMBOLS", "25"))


def _side_sign(side: str) -> float:
    s = str(side or "FLAT").upper()
    if s == "LONG":
        return 1.0
    if s == "SHORT":
        return -1.0
    return 0.0


def _cur_signed_weight(cur_row: Dict[str, Any]) -> float:
    if not cur_row:
        return 0.0
    w = float(cur_row.get("weight", 0.0) or 0.0)
    sgn = _side_sign(cur_row.get("side", "FLAT"))
    return float(w) * float(sgn)


def _tgt_signed_weight(tgt_row: Dict[str, Any]) -> float:
    if not tgt_row:
        return 0.0
    w = float(tgt_row.get("weight", 0.0) or 0.0)
    sgn = _side_sign(tgt_row.get("side", "FLAT"))
    return float(w) * float(sgn)


def _gross(desired: Dict[str, Dict[str, Any]]) -> float:
    g = 0.0
    for v in (desired or {}).values():
        try:
            g += abs(float(v.get("weight", 0.0) or 0.0))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_GROSS_ACCUMULATION_FAILED", e, once_key="gross_accumulation")
    return float(g)


def _net(desired: Dict[str, Dict[str, Any]]) -> float:
    n = 0.0
    for v in (desired or {}).values():
        try:
            n += _tgt_signed_weight(v)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_NET_ACCUMULATION_FAILED", e, once_key="net_accumulation")
    return float(n)


def _finite_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(float(out)):
        return None
    return float(out)


def _env_float(*keys: str, default: float) -> float:
    for key in keys:
        raw = os.environ.get(str(key))
        if raw in (None, ""):
            continue
        parsed = _finite_float(raw)
        if parsed is not None:
            return float(parsed)
    return float(default)


def _table_columns(con: Any, table_name: str) -> set[str]:
    try:
        return {
            str(row[1]).strip().lower()
            for row in (con.execute(f"PRAGMA table_info({table_name})").fetchall() or [])
            if row and len(row) > 1 and row[1]
        }
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_TABLE_COLUMNS_FAILED",
            e,
            once_key=f"table_columns:{table_name}",
            table=str(table_name),
        )
        return set()


def _latest_price(con: Any, symbol: str) -> Optional[float]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    try:
        if not table_exists(con, "prices"):
            return None
        cols = _table_columns(con, "prices")
        price_col = "price" if "price" in cols else ("px" if "px" in cols else None)
        if price_col is None:
            return None
        row = con.execute(
            f"""
            SELECT {price_col}
            FROM prices
            WHERE UPPER(symbol)=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (sym,),
        ).fetchone()
        if not row:
            return None
        px = _finite_float(row[0])
        return float(px) if px is not None and px > 0.0 else None
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_PRICE_READ_FAILED",
            e,
            once_key=f"latest_price:{sym}",
            symbol=str(sym),
        )
        return None


def _read_execution_equity(con: Any, explicit_equity: Optional[float]) -> Tuple[Optional[float], Optional[str]]:
    eq = _finite_float(explicit_equity)
    if eq is not None and eq > 0.0:
        return float(eq), None

    try:
        if not table_exists(con, "broker_account"):
            return None, None
        cols = _table_columns(con, "broker_account")
        if "equity" not in cols:
            return None, "broker_account_missing_equity"
        order_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "")
        sql = "SELECT equity FROM broker_account"
        if "id" in cols:
            sql += " WHERE id=1"
        if order_col:
            sql += f" ORDER BY {order_col} DESC"
        sql += " LIMIT 1"
        row = con.execute(sql).fetchone()
        if not row:
            return None, None
        eq = _finite_float(row[0])
        if eq is None or eq <= 0.0:
            return None, "broker_account_invalid_equity"
        return float(eq), None
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_EQUITY_READ_FAILED",
            e,
            once_key="execution_equity_read",
        )
        return None, "broker_account_equity_read_failed"


def _order_signed_target_weight(
    con: Any,
    order: Dict[str, Any],
    equity: Optional[float],
) -> Tuple[Optional[float], Optional[str]]:
    if not isinstance(order, dict):
        return None, "order_not_mapping"

    side = str(order.get("to_side") or order.get("side") or "").strip().upper()
    raw_weight = order.get("to_weight")
    if raw_weight is None and order.get("weight") is not None:
        raw_weight = order.get("weight")
    if raw_weight not in (None, ""):
        weight = _finite_float(raw_weight)
        if weight is None:
            return None, "invalid_to_weight"
        if side == "FLAT":
            return 0.0, None
        if side == "SHORT":
            return -abs(float(weight)), None
        if side == "LONG":
            return abs(float(weight)), None
        return float(weight), None

    raw_qty = order.get("qty")
    if raw_qty in (None, ""):
        return None, "missing_to_weight"
    qty = _finite_float(raw_qty)
    if qty is None:
        return None, "invalid_qty"

    eq = _finite_float(equity)
    if eq is None or eq <= 0.0:
        return None, "missing_equity_for_quantity_order"

    px = None
    for key in ("ref_px", "mid_px", "expected_px", "limit_px", "price"):
        if order.get(key) in (None, ""):
            continue
        px = _finite_float(order.get(key))
        if px is not None and px > 0.0:
            break
    if px is None or px <= 0.0:
        px = _latest_price(con, str(order.get("symbol") or ""))
    if px is None or px <= 0.0:
        return None, "missing_price_for_quantity_order"

    signed_qty = float(qty)
    if signed_qty > 0.0 and side == "SHORT":
        signed_qty = -abs(float(signed_qty))
    elif signed_qty < 0.0 and side == "LONG":
        signed_qty = abs(float(signed_qty))
    return float(signed_qty) * float(px) / float(eq), None


def _read_existing_position_weights(
    con: Any,
    equity: Optional[float],
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    out: Dict[str, float] = {}
    errors: List[Dict[str, Any]] = []
    try:
        if not table_exists(con, "broker_positions"):
            return out, errors
        cols = _table_columns(con, "broker_positions")
        if not {"symbol", "qty"}.issubset(cols):
            return out, errors
        px_col = "avg_px" if "avg_px" in cols else ("price" if "price" in cols else None)
        order_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("ts_ms" if "ts_ms" in cols else "")
        select_px = px_col if px_col else "NULL"
        sql = f"SELECT symbol, qty, {select_px} FROM broker_positions"
        if order_col:
            sql += f" ORDER BY {order_col} ASC"
        rows = con.execute(sql).fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_POSITIONS_READ_FAILED",
            e,
            once_key="execution_positions_read",
        )
        return out, [{"reason": "broker_positions_read_failed", "error": str(e)}]

    eq = _finite_float(equity)
    for row in rows:
        sym = str(row[0] if row else "").strip().upper()
        if not sym:
            continue
        qty = _finite_float(row[1] if len(row) > 1 else None)
        if qty is None:
            errors.append({"symbol": sym, "reason": "invalid_position_qty"})
            continue
        if abs(float(qty)) <= 1e-12:
            out.pop(sym, None)
            continue
        if eq is None or eq <= 0.0:
            errors.append({"symbol": sym, "reason": "missing_equity_for_position"})
            continue
        px = _finite_float(row[2] if len(row) > 2 else None)
        if px is None or px <= 0.0:
            px = _latest_price(con, sym)
        if px is None or px <= 0.0:
            errors.append({"symbol": sym, "reason": "missing_position_price"})
            continue
        out[sym] = float(qty) * float(px) / float(eq)
    return out, errors


_PENDING_ORDER_STATES = frozenset(
    {
        "PENDING",
        "NEW",
        "OPEN",
        "SUBMITTED",
        "ACCEPTED",
        "PARTIALLY_FILLED",
        "HELD",
    }
)


def _read_pending_order_weights(
    con: Any,
    equity: Optional[float],
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    out: Dict[str, float] = {}
    errors: List[Dict[str, Any]] = []
    try:
        if not table_exists(con, "broker_order_state"):
            return out, errors
        cols = _table_columns(con, "broker_order_state")
        if not {"symbol", "state"}.issubset(cols):
            return out, errors
        order_col = "updated_ts_ms" if "updated_ts_ms" in cols else ("created_ts_ms" if "created_ts_ms" in cols else "")
        meta_col = "meta_json" if "meta_json" in cols else "NULL"
        sql = f"SELECT symbol, state, {meta_col} FROM broker_order_state"
        if order_col:
            sql += f" ORDER BY {order_col} ASC"
        rows = con.execute(sql).fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_PENDING_ORDERS_READ_FAILED",
            e,
            once_key="execution_pending_orders_read",
        )
        return out, [{"reason": "pending_orders_read_failed", "error": str(e)}]

    for row in rows:
        sym = str(row[0] if row else "").strip().upper()
        state = str(row[1] if len(row) > 1 else "").strip().upper()
        if (not sym) or state not in _PENDING_ORDER_STATES:
            continue
        raw_meta = row[2] if len(row) > 2 else None
        try:
            meta = json.loads(raw_meta) if raw_meta else {}
        except Exception as e:
            errors.append({"symbol": sym, "reason": "invalid_pending_order_meta", "error": str(e)})
            continue
        if not isinstance(meta, dict):
            errors.append({"symbol": sym, "reason": "invalid_pending_order_meta_type"})
            continue
        meta.setdefault("symbol", sym)
        signed, reason = _order_signed_target_weight(con, meta, equity)
        if signed is None:
            errors.append({"symbol": sym, "reason": f"pending_{reason or 'invalid_exposure'}"})
            continue
        out[sym] = float(signed)
    return out, errors


def _portfolio_metrics(weights: Dict[str, float]) -> Tuple[float, float]:
    gross = 0.0
    net = 0.0
    for value in dict(weights or {}).values():
        parsed = _finite_float(value)
        if parsed is None:
            continue
        gross += abs(float(parsed))
        net += float(parsed)
    return float(gross), float(net)


def _max_cap_scale(
    *,
    fixed_gross: float,
    fixed_net: float,
    mutable_gross: float,
    mutable_net: float,
    gross_cap: float,
    net_cap: float,
) -> Optional[float]:
    low = 0.0
    high = 1.0

    if gross_cap > 0.0:
        if fixed_gross > gross_cap + 1e-12 and mutable_gross <= 1e-12:
            return None
        if mutable_gross > 1e-12:
            high = min(high, (float(gross_cap) - float(fixed_gross)) / float(mutable_gross))

    if net_cap > 0.0:
        if abs(float(mutable_net)) <= 1e-12:
            if abs(float(fixed_net)) > float(net_cap) + 1e-12:
                return None
        else:
            a = (-float(net_cap) - float(fixed_net)) / float(mutable_net)
            b = (float(net_cap) - float(fixed_net)) / float(mutable_net)
            lo_net = min(float(a), float(b))
            hi_net = max(float(a), float(b))
            low = max(low, lo_net)
            high = min(high, hi_net)

    if high < low - 1e-12:
        return None
    scale = min(1.0, max(0.0, float(high)))
    if scale < low - 1e-12:
        return None
    return float(scale)


def _apply_execution_exposure_caps(
    con: Any,
    orders: List[Dict[str, Any]],
    *,
    equity_usd: Optional[float],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    gross_cap = _env_float(
        "EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP",
        "PORTFOLIO_RISK_MAX_GROSS",
        "PORTFOLIO_GROSS_CAP",
        default=1.0,
    )
    net_cap = _env_float(
        "EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP",
        "PORTFOLIO_RISK_MAX_NET",
        "PORTFOLIO_MAX_NET_EXPOSURE",
        default=0.60,
    )

    info: Dict[str, Any] = {
        "enabled": True,
        "gross_cap": float(gross_cap),
        "net_cap": float(net_cap),
        "scaled": False,
        "suppressed_n": 0,
    }
    if gross_cap <= 0.0 and net_cap <= 0.0:
        info["enabled"] = False
        return list(orders or []), info
    if not orders:
        return [], info

    equity, equity_error = _read_execution_equity(con, equity_usd)
    if equity_error:
        info["equity_error"] = str(equity_error)

    existing, existing_errors = _read_existing_position_weights(con, equity)
    pending, pending_errors = _read_pending_order_weights(con, equity)
    exposure_errors: List[Dict[str, Any]] = list(existing_errors or []) + list(pending_errors or [])

    mutable: List[Tuple[int, Dict[str, Any], str, float]] = []
    symbols = set()
    for idx, order in enumerate(list(orders or [])):
        sym = str((order or {}).get("symbol") or "").strip().upper()
        if not sym:
            exposure_errors.append({"index": int(idx), "reason": "missing_symbol"})
            continue
        signed, reason = _order_signed_target_weight(con, order, equity)
        if signed is None:
            exposure_errors.append({"index": int(idx), "symbol": sym, "reason": str(reason or "invalid_exposure")})
            continue
        mutable.append((int(idx), order, sym, float(signed)))
        symbols.add(sym)

    if exposure_errors:
        info.update(
            {
                "ok": False,
                "status": "blocked_invalid_exposure_data",
                "errors": exposure_errors,
            }
        )
        return [], info

    fixed = dict(existing or {})
    fixed.update(pending or {})
    for sym in symbols:
        fixed.pop(str(sym), None)

    mutable_weights: Dict[str, float] = {}
    for _idx, _order, sym, signed in mutable:
        mutable_weights[sym] = float(signed)

    fixed_gross, fixed_net = _portfolio_metrics(fixed)
    mutable_gross, mutable_net = _portfolio_metrics(mutable_weights)
    pre_weights = dict(fixed)
    pre_weights.update(mutable_weights)
    pre_gross, pre_net = _portfolio_metrics(pre_weights)

    scale = _max_cap_scale(
        fixed_gross=float(fixed_gross),
        fixed_net=float(fixed_net),
        mutable_gross=float(mutable_gross),
        mutable_net=float(mutable_net),
        gross_cap=float(gross_cap),
        net_cap=float(net_cap),
    )
    if scale is None:
        info.update(
            {
                "ok": False,
                "status": "blocked_exposure_caps_infeasible",
                "pre_gross": float(pre_gross),
                "pre_net": float(pre_net),
                "fixed_gross": float(fixed_gross),
                "fixed_net": float(fixed_net),
                "mutable_gross": float(mutable_gross),
                "mutable_net": float(mutable_net),
            }
        )
        return [], info

    if pre_gross <= float(gross_cap if gross_cap > 0.0 else pre_gross) + 1e-12 and (
        net_cap <= 0.0 or abs(float(pre_net)) <= float(net_cap) + 1e-12
    ):
        scale = 1.0

    out: List[Dict[str, Any]] = []
    suppressed = 0
    for _idx, order, _sym, signed in mutable:
        scaled_signed = float(signed) * float(scale)
        if abs(float(scaled_signed)) <= 1e-12 and abs(float(signed)) > 1e-12:
            suppressed += 1
            continue
        new_order = dict(order)
        new_side = "LONG" if scaled_signed > 0.0 else ("SHORT" if scaled_signed < 0.0 else "FLAT")
        new_abs = abs(float(scaled_signed))
        from_weight = _finite_float(new_order.get("from_weight"))
        if from_weight is None:
            from_weight = 0.0
        new_order["to_side"] = str(new_side)
        new_order["to_weight"] = float(new_abs)
        new_order["delta_weight"] = float(new_abs) - float(abs(from_weight))
        cap_meta = {
            "gross_cap": float(gross_cap),
            "net_cap": float(net_cap),
            "scale": float(scale),
            "pre_gross": float(pre_gross),
            "pre_net": float(pre_net),
            "fixed_gross": float(fixed_gross),
            "fixed_net": float(fixed_net),
        }
        new_order.setdefault("exposure_cap", cap_meta)
        explain = new_order.get("explain")
        if isinstance(explain, dict):
            explain.setdefault("execution", {})
            if isinstance(explain.get("execution"), dict):
                explain["execution"]["exposure_cap"] = cap_meta
        out.append(new_order)

    post_mutable = {sym: signed * float(scale) for _idx, _order, sym, signed in mutable}
    post_weights = dict(fixed)
    post_weights.update(post_mutable)
    post_gross, post_net = _portfolio_metrics(post_weights)
    info.update(
        {
            "ok": True,
            "status": "exposure_caps_applied",
            "scale": float(scale),
            "scaled": bool(scale < 0.999999),
            "suppressed_n": int(suppressed),
            "pre_gross": float(pre_gross),
            "pre_net": float(pre_net),
            "post_gross": float(post_gross),
            "post_net": float(post_net),
            "fixed_gross": float(fixed_gross),
            "fixed_net": float(fixed_net),
            "mutable_gross": float(mutable_gross),
            "mutable_net": float(mutable_net),
        }
    )
    return out, info


def _turnover(desired: Dict[str, Dict[str, Any]], state: Dict[str, Dict[str, Any]]) -> float:
    syms = set()
    for s in (desired or {}).keys():
        syms.add(str(s))
    for s in (state or {}).keys():
        syms.add(str(s))

    tot = 0.0
    for sym in syms:
        cur = dict((state or {}).get(sym) or {})
        tgt = dict((desired or {}).get(sym) or {})
        cur_w = abs(_cur_signed_weight(cur))
        tgt_w = abs(_tgt_signed_weight(tgt))
        tot += abs(float(tgt_w) - float(cur_w))
    return float(tot)


def _portfolio_weather_risk(desired: Dict[str, Dict[str, Any]], now_ms: int) -> Dict[str, float]:
    """
    Portfolio-level weather summary computed from per-symbol weather snapshots.

    Returns:
      storm_risk_max: max storm risk across evaluated symbols
      storm_risk_w:   weight-weighted average storm risk (abs weights)
      spread_7d_w:    weight-weighted avg forecast spread
      n_eval:         number of symbols evaluated

    Bounded cost: only evaluates top WX_MAX_SYMBOLS by abs(target weight).
    """
    if not USE_WX_RISK:
        return {"storm_risk_max": 0.0, "storm_risk_w": 0.0, "spread_7d_w": 0.0, "n_eval": 0.0}

    # choose top-N by abs weight (stable + bounded)
    items = []
    for sym, row in (desired or {}).items():
        try:
            w = abs(float((row or {}).get("weight", 0.0) or 0.0))
            if w > 0.0:
                items.append((str(sym), float(w)))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_TOP_WEIGHTS_BUILD_FAILED", e, once_key=f"top_weights:{sym}", symbol=str(sym))
    items.sort(key=lambda t: t[1], reverse=True)
    if WX_MAX_SYMBOLS > 0:
        items = items[: int(WX_MAX_SYMBOLS)]

    denom = sum(w for _, w in items) if items else 0.0
    if denom <= 1e-12:
        return {"storm_risk_max": 0.0, "storm_risk_w": 0.0, "spread_7d_w": 0.0, "n_eval": 0.0}

    storm_max = 0.0
    storm_w = 0.0
    spread_w = 0.0
    n_eval = 0

    for sym, w in items:
        try:
            wx = get_weather_feature_snapshot(symbol=str(sym), ts_ms=int(now_ms)) or {}
            sr = float(wx.get("storm_risk", 0.0) or 0.0)
            sp = float(wx.get("spread_7d", 0.0) or 0.0)

            storm_max = max(storm_max, sr)
            storm_w += float(w) * sr
            spread_w += float(w) * sp
            n_eval += 1
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_GATE_WEATHER_RISK_PARSE_FAILED",
                e,
                once_key=f"weather_risk:{sym}",
                symbol=str(sym),
            )
            continue

    return {
        "storm_risk_max": float(storm_max),
        "storm_risk_w": float(storm_w / denom) if denom > 1e-12 else 0.0,
        "spread_7d_w": float(spread_w / denom) if denom > 1e-12 else 0.0,
        "n_eval": float(n_eval),
    }


def _annotate(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> None:
    for sym in list((desired or {}).keys()):
        try:
            desired[sym].setdefault("reason", {})
            if not isinstance(desired[sym]["reason"], dict):
                desired[sym]["reason"] = {"raw": desired[sym]["reason"]}
            desired[sym]["reason"]["risk_gate"] = dict(info)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_ANNOTATE_FAILED", e, once_key=f"annotate:{sym}", symbol=str(sym))


def _hold_current_targets(
    desired: Dict[str, Dict[str, Any]],
    state: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for sym, row in (state or {}).items():
        try:
            out[str(sym)] = dict(row or {})
            out[str(sym)].setdefault("side", str((row or {}).get("side") or "FLAT"))
            out[str(sym)]["weight"] = abs(float((row or {}).get("weight", 0.0) or 0.0))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_HOLD_CURRENT_TARGET_FAILED", e, once_key=f"hold_current:{sym}", symbol=str(sym))
    for sym, row in (desired or {}).items():
        if str(sym) in out:
            continue
        try:
            flat = dict(row or {})
            flat["side"] = "FLAT"
            flat["weight"] = 0.0
            out[str(sym)] = flat
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_GATE_FLAT_NEW_TARGET_FAILED", e, once_key=f"flat_new:{sym}", symbol=str(sym))
    return out


def apply_portfolio_risk_gate(
    con,
    desired: Dict[str, Dict[str, Any]],
    state: Dict[str, Dict[str, Any]],
    now_ms: int,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (desired_clamped, gate_info)
    """
    if not USE:
        return desired, {"enabled": False}

    out = dict(desired or {})
    info: Dict[str, Any] = {"enabled": True}

    # drawdown snapshot
    diagnostic = evaluate_current_drawdown(con)
    info["drawdown_state"] = diagnostic.to_dict()
    if not diagnostic.ok:
        info["drawdown"] = None
        info["blocked"] = True
        info["block_reason"] = {
            "type": "drawdown_state_unavailable",
            "reason_code": str(diagnostic.reason_code),
        }
        out = _hold_current_targets(out, state or {})
        _annotate(out, info)
        return out, info

    dd = float(diagnostic.drawdown or 0.0)
    info["drawdown"] = float(dd)

    # drawdown-based gross cap
    eff_gross_cap = float(GROSS_CAP)
    if dd >= float(DD_ADD_BLOCK):
        eff_gross_cap = float(GROSS_CAP) * float(DD_GROSS_MULT)

    # ------            -- ------------------------------------------------------
    # Optional: weather-based clamps (portfolio-level)
    # ------            -- ------------------------------------------------------
    wx = {"storm_risk_max": 0.0, "storm_risk_w": 0.0, "spread_7d_w": 0.0, "n_eval": 0.0}
    try:
        if USE_WX_RISK:
            wx = _portfolio_weather_risk(out, int(now_ms)) or wx
    except Exception:
        wx = wx

    info["wx_storm_risk_max"] = float(wx.get("storm_risk_max", 0.0) or 0.0)
    info["wx_storm_risk_w"] = float(wx.get("storm_risk_w", 0.0) or 0.0)
    info["wx_spread_7d_w"] = float(wx.get("spread_7d_w", 0.0) or 0.0)
    info["wx_n_eval"] = int(wx.get("n_eval", 0.0) or 0.0)

    # If storm risk is high, apply additional gross cap multiplier (fail-soft)
    if float(info["wx_storm_risk_max"]) >= float(WX_STORM_ADD_BLOCK):
        eff_gross_cap = min(float(eff_gross_cap), float(GROSS_CAP) * float(WX_STORM_GROSS_MULT))
        info["wx_gross_mult_applied"] = float(WX_STORM_GROSS_MULT)

    info["gross_cap"] = float(GROSS_CAP)
    info["eff_gross_cap"] = float(eff_gross_cap)

    # Enforce drawdown add-block: do not allow increasing gross exposure vs current state
    cur_gross = 0.0
    try:
        for _sym, cur in (state or {}).items():
            cur_gross += abs(_cur_signed_weight(cur))
    except Exception:
        cur_gross = 0.0
    info["cur_gross"] = float(cur_gross)

    tgt_gross = _gross(out)
    info["tgt_gross_pre"] = float(tgt_gross)

    wx_block = (
        (float(info.get("wx_storm_risk_max", 0.0)) >= float(WX_STORM_ADD_BLOCK)) if USE_WX_RISK else False
    )

    if (dd >= float(DD_ADD_BLOCK) or wx_block) and tgt_gross > cur_gross + 1e-12:
        # scale DOWN targets so gross <= current gross
        if tgt_gross > 1e-12:
            scale = float(cur_gross) / float(tgt_gross)
            for sym in list(out.keys()):
                try:
                    out[sym]["weight"] = float(out[sym].get("weight", 0.0) or 0.0) * float(scale)
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_RISK_GATE_DD_SCALE_FAILED", e, once_key=f"dd_scale:{sym}", symbol=str(sym))
            if dd >= float(DD_ADD_BLOCK):
                info["dd_add_block"] = True
                info["dd_add_scale"] = float(scale)
            if wx_block:
                info["wx_add_block"] = True
                info["wx_add_scale"] = float(scale)
        else:
            if dd >= float(DD_ADD_BLOCK):
                info["dd_add_block"] = True
                info["dd_add_scale"] = 0.0
            if wx_block:
                info["wx_add_block"] = True
                info["wx_add_scale"] = 0.0

    # Enforce effective gross cap (post dd scaling)
    tgt_gross2 = _gross(out)
    info["tgt_gross_post_dd"] = float(tgt_gross2)
    if tgt_gross2 > float(eff_gross_cap) and tgt_gross2 > 1e-12:
        scale = float(eff_gross_cap) / float(tgt_gross2)
        for sym in list(out.keys()):
            try:
                out[sym]["weight"] = float(out[sym].get("weight", 0.0) or 0.0) * float(scale)
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_GATE_GROSS_CAP_SCALE_FAILED", e, once_key=f"gross_cap_scale:{sym}", symbol=str(sym))
        info["gross_scale"] = float(scale)

    # Hard sleeve caps (asset-class sleeves) BEFORE net/turnover caps
    try:
        _apply_sleeve_caps(out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_GATE_APPLY_SLEEVE_CAPS_FAILED", e, once_key="apply_sleeve_caps")

    # Enforce max net exposure by scaling the overweight side only
    net = _net(out)
    info["net_pre"] = float(net)
    info["max_net"] = float(MAX_NET)

    if float(MAX_NET) > 0.0 and abs(net) > float(MAX_NET) + 1e-12:
        # If net too long -> scale LONG weights down
        # If net too short -> scale SHORT weights down
        if net > 0:
            side_to_scale = "LONG"
            denom = 0.0
            for _sym, tgt in out.items():
                if str(tgt.get("side", "FLAT")).upper() == "LONG":
                    denom += float(tgt.get("weight", 0.0) or 0.0)
            if denom > 1e-12:
                target_long_sum = denom - (abs(net) - float(MAX_NET))
                scale = max(0.0, float(target_long_sum) / float(denom))
                for _sym, tgt in out.items():
                    if str(tgt.get("side", "FLAT")).upper() == "LONG":
                        tgt["weight"] = float(tgt.get("weight", 0.0) or 0.0) * float(scale)
                info["net_scale_side"] = side_to_scale
                info["net_scale"] = float(scale)
        else:
            side_to_scale = "SHORT"
            denom = 0.0
            for _sym, tgt in out.items():
                if str(tgt.get("side", "FLAT")).upper() == "SHORT":
                    denom += float(tgt.get("weight", 0.0) or 0.0)
            if denom > 1e-12:
                target_short_sum = denom - (abs(net) - float(MAX_NET))
                scale = max(0.0, float(target_short_sum) / float(denom))
                for _sym, tgt in out.items():
                    if str(tgt.get("side", "FLAT")).upper() == "SHORT":
                        tgt["weight"] = float(tgt.get("weight", 0.0) or 0.0) * float(scale)
                info["net_scale_side"] = side_to_scale
                info["net_scale"] = float(scale)

    info["net_post"] = float(_net(out))

    # Enforce turnover cap by scaling *deltas* (keeps direction, reduces churn)
    to = _turnover(out, state or {})
    info["turnover_pre"] = float(to)
    info["max_turnover"] = float(MAX_TURNOVER)

    if float(MAX_TURNOVER) > 0.0 and to > float(MAX_TURNOVER) + 1e-12:
        # Scale targets toward current state: tgt = cur + k*(tgt-cur)
        k = float(MAX_TURNOVER) / float(to) if to > 1e-12 else 0.0
        syms = set()
        for s in (out or {}).keys():
            syms.add(str(s))
        for s in (state or {}).keys():
            syms.add(str(s))

        for sym in syms:
            cur = dict((state or {}).get(sym) or {})
            tgt = (out or {}).get(sym)
            if not tgt:
                continue

            cur_abs = abs(_cur_signed_weight(cur))
            tgt_abs = abs(_tgt_signed_weight(tgt))
            new_abs = float(cur_abs) + float(k) * (float(tgt_abs) - float(cur_abs))
            if new_abs < 1e-12 or str(tgt.get("side", "FLAT")).upper() == "FLAT":
                tgt["side"] = "FLAT"
                tgt["weight"] = 0.0
            else:
                tgt["weight"] = float(max(0.0, new_abs))

        info["turnover_scale_k"] = float(k)

    info["turnover_post"] = float(_turnover(out, state or {}))

    _annotate(out, info)
    return out, info


def apply_execution_risk_governor(
    con,
    orders: List[Dict[str, Any]],
    *,
    broker: str,
    mode: str,
    equity_usd: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], dict]:
    """
    Execution-time risk governor (institutional layer):
    - global pause via risk_state key: execution_pause=1
    - caps per-symbol max abs weight (EXEC_MAX_ABS_WEIGHT)
    - caps per-symbol max abs delta weight (EXEC_MAX_ABS_DELTA_WEIGHT)
    - caps max orders per pass (EXEC_MAX_ORDERS_PER_PASS)
    """
    broker = str(broker or "").strip().lower()
    mode = str(mode or "").strip().lower()

    # global pause switch (fail closed)
    try:
        from engine.runtime.risk_state import get_state

        # portfolio risk engine (if enabled) can hard-block execution
        if str(get_state("portfolio_risk_block", "0") or "0").strip() == "1":
            details = str(get_state("portfolio_risk_info", "") or "")
            return [], {
                "ok": False,
                "status": "blocked_portfolio_risk",
                "broker": broker,
                "mode": mode,
                "portfolio_risk_info": details,
            }

        if str(get_state("execution_pause", "0") or "0").strip() == "1":
            return [], {"ok": False, "status": "blocked_execution_pause", "broker": broker, "mode": mode}
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GATE_STATE_READ_FAILED",
            e,
            once_key="portfolio_risk_gate_state_read",
            broker=str(broker),
            mode=str(mode),
        )
        return [], {"ok": False, "status": "blocked_risk_state_error", "broker": broker, "mode": mode}

    # caps (env)
    try:
        max_abs_w = float(os.environ.get("EXEC_MAX_ABS_WEIGHT", "0.35"))
        max_abs_dw = float(os.environ.get("EXEC_MAX_ABS_DELTA_WEIGHT", "0.15"))
        max_n = int(os.environ.get("EXEC_MAX_ORDERS_PER_PASS", "50"))
    except Exception:
        max_abs_w, max_abs_dw, max_n = 0.35, 0.15, 50

    out: List[Dict[str, Any]] = []
    dropped = 0

    for o in list(orders or [])[: int(max_n)]:
        if not isinstance(o, dict):
            continue
        sym = str(o.get("symbol") or "").strip()
        if not sym:
            continue

        # weight caps (defense in depth; upstream should already manage this)
        to_w = o.get("to_weight")
        try:
            to_wf = float(to_w) if to_w is not None else 0.0
        except Exception:
            to_wf = 0.0
        if abs(to_wf) > float(max_abs_w):
            dropped += 1
            continue

        # delta-weight cap (if present)
        dw = o.get("delta_weight")
        if dw is not None:
            try:
                dwf = float(dw)
                if abs(dwf) > float(max_abs_dw):
                    dropped += 1
                    continue
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_GATE_DELTA_WEIGHT_PARSE_FAILED", e, once_key=f"delta_weight:{sym}", symbol=str(sym))

        out.append(o)

    out, exposure_info = _apply_execution_exposure_caps(
        con,
        out,
        equity_usd=equity_usd,
    )
    if isinstance(exposure_info, dict) and exposure_info.get("ok") is False:
        return [], {
            "ok": False,
            "status": str(exposure_info.get("status") or "blocked_exposure_caps"),
            "broker": broker,
            "mode": mode,
            "exposure_caps": dict(exposure_info),
        }

    info = {
        "ok": True,
        "status": "governed",
        "broker": broker,
        "mode": mode,
        "in_n": int(len(list(orders or []))),
        "out_n": int(len(out)),
        "dropped_n": int(dropped),
        "exposure_cap_dropped_n": int((exposure_info or {}).get("suppressed_n") or 0),
        "equity_usd": (float(equity_usd) if equity_usd is not None else None),
        "max_abs_weight": float(max_abs_w),
        "max_abs_delta_weight": float(max_abs_dw),
        "max_orders_per_pass": int(max_n),
        "exposure_caps": dict(exposure_info or {}),
    }
    return out, info
"""
FILE: portfolio_risk_gate.py

Applies portfolio-level risk caps and sleeve constraints after desired weights
have been generated. This is the final portfolio sanitation layer before
execution intent generation.
"""
