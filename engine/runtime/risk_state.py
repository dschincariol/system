"""
FILE: risk_state.py

Runtime subsystem module for `risk_state`.
"""

# dev_core/risk_state.py
import os
import time
from typing import Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, run_write_txn
from engine.runtime.state_cache import cache_get, cache_set
from engine.runtime.metrics import emit_gauge
from engine.runtime.tracing import trace_event
LOG = get_logger("engine.runtime.risk_state")


def _cache_key(key: str) -> str:
    key_s = str(key)
    try:
        from engine.runtime.db_guard import resolve_db_path

        db_key = str(resolve_db_path())
    except Exception:
        db_key = str(os.environ.get("DB_PATH", "") or "")
    return f"{db_key}|{key_s}"


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="risk_state_nonfatal",
        code=code,
        message=code,
        error=error,
        level=30,
        component="engine.runtime.risk_state",
        extra=extra or None,
        persist=False,
    )

def _cost_bps_from_trade(trade: dict, px_in: float, px_out: float, side: int) -> dict:
    """
    Step 6: Best-effort execution cost decomposition in bps.
    Returns dict with:
      fees_bps, slippage_bps, spread_bps, total_cost_bps, spread_in
    All fields are floats (>=0 where applicable).
    """
    try:
        pin = float(px_in)
        sgn = float(side) if int(side) != 0 else 1.0
    except Exception as e:
        _warn_nonfatal("RISK_STATE_COST_BREAKDOWN_FAILED", e, px_in=repr(px_in)[:120], px_out=repr(px_out)[:120], side=repr(side)[:120])
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    if pin <= 1e-12:
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    # Fees: accept a few possible keys
    fees_total = 0.0
    try:
        fees_total = float(trade.get("fees_total") or trade.get("fees") or 0.0)
    except Exception:
        fees_total = 0.0

    # Convert fees into bps relative to entry notional (qty is unknown here, so treat px as 1-share notional)
    # If you later add qty, swap this to fees / (abs(qty)*pin).
    fees_bps = 0.0
    try:
        fees_bps = float(fees_total) / float(pin) * 10000.0
        if fees_bps != fees_bps or fees_bps < 0:
            fees_bps = 0.0
    except Exception:
        fees_bps = 0.0

    # Slippage: if trade provides a ref price, compare fill to ref in sign-aware bps.
    slippage_bps = 0.0
    ref_px = None
    try:
        ref_px = trade.get("ref_px")
        if ref_px is None:
            ref_px = trade.get("mid_in")
        if ref_px is not None:
            ref_px = float(ref_px)
    except Exception:
        ref_px = None

    if ref_px is not None and ref_px > 1e-12:
        try:
            # buy worse if fill > ref; sell worse if fill < ref -> sign by side
            slippage_bps = ((float(pin) - float(ref_px)) / float(ref_px)) * 10000.0 * float(sgn)
            # cost should be positive "worse"; flip sign if needed
            slippage_bps = -float(slippage_bps)
            if slippage_bps != slippage_bps:
                slippage_bps = 0.0
        except Exception:
            slippage_bps = 0.0

    # Spread: if trade provides spread_in or bid/ask, compute.
    spread_in = None
    spread_bps = 0.0
    try:
        si = trade.get("spread_in")
        if si is None:
            bid = trade.get("bid_in")
            ask = trade.get("ask_in")
            if bid is not None and ask is not None:
                si = float(ask) - float(bid)
        if si is not None:
            spread_in = float(si)
    except Exception:
        spread_in = None

    if spread_in is not None and pin > 1e-12:
        try:
            spread_bps = float(spread_in) / float(pin) * 10000.0
            if spread_bps != spread_bps or spread_bps < 0:
                spread_bps = 0.0
        except Exception:
            spread_bps = 0.0

    total_cost_bps = float(max(0.0, fees_bps)) + float(max(0.0, slippage_bps)) + float(max(0.0, spread_bps))

    return {
        "fees_bps": float(max(0.0, fees_bps)),
        "slippage_bps": float(max(0.0, slippage_bps)),
        "spread_bps": float(max(0.0, spread_bps)),
        "total_cost_bps": float(max(0.0, total_cost_bps)),
        "spread_in": (float(spread_in) if spread_in is not None else None),
    }


def _now_ms():
    return int(time.time() * 1000)


def _active_write_txn_connection():
    try:
        con = connect(readonly=False)
    except Exception as e:
        _warn_nonfatal("RISK_STATE_ACTIVE_WRITE_TXN_CONNECTION_FAILED", e)
        return None
    return con if bool(getattr(con, "in_transaction", False)) else None


def set_state(key: str, value: str):
    from engine.runtime.state_cache import cache_invalidate_namespace

    init_db()
    ts_ms = _now_ms()
    key_s = str(key)
    cache_key = _cache_key(key_s)
    value_s = str(value)

    def _write(con) -> None:
        con.execute(
            """
            INSERT OR REPLACE INTO risk_state(key, value, updated_ts_ms)
            VALUES (?,?,?)
            """,
            (key_s, value_s, ts_ms),
        )

    active_con = _active_write_txn_connection()
    if active_con is not None:
        _write(active_con)
    else:
        run_write_txn(
            _write,
            table="risk_state",
            operation="set_state",
            context={"key": key_s},
        )

    # risk_state is used as low-latency runtime control data, so writes
    # update cache immediately and invalidate dependent API snapshots.
    cache_set("risk_state", cache_key, value_s, ttl_s=3600.0)
    cache_set("risk_state_row", cache_key, (value_s, int(ts_ms)), ttl_s=3600.0)
    cache_invalidate_namespace("api_read", prefix="execution_stats")
    cache_invalidate_namespace("api_read", prefix="execution_metrics")
    cache_invalidate_namespace("portfolio_snapshot")


def get_state(key: str, default: str = "") -> str:
    init_db()
    key_s = str(key)
    cache_key = _cache_key(key_s)
    cached = cache_get("risk_state", cache_key)
    if cached is not None:
        return str(cached)

    con = connect()
    try:
        r = con.execute(
            "SELECT value FROM risk_state WHERE key=?",
            (key_s,),
        ).fetchone()
        value = str(r[0]) if r else str(default)
        cache_set("risk_state", cache_key, value, ttl_s=3600.0)
        return value
    finally:
        con.close()


def get_state_row(key: str, default: str = "") -> Tuple[str, int]:
    """
    Returns (value, updated_ts_ms).
    Callers that need freshness checks should prefer this over get_state().
    """
    init_db()
    key_s = str(key)
    cache_key = _cache_key(key_s)
    cached = cache_get("risk_state_row", cache_key)
    if cached is not None:
        try:
            return str(cached[0]), int(cached[1] or 0)
        except Exception as e:
            _warn_nonfatal("RISK_STATE_CACHE_ROW_PARSE_FAILED", e, key=key_s)

    con = connect()
    try:
        r = con.execute(
            "SELECT value, updated_ts_ms FROM risk_state WHERE key=?",
            (key_s,),
        ).fetchone()
        if not r:
            out = (str(default), 0)
        else:
            out = (str(r[0]), int(r[1] or 0))

        cache_set("risk_state_row", cache_key, out, ttl_s=3600.0)
        cache_set("risk_state", cache_key, out[0], ttl_s=3600.0)
        return out
    finally:
        con.close()


def evaluate_risk_guards() -> dict:
    """
    Fail-closed runtime risk gate used by trade_pipeline_job.
    This is the shared "can we continue?" decision surface for runtime-level
    risk controls, not the place where those controls are computed.
    """
    try:
        portfolio_risk_block = str(get_state("portfolio_risk_block", "0") or "0").strip()
        execution_pause = str(get_state("execution_pause", "0") or "0").strip()
        capital_mode = str(get_state("capital_mode", "normal") or "normal").strip().lower()

        ok = True
        reasons = []

        if portfolio_risk_block == "1":
            ok = False
            reasons.append("portfolio_risk_block")

        if execution_pause == "1":
            ok = False
            reasons.append("execution_pause")

        if capital_mode not in ("normal", "preserve"):
            ok = False
            reasons.append(f"invalid_capital_mode:{capital_mode}")

        out = {
            "ok": ok,
            "reasons": reasons,
            "portfolio_risk_block": portfolio_risk_block,
            "execution_pause": execution_pause,
            "capital_mode": capital_mode,
        }

        emit_gauge(
            "job_health",
            1.0 if ok else 0.0,
            component="engine.runtime.risk_state",
            extra_tags={"metric_scope": "risk_validation"},
        )

        trace_event(
            "risk_validation",
            component="engine.runtime.risk_state",
            entity_type="risk_guard",
            entity_id="runtime",
            payload=out,
        )

        return out
    except Exception as e:
        _warn_nonfatal("RISK_STATE_GUARD_EVAL_FAILED", e)
        return {
            "ok": False,
            "reasons": [f"risk_guard_exception:{e}"],
        }
