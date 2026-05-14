"""
FILE: compute_exec_labels_from_fills.py

Operational helper script for `compute_exec_labels_from_fills`.
"""

# compute_exec_labels_from_fills.py
"""
Phase 5.1: Override execution labels using real broker fills.
"""

import json
import logging
import os
import time
from typing import Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)

from engine.execution.broker_fill_utils import get_realized_trade
from engine.strategy.alpha_lifecycle_engine import compute_alpha_decay_metrics
from engine.strategy.regime_compat import update_regime_compat
from engine.strategy.model_v2 import get_current_regime

# -----------------------------
# Step 7: Production job safety
# -----------------------------
JOB_NAME = "compute_exec_labels_from_fills"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

MAX_BATCH = int(os.environ.get("LABELS_EXEC_MAX_BATCH", "10000"))
COMMIT_EVERY = int(os.environ.get("LABELS_EXEC_COMMIT_EVERY", "250"))
LOG = get_logger("ops.compute_exec_labels_from_fills")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="ops.compute_exec_labels_from_fills",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _latest_price(con, symbol: str):
    """
    Best-effort mark-to-market price.
    Returns: (ts_ms, price) or None
    """
    try:
        row = con.execute(
            """
            SELECT ts_ms, price
            FROM prices
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
        if not row:
            return None
        return int(row[0]), float(row[1])
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_LATEST_PRICE_FAILED", e, once_key=f"latest_price:{symbol}", symbol=str(symbol))
        return None


def _price_path(con, symbol: str, entry_ts_ms: int, exit_ts_ms: int, max_points: int = 512):
    """
    Returns list[(ts_ms, price)] ascending between [entry_ts_ms, exit_ts_ms].
    Downsamples if too many points.
    """
    try:
        rows = con.execute(
            """
            SELECT ts_ms, price
            FROM prices
            WHERE symbol=?
              AND ts_ms BETWEEN ? AND ?
            ORDER BY ts_ms ASC
            """,
            (str(symbol), int(entry_ts_ms), int(exit_ts_ms)),
        ).fetchall() or []
        out = [(int(ts), float(px)) for ts, px in rows if px is not None]
        if not out:
            return []
        if len(out) <= int(max_points):
            return out
        step = max(1, int(len(out) / int(max_points)))
        return out[::step]
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_PRICE_PATH_FAILED", e, once_key=f"price_path:{symbol}", symbol=str(symbol))
        return []



import math


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
        _warn_nonfatal("COMPUTE_EXEC_LABELS_COST_INPUT_PARSE_FAILED", e, once_key="cost_input_parse")
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    if pin <= 1e-12:
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    # Fill schemas differ by broker adapter, so this function is intentionally
    # tolerant and computes a conservative decomposition from whatever is present.
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


def _rolling_exec_z(
    con,
    symbol: str,
    horizon_s: int,
    new_ret: float,
    lookback: int = 500,
    exclude_event_id: Optional[int] = None,
    col: str = "net_ret",  # 'net_ret' or 'gross_ret'
) -> float:
    """
    Rolling z-score over realized execution returns.

    Step 5B/6C:
    - Excludes current event_id to avoid rerun double-counting.
    - Uses realized=1 rows only (prevents M2M rows from polluting z).
    - Supports z-scoring either net_ret or gross_ret via `col`.
    """
    try:
        symbol_s = str(symbol)
        h = int(horizon_s)
        lb = int(lookback)
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_ROLLING_Z_INPUT_PARSE_FAILED", e, once_key="rolling_exec_z_input_parse")
        return 0.0

    c = str(col or "net_ret").strip()
    if c not in ("net_ret", "gross_ret"):
        c = "net_ret"

    try:
        if exclude_event_id is None:
            rows = con.execute(
                f"""
                SELECT {c}
                FROM labels_exec
                WHERE symbol=?
                  AND horizon_s=?
                  AND realized=1
                  AND {c} IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (symbol_s, h, lb),
            ).fetchall()
        else:
            rows = con.execute(
                f"""
                SELECT {c}
                FROM labels_exec
                WHERE symbol=?
                  AND horizon_s=?
                  AND realized=1
                  AND {c} IS NOT NULL
                  AND event_id != ?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (symbol_s, h, int(exclude_event_id), lb),
            ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "COMPUTE_EXEC_LABELS_ROLLING_Z_QUERY_FAILED",
            e,
            once_key=f"rolling_exec_z_query:{symbol_s}:{h}:{c}",
            symbol=str(symbol_s),
            horizon_s=int(h),
            column=str(c),
        )
        rows = []

    rets = []
    for (r,) in rows or []:
        try:
            rets.append(float(r))
        except Exception as e:
            _warn_nonfatal(
                "COMPUTE_EXEC_LABELS_RET_PARSE_FAILED",
                e,
                once_key="rolling_exec_z_ret_parse",
                symbol=symbol_s,
                horizon_s=h,
                column=c,
            )

    if len(rets) < 20:
        return 0.0

    m = sum(rets) / len(rets)
    v = sum((x - m) ** 2 for x in rets) / max(1, len(rets) - 1)
    sd = math.sqrt(max(v, 1e-12))
    z = (float(new_ret) - m) / sd

    if z != z:
        return 0.0

    return float(max(-8.0, min(8.0, z)))

def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    # Step 7: lock so only one instance runs (prevents double work / contention)
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        print("[labels_exec:fills] lock held by another instance; exiting")
        raise SystemExit(2)

    last_hb_s = 0.0

    con = connect()
    try:
        # Fail-soft if labels table isn't created yet
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal("COMPUTE_EXEC_LABELS_LABELS_TABLE_CHECK_FAILED", e, once_key="labels_table_check")
            return 0

        rows = con.execute(
            """
            SELECT p.event_id, p.symbol, p.horizon_s, p.ts_ms
            FROM predictions p
            JOIN broker_orders bo
              ON bo.symbol=p.symbol AND bo.ts_ms >= p.ts_ms
            LEFT JOIN labels_exec le
              ON le.event_id=p.event_id
             AND le.symbol=p.symbol
             AND le.horizon_s=p.horizon_s
            WHERE le.realized IS NULL
            ORDER BY p.ts_ms ASC
            LIMIT ?
            """,
            (int(max(1, MAX_BATCH)),),
        ).fetchall()

        n_used = 0
        n_skip = 0
        n_err = 0

        for eid, sym, horizon_s, ts_ms in rows:
            # heartbeat / lock touch
            now_s = time.time()
            if now_s - last_hb_s >= HEARTBEAT_EVERY_S:
                try:
                    touch_job_lock(JOB_NAME, OWNER, PID)
                    put_job_heartbeat(
                        JOB_NAME,
                        OWNER,
                        PID,
                        extra_json=json.dumps(
                            {"event_id": int(eid), "symbol": str(sym), "horizon_s": int(horizon_s), "ts_ms": int(ts_ms)},
                            separators=(",", ":"),
                        ),
                    )

                except Exception as e:
                    _warn_nonfatal(
                        "COMPUTE_EXEC_LABELS_HEARTBEAT_FAILED",
                        e,
                        once_key="job_heartbeat",
                        job_name=JOB_NAME,
                        event_id=int(eid),
                        symbol=str(sym),
                        horizon_s=int(horizon_s),
                    )
                last_hb_s = now_s

            try:
                exit_ts = int(ts_ms) + int(horizon_s) * 1000

                trade = get_realized_trade(
                    symbol=str(sym),
                    entry_ts_ms=int(ts_ms),
                    exit_ts_ms=int(exit_ts),
                )

                if not trade:
                    n_skip += 1
                    continue

                side = trade["side"]
                px_in = trade["px_in"]
                px_out = trade["px_out"]

                realized = 1
                px_out_use = px_out
                m2m_ctx = None

                if px_out is None:
                    realized = 0
                    lp = _latest_price(con, str(sym))
                    if not lp:
                        n_skip += 1
                        continue
                    m2m_ts_ms, m2m_px = lp
                    if m2m_px <= 0:
                        n_skip += 1
                        continue
                    px_out_use = float(m2m_px)
                    m2m_ctx = {
                        "m2m_ts_ms": int(m2m_ts_ms),
                        "exit_target_ts_ms": int(exit_ts),
                    }

                if float(px_in) <= 1e-12:
                    n_skip += 1
                    continue
                gross_ret = (float(px_out_use) / float(px_in) - 1.0) * float(side)
                # Convert total_cost_bps into return units for net_ret consistency
                cost_bps = float(_cost_bps_from_trade(trade, float(px_in), float(px_out_use), int(side)).get("total_cost_bps") or 0.0)
                net_ret = float(gross_ret) - (float(cost_bps) / 10000.0)

                # alpha decay metrics (per event_id/symbol/horizon_s)
                try:

                    exit_use_ts = int(exit_ts)
                    if px_out is None and m2m_ctx and "m2m_ts_ms" in m2m_ctx:
                        exit_use_ts = int(m2m_ctx["m2m_ts_ms"])

                    path = _price_path(con, str(sym), int(ts_ms), int(exit_use_ts))
                    decay = compute_alpha_decay_metrics(
                        signal_ts_ms=int(ts_ms),
                        entry_ts_ms=int(ts_ms),
                        exit_ts_ms=int(exit_use_ts),
                        prices=path,
                        side=("long" if int(side) > 0 else "short"),
                        ttl_ms=int(horizon_s) * 1000,
                    )
                    decay["_meta"] = {
                        "event_id": int(eid),
                        "symbol": str(sym),
                        "horizon_s": int(horizon_s),
                        "ts_ms": int(ts_ms),
                        "realized": int(realized),
                        "side": int(side),
                    }

                    con.execute(
                        "INSERT OR REPLACE INTO alpha_decay_metrics VALUES (?,?,?,?,?)",
                        (int(eid), str(sym), int(horizon_s), int(ts_ms), json.dumps(decay, separators=(",", ":"), sort_keys=True)),
                    )
                except Exception as e:
                    _warn_nonfatal(
                        "COMPUTE_EXEC_LABELS_ALPHA_DECAY_WRITE_FAILED",
                        e,
                        once_key="alpha_decay_write",
                        event_id=int(eid),
                        symbol=str(sym),
                        horizon_s=int(horizon_s),
                    )
                net_z = _rolling_exec_z(
                    con, str(sym), int(horizon_s), float(net_ret),
                    exclude_event_id=int(eid),
                    col="net_ret",
                )
                gross_z = _rolling_exec_z(
                    con, str(sym), int(horizon_s), float(gross_ret),
                    exclude_event_id=int(eid),
                    col="gross_ret",
                )

                cost = _cost_bps_from_trade(
                    trade if isinstance(trade, dict) else {},
                    float(px_in),
                    float(px_out_use),
                    int(side),
                )

                con.execute(
                    """
                    INSERT OR REPLACE INTO labels_exec(
                      event_id, symbol, horizon_s, ts_ms,
                      side, gross_ret, net_ret,
                      gross_z, net_z,
                      mid_in, mid_out, spread_in,
                      fees_bps, slippage_bps, spread_bps, total_cost_bps,
                      source, realized, extra_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(eid),
                        str(sym),
                        int(horizon_s),
                        int(ts_ms),
                        int(side),
                        float(gross_ret),
                        float(net_ret),
                        float(gross_z),
                        float(net_z),
                        float(px_in),
                        float(px_out_use),
                        cost.get("spread_in"),
                        float(cost.get("fees_bps") or 0.0),
                        float(cost.get("slippage_bps") or 0.0),
                        float(cost.get("spread_bps") or 0.0),
                        float(cost.get("total_cost_bps") or 0.0),
                        "broker_fills_v2",
                        int(realized),
                        json.dumps(
                            {
                                "version": "v2",
                                "source": "broker_fills",
                                "computed_at_ts_ms": _now_ms(),
                                "realized": int(realized),
                                "trade": trade,
                                "m2m": m2m_ctx,
                                "exec_stress": (
                                    trade.get("raw", {}).get("exec_stress")
                                    if isinstance(trade, dict)
                                    else None
                                ),
                                "alpha_decay": decay,

                            },

                            separators=(",", ":"),
                        ),
                    ),
                )
                # ----------------------------
                # Regime compatibility update
                # Only update on fully realized trades
                # ----------------------------
                if int(realized) == 1:
                    try:
                        model_name = os.environ.get("MODEL_NAME", "embed_regressor").strip() or "embed_regressor"
                        regime = get_current_regime("SPY") or "MID"

                        update_regime_compat(
                            model_name=str(model_name),
                            regime=str(regime).upper(),
                            net_return=float(net_ret),
                        )
                    except Exception as e:
                        _warn_nonfatal(
                            "COMPUTE_EXEC_LABELS_REGIME_COMPAT_UPDATE_FAILED",
                            e,
                            once_key="regime_compat_update",
                            event_id=int(eid),
                            symbol=str(sym),
                            horizon_s=int(horizon_s),
                            model_name=str(model_name),
                        )

                n_used += 1

                # crash-safe: commit in small chunks so reruns resume naturally
                if n_used % int(max(1, int(COMMIT_EVERY))) == 0:

                    try:
                        con.commit()
                    except Exception as e:
                        _warn_nonfatal(
                            "COMPUTE_EXEC_LABELS_COMMIT_FAILED",
                            e,
                            once_key="chunk_commit",
                            used=int(n_used),
                            skipped=int(n_skip),
                            errors=int(n_err),
                        )

            except Exception as e:
                _warn_nonfatal("COMPUTE_EXEC_LABELS_ROW_PROCESS_FAILED", e, once_key="row_process_failed")
                n_err += 1
                continue

        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal(
                "COMPUTE_EXEC_LABELS_FINAL_COMMIT_FAILED",
                e,
                once_key="final_commit",
                used=int(n_used),
                skipped=int(n_skip),
                errors=int(n_err),
            )

        print(f"[labels_exec:fills] used={n_used} skipped={n_skip} err={n_err}")

    finally:
        try:
            con.close()
        finally:
            try:
                release_job_lock(JOB_NAME, OWNER, PID)
            except Exception as e:
                _warn_nonfatal(
                    "COMPUTE_EXEC_LABELS_RELEASE_LOCK_FAILED",
                    e,
                    once_key="release_lock",
                    job_name=JOB_NAME,
                )

if __name__ == "__main__":
    main()
