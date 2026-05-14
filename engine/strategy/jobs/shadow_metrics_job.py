"""
FILE: shadow_metrics_job.py

Computes shadow-model performance metrics over a recent window.
"""

import json
import time
import math
import logging
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

WINDOW_MS = 6 * 60 * 60 * 1000  # 6h
LOG = get_logger("strategy.jobs.shadow_metrics_job")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_shadow_metrics_job_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.shadow_metrics_job",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _cost_bps_from_trade(trade: dict, px_in: float, px_out: float, side: int) -> dict:
    """
    Best-effort execution cost decomposition in bps.
    Returns dict with:
      fees_bps, slippage_bps, spread_bps, total_cost_bps, spread_in
    All fields are floats (>=0 where applicable).
    """
    try:
        pin = float(px_in)
        sgn = float(side) if int(side) != 0 else 1.0
    except Exception as e:
        _warn_nonfatal("SHADOW_METRICS_JOB_COST_PARSE_FAILED", e, once_key="cost_parse")
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

def run():
    con = connect()
    try:
        end_ms = _now_ms()
        start_ms = end_ms - WINDOW_MS

        rows = con.execute(
            """
            SELECT p.symbol, p.regime, p.horizon_s, p.model_name,
                   p.predicted_z,
                   p.net_pred_z,
                   CASE
                     WHEN le.realized = 1 THEN le.net_z
                     ELSE COALESCE(le.net_z, l.impact_z)
                   END AS realized_z
            FROM shadow_predictions p
            JOIN labels l
              ON l.event_id = p.event_id
             AND l.symbol = p.symbol
             AND l.horizon_s = p.horizon_s
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
            WHERE p.ts_ms BETWEEN ? AND ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            """,
            (start_ms, end_ms),
        ).fetchall()

        by_key = {}
        for sym, reg, h, m, pz, npz, rz in rows:
            k = (reg, h, m)
            by_key.setdefault(k, []).append(
                (float(pz), float(npz) if npz is not None else None, float(rz))
            )

        for (reg, h, m), vals in by_key.items():
            n = len(vals)
            if n < 5:
                continue

            se = 0.0
            ne = 0.0
            ae = 0.0
            da = 0
            cntn = 0

            drawdown_contrib = 0.0
            gross_alpha = 0.0
            slippage_cost = 0.0

            for pz, npz, rz in vals:
                e = pz - rz
                se += e * e
                ae += abs(e)

                if (pz >= 0) == (rz >= 0):
                    da += 1

                if npz is not None:
                    ne += (npz - rz) ** 2
                    cntn += 1

                # negative realized outcomes contribute to drawdown
                if rz < 0:
                    drawdown_contrib += abs(rz)

                gross_alpha += rz

                # net_pred_z already includes execution costs if present
                if npz is not None:
                    slippage_cost += (pz - npz)

            rmse = math.sqrt(se / n)
            mae = ae / n
            dir_acc = da / n
            net_rmse = math.sqrt(ne / cntn) if cntn else None

            avg_slippage = slippage_cost / n if n else 0.0

            capital_efficiency = (
                gross_alpha / drawdown_contrib
                if drawdown_contrib > 1e-9
                else gross_alpha
            )

            extra = {
                "drawdown_contribution": float(drawdown_contrib),
                "avg_slippage_impact": float(avg_slippage),
                "capital_efficiency": float(capital_efficiency),
            }

            con.execute(
                """
                INSERT INTO shadow_metrics
                  (window_start_ms, window_end_ms, regime, model_name,
                   horizon_s, rmse, mae, dir_acc, avg_cost, net_rmse, n, extra_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    start_ms,
                    end_ms,
                    reg,
                    m,
                    h,
                    rmse,
                    mae,
                    dir_acc,
                    float(avg_slippage),
                    net_rmse,
                    n,
                    json.dumps(extra, separators=(",", ":"), sort_keys=True),
                ),
            )

        con.commit()
    finally:
        con.close()

if __name__ == "__main__":
    run()
"""
FILE: shadow_metrics_job.py

Job entrypoint wrapper for shadow metrics computation.
"""
