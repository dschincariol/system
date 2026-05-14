"""
FILE: shadow_trainer.py

Trains shadow-stage models from realized labels and registers them in the model
registry. This is the offline path that feeds shadow evaluation before any
promotion to champion.
"""

import json
import time
import logging
from typing import Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect
from engine.model_registry import register_model
from engine.training_guard import training_allowed
from engine.strategy.model_v2 import train_regime_model

LOG = logging.getLogger("shadow_trainer")


def _warn_nonfatal(event: str, error: BaseException, **extra) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.shadow_trainer",
        extra=extra,
        persist=False,
    )

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
        _warn_nonfatal("shadow_trainer_cost_parse_failed", e)
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    if pin <= 1e-12:
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    # Fees: accept a few possible keys
    fees_total = 0.0
    try:
        fees_total = float(trade.get("fees_total") or trade.get("fees") or 0.0)
    except Exception:
        fees_total = 0.0

    # Qty is not available on all historical records, so we normalize as if the
    # fill were for one share. If qty becomes reliable, update this denominator.
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

def train_shadow(
    *,
    model_name: str,
    horizon_s: int,
    regime: Optional[str] = None,
    min_rows: int = 100,
) -> None:
    # Fail closed when global training is disabled; shadow jobs should respect
    # the same operator guard as champion training.
    if not training_allowed():
        return

    con = connect()
    run_id = None
    try:
        rows = con.execute(
            """
            SELECT event_id, symbol, horizon_s, realized_z, vol_proxy, regime
            FROM labels
            WHERE horizon_s=?
              AND realized_z IS NOT NULL
              AND (? IS NULL OR regime=?)
            ORDER BY ts_ms DESC
            """,
            (horizon_s, regime, regime),
        ).fetchall()

        if len(rows) < min_rows:
            return

        run_id = con.execute(
            """
            INSERT INTO shadow_training_runs
              (ts_ms, model_name, regime, horizon_s, train_rows, metrics_json, status)
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                _now_ms(),
                model_name,
                regime,
                horizon_s,
                len(rows),
                "{}",
                "running",
            ),
        ).lastrowid
        con.commit()

        model, metrics = train_regime_model(
            rows=rows,
            horizon_s=horizon_s,
            regime=regime,
            shadow=True,
        )

        register_model(
            model_name=model_name,
            model_kind=str((model or {}).get("model_kind") or "shadow_regime_stats"),
            model_ts_ms=int((model or {}).get("model_ts_ms") or _now_ms()),
            stage="shadow",
            regime=regime,
            metrics=metrics,
            note=json.dumps(
                {
                    "shadow": True,
                    "horizon_s": int(horizon_s),
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )

        con.execute(
            """
            UPDATE shadow_training_runs
            SET status='ok', metrics_json=?
            WHERE id=?
            """,
            (json.dumps(metrics or {}, separators=(",", ":"), sort_keys=True), run_id),
        )
        con.commit()

    except Exception as e:
        if run_id is not None:
            try:
                con.execute(
                    """
                    UPDATE shadow_training_runs
                    SET status='error', error=?
                    WHERE id=?
                    """,
                    (str(e), run_id),
                )
                con.commit()
            except Exception as update_error:
                _warn_nonfatal(
                    "shadow_trainer_error_state_persist_failed",
                    update_error,
                    model_name=str(model_name),
                    run_id=int(run_id),
                )
        raise

    finally:
        con.close()
