"""
FILE: edge_filter.py

Converts estimated execution costs into z-score space and subtracts that from
paper edge. The goal is to surface net trade quality rather than raw model
strength.
"""

import math
import logging
import os
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.strategy.risk import realized_vol_from_prices
from engine.execution.execution_costs import estimate_cost_bps

USE = os.environ.get("ALERT_USE_EXEC_COST_FILTER", "0") == "1"

PRICE_STEP_S = int(os.environ.get("ALERT_PRICE_STEP_S", "60"))

FEES_BPS = float(os.environ.get("ALERT_EXEC_FEES_BPS", os.environ.get("EXEC_FEES_BPS", "0.5")))
SLIPPAGE_BPS = float(os.environ.get("ALERT_EXEC_SLIPPAGE_BPS", os.environ.get("EXEC_SLIPPAGE_BPS", "2.0")))

MIN_NET_ABS_Z = float(os.environ.get("ALERT_MIN_NET_ABS_Z", "0.0"))
LOG = get_logger("strategy.edge_filter")

def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="strategy_edge_filter_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.edge_filter",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _safe_float(x, d=0.0):
    try:
        return float(x)
    except Exception as e:
        _warn_nonfatal(
            "EDGE_FILTER_SAFE_FLOAT_FAILED",
            e,
            value=repr(x)[:120],
        )
        return float(d)


def adjust_expected_z_for_costs(
    *,
    symbol: str,
    horizon_s: int,
    expected_z: float,
    side: int = 1,
) -> Optional[Dict[str, Any]]:
    """
    Returns dict:
      {
        "expected_z_adj": float,
        "cost_z": float,
        "cost_bps": float,
        "vol_step": float,
        "vol_horizon": float,
      }
    Or None if filter disabled / missing vol.

    Notes:
    - We approximate horizon vol as vol_step * sqrt(steps)
      where steps ~ horizon_s / PRICE_STEP_S.
    - We convert bps cost into return space by (bps / 1e4) and
      then into z by dividing by vol_horizon.
    """
    if not USE:
        return None

    sym = str(symbol)
    h = int(horizon_s)
    ez = _safe_float(expected_z, 0.0)

    con = connect()
    try:
        vol_step = realized_vol_from_prices(con, sym)
    finally:
        con.close()

    if vol_step is None:
        return None

    vol_step = _safe_float(vol_step, 0.0)
    if vol_step <= 0:
        return None

    steps = max(1.0, float(h) / max(1.0, float(PRICE_STEP_S)))
    vol_h = vol_step * math.sqrt(steps)

    # This stays intentionally conservative when spread data is unavailable.
    costs = estimate_cost_bps(
        px=1.0,
        bid=None,
        ask=None,
        side=int(side),
        fees_bps=float(FEES_BPS),
        slippage_bps=float(SLIPPAGE_BPS),
    )
    total_bps = _safe_float(costs.get("total_cost_bps", 0.0), 0.0)
    cost_ret = total_bps / 1e4

    # cost in z-units
    cost_z = 0.0
    if vol_h > 1e-12:
        cost_z = float(cost_ret / vol_h)

    ez_adj = float(ez) - float(cost_z)

    if MIN_NET_ABS_Z > 0.0 and abs(ez_adj) < float(MIN_NET_ABS_Z):
        # hard reject by signaling "no edge"
        return {
            "expected_z_adj": float("nan"),
            "cost_z": float(cost_z),
            "cost_bps": float(total_bps),
            "vol_step": float(vol_step),
            "vol_horizon": float(vol_h),
        }

    return {
        "expected_z_adj": float(ez_adj),
        "cost_z": float(cost_z),
        "cost_bps": float(total_bps),
        "vol_step": float(vol_step),
        "vol_horizon": float(vol_h),
    }
