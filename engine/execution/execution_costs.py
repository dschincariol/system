"""
FILE: execution_costs.py

Execution subsystem module for `execution_costs`.
"""

# dev_core/execution_costs.py
import os
from typing import Dict, Optional

DEFAULT_FEES_BPS = float(os.environ.get("EXEC_FEES_BPS", "0.5"))          # commission/fees
DEFAULT_SLIPPAGE_BPS = float(os.environ.get("EXEC_SLIPPAGE_BPS", "2.0"))  # impact/queue/slip
DEFAULT_SPREAD_BPS_CAP = float(os.environ.get("EXEC_SPREAD_BPS_CAP", "30.0"))

def _bps(x: float) -> float:
    return float(x) * 1e4

def estimate_cost_bps(
    *,
    px: float,
    bid: Optional[float],
    ask: Optional[float],
    side: int,
    fees_bps: float = DEFAULT_FEES_BPS,
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS,
    spread_bps_override: Optional[float] = None,
    extra_cost_bps: float = 0.0,
) -> Dict[str, float]:
    """
    Cost model (bps):
    - spread_bps: if bid/ask known => half-spread as expected crossing cost (entry only)
    - slippage_bps: fixed additional penalty (models impact + queue + latency)
    - fees_bps: fixed commission/fees

    Returned:
      spread_bps, slippage_bps, fees_bps, total_cost_bps
    """
    # This is an intentionally simple ex-ante cost model used for policy and
    # explainability, not a substitute for realized post-trade attribution.
    spread_bps = 0.0
    if spread_bps_override is not None:
        try:
            spread_bps = max(0.0, float(spread_bps_override))
        except Exception:
            spread_bps = 0.0
    elif bid is not None and ask is not None:
        try:
            spr = max(0.0, float(ask) - float(bid))
            if px > 0:
                # expected crossing cost approx half spread (entry)
                spread_bps = min(DEFAULT_SPREAD_BPS_CAP, _bps(0.5 * spr / float(px)))
        except Exception:
            spread_bps = 0.0

    extra_bps = max(0.0, float(extra_cost_bps or 0.0))
    total = float(fees_bps) + float(slippage_bps) + float(spread_bps) + float(extra_bps)
    return {
        "spread_bps": float(spread_bps),
        "slippage_bps": float(slippage_bps),
        "fees_bps": float(fees_bps),
        "extra_cost_bps": float(extra_bps),
        "total_cost_bps": float(total),
    }

def apply_cost_to_return(gross_ret: float, total_cost_bps: float, side: int) -> float:
    """
    Convert gross return to net by subtracting costs in direction-of-trade terms.
    Costs always reduce P&L, so subtract absolute bps regardless of side.
    """
    # returns are in decimal (e.g., 0.001 = 10 bps)
    cost = float(total_cost_bps) / 1e4
    return float(gross_ret) - cost
