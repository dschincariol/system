"""
FILE: capital_efficiency.py

Runtime subsystem module for `capital_efficiency`.
"""

# engine/runtime/capital_efficiency.py
import time
from typing import Dict, Any


def _now_ms() -> int:
    return int(time.time() * 1000)


def capital_efficiency_snapshot(strategy_key: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Produces a capital efficiency score + scaling multiplier.
    This is an advisory heuristic, not a hard risk gate; other allocators
    can consume it as one input without treating it as authoritative.
    """
    dd = float(metrics.get("drawdown", 0.0) or 0.0)
    sharpe = float(metrics.get("decay_sharpe", 0.0) or 0.0)
    slip = float(metrics.get("slippage_pct", 0.0) or 0.0)

    # Conservative baseline: poor risk-adjusted performance and execution
    # friction reduce the score faster than good Sharpe increases it.
    # That keeps this multiplier defensive by default.
    score = 0.0
    score += max(-2.0, min(2.0, sharpe))
    score -= min(2.0, abs(dd) * 10.0)
    score -= min(2.0, abs(slip) * 5.0)

    # Map score bands into a coarse multiplier so downstream allocators do not
    # overfit to tiny score changes.
    mult = 1.0
    if score < -1.0:
        mult = 0.25
    elif score < 0.0:
        mult = 0.5
    elif score > 1.0:
        mult = 1.25

    return {
        "ok": True,
        "ts_ms": _now_ms(),
        "strategy": strategy_key,
        "score": score,
        "multiplier": mult,
        "inputs": {"drawdown": dd, "decay_sharpe": sharpe, "slippage_pct": slip},
        "reasons": [],
    }
