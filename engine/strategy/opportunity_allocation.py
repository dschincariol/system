"""
FILE: opportunity_allocation.py

Maps signal confidence, regime compatibility, and execution confidence into a
bounded opportunity-weight multiplier. This is a deterministic convex scaling
layer, not a Kelly-style allocator.
"""



def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def opportunity_weight(
    signal_conf: float,
    regime_mult: float,
    exec_conf: float,
    *,
    max_cap: float = 1.0,
    min_cap: float = 0.0,
    convex_power: float = 2.0,
    regime_floor: float = 0.50,
) -> float:
    """
    Convex opportunity scaling:

    base = (signal_conf ^ p) * (exec_conf ^ p)

    final = base * regime_adjustment

    Bounded [min_cap, max_cap]
    """

    try:
        sc = _clamp(float(signal_conf), 0.0, 1.0)
    except Exception:
        sc = 0.0

    try:
        ec = _clamp(float(exec_conf), 0.0, 1.0)
    except Exception:
        ec = 0.0

    try:
        rm = float(regime_mult)
    except Exception:
        rm = 1.0

    # Convex scaling (shrinks mediocre trades aggressively)
    base = (sc ** convex_power) * (ec ** convex_power)

    # Regime compatibility compression
    regime_adj = _clamp(rm, regime_floor, 1.5)

    weight = base * regime_adj

    return _clamp(weight, min_cap, max_cap)
