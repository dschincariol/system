"""
FILE: corr_opt.py

Provides a deterministic correlation-aware portfolio optimizer. It combines a
quadratic risk term with linear utility and projects the result into bounded
gross-exposure constraints.
"""

import logging
import math
from typing import Dict, Any, List, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.strategy.corr_opt")


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _safe_float(x, d=0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(d)
        return float(v)
    except Exception as e:
        _warn_nonfatal("CORR_OPT_SAFE_FLOAT_FAILED", e, value=repr(x))
        return float(d)


def _sign_from_side(side: str) -> float:
    s = str(side or "FLAT").upper().strip()
    if s == "SHORT":
        return -1.0
    if s == "LONG":
        return 1.0
    return 0.0


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="corr_opt_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.corr_opt",
        extra=extra or None,
        persist=False,
    )


def _project_capped_simplex(y: List[float], cap: float, ub: List[float]) -> List[float]:
    """
    Projection onto {x: 0<=x_i<=ub_i, sum x_i = cap} via bisection on lambda.
    If cap <= 0 => all zeros.
    If sum(ub) <= cap => return ub (can't reach exact cap).
    """
    n = len(y)
    if n == 0:
        return []

    cap = float(cap)
    if cap <= 0:
        return [0.0 for _ in range(n)]

    ub = [max(0.0, float(u)) for u in ub]
    sum_ub = sum(ub)
    if sum_ub <= cap + 1e-12:
        return [float(u) for u in ub]

    # The projection is solved by bisection on the simplex dual variable.
    lo = -1e6
    hi = 1e6

    def _sum_x(lam: float) -> float:
        s = 0.0
        for i in range(n):
            xi = y[i] - lam
            if xi <= 0:
                continue
            if xi >= ub[i]:
                s += ub[i]
            else:
                s += xi
        return float(s)

    # tighten bounds based on y/ub
    ymin = min(y)
    ymax = max(y)
    lo = ymin - max(ub) - abs(cap) - 1.0
    hi = ymax + abs(cap) + 1.0

    for _ in range(80):
        mid = 0.5 * (lo + hi)
        s = _sum_x(mid)
        if s > cap:
            lo = mid
        else:
            hi = mid

    lam = hi
    x = []
    for i in range(n):
        xi = y[i] - lam
        if xi <= 0:
            x.append(0.0)
        elif xi >= ub[i]:
            x.append(float(ub[i]))
        else:
            x.append(float(xi))

    # final tiny renorm if numerically off and feasible
    s = sum(x)
    if s > 1e-12 and abs(s - cap) / max(1e-9, cap) > 1e-6:
        # distribute proportional to slack (within bounds)
        if s > cap:
            # scale down within bounds
            sc = cap / s
            x = [min(ub[i], max(0.0, x[i] * sc)) for i in range(n)]
        else:
            # scale up but respect ub (rare after projection)
            sc = cap / s
            x = [min(ub[i], max(0.0, x[i] * sc)) for i in range(n)]
    return x


def _utility_from_target(tgt: Dict[str, Any]) -> float:
    """
    Prefer alloc_util if present (from _optimize_capital_allocation),
    else fallback to adjusted expected_ret_net / expected_dd when available,
    then raw expected_ret_net / expected_dd from explain_json.
    """
    try:
        r = tgt.get("reason") or {}
        if isinstance(r, dict) and ("alloc_util" in r):
            return _safe_float(r.get("alloc_util", 0.0), 0.0)
    except Exception as e:
        _warn_nonfatal("CORR_OPT_REASON_UTILITY_FAILED", e, symbol=str(tgt.get("symbol") or ""))

    # fallback: parse explain_json
    exj = tgt.get("explain_json", "{}")
    try:
        import json
        o = json.loads(exj or "{}")
    except Exception:
        o = {}

    # expected_ret_net, expected_dd (conservative)
    net = _safe_float((tgt or {}).get("adjusted_expected_ret_net"), float("nan"))
    if not math.isfinite(net):
        reason = tgt.get("reason") or {}
        if isinstance(reason, dict):
            black_litterman = reason.get("black_litterman")
            if isinstance(black_litterman, dict):
                net = _safe_float(black_litterman.get("adjusted_expected_ret_net"), float("nan"))
    if not math.isfinite(net):
        tradability = (o or {}).get("tradability")
        if isinstance(tradability, dict):
            net = _safe_float(tradability.get("expected_ret_net", 0.0), 0.0)
        else:
            net = _safe_float((o or {}).get("expected_ret_net", 0.0), 0.0)
    dd = _safe_float((o or {}).get("expected_dd", 0.0), 0.0)
    dd = _clamp(dd, 0.0, 1.0)

    netp = max(0.0, net)
    u = netp / (dd + 1e-6)
    return float(max(0.0, u))


def _build_covariance(con, syms: List[str], lookback: int) -> Tuple[List[List[float]], List[float]]:
    """
    Σ_ij = corr(i,j) * vol_i * vol_j
    Returns (Sigma, vols)
    """
    from engine.strategy.risk import realized_vol_from_prices, corr_from_prices

    n = len(syms)
    vols = []
    for s in syms:
        v = realized_vol_from_prices(con, s, lookback=int(lookback))
        vols.append(_safe_float(v, 0.0))

    # Flooring vols keeps the covariance matrix numerically usable even when a
    # symbol has nearly flat recent prices.
    vols = [max(1e-6, float(v)) for v in vols]

    Sigma = [[0.0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        Sigma[i][i] = float(vols[i] * vols[i])

    for i in range(n):
        for j in range(i + 1, n):
            c = corr_from_prices(con, syms[i], syms[j], lookback=int(lookback))
            if c is None:
                cc = 0.0
            else:
                cc = _clamp(_safe_float(c, 0.0), -1.0, 1.0)
            cov = float(cc) * float(vols[i]) * float(vols[j])
            Sigma[i][j] = float(cov)
            Sigma[j][i] = float(cov)

    return Sigma, vols


def corr_aware_optimize_desired(
    con,
    desired: Dict[str, Dict[str, Any]],
    *,
    gross_cap: float,
    lookback: int,
    corr_max: float,
    gamma: float = 1.0,
    ridge: float = 1e-6,
    iters: int = 30,
) -> Dict[str, Dict[str, Any]]:
    """
    Convex optimizer with correlation-aware risk penalty.
    Uses projected gradient on capped simplex (with per-symbol caps).
    Also applies a soft correlation clamp: if pairwise |corr| > corr_max, increase risk penalty.

    Returns updated desired with weights adjusted; preserves side.
    """
    if not desired or len(desired) < 2:
        return desired

    syms = []
    side_sign = []
    w0 = []
    ub = []
    util = []

    for sym, tgt in desired.items():
        s = str(sym)
        sign = _sign_from_side((tgt or {}).get("side", "FLAT"))
        if sign == 0.0:
            continue

        w = _safe_float((tgt or {}).get("weight", 0.0), 0.0)
        if w <= 0:
            continue

        # cap already applied upstream, but keep safe
        cap_i = _safe_float((tgt or {}).get("weight_cap", w), w)
        # If not provided, just allow current weight as UB; portfolio gross cap renorm happens later anyway.
        cap_i = max(w, cap_i)

        syms.append(s)
        side_sign.append(float(sign))
        w0.append(float(w))
        ub.append(float(cap_i))
        util.append(float(_utility_from_target(tgt)))

    n = len(syms)
    if n < 2:
        return desired

    # Normalize utility to [0..1] scale for stability
    umax = max(util) if util else 0.0
    if umax <= 1e-12:
        # nothing to optimize (no positive utility)
        return desired
    u = [float(x / umax) for x in util]

    # covariance + correlation soft clamp
    Sigma, vols = _build_covariance(con, syms, lookback=int(lookback))

    # if corr exceeds corr_max, inflate covariance magnitude for that pair (soft constraint)
    from engine.strategy.risk import corr_from_prices
    cm = float(max(0.0, min(0.999, float(corr_max))))
    for i in range(n):
        for j in range(i + 1, n):
            c = corr_from_prices(con, syms[i], syms[j], lookback=int(lookback))
            if c is None:
                continue
            cc = abs(_clamp(_safe_float(c, 0.0), -1.0, 1.0))
            if cc > cm:
                # inflate covariance to discourage simultaneously holding both
                # scale factor grows as correlation exceeds threshold
                t = (cc - cm) / max(1e-9, (1.0 - cm))
                infl = 1.0 + 3.0 * _clamp(t, 0.0, 1.0)  # up to 4x
                Sigma[i][j] *= infl
                Sigma[j][i] *= infl

    # Apply sign to covariance: risk on signed weights
    # A = S Σ S  where S=diag(sign)
    A = [[0.0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        for j in range(n):
            A[i][j] = float(side_sign[i]) * float(Sigma[i][j]) * float(side_sign[j])

    # Ridge for strong convexity / numerical stability
    for i in range(n):
        A[i][i] = float(A[i][i]) + float(ridge)

    # Decide target gross cap for the optimizer:
    cap = float(gross_cap)
    cap = max(0.0, cap)

    # Start point: scale current weights to fit cap
    s0 = sum(w0)
    if s0 <= 1e-12:
        return desired
    x = [float(w) for w in w0]
    if s0 > cap and cap > 1e-12:
        sc = cap / s0
        x = [min(ub[i], max(0.0, x[i] * sc)) for i in range(n)]
        # enforce exact cap if possible
        x = _project_capped_simplex(x, cap, ub)

    # Step size based on diagonal (conservative)
    diag = [max(1e-9, float(A[i][i])) for i in range(n)]
    lr = 0.5 / max(diag)

    g = float(max(0.0, gamma))

    for _ in range(int(iters)):
        # grad = A x - g u
        grad = [0.0 for _ in range(n)]
        for i in range(n):
            s = 0.0
            Ai = A[i]
            for j in range(n):
                s += float(Ai[j]) * float(x[j])
            grad[i] = float(s) - float(g) * float(u[i])

        # gradient step
        y = [float(x[i] - lr * grad[i]) for i in range(n)]

        # project back to feasible set (gross cap + bounds)
        x = _project_capped_simplex(y, cap, ub)

    # Write back into desired (preserve side)
    out = desired
    for i, sym in enumerate(syms):
        if sym not in out:
            continue
        out[sym]["weight"] = float(max(0.0, x[i]))
        out[sym].setdefault("reason", {})
        try:
            out[sym]["reason"]["corr_opt"] = True
            out[sym]["reason"]["corr_opt_u"] = float(u[i])
            out[sym]["reason"]["corr_opt_w0"] = float(w0[i])
            out[sym]["reason"]["corr_opt_w"] = float(x[i])
        except Exception as e:
            _warn_nonfatal("CORR_OPT_REASON_WRITE_FAILED", e, symbol=str(sym), index=int(i))

    return out
