"""Pure normalization helpers for portfolio construction.

The public compatibility surface stays in ``engine.strategy.portfolio``.
These helpers carry the implementation so the oversized facade can delegate
stable, low-risk behavior without changing caller imports.
"""

from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Optional


WarnFn = Callable[..., None]


def normalize_model_id(model_id: Any) -> str:
    mid = str(model_id or "").strip()
    return mid or "baseline"


def clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def safe_float(
    value: Any, default: float = 0.0, *, warn_nonfatal: WarnFn | None = None
) -> float:
    try:
        out = float(value)
    except Exception as exc:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "PORTFOLIO_SAFE_FLOAT_FAILED",
                exc,
                once_key="safe_float_failed",
                value_type=type(value).__name__,
            )
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def safe_int(
    value: Any, default: int = 0, *, warn_nonfatal: WarnFn | None = None
) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except Exception as exc:
        if warn_nonfatal is not None:
            warn_nonfatal(
                "PORTFOLIO_SAFE_INT_FAILED",
                exc,
                once_key=f"safe_int:{type(value).__name__}:{str(value)[:64]}",
                value_type=type(value).__name__,
            )
        return int(default)


def dict_str_any(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, item in value.items():
        out[str(key)] = item
    return out


def signed_weight(
    row: Optional[Dict[str, Any]],
    *,
    safe_float_fn: Callable[[Any, float], float] = safe_float,
) -> float:
    row = row or {}
    weight = safe_float_fn(row.get("weight", 0.0), 0.0)
    side = str(row.get("side", "") or "").upper().strip()
    if side == "SHORT":
        return -abs(float(weight))
    if side == "LONG":
        return abs(float(weight))
    if side == "FLAT":
        return 0.0
    return float(weight)


def ensure_reason_dict(target: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    row = target or {}
    reason = row.get("reason")
    if isinstance(reason, dict):
        return reason
    row["reason"] = {"raw": reason} if reason not in (None, "") else {}
    return row["reason"]


def normalize_nonnegative_weights(
    weights: List[float],
    *,
    safe_float_fn: Callable[[Any, float], float] = safe_float,
) -> List[float]:
    cleaned = [max(0.0, safe_float_fn(weight, 0.0)) for weight in (weights or [])]
    total = sum(cleaned)
    if total <= 1e-12:
        n = len(cleaned)
        return [1.0 / float(n) for _ in range(n)] if n > 0 else []
    return [float(weight) / float(total) for weight in cleaned]


def side_signed_weight(
    side: Any,
    weight: Any,
    *,
    safe_float_fn: Callable[[Any, float], float] = safe_float,
) -> float:
    side_s = str(side or "").strip().upper()
    magnitude = abs(float(safe_float_fn(weight, 0.0)))
    if magnitude <= 1e-12 or side_s in {"", "FLAT", "NONE"}:
        return 0.0
    if side_s == "SHORT":
        return -magnitude
    if side_s == "LONG":
        return magnitude
    return float(safe_float_fn(weight, 0.0))
