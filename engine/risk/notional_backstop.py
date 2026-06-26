"""Non-bypassable gross/net notional backstop for portfolio targets."""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Tuple


def _env_float(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default) or default)
    except Exception:
        return float("nan")


BACKSTOP_MAX_GROSS = _env_float("PORTFOLIO_BACKSTOP_MAX_GROSS", "1.00")
BACKSTOP_MAX_NET = _env_float("PORTFOLIO_BACKSTOP_MAX_NET", "0.60")
BACKSTOP_ENABLED = os.environ.get("PORTFOLIO_NOTIONAL_BACKSTOP", "1") == "1"


def _weight(row: Dict[str, Any] | None) -> float:
    value = float((row or {}).get("weight", 0.0) or 0.0)
    if not math.isfinite(value):
        raise ValueError("portfolio notional backstop saw non-finite weight")
    return float(value)


def _gross(rows: Dict[str, Dict[str, Any]]) -> float:
    return float(sum(abs(_weight(row)) for row in (rows or {}).values()))


def _net_abs(rows: Dict[str, Dict[str, Any]]) -> float:
    return float(abs(sum(_weight(row) for row in (rows or {}).values())))


def _validated_cap(name: str, value: float) -> float:
    cap = float(value)
    if not math.isfinite(cap) or cap < 0.0:
        raise ValueError(f"{name} must be finite and non-negative")
    return cap


def _copy_desired(desired: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(sym): dict(row or {}) for sym, row in (desired or {}).items()}


def _scale_weights(out: Dict[str, Dict[str, Any]], scale: float, reason_key: str, reason: Dict[str, Any]) -> None:
    if not math.isfinite(scale) or scale < 0.0:
        raise ValueError("portfolio notional backstop scale must be finite and non-negative")
    for sym in list(out.keys()):
        row = out.get(sym)
        if not isinstance(row, dict):
            row = {}
            out[sym] = row
        row["weight"] = float(_weight(row) * float(scale))
        existing_reason = row.get("reason")
        if not isinstance(existing_reason, dict):
            existing_reason = {}
            row["reason"] = existing_reason
        backstop_reason = existing_reason.setdefault("portfolio_notional_backstop", {})
        if isinstance(backstop_reason, dict):
            backstop_reason[reason_key] = dict(reason)


def apply_notional_backstop(
    desired: Dict[str, Dict[str, Any]],
    *,
    is_live: bool,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Clamp final desired weights to the independent gross/net backstop caps."""

    out = _copy_desired(desired or {})
    gross_cap = _validated_cap("PORTFOLIO_BACKSTOP_MAX_GROSS", BACKSTOP_MAX_GROSS)
    net_cap = _validated_cap("PORTFOLIO_BACKSTOP_MAX_NET", BACKSTOP_MAX_NET)
    gross_pre = _gross(out)
    net_pre = _net_abs(out)
    meta: Dict[str, Any] = {
        "enabled": bool(BACKSTOP_ENABLED),
        "is_live": bool(is_live),
        "gross_pre": float(gross_pre),
        "net_pre": float(net_pre),
        "gross_cap": float(gross_cap),
        "net_cap": float(net_cap),
        "scaled": False,
    }

    if not BACKSTOP_ENABLED:
        meta["gross_post"] = float(gross_pre)
        meta["net_post"] = float(net_pre)
        meta["status"] = "disabled"
        return out, meta

    if gross_pre > gross_cap + 1e-12:
        scale = float(gross_cap / gross_pre) if gross_pre > 1e-12 else 0.0
        _scale_weights(
            out,
            scale,
            "gross",
            {"pre": float(gross_pre), "cap": float(gross_cap), "scale": float(scale)},
        )
        meta["scaled"] = True
        meta["gross_scaled"] = True
        meta["gross_scale"] = float(scale)

    net_after_gross = _net_abs(out)
    if net_after_gross > net_cap + 1e-12:
        scale = float(net_cap / net_after_gross) if net_after_gross > 1e-12 else 0.0
        _scale_weights(
            out,
            scale,
            "net",
            {"pre": float(net_after_gross), "cap": float(net_cap), "scale": float(scale)},
        )
        meta["scaled"] = True
        meta["net_scaled"] = True
        meta["net_scale"] = float(scale)

    meta["gross_post"] = float(_gross(out))
    meta["net_post"] = float(_net_abs(out))
    meta["status"] = "scaled" if bool(meta.get("scaled")) else "clear"
    return out, meta
