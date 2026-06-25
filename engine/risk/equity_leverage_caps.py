"""Conservative leverage caps for EQUITY asset-class sizing.

This module is pure configuration logic. It does not read broker state, does
not touch the database, and never returns an unbounded leverage allowance.
"""

from __future__ import annotations

import json
import math
import os
from typing import Any

_DEFAULT_CAPS = {
    "cash": 1.0,
    # Reg-T initial margin is 50%, which permits 2:1 initial stock leverage.
    "reg_t": 2.0,
}
_VALID_MODES = frozenset(_DEFAULT_CAPS)


def _normalize_mode(raw: Any) -> str:
    mode = str(raw or "").strip().lower().replace("-", "_")
    if mode in {"regt", "reg_t"}:
        return "reg_t"
    if mode == "cash":
        return "cash"
    return "cash"


def _safe_positive_float(value: Any, default: float) -> float:
    try:
        out = float(value)
        if math.isfinite(out) and out > 0.0:
            return float(out)
    except Exception:
        return float(default)
    return float(default)


def _env_caps() -> dict[str, Any]:
    raw = str(os.environ.get("EQUITY_LEVERAGE_CAPS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {str(key).strip().lower().replace("-", "_"): value for key, value in parsed.items()}


def equity_leverage_mode() -> str:
    """Return the configured equity leverage mode, failing closed to cash."""

    return _normalize_mode(os.environ.get("EQUITY_LEVERAGE_MODE", "cash"))


def max_equity_leverage(*, mode: str | None = None) -> float:
    """Return the maximum EQUITY gross leverage for a mode.

    ``cash`` defaults to 1.0. ``reg_t`` defaults to 2.0 because Reg-T initial
    margin is 50%, which permits 2:1 initial stock leverage. Unknown modes and
    malformed overrides fail closed to the conservative cash default.
    """

    normalized = _normalize_mode(mode if mode is not None else equity_leverage_mode())
    if normalized not in _VALID_MODES:
        return 1.0
    default = float(_DEFAULT_CAPS.get(normalized, 1.0))
    override = _env_caps().get(normalized)
    return _safe_positive_float(override, default)


__all__ = ["equity_leverage_mode", "max_equity_leverage"]
