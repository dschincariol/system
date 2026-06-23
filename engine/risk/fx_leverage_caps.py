"""Regulatory FX leverage caps used by portfolio-risk enforcement.

The seed values below are copied from FX-00 section 6: EU/ESMA retail
major pairs at 30:1, lower non-major caps, and US/NFA majors at 50:1 with
lower non-major caps. There is no persisted FX-00 machine-readable artifact;
this module is the runtime source of truth.
"""

from __future__ import annotations

import json
import os
from typing import Any, Mapping

try:
    from engine.data.fx_instrument import parse_fx_symbol
except Exception:  # pragma: no cover - defensive import fallback
    parse_fx_symbol = None  # type: ignore


_MAJOR_PAIRS = frozenset(
    {
        "EURUSD",
        "USDJPY",
        "GBPUSD",
        "USDCHF",
        "USDCAD",
        "AUDUSD",
        "NZDUSD",
    }
)

_G10_CURRENCIES = frozenset({"AUD", "CAD", "CHF", "EUR", "GBP", "JPY", "NOK", "NZD", "SEK", "USD"})

_DEFAULT_CAPS = {
    # FX-00 §6: ESMA/EU retail major FX pairs are capped around 30:1.
    "EU": {"major": 30.0, "minor": 20.0, "exotic": 10.0},
    # FX-00 §6: NFA/US retail major FX pairs are capped around 50:1.
    "US": {"major": 50.0, "minor": 20.0, "exotic": 10.0},
}


def _safe_float(value: Any, default: float) -> float:
    try:
        out = float(value)
        if out > 0.0 and out == out:
            return float(out)
    except Exception:
        return float(default)
    return float(default)


def _canonical_symbol(symbol: object) -> str:
    if parse_fx_symbol is not None:
        try:
            parsed = parse_fx_symbol(symbol)
            if parsed is not None:
                return str(parsed.symbol).upper().strip()
        except Exception:
            return str(symbol or "").upper().replace("/", "").replace("_", "").strip()
    return str(symbol or "").upper().replace("/", "").replace("_", "").strip()


def _pair_tier(symbol: str) -> str:
    s = _canonical_symbol(symbol)
    if s in _MAJOR_PAIRS:
        return "major"
    if len(s) == 6 and s[:3] in _G10_CURRENCIES and s[3:] in _G10_CURRENCIES:
        return "minor"
    return "exotic"


def _env_caps() -> dict[str, Any]:
    raw = str(os.environ.get("FX_REGULATORY_LEVERAGE_CAPS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _profile_caps(jurisdiction: str | None) -> Mapping[str, Any]:
    caps = {key: dict(value) for key, value in _DEFAULT_CAPS.items()}
    override = _env_caps()
    for key, value in override.items():
        if isinstance(value, dict):
            base = caps.setdefault(str(key).upper(), {})
            for tier, cap in value.items():
                base[str(tier).lower()] = _safe_float(cap, _safe_float(base.get(str(tier).lower()), 10.0))
    profile = str(jurisdiction or os.environ.get("FX_LEVERAGE_JURISDICTION") or "EU").upper().strip() or "EU"
    return caps.get(profile) or caps["EU"]


def regulatory_leverage_cap(symbol: str, *, jurisdiction: str | None = None) -> float:
    """Return the jurisdictional regulatory leverage cap for an FX pair.

    Unknown symbols are treated as exotic and receive the most conservative
    cap in the selected profile. Per-symbol overrides can be supplied in
    ``FX_REGULATORY_LEVERAGE_CAPS_JSON`` using a top-level canonical symbol key.
    """

    s = _canonical_symbol(symbol)
    override = _env_caps()
    if s and s in override and not isinstance(override.get(s), dict):
        return _safe_float(override.get(s), 10.0)
    profile = _profile_caps(jurisdiction)
    tier = _pair_tier(s)
    conservative = min(_safe_float(value, 10.0) for value in profile.values()) if profile else 10.0
    return _safe_float(profile.get(tier), conservative)


def effective_leverage_cap(symbol: str, instrument: dict | None) -> float:
    """Return ``min(instrument_cap, regulatory_cap)`` with fail-closed defaults."""

    regulatory = regulatory_leverage_cap(symbol)
    inst_cap = None
    if isinstance(instrument, dict):
        inst_cap = instrument.get("leverage_cap")
        if inst_cap is None:
            inst_cap = instrument.get("max_leverage")
    cap = _safe_float(inst_cap, regulatory)
    return float(min(cap, regulatory))


__all__ = ["regulatory_leverage_cap", "effective_leverage_cap"]
