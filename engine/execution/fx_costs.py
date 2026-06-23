"""Offline FX cost assumptions for backtests and promotion gates.

The values in this module are conservative CALIBRATION TODO placeholders, not
live broker quotes. Runtime FX symbol semantics come from FX-02 when available;
the local pip-size and reference-price tables are fallback/proxy data for
offline return-space cost conversion only.
"""

from __future__ import annotations

import json
import math
import os
from typing import Any, Dict, Mapping


FX_DEFAULT_SYMBOL = "EUR_USD"

# CALIBRATION TODO: placeholder all-in bid/ask spreads in pips.
FX_PIP_SPREAD: Dict[str, float] = {
    "EUR_USD": 0.8,
    "USD_JPY": 1.0,
    "GBP_USD": 1.1,
    "AUD_USD": 1.0,
    "USD_CHF": 1.2,
    "USD_CAD": 1.1,
    "NZD_USD": 1.3,
    "EUR_GBP": 1.0,
    "EUR_JPY": 1.4,
    "GBP_JPY": 1.8,
    "EUR_CHF": 1.3,
}

# CALIBRATION TODO: signed overnight swap/carry pips per night. Positive values
# are costs; negative values are carry credits. Long and short differ by pair.
FX_SWAP_PIPS_LONG: Dict[str, float] = {
    "EUR_USD": 0.08,
    "USD_JPY": 0.10,
    "GBP_USD": 0.09,
    "AUD_USD": 0.10,
    "USD_CHF": 0.08,
    "USD_CAD": 0.07,
    "NZD_USD": 0.10,
    "EUR_GBP": 0.07,
    "EUR_JPY": 0.12,
    "GBP_JPY": 0.15,
    "EUR_CHF": 0.08,
}
FX_SWAP_PIPS_SHORT: Dict[str, float] = {
    "EUR_USD": 0.03,
    "USD_JPY": 0.05,
    "GBP_USD": 0.04,
    "AUD_USD": 0.05,
    "USD_CHF": 0.04,
    "USD_CAD": 0.03,
    "NZD_USD": 0.05,
    "EUR_GBP": 0.03,
    "EUR_JPY": 0.06,
    "GBP_JPY": 0.08,
    "EUR_CHF": 0.04,
}

# Fallback only. Prefer FX-02's parser/accessor for pip size.
FX_PIP_SIZE: Dict[str, float] = {
    "EUR_USD": 0.0001,
    "USD_JPY": 0.01,
    "GBP_USD": 0.0001,
    "AUD_USD": 0.0001,
    "USD_CHF": 0.0001,
    "USD_CAD": 0.0001,
    "NZD_USD": 0.0001,
    "EUR_GBP": 0.0001,
    "EUR_JPY": 0.01,
    "GBP_JPY": 0.01,
    "EUR_CHF": 0.0001,
}

# CALIBRATION TODO: mid-price proxies used only to convert pip costs into bps in
# offline return-space paths where no live mid is available.
FX_REF_PRICE: Dict[str, float] = {
    "EUR_USD": 1.10,
    "USD_JPY": 145.0,
    "GBP_USD": 1.28,
    "AUD_USD": 0.66,
    "USD_CHF": 0.90,
    "USD_CAD": 1.36,
    "NZD_USD": 0.61,
    "EUR_GBP": 0.86,
    "EUR_JPY": 158.0,
    "GBP_JPY": 185.0,
    "EUR_CHF": 0.97,
}

# CALIBRATION TODO: relative weekend-gap risk proxy by pair.
FX_WEEKEND_GAP_RISK_MULTIPLIER: Dict[str, float] = {
    "EUR_USD": 1.0,
    "USD_JPY": 1.1,
    "GBP_USD": 1.2,
    "AUD_USD": 1.15,
    "USD_CHF": 1.05,
    "USD_CAD": 1.05,
    "NZD_USD": 1.2,
    "EUR_GBP": 1.0,
    "EUR_JPY": 1.2,
    "GBP_JPY": 1.35,
    "EUR_CHF": 1.1,
}

_KNOWN_CCY = {
    "USD",
    "EUR",
    "JPY",
    "GBP",
    "CHF",
    "AUD",
    "NZD",
    "CAD",
    "SEK",
    "NOK",
    "MXN",
}


def is_fx_asset_class(asset_class: str | None) -> bool:
    """Return True for FX-style asset-class tags, including FX_MAJOR/MINOR."""

    return str(asset_class or "").upper().strip().startswith("FX")


def _parse_with_fx02(symbol: str) -> Any | None:
    try:
        from engine.data.fx_instrument import parse_fx_symbol

        return parse_fx_symbol(symbol)
    except Exception:
        return None


def normalize_fx_symbol(symbol: str) -> str:
    """Normalize FX pair symbols to the underscore cost-table key form.

    FX-02 is authoritative when present. The 6-letter split below is a defensive
    fallback for offline cost lookup only, not a second runtime instrument model.
    """

    parsed = _parse_with_fx02(symbol)
    if parsed is not None:
        base = str(getattr(parsed, "base_ccy", "") or "").upper().strip()
        quote = str(getattr(parsed, "quote_ccy", "") or "").upper().strip()
        if base and quote:
            return f"{base}_{quote}"
        parsed_symbol = str(getattr(parsed, "symbol", "") or "").upper().strip()
        if parsed_symbol:
            return parsed_symbol

    text = str(symbol or "").upper().strip().replace("/", "_").replace("-", "_")
    if "_" in text:
        parts = [part for part in text.split("_") if part]
        if len(parts) == 2 and len(parts[0]) == 3 and len(parts[1]) == 3:
            return f"{parts[0]}_{parts[1]}"
    compact = text.replace("_", "")
    if len(compact) == 6 and compact[:3] in _KNOWN_CCY and compact[3:] in _KNOWN_CCY:
        return f"{compact[:3]}_{compact[3:]}"
    return text


def is_fx_symbol(symbol: str) -> bool:
    """Return True for FX spot-pair symbols, preferring FX-02 when importable."""

    try:
        from engine.data.fx_instrument import is_fx_symbol as fx02_is_fx_symbol

        if bool(fx02_is_fx_symbol(symbol)):
            return True
    except Exception:
        # no-op-guard: allow - FX-02 may be absent; fallback parser below is intentional.
        pass
    norm = normalize_fx_symbol(symbol)
    if "_" not in norm:
        return False
    base, quote = norm.split("_", 1)
    return len(base) == 3 and len(quote) == 3 and base in _KNOWN_CCY and quote in _KNOWN_CCY


def _safe_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    return float(parsed) if math.isfinite(parsed) else float(default)


def _json_payload(name: str) -> Mapping[str, Any]:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return {}
    try:
        payload = json.loads(str(raw))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _float_override_map(name: str, base: Mapping[str, float], *, allow_negative: bool = False) -> Dict[str, float]:
    out = {str(k): float(v) for k, v in dict(base).items()}
    for raw_key, raw_value in _json_payload(name).items():
        key = normalize_fx_symbol(str(raw_key))
        value = _safe_float(raw_value, math.nan)
        if not key or not math.isfinite(value):
            continue
        if not allow_negative:
            value = max(0.0, value)
        out[key] = float(value)
    return out


def _swap_maps() -> tuple[Dict[str, float], Dict[str, float]]:
    long_map = {str(k): float(v) for k, v in FX_SWAP_PIPS_LONG.items()}
    short_map = {str(k): float(v) for k, v in FX_SWAP_PIPS_SHORT.items()}
    payload = _json_payload("FX_SWAP_PIPS_OVERRIDE_JSON")
    if "long" in payload or "short" in payload:
        for raw_key, raw_value in dict(payload.get("long") or {}).items():
            value = _safe_float(raw_value, math.nan)
            if math.isfinite(value):
                long_map[normalize_fx_symbol(str(raw_key))] = float(value)
        for raw_key, raw_value in dict(payload.get("short") or {}).items():
            value = _safe_float(raw_value, math.nan)
            if math.isfinite(value):
                short_map[normalize_fx_symbol(str(raw_key))] = float(value)
        return long_map, short_map

    for raw_key, raw_value in payload.items():
        key = normalize_fx_symbol(str(raw_key))
        if isinstance(raw_value, dict):
            long_value = _safe_float(raw_value.get("long"), math.nan)
            short_value = _safe_float(raw_value.get("short"), math.nan)
            if math.isfinite(long_value):
                long_map[key] = float(long_value)
            if math.isfinite(short_value):
                short_map[key] = float(short_value)
        else:
            value = _safe_float(raw_value, math.nan)
            if math.isfinite(value):
                long_map[key] = float(value)
                short_map[key] = float(value)
    return long_map, short_map


def _spread_table() -> Dict[str, float]:
    return _float_override_map("FX_PIP_SPREAD_OVERRIDE_JSON", FX_PIP_SPREAD)


def _pair_key(symbol: str) -> str:
    norm = normalize_fx_symbol(symbol)
    return norm if norm else FX_DEFAULT_SYMBOL


def _pip_size(symbol: str) -> float:
    parsed = _parse_with_fx02(symbol)
    if parsed is not None:
        value = _safe_float(getattr(parsed, "pip_size", None), math.nan)
        if math.isfinite(value) and value > 0.0:
            return float(value)
    key = _pair_key(symbol)
    if key in FX_PIP_SIZE:
        return float(FX_PIP_SIZE[key])
    return 0.01 if key.endswith("_JPY") else 0.0001


def _ref_price(symbol: str) -> float:
    key = _pair_key(symbol)
    if key in FX_REF_PRICE:
        return max(1e-12, float(FX_REF_PRICE[key]))
    return 145.0 if key.endswith("_JPY") else 1.0


def pip_spread_bps(symbol: str, *, half: bool = True) -> float:
    """Return spread in bps of price using deterministic offline mid proxies."""

    key = _pair_key(symbol)
    spread_table = _spread_table()
    pips = float(spread_table.get(key, max(spread_table.values() or [1.0])))
    full_bps = float((pips * _pip_size(key) / _ref_price(key)) * 10000.0)
    return float(full_bps / 2.0 if half else full_bps)


def swap_bps(symbol: str, side_sign: float, nights: int) -> float:
    """Return signed overnight swap/carry bps for the side and hold length."""

    night_count = max(0, int(nights or 0))
    if night_count <= 0:
        return 0.0
    key = _pair_key(symbol)
    long_map, short_map = _swap_maps()
    table = long_map if float(side_sign or 0.0) >= 0.0 else short_map
    pips = float(table.get(key, table.get(FX_DEFAULT_SYMBOL, 0.0)))
    return float((pips * _pip_size(key) / _ref_price(key)) * 10000.0 * float(night_count))


def weekend_gap_bps(symbol: str, *, crosses_weekend: bool) -> float:
    """Return a conservative weekend-gap surcharge when the hold crosses Fri-Sun."""

    if not bool(crosses_weekend):
        return 0.0
    base_bps = max(0.0, _safe_float(os.environ.get("FX_WEEKEND_GAP_BPS"), 3.0))
    key = _pair_key(symbol)
    multiplier = float(FX_WEEKEND_GAP_RISK_MULTIPLIER.get(key, 1.25))
    return float(base_bps * max(0.0, multiplier))
