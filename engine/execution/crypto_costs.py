"""Offline crypto cost assumptions for backtests and promotion gates.

The values in this module are conservative CALIBRATION-TODO placeholders, not
broker-calibrated quotes. Runtime crypto symbol ownership is still missing
(``engine/data/crypto_instrument.py`` does not exist), so the local normalizer
uses the bare-root convention already used by ``asset_map.py`` and
``crypto_funding_rates``.
"""

from __future__ import annotations

import json
import math
import os
from typing import Any, Dict, Mapping


CRYPTO_DEFAULT_SYMBOL = "BTC"
_QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BTC", "ETH", "EUR")
_BASE_ALIASES = {"XBT": "BTC"}

# CALIBRATION-TODO: placeholder maker/taker fee assumptions in bps.
CRYPTO_TAKER_BPS: Dict[str, float] = {
    "BTC": 10.0,
    "ETH": 10.0,
    "SOL": 12.0,
    "BNB": 12.0,
    "XRP": 12.0,
}
CRYPTO_MAKER_BPS: Dict[str, float] = {
    "BTC": 4.0,
    "ETH": 4.0,
    "SOL": 5.0,
    "BNB": 5.0,
    "XRP": 5.0,
}

# CALIBRATION-TODO: full bid/ask spread in bps of spot/perp notional.
CRYPTO_SPREAD_BPS: Dict[str, float] = {
    "BTC": 4.0,
    "ETH": 5.0,
    "SOL": 8.0,
    "BNB": 8.0,
    "XRP": 10.0,
}

# CALIBRATION-TODO: positive funding means long positions pay and shorts
# receive. Values are bps per held day.
CRYPTO_FUNDING_BPS_PER_DAY: Dict[str, float] = {
    "BTC": 3.0,
    "ETH": 3.5,
    "SOL": 5.0,
    "BNB": 4.0,
    "XRP": 4.0,
}


def is_crypto_asset_class(asset_class: str | None) -> bool:
    """Return True for CRYPTO-style asset-class tags."""

    asset = str(asset_class or "").upper().strip()
    return asset == "CRYPTO" or asset.startswith("CRYPTO")


def normalize_crypto_symbol(symbol: Any) -> str:
    """Normalize crypto spot/perp symbols to the bare-root storage key.

    This is a local fallback only. The canonical instrument owner is still
    absent; do not let this grow into a second instrument registry.
    """

    text = str(symbol or "").upper().strip()
    if not text:
        return ""
    text = text.replace("-", "/").replace("_", "/")
    if "/" in text:
        parts = [part for part in text.split("/") if part]
        if parts:
            root = parts[0].split(":")[0].strip().upper()
            return _BASE_ALIASES.get(root, root)
    compact = text.replace("/", "").split(":")[0].strip().upper()
    for suffix in _QUOTE_SUFFIXES:
        if compact.endswith(suffix) and len(compact) > len(suffix):
            compact = compact[: -len(suffix)]
            break
    return _BASE_ALIASES.get(compact, compact)


def is_crypto_symbol(symbol: Any) -> bool:
    """Return True when ``asset_map`` classifies the normalized root as crypto."""

    root = normalize_crypto_symbol(symbol)
    if not root:
        return False
    try:
        from engine.data.asset_map import asset_class_for_symbol

        return bool(asset_class_for_symbol(root) == "CRYPTO")
    except Exception:
        return root in CRYPTO_SPREAD_BPS or root in CRYPTO_TAKER_BPS


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
    out = {str(k).upper().strip(): float(v) for k, v in dict(base).items()}
    for raw_key, raw_value in _json_payload(name).items():
        key = normalize_crypto_symbol(raw_key)
        value = _safe_float(raw_value, math.nan)
        if not key or not math.isfinite(value):
            continue
        if not allow_negative:
            value = max(0.0, value)
        out[key] = float(value)
    return out


def _scalar_override(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return float(default)
    return max(0.0, _safe_float(raw, default))


def _table_value(table: Mapping[str, float], symbol: Any, default_symbol: str = CRYPTO_DEFAULT_SYMBOL) -> float:
    key = normalize_crypto_symbol(symbol) or default_symbol
    if key in table:
        return float(table[key])
    if default_symbol in table:
        return float(table[default_symbol])
    values = [float(value) for value in table.values()]
    return float(max(values or [0.0]))


def _spread_table() -> Dict[str, float]:
    table = _float_override_map("CRYPTO_SPREAD_BPS_OVERRIDE_JSON", CRYPTO_SPREAD_BPS)
    scalar = os.environ.get("CRYPTO_SPREAD_BPS")
    if scalar not in (None, ""):
        table[CRYPTO_DEFAULT_SYMBOL] = _scalar_override("CRYPTO_SPREAD_BPS", table.get(CRYPTO_DEFAULT_SYMBOL, 4.0))
    return table


def _fee_table(*, taker: bool) -> Dict[str, float]:
    if taker:
        table = _float_override_map("CRYPTO_TAKER_BPS_OVERRIDE_JSON", CRYPTO_TAKER_BPS)
        scalar_name = "CRYPTO_TAKER_BPS"
    else:
        table = _float_override_map("CRYPTO_MAKER_BPS_OVERRIDE_JSON", CRYPTO_MAKER_BPS)
        scalar_name = "CRYPTO_MAKER_BPS"
    scalar = os.environ.get(scalar_name)
    if scalar not in (None, ""):
        table[CRYPTO_DEFAULT_SYMBOL] = _scalar_override(scalar_name, table.get(CRYPTO_DEFAULT_SYMBOL, 0.0))
    return table


def _funding_table() -> Dict[str, float]:
    table = _float_override_map(
        "CRYPTO_FUNDING_BPS_PER_DAY_OVERRIDE_JSON",
        CRYPTO_FUNDING_BPS_PER_DAY,
        allow_negative=True,
    )
    # Backward-compatible alias for prompt-level CRYPTO_FUNDING_* overrides.
    for raw_key, raw_value in _json_payload("CRYPTO_FUNDING_OVERRIDE_JSON").items():
        key = normalize_crypto_symbol(raw_key)
        value = _safe_float(raw_value, math.nan)
        if key and math.isfinite(value):
            table[key] = float(value)
    scalar = os.environ.get("CRYPTO_FUNDING_BPS_PER_DAY")
    if scalar not in (None, ""):
        table[CRYPTO_DEFAULT_SYMBOL] = _safe_float(scalar, table.get(CRYPTO_DEFAULT_SYMBOL, 0.0))
    return table


def spread_bps(symbol: Any, *, half: bool = True) -> float:
    """Return a deterministic offline crypto spread in bps."""

    full_bps = max(0.0, _table_value(_spread_table(), symbol))
    return float(full_bps / 2.0 if half else full_bps)


def fee_bps(symbol: Any, *, taker: bool = True) -> float:
    """Return maker/taker fee bps for deterministic offline crypto costs."""

    return float(max(0.0, _table_value(_fee_table(taker=bool(taker)), symbol)))


def funding_carry_bps(symbol: Any, side_sign: float, nights: int | float) -> float:
    """Return signed perpetual funding carry bps for the side and hold length.

    Positive funding is a cost for longs and a credit for shorts.
    """

    try:
        night_count = int(max(0.0, float(nights or 0.0)))
    except Exception:
        night_count = 0
    if night_count <= 0:
        return 0.0
    base_bps = float(_table_value(_funding_table(), symbol))
    side = 1.0 if float(side_sign or 0.0) >= 0.0 else -1.0
    return float(base_bps * side * float(night_count))


__all__ = [
    "CRYPTO_DEFAULT_SYMBOL",
    "CRYPTO_FUNDING_BPS_PER_DAY",
    "CRYPTO_MAKER_BPS",
    "CRYPTO_SPREAD_BPS",
    "CRYPTO_TAKER_BPS",
    "fee_bps",
    "funding_carry_bps",
    "is_crypto_asset_class",
    "is_crypto_symbol",
    "normalize_crypto_symbol",
    "spread_bps",
]
