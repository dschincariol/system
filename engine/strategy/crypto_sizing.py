"""Crypto sizing helpers for portfolio-risk metadata.

The helpers are consume-only: they attach fractional-unit, notional, leverage,
and optional volatility-cap diagnostics to target weights. They do not route
orders, authorize trading, or rewrite broker-sim weight-to-quantity math.

CALIBRATION-TODO: this module uses local bare-root normalization because a
canonical ``crypto_instrument.py`` does not exist yet.
"""

from __future__ import annotations

import math
import os
from typing import Any, Mapping


_KNOWN_CRYPTO_BASES = {"BTC", "ETH", "SOL", "BNB", "XRP"}
_KNOWN_CRYPTO_QUOTES = {"USD", "USDT", "USDC"}
_CRYPTO_BASE_ALIASES = {"XBT": "BTC"}


def _float_or_none(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(float(out)):
        return None
    return float(out)


def _positive_float(value: Any) -> float | None:
    out = _float_or_none(value)
    if out is None or out <= 0.0:
        return None
    return float(out)


def _env_float(name: str, default: float) -> float:
    parsed = _float_or_none(os.environ.get(str(name)))
    if parsed is None:
        return float(default)
    return float(parsed)


def _first(raw: Any, *names: str) -> Any:
    if isinstance(raw, Mapping):
        for name in names:
            if name in raw and raw.get(name) not in (None, ""):
                return raw.get(name)
        return None
    for name in names:
        value = getattr(raw, name, None)
        if value not in (None, ""):
            return value
    return None


def normalize_crypto_symbol(symbol: Any) -> str:
    """Return the local bare-root crypto symbol used by current storage paths."""

    text = str(symbol or "").upper().strip()
    if not text:
        return ""
    if text.startswith("X:"):
        text = text[2:]
    for separator in ("/", "-", "_", ":"):
        if separator in text:
            text = text.split(separator, 1)[0]
            break
    for quote in sorted(_KNOWN_CRYPTO_QUOTES, key=len, reverse=True):
        if text.endswith(quote) and len(text) > len(quote):
            text = text[: -len(quote)]
            break
    return _CRYPTO_BASE_ALIASES.get(text, text)


def _quote_currency(symbol: Any, raw: Any = None) -> str:
    quote = str(_first(raw, "quote_ccy", "quote_currency", "currency") or "").upper().strip()
    if quote:
        return quote
    text = str(symbol or "").upper().strip()
    if "/" in text:
        right = text.split("/", 1)[1].split(":", 1)[0]
        return str(right or "USD").upper().strip() or "USD"
    for candidate in sorted(_KNOWN_CRYPTO_QUOTES, key=len, reverse=True):
        if text.endswith(candidate) and len(text) > len(candidate):
            return candidate
    return "USD"


def _normalize_instrument(raw: Any, symbol: Any) -> dict[str, Any] | None:
    root = normalize_crypto_symbol(_first(raw, "base_ccy", "base_currency", "root", "symbol") or symbol)
    if not root:
        return None

    asset_class = str(_first(raw, "asset_class") or "").upper().strip()
    if asset_class and asset_class not in {"CRYPTO", "CRYPTOCURRENCY"}:
        return None
    if root not in _KNOWN_CRYPTO_BASES:
        try:
            from engine.data.asset_map import asset_class_for_symbol

            if str(asset_class_for_symbol(root) or "").upper().strip() != "CRYPTO":
                return None
        except Exception:
            return None

    min_increment = _positive_float(
        _first(raw, "min_increment", "min_qty", "quantity_increment", "lot_size", "step_size")
    )
    leverage_cap = _positive_float(_first(raw, "leverage_cap", "max_leverage", "effective_leverage_cap"))
    volatility = _positive_float(
        _first(raw, "volatility", "realized_vol", "forecast_vol", "daily_vol", "vol")
    )

    return {
        "asset_class": "CRYPTO",
        "symbol": root,
        "base_ccy": root,
        "quote_ccy": _quote_currency(symbol, raw),
        "min_increment": float(min_increment or 1.0e-8),
        "fractional": True,
        "leverage_cap": (float(leverage_cap) if leverage_cap is not None else None),
        "volatility": (float(volatility) if volatility is not None else None),
    }


def _crypto_instrument(con: Any, symbol: Any) -> dict[str, Any] | None:
    """Return local crypto metadata without requiring a schema change."""

    sym = str(symbol or "").upper().strip()
    try:
        from engine.data.universe import get_instrument_metadata

        if con is not None:
            normalized = _normalize_instrument(get_instrument_metadata(con, sym), sym)
            if normalized is not None:
                return normalized
    except Exception:  # no-op-guard: allow - fallback local crypto metadata is intentional.
        pass
    return _normalize_instrument({"asset_class": "CRYPTO", "symbol": sym}, sym)


def _crypto_effective_leverage_cap(instrument: Mapping[str, Any] | None) -> tuple[float, dict[str, Any]]:
    inst = dict(instrument or {})
    configured_cap = max(0.0, _env_float("CRYPTO_MAX_LEVERAGE", 1.0))
    inst_cap = _positive_float(inst.get("leverage_cap"))
    cap = float(configured_cap)
    cap_source = "CRYPTO_MAX_LEVERAGE"
    if inst_cap is not None:
        cap = min(float(cap), float(inst_cap)) if cap > 0.0 else float(inst_cap)
        cap_source = "instrument_leverage_cap"

    vol = _positive_float(inst.get("volatility"))
    vol_target = max(0.0, _env_float("CRYPTO_VOL_TARGET", 0.03))
    vol_floor = max(0.0, _env_float("CRYPTO_VOL_FLOOR", 0.0))
    vol_ceil = max(0.0, _env_float("CRYPTO_VOL_CEIL", 0.0))
    if vol is not None:
        v = float(vol)
        if vol_floor > 0.0:
            v = max(float(vol_floor), v)
        if vol_ceil > 0.0:
            v = min(float(vol_ceil), v)
        if vol_target > 0.0 and v > 0.0:
            vol_cap = float(vol_target) / float(v)
            if vol_cap > 0.0:
                cap = min(float(cap), float(vol_cap)) if cap > 0.0 else float(vol_cap)
                cap_source = "crypto_vol_target"

    return float(cap), {
        "configured_max_leverage": float(configured_cap),
        "instrument_leverage_cap": (float(inst_cap) if inst_cap is not None else None),
        "volatility": (float(vol) if vol is not None else None),
        "vol_target": float(vol_target),
        "effective_leverage_cap": float(cap),
        "cap_source": str(cap_source),
    }


def crypto_weight_to_notional(
    symbol: str,
    weight: float,
    equity: float,
    instrument: Mapping[str, Any] | None,
    *,
    price: float | None = None,
) -> dict[str, Any]:
    """Convert a signed crypto weight to USD notional and fractional units."""

    inst = dict(instrument or {})
    root = normalize_crypto_symbol(inst.get("symbol") or symbol)
    w = float(weight or 0.0)
    eq = max(0.0, float(equity or 0.0))
    px = _positive_float(price)
    notional = abs(float(w)) * float(eq)
    units = float(notional / px) if px and px > 0.0 else None
    min_increment = _positive_float(inst.get("min_increment")) or 1.0e-8
    cap, cap_diag = _crypto_effective_leverage_cap(inst)

    return {
        "asset_class": "CRYPTO",
        "symbol": str(root),
        "base_ccy": str(inst.get("base_ccy") or root),
        "quote_ccy": str(inst.get("quote_ccy") or "USD"),
        "weight": float(w),
        "notional_usd": float(notional),
        "units": (float(units) if units is not None else None),
        "price": (float(px) if px is not None else None),
        "fractional": True,
        "min_increment": float(min_increment),
        "effective_leverage": float(notional / eq) if eq > 0.0 else 0.0,
        "effective_leverage_cap": float(cap),
        **cap_diag,
    }


def clamp_crypto_weight_to_leverage(
    symbol: str,
    weight: float,
    equity: float,
    instrument: Mapping[str, Any] | None,
) -> tuple[float, dict[str, Any]]:
    """Clamp signed crypto weight to the configured leverage/vol cap."""

    w = float(weight or 0.0)
    inst = dict(instrument or {})
    cap, cap_diag = _crypto_effective_leverage_cap(inst)
    abs_w = abs(float(w))
    fractional = True
    min_increment = _positive_float(inst.get("min_increment")) or 1.0e-8

    if cap <= 0.0:
        return float(w), {
            "type": "crypto_leverage_cap_invalid",
            "clamped": False,
            "pre_weight": float(w),
            "post_weight": float(w),
            "effective_leverage": float(abs_w),
            "fractional": bool(fractional),
            "min_increment": float(min_increment),
            **cap_diag,
        }

    if abs_w <= cap + 1e-12:
        return float(w), {
            "type": "crypto_leverage_within_cap",
            "clamped": False,
            "pre_weight": float(w),
            "post_weight": float(w),
            "effective_leverage": float(abs_w),
            "fractional": bool(fractional),
            "min_increment": float(min_increment),
            **cap_diag,
        }

    sgn = 1.0 if w >= 0.0 else -1.0
    clamped = float(cap) * float(sgn)
    return float(clamped), {
        "type": "crypto_leverage_cap",
        "clamped": True,
        "pre_weight": float(w),
        "post_weight": float(clamped),
        "effective_leverage": float(abs_w),
        "fractional": bool(fractional),
        "min_increment": float(min_increment),
        "scale": float(cap / abs_w) if abs_w > 0.0 else 0.0,
        **cap_diag,
    }


def attach_crypto_sizing_context(
    row: Mapping[str, Any] | None,
    crypto_meta: Mapping[str, Any],
    clamp_reason: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a row copy with crypto sizing diagnostics attached."""

    out = dict(row or {})
    reason = out.get("reason")
    if not isinstance(reason, dict):
        reason = {"raw": reason} if reason not in (None, "") else {}
    crypto_reason = dict(reason.get("crypto") or {}) if isinstance(reason.get("crypto"), dict) else {}
    crypto_reason["sizing"] = dict(clamp_reason or {})
    crypto_reason["fractional"] = bool(crypto_meta.get("fractional", True))
    crypto_reason["min_increment"] = float(crypto_meta.get("min_increment") or 1.0e-8)
    reason["crypto"] = crypto_reason
    reason["crypto_sizing"] = dict(clamp_reason or {})
    out["reason"] = reason
    out["crypto"] = dict(crypto_meta or {})
    return out


__all__ = [
    "_crypto_instrument",
    "attach_crypto_sizing_context",
    "clamp_crypto_weight_to_leverage",
    "crypto_weight_to_notional",
    "normalize_crypto_symbol",
]
