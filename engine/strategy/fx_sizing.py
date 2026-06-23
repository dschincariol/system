"""Currency-aware FX sizing helpers.

These helpers attach notional/lots metadata to FX target weights. They never
route orders and never convert a target into an equity-style share count.
"""

from __future__ import annotations

from typing import Any

from engine.risk.fx_leverage_caps import effective_leverage_cap, regulatory_leverage_cap


def _float_or_none(value: Any) -> float | None:
    try:
        out = float(value)
        if out > 0.0 and out == out:
            return float(out)
    except Exception:
        return None
    return None


def _first(raw: Any, *names: str) -> Any:
    if isinstance(raw, dict):
        for name in names:
            if name in raw and raw.get(name) not in (None, ""):
                return raw.get(name)
        return None
    for name in names:
        value = getattr(raw, name, None)
        if value not in (None, ""):
            return value
    return None


def _normalize_instrument(raw: Any) -> dict | None:
    asset_class = str(_first(raw, "asset_class") or "").upper().strip()
    base_ccy = _first(raw, "base_ccy", "base_currency")
    quote_ccy = _first(raw, "quote_ccy", "quote_currency")
    pip_size = _float_or_none(_first(raw, "pip_size", "pip"))
    contract_size = _float_or_none(_first(raw, "contract_size", "lot_size", "standard_lot"))
    leverage_cap = _float_or_none(_first(raw, "leverage_cap", "max_leverage"))
    symbol = str(_first(raw, "symbol") or "").upper().strip()

    if asset_class != "FX" and not (base_ccy and quote_ccy):
        return None
    if not contract_size:
        contract_size = 1.0

    return {
        "asset_class": "FX",
        "base_ccy": str(base_ccy or "").upper().strip() or None,
        "quote_ccy": str(quote_ccy or "").upper().strip() or None,
        "pip_size": float(pip_size or 0.0),
        "contract_size": float(contract_size),
        "leverage_cap": (float(leverage_cap) if leverage_cap else None),
        "symbol": symbol,
    }


def _fx_instrument(con, symbol) -> dict | None:
    """Return normalized FX-02 instrument metadata for ``symbol``.

    FX-02 currently exposes ``engine.data.universe.get_instrument_metadata``
    returning dict fields named ``base_ccy``, ``quote_ccy``, ``pip_size``,
    ``contract_size``, and ``leverage_cap``. The normalizer also accepts the
    earlier naming convention so FX-05 code does not consume raw FX-02 spelling.
    """

    try:
        from engine.data.universe import get_instrument_metadata

        raw = get_instrument_metadata(con, symbol)
        normalized = _normalize_instrument(raw)
        if normalized is not None:
            return normalized
    except Exception:
        normalized = None

    try:
        from engine.data.fx_instrument import parse_fx_symbol

        parsed = parse_fx_symbol(symbol)
        return _normalize_instrument(parsed)
    except Exception:
        return None


def fx_weight_to_notional(
    symbol: str,
    weight: float,
    equity: float,
    instrument: dict | None,
    *,
    pair_rate: float,
) -> dict:
    """Convert an FX weight to base/quote notional and lots.

    Convention: for EURUSD-style spot pairs, ``quote_notional = base_notional *
    pair_rate``. A 0.10 weight on 100000 equity at EURUSD 1.08 is therefore
    10000 EUR base notional and 10800 USD quote notional. This is not
    ``weight * equity / price`` and is not a share quantity.
    """

    inst = dict(instrument or {})
    w = float(weight or 0.0)
    eq = max(0.0, float(equity or 0.0))
    rate = max(0.0, float(pair_rate or 0.0))
    contract_size = _float_or_none(inst.get("contract_size")) or 1.0
    base_notional = abs(w) * eq
    quote_notional = base_notional * rate
    units = base_notional
    lots = units / contract_size if contract_size > 0.0 else 0.0
    inst_cap = _float_or_none(inst.get("leverage_cap"))
    reg_cap = regulatory_leverage_cap(symbol)
    eff_cap = effective_leverage_cap(symbol, inst)
    effective_leverage = base_notional / eq if eq > 0.0 else 0.0

    return {
        "asset_class": "FX",
        "base_ccy": inst.get("base_ccy"),
        "quote_ccy": inst.get("quote_ccy"),
        "weight": float(w),
        "base_notional": float(base_notional),
        "quote_notional": float(quote_notional),
        "units": float(units),
        "lots": float(lots),
        "effective_leverage": float(effective_leverage),
        "pair_rate": float(rate),
        "instrument_leverage": (float(inst_cap) if inst_cap else None),
        "regulatory_leverage": float(reg_cap),
        "effective_leverage_cap": float(eff_cap),
    }


def clamp_fx_weight_to_leverage(symbol: str, weight: float, equity: float, instrument: dict | None) -> tuple[float, dict]:
    """Clamp signed FX weight to the effective leverage cap.

    Missing FX-02 metadata degrades to unchanged weight with an explicit reason;
    the risk engine can then decide whether another fail-closed condition, such
    as a missing pair rate, applies.
    """

    w = float(weight or 0.0)
    if not instrument:
        return float(w), {
            "type": "fx_instrument_missing",
            "clamped": False,
            "pre_weight": float(w),
            "post_weight": float(w),
        }

    cap = effective_leverage_cap(symbol, instrument)
    if cap <= 0.0:
        return float(w), {
            "type": "fx_leverage_cap_invalid",
            "clamped": False,
            "pre_weight": float(w),
            "post_weight": float(w),
            "effective_leverage_cap": float(cap),
        }

    abs_w = abs(w)
    if abs_w <= cap:
        return float(w), {
            "type": "fx_leverage_within_cap",
            "clamped": False,
            "pre_weight": float(w),
            "post_weight": float(w),
            "effective_leverage": float(abs_w),
            "effective_leverage_cap": float(cap),
        }

    sgn = 1.0 if w >= 0.0 else -1.0
    clamped = float(cap) * sgn
    return float(clamped), {
        "type": "fx_leverage_cap",
        "clamped": True,
        "pre_weight": float(w),
        "post_weight": float(clamped),
        "effective_leverage": float(abs_w),
        "effective_leverage_cap": float(cap),
        "scale": float(cap / abs_w) if abs_w > 0.0 else 0.0,
    }


__all__ = ["_fx_instrument", "fx_weight_to_notional", "clamp_fx_weight_to_leverage"]
