"""Futures contract sizing and margin enforcement helpers.

These helpers are pure risk/sizing utilities. They do not route orders, mutate
broker state, or assert profitability. ``margin_ref`` from FUT-01 is treated as
reference data; enforcement reconciles it with broker/regulatory input using
``min(reference, broker_or_regulatory)`` per FUT-07.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Mapping

LOG = logging.getLogger(__name__)
_WARNED_KEYS: set[str] = set()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if out == out and math.isfinite(out):
            return float(out)
    except Exception:
        return float(default)
    return float(default)


def _warn_once(key: str, message: str) -> None:
    if key in _WARNED_KEYS:
        return
    _WARNED_KEYS.add(key)
    LOG.warning("%s", message)


def currency_conversion_rate(
    price_ccy: str,
    account_ccy: str = "USD",
    fx_rates: Mapping[str, Any] | None = None,
) -> float:
    """Return a price-currency to account-currency conversion rate.

    ``fx_rates`` may contain direct keys such as ``"EURUSD"`` or inverse keys
    such as ``"USDEUR"``. Missing rates degrade to 1.0 with a warning so risk
    math remains finite and explicit.
    """

    price = str(price_ccy or account_ccy or "USD").upper().strip() or "USD"
    account = str(account_ccy or "USD").upper().strip() or "USD"
    if price == account:
        return 1.0
    rates = dict(fx_rates or {})
    direct = f"{price}{account}"
    inverse = f"{account}{price}"
    direct_rate = _safe_float(rates.get(direct), 0.0)
    if direct_rate > 0.0:
        return float(direct_rate)
    inverse_rate = _safe_float(rates.get(inverse), 0.0)
    if inverse_rate > 0.0:
        return float(1.0 / inverse_rate)
    _warn_once(
        f"missing_fx_rate:{price}:{account}",
        f"Missing futures FX conversion rate {price}->{account}; using 1.0",
    )
    return 1.0


def contract_notional(
    contracts: int | float,
    price: float,
    multiplier: float,
    *,
    price_ccy: str = "USD",
    account_ccy: str = "USD",
    fx_rates: Mapping[str, Any] | None = None,
) -> float:
    rate = currency_conversion_rate(price_ccy, account_ccy, fx_rates)
    return float(abs(float(contracts or 0.0)) * max(0.0, float(price or 0.0)) * max(0.0, float(multiplier or 0.0)) * rate)


def weight_to_contracts(weight: float, capital: float, multiplier: float, price: float) -> int:
    """Convert signed target weight to whole futures contracts without oversizing."""

    w = float(weight or 0.0)
    cap = max(0.0, float(capital or 0.0))
    mult = max(0.0, float(multiplier or 0.0))
    px = max(0.0, float(price or 0.0))
    denom = mult * px
    if abs(w) <= 1e-12 or cap <= 0.0 or denom <= 0.0:
        return 0
    contracts = int(math.floor(abs(w) * cap / denom))
    return contracts if w > 0.0 else -contracts


def enforced_margin_per_contract(reference_margin: float, regulatory_or_broker_margin: float | None = None) -> float:
    """Return the margin value FUT-07 enforces per contract."""

    ref = _safe_float(reference_margin, 0.0)
    reg = _safe_float(regulatory_or_broker_margin, 0.0) if regulatory_or_broker_margin is not None else 0.0
    if ref > 0.0 and reg > 0.0:
        return float(min(ref, reg))
    if ref > 0.0:
        return float(ref)
    if reg > 0.0:
        return float(reg)
    return 0.0


def required_margin(
    contracts: int | float,
    reference_margin: float,
    regulatory_or_broker_margin: float | None = None,
    *,
    price_ccy: str = "USD",
    account_ccy: str = "USD",
    fx_rates: Mapping[str, Any] | None = None,
) -> dict[str, float]:
    """Compute initial/maintenance margin for a signed futures position."""

    per_contract = enforced_margin_per_contract(reference_margin, regulatory_or_broker_margin)
    rate = currency_conversion_rate(price_ccy, account_ccy, fx_rates)
    contracts_abs = abs(int(float(contracts or 0.0)))
    total = float(contracts_abs) * float(per_contract) * float(rate)
    return {
        "contracts": float(contracts_abs),
        "reference_margin": float(_safe_float(reference_margin, 0.0)),
        "regulatory_or_broker_margin": float(_safe_float(regulatory_or_broker_margin, 0.0)),
        "enforced_margin_per_contract": float(per_contract),
        "fx_rate": float(rate),
        "initial_margin": float(total),
        "maintenance_margin": float(total),
    }


def cap_contracts_by_margin(
    desired_contracts: int | float,
    capital: float,
    budget_weight: float,
    reference_margin: float,
    regulatory_or_broker_margin: float | None = None,
    *,
    price_ccy: str = "USD",
    account_ccy: str = "USD",
    fx_rates: Mapping[str, Any] | None = None,
) -> tuple[int, dict[str, float | bool]]:
    """Cap signed contracts so aggregate enforced margin stays within budget."""

    desired = int(float(desired_contracts or 0.0))
    if desired == 0:
        margin = required_margin(
            0,
            reference_margin,
            regulatory_or_broker_margin,
            price_ccy=price_ccy,
            account_ccy=account_ccy,
            fx_rates=fx_rates,
        )
        return 0, {**margin, "budget": 0.0, "clamped": False, "max_contracts": 0.0}

    cap = max(0.0, float(capital or 0.0))
    budget = max(0.0, float(budget_weight or 0.0)) * cap
    margin = required_margin(
        desired,
        reference_margin,
        regulatory_or_broker_margin,
        price_ccy=price_ccy,
        account_ccy=account_ccy,
        fx_rates=fx_rates,
    )
    per_contract_base = float(margin.get("enforced_margin_per_contract", 0.0)) * float(margin.get("fx_rate", 1.0))
    if budget <= 0.0 or per_contract_base <= 0.0:
        return 0, {**margin, "budget": float(budget), "clamped": True, "max_contracts": 0.0}

    max_contracts = int(math.floor(float(budget) / float(per_contract_base)))
    capped_abs = min(abs(desired), max_contracts)
    capped = capped_abs if desired > 0 else -capped_abs
    capped_margin = required_margin(
        capped,
        reference_margin,
        regulatory_or_broker_margin,
        price_ccy=price_ccy,
        account_ccy=account_ccy,
        fx_rates=fx_rates,
    )
    return int(capped), {
        **capped_margin,
        "budget": float(budget),
        "clamped": bool(abs(capped) < abs(desired)),
        "max_contracts": float(max_contracts),
    }


__all__ = [
    "cap_contracts_by_margin",
    "contract_notional",
    "currency_conversion_rate",
    "enforced_margin_per_contract",
    "required_margin",
    "weight_to_contracts",
]
