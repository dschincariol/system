"""Almgren-Chriss style expected market-impact costs."""

from __future__ import annotations

import math
import logging
from dataclasses import dataclass, field
from typing import Dict, Mapping

LOGGER = logging.getLogger(__name__)
_PARTICIPATION_CLAMP_WARNED = False


_DEFAULT_OVERRIDES: Dict[str, tuple[float, float]] = {
    "US_EQUITY": (0.142, 0.314),
    "EQUITY": (0.142, 0.314),
}


def _finite_float(value: float, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return out if math.isfinite(out) else float(default)


def _clamp_participation(value: float) -> float:
    global _PARTICIPATION_CLAMP_WARNED

    raw = _finite_float(value, 0.0)
    clamped = max(0.0, min(1.0, raw))
    if clamped != raw and not _PARTICIPATION_CLAMP_WARNED:
        LOGGER.warning(
            "ALMGREN_CHRISS_PARTICIPATION_CLAMPED: participation=%s clamped=%s",
            raw,
            clamped,
        )
        _PARTICIPATION_CLAMP_WARNED = True
    return float(clamped)


@dataclass(frozen=True)
class AlmgrenChrissCost:
    """Expected slippage in basis points.

    The calculation follows the project convention from the CPCV prompt:
    ``half_spread_bps + eta * sigma_daily * sqrt(notional / adv) +
    gamma * (notional / adv)``. Pass ``sigma_daily`` in the same bps units
    expected by that calibration.
    """

    eta: float = 0.142
    gamma: float = 0.314
    default_participation: float = 0.10
    asset_class_coefficients: Mapping[str, tuple[float, float]] = field(default_factory=lambda: dict(_DEFAULT_OVERRIDES))

    def coefficients_for(self, asset_class: str | None = None) -> tuple[float, float]:
        key = str(asset_class or "").upper().strip()
        if key and key in self.asset_class_coefficients:
            eta, gamma = self.asset_class_coefficients[key]
            return float(eta), float(gamma)
        return float(self.eta), float(self.gamma)

    def components_bps(
        self,
        *,
        notional: float,
        adv: float,
        sigma_daily: float,
        participation: float,
        half_spread_bps: float = 0.0,
        asset_class: str | None = None,
    ) -> dict:
        notional_f = abs(_finite_float(notional, 0.0))
        adv_f = max(_finite_float(adv, 0.0), 1e-12)
        sigma_f = max(_finite_float(sigma_daily, 0.0), 0.0)
        participation_f = _clamp_participation(participation)
        half_spread_f = max(_finite_float(half_spread_bps, 0.0), 0.0)
        eta, gamma = self.coefficients_for(asset_class)

        adv_fraction = max(0.0, notional_f / adv_f)
        base_participation = max(_finite_float(self.default_participation, 0.10), 1e-12)
        urgency = math.sqrt(max(participation_f, 0.0) / base_participation) if participation_f > 0.0 else 0.0
        temporary_bps = float(eta) * sigma_f * math.sqrt(adv_fraction) * urgency
        permanent_bps = float(gamma) * adv_fraction
        total_bps = half_spread_f + temporary_bps + permanent_bps

        return {
            "half_spread_bps": float(half_spread_f),
            "temporary_impact_bps": float(max(0.0, temporary_bps)),
            "permanent_impact_bps": float(max(0.0, permanent_bps)),
            "total_cost_bps": float(max(0.0, total_bps)),
            "notional": float(notional_f),
            "adv": float(adv_f),
            "adv_fraction": float(adv_fraction),
            "sigma_daily": float(sigma_f),
            "participation": float(participation_f),
            "eta": float(eta),
            "gamma": float(gamma),
            "asset_class": (str(asset_class).upper().strip() if asset_class else None),
        }

    def cost_bps(
        self,
        *,
        notional: float,
        adv: float,
        sigma_daily: float,
        participation: float,
        half_spread_bps: float = 0.0,
        asset_class: str | None = None,
    ) -> float:
        return float(
            self.components_bps(
                notional=notional,
                adv=adv,
                sigma_daily=sigma_daily,
                participation=participation,
                half_spread_bps=half_spread_bps,
                asset_class=asset_class,
            )["total_cost_bps"]
        )


__all__ = ["AlmgrenChrissCost"]
