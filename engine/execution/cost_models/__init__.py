"""Execution cost model interfaces."""

from __future__ import annotations

from typing import Protocol


class CostModel(Protocol):
    # Live implementation: engine.execution.cost_models.almgren_chriss.AlmgrenChrissCost.
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
        """Return the round-trip execution cost in basis points for a
        notional dollar order against the supplied ADV, daily volatility,
        and participation rate. Implementations live in
        `engine.execution.cost_models.almgren_chriss.AlmgrenChrissCost`.
        """


from engine.execution.cost_models.almgren_chriss import AlmgrenChrissCost

__all__ = ["AlmgrenChrissCost", "CostModel"]
