"""Quarantined legacy allocation module.

Production portfolio allocation lives in `engine.strategy.portfolio`. This
legacy module is intentionally left importable only so accidental callers fail
closed with an explicit runtime error instead of silently diverging from the
live allocation path.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


_QUARANTINE_MESSAGE = (
    "allocation_engine.py is quarantined because it is not wired into the "
    "production portfolio path. Use engine.strategy.portfolio instead."
)


def _raise_quarantined(entrypoint: str) -> None:
    raise RuntimeError(f"{_QUARANTINE_MESSAGE} blocked_entrypoint={entrypoint}")


@dataclass(frozen=True)
class AllocationInput:
    """Legacy allocation candidate payload retained only for compatibility checks."""
    name: str
    confidence: float
    historical_pnl: float
    volatility: float
    max_cap: float | None = None


@dataclass(frozen=True)
class AllocationResult:
    """Legacy allocation result shape kept so quarantined imports fail predictably."""
    name: str
    confidence: float
    historical_pnl: float
    volatility: float
    confidence_score: float
    pnl_score: float
    volatility_score: float
    composite_score: float
    weight: float
    position_size: float
    applied_cap: float
    capped: bool


class AllocationEngine:
    """Fail-closed shim for the removed legacy allocation engine."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        _raise_quarantined("AllocationEngine")


DEFAULT_ENGINE = None


def allocate_capital(*args: Any, **kwargs: Any):
    """Fail closed for callers of the removed legacy allocation entrypoint."""
    _raise_quarantined("allocate_capital")


__all__ = [
    "AllocationEngine",
    "AllocationInput",
    "AllocationResult",
    "DEFAULT_ENGINE",
    "allocate_capital",
]
