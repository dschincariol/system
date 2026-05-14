"""Quarantined legacy HRP allocator module.

Production HRP allocation is implemented directly in `engine.strategy.portfolio`.
This module remains only as an explicit fail-closed shim so future callers do
not accidentally bypass the live portfolio path.
"""

from __future__ import annotations

from typing import Any


_QUARANTINE_MESSAGE = (
    "engine.strategy.hrp_allocator is quarantined because production HRP logic "
    "lives in engine.strategy.portfolio."
)


def hrp_optimize_desired(*args: Any, **kwargs: Any):
    """Fail closed for callers of the quarantined legacy HRP allocator entrypoint."""
    raise RuntimeError(f"{_QUARANTINE_MESSAGE} blocked_entrypoint=hrp_optimize_desired")


__all__ = ["hrp_optimize_desired"]
