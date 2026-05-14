"""Authoritative runtime-facing price cache APIs.

The legacy data-layer cache keeps hybrid helpers for tests, replay, and
backfills. This runtime wrapper makes the live contract explicit:

- in-memory state is authoritative
- runtime reads default to no DB recovery
- health/staleness is exposed for startup and operator checks
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from engine.data.price_cache import (
    PRICE_CACHE_TTL_S,
    PricePoint,
    PriceSnapshot,
    clear_price_cache,
    get_cache_snapshot,
    get_live_symbol_snapshot,
    get_symbol_snapshot as _get_symbol_snapshot,
    is_cache_stale,
    load_symbol_snapshot,
    price_cache_initialized,
    record_price_rows,
    snapshot_from_rows,
)


def get_symbol_snapshot(symbol: str, *, allow_db_recovery: bool = False) -> PriceSnapshot:
    """Return the authoritative runtime price snapshot for one symbol."""
    return _get_symbol_snapshot(symbol, allow_db_recovery=bool(allow_db_recovery))


def update_price_rows(rows: Iterable[Mapping[str, Any]]) -> int:
    """Write price rows into the authoritative in-memory runtime cache."""
    return record_price_rows(rows)


def get_cache_health_snapshot(*, stale_after_s: float | None = None) -> dict[str, Any]:
    """Return runtime cache freshness and initialization metadata."""
    return get_cache_snapshot(stale_after_s=stale_after_s or PRICE_CACHE_TTL_S)


__all__ = [
    "PRICE_CACHE_TTL_S",
    "PricePoint",
    "PriceSnapshot",
    "clear_price_cache",
    "get_cache_health_snapshot",
    "get_live_symbol_snapshot",
    "get_symbol_snapshot",
    "is_cache_stale",
    "load_symbol_snapshot",
    "price_cache_initialized",
    "record_price_rows",
    "snapshot_from_rows",
    "update_price_rows",
]
