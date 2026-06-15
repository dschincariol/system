"""Live ingestion coordination tables."""

from __future__ import annotations

from engine.runtime.storage_live_ingestion_schema import (
    ensure_options_symbol_ingestion_state_schema,
    ensure_price_feed_lock_schema,
)

id = 30
description = "live ingestion coordination tables"


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    raise RuntimeError(f"{code}:{type(error).__name__}:{error}:{extra or {}}")


def up(conn) -> None:
    ensure_price_feed_lock_schema(conn, warn_nonfatal=_warn_nonfatal)
    ensure_options_symbol_ingestion_state_schema(conn, warn_nonfatal=_warn_nonfatal)
