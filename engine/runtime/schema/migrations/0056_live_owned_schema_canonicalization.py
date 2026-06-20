"""Canonicalize live-owned table contracts required by production preflight."""

from __future__ import annotations

id = 56
description = "canonicalize live-owned schema contracts"


def _warn_nonfatal(*_args, **_kwargs) -> None:
    return None


def up(conn) -> None:
    from engine.runtime.storage_live_ingestion_schema import (
        ensure_price_quotes_raw_schema,
        ensure_prices_schema,
    )

    conn.execute("ALTER TABLE strategy_metrics ADD COLUMN IF NOT EXISTS is_active BIGINT NOT NULL DEFAULT 1")
    ensure_prices_schema(conn, warn_nonfatal=_warn_nonfatal)
    ensure_price_quotes_raw_schema(conn, warn_nonfatal=_warn_nonfatal)
