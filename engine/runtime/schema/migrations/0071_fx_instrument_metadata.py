"""Add FX instrument metadata columns to symbols."""

from __future__ import annotations

id = 71
description = "fx instrument metadata columns"


def up(conn) -> None:
    # Backfill is lazy/on-write through engine.data.universe.upsert_symbol.
    # get_instrument_metadata also falls back to the parser when columns are empty.
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS instrument_kind TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS base_ccy TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS quote_ccy TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS pip_size DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS contract_size DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS pnl_ccy TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS leverage_cap DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS session_calendar TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS instrument_meta_source TEXT")
