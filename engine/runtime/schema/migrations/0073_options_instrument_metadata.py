"""Add options contract metadata columns to symbols."""

from __future__ import annotations

id = 73
description = "options contract metadata columns"


def up(conn) -> None:
    # Backfill is lazy/on-write through engine.data.universe.upsert_symbol.
    # get_instrument_metadata also falls back to the parser when columns are empty.
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_underlying TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_expiry TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_right TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_strike DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_multiplier DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_exercise_style TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_settlement TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS opt_price_ccy TEXT")
