"""Add futures contract metadata columns to symbols."""

from __future__ import annotations

id = 72
description = "futures contract metadata columns"


def up(conn) -> None:
    # Backfill is lazy/on-write through engine.data.universe.upsert_symbol.
    # get_instrument_metadata also falls back to the parser when columns are empty.
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_root TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_exchange TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_multiplier DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_tick_size DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_tick_value DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_price_ccy TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_margin_ref DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_expiry_rule TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_roll_method TEXT")
    conn.execute("ALTER TABLE IF EXISTS symbols ADD COLUMN IF NOT EXISTS fut_continuous_alias TEXT")
