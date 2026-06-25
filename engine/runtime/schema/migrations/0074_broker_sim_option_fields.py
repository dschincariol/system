"""Add broker simulator option realism fill columns."""

from __future__ import annotations

id = 74
description = "broker simulator option fill metadata columns"


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS broker_fills ADD COLUMN IF NOT EXISTS contract_multiplier DOUBLE PRECISION")
    conn.execute("ALTER TABLE IF EXISTS broker_fills ADD COLUMN IF NOT EXISTS option_quote_source TEXT")
    conn.execute("ALTER TABLE IF EXISTS broker_fills ADD COLUMN IF NOT EXISTS option_margin_debit DOUBLE PRECISION")
