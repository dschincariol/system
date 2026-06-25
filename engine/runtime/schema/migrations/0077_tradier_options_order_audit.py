"""Add live Tradier options order audit metadata columns."""

from __future__ import annotations

id = 77
description = "tradier options live order audit metadata columns"


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS broker_fills ADD COLUMN IF NOT EXISTS live_options_broker TEXT")
    conn.execute("ALTER TABLE IF EXISTS broker_fills ADD COLUMN IF NOT EXISTS tradier_option_symbol TEXT")
    conn.execute("ALTER TABLE IF EXISTS broker_fills ADD COLUMN IF NOT EXISTS tradier_order_id TEXT")
    conn.execute("ALTER TABLE IF EXISTS broker_fills ADD COLUMN IF NOT EXISTS tradier_order_status TEXT")
