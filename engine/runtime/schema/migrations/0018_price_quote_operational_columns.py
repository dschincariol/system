"""Price quote freshness columns required by the live router."""

from __future__ import annotations

id = 18
description = "price quote operational timestamp columns"


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS price_quotes ADD COLUMN IF NOT EXISTS last_trade_ts_ms BIGINT")
    conn.execute("ALTER TABLE IF EXISTS price_quotes ADD COLUMN IF NOT EXISTS last_quote_ts_ms BIGINT")
    conn.execute("ALTER TABLE IF EXISTS price_quotes ADD COLUMN IF NOT EXISTS last_update_ts_ms BIGINT")
