"""Form 4 insider classifier availability columns."""

from __future__ import annotations

id = 32
description = "form4 insider classifier availability columns"


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS insider_transactions ADD COLUMN IF NOT EXISTS availability_ts_ms BIGINT")
    conn.execute("ALTER TABLE IF EXISTS insider_transactions ADD COLUMN IF NOT EXISTS filing_accepted_at TEXT")
    conn.execute("ALTER TABLE IF EXISTS insider_transactions ADD COLUMN IF NOT EXISTS is_10b5_1_plan BOOLEAN")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_availability
          ON insider_transactions(symbol, availability_ts_ms DESC)
        """
    )
