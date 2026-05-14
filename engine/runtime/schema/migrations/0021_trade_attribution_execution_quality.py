"""Add execution quality columns written by trade attribution."""

from __future__ import annotations

id = 21
description = "trade attribution execution quality columns"


def up(conn) -> None:
    for column, column_type in (
        ("expected_price", "DOUBLE PRECISION"),
        ("fill_price", "DOUBLE PRECISION"),
        ("execution_slippage", "DOUBLE PRECISION"),
        ("execution_latency_ms", "DOUBLE PRECISION"),
    ):
        conn.execute(
            f"ALTER TABLE IF EXISTS trade_attribution_ledger "
            f"ADD COLUMN IF NOT EXISTS {column} {column_type}"
        )
