"""Realized outcome series used by model scoring."""

from __future__ import annotations

id = 50
description = "realized outcomes table"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS realized_outcomes (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            realized_return DOUBLE PRECISION NOT NULL,
            metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_ts_ms BIGINT,
            updated_ts_ms BIGINT,
            UNIQUE(symbol, ts_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_realized_outcomes_symbol_ts
          ON realized_outcomes(symbol, ts_ms)
        """
    )
