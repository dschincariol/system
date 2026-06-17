"""Persist rejected terminal intents with operator-facing reason codes."""

from __future__ import annotations

id = 52
description = "terminal intent rejection audit"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS terminal_intent_rejections (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT,
          qty DOUBLE PRECISION,
          reason_code TEXT NOT NULL,
          reason TEXT NOT NULL,
          source TEXT NOT NULL,
          detail_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_terminal_intent_rejections_symbol_ts
          ON terminal_intent_rejections(symbol, ts_ms DESC)
        """
    )
