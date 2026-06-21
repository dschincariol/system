"""Polymarket event-signal metadata on shared prediction-market tables."""

from __future__ import annotations


id = 66
description = "prediction-market event signal metadata"


def up(conn) -> None:
    for column_name in ("semantic_event_id", "resolution_semantics"):
        conn.execute(f"ALTER TABLE prediction_market_events ADD COLUMN IF NOT EXISTS {column_name} TEXT")

    for column_name in (
        "condition_id",
        "token_id",
        "outcome_name",
        "semantic_event_id",
        "resolution_semantics",
    ):
        conn.execute(f"ALTER TABLE prediction_market_markets ADD COLUMN IF NOT EXISTS {column_name} TEXT")

    for table_name in ("prediction_market_orderbook_snapshots", "prediction_market_price_history"):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS condition_id TEXT")
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS token_id TEXT")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_semantic
          ON prediction_market_markets(semantic_event_id, resolution_semantics, availability_ts_ms DESC)
        """
    )
