"""Regulated event-contract metadata on shared prediction-market tables."""

from __future__ import annotations


id = 67
description = "regulated event-contract prediction-market metadata"


def up(conn) -> None:
    for column_name in (
        "product_id",
        "official_resolution_source",
        "source_file_date",
        "source_file_kind",
        "refresh_cadence",
    ):
        conn.execute(f"ALTER TABLE prediction_market_events ADD COLUMN IF NOT EXISTS {column_name} TEXT")
    conn.execute("ALTER TABLE prediction_market_events ADD COLUMN IF NOT EXISTS provider_timestamp_ms BIGINT")

    for column_name in (
        "provider_contract_id",
        "product_id",
        "official_resolution_source",
        "source_file_date",
        "source_file_kind",
        "refresh_cadence",
    ):
        conn.execute(f"ALTER TABLE prediction_market_markets ADD COLUMN IF NOT EXISTS {column_name} TEXT")
    conn.execute("ALTER TABLE prediction_market_markets ADD COLUMN IF NOT EXISTS provider_timestamp_ms BIGINT")

    for table_name in ("prediction_market_orderbook_snapshots", "prediction_market_price_history"):
        for column_name in ("provider_contract_id", "product_id", "source_file_date", "source_file_kind"):
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} TEXT")

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_product
          ON prediction_market_markets(provider_name, product_id, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_contract_source
          ON prediction_market_markets(provider_contract_id, source_file_date, source_file_kind)
        """
    )
