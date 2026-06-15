"""FINRA short-sale volume and short-interest raw data tables."""

from __future__ import annotations

id = 33
description = "FINRA short sale volume and short interest raw data tables"


SHORT_VOLUME_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ts_ms", "BIGINT"),
    ("symbol", "TEXT"),
    ("trade_date", "TEXT"),
    ("trade_ts_ms", "BIGINT"),
    ("availability_ts_ms", "BIGINT"),
    ("source_record_id", "TEXT"),
    ("source_url", "TEXT"),
    ("ingested_ts_ms", "BIGINT"),
    ("short_volume", "DOUBLE PRECISION"),
    ("short_exempt_volume", "DOUBLE PRECISION"),
    ("total_volume", "DOUBLE PRECISION"),
    ("market", "TEXT"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)

SHORT_INTEREST_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ts_ms", "BIGINT"),
    ("symbol", "TEXT"),
    ("settlement_date", "TEXT"),
    ("settlement_ts_ms", "BIGINT"),
    ("dissemination_date", "TEXT"),
    ("dissemination_ts_ms", "BIGINT"),
    ("availability_ts_ms", "BIGINT"),
    ("source_record_id", "TEXT"),
    ("ingested_ts_ms", "BIGINT"),
    ("short_interest_shares", "DOUBLE PRECISION"),
    ("days_to_cover", "DOUBLE PRECISION"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)


def _add_columns(conn, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    for column_name, column_type in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS finra_short_sale_volume (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            trade_date TEXT,
            trade_ts_ms BIGINT,
            availability_ts_ms BIGINT,
            source_record_id TEXT,
            source_url TEXT,
            ingested_ts_ms BIGINT,
            short_volume DOUBLE PRECISION,
            short_exempt_volume DOUBLE PRECISION,
            total_volume DOUBLE PRECISION,
            market TEXT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "finra_short_sale_volume", SHORT_VOLUME_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_finra_short_sale_volume_source_record_id
          ON finra_short_sale_volume(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finra_short_sale_volume_symbol_availability
          ON finra_short_sale_volume(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finra_short_sale_volume_symbol_trade_date
          ON finra_short_sale_volume(symbol, trade_date DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS finra_short_interest (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            settlement_date TEXT,
            settlement_ts_ms BIGINT,
            dissemination_date TEXT,
            dissemination_ts_ms BIGINT,
            availability_ts_ms BIGINT,
            source_record_id TEXT,
            ingested_ts_ms BIGINT,
            short_interest_shares DOUBLE PRECISION,
            days_to_cover DOUBLE PRECISION,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "finra_short_interest", SHORT_INTEREST_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_finra_short_interest_source_record_id
          ON finra_short_interest(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finra_short_interest_symbol_availability
          ON finra_short_interest(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finra_short_interest_symbol_settlement
          ON finra_short_interest(symbol, settlement_ts_ms DESC)
        """
    )
