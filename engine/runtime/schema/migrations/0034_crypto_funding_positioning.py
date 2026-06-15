"""Crypto perpetual funding and basis raw data table."""

from __future__ import annotations

id = 34
description = "crypto funding rates and perpetual basis raw data"


CRYPTO_FUNDING_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ts_ms", "BIGINT"),
    ("symbol", "TEXT"),
    ("exchange", "TEXT"),
    ("perp_market", "TEXT"),
    ("spot_market", "TEXT"),
    ("funding_ts_ms", "BIGINT"),
    ("availability_ts_ms", "BIGINT"),
    ("funding_rate", "DOUBLE PRECISION"),
    ("mark_price", "DOUBLE PRECISION"),
    ("index_price", "DOUBLE PRECISION"),
    ("spot_price", "DOUBLE PRECISION"),
    ("spot_ts_ms", "BIGINT"),
    ("perp_ts_ms", "BIGINT"),
    ("perp_basis_pct", "DOUBLE PRECISION"),
    ("source_record_id", "TEXT"),
    ("ingested_ts_ms", "BIGINT"),
    ("is_live", "BOOLEAN"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)


def _add_columns(conn, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    for column_name, column_type in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_funding_rates (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            exchange TEXT,
            perp_market TEXT,
            spot_market TEXT,
            funding_ts_ms BIGINT,
            availability_ts_ms BIGINT,
            funding_rate DOUBLE PRECISION,
            mark_price DOUBLE PRECISION,
            index_price DOUBLE PRECISION,
            spot_price DOUBLE PRECISION,
            spot_ts_ms BIGINT,
            perp_ts_ms BIGINT,
            perp_basis_pct DOUBLE PRECISION,
            source_record_id TEXT,
            ingested_ts_ms BIGINT,
            is_live BOOLEAN,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "crypto_funding_rates", CRYPTO_FUNDING_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_crypto_funding_rates_source_record_id
          ON crypto_funding_rates(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_crypto_funding_rates_symbol_availability
          ON crypto_funding_rates(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_crypto_funding_rates_symbol_funding
          ON crypto_funding_rates(symbol, funding_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_crypto_funding_rates_exchange_market
          ON crypto_funding_rates(exchange, perp_market, funding_ts_ms DESC)
        """
    )
