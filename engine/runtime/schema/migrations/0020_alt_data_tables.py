"""Alternative data ingestion tables used by Form 4 and congressional feeds."""

from __future__ import annotations

id = 20
description = "alt data ingestion persistence tables"


INSIDER_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ts_ms", "BIGINT"),
    ("symbol", "TEXT"),
    ("event_id", "BIGINT"),
    ("source_transaction_id", "TEXT"),
    ("created_ts_ms", "BIGINT"),
    ("ingested_ts_ms", "BIGINT"),
    ("source", "TEXT"),
    ("filing_accession", "TEXT"),
    ("filing_identifier", "TEXT"),
    ("filing_url", "TEXT"),
    ("filing_ts_ms", "BIGINT"),
    ("availability_ts_ms", "BIGINT"),
    ("filing_date", "TEXT"),
    ("filing_accepted_at", "TEXT"),
    ("transaction_ts_ms", "BIGINT"),
    ("transaction_date", "TEXT"),
    ("issuer_name", "TEXT"),
    ("issuer_cik", "TEXT"),
    ("insider_name", "TEXT"),
    ("insider_cik", "TEXT"),
    ("insider_role", "TEXT"),
    ("insider_title", "TEXT"),
    ("transaction_code", "TEXT"),
    ("transaction_type", "TEXT"),
    ("direction", "TEXT"),
    ("security_type", "TEXT"),
    ("shares", "DOUBLE PRECISION"),
    ("price", "DOUBLE PRECISION"),
    ("value", "DOUBLE PRECISION"),
    ("ownership_nature", "TEXT"),
    ("is_10b5_1_plan", "BOOLEAN"),
    ("entity_id", "TEXT"),
    ("resolution_status", "TEXT"),
    ("resolution_method", "TEXT"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)

CONGRESSIONAL_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ts_ms", "BIGINT"),
    ("symbol", "TEXT"),
    ("event_id", "BIGINT"),
    ("source_trade_id", "TEXT"),
    ("source_record_id", "TEXT"),
    ("source_url", "TEXT"),
    ("created_ts_ms", "BIGINT"),
    ("ingested_ts_ms", "BIGINT"),
    ("source", "TEXT"),
    ("chamber", "TEXT"),
    ("office", "TEXT"),
    ("politician_name", "TEXT"),
    ("owner_name", "TEXT"),
    ("issuer_name", "TEXT"),
    ("transaction_type_raw", "TEXT"),
    ("transaction_type", "TEXT"),
    ("direction", "TEXT"),
    ("amount_range", "TEXT"),
    ("amount_low", "DOUBLE PRECISION"),
    ("amount_high", "DOUBLE PRECISION"),
    ("amount_mid", "DOUBLE PRECISION"),
    ("transaction_date", "TEXT"),
    ("transaction_ts_ms", "BIGINT"),
    ("disclosure_date", "TEXT"),
    ("disclosure_ts_ms", "BIGINT"),
    ("entity_id", "TEXT"),
    ("resolution_status", "TEXT"),
    ("resolution_method", "TEXT"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)


def _add_columns(conn, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    for column_name, column_type in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS insider_transactions (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            event_id BIGINT,
            source_transaction_id TEXT,
            created_ts_ms BIGINT,
            ingested_ts_ms BIGINT,
            source TEXT,
            filing_accession TEXT,
            filing_identifier TEXT,
            filing_url TEXT,
            filing_ts_ms BIGINT,
            availability_ts_ms BIGINT,
            filing_date TEXT,
            filing_accepted_at TEXT,
            transaction_ts_ms BIGINT,
            transaction_date TEXT,
            issuer_name TEXT,
            issuer_cik TEXT,
            insider_name TEXT,
            insider_cik TEXT,
            insider_role TEXT,
            insider_title TEXT,
            transaction_code TEXT,
            transaction_type TEXT,
            direction TEXT,
            security_type TEXT,
            shares DOUBLE PRECISION,
            price DOUBLE PRECISION,
            value DOUBLE PRECISION,
            ownership_nature TEXT,
            is_10b5_1_plan BOOLEAN,
            entity_id TEXT,
            resolution_status TEXT,
            resolution_method TEXT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "insider_transactions", INSIDER_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_insider_transactions_source_transaction_id
          ON insider_transactions(source_transaction_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_ts
          ON insider_transactions(symbol, transaction_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_availability
          ON insider_transactions(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_insider_transactions_resolution_ts
          ON insider_transactions(resolution_status, transaction_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS congressional_trades (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            event_id BIGINT,
            source_trade_id TEXT,
            source_record_id TEXT,
            source_url TEXT,
            created_ts_ms BIGINT,
            ingested_ts_ms BIGINT,
            source TEXT,
            chamber TEXT,
            office TEXT,
            politician_name TEXT,
            owner_name TEXT,
            issuer_name TEXT,
            transaction_type_raw TEXT,
            transaction_type TEXT,
            direction TEXT,
            amount_range TEXT,
            amount_low DOUBLE PRECISION,
            amount_high DOUBLE PRECISION,
            amount_mid DOUBLE PRECISION,
            transaction_date TEXT,
            transaction_ts_ms BIGINT,
            disclosure_date TEXT,
            disclosure_ts_ms BIGINT,
            entity_id TEXT,
            resolution_status TEXT,
            resolution_method TEXT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "congressional_trades", CONGRESSIONAL_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_congressional_trades_source_trade_id
          ON congressional_trades(source_trade_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_congressional_trades_symbol_ts
          ON congressional_trades(symbol, transaction_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_congressional_trades_resolution_ts
          ON congressional_trades(resolution_status, transaction_ts_ms DESC)
        """
    )
