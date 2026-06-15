"""ETF shares outstanding and unexpected-flow feature tables."""

from __future__ import annotations

id = 38
description = "ETF shares outstanding and unexpected flow features"


ETF_SHARES_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ts_ms", "BIGINT"),
    ("symbol", "TEXT"),
    ("asof_date", "TEXT"),
    ("asof_ts_ms", "BIGINT"),
    ("availability_ts_ms", "BIGINT"),
    ("shares_outstanding", "DOUBLE PRECISION"),
    ("source", "TEXT"),
    ("source_record_id", "TEXT"),
    ("price", "DOUBLE PRECISION"),
    ("nav", "DOUBLE PRECISION"),
    ("premium_pct", "DOUBLE PRECISION"),
    ("ingested_ts_ms", "BIGINT"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)

ETF_FLOW_FEATURE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("symbol", "TEXT"),
    ("asof_ts_ms", "BIGINT"),
    ("bucket_ts_ms", "BIGINT"),
    ("etf_unexpected_flow_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("etf_flow_3d_sum_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("etf_flow_reversal_flag", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("latest_shares_outstanding", "DOUBLE PRECISION"),
    ("latest_flow_dollars", "DOUBLE PRECISION"),
    ("latest_unexpected_flow", "DOUBLE PRECISION"),
    ("latest_aum", "DOUBLE PRECISION"),
    ("source_max_availability_ts_ms", "BIGINT"),
    ("created_ts_ms", "BIGINT"),
    ("meta_json", "JSONB"),
)


def _add_columns(conn, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    for column_name, column_type in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etf_shares_outstanding (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT NOT NULL,
            asof_date TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            shares_outstanding DOUBLE PRECISION NOT NULL,
            source TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            price DOUBLE PRECISION,
            nav DOUBLE PRECISION,
            premium_pct DOUBLE PRECISION,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "etf_shares_outstanding", ETF_SHARES_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_etf_shares_outstanding_source_record_id
          ON etf_shares_outstanding(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_etf_shares_outstanding_symbol_availability
          ON etf_shares_outstanding(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_etf_shares_outstanding_symbol_asof
          ON etf_shares_outstanding(symbol, asof_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS etf_flow_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            bucket_ts_ms BIGINT NOT NULL,
            etf_unexpected_flow_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            etf_flow_3d_sum_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            etf_flow_reversal_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            latest_shares_outstanding DOUBLE PRECISION,
            latest_flow_dollars DOUBLE PRECISION,
            latest_unexpected_flow DOUBLE PRECISION,
            latest_aum DOUBLE PRECISION,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )
    _add_columns(conn, "etf_flow_features", ETF_FLOW_FEATURE_COLUMNS)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_etf_flow_features_symbol_asof
          ON etf_flow_features(symbol, asof_ts_ms DESC)
        """
    )
