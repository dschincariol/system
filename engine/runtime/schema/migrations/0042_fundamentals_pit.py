"""Point-in-time fundamentals vendor rows and backfill state."""

from __future__ import annotations

id = 42
description = "point-in-time fundamentals rows keyed by publication timestamp"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_pit (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT NOT NULL,
            fiscal_period TEXT NOT NULL,
            metric TEXT NOT NULL,
            value DOUBLE PRECISION,
            publish_ts_ms BIGINT NOT NULL,
            publish_date TEXT,
            vendor TEXT NOT NULL,
            source_record_id TEXT NOT NULL,
            fiscal_year BIGINT,
            fiscal_quarter BIGINT,
            statement_type TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_fundamentals_pit_source_record_id
          ON fundamentals_pit(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_pit_symbol_metric_publish
          ON fundamentals_pit(symbol, metric, publish_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_pit_symbol_period_metric
          ON fundamentals_pit(symbol, fiscal_period, metric, publish_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_pit_backfill_state (
            vendor TEXT NOT NULL,
            state_key TEXT NOT NULL,
            cursor TEXT,
            completed BIGINT NOT NULL DEFAULT 0,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(vendor, state_key)
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamentals_pit_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            fund_revenue DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_eps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_gross_margin DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_net_margin DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_shares DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_book_value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            fund_fcf DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_publish_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fundamentals_pit_symbol_features_symbol_asof
          ON fundamentals_pit_symbol_features(symbol, asof_ts_ms DESC)
        """
    )
