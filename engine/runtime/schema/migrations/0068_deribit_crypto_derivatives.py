"""Deribit public crypto derivatives snapshots and provider readiness."""

from __future__ import annotations


id = 68
description = "deribit public crypto derivatives market-data snapshots"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS deribit_instruments (
          instrument_name TEXT PRIMARY KEY,
          base_asset TEXT NOT NULL,
          quote_currency TEXT,
          instrument_type TEXT NOT NULL,
          kind TEXT,
          expiry_ts_ms BIGINT,
          strike DOUBLE PRECISION,
          option_type TEXT,
          settlement_period TEXT,
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          source_ts_ms BIGINT,
          availability_ts_ms BIGINT NOT NULL,
          raw_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS deribit_market_snapshots (
          source_record_id TEXT PRIMARY KEY,
          instrument_name TEXT NOT NULL,
          base_asset TEXT NOT NULL,
          quote_currency TEXT,
          instrument_type TEXT NOT NULL,
          kind TEXT,
          expiry_ts_ms BIGINT,
          strike DOUBLE PRECISION,
          option_type TEXT,
          mark_price DOUBLE PRECISION,
          index_price DOUBLE PRECISION,
          bid_price DOUBLE PRECISION,
          ask_price DOUBLE PRECISION,
          mid_price DOUBLE PRECISION,
          last_price DOUBLE PRECISION,
          underlying_price DOUBLE PRECISION,
          mark_iv DOUBLE PRECISION,
          bid_iv DOUBLE PRECISION,
          ask_iv DOUBLE PRECISION,
          delta DOUBLE PRECISION,
          gamma DOUBLE PRECISION,
          theta DOUBLE PRECISION,
          vega DOUBLE PRECISION,
          open_interest DOUBLE PRECISION,
          volume DOUBLE PRECISION,
          volume_usd DOUBLE PRECISION,
          current_funding DOUBLE PRECISION,
          funding_8h DOUBLE PRECISION,
          futures_basis DOUBLE PRECISION,
          perp_basis DOUBLE PRECISION,
          best_bid_amount DOUBLE PRECISION,
          best_ask_amount DOUBLE PRECISION,
          spread_bps DOUBLE PRECISION,
          source_ts_ms BIGINT NOT NULL,
          availability_ts_ms BIGINT NOT NULL,
          ingested_ts_ms BIGINT NOT NULL,
          raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          diagnostics_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS deribit_provider_state (
          source_key TEXT PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          readiness_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deribit_instruments_base_type
          ON deribit_instruments(base_asset, instrument_type, expiry_ts_ms)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deribit_snapshots_base_availability
          ON deribit_market_snapshots(base_asset, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deribit_snapshots_instrument_availability
          ON deribit_market_snapshots(instrument_name, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deribit_snapshots_type_availability
          ON deribit_market_snapshots(base_asset, instrument_type, availability_ts_ms DESC)
        """
    )
