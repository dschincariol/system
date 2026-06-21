"""Prediction-market macro expectation storage."""

from __future__ import annotations

id = 65
description = "prediction-market macro expectation storage"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_events (
            id BIGSERIAL PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            event_ticker TEXT NOT NULL,
            series_ticker TEXT,
            title TEXT,
            provider_category TEXT NOT NULL,
            event_type TEXT,
            event_ts_ms BIGINT,
            resolution_ts_ms BIGINT,
            source_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            affected_assets_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            raw_payload_hash TEXT NOT NULL,
            raw_json JSONB NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            updated_ts_ms BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_prediction_market_events_provider_event
          ON prediction_market_events(provider_name, provider_event_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_events_avail
          ON prediction_market_events(provider_category, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_events_resolution
          ON prediction_market_events(provider_name, resolution_ts_ms)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_markets (
            id BIGSERIAL PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_market_id TEXT NOT NULL,
            provider_event_id TEXT NOT NULL,
            market_ticker TEXT NOT NULL,
            series_ticker TEXT,
            title TEXT,
            subtitle TEXT,
            provider_category TEXT NOT NULL,
            event_type TEXT,
            status TEXT,
            probability DOUBLE PRECISION,
            previous_probability DOUBLE PRECISION,
            probability_delta DOUBLE PRECISION,
            bid_probability DOUBLE PRECISION,
            ask_probability DOUBLE PRECISION,
            last_price DOUBLE PRECISION,
            liquidity DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            volume_24h DOUBLE PRECISION,
            open_interest DOUBLE PRECISION,
            spread DOUBLE PRECISION,
            event_ts_ms BIGINT,
            close_ts_ms BIGINT,
            resolution_ts_ms BIGINT,
            source_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            affected_assets_json JSONB NOT NULL DEFAULT '[]'::jsonb,
            raw_payload_hash TEXT NOT NULL,
            raw_json JSONB NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            updated_ts_ms BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_prediction_market_markets_provider_market
          ON prediction_market_markets(provider_name, provider_market_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_avail
          ON prediction_market_markets(provider_category, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_markets_event
          ON prediction_market_markets(provider_name, provider_event_id, availability_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_orderbook_snapshots (
            id BIGSERIAL PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_market_id TEXT NOT NULL,
            source_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            best_yes_bid DOUBLE PRECISION,
            best_yes_ask DOUBLE PRECISION,
            best_no_bid DOUBLE PRECISION,
            best_no_ask DOUBLE PRECISION,
            mid_probability DOUBLE PRECISION,
            spread DOUBLE PRECISION,
            yes_depth DOUBLE PRECISION,
            no_depth DOUBLE PRECISION,
            liquidity DOUBLE PRECISION,
            imbalance DOUBLE PRECISION,
            raw_payload_hash TEXT NOT NULL,
            raw_json JSONB NOT NULL,
            created_ts_ms BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_prediction_market_orderbook_snapshot
          ON prediction_market_orderbook_snapshots(provider_name, provider_market_id, availability_ts_ms, raw_payload_hash)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_orderbook_avail
          ON prediction_market_orderbook_snapshots(provider_name, provider_market_id, availability_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_price_history (
            id BIGSERIAL PRIMARY KEY,
            provider_name TEXT NOT NULL,
            provider_market_id TEXT NOT NULL,
            trade_id TEXT NOT NULL,
            trade_ts_ms BIGINT NOT NULL,
            source_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            price DOUBLE PRECISION,
            size DOUBLE PRECISION,
            side TEXT,
            raw_payload_hash TEXT NOT NULL,
            raw_json JSONB NOT NULL,
            created_ts_ms BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_prediction_market_price_history_trade
          ON prediction_market_price_history(provider_name, provider_market_id, trade_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_market_price_history_avail
          ON prediction_market_price_history(provider_name, provider_market_id, availability_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_market_backfill_state (
            provider_name TEXT NOT NULL,
            state_key TEXT NOT NULL,
            status TEXT,
            cursor_json JSONB,
            updated_ts_ms BIGINT NOT NULL,
            error TEXT,
            PRIMARY KEY(provider_name, state_key)
        )
        """
    )
