"""Quiver government-flow raw data and conditional feature tables."""

from __future__ import annotations

id = 41
description = "Quiver congressional, lobbying, and government-contract data"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quiver_congressional_trades (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            source_record_id TEXT NOT NULL,
            dedupe_key TEXT NOT NULL,
            member_name TEXT,
            chamber TEXT,
            party TEXT,
            district TEXT,
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
            availability_ts_ms BIGINT NOT NULL,
            source_url TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_quiver_congressional_trades_source_record_id
          ON quiver_congressional_trades(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quiver_congressional_trades_symbol_avail
          ON quiver_congressional_trades(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quiver_congressional_trades_dedupe
          ON quiver_congressional_trades(dedupe_key)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quiver_lobbying_filings (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            sector TEXT,
            source_record_id TEXT NOT NULL,
            client_name TEXT,
            registrant_name TEXT,
            issue_area TEXT,
            filing_date TEXT,
            filing_ts_ms BIGINT,
            disclosure_date TEXT,
            disclosure_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            amount_usd DOUBLE PRECISION,
            source_url TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_quiver_lobbying_filings_source_record_id
          ON quiver_lobbying_filings(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quiver_lobbying_filings_symbol_avail
          ON quiver_lobbying_filings(symbol, availability_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS quiver_gov_contracts (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            symbol TEXT,
            sector TEXT,
            source_record_id TEXT NOT NULL,
            recipient_name TEXT,
            agency TEXT,
            contract_id TEXT,
            description TEXT,
            award_date TEXT,
            award_ts_ms BIGINT,
            disclosure_date TEXT,
            disclosure_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            amount_usd DOUBLE PRECISION,
            source_url TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_quiver_gov_contracts_source_record_id
          ON quiver_gov_contracts(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quiver_gov_contracts_symbol_avail
          ON quiver_gov_contracts(symbol, availability_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_member_committee_map (
            member_name TEXT NOT NULL,
            committee TEXT NOT NULL,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(member_name, committee)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_committee_sector_map (
            committee TEXT NOT NULL,
            sector TEXT NOT NULL,
            weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(committee, sector)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_member_leadership_map (
            member_name TEXT PRIMARY KEY,
            leadership_role TEXT,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_symbol_sector_map (
            symbol TEXT PRIMARY KEY,
            sector TEXT,
            source TEXT,
            updated_ts_ms BIGINT,
            meta_json JSONB
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gov_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            congress_committee_buy_30d DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            congress_leadership_trade_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            congress_sale_signal_30d DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            lobbying_spend_z_yoy DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            gov_contract_award_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gov_symbol_features_symbol_asof
          ON gov_symbol_features(symbol, asof_ts_ms DESC)
        """
    )
