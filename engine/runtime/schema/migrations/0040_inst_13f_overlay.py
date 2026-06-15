"""SEC 13F institutional holdings overlay tables."""

from __future__ import annotations

id = 40
description = "SEC 13F low-turnover manager overlay features"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_manager_universe (
            manager_cik TEXT PRIMARY KEY,
            manager_name TEXT,
            active BIGINT NOT NULL DEFAULT 1,
            turnover_threshold DOUBLE PRECISION,
            source TEXT,
            updated_ts_ms BIGINT,
            meta_json JSONB
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_filings (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            manager_cik TEXT NOT NULL,
            manager_name TEXT,
            accession TEXT NOT NULL,
            form TEXT,
            filing_date TEXT,
            report_date TEXT,
            report_ts_ms BIGINT,
            acceptance_datetime TEXT,
            acceptance_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            primary_doc_url TEXT,
            info_table_url TEXT,
            total_value_usd DOUBLE PRECISION,
            holdings_count BIGINT,
            source_record_id TEXT NOT NULL,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_inst_13f_filings_source_record_id
          ON inst_13f_filings(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inst_13f_filings_manager_avail
          ON inst_13f_filings(manager_cik, availability_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_holdings (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            manager_cik TEXT NOT NULL,
            manager_name TEXT,
            accession TEXT NOT NULL,
            report_date TEXT,
            report_ts_ms BIGINT,
            availability_ts_ms BIGINT NOT NULL,
            issuer_name TEXT,
            title_of_class TEXT,
            cusip TEXT,
            value_usd DOUBLE PRECISION,
            value_thousands DOUBLE PRECISION,
            shares DOUBLE PRECISION,
            share_type TEXT,
            put_call TEXT,
            investment_discretion TEXT,
            voting_sole DOUBLE PRECISION,
            voting_shared DOUBLE PRECISION,
            voting_none DOUBLE PRECISION,
            symbol TEXT,
            mapping_status TEXT,
            source_record_id TEXT NOT NULL,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_inst_13f_holdings_source_record_id
          ON inst_13f_holdings(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inst_13f_holdings_symbol_avail
          ON inst_13f_holdings(symbol, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inst_13f_holdings_manager_report
          ON inst_13f_holdings(manager_cik, report_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_cusip_symbol_map (
            cusip TEXT PRIMARY KEY,
            symbol TEXT,
            source TEXT,
            confidence DOUBLE PRECISION,
            updated_ts_ms BIGINT,
            payload_json JSONB
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_13f_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            "13f_consensus_holders" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            "13f_conviction_max" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            "13f_new_position_flag" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            "13f_add_flag" DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_inst_13f_symbol_features_symbol_asof
          ON inst_13f_symbol_features(symbol, asof_ts_ms DESC)
        """
    )
