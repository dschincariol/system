"""CFTC Commitments of Traders raw positioning and feature tables."""

from __future__ import annotations

id = 39
description = "CFTC Commitments of Traders positioning features"


CFTC_COT_POSITION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("ts_ms", "BIGINT"),
    ("report_type", "TEXT"),
    ("contract_key", "TEXT"),
    ("market_and_exchange_names", "TEXT"),
    ("contract_market_name", "TEXT"),
    ("cftc_contract_market_code", "TEXT"),
    ("report_date", "TEXT"),
    ("report_ts_ms", "BIGINT"),
    ("release_ts_ms", "BIGINT"),
    ("availability_ts_ms", "BIGINT"),
    ("source_record_id", "TEXT"),
    ("open_interest", "DOUBLE PRECISION"),
    ("commercial_long", "DOUBLE PRECISION"),
    ("commercial_short", "DOUBLE PRECISION"),
    ("commercial_spread", "DOUBLE PRECISION"),
    ("noncommercial_long", "DOUBLE PRECISION"),
    ("noncommercial_short", "DOUBLE PRECISION"),
    ("noncommercial_spread", "DOUBLE PRECISION"),
    ("producer_merchant_long", "DOUBLE PRECISION"),
    ("producer_merchant_short", "DOUBLE PRECISION"),
    ("producer_merchant_spread", "DOUBLE PRECISION"),
    ("swap_dealer_long", "DOUBLE PRECISION"),
    ("swap_dealer_short", "DOUBLE PRECISION"),
    ("swap_dealer_spread", "DOUBLE PRECISION"),
    ("managed_money_long", "DOUBLE PRECISION"),
    ("managed_money_short", "DOUBLE PRECISION"),
    ("managed_money_spread", "DOUBLE PRECISION"),
    ("other_reportable_long", "DOUBLE PRECISION"),
    ("other_reportable_short", "DOUBLE PRECISION"),
    ("other_reportable_spread", "DOUBLE PRECISION"),
    ("nonreportable_long", "DOUBLE PRECISION"),
    ("nonreportable_short", "DOUBLE PRECISION"),
    ("ingested_ts_ms", "BIGINT"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)

COT_SYMBOL_FEATURE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("symbol", "TEXT"),
    ("asof_ts_ms", "BIGINT"),
    ("cot_commercial_net_pctile_3y", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("cot_noncomm_net_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("cot_noncomm_extreme_flag", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("cot_open_interest_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
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
        CREATE TABLE IF NOT EXISTS cftc_cot_positions (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            report_type TEXT NOT NULL,
            contract_key TEXT NOT NULL,
            market_and_exchange_names TEXT,
            contract_market_name TEXT,
            cftc_contract_market_code TEXT,
            report_date TEXT NOT NULL,
            report_ts_ms BIGINT NOT NULL,
            release_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            source_record_id TEXT NOT NULL,
            open_interest DOUBLE PRECISION,
            commercial_long DOUBLE PRECISION,
            commercial_short DOUBLE PRECISION,
            commercial_spread DOUBLE PRECISION,
            noncommercial_long DOUBLE PRECISION,
            noncommercial_short DOUBLE PRECISION,
            noncommercial_spread DOUBLE PRECISION,
            producer_merchant_long DOUBLE PRECISION,
            producer_merchant_short DOUBLE PRECISION,
            producer_merchant_spread DOUBLE PRECISION,
            swap_dealer_long DOUBLE PRECISION,
            swap_dealer_short DOUBLE PRECISION,
            swap_dealer_spread DOUBLE PRECISION,
            managed_money_long DOUBLE PRECISION,
            managed_money_short DOUBLE PRECISION,
            managed_money_spread DOUBLE PRECISION,
            other_reportable_long DOUBLE PRECISION,
            other_reportable_short DOUBLE PRECISION,
            other_reportable_spread DOUBLE PRECISION,
            nonreportable_long DOUBLE PRECISION,
            nonreportable_short DOUBLE PRECISION,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "cftc_cot_positions", CFTC_COT_POSITION_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_cftc_cot_positions_source_record_id
          ON cftc_cot_positions(source_record_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cftc_cot_positions_contract_avail
          ON cftc_cot_positions(contract_key, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cftc_cot_positions_contract_report
          ON cftc_cot_positions(contract_key, report_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cot_contract_symbol_map (
            contract_key TEXT NOT NULL,
            symbol TEXT NOT NULL,
            topic TEXT,
            weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(contract_key, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cot_contract_symbol_map_symbol
          ON cot_contract_symbol_map(symbol, active)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cot_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            cot_commercial_net_pctile_3y DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            cot_noncomm_net_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            cot_noncomm_extreme_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            cot_open_interest_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )
    _add_columns(conn, "cot_symbol_features", COT_SYMBOL_FEATURE_COLUMNS)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cot_symbol_features_symbol_asof
          ON cot_symbol_features(symbol, asof_ts_ms DESC)
        """
    )
