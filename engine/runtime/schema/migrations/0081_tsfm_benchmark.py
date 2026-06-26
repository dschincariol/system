"""Governed TSFM benchmark and shadow risk-input evidence."""

from __future__ import annotations

id = 81
description = "time-series foundation model benchmark evidence"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tsfm_benchmark_runs (
            run_id TEXT PRIMARY KEY,
            created_ts_ms BIGINT NOT NULL,
            updated_ts_ms BIGINT NOT NULL,
            status TEXT NOT NULL,
            stage TEXT NOT NULL DEFAULT 'shadow',
            config_json JSONB NOT NULL,
            artifact_alias TEXT,
            artifact_sha256 TEXT,
            summary_json JSONB NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tsfm_benchmark_rows (
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_class TEXT NOT NULL,
            task TEXT NOT NULL,
            family TEXT NOT NULL,
            row_kind TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            target_ts_ms BIGINT NOT NULL,
            horizon_s BIGINT NOT NULL,
            prediction DOUBLE PRECISION,
            target DOUBLE PRECISION,
            abs_error DOUBLE PRECISION,
            squared_error DOUBLE PRECISION,
            quantiles_json JSONB,
            horizon_path_json JSONB,
            feature_snapshot_json JSONB,
            latency_ms DOUBLE PRECISION,
            resource_json JSONB,
            provenance_json JSONB,
            status TEXT NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(run_id, symbol, task, family, row_kind, ts_ms, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tsfm_benchmark_rows_symbol_ts
          ON tsfm_benchmark_rows(symbol, task, ts_ms)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tsfm_benchmark_rows_family_status
          ON tsfm_benchmark_rows(family, status, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tsfm_risk_inputs (
            run_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            target_ts_ms BIGINT NOT NULL,
            horizon_s BIGINT NOT NULL,
            adapter TEXT NOT NULL,
            risk_input_kind TEXT NOT NULL,
            value DOUBLE PRECISION NOT NULL,
            stage TEXT NOT NULL DEFAULT 'shadow',
            provenance_json JSONB NOT NULL,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(run_id, symbol, ts_ms, horizon_s, adapter, risk_input_kind)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tsfm_risk_inputs_symbol_ts
          ON tsfm_risk_inputs(symbol, risk_input_kind, ts_ms)
        """
    )
