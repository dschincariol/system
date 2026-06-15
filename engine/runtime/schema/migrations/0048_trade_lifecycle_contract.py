"""Repair trade lifecycle persistence contract tables."""

from __future__ import annotations

id = 48
description = "trade lifecycle execution and marketplace schema contract"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS labels_exec (
          event_id BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s BIGINT NOT NULL,
          ts_ms BIGINT NOT NULL,
          source TEXT NOT NULL DEFAULT 'heuristic',
          realized BIGINT NOT NULL DEFAULT 0,
          side BIGINT NOT NULL,
          gross_ret DOUBLE PRECISION NOT NULL,
          net_ret DOUBLE PRECISION NOT NULL,
          gross_z DOUBLE PRECISION,
          net_z DOUBLE PRECISION,
          mid_in DOUBLE PRECISION,
          mid_out DOUBLE PRECISION,
          spread_in DOUBLE PRECISION,
          fees_bps DOUBLE PRECISION NOT NULL,
          slippage_bps DOUBLE PRECISION NOT NULL,
          spread_bps DOUBLE PRECISION NOT NULL,
          total_cost_bps DOUBLE PRECISION NOT NULL,
          extra_json JSONB,
          PRIMARY KEY (event_id, symbol, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_metrics (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s BIGINT NOT NULL,
          n BIGINT NOT NULL,
          metrics_json JSONB NOT NULL,
          UNIQUE(model_name, symbol, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_marketplace_scores (
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s BIGINT NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          stage TEXT NOT NULL DEFAULT 'challenger',
          score DOUBLE PRECISION NOT NULL DEFAULT 0,
          trades BIGINT NOT NULL DEFAULT 0,
          wins BIGINT NOT NULL DEFAULT 0,
          losses BIGINT NOT NULL DEFAULT 0,
          gross_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
          net_pnl DOUBLE PRECISION NOT NULL DEFAULT 0,
          avg_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
          last_signal_ts_ms BIGINT,
          updated_ts_ms BIGINT NOT NULL,
          meta_json JSONB,
          PRIMARY KEY (model_id, model_name, symbol, horizon_s, regime)
        )
        """
    )
    for column_name, ddl in (
        ("model_id", "TEXT NOT NULL DEFAULT 'baseline'"),
        ("model_name", "TEXT NOT NULL DEFAULT ''"),
        ("symbol", "TEXT NOT NULL DEFAULT ''"),
        ("horizon_s", "BIGINT NOT NULL DEFAULT 0"),
        ("regime", "TEXT NOT NULL DEFAULT 'global'"),
        ("stage", "TEXT NOT NULL DEFAULT 'challenger'"),
        ("score", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("trades", "BIGINT NOT NULL DEFAULT 0"),
        ("wins", "BIGINT NOT NULL DEFAULT 0"),
        ("losses", "BIGINT NOT NULL DEFAULT 0"),
        ("gross_pnl", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("net_pnl", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("avg_confidence", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
        ("last_signal_ts_ms", "BIGINT"),
        ("updated_ts_ms", "BIGINT NOT NULL DEFAULT 0"),
        ("meta_json", "JSONB"),
    ):
        conn.execute(
            f"ALTER TABLE IF EXISTS model_marketplace_scores ADD COLUMN IF NOT EXISTS {column_name} {ddl}"
        )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_model_marketplace_scores_key
          ON model_marketplace_scores(model_id, model_name, symbol, horizon_s, regime)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_marketplace_stage_score
          ON model_marketplace_scores(stage, score DESC, updated_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_marketplace_symbol_horizon
          ON model_marketplace_scores(symbol, horizon_s, score DESC)
        """
    )
