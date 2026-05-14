"""Inference runtime persistence tables and regime metadata columns."""

from __future__ import annotations

id = 15
description = "inference runtime persistence tables"


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        (str(table),),
    ).fetchone()
    return bool(row)


def _add_column(conn, table: str, column: str, definition: str) -> None:
    if not _table_exists(conn, table):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")


def up(conn) -> None:
    for table in ("predictions", "prediction_history"):
        _add_column(conn, table, "regime_time_ms", "BIGINT")
        _add_column(conn, table, "volatility_regime", "TEXT")
        _add_column(conn, table, "trend_regime", "TEXT")
        _add_column(conn, table, "liquidity_regime", "TEXT")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_state (
          time BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          volatility_regime TEXT NOT NULL,
          trend_regime TEXT NOT NULL,
          liquidity_regime TEXT NOT NULL,
          created_ts_ms BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
          PRIMARY KEY(symbol, time)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_regime_state_symbol_time_desc
          ON regime_state(symbol, time DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_model_registry (
          model_name TEXT NOT NULL,
          version TEXT NOT NULL,
          created_ts_ms BIGINT NOT NULL,
          updated_ts_ms BIGINT NOT NULL,
          metadata_json TEXT NOT NULL DEFAULT '{}',
          PRIMARY KEY(model_name, version)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_model_registry_updated
          ON tracked_model_registry(updated_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_predictions (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          symbol TEXT NOT NULL,
          model_name TEXT NOT NULL,
          model_version TEXT NOT NULL,
          prediction DOUBLE PRECISION NOT NULL,
          confidence DOUBLE PRECISION NOT NULL,
          features_version TEXT NOT NULL,
          event_id BIGINT,
          horizon_s BIGINT,
          prediction_id BIGINT,
          source_alert_id BIGINT,
          model_id TEXT,
          tracking_source TEXT,
          metadata_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_predictions_ts
          ON tracked_predictions(ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_predictions_symbol_ts
          ON tracked_predictions(symbol, ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_explanations (
          id BIGSERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          ts BIGINT NOT NULL,
          model_family TEXT NOT NULL,
          model_name TEXT,
          version TEXT,
          explanation_type TEXT NOT NULL,
          top_features JSONB,
          base_value DOUBLE PRECISION,
          diagnostics JSONB,
          created_ts BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_prediction_explanations_symbol_ts
          ON prediction_explanations(symbol, ts DESC)
        """
    )
