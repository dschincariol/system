"""Operational ensemble diagnostics and blend tables."""

from __future__ import annotations

id = 17
description = "ensemble operational diagnostics tables"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ensemble_blend_weights (
            id BIGSERIAL PRIMARY KEY,
            created_ts BIGINT NOT NULL,
            mode TEXT NOT NULL,
            regime TEXT,
            weights_json TEXT NOT NULL,
            meta_blob BYTEA,
            meta_artifact_sha256 TEXT,
            meta_artifact_alias TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ensemble_blend_weights_mode_created
          ON ensemble_blend_weights(mode, regime, created_ts DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ensemble_predictions (
            id BIGSERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            ts BIGINT NOT NULL,
            blended_prediction DOUBLE PRECISION NOT NULL,
            family_preds_json TEXT NOT NULL,
            weights_json TEXT NOT NULL,
            agreement DOUBLE PRECISION NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ensemble_predictions_symbol_ts
          ON ensemble_predictions(symbol, ts DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ensemble_predictions_ts
          ON ensemble_predictions(ts DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ensemble_family_performance (
            id BIGSERIAL PRIMARY KEY,
            window_start_ts BIGINT NOT NULL,
            window_end_ts BIGINT NOT NULL,
            family TEXT NOT NULL,
            n_predictions BIGINT NOT NULL,
            realized_sharpe DOUBLE PRECISION,
            hit_rate DOUBLE PRECISION
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ensemble_family_performance_window
          ON ensemble_family_performance(window_end_ts DESC, family)
        """
    )
