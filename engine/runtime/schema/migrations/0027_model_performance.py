"""Model performance history used by ensemble weighting and scoring."""

from __future__ import annotations

id = 27
description = "model performance history table"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_performance (
            id BIGSERIAL PRIMARY KEY,
            tracked_prediction_id BIGINT,
            prediction_id BIGINT,
            outcome_id BIGINT,
            "time" BIGINT NOT NULL,
            prediction_time BIGINT,
            symbol TEXT,
            model_id TEXT,
            model_name TEXT,
            model_version TEXT,
            horizon_s BIGINT,
            prediction DOUBLE PRECISION,
            realized_return DOUBLE PRECISION,
            error DOUBLE PRECISION,
            directional_accuracy BIGINT,
            pnl_impact DOUBLE PRECISION,
            rolling_score DOUBLE PRECISION,
            regime_time_ms BIGINT,
            volatility_regime TEXT NOT NULL DEFAULT 'unknown',
            trend_regime TEXT NOT NULL DEFAULT 'unknown',
            liquidity_regime TEXT NOT NULL DEFAULT 'unknown',
            metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_ts_ms BIGINT,
            updated_ts_ms BIGINT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_performance_identity_time
          ON model_performance(model_name, model_version, symbol, "time" DESC, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_performance_model_id_time
          ON model_performance(model_id, symbol, "time" DESC, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_performance_regime_time
          ON model_performance(
            model_name, model_version, symbol,
            volatility_regime, trend_regime, liquidity_regime,
            "time" DESC, id DESC
          )
        """
    )
