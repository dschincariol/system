"""GARCH-family volatility forecasts for risk sizing."""

from __future__ import annotations

id = 80
description = "garch-family volatility forecast rows"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS garch_vol_forecasts (
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            asof_ts_ms BIGINT,
            model_type TEXT NOT NULL,
            distribution TEXT NOT NULL,
            horizon_days BIGINT NOT NULL DEFAULT 1,
            return_source TEXT NOT NULL,
            trailing_vol DOUBLE PRECISION,
            forecast_rv_1d DOUBLE PRECISION NOT NULL,
            forecast_vol_1d DOUBLE PRECISION NOT NULL,
            forecast_ann_vol DOUBLE PRECISION NOT NULL,
            forecast_ratio DOUBLE PRECISION NOT NULL,
            n_obs BIGINT NOT NULL DEFAULT 0,
            n_train BIGINT NOT NULL DEFAULT 0,
            min_history BIGINT NOT NULL DEFAULT 120,
            converged BIGINT NOT NULL DEFAULT 0,
            convergence_status TEXT,
            loglikelihood DOUBLE PRECISION,
            aic DOUBLE PRECISION,
            bic DOUBLE PRECISION,
            fallback BIGINT NOT NULL DEFAULT 0,
            fallback_reason TEXT,
            diagnostics_json JSONB,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(symbol, ts_ms, model_type)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_garch_vol_forecasts_symbol_model_ts_desc
          ON garch_vol_forecasts(symbol, model_type, ts_ms DESC)
        """
    )
