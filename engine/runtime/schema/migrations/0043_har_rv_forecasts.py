"""HAR-RV realized-volatility forecasts for sizing and stress inputs."""

from __future__ import annotations

id = 43
description = "har-rv volatility forecast rows"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS har_rv_forecasts (
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            asof_ts_ms BIGINT,
            rv DOUBLE PRECISION,
            trailing_vol DOUBLE PRECISION,
            forecast_rv_1d DOUBLE PRECISION NOT NULL,
            forecast_vol_1d DOUBLE PRECISION NOT NULL,
            forecast_ann_vol DOUBLE PRECISION NOT NULL,
            forecast_ratio DOUBLE PRECISION NOT NULL,
            intercept DOUBLE PRECISION,
            beta_daily DOUBLE PRECISION,
            beta_weekly DOUBLE PRECISION,
            beta_monthly DOUBLE PRECISION,
            n_obs BIGINT NOT NULL DEFAULT 0,
            n_train BIGINT NOT NULL DEFAULT 0,
            min_history BIGINT NOT NULL DEFAULT 60,
            source TEXT NOT NULL,
            fallback BIGINT NOT NULL DEFAULT 0,
            diagnostics_json JSONB,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(symbol, ts_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_har_rv_forecasts_symbol_ts_desc
          ON har_rv_forecasts(symbol, ts_ms DESC)
        """
    )
