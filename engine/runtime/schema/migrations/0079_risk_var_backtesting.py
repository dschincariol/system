"""Risk VaR/CVaR forecast and exception backtesting evidence."""

from __future__ import annotations

id = 79
description = "risk var backtesting evidence"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_var_forecasts (
          id BIGSERIAL PRIMARY KEY,
          forecast_id TEXT NOT NULL UNIQUE,
          forecast_ts_ms BIGINT NOT NULL,
          horizon_steps BIGINT NOT NULL,
          var_95 DOUBLE PRECISION NULL,
          var_99 DOUBLE PRECISION NULL,
          cvar_95 DOUBLE PRECISION NULL,
          cvar_99 DOUBLE PRECISION NULL,
          simulation_method TEXT NULL,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_ts_ms BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_var_forecasts_ts
          ON risk_var_forecasts(forecast_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_var_backtest_results (
          id BIGSERIAL PRIMARY KEY,
          forecast_id TEXT NOT NULL,
          forecast_ts_ms BIGINT NOT NULL,
          realized_ts_ms BIGINT NOT NULL,
          horizon_steps BIGINT NOT NULL,
          confidence_level DOUBLE PRECISION NOT NULL,
          var_value DOUBLE PRECISION NOT NULL,
          cvar_value DOUBLE PRECISION NULL,
          realized_portfolio_return DOUBLE PRECISION NOT NULL,
          realized_portfolio_loss DOUBLE PRECISION NOT NULL,
          exception BIGINT NOT NULL,
          kupiec_pof_stat DOUBLE PRECISION NULL,
          kupiec_pof_p_value DOUBLE PRECISION NULL,
          kupiec_pof_status TEXT NULL,
          christoffersen_ind_stat DOUBLE PRECISION NULL,
          christoffersen_ind_p_value DOUBLE PRECISION NULL,
          christoffersen_ind_status TEXT NULL,
          rolling_exception_rate DOUBLE PRECISION NULL,
          rolling_window BIGINT NULL,
          traffic_light_status TEXT NULL,
          traffic_light_reason TEXT NULL,
          metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_ts_ms BIGINT NOT NULL,
          UNIQUE(forecast_id, confidence_level)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_var_backtest_results_ts
          ON risk_var_backtest_results(forecast_ts_ms DESC, confidence_level)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_risk_var_backtest_results_status
          ON risk_var_backtest_results(traffic_light_status, forecast_ts_ms DESC)
        """
    )
