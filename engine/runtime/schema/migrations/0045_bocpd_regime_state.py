"""BOCPD regime changepoint posterior summaries."""

from __future__ import annotations

id = 45
description = "bocpd regime changepoint summaries"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bocpd_regime_state (
            series_key TEXT NOT NULL,
            series_type TEXT NOT NULL,
            symbol TEXT NOT NULL DEFAULT '*',
            ts_ms BIGINT NOT NULL,
            cp_prob_5d DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            map_run_length BIGINT NOT NULL DEFAULT 0,
            expected_run_length DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            run_length_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            active_states BIGINT NOT NULL DEFAULT 0,
            n_obs BIGINT NOT NULL DEFAULT 0,
            posterior_json JSONB,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(series_key, ts_ms)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bocpd_regime_state_symbol_ts
          ON bocpd_regime_state(symbol, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bocpd_regime_state_type_ts
          ON bocpd_regime_state(series_type, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bocpd_ensemble_triggers (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s BIGINT NOT NULL,
            cp_prob_5d DOUBLE PRECISION NOT NULL,
            threshold DOUBLE PRECISION NOT NULL,
            mode TEXT NOT NULL,
            base_window BIGINT NOT NULL,
            effective_window BIGINT NOT NULL,
            series_key TEXT,
            meta_json JSONB
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_bocpd_ensemble_triggers_symbol_ts
          ON bocpd_ensemble_triggers(symbol, horizon_s, ts_ms DESC)
        """
    )
