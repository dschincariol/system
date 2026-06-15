"""ADWIN residual drift state for champion models."""

from __future__ import annotations

id = 46
description = "adwin champion residual drift state"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS champion_residual_adwin_state (
            model_name TEXT NOT NULL,
            family TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL,
            horizon_s BIGINT NOT NULL,
            delta DOUBLE PRECISION NOT NULL DEFAULT 0.002,
            window_json JSONB NOT NULL DEFAULT '[]',
            n_seen BIGINT NOT NULL DEFAULT 0,
            n_detections BIGINT NOT NULL DEFAULT 0,
            last_decision_ts_ms BIGINT NOT NULL DEFAULT 0,
            width BIGINT NOT NULL DEFAULT 0,
            mean DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            updated_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(model_name, symbol, horizon_s)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_champion_residual_adwin_symbol
          ON champion_residual_adwin_state(symbol, horizon_s, updated_ts_ms DESC)
        """
    )
