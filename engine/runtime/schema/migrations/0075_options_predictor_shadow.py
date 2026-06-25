"""Add shadow options predictor forecast/intention table."""

from __future__ import annotations

id = 75
description = "options predictor shadow forecasts and intents"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS options_predictor_shadow (
          underlying TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          vrp_signal DOUBLE PRECISION,
          iv_forecast DOUBLE PRECISION,
          realized_vol DOUBLE PRECISION,
          confidence DOUBLE PRECISION,
          structure_json TEXT,
          evidence_gate_ok BOOLEAN,
          UNIQUE(underlying, ts_ms)
        )
        """
    )
