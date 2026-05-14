"""Stacked ensemble OOS prediction and ridge weight tables."""

from __future__ import annotations

id = 10
description = "stacked ensemble oos predictions and ridge weights"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_oos_predictions (
            symbol TEXT NOT NULL,
            horizon BIGINT NOT NULL,
            family TEXT NOT NULL,
            ts BIGINT NOT NULL,
            prediction DOUBLE PRECISION NOT NULL,
            target DOUBLE PRECISION NULL,
            PRIMARY KEY(symbol, horizon, family, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_oos_predictions_lookup
          ON model_oos_predictions(symbol, horizon, ts)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_oos_predictions_family_ts
          ON model_oos_predictions(family, ts)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ensemble_weights (
            symbol TEXT NOT NULL,
            horizon BIGINT NOT NULL,
            ts BIGINT NOT NULL,
            weights_json TEXT NOT NULL,
            intercept DOUBLE PRECISION NOT NULL DEFAULT 0,
            alpha DOUBLE PRECISION NOT NULL DEFAULT 0,
            n_train_obs BIGINT NOT NULL DEFAULT 0,
            val_metric DOUBLE PRECISION NULL,
            PRIMARY KEY(symbol, horizon, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ensemble_weights_lookup
          ON ensemble_weights(symbol, horizon, ts DESC)
        """
    )
