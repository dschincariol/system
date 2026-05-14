"""Causal diagnostics score and curated DAG tables."""

from __future__ import annotations

id = 13
description = "causal diagnostics scores and curated dags"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS causal_scores (
            feature TEXT NOT NULL,
            target TEXT NOT NULL,
            "window" TEXT NOT NULL,
            ts BIGINT NOT NULL,
            granger_p DOUBLE PRECISION NOT NULL,
            granger_lag BIGINT NOT NULL,
            dowhy_effect DOUBLE PRECISION NULL,
            dowhy_p DOUBLE PRECISION NULL,
            score DOUBLE PRECISION NOT NULL,
            decision TEXT NOT NULL,
            PRIMARY KEY(feature, target, "window", ts)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_causal_scores_latest
          ON causal_scores(feature, target, "window", ts)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS causal_dags (
            name TEXT PRIMARY KEY,
            dag_json TEXT NOT NULL,
            created_ts BIGINT NOT NULL
        )
        """
    )
