"""Automated feature discovery candidate and registry tables."""

from __future__ import annotations

id = 11
description = "automated feature discovery tables"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_candidates (
            id BIGSERIAL PRIMARY KEY,
            ts BIGINT NOT NULL,
            source TEXT NOT NULL,
            symbol TEXT NOT NULL,
            expression TEXT NOT NULL,
            params_json TEXT NOT NULL,
            hash TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feature_candidates_source_symbol
          ON feature_candidates(source, symbol, ts)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_evaluation (
            candidate_id BIGINT NOT NULL,
            ts BIGINT NOT NULL,
            t_stat DOUBLE PRECISION,
            p_value DOUBLE PRECISION,
            q_value DOUBLE PRECISION,
            oos_ic DOUBLE PRECISION,
            decision TEXT NOT NULL,
            PRIMARY KEY(candidate_id, ts)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feature_evaluation_decision_ts
          ON feature_evaluation(decision, ts)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_registry (
            feature_id TEXT PRIMARY KEY,
            stage TEXT NOT NULL DEFAULT 'shadow' CHECK(stage IN ('shadow', 'live')),
            source TEXT NOT NULL,
            expression TEXT NOT NULL,
            params_json TEXT NOT NULL,
            hash TEXT NOT NULL UNIQUE,
            created_ts BIGINT NOT NULL,
            accepted_candidate_id BIGINT,
            metadata_json TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feature_registry_stage_source
          ON feature_registry(stage, source)
        """
    )
