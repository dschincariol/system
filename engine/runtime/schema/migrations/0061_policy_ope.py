"""Doubly robust off-policy evaluation evidence tables."""

from __future__ import annotations

id = 61
description = "policy off-policy evaluation observations and evidence"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_ope_observations (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          candidate_key TEXT,
          model_id TEXT,
          model_name TEXT NOT NULL,
          candidate_type TEXT NOT NULL,
          candidate_version TEXT,
          symbol TEXT,
          horizon_s BIGINT NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          logged_action TEXT,
          target_action TEXT,
          behavior_propensity DOUBLE PRECISION,
          target_propensity DOUBLE PRECISION,
          outcome DOUBLE PRECISION,
          logged_model_estimate DOUBLE PRECISION,
          target_model_estimate DOUBLE PRECISION,
          source_table TEXT,
          source_id TEXT,
          meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          prev_hash BYTEA,
          row_hash BYTEA
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_candidate_ts
          ON policy_ope_observations(candidate_key, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_model_ts
          ON policy_ope_observations(model_id, model_name, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_scope_ts
          ON policy_ope_observations(symbol, horizon_s, regime, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_ope_evidence (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          candidate_key TEXT,
          model_id TEXT,
          model_name TEXT NOT NULL,
          candidate_type TEXT NOT NULL,
          candidate_version TEXT,
          symbol TEXT,
          horizon_s BIGINT NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          policy_value DOUBLE PRECISION,
          standard_error DOUBLE PRECISION,
          ci_lower DOUBLE PRECISION,
          ci_upper DOUBLE PRECISION,
          n_obs BIGINT NOT NULL DEFAULT 0,
          effective_n DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          support DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          max_importance_weight DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          confidence_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          decision TEXT NOT NULL,
          reason TEXT NOT NULL,
          config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          diagnostics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          prev_hash BYTEA,
          row_hash BYTEA
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_candidate_ts
          ON policy_ope_evidence(candidate_key, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_model_ts
          ON policy_ope_evidence(model_id, model_name, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_decision_ts
          ON policy_ope_evidence(decision, ts_ms DESC)
        """
    )
