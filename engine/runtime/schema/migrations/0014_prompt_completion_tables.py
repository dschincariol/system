"""Prompt completion tables for statistical evidence, CPCV, tuning, and RL."""

from __future__ import annotations

id = 14
description = "prompt completion support tables"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS promotion_statistical_evidence (
            id BIGSERIAL PRIMARY KEY,
            ts BIGINT NOT NULL,
            model_id TEXT NOT NULL,
            feature_id TEXT NULL,
            test_name TEXT NOT NULL,
            t_stat DOUBLE PRECISION NULL,
            p_value DOUBLE PRECISION NULL,
            q_value DOUBLE PRECISION NULL,
            bootstrap_samples BIGINT NULL,
            decision TEXT NOT NULL,
            payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            prev_hash BYTEA NULL,
            row_hash BYTEA NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_promotion_statistical_evidence_model_ts_desc
          ON promotion_statistical_evidence(model_id, ts DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_promotion_statistical_evidence_decision_ts
          ON promotion_statistical_evidence(decision, ts DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_cpcv_runs (
            id BIGSERIAL PRIMARY KEY,
            created_ts BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
            ts BIGINT NULL,
            model_name TEXT NULL,
            candidate_version TEXT NULL,
            model_id TEXT NULL,
            n_splits BIGINT NULL,
            n_test_splits BIGINT NULL,
            embargo_pct DOUBLE PRECISION NULL,
            n_paths BIGINT NULL,
            path_index BIGINT NULL,
            path_returns JSONB NULL,
            path_sharpes JSONB NULL,
            mean_sharpe DOUBLE PRECISION NULL,
            median_sharpe DOUBLE PRECISION NULL,
            pbo DOUBLE PRECISION NULL,
            sharpe DOUBLE PRECISION NULL,
            deflated_sharpe DOUBLE PRECISION NULL,
            n_trials BIGINT NULL,
            total_return DOUBLE PRECISION NULL,
            max_drawdown DOUBLE PRECISION NULL,
            cfg JSONB NULL,
            payload JSONB NULL,
            diagnostics JSONB NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_cpcv_runs_model_candidate_created
          ON backtest_cpcv_runs(model_name, candidate_version, created_ts DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_cpcv_runs_model_id_path
          ON backtest_cpcv_runs(model_id, path_index)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_cpcv_path_results (
            id BIGSERIAL PRIMARY KEY,
            ts BIGINT NOT NULL,
            model_id TEXT NOT NULL,
            cfg JSONB NULL,
            path_index BIGINT NOT NULL,
            sharpe DOUBLE PRECISION NOT NULL,
            deflated_sharpe DOUBLE PRECISION NULL,
            n_trials BIGINT NULL,
            total_return DOUBLE PRECISION NULL,
            max_drawdown DOUBLE PRECISION NULL,
            payload JSONB NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_backtest_cpcv_path_results_model_ts
          ON backtest_cpcv_path_results(model_id, ts DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_hyperparameter_registry (
            id BIGSERIAL PRIMARY KEY,
            ts BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT,
            model_name TEXT NOT NULL,
            model_family TEXT NOT NULL,
            tuner TEXT NOT NULL,
            objective TEXT NOT NULL,
            metric_value DOUBLE PRECISION NOT NULL,
            params JSONB NOT NULL,
            study_name TEXT NOT NULL,
            trial_count BIGINT NOT NULL DEFAULT 0,
            best_trial_number BIGINT NOT NULL DEFAULT 0,
            cpcv_mean_sharpe DOUBLE PRECISION NULL,
            cpcv_median_sharpe DOUBLE PRECISION NULL,
            cpcv_pbo DOUBLE PRECISION NULL,
            diagnostics JSONB NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_hparam_registry_family_ts
          ON model_hyperparameter_registry(model_family, ts DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS model_best_params (
            model_family TEXT NOT NULL,
            symbol TEXT NOT NULL,
            ts BIGINT NOT NULL,
            study_name TEXT NOT NULL,
            params_json JSONB NOT NULL,
            value DOUBLE PRECISION NOT NULL,
            trial_number BIGINT NULL,
            seed BIGINT NULL,
            PRIMARY KEY(model_family, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_model_best_params_ts
          ON model_best_params(ts DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rl_training_runs (
            id BIGSERIAL PRIMARY KEY,
            ts BIGINT NOT NULL,
            algo TEXT NOT NULL,
            config_json JSONB NOT NULL,
            total_steps BIGINT NOT NULL,
            eval_reward DOUBLE PRECISION NULL,
            artifact_path TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rl_training_runs_ts
          ON rl_training_runs(ts)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_rl_training_runs_algo_ts
          ON rl_training_runs(algo, ts)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rl_shadow_decisions (
            ts BIGINT NOT NULL,
            symbol TEXT NOT NULL,
            live_weight DOUBLE PRECISION NOT NULL,
            rl_weight DOUBLE PRECISION NOT NULL,
            delta DOUBLE PRECISION NOT NULL,
            obs_hash TEXT NOT NULL,
            PRIMARY KEY(ts, symbol)
        )
        """
    )
