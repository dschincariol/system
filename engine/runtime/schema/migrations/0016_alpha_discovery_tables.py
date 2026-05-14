"""Alpha discovery, drift retrain, and hypothesis persistence tables."""

from __future__ import annotations

id = 16
description = "alpha discovery persistence tables"

_NOW_MS_DEFAULT = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT"


def _add_column(conn, table: str, column: str, definition: str) -> None:
    conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hypothesis_registry (
          id BIGSERIAL PRIMARY KEY
        )
        """
    )
    _add_column(conn, "hypothesis_registry", "created_ts", f"BIGINT NOT NULL DEFAULT {_NOW_MS_DEFAULT}")
    _add_column(conn, "hypothesis_registry", "model_name", "TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "hypothesis_registry", "candidate_version", "TEXT")
    _add_column(conn, "hypothesis_registry", "n_observations", "BIGINT")
    _add_column(conn, "hypothesis_registry", "t_statistic", "DOUBLE PRECISION")
    _add_column(conn, "hypothesis_registry", "deflated_sharpe", "DOUBLE PRECISION")
    _add_column(conn, "hypothesis_registry", "threshold_t", "DOUBLE PRECISION")
    _add_column(conn, "hypothesis_registry", "n_competing_trials", "BIGINT")
    _add_column(conn, "hypothesis_registry", "passed", "BOOLEAN")
    _add_column(conn, "hypothesis_registry", "diagnostics", "JSONB NOT NULL DEFAULT '{}'::jsonb")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_hypothesis_registry_model_created
          ON hypothesis_registry(model_name, created_ts DESC, id DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alpha_candidates (
          id BIGSERIAL PRIMARY KEY
        )
        """
    )
    _add_column(conn, "alpha_candidates", "candidate_name", "TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "alpha_candidates", "candidate_version", "TEXT")
    _add_column(conn, "alpha_candidates", "model_family", "TEXT")
    _add_column(conn, "alpha_candidates", "feature_ids", "JSONB NOT NULL DEFAULT '[]'::jsonb")
    _add_column(conn, "alpha_candidates", "generation_method", "TEXT")
    _add_column(conn, "alpha_candidates", "hyperparams", "JSONB NOT NULL DEFAULT '{}'::jsonb")
    _add_column(conn, "alpha_candidates", "status", "TEXT NOT NULL DEFAULT 'generated'")
    _add_column(conn, "alpha_candidates", "diagnostics", "JSONB NOT NULL DEFAULT '{}'::jsonb")
    _add_column(conn, "alpha_candidates", "created_ts", f"BIGINT NOT NULL DEFAULT {_NOW_MS_DEFAULT}")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_name_created
          ON alpha_candidates(candidate_name, created_ts DESC, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alpha_candidates_status_created
          ON alpha_candidates(status, created_ts DESC, id DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alpha_lifecycle (
          id BIGSERIAL PRIMARY KEY
        )
        """
    )
    _add_column(conn, "alpha_lifecycle", "id", "BIGSERIAL")
    _add_column(conn, "alpha_lifecycle", "candidate_id", "BIGINT")
    _add_column(conn, "alpha_lifecycle", "stage", "TEXT")
    _add_column(conn, "alpha_lifecycle", "outcome", "TEXT")
    _add_column(conn, "alpha_lifecycle", "metrics", "JSONB NOT NULL DEFAULT '{}'::jsonb")
    _add_column(conn, "alpha_lifecycle", "notes", "JSONB NOT NULL DEFAULT '{}'::jsonb")
    _add_column(conn, "alpha_lifecycle", "created_ts", f"BIGINT NOT NULL DEFAULT {_NOW_MS_DEFAULT}")
    _add_column(conn, "alpha_lifecycle", "alert_id", "BIGINT")
    _add_column(conn, "alpha_lifecycle", "created_ts_ms", "BIGINT")
    _add_column(conn, "alpha_lifecycle", "expires_ts_ms", "BIGINT")
    _add_column(conn, "alpha_lifecycle", "half_life_ms", "BIGINT")
    _add_column(conn, "alpha_lifecycle", "volatility", "DOUBLE PRECISION")
    _add_column(conn, "alpha_lifecycle", "status", "TEXT")
    _add_column(conn, "alpha_lifecycle", "last_touch_ts_ms", "BIGINT")
    _add_column(conn, "alpha_lifecycle", "meta_json", "JSONB")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alpha_lifecycle_candidate_created
          ON alpha_lifecycle(candidate_id, created_ts DESC, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alpha_lifecycle_exp
          ON alpha_lifecycle(expires_ts_ms)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_alpha_lifecycle_alert_id
          ON alpha_lifecycle(alert_id)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS drift_retrain_events (
          id BIGSERIAL PRIMARY KEY
        )
        """
    )
    _add_column(conn, "drift_retrain_events", "created_ts", f"BIGINT NOT NULL DEFAULT {_NOW_MS_DEFAULT}")
    _add_column(conn, "drift_retrain_events", "model_name", "TEXT NOT NULL DEFAULT ''")
    _add_column(conn, "drift_retrain_events", "family", "TEXT")
    _add_column(conn, "drift_retrain_events", "trigger_type", "TEXT")
    _add_column(conn, "drift_retrain_events", "trigger_metrics", "JSONB NOT NULL DEFAULT '{}'::jsonb")
    _add_column(conn, "drift_retrain_events", "action_taken", "TEXT")
    _add_column(conn, "drift_retrain_events", "cooldown_applied", "BOOLEAN NOT NULL DEFAULT FALSE")
    _add_column(conn, "drift_retrain_events", "candidate_version", "TEXT")
    _add_column(conn, "drift_retrain_events", "outcome_status", "TEXT")
    _add_column(conn, "drift_retrain_events", "diagnostics", "JSONB NOT NULL DEFAULT '{}'::jsonb")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_drift_retrain_events_model_created
          ON drift_retrain_events(model_name, created_ts DESC, id DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_drift_retrain_events_family_created
          ON drift_retrain_events(family, created_ts DESC, id DESC)
        """
    )
