"""Runtime lock expiry and walk-forward registry compatibility columns."""

from __future__ import annotations

id = 26
description = "runtime lock expiry and walk-forward registry columns"


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS job_locks ADD COLUMN IF NOT EXISTS expires_ms BIGINT")
    conn.execute("ALTER TABLE IF EXISTS walk_forward_runs ADD COLUMN IF NOT EXISTS model_selection_json JSONB")
    conn.execute("ALTER TABLE IF EXISTS walk_forward_scores ADD COLUMN IF NOT EXISTS model_name TEXT")
    conn.execute("ALTER TABLE IF EXISTS walk_forward_scores ADD COLUMN IF NOT EXISTS model_version TEXT")
    conn.execute("ALTER TABLE IF EXISTS walk_forward_scores ADD COLUMN IF NOT EXISTS model_kind TEXT")
