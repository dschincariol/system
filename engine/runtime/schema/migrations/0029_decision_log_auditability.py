"""Decision log auditability columns."""

from __future__ import annotations

id = 29
description = "decision log model version and component vector"


def up(conn) -> None:
    conn.execute(
        "ALTER TABLE IF EXISTS decision_log ADD COLUMN IF NOT EXISTS model_version TEXT NULL"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS decision_log ADD COLUMN IF NOT EXISTS components_json JSONB NULL"
    )
