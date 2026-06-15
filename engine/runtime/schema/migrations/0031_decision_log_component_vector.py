"""Decision log component vector audit column."""

from __future__ import annotations

id = 31
description = "decision log component vector column"


def up(conn) -> None:
    conn.execute(
        "ALTER TABLE IF EXISTS decision_log ADD COLUMN IF NOT EXISTS component_vector JSONB NULL"
    )
