"""Add explicit policy metadata columns used by size-policy readers."""

from __future__ import annotations

id = 24
description = "size policy metadata columns"


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS size_policy ADD COLUMN IF NOT EXISTS lookback_days BIGINT NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE IF EXISTS size_policy ADD COLUMN IF NOT EXISTS buckets BIGINT NOT NULL DEFAULT 0")
