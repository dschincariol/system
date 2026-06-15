"""Ensure news event feature payload storage exists on Postgres."""

from __future__ import annotations

id = 49
description = "news event feature payload_json contract"


def up(conn) -> None:
    conn.execute("ALTER TABLE news_event_features ADD COLUMN IF NOT EXISTS payload_json JSONB")
