"""Add alert lifecycle, shelving, and acknowledgement expiry state."""

from __future__ import annotations

id = 54
description = "alert lifecycle shelving and ack expiry"


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS alert_acks ADD COLUMN IF NOT EXISTS expires_ts_ms BIGINT")
    conn.execute("ALTER TABLE IF EXISTS alert_acks ADD COLUMN IF NOT EXISTS reason TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_shelves (
          alert_id BIGINT PRIMARY KEY,
          shelved_ts_ms BIGINT NOT NULL,
          expires_ts_ms BIGINT NOT NULL,
          shelved_by TEXT,
          reason TEXT NOT NULL,
          source TEXT,
          severity TEXT,
          detail_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_lifecycle_events (
          id BIGSERIAL PRIMARY KEY,
          alert_id BIGINT NOT NULL,
          ts_ms BIGINT NOT NULL,
          lifecycle_state TEXT NOT NULL,
          actor TEXT,
          reason TEXT,
          source TEXT,
          detail_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alert_lifecycle_events_alert_ts
          ON alert_lifecycle_events(alert_id, ts_ms DESC)
        """
    )
