"""Add broker configuration audit storage."""

from __future__ import annotations

id = 53
description = "broker config control-plane audit"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_config_audit (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          action TEXT NOT NULL,
          actor TEXT NOT NULL,
          active_broker TEXT,
          success BIGINT NOT NULL,
          message TEXT,
          detail_json JSONB NOT NULL DEFAULT '{}'::jsonb
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_broker_config_audit_ts
          ON broker_config_audit(ts_ms DESC)
        """
    )
