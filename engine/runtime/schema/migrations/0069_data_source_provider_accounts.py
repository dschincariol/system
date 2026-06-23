"""Add data-source provider account catalog table to existing Postgres schemas."""

from __future__ import annotations

id = 69
description = "data-source provider account catalog"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_source_provider_accounts (
          id BIGSERIAL PRIMARY KEY,
          account_key TEXT NOT NULL UNIQUE,
          display_name TEXT NOT NULL,
          provider_name TEXT,
          credentials_enc TEXT,
          key_version TEXT DEFAULT 'master_key',
          status TEXT,
          last_error TEXT,
          last_test_ts_ms BIGINT,
          config_hash TEXT,
          created_ts_ms BIGINT NOT NULL,
          updated_ts_ms BIGINT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_data_source_provider_accounts_provider
          ON data_source_provider_accounts(provider_name)
        """
    )
