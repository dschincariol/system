"""Live ingestion coordination tables."""

from __future__ import annotations

id = 30
description = "live ingestion coordination tables"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_feed_lock (
          id INTEGER PRIMARY KEY,
          owner TEXT NOT NULL DEFAULT '',
          pid BIGINT NOT NULL DEFAULT 0,
          ts_ms BIGINT NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute("ALTER TABLE IF EXISTS price_feed_lock ADD COLUMN IF NOT EXISTS owner TEXT NOT NULL DEFAULT ''")
    conn.execute("ALTER TABLE IF EXISTS price_feed_lock ADD COLUMN IF NOT EXISTS pid BIGINT NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE IF EXISTS price_feed_lock ADD COLUMN IF NOT EXISTS ts_ms BIGINT NOT NULL DEFAULT 0")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS options_symbol_ingestion_state (
          symbol TEXT NOT NULL PRIMARY KEY,
          provider TEXT NOT NULL DEFAULT '',
          consecutive_failures BIGINT NOT NULL DEFAULT 0,
          total_failures BIGINT NOT NULL DEFAULT 0,
          last_failure_ts_ms BIGINT,
          last_failure_error TEXT,
          last_success_ts_ms BIGINT,
          last_fresh_snapshot_ts_ms BIGINT,
          last_cached_snapshot_ts_ms BIGINT,
          last_fallback_ts_ms BIGINT,
          last_row_count BIGINT NOT NULL DEFAULT 0,
          disabled_until_ts_ms BIGINT NOT NULL DEFAULT 0,
          updated_ts_ms BIGINT NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT ''"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS consecutive_failures BIGINT NOT NULL DEFAULT 0"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS total_failures BIGINT NOT NULL DEFAULT 0"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS last_failure_ts_ms BIGINT"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS last_failure_error TEXT"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS last_success_ts_ms BIGINT"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS last_fresh_snapshot_ts_ms BIGINT"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS last_cached_snapshot_ts_ms BIGINT"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS last_fallback_ts_ms BIGINT"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS last_row_count BIGINT NOT NULL DEFAULT 0"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS disabled_until_ts_ms BIGINT NOT NULL DEFAULT 0"
    )
    conn.execute(
        "ALTER TABLE IF EXISTS options_symbol_ingestion_state ADD COLUMN IF NOT EXISTS updated_ts_ms BIGINT NOT NULL DEFAULT 0"
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_options_symbol_ingestion_disabled
          ON options_symbol_ingestion_state(disabled_until_ts_ms)
        """
    )
