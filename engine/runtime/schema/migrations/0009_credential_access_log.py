"""Credential access audit log."""

from __future__ import annotations

from engine.runtime.schema.table_classification import hypertable_chunk_interval

id = 9
description = "credential access audit log"


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
          AND column_name = ?
        LIMIT 1
        """,
        (str(table_name), str(column_name)),
    ).fetchone()
    return bool(row)


def _timescale_available(conn) -> bool:
    row = conn.execute(
        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb' LIMIT 1"
    ).fetchone()
    return bool(row)


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS credential_access_log (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            name TEXT NOT NULL,
            pid INTEGER NOT NULL,
            service_name TEXT NOT NULL,
            host TEXT NOT NULL,
            provider TEXT NOT NULL,
            ok BOOLEAN NOT NULL,
            error TEXT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_credential_access_log_ts
          ON credential_access_log(ts DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_credential_access_log_name_ts
          ON credential_access_log(name, ts DESC)
        """
    )
    conn.execute(
        """
        ALTER TABLE IF EXISTS data_sources
          ADD COLUMN IF NOT EXISTS key_version TEXT NOT NULL DEFAULT 'master_key'
        """
    )

    if not _timescale_available(conn):
        return
    conn.execute("ALTER TABLE credential_access_log DROP CONSTRAINT IF EXISTS credential_access_log_pkey")
    conn.execute(
        """
        ALTER TABLE credential_access_log
          ADD CONSTRAINT credential_access_log_pkey PRIMARY KEY (id, ts)
        """
    )
    conn.execute(
        """
        SELECT create_hypertable(
          'credential_access_log'::regclass,
          'ts',
          chunk_time_interval => ?::interval,
          if_not_exists => TRUE,
          migrate_data => TRUE
        )
        """,
        (hypertable_chunk_interval("credential_access_log"),),
    )
    conn.execute(
        "SELECT set_chunk_time_interval('credential_access_log'::regclass, ?::interval)",
        (hypertable_chunk_interval("credential_access_log"),),
    )
    conn.execute(
        """
        SELECT add_retention_policy(
          'credential_access_log'::regclass,
          '1 year'::interval,
          if_not_exists => TRUE
        )
        """
    )
