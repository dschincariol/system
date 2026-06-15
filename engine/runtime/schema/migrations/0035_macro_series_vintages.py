"""Vintage-aware macro series storage for ALFRED/FRED observations."""

from __future__ import annotations

id = 35
description = "macro series vintages and backfill state"


MACRO_SERIES_VINTAGE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("series_id", "TEXT"),
    ("obs_date", "TEXT"),
    ("obs_ts_ms", "BIGINT"),
    ("vintage_date", "TEXT"),
    ("vintage_ts_ms", "BIGINT"),
    ("realtime_end", "TEXT"),
    ("value", "DOUBLE PRECISION"),
    ("availability_ts_ms", "BIGINT"),
    ("source", "TEXT"),
    ("ingested_ts_ms", "BIGINT"),
    ("payload_json", "JSONB"),
    ("diagnostics_json", "JSONB"),
)

MACRO_VINTAGE_BACKFILL_STATE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("series_id", "TEXT"),
    ("status", "TEXT"),
    ("last_vintage_date", "TEXT"),
    ("updated_ts_ms", "BIGINT"),
    ("cursor_json", "JSONB"),
    ("error", "TEXT"),
)


def _add_columns(conn, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    for column_name, column_type in columns:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS macro_series_vintages (
            id BIGSERIAL PRIMARY KEY,
            series_id TEXT NOT NULL,
            obs_date TEXT NOT NULL,
            obs_ts_ms BIGINT,
            vintage_date TEXT NOT NULL,
            vintage_ts_ms BIGINT,
            realtime_end TEXT,
            value DOUBLE PRECISION,
            availability_ts_ms BIGINT NOT NULL,
            source TEXT,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    _add_columns(conn, "macro_series_vintages", MACRO_SERIES_VINTAGE_COLUMNS)
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_vintages_series_obs_vintage
          ON macro_series_vintages(series_id, obs_date, vintage_date)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_macro_series_vintages_series_availability
          ON macro_series_vintages(series_id, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_macro_series_vintages_series_obs
          ON macro_series_vintages(series_id, obs_ts_ms DESC)
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS macro_vintage_backfill_state (
            series_id TEXT PRIMARY KEY,
            status TEXT,
            last_vintage_date TEXT,
            updated_ts_ms BIGINT,
            cursor_json JSONB,
            error TEXT
        )
        """
    )
    _add_columns(conn, "macro_vintage_backfill_state", MACRO_VINTAGE_BACKFILL_STATE_COLUMNS)
