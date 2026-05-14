"""Compatibility defaults for legacy alert and attribution inserts."""

from __future__ import annotations

id = 25
description = "alert prediction lineage and legacy insert defaults"


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM pg_attribute a
        JOIN pg_class c
          ON c.oid = a.attrelid
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relname = ?
          AND a.attname = ?
          AND NOT a.attisdropped
        """,
        (str(table_name), str(column_name)),
    ).fetchone()
    return bool(row)


def up(conn) -> None:
    conn.execute("ALTER TABLE IF EXISTS alerts ADD COLUMN IF NOT EXISTS prediction_id BIGINT")
    if _column_exists(conn, "alerts", "dedupe_key"):
        conn.execute(
            "ALTER TABLE alerts ALTER COLUMN dedupe_key "
            "SET DEFAULT ('auto:' || md5(random()::text || clock_timestamp()::text))"
        )
    if _column_exists(conn, "trade_attribution_ledger", "row_hash"):
        conn.execute(
            "ALTER TABLE trade_attribution_ledger ALTER COLUMN row_hash "
            "SET DEFAULT decode(md5(random()::text || clock_timestamp()::text), 'hex')"
        )
