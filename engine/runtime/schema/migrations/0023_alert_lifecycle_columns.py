"""Add portfolio lifecycle tracking columns to alerts."""

from __future__ import annotations

id = 23
description = "alert portfolio lifecycle columns"


def _table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = ?
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def up(conn) -> None:
    if not _table_exists(conn, "alerts"):
        return
    conn.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS portfolio_first_seen_ts_ms BIGINT NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS portfolio_last_seen_ts_ms BIGINT NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS portfolio_consumed_ts_ms BIGINT NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS portfolio_expired_ts_ms BIGINT NOT NULL DEFAULT 0")
    conn.execute("ALTER TABLE alerts ADD COLUMN IF NOT EXISTS portfolio_status TEXT NOT NULL DEFAULT 'new'")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_portfolio_status_ts "
        "ON alerts(portfolio_status, ts_ms)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_portfolio_expired_ts "
        "ON alerts(portfolio_expired_ts_ms)"
    )
