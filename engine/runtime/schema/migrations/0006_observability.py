"""Indexes for dashboard-grade runtime observability lookups."""

from __future__ import annotations

id = 6
description = "observability runtime metrics indexes"


def _column_exists(conn, table: str, column: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
          AND column_name = ?
        LIMIT 1
        """,
        (str(table), str(column)),
    ).fetchone()
    return bool(row)


def up(conn) -> None:
    if _column_exists(conn, "runtime_metrics", "metric") and _column_exists(conn, "runtime_metrics", "ts_ms"):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_runtime_metrics_metric_ts_desc
              ON runtime_metrics(metric, ts_ms DESC)
            """
        )
    if _column_exists(conn, "runtime_metrics", "metric_name") and _column_exists(conn, "runtime_metrics", "ts"):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_runtime_metrics_metric_name_ts_desc
              ON runtime_metrics(metric_name, ts DESC)
            """
        )
