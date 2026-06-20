"""Indexes for unresolved model-scoring lookups."""

from __future__ import annotations

id = 63
description = "model scoring unresolved prediction indexes"


def _table_has_columns(conn, table_name: str, *column_names: str) -> bool:
    for column_name in column_names:
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
        if not row:
            return False
    return True


def _dedupe_model_performance_tracked_ids(conn) -> None:
    if not _table_has_columns(
        conn,
        "model_performance",
        "id",
        "tracked_prediction_id",
        "time",
        "created_ts_ms",
        "updated_ts_ms",
    ):
        return
    conn.execute(
        """
        DELETE FROM model_performance mp
        USING (
          SELECT id
          FROM (
            SELECT
              id,
              ROW_NUMBER() OVER (
                PARTITION BY tracked_prediction_id
                ORDER BY COALESCE(updated_ts_ms, created_ts_ms, "time", 0) DESC, id DESC
              ) AS rn
            FROM model_performance
            WHERE tracked_prediction_id IS NOT NULL
          ) ranked
          WHERE rn > 1
        ) duplicate_rows
        WHERE mp.id = duplicate_rows.id
        """
    )


def up(conn) -> None:
    if _table_has_columns(conn, "tracked_predictions", "prediction_id", "ts_ms", "id"):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tracked_predictions_prediction_id_ts_id
              ON tracked_predictions(prediction_id, ts_ms DESC, id DESC)
              WHERE prediction_id IS NOT NULL
            """
        )

    if _table_has_columns(conn, "model_performance", "prediction_id"):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_model_performance_prediction_id
              ON model_performance(prediction_id)
              WHERE prediction_id IS NOT NULL
            """
        )

    _dedupe_model_performance_tracked_ids(conn)
    if _table_has_columns(conn, "model_performance", "tracked_prediction_id"):
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_model_performance_tracked_prediction_id
              ON model_performance(tracked_prediction_id)
            """
        )
