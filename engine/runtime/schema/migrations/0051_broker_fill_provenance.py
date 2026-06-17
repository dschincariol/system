"""Add broker fill provenance columns for real-vs-shadow isolation."""

from __future__ import annotations

id = 51
description = "broker fill source and book provenance"


def _columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name = ?
        """,
        (str(table_name),),
    ).fetchall()
    return {str(row[0]) for row in rows or []}


def _ensure_table(conn, table_name: str, index_name: str) -> None:
    conn.execute(
        f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'real'"
    )
    conn.execute(f"ALTER TABLE IF EXISTS {table_name} ADD COLUMN IF NOT EXISTS book_key TEXT")
    row = conn.execute("SELECT to_regclass(?)", (str(table_name),)).fetchone()
    if not row or row[0] is None:
        return
    cols = _columns(conn, table_name)
    ts_col = "ts_ms" if "ts_ms" in cols else ("fill_ts_ms" if "fill_ts_ms" in cols else "")
    if "symbol" not in cols or not ts_col:
        return
    conn.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {index_name}
          ON {table_name}(source, book_key, symbol, {ts_col})
        """
    )


def up(conn) -> None:
    _ensure_table(conn, "broker_fills", "idx_broker_fills_source_book_ts")
    _ensure_table(conn, "broker_fills_v2", "idx_broker_fills_v2_source_book_ts")
