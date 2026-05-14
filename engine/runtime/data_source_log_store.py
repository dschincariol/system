"""Owner module for data-source log storage paths."""

from __future__ import annotations

from typing import Any

from engine.runtime.storage import run_write_txn


def ensure_data_source_logs_schema(con: Any) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS data_source_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          source_key TEXT NOT NULL,
          level TEXT NOT NULL,
          event_type TEXT NOT NULL,
          message TEXT,
          detail_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_data_source_logs_source_ts
          ON data_source_logs(source_key, ts_ms DESC);
        """
    )


def append_data_source_log_row(
    con: Any,
    *,
    ts_ms: int,
    source_key: str,
    level: str,
    event_type: str,
    message: str,
    detail_json: str,
) -> None:
    con.execute(
        """
        INSERT INTO data_source_logs(ts_ms, source_key, level, event_type, message, detail_json)
        VALUES(?,?,?,?,?,?)
        """,
        (
            int(ts_ms),
            str(source_key),
            str(level),
            str(event_type),
            str(message)[:1000],
            str(detail_json),
        ),
    )


def delete_data_source_logs_for_source(con: Any, source_key: str) -> None:
    con.execute("DELETE FROM data_source_logs WHERE source_key = ?", (str(source_key),))


def log_data_source_event(
    *,
    ts_ms: int,
    source_key: str,
    level: str,
    event_type: str,
    message: str,
    detail_json: str,
    timeout_s: float,
    busy_timeout_ms: int,
) -> None:
    def _txn(con: Any) -> None:
        append_data_source_log_row(
            con,
            ts_ms=int(ts_ms),
            source_key=str(source_key),
            level=str(level),
            event_type=str(event_type),
            message=str(message),
            detail_json=str(detail_json),
        )

    run_write_txn(
        _txn,
        attempts=1,
        table="data_source_logs",
        operation="log_event",
        context={"source_key": str(source_key), "event_type": str(event_type or "event")},
        direct=True,
        maintenance=False,
        timeout_s=float(timeout_s),
        busy_timeout_ms=int(busy_timeout_ms),
    )

