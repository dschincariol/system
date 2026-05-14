"""
Durable execution-boundary storage for order commands and terminal order events.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Callable, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect_rw_direct


LOG = logging.getLogger("engine.execution.order_command_boundary")

SCHEMA = """
CREATE TABLE IF NOT EXISTS order_commands (
  command_id TEXT PRIMARY KEY,
  ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  batch_id INTEGER,
  payload_ts_ms INTEGER,
  correlation_id TEXT,
  mode TEXT NOT NULL,
  broker TEXT NOT NULL,
  payload_source TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ready',
  real_order_count INTEGER NOT NULL DEFAULT 0,
  shadow_order_count INTEGER NOT NULL DEFAULT 0,
  blocked_order_count INTEGER NOT NULL DEFAULT 0,
  command_json TEXT NOT NULL,
  result_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_order_commands_ts
  ON order_commands(ts_ms);

CREATE INDEX IF NOT EXISTS idx_order_commands_batch_mode
  ON order_commands(batch_id, mode, ts_ms);

CREATE TABLE IF NOT EXISTS order_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  command_id TEXT,
  batch_id INTEGER,
  correlation_id TEXT,
  event_type TEXT NOT NULL,
  mode TEXT NOT NULL,
  broker TEXT NOT NULL,
  status TEXT NOT NULL,
  payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_events_ts
  ON order_events(ts_ms);

CREATE INDEX IF NOT EXISTS idx_order_events_command_ts
  ON order_events(command_id, ts_ms);

CREATE INDEX IF NOT EXISTS idx_order_events_type_ts
  ON order_events(event_type, ts_ms);
"""


def _warn_nonfatal(code: str, error: Exception, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.order_command_boundary",
        extra=extra or {},
        include_health=False,
        persist=False,
    )


def _dump_json(payload: Optional[Dict[str, Any]]) -> str:
    try:
        return json.dumps(payload or {}, separators=(",", ":"), sort_keys=True, default=str)
    except Exception as e:
        _warn_nonfatal("ORDER_COMMAND_BOUNDARY_JSON_DUMP_FAILED", e)
        return "{}"


def ensure_order_command_boundary_schema(con) -> None:
    con.executescript(SCHEMA)


def init_order_command_boundary() -> None:
    con = connect_rw_direct()
    try:
        ensure_order_command_boundary_schema(con)
        con.commit()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("ORDER_COMMAND_BOUNDARY_CLOSE_FAILED", e, operation="init")


def _execute_write(
    operation: Callable[[Any], Any],
    *,
    operation_name: str,
    con=None,
) -> Any:
    own_con = con is None
    db = con if con is not None else connect_rw_direct()
    try:
        ensure_order_command_boundary_schema(db)
        result = operation(db)
        if own_con:
            db.commit()
        return result
    except Exception:
        if own_con:
            try:
                db.rollback()
            except Exception as rollback_error:
                _warn_nonfatal(
                    "ORDER_COMMAND_BOUNDARY_ROLLBACK_FAILED",
                    rollback_error,
                    operation=str(operation_name),
                )
        raise
    finally:
        if own_con:
            try:
                db.close()
            except Exception as close_error:
                _warn_nonfatal(
                    "ORDER_COMMAND_BOUNDARY_CLOSE_FAILED",
                    close_error,
                    operation=str(operation_name),
                )


def record_order_command(
    *,
    ts_ms: int,
    batch_id: Optional[int],
    payload_ts_ms: Optional[int],
    correlation_id: Optional[str],
    mode: str,
    broker: str,
    payload_source: str,
    real_order_count: int,
    shadow_order_count: int,
    blocked_order_count: int,
    payload: Optional[Dict[str, Any]],
    status: str = "ready",
    command_id: Optional[str] = None,
    con=None,
) -> str:
    resolved_command_id = str(command_id or f"oc_{uuid.uuid4().hex}")
    payload_json = _dump_json(payload)

    def _write(db):
        db.execute(
            """
            INSERT INTO order_commands(
              command_id, ts_ms, updated_ts_ms, batch_id, payload_ts_ms, correlation_id,
              mode, broker, payload_source, status, real_order_count, shadow_order_count,
              blocked_order_count, command_json, result_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                resolved_command_id,
                int(ts_ms),
                int(ts_ms),
                (int(batch_id) if batch_id is not None else None),
                (int(payload_ts_ms) if payload_ts_ms is not None else None),
                (str(correlation_id) if correlation_id is not None else None),
                str(mode),
                str(broker),
                str(payload_source),
                str(status),
                int(real_order_count),
                int(shadow_order_count),
                int(blocked_order_count),
                payload_json,
                None,
            ),
        )
        return resolved_command_id

    return str(_execute_write(_write, operation_name="record_order_command", con=con))


def record_order_event(
    *,
    ts_ms: int,
    event_type: str,
    mode: str,
    broker: str,
    status: str,
    payload: Optional[Dict[str, Any]],
    command_id: Optional[str] = None,
    batch_id: Optional[int] = None,
    correlation_id: Optional[str] = None,
    con=None,
) -> Optional[int]:
    payload_json = _dump_json(payload)

    def _write(db):
        if command_id:
            db.execute(
                """
                UPDATE order_commands
                SET status=?, updated_ts_ms=?, result_json=?
                WHERE command_id=?
                """,
                (
                    str(status),
                    int(ts_ms),
                    payload_json,
                    str(command_id),
                ),
            )
        cursor = db.execute(
            """
            INSERT INTO order_events(
              ts_ms, command_id, batch_id, correlation_id, event_type, mode, broker, status, payload_json
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                (str(command_id) if command_id else None),
                (int(batch_id) if batch_id is not None else None),
                (str(correlation_id) if correlation_id is not None else None),
                str(event_type),
                str(mode),
                str(broker),
                str(status),
                payload_json,
            ),
        )
        return int(cursor.lastrowid or 0)

    return _execute_write(_write, operation_name="record_order_event", con=con)
