"""Dashboard DB health and schema handler implementation."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable


def db_health_snapshot(
    *,
    db_path: Path,
    base_dir: str,
    connect_ro: Callable[[], Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "db_path": str(db_path),
        "cwd": os.getcwd(),
        "base_dir": base_dir,
        "exists": db_path.exists(),
        "size_bytes": 0,
        "wal_bytes": 0,
        "integrity": "unknown",
        "error": None,
        "tables": [],
        "row_counts": {},
    }
    try:
        if db_path.exists():
            result["size_bytes"] = db_path.stat().st_size
            wal_path = db_path.with_suffix(db_path.suffix + "-wal")
            if wal_path.exists():
                result["wal_bytes"] = wal_path.stat().st_size

        con = connect_ro()
        try:
            row = con.execute("PRAGMA quick_check;").fetchone()
            if row:
                result["integrity"] = str(row[0])
                if str(row[0]).lower() != "ok":
                    result["ok"] = False
        finally:
            con.close()
    except Exception as e:
        result["ok"] = False
        result["error"] = str(e)

    return result


def api_get_db_health(
    _parsed: Any,
    _ctx: Any = None,
    *,
    db_health_snapshot_fn: Callable[[], dict[str, Any]],
    dashboard_db_connect: Callable[[], Any],
    jobs: Any,
    supervisor: Any,
    api_handlers: dict[str, Any],
) -> dict[str, Any]:
    del _parsed, _ctx
    snap = db_health_snapshot_fn()
    ts_ms = int(time.time() * 1000)
    snap["ts"] = ts_ms
    snap["ts_ms"] = ts_ms

    try:
        from engine.api.api_system import api_get_system_state

        snap["system_snapshot"] = api_get_system_state(
            None,
            {
                "JOBS": jobs,
                "SUPERVISOR": supervisor,
                "API_HANDLERS": api_handlers,
            },
        )
    except Exception as e:
        snap["system_snapshot_error"] = str(e)

    try:
        from engine.runtime.health import get_health_snapshot

        snap["runtime_health"] = get_health_snapshot()
    except Exception as e:
        snap["runtime_health_error"] = str(e)

    try:
        from engine.api.api_system import _recent_runtime_errors

        snap["recent_errors"] = _recent_runtime_errors(limit=10)
    except Exception:
        snap["recent_errors"] = []

    try:
        con = dashboard_db_connect()
        try:
            cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]
            snap["tables"] = tables
            row_counts = {}
            for table in tables:
                try:
                    row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    row_counts[table] = int(row[0]) if row else 0
                except Exception:
                    row_counts[table] = None
            snap["row_counts"] = row_counts
        finally:
            con.close()
    except Exception as e:
        snap["ok"] = False
        snap["error"] = str(e)

    try:
        from engine.runtime.storage import get_db_debug_snapshot

        snap["storage_debug"] = get_db_debug_snapshot()
    except Exception as e:
        snap["storage_debug_error"] = str(e)

    return snap


def api_get_schema_audit(_parsed: Any, *, get_schema_audit: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    del _parsed
    return get_schema_audit()
