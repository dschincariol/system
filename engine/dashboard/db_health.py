"""Dashboard DB health and schema handler implementation."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Callable


_SQLITE_LOCAL_SUFFIXES = frozenset(("", "-wal", "-shm"))


def _normalize_backend_name(backend_name: str | None) -> str:
    backend = str(backend_name or "").strip().lower()
    if backend in {"pg", "postgresql"}:
        return "postgres"
    if backend in {"sqlite-test", "test"}:
        return "sqlite"
    return backend or "sqlite"


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sqlite_local_file_path(db_path: Path, suffix: str) -> tuple[Path | None, dict[str, Any] | None]:
    suffix_text = str(suffix or "")
    if suffix_text not in _SQLITE_LOCAL_SUFFIXES:
        return None, {
            "code": "unexpected_sqlite_local_suffix",
            "suffix": suffix_text,
            "detail": "local sqlite sizing accepts only the main file, -wal, and -shm",
        }
    try:
        if not db_path.name:
            return None, {
                "code": "db_path_has_no_name",
                "suffix": suffix_text,
                "detail": "cannot derive sqlite sidecar path from an unnamed database path",
            }
        if suffix_text == "":
            return db_path, None
        return db_path.with_name(f"{db_path.name}{suffix_text}"), None
    except Exception as exc:
        return None, {
            "code": "sqlite_local_path_error",
            "suffix": suffix_text,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _sqlite_local_file_size(db_path: Path, suffix: str) -> tuple[int | None, dict[str, Any] | None]:
    path, note = _sqlite_local_file_path(db_path, suffix)
    if path is None:
        return None, note
    try:
        if path.exists():
            return int(path.stat().st_size), None
        return 0, None
    except OSError as exc:
        return None, {
            "code": "sqlite_local_file_stat_error",
            "path": str(path),
            "suffix": str(suffix or ""),
            "error": f"{type(exc).__name__}: {exc}",
        }


def _run_db_liveness_check(con: Any, *, backend: str) -> tuple[bool, str, str | None]:
    try:
        if backend == "postgres":
            row = con.execute("SELECT 1").fetchone()
            ok = bool(row and int(row[0]) == 1)
            return ok, "not_applicable", None if ok else "postgres_liveness_check_failed"

        row = con.execute("PRAGMA quick_check;").fetchone()
        integrity = str(row[0]) if row else "unknown"
        return integrity.lower() == "ok", integrity, None
    except Exception as exc:
        return False, "unknown", f"{type(exc).__name__}: {exc}"


def _list_tables(con: Any, *, backend: str) -> list[str]:
    if backend == "postgres":
        rows = con.execute(
            """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = current_schema()
            ORDER BY tablename
            """
        ).fetchall()
    else:
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return [str(row[0]) for row in (rows or []) if row and row[0] is not None]


def db_health_snapshot(
    *,
    db_path: Path,
    base_dir: str,
    connect_ro: Callable[[], Any],
    backend_name: str | None = None,
) -> dict[str, Any]:
    backend = _normalize_backend_name(backend_name)
    result: dict[str, Any] = {
        "ok": True,
        "backend": backend,
        "storage": backend,
        "db_path": str(db_path),
        "cwd": os.getcwd(),
        "base_dir": base_dir,
        "exists": db_path.exists(),
        "size_bytes": None if backend == "postgres" else 0,
        "wal_bytes": None if backend == "postgres" else 0,
        "shm_bytes": None if backend == "postgres" else 0,
        "local_db_file_applicable": backend == "sqlite",
        "local_wal_applicable": backend == "sqlite",
        "wal_source": "sqlite_local_files" if backend == "sqlite" else "not_applicable_postgres",
        "wal_notes": [],
        "integrity": "unknown",
        "liveness": "unknown",
        "error": None,
        "tables": [],
        "row_counts": {},
    }
    try:
        if backend == "sqlite":
            for suffix, field in (("", "size_bytes"), ("-wal", "wal_bytes"), ("-shm", "shm_bytes")):
                size, note = _sqlite_local_file_size(db_path, suffix)
                if note is not None:
                    result["wal_notes"].append(note)
                    continue
                result[field] = int(size or 0)
        else:
            result["wal_notes"].append(
                {
                    "code": "local_wal_not_applicable",
                    "backend": backend,
                    "detail": "Postgres WAL is owned by the server, not a local DB_PATH-wal file",
                }
            )

        con = connect_ro()
        try:
            liveness_ok, integrity, liveness_error = _run_db_liveness_check(con, backend=backend)
            result["integrity"] = integrity
            result["liveness"] = "ok" if liveness_ok else "failed"
            if not liveness_ok:
                result["ok"] = False
                result["error"] = liveness_error or "database_liveness_check_failed"
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
            backend = _normalize_backend_name(str(snap.get("backend") or snap.get("storage") or ""))
            tables = _list_tables(con, backend=backend)
            snap["tables"] = tables
            row_counts = {}
            for table in tables:
                try:
                    count_table = _quote_ident(table) if backend == "postgres" else str(table)
                    row = con.execute(f"SELECT COUNT(*) FROM {count_table}").fetchone()
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
