"""Storage/schema helper functions for runtime health."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional


WarnFn = Callable[..., None]


def sqlite_wal_path(db_path: Path) -> Optional[Path]:
    if not db_path.name:
        return None
    return db_path.with_name(f"{db_path.name}-wal")


def table_exists(con: Any, table: str, *, warn: WarnFn) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        warn("health.table_exists", e, table=str(table))
        return False


def get_table_cols(con: Any, table: str, *, warn: WarnFn) -> list[Any]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        return [r[1] for r in rows] if rows else []
    except Exception as e:
        warn("health.table_cols", e, table=str(table))
        return []


def get_index_names(con: Any, *, warn: WarnFn) -> set[str]:
    try:
        rows = con.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall() or []
        return {
            str(row[0]).strip()
            for row in rows
            if row and row[0] is not None and str(row[0]).strip()
        }
    except Exception as e:
        warn("health.index_names", e)
        return set()


def schema_audit(
    *,
    get_db_validation_snapshot: Callable[..., Any],
    storage_schema_version: int,
    warn: WarnFn,
    now_ms: int | None = None,
) -> Dict[str, Any]:
    ts_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    try:
        validation = dict(get_db_validation_snapshot(include_quick_check=False) or {})
    except Exception as e:
        warn("health.db_validation.snapshot", e)
        return {
            "ok": False,
            "ts_ms": ts_ms,
            "missing_tables": [],
            "missing_cols": {},
            "missing_columns": {},
            "missing_indexes": [],
            "have_tables": [],
            "schema_version": None,
            "expected_schema_version": storage_schema_version,
            "schema_version_status": "unavailable",
            "schema_status": "unavailable",
            "schema_version_notes": f"{type(e).__name__}: {e}",
            "schema_version_ok": False,
            "error": f"{type(e).__name__}: {e}",
        }

    missing_cols = dict(validation.get("missing_columns") or validation.get("missing_cols") or {})
    schema_status = str(validation.get("schema_status") or "")
    return {
        "ok": bool(validation.get("ok")),
        "ts_ms": ts_ms,
        "missing_tables": list(validation.get("missing_tables") or []),
        "missing_cols": missing_cols,
        "missing_columns": missing_cols,
        "missing_indexes": list(validation.get("missing_indexes") or []),
        "have_tables": list(validation.get("have_tables") or []),
        "schema_version": validation.get("schema_version"),
        "expected_schema_version": validation.get("expected_schema_version", storage_schema_version),
        "schema_version_status": schema_status,
        "schema_status": schema_status,
        "schema_version_notes": str(validation.get("schema_version_notes") or validation.get("error") or ""),
        "schema_version_ok": bool(validation.get("schema_version_ok", True)),
        "backend": str(validation.get("backend") or validation.get("storage") or ""),
        "storage": str(validation.get("storage") or validation.get("backend") or ""),
        "quick_check": str(validation.get("quick_check") or ""),
        "owned_schema_ok": bool(validation.get("owned_schema_ok", True)),
        "owned_drift_tables": list(validation.get("owned_drift_tables") or []),
    }
