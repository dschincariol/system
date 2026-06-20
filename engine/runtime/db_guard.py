"""
FILE: db_guard.py

Runtime storage readiness guard.
"""

from __future__ import annotations

import os
from pathlib import Path

from engine.runtime.config_schema import load_runtime_config
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_data_root


LOG = get_logger("engine.runtime.db_guard")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _looks_like_file_path(path: Path) -> bool:
    return bool(path.suffix)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.db_guard",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def resolve_db_path() -> Path:
    """Return the runtime data root kept for legacy callers.

    Connection targets come from ``TS_PG_DSN``. Older modules still ask for a
    ``DB_PATH`` value, so the compatibility value is a directory, not a database
    file.
    """

    try:
        cfg = load_runtime_config()
        raw = str(getattr(cfg, "db_path", "") or "").strip()
    except Exception as exc:
        _warn_nonfatal(
            "DB_GUARD_LOAD_RUNTIME_CONFIG_FAILED",
            exc,
            once_key="resolve_db_path_load_runtime_config",
        )
        raw = str(os.environ.get("DB_PATH", "") or "").strip()

    raw_path = Path(raw).expanduser() if raw else default_data_root()
    root = raw_path.parent if raw and _looks_like_file_path(raw_path) else raw_path
    resolved = root.resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    if not raw or not _looks_like_file_path(raw_path):
        os.environ["DB_PATH"] = str(resolved)
    return resolved


def _resolve_db_path() -> Path:
    return resolve_db_path()


def _load_schema_contract_summary() -> dict:
    try:
        from engine.runtime.storage import get_db_validation_snapshot

        validation = dict(get_db_validation_snapshot(include_quick_check=False, strict=True) or {})
    except Exception as exc:
        _warn_nonfatal(
            "DB_GUARD_SCHEMA_CONTRACT_READ_FAILED",
            exc,
            once_key="ensure_db_ok_schema_contract",
        )
        return {
            "checked": False,
            "error": f"schema_contract_read_failed:{type(exc).__name__}:{exc}",
        }

    return {
        "checked": True,
        "owned_schema_ok": bool(validation.get("owned_schema_ok", validation.get("ok", True))),
        "owned_drift_tables": list(validation.get("owned_drift_tables") or []),
        "owned_missing_tables": list(validation.get("owned_missing_tables") or validation.get("missing_tables") or []),
        "owned_missing_columns": dict(validation.get("owned_missing_columns") or validation.get("missing_columns") or {}),
        "owned_unexpected_columns": dict(validation.get("owned_unexpected_columns") or {}),
        "owned_type_mismatches": dict(validation.get("owned_type_mismatches") or {}),
        "owned_pk_mismatches": dict(validation.get("owned_pk_mismatches") or {}),
        "owned_missing_indexes": dict(validation.get("owned_missing_indexes") or {}),
        "schema_version": validation.get("schema_version"),
        "expected_schema_version": validation.get("expected_schema_version"),
        "schema_version_ok": bool(validation.get("schema_version_ok", False)),
    }


def _attach_schema_contract_summary(out: dict) -> dict:
    summary = _load_schema_contract_summary()
    if summary:
        out["schema_contract"] = summary
        if (
            summary.get("checked")
            and not bool(summary.get("owned_schema_ok", True))
            and str(out.get("action") or "none") == "none"
        ):
            out["action"] = "schema_repair_required"
    return out


def _explicit_sqlite_storage_backend() -> bool:
    backend = str(os.environ.get("TS_STORAGE_BACKEND") or "").strip().lower()
    return backend in {"sqlite", "sqlite-test", "test"}


def ensure_db_ok(*, include_quick_check: bool = True) -> dict:
    del include_quick_check
    storage_name = "sqlite" if _explicit_sqlite_storage_backend() else "postgres"
    out = {"ok": True, "db_path": None, "action": "none", "error": None, "storage": storage_name}
    data_root = resolve_db_path()
    out["db_path"] = str(data_root)

    try:
        from engine.runtime.storage import connect_rw_direct

        if storage_name == "sqlite":
            conn = connect_rw_direct()
        else:
            conn = connect_rw_direct(timeout_s=30.0, busy_timeout_ms=60000)
        try:
            conn.execute("SELECT 1").fetchone()
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        _warn_nonfatal(
            "DB_GUARD_STORAGE_CONNECT_FAILED",
            exc,
            once_key=f"ensure_db_ok_{storage_name}_connect",
            data_root=str(data_root),
            storage=storage_name,
        )
        out["ok"] = False
        out["error"] = f"{storage_name}_connect_failed:{type(exc).__name__}:{exc}"
        return out

    return _attach_schema_contract_summary(out)
