"""
FILE: db_repair.py

Runtime subsystem module for `db_repair`.
"""

import os
import logging

from engine.runtime.db_guard import ensure_db_ok
from engine.runtime.event_log import reindex_event_log_indexes
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.jobs.repair_schema import run as repair_schema
from engine.runtime.storage import connect, get_db_validation_snapshot, init_db


LOG = get_logger("runtime.db_repair")
_EVENT_LOG_REINDEXABLE_INDEXES = ("idx_event_log_corr", "idx_event_log_type_ts")


def _integrity_check_rows() -> list[str]:
    con = connect(readonly=True)
    try:
        rows = con.execute("PRAGMA integrity_check;").fetchall() or []
    finally:
        con.close()
    findings = [str((row[0] if row else "") or "").strip() for row in rows if str((row[0] if row else "") or "").strip()]
    return findings or ["empty_result"]


def _event_log_index_findings(findings: list[str]) -> list[str]:
    matched: list[str] = []
    for index_name in _EVENT_LOG_REINDEXABLE_INDEXES:
        if any(index_name in str(finding or "") for finding in findings):
            matched.append(str(index_name))
    return matched


def _auto_reindex_enabled() -> bool:
    return str(os.environ.get("DB_REPAIR_AUTO_REINDEX_EVENT_LOG_INDEXES", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _reindex_event_log_indexes(indexes: list[str]) -> list[str]:
    requested = [str(name) for name in list(indexes or []) if str(name) in _EVENT_LOG_REINDEXABLE_INDEXES]
    if not requested:
        return []
    return list(
        reindex_event_log_indexes(
            requested,
            operation="db_repair_reindex_event_log_indexes",
        )
        or []
    )


def _log_repair_step(step: str, status: str, started_ts_ms: int, **extra: object) -> None:
    payload = {
        "step": str(step),
        "status": str(status),
        "duration_ms": max(0, int(__import__("time").time() * 1000) - int(started_ts_ms)),
        "ts_ms": int(__import__("time").time() * 1000),
    }
    payload.update(extra or {})
    try:
        LOG.info(
            "runtime_db_repair_step %s %s",
            str(step),
            str(status),
            extra={
                "event": "runtime_db_repair_step",
                "extra_json": payload,
            },
        )
    except Exception:
        LOG.info(
            "runtime_db_repair_step step=%s status=%s duration_ms=%s extra=%s",
            str(step),
            str(status),
            int(payload.get("duration_ms") or 0),
            repr(payload),
        )


def repair(*, startup_fast_path: bool = False):
    # Guard first: schema repair is only safe if the database path and basic
    # integrity checks already say the runtime can operate on this DB.
    started_ts_ms = int(__import__("time").time() * 1000)
    db_guard = ensure_db_ok(include_quick_check=not startup_fast_path)
    _log_repair_step(
        "db_guard",
        "ok" if isinstance(db_guard, dict) and bool(db_guard.get("ok")) else "failed",
        started_ts_ms,
        startup_fast_path=bool(startup_fast_path),
        quick_check_skipped=bool(startup_fast_path),
        error=str((db_guard or {}).get("error") or "") if isinstance(db_guard, dict) else "",
    )
    if not isinstance(db_guard, dict) or not db_guard.get("ok"):
        return {
            "ok": False,
            "error": (
                db_guard.get("error")
                if isinstance(db_guard, dict)
                else "db_guard_failed"
            ),
            "db_guard": db_guard,
        }

    # Schema repair is idempotent and is the authoritative place to reconcile
    # older on-disk schemas with current runtime expectations.
    started_ts_ms = int(__import__("time").time() * 1000)
    result = repair_schema(include_quick_check=not startup_fast_path)
    _log_repair_step(
        "repair_schema",
        "ok" if isinstance(result, dict) and bool(result.get("ok")) else "failed",
        started_ts_ms,
        startup_fast_path=bool(startup_fast_path),
        quick_check_skipped=bool(startup_fast_path),
        schema_version=(result or {}).get("schema_version") if isinstance(result, dict) else None,
        expected_schema_version=(result or {}).get("expected_schema_version") if isinstance(result, dict) else None,
        error=str((result or {}).get("error") or "") if isinstance(result, dict) else "",
    )
    if not isinstance(result, dict) or not result.get("ok"):
        return result

    post_repair_init_db_required = bool(result.get("post_repair_init_db_required", True))
    if not post_repair_init_db_required:
        result = dict(result)
        result["db_guard"] = db_guard
        result["init_db"] = "skipped_schema_valid"
        result["startup_fast_path"] = bool(startup_fast_path)
        _log_repair_step(
            "init_db",
            "skipped",
            int(__import__("time").time() * 1000),
            startup_fast_path=bool(startup_fast_path),
            reason="schema_valid_after_repair",
        )
    else:
        try:
            # init_db() re-applies runtime-side table/index creation after repair
            # so the full schema surface is present before boot continues.
            started_ts_ms = int(__import__("time").time() * 1000)
            init_db()
            _log_repair_step(
                "init_db",
                "ok",
                started_ts_ms,
                startup_fast_path=bool(startup_fast_path),
            )
        except Exception as e:
            _log_repair_step(
                "init_db",
                "failed",
                started_ts_ms,
                startup_fast_path=bool(startup_fast_path),
                error=str(e),
            )
            log_failure(
                LOG,
                event="runtime_db_repair_init_db_failed",
                code="RUNTIME_DB_REPAIR_INIT_DB_FAILED",
                message="runtime_db_repair_init_db_failed",
                error=e,
                level=logging.WARNING,
                component="engine.runtime.db_repair",
                persist=False,
            )
            return {
                "ok": False,
                "error": f"init_db_failed: {e}",
                "schema": result,
                "db_guard": db_guard,
            }

        result = dict(result)
        result["db_guard"] = db_guard
        result["init_db"] = True
        result["startup_fast_path"] = bool(startup_fast_path)
    started_ts_ms = int(__import__("time").time() * 1000)
    try:
        storage_validation = dict(get_db_validation_snapshot(include_quick_check=False) or {})
    except Exception as e:
        _log_repair_step(
            "storage_validation",
            "failed",
            started_ts_ms,
            startup_fast_path=bool(startup_fast_path),
            error=str(e),
        )
        log_failure(
            LOG,
            event="runtime_db_repair_storage_validation_failed",
            code="RUNTIME_DB_REPAIR_STORAGE_VALIDATION_FAILED",
            message="runtime_db_repair_storage_validation_failed",
            error=e,
            level=logging.WARNING,
            component="engine.runtime.db_repair",
            persist=False,
        )
        return {
            "ok": False,
            "error": f"storage_validation_failed:{e}",
            "schema": result,
            "db_guard": db_guard,
        }

    result["storage_validation"] = storage_validation
    owned_schema_ok = bool(storage_validation.get("owned_schema_ok", True))
    owned_drift_tables = list(storage_validation.get("owned_drift_tables") or [])
    _log_repair_step(
        "storage_validation",
        "ok" if owned_schema_ok else "failed",
        started_ts_ms,
        startup_fast_path=bool(startup_fast_path),
        owned_drift_tables=list(owned_drift_tables),
    )
    if not owned_schema_ok:
        result["ok"] = False
        result["error"] = "owned_table_schema_drift_detected"
        result["maintenance_required"] = {
            "action": "storage_owned_table_repair",
            "tables": list(owned_drift_tables),
            "unexpected_columns": dict(storage_validation.get("owned_unexpected_columns") or {}),
            "pk_mismatches": dict(storage_validation.get("owned_pk_mismatches") or {}),
            "missing_indexes": dict(storage_validation.get("owned_missing_indexes") or {}),
        }
        return result

    if startup_fast_path:
        result["integrity_check"] = {
            "ok": True,
            "verified": False,
            "deferred": True,
            "mode": "startup_fast_path",
            "findings": [],
            "repairable_indexes": [],
        }
        _log_repair_step(
            "integrity_check",
            "skipped",
            started_ts_ms,
            startup_fast_path=True,
            deferred=True,
            verified=False,
            reason="startup_fast_path",
        )
        return result

    backend_name = str(
        storage_validation.get("backend")
        or storage_validation.get("storage")
        or result.get("backend")
        or ""
    ).strip().lower()
    if backend_name == "postgres":
        result["integrity_check"] = {
            "ok": True,
            "verified": False,
            "deferred": True,
            "mode": "postgres_not_applicable",
            "findings": [],
            "repairable_indexes": [],
        }
        _log_repair_step(
            "integrity_check",
            "skipped",
            started_ts_ms,
            startup_fast_path=False,
            deferred=True,
            verified=False,
            reason="postgres_not_applicable",
        )
        return result

    started_ts_ms = int(__import__("time").time() * 1000)
    integrity_rows = _integrity_check_rows()
    integrity_ok = len(integrity_rows) == 1 and str(integrity_rows[0]).lower() == "ok"
    repairable_indexes = _event_log_index_findings(integrity_rows) if not integrity_ok else []
    result["integrity_check"] = {
        "ok": bool(integrity_ok),
        "findings": list(integrity_rows),
        "repairable_indexes": list(repairable_indexes),
    }
    _log_repair_step(
        "integrity_check",
        "ok" if integrity_ok else "failed",
        started_ts_ms,
        startup_fast_path=bool(startup_fast_path),
        repairable_indexes=list(repairable_indexes),
        finding_count=len(list(integrity_rows or [])),
    )
    if integrity_ok:
        return result

    if repairable_indexes and _auto_reindex_enabled():
        try:
            reindexed = _reindex_event_log_indexes(repairable_indexes)
        except Exception as e:
            log_failure(
                LOG,
                event="runtime_db_repair_reindex_failed",
                code="RUNTIME_DB_REPAIR_REINDEX_FAILED",
                message="runtime_db_repair_reindex_failed",
                error=e,
                level=logging.WARNING,
                component="engine.runtime.db_repair",
                extra={"indexes": list(repairable_indexes)},
                persist=False,
            )
            result["ok"] = False
            result["error"] = f"event_log_index_reindex_failed:{e}"
            result["maintenance_required"] = {
                "action": "manual_reindex",
                "indexes": list(repairable_indexes),
            }
            return result

        post_rows = _integrity_check_rows()
        post_ok = len(post_rows) == 1 and str(post_rows[0]).lower() == "ok"
        result["integrity_check"] = {
            "ok": bool(post_ok),
            "findings": list(post_rows),
            "repairable_indexes": _event_log_index_findings(post_rows) if not post_ok else [],
            "auto_reindexed": list(reindexed),
        }
        if post_ok:
            result["event_log_index_maintenance"] = {
                "status": "reindexed",
                "indexes": list(reindexed),
            }
            return result

    result["ok"] = False
    result["error"] = "integrity_check_failed"
    result["maintenance_required"] = {
        "action": ("reindex_event_log_indexes" if repairable_indexes else "manual_db_recovery"),
        "indexes": list(repairable_indexes),
        "recommended_sql": (
            [f"REINDEX {name};" for name in repairable_indexes]
            if repairable_indexes
            else []
        ),
        "integrity_findings": list(integrity_rows),
    }
    return result
