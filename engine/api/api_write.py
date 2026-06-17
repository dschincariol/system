"""
FILE: api_write.py

HTTP/API handlers for write endpoints.
"""

"""
Write-only API layer.

All DB mutations previously inside dashboard_server.py now live here.
No supervisor logic.
No runtime orchestration.
Pure DB mutations.
"""

import json
import logging
import os
import time

from engine.runtime.failure_diagnostics import failure_response, log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import run_write_txn

LOG = get_logger("api.write")
_WARNED_KEYS: set[str] = set()

# ============================================================
# ALERT ACK / RESOLVE
# ============================================================

def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        value = int(float(os.environ.get(name, default)))
    except Exception:
        value = int(default)
    return max(int(minimum), int(value))


def _warn_nonfatal(code: str, error: BaseException, *, warn_key: str | None = None, **extra) -> None:
    if warn_key and warn_key in _WARNED_KEYS:
        return
    log_failure(
        LOG,
        event="api_write_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.api.api_write",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_KEYS.add(warn_key)


def _sqlite_connection(con) -> bool:
    try:
        con.execute("SELECT sqlite_version()").fetchone()
        return True
    except Exception as e:
        _warn_nonfatal("API_WRITE_SQLITE_PROBE_FAILED", e, warn_key="sqlite_probe")
        return False


def _add_column_if_missing(con, table: str, column_def: str) -> None:
    try:
        if _sqlite_connection(con):
            con.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")
        else:
            name = str(column_def).split()[0]
            rest = str(column_def)[len(name):].strip()
            con.execute(f"ALTER TABLE IF EXISTS {table} ADD COLUMN IF NOT EXISTS {name} {rest}")
    except Exception as e:
        _warn_nonfatal(
            "API_WRITE_ADD_COLUMN_FAILED",
            e,
            warn_key=f"add_column:{table}:{column_def}",
            table=str(table),
            column_def=str(column_def),
        )
        return


def _ensure_alert_lifecycle_schema(con) -> None:
    is_sqlite = _sqlite_connection(con)
    json_type = "TEXT" if is_sqlite else "JSONB"
    json_default = "'{}'" if is_sqlite else "'{}'::jsonb"
    id_decl = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "BIGSERIAL PRIMARY KEY"
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_acks (
          alert_id BIGINT PRIMARY KEY,
          acked_ts_ms BIGINT NOT NULL,
          acked_by TEXT,
          source TEXT
        )
        """
    )
    _add_column_if_missing(con, "alert_acks", "expires_ts_ms BIGINT")
    _add_column_if_missing(con, "alert_acks", "reason TEXT")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS alert_shelves (
          alert_id BIGINT PRIMARY KEY,
          shelved_ts_ms BIGINT NOT NULL,
          expires_ts_ms BIGINT NOT NULL,
          shelved_by TEXT,
          reason TEXT NOT NULL,
          source TEXT,
          severity TEXT,
          detail_json {json_type} NOT NULL DEFAULT {json_default}
        )
        """
    )
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS alert_lifecycle_events (
          id {id_decl},
          alert_id BIGINT NOT NULL,
          ts_ms BIGINT NOT NULL,
          lifecycle_state TEXT NOT NULL,
          actor TEXT,
          reason TEXT,
          source TEXT,
          detail_json {json_type} NOT NULL DEFAULT {json_default}
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_alert_lifecycle_events_alert_ts ON alert_lifecycle_events(alert_id, ts_ms DESC)"
    )


def _write_alert_lifecycle_event(con, *, alert_id: int, state: str, actor: str, reason: str, source: str, detail: dict | None = None) -> None:
    con.execute(
        """
        INSERT INTO alert_lifecycle_events
        (alert_id, ts_ms, lifecycle_state, actor, reason, source, detail_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(alert_id),
            _now_ms(),
            str(state or ""),
            str(actor or ""),
            str(reason or ""),
            str(source or ""),
            json.dumps(detail or {}, separators=(",", ":"), sort_keys=True),
        ),
    )


def ack_alert(alert_id: int, who: str = "", source: str = "", reason: str = "", timeout_ms: int | None = None):
    now_ms = _now_ms()
    ttl_ms = int(timeout_ms) if timeout_ms is not None else _env_int("ALERT_ACK_TIMEOUT_MS", 30 * 60 * 1000, minimum=60_000)
    expires_ts_ms = int(now_ms) + int(ttl_ms)

    def _txn(con):
        _ensure_alert_lifecycle_schema(con)
        # Alert acknowledgements are persisted as explicit write-side records
        # rather than mutating the original alert row.
        con.execute(
            """
            INSERT INTO alert_acks
            (alert_id, acked_ts_ms, acked_by, source, expires_ts_ms, reason)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(alert_id) DO UPDATE SET
              acked_ts_ms=excluded.acked_ts_ms,
              acked_by=excluded.acked_by,
              source=excluded.source,
              expires_ts_ms=excluded.expires_ts_ms,
              reason=excluded.reason
            """,
            (
                int(alert_id),
                int(now_ms),
                str(who or ""),
                str(source or ""),
                int(expires_ts_ms),
                str(reason or ""),
            ),
        )
        _write_alert_lifecycle_event(
            con,
            alert_id=int(alert_id),
            state="acknowledged",
            actor=str(who or ""),
            reason=str(reason or ""),
            source=str(source or ""),
            detail={"expires_ts_ms": int(expires_ts_ms), "timeout_ms": int(ttl_ms)},
        )

    run_write_txn(_txn)
    return {"ok": True, "alert_id": int(alert_id), "acked_ts_ms": now_ms, "expires_ts_ms": expires_ts_ms}


def shelve_alert(
    alert_id: int,
    *,
    who: str = "",
    reason: str = "",
    source: str = "",
    expires_ts_ms: int | None = None,
    duration_ms: int | None = None,
    severity: str = "",
):
    if not str(reason or "").strip():
        return {"ok": False, "error": "shelve_reason_required", "meta": {"status": 422}}
    now_ms = _now_ms()
    expiry = int(expires_ts_ms or 0)
    if expiry <= now_ms:
        expiry = now_ms + int(duration_ms or _env_int("ALERT_SHELVE_DEFAULT_MS", 30 * 60 * 1000, minimum=60_000))
    max_ms = _env_int("ALERT_SHELVE_MAX_MS", 24 * 60 * 60 * 1000, minimum=60_000)
    if expiry - now_ms > max_ms:
        return {"ok": False, "error": "shelve_expiry_too_long", "max_duration_ms": max_ms, "meta": {"status": 422}}

    def _txn(con):
        _ensure_alert_lifecycle_schema(con)
        con.execute(
            """
            INSERT INTO alert_shelves
            (alert_id, shelved_ts_ms, expires_ts_ms, shelved_by, reason, source, severity, detail_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(alert_id) DO UPDATE SET
              shelved_ts_ms=excluded.shelved_ts_ms,
              expires_ts_ms=excluded.expires_ts_ms,
              shelved_by=excluded.shelved_by,
              reason=excluded.reason,
              source=excluded.source,
              severity=excluded.severity,
              detail_json=excluded.detail_json
            """,
            (
                int(alert_id),
                int(now_ms),
                int(expiry),
                str(who or ""),
                str(reason or ""),
                str(source or ""),
                str(severity or ""),
                json.dumps({"duration_ms": int(expiry - now_ms)}, separators=(",", ":"), sort_keys=True),
            ),
        )
        _write_alert_lifecycle_event(
            con,
            alert_id=int(alert_id),
            state="shelved",
            actor=str(who or ""),
            reason=str(reason or ""),
            source=str(source or ""),
            detail={"expires_ts_ms": int(expiry)},
        )

    run_write_txn(_txn)
    return {"ok": True, "alert_id": int(alert_id), "shelved_ts_ms": now_ms, "expires_ts_ms": int(expiry)}


def resolve_alert(alert_id: int, who: str = "", reason: str = "", source: str = ""):
    now_ms = _now_ms()

    def _txn(con):
        _ensure_alert_lifecycle_schema(con)
        con.execute(
            """
            INSERT INTO alert_resolutions
            (alert_id, resolved_ts_ms, resolved_by, reason, source)
            VALUES (?,?,?,?,?)
            ON CONFLICT(alert_id) DO NOTHING
            """,
            (
                int(alert_id),
                int(now_ms),
                str(who or ""),
                str(reason or ""),
                str(source or ""),
            ),
        )
        _write_alert_lifecycle_event(
            con,
            alert_id=int(alert_id),
            state="resolved",
            actor=str(who or ""),
            reason=str(reason or ""),
            source=str(source or ""),
        )

    run_write_txn(_txn)
    return {"ok": True, "alert_id": int(alert_id), "resolved_ts_ms": now_ms}

# ============================================================
# JOB HISTORY
# ============================================================

def write_job_event(job_name: str, event: str, detail: dict | None = None):
    from engine.runtime.locks import write_job_history

    try:
        # This preserves the write-side boundary: API code delegates job-history
        # writes to the runtime lock/history subsystem.
        write_job_history(
            job_name=job_name,
            event=event,
            detail=json.dumps(detail or {}, separators=(",", ":"), sort_keys=True),
        )
        return {"ok": True}
    except Exception as e:
        out = failure_response(
            LOG,
            event="api_write_job_event_failed",
            code="API_WRITE_JOB_EVENT_FAILED",
            message="Failed to write job history event.",
            error=e,
            component="engine.api.api_write",
            extra={
                "job_name": str(job_name or ""),
                "event_name": str(event or ""),
            },
        )
        out["job_name"] = str(job_name or "")
        out["event_name"] = str(event or "")
        return out


# ============================================================
# PROMOTION GUARD
# ============================================================

def set_promotion_enabled(value: str):
    from engine.strategy.promotion_guard import set_guard

    # API writes normalize inputs to the narrow guard contract.
    v = "1" if str(value) == "1" else "0"
    set_guard("promotion_enabled", v)
    return {"ok": True, "promotion_enabled": v}
