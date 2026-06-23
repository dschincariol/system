"""Owner module for data-source log storage paths."""

from __future__ import annotations

import json
from typing import Any

from engine.runtime.storage import run_write_txn

DATA_SOURCE_LOG_REDACTION_MARKER = "[REDACTED]"
DATA_SOURCE_LOG_REDACTION_SQLITE_MARKER_KEY = "data_source_logs_sqlite_redaction_v1"
DATA_SOURCE_LOG_REDACTION_TIMESCALE_MARKER_KEY = "data_source_logs_timescale_redaction_v1"

_SECRET_DETAIL_KEY_NAMES = {
    "access_token",
    "api_key",
    "api_secret",
    "api_token",
    "client_secret",
    "credentials",
    "credentials_enc",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "secret_key",
    "token",
}
_SECRET_DETAIL_KEY_COMPACT_NAMES = {name.replace("_", "") for name in _SECRET_DETAIL_KEY_NAMES}


def _normalize_detail_key(key: Any) -> str:
    text = str(key or "").strip().lower()
    out: list[str] = []
    previous_sep = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            previous_sep = False
        elif not previous_sep:
            out.append("_")
            previous_sep = True
    return "".join(out).strip("_")


def data_source_log_detail_key_is_secret(key: Any) -> bool:
    """Return whether a structured detail key is credential-bearing."""
    normalized = _normalize_detail_key(key)
    return normalized in _SECRET_DETAIL_KEY_NAMES or normalized.replace("_", "") in _SECRET_DETAIL_KEY_COMPACT_NAMES


def sanitize_data_source_log_detail(detail: Any) -> Any:
    """Return a copy of data-source log detail with credential fields redacted."""

    def _sanitize(value: Any, *, key: Any = None) -> Any:
        if data_source_log_detail_key_is_secret(key):
            return DATA_SOURCE_LOG_REDACTION_MARKER
        if isinstance(value, dict):
            return {
                str(item_key): _sanitize(item_value, key=item_key)
                for item_key, item_value in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [_sanitize(item) for item in value]
        return value

    return _sanitize(detail)


def data_source_log_detail_to_json(detail: Any) -> str:
    sanitized = sanitize_data_source_log_detail(detail if detail is not None else {})
    return json.dumps(sanitized, separators=(",", ":"), sort_keys=True, default=str)


def data_source_log_detail_from_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        payload = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except Exception:
            return {}
        if not isinstance(parsed, dict):
            return {}
        payload = parsed
    sanitized = sanitize_data_source_log_detail(payload)
    return dict(sanitized) if isinstance(sanitized, dict) else {}


def sanitize_data_source_log_detail_json(raw: Any) -> str:
    return data_source_log_detail_to_json(data_source_log_detail_from_json(raw))


def _sqlite_table_exists(con: Any, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(name),),
    ).fetchone()
    return bool(row)


def _column_type(con: Any, table: str, column: str) -> str:
    try:
        rows = con.execute(f"PRAGMA table_info({str(table)})").fetchall() or []
    except Exception:
        return ""
    for row in rows:
        try:
            if str(row[1] or "") == str(column):
                return str(row[2] or "").upper()
        except Exception:
            continue
    return ""


def _detail_json_storage_value(con: Any, sanitized_json: str) -> Any:
    column_type = _column_type(con, "data_source_logs", "detail_json")
    if "JSON" not in column_type:
        return str(sanitized_json)
    try:
        return json.loads(str(sanitized_json or "{}"))
    except Exception:
        return {}


def _raw_detail_json_text(raw: Any) -> str:
    if isinstance(raw, (dict, list)):
        return json.dumps(raw, separators=(",", ":"), sort_keys=True, default=str)
    return "" if raw is None else str(raw)


def redact_existing_data_source_log_details(con: Any) -> dict[str, int]:
    """Idempotently redact already-persisted runtime data-source log details."""
    if not _sqlite_table_exists(con, "data_source_logs"):
        return {"scanned": 0, "updated": 0}
    rows = con.execute(
        """
        SELECT id, detail_json
        FROM data_source_logs
        WHERE detail_json IS NOT NULL
        ORDER BY id ASC
        """
    ).fetchall() or []
    scanned = 0
    updated = 0
    for row in rows:
        scanned += 1
        row_id = int(row[0] or 0)
        raw_detail = row[1]
        original = _raw_detail_json_text(raw_detail)
        sanitized = sanitize_data_source_log_detail_json(raw_detail)
        if sanitized == original:
            continue
        con.execute(
            "UPDATE data_source_logs SET detail_json = ? WHERE id = ?",
            (_detail_json_storage_value(con, sanitized), row_id),
        )
        updated += 1
    return {"scanned": int(scanned), "updated": int(updated)}


def redact_existing_data_source_log_details_once(
    *,
    now_ms: int,
    timeout_s: float | None = None,
    busy_timeout_ms: int | None = None,
) -> dict[str, Any]:
    """Run the primary runtime log cleanup once and mark it in runtime_meta."""

    result: dict[str, Any] = {"skipped": False}

    def _txn(con: Any) -> None:
        cleanup_marker = con.execute(
            "SELECT value FROM runtime_meta WHERE key = ? LIMIT 1",
            (DATA_SOURCE_LOG_REDACTION_SQLITE_MARKER_KEY,),
        ).fetchone()
        if str((cleanup_marker or [""])[0] or "").strip() == "1":
            result.update({"skipped": True, "scanned": 0, "updated": 0})
            return
        cleanup_result = redact_existing_data_source_log_details(con)
        result.update(cleanup_result)
        con.execute(
            """
            INSERT INTO runtime_meta(key, value, updated_ts_ms)
            VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (
                DATA_SOURCE_LOG_REDACTION_SQLITE_MARKER_KEY,
                "1",
                int(now_ms),
            ),
        )
        con.execute(
            """
            INSERT INTO runtime_meta(key, value, updated_ts_ms)
            VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (
                f"{DATA_SOURCE_LOG_REDACTION_SQLITE_MARKER_KEY}_summary",
                json.dumps(cleanup_result, separators=(",", ":"), sort_keys=True, default=str),
                int(now_ms),
            ),
        )

    run_write_txn(
        _txn,
        table="data_source_logs",
        operation="data_source_log_redaction_cleanup",
        attempts=3,
        direct=False,
        maintenance=True,
        timeout_s=timeout_s,
        busy_timeout_ms=busy_timeout_ms,
    )
    return dict(result)


def redact_existing_timescale_data_source_log_details(*, batch_size: int = 1000) -> dict[str, Any]:
    """Idempotently redact already-mirrored Timescale data-source log details."""
    try:
        from engine.runtime.telemetry_read_router import _timescale_connection
        from engine.runtime.timescale_client import _quote_ident
    except Exception as exc:
        return {"attempted": False, "ok": False, "error": f"{type(exc).__name__}: {exc}"}

    scanned = 0
    updated = 0
    try:
        with _timescale_connection() as (con, schema_name):
            schema_ref = _quote_ident(schema_name)
            table_ref = f"{schema_ref}.data_source_logs"
            offset = 0
            while True:
                with con.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT sqlite_rowid, "time", detail_json::text
                        FROM {table_ref}
                        WHERE detail_json IS NOT NULL
                        ORDER BY "time" ASC, sqlite_rowid ASC
                        LIMIT %s OFFSET %s
                        """,
                        (int(max(1, batch_size)), int(offset)),
                    )
                    rows = cur.fetchall() or []
                if not rows:
                    break
                for row in rows:
                    scanned += 1
                    sqlite_rowid = int(row[0] or 0)
                    timestamp = row[1]
                    original = "" if row[2] is None else str(row[2])
                    sanitized = sanitize_data_source_log_detail_json(original)
                    if sanitized == original:
                        continue
                    with con.cursor() as cur:
                        cur.execute(
                            f"""
                            UPDATE {table_ref}
                            SET detail_json = %s::jsonb
                            WHERE sqlite_rowid = %s AND "time" = %s
                            """,
                            (sanitized, sqlite_rowid, timestamp),
                        )
                    updated += 1
                offset += len(rows)
        return {"attempted": True, "ok": True, "scanned": int(scanned), "updated": int(updated)}
    except Exception as exc:
        return {
            "attempted": True,
            "ok": False,
            "scanned": int(scanned),
            "updated": int(updated),
            "error": f"{type(exc).__name__}: {exc}",
        }


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
    sanitized_detail_json = sanitize_data_source_log_detail_json(detail_json)
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
            sanitized_detail_json,
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
    sanitized_detail_json = sanitize_data_source_log_detail_json(detail_json)

    def _txn(con: Any) -> None:
        append_data_source_log_row(
            con,
            ts_ms=int(ts_ms),
            source_key=str(source_key),
            level=str(level),
            event_type=str(event_type),
            message=str(message),
            detail_json=sanitized_detail_json,
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


__all__ = [
    "DATA_SOURCE_LOG_REDACTION_MARKER",
    "DATA_SOURCE_LOG_REDACTION_SQLITE_MARKER_KEY",
    "DATA_SOURCE_LOG_REDACTION_TIMESCALE_MARKER_KEY",
    "append_data_source_log_row",
    "data_source_log_detail_from_json",
    "data_source_log_detail_key_is_secret",
    "data_source_log_detail_to_json",
    "delete_data_source_logs_for_source",
    "ensure_data_source_logs_schema",
    "log_data_source_event",
    "redact_existing_data_source_log_details",
    "redact_existing_data_source_log_details_once",
    "redact_existing_timescale_data_source_log_details",
    "run_write_txn",
    "sanitize_data_source_log_detail",
    "sanitize_data_source_log_detail_json",
]
