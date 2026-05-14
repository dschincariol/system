"""Read-routing helpers for mirrored telemetry tables during SQLite -> Timescale cutover."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any

try:
    import psycopg2
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg2 = None  # type: ignore[assignment]

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect_ro
from engine.runtime.telemetry_migration_validation import get_telemetry_migration_validation_snapshot
from engine.runtime.timescale_client import TimescaleConfig, _quote_ident

LOG = get_logger("runtime.telemetry_read_router")
_READ_BACKEND = str(os.environ.get("TELEMETRY_READ_BACKEND", "auto") or "auto").strip().lower()
_READ_FALLBACK_TO_SQLITE = str(os.environ.get("TELEMETRY_READ_FALLBACK_TO_SQLITE", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_READ_REQUIRE_VALIDATION = str(os.environ.get("TELEMETRY_READ_REQUIRE_VALIDATION", "1")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_READ_BACKEND_MODES = {"auto", "sqlite", "timescale"}


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.telemetry_read_router",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _sqlite_table_exists(con: Any, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(name),),
    ).fetchone()
    return bool(row)


def _timescale_enabled() -> bool:
    config = TimescaleConfig.from_env()
    return bool(config.enabled and config.dsn and psycopg2 is not None)


def _read_backend_mode() -> str:
    return str(_READ_BACKEND if _READ_BACKEND in _READ_BACKEND_MODES else "auto")


def get_telemetry_read_backend() -> str:
    if _read_backend_mode() == "sqlite":
        return "sqlite"
    if _timescale_enabled():
        if _READ_REQUIRE_VALIDATION:
            try:
                snapshot = dict(get_telemetry_migration_validation_snapshot() or {})
            except Exception as exc:
                _warn_nonfatal("TELEMETRY_READ_ROUTER_VALIDATION_FETCH_FAILED", exc)
                return "sqlite"
            if not bool(snapshot.get("enabled")):
                return "sqlite"
            if not bool(snapshot.get("ok")):
                return "sqlite"
        return "timescale"
    return "sqlite"


@contextmanager
def _timescale_connection():
    config = TimescaleConfig.from_env()
    if psycopg2 is None or not str(config.dsn or "").strip():
        raise RuntimeError("timescale_telemetry_reader_not_configured")
    con = psycopg2.connect(
        dsn=str(config.dsn),
        connect_timeout=int(max(1, round(float(config.connect_timeout_s)))),
        application_name="trading-system-telemetry-read-router",
    )
    try:
        yield con, str(config.schema_name or "public")
    finally:
        con.close()


def _json_dict_or_empty(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _fetch_sqlite_runtime_metrics(*, metric: str | None, since_ms: int | None, limit: int) -> dict[str, Any]:
    con = connect_ro()
    try:
        if not _sqlite_table_exists(con, "runtime_metrics"):
            return {
                "ok": True,
                "metric": (str(metric) if metric else None),
                "since_ms": (int(since_ms) if since_ms is not None else None),
                "rows": [],
            }

        params: list[Any] = []
        where: list[str] = []
        if metric:
            where.append("metric = ?")
            params.append(str(metric))
        if since_ms is not None:
            where.append("ts_ms >= ?")
            params.append(int(since_ms))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        rows = con.execute(
            f"""
            SELECT ts_ms, metric, value_num, value_text, tags_json
            FROM runtime_metrics
            {where_sql}
            ORDER BY ts_ms DESC, id DESC
            LIMIT ?
            """,
            tuple(params + [int(limit)]),
        ).fetchall() or []
        return {
            "ok": True,
            "metric": (str(metric) if metric else None),
            "since_ms": (int(since_ms) if since_ms is not None else None),
            "rows": [
                {
                    "ts_ms": int(row[0] or 0),
                    "metric": str(row[1] or ""),
                    "value_num": (float(row[2]) if row[2] is not None else None),
                    "value_text": (str(row[3]) if row[3] is not None else None),
                    "tags": _json_dict_or_empty(row[4]),
                }
                for row in rows
            ],
        }
    finally:
        con.close()


def _fetch_timescale_runtime_metrics(*, metric: str | None, since_ms: int | None, limit: int) -> dict[str, Any]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        params: list[Any] = []
        where: list[str] = []
        if metric:
            where.append("metric = %s")
            params.append(str(metric))
        if since_ms is not None:
            where.append('"time" >= TO_TIMESTAMP(%s / 1000.0)')
            params.append(int(since_ms))
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        params.append(int(limit))
        with con.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  (EXTRACT(EPOCH FROM "time") * 1000)::BIGINT AS ts_ms,
                  metric,
                  value_num,
                  value_text,
                  tags_json::text
                FROM {schema_ref}.runtime_metrics
                {where_sql}
                ORDER BY "time" DESC, sqlite_rowid DESC
                LIMIT %s
                """,
                tuple(params),
            )
            rows = cur.fetchall() or []
        return {
            "ok": True,
            "metric": (str(metric) if metric else None),
            "since_ms": (int(since_ms) if since_ms is not None else None),
            "rows": [
                {
                    "ts_ms": int(row[0] or 0),
                    "metric": str(row[1] or ""),
                    "value_num": (float(row[2]) if row[2] is not None else None),
                    "value_text": (str(row[3]) if row[3] is not None else None),
                    "tags": _json_dict_or_empty(row[4]),
                }
                for row in rows
            ],
        }


def fetch_runtime_metrics(*, metric: str | None = None, since_ms: int | None = None, limit: int = 500) -> dict[str, Any]:
    bounded_limit = max(1, min(5000, int(limit or 500)))
    backend = get_telemetry_read_backend()
    if backend == "timescale":
        try:
            return _fetch_timescale_runtime_metrics(metric=metric, since_ms=since_ms, limit=bounded_limit)
        except Exception as exc:
            _warn_nonfatal(
                "TELEMETRY_READ_ROUTER_TIMESCALE_RUNTIME_METRICS_FAILED",
                exc,
                metric=(str(metric) if metric else ""),
                limit=int(bounded_limit),
            )
            if not _READ_FALLBACK_TO_SQLITE:
                raise
    return _fetch_sqlite_runtime_metrics(metric=metric, since_ms=since_ms, limit=bounded_limit)


def _fetch_sqlite_event_log_summary() -> dict[str, Any]:
    try:
        from engine.runtime.event_log import flush_event_log_buffer
        from engine.runtime.startup_write_gate import should_defer_noncritical_startup_write

        if not should_defer_noncritical_startup_write():
            flush_event_log_buffer(max_batches=64)
    except Exception as exc:
        _warn_nonfatal("TELEMETRY_READ_ROUTER_EVENT_LOG_FLUSH_FAILED", exc, scope="event_log_summary")
    con = connect_ro()
    try:
        if not _sqlite_table_exists(con, "event_log"):
            return {"ok": False, "count": 0, "last_ts_ms": None}
        row = con.execute("SELECT COUNT(*), MAX(ts_ms) FROM event_log").fetchone() or (0, None)
        count = int(row[0] or 0)
        last_ts_ms = (int(row[1]) if row[1] is not None else None)
        return {"ok": bool(count > 0 and last_ts_ms is not None), "count": count, "last_ts_ms": last_ts_ms}
    finally:
        con.close()


def _fetch_timescale_event_log_summary() -> dict[str, Any]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        with con.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*), MAX((EXTRACT(EPOCH FROM "time") * 1000)::BIGINT)
                FROM {schema_ref}.event_log
                """
            )
            row = cur.fetchone() or (0, None)
        count = int(row[0] or 0)
        last_ts_ms = (int(row[1]) if row[1] is not None else None)
        return {"ok": bool(count > 0 and last_ts_ms is not None), "count": count, "last_ts_ms": last_ts_ms}


def fetch_event_log_summary() -> dict[str, Any]:
    backend = get_telemetry_read_backend()
    if backend == "timescale":
        try:
            return _fetch_timescale_event_log_summary()
        except Exception as exc:
            _warn_nonfatal("TELEMETRY_READ_ROUTER_TIMESCALE_EVENT_LOG_SUMMARY_FAILED", exc)
            if not _READ_FALLBACK_TO_SQLITE:
                raise
    return _fetch_sqlite_event_log_summary()


def _fetch_sqlite_runtime_failure_events(*, limit: int) -> list[dict[str, Any]]:
    try:
        from engine.runtime.event_log import flush_event_log_buffer
        from engine.runtime.startup_write_gate import should_defer_noncritical_startup_write

        if not should_defer_noncritical_startup_write():
            flush_event_log_buffer(max_batches=64)
    except Exception as exc:
        _warn_nonfatal("TELEMETRY_READ_ROUTER_EVENT_LOG_FLUSH_FAILED", exc, scope="runtime_failure_events")
    con = connect_ro()
    try:
        if not _sqlite_table_exists(con, "event_log"):
            return []
        rows = con.execute(
            """
            SELECT ts_ms, event_source, payload_json
            FROM event_log
            WHERE event_type='runtime_failure'
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []
        return [
            {
                "ts_ms": int(row[0] or 0),
                "event_source": str(row[1] or ""),
                "payload": _json_dict_or_empty(row[2]),
            }
            for row in rows
        ]
    finally:
        con.close()


def _fetch_timescale_runtime_failure_events(*, limit: int) -> list[dict[str, Any]]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        with con.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  (EXTRACT(EPOCH FROM "time") * 1000)::BIGINT AS ts_ms,
                  event_source,
                  payload_json::text
                FROM {schema_ref}.event_log
                WHERE event_type = %s
                ORDER BY "time" DESC, sqlite_rowid DESC
                LIMIT %s
                """,
                ("runtime_failure", int(limit)),
            )
            rows = cur.fetchall() or []
        return [
            {
                "ts_ms": int(row[0] or 0),
                "event_source": str(row[1] or ""),
                "payload": _json_dict_or_empty(row[2]),
            }
            for row in rows
        ]


def fetch_recent_runtime_failure_events(*, limit: int = 10) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(1000, int(limit or 10)))
    backend = get_telemetry_read_backend()
    if backend == "timescale":
        try:
            return _fetch_timescale_runtime_failure_events(limit=bounded_limit)
        except Exception as exc:
            _warn_nonfatal(
                "TELEMETRY_READ_ROUTER_TIMESCALE_RUNTIME_FAILURES_FAILED",
                exc,
                limit=int(bounded_limit),
            )
            if not _READ_FALLBACK_TO_SQLITE:
                raise
    return _fetch_sqlite_runtime_failure_events(limit=bounded_limit)


def _fetch_sqlite_provider_health_rows() -> list[dict[str, Any]]:
    try:
        from engine.runtime.telemetry_append_buffer import flush_telemetry_append_buffers

        flush_telemetry_append_buffers(max_batches=64, tables=("price_provider_health",))
    except Exception as exc:
        _warn_nonfatal("TELEMETRY_READ_ROUTER_PROVIDER_HEALTH_FLUSH_FAILED", exc, scope="provider_health_rows")
    con = connect_ro()
    try:
        if not _sqlite_table_exists(con, "price_provider_health"):
            return []
        cols = {
            str(row[1] or "")
            for row in (con.execute("PRAGMA table_info(price_provider_health)").fetchall() or [])
            if row and len(row) > 1
        }
        latency_expr = "latency_ms" if "latency_ms" in cols else "NULL"
        n_symbols_expr = "n_symbols" if "n_symbols" in cols else "NULL"
        error_expr = "error" if "error" in cols else "NULL"
        rows = con.execute(
            f"""
            WITH latest AS (
                SELECT provider, MAX(ts_ms) AS last_ts_ms
                FROM price_provider_health
                GROUP BY provider
            )
            SELECT
                h.provider,
                h.ts_ms,
                h.ok,
                {latency_expr} AS latency_ms,
                {n_symbols_expr} AS n_symbols,
                {error_expr} AS error
            FROM price_provider_health h
            JOIN latest l
              ON l.provider = h.provider
             AND l.last_ts_ms = h.ts_ms
            ORDER BY h.provider ASC
            """
        ).fetchall() or []
        return [
            {
                "provider": str(row[0] or ""),
                "ts_ms": (int(row[1]) if row[1] is not None else None),
                "ok": bool(int(row[2] or 0) == 1) if row[2] is not None else False,
                "latency_ms": (float(row[3]) if row[3] is not None else None),
                "n_symbols": (int(row[4]) if row[4] is not None else None),
                "error": (str(row[5]) if row[5] is not None else None),
            }
            for row in rows
        ]
    finally:
        con.close()


def _fetch_timescale_provider_health_rows() -> list[dict[str, Any]]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        with con.cursor() as cur:
            cur.execute(
                f"""
                WITH latest AS (
                    SELECT provider, MAX("time") AS max_time
                    FROM {schema_ref}.price_provider_health
                    GROUP BY provider
                )
                SELECT
                  h.provider,
                  (EXTRACT(EPOCH FROM h."time") * 1000)::BIGINT AS ts_ms,
                  h.ok,
                  h.latency_ms,
                  h.n_symbols,
                  h.error
                FROM {schema_ref}.price_provider_health h
                JOIN latest l
                  ON l.provider = h.provider
                 AND l.max_time = h."time"
                ORDER BY h.provider ASC
                """
            )
            rows = cur.fetchall() or []
        return [
            {
                "provider": str(row[0] or ""),
                "ts_ms": (int(row[1]) if row[1] is not None else None),
                "ok": bool(row[2]) if row[2] is not None else False,
                "latency_ms": (float(row[3]) if row[3] is not None else None),
                "n_symbols": (int(row[4]) if row[4] is not None else None),
                "error": (str(row[5]) if row[5] is not None else None),
            }
            for row in rows
        ]


def fetch_provider_health_rows() -> list[dict[str, Any]]:
    backend = get_telemetry_read_backend()
    if backend == "timescale":
        try:
            return _fetch_timescale_provider_health_rows()
        except Exception as exc:
            _warn_nonfatal("TELEMETRY_READ_ROUTER_TIMESCALE_PROVIDER_HEALTH_FAILED", exc)
            if not _READ_FALLBACK_TO_SQLITE:
                raise
    return _fetch_sqlite_provider_health_rows()


def _fetch_sqlite_data_source_logs(*, source_key: str, limit: int) -> list[dict[str, Any]]:
    con = connect_ro()
    try:
        if not _sqlite_table_exists(con, "data_source_logs"):
            return []
        rows = con.execute(
            """
            SELECT ts_ms, source_key, level, event_type, message, detail_json
            FROM data_source_logs
            WHERE source_key = ?
            ORDER BY ts_ms DESC, id DESC
            LIMIT ?
            """,
            (str(source_key), int(limit)),
        ).fetchall() or []
        return [
            {
                "ts_ms": int(row[0] or 0),
                "source_key": str(row[1] or ""),
                "level": str(row[2] or ""),
                "event_type": str(row[3] or ""),
                "message": str(row[4] or ""),
                "detail": _json_dict_or_empty(row[5]),
            }
            for row in rows
        ]
    finally:
        con.close()


def _fetch_timescale_data_source_logs(*, source_key: str, limit: int) -> list[dict[str, Any]]:
    with _timescale_connection() as (con, schema_name):
        schema_ref = _quote_ident(schema_name)
        with con.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  (EXTRACT(EPOCH FROM "time") * 1000)::BIGINT AS ts_ms,
                  source_key,
                  level,
                  event_type,
                  message,
                  detail_json::text
                FROM {schema_ref}.data_source_logs
                WHERE source_key = %s
                ORDER BY "time" DESC, sqlite_rowid DESC
                LIMIT %s
                """,
                (str(source_key), int(limit)),
            )
            rows = cur.fetchall() or []
        return [
            {
                "ts_ms": int(row[0] or 0),
                "source_key": str(row[1] or ""),
                "level": str(row[2] or ""),
                "event_type": str(row[3] or ""),
                "message": str(row[4] or ""),
                "detail": _json_dict_or_empty(row[5]),
            }
            for row in rows
        ]


def fetch_data_source_logs(*, source_key: str, limit: int = 200) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit or 200), 1000))
    backend = get_telemetry_read_backend()
    if backend == "timescale":
        try:
            return _fetch_timescale_data_source_logs(source_key=str(source_key), limit=bounded_limit)
        except Exception as exc:
            _warn_nonfatal(
                "TELEMETRY_READ_ROUTER_TIMESCALE_DATA_SOURCE_LOGS_FAILED",
                exc,
                source_key=str(source_key),
                limit=int(bounded_limit),
            )
            if not _READ_FALLBACK_TO_SQLITE:
                raise
    return _fetch_sqlite_data_source_logs(source_key=str(source_key), limit=bounded_limit)


__all__ = [
    "fetch_data_source_logs",
    "fetch_event_log_summary",
    "fetch_provider_health_rows",
    "fetch_recent_runtime_failure_events",
    "fetch_runtime_metrics",
    "get_telemetry_read_backend",
]
