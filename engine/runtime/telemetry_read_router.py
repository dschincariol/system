"""Read-routing helpers for mirrored telemetry tables during SQLite -> Timescale cutover."""

from __future__ import annotations

import atexit
import json
import os
import threading
from contextlib import contextmanager
from typing import Any

try:
    import psycopg
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover - optional dependency at runtime
    psycopg = None  # type: ignore[assignment]
    ConnectionPool = None  # type: ignore[assignment]

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.state_cache import cache_get_or_load
from engine.runtime.storage import connect_ro
from engine.runtime.data_source_log_store import data_source_log_detail_from_json
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
_READ_CACHE_TTL_S = 0.75
_POOL_ROLE = "telemetry_read"
_CONFIG_ENV_KEYS = (
    "INGESTION_TUNING_PROFILE",
    "TIMESCALE_DSN",
    "TIMESCALE_URL",
    "TIMESCALE_DATABASE_URL",
    "TIMESCALE_ENABLED",
    "TIMESCALE_SCHEMA",
    "TIMESCALE_POOL_MIN_SIZE",
    "TIMESCALE_POOL_MAX_SIZE",
    "TIMESCALE_CONNECT_TIMEOUT_S",
    "TIMESCALE_LOCK_TIMEOUT_S",
    "TIMESCALE_COMMAND_TIMEOUT_S",
    "TIMESCALE_IDLE_IN_TXN_TIMEOUT_S",
    "TIMESCALE_APPLICATION_NAME",
)
_PG_PASSWORD_ENV_KEYS = (
    "TS_PG_PASSWORD_FILE",
    "TIMESCALE_PASSWORD_FILE",
    "TS_PG_PASSWORD_APP_FILE",
    "TS_PG_APP_PASSWORD_FILE",
    "TS_PG_PASSWORD_INGEST_FILE",
    "TS_PG_INGEST_PASSWORD_FILE",
    "TS_PG_PASSWORD_READER_FILE",
    "TS_PG_READER_PASSWORD_FILE",
    "PGPASSWORD_FILE",
    "TS_PG_PASSWORD_SECRET",
    "TIMESCALE_PASSWORD_SECRET",
    "TS_PG_PASSWORD_APP_SECRET",
    "TS_PG_APP_PASSWORD_SECRET",
    "TS_PG_PASSWORD_INGEST_SECRET",
    "TS_PG_INGEST_PASSWORD_SECRET",
    "TS_PG_PASSWORD_READER_SECRET",
    "TS_PG_READER_PASSWORD_SECRET",
    "PGPASSWORD_SECRET",
    "TS_PG_PASSWORD",
    "TIMESCALE_PASSWORD",
    "TS_PG_PASSWORD_APP",
    "TS_PG_APP_PASSWORD",
    "TS_PG_PASSWORD_INGEST",
    "TS_PG_INGEST_PASSWORD",
    "TS_PG_PASSWORD_READER",
    "TS_PG_READER_PASSWORD",
    "PGPASSWORD",
    "TS_SECRETS_PROVIDER",
    "TS_DEV_SECRETS_DIR",
    "CREDENTIALS_DIRECTORY",
)
_POOL_LOCK = threading.RLock()
_CONFIG_KEY: tuple[Any, ...] | None = None
_CONFIG: TimescaleConfig | None = None
_ACTIVE_POOL_KEY: tuple[Any, ...] | None = None
_POOLS: dict[tuple[Any, ...], Any] = {}


def _file_fingerprint(path: str) -> tuple[Any, ...]:
    text = str(path or "").strip()
    if not text:
        return ("", None)
    try:
        stat = os.stat(os.path.expanduser(text))
    except OSError:
        return (text, "missing")
    return (text, int(stat.st_mtime_ns), int(stat.st_size))


def _env_fingerprint(keys: tuple[str, ...]) -> tuple[Any, ...]:
    items: list[Any] = []
    for key in keys:
        value = os.environ.get(key)
        items.append((key, value))
        if key.endswith("_FILE") and value:
            items.append((f"{key}:stat", _file_fingerprint(value)))
    return tuple(items)


def _get_timescale_config() -> TimescaleConfig:
    global _CONFIG, _CONFIG_KEY
    key = _env_fingerprint(_CONFIG_ENV_KEYS + _PG_PASSWORD_ENV_KEYS)
    with _POOL_LOCK:
        if _CONFIG is not None and _CONFIG_KEY == key:
            return _CONFIG
        config = TimescaleConfig.from_env()
        _CONFIG = config
        _CONFIG_KEY = key
        return config


def _session_timeout_ms(timeout_s: Any) -> int:
    try:
        seconds = float(timeout_s)
    except (TypeError, ValueError):
        seconds = 1.0
    if seconds < 1.0:
        seconds = 1.0
    return int(seconds * 1000)


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
    config = _get_timescale_config()
    return bool(config.enabled and config.dsn and psycopg is not None and ConnectionPool is not None)


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


def _pool_application_name(config: TimescaleConfig) -> str:
    base = str(config.application_name or "trading-system").strip() or "trading-system"
    suffix = "telemetry-read-router"
    return base if suffix in base else f"{base}-{suffix}"


def _pool_key(config: TimescaleConfig) -> tuple[Any, ...]:
    return (
        _POOL_ROLE,
        str(config.dsn),
        str(config.schema_name or "public"),
        int(config.pool_min_size),
        int(config.pool_max_size),
        float(config.connect_timeout_s),
        float(config.lock_timeout_s),
        float(config.command_timeout_s),
        float(config.idle_in_txn_timeout_s),
        _pool_application_name(config),
    )


def _close_pool(pool: Any, *, timeout_s: float) -> None:
    try:
        pool.close(timeout=float(timeout_s))
    except TypeError:
        try:
            pool.close()
        except Exception as exc:
            LOG.debug("telemetry read pool close failed: %s", exc, exc_info=True)
    except Exception as exc:
        LOG.debug("telemetry read pool close failed: %s", exc, exc_info=True)


def close_timescale_read_pool() -> None:
    global _ACTIVE_POOL_KEY
    with _POOL_LOCK:
        pools = list(_POOLS.items())
        _POOLS.clear()
        _ACTIVE_POOL_KEY = None
    for key, pool in pools:
        timeout_s = float(key[5]) if len(key) > 5 else 1.0
        _close_pool(pool, timeout_s=timeout_s)


def _get_timescale_read_pool(config: TimescaleConfig) -> Any:
    global _ACTIVE_POOL_KEY
    if ConnectionPool is None or psycopg is None:
        raise RuntimeError("timescale_telemetry_reader_pool_unavailable")
    key = _pool_key(config)
    with _POOL_LOCK:
        pool = _POOLS.get(key)
        if pool is not None:
            _ACTIVE_POOL_KEY = key
            return pool
        old_keys = [pool_key for pool_key in _POOLS if pool_key[0] == _POOL_ROLE]
        for old_key in old_keys:
            old_pool = _POOLS.pop(old_key, None)
            if old_pool is not None:
                timeout_s = float(old_key[5]) if len(old_key) > 5 else float(config.connect_timeout_s)
                _close_pool(old_pool, timeout_s=timeout_s)
        pool = ConnectionPool(
            conninfo=str(config.dsn),
            min_size=int(config.pool_min_size),
            max_size=int(config.pool_max_size),
            timeout=float(config.connect_timeout_s),
            kwargs={
                "connect_timeout": int(max(1, round(float(config.connect_timeout_s)))),
                "application_name": _pool_application_name(config),
            },
            open=False,
        )
        try:
            pool.open(wait=True, timeout=float(config.connect_timeout_s))
        except Exception:
            _close_pool(pool, timeout_s=float(config.connect_timeout_s))
            raise
        _POOLS[key] = pool
        _ACTIVE_POOL_KEY = key
        return pool


def _prepare_timescale_connection(con: Any, config: TimescaleConfig) -> None:
    try:
        con.autocommit = True
    except Exception as exc:
        _warn_nonfatal("TELEMETRY_READ_ROUTER_AUTOCOMMIT_SET_FAILED", exc)
    with con.cursor() as cur:
        cur.execute(f"SET SESSION statement_timeout = {_session_timeout_ms(config.command_timeout_s)}")
        cur.execute(f"SET SESSION lock_timeout = {_session_timeout_ms(config.lock_timeout_s)}")
        cur.execute(
            "SET SESSION idle_in_transaction_session_timeout = "
            f"{_session_timeout_ms(config.idle_in_txn_timeout_s)}"
        )
        cur.execute("SET SESSION TIME ZONE 'UTC'")


@contextmanager
def _timescale_connection():
    config = _get_timescale_config()
    if psycopg is None or ConnectionPool is None or not str(config.dsn or "").strip():
        raise RuntimeError("timescale_telemetry_reader_not_configured")
    pool = _get_timescale_read_pool(config)
    con = pool.getconn(timeout=float(config.connect_timeout_s))
    discard = False
    try:
        _prepare_timescale_connection(con, config)
        yield con, str(config.schema_name or "public")
    except Exception:
        discard = True
        try:
            con.rollback()
        except Exception as exc:
            _warn_nonfatal("TELEMETRY_READ_ROUTER_ROLLBACK_FAILED", exc)
        raise
    finally:
        if discard:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("TELEMETRY_READ_ROUTER_CONNECTION_CLOSE_FAILED", exc)
        try:
            pool.putconn(con)
        except Exception as exc:
            _warn_nonfatal("TELEMETRY_READ_ROUTER_POOL_RETURN_FAILED", exc)


atexit.register(close_timescale_read_pool)


def _cached_read(cache_key: str, loader: Any) -> Any:
    return cache_get_or_load("telemetry_read_router", str(cache_key), loader, ttl_s=_READ_CACHE_TTL_S)


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


def _data_source_log_detail_or_empty(raw: Any) -> dict[str, Any]:
    return data_source_log_detail_from_json(raw)


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

    def _load() -> dict[str, Any]:
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

    return _cached_read(
        f"runtime_metrics:{backend}:{metric or ''}:{since_ms if since_ms is not None else ''}:{bounded_limit}",
        _load,
    )


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

    def _load() -> dict[str, Any]:
        if backend == "timescale":
            try:
                return _fetch_timescale_event_log_summary()
            except Exception as exc:
                _warn_nonfatal("TELEMETRY_READ_ROUTER_TIMESCALE_EVENT_LOG_SUMMARY_FAILED", exc)
                if not _READ_FALLBACK_TO_SQLITE:
                    raise
        return _fetch_sqlite_event_log_summary()

    return _cached_read(f"event_log_summary:{backend}", _load)


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

    def _load() -> list[dict[str, Any]]:
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

    return _cached_read(f"runtime_failure_events:{backend}:{bounded_limit}", _load)


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
        last_success_expr = "last_success_ts_ms" if "last_success_ts_ms" in cols else "NULL"
        error_count_expr = "error_count" if "error_count" in cols else "NULL"
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
                {error_expr} AS error,
                {last_success_expr} AS last_success_ts_ms,
                {error_count_expr} AS error_count
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
                "last_success_ts_ms": (int(row[6]) if row[6] is not None else None),
                "error_count": (int(row[7]) if row[7] is not None else None),
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
                  h.error,
                  h.last_success_ts_ms,
                  h.error_count
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
                "last_success_ts_ms": (int(row[6]) if row[6] is not None else None),
                "error_count": (int(row[7]) if row[7] is not None else None),
            }
            for row in rows
        ]


def fetch_provider_health_rows() -> list[dict[str, Any]]:
    backend = get_telemetry_read_backend()

    def _load() -> list[dict[str, Any]]:
        if backend == "timescale":
            try:
                return _fetch_timescale_provider_health_rows()
            except Exception as exc:
                _warn_nonfatal("TELEMETRY_READ_ROUTER_TIMESCALE_PROVIDER_HEALTH_FAILED", exc)
                if not _READ_FALLBACK_TO_SQLITE:
                    raise
        return _fetch_sqlite_provider_health_rows()

    return _cached_read(f"provider_health:{backend}", _load)


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
                "detail": _data_source_log_detail_or_empty(row[5]),
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
                "detail": _data_source_log_detail_or_empty(row[5]),
            }
            for row in rows
        ]


def fetch_data_source_logs(*, source_key: str, limit: int = 200) -> list[dict[str, Any]]:
    bounded_limit = max(1, min(int(limit or 200), 1000))
    backend = get_telemetry_read_backend()

    def _load() -> list[dict[str, Any]]:
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

    return _cached_read(f"data_source_logs:{backend}:{source_key}:{bounded_limit}", _load)


__all__ = [
    "fetch_data_source_logs",
    "fetch_event_log_summary",
    "fetch_provider_health_rows",
    "fetch_recent_runtime_failure_events",
    "fetch_runtime_metrics",
    "close_timescale_read_pool",
    "get_telemetry_read_backend",
]
