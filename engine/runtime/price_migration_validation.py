"""Parity and health guardrails for SQLite -> Timescale price cutover."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

from engine.runtime.async_writer import get_async_writer
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect_ro_direct
from engine.runtime.storage_pg_prices import (
    PostgresPriceStorageConfig,
    _quote_ident,
    get_price_storage,
    psycopg2,
)

LOG = get_logger("runtime.price_migration_validation")
_VALIDATION_LOCK = threading.RLock()
_VALIDATION_CACHE: dict[str, Any] = {"ts_ms": 0, "snapshot": None}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.price_migration_validation",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _sqlite_summary(db_path: str, since_ts_ms: int) -> dict[str, Any]:
    del db_path
    con = connect_ro_direct(timeout_s=5.0)
    try:
        out: dict[str, Any] = {}
        for table_name in ("prices", "price_quotes", "price_quotes_raw"):
            row = con.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = ANY (current_schemas(false))
                  AND table_name=?
                LIMIT 1
                """,
                (str(table_name),),
            ).fetchone()
            exists = bool(row)
            if not exists:
                out[table_name] = {
                    "exists": False,
                    "count": 0,
                    "min_ts_ms": None,
                    "max_ts_ms": None,
                }
                continue
            row = con.execute(
                f"""
                SELECT COUNT(*), MIN(ts_ms), MAX(ts_ms)
                FROM {table_name}
                WHERE ts_ms >= ?
                """,
                (int(since_ts_ms),),
            ).fetchone()
            out[table_name] = {
                "exists": True,
                "count": int(row[0] or 0),
                "min_ts_ms": (int(row[1]) if row and row[1] is not None else None),
                "max_ts_ms": (int(row[2]) if row and row[2] is not None else None),
            }
        return out
    finally:
        con.close()


def _timescale_summary(config: PostgresPriceStorageConfig, since_ts_ms: int) -> dict[str, Any]:
    if psycopg2 is None:
        raise RuntimeError("psycopg2_not_installed")
    schema_ref = _quote_ident(str(config.schema_name or "public"))
    con = psycopg2.connect(
        dsn=str(config.dsn),
        connect_timeout=int(max(1, round(float(config.connect_timeout_s)))),
        application_name="trading-system-price-migration-validation",
    )
    try:
        out: dict[str, Any] = {}
        with con.cursor() as cur:
            for table_name in ("price_ticks", "price_quotes", "price_quotes_raw"):
                cur.execute(
                    f"""
                    SELECT
                      COUNT(*),
                      MIN((EXTRACT(EPOCH FROM "time") * 1000)::BIGINT),
                      MAX((EXTRACT(EPOCH FROM "time") * 1000)::BIGINT)
                    FROM {schema_ref}.{table_name}
                    WHERE "time" >= TO_TIMESTAMP(%s / 1000.0)
                    """,
                    (int(since_ts_ms),),
                )
                row = cur.fetchone()
                out[table_name] = {
                    "exists": True,
                    "count": int(row[0] or 0),
                    "min_ts_ms": (int(row[1]) if row and row[1] is not None else None),
                    "max_ts_ms": (int(row[2]) if row and row[2] is not None else None),
                }
        return out
    finally:
        con.close()


def _table_comparison(
    *,
    sqlite_name: str,
    timescale_name: str,
    sqlite_summary: dict[str, Any],
    timescale_summary: dict[str, Any],
    max_count_delta: int,
    max_last_ts_lag_ms: int,
) -> dict[str, Any]:
    sqlite_row = dict((sqlite_summary or {}).get(sqlite_name) or {})
    timescale_row = dict((timescale_summary or {}).get(timescale_name) or {})
    sqlite_count = int(sqlite_row.get("count") or 0)
    timescale_count = int(timescale_row.get("count") or 0)
    count_delta = int(sqlite_count - timescale_count)
    abs_count_delta = abs(int(count_delta))
    sqlite_max_ts_ms = sqlite_row.get("max_ts_ms")
    timescale_max_ts_ms = timescale_row.get("max_ts_ms")
    max_ts_lag_ms = None
    if sqlite_max_ts_ms is not None and timescale_max_ts_ms is not None:
        max_ts_lag_ms = abs(int(sqlite_max_ts_ms) - int(timescale_max_ts_ms))
    count_ok = abs_count_delta <= int(max_count_delta)
    lag_ok = max_ts_lag_ms is None or max_ts_lag_ms <= int(max_last_ts_lag_ms)
    return {
        "sqlite_table": str(sqlite_name),
        "timescale_table": str(timescale_name),
        "sqlite_count": int(sqlite_count),
        "timescale_count": int(timescale_count),
        "count_delta": int(count_delta),
        "abs_count_delta": int(abs_count_delta),
        "sqlite_max_ts_ms": (int(sqlite_max_ts_ms) if sqlite_max_ts_ms is not None else None),
        "timescale_max_ts_ms": (int(timescale_max_ts_ms) if timescale_max_ts_ms is not None else None),
        "max_ts_lag_ms": (int(max_ts_lag_ms) if max_ts_lag_ms is not None else None),
        "count_ok": bool(count_ok),
        "lag_ok": bool(lag_ok),
        "ok": bool(count_ok and lag_ok),
    }


def build_price_migration_validation_snapshot(
    *,
    lookback_minutes: int,
    max_count_delta: int,
    max_last_ts_lag_ms: int,
    require_async_writer: bool,
    require_pg_storage: bool,
    max_queue_depth: int,
) -> dict[str, Any]:
    now_ts_ms = int(time.time() * 1000)
    effective_lookback_minutes = max(1, int(lookback_minutes))
    since_ts_ms = int(now_ts_ms - effective_lookback_minutes * 60_000)
    async_writer_snapshot = dict(get_async_writer().get_snapshot() or {})
    pg_storage_snapshot = dict(get_price_storage().get_snapshot() or {})
    config = PostgresPriceStorageConfig.from_env()
    reasons: list[str] = []
    sqlite_summary: dict[str, Any] = {}
    timescale_summary: dict[str, Any] = {}
    tables: dict[str, Any] = {}

    if require_async_writer:
        if not bool(async_writer_snapshot.get("enabled")):
            reasons.append("async_price_writer_disabled")
        elif not bool(async_writer_snapshot.get("ok")):
            reasons.append("async_price_writer_not_ok")
        if int(async_writer_snapshot.get("queue_depth") or 0) > int(max_queue_depth):
            reasons.append("async_price_writer_queue_depth_exceeded")

    if require_pg_storage:
        if not bool(pg_storage_snapshot.get("enabled")):
            reasons.append("pg_price_storage_disabled")
        elif not bool(pg_storage_snapshot.get("ok")):
            reasons.append("pg_price_storage_not_ok")

    if not bool(config.enabled):
        reasons.append("timescale_price_storage_disabled")
    if not str(config.dsn or "").strip():
        reasons.append("timescale_price_storage_dsn_missing")
    if psycopg2 is None:
        reasons.append("timescale_driver_unavailable")

    sqlite_error = ""
    timescale_error = ""
    try:
        sqlite_summary = _sqlite_summary("", int(since_ts_ms))
    except Exception as exc:
        sqlite_error = f"{type(exc).__name__}:{exc}"
        reasons.append("sqlite_summary_failed")
        _warn_nonfatal("PRICE_MIGRATION_SQLITE_SUMMARY_FAILED", exc, since_ts_ms=int(since_ts_ms))

    if not any(
        reason
        for reason in reasons
        if reason in {"timescale_price_storage_disabled", "timescale_price_storage_dsn_missing", "timescale_driver_unavailable"}
    ):
        try:
            timescale_summary = _timescale_summary(config, int(since_ts_ms))
        except Exception as exc:
            timescale_error = f"{type(exc).__name__}:{exc}"
            reasons.append("timescale_summary_failed")
            _warn_nonfatal("PRICE_MIGRATION_TIMESCALE_SUMMARY_FAILED", exc, since_ts_ms=int(since_ts_ms))

    if sqlite_summary and timescale_summary:
        tables = {
            "prices": _table_comparison(
                sqlite_name="prices",
                timescale_name="price_ticks",
                sqlite_summary=sqlite_summary,
                timescale_summary=timescale_summary,
                max_count_delta=int(max_count_delta),
                max_last_ts_lag_ms=int(max_last_ts_lag_ms),
            ),
            "quotes": _table_comparison(
                sqlite_name="price_quotes",
                timescale_name="price_quotes",
                sqlite_summary=sqlite_summary,
                timescale_summary=timescale_summary,
                max_count_delta=int(max_count_delta),
                max_last_ts_lag_ms=int(max_last_ts_lag_ms),
            ),
            "raw": _table_comparison(
                sqlite_name="price_quotes_raw",
                timescale_name="price_quotes_raw",
                sqlite_summary=sqlite_summary,
                timescale_summary=timescale_summary,
                max_count_delta=int(max_count_delta),
                max_last_ts_lag_ms=int(max_last_ts_lag_ms),
            ),
        }
        for name, comparison in tables.items():
            if not bool(comparison.get("ok")):
                reasons.append(f"{name}_parity_out_of_bounds")

    detail = "validation_ok" if not reasons else ",".join(reasons[:6])
    return {
        "ok": not reasons,
        "enabled": True,
        "detail": str(detail),
        "reasons": list(reasons),
        "lookback_minutes": int(effective_lookback_minutes),
        "since_ts_ms": int(since_ts_ms),
        "max_count_delta": int(max_count_delta),
        "max_last_ts_lag_ms": int(max_last_ts_lag_ms),
        "max_queue_depth": int(max_queue_depth),
        "require_async_writer": bool(require_async_writer),
        "require_pg_storage": bool(require_pg_storage),
        "sqlite": dict(sqlite_summary),
        "timescale": dict(timescale_summary),
        "tables": dict(tables),
        "sqlite_error": str(sqlite_error or ""),
        "timescale_error": str(timescale_error or ""),
        "async_price_writer": dict(async_writer_snapshot),
        "pg_price_storage": dict(pg_storage_snapshot),
        "ts_ms": int(now_ts_ms),
    }


def get_price_migration_validation_snapshot(*, force: bool = False) -> dict[str, Any]:
    enabled = _env_bool("PRICE_MIGRATION_VALIDATION_ENABLED", default=False)
    if not enabled:
        return {
            "ok": True,
            "enabled": False,
            "detail": "validation_disabled",
            "reasons": [],
            "ts_ms": int(time.time() * 1000),
        }

    cache_ttl_s = max(0.0, _env_float("PRICE_MIGRATION_VALIDATION_CACHE_TTL_S", 15.0))
    now_ts_ms = int(time.time() * 1000)
    with _VALIDATION_LOCK:
        cached = dict(_VALIDATION_CACHE.get("snapshot") or {})
        cached_ts_ms = int(_VALIDATION_CACHE.get("ts_ms") or 0)
        if (not force) and cached and cache_ttl_s > 0 and (now_ts_ms - cached_ts_ms) <= int(cache_ttl_s * 1000.0):
            cached["cached"] = True
            return cached

    snapshot = build_price_migration_validation_snapshot(
        lookback_minutes=max(1, _env_int("PRICE_MIGRATION_VALIDATE_LOOKBACK_MINUTES", 5)),
        max_count_delta=max(0, _env_int("PRICE_MIGRATION_MAX_COUNT_DELTA", 0)),
        max_last_ts_lag_ms=max(0, _env_int("PRICE_MIGRATION_MAX_LAST_TS_LAG_MS", 5000)),
        require_async_writer=_env_bool("PRICE_MIGRATION_REQUIRE_HEALTHY_ASYNC_WRITER", default=True),
        require_pg_storage=_env_bool("PRICE_MIGRATION_REQUIRE_HEALTHY_PG_STORAGE", default=True),
        max_queue_depth=max(0, _env_int("PRICE_MIGRATION_MAX_QUEUE_DEPTH", 0)),
    )
    snapshot["cached"] = False
    with _VALIDATION_LOCK:
        _VALIDATION_CACHE["ts_ms"] = int(now_ts_ms)
        _VALIDATION_CACHE["snapshot"] = dict(snapshot)
    return snapshot


__all__ = [
    "build_price_migration_validation_snapshot",
    "get_price_migration_validation_snapshot",
]
