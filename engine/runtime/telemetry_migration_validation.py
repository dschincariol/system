"""Parity and health validation for mirrored telemetry Timescale tables."""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect_ro_direct
from engine.runtime.storage import get_timeseries_storage_snapshot
from engine.runtime.timescale_client import TimescaleConfig, asyncpg, _quote_ident

LOG = get_logger("runtime.telemetry_migration_validation")
_VALIDATION_LOCK = threading.RLock()
_VALIDATION_CACHE: dict[str, Any] = {"ts_ms": 0, "snapshot": None}

_TABLE_MAP: dict[str, tuple[str, str]] = {
    "runtime_metrics": ("runtime_metrics", "time"),
    "event_log": ("event_log", "time"),
    "ingestion_pipeline_health": ("ingestion_pipeline_health", "time"),
    "price_provider_health": ("price_provider_health", "time"),
    "weather_provider_health": ("weather_provider_health", "time"),
    "data_source_logs": ("data_source_logs", "time"),
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.telemetry_migration_validation",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _asyncpg_connect_available() -> bool:
    return asyncpg is not None and callable(getattr(asyncpg, "connect", None))


def _sqlite_summary(db_path: str, since_ts_ms: int) -> dict[str, Any]:
    del db_path
    out: dict[str, Any] = {}
    con = connect_ro_direct(timeout_s=5.0)
    try:
        for table_name in _TABLE_MAP:
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
            if not row:
                out[table_name] = {
                    "exists": False,
                    "count": 0,
                    "max_ts_ms": None,
                    "max_rowid": None,
                }
                continue
            row = con.execute(
                f"""
                SELECT COUNT(*), MAX(ts_ms), NULL
                FROM {table_name}
                WHERE ts_ms >= ?
                """,
                (int(since_ts_ms),),
            ).fetchone()
            out[table_name] = {
                "exists": True,
                "count": int(row[0] or 0),
                "max_ts_ms": (int(row[1]) if row and row[1] is not None else None),
                "max_rowid": (int(row[2]) if row and row[2] is not None else None),
            }
    finally:
        con.close()
    return out


async def _timescale_summary_async(config: TimescaleConfig, since_ts_ms: int) -> dict[str, Any]:
    if not _asyncpg_connect_available():
        raise RuntimeError("asyncpg_not_installed")
    conn = await asyncpg.connect(
        dsn=str(config.dsn),
        timeout=float(config.connect_timeout_s),
        server_settings={"application_name": "telemetry-migration-validation", "timezone": "UTC"},
    )
    try:
        out: dict[str, Any] = {}
        schema_ref = _quote_ident(str(config.schema_name or "public"))
        for sqlite_table, (timescale_table, time_column) in _TABLE_MAP.items():
            row = await conn.fetchrow(
                f"""
                SELECT
                  COUNT(*) AS count,
                  MAX((EXTRACT(EPOCH FROM { _quote_ident(time_column) }) * 1000)::BIGINT) AS max_ts_ms,
                  MAX(sqlite_rowid) AS max_rowid
                FROM {schema_ref}.{_quote_ident(timescale_table)}
                WHERE { _quote_ident(time_column) } >= TO_TIMESTAMP($1 / 1000.0)
                """,
                int(since_ts_ms),
            )
            row_dict = dict(row or {})
            out[sqlite_table] = {
                "exists": True,
                "count": int(row_dict.get("count") or 0),
                "max_ts_ms": (int(row_dict.get("max_ts_ms")) if row_dict.get("max_ts_ms") is not None else None),
                "max_rowid": (int(row_dict.get("max_rowid")) if row_dict.get("max_rowid") is not None else None),
            }
        return out
    finally:
        await conn.close()


def _timescale_summary(config: TimescaleConfig, since_ts_ms: int) -> dict[str, Any]:
    return asyncio.run(_timescale_summary_async(config, since_ts_ms))


def _table_comparison(
    *,
    table_name: str,
    sqlite_summary: dict[str, Any],
    timescale_summary: dict[str, Any],
    max_count_delta: int,
    max_last_ts_lag_ms: int,
) -> dict[str, Any]:
    sqlite_row = dict((sqlite_summary or {}).get(table_name) or {})
    timescale_row = dict((timescale_summary or {}).get(table_name) or {})
    sqlite_count = int(sqlite_row.get("count") or 0)
    timescale_count = int(timescale_row.get("count") or 0)
    count_delta = int(sqlite_count - timescale_count)
    sqlite_max_ts_ms = sqlite_row.get("max_ts_ms")
    timescale_max_ts_ms = timescale_row.get("max_ts_ms")
    sqlite_max_rowid = sqlite_row.get("max_rowid")
    timescale_max_rowid = timescale_row.get("max_rowid")
    max_ts_lag_ms = None
    if sqlite_max_ts_ms is not None and timescale_max_ts_ms is not None:
        max_ts_lag_ms = abs(int(sqlite_max_ts_ms) - int(timescale_max_ts_ms))
    rowid_lag = None
    if sqlite_max_rowid is not None and timescale_max_rowid is not None:
        rowid_lag = abs(int(sqlite_max_rowid) - int(timescale_max_rowid))
    count_ok = abs(int(count_delta)) <= int(max_count_delta)
    lag_ok = max_ts_lag_ms is None or int(max_ts_lag_ms) <= int(max_last_ts_lag_ms)
    return {
        "sqlite_count": int(sqlite_count),
        "timescale_count": int(timescale_count),
        "count_delta": int(count_delta),
        "sqlite_max_ts_ms": (int(sqlite_max_ts_ms) if sqlite_max_ts_ms is not None else None),
        "timescale_max_ts_ms": (int(timescale_max_ts_ms) if timescale_max_ts_ms is not None else None),
        "max_ts_lag_ms": (int(max_ts_lag_ms) if max_ts_lag_ms is not None else None),
        "sqlite_max_rowid": (int(sqlite_max_rowid) if sqlite_max_rowid is not None else None),
        "timescale_max_rowid": (int(timescale_max_rowid) if timescale_max_rowid is not None else None),
        "rowid_lag": (int(rowid_lag) if rowid_lag is not None else None),
        "count_ok": bool(count_ok),
        "lag_ok": bool(lag_ok),
        "ok": bool(count_ok and lag_ok),
    }


def build_telemetry_migration_validation_snapshot(
    *,
    lookback_minutes: int,
    max_count_delta: int,
    max_last_ts_lag_ms: int,
    require_healthy_mirror: bool,
    require_healthy_timescale: bool,
) -> dict[str, Any]:
    now_ts_ms = int(time.time() * 1000)
    since_ts_ms = int(now_ts_ms - max(1, int(lookback_minutes)) * 60_000)
    reasons: list[str] = []
    try:
        from engine.runtime.telemetry_append_buffer import flush_telemetry_append_buffers

        flush_telemetry_append_buffers(max_batches=64)
    except Exception as exc:
        _warn_nonfatal("TELEMETRY_MIGRATION_VALIDATION_FLUSH_FAILED", exc, scope="validation_snapshot")
    storage_snapshot = dict(get_timeseries_storage_snapshot() or {})
    telemetry_mirror_snapshot = dict(storage_snapshot.get("telemetry_mirror") or {})
    if require_healthy_mirror:
        if not bool(telemetry_mirror_snapshot.get("enabled")):
            reasons.append("telemetry_mirror_disabled")
        elif not bool(telemetry_mirror_snapshot.get("ok")):
            reasons.append("telemetry_mirror_not_ok")

    if require_healthy_timescale:
        if not bool(storage_snapshot.get("enabled")):
            reasons.append("timescale_disabled")
        elif not bool(storage_snapshot.get("ok")):
            reasons.append("timescale_not_ok")

    config = TimescaleConfig.from_env()
    if not bool(config.enabled):
        reasons.append("timescale_disabled")
    if not str(config.dsn or "").strip():
        reasons.append("timescale_dsn_missing")
    if asyncpg is None:
        reasons.append("timescale_driver_unavailable")

    sqlite_summary: dict[str, Any] = {}
    timescale_summary: dict[str, Any] = {}
    sqlite_error = ""
    timescale_error = ""

    try:
        sqlite_summary = _sqlite_summary("", int(since_ts_ms))
    except Exception as exc:
        sqlite_error = f"{type(exc).__name__}:{exc}"
        reasons.append("sqlite_summary_failed")
        _warn_nonfatal("TELEMETRY_MIGRATION_SQLITE_SUMMARY_FAILED", exc, since_ts_ms=int(since_ts_ms))

    if not any(reason for reason in reasons if reason in {"timescale_disabled", "timescale_dsn_missing", "timescale_driver_unavailable"}):
        try:
            timescale_summary = _timescale_summary(config, int(since_ts_ms))
        except Exception as exc:
            timescale_error = f"{type(exc).__name__}:{exc}"
            reasons.append("timescale_summary_failed")
            _warn_nonfatal("TELEMETRY_MIGRATION_TIMESCALE_SUMMARY_FAILED", exc, since_ts_ms=int(since_ts_ms))

    tables: dict[str, Any] = {}
    if sqlite_summary and timescale_summary:
        for table_name in _TABLE_MAP:
            comparison = _table_comparison(
                table_name=str(table_name),
                sqlite_summary=sqlite_summary,
                timescale_summary=timescale_summary,
                max_count_delta=int(max_count_delta),
                max_last_ts_lag_ms=int(max_last_ts_lag_ms),
            )
            tables[str(table_name)] = comparison
            if not bool(comparison.get("ok")):
                reasons.append(f"{table_name}_parity_out_of_bounds")

    detail = "validation_ok" if not reasons else ",".join(reasons[:6])
    return {
        "ok": not reasons,
        "enabled": True,
        "detail": str(detail),
        "reasons": list(reasons),
        "lookback_minutes": int(max(1, int(lookback_minutes))),
        "since_ts_ms": int(since_ts_ms),
        "max_count_delta": int(max_count_delta),
        "max_last_ts_lag_ms": int(max_last_ts_lag_ms),
        "require_healthy_mirror": bool(require_healthy_mirror),
        "require_healthy_timescale": bool(require_healthy_timescale),
        "sqlite": dict(sqlite_summary),
        "timescale": dict(timescale_summary),
        "tables": dict(tables),
        "timeseries_storage": dict(storage_snapshot),
        "telemetry_mirror": dict(telemetry_mirror_snapshot),
        "sqlite_error": str(sqlite_error or ""),
        "timescale_error": str(timescale_error or ""),
        "ts_ms": int(now_ts_ms),
    }


def get_telemetry_migration_validation_snapshot(*, force: bool = False) -> dict[str, Any]:
    enabled = _env_bool("TIMESCALE_TELEMETRY_VALIDATION_ENABLED", default=False)
    if not enabled:
        return {
            "ok": True,
            "enabled": False,
            "detail": "validation_disabled",
            "reasons": [],
            "ts_ms": int(time.time() * 1000),
        }

    ttl_s = max(0.0, _env_float("TIMESCALE_TELEMETRY_VALIDATION_CACHE_TTL_S", 15.0))
    now_ts_ms = int(time.time() * 1000)
    with _VALIDATION_LOCK:
        cached = dict(_VALIDATION_CACHE.get("snapshot") or {})
        cached_ts_ms = int(_VALIDATION_CACHE.get("ts_ms") or 0)
        if (not force) and cached and ttl_s > 0 and (now_ts_ms - cached_ts_ms) <= int(ttl_s * 1000.0):
            cached["cached"] = True
            return cached

    snapshot = build_telemetry_migration_validation_snapshot(
        lookback_minutes=max(1, _env_int("TIMESCALE_TELEMETRY_VALIDATE_LOOKBACK_MINUTES", 5)),
        max_count_delta=max(0, _env_int("TIMESCALE_TELEMETRY_MAX_COUNT_DELTA", 0)),
        max_last_ts_lag_ms=max(0, _env_int("TIMESCALE_TELEMETRY_MAX_LAST_TS_LAG_MS", 5000)),
        require_healthy_mirror=_env_bool("TIMESCALE_TELEMETRY_REQUIRE_HEALTHY_MIRROR", default=True),
        require_healthy_timescale=_env_bool("TIMESCALE_TELEMETRY_REQUIRE_HEALTHY_TIMESCALE", default=True),
    )
    snapshot["cached"] = False
    with _VALIDATION_LOCK:
        _VALIDATION_CACHE["ts_ms"] = int(now_ts_ms)
        _VALIDATION_CACHE["snapshot"] = dict(snapshot)
    return snapshot


__all__ = [
    "build_telemetry_migration_validation_snapshot",
    "get_telemetry_migration_validation_snapshot",
]
