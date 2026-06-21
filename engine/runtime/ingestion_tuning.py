"""Bounded ingestion tuning knobs and preflight checks."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}
HOST_32T_123G_PROFILE = "host_32t_123g"

_PROFILE_ALIASES = {
    "": "safe",
    "safe": "safe",
    "default": "safe",
    "off": "safe",
    "host_32t_123g": HOST_32T_123G_PROFILE,
    "32t_123g": HOST_32T_123G_PROFILE,
    "32-thread-123g": HOST_32T_123G_PROFILE,
    "32_thread_123g": HOST_32T_123G_PROFILE,
    "production_32t_123g": HOST_32T_123G_PROFILE,
}

_HOST_32T_123G_DEFAULTS: dict[str, int | float | str] = {
    "INGESTION_CHILD_TS_PG_POOL_SIZE": 3,
    "INGESTION_CHILD_TS_PG_POOL_MIN_SIZE": 1,
    "INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE": 4,
    "INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE": 4,
    "ASYNC_PRICE_WRITER_QUEUE_MAXSIZE": 1024,
    "ASYNC_PRICE_WRITER_BATCH_SIZE": 512,
    "ASYNC_PRICE_WRITER_FLUSH_INTERVAL_S": 0.25,
    "TELEMETRY_APPEND_BUFFER_MAX_BATCH": 256,
    "TELEMETRY_APPEND_BUFFER_MAX_ROWS": 4096,
    "TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S": 0.25,
    "TIMESCALE_POOL_MAX_SIZE": 8,
    "TIMESCALE_BATCH_SIZE": 1000,
    "TIMESCALE_FLUSH_INTERVAL_S": 0.5,
    "TIMESCALE_QUEUE_MAXSIZE": 512,
    "TIMESCALE_BACKPRESSURE_TIMEOUT_S": 3.0,
    "TIMESCALE_PRICES_POOL_MAX_SIZE": 8,
    "TS_REDIS_POOL_SIZE": 32,
    "INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS": 32,
}

_HOST_32T_123G_PG_POOL_DEFAULTS = {
    "application": 4,
    "ingestion": 12,
    "jobs": 3,
}


@dataclass(frozen=True)
class Bound:
    name: str
    default: int | float | str
    minimum: int | float
    maximum: int | float
    kind: str


BOUNDS: dict[str, Bound] = {
    "ASYNC_PRICE_WRITER_QUEUE_MAXSIZE": Bound("ASYNC_PRICE_WRITER_QUEUE_MAXSIZE", 2048, 32, 32768, "int"),
    "ASYNC_PRICE_WRITER_BATCH_SIZE": Bound("ASYNC_PRICE_WRITER_BATCH_SIZE", 256, 1, 4096, "int"),
    "ASYNC_PRICE_WRITER_FLUSH_INTERVAL_S": Bound("ASYNC_PRICE_WRITER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0, "float"),
    "ASYNC_PRICE_WRITER_RETRY_ATTEMPTS": Bound("ASYNC_PRICE_WRITER_RETRY_ATTEMPTS", 4, 1, 10, "int"),
    "ASYNC_PRICE_WRITER_RETRY_BASE_S": Bound("ASYNC_PRICE_WRITER_RETRY_BASE_S", 0.25, 0.01, 5.0, "float"),
    "ASYNC_PRICE_WRITER_RETRY_MAX_S": Bound("ASYNC_PRICE_WRITER_RETRY_MAX_S", 5.0, 0.1, 30.0, "float"),
    "ASYNC_PRICE_WRITER_ENQUEUE_TIMEOUT_S": Bound("ASYNC_PRICE_WRITER_ENQUEUE_TIMEOUT_S", 0.05, 0.0, 5.0, "float"),
    "ASYNC_PRICE_WRITER_SPOOL_MAX_BYTES": Bound("ASYNC_PRICE_WRITER_SPOOL_MAX_BYTES", 268435456, 1048576, 8589934592, "int"),
    "ASYNC_PRICE_WRITER_SPOOL_BUSY_TIMEOUT_MS": Bound("ASYNC_PRICE_WRITER_SPOOL_BUSY_TIMEOUT_MS", 50, 10, 60000, "int"),
    "TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S": Bound("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0, "float"),
    "TELEMETRY_APPEND_BUFFER_FLUSH_JITTER_RATIO": Bound("TELEMETRY_APPEND_BUFFER_FLUSH_JITTER_RATIO", 0.25, 0.0, 1.0, "float"),
    "TELEMETRY_APPEND_BUFFER_MAX_BATCH": Bound("TELEMETRY_APPEND_BUFFER_MAX_BATCH", 128, 1, 4096, "int"),
    "TELEMETRY_APPEND_BUFFER_MAX_ROWS": Bound("TELEMETRY_APPEND_BUFFER_MAX_ROWS", 4096, 1, 65536, "int"),
    "TIMESCALE_POOL_MIN_SIZE": Bound("TIMESCALE_POOL_MIN_SIZE", 1, 1, 16, "int"),
    "TIMESCALE_POOL_MAX_SIZE": Bound("TIMESCALE_POOL_MAX_SIZE", 4, 1, 16, "int"),
    "TIMESCALE_BATCH_SIZE": Bound("TIMESCALE_BATCH_SIZE", 500, 1, 5000, "int"),
    "TIMESCALE_FLUSH_INTERVAL_S": Bound("TIMESCALE_FLUSH_INTERVAL_S", 1.0, 0.05, 10.0, "float"),
    "TIMESCALE_QUEUE_MAXSIZE": Bound("TIMESCALE_QUEUE_MAXSIZE", 1024, 1, 32768, "int"),
    "TIMESCALE_RETRY_ATTEMPTS": Bound("TIMESCALE_RETRY_ATTEMPTS", 5, 1, 10, "int"),
    "TIMESCALE_RETRY_BASE_S": Bound("TIMESCALE_RETRY_BASE_S", 0.25, 0.01, 5.0, "float"),
    "TIMESCALE_RETRY_MAX_S": Bound("TIMESCALE_RETRY_MAX_S", 5.0, 0.1, 30.0, "float"),
    "TIMESCALE_BACKPRESSURE_TIMEOUT_S": Bound("TIMESCALE_BACKPRESSURE_TIMEOUT_S", 5.0, 0.05, 30.0, "float"),
    "TIMESCALE_START_TIMEOUT_S": Bound("TIMESCALE_START_TIMEOUT_S", 5.0, 0.1, 30.0, "float"),
    "TIMESCALE_CONNECT_TIMEOUT_S": Bound("TIMESCALE_CONNECT_TIMEOUT_S", 5.0, 0.1, 30.0, "float"),
    "TIMESCALE_LOCK_TIMEOUT_S": Bound("TIMESCALE_LOCK_TIMEOUT_S", 5.0, 0.05, 30.0, "float"),
    "TIMESCALE_COMMAND_TIMEOUT_S": Bound("TIMESCALE_COMMAND_TIMEOUT_S", 30.0, 1.0, 120.0, "float"),
    "TIMESCALE_IDLE_IN_TXN_TIMEOUT_S": Bound("TIMESCALE_IDLE_IN_TXN_TIMEOUT_S", 60.0, 1.0, 300.0, "float"),
    "TIMESCALE_PRICES_POOL_MIN_SIZE": Bound("TIMESCALE_PRICES_POOL_MIN_SIZE", 1, 1, 16, "int"),
    "TIMESCALE_PRICES_POOL_MAX_SIZE": Bound("TIMESCALE_PRICES_POOL_MAX_SIZE", 4, 1, 16, "int"),
    "TIMESCALE_PRICES_CONNECT_TIMEOUT_S": Bound("TIMESCALE_PRICES_CONNECT_TIMEOUT_S", 5.0, 0.1, 30.0, "float"),
    "TIMESCALE_PRICES_LOCK_TIMEOUT_S": Bound("TIMESCALE_PRICES_LOCK_TIMEOUT_S", 5.0, 0.05, 30.0, "float"),
    "TIMESCALE_PRICES_COMMAND_TIMEOUT_S": Bound("TIMESCALE_PRICES_COMMAND_TIMEOUT_S", 30.0, 1.0, 120.0, "float"),
    "TIMESCALE_PRICES_IDLE_IN_TXN_TIMEOUT_S": Bound("TIMESCALE_PRICES_IDLE_IN_TXN_TIMEOUT_S", 60.0, 1.0, 300.0, "float"),
    "TIMESCALE_PRICES_RETRY_ATTEMPTS": Bound("TIMESCALE_PRICES_RETRY_ATTEMPTS", 3, 1, 10, "int"),
    "TIMESCALE_PRICES_RETRY_BASE_S": Bound("TIMESCALE_PRICES_RETRY_BASE_S", 0.25, 0.01, 5.0, "float"),
    "TIMESCALE_PRICES_RETRY_MAX_S": Bound("TIMESCALE_PRICES_RETRY_MAX_S", 5.0, 0.1, 30.0, "float"),
    "TS_PG_POOL_SIZE": Bound("TS_PG_POOL_SIZE", 4, 1, 32, "int"),
    "TS_PG_POOL_MIN_SIZE": Bound("TS_PG_POOL_MIN_SIZE", 2, 1, 16, "int"),
    "INGESTION_CHILD_TS_PG_POOL_SIZE": Bound("INGESTION_CHILD_TS_PG_POOL_SIZE", 2, 1, 8, "int"),
    "INGESTION_CHILD_TS_PG_POOL_MIN_SIZE": Bound("INGESTION_CHILD_TS_PG_POOL_MIN_SIZE", 1, 1, 8, "int"),
    "INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE": Bound("INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE", 2, 1, 8, "int"),
    "INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE": Bound("INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE", 2, 1, 8, "int"),
    "TS_REDIS_POOL_SIZE": Bound("TS_REDIS_POOL_SIZE", 16, 1, 64, "int"),
    "TS_REDIS_CONNECT_TIMEOUT_S": Bound("TS_REDIS_CONNECT_TIMEOUT_S", 0.25, 0.05, 5.0, "float"),
    "TS_REDIS_SOCKET_TIMEOUT_S": Bound("TS_REDIS_SOCKET_TIMEOUT_S", 0.25, 0.05, 5.0, "float"),
    "INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS": Bound("INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS", 24, 4, 64, "int"),
    "INGESTION_TUNING_MAX_BUFFERED_ROWS": Bound("INGESTION_TUNING_MAX_BUFFERED_ROWS", 1200000, 1000, 5000000, "int"),
    "EVENT_LOG_BUFFER_FLUSH_INTERVAL_S": Bound("EVENT_LOG_BUFFER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0, "float"),
    "EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO": Bound("EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO", 0.25, 0.0, 1.0, "float"),
    "EVENT_LOG_BUFFER_MAX_BATCH": Bound("EVENT_LOG_BUFFER_MAX_BATCH", 128, 1, 4096, "int"),
    "EVENT_LOG_BUFFER_MAX_ROWS": Bound("EVENT_LOG_BUFFER_MAX_ROWS", 2048, 1, 65536, "int"),
    "RUNTIME_METRICS_FLUSH_INTERVAL_S": Bound("RUNTIME_METRICS_FLUSH_INTERVAL_S", 3.0, 0.05, 30.0, "float"),
    "RUNTIME_METRICS_FLUSH_JITTER_RATIO": Bound("RUNTIME_METRICS_FLUSH_JITTER_RATIO", 0.5, 0.0, 1.0, "float"),
    "RUNTIME_METRICS_BUFFER_MAX_BATCH": Bound("RUNTIME_METRICS_BUFFER_MAX_BATCH", 256, 1, 4096, "int"),
    "RUNTIME_METRICS_BUFFER_MAX_ROWS": Bound("RUNTIME_METRICS_BUFFER_MAX_ROWS", 4096, 1, 65536, "int"),
    "RUNTIME_META_BEST_EFFORT_MIN_INTERVAL_S": Bound("RUNTIME_META_BEST_EFFORT_MIN_INTERVAL_S", 2.0, 0.0, 60.0, "float"),
    "RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S": Bound("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S", 2.0, 0.05, 30.0, "float"),
    "RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO": Bound("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO", 0.5, 0.0, 1.0, "float"),
    "RUNTIME_META_BEST_EFFORT_BUFFER_MAX_BATCH": Bound("RUNTIME_META_BEST_EFFORT_BUFFER_MAX_BATCH", 64, 1, 4096, "int"),
    "RUNTIME_META_BEST_EFFORT_BUFFER_MAX_KEYS": Bound("RUNTIME_META_BEST_EFFORT_BUFFER_MAX_KEYS", 512, 1, 65536, "int"),
}


def _env(env: Mapping[str, str] | None = None) -> Mapping[str, str]:
    return os.environ if env is None else env


def _profile(env: Mapping[str, str] | None = None) -> str:
    raw = str(_env(env).get("INGESTION_TUNING_PROFILE") or "").strip().lower()
    return _PROFILE_ALIASES.get(raw, raw or "safe")


def active_profile(env: Mapping[str, str] | None = None) -> str:
    profile = _profile(env)
    return profile if profile in {"safe", HOST_32T_123G_PROFILE} else "safe"


def _profile_default(name: str, default: int | float | str, env: Mapping[str, str] | None = None) -> int | float | str:
    if active_profile(env) == HOST_32T_123G_PROFILE and name in _HOST_32T_123G_DEFAULTS:
        return _HOST_32T_123G_DEFAULTS[name]
    return default


def _raw_env_value(name: str, default: int | float | str, env: Mapping[str, str] | None = None) -> str:
    source = _env(env)
    raw = str(source.get(name) or "").strip()
    if raw:
        return raw
    return str(_profile_default(name, default, source))


def env_bool(name: str, default: bool = False, *, env: Mapping[str, str] | None = None) -> bool:
    raw = str(_env(env).get(name) or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in TRUE_VALUES:
        return True
    if raw in FALSE_VALUES:
        return False
    return bool(default)


def tuned_int(name: str, default: int, minimum: int, maximum: int, *, env: Mapping[str, str] | None = None) -> int:
    bound = BOUNDS.get(name)
    effective_default = int(default)
    min_value = int(bound.minimum if bound else minimum)
    max_value = int(bound.maximum if bound else maximum)
    raw = _raw_env_value(name, effective_default, env)
    try:
        value = int(float(raw))
    except Exception:
        value = int(effective_default)
    return int(max(min_value, min(max_value, value)))


def tuned_float(name: str, default: float, minimum: float, maximum: float, *, env: Mapping[str, str] | None = None) -> float:
    bound = BOUNDS.get(name)
    effective_default = float(default)
    min_value = float(bound.minimum if bound else minimum)
    max_value = float(bound.maximum if bound else maximum)
    raw = _raw_env_value(name, effective_default, env)
    try:
        value = float(raw)
    except Exception:
        value = float(effective_default)
    return float(max(min_value, min(max_value, value)))


def raw_bound_violations(env: Mapping[str, str] | None = None) -> list[str]:
    source = _env(env)
    violations: list[str] = []
    for name, bound in sorted(BOUNDS.items()):
        raw = str(source.get(name) or "").strip()
        if not raw:
            continue
        try:
            value = float(raw)
        except Exception:
            violations.append(f"{name} is not numeric: {raw}")
            continue
        if value < float(bound.minimum):
            violations.append(f"{name} below hard bound {bound.minimum}: {raw}")
        elif value > float(bound.maximum):
            violations.append(f"{name} above hard bound {bound.maximum}: {raw}")
    return violations


def pg_pool_default_for_role(role: str, *, env: Mapping[str, str] | None = None) -> int:
    role_name = str(role or "application").strip().lower()
    if role_name in {"ingestion_child", "ingestion-child", "ingest_child", "ingest-child"}:
        role_name = "jobs"
    if role_name not in {"application", "ingestion", "jobs"}:
        role_name = "application"
    if active_profile(env) == HOST_32T_123G_PROFILE:
        return int(_HOST_32T_123G_PG_POOL_DEFAULTS.get(role_name, 4))
    if role_name == "ingestion":
        return 8
    if role_name == "jobs":
        return 2
    return 4


def _bool_from_env_or_dsn(name: str, *, dsn_keys: tuple[str, ...], env: Mapping[str, str]) -> bool:
    if str(env.get(name) or "").strip():
        return env_bool(name, default=False, env=env)
    return any(str(env.get(key) or "").strip() for key in dsn_keys)


def ingestion_tuning_snapshot(env: Mapping[str, str] | None = None, *, pg_pool_role: str = "ingestion") -> dict[str, Any]:
    source = _env(env)
    requested_profile = _profile(source)
    profile = active_profile(source)
    pg_pool_size = tuned_int(
        "TS_PG_POOL_SIZE",
        pg_pool_default_for_role(pg_pool_role, env=source),
        1,
        32,
        env=source,
    )
    pg_pool_min = min(
        pg_pool_size,
        tuned_int("TS_PG_POOL_MIN_SIZE", 2, 1, 16, env=source),
    )
    child_pg_pool_size = tuned_int(
        "INGESTION_CHILD_TS_PG_POOL_SIZE",
        pg_pool_default_for_role("jobs", env=source),
        1,
        8,
        env=source,
    )
    child_pg_pool_min = min(
        child_pg_pool_size,
        tuned_int("INGESTION_CHILD_TS_PG_POOL_MIN_SIZE", 1, 1, 8, env=source),
    )
    child_timescale_pool_max = tuned_int("INGESTION_CHILD_TIMESCALE_POOL_MAX_SIZE", 2, 1, 8, env=source)
    child_prices_pool_max = tuned_int("INGESTION_CHILD_TIMESCALE_PRICES_POOL_MAX_SIZE", 2, 1, 8, env=source)
    timescale_enabled = _bool_from_env_or_dsn(
        "TIMESCALE_ENABLED",
        dsn_keys=("TIMESCALE_DSN", "TIMESCALE_URL", "TIMESCALE_DATABASE_URL"),
        env=source,
    )
    prices_enabled = _bool_from_env_or_dsn(
        "TIMESCALE_PRICES_ENABLED",
        dsn_keys=("TIMESCALE_PRICES_DSN", "TIMESCALE_DSN", "TIMESCALE_URL", "TIMESCALE_DATABASE_URL"),
        env=source,
    )
    async_enabled = env_bool("ASYNC_PRICE_WRITER_ENABLED", default=prices_enabled, env=source)
    telemetry_enabled = env_bool("TELEMETRY_APPEND_BUFFER_ENABLED", default=True, env=source)

    timescale_pool_max = tuned_int("TIMESCALE_POOL_MAX_SIZE", 4, 1, 16, env=source)
    timescale_pool_min = min(timescale_pool_max, tuned_int("TIMESCALE_POOL_MIN_SIZE", 1, 1, 16, env=source))
    price_pool_max = tuned_int("TIMESCALE_PRICES_POOL_MAX_SIZE", 4, 1, 16, env=source)
    price_pool_min = min(price_pool_max, tuned_int("TIMESCALE_PRICES_POOL_MIN_SIZE", 1, 1, 16, env=source))
    timescale_queue = tuned_int("TIMESCALE_QUEUE_MAXSIZE", 1024, 1, 32768, env=source)
    timescale_batch = tuned_int("TIMESCALE_BATCH_SIZE", 500, 1, 5000, env=source)
    async_queue = tuned_int("ASYNC_PRICE_WRITER_QUEUE_MAXSIZE", 2048, 32, 32768, env=source)
    async_batch = tuned_int("ASYNC_PRICE_WRITER_BATCH_SIZE", 256, 1, 4096, env=source)
    async_spool_max_bytes = tuned_int("ASYNC_PRICE_WRITER_SPOOL_MAX_BYTES", 268435456, 1048576, 8589934592, env=source)
    async_spool_busy_timeout_ms = tuned_int("ASYNC_PRICE_WRITER_SPOOL_BUSY_TIMEOUT_MS", 50, 10, 60000, env=source)
    telemetry_batch = tuned_int("TELEMETRY_APPEND_BUFFER_MAX_BATCH", 128, 1, 4096, env=source)
    telemetry_rows = max(
        telemetry_batch,
        tuned_int("TELEMETRY_APPEND_BUFFER_MAX_ROWS", 4096, 1, 65536, env=source),
    )
    event_log_batch = tuned_int("EVENT_LOG_BUFFER_MAX_BATCH", 128, 1, 4096, env=source)
    event_log_rows = max(event_log_batch, tuned_int("EVENT_LOG_BUFFER_MAX_ROWS", 2048, 1, 65536, env=source))
    runtime_metrics_batch = tuned_int("RUNTIME_METRICS_BUFFER_MAX_BATCH", 256, 1, 4096, env=source)
    runtime_metrics_rows = max(
        runtime_metrics_batch,
        tuned_int("RUNTIME_METRICS_BUFFER_MAX_ROWS", 4096, 1, 65536, env=source),
    )
    runtime_meta_batch = tuned_int("RUNTIME_META_BEST_EFFORT_BUFFER_MAX_BATCH", 64, 1, 4096, env=source)
    runtime_meta_keys = max(
        runtime_meta_batch,
        tuned_int("RUNTIME_META_BEST_EFFORT_BUFFER_MAX_KEYS", 512, 1, 65536, env=source),
    )
    redis_pool = tuned_int("TS_REDIS_POOL_SIZE", 16, 1, 64, env=source)
    max_total_db = tuned_int("INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS", 24, 4, 64, env=source)
    max_buffered_rows = tuned_int("INGESTION_TUNING_MAX_BUFFERED_ROWS", 1200000, 1000, 5000000, env=source)

    total_db_connections = int(pg_pool_size)
    if timescale_enabled:
        total_db_connections += int(timescale_pool_max)
    if prices_enabled:
        total_db_connections += int(price_pool_max)

    queue_risk_rows = 0
    if timescale_enabled:
        queue_risk_rows += int(timescale_queue * timescale_batch)
    if async_enabled:
        queue_risk_rows += int(async_queue * async_batch)
    if telemetry_enabled:
        queue_risk_rows += int(telemetry_rows)
    if env_bool("EVENT_LOG_BUFFER_ENABLED", default=True, env=source):
        queue_risk_rows += int(event_log_rows)
    if env_bool("RUNTIME_METRICS_BUFFER_ENABLED", default=True, env=source):
        queue_risk_rows += int(runtime_metrics_rows)
    if env_bool("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", default=True, env=source):
        queue_risk_rows += int(runtime_meta_keys)

    warnings: list[str] = []
    errors: list[str] = raw_bound_violations(source)
    if requested_profile not in {"safe", HOST_32T_123G_PROFILE}:
        warnings.append(f"unknown INGESTION_TUNING_PROFILE={requested_profile}; using safe")
    timescale_dsn_configured = any(
        str(source.get(key) or "").strip()
        for key in ("TIMESCALE_DSN", "TIMESCALE_URL", "TIMESCALE_DATABASE_URL")
    )
    prices_dsn_configured = any(
        str(source.get(key) or "").strip()
        for key in ("TIMESCALE_PRICES_DSN", "TIMESCALE_DSN", "TIMESCALE_URL", "TIMESCALE_DATABASE_URL")
    )
    if timescale_enabled and not timescale_dsn_configured:
        errors.append("TIMESCALE_ENABLED is true but no TIMESCALE_DSN/TIMESCALE_URL/TIMESCALE_DATABASE_URL is configured")
    if prices_enabled and not prices_dsn_configured:
        errors.append("TIMESCALE_PRICES_ENABLED is true but no TIMESCALE_PRICES_DSN or TIMESCALE_DSN is configured")
    if total_db_connections > max_total_db:
        errors.append(
            "total ingestion DB pool budget exceeded: "
            f"{total_db_connections}>{max_total_db} "
            "(TS_PG_POOL_SIZE + enabled TIMESCALE_POOL_MAX_SIZE + enabled TIMESCALE_PRICES_POOL_MAX_SIZE)"
        )
    if queue_risk_rows > max_buffered_rows:
        errors.append(
            "ingestion buffered-row risk budget exceeded: "
            f"{queue_risk_rows}>{max_buffered_rows}"
        )
    if timescale_enabled and timescale_batch > max(1, timescale_queue * 4):
        warnings.append("TIMESCALE_BATCH_SIZE is large relative to TIMESCALE_QUEUE_MAXSIZE")
    if async_enabled and not prices_enabled:
        warnings.append("ASYNC_PRICE_WRITER_ENABLED is on while TIMESCALE_PRICES storage is disabled")
    if telemetry_enabled and telemetry_rows < telemetry_batch:
        warnings.append("TELEMETRY_APPEND_BUFFER_MAX_ROWS is below TELEMETRY_APPEND_BUFFER_MAX_BATCH after parsing")

    snapshot = {
        "ok": not bool(errors),
        "profile": profile,
        "requested_profile": requested_profile,
        "host_profile": HOST_32T_123G_PROFILE if profile == HOST_32T_123G_PROFILE else "",
        "warnings": warnings,
        "errors": errors,
        "bounds": {
            name: {
                "minimum": bound.minimum,
                "maximum": bound.maximum,
                "default": bound.default,
            }
            for name, bound in sorted(BOUNDS.items())
        },
        "runtime_postgres_pool": {
            "profile": str(pg_pool_role),
            "pool_min_size": int(pg_pool_min),
            "pool_max_size": int(pg_pool_size),
        },
        "ingestion_child_postgres_pool": {
            "profile": "jobs",
            "pool_min_size": int(child_pg_pool_min),
            "pool_max_size": int(child_pg_pool_size),
        },
        "ingestion_child_sidecar_pools": {
            "timescale_pool_min_size": 1,
            "timescale_pool_max_size": int(child_timescale_pool_max),
            "price_storage_pool_min_size": 1,
            "price_storage_pool_max_size": int(child_prices_pool_max),
        },
        "redis_pool": {
            "pool_max_size": int(redis_pool),
            "connect_timeout_s": tuned_float("TS_REDIS_CONNECT_TIMEOUT_S", 0.25, 0.05, 5.0, env=source),
            "socket_timeout_s": tuned_float("TS_REDIS_SOCKET_TIMEOUT_S", 0.25, 0.05, 5.0, env=source),
        },
        "timescale": {
            "enabled": bool(timescale_enabled),
            "pool_min_size": int(timescale_pool_min),
            "pool_max_size": int(timescale_pool_max),
            "batch_size": int(timescale_batch),
            "flush_interval_s": tuned_float("TIMESCALE_FLUSH_INTERVAL_S", 1.0, 0.05, 10.0, env=source),
            "queue_maxsize": int(timescale_queue),
            "retry_attempts": tuned_int("TIMESCALE_RETRY_ATTEMPTS", 5, 1, 10, env=source),
            "backpressure_timeout_s": tuned_float("TIMESCALE_BACKPRESSURE_TIMEOUT_S", 5.0, 0.05, 30.0, env=source),
        },
        "price_storage": {
            "enabled": bool(prices_enabled),
            "pool_min_size": int(price_pool_min),
            "pool_max_size": int(price_pool_max),
            "retry_attempts": tuned_int("TIMESCALE_PRICES_RETRY_ATTEMPTS", 3, 1, 10, env=source),
        },
        "async_price_writer": {
            "enabled": bool(async_enabled),
            "queue_maxsize": int(async_queue),
            "batch_size": int(async_batch),
            "flush_interval_s": tuned_float("ASYNC_PRICE_WRITER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0, env=source),
            "retry_attempts": tuned_int("ASYNC_PRICE_WRITER_RETRY_ATTEMPTS", 4, 1, 10, env=source),
            "enqueue_timeout_s": tuned_float("ASYNC_PRICE_WRITER_ENQUEUE_TIMEOUT_S", 0.05, 0.0, 5.0, env=source),
            "spool_max_envelopes": int(async_queue),
            "spool_max_bytes": int(async_spool_max_bytes),
            "spool_busy_timeout_ms": int(async_spool_busy_timeout_ms),
        },
        "telemetry_append_buffer": {
            "enabled": bool(telemetry_enabled),
            "max_batch": int(telemetry_batch),
            "max_rows": int(telemetry_rows),
            "flush_interval_s": tuned_float("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0, env=source),
            "flush_jitter_ratio": tuned_float("TELEMETRY_APPEND_BUFFER_FLUSH_JITTER_RATIO", 0.25, 0.0, 1.0, env=source),
        },
        "event_log_buffer": {
            "enabled": env_bool("EVENT_LOG_BUFFER_ENABLED", default=True, env=source),
            "max_batch": int(event_log_batch),
            "max_rows": int(event_log_rows),
            "flush_interval_s": tuned_float("EVENT_LOG_BUFFER_FLUSH_INTERVAL_S", 0.5, 0.05, 5.0, env=source),
            "flush_jitter_ratio": tuned_float("EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO", 0.25, 0.0, 1.0, env=source),
        },
        "runtime_metrics_buffer": {
            "enabled": env_bool("RUNTIME_METRICS_BUFFER_ENABLED", default=True, env=source),
            "max_batch": int(runtime_metrics_batch),
            "max_rows": int(runtime_metrics_rows),
            "flush_interval_s": tuned_float("RUNTIME_METRICS_FLUSH_INTERVAL_S", 3.0, 0.05, 30.0, env=source),
            "flush_jitter_ratio": tuned_float("RUNTIME_METRICS_FLUSH_JITTER_RATIO", 0.5, 0.0, 1.0, env=source),
        },
        "runtime_meta_buffer": {
            "enabled": env_bool("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", default=True, env=source),
            "min_same_value_interval_s": tuned_float("RUNTIME_META_BEST_EFFORT_MIN_INTERVAL_S", 2.0, 0.0, 60.0, env=source),
            "max_batch": int(runtime_meta_batch),
            "max_keys": int(runtime_meta_keys),
            "flush_interval_s": tuned_float("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S", 2.0, 0.05, 30.0, env=source),
            "flush_jitter_ratio": tuned_float("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO", 0.5, 0.0, 1.0, env=source),
        },
        "capacity": {
            "total_db_pool_connections": int(total_db_connections),
            "max_total_db_connections": int(max_total_db),
            "per_child_db_pool_connections": int(
                child_pg_pool_size
                + (child_timescale_pool_max if timescale_enabled else 0)
                + (child_prices_pool_max if prices_enabled else 0)
            ),
            "buffered_row_risk_estimate": int(queue_risk_rows),
            "max_buffered_rows": int(max_buffered_rows),
        },
    }
    return snapshot


def assert_ingestion_tuning_safe(env: Mapping[str, str] | None = None, *, pg_pool_role: str = "ingestion") -> dict[str, Any]:
    snapshot = ingestion_tuning_snapshot(env, pg_pool_role=pg_pool_role)
    errors = [str(item) for item in list(snapshot.get("errors") or []) if str(item).strip()]
    if errors:
        raise RuntimeError("unsafe_ingestion_tuning:" + "; ".join(errors))
    return snapshot


__all__ = [
    "BOUNDS",
    "HOST_32T_123G_PROFILE",
    "active_profile",
    "assert_ingestion_tuning_safe",
    "env_bool",
    "ingestion_tuning_snapshot",
    "pg_pool_default_for_role",
    "raw_bound_violations",
    "tuned_float",
    "tuned_int",
]
