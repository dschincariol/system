"""Read-only ingestion soak and Timescale policy evidence."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


def _now_ms() -> int:
    return int(time.time() * 1000)


def _bool_env(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = env.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _float_env(env: Mapping[str, str], name: str, default: float) -> float:
    try:
        return float(str(env.get(str(name), default)).strip())
    except Exception:
        return float(default)


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    try:
        return int(float(str(env.get(str(name), default)).strip()))
    except Exception:
        return int(default)


def _strict_runtime(env: Mapping[str, str]) -> bool:
    mode = str(env.get("ENGINE_MODE") or env.get("EXECUTION_MODE") or "").strip().lower()
    env_name = str(env.get("ENV") or env.get("NODE_ENV") or "").strip().lower()
    supervised = _bool_env(env, "ENGINE_SUPERVISED", False)
    return bool(supervised or env_name in {"prod", "production"} or mode in {"paper", "shadow", "live"})


def ingestion_soak_required(env: Mapping[str, str] | None = None) -> bool:
    source = env or os.environ
    explicit = source.get("INGESTION_SOAK_REQUIRE_EVIDENCE")
    if explicit is None:
        explicit = source.get("PREFLIGHT_REQUIRE_INGESTION_SOAK_EVIDENCE")
    if explicit is not None and str(explicit).strip() != "":
        return _bool_env(
            source,
            "INGESTION_SOAK_REQUIRE_EVIDENCE",
            _bool_env(source, "PREFLIGHT_REQUIRE_INGESTION_SOAK_EVIDENCE", False),
        )
    return _strict_runtime(source)


def _policy_query_enabled(env: Mapping[str, str], *, required: bool) -> bool:
    explicit = env.get("INGESTION_SOAK_QUERY_TIMESCALE") or env.get("PREFLIGHT_QUERY_INGESTION_SOAK_TIMESCALE")
    if explicit is not None and str(explicit).strip() != "":
        return _bool_env(
            env,
            "INGESTION_SOAK_QUERY_TIMESCALE",
            _bool_env(env, "PREFLIGHT_QUERY_INGESTION_SOAK_TIMESCALE", False),
        )
    return bool(required)


def _thresholds(env: Mapping[str, str]) -> dict[str, Any]:
    max_spool_age_s = _float_env(env, "INGESTION_SOAK_MAX_SPOOL_AGE_S", 300.0)
    return {
        "max_queue_fill_ratio": _float_env(env, "INGESTION_SOAK_MAX_QUEUE_FILL_RATIO", 0.75),
        "max_spool_bytes_fill_ratio": _float_env(env, "INGESTION_SOAK_MAX_SPOOL_BYTES_FILL_RATIO", 0.80),
        "max_spool_rows_fill_ratio": _float_env(env, "INGESTION_SOAK_MAX_SPOOL_ROWS_FILL_RATIO", 0.80),
        "max_spool_age_ms": int(max(0.0, max_spool_age_s) * 1000.0),
        "max_copy_fallbacks": _int_env(env, "INGESTION_SOAK_MAX_COPY_FALLBACKS", 0),
        "max_write_failures": _int_env(env, "INGESTION_SOAK_MAX_WRITE_FAILURES", 0),
        "max_flush_failures": _int_env(env, "INGESTION_SOAK_MAX_FLUSH_FAILURES", 0),
    }


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, (list, tuple)) else []


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _copy_selected(source: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    return {key: source.get(key) for key in keys if key in source}


def _load_report_from_file(path: str | Path) -> dict[str, Any]:
    data = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    return dict(data) if isinstance(data, Mapping) else {}


def _health_snapshot_from_runtime() -> dict[str, Any]:
    from engine.runtime.health import get_health_snapshot

    return dict(get_health_snapshot() or {})


def _redis_evidence() -> dict[str, Any]:
    from engine.cache.circuit import cache_circuit
    from engine.cache.redis_pool import (
        redis_dependency_available,
        redis_pool_healthcheck_interval_s,
        redis_pool_size,
    )

    circuit = cache_circuit()
    return {
        "dependency_available": bool(redis_dependency_available()),
        "pool_size": int(redis_pool_size()),
        "healthcheck_interval_s": float(redis_pool_healthcheck_interval_s()),
        "cache_circuit_state": str(circuit.state),
        "cache_circuit_failures": int(circuit.failures),
    }


def _options_buffer_from_health(health: Mapping[str, Any]) -> dict[str, Any]:
    runtime = _as_dict(health.get("ingestion_runtime"))
    writer_diagnostics = _as_dict(runtime.get("writer_diagnostics"))
    options = _as_dict(writer_diagnostics.get("options_poll_durable_buffer"))
    if options:
        return options
    status = _as_dict(health.get("options_ingestion"))
    meta = _as_dict(status.get("meta"))
    if not meta:
        return {}
    return {
        "ok": bool(status.get("ok", True)),
        "pending_rows": _int(meta.get("durable_buffer_pending_rows")),
        "pending_bytes": _int(meta.get("durable_buffer_pending_bytes")),
        "oldest_age_ms": _int(meta.get("durable_buffer_oldest_age_ms")),
        "rows_fill_ratio": _float(meta.get("durable_buffer_rows_fill_ratio")),
        "bytes_fill_ratio": _float(meta.get("durable_buffer_bytes_fill_ratio")),
        "backpressure_active": bool(meta.get("durable_buffer_backpressure_active")),
        "backpressure_events": _int(meta.get("durable_buffer_backpressure_events")),
        "rejected_rows": _int(meta.get("durable_buffer_rejected_rows")),
        "dropped_rows": _int(meta.get("durable_buffer_dropped_rows")),
        "enqueue_failures": _int(meta.get("durable_buffer_enqueue_failures")),
        "replay_failures": _int(meta.get("durable_buffer_replay_failures")),
        "delete_failures": _int(meta.get("durable_buffer_delete_failures")),
        "corrupt_payload_rows": _int(meta.get("durable_buffer_corrupt_payload_rows")),
        "last_error": str(meta.get("durable_buffer_last_error") or ""),
    }


def _component_error(errors: list[str], code: str, component: str) -> None:
    errors.append(f"{component}:{code}")


def _evaluate_async_price_writer(snapshot: Mapping[str, Any], thresholds: Mapping[str, Any], errors: list[str]) -> None:
    if not snapshot:
        return
    component = "async_price_writer"
    if bool(snapshot.get("enabled")) and not bool(snapshot.get("ok", True)):
        _component_error(errors, "not_ok", component)
    if bool(snapshot.get("backpressure_active")):
        _component_error(errors, "backpressure_active", component)
    if _float(snapshot.get("queue_fill_ratio")) >= _float(thresholds.get("max_queue_fill_ratio"), 0.75):
        _component_error(errors, "queue_pressure", component)
    if _float(snapshot.get("spool_bytes_fill_ratio")) >= _float(thresholds.get("max_spool_bytes_fill_ratio"), 0.80):
        _component_error(errors, "spool_byte_pressure", component)
    if _int(snapshot.get("spool_oldest_age_ms")) > _int(thresholds.get("max_spool_age_ms"), 300_000):
        _component_error(errors, "spool_oldest_age_exceeded", component)
    for key in (
        "dead_letters",
        "dropped_rows",
        "rejected_rows",
        "residual_loss_rows",
        "spool_corrupt_rows",
        "spool_corrupt_payload_rows",
        "spool_corruption_events",
    ):
        if _int(snapshot.get(key)) > 0:
            _component_error(errors, key, component)


def _evaluate_telemetry_buffer(snapshot: Mapping[str, Any], thresholds: Mapping[str, Any], errors: list[str]) -> None:
    if not snapshot:
        return
    component = "telemetry_append_buffer"
    if bool(snapshot.get("enabled")) and not bool(snapshot.get("ok", True)):
        _component_error(errors, "not_ok", component)
    if bool(snapshot.get("backpressure_active")):
        _component_error(errors, "backpressure_active", component)
    if _float(snapshot.get("queue_fill_ratio")) >= _float(thresholds.get("max_queue_fill_ratio"), 0.75):
        _component_error(errors, "queue_pressure", component)
    if _float(snapshot.get("spool_bytes_fill_ratio")) >= _float(thresholds.get("max_spool_bytes_fill_ratio"), 0.80):
        _component_error(errors, "spool_byte_pressure", component)
    if _int(snapshot.get("oldest_age_ms") or snapshot.get("spool_oldest_age_ms")) > _int(thresholds.get("max_spool_age_ms"), 300_000):
        _component_error(errors, "spool_oldest_age_exceeded", component)
    if _int(snapshot.get("flush_failures")) > _int(thresholds.get("max_flush_failures")):
        _component_error(errors, "flush_failures", component)
    for key in (
        "dropped_rows",
        "residual_loss_rows",
        "spool_corrupt_rows",
        "spool_corrupt_payload_rows",
        "spool_corruption_events",
        "spool_unavailable_count",
    ):
        if _int(snapshot.get(key)) > 0:
            _component_error(errors, key, component)


def _evaluate_options_buffer(snapshot: Mapping[str, Any], thresholds: Mapping[str, Any], errors: list[str]) -> None:
    if not snapshot:
        return
    component = "options_durable_buffer"
    if not bool(snapshot.get("ok", True)):
        _component_error(errors, "not_ok", component)
    if bool(snapshot.get("backpressure_active")):
        _component_error(errors, "backpressure_active", component)
    if max(_float(snapshot.get("rows_fill_ratio")), _float(snapshot.get("bytes_fill_ratio"))) >= max(
        _float(thresholds.get("max_spool_rows_fill_ratio"), 0.80),
        _float(thresholds.get("max_spool_bytes_fill_ratio"), 0.80),
    ):
        _component_error(errors, "spool_pressure", component)
    if _int(snapshot.get("oldest_age_ms")) > _int(thresholds.get("max_spool_age_ms"), 300_000):
        _component_error(errors, "spool_oldest_age_exceeded", component)
    for key in (
        "rejected_rows",
        "dropped_rows",
        "enqueue_failures",
        "replay_failures",
        "delete_failures",
        "corrupt_payload_rows",
    ):
        if _int(snapshot.get(key)) > 0:
            _component_error(errors, key, component)


def _evaluate_timescale_writer(snapshot: Mapping[str, Any], thresholds: Mapping[str, Any], errors: list[str]) -> None:
    if not snapshot:
        return
    component = "timescale_client"
    metrics = _as_dict(snapshot.get("metrics"))
    if bool(snapshot.get("enabled")) and not bool(snapshot.get("ok", True)):
        _component_error(errors, "not_ok", component)
    if bool(snapshot.get("degraded")):
        for reason in _as_list(snapshot.get("degraded_reasons")):
            _component_error(errors, f"degraded:{reason}", component)
    if _int(snapshot.get("queue_depth")) >= _int(snapshot.get("queue_maxsize"), 1):
        _component_error(errors, "queue_full", component)
    if _int(metrics.get("backpressure_count")) > 0 or bool(metrics.get("backpressure_active")):
        _component_error(errors, "backpressure", component)
    if _int(metrics.get("copy_fallback_count")) > _int(thresholds.get("max_copy_fallbacks")):
        _component_error(errors, "copy_fallbacks", component)
    if _int(metrics.get("flush_failure_count")) > _int(thresholds.get("max_flush_failures")):
        _component_error(errors, "flush_failures", component)


def _evaluate_price_storage(snapshot: Mapping[str, Any], thresholds: Mapping[str, Any], errors: list[str]) -> None:
    if not snapshot:
        return
    component = "pg_price_storage"
    if bool(snapshot.get("enabled")) and not bool(snapshot.get("ok", True)):
        _component_error(errors, "not_ok", component)
    if bool(snapshot.get("write_circuit_open")) or bool(snapshot.get("backpressure_active")):
        _component_error(errors, "write_circuit_open", component)
    if _int(snapshot.get("copy_fallbacks")) > _int(thresholds.get("max_copy_fallbacks")):
        _component_error(errors, "copy_fallbacks", component)
    if _int(snapshot.get("write_failures")) > _int(thresholds.get("max_write_failures")):
        _component_error(errors, "write_failures", component)
    if bool(snapshot.get("copy_enabled")) and str(snapshot.get("last_write_path") or "").startswith("values_upsert_copy_unavailable"):
        _component_error(errors, "copy_path_failed", component)


def _evaluate_redis(snapshot: Mapping[str, Any], errors: list[str]) -> None:
    if not snapshot:
        return
    if str(snapshot.get("cache_circuit_state") or "").strip().lower() == "open":
        _component_error(errors, "cache_circuit_open", "redis")


def _row_get(row: Any, index: int, name: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(name)
    try:
        return row[name]
    except Exception:
        try:
            return row[index]
        except Exception:
            return None


def _fetch_rows(conn: Any, sql: str, params: Sequence[Any]) -> list[Any]:
    result = conn.execute(sql, tuple(params))
    fetchall = getattr(result, "fetchall", None)
    if callable(fetchall):
        return list(fetchall() or [])
    return list(result or [])


def _expected_timescale_tables() -> tuple[str, ...]:
    from engine.runtime.price_timescale_schema import PRICE_TIMESCALE_TABLES
    from engine.runtime.timescale_client import _TIMESCALE_HYPERTABLE_TABLES

    return tuple(sorted({*tuple(_TIMESCALE_HYPERTABLE_TABLES), *tuple(PRICE_TIMESCALE_TABLES)}))


def _expected_indexes() -> tuple[str, ...]:
    from engine.runtime.price_timescale_schema import PRICE_TIMESCALE_SCHEMA_INDEXES
    from engine.runtime.timescale_client import _TIMESCALE_REQUIRED_INDEXES

    scoring = (
        "idx_tracked_predictions_prediction_id_ts_id",
        "idx_model_performance_prediction_id",
        "ux_model_performance_tracked_prediction_id",
    )
    return tuple(sorted({*tuple(_TIMESCALE_REQUIRED_INDEXES), *tuple(PRICE_TIMESCALE_SCHEMA_INDEXES), *scoring}))


def _desired_chunk_interval_ms(table_name: str) -> int:
    from engine.runtime.schema.table_classification import hypertable_chunk_interval_ms

    return int(hypertable_chunk_interval_ms(str(table_name), default="1 week"))


def _interval_text_to_ms(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        from engine.runtime.schema.table_classification import interval_to_ms

        return int(interval_to_ms(text))
    except Exception:
        return None


def query_timescale_policy_evidence(conn: Any, *, schema_name: str = "public") -> dict[str, Any]:
    expected_tables = _expected_timescale_tables()
    expected_indexes = _expected_indexes()
    schema = str(schema_name or "public").strip() or "public"

    hypertable_rows = _fetch_rows(
        conn,
        """
        SELECT hypertable_schema, hypertable_name, num_dimensions, compression_enabled
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = %s
        """,
        (schema,),
    )
    dimension_rows = _fetch_rows(
        conn,
        """
        SELECT hypertable_name, column_name, time_interval::text, integer_interval
        FROM timescaledb_information.dimensions
        WHERE hypertable_schema = %s
        """,
        (schema,),
    )
    job_rows = _fetch_rows(
        conn,
        """
        SELECT hypertable_name, proc_name, schedule_interval::text
        FROM timescaledb_information.jobs
        WHERE hypertable_schema = %s
          AND proc_name IN ('policy_compression', 'policy_retention')
        """,
        (schema,),
    )
    index_rows = _fetch_rows(
        conn,
        """
        SELECT tablename, indexname
        FROM pg_indexes
        WHERE schemaname = %s
        """,
        (schema,),
    )

    hypertables: dict[str, dict[str, Any]] = {}
    for row in hypertable_rows:
        name = str(_row_get(row, 1, "hypertable_name") or "").strip()
        if not name:
            continue
        hypertables[name] = {
            "schema": str(_row_get(row, 0, "hypertable_schema") or ""),
            "num_dimensions": _int(_row_get(row, 2, "num_dimensions")),
            "compression_enabled": bool(_row_get(row, 3, "compression_enabled")),
        }

    dimensions: dict[str, dict[str, Any]] = {}
    for row in dimension_rows:
        name = str(_row_get(row, 0, "hypertable_name") or "").strip()
        if not name:
            continue
        interval_text = str(_row_get(row, 2, "time_interval") or "").strip()
        integer_interval = _row_get(row, 3, "integer_interval")
        actual_ms = _int(integer_interval) if integer_interval not in (None, "") else _interval_text_to_ms(interval_text)
        dimensions[name] = {
            "column_name": str(_row_get(row, 1, "column_name") or ""),
            "time_interval": interval_text,
            "integer_interval": integer_interval,
            "actual_interval_ms": actual_ms,
            "desired_interval_ms": _desired_chunk_interval_ms(name),
        }

    jobs: dict[str, dict[str, Any]] = {}
    for row in job_rows:
        name = str(_row_get(row, 0, "hypertable_name") or "").strip()
        proc_name = str(_row_get(row, 1, "proc_name") or "").strip()
        if not name or not proc_name:
            continue
        jobs.setdefault(name, {})[proc_name] = {
            "schedule_interval": str(_row_get(row, 2, "schedule_interval") or ""),
        }

    present_indexes = sorted({str(_row_get(row, 1, "indexname") or "").strip() for row in index_rows if str(_row_get(row, 1, "indexname") or "").strip()})
    missing_hypertables = [name for name in expected_tables if name not in hypertables]
    missing_indexes = [name for name in expected_indexes if name not in present_indexes]
    missing_dimensions = [name for name in expected_tables if name in hypertables and name not in dimensions]
    chunk_interval_mismatches = []
    for name, row in dimensions.items():
        actual = row.get("actual_interval_ms")
        desired = row.get("desired_interval_ms")
        if actual not in (None, 0) and desired not in (None, 0) and int(actual) != int(desired):
            chunk_interval_mismatches.append({"table": name, "actual_interval_ms": int(actual), "desired_interval_ms": int(desired)})

    required_compression_tables = [
        name for name, row in hypertables.items()
        if bool(row.get("compression_enabled"))
    ]
    missing_compression_jobs = [
        name for name in required_compression_tables
        if "policy_compression" not in _as_dict(jobs.get(name))
    ]

    errors = []
    if missing_hypertables:
        errors.append("missing_hypertables:" + ",".join(missing_hypertables))
    if missing_dimensions:
        errors.append("missing_hypertable_dimensions:" + ",".join(missing_dimensions))
    if chunk_interval_mismatches:
        errors.append("chunk_interval_mismatch:" + ",".join(str(item["table"]) for item in chunk_interval_mismatches))
    if missing_indexes:
        errors.append("missing_indexes:" + ",".join(missing_indexes))
    if missing_compression_jobs:
        errors.append("missing_compression_jobs:" + ",".join(missing_compression_jobs))

    return {
        "ok": not bool(errors),
        "evidence_available": True,
        "schema": schema,
        "expected_hypertables": list(expected_tables),
        "hypertables": hypertables,
        "dimensions": dimensions,
        "jobs": jobs,
        "required_indexes": list(expected_indexes),
        "present_indexes": present_indexes,
        "missing_hypertables": missing_hypertables,
        "missing_dimensions": missing_dimensions,
        "chunk_interval_mismatches": chunk_interval_mismatches,
        "missing_indexes": missing_indexes,
        "missing_compression_jobs": missing_compression_jobs,
        "errors": errors,
    }


def collect_timescale_policy_evidence(
    *,
    env: Mapping[str, str] | None = None,
    conn: Any | None = None,
    schema_name: str | None = None,
) -> dict[str, Any]:
    source = env or os.environ
    schema = str(schema_name or source.get("TIMESCALE_SCHEMA") or "public").strip() or "public"
    if conn is not None:
        return query_timescale_policy_evidence(conn, schema_name=schema)
    try:
        import psycopg
        from engine.runtime.platform import connection_info_with_pg_password
        from engine.runtime.timescale_client import TimescaleConfig

        config = TimescaleConfig.from_env()
        dsn = str(config.dsn or source.get("TIMESCALE_DSN") or "").strip()
        if not dsn:
            return {
                "ok": False,
                "evidence_available": False,
                "schema": schema,
                "reason": "timescale_dsn_not_configured",
                "errors": ["timescale_policy_evidence_unavailable"],
            }
        conninfo = connection_info_with_pg_password(dsn)
        timeout_s = max(1, int(_float_env(source, "INGESTION_SOAK_TIMESCALE_CONNECT_TIMEOUT_S", 3.0)))
        with psycopg.connect(conninfo, autocommit=True, connect_timeout=timeout_s) as live_conn:
            return query_timescale_policy_evidence(live_conn, schema_name=str(config.schema_name or schema))
    except Exception as exc:
        return {
            "ok": False,
            "evidence_available": False,
            "schema": schema,
            "reason": f"{type(exc).__name__}:{exc}",
            "errors": ["timescale_policy_evidence_unavailable"],
        }


def _policy_evidence_from_file(env: Mapping[str, str]) -> dict[str, Any]:
    path = str(env.get("INGESTION_SOAK_TIMESCALE_POLICY_JSON") or env.get("PREFLIGHT_INGESTION_SOAK_POLICY_JSON") or "").strip()
    if not path:
        return {}
    report = _load_report_from_file(path)
    policy = _as_dict(report.get("timescale_policy") or report.get("policy") or report)
    policy["evidence_source"] = str(Path(path).expanduser())
    return policy


def collect_ingestion_soak_report(
    health: Mapping[str, Any] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    query_policy: bool | None = None,
    policy_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = env or os.environ
    thresholds = _thresholds(source)
    required = ingestion_soak_required(source)
    if health is None:
        health = _health_snapshot_from_runtime()
    health = _as_dict(health)

    async_price = _as_dict(health.get("async_price_persistence"))
    telemetry = _as_dict(health.get("telemetry_append_buffer"))
    timescale = _as_dict(health.get("timescale"))
    price_storage = _as_dict(health.get("pg_price_storage"))
    options_buffer = _options_buffer_from_health(health)
    try:
        redis = _redis_evidence()
    except Exception as exc:
        redis = {
            "dependency_available": False,
            "cache_circuit_state": "unknown",
            "error": f"{type(exc).__name__}:{exc}",
        }

    policy = _as_dict(policy_evidence)
    if not policy:
        policy = _policy_evidence_from_file(source)
    should_query_policy = _policy_query_enabled(source, required=required) if query_policy is None else bool(query_policy)
    if not policy and should_query_policy:
        policy = collect_timescale_policy_evidence(env=source)
    if not policy:
        policy = {
            "ok": not required,
            "evidence_available": False,
            "reason": "timescale_policy_not_queried",
            "errors": ([] if not required else ["timescale_policy_evidence_missing"]),
        }

    errors: list[str] = []
    warnings: list[str] = []
    _evaluate_async_price_writer(async_price, thresholds, errors)
    _evaluate_telemetry_buffer(telemetry, thresholds, errors)
    _evaluate_options_buffer(options_buffer, thresholds, errors)
    _evaluate_timescale_writer(timescale, thresholds, errors)
    _evaluate_price_storage(price_storage, thresholds, errors)
    _evaluate_redis(redis, errors)
    if required and not any((async_price, telemetry, options_buffer, timescale, price_storage)):
        errors.append("ingestion_soak:writer_evidence_missing")

    policy_errors = [str(item) for item in _as_list(policy.get("errors")) if str(item).strip()]
    if required and not bool(policy.get("evidence_available")):
        errors.append("timescale_policy:evidence_missing")
    if bool(policy.get("evidence_available")) and not bool(policy.get("ok", True)):
        errors.extend(f"timescale_policy:{item}" for item in policy_errors)
    elif not bool(policy.get("evidence_available")):
        warnings.append("timescale_policy_evidence_not_queried")

    ok = not bool(errors)
    writer_evidence = {
        "async_price_writer": _copy_selected(
            async_price,
            (
                "ok",
                "enabled",
                "worker_count",
                "worker_alive_count",
                "queue_depth",
                "queue_rows",
                "queue_maxsize",
                "queue_fill_ratio",
                "spool_pending_batches",
                "spool_pending_rows",
                "spool_pending_bytes",
                "spool_oldest_age_ms",
                "spool_deleted_rows",
                "replayed_rows",
                "rejected_rows",
                "dropped_rows",
                "residual_loss_rows",
                "spool_corruption_events",
                "dead_letters",
                "backpressure_active",
                "backpressure_events",
                "retry_count",
                "last_flush_latency_ms",
                "last_db_write_duration_ms",
                "last_error",
                "shards",
            ),
        ),
        "telemetry_append_buffer": _copy_selected(
            telemetry,
            (
                "ok",
                "enabled",
                "write_path",
                "queue_depth",
                "buffered_rows",
                "queue_fill_ratio",
                "oldest_age_ms",
                "spool_pending_rows",
                "spool_pending_bytes",
                "spool_file_bytes",
                "spool_oldest_age_ms",
                "deleted_rows",
                "replayed_rows",
                "dropped_rows",
                "residual_loss_rows",
                "spool_corruption_events",
                "backpressure_active",
                "backpressure_events",
                "retry_count",
                "last_flush_latency_ms",
                "last_db_write_duration_ms",
                "flush_failures",
                "last_error",
                "pending_by_table",
            ),
        ),
        "options_durable_buffer": dict(options_buffer),
        "timescale_client": _copy_selected(
            timescale,
            (
                "ok",
                "enabled",
                "queue_depth",
                "queue_maxsize",
                "batch_size",
                "pool_min_size",
                "pool_max_size",
                "copy_staging_enabled",
                "copy_staging_fallback_enabled",
                "schema_ready",
                "schema_ok",
                "policy_status",
                "degraded",
                "degraded_reasons",
                "metrics",
            ),
        ),
        "pg_price_storage": _copy_selected(
            price_storage,
            (
                "ok",
                "enabled",
                "pool_ready",
                "pool_min_size",
                "pool_max_size",
                "copy_enabled",
                "copy_fallback_enabled",
                "copy_fallbacks",
                "write_circuit_open",
                "write_circuit_rejected_batches",
                "last_write_path",
                "last_write_duration_ms",
                "write_failures",
                "retryable_failures",
                "fatal_failures",
                "dropped_rows",
                "retry_count",
                "policy_status",
            ),
        ),
        "redis": redis,
    }

    return {
        "ok": bool(ok),
        "required": bool(required),
        "status": "ok" if ok else "degraded",
        "ts_ms": _now_ms(),
        "thresholds": thresholds,
        "writer_evidence": writer_evidence,
        "timescale_policy": policy,
        "errors": errors,
        "warnings": warnings,
        "operator_commands": [
            "python - <<'PY'\nfrom engine.runtime.health import get_health_snapshot\nimport json\nprint(json.dumps(get_health_snapshot().get('ingestion_soak', {}), indent=2, sort_keys=True))\nPY",
            "psql \"$TIMESCALE_DSN\" -c \"SELECT hypertable_schema, hypertable_name, num_dimensions, compression_enabled FROM timescaledb_information.hypertables ORDER BY hypertable_name;\"",
            "psql \"$TIMESCALE_DSN\" -c \"SELECT hypertable_name, column_name, time_interval, integer_interval FROM timescaledb_information.dimensions ORDER BY hypertable_name;\"",
            "psql \"$TIMESCALE_DSN\" -c \"SELECT tablename, indexname FROM pg_indexes WHERE schemaname = current_schema() ORDER BY tablename, indexname;\"",
        ],
        "summary": {
            "async_price_pending_rows": _int(async_price.get("spool_pending_rows") or async_price.get("queue_rows")),
            "telemetry_pending_rows": _int(telemetry.get("spool_pending_rows") or telemetry.get("buffered_rows")),
            "options_pending_rows": _int(options_buffer.get("pending_rows")),
            "timescale_queue_depth": _int(timescale.get("queue_depth")),
            "price_copy_fallbacks": _int(price_storage.get("copy_fallbacks")),
            "policy_evidence_available": bool(policy.get("evidence_available")),
        },
    }


def collect_ingestion_soak_report_from_file(path: str | Path) -> dict[str, Any]:
    return _load_report_from_file(path)


__all__ = [
    "collect_ingestion_soak_report",
    "collect_ingestion_soak_report_from_file",
    "collect_timescale_policy_evidence",
    "ingestion_soak_required",
    "query_timescale_policy_evidence",
]
