from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_safe_defaults_are_bounded_and_preflight_clean() -> None:
    from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot

    snapshot = ingestion_tuning_snapshot({}, pg_pool_role="ingestion")

    assert snapshot["ok"] is True
    assert snapshot["runtime_postgres_pool"]["pool_max_size"] == 8
    assert snapshot["ingestion_child_postgres_pool"]["pool_max_size"] == 2
    assert snapshot["redis_pool"]["pool_max_size"] == 16
    assert snapshot["timescale"]["batch_size"] == 2000
    assert snapshot["timescale"]["queue_maxsize"] == 256
    assert snapshot["async_price_writer"]["spool_synchronous"] == "NORMAL"
    assert snapshot["telemetry_append_buffer"]["max_rows"] == 4096
    assert snapshot["telemetry_append_buffer"]["spool_max_bytes"] == 67108864
    assert snapshot["telemetry_append_buffer"]["spool_synchronous"] == "NORMAL"
    assert snapshot["event_log_buffer"]["max_rows"] == 2048
    assert snapshot["runtime_metrics_buffer"]["max_rows"] == 4096
    assert snapshot["runtime_meta_buffer"]["max_keys"] == 512
    assert snapshot["capacity"]["total_db_pool_connections"] <= snapshot["capacity"]["max_total_db_connections"]
    assert snapshot["errors"] == []


def test_async_spool_synchronous_mode_is_explicitly_validated() -> None:
    from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot

    strict = ingestion_tuning_snapshot(
        {"ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS": "FULL"},
        pg_pool_role="ingestion",
    )
    invalid = ingestion_tuning_snapshot(
        {"ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS": "FAST"},
        pg_pool_role="ingestion",
    )

    assert strict["ok"] is True
    assert strict["async_price_writer"]["spool_synchronous"] == "FULL"
    rendered = "\n".join(str(item) for item in list(invalid.get("errors") or []))
    assert "ASYNC_PRICE_WRITER_SPOOL_SYNCHRONOUS must be one of" in rendered


def test_telemetry_spool_synchronous_mode_is_explicitly_validated() -> None:
    from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot

    strict = ingestion_tuning_snapshot(
        {"TELEMETRY_APPEND_BUFFER_SPOOL_SYNCHRONOUS": "FULL"},
        pg_pool_role="ingestion",
    )
    invalid = ingestion_tuning_snapshot(
        {"TELEMETRY_APPEND_BUFFER_SPOOL_SYNCHRONOUS": "FAST"},
        pg_pool_role="ingestion",
    )

    assert strict["ok"] is True
    assert strict["telemetry_append_buffer"]["spool_synchronous"] == "FULL"
    rendered = "\n".join(str(item) for item in list(invalid.get("errors") or []))
    assert "TELEMETRY_APPEND_BUFFER_SPOOL_SYNCHRONOUS must be one of" in rendered


def test_32_thread_123g_profile_increases_throughput_without_expanding_row_window() -> None:
    from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot

    safe_env = {
        "TIMESCALE_ENABLED": "1",
        "TIMESCALE_DSN": "postgres://example",
        "TIMESCALE_PRICES_ENABLED": "1",
        "ASYNC_PRICE_WRITER_ENABLED": "1",
    }
    host_env = {
        **safe_env,
        "INGESTION_TUNING_PROFILE": "32t_123g",
    }

    safe = ingestion_tuning_snapshot(safe_env, pg_pool_role="ingestion")
    host = ingestion_tuning_snapshot(host_env, pg_pool_role="ingestion")

    assert host["ok"] is True
    assert host["runtime_postgres_pool"]["pool_max_size"] == 12
    assert host["ingestion_child_postgres_pool"]["pool_max_size"] == 3
    assert host["ingestion_child_sidecar_pools"]["timescale_pool_max_size"] == 4
    assert host["ingestion_child_sidecar_pools"]["price_storage_pool_max_size"] == 4
    assert host["ingestion_child_sidecar_pools"]["async_price_writer_workers"] == 4
    assert host["timescale"]["pool_max_size"] == 8
    assert host["timescale"]["batch_size"] == 4000
    assert host["timescale"]["queue_maxsize"] == 128
    assert host["timescale"]["batch_size"] > safe["timescale"]["batch_size"]
    assert host["timescale"]["queue_maxsize"] < safe["timescale"]["queue_maxsize"]
    assert host["async_price_writer"]["workers"] == 8
    assert host["async_price_writer"]["batch_size"] > safe["async_price_writer"]["batch_size"]
    assert host["async_price_writer"]["queue_maxsize"] < safe["async_price_writer"]["queue_maxsize"]
    assert host["capacity"]["buffered_row_risk_estimate"] <= safe["capacity"]["buffered_row_risk_estimate"]


def test_raw_env_bounds_are_reported_and_effective_values_are_clamped() -> None:
    from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot, tuned_int

    env = {
        "ASYNC_PRICE_WRITER_QUEUE_MAXSIZE": "999999",
        "TIMESCALE_BATCH_SIZE": "0",
        "TS_REDIS_POOL_SIZE": "999",
        "EVENT_LOG_BUFFER_MAX_ROWS": "999999",
    }

    snapshot = ingestion_tuning_snapshot(env, pg_pool_role="ingestion")

    assert tuned_int("ASYNC_PRICE_WRITER_QUEUE_MAXSIZE", 2048, 32, 32768, env=env) == 32768
    assert tuned_int("TIMESCALE_BATCH_SIZE", 500, 1, 5000, env=env) == 1
    assert snapshot["redis_pool"]["pool_max_size"] == 64
    rendered = "\n".join(snapshot["errors"])
    assert "ASYNC_PRICE_WRITER_QUEUE_MAXSIZE above hard bound 32768" in rendered
    assert "TIMESCALE_BATCH_SIZE below hard bound 1" in rendered
    assert "TS_REDIS_POOL_SIZE above hard bound 64" in rendered
    assert "EVENT_LOG_BUFFER_MAX_ROWS above hard bound 65536" in rendered


def test_unknown_profile_warns_and_enabled_timescale_requires_dsn() -> None:
    from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot

    snapshot = ingestion_tuning_snapshot(
        {
            "INGESTION_TUNING_PROFILE": "auto_big_host",
            "TIMESCALE_ENABLED": "1",
        },
        pg_pool_role="ingestion",
    )

    assert snapshot["profile"] == "safe"
    assert any("unknown INGESTION_TUNING_PROFILE=auto_big_host" in item for item in snapshot["warnings"])
    assert any("TIMESCALE_ENABLED is true but no TIMESCALE_DSN" in item for item in snapshot["errors"])


def test_unsafe_pool_combination_fails_assertion() -> None:
    from engine.runtime.ingestion_tuning import assert_ingestion_tuning_safe

    env = {
        "TS_PG_POOL_SIZE": "16",
        "TIMESCALE_ENABLED": "1",
        "TIMESCALE_DSN": "postgres://example",
        "TIMESCALE_POOL_MAX_SIZE": "16",
        "TIMESCALE_PRICES_ENABLED": "1",
        "TIMESCALE_PRICES_POOL_MAX_SIZE": "16",
        "INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS": "24",
    }

    with pytest.raises(RuntimeError, match="total ingestion DB pool budget exceeded"):
        assert_ingestion_tuning_safe(env, pg_pool_role="ingestion")


def test_async_price_writer_workers_require_matching_price_pool() -> None:
    from engine.runtime.ingestion_tuning import assert_ingestion_tuning_safe, ingestion_tuning_snapshot

    env = {
        "TIMESCALE_PRICES_ENABLED": "1",
        "TIMESCALE_PRICES_DSN": "postgres://example",
        "ASYNC_PRICE_WRITER_ENABLED": "1",
        "ASYNC_PRICE_WRITER_WORKERS": "8",
        "TIMESCALE_PRICES_POOL_MAX_SIZE": "4",
    }

    snapshot = ingestion_tuning_snapshot(env, pg_pool_role="ingestion")
    rendered = "\n".join(str(item) for item in list(snapshot.get("errors") or []))
    assert "TIMESCALE_PRICES_POOL_MAX_SIZE must be >= ASYNC_PRICE_WRITER_WORKERS: 4<8" in rendered

    with pytest.raises(RuntimeError, match="TIMESCALE_PRICES_POOL_MAX_SIZE must be >= ASYNC_PRICE_WRITER_WORKERS"):
        assert_ingestion_tuning_safe(env, pg_pool_role="ingestion")


def test_price_storage_config_rejects_pool_smaller_than_async_workers(monkeypatch) -> None:
    monkeypatch.setenv("TIMESCALE_PRICES_ENABLED", "1")
    monkeypatch.setenv("TIMESCALE_PRICES_DSN", "postgres://user:pw@example/db")
    monkeypatch.setenv("ASYNC_PRICE_WRITER_ENABLED", "1")
    monkeypatch.setenv("ASYNC_PRICE_WRITER_WORKERS", "8")
    monkeypatch.setenv("TIMESCALE_PRICES_POOL_MAX_SIZE", "4")

    from engine.runtime.storage_pg_prices import PostgresPriceStorageConfig

    with pytest.raises(RuntimeError, match="timescale_prices_pool_too_small_for_async_writer"):
        PostgresPriceStorageConfig.from_env()


def test_prod_preflight_ingestion_tuning_gate_reports_errors(monkeypatch) -> None:
    env = {
        "TS_PG_POOL_SIZE": "16",
        "TIMESCALE_ENABLED": "1",
        "TIMESCALE_DSN": "postgres://example",
        "TIMESCALE_POOL_MAX_SIZE": "16",
        "TIMESCALE_PRICES_ENABLED": "1",
        "TIMESCALE_PRICES_POOL_MAX_SIZE": "16",
        "INGESTION_TUNING_MAX_TOTAL_DB_CONNECTIONS": "24",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    prod_preflight = importlib.reload(importlib.import_module("engine.runtime.prod_preflight"))
    notes, warnings, errors, snapshot = prod_preflight._ingestion_tuning_gate()

    assert notes == []
    assert warnings == []
    assert snapshot["ok"] is False
    assert any("total ingestion DB pool budget exceeded" in error for error in errors)


def test_storage_and_redis_pool_defaults_honor_host_profile(monkeypatch) -> None:
    monkeypatch.delenv("TS_PG_POOL_SIZE", raising=False)
    monkeypatch.setenv("INGESTION_TUNING_PROFILE", "host_32t_123g")
    monkeypatch.setenv("TS_PG_POOL_PROFILE", "ingest")

    storage_pool = importlib.reload(importlib.import_module("engine.runtime.storage_pool"))
    redis_pool = importlib.reload(importlib.import_module("engine.cache.redis_pool"))

    assert storage_pool.default_pool_size() == 12
    assert redis_pool.redis_pool_size() == 32


def test_timescale_flush_metric_fields_are_recorded_without_external_db() -> None:
    from engine.runtime.timescale_client import TimescaleClient, TimescaleConfig

    client = TimescaleClient(
        TimescaleConfig(
            enabled=True,
            dsn="postgres://unit-test",
            schema_name="public",
            pool_min_size=1,
            pool_max_size=1,
            batch_size=10,
            flush_interval_s=0.5,
            queue_maxsize=32,
            retry_attempts=1,
            retry_base_s=0.01,
            retry_max_s=0.01,
            backpressure_timeout_s=0.1,
            start_timeout_s=0.1,
            connect_timeout_s=0.1,
            lock_timeout_s=0.1,
            command_timeout_s=1.0,
            idle_in_txn_timeout_s=1.0,
            application_name="unit-test",
        )
    )

    client._note_flush_success(
        "price_data",
        3,
        write_path="copy_staging",
        deduped_rows=1,
        flush_latency_ms=12.4,
        db_write_duration_ms=7.6,
    )
    snapshot = client.get_snapshot()
    metrics = snapshot["metrics"]

    assert metrics["last_flush_latency_ms"] == 12
    assert metrics["last_db_write_duration_ms"] == 8
    assert metrics["last_write_path"] == "copy_staging"
    assert metrics["copy_batches"] == 1
    assert metrics["copy_rows"] == 3
    assert metrics["deduped_rows"] == 1
    assert metrics["table_stats"]["price_data"]["last_flush_latency_ms"] == 12
    assert metrics["table_stats"]["price_data"]["last_db_write_duration_ms"] == 8
    assert metrics["table_stats"]["price_data"]["last_write_path"] == "copy_staging"
