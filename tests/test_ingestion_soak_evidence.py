from __future__ import annotations

import json
from typing import Any


def _policy_ok() -> dict[str, Any]:
    return {
        "ok": True,
        "evidence_available": True,
        "errors": [],
        "missing_hypertables": [],
        "missing_dimensions": [],
        "chunk_interval_mismatches": [],
        "missing_indexes": [],
        "missing_compression_jobs": [],
    }


def _healthy_writer_health() -> dict[str, Any]:
    return {
        "async_price_persistence": {
            "ok": True,
            "enabled": True,
            "queue_depth": 1,
            "queue_rows": 10,
            "queue_maxsize": 100,
            "queue_fill_ratio": 0.1,
            "spool_pending_rows": 0,
            "spool_pending_bytes": 0,
            "spool_oldest_age_ms": 0,
            "dead_letters": 0,
            "backpressure_active": False,
            "retry_count": 0,
            "last_flush_latency_ms": 2,
            "last_db_write_duration_ms": 4,
        },
        "telemetry_append_buffer": {
            "ok": True,
            "enabled": True,
            "write_path": "postgres_copy",
            "queue_depth": 1,
            "queue_fill_ratio": 0.1,
            "oldest_age_ms": 0,
            "spool_pending_rows": 0,
            "spool_pending_bytes": 0,
            "backpressure_active": False,
            "flush_failures": 0,
        },
        "ingestion_runtime": {
            "writer_diagnostics": {
                "options_poll_durable_buffer": {
                    "ok": True,
                    "pending_rows": 0,
                    "pending_bytes": 0,
                    "oldest_age_ms": 0,
                    "rows_fill_ratio": 0.0,
                    "bytes_fill_ratio": 0.0,
                    "backpressure_active": False,
                }
            }
        },
        "timescale": {
            "ok": True,
            "enabled": True,
            "queue_depth": 0,
            "queue_maxsize": 64,
            "batch_size": 4000,
            "copy_staging_enabled": True,
            "copy_staging_fallback_enabled": True,
            "schema_ready": True,
            "schema_ok": True,
            "metrics": {
                "copy_fallback_count": 0,
                "flush_failure_count": 0,
                "backpressure_count": 0,
            },
        },
        "pg_price_storage": {
            "ok": True,
            "enabled": True,
            "pool_ready": True,
            "copy_enabled": True,
            "copy_fallbacks": 0,
            "write_circuit_open": False,
            "last_write_path": "copy_staging",
            "write_failures": 0,
        },
    }


def test_ingestion_soak_report_accepts_healthy_live_evidence_and_redacts_env(monkeypatch) -> None:
    from engine.runtime import ingestion_soak

    monkeypatch.setattr(
        ingestion_soak,
        "_redis_evidence",
        lambda: {
            "dependency_available": True,
            "pool_size": 16,
            "healthcheck_interval_s": 15.0,
            "cache_circuit_state": "closed",
            "cache_circuit_failures": 0,
        },
    )
    env = {
        "INGESTION_SOAK_REQUIRE_EVIDENCE": "1",
        "INGESTION_SOAK_QUERY_TIMESCALE": "0",
        "TIMESCALE_DSN": "postgresql://trading:super-secret@timescaledb/trading",
    }

    report = ingestion_soak.collect_ingestion_soak_report(
        _healthy_writer_health(),
        env=env,
        policy_evidence=_policy_ok(),
    )

    assert report["ok"] is True
    assert report["required"] is True
    assert report["summary"]["policy_evidence_available"] is True
    rendered = json.dumps(report, sort_keys=True)
    assert "super-secret" not in rendered


def test_ingestion_soak_report_blocks_backpressure_copy_fallback_and_spool_pressure(monkeypatch) -> None:
    from engine.runtime import ingestion_soak

    monkeypatch.setattr(ingestion_soak, "_redis_evidence", lambda: {"cache_circuit_state": "closed"})
    health = _healthy_writer_health()
    health["async_price_persistence"]["backpressure_active"] = True
    health["async_price_persistence"]["spool_pending_bytes"] = 900
    health["async_price_persistence"]["spool_bytes_fill_ratio"] = 0.95
    health["pg_price_storage"]["copy_fallbacks"] = 1

    report = ingestion_soak.collect_ingestion_soak_report(
        health,
        env={"INGESTION_SOAK_REQUIRE_EVIDENCE": "1", "INGESTION_SOAK_QUERY_TIMESCALE": "0"},
        policy_evidence=_policy_ok(),
    )

    assert report["ok"] is False
    assert "async_price_writer:backpressure_active" in report["errors"]
    assert "async_price_writer:spool_byte_pressure" in report["errors"]
    assert "pg_price_storage:copy_fallbacks" in report["errors"]


def test_required_ingestion_soak_blocks_missing_writer_and_policy_evidence(monkeypatch) -> None:
    from engine.runtime import ingestion_soak

    monkeypatch.setattr(ingestion_soak, "_redis_evidence", lambda: {"cache_circuit_state": "closed"})

    report = ingestion_soak.collect_ingestion_soak_report(
        {},
        env={"INGESTION_SOAK_REQUIRE_EVIDENCE": "1", "INGESTION_SOAK_QUERY_TIMESCALE": "0"},
    )

    assert report["ok"] is False
    assert "ingestion_soak:writer_evidence_missing" in report["errors"]
    assert "timescale_policy:evidence_missing" in report["errors"]


class _FakeResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class _FakePolicyConn:
    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> _FakeResult:
        del params
        text = str(sql)
        if "timescaledb_information.hypertables" in text:
            return _FakeResult(
                [
                    {
                        "hypertable_schema": "public",
                        "hypertable_name": "price_ticks",
                        "num_dimensions": 1,
                        "compression_enabled": True,
                    }
                ]
            )
        if "timescaledb_information.dimensions" in text:
            return _FakeResult(
                [
                    {
                        "hypertable_name": "price_ticks",
                        "column_name": "ts_ms",
                        "time_interval": "7 days",
                        "integer_interval": None,
                    }
                ]
            )
        if "timescaledb_information.jobs" in text:
            return _FakeResult([])
        if "pg_indexes" in text:
            return _FakeResult([])
        raise AssertionError(f"unexpected sql: {text}")


def test_timescale_policy_evidence_reports_applied_policy_mismatches(monkeypatch) -> None:
    from engine.runtime import ingestion_soak

    monkeypatch.setattr(ingestion_soak, "_expected_timescale_tables", lambda: ("price_ticks",))
    monkeypatch.setattr(ingestion_soak, "_expected_indexes", lambda: ("idx_required",))
    monkeypatch.setattr(ingestion_soak, "_desired_chunk_interval_ms", lambda _table: 86_400_000)

    policy = ingestion_soak.query_timescale_policy_evidence(_FakePolicyConn())

    assert policy["ok"] is False
    assert policy["chunk_interval_mismatches"] == [
        {
            "table": "price_ticks",
            "actual_interval_ms": 604_800_000,
            "desired_interval_ms": 86_400_000,
        }
    ]
    assert policy["missing_indexes"] == ["idx_required"]
    assert policy["missing_compression_jobs"] == ["price_ticks"]


def test_readiness_blocks_when_required_ingestion_soak_is_unhealthy() -> None:
    from engine.runtime.health_readiness import get_readiness_snapshot

    health = {
        "db": {"ok": True, "initialized": True},
        "prices": {"ok": True},
        "providers": {"ok": True},
        "job_summary": {"ok": True, "total": 1},
        "labels": {"ok": True},
        "model": {"ok": True},
        "execution_barrier": {"allowed": True},
        "timeseries_storage": {"enabled": True, "ok": True},
        "ingestion_soak": {
            "required": True,
            "ok": False,
            "status": "degraded",
            "errors": ["async_price_writer:backpressure_active"],
        },
        "portfolio_runtime": {"degraded": False},
        "position_reconcile": {"ok": True},
        "execution_supervisor": {"ok": True, "state": "ok"},
    }

    readiness = get_readiness_snapshot(health=health, environ={"ENGINE_MODE": "safe"})

    assert readiness["ready"] is False
    assert "ingestion_soak" in readiness["waiting_on"]
    assert any(issue["code"] == "ingestion_soak_not_ready" for issue in readiness["issues"])
