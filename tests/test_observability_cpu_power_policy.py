from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_observability_snapshot_records_cpu_power_policy_drift(monkeypatch):
    observability = importlib.reload(importlib.import_module("engine.runtime.observability"))
    job = importlib.reload(importlib.import_module("engine.strategy.jobs.observability_snapshot"))

    monkeypatch.setattr(
        job,
        "snapshot_pg_observability",
        lambda ts_ms: {"ok": True, "emitted": 0, "skipped": False, "reason": ""},
    )
    monkeypatch.setattr(job, "_emit_redis_circuit_state", lambda ts_ms: 0)
    monkeypatch.setattr(job, "host_memory_pressure_snapshot", lambda: {"required": False, "ok": True, "status": "pass"})
    monkeypatch.setattr(
        job,
        "verify_cpu_power_policy",
        lambda: {
            "required": True,
            "ok": False,
            "status": "drift",
            "reason": "cpu_power_policy_drift",
            "returncode": 1,
            "summary": (
                "power_profile=balanced "
                "energy_performance_preference=balance_performance (2/2) "
                "intended_state=FAIL expected profile=performance or epp=performance or governor=performance"
            ),
            "parsed": {
                "power_profile": "balanced",
                "energy_performance_preference": "balance_performance (2/2)",
                "intended_state": (
                    "FAIL expected profile=performance or epp=performance or governor=performance"
                ),
            },
        },
    )

    result = job.run_once()
    health = observability.get_component_health_snapshot("cpu_power_policy")

    assert result["cpu_power_policy"]["status"] == "drift"
    assert result["cpu_power_policy"]["health_ok"] is False
    assert health["ok"] is False
    assert health["status"] == "drift"
    assert health["detail"] == "cpu_power_policy_drift"
    assert "intended_state=FAIL" in health["summary"]


def test_observability_snapshot_emits_storage_wal_guard_alerts(monkeypatch):
    observability = importlib.reload(importlib.import_module("engine.runtime.observability"))
    storage_placement = importlib.reload(importlib.import_module("engine.runtime.storage_placement"))
    backup_evidence = importlib.reload(importlib.import_module("engine.runtime.backup_evidence"))
    health_mod = importlib.reload(importlib.import_module("engine.runtime.health"))
    job = importlib.reload(importlib.import_module("engine.strategy.jobs.observability_snapshot"))

    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("PREFLIGHT_REQUIRE_ZFS_STORAGE", "1")
    monkeypatch.setattr(
        job,
        "snapshot_pg_observability",
        lambda ts_ms: {"ok": True, "emitted": 0, "skipped": False, "reason": ""},
    )
    monkeypatch.setattr(job, "_emit_redis_circuit_state", lambda ts_ms: 0)
    monkeypatch.setattr(job, "verify_cpu_power_policy", lambda: {"required": False, "ok": True, "status": "ok"})
    monkeypatch.setattr(job, "host_memory_pressure_snapshot", lambda: {"required": False, "ok": True, "status": "pass"})
    monkeypatch.setattr(
        storage_placement,
        "check_storage_placement",
        lambda: {
            "ok": False,
            "errors": [
                "storage placement invalid target=timescale_pgdata "
                "reason=forbidden_host_prefix path=/var/lib/docker/volumes/timescaledb-data/_data"
            ],
            "warnings": [],
        },
    )
    monkeypatch.setattr(storage_placement, "storage_pressure_paths", lambda _env: [("root", Path("/"))])
    monkeypatch.setattr(
        backup_evidence,
        "wal_archiver_runtime_snapshot",
        lambda **_kwargs: {"ok": False, "blockers": ["wal_archiver_failed_after_last_archive"], "warnings": []},
    )
    monkeypatch.setattr(
        backup_evidence,
        "pg_wal_disk_risk_snapshot",
        lambda **_kwargs: {"ok": False, "blockers": ["pg_wal_bytes_exceeds_budget"], "warnings": []},
    )
    monkeypatch.setattr(
        health_mod,
        "get_disk_pressure_snapshot",
        lambda _paths: {"ok": False, "critical": ["zfs_pool:disk_critical:free_bytes=1:free_pct=0.01"], "warnings": []},
    )
    emitted = []
    monkeypatch.setattr(
        job,
        "_emit_storage_runtime_alert",
        lambda **kwargs: emitted.append(kwargs["rule_id"]) or 1,
    )

    result = job.run_once()
    guard = observability.get_component_health_snapshot("storage_wal_guards")

    assert result["storage_wal_alerts"]["ok"] is False
    assert guard["ok"] is False
    assert "STORAGE_PLACEMENT_INVALID" in emitted
    assert "WAL_ARCHIVER_OUTAGE" in emitted
    assert "PG_WAL_DISK_RISK" in emitted
    assert "STORAGE_FREE_SPACE_CRITICAL" in emitted


def test_observability_snapshot_records_memory_pressure_drift(monkeypatch):
    observability = importlib.reload(importlib.import_module("engine.runtime.observability"))
    job = importlib.reload(importlib.import_module("engine.strategy.jobs.observability_snapshot"))

    monkeypatch.setattr(
        job,
        "snapshot_pg_observability",
        lambda ts_ms: {"ok": True, "emitted": 0, "skipped": False, "reason": ""},
    )
    monkeypatch.setattr(job, "_emit_redis_circuit_state", lambda ts_ms: 0)
    monkeypatch.setattr(job, "verify_cpu_power_policy", lambda: {"required": False, "ok": True, "status": "ok"})
    monkeypatch.setattr(
        job,
        "host_memory_pressure_snapshot",
        lambda: {
            "required": True,
            "ok": False,
            "status": "fail",
            "reason": "memory_pressure_total_swap_below_policy",
            "errors": ["memory_pressure_total_swap_below_policy"],
            "warnings": [],
        },
    )
    monkeypatch.setattr(job, "_emit_storage_wal_alerts", lambda ts_ms: {"ok": True, "enabled": False, "emitted": 0})

    result = job.run_once()
    health = observability.get_component_health_snapshot("memory_pressure_policy")

    assert result["memory_pressure_policy"]["status"] == "fail"
    assert result["memory_pressure_policy"]["health_ok"] is False
    assert health["ok"] is False
    assert health["status"] == "fail"
    assert health["detail"] == "memory_pressure_total_swap_below_policy"
