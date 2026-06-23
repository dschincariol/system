from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _reload_alert_modules():
    storage_placement = importlib.reload(importlib.import_module("engine.runtime.storage_placement"))
    backup_evidence = importlib.reload(importlib.import_module("engine.runtime.backup_evidence"))
    health_mod = importlib.reload(importlib.import_module("engine.runtime.health"))
    alerts = importlib.reload(importlib.import_module("engine.runtime.alerts"))
    alerts_notify = importlib.reload(importlib.import_module("engine.runtime.alerts_notify"))
    metrics_store = importlib.reload(importlib.import_module("engine.runtime.metrics_store"))
    job = importlib.reload(importlib.import_module("engine.strategy.jobs.observability_snapshot"))
    return job, storage_placement, backup_evidence, health_mod, alerts, alerts_notify, metrics_store


def _install_ok_storage(monkeypatch, storage_placement, health_mod) -> None:
    monkeypatch.setattr(
        storage_placement,
        "check_storage_placement",
        lambda: {"ok": True, "errors": [], "warnings": []},
    )
    monkeypatch.setattr(storage_placement, "storage_pressure_paths", lambda _env: [("zfs_pool", Path("/zpool"))])
    monkeypatch.setattr(
        health_mod,
        "get_disk_pressure_snapshot",
        lambda _paths: {"ok": True, "critical": [], "warnings": [], "paths": []},
    )


def _install_ok_pg_wal(monkeypatch, backup_evidence) -> None:
    monkeypatch.setattr(
        backup_evidence,
        "pg_wal_disk_risk_snapshot",
        lambda **_kwargs: {
            "ok": True,
            "blockers": [],
            "warnings": [],
            "wal_bytes": 1024,
            "ready_count": 0,
            "local_space": {"path": "/var/lib/postgresql/data/pg_wal", "free_bytes": 50_000_000_000},
        },
    )


def test_wal_archiver_failed_count_transition_emits_warn_once(monkeypatch):
    (
        job,
        storage_placement,
        backup_evidence,
        health_mod,
        alerts,
        alerts_notify,
        metrics_store,
    ) = _reload_alert_modules()
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("PREFLIGHT_REQUIRE_ZFS_STORAGE", "1")
    _install_ok_storage(monkeypatch, storage_placement, health_mod)
    _install_ok_pg_wal(monkeypatch, backup_evidence)

    failed_count = {"value": 1}

    def wal_archiver_snapshot(**_kwargs):
        return {
            "ok": True,
            "blockers": [],
            "warnings": [],
            "failed_count": failed_count["value"],
            "last_failed_wal": "0000000100000000000000CC",
            "last_failed_at_ts": 10.0,
            "last_archived_wal": "0000000100000000000000CD",
            "last_archived_at_ts": 20.0,
            "policy": {"wal_archive_max_age_s": 5.0},
        }

    emitted = []
    notified = []
    metrics = []
    monkeypatch.setattr(backup_evidence, "wal_archiver_runtime_snapshot", wal_archiver_snapshot)
    monkeypatch.setattr(alerts, "emit_runtime_alert", lambda **kwargs: emitted.append(kwargs) or {"inserted": True})
    monkeypatch.setattr(
        alerts_notify,
        "send_runtime_alert_notification",
        lambda payload, **kwargs: notified.append((payload, kwargs)) or {"ok": True, "delivered": 1},
    )
    monkeypatch.setattr(metrics_store, "write_runtime_metric", lambda *args, **kwargs: metrics.append((args, kwargs)))

    first = job._emit_storage_wal_alerts(ts_ms=1_000_000)
    failed_count["value"] = 2
    second = job._emit_storage_wal_alerts(ts_ms=1_001_000)
    third = job._emit_storage_wal_alerts(ts_ms=1_002_000)

    assert first["ok"] is True
    assert first["warnings"] == []
    assert second["ok"] is True
    assert any("wal_archiver_failed_count_increased" in item for item in second["warnings"])
    assert third["ok"] is True
    assert len(emitted) == 1
    assert len(notified) == 1
    assert emitted[0]["rule_id"] == "STORAGE_WAL_WARNING"
    assert emitted[0]["severity"] == "WARN"
    assert "last_failed_wal=0000000100000000000000CC" in "\n".join(emitted[0]["detail"]["warnings"])
    assert any(
        args[0] == "postgres.wal.alert_state" and kwargs.get("value_text") == "warning"
        for args, kwargs in metrics
    )


def test_pg_wal_disk_risk_alert_payload_names_wal_mount_and_dedupes(monkeypatch):
    (
        job,
        storage_placement,
        backup_evidence,
        health_mod,
        alerts,
        alerts_notify,
        metrics_store,
    ) = _reload_alert_modules()
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("PREFLIGHT_REQUIRE_ZFS_STORAGE", "1")
    _install_ok_storage(monkeypatch, storage_placement, health_mod)
    monkeypatch.setattr(
        backup_evidence,
        "wal_archiver_runtime_snapshot",
        lambda **_kwargs: {
            "ok": True,
            "blockers": [],
            "warnings": [],
            "failed_count": 0,
            "last_failed_at_ts": 0.0,
            "last_archived_at_ts": 1_000.0,
            "policy": {"wal_archive_max_age_s": 120.0},
        },
    )
    monkeypatch.setattr(
        backup_evidence,
        "pg_wal_disk_risk_snapshot",
        lambda **_kwargs: {
            "ok": False,
            "blockers": ["pg_wal_ready_backlog_critical", "pg_wal_free_space_critical"],
            "warnings": [],
            "wal_bytes": 42_000_000_000,
            "ready_count": 19,
            "local_space": {
                "path": "/var/lib/postgresql/data/pg_wal",
                "free_bytes": 1024,
                "status": "critical",
            },
        },
    )

    emitted = []
    notified = []
    metrics = []
    monkeypatch.setattr(alerts, "emit_runtime_alert", lambda **kwargs: emitted.append(kwargs) or {"inserted": True})
    monkeypatch.setattr(
        alerts_notify,
        "send_runtime_alert_notification",
        lambda payload, **kwargs: notified.append((payload, kwargs)) or {"ok": True, "delivered": 1},
    )
    monkeypatch.setattr(metrics_store, "write_runtime_metric", lambda *args, **kwargs: metrics.append((args, kwargs)))

    first = job._emit_storage_wal_alerts(ts_ms=2_000_000)
    second = job._emit_storage_wal_alerts(ts_ms=2_001_000)

    assert first["ok"] is False
    assert second["ok"] is False
    assert len(emitted) == 1
    assert len(notified) == 1
    alert = emitted[0]
    assert alert["rule_id"] == "PG_WAL_DISK_RISK"
    assert alert["severity"] == "CRIT"
    assert alert["detail"]["ready_count"] == 19
    assert alert["detail"]["wal_bytes"] == 42_000_000_000
    assert alert["detail"]["local_space"]["path"] == "/var/lib/postgresql/data/pg_wal"
    assert alert["detail"]["local_space"]["free_bytes"] == 1024
    assert any(
        args[0] == "postgres.wal.alert_state" and kwargs.get("value_text") == "critical"
        for args, kwargs in metrics
    )
