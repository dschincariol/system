from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        if name in sys.modules:
            modules.append(importlib.reload(sys.modules[name]))
        else:
            modules.append(importlib.import_module(name))
    return modules


def _source(*, configured: bool = True) -> dict:
    return {
        "source_key": "unit",
        "source_type": "price_provider",
        "job_name": "poll_prices",
        "credential_fields": ["api_key"],
        "credentials_configured": configured,
        "credentials_stored": configured,
        "credential_error": "",
        "error_count": 0,
    }


def test_provider_readiness_blocks_missing_required_credentials(monkeypatch):
    health, creds = _reload_modules("engine.runtime.health", "engine.data._credentials")
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(
        health,
        "_manager_provider_sources",
        lambda: (
            {
                "polygon": _source(configured=False),
                "tradier": {**_source(configured=False), "source_type": "options_provider", "job_name": "options_poll"},
            },
            ["polygon", "tradier"],
            "",
        ),
    )
    monkeypatch.setattr(creds, "get_data_credential", lambda *_args, **_kwargs: "")

    snapshot = health.provider_readiness_snapshot(
        mode="paper",
        health={
            "providers": {
                "by_provider": {
                    "polygon": {"ok": True, "age_s": 1, "last_ts_ms": now_ms},
                    "tradier": {"ok": True, "age_s": 1, "last_ts_ms": now_ms},
                }
            }
        },
        now_ms=now_ms,
    )

    assert snapshot["ok"] is False
    assert "provider_unauthenticated:polygon" in snapshot["blockers"]
    assert "provider_unauthenticated:tradier" in snapshot["blockers"]
    assert snapshot["by_provider"]["polygon"]["credential_source"] == "missing"


def test_provider_readiness_blocks_stale_and_open_circuit(monkeypatch):
    (health,) = _reload_modules("engine.runtime.health")
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(
        health,
        "_manager_provider_sources",
        lambda: (
            {
                "polygon": _source(configured=True),
                "tradier": {**_source(configured=True), "source_type": "options_provider", "job_name": "options_poll"},
            },
            ["polygon", "tradier"],
            "",
        ),
    )

    snapshot = health.provider_readiness_snapshot(
        mode="live",
        health={
            "providers": {
                "by_provider": {
                    "polygon": {"ok": True, "age_s": 999, "last_ts_ms": now_ms - 999_000},
                    "tradier": {
                        "ok": False,
                        "age_s": 2,
                        "last_ts_ms": now_ms - 2_000,
                        "error_count": 3,
                        "circuit_open": True,
                        "error": "tradier_http_error:503",
                    },
                }
            }
        },
        now_ms=now_ms,
    )

    assert snapshot["ok"] is False
    assert "provider_stale:polygon" in snapshot["blockers"]
    assert "provider_circuit_open:tradier" in snapshot["blockers"]
    assert "provider_unhealthy:tradier" in snapshot["blockers"]


def test_provider_readiness_recovers_after_fresh_success_with_prior_errors(monkeypatch):
    (health,) = _reload_modules("engine.runtime.health")
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(
        health,
        "_manager_provider_sources",
        lambda: ({"polygon": _source(configured=True)}, ["polygon"], ""),
    )

    snapshot = health.provider_readiness_snapshot(
        mode="live",
        health={
            "providers": {
                "by_provider": {
                    "polygon": {
                        "ok": True,
                        "age_s": 1,
                        "last_ts_ms": now_ms - 1_000,
                        "error_count": 99,
                        "circuit_open": False,
                    }
                }
            }
        },
        now_ms=now_ms,
    )

    assert snapshot["ok"] is True
    assert snapshot["blockers"] == []
    assert snapshot["by_provider"]["polygon"]["error_count"] == 99


def test_readiness_snapshot_blocks_required_provider_readiness():
    (health,) = _reload_modules("engine.runtime.health")

    snapshot = health.get_readiness_snapshot(
        health={
            "prices": {"ok": True, "last_ts_ms": 123, "age_s": 1, "max_age_s": 60},
            "providers": {"ok": True, "healthy": 1, "total": 1},
            "provider_readiness": {
                "ok": False,
                "required": True,
                "required_providers": ["polygon"],
                "blockers": ["provider_stale:polygon"],
            },
            "labels": {"ok": True, "count": 10},
            "model": {"ok": True, "support_n": 10},
            "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": True},
            "broker_connection": {"ok": True, "state": "connected", "broker": "paper"},
            "db": {"ok": True, "initialized": True, "exists": True, "db_path": ":memory:"},
            "job_summary": {"ok": True, "total": 1, "stale": 0, "stale_jobs": []},
            "timeseries_storage": {"ok": True, "enabled": False},
            "feature_store": {"ok": True, "enabled": False},
            "portfolio_runtime": {"degraded": False, "detail": "ok"},
            "position_reconcile": {"required": True, "ok": True, "available": True, "blockers": []},
            "execution_supervisor": {"ok": True, "state": "ok", "failed_gates": []},
            "execution_degraded": {"active": False, "severity": "WARNING"},
            "startup_validation": {"ok": True},
        },
        preflight={"ok": True},
        system_state={"state": "LIVE", "mode": "paper"},
        graph={"ok": True},
    )

    issue_codes = {str(item.get("code") or "") for item in list(snapshot.get("issues") or [])}
    assert snapshot["ok"] is False
    assert snapshot["provider_readiness_ok"] is False
    assert "provider_readiness_failed" in issue_codes
    assert "provider_readiness" in list(snapshot.get("waiting_on") or [])


def test_promotion_guard_blocks_paper_when_provider_readiness_fails(monkeypatch, tmp_path):
    db_path = tmp_path / "promotion_provider_readiness.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setenv("BROKER", "paper")
    monkeypatch.setenv("BROKER_NAME", "paper")

    _, storage, promotion_guard, position_reconcile, health = _reload_modules(
        "engine.runtime.db_guard",
        "engine.runtime.storage",
        "engine.strategy.promotion_guard",
        "engine.execution.position_reconcile",
        "engine.runtime.health",
    )
    storage.init_db()
    monkeypatch.setattr(
        position_reconcile,
        "position_reconcile_evidence_snapshot",
        lambda **_kwargs: {"ok": True, "required": True, "available": True, "blockers": []},
    )
    monkeypatch.setattr(
        health,
        "provider_readiness_snapshot",
        lambda **_kwargs: {
            "ok": False,
            "required": True,
            "blockers": ["provider_stale:polygon"],
            "required_providers": ["polygon"],
        },
    )

    try:
        allowed, reason = promotion_guard.promotion_allowed()
    finally:
        storage.close_pooled_connections()

    assert allowed is False
    assert "provider_stale:polygon" in list(reason.get("blockers") or [])
    assert reason["provider_readiness"]["required"] is True
