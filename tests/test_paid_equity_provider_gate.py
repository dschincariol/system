from __future__ import annotations

import importlib
import json
import sys
import uuid
from typing import Any, Dict

import pytest


pytestmark = pytest.mark.safety_critical


def _load_preflight():
    import engine.runtime.prod_preflight as prod_preflight

    return importlib.reload(prod_preflight)


def _degraded_polygon_snapshot(**_kwargs: Any) -> Dict[str, Any]:
    return {
        "ok": False,
        "required": True,
        "mode": "paper",
        "reason": "provider_unauthenticated:polygon",
        "blockers": ["provider_unauthenticated:polygon"],
        "required_providers": ["polygon"],
        "healthy_required": 0,
        "total_required": 1,
        "by_provider": {
            "polygon": {
                "ok": False,
                "credential_source": "missing",
                "credential_secret_names": ["POLYGON_API_KEY"],
                "blockers": ["provider_unauthenticated:polygon"],
                "telemetry_present": False,
                "telemetry_ok": False,
            }
        },
    }


def _healthy_polygon_snapshot(**_kwargs: Any) -> Dict[str, Any]:
    return {
        "ok": True,
        "required": True,
        "mode": "paper",
        "reason": "ok",
        "blockers": [],
        "required_providers": ["polygon"],
        "healthy_required": 1,
        "total_required": 1,
        "by_provider": {
            "polygon": {
                "ok": True,
                "credential_source": "secret_loader",
                "credential_secret_names": ["POLYGON_API_KEY"],
                "blockers": [],
                "telemetry_present": True,
                "telemetry_ok": True,
            }
        },
    }


def _no_equity_snapshot(**_kwargs: Any) -> Dict[str, Any]:
    return {
        "ok": True,
        "required": True,
        "mode": "paper",
        "reason": "ok",
        "blockers": [],
        "required_providers": ["ccxt"],
        "by_provider": {"ccxt": {"ok": True}},
    }


def _patch_preflight_until_paid_gate(monkeypatch: pytest.MonkeyPatch, prod_preflight: Any) -> None:
    monkeypatch.setattr(prod_preflight, "_production_provisioning_gate", lambda: (["provisioning ok"], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_cpu_power_policy_gate", lambda: (["cpu ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_disk_pressure_gate", lambda: (["disk ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_memory_pressure_gate", lambda: (["memory ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_storage_placement_gate", lambda: (["storage ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_postgres_tuning_gate", lambda: (["pg tuning ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_wal_archiver_runtime_gate", lambda: (["wal archiver ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_pg_wal_disk_risk_gate", lambda: (["pg wal ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_ingestion_tuning_gate", lambda: (["ingestion tuning ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_ingestion_soak_gate", lambda: (["ingestion soak ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_refetchable_pg_durability_gate", lambda: (["durability ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_ingestion_shard_gate", lambda: (["ingestion shard ok"], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_runtime_config_gate", lambda: (["runtime config ok"], []))
    monkeypatch.setattr(prod_preflight, "_api_mutation_auth_gate", lambda: (["api auth ok"], []))
    monkeypatch.setattr(prod_preflight, "_operator_sidecar_security_gate", lambda: (["sidecar ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_network_exposure_gate", lambda: (["network ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_resource_isolation_gate", lambda: (["resource ok"], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_compile_files", lambda _files: [])
    monkeypatch.setattr(prod_preflight, "_ensure_schemas", lambda: ["schema ok"])
    monkeypatch.setattr(prod_preflight, "_verify_sqlite_contract", lambda: (["sqlite ok"], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_capital_equity_freshness_gate", lambda: (["capital equity ok"], [], [], {"ok": True}))
    monkeypatch.setattr(prod_preflight, "_check_external_services", lambda: (_ for _ in ()).throw(AssertionError("must stop before external services")))


def _last_json(stdout: str) -> Dict[str, Any]:
    for line in reversed(str(stdout).splitlines()):
        text = line.strip()
        if text.startswith("{") and text.endswith("}"):
            return dict(json.loads(text))
    raise AssertionError(f"no JSON object in stdout: {stdout!r}")


def test_paid_equity_provider_gate_fails_loud_and_main_returns_3(monkeypatch, capsys, caplog) -> None:
    prod_preflight = _load_preflight()
    canary = f"EQ08_SECRET_VALUE_{uuid.uuid4().hex}"
    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setenv("POLYGON_API_KEY", canary)
    monkeypatch.setattr(prod_preflight, "provider_readiness_snapshot", _degraded_polygon_snapshot, raising=False)

    notes, warnings, errors, state = prod_preflight._paid_equity_provider_degradation_gate()
    assert notes == []
    assert warnings == []
    assert errors
    assert "polygon" in errors[0]
    assert "POLYGON_API_KEY" in errors[0]
    assert state["degraded"][0]["provider"] == "polygon"

    _patch_preflight_until_paid_gate(monkeypatch, prod_preflight)
    monkeypatch.setattr(sys, "argv", ["prod_preflight.py", "--json"])
    rc = prod_preflight.main()

    captured = capsys.readouterr()
    payload = _last_json(captured.out)
    rendered = json.dumps(payload, sort_keys=True)
    assert rc == 3
    assert payload["paid_equity_provider_degradation"]["degraded"][0]["provider"] == "polygon"
    assert "POLYGON_API_KEY" in rendered
    assert canary not in rendered
    assert canary not in caplog.text


def test_paid_equity_provider_gate_fails_closed_on_snapshot_error(monkeypatch) -> None:
    prod_preflight = _load_preflight()
    monkeypatch.setenv("ENGINE_MODE", "live")

    def boom(**_kwargs: Any) -> Dict[str, Any]:
        raise RuntimeError("snapshot unavailable")

    monkeypatch.setattr(prod_preflight, "provider_readiness_snapshot", boom, raising=False)
    notes, warnings, errors, state = prod_preflight._paid_equity_provider_degradation_gate()

    assert notes == []
    assert warnings == []
    assert errors
    assert state["ok"] is False
    assert state["reason"] == "provider_readiness_snapshot_failed"


def test_paid_equity_provider_gate_strict_superset_paths(monkeypatch) -> None:
    prod_preflight = _load_preflight()

    def boom(**_kwargs: Any) -> Dict[str, Any]:
        raise AssertionError("safe mode should not inspect provider readiness")

    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setattr(prod_preflight, "provider_readiness_snapshot", boom, raising=False)
    notes, _warnings, errors, state = prod_preflight._paid_equity_provider_degradation_gate()
    assert errors == []
    assert state["reason"] == "mode_not_enforced"
    assert notes

    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setenv("PREFLIGHT_ENFORCE_PAID_EQUITY_PROVIDERS", "0")
    notes, _warnings, errors, state = prod_preflight._paid_equity_provider_degradation_gate()
    assert errors == []
    assert state["reason"] == "disabled"
    assert notes

    monkeypatch.setenv("PREFLIGHT_ENFORCE_PAID_EQUITY_PROVIDERS", "1")
    monkeypatch.setattr(prod_preflight, "provider_readiness_snapshot", _no_equity_snapshot, raising=False)
    notes, _warnings, errors, state = prod_preflight._paid_equity_provider_degradation_gate()
    assert errors == []
    assert state["reason"] == "no_required_equity_provider"
    assert notes

    monkeypatch.setattr(prod_preflight, "provider_readiness_snapshot", _healthy_polygon_snapshot, raising=False)
    notes, _warnings, errors, state = prod_preflight._paid_equity_provider_degradation_gate()
    assert errors == []
    assert state["ok"] is True
    assert state["required_equity_providers"] == ["polygon"]
