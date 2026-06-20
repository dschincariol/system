from __future__ import annotations

from urllib.parse import urlparse

from engine.api import api_system
from engine.api.readiness_evidence import build_readiness_evidence


NOW_MS = 1_800_000_000_000


def _base_health(now_ms: int = NOW_MS) -> dict:
    return {
        "ok": True,
        "ts_ms": now_ms,
        "provider_readiness": {
            "ok": True,
            "required": True,
            "required_providers": ["alpaca"],
            "by_provider": {
                "alpaca": {
                    "ok": True,
                    "last_ts_ms": now_ms,
                    "max_age_s": 300,
                    "source_key": "alpaca_prices",
                }
            },
        },
        "ingestion_freshness": {
            "ok": True,
            "critical_ok": True,
            "updated_ts_ms": now_ms,
        },
        "ingestion_sources": {
            "prices": {
                "ok": True,
                "stale": False,
                "updated_ts_ms": now_ms,
            }
        },
    }


def _base_readiness(mode: str = "live", now_ms: int = NOW_MS) -> dict:
    return {
        "ok": True,
        "ready": True,
        "mode": mode,
        "execution_mode": mode,
        "ts_ms": now_ms,
        "production_validation": {
            "gate_order": ["database_reachable"],
            "gates": {
                "database_reachable": {
                    "name": "database_reachable",
                    "ok": True,
                    "critical": True,
                    "reason": "ok",
                    "affected_subsystem": "storage",
                    "last_evaluated_ts_ms": now_ms,
                    "source": "startup_validation",
                }
            },
        },
    }


def _full_live_preflight(now_ms: int = NOW_MS) -> dict:
    return {
        "ok": True,
        "required": True,
        "reason": "ok",
        "blockers": [],
        "ts_ms": now_ms,
        "deployment_contract": {"ok": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "broker_contract": {"ok": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "broker_preflight": {"ok": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "initial_kill_switch_hold": {"ok": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "backup_restore_evidence": {"ok": True, "fresh": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "live_ai_safety": {"ok": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "execution_arming_audit": {"ok": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "position_reconcile_evidence": {"ok": True, "required": True, "reason": "ok", "ts_ms": now_ms},
        "options_instruments": {"ok": True, "required": False, "reason": "ok", "ts_ms": now_ms},
        "lob_deeplob_shadow": {"ok": True, "enabled": False, "required": False, "reason": "disabled", "ts_ms": now_ms},
    }


def test_readiness_evidence_normalizes_sources_and_fails_closed_for_missing_live_critical_evidence():
    payload = build_readiness_evidence(
        readiness_payload=_base_readiness("live"),
        health_payload=_base_health(),
        liveness_payload={"ok": True, "alive": True, "ts_ms": NOW_MS},
        execution_barrier={"allowed": True, "reason": "ok", "ts_ms": NOW_MS},
        kill_switches={"state": [], "loaded_ts_ms": NOW_MS, "cache_fresh": True},
        live_preflight=_full_live_preflight(),
        broker_config={
            "ok": True,
            "ts_ms": NOW_MS,
            "config": {
                "active_broker": "alpaca",
                "last_test_result": {"ok": True, "broker": "alpaca", "tested_ts_ms": NOW_MS},
            },
        },
        governance_evidence={"ok": True, "evidence": []},
        mode="live",
        execution_mode="live",
        target_broker="alpaca",
        now_ms=NOW_MS,
    )

    assert payload["ok"] is False
    assert payload["status"] == "blocked"
    assert payload["critical_context"] is True
    assert "/api/governance/evidence" in payload["source_routes"]

    item = next(row for row in payload["items"] if row["id"] == "governance.ope_gate")
    assert item.keys() >= {
        "id",
        "title",
        "status",
        "severity",
        "blocking",
        "source_subsystem",
        "source_route",
        "source_config_key",
        "freshness",
        "detail",
        "remediation",
    }
    assert item["status"] == "unavailable"
    assert item["severity"] == "critical"
    assert item["blocking"] is True
    assert item["freshness"]["stale"] is True


def test_readiness_evidence_marks_stale_broker_test_as_blocking_in_paper_mode(monkeypatch):
    monkeypatch.setenv("BROKER_CONNECTION_TEST_MAX_AGE_S", "60")
    payload = build_readiness_evidence(
        readiness_payload=_base_readiness("paper"),
        health_payload=_base_health(),
        liveness_payload={"ok": True, "alive": True, "ts_ms": NOW_MS},
        execution_barrier={"allowed": True, "reason": "ok", "ts_ms": NOW_MS},
        kill_switches={"state": [], "loaded_ts_ms": NOW_MS, "cache_fresh": True},
        live_preflight={"ok": True, "required": False, "reason": "not_required", "blockers": [], "ts_ms": NOW_MS},
        broker_config={
            "ok": True,
            "ts_ms": NOW_MS,
            "config": {
                "active_broker": "alpaca",
                "last_test_result": {"ok": True, "broker": "alpaca", "tested_ts_ms": NOW_MS - 120_000},
            },
        },
        governance_evidence={
            "ok": True,
            "evidence": [
                {"key": "ope_gate", "state": "pass", "freshness": "fresh", "last_update_ts_ms": NOW_MS},
                {"key": "experiment_ledger", "state": "pass", "freshness": "fresh", "last_update_ts_ms": NOW_MS},
                {"key": "production_monitoring", "state": "pass", "freshness": "fresh", "last_update_ts_ms": NOW_MS},
            ],
        },
        mode="paper",
        execution_mode="paper",
        target_broker="alpaca",
        now_ms=NOW_MS,
    )

    item = next(row for row in payload["items"] if row["id"] == "broker.config_test")
    assert item["status"] == "blocked"
    assert item["blocking"] is True
    assert item["freshness"]["stale"] is True
    assert payload["action_guards"]["broker_activation"]["allowed"] is False


def test_readiness_evidence_safe_mode_keeps_missing_governance_nonblocking():
    payload = build_readiness_evidence(
        readiness_payload=_base_readiness("safe"),
        health_payload=_base_health(),
        liveness_payload={"ok": True, "alive": True, "ts_ms": NOW_MS},
        execution_barrier={"allowed": False, "reason": "mode_safe", "ts_ms": NOW_MS},
        kill_switches={"state": [], "loaded_ts_ms": NOW_MS, "cache_fresh": True},
        live_preflight={"ok": True, "required": False, "reason": "not_required", "blockers": [], "ts_ms": NOW_MS},
        broker_config={"ok": True, "ts_ms": NOW_MS, "config": {"active_broker": "sim", "last_test_result": {}}},
        governance_evidence={"ok": True, "evidence": []},
        mode="safe",
        execution_mode="safe",
        target_broker="sim",
        now_ms=NOW_MS,
    )

    item = next(row for row in payload["items"] if row["id"] == "governance.ope_gate")
    assert item["status"] == "warning"
    assert item["severity"] == "warning"
    assert item["blocking"] is False


def test_api_get_readiness_evidence_route_uses_authoritative_sources(monkeypatch):
    monkeypatch.setattr(api_system, "api_get_readiness", lambda *_args, **_kwargs: _base_readiness("live"))
    monkeypatch.setattr(api_system, "_cached_health_snapshot", lambda **_kwargs: _base_health())
    monkeypatch.setattr(api_system, "api_get_liveness", lambda *_args, **_kwargs: {"ok": True, "alive": True, "ts_ms": NOW_MS})
    monkeypatch.setattr(
        api_system,
        "api_get_execution_barrier",
        lambda *_args, **_kwargs: {"ok": True, "allowed": True, "ts_ms": NOW_MS, "execution_barrier": {"allowed": True, "reason": "ok", "ts_ms": NOW_MS}},
    )
    monkeypatch.setattr(
        api_system,
        "market_data_status",
        lambda: {
            "ok": True,
            "running": True,
            "healthy_providers": 1,
            "fresh_rows": 10,
            "fresh_symbols": 1,
            "last_price_ts_ms": NOW_MS,
            "price_age_ms": 0,
            "providers": {},
            "updated_ts_ms": NOW_MS,
        },
    )

    import engine.runtime.health as runtime_health
    import engine.runtime.live_trading_preflight as live_preflight
    import engine.api.api_broker_config as broker_config
    import engine.api.governance_evidence as governance

    monkeypatch.setattr(runtime_health, "get_kill_switch_snapshot_readonly", lambda: {"state": [], "loaded_ts_ms": NOW_MS, "cache_fresh": True})
    monkeypatch.setattr(live_preflight, "live_trading_preflight", lambda **_kwargs: _full_live_preflight())
    monkeypatch.setattr(
        broker_config,
        "api_get_broker_config",
        lambda *_args, **_kwargs: {
            "ok": True,
            "ts_ms": NOW_MS,
            "config": {
                "active_broker": "alpaca",
                "last_test_result": {"ok": True, "broker": "alpaca", "tested_ts_ms": NOW_MS},
            },
        },
    )
    monkeypatch.setattr(
        governance,
        "build_governance_evidence_summary",
        lambda **_kwargs: {
            "ok": True,
            "evidence": [
                {"key": "ope_gate", "state": "pass", "freshness": "fresh", "last_update_ts_ms": NOW_MS},
                {"key": "experiment_ledger", "state": "pass", "freshness": "fresh", "last_update_ts_ms": NOW_MS},
                {"key": "production_monitoring", "state": "pass", "freshness": "fresh", "last_update_ts_ms": NOW_MS},
            ],
        },
    )

    payload = api_system.api_get_readiness_evidence(
        urlparse("/api/operator/readiness_evidence?mode=live&execution_mode=live&broker=alpaca"),
        {},
    )

    assert payload["ok"] is True
    assert payload["mode"] == "live"
    assert any(row["id"] == "broker.config_test" for row in payload["items"])
    assert any(row["source_route"] == "/api/operator/provider_telemetry" for row in payload["items"])
