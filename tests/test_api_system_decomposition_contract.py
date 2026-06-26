from __future__ import annotations

import importlib
import inspect
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_api_system():
    module = importlib.import_module("engine.api.api_system")
    return importlib.reload(module)


EXPECTED_ROUTE_SPECS_SYSTEM = [
    ("GET", "/api/system/kill_switches", "api_get_kill_switches"),
    ("GET", "/api/system/state", "api_get_system_state"),
    ("GET", "/api/system/health", "api_get_health"),
    ("GET", "/api/system/liveness", "api_get_liveness"),
    ("GET", "/api/system/competition", "api_get_competition_view"),
    ("GET", "/api/system/replay_freshness", "api_get_replay_freshness"),
    ("GET", "/api/system/attribution_quality", "api_get_attribution_quality"),
    ("GET", "/api/system/mode", "api_get_status"),
    ("GET", "/api/system/config", "api_get_runtime_config"),
    ("GET", "/api/supervisor/status", "api_get_supervisor_status"),
    ("GET", "/api/ingestion/status", "api_get_ingestion_status"),
    ("GET", "/api/health", "api_get_health"),
    ("GET", "/api/liveness", "api_get_liveness"),
    ("GET", "/api/status", "api_get_status"),
    ("GET", "/api/readiness", "api_get_readiness"),
    ("GET", "/api/readiness/evidence", "api_get_readiness_evidence"),
    ("GET", "/api/system/readiness_evidence", "api_get_readiness_evidence"),
    ("GET", "/api/system/trading_readiness", "api_get_trading_readiness"),
    ("GET", "/api/operator/readiness_evidence", "api_get_readiness_evidence"),
    ("GET", "/api/operator/trading_readiness", "api_get_trading_readiness"),
    ("GET", "/api/operator/preflight_report", "api_get_preflight_report"),
    ("GET", "/api/operator/runtime_watchdogs", "api_get_runtime_watchdogs"),
    ("GET", "/api/operator/service_status", "api_get_service_status"),
    ("GET", "/api/operator/support_snapshot", "api_get_support_snapshot"),
    ("GET", "/api/operator/snapshot", "api_get_support_snapshot"),
    ("GET", "/api/operator/competition", "api_get_competition_view"),
    ("GET", "/api/operator/replay_freshness", "api_get_replay_freshness"),
    ("GET", "/api/operator/attribution_quality", "api_get_attribution_quality"),
    ("GET", "/api/operator/provider_telemetry", "api_get_provider_telemetry"),
    ("GET", "/api/operator/supervisor_diagnostics", "api_get_supervisor_diagnostics"),
    ("GET", "/api/telemetry", "api_get_telemetry"),
    ("GET", "/api/telemetry/history", "api_get_telemetry_history"),
    ("GET", "/api/execution/barrier", "api_get_execution_barrier"),
    ("GET", "/api/risk/portfolio", "api_get_portfolio_risk"),
    ("GET", "/api/risk/monte_carlo", "api_get_monte_carlo_risk"),
    ("GET", "/api/risk/var_backtest", "api_get_risk_var_backtest"),
    ("GET", "/api/alpha_decay", "api_get_alpha_decay"),
    ("GET", "/api/regime/context", "api_get_regime_context"),
    ("GET", "/api/regime/history", "api_get_regime_history"),
    ("GET", "/api/drift/explainer", "api_get_drift_explainer"),
    ("GET", "/api/allocator/status", "api_get_allocator_status"),
    ("GET", "/api/training_status", "api_get_training_status"),
    ("GET", "/api/server/status", "api_get_server_status"),
    ("POST", "/api/server/shutdown", "api_post_server_shutdown"),
]


def test_api_system_route_table_and_handler_surface_are_characterized():
    api_system = _reload_api_system()

    assert api_system.ROUTE_SPECS_SYSTEM == EXPECTED_ROUTE_SPECS_SYSTEM
    assert ("POST", "/api/system/self_repair", "api_post_self_repair") not in api_system.ROUTE_SPECS_SYSTEM
    assert ("POST", "/api/repair_schema", "api_post_repair_schema") not in api_system.ROUTE_SPECS_SYSTEM

    public_handlers = {
        "api_get_runtime_config": "(_parsed, ctx=None)",
        "api_get_system_state": "(_parsed, ctx=None)",
        "api_get_supervisor_status": "(_parsed, ctx=None)",
        "api_get_ingestion_status": "(_parsed, ctx=None)",
        "api_get_health": "(_parsed, ctx=None)",
        "api_get_liveness": "(_parsed, ctx=None)",
        "api_get_runtime_health": "(_parsed, ctx=None)",
        "api_get_status": "(_parsed, ctx=None)",
        "api_get_readiness": "(_parsed, ctx=None)",
        "api_get_trading_readiness": "(_parsed, ctx=None)",
        "api_get_readiness_evidence": "(_parsed, ctx=None)",
        "api_get_preflight_report": "(_parsed, ctx=None)",
        "api_get_runtime_watchdogs": "(_parsed, ctx=None)",
        "api_get_service_status": "(_parsed, ctx=None)",
        "api_get_support_snapshot": "(_parsed, ctx=None)",
        "api_get_provider_telemetry": "(_parsed, ctx=None)",
        "api_get_supervisor_diagnostics": "(_parsed, ctx=None)",
        "api_get_telemetry": "(_parsed, ctx=None)",
        "api_get_telemetry_history": "(parsed, ctx=None)",
        "api_get_alpha_decay": "(parsed=None, ctx=None)",
        "api_get_portfolio_risk": "(_parsed, ctx=None)",
        "api_get_risk_var_backtest": "(parsed=None, ctx=None)",
    }
    for name, signature in public_handlers.items():
        handler = getattr(api_system, name, None)
        assert callable(handler), name
        assert str(inspect.signature(handler)) == signature
        assert getattr(handler, "__module__", "") == "engine.api.api_system"

    assert str(inspect.signature(api_system.api_post_repair_schema)) == "(_parsed=None, body=None, ctx=None)"
    assert str(inspect.signature(api_system.api_post_self_repair)) == "(_parsed=None, body=None, ctx=None)"
    assert getattr(api_system.api_post_repair_schema, "__module__", "") == "engine.api.api_self_repair"
    assert getattr(api_system.api_post_self_repair, "__module__", "") == "engine.api.api_self_repair"


def test_api_system_shared_response_helpers_are_characterized():
    api_system = _reload_api_system()

    assert api_system._dedupe_reasons(["a", "b"], ["b", "", None, "c"]) == ["a", "b", "c"]
    assert api_system._safe_json_dict('{"ok": true, "n": 2}') == {"ok": True, "n": 2}
    assert api_system._safe_json_dict("[1, 2]") == {}
    assert api_system._float_or_none("") is None
    assert api_system._float_or_none("3.25") == 3.25
    assert api_system._dict_or_empty({"a": 1}) == {"a": 1}
    assert api_system._dict_or_empty([("a", 1)]) == {}
    assert api_system._list_or_empty([1, 2]) == [1, 2]
    assert api_system._list_or_empty(("x",)) == []

    snapshot_payload = api_system._snapshot_response(
        {
            "status": "RUNNING",
            "state": "RUNNING",
            "mode": "safe",
            "execution_allowed": True,
            "reasons": ["base", "duplicate"],
        },
        ok=True,
        reasons=["duplicate", "extra"],
    )
    assert snapshot_payload["ok"] is True
    assert snapshot_payload["status"] == "RUNNING"
    assert snapshot_payload["execution_allowed"] is True
    assert snapshot_payload["reasons"] == ["base", "duplicate", "extra"]
    for key in ("health", "ingestion", "services", "readiness", "timestamps"):
        assert isinstance(snapshot_payload[key], dict)

    required_tables = api_system._required_tables_status(
        {
            "have_tables": ["prices", "job_history"],
            "missing_tables": ["alerts"],
        }
    )
    assert required_tables["ok"] is False
    assert required_tables["tables"]["prices"]["ok"] is True
    assert required_tables["tables"]["jobs"]["ok"] is True
    assert required_tables["tables"]["alerts"]["ok"] is False
    assert "required_table_missing:alerts" in required_tables["reasons"]

    storage = api_system._storage_readiness_from_health(
        {
            "startup_validation": {
                "checks": {
                    "core_services_initialized": {
                        "storage": {"status": "ready", "detail": "ok"}
                    }
                }
            }
        }
    )
    assert storage == {"status": "ready", "detail": "ok", "ok": True, "checked": True}
    assert api_system._normalized_health_from_snapshot({"health": {"health": {"ok": True}}}) == {"ok": True}
