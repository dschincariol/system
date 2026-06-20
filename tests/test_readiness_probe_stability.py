import importlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def test_health_snapshot_singleflight_returns_prompt_degraded(monkeypatch):
    health = importlib.reload(importlib.import_module("engine.runtime.health"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setitem(health._HEALTH_SNAPSHOT_CACHE, "ts_ms", 0)
    monkeypatch.setitem(health._HEALTH_SNAPSHOT_CACHE, "payload", None)
    monkeypatch.setattr(
        health,
        "_db_connect",
        lambda: (_ for _ in ()).throw(AssertionError("db connect should not run while refresh is in flight")),
    )

    assert health._HEALTH_SNAPSHOT_REFRESH_LOCK.acquire(blocking=False)
    try:
        started = time.perf_counter()
        snapshot = health.get_health_snapshot()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
    finally:
        health._HEALTH_SNAPSHOT_REFRESH_LOCK.release()

    assert elapsed_ms < 100
    assert snapshot["ok"] is False
    assert snapshot["cache"]["refresh_in_flight"] is True
    assert snapshot["execution_barrier"]["real_trading_allowed"] is False


def test_repeated_readiness_probes_coalesce_health_refresh(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")

    with api_system._HEALTH_CACHE_LOCK:
        api_system._HEALTH_CACHE["ts_ms"] = 0
        api_system._HEALTH_CACHE["payload"] = None
    with api_system._HEALTH_CACHE_REFRESH_LOCK:
        api_system._HEALTH_CACHE_REFRESH_IN_FLIGHT = False
    with api_system._SYSTEM_SNAPSHOT_CACHE_LOCK:
        api_system._SYSTEM_SNAPSHOT_CACHE["ts_ms"] = 0
        api_system._SYSTEM_SNAPSHOT_CACHE["payload"] = None

    started = threading.Event()
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def slow_health_snapshot():
        nonlocal calls
        with calls_lock:
            calls += 1
        started.set()
        release.wait(timeout=1.0)
        return {
            "ok": False,
            "reasons": ["warming"],
            "lifecycle": {"state": "WARMING_UP", "detail": "warming"},
            "execution_barrier": {
                "ok": True,
                "allowed": False,
                "mode": "safe",
                "reason": "health_fast_path",
                "real_trading_allowed": False,
            },
        }

    monkeypatch.setattr(api_system, "get_health_snapshot", slow_health_snapshot)
    monkeypatch.setattr(
        api_system,
        "_build_system_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("full system snapshot should not run")),
    )
    monkeypatch.setattr(
        api_system,
        "api_get_runtime_watchdogs",
        lambda *_args, **_kwargs: {"ok": False, "status": "warming"},
    )
    monkeypatch.setattr(
        api_system,
        "execution_gate_snapshot",
        lambda: {
            "ok": True,
            "allowed": False,
            "allow_execution": False,
            "mode": "safe",
            "reason": "health_fast_path",
            "real_trading_allowed": False,
        },
    )
    ctx = {
        "_boot_diagnostics": lambda: {
            "storage": {
                "checked": True,
                "ok": True,
                "status": "ready",
                "detail": "ok",
            }
        }
    }

    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(api_system.api_get_readiness, {}, ctx) for _ in range(16)]
            assert started.wait(timeout=1.0)
            results = [future.result(timeout=1.0) for future in futures]
    finally:
        release.set()

    assert calls == 1
    assert all(result["ok"] is False for result in results)
    assert all(result["execution_allowed"] is False for result in results)


def test_degraded_storage_readiness_returns_prompt_failure(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setattr(
        api_system,
        "_build_readiness_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("degraded storage should short-circuit")),
    )
    monkeypatch.setattr(api_system, "_cached_health_snapshot", lambda *, allow_sync_on_miss=True: {})

    started = time.perf_counter()
    response = api_system.api_get_readiness(
        {},
        {
            "_boot_diagnostics": lambda: {
                "storage": {
                    "checked": True,
                    "ok": False,
                    "status": "unavailable",
                    "detail": "pool_timeout",
                    "error_type": "StoragePoolTimeout",
                }
            }
        },
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0

    assert elapsed_ms < 100
    assert response["ok"] is False
    assert response["failed"] is True
    assert response["execution_allowed"] is False
    assert response["reasons"] == ["storage_unavailable"]


def test_operator_trading_readiness_is_prompt_during_warmup(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *, allow_sync_on_miss=True: {
            "ok": False,
            "reasons": ["warming"],
            "lifecycle": {"state": "WARMING_UP", "detail": "awaiting_first_price_tick"},
            "execution_barrier": {
                "ok": True,
                "allowed": False,
                "mode": "safe",
                "reason": "health_fast_path",
                "real_trading_allowed": False,
            },
        },
    )
    monkeypatch.setattr(
        api_system,
        "api_get_runtime_watchdogs",
        lambda *_args, **_kwargs: {"ok": False, "status": "warming"},
    )
    monkeypatch.setattr(
        api_system,
        "execution_gate_snapshot",
        lambda: {
            "ok": True,
            "allowed": False,
            "allow_execution": False,
            "mode": "safe",
            "reason": "health_fast_path",
            "real_trading_allowed": False,
        },
    )

    started = time.perf_counter()
    response = api_system.api_get_trading_readiness(
        {},
        {
            "_boot_diagnostics": lambda: {
                "storage": {
                    "checked": True,
                    "ok": True,
                    "status": "ready",
                    "detail": "ok",
                }
            }
        },
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0

    assert elapsed_ms < 100
    assert response["ok"] is False
    assert response["ready"] is False
    assert response["execution_allowed"] is False


def test_safe_no_credential_readiness_tolerates_skipped_trading_gates(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("BROKER", "sim")
    monkeypatch.setenv("BROKER_NAME", "sim")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", "0")

    startup_gates = {
        "config_valid": {"ok": True, "detail": "ok"},
        "database_reachable": {"ok": True, "detail": "ok"},
        "schema_valid": {"ok": True, "detail": "ok"},
        "core_services_initialized": {"ok": True, "detail": "ok"},
        "required_api_dependencies_available": {"ok": True, "detail": "ok"},
        "no_port_binding_conflict": {"ok": True, "detail": "ok"},
        "ui_static_assets_present": {"ok": True, "detail": "ok"},
    }
    data_gates = {
        "ingestion_active": {"ok": True, "detail": "ok"},
        "ingestion_not_stale": {"ok": True, "detail": "ok"},
        "critical_features_valid": {"ok": False, "detail": "feature_validation_missing"},
        "model_inputs_valid": {"ok": False, "detail": "model_input_validation_missing"},
        "scoring_pipeline_operational": {"ok": False, "detail": "scoring_pipeline_unreported"},
    }
    snapshot = {
        "ts_ms": 123,
        "status": "RUNNING",
        "state": "LIVE",
        "system_state_detail": {"state": "LIVE", "detail": "market_data_healthy"},
        "health": {
            "ok": True,
            "prices": {"ok": True},
            "providers": {"ok": True},
            "startup_validation": {"ok": True, "gates": startup_gates},
            "data_pipeline_gates": {"ok": False, "gates": data_gates},
            "execution_supervisor": {"gates": {}},
            "execution_barrier": {"allowed": False, "reason": "mode_safe"},
        },
        "graph": {"ok": True},
    }

    validation = api_system._build_production_validation(snapshot)

    assert validation["status"] == "healthy"
    assert validation["safe_to_operate"] is True
    assert validation["gates"]["scoring_pipeline_operational"]["safe_mode_skipped"] is True
    assert validation["gates"]["execution_engine_initialized"]["safe_mode_skipped"] is True


def test_api_readiness_downgrades_safe_lifecycle_live(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setattr(api_system, "_cached_health_snapshot", lambda *, allow_sync_on_miss=True: {})
    monkeypatch.setattr(
        api_system,
        "_build_readiness_snapshot",
        lambda *_args, **_kwargs: {
            "ts_ms": 123456,
            "status": "RUNNING",
            "state": "LIVE",
            "mode": "safe",
            "execution_mode": "safe",
            "execution_allowed": False,
            "reasons": [],
            "readiness": {"ok": True, "ready": True, "issues": [], "steps": []},
            "production_validation": {
                "status": "healthy",
                "safe_to_operate": True,
                "unsafe_to_operate": False,
                "summary_reason": "production_validation_ok",
                "current_degraded_reasons": [],
            },
            "health": {"ok": True, "lifecycle": {"state": "LIVE", "detail": "market_data_healthy"}},
            "graph": {"ok": True},
            "system_state_detail": {"state": "LIVE", "detail": "market_data_healthy"},
        },
    )

    response = api_system.api_get_readiness({}, {})

    assert response["ok"] is False
    assert response["ready"] is False
    assert response["status"] == "DEGRADED"
    assert response["mode"] == "safe"
    assert response["state"] == "DEGRADED"
    assert response["runtime_lifecycle_state"]["state"] == "LIVE"
    assert "mode_safe_not_live" in response["reasons"]


def test_production_validation_requires_live_trading_preflight(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    live_preflight = importlib.reload(importlib.import_module("engine.runtime.live_trading_preflight"))
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setattr(
        live_preflight,
        "live_trading_preflight",
        lambda **_kwargs: {
            "ok": False,
            "required": True,
            "mode": "live",
            "execution_mode": "live",
            "reason": "backup_evidence_json_missing",
            "blockers": ["backup_evidence_json_missing", "position_reconcile_not_exercised"],
            "backup_restore_evidence": {"ok": False, "required": True},
            "position_reconcile_evidence": {"ok": False, "required": True},
        },
    )
    startup_gates = {
        "config_valid": {"ok": True, "detail": "ok"},
        "database_reachable": {"ok": True, "detail": "ok"},
        "schema_valid": {"ok": True, "detail": "ok"},
        "core_services_initialized": {"ok": True, "detail": "ok"},
        "required_api_dependencies_available": {"ok": True, "detail": "ok"},
        "no_port_binding_conflict": {"ok": True, "detail": "ok"},
        "ui_static_assets_present": {"ok": True, "detail": "ok"},
    }
    data_gates = {
        "ingestion_active": {"ok": True, "detail": "ok"},
        "ingestion_not_stale": {"ok": True, "detail": "ok"},
        "critical_features_valid": {"ok": True, "detail": "ok"},
        "model_inputs_valid": {"ok": True, "detail": "ok"},
        "scoring_pipeline_operational": {"ok": True, "detail": "ok"},
    }
    execution_gates = {
        "execution_engine_initialized": {"ok": True, "detail": "ok"},
        "order_state_consistent": {"ok": True, "detail": "ok"},
        "position_state_consistent": {"ok": True, "detail": "ok"},
        "pnl_calculation_valid": {"ok": True, "detail": "ok"},
    }
    snapshot = {
        "ts_ms": 123456,
        "status": "RUNNING",
        "state": "LIVE",
        "mode": "live",
        "execution_mode": "live",
        "system_state_detail": {"state": "LIVE", "detail": "ok"},
        "health": {
            "startup_validation": {"ok": True, "gates": startup_gates},
            "data_pipeline_gates": {"ok": True, "gates": data_gates},
            "execution_supervisor": {"ok": True, "state": "ok", "gates": execution_gates, "alerts": []},
            "execution_barrier": {"mode": "live", "allowed": False, "reason": "readiness_not_ready"},
            "lifecycle": {"state": "LIVE", "detail": "ok"},
        },
        "services": {"engine": {"running": True}},
        "ingestion": {"ok": True},
        "graph": {"ok": True},
        "database_debug": {"failure_classification": {"primary_cause": ""}},
        "readiness": {"ok": False, "ready": False},
    }

    validation = api_system._build_production_validation(
        snapshot,
        ctx=None,
        runtime_watchdogs={"ok": True, "pipeline_watchdog_state": {}},
    )

    assert validation["status"] == "failed"
    assert validation["safe_to_operate"] is False
    assert validation["gates"]["live_trading_preflight"]["status"] == "failed"
    assert "backup_evidence_json_missing" in validation["gates"]["live_trading_preflight"]["blockers"]
    assert "position_reconcile_not_exercised" in validation["live_trading_preflight"]["blockers"]


def test_trading_readiness_stays_blocked_when_service_readiness_is_ok(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setattr(
        api_system,
        "api_get_readiness",
        lambda *_args, **_kwargs: {
            "ok": True,
            "ready": True,
            "status": "HEALTHY",
            "reasons": [],
            "execution_allowed": False,
        },
    )
    monkeypatch.setattr(
        api_system,
        "execution_gate_snapshot",
        lambda: {
            "ok": True,
            "allowed": False,
            "allow_execution": False,
            "real_trading_allowed": False,
            "reason": "mode_safe",
        },
    )

    response = api_system.api_get_trading_readiness({}, {})

    assert response["ok"] is False
    assert response["ready"] is False
    assert response["trading_ready"] is False
    assert response["real_trading_allowed"] is False
    assert response["reason"] == "mode_safe"


def test_storage_connection_close_closes_tracked_cursors(monkeypatch):
    storage_pg = importlib.import_module("engine.runtime.storage_pg")
    released = []

    class _Info:
        transaction_status = storage_pg.TransactionStatus.IDLE

    class _Cursor:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    class _Raw:
        info = _Info()

        def __init__(self):
            self.cursor_obj = _Cursor()

        def cursor(self):
            return self.cursor_obj

        def rollback(self):
            raise AssertionError("idle connection should not rollback")

    raw = _Raw()
    monkeypatch.setattr(storage_pg, "release", lambda value: released.append(value))

    con = storage_pg.StorageConnection(raw, readonly=True)
    cursor = con.cursor()

    con.close()

    assert raw.cursor_obj.closed is True
    assert released == [raw]
    cursor.close()
