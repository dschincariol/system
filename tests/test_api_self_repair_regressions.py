import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _FakeSupervisor:
    def deterministic_start(self, *_args, **_kwargs):
        return {"ok": False, "error": "supervisor_unavailable"}


class _RecordingSupervisor:
    def __init__(self) -> None:
        self.started: list[str] = []

    def deterministic_start(self, targets, *_args, **_kwargs):
        names = [str(name) for name in (targets or [])]
        self.started.extend(names)
        return {"ok": True, "started": names}


class _EmptyRowsConnection:
    def execute(self, *_args, **_kwargs):
        return self

    def fetchall(self):
        return []

    def close(self):
        return None


def _patch_self_repair_lightweight(api_self_repair, monkeypatch):
    first_run = importlib.import_module("engine.runtime.first_run")
    repair_schema_module = importlib.import_module("engine.runtime.jobs.repair_schema")
    monkeypatch.setattr(api_self_repair, "run_preflight", lambda: {"ok": True})
    monkeypatch.setattr(api_self_repair, "api_get_runtime_health", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(api_self_repair, "api_get_trading_readiness", lambda *_args, **_kwargs: {"ready": True})
    monkeypatch.setattr(api_self_repair, "_db_connect", lambda readonly=True: _EmptyRowsConnection())
    monkeypatch.setattr(api_self_repair, "run_write_txn", lambda fn, **_kwargs: fn(_EmptyRowsConnection()))
    monkeypatch.setattr(repair_schema_module, "run", lambda: {"ok": True})
    monkeypatch.setattr(first_run, "bootstrap_first_run", lambda mode="paper": {"ok": True, "mode": mode})


def test_api_post_self_repair_uses_runtime_mode_without_snapshot(tmp_path, monkeypatch):
    db_path = tmp_path / "self_repair.db"

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")

    api_self_repair = importlib.reload(importlib.import_module("engine.api.api_self_repair"))
    _patch_self_repair_lightweight(api_self_repair, monkeypatch)

    def _unexpected_snapshot(*_args, **_kwargs):
        raise AssertionError("_build_system_snapshot should not be used by api_post_self_repair")

    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setattr(api_system, "_build_system_snapshot", _unexpected_snapshot)

    assert api_system.api_post_self_repair is api_self_repair.api_post_self_repair

    result = api_self_repair.api_post_self_repair(None, None, {"SUPERVISOR": _FakeSupervisor()})

    assert result["ok"] is True
    assert result["mode"] == "safe"
    assert result["duration_ms"] >= 0
    assert isinstance(result["steps"], list)
    assert result["steps"]


def test_self_repair_skips_provider_daemons_under_isolated_ingestion(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("START_INGESTION_WITH_SERVER", "1")
    monkeypatch.setenv("YFINANCE_ENABLED", "1")
    monkeypatch.setenv("POLYGON_WS_ENABLED", "0")
    monkeypatch.setenv("IBKR_ENABLED", "0")

    api_self_repair = importlib.reload(importlib.import_module("engine.api.api_self_repair"))
    _patch_self_repair_lightweight(api_self_repair, monkeypatch)

    supervisor = _RecordingSupervisor()
    result = api_self_repair.api_post_self_repair(None, None, {"SUPERVISOR": supervisor})

    assert result["ok"] is True
    assert "ingestion_runtime" not in supervisor.started
    assert "stream_prices_polygon_ws" not in supervisor.started
    assert "poll_prices" not in supervisor.started
    assert "provider_monitor" in supervisor.started
    assert "metrics_collector" in supervisor.started
    daemon_step = next(step for step in result["steps"] if step["step"] == "restart_runtime_daemons")
    daemon_results = daemon_step["detail"]["results"]
    assert daemon_results["ingestion_runtime"]["skipped"] is True
    assert daemon_results["stream_prices_polygon_ws"]["skipped"] is True
    assert daemon_results["poll_prices"]["skipped"] is True


def test_self_repair_does_not_start_disabled_paid_provider_daemons(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("START_INGESTION_WITH_SERVER", "0")
    monkeypatch.setenv("YFINANCE_ENABLED", "1")
    monkeypatch.setenv("POLYGON_WS_ENABLED", "0")
    monkeypatch.setenv("IBKR_ENABLED", "0")
    monkeypatch.setenv("POLYGON_API_KEY", "unit-test-placeholder")

    api_self_repair = importlib.reload(importlib.import_module("engine.api.api_self_repair"))
    _patch_self_repair_lightweight(api_self_repair, monkeypatch)

    supervisor = _RecordingSupervisor()
    result = api_self_repair.api_post_self_repair(None, None, {"SUPERVISOR": supervisor})

    assert result["ok"] is True
    assert "stream_prices_polygon_ws" not in supervisor.started
    assert "poll_prices" in supervisor.started
    daemon_step = next(step for step in result["steps"] if step["step"] == "restart_runtime_daemons")
    assert daemon_step["detail"]["results"]["stream_prices_polygon_ws"]["skipped"] is True
