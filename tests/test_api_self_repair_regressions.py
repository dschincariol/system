import importlib
import sqlite3
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _FakeSupervisor:
    def deterministic_start(self, *_args, **_kwargs):
        return {"ok": False, "error": "supervisor_unavailable"}


def test_api_post_self_repair_uses_runtime_mode_without_snapshot(tmp_path, monkeypatch):
    db_path = tmp_path / "self_repair.db"

    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")

    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    storage.init_db()

    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    first_run = importlib.reload(importlib.import_module("engine.runtime.first_run"))
    repair_schema_module = importlib.reload(importlib.import_module("engine.runtime.jobs.repair_schema"))

    def _unexpected_snapshot(*_args, **_kwargs):
        raise AssertionError("_build_system_snapshot should not be used by api_post_self_repair")

    monkeypatch.setattr(api_system, "_build_system_snapshot", _unexpected_snapshot)
    monkeypatch.setattr(api_system, "run_preflight", lambda: {"ok": True})
    monkeypatch.setattr(repair_schema_module, "run", lambda: {"ok": True})
    monkeypatch.setattr(first_run, "bootstrap_first_run", lambda mode="paper": {"ok": True, "mode": mode})
    monkeypatch.setattr(api_system, "api_get_runtime_health", lambda *_args, **_kwargs: {"ok": True})
    monkeypatch.setattr(api_system, "api_get_trading_readiness", lambda *_args, **_kwargs: {"ready": True})

    result = api_system.api_post_self_repair(None, None, {"SUPERVISOR": _FakeSupervisor()})

    assert result["ok"] is True
    assert result["mode"] == "safe"
    assert result["duration_ms"] >= 0
    assert isinstance(result["steps"], list)
    assert result["steps"]

    with sqlite3.connect(str(db_path)) as con:
        assert con.execute(
            "select name from sqlite_master where type='table' and name='runtime_meta'"
        ).fetchone()
