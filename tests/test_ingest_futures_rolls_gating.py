from __future__ import annotations

import base64
import importlib
import inspect

import pytest


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


class _FakeManager:
    def __init__(self, enabled: bool = False) -> None:
        self.enabled = bool(enabled)
        self.statuses: list[dict] = []

    def record_job_status(self, job_name: str, *, ok: bool, message: str = "", error: str = "", meta=None) -> None:
        self.statuses.append(
            {
                "job_name": job_name,
                "ok": bool(ok),
                "message": str(message or ""),
                "error": str(error or ""),
                "meta": dict(meta or {}),
            }
        )

    def is_job_enabled(self, job_name: str, *, default: bool = True) -> bool:
        del job_name, default
        return bool(self.enabled)


def _reload_module(name: str):
    module = importlib.import_module(name)
    return importlib.reload(module)


def _isolated_data_source_modules(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "futures_rolls_data_sources.sqlite"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", "1")
    monkeypatch.setenv("ENGINE_SUPERVISED", "0")
    monkeypatch.setenv("DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_S", "0")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")
    monkeypatch.setenv("TELEMETRY_READ_BACKEND", "sqlite")
    monkeypatch.setenv("TELEMETRY_READ_REQUIRE_VALIDATION", "0")
    storage = _reload_module("engine.runtime.storage")
    storage.init_db()
    data_source_manager = _reload_module("services.data_source_manager")
    return storage, data_source_manager


def test_main_requires_supervisor(monkeypatch) -> None:
    monkeypatch.delenv("ENGINE_SUPERVISED", raising=False)
    monkeypatch.delenv("INGEST_FUTURES_ROLLS_ENABLED", raising=False)
    mod = _reload_module("engine.data.jobs.ingest_futures_rolls")

    with pytest.raises(SystemExit) as exc:
        mod.main()

    assert exc.value.code == 1


def test_main_disabled_by_env_records_status_without_write(monkeypatch) -> None:
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("INGEST_FUTURES_ROLLS_ENABLED", "0")
    mod = _reload_module("engine.data.jobs.ingest_futures_rolls")
    fake = _FakeManager(enabled=True)
    monkeypatch.setattr(mod, "get_manager", lambda: fake)

    mod.main()

    assert fake.statuses
    assert fake.statuses[-1]["job_name"] == "ingest_futures_rolls"
    assert fake.statuses[-1]["ok"] is True
    assert fake.statuses[-1]["message"] == "futures roll ingestion disabled by env flag"


def test_control_plane_gates_futures_roll_job(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    assert manager.build_job_environment("ingest_futures_rolls")["INGEST_FUTURES_ROLLS_ENABLED"] == "0"
    assert "ingest_futures_rolls" not in set(manager.get_desired_ingestion_jobs())
    assert manager.is_job_enabled("ingest_futures_rolls", default=False) is False

    manager.update_source({"source_key": "futures_rolls", "enabled": True, "actor": "unit-test"})

    assert manager.build_job_environment("ingest_futures_rolls")["INGEST_FUTURES_ROLLS_ENABLED"] == "1"
    assert "ingest_futures_rolls" in set(manager.get_desired_ingestion_jobs())
    assert manager.is_job_enabled("ingest_futures_rolls", default=False) is True


def test_ingest_batch_noops_without_raw_futures_table(monkeypatch, tmp_path) -> None:
    storage, _data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    futures_roll = _reload_module("engine.data.futures_roll")

    summary = futures_roll.ingest_futures_rolls_batch(now_ms=1_800_000_000_000)

    assert summary["ok"] is True
    assert summary["raw_rows"] == 0
    assert summary["written"] == 0
    con = storage.connect()
    try:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'futures_%' ORDER BY name"
            )
        }
    finally:
        con.close()
    assert "futures_roll_calendar" in tables
    assert "futures_continuous_bars" in tables
    assert "futures_roll_yield" in tables


def test_ingest_futures_rolls_imports_no_broker_order_path() -> None:
    mod = _reload_module("engine.data.jobs.ingest_futures_rolls")
    source = inspect.getsource(mod)

    assert "broker_ibkr_gateway" not in source
    assert "broker_router" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
