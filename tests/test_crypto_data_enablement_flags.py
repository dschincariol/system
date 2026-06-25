from __future__ import annotations

import base64
import importlib
import sys
import types
from pathlib import Path


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")
PROFILE_PATH = Path(__file__).resolve().parents[1] / "deploy" / "profiles" / "crypto_sim.env.example"


def _apply_profile(monkeypatch) -> None:
    for line in PROFILE_PATH.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, _, value = text.partition("=")
        monkeypatch.setenv(key.strip(), value.strip())


def _reload_provider_registry(monkeypatch):
    fake_manager = types.SimpleNamespace(
        inject_provider_registry=lambda: {},
        desired_ingestion_jobs=lambda read_only=True: [],
    )
    import services

    monkeypatch.setitem(sys.modules, "services.data_source_manager", fake_manager)
    monkeypatch.setattr(services, "data_source_manager", fake_manager, raising=False)
    import engine.data.provider_registry as provider_registry

    return importlib.reload(provider_registry)


def _isolated_manager(monkeypatch, tmp_path):
    monkeypatch.delitem(sys.modules, "services.data_source_manager", raising=False)
    services_pkg = sys.modules.get("services")
    if services_pkg is not None:
        monkeypatch.delattr(services_pkg, "data_source_manager", raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "crypto_data_sources.sqlite"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", "1")
    monkeypatch.setenv("ENGINE_SUPERVISED", "0")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    storage.init_db()
    data_source_manager = importlib.reload(importlib.import_module("services.data_source_manager"))
    return data_source_manager.get_manager()


def test_ccxt_provider_honors_crypto_profile_flag(monkeypatch) -> None:
    _apply_profile(monkeypatch)
    provider_registry = _reload_provider_registry(monkeypatch)

    definition = provider_registry.get_provider_definition("ccxt")
    assert definition is not None
    assert definition.enabled is True
    assert definition.supports["asset_classes"] == ["crypto"]
    assert "ccxt" in provider_registry.get_polling_provider_names()

    monkeypatch.setenv("CCXT_ENABLED", "0")
    provider_registry = _reload_provider_registry(monkeypatch)

    definition = provider_registry.get_provider_definition("ccxt")
    assert definition is not None
    assert definition.enabled is False
    assert "ccxt" not in provider_registry.get_polling_provider_names()


def test_crypto_funding_source_projects_enabled_job_only_when_source_enabled(monkeypatch, tmp_path) -> None:
    _apply_profile(monkeypatch)
    manager = _isolated_manager(monkeypatch, tmp_path)

    disabled_jobs = manager.get_desired_ingestion_jobs(project_credentials=False)
    disabled_env = manager.build_job_environment("ingest_crypto_funding")
    assert "ingest_crypto_funding" not in disabled_jobs
    assert disabled_env["INGEST_CRYPTO_FUNDING_ENABLED"] == "0"

    manager.update_source(
        {
            "source_key": "crypto_funding",
            "enabled": True,
            "settings": {"funding_exchange_id": "binanceusdm", "perp_markets": ""},
            "replace_settings": True,
        }
    )

    enabled_jobs = manager.get_desired_ingestion_jobs(project_credentials=False)
    enabled_env = manager.build_job_environment("ingest_crypto_funding")
    assert "ingest_crypto_funding" in enabled_jobs
    assert enabled_env["INGEST_CRYPTO_FUNDING_ENABLED"] == "1"
    assert enabled_env["CCXT_FUNDING_EXCHANGE_ID"] == "binanceusdm"

    manager.update_source({"source_key": "crypto_funding", "enabled": False})

    re_disabled_jobs = manager.get_desired_ingestion_jobs(project_credentials=False)
    re_disabled_env = manager.build_job_environment("ingest_crypto_funding")
    assert "ingest_crypto_funding" not in re_disabled_jobs
    assert re_disabled_env["INGEST_CRYPTO_FUNDING_ENABLED"] == "0"
