from __future__ import annotations

import base64
import importlib
import json
import uuid


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _isolated_data_source_modules(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "futures_data_sources.sqlite"))
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
    storage, = _reload_modules("engine.runtime.storage")
    storage.init_db()
    data_source_manager, routes = _reload_modules(
        "services.data_source_manager",
        "routes.data_sources_routes",
    )
    return storage, data_source_manager, routes


def test_futures_data_catalog_and_registry_entries() -> None:
    import services.data_source_manager as data_source_manager

    mod = importlib.reload(data_source_manager)
    definition = mod._default_catalog()["futures_data"]

    assert definition.source_type == "price_provider"
    assert definition.provider_name == "futures"
    assert definition.default_enabled is False
    assert definition.safe_to_auto_enable is False
    assert definition.credential_env["api_key"] == "DATABENTO_API_KEY"
    assert "futures_contract_bars" in definition.storage_tables
    assert mod._PROVIDER_TEST_REGISTRY["futures_data"]["handler"] == "_test_futures_connection"
    account = mod._provider_account_catalog()["futures"]
    assert account.credential_env["api_key"] == "DATABENTO_API_KEY"


def test_futures_connection_missing_token_fails_closed(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    source = manager.get_source("futures_data", include_credentials=True)

    result = manager._test_futures_connection(source)

    assert result.status == "fail"
    assert result.classification == "missing_credentials"
    payload = result.payload(source_key="futures_data")
    assert "DATABENTO_API_KEY" in payload["evidence"]["missing_env_vars"]


def test_futures_connection_success_probe_does_not_leak_token(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = f"databento-canary-{uuid.uuid4().hex}"
    manager.update_source(
        {
            "source_key": "futures_data",
            "credentials": {"api_key": canary},
            "settings": {"provider": "databento", "roots": "ES,NQ"},
            "replace_credentials": True,
            "replace_settings": True,
        }
    )
    source = manager.get_source("futures_data", include_credentials=True)

    def _fake_probe(_source, **kwargs):
        assert kwargs["provider"] == "futures_data"
        assert kwargs["headers"]["Authorization"] == f"Bearer {canary}"
        assert "params" not in kwargs or canary not in json.dumps(kwargs["params"], sort_keys=True, default=str)
        return manager._connection_pass(
            "futures_data_connection_ok",
            provider="futures_data",
            endpoint="https://hist.databento.com/v0/metadata.list_publishers",
            payload_count=1,
        )

    monkeypatch.setattr(manager, "_http_json_probe", _fake_probe)

    result = manager._test_futures_connection(source)
    rendered = json.dumps(result.payload(source_key="futures_data"), sort_keys=True, default=str)

    assert result.status == "pass"
    assert canary not in rendered


def test_futures_enable_flows_to_registry_and_routes_without_secret_leak(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = f"databento-canary-{uuid.uuid4().hex}"
    manager.update_provider_account(
        {
            "account_key": "futures",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
        }
    )
    manager.update_source(
        {
            "source_key": "futures_data",
            "enabled": True,
            "settings": {"provider": "databento", "roots": "ES,NQ,CL"},
            "replace_settings": True,
        }
    )

    overrides = manager.inject_into_provider_registry()
    desired_jobs = manager.get_desired_ingestion_jobs()
    env = manager.build_job_environment("poll_prices")
    payload = routes.api_get_data_sources(None)
    rendered = json.dumps(payload, sort_keys=True, default=str)

    assert overrides["futures"]["enabled"] is True
    assert overrides["futures"]["source_key"] == "futures_data"
    assert "poll_prices" in desired_jobs
    assert env["FUTURES_ENABLED"] == "1"
    assert "futures_data" in {item["template_key"] for item in payload["templates"]}
    assert "futures_data" in {item["source_key"] for item in payload["sources"]}
    assert canary not in rendered
