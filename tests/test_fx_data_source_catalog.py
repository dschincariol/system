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
    monkeypatch.setenv("DB_PATH", str(tmp_path / "fx_data_sources.sqlite"))
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


def test_oanda_fx_catalog_and_registry_entries() -> None:
    import services.data_source_manager as data_source_manager

    mod = importlib.reload(data_source_manager)
    definition = mod._default_catalog()["oanda_fx"]

    assert definition.source_type == "price_provider"
    assert definition.provider_name == "oanda"
    assert definition.default_enabled is False
    assert definition.safe_to_auto_enable is False
    assert mod._PROVIDER_TEST_REGISTRY["oanda_fx"]["handler"] == "_test_oanda_connection"
    account = mod._provider_account_catalog()["oanda"]
    assert account.credential_env["access_token"] == "OANDA_ACCESS_TOKEN"
    assert account.credential_env["api_key"] == "OANDA_API_KEY"


def test_oanda_connection_missing_token_fails_closed(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    source = manager.get_source("oanda_fx", include_credentials=True)

    result = manager._test_oanda_connection(source)

    assert result.status == "fail"
    assert result.classification == "missing_credentials"
    payload = result.payload(source_key="oanda_fx")
    assert "OANDA_ACCESS_TOKEN" in payload["evidence"]["missing_env_vars"]


def test_oanda_connection_success_probe_does_not_leak_token(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = f"oanda-canary-{uuid.uuid4().hex}"
    manager.update_source(
        {
            "source_key": "oanda_fx",
            "credentials": {"access_token": canary},
            "settings": {"account_id": "101-001-00000000-001", "environment": "practice"},
            "replace_credentials": True,
            "replace_settings": True,
        }
    )
    source = manager.get_source("oanda_fx", include_credentials=True)

    def _fake_probe(_source, **kwargs):
        assert kwargs["provider"] == "oanda_fx"
        assert kwargs["headers"]["Authorization"] == f"Bearer {canary}"
        assert canary not in json.dumps(kwargs["params"], sort_keys=True, default=str)
        return manager._connection_pass(
            "oanda_fx_connection_ok",
            provider="oanda_fx",
            endpoint="https://api-fxpractice.oanda.com/v3/accounts/101-001-00000000-001/pricing",
            payload_count=1,
        )

    monkeypatch.setattr(manager, "_http_json_probe", _fake_probe)

    result = manager._test_oanda_connection(source)
    rendered = json.dumps(result.payload(source_key="oanda_fx"), sort_keys=True, default=str)

    assert result.status == "pass"
    assert canary not in rendered


def test_oanda_enable_flows_to_registry_and_routes_without_secret_leak(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = f"oanda-canary-{uuid.uuid4().hex}"
    manager.update_provider_account(
        {
            "account_key": "oanda",
            "credentials": {"access_token": canary},
            "replace_credentials": True,
        }
    )
    manager.update_source(
        {
            "source_key": "oanda_fx",
            "enabled": True,
            "settings": {"account_id": "101-001-00000000-001", "environment": "practice"},
            "replace_settings": True,
        }
    )

    overrides = manager.inject_into_provider_registry()
    desired_jobs = manager.get_desired_ingestion_jobs()
    payload = routes.api_get_data_sources(None)
    rendered = json.dumps(payload, sort_keys=True, default=str)

    assert overrides["oanda"]["enabled"] is True
    assert overrides["oanda"]["source_key"] == "oanda_fx"
    assert "poll_prices" in desired_jobs
    assert "oanda_fx" in {item["template_key"] for item in payload["templates"]}
    assert "oanda_fx" in {item["source_key"] for item in payload["sources"]}
    assert canary not in rendered
