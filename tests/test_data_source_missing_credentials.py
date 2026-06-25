from __future__ import annotations

import base64
import importlib
import json

import pytest


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


@pytest.fixture()
def data_source_context(monkeypatch, tmp_path):
    for key in (
        "ALPACA_KEY_ID",
        "ALPACA_KEY_ID_FILE",
        "ALPACA_SECRET_KEY",
        "ALPACA_SECRET_KEY_FILE",
        "OPENAI_API_KEY",
        "OPENAI_API_KEY_FILE",
        "POLYGON_API_KEY",
        "POLYGON_API_KEY_FILE",
        "POLYGON_KEY",
        "POLYGON_KEY_FILE",
        "TRADIER_API_TOKEN",
        "TRADIER_API_TOKEN_FILE",
        "TS_SECRETS_PROVIDER",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "missing_credentials.sqlite"))
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
    data_source_manager, routes, creds, secret_sources = _reload_modules(
        "services.data_source_manager",
        "routes.data_sources_routes",
        "engine.data._credentials",
        "engine.runtime.secret_sources",
    )
    return data_source_manager.get_manager(), routes, creds, secret_sources


def _source(payload: dict, source_key: str) -> dict:
    for row in payload.get("sources") or []:
        if str(row.get("source_key") or "") == source_key:
            return dict(row)
    raise AssertionError(f"missing source {source_key}")


def test_empty_secret_file_surfaces_needs_credentials_without_live_probe(
    data_source_context,
    monkeypatch,
    tmp_path,
) -> None:
    manager, routes, creds, secret_sources = data_source_context
    secret_file = tmp_path / "secrets" / "polygon_api_key"
    secret_file.parent.mkdir(parents=True, exist_ok=True)
    secret_file.write_text("", encoding="utf-8")
    secret_file.chmod(0o600)
    monkeypatch.setenv("POLYGON_API_KEY_FILE", str(secret_file))
    monkeypatch.setenv("POLYGON_KEY_FILE", str(secret_file))

    calls: list[str] = []

    def _live_call_forbidden(*_args, **_kwargs):
        calls.append("requests.get")
        raise AssertionError("missing credentials must not make a provider call")

    monkeypatch.setattr(importlib.import_module("services.data_source_manager").requests, "get", _live_call_forbidden)

    read_result = secret_sources.read_secret_text_file(secret_file)
    creds.clear_data_credential_cache()
    test_result = manager.test_connection("polygon", actor="unit-test")
    payload = routes.api_get_data_sources(None)
    polygon = _source(payload, "polygon")

    assert read_result.ok is False
    assert read_result.empty is True
    assert read_result.reason == "empty"
    assert read_result.value == ""
    assert "value" not in repr(read_result)
    assert creds.get_data_credential("POLYGON_API_KEY", ttl_s=0) == ""
    assert test_result["ok"] is False
    assert test_result["classification"] == "missing_credentials"
    assert calls == []
    assert polygon["status"] == "needs_credentials"
    assert polygon["stored_status"] in {"configured", "test_failed"}
    assert polygon["credential_status"] == "needs_credentials"
    assert polygon["needs_credentials"] is True
    assert polygon["missing_credential_env_vars"] == ["POLYGON_API_KEY"]
    assert polygon["runtime_desired_eligible"] is False
    assert "POLYGON_API_KEY=" not in json.dumps(payload, sort_keys=True, default=str)


def test_missing_secret_file_is_structured_and_rss_remains_enabled(
    data_source_context,
    monkeypatch,
    tmp_path,
) -> None:
    manager, routes, creds, secret_sources = data_source_context
    missing_path = tmp_path / "secrets" / "polygon_api_key"
    monkeypatch.setenv("POLYGON_API_KEY_FILE", str(missing_path))
    monkeypatch.setenv("POLYGON_KEY_FILE", str(missing_path))

    read_result = secret_sources.read_secret_text_file(missing_path)
    creds.clear_data_credential_cache()
    rss = manager.create_source(
        {
            "source_key": "rss:credentialless_unit_feed",
            "display_name": "Credentialless Unit Feed",
            "source_type": "rss_feed",
            "enabled": True,
            "settings": {
                "name": "Credentialless Unit Feed",
                "url": "https://example.com/feed.xml",
            },
            "actor": "unit-test",
        }
    )
    payload = routes.api_get_data_sources(None)
    polygon = _source(payload, "polygon")
    rss_source = _source(payload, str(rss["source_key"]))

    assert read_result.ok is False
    assert read_result.missing is True
    assert read_result.reason == "missing"
    assert creds.get_data_credential("POLYGON_API_KEY", ttl_s=0) == ""
    assert polygon["status"] == "needs_credentials"
    assert polygon["effective_status"] == "needs_credentials"
    assert rss_source["enabled"] is True
    assert rss_source["status"] != "needs_credentials"
    assert rss_source["credential_status"] == "not_required"
