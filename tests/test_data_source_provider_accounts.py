from __future__ import annotations

import base64
import importlib
import json
import uuid
from pathlib import Path

import pytest


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")
pytestmark = pytest.mark.safety_critical


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _isolated_data_source_modules(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "provider_accounts.sqlite"))
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


def _canary(prefix: str) -> str:
    return f"codex-canary-{prefix}-{uuid.uuid4().hex}"


class _JsonResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None, headers: dict[str, str] | None = None) -> None:
        self.status_code = int(status_code)
        self.headers: dict[str, str] = dict(headers or {})
        self._payload = payload if payload is not None else {"results": [{"ticker": "SPY"}]}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_status:{self.status_code}")

    def json(self) -> dict:
        return dict(self._payload)


def _mock_polygon_get(data_source_manager, monkeypatch, expected_key: str, calls: list[str] | None = None) -> None:
    def _fake_get(_url, **kwargs):
        if calls is not None:
            calls.append("request")
        assert kwargs["params"]["apiKey"] == expected_key
        return _JsonResponse()

    monkeypatch.setattr(data_source_manager.requests, "get", _fake_get)


def test_polygon_account_enables_polygon_dependent_feeds(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("polygon-account")

    manager.update_provider_account(
        {
            "account_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    manager.update_source({"source_key": "etf_flows", "enabled": True, "actor": "unit-test"})
    manager.update_source({"source_key": "inst_13f", "enabled": True, "actor": "unit-test"})

    payload = routes.api_get_data_sources(None)
    rendered = json.dumps(payload, sort_keys=True, default=str)
    assert canary not in rendered

    for source_key in ("polygon", "polygon_ws", "polygon_options", "etf_flows", "inst_13f"):
        source = manager.get_source(source_key)
        resolution = [
            item
            for item in (source or {}).get("credential_resolution", [])
            if item.get("env_var") == "POLYGON_API_KEY"
        ]
        assert resolution
        assert resolution[0]["mode"] == "inherited"
        assert resolution[0]["account_key"] == "polygon"

    assert manager.build_job_environment("poll_prices")["POLYGON_API_KEY"] == canary
    assert manager.build_job_environment("stream_prices_polygon_ws")["POLYGON_API_KEY"] == canary
    assert manager.build_job_environment("options_poll")["POLYGON_API_KEY"] == canary
    assert manager.build_job_environment("ingest_etf_flows")["POLYGON_API_KEY"] == canary
    assert manager.build_job_environment("ingest_13f")["POLYGON_API_KEY"] == canary


def test_source_override_wins_over_provider_account(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    account_canary = _canary("polygon-account")
    override_canary = _canary("polygon-source-override")

    manager.update_provider_account(
        {
            "account_key": "polygon",
            "credentials": {"api_key": account_canary},
            "replace_credentials": True,
        }
    )
    manager.update_source(
        {
            "source_key": "polygon",
            "credentials": {"api_key": override_canary},
            "replace_credentials": True,
        }
    )

    polygon = manager.get_source("polygon")
    resolution = next(
        item for item in (polygon or {}).get("credential_resolution", []) if item["env_var"] == "POLYGON_API_KEY"
    )
    assert resolution["mode"] == "overridden"
    assert manager.build_job_environment("poll_prices")["POLYGON_API_KEY"] == override_canary
    assert manager.build_job_environment("stream_prices_polygon_ws")["POLYGON_API_KEY"] == account_canary


def test_masked_account_values_are_not_persisted_when_resubmitted(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("fmp-account")

    manager.update_provider_account(
        {
            "account_key": "fmp",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
        }
    )
    payload = routes.api_get_data_sources(None)
    rendered = json.dumps(payload, sort_keys=True, default=str)
    assert canary not in rendered
    account = next(item for item in payload["provider_accounts"] if item["account_key"] == "fmp")
    masked = account["masked_credentials"]["api_key"]
    assert masked and masked != canary

    manager.update_provider_account(
        {
            "account_key": "fmp",
            "credentials": {"api_key": masked},
            "replace_credentials": True,
        }
    )

    env = manager.build_job_environment("poll_earnings")
    assert env["FMP_API_KEY"] == canary
    assert env["FMP_API_KEY"] != masked


def test_clearing_account_withdraws_inherited_runtime_credentials(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("polygon-clear")

    manager.update_provider_account(
        {
            "account_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
        }
    )
    assert manager.build_job_environment("stream_prices_polygon_ws")["POLYGON_API_KEY"] == canary

    manager.update_provider_account(
        {
            "account_key": "polygon",
            "clear_credential_fields": ["api_key"],
        }
    )
    env = manager.build_job_environment("stream_prices_polygon_ws")
    assert "POLYGON_API_KEY" not in env
    assert "POLYGON_API_KEY_FILE" not in env
    assert env["POLYGON_WS_ENABLED"] == "0"


def test_credential_cache_is_cleared_before_source_retest(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("polygon-retest")
    events: list[str] = []

    manager.update_provider_account(
        {
            "account_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
        }
    )

    import engine.data._credentials as credential_cache

    monkeypatch.setattr(credential_cache, "clear_data_credential_cache", lambda: events.append("clear"))

    def _fake_get(_url, **kwargs):
        events.append("request")
        assert kwargs["params"]["apiKey"] == canary
        return _JsonResponse()

    monkeypatch.setattr(data_source_manager.requests, "get", _fake_get)

    result = manager.test_connection("polygon", actor="unit-test")

    assert result["ok"] is True
    assert "clear" in events[: events.index("request")]


def test_test_save_uses_ui_stored_credentials_without_response_leak(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    canary = _canary("test-save-source")
    _mock_polygon_get(data_source_manager, monkeypatch, canary)

    result = routes.api_post_data_source_test_save(
        None,
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        },
    )

    rendered = json.dumps(result, sort_keys=True, default=str)
    assert result["ok"] is True
    assert result["saved"] is True
    assert result["test"]["ok"] is True
    assert canary not in rendered


def test_connection_test_uses_env_only_credentials(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("env-only")
    monkeypatch.setenv("POLYGON_API_KEY", canary)
    _mock_polygon_get(data_source_manager, monkeypatch, canary)

    result = manager.test_connection("polygon", actor="unit-test")

    assert result["ok"] is True
    assert canary not in json.dumps(result, sort_keys=True, default=str)


def test_missing_credentials_and_empty_payload_are_not_fake_green(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    missing = manager.test_connection("polygon", actor="unit-test")
    source = manager.get_source("polygon")
    assert missing["ok"] is False
    assert missing["status"] == "fail"
    assert missing["classification"] == "missing_credentials"
    assert source["status"] == "test_failed"

    canary = _canary("empty-payload")
    manager.update_source(
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    def _fake_get(_url, **kwargs):
        if kwargs["params"].get("apiKey") != canary:
            raise AssertionError("polygon_key_mismatch")
        return _JsonResponse(payload={"results": []})

    monkeypatch.setattr(data_source_manager.requests, "get", _fake_get)
    empty = manager.test_connection("polygon", actor="unit-test")
    rendered = json.dumps(empty, sort_keys=True, default=str)
    assert empty["ok"] is False
    assert empty["status"] == "fail"
    assert empty["classification"] == "empty_payload"
    assert canary not in rendered


def test_data_source_test_missing_credentials_returns_structured_422(monkeypatch, tmp_path) -> None:
    _storage, _data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)

    result = routes.api_post_data_source_test(
        None,
        {"source_key": "polygon", "actor": "unit-test", "client_ip": "127.0.0.1"},
        {},
    )
    rendered = json.dumps(result, sort_keys=True, default=str)

    assert result["ok"] is False
    assert result["classification"] == "missing_credentials"
    assert result["reason_code"] == "missing_credentials"
    assert result["provider_reason_code"] == "polygon_credentials_missing"
    assert result["http_status"] == 422
    assert result["meta"]["status"] == 422
    assert result["message"] == "polygon_credentials_missing"
    assert "POLYGON_API_KEY=" not in rendered


def test_rate_limited_connection_is_degraded_and_not_counted_as_success(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("rate-limit")
    manager.update_source(
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    def _fake_get(_url, **kwargs):
        if kwargs["params"].get("apiKey") != canary:
            raise AssertionError("polygon_key_mismatch")
        return _JsonResponse(status_code=429, payload={"results": [{"ticker": "SPY"}]}, headers={"Retry-After": "17"})

    monkeypatch.setattr(data_source_manager.requests, "get", _fake_get)
    result = manager.test_connection("polygon", actor="unit-test")
    source = manager.get_source("polygon")
    rendered = json.dumps({"result": result, "logs": manager.list_logs("polygon", limit=20)}, sort_keys=True, default=str)
    assert result["ok"] is False
    assert result["status"] == "degraded"
    assert result["classification"] == "rate_limited"
    assert result["evidence"]["retry_after_s"] == 17.0
    assert source["status"] == "test_degraded"
    assert source["last_success_ts_ms"] == 0
    assert canary not in rendered


def test_unsupported_registered_source_is_not_success(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    result = manager.test_connection("model_feature_snapshots", actor="unit-test")
    source = manager.get_source("model_feature_snapshots")

    assert result["ok"] is False
    assert result["status"] == "unsupported"
    assert result["classification"] == "unsupported"
    assert source["status"] == "test_unsupported"
    assert source["last_success_ts_ms"] == 0


def test_connection_test_uses_file_credentials(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("file")
    secret_file = tmp_path / "secrets" / "polygon_api_key"
    secret_file.parent.mkdir(parents=True, exist_ok=True)
    secret_file.write_text(canary, encoding="utf-8")
    secret_file.chmod(0o600)
    monkeypatch.setenv("POLYGON_API_KEY_FILE", str(secret_file))
    _mock_polygon_get(data_source_manager, monkeypatch, canary)

    result = manager.test_connection("polygon", actor="unit-test")

    assert result["ok"] is True
    assert canary not in json.dumps(result, sort_keys=True, default=str)


def test_stored_credentials_ignore_stale_ambient_file_reference(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("stored-over-stale-file")
    monkeypatch.setenv("POLYGON_API_KEY_FILE", str(tmp_path / "missing_polygon_key"))
    manager.update_source(
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    _mock_polygon_get(data_source_manager, monkeypatch, canary)

    result = manager.test_connection("polygon", actor="unit-test")

    assert result["ok"] is True
    assert canary not in json.dumps(result, sort_keys=True, default=str)


def test_missing_master_key_blocks_test_save_before_storing_credentials(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("missing-master-key")
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY", raising=False)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)
    monkeypatch.delenv("CREDENTIALS_DIRECTORY", raising=False)
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "systemd-creds")

    result = routes.api_post_data_source_test_save(
        None,
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        },
    )

    rendered = json.dumps(result, sort_keys=True, default=str)
    assert result["ok"] is False
    assert result["saved"] is False
    assert "test_save_failed" in result["error"]
    assert canary not in rendered
    assert manager.get_source("polygon", include_credentials=True)["credentials"] == {}


def test_strict_connection_test_projects_stored_credentials_to_runtime_file(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("strict-file")
    runtime_dir = tmp_path / "runtime-secrets"
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("DATA_SOURCE_MANAGER_RUNTIME_SECRET_DIR", str(runtime_dir))
    manager.update_source(
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    _mock_polygon_get(data_source_manager, monkeypatch, canary)

    result = manager.test_connection("polygon", actor="unit-test")

    files = list(Path(runtime_dir).glob("polygon_api_key"))
    assert result["ok"] is True
    assert files and files[0].read_text(encoding="utf-8").strip() == canary
    assert "POLYGON_API_KEY" not in data_source_manager.os.environ
    assert canary not in json.dumps(result, sort_keys=True, default=str)


def test_masked_source_credential_values_are_rejected(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    try:
        manager.update_source(
            {
                "source_key": "polygon",
                "credentials": {"api_key": "***"},
                "replace_credentials": True,
                "actor": "unit-test",
            }
        )
    except ValueError as exc:
        assert str(exc) == "masked_credential_value_rejected:api_key"
    else:
        raise AssertionError("masked source credential was accepted")


def test_connection_test_clears_cached_file_credentials_after_rotation(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    first = _canary("file-before-rotation")
    second = _canary("file-after-rotation")
    secret_file = tmp_path / "secrets" / "polygon_api_key"
    secret_file.parent.mkdir(parents=True, exist_ok=True)
    secret_file.write_text(first, encoding="utf-8")
    secret_file.chmod(0o600)
    monkeypatch.setenv("POLYGON_API_KEY_FILE", str(secret_file))

    from engine.data._credentials import get_data_credential

    assert get_data_credential("POLYGON_API_KEY") == first
    secret_file.write_text(second, encoding="utf-8")
    _mock_polygon_get(data_source_manager, monkeypatch, second)

    result = manager.test_connection("polygon", actor="unit-test")

    assert result["ok"] is True
    assert first not in json.dumps(result, sort_keys=True, default=str)
    assert second not in json.dumps(result, sort_keys=True, default=str)


def test_failed_test_save_does_not_leave_source_healthy(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = _canary("failed-test-save")

    def _fake_get(_url, **kwargs):
        assert kwargs["params"]["apiKey"] == canary
        return _JsonResponse(status_code=401, payload={"error": "unauthorized"})

    monkeypatch.setattr(data_source_manager.requests, "get", _fake_get)

    result = routes.api_post_data_source_test_save(
        None,
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        },
    )
    source = manager.get_source("polygon")

    assert result["ok"] is False
    assert result["saved"] is True
    assert result["test"]["classification"] == "wrong_credentials"
    assert source["status"] == "test_failed"
    assert source["last_error"]
    assert canary not in json.dumps(result, sort_keys=True, default=str)
