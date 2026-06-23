from __future__ import annotations

import base64
import importlib
import json
import sys
import types
import uuid
from urllib.parse import urlparse

import pytest


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _isolated_data_source_modules(monkeypatch, tmp_path):
    for key in (
        "ALPACA_KEY_ID",
        "ALPACA_KEY_ID_FILE",
        "ALPACA_SECRET_KEY",
        "ALPACA_SECRET_KEY_FILE",
        "ALPACA_BASE_URL",
        "DATA_SOURCE_ALLOW_LIVE_ALPACA_BROKER_DATA",
        "IBKR_HOST",
        "IBKR_PORT",
        "IBKR_CLIENT_ID",
        "IBKR_MARKET_DATA_TYPE",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "broker_data_sources.sqlite"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", "1")
    monkeypatch.setenv("DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_S", "0")
    monkeypatch.setenv("ENGINE_SUPERVISED", "0")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")
    monkeypatch.setenv("TELEMETRY_READ_BACKEND", "sqlite")
    monkeypatch.setenv("TELEMETRY_READ_REQUIRE_VALIDATION", "0")
    storage, = _reload_modules("engine.runtime.storage")
    storage.init_db()
    _broker_readonly, data_source_manager = _reload_modules(
        "engine.data.broker_readonly",
        "services.data_source_manager",
    )
    return data_source_manager


def _canary(prefix: str) -> str:
    return f"codex-canary-{prefix}-{uuid.uuid4().hex}"


class _JsonResponse:
    def __init__(self, status_code: int = 200, payload=None, headers: dict[str, str] | None = None) -> None:
        self.status_code = int(status_code)
        self.headers = dict(headers or {})
        self._payload = payload if payload is not None else {"id": "account"}

    def json(self):
        return self._payload


def _assert_canaries_absent(payload: object, *canaries: str) -> None:
    rendered = json.dumps(payload, sort_keys=True, default=str)
    for canary in canaries:
        assert canary not in rendered


def _configure_alpaca_account(manager, key_canary: str, secret_canary: str) -> None:
    manager.update_provider_account(
        {
            "account_key": "alpaca_data",
            "credentials": {"key_id": key_canary, "secret_key": secret_canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )


def test_alpaca_broker_data_missing_credentials_does_not_touch_http(monkeypatch, tmp_path) -> None:
    data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    monkeypatch.setattr(
        data_source_manager.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("alpaca_http_must_not_run_without_credentials")),
    )

    result = manager.test_connection("alpaca_broker_data", actor="unit-test")

    assert result["ok"] is False
    assert result["classification"] == "missing_credentials"
    assert result["evidence"]["missing_env_vars"] == ["ALPACA_KEY_ID", "ALPACA_SECRET_KEY"]


def test_alpaca_broker_data_readonly_success_lists_account_and_positions(monkeypatch, tmp_path) -> None:
    data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    key_canary = _canary("alpaca-key")
    secret_canary = _canary("alpaca-secret")
    _configure_alpaca_account(manager, key_canary, secret_canary)
    calls: list[str] = []

    def _fake_get(url, **kwargs):
        headers = kwargs.get("headers") or {}
        assert headers.get("APCA-API-KEY-ID") == key_canary
        assert headers.get("APCA-API-SECRET-KEY") == secret_canary
        path = urlparse(str(url)).path
        calls.append(path)
        if path == "/v2/account":
            return _JsonResponse(payload={"id": "account-canary"})
        if path == "/v2/positions":
            return _JsonResponse(payload=[])
        raise AssertionError(f"unexpected_alpaca_path:{path}")

    monkeypatch.setattr(data_source_manager.requests, "get", _fake_get)
    monkeypatch.setattr(
        data_source_manager.requests,
        "post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("alpaca_post_forbidden")),
    )

    result = manager.test_connection("alpaca_broker_data", actor="unit-test")

    assert result["ok"] is True
    assert calls == ["/v2/account", "/v2/positions"]
    assert "/v2/orders" not in calls
    assert result["evidence"]["broker_data_readonly"] is True
    assert result["evidence"]["order_authority"] is False
    assert result["evidence"]["readonly_guard"]["alpaca_allowed_http_methods"] == ["GET"]
    _assert_canaries_absent(result, key_canary, secret_canary)


def test_alpaca_broker_data_rejected_credentials_are_classified_without_secret_leak(monkeypatch, tmp_path) -> None:
    data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    key_canary = _canary("alpaca-rejected-key")
    secret_canary = _canary("alpaca-rejected-secret")
    _configure_alpaca_account(manager, key_canary, secret_canary)

    monkeypatch.setattr(
        data_source_manager.requests,
        "get",
        lambda *_args, **_kwargs: _JsonResponse(status_code=401, payload={"message": "unauthorized"}),
    )

    result = manager.test_connection("alpaca_broker_data", actor="unit-test")

    assert result["ok"] is False
    assert result["classification"] == "wrong_credentials"
    assert result["message"] == "alpaca_broker_data_credentials_rejected"
    _assert_canaries_absent(result, key_canary, secret_canary)
    _assert_canaries_absent(manager.list_logs("alpaca_broker_data", limit=20), key_canary, secret_canary)


def test_alpaca_live_base_url_is_policy_blocked_before_http(monkeypatch, tmp_path) -> None:
    data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    key_canary = _canary("alpaca-live-key")
    secret_canary = _canary("alpaca-live-secret")
    _configure_alpaca_account(manager, key_canary, secret_canary)
    manager.update_source(
        {
            "source_key": "alpaca_broker_data",
            "settings": {"base_url": "https://api.alpaca.markets"},
            "actor": "unit-test",
        }
    )
    monkeypatch.setattr(
        data_source_manager.requests,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("live_alpaca_http_must_be_blocked")),
    )

    result = manager.test_connection("alpaca_broker_data", actor="unit-test")

    assert result["ok"] is False
    assert result["classification"] == "policy_blocked"
    assert result["message"] == "alpaca_live_base_url_blocked"
    assert result["evidence"]["live_base_url"] is True
    _assert_canaries_absent(result, key_canary, secret_canary)


def test_alpaca_broker_data_enable_does_not_project_runtime_secrets(monkeypatch, tmp_path) -> None:
    data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    runtime_secret_dir = tmp_path / "runtime-secrets"
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("DATA_SOURCE_MANAGER_RUNTIME_SECRET_DIR", str(runtime_secret_dir))
    key_canary = _canary("alpaca-no-project-key")
    secret_canary = _canary("alpaca-no-project-secret")
    _configure_alpaca_account(manager, key_canary, secret_canary)
    manager.update_source({"source_key": "alpaca_broker_data", "enabled": True, "actor": "unit-test"})

    desired = manager.get_desired_ingestion_jobs()
    env = manager.build_job_environment("alpaca_broker_data_readonly")

    assert "alpaca_broker_data_readonly" not in desired
    assert env == {}
    assert not runtime_secret_dir.exists() or list(runtime_secret_dir.iterdir()) == []


def _install_fake_ib_insync(monkeypatch, ib_cls):
    module = types.ModuleType("ib_insync")

    class FakeStock:
        def __init__(self, symbol, exchange, currency):
            self.symbol = symbol
            self.exchange = exchange
            self.currency = currency

    module.IB = ib_cls
    module.Stock = FakeStock
    monkeypatch.setitem(sys.modules, "ib_insync", module)
    return module


def test_ibkr_readonly_success_performs_authenticated_historical_data_read(monkeypatch, tmp_path) -> None:
    data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    class FakeIB:
        instances: list["FakeIB"] = []

        def __init__(self):
            self.calls: list[tuple] = []
            self.connected = False
            self.instances.append(self)

        def connect(self, host, port, *, clientId, timeout, readonly):
            self.calls.append(("connect", host, port, clientId, timeout, readonly))
            self.connected = True

        def reqMarketDataType(self, value):
            self.calls.append(("reqMarketDataType", value))

        def qualifyContracts(self, contract):
            self.calls.append(("qualifyContracts", contract.symbol))
            return [contract]

        def reqHistoricalData(self, contract, **kwargs):
            self.calls.append(("reqHistoricalData", contract.symbol, kwargs.get("whatToShow")))
            return [object()]

        def isConnected(self):
            self.calls.append(("isConnected",))
            return self.connected

        def disconnect(self):
            self.calls.append(("disconnect",))
            self.connected = False

        def placeOrder(self, *_args, **_kwargs):
            raise AssertionError("ibkr_place_order_forbidden")

        def cancelOrder(self, *_args, **_kwargs):
            raise AssertionError("ibkr_cancel_order_forbidden")

    _install_fake_ib_insync(monkeypatch, FakeIB)
    manager.update_source(
        {
            "source_key": "ibkr",
            "settings": {"host": "127.0.0.1", "port": "7497", "client_id": "88", "market_data_type": "3"},
            "actor": "unit-test",
        }
    )

    result = manager.test_connection("ibkr", actor="unit-test")

    assert result["ok"] is True
    assert result["evidence"]["readonly"] is True
    assert result["evidence"]["authenticated_read"] is True
    assert result["evidence"]["market_data_type"] == 3
    calls = FakeIB.instances[-1].calls
    assert ("connect", "127.0.0.1", 7497, 88, 5.0, True) in calls
    assert ("reqMarketDataType", 3) in calls
    assert any(call[0] == "reqHistoricalData" for call in calls)
    assert not any(call[0] in {"placeOrder", "cancelOrder"} for call in calls)


def test_ibkr_gateway_absent_fails_without_order_methods(monkeypatch, tmp_path) -> None:
    data_source_manager = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    class GatewayAbsentIB:
        calls: list[tuple] = []

        def connect(self, *_args, **kwargs):
            self.calls.append(("connect", kwargs.get("readonly")))
            raise ConnectionRefusedError("gateway_absent")

        def placeOrder(self, *_args, **_kwargs):
            raise AssertionError("ibkr_place_order_forbidden")

        def cancelOrder(self, *_args, **_kwargs):
            raise AssertionError("ibkr_cancel_order_forbidden")

    _install_fake_ib_insync(monkeypatch, GatewayAbsentIB)
    manager.update_source(
        {
            "source_key": "ibkr",
            "settings": {"host": "127.0.0.1", "port": "7497", "client_id": "89", "market_data_type": "1"},
            "actor": "unit-test",
        }
    )

    result = manager.test_connection("ibkr", actor="unit-test")

    assert result["ok"] is False
    assert result["classification"] == "provider_unreachable"
    assert result["evidence"]["error_type"] == "ConnectionRefusedError"
    assert GatewayAbsentIB.calls == [("connect", True)]


def test_static_readonly_guards_reject_forbidden_broker_operations() -> None:
    from engine.data import broker_readonly

    snapshot = broker_readonly.readonly_guard_snapshot()
    assert snapshot["alpaca_allowed_http_methods"] == ["GET"]
    assert snapshot["alpaca_allowed_paths"] == ["/v2/account", "/v2/positions"]
    assert "submit_order" in snapshot["forbidden_broker_operations"]
    assert "placeOrder" in snapshot["forbidden_broker_operations"]

    with pytest.raises(broker_readonly.BrokerDataReadOnlyViolation):
        broker_readonly.assert_alpaca_readonly_request("GET", "/v2/orders")
    with pytest.raises(broker_readonly.BrokerDataReadOnlyViolation):
        broker_readonly.assert_alpaca_readonly_request("POST", "/v2/orders")
    with pytest.raises(broker_readonly.BrokerDataReadOnlyViolation):
        broker_readonly.assert_ibkr_readonly_method("placeOrder")
    with pytest.raises(broker_readonly.BrokerDataReadOnlyViolation):
        broker_readonly.assert_data_source_broker_runtime_allowed(
            source_key="alpaca_broker_data",
            source_type="broker_data_provider",
            provider_name="alpaca",
            job_name="broker_apply_orders",
            runtime_runnable=True,
        )

    client = broker_readonly.AlpacaBrokerDataReadOnlyClient(
        key_id="codex-canary-static-key",
        secret_key="codex-canary-static-secret",
        settings=broker_readonly.AlpacaBrokerDataSettings(),
        http_get=lambda *_args, **_kwargs: _JsonResponse(),
    )
    with pytest.raises(broker_readonly.BrokerDataReadOnlyViolation):
        client.submit_order({})
