from __future__ import annotations

import importlib
import sys
import types


def _reload_registry(monkeypatch):
    fake_manager = types.SimpleNamespace(
        inject_provider_registry=lambda: {},
        desired_ingestion_jobs=lambda read_only=True: [],
    )
    monkeypatch.setitem(sys.modules, "services.data_source_manager", fake_manager)
    import engine.data.provider_registry as provider_registry

    return importlib.reload(provider_registry)


def test_oanda_enabled_provider_is_polling_fx_provider(monkeypatch) -> None:
    monkeypatch.setenv("OANDA_ENABLED", "1")
    provider_registry = _reload_registry(monkeypatch)

    assert "oanda" in provider_registry.get_polling_provider_names()
    definition = provider_registry.get_provider_definition("oanda")
    assert definition is not None
    assert definition.mode == "polling"
    assert definition.implementation_kind == "live_price_provider"
    assert definition.supports["asset_classes"] == ["fx"]
    assert definition.supports["transport"] == "rest"


def test_oanda_default_off_and_poll_prices_fallback(monkeypatch) -> None:
    monkeypatch.delenv("OANDA_ENABLED", raising=False)
    monkeypatch.delenv("INGESTION_CHILD_JOBS", raising=False)
    monkeypatch.delenv("LIVE_PRICE_PROVIDER_CHAIN", raising=False)
    monkeypatch.setenv("POLYGON_WS_ENABLED", "0")
    monkeypatch.setenv("POLYGON_REST_ENABLED", "0")
    monkeypatch.setenv("YFINANCE_ENABLED", "1")
    monkeypatch.setenv("CCXT_ENABLED", "0")
    provider_registry = _reload_registry(monkeypatch)
    monkeypatch.setattr(provider_registry, "get_data_credential", lambda _name: "")

    assert "oanda" not in provider_registry.get_polling_provider_names()
    assert provider_registry.get_enabled_market_data_job_names() == ["poll_prices"]


def test_oanda_enabled_keeps_poll_prices_when_credentialed(monkeypatch) -> None:
    monkeypatch.setenv("OANDA_ENABLED", "1")
    monkeypatch.setenv("YFINANCE_ENABLED", "0")
    monkeypatch.setenv("POLYGON_REST_ENABLED", "0")
    monkeypatch.setenv("POLYGON_WS_ENABLED", "0")
    monkeypatch.setenv("CCXT_ENABLED", "0")
    monkeypatch.setenv("LIVE_PRICE_PROVIDER_CHAIN", "oanda")
    monkeypatch.setenv("INGESTION_CHILD_JOBS", "poll_prices")
    provider_registry = _reload_registry(monkeypatch)
    monkeypatch.setattr(
        provider_registry,
        "get_data_credential",
        lambda name: "oanda-canary-token" if name == "OANDA_ACCESS_TOKEN" else "",
    )

    assert provider_registry.get_enabled_market_data_job_names() == ["poll_prices"]


def test_ibkr_definition_advertises_fx_support(monkeypatch) -> None:
    provider_registry = _reload_registry(monkeypatch)

    definition = provider_registry.get_provider_definition("ibkr")

    assert definition is not None
    assert "fx" in definition.supports["asset_classes"]
