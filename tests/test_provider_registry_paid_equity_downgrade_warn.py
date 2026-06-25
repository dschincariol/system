from __future__ import annotations

import importlib
import json
import sys
import types
import uuid
from typing import Any


def _reload_provider_registry():
    import engine.data.provider_registry as provider_registry

    return importlib.reload(provider_registry)


def _disable_desired_jobs(monkeypatch) -> None:
    fake_manager = types.SimpleNamespace(desired_ingestion_jobs=lambda read_only=True: [])
    monkeypatch.setitem(sys.modules, "services.data_source_manager", fake_manager)


def test_paid_equity_downgrade_warn_does_not_change_returned_jobs(monkeypatch) -> None:
    provider_registry = _reload_provider_registry()
    canary = f"EQ08_POLYGON_VALUE_{uuid.uuid4().hex}"
    calls: list[dict[str, Any]] = []

    _disable_desired_jobs(monkeypatch)
    monkeypatch.delenv("INGESTION_CHILD_JOBS", raising=False)
    monkeypatch.setenv("LIVE_PRICE_PROVIDER_CHAIN", "yfinance")
    monkeypatch.setenv("YFINANCE_ENABLED", "1")
    monkeypatch.setenv("POLYGON_WS_ENABLED", "1")
    monkeypatch.setenv("POLYGON_REST_ENABLED", "1")
    monkeypatch.setenv("IBKR_ENABLED", "0")
    monkeypatch.setattr(
        provider_registry,
        "get_data_credential",
        lambda name, *args, **kwargs: canary if str(name) == "POLYGON_API_KEY" else "",
    )

    def fake_warn(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
        calls.append({"code": code, "once_key": once_key, "extra": dict(extra or {})})

    monkeypatch.setattr(provider_registry, "_warn_nonfatal", fake_warn)

    out = provider_registry.get_enabled_market_data_job_names()

    assert out == ["poll_prices"]
    assert calls
    assert calls[0]["code"] == "PROVIDER_REGISTRY_PAID_EQUITY_DOWNGRADE"
    assert calls[0]["extra"]["returned_jobs"] == ["poll_prices"]
    rendered = json.dumps(calls, sort_keys=True, default=str)
    assert canary not in rendered


def test_no_paid_equity_provider_configured_emits_no_downgrade_warn(monkeypatch) -> None:
    provider_registry = _reload_provider_registry()
    calls: list[dict[str, Any]] = []

    _disable_desired_jobs(monkeypatch)
    monkeypatch.delenv("INGESTION_CHILD_JOBS", raising=False)
    monkeypatch.delenv("LIVE_PRICE_PROVIDER_CHAIN", raising=False)
    monkeypatch.setenv("YFINANCE_ENABLED", "1")
    monkeypatch.setenv("POLYGON_WS_ENABLED", "1")
    monkeypatch.setenv("POLYGON_REST_ENABLED", "1")
    monkeypatch.setenv("IBKR_ENABLED", "0")
    monkeypatch.setattr(provider_registry, "get_data_credential", lambda *args, **kwargs: "")
    monkeypatch.setattr(
        provider_registry,
        "_warn_nonfatal",
        lambda code, error, **extra: calls.append({"code": code, "extra": dict(extra or {})}),
    )

    out = provider_registry.get_enabled_market_data_job_names()

    assert out == ["poll_prices"]
    assert calls == []
