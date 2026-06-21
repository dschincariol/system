from __future__ import annotations

import os
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.data import provider_registry
from services.data_source_manager import DataSourceManager


def test_disabled_paid_desired_jobs_are_filtered(monkeypatch) -> None:
    fake_manager = types.SimpleNamespace(
        desired_ingestion_jobs=lambda read_only=True: [
            "stream_prices_polygon_ws",
            "options_poll",
            "poll_prices",
        ]
    )
    monkeypatch.setitem(sys.modules, "services.data_source_manager", fake_manager)
    monkeypatch.setattr(provider_registry, "get_data_credential", lambda name: "")
    monkeypatch.delenv("INGESTION_CHILD_JOBS", raising=False)
    monkeypatch.delenv("LIVE_PRICE_PROVIDER_CHAIN", raising=False)
    monkeypatch.setenv("POLYGON_WS_ENABLED", "0")
    monkeypatch.setenv("POLYGON_REST_ENABLED", "0")
    monkeypatch.setenv("TRADIER_ENABLED", "0")
    monkeypatch.setenv("IBKR_ENABLED", "0")
    monkeypatch.setenv("CCXT_ENABLED", "0")
    monkeypatch.setenv("YFINANCE_ENABLED", "1")

    assert provider_registry.get_enabled_market_data_job_names() == ["poll_prices"]


def test_env_override_cannot_bypass_disabled_provider(monkeypatch) -> None:
    monkeypatch.setattr(provider_registry, "get_data_credential", lambda name: "")
    monkeypatch.setenv("INGESTION_CHILD_JOBS", "stream_prices_polygon_ws,poll_prices")
    monkeypatch.setenv("POLYGON_WS_ENABLED", "0")
    monkeypatch.setenv("YFINANCE_ENABLED", "1")

    assert provider_registry.get_enabled_market_data_job_names() == ["poll_prices"]


def test_safe_mode_data_source_projection_removes_credential_provider_env(monkeypatch) -> None:
    manager = DataSourceManager()
    monkeypatch.setattr(manager, "initialize", lambda: None)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("BROKER", "sim")
    monkeypatch.setenv("BROKER_NAME", "sim")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("POLYGON_API_KEY", "dummy")
    monkeypatch.setenv("POLYGON_KEY", "dummy")
    monkeypatch.setenv("TRADIER_API_TOKEN", "dummy")
    monkeypatch.setenv("IBKR_HOST", "dummy")
    monkeypatch.setenv("IBKR_CLIENT_ID", "dummy")
    monkeypatch.setenv("FINNHUB_API_KEY", "dummy")
    monkeypatch.setenv("FMP_API_KEY", "dummy")
    monkeypatch.setenv("ALPACA_KEY_ID", "dummy")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "dummy")
    monkeypatch.setenv("OPENAI_API_KEY", "dummy")
    monkeypatch.setenv("POLYGON_WS_ENABLED", "1")
    monkeypatch.setenv("TRADIER_ENABLED", "1")

    projected = manager.apply_runtime_environment()

    assert projected["YFINANCE_ENABLED"] == "1"
    assert projected["POLYGON_WS_ENABLED"] == "0"
    assert projected["TRADIER_ENABLED"] == "0"
    assert "POLYGON_API_KEY" not in os.environ
    assert "POLYGON_KEY" not in os.environ
    assert "TRADIER_API_TOKEN" not in os.environ
    assert "IBKR_HOST" not in os.environ
    assert "IBKR_CLIENT_ID" not in os.environ
    assert "FINNHUB_API_KEY" not in os.environ
    assert "FMP_API_KEY" not in os.environ
    assert "ALPACA_KEY_ID" not in os.environ
    assert "ALPACA_SECRET_KEY" not in os.environ
    assert "OPENAI_API_KEY" not in os.environ


def test_safe_mode_projection_sanitizes_child_env_dict(monkeypatch) -> None:
    from services.data_source_manager import apply_safe_no_credential_runtime_environment

    env = {
        "POLYGON_API_KEY": "dummy",
        "TRADIER_API_TOKEN": "dummy",
        "IBKR_HOST": "dummy",
        "FMP_API_KEY": "dummy",
        "ALPACA_KEY_ID": "dummy",
        "ALPACA_SECRET_KEY": "dummy",
        "OPENAI_API_KEY": "dummy",
        "POLYGON_WS_ENABLED": "1",
        "TRADIER_ENABLED": "1",
    }

    projected = apply_safe_no_credential_runtime_environment(env)

    assert projected["YFINANCE_ENABLED"] == "1"
    assert env["POLYGON_WS_ENABLED"] == "0"
    assert env["TRADIER_ENABLED"] == "0"
    assert "POLYGON_API_KEY" not in env
    assert "TRADIER_API_TOKEN" not in env
    assert "IBKR_HOST" not in env
    assert "FMP_API_KEY" not in env
    assert "ALPACA_KEY_ID" not in env
    assert "ALPACA_SECRET_KEY" not in env
    assert "OPENAI_API_KEY" not in env


def test_strict_projection_skips_enabled_providers_with_missing_credential_files(
    monkeypatch,
) -> None:
    manager = DataSourceManager()
    rows = [
        {
            "source_key": "polygon_ws",
            "source_type": "price_provider",
            "provider_name": "polygon_ws",
            "job_name": "stream_prices_polygon_ws",
            "enabled": True,
            "credentials": {},
            "settings": {},
        },
        {
            "source_key": "polygon",
            "source_type": "price_provider",
            "provider_name": "polygon",
            "job_name": "poll_prices",
            "enabled": True,
            "credentials": {},
            "settings": {},
        },
        {
            "source_key": "tradier",
            "source_type": "options_provider",
            "provider_name": "tradier",
            "job_name": "options_poll",
            "enabled": True,
            "credentials": {},
            "settings": {},
        },
    ]
    monkeypatch.setattr(manager, "initialize", lambda: None)
    monkeypatch.setattr(manager, "list_sources", lambda include_credentials=False: rows)
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("POLYGON_API_KEY_FILE", "./data/secrets/polygon_api_key")
    monkeypatch.setenv("POLYGON_KEY_FILE", "./data/secrets/polygon_api_key")
    monkeypatch.setenv("TRADIER_API_TOKEN_FILE", "./data/secrets/tradier_api_token")

    projected = manager.apply_runtime_environment()

    assert projected["POLYGON_REST_ENABLED"] == "0"
    assert projected["POLYGON_WS_ENABLED"] == "0"
    assert projected["TRADIER_ENABLED"] == "0"
    assert "POLYGON_API_KEY_FILE" not in os.environ
    assert "POLYGON_KEY_FILE" not in os.environ
    assert "TRADIER_API_TOKEN_FILE" not in os.environ


def test_strict_projection_writes_db_credentials_to_runtime_files(
    monkeypatch,
    tmp_path,
) -> None:
    manager = DataSourceManager()
    rows = [
        {
            "source_key": "polygon",
            "source_type": "price_provider",
            "provider_name": "polygon",
            "job_name": "poll_prices",
            "enabled": True,
            "credentials": {"api_key": "polygon-secret"},
            "settings": {},
        }
    ]
    monkeypatch.setattr(manager, "initialize", lambda: None)
    monkeypatch.setattr(manager, "list_sources", lambda include_credentials=False: rows)
    monkeypatch.setenv("ENV", "prod")
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("DATA_SOURCE_MANAGER_RUNTIME_SECRET_DIR", str(tmp_path))

    projected = manager.apply_runtime_environment()

    assert projected["POLYGON_REST_ENABLED"] == "1"
    assert "POLYGON_API_KEY" not in projected
    secret_path = Path(projected["POLYGON_API_KEY_FILE"])
    assert secret_path.read_text(encoding="utf-8") == "polygon-secret"
    assert oct(secret_path.stat().st_mode & 0o777) == "0o600"


def test_credential_runtime_env_keys_warns_when_catalog_fails(monkeypatch) -> None:
    import services.data_source_manager as module

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(module, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))
    monkeypatch.setattr(module, "_default_catalog", lambda: (_ for _ in ()).throw(RuntimeError("catalog failed")))

    keys = module.credential_runtime_env_keys()

    assert "OPENAI_API_KEY" in keys
    assert calls
    assert calls[0][0][0] == "DATA_SOURCE_MANAGER_CREDENTIAL_CATALOG_KEYS_FAILED"
