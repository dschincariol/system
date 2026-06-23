from __future__ import annotations

import base64
import importlib
import json
import os
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")
POLYGON_CANARY = "canary-df07-polygon-runtime-key"
ALPACA_CANARY_KEY = "canary-df07-alpaca-key-id"
ALPACA_CANARY_SECRET = "canary-df07-alpaca-secret-key"


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


@pytest.fixture()
def runnable_context(monkeypatch, tmp_path):
    for key in (
        "POLYGON_API_KEY",
        "POLYGON_API_KEY_FILE",
        "POLYGON_KEY",
        "POLYGON_KEY_FILE",
        "TRADIER_API_TOKEN",
        "FINNHUB_API_KEY",
        "FMP_API_KEY",
        "ALPACA_KEY_ID",
        "ALPACA_SECRET_KEY",
        "DATA_SOURCE_MANAGER_PROJECTED_KEYS",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "runnable_states.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", "1")

    storage, manager_mod, routes_mod, health_mod = _reload_modules(
        "engine.runtime.storage",
        "services.data_source_manager",
        "routes.data_sources_routes",
        "engine.runtime.health",
    )
    storage.init_db()
    manager = manager_mod.DataSourceManager()
    manager_mod._MANAGER = manager
    manager.initialize()
    return manager, routes_mod, health_mod


def _source_from_payload(payload: dict, source_key: str) -> dict:
    for row in payload.get("sources") or []:
        if str(row.get("source_key") or "") == source_key:
            return dict(row)
    raise AssertionError(f"missing source {source_key}")


def _assert_canaries_absent(payload: object) -> None:
    text = json.dumps(payload, sort_keys=True, default=str)
    assert POLYGON_CANARY not in text
    assert ALPACA_CANARY_KEY not in text
    assert ALPACA_CANARY_SECRET not in text


def test_enabled_missing_key_feed_is_not_desired_or_projected(runnable_context) -> None:
    manager, routes, health = runnable_context
    manager.update_source(
        {
            "source_key": "polygon_ws",
            "enabled": True,
            "clear_credential_fields": ["api_key"],
            "actor": "unit-test",
        }
    )

    desired = manager.get_desired_ingestion_jobs(default_jobs=["stream_prices_polygon_ws"])
    env = manager.build_job_environment("stream_prices_polygon_ws")
    payload = routes.api_get_data_sources(None)
    source = _source_from_payload(payload, "polygon_ws")
    readiness = health.provider_readiness_snapshot(mode="paper", required_providers=["polygon_ws"])

    assert "stream_prices_polygon_ws" not in desired
    assert env["POLYGON_WS_ENABLED"] == "0"
    assert source["runnable_state"] == "enabled-missing-credential"
    assert source["missing_credential_env_vars"] == ["POLYGON_API_KEY"]
    assert readiness["by_provider"]["polygon_ws"]["runnable_state"] == "enabled-missing-credential"
    assert readiness["by_provider"]["polygon_ws"]["credential_configured"] is False
    _assert_canaries_absent(payload)
    _assert_canaries_absent(readiness)


def test_entering_key_and_enabling_feed_schedules_pending_without_secret_leak(runnable_context) -> None:
    manager, routes, _health = runnable_context
    manager.update_source(
        {
            "source_key": "polygon_ws",
            "enabled": True,
            "credentials": {"api_key": POLYGON_CANARY},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    desired = manager.get_desired_ingestion_jobs(default_jobs=["stream_prices_polygon_ws"])
    env = manager.build_job_environment("stream_prices_polygon_ws")
    payload = routes.api_get_data_sources(None)
    source = _source_from_payload(payload, "polygon_ws")
    logs_payload = routes.api_get_data_source_logs(
        {"source_key": "polygon_ws", "limit": "20"},
    )

    assert "stream_prices_polygon_ws" in desired
    assert env["POLYGON_WS_ENABLED"] == "1"
    assert env["POLYGON_API_KEY"] == POLYGON_CANARY
    assert source["runnable_state"] == "scheduled-waiting"
    assert source["runtime_credentialed"] is True
    assert logs_payload["runnable_state"] in {"enabled-credentialed-not-scheduled", "scheduled-waiting"}
    _assert_canaries_absent(payload)
    _assert_canaries_absent(logs_payload)


def test_clearing_key_removes_desired_job_and_disabling_withdraws_projection(runnable_context) -> None:
    manager, _routes, _health = runnable_context
    manager.update_source(
        {
            "source_key": "polygon_ws",
            "enabled": True,
            "credentials": {"api_key": POLYGON_CANARY},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    projected_before = manager.apply_runtime_environment()
    assert projected_before["POLYGON_WS_ENABLED"] == "1"
    assert os.environ.get("POLYGON_API_KEY") == POLYGON_CANARY

    manager.update_source(
        {
            "source_key": "polygon_ws",
            "clear_credential_fields": ["api_key"],
            "actor": "unit-test",
        }
    )
    desired_after_clear = manager.get_desired_ingestion_jobs(default_jobs=["stream_prices_polygon_ws"])
    cleared = manager.get_source("polygon_ws") or {}
    assert "stream_prices_polygon_ws" not in desired_after_clear
    assert cleared["runnable_state"] == "enabled-missing-credential"

    manager.update_source(
        {
            "source_key": "polygon_ws",
            "enabled": False,
            "credentials": {"api_key": POLYGON_CANARY},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    projected_after_disable = manager.apply_runtime_environment()
    disabled = manager.get_source("polygon_ws") or {}
    assert disabled["runnable_state"] == "off"
    assert projected_after_disable["POLYGON_WS_ENABLED"] == "0"
    assert "POLYGON_API_KEY" not in os.environ


def test_shared_ingest_now_runs_for_keyless_rss_without_missing_key_feeds(runnable_context) -> None:
    manager, routes, _health = runnable_context
    manager.update_source(
        {
            "source_key": "company_news",
            "enabled": True,
            "clear_credential_fields": ["api_key"],
            "actor": "unit-test",
        }
    )
    rss = manager.create_source(
        {
            "source_key": "rss:df07_unit_feed",
            "display_name": "DF07 Unit Feed",
            "source_type": "rss_feed",
            "enabled": True,
            "settings": {
                "name": "DF07 Unit Feed",
                "url": "https://example.com/feed.xml",
            },
            "actor": "unit-test",
        }
    )

    desired = manager.get_desired_ingestion_jobs(default_jobs=["ingest_now"])
    env = manager.build_job_environment("ingest_now")
    payload = routes.api_get_data_sources(None)
    company_news = _source_from_payload(payload, "company_news")
    rss_source = _source_from_payload(payload, rss["source_key"])

    assert "ingest_now" in desired
    assert env["INGEST_NOW_ENABLE_COMPANY_NEWS"] == "0"
    assert company_news["runnable_state"] == "enabled-missing-credential"
    assert rss_source["runnable_state"] == "scheduled-waiting"
    _assert_canaries_absent(payload)


def test_enabled_non_runnable_source_does_not_create_looping_job(runnable_context) -> None:
    manager, routes, _health = runnable_context
    manager.update_source(
        {
            "source_key": "alpaca_broker_data",
            "enabled": True,
            "credentials": {
                "key_id": ALPACA_CANARY_KEY,
                "secret_key": ALPACA_CANARY_SECRET,
            },
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    desired = manager.get_desired_ingestion_jobs()
    payload = routes.api_get_data_sources(None)
    source = _source_from_payload(payload, "alpaca_broker_data")

    assert "alpaca_broker_data_readonly" not in desired
    assert source["runtime_runnable"] is False
    assert source["runnable_state"] == "enabled-credentialed-not-scheduled"
    assert source["runtime_desired_eligible"] is False
    _assert_canaries_absent(payload)
