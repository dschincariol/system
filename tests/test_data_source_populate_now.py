from __future__ import annotations

import base64
import importlib
import json
from pathlib import Path
import sys
import time
import uuid

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _JsonResponse:
    def __init__(self, payload, *, status_code: int = 200, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.status_code = int(status_code)
        self.headers = dict(headers or {})
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


class _TextResponse:
    def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None) -> None:
        self.text = str(text)
        self.status_code = int(status_code)
        self.headers = dict(headers or {})

    def json(self):
        raise ValueError("not json")


@pytest.fixture()
def populate_context(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "data_source_populate_now.db"))
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
    storage, manager_mod, routes = _reload_modules(
        "engine.runtime.storage",
        "services.data_source_manager",
        "routes.data_sources_routes",
    )
    storage.init_db()
    manager = manager_mod.DataSourceManager()
    manager_mod._MANAGER = manager
    manager.initialize()
    yield storage, manager_mod, manager, routes
    try:
        storage.close_pooled_connections()
    except Exception:
        pass


def _assert_canary_absent(payload: object, *canaries: str) -> None:
    text = json.dumps(payload, sort_keys=True, default=str)
    for canary in canaries:
        assert canary not in text


def _count(storage, sql: str, params: tuple = ()) -> int:
    con = storage.connect_ro_direct()
    try:
        row = con.execute(sql, params).fetchone()
        return int((row or [0])[0] or 0)
    finally:
        con.close()


def test_populate_now_price_feed_lands_price_contract_and_hides_secret(populate_context, monkeypatch) -> None:
    storage, manager_mod, manager, routes = populate_context
    canary = f"codex-canary-polygon-{uuid.uuid4().hex}"
    manager.update_source(
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    def fake_get(url, **_kwargs):
        assert "orders" not in str(url).lower()
        return _JsonResponse(
            {
                "ticker": {
                    "ticker": "AAPL",
                    "lastTrade": {"p": 201.25, "t": int(time.time() * 1000)},
                }
            }
        )

    monkeypatch.setattr(manager_mod.requests, "get", fake_get)
    payload = routes.api_post_data_source_populate_now(None, {"source_key": "polygon", "actor": "unit-test"})

    assert payload["ok"] is True
    assert payload["populate_evidence"]["contract_status"] == "pass"
    assert payload["populate_evidence"]["storage_table"] == "prices"
    assert _count(storage, "SELECT COUNT(*) FROM prices WHERE symbol = ? AND source = ?", ("AAPL", "polygon")) == 1
    _assert_canary_absent(routes.api_get_data_sources(None), canary)
    _assert_canary_absent(routes.api_get_data_source_logs({"source_key": "polygon", "limit": "50"}), canary)


def test_populate_now_credentialed_api_feed_lands_event_contract(populate_context, monkeypatch) -> None:
    storage, manager_mod, manager, routes = populate_context
    canary = f"codex-canary-finnhub-{uuid.uuid4().hex}"
    manager.update_source(
        {
            "source_key": "company_news",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    def fake_get(url, **_kwargs):
        assert "orders" not in str(url).lower()
        return _JsonResponse(
            [
                    {
                        "id": 123,
                        "datetime": int(time.time()),
                        "headline": "Apple unit-test headline",
                    "summary": "summary",
                    "url": "https://example.test/news/123",
                    "source": "unit",
                }
            ]
        )

    monkeypatch.setattr(manager_mod.requests, "get", fake_get)
    payload = routes.api_post_data_source_populate_now(None, {"source_key": "company_news", "actor": "unit-test"})

    assert payload["ok"] is True
    assert payload["populate_evidence"]["contract_status"] == "pass"
    assert _count(storage, "SELECT COUNT(*) FROM events WHERE source = ? AND event_type = ?", ("company_news", "company_news")) == 1
    _assert_canary_absent(payload, canary)
    _assert_canary_absent(routes.api_get_data_sources(None), canary)


def test_populate_now_keyless_external_feed_lands_gdelt_event_contract(populate_context, monkeypatch) -> None:
    storage, manager_mod, _manager, routes = populate_context

    def fake_get(url, **_kwargs):
        assert "orders" not in str(url).lower()
        return _JsonResponse(
            {
                "articles": [
                        {
                            "title": "Market unit test",
                            "url": "https://example.test/gdelt",
                            "seendate": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                            "domain": "example.test",
                    }
                ]
            }
        )

    monkeypatch.setattr(manager_mod.requests, "get", fake_get)
    payload = routes.api_post_data_source_populate_now(None, {"source_key": "gdelt", "actor": "unit-test"})

    assert payload["ok"] is True
    assert payload["populate_evidence"]["contract_status"] == "pass"
    assert _count(storage, "SELECT COUNT(*) FROM events WHERE source = ? AND event_type = ?", ("gdelt", "gdelt_news")) == 1


def test_populate_now_pit_feed_lands_fundamentals_contract(populate_context, monkeypatch) -> None:
    storage, manager_mod, manager, routes = populate_context
    canary = f"codex-canary-simfin-{uuid.uuid4().hex}"
    manager.update_source(
        {
            "source_key": "fundamentals_pit",
            "credentials": {"simfin_api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    def fake_get(url, **_kwargs):
        assert "orders" not in str(url).lower()
        return _JsonResponse({"data": [{"ticker": "AAPL"}]})

    monkeypatch.setattr(manager_mod.requests, "get", fake_get)
    payload = routes.api_post_data_source_populate_now(None, {"source_key": "fundamentals_pit", "actor": "unit-test"})

    assert payload["ok"] is True
    assert payload["populate_evidence"]["contract_status"] == "pass"
    assert _count(storage, "SELECT COUNT(*) FROM fundamentals_pit WHERE symbol = ? AND vendor = ?", ("AAPL", "simfin")) == 1
    _assert_canary_absent(payload, canary)


def test_populate_now_rss_feed_lands_event_contract(populate_context, monkeypatch) -> None:
    storage, manager_mod, manager, routes = populate_context
    source = manager.create_source(
        {
            "source_key": "rss:df09_unit_feed",
            "display_name": "DF09 Unit Feed",
            "source_type": "rss_feed",
            "enabled": True,
            "settings": {"name": "DF09 Unit Feed", "url": "https://example.test/feed.xml"},
            "actor": "unit-test",
        }
    )

    def fake_get(url, **_kwargs):
        assert "orders" not in str(url).lower()
        return _TextResponse(
            """
            <rss><channel><item>
              <title>RSS unit item</title>
              <link>https://example.test/item</link>
              <pubDate>Tue, 02 Jan 2026 03:04:05 GMT</pubDate>
              <description>body</description>
            </item></channel></rss>
            """
        )

    monkeypatch.setattr(manager_mod.requests, "get", fake_get)
    payload = routes.api_post_data_source_populate_now(None, {"source_key": source["source_key"], "actor": "unit-test"})

    assert payload["ok"] is True
    assert payload["populate_evidence"]["contract_status"] == "pass"
    assert _count(storage, "SELECT COUNT(*) FROM events WHERE source = ? AND event_type = ?", (source["source_key"], "rss_article")) == 1


def test_populate_now_broker_readonly_feed_never_calls_order_paths(populate_context, monkeypatch) -> None:
    storage, manager_mod, manager, routes = populate_context
    key_canary = f"codex-canary-alpaca-key-{uuid.uuid4().hex}"
    secret_canary = f"codex-canary-alpaca-secret-{uuid.uuid4().hex}"
    manager.update_provider_account(
        {
            "account_key": "alpaca_data",
            "credentials": {"key_id": key_canary, "secret_key": secret_canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    manager.update_source({"source_key": "alpaca_broker_data", "enabled": True, "actor": "unit-test"})
    calls: list[str] = []

    def fake_get(url, **_kwargs):
        calls.append(str(url))
        if str(url).endswith("/v2/account"):
            return _JsonResponse({"id": "acct-unit", "status": "ACTIVE"})
        if str(url).endswith("/v2/positions"):
            return _JsonResponse(
                [
                    {
                        "symbol": "AAPL",
                        "qty": "2",
                        "avg_entry_price": "100",
                        "current_price": "101",
                        "market_value": "202",
                        "unrealized_pl": "2",
                        "side": "long",
                    }
                ]
            )
        raise AssertionError(f"unexpected Alpaca path {url}")

    monkeypatch.setattr(manager_mod.requests, "get", fake_get)
    monkeypatch.setattr(
        manager_mod.requests,
        "post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("alpaca_post_forbidden")),
    )
    payload = routes.api_post_data_source_populate_now(None, {"source_key": "alpaca_broker_data", "actor": "unit-test"})

    assert payload["ok"] is True
    assert payload["populate_evidence"]["contract_status"] == "pass"
    assert {call.rsplit("/", 2)[-2] + "/" + call.rsplit("/", 2)[-1] for call in calls} == {"v2/account", "v2/positions"}
    assert all("/v2/orders" not in call for call in calls)
    assert _count(storage, "SELECT COUNT(*) FROM broker_connection_health WHERE broker = ?", ("alpaca",)) == 1
    assert _count(storage, "SELECT COUNT(*) FROM broker_positions WHERE symbol = ?", ("AAPL",)) == 1
    _assert_canary_absent(payload, key_canary, secret_canary)
    _assert_canary_absent(routes.api_get_data_sources(None), key_canary, secret_canary)


def test_runtime_health_blocks_healthy_without_landed_rows_or_passing_contract(populate_context) -> None:
    _storage, manager_mod, manager, _routes = populate_context
    source = manager.get_source("polygon")
    assert source is not None

    no_rows_source = {
        **source,
        "runnable_state": manager_mod.RUNNABLE_STATE_HEALTHY,
        "job_runnable_state": {"state": manager_mod.RUNNABLE_STATE_HEALTHY},
        "populate_evidence": None,
    }
    [no_rows_result] = manager.attach_runtime_states_to_sources(
        [no_rows_source],
        runtime_snapshot={},
        desired_jobs=[],
        job_states={},
    )

    assert no_rows_result["runnable_state"] == manager_mod.RUNNABLE_STATE_DEGRADED
    assert no_rows_result["runnable_state_reason"] == "storage_contract_no_rows"
    assert no_rows_result["contract_health_gate"]["row_count"] == 0

    failed_contract_source = {
        **source,
        "runnable_state": manager_mod.RUNNABLE_STATE_HEALTHY,
        "job_runnable_state": {"state": manager_mod.RUNNABLE_STATE_HEALTHY},
        "populate_evidence": {
            "contract_status": "fail",
            "row_count": 0,
            "storage_table": "prices",
            "error": "unit_contract_failure",
        },
    }
    [failed_contract_result] = manager.attach_runtime_states_to_sources(
        [failed_contract_source],
        runtime_snapshot={},
        desired_jobs=[],
        job_states={},
    )

    assert failed_contract_result["runnable_state"] == manager_mod.RUNNABLE_STATE_DEGRADED
    assert failed_contract_result["runnable_state_reason"] == "populate_contract_failed"
    assert failed_contract_result["contract_health_gate"]["error"] == "unit_contract_failure"
