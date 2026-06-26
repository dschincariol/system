from __future__ import annotations

import base64
import importlib
import json
import time
import uuid
from pathlib import Path


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload_runtime(monkeypatch, tmp_path, *, max_age_s: int = 120):
    monkeypatch.setenv("DB_PATH", str(tmp_path / f"sim_prices_{uuid.uuid4().hex}.sqlite"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TIMESCALE_PRICES_ENABLED", "0")
    monkeypatch.setenv("TELEMETRY_READ_BACKEND", "sqlite")
    monkeypatch.setenv("TELEMETRY_READ_REQUIRE_VALIDATION", "0")
    monkeypatch.setenv("PRICE_READ_BACKEND", "sqlite")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("BROKER", "sim")
    monkeypatch.setenv("BROKER_NAME", "sim")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("SIMULATED_MARKET_DATA_ENABLED", "1")
    monkeypatch.setenv("SIMULATED_MARKET_DATA_SYMBOLS", "SPY,AAPL")
    monkeypatch.setenv("HEALTH_PRICES_MAX_AGE_S", str(max_age_s))
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)

    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    importlib.reload(importlib.import_module("engine.runtime.price_router"))
    sim_ingestion = importlib.reload(importlib.import_module("engine.data.simulated_price_ingestion"))
    api_read = importlib.reload(importlib.import_module("engine.api.api_read"))
    health = importlib.reload(importlib.import_module("engine.runtime.health"))
    state_cache = importlib.import_module("engine.runtime.state_cache")
    state_cache.cache_invalidate_namespace("api_read", prefix="feed_status")
    storage.init_db()
    return storage, sim_ingestion, api_read, health


def _count(storage, sql: str, params: tuple = ()) -> int:
    con = storage.connect_ro_direct()
    try:
        row = con.execute(sql, params).fetchone()
        return int((row or [0])[0] or 0)
    finally:
        con.close()


def test_safe_simulated_ingestion_writes_fresh_prices_and_api_freshness(monkeypatch, tmp_path, caplog) -> None:
    storage, sim_ingestion, api_read, health = _reload_runtime(monkeypatch, tmp_path)
    canary = f"codex-canary-sim-ingest-{uuid.uuid4().hex}"
    monkeypatch.setenv("POLYGON_API_KEY", canary)

    now_ms = int(time.time() * 1000)
    result = sim_ingestion.run_simulated_price_ingestion_once(symbols=["SPY", "AAPL"], ts_ms=now_ms)

    assert result["ok"] is True
    assert result["provider"] == "simulated"
    assert result["simulated"] is True
    assert _count(storage, "SELECT COUNT(*) FROM prices WHERE source = ?", ("simulated",)) >= 2
    assert _count(storage, "SELECT COUNT(*) FROM price_quotes WHERE source = ?", ("simulated",)) >= 2
    assert _count(storage, "SELECT COUNT(*) FROM price_quotes_raw WHERE provider = ?", ("simulated",)) >= 2

    feed = api_read.get_feed_status()
    assert feed["price_freshness"]["ok"] is True
    assert feed["price_freshness"]["status"] == "fresh"
    assert feed["price_freshness"]["source"] == "simulated"
    assert feed["price_freshness"]["simulated"] is True
    assert feed["price_freshness"]["live_market_data_ok"] is False
    assert feed["price_freshness"]["live_feed_status"] == "simulated"

    con = storage.connect_ro_direct()
    try:
        ctx = health.HealthSnapshotContext(con=con, now_ms=now_ms + 1000, out={"db": {}})
        health._check_prices(ctx)
    finally:
        con.close()
    assert ctx.out["prices"]["ok"] is True
    assert ctx.out["prices"]["status"] == "fresh"
    assert ctx.out["prices"]["simulated"] is True
    assert ctx.out["prices"]["live_market_data_ok"] is False
    assert ctx.out["prices"]["live_feed_status"] == "simulated"

    rendered = json.dumps({"result": result, "feed": feed, "health": ctx.out}, sort_keys=True, default=str)
    assert canary not in rendered
    assert canary not in caplog.text


def test_missing_real_provider_credentials_degrade_without_network(monkeypatch, tmp_path) -> None:
    storage, _sim_ingestion, _api_read, _health = _reload_runtime(monkeypatch, tmp_path)
    manager_mod = importlib.reload(importlib.import_module("services.data_source_manager"))
    manager = manager_mod.DataSourceManager()
    manager.initialize()
    manager_mod._MANAGER = manager

    def fail_get(*_args, **_kwargs):
        raise AssertionError("network should not be called when credentials are missing")

    monkeypatch.setattr(manager_mod.requests, "get", fail_get)
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    monkeypatch.delenv("POLYGON_API_KEY_FILE", raising=False)

    result = manager.test_connection("polygon", actor="unit-test")

    assert result["ok"] is False
    assert result["classification"] == "missing_credentials"
    assert result["message"] == "polygon_credentials_missing"
    assert result["evidence"]["missing_env_vars"] == ["POLYGON_API_KEY"]
    assert result["missing_credential_env_vars"] == ["POLYGON_API_KEY"]
    assert result["live_market_data_ok"] is False
    storage.close_pooled_connections()


def test_simulated_connection_test_is_not_live_green_and_lists_missing_live_credentials(monkeypatch, tmp_path) -> None:
    storage, _sim_ingestion, _api_read, _health = _reload_runtime(monkeypatch, tmp_path)
    for env_name in (
        "POLYGON_API_KEY",
        "POLYGON_API_KEY_FILE",
        "POLYGON_KEY",
        "POLYGON_KEY_FILE",
        "TRADIER_API_TOKEN",
        "TRADIER_API_TOKEN_FILE",
    ):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setenv("DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_S", "0")
    manager_mod = importlib.reload(importlib.import_module("services.data_source_manager"))
    manager = manager_mod.DataSourceManager()
    manager.initialize()
    manager_mod._MANAGER = manager

    result = manager.test_connection("simulated", actor="unit-test")

    assert result["ok"] is False
    assert result["status"] == "degraded"
    assert result["classification"] == "simulated_not_live"
    assert result["simulated"] is True
    assert result["live_market_data_ok"] is False
    assert result["live_feed_status"] == "simulated"
    assert "POLYGON_API_KEY" in result["missing_credential_env_vars"]
    assert "TRADIER_API_TOKEN" in result["missing_credential_env_vars"]
    storage.close_pooled_connections()


def test_stale_price_data_is_reported_as_stale_by_health_and_api(monkeypatch, tmp_path) -> None:
    storage, sim_ingestion, api_read, health = _reload_runtime(monkeypatch, tmp_path, max_age_s=30)
    now_ms = int(time.time() * 1000)
    old_ts_ms = now_ms - 120_000

    result = sim_ingestion.run_simulated_price_ingestion_once(symbols=["SPY"], ts_ms=old_ts_ms)
    assert result["ok"] is True

    feed = api_read.get_feed_status()
    assert feed["price_freshness"]["ok"] is False
    assert feed["price_freshness"]["status"] == "stale"
    assert feed["price_freshness"]["stale"] is True
    assert feed["price_freshness"]["source"] == "simulated"

    con = storage.connect_ro_direct()
    try:
        ctx = health.HealthSnapshotContext(con=con, now_ms=now_ms, out={"db": {}})
        health._check_prices(ctx)
    finally:
        con.close()
    assert ctx.out["prices"]["ok"] is False
    assert ctx.out["prices"]["status"] == "stale"
    assert ctx.out["prices"]["stale"] is True


def test_poll_prices_safe_chain_appends_simulated_and_classifies_provider_errors(monkeypatch) -> None:
    poll_prices = importlib.reload(importlib.import_module("engine.data.poll_prices"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("BROKER", "sim")
    monkeypatch.setenv("BROKER_NAME", "sim")
    monkeypatch.setenv("SIMULATED_MARKET_DATA_ENABLED", "1")

    assert poll_prices._append_simulated_provider_fallback(["yfinance"]) == ["yfinance", "simulated"]
    assert poll_prices._classify_provider_error("RuntimeError: POLYGON_API_KEY not set") == "missing_credentials"
    assert poll_prices._classify_provider_error("HTTP 429 rate limit") == "rate_limited"
    assert poll_prices._classify_provider_error("TimeoutError: provider timeout") == "transient_network"

    captured: dict[str, object] = {}

    class Manager:
        def record_job_status(self, *_args, **kwargs):
            captured["job_meta"] = dict(kwargs.get("meta") or {})

    monkeypatch.setattr(
        poll_prices,
        "record_pipeline_status",
        lambda _name, **kwargs: {"meta": dict(kwargs.get("meta") or {})},
    )

    status = poll_prices._record_poll_prices_status(
        Manager(),
        ok=True,
        providers=["polygon", "simulated"],
        provider_errors={"polygon": "RuntimeError: POLYGON_API_KEY not set"},
        provider_result_counts={"polygon": 0, "simulated": 2},
        price_rows=2,
        quote_rows=2,
    )

    assert status["meta"]["provider_error_classifications"]["polygon"] == "missing_credentials"
    assert status["meta"]["provider_result_counts"]["simulated"] == 2
    assert captured["job_meta"]["provider_error_classifications"]["polygon"] == "missing_credentials"


def test_dashboard_data_health_uses_feed_freshness_contract() -> None:
    data_health_js = (
        Path(__file__).resolve().parents[1] / "ui" / "data_health.js"
    ).read_text(encoding="utf-8")

    assert 'feedStatus: Object.freeze({ path: "/api/feeds" })' in data_health_js
    assert "price_freshness" in data_health_js
    assert "dataFreshnessPill" in data_health_js
    assert "priceFreshness.simulated" in data_health_js
    assert "live_market_data_ok" in data_health_js
    assert "missing_credential_env_vars" in data_health_js
    assert "prices ${priceFreshnessStatus}" in data_health_js
