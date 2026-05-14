from __future__ import annotations

import importlib
import threading

from engine.runtime.timeseries_write_policy import PriceRouterWritePlan


class _NoSqlitePricePolicy:
    require_async_during_cutover = False

    def validate_high_volume_runtime(self):
        return {"ok": True, "reason": "ok"}

    def plan_price_router_writes(self, *, write_prices: bool, write_quotes: bool, write_raw: bool):
        return PriceRouterWritePlan(
            sqlite_write_prices=False,
            sqlite_write_quotes=False,
            sqlite_write_raw=False,
            async_required=bool(write_prices or write_quotes),
        )

    def price_persistence_mode(self, *, async_price_writer_enabled: bool):
        return {"async_price_writer_enabled": bool(async_price_writer_enabled)}


class _FakeAsyncWriter:
    enabled = True


class _FakePostgresStorage:
    __module__ = "engine.runtime.storage_pg"

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql, params=()):
        del params
        self.statements.append(str(sql))
        return self


class _BusyJobs:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs = {}

    def list_jobs(self, *, timeout_s=None, include_persisted=True):
        del include_persisted
        if timeout_s is not None:
            raise TimeoutError("jobs_manager_lock_timeout")
        raise AssertionError("api path should use a bounded jobs listing")


def test_postgres_symbol_price_status_update_uses_jsonb_not_sqlite_json(monkeypatch):
    price_router = importlib.reload(importlib.import_module("engine.runtime.price_router"))
    db = _FakePostgresStorage()

    price_router._LAST_EVENT_KEY_BY_STREAM.clear()
    price_router._LAST_EVENT_TS_BY_STREAM.clear()
    monkeypatch.setattr(price_router, "get_timeseries_write_policy", lambda: _NoSqlitePricePolicy())
    monkeypatch.setattr(price_router, "get_async_writer", lambda: _FakeAsyncWriter())
    monkeypatch.setattr(price_router, "register_after_commit", lambda _db, _callback: None)
    monkeypatch.setattr(price_router, "publish_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(price_router, "emit_counter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(price_router, "record_component_health", lambda *_args, **_kwargs: None)

    counts = price_router.publish_price_events(
        [
            {
                "symbol": "SPY",
                "provider": "yfinance",
                "source": "yfinance",
                "timestamp": 1_800_000_000_000,
                "last": 500.25,
            }
        ],
        con=db,
        write_prices=False,
        write_quotes=False,
        write_raw=False,
        emit_telemetry=False,
        update_symbols=True,
    )

    assert counts["events"] == 1
    sql = "\n".join(db.statements).lower()
    assert "jsonb_set" in sql
    assert "to_jsonb" in sql
    assert "json_set(" not in sql


def test_readiness_degrades_instead_of_blocking_on_busy_jobs(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))

    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *args, **kwargs: {
            "ok": False,
            "status": "BOOTING",
            "reasons": ["health_snapshot_pending"],
            "execution_barrier": {"ok": True, "allowed": False, "mode": "safe"},
            "lifecycle": {"state": "BOOTING", "detail": "health_snapshot_pending"},
        },
    )
    monkeypatch.setattr(api_system, "_get_supervisor_graph", lambda *_args, **_kwargs: {"ok": False, "error": "supervisor_missing"})
    monkeypatch.setattr(api_system, "api_get_runtime_watchdogs", lambda *_args, **_kwargs: {"ok": False})

    response = api_system.api_get_readiness(
        {},
        {
            "JOBS": _BusyJobs(),
            "_boot_diagnostics": lambda: {"storage": {"checked": True, "ok": True, "status": "ready"}},
        },
    )

    assert response["ok"] is False
    assert response["ready"] is False
    assert response["execution_allowed"] is False
    assert "jobs_list_timeout" in response["reasons"]
