import importlib
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def test_health_snapshot_singleflight_returns_prompt_degraded(monkeypatch):
    health = importlib.reload(importlib.import_module("engine.runtime.health"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setitem(health._HEALTH_SNAPSHOT_CACHE, "ts_ms", 0)
    monkeypatch.setitem(health._HEALTH_SNAPSHOT_CACHE, "payload", None)
    monkeypatch.setattr(
        health,
        "_db_connect",
        lambda: (_ for _ in ()).throw(AssertionError("db connect should not run while refresh is in flight")),
    )

    assert health._HEALTH_SNAPSHOT_REFRESH_LOCK.acquire(blocking=False)
    try:
        started = time.perf_counter()
        snapshot = health.get_health_snapshot()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
    finally:
        health._HEALTH_SNAPSHOT_REFRESH_LOCK.release()

    assert elapsed_ms < 100
    assert snapshot["ok"] is False
    assert snapshot["cache"]["refresh_in_flight"] is True
    assert snapshot["execution_barrier"]["real_trading_allowed"] is False


def test_repeated_readiness_probes_coalesce_health_refresh(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")

    with api_system._HEALTH_CACHE_LOCK:
        api_system._HEALTH_CACHE["ts_ms"] = 0
        api_system._HEALTH_CACHE["payload"] = None
    with api_system._HEALTH_CACHE_REFRESH_LOCK:
        api_system._HEALTH_CACHE_REFRESH_IN_FLIGHT = False
    with api_system._SYSTEM_SNAPSHOT_CACHE_LOCK:
        api_system._SYSTEM_SNAPSHOT_CACHE["ts_ms"] = 0
        api_system._SYSTEM_SNAPSHOT_CACHE["payload"] = None

    started = threading.Event()
    release = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def slow_health_snapshot():
        nonlocal calls
        with calls_lock:
            calls += 1
        started.set()
        release.wait(timeout=1.0)
        return {
            "ok": False,
            "reasons": ["warming"],
            "lifecycle": {"state": "WARMING_UP", "detail": "warming"},
            "execution_barrier": {
                "ok": True,
                "allowed": False,
                "mode": "safe",
                "reason": "health_fast_path",
                "real_trading_allowed": False,
            },
        }

    monkeypatch.setattr(api_system, "get_health_snapshot", slow_health_snapshot)
    monkeypatch.setattr(
        api_system,
        "_build_system_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("full system snapshot should not run")),
    )
    monkeypatch.setattr(
        api_system,
        "api_get_runtime_watchdogs",
        lambda *_args, **_kwargs: {"ok": False, "status": "warming"},
    )
    ctx = {
        "_boot_diagnostics": lambda: {
            "storage": {
                "checked": True,
                "ok": True,
                "status": "ready",
                "detail": "ok",
            }
        }
    }

    try:
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(api_system.api_get_readiness, {}, ctx) for _ in range(16)]
            assert started.wait(timeout=1.0)
            results = [future.result(timeout=1.0) for future in futures]
    finally:
        release.set()

    assert calls == 1
    assert all(result["ok"] is False for result in results)
    assert all(result["execution_allowed"] is False for result in results)


def test_degraded_storage_readiness_returns_prompt_failure(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setattr(
        api_system,
        "_build_readiness_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("degraded storage should short-circuit")),
    )
    monkeypatch.setattr(api_system, "_cached_health_snapshot", lambda *, allow_sync_on_miss=True: {})

    started = time.perf_counter()
    response = api_system.api_get_readiness(
        {},
        {
            "_boot_diagnostics": lambda: {
                "storage": {
                    "checked": True,
                    "ok": False,
                    "status": "unavailable",
                    "detail": "pool_timeout",
                    "error_type": "StoragePoolTimeout",
                }
            }
        },
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0

    assert elapsed_ms < 100
    assert response["ok"] is False
    assert response["failed"] is True
    assert response["execution_allowed"] is False
    assert response["reasons"] == ["storage_unavailable"]


def test_operator_trading_readiness_is_prompt_during_warmup(monkeypatch):
    api_system = importlib.reload(importlib.import_module("engine.api.api_system"))
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "paper")
    monkeypatch.setattr(
        api_system,
        "_cached_health_snapshot",
        lambda *, allow_sync_on_miss=True: {
            "ok": False,
            "reasons": ["warming"],
            "lifecycle": {"state": "WARMING_UP", "detail": "awaiting_first_price_tick"},
            "execution_barrier": {
                "ok": True,
                "allowed": False,
                "mode": "safe",
                "reason": "health_fast_path",
                "real_trading_allowed": False,
            },
        },
    )
    monkeypatch.setattr(
        api_system,
        "api_get_runtime_watchdogs",
        lambda *_args, **_kwargs: {"ok": False, "status": "warming"},
    )

    started = time.perf_counter()
    response = api_system.api_get_trading_readiness(
        {},
        {
            "_boot_diagnostics": lambda: {
                "storage": {
                    "checked": True,
                    "ok": True,
                    "status": "ready",
                    "detail": "ok",
                }
            }
        },
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0

    assert elapsed_ms < 100
    assert response["ok"] is False
    assert response["ready"] is False
    assert response["execution_allowed"] is False


def test_storage_connection_close_closes_tracked_cursors(monkeypatch):
    storage_pg = importlib.import_module("engine.runtime.storage_pg")
    released = []

    class _Info:
        transaction_status = storage_pg.TransactionStatus.IDLE

    class _Cursor:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    class _Raw:
        info = _Info()

        def __init__(self):
            self.cursor_obj = _Cursor()

        def cursor(self):
            return self.cursor_obj

        def rollback(self):
            raise AssertionError("idle connection should not rollback")

    raw = _Raw()
    monkeypatch.setattr(storage_pg, "release", lambda value: released.append(value))

    con = storage_pg.StorageConnection(raw, readonly=True)
    cursor = con.cursor()

    con.close()

    assert raw.cursor_obj.closed is True
    assert released == [raw]
    cursor.close()
