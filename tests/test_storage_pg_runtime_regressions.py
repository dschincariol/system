from __future__ import annotations

import sys
import threading
import time
import types
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime import storage_pg
from engine.runtime import storage_pool


def _reset_autoinit_state() -> None:
    storage_pg._AUTO_INIT_SCHEMAS.clear()
    storage_pg._AUTO_INIT_ACTIVE_SCHEMAS.clear()
    storage_pg._AUTO_INIT_LOCKS.clear()


def test_storage_pg_param_rewrite_cache_is_not_id_keyed():
    assert not hasattr(storage_pg, "_PARAM_CACHE")
    assert storage_pg._normalize_sql("SELECT '?' AS literal WHERE symbol=?") == (
        "SELECT '?' AS literal WHERE symbol=%s"
    )


def test_init_db_runs_concurrently_for_different_schemas(monkeypatch):
    _reset_autoinit_state()
    entered = threading.Barrier(2)
    calls: list[tuple[str, str, float]] = []
    errors: list[BaseException] = []
    calls_lock = threading.Lock()

    def fake_apply_migrations() -> list[int]:
        schema = storage_pool.schema_name()
        with calls_lock:
            calls.append(("enter", schema, time.perf_counter()))
        entered.wait(timeout=2.0)
        time.sleep(0.05)
        with calls_lock:
            calls.append(("exit", schema, time.perf_counter()))
        return [1]

    ledger_module = types.ModuleType("engine.execution.execution_ledger")
    ledger_module.init_execution_ledger = lambda: None

    monkeypatch.setattr(storage_pg, "apply_migrations", fake_apply_migrations)
    monkeypatch.setattr(storage_pg, "_ensure_sqlite_compat_bigints", lambda: None)
    monkeypatch.setitem(sys.modules, "engine.execution.execution_ledger", ledger_module)

    def worker(schema: str) -> None:
        try:
            storage_pg.init_db(schema)
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=("schema_a",)),
        threading.Thread(target=worker, args=("schema_b",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3.0)

    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    enters = [schema for event, schema, _ in calls if event == "enter"]
    assert sorted(enters) == ["schema_a", "schema_b"]
    assert {"schema_a", "schema_b"}.issubset(storage_pg._AUTO_INIT_SCHEMAS)


def test_autoinit_rechecks_when_schema_name_changes(monkeypatch):
    _reset_autoinit_state()
    schemas = iter(("schema_a", "schema_b"))
    calls: list[str | None] = []

    def fake_init_db(schema: str | None = None):
        calls.append(schema)
        if schema is not None:
            storage_pg._AUTO_INIT_SCHEMAS.add(schema)
        return []

    monkeypatch.setenv("TRADING_PG_AUTOINIT_ON_CONNECT", "1")
    monkeypatch.setattr(storage_pool, "schema_name", lambda: next(schemas))
    monkeypatch.setattr(storage_pg, "init_db", fake_init_db)

    storage_pg._ensure_autoinit_schema()
    storage_pg._ensure_autoinit_schema()

    assert calls == ["schema_a", "schema_b"]


def test_get_pool_resolves_dsn_before_taking_pool_lock(monkeypatch):
    monkeypatch.setattr(storage_pool, "_POOL", None)
    lock_was_held: list[bool] = []

    def fake_dsn() -> str:
        acquired = storage_pool._POOL_LOCK.acquire(blocking=False)
        if acquired:
            storage_pool._POOL_LOCK.release()
        lock_was_held.append(not acquired)
        return "host=127.0.0.1 port=5432 user=ts_app dbname=trading password=test"

    class FakePool:
        def __init__(self, **kwargs):
            self.kwargs = dict(kwargs)
            self.opened = False

        def open(self, *, wait: bool, timeout: float) -> None:
            self.opened = True
            self.wait = wait
            self.timeout = timeout

    monkeypatch.setattr(storage_pool, "_dsn", fake_dsn)
    monkeypatch.setattr(storage_pool, "ConnectionPool", FakePool)
    monkeypatch.setattr(storage_pool, "_pool_timeout_s", lambda: 0.25)
    monkeypatch.setattr(storage_pool, "default_pool_size", lambda: 1)

    pool = storage_pool.get_pool()

    assert lock_was_held == [False]
    assert isinstance(pool, FakePool)
    assert pool.opened is True
    assert pool.kwargs["conninfo"].startswith("host=127.0.0.1")


def test_get_db_validation_snapshot_strict_raises(monkeypatch):
    class BrokenConnection:
        def __enter__(self):
            raise RuntimeError("db down")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(storage_pg, "connection", lambda readonly=True: BrokenConnection())

    snapshot = storage_pg.get_db_validation_snapshot()
    assert snapshot["ok"] is False
    assert "RuntimeError: db down" in snapshot["error"]

    with pytest.raises(RuntimeError, match="db down"):
        storage_pg.get_db_validation_snapshot(strict=True)
