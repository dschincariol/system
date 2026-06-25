from __future__ import annotations

import importlib
from typing import Any


class _FakeResult:
    def __init__(self, row: tuple[Any, ...] | None = None) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._row


class _FakeCursor:
    def __init__(self, raw: "_FakeRaw") -> None:
        self.raw = raw

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.raw.statements.append((str(sql), tuple(params or ())))


class _FakeRaw:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[Any, ...]]] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)


class _FakeConnection:
    def __init__(self) -> None:
        self.raw = _FakeRaw()
        self.operations: list[tuple[str, tuple[Any, ...]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> _FakeResult:
        self.operations.append((str(sql), tuple(params or ())))
        return _FakeResult()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _assert_set_config_timeout(statements: list[tuple[str, tuple[Any, ...]]], value: str) -> None:
    assert ("SELECT set_config('lock_timeout', %s, true)", (value,)) in statements
    assert ("SELECT set_config('statement_timeout', %s, true)", (value,)) in statements
    assert not any("SET LOCAL" in sql and "%s" in sql for sql, _params in statements)


def test_apply_write_txn_timeouts_uses_parameterized_set_config() -> None:
    storage_pg = importlib.reload(importlib.import_module("engine.runtime.storage_pg"))
    con = _FakeConnection()

    storage_pg._apply_write_txn_timeouts(con, timeout_s=0.5)

    _assert_set_config_timeout(con.raw.statements, "500ms")


def test_apply_write_txn_timeouts_disabled_emits_no_timeout_sql(monkeypatch) -> None:
    storage_pg = importlib.reload(importlib.import_module("engine.runtime.storage_pg"))
    monkeypatch.delenv("TS_PG_WRITE_LOCK_TIMEOUT_S", raising=False)
    monkeypatch.delenv("TS_PG_WRITE_STATEMENT_TIMEOUT_S", raising=False)
    con = _FakeConnection()

    storage_pg._apply_write_txn_timeouts(con, timeout_s=None)

    assert con.raw.statements == []


def test_run_write_txn_with_timeout_commits_after_set_config(monkeypatch) -> None:
    storage_pg = importlib.reload(importlib.import_module("engine.runtime.storage_pg"))
    connections: list[_FakeConnection] = []

    def _connect(*, readonly: bool = False, timeout_s: float | None = None) -> _FakeConnection:
        assert readonly is False
        assert timeout_s == 0.5
        con = _FakeConnection()
        connections.append(con)
        return con

    monkeypatch.setattr(storage_pg, "connect", _connect)

    result = storage_pg.run_write_txn(
        lambda con: con.execute("SELECT 1"),
        attempts=1,
        timeout_s=0.5,
    )

    assert isinstance(result, _FakeResult)
    assert len(connections) == 1
    con = connections[0]
    _assert_set_config_timeout(con.raw.statements, "500ms")
    assert con.operations == [("SELECT 1", ())]
    assert con.commits == 1
    assert con.rollbacks == 0
    assert con.closed is True


def test_locks_pg_acquire_and_release_use_bounded_write_txn_timeout(monkeypatch) -> None:
    storage_pg = importlib.reload(importlib.import_module("engine.runtime.storage_pg"))
    locks_pg = importlib.reload(importlib.import_module("engine.runtime.locks_pg"))
    connections: list[_FakeConnection] = []

    def _connect(*, readonly: bool = False, timeout_s: float | None = None) -> _FakeConnection:
        assert readonly is False
        con = _FakeConnection()
        connections.append(con)
        return con

    monkeypatch.setattr(storage_pg, "connect", _connect)
    monkeypatch.setattr(locks_pg, "run_write_txn", storage_pg.run_write_txn)

    assert locks_pg.acquire_lock("unit-timeout-lock", ttl_ms=1_000) is True
    locks_pg.release_lock("unit-timeout-lock")

    timed_connections = [con for con in connections if con.raw.statements]
    assert len(timed_connections) == 2
    for con in timed_connections:
        _assert_set_config_timeout(con.raw.statements, "500ms")
        assert con.commits == 1
        assert con.closed is True

    operations = [sql for con in connections for sql, _params in con.operations]
    assert any("INSERT INTO job_locks" in sql for sql in operations)
    assert any("DELETE FROM job_locks" in sql for sql in operations)
