from __future__ import annotations

from typing import Any

import pytest

from engine.runtime import storage_pg_prices


def _config(**overrides: Any) -> storage_pg_prices.PostgresPriceStorageConfig:
    values = {
        "enabled": True,
        "dsn": "postgresql://unit-test/trading",
        "schema_name": "public",
        "pool_min_size": 1,
        "pool_max_size": 2,
        "connect_timeout_s": 0.2,
        "lock_timeout_s": 0.2,
        "command_timeout_s": 1.0,
        "idle_in_txn_timeout_s": 1.0,
        "retry_attempts": 1,
        "retry_base_s": 0.01,
        "retry_max_s": 0.01,
        "application_name": "unit-price-storage",
    }
    values.update(overrides)
    return storage_pg_prices.PostgresPriceStorageConfig(**values)


def test_price_storage_uses_psycopg3_connection_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[Any] = []

    class FakePool:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = dict(kwargs)
            self.open_calls: list[tuple[bool, float]] = []
            self.close_calls: list[float] = []
            created.append(self)

        def open(self, *, wait: bool, timeout: float) -> None:
            self.open_calls.append((bool(wait), float(timeout)))

        def close(self, *, timeout: float) -> None:
            self.close_calls.append(float(timeout))

    monkeypatch.setattr(storage_pg_prices, "psycopg", object())
    monkeypatch.setattr(storage_pg_prices, "ConnectionPool", FakePool)
    monkeypatch.setattr(
        storage_pg_prices.PostgresPriceStorage,
        "ensure_schema",
        lambda self: self.get_snapshot(),
    )

    store = storage_pg_prices.PostgresPriceStorage(_config())
    store.start()
    store.close()

    assert len(created) == 1
    pool = created[0]
    assert pool.kwargs["conninfo"] == "postgresql://unit-test/trading"
    assert pool.kwargs["min_size"] == 1
    assert pool.kwargs["max_size"] == 2
    assert pool.kwargs["kwargs"]["connect_timeout"] == 1
    assert pool.kwargs["kwargs"]["application_name"] == "unit-price-storage"
    assert pool.kwargs["open"] is False
    assert pool.open_calls == [(True, 0.2)]
    assert pool.close_calls == [0.2]


def test_execute_many_values_renders_psycopg3_executemany() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, list[tuple[Any, ...]]]] = []

        def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            self.calls.append((str(sql), list(rows)))

    cur = FakeCursor()

    storage_pg_prices._execute_many_values(
        cur,
        "INSERT INTO public.price_ticks(symbol, last) VALUES %s "
        "ON CONFLICT(symbol) DO UPDATE SET last=EXCLUDED.last",
        [("SPY", 500.0), ("QQQ", 430.0)],
    )

    assert len(cur.calls) == 1
    sql, rows = cur.calls[0]
    assert "VALUES (%s, %s)" in sql
    assert "ON CONFLICT(symbol)" in sql
    assert rows == [("SPY", 500.0), ("QQQ", 430.0)]


def test_prepare_connection_sets_timeouts_without_bind_parameters() -> None:
    executions: list[tuple[str, tuple[Any, ...] | None]] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
            executions.append((str(sql), tuple(params) if params is not None else None))

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

    store = storage_pg_prices.PostgresPriceStorage(
        _config(command_timeout_s=2.5, lock_timeout_s=0.2, idle_in_txn_timeout_s=3.0)
    )
    store._prepare_connection(FakeConnection())

    assert executions[:5] == [
        ("SET SESSION statement_timeout = 2500", None),
        ("SET SESSION lock_timeout = 1000", None),
        ("SET SESSION idle_in_transaction_session_timeout = 3000", None),
        ("SET SESSION TIME ZONE 'UTC'", None),
        ("SELECT 1", None),
    ]
    assert all("$1" not in sql and "%s" not in sql for sql, _params in executions[:3])


def test_failed_price_storage_connection_rolls_back_and_discards(monkeypatch: pytest.MonkeyPatch) -> None:
    statements: list[str] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
            del params
            statements.append(str(sql))

    class FakeConnection:
        def __init__(self) -> None:
            self.autocommit = True
            self.rollbacks = 0
            self.closed = False

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def rollback(self) -> None:
            self.rollbacks += 1

        def close(self) -> None:
            self.closed = True

    class FakePool:
        def __init__(self, conn: FakeConnection) -> None:
            self.conn = conn
            self.get_timeout: float | None = None
            self.returned: FakeConnection | None = None

        def getconn(self, *, timeout: float) -> FakeConnection:
            self.get_timeout = float(timeout)
            return self.conn

        def putconn(self, conn: FakeConnection) -> None:
            self.returned = conn

    conn = FakeConnection()
    pool = FakePool(conn)
    store = storage_pg_prices.PostgresPriceStorage(_config())
    monkeypatch.setattr(store, "_pool", pool)

    with pytest.raises(RuntimeError, match="unit failure"):
        with store._connection():
            raise RuntimeError("unit failure")

    assert pool.get_timeout == 0.2
    assert conn.autocommit is False
    assert conn.rollbacks == 1
    assert conn.closed is True
    assert pool.returned is conn
    assert statements[-1] == "SELECT 1"
