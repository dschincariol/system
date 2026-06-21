from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest

from engine.runtime import storage_pg, storage_pg_prices, timescale_client
from engine.runtime.pg_durability import SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL


def test_runtime_refetchable_telemetry_wrapper_sets_local_when_relaxed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    class FakeConnection:
        def execute(self, sql: str, params: Any = None) -> None:
            del params
            statements.append(str(sql))

        def commit(self) -> None:
            statements.append("COMMIT")

        def rollback(self) -> None:
            statements.append("ROLLBACK")

        def close(self) -> None:
            statements.append("CLOSE")

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")
    monkeypatch.setattr(storage_pg, "connect", lambda readonly=False, timeout_s=None: FakeConnection())

    storage_pg.run_refetchable_ingestion_telemetry_txn(
        lambda con: con.execute("INSERT INTO price_provider_health VALUES (...)"),
        table="price_provider_health",
        operation="flush_price_provider_health_buffer",
        attempts=1,
    )

    assert statements[0] == SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL
    assert "INSERT INTO price_provider_health" in statements[1]
    assert "COMMIT" in statements


@pytest.mark.parametrize(
    ("table", "operation"),
    (
        ("execution_orders", "record_execution_order"),
        ("trade_attribution_ledger", "append_ledger_row"),
        ("broker_order_state", "update_broker_order_state"),
        ("event_log", "append_audit_event"),
        ("equity_history", "record_capital_state"),
        ("capital_preservation_audit", "record_capital_audit"),
    ),
)
def test_runtime_protected_write_txn_keeps_default_durability_when_relaxed_env_is_set(
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    operation: str,
) -> None:
    statements: list[str] = []

    class FakeConnection:
        def execute(self, sql: str, params: Any = None) -> None:
            del params
            statements.append(str(sql))

        def commit(self) -> None:
            statements.append("COMMIT")

        def rollback(self) -> None:
            statements.append("ROLLBACK")

        def close(self) -> None:
            statements.append("CLOSE")

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")
    monkeypatch.setattr(storage_pg, "connect", lambda readonly=False, timeout_s=None: FakeConnection())

    storage_pg.run_write_txn(
        lambda con: con.execute(f"INSERT INTO {table} VALUES (...)"),
        table=table,
        operation=operation,
        attempts=1,
    )

    assert SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL not in statements
    assert statements[0].startswith(f"INSERT INTO {table}")
    assert "COMMIT" in statements


@pytest.mark.parametrize(
    ("table", "operation"),
    (
        ("execution_orders", "record_execution_order"),
        ("trade_attribution_ledger", "append_ledger_row"),
        ("broker_order_state", "update_broker_order_state"),
        ("event_log", "append_audit_event"),
        ("equity_history", "record_capital_state"),
        ("capital_preservation_audit", "record_capital_audit"),
    ),
)
def test_refetchable_telemetry_wrapper_rejects_protected_financial_tables(
    table: str,
    operation: str,
) -> None:
    with pytest.raises(ValueError, match="unapproved_refetchable_ingestion_telemetry_write"):
        storage_pg.run_refetchable_ingestion_telemetry_txn(
            lambda con: con.execute(f"INSERT INTO {table} VALUES (...)"),
            table=table,
            operation=operation,
            attempts=1,
        )


def _price_config() -> storage_pg_prices.PostgresPriceStorageConfig:
    return storage_pg_prices.PostgresPriceStorageConfig(
        enabled=True,
        dsn="postgresql://unit-test/trading",
        schema_name="public",
        pool_min_size=1,
        pool_max_size=1,
        connect_timeout_s=0.2,
        lock_timeout_s=0.2,
        command_timeout_s=1.0,
        idle_in_txn_timeout_s=1.0,
        retry_attempts=1,
        retry_base_s=0.01,
        retry_max_s=0.01,
        application_name="unit-price-storage",
    )


def test_price_storage_write_batch_sets_local_only_inside_price_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: Any = None) -> None:
            del params
            statements.append(str(sql))

        def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            statements.append(str(sql))
            statements.append(f"ROWS:{len(rows)}")

    class FakeConnection:
        def __init__(self) -> None:
            self.autocommit = False

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            statements.append("COMMIT")

        def rollback(self) -> None:
            statements.append("ROLLBACK")

    class FakePool:
        def __init__(self, conn: FakeConnection) -> None:
            self.conn = conn

        def getconn(self, *, timeout: float) -> FakeConnection:
            del timeout
            return self.conn

        def putconn(self, conn: FakeConnection) -> None:
            assert conn is self.conn

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")
    store = storage_pg_prices.PostgresPriceStorage(_price_config())
    monkeypatch.setattr(store, "_pool", FakePool(FakeConnection()))
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())

    result = store.write_batch(
        prices=[
            {
                "symbol": "SPY",
                "ts_ms": 1_700_000_000_000,
                "price": 500.0,
                "source": "unit",
            }
        ]
    )

    assert result["ok"] is True
    assert SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL in statements
    assert statements.index(SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL) < next(
        idx for idx, statement in enumerate(statements) if "INSERT INTO" in statement
    )


def _timescale_config() -> timescale_client.TimescaleConfig:
    return timescale_client.TimescaleConfig(
        enabled=True,
        dsn="postgres://example",
        schema_name="public",
        pool_min_size=1,
        pool_max_size=1,
        batch_size=10,
        flush_interval_s=0.5,
        queue_maxsize=32,
        retry_attempts=1,
        retry_base_s=0.1,
        retry_max_s=0.1,
        backpressure_timeout_s=1.0,
        start_timeout_s=1.0,
        connect_timeout_s=1.0,
        lock_timeout_s=1.0,
        command_timeout_s=5.0,
        idle_in_txn_timeout_s=30.0,
        application_name="unit-test",
    )


def test_timescale_price_flush_sets_local_when_relaxed(monkeypatch: pytest.MonkeyPatch) -> None:
    statements: list[str] = []

    class FakeTransaction:
        async def __aenter__(self) -> None:
            statements.append("BEGIN")

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            statements.append("END")

    class FakeConnection:
        def transaction(self) -> FakeTransaction:
            return FakeTransaction()

        async def execute(self, sql: str, *params: Any) -> None:
            del params
            statements.append(str(sql))

        async def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            statements.append(str(sql))
            statements.append(f"ROWS:{len(rows)}")

    class FakeAcquire:
        def __init__(self, conn: FakeConnection) -> None:
            self.conn = conn

        async def __aenter__(self) -> FakeConnection:
            return self.conn

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class FakePool:
        def __init__(self, conn: FakeConnection) -> None:
            self.conn = conn

        def acquire(self) -> FakeAcquire:
            return FakeAcquire(self.conn)

    async def fake_ensure_schema() -> None:
        return None

    async def fake_ensure_pool() -> FakePool:
        return FakePool(FakeConnection())

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")
    client = timescale_client.TimescaleClient(_timescale_config())
    monkeypatch.setattr(client, "_ensure_schema", fake_ensure_schema)
    monkeypatch.setattr(client, "_ensure_pool", fake_ensure_pool)
    rows = [("SPY", datetime(2024, 1, 1, tzinfo=timezone.utc), 1.0, 2.0, 0.5, 1.5, 1000.0)]

    assert asyncio.run(client._flush_with_retry("price_data", rows)) is True
    assert statements[:2] == ["BEGIN", SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL]
    assert any("INSERT INTO" in statement and "price_data" in statement for statement in statements)


def test_timescale_financial_table_keeps_default_durability_when_relaxed_env_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    class FakeTransaction:
        async def __aenter__(self) -> None:
            statements.append("BEGIN")

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            statements.append("END")

    class FakeConnection:
        def transaction(self) -> FakeTransaction:
            return FakeTransaction()

        async def execute(self, sql: str, *params: Any) -> None:
            del params
            statements.append(str(sql))

        async def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            statements.append(str(sql))
            statements.append(f"ROWS:{len(rows)}")

    class FakeAcquire:
        async def __aenter__(self) -> FakeConnection:
            return FakeConnection()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    class FakePool:
        def acquire(self) -> FakeAcquire:
            return FakeAcquire()

    async def fake_ensure_schema() -> None:
        return None

    async def fake_ensure_pool() -> FakePool:
        return FakePool()

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")
    client = timescale_client.TimescaleClient(_timescale_config())
    monkeypatch.setattr(client, "_ensure_schema", fake_ensure_schema)
    monkeypatch.setattr(client, "_ensure_pool", fake_ensure_pool)
    rows = [("trade-1", datetime(2024, 1, 1, tzinfo=timezone.utc), 0.0, "filled")]

    assert asyncio.run(client._flush_with_retry("trade_outcomes", rows)) is True
    assert SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL not in statements
    assert statements[0] == "BEGIN"
    assert any("INSERT INTO" in statement and "trade_outcomes" in statement for statement in statements)
