from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from engine.runtime import storage_pg, storage_pg_prices, telemetry_append_buffer, timescale_client
from engine.runtime.pg_durability import (
    SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL,
    RefetchablePgDurabilityConfigError,
    RefetchablePgDurabilityScopeError,
    maybe_apply_async_refetchable_pg_durability,
    maybe_apply_sync_refetchable_pg_durability,
    parse_refetchable_pg_durability_tier,
    protected_refetchable_pg_durability_tables,
    refetchable_pg_durability_snapshot,
    refetchable_pg_durability_tier,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    ("raw", "expected"),
    (
        (None, "default"),
        ("", "default"),
        ("   ", "default"),
        ("default", "default"),
        (" DEFAULT ", "default"),
        ("relaxed", "relaxed"),
        (" RELAXED ", "relaxed"),
    ),
)
def test_refetchable_pg_durability_tier_parsing(raw: str | None, expected: str) -> None:
    env = {} if raw is None else {"TRADING_REFETCHABLE_PG_DURABILITY_TIER": raw}

    assert parse_refetchable_pg_durability_tier(env) == expected


def test_refetchable_pg_durability_tier_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relax")

    with pytest.raises(RefetchablePgDurabilityConfigError, match="TRADING_REFETCHABLE_PG_DURABILITY_TIER"):
        refetchable_pg_durability_tier()


def test_refetchable_pg_durability_snapshot_exposes_default_and_allowlists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", raising=False)

    snapshot = refetchable_pg_durability_snapshot()

    assert snapshot["tier"] == "default"
    assert snapshot["relaxed"] is False
    assert snapshot["supported_tiers"] == ["default", "relaxed"]
    assert "storage_pg_prices.write_batch" in snapshot["approved_scopes"]
    assert "price_ticks" in snapshot["approved_price_storage_tables"]
    assert "price_data" in snapshot["approved_timescale_tables"]
    assert "execution_orders" in snapshot["protected_tables"]


def test_prod_preflight_refetchable_pg_durability_gate_reports_relaxed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine.runtime import prod_preflight

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")

    notes, warnings, errors, state = prod_preflight._refetchable_pg_durability_gate()

    assert warnings == []
    assert errors == []
    assert state["tier"] == "relaxed"
    assert state["relaxed"] is True
    assert any("refetchable pg durability ok tier=relaxed" in note for note in notes)


def test_prod_preflight_refetchable_pg_durability_gate_rejects_invalid_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine.runtime import prod_preflight

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "unsafe")

    notes, warnings, errors, state = prod_preflight._refetchable_pg_durability_gate()

    assert notes == []
    assert warnings == []
    assert state == {}
    assert any("refetchable pg durability invalid" in error for error in errors)


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


def test_runtime_refetchable_telemetry_wrapper_keeps_default_without_relaxed_env(
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

    monkeypatch.delenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", raising=False)
    monkeypatch.setattr(storage_pg, "connect", lambda readonly=False, timeout_s=None: FakeConnection())

    storage_pg.run_refetchable_ingestion_telemetry_txn(
        lambda con: con.execute("INSERT INTO price_provider_health VALUES (...)"),
        table="price_provider_health",
        operation="flush_price_provider_health_buffer",
        attempts=1,
    )

    assert SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL not in statements
    assert statements[0].startswith("INSERT INTO price_provider_health")
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
    with pytest.raises(RefetchablePgDurabilityScopeError, match="protected_refetchable_pg_durability_table"):
        storage_pg.run_refetchable_ingestion_telemetry_txn(
            lambda con: con.execute(f"INSERT INTO {table} VALUES (...)"),
            table=table,
            operation=operation,
            attempts=1,
        )


@pytest.mark.parametrize(
    "target_tables",
    (
        (),
        ("execution_orders",),
        ("price_ticks", "trade_attribution_ledger"),
        ("price_quotes", "event_log"),
    ),
)
def test_postgres_price_storage_scope_rejects_unapproved_target_tables(
    monkeypatch: pytest.MonkeyPatch,
    target_tables: tuple[str, ...],
) -> None:
    statements: list[str] = []

    class FakeExecutor:
        def execute(self, sql: str) -> None:
            statements.append(str(sql))

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")

    with pytest.raises(
        RefetchablePgDurabilityScopeError,
        match="unapproved_refetchable_postgres_price_write|protected_refetchable_pg_durability_table",
    ):
        maybe_apply_sync_refetchable_pg_durability(
            FakeExecutor(),
            scope="storage_pg_prices.write_batch",
            target_tables=target_tables,
        )

    assert SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL not in statements


def test_runtime_relaxed_helper_rejects_protected_table_directly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    class FakeExecutor:
        def execute(self, sql: str) -> None:
            statements.append(str(sql))

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")

    with pytest.raises(RefetchablePgDurabilityScopeError, match="protected_refetchable_pg_durability_table"):
        maybe_apply_sync_refetchable_pg_durability(
            FakeExecutor(),
            scope="runtime_refetchable_ingestion_telemetry",
            table="risk_state",
            operation="set_state",
        )

    assert SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL not in statements


def test_timescale_relaxed_helper_rejects_protected_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    class FakeExecutor:
        async def execute(self, sql: str) -> None:
            statements.append(str(sql))

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")

    with pytest.raises(RefetchablePgDurabilityScopeError, match="protected_refetchable_pg_durability_table"):
        asyncio.run(
            maybe_apply_async_refetchable_pg_durability(
                FakeExecutor(),
                scope="timescale_price_telemetry",
                table="execution_orders",
            )
        )

    assert SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL not in statements


def test_timescale_relaxed_helper_allows_approved_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    class FakeExecutor:
        async def execute(self, sql: str) -> None:
            statements.append(str(sql))

    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")

    applied = asyncio.run(
        maybe_apply_async_refetchable_pg_durability(
            FakeExecutor(),
            scope="timescale_price_telemetry",
            table="price_data",
        )
    )

    assert applied is True
    assert statements == [SET_LOCAL_SYNCHRONOUS_COMMIT_OFF_SQL]


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


def test_telemetry_append_buffer_snapshot_exposes_refetchable_durability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")

    snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()

    assert snapshot["durability"]["tier"] == "relaxed"
    assert snapshot["durability"]["relaxed"] is True
    assert "price_provider_health:flush_price_provider_health_buffer" in snapshot["durability"][
        "approved_runtime_telemetry_writes"
    ]


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


def test_refetchable_pg_durability_docs_and_env_allowlists_are_consistent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRADING_REFETCHABLE_PG_DURABILITY_TIER", "relaxed")
    snapshot = refetchable_pg_durability_snapshot()

    database_map = (REPO_ROOT / "docs" / "README_DATABASE_MAP.md").read_text(encoding="utf-8")
    production_checklist = (REPO_ROOT / "docs" / "PRODUCTION_CHECKLIST.md").read_text(encoding="utf-8")
    glossary = (REPO_ROOT / "docs" / "REFERENCE_CONFIGURATION_GLOSSARY.md").read_text(encoding="utf-8")
    env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    compose_env_example = (REPO_ROOT / "deploy" / "compose" / ".env.example").read_text(encoding="utf-8")
    compose_stack = (REPO_ROOT / "deploy" / "compose" / "docker-compose.stack.yml").read_text(encoding="utf-8")
    legacy_allowlist = (REPO_ROOT / "docs" / "config_env_allowlist.txt").read_text(encoding="utf-8")

    assert "TRADING_REFETCHABLE_PG_DURABILITY_TIER=default" in env_example
    assert "TRADING_REFETCHABLE_PG_DURABILITY_TIER=relaxed" in compose_env_example
    assert "TRADING_REFETCHABLE_PG_DURABILITY_TIER: ${TRADING_REFETCHABLE_PG_DURABILITY_TIER:-relaxed}" in compose_stack
    assert "TRADING_REFETCHABLE_PG_DURABILITY_TIER" in glossary
    assert "\nTRADING_REFETCHABLE_PG_DURABILITY_TIER\n" not in legacy_allowlist
    assert "refetchable_pg_durability" in production_checklist
    assert "default" in env_example and "relaxed" in env_example

    for table in snapshot["approved_price_storage_tables"]:
        assert table in database_map
    for table in snapshot["approved_timescale_tables"]:
        assert table in database_map
    for write in snapshot["approved_runtime_telemetry_writes"]:
        table, _operation = str(write).split(":", 1)
        assert table in database_map

    protected_tables = protected_refetchable_pg_durability_tables()
    for table in (
        "broker_order_state",
        "equity_history",
        "event_log",
        "execution_orders",
        "model_registry",
        "risk_state",
        "trade_attribution_ledger",
        "trade_outcomes",
    ):
        assert table in protected_tables
        assert table in database_map
