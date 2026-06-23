from __future__ import annotations

import asyncio
import importlib
from contextlib import contextmanager
from typing import Any

import pytest

from engine.runtime import storage_pg, storage_pg_prices, timescale_client
from engine.runtime.schema.table_classification import (
    TABLE_CLASS,
    hypertable_chunk_interval,
    hypertable_chunk_interval_ms,
)


def _price_config(**overrides: Any) -> storage_pg_prices.PostgresPriceStorageConfig:
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


def _timescale_config(**overrides: Any) -> timescale_client.TimescaleConfig:
    values = {
        "enabled": True,
        "dsn": "postgres://example",
        "schema_name": "public",
        "pool_min_size": 1,
        "pool_max_size": 1,
        "batch_size": 10,
        "flush_interval_s": 0.5,
        "queue_maxsize": 32,
        "retry_attempts": 1,
        "retry_base_s": 0.1,
        "retry_max_s": 1.0,
        "backpressure_timeout_s": 1.0,
        "start_timeout_s": 1.0,
        "connect_timeout_s": 1.0,
        "lock_timeout_s": 1.0,
        "command_timeout_s": 5.0,
        "idle_in_txn_timeout_s": 30.0,
        "application_name": "unit-test",
    }
    values.update(overrides)
    return timescale_client.TimescaleConfig(**values)


def test_table_policy_defines_daily_high_rate_and_weekly_low_rate_chunks() -> None:
    assert hypertable_chunk_interval("price_ticks") == "1 day"
    assert hypertable_chunk_interval("price_quotes_raw") == "1 day"
    assert hypertable_chunk_interval("price_data") == "1 day"
    assert hypertable_chunk_interval("runtime_metrics") == "1 day"
    assert hypertable_chunk_interval("price_provider_health") == "1 day"
    assert hypertable_chunk_interval("feature_data") == "1 week"
    assert hypertable_chunk_interval("model_predictions") == "1 week"
    assert hypertable_chunk_interval("credential_access_log") == "1 week"
    assert hypertable_chunk_interval_ms("price_ticks") == 86_400_000
    assert hypertable_chunk_interval_ms("feature_data") == 604_800_000


def test_main_migration_resets_existing_hypertable_chunk_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0002_hypertables")

    class FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> "FakeConn":
            self.calls.append((str(sql), tuple(params or ())))
            return self

    conn = FakeConn()
    monkeypatch.setattr(migration, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(migration, "_column_exists", lambda _conn, _table, _column: True)
    monkeypatch.setattr(migration, "_is_hypertable", lambda _conn, _table: True)
    monkeypatch.setattr(migration, "_is_integer_time", lambda _conn, _table, _column: False)

    migration._create_hypertable(conn, "price_ticks", TABLE_CLASS["price_ticks"])

    rendered_sql = "\n".join(sql for sql, _params in conn.calls)
    assert "create_hypertable" not in rendered_sql
    assert "set_chunk_time_interval" in rendered_sql
    assert ("price_ticks", "1 day") in [params for _sql, params in conn.calls]


def test_main_migration_uses_integer_policy_interval_for_epoch_ms_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0002_hypertables")

    class FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> "FakeConn":
            self.calls.append((str(sql), tuple(params or ())))
            return self

    conn = FakeConn()
    monkeypatch.setattr(migration, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(migration, "_column_exists", lambda _conn, _table, _column: True)
    monkeypatch.setattr(migration, "_is_hypertable", lambda _conn, _table: True)
    monkeypatch.setattr(migration, "_is_integer_time", lambda _conn, _table, _column: True)

    migration._create_hypertable(conn, "prices", TABLE_CLASS["prices"])

    assert any(
        "set_chunk_time_interval" in sql and params == ("prices", 86_400_000)
        for sql, params in conn.calls
    )


def test_hypertable_primary_key_rewrite_uses_full_key_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0002_hypertables")

    class FakeConn:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.last_params: tuple[Any, ...] = ()

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> "FakeConn":
            self.calls.append(str(sql))
            self.last_params = tuple(params or ())
            return self

        def fetchone(self) -> tuple[str, str] | None:
            if self.last_params == ("pk_price_quotes_raw_symbol_provider_event_key_ts_ms",):
                return ("i", "price_quotes_raw_legacy_exact_once")
            return None

    conn = FakeConn()
    monkeypatch.setattr(
        migration,
        "_unique_constraints",
        lambda _conn, _table: [
            ("price_quotes_raw_pkey", "p", ("symbol", "provider", "event_key"))
        ],
    )
    monkeypatch.setattr(migration, "_unique_indexes", lambda _conn, _table: [])

    migration._normalize_constraints_for_hypertable(conn, "price_quotes_raw", "ts_ms")

    add_constraint_sql = [sql for sql in conn.calls if "ADD CONSTRAINT" in sql][0]
    assert "pk_price_quotes_raw_symbol_provider_event_key_ts_ms_2" in add_constraint_sql
    assert "PRIMARY KEY (\"symbol\", \"provider\", \"event_key\", \"ts_ms\")" in add_constraint_sql


def test_chunk_interval_retrofit_migration_creates_missing_classified_hypertables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = importlib.import_module("engine.runtime.schema.migrations.0065_hypertable_chunk_intervals")
    hypertables = importlib.import_module("engine.runtime.schema.migrations.0002_hypertables")
    calls: list[tuple[str, Any]] = []

    class FakeConn:
        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> "FakeConn":
            calls.append(("execute", str(sql)))
            return self

    monkeypatch.delenv("TRADING_UNIT_TEST_SCHEMA_FAST", raising=False)
    monkeypatch.setattr(hypertables, "_create_integer_now_func", lambda _conn: calls.append(("now", None)))
    monkeypatch.setattr(
        hypertables,
        "_create_hypertable",
        lambda _conn, table_name, spec: calls.append((str(table_name), spec.chunk)),
    )

    migration.up(FakeConn())

    assert ("price_ticks", "1 day") in calls
    assert ("price_quotes_raw", "1 day") in calls
    assert ("feature_data", "1 week") in calls
    assert not any(call[0] == "_set_chunk_interval" for call in calls)


def test_price_quotes_raw_conflict_migration_aligns_primary_key() -> None:
    migration = importlib.import_module(
        "engine.runtime.schema.migrations.0067_price_quotes_raw_event_key_conflict"
    )

    class FakeCursor:
        def __init__(self, row: tuple[Any, ...] | None = None) -> None:
            self.row = row

        def fetchone(self) -> tuple[Any, ...] | None:
            return self.row

    class FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> FakeCursor:
            text = str(sql)
            self.calls.append((text, tuple(params or ())))
            if "FROM pg_attribute" in text:
                return FakeCursor((1,))
            if "FROM pg_constraint" in text:
                return FakeCursor(("price_quotes_raw_pkey", ["symbol", "provider", "event_key"]))
            if "to_regclass" in text:
                return FakeCursor(("price_quotes_raw",))
            return FakeCursor()

    conn = FakeConn()
    migration.up(conn)

    sql = "\n".join(text for text, _params in conn.calls)
    assert "PRIMARY KEY(symbol, provider, event_key, ts_ms)" in sql
    assert 'DROP CONSTRAINT IF EXISTS "price_quotes_raw_pkey"' in sql
    update_sql = next(text for text, _params in conn.calls if "UPDATE price_quotes_raw" in text)
    assert " last" not in update_sql
    assert " bid" not in update_sql
    assert " ask" not in update_sql
    assert "volume" not in update_sql


def test_backend_compat_labels_price_uses_classified_hypertable_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import_module = importlib.import_module
    calls: list[tuple[str, str, Any]] = []

    class FakeHypertables:
        @staticmethod
        def _create_integer_now_func(conn: Any) -> None:
            calls.append(("now", "", conn))

        @staticmethod
        def _create_hypertable(conn: Any, table_name: str, spec: Any) -> None:
            calls.append(("hypertable", str(table_name), spec.chunk))

    def fake_import_module(name: str, package: str | None = None) -> Any:
        if name == "engine.runtime.schema.migrations.0002_hypertables":
            return FakeHypertables
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    conn = object()
    storage_pg._ensure_classified_hypertable(conn, "labels_price")

    assert ("now", "", conn) in calls
    assert ("hypertable", "labels_price", hypertable_chunk_interval("labels_price")) in calls


def test_price_sidecar_schema_uses_policy_chunks_and_records_actual_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []
            self._fetchall: list[tuple[Any, ...]] = []
            self._fetchone: tuple[Any, ...] | None = None

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
            text = str(sql)
            self.calls.append((text, tuple(params or ())))
            if "FROM information_schema.tables" in text:
                self._fetchall = [
                    (table_name,)
                    for table_name in (
                        *storage_pg_prices._PG_PRICE_SCHEMA_TABLE_COLUMNS,
                        *storage_pg_prices._PG_PRICE_STAGING_TABLE_COLUMNS,
                    )
                ]
            elif "FROM information_schema.columns" in text:
                self._fetchall = [
                    (table_name, column)
                    for table_name, columns in {
                        **storage_pg_prices._PG_PRICE_SCHEMA_TABLE_COLUMNS,
                        **storage_pg_prices._PG_PRICE_STAGING_TABLE_COLUMNS,
                    }.items()
                    for column in columns
                ]
            elif "FROM pg_indexes" in text:
                self._fetchall = [(index_name,) for index_name in storage_pg_prices._PG_PRICE_SCHEMA_INDEXES]
            elif "FROM timescaledb_information.dimensions" in text:
                self._fetchone = (hypertable_chunk_interval(params[1]), hypertable_chunk_interval_ms(params[1]))

        def fetchall(self) -> list[tuple[Any, ...]]:
            return list(self._fetchall)

        def fetchone(self) -> tuple[Any, ...] | None:
            return self._fetchone

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

    con = FakeConnection()
    gauges: list[tuple[str, int, dict[str, Any]]] = []

    @contextmanager
    def fake_connection(_self: storage_pg_prices.PostgresPriceStorage):
        yield con

    monkeypatch.setattr(storage_pg_prices.PostgresPriceStorage, "_connection", fake_connection)
    monkeypatch.setattr(
        storage_pg_prices,
        "emit_gauge",
        lambda metric, value, **kwargs: gauges.append((str(metric), int(value), dict(kwargs))),
    )
    monkeypatch.setattr(storage_pg_prices, "record_component_health", lambda *args, **kwargs: None)

    store = storage_pg_prices.PostgresPriceStorage(_price_config())
    snapshot = store.ensure_schema()

    calls = con.cursor_obj.calls
    assert any("create_hypertable" in sql and params[-1] == "1 day" for sql, params in calls)
    assert any(
        "set_chunk_time_interval" in sql and params == ("public.price_ticks", "1 day")
        for sql, params in calls
    )
    assert snapshot["policy_status"]["chunk_intervals"]["price_ticks"]["actual_interval_ms"] == 86_400_000
    assert snapshot["policy_status"]["chunk_intervals"]["price_quotes_raw"]["desired_interval"] == "1 day"
    assert any(metric == "storage_pg_prices_hypertable_chunk_interval_ms" for metric, _value, _kwargs in gauges)


def test_timescale_client_create_and_v5_migration_use_policy_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeAsyncConn:
        def __init__(self) -> None:
            self.execute_calls: list[tuple[str, tuple[Any, ...]]] = []
            self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []

        async def execute(self, sql: str, *args: Any) -> str:
            self.execute_calls.append((str(sql), tuple(args)))
            return "OK"

        async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any]:
            self.fetchrow_calls.append((str(sql), tuple(args)))
            table_name = str(args[1])
            return {
                "time_interval": hypertable_chunk_interval(table_name),
                "time_interval_ms": hypertable_chunk_interval_ms(table_name),
            }

    conn = FakeAsyncConn()
    gauges: list[tuple[str, int, dict[str, Any]]] = []
    monkeypatch.setattr(
        timescale_client,
        "emit_gauge",
        lambda metric, value, **kwargs: gauges.append((str(metric), int(value), dict(kwargs))),
    )
    client = timescale_client.TimescaleClient(config=_timescale_config())

    asyncio.run(client._create_hypertable(conn, "price_data"))
    asyncio.run(client._apply_migration_v5(conn))
    asyncio.run(client._record_actual_chunk_intervals(conn))

    assert any(
        "create_hypertable" in sql and args == ("public.price_data", "timestamp", "1 day")
        for sql, args in conn.execute_calls
    )
    assert any(
        "set_chunk_time_interval" in sql and args == ("public.runtime_metrics", "1 day")
        for sql, args in conn.execute_calls
    )
    assert any(
        "set_chunk_time_interval" in sql and args == ("public.feature_data", "1 week")
        for sql, args in conn.execute_calls
    )
    snapshot = client.get_snapshot()
    assert snapshot["policy_status"]["chunk_intervals"]["price_data"]["actual_interval_ms"] == 86_400_000
    assert snapshot["policy_status"]["chunk_intervals"]["feature_data"]["actual_interval_ms"] == 604_800_000
    assert any(metric == "timescale_hypertable_chunk_interval_ms" for metric, _value, _kwargs in gauges)
