from __future__ import annotations

import importlib
import re
from contextlib import contextmanager
from typing import Any

from engine.runtime import price_timescale_schema, storage_pg_prices
from engine.runtime.price_timescale_schema import (
    PRICE_TIMESCALE_BASELINE_TABLE_DEFS,
    PRICE_TIMESCALE_COPY_TYPES,
    PRICE_TIMESCALE_SCHEMA_INDEXES,
    PRICE_TIMESCALE_STAGING_TABLE_COLUMNS,
    PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS,
    PRICE_TIMESCALE_STAGING_TABLE_NAMES,
    PRICE_TIMESCALE_TABLES,
    PRICE_TIMESCALE_TABLE_COLUMNS,
    price_timescale_table_body,
    price_timescale_time_after_ms_predicate,
    price_timescale_time_ref,
    price_timescale_ts_ms_expr,
)
from engine.runtime.schema.table_classification import TABLE_CLASS


def _normalize_sql(sql: str) -> str:
    return " ".join(str(sql).split()).lower()


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


def test_baseline_migration_uses_canonical_timescale_price_sidecar_defs() -> None:
    baseline = importlib.import_module("engine.runtime.schema.migrations.0001_baseline")
    baseline_defs = {str(name): str(body) for name, body in baseline.TABLE_DEFS}

    assert dict(PRICE_TIMESCALE_BASELINE_TABLE_DEFS) == {
        table_name: price_timescale_table_body(table_name)
        for table_name in PRICE_TIMESCALE_TABLES
    }
    for table_name in PRICE_TIMESCALE_TABLES:
        assert _normalize_sql(baseline_defs[table_name]) == _normalize_sql(
            price_timescale_table_body(table_name)
        )
        normalized_body = _normalize_sql(baseline_defs[table_name])
        assert '"time" timestamptz not null' in normalized_body
        assert not re.search(r'(^|[,\s"]+)ts_ms\s+bigint', normalized_body)
        assert "primary key(symbol, ts_ms)" not in normalized_body

    assert "ts_ms bigint not null" in _normalize_sql(baseline_defs["prices"])


def test_storage_pg_prices_imports_canonical_timescale_price_metadata() -> None:
    assert storage_pg_prices._PG_PRICE_HYPERTABLE_TABLES == PRICE_TIMESCALE_TABLES
    assert storage_pg_prices._PG_PRICE_SCHEMA_TABLE_COLUMNS == PRICE_TIMESCALE_TABLE_COLUMNS
    assert storage_pg_prices._PG_PRICE_STAGING_TABLE_NAMES == PRICE_TIMESCALE_STAGING_TABLE_NAMES
    assert storage_pg_prices._PG_PRICE_STAGING_TABLE_COLUMN_SPECS == PRICE_TIMESCALE_STAGING_TABLE_COLUMN_SPECS
    assert storage_pg_prices._PG_PRICE_STAGING_TABLE_COLUMNS == PRICE_TIMESCALE_STAGING_TABLE_COLUMNS
    assert storage_pg_prices._PG_PRICE_COPY_TYPES == PRICE_TIMESCALE_COPY_TYPES
    assert storage_pg_prices._PG_PRICE_SCHEMA_INDEXES == PRICE_TIMESCALE_SCHEMA_INDEXES
    assert storage_pg_prices.price_timescale_create_table_sql is price_timescale_schema.price_timescale_create_table_sql


def test_storage_pg_validation_uses_canonical_price_sidecar_contract() -> None:
    from engine.runtime import storage_pg

    required_columns, _required_indexes = storage_pg._validation_contract()
    owned_specs, owned_indexes = storage_pg._postgres_owned_live_table_contract()

    assert "time" in required_columns["price_quotes"]
    assert "ts_ms" not in required_columns["price_quotes"]
    assert "time" in required_columns["price_quotes_raw"]
    assert "ts_ms" not in required_columns["price_quotes_raw"]
    assert owned_specs["price_quotes"]["time"]["pk"] == 2
    assert owned_specs["price_quotes_raw"]["time"]["pk"] == 4
    assert owned_indexes["price_quotes"] == ("idx_price_quotes_time_desc",)
    assert owned_indexes["price_quotes_raw"] == ("idx_price_quotes_raw_time_desc",)


def test_timescale_timestamp_helpers_project_api_ts_ms_without_schema_column() -> None:
    assert price_timescale_time_ref() == '"time"'
    assert price_timescale_time_ref("q") == '"q"."time"'
    assert price_timescale_ts_ms_expr() == '(EXTRACT(EPOCH FROM "time") * 1000)::BIGINT'
    assert (
        price_timescale_time_after_ms_predicate(placeholder="%s")
        == '"time" > TO_TIMESTAMP(%s / 1000.0)'
    )
    assert (
        price_timescale_time_after_ms_predicate(table_alias="q", placeholder="$1")
        == '"q"."time" > TO_TIMESTAMP($1 / 1000.0)'
    )


def test_price_sidecar_ensure_schema_emits_canonical_create_table_sql(monkeypatch) -> None:
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
            if "FROM pg_constraint" in text:
                self._fetchone = ("price_quotes_raw_pkey", ["symbol", "provider", "event_key", "time"])
            elif "FROM information_schema.tables" in text:
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
                self._fetchone = ("1 day", 86_400_000)

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

    emitted_tables: list[str] = []
    original_create_sql = price_timescale_schema.price_timescale_create_table_sql

    def spy_create_table_sql(relation_ref: str, table_name: str) -> str:
        emitted_tables.append(str(table_name))
        return original_create_sql(relation_ref, table_name)

    con = FakeConnection()

    @contextmanager
    def fake_connection(_self: storage_pg_prices.PostgresPriceStorage):
        yield con

    monkeypatch.setattr(storage_pg_prices.PostgresPriceStorage, "_connection", fake_connection)
    monkeypatch.setattr(storage_pg_prices, "price_timescale_create_table_sql", spy_create_table_sql)
    monkeypatch.setattr(storage_pg_prices, "emit_gauge", lambda *args, **kwargs: None)
    monkeypatch.setattr(storage_pg_prices, "record_component_health", lambda *args, **kwargs: None)

    store = storage_pg_prices.PostgresPriceStorage(_config())
    snapshot = store.ensure_schema()

    assert snapshot["schema_ready"] is True
    assert emitted_tables == list(PRICE_TIMESCALE_TABLES)
    create_sql = "\n".join(sql for sql, _params in con.cursor_obj.calls if "CREATE TABLE IF NOT EXISTS" in sql)
    for table_name in PRICE_TIMESCALE_TABLES:
        expected = original_create_sql(f'"public".{table_name}', table_name)
        assert _normalize_sql(expected) in _normalize_sql(create_sql)
    assert not re.search(r'(^|[,\s"]+)ts_ms\s+bigint', _normalize_sql(create_sql))
    assert "primary key(symbol, ts_ms)" not in _normalize_sql(create_sql)


def test_timescale_price_compat_migration_converts_legacy_ts_ms_tables(monkeypatch) -> None:
    migration = importlib.import_module(
        "engine.runtime.schema.migrations.0068_canonical_timescale_price_sidecar_schema"
    )
    hypertable_calls: list[tuple[str, str]] = []

    class FakeHypertables:
        @staticmethod
        def _create_hypertable(_conn, table_name: str, spec: Any) -> None:
            hypertable_calls.append((str(table_name), str(getattr(spec, "time_column", ""))))

    monkeypatch.setattr(migration.importlib, "import_module", lambda _name: FakeHypertables)

    class FakeCursor:
        def __init__(self, row: tuple[Any, ...] | None = None) -> None:
            self.row = row

        def fetchone(self) -> tuple[Any, ...] | None:
            return self.row

    class FakeConn:
        def __init__(self) -> None:
            self.tables = {"prices", "price_quotes", "price_quotes_raw"}
            self.columns = {
                "prices": {"ts_ms", "symbol", "price", "px", "source"},
                "price_quotes": {
                    "ts_ms",
                    "symbol",
                    "last",
                    "bid",
                    "ask",
                    "spread",
                    "volume",
                    "source",
                    "last_trade_ts_ms",
                    "last_quote_ts_ms",
                    "last_update_ts_ms",
                },
                "price_quotes_raw": {
                    "ts_ms",
                    "symbol",
                    "provider",
                    "event_key",
                    "event_type",
                    "event_ts_ms",
                    "last",
                    "bid",
                    "ask",
                    "spread",
                    "volume",
                    "trade_ts_ms",
                    "quote_ts_ms",
                    "ingest_ts_ms",
                    "source",
                },
            }
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> FakeCursor:
            text = str(sql)
            self.calls.append((text, tuple(params or ())))
            if "FROM pg_constraint" in text:
                table = str((params or ("",))[0])
                if table == "price_quotes_legacy_ts_ms":
                    return FakeCursor(("price_quotes_pkey", ["symbol", "ts_ms"]))
                if table == "price_quotes_raw_legacy_ts_ms":
                    return FakeCursor(("price_quotes_raw_pkey", ["symbol", "provider", "event_key", "ts_ms"]))
                return FakeCursor()
            if "FROM pg_class c" in text and "JOIN pg_namespace" in text:
                table = str((params or ("",))[0])
                return FakeCursor(("public",) if table in self.tables else None)
            if "to_regclass" in text:
                table = str((params or ("",))[0])
                return FakeCursor((table,) if table in self.tables else (None,))
            if "information_schema.columns" in text:
                if len(params or ()) >= 3:
                    _schema, table, column = params[:3]
                else:
                    table = str((params or ("", ""))[0])
                    column = str((params or ("", ""))[1])
                return FakeCursor((1,) if column in self.columns.get(table, set()) else None)
            match = re.search(r'ALTER TABLE "([^"]+)" RENAME TO "([^"]+)"', text)
            if match:
                old, new = match.groups()
                self.tables.discard(old)
                self.tables.add(new)
                self.columns[new] = set(self.columns.pop(old, set()))
            return FakeCursor()

    conn = FakeConn()
    migration.up(conn)
    sql = "\n".join(text for text, _params in conn.calls)

    assert 'ALTER TABLE "price_quotes" RENAME TO "price_quotes_legacy_ts_ms"' in sql
    assert 'RENAME CONSTRAINT "price_quotes_pkey" TO "price_quotes_legacy_ts_ms_pkey"' in sql
    assert 'ALTER TABLE "price_quotes_raw" RENAME TO "price_quotes_raw_legacy_ts_ms"' in sql
    assert 'RENAME CONSTRAINT "price_quotes_raw_pkey" TO "price_quotes_raw_legacy_ts_ms_pkey"' in sql
    assert 'CREATE TABLE IF NOT EXISTS "price_ticks"' in sql
    assert 'CREATE TABLE IF NOT EXISTS "price_quotes"' in sql
    assert '"time" TIMESTAMPTZ NOT NULL' in sql
    assert "INSERT INTO price_ticks" in sql
    assert "INSERT INTO price_quotes(" in sql
    assert "INSERT INTO price_quotes_raw(" in sql
    assert "to_timestamp((ts_ms)::double precision / 1000.0)" in sql
    assert sorted(hypertable_calls) == [
        ("price_quotes", "time"),
        ("price_quotes_raw", "time"),
        ("price_ticks", "time"),
    ]


def test_timescale_price_compat_migration_checks_columns_on_resolved_schema() -> None:
    migration = importlib.import_module(
        "engine.runtime.schema.migrations.0068_canonical_timescale_price_sidecar_schema"
    )

    class FakeCursor:
        def __init__(self, row: tuple[Any, ...] | None = None) -> None:
            self.row = row

        def fetchone(self) -> tuple[Any, ...] | None:
            return self.row

    class FakeConn:
        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> FakeCursor:
            text = str(sql)
            if "SELECT to_regclass" in text:
                return FakeCursor(("price_quotes",))
            if "FROM pg_class c" in text and "JOIN pg_namespace" in text:
                return FakeCursor(("trading",))
            if "FROM information_schema.columns" in text and "table_schema = ?" in text:
                schema, table, column = params
                columns = {
                    ("public", "price_quotes"): {"time", "symbol"},
                    ("trading", "price_quotes"): {"ts_ms", "symbol"},
                }
                return FakeCursor((1,) if str(column) in columns.get((str(schema), str(table)), set()) else None)
            raise AssertionError(f"unexpected SQL: {text}")

    conn = FakeConn()

    assert migration._column_exists(conn, "price_quotes", "ts_ms") is True
    assert migration._column_exists(conn, "price_quotes", "time") is False
    assert TABLE_CLASS["price_quotes"].time_column == "time"
    assert TABLE_CLASS["price_quotes_raw"].time_column == "time"
    assert TABLE_CLASS["prices"].time_column == "ts_ms"
