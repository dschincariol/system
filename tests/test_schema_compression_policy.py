from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TESTS_DIR = ROOT / "tests"
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))

from engine.runtime.schema.table_classification import Hypertable, TABLE_CLASS
from test_schema_hypertable_creation import _existing_classified_hypertables, _prepare_db


def test_main_migration_generates_compression_orderby_for_real_time_column(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = __import__(
        "engine.runtime.schema.migrations.0002_hypertables",
        fromlist=["_enable_compression"],
    )

    class FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> "FakeConn":
            self.calls.append((str(sql), tuple(params or ())))
            return self

    spec = TABLE_CLASS["price_data"]
    assert isinstance(spec, Hypertable)
    conn = FakeConn()
    monkeypatch.setattr(migration, "_table_exists", lambda _conn, _table: True)
    monkeypatch.setattr(migration, "_is_hypertable", lambda _conn, _table: True)
    monkeypatch.setattr(migration, "_existing_columns", lambda _conn, _table, columns: tuple(columns))
    monkeypatch.setattr(migration, "_is_integer_time", lambda _conn, _table, _column: False)

    migration._enable_compression(conn, "price_data", spec)

    alter_sql = next(sql for sql, _params in conn.calls if "ALTER TABLE" in sql)
    assert 'timescaledb.compress_orderby = \'"timestamp" DESC\'' in alter_sql
    assert 'timescaledb.compress_segmentby = \'"symbol"\'' in alter_sql
    assert any(
        "add_compression_policy" in sql and params == ("price_data", "7 days")
        for sql, params in conn.calls
    )


def test_compression_orderby_retrofit_migration_reapplies_compression_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = __import__(
        "engine.runtime.schema.migrations.0066_timescale_compression_orderby",
        fromlist=["up"],
    )
    hypertables = __import__(
        "engine.runtime.schema.migrations.0002_hypertables",
        fromlist=["_enable_compression"],
    )
    calls: list[tuple[str, str | None]] = []

    class FakeConn:
        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> "FakeConn":
            del params
            calls.append(("execute", str(sql)))
            return self

    def fake_enable_compression(_conn: Any, table_name: str, spec: Hypertable) -> None:
        calls.append((str(table_name), spec.compress_after))

    monkeypatch.delenv("TRADING_UNIT_TEST_SCHEMA_FAST", raising=False)
    monkeypatch.setattr(hypertables, "_enable_compression", fake_enable_compression)

    migration.up(FakeConn())

    assert ("execute", "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE") in calls
    assert ("price_data", "7 days") in calls
    assert ("runtime_metrics", "14 days") in calls
    assert ("trade_attribution_ledger", None) in calls


def test_compression_policy_exists_for_each_compressed_hypertable() -> None:
    storage_pg = _prepare_db()
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        expected = {
            table_name
            for table_name, classification in _existing_classified_hypertables(conn).items()
            if classification.compress_after
        }
        rows = conn.execute(
            """
            SELECT hypertable_name
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_compression'
              AND hypertable_schema = ANY (current_schemas(false))
            """
        ).fetchall()
        actual = {str(row[0]) for row in rows or []}
        missing = sorted(expected - actual)
        assert not missing, "Compression policy missing for: " + ", ".join(missing)


def test_compliance_ledger_has_no_compression_policy() -> None:
    storage_pg = _prepare_db()
    ledger = TABLE_CLASS["trade_attribution_ledger"]
    assert isinstance(ledger, Hypertable)
    with storage_pg.connect_ro_direct(timeout_s=1) as conn:
        rows = conn.execute(
            """
            SELECT 1
            FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_compression'
              AND hypertable_schema = ANY (current_schemas(false))
              AND hypertable_name = 'trade_attribution_ledger'
            """
        ).fetchall()
        assert rows == []
