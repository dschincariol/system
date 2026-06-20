from __future__ import annotations

import sys
import threading
import time
import types
from pathlib import Path

import pytest
from psycopg.pq import TransactionStatus


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


def test_configure_connection_rolls_back_dirty_connection(monkeypatch):
    statements: list[str] = []

    class FakeInfo:
        transaction_status = TransactionStatus.INTRANS

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql: str) -> None:
            statements.append(str(sql))

    class FakeConnection:
        def __init__(self) -> None:
            self.info = FakeInfo()
            self.autocommit = False
            self.rollbacks = 0

        def rollback(self) -> None:
            self.rollbacks += 1
            self.info.transaction_status = TransactionStatus.IDLE

        def cursor(self):
            return FakeCursor()

    conn = FakeConnection()
    monkeypatch.setattr(storage_pool, "_POOL_TRANSACTION_MODE", False)

    storage_pool._configure_connection(conn)  # type: ignore[arg-type]

    assert conn.rollbacks == 1
    assert conn.autocommit is False
    assert statements == [f"SET search_path = {storage_pool.quote_ident(storage_pool.schema_name())}, public"]


def test_risk_state_probe_closes_idle_connection(monkeypatch):
    from engine.runtime import risk_state

    class FakeConnection:
        in_transaction = False

        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    conn = FakeConnection()
    monkeypatch.setattr(risk_state, "connect", lambda readonly=False: conn)

    assert risk_state._active_write_txn_connection() is None
    assert conn.closed is True


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


class _FakeValidationCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeValidationConnection:
    def __init__(self, *, tables, columns, indexes, migration_ids):
        self._tables = list(tables)
        self._columns = dict(columns)
        self._indexes = list(indexes)
        self._migration_ids = list(migration_ids)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        text = str(sql)
        if "FROM information_schema.tables" in text and "table_type = 'BASE TABLE'" in text:
            return _FakeValidationCursor([(table,) for table in sorted(self._tables)])
        if "FROM information_schema.columns c" in text:
            table = str((params or ("",))[0])
            rows = []
            for name, spec in self._columns.get(table, {}).items():
                rows.append(
                    (
                        name,
                        spec.get("type", "TEXT"),
                        spec.get("udt_name", "text"),
                        1 if spec.get("notnull", False) else 0,
                        spec.get("default"),
                        int(spec.get("pk", 0) or 0),
                    )
                )
            return _FakeValidationCursor(rows)
        if "FROM pg_indexes" in text:
            return _FakeValidationCursor([(index_name,) for index_name in self._indexes])
        if "SELECT id FROM schema_migrations" in text:
            return _FakeValidationCursor([(migration_id,) for migration_id in self._migration_ids])
        raise AssertionError(f"unexpected validation SQL: {text}")


def _schema_migrations_columns():
    return {
        "id": {"type": "INTEGER", "udt_name": "int4", "pk": 1},
        "description": {"type": "TEXT", "udt_name": "text"},
        "applied_at": {"type": "TIMESTAMP WITH TIME ZONE", "udt_name": "timestamptz"},
    }


def test_postgres_validation_fails_missing_recent_migration_column_and_index(monkeypatch):
    columns = {
        "schema_migrations": _schema_migrations_columns(),
        "alert_acks": {
            "alert_id": {"type": "BIGINT", "udt_name": "int8", "pk": 1},
            "acked_ts_ms": {"type": "BIGINT", "udt_name": "int8"},
        },
        "alert_lifecycle_events": {
            "id": {"type": "BIGINT", "udt_name": "int8", "pk": 1},
            "alert_id": {"type": "BIGINT", "udt_name": "int8"},
            "ts_ms": {"type": "BIGINT", "udt_name": "int8"},
        },
    }
    fake = _FakeValidationConnection(
        tables=columns,
        columns=columns,
        indexes=[],
        migration_ids=[1, 54, 55],
    )

    monkeypatch.setattr(storage_pg, "_validation_contract", lambda: (
        {
            "schema_migrations": ("id", "description", "applied_at"),
            "alert_acks": ("alert_id", "acked_ts_ms", "expires_ts_ms"),
            "alert_lifecycle_events": ("id", "alert_id", "ts_ms"),
        },
        ("idx_alert_lifecycle_events_alert_ts",),
    ))
    monkeypatch.setattr(storage_pg, "_expected_migration_ids", lambda: (1, 54, 55))
    monkeypatch.setattr(storage_pg, "_current_expected_schema_version", lambda: 55)
    monkeypatch.setattr(storage_pg, "connection", lambda readonly=True: fake)

    snapshot = storage_pg.get_db_validation_snapshot(include_quick_check=False)

    assert snapshot["ok"] is False, snapshot
    assert snapshot["schema_version_ok"] is True, snapshot
    assert snapshot["missing_columns"] == {"alert_acks": ["expires_ts_ms"]}, snapshot
    assert snapshot["missing_indexes"] == ["idx_alert_lifecycle_events_alert_ts"], snapshot


def test_postgres_validation_fails_stale_schema_migrations(monkeypatch):
    columns = {"schema_migrations": _schema_migrations_columns()}
    fake = _FakeValidationConnection(
        tables=columns,
        columns=columns,
        indexes=[],
        migration_ids=[1, 54],
    )

    monkeypatch.setattr(storage_pg, "_validation_contract", lambda: (
        {"schema_migrations": ("id", "description", "applied_at")},
        (),
    ))
    monkeypatch.setattr(storage_pg, "_expected_migration_ids", lambda: (1, 54, 55))
    monkeypatch.setattr(storage_pg, "_current_expected_schema_version", lambda: 55)
    monkeypatch.setattr(storage_pg, "connection", lambda readonly=True: fake)

    snapshot = storage_pg.get_db_validation_snapshot(include_quick_check=False)

    assert snapshot["ok"] is False, snapshot
    assert snapshot["schema_version"] == 54, snapshot
    assert snapshot["expected_schema_version"] == 55, snapshot
    assert snapshot["schema_version_ok"] is False, snapshot
    assert snapshot["schema_status"] == "stale", snapshot
    assert snapshot["schema_migration_missing_ids"] == [55], snapshot


def test_postgres_validation_fails_owned_live_ingestion_type_drift(monkeypatch):
    import engine.runtime.storage_live_ingestion_schema as live_schema

    columns = {
        "schema_migrations": _schema_migrations_columns(),
        "prices": {
            "symbol": {"type": "TEXT", "udt_name": "text", "pk": 1},
            "ts_ms": {"type": "BIGINT", "udt_name": "int8", "pk": 2},
            "price": {"type": "TEXT", "udt_name": "text"},
        },
    }
    fake = _FakeValidationConnection(
        tables=columns,
        columns=columns,
        indexes=[],
        migration_ids=[1, 55],
    )

    monkeypatch.setattr(storage_pg, "_validation_contract", lambda: (
        {"schema_migrations": ("id", "description", "applied_at")},
        (),
    ))
    monkeypatch.setattr(storage_pg, "_expected_migration_ids", lambda: (1, 55))
    monkeypatch.setattr(storage_pg, "_current_expected_schema_version", lambda: 55)
    monkeypatch.setattr(storage_pg, "connection", lambda readonly=True: fake)
    monkeypatch.setattr(
        live_schema,
        "OWNED_LIVE_TABLE_COLUMN_SPECS",
        {
            "prices": {
                "symbol": {"type": "TEXT", "pk": 1},
                "ts_ms": {"type": "INTEGER", "pk": 2},
                "price": {"type": "REAL", "pk": 0},
            },
        },
    )
    monkeypatch.setattr(live_schema, "OWNED_LIVE_TABLE_REQUIRED_INDEXES", {"prices": ()})

    snapshot = storage_pg.get_db_validation_snapshot(include_quick_check=False)

    assert snapshot["ok"] is False, snapshot
    assert snapshot["owned_schema_ok"] is False, snapshot
    assert snapshot["owned_type_mismatches"] == {
        "prices": {"price": {"expected": "REAL", "actual": "TEXT"}}
    }, snapshot
    assert snapshot["owned_drift_tables"] == ["prices"], snapshot


def test_put_normalized_event_uses_timescale_conflict_key(monkeypatch):
    statements: list[tuple[str, tuple[object, ...]]] = []

    class FakeCursor:
        rowcount = 1

        def fetchone(self):
            return (42,)

    class FakeConnection:
        def execute(self, sql: str, params=()):
            statements.append((str(sql), tuple(params or ())))
            return FakeCursor()

    event_id = storage_pg.put_normalized_event(
        {
            "ts_ms": 1234567890,
            "source": "unit_test",
            "title": "probe",
            "event_key": "unit-test-event",
        },
        con=FakeConnection(),  # type: ignore[arg-type]
    )

    assert event_id == 42
    sql = statements[-1][0]
    assert "ON CONFLICT(event_key, ts_ms) DO UPDATE SET" in sql
    assert "event_key=COALESCE" not in sql
    assert "ts_ms=COALESCE" not in sql


def test_price_quotes_raw_buffer_uses_timescale_conflict_key_for_postgres():
    from engine.runtime import telemetry_append_buffer

    class FakeRaw:
        pass

    FakeRaw.__module__ = "psycopg.connection"

    class FakeConnection:
        raw = FakeRaw()

    sql = telemetry_append_buffer._sql_for_table("price_quotes_raw", FakeConnection())
    compact_sql = " ".join(sql.split())

    assert "ON CONFLICT(symbol, provider, event_key, ts_ms) DO UPDATE SET" in compact_sql


def test_price_quotes_raw_buffer_keeps_sqlite_conflict_key_for_compatibility():
    from engine.runtime import telemetry_append_buffer

    class FakeConnection:
        raw = object()

    sql = telemetry_append_buffer._sql_for_table("price_quotes_raw", FakeConnection())
    compact_sql = " ".join(sql.split())

    assert "ON CONFLICT(symbol, provider, event_key) DO UPDATE SET" in compact_sql
    assert "event_key, ts_ms" not in compact_sql
