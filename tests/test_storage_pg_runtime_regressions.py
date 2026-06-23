from __future__ import annotations

import json
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


def test_storage_pg_sql_normalization_cache_reuses_identical_sql(monkeypatch):
    storage_pg._clear_sql_normalization_cache()
    calls: list[str] = []
    original = storage_pg._rewrite_insert_or_ignore

    def spy_rewrite_insert_or_ignore(sql: str) -> str:
        calls.append(str(sql))
        return original(sql)

    monkeypatch.setattr(storage_pg, "_rewrite_insert_or_ignore", spy_rewrite_insert_or_ignore)
    raw_sql = "SELECT '?' AS literal WHERE symbol=?"

    assert storage_pg._normalize_sql(raw_sql) == "SELECT '?' AS literal WHERE symbol=%s"
    assert storage_pg._normalize_sql(raw_sql) == "SELECT '?' AS literal WHERE symbol=%s"
    assert calls == ["SELECT '?' AS literal WHERE symbol=%s"]
    storage_pg._clear_sql_normalization_cache()


def test_storage_pg_sql_normalization_cache_keeps_distinct_sql_classification(monkeypatch):
    storage_pg._clear_sql_normalization_cache()
    calls: list[str] = []
    original = storage_pg._rewrite_insert_or_ignore

    def spy_rewrite_insert_or_ignore(sql: str) -> str:
        calls.append(str(sql))
        return original(sql)

    monkeypatch.setattr(storage_pg, "_rewrite_insert_or_ignore", spy_rewrite_insert_or_ignore)
    ddl_sql = "CREATE TABLE cache_probe (id INTEGER, payload BLOB)"
    select_sql = "SELECT INTEGER AS marker FROM cache_probe WHERE id=?"

    assert storage_pg._normalize_sql(ddl_sql) == "CREATE TABLE cache_probe (id BIGINT, payload BYTEA)"
    assert storage_pg._normalize_sql(select_sql) == "SELECT INTEGER AS marker FROM cache_probe WHERE id=%s"
    assert storage_pg._normalize_sql(ddl_sql) == "CREATE TABLE cache_probe (id BIGINT, payload BYTEA)"
    assert calls == [ddl_sql, "SELECT INTEGER AS marker FROM cache_probe WHERE id=%s"]
    storage_pg._clear_sql_normalization_cache()


def test_storage_pg_sql_normalization_cache_is_bounded(monkeypatch):
    storage_pg._clear_sql_normalization_cache()
    monkeypatch.setattr(storage_pg, "_SQL_NORMALIZATION_CACHE_MAXSIZE", 2)

    for idx in range(3):
        assert storage_pg._normalize_sql(f"SELECT {idx} WHERE symbol=?") == f"SELECT {idx} WHERE symbol=%s"

    assert len(storage_pg._SQL_NORMALIZATION_CACHE) == 2
    assert "SELECT 0 WHERE symbol=?" not in storage_pg._SQL_NORMALIZATION_CACHE
    assert "SELECT 1 WHERE symbol=?" in storage_pg._SQL_NORMALIZATION_CACHE
    assert "SELECT 2 WHERE symbol=?" in storage_pg._SQL_NORMALIZATION_CACHE
    storage_pg._clear_sql_normalization_cache()


def test_storage_pg_connectionless_insert_or_replace_does_not_seed_cache():
    storage_pg._clear_sql_normalization_cache()
    raw_sql = "INSERT OR REPLACE INTO cache_probe(id, value) VALUES (?, ?)"

    assert storage_pg._normalize_sql(raw_sql) == (
        "INSERT INTO cache_probe(id, value) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    )
    assert raw_sql not in storage_pg._SQL_NORMALIZATION_CACHE


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
    monkeypatch.setattr(storage_pg, "_ensure_timescale_classified_tables", lambda: None)
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


def test_acquire_configures_fresh_connection_once_and_reuses_search_path_marker(monkeypatch):
    storage_pool.close_pool()
    statements: list[str] = []

    class FakeInfo:
        transaction_status = TransactionStatus.IDLE

    class FakeCursor:
        rowcount = 0
        description = ()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql: str, params=None) -> None:
            del params
            statements.append(str(sql))

        def close(self) -> None:
            pass

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

    class FakePool:
        def __init__(self, **kwargs):
            self.kwargs = dict(kwargs)
            self.conn = FakeConnection()
            self.getconn_count = 0
            self.putconn_count = 0
            self.close_count = 0

        def open(self, *, wait: bool, timeout: float) -> None:
            assert wait is True
            assert timeout == 0.25
            self.kwargs["configure"](self.conn)

        def getconn(self, *, timeout: float):
            assert timeout == 0.25
            self.getconn_count += 1
            return self.conn

        def putconn(self, conn) -> None:
            assert conn is self.conn
            self.putconn_count += 1

        def close(self, timeout=None) -> None:
            del timeout
            self.close_count += 1

    monkeypatch.setattr(storage_pool, "_POOL", None)
    monkeypatch.setattr(storage_pool, "_POOL_TRANSACTION_MODE", False)
    monkeypatch.setattr(storage_pool, "_dsn", lambda: "host=127.0.0.1 port=5432 dbname=trading")
    monkeypatch.setattr(storage_pool, "_pool_timeout_s", lambda: 0.25)
    monkeypatch.setattr(storage_pool, "_connect_timeout_s", lambda timeout_s=None: 1)
    monkeypatch.setattr(storage_pool, "default_pool_size", lambda: 1)
    monkeypatch.setattr(storage_pool, "schema_name", lambda: "trading")
    monkeypatch.setattr(storage_pool, "ConnectionPool", FakePool)

    try:
        first = storage_pool.acquire()
        storage_pool.release(first)
        second = storage_pool.acquire()
        storage_pool.release(second)

        assert first is second
        assert statements == ['SET search_path = "trading", public']
        assert storage_pool._POOL.getconn_count == 2  # type: ignore[union-attr]
        assert storage_pool._POOL.putconn_count == 2  # type: ignore[union-attr]
    finally:
        storage_pool.close_pool()


def test_storage_connection_session_state_sql_invalidates_search_path_marker(monkeypatch):
    statements: list[str] = []

    class FakeInfo:
        transaction_status = TransactionStatus.IDLE

    class FakeCursor:
        rowcount = 0
        description = ()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self.close()
            return False

        def execute(self, sql: str, params=None) -> None:
            del params
            statements.append(str(sql))

        def close(self) -> None:
            pass

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
    monkeypatch.setattr(storage_pool, "schema_name", lambda: "trading")

    storage_pool._configure_connection(conn)  # type: ignore[arg-type]
    storage_pool._configure_connection(conn)  # type: ignore[arg-type]
    cursor = storage_pg.StorageConnection(conn).execute("SET search_path = other, public")
    cursor.close()
    storage_pool._configure_connection(conn)  # type: ignore[arg-type]

    assert statements == [
        'SET search_path = "trading", public',
        "SET search_path = other, public",
        'SET search_path = "trading", public',
    ]


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


def test_risk_state_read_path_bypasses_postgres_autoinit(monkeypatch):
    from engine.runtime import risk_state

    key = "risk_state_read_autoinit_probe"
    calls: list[dict[str, object]] = []

    class FakeCursor:
        def fetchone(self):
            return ("preserve",)

    class FakeConnection:
        closed = False

        def execute(self, sql: str, params=()):
            calls.append({"sql": str(sql), "params": tuple(params or ())})
            return FakeCursor()

        def close(self) -> None:
            self.closed = True

    def fail_init_db():
        raise AssertionError("risk_state read path must not initialize schema")

    def fake_connect(*, readonly=False, **kwargs):
        calls.append({"readonly": readonly, **dict(kwargs)})
        return FakeConnection()

    monkeypatch.setattr(risk_state, "get_active_backend_name", lambda: "postgres")
    monkeypatch.setattr(risk_state, "init_db", fail_init_db)
    monkeypatch.setattr(risk_state, "connect", fake_connect)

    assert risk_state.get_state(key, "normal") == "preserve"
    assert calls[0] == {"readonly": True, "_skip_autoinit": True}


def test_risk_state_missing_table_read_returns_default_without_schema_init(monkeypatch):
    from engine.runtime import risk_state

    connections: list[object] = []

    class FakeConnection:
        def __init__(self) -> None:
            self.closed = False

        def execute(self, sql: str, params=()):
            raise RuntimeError('psycopg.errors.UndefinedTable: relation "risk_state" does not exist')

        def close(self) -> None:
            self.closed = True

    def fail_init_db():
        raise AssertionError("missing risk_state table read must not initialize schema")

    def fake_connect(*, readonly=False, **kwargs):
        assert readonly is True
        assert kwargs.get("_skip_autoinit") is True
        conn = FakeConnection()
        connections.append(conn)
        return conn

    monkeypatch.setattr(risk_state, "get_active_backend_name", lambda: "postgres")
    monkeypatch.setattr(risk_state, "init_db", fail_init_db)
    monkeypatch.setattr(risk_state, "connect", fake_connect)

    assert risk_state.get_state("risk_state_missing_table_probe", "normal") == "normal"
    assert risk_state.get_state_row("risk_state_missing_table_row_probe", "normal") == ("normal", 0)
    assert len(connections) == 2
    assert all(bool(getattr(conn, "closed", False)) for conn in connections)


def test_size_policy_supplied_connection_read_does_not_open_schema_connection(monkeypatch):
    from engine.strategy import size_policy

    class EmptyCursor:
        def fetchall(self):
            return []

    class FakeConnection:
        def execute(self, sql: str, params=()):
            if "pragma table_info" in str(sql).lower():
                return EmptyCursor()
            raise RuntimeError('psycopg.errors.UndefinedTable: relation "size_policy" does not exist')

    monkeypatch.setattr(
        size_policy,
        "connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("supplied size_policy read must not open a new connection")
        ),
    )

    assert size_policy.load_latest_size_policy(FakeConnection()) is None


def test_size_policy_owned_postgres_read_bypasses_autoinit(monkeypatch):
    from engine.strategy import size_policy

    calls: list[dict[str, object]] = []

    class EmptyCursor:
        def fetchall(self):
            return []

        def fetchone(self):
            return None

    class FakeConnection:
        closed = False

        def execute(self, sql: str, params=()):
            return EmptyCursor()

        def close(self) -> None:
            self.closed = True

    def fake_connect(*, readonly=False, **kwargs):
        calls.append({"readonly": readonly, **dict(kwargs)})
        return FakeConnection()

    monkeypatch.setattr(size_policy, "get_active_backend_name", lambda: "postgres")
    monkeypatch.setattr(size_policy, "connect", fake_connect)

    assert size_policy.load_latest_size_policy() is None
    assert calls == [{"readonly": True, "_skip_autoinit": True}]


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


def test_put_normalized_event_uses_schema_conflict_key(monkeypatch):
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
    assert "ON CONFLICT(event_key) DO UPDATE SET" in sql
    assert "event_key=COALESCE" not in sql


def test_price_quotes_raw_buffer_uses_aligned_conflict_key_for_postgres():
    from engine.runtime import telemetry_append_buffer

    class FakeRaw:
        pass

    FakeRaw.__module__ = "psycopg.connection"

    class FakeConnection:
        raw = FakeRaw()

    sql = telemetry_append_buffer._sql_for_table("price_quotes_raw", FakeConnection())
    compact_sql = " ".join(sql.split())

    assert "ON CONFLICT(symbol, provider, event_key, ts_ms) DO UPDATE SET" in compact_sql


def test_price_quotes_raw_buffer_uses_aligned_conflict_key_for_sqlite():
    from engine.runtime import telemetry_append_buffer

    class FakeConnection:
        raw = object()

    sql = telemetry_append_buffer._sql_for_table("price_quotes_raw", FakeConnection())
    compact_sql = " ".join(sql.split())

    assert "ON CONFLICT(symbol, provider, event_key, ts_ms) DO UPDATE SET" in compact_sql


class _SmallWriteCursor:
    rowcount = 1

    def __init__(self, row: tuple[object, ...] | None = None) -> None:
        self._row = row

    def fetchone(self):
        return self._row

    def fetchall(self):
        return []


class _SmallWriteConnection:
    def __init__(self, *, fail_on_execute: int | None = None, returning_id: int = 101) -> None:
        self.fail_on_execute = fail_on_execute
        self.returning_id = returning_id
        self.executes: list[tuple[str, tuple[object, ...]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def execute(self, sql: str, params=()):
        self.executes.append((str(sql), tuple(params or ())))
        if self.fail_on_execute is not None and len(self.executes) == self.fail_on_execute:
            raise RuntimeError("small write failure")
        if "RETURNING id" in str(sql):
            return _SmallWriteCursor((self.returning_id,))
        return _SmallWriteCursor()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closes += 1


def _capture_small_write_metrics(monkeypatch):
    counters: list[tuple[str, int, dict[str, object]]] = []
    timings: list[tuple[str, float, dict[str, object]]] = []

    def fake_counter(metric: str, value: int = 1, **tags: object) -> None:
        counters.append((str(metric), int(value), dict(tags)))

    def fake_timing(metric: str, latency_ms: float, **tags: object) -> None:
        timings.append((str(metric), float(latency_ms), dict(tags)))

    monkeypatch.setattr(storage_pg, "_emit_small_write_counter", fake_counter)
    monkeypatch.setattr(storage_pg, "_emit_small_write_timing", fake_timing)
    return counters, timings


def test_batchable_cpcv_path_result_reuses_one_write_transaction(monkeypatch):
    counters, timings = _capture_small_write_metrics(monkeypatch)
    connections: list[_SmallWriteConnection] = []

    def fake_connect(*, readonly: bool = False, **kwargs):
        assert readonly is False
        conn = _SmallWriteConnection(returning_id=321)
        connections.append(conn)
        return conn

    monkeypatch.setattr(storage_pg, "connect", fake_connect)

    result = storage_pg.record_backtest_cpcv_path_result(
        model_id="model-a",
        path_index=3,
        sharpe=1.25,
        ts=123456,
    )

    assert result == 321
    assert len(connections) == 1
    conn = connections[0]
    assert len(conn.executes) == 2
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.closes == 1
    assert [entry[0] for entry in counters] == [
        "storage_pg_small_write_coalesce_attempted",
        "storage_pg_small_write_coalesce_committed",
        "storage_pg_small_write_coalesced_calls",
        "storage_pg_small_write_coalesced_rows",
    ]
    assert counters[2][1] == 2
    assert counters[3][1] == 2
    assert counters[0][2]["operation"] == "record_backtest_cpcv_path_result"
    assert counters[1][2]["status"] == "committed"
    assert [entry[0] for entry in timings] == ["storage_pg_small_write_coalesce_latency_ms"]
    assert timings[0][2]["status"] == "committed"


def test_batchable_cpcv_path_result_rolls_back_both_writes_on_failure(monkeypatch):
    counters, timings = _capture_small_write_metrics(monkeypatch)
    connections: list[_SmallWriteConnection] = []

    def fake_connect(*, readonly: bool = False, **kwargs):
        assert readonly is False
        conn = _SmallWriteConnection(fail_on_execute=2)
        connections.append(conn)
        return conn

    monkeypatch.setattr(storage_pg, "connect", fake_connect)

    with pytest.raises(RuntimeError, match="small write failure"):
        storage_pg.record_backtest_cpcv_path_result(
            model_id="model-a",
            path_index=3,
            sharpe=1.25,
            ts=123456,
        )

    assert len(connections) == 1
    conn = connections[0]
    assert len(conn.executes) == 2
    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert conn.closes == 1
    assert [entry[0] for entry in counters] == [
        "storage_pg_small_write_coalesce_attempted",
        "storage_pg_small_write_coalesce_failed",
    ]
    assert counters[1][2]["status"] == "failed"
    assert [entry[0] for entry in timings] == ["storage_pg_small_write_coalesce_latency_ms"]
    assert timings[0][2]["status"] == "failed"


def test_cpcv_path_result_supplied_connection_bypasses_owned_coalescing(monkeypatch):
    counters, timings = _capture_small_write_metrics(monkeypatch)
    conn = _SmallWriteConnection(returning_id=77)
    monkeypatch.setattr(
        storage_pg,
        "connect",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("supplied connection path must not acquire another connection")
        ),
    )

    result = storage_pg.record_backtest_cpcv_path_result(
        con=conn,  # type: ignore[arg-type]
        model_id="model-a",
        path_index=3,
        sharpe=1.25,
        ts=123456,
    )

    assert result == 77
    assert len(conn.executes) == 2
    assert conn.commits == 0
    assert conn.rollbacks == 0
    assert conn.closes == 0
    assert counters == [
        (
            "storage_pg_small_write_coalesce_bypassed",
            1,
            {
                "operation": "record_backtest_cpcv_path_result",
                "status": "bypassed",
                "table": "backtest_cpcv_path_results",
                "reason": "caller_connection",
            },
        )
    ]
    assert timings == []


def test_critical_job_heartbeat_commits_independently_and_bypasses_coalescing(monkeypatch):
    counters, timings = _capture_small_write_metrics(monkeypatch)
    connections: list[_SmallWriteConnection] = []

    def fake_connect(*, readonly: bool = False, **kwargs):
        assert readonly is False
        assert kwargs.get("timeout_s") == 0.5
        conn = _SmallWriteConnection()
        connections.append(conn)
        return conn

    monkeypatch.setattr(storage_pg, "connect", fake_connect)

    storage_pg.put_job_heartbeat("job-a", "owner-a", 111, extra_json='{"n":1}')
    storage_pg.put_job_heartbeat("job-a", "owner-a", 111, extra_json='{"n":2}')

    assert len(connections) == 2
    assert [len(conn.executes) for conn in connections] == [2, 2]
    assert [conn.commits for conn in connections] == [1, 1]
    assert [conn.rollbacks for conn in connections] == [0, 0]
    assert [conn.closes for conn in connections] == [1, 1]
    assert counters == [
        (
            "storage_pg_small_write_coalesce_bypassed",
            1,
            {
                "operation": "put_job_heartbeat",
                "status": "bypassed",
                "table": "job_heartbeats",
                "reason": "critical",
            },
        ),
        (
            "storage_pg_small_write_coalesce_bypassed",
            1,
            {
                "operation": "put_job_heartbeat",
                "status": "bypassed",
                "table": "job_heartbeats",
                "reason": "critical",
            },
        ),
    ]
    assert timings == []


def _reset_pg_liveness_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    storage_pg._PG_LIVENESS_STOP.set()
    thread = storage_pg._PG_LIVENESS_THREAD
    if thread is not None and thread.is_alive():
        thread.join(timeout=0.5)
    with storage_pg._PG_LIVENESS_LOCK:
        storage_pg._PG_LIVENESS_PENDING.clear()
        storage_pg._PG_LIVENESS_LAST_PERSIST_MS.clear()
        storage_pg._PG_LIVENESS_STATE.clear()
        storage_pg._PG_LIVENESS_STATE.update(
            {
                "pending_count": 0,
                "flush_batches": 0,
                "flushed": 0,
                "enqueued": 0,
                "coalesced": 0,
                "coalesced_unreported": 0,
                "last_enqueue_ts_ms": 0,
                "last_flush_ts_ms": 0,
                "last_error": "",
                "last_error_ts_ms": 0,
            }
        )
    storage_pg._PG_LIVENESS_STOP.clear()
    monkeypatch.setattr(storage_pg, "_PG_LIVENESS_THREAD", None)
    monkeypatch.setattr(storage_pg, "_PG_LIVENESS_QUEUE_ENABLED", True)
    monkeypatch.setattr(storage_pg, "_PG_LIVENESS_MAX_BATCH", 64)
    monkeypatch.setattr(storage_pg, "_PG_LIVENESS_MIN_PERSIST_INTERVAL_MS", 0)
    monkeypatch.setattr(storage_pg, "_ensure_job_liveness_writer_started", lambda: None)


class _FakeWriteCursor:
    rowcount = 1

    def __init__(self, row=None) -> None:
        self._row = row

    def fetchone(self):
        return self._row


class _FakeWriteConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple[object, ...]]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closes = 0

    def execute(self, sql: str, params=()):
        self.statements.append((str(sql), tuple(params or ())))
        if str(sql).lstrip().upper().startswith("SELECT "):
            return _FakeWriteCursor(None)
        return _FakeWriteCursor()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closes += 1


def test_postgres_best_effort_liveness_writes_coalesce_and_reuse_one_transaction(monkeypatch):
    _reset_pg_liveness_queue(monkeypatch)
    connections: list[_FakeWriteConnection] = []
    emitted: list[tuple[str, int]] = []

    def fake_connect(*, readonly=False, timeout_s=None, **_kwargs):
        assert readonly is False
        conn = _FakeWriteConnection()
        connections.append(conn)
        return conn

    monkeypatch.setattr(storage_pg, "connect", fake_connect)
    monkeypatch.setattr(
        storage_pg,
        "_emit_pg_liveness_counter",
        lambda metric, value, **_tags: emitted.append((str(metric), int(value))),
    )

    storage_pg.touch_job_lock("poll_prices", "test-owner", 1234, best_effort=True)
    storage_pg.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"phase":"a"}', best_effort=True)
    storage_pg.put_job_heartbeat(
        "poll_prices",
        "test-owner",
        1234,
        '{"providers":{"yfinance":{"connected":true}}}',
        best_effort=True,
    )

    assert connections == []
    queued = storage_pg._job_liveness_queue_snapshot()
    assert queued["pending_count"] == 1
    assert queued["coalesced"] == 2

    flushed = storage_pg.flush_job_liveness_queue(max_batches=4, force=True)

    assert flushed["flushed"] == 1
    assert flushed["pending_count"] == 0
    assert len(connections) == 1
    assert connections[0].commits == 1
    assert connections[0].rollbacks == 0
    assert connections[0].closes == 1

    insert_params = [
        params
        for sql, params in connections[0].statements
        if "INSERT INTO job_heartbeats" in " ".join(sql.split())
    ]
    lock_updates = [
        params
        for sql, params in connections[0].statements
        if "UPDATE job_locks SET heartbeat_ts_ms" in " ".join(sql.split())
    ]
    assert len(insert_params) == 1
    assert len(lock_updates) == 1
    payload = json.loads(str(insert_params[0][4] or "{}"))
    assert payload["phase"] == "a"
    assert payload["providers"]["yfinance"]["connected"] is True
    assert ("storage_pg_liveness_flush_batches_total", 1) in emitted
    assert ("storage_pg_liveness_flushed_rows_total", 1) in emitted
    assert ("storage_pg_liveness_coalesced_writes_total", 2) in emitted


def test_postgres_critical_lock_writes_still_commit_independently(monkeypatch):
    _reset_pg_liveness_queue(monkeypatch)
    connections: list[_FakeWriteConnection] = []

    def fake_connect(*, readonly=False, timeout_s=None, **_kwargs):
        assert readonly is False
        conn = _FakeWriteConnection()
        connections.append(conn)
        return conn

    monkeypatch.setattr(storage_pg, "connect", fake_connect)
    monkeypatch.setattr(storage_pg, "_emit_pg_liveness_counter", lambda *_args, **_kwargs: None)

    assert storage_pg.acquire_job_lock("critical_job", "owner", 4321, ttl_s=30) is True
    storage_pg.release_job_lock("critical_job", "owner", 4321)

    assert storage_pg._job_liveness_queue_snapshot()["pending_count"] == 0
    assert len(connections) == 2
    assert [conn.commits for conn in connections] == [1, 1]
    assert [conn.closes for conn in connections] == [1, 1]
    assert all(conn.rollbacks == 0 for conn in connections)
