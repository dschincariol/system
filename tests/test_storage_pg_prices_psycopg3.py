from __future__ import annotations

from contextlib import contextmanager
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


def test_execute_many_values_emits_one_execute_per_page_not_executemany() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            self.calls.append((str(sql), tuple(params)))

        def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            raise AssertionError(f"unexpected executemany: {sql} {rows}")

    cur = FakeCursor()

    storage_pg_prices._execute_many_values(
        cur,
        "INSERT INTO public.price_ticks(symbol, last) VALUES %s "
        "ON CONFLICT(symbol) DO UPDATE SET last=EXCLUDED.last",
        [
            ("SPY", 500.0),
            ("QQQ", 430.0),
            ("IWM", 220.0),
            ("DIA", 390.0),
            ("TLT", 90.0),
        ],
        conflict_key_indexes=(0,),
        page_size=2,
    )

    assert len(cur.calls) == 3
    assert "VALUES (%s, %s), (%s, %s)" in cur.calls[0][0]
    assert "VALUES (%s, %s), (%s, %s)" in cur.calls[1][0]
    assert "VALUES (%s, %s)" in cur.calls[2][0]
    assert "ON CONFLICT(symbol)" in cur.calls[0][0]
    assert cur.calls[0][1] == ("SPY", 500.0, "QQQ", 430.0)
    assert cur.calls[1][1] == ("IWM", 220.0, "DIA", 390.0)
    assert cur.calls[2][1] == ("TLT", 90.0)


def test_execute_many_values_pages_by_psycopg_parameter_limit() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            self.calls.append((str(sql), tuple(params)))

        def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            raise AssertionError(f"unexpected executemany: {sql} {rows}")

    cur = FakeCursor()

    storage_pg_prices._execute_many_values(
        cur,
        "INSERT INTO public.price_ticks(symbol, last, volume) VALUES %s "
        "ON CONFLICT(symbol) DO UPDATE SET last=EXCLUDED.last",
        [(f"S{i}", float(i), i * 100) for i in range(5)],
        conflict_key_indexes=(0,),
        page_size=100,
        max_bind_params=6,
    )

    assert len(cur.calls) == 3
    assert [len(params) for _sql, params in cur.calls] == [6, 6, 3]
    assert all(len(params) <= 6 for _sql, params in cur.calls)


def test_execute_many_values_dedupes_duplicate_conflict_keys_within_page() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            self.calls.append((str(sql), tuple(params)))

        def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            raise AssertionError(f"unexpected executemany: {sql} {rows}")

    cur = FakeCursor()
    ts = "2026-06-21T00:00:00Z"

    storage_pg_prices._execute_many_values(
        cur,
        'INSERT INTO public.price_ticks(symbol, "time", last) VALUES %s '
        'ON CONFLICT(symbol, "time") DO UPDATE SET last=EXCLUDED.last',
        [
            ("SPY", ts, 500.0),
            ("QQQ", ts, 430.0),
            ("SPY", ts, 501.0),
        ],
        conflict_key_indexes=(0, 1),
    )

    assert len(cur.calls) == 1
    sql, params = cur.calls[0]
    assert "VALUES (%s, %s, %s), (%s, %s, %s)" in sql
    assert params == ("SPY", ts, 501.0, "QQQ", ts, 430.0)


def test_staging_table_ddl_is_fixed_unlogged_schema() -> None:
    ddl = storage_pg_prices._staging_table_ddl('"public"', "price_ticks")

    assert 'CREATE UNLOGGED TABLE IF NOT EXISTS "public"."price_ticks_write_staging"' in ddl
    assert '"staging_session" TEXT NOT NULL' in ddl
    assert '"staging_ordinal" BIGINT NOT NULL' in ddl
    assert storage_pg_prices._staging_index_name("price_ticks") == "idx_price_ticks_write_staging_session"


def test_staging_validation_columns_are_keyed_by_staging_table_names() -> None:
    assert "staging_session" not in storage_pg_prices._PG_PRICE_SCHEMA_TABLE_COLUMNS["price_ticks"]
    assert (
        "staging_session"
        in storage_pg_prices._PG_PRICE_STAGING_TABLE_COLUMNS["price_ticks_write_staging"]
    )
    assert (
        "staging_ordinal"
        in storage_pg_prices._PG_PRICE_STAGING_TABLE_COLUMNS["price_quotes_raw_write_staging"]
    )


def test_staging_copy_columns_resolve_for_each_base_table() -> None:
    for table_name in ("price_ticks", "price_quotes", "price_quotes_raw"):
        staging_table_name = storage_pg_prices._PG_PRICE_STAGING_TABLE_NAMES[table_name]
        staging_columns = storage_pg_prices._PG_PRICE_STAGING_TABLE_COLUMNS[staging_table_name]

        assert staging_columns[:2] == ("staging_session", "staging_ordinal")
        assert staging_columns[2:] == storage_pg_prices._PG_PRICE_SCHEMA_TABLE_COLUMNS[table_name]
        assert len(staging_columns) == len(storage_pg_prices._PG_PRICE_COPY_TYPES[table_name])


def test_timescale_policy_sql_preserves_segmentby_and_orders_by_real_time_column() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...] | None]] = []

        def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
            self.calls.append((str(sql), tuple(params) if params is not None else None))

    cur = FakeCursor()
    store = storage_pg_prices.PostgresPriceStorage(_config(compression_after_days=7))

    store._apply_timescale_policies(cur, '"public"."price_quotes_raw"', "price_quotes_raw")

    alter_sql = next(sql for sql, _params in cur.calls if "ALTER TABLE" in sql)
    assert 'timescaledb.compress_orderby = \'"time" DESC\'' in alter_sql
    assert "timescaledb.compress_segmentby = 'symbol'" in alter_sql
    assert any(
        "add_compression_policy" in sql and params == ('"public"."price_quotes_raw"', "7 days")
        for sql, params in cur.calls
    )


def test_raw_event_key_fallback_excludes_mutable_floats() -> None:
    base = {
        "ts_ms": 1_760_000_000_000,
        "symbol": "SPY",
        "provider": "polygon_ws",
        "event_type": "T",
        "trade_id": "trade-1",
        "sequence_number": "42",
        "exchange": "N",
        "last": 500.25,
        "bid": 500.2,
        "ask": 500.3,
        "volume": 1000,
    }
    changed_values = dict(base, last=501.25, bid=501.2, ask=501.3, volume=2000)

    key_a = storage_pg_prices._normalize_event_key(base)
    key_b = storage_pg_prices._normalize_event_key(changed_values)

    assert key_a == key_b
    assert key_a.startswith("price_raw:v1:")
    assert "500.25" not in key_a
    assert "1000" not in key_a


def test_write_batch_trusts_mapping_rows_without_mutating_callers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[tuple[Any, ...]]] = {}

    def fake_write_batch_copy(**kwargs: Any) -> str:
        captured["price_rows"] = list(kwargs["price_rows"])
        captured["quote_rows"] = list(kwargs["quote_rows"])
        captured["raw_rows"] = list(kwargs["raw_rows"])
        return "copy_staging"

    store = storage_pg_prices.PostgresPriceStorage(_config())
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_run_with_retry", lambda callback, *, operation: callback())
    monkeypatch.setattr(store, "_write_batch_copy", fake_write_batch_copy)

    price = {"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}
    quote = {"ts_ms": 1_700_000_000_000, "symbol": "qqq", "last": 430.0}
    raw = {
        "ts_ms": 1_700_000_000_000,
        "symbol": "iwm",
        "provider": "unit",
        "event_key": "event-1",
    }

    result = store.write_batch(prices=[price], quotes=[quote], raw=[raw])

    assert price == {"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}
    assert quote == {"ts_ms": 1_700_000_000_000, "symbol": "qqq", "last": 430.0}
    assert raw == {
        "ts_ms": 1_700_000_000_000,
        "symbol": "iwm",
        "provider": "unit",
        "event_key": "event-1",
    }
    price["symbol"] = "mutated"
    price["price"] = 1.0

    assert captured["price_rows"][0][1] == "SPY"
    assert captured["price_rows"][0][2] == 500.0
    assert result["row_copy_avoided_rows"] == 3
    assert result["row_copy_fallback_rows"] == 0
    snapshot = store.get_snapshot()
    assert snapshot["normalization_input_rows"] == 3
    assert snapshot["row_copy_avoided_rows"] == 3
    assert snapshot["row_copy_fallback_rows"] == 0


def test_normalize_price_write_rows_reuses_shared_row_conversions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_safe_float = storage_pg_prices._safe_float
    original_safe_int = storage_pg_prices._safe_int
    float_values: list[Any] = []
    int_values: list[Any] = []

    def counting_safe_float(value: Any) -> float | None:
        float_values.append(value)
        return original_safe_float(value)

    def counting_safe_int(value: Any) -> int | None:
        int_values.append(value)
        return original_safe_int(value)

    monkeypatch.setattr(storage_pg_prices, "_safe_float", counting_safe_float)
    monkeypatch.setattr(storage_pg_prices, "_safe_int", counting_safe_int)

    row = {
        "timestamp": 1_700_000_000_000,
        "symbol": "spy",
        "price": "500.50",
        "last": "500.25",
        "provider": "unit",
        "source": "unit-source",
        "event_key": "event-1",
        "bid": "500.20",
        "ask": "500.30",
        "spread": "0.10",
        "volume": "1000",
        "latency_ms": "7",
        "provider_score": "0.95",
        "last_trade_ts_ms": "1700000000101",
        "last_quote_ts_ms": "1700000000102",
        "last_update_ts_ms": "1700000000103",
        "trade_ts_ms": "1700000000104",
        "quote_ts_ms": "1700000000105",
        "ingest_ts_ms": "1700000000106",
    }

    normalized = storage_pg_prices._normalize_price_write_rows(
        prices=[row],
        quotes=[row],
        raw=[row],
    )

    assert len(normalized.price_rows) == 1
    assert len(normalized.quote_rows) == 1
    assert len(normalized.raw_rows) == 1
    assert normalized.price_rows[0][2] == 500.5
    assert normalized.quote_rows[0][2] == 500.25
    assert normalized.raw_rows[0][3] == "event-1"
    assert normalized.safe_float_calls == 7
    assert normalized.safe_int_calls == 8
    assert normalized.datetime_conversions == 1
    assert normalized.symbol_parses == 1
    assert normalized.event_key_normalizations == 1
    assert normalized.safe_float_calls < (6 + 5 + 5)
    assert normalized.safe_int_calls < (4 + 4 + 5)
    assert normalized.datetime_conversions < 3
    assert normalized.symbol_parses < 3
    assert float_values.count("500.20") == 1
    assert float_values.count("500.30") == 1
    assert float_values.count("0.10") == 1
    assert float_values.count("1000") == 1
    assert int_values.count(1_700_000_000_000) == 1


def test_normalize_price_write_rows_preserves_edge_case_validation() -> None:
    ts_ms = 1_700_000_000_000
    normalized = storage_pg_prices._normalize_price_write_rows(
        prices=[
            {
                "ts_ms": ts_ms,
                "symbol": " spy ",
                "price": 0,
                "last": 999.0,
                "provider": "unit",
                "latency_ms": "bad",
                "provider_score": "nan",
                "last_update_ts_ms": "",
                "ingest_ts_ms": "1700000000001",
            },
            {"ts_ms": ts_ms, "symbol": "   ", "price": 1.0},
        ],
        quotes=[
            {
                "timestamp": ts_ms,
                "symbol": " qqq ",
                "last": "inf",
                "bid": "bad",
                "ask": "101.1",
                "spread": "",
                "volume": "NaN",
                "source": "quote-source",
                "last_trade_ts_ms": 0,
                "trade_ts_ms": "1700000000201",
                "last_quote_ts_ms": "",
                "quote_ts_ms": "1700000000202",
                "last_update_ts_ms": "bad",
            }
        ],
        raw=[
            {
                "timestamp": ts_ms,
                "symbol": " iwm ",
                "provider": " unit ",
                "source": "raw-source",
                "event_type": "",
                "event_ts_ms": 0,
                "last": "501.5",
                "bid": "-inf",
                "ask": "502.1",
                "spread": "0.6",
                "volume": "bad",
                "trade_ts_ms": "1700000000301",
                "quote_ts_ms": "1700000000302",
                "ingest_ts_ms": "1700000000303",
            },
            {"timestamp": ts_ms, "symbol": "dia", "last": 1.0},
        ],
    )

    assert normalized.dropped_rows == {"prices": 1, "quotes": 0, "raw": 1}
    price_row = normalized.price_rows[0]
    assert price_row[1] == "SPY"
    assert price_row[2] == 0.0
    assert price_row[9] is None
    assert price_row[10] is None
    assert price_row[11] is None
    assert price_row[12] == 1_700_000_000_001

    quote_row = normalized.quote_rows[0]
    assert quote_row[1] == "QQQ"
    assert quote_row[2] is None
    assert quote_row[3] is None
    assert quote_row[4] == 101.1
    assert quote_row[6] is None
    assert quote_row[8] == 1_700_000_000_201
    assert quote_row[9] == 1_700_000_000_202
    assert quote_row[10] is None

    raw_row = normalized.raw_rows[0]
    assert raw_row[1] == "IWM"
    assert raw_row[2] == " unit "
    assert raw_row[3].startswith("price_raw:v1:")
    assert raw_row[4] == ""
    assert raw_row[5] == ts_ms
    assert raw_row[6] == 501.5
    assert raw_row[7] is None
    assert raw_row[10] is None
    assert raw_row[14] == "raw-source"


def test_write_batch_uses_binary_copy_staging_upserts(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCopy:
        def __init__(self, sql: str) -> None:
            self.sql = str(sql)
            self.types: list[str] = []
            self.rows: list[tuple[Any, ...]] = []

        def __enter__(self) -> "FakeCopy":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def set_types(self, types: list[str]) -> None:
            self.types = list(types)

        def write_row(self, row: tuple[Any, ...]) -> None:
            self.rows.append(tuple(row))

    class FakeCursor:
        def __init__(self) -> None:
            self.executions: list[tuple[str, tuple[Any, ...] | None]] = []
            self.copies: list[FakeCopy] = []

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
            self.executions.append((str(sql), tuple(params) if params is not None else None))

        def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            raise AssertionError(f"unexpected executemany: {sql} {rows}")

        def copy(self, sql: str) -> FakeCopy:
            copy = FakeCopy(str(sql))
            self.copies.append(copy)
            return copy

    class FakeConnection:
        def __init__(self, cursor: FakeCursor) -> None:
            self.cursor_obj = cursor
            self.commits = 0

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

        def commit(self) -> None:
            self.commits += 1

    cursor = FakeCursor()
    connection = FakeConnection(cursor)

    @contextmanager
    def fake_connection():
        yield connection

    store = storage_pg_prices.PostgresPriceStorage(_config())
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_connection", fake_connection)
    monkeypatch.setattr(store, "_run_with_retry", lambda callback, *, operation: callback())
    monkeypatch.setattr(
        storage_pg_prices,
        "maybe_apply_sync_refetchable_pg_durability",
        lambda *_args, **_kwargs: None,
    )

    result = store.write_batch(
        prices=[
            {"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0},
            {"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 501.0},
        ],
        quotes=[{"ts_ms": 1_700_000_000_000, "symbol": "qqq", "last": 430.0}],
        raw=[
            {
                "ts_ms": 1_700_000_000_000,
                "symbol": "iwm",
                "provider": "unit",
                "event_key": "event-1",
            }
        ],
    )

    assert result["ok"] is True
    assert result["write_path"] == "copy_staging"
    assert connection.commits == 1
    assert len(cursor.copies) == 3
    assert all("FROM STDIN (FORMAT BINARY)" in copy.sql for copy in cursor.copies)
    assert '"public"."price_ticks_write_staging"' in cursor.copies[0].sql
    assert cursor.copies[0].types == list(storage_pg_prices._PG_PRICE_COPY_TYPES["price_ticks"])
    assert [row[1] for row in cursor.copies[0].rows] == [0, 1]
    assert cursor.copies[0].rows[0][0] == cursor.copies[0].rows[1][0]
    assert cursor.copies[0].rows[1][4] == 501.0

    executed_sql = "\n".join(sql for sql, _params in cursor.executions)
    assert "CREATE " not in executed_sql
    assert 'SELECT DISTINCT ON ("symbol", "time")' in executed_sql
    assert 'ORDER BY "symbol", "time", staging_ordinal DESC' in executed_sql
    assert 'ON CONFLICT(symbol, "time") DO UPDATE SET' in executed_sql
    assert 'SELECT DISTINCT ON ("symbol", "provider", "event_key", "time")' in executed_sql
    assert 'ON CONFLICT(symbol, provider, event_key, "time") DO UPDATE SET' in executed_sql
    assert "price_quotes_raw_write_staging" in executed_sql
    price_tick_cleanup = [
        params
        for sql, params in cursor.executions
        if 'DELETE FROM "public"."price_ticks_write_staging" WHERE staging_session = %s' in sql
    ]
    assert len(price_tick_cleanup) == 2
    assert price_tick_cleanup[0] == price_tick_cleanup[1]


def test_write_batch_records_copy_row_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: list[tuple[str, int, dict[str, Any]]] = []

    def fake_emit_counter(metric: str, value: int = 1, **kwargs: Any) -> None:
        emitted.append((str(metric), int(value), dict(kwargs)))

    store = storage_pg_prices.PostgresPriceStorage(_config())
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_run_with_retry", lambda callback, *, operation: callback())
    monkeypatch.setattr(store, "_write_batch_copy", lambda **_kwargs: "copy_staging")
    monkeypatch.setattr(storage_pg_prices, "emit_counter", fake_emit_counter)
    monkeypatch.setattr(
        storage_pg_prices,
        "emit_timing",
        lambda *_args, **_kwargs: None,
    )

    result = store.write_batch(
        prices=[{"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}],
        quotes=[{"ts_ms": 1_700_000_000_000, "symbol": "qqq", "last": 430.0}],
        raw=[
            {
                "ts_ms": 1_700_000_000_000,
                "symbol": "iwm",
                "provider": "unit",
                "event_key": "event-1",
            }
        ],
    )

    snapshot = store.get_snapshot()
    assert result["write_path"] == "copy_staging"
    assert snapshot["copy_batches"] == 1
    assert snapshot["copy_rows"] == 3
    assert snapshot["values_batches"] == 0
    assert snapshot["values_rows"] == 0
    assert ("storage_pg_prices_copy_batches", 1) in [(metric, value) for metric, value, _kwargs in emitted]
    assert ("storage_pg_prices_copy_rows", 3) in [(metric, value) for metric, value, _kwargs in emitted]
    table_row_metrics = [
        (value, kwargs["extra_tags"]["table"])
        for metric, value, kwargs in emitted
        if metric == "storage_pg_prices_written_rows" and kwargs.get("extra_tags", {}).get("table") != "all"
    ]
    assert sorted(table_row_metrics) == [(1, "price_quotes"), (1, "price_quotes_raw"), (1, "price_ticks")]


def test_write_batch_records_write_failure_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: list[tuple[str, int, dict[str, Any]]] = []

    def fake_emit_counter(metric: str, value: int = 1, **kwargs: Any) -> None:
        emitted.append((str(metric), int(value), dict(kwargs)))

    def fail_copy(**_kwargs: Any) -> str:
        raise RuntimeError("copy failed")

    store = storage_pg_prices.PostgresPriceStorage(_config())
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_write_batch_copy", fail_copy)
    monkeypatch.setattr(storage_pg_prices, "emit_counter", fake_emit_counter)

    with pytest.raises(RuntimeError, match="storage_pg_prices_write_batch_failed"):
        store.write_batch(prices=[{"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}])

    snapshot = store.get_snapshot()
    assert snapshot["write_failures"] == 1
    failure_tags = [
        kwargs["extra_tags"]
        for metric, value, kwargs in emitted
        if metric == "storage_pg_prices_failures" and value == 1
    ]
    assert failure_tags == [
        {
            "operation": "write_batch",
            "error": "RuntimeError",
            "failure_class": "fatal",
            "retryable": "false",
            "reset_pool": "false",
        }
    ]


def test_run_with_retry_retries_transient_failure_without_pool_reset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    LockNotAvailable = type("LockNotAvailable", (Exception,), {})
    attempts = 0
    reset_calls = 0

    def flaky_write() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise LockNotAvailable("statement timeout while waiting for lock")
        return "ok"

    def fake_reset_pool() -> None:
        nonlocal reset_calls
        reset_calls += 1

    store = storage_pg_prices.PostgresPriceStorage(_config(retry_attempts=2))
    monkeypatch.setattr(store, "_reset_pool", fake_reset_pool)
    monkeypatch.setattr(storage_pg_prices.time, "sleep", lambda _seconds: None)

    assert store._run_with_retry(flaky_write, operation="write_batch") == "ok"

    snapshot = store.get_snapshot()
    assert attempts == 2
    assert reset_calls == 0
    assert snapshot["retry_count"] == 1
    assert snapshot["retryable_failures"] == 1
    assert snapshot["fatal_failures"] == 0
    assert snapshot["pool_resets"] == 0
    assert snapshot["last_failure_class"] == "retryable"
    assert snapshot["last_failure_retryable"] is True
    assert snapshot["last_failure_reset_pool"] is False
    assert snapshot["last_error"] is None
    assert snapshot["write_circuit_open"] is False


def test_run_with_retry_stops_immediately_on_fatal_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ProgrammingError = type("ProgrammingError", (Exception,), {})
    attempts = 0

    def fatal_write() -> str:
        nonlocal attempts
        attempts += 1
        raise ProgrammingError("undefined column unit_test")

    store = storage_pg_prices.PostgresPriceStorage(
        _config(retry_attempts=3, circuit_failure_threshold=1)
    )
    monkeypatch.setattr(storage_pg_prices.time, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="storage_pg_prices_write_batch_failed"):
        store._run_with_retry(fatal_write, operation="write_batch")

    snapshot = store.get_snapshot()
    assert attempts == 1
    assert snapshot["retry_count"] == 0
    assert snapshot["retryable_failures"] == 0
    assert snapshot["fatal_failures"] == 1
    assert snapshot["last_failure_class"] == "fatal"
    assert snapshot["last_failure_retryable"] is False
    assert snapshot["write_circuit_open"] is False


def test_write_batch_opens_circuit_after_sustained_retryable_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    LockNotAvailable = type("LockNotAvailable", (Exception,), {})
    calls = 0

    def fail_copy(**_kwargs: Any) -> str:
        nonlocal calls
        calls += 1
        raise LockNotAvailable("statement timeout while waiting for lock")

    store = storage_pg_prices.PostgresPriceStorage(
        _config(
            retry_attempts=1,
            circuit_failure_threshold=2,
            circuit_open_s=60.0,
        )
    )
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_write_batch_copy", fail_copy)
    monkeypatch.setattr(storage_pg_prices.time, "sleep", lambda _seconds: None)

    row = {"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}
    with pytest.raises(RuntimeError, match="storage_pg_prices_write_batch_failed"):
        store.write_batch(prices=[row])
    with pytest.raises(RuntimeError, match="storage_pg_prices_write_batch_failed"):
        store.write_batch(prices=[row])

    opened = store.get_snapshot()
    assert calls == 2
    assert opened["write_circuit_open"] is True
    assert opened["backpressure_active"] is True
    assert opened["write_circuit_opened_count"] == 1
    assert opened["write_circuit_consecutive_failures"] == 2
    assert opened["retryable_failures"] == 2
    assert opened["fatal_failures"] == 0

    with pytest.raises(RuntimeError, match="storage_pg_prices_write_batch_circuit_open"):
        store.write_batch(prices=[row])

    rejected = store.get_snapshot()
    assert calls == 2
    assert rejected["write_circuit_rejected_batches"] == 1
    assert rejected["write_circuit_open"] is True


def test_write_batch_schema_setup_is_not_run_per_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    executions: list[str] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
            del params
            executions.append(str(sql))

        def fetchone(self) -> tuple[str, tuple[str, ...]]:
            return ("price_quotes_raw_pkey", ("symbol", "provider", "event_key", "time"))

    class FakeConnection:
        def __init__(self) -> None:
            self.commits = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commits += 1

        def rollback(self) -> None:
            return None

    connection = FakeConnection()

    @contextmanager
    def fake_connection():
        yield connection

    store = storage_pg_prices.PostgresPriceStorage(_config())
    store._pool = object()
    monkeypatch.setattr(store, "_connection", fake_connection)
    monkeypatch.setattr(store, "_run_with_retry", lambda callback, *, operation: callback())
    monkeypatch.setattr(
        store,
        "_validate_schema",
        lambda _cur: {"missing_tables": [], "missing_columns": {}, "missing_indexes": []},
    )
    monkeypatch.setattr(store, "_write_batch_copy", lambda **_kwargs: "copy_staging")
    monkeypatch.setattr(
        storage_pg_prices,
        "emit_timing",
        lambda *_args, **_kwargs: None,
    )

    first = store.write_batch(prices=[{"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}])
    create_count_after_first_flush = sum(1 for sql in executions if "CREATE " in sql)

    second = store.write_batch(prices=[{"ts_ms": 1_700_000_000_001, "symbol": "spy", "price": 501.0}])
    create_count_after_second_flush = sum(1 for sql in executions if "CREATE " in sql)

    assert first["write_path"] == "copy_staging"
    assert second["write_path"] == "copy_staging"
    assert create_count_after_first_flush > 0
    assert create_count_after_second_flush == create_count_after_first_flush


def test_write_batch_falls_back_explicitly_when_copy_api_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[tuple[Any, ...]], tuple[int, ...]]] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
            del sql, params

    class FakeConnection:
        def __init__(self) -> None:
            self.commits = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commits += 1

    @contextmanager
    def fake_connection():
        yield FakeConnection()

    def fake_execute_many_values(
        cur: Any,
        sql: str,
        rows: list[tuple[Any, ...]],
        *,
        conflict_key_indexes: tuple[int, ...],
    ) -> None:
        del cur
        calls.append((str(sql), list(rows), tuple(conflict_key_indexes)))

    store = storage_pg_prices.PostgresPriceStorage(_config())
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_connection", fake_connection)
    monkeypatch.setattr(store, "_run_with_retry", lambda callback, *, operation: callback())
    monkeypatch.setattr(
        storage_pg_prices,
        "maybe_apply_sync_refetchable_pg_durability",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(storage_pg_prices, "_execute_many_values", fake_execute_many_values)

    result = store.write_batch(
        prices=[{"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}],
    )

    snapshot = store.get_snapshot()
    assert result["write_path"] == "values_upsert_copy_unavailable"
    assert snapshot["copy_fallbacks"] == 1
    assert snapshot["values_batches"] == 1
    assert snapshot["values_rows"] == 1
    assert "cursor_copy_api_missing" in snapshot["last_copy_unavailable"]
    assert [call[2] for call in calls] == [storage_pg_prices._PRICE_TICKS_CONFLICT_KEY_INDEXES]


def test_write_batch_does_not_use_read_router_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from engine.runtime import price_read_router, state_cache

    def fail_cache(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("read cache should not be used by price write paths")

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    class FakeConnection:
        def __init__(self) -> None:
            self.commits = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commits += 1

    @contextmanager
    def fake_connection():
        yield FakeConnection()

    monkeypatch.setattr(price_read_router, "cache_get_or_load", fail_cache)
    monkeypatch.setattr(state_cache, "cache_get_or_load", fail_cache)
    monkeypatch.setattr(
        storage_pg_prices,
        "maybe_apply_sync_refetchable_pg_durability",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(storage_pg_prices, "_execute_many_values", lambda *_args, **_kwargs: None)

    store = storage_pg_prices.PostgresPriceStorage(_config(copy_enabled=False))
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_connection", fake_connection)
    monkeypatch.setattr(store, "_run_with_retry", lambda callback, *, operation: callback())

    result = store.write_batch(prices=[{"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}])

    assert result["write_path"] == "values_upsert_copy_disabled"


def test_write_batch_passes_table_conflict_keys_to_values_upserts(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[tuple[Any, ...]], tuple[int, ...]]] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

    class FakeConnection:
        def __init__(self) -> None:
            self.commits = 0

        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            self.commits += 1

    @contextmanager
    def fake_connection():
        yield FakeConnection()

    def fake_execute_many_values(
        cur: Any,
        sql: str,
        rows: list[tuple[Any, ...]],
        *,
        conflict_key_indexes: tuple[int, ...],
    ) -> None:
        del cur
        calls.append((str(sql), list(rows), tuple(conflict_key_indexes)))

    store = storage_pg_prices.PostgresPriceStorage(_config(copy_enabled=False))
    monkeypatch.setattr(store, "start", lambda: store.get_snapshot())
    monkeypatch.setattr(store, "_connection", fake_connection)
    monkeypatch.setattr(store, "_run_with_retry", lambda callback, *, operation: callback())
    monkeypatch.setattr(
        storage_pg_prices,
        "maybe_apply_sync_refetchable_pg_durability",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(storage_pg_prices, "_execute_many_values", fake_execute_many_values)

    result = store.write_batch(
        prices=[{"ts_ms": 1_700_000_000_000, "symbol": "spy", "price": 500.0}],
        quotes=[{"ts_ms": 1_700_000_000_000, "symbol": "qqq", "last": 430.0}],
        raw=[
            {
                "ts_ms": 1_700_000_000_000,
                "symbol": "iwm",
                "provider": "unit",
                "event_key": "event-1",
            }
        ],
    )

    assert result["ok"] is True
    assert result["write_path"] == "values_upsert_copy_disabled"
    assert [call[2] for call in calls] == [
        storage_pg_prices._PRICE_TICKS_CONFLICT_KEY_INDEXES,
        storage_pg_prices._PRICE_QUOTES_CONFLICT_KEY_INDEXES,
        storage_pg_prices._PRICE_QUOTES_RAW_CONFLICT_KEY_INDEXES,
    ]
    assert [len(call[1]) for call in calls] == [1, 1, 1]
    assert 'ON CONFLICT(symbol, provider, event_key, "time") DO UPDATE SET' in calls[-1][0]


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
