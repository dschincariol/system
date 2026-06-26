from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest
from psycopg.pq import TransactionStatus

from engine.api import api_market
from engine.runtime import price_read_router


BASE_TS_MS = 1_700_000_000_000


def _connect_factory(db_path: Path):
    def _connect():
        return sqlite3.connect(str(db_path))

    return _connect


def _assert_ascending(candles: list[dict[str, Any]]) -> None:
    times = [int(row["ts_ms"]) for row in candles]
    assert times == sorted(times)


def test_sqlite_quote_rows_limit_bounds_newest_rows_and_returns_ascending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "quotes.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE price_quotes (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              volume REAL,
              PRIMARY KEY(symbol, ts_ms)
            );
            """
        )
        con.executemany(
            "INSERT INTO price_quotes(ts_ms, symbol, last, volume) VALUES (?, ?, ?, ?)",
            [(BASE_TS_MS + i, "SPY", 100.0 + i, float(i)) for i in range(20)],
        )
        con.commit()

    monkeypatch.setattr(price_read_router, "connect_ro", _connect_factory(db_path))

    rows = price_read_router._fetch_sqlite_quote_rows(symbol="SPY", since_ts_ms=0, limit=5)

    assert [row[0] for row in rows] == [BASE_TS_MS + i for i in range(15, 20)]
    assert [row[1] for row in rows] == [115.0, 116.0, 117.0, 118.0, 119.0]


def test_sqlite_quote_rows_supports_canonical_time_column(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "quotes-time.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE price_quotes (
              time INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              volume REAL,
              PRIMARY KEY(symbol, time)
            );
            """
        )
        con.executemany(
            'INSERT INTO price_quotes("time", symbol, last, volume) VALUES (?, ?, ?, ?)',
            [(BASE_TS_MS + i, "GLD", 200.0 + i, float(i)) for i in range(8)],
        )
        con.commit()

    monkeypatch.setattr(price_read_router, "connect_ro", _connect_factory(db_path))

    rows = price_read_router._fetch_sqlite_quote_rows(symbol="GLD", since_ts_ms=BASE_TS_MS + 2, limit=3)

    assert rows == [
        (BASE_TS_MS + 5, 205.0, 5.0),
        (BASE_TS_MS + 6, 206.0, 6.0),
        (BASE_TS_MS + 7, 207.0, 7.0),
    ]


def test_postgres_storage_quote_fallback_uses_postgres_timestamp_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeResult:
        def __init__(
            self,
            *,
            one: tuple[Any, ...] | None = None,
            many: list[tuple[Any, ...]] | None = None,
        ) -> None:
            self._one = one
            self._many = many or []

        def fetchone(self):
            return self._one

        def fetchall(self):
            return list(self._many)

    class FakePostgresStorageConnection:
        __module__ = "engine.runtime.storage_pg"

        def __init__(self) -> None:
            self.data_sql = ""
            self.data_params: tuple[Any, ...] = ()
            self.closed = False

        def execute(self, sql: str, params: tuple[Any, ...] | None = None):
            sql_text = str(sql)
            if "sqlite_master" in sql_text:
                return FakeResult(one=(1,) if params == ("price_quotes",) else None)
            if sql_text.startswith("PRAGMA table_info"):
                return FakeResult(
                    many=[
                        (0, "time", "TIMESTAMPTZ", 1, None, 1),
                        (1, "symbol", "TEXT", 1, None, 2),
                        (2, "last", "DOUBLE PRECISION", 0, None, 0),
                        (3, "volume", "DOUBLE PRECISION", 0, None, 0),
                    ]
                )
            self.data_sql = sql_text
            self.data_params = tuple(params or ())
            return FakeResult(many=[(BASE_TS_MS, 101.0, 1.0)])

        def close(self) -> None:
            self.closed = True

    fake = FakePostgresStorageConnection()
    monkeypatch.setattr(price_read_router, "connect_ro", lambda: fake)

    rows = price_read_router._fetch_sqlite_quote_rows(symbol="GLD", since_ts_ms=BASE_TS_MS - 1, limit=5)

    assert rows == [(BASE_TS_MS, 101.0, 1.0)]
    assert "strftime" not in fake.data_sql
    assert "typeof" not in fake.data_sql
    assert 'EXTRACT(EPOCH FROM "time")' in fake.data_sql
    assert '"time" > TO_TIMESTAMP(? / 1000.0)' in fake.data_sql
    assert 'ORDER BY "time" DESC' in fake.data_sql
    assert fake.data_params == ("GLD", BASE_TS_MS - 1, 5)
    assert fake.closed is True


def test_sqlite_quote_rows_falls_back_to_prices_price_px_without_volume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "prices.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE prices (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              price REAL,
              px REAL,
              source TEXT,
              PRIMARY KEY(symbol, ts_ms)
            );
            """
        )
        con.executemany(
            "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?, ?, ?, ?, ?)",
            [
                (BASE_TS_MS + 1, "GLD", None, 190.0, "unit"),
                (BASE_TS_MS + 2, "GLD", 191.0, 190.5, "unit"),
                (BASE_TS_MS + 3, "GLD", 192.0, 191.5, "unit"),
            ],
        )
        con.commit()

    monkeypatch.setattr(price_read_router, "connect_ro", _connect_factory(db_path))

    rows = price_read_router._fetch_sqlite_quote_rows(symbol="GLD", since_ts_ms=0, limit=10)

    assert rows == [
        (BASE_TS_MS + 1, 190.0, None),
        (BASE_TS_MS + 2, 191.0, None),
        (BASE_TS_MS + 3, 192.0, None),
    ]


def test_timescale_quote_rows_query_bounds_newest_rows_and_returns_ascending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.executed: list[tuple[str, tuple[Any, ...]]] = []
            self.last_sql = ""
            self.last_params: tuple[Any, ...] = ()

        def __enter__(self):
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            self.last_sql = str(sql)
            self.last_params = tuple(params or ())
            self.executed.append((sql, params))

        def fetchall(self):
            if "information_schema.columns" in self.last_sql:
                assert self.last_params == ("public", "price_quotes")
                return [("time",), ("symbol",), ("last",), ("volume",), ("source",)]
            return [
                (BASE_TS_MS + 2, 102.0, 2.0),
                (BASE_TS_MS + 1, 101.0, 1.0),
            ]

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    class FakeTimescaleContext:
        def __init__(self, con: FakeConnection) -> None:
            self.con = con

        def __enter__(self):
            return self.con, "public"

        def __exit__(self, *_exc: object) -> None:
            return None

    fake_con = FakeConnection()
    price_read_router._TIMESCALE_TABLE_COLUMNS_CACHE.clear()
    monkeypatch.setattr(price_read_router, "_timescale_connection", lambda: FakeTimescaleContext(fake_con))

    rows = price_read_router._fetch_timescale_quote_rows(symbol="SPY", since_ts_ms=123, limit=2)

    assert [row[0] for row in rows] == [BASE_TS_MS + 1, BASE_TS_MS + 2]
    sql, params = fake_con.cursor_obj.executed[-1]
    assert 'ORDER BY "time" DESC' in sql
    assert "LIMIT %s" in sql
    assert "ORDER BY ts_ms ASC" in sql
    assert params == ("SPY", 123, 2)


def test_market_candles_timescale_time_schema_returns_populated_candles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.executed: list[tuple[str, tuple[Any, ...]]] = []
            self.last_sql = ""
            self.last_params: tuple[Any, ...] = ()

        def __enter__(self):
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            self.last_sql = str(sql)
            self.last_params = tuple(params or ())
            normalized = " ".join(str(sql).split())
            if "FROM \"public\".\"price_quotes\"" in normalized:
                assert '"time" > TO_TIMESTAMP' in normalized
                assert 'ORDER BY "time" DESC' in normalized
                assert "AND ts_ms" not in normalized
                assert "ORDER BY ts_ms DESC" not in normalized
            self.executed.append((sql, params))

        def fetchall(self):
            if "information_schema.columns" in self.last_sql:
                assert self.last_params == ("public", "price_quotes")
                return [("time",), ("symbol",), ("last",), ("volume",), ("source",)]
            return [
                (BASE_TS_MS + 121_000, 103.0, 3.0),
                (BASE_TS_MS + 61_000, 102.0, 2.0),
                (BASE_TS_MS + 1_000, 101.0, 1.0),
            ]

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self):
            return self.cursor_obj

    class FakeTimescaleContext:
        def __init__(self, con: FakeConnection) -> None:
            self.con = con

        def __enter__(self):
            return self.con, "public"

        def __exit__(self, *_exc: object) -> None:
            return None

    fake_con = FakeConnection()
    price_read_router._TIMESCALE_TABLE_COLUMNS_CACHE.clear()
    monkeypatch.setattr(price_read_router, "get_price_read_backend", lambda: "timescale")
    monkeypatch.setattr(price_read_router, "_timescale_connection", lambda: FakeTimescaleContext(fake_con))
    monkeypatch.setattr(api_market, "cache_get_or_load", lambda _scope, _key, loader, ttl_s=0.0: loader())
    monkeypatch.setattr(api_market.time, "time", lambda: (BASE_TS_MS + 180_000) / 1000.0)

    payload = api_market.api_get_market_candles({"symbol": "TSMS", "tf": "1m", "limit": "10"}, None)

    assert payload["ok"] is True
    assert payload["meta"]["ready"] is True
    candles = payload["candles"]
    assert candles
    _assert_ascending(candles)
    assert candles[-1]["close"] == 103.0
    assert candles[-1]["volume"] == 3.0
    assert fake_con.cursor_obj.executed
    data_query = [item for item in fake_con.cursor_obj.executed if 'FROM "public"."price_quotes"' in item[0]][0]
    assert data_query[1][0] == "TSMS"


def test_market_stream_reads_canonical_time_quote_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "stream-quotes.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE price_quotes (
              time INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              volume REAL,
              PRIMARY KEY(symbol, time)
            );
            """
        )
        con.executemany(
            'INSERT INTO price_quotes("time", symbol, last, volume) VALUES (?, ?, ?, ?)',
            [(BASE_TS_MS + (i * 1000), "GLD", 180.0 + i, 1.0) for i in range(3)],
        )
        con.commit()

    monkeypatch.setattr(price_read_router, "_READ_BACKEND", "sqlite")
    monkeypatch.setattr(price_read_router, "connect_ro", _connect_factory(db_path))
    response = api_market.api_get_market_stream({"symbol": "GLD", "tf": "1m"}, None)
    writes: list[bytes] = []

    class FakeWfile:
        def write(self, data: bytes) -> None:
            writes.append(bytes(data))
            if data.startswith(b": ping"):
                raise BrokenPipeError("stop stream after first candle")

        def flush(self) -> None:
            return None

    class FakeHandler:
        wfile = FakeWfile()

    response.stream_fn(FakeHandler())

    body = b"".join(writes)
    assert b"event: hello" in body
    assert b"event: candle" in body
    assert b'"close":182.0' in body


def test_operator_market_data_uses_candles_handler_with_canonical_time_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from engine.api import api_operator_handlers

    db_path = tmp_path / "operator-quotes.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE price_quotes (
              time INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              volume REAL,
              PRIMARY KEY(symbol, time)
            );
            """
        )
        con.executemany(
            'INSERT INTO price_quotes("time", symbol, last, volume) VALUES (?, ?, ?, ?)',
            [(BASE_TS_MS + (i * 60_000), "GLD", 170.0 + i, 2.0) for i in range(4)],
        )
        con.commit()

    monkeypatch.setattr(price_read_router, "_READ_BACKEND", "sqlite")
    monkeypatch.setattr(price_read_router, "connect_ro", _connect_factory(db_path))
    monkeypatch.setattr(api_market, "cache_get_or_load", lambda _scope, _key, loader, ttl_s=0.0: loader())
    monkeypatch.setattr(api_market.time, "time", lambda: (BASE_TS_MS + (5 * 60_000)) / 1000.0)

    payload = api_operator_handlers.api_get_operator_market_data(
        {"symbol": "GLD", "tf": "1m", "limit": "10"},
        {"API_HANDLERS": {"api_get_market_candles": api_market.api_get_market_candles}},
    )

    assert payload["ok"] is True
    assert payload["symbol"] == "GLD"
    assert payload["candles"]
    assert payload["candles"][-1]["close"] == 173.0


def test_price_reader_reuses_pool_until_dsn_or_schema_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_PRICES_ENABLED", "1")
    monkeypatch.setenv("TIMESCALE_PRICES_DSN", "postgres://unit-price-test")
    monkeypatch.setenv("TIMESCALE_PRICES_SCHEMA", "prices")
    monkeypatch.setenv("TIMESCALE_PRICES_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("TIMESCALE_PRICES_POOL_MAX_SIZE", "4")
    monkeypatch.setenv("TIMESCALE_PRICES_CONNECT_TIMEOUT_S", "0.2")
    monkeypatch.setenv("ASYNC_PRICE_WRITER_ENABLED", "0")
    created: list[Any] = []

    class FakeCursor:
        def __init__(self) -> None:
            self.executed: list[tuple[str, Any]] = []

        def __enter__(self):
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: Any = None) -> None:
            self.executed.append((str(sql), params))

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()
            self.autocommit = False
            self.closed = False

        def cursor(self):
            return self.cursor_obj

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            self.closed = True

    class FakePool:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = dict(kwargs)
            self.open_calls: list[tuple[bool, float]] = []
            self.close_calls: list[float] = []
            self.getconn_calls: list[float] = []
            self.putconn_calls: list[Any] = []
            self.connection = FakeConnection()
            created.append(self)

        def open(self, *, wait: bool, timeout: float) -> None:
            self.open_calls.append((bool(wait), float(timeout)))

        def close(self, *, timeout: float) -> None:
            self.close_calls.append(float(timeout))

        def getconn(self, *, timeout: float) -> FakeConnection:
            self.getconn_calls.append(float(timeout))
            return self.connection

        def putconn(self, con: FakeConnection) -> None:
            self.putconn_calls.append(con)

    monkeypatch.setattr(price_read_router, "psycopg", object())
    monkeypatch.setattr(price_read_router, "ConnectionPool", FakePool)
    real_from_env = price_read_router.PostgresPriceStorageConfig.from_env
    from_env_calls = 0

    def counted_from_env():
        nonlocal from_env_calls
        from_env_calls += 1
        return real_from_env()

    monkeypatch.setattr(price_read_router.PostgresPriceStorageConfig, "from_env", counted_from_env)

    with price_read_router._timescale_connection() as (_con, schema):
        assert schema == "prices"
    with price_read_router._timescale_connection() as (_con, schema):
        assert schema == "prices"
    assert from_env_calls == 1
    assert all(pool_key[0] == "price_read" for pool_key in price_read_router._POOLS)
    monkeypatch.setenv("TIMESCALE_PRICES_SCHEMA", "prices_alt")
    with price_read_router._timescale_connection() as (_con, schema):
        assert schema == "prices_alt"
    assert from_env_calls == 2
    assert all(pool_key[0] == "price_read" for pool_key in price_read_router._POOLS)
    price_read_router.close_timescale_price_read_pool()

    assert len(created) == 2
    assert "unit-price-test" in str(created[0].kwargs["conninfo"])
    assert created[0].kwargs["min_size"] == 1
    assert created[0].kwargs["max_size"] == 4
    assert created[0].kwargs["check"] is price_read_router._check_timescale_price_read_connection
    assert created[0].kwargs["reset"] is price_read_router._reset_timescale_price_read_connection
    assert created[0].open_calls == [(True, 0.2)]
    assert created[0].getconn_calls == [0.2, 0.2]
    assert len(created[0].putconn_calls) == 2
    assert created[0].close_calls == [0.2]
    assert created[1].getconn_calls == [0.2]
    assert created[1].close_calls == [0.2]


def test_price_read_pool_recovers_after_failed_query_on_same_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALE_PRICES_ENABLED", "1")
    monkeypatch.setenv("TIMESCALE_PRICES_DSN", "postgres://unit-price-test")
    monkeypatch.setenv("TIMESCALE_PRICES_SCHEMA", "prices")
    monkeypatch.setenv("TIMESCALE_PRICES_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("TIMESCALE_PRICES_POOL_MAX_SIZE", "1")
    monkeypatch.setenv("TIMESCALE_PRICES_CONNECT_TIMEOUT_S", "0.2")
    monkeypatch.setenv("ASYNC_PRICE_WRITER_ENABLED", "0")

    class FakeInfo:
        transaction_status = TransactionStatus.IDLE

    class FakeCursor:
        def __init__(self, conn: "FakeConnection") -> None:
            self.conn = conn
            self.last_sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: Any = None) -> None:
            del params
            self.last_sql = str(sql)
            if "failing_query" in self.last_sql:
                self.conn.info.transaction_status = TransactionStatus.INERROR
                raise RuntimeError("originating price read failure")
            if self.conn.info.transaction_status != TransactionStatus.IDLE:
                raise RuntimeError("InFailedSqlTransaction")
            if not bool(self.conn.autocommit):
                self.conn.info.transaction_status = TransactionStatus.INTRANS

        def fetchone(self):
            return (1,)

    class FakeConnection:
        def __init__(self) -> None:
            self.info = FakeInfo()
            self.autocommit = False
            self.closed = False
            self.rollbacks = 0

        def cursor(self):
            return FakeCursor(self)

        def rollback(self) -> None:
            self.rollbacks += 1
            self.info.transaction_status = TransactionStatus.IDLE

        def close(self) -> None:
            self.closed = True

    class FakePool:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = dict(kwargs)
            self.connections = [FakeConnection(), FakeConnection()]
            self.getconn_calls: list[float] = []
            self.putconn_calls: list[FakeConnection] = []

        def open(self, *, wait: bool, timeout: float) -> None:
            del wait, timeout

        def close(self, *, timeout: float) -> None:
            del timeout

        def getconn(self, *, timeout: float) -> FakeConnection:
            self.getconn_calls.append(float(timeout))
            return self.connections[1] if self.connections[0].closed else self.connections[0]

        def putconn(self, con: FakeConnection) -> None:
            self.putconn_calls.append(con)

    price_read_router.close_timescale_price_read_pool()
    price_read_router._CONFIG = None
    price_read_router._CONFIG_KEY = None
    monkeypatch.setattr(price_read_router, "psycopg", object())
    monkeypatch.setattr(price_read_router, "ConnectionPool", FakePool)

    with pytest.raises(RuntimeError, match="originating price read failure"):
        with price_read_router._timescale_connection() as (con, _schema):
            with con.cursor() as cur:
                cur.execute("SELECT failing_query")

    pool = next(iter(price_read_router._POOLS.values()))
    first = pool.connections[0]
    assert first.rollbacks == 1
    assert first.closed is True

    with price_read_router._timescale_connection() as (con, _schema):
        with con.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    assert pool.getconn_calls == [0.2, 0.2]
    assert len(pool.putconn_calls) == 2
    assert pool.connections[1].closed is False
    price_read_router.close_timescale_price_read_pool()


def test_market_candles_dense_sqlite_history_keeps_newest_ticks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "dense-quotes.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE price_quotes (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              volume REAL,
              PRIMARY KEY(symbol, ts_ms)
            );
            """
        )
        con.executemany(
            "INSERT INTO price_quotes(ts_ms, symbol, last, volume) VALUES (?, ?, ?, ?)",
            [(BASE_TS_MS + (i * 1000), "SPY", 1000.0 + i, 1.0) for i in range(500)],
        )
        con.commit()

    monkeypatch.setattr(price_read_router, "_READ_BACKEND", "sqlite")
    monkeypatch.setattr(price_read_router, "connect_ro", _connect_factory(db_path))
    monkeypatch.setattr(api_market.time, "time", lambda: (BASE_TS_MS + 501_000) / 1000.0)

    payload = api_market.api_get_market_candles({"symbol": "SPY", "tf": "1m", "limit": "10"}, None)

    assert payload["ok"] is True
    candles = payload["candles"]
    assert candles
    _assert_ascending(candles)
    assert candles[-1]["close"] == 1499.0
    assert candles[-1]["ts_ms"] == api_market._bucket_ms(BASE_TS_MS + 499_000, 60_000)
    assert candles[0]["ts_ms"] >= api_market._bucket_ms(BASE_TS_MS + 300_000, 60_000)
    assert payload["meta"]["limit"] == 10
    assert payload["meta"]["max_points"] == 10
    assert payload["meta"]["fetch_limit"] == 200
    assert payload["meta"]["order"] == "ascending"


def test_market_candles_handler_uses_short_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    rows = [(BASE_TS_MS + (i * 60_000), float(i), 1.0) for i in range(3)]

    def _rows_since(**kwargs: Any):
        calls.append(dict(kwargs))
        return list(rows)

    monkeypatch.setattr(api_market, "_rows_since", _rows_since)
    monkeypatch.setattr(api_market.time, "time", lambda: (BASE_TS_MS + (4 * 60_000)) / 1000.0)

    first = api_market.api_get_market_candles({"symbol": "CACH", "tf": "1m", "limit": "10"}, None)
    second = api_market.api_get_market_candles({"symbol": "CACH", "tf": "1m", "limit": "10"}, None)

    assert len(calls) == 1
    assert first == second
    assert first["candles"][-1]["close"] == 2.0


def test_market_candles_falls_back_to_latest_rows_when_recent_window_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def _rows_since(**kwargs: Any):
        calls.append(dict(kwargs))
        if int(kwargs["since_ts_ms"]) > 0:
            return []
        return [(BASE_TS_MS + (i * 60_000), 100.0 + float(i), 2.0) for i in range(3)]

    monkeypatch.setattr(api_market, "_rows_since", _rows_since)
    monkeypatch.setattr(api_market, "cache_get_or_load", lambda _scope, _key, loader, ttl_s=0.0: loader())
    monkeypatch.setattr(api_market.time, "time", lambda: (BASE_TS_MS + (10 * 24 * 60 * 60_000)) / 1000.0)

    payload = api_market.api_get_market_candles({"symbol": "GLD", "tf": "1m", "limit": "10"}, None)

    assert [int(call["since_ts_ms"]) for call in calls] == [BASE_TS_MS + (10 * 24 * 60 * 60_000) - (6 * 60 * 60_000), 0]
    assert payload["ok"] is True
    assert payload["meta"]["ready"] is True
    assert payload["meta"]["stale_fallback"] is True
    assert payload["candles"]
    assert payload["candles"][0]["open"] == 100.0
    assert payload["candles"][-1]["close"] == 102.0


def test_dashboard_prices_handler_uses_short_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    from engine.api import api_dashboard_reads

    calls: list[tuple[str, int]] = []

    def fake_fetch_price_rows(*, symbol: str = "", limit: int = 200):
        calls.append((symbol, int(limit)))
        return [
            {
                "ts_ms": BASE_TS_MS,
                "symbol": symbol,
                "price": 101.0,
                "px": 101.0,
                "source": "unit",
            }
        ]

    monkeypatch.setattr(api_dashboard_reads, "fetch_price_rows", fake_fetch_price_rows)

    first = api_dashboard_reads.api_get_prices({"symbol": "CACP", "limit": "2"}, None)
    second = api_dashboard_reads.api_get_prices({"symbol": "CACP", "limit": "2"}, None)

    assert calls == [("CACP", 2)]
    assert first == second
    assert first["candles"][0]["close"] == 101.0


def test_read_caches_do_not_intercept_write_api(monkeypatch: pytest.MonkeyPatch) -> None:
    from engine.api import api_dashboard_reads, api_write

    def fail_cache(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("read cache should not be used by write paths")

    monkeypatch.setattr(api_market, "cache_get_or_load", fail_cache)
    monkeypatch.setattr(api_dashboard_reads, "cache_get_or_load", fail_cache)
    monkeypatch.setattr(price_read_router, "cache_get_or_load", fail_cache)

    con = sqlite3.connect(":memory:")
    try:
        con.execute("CREATE TABLE alerts (id INTEGER PRIMARY KEY)")
        con.execute("INSERT INTO alerts (id) VALUES (123)")
        monkeypatch.setattr(api_write, "run_write_txn", lambda fn: fn(con))
        out = api_write.ack_alert(123, who="operator", source="unit", reason="checking")
    finally:
        con.close()

    assert out["ok"] is True
    assert out["alert_id"] == 123


def test_market_candles_downsampling_preserves_latest_candle_and_limit_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [(BASE_TS_MS + (i * 60_000), float(i), 1.0) for i in range(600)]
    captured: dict[str, Any] = {}

    def _rows_since(**kwargs: Any):
        captured.update(kwargs)
        return list(reversed(rows))

    monkeypatch.setattr(api_market, "_rows_since", _rows_since)
    monkeypatch.setattr(api_market.time, "time", lambda: (BASE_TS_MS + (601 * 60_000)) / 1000.0)

    payload = api_market.api_get_market_candles(
        {"symbol": "SPY", "tf": "1m", "limit": "500", "max_points": "50"},
        None,
    )

    assert payload["ok"] is True
    candles = payload["candles"]
    assert len(candles) == 50
    _assert_ascending(candles)
    assert candles[0]["close"] == 100.0
    assert candles[-1]["close"] == 599.0
    assert captured["limit"] == 5000
    assert payload["meta"]["limit"] == 500
    assert payload["meta"]["max_points"] == 50
    assert payload["meta"]["count"] == 50
