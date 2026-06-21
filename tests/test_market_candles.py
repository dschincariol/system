from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

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


def test_timescale_quote_rows_query_bounds_newest_rows_and_returns_ascending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.executed: list[tuple[str, tuple[Any, ...]]] = []

        def __enter__(self):
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            self.executed.append((sql, params))

        def fetchall(self):
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
    monkeypatch.setattr(price_read_router, "_timescale_connection", lambda: FakeTimescaleContext(fake_con))

    rows = price_read_router._fetch_timescale_quote_rows(symbol="SPY", since_ts_ms=123, limit=2)

    assert [row[0] for row in rows] == [BASE_TS_MS + 1, BASE_TS_MS + 2]
    sql, params = fake_con.cursor_obj.executed[0]
    assert 'ORDER BY "time" DESC' in sql
    assert "LIMIT %s" in sql
    assert "ORDER BY ts_ms ASC" in sql
    assert params == ("SPY", 123, 2)


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
