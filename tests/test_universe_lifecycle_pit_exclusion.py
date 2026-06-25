from __future__ import annotations

import json
import sqlite3

import pytest

from engine.data.universe_lifecycle import run_lifecycle_once
from engine.data.universe_pit import backfill_universe_pit, get_pit_universe_symbols

DAY_MS = 24 * 60 * 60 * 1000


def _con() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.executescript(
        """
        CREATE TABLE symbols(
          symbol TEXT PRIMARY KEY,
          asset_class TEXT NOT NULL DEFAULT 'UNKNOWN',
          status TEXT NOT NULL DEFAULT 'WATCH',
          score REAL NOT NULL DEFAULT 0.0,
          last_seen_event_ts_ms INTEGER,
          last_traded_ts_ms INTEGER,
          meta_json TEXT,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL
        );
        CREATE TABLE prices(
          ts_ms INTEGER,
          symbol TEXT,
          price REAL,
          px REAL,
          source TEXT
        );
        CREATE TABLE price_quotes(
          ts_ms INTEGER,
          symbol TEXT,
          bid REAL,
          ask REAL,
          source TEXT
        );
        CREATE TABLE universe_audit(
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          status_before TEXT,
          status_after TEXT,
          include INTEGER NOT NULL,
          score REAL,
          reasons_json TEXT,
          features_json TEXT,
          PRIMARY KEY(ts_ms, symbol)
        );
        """
    )
    return con


def _seed_symbol(con: sqlite3.Connection, symbol: str, *, created: int, updated: int) -> None:
    con.execute(
        """
        INSERT INTO symbols(
          symbol, asset_class, status, score, last_seen_event_ts_ms,
          last_traded_ts_ms, meta_json, created_ts_ms, updated_ts_ms
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (symbol, "EQUITY", "ACTIVE", 1.0, None, None, "{}", int(created), int(updated)),
    )


@pytest.mark.safety_critical
def test_lifecycle_retirement_excludes_symbol_from_pit_after_last_seen(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "1")
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_REFERENCE_ENABLED", "0")
    con = _con()
    now_ms = 100 * DAY_MS
    delist_ts_ms = now_ms - (60 * DAY_MS)
    _seed_symbol(con, "SPY", created=delist_ts_ms - DAY_MS, updated=now_ms)
    _seed_symbol(con, "QQQ", created=delist_ts_ms - DAY_MS, updated=now_ms)
    con.executemany(
        "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
        [
            (delist_ts_ms, "SPY", 10.0, 10.0, "test"),
            (now_ms - DAY_MS, "QQQ", 20.0, 20.0, "test"),
        ],
    )

    lifecycle_summary = run_lifecycle_once(con, now_ms=now_ms, stale_ms=45 * DAY_MS)
    pit_summary = backfill_universe_pit(con, now_ts_ms=now_ms)
    con.commit()

    assert lifecycle_summary["retired"] == 1
    assert pit_summary["row_count"] == 2
    assert "SPY" in get_pit_universe_symbols(con, ts_ms=delist_ts_ms)
    assert "SPY" not in get_pit_universe_symbols(con, ts_ms=delist_ts_ms + 1)
    assert "QQQ" in get_pit_universe_symbols(con, ts_ms=now_ms)
    row = con.execute(
        "SELECT last_seen_ts, is_active, metadata_json FROM universe_pit WHERE symbol='SPY'"
    ).fetchone()
    assert row is not None
    assert int(row[0]) == delist_ts_ms
    assert int(row[1]) == 0
    metadata = json.loads(str(row[2] or "{}"))
    assert metadata["lifecycle_delist_ts"] == delist_ts_ms
    assert metadata["delisted_inferred"] is True
