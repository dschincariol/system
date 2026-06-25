from __future__ import annotations

import json
import sqlite3
import uuid

import pytest

from engine.data.universe import get_active_symbols
from engine.data.universe_lifecycle import run_lifecycle_once

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


def _seed_symbol(con: sqlite3.Connection, symbol: str, *, status: str = "ACTIVE", score: float = 1.0) -> None:
    con.execute(
        """
        INSERT INTO symbols(
          symbol, asset_class, status, score, last_seen_event_ts_ms,
          last_traded_ts_ms, meta_json, created_ts_ms, updated_ts_ms
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (symbol, "EQUITY", status, score, None, None, "{}", 1000, 1000),
    )


def _meta(con: sqlite3.Connection, symbol: str) -> dict:
    raw = con.execute("SELECT meta_json FROM symbols WHERE symbol=?", (symbol,)).fetchone()[0]
    return json.loads(str(raw or "{}"))


@pytest.mark.safety_critical
def test_stale_equity_retirement_updates_status_audit_and_live_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "1")
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_REFERENCE_ENABLED", "0")
    con = _con()
    now_ms = 100 * DAY_MS
    stale_price_ts = now_ms - (60 * DAY_MS)
    fresh_price_ts = now_ms - DAY_MS
    for symbol in ("SPY", "QQQ", "BTC", "EURUSD"):
        _seed_symbol(con, symbol)
    con.executemany(
        "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
        [
            (stale_price_ts, "SPY", 10.0, 10.0, "test"),
            (fresh_price_ts, "QQQ", 20.0, 20.0, "test"),
            (stale_price_ts, "BTC", 30.0, 30.0, "test"),
            (stale_price_ts, "EURUSD", 1.1, 1.1, "test"),
        ],
    )

    summary = run_lifecycle_once(con, now_ms=now_ms, stale_ms=45 * DAY_MS)

    assert summary["ok"] is True
    assert summary["retired"] == 1
    assert summary["reason_counts"] == {"stale_inactive": 1}
    assert con.execute("SELECT status FROM symbols WHERE symbol='SPY'").fetchone()[0] == "DISABLED"
    assert con.execute("SELECT status FROM symbols WHERE symbol='QQQ'").fetchone()[0] == "ACTIVE"
    assert con.execute("SELECT status FROM symbols WHERE symbol='BTC'").fetchone()[0] == "ACTIVE"
    assert con.execute("SELECT status FROM symbols WHERE symbol='EURUSD'").fetchone()[0] == "ACTIVE"
    assert "SPY" not in get_active_symbols(con, limit=None)

    row = con.execute(
        """
        SELECT status_before, status_after, include, reasons_json, features_json
        FROM universe_audit
        WHERE symbol='SPY'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "ACTIVE"
    assert row[1] == "DISABLED"
    assert int(row[2]) == 0
    assert json.loads(row[3])["reason"] == "stale_inactive"
    assert json.loads(row[4])["last_market_ts_ms"] == stale_price_ts
    assert _meta(con, "SPY")["lifecycle"]["retired"] is True
    assert _meta(con, "SPY")["lifecycle"]["delist_ts_ms"] == stale_price_ts
    assert con.execute("SELECT last_traded_ts_ms FROM symbols WHERE symbol='SPY'").fetchone()[0] == stale_price_ts

    summary_2 = run_lifecycle_once(con, now_ms=now_ms + 1, stale_ms=45 * DAY_MS)
    assert summary_2["retired"] == 0
    assert con.execute("SELECT COUNT(*) FROM universe_audit WHERE symbol='SPY'").fetchone()[0] == 1


@pytest.mark.safety_critical
def test_reference_delist_and_rename_record_lineage_without_successor_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "1")
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_REFERENCE_ENABLED", "1")
    con = _con()
    _seed_symbol(con, "QQQ")
    _seed_symbol(con, "DIA")

    def fetch_reference(symbol: str) -> dict:
        if symbol == "QQQ":
            return {"results": {"ticker": "QQQ", "active": False}}
        if symbol == "DIA":
            return {"results": {"ticker": "DIA_NEW", "active": True}}
        return {"results": {"ticker": symbol, "active": True}}

    summary = run_lifecycle_once(con, now_ms=2000, fetch_reference=fetch_reference)

    assert summary["retired"] == 2
    assert summary["reason_counts"] == {"delisted_reference": 1, "renamed_reference": 1}
    assert con.execute("SELECT status FROM symbols WHERE symbol='QQQ'").fetchone()[0] == "DISABLED"
    assert con.execute("SELECT status FROM symbols WHERE symbol='DIA'").fetchone()[0] == "DISABLED"
    assert _meta(con, "DIA")["lifecycle"]["renamed_to"] == "DIA_NEW"
    assert con.execute("SELECT COUNT(*) FROM symbols WHERE symbol='DIA_NEW'").fetchone()[0] == 0


@pytest.mark.safety_critical
def test_reference_evidence_scrubs_secret_like_payload_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "1")
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_REFERENCE_ENABLED", "1")
    canary = f"canary-{uuid.uuid4()}"
    con = _con()
    _seed_symbol(con, "VOO")

    def fetch_reference(symbol: str) -> dict:
        return {
            "results": {
                "ticker": symbol,
                "active": False,
                "apiKey": canary,
                "authorization": canary,
                "raw_secret": canary,
            }
        }

    summary = run_lifecycle_once(con, now_ms=2000, fetch_reference=fetch_reference)
    serialized = json.dumps(
        {
            "summary": summary,
            "audit": con.execute("SELECT reasons_json, features_json FROM universe_audit").fetchall(),
            "meta": con.execute("SELECT meta_json FROM symbols").fetchall(),
        },
        sort_keys=True,
        default=str,
    )

    assert summary["retired"] == 1
    assert canary not in serialized


@pytest.mark.safety_critical
def test_default_off_lifecycle_makes_no_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UNIVERSE_LIFECYCLE_ENABLED", "0")
    con = _con()
    _seed_symbol(con, "SPY")
    con.execute(
        "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
        (1, "SPY", 10.0, 10.0, "test"),
    )

    summary = run_lifecycle_once(con, now_ms=100 * DAY_MS, stale_ms=45 * DAY_MS)

    assert summary == {"ok": True, "enabled": False, "scanned": 0, "retired": 0}
    assert con.execute("SELECT status FROM symbols WHERE symbol='SPY'").fetchone()[0] == "ACTIVE"
    assert con.execute("SELECT COUNT(*) FROM universe_audit").fetchone()[0] == 0
