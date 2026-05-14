from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from engine.api import api_replay


BASE_TS_MS = int(datetime(2026, 1, 2, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)


def _connect_factory(db_path: Path):
    def _connect():
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        return con

    return _connect


def _payload(monkeypatch: pytest.MonkeyPatch, db_path: Path, **params):
    monkeypatch.setattr(api_replay, "connect_ro", _connect_factory(db_path))
    query = {"date": "2026-01-02", "tz": "UTC", "symbol": "SPY", **params}
    return api_replay.api_get_replay_day(query, None)


def _gap_codes(payload):
    return {str(gap.get("code")) for gap in payload.get("gaps", [])}


def test_replay_day_handles_empty_day(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "empty.db"
    sqlite3.connect(str(db_path)).close()

    payload = _payload(monkeypatch, db_path)

    assert payload["ok"] is True
    assert payload["read_only"] is True
    assert payload["meta"]["ready"] is False
    assert payload["meta"]["counts"] == {
        "candles": 0,
        "decisions": 0,
        "orders": 0,
        "fills": 0,
        "risk": 0,
        "pnl": 0,
    }
    assert "no_data_for_date" in _gap_codes(payload)


def test_replay_day_storage_unavailable_returns_structured_503(monkeypatch: pytest.MonkeyPatch) -> None:
    from engine.runtime.storage_pool import StoragePoolTimeout

    def _unavailable():
        raise StoragePoolTimeout("couldn't get a connection after 0.05 sec")

    monkeypatch.setattr(api_replay, "connect_ro", _unavailable)

    payload = api_replay.api_get_replay_day(
        {"date": "2026-01-02", "tz": "UTC", "symbol": "SPY"},
        None,
    )

    assert payload["ok"] is False
    assert payload["error"] == "storage_unavailable"
    assert payload["meta"]["status"] == 503
    assert payload["storage"]["status"] in {"unknown", "unavailable"}


def test_replay_day_handles_partial_day_and_malformed_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "partial.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE price_quotes (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              volume REAL
            );
            CREATE TABLE decision_log (
              id INTEGER PRIMARY KEY,
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              model_name TEXT,
              predicted_z REAL,
              confidence REAL,
              extra_json TEXT,
              explain_json TEXT,
              components_json TEXT
            );
            """
        )
        con.executemany(
            "INSERT INTO price_quotes(ts_ms, symbol, last, volume) VALUES (?, ?, ?, ?)",
            [
                (BASE_TS_MS, "SPY", 100.0, 10.0),
                (BASE_TS_MS + 60_000, "SPY", 101.0, 12.0),
            ],
        )
        con.execute(
            """
            INSERT INTO decision_log(
              id, ts_ms, symbol, model_name, predicted_z, confidence,
              extra_json, explain_json, components_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, BASE_TS_MS + 30_000, "SPY", "model-a", 0.7, 0.82, "{bad-json", "{}", "{}"),
        )

    payload = _payload(monkeypatch, db_path)

    assert payload["meta"]["ready"] is True
    assert payload["meta"]["counts"]["candles"] == 2
    assert payload["meta"]["counts"]["decisions"] == 1
    assert payload["meta"]["counts"]["orders"] == 0
    assert payload["meta"]["counts"]["fills"] == 0
    assert payload["decisions"][0]["label"] == "LONG"
    codes = _gap_codes(payload)
    assert "malformed_json" in codes
    assert "order_tables_missing" in codes
    assert "fill_tables_missing" in codes
    assert "risk_history_missing" in codes


def test_replay_day_handles_full_day(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "full.db"
    with sqlite3.connect(str(db_path)) as con:
        con.executescript(
            """
            CREATE TABLE price_quotes (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              last REAL,
              volume REAL
            );
            CREATE TABLE decision_log (
              id INTEGER PRIMARY KEY,
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              model_name TEXT,
              predicted_z REAL,
              confidence REAL,
              extra_json TEXT,
              explain_json TEXT,
              components_json TEXT
            );
            CREATE TABLE portfolio_orders (
              id INTEGER PRIMARY KEY,
              ts_ms INTEGER NOT NULL,
              model_id TEXT,
              symbol TEXT,
              action TEXT,
              from_side TEXT,
              to_side TEXT,
              delta_weight REAL,
              source_alert_id INTEGER
            );
            CREATE TABLE broker_order_state (
              id INTEGER PRIMARY KEY,
              source_order_id TEXT,
              symbol TEXT,
              state TEXT,
              created_ts_ms INTEGER,
              updated_ts_ms INTEGER,
              meta_json TEXT
            );
            CREATE TABLE broker_fills (
              id INTEGER PRIMARY KEY,
              ts_ms INTEGER NOT NULL,
              symbol TEXT,
              qty REAL,
              px REAL,
              source_order_id TEXT,
              note TEXT,
              explain_json TEXT
            );
            CREATE TABLE portfolio_risk_snapshots (
              ts_ms INTEGER PRIMARY KEY,
              gross REAL,
              net REAL,
              vol_proxy REAL,
              drawdown REAL,
              blocked INTEGER,
              info_json TEXT
            );
            CREATE TABLE equity_history (
              ts_ms INTEGER PRIMARY KEY,
              equity REAL
            );
            CREATE TABLE broker_account (
              ts_ms INTEGER PRIMARY KEY,
              updated_ts_ms INTEGER,
              equity REAL,
              cash REAL,
              day_pnl REAL,
              unrealized_pnl REAL,
              realized_pnl REAL
            );
            """
        )
        con.executemany(
            "INSERT INTO price_quotes(ts_ms, symbol, last, volume) VALUES (?, ?, ?, ?)",
            [
                (BASE_TS_MS, "SPY", 100.0, 10.0),
                (BASE_TS_MS + 60_000, "SPY", 101.0, 11.0),
                (BASE_TS_MS + 120_000, "SPY", 99.5, 9.0),
            ],
        )
        con.execute(
            """
            INSERT INTO decision_log(
              id, ts_ms, symbol, model_name, predicted_z, confidence,
              extra_json, explain_json, components_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, BASE_TS_MS + 30_000, "SPY", "model-a", 0.4, 0.75, '{"action":"BUY"}', "{}", "{}"),
        )
        con.execute(
            """
            INSERT INTO portfolio_orders(
              id, ts_ms, model_id, symbol, action, from_side, to_side,
              delta_weight, source_alert_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, BASE_TS_MS + 40_000, "model-a", "SPY", "BUY", "FLAT", "LONG", 0.2, 7),
        )
        con.execute(
            """
            INSERT INTO broker_order_state(
              id, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "ord-1", "SPY", "filled", BASE_TS_MS + 45_000, BASE_TS_MS + 55_000, "{}"),
        )
        con.execute(
            """
            INSERT INTO broker_fills(
              id, ts_ms, symbol, qty, px, source_order_id, note, explain_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, BASE_TS_MS + 55_000, "SPY", 10.0, 100.25, "ord-1", "filled", "{}"),
        )
        con.execute(
            """
            INSERT INTO portfolio_risk_snapshots(
              ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (BASE_TS_MS + 50_000, 0.4, 0.2, 0.1, 0.02, 0, '{"limit":"ok"}'),
        )
        con.execute(
            "INSERT INTO equity_history(ts_ms, equity) VALUES (?, ?)",
            (BASE_TS_MS + 50_000, 100100.0),
        )
        con.execute(
            """
            INSERT INTO broker_account(
              ts_ms, updated_ts_ms, equity, cash, day_pnl, unrealized_pnl, realized_pnl
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (BASE_TS_MS + 50_000, BASE_TS_MS + 50_000, 100100.0, 50000.0, 100.0, 20.0, 80.0),
        )

    payload = _payload(monkeypatch, db_path)

    assert payload["meta"]["ready"] is True
    assert payload["meta"]["counts"]["candles"] == 3
    assert payload["meta"]["counts"]["decisions"] == 1
    assert payload["meta"]["counts"]["orders"] == 2
    assert payload["meta"]["counts"]["fills"] == 1
    assert payload["meta"]["counts"]["risk"] == 1
    assert payload["meta"]["counts"]["pnl"] == 2
    assert payload["fills"][0]["side"] == "BUY"
    assert "no_data_for_date" not in _gap_codes(payload)
