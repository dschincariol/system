from __future__ import annotations

import importlib
import json
import sqlite3

import pytest

from engine.data import corporate_actions as ca


class _NoClose:
    def __init__(self, con: sqlite3.Connection):
        self._con = con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def commit(self):
        return self._con.commit()

    def close(self):
        return None


def _make_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE predictions(ts_ms INTEGER, symbol TEXT, horizon_s INTEGER)")
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, price REAL)")
    con.execute(
        """
        CREATE TABLE labels_price(
          ts_pred_ms INTEGER NOT NULL,
          ts_eval_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          entry_price REAL NOT NULL,
          exit_price REAL NOT NULL,
          ret REAL NOT NULL,
          ret_z REAL,
          dir INTEGER,
          meta_json TEXT,
          PRIMARY KEY(ts_pred_ms, symbol, horizon_s)
        )
        """
    )
    ca.ensure_corporate_actions_tables(con)
    return con


def _run_backfill(monkeypatch, con: sqlite3.Connection, *, enabled: bool = True) -> None:
    mod = importlib.reload(importlib.import_module("engine.data.jobs.backfill_labels_price_from_prices"))
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setattr(mod, "connect", lambda: _NoClose(con))
    monkeypatch.setattr(mod, "init_db", lambda: None)
    monkeypatch.setattr(mod, "acquire_job_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(mod, "release_job_lock", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "touch_job_lock", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "put_job_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(mod, "HORIZONS", [86400])
    monkeypatch.setattr(mod, "LOOKBACK_DAYS", 10000)
    monkeypatch.setattr(mod, "LABELS_USE_CORP_ACTION_ADJUSTMENT", bool(enabled))
    mod.main()


def _seed_prediction_prices(con: sqlite3.Connection, *, symbol: str, entry_px: float, exit_px: float) -> tuple[int, int]:
    ts_pred = ca.date_to_ms("2026-01-01")
    ts_exit = ca.date_to_ms("2026-01-02")
    con.execute("INSERT INTO predictions(ts_ms, symbol, horizon_s) VALUES (?,?,?)", (ts_pred, symbol, 86400))
    con.execute("INSERT INTO prices(ts_ms, symbol, price) VALUES (?,?,?)", (ts_pred, symbol, entry_px))
    con.execute("INSERT INTO prices(ts_ms, symbol, price) VALUES (?,?,?)", (ts_exit, symbol, exit_px))
    con.commit()
    return ts_pred, ts_exit


def _label_row(con: sqlite3.Connection):
    row = con.execute("SELECT ret, meta_json FROM labels_price ORDER BY ts_pred_ms DESC LIMIT 1").fetchone()
    assert row is not None
    return float(row[0]), json.loads(row[1] or "{}")


@pytest.mark.safety_critical
def test_dividend_total_return_adjustment_offsets_ex_dividend_gap(monkeypatch) -> None:
    con = _make_db()
    _seed_prediction_prices(con, symbol="SPY", entry_px=100.0, exit_px=99.0)
    dividend = ca.normalize_corporate_action(
        {
            "ticker": "SPY",
            "cash_amount": 1.0,
            "ex_dividend_date": "2026-01-02",
            "pay_date": "2026-01-15",
            "declaration_date": "2025-12-15",
        },
        source="polygon_dividends",
        ingested_ts_ms=ca.date_to_ms("2026-01-02"),
    )[0]
    ca.put_corporate_action_row(dividend, con=con)

    _run_backfill(monkeypatch, con, enabled=True)
    adjusted_ret, meta = _label_row(con)

    assert adjusted_ret == pytest.approx(0.0)
    assert adjusted_ret >= 0.0
    assert meta["corporate_actions"]["dividend_return"] == pytest.approx(0.01)
    assert meta["corporate_actions"]["raw_return"] == pytest.approx(-0.01)

    _run_backfill(monkeypatch, con, enabled=False)
    raw_ret, raw_meta = _label_row(con)

    assert raw_ret == pytest.approx(-0.01)
    assert "corporate_actions" not in raw_meta


@pytest.mark.safety_critical
def test_split_total_return_adjustment_normalizes_halved_price(monkeypatch) -> None:
    con = _make_db()
    _seed_prediction_prices(con, symbol="SPY", entry_px=100.0, exit_px=50.0)
    split = ca.normalize_corporate_action(
        {
            "ticker": "SPY",
            "split_from": 1.0,
            "split_to": 2.0,
            "execution_date": "2026-01-02",
            "declaration_date": "2025-12-15",
        },
        source="polygon_splits",
        ingested_ts_ms=ca.date_to_ms("2026-01-02"),
    )[0]
    ca.put_corporate_action_row(split, con=con)

    _run_backfill(monkeypatch, con, enabled=True)
    adjusted_ret, meta = _label_row(con)

    assert adjusted_ret == pytest.approx(0.0)
    assert meta["corporate_actions"]["split_factor"] == pytest.approx(2.0)
    assert meta["corporate_actions"]["raw_return"] == pytest.approx(-0.5)
