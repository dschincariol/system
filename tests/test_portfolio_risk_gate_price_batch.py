from __future__ import annotations

import sqlite3

from engine.strategy import portfolio_risk_gate


class CountingCon:
    def __init__(self, inner: sqlite3.Connection) -> None:
        self.inner = inner
        self.latest_price_queries = 0

    def execute(self, sql: str, params=()):
        text = str(sql)
        if "ROW_NUMBER()" in text and "FROM prices" in text:
            self.latest_price_queries += 1
        return self.inner.execute(sql, params)


def _table_exists(con: CountingCon, table: str) -> bool:
    row = con.inner.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return bool(row)


def test_execution_exposure_caps_batch_latest_prices_for_quantity_orders(monkeypatch):
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(symbol TEXT NOT NULL, ts_ms INTEGER NOT NULL, price REAL)")
    con.executemany(
        "INSERT INTO prices(symbol, ts_ms, price) VALUES (?, ?, ?)",
        [
            ("AAPL", 1000, 100.0),
            ("AAPL", 900, 90.0),
            ("MSFT", 1100, 200.0),
        ],
    )
    counting = CountingCon(con)
    monkeypatch.setattr(portfolio_risk_gate, "table_exists", _table_exists)
    monkeypatch.setenv("EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP", "1.0")
    monkeypatch.setenv("EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP", "1.0")

    orders = [
        {"symbol": "AAPL", "to_side": "LONG", "qty": 100.0},
        {"symbol": "MSFT", "to_side": "LONG", "qty": 50.0},
    ]

    routed, info = portfolio_risk_gate._apply_execution_exposure_caps(
        counting,
        orders,
        equity_usd=100_000.0,
    )

    assert counting.latest_price_queries == 1
    assert info["ok"] is True
    assert [order["symbol"] for order in routed] == ["AAPL", "MSFT"]
    assert abs(float(routed[0]["to_weight"]) - 0.10) < 1e-12
    assert abs(float(routed[1]["to_weight"]) - 0.10) < 1e-12
