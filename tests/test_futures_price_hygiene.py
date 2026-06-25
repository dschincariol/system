from __future__ import annotations

import sqlite3


def _price_db(symbol: str, price: float = 100.0):
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, price REAL, px REAL)")
    con.execute("INSERT INTO prices(ts_ms, symbol, price, px) VALUES (?,?,?,?)", (1_000, symbol, price, price))
    return con


def test_futures_roll_like_gap_is_accepted_by_split_filter(monkeypatch) -> None:
    from engine.data import price_hygiene

    monkeypatch.setattr(price_hygiene, "log_failure", lambda *args, **kwargs: None)
    con = _price_db("ES.c.0", 100.0)
    try:
        accepted, flagged = price_hygiene.filter_split_like_price_rows(
            con,
            [{"ts_ms": 2_000, "symbol": "ES.c.0", "price": 200.0, "source": "unit"}],
        )
    finally:
        con.close()

    assert len(accepted) == 1
    assert accepted[0]["symbol"] == "ES.c.0"
    assert flagged == []


def test_equity_split_like_gap_is_still_flagged(monkeypatch) -> None:
    from engine.data import price_hygiene

    monkeypatch.setattr(price_hygiene, "log_failure", lambda *args, **kwargs: None)
    con = _price_db("SPY", 100.0)
    try:
        accepted, flagged = price_hygiene.filter_split_like_price_rows(
            con,
            [{"ts_ms": 2_000, "symbol": "SPY", "price": 200.0, "source": "unit"}],
        )
    finally:
        con.close()

    assert accepted == []
    assert len(flagged) == 1
    assert flagged[0]["symbol"] == "SPY"


def test_equity_hygiene_thresholds_remain_unchanged() -> None:
    from engine.data import price_hygiene

    assert price_hygiene.SPLIT_DOWN_RETURN == -0.45
    assert price_hygiene.SPLIT_UP_RETURN == 0.90
    assert price_hygiene.is_split_like_price_jump(100.0, 150.0) is False
    assert price_hygiene.is_split_like_price_jump(100.0, 200.0) is True
