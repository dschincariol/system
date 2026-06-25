from __future__ import annotations

import sqlite3

from engine.data import corporate_actions as ca
from engine.data import price_hygiene


def _price_db(*, with_actions: bool) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, price REAL, px REAL)")
    con.execute("INSERT INTO prices(ts_ms, symbol, price, px) VALUES (?,?,?,?)", (ca.date_to_ms("2026-01-01"), "SPY", 100.0, 100.0))
    if with_actions:
        ca.ensure_corporate_actions_tables(con)
    return con


def _split_row() -> dict:
    row = ca.normalize_corporate_action(
        {
            "ticker": "SPY",
            "split_from": 1,
            "split_to": 2,
            "execution_date": "2026-01-02",
            "declaration_date": "2025-12-15",
        },
        source="polygon_splits",
        ingested_ts_ms=ca.date_to_ms("2026-01-02"),
    )[0]
    row["availability_ts_ms"] = ca.date_to_ms("2026-01-01")
    return row


def test_known_split_date_is_accepted_without_logging(monkeypatch) -> None:
    con = _price_db(with_actions=True)
    ca.put_corporate_action_row(_split_row(), con=con)
    logged = []
    monkeypatch.setattr(price_hygiene, "PRICE_HYGIENE_USE_CORP_ACTION_CALENDAR", True)
    monkeypatch.setattr(price_hygiene, "log_split_like_price_row", lambda **kwargs: logged.append(kwargs))

    accepted, flagged = price_hygiene.filter_split_like_price_rows(
        con,
        [{"ts_ms": ca.date_to_ms("2026-01-02") + 15 * 60 * 60 * 1000, "symbol": "SPY", "price": 50.0, "source": "unit"}],
    )

    assert len(accepted) == 1
    assert flagged == []
    assert logged == []


def test_split_heuristic_still_flags_when_no_calendar_row(monkeypatch) -> None:
    con = _price_db(with_actions=True)
    logged = []
    monkeypatch.setattr(price_hygiene, "PRICE_HYGIENE_USE_CORP_ACTION_CALENDAR", True)
    monkeypatch.setattr(price_hygiene, "log_split_like_price_row", lambda **kwargs: logged.append(kwargs))

    accepted, flagged = price_hygiene.filter_split_like_price_rows(
        con,
        [{"ts_ms": ca.date_to_ms("2026-01-02") + 15 * 60 * 60 * 1000, "symbol": "SPY", "price": 50.0, "source": "unit"}],
    )

    assert accepted == []
    assert len(flagged) == 1
    assert len(logged) == 1


def test_missing_corporate_action_table_falls_back_to_heuristic(monkeypatch) -> None:
    con = _price_db(with_actions=False)
    logged = []
    monkeypatch.setattr(price_hygiene, "PRICE_HYGIENE_USE_CORP_ACTION_CALENDAR", True)
    monkeypatch.setattr(price_hygiene, "log_split_like_price_row", lambda **kwargs: logged.append(kwargs))

    accepted, flagged = price_hygiene.filter_split_like_price_rows(
        con,
        [{"ts_ms": ca.date_to_ms("2026-01-02") + 15 * 60 * 60 * 1000, "symbol": "SPY", "price": 50.0, "source": "unit"}],
    )

    assert accepted == []
    assert len(flagged) == 1
    assert len(logged) == 1
