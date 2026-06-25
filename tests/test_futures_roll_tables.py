from __future__ import annotations

import sqlite3

from engine.data.futures_roll import ensure_futures_roll_tables


def test_ensure_futures_roll_tables_is_idempotent_and_round_trips_float() -> None:
    con = sqlite3.connect(":memory:")
    try:
        ensure_futures_roll_tables(con)
        ensure_futures_roll_tables(con)

        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'futures_%' ORDER BY name"
            )
        }
        assert "futures_roll_calendar" in tables
        assert "futures_continuous_bars" in tables
        assert "futures_roll_yield" in tables

        con.execute(
            "INSERT INTO futures_roll_yield(root, ts_ms, roll_yield) VALUES (?,?,?)",
            ("ES", 1_000, 0.1234),
        )
        row = con.execute("SELECT roll_yield FROM futures_roll_yield WHERE root=? AND ts_ms=?", ("ES", 1_000)).fetchone()
        assert isinstance(row[0], float)
        assert row[0] == 0.1234
    finally:
        con.close()
