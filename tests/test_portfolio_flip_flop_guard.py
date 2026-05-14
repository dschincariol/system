from __future__ import annotations

import json
import sqlite3

from engine.strategy import portfolio


def test_flip_flop_penalty_records_previous_rebalance_flip(monkeypatch) -> None:
    monkeypatch.setenv("TS_PORTFOLIO_FLIP_LAMBDA", "0.01")
    con = sqlite3.connect(":memory:")
    con.executescript(portfolio.SCHEMA)

    state = {
        "baseline:AAPL": {
            "model_id": "baseline",
            "symbol": "AAPL",
            "side": "LONG",
            "weight": 0.2,
        }
    }
    desired = {
        "baseline:AAPL": {
            "model_id": "baseline",
            "symbol": "AAPL",
            "side": "SHORT",
            "weight": 0.3,
            "reason": {},
        }
    }

    _, meta = portfolio._apply_flip_flop_penalty(con, desired, state)

    assert meta["flip_count"] == 1
    assert abs(float(meta["penalty"]) - 0.005) < 1e-12
    assert abs(float(desired["baseline:AAPL"]["reason"]["flip_flop_penalty"]["penalty"]) - 0.005) < 1e-12
    saved = json.loads(
        con.execute("SELECT value FROM portfolio_meta WHERE key='last_flip_flop_penalty'").fetchone()[0]
    )
    assert saved["flip_count"] == 1
    assert abs(float(saved["penalty"]) - 0.005) < 1e-12
