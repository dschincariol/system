from __future__ import annotations

import sqlite3
import time

import numpy as np

from engine.backtest.cpcv import CombinatorialPurgedKFold, purged_train_indices
from engine.data.futures_roll import load_futures_roll_boundaries
from engine.strategy.jobs.backtest_cpcv import run_cpcv_backtest


def test_roll_times_purge_train_samples_that_straddle_roll_boundary() -> None:
    starts = np.arange(10, dtype=float)
    ends = starts + 2.0
    train = [0, 1, 2, 3, 4, 5, 7, 8, 9]
    test = [6]

    purged = purged_train_indices(
        train,
        test,
        label_start_times=starts,
        label_end_times=ends,
        roll_times=[5.5],
        n_samples=10,
    )

    assert 4 not in set(purged.tolist())
    assert 5 not in set(purged.tolist())
    for train_i in purged:
        assert not (starts[int(train_i)] <= 5.5 <= ends[int(train_i)])


def test_cpcv_splits_are_identical_when_no_roll_dates_are_supplied() -> None:
    X = np.arange(24)
    starts = np.arange(24, dtype=float)
    ends = starts + 1.0
    baseline = CombinatorialPurgedKFold(
        n_splits=6,
        n_test_splits=2,
        label_start_times=starts,
        label_end_times=ends,
    )
    with_empty_rolls = CombinatorialPurgedKFold(
        n_splits=6,
        n_test_splits=2,
        label_start_times=starts,
        label_end_times=ends,
        roll_times=[],
    )

    baseline_splits = [(train.tolist(), test.tolist()) for train, test in baseline.split(X)]
    roll_splits = [(train.tolist(), test.tolist()) for train, test in with_empty_rolls.split(X)]

    assert roll_splits == baseline_splits


def test_production_cpcv_job_loads_futures_roll_boundaries_from_db() -> None:
    con = sqlite3.connect(":memory:")
    try:
        con.executescript(
            """
            CREATE TABLE labels_price(
              ts_pred_ms INTEGER,
              ts_eval_ms INTEGER,
              symbol TEXT,
              horizon_s INTEGER,
              ret REAL,
              ret_z REAL,
              dir INTEGER
            );
            CREATE TABLE futures_roll_calendar(
              root TEXT NOT NULL,
              roll_ts_ms INTEGER NOT NULL,
              from_contract TEXT NOT NULL,
              to_contract TEXT NOT NULL,
              gap_ratio REAL NOT NULL,
              method TEXT NOT NULL,
              ingested_ts_ms INTEGER,
              PRIMARY KEY(root, roll_ts_ms)
            );
            """
        )
        base = int(time.time() * 1000) - 60_000
        horizon_ms = 300_000
        for idx in range(9):
            ts_ms = base + idx * horizon_ms
            con.execute(
                """
                INSERT INTO labels_price(ts_pred_ms, ts_eval_ms, symbol, horizon_s, ret, ret_z, dir)
                VALUES (?,?,?,?,?,?,?)
                """,
                (ts_ms, ts_ms + horizon_ms, "ES.c.0", 300, 0.001 * (idx + 1), 0.1 * (idx + 1), 1),
            )
        roll_ts = base + 4 * horizon_ms + 150_000
        con.execute(
            """
            INSERT INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
            VALUES (?,?,?,?,?,?,?)
            """,
            ("ES", roll_ts, "ESH26", "ESM26", 1.01, "oi_volume", base),
        )
        con.commit()

        loaded = load_futures_roll_boundaries(con, symbols=["ES.c.0"], start_ts_ms=base, end_ts_ms=base + 10 * horizon_ms)
        assert loaded == [roll_ts]

        result = run_cpcv_backtest(
            {"n_splits": 3, "n_test_splits": 1, "holding_horizon_bars": 1, "lookback_days": 1},
            con=con,
            persist=False,
        )

        assert result["ok"] is True
        assert result["futures_roll_boundary_count"] == 1
        assert result["futures_roll_boundaries"] == [roll_ts]
    finally:
        con.close()
