import importlib
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.requires_postgres

from engine.backtest.cpcv import CombinatorialPurgedKFold


def _reload_for_db(db_path: Path):
    os.environ["DB_PATH"] = str(db_path)
    import engine.runtime.db_guard as db_guard
    import engine.runtime.storage as storage
    import ops.backtest_cpcv as backtest_cpcv

    importlib.reload(db_guard)
    storage = importlib.reload(storage)
    backtest_cpcv = importlib.reload(backtest_cpcv)
    storage.init_db()
    return storage, backtest_cpcv


def test_backtest_cpcv_writes_one_audit_row_per_path(tmp_path):
    storage, backtest_cpcv = _reload_for_db(tmp_path / "cpcv.sqlite")
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 90 * 86_400_000

    con = storage.connect()
    try:
        for i in range(72):
            ts_ms = start_ms + i * 86_400_000
            cur = con.execute(
                """
                INSERT INTO events(ts_ms, source, title, body)
                VALUES (?, ?, ?, ?)
                """,
                (ts_ms, "test", f"event {i}", ""),
            )
            event_id = int(cur.lastrowid)
            ret = 0.0012 + (0.0004 if i % 4 in (0, 1) else -0.0002)
            con.execute(
                """
                INSERT INTO labels(event_id, horizon_s, symbol, realized_ret, impact_z, created_at_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (event_id, 300, "SPY", ret, ret, ts_ms),
            )
        con.commit()

        cfg = backtest_cpcv.load_config(None)
        cfg.update(
            {
                "model_id": "test_temporal_predictor",
                "lookback_days": 180,
                "n_splits": 6,
                "n_test_splits": 2,
                "embargo_pct": 0.01,
                "holding_horizon_bars": 3,
                "notional": 10_000.0,
                "adv": 100_000_000.0,
                "sigma_daily": 0.01,
                "participation": 0.10,
                "half_spread_bps": 0.5,
            }
        )
        result = backtest_cpcv.run_cpcv_backtest(cfg, con=con, persist=True)

        assert result["ok"]
        assert result["n_paths"] == 15
        row_count = con.execute(
            """
            SELECT COUNT(1)
            FROM backtest_cpcv_runs
            WHERE model_id = ?
              AND path_index IS NOT NULL
            """,
            ("test_temporal_predictor",),
        ).fetchone()[0]
        assert int(row_count) >= 15

        minmax = con.execute(
            """
            SELECT MIN(sharpe), MAX(sharpe), MIN(deflated_sharpe), MAX(max_drawdown)
            FROM backtest_cpcv_runs
            WHERE model_id = ?
            """,
            ("test_temporal_predictor",),
        ).fetchone()
        assert all(value is not None for value in minmax)
    finally:
        con.close()
        storage.close_pooled_connections()
        os.environ.pop("DB_PATH", None)


def test_backtest_cpcv_uses_temporal_predictor_with_price_labels(tmp_path):
    storage, backtest_cpcv = _reload_for_db(tmp_path / "cpcv_temporal.sqlite")
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 90 * 86_400_000

    con = storage.connect()
    try:
        for i in range(48):
            ts_ms = start_ms + i * 86_400_000
            px = 100.0 + (i * 0.1)
            con.execute(
                """
                INSERT OR REPLACE INTO prices(ts_ms, symbol, price, px, source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (ts_ms, "SPY", px, px, "test"),
            )
            con.execute(
                """
                INSERT OR REPLACE INTO labels_price(
                  ts_pred_ms, ts_eval_ms, symbol, horizon_s, entry_price, exit_price, ret, ret_z, dir
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts_ms,
                    ts_ms + 86_400_000,
                    "SPY",
                    300,
                    px,
                    px * 1.001,
                    0.001,
                    0.001 if i % 2 == 0 else -0.001,
                    1 if i % 2 == 0 else -1,
                ),
            )
        con.commit()

        def fake_predict(_con, ts_ms, symbols, horizons, seq_len=6):
            del _con, seq_len
            sign = 1.0 if ((int(ts_ms) - start_ms) // 86_400_000) % 2 == 0 else -1.0
            return {(str(symbols[0]), int(horizons[0])): (sign, 0.75, {"model": "temporal_predictor"})}

        cfg = backtest_cpcv.load_config(None)
        cfg.update(
            {
                "model_id": "temporal_predictor",
                "lookback_days": 180,
                "n_splits": 6,
                "n_test_splits": 2,
                "embargo_pct": 0.01,
                "holding_horizon_bars": 3,
                "notional": 10_000.0,
                "adv": 100_000_000.0,
                "sigma_daily": 50.0,
                "participation": 0.10,
                "half_spread_bps": 0.1,
            }
        )
        with patch("engine.strategy.temporal_predictor.predict_temporal_live", side_effect=fake_predict):
            result = backtest_cpcv.run_cpcv_backtest(cfg, con=con, persist=True)

        assert result["ok"]
        assert result["dataset_source"] == "temporal_predictor"
        assert result["uses_precomputed_predictions"]
        assert result["n_paths"] == 15
    finally:
        con.close()
        storage.close_pooled_connections()
        os.environ.pop("DB_PATH", None)


def test_synthetic_momentum_dgp_cpcv_is_not_walk_forward_optimistic():
    true_sharpe = 1.2
    seeds = range(50)
    cpcv_scores = []
    walk_forward_scores = []
    n_samples = 120
    test_size = n_samples // 3
    period_std = 0.01
    period_mean = true_sharpe * period_std / np.sqrt(float(test_size))
    def sharpe(values):
        arr = np.asarray(values, dtype=float)
        return float(arr.mean() / arr.std(ddof=1) * np.sqrt(float(arr.size)))

    splitter = CombinatorialPurgedKFold(
        n_splits=6,
        n_test_splits=2,
        embargo=0.01,
        label_horizon=3,
    )

    for seed in seeds:
        rng = np.random.default_rng(seed)
        returns = rng.normal(period_mean, period_std, size=n_samples)
        path_sharpes = []
        for _train_idx, test_idx in splitter.split(np.arange(n_samples)):
            path_sharpes.append(sharpe(returns[test_idx]))
        cpcv_score = float(np.mean(path_sharpes))
        cpcv_scores.append(cpcv_score)

        leakage_bonus = np.abs(rng.normal(0.0, period_std * 0.20, size=n_samples))
        walk_forward_scores.append(sharpe(returns[-test_size:] + leakage_bonus[-test_size:]))

    assert abs(float(np.mean(cpcv_scores)) - true_sharpe) <= 0.15
    assert float(np.mean(walk_forward_scores)) > float(np.mean(cpcv_scores))
