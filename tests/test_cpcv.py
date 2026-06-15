from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.requires_postgres


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class CPCVTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "cpcv_test.db")
        os.environ["CHAMPION_PROMOTION_COOLDOWN_S"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_TRADES"] = "3"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATION_S"] = "1"
        os.environ["CHAMPION_PROMOTION_MIN_SCORE"] = "0.9"
        os.environ["CHAMPION_PROMOTION_MIN_NET_PNL_DELTA"] = "0"
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
        self._reload_runtime_modules()

    def tearDown(self) -> None:
        for key in (
            "DB_PATH",
            "CHAMPION_PROMOTION_COOLDOWN_S",
            "CHAMPION_PROMOTION_MIN_TRADES",
            "CHAMPION_PROMOTION_MIN_OBSERVATION_S",
            "CHAMPION_PROMOTION_MIN_SCORE",
            "CHAMPION_PROMOTION_MIN_NET_PNL_DELTA",
            "CHAMPION_PROMOTION_USE_STAT_GATE",
            "CPCV_ENABLED",
            "CPCV_N_SPLITS",
            "CPCV_N_TEST_SPLITS",
            "CPCV_EMBARGO_PCT",
            "CPCV_LABEL_HORIZON",
            "CPCV_MAX_PBO",
            "CPCV_MIN_PATH_SHARPE",
        ):
            os.environ.pop(key, None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _reload_runtime_modules(self) -> None:
        modules = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.cpcv",
            "engine.strategy.promotion_guard",
            "engine.strategy.champion_manager",
        )
        self.storage = modules[1]
        self.cpcv = modules[2]
        self.promotion_guard = modules[3]
        self.champion_manager = modules[4]
        self.storage.init_db()

    def _insert_marketplace_row(
        self,
        *,
        model_id: str,
        model_name: str,
        symbol: str,
        horizon_s: int,
        score: float,
        net_pnl: float,
        trades: int,
        wins: int,
        losses: int,
        first_signal_ts_ms: int,
        last_signal_ts_ms: int,
        stage: str,
        realized_trade_pnls: list[float] | None = None,
    ) -> None:
        meta = {
            "score_source": "pnl_attribution",
            "risk_adjusted_score": float(score),
            "rolling_realized_pnl": float(net_pnl),
            "rolling_unrealized_pnl": 0.0,
            "rolling_total_pnl": float(net_pnl),
            "realized_pnl": float(net_pnl),
            "unrealized_pnl": 0.0,
            "total_pnl": float(net_pnl),
            "transaction_cost": 0.0,
            "rolling_window_ms": 86_400_000,
            "observation_duration_ms": int(last_signal_ts_ms - first_signal_ts_ms),
            "first_signal_ts_ms": int(first_signal_ts_ms),
            "last_signal_ts_ms": int(last_signal_ts_ms),
            "recent_total_pnl": float(net_pnl),
            "prior_total_pnl": 0.0,
            "max_drawdown": 0.0,
            "model_kind": "test_model",
            "model_ts_ms": int(first_signal_ts_ms),
        }
        if realized_trade_pnls is not None:
            meta["realized_trade_pnls"] = [float(value) for value in list(realized_trade_pnls)]

        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO model_marketplace_scores(
                  model_id, model_name, symbol, horizon_s, regime, stage, score, trades, wins, losses,
                  gross_pnl, net_pnl, avg_confidence, last_signal_ts_ms, updated_ts_ms, meta_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(model_id),
                    str(model_name),
                    str(symbol),
                    int(horizon_s),
                    "global",
                    str(stage),
                    float(score),
                    int(trades),
                    int(wins),
                    int(losses),
                    float(net_pnl),
                    float(net_pnl),
                    0.75,
                    int(last_signal_ts_ms),
                    int(last_signal_ts_ms),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

    def _seed_competition_pair(self, challenger_name: str) -> dict:
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        self._insert_marketplace_row(
            model_id="current_champion",
            model_name="current_champion",
            symbol="AAPL",
            horizon_s=300,
            score=0.60,
            net_pnl=10.0,
            trades=5,
            wins=4,
            losses=1,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="champion",
            realized_trade_pnls=[2.0, 2.0, 2.0, 2.0, 2.0],
        )
        challenger_returns = [3.0, 2.5, 2.75, 3.25, 2.6, 2.9]
        self._insert_marketplace_row(
            model_id=challenger_name,
            model_name=challenger_name,
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=sum(challenger_returns),
            trades=len(challenger_returns),
            wins=len(challenger_returns),
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            realized_trade_pnls=challenger_returns,
        )

        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO champion_assignments(
                  scope, symbol, horizon_s, model_name, challenger_name, regime, state, assigned_ts_ms, updated_ts_ms, meta_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "global",
                    "AAPL",
                    300,
                    "current_champion",
                    "",
                    "global",
                    "champion",
                    now_ms - 5_000,
                    now_ms - 5_000,
                    json.dumps({"last_promotion_ts_ms": 0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        return {
            "current_champion|AAPL|300|global": {
                "approved": True,
                "model_name": "current_champion",
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            },
            f"{challenger_name}|AAPL|300|global": {
                "approved": True,
                "model_name": str(challenger_name),
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            },
        }

    def test_make_cpcv_splits_generates_all_fold_combinations(self) -> None:
        splits = self.cpcv.make_cpcv_splits(n_samples=12, n_splits=4, n_test_splits=2)

        self.assertEqual(len(splits), 6)
        for train_idx, test_idx in splits:
            self.assertEqual(len(train_idx), 6)
            self.assertEqual(len(test_idx), 6)
            self.assertEqual(len(set(train_idx).intersection(set(test_idx))), 0)

    def test_purge_train_indices_removes_pre_test_overlap_window(self) -> None:
        purged = self.cpcv.purge_train_indices(
            train_idx=[0, 1, 2, 3, 4, 7, 8, 9],
            test_idx=[5, 6],
            label_horizon=2,
        )

        self.assertEqual(list(purged), [0, 1, 2, 7, 8, 9])

    def test_embargo_train_indices_removes_post_test_window(self) -> None:
        embargoed = self.cpcv.embargo_train_indices(
            train_idx=[0, 1, 2, 5, 6, 7, 8, 9],
            test_idx=[3, 4],
            embargo_pct=0.2,
        )

        self.assertEqual(list(embargoed), [0, 1, 2, 7, 8, 9])

    def test_compute_pbo_single_series_proxy_is_sane(self) -> None:
        result = self.cpcv.compute_pbo(
            in_sample_scores=[1.0, 0.8, 0.6],
            out_of_sample_scores=[-0.1, 0.2, -0.3],
        )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "single_series_proxy")
        self.assertAlmostEqual(float(result.get("pbo") or 0.0), 2.0 / 3.0, places=6)

    def test_promotion_is_blocked_when_cpcv_pbo_exceeds_threshold(self) -> None:
        os.environ["CPCV_ENABLED"] = "1"
        os.environ["CPCV_MAX_PBO"] = "0.5"
        os.environ["CPCV_MIN_PATH_SHARPE"] = "0.5"
        self._reload_runtime_modules()

        replay_models = self._seed_competition_pair("challenger_pbo")
        self.storage.record_backtest_cpcv_run(
            model_name="challenger_pbo",
            candidate_version="challenger_pbo",
            n_splits=6,
            n_test_splits=2,
            embargo_pct=0.01,
            path_returns=[[0.1, 0.2], [0.15, 0.05]],
            path_sharpes=[1.2, 1.0],
            mean_sharpe=1.1,
            median_sharpe=1.1,
            pbo=0.9,
            diagnostics={"status": "evaluated"},
        )

        with patch.object(
            self.champion_manager,
            "get_cached_replay_validation_snapshot",
            return_value={"fresh": True, "snapshot": {"models": replay_models}},
        ), patch.object(
            self.champion_manager,
            "run_self_critic",
            return_value={"blocked_keys": []},
        ), patch.object(
            self.champion_manager,
            "compute_capital_plan",
            return_value={},
        ), patch.object(
            self.champion_manager,
            "_sync_assignment_to_model_registry",
            return_value=None,
        ), patch.object(
            self.champion_manager,
            "_sync_registry_runtime",
            return_value=None,
        ), patch.object(
            self.champion_manager,
            "audit",
            return_value=None,
        ):
            result = self.champion_manager.evaluate_competition_cycle()

        assignment = self.champion_manager.get_champion_assignment("global", "AAPL", 300)
        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "current_champion")

        gate_result = self.promotion_guard.evaluate_statistical_promotion_gate(
            model_name="challenger_pbo",
            candidate_version="challenger_pbo",
            returns=[1.0, 1.5, 1.25, 1.75, 1.1, 1.2],
            n_competing_trials=1,
            persist=False,
        )
        self.assertFalse(bool(gate_result[0]))
        self.assertEqual(str((gate_result[1] or {}).get("cpcv", {}).get("status") or ""), "pbo_above_threshold")

    def test_disabled_cpcv_path_remains_backward_compatible(self) -> None:
        os.environ["CPCV_ENABLED"] = "0"
        self._reload_runtime_modules()

        replay_models = self._seed_competition_pair("challenger_disabled")

        with patch.object(
            self.champion_manager,
            "get_cached_replay_validation_snapshot",
            return_value={"fresh": True, "snapshot": {"models": replay_models}},
        ), patch.object(
            self.champion_manager,
            "run_self_critic",
            return_value={"blocked_keys": []},
        ), patch.object(
            self.champion_manager,
            "compute_capital_plan",
            return_value={},
        ), patch.object(
            self.champion_manager,
            "_sync_assignment_to_model_registry",
            return_value=None,
        ), patch.object(
            self.champion_manager,
            "_sync_registry_runtime",
            return_value=None,
        ), patch.object(
            self.champion_manager,
            "audit",
            return_value=None,
        ):
            result = self.champion_manager.evaluate_competition_cycle()

        assignment = self.champion_manager.get_champion_assignment("global", "AAPL", 300)
        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "challenger_disabled")

    def test_missing_cpcv_run_is_materialized_on_demand(self) -> None:
        os.environ["CPCV_ENABLED"] = "1"
        os.environ["CPCV_N_SPLITS"] = "6"
        os.environ["CPCV_N_TEST_SPLITS"] = "2"
        os.environ["CPCV_EMBARGO_PCT"] = "0.01"
        os.environ["CPCV_LABEL_HORIZON"] = "3"
        os.environ["CPCV_MAX_PBO"] = "0.5"
        os.environ["CPCV_MIN_PATH_SHARPE"] = "0.5"
        self._reload_runtime_modules()

        calls: list[dict] = []

        def _fake_run_backtest_cpcv_job(**kwargs):
            calls.append(dict(kwargs))
            run_id = self.storage.record_backtest_cpcv_run(
                model_name=str(kwargs["model_name"]),
                candidate_version=str(kwargs["candidate_version"]),
                n_splits=int(kwargs["n_splits"]),
                n_test_splits=int(kwargs["n_test_splits"]),
                embargo_pct=float(kwargs["embargo_pct"]),
                path_returns=[[0.10, 0.20], [0.15, 0.05]],
                path_sharpes=[1.20, 1.00],
                mean_sharpe=1.10,
                median_sharpe=1.10,
                pbo=0.10,
                diagnostics={"status": "evaluated", "source": "auto"},
            )
            return {
                "ok": True,
                "status": "evaluated",
                "run_id": int(run_id),
                "model_name": str(kwargs["model_name"]),
                "candidate_version": str(kwargs["candidate_version"]),
                "n_paths": 2,
                "mean_sharpe": 1.10,
                "median_sharpe": 1.10,
                "pbo": 0.10,
                "diagnostics": {"source": "auto"},
            }

        with patch.object(self.cpcv, "run_backtest_cpcv_job", side_effect=_fake_run_backtest_cpcv_job):
            passed, diagnostics = self.promotion_guard.evaluate_cpcv_promotion_gate(
                model_name="challenger_auto",
                candidate_version="challenger_auto.v1",
            )

        self.assertTrue(passed)
        self.assertEqual(len(calls), 1)
        self.assertEqual(int(calls[0]["n_splits"]), 6)
        self.assertEqual(int(calls[0]["n_test_splits"]), 2)
        self.assertAlmostEqual(float(calls[0]["embargo_pct"]), 0.01, places=9)
        self.assertEqual(int(calls[0]["label_horizon"]), 3)
        self.assertEqual(str(diagnostics.get("status") or ""), "evaluated")
        self.assertTrue(bool(dict(diagnostics.get("auto_run") or {}).get("ok")))
        self.assertEqual(int(dict(diagnostics.get("latest_run") or {}).get("n_paths") or 0), 2)
        self.assertAlmostEqual(float(dict(diagnostics.get("latest_run") or {}).get("pbo") or 0.0), 0.10, places=6)


if __name__ == "__main__":
    unittest.main()
