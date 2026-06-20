from __future__ import annotations

import importlib
import json
import math
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

from tests.promotion_test_helpers import passing_deconfounded_payload

pytestmark = pytest.mark.requires_postgres


def _postgres_backend_enabled() -> bool:
    backend = str(os.environ.get("TS_STORAGE_BACKEND") or "").strip().lower()
    return backend in {"postgres", "postgresql", "timescale"} and bool(os.environ.get("TS_PG_DSN"))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class ModelCompetitionRealPnlTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "competition_real_pnl_test.db")
        os.environ["CHAMPION_PROMOTION_COOLDOWN_S"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_TRADES"] = "3"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATION_S"] = "1"
        os.environ["CHAMPION_PROMOTION_MIN_SCORE"] = "0.9"
        os.environ["CHAMPION_PROMOTION_MIN_NET_PNL_DELTA"] = "0"
        self._reload_runtime_modules()

    def tearDown(self) -> None:
        for key in (
            "CHAMPION_PROMOTION_COOLDOWN_S",
            "CHAMPION_PROMOTION_MIN_TRADES",
            "CHAMPION_PROMOTION_MIN_OBSERVATION_S",
            "CHAMPION_PROMOTION_MIN_SCORE",
            "CHAMPION_PROMOTION_MIN_NET_PNL_DELTA",
            "CHAMPION_PROMOTION_USE_STAT_GATE",
            "CHAMPION_PROMOTION_MIN_T_STAT",
            "CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE",
            "CHAMPION_PROMOTION_MIN_OBSERVATIONS",
            "CHAMPION_PROMOTION_FDR_ALPHA",
            "CPCV_ENABLED",
            "ENGINE_MODE",
            "ENV",
            "ENGINE_SUPERVISED",
            "SPA_TEST_ENABLED",
            "SPA_MIN_MODELS",
            "SPA_BOOTSTRAP_SAMPLES",
            "SPA_TEST_SEED",
            "SPA_ALPHA",
        ):
            os.environ.pop(key, None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(
                f"[test_model_competition_real_pnl] close_pooled_connections_failed: {type(e).__name__}: {e}\n"
            )
        self.tmp.cleanup()

    def _reload_runtime_modules(self) -> None:
        self.storage, self.champion_manager, self.promotion_guard, self.model_marketplace = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.champion_manager",
            "engine.strategy.promotion_guard",
            "engine.strategy.model_marketplace",
        )[1:]
        self.storage.init_db()

    @staticmethod
    def _returns_with_t_stat(target_t: float, n_obs: int = 50) -> list[float]:
        if n_obs < 2 or (n_obs % 2) != 0:
            raise ValueError("n_obs_must_be_even_and_ge_2")
        mean = float(target_t) / math.sqrt(float(n_obs - 1))
        half = int(n_obs // 2)
        return ([float(mean + 1.0)] * half) + ([float(mean - 1.0)] * half)

    @staticmethod
    def _flat_oos_returns(value: float, n_obs: int = 60) -> list[float]:
        return [float(value)] * int(n_obs)

    @staticmethod
    def _stat_gate_row(model_name: str, returns: list[float]) -> dict:
        return {
            "model_id": str(model_name),
            "model_name": str(model_name),
            "score": 0.99,
            "trades": len(returns),
            "wins": sum(1 for value in returns if value > 0.0),
            "losses": sum(1 for value in returns if value < 0.0),
            "net_pnl": float(sum(returns)),
            "meta": {
                "score_source": "pnl_attribution",
                "realized_trade_pnls": [float(value) for value in returns],
                "rolling_total_pnl": float(sum(returns)),
                "net_cost_label_count": len(returns),
                "net_cost_evidence_available": True,
                "net_cost_evidence": {"available": True, "n": len(returns)},
            },
        }

    def _latest_statistical_evidence(self, model_id: str, test_name: str = "white_reality_check") -> dict:
        con = self.storage.connect()
        try:
            row = con.execute(
                """
                SELECT id, ts, model_id, feature_id, test_name, t_stat, p_value, q_value,
                       bootstrap_samples, decision, payload_json
                FROM promotion_statistical_evidence
                WHERE model_id=? AND test_name=?
                ORDER BY ts DESC, id DESC
                LIMIT 1
                """,
                (str(model_id), str(test_name)),
            ).fetchone()
        finally:
            con.close()
        if not row:
            return {}
        evidence = dict(row)
        try:
            evidence["payload"] = json.loads(str(evidence.get("payload_json") or "{}"))
        except Exception:
            evidence["payload"] = {}
        return evidence

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
        max_drawdown: float = 0.0,
        avg_confidence: float = 0.5,
        stage: str = "challenger",
        score_source: str = "pnl_attribution",
        model_kind: str = "test_model",
        realized_trade_pnls: list[float] | None = None,
        notional_traded: float | None = None,
        net_cost_evidence: bool = True,
    ) -> None:
        meta = {
            "score_source": str(score_source),
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
            "max_drawdown": float(max_drawdown),
            "model_kind": str(model_kind),
            "model_ts_ms": int(first_signal_ts_ms),
        }
        if net_cost_evidence:
            meta["net_cost_label_count"] = int(max(1, trades))
            meta["net_cost_evidence_available"] = True
            meta["net_cost_evidence"] = {
                "available": True,
                "n": int(max(1, trades)),
                "avg_net_return": float(net_pnl) / float(max(1, trades)),
                "avg_gross_return": (float(net_pnl) / float(max(1, trades))) + 0.001,
                "avg_execution_cost_return": 0.001,
                "avg_total_cost_bps": 10.0,
            }
        if realized_trade_pnls is not None:
            meta["realized_trade_pnls"] = [float(value) for value in list(realized_trade_pnls)]
            deconfounded_n_obs = len(meta["realized_trade_pnls"])
        else:
            deconfounded_n_obs = int(max(8, trades))
        meta["deconfounded_validation"] = passing_deconfounded_payload(deconfounded_n_obs)
        if notional_traded is not None:
            meta["notional_traded"] = float(notional_traded)
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
                    model_id,
                    model_name,
                    symbol,
                    int(horizon_s),
                    "global",
                    stage,
                    float(score),
                    int(trades),
                    int(wins),
                    int(losses),
                    float(net_pnl),
                    float(net_pnl),
                    float(avg_confidence),
                    int(last_signal_ts_ms),
                    int(last_signal_ts_ms),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

    def _insert_champion_assignment(
        self,
        *,
        model_name: str,
        symbol: str = "AAPL",
        horizon_s: int = 300,
        assigned_ts_ms: int | None = None,
    ) -> None:
        now_ms = int(assigned_ts_ms or int(time.time() * 1000))
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
                    str(symbol).upper().strip(),
                    int(horizon_s),
                    str(model_name),
                    "",
                    "global",
                    "champion",
                    int(now_ms),
                    int(now_ms),
                    json.dumps({"last_promotion_ts_ms": 0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

    def test_rank_models_prefers_higher_real_pnl_over_higher_score(self) -> None:
        ranked = self.champion_manager._rank_models(
            [
                {
                    "model_name": "score_favored",
                    "score": 0.95,
                    "net_pnl": 20.0,
                    "event_pnls": [20.0],
                },
                {
                    "model_name": "pnl_favored",
                    "score": 0.10,
                    "net_pnl": 50.0,
                    "event_pnls": [50.0],
                },
            ]
        )

        self.assertEqual(str(ranked[0]["model_name"]), "pnl_favored")
        self.assertEqual(int(ranked[0]["rank"]), 1)

    def test_rank_models_prefers_higher_capital_adjusted_return_over_higher_raw_pnl(self) -> None:
        ranked = self.champion_manager._rank_models(
            [
                {
                    "model_name": "nominal_big",
                    "score": 0.95,
                    "net_pnl": 100.0,
                    "capital_base_sum": 10_000.0,
                    "event_pnls": [100.0],
                },
                {
                    "model_name": "efficient_small",
                    "score": 0.10,
                    "net_pnl": 50.0,
                    "capital_base_sum": 500.0,
                    "event_pnls": [50.0],
                },
            ]
        )

        self.assertEqual(str(ranked[0]["model_name"]), "efficient_small")
        self.assertAlmostEqual(float(ranked[0]["return_pct"]), 10.0, places=6)
        self.assertAlmostEqual(float(ranked[1]["return_pct"]), 1.0, places=6)

    def test_recompute_model_rankings_uses_notional_traded_as_capital_base(self) -> None:
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        self._insert_marketplace_row(
            model_id="nominal_big",
            model_name="nominal_big",
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=100.0,
            trades=5,
            wins=4,
            losses=1,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            notional_traded=10_000.0,
        )
        self._insert_marketplace_row(
            model_id="efficient_small",
            model_name="efficient_small",
            symbol="AAPL",
            horizon_s=300,
            score=0.10,
            net_pnl=50.0,
            trades=5,
            wins=3,
            losses=2,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            notional_traded=500.0,
        )

        rankings = self.champion_manager.recompute_model_rankings()
        rows = list(rankings.get("rows") or [])

        self.assertEqual(str(rows[0]["model_name"]), "efficient_small")
        self.assertAlmostEqual(float(rows[0]["return_pct"]), 10.0, places=6)
        self.assertAlmostEqual(float(rows[1]["return_pct"]), 1.0, places=6)
        self.assertAlmostEqual(float(rows[0]["capital_base_sum"]), 500.0, places=6)

    def test_safe_numeric_helpers_ignore_missing_values_without_warning(self) -> None:
        with patch.object(self.champion_manager, "_warn_nonfatal") as warn_nonfatal:
            self.assertEqual(self.champion_manager._safe_float(None, 1.25), 1.25)
            self.assertEqual(self.champion_manager._safe_float("   ", 2.5), 2.5)
            self.assertEqual(self.champion_manager._safe_int(None, 7), 7)
            self.assertEqual(self.champion_manager._safe_int("", 9), 9)
        warn_nonfatal.assert_not_called()

    def test_safe_numeric_helpers_still_warn_on_invalid_non_missing_values(self) -> None:
        with patch.object(self.champion_manager, "_warn_nonfatal") as warn_nonfatal:
            self.assertEqual(self.champion_manager._safe_float(object(), 1.5), 1.5)
            self.assertEqual(self.champion_manager._safe_int(object(), 3), 3)
        self.assertEqual(warn_nonfatal.call_count, 2)

    def test_candidate_eligibility_ignores_score_floor_when_real_pnl_is_positive(self) -> None:
        now_ms = int(time.time() * 1000)
        eligible = self.champion_manager._candidate_is_eligible(
            {
                "score": -0.75,
                "trades": 4,
                "wins": 3,
                "losses": 1,
                "net_pnl": 40.0,
                "meta": {
                    "rolling_total_pnl": 40.0,
                    "risk_adjusted_score": -0.75,
                    "first_signal_ts_ms": now_ms - 10_000,
                    "last_signal_ts_ms": now_ms,
                    "observation_duration_ms": 10_000,
                    "net_cost_label_count": 4,
                    "net_cost_evidence": {"available": True, "n": 4},
                },
            }
        )

        self.assertTrue(eligible)

    def test_candidate_eligibility_blocks_missing_net_cost_evidence(self) -> None:
        now_ms = int(time.time() * 1000)
        eligible = self.champion_manager._candidate_is_eligible(
            {
                "score": 0.95,
                "trades": 4,
                "wins": 3,
                "losses": 1,
                "net_pnl": 40.0,
                "meta": {
                    "rolling_total_pnl": 40.0,
                    "first_signal_ts_ms": now_ms - 10_000,
                    "last_signal_ts_ms": now_ms,
                    "observation_duration_ms": 10_000,
                },
            }
        )

        self.assertFalse(eligible)

    def test_promotion_stat_gate_blocks_insufficient_observations_in_live_mode(self) -> None:
        os.environ["ENGINE_MODE"] = "live"
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
        os.environ["CPCV_ENABLED"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "5"
        self._reload_runtime_modules()

        row = self._stat_gate_row("live_low_obs", [0.2, 0.2, 0.2, 0.2])
        champion = self._stat_gate_row("live_current", [0.0, 0.0, 0.0, 0.0])

        with patch.object(self.champion_manager, "assess_challenger") as assess_challenger:
            passed, diagnostics = self.champion_manager._evaluate_promotion_stat_gate(
                row,
                n_competing_trials=1,
                champion_row=champion,
            )

        assess_challenger.assert_not_called()
        self.assertFalse(bool(passed))
        self.assertFalse(bool(diagnostics.get("passed")))
        self.assertEqual(str(diagnostics.get("status") or ""), "insufficient_observations")
        self.assertEqual(str(diagnostics.get("promotion_mode") or ""), "live")
        self.assertTrue(bool(diagnostics.get("fail_closed")))
        self.assertEqual(int(diagnostics.get("current_observations") or 0), 4)
        self.assertEqual(int(diagnostics.get("required_observations") or 0), 5)
        self.assertIn("insufficient_observations", list(diagnostics.get("blockers") or []))

    def test_promotion_stat_gate_blocks_insufficient_observations_in_paper_mode(self) -> None:
        os.environ["ENGINE_MODE"] = "paper"
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
        os.environ["CPCV_ENABLED"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "5"
        self._reload_runtime_modules()

        row = self._stat_gate_row("paper_low_obs", [0.2, 0.2, 0.2, 0.2])

        passed, diagnostics = self.champion_manager._evaluate_promotion_stat_gate(row, n_competing_trials=1)

        self.assertFalse(bool(passed))
        self.assertFalse(bool(diagnostics.get("passed")))
        self.assertEqual(str(diagnostics.get("status") or ""), "insufficient_observations")
        self.assertEqual(str(diagnostics.get("promotion_mode") or ""), "paper")
        self.assertEqual(int(diagnostics.get("challenger_observations") or 0), 4)
        self.assertEqual(int(diagnostics.get("champion_observations", -1)), 0)
        self.assertEqual(int(diagnostics.get("current_observations") or 0), 4)
        self.assertEqual(int(diagnostics.get("min_observations") or 0), 5)

    def test_safe_and_shadow_modes_keep_explicit_low_observation_advisory(self) -> None:
        for mode_name in ("safe", "shadow"):
            with self.subTest(mode_name=mode_name):
                os.environ["ENGINE_MODE"] = mode_name
                os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
                os.environ["CPCV_ENABLED"] = "0"
                os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "5"
                self._reload_runtime_modules()

                row = self._stat_gate_row(f"{mode_name}_low_obs", [0.2, 0.2, 0.2, 0.2])
                passed, diagnostics = self.champion_manager._evaluate_promotion_stat_gate(row, n_competing_trials=1)

                self.assertTrue(bool(passed))
                self.assertTrue(bool(diagnostics.get("passed")))
                self.assertEqual(str(diagnostics.get("status") or ""), "insufficient_observations_advisory")
                self.assertEqual(str(diagnostics.get("promotion_mode") or ""), mode_name)
                self.assertFalse(bool(diagnostics.get("fail_closed")))
                self.assertTrue(bool(diagnostics.get("advisory")))

    def test_competition_cycle_blocks_low_observation_bootstrap_without_incumbent(self) -> None:
        if not _postgres_backend_enabled():
            self.skipTest("postgres backend required for assignment-path promotion schema")
        os.environ["ENGINE_MODE"] = "paper"
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
        os.environ["CPCV_ENABLED"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "5"
        self._reload_runtime_modules()

        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        returns = [0.3, 0.3, 0.3, 0.3]
        self._insert_marketplace_row(
            model_id="low_obs_bootstrap",
            model_name="low_obs_bootstrap",
            symbol="AAPL",
            horizon_s=300,
            score=0.99,
            net_pnl=sum(returns),
            trades=len(returns),
            wins=len(returns),
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            realized_trade_pnls=returns,
        )
        replay_models = {
            "low_obs_bootstrap|AAPL|300|global": {
                "approved": True,
                "model_name": "low_obs_bootstrap",
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            }
        }

        with patch.object(self.champion_manager, "promotion_allowed", return_value=(True, {"allowed": True})), patch.object(
            self.champion_manager,
            "_evaluate_candidate_graph_gate",
            return_value=(True, {"applied": False, "passed": True}),
        ), patch.object(
            self.champion_manager,
            "_evaluate_candidate_ope_gate",
            return_value=(True, {"applied": False, "passed": True}),
        ), patch.object(
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
        changes = list(result.get("changes") or [])
        blocked = next((change for change in changes if str((change or {}).get("symbol") or "") == "AAPL"), {})
        stat_gate = dict(blocked.get("stat_gate") or {})

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(assignment, {})
        self.assertEqual(str(blocked.get("reason") or ""), "bootstrap_best_stat_gate_blocked")
        self.assertEqual(str(blocked.get("to_model_name") or ""), "")
        self.assertEqual(str(stat_gate.get("status") or ""), "insufficient_observations")
        self.assertEqual(int(stat_gate.get("current_observations") or 0), 4)
        self.assertEqual(int(stat_gate.get("required_observations") or 0), 5)

    def test_competition_cycle_blocks_low_observation_challenger_with_incumbent(self) -> None:
        if not _postgres_backend_enabled():
            self.skipTest("postgres backend required for assignment-path promotion schema")
        os.environ["ENGINE_MODE"] = "paper"
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
        os.environ["CPCV_ENABLED"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "5"
        self._reload_runtime_modules()

        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        current_returns = [0.0, 0.0, 0.0, 0.0]
        challenger_returns = [0.3, 0.3, 0.3, 0.3]
        self._insert_marketplace_row(
            model_id="low_obs_current",
            model_name="low_obs_current",
            symbol="AAPL",
            horizon_s=300,
            score=0.50,
            net_pnl=0.1,
            trades=len(current_returns),
            wins=1,
            losses=3,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="champion",
            realized_trade_pnls=current_returns,
        )
        self._insert_marketplace_row(
            model_id="low_obs_challenger",
            model_name="low_obs_challenger",
            symbol="AAPL",
            horizon_s=300,
            score=0.99,
            net_pnl=sum(challenger_returns),
            trades=len(challenger_returns),
            wins=len(challenger_returns),
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            realized_trade_pnls=challenger_returns,
        )
        self._insert_champion_assignment(
            model_name="low_obs_current",
            assigned_ts_ms=now_ms - 5_000,
        )
        replay_models = {
            "low_obs_current|AAPL|300|global": {"approved": True, "model_name": "low_obs_current", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "low_obs_challenger|AAPL|300|global": {"approved": True, "model_name": "low_obs_challenger", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
        }

        with patch.object(self.champion_manager, "promotion_allowed", return_value=(True, {"allowed": True})), patch.object(
            self.champion_manager,
            "_evaluate_candidate_graph_gate",
            return_value=(True, {"applied": False, "passed": True}),
        ), patch.object(
            self.champion_manager,
            "_evaluate_candidate_ope_gate",
            return_value=(True, {"applied": False, "passed": True}),
        ), patch.object(
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
        blocked = next(
            (
                change
                for change in list(result.get("changes") or [])
                if str((change or {}).get("symbol") or "") == "AAPL"
                and str((change or {}).get("reason") or "").endswith("_stat_gate_blocked")
            ),
            {},
        )
        stat_gate = dict(blocked.get("stat_gate") or {})

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "low_obs_current")
        self.assertEqual(str(blocked.get("from_model_name") or ""), "low_obs_current")
        self.assertEqual(str(blocked.get("to_model_name") or ""), "low_obs_current")
        self.assertEqual(str(stat_gate.get("status") or ""), "insufficient_observations")
        self.assertEqual(int(stat_gate.get("current_observations") or 0), 4)
        self.assertEqual(int(stat_gate.get("required_observations") or 0), 5)

    def test_competition_cycle_promotes_higher_real_pnl_challenger_even_with_lower_score(self) -> None:
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        self._insert_marketplace_row(
            model_id="score_king",
            model_name="score_king",
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=25.0,
            trades=5,
            wins=4,
            losses=1,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="champion",
            realized_trade_pnls=self._flat_oos_returns(0.0),
        )
        self._insert_marketplace_row(
            model_id="pnl_winner",
            model_name="pnl_winner",
            symbol="AAPL",
            horizon_s=300,
            score=0.10,
            net_pnl=80.0,
            trades=5,
            wins=3,
            losses=2,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            realized_trade_pnls=self._flat_oos_returns(0.2),
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
                    "score_king",
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

        replay_models = {
            "score_king|AAPL|300|global": {"approved": True, "model_name": "score_king", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "pnl_winner|AAPL|300|global": {"approved": True, "model_name": "pnl_winner", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
        }

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
        self.assertEqual(str(assignment.get("model_name") or ""), "pnl_winner")
        self.assertEqual(str((result.get("changes") or [{}])[0].get("reason") or ""), "challenger_outperformance")

    def test_competition_cycle_promotes_higher_return_challenger_when_capital_base_known(self) -> None:
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        self._insert_marketplace_row(
            model_id="capital_heavy_champ",
            model_name="capital_heavy_champ",
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=80.0,
            trades=5,
            wins=4,
            losses=1,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="champion",
            notional_traded=10_000.0,
            realized_trade_pnls=self._flat_oos_returns(0.0),
        )
        self._insert_marketplace_row(
            model_id="capital_efficient_challenger",
            model_name="capital_efficient_challenger",
            symbol="AAPL",
            horizon_s=300,
            score=0.10,
            net_pnl=40.0,
            trades=5,
            wins=3,
            losses=2,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            notional_traded=200.0,
            realized_trade_pnls=self._flat_oos_returns(0.2),
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
                    "capital_heavy_champ",
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

        replay_models = {
            "capital_heavy_champ|AAPL|300|global": {
                "approved": True,
                "model_name": "capital_heavy_champ",
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            },
            "capital_efficient_challenger|AAPL|300|global": {
                "approved": True,
                "model_name": "capital_efficient_challenger",
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            },
        }

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
        changes = list(result.get("changes") or [])
        local_change = next((row for row in changes if str((row or {}).get("symbol") or "") == "AAPL"), {})

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "capital_efficient_challenger")
        self.assertEqual(str(local_change.get("reason") or ""), "challenger_outperformance")
        self.assertEqual(str(local_change.get("comparison_metric") or ""), "return_pct")
        self.assertGreater(float(local_change.get("challenger_return_delta") or 0.0), 0.0)
        self.assertLess(float(local_change.get("challenger_pnl_delta") or 0.0), 0.0)

    def test_competition_cycle_blocks_challenger_when_stat_gate_enabled_and_t_below_threshold(self) -> None:
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "1"
        os.environ["CHAMPION_PROMOTION_MIN_T_STAT"] = "3.0"
        os.environ["CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE"] = "0.0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "50"
        os.environ["CHAMPION_PROMOTION_FDR_ALPHA"] = "0.05"
        self._reload_runtime_modules()

        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        challenger_returns = self._returns_with_t_stat(2.5, n_obs=50)
        self._insert_marketplace_row(
            model_id="current_champ_t25",
            model_name="current_champ_t25",
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
            realized_trade_pnls=self._flat_oos_returns(0.0, n_obs=50),
        )
        self._insert_marketplace_row(
            model_id="challenger_t25",
            model_name="challenger_t25",
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=sum(challenger_returns),
            trades=len(challenger_returns),
            wins=sum(1 for value in challenger_returns if value > 0.0),
            losses=sum(1 for value in challenger_returns if value < 0.0),
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
                    "current_champ_t25",
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

        replay_models = {
            "current_champ_t25|AAPL|300|global": {"approved": True, "model_name": "current_champ_t25", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "challenger_t25|AAPL|300|global": {"approved": True, "model_name": "challenger_t25", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
        }

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
        evidence = self._latest_statistical_evidence("challenger_t25")

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "current_champ_t25")
        self.assertFalse(
            any(str((change or {}).get("to_model_name") or "") == "challenger_t25" for change in list(result.get("changes") or []))
        )
        self.assertEqual(str(evidence.get("test_name") or ""), "white_reality_check")
        self.assertEqual(str(evidence.get("decision") or ""), "fail")
        self.assertGreaterEqual(float(evidence.get("p_value") or 0.0), 0.05)

    def test_competition_cycle_allows_challenger_when_statistical_evidence_passes(self) -> None:
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "1"
        os.environ["CHAMPION_PROMOTION_MIN_T_STAT"] = "3.0"
        os.environ["CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE"] = "0.0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "50"
        os.environ["CHAMPION_PROMOTION_FDR_ALPHA"] = "0.05"
        self._reload_runtime_modules()

        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        challenger_returns = self._flat_oos_returns(0.4, n_obs=50)
        self._insert_marketplace_row(
            model_id="current_champ_t35",
            model_name="current_champ_t35",
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
            realized_trade_pnls=self._flat_oos_returns(0.0, n_obs=50),
        )
        self._insert_marketplace_row(
            model_id="challenger_t35",
            model_name="challenger_t35",
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=sum(challenger_returns),
            trades=len(challenger_returns),
            wins=sum(1 for value in challenger_returns if value > 0.0),
            losses=sum(1 for value in challenger_returns if value < 0.0),
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
                    "current_champ_t35",
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

        replay_models = {
            "current_champ_t35|AAPL|300|global": {"approved": True, "model_name": "current_champ_t35", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "challenger_t35|AAPL|300|global": {"approved": True, "model_name": "challenger_t35", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
        }

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
        evidence = self._latest_statistical_evidence("challenger_t35")

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "challenger_t35")
        self.assertEqual(str(evidence.get("test_name") or ""), "white_reality_check")
        self.assertEqual(str(evidence.get("decision") or ""), "pass")
        self.assertLess(float(evidence.get("p_value") or 1.0), 0.05)
        self.assertEqual(int(evidence.get("bootstrap_samples") or 0), 10_000)

    def test_competition_cycle_records_reality_check_evidence_when_multiple_candidates_exist(self) -> None:
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "1"
        os.environ["CHAMPION_PROMOTION_MIN_T_STAT"] = "3.0"
        os.environ["CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE"] = "0.0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "50"
        os.environ["CHAMPION_PROMOTION_FDR_ALPHA"] = "0.05"
        os.environ["SPA_TEST_ENABLED"] = "1"
        os.environ["SPA_MIN_MODELS"] = "3"
        os.environ["SPA_BOOTSTRAP_SAMPLES"] = "256"
        os.environ["SPA_TEST_SEED"] = "41"
        self._reload_runtime_modules()

        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        challenger_returns = self._flat_oos_returns(0.2, n_obs=50)
        peer_returns = self._flat_oos_returns(0.03, n_obs=50)
        laggard_returns = self._flat_oos_returns(0.0, n_obs=50)
        self._insert_marketplace_row(
            model_id="current_champ_spa",
            model_name="current_champ_spa",
            symbol="AAPL",
            horizon_s=300,
            score=0.60,
            net_pnl=8.0,
            trades=5,
            wins=4,
            losses=1,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="champion",
            realized_trade_pnls=laggard_returns,
        )
        self._insert_marketplace_row(
            model_id="challenger_spa",
            model_name="challenger_spa",
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=sum(challenger_returns),
            trades=len(challenger_returns),
            wins=sum(1 for value in challenger_returns if value > 0.0),
            losses=sum(1 for value in challenger_returns if value < 0.0),
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            realized_trade_pnls=challenger_returns,
        )
        self._insert_marketplace_row(
            model_id="peer_spa",
            model_name="peer_spa",
            symbol="AAPL",
            horizon_s=300,
            score=0.85,
            net_pnl=sum(peer_returns),
            trades=len(peer_returns),
            wins=sum(1 for value in peer_returns if value > 0.0),
            losses=sum(1 for value in peer_returns if value < 0.0),
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            realized_trade_pnls=peer_returns,
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
                    "current_champ_spa",
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

        replay_models = {
            "current_champ_spa|AAPL|300|global": {"approved": True, "model_name": "current_champ_spa", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "challenger_spa|AAPL|300|global": {"approved": True, "model_name": "challenger_spa", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "peer_spa|AAPL|300|global": {"approved": True, "model_name": "peer_spa", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
        }

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
        evidence = self._latest_statistical_evidence("challenger_spa")
        payload = dict(evidence.get("payload") or {})

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "challenger_spa")
        self.assertEqual(str(evidence.get("test_name") or ""), "white_reality_check")
        self.assertEqual(str(evidence.get("decision") or ""), "pass")
        self.assertLess(float(evidence.get("p_value") or 1.0), 0.05)
        self.assertTrue(list(payload.get("bootstrap_distribution") or []))

    def test_competition_cycle_ignores_disabled_legacy_stat_gate_when_reality_check_fails(self) -> None:
        os.environ["CHAMPION_PROMOTION_USE_STAT_GATE"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_T_STAT"] = "3.0"
        os.environ["CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE"] = "0.0"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATIONS"] = "50"
        os.environ["CHAMPION_PROMOTION_FDR_ALPHA"] = "0.05"
        self._reload_runtime_modules()

        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        challenger_returns = self._returns_with_t_stat(2.5, n_obs=50)
        self._insert_marketplace_row(
            model_id="current_champ_disabled",
            model_name="current_champ_disabled",
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
            realized_trade_pnls=self._flat_oos_returns(0.0, n_obs=50),
        )
        self._insert_marketplace_row(
            model_id="challenger_disabled",
            model_name="challenger_disabled",
            symbol="AAPL",
            horizon_s=300,
            score=0.95,
            net_pnl=sum(challenger_returns),
            trades=len(challenger_returns),
            wins=sum(1 for value in challenger_returns if value > 0.0),
            losses=sum(1 for value in challenger_returns if value < 0.0),
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
                    "current_champ_disabled",
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

        replay_models = {
            "current_champ_disabled|AAPL|300|global": {"approved": True, "model_name": "current_champ_disabled", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "challenger_disabled|AAPL|300|global": {"approved": True, "model_name": "challenger_disabled", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
        }

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
        evidence = self._latest_statistical_evidence("challenger_disabled")
        hypotheses = self.storage.fetch_recent_hypothesis_registry(limit=5, model_name="challenger_disabled")

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(assignment.get("model_name") or ""), "current_champ_disabled")
        self.assertEqual(str(evidence.get("test_name") or ""), "white_reality_check")
        self.assertEqual(str(evidence.get("decision") or ""), "fail")
        self.assertFalse(hypotheses)

    def test_shadow_challenger_is_visible_but_not_promoted_live(self) -> None:
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        self._insert_marketplace_row(
            model_id="live_champ",
            model_name="live_champ",
            symbol="AAPL",
            horizon_s=300,
            score=0.60,
            net_pnl=25.0,
            trades=5,
            wins=4,
            losses=1,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="champion",
            score_source="pnl_attribution",
            model_kind="test_model",
        )
        self._insert_marketplace_row(
            model_id="shadow_regime_stats_v2",
            model_name="regime_stats_shadow_v2",
            symbol="AAPL",
            horizon_s=300,
            score=0.99,
            net_pnl=80.0,
            trades=5,
            wins=5,
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
            score_source="shadow_predictions",
            model_kind="shadow_regime_stats",
        )

        challengers = self.model_marketplace.top_challengers(limit=10)
        shadow_row = next(
            (row for row in challengers if str(row.get("model_name") or "") == "regime_stats_shadow_v2"),
            {},
        )
        self.assertEqual(str(shadow_row.get("model_name") or ""), "regime_stats_shadow_v2")
        self.assertEqual(str(dict(shadow_row.get("meta") or {}).get("score_source") or ""), "shadow_predictions")

        rankings = self.champion_manager.recompute_model_rankings()
        ranked_names = [str((row or {}).get("model_name") or "") for row in list(rankings.get("rows") or [])]
        self.assertIn("live_champ", ranked_names)
        self.assertNotIn("regime_stats_shadow_v2", ranked_names)

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
                    "live_champ",
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

        replay_models = {
            "live_champ|AAPL|300|global": {"approved": True, "model_name": "live_champ", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "regime_stats_shadow_v2|AAPL|300|global": {"approved": True, "model_name": "regime_stats_shadow_v2", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
        }

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
        self.assertEqual(str(assignment.get("model_name") or ""), "live_champ")
        self.assertFalse(
            any(
                str((change or {}).get("to_model_name") or "") == "regime_stats_shadow_v2"
                for change in list(result.get("changes") or [])
            )
        )

    def test_global_model_competition_blocks_best_when_self_critic_blocks_context(self) -> None:
        if not _postgres_backend_enabled():
            self.skipTest("postgres backend required for assignment-path promotion schema")
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        self._insert_marketplace_row(
            model_id="global_blocked",
            model_name="global_blocked",
            symbol="AAPL",
            horizon_s=300,
            score=0.99,
            net_pnl=80.0,
            trades=60,
            wins=60,
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            realized_trade_pnls=self._flat_oos_returns(0.2),
        )
        replay_models = {
            "global_blocked|baseline|AAPL|300|global": {
                "approved": True,
                "model_name": "global_blocked",
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            }
        }

        with patch.object(self.champion_manager, "promotion_allowed", return_value=(True, {"allowed": True})), patch.object(
            self.champion_manager,
            "get_cached_replay_validation_snapshot",
            return_value={"fresh": True, "snapshot": {"models": replay_models}},
        ), patch.object(
            self.champion_manager,
            "run_self_critic",
            return_value={"blocked_keys": ["global_blocked|baseline|AAPL|300|global"]},
        ), patch.object(
            self.champion_manager,
            "compute_capital_plan",
            return_value={},
        ), patch.object(
            self.champion_manager,
            "_sync_registry_runtime",
            return_value=None,
        ):
            result = self.champion_manager.evaluate_competition_cycle()

        assignment = self.champion_manager.get_champion_assignment(
            self.champion_manager.MODEL_COMPETITION_SCOPE,
            self.champion_manager.MODEL_COMPETITION_SYMBOL,
            self.champion_manager.MODEL_COMPETITION_HORIZON_S,
        )
        global_change = next(
            (row for row in list(result.get("changes") or []) if str((row or {}).get("scope") or "") == self.champion_manager.MODEL_COMPETITION_SCOPE),
            {},
        )

        self.assertEqual(assignment, {})
        self.assertEqual(str(global_change.get("reason") or ""), "best_blocked_self_critic")
        self.assertIn("self_critic_blocked", list(dict(global_change.get("best_promotion_eligibility") or {}).get("block_reasons") or []))

    def test_global_model_competition_blocks_best_without_replay_approval(self) -> None:
        if not _postgres_backend_enabled():
            self.skipTest("postgres backend required for assignment-path promotion schema")
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        self._insert_marketplace_row(
            model_id="global_missing_replay",
            model_name="global_missing_replay",
            symbol="AAPL",
            horizon_s=300,
            score=0.99,
            net_pnl=80.0,
            trades=60,
            wins=60,
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            realized_trade_pnls=self._flat_oos_returns(0.2),
        )

        with patch.object(self.champion_manager, "promotion_allowed", return_value=(True, {"allowed": True})), patch.object(
            self.champion_manager,
            "get_cached_replay_validation_snapshot",
            return_value={"fresh": True, "snapshot": {"models": {}}},
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
            "_sync_registry_runtime",
            return_value=None,
        ):
            result = self.champion_manager.evaluate_competition_cycle()

        assignment = self.champion_manager.get_champion_assignment(
            self.champion_manager.MODEL_COMPETITION_SCOPE,
            self.champion_manager.MODEL_COMPETITION_SYMBOL,
            self.champion_manager.MODEL_COMPETITION_HORIZON_S,
        )
        global_change = next(
            (row for row in list(result.get("changes") or []) if str((row or {}).get("scope") or "") == self.champion_manager.MODEL_COMPETITION_SCOPE),
            {},
        )

        self.assertEqual(assignment, {})
        self.assertEqual(str(global_change.get("reason") or ""), "replay_gate_blocked")
        self.assertIn("replay_missing", list(dict(global_change.get("best_promotion_eligibility") or {}).get("block_reasons") or []))

    def test_global_model_competition_blocks_best_when_replay_snapshot_is_stale(self) -> None:
        if not _postgres_backend_enabled():
            self.skipTest("postgres backend required for assignment-path promotion schema")
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        self._insert_marketplace_row(
            model_id="global_stale_replay",
            model_name="global_stale_replay",
            symbol="AAPL",
            horizon_s=300,
            score=0.99,
            net_pnl=80.0,
            trades=60,
            wins=60,
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            realized_trade_pnls=self._flat_oos_returns(0.2),
        )
        replay_models = {
            "global_stale_replay|baseline|AAPL|300|global": {
                "approved": True,
                "model_name": "global_stale_replay",
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            }
        }

        with patch.object(
            self.champion_manager,
            "get_cached_replay_validation_snapshot",
            return_value={"fresh": False, "snapshot": {"models": replay_models}},
        ), patch.object(
            self.champion_manager,
            "run_self_critic",
            return_value={"blocked_keys": []},
        ), patch.object(
            self.champion_manager,
            "compute_capital_plan",
            return_value={},
        ):
            result = self.champion_manager.evaluate_competition_cycle()

        assignment = self.champion_manager.get_champion_assignment(
            self.champion_manager.MODEL_COMPETITION_SCOPE,
            self.champion_manager.MODEL_COMPETITION_SYMBOL,
            self.champion_manager.MODEL_COMPETITION_HORIZON_S,
        )
        global_change = next(
            (row for row in list(result.get("changes") or []) if str((row or {}).get("scope") or "") == self.champion_manager.MODEL_COMPETITION_SCOPE),
            {},
        )

        self.assertEqual(str(result.get("status") or ""), "replay_stale")
        self.assertEqual(assignment, {})
        self.assertEqual(str(global_change.get("reason") or ""), "replay_stale")
        self.assertIn("replay_stale", list(dict(global_change.get("best_promotion_eligibility") or {}).get("block_reasons") or []))

    def test_global_model_competition_assigns_valid_replay_approved_candidate(self) -> None:
        if not _postgres_backend_enabled():
            self.skipTest("postgres backend required for assignment-path promotion schema")
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 60_000
        self._insert_marketplace_row(
            model_id="global_replay_valid",
            model_name="global_replay_valid",
            symbol="AAPL",
            horizon_s=300,
            score=0.99,
            net_pnl=80.0,
            trades=60,
            wins=60,
            losses=0,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            realized_trade_pnls=self._flat_oos_returns(0.2),
        )
        replay_models = {
            "global_replay_valid|baseline|AAPL|300|global": {
                "approved": True,
                "model_name": "global_replay_valid",
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": "global",
            }
        }

        with patch.object(self.champion_manager, "promotion_allowed", return_value=(True, {"allowed": True})), patch.object(
            self.champion_manager,
            "_evaluate_candidate_graph_gate",
            return_value=(True, {"applied": False, "passed": True}),
        ), patch.object(
            self.champion_manager,
            "_evaluate_candidate_ope_gate",
            return_value=(True, {"applied": False, "passed": True}),
        ), patch.object(
            self.champion_manager,
            "_evaluate_promotion_stat_gate",
            return_value=(True, {"enabled": True, "validation_enabled": True, "passed": True, "status": "passed"}),
        ), patch.object(
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

        assignment = self.champion_manager.get_champion_assignment(
            self.champion_manager.MODEL_COMPETITION_SCOPE,
            self.champion_manager.MODEL_COMPETITION_SYMBOL,
            self.champion_manager.MODEL_COMPETITION_HORIZON_S,
        )
        global_change = next(
            (row for row in list(result.get("changes") or []) if str((row or {}).get("scope") or "") == self.champion_manager.MODEL_COMPETITION_SCOPE),
            {},
        )

        self.assertEqual(str(assignment.get("model_name") or ""), "global_replay_valid")
        self.assertEqual(str(global_change.get("reason") or ""), "bootstrap_best")
        self.assertTrue(bool(dict(global_change.get("promotion_eligibility") or {}).get("eligible")))
        self.assertTrue(bool(dict(assignment.get("meta") or {}).get("promotion_eligibility", {}).get("eligible")))

    def test_compute_capital_plan_globally_scales_group_budgets(self) -> None:
        prev_total_cap = os.environ.get("COMPETITION_TOTAL_CAPITAL_FRACTION")
        os.environ["COMPETITION_TOTAL_CAPITAL_FRACTION"] = "0.50"
        try:
            self.model_marketplace, self.champion_manager = _reload_modules(
                "engine.strategy.model_marketplace",
                "engine.strategy.champion_manager",
            )
            now_ms = int(time.time() * 1000)
            first_ts = now_ms - 10_000
            self._insert_marketplace_row(
                model_id="alpha_aapl_v1",
                model_name="alpha_aapl_v1",
                symbol="AAPL",
                horizon_s=300,
                score=0.95,
                net_pnl=90.0,
                trades=12,
                wins=9,
                losses=3,
                first_signal_ts_ms=first_ts,
                last_signal_ts_ms=now_ms,
            )
            self._insert_marketplace_row(
                model_id="alpha_msft_v1",
                model_name="alpha_msft_v1",
                symbol="MSFT",
                horizon_s=300,
                score=0.92,
                net_pnl=85.0,
                trades=11,
                wins=8,
                losses=3,
                first_signal_ts_ms=first_ts,
                last_signal_ts_ms=now_ms,
            )

            plan = self.model_marketplace.compute_capital_plan()
        finally:
            if prev_total_cap is None:
                os.environ.pop("COMPETITION_TOTAL_CAPITAL_FRACTION", None)
            else:
                os.environ["COMPETITION_TOTAL_CAPITAL_FRACTION"] = str(prev_total_cap)
            self.model_marketplace, self.champion_manager = _reload_modules(
                "engine.strategy.model_marketplace",
                "engine.strategy.champion_manager",
            )

        allocations = dict(plan.get("allocations") or {})
        self.assertEqual(len(allocations), 2)
        self.assertGreater(float(plan.get("total_group_budget_fraction_pre") or 0.0), 0.50)
        self.assertAlmostEqual(float(plan.get("competition_total_capital_fraction") or 0.0), 0.50, places=6)
        self.assertAlmostEqual(float(plan.get("total_group_budget_fraction_post") or 0.0), 0.50, places=6)
        self.assertLess(float(plan.get("global_budget_scale") or 0.0), 1.0)
        for alloc in allocations.values():
            group_budget_fraction = float((alloc or {}).get("group_budget_fraction") or 0.0)
            unscaled_group_budget_fraction = float((alloc or {}).get("group_budget_fraction_unscaled") or 0.0)
            models = list((alloc or {}).get("models") or [])
            self.assertLess(group_budget_fraction, unscaled_group_budget_fraction)
            self.assertTrue(models)
            self.assertAlmostEqual(
                float((models[0] or {}).get("effective_allocation_fraction") or 0.0),
                group_budget_fraction,
                places=6,
            )

    def test_compute_capital_plan_uses_model_confidence_in_allocations(self) -> None:
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        self._insert_marketplace_row(
            model_id="high_conf_v1",
            model_name="high_conf_v1",
            symbol="AAPL",
            horizon_s=300,
            score=0.80,
            net_pnl=60.0,
            trades=8,
            wins=5,
            losses=3,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            max_drawdown=20.0,
            avg_confidence=0.90,
        )
        self._insert_marketplace_row(
            model_id="low_conf_v1",
            model_name="low_conf_v1",
            symbol="AAPL",
            horizon_s=300,
            score=0.80,
            net_pnl=60.0,
            trades=8,
            wins=5,
            losses=3,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            max_drawdown=20.0,
            avg_confidence=0.30,
        )

        plan = self.model_marketplace.compute_capital_plan()
        group = dict((plan.get("allocations") or {}).get("AAPL|300|global") or {})
        models = {
            str((row or {}).get("model_name") or ""): dict(row or {})
            for row in list(group.get("models") or [])
        }

        self.assertIn("high_conf_v1", models)
        self.assertIn("low_conf_v1", models)
        self.assertGreater(
            float((models["high_conf_v1"] or {}).get("allocation_fraction") or 0.0),
            float((models["low_conf_v1"] or {}).get("allocation_fraction") or 0.0),
        )

    def test_competition_policy_keeps_group_budget_unscaled_by_risk_multiplier(self) -> None:
        now_ms = int(time.time() * 1000)
        self.champion_manager.meta_set(
            "competition_capital_plan",
            json.dumps(
                {
                    "updated_ts_ms": int(now_ms),
                    "allocation_strategy": "proportional",
                    "allocations": {
                        "AAPL|300|global": {
                            "symbol": "AAPL",
                            "horizon_s": 300,
                            "regime": "global",
                            "champion_model_name": "champ_aapl_v1",
                            "allocation_strategy": "proportional",
                            "group_budget_fraction": 0.40,
                            "risk_limit_multiplier": 0.80,
                            "models": [
                                {
                                    "model_name": "champ_aapl_v1",
                                    "allocation_fraction": 0.75,
                                    "effective_allocation_fraction": 0.30,
                                    "model_risk_limit_multiplier": 0.50,
                                },
                                {
                                    "model_name": "challenger_aapl_v1",
                                    "allocation_fraction": 0.25,
                                    "effective_allocation_fraction": 0.10,
                                    "model_risk_limit_multiplier": 0.90,
                                },
                            ],
                        }
                    },
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )

        policy = self.champion_manager.get_competition_policy_for_intent(
            symbol="AAPL",
            horizon_s=300,
            model_name="champ_aapl_v1",
            regime="global",
        )

        self.assertTrue(bool(policy.get("capital_plan_fresh")))
        self.assertAlmostEqual(float(policy.get("group_budget_fraction") or 0.0), 0.40, places=6)
        self.assertAlmostEqual(float(policy.get("model_budget_fraction") or 0.0), 0.30, places=6)
        self.assertAlmostEqual(float(policy.get("risk_limit_multiplier") or 0.0), 0.50, places=6)
        self.assertAlmostEqual(float(policy.get("group_risk_limit_multiplier") or 0.0), 0.80, places=6)

    def test_promotion_guard_blocks_negative_real_pnl_models(self) -> None:
        now_ms = int(time.time() * 1000)
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO trade_attribution_ledger(
                  ts_ms, source_alert_id, model_id, symbol, signal_json, model_json,
                  regime_vector_json, execution_policy_json, suppression_reason, pnl, fees,
                  slippage_bps, decision_json, created_ts_ms
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    now_ms,
                    None,
                    "loser_v1",
                    "AAPL",
                    json.dumps(
                        {
                            "pnl_attribution": {
                                "realized_pnl": -20.0,
                                "unrealized_pnl": 0.0,
                                "total_pnl": -25.0,
                                "extra": {"slippage_cost": 0.0},
                            }
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    json.dumps({"model_name": "loser_v1"}, separators=(",", ":"), sort_keys=True),
                    None,
                    None,
                    None,
                    999.0,
                    0.0,
                    0.0,
                    None,
                    now_ms,
                ),
            )
            con.commit()
        finally:
            con.close()

        allowed, reason = self.promotion_guard.promotion_allowed()

        self.assertFalse(allowed)
        self.assertIn("negative_real_pnl_models", list(reason.get("blockers") or []))
        self.assertIn("loser_v1", list(reason.get("negative_models") or []))


if __name__ == "__main__":
    unittest.main()
