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

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class CompetitionPostCommitOutboxTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "competition_post_commit.db")
        os.environ["CHAMPION_PROMOTION_COOLDOWN_S"] = "0"
        os.environ["CHAMPION_PROMOTION_MIN_TRADES"] = "3"
        os.environ["CHAMPION_PROMOTION_MIN_OBSERVATION_S"] = "1"
        os.environ["CHAMPION_PROMOTION_MIN_SCORE"] = "0.0"
        os.environ["CHAMPION_PROMOTION_MIN_NET_PNL_DELTA"] = "0"
        os.environ["COMPETITION_POST_COMMIT_RETRY_MAX_MS"] = "0"
        self._reload_runtime_modules()

    def tearDown(self) -> None:
        for key in (
            "DB_PATH",
            "CHAMPION_PROMOTION_COOLDOWN_S",
            "CHAMPION_PROMOTION_MIN_TRADES",
            "CHAMPION_PROMOTION_MIN_OBSERVATION_S",
            "CHAMPION_PROMOTION_MIN_SCORE",
            "CHAMPION_PROMOTION_MIN_NET_PNL_DELTA",
            "COMPETITION_POST_COMMIT_RETRY_MAX_MS",
        ):
            os.environ.pop(key, None)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def _reload_runtime_modules(self) -> None:
        self.storage, _runtime_meta, self.champion_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.strategy.champion_manager",
        )
        self.storage.init_db()

    def test_post_commit_status_read_path_does_not_run_schema_ddl(self) -> None:
        class _Cursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return list(self._rows)

        class _Connection:
            def __init__(self) -> None:
                self.statements: list[str] = []
                self.closed = False

            def execute(self, sql: str, params=()):
                self.statements.append(str(sql))
                if "GROUP BY status" in str(sql):
                    return _Cursor([("completed", 2), ("failed", 1)])
                return _Cursor([(10, "audit", 3, "failed", 100, 0, "boom")])

            def close(self) -> None:
                self.closed = True

        con = _Connection()

        with patch.object(self.champion_manager, "connect", return_value=con), patch.object(
            self.champion_manager,
            "init_db",
            side_effect=AssertionError("status read path must not run schema initialization"),
        ):
            status = self.champion_manager.get_competition_post_commit_status()

        self.assertTrue(con.closed)
        self.assertEqual(status["completed_count"], 2)
        self.assertEqual(status["failed_count"], 1)
        self.assertEqual(status["failed_actions"][0]["action_name"], "audit")
        self.assertFalse(
            any(
                token in statement.upper()
                for statement in con.statements
                for token in ("CREATE TABLE", "CREATE INDEX", "ALTER TABLE")
            )
        )

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
            "net_cost_label_count": int(max(1, trades)),
            "net_cost_evidence_available": True,
            "net_cost_evidence": {
                "available": True,
                "n": int(max(1, trades)),
                "avg_net_return": float(net_pnl) / float(max(1, trades)),
                "avg_gross_return": (float(net_pnl) / float(max(1, trades))) + 0.001,
                "avg_execution_cost_return": 0.001,
                "avg_total_cost_bps": 10.0,
            },
        }
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
                    0.5,
                    int(last_signal_ts_ms),
                    int(last_signal_ts_ms),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

    def test_failed_post_commit_actions_are_durable_and_recoverable(self) -> None:
        now_ms = int(time.time() * 1000)
        first_ts = now_ms - 10_000
        self._insert_marketplace_row(
            model_id="current_champ",
            model_name="current_champ",
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
        )
        self._insert_marketplace_row(
            model_id="challenger",
            model_name="challenger",
            symbol="AAPL",
            horizon_s=300,
            score=0.30,
            net_pnl=40.0,
            trades=5,
            wins=3,
            losses=2,
            first_signal_ts_ms=first_ts,
            last_signal_ts_ms=now_ms,
            stage="challenger",
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
                    "current_champ",
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
            "current_champ|AAPL|300|global": {"approved": True, "model_name": "current_champ", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
            "challenger|AAPL|300|global": {"approved": True, "model_name": "challenger", "symbol": "AAPL", "horizon_s": 300, "regime": "global"},
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
            side_effect=RuntimeError("registry_runtime_unavailable"),
        ), patch.object(
            self.champion_manager,
            "audit",
            return_value=None,
        ):
            result = self.champion_manager.evaluate_competition_cycle()

        assignment = self.champion_manager.get_champion_assignment("global", "AAPL", 300)
        snapshot = dict(result.get("snapshot") or {})
        snapshot_aapl = next(
            (
                row
                for row in list(snapshot.get("champions") or [])
                if str((row or {}).get("symbol") or "") == "AAPL"
                and int((row or {}).get("horizon_s") or 0) == 300
            ),
            {},
        )
        post_commit_status = self.champion_manager.get_competition_post_commit_status()

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "post_commit_degraded")
        self.assertEqual(str(assignment.get("model_name") or ""), "challenger")
        self.assertEqual(str(snapshot_aapl.get("model_name") or ""), "challenger")
        self.assertTrue(bool(post_commit_status.get("degraded")))
        self.assertGreater(int(post_commit_status.get("failed_count") or 0), 0)

        with patch.object(
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
            replay = self.champion_manager.drain_competition_post_commit_actions(max_actions=64)

        final_status = self.champion_manager.get_competition_post_commit_status()
        self.assertGreater(int(replay.get("completed_now") or 0), 0)
        self.assertFalse(bool(final_status.get("degraded")))
        self.assertEqual(int(final_status.get("failed_count") or 0), 0)
        self.assertEqual(int(final_status.get("pending_count") or 0), 0)


if __name__ == "__main__":
    unittest.main()
