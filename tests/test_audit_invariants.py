"""Regression tests for audit and allocation invariants across recent changes."""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
import sys
from types import SimpleNamespace
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


class AuditInvariantTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "audit_test.db"
        os.environ["DB_PATH"] = str(self.db_path)

        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
        )

    def tearDown(self) -> None:
        try:
            metrics_store = importlib.import_module("engine.runtime.metrics_store")
            metrics_store.shutdown_runtime_metrics_buffer(timeout_s=1.0)
        except Exception as e:
            sys.stderr.write(f"[test_audit_invariants] shutdown_runtime_metrics_buffer_failed: {type(e).__name__}: {e}\n")
        try:
            event_log = importlib.import_module("engine.runtime.event_log")
            event_log.shutdown_event_log_buffer(timeout_s=1.0)
        except Exception as e:
            sys.stderr.write(f"[test_audit_invariants] shutdown_event_log_buffer_failed: {type(e).__name__}: {e}\n")
        try:
            telemetry_append_buffer = importlib.import_module("engine.runtime.telemetry_append_buffer")
            telemetry_append_buffer.shutdown_telemetry_append_buffers(timeout_s=1.0)
        except Exception as e:
            sys.stderr.write(
                f"[test_audit_invariants] shutdown_telemetry_append_buffers_failed: {type(e).__name__}: {e}\n"
            )
        try:
            storage = importlib.import_module("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(f"[test_audit_invariants] close_pooled_connections_failed: {type(e).__name__}: {e}\n")
        self.tmp.cleanup()

    def _executescript(self, script: str) -> None:
        con = sqlite3.connect(str(self.db_path))
        try:
            con.executescript(script)
            con.commit()
        finally:
            con.close()

    def test_rebalance_keeps_same_symbol_models_isolated(self) -> None:
        storage, portfolio, strategy_allocator, capital_guard = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.runtime.strategy_allocator",
            "engine.strategy.capital_guard",
        )
        self._executescript(portfolio.SCHEMA)

        def _target(model_id: str, model_name: str, weight: float) -> dict:
            explain = json.dumps(
                {
                    "model_id": model_id,
                    "model_name": model_name,
                    "confidence": 0.9,
                    "model_intent": {"score": 1.0},
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            return {
                "symbol": "AAPL",
                "side": "LONG",
                "weight": float(weight),
                "source_alert_id": 101,
                "reason": {"confidence": 0.9, "score": 1.0, "expected_z": 1.2},
                "confidence": 0.9,
                "explain_json": explain,
            }

        modules = {
            "s1": SimpleNamespace(build_desired=lambda alerts, now_ms: {"AAPL": _target("m1", "model_one", 0.20)}),
            "s2": SimpleNamespace(build_desired=lambda alerts, now_ms: {"AAPL": _target("m2", "model_two", 0.30)}),
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(capital_guard, "trading_allowed", return_value=True))
            stack.enter_context(
                patch.object(
                    strategy_allocator,
                    "compute_and_persist_strategy_allocations",
                    return_value={
                        "allocations": {"s1": 1.0, "s2": 1.0},
                        "details": {},
                        "regime": {},
                        "regime_confidence": 0.0,
                        "reason": {},
                        "alpha_decay_runtime": {},
                        "portfolio_target_gross": 1.0,
                    },
                )
            )
            stack.enter_context(patch.object(portfolio, "_load_recent_alert_candidates", return_value=[]))
            stack.enter_context(patch.object(portfolio, "_load_live_strategies", return_value=["s1", "s2"]))
            stack.enter_context(patch.object(portfolio, "_load_shadow_strategies", return_value=[]))
            stack.enter_context(patch.object(portfolio, "load_strategy_module", side_effect=lambda name: modules[str(name)]))
            stack.enter_context(patch.object(portfolio, "_optimize_capital_allocation", side_effect=lambda con, desired: desired))
            stack.enter_context(patch.object(portfolio, "_apply_impact_aware_sizing", side_effect=lambda con, desired: desired))
            stack.enter_context(
                patch.object(portfolio, "_apply_model_diversification_scoring", side_effect=lambda con, desired: (desired, {}))
            )
            stack.enter_context(
                patch.object(portfolio, "apply_portfolio_risk_engine", side_effect=lambda con, desired, state, now_ms: (desired, {}))
            )
            stack.enter_context(
                patch.object(portfolio, "apply_portfolio_risk_gate", side_effect=lambda con, desired, state, now_ms: (desired, {}))
            )
            stack.enter_context(patch.object(portfolio, "_apply_temporal_dampener", side_effect=lambda con, desired, now_ms: desired))
            stack.enter_context(patch.object(portfolio, "_apply_capital_at_risk_gate", side_effect=lambda desired: (desired, {})))
            stack.enter_context(
                patch.object(portfolio, "_apply_same_direction_exposure_netting", side_effect=lambda con, desired: (desired, {}))
            )
            stack.enter_context(
                patch.object(portfolio, "_apply_total_portfolio_risk_limit", side_effect=lambda con, desired: (desired, {}))
            )
            stack.enter_context(patch.object(portfolio, "_build_portfolio_correlation_diagnostics", return_value={}))
            stack.enter_context(patch.object(portfolio, "_persist_portfolio_correlation_diagnostics", return_value=None))
            stack.enter_context(patch.object(portfolio, "request_monte_carlo_refresh", return_value=None))
            stack.enter_context(patch.object(portfolio, "is_blacklisted", return_value=False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_ALLOC_OPT", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_CORR_OPT", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_VOL_TARGET", False, create=True))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_STRESS_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_SOCIAL_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_VOV_GATE", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_EXEC_REALISM", False))
            stack.enter_context(patch.object(portfolio, "PORTFOLIO_USE_EXEC_REGIME", False))

            result = portfolio.compute_rebalance()

        self.assertTrue(result.get("ok"), result)

        con = storage.connect(readonly=True)
        try:
            rows = con.execute(
                """
                SELECT model_id, symbol, action
                FROM portfolio_orders
                ORDER BY model_id ASC, symbol ASC, ts_ms ASC
                """
            ).fetchall()
        finally:
            con.close()

        opens = [(str(row[0]), str(row[1]), str(row[2])) for row in rows if str(row[2]) == "OPEN"]
        self.assertEqual(opens, [("m1", "AAPL", "OPEN"), ("m2", "AAPL", "OPEN")])

    def test_portfolio_alert_lifecycle_helpers_mark_seen_consumed_and_expired(self) -> None:
        storage, portfolio = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
        )
        storage.init_db()
        now_ms = int(time.time() * 1000)

        con = storage.connect()
        try:
            recent_seen = con.execute(
                """
                INSERT INTO alerts(
                  ts_ms, event_title, symbol, horizon_s, expected_z, confidence, severity,
                  rule_id, explain_json, dedupe_key
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms - 1_000),
                    "recent-seen",
                    "AAPL",
                    300,
                    1.0,
                    0.8,
                    "medium",
                    "unit_test",
                    "{}",
                    "recent-seen",
                ),
            ).lastrowid
            recent_consumed = con.execute(
                """
                INSERT INTO alerts(
                  ts_ms, event_title, symbol, horizon_s, expected_z, confidence, severity,
                  rule_id, explain_json, dedupe_key
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms - 2_000),
                    "recent-consumed",
                    "MSFT",
                    300,
                    1.0,
                    0.8,
                    "medium",
                    "unit_test",
                    "{}",
                    "recent-consumed",
                ),
            ).lastrowid
            stale_alert = con.execute(
                """
                INSERT INTO alerts(
                  ts_ms, event_title, symbol, horizon_s, expected_z, confidence, severity,
                  rule_id, explain_json, dedupe_key
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms - ((int(portfolio.PORTFOLIO_LOOKBACK_S) + 10) * 1000)),
                    "stale-alert",
                    "NVDA",
                    300,
                    1.0,
                    0.8,
                    "medium",
                    "unit_test",
                    "{}",
                    "stale-alert",
                ),
            ).lastrowid

            portfolio._mark_alert_candidates_seen(con, [recent_seen, recent_consumed], int(now_ms))
            portfolio._mark_alerts_consumed(con, [recent_consumed], int(now_ms))
            expired = portfolio._expire_stale_unconsumed_alerts(con, int(now_ms), int(portfolio.PORTFOLIO_LOOKBACK_S))
            con.commit()
        finally:
            con.close()

        self.assertEqual(int(expired), 1)

        con = storage.connect(readonly=True)
        try:
            rows = con.execute(
                """
                SELECT id, portfolio_first_seen_ts_ms, portfolio_last_seen_ts_ms,
                       portfolio_consumed_ts_ms, portfolio_expired_ts_ms, portfolio_status
                FROM alerts
                ORDER BY id ASC
                """
            ).fetchall()
        finally:
            con.close()

        status_by_id = {
            int(row[0]): {
                "first_seen": int(row[1] or 0),
                "last_seen": int(row[2] or 0),
                "consumed": int(row[3] or 0),
                "expired": int(row[4] or 0),
                "status": str(row[5] or ""),
            }
            for row in rows
        }
        self.assertEqual(str(status_by_id[int(recent_seen)]["status"]), "seen")
        self.assertGreater(int(status_by_id[int(recent_seen)]["first_seen"]), 0)
        self.assertGreater(int(status_by_id[int(recent_seen)]["last_seen"]), 0)
        self.assertEqual(int(status_by_id[int(recent_seen)]["consumed"]), 0)
        self.assertEqual(str(status_by_id[int(recent_consumed)]["status"]), "consumed")
        self.assertGreater(int(status_by_id[int(recent_consumed)]["consumed"]), 0)
        self.assertEqual(str(status_by_id[int(stale_alert)]["status"]), "expired")
        self.assertGreater(int(status_by_id[int(stale_alert)]["expired"]), 0)

    def test_recompute_marketplace_scores_persists_shadow_predictions(self) -> None:
        storage, model_marketplace = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.model_marketplace",
        )
        self._executescript(
            """
            CREATE TABLE IF NOT EXISTS prices (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              price REAL,
              px REAL,
              source TEXT,
              PRIMARY KEY(symbol, ts_ms)
            );
            CREATE TABLE IF NOT EXISTS labels_exec (
              event_id INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL,
              ts_ms INTEGER NOT NULL,
              source TEXT NOT NULL DEFAULT 'heuristic',
              realized INTEGER NOT NULL DEFAULT 0,
              side INTEGER NOT NULL,
              gross_ret REAL NOT NULL,
              net_ret REAL NOT NULL,
              gross_z REAL,
              net_z REAL,
              mid_in REAL,
              mid_out REAL,
              spread_in REAL,
              fees_bps REAL NOT NULL,
              slippage_bps REAL NOT NULL,
              spread_bps REAL NOT NULL,
              total_cost_bps REAL NOT NULL,
              extra_json TEXT,
              PRIMARY KEY (event_id, symbol, horizon_s)
            );
            CREATE TABLE IF NOT EXISTS shadow_predictions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              event_id INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              regime TEXT,
              horizon_s INTEGER NOT NULL,
              model_name TEXT NOT NULL,
              model_kind TEXT,
              model_ts_ms INTEGER,
              predicted_z REAL NOT NULL,
              confidence REAL NOT NULL,
              cost_est REAL,
              net_pred_z REAL,
              extra_json TEXT
            );
            CREATE TABLE IF NOT EXISTS champion_assignments (
              scope TEXT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              model_name TEXT NOT NULL,
              challenger_name TEXT,
              regime TEXT NOT NULL DEFAULT 'global',
              state TEXT NOT NULL DEFAULT 'champion',
              assigned_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              meta_json TEXT,
              PRIMARY KEY (scope, symbol, horizon_s)
            );
            CREATE TABLE IF NOT EXISTS model_marketplace_scores (
              model_id TEXT NOT NULL DEFAULT 'baseline',
              model_name TEXT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              regime TEXT NOT NULL DEFAULT 'global',
              stage TEXT NOT NULL DEFAULT 'challenger',
              score REAL NOT NULL DEFAULT 0,
              trades INTEGER NOT NULL DEFAULT 0,
              wins INTEGER NOT NULL DEFAULT 0,
              losses INTEGER NOT NULL DEFAULT 0,
              gross_pnl REAL NOT NULL DEFAULT 0,
              net_pnl REAL NOT NULL DEFAULT 0,
              avg_confidence REAL NOT NULL DEFAULT 0,
              last_signal_ts_ms INTEGER,
              updated_ts_ms INTEGER NOT NULL,
              meta_json TEXT,
              PRIMARY KEY (model_id, model_name, symbol, horizon_s, regime)
            );
            """
        )

        now_ms = int(time.time() * 1000)
        horizon_s = 300

        con = storage.connect()
        try:
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (int(now_ms), "AAPL", 100.0, 100.0, "test"),
            )
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (int(now_ms + horizon_s * 1000), "AAPL", 101.5, 101.5, "test"),
            )
            con.execute(
                """
                INSERT INTO shadow_predictions(
                  ts_ms, event_id, symbol, regime, horizon_s,
                  model_name, model_kind, model_ts_ms,
                  predicted_z, confidence, cost_est, net_pred_z, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    1,
                    "AAPL",
                    "global",
                    int(horizon_s),
                    "regime_stats_v2",
                    "shadow_regime_stats",
                    int(now_ms),
                    1.0,
                    0.8,
                    2.0,
                    1.0,
                    json.dumps({"model_id": "shadow_m1", "meta": {"model_id": "shadow_m1"}}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        result = model_marketplace.recompute_marketplace_scores()
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(int(result.get("shadow_predictions_scored") or 0), 1)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT model_id, model_name, symbol, trades, net_pnl, meta_json
                FROM model_marketplace_scores
                WHERE model_id='shadow_m1' AND symbol='AAPL'
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        meta = json.loads(row[5] or "{}")
        self.assertEqual(str(row[0]), "shadow_m1")
        self.assertEqual(str(row[1]), "regime_stats_v2")
        self.assertEqual(str(row[2]), "AAPL")
        self.assertEqual(int(row[3] or 0), 1)
        self.assertAlmostEqual(float(row[4] or 0.0), 11.84, places=6)
        self.assertEqual(str(meta.get("score_source") or ""), "shadow_predictions")
        self.assertEqual(int(meta.get("shadow_predictions_scored") or 0), 1)

    def test_recompute_marketplace_scores_uses_real_rolling_risk_adjusted_score(self) -> None:
        with patch.dict(os.environ, {"MODEL_COMPETITION_WINDOW_S": "86400"}, clear=False):
            storage, model_marketplace = _reload_modules(
                "engine.runtime.storage",
                "engine.strategy.model_marketplace",
            )

        self._executescript(
            """
            CREATE TABLE IF NOT EXISTS alerts (
              id INTEGER PRIMARY KEY,
              ts_ms INTEGER,
              event_title TEXT,
              symbol TEXT,
              horizon_s INTEGER,
              expected_z REAL,
              confidence REAL,
              severity TEXT,
              rule_id TEXT,
              explain_json TEXT
            );
            CREATE TABLE IF NOT EXISTS execution_orders (
              client_order_id TEXT PRIMARY KEY,
              broker TEXT,
              portfolio_orders_id INTEGER,
              source_alert_id INTEGER,
              model_id TEXT,
              model_version TEXT,
              symbol TEXT,
              qty REAL,
              submit_ts_ms INTEGER,
              ref_px REAL,
              expected_px REAL,
              mid_px REAL,
              bid_px REAL,
              ask_px REAL,
              spread_bps REAL,
              broker_order_id TEXT,
              status TEXT,
              extra_json TEXT
            );
            CREATE TABLE IF NOT EXISTS pnl_attribution (
              ts_ms INTEGER NOT NULL,
              source_alert_id INTEGER NOT NULL,
              model_id TEXT NOT NULL DEFAULT 'baseline',
              model_version TEXT,
              symbol TEXT NOT NULL,
              pnl REAL NOT NULL,
              fees REAL NOT NULL,
              slippage_bps REAL,
              position_size REAL,
              avg_price REAL,
              realized_pnl REAL,
              unrealized_pnl REAL,
              extra_json TEXT,
              PRIMARY KEY (ts_ms, source_alert_id, model_id, symbol)
            );
            CREATE TABLE IF NOT EXISTS champion_assignments (
              scope TEXT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              model_name TEXT NOT NULL,
              challenger_name TEXT,
              regime TEXT NOT NULL DEFAULT 'global',
              state TEXT NOT NULL DEFAULT 'champion',
              assigned_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              meta_json TEXT,
              PRIMARY KEY (scope, symbol, horizon_s)
            );
            CREATE TABLE IF NOT EXISTS model_marketplace_scores (
              model_id TEXT NOT NULL DEFAULT 'baseline',
              model_name TEXT NOT NULL,
              symbol TEXT NOT NULL,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              regime TEXT NOT NULL DEFAULT 'global',
              stage TEXT NOT NULL DEFAULT 'challenger',
              score REAL NOT NULL DEFAULT 0,
              trades INTEGER NOT NULL DEFAULT 0,
              wins INTEGER NOT NULL DEFAULT 0,
              losses INTEGER NOT NULL DEFAULT 0,
              gross_pnl REAL NOT NULL DEFAULT 0,
              net_pnl REAL NOT NULL DEFAULT 0,
              avg_confidence REAL NOT NULL DEFAULT 0,
              last_signal_ts_ms INTEGER,
              updated_ts_ms INTEGER NOT NULL,
              meta_json TEXT,
              PRIMARY KEY (model_id, model_name, symbol, horizon_s, regime)
            );
            """
        )

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            rows = [
                ("a1", 1, "m1", "model_a", "AAPL", 300, 0.85, 40.0),
                ("a2", 2, "m1", "model_a", "AAPL", 300, 0.80, -20.0),
                ("b1", 3, "m2", "model_b", "AAPL", 300, 0.82, 10.0),
                ("b2", 4, "m2", "model_b", "AAPL", 300, 0.81, 10.0),
            ]
            for idx, (order_id, alert_id, model_id, model_name, symbol, horizon_s, confidence, pnl_value) in enumerate(rows):
                submit_ts_ms = int(now_ms - ((len(rows) - idx) * 1000))
                explain_json = json.dumps(
                    {"model_name": model_name, "model_id": model_id},
                    separators=(",", ":"),
                    sort_keys=True,
                )
                order_extra = json.dumps(
                    {
                        "model_name": model_name,
                        "model_id": model_id,
                        "horizon_s": horizon_s,
                        "confidence": confidence,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
                con.execute(
                    """
                    INSERT INTO alerts(
                      id, ts_ms, event_title, symbol, horizon_s, expected_z, confidence,
                      severity, rule_id, explain_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(alert_id),
                        int(submit_ts_ms),
                        "test",
                        symbol,
                        int(horizon_s),
                        0.0,
                        float(confidence),
                        "info",
                        "unit",
                        explain_json,
                    ),
                )
                con.execute(
                    """
                    INSERT INTO execution_orders(
                      client_order_id, broker, portfolio_orders_id, source_alert_id, model_id,
                      model_version, symbol, qty, submit_ts_ms, ref_px, expected_px, mid_px,
                      bid_px, ask_px, spread_bps, broker_order_id, status, extra_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        order_id,
                        "paper",
                        None,
                        int(alert_id),
                        model_id,
                        None,
                        symbol,
                        1.0,
                        int(submit_ts_ms),
                        100.0,
                        100.0,
                        100.0,
                        None,
                        None,
                        None,
                        None,
                        "filled",
                        order_extra,
                    ),
                )
                con.execute(
                    """
                    INSERT INTO pnl_attribution(
                      ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                      slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(now_ms),
                        int(alert_id),
                        model_id,
                        None,
                        symbol,
                        float(pnl_value),
                        0.0,
                        0.0,
                        0.0,
                        100.0,
                        float(pnl_value),
                        0.0,
                        json.dumps({"total_pnl": float(pnl_value)}, separators=(",", ":"), sort_keys=True),
                    ),
                )
            con.commit()
        finally:
            con.close()

        result = model_marketplace.recompute_marketplace_scores()
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(int(result.get("rows_written") or 0), 2)

        con = storage.connect(readonly=True)
        try:
            scored = {
                str(row[0]): {
                    "score": float(row[1] or 0.0),
                    "net_pnl": float(row[2] or 0.0),
                    "meta": json.loads(row[3] or "{}"),
                }
                for row in con.execute(
                    """
                    SELECT model_name, score, net_pnl, meta_json
                    FROM model_marketplace_scores
                    ORDER BY model_name ASC
                    """
                ).fetchall()
            }
        finally:
            con.close()

        self.assertEqual(set(scored.keys()), {"model_a", "model_b"})
        self.assertAlmostEqual(scored["model_a"]["net_pnl"], 20.0)
        self.assertAlmostEqual(scored["model_b"]["net_pnl"], 20.0)
        self.assertGreater(scored["model_b"]["score"], scored["model_a"]["score"])
        self.assertEqual(str(scored["model_a"]["meta"].get("score_source") or ""), "pnl_attribution")
        self.assertIn("risk_adjusted_score", scored["model_a"]["meta"])
        self.assertIn("rolling_sortino_like", scored["model_a"]["meta"])
        self.assertIn("rolling_pnl_volatility", scored["model_a"]["meta"])
        self.assertGreater(
            float(scored["model_a"]["meta"].get("max_drawdown") or 0.0),
            float(scored["model_b"]["meta"].get("max_drawdown") or 0.0),
        )

    def test_evaluate_competition_cycle_clears_champion_on_drawdown_without_real_fallback(self) -> None:
        with patch.dict(
            os.environ,
            {
                "CHAMPION_PROMOTION_MIN_TRADES": "3",
                "CHAMPION_PROMOTION_MIN_OBSERVATION_S": "1",
                "CHAMPION_DEMOTION_MAX_DRAWDOWN": "100",
            },
            clear=False,
        ):
            storage, champion_manager = _reload_modules(
                "engine.runtime.storage",
                "engine.strategy.champion_manager",
            )

        storage.init_db()
        now_ms = int(time.time() * 1000)
        two_hours_ago = int(now_ms - (2 * 60 * 60 * 1000))

        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO model_marketplace_scores(
                  model_id, model_name, symbol, horizon_s, regime, stage, score, trades, wins,
                  losses, gross_pnl, net_pnl, avg_confidence, last_signal_ts_ms, updated_ts_ms, meta_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "m1",
                    "model_a",
                    "AAPL",
                    300,
                    "global",
                    "champion",
                    0.45,
                    4,
                    2,
                    2,
                    60.0,
                    60.0,
                    0.8,
                    int(now_ms - 1000),
                    int(now_ms),
                    json.dumps(
                        {
                            "score_source": "pnl_attribution",
                            "risk_adjusted_score": 0.45,
                            "rolling_total_pnl": 60.0,
                            "rolling_realized_pnl": 60.0,
                            "rolling_unrealized_pnl": 0.0,
                            "rolling_window_ms": 86400000,
                            "recent_total_pnl": -90.0,
                            "prior_total_pnl": 30.0,
                            "max_drawdown": 180.0,
                            "first_signal_ts_ms": int(two_hours_ago),
                            "last_signal_ts_ms": int(now_ms - 1000),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
            con.execute(
                """
                INSERT INTO model_marketplace_scores(
                  model_id, model_name, symbol, horizon_s, regime, stage, score, trades, wins,
                  losses, gross_pnl, net_pnl, avg_confidence, last_signal_ts_ms, updated_ts_ms, meta_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "m2",
                    "model_b",
                    "AAPL",
                    300,
                    "global",
                    "challenger",
                    0.20,
                    1,
                    1,
                    0,
                    25.0,
                    25.0,
                    0.7,
                    int(now_ms - 500),
                    int(now_ms),
                    json.dumps(
                        {
                            "score_source": "pnl_attribution",
                            "risk_adjusted_score": 0.20,
                            "rolling_total_pnl": 25.0,
                            "rolling_realized_pnl": 25.0,
                            "rolling_unrealized_pnl": 0.0,
                            "rolling_window_ms": 86400000,
                            "recent_total_pnl": 25.0,
                            "prior_total_pnl": 0.0,
                            "max_drawdown": 5.0,
                            "first_signal_ts_ms": int(now_ms - 5000),
                            "last_signal_ts_ms": int(now_ms - 500),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
            con.execute(
                """
                INSERT INTO champion_assignments(
                  scope, symbol, horizon_s, model_name, challenger_name, regime, state, assigned_ts_ms, updated_ts_ms, meta_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "global",
                    "AAPL",
                    300,
                    "model_a",
                    "",
                    "global",
                    "champion",
                    int(now_ms),
                    int(now_ms),
                    json.dumps({"last_promotion_ts_ms": int(two_hours_ago)}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    champion_manager,
                    "get_cached_replay_validation_snapshot",
                    return_value={"fresh": True, "snapshot": {"models": {}}},
                )
            )
            stack.enter_context(
                patch.object(
                    champion_manager,
                    "run_self_critic",
                    return_value={"blocked_keys": [], "alerts_written": 0},
                )
            )
            stack.enter_context(
                patch.object(
                    champion_manager,
                    "compute_capital_plan",
                    return_value={"ok": True, "allocations": {}, "updated_ts_ms": int(now_ms)},
                )
            )
            result = champion_manager.evaluate_competition_cycle()

        self.assertTrue(result.get("ok"), result)
        self.assertTrue(
            any(
                str(change.get("reason") or "") == "demotion_drawdown"
                and str(change.get("symbol") or "") == "AAPL"
                and str(change.get("to_model_name") or "") == ""
                for change in (result.get("changes") or [])
            ),
            result,
        )
        self.assertEqual(
            champion_manager.get_champion_assignment("global", "AAPL", 300),
            {},
        )

    def test_get_pnl_snapshot_prefers_canonical_state_over_broker_fallback(self) -> None:
        storage, position_store, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.position_store",
            "engine.execution.execution_ledger",
        )
        self._executescript(
            """
            CREATE TABLE IF NOT EXISTS prices (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              price REAL,
              px REAL,
              source TEXT,
              PRIMARY KEY(symbol, ts_ms)
            );
            CREATE TABLE IF NOT EXISTS model_position_state (
              model_id TEXT NOT NULL DEFAULT 'baseline',
              symbol TEXT NOT NULL,
              net_qty REAL NOT NULL DEFAULT 0,
              avg_entry_price REAL NOT NULL DEFAULT 0,
              realized_pnl REAL NOT NULL DEFAULT 0,
              last_update_ts_ms INTEGER NOT NULL,
              PRIMARY KEY (model_id, symbol)
            );
            CREATE TABLE IF NOT EXISTS pnl_attribution (
              ts_ms INTEGER NOT NULL,
              source_alert_id INTEGER NOT NULL,
              model_id TEXT NOT NULL DEFAULT 'baseline',
              model_version TEXT,
              symbol TEXT NOT NULL,
              pnl REAL NOT NULL,
              fees REAL NOT NULL,
              slippage_bps REAL,
              position_size REAL,
              avg_price REAL,
              realized_pnl REAL,
              unrealized_pnl REAL,
              extra_json TEXT,
              PRIMARY KEY (ts_ms, source_alert_id, model_id, symbol)
            );
            CREATE TABLE IF NOT EXISTS broker_account (
              ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER,
              broker TEXT,
              account_id TEXT,
              equity REAL,
              cash REAL,
              buying_power REAL,
              maintenance_margin REAL,
              day_pnl REAL,
              unrealized_pnl REAL,
              realized_pnl REAL,
              currency TEXT,
              extra_json TEXT
            );
            """
        )

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (int(now_ms), "AAPL", 110.0, 110.0, "test"),
            )
            con.execute(
                """
                INSERT INTO model_position_state(model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms)
                VALUES (?,?,?,?,?,?)
                """,
                ("m1", "AAPL", 10.0, 100.0, 25.0, int(now_ms)),
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    1,
                    "m1",
                    None,
                    "AAPL",
                    120.0,
                    5.0,
                    None,
                    10.0,
                    100.0,
                    25.0,
                    100.0,
                    json.dumps({"total_pnl": 120.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO broker_account(
                  ts_ms, updated_ts_ms, broker, account_id, equity, cash, buying_power,
                  maintenance_margin, day_pnl, unrealized_pnl, realized_pnl, currency, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    int(now_ms),
                    "paper",
                    "acct",
                    999999.0,
                    777.0,
                    0.0,
                    0.0,
                    5000.0,
                    5000.0,
                    5000.0,
                    "USD",
                    json.dumps({"total_pnl": 5000.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        canonical = position_store.get_pnl_snapshot("m1")
        self.assertEqual(canonical.get("source"), "canonical")
        self.assertAlmostEqual(float(canonical.get("realized") or 0.0), 25.0)
        self.assertAlmostEqual(float(canonical.get("unrealized") or 0.0), 100.0)
        self.assertAlmostEqual(float(canonical.get("total") or 0.0), 120.0)
        self.assertEqual(float(canonical.get("cash") or 0.0), 0.0)
        self.assertEqual(float(canonical.get("equity") or 0.0), 0.0)

        diagnostic = position_store.get_pnl_snapshot_diagnostic("m1")
        self.assertEqual(diagnostic.get("source"), "canonical+diagnostic")
        self.assertAlmostEqual(float(diagnostic.get("cash") or 0.0), 777.0)

    def test_pnl_attribution_legacy_pnl_matches_total_pnl(self) -> None:
        storage, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
        )
        self._executescript(
            execution_ledger.SCHEMA
            + """
            CREATE TABLE IF NOT EXISTS prices (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              price REAL,
              px REAL,
              source TEXT,
              PRIMARY KEY(symbol, ts_ms)
            );
            """
        )

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (int(now_ms), "AAPL", 110.0, 110.0, "test"),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, portfolio_orders_id, source_alert_id, model_id,
                  model_version, symbol, qty, submit_ts_ms, ref_px, expected_px, mid_px,
                  bid_px, ask_px, spread_bps, broker_order_id, status, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "oid-1",
                    "paper",
                    None,
                    99,
                    "m1",
                    None,
                    "AAPL",
                    10.0,
                    int(now_ms),
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    None,
                    None,
                    "submitted",
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, model_version, symbol,
                  ts_ms, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, expected_px, mid_px,
                  bid_px, ask_px, spread_bps, slippage_bps, fill_latency_ms, fees,
                  commission, liquidity, raw_json, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "oid-1",
                    "fill-1",
                    "paper",
                    "m1",
                    None,
                    "AAPL",
                    int(now_ms),
                    int(now_ms),
                    int(now_ms),
                    10.0,
                    100.0,
                    95.0,
                    96.0,
                    None,
                    None,
                    None,
                    10.0,
                    0,
                    5.0,
                    5.0,
                    "maker",
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_metrics(
                  ts_ms, client_order_id, broker, symbol, submit_qty, filled_qty,
                  ref_px, expected_px, mid_px, fill_px, fill_vwap, spread_bps,
                  slippage_bps, fill_latency_ms, fees, m2m_pnl, last_px
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "oid-1",
                    "paper",
                    "AAPL",
                    10.0,
                    10.0,
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    None,
                    0.0,
                    0,
                    5.0,
                    100.0,
                    110.0,
                ),
            )
            con.commit()
        finally:
            con.close()

        con = storage.connect()
        try:
            result = execution_ledger._recompute_pnl_attribution_snapshot(
                con,
                snapshot_ts_ms=int(now_ms),
                lookback_orders=100,
                historical=False,
            )
            con.commit()
        finally:
            con.close()
        self.assertTrue(result.get("ok"), result)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT pnl, realized_pnl, unrealized_pnl, fees, extra_json
                FROM pnl_attribution
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        extra = json.loads(row[4] or "{}")
        self.assertAlmostEqual(float(row[0] or 0.0), float(extra.get("total_pnl") or 0.0))
        self.assertAlmostEqual(
            float(row[0] or 0.0),
            float(row[1] or 0.0) + float(row[2] or 0.0) - float(row[3] or 0.0) - float(extra.get("slippage_cost") or 0.0),
        )
        self.assertAlmostEqual(float(extra.get("slippage_cost") or 0.0), 1.0)
        self.assertAlmostEqual(float(row[0] or 0.0), 94.0)
        self.assertNotIn("execution_mid_cost", extra)

    def test_canonical_pnl_snapshot_ignores_legacy_pnl_fallbacks(self) -> None:
        storage, position_store, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.position_store",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    101,
                    "m1",
                    "v1",
                    "AAPL",
                    999.0,
                    3.0,
                    0.0,
                    0.0,
                    None,
                    None,
                    10.0,
                    json.dumps({"slippage_cost": 2.0, "total_pnl": 999.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms + 1000),
                    102,
                    "m2",
                    "v2",
                    "MSFT",
                    50.0,
                    1.0,
                    0.0,
                    0.0,
                    None,
                    5.0,
                    6.0,
                    json.dumps({"slippage_cost": 1.0, "total_pnl": 50.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        snapshot = position_store.get_pnl_snapshot("m1")
        self.assertEqual(snapshot.get("source"), "canonical")
        self.assertAlmostEqual(float(snapshot.get("realized") or 0.0), 0.0)
        self.assertAlmostEqual(float(snapshot.get("unrealized") or 0.0), 10.0)
        self.assertAlmostEqual(float(snapshot.get("fees") or 0.0), 3.0)
        self.assertAlmostEqual(float(snapshot.get("slippage") or 0.0), 2.0)
        self.assertAlmostEqual(float(snapshot.get("total") or 0.0), 5.0)

    def test_trade_attribution_ignores_legacy_pnl_fields(self) -> None:
        storage, execution_ledger, trade_attribution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.execution.trade_attribution_ledger",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    202,
                    "m1",
                    "v1",
                    "AAPL",
                    999.0,
                    3.0,
                    0.0,
                    0.0,
                    None,
                    None,
                    10.0,
                    json.dumps({"slippage_cost": 2.0, "total_pnl": 999.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms + 1000),
                    304,
                    "m2",
                    "v2",
                    "MSFT",
                    25.0,
                    1.0,
                    0.0,
                    0.0,
                    None,
                    4.0,
                    5.0,
                    json.dumps({"slippage_cost": 1.0, "total_pnl": 25.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        result = trade_attribution_ledger.upsert_from_pnl_attribution_snapshot(int(now_ms))
        self.assertTrue(result.get("ok"), result)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT pnl, signal_json
                FROM trade_attribution_ledger
                WHERE source_alert_id=202 AND model_id='m1' AND symbol='AAPL'
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        signal_json = json.loads(row[1] or "{}")
        pnl_block = dict(signal_json.get("pnl_attribution") or {})
        self.assertAlmostEqual(float(row[0] or 0.0), 5.0)
        self.assertAlmostEqual(float(pnl_block.get("realized_pnl") or 0.0), 0.0)
        self.assertAlmostEqual(float(pnl_block.get("unrealized_pnl") or 0.0), 10.0)
        self.assertAlmostEqual(float(pnl_block.get("total_pnl") or 0.0), 5.0)

    def test_trade_attribution_loads_execution_order_context_from_projected_events(self) -> None:
        storage, execution_ledger, order_command_boundary, trade_attribution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.execution.order_command_boundary",
            "engine.execution.trade_attribution_ledger",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA + order_command_boundary.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    505,
                    int(now_ms - 50),
                    91,
                    None,
                    "Projected attribution",
                    "AAPL",
                    300,
                    1.1,
                    0.87,
                    "HIGH",
                    "rule.projected",
                    json.dumps({"model_name": "model_one", "model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    "AAPL:300:projected:505",
                    "model_one",
                    "m1",
                    "v1",
                ),
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    505,
                    "m1",
                    "v1",
                    "AAPL",
                    12.5,
                    0.5,
                    4.0,
                    5.0,
                    100.1,
                    7.0,
                    5.5,
                    json.dumps({"slippage_cost": 0.5, "total_pnl": 12.5}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute("DROP TABLE execution_orders")
            command_id = order_command_boundary.record_order_command(
                ts_ms=int(now_ms - 20),
                batch_id=11,
                payload_ts_ms=int(now_ms - 20),
                correlation_id="cid-projected-attribution",
                mode="paper",
                broker="paper",
                payload_source="unit_test",
                real_order_count=1,
                shadow_order_count=0,
                blocked_order_count=0,
                payload={"source_alert_id": 505, "model_id": "m1", "symbol": "AAPL"},
                con=con,
            )
            order_command_boundary.record_order_event(
                ts_ms=int(now_ms - 10),
                event_type="order_submit",
                mode="paper",
                broker="paper",
                status="submitted",
                command_id=str(command_id),
                batch_id=11,
                correlation_id="cid-projected-attribution",
                payload={
                    "client_order_id": "cid-projected-attribution",
                    "portfolio_orders_id": 11,
                    "source_alert_id": 505,
                    "model_id": "m1",
                    "model_version": "v1",
                    "symbol": "AAPL",
                    "submit_ts_ms": int(now_ms - 10),
                    "horizon_s": 300,
                    "regime": "bullish",
                },
                con=con,
            )
            con.commit()
        finally:
            con.close()

        result = trade_attribution_ledger.upsert_from_pnl_attribution_snapshot(int(now_ms))
        self.assertTrue(result.get("ok"), result)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT signal_json, regime_vector_json
                FROM trade_attribution_ledger
                WHERE source_alert_id=505 AND model_id='m1' AND symbol='AAPL'
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        signal_json = json.loads(row[0] or "{}")
        regime_vector_json = json.loads(row[1] or "{}")
        self.assertEqual(
            str(((signal_json.get("execution_order") or {}).get("client_order_id")) or ""),
            "cid-projected-attribution",
        )
        self.assertEqual(int((signal_json.get("execution_order") or {}).get("submit_ts_ms") or 0), int(now_ms - 10))
        self.assertEqual(str(regime_vector_json.get("regime") or ""), "bullish")

    def test_api_execution_stats_ignores_legacy_pnl_fallbacks(self) -> None:
        storage, execution_ledger, api_read = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.api.api_read",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    303,
                    "m1",
                    "v1",
                    "AAPL",
                    999.0,
                    3.0,
                    0.0,
                    0.0,
                    None,
                    None,
                    10.0,
                    json.dumps({"slippage_cost": 2.0, "total_pnl": 999.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        result = api_read.get_execution_stats(model_id="m1")
        metrics = dict(result.get("metrics") or {})
        self.assertTrue(result.get("ok"), result)
        self.assertAlmostEqual(float(metrics.get("sum_realized_pnl") or 0.0), 0.0)
        self.assertAlmostEqual(float(metrics.get("sum_unrealized_pnl") or 0.0), 10.0)
        self.assertAlmostEqual(float(metrics.get("sum_total_pnl") or 0.0), 5.0)

    def test_blacklist_update_job_ignores_legacy_total_pnl_fields(self) -> None:
        storage, execution_ledger, blacklist_update_job = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.runtime.jobs.blacklist_update_job",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    401,
                    "m1",
                    "v1",
                    "AAPL",
                    -999.0,
                    3.0,
                    0.0,
                    0.0,
                    None,
                    10.0,
                    0.0,
                    json.dumps({"slippage_cost": 2.0, "total_pnl": -999.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        with patch.object(blacklist_update_job, "MIN_TRADES", 1), patch.object(
            blacklist_update_job, "PNL_THRESH", -50.0
        ), patch.object(
            blacklist_update_job, "SLIP_BPS_THRESH", 25.0
        ), patch.object(
            blacklist_update_job, "LOOKBACK_HOURS", 72
        ):
            rc = blacklist_update_job.main()

        self.assertEqual(rc, 0)
        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM symbol_blacklist WHERE symbol='AAPL'"
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 0)

    def test_live_stability_guard_job_ignores_legacy_total_pnl_fields(self) -> None:
        storage, execution_ledger, live_stability_guard_job = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.strategy.jobs.live_stability_guard_job",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        today_start_ms = int(now_ms // 86400000) * 86400000
        con = storage.connect()
        try:
            con.execute(
                "INSERT INTO equity_history(ts_ms, equity) VALUES (?, ?)",
                (int(today_start_ms), 100000.0),
            )
            con.execute(
                "INSERT INTO equity_history(ts_ms, equity) VALUES (?, ?)",
                (int(now_ms), 100005.0),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, source_alert_id, model_id, model_version, symbol, qty, submit_ts_ms, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    "guard-order-1",
                    "paper",
                    501,
                    "m1",
                    "v1",
                    "AAPL",
                    1.0,
                    int(now_ms),
                    json.dumps({"execution_target": "real"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    501,
                    "m1",
                    "v1",
                    "AAPL",
                    -999.0,
                    3.0,
                    0.0,
                    0.0,
                    None,
                    10.0,
                    0.0,
                    json.dumps({"slippage_cost": 2.0, "total_pnl": -999.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        printed: list[dict] = []
        with patch.object(live_stability_guard_job, "MAX_DD", 1.0), patch.object(
            live_stability_guard_job, "MAX_DAILY_LOSS", 100.0
        ), patch.object(
            live_stability_guard_job, "MAX_TURNOVER", 10.0
        ), patch.object(
            live_stability_guard_job, "MAX_SLIPPAGE_DRIFT", 1.0
        ), patch.object(live_stability_guard_job, "_print", side_effect=lambda payload: printed.append(dict(payload))), patch.object(
            live_stability_guard_job, "set_execution_armed", return_value=None
        ) as armed_mock, patch.object(
            live_stability_guard_job, "set_execution_mode", return_value=None
        ) as mode_mock:
            rc = live_stability_guard_job.main()

        self.assertEqual(rc, 0)
        self.assertTrue(printed)
        self.assertFalse(bool(printed[-1].get("breach")))
        self.assertAlmostEqual(float(printed[-1].get("daily_pnl") or 0.0), 5.0)
        armed_mock.assert_not_called()
        mode_mock.assert_not_called()

    def test_pnl_decomposition_snapshot_ignores_legacy_realized_pnl_fields(self) -> None:
        storage, execution_ledger, pnl_decomposition_engine = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.strategy.pnl_decomposition_engine",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    601,
                    "m1",
                    "v1",
                    "AAPL",
                    999.0,
                    1.0,
                    0.0,
                    0.0,
                    None,
                    None,
                    4.0,
                    json.dumps({"slippage_cost": 2.0, "total_pnl": 999.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        result = pnl_decomposition_engine.compute_pnl_decomposition_snapshot()
        self.assertTrue(result.get("ok"), result)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT realized_pnl, fees, reconstruction_error, residual_share, quality_status, meta_json
                FROM pnl_decomposition
                WHERE source_alert_id=601 AND symbol='AAPL'
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        meta = json.loads(row[5] or "{}")
        self.assertAlmostEqual(float(row[0] or 0.0), 0.0)
        self.assertAlmostEqual(float(row[1] or 0.0), 1.0)
        self.assertAlmostEqual(float(row[2] or 0.0), 0.0, places=6)
        self.assertGreaterEqual(float(row[3] or 0.0), 0.0)
        self.assertEqual(str(row[4] or ""), "warn")
        self.assertAlmostEqual(float(meta.get("unrealized_pnl") or 0.0), 4.0)

    def test_kill_switch_model_pnl_rows_ignore_legacy_total_pnl_fields(self) -> None:
        storage, execution_ledger, kill_switch = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.execution.kill_switch",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    701,
                    "m1",
                    "v1",
                    "AAPL",
                    -999.0,
                    3.0,
                    0.0,
                    0.0,
                    None,
                    10.0,
                    0.0,
                    json.dumps({"slippage_cost": 2.0, "total_pnl": -999.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        con = storage.connect(readonly=True)
        try:
            rows = kill_switch._model_pnl_rows(con, "m1")
        finally:
            con.close()

        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(float(rows[0][1] or 0.0), 5.0)

    def test_suppression_cost_snapshot_ignores_legacy_trade_attribution_pnl_fields(self) -> None:
        storage, trade_attribution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.trade_attribution_ledger",
        )
        storage.init_db()

        now_ms = int(time.time() * 1000)
        con = storage.connect()
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
                    int(now_ms),
                    801,
                    "m1",
                    "AAPL",
                    json.dumps(
                        {
                            "pnl_attribution": {
                                "realized_pnl": 10.0,
                                "unrealized_pnl": 0.0,
                                "total_pnl": 5.0,
                                "extra": {"slippage_cost": 2.0},
                            }
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    json.dumps({"model_name": "m1"}, separators=(",", ":"), sort_keys=True),
                    None,
                    None,
                    None,
                    -999.0,
                    3.0,
                    0.0,
                    None,
                    int(now_ms),
                ),
            )
            con.commit()
        finally:
            con.close()

        snap = trade_attribution_ledger.suppression_cost_snapshot(lookback_ms=60_000)
        self.assertAlmostEqual(float(snap.get("executed_pnl") or 0.0), 5.0)

    def test_pnl_attribution_tracks_realized_trade_pnl_net_of_costs(self) -> None:
        storage, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        self._executescript(execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, source_alert_id, model_id, model_version, symbol, qty, submit_ts_ms
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                ("entry-1", "paper", 11, "m1", "v1", "AAPL", 10.0, int(now_ms - 2_000)),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, source_alert_id, model_id, model_version, symbol, qty, submit_ts_ms
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                ("exit-1", "paper", 11, "m1", "v1", "AAPL", -10.0, int(now_ms - 1_000)),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, model_version, symbol,
                  ts_ms, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, fees
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("entry-1", "fill-entry-1", "paper", "m1", "v1", "AAPL", int(now_ms - 2_000), int(now_ms - 2_000), int(now_ms - 2_000), 10.0, 100.0, 1.0),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, model_version, symbol,
                  ts_ms, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, fees
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("exit-1", "fill-exit-1", "paper", "m1", "v1", "AAPL", int(now_ms - 1_000), int(now_ms - 1_000), int(now_ms - 1_000), -10.0, 110.0, 2.0),
            )
            con.execute(
                """
                INSERT INTO execution_metrics(
                  ts_ms, client_order_id, broker, symbol, submit_qty, filled_qty,
                  ref_px, expected_px, mid_px, fill_px, fill_vwap, spread_bps,
                  slippage_bps, fill_latency_ms, fees, m2m_pnl, last_px
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), "entry-1", "paper", "AAPL", 10.0, 10.0, 100.0, 100.0, 100.0, 100.0, 100.0, 0.0, 0.0, 0, 1.0, 0.0, 100.0),
            )
            con.execute(
                """
                INSERT INTO execution_metrics(
                  ts_ms, client_order_id, broker, symbol, submit_qty, filled_qty,
                  ref_px, expected_px, mid_px, fill_px, fill_vwap, spread_bps,
                  slippage_bps, fill_latency_ms, fees, m2m_pnl, last_px
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), "exit-1", "paper", "AAPL", -10.0, -10.0, 110.0, 110.0, 110.0, 110.0, 110.0, 0.0, 0.0, 0, 2.0, 0.0, 110.0),
            )
            con.commit()
        finally:
            con.close()

        con = storage.connect()
        try:
            result = execution_ledger._recompute_pnl_attribution_snapshot(
                con,
                snapshot_ts_ms=int(now_ms),
                lookback_orders=100,
                historical=False,
            )
            con.commit()
        finally:
            con.close()
        self.assertTrue(result.get("ok"), result)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT realized_pnl, pnl, extra_json
                FROM pnl_attribution
                WHERE source_alert_id=11 AND model_id='m1' AND symbol='AAPL'
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        extra = json.loads(row[2] or "{}")
        self.assertAlmostEqual(float(row[0] or 0.0), 100.0)
        self.assertAlmostEqual(float(row[1] or 0.0), 97.0)
        self.assertEqual(int(extra.get("realized_trade_count") or 0), 1)
        self.assertEqual(list(extra.get("realized_trade_client_order_ids") or []), ["exit-1"])
        self.assertEqual(len(list(extra.get("realized_trade_pnls") or [])), 1)
        self.assertAlmostEqual(float((extra.get("realized_trade_pnls") or [0.0])[0] or 0.0), 97.0)

    def test_marketplace_scores_use_trade_attribution_identity_and_keep_open_positions_live(self) -> None:
        os.environ["MODEL_COMPETITION_WINDOW_S"] = "60"
        try:
            storage, execution_ledger, portfolio, model_marketplace = _reload_modules(
                "engine.runtime.storage",
                "engine.execution.execution_ledger",
                "engine.strategy.portfolio",
                "engine.strategy.model_marketplace",
            )
            storage.init_db()
            self._executescript(execution_ledger.SCHEMA + portfolio.SCHEMA)

            now_ms = int(time.time() * 1000)
            old_submit_ts_ms = int(now_ms - 300_000)
            con = storage.connect()
            try:
                con.execute(
                    """
                    INSERT INTO pnl_attribution(
                      ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                      slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(now_ms),
                        21,
                        "m_live",
                        "v1",
                        "AAPL",
                        35.0,
                        5.0,
                        0.0,
                        10.0,
                        100.0,
                        0.0,
                        40.0,
                        json.dumps(
                            {
                                "total_pnl": 35.0,
                                "last_px": 104.0,
                                "notional_traded": 1000.0,
                                "realized_trade_count": 0,
                                "realized_trade_pnls": [],
                            },
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                    ),
                )
                con.execute(
                    """
                    INSERT INTO execution_orders(
                      client_order_id, broker, source_alert_id, model_id, model_version, symbol, qty, submit_ts_ms, extra_json
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "cid-open-1",
                        "paper",
                        21,
                        "m_live",
                        "v1",
                        "AAPL",
                        10.0,
                        int(old_submit_ts_ms),
                        json.dumps({}, separators=(",", ":"), sort_keys=True),
                    ),
                )
                con.execute(
                    """
                    INSERT INTO alerts(
                      id, ts_ms, event_id, event_title, symbol, horizon_s, expected_z,
                      confidence, severity, rule_id, explain_json, dedupe_key
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        21,
                        int(old_submit_ts_ms),
                        2100,
                        "AAPL live position",
                        "AAPL",
                        300,
                        1.0,
                        0.8,
                        "high",
                        "rule.live",
                        json.dumps({}, separators=(",", ":"), sort_keys=True),
                        "dedupe-live-21",
                    ),
                )
                con.execute(
                    """
                    INSERT INTO portfolio_orders(
                      ts_ms, model_id, symbol, action, from_side, to_side, from_weight, to_weight, delta_weight, source_alert_id, explain_json
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(old_submit_ts_ms),
                        "m_live",
                        "AAPL",
                        "OPEN",
                        "FLAT",
                        "LONG",
                        0.0,
                        0.1,
                        0.1,
                        21,
                        json.dumps({"strategy": {"name": "model_live"}}, separators=(",", ":"), sort_keys=True),
                    ),
                )
                con.commit()
            finally:
                con.close()

            result = model_marketplace.recompute_marketplace_scores()
            self.assertTrue(result.get("ok"), result)
            self.assertEqual(int(result.get("rows_written") or 0), 1)

            con = storage.connect(readonly=True)
            try:
                row = con.execute(
                    """
                    SELECT model_id, model_name, symbol, horizon_s, net_pnl, meta_json
                    FROM model_marketplace_scores
                    WHERE model_id='m_live'
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                con.close()

            self.assertIsNotNone(row)
            meta = json.loads(row[5] or "{}")
            self.assertEqual(str(row[0]), "m_live")
            self.assertEqual(str(row[1]), "model_live")
            self.assertEqual(str(row[2]), "AAPL")
            self.assertEqual(int(row[3] or 0), 300)
            self.assertAlmostEqual(float(row[4] or 0.0), 35.0)
            self.assertEqual(str(meta.get("score_source") or ""), "pnl_attribution")
            self.assertAlmostEqual(float(meta.get("rolling_unrealized_pnl") or 0.0), 40.0)
            self.assertAlmostEqual(float(meta.get("rolling_total_pnl") or 0.0), 35.0)
        finally:
            os.environ.pop("MODEL_COMPETITION_WINDOW_S", None)

    def test_event_replay_capital_reconciliation_snapshot_passes_authoritative_batch(self) -> None:
        storage, portfolio, event_log, event_replay = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.runtime.event_log",
            "engine.runtime.event_replay",
        )
        storage.init_db()
        self._executescript(
            portfolio.SCHEMA
            + """
            CREATE TABLE IF NOT EXISTS earnings_calendar (
              symbol TEXT NOT NULL,
              earnings_date TEXT NOT NULL,
              time_of_day TEXT,
              eps_est REAL,
              eps_act REAL,
              revenue_est REAL,
              revenue_act REAL,
              source TEXT,
              updated_ts_ms INTEGER NOT NULL,
              PRIMARY KEY(symbol, earnings_date)
            );
            """
        )

        now_ms = int(time.time() * 1000)
        explain = {
            "model_id": "m1",
            "model_name": "model_one",
            "model_version": "v1",
            "regime": "global",
            "horizon_s": 300,
            "strategy": {"name": "s1"},
            "execution": {"strategy_alloc": {"s1": 1.0}},
            "reason": {
                "strategy": "s1",
                "strategy_alloc": {"s1": 1.0},
                "competition": {
                    "policy": {
                        "group_key": "AAPL|300|global",
                        "group_budget_fraction": 0.20,
                        "model_budget_fraction": 0.20,
                        "regime": "global",
                    },
                    "reason_code": "competition_capital_applied",
                    "capital_applied_upstream": True,
                    "model_name": "model_one",
                    "regime": "global",
                    "horizon_s": 300,
                },
            },
        }

        con = storage.connect()
        try:
            event_log.append_event(
                event_type="allocator_decision",
                event_source="unit_test",
                entity_type="rebalance",
                entity_id="rebalance-1",
                payload={"ok": True},
                ts_ms=int(now_ms),
                con=con,
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, event_title, symbol, horizon_s, expected_z,
                  confidence, severity, rule_id, explain_json, dedupe_key
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    101,
                    int(now_ms),
                    1001,
                    "AAPL long",
                    "AAPL",
                    300,
                    1.2,
                    0.9,
                    "high",
                    "rule.s1",
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    "dedupe-aapl-101",
                ),
            )
            con.execute(
                """
                INSERT INTO strategy_allocations(ts_ms, window_days, allocations_json, reason_json)
                VALUES (?,?,?,?)
                """,
                (
                    int(now_ms),
                    0,
                    json.dumps({"s1": 1.0}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"allocation_sum": 1.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO runtime_meta(key, value, updated_ts_ms)
                VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts_ms=excluded.updated_ts_ms
                """,
                (
                    "competition_capital_plan",
                    json.dumps(
                        {
                            "updated_ts_ms": int(now_ms),
                            "competition_total_capital_fraction": 0.20,
                            "total_group_budget_fraction_post": 0.20,
                            "allocations": {
                                "AAPL|300|global": {
                                    "symbol": "AAPL",
                                    "horizon_s": 300,
                                    "regime": "global",
                                    "group_budget_fraction": 0.20,
                                    "models": [
                                        {
                                            "model_name": "model_one",
                                            "allocation_fraction": 1.0,
                                            "effective_allocation_fraction": 0.20,
                                            "model_risk_limit_multiplier": 1.0,
                                        }
                                    ],
                                }
                            },
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    int(now_ms),
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side, from_weight, to_weight, delta_weight, source_alert_id, explain_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "m1",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.20,
                    0.20,
                    101,
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_risk_snapshots(ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    0.20,
                    0.20,
                    0.05,
                    0.01,
                    0,
                    json.dumps(
                        {
                            "final_gross": 0.20,
                            "final_net": 0.20,
                            "allocation_reconciliation": {
                                "by_strategy": {"s1": {"pre": 0.20, "post": 0.20, "delta": 0.0}},
                                "by_model": {"model_one|global": {"pre": 0.20, "post": 0.20, "delta": 0.0}},
                            },
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
            con.commit()
        finally:
            con.close()

        snap = event_replay.replay_capital_reconciliation_snapshot(limit=10)

        self.assertTrue(bool(snap.get("ok")))
        self.assertTrue(bool(snap.get("passed")), snap)
        self.assertEqual(int((snap.get("severity_counts") or {}).get("error") or 0), 0)
        self.assertEqual(
            int(((snap.get("summary") or {}).get("latest_portfolio_orders") or {}).get("actionable_order_count") or 0),
            1,
        )
        self.assertEqual(
            int(((snap.get("summary") or {}).get("execution_intents") or {}).get("real_count") or 0),
            1,
        )

    def test_event_replay_capital_reconciliation_snapshot_flags_upstream_budget_overage(self) -> None:
        storage, portfolio, event_log, event_replay = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.runtime.event_log",
            "engine.runtime.event_replay",
        )
        storage.init_db()
        self._executescript(
            portfolio.SCHEMA
            + """
            CREATE TABLE IF NOT EXISTS earnings_calendar (
              symbol TEXT NOT NULL,
              earnings_date TEXT NOT NULL,
              time_of_day TEXT,
              eps_est REAL,
              eps_act REAL,
              revenue_est REAL,
              revenue_act REAL,
              source TEXT,
              updated_ts_ms INTEGER NOT NULL,
              PRIMARY KEY(symbol, earnings_date)
            );
            """
        )

        now_ms = int(time.time() * 1000)
        explain = {
            "model_id": "m1",
            "model_name": "model_one",
            "model_version": "v1",
            "regime": "global",
            "horizon_s": 300,
            "strategy": {"name": "s1"},
            "execution": {"strategy_alloc": {"s1": 1.0}},
            "reason": {
                "strategy": "s1",
                "strategy_alloc": {"s1": 1.0},
                "competition": {
                    "policy": {
                        "group_key": "AAPL|300|global",
                        "group_budget_fraction": 0.20,
                        "model_budget_fraction": 0.20,
                        "regime": "global",
                    },
                    "reason_code": "competition_capital_applied",
                    "capital_applied_upstream": True,
                    "model_name": "model_one",
                    "regime": "global",
                    "horizon_s": 300,
                },
            },
        }

        con = storage.connect()
        try:
            event_log.append_event(
                event_type="allocator_decision",
                event_source="unit_test",
                entity_type="rebalance",
                entity_id="rebalance-2",
                payload={"ok": True},
                ts_ms=int(now_ms),
                con=con,
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, event_title, symbol, horizon_s, expected_z,
                  confidence, severity, rule_id, explain_json, dedupe_key
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    102,
                    int(now_ms),
                    1002,
                    "AAPL long over budget",
                    "AAPL",
                    300,
                    1.2,
                    0.9,
                    "high",
                    "rule.s1",
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    "dedupe-aapl-102",
                ),
            )
            con.execute(
                """
                INSERT INTO strategy_allocations(ts_ms, window_days, allocations_json, reason_json)
                VALUES (?,?,?,?)
                """,
                (
                    int(now_ms),
                    0,
                    json.dumps({"s1": 1.0}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"allocation_sum": 1.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO runtime_meta(key, value, updated_ts_ms)
                VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts_ms=excluded.updated_ts_ms
                """,
                (
                    "competition_capital_plan",
                    json.dumps(
                        {
                            "updated_ts_ms": int(now_ms),
                            "competition_total_capital_fraction": 0.20,
                            "total_group_budget_fraction_post": 0.20,
                            "allocations": {
                                "AAPL|300|global": {
                                    "symbol": "AAPL",
                                    "horizon_s": 300,
                                    "regime": "global",
                                    "group_budget_fraction": 0.20,
                                    "models": [
                                        {
                                            "model_name": "model_one",
                                            "allocation_fraction": 1.0,
                                            "effective_allocation_fraction": 0.20,
                                            "model_risk_limit_multiplier": 1.0,
                                        }
                                    ],
                                }
                            },
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    int(now_ms),
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side, from_weight, to_weight, delta_weight, source_alert_id, explain_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "m1",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.25,
                    0.25,
                    102,
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_risk_snapshots(ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    0.25,
                    0.25,
                    0.05,
                    0.01,
                    0,
                    json.dumps(
                        {
                            "final_gross": 0.25,
                            "final_net": 0.25,
                            "allocation_reconciliation": {
                                "by_strategy": {"s1": {"pre": 0.25, "post": 0.25, "delta": 0.0}},
                                "by_model": {"model_one|global": {"pre": 0.25, "post": 0.25, "delta": 0.0}},
                            },
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
            con.commit()
        finally:
            con.close()

        snap = event_replay.replay_capital_reconciliation_snapshot(limit=10)
        finding_codes = {str((row or {}).get("code") or "") for row in list(snap.get("findings") or [])}

        self.assertTrue(bool(snap.get("ok")))
        self.assertFalse(bool(snap.get("passed")), snap)
        self.assertIn("portfolio_orders_model_budget_overage", finding_codes)


if __name__ == "__main__":
    unittest.main()
