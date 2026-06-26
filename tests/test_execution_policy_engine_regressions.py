from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import unittest
from contextlib import ExitStack
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


class ExecutionPolicyEngineRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_backup = {
            "ENGINE_SUPERVISED": os.environ.get("ENGINE_SUPERVISED"),
        }
        os.environ["ENGINE_SUPERVISED"] = "1"
        (self.execution_policy_engine,) = _reload_modules(
            "engine.execution.execution_policy_engine",
        )
        self.con = sqlite3.connect(":memory:")
        self._init_db_patch = patch.object(self.execution_policy_engine, "init_db", return_value=None)
        self._init_db_patch.start()

    def tearDown(self) -> None:
        try:
            self._init_db_patch.stop()
        except Exception:
            pass
        try:
            self.con.close()
        except Exception:
            pass
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)

    def _record_suppression(self, **kwargs) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_attribution_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                suppression_reason TEXT,
                decision_json TEXT
            )
            """
        )
        self.con.execute(
            """
            INSERT INTO trade_attribution_ledger(suppression_reason, decision_json)
            VALUES(?,?)
            """,
            (
                str(kwargs.get("suppression_reason") or ""),
                json.dumps(kwargs.get("decision_json") or {}, separators=(",", ":"), sort_keys=True),
            ),
        )

    def _patch_policy_dependencies(self) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "execution_allowed",
                return_value=(True, "", {}),
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "evaluate_trade_suppression",
                return_value={
                    "state": "NONE",
                    "action": "NONE",
                    "size_mult": 1.0,
                    "throttle_mult": 1.0,
                    "hard_block": False,
                    "reason": "",
                },
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "update_capital_preservation_mode",
                return_value={},
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "get_state",
                return_value="normal",
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "_regime_compatibility",
                return_value=(1.0, {"regime": "test"}),
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "load_execution_feedback_snapshot",
                return_value={},
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "build_alpha_handoff",
                side_effect=lambda *_args, **kwargs: dict(kwargs),
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "decide_execution_strategy",
                return_value={
                    "order_type": "MARKET",
                    "aggressiveness": "AGGRESSIVE",
                    "latency_mult": 1.0,
                    "chunk_pct": 1.0,
                    "sim_extra_slippage_bps": 0.0,
                    "size_mult": 1.0,
                    "execution_policy": "balanced",
                    "entry_strategy": "immediate",
                    "entry_delay_ms": 0,
                    "expected_slippage_bps": 0.0,
                    "expected_fill_latency_ms": 0,
                    "limit_offset_bps": 0.0,
                },
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "get_execution_mode",
                return_value="paper",
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "log_suppression",
                side_effect=self._record_suppression,
            )
        )
        return stack

    def test_qty_slicing_preserves_total_requested_quantity(self) -> None:
        now_ms = 1_710_000_000_000
        order = {
            "symbol": "AAPL",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.04,
            "confidence": 0.9,
            "expected_z": 1.5,
            "source_order_id": 101,
        }

        with self._patch_policy_dependencies():
            with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                shaped = self.execution_policy_engine.apply_execution_policy(
                    [order],
                    con=self.con,
                    actor="test",
                    mode="paper",
                    broker="sim",
                )

        self.assertEqual(len(shaped), 7)
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 10.0, places=9)
        self.assertTrue(all(float(row.get("qty") or 0.0) > 0.0 for row in shaped))

        audit_row = self.con.execute(
            "SELECT policy_json FROM execution_policy_audit ORDER BY id DESC LIMIT 1"
        ).fetchone()

        self.assertIsNotNone(audit_row)
        policy = json.loads(str(audit_row[0] or "{}"))
        self.assertEqual(int(policy.get("slices") or 0), 7)

    def test_none_trade_suppression_live_defaults_to_soft_throttle_and_warns(self) -> None:
        now_ms = 1_710_000_000_000
        order = {
            "symbol": "TEST",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "source_order_id": 401,
        }
        warnings: list[tuple[str, Exception, dict]] = []

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("EXECUTION_NONE_SUPPRESSION_SOFT_MULT", None)
            with self._patch_policy_dependencies():
                with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                    with patch.object(
                        self.execution_policy_engine,
                        "live_ai_order_guard",
                        return_value={"ok": True, "required": False, "reason": "test"},
                    ):
                        with patch.object(
                            self.execution_policy_engine,
                            "uncertainty_gate_from_payload",
                            return_value={
                                "enabled": True,
                                "applied": False,
                                "hard_block": False,
                                "action": "NONE",
                                "multiplier": 1.0,
                                "reason": "",
                            },
                        ):
                            with patch.object(
                                self.execution_policy_engine,
                                "_warn_nonfatal",
                                side_effect=lambda code, error, **kwargs: warnings.append((str(code), error, dict(kwargs))),
                            ):
                                shaped = self.execution_policy_engine.apply_execution_policy(
                                    [order],
                                    con=self.con,
                                    actor="test",
                                    mode="live",
                                    broker="sim",
                                    trade_suppression_fn=lambda **_kwargs: None,
                                )

        self.assertTrue(shaped)
        self.assertTrue(all(str(row.get("tse_state") or "") == "SOFT_THROTTLE" for row in shaped))
        self.assertTrue(all(str(row.get("tse_action") or "") == "SOFT_THROTTLE" for row in shaped))
        self.assertTrue(all(abs(float(row.get("tse_size_mult") or 0.0) - 0.5) <= 1.0e-12 for row in shaped))
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 5.0, places=9)
        self.assertIn("EXECUTION_POLICY_ENGINE_SUPPRESSION_NONE_LIVE", [code for code, _error, _kwargs in warnings])

        audit_row = self.con.execute(
            "SELECT decision_json FROM execution_policy_audit ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(audit_row)
        decision = json.loads(str(audit_row[0] or "{}"))
        tse = dict(decision.get("tse") or {})
        self.assertEqual(str(tse.get("state") or ""), "SOFT_THROTTLE")
        self.assertAlmostEqual(float(tse.get("size_mult") or 0.0), 0.5, places=9)
        self.assertIs(tse.get("hard_block"), False)
        self.assertEqual(str(tse.get("reason") or ""), "suppression_eval_none_conservative_default")

    def test_none_trade_suppression_non_live_preserves_none_default(self) -> None:
        now_ms = 1_710_000_000_000
        order = {
            "symbol": "TEST",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "source_order_id": 402,
        }
        warnings: list[tuple[str, Exception, dict]] = []

        with self._patch_policy_dependencies():
            with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                with patch.object(
                    self.execution_policy_engine,
                    "_warn_nonfatal",
                    side_effect=lambda code, error, **kwargs: warnings.append((str(code), error, dict(kwargs))),
                ):
                    shaped = self.execution_policy_engine.apply_execution_policy(
                        [order],
                        con=self.con,
                        actor="test",
                        mode="paper",
                        broker="sim",
                        trade_suppression_fn=lambda **_kwargs: None,
                    )

        self.assertTrue(shaped)
        self.assertTrue(all(str(row.get("tse_state") or "") == "NONE" for row in shaped))
        self.assertTrue(all(abs(float(row.get("tse_size_mult") or 0.0) - 1.0) <= 1.0e-12 for row in shaped))
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 10.0, places=9)
        self.assertNotIn("EXECUTION_POLICY_ENGINE_SUPPRESSION_NONE_LIVE", [code for code, _error, _kwargs in warnings])

    def test_learned_execution_slicing_preserves_trade_intent_and_stamps_guard(self) -> None:
        from engine.execution.contextual_bandit_slicer import validate_routed_learned_orders

        now_ms = 1_710_000_005_000
        order = {
            "symbol": "AAPL",
            "qty": 20.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.04,
            "confidence": 0.9,
            "expected_z": 1.5,
            "source_order_id": 111,
            "learned_execution_slicing": 1,
            "true_spread_bps": 35.0,
            "intraday_vol_bps": 90.0,
            "adverse_selection_bps": 14.0,
            "fill_risk": 0.05,
        }

        with self._patch_policy_dependencies():
            with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                with patch.object(
                    self.execution_policy_engine,
                    "evaluate_policy_ope_gate",
                    return_value=(True, {"applied": True, "passed": True, "status": "passed"}),
                ):
                    shaped = self.execution_policy_engine.apply_execution_policy(
                        [order],
                        con=self.con,
                        actor="test",
                        mode="paper",
                        broker="sim",
                    )

        self.assertGreater(len(shaped), 0)
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 20.0, places=9)
        self.assertTrue(all(str(row.get("symbol") or "") == "AAPL" for row in shaped))
        self.assertTrue(all(str(row.get("side") or "").upper() == "BUY" for row in shaped))
        self.assertTrue(all(int(row.get("execution_policy_locked") or 0) == 1 for row in shaped))
        self.assertTrue(all(int(row.get("learned_execution_locked") or 0) == 1 for row in shaped))
        self.assertIsNone(validate_routed_learned_orders(shaped))

        first = dict(shaped[0])
        self.assertEqual(first.get("learned_execution_policy_scope"), "execution_only")
        constraints = dict(first.get("learned_execution_constraints") or {})
        self.assertLessEqual(
            float(first.get("learned_execution_slice_pct") or 0.0),
            float(constraints.get("base_slice_pct") or 0.0),
        )
        self.assertEqual(
            sorted(first.get("learned_execution_allowed_fields") or []),
            [
                "entry_delay_ms",
                "slice_interval_ms",
                "slice_pct",
                "target_participation",
            ],
        )

    def test_learned_execution_slicing_blocks_without_passing_ope(self) -> None:
        now_ms = 1_710_000_005_500
        order = {
            "symbol": "AAPL",
            "qty": 20.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.04,
            "confidence": 0.9,
            "expected_z": 1.5,
            "source_order_id": 112,
            "learned_execution_slicing": 1,
            "true_spread_bps": 35.0,
            "intraday_vol_bps": 90.0,
            "adverse_selection_bps": 14.0,
            "fill_risk": 0.05,
        }

        with self._patch_policy_dependencies():
            with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                with patch.object(
                    self.execution_policy_engine,
                    "evaluate_policy_ope_gate",
                    return_value=(False, {"applied": True, "passed": False, "status": "missing_propensities"}),
                ):
                    shaped = self.execution_policy_engine.apply_execution_policy(
                        [order],
                        con=self.con,
                        actor="test",
                        mode="paper",
                        broker="sim",
                    )

        self.assertGreater(len(shaped), 0)
        self.assertTrue(all(int(row.get("execution_policy_locked") or 0) == 1 for row in shaped))
        self.assertTrue(all(int(row.get("learned_execution_locked") or 0) == 0 for row in shaped))
        learned = dict(shaped[0].get("learned_execution") or {})
        self.assertEqual(learned.get("reason"), "ope_gate_blocked")
        self.assertEqual(dict(learned.get("ope_gate") or {}).get("status"), "missing_propensities")

    def test_ood_log_only_audits_without_suppressing(self) -> None:
        now_ms = 1_710_000_010_000
        order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "ood_score": 9.0,
            "ood_threshold": 1.5,
            "ood_hard_threshold": 3.0,
            "source_order_id": 301,
        }

        with patch.dict(os.environ, {"OOD_MODE": "log_only"}, clear=False):
            with self._patch_policy_dependencies():
                with patch.object(
                    self.execution_policy_engine,
                    "score_order_meta_label",
                    return_value={"enabled": False, "applied": False, "probability": None, "multiplier": 1.0},
                ):
                    with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=self.con,
                            actor="test",
                            mode="paper",
                            broker="sim",
                        )

        self.assertGreater(len(shaped), 0)
        self.assertTrue(all(float(row.get("ood_size_mult") or 0.0) == 1.0 for row in shaped))
        audit_row = self.con.execute(
            "SELECT policy_json FROM execution_policy_audit ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(audit_row)
        policy = json.loads(str(audit_row[0] or "{}"))
        ood_gate = dict(policy.get("ood_gate") or {})
        self.assertEqual(str(ood_gate.get("mode") or ""), "log_only")
        self.assertEqual(str(ood_gate.get("action") or ""), "LOG_ONLY")

    def test_ood_suppress_mode_hard_blocks_far_vector(self) -> None:
        now_ms = 1_710_000_020_000
        order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "ood_score": 4.0,
            "ood_threshold": 1.5,
            "ood_hard_threshold": 3.0,
            "source_order_id": 302,
        }

        with patch.dict(os.environ, {"OOD_MODE": "suppress"}, clear=False):
            with self._patch_policy_dependencies():
                with patch.object(
                    self.execution_policy_engine,
                    "score_order_meta_label",
                    return_value={"enabled": False, "applied": False, "probability": None, "multiplier": 1.0},
                ):
                    with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=self.con,
                            actor="test",
                            mode="paper",
                            broker="sim",
                        )

        self.assertEqual(shaped, [])
        row = self.con.execute(
            "SELECT suppression_reason, decision_json FROM trade_attribution_ledger ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0] or ""), "ood_hard_block")
        decision = json.loads(str(row[1] or "{}"))
        self.assertEqual(str(decision.get("blocked_by") or ""), "ood_hard_block")

    def test_live_risk_increasing_order_requires_uncertainty_production_policy(self) -> None:
        now_ms = 1_710_000_030_000
        order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "source_order_id": 303,
        }

        env = {
            "DECISION_ENGINE_ENABLED": "1",
            "DECISION_MIN_CONFIDENCE": "0.70",
            "DECISION_MIN_ABS_PREDICTION": "0.80",
            "UNCERTAINTY_SIZING_MODE": "log_only",
            "UNCERTAINTY_HIGH_THRESHOLD": "0.70",
            "UNCERTAINTY_HARD_THRESHOLD": "0.95",
            "UNCERTAINTY_MAX_AGE_MS": "300000",
            "OOD_SUPPRESS_THRESHOLD": "1.50",
            "OOD_HARD_THRESHOLD": "3.00",
            "EXECUTION_MODE": "live",
        }
        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("UNCERTAINTY_SIZING_PRODUCTION_POLICY", None)
            with self._patch_policy_dependencies():
                with patch.object(
                    self.execution_policy_engine,
                    "score_order_meta_label",
                    return_value={"enabled": False, "applied": False, "probability": None, "multiplier": 1.0},
                ):
                    with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=self.con,
                            actor="test",
                            mode="live",
                            broker="ibkr",
                        )

        self.assertEqual(shaped, [])
        row = self.con.execute(
            "SELECT suppression_reason, decision_json FROM trade_attribution_ledger ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0] or ""), "live_ai_safety_live_uncertainty_production_policy_missing")
        decision = json.loads(str(row[1] or "{}"))
        self.assertEqual(str(decision.get("blocked_by") or ""), "live_ai_safety")

    def test_live_order_blocks_model_serving_fallback_before_execution(self) -> None:
        now_ms = 1_710_000_035_000
        order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "model_name": "live_model",
            "serve_fallback_active": True,
            "fallback_reason": "requested_live_model_unavailable",
            "source_order_id": 304,
        }

        env = {
            "DECISION_ENGINE_ENABLED": "1",
            "DECISION_MIN_CONFIDENCE": "0.70",
            "DECISION_MIN_ABS_PREDICTION": "0.80",
            "UNCERTAINTY_SIZING_MODE": "log_only",
            "UNCERTAINTY_SIZING_PRODUCTION_POLICY": "strict",
            "UNCERTAINTY_HIGH_THRESHOLD": "0.70",
            "UNCERTAINTY_HARD_THRESHOLD": "0.95",
            "UNCERTAINTY_MAX_AGE_MS": "300000",
            "OOD_SUPPRESS_THRESHOLD": "1.50",
            "OOD_HARD_THRESHOLD": "3.00",
            "EXECUTION_MODE": "live",
        }
        with patch.dict(os.environ, env, clear=False):
            with self._patch_policy_dependencies():
                with patch.object(
                    self.execution_policy_engine,
                    "score_order_meta_label",
                    return_value={"enabled": False, "applied": False, "probability": None, "multiplier": 1.0},
                ):
                    with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=self.con,
                            actor="test",
                            mode="live",
                            broker="ibkr",
                        )

        self.assertEqual(shaped, [])
        row = self.con.execute(
            "SELECT suppression_reason, decision_json FROM trade_attribution_ledger ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0] or ""), "live_ai_safety_live_model_resolution_fallback")
        decision = json.loads(str(row[1] or "{}"))
        self.assertEqual(str(decision.get("blocked_by") or ""), "live_ai_safety")
        gate = dict(decision.get("live_ai_safety") or {})
        self.assertIn("live_model_resolution_fallback", list(gate.get("blockers") or []))

    def test_conformal_gate_and_size_shrinks_wide_interval_order(self) -> None:
        now_ms = 1_710_000_040_000
        order = {
            "symbol": "AAPL",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "conformal": {
                "available": True,
                "ts_ms": now_ms,
                "interval_excludes_zero": True,
                "interval_lower": 0.1,
                "interval_upper": 3.1,
                "interval_width": 3.0,
                "size_mult": 0.25,
            },
            "source_order_id": 304,
        }

        with patch.dict(os.environ, {"CONFORMAL_MODE": "gate_and_size"}, clear=False):
            with self._patch_policy_dependencies():
                with patch.object(
                    self.execution_policy_engine,
                    "score_order_meta_label",
                    return_value={"enabled": False, "applied": False, "probability": None, "multiplier": 1.0},
                ):
                    with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=self.con,
                            actor="test",
                            mode="paper",
                            broker="sim",
                        )

        self.assertTrue(shaped)
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 2.5, places=9)
        for row in shaped:
            self.assertAlmostEqual(float(row.get("uncertainty_size_mult") or 0.0), 0.25, places=9)
            self.assertEqual(str(row.get("uncertainty_action") or ""), "SIZE_COMPRESSION")

    def test_high_epistemic_uncertainty_shrinks_risk_increasing_order(self) -> None:
        now_ms = 1_710_000_050_000
        order = {
            "symbol": "AAPL",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "model_intent": {
                "epistemic_uncertainty": 0.75,
                "uncertainty_ts_ms": now_ms,
            },
            "source_order_id": 305,
        }
        env = {
            "UNCERTAINTY_SIZING_MODE": "enforce",
            "UNCERTAINTY_HIGH_THRESHOLD": "0.50",
            "UNCERTAINTY_HARD_THRESHOLD": "1.00",
        }

        with patch.dict(os.environ, env, clear=False):
            with self._patch_policy_dependencies():
                with patch.object(
                    self.execution_policy_engine,
                    "score_order_meta_label",
                    return_value={"enabled": False, "applied": False, "probability": None, "multiplier": 1.0},
                ):
                    with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=self.con,
                            actor="test",
                            mode="paper",
                            broker="sim",
                        )

        self.assertTrue(shaped)
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 5.0, places=9)
        for row in shaped:
            self.assertAlmostEqual(float(row.get("uncertainty_size_mult") or 0.0), 0.5, places=9)
            gate = dict(row.get("uncertainty_gate") or {})
            self.assertIn("high_model_uncertainty", list(gate.get("reasons") or []))

    def test_stale_uncertainty_shrinks_live_order_when_policy_allows(self) -> None:
        now_ms = 1_710_000_060_000
        order = {
            "symbol": "AAPL",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.9,
            "expected_z": 1.5,
            "model_intent": {
                "uncertainty": 0.1,
                "uncertainty_ts_ms": now_ms - 10_000,
            },
            "source_order_id": 306,
        }
        env = {
            "DECISION_ENGINE_ENABLED": "1",
            "DECISION_MIN_CONFIDENCE": "0.70",
            "DECISION_MIN_ABS_PREDICTION": "0.80",
            "EXECUTION_MODE": "live",
            "UNCERTAINTY_SIZING_PRODUCTION_POLICY": "shrink",
            "UNCERTAINTY_HIGH_THRESHOLD": "0.70",
            "UNCERTAINTY_HARD_THRESHOLD": "0.95",
            "UNCERTAINTY_MAX_AGE_MS": "1000",
            "UNCERTAINTY_STALE_SIZE_MULT": "0.20",
            "OOD_SUPPRESS_THRESHOLD": "1.50",
            "OOD_HARD_THRESHOLD": "3.00",
        }

        with patch.dict(os.environ, env, clear=False):
            with self._patch_policy_dependencies():
                with patch.object(
                    self.execution_policy_engine,
                    "score_order_meta_label",
                    return_value={"enabled": False, "applied": False, "probability": None, "multiplier": 1.0},
                ):
                    with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=self.con,
                            actor="test",
                            mode="live",
                            broker="ibkr",
                        )

        self.assertTrue(shaped)
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 2.0, places=9)
        for row in shaped:
            gate = dict(row.get("uncertainty_gate") or {})
            self.assertAlmostEqual(float(row.get("uncertainty_size_mult") or 0.0), 0.2, places=9)
            self.assertIn("stale_uncertainty", list(gate.get("reasons") or []))

    def test_alpha_decay_boundary_suppresses_order_at_exact_ttl(self) -> None:
        now_ms = 1_710_000_100_000
        order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "side": "BUY",
            "signal_ts_ms": now_ms - 1_000,
            "alpha_ttl_ms": 1_000,
            "alpha_half_life_ms": 250,
            "confidence": 0.8,
            "expected_z": 1.0,
            "source_alert_id": 55,
            "source_order_id": 202,
        }

        with self._patch_policy_dependencies():
            with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                shaped = self.execution_policy_engine.apply_execution_policy(
                    [order],
                    con=self.con,
                    actor="test",
                    mode="paper",
                    broker="sim",
                )

        self.assertEqual(shaped, [])

        suppression_row = self.con.execute(
            """
            SELECT suppression_reason
            FROM trade_attribution_ledger
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

        self.assertIsNotNone(suppression_row)
        self.assertEqual(str(suppression_row[0] or ""), "alpha_decay_expired")

    def test_meta_label_probability_scales_size_and_is_audited(self) -> None:
        now_ms = 1_710_000_150_000
        order = {
            "symbol": "AAPL",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.8,
            "expected_z": 1.2,
            "meta_label_probability": 0.55,
            "source_order_id": 303,
        }

        with self._patch_policy_dependencies():
            with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                shaped = self.execution_policy_engine.apply_execution_policy(
                    [order],
                    con=self.con,
                    actor="test",
                    mode="paper",
                    broker="sim",
                )

        self.assertTrue(shaped)
        self.assertAlmostEqual(sum(float(row.get("qty") or 0.0) for row in shaped), 5.0, places=9)
        for row in shaped:
            self.assertAlmostEqual(float(row.get("meta_label_size_mult") or 0.0), 0.5, places=9)

        audit_row = self.con.execute(
            """
            SELECT decision_json, policy_json
            FROM execution_policy_audit
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        self.assertIsNotNone(audit_row)
        decision = json.loads(str(audit_row[0] or "{}"))
        policy = json.loads(str(audit_row[1] or "{}"))
        self.assertIn("meta_label", decision)
        self.assertAlmostEqual(float(decision["meta_label"]["probability"]), 0.55, places=9)
        self.assertAlmostEqual(float(decision["meta_label"]["multiplier"]), 0.5, places=9)
        self.assertIn("meta_label", policy)


class ExecutionPolicyEngineStorageFreeTests(unittest.TestCase):
    def setUp(self) -> None:
        (self.execution_policy_engine,) = _reload_modules(
            "engine.execution.execution_policy_engine",
        )

    def _patch_policy_dependencies(self, logged_suppressions: list[dict]) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(patch.object(self.execution_policy_engine, "init_db", return_value=None))
        stack.enter_context(patch.object(self.execution_policy_engine, "_ensure_tables", return_value=None))
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "execution_allowed",
                return_value=(True, "", {}),
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "update_capital_preservation_mode",
                return_value={},
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "get_state",
                return_value="normal",
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "_regime_compatibility",
                return_value=(1.0, {"regime": "test"}),
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "load_execution_feedback_snapshot",
                return_value={},
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "build_alpha_handoff",
                side_effect=lambda *_args, **kwargs: dict(kwargs),
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "decide_execution_strategy",
                return_value={
                    "order_type": "MARKET",
                    "aggressiveness": "AGGRESSIVE",
                    "latency_mult": 1.0,
                    "chunk_pct": 1.0,
                    "sim_extra_slippage_bps": 0.0,
                    "size_mult": 1.0,
                    "execution_policy": "balanced",
                    "entry_strategy": "immediate",
                    "entry_delay_ms": 0,
                    "expected_slippage_bps": 0.0,
                    "expected_fill_latency_ms": 0,
                    "limit_offset_bps": 0.0,
                },
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "get_execution_mode",
                return_value="paper",
            )
        )
        stack.enter_context(
            patch.object(
                self.execution_policy_engine,
                "log_suppression",
                side_effect=lambda **kwargs: logged_suppressions.append(dict(kwargs)),
            )
        )
        return stack

    def test_size_compression_scaled_to_zero_logs_dedicated_suppression(self) -> None:
        now_ms = 1_710_000_200_000
        order = {
            "symbol": "AAPL",
            "qty": 1.5e-9,
            "side": "BUY",
            "signal_ts_ms": now_ms,
            "alpha_ttl_ms": 60_000,
            "alpha_half_life_ms": 60_000,
            "volatility": 0.01,
            "confidence": 0.8,
            "expected_z": 1.0,
            "source_alert_id": 56,
            "source_order_id": 203,
        }
        tse_snapshot = {
            "state": "SIZE_COMPRESSION",
            "action": "SIZE_COMPRESSION",
            "size_mult": 0.65,
            "throttle_mult": 1.0,
            "hard_block": False,
            "reason": "unit_test_compression",
        }
        logged_suppressions: list[dict] = []
        fake_con = type("FakeConnection", (), {"commit": lambda self: None})()

        with self._patch_policy_dependencies(logged_suppressions):
            with patch.object(self.execution_policy_engine, "evaluate_trade_suppression", return_value=tse_snapshot):
                with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                    shaped = self.execution_policy_engine.apply_execution_policy(
                        [order],
                        con=fake_con,
                        actor="test",
                        mode="paper",
                        broker="sim",
                    )

        self.assertEqual(shaped, [])
        self.assertEqual(len(logged_suppressions), 1)
        self.assertEqual(
            str(logged_suppressions[0].get("suppression_reason") or ""),
            "tse_size_compression_scaled_to_zero",
        )
        decision = dict(logged_suppressions[0].get("decision_json") or {})
        meta = dict(decision.get("meta") or {})
        self.assertAlmostEqual(float(meta.get("original_qty") or 0.0), 1.5e-9, places=12)
        self.assertAlmostEqual(float(meta.get("compressed_qty") or 0.0), 9.75e-10, places=12)

    def test_size_compression_suppresses_when_alpha_below_gate(self) -> None:
        now_ms = 1_710_000_200_000
        order = {
            "symbol": "AAPL",
            "qty": 10.0,
            "side": "BUY",
            "signal_ts_ms": now_ms - 4_000,
            "alpha_ttl_ms": 10_000,
            "alpha_half_life_ms": 1_000,
            "volatility": 0.01,
            "confidence": 0.8,
            "expected_z": 1.0,
            "source_alert_id": 57,
            "source_order_id": 204,
        }
        tse_snapshot = {
            "state": "SIZE_COMPRESSION",
            "action": "SIZE_COMPRESSION",
            "size_mult": 0.65,
            "throttle_mult": 1.0,
            "hard_block": False,
            "reason": "unit_test_low_alpha",
        }
        logged_suppressions: list[dict] = []
        fake_con = type("FakeConnection", (), {"commit": lambda self: None})()

        with self._patch_policy_dependencies(logged_suppressions):
            with patch.object(self.execution_policy_engine, "evaluate_trade_suppression", return_value=tse_snapshot):
                with patch.object(self.execution_policy_engine, "_now_ms", return_value=now_ms):
                    with patch.object(self.execution_policy_engine, "TSE_SOFT_MIN_ALPHA", 0.30):
                        shaped = self.execution_policy_engine.apply_execution_policy(
                            [order],
                            con=fake_con,
                            actor="test",
                            mode="paper",
                            broker="sim",
                        )

        self.assertEqual(shaped, [])
        self.assertEqual(len(logged_suppressions), 1)
        self.assertEqual(
            str(logged_suppressions[0].get("suppression_reason") or ""),
            "tse_size_compression_alpha_gate",
        )
        decision = dict(logged_suppressions[0].get("decision_json") or {})
        self.assertEqual(str(decision.get("blocked_by") or ""), "tse_size_compression")
        self.assertLess(float(decision.get("alpha_remaining") or 0.0), 0.30)


if __name__ == "__main__":
    unittest.main()
