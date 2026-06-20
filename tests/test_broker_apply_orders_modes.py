from __future__ import annotations

import importlib
import io
import json
import os
import sys
import tempfile
import time
import unittest
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from typing import Any, Dict, Iterable
from unittest.mock import Mock, patch

from engine.runtime.live_trading_preflight import DEFAULT_LIVE_CONFIRM_PHRASE

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class BrokerApplyOrdersModeTests(unittest.TestCase):
    ENV_KEYS = (
        "DB_PATH",
        "ENGINE_SUPERVISED",
        "ALLOW_TRAINING",
        "LIVE_BROKER",
        "BROKER",
        "BROKER_NAME",
        "BROKER_FAILOVER",
        "BROKER_START_CASH",
        "EXECUTION_MODE",
        "ENGINE_MODE",
        "OPERATOR_MODE",
        "MODE",
        "ENGINE_RUNTIME_MODE",
        "RUNTIME_MODE",
        "ENV",
        "TS_ENV",
        "PROD_LOCK",
        "LIVE_TRADING_CONFIRM",
        "STARTUP_HEALTH_LATE_FAILURE",
        "EXEC_ADAPTIVE_SLICING",
        "BROKER_LATENCY_SLEEP",
        "KILL_SWITCH_GLOBAL",
        "TRADING_KILL_SWITCH",
        "KILL_SWITCH",
        "KILL_SWITCH_SYMBOLS",
        "KILL_SWITCH_REGIMES",
        "KILL_SWITCH_MODELS",
        "KILL_SWITCH_REQUIRE_FRESH_DATA",
        "KILL_SWITCH_REQUIRE_FRESH_JOBS",
        "KILL_SWITCH_REQUIRED_JOBS",
        "KILL_SWITCH_MODEL_MAX_DRAWDOWN",
        "KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES",
        "CAPITAL_AWARE_KILL_SWITCH",
        "MODEL_AWARE_KILL_SWITCH",
        "CAPITAL_STOP_DRAWDOWN",
        "DRAWDOWN_MIN_HISTORY_POINTS",
        "DISABLE_LIVE_EXECUTION",
        "EXECUTION_PRELIVE_RECONCILE",
        "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS",
        "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR",
        "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON",
        "TS_STORAGE_BACKEND",
        "TS_PG_DSN",
        "PG_DSN",
        "TIMESCALE_DSN",
        "TS_PG_SCHEMA",
        "TS_PG_SCHEMA_PER_DB_PATH",
        "TS_REDIS_KEY_PREFIX",
        "DECISION_ENGINE_ENABLED",
        "DECISION_MIN_CONFIDENCE",
        "DECISION_MIN_ABS_PREDICTION",
        "UNCERTAINTY_SIZING_PRODUCTION_POLICY",
        "UNCERTAINTY_HIGH_THRESHOLD",
        "UNCERTAINTY_HARD_THRESHOLD",
        "UNCERTAINTY_MAX_AGE_MS",
        "OOD_SUPPRESS_THRESHOLD",
        "OOD_HARD_THRESHOLD",
        "RL_ALLOW_FALLBACK_AGENT",
        "EXECUTION_MAX_SIGNAL_AGE_S",
    )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "broker_apply_orders_modes.db"
        self._env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}

        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ["ALLOW_TRAINING"] = "0"
        os.environ["LIVE_BROKER"] = "sim"
        os.environ["BROKER"] = "sim"
        os.environ["BROKER_NAME"] = "sim"
        os.environ["BROKER_FAILOVER"] = "sim"
        os.environ["BROKER_START_CASH"] = "100000"
        os.environ.pop("OPERATOR_MODE", None)
        os.environ.pop("MODE", None)
        os.environ.pop("ENGINE_RUNTIME_MODE", None)
        os.environ.pop("RUNTIME_MODE", None)
        os.environ.pop("ENV", None)
        os.environ.pop("TS_ENV", None)
        os.environ.pop("PROD_LOCK", None)
        os.environ.pop("LIVE_TRADING_CONFIRM", None)
        os.environ.pop("STARTUP_HEALTH_LATE_FAILURE", None)
        os.environ["EXEC_ADAPTIVE_SLICING"] = "0"
        os.environ["BROKER_LATENCY_SLEEP"] = "0"
        os.environ["KILL_SWITCH_GLOBAL"] = "0"
        os.environ["TRADING_KILL_SWITCH"] = "0"
        os.environ["KILL_SWITCH"] = "0"
        os.environ["KILL_SWITCH_SYMBOLS"] = ""
        os.environ["KILL_SWITCH_REGIMES"] = ""
        os.environ["KILL_SWITCH_MODELS"] = ""
        os.environ["KILL_SWITCH_REQUIRE_FRESH_DATA"] = "1"
        os.environ["KILL_SWITCH_REQUIRE_FRESH_JOBS"] = "1"
        os.environ["KILL_SWITCH_REQUIRED_JOBS"] = "ingestion_runtime,process_events"
        os.environ["KILL_SWITCH_MODEL_MAX_DRAWDOWN"] = "0"
        os.environ["KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES"] = "0"
        os.environ["CAPITAL_AWARE_KILL_SWITCH"] = "1"
        os.environ["MODEL_AWARE_KILL_SWITCH"] = "1"
        os.environ["CAPITAL_STOP_DRAWDOWN"] = "0.25"
        os.environ["DRAWDOWN_MIN_HISTORY_POINTS"] = "5"
        os.environ["DISABLE_LIVE_EXECUTION"] = "1"
        os.environ["EXECUTION_PRELIVE_RECONCILE"] = "1"
        os.environ.pop("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS", None)
        os.environ.pop("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR", None)
        os.environ.pop("EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON", None)
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ.pop("TS_PG_DSN", None)
        os.environ.pop("PG_DSN", None)
        os.environ.pop("TIMESCALE_DSN", None)
        os.environ.pop("TS_PG_SCHEMA", None)
        os.environ["TS_PG_SCHEMA_PER_DB_PATH"] = "1"
        os.environ["TS_REDIS_KEY_PREFIX"] = f"broker_apply_orders_modes_{Path(self.tmp.name).name}"
        os.environ["EXECUTION_MODE"] = "paper"
        os.environ["ENGINE_MODE"] = "paper"
        os.environ.pop("DECISION_ENGINE_ENABLED", None)
        os.environ.pop("DECISION_MIN_CONFIDENCE", None)
        os.environ.pop("DECISION_MIN_ABS_PREDICTION", None)
        os.environ.pop("UNCERTAINTY_SIZING_PRODUCTION_POLICY", None)
        os.environ.pop("UNCERTAINTY_HIGH_THRESHOLD", None)
        os.environ.pop("UNCERTAINTY_HARD_THRESHOLD", None)
        os.environ.pop("UNCERTAINTY_MAX_AGE_MS", None)
        os.environ.pop("OOD_SUPPRESS_THRESHOLD", None)
        os.environ.pop("OOD_HARD_THRESHOLD", None)
        os.environ.pop("RL_ALLOW_FALLBACK_AGENT", None)
        os.environ["EXECUTION_MAX_SIGNAL_AGE_S"] = "3600"

        self._reload_runtime_modules()
        self._reset_runtime_controls()

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass

        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

        self.tmp.cleanup()

    def _reload_runtime_modules(self) -> None:
        (
            _,
            self.storage,
            self.state_cache,
            self.risk_state,
            self.runtime_meta,
            self.lifecycle_state,
            self.execution_mode,
            self.drawdown_state,
            self.capital_guard,
            self.kill_switch,
            _decision_engine,
            self.portfolio_execution_intents,
            self.execution_policy_engine,
            self.broker_router,
            self.broker_sim,
            self.broker_apply_orders,
        ) = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.runtime.state_cache",
            "engine.runtime.risk_state",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.execution.execution_mode",
            "engine.strategy.drawdown_state",
            "engine.strategy.capital_guard",
            "engine.execution.kill_switch",
            "engine.decision_engine",
            "engine.strategy.portfolio_execution_intents",
            "engine.execution.execution_policy_engine",
            "engine.execution.broker_router",
            "engine.execution.broker_sim",
            "engine.execution.broker_apply_orders",
        )

    def _reset_runtime_controls(self) -> None:
        self.storage.init_db()
        for namespace in ("risk_state", "risk_state_row", "api_read", "portfolio_snapshot"):
            self.state_cache.cache_invalidate_namespace(namespace)
        for key, value in (
            ("trading_state", "enabled"),
            ("stop_reason", ""),
            ("portfolio_risk_block", "0"),
            ("portfolio_risk_info", ""),
            ("portfolio_risk_status", ""),
            ("execution_pause", "0"),
            ("tse_state", "NONE"),
            ("tse_action", "NONE"),
            ("tse_reason", ""),
        ):
            self.risk_state.set_state(key, value)

    def _db_fetchone(self, sql: str, params: Iterable[Any] = ()) -> Any:
        con = self.storage.connect_ro_direct()
        try:
            row = con.execute(sql, tuple(params)).fetchone()
            return None if row is None else row[0]
        finally:
            con.close()

    def _seed_runtime_live(self, *, ts_ms: int | None = None) -> int:
        now_ms = int(ts_ms or int(time.time() * 1000))
        self.storage.init_db()
        self.runtime_meta.meta_set("first_price_ts_ms", str(now_ms - 1000))
        self.lifecycle_state.set_state(self.lifecycle_state.LIVE, "test_runtime_live")

        con = self.storage.connect()
        try:
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (now_ms, "AAPL", 100.0, 100.0, "test"),
            )
            con.execute(
                """
                INSERT INTO events(
                  ts_ms, timestamp, event_type, symbol, source, title, body, url,
                  importance_score, raw_payload, derived_features, meta_json,
                  source_id, dedupe_hash, event_key
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    now_ms,
                    now_ms,
                    "test_event",
                    "AAPL",
                    "test",
                    "test",
                    "test",
                    None,
                    0.1,
                    "{}",
                    "{}",
                    "{}",
                    "src",
                    "hash",
                    "event-key-1",
                ),
            )
            con.execute(
                """
                INSERT INTO predictions(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id,
                  model_version, regime_time_ms, volatility_regime,
                  trend_regime, liquidity_regime
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    now_ms,
                    1,
                    "AAPL",
                    3600,
                    1.0,
                    0.7,
                    0.7,
                    1.0,
                    "baseline",
                    "baseline",
                    "v1",
                    now_ms,
                    "normal",
                    "up",
                    "normal",
                ),
            )
            for job_name in ("ingestion_runtime", "process_events"):
                con.execute(
                    "INSERT INTO job_heartbeats(job_name, owner, pid, ts_ms, extra_json) VALUES (?,?,?,?,?)",
                    (job_name, "test", 123, now_ms, "{}"),
                )
            con.commit()
        finally:
            con.close()
        return now_ms

    def _set_mode(self, mode: str, *, armed: int = 0) -> None:
        os.environ["EXECUTION_MODE"] = str(mode)
        os.environ["ENGINE_MODE"] = str(mode)
        if str(mode).strip().lower() == "live":
            self._set_live_ai_contract_env()
        if str(mode).strip().lower() == "live" and int(armed or 0) == 1:
            os.environ.setdefault("LIVE_TRADING_CONFIRM", DEFAULT_LIVE_CONFIRM_PHRASE)
        self._reload_runtime_modules()
        self.execution_mode.set_execution_mode(str(mode), actor="test", reason="unit_test")
        if str(mode).strip().lower() == "live" and int(armed or 0) == 1:
            with patch.object(
                self.execution_mode,
                "_assert_live_arming_preflight",
                return_value=None,
            ):
                self.execution_mode.set_execution_armed(int(armed), actor="test", reason="unit_test")
        else:
            self.execution_mode.set_execution_armed(int(armed), actor="test", reason="unit_test")

    def _set_live_ai_contract_env(self) -> None:
        os.environ["DECISION_ENGINE_ENABLED"] = "1"
        os.environ["DECISION_MIN_CONFIDENCE"] = "0.01"
        os.environ["DECISION_MIN_ABS_PREDICTION"] = "0.01"
        os.environ["UNCERTAINTY_SIZING_PRODUCTION_POLICY"] = "strict"
        os.environ["UNCERTAINTY_HIGH_THRESHOLD"] = "0.70"
        os.environ["UNCERTAINTY_HARD_THRESHOLD"] = "0.95"
        os.environ["UNCERTAINTY_MAX_AGE_MS"] = "300000"
        os.environ["OOD_SUPPRESS_THRESHOLD"] = "1.50"
        os.environ["OOD_HARD_THRESHOLD"] = "3.00"
        os.environ["RL_ALLOW_FALLBACK_AGENT"] = "0"

    def _seed_live_alpaca_route(self) -> None:
        os.environ["DISABLE_LIVE_EXECUTION"] = "0"
        os.environ["LIVE_TRADING_CONFIRM"] = DEFAULT_LIVE_CONFIRM_PHRASE
        os.environ["LIVE_BROKER"] = "alpaca"
        os.environ["BROKER"] = "alpaca"
        os.environ["BROKER_NAME"] = "alpaca"
        os.environ["BROKER_FAILOVER"] = "alpaca"
        os.environ["CAPITAL_AWARE_KILL_SWITCH"] = "0"
        os.environ["MODEL_AWARE_KILL_SWITCH"] = "0"
        now_ms = self._seed_runtime_live()
        self._set_mode("live", armed=1)
        self._seed_portfolio_order(ts_ms=now_ms)

    def _healthy_live_route_stack(self, stack: ExitStack, *, adapter: Mock) -> None:
        gates = importlib.import_module("engine.runtime.gates")
        def _passthrough_execution_policy(*args: Any, **kwargs: Any) -> list[dict]:
            if "intents" in kwargs:
                return list(kwargs.get("intents") or [])
            return list((args[0] if args else []) or [])

        stack.enter_context(patch.object(gates, "live_trading_preflight", return_value={"ok": True, "reason": "ok"}))
        stack.enter_context(patch("engine.runtime.health.run_preflight", return_value={"ok": True}))
        stack.enter_context(patch.object(self.broker_apply_orders, "apply_execution_policy", side_effect=_passthrough_execution_policy))
        stack.enter_context(patch.object(self.broker_apply_orders, "execution_allowed", return_value=(True, "ok", {})))
        stack.enter_context(patch.object(self.broker_apply_orders, "get_competition_policy_for_intent", return_value={}))
        stack.enter_context(patch.object(self.broker_apply_orders, "pre_live_position_reconcile", return_value={"ok": True}))
        stack.enter_context(
            patch.object(
                self.broker_apply_orders,
                "apply_execution_risk_governor",
                side_effect=lambda _con, orders, **_kwargs: (list(orders or []), {"ok": True}),
            )
        )
        stack.enter_context(
            patch.object(
                self.broker_apply_orders,
                "refresh_broker_connection_health",
                return_value={"ok": True, "state": "connected"},
            )
        )
        stack.enter_context(
            patch.object(
                self.broker_apply_orders,
                "refresh_execution_quality_supervisor",
                return_value={"ok": True, "state": "ok"},
            )
        )
        stack.enter_context(patch.object(self.broker_router, "_prelive_reconcile", return_value={"ok": True}))
        stack.enter_context(patch.object(self.broker_router, "_alpaca_apply", adapter))

    def _seed_portfolio_order(
        self,
        *,
        ts_ms: int,
        symbol: str = "AAPL",
        model_id: str = "baseline",
        action: str = "rebalance",
        from_side: str = "FLAT",
        to_side: str = "LONG",
        from_weight: float = 0.0,
        to_weight: float = 0.10,
        source_alert_id: int = 1,
        explain: Dict[str, Any] | None = None,
    ) -> None:
        explain_payload = dict(explain or {"model_name": model_id, "regime": "global"})
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side,
                  from_weight, to_weight, delta_weight, source_alert_id, explain_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(ts_ms),
                    str(model_id),
                    str(symbol),
                    str(action),
                    str(from_side),
                    str(to_side),
                    float(from_weight),
                    float(to_weight),
                    float(to_weight - from_weight),
                    int(source_alert_id),
                    json.dumps(explain_payload, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

    def _run_main(self, *, competition_policy: Dict[str, Any] | None = None) -> tuple[int, Dict[str, Any], str]:
        tse_snapshot = {
            "state": "NONE",
            "action": "NONE",
            "size_mult": 1.0,
            "throttle_mult": 1.0,
            "hard_block": False,
            "reason": "",
        }

        def _build_alpha_handoff(order: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
            payload = {"symbol": order.get("symbol")}
            payload.update(kwargs)
            return payload

        def _decide_execution_strategy(**_: Any) -> Dict[str, Any]:
            return {
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
            }

        with ExitStack() as stack:
            stack.enter_context(patch.object(self.broker_apply_orders, "evaluate_rules", return_value=None))
            stack.enter_context(patch.object(self.broker_apply_orders, "persist_execution_advisories", return_value=None))
            stack.enter_context(
                patch.object(
                    self.portfolio_execution_intents,
                    "get_competition_policy_for_intent",
                    return_value=dict(competition_policy or {}),
                )
            )
            stack.enter_context(patch.object(self.execution_policy_engine, "evaluate_trade_suppression", return_value=tse_snapshot))
            stack.enter_context(patch.object(self.execution_policy_engine, "update_capital_preservation_mode", return_value={}))
            stack.enter_context(patch.object(self.execution_policy_engine, "compute_regime_vector", return_value={"regime": "test"}))
            stack.enter_context(patch.object(self.execution_policy_engine, "regime_compatibility", return_value=1.0))
            stack.enter_context(patch.object(self.execution_policy_engine, "regime_model_version", return_value="test-regime"))
            stack.enter_context(patch.object(self.execution_policy_engine, "load_execution_feedback_snapshot", return_value={}))
            stack.enter_context(patch.object(self.execution_policy_engine, "build_alpha_handoff", side_effect=_build_alpha_handoff))
            stack.enter_context(patch.object(self.execution_policy_engine, "decide_execution_strategy", side_effect=_decide_execution_strategy))
            stack.enter_context(patch.object(self.broker_router.time, "sleep", return_value=None))
            stack.enter_context(patch.object(self.broker_sim.time, "sleep", return_value=None))
            stack.enter_context(patch.object(self.broker_sim, "_earnings_proximity_decay", return_value=0.0))
            stack.enter_context(patch.object(self.broker_sim, "get_execution_liquidity_snapshot", return_value={}))

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = self.broker_apply_orders.main()

        stdout = buf.getvalue()
        json_lines = [line for line in stdout.splitlines() if line.strip().startswith("{")]
        self.assertTrue(json_lines, msg=f"expected broker_apply_orders JSON output, got: {stdout!r}")
        return rc, json.loads(json_lines[-1]), stdout

    def test_shadow_mode_execution(self) -> None:
        now_ms = self._seed_runtime_live()
        self._set_mode("shadow")
        self._seed_portfolio_order(ts_ms=now_ms)

        rc, out, _ = self._run_main(
            competition_policy={
                "allowed": False,
                "blocked": True,
                "reason": "champion_mismatch",
                "champion_model_name": "champion",
            }
        )

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["mode"], "shadow")
        self.assertTrue(out["executed"])
        self.assertGreaterEqual(int(out["shadow_result"]["submitted_models"]), 1)
        self.assertEqual(
            self._db_fetchone("SELECT COUNT(*) FROM broker_shadow_account WHERE book_key='shadow:baseline'"),
            1,
        )
        self.assertGreater(
            int(self._db_fetchone("SELECT COUNT(*) FROM shadow_order_intents") or 0),
            0,
        )
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 1)
        self.assertEqual(self._db_fetchone("SELECT mode FROM order_commands ORDER BY ts_ms DESC LIMIT 1"), "shadow")
        self.assertEqual(self._db_fetchone("SELECT status FROM order_commands ORDER BY ts_ms DESC LIMIT 1"), "executed")
        self.assertEqual(self._db_fetchone("SELECT status FROM order_events ORDER BY id DESC LIMIT 1"), "executed")

    def test_paper_mode_execution(self) -> None:
        now_ms = self._seed_runtime_live()
        self._set_mode("paper")
        self._seed_portfolio_order(ts_ms=now_ms)

        rc, out, _ = self._run_main(competition_policy={})

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["mode"], "paper")
        self.assertEqual(out["result"]["broker"], "sim")
        self.assertEqual(
            self._db_fetchone("SELECT value FROM execution_meta WHERE key='last_execution_source'"),
            "paper_broker_sim",
        )
        self.assertGreater(
            int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0),
            0,
        )
        self.assertEqual(
            int(self._db_fetchone("SELECT COUNT(*) FROM broker_shadow_account") or 0),
            0,
        )
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 1)
        self.assertEqual(self._db_fetchone("SELECT mode FROM order_commands ORDER BY ts_ms DESC LIMIT 1"), "paper")
        self.assertEqual(self._db_fetchone("SELECT status FROM order_commands ORDER BY ts_ms DESC LIMIT 1"), "executed")
        self.assertEqual(self._db_fetchone("SELECT status FROM order_events ORDER BY id DESC LIMIT 1"), "executed")

    def test_safe_mode_blocks_execution(self) -> None:
        now_ms = self._seed_runtime_live()
        self._set_mode("safe")
        self._seed_portfolio_order(ts_ms=now_ms)

        rc, out, _ = self._run_main()

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "execution_gate")
        self.assertEqual(out["mode"], "safe")
        self.assertEqual(out["broker"], "sim")
        self.assertFalse(bool(out["gate"]["allowed"]))
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 0)
        self.assertEqual(self._db_fetchone("SELECT status FROM order_events ORDER BY id DESC LIMIT 1"), "blocked")

    def test_paper_mode_blocks_missing_broker_result(self) -> None:
        now_ms = self._seed_runtime_live()
        self._set_mode("paper")
        self._seed_portfolio_order(ts_ms=now_ms)

        with patch.object(
            self.broker_apply_orders,
            "apply_shadow_portfolio_orders",
            return_value={"ok": True, "status": "applied"},
        ):
            rc, out, _ = self._run_main(competition_policy={})

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "paper_broker_result")
        self.assertEqual(out["mode"], "paper")
        self.assertEqual(out["broker"], "sim")
        self.assertEqual(out["reason"], "paper_broker_missing_broker")
        self.assertEqual(out["result_status"], "applied")
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(self._db_fetchone("SELECT status FROM order_events ORDER BY id DESC LIMIT 1"), "blocked")

    def test_live_mode_blocked_when_not_allowed(self) -> None:
        os.environ["DISABLE_LIVE_EXECUTION"] = "0"
        now_ms = self._seed_runtime_live()
        self._set_mode("live", armed=0)
        self._seed_portfolio_order(ts_ms=now_ms)

        rc, out, _ = self._run_main()

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "execution_gate")
        self.assertEqual(out["reason"], "mode_live_unarmed")
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(
            int(self._db_fetchone("SELECT COUNT(*) FROM event_log WHERE event_type='order_decision'") or 0),
            0,
        )
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_events") or 0), 1)
        self.assertEqual(self._db_fetchone("SELECT event_type FROM order_events ORDER BY id DESC LIMIT 1"), "execution_block")
        self.assertEqual(self._db_fetchone("SELECT status FROM order_events ORDER BY id DESC LIMIT 1"), "blocked")

    def test_live_mode_unarmed_non_kill_switch_cascade_still_execution_gate(self) -> None:
        os.environ["DISABLE_LIVE_EXECUTION"] = "0"
        now_ms = self._seed_runtime_live()
        self._set_mode("live", armed=0)
        self._seed_portfolio_order(ts_ms=now_ms)

        with patch.object(
            self.broker_apply_orders,
            "execution_allowed",
            return_value=(False, "runtime_state_block", {"scope": "global", "key": "UNKNOWN"}),
        ):
            rc, out, _ = self._run_main()

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "execution_gate")
        self.assertEqual(out["reason"], "mode_live_unarmed")
        self.assertEqual(out["kill_switch_cascade"]["reason"], "runtime_state_block")
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_events") or 0), 1)

    def test_live_mode_unarmed_preserves_actual_kill_switch_layer(self) -> None:
        os.environ["DISABLE_LIVE_EXECUTION"] = "0"
        now_ms = self._seed_runtime_live()
        self._set_mode("live", armed=0)
        self._seed_portfolio_order(ts_ms=now_ms)
        self.kill_switch.activate("global", "global", reason="unit_test", actor="test")
        self.lifecycle_state.set_state(self.lifecycle_state.LIVE, "kill_switch_db_still_active")

        rc, out, _ = self._run_main()

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "kill_switch")
        self.assertIn("kill_switch", str(out["reason"]))
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_events") or 0), 1)

    def test_live_mode_armed_blocked_by_disable_live_execution(self) -> None:
        os.environ["DISABLE_LIVE_EXECUTION"] = "0"
        os.environ["LIVE_TRADING_CONFIRM"] = DEFAULT_LIVE_CONFIRM_PHRASE
        now_ms = self._seed_runtime_live()
        self._set_mode("live", armed=1)
        os.environ["DISABLE_LIVE_EXECUTION"] = "1"
        self._seed_portfolio_order(ts_ms=now_ms)

        with patch.object(self.kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}):
            rc, out, _ = self._run_main()

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "kill_switch")
        self.assertEqual(out["reason"], "disable_live_execution_env")
        self.assertEqual(out["meta"]["key"], "DISABLE_LIVE_EXECUTION")
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(
            int(self._db_fetchone("SELECT COUNT(*) FROM event_log WHERE event_type='order_decision'") or 0),
            0,
        )
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_events") or 0), 1)
        self.assertEqual(self._db_fetchone("SELECT event_type FROM order_events ORDER BY id DESC LIMIT 1"), "execution_block")
        self.assertEqual(self._db_fetchone("SELECT status FROM order_events ORDER BY id DESC LIMIT 1"), "blocked")

    def test_live_mode_blocks_disabled_prelive_reconcile_before_position_reconcile(self) -> None:
        os.environ["DISABLE_LIVE_EXECUTION"] = "0"
        os.environ["EXECUTION_PRELIVE_RECONCILE"] = "0"
        os.environ["LIVE_TRADING_CONFIRM"] = DEFAULT_LIVE_CONFIRM_PHRASE
        now_ms = self._seed_runtime_live()
        self._set_mode("live", armed=1)
        self._seed_portfolio_order(ts_ms=now_ms)

        allow_live_gate = {
            "ok": True,
            "allowed": True,
            "allow_execution_pipeline": True,
            "real_trading_allowed": True,
            "mode": "live",
            "armed": 1,
            "reason": "mode_live_armed",
        }

        with patch.object(self.broker_apply_orders, "execution_gate_snapshot", return_value=allow_live_gate):
            with patch("engine.runtime.health.run_preflight", return_value={"ok": True}):
                with patch.object(
                    self.broker_apply_orders,
                    "pre_live_position_reconcile",
                    side_effect=AssertionError("position reconcile should not run after policy block"),
                ):
                    rc, out, _ = self._run_main(competition_policy={})

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "position_reconcile_policy")
        self.assertEqual(out["reason"], "prelive_reconcile_disabled_for_live")
        self.assertFalse(bool(out["prelive_reconcile_policy"]["ok"]))
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)

    def test_live_mode_routes_real_order_to_adapter_with_production_gate_providers(self) -> None:
        self._seed_live_alpaca_route()
        gates = importlib.import_module("engine.runtime.gates")
        adapter = Mock(return_value={"ok": True, "status": "adapter_called", "broker": "alpaca", "submitted_n": 1})

        with ExitStack() as stack:
            self._healthy_live_route_stack(stack, adapter=adapter)
            rc, out, _ = self._run_main(competition_policy={})

        self.assertEqual(rc, 0)
        self.assertIs(self.broker_router._execution_gate_snapshot, gates.execution_gate_snapshot)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["mode"], "live")
        self.assertTrue(bool(out["result"]["ok"]))
        self.assertEqual(out["result"]["status"], "adapter_called")
        adapter.assert_called_once()
        routed_orders = adapter.call_args.kwargs["override_orders"]
        self.assertEqual(len(routed_orders), 1)
        self.assertEqual(routed_orders[0]["symbol"], "AAPL")

    def test_live_mode_missing_router_gate_provider_blocks_before_adapter(self) -> None:
        self._seed_live_alpaca_route()
        adapter = Mock(side_effect=AssertionError("adapter must not run without router gate provider"))

        with ExitStack() as stack:
            self._healthy_live_route_stack(stack, adapter=adapter)
            stack.enter_context(patch.object(self.broker_router, "_execution_gate_snapshot", None))
            rc, out, _ = self._run_main(competition_policy={})

        self.assertEqual(rc, 0)
        self.assertEqual(out["mode"], "live")
        self.assertFalse(bool(out["result"]["ok"]))
        self.assertEqual(out["result"]["status"], "execution_blocked_gate_unavailable")
        adapter.assert_not_called()
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)

    def test_execution_policy_applied(self) -> None:
        now_ms = self._seed_runtime_live()
        self._set_mode("paper")
        stale_ts_ms = now_ms - 5_000_000
        self._seed_portfolio_order(ts_ms=stale_ts_ms)

        rc, out, _ = self._run_main(competition_policy={})

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["mode"], "paper")
        self.assertEqual(out["result"]["status"], "no_real_orders")

        decision_payload = json.loads(
            str(
                self._db_fetchone(
                    "SELECT payload_json FROM event_log WHERE event_type='order_decision' ORDER BY id DESC LIMIT 1"
                )
            )
        )
        self.assertEqual(int(decision_payload["raw_count"]), 1)
        self.assertEqual(int(decision_payload["shaped_count"]), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(
            self._db_fetchone("SELECT suppression_reason FROM trade_attribution_ledger ORDER BY id DESC LIMIT 1"),
            "ttl_expired",
        )

    def test_kill_switch_blocks_execution(self) -> None:
        now_ms = self._seed_runtime_live()
        self._set_mode("paper")
        self._seed_portfolio_order(ts_ms=now_ms)

        self.kill_switch.activate("global", "global", reason="unit_test", actor="test")
        self.lifecycle_state.set_state(self.lifecycle_state.LIVE, "kill_switch_db_still_active")

        rc, out, _ = self._run_main()

        self.assertEqual(rc, 0)
        self.assertEqual(out["status"], "blocked")
        self.assertEqual(out["layer"], "kill_switch")
        self.assertEqual(out["reason"], "kill_switch_db_global")
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM execution_orders") or 0), 0)
        self.assertEqual(
            int(self._db_fetchone("SELECT COUNT(*) FROM event_log WHERE event_type='order_decision'") or 0),
            0,
        )
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_commands") or 0), 0)
        self.assertEqual(int(self._db_fetchone("SELECT COUNT(*) FROM order_events") or 0), 1)
        self.assertEqual(self._db_fetchone("SELECT status FROM order_events ORDER BY id DESC LIMIT 1"), "blocked")
