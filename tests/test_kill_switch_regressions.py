from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class ManagedConnection:
    def __init__(self) -> None:
        self._db_path = REPO_ROOT / "tmp" / "kill_switch_regressions_memory.sqlite"
        self._con = sqlite3.connect(":memory:")

    @property
    def in_transaction(self) -> bool:
        return bool(self._con.in_transaction)

    def begin_managed_write(self) -> None:
        self._con.execute("BEGIN IMMEDIATE")

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self._con.executescript(*args, **kwargs)

    def commit(self) -> None:
        self._con.commit()

    def rollback(self) -> None:
        self._con.rollback()

    def close(self) -> None:
        self._con.close()


class RulesManagedConnection(ManagedConnection):
    def close(self) -> None:
        pass

    def real_close(self) -> None:
        self._con.close()


class _DrawdownState:
    def __init__(self, *, ok: bool = True, drawdown: float = 0.0, reason_code: str = "ok") -> None:
        self.ok = bool(ok)
        self.drawdown = float(drawdown)
        self.reason_code = str(reason_code)

    def to_dict(self) -> dict:
        return {
            "ok": bool(self.ok),
            "drawdown": float(self.drawdown),
            "reason_code": str(self.reason_code),
        }


def _rules_side_effect_patches(kill_switch):
    return (
        patch.object(kill_switch, "append_event", return_value=None),
        patch("engine.execution.kill_switch_reactivity.notify_kill_switch_state_changed", return_value=None),
        patch("engine.cache.wrappers.kill_switch.prime_kill_switch", return_value=None),
        patch("engine.runtime.lifecycle_state.set_state", return_value=None),
    )


def _run_rules_engine(rules_engine, kill_switch, con, *, drawdown: float, drift: float = 0.0, winrate: float = 0.60):
    side_effects = _rules_side_effect_patches(kill_switch)
    with patch.object(rules_engine, "connect", return_value=con), patch.object(
        rules_engine, "_detect_cost_spike", return_value={"spike": False, "n": 0}
    ), patch.object(
        rules_engine, "evaluate_current_drawdown", return_value=_DrawdownState(drawdown=drawdown)
    ), patch.object(
        rules_engine, "get_max_drift_ratio", return_value=drift
    ), patch.object(
        rules_engine, "get_exec_winrate_global", return_value=winrate
    ), patch.object(
        rules_engine, "get_exec_stats_by_symbol", return_value={}
    ), side_effects[0], side_effects[1], side_effects[2], side_effects[3]:
        return rules_engine.evaluate_rules()


def test_execution_allowed_auto_expires_stale_cached_switch():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = ManagedConnection()
    now_ms = 1_710_000_000_000
    expired_until_ms = now_ms - 1_000

    try:
        kill_switch._ensure_schema(con)
        con.execute(
            """
            INSERT INTO kill_switch_state(scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                "global",
                "global",
                1,
                "expired_unit_test",
                "test",
                f'{{"until_ts_ms":{expired_until_ms}}}',
                now_ms - 10_000,
                now_ms - 10_000,
            ),
        )
        con.commit()

        cached_snapshot = {
            "state": [
                {
                    "scope": "global",
                    "key": "global",
                    "enabled": 1,
                    "reason": "expired_unit_test",
                    "actor": "test",
                    "meta": {"until_ts_ms": expired_until_ms},
                    "created_ts_ms": now_ms - 10_000,
                    "updated_ts_ms": now_ms - 10_000,
                }
            ]
        }

        with patch.object(kill_switch, "_now_ms", return_value=now_ms), patch.object(
            kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}
        ), patch.object(kill_switch, "REQUIRE_FRESH_DATA", False), patch.object(
            kill_switch, "REQUIRE_FRESH_JOBS", False
        ), patch.object(kill_switch, "_capital_risk_trigger", return_value=None), patch.object(
            kill_switch, "_model_risk_trigger", return_value=None
        ), patch("engine.strategy.capital_guard.trading_allowed", return_value=True), patch(
            "engine.cache.wrappers.kill_switch.read_kill_switch", return_value=cached_snapshot
        ), patch(
            "engine.cache.wrappers.kill_switch.prime_kill_switch", return_value=None
        ), patch(
            "engine.runtime.storage.init_db", return_value=None
        ), patch.object(
            kill_switch, "append_event", return_value=None
        ), patch(
            "engine.runtime.lifecycle_state.set_state", return_value=None
        ):
            allowed, reason, meta = kill_switch.execution_allowed(con=con)

        assert allowed is True
        assert reason == "ok"
        assert meta == {"scope": None, "key": None}
        row = con.execute(
            "SELECT enabled, reason, actor, meta_json FROM kill_switch_state WHERE scope=? AND key=?",
            ("global", "global"),
        ).fetchone()
        assert row is not None
        assert int(row[0] or 0) == 0
        assert str(row[1] or "") == "auto_expire"
    finally:
        con.close()


def test_rules_created_halt_auto_clears_when_enabled():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    rules_engine = importlib.reload(importlib.import_module("engine.strategy.rules_engine"))
    con = RulesManagedConnection()
    env = {
        "RULES_AUTO_RESUME": "1",
        "EXECUTION_MODE": "paper",
        "ENGINE_MODE": "paper",
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
    }

    try:
        with patch.dict(os.environ, env, clear=False):
            activated = _run_rules_engine(rules_engine, kill_switch, con, drawdown=0.25)
            cleared = _run_rules_engine(rules_engine, kill_switch, con, drawdown=0.01)

        assert any(a["key"] == "rules:drawdown" and int(a["enabled"]) == 1 for a in activated["actions"])
        assert any(a["key"] == "rules:drawdown" and int(a["enabled"]) == 0 for a in cleared["actions"])
        row = con.execute(
            "SELECT enabled, reason, actor, meta_json FROM kill_switch_state WHERE scope=? AND key=?",
            ("global", "rules:drawdown"),
        ).fetchone()
        assert row is not None
        assert int(row[0] or 0) == 0
        assert row[1] == "rules_drawdown_clear"
        assert row[2] == "rules_engine"
        meta = json.loads(row[3])
        assert meta["trigger"] == "drawdown"
        assert meta["rules_auto_resume_opt_in"] is True
    finally:
        con.real_close()


def test_rules_auto_resume_does_not_clear_operator_created_halt():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    rules_engine = importlib.reload(importlib.import_module("engine.strategy.rules_engine"))
    con = RulesManagedConnection()
    env = {
        "RULES_AUTO_RESUME": "1",
        "EXECUTION_MODE": "paper",
        "ENGINE_MODE": "paper",
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
    }
    side_effects = _rules_side_effect_patches(kill_switch)

    try:
        kill_switch._ensure_schema(con)
        con.commit()
        with side_effects[0], side_effects[1], side_effects[2], side_effects[3]:
            kill_switch.activate(
                "global",
                "global",
                reason="operator_emergency_stop",
                actor="operator",
                meta={"source": "operator_api"},
                con=con,
            )
            kill_switch.activate(
                "global",
                "rules:drawdown",
                reason="operator_manual_drawdown_hold",
                actor="operator",
                meta={"source": "operator_api", "trigger": "drawdown"},
                con=con,
            )

        with patch.dict(os.environ, env, clear=False):
            _run_rules_engine(rules_engine, kill_switch, con, drawdown=0.01)

        rows = con.execute(
            """
            SELECT key, enabled, actor, reason
            FROM kill_switch_state
            WHERE scope='global' AND key IN ('global', 'rules:drawdown')
            ORDER BY key
            """
        ).fetchall()
        assert {row[0]: (int(row[1] or 0), row[2], row[3]) for row in rows} == {
            "global": (1, "operator", "operator_emergency_stop"),
            "rules:drawdown": (1, "operator", "operator_manual_drawdown_hold"),
        }
    finally:
        con.real_close()


def test_rules_auto_resume_live_default_disabled():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    rules_engine = importlib.reload(importlib.import_module("engine.strategy.rules_engine"))
    con = RulesManagedConnection()
    env = {
        "EXECUTION_MODE": "live",
        "ENGINE_MODE": "live",
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
    }
    side_effects = _rules_side_effect_patches(kill_switch)

    try:
        kill_switch._ensure_schema(con)
        con.commit()
        with side_effects[0], side_effects[1], side_effects[2], side_effects[3]:
            kill_switch.activate_owned(
                "global",
                "rules:drawdown",
                reason="rules_drawdown dd=0.250",
                owner_actor="rules_engine",
                trigger="drawdown",
                meta={"dd": 0.25},
                con=con,
            )

        with patch.dict(os.environ, env, clear=False):
            os.environ.pop("RULES_AUTO_RESUME", None)
            result = _run_rules_engine(rules_engine, kill_switch, con, drawdown=0.01)

        row = con.execute(
            "SELECT enabled, reason, actor, meta_json FROM kill_switch_state WHERE scope=? AND key=?",
            ("global", "rules:drawdown"),
        ).fetchone()
        assert result["rules_auto_resume_enabled"] is False
        assert result["rules_auto_resume_live_mode"] is True
        assert row is not None
        assert int(row[0] or 0) == 1
        assert row[1] == "rules_drawdown dd=0.250"
        assert row[2] == "rules_engine"
    finally:
        con.real_close()


def test_rules_mixed_global_triggers_do_not_overwrite_each_other():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    rules_engine = importlib.reload(importlib.import_module("engine.strategy.rules_engine"))
    con = RulesManagedConnection()
    env = {
        "RULES_AUTO_RESUME": "1",
        "EXECUTION_MODE": "paper",
        "ENGINE_MODE": "paper",
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
    }

    try:
        with patch.dict(os.environ, env, clear=False):
            _run_rules_engine(rules_engine, kill_switch, con, drawdown=0.25, drift=4.0)
            _run_rules_engine(rules_engine, kill_switch, con, drawdown=0.01, drift=4.0)

        rows = con.execute(
            """
            SELECT key, enabled, actor, meta_json
            FROM kill_switch_state
            WHERE scope='global' AND key IN ('rules:drawdown', 'rules:drift')
            ORDER BY key
            """
        ).fetchall()
        state = {row[0]: (int(row[1] or 0), row[2], json.loads(row[3])) for row in rows}
        assert state["rules:drawdown"][0] == 0
        assert state["rules:drawdown"][1] == "rules_engine"
        assert state["rules:drawdown"][2]["trigger"] == "drawdown"
        assert state["rules:drift"][0] == 1
        assert state["rules:drift"][1] == "rules_engine"
        assert state["rules:drift"][2]["trigger"] == "drift"

        with patch.object(kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}), patch.object(
            kill_switch, "REQUIRE_FRESH_DATA", False
        ), patch.object(kill_switch, "REQUIRE_FRESH_JOBS", False), patch.object(
            kill_switch, "_capital_risk_trigger", return_value=None
        ), patch.object(kill_switch, "_model_risk_trigger", return_value=None), patch(
            "engine.strategy.capital_guard.trading_allowed", return_value=True
        ), patch(
            "engine.cache.wrappers.kill_switch.read_kill_switch", return_value={"state": []}
        ):
            allowed, reason, meta = kill_switch.execution_allowed(con=con)
        assert allowed is False
        assert reason == "kill_switch_db_global"
        assert meta["key"] == "rules:drift"
    finally:
        con.real_close()


def test_operator_clear_manual_halt_endpoint_requires_confirmation_and_refuses_rules_owned_rows():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    api_operator_handlers = importlib.reload(importlib.import_module("engine.api.api_operator_handlers"))
    con = RulesManagedConnection()
    side_effects = _rules_side_effect_patches(kill_switch)

    try:
        kill_switch._ensure_schema(con)
        con.commit()
        with side_effects[0], side_effects[1], side_effects[2], side_effects[3], patch.object(
            kill_switch, "connect", return_value=con
        ):
            kill_switch.activate(
                "global",
                "global",
                reason="operator_emergency_stop",
                actor="operator",
                meta={"source": "operator_api"},
                con=con,
            )
            denied = api_operator_handlers.api_post_operator_clear_manual_halt(None, {"confirm": "CLEAR_MANUAL_HALT"}, {})
            assert denied["ok"] is False
            assert denied["error"] == "confirmation_required"

            cleared = api_operator_handlers.api_post_operator_clear_manual_halt(
                None,
                {
                    "confirm": "CLEAR_MANUAL_HALT",
                    "consequence_ack": True,
                    "actor": "ops",
                    "source": "unit_test",
                    "reason": "incident_resolved",
                },
                {},
            )

        assert cleared["ok"] is True
        row = con.execute(
            "SELECT enabled, reason, actor, meta_json FROM kill_switch_state WHERE scope=? AND key=?",
            ("global", "global"),
        ).fetchone()
        assert int(row[0] or 0) == 0
        assert row[1] == "incident_resolved"
        assert row[2] == "ops"
        assert json.loads(row[3])["manual_clear"] is True

        side_effects = _rules_side_effect_patches(kill_switch)
        with side_effects[0], side_effects[1], side_effects[2], side_effects[3], patch.object(
            kill_switch, "connect", return_value=con
        ):
            kill_switch.activate_owned(
                "global",
                "rules:drift",
                reason="rules_drift drift=4.00",
                owner_actor="rules_engine",
                trigger="drift",
                meta={"drift": 4.0},
                con=con,
            )
            refused = api_operator_handlers.api_post_operator_clear_manual_halt(
                None,
                {
                    "scope": "global",
                    "key": "rules:drift",
                    "confirm": "CLEAR_MANUAL_HALT",
                    "consequence_ack": True,
                    "actor": "ops",
                    "source": "unit_test",
                    "reason": "try_wrong_workflow",
                },
                {},
            )
        assert refused["ok"] is False
        assert refused["error"] == "manual_clear_refused_rules_owned_halt"
    finally:
        con.real_close()


def test_execution_allowed_blocks_live_mode_when_disable_live_execution_truthy():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    previous = {
        "DISABLE_LIVE_EXECUTION": os.environ.get("DISABLE_LIVE_EXECUTION"),
        "EXECUTION_MODE": os.environ.get("EXECUTION_MODE"),
        "ENGINE_MODE": os.environ.get("ENGINE_MODE"),
    }
    try:
        os.environ["DISABLE_LIVE_EXECUTION"] = "yes"
        os.environ["EXECUTION_MODE"] = "live"
        os.environ["ENGINE_MODE"] = "live"
        with patch.object(kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}):
            allowed, reason, meta = kill_switch.execution_allowed(con=None)

        assert allowed is False
        assert reason == "disable_live_execution_env"
        assert meta["scope"] == "global"
        assert meta["key"] == "DISABLE_LIVE_EXECUTION"
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_execution_allowed_blocks_provider_unavailable_cache_snapshot():
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = ManagedConnection()
    env = {
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
        "KILL_SWITCH_SYMBOLS": "",
        "KILL_SWITCH_REGIMES": "",
        "KILL_SWITCH_MODELS": "",
        "DISABLE_LIVE_EXECUTION": "0",
        "EXECUTION_MODE": "live",
        "ENGINE_MODE": "live",
    }
    provider_unavailable = {
        "state": [
            {
                "scope": "global",
                "key": "provider_unavailable",
                "enabled": 1,
                "reason": "kill_switch_provider_unavailable",
                "actor": "unit",
                "meta": {"error": "db down"},
                "created_ts_ms": 0,
                "updated_ts_ms": 123,
            }
        ],
        "source": "engine.cache.wrappers.kill_switch:provider_unavailable",
        "loaded_ts_ms": 123,
        "max_age_ms": 30_000,
        "cache_fresh": True,
    }

    try:
        kill_switch._ensure_schema(con)
        con.commit()
        with patch.dict(os.environ, env, clear=False), patch.object(
            kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}
        ), patch.object(kill_switch, "REQUIRE_FRESH_DATA", False), patch.object(
            kill_switch, "REQUIRE_FRESH_JOBS", False
        ), patch.object(kill_switch, "_capital_risk_trigger", return_value=None), patch.object(
            kill_switch, "_model_risk_trigger", return_value=None
        ), patch("engine.strategy.capital_guard.trading_allowed", return_value=True), patch(
            "engine.cache.wrappers.kill_switch.read_kill_switch", return_value=provider_unavailable
        ):
            allowed, reason, meta = kill_switch.execution_allowed(con=con)

        assert allowed is False
        assert reason == "kill_switch_provider_unavailable"
        assert meta["scope"] == "global"
        assert meta["key"] == "provider_unavailable"
    finally:
        con.close()


def test_execution_gate_blocks_explicit_stale_kill_switch_cache_snapshot():
    env = {
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
        "DISABLE_LIVE_EXECUTION": "0",
        "EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE": "0",
    }
    with patch.dict(os.environ, env, clear=False):
        gates = importlib.reload(importlib.import_module("engine.runtime.gates"))
        with patch.object(gates, "_now_ms", return_value=1_000_000), patch.object(
            gates, "_get_lifecycle_state", return_value={"state": "LIVE"}
        ), patch.object(gates, "live_trading_preflight", return_value={"ok": True, "reason": "ok"}) as preflight:
            blocked = gates.execution_gate_snapshot(
                get_execution_mode_fn=lambda: {"mode": "live", "armed": 1},
                kill_switches={
                    "state": [],
                    "loaded_ts_ms": 900_000,
                    "max_age_ms": 30_000,
                    "source": "engine.cache.wrappers.kill_switch:db",
                    "cache_fresh": False,
                    "cache_status": "fresh",
                },
                risk_state_getter=lambda _key, default=None: default,
            )

    assert blocked["allowed"] is False
    assert blocked["real_trading_allowed"] is False
    assert blocked["reason"] == "kill_switch_cache_stale"
    assert blocked["kill_switch_cache"]["cache_fresh"] is False
    preflight.assert_not_called()


def _patch_activation_failure_side_effects(kill_switch):
    return (
        patch.object(kill_switch, "append_event", return_value=1),
        patch.object(kill_switch, "log_failure", return_value={}),
        patch("engine.runtime.event_log.flush_event_log_buffer", return_value={"flushed": 1}),
        patch("engine.runtime.risk_state.set_state", return_value=None),
        patch("engine.runtime.lifecycle_state.set_state", return_value={"state": "DEGRADED"}),
    )


def test_capital_breach_activation_write_failure_blocks_and_persists_operator_visible_marker(tmp_path):
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = ManagedConnection()
    breach = {
        "reason": "capital_daily_drawdown_breach pct=0.2500",
        "meta": {"trigger": "daily_drawdown", "daily_drawdown_pct": 0.25, "threshold": 0.05},
    }

    env = {
        "KILL_SWITCH_FAILURE_DIR": str(tmp_path),
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
        "KILL_SWITCH_SYMBOLS": "",
        "KILL_SWITCH_REGIMES": "",
        "KILL_SWITCH_MODELS": "",
        "DISABLE_LIVE_EXECUTION": "0",
        "EXECUTION_MODE": "live",
        "ENGINE_MODE": "live",
    }

    try:
        kill_switch._ensure_schema(con)
        con.commit()
        side_effects = _patch_activation_failure_side_effects(kill_switch)
        with patch.dict(os.environ, env, clear=False), patch.object(
            kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}
        ), patch.object(kill_switch, "REQUIRE_FRESH_DATA", False), patch.object(
            kill_switch, "REQUIRE_FRESH_JOBS", False
        ), patch.object(kill_switch, "_capital_risk_trigger", return_value=breach), patch.object(
            kill_switch, "_model_risk_trigger", return_value=None
        ), patch("engine.strategy.capital_guard.trading_allowed", return_value=True), patch.object(
            kill_switch, "activate", side_effect=RuntimeError("unit test write failed")
        ), side_effects[0], side_effects[1], side_effects[2], side_effects[3], side_effects[4]:
            allowed, reason, meta = kill_switch.execution_allowed(con=con)

        assert allowed is False
        assert reason == "capital_kill_switch_activation_failed"
        assert meta["activation_failure"]["active"] is True
        assert meta["activation_failure"]["scope"] == "global"
        assert meta["activation_failure"]["key"] == "global"

        state_path = tmp_path / "kill_switch_activation_failure_state.json"
        evidence_path = tmp_path / "kill_switch_activation_failures.jsonl"
        state_payload = json.loads(state_path.read_text(encoding="utf-8"))
        evidence_lines = evidence_path.read_text(encoding="utf-8").strip().splitlines()
        assert state_payload["active"] is True
        assert state_payload["trigger_kind"] == "capital"
        assert state_payload["reason_code"] == "kill_switch_activation_write_failed"
        assert state_payload["meta"]["daily_drawdown_pct"] == 0.25
        assert evidence_lines

        with patch.dict(os.environ, env, clear=False):
            snapshot = kill_switch.snapshot(con=con)
            assert snapshot["activation_failure"]["active"] is True

            health = importlib.reload(importlib.import_module("engine.runtime.health"))
            health_snapshot = health.get_kill_switch_snapshot_readonly(con=con)
            assert health_snapshot["activation_failure"]["active"] is True

            gates = importlib.reload(importlib.import_module("engine.runtime.gates"))
            degraded = gates.get_execution_degraded_snapshot()
            assert degraded["active"] is True
            assert degraded["severity"] == "CRITICAL"
            assert degraded["reason"] == "kill_switch_activation_failed"
            assert "kill_switch_activation_write_failed" in degraded["reason_codes"]

            restarted = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
            restarted_allowed, restarted_reason, restarted_meta = restarted.execution_allowed(con=None)
        assert restarted_allowed is False
        assert restarted_reason == "kill_switch_activation_failed"
        assert restarted_meta["activation_failure"]["trigger_kind"] == "capital"
    finally:
        con.close()


def test_model_breach_activation_write_failure_blocks_and_persists_marker(tmp_path):
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = ManagedConnection()
    breach = {
        "reason": "model_consecutive_losses_breach model_id=model_a losses=4",
        "meta": {"trigger": "model_consecutive_losses", "model_id": "model_a", "consecutive_losses": 4},
    }
    env = {
        "KILL_SWITCH_FAILURE_DIR": str(tmp_path),
        "KILL_SWITCH_GLOBAL": "0",
        "TRADING_KILL_SWITCH": "0",
        "KILL_SWITCH": "0",
        "KILL_SWITCH_SYMBOLS": "",
        "KILL_SWITCH_REGIMES": "",
        "KILL_SWITCH_MODELS": "",
        "DISABLE_LIVE_EXECUTION": "0",
        "EXECUTION_MODE": "live",
        "ENGINE_MODE": "live",
    }

    try:
        kill_switch._ensure_schema(con)
        con.commit()
        side_effects = _patch_activation_failure_side_effects(kill_switch)
        with patch.dict(os.environ, env, clear=False), patch.object(
            kill_switch, "_get_lifecycle_state", return_value={"state": "LIVE"}
        ), patch.object(kill_switch, "REQUIRE_FRESH_DATA", False), patch.object(
            kill_switch, "REQUIRE_FRESH_JOBS", False
        ), patch.object(kill_switch, "_capital_risk_trigger", return_value=None), patch.object(
            kill_switch, "_model_risk_trigger", return_value=breach
        ), patch("engine.strategy.capital_guard.trading_allowed", return_value=True), patch.object(
            kill_switch, "activate", side_effect=RuntimeError("unit test model write failed")
        ), side_effects[0], side_effects[1], side_effects[2], side_effects[3], side_effects[4]:
            allowed, reason, meta = kill_switch.execution_allowed(con=con, model_id="model_a")

        assert allowed is False
        assert reason == "model_kill_switch_activation_failed"
        assert meta["activation_failure"]["scope"] == "model"
        assert meta["activation_failure"]["key"] == "model_a"

        state_payload = json.loads((tmp_path / "kill_switch_activation_failure_state.json").read_text(encoding="utf-8"))
        assert state_payload["active"] is True
        assert state_payload["trigger_kind"] == "model"
        assert state_payload["meta"]["model_id"] == "model_a"
    finally:
        con.close()


def test_activate_in_uncommitted_transaction_does_not_clear_activation_failure_marker(tmp_path):
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    con = ManagedConnection()
    env = {"KILL_SWITCH_FAILURE_DIR": str(tmp_path)}
    marker = {
        "active": True,
        "status": "UNRESOLVED",
        "scope": "global",
        "key": "global",
        "reason": "capital_daily_drawdown_breach pct=0.2500",
        "trigger_kind": "capital",
        "reason_code": "kill_switch_activation_write_failed",
    }
    state_path = tmp_path / "kill_switch_activation_failure_state.json"
    state_path.write_text(json.dumps(marker), encoding="utf-8")

    try:
        kill_switch._ensure_schema(con)
        con.commit()

        with patch.dict(os.environ, env, clear=False):
            con.begin_managed_write()
            kill_switch.activate(
                "global",
                "global",
                reason="retry_activation_inside_outer_txn",
                actor="test",
                con=con,
            )
            assert kill_switch.activation_failure_snapshot()["active"] is True
            con.rollback()
            assert kill_switch.activation_failure_snapshot()["active"] is True
    finally:
        con.close()
