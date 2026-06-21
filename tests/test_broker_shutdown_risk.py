from __future__ import annotations

import importlib
import json
import sqlite3
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _setup_runtime(monkeypatch, tmp_path: Path):
    db_path = tmp_path / "broker_shutdown_risk.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TRADING_DATA", str(tmp_path))
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("BROKER", "alpaca")
    monkeypatch.setenv("BROKER_NAME", "alpaca")
    monkeypatch.setenv("LIVE_BROKER", "alpaca")
    monkeypatch.setenv("BROKER_FAILOVER", "alpaca")
    monkeypatch.delenv("BROKER_SHUTDOWN_POLICY", raising=False)
    _reload_modules(
        "engine.runtime.storage",
        "engine.execution.order_command_boundary",
    )
    (broker_shutdown_risk,) = _reload_modules("engine.execution.broker_shutdown_risk")
    return db_path, broker_shutdown_risk


def _command_row(db_path: Path, command_id: str) -> dict:
    con = sqlite3.connect(str(db_path))
    try:
        row = con.execute(
            """
            SELECT status, command_json, result_json
            FROM order_commands
            WHERE command_id=?
            """,
            (str(command_id),),
        ).fetchone()
    finally:
        con.close()
    assert row is not None
    return {
        "status": str(row[0] or ""),
        "command": json.loads(str(row[1] or "{}")),
        "result": json.loads(str(row[2] or "{}")) if row[2] else {},
    }


def test_live_shutdown_requires_explicit_policy_and_does_not_touch_broker(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("broker must not be touched")),
    )

    result = mod.handle_broker_shutdown_risk(
        policy=None,
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-policy-required",
        timeout_s=1.0,
        reason="signal:15",
    )

    assert result["ok"] is False
    assert result["status"] == "broker_shutdown_policy_required_for_live"
    row = _command_row(db_path, "cmd-policy-required")
    assert row["status"] == "broker_shutdown_policy_required_for_live"
    assert row["command"]["reason"] == "signal:15"


def test_sigterm_cancel_only_policy_cancels_and_audits(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    calls = {"cancel": 0}
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: {"ok": True, "broker": "alpaca", "orders": [{"id": "ord-1"}], "open_order_count": 1},
    )

    def _cancel(**kwargs):
        calls["cancel"] += 1
        assert kwargs["command_id"] == "cmd-sigterm-cancel"
        return {"ok": True, "broker": "alpaca", "status": "cancel_open_orders_complete", "cancelled_n": 1, "failed_n": 0}

    monkeypatch.setattr(mod, "cancel_open_orders_for_broker", _cancel)

    result = mod.handle_broker_shutdown_risk(
        policy="cancel_only",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-sigterm-cancel",
        timeout_s=2.0,
        reason="signal:15",
    )

    assert result["ok"] is True
    assert result["policy"] == "cancel_only"
    assert calls["cancel"] == 1
    row = _command_row(db_path, "cmd-sigterm-cancel")
    assert row["status"] == "broker_shutdown_policy_applied"
    assert row["result"]["cancel"]["cancelled_n"] == 1
    assert row["command"]["reason"] == "signal:15"


def test_shutdown_timeout_records_fail_closed_status(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)

    def _slow_list(**_kwargs):
        time.sleep(0.12)
        return {"ok": True, "broker": "alpaca", "orders": [{"id": "ord-1"}], "open_order_count": 1}

    monkeypatch.setattr(mod, "list_open_orders_for_broker", _slow_list)
    monkeypatch.setattr(
        mod,
        "cancel_open_orders_for_broker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("cancel should not run after timeout")),
    )

    result = mod.handle_broker_shutdown_risk(
        policy="cancel_only",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-timeout",
        timeout_s=0.1,
        reason="signal:15",
    )

    assert result["ok"] is False
    assert result["status"] == "broker_shutdown_timeout"
    assert _command_row(db_path, "cmd-timeout")["status"] == "broker_shutdown_timeout"


def test_broker_cancel_failure_is_audited(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: {"ok": True, "broker": "alpaca", "orders": [{"id": "ord-1"}], "open_order_count": 1},
    )
    monkeypatch.setattr(
        mod,
        "cancel_open_orders_for_broker",
        lambda **_kwargs: {"ok": False, "broker": "alpaca", "status": "cancel_open_orders_incomplete", "failed_n": 1},
    )

    result = mod.handle_broker_shutdown_risk(
        policy="cancel_only",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-cancel-failure",
        timeout_s=2.0,
    )

    assert result["ok"] is False
    assert result["status"] == "broker_shutdown_cancel_failed"
    row = _command_row(db_path, "cmd-cancel-failure")
    assert row["status"] == "broker_shutdown_cancel_failed"
    assert row["result"]["cancel"]["failed_n"] == 1


def test_duplicate_command_returns_previous_result_without_reissuing_broker_calls(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    calls = {"cancel": 0}
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: {"ok": True, "broker": "alpaca", "orders": [{"id": "ord-1"}], "open_order_count": 1},
    )

    def _cancel(**_kwargs):
        calls["cancel"] += 1
        return {"ok": True, "broker": "alpaca", "status": "cancel_open_orders_complete", "cancelled_n": 1, "failed_n": 0}

    monkeypatch.setattr(mod, "cancel_open_orders_for_broker", _cancel)
    first = mod.handle_broker_shutdown_risk(
        policy="cancel_only",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-duplicate",
        timeout_s=2.0,
    )
    monkeypatch.setattr(
        mod,
        "cancel_open_orders_for_broker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("duplicate must not cancel again")),
    )
    second = mod.handle_broker_shutdown_risk(
        policy="cancel_only",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-duplicate",
        timeout_s=2.0,
    )

    assert first["ok"] is True
    assert second["ok"] is True
    assert second["duplicate"] is True
    assert calls["cancel"] == 1
    assert _command_row(db_path, "cmd-duplicate")["status"] == "broker_shutdown_policy_applied"


def test_flatten_policy_requires_configured_limits_before_adapter(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: {"ok": True, "broker": "alpaca", "orders": [], "open_order_count": 0},
    )
    monkeypatch.setattr(
        mod,
        "cancel_open_orders_for_broker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("flatten_positions must not cancel orders")),
    )
    monkeypatch.setattr(
        mod,
        "_run_reconcile_gate",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("reconcile should wait for configured limits")),
    )

    result = mod.handle_broker_shutdown_risk(
        policy="flatten_positions",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-flatten-limits",
        timeout_s=2.0,
    )

    assert result["ok"] is False
    assert result["status"] == "broker_shutdown_flatten_failed"
    assert result["flatten"]["status"] == "flatten_limits_required"
    assert _command_row(db_path, "cmd-flatten-limits")["status"] == "broker_shutdown_flatten_failed"


def test_flatten_policy_requires_successful_position_reconcile(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    monkeypatch.setenv("BROKER_SHUTDOWN_FLATTEN_MAX_ABS_QTY_PER_SYMBOL", "100")
    monkeypatch.setenv("BROKER_SHUTDOWN_FLATTEN_MAX_TOTAL_ABS_QTY", "200")
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: {"ok": True, "broker": "alpaca", "orders": [], "open_order_count": 0},
    )
    monkeypatch.setattr(
        mod,
        "cancel_open_orders_for_broker",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("flatten_positions must not cancel orders")),
    )
    monkeypatch.setattr(
        mod,
        "_run_reconcile_gate",
        lambda **_kwargs: {"ok": False, "status": "needs_reconcile", "reason": "needs_reconcile"},
    )

    result = mod.handle_broker_shutdown_risk(
        policy="flatten_positions",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-flatten-reconcile",
        timeout_s=2.0,
    )

    assert result["ok"] is False
    assert result["flatten"]["status"] == "flatten_reconcile_block"
    assert _command_row(db_path, "cmd-flatten-reconcile")["status"] == "broker_shutdown_flatten_failed"


def test_flatten_policy_rejects_skipped_position_reconcile(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    monkeypatch.setenv("BROKER_SHUTDOWN_FLATTEN_MAX_ABS_QTY_PER_SYMBOL", "100")
    monkeypatch.setenv("BROKER_SHUTDOWN_FLATTEN_MAX_TOTAL_ABS_QTY", "200")
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: {"ok": True, "broker": "alpaca", "orders": [], "open_order_count": 0},
    )
    import engine.execution.position_reconcile as position_reconcile

    monkeypatch.setattr(
        position_reconcile,
        "pre_live_position_reconcile",
        lambda _broker: {"ok": True, "status": "skipped_disabled", "broker": "alpaca"},
    )
    monkeypatch.setattr(
        mod,
        "_adapter_module",
        lambda _broker: (_ for _ in ()).throw(AssertionError("adapter must not run after skipped reconcile")),
    )

    result = mod.handle_broker_shutdown_risk(
        policy="flatten_positions",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-flatten-skipped-reconcile",
        timeout_s=2.0,
    )

    assert result["ok"] is False
    assert result["flatten"]["status"] == "flatten_reconcile_block"
    assert result["flatten"]["reason"] == "prelive_reconcile_required_for_flatten"
    assert _command_row(db_path, "cmd-flatten-skipped-reconcile")["status"] == "broker_shutdown_flatten_failed"


def test_cancel_and_flatten_cancels_before_flatten(monkeypatch, tmp_path):
    db_path, mod = _setup_runtime(monkeypatch, tmp_path)
    monkeypatch.setenv("BROKER_SHUTDOWN_FLATTEN_MAX_ABS_QTY_PER_SYMBOL", "100")
    monkeypatch.setenv("BROKER_SHUTDOWN_FLATTEN_MAX_TOTAL_ABS_QTY", "200")
    calls = []
    monkeypatch.setattr(
        mod,
        "list_open_orders_for_broker",
        lambda **_kwargs: {"ok": True, "broker": "alpaca", "orders": [{"id": "ord-1"}], "open_order_count": 1},
    )

    def _cancel(**_kwargs):
        calls.append("cancel")
        return {"ok": True, "broker": "alpaca", "status": "cancel_open_orders_complete", "cancelled_n": 1, "failed_n": 0}

    def _flatten(**_kwargs):
        calls.append("flatten")
        return {"ok": True, "broker": "alpaca", "status": "flatten_positions_submitted", "submitted_n": 1, "failed_n": 0}

    monkeypatch.setattr(mod, "cancel_open_orders_for_broker", _cancel)
    monkeypatch.setattr(mod, "flatten_positions_for_broker", _flatten)

    result = mod.handle_broker_shutdown_risk(
        policy="cancel_and_flatten",
        broker="alpaca",
        engine_mode="live",
        command_id="cmd-cancel-and-flatten",
        timeout_s=2.0,
    )

    assert result["ok"] is True
    assert calls == ["cancel", "flatten"]
    assert _command_row(db_path, "cmd-cancel-and-flatten")["status"] == "broker_shutdown_policy_applied"


def test_runtime_shutdown_runs_broker_risk_with_signal_reason(monkeypatch, tmp_path):
    db_path, _mod = _setup_runtime(monkeypatch, tmp_path)
    del db_path
    (shutdown,) = _reload_modules("engine.runtime.shutdown")
    captured = {}

    def _capture_broker_risk(*, shutdown_reason: str):
        captured["reason"] = shutdown_reason
        return {"ok": True, "status": "unit_test"}

    monkeypatch.setattr(
        shutdown,
        "_run_broker_shutdown_risk",
        _capture_broker_risk,
    )

    shutdown.runtime_shutdown(shutdown_reason="signal:15")

    assert captured["reason"] == "signal:15"


def test_runtime_shutdown_broker_risk_runs_once_per_process(monkeypatch, tmp_path):
    db_path, broker_mod = _setup_runtime(monkeypatch, tmp_path)
    del db_path
    (shutdown,) = _reload_modules("engine.runtime.shutdown")
    calls = []

    def _handle(**kwargs):
        calls.append(dict(kwargs))
        return {"ok": True, "status": "unit_test", "command_id": kwargs.get("command_id")}

    monkeypatch.setenv("BROKER_SHUTDOWN_POLICY", "cancel_only")
    monkeypatch.setattr(broker_mod, "handle_broker_shutdown_risk", _handle)

    first = shutdown._run_broker_shutdown_risk(shutdown_reason="signal:15")
    second = shutdown._run_broker_shutdown_risk(shutdown_reason="main_finally")

    assert first["ok"] is True
    assert second["ok"] is True
    assert second["duplicate_runtime_shutdown"] is True
    assert len(calls) == 1
    assert calls[0]["reason"] == "signal:15"
    assert str(calls[0]["command_id"]).startswith("broker-risk-runtime-")
