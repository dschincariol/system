from __future__ import annotations

import importlib
import io
import json
import socket
import sys
import time
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

from engine.runtime.live_trading_preflight import DEFAULT_LIVE_CONFIRM_PHRASE


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _reload_runtime_modules() -> SimpleNamespace:
    (
        event_log,
        storage,
        state_cache,
        risk_state,
        runtime_meta,
        lifecycle_state,
        execution_mode,
        kill_switch,
        position_reconcile,
        order_idempotency,
        portfolio_execution_intents,
        broker_router,
        broker_apply_orders,
    ) = _reload_modules(
        "engine.runtime.event_log",
        "engine.runtime.storage",
        "engine.runtime.state_cache",
        "engine.runtime.risk_state",
        "engine.runtime.runtime_meta",
        "engine.runtime.lifecycle_state",
        "engine.execution.execution_mode",
        "engine.execution.kill_switch",
        "engine.execution.position_reconcile",
        "engine.execution.order_idempotency",
        "engine.strategy.portfolio_execution_intents",
        "engine.execution.broker_router",
        "engine.execution.broker_apply_orders",
    )
    return SimpleNamespace(
        event_log=event_log,
        storage=storage,
        state_cache=state_cache,
        risk_state=risk_state,
        runtime_meta=runtime_meta,
        lifecycle_state=lifecycle_state,
        execution_mode=execution_mode,
        kill_switch=kill_switch,
        position_reconcile=position_reconcile,
        order_idempotency=order_idempotency,
        portfolio_execution_intents=portfolio_execution_intents,
        broker_router=broker_router,
        broker_apply_orders=broker_apply_orders,
    )


class _FakeBrokerClient:
    def __init__(self, modules: SimpleNamespace) -> None:
        self.modules = modules
        self.calls: list[dict[str, Any]] = []

    def apply(
        self,
        *,
        dry_run: bool,
        override_orders: list[dict] | None = None,
        override_order_id: int | None = None,
        override_ts_ms: int | None = None,
    ) -> dict[str, Any]:
        orders = [dict(order or {}) for order in list(override_orders or [])]
        self.calls.append(
            {
                "dry_run": bool(dry_run),
                "override_orders": orders,
                "override_order_id": override_order_id,
                "override_ts_ms": override_ts_ms,
            }
        )
        if dry_run:
            return {"ok": True, "status": "fake_dry_run", "broker": "alpaca", "submitted_n": 0}

        con = self.modules.storage.connect()
        submitted: list[dict[str, Any]] = []
        try:
            for index, order in enumerate(orders, start=1):
                claim = self.modules.order_idempotency.claim_order_submission(
                    con=con,
                    broker="alpaca",
                    portfolio_orders_id=override_order_id,
                    portfolio_ts_ms=override_ts_ms,
                    order=order,
                )
                broker_order_id = f"fake-alpaca-{index}"
                if not bool(claim.get("duplicate")):
                    self.modules.order_idempotency.mark_order_submission_submitted(
                        con=con,
                        order_uid=str(claim["order_uid"]),
                        client_order_id=str(claim["client_order_id"]),
                        broker_order_id=broker_order_id,
                        submit_ts_ms=int(time.time() * 1000),
                    )
                submitted.append(
                    {
                        "symbol": str(order.get("symbol") or "").upper(),
                        "client_order_id": str(claim.get("client_order_id") or ""),
                        "broker_order_id": broker_order_id,
                        "duplicate": bool(claim.get("duplicate")),
                    }
                )
        finally:
            con.close()

        return {
            "ok": True,
            "status": "fake_submitted",
            "broker": "alpaca",
            "submitted_n": len(submitted),
            "submitted": submitted,
        }


def _configure_live_env(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "real_capital_safety_e2e.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_PG_SCHEMA_PER_DB_PATH", "1")
    monkeypatch.setenv("TS_REDIS_KEY_PREFIX", f"real_capital_safety_e2e_{tmp_path.name}")
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("ALLOW_TRAINING", "0")
    monkeypatch.setenv("DASHBOARD_HOST", "127.0.0.1")
    monkeypatch.setenv("DASHBOARD_PORT", str(_free_tcp_port()))
    monkeypatch.setenv("EVENT_LOG_BUFFER_ENABLED", "0")
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    monkeypatch.setenv("EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE", "0")
    monkeypatch.setenv("EXEC_ADAPTIVE_SLICING", "0")
    monkeypatch.setenv("BROKER_ROUTER_RETRY_ATTEMPTS", "1")
    monkeypatch.setenv("BROKER_ROUTER_RETRY_BASE_S", "0")
    monkeypatch.setenv("BROKER_ROUTER_RETRY_MAX_S", "0")
    monkeypatch.setenv("BROKER_LATENCY_SLEEP", "0")

    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "0")
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "1")
    monkeypatch.setenv("LIVE_TRADING_CONFIRM", DEFAULT_LIVE_CONFIRM_PHRASE)
    monkeypatch.setenv("LIVE_TRADING_REQUIRE_CONFIRMATION", "1")
    monkeypatch.setenv("DASHBOARD_API_TOKEN", "live-token-1234567890")
    monkeypatch.setenv("LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN", "1")

    monkeypatch.setenv("LIVE_BROKER", "alpaca")
    monkeypatch.setenv("BROKER", "alpaca")
    monkeypatch.setenv("BROKER_NAME", "alpaca")
    monkeypatch.setenv("BROKER_FAILOVER", "alpaca")
    monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")
    monkeypatch.setenv("ALPACA_KEY_ID", "fake-alpaca-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "fake-alpaca-secret")

    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("TRADING_KILL_SWITCH", "0")
    monkeypatch.setenv("KILL_SWITCH", "0")
    monkeypatch.setenv("KILL_SWITCH_SYMBOLS", "")
    monkeypatch.setenv("KILL_SWITCH_REGIMES", "")
    monkeypatch.setenv("KILL_SWITCH_MODELS", "")
    monkeypatch.setenv("KILL_SWITCH_REQUIRE_FRESH_DATA", "1")
    monkeypatch.setenv("KILL_SWITCH_REQUIRE_FRESH_JOBS", "1")
    monkeypatch.setenv("KILL_SWITCH_REQUIRED_JOBS", "ingestion_runtime,process_events")
    monkeypatch.setenv("CAPITAL_AWARE_KILL_SWITCH", "0")
    monkeypatch.setenv("MODEL_AWARE_KILL_SWITCH", "1")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_VAR_95_BLOCK", "0.50")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_VAR_99_BLOCK", "0.60")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_CVAR_95_BLOCK", "0.70")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_CVAR_99_BLOCK", "0.80")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK", "0.30")
    monkeypatch.setenv("PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK", "0.40")
    monkeypatch.setenv("PORTFOLIO_RISK_VOL_HARD_BLOCK", "1.00")
    monkeypatch.setenv("KILL_SWITCH_MODEL_MAX_DRAWDOWN", "0.50")
    monkeypatch.setenv("KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES", "100")
    monkeypatch.setenv("CAPITAL_STOP_DRAWDOWN", "0.25")
    monkeypatch.setenv("DRAWDOWN_MIN_HISTORY_POINTS", "5")

    monkeypatch.setenv("POSITION_RECONCILE_EVIDENCE_MAX_AGE_S", "3600")
    monkeypatch.setenv("EXECUTION_RECONCILE_REQUIRE_BASELINE", "1")
    monkeypatch.setenv("EXECUTION_RECONCILE_ALLOW_BOOTSTRAP", "0")

    now = time.time()
    evidence_path = tmp_path / "latest_backup_restore_evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generated_at_ts": now,
                "status": "pass",
                "base_backup": {"status": "pass", "verified_at_ts": now},
                "wal_archive": {"status": "pass", "verified_at_ts": now},
                "restore_drill": {
                    "status": "pass",
                    "verified_at_ts": now,
                    "time_to_recover_s": 60,
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(evidence_path))
    monkeypatch.setenv("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RPO_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "3600")
    monkeypatch.setenv("BACKUP_EVIDENCE_RTO_S", "300")
    monkeypatch.delenv("BACKUP_EVIDENCE_REQUIRE_SIGNATURE", raising=False)
    monkeypatch.delenv("BACKUP_EVIDENCE_SIGNATURE_REQUIRED", raising=False)


def _invalidate_execution_mode_cache() -> None:
    from engine.cache import keys, store

    store.invalidate(keys.execution_mode())


def _seed_runtime(modules: SimpleNamespace, *, now_ms: int | None = None) -> int:
    now = int(now_ms or int(time.time() * 1000))
    modules.storage.init_db()
    modules.runtime_meta.meta_set("first_price_ts_ms", str(now - 1_000))
    modules.lifecycle_state.set_state(modules.lifecycle_state.LIVE, "real_capital_safety_e2e")

    con = modules.storage.connect()
    try:
        modules.order_idempotency.init_order_idempotency(con)
        con.execute(
            "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
            (now, "AAPL", 100.0, 100.0, "test"),
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
                now,
                now,
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
                f"event-key-{now}",
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
                now,
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
                now,
                "normal",
                "up",
                "normal",
            ),
        )
        for job_name in ("ingestion_runtime", "process_events"):
            con.execute(
                "INSERT INTO job_heartbeats(job_name, owner, pid, ts_ms, extra_json) VALUES (?,?,?,?,?)",
                (job_name, "test", 123, now, "{}"),
            )
        for offset in range(5):
            con.execute(
                "INSERT OR REPLACE INTO equity_history(ts_ms, equity) VALUES (?,?)",
                (now - (4 - offset) * 1_000, 100_000.0),
            )
        con.commit()
    finally:
        con.close()

    for namespace in ("risk_state", "risk_state_row", "api_read", "portfolio_snapshot"):
        modules.state_cache.cache_invalidate_namespace(namespace)
    for key, value in (
        ("trading_state", "enabled"),
        ("stop_reason", ""),
        ("portfolio_risk_block", "0"),
        ("portfolio_risk_info", ""),
        ("portfolio_risk_status", ""),
        ("execution_pause", "0"),
    ):
        modules.risk_state.set_state(key, value)
    return now


def _seed_position_reconcile_evidence(modules: SimpleNamespace, *, ts_ms: int) -> None:
    con = modules.storage.connect()
    try:
        modules.position_reconcile._ensure_schema(con)
        modules.position_reconcile._save_baseline(con, "alpaca", int(ts_ms), {})
        modules.position_reconcile._append_reconcile_audit(
            con,
            ts_ms=int(ts_ms),
            broker="alpaca",
            ok=True,
            status="ok",
            mismatched_n=0,
            max_abs_qty_diff=0.0,
            total_abs_qty_diff=0.0,
            detail={"status": "ok", "mismatched": []},
        )
        con.commit()
    finally:
        con.close()


def _arm_live_through_audited_db(modules: SimpleNamespace, monkeypatch) -> None:
    _invalidate_execution_mode_cache()
    modules.execution_mode.set_execution_mode("live", actor="e2e_operator", reason="real_capital_safety_e2e")
    modules.execution_mode.set_execution_armed(1, actor="e2e_operator", reason="real_capital_safety_e2e")
    _invalidate_execution_mode_cache()
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "0")


def _seed_model_intent(
    modules: SimpleNamespace,
    *,
    ts_ms: int,
    model_id: str = "baseline",
    model_name: str = "baseline",
    model_kind: str = "production",
    source_alert_id: int | None = None,
) -> None:
    explain = {
        "model_id": str(model_id),
        "model_name": str(model_name),
        "model_kind": str(model_kind),
        "model_version": "v1",
        "regime": "global",
        "horizon_s": 3600,
        "signal": {"expected_z": 1.0, "confidence": 0.7},
    }
    con = modules.storage.connect()
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
                "AAPL",
                "rebalance",
                "FLAT",
                "LONG",
                0.0,
                0.10,
                0.10,
                (int(source_alert_id) if source_alert_id is not None else None),
                json.dumps(explain, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()


def _db_fetchone(modules: SimpleNamespace, sql: str, params: tuple[Any, ...] = ()) -> Any:
    con = modules.storage.connect_ro_direct()
    try:
        row = con.execute(sql, tuple(params)).fetchone()
        return None if row is None else row[0]
    finally:
        con.close()


def _db_fetchjson(modules: SimpleNamespace, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    raw = _db_fetchone(modules, sql, params)
    return dict(json.loads(str(raw or "{}")))


def _setup_live_case(monkeypatch, tmp_path: Path) -> SimpleNamespace:
    _configure_live_env(monkeypatch, tmp_path)
    modules = _reload_runtime_modules()
    now_ms = _seed_runtime(modules)
    _seed_position_reconcile_evidence(modules, ts_ms=now_ms)
    _arm_live_through_audited_db(modules, monkeypatch)
    modules = _reload_runtime_modules()
    return modules


def _passthrough_execution_policy(*args: Any, **kwargs: Any) -> list[dict]:
    if "intents" in kwargs:
        return [dict(order or {}) for order in list(kwargs.get("intents") or [])]
    first = args[0] if args else []
    return [dict(order or {}) for order in list(first or [])]


def _allowed_competition_policy(**_: Any) -> dict[str, Any]:
    return {
        "allowed": True,
        "blocked": False,
        "champion_model_name": "baseline",
        "allocation_fraction": 1.0,
        "model_weight": 1.0,
        "capital_multiplier": 1.0,
        "group_budget_fraction": 1.0,
        "model_budget_fraction": 1.0,
        "risk_limit_multiplier": 1.0,
        "regime": "global",
    }


def _run_apply_job(
    modules: SimpleNamespace,
    fake_broker: _FakeBrokerClient,
    *,
    patch_position_fetch: bool = True,
) -> tuple[int, dict[str, Any], str]:
    with ExitStack() as stack:
        stack.enter_context(patch.object(modules.broker_apply_orders, "evaluate_rules", return_value=None))
        stack.enter_context(patch.object(modules.broker_apply_orders, "persist_execution_advisories", return_value=None))
        stack.enter_context(patch.object(modules.broker_apply_orders, "apply_execution_policy", side_effect=_passthrough_execution_policy))
        stack.enter_context(
            patch.object(
                modules.portfolio_execution_intents,
                "get_competition_policy_for_intent",
                side_effect=_allowed_competition_policy,
            )
        )
        stack.enter_context(
            patch.object(
                modules.broker_apply_orders,
                "get_competition_policy_for_intent",
                side_effect=_allowed_competition_policy,
            )
        )
        stack.enter_context(patch.object(modules.portfolio_execution_intents, "DEFAULT_DECISION_ENGINE", None))
        if patch_position_fetch:
            stack.enter_context(patch.object(modules.position_reconcile, "_broker_positions", return_value=(True, "ok", [])))
        stack.enter_context(
            patch.object(
                modules.broker_apply_orders,
                "refresh_broker_connection_health",
                return_value={"ok": True, "state": "connected"},
            )
        )
        stack.enter_context(
            patch.object(
                modules.broker_apply_orders,
                "refresh_execution_quality_supervisor",
                return_value={"ok": True, "state": "ok"},
            )
        )
        stack.enter_context(patch.object(modules.broker_router, "_alpaca_apply", fake_broker.apply))
        stack.enter_context(patch.object(modules.broker_router.time, "sleep", return_value=None))

        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = modules.broker_apply_orders.main()

    stdout = buf.getvalue()
    json_lines = [line for line in stdout.splitlines() if line.strip().startswith("{")]
    assert json_lines, f"expected broker_apply_orders JSON output, got: {stdout!r}"
    return int(rc), json.loads(json_lines[-1]), stdout


def test_valid_audited_live_arming_routes_once_and_writes_idempotency_and_audit(monkeypatch, tmp_path):
    modules = _setup_live_case(monkeypatch, tmp_path)
    _seed_model_intent(modules, ts_ms=int(time.time() * 1000))
    fake_broker = _FakeBrokerClient(modules)

    rc, out, _stdout = _run_apply_job(modules, fake_broker)

    assert rc == 0
    assert out["status"] == "ok"
    assert out["mode"] == "live"
    assert out["result"]["status"] == "fake_submitted"
    assert out["result"]["submitted_n"] == 1
    assert len(fake_broker.calls) == 1
    assert fake_broker.calls[0]["dry_run"] is False
    assert fake_broker.calls[0]["override_orders"][0]["symbol"] == "AAPL"

    assert _db_fetchone(
        modules,
        "SELECT COUNT(*) FROM execution_mode_audit WHERE new_mode='live' AND COALESCE(new_armed,0)=1 AND row_hash IS NOT NULL",
    ) == 1
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM order_commands") == 1
    assert _db_fetchone(modules, "SELECT mode FROM order_commands ORDER BY ts_ms DESC LIMIT 1") == "live"
    assert _db_fetchone(modules, "SELECT status FROM order_commands ORDER BY ts_ms DESC LIMIT 1") == "submitted"
    assert _db_fetchone(modules, "SELECT real_order_count FROM order_commands ORDER BY ts_ms DESC LIMIT 1") == 1
    assert _db_fetchone(modules, "SELECT status FROM order_events ORDER BY id DESC LIMIT 1") == "submitted"
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM event_log WHERE event_type='order_decision'") == 1
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM event_log WHERE event_type='order_submit_result'") == 1
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM position_reconcile_audit WHERE broker='alpaca' AND ok=1") >= 2
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM execution_order_idempotency WHERE broker='alpaca' AND status='submitted'") == 1

    command_payload = _db_fetchjson(
        modules,
        "SELECT command_json FROM order_commands ORDER BY ts_ms DESC LIMIT 1",
    )
    assert command_payload["real_count"] == 1
    assert command_payload["blocked_count"] == 0


def test_non_production_model_intent_is_blocked_before_router_or_broker_adapter(monkeypatch, tmp_path):
    modules = _setup_live_case(monkeypatch, tmp_path)
    _seed_model_intent(
        modules,
        ts_ms=int(time.time() * 1000),
        model_id="openai-advisor",
        model_name="OpenAI Advisor",
        model_kind="llm",
    )
    fake_broker = _FakeBrokerClient(modules)

    rc, out, _stdout = _run_apply_job(modules, fake_broker)

    assert rc == 0
    assert out["status"] == "blocked"
    assert out["layer"] == "production_model_guard"
    assert out["reason"] == "all_orders_blocked_non_production_models"
    assert out["blocked_orders"][0]["reason"] == "non_production_model_blocked"
    assert fake_broker.calls == []
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM order_commands") == 0
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM execution_order_idempotency") == 0
    assert _db_fetchone(modules, "SELECT status FROM order_events ORDER BY id DESC LIMIT 1") == "blocked"
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM event_log WHERE event_type='order_decision'") == 1
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM event_log WHERE event_type='execution_block'") == 1


def test_portfolio_risk_gate_blocks_before_router_or_broker_adapter(monkeypatch, tmp_path):
    modules = _setup_live_case(monkeypatch, tmp_path)
    _seed_model_intent(modules, ts_ms=int(time.time() * 1000))
    modules.risk_state.set_state("portfolio_risk_block", "1")
    modules.risk_state.set_state("portfolio_risk_info", "unit-test-var-breach")
    fake_broker = _FakeBrokerClient(modules)

    rc, out, _stdout = _run_apply_job(modules, fake_broker)

    assert rc == 0
    assert out["status"] == "blocked"
    assert out["layer"] == "portfolio_risk_engine"
    assert out["reason"] == "portfolio_risk_block"
    assert "unit-test-var-breach" in out["portfolio_risk_info"]
    assert fake_broker.calls == []
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM order_commands") == 0
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM execution_order_idempotency") == 0
    assert _db_fetchone(modules, "SELECT status FROM order_events ORDER BY id DESC LIMIT 1") == "blocked"


def test_disabled_prelive_reconcile_gate_blocks_before_position_fetch_or_adapter(monkeypatch, tmp_path):
    modules = _setup_live_case(monkeypatch, tmp_path)
    _seed_model_intent(modules, ts_ms=int(time.time() * 1000))
    monkeypatch.setenv("EXECUTION_PRELIVE_RECONCILE", "0")
    fake_broker = _FakeBrokerClient(modules)

    with patch.object(
        modules.position_reconcile,
        "_broker_positions",
        side_effect=AssertionError("position fetch should not run when policy gate is disabled"),
    ):
        rc, out, _stdout = _run_apply_job(modules, fake_broker, patch_position_fetch=False)

    assert rc == 0
    assert out["status"] == "blocked"
    assert out["layer"] == "execution_gate"
    assert out["reason"] == "prelive_reconcile_disabled_for_live"
    assert out["gate"]["real_trading_allowed"] is False
    assert out["gate"]["live_trading_preflight"]["prelive_reconcile"]["ok"] is False
    assert fake_broker.calls == []
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM execution_order_idempotency") == 0
    assert _db_fetchone(modules, "SELECT status FROM order_events ORDER BY id DESC LIMIT 1") == "blocked"


def test_global_kill_switch_overrides_valid_arming_risk_and_adapter_path(monkeypatch, tmp_path):
    modules = _setup_live_case(monkeypatch, tmp_path)
    _seed_model_intent(modules, ts_ms=int(time.time() * 1000))
    modules.kill_switch.activate("global", "global", reason="unit_test_kill", actor="e2e_test")
    modules.lifecycle_state.set_state(modules.lifecycle_state.LIVE, "kill_switch_db_still_active")
    fake_broker = _FakeBrokerClient(modules)

    rc, out, _stdout = _run_apply_job(modules, fake_broker)

    assert rc == 0
    assert out["status"] == "blocked"
    assert out["layer"] == "kill_switch"
    assert out["reason"] == "kill_switch_db_global"
    assert fake_broker.calls == []
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM order_commands") == 0
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM execution_order_idempotency") == 0
    assert _db_fetchone(modules, "SELECT COUNT(*) FROM kill_switch_audit WHERE enabled=1 AND reason='unit_test_kill'") == 1
    assert _db_fetchone(modules, "SELECT status FROM order_events ORDER BY id DESC LIMIT 1") == "blocked"
