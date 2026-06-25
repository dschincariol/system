from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path
from unittest.mock import Mock

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _configure_paper_sim_env(monkeypatch: pytest.MonkeyPatch, db_path: Path) -> None:
    env = {
        "DB_PATH": str(db_path),
        "TS_TESTING": "1",
        "TS_STORAGE_BACKEND": "sqlite",
        "ENGINE_MODE": "paper",
        "EXECUTION_MODE": "paper",
        "OPERATOR_MODE": "paper",
        "BROKER": "sim",
        "BROKER_NAME": "sim",
        "BROKER_FAILOVER": "sim",
        "LIVE_BROKER": "sim",
        "INTENDED_LIVE_BROKER": "sim",
        "DISABLE_LIVE_EXECUTION": "1",
        "KILL_SWITCH_GLOBAL": "0",
        "LIVE_TRADING_CONFIRM": "",
        "LIVE_TRADING_REQUIRE_CONFIRMATION": "1",
        "BROKER_START_CASH": "100000",
        "BROKER_LATENCY_SLEEP": "0",
        "BROKER_ROUTER_RETRY_ATTEMPTS": "1",
        "BROKER_ROUTER_RETRY_BASE_S": "0",
        "BROKER_ROUTER_RETRY_MAX_S": "0",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def _init_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    _configure_paper_sim_env(monkeypatch, tmp_path / "paper_mode_sim_fill.db")
    storage, gates, broker_router, broker_sim, execution_ledger, trade_attribution_ledger = _reload_modules(
        "engine.runtime.storage",
        "engine.runtime.gates",
        "engine.execution.broker_router",
        "engine.execution.broker_sim",
        "engine.execution.execution_ledger",
        "engine.execution.trade_attribution_ledger",
    )
    storage.init_db()
    broker_sim.init_broker_db()
    execution_ledger.init_execution_ledger()
    trade_attribution_ledger.ensure_trade_attribution_ready()

    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(gates, "_get_lifecycle_state", lambda: {"state": "LIVE", "detail": "test"})
    monkeypatch.setattr(gates, "_get_risk_state", lambda _key, default="": default)
    monkeypatch.setattr(
        gates,
        "get_execution_degraded_snapshot",
        lambda *_args, **_kwargs: {"active": False, "severity": "WARNING", "reason_codes": [], "sources": []},
    )
    monkeypatch.setattr(broker_router, "_kill_switch_snapshot", lambda: {"state": [], "loaded_ts_ms": now_ms, "max_age_ms": 60000})
    monkeypatch.setattr(broker_router, "_get_execution_mode", lambda: {"mode": "paper", "armed": 0, "source": "test"})
    monkeypatch.setattr(broker_router, "_execution_degraded_from_cache", lambda: {"active": False, "detail": {}})

    kill_switch = importlib.import_module("engine.execution.kill_switch")
    monkeypatch.setattr(kill_switch, "execution_allowed", lambda **_kwargs: (True, None, None))
    monkeypatch.setattr(broker_sim, "get_execution_liquidity_snapshot", lambda *_, **__: {})
    monkeypatch.setattr(broker_sim, "_earnings_proximity_decay", lambda *_, **__: 0.0)
    monkeypatch.setattr(broker_sim, "_get_factor_feature_asof", lambda *_, **__: 0.0)
    monkeypatch.setattr(broker_sim, "_prime_broker_order_state_after_commit", lambda *_, **__: None)

    return storage, gates, broker_router, execution_ledger, trade_attribution_ledger


def _seed_price(storage, *, symbol: str, price: float, ts_ms: int) -> None:
    con = storage.connect()
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              price REAL,
              px REAL,
              source TEXT,
              PRIMARY KEY(symbol, ts_ms)
            )
            """
        )
        con.execute(
            "INSERT OR REPLACE INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
            (int(ts_ms), str(symbol).upper(), float(price), float(price), "paper_sim_test"),
        )
        con.commit()
    finally:
        con.close()


def test_paper_mode_sim_fill_routes_and_writes_attribution(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    storage, gates, broker_router, execution_ledger, trade_attribution_ledger = _init_runtime(monkeypatch, tmp_path)
    now_ms = int(time.time() * 1000)
    _seed_price(storage, symbol="AAPL", price=100.0, ts_ms=now_ms)

    gate = gates.execution_gate_snapshot(
        get_execution_mode_fn=lambda: {"mode": "paper", "armed": 0, "source": "test"},
        kill_switches={"state": [], "loaded_ts_ms": now_ms, "max_age_ms": 60000},
    )
    assert gate["mode"] == "paper"
    assert gate["allow_simulation"] is True
    assert gate["real_trading_allowed"] is False
    assert gate["allowed"] is True

    assert broker_router.effective_broker_chain() == ["sim"]
    paper_contract = broker_router._paper_sim_broker_contract(["sim"])
    assert paper_contract["ok"] is True
    assert paper_contract["live_adapter_import_reachable"] is False

    live_adapter = Mock(side_effect=AssertionError("live adapter must not be import-reachable in paper sim"))
    monkeypatch.setattr(broker_router, "_resolve_alpaca_apply", live_adapter)
    monkeypatch.setattr(broker_router, "_resolve_ibkr_apply", live_adapter)

    orders = [
        {
            "source_order_id": 91001,
            "symbol": "AAPL",
            "to_side": "LONG",
            "qty": 1.0,
            "source_alert_id": 81001,
            "event_id": 71001,
            "horizon_s": 300,
            "model_id": "paper-sim-e2e",
            "model_version": "v1",
        }
    ]
    routed = broker_router.apply_new_portfolio_orders_router(
        dry_run=False,
        override_orders=[dict(orders[0])],
        override_order_id=91001,
        override_ts_ms=now_ms,
    )
    assert routed["ok"] is True, routed
    assert routed["broker"] == "sim"
    assert routed["status"] == "applied"
    assert int(routed.get("fills_written") or 0) >= 1
    live_adapter.assert_not_called()

    metrics = execution_ledger.compute_metrics_snapshot(limit_orders=5000)
    pnl = execution_ledger.compute_pnl_attribution_snapshot(lookback_orders=5000)
    attribution = trade_attribution_ledger.upsert_from_latest_pnl_attribution_snapshot()
    assert metrics["ok"] is True
    assert int(metrics.get("metrics_written") or 0) >= 1
    assert pnl["ok"] is True
    assert int(pnl.get("attribution_written") or 0) >= 1
    assert attribution["ok"] is True
    assert int(attribution.get("rows_upserted") or 0) >= 1

    con = storage.connect(readonly=True)
    try:
        broker_fills = int(con.execute("SELECT COUNT(*) FROM broker_fills WHERE symbol='AAPL'").fetchone()[0] or 0)
        execution_fills = int(con.execute("SELECT COUNT(*) FROM execution_fills WHERE symbol='AAPL'").fetchone()[0] or 0)
        trade_row = con.execute(
            """
            SELECT model_id, symbol, signal_json
            FROM trade_attribution_ledger
            WHERE source_alert_id=? AND model_id=? AND symbol=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (81001, "paper-sim-e2e", "AAPL"),
        ).fetchone()
    finally:
        con.close()

    assert broker_fills >= 1
    assert execution_fills >= 1
    assert trade_row is not None
    assert trade_row[0] == "paper-sim-e2e"
    assert trade_row[1] == "AAPL"
    signal_json = json.loads(trade_row[2])
    assert signal_json["pnl_attribution"]["extra"]["fill_count"] >= 1


def test_paper_mode_rejects_live_broker_profile_before_adapter(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_paper_sim_env(monkeypatch, tmp_path / "paper_rejects_live_broker.db")
    monkeypatch.setenv("BROKER", "alpaca")
    monkeypatch.setenv("BROKER_NAME", "alpaca")
    monkeypatch.setenv("BROKER_FAILOVER", "alpaca")
    (broker_router,) = _reload_modules("engine.execution.broker_router")
    live_adapter = Mock(side_effect=AssertionError("live adapter must not be resolved"))
    sim_adapter = Mock(side_effect=AssertionError("sim adapter must not run when paper broker contract is invalid"))
    monkeypatch.setattr(broker_router, "_resolve_alpaca_apply", live_adapter)
    monkeypatch.setattr(broker_router, "_resolve_ibkr_apply", live_adapter)
    monkeypatch.setattr(broker_router, "_resolve_sim_apply", sim_adapter)

    result = broker_router.apply_new_portfolio_orders_router(
        dry_run=False,
        override_orders=[{"symbol": "AAPL", "to_side": "LONG", "qty": 1.0}],
    )

    assert result["ok"] is False
    assert result["status"] == "paper_sim_broker_contract_invalid"
    assert result["paper_sim_broker_contract"]["live_adapter_import_reachable"] is True
    live_adapter.assert_not_called()
    sim_adapter.assert_not_called()


def test_global_kill_switch_still_blocks_paper_simulation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_paper_sim_env(monkeypatch, tmp_path / "paper_global_kill.db")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    (gates,) = _reload_modules("engine.runtime.gates")
    monkeypatch.setattr(gates, "_get_lifecycle_state", lambda: {"state": "LIVE", "detail": "test"})
    monkeypatch.setattr(
        gates,
        "get_execution_degraded_snapshot",
        lambda *_args, **_kwargs: {"active": False, "severity": "WARNING", "reason_codes": [], "sources": []},
    )

    gate = gates.execution_gate_snapshot(
        get_execution_mode_fn=lambda: {"mode": "paper", "armed": 0, "source": "test"},
        kill_switches={"state": []},
        risk_state_getter=lambda _key, default="": default,
    )

    assert gate["mode"] == "paper"
    assert gate["allowed"] is False
    assert gate["allow_simulation"] is False
    assert gate["real_trading_allowed"] is False
    assert gate["reason"] == "kill_switch_env_global"


def test_paper_mode_price_source_failure_allows_only_simulation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_paper_sim_env(monkeypatch, tmp_path / "paper_price_source_failed.db")
    (gates,) = _reload_modules("engine.runtime.gates")
    monkeypatch.setattr(
        gates,
        "_get_lifecycle_state",
        lambda: {"state": "DEGRADED", "detail": "critical_source_failed:prices"},
    )
    monkeypatch.setattr(
        gates,
        "get_execution_degraded_snapshot",
        lambda *_args, **_kwargs: {"active": False, "severity": "WARNING", "reason_codes": [], "sources": []},
    )

    gate = gates.execution_gate_snapshot(
        get_execution_mode_fn=lambda: {"mode": "paper", "armed": 0, "source": "test"},
        kill_switches={"state": []},
        risk_state_getter=lambda _key, default="": default,
    )

    assert gate["mode"] == "paper"
    assert gate["allowed"] is True
    assert gate["allow_simulation"] is True
    assert gate["real_trading_allowed"] is False

    monkeypatch.setattr(
        gates,
        "_get_lifecycle_state",
        lambda: {"state": "DEGRADED", "detail": "critical_source_failed:macro"},
    )
    blocked = gates.execution_gate_snapshot(
        get_execution_mode_fn=lambda: {"mode": "paper", "armed": 0, "source": "test"},
        kill_switches={"state": []},
        risk_state_getter=lambda _key, default="": default,
    )
    assert blocked["allowed"] is False
    assert blocked["allow_simulation"] is False


def test_live_arming_rejected_and_safe_mode_blocks_same_sim_flow(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _configure_paper_sim_env(monkeypatch, tmp_path / "paper_live_arming.db")
    storage, broker_router, cached_execution_mode = _reload_modules(
        "engine.runtime.storage",
        "engine.execution.broker_router",
        "engine.cache.wrappers.execution_mode",
    )
    storage.init_db()

    with pytest.raises(RuntimeError):
        cached_execution_mode.set_execution_mode("live", actor="test", reason="must_not_arm", armed=1)

    state = cached_execution_mode.read_execution_mode()
    assert not (state["mode"] == "live" and int(state.get("armed") or 0) == 1)

    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("OPERATOR_MODE", "safe")
    sim_adapter = Mock(side_effect=AssertionError("safe mode must block before the simulator"))
    monkeypatch.setattr(broker_router, "_resolve_sim_apply", sim_adapter)

    result = broker_router.apply_new_portfolio_orders_router(
        dry_run=False,
        override_orders=[{"symbol": "AAPL", "to_side": "LONG", "qty": 1.0}],
    )

    assert result["ok"] is False
    assert result["status"] == "execution_blocked"
    sim_adapter.assert_not_called()
