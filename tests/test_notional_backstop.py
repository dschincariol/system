from __future__ import annotations

import importlib
import json
from typing import Any


class _Result:
    def __init__(self, row: tuple[Any, ...] | None = None) -> None:
        self._row = row

    def fetchone(self):
        return self._row


class _MetaConnection:
    def __init__(self) -> None:
        self.meta: dict[str, str] = {}

    def execute(self, sql: str, params: tuple[Any, ...] = ()):
        text = " ".join(str(sql).split()).lower()
        if text.startswith("pragma database_list"):
            return _Result((0, "main", ""))
        if "insert into portfolio_meta" in text and len(params) >= 2:
            self.meta[str(params[0])] = str(params[1])
        return _Result()


def _reload_backstop(monkeypatch, **env):
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
    import engine.risk.notional_backstop as notional_backstop

    return importlib.reload(notional_backstop)


def _reload_portfolio_stack(monkeypatch, **env):
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))

    import engine.risk.notional_backstop as notional_backstop
    import engine.risk.portfolio_risk_engine as portfolio_risk_engine
    import engine.strategy.portfolio_risk_gate as portfolio_risk_gate
    import engine.strategy.portfolio as portfolio

    notional_backstop = importlib.reload(notional_backstop)
    portfolio_risk_engine = importlib.reload(portfolio_risk_engine)
    portfolio_risk_gate = importlib.reload(portfolio_risk_gate)
    portfolio = importlib.reload(portfolio)
    return portfolio, portfolio_risk_engine, portfolio_risk_gate, notional_backstop


def _gross(rows: dict[str, dict[str, Any]]) -> float:
    return float(sum(abs(float((row or {}).get("weight", 0.0) or 0.0)) for row in rows.values()))


def _net_abs(rows: dict[str, dict[str, Any]]) -> float:
    return float(abs(sum(float((row or {}).get("weight", 0.0) or 0.0) for row in rows.values())))


def _patch_fast_risk_stage(monkeypatch, portfolio):
    monkeypatch.setattr(portfolio, "request_monte_carlo_refresh", lambda _desired: None)
    monkeypatch.setattr(portfolio, "_apply_temporal_dampener", lambda _con, desired, now_ms: desired)
    monkeypatch.setattr(portfolio, "_apply_capital_at_risk_gate", lambda desired: (desired, {}))
    monkeypatch.setattr(portfolio, "_apply_same_direction_exposure_netting", lambda _con, desired: (desired, {}))
    monkeypatch.setattr(portfolio, "_apply_total_portfolio_risk_limit", lambda _con, desired: (desired, {}))
    monkeypatch.setattr(portfolio, "_apply_flip_flop_penalty", lambda _con, desired, _state: (desired, {}))
    monkeypatch.setattr(portfolio, "_build_portfolio_correlation_diagnostics", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(portfolio, "_persist_portfolio_correlation_diagnostics", lambda *_args, **_kwargs: None)


def test_notional_backstop_clamps_gross_and_net(monkeypatch):
    backstop = _reload_backstop(
        monkeypatch,
        PORTFOLIO_NOTIONAL_BACKSTOP="1",
        PORTFOLIO_BACKSTOP_MAX_GROSS="1.00",
        PORTFOLIO_BACKSTOP_MAX_NET="0.60",
    )
    desired = {
        "AAPL": {"weight": 2.25, "reason": {}},
        "MSFT": {"weight": -0.75, "reason": {}},
    }

    out, meta = backstop.apply_notional_backstop(desired, is_live=True)

    assert _gross(out) <= 1.00 + 1e-9
    assert _net_abs(out) <= 0.60 + 1e-9
    assert meta["scaled"] is True
    assert meta["gross_pre"] == 3.0
    assert meta["net_pre"] == 1.5
    for row in out.values():
        reason = row["reason"]["portfolio_notional_backstop"]
        assert "gross" in reason

    net_out, net_meta = backstop.apply_notional_backstop({"AAPL": {"weight": 0.80, "reason": {}}}, is_live=True)
    assert _gross(net_out) <= 1.00 + 1e-9
    assert _net_abs(net_out) <= 0.60 + 1e-9
    assert net_meta["net_scaled"] is True
    assert "net" in net_out["AAPL"]["reason"]["portfolio_notional_backstop"]


def test_backstop_runs_when_both_flags_disabled(monkeypatch):
    portfolio, risk_engine, _risk_gate, _backstop = _reload_portfolio_stack(
        monkeypatch,
        DISABLE_LIVE_EXECUTION="0",
        PORTFOLIO_USE_RISK_ENGINE="0",
        PORTFOLIO_USE_RISK_GATE="0",
        PORTFOLIO_NOTIONAL_BACKSTOP="1",
        PORTFOLIO_BACKSTOP_MAX_GROSS="1.00",
        PORTFOLIO_BACKSTOP_MAX_NET="0.60",
    )
    captured_state: dict[str, str] = {}
    monkeypatch.setattr(risk_engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    _patch_fast_risk_stage(monkeypatch, portfolio)

    con = _MetaConnection()
    ctx = portfolio._RebalanceContext(
        con=con,
        now_ms=1_700_000_000_000,
        state={},
        desired={
            "AAPL": {"weight": 1.50, "reason": {}},
            "MSFT": {"weight": 1.50, "reason": {}},
        },
    )

    portfolio._apply_rebalance_risk_gates_stage(ctx)

    assert captured_state["portfolio_risk_block"] == "0"
    assert _gross(ctx.desired) <= 1.00 + 1e-9
    assert _net_abs(ctx.desired) <= 0.60 + 1e-9
    backstop_meta = json.loads(con.meta["last_notional_backstop"])
    assert backstop_meta["enabled"] is True
    assert backstop_meta["is_live"] is True
    assert backstop_meta["scaled"] is True


def test_backstop_is_final_after_flip_flop_transform(monkeypatch):
    portfolio, risk_engine, _risk_gate, _backstop = _reload_portfolio_stack(
        monkeypatch,
        DISABLE_LIVE_EXECUTION="0",
        PORTFOLIO_USE_RISK_ENGINE="0",
        PORTFOLIO_USE_RISK_GATE="0",
        PORTFOLIO_NOTIONAL_BACKSTOP="1",
        PORTFOLIO_BACKSTOP_MAX_GROSS="1.00",
        PORTFOLIO_BACKSTOP_MAX_NET="0.60",
    )
    captured_state: dict[str, str] = {}
    monkeypatch.setattr(risk_engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    _patch_fast_risk_stage(monkeypatch, portfolio)
    monkeypatch.setattr(
        portfolio,
        "_apply_flip_flop_penalty",
        lambda _con, _desired, _state: (
            {
                "AAPL": {"weight": 2.25, "reason": {}},
                "MSFT": {"weight": -0.75, "reason": {}},
            },
            {"unit_test_inflated": True},
        ),
    )

    con = _MetaConnection()
    ctx = portfolio._RebalanceContext(
        con=con,
        now_ms=1_700_000_000_000,
        state={},
        desired={"AAPL": {"weight": 0.10, "reason": {}}},
    )

    portfolio._apply_rebalance_risk_gates_stage(ctx)

    assert captured_state["portfolio_risk_block"] == "0"
    assert _gross(ctx.desired) <= 1.00 + 1e-9
    assert _net_abs(ctx.desired) <= 0.60 + 1e-9
    backstop_meta = json.loads(con.meta["last_notional_backstop"])
    assert backstop_meta["gross_pre"] == 3.0
    assert backstop_meta["net_pre"] == 1.5
    assert backstop_meta["scaled"] is True


def test_live_block_when_backstop_disabled(monkeypatch):
    _portfolio, risk_engine, risk_gate, _backstop = _reload_portfolio_stack(
        monkeypatch,
        ENGINE_MODE="live",
        EXECUTION_MODE="live",
        DISABLE_LIVE_EXECUTION="0",
        PORTFOLIO_USE_RISK_ENGINE="0",
        PORTFOLIO_USE_RISK_GATE="0",
        PORTFOLIO_NOTIONAL_BACKSTOP="0",
    )
    captured_state: dict[str, str] = {}
    monkeypatch.setattr(risk_engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))

    out, info = risk_engine.apply_portfolio_risk_engine(
        None,
        {"AAPL": {"weight": 2.0, "reason": {}}},
        {},
        now_ms=1_700_000_000_000,
    )

    assert out["AAPL"]["weight"] == 2.0
    assert info["enabled"] is False
    assert info["blocked"] is True
    assert info["status"] == "risk_engine_disabled_live"
    assert captured_state["portfolio_risk_block"] == "1"

    _gate_out, gate_info = risk_gate.apply_portfolio_risk_gate(
        None,
        {"AAPL": {"weight": 2.0, "reason": {}}},
        {},
        now_ms=1_700_000_000_000,
    )
    assert gate_info["blocked"] is True
    assert gate_info["status"] == "risk_gate_disabled_live"

    import engine.runtime.gates as gates

    gates = importlib.reload(gates)
    gate = gates.execution_gate_snapshot(
        get_execution_mode_fn=lambda: {"mode": "live", "armed": 1},
        system_state={"state": "LIVE"},
        kill_switches={"state": []},
        risk_state_getter=lambda key, default=None: captured_state.get(key, default),
    )

    assert gate["allowed"] is False
    assert gate["real_trading_allowed"] is False
    assert gate["reason"] == "portfolio_risk_block"
    assert gate["severity"] == "CRITICAL"


def test_live_block_when_backstop_unavailable(monkeypatch):
    portfolio, risk_engine, _risk_gate, _backstop = _reload_portfolio_stack(
        monkeypatch,
        DISABLE_LIVE_EXECUTION="0",
        PORTFOLIO_USE_RISK_ENGINE="0",
        PORTFOLIO_USE_RISK_GATE="0",
        PORTFOLIO_NOTIONAL_BACKSTOP="1",
        PORTFOLIO_BACKSTOP_MAX_GROSS="not-a-number",
        PORTFOLIO_BACKSTOP_MAX_NET="0.60",
    )
    captured_state: dict[str, str] = {}
    monkeypatch.setattr(risk_engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    monkeypatch.setattr(portfolio, "set_state", lambda key, value: captured_state.__setitem__(key, value))
    _patch_fast_risk_stage(monkeypatch, portfolio)

    con = _MetaConnection()
    ctx = portfolio._RebalanceContext(
        con=con,
        now_ms=1_700_000_000_000,
        state={},
        desired={"AAPL": {"weight": 2.0, "reason": {}}},
    )

    portfolio._apply_rebalance_risk_gates_stage(ctx)

    assert captured_state["portfolio_risk_block"] == "1"
    assert captured_state["portfolio_risk_status"] == "backstop_unavailable"
    state_info = json.loads(captured_state["portfolio_risk_info"])
    assert state_info["blocked"] is True
    assert state_info["status"] == "backstop_unavailable"
    assert state_info["error_type"] == "ValueError"
    assert portfolio._portfolio_execution_blocked_codes(
        [{"code": "PORTFOLIO_NOTIONAL_BACKSTOP_FAILED"}],
        is_live=True,
    ) == ["PORTFOLIO_NOTIONAL_BACKSTOP_FAILED"]
    assert portfolio._portfolio_execution_blocked_codes(
        [{"code": "PORTFOLIO_NOTIONAL_BACKSTOP_FAILED"}],
        is_live=False,
    ) == []


def test_nonlive_disable_does_not_block(monkeypatch):
    _portfolio, risk_engine, _risk_gate, _backstop = _reload_portfolio_stack(
        monkeypatch,
        DISABLE_LIVE_EXECUTION="1",
        PORTFOLIO_USE_RISK_ENGINE="0",
        PORTFOLIO_NOTIONAL_BACKSTOP="0",
    )
    captured_state: dict[str, str] = {}
    monkeypatch.setattr(risk_engine, "set_state", lambda key, value: captured_state.__setitem__(key, value))

    _out, info = risk_engine.apply_portfolio_risk_engine(
        None,
        {"AAPL": {"weight": 2.0, "reason": {}}},
        {},
        now_ms=1_700_000_000_000,
    )

    assert info["enabled"] is False
    assert info["blocked"] is False
    assert info["status"] == "disabled"
    assert captured_state["portfolio_risk_block"] == "0"
