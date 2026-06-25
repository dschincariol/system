from __future__ import annotations

import importlib
import sqlite3

import pytest

from engine.execution.execution_costs import estimate_cost_bps
from engine.strategy import statistical_gates
from engine.strategy.gated_backtest import run_gated_backtest


def _reload_portfolio_backtest(monkeypatch):
    monkeypatch.setenv("PORTFOLIO_BACKTEST_USE_EXEC_COSTS", "1")
    monkeypatch.setenv("PORTFOLIO_BACKTEST_FUTURES_SLIPPAGE_TICKS", "1")
    monkeypatch.setenv("PORTFOLIO_BACKTEST_FUTURES_ROLL_TICKS", "2")
    monkeypatch.setenv("PORTFOLIO_BACKTEST_FUTURES_ROLL_WINDOW_MS", "1000")
    monkeypatch.setenv("BROKER_FEE_BPS", "0")
    monkeypatch.setenv("BROKER_SPREAD_BPS", "0")

    import engine.strategy.portfolio_backtest as portfolio_backtest

    return importlib.reload(portfolio_backtest)


def _futures_con() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(symbol TEXT NOT NULL, ts_ms INTEGER NOT NULL, price REAL, px REAL)")
    con.execute(
        "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?, ?, ?, ?)",
        ("ES.C.0", 1_000, 5_000.0, None),
    )
    con.execute(
        """
        CREATE TABLE futures_roll_calendar(
            root TEXT NOT NULL,
            roll_ts_ms INTEGER NOT NULL,
            from_contract TEXT,
            to_contract TEXT,
            gap_ratio REAL,
            method TEXT,
            ingested_ts_ms INTEGER,
            PRIMARY KEY(root, roll_ts_ms)
        )
        """
    )
    con.execute(
        """
        INSERT INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        ("ES", 1_000, "ESM26", "ESU26", 1.0, "oi_volume", 1_000),
    )
    return con


def test_futures_backtest_costs_use_point_value_ticks_and_roll_cost(monkeypatch) -> None:
    portfolio_backtest = _reload_portfolio_backtest(monkeypatch)
    con = _futures_con()
    try:
        trade_cost = portfolio_backtest._estimate_weight_delta_trade_cost(
            con,
            "ES.c.0",
            delta_weight=2.6,
            equity=100_000.0,
            ts_ms=1_000,
        )
    finally:
        con.close()

    assert trade_cost["status"] == "estimated_futures"
    assert trade_cost["contracts"] == 1
    assert trade_cost["notional"] == pytest.approx(250_000.0)
    assert trade_cost["tick_slippage_cost"] == pytest.approx(12.5)
    assert trade_cost["roll_cost"] == pytest.approx(50.0)
    assert trade_cost["roll_cost_bps"] == pytest.approx(2.0)
    assert trade_cost["cost_bps"]["slippage_bps"] == pytest.approx(0.5)
    assert portfolio_backtest.futures_point_value_pnl(
        contracts=2,
        multiplier=50.0,
        entry_px=5_000.0,
        exit_px=5_005.0,
        side=1,
    ) == pytest.approx(500.0)


def test_futures_tick_cost_bps_differs_from_equity_fractional_spread() -> None:
    equity = estimate_cost_bps(
        px=5_000.0,
        bid=4_999.8,
        ask=5_000.0,
        side=1,
        fees_bps=0.0,
        slippage_bps=0.0,
    )
    futures = estimate_cost_bps(
        px=5_000.0,
        bid=4_999.8,
        ask=5_000.0,
        side=1,
        fees_bps=0.0,
        slippage_bps=0.0,
        contract_multiplier=50.0,
        tick_size=0.25,
        tick_value=12.5,
    )

    assert equity["spread_bps"] == pytest.approx(0.2)
    assert futures["spread_bps"] == pytest.approx(0.5)


def test_futures_challenger_gate_consumes_cost_adjusted_returns(monkeypatch) -> None:
    _reload_portfolio_backtest(monkeypatch)
    con = _futures_con()
    predictions = [2.6, -2.6, 2.6, -2.6]
    realized_returns = [0.006, -0.007, 0.008, -0.009]
    sample_times = [1_000, 2_000, 3_000, 4_000]
    cost_config = {"enabled": True, "asset_class": "FUTURES", "equity": 100_000.0}

    try:
        futures_costed = run_gated_backtest(
            predictions,
            realized_returns,
            sample_times_ms=sample_times,
            symbols=["ES.c.0"] * len(predictions),
            cost_config=cost_config,
            con=con,
        )
        zero_cost = run_gated_backtest(
            predictions,
            realized_returns,
            sample_times_ms=sample_times,
            symbols=["ES.c.0"] * len(predictions),
            cost_config={**cost_config, "enabled": False},
            con=con,
        )
    finally:
        con.close()

    assert futures_costed["costs"]["total_futures_cost_return"] > 0.0
    assert futures_costed["returns"] == futures_costed["cost_adjusted_returns"]
    assert futures_costed["returns"] != zero_cost["returns"]
    assert sum(futures_costed["returns"]) < sum(zero_cost["returns"])

    captured_dsr_calls: list[dict[str, float | int]] = []
    original_dsr = statistical_gates.deflated_sharpe_ratio

    def _recording_dsr(sharpe, n_trials, n_obs, skew, kurt):
        captured_dsr_calls.append(
            {
                "sharpe": float(sharpe),
                "n_trials": int(n_trials),
                "n_obs": int(n_obs),
            }
        )
        return original_dsr(sharpe, n_trials, n_obs, skew, kurt)

    monkeypatch.setattr(statistical_gates, "deflated_sharpe_ratio", _recording_dsr)
    _gate_passed, diagnostics = statistical_gates.passes_promotion_gate(
        futures_costed["returns"],
        n_competing_trials=4,
        config={
            "enabled": True,
            "min_observations": len(futures_costed["returns"]),
            "min_t_stat": -999.0,
            "min_deflated_sharpe": -999.0,
            "fdr_alpha": 1.0,
        },
    )

    assert captured_dsr_calls
    assert diagnostics["applied"] is True
    assert diagnostics["status"] == "evaluated"
    assert captured_dsr_calls[0]["n_obs"] == len(futures_costed["returns"])
    assert diagnostics["mean_return"] == pytest.approx(
        sum(futures_costed["returns"]) / len(futures_costed["returns"])
    )
    assert diagnostics["mean_return"] != pytest.approx(
        sum(zero_cost["returns"]) / len(zero_cost["returns"])
    )
