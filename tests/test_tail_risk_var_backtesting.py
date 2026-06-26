from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path

from engine.api import api_system
from engine.risk import monte_carlo_risk_engine as mc
from engine.risk import var_backtesting


def _connect(path: Path):
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    return con


def test_student_t_simulation_has_heavier_left_tail_than_gaussian(monkeypatch) -> None:
    monkeypatch.setattr(mc, "MC_SIMULATIONS", 5000)
    monkeypatch.setattr(mc, "MC_HORIZON", 1)
    monkeypatch.setattr(mc, "MC_STUDENT_T_ESTIMATE_DOF", False)

    gaussian_pnl, _, _, gaussian_meta = mc._simulate(
        [1.0],
        [0.02],
        [0.0],
        [[1.0]],
        method="gaussian",
        seed=17,
        return_metadata=True,
    )
    student_pnl, _, _, student_meta = mc._simulate(
        [1.0],
        [0.02],
        [0.0],
        [[1.0]],
        method="student_t",
        student_t_dof=3.5,
        seed=17,
        return_metadata=True,
    )

    assert student_meta["method"] == "student_t"
    assert gaussian_meta["method"] == "gaussian"
    assert mc._pct(student_pnl, 0.01) < mc._pct(gaussian_pnl, 0.01)


def test_historical_simulation_insufficient_data_falls_back_with_diagnostics(monkeypatch) -> None:
    monkeypatch.setattr(mc, "MC_SIMULATIONS", 20)
    monkeypatch.setattr(mc, "MC_HORIZON", 1)
    monkeypatch.setattr(mc, "MC_HISTORICAL_MIN_OBS", 30)

    _pnl, _dd, _fan, meta = mc._simulate(
        [1.0],
        [0.01],
        [0.0],
        [[1.0]],
        method="historical",
        historical_returns=[[0.01], [-0.01], [0.0]],
        seed=1,
        return_metadata=True,
    )

    assert meta["method"] == "gaussian"
    assert meta["requested_method"] == "historical"
    assert any(str(reason).startswith("insufficient_historical_data") for reason in meta["fallback_reasons"])


def test_tail_metrics_are_finite_and_ordered_with_evt_enabled(monkeypatch) -> None:
    monkeypatch.setattr(mc, "MC_EVT_CVAR_ENABLED", True)
    monkeypatch.setattr(mc, "MC_EVT_MIN_TAIL", 2)
    samples = [0.02, 0.01, -0.01, -0.02, -0.03, -0.08, -0.13, math.inf, -math.inf]

    metrics, evt = mc._tail_risk_metrics(samples)

    assert evt["enabled"] is True
    for value in metrics.values():
        assert math.isfinite(value)
    assert metrics["cvar_95"] <= metrics["var_95"]
    assert metrics["cvar_99"] <= metrics["var_99"]


def test_var_backtest_persists_pit_aligned_exception_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "var_backtest.db"
    con = _connect(db_path)
    try:
        var_backtesting.ensure_var_backtest_schema(con)
        con.execute("CREATE TABLE equity_history(ts_ms INTEGER PRIMARY KEY, equity REAL NOT NULL)")
        con.executemany(
            "INSERT INTO equity_history(ts_ms, equity) VALUES (?, ?)",
            [(1, 100.0), (2, 94.0), (3, 93.0)],
        )
        con.execute(
            """
            INSERT INTO risk_var_forecasts(
              forecast_id, forecast_ts_ms, horizon_steps, var_95, var_99, cvar_95, cvar_99,
              simulation_method, metadata_json, created_ts_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("f1", 1, 1, -0.05, -0.10, -0.07, -0.12, "student_t", "{}", 1),
        )

        result = var_backtesting.run_var_backtest(con=con, now_ms=3, step_ms=1, rolling_window=10)

        assert result["ok"] is True
        assert result["written"] == 2
        row = con.execute(
            """
            SELECT forecast_id, forecast_ts_ms, realized_ts_ms, confidence_level,
                   realized_portfolio_return, exception, metadata_json
            FROM risk_var_backtest_results
            WHERE confidence_level=0.95
            """
        ).fetchone()
        assert row["forecast_id"] == "f1"
        assert row["forecast_ts_ms"] == 1
        assert row["realized_ts_ms"] == 2
        assert round(float(row["realized_portfolio_return"]), 4) == -0.06
        assert bool(row["exception"]) is True
        metadata = json.loads(row["metadata_json"])
        assert metadata["pit_alignment"]["start_ts_ms"] == 1
        assert metadata["pit_alignment"]["target_ts_ms"] == 2
    finally:
        con.close()


def test_kupiec_christoffersen_and_traffic_light_statuses_are_controlled() -> None:
    passing = [1 if idx in (80, 160) else 0 for idx in range(250)]
    failing_coverage = [1 if idx % 5 == 0 else 0 for idx in range(250)]
    clustered = [0] * 100 + [1] * 20 + [0] * 130

    assert var_backtesting.kupiec_pof_test(passing, 0.99)["status"] == "pass"
    assert var_backtesting.kupiec_pof_test(failing_coverage, 0.99)["status"] == "fail"
    assert var_backtesting.christoffersen_independence_test(passing)["status"] == "pass"
    assert var_backtesting.christoffersen_independence_test(clustered)["status"] == "fail"
    assert var_backtesting.traffic_light_status(passing, 0.99)["status"] == "green"
    assert var_backtesting.traffic_light_status(failing_coverage, 0.99)["status"] == "red"


def test_var_backtest_api_degrades_when_tables_are_missing(tmp_path: Path, monkeypatch) -> None:
    con = _connect(tmp_path / "missing_var_tables.db")

    import engine.risk.var_backtesting as module

    monkeypatch.setattr(module, "connect", lambda readonly=True: con)

    payload = api_system.api_get_risk_var_backtest(None)

    assert payload["ok"] is True
    assert payload["ready"] is False
    assert payload["status"] == "schema_missing"
    assert payload["rows"] == []
