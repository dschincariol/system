from __future__ import annotations

import inspect

import numpy as np

from engine.strategy import fx_profitability_report
from engine.strategy.fx_profitability_report import evaluate_fx_challengers


def test_evaluate_fx_challengers_reports_pass_and_fail_net_of_costs() -> None:
    strong_n = 60
    weak_n = 60
    strong_pred = np.asarray([1.0] * strong_n, dtype=float)
    weak_pred = np.asarray([1.0, -1.0] * (weak_n // 2), dtype=float)
    report = evaluate_fx_challengers(
        [
            {
                "pair": "EURUSD",
                "factor": "carry_quality",
                "predictions": strong_pred,
                "realized_returns": np.ones(strong_n, dtype=float) * 0.01,
            },
            {
                "pair": "EUR_USD",
                "factor": "cost_eaten_noise",
                "predictions": weak_pred,
                "realized_returns": np.sign(weak_pred) * 0.00001,
                "crosses_weekend": True,
            },
        ],
        n_competing_trials=2,
        gate_config={
            "enabled": True,
            "min_observations": 10,
            "min_t_stat": 0.0,
            "min_deflated_sharpe": -10.0,
            "fdr_alpha": 1.0,
        },
    )

    rows = report["pairs"]["EUR_USD"]
    assert rows["carry_quality"]["passed"] is True
    assert rows["cost_eaten_noise"]["passed"] is False
    assert rows["cost_eaten_noise"]["cost_drag_bps"] > 0.0
    assert report["summary"]["n_pass"] == 1
    assert report["summary"]["n_fail"] == 1


def test_profitability_report_has_no_live_broker_import_path() -> None:
    source = inspect.getsource(fx_profitability_report)
    assert "broker_router" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source

