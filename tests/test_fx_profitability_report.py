from __future__ import annotations

import inspect

import numpy as np

from engine.strategy import fx_profitability_report
from engine.strategy.fx_profitability_report import evaluate_fx_challengers
from tests.promotion_test_helpers import passing_deconfounded_payload


def test_evaluate_fx_challengers_reports_pass_and_fail_net_of_costs() -> None:
    strong_n = 80
    weak_n = 60
    strong_pred = np.asarray([1.0] * strong_n, dtype=float)
    strong_realized = np.asarray(
        [0.012 + 0.001 * float((idx % 7) - 3) for idx in range(strong_n)],
        dtype=float,
    )
    weak_pred = np.asarray([1.0, -1.0] * (weak_n // 2), dtype=float)
    report = evaluate_fx_challengers(
        [
            {
                "pair": "EURUSD",
                "factor": "carry_quality",
                "predictions": strong_pred,
                "realized_returns": strong_realized,
                "deconfounded_validation": passing_deconfounded_payload(strong_n),
                "bootstrap_samples": 199,
            },
            {
                "pair": "EUR_USD",
                "factor": "cost_eaten_noise",
                "predictions": weak_pred,
                "realized_returns": np.sign(weak_pred) * 0.00001,
                "crosses_weekend": True,
                "deconfounded_validation": passing_deconfounded_payload(weak_n),
                "bootstrap_samples": 199,
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
    assert rows["carry_quality"]["governance_passed"] is True
    assert rows["carry_quality"]["governance_diagnostics"]["applied"] is True
    assert rows["cost_eaten_noise"]["passed"] is False
    assert rows["cost_eaten_noise"]["governance_passed"] is False
    assert rows["cost_eaten_noise"]["cost_drag_bps"] > 0.0
    assert report["summary"]["n_pass"] == 1
    assert report["summary"]["n_fail"] == 1


def test_profitability_report_has_no_live_broker_import_path() -> None:
    source = inspect.getsource(fx_profitability_report)
    assert "broker_router" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "assess_challenger(" in source


def test_profitability_report_requires_assess_challenger_verdict(monkeypatch) -> None:
    calls: list[dict] = []

    def _blocked_assessment(**kwargs):
        calls.append(dict(kwargs))
        return False, {"applied": True, "passed": False, "status": "blocked_by_test_governance"}

    monkeypatch.setattr(fx_profitability_report, "assess_challenger", _blocked_assessment)
    n = 24
    report = evaluate_fx_challengers(
        [
            {
                "pair": "EURUSD",
                "factor": "carry_quality",
                "predictions": np.ones(n, dtype=float),
                "realized_returns": np.ones(n, dtype=float) * 0.01,
                "deconfounded_validation": passing_deconfounded_payload(n),
                "bootstrap_samples": 199,
            }
        ],
        n_competing_trials=1,
        gate_config={
            "enabled": True,
            "min_observations": 10,
            "min_t_stat": 0.0,
            "min_deflated_sharpe": -10.0,
            "fdr_alpha": 1.0,
        },
    )

    row = report["pairs"]["EUR_USD"]["carry_quality"]
    assert calls
    assert calls[0]["persist"] is False
    assert calls[0]["candidate_symbols"] == ["EUR_USD"] * n
    assert row["passed"] is False
    assert row["governance_passed"] is False
    assert row["governance_diagnostics"]["status"] == "blocked_by_test_governance"
    assert report["summary"]["n_pass"] == 0
    assert report["summary"]["n_fail"] == 1
