from __future__ import annotations

import numpy as np

from engine.strategy.cpcv import _compute_sharpe
from engine.strategy.gated_backtest import run_gated_backtest


def _times(n: int) -> list[int]:
    base = 1_700_000_000_000
    return [base + idx * 60_000 for idx in range(n)]


def test_fx_gated_backtest_net_costs_reduce_sharpe() -> None:
    n = 12
    predictions = np.asarray([1.0, -1.0] * (n // 2), dtype=float)
    realized = np.sign(predictions) * np.asarray([0.003, 0.005] * (n // 2), dtype=float)
    costed = run_gated_backtest(
        predictions,
        realized,
        sample_times_ms=_times(n),
        symbols=["EUR_USD"] * n,
        cost_config={
            "enabled": True,
            "asset_class": "FX_MAJOR",
            "symbol": "EUR_USD",
            "nights": 1,
            "crosses_weekend": True,
        },
    )
    frictionless = np.asarray(costed["frictionless_returns"], dtype=float)
    net = np.asarray(costed["returns"], dtype=float)
    assert float(costed["costs"]["total_cost_return"]) > 0.0
    assert float(np.sum(net)) < float(np.sum(frictionless))
    assert _compute_sharpe(net) < _compute_sharpe(frictionless)


def test_marginal_fx_signal_flips_negative_after_costs() -> None:
    n = 10
    predictions = np.asarray([1.0, -1.0] * (n // 2), dtype=float)
    realized = np.sign(predictions) * 0.00005
    costed = run_gated_backtest(
        predictions,
        realized,
        sample_times_ms=_times(n),
        symbols=["EURUSD"] * n,
        cost_config={
            "enabled": True,
            "asset_class": "FX",
            "symbol": "EURUSD",
            "nights": 1,
            "crosses_weekend": True,
        },
    )
    assert float(sum(costed["frictionless_returns"])) > 0.0
    assert float(sum(costed["returns"])) < 0.0
