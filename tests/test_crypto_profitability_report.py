from __future__ import annotations

import importlib
import sys

import numpy as np

from engine.strategy.crypto_profitability_report import evaluate_crypto_challengers


_LIVE_BROKER_MODULES = {
    "engine.execution.broker_coinbase",
    "engine.execution.broker_binance",
    "engine.execution.broker_alpaca_rest",
    "engine.execution.broker_ibkr_gateway",
}


def test_crypto_profitability_report_passes_strong_and_fails_cost_eaten_signal() -> None:
    strong_predictions = np.ones(12, dtype=float)
    strong_realized = np.asarray([0.020] * 12, dtype=float)
    marginal_predictions = np.asarray([1.0, -1.0] * 6, dtype=float)
    marginal_realized = np.sign(marginal_predictions) * np.asarray([0.0004] * 12, dtype=float)

    report = evaluate_crypto_challengers(
        [
            {
                "symbol": "BTC/USD",
                "factor": "strong",
                "predictions": strong_predictions,
                "realized_returns": strong_realized,
                "gate_config": {"enabled": True, "min_observations": 2, "min_t_stat": 0.0},
            },
            {
                "symbol": "BTC",
                "factor": "cost_eaten",
                "predictions": marginal_predictions,
                "realized_returns": marginal_realized,
                "gate_config": {"enabled": True, "min_observations": 2, "min_t_stat": 0.0},
            },
        ],
        n_competing_trials=1,
    )

    assert report["symbols"]["BTC"]["strong"]["passed"] is True
    assert report["symbols"]["BTC"]["strong"]["cost_drag_bps"] > 0.0
    assert report["symbols"]["BTC"]["cost_eaten"]["passed"] is False
    assert report["summary"] == {"n_pass": 1, "n_fail": 1}


def test_crypto_profitability_report_imports_no_live_broker_modules() -> None:
    saved_live_modules = {name: sys.modules.pop(name) for name in _LIVE_BROKER_MODULES if name in sys.modules}
    before = set(str(name) for name in sys.modules)
    try:
        module = importlib.reload(importlib.import_module("engine.strategy.crypto_profitability_report"))
        introduced = set(str(name) for name in sys.modules) - before
        evaluate_crypto_challengers(
            [{"symbol": "BTC", "factor": "empty", "predictions": [], "realized_returns": []}],
            n_competing_trials=1,
        )
        introduced_after_eval = set(str(name) for name in sys.modules) - before
        assert not (_LIVE_BROKER_MODULES & introduced)
        assert not (_LIVE_BROKER_MODULES & introduced_after_eval)
        assert module.evaluate_crypto_challengers is not None
    finally:
        for name in _LIVE_BROKER_MODULES:
            if name not in saved_live_modules:
                sys.modules.pop(name, None)
        sys.modules.update(saved_live_modules)
