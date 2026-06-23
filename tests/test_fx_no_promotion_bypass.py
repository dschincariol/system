from __future__ import annotations

import numpy as np
import pytest

from engine.strategy.gated_backtest import run_gated_backtest
from engine.strategy.promotion_guard import assess_challenger
from tests.promotion_test_helpers import passing_deconfounded_payload

pytestmark = pytest.mark.safety_critical


def test_failing_fx_challenger_is_rejected_by_assess_challenger() -> None:
    n = 40
    predictions = np.asarray([1.0, -1.0] * (n // 2), dtype=float)
    realized = np.sign(predictions) * 0.00001
    costed = run_gated_backtest(
        predictions,
        realized,
        sample_times_ms=[1_700_000_000_000 + idx * 60_000 for idx in range(n)],
        symbols=["EURUSD"] * n,
        cost_config={
            "enabled": True,
            "asset_class": "FX",
            "symbol": "EURUSD",
            "nights": 1,
            "crosses_weekend": True,
        },
    )
    net_returns = list(costed["returns"])
    assert sum(net_returns) < 0.0

    passed, diagnostics = assess_challenger(
        model_id="fx_cost_eaten_challenger",
        model_name="fx_cost_eaten_challenger",
        challenger_returns=net_returns,
        champion_returns=[0.0] * len(net_returns),
        deconfounded_validation=passing_deconfounded_payload(len(net_returns)),
        bootstrap_samples=199,
        random_state=17,
        persist=False,
    )

    assert passed is False
    assert diagnostics["passed"] is False
    assert diagnostics["status"] == "fail"

