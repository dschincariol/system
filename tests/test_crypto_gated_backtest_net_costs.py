from __future__ import annotations

import numpy as np

from engine.execution.cost_models.almgren_chriss import AlmgrenChrissCost
from engine.strategy import cpcv
from engine.strategy.gated_backtest import run_gated_backtest


def test_crypto_gated_backtest_net_returns_are_below_gross() -> None:
    predictions = np.asarray([1.0, -1.0, 1.0, -1.0, 1.0, -1.0], dtype=float)
    realized = np.sign(predictions) * np.asarray([0.010, 0.011, 0.012, 0.013, 0.011, 0.012], dtype=float)
    result = run_gated_backtest(
        predictions,
        realized,
        sample_times_ms=np.arange(6, dtype=int),
        symbols=["BTC"] * 6,
        cost_config={"enabled": True, "asset_class": "CRYPTO", "symbol": "BTC", "nights": 1},
    )

    assert result["ok"]
    assert sum(result["returns"]) < sum(result["frictionless_returns"])
    assert cpcv._compute_sharpe(result["returns"]) < cpcv._compute_sharpe(result["frictionless_returns"])
    assert result["costs"]["components"][0]["crypto_fee_bps"] > 0.0


def test_marginal_crypto_signal_flips_net_negative() -> None:
    predictions = np.asarray([1.0, -1.0, 1.0, -1.0, 1.0, -1.0], dtype=float)
    realized = np.sign(predictions) * np.asarray([0.0004] * 6, dtype=float)
    result = run_gated_backtest(
        predictions,
        realized,
        sample_times_ms=np.arange(6, dtype=int),
        symbols=["BTC"] * 6,
        cost_config={"enabled": True, "asset_class": "CRYPTO", "symbol": "BTC", "nights": 1},
    )

    assert sum(result["frictionless_returns"]) > 0.0
    assert sum(result["returns"]) < 0.0


def test_crypto_almgren_chriss_uses_crypto_coefficients() -> None:
    model = AlmgrenChrissCost()
    equity = model.components_bps(
        notional=100_000.0,
        adv=10_000_000.0,
        sigma_daily=100.0,
        participation=0.10,
        asset_class="US_EQUITY",
    )
    crypto = model.components_bps(
        notional=100_000.0,
        adv=10_000_000.0,
        sigma_daily=100.0,
        participation=0.10,
        asset_class="CRYPTO",
    )
    assert crypto["temporary_impact_bps"] > equity["temporary_impact_bps"]
