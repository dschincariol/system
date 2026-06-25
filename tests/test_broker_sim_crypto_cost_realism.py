from __future__ import annotations

import pytest

from engine.execution.broker_sim import _offline_ac_cost_components

pytestmark = pytest.mark.safety_critical


def test_crypto_offline_cost_components_include_spread_fee_and_funding() -> None:
    base_cfg = {
        "enabled": True,
        "asset_class": "CRYPTO",
        "symbol": "BTC",
        "notional": 100_000.0,
        "adv": 25_000_000.0,
        "sigma_daily": 350.0,
        "participation": 0.05,
        "nights": 0,
        "side_sign": 1.0,
    }
    intraday = _offline_ac_cost_components(1.0, cost_config=base_cfg)
    held = _offline_ac_cost_components(1.0, cost_config={**base_cfg, "nights": 2})

    assert held["crypto_spread_bps"] > 0.0
    assert held["crypto_fee_bps"] > 0.0
    assert held["funding_carry_bps"] > 0.0
    assert held["total_cost_bps"] > intraday["total_cost_bps"]
    assert held["cost_return"] > intraday["cost_return"]


def test_crypto_short_funding_credit_offsets_total_cost_without_going_negative() -> None:
    long_cost = _offline_ac_cost_components(
        1.0,
        cost_config={"enabled": True, "asset_class": "CRYPTO", "symbol": "BTC", "nights": 2, "side_sign": 1.0},
    )
    short_cost = _offline_ac_cost_components(
        1.0,
        cost_config={"enabled": True, "asset_class": "CRYPTO", "symbol": "BTC", "nights": 2, "side_sign": -1.0},
    )
    assert short_cost["funding_carry_bps"] < 0.0
    assert short_cost["total_cost_bps"] >= 0.0
    assert short_cost["total_cost_bps"] < long_cost["total_cost_bps"]


def test_non_crypto_offline_cost_output_shape_is_unchanged() -> None:
    cfg = {
        "enabled": True,
        "asset_class": "US_EQUITY",
        "commission_bps": 1.0,
        "half_spread_bps": 2.0,
        "notional": 100_000.0,
        "adv": 10_000_000.0,
        "sigma_daily": 100.0,
        "participation": 0.10,
    }
    result = _offline_ac_cost_components(1.0, cost_config=cfg)
    assert set(result) == {
        "turnover",
        "commission_bps",
        "half_spread_bps",
        "temporary_impact_bps",
        "total_cost_bps",
        "cost_return",
    }
    assert result["commission_bps"] == pytest.approx(1.0)
    assert result["half_spread_bps"] == pytest.approx(2.0)
