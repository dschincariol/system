from __future__ import annotations

import pytest

from engine.execution.broker_sim import _offline_ac_cost_components

pytestmark = pytest.mark.safety_critical


def test_fx_offline_cost_components_include_spread_swap_and_weekend() -> None:
    base_cfg = {
        "enabled": True,
        "asset_class": "FX_MAJOR",
        "symbol": "EUR_USD",
        "commission_bps": 0.1,
        "notional": 100_000.0,
        "adv": 50_000_000.0,
        "sigma_daily": 80.0,
        "participation": 0.05,
        "nights": 0,
        "crosses_weekend": False,
    }
    no_overnight = _offline_ac_cost_components(1.0, cost_config=base_cfg)
    with_overnight = _offline_ac_cost_components(
        1.0,
        cost_config={**base_cfg, "nights": 2, "crosses_weekend": True},
    )

    assert with_overnight["fx_pip_spread_bps"] > 0.0
    assert with_overnight["swap_carry_bps"] > 0.0
    assert with_overnight["weekend_gap_bps"] > 0.0
    assert with_overnight["total_cost_bps"] > no_overnight["total_cost_bps"]
    assert with_overnight["cost_return"] > no_overnight["cost_return"]


def test_non_fx_offline_cost_output_shape_is_unchanged() -> None:
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
    assert "fx_pip_spread_bps" not in result
    assert result["commission_bps"] == pytest.approx(1.0)
    assert result["half_spread_bps"] == pytest.approx(2.0)

