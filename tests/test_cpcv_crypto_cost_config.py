from __future__ import annotations

import pytest

from engine.execution.crypto_costs import fee_bps, spread_bps
from engine.strategy import cpcv


def test_crypto_cost_config_resolves_spread_fee_and_carries_context() -> None:
    cfg = cpcv.cpcv_cost_config_from_env({"asset_class": "CRYPTO", "symbol": "BTC", "nights": 2})
    assert cfg["asset_class"] == "CRYPTO"
    assert cfg["symbol"] == "BTC"
    assert cfg["commission_bps"] == pytest.approx(fee_bps("BTC", taker=True))
    assert cfg["half_spread_bps"] == pytest.approx(spread_bps("BTC", half=True))
    assert cfg["nights"] == 2
    assert cfg["liquidity"] == "taker"


def test_crypto_cpcv_component_includes_funding_carry() -> None:
    cfg = cpcv.cpcv_cost_config_from_env({"asset_class": "CRYPTO", "symbol": "BTC", "nights": 2})
    components = cpcv._cost_components_for_turnover(1.0, cost_config=cfg)
    assert components["crypto_spread_bps"] == pytest.approx(spread_bps("BTC", half=False))
    assert components["crypto_fee_bps"] == pytest.approx(fee_bps("BTC", taker=True))
    assert components["funding_carry_bps"] == pytest.approx(6.0)


def test_non_crypto_cost_config_shape_is_unchanged() -> None:
    cfg = cpcv.cpcv_cost_config_from_env(
        {
            "asset_class": "US_EQUITY",
            "commission_bps": 1.23,
            "half_spread_bps": 4.56,
        }
    )
    assert cfg["asset_class"] == "US_EQUITY"
    assert cfg["commission_bps"] == pytest.approx(1.23)
    assert cfg["half_spread_bps"] == pytest.approx(4.56)
    assert "symbol" not in cfg
    assert "nights" not in cfg
    assert "liquidity" not in cfg
