from __future__ import annotations

import pytest

from engine.execution.fx_costs import pip_spread_bps
from engine.strategy import cpcv


def test_fx_major_cost_config_resolves_spread_and_carries_context() -> None:
    cfg = cpcv.cpcv_cost_config_from_env(
        {
            "asset_class": "FX_MAJOR",
            "symbol": "EUR_USD",
            "nights": 2,
            "crosses_weekend": True,
        }
    )
    assert cfg["asset_class"] == "FX_MAJOR"
    assert cfg["symbol"] == "EUR_USD"
    assert cfg["half_spread_bps"] == pytest.approx(pip_spread_bps("EUR_USD", half=True))
    assert cfg["nights"] == 2
    assert cfg["crosses_weekend"] is True


def test_fx_asset_class_prefix_is_tag_agnostic() -> None:
    cfg = cpcv.cpcv_cost_config_from_env({"asset_class": "FX", "symbol": "EURUSD"})
    assert cfg["asset_class"] == "FX"
    assert cfg["symbol"] == "EUR_USD"
    assert cfg["half_spread_bps"] == pytest.approx(pip_spread_bps("EUR_USD", half=True))


def test_fx_commission_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CPCV_FX_COMMISSION_BPS", "0.42")
    assert cpcv._default_commission_bps("FX_MINOR") == pytest.approx(0.42)


def test_non_fx_cost_config_is_unchanged_shape() -> None:
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
    assert "crosses_weekend" not in cfg

