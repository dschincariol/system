from __future__ import annotations

import logging
import uuid

import pytest

from engine.execution import fx_costs


def test_pip_spread_bps_uses_pip_size_and_reference_price() -> None:
    expected = (
        fx_costs.FX_PIP_SPREAD["EUR_USD"]
        * fx_costs.FX_PIP_SIZE["EUR_USD"]
        / fx_costs.FX_REF_PRICE["EUR_USD"]
        * 10000.0
    )
    assert fx_costs.pip_spread_bps("EUR_USD", half=False) == pytest.approx(expected)
    assert fx_costs.pip_spread_bps("EURUSD", half=False) == pytest.approx(expected)
    assert fx_costs.pip_spread_bps("EUR/USD", half=True) == pytest.approx(expected / 2.0)


def test_jpy_pair_uses_jpy_pip_size() -> None:
    expected = (
        fx_costs.FX_PIP_SPREAD["USD_JPY"]
        * fx_costs.FX_PIP_SIZE["USD_JPY"]
        / fx_costs.FX_REF_PRICE["USD_JPY"]
        * 10000.0
    )
    assert fx_costs.pip_spread_bps("USDJPY", half=False) == pytest.approx(expected)


def test_normalize_fx_symbol_variants_hit_same_key() -> None:
    assert fx_costs.normalize_fx_symbol("EURUSD") == "EUR_USD"
    assert fx_costs.normalize_fx_symbol("EUR/USD") == "EUR_USD"
    assert fx_costs.normalize_fx_symbol("eur_usd") == "EUR_USD"


def test_swap_bps_is_side_aware_and_scales_by_nights() -> None:
    long_one = fx_costs.swap_bps("EUR_USD", side_sign=1.0, nights=1)
    short_one = fx_costs.swap_bps("EUR_USD", side_sign=-1.0, nights=1)
    long_two = fx_costs.swap_bps("EUR_USD", side_sign=1.0, nights=2)
    assert long_one != short_one
    assert long_two == pytest.approx(2.0 * long_one)
    assert fx_costs.swap_bps("EUR_USD", side_sign=1.0, nights=0) == 0.0


def test_weekend_gap_bps_only_when_crossing_weekend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FX_WEEKEND_GAP_BPS", "4.0")
    assert fx_costs.weekend_gap_bps("EUR_USD", crosses_weekend=False) == 0.0
    assert fx_costs.weekend_gap_bps("EUR_USD", crosses_weekend=True) == pytest.approx(4.0)


def test_malformed_override_is_ignored_without_canary_leak(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    canary = "CANARY-" + uuid.uuid4().hex
    monkeypatch.setenv("FX_PIP_SPREAD_OVERRIDE_JSON", canary)
    with caplog.at_level(logging.WARNING):
        value = fx_costs.pip_spread_bps("EUR_USD", half=False)
    assert value > 0.0
    assert canary not in str(value)
    assert canary not in caplog.text

