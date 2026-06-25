from __future__ import annotations

import logging
import uuid

import pytest

from engine.execution import crypto_costs


def test_fee_spread_and_funding_carry_are_deterministic() -> None:
    assert crypto_costs.fee_bps("BTC", taker=True) == pytest.approx(crypto_costs.CRYPTO_TAKER_BPS["BTC"])
    assert crypto_costs.fee_bps("BTC", taker=False) == pytest.approx(crypto_costs.CRYPTO_MAKER_BPS["BTC"])
    assert crypto_costs.spread_bps("BTC", half=False) == pytest.approx(crypto_costs.CRYPTO_SPREAD_BPS["BTC"])
    assert crypto_costs.spread_bps("BTC", half=True) == pytest.approx(crypto_costs.CRYPTO_SPREAD_BPS["BTC"] / 2.0)

    long_two = crypto_costs.funding_carry_bps("BTC", side_sign=1.0, nights=2)
    short_two = crypto_costs.funding_carry_bps("BTC", side_sign=-1.0, nights=2)
    assert long_two == pytest.approx(6.0)
    assert short_two == pytest.approx(-6.0)
    assert crypto_costs.funding_carry_bps("BTC", side_sign=1.0, nights=0) == 0.0


def test_normalize_crypto_symbol_variants_hit_same_key() -> None:
    assert crypto_costs.normalize_crypto_symbol("BTCUSD") == "BTC"
    assert crypto_costs.normalize_crypto_symbol("BTC/USD") == "BTC"
    assert crypto_costs.normalize_crypto_symbol("BTC") == "BTC"
    assert crypto_costs.normalize_crypto_symbol("ETHUSDT") == "ETH"
    assert crypto_costs.is_crypto_symbol("BTC/USD") is True
    assert crypto_costs.is_crypto_symbol("AAPL") is False


def test_override_json_is_safe_and_canary_does_not_leak(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    canary = "CRYPTO-COST-CANARY-" + uuid.uuid4().hex
    monkeypatch.setenv("CRYPTO_SPREAD_BPS_OVERRIDE_JSON", '{"BTC": 7.5}')
    assert crypto_costs.spread_bps("BTC", half=False) == pytest.approx(7.5)

    monkeypatch.setenv("CRYPTO_TAKER_BPS_OVERRIDE_JSON", canary)
    with caplog.at_level(logging.WARNING):
        value = crypto_costs.fee_bps("BTC", taker=True)
    assert value == pytest.approx(crypto_costs.CRYPTO_TAKER_BPS["BTC"])
    assert canary not in str(value)
    assert canary not in caplog.text
