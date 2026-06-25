from __future__ import annotations

import pytest

from engine.strategy.crypto_sizing import (
    clamp_crypto_weight_to_leverage,
    crypto_weight_to_notional,
    normalize_crypto_symbol,
)


def test_crypto_leverage_clamp_and_fractional_diagnostics(monkeypatch):
    monkeypatch.setenv("CRYPTO_MAX_LEVERAGE", "0.25")
    monkeypatch.setenv("CRYPTO_VOL_TARGET", "0")
    instrument = {"asset_class": "CRYPTO", "symbol": "BTC", "min_increment": 0.00000001}

    clamped, diag = clamp_crypto_weight_to_leverage("BTCUSD", 0.50, 100_000.0, instrument)

    assert clamped == pytest.approx(0.25)
    assert diag["clamped"] is True
    assert diag["fractional"] is True
    assert diag["min_increment"] == pytest.approx(0.00000001)
    assert diag["effective_leverage_cap"] == pytest.approx(0.25)


def test_crypto_leverage_within_cap_is_pure(monkeypatch):
    monkeypatch.setenv("CRYPTO_MAX_LEVERAGE", "1.0")
    monkeypatch.setenv("CRYPTO_VOL_TARGET", "0")
    instrument = {"asset_class": "CRYPTO", "symbol": "ETH", "min_increment": 0.000001}

    first = clamp_crypto_weight_to_leverage("ETH/USD", -0.15, 250_000.0, instrument)
    second = clamp_crypto_weight_to_leverage("ETH/USD", -0.15, 250_000.0, instrument)

    assert first == second
    assert first[0] == pytest.approx(-0.15)
    assert first[1]["clamped"] is False
    assert first[1]["fractional"] is True


def test_crypto_vol_target_can_tighten_effective_cap(monkeypatch):
    monkeypatch.setenv("CRYPTO_MAX_LEVERAGE", "2.0")
    monkeypatch.setenv("CRYPTO_VOL_TARGET", "0.03")
    instrument = {"asset_class": "CRYPTO", "symbol": "SOL", "volatility": 0.12}

    clamped, diag = clamp_crypto_weight_to_leverage("SOLUSDT", 0.60, 100_000.0, instrument)

    assert clamped == pytest.approx(0.25)
    assert diag["cap_source"] == "crypto_vol_target"
    assert diag["volatility"] == pytest.approx(0.12)


def test_crypto_notional_uses_fractional_units(monkeypatch):
    monkeypatch.setenv("CRYPTO_MAX_LEVERAGE", "1.0")
    monkeypatch.setenv("CRYPTO_VOL_TARGET", "0")
    meta = crypto_weight_to_notional(
        "BTCUSD",
        0.10,
        100_000.0,
        {"asset_class": "CRYPTO", "symbol": "BTC", "min_increment": 0.00000001},
        price=50_000.0,
    )

    assert meta["notional_usd"] == pytest.approx(10_000.0)
    assert meta["units"] == pytest.approx(0.20)
    assert meta["fractional"] is True
    assert meta["min_increment"] == pytest.approx(0.00000001)


def test_normalize_crypto_symbol_variants():
    assert normalize_crypto_symbol("BTC/USD") == "BTC"
    assert normalize_crypto_symbol("eth-usdt") == "ETH"
    assert normalize_crypto_symbol("SOLUSDC") == "SOL"
    assert normalize_crypto_symbol("XBTUSD") == "BTC"
