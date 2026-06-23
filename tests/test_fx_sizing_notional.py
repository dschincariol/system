from __future__ import annotations

import json
import uuid

from engine.strategy.fx_sizing import clamp_fx_weight_to_leverage, fx_weight_to_notional


def test_eurusd_weight_converts_to_fx_notional_not_share_count():
    canary = f"canary-{uuid.uuid4()}"
    instrument = {
        "asset_class": "FX",
        "base_ccy": "EUR",
        "quote_ccy": "USD",
        "pip_size": 0.0001,
        "contract_size": 100_000.0,
        "leverage_cap": 50.0,
    }

    out = fx_weight_to_notional("EURUSD", 0.10, 100_000.0, instrument, pair_rate=1.08)

    assert out["asset_class"] == "FX"
    assert out["base_notional"] == 10_000.0
    assert out["quote_notional"] == 10_800.0
    assert out["units"] == 10_000.0
    assert out["lots"] == 0.10
    assert out["units"] != (0.10 * 100_000.0 / 1.08)
    assert canary not in json.dumps(out, sort_keys=True)


def test_usdjpy_weight_uses_pair_rate_and_standard_contract_size():
    instrument = {
        "asset_class": "FX",
        "base_ccy": "USD",
        "quote_ccy": "JPY",
        "pip_size": 0.01,
        "contract_size": 100_000.0,
        "leverage_cap": 50.0,
    }

    out = fx_weight_to_notional("USDJPY", 0.05, 200_000.0, instrument, pair_rate=150.0)

    assert out["base_ccy"] == "USD"
    assert out["quote_ccy"] == "JPY"
    assert out["base_notional"] == 10_000.0
    assert out["quote_notional"] == 1_500_000.0
    assert out["lots"] == 0.10
    assert out["units"] != (0.05 * 200_000.0 / 150.0)


def test_missing_fx02_metadata_clamp_degrades_without_raise():
    clamped, reason = clamp_fx_weight_to_leverage("EURUSD", 2.0, 100_000.0, None)

    assert clamped == 2.0
    assert reason["type"] == "fx_instrument_missing"
    assert reason["clamped"] is False
