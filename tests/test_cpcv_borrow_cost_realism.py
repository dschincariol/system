from __future__ import annotations

import math

import numpy as np
import pytest

from engine.strategy import cpcv
from engine.strategy.borrow_cost_model import borrow_bps_for_period


pytestmark = pytest.mark.safety_critical

_BORROW_ENV_VARS = (
    "EQUITY_BORROW_COST_ENABLED",
    "CPCV_BORROW_COST_ENABLED",
    "EQUITY_BORROW_BPS_PER_YEAR_JSON",
    "EQUITY_BORROW_DTC_THRESHOLDS_JSON",
    "EQUITY_BORROW_DEFAULT_BUCKET",
    "CPCV_BORROW_BUCKET",
    "CPCV_BORROW_DAYS_TO_COVER",
    "CPCV_BORROW_SHORT_INTEREST_SHARES",
    "CPCV_BORROW_FLOAT_SHARES",
    "CPCV_PERIOD_DAYS",
)


def _zero_cost_config(**overrides):
    cfg = {
        "enabled": True,
        "asset_class": "US_EQUITY",
        "commission_bps": 0.0,
        "half_spread_bps": 0.0,
        "notional": 100.0,
        "adv": 10_000.0,
        "sigma_daily": 0.0,
        "participation": 0.0,
        "borrow_enabled": True,
        "borrow_bucket": "HARD",
        "period_days": 2.0,
    }
    cfg.update(overrides)
    return cfg


def _clear_borrow_env(monkeypatch) -> None:
    for name in _BORROW_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_cpcv_borrow_cost_binds_with_default_config(monkeypatch) -> None:
    _clear_borrow_env(monkeypatch)
    predictions = np.asarray([-1.0], dtype=float)
    realized = np.asarray([0.010], dtype=float)
    period_bps = borrow_bps_for_period(holding_days=1.0)

    adjusted, meta = cpcv._apply_transaction_costs_to_returns(
        predictions,
        realized,
        cost_config={
            "enabled": True,
            "asset_class": "US_EQUITY",
            "commission_bps": 0.0,
            "half_spread_bps": 0.0,
            "notional": 100.0,
            "adv": 10_000.0,
            "sigma_daily": 0.0,
            "participation": 0.0,
        },
    )

    assert period_bps > 0.0
    assert np.allclose(adjusted, np.asarray([-0.010 - period_bps / 10000.0]), atol=1e-12)
    assert meta["config"]["borrow_enabled"] is True
    assert meta["config"]["period_days"] == pytest.approx(1.0)
    assert meta["borrow_bps"] == pytest.approx([period_bps])
    assert meta["total_borrow_return"] == pytest.approx(period_bps / 10000.0)


def test_cpcv_borrow_cost_reduces_short_held_equity_intervals(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "1")
    predictions = np.asarray([1.0, -1.0, -1.0, 0.0], dtype=float)
    realized = np.asarray([0.010, 0.020, -0.030, 0.010], dtype=float)
    period_bps = borrow_bps_for_period(holding_days=2.0, bucket="HARD")

    adjusted, meta = cpcv._apply_transaction_costs_to_returns(
        predictions,
        realized,
        cost_config=_zero_cost_config(),
    )

    expected = np.asarray(
        [
            0.010,
            -0.020 - period_bps / 10000.0,
            0.030 - period_bps / 10000.0,
            0.0,
        ],
        dtype=float,
    )
    assert np.allclose(adjusted, expected, atol=1e-12)
    assert meta["borrow_bps"] == pytest.approx([0.0, period_bps, period_bps, 0.0])
    assert meta["total_borrow_return"] == pytest.approx(2.0 * period_bps / 10000.0)


def test_cpcv_borrow_cost_flag_off_preserves_legacy_cost_array(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "0")
    predictions = np.asarray([1.0, 1.0, -1.0, 0.0], dtype=float)
    realized = np.asarray([0.010, 0.020, -0.010, 0.030], dtype=float)
    cost_config = {
        "enabled": True,
        "asset_class": "US_EQUITY",
        "commission_bps": 1.0,
        "half_spread_bps": 2.0,
        "notional": 100.0,
        "adv": 10_000.0,
        "sigma_daily": 100.0,
        "participation": 0.10,
        "borrow_enabled": False,
    }

    adjusted, meta = cpcv._apply_transaction_costs_to_returns(
        predictions,
        realized,
        cost_config=cost_config,
    )

    eta = 0.142
    temp_one = eta * 100.0 * math.sqrt(100.0 / 10_000.0)
    temp_flip = eta * 100.0 * math.sqrt(200.0 / 10_000.0)
    expected = np.asarray(
        [
            0.010 - ((1.0 + 2.0 + temp_one) / 10_000.0),
            0.020,
            0.010 - (2.0 * (1.0 + 2.0 + temp_flip) / 10_000.0),
            0.0 - ((1.0 + 2.0 + temp_one) / 10_000.0),
        ],
        dtype=float,
    )
    assert np.allclose(adjusted, expected, atol=1e-12)
    assert meta["total_borrow_return"] == 0.0
    assert meta["borrow_bps"] == [0.0, 0.0, 0.0, 0.0]


def test_cpcv_borrow_cost_ignores_long_and_non_equity(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "1")
    realized = np.asarray([0.010, -0.010], dtype=float)

    long_adjusted, long_meta = cpcv._apply_transaction_costs_to_returns(
        np.asarray([1.0, 1.0], dtype=float),
        realized,
        cost_config=_zero_cost_config(),
    )
    assert np.allclose(long_adjusted, realized, atol=1e-12)
    assert long_meta["total_borrow_return"] == 0.0

    non_equity_adjusted, non_equity_meta = cpcv._apply_transaction_costs_to_returns(
        np.asarray([-1.0, -1.0], dtype=float),
        realized,
        cost_config=_zero_cost_config(asset_class="CRYPTO"),
    )
    assert np.allclose(non_equity_adjusted, -realized, atol=1e-12)
    assert non_equity_meta["total_borrow_return"] == 0.0
