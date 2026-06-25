from __future__ import annotations

import pytest

from engine.strategy import borrow_cost_model


def test_borrow_cost_flags_default_on_and_cpcv_inherits(monkeypatch) -> None:
    monkeypatch.delenv("EQUITY_BORROW_COST_ENABLED", raising=False)
    monkeypatch.delenv("CPCV_BORROW_COST_ENABLED", raising=False)
    assert borrow_cost_model.borrow_cost_enabled() is True
    assert borrow_cost_model.cpcv_borrow_cost_enabled() is True

    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "0")
    monkeypatch.delenv("CPCV_BORROW_COST_ENABLED", raising=False)
    assert borrow_cost_model.borrow_cost_enabled() is False
    assert borrow_cost_model.cpcv_borrow_cost_enabled() is False

    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "1")
    monkeypatch.setenv("CPCV_BORROW_COST_ENABLED", "0")
    assert borrow_cost_model.borrow_cost_enabled() is True
    assert borrow_cost_model.cpcv_borrow_cost_enabled() is False


def test_borrow_difficulty_bucket_uses_days_to_cover_thresholds(monkeypatch) -> None:
    monkeypatch.delenv("EQUITY_BORROW_DTC_THRESHOLDS_JSON", raising=False)

    assert borrow_cost_model.borrow_difficulty_bucket(days_to_cover=1.0) == "GC"
    assert borrow_cost_model.borrow_difficulty_bucket(days_to_cover=4.0) == "MODERATE"
    assert borrow_cost_model.borrow_difficulty_bucket(days_to_cover=8.0) == "HARD"
    assert borrow_cost_model.borrow_difficulty_bucket(days_to_cover=12.0) == "SPECIAL"


def test_borrow_schedule_and_threshold_overrides(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_BPS_PER_YEAR_JSON", '{"HARD": 730, "SPECIAL": 1460}')
    monkeypatch.setenv("EQUITY_BORROW_DTC_THRESHOLDS_JSON", '{"GC": 1, "MODERATE": 2, "HARD": 3}')

    assert borrow_cost_model.borrow_difficulty_bucket(days_to_cover=2.5) == "HARD"
    assert borrow_cost_model.annual_borrow_bps(bucket="HARD") == pytest.approx(730.0)
    assert borrow_cost_model.annual_borrow_bps(days_to_cover=4.0) == pytest.approx(1460.0)


def test_borrow_bps_for_period_and_missing_difficulty_floor(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_BPS_PER_YEAR_JSON", '{"GC": 365, "MODERATE": 730}')
    monkeypatch.setenv("EQUITY_BORROW_DEFAULT_BUCKET", "MODERATE")

    # 730 annual bps for half a day produces exactly 1 period bps.
    assert borrow_cost_model.borrow_bps_for_period(holding_days=0.5) == pytest.approx(1.0)
    assert borrow_cost_model.borrow_bps_for_period(holding_days=0.0, bucket="GC") == 0.0
    assert borrow_cost_model.annual_borrow_bps() == pytest.approx(730.0)


def test_is_borrowable_short_equity_gate() -> None:
    assert borrow_cost_model.is_borrowable_short_equity(side=-1, asset_class="EQUITY") is True
    assert borrow_cost_model.is_borrowable_short_equity(side=-1, asset_class="US_EQUITY") is True
    assert borrow_cost_model.is_borrowable_short_equity(side=1, asset_class="EQUITY") is False
    assert borrow_cost_model.is_borrowable_short_equity(side=-1, asset_class="FX") is False
    assert borrow_cost_model.is_borrowable_short_equity(side=-1, asset_class="UNKNOWN") is False
