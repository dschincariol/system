from __future__ import annotations

import json
import uuid

import pytest

from engine.strategy.borrow_cost_model import borrow_bps_for_period
from engine.strategy.net_after_cost_labels import build_net_after_cost_label


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


def _clear_borrow_env(monkeypatch) -> None:
    for name in _BORROW_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_short_equity_net_after_cost_label_binds_with_default_borrow_config(monkeypatch) -> None:
    canary = f"canary-{uuid.uuid4()}"
    _clear_borrow_env(monkeypatch)
    expected_bps = borrow_bps_for_period("SPY", holding_days=1.0, days_to_cover=6.0)
    expected_return = expected_bps / 10000.0

    artifact = build_net_after_cost_label(
        event_id=1,
        symbol="SPY",
        horizon_s=86_400,
        label_ts_ms=1_000_000,
        side=-1,
        gross_return=0.0100,
        net_return=0.0100,
        realized_forward_return=0.0100,
        source="unit-test",
        realized=1,
        entry_ts_ms=1_000_000,
        exit_ts_ms=1_000_000 + 86_400_000,
        context={"alert_detail_json": json.dumps({"days_to_cover": 6.0, "debug": canary})},
        execution_trace={"notional": 100_000.0},
    )

    assert expected_bps > 0.0
    assert artifact["borrow_bps"] == pytest.approx(expected_bps)
    assert artifact["borrow_cost"] == pytest.approx(100_000.0 * expected_return)
    assert artifact["net_return"] == pytest.approx(0.0100 - expected_return)
    assert artifact["execution_cost_return"] == pytest.approx(expected_return)
    assert artifact["total_cost_bps"] >= expected_bps
    metadata = json.loads(artifact["label_metadata_json"])
    assert metadata["cost_evidence"]["borrow_source"] == "synthesized_short_equity"
    assert metadata["borrow_costs"]["bucket"] == "HARD"
    assert metadata["borrow_costs"]["holding_days"] == pytest.approx(1.0)
    serialized = json.dumps(
        {
            "label_metadata_json": artifact["label_metadata_json"],
            "confidence_metadata_json": artifact["confidence_metadata_json"],
        },
        sort_keys=True,
    )
    assert canary not in serialized


def test_borrow_label_flag_off_and_longs_leave_net_return_unchanged(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "0")
    short_off = build_net_after_cost_label(
        event_id=2,
        symbol="SPY",
        horizon_s=86_400,
        label_ts_ms=1_000_000,
        side=-1,
        gross_return=0.0200,
        net_return=0.0200,
        realized_forward_return=0.0200,
        source="unit-test",
        realized=1,
        entry_ts_ms=1_000_000,
        exit_ts_ms=1_000_000 + 86_400_000,
        context={"alert_detail_json": json.dumps({"days_to_cover": 20.0})},
        execution_trace={"notional": 100_000.0},
    )
    assert short_off["borrow_bps"] == 0.0
    assert short_off["borrow_cost"] == 0.0
    assert short_off["net_return"] == 0.0200

    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "1")
    long_on = build_net_after_cost_label(
        event_id=3,
        symbol="SPY",
        horizon_s=86_400,
        label_ts_ms=1_000_000,
        side=1,
        gross_return=0.0200,
        net_return=0.0200,
        realized_forward_return=0.0200,
        source="unit-test",
        realized=1,
        entry_ts_ms=1_000_000,
        exit_ts_ms=1_000_000 + 86_400_000,
        context={"alert_detail_json": json.dumps({"days_to_cover": 20.0})},
        execution_trace={"notional": 100_000.0},
    )
    assert long_on["borrow_bps"] == 0.0
    assert long_on["borrow_cost"] == 0.0
    assert long_on["net_return"] == 0.0200


def test_existing_upstream_borrow_is_not_double_counted(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "1")
    monkeypatch.setenv("EQUITY_BORROW_BPS_PER_YEAR_JSON", '{"SPECIAL": 3650.0}')

    artifact = build_net_after_cost_label(
        event_id=4,
        symbol="SPY",
        horizon_s=86_400,
        label_ts_ms=1_000_000,
        side=-1,
        gross_return=0.0300,
        net_return=0.0300,
        realized_forward_return=0.0300,
        source="unit-test",
        realized=1,
        entry_ts_ms=1_000_000,
        exit_ts_ms=1_000_000 + 86_400_000,
        context={"alert_explain_json": json.dumps({"carry": {"borrow_bps": 2.0}, "days_to_cover": 20.0})},
        execution_trace={"notional": 100_000.0},
    )

    assert artifact["borrow_bps"] == pytest.approx(2.0)
    assert artifact["borrow_cost"] == pytest.approx(20.0)
    assert artifact["net_return"] == pytest.approx(0.0300 - 0.0002)
    metadata = json.loads(artifact["label_metadata_json"])
    assert metadata["cost_evidence"]["borrow_source"] == "upstream"
    assert metadata["borrow_costs"] == {}


def test_non_equity_short_label_does_not_synthesize_borrow(monkeypatch) -> None:
    monkeypatch.setenv("EQUITY_BORROW_COST_ENABLED", "1")
    artifact = build_net_after_cost_label(
        event_id=5,
        symbol="EURUSD",
        horizon_s=86_400,
        label_ts_ms=1_000_000,
        side=-1,
        gross_return=0.0100,
        net_return=0.0100,
        realized_forward_return=0.0100,
        source="unit-test",
        realized=1,
        entry_ts_ms=1_000_000,
        exit_ts_ms=1_000_000 + 86_400_000,
        context={"alert_detail_json": json.dumps({"days_to_cover": 20.0})},
        execution_trace={"notional": 100_000.0},
    )

    assert artifact["borrow_bps"] == 0.0
    assert artifact["borrow_cost"] == 0.0
    assert artifact["net_return"] == 0.0100
