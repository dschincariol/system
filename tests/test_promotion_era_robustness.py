from __future__ import annotations

import datetime as dt

from engine.strategy import promotion_guard


def _month_ts(year: int, month: int, day: int) -> int:
    return int(dt.datetime(year, month, day, tzinfo=dt.timezone.utc).timestamp() * 1000)


def test_regime_fragile_challenger_fails_worst_era_gate(monkeypatch) -> None:
    monkeypatch.delenv("PROMOTION_MIN_WORST_ERA_SHARPE", raising=False)
    monkeypatch.setenv("PROMOTION_ERA_STD_LOG_ONLY", "1")
    timestamps = (
        [_month_ts(2025, 1, day) for day in (2, 8, 16, 24)]
        + [_month_ts(2025, 2, day) for day in (2, 8, 16, 24)]
        + [_month_ts(2025, 3, day) for day in (2, 8, 16, 24)]
    )
    challenger = [
        0.10,
        0.08,
        0.09,
        0.07,
        -0.05,
        -0.04,
        -0.06,
        -0.05,
        0.08,
        0.09,
        0.07,
        0.08,
    ]
    champion = [0.0] * len(challenger)

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="era_bad_AAPL_1700000000000_abcdef1",
        model_name="era_bad",
        challenger_returns=challenger,
        champion_returns=champion,
        evaluation_timestamps=timestamps,
        challenger_predictions=challenger,
        realized_returns=challenger,
        persist=False,
        alpha=1.0,
        bootstrap_samples=99,
    )

    gate = diagnostics["tests"]["era_regime_robustness"]
    assert not passed
    assert gate["applied"]
    assert gate["status"] == "worst_quartile_sharpe_below_threshold"
    assert gate["min_worst_era_sharpe_source"] == "champion_worst_quartile"
    assert gate["worst_quartile_sharpe"] < gate["champion_worst_quartile_sharpe"]


def test_consistent_challenger_passes_era_gate_with_regime_labels(monkeypatch) -> None:
    monkeypatch.delenv("PROMOTION_MIN_WORST_ERA_SHARPE", raising=False)
    monkeypatch.setenv("PROMOTION_ERA_STD_LOG_ONLY", "1")
    timestamps = (
        [_month_ts(2025, 1, day) for day in (2, 8, 16, 24)]
        + [_month_ts(2025, 2, day) for day in (2, 8, 16, 24)]
        + [_month_ts(2025, 3, day) for day in (2, 8, 16, 24)]
    )
    challenger = [
        0.020,
        0.015,
        0.025,
        0.018,
        0.021,
        0.016,
        0.024,
        0.019,
        0.022,
        0.017,
        0.023,
        0.020,
    ]
    champion = [0.0] * len(challenger)

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="era_good_AAPL_1700000000001_abcdef2",
        model_name="era_good",
        challenger_returns=challenger,
        champion_returns=champion,
        evaluation_timestamps=timestamps,
        regime_labels=["risk_on"] * 4 + ["risk_off"] * 4 + ["neutral"] * 4,
        challenger_predictions=challenger,
        realized_returns=challenger,
        persist=False,
        alpha=1.0,
        bootstrap_samples=99,
    )

    gate = diagnostics["tests"]["era_regime_robustness"]
    assert passed
    assert gate["applied"]
    assert gate["bucket_mode"] == "calendar_month+regime"
    assert gate["era_count"] == 3
    assert gate["worst_quartile_sharpe"] >= gate["champion_worst_quartile_sharpe"]
    assert all(row["ic"] is not None for row in gate["eras"])


def test_era_table_is_persisted_as_promotion_evidence(monkeypatch) -> None:
    monkeypatch.delenv("PROMOTION_MIN_WORST_ERA_SHARPE", raising=False)
    timestamps = (
        [_month_ts(2025, 1, day) for day in (2, 8, 16, 24)]
        + [_month_ts(2025, 2, day) for day in (2, 8, 16, 24)]
    )
    returns = [0.020, 0.015, 0.025, 0.018, 0.021, 0.016, 0.024, 0.019]
    calls: list[dict] = []

    def _capture_evidence(**kwargs):
        calls.append(dict(kwargs))
        return len(calls)

    monkeypatch.setattr(promotion_guard, "record_statistical_evidence", _capture_evidence)

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="era_persist_AAPL_1700000000002_abcdef3",
        model_name="era_persist",
        challenger_returns=returns,
        champion_returns=[0.0] * len(returns),
        evaluation_timestamps=timestamps,
        challenger_predictions=returns,
        realized_returns=returns,
        persist=True,
        alpha=1.0,
        bootstrap_samples=99,
    )

    payload_by_test = {str(call.get("test_name")): dict(call.get("payload") or {}) for call in calls}
    assert passed
    assert diagnostics["tests"]["era_regime_robustness"]["applied"]
    assert "era_regime_robustness" in payload_by_test
    assert payload_by_test["era_regime_robustness"]["eras"]
