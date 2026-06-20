from __future__ import annotations

from engine.execution.contextual_bandit_slicer import (
    ALLOWED_PARAMETER_FIELDS,
    POLICY_NAME,
    build_constraints,
    build_context,
    evaluate_against_baselines,
    select_execution_adjustment,
)


def test_contextual_bandit_outputs_only_bounded_execution_parameters() -> None:
    constraints = build_constraints(
        order={"symbol": "AAPL", "qty": 100.0},
        symbol="AAPL",
        side="BUY",
        parent_qty=100.0,
        parent_id="parent-1",
        base_slice_pct=0.25,
        base_participation=0.04,
        base_slice_interval_ms=250,
        base_entry_delay_ms=50,
        max_slices=25,
    )
    context = build_context(
        order={
            "symbol": "AAPL",
            "qty": 100.0,
            "true_spread_bps": 30.0,
            "intraday_vol_bps": 90.0,
            "adverse_selection_bps": 18.0,
            "fill_risk": 0.05,
        },
    )

    decision = select_execution_adjustment(context=context, constraints=constraints)

    assert decision.policy_name == POLICY_NAME
    assert decision.policy_scope == "execution_only"
    assert set(decision.parameters) == set(ALLOWED_PARAMETER_FIELDS)
    assert constraints.min_slice_pct <= float(decision.parameters["slice_pct"]) <= constraints.max_slice_pct
    assert float(decision.parameters["slice_pct"]) <= constraints.base_slice_pct
    assert constraints.min_participation <= float(decision.parameters["target_participation"]) <= constraints.max_participation
    assert int(decision.parameters["slice_interval_ms"]) >= constraints.base_slice_interval_ms
    assert int(decision.parameters["entry_delay_ms"]) >= constraints.base_entry_delay_ms


def test_execution_slicing_evaluator_reports_required_baseline_metrics() -> None:
    report = evaluate_against_baselines(
        [
            {
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 50.0,
                "base_slice_pct": 0.20,
                "context": {
                    "adverse_selection_bps": 5.0,
                    "fill_risk": 0.15,
                },
                "feedback": {
                    "spread_bps": 8.0,
                    "slippage_bps": 4.0,
                    "volatility_bps": 45.0,
                },
            },
            {
                "symbol": "MSFT",
                "side": "SELL",
                "qty": -25.0,
                "base_slice_pct": 0.15,
                "feedback": {
                    "spread_bps": 14.0,
                    "slippage_bps": 9.0,
                    "adverse_selection_bps": 7.0,
                    "fill_risk": 0.2,
                },
            },
        ]
    )

    assert report["ok"] is True
    assert set(report["summary"]) == {"learned", "twap", "vwap", "pov", "adaptive"}
    for metrics in report["summary"].values():
        assert metrics["n"] == 2.0
        for key in (
            "implementation_shortfall_bps",
            "slippage_bps",
            "fill_risk",
            "adverse_selection_bps",
        ):
            assert key in metrics
            assert float(metrics[key]) >= 0.0


def test_router_rejects_direct_learned_policy_order_without_epe_guard() -> None:
    from engine.execution import broker_router

    result = broker_router.apply_new_portfolio_orders_router(
        dry_run=True,
        override_orders=[
            {
                "symbol": "AAPL",
                "side": "BUY",
                "qty": 1.0,
                "source": "learned_execution.contextual_bandit",
                "learned_execution_policy": POLICY_NAME,
            }
        ],
    )

    assert result["ok"] is False
    assert result["status"] == "learned_execution_policy_forbidden"
    assert result["blocked_orders"][0]["reason"] == "execution_policy_not_locked"
