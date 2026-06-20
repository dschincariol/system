from __future__ import annotations

from engine.strategy import promotion_guard
from tests.promotion_test_helpers import passing_deconfounded_payload


def _champion_returns() -> list[float]:
    return [0.04, -0.03, 0.03, -0.02] * 20


def test_clone_of_champion_is_rejected_by_pool_correlation_gate() -> None:
    champion = _champion_returns()
    challenger = list(champion)

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="clone_AAPL_1700000000000_abcdef1",
        model_name="clone_model",
        challenger_returns=challenger,
        champion_returns=champion,
        models_returns={"champion_model": champion, "clone_model": challenger},
        persist=False,
        alpha=1.0,
        bootstrap_samples=99,
        max_pool_corr=0.70,
        corr_sharpe_uplift=0.10,
    )

    corr_gate = diagnostics["tests"]["pool_correlation"]
    assert not passed
    assert corr_gate["applied"]
    assert corr_gate["max_correlation"] > 0.99
    assert corr_gate["status"] == "high_correlation_without_uplift"
    assert not corr_gate["passed"]


def test_decorrelated_additive_signal_passes_marginal_contribution_gate() -> None:
    champion = _champion_returns()
    challenger = [0.02, -0.005, -0.005, 0.02] * 20

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="additive_AAPL_1700000000001_abcdef2",
        model_name="additive_model",
        challenger_returns=challenger,
        champion_returns=champion,
        models_returns={"champion_model": champion, "additive_model": challenger},
        deconfounded_validation=passing_deconfounded_payload(len(challenger)),
        persist=False,
        alpha=1.0,
        bootstrap_samples=99,
        max_pool_corr=0.70,
        min_mpc=0.0,
    )

    corr_gate = diagnostics["tests"]["pool_correlation"]
    mpc_gate = diagnostics["tests"]["marginal_portfolio_contribution"]
    assert passed
    assert corr_gate["max_correlation"] < 0.70
    assert mpc_gate["applied"]
    assert mpc_gate["passed"]
    assert mpc_gate["marginal_sharpe"] > 0.0
    assert mpc_gate["marginal_pnl"] > 0.0


def test_correlated_challenger_with_sharpe_uplift_passes_correlation_override() -> None:
    champion = _champion_returns()
    challenger = [value + 0.01 for value in champion]

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="uplift_AAPL_1700000000002_abcdef3",
        model_name="uplift_model",
        challenger_returns=challenger,
        champion_returns=champion,
        models_returns={"champion_model": champion, "uplift_model": challenger},
        deconfounded_validation=passing_deconfounded_payload(len(challenger)),
        persist=False,
        alpha=1.0,
        bootstrap_samples=99,
        max_pool_corr=0.70,
        corr_sharpe_uplift=0.10,
    )

    corr_gate = diagnostics["tests"]["pool_correlation"]
    assert passed
    assert corr_gate["max_correlation"] > 0.99
    assert corr_gate["uplift_override"]
    assert corr_gate["challenger_sharpe"] >= corr_gate["required_uplift_sharpe"]
    assert corr_gate["passed"]


def test_pool_gate_metrics_are_persisted_as_promotion_evidence(monkeypatch) -> None:
    champion = _champion_returns()
    challenger = [value + 0.01 for value in champion]
    calls: list[dict] = []

    def _capture_evidence(**kwargs):
        calls.append(dict(kwargs))
        return len(calls)

    monkeypatch.setattr(promotion_guard, "record_statistical_evidence", _capture_evidence)

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="persist_AAPL_1700000000003_abcdef4",
        model_name="persist_model",
        challenger_returns=challenger,
        champion_returns=champion,
        models_returns={"champion_model": champion, "persist_model": challenger},
        deconfounded_validation=passing_deconfounded_payload(len(challenger)),
        persist=True,
        alpha=1.0,
        bootstrap_samples=99,
        max_pool_corr=0.70,
        corr_sharpe_uplift=0.10,
    )

    payload_by_test = {str(call.get("test_name")): dict(call.get("payload") or {}) for call in calls}
    assert passed
    assert diagnostics["tests"]["pool_correlation"]["applied"]
    assert diagnostics["tests"]["marginal_portfolio_contribution"]["applied"]
    assert "pool_correlation" in payload_by_test
    assert "marginal_portfolio_contribution" in payload_by_test
    assert payload_by_test["pool_correlation"]["max_correlation"] > 0.99
    assert payload_by_test["marginal_portfolio_contribution"]["marginal_sharpe"] > 0.0
