from __future__ import annotations

from engine.strategy.deconfounded_promotion import validate_deconfounded_signal
from engine.strategy import promotion_guard
from tests.promotion_test_helpers import (
    confounded_deconfounded_payload,
    passing_deconfounded_payload,
)


def test_deconfounded_validator_passes_stable_residual_effect() -> None:
    payload = passing_deconfounded_payload(72)
    result = validate_deconfounded_signal(
        candidate_signal=payload["candidate_signal"],
        outcome=payload["outcome"],
        controls=payload["controls"],
        stability_labels=payload["stability_labels"],
    )

    assert result["passed"] is True
    assert result["status"] == "evaluated"
    assert result["coefficient"] > 0.0
    assert result["p_value"] <= result["config"]["alpha"]
    assert result["residual_ic"] >= result["config"]["min_residual_ic"]
    assert result["controls_present"] == {
        "beta": True,
        "sector": True,
        "size": True,
        "volatility": True,
        "liquidity": True,
        "regime": True,
        "existing_model_exposure": True,
    }


def test_deconfounded_validator_blocks_signal_explained_by_confounders() -> None:
    payload = confounded_deconfounded_payload(72)
    result = validate_deconfounded_signal(
        candidate_signal=payload["candidate_signal"],
        outcome=payload["outcome"],
        controls=payload["controls"],
        stability_labels=payload["stability_labels"],
    )

    assert result["passed"] is False
    assert result["status"] == "signal_explained_by_confounders"
    assert result["explained_by_confounders"] is True
    assert result["signal_confounder_r2"] >= result["config"]["max_signal_confounder_r2"]


def test_deconfounded_validator_blocks_unstable_residual_effect() -> None:
    payload = passing_deconfounded_payload(90, effect=0.18)
    signal = list(payload["candidate_signal"])
    controls = list(payload["controls"])
    labels = ["early"] * 30 + ["middle"] * 30 + ["late"] * 30
    outcome = []
    for idx, value in enumerate(signal):
        sign = -1.0 if idx >= 60 else 1.0
        outcome.append(sign * 0.18 * float(value))

    result = validate_deconfounded_signal(
        candidate_signal=signal,
        outcome=outcome,
        controls=controls,
        stability_labels=labels,
        config={"min_stability_share": 0.75, "min_residual_ic": -1.0, "alpha": 1.0},
    )

    assert result["passed"] is False
    assert result["status"] == "unstable_incremental_effect"
    assert result["stability"]["positive_effect_share"] < 0.75


def test_assess_challenger_blocks_confounded_signal_gate() -> None:
    deconfounded = confounded_deconfounded_payload(60)
    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="confounded_AAPL_1700000000000_abcdef1",
        model_name="confounded_model",
        challenger_returns=[0.02] * 60,
        champion_returns=[0.0] * 60,
        deconfounded_validation=deconfounded,
        persist=False,
        alpha=1.0,
        bootstrap_samples=99,
    )

    gate = diagnostics["tests"]["deconfounded_signal_validation"]
    assert passed is False
    assert gate["passed"] is False
    assert gate["status"] == "signal_explained_by_confounders"
