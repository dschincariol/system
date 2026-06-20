from __future__ import annotations

import pytest

from engine.strategy import champion_manager


def _gate_row(model_name: str, returns: list[float]) -> dict:
    return {
        "model_id": str(model_name),
        "model_name": str(model_name),
        "meta": {
            "model_version": "v1",
            "realized_trade_pnls": [float(value) for value in returns],
            "net_cost_label_count": len(returns),
            "net_cost_evidence_available": True,
            "net_cost_evidence": {"available": True, "n": len(returns)},
        },
    }


def test_statistical_promotion_gate_fails_closed_without_model_id(monkeypatch) -> None:
    monkeypatch.setenv("CHAMPION_PROMOTION_USE_STAT_GATE", "1")

    passed, diagnostics = champion_manager._evaluate_promotion_stat_gate(
        {
            "model_name": "shared_family",
            "meta": {"model_version": "v1"},
            "metrics": {},
        },
        n_competing_trials=1,
    )

    assert not passed
    assert diagnostics["status"] == "missing_model_id"
    assert diagnostics["model_name"] == "shared_family"
    assert diagnostics["candidate_version"] == "v1"


@pytest.mark.parametrize("mode_name", ["live", "paper"])
def test_statistical_promotion_gate_blocks_low_observations_without_incumbent(monkeypatch, mode_name: str) -> None:
    monkeypatch.setenv("ENGINE_MODE", mode_name)
    monkeypatch.setenv("CHAMPION_PROMOTION_USE_STAT_GATE", "0")
    monkeypatch.setenv("CPCV_ENABLED", "0")
    monkeypatch.setenv("CHAMPION_PROMOTION_MIN_OBSERVATIONS", "5")

    passed, diagnostics = champion_manager._evaluate_promotion_stat_gate(
        _gate_row(f"{mode_name}_low_obs", [0.1, 0.1, 0.1, 0.1]),
        n_competing_trials=1,
    )

    assert not passed
    assert diagnostics["status"] == "insufficient_observations"
    assert diagnostics["promotion_mode"] == mode_name
    assert diagnostics["current_observations"] == 4
    assert diagnostics["required_observations"] == 5
    assert diagnostics["champion_observations"] == 0
    assert "insufficient_observations" in diagnostics["blockers"]


@pytest.mark.parametrize("mode_name", ["live", "paper"])
def test_statistical_promotion_gate_blocks_low_observations_with_incumbent(monkeypatch, mode_name: str) -> None:
    monkeypatch.setenv("ENGINE_MODE", mode_name)
    monkeypatch.setenv("CHAMPION_PROMOTION_USE_STAT_GATE", "0")
    monkeypatch.setenv("CPCV_ENABLED", "0")
    monkeypatch.setenv("CHAMPION_PROMOTION_MIN_OBSERVATIONS", "5")

    passed, diagnostics = champion_manager._evaluate_promotion_stat_gate(
        _gate_row(f"{mode_name}_low_obs", [0.2, 0.2, 0.2, 0.2]),
        n_competing_trials=1,
        champion_row=_gate_row(f"{mode_name}_incumbent", [0.0, 0.0, 0.0, 0.0]),
    )

    assert not passed
    assert diagnostics["status"] == "insufficient_observations"
    assert diagnostics["current_observations"] == 4
    assert diagnostics["required_observations"] == 5
    assert diagnostics["champion_observations"] == 4


@pytest.mark.parametrize("mode_name", ["safe", "shadow"])
def test_statistical_promotion_gate_keeps_low_observation_advisory_in_non_strict_modes(
    monkeypatch,
    mode_name: str,
) -> None:
    monkeypatch.setenv("ENGINE_MODE", mode_name)
    monkeypatch.delenv("ENV", raising=False)
    monkeypatch.delenv("NODE_ENV", raising=False)
    monkeypatch.delenv("ENGINE_SUPERVISED", raising=False)
    monkeypatch.setenv("CHAMPION_PROMOTION_USE_STAT_GATE", "0")
    monkeypatch.setenv("CPCV_ENABLED", "0")
    monkeypatch.setenv("CHAMPION_PROMOTION_MIN_OBSERVATIONS", "5")

    passed, diagnostics = champion_manager._evaluate_promotion_stat_gate(
        _gate_row(f"{mode_name}_low_obs", [0.1, 0.1, 0.1, 0.1]),
        n_competing_trials=1,
    )

    assert passed
    assert diagnostics["status"] == "insufficient_observations_advisory"
    assert diagnostics["promotion_mode"] == mode_name
    assert diagnostics["advisory"] is True
