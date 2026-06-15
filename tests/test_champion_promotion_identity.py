from __future__ import annotations

from engine.strategy import champion_manager


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
