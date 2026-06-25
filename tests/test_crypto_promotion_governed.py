from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from engine.strategy import champion_manager
from tests.promotion_test_helpers import passing_deconfounded_payload


def _gate_row(model_name: str, returns: list[float]) -> dict:
    return {
        "model_id": str(model_name),
        "model_name": str(model_name),
        "metrics": {"model_id": str(model_name)},
        "meta": {
            "model_version": "crypto-v1",
            "realized_trade_pnls": [float(value) for value in returns],
            "net_cost_label_count": len(returns),
            "net_cost_evidence_available": True,
            "net_cost_evidence": {"available": True, "n": len(returns)},
        },
    }


def test_crypto_ranker_failing_challenger_returns_false_via_assess_challenger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "crypto_ranker_promotion_guard.db"))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    promotion_guard = importlib.reload(importlib.import_module("engine.strategy.promotion_guard"))
    storage.init_db()
    try:
        returns = [-0.002, -0.001, 0.0, -0.002] * 10
        passed, diagnostics = promotion_guard.assess_challenger(
            model_id="crypto_ranker_failing_candidate",
            model_name="crypto_ranker_failing_candidate",
            candidate_version="crypto-v1",
            challenger_returns=returns,
            champion_returns=[0.0] * len(returns),
            candidate_symbols=["BTC", "ETH"],
            deconfounded_validation=passing_deconfounded_payload(len(returns)),
            bootstrap_samples=199,
            random_state=17,
        )

        assert passed is False
        assert diagnostics["passed"] is False
        assert diagnostics["candidate_symbols"] == ["BTC", "ETH"]
        assert diagnostics["tests"]["white_reality_check"]["passed"] is False
    finally:
        storage.close_pooled_connections()


def test_champion_manager_stat_gate_blocks_failing_crypto_ranker(monkeypatch) -> None:
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("CHAMPION_PROMOTION_USE_STAT_GATE", "1")
    monkeypatch.setenv("CPCV_ENABLED", "0")
    monkeypatch.setenv("CHAMPION_PROMOTION_MIN_OBSERVATIONS", "5")

    passed, diagnostics = champion_manager._evaluate_promotion_stat_gate(
        _gate_row("crypto_ranker_failing_candidate", [-0.01] * 20),
        n_competing_trials=1,
        champion_row=_gate_row("crypto_ranker_incumbent", [0.0] * 20),
    )

    assert passed is False
    assert diagnostics["passed"] is False
