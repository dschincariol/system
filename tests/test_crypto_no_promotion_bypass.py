from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from tests.promotion_test_helpers import passing_deconfounded_payload

pytestmark = pytest.mark.safety_critical


def test_crypto_challenger_still_fails_through_assess_challenger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "crypto_promotion_guard.db"))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    promotion_guard = importlib.reload(importlib.import_module("engine.strategy.promotion_guard"))
    storage.init_db()
    try:
        returns = [-0.002, -0.001, 0.0, -0.002] * 10
        passed, diagnostics = promotion_guard.assess_challenger(
            model_id="crypto_cost_adjusted_failing_candidate",
            model_name="crypto_cost_adjusted_failing_candidate",
            challenger_returns=returns,
            champion_returns=[0.0] * len(returns),
            candidate_symbols=["BTC"],
            deconfounded_validation=passing_deconfounded_payload(len(returns)),
            bootstrap_samples=199,
            random_state=17,
        )

        assert passed is False
        assert diagnostics["passed"] is False
        assert diagnostics["candidate_symbols"] == ["BTC"]
    finally:
        storage.close_pooled_connections()
