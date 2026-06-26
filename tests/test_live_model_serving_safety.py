from __future__ import annotations

from typing import Any


def _patch_serving_dependencies(monkeypatch, resolution: dict[str, Any]) -> None:
    import engine.runtime.live_ai_safety as safety
    from engine.strategy import predictor

    monkeypatch.setattr(predictor, "_live_model_resolution", lambda _symbol, _horizon_s: dict(resolution))
    monkeypatch.setattr(
        safety,
        "model_artifact_snapshot",
        lambda model_name: {"ok": True, "reason": "ok", "model_name": str(model_name)},
    )
    monkeypatch.setattr(
        safety,
        "model_feature_contract_snapshot",
        lambda model_name: {"ok": True, "reason": "ok", "model_name": str(model_name)},
    )


def _snapshot() -> dict[str, Any]:
    import engine.runtime.live_ai_safety as safety

    return safety.live_model_serving_snapshot(
        engine_mode="live",
        execution_mode="live",
        broker="ibkr",
        symbols=["AAPL"],
        horizons_s=[300],
    )


def test_live_model_serving_blocks_env_default_without_governed_champion(monkeypatch):
    monkeypatch.delenv("LIVE_ALLOW_ENV_DEFAULT_MODEL", raising=False)
    _patch_serving_dependencies(
        monkeypatch,
        {
            "requested_model_name": "embed_regressor.env_default",
            "resolved_model_name": "embed_regressor.env_default",
            "resolution_source": "env_default",
            "candidate_names": ["embed_regressor.env_default"],
            "env_default_fallback": True,
            "serve_fallback_active": False,
            "fallback_reason": "no_governed_champion_env_default",
        },
    )

    snapshot = _snapshot()

    assert snapshot["ok"] is False
    assert snapshot["allow_env_default_model"] is False
    assert "live_model_no_governed_champion" in snapshot["blockers"]
    assert snapshot["probes"][0]["env_default_fallback"] is True


def test_live_model_serving_allows_env_default_when_explicitly_enabled(monkeypatch):
    monkeypatch.setenv("LIVE_ALLOW_ENV_DEFAULT_MODEL", "1")
    _patch_serving_dependencies(
        monkeypatch,
        {
            "requested_model_name": "embed_regressor.env_default",
            "resolved_model_name": "embed_regressor.env_default",
            "resolution_source": "env_default",
            "candidate_names": ["embed_regressor.env_default"],
            "env_default_fallback": True,
            "serve_fallback_active": False,
            "fallback_reason": "no_governed_champion_env_default",
        },
    )

    snapshot = _snapshot()

    assert snapshot["ok"] is True
    assert snapshot["allow_env_default_model"] is True
    assert "live_model_no_governed_champion" not in snapshot["blockers"]
    assert snapshot["probes"][0]["env_default_fallback"] is True


def test_live_model_serving_does_not_block_governed_champion(monkeypatch):
    monkeypatch.delenv("LIVE_ALLOW_ENV_DEFAULT_MODEL", raising=False)
    _patch_serving_dependencies(
        monkeypatch,
        {
            "requested_model_name": "embed_regressor.governed_champion",
            "resolved_model_name": "embed_regressor.governed_champion",
            "resolution_source": "assignment",
            "candidate_names": ["embed_regressor.governed_champion", "embed_regressor.env_default"],
            "env_default_fallback": False,
            "serve_fallback_active": False,
            "fallback_reason": "",
        },
    )

    snapshot = _snapshot()

    assert snapshot["ok"] is True
    assert snapshot["allow_env_default_model"] is False
    assert "live_model_no_governed_champion" not in snapshot["blockers"]
    assert "env_default_fallback" not in snapshot["probes"][0]
