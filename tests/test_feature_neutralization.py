from __future__ import annotations

import numpy as np
import pytest

from engine.strategy import feature_neutralization
from engine.strategy import predictor
from engine.strategy import promotion_guard


def _feature_rows(values: np.ndarray, *, second_col: np.ndarray | None = None) -> list[dict[str, float]]:
    rows = []
    for idx, value in enumerate(values):
        row = {
            "tech.kama_slope": float(value),
            "tech.rv_20": float(abs(value) + 0.1),
        }
        if second_col is not None:
            row["price.momentum_1d"] = float(second_col[idx])
        rows.append(row)
    return rows


def test_feature_neutral_ic_removes_pure_momentum_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEUTRALIZE_FEATURE_IDS", "tech.kama_slope")
    momentum = np.linspace(-2.0, 2.0, 16)

    metrics = feature_neutralization.feature_neutral_ic(
        momentum.tolist(),
        momentum.tolist(),
        _feature_rows(momentum),
    )

    assert metrics["raw_ic"] > 0.99
    assert abs(metrics["fnc"]) < 1.0e-6
    assert metrics["raw_minus_fnc"] > 0.99


def test_feature_neutral_ic_preserves_orthogonal_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEUTRALIZE_FEATURE_IDS", "tech.kama_slope")
    feature = np.linspace(-1.0, 1.0, 20)
    raw_signal = np.sin(np.arange(20, dtype=float))
    signal = raw_signal - feature * (float(np.dot(raw_signal, feature)) / float(np.dot(feature, feature)))

    metrics = feature_neutralization.feature_neutral_ic(
        signal.tolist(),
        signal.tolist(),
        _feature_rows(feature),
    )

    assert metrics["raw_ic"] > 0.99
    assert metrics["fnc"] == pytest.approx(metrics["raw_ic"], abs=1.0e-6)


def test_neutralizer_is_stable_with_collinear_features(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEUTRALIZE_FEATURE_IDS", "tech.kama_slope,price.momentum_1d")
    x = np.linspace(-1.0, 1.0, 12)
    predictions = {f"S{idx:02d}": float(value) for idx, value in enumerate(x)}
    snapshots = {
        f"S{idx:02d}": {"tech.kama_slope": float(value), "price.momentum_1d": float(2.0 * value)}
        for idx, value in enumerate(x)
    }

    result = feature_neutralization.neutralize_predictions(
        predictions,
        snapshots,
        mode="serve",
        strength=1.0,
        ridge_lambda=1.0e-9,
    )

    assert result.applied
    assert all(np.isfinite(list(result.neutralized_predictions.values())))
    assert max(abs(value) for value in result.neutralized_predictions.values()) < 1.0e-5


def test_neutralizer_falls_back_for_small_cross_section(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEUTRALIZE_FEATURE_IDS", "tech.kama_slope")
    predictions = {f"S{idx:02d}": float(idx) for idx in range(7)}
    snapshots = {symbol: {"tech.kama_slope": float(idx)} for idx, symbol in enumerate(predictions)}

    result = feature_neutralization.neutralize_predictions(predictions, snapshots, mode="serve")

    assert not result.applied
    assert result.reason == "cross_section_too_small"
    assert result.neutralized_predictions == result.raw_predictions


def test_predictor_serve_mode_applies_existing_snapshot_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEUTRALIZE_MODE", "serve")
    monkeypatch.setenv("NEUTRALIZE_FEATURE_IDS", "tech.kama_slope")
    monkeypatch.setenv("NEUTRALIZE_STRENGTH", "1.0")
    symbols = [f"S{idx:02d}" for idx in range(8)]
    out = {}
    for idx, symbol in enumerate(symbols):
        value = float(idx - 3.5)
        out[(symbol, 60)] = (
            value,
            0.5,
            {"feature_snapshot": {"tech.kama_slope": value}, "feature_ids": ["tech.kama_slope"]},
        )

    updated = predictor._maybe_apply_feature_neutralization(out, symbols=symbols, horizon_s=60)

    served = [updated[(symbol, 60)][0] for symbol in symbols]
    assert max(abs(value) for value in served) < 1.0e-5
    for symbol in symbols:
        diag = updated[(symbol, 60)][2]["feature_neutralization"]
        assert diag["served"] is True
        assert diag["applied"] is True


def test_promotion_guard_flags_raw_ic_fnc_gap_log_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEUTRALIZE_FEATURE_IDS", "tech.kama_slope")
    monkeypatch.setenv("PROMOTION_MAX_FACTOR_DEPENDENCE", "0.25")
    momentum = np.linspace(-2.0, 2.0, 16)

    passed, diagnostics = promotion_guard.assess_challenger(
        model_id="feature_neutral_model",
        model_name="feature_neutral_model",
        candidate_version="v1",
        challenger_returns=momentum.tolist(),
        champion_returns=[0.0 for _ in momentum],
        challenger_predictions=momentum.tolist(),
        realized_returns=momentum.tolist(),
        neutralization_features=_feature_rows(momentum),
        bootstrap_samples=32,
        persist=False,
    )

    fnc = diagnostics["tests"]["feature_neutral_ic"]
    assert isinstance(passed, bool)
    assert fnc["applied"] is True
    assert fnc["flagged"] is True
    assert fnc["passed"] is True
    assert fnc["raw_minus_fnc"] > 0.25
