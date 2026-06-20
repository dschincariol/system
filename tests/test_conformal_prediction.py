from __future__ import annotations

import numpy as np
import pytest

from engine.strategy.conformal import (
    conformal_quantile,
    evaluate_adaptive_coverage,
    score_interval_from_residuals,
)
from engine.strategy.model_intent import build_model_intent


def test_split_conformal_coverage_matches_target_on_known_noise(monkeypatch):
    monkeypatch.setenv("CONFORMAL_ALPHA", "0.2")
    rng = np.random.default_rng(101)
    calibration = np.abs(rng.normal(0.0, 1.0, size=6000))
    holdout = np.abs(rng.normal(0.0, 1.0, size=6000))

    q = conformal_quantile(calibration, 0.2)
    coverage = float(np.mean(holdout <= q))

    assert coverage == pytest.approx(0.8, abs=0.03)


def test_adaptive_conformal_recovers_after_variance_drift(monkeypatch):
    monkeypatch.setenv("CONFORMAL_ALPHA", "0.2")
    monkeypatch.setenv("CONFORMAL_ACI_GAMMA", "0.005")
    rng = np.random.default_rng(202)
    initial = np.abs(rng.normal(0.0, 1.0, size=250))
    shifted = np.abs(rng.normal(0.0, 2.0, size=1200))

    static_q = conformal_quantile(initial, 0.2)
    static_last_coverage = float(np.mean(shifted[-250:] <= static_q))
    adaptive = evaluate_adaptive_coverage(
        initial,
        shifted,
        alpha_target=0.2,
        gamma=0.005,
        window=250,
        min_history=40,
    )

    assert static_last_coverage < 0.7
    assert adaptive["last_window_coverage"] > static_last_coverage + 0.15
    assert adaptive["last_window_coverage"] == pytest.approx(0.8, abs=0.08)


def test_interval_geometry_produces_confidence_and_zero_exclusion(monkeypatch):
    monkeypatch.setenv("CONFORMAL_MODE", "gate_and_size")
    residuals = [0.1, 0.12, 0.15, 0.08, 0.09] * 20

    result = score_interval_from_residuals(1.0, residuals, alpha=0.2, group_key="unit")

    assert result["available"] is True
    assert result["interval_excludes_zero"] is True
    assert result["confidence"] > 0.0
    assert 0.0 < result["size_mult"] <= 1.0


def test_model_intent_carries_conformal_fields_and_overrides_confidence_in_gate(monkeypatch):
    monkeypatch.setenv("CONFORMAL_MODE", "gate")
    intent = build_model_intent(
        symbol="AAPL",
        horizon_s=3600,
        expected_z=1.0,
        confidence=0.9,
        explain={
            "conformal": {
                "available": True,
                "interval_excludes_zero": True,
                "interval_lower": 0.25,
                "interval_upper": 1.75,
                "interval_width": 1.5,
                "confidence": 0.42,
                "size_mult": 0.66,
            }
        },
    )

    assert intent["confidence"] == pytest.approx(0.42)
    assert intent["interval_excludes_zero"] is True
    assert intent["conformal_interval_lower"] == pytest.approx(0.25)
    assert intent["conformal_interval_upper"] == pytest.approx(1.75)
    assert intent["conformal_size_mult"] == pytest.approx(0.66)


def test_model_intent_carries_epistemic_uncertainty_for_sizing():
    intent = build_model_intent(
        symbol="AAPL",
        horizon_s=3600,
        expected_z=1.0,
        confidence=0.8,
        explain={
            "epistemic_uncertainty": 0.34,
            "aleatoric_uncertainty": 0.12,
            "uncertainty_ts_ms": 1_710_000_000_000,
        },
    )

    assert intent["epistemic_uncertainty"] == pytest.approx(0.34)
    assert intent["aleatoric_uncertainty"] == pytest.approx(0.12)
    assert intent["uncertainty_ts_ms"] == 1_710_000_000_000
    assert intent["uncertainty_detail"]["epistemic_uncertainty"] == pytest.approx(0.34)
