from __future__ import annotations

import numpy as np
import pytest

from engine.strategy.conformal import (
    calibrate_conformal_risk_control,
    conformal_gate_from_payload,
    conformal_quantile,
    evaluate_adaptive_coverage,
    extract_conformal_payload,
    score_interval_from_residuals,
    score_interval_from_quantile_forecast,
)
from engine.strategy.model_intent import build_model_intent
from engine.strategy.uncertainty_sizing import uncertainty_gate_from_payload


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


def test_cqr_asymmetric_intervals_use_quantile_forecast(monkeypatch):
    monkeypatch.setenv("CONFORMAL_MIN_RESIDUALS", "5")
    scores = [0.02, 0.04, 0.06, 0.08, 0.10] * 8
    qhat = conformal_quantile(scores, 0.2)

    result = score_interval_from_quantile_forecast(
        {"lower": -0.15, "median": 0.35, "upper": 1.25, "lower_quantile": 0.1, "upper_quantile": 0.9},
        scores,
        alpha=0.2,
        group_key="unit:cqr",
    )

    assert result["available"] is True
    assert result["method"] == "conformalized_quantile_regression"
    assert result["interval_lower"] == pytest.approx(-0.15 - qhat)
    assert result["interval_upper"] == pytest.approx(1.25 + qhat)
    assert (result["prediction"] - result["interval_lower"]) != pytest.approx(
        result["interval_upper"] - result["prediction"]
    )
    assert result["calibration"]["cqr_sample_size"] == len(scores)


def test_symmetric_fallback_payload_remains_backward_compatible(monkeypatch):
    monkeypatch.setenv("CONFORMAL_MIN_RESIDUALS", "5")
    result = score_interval_from_residuals(1.0, [0.1, 0.2, 0.15, 0.12, 0.18] * 4, alpha=0.2)

    extracted = extract_conformal_payload({"conformal": result})
    gate = conformal_gate_from_payload({"conformal": result})

    assert result["method"] == "split_conformal_aci"
    assert extracted["interval_lower"] == pytest.approx(result["interval_lower"])
    assert "interval" in result
    assert "calibration" in result
    assert gate["hard_block"] is False
    assert gate["size_mult"] == pytest.approx(result["size_mult"])


def test_risk_control_calibrates_monotone_loss_and_recommends_size_compression(monkeypatch):
    monkeypatch.setenv("CONFORMAL_RISK_CONTROL_ENABLED", "1")
    monkeypatch.setenv("CONFORMAL_RISK_CONTROL_MODE", "size_compress")
    monkeypatch.setenv("CONFORMAL_RISK_MIN_SAMPLES", "10")
    losses = [0.10] * 50 + [0.20] * 30 + [0.50] * 20
    threshold = conformal_quantile(losses, 0.2)

    result = calibrate_conformal_risk_control(
        losses,
        target_risk=0.2,
        loss_definition="accepted_trade_loss",
        current_loss=threshold * 1.10,
        min_samples=10,
    )

    assert result["available"] is True
    assert result["loss_definition"] == "accepted_trade_loss"
    assert result["target_risk"] == pytest.approx(0.2)
    assert result["calibrated_loss_threshold"] == pytest.approx(threshold)
    assert result["recommended_action"] == "size_compress"
    assert 0.0 < result["size_mult"] < 1.0


def test_dtaci_controller_adapts_across_regime_shift(monkeypatch):
    monkeypatch.setenv("CONFORMAL_ADAPTIVE_CONTROLLER", "dtaci")
    rng = np.random.default_rng(303)
    initial = np.abs(rng.normal(0.0, 0.6, size=180))
    shifted = np.abs(rng.normal(0.0, 2.0, size=700))
    static_q = conformal_quantile(initial, 0.2)
    static_last_coverage = float(np.mean(shifted[-180:] <= static_q))

    adaptive = evaluate_adaptive_coverage(
        initial,
        shifted,
        alpha_target=0.2,
        gamma=0.004,
        window=180,
        min_history=30,
        controller="dtaci",
    )

    assert adaptive["controller"] == "dtaci"
    assert adaptive["last_window_coverage"] > static_last_coverage + 0.10
    assert adaptive["controller_state"]["gamma_multiplier_max"] >= 1.0


def test_insufficient_cqr_calibration_falls_back_to_symmetric_path(monkeypatch):
    monkeypatch.setenv("CONFORMAL_MIN_RESIDUALS", "5")
    cqr = score_interval_from_quantile_forecast(
        {"lower": -0.2, "median": 0.4, "upper": 0.8},
        [0.01, 0.02],
        alpha=0.2,
    )
    symmetric = score_interval_from_residuals(0.4, [0.1, 0.2, 0.15, 0.12, 0.18], alpha=0.2)

    assert cqr["available"] is False
    assert cqr["reason"] == "insufficient_cqr_calibration_scores"
    assert symmetric["available"] is True
    assert symmetric["method"] == "split_conformal_aci"


def test_legacy_flat_payload_extracts_without_schema_break(monkeypatch):
    monkeypatch.setenv("CONFORMAL_MODE", "gate_and_size")
    payload = {
        "conformal_interval_excludes_zero": True,
        "conformal_interval_lower": 0.25,
        "conformal_interval_upper": 1.75,
        "conformal_interval_width": 1.50,
        "conformal_confidence": 0.60,
        "conformal_size_mult": 0.70,
    }

    extracted = extract_conformal_payload(payload)
    gate = conformal_gate_from_payload(payload)

    assert extracted["available"] is True
    assert extracted["interval_lower"] == pytest.approx(0.25)
    assert extracted["size_mult"] == pytest.approx(0.70)
    assert gate["hard_block"] is False
    assert gate["action"] == "NONE"


def test_model_conformal_output_cannot_bypass_uncertainty_gate(monkeypatch):
    monkeypatch.setenv("UNCERTAINTY_SIZING_MODE", "enforce")
    monkeypatch.setenv("UNCERTAINTY_HIGH_THRESHOLD", "0.50")
    monkeypatch.setenv("UNCERTAINTY_HARD_THRESHOLD", "0.90")
    monkeypatch.setenv("UNCERTAINTY_MAX_AGE_MS", "300000")
    now_ms = 1_710_000_000_000
    payload = {
        "signal_ts_ms": now_ms,
        "conformal": {
            "available": True,
            "interval_excludes_zero": True,
            "interval_lower": 0.2,
            "interval_upper": 1.2,
            "interval_width": 1.0,
            "size_mult": 1.0,
            "recommended_action": "log_only",
        },
        "model_intent": {
            "epistemic_uncertainty": 0.95,
            "uncertainty_ts_ms": now_ms,
        },
    }

    gate = uncertainty_gate_from_payload(payload, execution_mode="paper", broker="sim", now_ms=now_ms)

    assert gate["hard_block"] is True
    assert gate["action"] == "HARD_BLOCK"
    assert "high_model_uncertainty" in gate["reasons"]


def test_live_conformal_risk_control_blocks_when_thresholds_missing(monkeypatch):
    from engine.runtime.live_ai_safety import live_uncertainty_threshold_snapshot

    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("EXECUTION_MODE", "live")
    monkeypatch.setenv("BROKER", "ibkr")
    monkeypatch.setenv("UNCERTAINTY_SIZING_PRODUCTION_POLICY", "strict")
    monkeypatch.setenv("UNCERTAINTY_HIGH_THRESHOLD", "0.70")
    monkeypatch.setenv("UNCERTAINTY_HARD_THRESHOLD", "0.95")
    monkeypatch.setenv("UNCERTAINTY_MAX_AGE_MS", "300000")
    monkeypatch.setenv("OOD_SUPPRESS_THRESHOLD", "1.50")
    monkeypatch.setenv("OOD_HARD_THRESHOLD", "3.00")
    monkeypatch.setenv("CONFORMAL_RISK_CONTROL_ENABLED", "1")
    monkeypatch.setenv("CONFORMAL_RISK_CONTROL_MODE", "suppress")
    monkeypatch.setenv("CONFORMAL_RISK_LOSS", "accepted_trade_loss")
    monkeypatch.delenv("CONFORMAL_RISK_TARGET", raising=False)
    monkeypatch.delenv("CONFORMAL_RISK_MIN_SAMPLES", raising=False)

    snapshot = live_uncertainty_threshold_snapshot(engine_mode="live", execution_mode="live", broker="ibkr")

    assert snapshot["ok"] is False
    assert "live_conformal_risk_threshold_missing:CONFORMAL_RISK_TARGET" in snapshot["blockers"]
    assert snapshot["conformal_risk_control"]["enabled"] is True


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
