from __future__ import annotations

import importlib

import numpy as np
import pytest

from engine.strategy import feature_registry
from engine.strategy.ood import build_ood_profile, ood_gate_from_payload, score_ood


FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
]


def test_knn_ood_score_flags_far_vector_and_accepts_training_like_vector(monkeypatch):
    monkeypatch.delenv("OOD_SUPPRESS_THRESHOLD", raising=False)
    monkeypatch.delenv("OOD_HARD_THRESHOLD", raising=False)
    monkeypatch.setenv("OOD_MODE", "suppress")
    rng = np.random.default_rng(7)
    X = rng.normal(0.0, 1.0, size=(160, len(FEATURE_IDS))).astype(np.float32)
    profile = build_ood_profile(X, FEATURE_IDS, max_reference_rows=120, k=5)

    inside = score_ood(profile, {"features": dict(zip(FEATURE_IDS, X[20].tolist()))})
    far_values = dict(zip(FEATURE_IDS, [10.0, -10.0, 10.0]))
    far = score_ood(profile, {"features": far_values})
    gate = ood_gate_from_payload({"ood": far})

    assert inside["available"] is True
    assert inside["ood_score"] < inside["threshold"]
    assert far["available"] is True
    assert far["ood_score"] > far["threshold"]
    assert far["range_violation_count"] == len(FEATURE_IDS)
    assert gate["applied"] is True
    assert gate["action"] in {"SIZE_COMPRESSION", "HARD_BLOCK"}
    assert gate["multiplier"] < 1.0


def test_lgbm_artifact_round_trips_ood_profile_and_scores_identically(monkeypatch, tmp_path):
    pytest.importorskip("lightgbm")
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    rng = np.random.default_rng(11)
    matrix = rng.normal(0.0, 1.0, size=(96, len(FEATURE_IDS))).astype(np.float32)
    rows = [dict(zip(FEATURE_IDS, row.tolist())) for row in matrix]
    y = (1.5 * matrix[:, 0] - 0.75 * matrix[:, 1] + 0.25 * matrix[:, 2]).astype(np.float32)

    model = module.train_lgbm_regressor(
        rows,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={"n_estimators": 8, "num_leaves": 7, "min_child_samples": 1, "learning_rate": 0.1},
        model_name="lgbm_regressor.ood_roundtrip",
    )
    sample = {"features": dict(rows[5])}
    before = model.score_ood(sample)
    loaded = module.LGBMRegressorModel.load(model.save(tmp_path / "lgbm_ood.joblib"))
    after = loaded.score_ood(sample)

    assert loaded.ood_profile["enabled"] is True
    assert loaded.training_metrics["ood_profile_summary"]["enabled"] is True
    for key in ("ood_score", "raw_distance", "threshold", "hard_threshold"):
        assert after[key] == pytest.approx(before[key], rel=0.0, abs=1.0e-7)
    assert after["latency_ms"] < 5.0
