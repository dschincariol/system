from __future__ import annotations

import importlib

import numpy as np
import pytest

from engine.strategy import feature_registry

pytest.importorskip("lightgbm")


FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
]


def _dataset():
    rows = []
    y = []
    for i in range(24):
        f0 = float(i % 6) / 5.0
        f1 = float((23 - i) % 7) / 6.0
        f2 = 1.0 if i % 3 == 0 else 0.0
        rows.append({FEATURE_IDS[0]: f0, FEATURE_IDS[1]: f1, FEATURE_IDS[2]: f2})
        y.append((2.0 * f0) - (1.25 * f1) + (0.4 * f2))
    return rows, np.asarray(y, dtype=np.float32)


def test_lgbm_fit_predict_save_load_schema_bound(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    X, y = _dataset()

    model = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={"n_estimators": 24, "num_leaves": 7, "min_child_samples": 1, "learning_rate": 0.1},
        model_name="lgbm_regressor.unit",
    )
    sample = {"features": {FEATURE_IDS[0]: 0.9, FEATURE_IDS[1]: 0.1, FEATURE_IDS[2]: 1.0}}
    before = model.predict(sample)

    path = model.save(tmp_path / "lgbm.joblib")
    loaded = module.LGBMRegressorModel.load(path)
    after = loaded.predict(sample)
    np.testing.assert_array_equal(before, after)

    reversed_ids = list(reversed(FEATURE_IDS))
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: reversed_ids)
    reversed_pred = loaded.predict(sample)
    manual_matrix = np.asarray([[sample["features"][fid] for fid in reversed_ids]], dtype=np.float32)
    manual_pred = loaded.model.predict(manual_matrix)
    np.testing.assert_allclose(reversed_pred, manual_pred, rtol=0.0, atol=1e-7)
    assert not np.allclose(before, reversed_pred)


def test_lgbm_family_defaults_to_shadow_on_import():
    registry = importlib.import_module("engine.model_registry")
    family = registry.get_registered_model_family("lgbm_regressor")
    assert family["default_stage"] == "shadow"
    assert family["promotion_guard"].endswith("promotion_guard.assess_challenger")
