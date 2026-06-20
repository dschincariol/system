from __future__ import annotations

import importlib

import numpy as np

from engine.strategy import feature_registry

FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
]


def _dataset():
    rows = []
    y = []
    for i in range(28):
        f0 = float(i % 7) / 6.0
        f1 = float((27 - i) % 5) / 4.0
        f2 = 1.0 if i % 4 == 0 else 0.0
        rows.append({FEATURE_IDS[0]: f0, FEATURE_IDS[1]: f1, FEATURE_IDS[2]: f2})
        y.append((1.7 * f0) - (1.1 * f1) + (0.55 * f2))
    return rows, np.asarray(y, dtype=np.float32)


def test_xgb_fit_predict_save_load_schema_bound(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.xgb_regressor")
    X, y = _dataset()

    model = module.train_xgb_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={"n_estimators": 24, "max_depth": 2, "learning_rate": 0.1, "random_state": 11},
        model_name="xgb_regressor.unit",
    )
    sample = {"features": {FEATURE_IDS[0]: 0.9, FEATURE_IDS[1]: 0.1, FEATURE_IDS[2]: 1.0}}
    before = model.predict(sample)

    path = model.save(tmp_path / "xgb.joblib")
    loaded = module.XGBRegressorModel.load(path)
    after = loaded.predict(sample)
    np.testing.assert_array_equal(before, after)

    reversed_ids = list(reversed(FEATURE_IDS))
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: reversed_ids)
    reversed_pred = loaded.predict(sample)
    manual_matrix = np.asarray([[sample["features"][fid] for fid in reversed_ids]], dtype=np.float32)
    manual_pred = loaded.model.predict(manual_matrix)
    np.testing.assert_allclose(reversed_pred, manual_pred, rtol=0.0, atol=1e-7)
    assert not np.allclose(before, reversed_pred)


def test_xgb_family_defaults_to_shadow_on_import():
    registry = importlib.import_module("engine.model_registry")
    family = registry.get_registered_model_family("xgb_regressor")
    assert family["default_stage"] == "shadow"
    assert family["promotion_guard"].endswith("promotion_guard.assess_challenger")


def test_xgb_default_n_jobs_is_configurable_and_bounded(monkeypatch):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setenv("RUNTIME_WORKLOAD_PROFILE", "offline")
    monkeypatch.setenv("XGB_N_JOBS", "10")
    monkeypatch.setenv("MODEL_TRAIN_MAX_N_JOBS", "4")
    module = importlib.import_module("engine.strategy.models.xgb_regressor")

    model = module.XGBRegressorModel(feature_ids=list(FEATURE_IDS))

    assert model.hyperparams["n_jobs"] == 4
