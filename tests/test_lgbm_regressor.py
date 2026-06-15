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


def _era_boost_dataset(seed=7, n_per=(120, 120, 24, 24)):
    rng = np.random.default_rng(seed)
    rows = []
    y = []
    labels = []
    ts_ms = []
    month_starts = [1_704_067_200_000, 1_706_745_600_000, 1_709_251_200_000, 1_711_929_600_000]
    for era_idx, n_rows in enumerate(n_per):
        sign = 1.0 if era_idx % 2 == 0 else -1.0
        regime = 1.0 if sign < 0 else 0.0
        for row_idx in range(int(n_rows)):
            x = float(rng.uniform(-1.0, 1.0))
            noise = float(rng.normal(0.0, 0.05))
            rows.append(
                {
                    FEATURE_IDS[0]: x,
                    FEATURE_IDS[1]: regime,
                    FEATURE_IDS[2]: x * regime,
                }
            )
            y.append((2.0 * sign * x) + (0.2 * regime) + noise)
            labels.append(f"era_{era_idx}")
            ts_ms.append(int(month_starts[era_idx] + row_idx * 1000))
    return rows, np.asarray(y, dtype=np.float32), labels, ts_ms


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


def test_lgbm_train_persist_load_predict_preserves_training_column_order(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    original_matrix_from_features = module._matrix_from_features
    observed_columns = []

    def spy_matrix_from_features(features, columns, *args, **kwargs):
        observed_columns.append(list(columns))
        return original_matrix_from_features(features, columns, *args, **kwargs)

    monkeypatch.setattr(module, "_matrix_from_features", spy_matrix_from_features)
    X, y = _dataset()
    model = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={"n_estimators": 12, "num_leaves": 7, "min_child_samples": 1, "learning_rate": 0.1},
        model_name="lgbm_regressor.order_roundtrip",
    )
    training_columns = list(observed_columns[0])
    path = model.save(tmp_path / "lgbm_order.joblib")
    loaded = module.LGBMRegressorModel.load(path)
    loaded.predict({"features": {FEATURE_IDS[0]: 0.2, FEATURE_IDS[1]: 0.4, FEATURE_IDS[2]: 1.0}})

    assert loaded.persisted_feature_schema["feature_ids"] == training_columns
    assert observed_columns[-1] == training_columns


def test_lgbm_expected_columns_sorts_unordered_feature_ids(monkeypatch):
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    monkeypatch.setattr(feature_registry, "expected_columns", lambda feature_ids=None, **kwargs: list(feature_ids or []))

    first = module._expected_columns(set(FEATURE_IDS))
    for _ in range(20):
        assert module._expected_columns(set(FEATURE_IDS)) == first


def test_lgbm_family_defaults_to_shadow_on_import():
    registry = importlib.import_module("engine.model_registry")
    family = registry.get_registered_model_family("lgbm_regressor")
    assert family["default_stage"] == "shadow"
    assert family["promotion_guard"].endswith("promotion_guard.assess_challenger")


def test_lgbm_era_boost_reduces_per_era_score_std_and_persists(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    era_boost = importlib.import_module("engine.strategy.era_boost")
    X, y, labels, _ts_ms = _era_boost_dataset()
    val_X, val_y, val_labels, _val_ts = _era_boost_dataset(seed=99, n_per=(30, 30, 30, 30))
    hyperparams = {
        "n_estimators": 4,
        "num_leaves": 3,
        "min_child_samples": 5,
        "learning_rate": 0.08,
        "random_state": 11,
        "deterministic": True,
        "force_col_wise": True,
        "n_jobs": 1,
        "verbosity": -1,
    }

    monkeypatch.delenv("LGBM_ERA_BOOST", raising=False)
    baseline = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams=dict(hyperparams),
        model_name="lgbm_regressor.era_baseline",
    )
    baseline_scores = era_boost.era_score_table(y, baseline.predict(X), labels, score_kind="neg_mse")
    baseline_std = era_boost.score_std(baseline_scores)
    baseline_mse = -float(np.mean([row["score"] for row in baseline_scores]))

    monkeypatch.setenv("LGBM_ERA_BOOST", "1")
    monkeypatch.setenv("LGBM_ERA_BOOST_ITERS", "4")
    monkeypatch.setenv("LGBM_ERA_BOOST_ROUNDS", "8")
    monkeypatch.setenv("ERA_BOOST_MAX_DEGRADE", "1.0")
    monkeypatch.setenv("LGBM_ERA_BOOST_WEIGHT_MULTIPLIER", "2.0")
    boosted = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams=dict(hyperparams),
        model_name="lgbm_regressor.era_boosted",
        era_labels=list(labels),
        validation_data=(val_X, val_y),
        validation_era_labels=list(val_labels),
    )
    payload = boosted.training_metrics["era_boost"]
    boosted_scores = era_boost.era_score_table(y, boosted.predict(X), labels, score_kind="neg_mse")
    boosted_std = era_boost.score_std(boosted_scores)
    boosted_mse = -float(np.mean([row["score"] for row in boosted_scores]))

    assert payload["applied"] is True
    assert boosted_std < baseline_std * 0.6
    assert boosted_mse <= baseline_mse * 1.05
    assert payload["before"]["era_scores"]
    assert payload["after"]["era_scores"]

    path = boosted.save(tmp_path / "era_boosted.joblib")
    loaded = module.LGBMRegressorModel.load(path)
    loaded_payload = loaded.training_metrics["era_boost"]
    assert loaded_payload["config"]["enabled"] is True
    assert loaded_payload["before"]["era_scores"]
    assert loaded_payload["after"]["era_scores"]


def test_lgbm_era_boost_is_deterministic_for_same_seed(monkeypatch):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setenv("LGBM_ERA_BOOST", "1")
    monkeypatch.setenv("LGBM_ERA_BOOST_ITERS", "3")
    monkeypatch.setenv("LGBM_ERA_BOOST_ROUNDS", "6")
    monkeypatch.setenv("ERA_BOOST_MAX_DEGRADE", "1.0")
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    X, y, labels, _ts_ms = _era_boost_dataset(seed=21)
    val_X, val_y, val_labels, _val_ts = _era_boost_dataset(seed=22, n_per=(20, 20, 20, 20))
    hyperparams = {
        "n_estimators": 5,
        "num_leaves": 3,
        "min_child_samples": 4,
        "learning_rate": 0.07,
        "random_state": 123,
        "deterministic": True,
        "force_col_wise": True,
        "n_jobs": 1,
        "verbosity": -1,
    }

    first = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams=dict(hyperparams),
        model_name="lgbm_regressor.era_det",
        era_labels=list(labels),
        validation_data=(val_X, val_y),
        validation_era_labels=list(val_labels),
    )
    second = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams=dict(hyperparams),
        model_name="lgbm_regressor.era_det",
        era_labels=list(labels),
        validation_data=(val_X, val_y),
        validation_era_labels=list(val_labels),
    )

    np.testing.assert_allclose(first.predict(X), second.predict(X), rtol=0.0, atol=1e-7)


def test_lgbm_era_boost_weights_use_training_eras_only(monkeypatch):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setenv("LGBM_ERA_BOOST", "1")
    monkeypatch.setenv("LGBM_ERA_BOOST_ITERS", "2")
    monkeypatch.setenv("LGBM_ERA_BOOST_ROUNDS", "4")
    monkeypatch.setenv("ERA_BOOST_MAX_DEGRADE", "1.0")
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    X, y, labels, _ts_ms = _era_boost_dataset(seed=31)
    val_X, val_y, _val_labels, _val_ts = _era_boost_dataset(seed=32, n_per=(20, 20, 20, 20))
    validation_only_labels = ["validation_only"] * len(val_y)

    model = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={
            "n_estimators": 4,
            "num_leaves": 3,
            "min_child_samples": 5,
            "learning_rate": 0.08,
            "random_state": 17,
            "deterministic": True,
            "force_col_wise": True,
            "n_jobs": 1,
            "verbosity": -1,
        },
        model_name="lgbm_regressor.era_no_leak",
        era_labels=list(labels),
        validation_data=(val_X, val_y),
        validation_era_labels=list(validation_only_labels),
    )

    payload = model.training_metrics["era_boost"]
    assert payload["label_diagnostics"]["bucket_mode"] == "explicit_era"
    assert payload["iterations"]
    for iteration in payload["iterations"]:
        assert "validation_only" not in set(iteration.get("weight_source_eras") or [])
