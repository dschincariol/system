from __future__ import annotations

import importlib

import numpy as np
import pytest

from engine.strategy import feature_registry

pytest.importorskip("lightgbm")


FEATURE_IDS = ["a", "b", "c"]


def _stub_training_config(monkeypatch, *, family: str, model_name: str, feature_ids: list[str]):
    model_config = importlib.import_module("engine.strategy.model_config")
    cfg = {
        "family": family,
        "model_name": model_name,
        "feature_ids": list(feature_ids),
        "horizons_s": [3600],
        "symbol_universe": ["*"],
    }
    monkeypatch.setattr(
        model_config,
        "get_model_config",
        lambda name, family=None: dict(cfg) if str(name) == model_name else {},
    )
    monkeypatch.setattr(model_config, "load_model_configs", lambda family=None, include_disabled=True: [dict(cfg)])


@pytest.mark.parametrize(
    ("family", "resolver"),
    [
        (
            "lgbm_regressor",
            lambda plan: importlib.import_module("engine.strategy.models.lgbm_regressor")._resolve_training_config(
                "lgbm_regressor",
                plan,
            ),
        ),
        (
            "xgb_regressor",
            lambda plan: importlib.import_module("engine.strategy.models.lgbm_regressor")._resolve_training_config(
                "xgb_regressor",
                plan,
            ),
        ),
        (
            "patchtst",
            lambda plan: importlib.import_module("engine.strategy.models.patchtst")._resolve_training_config(plan),
        ),
    ],
)
def test_retrain_feature_schema_change_requires_operator_ack(monkeypatch, family, resolver):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda feature_ids=None, **kwargs: list(feature_ids or []))
    tabular_module = importlib.import_module("engine.strategy.models.lgbm_regressor")
    model_name = f"{family}.schema_change"
    _stub_training_config(monkeypatch, family=family, model_name=model_name, feature_ids=["a", "b", "d"])
    monkeypatch.setattr(
        tabular_module,
        "_load_previous_feature_schema",
        lambda family_arg, model_name_arg: {"feature_set_tag": "previous-tag", "model_version": "old-version"},
    )
    monkeypatch.delenv("TS_ALLOW_SCHEMA_CHANGE", raising=False)

    with pytest.raises(RuntimeError, match="feature_schema_change_requires_ack"):
        resolver({"model_name": model_name})

    monkeypatch.setenv("TS_ALLOW_SCHEMA_CHANGE", "1")
    cfg = resolver({"model_name": model_name})

    assert cfg["feature_schema_changed"] is True
    assert cfg["feature_set_tag"] != "previous-tag"
    assert str(cfg["training_version_id"]).strip()
    assert cfg["model_version"] == cfg["training_version_id"]


def test_lgbm_load_rejects_feature_schema_drift(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.lgbm_regressor")

    X = [
        {"a": float(i), "b": float(i % 3), "c": float(i % 5)}
        for i in range(24)
    ]
    y = np.asarray(
        [(0.7 * row["a"]) - (1.2 * row["b"]) + (0.3 * row["c"]) for row in X],
        dtype=np.float32,
    )
    model = module.train_lgbm_regressor(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={
            "n_estimators": 8,
            "num_leaves": 4,
            "min_child_samples": 1,
            "learning_rate": 0.1,
        },
        model_name="lgbm_regressor.schema_drift",
    )
    path = model.save(tmp_path / "lgbm.joblib")

    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: ["a", "c", "b"])
    with pytest.raises(ValueError, match="feature_schema_drift"):
        module.LGBMRegressorModel.load(path)
