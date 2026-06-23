from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
]


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _sequence_dataset(n: int = 10, seq_len: int = 8, n_features: int = 3, n_horizons: int = 2):
    rng = np.random.default_rng(17)
    X = rng.normal(0.0, 1.0, size=(n, seq_len, n_features)).astype(np.float32)
    target0 = X[:, -3:, 0].mean(axis=1) - 0.25 * X[:, -2:, 1].mean(axis=1)
    target1 = X[:, -4:, 2].mean(axis=1) + 0.15 * X[:, -1, 0]
    y = np.stack([target0, target1], axis=1).astype(np.float32)
    return X, y


def _configure_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "itransformer.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")


def _fit_unit_model(module):
    X, y = _sequence_dataset()
    model = module.ITransformerRegressor(
        model_name="itransformer.unit",
        feature_ids=list(FEATURE_IDS),
        seq_len=8,
        n_horizons=2,
        n_layers=1,
        n_heads=2,
        d_model=8,
        dropout=0.0,
        seed=7,
        device="cpu",
    )
    model.fit(X[:8], y[:8], epochs=2, lr=0.01, weight_decay=0.0)
    return model, X, y


def test_itransformer_default_device_is_cpu_first(monkeypatch) -> None:
    module = importlib.import_module("engine.strategy.models.itransformer")
    monkeypatch.delenv("ITRANSFORMER_DEVICE", raising=False)
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("ITRANSFORMER_USE_CUDA", raising=False)
    monkeypatch.delenv("RUNTIME_HARDWARE_PROFILE", raising=False)
    monkeypatch.delenv("TRADING_DEPENDENCY_PROFILE", raising=False)
    monkeypatch.delenv("DEPENDENCY_PROFILE", raising=False)
    monkeypatch.delenv("RUNTIME_DEPENDENCY_PROFILE", raising=False)
    monkeypatch.setattr(module.torch.cuda, "is_available", lambda: True)

    assert module._default_device().type == "cpu"

    monkeypatch.setenv("ITRANSFORMER_DEVICE", "auto")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "nvidia")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "nvidia-cuda")
    assert module._default_device().type == "cuda"


def test_itransformer_shadow_registration_oos_and_champion_visibility(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    storage, registry, lifecycle, champion_manager, _oos_store, module = _reload_modules(
        "engine.runtime.storage",
        "engine.model_registry",
        "engine.strategy.model_lifecycle",
        "engine.strategy.champion_manager",
        "engine.strategy.ensemble.oos_store",
        "engine.strategy.models.itransformer",
    )
    storage.init_db()
    monkeypatch.setattr(module, "_expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    model, X, y = _fit_unit_model(module)
    preds = model.predict(X[8:])
    oos_rows = [
        {
            "symbol": "AAPL",
            "horizon": 3600,
            "family": module.FAMILY,
            "ts": 1_710_000_000_000 + idx,
            "run_id": "unit-oos",
            "prediction": float(np.asarray(preds[idx]).reshape(-1)[0]),
            "target": float(np.asarray(y[8 + idx]).reshape(-1)[0]),
        }
        for idx in range(2)
    ]

    result = module.register_shadow_model(model, symbol="AAPL", version="itransformer-test-v1", oos_predictions=oos_rows)

    assert result["stage"] == "shadow"
    assert result["metrics"]["model_family"] == module.FAMILY
    assert result["metrics"]["oos_prediction_count"] == 2
    assert result["metrics"]["feature_schema"]["feature_ids"] == FEATURE_IDS
    shadow = registry.get_stage_latest("itransformer.unit", "shadow", regime="global")
    assert shadow is not None
    assert shadow["model_kind"] == "itransformer"
    assert registry.get_stage_latest("itransformer.unit", "champion", regime="global") is None
    version = lifecycle.get_model_version("itransformer.unit", "itransformer-test-v1")
    assert version["stage"] == "shadow"
    assert version["live_ready"] is False

    con = storage.connect(readonly=True)
    try:
        oos_count = con.execute("SELECT COUNT(*) FROM model_oos_predictions WHERE family='itransformer'").fetchone()[0]
        score_row = con.execute(
            """
            SELECT stage, meta_json
            FROM model_marketplace_scores
            WHERE model_name='itransformer.unit' AND symbol='AAPL' AND horizon_s=3600
            LIMIT 1
            """
        ).fetchone()
    finally:
        con.close()
    assert int(oos_count) == 2
    assert score_row is not None
    meta = json.loads(score_row[1])
    assert score_row[0] == "shadow"
    assert meta["score_source"] == "model_oos_predictions"
    candidate = {
        "model_name": "itransformer.unit",
        "symbol": "AAPL",
        "horizon_s": 3600,
        "stage": "shadow",
        "meta": meta,
    }
    assert champion_manager._score_source_is_competition_candidate(meta) is True
    assert champion_manager._candidate_is_live_promotable(candidate) is False


def test_itransformer_artifact_roundtrip(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    storage, module = _reload_modules("engine.runtime.storage", "engine.strategy.models.itransformer")
    storage.init_db()
    monkeypatch.setattr(module, "_expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    model, X, _y = _fit_unit_model(module)

    manifest = module.persist_model_artifact(model, symbol="AAPL", version="itransformer-test-v1")
    loaded = module.load_model_from_artifact(alias=manifest["alias"], sha256=manifest["sha256"])

    np.testing.assert_array_equal(model.predict(X[:2]), loaded.predict(X[:2]))
    assert loaded.feature_schema["feature_ids"] == FEATURE_IDS


def test_itransformer_load_rejects_feature_schema_mismatch(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    _storage, module = _reload_modules("engine.runtime.storage", "engine.strategy.models.itransformer")
    monkeypatch.setattr(module, "_expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setattr(module, "_emit_config_feature_schema_load_failure", lambda **kwargs: None)
    model, _X, _y = _fit_unit_model(module)
    model_dir = model.save(tmp_path / "itransformer_schema_drift")
    config_path = model_dir / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    persisted_tag = config["feature_schema"]["feature_set_tag"]
    config["feature_schema"]["feature_set_tag"] = "tampered-itransformer-tag"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    try:
        module.ITransformerRegressor.load(model_dir)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("ITransformerRegressor.load accepted feature schema drift")

    assert "itransformer_feature_schema_drift" in message
    assert "tampered-itransformer-tag" in message
    assert persisted_tag not in "tampered-itransformer-tag"


def test_itransformer_shadow_stage_cannot_serve_via_predictor(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    storage, _oos_store, module, predictor = _reload_modules(
        "engine.runtime.storage",
        "engine.strategy.ensemble.oos_store",
        "engine.strategy.models.itransformer",
        "engine.strategy.predictor",
    )
    storage.init_db()
    monkeypatch.setattr(module, "_expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    model, _X, _y = _fit_unit_model(module)
    module.register_shadow_model(model, symbol="AAPL", version="itransformer-test-v1")

    def _fail_load(*args, **kwargs):
        raise AssertionError("shadow iTransformer artifact should not be loaded")

    monkeypatch.setattr(predictor, "load_itransformer_model_from_artifact", _fail_load)
    served = predictor._predict_via_itransformer_adapter(
        np.zeros((len(FEATURE_IDS),), dtype=np.float32),
        "AAPL",
        3600,
        event={"ts_ms": 1_710_000_000_000, "title": "", "body": "", "source": "unit"},
        active_model_name="itransformer.unit",
        active_model_version="itransformer-test-v1",
        active_family="itransformer",
        feature_ids=list(FEATURE_IDS),
        knn_z=0.0,
        wsum=0.0,
        knn_ex={},
        regime_at_trade="MID",
    )

    assert served is None


def test_itransformer_job_registered_shadow_default(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    job_registry = importlib.reload(importlib.import_module("engine.runtime.job_registry"))

    spec = job_registry.ALLOWED_JOBS["train_itransformer_models"]
    assert spec[0] == "engine/strategy/jobs/train_itransformer_models.py"
    assert spec[1] == "oneshot"
    assert spec[3]["execution"] is False
    assert spec[3]["default_stage"] == "shadow"


def test_itransformer_family_and_tuning_catalog_are_registered(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    registry, predictor, catalog = _reload_modules(
        "engine.model_registry",
        "engine.strategy.predictor",
        "engine.strategy.tuning.catalog",
    )

    family = registry.get_registered_model_family("itransformer")
    assert family["default_stage"] == "shadow"
    assert family["training_entrypoint"] == "engine.strategy.jobs.train_itransformer_models"
    assert "itransformer" in predictor.available_model_families()
    assert catalog.catalog_defaults("itransformer")["seq_len"] == 128
    assert "ITRANSFORMER_SEQ_LEN" in catalog.managed_env_names("itransformer")
