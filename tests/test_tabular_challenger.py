from __future__ import annotations

import builtins
import importlib
import json
from pathlib import Path

import numpy as np
import pytest

from engine.strategy import feature_registry

FEATURE_IDS = ["base.source_credibility", "base.log_recency_hours", "macro.vix_close"]


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _configure_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "tabular_challenger.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_ARTIFACTS_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    monkeypatch.setenv("RUNTIME_WORKLOAD_PROFILE", "offline")
    monkeypatch.setenv("ALLOW_TRAINING", "1")


def _rows(n: int = 24):
    out = []
    y = []
    for idx in range(n):
        x0 = float(idx) / 10.0
        x1 = float((idx % 5) - 2)
        x2 = float((idx % 3) - 1)
        out.append({FEATURE_IDS[0]: x0, FEATURE_IDS[1]: x1, FEATURE_IDS[2]: x2})
        y.append(1.5 * x0 - 0.25 * x1 + 0.1 * x2)
    return out, np.asarray(y, dtype=np.float32)


def test_optional_tabpfn_absence_fails_closed(monkeypatch):
    module = importlib.import_module("engine.strategy.models.tabular_challenger")
    monkeypatch.setenv("TABULAR_CHALLENGER_LICENSE_REVIEW_ACK", "1")
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if str(name).startswith("tabpfn"):
            raise ImportError("unit missing tabpfn")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    X, y = _rows(6)
    model = module.TabularFoundationChallengerModel(
        model_name="tabular_foundation_challenger.unit",
        feature_ids=list(FEATURE_IDS),
        backend="tabpfn",
        config={"fallback_policy": "fail_closed"},
    )

    with pytest.raises(module.OptionalDependencyMissing):
        model.fit(X, y)


def test_feature_schema_persistence_and_load_parity(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    storage, module = _reload_modules("engine.runtime.storage", "engine.strategy.models.tabular_challenger")
    storage.init_db()
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    X, y = _rows(12)
    model = module.train_tabular_challenger(
        X,
        y,
        feature_ids=list(FEATURE_IDS),
        backend="fake",
        model_name="tabular_foundation_challenger.schema",
    )
    manifest = module.persist_model_artifact(model, symbol="AAPL", version="schema-v1")

    loaded = module.load_model_from_artifact(alias=manifest["alias"], sha256=manifest["sha256"])
    np.testing.assert_allclose(model.predict(X[:3]), loaded.predict(X[:3]), rtol=1e-6, atol=1e-6)
    assert loaded.feature_schema["feature_ids"] == FEATURE_IDS

    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(reversed(FEATURE_IDS)))
    with pytest.raises(ValueError, match="feature_schema_drift"):
        module.load_model_from_artifact(alias=manifest["alias"], sha256=manifest["sha256"])


def test_deterministic_fake_registration_writes_oos_predictions(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    storage, registry, lifecycle, module = _reload_modules(
        "engine.runtime.storage",
        "engine.model_registry",
        "engine.strategy.model_lifecycle",
        "engine.strategy.models.tabular_challenger",
    )
    storage.init_db()
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    X, y = _rows(16)
    model = module.train_tabular_challenger(
        X[:12],
        y[:12],
        feature_ids=list(FEATURE_IDS),
        backend="fake",
        model_name="tabular_foundation_challenger.oos",
    )
    preds = model.predict(X[12:])
    oos_rows = [
        {
            "symbol": "AAPL",
            "horizon": 3600,
            "family": module.FAMILY,
            "ts": 1_720_000_000_000 + idx,
            "run_id": "unit-oos",
            "prediction": float(preds[idx]),
            "target": float(y[12 + idx]),
        }
        for idx in range(4)
    ]

    result = module.register_shadow_model(
        model,
        symbol="AAPL",
        version="tabular-unit-v1",
        training_window={"start_ts_ms": 1, "end_ts_ms": 2, "lookback_days": 1},
        oos_predictions=oos_rows,
    )

    assert result["stage"] == "shadow"
    assert result["metrics"]["oos_prediction_count"] == 4
    assert result["metrics"]["feature_schema"]["feature_ids"] == FEATURE_IDS
    assert result["metrics"]["model_card"]["primary_baselines"] == ["lightgbm", "xgboost", "sklearn_gbm"]
    assert registry.get_stage_latest("tabular_foundation_challenger.oos", "shadow", regime="global") is not None
    version = lifecycle.get_model_version("tabular_foundation_challenger.oos", "tabular-unit-v1")
    assert version["stage"] == "shadow"
    assert version["live_ready"] is False
    assert dict(version["train_scope"])["promotion_requirements"]["require_cpcv"] is True

    con = storage.connect(readonly=True)
    try:
        count = con.execute(
            "SELECT COUNT(*) FROM model_oos_predictions WHERE family=? AND run_id=?",
            (module.FAMILY, "unit-oos"),
        ).fetchone()[0]
    finally:
        con.close()
    assert int(count) == 4


def test_training_job_insufficient_sample_noop(monkeypatch, capsys):
    module = importlib.reload(importlib.import_module("engine.strategy.models.tabular_challenger"))
    monkeypatch.setattr(module, "assert_offline_work_allowed", lambda **kwargs: None)
    monkeypatch.setattr(module, "init_db", lambda: None)
    monkeypatch.setattr(module, "load_lifecycle_plan", lambda family: {})
    monkeypatch.setattr(
        module,
        "_resolve_challenger_training_config",
        lambda plan: {
            "enabled": True,
            "model_name": "tabular_foundation_challenger.small",
            "feature_ids": list(FEATURE_IDS),
            "symbol_universe": ["*"],
            "training_window_days": 30,
            "horizon_s": 3600,
            "backend": "fake",
            "task": "regression",
            "fallback_policy": "fail_closed",
        },
    )
    monkeypatch.setattr(
        module,
        "_load_training_rows",
        lambda **kwargs: ([{FEATURE_IDS[0]: 1.0}], [0.1], [{"symbol": "AAPL", "ts": 1, "horizon": 3600}]),
    )
    monkeypatch.setenv("TABULAR_CHALLENGER_MIN_SAMPLES", "5")

    rc = module.run_training_job()

    assert rc == 0
    assert "insufficient_samples n=1 min_required=5" in capsys.readouterr().out


def test_artifact_load_failure_fails_closed(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    storage, module = _reload_modules("engine.runtime.storage", "engine.strategy.models.tabular_challenger")
    storage.init_db()

    with pytest.raises(FileNotFoundError, match="tabular_challenger_artifact_not_found"):
        module.load_model_from_artifact(alias="model:tabular_foundation_challenger:missing:*:current")


def test_family_registration_job_and_predictor_visibility(monkeypatch, tmp_path):
    _configure_env(monkeypatch, tmp_path)
    registry, predictor, job_registry, catalog = _reload_modules(
        "engine.model_registry",
        "engine.strategy.predictor",
        "engine.runtime.job_registry",
        "engine.strategy.tuning.catalog",
    )

    family = registry.get_registered_model_family("tabular_foundation_challenger")
    assert family["default_stage"] == "shadow"
    assert family["promotion_guard"].endswith("promotion_guard.assess_challenger")
    assert "tabular_foundation_challenger" in predictor.available_model_families()
    assert job_registry.ALLOWED_JOBS["train_tabular_challenger_models"][3]["default_stage"] == "shadow"
    assert "TABULAR_CHALLENGER_MAX_ROWS" in catalog.managed_env_names("tabular_foundation_challenger")


def test_benchmark_comparison_against_sklearn_gbm(monkeypatch):
    module = importlib.import_module("engine.strategy.models.tabular_challenger")
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    X, y = _rows(32)

    result = module.benchmark_against_sklearn_gbm_baseline(X, y, feature_ids=list(FEATURE_IDS), challenger_backend="fake")

    assert result["primary_baseline"] == "sklearn_gbm"
    assert result["challenger"]["n"] == 32
    assert result["sklearn_gbm_baseline"]["n"] == 32
    assert result["challenger"]["rmse"] <= 1e-5
    assert result["sklearn_gbm_baseline"]["rmse"] >= 0.0


def test_oos_shadow_candidate_is_not_live_promotable():
    champion_manager = importlib.import_module("engine.strategy.champion_manager")
    row = {
        "model_id": "tabular_foundation_challenger.unit:v1",
        "model_name": "tabular_foundation_challenger.unit",
        "stage": "shadow",
        "meta": {
            "score_source": "model_oos_predictions",
            "model_family": "tabular_foundation_challenger",
            "model_version": "v1",
            "net_cost_evidence_available": True,
            "net_cost_label_count": 50,
        },
    }

    assert champion_manager._score_source_is_competition_candidate(row["meta"]) is True
    assert champion_manager._candidate_is_live_promotable(row) is False


def test_tabular_dependencies_are_optional_and_license_gated_by_default():
    from tools import validate_dependency_lock

    errors, warnings = validate_dependency_lock._tabular_challenger_optional_dependency_report()
    assert errors == []
    assert warnings == []
    for manifest_name in validate_dependency_lock.TABULAR_CHALLENGER_DEFAULT_MANIFESTS:
        entries = validate_dependency_lock._requirements_entries(validate_dependency_lock.ROOT / manifest_name)
        assert not (set(entries) & validate_dependency_lock.TABULAR_CHALLENGER_OPTIONAL_REQUIREMENTS), manifest_name
    project = json.loads(json.dumps({"extra": sorted(validate_dependency_lock.TABULAR_CHALLENGER_OPTIONAL_REQUIREMENTS)}))
    assert "tabpfn" in project["extra"]
