from __future__ import annotations

import importlib
import json
import sqlite3

import numpy as np
import torch

from engine.strategy import feature_registry

FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
]


def _sequence_dataset(n: int = 18, seq_len: int = 16, n_features: int = 3, n_horizons: int = 2):
    rng = np.random.default_rng(42)
    X = rng.normal(0.0, 1.0, size=(n, seq_len, n_features)).astype(np.float32)
    target0 = X[:, -4:, 0].mean(axis=1) - 0.4 * X[:, -2:, 1].mean(axis=1)
    target1 = X[:, -8:, 2].mean(axis=1) + 0.25 * X[:, -1, 0]
    y = np.stack([target0, target1], axis=1).astype(np.float32)
    return X, y


def test_patchtst_forward_shape():
    module = importlib.import_module("engine.strategy.models.patchtst")
    model = module.PatchTST(
        seq_len=16,
        n_features=3,
        n_horizons=2,
        patch_len=4,
        stride=2,
        d_model=16,
        n_layers=1,
        n_heads=2,
        dropout=0.0,
    )
    out = model(torch.zeros((5, 16, 3), dtype=torch.float32))
    assert tuple(out.shape) == (5, 2)


def test_patchtst_default_device_is_cpu_first(monkeypatch):
    module = importlib.import_module("engine.strategy.models.patchtst")
    monkeypatch.delenv("PATCHTST_DEVICE", raising=False)
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("PATCHTST_USE_CUDA", raising=False)
    monkeypatch.delenv("RUNTIME_HARDWARE_PROFILE", raising=False)
    monkeypatch.delenv("TRADING_DEPENDENCY_PROFILE", raising=False)
    monkeypatch.delenv("DEPENDENCY_PROFILE", raising=False)
    monkeypatch.delenv("RUNTIME_DEPENDENCY_PROFILE", raising=False)
    monkeypatch.setattr(module.torch.cuda, "is_available", lambda: True)

    assert module._default_device().type == "cpu"

    monkeypatch.setenv("PATCHTST_DEVICE", "auto")
    monkeypatch.setenv("RUNTIME_HARDWARE_PROFILE", "nvidia")
    monkeypatch.setenv("TRADING_DEPENDENCY_PROFILE", "nvidia-cuda")
    assert module._default_device().type == "cuda"


def test_patchtst_loss_decreases_and_roundtrips(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.patchtst")
    X, y = _sequence_dataset()

    reg = module.PatchTSTRegressor(
        model_name="patchtst.unit",
        feature_ids=list(FEATURE_IDS),
        seq_len=16,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=7,
        device="cpu",
    )
    losses = reg.fit(X, y, epochs=35, lr=0.01, weight_decay=0.0, return_losses=True)
    assert losses[-1] < losses[0]

    before = reg.predict(X[:3])
    model_dir = reg.save(tmp_path / "patchtst_artifact")
    loaded = module.PatchTSTRegressor.load(model_dir)
    after = loaded.predict(X[:3])
    np.testing.assert_array_equal(before, after)


def test_patchtst_masked_pretraining_applies_and_roundtrips(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.patchtst")
    X, y = _sequence_dataset(n=10, seq_len=8, n_features=3, n_horizons=2)

    pretraining = module.train_masked_pretraining_artifact(
        X,
        model_name="patchtst.pretrain_unit",
        feature_ids=list(FEATURE_IDS),
        seq_len=8,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=11,
        epochs=2,
        lr=0.01,
        weight_decay=0.0,
        device="cpu",
    )

    reg = module.PatchTSTRegressor(
        model_name="patchtst.pretrain_unit",
        feature_ids=list(FEATURE_IDS),
        seq_len=8,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=11,
        device="cpu",
    )
    reg.fit(X, y, epochs=1, lr=0.01, weight_decay=0.0, pretraining_artifact=pretraining)

    assert reg.training_metrics["pretraining_applied"] is True
    assert reg.pretraining_metadata["loaded_encoder_tensors"] > 0
    model_dir = reg.save(tmp_path / "patchtst_pretrained_finetune")
    loaded = module.PatchTSTRegressor.load(model_dir)
    assert loaded.pretraining_metadata["artifact_kind"] == module.PRETRAIN_ARTIFACT_KIND
    np.testing.assert_array_equal(reg.predict(X[:2]), loaded.predict(X[:2]))


def test_patchtst_load_rejects_pretraining_schema_drift(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.patchtst")
    X, y = _sequence_dataset(n=8, seq_len=8, n_features=3, n_horizons=2)
    pretraining = module.train_masked_pretraining_artifact(
        X,
        model_name="patchtst.pretrain_drift",
        feature_ids=list(FEATURE_IDS),
        seq_len=8,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=13,
        epochs=1,
        lr=0.01,
        weight_decay=0.0,
        device="cpu",
    )
    reg = module.PatchTSTRegressor(
        model_name="patchtst.pretrain_drift",
        feature_ids=list(FEATURE_IDS),
        seq_len=8,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=13,
        device="cpu",
    )
    reg.fit(X, y, epochs=1, lr=0.01, weight_decay=0.0, pretraining_artifact=pretraining)
    model_dir = reg.save(tmp_path / "patchtst_pretraining_schema_drift")
    config_path = model_dir / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["pretraining"]["feature_schema"]["feature_set_tag"] = "tampered-pretraining-tag"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    try:
        module.PatchTSTRegressor.load(model_dir)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("PatchTSTRegressor.load accepted pretraining schema drift")

    assert "patchtst_pretraining_schema_drift" in message
    assert "tampered-pretraining-tag" in message


def test_patchtst_training_rows_prefer_net_after_cost_labels(monkeypatch, tmp_path):
    module = importlib.import_module("engine.strategy.models.patchtst")
    monkeypatch.setattr(module, "_expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setattr(
        module,
        "build_feature_snapshot",
        lambda *, event, symbol, feature_ids: {fid: float(int(event["ts_ms"]) % 1000) for fid in feature_ids},
    )
    db_path = tmp_path / "patchtst_net_labels.db"

    def _connect(*args, **kwargs):
        return sqlite3.connect(db_path)

    def _table_exists(con, table_name):
        row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (str(table_name),)).fetchone()
        return row is not None

    monkeypatch.setattr(module, "connect", _connect)
    monkeypatch.setattr(module, "table_exists", _table_exists)
    con = sqlite3.connect(db_path)
    try:
        con.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, ts_ms INTEGER, title TEXT, body TEXT, source TEXT)")
        con.execute("CREATE TABLE labels(event_id INTEGER, symbol TEXT, horizon_s INTEGER, impact_z REAL)")
        con.execute("CREATE TABLE labels_exec(event_id INTEGER, symbol TEXT, horizon_s INTEGER, realized INTEGER, net_z REAL)")
        con.execute(
            """
            CREATE TABLE net_after_cost_labels(
              event_id INTEGER,
              symbol TEXT,
              horizon_s INTEGER,
              label_ts_ms INTEGER,
              computed_at_ts_ms INTEGER,
              realized INTEGER,
              net_return REAL,
              realized_forward_return REAL,
              source TEXT
            )
            """
        )
        now_ms = int(module.time.time() * 1000)
        for idx in range(4):
            ts_ms = now_ms - (4 - idx) * 1000
            con.execute("INSERT INTO events(id, ts_ms, title, body, source) VALUES(?,?,?,?,?)", (idx + 1, ts_ms, "t", "b", "unit"))
            con.execute("INSERT INTO labels(event_id, symbol, horizon_s, impact_z) VALUES(?,?,?,?)", (idx + 1, "SPY", 3600, 99.0))
            con.execute(
                "INSERT INTO labels_exec(event_id, symbol, horizon_s, realized, net_z) VALUES(?,?,?,?,?)",
                (idx + 1, "SPY", 3600, 1, 88.0),
            )
            con.execute(
                """
                INSERT INTO net_after_cost_labels(
                  event_id, symbol, horizon_s, label_ts_ms, computed_at_ts_ms, realized,
                  net_return, realized_forward_return, source
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (idx + 1, "SPY", 3600, ts_ms, ts_ms + 10, 1, 0.01 * (idx + 1), 0.02 * (idx + 1), "broker_fills_v2"),
            )
        con.commit()
    finally:
        con.close()

    built = module._load_sequence_training_rows(
        {
            "feature_ids": list(FEATURE_IDS),
            "seq_len": 3,
            "n_horizons": 1,
            "training_window_days": 1,
            "symbol_universe": ["SPY"],
            "horizon_s": 3600,
            "supervised_target": "net_edge",
        }
    )

    assert built is not None
    _X, y, meta = built
    assert y.shape == (2, 1)
    np.testing.assert_allclose(y.reshape(-1), np.asarray([0.03, 0.04], dtype=np.float32))
    assert {row["target_source"] for row in meta} == {"net_after_cost_labels.net_return"}


def test_patchtst_pretraining_job_registered_shadow_default():
    registry = importlib.import_module("engine.runtime.job_registry")
    spec = registry.ALLOWED_JOBS["pretrain_patchtst_models"]

    assert spec[0] == "engine/strategy/jobs/pretrain_patchtst_models.py"
    assert spec[1] == "oneshot"
    assert spec[3]["default_stage"] == "shadow"


def test_patchtst_cuda_trained_config_loads_on_cpu_with_warning(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.patchtst")
    monkeypatch.setattr(module.torch.cuda, "is_available", lambda: False)
    X, y = _sequence_dataset(n=6, seq_len=8, n_features=3, n_horizons=2)

    reg = module.PatchTSTRegressor(
        model_name="patchtst.unit",
        feature_ids=list(FEATURE_IDS),
        seq_len=8,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=7,
        device="cpu",
    )
    reg.fit(X, y, epochs=1, lr=0.01, weight_decay=0.0)
    model_dir = reg.save(tmp_path / "patchtst_cuda_config")
    config_path = model_dir / "config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["device_at_train"] = "cuda"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    loaded = module.PatchTSTRegressor.load(model_dir)

    assert loaded.device.type == "cpu"


def test_patchtst_load_rejects_feature_set_tag_drift(monkeypatch, tmp_path):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.patchtst")
    monkeypatch.setattr(module, "_emit_config_feature_schema_load_failure", lambda **kwargs: None)
    X, y = _sequence_dataset(n=8, seq_len=8, n_features=3, n_horizons=2)

    reg = module.PatchTSTRegressor(
        model_name="patchtst.tag_drift",
        feature_ids=list(FEATURE_IDS),
        seq_len=8,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=7,
        device="cpu",
    )
    reg.fit(X, y, epochs=1, lr=0.01, weight_decay=0.0)
    model_dir = reg.save(tmp_path / "patchtst_tag_drift")
    persisted_tag = json.loads((model_dir / "config.json").read_text(encoding="utf-8"))["feature_schema"][
        "feature_set_tag"
    ]

    monkeypatch.setattr(module, "feature_set_tag_from_ids", lambda ids: "mutated-patchtst-tag")

    try:
        module.PatchTSTRegressor.load(model_dir)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("PatchTSTRegressor.load accepted feature_set_tag drift")

    assert "feature_schema_drift" in message
    assert persisted_tag in message
    assert "mutated-patchtst-tag" in message
