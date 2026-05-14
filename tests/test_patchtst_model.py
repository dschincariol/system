from __future__ import annotations

import importlib
import json

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
