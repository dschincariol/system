from __future__ import annotations

import importlib

import numpy as np
from sklearn.linear_model import Ridge

from engine.strategy import feature_registry

FEATURE_IDS = [
    "base.source_credibility",
    "base.log_recency_hours",
    "base.normalized_text_len",
]


def _tabular(seed: int = 123):
    rng = np.random.default_rng(seed)
    X = rng.normal(0.0, 1.0, size=(36, len(FEATURE_IDS))).astype(np.float32)
    y = (1.4 * X[:, 0] - 0.8 * X[:, 1] + 0.35 * X[:, 2]).astype(np.float32)
    rows = [dict(zip(FEATURE_IDS, row.tolist())) for row in X]
    return rows, X, y


def _sequence_from_tabular(X: np.ndarray, seq_len: int = 16, n_horizons: int = 2):
    windows = []
    ys = []
    for idx in range(seq_len - 1, int(X.shape[0])):
        window = X[idx - seq_len + 1 : idx + 1]
        windows.append(window)
        target = 1.4 * window[-1, 0] - 0.8 * window[-1, 1] + 0.35 * window[-1, 2]
        ys.append(np.full((n_horizons,), float(target), dtype=np.float32))
    return np.stack(windows).astype(np.float32), np.stack(ys).astype(np.float32)


def _rank_once(monkeypatch):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    lgbm = importlib.import_module("engine.strategy.models.lgbm_regressor")
    xgb = importlib.import_module("engine.strategy.models.xgb_regressor")
    patchtst = importlib.import_module("engine.strategy.models.patchtst")
    champion_manager = importlib.import_module("engine.strategy.champion_manager")
    predictor = importlib.import_module("engine.strategy.predictor")
    registry = importlib.import_module("engine.model_registry")

    rows, X, y = _tabular()
    lgbm_model = lgbm.train_lgbm_regressor(
        rows,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={"n_estimators": 16, "num_leaves": 7, "min_child_samples": 1, "learning_rate": 0.1},
        model_name="lgbm_regressor.integration",
    )
    xgb_model = xgb.train_xgb_regressor(
        rows,
        y,
        feature_ids=list(FEATURE_IDS),
        hyperparams={"n_estimators": 16, "max_depth": 2, "learning_rate": 0.1, "random_state": 5},
        model_name="xgb_regressor.integration",
    )
    X_seq, y_seq = _sequence_from_tabular(X)
    patch_model = patchtst.PatchTSTRegressor(
        model_name="patchtst.integration",
        feature_ids=list(FEATURE_IDS),
        seq_len=16,
        n_horizons=2,
        patch_len=4,
        stride=2,
        n_layers=1,
        n_heads=2,
        d_model=16,
        dropout=0.0,
        seed=5,
        device="cpu",
    )
    patch_model.fit(X_seq, y_seq, epochs=20, lr=0.01, weight_decay=0.0)

    ridge = Ridge(alpha=1.0).fit(X[:24], y[:24])
    eval_rows = rows[24:]
    eval_X = X[24:]
    eval_y = y[24:]
    preds = {
        "lgbm_regressor": lgbm_model.predict(eval_rows),
        "xgb_regressor": xgb_model.predict(eval_rows),
        "patchtst": patch_model.predict(X_seq[-len(eval_y) :])[:, 0],
        "embed_regressor": ridge.predict(eval_X),
    }

    candidates = []
    for name, pred in preds.items():
        rmse = float(np.sqrt(np.mean((np.asarray(pred).reshape(-1) - eval_y) ** 2)))
        candidates.append(
            {
                "model_name": name,
                "score": -rmse,
                "net_pnl": float(-rmse),
                "event_pnls": [float(-abs(p - t)) for p, t in zip(np.asarray(pred).reshape(-1), eval_y)],
                "trades": int(len(eval_y)),
                "wins": int(np.sum(np.sign(pred) == np.sign(eval_y))),
                "losses": int(len(eval_y) - np.sum(np.sign(pred) == np.sign(eval_y))),
            }
        )

    ranked = champion_manager._rank_models(candidates)
    ranked_names = [row["model_name"] for row in ranked]
    assert {"lgbm_regressor", "xgb_regressor", "patchtst", "embed_regressor"} == set(ranked_names)
    assert {"lgbm_regressor", "xgb_regressor", "patchtst"}.issubset(set(registry.registered_model_families()))
    assert {"lgbm_regressor", "xgb_regressor", "patchtst"}.issubset(set(predictor.available_model_families()))
    return ranked_names


def test_new_families_rank_stably_with_incumbent(monkeypatch):
    first = _rank_once(monkeypatch)
    second = _rank_once(monkeypatch)
    assert first == second
