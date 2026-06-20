from __future__ import annotations

import importlib

import numpy as np
import pytest

from engine.strategy import feature_registry

pytest.importorskip("lightgbm")


FEATURE_IDS = ["rank.alpha", "rank.noise"]


def _synthetic_rank_rows(n_groups: int = 48, group_size: int = 8):
    rng = np.random.default_rng(17)
    X_rows: list[dict[str, float]] = []
    returns: list[float] = []
    group_counts: list[int] = []
    group_ts: list[int] = []
    meta_rows: list[dict[str, int | str]] = []
    for group_idx in range(n_groups):
        alpha = np.linspace(-1.0, 1.0, group_size) + rng.normal(0.0, 0.03, size=group_size)
        perm = rng.permutation(group_size)
        group_counts.append(group_size)
        group_ts.append(1_700_000_000_000 + group_idx * 86_400_000)
        for row_idx, p in enumerate(perm):
            a = float(alpha[int(p)])
            n = float(rng.normal(0.0, 1.0))
            ret = float(1.7 * a + rng.normal(0.0, 0.03))
            X_rows.append({FEATURE_IDS[0]: a, FEATURE_IDS[1]: n})
            returns.append(ret)
            meta_rows.append({"symbol": f"SYM{row_idx}", "ts": group_ts[-1], "horizon": 3600})
    labels: list[int] = []
    module = importlib.import_module("engine.strategy.models.lgbm_ranker")
    offset = 0
    for count in group_counts:
        labels.extend(module._rank_relevance(returns[offset : offset + count], bins=5).tolist())
        offset += count
    return X_rows, np.asarray(labels, dtype=np.int32), np.asarray(returns, dtype=np.float32), group_counts, group_ts, meta_rows


def test_lgbm_ranker_recovers_planted_cross_sectional_order(monkeypatch):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    module = importlib.import_module("engine.strategy.models.lgbm_ranker")
    X, y, returns, groups, _group_ts, _meta = _synthetic_rank_rows()
    train_groups = groups[:36]
    train_n = int(sum(train_groups))
    model = module.train_lgbm_ranker(
        X[:train_n],
        y[:train_n],
        group=train_groups,
        feature_ids=list(FEATURE_IDS),
        hyperparams={
            "n_estimators": 80,
            "num_leaves": 15,
            "min_child_samples": 1,
            "learning_rate": 0.08,
            "random_state": 13,
        },
        model_name="lgbm_ranker.synthetic",
    )
    holdout_scores = model.predict(X[train_n:])
    metrics = module.ranker_metrics(returns[train_n:], holdout_scores, groups[36:])
    assert metrics["rank_ic"] > 0.8
    assert metrics["top_bottom_quintile_spread"] > 0.0


def test_lgbm_ranker_default_n_jobs_is_configurable_and_bounded(monkeypatch):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setenv("RUNTIME_WORKLOAD_PROFILE", "offline")
    monkeypatch.setenv("LGBM_RANKER_N_JOBS", "14")
    monkeypatch.setenv("MODEL_TRAIN_MAX_N_JOBS", "6")
    module = importlib.import_module("engine.strategy.models.lgbm_ranker")

    model = module.LGBMRankerModel(feature_ids=list(FEATURE_IDS))

    assert model.hyperparams["n_jobs"] == 6


def test_lgbm_ranker_training_job_requires_live_profile_ack(monkeypatch, capsys):
    monkeypatch.setenv("RUNTIME_WORKLOAD_PROFILE", "live")
    monkeypatch.delenv("OFFLINE_TRAINING_LIVE_PROFILE_ACK", raising=False)
    monkeypatch.delenv("OFFLINE_TRAINING_LIVE_PROFILE_OWNER", raising=False)
    monkeypatch.delenv("OFFLINE_TRAINING_LIVE_PROFILE_REASON", raising=False)
    module = importlib.import_module("engine.strategy.models.lgbm_ranker")

    rc = module.run_ranker_training_job()

    assert rc == 3
    assert "offline_training_live_profile_ack_required" in capsys.readouterr().out


def test_lgbm_ranker_era_boost_persists_training_tables(monkeypatch):
    monkeypatch.setattr(feature_registry, "expected_columns", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setenv("LGBM_ERA_BOOST", "1")
    monkeypatch.setenv("LGBM_ERA_BOOST_ITERS", "2")
    monkeypatch.setenv("LGBM_ERA_BOOST_ROUNDS", "4")
    monkeypatch.setenv("ERA_BOOST_MAX_DEGRADE", "1.0")
    module = importlib.import_module("engine.strategy.models.lgbm_ranker")
    X, y, _returns, groups, group_ts, _meta = _synthetic_rank_rows(n_groups=24, group_size=6)
    train_groups = groups[:18]
    train_n = int(sum(train_groups))
    validation_groups = groups[18:]
    train_eras = [f"rank_era_{idx // 5}" for idx in range(18)]
    validation_eras = [f"validation_era_{idx // 3}" for idx in range(6)]

    model = module.train_lgbm_ranker(
        X[:train_n],
        y[:train_n],
        group=train_groups,
        feature_ids=list(FEATURE_IDS),
        hyperparams={
            "n_estimators": 8,
            "num_leaves": 7,
            "min_child_samples": 1,
            "learning_rate": 0.08,
            "random_state": 19,
        },
        model_name="lgbm_ranker.era_boosted",
        era_timestamps=module._expand_group_values(group_ts[:18], train_groups),
        era_labels=module._expand_group_values(train_eras, train_groups),
        validation_data=(X[train_n:], y[train_n:]),
        validation_group=validation_groups,
        validation_timestamps=module._expand_group_values(group_ts[18:], validation_groups),
        validation_era_labels=module._expand_group_values(validation_eras, validation_groups),
    )

    payload = model.training_metrics["era_boost"]
    assert payload["applied"] is True
    assert payload["before"]["era_scores"]
    assert payload["after"]["era_scores"]
    assert payload["iterations"]


def test_lgbm_ranker_cpcv_splits_purge_overlapping_group_labels():
    module = importlib.import_module("engine.strategy.models.lgbm_ranker")
    group_ts = [1_000_000 + idx * 10_000 for idx in range(9)]
    splits = module.cpcv_group_splits(group_ts, horizon_s=15, n_splits=3, n_test_splits=1, embargo=0.0)
    assert splits
    starts = np.asarray(group_ts, dtype=float)
    ends = starts + 15_000.0
    for train_idx, test_idx in splits:
        for train_group in train_idx:
            for test_group in test_idx:
                assert not (starts[int(train_group)] <= ends[int(test_group)] and ends[int(train_group)] >= starts[int(test_group)])


def test_lgbm_ranker_serve_path_scores_one_batch_and_emits_top_bottom(monkeypatch):
    predictor = importlib.import_module("engine.strategy.predictor")
    symbols = ["AAA", "BBB", "CCC", "DDD"]
    scores_by_symbol = {"AAA": 0.1, "BBB": 0.9, "CCC": 0.5, "DDD": -0.2}
    calls = {"loads": 0, "predicts": 0}

    class FakeRanker:
        model_name = "lgbm_ranker.unit"
        model_kind = "lightgbm_ranker"
        feature_ids = list(FEATURE_IDS)
        feature_schema = {"feature_ids": list(FEATURE_IDS), "feature_set_tag": "unit"}
        training_metrics = {"rank_ic": 0.9, "n_train": 100}

        def predict(self, rows):
            calls["predicts"] += 1
            return np.asarray([float(row[FEATURE_IDS[0]]) for row in rows], dtype=np.float32)

    def fake_active(symbol, horizon_s, forced_model_name=None):
        return {
            "model_name": "lgbm_ranker.unit",
            "model_id": "lgbm_ranker.unit",
            "model_family": "lgbm_ranker",
            "family": "lgbm_ranker",
            "model_version": "unit-v1",
            "artifact_alias": "model:lgbm_ranker:unit:*:current",
            "feature_ids": list(FEATURE_IDS),
            "feature_schema": {"feature_ids": list(FEATURE_IDS), "feature_set_tag": "unit"},
        }

    def fake_load(**kwargs):
        calls["loads"] += 1
        return FakeRanker()

    monkeypatch.setenv("LGBM_RANKER_TOP_K", "1")
    monkeypatch.setenv("LGBM_RANKER_BOTTOM_K", "1")
    monkeypatch.setattr(predictor, "_resolve_active_model", fake_active)
    monkeypatch.setattr(predictor, "load_lgbm_ranker_model_from_artifact", fake_load)
    monkeypatch.setattr(predictor, "resolve_feature_ids", lambda *args, **kwargs: list(FEATURE_IDS))
    monkeypatch.setattr(predictor, "_registry_feature_set_tag", lambda *args, **kwargs: "unit")
    monkeypatch.setattr(predictor, "_track_prediction_output", lambda **kwargs: None)
    monkeypatch.setattr(
        predictor,
        "_cached_or_build_feature_snapshot",
        lambda *, event, symbol, feature_ids: {FEATURE_IDS[0]: scores_by_symbol[str(symbol)], FEATURE_IDS[1]: 0.0},
    )
    base = {(sym, 3600): (0.0, 0.1, {"model_name": "fallback"}) for sym in symbols}

    out = predictor._maybe_apply_lgbm_ranker_batch(
        base,
        symbols=list(symbols),
        horizon_s=3600,
        top_k=4,
        event={"ts_ms": 1_700_000_000_000, "title": "", "body": "", "source": "unit"},
    )

    assert calls == {"loads": 1, "predicts": 1}
    assert out[("BBB", 3600)][0] > 0.0
    assert out[("DDD", 3600)][0] < 0.0
    assert out[("AAA", 3600)][0] == 0.0
    assert out[("CCC", 3600)][0] == 0.0
    assert out[("BBB", 3600)][1] >= out[("CCC", 3600)][1]
    assert out[("BBB", 3600)][2]["ranker_rank"] == 1
    assert out[("BBB", 3600)][2]["ranker_selected"] is True
    assert out[("DDD", 3600)][2]["ranker_side"] == "SHORT"


def test_lgbm_ranker_family_registered_and_available():
    import engine.model_registry as registry
    from engine.strategy import predictor

    family = registry.get_registered_model_family("lgbm_ranker")
    assert family["default_stage"] == "shadow"
    assert family["promotion_guard"].endswith("promotion_guard.assess_challenger")
    assert "lgbm_ranker" in predictor.available_model_families()
