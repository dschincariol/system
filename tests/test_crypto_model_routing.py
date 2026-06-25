from __future__ import annotations

import numpy as np

from engine.strategy import predictor
from engine.strategy.models import lgbm_ranker


def test_rank_dataset_keeps_crypto_rows_only_when_crypto_scope_enabled() -> None:
    symbols = ["BTC", "ETH"]
    X_rows = []
    y_rows = []
    meta_rows = []
    for ts_ms in (1_700_000_000_000, 1_700_000_060_000):
        for idx, symbol in enumerate(symbols):
            X_rows.append({"rank.alpha": float(idx + 1)})
            y_rows.append(0.01 if symbol == "BTC" else -0.01)
            meta_rows.append({"symbol": symbol, "ts_ms": ts_ms, "horizon_s": 300})

    default_dataset = lgbm_ranker.make_cross_sectional_rank_dataset(
        X_rows,
        y_rows,
        meta_rows,
        min_group_size=2,
    )
    crypto_dataset = lgbm_ranker.make_cross_sectional_rank_dataset(
        X_rows,
        y_rows,
        meta_rows,
        min_group_size=2,
        asset_scope="CRYPTO",
    )

    assert default_dataset.group_counts == []
    assert default_dataset.y_relevance.tolist() == []
    assert crypto_dataset.group_counts == [2, 2]
    assert [row["symbol"] for row in crypto_dataset.meta_rows] == ["BTC", "ETH", "BTC", "ETH"]


def test_crypto_scoped_lgbm_ranker_serves_crypto_batch(monkeypatch) -> None:
    symbols = ["BTC", "ETH", "SOL"]
    horizon_s = 300
    feature_ids = ["rank.alpha"]
    base = {
        (symbol, horizon_s): (0.0, 0.1, {"model": "fallback", "feature_ids": list(feature_ids)})
        for symbol in symbols
    }
    scores = {"BTC": 0.9, "ETH": -0.4, "SOL": 0.1}
    active_model = {
        "model_name": "crypto_lgbm_ranker",
        "model_id": "crypto_lgbm_ranker",
        "model_family": "lgbm_ranker",
        "family": "lgbm_ranker",
        "asset_scope": "CRYPTO",
        "artifact_path": "/tmp/fake-crypto-ranker.json",
        "feature_ids": list(feature_ids),
    }
    feature_ids_for_model = list(feature_ids)

    class FakeRanker:
        model_name = "crypto_lgbm_ranker"
        model_kind = "lightgbm_ranker"
        feature_ids = feature_ids_for_model
        feature_schema = {"feature_ids": feature_ids_for_model}
        training_metrics = {"rank_ic": 0.25}

        def predict(self, rows):
            return np.asarray([float(row["rank.alpha"]) for row in rows], dtype=float)

    monkeypatch.setattr(predictor, "_resolve_active_model", lambda symbol, horizon: dict(active_model))
    monkeypatch.setattr(predictor, "load_lgbm_ranker_model_from_artifact", lambda **kwargs: FakeRanker())
    monkeypatch.setattr(predictor, "_latest_feature_snapshot_features_many", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        predictor,
        "_cached_or_build_feature_snapshot",
        lambda *, event, symbol, feature_ids: {"rank.alpha": float(scores[str(symbol)])},
    )
    monkeypatch.setattr(predictor, "build_feature_snapshot", lambda **kwargs: {})
    monkeypatch.setattr(predictor, "_attach_ood_diagnostics", lambda explain, model, feature_map, warn_key: explain)
    monkeypatch.setattr(predictor, "_track_prediction_output", lambda **kwargs: None)

    out = predictor._maybe_apply_lgbm_ranker_batch(
        dict(base),
        symbols=list(symbols),
        horizon_s=horizon_s,
        top_k=2,
        event={"ts_ms": 1_700_000_000_000, "title": "", "body": "", "source": "test"},
    )

    btc_z, btc_conf, btc_explain = out[("BTC", horizon_s)]
    eth_z, eth_conf, eth_explain = out[("ETH", horizon_s)]
    assert btc_z > 0.0
    assert eth_z < 0.0
    assert btc_conf > 0.0
    assert eth_conf > 0.0
    assert btc_explain["model_family"] == "lgbm_ranker"
    assert btc_explain["ranker_asset_scope"] == "CRYPTO"
    assert btc_explain["lgbm_ranker_batch"]["asset_scope"] == "CRYPTO"
    assert eth_explain["model_family"] == "lgbm_ranker"
