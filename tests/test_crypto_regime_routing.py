from __future__ import annotations

import numpy as np

from engine.strategy import predictor


def test_crypto_regime_context_uses_btcusd_anchor_and_crypto_mid_default(monkeypatch) -> None:
    seen: list[str] = []

    def fake_current_regime(anchor: str):
        seen.append(str(anchor))
        return None

    monkeypatch.delenv("CRYPTO_REGIME_ANCHOR_SYMBOL", raising=False)
    monkeypatch.setattr(predictor, "get_current_regime", fake_current_regime)

    regime, context = predictor._prediction_regime_context(
        "ETH",
        {"ts_ms": 1_700_000_000_000},
    )

    assert seen == ["BTCUSD"]
    assert regime == "CRYPTO_MID"
    assert context == {"anchor_symbol": "BTCUSD", "asset_class": "CRYPTO"}
    assert predictor._regime_anchor_symbol("BTC") == "BTCUSD"
    assert predictor._prediction_asset_class("BTCUSD") == "CRYPTO"


def test_crypto_prediction_explain_gets_crypto_regime_context(monkeypatch) -> None:
    active_model = {
        "model_name": "crypto_embed_regressor",
        "model_id": "crypto_embed_regressor",
        "family": "embed_regressor",
        "model_family": "embed_regressor",
        "feature_ids": [],
    }

    monkeypatch.setattr(predictor, "get_current_regime", lambda anchor: None)
    monkeypatch.setattr(predictor, "_knn_raw", lambda *args, **kwargs: (0.0, 0.0, {"source": "test"}))
    monkeypatch.setattr(
        predictor,
        "_adapter_predict",
        lambda *args, **kwargs: (
            0.25,
            0.75,
            {"model": "embed_regressor", "served_model_family": "embed_regressor"},
        ),
    )

    z, conf, explain = predictor._predict_resolved_model(
        np.zeros(3, dtype=float),
        "BTC",
        300,
        top_k=3,
        active_model=active_model,
        event={"ts_ms": 1_700_000_000_000},
    )

    assert z == 0.25
    assert conf == 0.75
    assert explain["regime_at_trade"] == "CRYPTO_MID"
    assert explain["regime_anchor_symbol"] == "BTCUSD"
    assert explain["crypto_regime_context"] == {"anchor_symbol": "BTCUSD", "asset_class": "CRYPTO"}
    assert "fx_regime_context" not in explain
