from __future__ import annotations

from engine.strategy import predictor
from engine.strategy.models import lgbm_ranker


def test_default_ranker_dataset_preserves_equity_golden_with_crypto_rows_present() -> None:
    symbols = ["SPY", "QQQ", "BTC", "ETH"]
    X_rows = []
    y_rows = []
    meta_rows = []
    for ts_ms in (1_700_000_000_000, 1_700_000_060_000):
        for symbol in symbols:
            X_rows.append({"rank.alpha": 1.0 if symbol == "SPY" else 0.0})
            y_rows.append(0.10 if symbol == "SPY" else -0.10 if symbol == "QQQ" else 0.50)
            meta_rows.append({"symbol": symbol, "ts_ms": ts_ms, "horizon_s": 300})

    default_dataset = lgbm_ranker.make_cross_sectional_rank_dataset(
        X_rows,
        y_rows,
        meta_rows,
        min_group_size=2,
    )
    explicit_equity_dataset = lgbm_ranker.make_cross_sectional_rank_dataset(
        X_rows,
        y_rows,
        meta_rows,
        min_group_size=2,
        asset_scope="EQUITY",
    )

    assert [row["symbol"] for row in default_dataset.meta_rows] == ["QQQ", "SPY", "QQQ", "SPY"]
    assert default_dataset.group_counts == [2, 2]
    assert default_dataset.group_ts_ms == [1_700_000_000_000, 1_700_000_060_000]
    assert default_dataset.y_relevance.tolist() == [0, 2, 0, 2]
    assert default_dataset.X_rows == explicit_equity_dataset.X_rows
    assert default_dataset.y_relevance.tolist() == explicit_equity_dataset.y_relevance.tolist()
    assert default_dataset.y_return.tolist() == explicit_equity_dataset.y_return.tolist()
    assert default_dataset.group_counts == explicit_equity_dataset.group_counts
    assert default_dataset.meta_rows == explicit_equity_dataset.meta_rows


def test_ranker_serving_scope_defaults_to_equity_and_crypto_requires_scope() -> None:
    assert predictor._ranker_symbol_in_asset_scope("SPY", {}) is True
    assert predictor._ranker_symbol_in_asset_scope("BTC", {}) is False
    assert predictor._ranker_symbol_in_asset_scope("BTC", {"asset_scope": "CRYPTO"}) is True
    assert predictor._ranker_symbol_in_asset_scope("SPY", {"asset_scope": "CRYPTO"}) is False
