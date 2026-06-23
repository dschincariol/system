from __future__ import annotations

import importlib

import pytest


def _reload_engine(monkeypatch, **env):
    defaults = {
        "PORTFOLIO_RISK_USE_CORR_CLUSTERS": "1",
        "PORTFOLIO_RISK_FX_CURRENCY_CLUSTERS": "1",
        "PORTFOLIO_RISK_CLUSTER_MAX_GROSS": "0.45",
    }
    defaults.update({key: str(value) for key, value in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def _fx_inst(_con, symbol):
    pairs = {
        "EURUSD": ("EUR", "USD"),
        "GBPUSD": ("GBP", "USD"),
        "AUDJPY": ("AUD", "JPY"),
    }
    base, quote = pairs.get(str(symbol), ("EUR", "USD"))
    return {
        "asset_class": "FX",
        "base_ccy": base,
        "quote_ccy": quote,
        "pip_size": 0.0001,
        "contract_size": 100_000.0,
        "leverage_cap": 50.0,
        "symbol": str(symbol),
    }


def test_shared_currency_fx_pairs_cluster_even_without_price_correlation(monkeypatch):
    engine = _reload_engine(monkeypatch)
    monkeypatch.setattr(engine, "_fx_instrument", _fx_inst)
    monkeypatch.setattr(engine, "corr_from_prices", lambda *_args, **_kwargs: None)
    desired = {
        "EURUSD": {"symbol": "EURUSD", "weight": 0.30, "side": "LONG", "reason": {}},
        "GBPUSD": {"symbol": "GBPUSD", "weight": 0.30, "side": "LONG", "reason": {}},
    }
    info = {}

    out = engine._apply_corr_cluster_caps(None, desired, info)

    assert out["EURUSD"]["weight"] == pytest.approx(0.225)
    assert out["GBPUSD"]["weight"] == pytest.approx(0.225)
    hit = info["corr_cluster_caps_hit"][0]
    assert hit["fx_shared_currency"][0]["shared_currency"] == ["USD"]
    assert out["EURUSD"]["reason"]["corr_cluster_cap"]["fx_shared_currency"]


def test_non_sharing_fx_pairs_do_not_structurally_cluster(monkeypatch):
    engine = _reload_engine(monkeypatch)
    monkeypatch.setattr(engine, "_fx_instrument", _fx_inst)
    monkeypatch.setattr(engine, "corr_from_prices", lambda *_args, **_kwargs: None)
    desired = {
        "EURUSD": {"symbol": "EURUSD", "weight": 0.30, "side": "LONG", "reason": {}},
        "AUDJPY": {"symbol": "AUDJPY", "weight": 0.30, "side": "LONG", "reason": {}},
    }
    info = {}

    out = engine._apply_corr_cluster_caps(None, desired, info)

    assert out["EURUSD"]["weight"] == pytest.approx(0.30)
    assert out["AUDJPY"]["weight"] == pytest.approx(0.30)
    assert "corr_cluster_caps_hit" not in info
