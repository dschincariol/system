from __future__ import annotations

from engine.api import api_system


def test_regime_context_route_spec_registers_read_endpoint() -> None:
    assert ("GET", "/api/regime/context", "api_get_regime_context") in api_system.ROUTE_SPECS_SYSTEM


def test_regime_context_from_vector_returns_layer_labels_and_timestamps() -> None:
    payload = api_system._regime_context_from_vector(
        {
            "ts_ms": 1_700_000_000_000,
            "macro": {"risk_on": 0.7, "risk_off": 0.2},
            "asset": {"etf_like": 1.0, "single_stock_like": 0.0},
            "micro": {"momentum_dominant": 0.8, "auction_heavy": 0.1},
            "regimes": {"volatility": "CALM", "liquidity": "NORMAL"},
            "confidence": {"overall": 0.6, "macro": 0.7, "asset": 0.8, "micro": 0.5},
        },
        source="unit_test",
        symbol="SPY",
    )

    assert payload["ok"] is True
    assert payload["source"] == "unit_test"
    assert payload["layers"]["macro"]["label"] == "RISK_ON"
    assert payload["layers"]["asset"]["label"] == "ETF_LIKE"
    assert payload["layers"]["micro"]["label"] == "MOMENTUM_DOMINANT"
    assert payload["layers"]["macro"]["ts_ms"] == 1_700_000_000_000
