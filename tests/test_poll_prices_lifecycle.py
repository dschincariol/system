from __future__ import annotations

from engine.data import poll_prices


def test_price_cycle_marks_live_when_first_price_marker_already_exists(monkeypatch):
    transitions = []

    monkeypatch.setattr(poll_prices, "close_pooled_connections", lambda: None)
    monkeypatch.setattr(poll_prices, "meta_set_if_missing", lambda _key, _value: False)
    monkeypatch.setattr(poll_prices, "meta_set", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        poll_prices,
        "set_state",
        lambda state, detail: transitions.append((state, detail)),
    )

    poll_prices._finalize_post_commit_price_cycle(
        [],
        {"first_ts_ms": 1234567890, "provider": "yfinance"},
    )

    assert transitions == [(poll_prices.LIVE, "market_data_healthy")]
