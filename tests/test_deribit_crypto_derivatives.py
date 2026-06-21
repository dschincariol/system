import sqlite3

import pytest

from engine.data.deribit_crypto_derivatives import (
    DERIBIT_FEATURE_GROUP,
    DERIBIT_FEATURE_IDS,
    DERIBIT_FEATURE_PREFIX,
    DeribitPublicClient,
    DeribitPublicWebSocketClient,
    build_deribit_provider_readiness,
    compute_deribit_crypto_derivative_features,
    deribit_base_assets_for_symbol,
    ensure_deribit_schema,
    fetch_deribit_public_batch,
    load_deribit_settings,
    normalize_deribit_instrument,
    normalize_deribit_snapshot,
    parse_deribit_instrument_name,
    put_deribit_batch,
    resolve_deribit_crypto_derivatives_snapshot,
    validate_deribit_settings,
)
from engine.runtime import job_registry
from engine.strategy.feature_registry import (
    FEATURE_STAGE_SHADOW,
    assert_no_shadow_features,
    feature_set_tag_from_ids,
    feature_stage,
)
from engine.strategy.model_feature_snapshots import build_model_feature_snapshot
from services.data_source_manager import MANAGED_DAEMON_JOBS, _default_catalog


NOW_MS = 1_700_000_000_000
DAY_MS = 86_400_000


def _option_row(name, *, option_type, strike, expiry, iv, oi=100.0, volume=10.0, delta=None, ts=NOW_MS):
    return {
        "source_record_id": f"{name}:{ts}",
        "instrument_name": name,
        "base_asset": "BTC",
        "quote_currency": "USD",
        "instrument_type": "option",
        "expiry_ts_ms": expiry,
        "strike": strike,
        "option_type": option_type,
        "mark_iv": iv,
        "delta": delta,
        "open_interest": oi,
        "volume": volume,
        "source_ts_ms": ts,
        "availability_ts_ms": ts,
        "ingested_ts_ms": ts,
    }


def _sample_rows(ts=NOW_MS):
    expiry_7d = ts + 7 * DAY_MS
    expiry_30d = ts + 30 * DAY_MS
    return [
        _option_row("BTC-01JAN26-40000-C", option_type="call", strike=40000, expiry=expiry_7d, iv=0.30, ts=ts - 3_600_000),
        _option_row("BTC-01JAN26-40000-C", option_type="call", strike=40000, expiry=expiry_7d, iv=0.50, oi=100, delta=0.24, ts=ts),
        _option_row("BTC-01JAN26-35000-P", option_type="put", strike=35000, expiry=expiry_7d, iv=0.60, oi=150, delta=-0.26, ts=ts),
        _option_row("BTC-31JAN26-40000-C", option_type="call", strike=40000, expiry=expiry_30d, iv=0.75, oi=80, delta=0.25, ts=ts),
        _option_row("BTC-31JAN26-35000-P", option_type="put", strike=35000, expiry=expiry_30d, iv=0.85, oi=120, delta=-0.25, ts=ts),
        {
            "source_record_id": f"BTC-26JUN26:{ts}",
            "instrument_name": "BTC-26JUN26",
            "base_asset": "BTC",
            "quote_currency": "USD",
            "instrument_type": "future",
            "expiry_ts_ms": expiry_30d,
            "mark_price": 42_000.0,
            "index_price": 40_000.0,
            "futures_basis": 0.05,
            "source_ts_ms": ts,
            "availability_ts_ms": ts,
            "ingested_ts_ms": ts,
        },
        {
            "source_record_id": f"BTC-PERPETUAL:{ts}",
            "instrument_name": "BTC-PERPETUAL",
            "base_asset": "BTC",
            "quote_currency": "USD",
            "instrument_type": "perpetual",
            "mark_price": 40_400.0,
            "index_price": 40_000.0,
            "perp_basis": 0.01,
            "funding_8h": 0.0002,
            "volume": 20.0,
            "source_ts_ms": ts,
            "availability_ts_ms": ts,
            "ingested_ts_ms": ts,
        },
    ]


def test_deribit_instrument_parsing_and_snapshot_normalization():
    perp = parse_deribit_instrument_name("BTC-PERPETUAL")
    assert perp["base_asset"] == "BTC"
    assert perp["instrument_type"] == "perpetual"

    parsed = parse_deribit_instrument_name("BTC-29SEP23-30000-C")
    assert parsed["base_asset"] == "BTC"
    assert parsed["instrument_type"] == "option"
    assert parsed["strike"] == 30000.0
    assert parsed["option_type"] == "call"

    instrument = normalize_deribit_instrument(
        {
            "instrument_name": "BTC-29SEP23-30000-C",
            "kind": "option",
            "base_currency": "BTC",
            "expiration_timestamp": NOW_MS + DAY_MS,
            "is_active": True,
        },
        now_ms=NOW_MS,
    )
    row = normalize_deribit_snapshot(
        {"instrument_name": "BTC-29SEP23-30000-C", "mark_price": 0.12, "open_interest": 100, "volume": 5},
        instrument=instrument,
        ticker={
            "timestamp": NOW_MS - 1_000,
            "best_bid_price": 0.10,
            "best_ask_price": 0.14,
            "mark_iv": 64.5,
            "greeks": {"delta": 0.24, "gamma": 0.01},
        },
        now_ms=NOW_MS,
    )
    assert row["instrument_name"] == "BTC-29SEP23-30000-C"
    assert row["base_asset"] == "BTC"
    assert row["mark_iv"] == pytest.approx(0.645)
    assert row["bid_price"] == pytest.approx(0.10)
    assert row["ask_price"] == pytest.approx(0.14)
    assert row["delta"] == pytest.approx(0.24)
    assert row["diagnostics_json"]["public_market_data_only"] is True
    assert row["diagnostics_json"]["direct_trading_authority"] is False


def test_deribit_fetcher_uses_public_ticker_and_order_book_when_enabled():
    class FakeClient:
        def __init__(self):
            self.ticker_calls = 0
            self.order_book_calls = 0

        def get_instruments(self, *, currency, kind):
            assert currency == "BTC"
            return [
                {
                    "instrument_name": "BTC-PERPETUAL",
                    "kind": "future",
                    "base_currency": "BTC",
                    "settlement_period": "perpetual",
                    "is_active": True,
                }
            ]

        def get_book_summary_by_currency(self, *, currency, kind):
            return [{"instrument_name": "BTC-PERPETUAL", "mark_price": 40_400, "index_price": 40_000, "open_interest": 1, "volume": 2}]

        def ticker(self, *, instrument_name):
            self.ticker_calls += 1
            return {"timestamp": NOW_MS, "funding_8h": 0.0002, "best_bid_price": 40_390, "best_ask_price": 40_410}

        def get_order_book(self, *, instrument_name, depth=1):
            self.order_book_calls += 1
            return {"timestamp": NOW_MS, "bids": [[40_390, 1]], "asks": [[40_410, 1]]}

    client = FakeClient()
    batch = fetch_deribit_public_batch(
        settings={
            "enabled_assets": "BTC",
            "instrument_types": "perpetual",
            "include_ticker": "1",
            "max_tickers": "1",
            "include_order_book": "1",
            "max_order_books": "1",
        },
        client=client,
        now_ms=NOW_MS,
    )

    assert client.ticker_calls == 1
    assert client.order_book_calls == 1
    assert batch["snapshots"][0]["funding_8h"] == pytest.approx(0.0002)
    assert batch["readiness"]["public_market_data_only"] is True


def test_deribit_websocket_mode_uses_public_json_rpc_only():
    class FakeConnection:
        def __init__(self):
            self.sent = []
            self.closed = False

        def send(self, payload):
            self.sent.append(payload)

        def recv(self):
            return '{"jsonrpc":"2.0","id":1,"result":{"instrument_name":"BTC-PERPETUAL"}}'

        def close(self):
            self.closed = True

    fake = FakeConnection()

    def factory(url, timeout):
        assert url == "wss://test.deribit.com/ws/api/v2"
        assert timeout == pytest.approx(2.0)
        return fake

    settings = load_deribit_settings({"mode": "websocket", "base_url": "https://test.deribit.com/api/v2"})
    assert settings.mode == "websocket"

    client = DeribitPublicWebSocketClient(
        base_url=settings.base_url,
        timeout_s=2.0,
        connection_factory=factory,
    )
    result = client.public_get("public/ticker", {"instrument_name": "BTC-PERPETUAL"})

    assert result["instrument_name"] == "BTC-PERPETUAL"
    sent_payload = fake.sent[0]
    assert '"method":"public/ticker"' in sent_payload
    assert '"instrument_name":"BTC-PERPETUAL"' in sent_payload
    assert client.last_reconnect_state == "connected"
    assert fake.closed is True

    with pytest.raises(ValueError):
        client.public_get("private/buy", {})


def test_deribit_feature_calculation_covers_iv_skew_basis_funding_and_volume():
    features, meta, available = compute_deribit_crypto_derivative_features(_sample_rows(), asof_ts_ms=NOW_MS)

    assert available is True
    assert features[f"{DERIBIT_FEATURE_PREFIX}available"] == 1.0
    assert features[f"{DERIBIT_FEATURE_PREFIX}short_dated_iv"] == pytest.approx(0.56)
    assert features[f"{DERIBIT_FEATURE_PREFIX}skew_25d_proxy"] == pytest.approx(0.10)
    assert features[f"{DERIBIT_FEATURE_PREFIX}term_structure_slope"] > 0.0
    assert features[f"{DERIBIT_FEATURE_PREFIX}put_call_open_interest_ratio"] > 1.0
    assert features[f"{DERIBIT_FEATURE_PREFIX}futures_basis"] == pytest.approx(0.05)
    assert features[f"{DERIBIT_FEATURE_PREFIX}perp_basis"] == pytest.approx(0.01)
    assert features[f"{DERIBIT_FEATURE_PREFIX}funding_pressure"] == pytest.approx(0.0002)
    assert 0.0 <= features[f"{DERIBIT_FEATURE_PREFIX}iv_rank"] <= 1.0
    assert meta["stage"] == "shadow"
    assert meta["direct_trading_authority"] is False


def test_deribit_storage_resolver_is_pit_safe_stale_aware_and_crypto_only(monkeypatch):
    con = sqlite3.connect(":memory:")
    ensure_deribit_schema(con)
    put_deribit_batch(con, snapshots=_sample_rows(), readiness={"ok": True}, now_ms=NOW_MS)

    features, meta, available = resolve_deribit_crypto_derivatives_snapshot(con, symbol="BTC", ts_ms=NOW_MS + 1)
    assert available is True
    assert features[f"{DERIBIT_FEATURE_PREFIX}available"] == 1.0
    assert meta["base_assets"] == ["BTC"]

    future_con = sqlite3.connect(":memory:")
    ensure_deribit_schema(future_con)
    future_rows = [dict(row, availability_ts_ms=NOW_MS + 10_000) for row in _sample_rows()]
    put_deribit_batch(future_con, snapshots=future_rows, readiness={"ok": True}, now_ms=NOW_MS)
    future_features, _future_meta, future_available = resolve_deribit_crypto_derivatives_snapshot(future_con, symbol="BTC", ts_ms=NOW_MS)
    assert future_available is False
    assert future_features[f"{DERIBIT_FEATURE_PREFIX}available"] == 0.0

    snap = build_model_feature_snapshot(
        symbol="BTC",
        ts_ms=NOW_MS + 31 * 60_000,
        feature_ids=list(DERIBIT_FEATURE_IDS),
        con=con,
    )
    assert snap["availability"][DERIBIT_FEATURE_GROUP] is False
    assert snap["features"][f"{DERIBIT_FEATURE_PREFIX}available"] == 0.0
    assert "feature_stale" in snap["pit_controls"][DERIBIT_FEATURE_GROUP]["reason_codes"]

    non_crypto_features, _meta, non_crypto_available = resolve_deribit_crypto_derivatives_snapshot(con, symbol="AAPL", ts_ms=NOW_MS + 1)
    assert non_crypto_available is False
    assert non_crypto_features[f"{DERIBIT_FEATURE_PREFIX}available"] == 0.0
    assert deribit_base_assets_for_symbol("COIN") == []
    assert "BTC" in deribit_base_assets_for_symbol("COIN", include_crypto_equity_mappings=True)


def test_deribit_provider_readiness_exposes_missing_iv_spread_stale_and_ws_state():
    instruments = [
        {"instrument_name": "BTC-01JAN26-40000-C", "base_asset": "BTC", "instrument_type": "option", "is_active": True},
        {"instrument_name": "BTC-PERPETUAL", "base_asset": "BTC", "instrument_type": "perpetual", "is_active": True},
    ]
    snapshots = [
        {
            "instrument_name": "BTC-01JAN26-40000-C",
            "base_asset": "BTC",
            "instrument_type": "option",
            "spread_bps": 1_000.0,
            "availability_ts_ms": NOW_MS - 120_000,
        }
    ]
    readiness = build_deribit_provider_readiness(
        instruments,
        snapshots,
        settings={"stale_threshold_ms": 60_000, "max_spread_bps": 500},
        now_ms=NOW_MS,
        errors=["sample_error"],
        ws_reconnect_state="backoff",
    )

    assert readiness["ok"] is False
    assert readiness["active_instruments"] == 2
    assert readiness["stale_instruments"] >= 1
    assert readiness["missing_iv_fields"] == 1
    assert readiness["order_book_spread_quality"]["wide_spread_count"] == 1
    assert readiness["websocket_reconnect_state"] == "backoff"
    assert readiness["direct_trading_authority"] is False


def test_deribit_public_only_controls_registry_and_control_plane():
    with pytest.raises(ValueError):
        validate_deribit_settings({"api_key": "secret"})
    with pytest.raises(ValueError):
        DeribitPublicClient(session=object()).public_get("private/buy", {})

    for fid in DERIBIT_FEATURE_IDS:
        assert feature_stage(fid) == FEATURE_STAGE_SHADOW
    assert "deribit_crypto_derivatives_v1_shadow" in feature_set_tag_from_ids(DERIBIT_FEATURE_IDS)
    with pytest.raises(ValueError):
        assert_no_shadow_features(DERIBIT_FEATURE_IDS, context="live_deribit_test")

    spec = job_registry.ALLOWED_JOBS["poll_deribit_crypto_derivatives"]
    assert spec[1] == "daemon"
    assert spec[3]["execution"] is False
    assert spec[3]["direct_trading_authority"] is False
    assert "poll_deribit_crypto_derivatives" in MANAGED_DAEMON_JOBS

    definition = _default_catalog()["deribit_crypto_derivatives"]
    assert definition.provider_name == "deribit"
    assert definition.source_type == "derivatives_provider"
    assert definition.default_enabled is False
    assert definition.credential_env == {}
    assert definition.setting_env["enabled_assets"] == "DERIBIT_ENABLED_ASSETS"
