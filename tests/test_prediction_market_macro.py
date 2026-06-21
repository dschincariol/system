from __future__ import annotations

import sqlite3
from typing import Any

from engine.data.prediction_market_features import (
    resolve_prediction_market_event_snapshot,
    resolve_prediction_market_macro_snapshot,
)
from engine.data.forecastex_event_contracts import fetch_forecastex_csv_batch, parse_forecastex_csv
from engine.data.ibkr_event_contracts import fetch_ibkr_event_contract_batch
from engine.data.prediction_market_providers import (
    PREDICTION_MARKET_EVENT_FEATURE_GROUP,
    PREDICTION_MARKET_EVENT_FEATURE_IDS,
    PREDICTION_MARKET_EVENT_PREFIX,
    PREDICTION_MARKET_MACRO_FEATURE_GROUP,
    PREDICTION_MARKET_MACRO_FEATURE_IDS,
    fetch_polymarket_event_batch,
    normalize_cme_fedwatch_forecasts,
    normalize_kalshi_market,
    normalize_kalshi_orderbook,
    normalize_polymarket_market,
    normalize_polymarket_orderbook,
    polymarket_asset_baskets_from_settings,
)
from engine.data.prediction_market_storage import put_prediction_market_batch
from engine.runtime import job_registry
from engine.strategy import feature_registry
from engine.strategy.model_feature_snapshots import build_model_feature_snapshot
from services.data_source_manager import DataSourceManager


NOW_MS = 1_700_000_000_000


def _memory_con() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


def _macro_market(provider: str = "kalshi", *, availability_ts_ms: int = NOW_MS, resolution_ts_ms: int | None = NOW_MS + 86_400_000) -> dict:
    return {
        "provider_name": provider,
        "provider_market_id": f"{provider}:FOMC:target",
        "provider_event_id": f"{provider}:FOMC",
        "market_ticker": f"{provider}:FOMC:target",
        "series_ticker": "FOMC",
        "title": "FOMC target rate",
        "provider_category": "macro",
        "event_type": "fomc_rate_decision",
        "status": "open",
        "probability": 0.62 if provider == "kalshi" else 0.55,
        "previous_probability": 0.57,
        "probability_delta": 0.05 if provider == "kalshi" else -0.02,
        "liquidity": 2000.0,
        "volume": 500.0,
        "spread": 0.04,
        "event_ts_ms": NOW_MS + 86_400_000,
        "resolution_ts_ms": resolution_ts_ms,
        "source_ts_ms": availability_ts_ms,
        "availability_ts_ms": availability_ts_ms,
        "affected_assets": ["SPY", "QQQ", "IWM", "TLT", "GLD", "BTC", "ETH", "COIN", "HOOD", "XLF", "KRE", "ITB", "XHB"],
        "raw_payload": {"provider": provider, "availability_ts_ms": availability_ts_ms},
    }


def _event_market(
    provider: str = "polymarket",
    *,
    status: str = "active",
    availability_ts_ms: int = NOW_MS,
    semantic_event_id: str = "crypto_sec_etf_approval_2026",
    resolution_semantics: str = "yes_if_spot_crypto_etf_approved_by_2026_12_31",
    probability: float = 0.70,
    liquidity: float = 1000.0,
    affected_assets: list[str] | None = None,
) -> dict:
    return {
        "provider_name": provider,
        "provider_market_id": f"{provider}:condition:yes",
        "provider_event_id": f"{provider}:event",
        "market_ticker": f"{provider}:crypto-etf",
        "series_ticker": "crypto",
        "title": "Will SEC approve a spot crypto ETF?",
        "provider_category": "event_signal" if provider == "polymarket" else "macro",
        "event_type": "crypto_regulation",
        "status": status,
        "probability": probability,
        "previous_probability": 0.62,
        "probability_delta": probability - 0.62,
        "liquidity": liquidity,
        "volume": 300.0,
        "volume_24h": 120.0,
        "open_interest": 80.0,
        "spread": 0.04,
        "event_ts_ms": NOW_MS + 86_400_000,
        "resolution_ts_ms": NOW_MS + 86_400_000,
        "source_ts_ms": availability_ts_ms,
        "availability_ts_ms": availability_ts_ms,
        "affected_assets": affected_assets or ["BTC", "ETH", "SOL", "COIN", "HOOD", "MSTR"],
        "semantic_event_id": semantic_event_id,
        "resolution_semantics": resolution_semantics,
        "condition_id": "condition",
        "token_id": "yes-token",
        "outcome_name": "Yes",
        "raw_payload": {"provider": provider, "availability_ts_ms": availability_ts_ms},
    }


class _Response:
    def __init__(self, payload: Any, status_code: int = 200, text: str | None = None) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = str(text if text is not None else payload)

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return self._payload


class _PolymarketSession:
    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: float = 10.0, **_kwargs: Any) -> _Response:
        self.calls.append((url, dict(params or {})))
        return _Response(self.payload)


class _ForecastExSession:
    def __init__(self, payloads: dict[tuple[str, str], str]) -> None:
        self.payloads = dict(payloads)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def get(self, url: str, params: dict[str, Any] | None = None, timeout: float = 10.0, **_kwargs: Any) -> _Response:
        query = dict(params or {})
        self.calls.append((url, query))
        key = (str(query.get("type") or ""), str(query.get("date") or ""))
        if key not in self.payloads:
            return _Response({"error": "missing"}, status_code=500, text='{"error":"missing"}')
        return _Response({}, text=self.payloads[key])


def test_kalshi_provider_normalization_derives_probability_and_orderbook_metrics() -> None:
    market = normalize_kalshi_market(
        {
            "ticker": "KXFED-TEST",
            "event_ticker": "KXFED",
            "series_ticker": "KXFED",
            "title": "Will the Fed cut rates?",
            "yes_bid_dollars": "0.6100",
            "yes_ask_dollars": "0.6500",
            "previous_price_dollars": "0.6000",
            "liquidity_dollars": "100.00",
            "volume_fp": "20.00",
            "updated_time": "2023-11-14T22:13:20Z",
        },
        now_ms=NOW_MS,
        affected_assets=["SPY"],
    )
    assert market["provider_name"] == "kalshi"
    assert market["probability"] == 0.63
    assert round(float(market["probability_delta"]), 4) == 0.03
    assert round(float(market["spread"]), 4) == 0.04

    orderbook = normalize_kalshi_orderbook(
        "KXFED-TEST",
        {"orderbook_fp": {"yes_dollars": [["0.61", "100"], ["0.60", "50"]], "no_dollars": [["0.35", "80"]]}},
        now_ms=NOW_MS,
    )
    assert orderbook["best_yes_bid"] == 0.61
    assert orderbook["best_yes_ask"] == 0.65
    assert round(float(orderbook["mid_probability"]), 4) == 0.63
    assert orderbook["imbalance"] > 0.0


def test_cme_fedwatch_provider_normalization_supports_forecast_payloads() -> None:
    payload = {
        "forecasts": [
            {
                "meetingDt": "2026-07-29",
                "reportingDt": "2026-06-20",
                "probabilities": [
                    {"targetRate": "4.00-4.25", "probability": 55.0, "previousProbability": 50.0},
                    {"targetRate": "4.25-4.50", "probability": 45.0},
                ],
            }
        ]
    }
    batch = normalize_cme_fedwatch_forecasts(payload, now_ms=NOW_MS, affected_assets=["SPY", "TLT"])
    assert len(batch["events"]) == 1
    assert len(batch["markets"]) == 2
    assert batch["markets"][0]["provider_name"] == "cme_fedwatch"
    assert batch["markets"][0]["probability"] == 0.55
    assert round(float(batch["markets"][0]["probability_delta"]), 4) == 0.05


def test_polymarket_market_parsing_and_asset_mapping_are_explicit() -> None:
    baskets = polymarket_asset_baskets_from_settings(
        {"asset_basket_map_json": '{"custom_crypto":["BTC","COIN","MSTR"]}'}
    )
    assert baskets["custom_crypto"] == ["BTC", "COIN", "MSTR"]

    market = normalize_polymarket_market(
        {
            "id": "123",
            "slug": "bitcoin-etf-approved",
            "conditionId": "0xabc",
            "question": "Will the SEC approve a Bitcoin ETF?",
            "active": True,
            "closed": False,
            "clobTokenIds": '["yes-token","no-token"]',
            "outcomes": '["Yes","No"]',
            "outcomePrices": '["0.64","0.36"]',
            "bestBid": "0.63",
            "bestAsk": "0.65",
            "liquidity": "1000",
            "volume": "200",
            "openInterest": "50",
            "updatedAt": "2023-11-14T22:13:20Z",
        },
        event={"id": "event-1", "title": "Crypto regulation"},
        now_ms=NOW_MS,
        affected_assets=["BTC", "COIN"],
        semantic_event_id="crypto_etf_2026",
        resolution_semantics="yes_if_approved",
        event_type="crypto_regulation",
    )
    assert market["provider_name"] == "polymarket"
    assert market["condition_id"] == "0xabc"
    assert market["token_id"] == "yes-token"
    assert market["outcome_name"] == "Yes"
    assert market["probability"] == 0.64
    assert round(float(market["spread"]), 4) == 0.02
    assert market["semantic_event_id"] == "crypto_etf_2026"


def test_polymarket_orderbook_normalization_uses_yes_token_book() -> None:
    orderbook = normalize_polymarket_orderbook(
        "0xabc:yes-token",
        {"bids": [{"price": "0.62", "size": "100"}, {"price": "0.61", "size": "25"}], "asks": [{"price": "0.66", "size": "40"}]},
        now_ms=NOW_MS,
        condition_id="0xabc",
        token_id="yes-token",
        midpoint={"mid": "0.64"},
        spread={"spread": "0.04"},
        last_trade={"price": "0.65"},
    )
    assert orderbook["condition_id"] == "0xabc"
    assert orderbook["token_id"] == "yes-token"
    assert orderbook["best_yes_bid"] == 0.62
    assert orderbook["best_yes_ask"] == 0.66
    assert orderbook["mid_probability"] == 0.64
    assert orderbook["imbalance"] > 0.0


def test_polymarket_discovery_filters_unmapped_or_illiquid_markets() -> None:
    payload = [
        {
            "id": "event-1",
            "slug": "bitcoin-policy",
            "title": "Bitcoin policy event",
            "category": "crypto",
            "tags": [{"slug": "crypto"}],
            "markets": [
                {
                    "id": "market-good",
                    "slug": "bitcoin-etf-approved",
                    "conditionId": "condition-good",
                    "question": "Will the SEC approve a Bitcoin ETF?",
                    "active": True,
                    "closed": False,
                    "clobTokenIds": '["yes-token","no-token"]',
                    "outcomes": '["Yes","No"]',
                    "outcomePrices": '["0.70","0.30"]',
                    "liquidity": 500.0,
                    "volume": 200.0,
                    "openInterest": 10.0,
                },
                {
                    "id": "market-low-liquidity",
                    "slug": "bitcoin-low-liquidity",
                    "question": "Will Bitcoin do something?",
                    "active": True,
                    "closed": False,
                    "clobTokenIds": '["low-yes","low-no"]',
                    "outcomes": '["Yes","No"]',
                    "outcomePrices": '["0.51","0.49"]',
                    "liquidity": 1.0,
                    "volume": 0.0,
                    "openInterest": 0.0,
                },
            ],
        },
        {
            "id": "event-2",
            "slug": "sports-event",
            "title": "Sports event",
            "category": "sports",
            "tags": [{"slug": "sports"}],
            "markets": [
                {
                    "id": "market-unmapped",
                    "slug": "team-wins",
                    "question": "Will a team win?",
                    "active": True,
                    "closed": False,
                    "liquidity": 1000.0,
                    "volume": 100.0,
                }
            ],
        },
    ]
    session = _PolymarketSession(payload)
    batch = fetch_polymarket_event_batch(
        settings={
            "tags": "crypto",
            "keyword_allowlist": "bitcoin,sec",
            "min_liquidity": 100,
            "include_orderbooks": "0",
            "max_pages": 1,
        },
        now_ms=NOW_MS,
        session=session,
    )
    assert len(batch["events"]) == 1
    assert len(batch["markets"]) == 1
    assert batch["markets"][0]["token_id"] == "yes-token"
    assert "BTC" in batch["markets"][0]["affected_assets"]
    assert batch["orderbooks"] == []


def test_forecastex_csv_provider_parses_summary_prices_pairs_and_health() -> None:
    settings = {
        "asset_map_json": '{"BPMI":["SPY","TLT"],"macro":["SPY"],"energy":["XLE"]}',
        "resolution_source_map_json": '{"BPMI":"US Census Building Permits release"}',
    }
    summary_csv = "product_id,product_name,product_category,total_pairs\nBPMI,US Building Permits Initial,Economic Indicators,33\n"
    summary = parse_forecastex_csv(summary_csv, file_kind="summary", file_date="20260616", now_ms=NOW_MS, settings=settings)
    products = summary["product_metadata"]
    assert summary["events"][0]["product_id"] == "BPMI"
    assert summary["events"][0]["official_resolution_source"] == "US Census Building Permits release"
    assert summary["health"]["rows_parsed"] == 1

    prices_csv = (
        "event_contract,subtype,expiration_date,date,start_price,high_price,low_price,end_price,settlement_price,pair_quantity,open_interest,vwap\n"
        "BPMI_0626_1556000,YES,2026-07-17T07:30:00-05:00,2026-06-16,0.09,0.10,0.08,0.11,0.11,4,11,0.10\n"
        "BPMI_0626_1556000,NO,2026-07-17T07:30:00-05:00,2026-06-16,0.91,0.92,0.90,0.89,0.89,4,11,0.90\n"
    )
    prices = parse_forecastex_csv(prices_csv, file_kind="prices", file_date="20260616", now_ms=NOW_MS, settings=settings, product_metadata=products)
    assert len(prices["markets"]) == 1
    market = prices["markets"][0]
    assert market["provider_name"] == "forecastex"
    assert market["provider_contract_id"] == "BPMI_0626_1556000"
    assert market["product_id"] == "BPMI"
    assert market["source_file_date"] == "2026-06-16"
    assert market["source_file_kind"] == "prices"
    assert market["refresh_cadence"] == "daily_eod"
    assert market["provider_timestamp_ms"] > 0
    assert market["probability"] == 0.11
    assert market["volume"] == 4.0
    assert market["open_interest"] == 11.0
    assert "SPY" in market["affected_assets"]

    pairs_csv = (
        "pair_id,event_contract,expiration_date,quantity,yes_price,no_price,pair_time\n"
        "PAIR1,BPMI_0626_1556000,2026-07-17T07:30:00-05:00,5,0.12,0.88,2026-06-16T09:15:02.000000000-05:00\n"
        "PAIR2,BPMI_0626_1556000,2026-07-17T07:30:00-05:00,7,0.14,0.86,2026-06-16T09:17:02.000000000-05:00\n"
    )
    pairs = parse_forecastex_csv(pairs_csv, file_kind="intraday_pairs", file_date="20260616", now_ms=NOW_MS, settings=settings, product_metadata=products)
    assert len(pairs["markets"]) == 1
    assert len(pairs["orderbooks"]) == 1
    assert len(pairs["trades"]) == 2
    assert pairs["markets"][0]["source_file_kind"] == "intraday_pairs"
    assert pairs["markets"][0]["refresh_cadence"] == "10m"
    assert pairs["health"]["rows_parsed"] == 2


def test_forecastex_fetch_backfills_duplicate_files_idempotently_and_preserves_metadata() -> None:
    summary_csv = "product_id,product_name,product_category,total_pairs\nBPMI,US Building Permits Initial,Economic Indicators,33\n"
    prices_csv = (
        "event_contract,subtype,expiration_date,date,start_price,high_price,low_price,end_price,settlement_price,pair_quantity,open_interest,vwap\n"
        "BPMI_0626_1556000,YES,2026-07-17T07:30:00-05:00,2026-06-16,0.09,0.10,0.08,0.11,0.11,4,11,0.10\n"
    )
    pairs_csv = (
        "pair_id,event_contract,expiration_date,quantity,yes_price,no_price,pair_time\n"
        "PAIR1,BPMI_0626_1556000,2026-07-17T07:30:00-05:00,5,0.12,0.88,2026-06-16T09:15:02.000000000-05:00\n"
    )
    session = _ForecastExSession(
        {
            ("summary", "20260616"): summary_csv,
            ("prices", "20260616"): prices_csv,
            ("pairs", "20260616"): pairs_csv,
        }
    )
    batch = fetch_forecastex_csv_batch(
        settings={
            "file_dates": "20260616",
            "file_kinds": "summary,prices,pairs",
            "asset_map_json": '{"BPMI":["SPY"]}',
            "resolution_source_map_json": '{"BPMI":"US Census"}',
        },
        now_ms=NOW_MS,
        session=session,
    )
    assert batch["health"]["last_successful_csv_date"] == "2026-06-16"
    assert batch["health"]["rows_parsed"] == 3
    assert batch["health"]["parse_error_count"] == 0

    con = _memory_con()
    put_prediction_market_batch(con, now_ms=NOW_MS, events=batch["events"], markets=batch["markets"], orderbooks=batch["orderbooks"], trades=batch["trades"])
    put_prediction_market_batch(con, now_ms=NOW_MS, events=batch["events"], markets=batch["markets"], orderbooks=batch["orderbooks"], trades=batch["trades"])
    assert con.execute("SELECT COUNT(*) FROM prediction_market_events").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM prediction_market_markets").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM prediction_market_orderbook_snapshots").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM prediction_market_price_history").fetchone()[0] == 1
    row = con.execute(
        """
        SELECT provider_contract_id, product_id, official_resolution_source, source_file_date
        FROM prediction_market_markets
        """
    ).fetchone()
    assert row == ("BPMI_0626_1556000", "BPMI", "US Census", "2026-06-16")


def test_forecastex_parser_marks_malformed_sparse_and_inactive_contracts_explicitly_unavailable() -> None:
    malformed_pairs = (
        "pair_id,event_contract,expiration_date,quantity,yes_price,no_price,pair_time\n"
        "BAD1,,2026-07-17T07:30:00-05:00,5,0.12,0.88,2026-06-16T09:15:02-05:00\n"
        "BAD2,BPMI_0626_1556000,2026-07-17T07:30:00-05:00,5,,0.88,2026-06-16T09:15:02-05:00\n"
    )
    parsed = parse_forecastex_csv(
        malformed_pairs,
        file_kind="pairs",
        file_date="20260616",
        now_ms=NOW_MS,
        settings={"asset_map_json": '{"BPMI":["SPY"]}'},
    )
    assert parsed["health"]["rows_skipped"] == 2
    assert parsed["health"]["parse_error_count"] == 2

    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[
            {
                **_event_market(
                    "forecastex",
                    status="active",
                    probability=0.60,
                    liquidity=0.0,
                    affected_assets=["SPY"],
                    semantic_event_id="regulated_sparse",
                ),
                "provider_market_id": "forecastex:sparse:YES",
                "event_type": "macro",
                "volume": 0.0,
                "volume_24h": 0.0,
                "open_interest": 0.0,
            },
            {
                **_event_market(
                    "forecastex",
                    status="expired",
                    probability=0.80,
                    liquidity=100.0,
                    affected_assets=["SPY"],
                    semantic_event_id="regulated_expired",
                ),
                "provider_market_id": "forecastex:expired:YES",
                "event_type": "macro",
            },
        ],
        orderbooks=[],
        trades=[],
    )
    features, meta, available = resolve_prediction_market_event_snapshot(con, symbol="SPY", ts_ms=NOW_MS + 1)
    assert available is False
    assert features[f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_macro_probability"] == 0.0
    assert features[f"{PREDICTION_MARKET_EVENT_PREFIX}forecastex_available"] == 0.0
    assert meta["unavailable_reason_counts"]["sparse_or_zero_liquidity"] == 1
    assert meta["unavailable_reason_counts"]["inactive_status"] == 1


def test_prediction_market_storage_writes_are_idempotent() -> None:
    con = _memory_con()
    batch = {
        "events": [
            {
                "provider_name": "kalshi",
                "provider_event_id": "KXFED",
                "event_ticker": "KXFED",
                "provider_category": "macro",
                "source_ts_ms": NOW_MS,
                "availability_ts_ms": NOW_MS,
                "affected_assets": ["SPY"],
                "raw_payload": {"event": "KXFED"},
            }
        ],
        "markets": [_macro_market("kalshi")],
        "orderbooks": [
            {
                "provider_name": "kalshi",
                "provider_market_id": "kalshi:FOMC:target",
                "source_ts_ms": NOW_MS,
                "availability_ts_ms": NOW_MS,
                "mid_probability": 0.62,
                "spread": 0.04,
                "liquidity": 180.0,
                "imbalance": 0.2,
                "raw_payload": {"book": 1},
            }
        ],
        "trades": [],
    }
    put_prediction_market_batch(con, now_ms=NOW_MS, **batch)
    put_prediction_market_batch(con, now_ms=NOW_MS, **batch)

    assert con.execute("SELECT COUNT(*) FROM prediction_market_events").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM prediction_market_markets").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM prediction_market_orderbook_snapshots").fetchone()[0] == 1


def test_prediction_market_features_are_pit_safe_and_resolution_filtered() -> None:
    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[_macro_market("kalshi"), _macro_market("cme_fedwatch")],
        orderbooks=[
            {
                "provider_name": "kalshi",
                "provider_market_id": "kalshi:FOMC:target",
                "source_ts_ms": NOW_MS,
                "availability_ts_ms": NOW_MS,
                "mid_probability": 0.62,
                "spread": 0.03,
                "liquidity": 500.0,
                "imbalance": 0.25,
                "raw_payload": {"book": 1},
            }
        ],
        trades=[],
    )

    features, meta, available = resolve_prediction_market_macro_snapshot(con, symbol="SPY", ts_ms=NOW_MS + 1)
    assert available is True
    assert meta["kalshi_available"] is True
    assert meta["cme_available"] is True
    assert features["prediction_market_macro_v1.available"] == 1.0
    assert features["prediction_market_macro_v1.cme_vs_kalshi_disagreement"] > 0.0

    future_con = _memory_con()
    put_prediction_market_batch(
        future_con,
        now_ms=NOW_MS,
        events=[],
        markets=[_macro_market("kalshi", availability_ts_ms=NOW_MS + 10_000)],
        orderbooks=[],
        trades=[],
    )
    future_features, _future_meta, future_available = resolve_prediction_market_macro_snapshot(future_con, symbol="SPY", ts_ms=NOW_MS)
    assert future_available is False
    assert future_features["prediction_market_macro_v1.available"] == 0.0

    resolved_con = _memory_con()
    put_prediction_market_batch(
        resolved_con,
        now_ms=NOW_MS,
        events=[],
        markets=[_macro_market("kalshi", resolution_ts_ms=NOW_MS - 1)],
        orderbooks=[],
        trades=[],
    )
    _resolved_features, _resolved_meta, resolved_available = resolve_prediction_market_macro_snapshot(resolved_con, symbol="SPY", ts_ms=NOW_MS)
    assert resolved_available is False


def test_polymarket_event_features_require_live_liquid_mapped_markets_and_explicit_dispersion() -> None:
    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[
            _event_market("polymarket", probability=0.70),
            _event_market("kalshi", probability=0.52),
            {
                **_event_market(
                    "kalshi",
                    probability=0.20,
                    semantic_event_id="",
                    resolution_semantics="",
                ),
                "provider_market_id": "kalshi:same-title-no-semantic",
                "title": "Will SEC approve a spot crypto ETF?",
            },
        ],
        orderbooks=[
            {
                "provider_name": "polymarket",
                "provider_market_id": "polymarket:condition:yes",
                "condition_id": "condition",
                "token_id": "yes-token",
                "source_ts_ms": NOW_MS,
                "availability_ts_ms": NOW_MS,
                "mid_probability": 0.70,
                "spread": 0.03,
                "liquidity": 600.0,
                "imbalance": 0.30,
                "raw_payload": {"book": 1},
            }
        ],
        trades=[],
    )
    features, meta, available = resolve_prediction_market_event_snapshot(con, symbol="BTC", ts_ms=NOW_MS + 1)
    assert available is True
    assert meta["polymarket_available"] is True
    assert features["prediction_market_event_v1.available"] == 1.0
    assert round(features["prediction_market_event_v1.crypto_regulation_probability"], 2) == 0.70
    assert round(features["prediction_market_event_v1.cross_provider_dispersion"], 2) == 0.18
    assert features["prediction_market_event_v1.orderbook_imbalance"] == 0.30


def test_regulated_event_contract_features_resolve_by_mapped_event_type() -> None:
    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[
            {
                **_event_market(
                    "forecastex",
                    probability=0.64,
                    affected_assets=["SPY", "TLT"],
                    semantic_event_id="forecastex_macro_bpm",
                    resolution_semantics="official_building_permits_release",
                ),
                "provider_market_id": "forecastex:BPMI_0626_1556000:YES",
                "provider_contract_id": "BPMI_0626_1556000",
                "product_id": "BPMI",
                "event_type": "macro",
                "official_resolution_source": "US Census Building Permits release",
                "source_file_date": "2026-06-16",
                "source_file_kind": "prices",
                "refresh_cadence": "daily_eod",
                "provider_timestamp_ms": NOW_MS,
            },
            {
                **_event_market(
                    "forecastex",
                    probability=0.41,
                    affected_assets=["XLE", "USO"],
                    semantic_event_id="forecastex_energy_oil",
                    resolution_semantics="official_energy_settlement",
                ),
                "provider_market_id": "forecastex:COMCL_0626_80:YES",
                "provider_contract_id": "COMCL_0626_80",
                "product_id": "COMCL",
                "event_type": "energy",
            },
            {
                **_event_market(
                    "ibkr_event_contracts",
                    probability=0.57,
                    affected_assets=["UUP", "TLT"],
                    semantic_event_id="ibkr_rates_ff",
                    resolution_semantics="forecastx_rate_contract",
                ),
                "provider_market_id": "ibkr:12345:YES",
                "provider_contract_id": "12345",
                "product_id": "FF",
                "event_type": "fx_rates",
            },
        ],
        orderbooks=[],
        trades=[],
    )
    features, meta, available = resolve_prediction_market_event_snapshot(con, symbol="SPY", ts_ms=NOW_MS + 1)
    assert available is True
    assert features[f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_macro_probability"] == 0.64
    assert features[f"{PREDICTION_MARKET_EVENT_PREFIX}forecastex_available"] == 1.0
    assert features[f"{PREDICTION_MARKET_EVENT_PREFIX}ibkr_event_contract_available"] == 0.0
    assert meta["forecastex_available"] is True
    assert meta["ibkr_event_contract_available"] is False

    rates_features, rates_meta, rates_available = resolve_prediction_market_event_snapshot(con, symbol="TLT", ts_ms=NOW_MS + 1)
    assert rates_available is True
    assert rates_features[f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_fx_rates_probability"] == 0.57
    assert rates_features[f"{PREDICTION_MARKET_EVENT_PREFIX}ibkr_event_contract_available"] == 1.0
    assert rates_meta["ibkr_event_contract_available"] is True


def test_polymarket_event_features_ignore_halted_and_unliquid_markets() -> None:
    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[
            _event_market("polymarket", status="halted", probability=0.80),
            {
                **_event_market(
                    "polymarket",
                    status="active",
                    probability=0.60,
                    liquidity=0.0,
                    affected_assets=["BTC"],
                    semantic_event_id="crypto_low_liquidity",
                ),
                "provider_market_id": "polymarket:low-liquidity",
                "volume": 0.0,
                "volume_24h": 0.0,
                "open_interest": 0.0,
            },
        ],
        orderbooks=[],
        trades=[],
    )
    features, _meta, available = resolve_prediction_market_event_snapshot(con, symbol="BTC", ts_ms=NOW_MS + 1)
    assert available is False
    assert features["prediction_market_event_v1.available"] == 0.0


def test_model_snapshot_explicitly_includes_polymarket_event_group_and_stales_it() -> None:
    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[_event_market("polymarket", availability_ts_ms=NOW_MS - 40 * 60 * 60 * 1000)],
        orderbooks=[],
        trades=[],
    )
    snapshot = build_model_feature_snapshot(
        symbol="BTC",
        ts_ms=NOW_MS,
        feature_ids=list(PREDICTION_MARKET_EVENT_FEATURE_IDS),
        con=con,
    )
    assert snapshot["availability"][PREDICTION_MARKET_EVENT_FEATURE_GROUP] is False
    assert snapshot["pit_controls"][PREDICTION_MARKET_EVENT_FEATURE_GROUP]["reason_codes"] == ["feature_stale"]
    assert all(snapshot["features"][fid] == 0.0 for fid in PREDICTION_MARKET_EVENT_FEATURE_IDS)


def test_model_snapshot_includes_regulated_event_contract_features_pit_safely() -> None:
    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[
            {
                **_event_market(
                    "forecastex",
                    availability_ts_ms=NOW_MS,
                    probability=0.66,
                    affected_assets=["SPY"],
                    semantic_event_id="forecastex_macro",
                ),
                "provider_market_id": "forecastex:BPMI:YES",
                "provider_contract_id": "BPMI_0626_1556000",
                "product_id": "BPMI",
                "event_type": "macro",
            }
        ],
        orderbooks=[],
        trades=[],
    )
    feature_ids = [
        f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_macro_probability",
        f"{PREDICTION_MARKET_EVENT_PREFIX}forecastex_available",
        f"{PREDICTION_MARKET_EVENT_PREFIX}available",
    ]
    snapshot = build_model_feature_snapshot(symbol="SPY", ts_ms=NOW_MS + 1, feature_ids=feature_ids, con=con)
    assert snapshot["availability"][PREDICTION_MARKET_EVENT_FEATURE_GROUP] is True
    assert snapshot["features"][f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_macro_probability"] == 0.66
    assert snapshot["source_timestamps"][PREDICTION_MARKET_EVENT_FEATURE_GROUP]["direct_trading_authority"] is False

    future_snapshot = build_model_feature_snapshot(symbol="SPY", ts_ms=NOW_MS - 1, feature_ids=feature_ids, con=con)
    assert future_snapshot["availability"][PREDICTION_MARKET_EVENT_FEATURE_GROUP] is False
    assert future_snapshot["features"][f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_macro_probability"] == 0.0


def test_model_snapshot_explicitly_includes_prediction_market_group_and_stales_it() -> None:
    con = _memory_con()
    put_prediction_market_batch(
        con,
        now_ms=NOW_MS,
        events=[],
        markets=[_macro_market("kalshi", availability_ts_ms=NOW_MS - 40 * 60 * 60 * 1000)],
        orderbooks=[],
        trades=[],
    )
    snapshot = build_model_feature_snapshot(
        symbol="SPY",
        ts_ms=NOW_MS,
        feature_ids=list(PREDICTION_MARKET_MACRO_FEATURE_IDS),
        con=con,
    )
    assert snapshot["availability"][PREDICTION_MARKET_MACRO_FEATURE_GROUP] is False
    assert snapshot["pit_controls"][PREDICTION_MARKET_MACRO_FEATURE_GROUP]["reason_codes"] == ["feature_stale"]
    assert all(snapshot["features"][fid] == 0.0 for fid in PREDICTION_MARKET_MACRO_FEATURE_IDS)

    default_ids = feature_registry.default_feature_ids()
    assert not any(fid.startswith("prediction_market_macro_v1.") for fid in default_ids)


def test_prediction_market_feature_registry_is_shadow_only_and_jobs_registered() -> None:
    assert PREDICTION_MARKET_MACRO_FEATURE_GROUP in feature_registry.FEATURE_GROUPS
    assert PREDICTION_MARKET_EVENT_FEATURE_GROUP in feature_registry.FEATURE_GROUPS
    assert feature_registry.shadow_feature_ids(PREDICTION_MARKET_MACRO_FEATURE_IDS) == PREDICTION_MARKET_MACRO_FEATURE_IDS
    assert feature_registry.shadow_feature_ids(PREDICTION_MARKET_EVENT_FEATURE_IDS) == PREDICTION_MARKET_EVENT_FEATURE_IDS
    assert "prediction_market_macro_v1_shadow" in feature_registry.feature_set_tag_from_ids(PREDICTION_MARKET_MACRO_FEATURE_IDS)
    assert "prediction_market_event_v1_shadow" in feature_registry.feature_set_tag_from_ids(PREDICTION_MARKET_EVENT_FEATURE_IDS)

    try:
        feature_registry.assert_no_shadow_features(
            PREDICTION_MARKET_MACRO_FEATURE_IDS + PREDICTION_MARKET_EVENT_FEATURE_IDS,
            context="live_model_serving",
            model_name="x",
        )
    except ValueError as exc:
        assert "live_model_serving_shadow_features_forbidden:x" in str(exc)
        assert "prediction_market_event_v1.available" in str(exc)
    else:
        raise AssertionError("shadow prediction-market features were not rejected")

    assert job_registry.ALLOWED_JOBS["poll_kalshi_prediction_markets"][1] == "daemon"
    assert job_registry.ALLOWED_JOBS["poll_cme_fedwatch"][1] == "daemon"
    assert job_registry.ALLOWED_JOBS["poll_polymarket_prediction_markets"][1] == "daemon"
    assert job_registry.ALLOWED_JOBS["poll_forecastex_event_contracts"][1] == "daemon"
    assert job_registry.ALLOWED_JOBS["poll_forecastex_event_contracts"][3]["execution"] is False
    assert job_registry.ALLOWED_JOBS["poll_forecastex_event_contracts"][3]["direct_trading_authority"] is False
    assert job_registry.ALLOWED_JOBS["backfill_prediction_market_macro"][1] == "oneshot"
    assert f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_macro_probability" in PREDICTION_MARKET_EVENT_FEATURE_IDS
    assert f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_energy_probability" in PREDICTION_MARKET_EVENT_FEATURE_IDS
    assert f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_climate_weather_probability" in PREDICTION_MARKET_EVENT_FEATURE_IDS
    assert f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_fx_rates_probability" in PREDICTION_MARKET_EVENT_FEATURE_IDS
    assert f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_equity_index_probability" in PREDICTION_MARKET_EVENT_FEATURE_IDS
    assert f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_commodity_probability" in PREDICTION_MARKET_EVENT_FEATURE_IDS


def test_ibkr_event_contracts_disabled_is_read_only_noop() -> None:
    result = fetch_ibkr_event_contract_batch(settings={"ibkr_enabled": "0", "ibkr_contract_allowlist": '[{"conid":"123"}]'}, now_ms=NOW_MS)
    assert result["events"] == []
    assert result["markets"] == []
    assert result["health"]["enabled"] is False
    assert result["health"]["status"] == "disabled"
    assert result["health"]["read_only"] is True
    assert result["health"]["direct_trading_authority"] is False


def test_data_source_projection_keeps_prediction_market_jobs_disabled_until_enabled(monkeypatch) -> None:
    manager = DataSourceManager()
    rows = [
        {
            "source_key": "kalshi_prediction_market_macro",
            "display_name": "Kalshi",
            "source_type": "prediction_market_provider",
            "provider_name": "kalshi",
            "job_name": "poll_kalshi_prediction_markets",
            "enabled": 0,
            "settings_json": "{}",
            "credentials_enc": "",
            "key_version": "master_key",
        },
        {
            "source_key": "cme_fedwatch",
            "display_name": "CME FedWatch",
            "source_type": "prediction_market_provider",
            "provider_name": "cme_fedwatch",
            "job_name": "poll_cme_fedwatch",
            "enabled": 0,
            "settings_json": "{}",
            "credentials_enc": "",
            "key_version": "master_key",
        },
        {
            "source_key": "polymarket_event_signals",
            "display_name": "Polymarket",
            "source_type": "prediction_market_provider",
            "provider_name": "polymarket",
            "job_name": "poll_polymarket_prediction_markets",
            "enabled": 0,
            "settings_json": "{}",
            "credentials_enc": "",
            "key_version": "master_key",
        },
        {
            "source_key": "forecastex_event_contracts",
            "display_name": "ForecastEx",
            "source_type": "prediction_market_provider",
            "provider_name": "forecastex",
            "job_name": "poll_forecastex_event_contracts",
            "enabled": 0,
            "settings_json": '{"base_url":"https://forecastex.com","file_date_lookback":2,"ibkr_enabled":"0"}',
            "credentials_enc": "",
            "key_version": "master_key",
        },
    ]
    monkeypatch.setattr(manager, "initialize", lambda: None)
    monkeypatch.setattr(manager, "_fetch_rows", lambda: rows)

    defaults = [
        "poll_macro",
        "poll_kalshi_prediction_markets",
        "poll_cme_fedwatch",
        "poll_polymarket_prediction_markets",
        "poll_forecastex_event_contracts",
    ]
    assert manager.get_desired_ingestion_jobs(default_jobs=defaults) == []

    rows[0]["enabled"] = 1
    rows[1]["enabled"] = 1
    rows[2]["enabled"] = 1
    rows[3]["enabled"] = 1
    assert manager.get_desired_ingestion_jobs(default_jobs=defaults) == [
        "poll_kalshi_prediction_markets",
        "poll_cme_fedwatch",
        "poll_polymarket_prediction_markets",
        "poll_forecastex_event_contracts",
    ]
    polymarket_definition = manager._catalog["polymarket_event_signals"]
    assert polymarket_definition.credential_env == {}
    assert "wallet" not in polymarket_definition.setting_env
    assert "private_key" not in polymarket_definition.setting_env
    forecastex_definition = manager._catalog["forecastex_event_contracts"]
    assert forecastex_definition.default_enabled is False
    assert forecastex_definition.credential_env == {}
    assert forecastex_definition.setting_env["base_url"] == "FORECASTEX_BASE_URL"
    env = manager.build_job_environment("poll_forecastex_event_contracts")
    assert env["FORECASTEX_BASE_URL"] == "https://forecastex.com"
    assert env["FORECASTEX_FILE_DATE_LOOKBACK"] == "2"
    assert env["FORECASTEX_IBKR_ENABLED"] == "0"
