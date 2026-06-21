"""Read-only prediction-market provider adapters for macro expectations."""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import requests

from engine.data.prediction_market_storage import (
    PROVIDER_CATEGORY_MACRO,
    raw_payload_hash,
    safe_float,
    safe_int,
)


KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
CME_FEDWATCH_BASE_URL = "https://markets.api.cmegroup.com/fedwatch/v1"
POLYMARKET_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
POLYMARKET_DATA_BASE_URL = "https://data-api.polymarket.com"
POLYMARKET_CLOB_BASE_URL = "https://clob.polymarket.com"
PREDICTION_MARKET_MACRO_FEATURE_GROUP = "prediction_market_macro_v1"
PREDICTION_MARKET_MACRO_PREFIX = "prediction_market_macro_v1."
PREDICTION_MARKET_MACRO_FEATURE_IDS = [
    f"{PREDICTION_MARKET_MACRO_PREFIX}probability_level",
    f"{PREDICTION_MARKET_MACRO_PREFIX}probability_delta",
    f"{PREDICTION_MARKET_MACRO_PREFIX}event_urgency",
    f"{PREDICTION_MARKET_MACRO_PREFIX}liquidity_adjusted_probability_move",
    f"{PREDICTION_MARKET_MACRO_PREFIX}orderbook_imbalance",
    f"{PREDICTION_MARKET_MACRO_PREFIX}spread_quality",
    f"{PREDICTION_MARKET_MACRO_PREFIX}cme_vs_kalshi_disagreement",
    f"{PREDICTION_MARKET_MACRO_PREFIX}kalshi_available",
    f"{PREDICTION_MARKET_MACRO_PREFIX}cme_available",
    f"{PREDICTION_MARKET_MACRO_PREFIX}available",
]
PREDICTION_MARKET_EVENT_FEATURE_GROUP = "prediction_market_event_v1"
PREDICTION_MARKET_EVENT_PREFIX = "prediction_market_event_v1."
PREDICTION_MARKET_EVENT_FEATURE_IDS = [
    f"{PREDICTION_MARKET_EVENT_PREFIX}crypto_regulation_probability",
    f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_macro_probability",
    f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_energy_probability",
    f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_climate_weather_probability",
    f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_fx_rates_probability",
    f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_equity_index_probability",
    f"{PREDICTION_MARKET_EVENT_PREFIX}regulated_commodity_probability",
    f"{PREDICTION_MARKET_EVENT_PREFIX}probability_momentum",
    f"{PREDICTION_MARKET_EVENT_PREFIX}liquidity_adjusted_event_shock",
    f"{PREDICTION_MARKET_EVENT_PREFIX}orderbook_imbalance",
    f"{PREDICTION_MARKET_EVENT_PREFIX}spread_quality",
    f"{PREDICTION_MARKET_EVENT_PREFIX}event_urgency",
    f"{PREDICTION_MARKET_EVENT_PREFIX}market_attention",
    f"{PREDICTION_MARKET_EVENT_PREFIX}cross_provider_dispersion",
    f"{PREDICTION_MARKET_EVENT_PREFIX}polymarket_available",
    f"{PREDICTION_MARKET_EVENT_PREFIX}forecastex_available",
    f"{PREDICTION_MARKET_EVENT_PREFIX}ibkr_event_contract_available",
    f"{PREDICTION_MARKET_EVENT_PREFIX}available",
]

DEFAULT_MACRO_ASSETS = [
    "SPY",
    "QQQ",
    "IWM",
    "TLT",
    "IEF",
    "SHY",
    "GLD",
    "UUP",
    "XLF",
    "KRE",
    "IAT",
    "ITB",
    "XHB",
    "BTC",
    "ETH",
    "COIN",
    "HOOD",
]

PROVIDER_CATEGORY_EVENT_SIGNAL = "event_signal"

POLYMARKET_ALLOWED_EVENT_TYPES = {
    "ai_tech",
    "crypto_regulation",
    "election",
    "geopolitical",
    "narrative",
    "policy",
}
POLYMARKET_LIVE_MARKET_STATUSES = {"", "active", "open", "live", "trading", "tradable"}
POLYMARKET_BLOCKED_MARKET_STATUS_TOKENS = {
    "cancelled",
    "closed",
    "expired",
    "halt",
    "halted",
    "paused",
    "resolved",
    "settled",
    "suspended",
}
POLYMARKET_FORBIDDEN_SETTING_KEYS = {
    "api_key",
    "bridge",
    "bridge_address",
    "funder",
    "funding_account",
    "passphrase",
    "private_key",
    "secret",
    "signature",
    "trading_key",
    "wallet",
    "wallet_address",
}
DEFAULT_POLYMARKET_ASSET_BASKETS: dict[str, list[str]] = {
    "crypto_regulation": [
        "BTC",
        "ETH",
        "SOL",
        "COIN",
        "HOOD",
        "MSTR",
        "MARA",
        "RIOT",
        "CLSK",
        "HUT",
        "BITF",
        "IREN",
        "IBIT",
        "ETHE",
    ],
    "btc": ["BTC", "COIN", "MSTR", "MARA", "RIOT", "CLSK", "HUT", "IBIT"],
    "eth": ["ETH", "COIN", "ETHE"],
    "sol": ["SOL", "COIN"],
    "miners": ["MARA", "RIOT", "CLSK", "HUT", "BITF", "IREN", "WULF"],
    "risk_on": ["SPY", "QQQ", "IWM", "BTC", "ETH", "SOL", "COIN", "HOOD", "MSTR"],
    "risk_off": ["TLT", "SHY", "GLD", "UUP"],
    "policy": ["SPY", "QQQ", "IWM", "TLT", "XLF", "KRE", "BTC", "ETH", "COIN", "HOOD"],
    "election": ["SPY", "QQQ", "IWM", "XLF", "KRE", "TLT", "BTC", "COIN"],
    "geopolitical": ["SPY", "QQQ", "IWM", "TLT", "GLD", "UUP", "XLE", "USO", "BTC"],
    "ai_tech": ["QQQ", "XLK", "SMH", "NVDA", "MSFT", "GOOGL", "META"],
    "narrative": ["SPY", "QQQ", "IWM", "BTC", "ETH"],
}


def utc_ms() -> int:
    return int(time.time() * 1000)


def parse_ts_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw <= 0:
            return None
        return int(raw * 1000) if raw < 10_000_000_000 else int(raw)
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return parse_ts_ms(float(text))
    normalized = text.replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            return int(datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except Exception:
            continue
    return None


def parse_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except Exception:
            pass
    return [part.strip() for part in text.split(",") if part.strip()]


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            parsed = json.loads(text)
        except Exception:
            return parse_list(text)
        if isinstance(parsed, list):
            return list(parsed)
        return []
    return []


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip().replace("$", "").replace(".", "-")


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    text = str(raw or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def macro_assets_from_settings(settings: Mapping[str, Any] | None = None) -> list[str]:
    raw = (settings or {}).get("asset_map_json") or os.environ.get("PREDICTION_MARKET_MACRO_ASSET_MAP_JSON")
    if raw:
        try:
            parsed = json.loads(str(raw)) if isinstance(raw, str) else raw
            if isinstance(parsed, dict):
                assets: list[str] = []
                for values in parsed.values():
                    assets.extend(parse_list(values))
                out = sorted({asset.upper() for asset in assets if asset})
                if out:
                    return out
            if isinstance(parsed, list):
                out = sorted({str(asset).upper().strip() for asset in parsed if str(asset).strip()})
                if out:
                    return out
        except Exception:
            pass
    configured = parse_list((settings or {}).get("affected_assets") or os.environ.get("PREDICTION_MARKET_MACRO_ASSETS"))
    if configured:
        return sorted({asset.upper() for asset in configured})
    return list(DEFAULT_MACRO_ASSETS)


def polymarket_asset_baskets_from_settings(settings: Mapping[str, Any] | None = None) -> dict[str, list[str]]:
    raw = (
        (settings or {}).get("asset_basket_map_json")
        or (settings or {}).get("asset_map_json")
        or os.environ.get("POLYMARKET_ASSET_BASKET_MAP_JSON")
        or os.environ.get("POLYMARKET_ASSET_MAP_JSON")
    )
    baskets = {str(key): list(values) for key, values in DEFAULT_POLYMARKET_ASSET_BASKETS.items()}
    parsed = _json_obj(raw)
    for key, values in parsed.items():
        assets = [_clean_symbol(item) for item in parse_list(values)]
        if assets:
            baskets[str(key).lower().strip()] = sorted({asset for asset in assets if asset})
    return baskets


def polymarket_semantic_event_map_from_settings(settings: Mapping[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    raw = (settings or {}).get("semantic_event_map_json") or os.environ.get("POLYMARKET_SEMANTIC_EVENT_MAP_JSON")
    parsed = _json_obj(raw)
    out: dict[str, dict[str, Any]] = {}
    for key, value in parsed.items():
        if not str(key or "").strip() or not isinstance(value, Mapping):
            continue
        item = dict(value)
        event_id = str(item.get("semantic_event_id") or item.get("event_id") or "").strip()
        semantics = str(item.get("resolution_semantics") or item.get("resolution") or "").strip()
        if not event_id or not semantics:
            continue
        out[str(key).strip()] = {
            "semantic_event_id": event_id,
            "resolution_semantics": semantics,
            "event_type": str(item.get("event_type") or "").strip(),
            "affected_assets": [_clean_symbol(asset) for asset in parse_list(item.get("affected_assets") or item.get("assets"))],
        }
    return out


def validate_polymarket_data_only_settings(
    settings: Mapping[str, Any] | None = None,
    credentials: Mapping[str, Any] | None = None,
) -> None:
    """Reject wallet, bridge, or trading credential material for Polymarket."""

    lowered_settings = {str(key).strip().lower() for key in dict(settings or {}).keys()}
    lowered_credentials = {str(key).strip().lower() for key in dict(credentials or {}).keys()}
    forbidden = sorted((lowered_settings | lowered_credentials) & POLYMARKET_FORBIDDEN_SETTING_KEYS)
    if forbidden:
        raise RuntimeError(f"polymarket_data_only_forbidden_keys:{','.join(forbidden)}")
    if lowered_credentials:
        raise RuntimeError("polymarket_credentials_forbidden")


def _probability(value: Any) -> float | None:
    if value is None or value == "":
        return None
    out = safe_float(value, 0.0)
    if out > 1.0:
        out = out / 100.0
    return max(0.0, min(1.0, float(out)))


def _price_probability(payload: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in payload and payload.get(key) not in (None, ""):
            return _probability(payload.get(key))
    return None


def _http_get_json(session: Any, base_url: str, path: str, *, params: Mapping[str, Any] | None = None, headers: Mapping[str, str] | None = None, timeout_s: float = 10.0) -> dict[str, Any]:
    url = f"{str(base_url).rstrip('/')}/{str(path).lstrip('/')}"
    response = session.get(url, params=dict(params or {}), headers=dict(headers or {}), timeout=float(timeout_s))
    response.raise_for_status()
    return dict(response.json() or {})


def _http_get_payload(
    session: Any,
    base_url: str,
    path: str,
    *,
    params: Mapping[str, Any] | None = None,
    timeout_s: float = 10.0,
) -> Any:
    url = f"{str(base_url).rstrip('/')}/{str(path).lstrip('/')}"
    response = session.get(url, params=dict(params or {}), timeout=float(timeout_s))
    response.raise_for_status()
    return response.json()


def _paginate(
    session: Any,
    base_url: str,
    path: str,
    *,
    params: Mapping[str, Any],
    collection_key: str,
    max_pages: int,
    timeout_s: float,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor = ""
    for _idx in range(max(1, int(max_pages))):
        query = dict(params or {})
        if cursor:
            query["cursor"] = cursor
        payload = _http_get_json(session, base_url, path, params=query, timeout_s=float(timeout_s))
        rows = payload.get(collection_key) or []
        out.extend([dict(row or {}) for row in rows if isinstance(row, Mapping)])
        cursor = str(payload.get("cursor") or "").strip()
        if not cursor:
            break
    return out


def normalize_kalshi_event(event: Mapping[str, Any], *, now_ms: int, affected_assets: Sequence[str]) -> dict[str, Any]:
    event_ticker = str(event.get("event_ticker") or event.get("ticker") or "").strip()
    source_ts_ms = parse_ts_ms(event.get("last_updated_ts") or event.get("updated_time")) or int(now_ms)
    event_ts_ms = parse_ts_ms(event.get("strike_date") or event.get("expected_expiration_time") or event.get("close_time"))
    category = str(event.get("category") or "").strip() or PROVIDER_CATEGORY_MACRO
    return {
        "provider_name": "kalshi",
        "provider_event_id": event_ticker,
        "event_ticker": event_ticker,
        "series_ticker": str(event.get("series_ticker") or "").strip(),
        "title": str(event.get("title") or event.get("sub_title") or "").strip(),
        "provider_category": PROVIDER_CATEGORY_MACRO,
        "event_type": _infer_event_type(category, str(event.get("title") or "")),
        "event_ts_ms": event_ts_ms,
        "resolution_ts_ms": parse_ts_ms(event.get("settlement_ts") or event.get("strike_date")),
        "source_ts_ms": int(source_ts_ms),
        "availability_ts_ms": int(now_ms),
        "affected_assets": list(affected_assets),
        "raw_payload_hash": raw_payload_hash(event),
        "raw_payload": dict(event),
    }


def normalize_kalshi_market(market: Mapping[str, Any], *, now_ms: int, affected_assets: Sequence[str]) -> dict[str, Any]:
    ticker = str(market.get("ticker") or "").strip()
    event_ticker = str(market.get("event_ticker") or "").strip()
    source_ts_ms = parse_ts_ms(market.get("updated_time") or market.get("created_time")) or int(now_ms)
    bid = _price_probability(market, "yes_bid_dollars", "yes_bid")
    ask = _price_probability(market, "yes_ask_dollars", "yes_ask")
    last = _price_probability(market, "last_price_dollars", "last_price")
    previous = _price_probability(market, "previous_price_dollars", "previous_price", "previous_yes_bid_dollars")
    if bid is not None and ask is not None:
        probability = (bid + ask) / 2.0
        spread = max(0.0, float(ask - bid))
    else:
        probability = last if last is not None else bid
        spread = None
    return {
        "provider_name": "kalshi",
        "provider_market_id": ticker,
        "provider_event_id": event_ticker,
        "market_ticker": ticker,
        "series_ticker": str(market.get("series_ticker") or "").strip(),
        "title": str(market.get("title") or "").strip(),
        "subtitle": str(market.get("subtitle") or market.get("yes_sub_title") or "").strip(),
        "provider_category": PROVIDER_CATEGORY_MACRO,
        "event_type": _infer_event_type(str(market.get("series_ticker") or ""), str(market.get("title") or "")),
        "status": str(market.get("status") or "").strip(),
        "probability": probability,
        "previous_probability": previous,
        "probability_delta": (float(probability) - float(previous)) if probability is not None and previous is not None else None,
        "bid_probability": bid,
        "ask_probability": ask,
        "last_price": last,
        "liquidity": safe_float(market.get("liquidity_dollars"), 0.0),
        "volume": safe_float(market.get("volume_fp"), 0.0),
        "volume_24h": safe_float(market.get("volume_24h_fp"), 0.0),
        "open_interest": safe_float(market.get("open_interest_fp"), 0.0),
        "spread": spread,
        "event_ts_ms": parse_ts_ms(market.get("occurrence_datetime") or market.get("expected_expiration_time")),
        "close_ts_ms": parse_ts_ms(market.get("close_time")),
        "resolution_ts_ms": parse_ts_ms(market.get("settlement_ts") or market.get("expiration_time")),
        "source_ts_ms": int(source_ts_ms),
        "availability_ts_ms": int(now_ms),
        "affected_assets": list(affected_assets),
        "raw_payload_hash": raw_payload_hash(market),
        "raw_payload": dict(market),
    }


def normalize_kalshi_orderbook(
    market_ticker: str,
    payload: Mapping[str, Any],
    *,
    now_ms: int,
) -> dict[str, Any]:
    orderbook = dict(payload.get("orderbook_fp") or payload.get("orderbook") or payload or {})
    yes_levels = _levels(orderbook.get("yes_dollars") or orderbook.get("yes") or [])
    no_levels = _levels(orderbook.get("no_dollars") or orderbook.get("no") or [])
    best_yes_bid = max((price for price, _qty in yes_levels), default=None)
    best_no_bid = max((price for price, _qty in no_levels), default=None)
    best_yes_ask = (1.0 - best_no_bid) if best_no_bid is not None else None
    best_no_ask = (1.0 - best_yes_bid) if best_yes_bid is not None else None
    if best_yes_bid is not None and best_yes_ask is not None:
        mid = (best_yes_bid + best_yes_ask) / 2.0
        spread = max(0.0, best_yes_ask - best_yes_bid)
    else:
        mid = best_yes_bid
        spread = None
    yes_depth = sum(qty for _price, qty in yes_levels)
    no_depth = sum(qty for _price, qty in no_levels)
    depth = max(1.0, yes_depth + no_depth)
    return {
        "provider_name": "kalshi",
        "provider_market_id": str(market_ticker),
        "source_ts_ms": int(now_ms),
        "availability_ts_ms": int(now_ms),
        "best_yes_bid": best_yes_bid,
        "best_yes_ask": best_yes_ask,
        "best_no_bid": best_no_bid,
        "best_no_ask": best_no_ask,
        "mid_probability": mid,
        "spread": spread,
        "yes_depth": yes_depth,
        "no_depth": no_depth,
        "liquidity": yes_depth + no_depth,
        "imbalance": (yes_depth - no_depth) / depth,
        "raw_payload_hash": raw_payload_hash(payload),
        "raw_payload": dict(payload),
    }


def _levels(value: Any) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    if not isinstance(value, list):
        return out
    for item in value:
        if isinstance(item, Mapping):
            price = item.get("price") or item.get("price_dollars")
            qty = item.get("quantity") or item.get("size") or item.get("count")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price, qty = item[0], item[1]
        else:
            continue
        parsed = _probability(price)
        if parsed is None:
            continue
        out.append((float(parsed), max(0.0, safe_float(qty, 0.0))))
    return out


def _polymarket_collection(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item or {}) for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        for key in keys:
            child = payload.get(key)
            if isinstance(child, list):
                return [dict(item or {}) for item in child if isinstance(item, Mapping)]
        for key in ("data", "results", "events", "markets", "trades", "history"):
            child = payload.get(key)
            if isinstance(child, list):
                return [dict(item or {}) for item in child if isinstance(item, Mapping)]
    return []


def _polymarket_tags(value: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    for key in ("tags", "tag", "categories", "category"):
        raw = value.get(key)
        if isinstance(raw, str):
            out.extend(parse_list(raw))
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, Mapping):
                    out.extend(parse_list(item.get("label") or item.get("name") or item.get("slug") or item.get("id")))
                else:
                    out.extend(parse_list(item))
        elif raw is not None:
            out.extend(parse_list(raw))
    return [_slug(item) for item in out if _slug(item)]


def _polymarket_haystack(event: Mapping[str, Any] | None, market: Mapping[str, Any] | None = None) -> str:
    parts: list[str] = []
    for payload in (event or {}, market or {}):
        parts.extend(
            str(payload.get(key) or "")
            for key in (
                "title",
                "question",
                "description",
                "slug",
                "category",
                "subcategory",
                "eventSlug",
                "groupItemTitle",
            )
        )
        parts.extend(_polymarket_tags(payload))
    return " ".join(parts).lower()


def _infer_polymarket_event_type(event: Mapping[str, Any] | None, market: Mapping[str, Any] | None = None) -> str:
    text = _polymarket_haystack(event, market)
    if any(token in text for token in ("crypto", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "sec", "etf", "coinbase", "binance")):
        if any(token in text for token in ("regulation", "regulatory", "lawsuit", "sec", "cftc", "etf", "stablecoin", "reserve")):
            return "crypto_regulation"
    if any(token in text for token in ("election", "president", "senate", "house", "congress", "trump", "biden", "primary")):
        return "election"
    if any(token in text for token in ("war", "ceasefire", "ukraine", "russia", "china", "taiwan", "israel", "iran", "tariff", "sanction")):
        return "geopolitical"
    if any(token in text for token in ("ai", "artificial intelligence", "openai", "nvidia", "semiconductor", "chips", "tech")):
        return "ai_tech"
    if any(token in text for token in ("fed", "rate", "regulation", "policy", "bill", "court", "supreme", "tax")):
        return "policy"
    return "narrative"


def _market_status_key(market: Mapping[str, Any]) -> str:
    for key in ("status", "state"):
        value = str(market.get(key) or "").strip().lower()
        if value:
            return value
    if bool(market.get("closed")) or bool(market.get("resolved")) or bool(market.get("archived")):
        return "closed"
    if bool(market.get("active")) or bool(market.get("acceptingOrders")) or bool(market.get("enableOrderBook")):
        return "active"
    return ""


def polymarket_market_is_live(market: Mapping[str, Any]) -> bool:
    status = _market_status_key(market)
    if any(token in status for token in POLYMARKET_BLOCKED_MARKET_STATUS_TOKENS):
        return False
    if status and status not in POLYMARKET_LIVE_MARKET_STATUSES:
        return False
    if market.get("active") is False:
        return False
    if market.get("closed") is True or market.get("archived") is True or market.get("resolved") is True:
        return False
    return True


def _polymarket_numeric(market: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        if market.get(key) not in (None, ""):
            return safe_float(market.get(key), 0.0)
    return 0.0


def _polymarket_outcome_context(market: Mapping[str, Any]) -> tuple[str, str, float | None, list[str], list[float]]:
    outcomes = [str(item or "").strip() for item in _json_list(market.get("outcomes")) if str(item or "").strip()]
    token_ids = [str(item or "").strip() for item in _json_list(market.get("clobTokenIds") or market.get("clob_token_ids")) if str(item or "").strip()]
    prices = [_probability(item) for item in _json_list(market.get("outcomePrices") or market.get("outcome_prices"))]
    yes_idx = 0
    for idx, outcome in enumerate(outcomes):
        if outcome.lower() in {"yes", "y"}:
            yes_idx = idx
            break
    outcome_name = outcomes[yes_idx] if yes_idx < len(outcomes) else "Yes"
    token_id = token_ids[yes_idx] if yes_idx < len(token_ids) else str(market.get("token_id") or market.get("clobTokenId") or "").strip()
    probability = prices[yes_idx] if yes_idx < len(prices) else _price_probability(market, "lastTradePrice", "last_price", "bestBid", "best_bid")
    parsed_prices = [float(item) for item in prices if item is not None]
    return outcome_name, token_id, probability, outcomes, parsed_prices


def _polymarket_semantic_mapping(
    event: Mapping[str, Any] | None,
    market: Mapping[str, Any],
    mapping: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidates = [
        str(market.get("conditionId") or market.get("condition_id") or "").strip(),
        str(market.get("id") or "").strip(),
        str(market.get("slug") or "").strip(),
        str(event.get("id") or "").strip() if event else "",
        str(event.get("slug") or "").strip() if event else "",
    ]
    for key in candidates:
        if key and key in mapping:
            return dict(mapping[key] or {})
    return {}


def _polymarket_assets(
    event: Mapping[str, Any] | None,
    market: Mapping[str, Any],
    *,
    event_type: str,
    baskets: Mapping[str, Sequence[str]],
    semantic: Mapping[str, Any] | None = None,
) -> list[str]:
    semantic_assets = [_clean_symbol(asset) for asset in parse_list((semantic or {}).get("affected_assets") or (semantic or {}).get("assets"))]
    if semantic_assets:
        return sorted({asset for asset in semantic_assets if asset})
    text = _polymarket_haystack(event, market)
    assets: list[str] = []
    for key, values in baskets.items():
        key_text = str(key or "").lower().strip()
        if not key_text:
            continue
        tokens = {key_text, key_text.replace("_", " "), key_text.replace("-", " ")}
        if key_text == event_type or any(token and token in text for token in tokens):
            assets.extend(_clean_symbol(asset) for asset in values)
    if not assets and event_type in baskets:
        assets.extend(_clean_symbol(asset) for asset in baskets[event_type])
    return sorted({asset for asset in assets if asset})


def _polymarket_event_passes_filters(event: Mapping[str, Any], settings: Mapping[str, Any]) -> bool:
    slugs = {_slug(item) for item in parse_list(settings.get("slugs") or settings.get("slug_allowlist") or os.environ.get("POLYMARKET_SLUG_ALLOWLIST"))}
    if slugs and _slug(event.get("slug") or event.get("eventSlug")) not in slugs:
        return False
    tags = {_slug(item) for item in parse_list(settings.get("tags") or os.environ.get("POLYMARKET_TAGS"))}
    event_tags = set(_polymarket_tags(event))
    if tags and not (tags & event_tags):
        return False
    categories = {_slug(item) for item in parse_list(settings.get("category_filters") or os.environ.get("POLYMARKET_CATEGORY_FILTERS"))}
    if categories:
        event_categories = {
            _slug(event.get("category")),
            _slug(event.get("subcategory")),
            *_polymarket_tags(event),
        }
        if not (categories & {item for item in event_categories if item}):
            return False
    keywords = [item.lower() for item in parse_list(settings.get("keyword_allowlist") or os.environ.get("POLYMARKET_KEYWORD_ALLOWLIST"))]
    if keywords and not any(keyword in _polymarket_haystack(event) for keyword in keywords):
        return False
    return True


def _polymarket_market_passes_filters(
    market: Mapping[str, Any],
    event: Mapping[str, Any] | None,
    settings: Mapping[str, Any],
    *,
    affected_assets: Sequence[str],
) -> bool:
    if not polymarket_market_is_live(market):
        return False
    allowed_status = {item.lower() for item in parse_list(settings.get("status") or os.environ.get("POLYMARKET_MARKET_STATUS"))}
    status = _market_status_key(market)
    if allowed_status and status not in allowed_status:
        return False
    event_type_filters = {item.lower() for item in parse_list(settings.get("event_type_filters") or os.environ.get("POLYMARKET_EVENT_TYPE_FILTERS"))}
    event_type = _infer_polymarket_event_type(event, market)
    if event_type_filters and event_type not in event_type_filters:
        return False
    keywords = [item.lower() for item in parse_list(settings.get("keyword_allowlist") or os.environ.get("POLYMARKET_KEYWORD_ALLOWLIST"))]
    if keywords and not any(keyword in _polymarket_haystack(event, market) for keyword in keywords):
        return False
    min_liquidity = safe_float(settings.get("min_liquidity") or os.environ.get("POLYMARKET_MIN_LIQUIDITY"), 0.0)
    min_volume = safe_float(settings.get("min_volume") or os.environ.get("POLYMARKET_MIN_VOLUME"), 0.0)
    min_open_interest = safe_float(settings.get("min_open_interest") or os.environ.get("POLYMARKET_MIN_OPEN_INTEREST"), 0.0)
    liquidity = _polymarket_numeric(market, "liquidity", "liquidityNum", "liquidity_num")
    volume = _polymarket_numeric(market, "volume", "volumeNum", "volume_num", "volume24hr", "volume24hrClob")
    open_interest = _polymarket_numeric(market, "openInterest", "open_interest")
    if liquidity < min_liquidity or volume < min_volume or open_interest < min_open_interest:
        return False
    if not list(affected_assets or []) and _env_bool("POLYMARKET_REQUIRE_ASSET_MAPPING", True):
        return False
    return bool((liquidity + volume + open_interest) > 0.0)


def normalize_polymarket_event(
    event: Mapping[str, Any],
    *,
    now_ms: int,
    affected_assets: Sequence[str],
    event_type: str | None = None,
    semantic_event_id: str = "",
    resolution_semantics: str = "",
) -> dict[str, Any]:
    event_id = str(event.get("id") or event.get("slug") or event.get("eventSlug") or raw_payload_hash(event)).strip()
    source_ts_ms = (
        parse_ts_ms(event.get("updatedAt") or event.get("updated_at") or event.get("createdAt") or event.get("created_at"))
        or int(now_ms)
    )
    end_ts_ms = parse_ts_ms(
        event.get("endDate")
        or event.get("end_date")
        or event.get("resolutionDate")
        or event.get("resolution_date")
        or event.get("closedTime")
    )
    inferred_event_type = str(event_type or _infer_polymarket_event_type(event, None)).strip()
    return {
        "provider_name": "polymarket",
        "provider_event_id": event_id,
        "event_ticker": str(event.get("slug") or event_id),
        "series_ticker": str(event.get("seriesSlug") or event.get("category") or "").strip(),
        "title": _clean_text(event.get("title") or event.get("question") or event.get("slug") or event_id),
        "provider_category": PROVIDER_CATEGORY_EVENT_SIGNAL,
        "event_type": inferred_event_type,
        "semantic_event_id": str(semantic_event_id or ""),
        "resolution_semantics": str(resolution_semantics or ""),
        "event_ts_ms": end_ts_ms,
        "resolution_ts_ms": end_ts_ms,
        "source_ts_ms": int(source_ts_ms),
        "availability_ts_ms": int(now_ms),
        "affected_assets": list(affected_assets),
        "raw_payload_hash": raw_payload_hash(event),
        "raw_payload": dict(event),
    }


def normalize_polymarket_market(
    market: Mapping[str, Any],
    *,
    event: Mapping[str, Any] | None = None,
    now_ms: int,
    affected_assets: Sequence[str],
    semantic_event_id: str = "",
    resolution_semantics: str = "",
    event_type: str | None = None,
) -> dict[str, Any]:
    outcome_name, token_id, outcome_probability, outcomes, outcome_prices = _polymarket_outcome_context(market)
    condition_id = str(market.get("conditionId") or market.get("condition_id") or market.get("condition_id_hex") or "").strip()
    market_id = str(market.get("id") or market.get("slug") or condition_id or token_id or raw_payload_hash(market)).strip()
    provider_market_id = f"{condition_id or market_id}:{token_id}" if token_id and condition_id else (token_id or condition_id or market_id)
    source_ts_ms = (
        parse_ts_ms(market.get("updatedAt") or market.get("updated_at") or market.get("createdAt") or market.get("created_at"))
        or int(now_ms)
    )
    event_ts_ms = parse_ts_ms(
        market.get("endDate")
        or market.get("end_date")
        or market.get("resolutionDate")
        or (event or {}).get("endDate")
        or (event or {}).get("end_date")
    )
    bid = _price_probability(market, "bestBid", "best_bid", "bid")
    ask = _price_probability(market, "bestAsk", "best_ask", "ask")
    spread = max(0.0, float(ask - bid)) if bid is not None and ask is not None else None
    probability = ((bid + ask) / 2.0) if bid is not None and ask is not None else outcome_probability
    previous = _price_probability(market, "previousPrice", "previous_price", "oneDayPriceChange")
    raw_change = market.get("oneDayPriceChange", market.get("one_day_price_change", market.get("priceChange24hr")))
    one_day_change = None
    if raw_change not in (None, ""):
        one_day_change = safe_float(raw_change, 0.0)
        if abs(float(one_day_change)) > 1.0:
            one_day_change = float(one_day_change) / 100.0
    probability_delta = one_day_change if one_day_change is not None else (
        float(probability) - float(previous) if probability is not None and previous is not None else None
    )
    inferred_event_type = str(event_type or _infer_polymarket_event_type(event, market)).strip()
    return {
        "provider_name": "polymarket",
        "provider_market_id": provider_market_id,
        "provider_event_id": str((event or {}).get("id") or (event or {}).get("slug") or market.get("event_id") or market.get("eventId") or ""),
        "market_ticker": str(market.get("slug") or provider_market_id),
        "series_ticker": str((event or {}).get("category") or market.get("category") or "").strip(),
        "title": _clean_text(market.get("question") or market.get("title") or (event or {}).get("title") or ""),
        "subtitle": str(outcome_name or ""),
        "provider_category": PROVIDER_CATEGORY_EVENT_SIGNAL,
        "event_type": inferred_event_type,
        "status": _market_status_key(market) or ("active" if polymarket_market_is_live(market) else "closed"),
        "probability": probability,
        "previous_probability": previous,
        "probability_delta": probability_delta,
        "bid_probability": bid,
        "ask_probability": ask,
        "last_price": _price_probability(market, "lastTradePrice", "last_price", "lastPrice") or probability,
        "liquidity": _polymarket_numeric(market, "liquidity", "liquidityNum", "liquidity_num"),
        "volume": _polymarket_numeric(market, "volume", "volumeNum", "volume_num"),
        "volume_24h": _polymarket_numeric(market, "volume24hr", "volume24hrClob", "volume_24h"),
        "open_interest": _polymarket_numeric(market, "openInterest", "open_interest"),
        "spread": spread,
        "event_ts_ms": event_ts_ms,
        "close_ts_ms": event_ts_ms,
        "resolution_ts_ms": event_ts_ms,
        "source_ts_ms": int(source_ts_ms),
        "availability_ts_ms": int(now_ms),
        "affected_assets": list(affected_assets),
        "condition_id": condition_id,
        "token_id": token_id,
        "outcome_name": str(outcome_name or ""),
        "semantic_event_id": str(semantic_event_id or ""),
        "resolution_semantics": str(resolution_semantics or ""),
        "raw_payload_hash": raw_payload_hash(market),
        "raw_payload": {
            **dict(market),
            "_normalized_outcomes": outcomes,
            "_normalized_outcome_prices": outcome_prices,
        },
    }


def normalize_polymarket_orderbook(
    provider_market_id: str,
    payload: Mapping[str, Any],
    *,
    now_ms: int,
    condition_id: str = "",
    token_id: str = "",
    midpoint: Any = None,
    spread: Any = None,
    last_trade: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    bids = _levels(payload.get("bids") or payload.get("buy") or payload.get("yes") or [])
    asks = _levels(payload.get("asks") or payload.get("sell") or [])
    best_yes_bid = max((price for price, _qty in bids), default=None)
    best_yes_ask = min((price for price, _qty in asks), default=None)
    midpoint_value = midpoint.get("mid") if isinstance(midpoint, Mapping) else midpoint
    spread_value = spread.get("spread") if isinstance(spread, Mapping) else spread
    mid = _probability(midpoint_value)
    parsed_spread = _probability(spread_value)
    if mid is None and best_yes_bid is not None and best_yes_ask is not None:
        mid = (best_yes_bid + best_yes_ask) / 2.0
    if parsed_spread is None and best_yes_bid is not None and best_yes_ask is not None:
        parsed_spread = max(0.0, best_yes_ask - best_yes_bid)
    bid_depth = sum(qty for _price, qty in bids)
    ask_depth = sum(qty for _price, qty in asks)
    depth = max(1.0, bid_depth + ask_depth)
    raw = {
        "book": dict(payload),
        "midpoint": midpoint,
        "spread": spread,
        "last_trade": dict(last_trade or {}),
    }
    return {
        "provider_name": "polymarket",
        "provider_market_id": str(provider_market_id),
        "condition_id": str(condition_id or ""),
        "token_id": str(token_id or ""),
        "source_ts_ms": int(now_ms),
        "availability_ts_ms": int(now_ms),
        "best_yes_bid": best_yes_bid,
        "best_yes_ask": best_yes_ask,
        "best_no_bid": (1.0 - best_yes_ask) if best_yes_ask is not None else None,
        "best_no_ask": (1.0 - best_yes_bid) if best_yes_bid is not None else None,
        "mid_probability": mid,
        "spread": parsed_spread,
        "yes_depth": bid_depth,
        "no_depth": ask_depth,
        "liquidity": bid_depth + ask_depth,
        "imbalance": (bid_depth - ask_depth) / depth,
        "raw_payload_hash": raw_payload_hash(raw),
        "raw_payload": raw,
    }


def normalize_polymarket_price_history(
    provider_market_id: str,
    payload: Any,
    *,
    now_ms: int,
    condition_id: str = "",
    token_id: str = "",
) -> list[dict[str, Any]]:
    rows = _polymarket_collection(payload, "history", "prices", "trades")
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        ts_ms = parse_ts_ms(row.get("t") or row.get("ts") or row.get("timestamp") or row.get("createdAt")) or int(now_ms)
        price = _probability(row.get("p") or row.get("price") or row.get("value"))
        if price is None:
            continue
        trade_id = str(row.get("id") or row.get("trade_id") or f"{provider_market_id}:{ts_ms}:{idx}")
        out.append(
            {
                "provider_name": "polymarket",
                "provider_market_id": str(provider_market_id),
                "condition_id": str(condition_id or ""),
                "token_id": str(token_id or ""),
                "trade_id": trade_id,
                "trade_ts_ms": int(ts_ms),
                "source_ts_ms": int(ts_ms),
                "availability_ts_ms": int(now_ms),
                "price": price,
                "size": safe_float(row.get("size") or row.get("shares") or row.get("amount"), 0.0) if row.get("size") is not None or row.get("shares") is not None else None,
                "side": str(row.get("side") or row.get("type") or "history"),
                "raw_payload_hash": raw_payload_hash(row),
                "raw_payload": dict(row),
            }
        )
    return out


def _polymarket_recent_trades_to_history(
    provider_market_id: str,
    payload: Any,
    *,
    now_ms: int,
    condition_id: str = "",
    token_id: str = "",
) -> list[dict[str, Any]]:
    rows = _polymarket_collection(payload, "trades", "data", "results")
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        ts_ms = parse_ts_ms(row.get("timestamp") or row.get("createdAt") or row.get("created_at") or row.get("time")) or int(now_ms)
        price = _probability(row.get("price") or row.get("yes_price") or row.get("outcomePrice"))
        if price is None:
            continue
        out.append(
            {
                "provider_name": "polymarket",
                "provider_market_id": str(provider_market_id),
                "condition_id": str(condition_id or row.get("conditionId") or ""),
                "token_id": str(token_id or row.get("asset") or row.get("token_id") or ""),
                "trade_id": str(row.get("id") or row.get("transactionHash") or row.get("txHash") or f"{provider_market_id}:data:{ts_ms}:{idx}"),
                "trade_ts_ms": int(ts_ms),
                "source_ts_ms": int(ts_ms),
                "availability_ts_ms": int(now_ms),
                "price": price,
                "size": safe_float(row.get("size") or row.get("amount") or row.get("shares"), 0.0),
                "side": str(row.get("side") or row.get("outcome") or "trade"),
                "raw_payload_hash": raw_payload_hash(row),
                "raw_payload": dict(row),
            }
        )
    return out


def fetch_polymarket_event_batch(
    *,
    settings: Mapping[str, Any] | None = None,
    now_ms: int | None = None,
    session: Any | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch read-only Polymarket event-market signals from public endpoints."""

    cfg = dict(settings or {})
    validate_polymarket_data_only_settings(cfg, {})
    now = int(now_ms or utc_ms())
    sess = session or requests.Session()
    gamma_base_url = str(cfg.get("gamma_base_url") or cfg.get("base_url") or os.environ.get("POLYMARKET_GAMMA_BASE_URL") or POLYMARKET_GAMMA_BASE_URL).rstrip("/")
    clob_base_url = str(cfg.get("clob_base_url") or os.environ.get("POLYMARKET_CLOB_BASE_URL") or POLYMARKET_CLOB_BASE_URL).rstrip("/")
    data_base_url = str(cfg.get("data_base_url") or os.environ.get("POLYMARKET_DATA_BASE_URL") or POLYMARKET_DATA_BASE_URL).rstrip("/")
    timeout_s = float(cfg.get("timeout_s") or os.environ.get("POLYMARKET_TIMEOUT_S") or 10.0)
    limit = max(1, min(500, safe_int(cfg.get("limit") or os.environ.get("POLYMARKET_LIMIT"), 100)))
    max_pages = max(1, safe_int(cfg.get("max_pages") or os.environ.get("POLYMARKET_MAX_PAGES"), 2))
    include_orderbooks = str(cfg.get("include_orderbooks") or os.environ.get("POLYMARKET_INCLUDE_ORDERBOOKS") or "1").strip().lower() not in {"0", "false", "no", "off"}
    include_history = str(cfg.get("include_history") or os.environ.get("POLYMARKET_INCLUDE_HISTORY") or "0").strip().lower() in {"1", "true", "yes", "on"}
    include_data_trades = str(cfg.get("include_data_trades") or os.environ.get("POLYMARKET_INCLUDE_DATA_TRADES") or "0").strip().lower() in {"1", "true", "yes", "on"}
    max_orderbooks = max(0, safe_int(cfg.get("max_orderbooks") or os.environ.get("POLYMARKET_MAX_ORDERBOOKS"), 50))
    max_history_markets = max(0, safe_int(cfg.get("max_history_markets") or os.environ.get("POLYMARKET_MAX_HISTORY_MARKETS"), 20))
    fidelity = max(1, safe_int(cfg.get("history_fidelity") or os.environ.get("POLYMARKET_HISTORY_FIDELITY"), 60))
    baskets = polymarket_asset_baskets_from_settings(cfg)
    semantic_map = polymarket_semantic_event_map_from_settings(cfg)

    events: list[dict[str, Any]] = []
    markets: list[dict[str, Any]] = []
    orderbooks: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []

    active = str(cfg.get("active") or os.environ.get("POLYMARKET_ACTIVE") or "true").strip().lower()
    closed = str(cfg.get("closed") or os.environ.get("POLYMARKET_CLOSED") or "false").strip().lower()
    for page in range(max_pages):
        params: dict[str, Any] = {
            "limit": limit,
            "offset": int(page * limit),
            "active": active,
            "closed": closed,
            "order": str(cfg.get("order") or os.environ.get("POLYMARKET_ORDER") or "volume24hr"),
            "ascending": str(cfg.get("ascending") or os.environ.get("POLYMARKET_ASCENDING") or "false"),
        }
        payload = _http_get_payload(sess, gamma_base_url, "events", params=params, timeout_s=timeout_s)
        raw_events = _polymarket_collection(payload, "events")
        if not raw_events:
            break
        for event in raw_events:
            if not _polymarket_event_passes_filters(event, cfg):
                continue
            nested_markets = event.get("markets") if isinstance(event.get("markets"), list) else []
            event_assets: set[str] = set()
            event_type_for_row = ""
            semantic_for_row = ""
            semantics_for_row = ""
            accepted_market_rows: list[dict[str, Any]] = []
            for market in [dict(item or {}) for item in nested_markets if isinstance(item, Mapping)]:
                semantic = _polymarket_semantic_mapping(event, market, semantic_map)
                event_type = str(semantic.get("event_type") or _infer_polymarket_event_type(event, market)).strip()
                if event_type not in POLYMARKET_ALLOWED_EVENT_TYPES:
                    event_type = "narrative"
                assets = _polymarket_assets(event, market, event_type=event_type, baskets=baskets, semantic=semantic)
                if not _polymarket_market_passes_filters(market, event, cfg, affected_assets=assets):
                    continue
                normalized = normalize_polymarket_market(
                    market,
                    event=event,
                    now_ms=now,
                    affected_assets=assets,
                    semantic_event_id=str(semantic.get("semantic_event_id") or ""),
                    resolution_semantics=str(semantic.get("resolution_semantics") or ""),
                    event_type=event_type,
                )
                accepted_market_rows.append(normalized)
                event_assets.update(str(asset) for asset in assets)
                event_type_for_row = event_type_for_row or event_type
                semantic_for_row = semantic_for_row or str(semantic.get("semantic_event_id") or "")
                semantics_for_row = semantics_for_row or str(semantic.get("resolution_semantics") or "")
            if not accepted_market_rows:
                continue
            events.append(
                normalize_polymarket_event(
                    event,
                    now_ms=now,
                    affected_assets=sorted(event_assets),
                    event_type=event_type_for_row,
                    semantic_event_id=semantic_for_row,
                    resolution_semantics=semantics_for_row,
                )
            )
            markets.extend(accepted_market_rows)

    history_market_count = 0
    data_trade_market_count = 0
    for market in markets:
        token_id = str(market.get("token_id") or "").strip()
        provider_market_id = str(market.get("provider_market_id") or "").strip()
        condition_id = str(market.get("condition_id") or "").strip()
        if not token_id or not provider_market_id:
            continue
        if include_orderbooks and len(orderbooks) < max_orderbooks:
            book_payload = _http_get_payload(sess, clob_base_url, "book", params={"token_id": token_id}, timeout_s=timeout_s)
            midpoint_payload = _http_get_payload(sess, clob_base_url, "midpoint", params={"token_id": token_id}, timeout_s=timeout_s)
            spread_payload = _http_get_payload(sess, clob_base_url, "spread", params={"token_id": token_id}, timeout_s=timeout_s)
            midpoint = midpoint_payload.get("mid") if isinstance(midpoint_payload, Mapping) else midpoint_payload
            spread = spread_payload.get("spread") if isinstance(spread_payload, Mapping) else spread_payload
            last_trade: Mapping[str, Any] = {}
            try:
                last_trade_payload = _http_get_payload(sess, clob_base_url, "last-trade-price", params={"token_id": token_id}, timeout_s=timeout_s)
                last_trade = dict(last_trade_payload or {}) if isinstance(last_trade_payload, Mapping) else {}
            except Exception:
                last_trade = {}
            orderbooks.append(
                normalize_polymarket_orderbook(
                    provider_market_id,
                    dict(book_payload or {}) if isinstance(book_payload, Mapping) else {},
                    now_ms=now,
                    condition_id=condition_id,
                    token_id=token_id,
                    midpoint=midpoint,
                    spread=spread,
                    last_trade=last_trade,
                )
            )
            if last_trade:
                price = _probability(last_trade.get("price"))
                ts_ms = parse_ts_ms(last_trade.get("timestamp") or last_trade.get("created_at") or last_trade.get("createdAt")) or now
                if price is not None:
                    trades.append(
                        {
                            "provider_name": "polymarket",
                            "provider_market_id": provider_market_id,
                            "condition_id": condition_id,
                            "token_id": token_id,
                            "trade_id": str(last_trade.get("id") or f"{provider_market_id}:last:{ts_ms}"),
                            "trade_ts_ms": int(ts_ms),
                            "source_ts_ms": int(ts_ms),
                            "availability_ts_ms": int(now),
                            "price": price,
                            "size": safe_float(last_trade.get("size"), 0.0) if last_trade.get("size") is not None else None,
                            "side": str(last_trade.get("side") or "last_trade"),
                            "raw_payload_hash": raw_payload_hash(last_trade),
                            "raw_payload": dict(last_trade),
                        }
                    )
        if include_history and history_market_count < max_history_markets:
            history_payload = _http_get_payload(
                sess,
                clob_base_url,
                "prices-history",
                params={"market": token_id, "fidelity": fidelity},
                timeout_s=timeout_s,
            )
            trades.extend(
                normalize_polymarket_price_history(
                    provider_market_id,
                    history_payload,
                    now_ms=now,
                    condition_id=condition_id,
                    token_id=token_id,
                )
            )
            history_market_count += 1
        if include_data_trades and condition_id and data_trade_market_count < max_history_markets:
            data_payload = _http_get_payload(
                sess,
                data_base_url,
                "trades",
                params={"market": condition_id, "limit": max(1, min(100, limit))},
                timeout_s=timeout_s,
            )
            trades.extend(
                _polymarket_recent_trades_to_history(
                    provider_market_id,
                    data_payload,
                    now_ms=now,
                    condition_id=condition_id,
                    token_id=token_id,
                )
            )
            data_trade_market_count += 1

    return {"events": events, "markets": markets, "orderbooks": orderbooks, "trades": trades}


def fetch_kalshi_macro_batch(
    *,
    settings: Mapping[str, Any] | None = None,
    now_ms: int | None = None,
    session: Any | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch Kalshi series/events/markets/orderbooks through public endpoints."""

    cfg = dict(settings or {})
    now = int(now_ms or utc_ms())
    sess = session or requests.Session()
    base_url = str(cfg.get("base_url") or os.environ.get("KALSHI_PUBLIC_BASE_URL") or KALSHI_BASE_URL).rstrip("/")
    timeout_s = float(cfg.get("timeout_s") or os.environ.get("KALSHI_POLL_TIMEOUT_S") or 10.0)
    max_pages = max(1, safe_int(cfg.get("max_pages") or os.environ.get("KALSHI_MAX_PAGES"), 2))
    limit = max(1, min(200, safe_int(cfg.get("limit") or os.environ.get("KALSHI_LIMIT"), 100)))
    status = str(cfg.get("status") or os.environ.get("KALSHI_MARKET_STATUS") or "open").strip()
    include_orderbooks = str(cfg.get("include_orderbooks") or os.environ.get("KALSHI_INCLUDE_ORDERBOOKS") or "1").strip().lower() not in {"0", "false", "no", "off"}
    max_orderbooks = max(0, safe_int(cfg.get("max_orderbooks") or os.environ.get("KALSHI_MAX_ORDERBOOKS"), 50))
    affected_assets = macro_assets_from_settings(cfg)
    series_allowlist = {item.upper() for item in parse_list(cfg.get("series_allowlist") or os.environ.get("KALSHI_SERIES_ALLOWLIST"))}
    category_filters = [item.lower() for item in parse_list(cfg.get("category_filters") or os.environ.get("KALSHI_CATEGORY_FILTERS") or "economics,financials,finance,rates,inflation,fed")]

    series_rows: list[dict[str, Any]] = []
    if series_allowlist:
        series_rows = [{"ticker": ticker} for ticker in sorted(series_allowlist)]
    else:
        raw_series = _paginate(
            sess,
            base_url,
            "series",
            params={"limit": min(1000, max(100, limit))},
            collection_key="series",
            max_pages=max_pages,
            timeout_s=timeout_s,
        )
        for row in raw_series:
            haystack = " ".join(
                str(row.get(key) or "")
                for key in ("ticker", "title", "category", "tags")
            ).lower()
            if any(token in haystack for token in category_filters):
                series_rows.append(row)

    events: list[dict[str, Any]] = []
    markets: list[dict[str, Any]] = []
    orderbooks: list[dict[str, Any]] = []
    for series in series_rows:
        series_ticker = str(series.get("ticker") or "").strip()
        if not series_ticker:
            continue
        event_params: dict[str, Any] = {
            "limit": limit,
            "series_ticker": series_ticker,
            "with_nested_markets": "true",
        }
        if status:
            event_params["status"] = status
        raw_events = _paginate(
            sess,
            base_url,
            "events",
            params=event_params,
            collection_key="events",
            max_pages=max_pages,
            timeout_s=timeout_s,
        )
        for event in raw_events:
            event["series_ticker"] = event.get("series_ticker") or series_ticker
            events.append(normalize_kalshi_event(event, now_ms=now, affected_assets=affected_assets))
            nested = event.get("markets") if isinstance(event.get("markets"), list) else []
            if not nested:
                market_params: dict[str, Any] = {"limit": min(1000, limit), "series_ticker": series_ticker}
                if status:
                    market_params["status"] = status
                nested = _paginate(
                    sess,
                    base_url,
                    "markets",
                    params=market_params,
                    collection_key="markets",
                    max_pages=max_pages,
                    timeout_s=timeout_s,
                )
            for market in nested:
                market["series_ticker"] = market.get("series_ticker") or series_ticker
                normalized = normalize_kalshi_market(market, now_ms=now, affected_assets=affected_assets)
                markets.append(normalized)
                ticker = str(normalized.get("provider_market_id") or "")
                if include_orderbooks and ticker and len(orderbooks) < max_orderbooks:
                    payload = _http_get_json(sess, base_url, f"markets/{ticker}/orderbook", timeout_s=timeout_s)
                    orderbooks.append(normalize_kalshi_orderbook(ticker, payload, now_ms=now))

    return {"events": events, "markets": markets, "orderbooks": orderbooks, "trades": []}


def normalize_cme_fedwatch_forecasts(
    payload: Mapping[str, Any],
    *,
    now_ms: int,
    affected_assets: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    """Normalize official CME FedWatch forecast payloads into market rows."""

    forecast_rows = _find_forecast_rows(payload)
    events: list[dict[str, Any]] = []
    markets: list[dict[str, Any]] = []
    seen_events: set[str] = set()
    for row in forecast_rows:
        meeting = (
            row.get("meetingDt")
            or row.get("meetingDate")
            or row.get("meeting_date")
            or row.get("fomcMeetingDate")
            or row.get("meeting")
        )
        meeting_ts_ms = parse_ts_ms(meeting)
        if not meeting_ts_ms:
            continue
        event_id = f"FOMC:{datetime.fromtimestamp(meeting_ts_ms / 1000, timezone.utc).date().isoformat()}"
        reporting_ts_ms = (
            parse_ts_ms(row.get("reportingDt") or row.get("reportingDate") or row.get("asOfDate") or row.get("asOf"))
            or int(now_ms)
        )
        if event_id not in seen_events:
            events.append(
                {
                    "provider_name": "cme_fedwatch",
                    "provider_event_id": event_id,
                    "event_ticker": event_id,
                    "series_ticker": "CME_FEDWATCH",
                    "title": f"FOMC rate target probabilities {event_id.rsplit(':', 1)[-1]}",
                    "provider_category": PROVIDER_CATEGORY_MACRO,
                    "event_type": "fomc_rate_decision",
                    "event_ts_ms": int(meeting_ts_ms),
                    "resolution_ts_ms": int(meeting_ts_ms),
                    "source_ts_ms": int(reporting_ts_ms),
                    "availability_ts_ms": int(now_ms),
                    "affected_assets": list(affected_assets),
                    "raw_payload_hash": raw_payload_hash(row),
                    "raw_payload": dict(row),
                }
            )
            seen_events.add(event_id)
        for prob in _probability_rows(row):
            probability = _probability(prob.get("probability") or prob.get("prob") or prob.get("value"))
            if probability is None:
                continue
            label = str(
                prob.get("targetRate")
                or prob.get("target_rate")
                or prob.get("rateRange")
                or prob.get("range")
                or prob.get("label")
                or prob.get("outcome")
                or "rate_range"
            ).strip()
            market_id = f"{event_id}:{label}".replace(" ", "_")
            previous = _probability(prob.get("previousProbability") or prob.get("priorProbability"))
            markets.append(
                {
                    "provider_name": "cme_fedwatch",
                    "provider_market_id": market_id,
                    "provider_event_id": event_id,
                    "market_ticker": market_id,
                    "series_ticker": "CME_FEDWATCH",
                    "title": f"CME FedWatch {label}",
                    "subtitle": label,
                    "provider_category": PROVIDER_CATEGORY_MACRO,
                    "event_type": "fomc_rate_decision",
                    "status": "open",
                    "probability": probability,
                    "previous_probability": previous,
                    "probability_delta": (probability - previous) if previous is not None else None,
                    "liquidity": 0.0,
                    "volume": safe_float(row.get("volume") or prob.get("volume"), 0.0),
                    "volume_24h": 0.0,
                    "open_interest": safe_float(row.get("openInterest") or prob.get("openInterest"), 0.0),
                    "spread": None,
                    "event_ts_ms": int(meeting_ts_ms),
                    "close_ts_ms": int(meeting_ts_ms),
                    "resolution_ts_ms": int(meeting_ts_ms),
                    "source_ts_ms": int(reporting_ts_ms),
                    "availability_ts_ms": int(now_ms),
                    "affected_assets": list(affected_assets),
                    "raw_payload_hash": raw_payload_hash(prob),
                    "raw_payload": {"forecast": dict(row), "probability": dict(prob)},
                }
            )
    return {"events": events, "markets": markets, "orderbooks": [], "trades": []}


def fetch_cme_fedwatch_batch(
    *,
    settings: Mapping[str, Any] | None = None,
    credentials: Mapping[str, Any] | None = None,
    now_ms: int | None = None,
    session: Any | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch CME FedWatch data from the entitled API or an explicitly enabled parser."""

    cfg = dict(settings or {})
    creds = dict(credentials or {})
    now = int(now_ms or utc_ms())
    sess = session or requests.Session()
    mode = str(cfg.get("mode") or os.environ.get("CME_FEDWATCH_MODE") or "official_api").strip().lower()
    affected_assets = macro_assets_from_settings(cfg)
    if mode in {"official", "official_api", "api"}:
        token = str(creds.get("oauth_token") or os.environ.get("CME_FEDWATCH_OAUTH_TOKEN") or "").strip()
        if not token:
            raise RuntimeError("cme_fedwatch_oauth_token_missing")
        base_url = str(cfg.get("base_url") or os.environ.get("CME_FEDWATCH_BASE_URL") or CME_FEDWATCH_BASE_URL).rstrip("/")
        payload = _http_get_json(
            sess,
            base_url,
            "forecasts",
            headers={"Authorization": f"Bearer {token}"},
            timeout_s=float(cfg.get("timeout_s") or os.environ.get("CME_FEDWATCH_TIMEOUT_S") or 10.0),
        )
        return normalize_cme_fedwatch_forecasts(payload, now_ms=now, affected_assets=affected_assets)

    if mode in {"public_page", "page", "scrape"}:
        allowed = str(cfg.get("allow_public_page_parse") or os.environ.get("CME_FEDWATCH_ALLOW_PUBLIC_PAGE_PARSE") or "0").strip().lower() in {"1", "true", "yes", "on"}
        if not allowed:
            raise RuntimeError("cme_fedwatch_public_page_parse_disabled")
        url = str(cfg.get("public_page_url") or os.environ.get("CME_FEDWATCH_PUBLIC_PAGE_URL") or "https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html")
        response = sess.get(url, timeout=float(cfg.get("timeout_s") or 10.0))
        response.raise_for_status()
        payload = parse_cme_fedwatch_public_page(response.text)
        return normalize_cme_fedwatch_forecasts(payload, now_ms=now, affected_assets=affected_assets)

    raise RuntimeError(f"unsupported_cme_fedwatch_mode:{mode}")


def parse_cme_fedwatch_public_page(html: str) -> dict[str, Any]:
    """Best-effort parser for public-page JSON blobs, disabled by default."""

    text = str(html or "")
    candidates = re.findall(r"<script[^>]*>(.*?)</script>", text, flags=re.IGNORECASE | re.DOTALL)
    for script in candidates:
        if "meeting" not in script.lower() or "prob" not in script.lower():
            continue
        for match in re.finditer(r"(\{.*?\})", script, flags=re.DOTALL):
            snippet = match.group(1)
            try:
                parsed = json.loads(snippet)
            except Exception:
                continue
            if _find_forecast_rows(parsed):
                return parsed
    return {"forecasts": [], "parser": "public_page_fragile", "raw_payload_hash": raw_payload_hash({"html": text[:5000]})}


def _find_forecast_rows(value: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if isinstance(value, Mapping):
        if _looks_like_forecast(value):
            out.append(dict(value))
        for key in ("forecasts", "forecast", "data", "results", "meetings"):
            child = value.get(key)
            if child is not None:
                out.extend(_find_forecast_rows(child))
        for child in value.values():
            if isinstance(child, (Mapping, list)):
                out.extend(_find_forecast_rows(child))
    elif isinstance(value, list):
        for item in value:
            out.extend(_find_forecast_rows(item))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in out:
        key = raw_payload_hash(row)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _looks_like_forecast(row: Mapping[str, Any]) -> bool:
    keys = {str(key).lower() for key in row.keys()}
    has_meeting = any(key in keys for key in {"meetingdt", "meetingdate", "meeting_date", "fomcmeetingdate", "meeting"})
    return bool(has_meeting and _probability_rows(row))


def _probability_rows(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    for key in ("probabilities", "rateProbabilities", "rate_probabilities", "probabilityDistribution", "rates", "ranges"):
        value = row.get(key)
        if isinstance(value, list):
            return [dict(item or {}) for item in value if isinstance(item, Mapping)]
        if isinstance(value, Mapping):
            out = []
            for label, probability in value.items():
                if isinstance(probability, Mapping):
                    item = dict(probability)
                    item.setdefault("label", str(label))
                else:
                    item = {"label": str(label), "probability": probability}
                out.append(item)
            return out
    direct = row.get("probability") or row.get("prob") or row.get("value")
    if direct is not None:
        return [dict(row)]
    return []


def _infer_event_type(category: str, title: str) -> str:
    text = f"{category} {title}".lower()
    if any(token in text for token in ("fed", "fomc", "rate", "interest")):
        return "fomc_rate_decision"
    if any(token in text for token in ("inflation", "cpi", "pce")):
        return "inflation_release"
    if any(token in text for token in ("jobs", "payroll", "unemployment")):
        return "labor_release"
    return "macro_event"
