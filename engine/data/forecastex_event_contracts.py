"""ForecastEx regulated event-contract CSV ingestion helpers."""

from __future__ import annotations

import csv
import json
import math
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from io import StringIO
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import requests

from engine.data.prediction_market_providers import parse_list, parse_ts_ms
from engine.data.prediction_market_storage import (
    PROVIDER_CATEGORY_EVENT_SIGNAL,
    raw_payload_hash,
    safe_float,
    safe_int,
)


FORECASTEX_PROVIDER_NAME = "forecastex"
FORECASTEX_BASE_URL = "https://forecastex.com"
FORECASTEX_DOWNLOAD_PATH = "/api/download"
FORECASTEX_DEFAULT_FILE_KINDS = ("pairs", "prices", "summary")
FORECASTEX_PAIR_FILE_KINDS = {"pairs", "daily_pairs", "intraday_pairs"}
FORECASTEX_FILE_TYPES = {
    "pairs": "pairs",
    "daily_pairs": "pairs",
    "intraday_pairs": "pairs",
    "prices": "prices",
    "summary": "summary",
}
FORECASTEX_REFRESH_CADENCE = {
    "pairs": "10m",
    "daily_pairs": "10m",
    "intraday_pairs": "10m",
    "prices": "daily_eod",
    "summary": "daily_eod",
}
FORECASTEX_EOD_RELEASE_LOCAL = dt_time(hour=16, minute=30)
FORECASTEX_LOCAL_TZ = ZoneInfo("America/Chicago")
REGULATED_EVENT_TYPES = {
    "macro",
    "energy",
    "climate_weather",
    "fx_rates",
    "equity_index",
    "commodity",
}
DEFAULT_FORECASTEX_ASSET_BASKETS: dict[str, list[str]] = {
    "macro": ["SPY", "QQQ", "IWM", "TLT", "IEF", "SHY", "GLD", "UUP", "XLF", "KRE"],
    "energy": ["XLE", "XOP", "USO", "UNG", "BOIL", "KOLD"],
    "climate_weather": ["XLU", "XLE", "DBA", "CORN", "WEAT", "UNG"],
    "fx_rates": ["UUP", "TLT", "IEF", "SHY", "GLD", "FXE", "FXY"],
    "equity_index": ["SPY", "QQQ", "IWM", "DIA"],
    "commodity": ["DBC", "GLD", "SLV", "CPER", "USO", "UNG", "DBA", "CORN", "WEAT"],
}


@dataclass
class ForecastExProductMeta:
    product_id: str
    product_name: str = ""
    product_category: str = ""
    total_pairs: float = 0.0
    event_type: str = ""
    affected_assets: list[str] = field(default_factory=list)
    official_resolution_source: str = ""


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_header(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _clean_product_id(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", str(value or "").strip().upper())


def _contract_product_id(contract_id: Any) -> str:
    text = str(contract_id or "").strip().upper()
    if "_" in text:
        return _clean_product_id(text.split("_", 1)[0])
    return _clean_product_id(text)


def _slug(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


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


def _probability(value: Any) -> float | None:
    if value in (None, ""):
        return None
    out = safe_float(value, float("nan"))
    if not math.isfinite(out):
        return None
    if out > 1.0:
        out = out / 100.0
    return max(0.0, min(1.0, float(out)))


def _positive_float(value: Any) -> float:
    return max(0.0, safe_float(value, 0.0))


def _parse_file_date(value: Any) -> date:
    text = str(value or "").strip()
    if not text:
        return datetime.now(timezone.utc).date()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    parsed = parse_ts_ms(text)
    if parsed:
        return datetime.fromtimestamp(parsed / 1000.0, tz=timezone.utc).date()
    raise ValueError(f"invalid_forecastex_file_date:{text}")


def _file_date_text(value: Any) -> str:
    return _parse_file_date(value).isoformat()


def _download_date_text(value: Any) -> str:
    return _parse_file_date(value).strftime("%Y%m%d")


def _file_eod_provider_ts_ms(file_date: Any) -> int:
    local_dt = datetime.combine(_parse_file_date(file_date), FORECASTEX_EOD_RELEASE_LOCAL, tzinfo=FORECASTEX_LOCAL_TZ)
    return int(local_dt.timestamp() * 1000)


def _fallback_provider_ts_ms(file_date: Any, file_kind: str) -> int:
    if str(file_kind) in FORECASTEX_PAIR_FILE_KINDS:
        return int(datetime.combine(_parse_file_date(file_date), dt_time.min, tzinfo=timezone.utc).timestamp() * 1000)
    return _file_eod_provider_ts_ms(file_date)


def _row_value(row: Mapping[str, Any], *names: str) -> Any:
    normalized = {_normalize_header(k): v for k, v in dict(row or {}).items()}
    for name in names:
        key = _normalize_header(name)
        if key in normalized and normalized[key] not in (None, ""):
            return normalized[key]
    return ""


def _read_csv_rows(csv_text: str) -> tuple[list[dict[str, Any]], list[str]]:
    text = str(csv_text or "")
    if text.startswith("\ufeff"):
        text = text.lstrip("\ufeff")
    reader = csv.DictReader(StringIO(text))
    fieldnames = [_normalize_header(name) for name in list(reader.fieldnames or [])]
    rows: list[dict[str, Any]] = []
    for row in reader:
        if row is None:
            continue
        rows.append({str(k or ""): v for k, v in dict(row or {}).items()})
    return rows, fieldnames


def _settings_asset_map(settings: Mapping[str, Any] | None) -> dict[str, list[str]]:
    out = {key: list(values) for key, values in DEFAULT_FORECASTEX_ASSET_BASKETS.items()}
    parsed = _json_obj((settings or {}).get("asset_map_json"))
    for key, value in parsed.items():
        assets = [str(item or "").upper().strip().replace("$", "") for item in parse_list(value)]
        assets = sorted({asset for asset in assets if asset})
        if assets:
            out[_slug(key)] = assets
            out[str(key).upper().strip()] = assets
    return out


def _resolution_source_map(settings: Mapping[str, Any] | None) -> dict[str, str]:
    parsed = _json_obj((settings or {}).get("resolution_source_map_json"))
    out: dict[str, str] = {}
    for key, value in parsed.items():
        text = _clean_text(value)
        if text:
            out[str(key).strip()] = text
            out[str(key).upper().strip()] = text
            out[_slug(key)] = text
    return out


def _infer_event_type(*, product_id: str, product_name: str = "", product_category: str = "", contract_id: str = "") -> str:
    text = " ".join([product_id, product_name, product_category, contract_id]).lower()
    if any(token in text for token in ("weather", "temperature", "rain", "snow", "hurricane", "storm", "co2", "carbon", "climate")):
        return "climate_weather"
    if any(token in text for token in ("energy", "electricity", "power", "oil", "crude", "wti", "brent", "gasoline", "natural gas", "natgas")):
        return "energy"
    if any(token in text for token in ("fed", "rate", "rates", "sofr", "overnight", "currency", "exchange rate", "fx", "dollar")):
        return "fx_rates"
    if any(token in text for token in ("s&p", "sp500", "nasdaq", "russell", "dow", "equity index", "stock index", "index")):
        return "equity_index"
    if any(token in text for token in ("commodity", "gold", "silver", "copper", "corn", "wheat", "soybean", "settlement", "futures")):
        return "commodity"
    if any(token in text for token in ("economic", "cpi", "inflation", "unemployment", "gdp", "payroll", "permits", "claims", "sales")):
        return "macro"
    return "macro" if product_category else "event_signal"


def _product_meta(
    *,
    product_id: str,
    product_name: str = "",
    product_category: str = "",
    total_pairs: Any = 0.0,
    contract_id: str = "",
    settings: Mapping[str, Any] | None = None,
    existing: Mapping[str, ForecastExProductMeta] | None = None,
) -> ForecastExProductMeta:
    pid = _clean_product_id(product_id or _contract_product_id(contract_id))
    current = (existing or {}).get(pid)
    name = _clean_text(product_name or (current.product_name if current else ""))
    category = _clean_text(product_category or (current.product_category if current else ""))
    event_type = _infer_event_type(product_id=pid, product_name=name, product_category=category, contract_id=contract_id)
    asset_map = _settings_asset_map(settings)
    assets = list(asset_map.get(pid) or asset_map.get(_slug(name)) or asset_map.get(_slug(category)) or asset_map.get(event_type) or [])
    resolution_map = _resolution_source_map(settings)
    resolution_source = (
        resolution_map.get(pid)
        or resolution_map.get(_slug(name))
        or resolution_map.get(_slug(category))
        or resolution_map.get(event_type)
        or (current.official_resolution_source if current else "")
        or ""
    )
    return ForecastExProductMeta(
        product_id=pid,
        product_name=name,
        product_category=category,
        total_pairs=_positive_float(total_pairs if total_pairs not in (None, "") else (current.total_pairs if current else 0.0)),
        event_type=event_type,
        affected_assets=assets,
        official_resolution_source=resolution_source,
    )


def _passes_allowlists(meta: ForecastExProductMeta, settings: Mapping[str, Any] | None) -> bool:
    products = {_clean_product_id(item) for item in parse_list((settings or {}).get("product_allowlist"))}
    if products and meta.product_id not in products:
        return False
    categories = {_slug(item) for item in parse_list((settings or {}).get("product_category_allowlist") or (settings or {}).get("category_allowlist"))}
    category_candidates = {_slug(meta.product_category), _slug(meta.event_type)}
    if categories and not (categories & {item for item in category_candidates if item}):
        return False
    return True


def _base_record_meta(
    *,
    file_kind: str,
    file_date: Any,
    provider_timestamp_ms: int,
) -> dict[str, Any]:
    kind = str(file_kind or "pairs").strip().lower()
    return {
        "source_file_date": _file_date_text(file_date),
        "source_file_kind": kind,
        "refresh_cadence": FORECASTEX_REFRESH_CADENCE.get(kind, "unknown"),
        "provider_timestamp_ms": int(provider_timestamp_ms),
    }


def _event_record(
    meta: ForecastExProductMeta,
    *,
    file_kind: str,
    file_date: Any,
    provider_timestamp_ms: int,
    now_ms: int,
    raw_payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "provider_name": FORECASTEX_PROVIDER_NAME,
        "provider_event_id": f"product:{meta.product_id}",
        "event_ticker": meta.product_id,
        "series_ticker": meta.product_id,
        "title": meta.product_name or meta.product_id,
        "product_id": meta.product_id,
        "official_resolution_source": meta.official_resolution_source,
        "provider_category": PROVIDER_CATEGORY_EVENT_SIGNAL,
        "event_type": meta.event_type,
        "semantic_event_id": f"forecastex:{meta.product_id}:{meta.event_type}",
        "resolution_semantics": meta.official_resolution_source,
        "source_ts_ms": int(provider_timestamp_ms),
        "availability_ts_ms": int(now_ms),
        "affected_assets": list(meta.affected_assets),
        "raw_payload_hash": raw_payload_hash(raw_payload),
        "raw_payload": dict(raw_payload),
        **_base_record_meta(file_kind=file_kind, file_date=file_date, provider_timestamp_ms=provider_timestamp_ms),
    }


def _market_record(
    *,
    contract_id: str,
    meta: ForecastExProductMeta,
    probability: float,
    previous_probability: float | None,
    bid_probability: float | None = None,
    ask_probability: float | None = None,
    last_price: float | None = None,
    liquidity: float = 0.0,
    volume: float = 0.0,
    volume_24h: float = 0.0,
    open_interest: float = 0.0,
    spread: float | None = None,
    event_ts_ms: int | None = None,
    source_ts_ms: int = 0,
    availability_ts_ms: int = 0,
    status: str = "active",
    file_kind: str = "pairs",
    file_date: Any = "",
    provider_timestamp_ms: int = 0,
    raw_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    previous = previous_probability
    return {
        "provider_name": FORECASTEX_PROVIDER_NAME,
        "provider_market_id": f"forecastex:{contract_id}:YES",
        "provider_contract_id": str(contract_id),
        "provider_event_id": f"forecastex:{contract_id}",
        "market_ticker": str(contract_id),
        "series_ticker": meta.product_id,
        "title": meta.product_name or str(contract_id),
        "subtitle": "YES",
        "product_id": meta.product_id,
        "official_resolution_source": meta.official_resolution_source,
        "provider_category": PROVIDER_CATEGORY_EVENT_SIGNAL,
        "event_type": meta.event_type,
        "condition_id": str(contract_id),
        "token_id": f"{contract_id}:YES",
        "outcome_name": "YES",
        "semantic_event_id": f"forecastex:{meta.product_id}:{meta.event_type}",
        "resolution_semantics": meta.official_resolution_source,
        "status": str(status or "active"),
        "probability": float(probability),
        "previous_probability": previous,
        "probability_delta": (float(probability) - float(previous)) if previous is not None else None,
        "bid_probability": bid_probability,
        "ask_probability": ask_probability,
        "last_price": last_price if last_price is not None else float(probability),
        "liquidity": float(max(0.0, liquidity)),
        "volume": float(max(0.0, volume)),
        "volume_24h": float(max(0.0, volume_24h)),
        "open_interest": float(max(0.0, open_interest)),
        "spread": spread,
        "event_ts_ms": int(event_ts_ms) if event_ts_ms else None,
        "close_ts_ms": int(event_ts_ms) if event_ts_ms else None,
        "resolution_ts_ms": int(event_ts_ms) if event_ts_ms else None,
        "source_ts_ms": int(source_ts_ms or provider_timestamp_ms or availability_ts_ms),
        "availability_ts_ms": int(availability_ts_ms),
        "affected_assets": list(meta.affected_assets),
        "raw_payload_hash": raw_payload_hash(raw_payload or {}),
        "raw_payload": dict(raw_payload or {}),
        **_base_record_meta(file_kind=file_kind, file_date=file_date, provider_timestamp_ms=provider_timestamp_ms),
    }


def _orderbook_from_pair_market(market: Mapping[str, Any], *, yes_depth: float, no_depth: float, raw_payload: Mapping[str, Any]) -> dict[str, Any]:
    probability = _probability(market.get("probability"))
    if probability is None:
        probability = 0.0
    no_probability = max(0.0, min(1.0, 1.0 - float(probability)))
    depth = max(1.0, float(yes_depth) + float(no_depth))
    return {
        "provider_name": FORECASTEX_PROVIDER_NAME,
        "provider_market_id": str(market.get("provider_market_id") or ""),
        "condition_id": str(market.get("condition_id") or ""),
        "token_id": str(market.get("token_id") or ""),
        "provider_contract_id": str(market.get("provider_contract_id") or ""),
        "product_id": str(market.get("product_id") or ""),
        "source_file_date": str(market.get("source_file_date") or ""),
        "source_file_kind": str(market.get("source_file_kind") or ""),
        "source_ts_ms": safe_int(market.get("source_ts_ms"), 0),
        "availability_ts_ms": safe_int(market.get("availability_ts_ms"), 0),
        "best_yes_bid": probability,
        "best_yes_ask": probability,
        "best_no_bid": no_probability,
        "best_no_ask": no_probability,
        "mid_probability": probability,
        "spread": 0.0,
        "yes_depth": float(max(0.0, yes_depth)),
        "no_depth": float(max(0.0, no_depth)),
        "liquidity": float(max(0.0, yes_depth) + max(0.0, no_depth)),
        "imbalance": (float(yes_depth) - float(no_depth)) / depth,
        "raw_payload_hash": raw_payload_hash(raw_payload),
        "raw_payload": dict(raw_payload),
    }


def _trade_record(
    *,
    pair_id: str,
    contract_id: str,
    meta: ForecastExProductMeta,
    trade_ts_ms: int,
    availability_ts_ms: int,
    price: float,
    size: float,
    file_kind: str,
    file_date: Any,
    raw_payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "provider_name": FORECASTEX_PROVIDER_NAME,
        "provider_market_id": f"forecastex:{contract_id}:YES",
        "condition_id": str(contract_id),
        "token_id": f"{contract_id}:YES",
        "provider_contract_id": str(contract_id),
        "product_id": meta.product_id,
        "source_file_date": _file_date_text(file_date),
        "source_file_kind": str(file_kind),
        "trade_id": str(pair_id),
        "trade_ts_ms": int(trade_ts_ms),
        "source_ts_ms": int(trade_ts_ms),
        "availability_ts_ms": int(availability_ts_ms),
        "price": float(price),
        "size": float(size),
        "side": "YES_PAIR",
        "raw_payload_hash": raw_payload_hash(raw_payload),
        "raw_payload": dict(raw_payload),
    }


def _status_from_expiration(expiration_ts_ms: int | None, now_ms: int) -> str:
    if expiration_ts_ms and int(expiration_ts_ms) <= int(now_ms):
        return "expired"
    return "active"


def _parse_summary_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    file_kind: str,
    file_date: Any,
    now_ms: int,
    settings: Mapping[str, Any] | None,
    product_metadata: dict[str, ForecastExProductMeta],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    events: list[dict[str, Any]] = []
    stats = {"rows_parsed": 0, "rows_skipped": 0, "parse_error_count": 0, "stale_count": 0}
    provider_ts = _fallback_provider_ts_ms(file_date, file_kind)
    for row in rows:
        product_id = _clean_product_id(_row_value(row, "product_id", "product"))
        if not product_id:
            stats["rows_skipped"] += 1
            stats["parse_error_count"] += 1
            continue
        meta = _product_meta(
            product_id=product_id,
            product_name=_row_value(row, "product_name", "name"),
            product_category=_row_value(row, "product_category", "category"),
            total_pairs=_row_value(row, "total_pairs", "pairs"),
            settings=settings,
            existing=product_metadata,
        )
        product_metadata[meta.product_id] = meta
        if not _passes_allowlists(meta, settings):
            stats["rows_skipped"] += 1
            continue
        if not meta.affected_assets:
            stats["stale_count"] += 1
        events.append(
            _event_record(
                meta,
                file_kind=file_kind,
                file_date=file_date,
                provider_timestamp_ms=provider_ts,
                now_ms=now_ms,
                raw_payload={"csv_row": dict(row), "file_kind": file_kind, "file_date": _file_date_text(file_date)},
            )
        )
        stats["rows_parsed"] += 1
    return events, stats


def _parse_price_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    file_kind: str,
    file_date: Any,
    now_ms: int,
    settings: Mapping[str, Any] | None,
    product_metadata: Mapping[str, ForecastExProductMeta],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    stats = {"rows_parsed": 0, "rows_skipped": 0, "parse_error_count": 0, "stale_count": 0}
    grouped: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in rows:
        contract_id = str(_row_value(row, "event_contract", "contract", "contract_id")).strip().upper()
        subtype = str(_row_value(row, "subtype", "side", "outcome_name") or "YES").strip().upper()
        if not contract_id:
            stats["rows_skipped"] += 1
            stats["parse_error_count"] += 1
            continue
        grouped.setdefault(contract_id, {})[subtype] = dict(row)

    markets: list[dict[str, Any]] = []
    for contract_id, sides in grouped.items():
        yes_row = sides.get("YES") or next(iter(sides.values()), {})
        no_row = sides.get("NO") or {}
        product_id = _contract_product_id(contract_id)
        meta = _product_meta(
            product_id=product_id,
            contract_id=contract_id,
            settings=settings,
            existing=product_metadata,
        )
        if not _passes_allowlists(meta, settings):
            stats["rows_skipped"] += 1
            continue
        probability = (
            _probability(_row_value(yes_row, "settlement_price"))
            or _probability(_row_value(yes_row, "end_price", "close_price"))
            or _probability(_row_value(yes_row, "vwap"))
            or _probability(_row_value(yes_row, "start_price", "open_price"))
        )
        if probability is None:
            stats["rows_skipped"] += 1
            stats["parse_error_count"] += 1
            continue
        previous = _probability(_row_value(yes_row, "start_price", "open_price"))
        volume = _positive_float(_row_value(yes_row, "pair_quantity", "quantity", "volume"))
        open_interest = _positive_float(_row_value(yes_row, "open_interest"))
        expiration_ts = parse_ts_ms(_row_value(yes_row, "expiration_date", "expiration", "resolution_date"))
        provider_ts = (
            parse_ts_ms(_row_value(yes_row, "provider_timestamp", "updated_at", "date"))
            or _fallback_provider_ts_ms(file_date, file_kind)
        )
        status = str(_row_value(yes_row, "status", "market_status") or _status_from_expiration(expiration_ts, now_ms)).strip().lower()
        if not meta.affected_assets or (volume + open_interest) <= 0.0 or status not in {"active", "open", "live", "trading", "tradable"}:
            stats["stale_count"] += 1
        raw = {
            "yes_csv_row": dict(yes_row),
            "no_csv_row": dict(no_row),
            "file_kind": file_kind,
            "file_date": _file_date_text(file_date),
        }
        market = _market_record(
            contract_id=contract_id,
            meta=meta,
            probability=probability,
            previous_probability=previous,
            last_price=probability,
            liquidity=volume + open_interest,
            volume=volume,
            volume_24h=volume,
            open_interest=open_interest,
            event_ts_ms=expiration_ts,
            source_ts_ms=provider_ts,
            availability_ts_ms=now_ms,
            status=status,
            file_kind=file_kind,
            file_date=file_date,
            provider_timestamp_ms=provider_ts,
            raw_payload=raw,
        )
        market["raw_payload_hash"] = raw_payload_hash(raw)
        markets.append(market)
        stats["rows_parsed"] += 1
    return markets, stats


def _parse_pair_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    file_kind: str,
    file_date: Any,
    now_ms: int,
    settings: Mapping[str, Any] | None,
    product_metadata: Mapping[str, ForecastExProductMeta],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    stats = {"rows_parsed": 0, "rows_skipped": 0, "parse_error_count": 0, "stale_count": 0}
    aggregates: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    for row in rows:
        contract_id = str(_row_value(row, "event_contract", "contract", "contract_id")).strip().upper()
        pair_id = str(_row_value(row, "pair_id", "trade_id", "id")).strip()
        quantity = _positive_float(_row_value(row, "quantity", "pair_quantity", "size"))
        yes_price = _probability(_row_value(row, "yes_price", "price", "yes"))
        no_price = _probability(_row_value(row, "no_price", "no"))
        pair_ts = parse_ts_ms(_row_value(row, "pair_time", "timestamp", "provider_timestamp", "updated_at"))
        expiration_ts = parse_ts_ms(_row_value(row, "expiration_date", "expiration", "resolution_date"))
        if not contract_id or not pair_id or yes_price is None or pair_ts is None:
            stats["rows_skipped"] += 1
            stats["parse_error_count"] += 1
            continue
        product_id = _contract_product_id(contract_id)
        meta = _product_meta(product_id=product_id, contract_id=contract_id, settings=settings, existing=product_metadata)
        if not _passes_allowlists(meta, settings):
            stats["rows_skipped"] += 1
            continue
        raw = {
            "csv_row": dict(row),
            "file_kind": file_kind,
            "file_date": _file_date_text(file_date),
        }
        trades.append(
            _trade_record(
                pair_id=pair_id,
                contract_id=contract_id,
                meta=meta,
                trade_ts_ms=pair_ts,
                availability_ts_ms=now_ms,
                price=yes_price,
                size=quantity,
                file_kind=file_kind,
                file_date=file_date,
                raw_payload=raw,
            )
        )
        rec = aggregates.setdefault(
            contract_id,
            {
                "contract_id": contract_id,
                "meta": meta,
                "quantity": 0.0,
                "weighted_yes": 0.0,
                "weighted_no": 0.0,
                "latest_ts": 0,
                "latest_yes": yes_price,
                "latest_no": no_price,
                "expiration_ts": expiration_ts,
                "rows": 0,
                "raw_rows": [],
            },
        )
        rec["quantity"] = float(rec["quantity"]) + float(quantity)
        rec["weighted_yes"] = float(rec["weighted_yes"]) + float(yes_price) * max(1.0, float(quantity))
        rec["weighted_no"] = float(rec["weighted_no"]) + float(no_price if no_price is not None else (1.0 - yes_price)) * max(1.0, float(quantity))
        rec["rows"] = int(rec["rows"]) + 1
        rec["raw_rows"].append(dict(row))
        if int(pair_ts) >= int(rec.get("latest_ts") or 0):
            rec["latest_ts"] = int(pair_ts)
            rec["latest_yes"] = float(yes_price)
            rec["latest_no"] = float(no_price if no_price is not None else (1.0 - yes_price))
        if not rec.get("expiration_ts") and expiration_ts:
            rec["expiration_ts"] = int(expiration_ts)
        stats["rows_parsed"] += 1

    markets: list[dict[str, Any]] = []
    orderbooks: list[dict[str, Any]] = []
    for contract_id, rec in aggregates.items():
        meta = rec["meta"]
        quantity = float(rec.get("quantity") or 0.0)
        denom = max(1.0, quantity)
        probability = safe_float(rec.get("weighted_yes"), 0.0) / denom
        latest_yes = _probability(rec.get("latest_yes"))
        if latest_yes is not None:
            probability = latest_yes
        status = _status_from_expiration(safe_int(rec.get("expiration_ts"), 0) or None, now_ms)
        if not meta.affected_assets or quantity <= 0.0 or status != "active":
            stats["stale_count"] += 1
        raw = {
            "aggregate": {
                "rows": int(rec.get("rows") or 0),
                "quantity": quantity,
                "latest_yes": rec.get("latest_yes"),
                "latest_no": rec.get("latest_no"),
            },
            "sample_rows": list(rec.get("raw_rows") or [])[:25],
            "file_kind": file_kind,
            "file_date": _file_date_text(file_date),
        }
        market = _market_record(
            contract_id=contract_id,
            meta=meta,
            probability=probability,
            previous_probability=None,
            bid_probability=probability,
            ask_probability=probability,
            last_price=probability,
            liquidity=quantity,
            volume=quantity,
            volume_24h=quantity,
            open_interest=0.0,
            spread=0.0,
            event_ts_ms=safe_int(rec.get("expiration_ts"), 0) or None,
            source_ts_ms=safe_int(rec.get("latest_ts"), now_ms),
            availability_ts_ms=now_ms,
            status=status,
            file_kind=file_kind,
            file_date=file_date,
            provider_timestamp_ms=safe_int(rec.get("latest_ts"), now_ms),
            raw_payload=raw,
        )
        markets.append(market)
        yes_depth = quantity * probability
        no_depth = quantity * max(0.0, 1.0 - probability)
        orderbooks.append(_orderbook_from_pair_market(market, yes_depth=yes_depth, no_depth=no_depth, raw_payload=raw))
    return markets, orderbooks, trades, stats


def _merge_stats(target: dict[str, int], incoming: Mapping[str, Any]) -> None:
    for key in ("rows_parsed", "rows_skipped", "parse_error_count", "stale_count"):
        target[key] = int(target.get(key) or 0) + safe_int((incoming or {}).get(key), 0)


def parse_forecastex_csv(
    csv_text: str,
    *,
    file_kind: str,
    file_date: Any,
    now_ms: int | None = None,
    settings: Mapping[str, Any] | None = None,
    product_metadata: Mapping[str, ForecastExProductMeta] | None = None,
) -> dict[str, Any]:
    """Parse one ForecastEx CSV into provider-neutral prediction-market rows."""

    now_value = int(now_ms if now_ms is not None else _now_ms())
    kind = str(file_kind or "pairs").strip().lower()
    if kind not in FORECASTEX_FILE_TYPES:
        raise ValueError(f"unsupported_forecastex_file_kind:{kind}")
    rows, headers = _read_csv_rows(csv_text)
    products = {str(k): v for k, v in dict(product_metadata or {}).items()}
    batch: dict[str, Any] = {"events": [], "markets": [], "orderbooks": [], "trades": []}
    health: dict[str, Any] = {
        "provider": FORECASTEX_PROVIDER_NAME,
        "file_kind": kind,
        "file_date": _file_date_text(file_date),
        "refresh_cadence": FORECASTEX_REFRESH_CADENCE.get(kind, "unknown"),
        "headers": headers,
        "rows_seen": int(len(rows)),
        "rows_parsed": 0,
        "rows_skipped": 0,
        "parse_error_count": 0,
        "stale_count": 0,
        "product_metadata_count": int(len(products)),
    }
    if kind == "summary":
        events, stats = _parse_summary_rows(
            rows,
            file_kind=kind,
            file_date=file_date,
            now_ms=now_value,
            settings=settings,
            product_metadata=products,
        )
        batch["events"].extend(events)
        _merge_stats(health, stats)
    elif kind == "prices":
        markets, stats = _parse_price_rows(
            rows,
            file_kind=kind,
            file_date=file_date,
            now_ms=now_value,
            settings=settings,
            product_metadata=products,
        )
        batch["markets"].extend(markets)
        _merge_stats(health, stats)
    else:
        markets, orderbooks, trades, stats = _parse_pair_rows(
            rows,
            file_kind=kind,
            file_date=file_date,
            now_ms=now_value,
            settings=settings,
            product_metadata=products,
        )
        batch["markets"].extend(markets)
        batch["orderbooks"].extend(orderbooks)
        batch["trades"].extend(trades)
        _merge_stats(health, stats)
    health["product_metadata_count"] = int(len(products))
    return {**batch, "health": health, "product_metadata": products}


def _merge_batch(target: dict[str, list[dict[str, Any]]], source: Mapping[str, Any]) -> None:
    for key in ("events", "markets", "orderbooks", "trades"):
        target.setdefault(key, [])
        target[key].extend([dict(item or {}) for item in list((source or {}).get(key) or []) if isinstance(item, Mapping)])


def _configured_file_kinds(settings: Mapping[str, Any] | None) -> list[str]:
    raw = parse_list((settings or {}).get("file_kinds") or (settings or {}).get("csv_types"))
    selected = [str(item).strip().lower() for item in raw if str(item).strip()] or list(FORECASTEX_DEFAULT_FILE_KINDS)
    out: list[str] = []
    seen: set[str] = set()
    for kind in selected:
        if kind not in FORECASTEX_FILE_TYPES or kind in seen:
            continue
        seen.add(kind)
        out.append(kind)
    return out or list(FORECASTEX_DEFAULT_FILE_KINDS)


def _configured_file_dates(settings: Mapping[str, Any] | None, *, now_ms: int) -> list[str]:
    explicit = parse_list((settings or {}).get("file_dates") or (settings or {}).get("file_date"))
    if explicit:
        return [_download_date_text(item) for item in explicit]
    lookback = max(0, safe_int((settings or {}).get("file_date_lookback"), 1))
    today = datetime.fromtimestamp(int(now_ms) / 1000.0, tz=timezone.utc).date()
    return [(today - timedelta(days=offset)).strftime("%Y%m%d") for offset in range(lookback + 1)]


def _download_csv(
    *,
    session: Any,
    base_url: str,
    file_kind: str,
    file_date: str,
    timeout_s: float,
) -> tuple[str, int]:
    response = session.get(
        f"{str(base_url).rstrip('/')}{FORECASTEX_DOWNLOAD_PATH}",
        params={"type": FORECASTEX_FILE_TYPES[str(file_kind)], "date": _download_date_text(file_date)},
        timeout=float(timeout_s),
    )
    status_code = int(getattr(response, "status_code", 0) or 0)
    response.raise_for_status()
    return str(getattr(response, "text", "") or ""), status_code


def fetch_forecastex_csv_batch(
    *,
    settings: Mapping[str, Any] | None = None,
    now_ms: int | None = None,
    session: Any | None = None,
) -> dict[str, Any]:
    """Fetch configured ForecastEx CSV files and normalize them idempotently."""

    now_value = int(now_ms if now_ms is not None else _now_ms())
    settings_map = dict(settings or {})
    http = session or requests.Session()
    base_url = str(settings_map.get("base_url") or FORECASTEX_BASE_URL).rstrip("/")
    timeout_s = safe_float(settings_map.get("timeout_s"), 20.0)
    kinds = _configured_file_kinds(settings_map)
    dates = _configured_file_dates(settings_map, now_ms=now_value)
    product_metadata: dict[str, ForecastExProductMeta] = {}
    batch: dict[str, list[dict[str, Any]]] = {"events": [], "markets": [], "orderbooks": [], "trades": []}
    health: dict[str, Any] = {
        "provider": FORECASTEX_PROVIDER_NAME,
        "ok": True,
        "base_url": base_url,
        "file_dates": list(dates),
        "file_kinds": list(kinds),
        "last_successful_csv_date": None,
        "rows_parsed": 0,
        "rows_skipped": 0,
        "parse_error_count": 0,
        "stale_count": 0,
        "rows_by_file": [],
        "contract_categories_enabled": sorted(
            {
                _slug(item)
                for item in parse_list(
                    settings_map.get("product_category_allowlist")
                    or settings_map.get("category_allowlist")
                    or ",".join(sorted(REGULATED_EVENT_TYPES))
                )
                if str(item).strip()
            }
        ),
        "direct_trading_authority": False,
        "stage": "shadow",
    }

    # Summary rows enrich product/category metadata for same-day prices/pairs.
    ordered_kinds = sorted(kinds, key=lambda item: 0 if item == "summary" else 1)
    for file_date in dates:
        for kind in ordered_kinds:
            try:
                csv_text, status_code = _download_csv(
                    session=http,
                    base_url=base_url,
                    file_kind=kind,
                    file_date=file_date,
                    timeout_s=timeout_s,
                )
                parsed = parse_forecastex_csv(
                    csv_text,
                    file_kind=kind,
                    file_date=file_date,
                    now_ms=now_value,
                    settings=settings_map,
                    product_metadata=product_metadata,
                )
                product_metadata = dict(parsed.get("product_metadata") or product_metadata)
                _merge_batch(batch, parsed)
                file_health = dict(parsed.get("health") or {})
                file_health["status_code"] = int(status_code)
                health["rows_by_file"].append(file_health)
                health["last_successful_csv_date"] = _file_date_text(file_date)
                for key in ("rows_parsed", "rows_skipped", "parse_error_count", "stale_count"):
                    health[key] = safe_int(health.get(key), 0) + safe_int(file_health.get(key), 0)
            except Exception as exc:
                health["ok"] = False
                health["parse_error_count"] = safe_int(health.get("parse_error_count"), 0) + 1
                health["rows_by_file"].append(
                    {
                        "file_kind": kind,
                        "file_date": _file_date_text(file_date),
                        "ok": False,
                        "error": str(exc)[:500],
                    }
                )

    health["product_metadata_count"] = int(len(product_metadata))
    return {**batch, "health": health, "product_metadata": product_metadata}
