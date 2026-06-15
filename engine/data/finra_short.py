"""FINRA short-sale and short-interest data helpers.

README:
- Source: FINRA consolidated daily short-sale volume files and the FINRA
  Query API ``EquityShortInterest`` dataset.
- Cadence: daily short-sale volume is published each trading day around
  6 p.m. ET; short interest is disseminated bi-monthly by FINRA after the
  settlement date.
- Availability lag: daily volume is available only at the file publication
  evening, so trading-day features use files from the previous trading day
  and earlier; short interest uses FINRA dissemination/availability time,
  not settlement date.
- Caveats: daily files cover off-exchange TRF/ADF/ORF volume only. Use
  relative or z-scored features, not absolute levels.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import time
from datetime import date, datetime, time as dt_time, timezone
from io import StringIO
from typing import Any, Dict, List, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests

FINRA_SHORT_VOLUME_URL_TEMPLATE = os.environ.get(
    "FINRA_SHORT_VOLUME_URL_TEMPLATE",
    "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{date}.txt",
)
FINRA_SHORT_INTEREST_API_URL = os.environ.get(
    "FINRA_SHORT_INTEREST_API_URL",
    "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest",
)
FINRA_REQUEST_TIMEOUT_S = float(os.environ.get("FINRA_REQUEST_TIMEOUT_S", "20"))
FINRA_SHORT_VOLUME_PUBLISH_HOUR_ET = int(os.environ.get("FINRA_SHORT_VOLUME_PUBLISH_HOUR_ET", "18"))
FINRA_SHORT_VOLUME_PUBLISH_MINUTE_ET = int(os.environ.get("FINRA_SHORT_VOLUME_PUBLISH_MINUTE_ET", "0"))

_EASTERN = ZoneInfo("America/New_York")
_UTC = timezone.utc
_KEY_NORMALIZER = re.compile(r"[^a-z0-9]+", re.IGNORECASE)


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _norm_key(key: Any) -> str:
    return _KEY_NORMALIZER.sub("", str(key or "").strip().lower())


def _field(record: Dict[str, Any], aliases: Sequence[str], default: Any = None) -> Any:
    if not isinstance(record, dict):
        return default
    normalized = {_norm_key(key): value for key, value in record.items()}
    for alias in aliases:
        key = _norm_key(alias)
        if key in normalized and normalized[key] not in (None, ""):
            return normalized[key]
    return default


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        out = float(text)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _safe_int_float(value: Any) -> float | None:
    parsed = _safe_float(value)
    return None if parsed is None else float(parsed)


def parse_date(value: Any) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty date")
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return datetime.strptime(text, "%Y-%m-%d").date()
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", text):
        return datetime.strptime(text, "%m/%d/%Y").date()
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


def parse_ts_ms(value: Any) -> int:
    if value is None:
        raise ValueError("empty timestamp")
    if isinstance(value, (int, float)):
        num = float(value)
        if num > 10_000_000_000:
            return int(num)
        return int(num * 1000)
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty timestamp")
    if re.fullmatch(r"\d{8}", text) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return date_to_ms(parse_date(text))
    normalized = text.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_EASTERN)
    return int(dt.astimezone(_UTC).timestamp() * 1000)


def date_to_ms(day: date | str) -> int:
    parsed = parse_date(day) if not isinstance(day, date) else day
    dt = datetime.combine(parsed, dt_time.min, tzinfo=_UTC)
    return int(dt.timestamp() * 1000)


def short_volume_publication_ts_ms(day: date | str) -> int:
    parsed = parse_date(day) if not isinstance(day, date) else day
    published = datetime.combine(
        parsed,
        dt_time(
            hour=max(0, min(23, int(FINRA_SHORT_VOLUME_PUBLISH_HOUR_ET))),
            minute=max(0, min(59, int(FINRA_SHORT_VOLUME_PUBLISH_MINUTE_ET))),
        ),
        tzinfo=_EASTERN,
    )
    return int(published.astimezone(_UTC).timestamp() * 1000)


def asof_date(ts_ms: int) -> date:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=_UTC).astimezone(_EASTERN).date()


def _source_record_id(*parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]
    return f"finra:{digest}"


def parse_short_volume_file(
    text: str,
    *,
    source_url: str = "",
    ingested_ts_ms: int | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    body = str(text or "").strip()
    if not body:
        return rows
    reader = csv.DictReader(StringIO(body), delimiter="|")
    if not reader.fieldnames:
        return rows
    ingested = int(ingested_ts_ms or utc_now_ms())
    for raw in reader:
        symbol = _clean_symbol(_field(raw, ["Symbol", "symbol"]))
        if not symbol or symbol in {"TOTAL", "ZZZ"}:
            continue
        try:
            trade_day = parse_date(_field(raw, ["Date", "tradeDate", "Trade Date"]))
        except Exception:
            continue
        short_volume = _safe_int_float(_field(raw, ["ShortVolume", "shortVolume", "Short Volume"]))
        total_volume = _safe_int_float(_field(raw, ["TotalVolume", "totalVolume", "Total Volume"]))
        if short_volume is None or total_volume is None:
            continue
        short_exempt = _safe_int_float(
            _field(raw, ["ShortExemptVolume", "shortExemptVolume", "Short Exempt Volume"], 0.0)
        )
        market = str(_field(raw, ["Market", "market"], "") or "").strip()
        trade_date = trade_day.isoformat()
        row = {
            "ts_ms": short_volume_publication_ts_ms(trade_day),
            "symbol": symbol,
            "trade_date": trade_date,
            "trade_ts_ms": date_to_ms(trade_day),
            "availability_ts_ms": short_volume_publication_ts_ms(trade_day),
            "source_record_id": _source_record_id("short_volume", trade_date, symbol, market),
            "source_url": str(source_url or ""),
            "ingested_ts_ms": int(ingested),
            "short_volume": float(short_volume),
            "short_exempt_volume": float(short_exempt or 0.0),
            "total_volume": float(total_volume),
            "market": market,
            "payload_json": dict(raw),
            "diagnostics_json": {"availability_rule": "same_day_18_00_et_publish_time"},
        }
        rows.append(row)
    return rows


def short_volume_url(day: date | str) -> str:
    parsed = parse_date(day) if not isinstance(day, date) else day
    return str(FINRA_SHORT_VOLUME_URL_TEMPLATE).format(date=parsed.strftime("%Y%m%d"))


def fetch_short_volume_file(day: date | str) -> List[Dict[str, Any]]:
    url = short_volume_url(day)
    response = requests.get(url, timeout=float(FINRA_REQUEST_TIMEOUT_S))
    response.raise_for_status()
    return parse_short_volume_file(response.text, source_url=url)


def _extract_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "records", "items", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict)]
    return [dict(payload)]


def normalize_short_interest_record(record: Dict[str, Any], *, ingested_ts_ms: int | None = None) -> Dict[str, Any] | None:
    symbol = _clean_symbol(
        _field(
            record,
            [
                "symbol",
                "securitySymbol",
                "issueSymbolIdentifier",
                "issueSymbol",
                "ticker",
            ],
        )
    )
    if not symbol:
        return None
    settlement_raw = _field(record, ["settlementDate", "settlement_date", "settlement"])
    dissemination_raw = _field(
        record,
        [
            "disseminationDate",
            "dissemination_date",
            "publicationDate",
            "releaseDate",
            "disseminationDatetime",
            "updateDatetime",
            "updatedAt",
        ],
    )
    try:
        settlement_day = parse_date(settlement_raw)
    except Exception:
        return None
    try:
        availability_ts = parse_ts_ms(dissemination_raw)
        dissemination_day = parse_date(dissemination_raw)
    except Exception:
        availability_ts = date_to_ms(settlement_day) + int(8 * 24 * 3600 * 1000)
        dissemination_day = datetime.fromtimestamp(availability_ts / 1000.0, tz=_UTC).date()
    shares = _safe_int_float(
        _field(
            record,
            [
                "shortInterestShares",
                "short_interest_shares",
                "currentShortShareNumber",
                "currentShortPositionQuantity",
                "shortInterest",
                "shortInterestQuantity",
            ],
        )
    )
    days_to_cover = _safe_float(
        _field(
            record,
            [
                "daysToCover",
                "days_to_cover",
                "daysToCoverNumber",
                "daysToCoverQuantity",
                "averageDailyShareVolume",
            ],
        )
    )
    if shares is None:
        return None
    source_id = str(
        _field(
            record,
            ["sourceRecordId", "id", "shortInterestId", "reportId", "sequenceNumber"],
            "",
        )
        or ""
    ).strip()
    settlement_date = settlement_day.isoformat()
    dissemination_date = dissemination_day.isoformat()
    row = {
        "ts_ms": int(availability_ts),
        "symbol": symbol,
        "settlement_date": settlement_date,
        "settlement_ts_ms": date_to_ms(settlement_day),
        "dissemination_date": dissemination_date,
        "dissemination_ts_ms": int(availability_ts),
        "availability_ts_ms": int(availability_ts),
        "source_record_id": source_id
        or _source_record_id("short_interest", settlement_date, dissemination_date, symbol),
        "ingested_ts_ms": int(ingested_ts_ms or utc_now_ms()),
        "short_interest_shares": float(shares),
        "days_to_cover": float(days_to_cover or 0.0),
        "payload_json": dict(record),
        "diagnostics_json": {"availability_rule": "finra_dissemination_datetime"},
    }
    return row


def fetch_short_interest_records(
    *,
    symbols: Sequence[str] | None = None,
    limit: int = 5000,
    max_pages: int = 20,
) -> List[Dict[str, Any]]:
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    cleaned_symbols = [_clean_symbol(sym) for sym in list(symbols or []) if _clean_symbol(sym)]
    out: List[Dict[str, Any]] = []
    page_size = max(1, int(limit))
    for offset in range(0, page_size * max(1, int(max_pages)), page_size):
        payload: Dict[str, Any] = {"limit": page_size, "offset": offset}
        if cleaned_symbols:
            payload["compareFilters"] = [
                {"fieldName": "issueSymbolIdentifier", "compareType": "IN", "fieldValue": cleaned_symbols}
            ]
        response = requests.post(
            FINRA_SHORT_INTEREST_API_URL,
            headers=headers,
            data=json.dumps(payload, separators=(",", ":"), sort_keys=True),
            timeout=float(FINRA_REQUEST_TIMEOUT_S),
        )
        if response.status_code in {404, 405} and offset == 0:
            response = requests.get(FINRA_SHORT_INTEREST_API_URL, headers=headers, timeout=float(FINRA_REQUEST_TIMEOUT_S))
        response.raise_for_status()
        records = _extract_records(response.json())
        normalized = [row for row in (normalize_short_interest_record(record) for record in records) if row]
        out.extend(normalized)
        if len(records) < page_size:
            break
    return out


def ewma(values: Sequence[float], *, alpha: float = 0.5) -> float | None:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if not clean:
        return None
    weight = max(0.0, min(1.0, float(alpha)))
    level = float(clean[0])
    for value in clean[1:]:
        level = float(weight * float(value) + (1.0 - weight) * float(level))
    return float(level)


def trailing_std(values: Sequence[float]) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if len(clean) < 2:
        return 0.0
    mean = sum(clean) / len(clean)
    variance = sum((value - mean) ** 2 for value in clean) / max(1, len(clean) - 1)
    return math.sqrt(max(0.0, variance))


def short_interest_surprise(
    readings: Sequence[Dict[str, Any]],
    *,
    alpha: float = 0.5,
    shares_normalizer: float | None = None,
) -> Tuple[float, float]:
    ordered = sorted(
        [dict(row) for row in readings if row],
        key=lambda row: int(row.get("settlement_ts_ms") or row.get("availability_ts_ms") or 0),
    )
    if len(ordered) < 2:
        return 0.0, 0.0
    latest = ordered[-1]
    prior = ordered[:-1]
    prior_values = [float(row.get("short_interest_shares") or 0.0) for row in prior]
    baseline = ewma(prior_values, alpha=float(alpha))
    if baseline is None:
        return 0.0, 0.0
    latest_value = float(latest.get("short_interest_shares") or 0.0)
    raw_surprise = float(latest_value - float(baseline))
    normalizer = float(shares_normalizer or 0.0)
    if normalizer <= 0.0:
        normalizer = trailing_std(prior_values)
    if normalizer <= 0.0:
        normalizer = 1.0
    latest_dtc = float(latest.get("days_to_cover") or 0.0)
    prior_dtc = float(prior[-1].get("days_to_cover") or 0.0)
    return float(raw_surprise / max(1.0, normalizer)), float(latest_dtc - prior_dtc)


def short_volume_ratio_z(rows: Sequence[Dict[str, Any]], *, lookback: int = 20) -> float:
    ordered = sorted(
        [dict(row) for row in rows if row],
        key=lambda row: int(row.get("trade_ts_ms") or row.get("availability_ts_ms") or 0),
    )
    ratios = []
    for row in ordered:
        total = float(row.get("total_volume") or 0.0)
        if total <= 0.0:
            continue
        ratios.append(float(row.get("short_volume") or 0.0) / total)
    if len(ratios) < 2:
        return 0.0
    latest = ratios[-1]
    history = ratios[-(int(lookback) + 1) : -1] or ratios[:-1]
    if len(history) < 2:
        return 0.0
    mean = sum(history) / len(history)
    std = trailing_std(history)
    if std <= 0.0:
        return 0.0
    return float(max(-10.0, min(10.0, (latest - mean) / std)))
