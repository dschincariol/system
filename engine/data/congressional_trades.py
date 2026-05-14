"""
Normalized congressional / STOCK Act trade ingestion helpers.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests

from engine.data.ingest.news_enrichment import infer_symbols
from engine.runtime.config import CONGRESSIONAL_BACKFILL_DAYS as CONFIG_CONGRESSIONAL_BACKFILL_DAYS
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

CONGRESSIONAL_BACKFILL_DAYS = max(1, int(CONFIG_CONGRESSIONAL_BACKFILL_DAYS))
CONGRESSIONAL_TIMEOUT_S = max(5.0, float(os.environ.get("CONGRESSIONAL_TIMEOUT_S", "20")))

DEFAULT_SOURCE_SPECS = (
    {
        "name": "senate_stock_watcher",
        "url": os.environ.get(
            "CONGRESSIONAL_SENATE_SOURCE_URL",
            "https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json",
        ),
        "chamber": "senate",
    },
    {
        "name": "house_stock_watcher",
        "url": os.environ.get(
            "CONGRESSIONAL_HOUSE_SOURCE_URL",
            "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json",
        ),
        "chamber": "house",
    },
)

_AMOUNT_RANGE_RX = re.compile(r"(?P<lo>\d[\d,]*(?:\.\d+)?)\s*(?:-|to)\s*(?P<hi>\d[\d,]*(?:\.\d+)?)", re.IGNORECASE)
_AMOUNT_SINGLE_RX = re.compile(r"(?P<value>\d[\d,]*(?:\.\d+)?)")

LOG = get_logger("engine.data.congressional_trades")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.congressional_trades",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper().replace("$", "")


def _entity_id(symbol: Any) -> Optional[str]:
    symbol_key = _normalize_symbol(symbol)
    return f"symbol:{symbol_key}" if symbol_key else None


def _merge_diagnostics(existing: Any, updates: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    if isinstance(existing, dict):
        merged.update(existing)
    elif isinstance(existing, str):
        try:
            parsed = json.loads(existing)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            merged.update(parsed)
    merged.update(dict(updates or {}))
    return merged


def _first_nonempty(record: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = record.get(str(key))
        if value not in (None, ""):
            return value
    return None


def _parse_ts_ms(value: Any) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(raw, fmt)
            parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            continue
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def _parse_amount_range(value: Any) -> tuple[Optional[float], Optional[float], Optional[float]]:
    raw = str(value or "").strip()
    if not raw:
        return None, None, None
    cleaned = raw.replace("$", "").replace(">", "").replace("<", "").strip()
    match = _AMOUNT_RANGE_RX.search(cleaned)
    if match:
        try:
            lo = float(match.group("lo").replace(",", ""))
            hi = float(match.group("hi").replace(",", ""))
            mid = float((lo + hi) / 2.0)
            return lo, hi, mid
        except Exception:
            return None, None, None
    match = _AMOUNT_SINGLE_RX.search(cleaned)
    if not match:
        return None, None, None
    try:
        value_num = float(match.group("value").replace(",", ""))
        return value_num, value_num, value_num
    except Exception:
        return None, None, None


def _normalize_direction(raw_type: Any) -> tuple[str, str]:
    text = str(raw_type or "").strip()
    lower = text.lower()
    if any(token in lower for token in ("purchase", "buy", "acquired")):
        return "purchase", "buy"
    if any(token in lower for token in ("sale", "sell", "disposed")):
        return "sale", "sell"
    if "exchange" in lower:
        return "exchange", "neutral"
    return "other", "neutral"


def _stable_id(parts: Sequence[Any]) -> str:
    payload = "|".join(str(part or "").strip() for part in parts)
    return hashlib.sha1(payload.encode("utf-8", "ignore")).hexdigest()


def _resolve_symbol(
    *,
    explicit_symbol: str = "",
    issuer_name: str = "",
    politician_name: str = "",
    allowed_symbols: Optional[Sequence[str]] = None,
) -> tuple[Optional[str], Dict[str, Any]]:
    diagnostics: Dict[str, Any] = {
        "allowed_symbol_count": int(len(list(allowed_symbols or []))),
    }
    symbol = _normalize_symbol(explicit_symbol)
    if symbol:
        diagnostics["symbol_match_method"] = "explicit"
        diagnostics["symbol_match_confidence"] = 1.0
        return symbol, diagnostics

    payload = {
        "title": str(issuer_name or ""),
        "body": f"{issuer_name or ''}\n{politician_name or ''}",
        "summary": str(issuer_name or ""),
    }
    try:
        inferred = infer_symbols(payload, allowed_symbols=allowed_symbols)
        symbols = list((inferred or {}).get("symbols") or [])
        if symbols:
            symbol = _normalize_symbol(symbols[0])
            diagnostics["symbol_match_method"] = str(((inferred or {}).get("match_method") or {}).get(symbol, "company_name"))
            diagnostics["symbol_match_confidence"] = float(((inferred or {}).get("match_confidence") or {}).get(symbol, 0.0))
            return symbol, diagnostics
        inferred = infer_symbols(payload, allowed_symbols=None)
        symbols = list((inferred or {}).get("symbols") or [])
        if symbols:
            symbol = _normalize_symbol(symbols[0])
            diagnostics["symbol_match_method"] = str(((inferred or {}).get("match_method") or {}).get(symbol, "company_name"))
            diagnostics["symbol_match_confidence"] = float(((inferred or {}).get("match_confidence") or {}).get(symbol, 0.0))
            return symbol, diagnostics
    except Exception as exc:
        _warn_nonfatal(
            "CONGRESSIONAL_TRADES_SYMBOL_RESOLUTION_FAILED",
            exc,
            once_key=f"congressional_symbol_resolution:{issuer_name}:{politician_name}",
            issuer_name=str(issuer_name or ""),
            politician_name=str(politician_name or ""),
        )
    diagnostics["symbol_match_method"] = "unresolved"
    diagnostics["symbol_match_confidence"] = 0.0
    return None, diagnostics


def apply_congressional_trade_resolution(
    row: Dict[str, Any],
    *,
    resolution_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Apply operator or resolver updates to one normalized congressional-trade row."""
    updated = dict(row or {})
    symbol = _normalize_symbol(updated.get("symbol"))
    merged_meta = _merge_diagnostics(updated.get("diagnostics_json"), dict(resolution_meta or {}))
    updated["symbol"] = symbol or None
    updated["entity_id"] = _entity_id(symbol)
    updated["resolution_status"] = "resolved" if symbol else "unresolved"
    updated["resolution_method"] = str((resolution_meta or {}).get("symbol_match_method") or ("explicit" if symbol else "unresolved")).strip() or None
    updated["diagnostics_json"] = merged_meta
    return updated


def refresh_congressional_trade_resolution(
    row: Dict[str, Any],
    *,
    allowed_symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Recompute symbol resolution from the row's current identifying fields."""
    updated = dict(row or {})
    resolved_symbol, resolution_meta = _resolve_symbol(
        explicit_symbol=str(updated.get("symbol") or ""),
        issuer_name=str(updated.get("issuer_name") or ""),
        politician_name=str(updated.get("politician_name") or ""),
        allowed_symbols=allowed_symbols,
    )
    if resolved_symbol:
        updated["symbol"] = resolved_symbol
    return apply_congressional_trade_resolution(updated, resolution_meta=resolution_meta)


def _iter_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("results", "data", "items", "transactions"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, dict)]
    return [dict(value) for value in payload.values() if isinstance(value, dict)]


def normalize_congressional_trade_record(
    raw: Dict[str, Any],
    *,
    source_name: str,
    source_url: str = "",
    default_chamber: str = "",
    allowed_symbols: Optional[Sequence[str]] = None,
    ingested_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Normalize one source record into the repo's congressional-trade contract."""
    record = dict(raw or {})
    politician_name = str(
        _first_nonempty(record, ("politician", "representative", "senator", "member", "name")) or ""
    ).strip()
    chamber = str(_first_nonempty(record, ("chamber", "house", "body")) or default_chamber or "").strip().lower()
    office = str(_first_nonempty(record, ("office", "district", "state", "branch")) or "").strip() or None
    explicit_symbol = _first_nonempty(record, ("ticker", "symbol", "asset_ticker", "assetTicker"))
    issuer_name = str(
        _first_nonempty(
            record,
            (
                "issuer",
                "issuer_name",
                "asset_description",
                "assetDescription",
                "asset_name",
                "assetName",
                "company",
                "company_name",
                "asset",
            ),
        )
        or ""
    ).strip()
    raw_transaction_type = _first_nonempty(record, ("type", "transaction", "transaction_type", "tx_type"))
    transaction_type, direction = _normalize_direction(raw_transaction_type)
    amount_range = str(_first_nonempty(record, ("amount", "amount_range", "range", "value")) or "").strip() or None
    amount_low, amount_high, amount_mid = _parse_amount_range(amount_range)
    transaction_date = str(
        _first_nonempty(record, ("transaction_date", "tx_date", "trade_date", "date", "transactionDate")) or ""
    ).strip() or None
    disclosure_date = str(
        _first_nonempty(
            record,
            ("disclosure_date", "date_received", "reported_date", "notification_date", "filed_date", "disclosureDate"),
        )
        or ""
    ).strip() or None
    owner_name = str(_first_nonempty(record, ("owner", "owner_name", "ownerType")) or "").strip() or None
    source_record_id = str(_first_nonempty(record, ("id", "ptr_id", "transaction_id", "doc_id", "disclosure_id")) or "").strip() or None
    resolved_symbol, resolution_meta = _resolve_symbol(
        explicit_symbol=str(explicit_symbol or ""),
        issuer_name=issuer_name,
        politician_name=politician_name,
        allowed_symbols=allowed_symbols,
    )
    ingested_ts = int(ingested_ts_ms or int(time.time() * 1000))
    source_trade_id = source_record_id or _stable_id(
        (
            source_name,
            politician_name,
            issuer_name,
            explicit_symbol,
            raw_transaction_type,
            transaction_date,
            disclosure_date,
            amount_range,
        )
    )

    diagnostics_json = dict(resolution_meta)
    if explicit_symbol:
        diagnostics_json["explicit_symbol"] = _normalize_symbol(explicit_symbol)
    row = {
        "source_trade_id": str(source_trade_id),
        "created_ts_ms": int(ingested_ts),
        "ingested_ts_ms": int(ingested_ts),
        "source": str(source_name or "congressional_trade"),
        "source_record_id": source_record_id,
        "source_url": source_url or None,
        "chamber": chamber or None,
        "office": office,
        "politician_name": politician_name or None,
        "owner_name": owner_name,
        "symbol": resolved_symbol,
        "issuer_name": issuer_name or None,
        "transaction_type_raw": str(raw_transaction_type or "").strip() or None,
        "transaction_type": transaction_type,
        "direction": direction,
        "amount_range": amount_range,
        "amount_low": amount_low,
        "amount_high": amount_high,
        "amount_mid": amount_mid,
        "transaction_date": transaction_date,
        "transaction_ts_ms": _parse_ts_ms(transaction_date),
        "disclosure_date": disclosure_date,
        "disclosure_ts_ms": _parse_ts_ms(disclosure_date),
        "payload_json": dict(record),
        "diagnostics_json": diagnostics_json,
    }
    return apply_congressional_trade_resolution(row, resolution_meta=resolution_meta)


def normalize_congressional_payload(
    payload: Any,
    *,
    source_name: str,
    source_url: str = "",
    default_chamber: str = "",
    allowed_symbols: Optional[Sequence[str]] = None,
    ingested_ts_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Normalize a raw provider payload into congressional-trade rows."""
    rows: List[Dict[str, Any]] = []
    for record in _iter_records(payload):
        rows.append(
            normalize_congressional_trade_record(
                record,
                source_name=source_name,
                source_url=source_url,
                default_chamber=default_chamber,
                allowed_symbols=allowed_symbols,
                ingested_ts_ms=ingested_ts_ms,
            )
        )
    return rows


def fetch_congressional_trades(
    *,
    backfill_days: Optional[int] = None,
    allowed_symbols: Optional[Sequence[str]] = None,
    session: Optional[requests.Session] = None,
    source_specs: Optional[Sequence[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Fetch and normalize congressional-trade disclosures from configured sources."""
    lookback_days = max(1, int(backfill_days or CONGRESSIONAL_BACKFILL_DAYS))
    cutoff_ms = int(time.time() * 1000) - int(lookback_days * 24 * 3600 * 1000)
    session_obj = session or requests.Session()
    rows: List[Dict[str, Any]] = []

    for spec in source_specs or DEFAULT_SOURCE_SPECS:
        source_name = str((spec or {}).get("name") or "congressional_trade").strip()
        source_url = str((spec or {}).get("url") or "").strip()
        default_chamber = str((spec or {}).get("chamber") or "").strip()
        if not source_url:
            continue
        try:
            response = session_obj.get(source_url, timeout=CONGRESSIONAL_TIMEOUT_S)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            _warn_nonfatal(
                "CONGRESSIONAL_TRADES_FETCH_FAILED",
                exc,
                once_key=f"congressional_fetch:{source_name}:{source_url}",
                source_name=source_name,
                source_url=source_url,
            )
            continue
        source_rows = normalize_congressional_payload(
            payload,
            source_name=source_name,
            source_url=source_url,
            default_chamber=default_chamber,
            allowed_symbols=allowed_symbols,
        )
        for row in source_rows:
            event_ts_ms = row.get("transaction_ts_ms") or row.get("disclosure_ts_ms") or row.get("ingested_ts_ms")
            if event_ts_ms is not None and int(event_ts_ms) < int(cutoff_ms):
                continue
            rows.append(row)
    return rows
