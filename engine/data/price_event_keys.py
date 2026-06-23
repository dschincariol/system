"""Stable idempotency keys for raw price events."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping

PRICE_RAW_EVENT_KEY_VERSION = "price_raw:v1"
_SLUG_RE = re.compile(r"[^A-Za-z0-9_.-]+")


def _clean_text(value: Any, *, upper: bool = False, lower: bool = False) -> str:
    text = str(value or "").strip()
    if upper:
        return text.upper()
    if lower:
        return text.lower()
    return text


def _slug(value: str, default: str) -> str:
    text = _SLUG_RE.sub("_", str(value or "").strip()).strip("_")
    return text or str(default)


def _int_ms(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        ts = int(float(value))
    except Exception:
        return 0
    if 0 < ts < 10_000_000_000:
        ts *= 1000
    return int(ts) if ts > 0 else 0


def compute_price_raw_event_key(
    event: Mapping[str, Any] | None = None,
    *,
    provider: Any = None,
    symbol: Any = None,
    event_type: Any = None,
    event_ts_ms: Any = None,
    ts_ms: Any = None,
) -> str:
    """Return a deterministic raw price-event key from stable IDs and timestamps.

    Mutable market values such as last/bid/ask/volume are intentionally excluded.
    """

    payload = dict(event or {})
    provider_s = _clean_text(provider or payload.get("provider") or payload.get("source"), lower=True)
    symbol_s = _clean_text(symbol or payload.get("symbol"), upper=True)
    event_type_s = _clean_text(event_type or payload.get("event_type") or payload.get("ev") or "U", upper=True) or "U"
    event_ts = _int_ms(
        event_ts_ms
        or payload.get("event_ts_ms")
        or payload.get("timestamp")
        or payload.get("ts_ms")
        or payload.get("t")
        or ts_ms
    )
    stable_payload = {
        "version": 1,
        "provider": provider_s,
        "symbol": symbol_s,
        "event_type": event_type_s,
        "event_ts_ms": int(event_ts),
        "trade_ts_ms": _int_ms(payload.get("trade_ts_ms")),
        "quote_ts_ms": _int_ms(payload.get("quote_ts_ms")),
        "trade_id": _clean_text(payload.get("trade_id") or payload.get("i")),
        "sequence_number": _clean_text(payload.get("sequence_number") or payload.get("q") or payload.get("seq")),
        "exchange": _clean_text(payload.get("exchange") or payload.get("x")),
        "bid_exchange": _clean_text(payload.get("bid_exchange") or payload.get("bx")),
        "ask_exchange": _clean_text(payload.get("ask_exchange") or payload.get("ax")),
        "tick_type": _clean_text(payload.get("tick_type") or payload.get("field")),
    }
    encoded = json.dumps(stable_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:32]
    return (
        f"{PRICE_RAW_EVENT_KEY_VERSION}:"
        f"{_slug(provider_s, 'unknown')}:"
        f"{_slug(symbol_s, 'UNKNOWN')}:"
        f"{_slug(event_type_s, 'U')}:"
        f"{int(event_ts)}:"
        f"{digest}"
    )


__all__ = ["PRICE_RAW_EVENT_KEY_VERSION", "compute_price_raw_event_key"]
