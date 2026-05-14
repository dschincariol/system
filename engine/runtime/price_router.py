"""
FILE: price_router.py

Runtime subsystem module for `price_router`.
"""

import os
import logging
import math
import time
import threading
from typing import Any, Dict, Iterable, List, Optional

from engine.runtime.metrics import emit_counter
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.observability import record_component_health
from engine.runtime.storage import (
    run_write_txn,
    _ensure_price_quotes_schema,
    _ensure_price_quotes_raw_schema,
    register_after_commit,
)
from engine.runtime.async_writer import enqueue_price_persistence, get_async_writer
from engine.runtime.telemetry_append_buffer import enqueue_price_quotes_raw_rows
from engine.runtime.timeseries_write_policy import get_timeseries_write_policy
from engine.runtime.event_bus import publish_event
from engine.data.provider_registry import get_provider_definition

LOG = get_logger("runtime.price_router")
_WARNED_NONFATAL_KEYS: set[str] = set()


def now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.price_router",
        persist=False,
        extra=dict(extra or {}) or None,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        result = float(value)
        if not math.isfinite(result):
            return None
        return float(result)
    except Exception as e:
        log_failure(
            LOG,
            event="runtime_price_router_as_float_failed",
            code="RUNTIME_PRICE_ROUTER_AS_FLOAT_FAILED",
            message="runtime_price_router_as_float_failed",
            error=e,
            level=logging.WARNING,
            component="engine.runtime.price_router",
            persist=False,
            extra={"value_repr": repr(value)},
        )
        return None


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _uses_postgres_storage(db: Any) -> bool:
    raw = getattr(db, "raw", None)
    module = f"{type(db).__module__}.{type(db).__name__} {type(raw).__module__}.{type(raw).__name__}".lower()
    return "storage_pg" in module or "psycopg" in module


_ROUTER_LOCK = threading.RLock()

_LAST_EVENT_KEY_BY_STREAM: Dict[str, str] = {}
_LAST_EVENT_TS_BY_STREAM: Dict[str, int] = {}
_GAP_EWMA_MS_BY_STREAM: Dict[str, float] = {}

_PRICE_EVENT_DEDUP_WINDOW_MS = int(os.environ.get("PRICE_EVENT_DEDUP_WINDOW_MS", "1500"))
_PRICE_EVENT_GAP_MIN_MS = int(os.environ.get("PRICE_EVENT_GAP_MIN_MS", "2000"))
_PRICE_EVENT_GAP_FACTOR = float(os.environ.get("PRICE_EVENT_GAP_FACTOR", "4.0"))
_PRICE_EVENT_GAP_EMA_ALPHA = float(os.environ.get("PRICE_EVENT_GAP_EMA_ALPHA", "0.20"))


def get_price_persistence_mode() -> Dict[str, Any]:
    writer_enabled = False
    try:
        writer_enabled = bool(getattr(get_async_writer(), "enabled", False))
    except Exception:
        writer_enabled = False
    return get_timeseries_write_policy().price_persistence_mode(
        async_price_writer_enabled=bool(writer_enabled)
    )


# =========================
# PROVIDER CONFIG
# =========================

def _provider_stale_after_ms(provider: str) -> int:
    name = str(provider or "").strip().lower()
    d = get_provider_definition(name)

    if d and d.supports:
        t = d.supports.get("transport")
        if t == "websocket":
            return int(os.environ.get("PROVIDER_STALE_MS_STREAM", "8000"))
        if t == "gateway":
            return int(os.environ.get("PROVIDER_STALE_MS_GATEWAY", "10000"))
        if t == "rest":
            return int(os.environ.get("PROVIDER_STALE_MS_REST", "120000"))

    return int(os.environ.get("PROVIDER_STALE_MS", "120000"))


def _compute_provider_score(latency_ms: int, age_ms: int, stale_after_ms: int) -> float:
    if stale_after_ms <= 0:
        return -1e9

    latency_score = max(0.0, 1.0 - (float(latency_ms) / float(stale_after_ms)))
    freshness_score = max(0.0, 1.0 - (float(age_ms) / float(stale_after_ms)))

    return (latency_score * 0.6) + (freshness_score * 0.4)


# =========================
# NORMALIZATION
# =========================

def _normalize_event_strict(e: Dict[str, Any], received_ts_ms: int) -> Optional[Dict[str, Any]]:
    symbol = _normalize_symbol(e.get("symbol"))
    if not symbol:
        return None

    provider = str(e.get("provider") or e.get("source") or "unknown").strip().lower()
    max_age_ms = 7 * 86400 * 1000

    ts_exchange = int(e.get("timestamp") or e.get("ts_ms") or 0)
    if 0 < ts_exchange < 10_000_000_000:
        ts_exchange *= 1000
    if ts_exchange <= 0:
        ts_exchange = received_ts_ms
    elif ts_exchange > (received_ts_ms + 300_000):
        ts_exchange = received_ts_ms
    elif ts_exchange < (received_ts_ms - max_age_ms):
        _warn_nonfatal(
            "RUNTIME_PRICE_ROUTER_ANCIENT_TIMESTAMP_CLAMPED",
            RuntimeError("ancient_price_timestamp"),
            once_key=f"ancient_timestamp:{provider}:{symbol}",
            provider=provider,
            symbol=symbol,
            original_ts_ms=int(ts_exchange),
            received_ts_ms=int(received_ts_ms),
        )
        ts_exchange = received_ts_ms

    bid = _as_float(e.get("bid"))
    ask = _as_float(e.get("ask"))
    last = _as_float(e.get("last") or e.get("price"))

    if bid is not None and bid <= 0:
        _warn_nonfatal(
            "RUNTIME_PRICE_ROUTER_NONPOSITIVE_BID",
            RuntimeError("nonpositive_bid"),
            once_key=f"nonpositive_bid:{provider}:{symbol}",
            provider=provider,
            symbol=symbol,
            bid=float(bid),
        )
        bid = None
    if ask is not None and ask <= 0:
        _warn_nonfatal(
            "RUNTIME_PRICE_ROUTER_NONPOSITIVE_ASK",
            RuntimeError("nonpositive_ask"),
            once_key=f"nonpositive_ask:{provider}:{symbol}",
            provider=provider,
            symbol=symbol,
            ask=float(ask),
        )
        ask = None

    if last is not None and last <= 0:
        _warn_nonfatal(
            "RUNTIME_PRICE_ROUTER_NONPOSITIVE_LAST",
            RuntimeError("nonpositive_last"),
            once_key=f"nonpositive_last:{provider}:{symbol}",
            provider=provider,
            symbol=symbol,
            last=float(last),
        )
        last = None

    if last is None and bid is not None and ask is not None:
        last = (bid + ask) / 2.0

    if last is None and bid is None and ask is None:
        return None

    latency_ms = int(e.get("latency_ms") or max(0, received_ts_ms - ts_exchange))
    event_type = str(e.get("event_type") or "").strip().upper()
    if event_type not in {"T", "Q", ""}:
        event_type = ""
    trade_ts_ms = int(e.get("trade_ts_ms") or (ts_exchange if event_type == "T" else 0) or 0)
    quote_ts_ms = int(e.get("quote_ts_ms") or (ts_exchange if event_type == "Q" else 0) or 0)
    last_update_ts_ms = max(int(trade_ts_ms or 0), int(quote_ts_ms or 0), int(ts_exchange or 0))
    event_key = str(e.get("event_key") or "").strip()
    if not event_key:
        event_key = f'{provider}|{symbol}|{event_type or "U"}|{ts_exchange}|{e.get("trade_id")}|{e.get("sequence_number")}|{last}|{bid}|{ask}|{e.get("volume")}'

    return {
        "symbol": symbol,
        "timestamp": int(ts_exchange),
        "provider": provider,
        "bid": bid,
        "ask": ask,
        "last": last,
        "volume": _as_float(e.get("volume")),
        "latency_ms": latency_ms,
        "spread": _as_float(e.get("spread") or (ask - bid if bid is not None and ask is not None else None)),
        "source": str(e.get("source") or provider),
        "event_type": event_type,
        "event_key": event_key,
        "trade_ts_ms": int(trade_ts_ms or 0),
        "quote_ts_ms": int(quote_ts_ms or 0),
        "last_update_ts_ms": int(last_update_ts_ms or 0),
        "ingest_ts_ms": int(e.get("ingest_ts_ms") or received_ts_ms),
    }


# =========================
# DEDUP / GAP
# =========================

def _stream_key(row: Dict[str, Any]) -> str:
    return f'{row["provider"]}::{row["symbol"]}::{row.get("event_type") or "U"}'


def _event_key(row: Dict[str, Any]) -> str:
    if row.get("event_key"):
        return str(row.get("event_key"))
    return f'{row["timestamp"]}|{row["last"]}|{row["bid"]}|{row["ask"]}|{row["volume"]}'


def _is_duplicate_event(row: Dict[str, Any]) -> bool:
    k = _stream_key(row)
    ek = _event_key(row)
    ts = int(row["timestamp"])

    with _ROUTER_LOCK:
        prev_k = _LAST_EVENT_KEY_BY_STREAM.get(k)
        prev_ts = _LAST_EVENT_TS_BY_STREAM.get(k, 0)
        return bool(prev_k == ek and abs(ts - prev_ts) <= _PRICE_EVENT_DEDUP_WINDOW_MS)


def _detect_gap_ms(row: Dict[str, Any]) -> int:
    k = _stream_key(row)
    ts = int(row["timestamp"])

    with _ROUTER_LOCK:
        prev_ts = _LAST_EVENT_TS_BY_STREAM.get(k, 0)
        prev_ema = _GAP_EWMA_MS_BY_STREAM.get(k, 0.0)

        gap = max(0, ts - prev_ts) if prev_ts > 0 else 0

        if gap > 0:
            ema = prev_ema if prev_ema > 0 else gap
            ema = (1 - _PRICE_EVENT_GAP_EMA_ALPHA) * ema + _PRICE_EVENT_GAP_EMA_ALPHA * gap
            _GAP_EWMA_MS_BY_STREAM[k] = ema

        threshold = max(_PRICE_EVENT_GAP_MIN_MS, int(prev_ema * _PRICE_EVENT_GAP_FACTOR))
        return gap if gap >= threshold else 0


def _record_event_state(row: Dict[str, Any]) -> None:
    k = _stream_key(row)
    ek = _event_key(row)
    ts = int(row["timestamp"])

    with _ROUTER_LOCK:
        _LAST_EVENT_KEY_BY_STREAM[k] = ek
        _LAST_EVENT_TS_BY_STREAM[k] = ts


# =========================
# MULTI-PROVIDER MERGE (FINAL PIECE)
# =========================

def _merge_best_by_symbol(rows: List[Dict[str, Any]], received_ts_ms: int) -> List[Dict[str, Any]]:
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}

    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    out: List[Dict[str, Any]] = []

    for symbol, group in by_symbol.items():
        best = None
        best_score = -1e12

        for r in group:
            age = received_ts_ms - r["timestamp"]
            stale = _provider_stale_after_ms(r["provider"])
            score = _compute_provider_score(r["latency_ms"], age, stale)

            if score > best_score:
                best_score = score
                best = r

        if best:
            best["provider_score"] = best_score
            out.append(best)

    return out


# =========================
# MAIN
# =========================

def publish_price_events(
    events: Iterable[Dict[str, Any]],
    *,
    con=None,
    write_prices: bool = True,
    write_quotes: bool = True,
    write_raw: bool = True,
    emit_telemetry: bool = True,
    component: str = "engine.runtime.price_router",
    job: Optional[str] = None,
    default_provider: Optional[str] = None,
    update_symbols: bool = False,
) -> Dict[str, int]:

    received_ts_ms = now_ms()

    normalized: List[Dict[str, Any]] = []
    dedup = 0
    gaps = 0
    normalization_failures = 0

    for e in events or []:
        try:
            row = _normalize_event_strict(e, received_ts_ms)
        except Exception as exc:
            normalization_failures += 1
            _warn_nonfatal(
                "PRICE_ROUTER_EVENT_NORMALIZATION_FAILED",
                exc,
                provider=str((e or {}).get("provider") or (e or {}).get("source") or ""),
                symbol=str((e or {}).get("symbol") or ""),
            )
            continue
        if not row:
            normalization_failures += 1
            continue

        if default_provider and (not row.get("provider") or row.get("provider") == "unknown"):
            row["provider"] = str(default_provider).strip().lower()
            row["source"] = str(default_provider).strip().lower()

        if _is_duplicate_event(row):
            dedup += 1
            continue

        gap = _detect_gap_ms(row)
        if gap > 0:
            row["gap_ms"] = gap
            gaps += 1

        _record_event_state(row)

        if row.get("symbol") and int(row.get("timestamp") or 0) > 0:
            normalized.append(row)

    if not normalized:
        return {
            "events": 0,
            "raw": 0,
            "quotes": 0,
            "prices": 0,
            "dedup_drops": dedup,
            "gap_events": gaps,
            "normalization_failures": normalization_failures,
        }

    merged = _merge_best_by_symbol(normalized, received_ts_ms)

    raw_rows = [
        (
            int(r["timestamp"]),
            str(r["symbol"]),
            str(r["provider"]),
            str(r.get("event_key") or _event_key(r)),
            str(r.get("event_type") or ""),
            int(r.get("timestamp") or 0),
            r.get("last"),
            r.get("bid"),
            r.get("ask"),
            r.get("spread"),
            r.get("volume"),
            (int(r.get("trade_ts_ms") or 0) or None),
            (int(r.get("quote_ts_ms") or 0) or None),
            (int(r.get("ingest_ts_ms") or received_ts_ms) or received_ts_ms),
            str(r.get("source") or r.get("provider") or ""),
        )
        for r in normalized
    ]

    quote_rows = [
        (
            int(r["timestamp"]),
            str(r["symbol"]),
            r.get("last"),
            r.get("bid"),
            r.get("ask"),
            r.get("spread"),
            r.get("volume"),
            str(r.get("source") or r.get("provider") or ""),
            (int(r.get("trade_ts_ms") or 0) or None),
            (int(r.get("quote_ts_ms") or 0) or None),
            int(r.get("last_update_ts_ms") or r.get("timestamp") or 0),
        )
        for r in merged
    ]

    price_rows = [
        (
            int(r["timestamp"]),
            str(r["symbol"]),
            r.get("last"),
            r.get("last"),
            str(r.get("provider") or r.get("source") or ""),
        )
        for r in merged
    ]
    pg_price_rows = [
        {
            "ts_ms": int(r["timestamp"]),
            "symbol": str(r["symbol"]),
            "price": _as_float(r.get("last")),
            "last": _as_float(r.get("last")),
            "bid": _as_float(r.get("bid")),
            "ask": _as_float(r.get("ask")),
            "spread": _as_float(r.get("spread")),
            "volume": _as_float(r.get("volume")),
            "provider": str(r.get("provider") or ""),
            "source": str(r.get("source") or r.get("provider") or ""),
            "latency_ms": int(r.get("latency_ms") or 0),
            "provider_score": float(r.get("provider_score") or 0.0),
            "last_update_ts_ms": int(r.get("last_update_ts_ms") or r.get("timestamp") or 0),
            "ingest_ts_ms": int(r.get("ingest_ts_ms") or received_ts_ms),
        }
        for r in merged
    ]
    pg_quote_rows = [
        {
            "ts_ms": int(r["timestamp"]),
            "symbol": str(r["symbol"]),
            "last": _as_float(r.get("last")),
            "bid": _as_float(r.get("bid")),
            "ask": _as_float(r.get("ask")),
            "spread": _as_float(r.get("spread")),
            "volume": _as_float(r.get("volume")),
            "source": str(r.get("source") or r.get("provider") or ""),
            "last_trade_ts_ms": (int(r.get("trade_ts_ms") or 0) or None),
            "last_quote_ts_ms": (int(r.get("quote_ts_ms") or 0) or None),
            "last_update_ts_ms": int(r.get("last_update_ts_ms") or r.get("timestamp") or 0),
        }
        for r in merged
    ]
    pg_raw_rows = [
        {
            "ts_ms": int(r["timestamp"]),
            "symbol": str(r["symbol"]),
            "provider": str(r["provider"]),
            "event_key": str(r.get("event_key") or _event_key(r)),
            "event_type": str(r.get("event_type") or ""),
            "event_ts_ms": int(r.get("timestamp") or 0),
            "last": _as_float(r.get("last")),
            "bid": _as_float(r.get("bid")),
            "ask": _as_float(r.get("ask")),
            "spread": _as_float(r.get("spread")),
            "volume": _as_float(r.get("volume")),
            "trade_ts_ms": (int(r.get("trade_ts_ms") or 0) or None),
            "quote_ts_ms": (int(r.get("quote_ts_ms") or 0) or None),
            "ingest_ts_ms": int(r.get("ingest_ts_ms") or received_ts_ms),
            "source": str(r.get("source") or r.get("provider") or ""),
        }
        for r in normalized
    ]
    persistence_mode = get_price_persistence_mode()
    write_policy = get_timeseries_write_policy()
    live_storage_guard = write_policy.validate_high_volume_runtime()
    if not bool(live_storage_guard.get("ok")):
        raise RuntimeError(str(live_storage_guard.get("reason") or "price_router_storage_policy_blocked"))
    write_plan = write_policy.plan_price_router_writes(
        write_prices=bool(write_prices),
        write_quotes=bool(write_quotes),
        write_raw=bool(write_raw),
    )
    sqlite_write_raw = bool(write_plan.sqlite_write_raw)
    sqlite_write_quotes = bool(write_plan.sqlite_write_quotes)
    sqlite_write_prices = bool(write_plan.sqlite_write_prices)
    async_required = bool(write_plan.async_required)
    async_writer_enabled = bool(persistence_mode.get("async_price_writer_enabled"))
    if async_required and bool(write_policy.require_async_during_cutover) and not async_writer_enabled:
        raise RuntimeError("price_router_sqlite_cutover_requires_async_price_writer")

    def _write(db):
        n_raw = int(len(raw_rows)) if bool(write_raw and not sqlite_write_raw) else 0
        n_quotes = int(len(quote_rows)) if bool(write_quotes and not sqlite_write_quotes) else 0
        n_prices = int(len(price_rows)) if bool(write_prices and not sqlite_write_prices) else 0

        # Keep deferred raw evidence fully off the immediate live-stream write
        # path. The telemetry append buffer owns raw-schema bootstrap when raw
        # evidence is buffered instead of sync-written here.
        if db is not None and sqlite_write_quotes:
            _ensure_price_quotes_schema(db)
        if db is not None and sqlite_write_raw:
            _ensure_price_quotes_raw_schema(db)

        if sqlite_write_raw and raw_rows:
            try:
                db.executemany(
                    """
                    INSERT INTO price_quotes_raw(
                      ts_ms, symbol, provider, event_key, event_type, event_ts_ms,
                      last, bid, ask, spread, volume,
                      trade_ts_ms, quote_ts_ms, ingest_ts_ms, source
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(symbol, provider, event_key) DO UPDATE SET
                      ts_ms=excluded.ts_ms,
                      event_type=excluded.event_type,
                      event_ts_ms=excluded.event_ts_ms,
                      last=excluded.last,
                      bid=excluded.bid,
                      ask=excluded.ask,
                      spread=excluded.spread,
                      volume=excluded.volume,
                      trade_ts_ms=excluded.trade_ts_ms,
                      quote_ts_ms=excluded.quote_ts_ms,
                      ingest_ts_ms=excluded.ingest_ts_ms,
                      source=excluded.source
                    """,
                    raw_rows,
                )
                n_raw = len(raw_rows)
            except Exception as e:
                log_failure(
                    LOG,
                    event="price_router_raw_quote_write_failed",
                    code="PRICE_ROUTER_RAW_QUOTE_WRITE_FAILED",
                    message="price_router_raw_quote_write_failed",
                    error=e,
                    level=logging.WARNING,
                    component="engine.runtime.price_router",
                    persist=False,
                    extra={"row_count": len(raw_rows)},
                )

        if sqlite_write_quotes and quote_rows:
            try:
                db.executemany(
                    """
                    INSERT INTO price_quotes(
                      ts_ms, symbol, last, bid, ask, spread, volume, source,
                      last_trade_ts_ms, last_quote_ts_ms, last_update_ts_ms
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                      last=
                        CASE
                          WHEN excluded.last_trade_ts_ms IS NOT NULL
                           AND COALESCE(price_quotes.last_trade_ts_ms, 0) <= excluded.last_trade_ts_ms
                          THEN excluded.last
                          WHEN price_quotes.last IS NULL AND excluded.last IS NOT NULL
                          THEN excluded.last
                          ELSE price_quotes.last
                        END,
                      bid=
                        CASE
                          WHEN excluded.last_quote_ts_ms IS NOT NULL
                           AND COALESCE(price_quotes.last_quote_ts_ms, 0) <= excluded.last_quote_ts_ms
                          THEN excluded.bid
                          ELSE price_quotes.bid
                        END,
                      ask=
                        CASE
                          WHEN excluded.last_quote_ts_ms IS NOT NULL
                           AND COALESCE(price_quotes.last_quote_ts_ms, 0) <= excluded.last_quote_ts_ms
                          THEN excluded.ask
                          ELSE price_quotes.ask
                        END,
                      spread=
                        CASE
                          WHEN excluded.last_quote_ts_ms IS NOT NULL
                           AND COALESCE(price_quotes.last_quote_ts_ms, 0) <= excluded.last_quote_ts_ms
                          THEN excluded.spread
                          ELSE price_quotes.spread
                        END,
                      volume=
                        CASE
                          WHEN excluded.last_trade_ts_ms IS NOT NULL
                           AND COALESCE(price_quotes.last_trade_ts_ms, 0) <= excluded.last_trade_ts_ms
                          THEN excluded.volume
                          WHEN price_quotes.volume IS NULL AND excluded.volume IS NOT NULL
                          THEN excluded.volume
                          ELSE price_quotes.volume
                        END,
                      source=
                        CASE
                          WHEN COALESCE(price_quotes.last_update_ts_ms, 0) <= COALESCE(excluded.last_update_ts_ms, 0)
                          THEN excluded.source
                          ELSE price_quotes.source
                        END,
                      last_trade_ts_ms=GREATEST(COALESCE(price_quotes.last_trade_ts_ms, 0), COALESCE(excluded.last_trade_ts_ms, 0)),
                      last_quote_ts_ms=GREATEST(COALESCE(price_quotes.last_quote_ts_ms, 0), COALESCE(excluded.last_quote_ts_ms, 0)),
                      last_update_ts_ms=GREATEST(COALESCE(price_quotes.last_update_ts_ms, 0), COALESCE(excluded.last_update_ts_ms, 0))
                    """,
                    quote_rows,
                )
                n_quotes = len(quote_rows)
            except Exception as e:
                log_failure(
                    LOG,
                    event="price_router_quote_write_failed",
                    code="PRICE_ROUTER_QUOTE_WRITE_FAILED",
                    message="price_router_quote_write_failed",
                    error=e,
                    level=logging.WARNING,
                    component="engine.runtime.price_router",
                    persist=False,
                    extra={"row_count": len(quote_rows)},
                )

        if sqlite_write_prices and price_rows:
            db.executemany(
                """
                INSERT INTO prices(ts_ms, symbol, price, px, source)
                VALUES (?,?,?,?,?)
                ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                  price=excluded.price,
                  px=excluded.px,
                  source=excluded.source
                """,
                price_rows,
            )
            n_prices = len(price_rows)

        feature_rows = []
        feature_symbols = []
        if price_rows:
            feature_rows = [
                {
                    "symbol": str(r.get("symbol") or ""),
                    "ts_ms": int(r.get("timestamp") or 0),
                    "price": r.get("last"),
                    "volume": r.get("volume"),
                    "source": str(r.get("source") or r.get("provider") or ""),
                }
                for r in merged
                if str(r.get("symbol") or "").strip() and int(r.get("timestamp") or 0) > 0 and _as_float(r.get("last")) is not None
            ]
            feature_symbols = sorted(
                {
                    str(row.get("symbol") or "").strip().upper()
                    for row in feature_rows
                    if str(row.get("symbol") or "").strip()
                }
            )

        if pg_price_rows or pg_quote_rows or pg_raw_rows:
            def _after_commit_runtime_fanout() -> None:
                if feature_rows and feature_symbols:
                    try:
                        from engine.data import feature_store as _feature_store
                        from engine.runtime import price_cache as _price_cache

                        _price_cache.record_price_rows(feature_rows)
                        _feature_store.refresh_symbols(feature_symbols, price_cache=_price_cache)
                    except Exception as e:
                        log_failure(
                            LOG,
                            event="price_router_feature_refresh_failed",
                            code="PRICE_ROUTER_FEATURE_REFRESH_FAILED",
                            message="price_router_feature_refresh_failed",
                            error=e,
                            level=logging.WARNING,
                            component="engine.runtime.price_router",
                            persist=False,
                            extra={"symbols": list(feature_symbols)},
                        )
                if write_raw and raw_rows and not sqlite_write_raw:
                    try:
                        accepted = enqueue_price_quotes_raw_rows(tuple(raw_rows))
                        if not accepted:
                            raise RuntimeError("price_router_raw_buffer_rejected")
                    except Exception as e:
                        log_failure(
                            LOG,
                            event="price_router_raw_quote_buffer_enqueue_failed",
                            code="PRICE_ROUTER_RAW_QUOTE_BUFFER_ENQUEUE_FAILED",
                            message="price_router_raw_quote_buffer_enqueue_failed",
                            error=e,
                            level=logging.WARNING,
                            component="engine.runtime.price_router",
                            persist=False,
                            extra={"row_count": len(raw_rows)},
                        )
                try:
                    enqueue_price_persistence(
                        prices=tuple(pg_price_rows),
                        quotes=tuple(pg_quote_rows),
                        raw=tuple(pg_raw_rows),
                        source=str(component),
                    )
                except Exception as e:
                    log_failure(
                        LOG,
                        event="price_router_async_persistence_enqueue_failed",
                        code="PRICE_ROUTER_ASYNC_PERSISTENCE_ENQUEUE_FAILED",
                        message="price_router_async_persistence_enqueue_failed",
                        error=e,
                        level=logging.WARNING,
                        component="engine.runtime.price_router",
                        persist=False,
                        extra={
                            "price_rows": int(len(pg_price_rows)),
                            "quote_rows": int(len(pg_quote_rows)),
                            "raw_rows": int(len(pg_raw_rows)),
                        },
                    )

            register_after_commit(db, _after_commit_runtime_fanout)

        if db is not None and update_symbols and merged:
            update_ts_ms = int(received_ts_ms)
            if _uses_postgres_storage(db):
                update_symbol_sql = """
                    UPDATE symbols SET
                      updated_ts_ms=?,
                      meta_json=jsonb_set(
                        COALESCE(meta_json, '{}'::jsonb),
                        '{price_status,last_seen_ts_ms}',
                        to_jsonb(CAST(? AS BIGINT)),
                        true
                      )
                    WHERE symbol=?
                    """
            else:
                update_symbol_sql = """
                    UPDATE symbols SET
                      updated_ts_ms=?,
                      meta_json=json_set(
                        COALESCE(meta_json,'{}'),
                        '$.price_status.last_seen_ts_ms', ?
                      )
                    WHERE symbol=?
                    """
            for r in merged:
                db.execute(
                    update_symbol_sql,
                    (update_ts_ms, int(r["timestamp"]), str(r["symbol"])),
                )

        return {
            "events": len(merged),
            "raw": n_raw,
            "quotes": n_quotes,
            "prices": n_prices,
        }

    needs_sqlite_txn = bool(con is not None or sqlite_write_raw or sqlite_write_quotes or sqlite_write_prices or update_symbols)
    counts = _write(con) if con is not None else (_write(None) if not needs_sqlite_txn else run_write_txn(_write))

    if emit_telemetry:
        emit_counter(
            "market_data_event",
            int(counts.get("events") or 0),
            component=component,
            job=job,
        )

    for r in merged:
        try:
            publish_event(
                "price_tick",
                {
                    "symbol": str(r.get("symbol") or ""),
                    "price": _as_float(r.get("last")),
                    "bid": _as_float(r.get("bid")),
                    "ask": _as_float(r.get("ask")),
                    "spread": _as_float(r.get("spread")),
                    "volume": _as_float(r.get("volume")),
                    "provider": str(r.get("provider") or ""),
                    "source": str(r.get("source") or r.get("provider") or ""),
                    "ts_ms": int(r.get("timestamp") or received_ts_ms),
                    "latency_ms": int(r.get("latency_ms") or 0),
                    "provider_score": float(r.get("provider_score") or 0.0),
                },
            )
        except Exception as e:
            try:
                log_failure(
                    LOG,
                    event="price_router_publish_event_failed",
                    code="PRICE_ROUTER_PUBLISH_EVENT_FAILED",
                    message="price_router_publish_event_failed",
                    error=e,
                    level=30,
                    component="engine.runtime.price_router",
                    extra={"symbol": str(r.get("symbol") or ""), "provider": str(r.get("provider") or "")},
                    persist=False,
                )
            except Exception:
                raise

    latest_tick_ts_ms = max((int(r.get("timestamp") or 0) for r in merged), default=0)
    latest_latency_ms = max(0, int(received_ts_ms) - int(latest_tick_ts_ms)) if latest_tick_ts_ms > 0 else 0
    record_component_health(
        "ingestion",
        ok=bool(merged),
        status="ok" if merged else "idle",
        detail="price_tick_published" if merged else "no_price_events",
        observed_ts_ms=int(received_ts_ms),
        latency_ms=float(latest_latency_ms),
        extra={
            "events": int(len(merged)),
            "dedup_drops": int(dedup),
            "gap_events": int(gaps),
            "normalization_failures": int(normalization_failures),
            "last_price_ts_ms": (int(latest_tick_ts_ms) if latest_tick_ts_ms > 0 else None),
        },
    )

    return {
        "events": int(counts.get("events") or 0),
        "raw": int(counts.get("raw") or 0),
        "quotes": int(counts.get("quotes") or 0),
        "prices": int(counts.get("prices") or 0),
        "dedup_drops": dedup,
        "gap_events": gaps,
        "normalization_failures": normalization_failures,
    }


def publish_price_event(event: Dict[str, Any], **kwargs) -> Dict[str, int]:
    return publish_price_events([event], **kwargs)
