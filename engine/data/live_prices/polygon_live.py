"""
FILE: polygon_live.py

Live price feed integration for `polygon_live`.
"""

"""
Polygon price adapter.

Provides:
- latest price
- best-effort spread proxy (NBBO if available)
- volume + timestamp
- recent 1-minute bars for reconnect gap fill

Contract:
- fetch_last_prices(ticker_map) -> dict
"""

import os
import math
import time
import requests
from urllib.parse import urlencode, urlparse, parse_qsl, urlunparse

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.json_codec import loads as _json_loads
from engine.runtime.logging import get_logger

_BASE = "https://api.polygon.io"
_BATCH_SNAPSHOT_PATH = "/v2/snapshot/locale/us/markets/stocks/tickers"
_SINGLE_SNAPSHOT_PATH = "/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
LOG = get_logger("engine.data.live_prices.polygon_live")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.data.live_prices.polygon_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _finite_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _response_json(response: object) -> object:
    content = getattr(response, "content", None)
    if isinstance(content, memoryview):
        return _json_loads(content)
    if isinstance(content, (bytes, bytearray)) and bytes(content).strip():
        return _json_loads(content)

    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        return _json_loads(text)

    json_fn = getattr(response, "json", None)
    if callable(json_fn):
        return json_fn() or {}
    return {}


def _chunked(items, size: int):
    chunk_size = max(1, int(size or 1))
    for idx in range(0, len(items), chunk_size):
        yield items[idx : idx + chunk_size]


def _polygon_key() -> str:
    return get_data_credential("POLYGON_API_KEY")


class _PolygonBatchSnapshotError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, reason: str = "batch_snapshot_error") -> None:
        super().__init__(str(message))
        self.status_code = status_code
        self.reason = str(reason or "batch_snapshot_error")


class _PolygonBatchSnapshotUnsupported(_PolygonBatchSnapshotError):
    pass


class _PolygonBatchSnapshotEntitlementError(_PolygonBatchSnapshotError):
    pass


class _PolygonBatchSnapshotRateLimited(_PolygonBatchSnapshotError):
    pass


def _response_status_code(response: object) -> int | None:
    raw = getattr(response, "status_code", None)
    try:
        status_code = int(raw)
    except Exception:
        return None
    if 100 <= status_code <= 599:
        return status_code
    return None


def _batch_snapshot_error_from_response(response: object) -> _PolygonBatchSnapshotError | None:
    status_code = _response_status_code(response)
    if status_code is None or status_code < 400:
        return None

    reason = f"http_{status_code}"
    if status_code in {404, 405}:
        return _PolygonBatchSnapshotUnsupported(
            f"polygon_batch_snapshot_unsupported:{reason}",
            status_code=status_code,
            reason="unsupported",
        )
    if status_code in {401, 403}:
        return _PolygonBatchSnapshotEntitlementError(
            f"polygon_batch_snapshot_entitlement:{reason}",
            status_code=status_code,
            reason="entitlement",
        )
    if status_code == 429:
        return _PolygonBatchSnapshotRateLimited(
            f"polygon_batch_snapshot_rate_limited:{reason}",
            status_code=status_code,
            reason="rate_limited",
        )
    return _PolygonBatchSnapshotError(
        f"polygon_batch_snapshot_failed:{reason}",
        status_code=status_code,
        reason=reason,
    )


def _batch_snapshot_error_from_payload(payload: object) -> _PolygonBatchSnapshotError | None:
    if not isinstance(payload, dict):
        return None
    status = str(payload.get("status") or "").strip().lower()
    if status not in {"error", "not_authorized", "not authorized", "forbidden"}:
        return None

    detail = str(payload.get("error") or payload.get("message") or status or "polygon_batch_snapshot_error")
    detail_l = detail.lower()
    if any(token in detail_l for token in ("entitlement", "not entitled", "permission", "not authorized", "forbidden", "subscription")):
        return _PolygonBatchSnapshotEntitlementError(
            f"polygon_batch_snapshot_entitlement:{detail[:160]}",
            reason="entitlement",
        )
    if any(token in detail_l for token in ("unsupported", "unknown endpoint", "not found")):
        return _PolygonBatchSnapshotUnsupported(
            f"polygon_batch_snapshot_unsupported:{detail[:160]}",
            reason="unsupported",
        )
    if "rate" in detail_l and "limit" in detail_l:
        return _PolygonBatchSnapshotRateLimited(
            f"polygon_batch_snapshot_rate_limited:{detail[:160]}",
            reason="rate_limited",
        )
    return _PolygonBatchSnapshotError(
        f"polygon_batch_snapshot_failed:{detail[:160]}",
        reason="payload_error",
    )


class PolygonPriceProvider:
    def __init__(self):
        if not _polygon_key():
            raise RuntimeError("POLYGON_API_KEY not set")

        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {"User-Agent": "market-impact-dev/1.0 (polygon)"}
        )
        self._snapshot_batch_size = max(
            1,
            int(os.environ.get("POLYGON_SNAPSHOT_BATCH_SIZE", "100") or "100"),
        )
        self._snapshot_timeout_s = max(
            1.0,
            float(os.environ.get("POLYGON_SNAPSHOT_TIMEOUT_S", "8") or "8"),
        )
        self._snapshot_batch_supported = True

    def _quote_from_snapshot_ticker(self, ticker_payload):
        ticker = dict(ticker_payload or {})
        day = ticker.get("day") or {}
        minute = ticker.get("min") or {}
        prev_day = ticker.get("prevDay") or {}
        last_trade = ticker.get("lastTrade") or ticker.get("last_trade") or {}
        last_quote = ticker.get("lastQuote") or ticker.get("last_quote") or {}

        px = last_trade.get("p")
        if px is None:
            px = last_trade.get("price")
        if px is None:
            px = minute.get("c")
        if px is None:
            px = day.get("c")
        if px is None:
            px = prev_day.get("c")
        if px is None:
            return {}

        ts_ns = ticker.get("updated") or last_trade.get("t") or last_quote.get("t")
        if ts_ns is not None:
            market_ts_ms = int(int(ts_ns) / 1_000_000)
        else:
            market_ts_ms = int((minute.get("t") or day.get("t") or time.time() * 1000))

        volume = last_trade.get("s")
        if volume is None:
            volume = last_trade.get("size")
        if volume is None:
            volume = minute.get("v")
        if volume is None:
            volume = day.get("v")

        bid = last_quote.get("bid_price")
        if bid is None:
            bid = last_quote.get("bp")
        if bid is None:
            bid = last_quote.get("p")
        ask = last_quote.get("ask_price")
        if ask is None:
            ask = last_quote.get("ap")
        if ask is None:
            ask = last_quote.get("P")
        bid_f = _finite_float_or_none(bid)
        ask_f = _finite_float_or_none(ask)
        spread = None
        if bid_f is not None and ask_f is not None:
            spread = float(ask_f) - float(bid_f)

        now_ms = int(time.time() * 1000)
        return {
            "px": px,
            "ts_ms": now_ms,
            "market_ts_ms": market_ts_ms,
            "bid": bid_f,
            "ask": ask_f,
            "spread": spread,
            "volume": volume,
            "source": "polygon_snapshot",
        }

    def _fetch_snapshot_batch(self, provider_symbols):
        symbols = [str(sym).strip().upper() for sym in (provider_symbols or []) if str(sym).strip()]
        if not symbols:
            return {}
        api_key = _polygon_key()
        if not api_key:
            raise RuntimeError("POLYGON_API_KEY not set")

        response = self.session.get(
            f"{_BASE}{_BATCH_SNAPSHOT_PATH}",
            params={"tickers": ",".join(symbols), "apiKey": api_key},
            timeout=self._snapshot_timeout_s,
        )
        response_error = _batch_snapshot_error_from_response(response)
        if response_error is not None:
            raise response_error
        if _response_status_code(response) is None:
            response.raise_for_status()
        payload = _response_json(response) or {}
        payload_error = _batch_snapshot_error_from_payload(payload)
        if payload_error is not None:
            raise payload_error
        rows = payload.get("tickers") or payload.get("results") or []
        out = {}
        for row in rows:
            ticker = str((row or {}).get("ticker") or "").strip().upper()
            if not ticker:
                continue
            quote = self._quote_from_snapshot_ticker(row)
            if quote:
                out[ticker] = quote
        return out

    def fetch_last_prices(self, ticker_map):
        out = {}
        requested = 0
        skipped = 0
        symbols = []
        reverse_map = {}

        for sym, provider_symbol in (ticker_map or {}).items():
            sym_s = str(sym)
            provider_symbol_s = str(provider_symbol).strip().upper()
            if not sym_s or not provider_symbol_s:
                continue
            requested += 1
            symbols.append((sym_s, provider_symbol_s))
            reverse_map.setdefault(provider_symbol_s, []).append(sym_s)

        snapshot_by_symbol = {}
        batch_errors = 0
        fallback_provider_symbols = set()
        batch_blocked_provider_symbols = set()
        batches_attempted = 0
        provider_symbol_keys = sorted(reverse_map.keys())
        if self._snapshot_batch_supported:
            for chunk in _chunked(provider_symbol_keys, self._snapshot_batch_size):
                if not self._snapshot_batch_supported:
                    fallback_provider_symbols.update(chunk)
                    continue
                batches_attempted += 1
                try:
                    snapshot_by_symbol.update(self._fetch_snapshot_batch(chunk))
                except _PolygonBatchSnapshotUnsupported as e:
                    batch_errors += 1
                    self._snapshot_batch_supported = False
                    fallback_provider_symbols.update(chunk)
                    _warn_nonfatal(
                        "POLYGON_LIVE_BATCH_SNAPSHOT_UNSUPPORTED",
                        e,
                        once_key="polygon_live_batch_snapshot_unsupported",
                        requested=len(chunk),
                        status_code=e.status_code,
                        reason=e.reason,
                    )
                except _PolygonBatchSnapshotEntitlementError as e:
                    batch_errors += 1
                    batch_blocked_provider_symbols.update(chunk)
                    _warn_nonfatal(
                        "POLYGON_LIVE_BATCH_SNAPSHOT_ENTITLEMENT_FAILED",
                        e,
                        once_key="polygon_live_batch_snapshot_entitlement",
                        requested=len(chunk),
                        status_code=e.status_code,
                        reason=e.reason,
                    )
                except _PolygonBatchSnapshotRateLimited as e:
                    batch_errors += 1
                    batch_blocked_provider_symbols.update(chunk)
                    _warn_nonfatal(
                        "POLYGON_LIVE_BATCH_SNAPSHOT_RATE_LIMITED",
                        e,
                        once_key="polygon_live_batch_snapshot_rate_limited",
                        requested=len(chunk),
                        status_code=e.status_code,
                        reason=e.reason,
                    )
                except Exception as e:
                    batch_errors += 1
                    batch_blocked_provider_symbols.update(chunk)
                    _warn_nonfatal(
                        "POLYGON_LIVE_BATCH_SNAPSHOT_FAILED",
                        e,
                        once_key=f"polygon_live_batch_snapshot:{','.join(chunk[:3])}",
                        requested=len(chunk),
                    )
        else:
            fallback_provider_symbols.update(provider_symbol_keys)

        for sym_s, provider_symbol_s in symbols:
            q = snapshot_by_symbol.get(provider_symbol_s)
            try:
                if not q and provider_symbol_s in fallback_provider_symbols:
                    q = self.get_latest(provider_symbol_s)
                if not q and provider_symbol_s in batch_blocked_provider_symbols:
                    skipped += 1
                    continue

                px = _finite_float_or_none((q or {}).get("px"))
                if px is None:
                    skipped += 1
                    _warn_nonfatal(
                        "POLYGON_LIVE_SKIP_NO_PRICE",
                        RuntimeError("polygon_live_skip_no_price"),
                        once_key=f"skip_no_price:{sym_s}",
                        symbol=str(sym_s),
                        provider_symbol=str(provider_symbol_s),
                    )
                    continue

                out[str(sym_s)] = {
                    "ts_ms": int(q.get("ts_ms") or int(time.time() * 1000)),
                    "price": float(px),
                    "bid": _finite_float_or_none(q.get("bid")),
                    "ask": _finite_float_or_none(q.get("ask")),
                    "spread": _finite_float_or_none(q.get("spread")),
                    "volume": _finite_float_or_none(q.get("volume")),
                    "source": str(q.get("source") or "polygon"),
                }
            except Exception as e:
                _warn_nonfatal("POLYGON_LIVE_TICKER_PARSE_FAILED", e, once_key=f"ticker:{sym_s}", symbol=str(sym_s), row=repr(q)[:200])
                continue

        LOG.info(
            "fetch_complete provider=polygon requested=%d returned=%d skipped=%d batches=%d batch_errors=%d fallback_symbols=%d blocked_symbols=%d",
            requested,
            len(out),
            skipped,
            int(batches_attempted),
            int(batch_errors),
            int(len(fallback_provider_symbols)),
            int(len(batch_blocked_provider_symbols)),
        )
        return out

    def fetch_recent_bars(self, ticker_map, since_ts_ms):
        out = {}
        end_ms = int(time.time() * 1000)
        start_ms = int(since_ts_ms or (end_ms - 5 * 60 * 1000))
        if start_ms >= end_ms:
            start_ms = max(0, end_ms - 60 * 1000)

        for sym, provider_symbol in (ticker_map or {}).items():
            try:
                r = self.session.get(
                    f"{_BASE}/v2/aggs/ticker/{str(provider_symbol).upper()}/range/1/minute/{int(start_ms)}/{int(end_ms)}",
                    params={"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": _polygon_key()},
                    timeout=8,
                )
                r.raise_for_status()
                payload = _response_json(r) or {}
                results = payload.get("results") or []
                bars = []
                for row in results:
                    try:
                        bars.append(
                            {
                                "ts_ms": int(row.get("t") or 0),
                                "open": float(row["o"]) if row.get("o") is not None else None,
                                "high": float(row["h"]) if row.get("h") is not None else None,
                                "low": float(row["l"]) if row.get("l") is not None else None,
                                "close": float(row["c"]) if row.get("c") is not None else None,
                                "volume": float(row["v"]) if row.get("v") is not None else None,
                                "vwap": float(row["vw"]) if row.get("vw") is not None else None,
                                "n": int(row.get("n") or 0),
                                "source": "polygon",
                            }
                        )
                    except Exception as e:
                        _warn_nonfatal("POLYGON_LIVE_BAR_PARSE_FAILED", e, once_key=f"bar:{sym}", symbol=str(sym), row=repr(row)[:200])
                        continue
                out[str(sym)] = bars
            except Exception:
                out[str(sym)] = []

        return out

    def _append_api_key(self, url: str) -> str:
        parsed = urlparse(str(url))
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.setdefault("apiKey", _polygon_key())
        return urlunparse(parsed._replace(query=urlencode(query)))

    def _paginate(self, url: str, params=None, max_pages: int = 8):
        out = []
        next_url = str(url)
        next_params = dict(params or {})
        pages = 0
        while next_url and pages < int(max_pages):
            pages += 1
            req_url = self._append_api_key(next_url)
            response = self.session.get(req_url, params=(next_params if pages == 1 else None), timeout=8)
            response.raise_for_status()
            payload = _response_json(response) or {}
            out.extend(list(payload.get("results") or []))
            next_url = str(payload.get("next_url") or "").strip()
            next_params = None
        return out

    def _ns(self, ts_ms):
        ts = int(ts_ms or 0)
        if ts <= 0:
            return 0
        return int(ts) * 1_000_000

    def fetch_historical_trades(self, ticker_map, since_ts_ms_by_symbol, until_ts_ms=None):
        out = {}
        until_ns = self._ns(until_ts_ms or int(time.time() * 1000))
        for sym, provider_symbol in (ticker_map or {}).items():
            sym_s = str(sym)
            start_ms = int((since_ts_ms_by_symbol or {}).get(sym_s) or 0)
            if start_ms <= 0:
                out[sym_s] = []
                continue
            params = {
                "timestamp.gte": self._ns(start_ms),
                "timestamp.lte": until_ns,
                "order": "asc",
                "sort": "timestamp",
                "limit": 50000,
            }
            try:
                rows = self._paginate(f"{_BASE}/v3/trades/{str(provider_symbol).upper()}", params=params)
            except Exception:
                rows = []
            parsed = []
            for row in rows or []:
                ts_ns = int(
                    row.get("sip_timestamp")
                    or row.get("participant_timestamp")
                    or row.get("trf_timestamp")
                    or row.get("timestamp")
                    or 0
                )
                ts_ms = int(ts_ns / 1_000_000) if ts_ns > 0 else 0
                if ts_ms <= 0:
                    continue
                parsed.append(
                    {
                        "symbol": sym_s,
                        "provider_symbol": str(provider_symbol),
                        "event_type": "T",
                        "timestamp": ts_ms,
                        "event_ts_ms": ts_ms,
                        "event_ts_ns": ts_ns,
                        "price": row.get("price", row.get("p")),
                        "size": row.get("size", row.get("s")),
                        "exchange": row.get("exchange", row.get("x")),
                        "sequence_number": row.get("sequence_number", row.get("q")),
                        "trade_id": row.get("id", row.get("i")),
                        "source": "polygon_rest_trade_replay",
                    }
                )
            out[sym_s] = parsed
        return out

    def fetch_historical_quotes(self, ticker_map, since_ts_ms_by_symbol, until_ts_ms=None):
        out = {}
        until_ns = self._ns(until_ts_ms or int(time.time() * 1000))
        for sym, provider_symbol in (ticker_map or {}).items():
            sym_s = str(sym)
            start_ms = int((since_ts_ms_by_symbol or {}).get(sym_s) or 0)
            if start_ms <= 0:
                out[sym_s] = []
                continue
            params = {
                "timestamp.gte": self._ns(start_ms),
                "timestamp.lte": until_ns,
                "order": "asc",
                "sort": "timestamp",
                "limit": 50000,
            }
            try:
                rows = self._paginate(f"{_BASE}/v3/quotes/{str(provider_symbol).upper()}", params=params)
            except Exception:
                rows = []
            parsed = []
            for row in rows or []:
                ts_ns = int(
                    row.get("sip_timestamp")
                    or row.get("participant_timestamp")
                    or row.get("trf_timestamp")
                    or row.get("timestamp")
                    or 0
                )
                ts_ms = int(ts_ns / 1_000_000) if ts_ns > 0 else 0
                if ts_ms <= 0:
                    continue
                bid = row.get("bid_price", row.get("bp"))
                ask = row.get("ask_price", row.get("ap"))
                parsed.append(
                    {
                        "symbol": sym_s,
                        "provider_symbol": str(provider_symbol),
                        "event_type": "Q",
                        "timestamp": ts_ms,
                        "event_ts_ms": ts_ms,
                        "event_ts_ns": ts_ns,
                        "bid": bid,
                        "ask": ask,
                        "bid_size": row.get("bid_size", row.get("bs")),
                        "ask_size": row.get("ask_size", row.get("as")),
                        "sequence_number": row.get("sequence_number", row.get("q")),
                        "source": "polygon_rest_quote_replay",
                    }
                )
            out[sym_s] = parsed
        return out

    def get_latest(self, symbol: str):
        sym = symbol.upper()
        # Snapshot includes quote/trade fields when entitled, avoiding separate
        # per-symbol last-trade/NBBO probes on every fallback symbol.
        try:
            rs = self.session.get(
                f"{_BASE}{_SINGLE_SNAPSHOT_PATH.format(symbol=sym)}",
                params={"apiKey": _polygon_key()},
                timeout=5,
            )
            rs.raise_for_status()
            payload = _response_json(rs) or {}
            quote = self._quote_from_snapshot_ticker(payload.get("ticker") or {})
            if quote:
                return quote
        except Exception as e:
            _warn_nonfatal(
                "POLYGON_LIVE_SNAPSHOT_FETCH_FAILED",
                e,
                once_key="polygon_live_snapshot_fetch",
                symbol=sym,
            )

        # Final fallback: previous close endpoint.
        rp = self.session.get(
            f"{_BASE}/v2/aggs/ticker/{sym}/prev",
            params={"adjusted": "true", "apiKey": _polygon_key()},
            timeout=5,
        )
        rp.raise_for_status()
        payload = _response_json(rp) or {}
        results = payload.get("results") or []
        row = results[0] if results else {}
        now_ms = int(time.time() * 1000)
        return {
            "px": row.get("c"),
            # Previous-close data is still useful off-hours; mark retrieval time as current
            # and keep the actual market bar timestamp for provenance.
            "ts_ms": now_ms,
            "market_ts_ms": int(row.get("t") or now_ms),
            "bid": None,
            "ask": None,
            "spread": None,
            "volume": row.get("v"),
            "source": "polygon_prev_close",
        }
