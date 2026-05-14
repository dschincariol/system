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
from engine.runtime.logging import get_logger

_BASE = "https://api.polygon.io"
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


def _chunked(items, size: int):
    chunk_size = max(1, int(size or 1))
    for idx in range(0, len(items), chunk_size):
        yield items[idx : idx + chunk_size]


def _polygon_key() -> str:
    return get_data_credential("POLYGON_API_KEY")


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

    def _quote_from_snapshot_ticker(self, ticker_payload):
        ticker = dict(ticker_payload or {})
        day = ticker.get("day") or {}
        minute = ticker.get("min") or {}
        prev_day = ticker.get("prevDay") or {}

        px = minute.get("c")
        if px is None:
            px = day.get("c")
        if px is None:
            px = prev_day.get("c")
        if px is None:
            return {}

        ts_ns = ticker.get("updated")
        if ts_ns is not None:
            market_ts_ms = int(int(ts_ns) / 1_000_000)
        else:
            market_ts_ms = int((minute.get("t") or day.get("t") or time.time() * 1000))

        volume = minute.get("v")
        if volume is None:
            volume = day.get("v")

        now_ms = int(time.time() * 1000)
        return {
            "px": px,
            "ts_ms": now_ms,
            "market_ts_ms": market_ts_ms,
            "bid": None,
            "ask": None,
            "spread": None,
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
            f"{_BASE}/v2/snapshot/locale/us/markets/stocks/tickers",
            params={"tickers": ",".join(symbols), "apiKey": api_key},
            timeout=self._snapshot_timeout_s,
        )
        response.raise_for_status()
        payload = response.json() or {}
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
        for chunk in _chunked(sorted(reverse_map.keys()), self._snapshot_batch_size):
            try:
                snapshot_by_symbol.update(self._fetch_snapshot_batch(chunk))
            except Exception as e:
                batch_errors += 1
                _warn_nonfatal(
                    "POLYGON_LIVE_BATCH_SNAPSHOT_FAILED",
                    e,
                    once_key=f"polygon_live_batch_snapshot:{','.join(chunk[:3])}",
                    requested=len(chunk),
                )

        for sym_s, provider_symbol_s in symbols:
            q = snapshot_by_symbol.get(provider_symbol_s)
            try:
                if not q:
                    q = self.get_latest(provider_symbol_s)

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
            "fetch_complete provider=polygon requested=%d returned=%d skipped=%d batches=%d batch_errors=%d",
            requested,
            len(out),
            skipped,
            int(math.ceil(len(reverse_map) / float(max(1, self._snapshot_batch_size)))) if reverse_map else 0,
            int(batch_errors),
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
                payload = r.json() or {}
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
            payload = response.json() or {}
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
        # Try last trade/NBBO first for plans that include that entitlement.
        try:
            r = self.session.get(
                f"{_BASE}/v2/last/trade/{sym}",
                params={"apiKey": _polygon_key()},
                timeout=5,
            )
            r.raise_for_status()
            j = r.json()
            t = j.get("results", {}) or {}

            px = t.get("p")
            ts_ms = t.get("t")

            bid = ask = spread = None
            try:
                rq = self.session.get(
                    f"{_BASE}/v2/last/nbbo/{sym}",
                    params={"apiKey": _polygon_key()},
                    timeout=5,
                )
                rq.raise_for_status()
                q = rq.json().get("results", {}) or {}

                bid = q.get("bid_price")
                ask = q.get("ask_price")
                if bid is not None and ask is not None:
                    spread = float(ask) - float(bid)
            except Exception as e:
                _warn_nonfatal(
                    "POLYGON_LIVE_NBBO_FETCH_FAILED",
                    e,
                    once_key="polygon_live_nbbo_fetch",
                    symbol=sym,
                )

            return {
                "px": px,
                "ts_ms": ts_ms or int(time.time() * 1000),
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "volume": t.get("s"),
                "source": "polygon",
            }
        except Exception as e:
            _warn_nonfatal(
                "POLYGON_LIVE_LAST_TRADE_FETCH_FAILED",
                e,
                once_key="polygon_live_last_trade_fetch",
                symbol=sym,
            )

        # Fallback for plans that have snapshot/aggregate access but not last-trade/NBBO access.
        try:
            rs = self.session.get(
                f"{_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{sym}",
                params={"apiKey": _polygon_key()},
                timeout=5,
            )
            rs.raise_for_status()
            payload = rs.json() or {}
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
        payload = rp.json() or {}
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
