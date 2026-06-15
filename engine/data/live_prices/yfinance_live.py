"""
FILE: yfinance_live.py

Live price feed integration for `yfinance_live`.
"""

# dev_core/live_prices/yfinance_live.py
import math
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Dict
import requests

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

try:
    import yfinance as yf
    _YFINANCE_IMPORT_ERROR = None
except Exception as _yfinance_import_error:
    yf = None  # type: ignore
    _YFINANCE_IMPORT_ERROR = _yfinance_import_error

_THREAD_LOCAL = threading.local()
LOG = get_logger("engine.data.live_prices.yfinance_live")
_WARNED_NONFATAL_KEYS: set[str] = set()
_YFINANCE_TIMEOUT_S = float(os.environ.get("YFINANCE_TIMEOUT_S", "15"))
_YFINANCE_BATCH_TIMEOUT_S = max(0.25, float(os.environ.get("YFINANCE_LIVE_BATCH_TIMEOUT_S", "6")))
_YFINANCE_BATCH_CURSOR = 0
_YFINANCE_BATCH_LOCK = threading.Lock()


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
        component="engine.data.live_prices.yfinance_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _log_partial_fetch_timeout(*, configured: int, requested: int, returned: int, pending: int, batch_timeout_s: float) -> None:
    LOG.log(
        logging.WARNING,
        "yfinance_live_fetch_partial configured=%d requested=%d returned=%d pending=%d batch_timeout_s=%.3f",
        int(configured),
        int(requested),
        int(returned),
        int(pending),
        float(batch_timeout_s),
        extra={
            "event": "yfinance_live_fetch_partial",
            "configured": int(configured),
            "requested": int(requested),
            "returned": int(returned),
            "pending": int(pending),
            "batch_timeout_s": float(batch_timeout_s),
        },
    )


def _normalize_ticker(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    while text.startswith("$"):
        text = text[1:]
    return text


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


def _get_http_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if isinstance(session, requests.Session):
        return session
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    _THREAD_LOCAL.session = session
    return session


def _remaining_budget_s(deadline_monotonic: float | None) -> float | None:
    if deadline_monotonic is None:
        return None
    return max(0.0, float(deadline_monotonic - time.monotonic()))


def _bounded_request_timeout_s(deadline_monotonic: float | None) -> float:
    remaining_s = _remaining_budget_s(deadline_monotonic)
    if remaining_s is None:
        return float(_YFINANCE_TIMEOUT_S)
    if remaining_s <= 0.0:
        return 0.0
    return max(0.25, min(float(_YFINANCE_TIMEOUT_S), float(remaining_s)))


def _fetch_chart_json(
    symbol: str,
    *,
    interval: str,
    range_: str,
    deadline_monotonic: float | None = None,
) -> dict | None:
    symbol = _normalize_ticker(symbol)
    if not symbol:
        return None
    try:
        timeout_s = _bounded_request_timeout_s(deadline_monotonic)
        if timeout_s <= 0.0:
            return None
        r = _get_http_session().get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
            params={
                "interval": str(interval),
                "range": str(range_),
                "includePrePost": "false",
                "events": "div,splits",
            },
            timeout=float(timeout_s),
        )
        r.raise_for_status()
        payload = r.json() or {}
        result = (payload.get("chart") or {}).get("result") or []
        return result[0] if result else None
    except Exception as e:
        _warn_nonfatal("YFINANCE_CHART_FETCH_FAILED", e, once_key="chart_fetch_failed")
        return None


def _extract_latest_from_chart(chart: dict | None) -> tuple[float | None, float | None]:
    if not isinstance(chart, dict):
        return None, None

    px = None
    vol = None

    try:
        meta = chart.get("meta") or {}
        regular = meta.get("regularMarketPrice")
        px = _finite_float_or_none(regular)
    except Exception:
        px = None

    try:
        quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0] or {}
        closes = quote.get("close") or []
        volumes = quote.get("volume") or []

        for val in reversed(closes):
            parsed = _finite_float_or_none(val)
            if parsed is not None:
                px = float(parsed)
                break

        for val in reversed(volumes):
            parsed = _finite_float_or_none(val)
            if parsed is not None:
                vol = float(parsed)
                break
    except Exception as e:
        _warn_nonfatal(
            "YFINANCE_LIVE_CHART_PARSE_FAILED",
            e,
            once_key="yfinance_live_chart_parse",
        )

    return px, vol


def _priority_tickers() -> set[str]:
    raw = str(
        os.environ.get(
            "YFINANCE_LIVE_PRIORITY_TICKERS",
            "SPY,QQQ,IWM,DIA,^VIX,^TNX,^FVX,TLT,IEF,SHY,HYG,LQD,GLD,XLK,XLF,XLE,XLV",
        )
        or ""
    )
    return {
        _normalize_ticker(part)
        for part in raw.split(",")
        if _normalize_ticker(part)
    }


def _select_batch_items(ticker_map: Dict[str, str]) -> list[tuple[str, str]]:
    global _YFINANCE_BATCH_CURSOR

    items = [
        (str(sym), _normalize_ticker(tkr))
        for sym, tkr in (ticker_map or {}).items()
        if str(sym).strip() and _normalize_ticker(tkr)
    ]
    if not items:
        return []

    max_batch = max(1, int(os.environ.get("YFINANCE_LIVE_BATCH_SIZE", "64")))
    if len(items) <= max_batch:
        return items

    priority = _priority_tickers()
    priority_items = [
        (sym, ticker)
        for sym, ticker in items
        if _normalize_ticker(sym) in priority or ticker in priority
    ]
    priority_seen = {(sym, ticker) for sym, ticker in priority_items}
    non_priority_items = [
        (sym, ticker)
        for sym, ticker in items
        if (sym, ticker) not in priority_seen
    ]

    selected = list(priority_items[:max_batch])
    remaining = max(0, max_batch - len(selected))
    if remaining <= 0 or not non_priority_items:
        return selected

    with _YFINANCE_BATCH_LOCK:
        start = int(_YFINANCE_BATCH_CURSOR % len(non_priority_items))
        rotated = non_priority_items[start:] + non_priority_items[:start]
        selected.extend(rotated[:remaining])
        _YFINANCE_BATCH_CURSOR = int((start + remaining) % len(non_priority_items))

    return selected


def _fetch_symbol_last_price(
    sym: str,
    ticker_symbol: str,
    now_ms: int,
    deadline_monotonic: float | None = None,
) -> tuple[str, dict] | None:
    px = None
    vol = None

    remaining_s = _remaining_budget_s(deadline_monotonic)
    if remaining_s is not None and remaining_s <= 0.0:
        return None

    chart = _fetch_chart_json(
        ticker_symbol,
        interval="1m",
        range_="5d",
        deadline_monotonic=deadline_monotonic,
    )
    px, vol = _extract_latest_from_chart(chart)

    if px is None and yf is not None and deadline_monotonic is None:
        try:
            ticker = yf.Ticker(ticker_symbol)
            fast = getattr(ticker, "fast_info", None) or {}
            px = _finite_float_or_none(fast.get("lastPrice"))
            vol = _finite_float_or_none(fast.get("lastVolume"))
        except Exception as e:
            _warn_nonfatal(
                "YFINANCE_LIVE_FAST_INFO_FETCH_FAILED",
                e,
                once_key=f"yfinance_live_fast_info_fetch:{ticker_symbol}",
                ticker_symbol=ticker_symbol,
            )

    if px is None:
        _warn_nonfatal(
            "YFINANCE_LIVE_SKIP_NO_PRICE",
            RuntimeError("yfinance_live_skip_no_price"),
            once_key=f"skip_no_price:{sym}",
            symbol=str(sym),
            ticker_symbol=ticker_symbol,
        )
        return None

    return str(sym), {
        "ts_ms": int(now_ms),
        "price": float(px),
        "bid": None,
        "ask": None,
        "spread": None,
        "volume": (float(vol) if vol is not None else None),
        "source": "yfinance",
    }

def fetch_last_prices_yf(ticker_map: Dict[str, str]) -> Dict[str, dict]:
    """
    Contract-compatible with poll_prices.py
    Returns:
      { "SPY": {"ts_ms":..., "price":..., "source":"yfinance"}, ... }
    """
    out: Dict[str, dict] = {}
    if not ticker_map:
        return out

    now_ms = int(time.time() * 1000)
    selected_items = _select_batch_items(ticker_map)
    if not selected_items:
        return out

    requested = int(len(selected_items))
    configured = int(len(ticker_map or {}))
    skipped = 0

    max_workers = max(1, min(requested, int(os.environ.get("YFINANCE_LIVE_MAX_WORKERS", "12"))))
    batch_timeout_s = max(0.25, float(os.environ.get("YFINANCE_LIVE_BATCH_TIMEOUT_S", str(_YFINANCE_BATCH_TIMEOUT_S))))
    deadline_monotonic = float(time.monotonic() + batch_timeout_s)
    pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="yf-live")
    pending = 0
    try:
        futures = {
            pool.submit(_fetch_symbol_last_price, sym, ticker_symbol, now_ms, deadline_monotonic): (sym, ticker_symbol)
            for sym, ticker_symbol in selected_items
        }
        done, not_done = wait(tuple(futures.keys()), timeout=float(batch_timeout_s))
        pending = int(len(not_done))
        for future in not_done:
            future.cancel()
        for future in done:
            sym, ticker_symbol = futures[future]
            try:
                result = future.result()
            except Exception as e:
                skipped += 1
                _warn_nonfatal(
                    "YFINANCE_LIVE_WORKER_FAILED",
                    e,
                    once_key=f"worker_failed:{sym}",
                    symbol=str(sym),
                    ticker_symbol=str(ticker_symbol),
                )
                continue
            if result is None:
                skipped += 1
                continue
            out[str(result[0])] = dict(result[1])
        skipped += pending
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    if pending:
        _log_partial_fetch_timeout(
            configured=int(configured),
            requested=int(requested),
            returned=int(len(out)),
            pending=int(pending),
            batch_timeout_s=float(batch_timeout_s),
        )

    LOG.info(
        "fetch_complete provider=yfinance configured=%d requested=%d returned=%d skipped=%d",
        configured,
        requested,
        len(out),
        skipped,
    )
    return out

class YFinancePriceProvider:
    """
    Provider used by poll_prices.py.

    Contract:
      fetch_last_prices(ticker_map) -> { "SPY": {"ts_ms":..., "price":...}, ... }
    """

    def fetch_last_prices(self, ticker_map: Dict[str, str]) -> Dict[str, dict]:
        return fetch_last_prices_yf(ticker_map)


def fetch_latest_ohlcv_yf(ticker_map: Dict[str, str], interval: str = "1m") -> Dict[str, dict]:
    """
    Fetch latest OHLCV bar for each ticker via yfinance history.
    Returns:
      { "SPY": {"ts_ms":..., "o":..,"h":..,"l":..,"c":..,"v":.., "tf_s":60}, ... }
    """
    out: Dict[str, dict] = {}
    if yf is None:
        return out
    tf_s = 60 if interval == "1m" else 300 if interval == "5m" else 60

    for sym, tkr in (ticker_map or {}).items():
        ticker_symbol = _normalize_ticker(tkr)
        if not ticker_symbol:
            continue
        chart = _fetch_chart_json(ticker_symbol, interval=interval, range_="5d")
        if isinstance(chart, dict):
            try:
                timestamps = chart.get("timestamp") or []
                quote = ((chart.get("indicators") or {}).get("quote") or [{}])[0] or {}
                opens = quote.get("open") or []
                highs = quote.get("high") or []
                lows = quote.get("low") or []
                closes = quote.get("close") or []
                volumes = quote.get("volume") or []

                for i in range(len(timestamps) - 1, -1, -1):
                    close_v = _finite_float_or_none(closes[i] if i < len(closes) else None)
                    if close_v is None:
                        continue
                    open_v = _finite_float_or_none(opens[i] if i < len(opens) else None)
                    high_v = _finite_float_or_none(highs[i] if i < len(highs) else None)
                    low_v = _finite_float_or_none(lows[i] if i < len(lows) else None)
                    out[str(sym)] = {
                        "ts_ms": int(float(timestamps[i]) * 1000.0),
                        "tf_s": int(tf_s),
                        "o": float(open_v if open_v is not None else close_v),
                        "h": float(high_v if high_v is not None else close_v),
                        "l": float(low_v if low_v is not None else close_v),
                        "c": float(close_v),
                        "v": _finite_float_or_none(volumes[i] if i < len(volumes) else None),
                    }
                    break
                if str(sym) in out:
                    continue
            except Exception as e:
                _warn_nonfatal(
                    "YFINANCE_LIVE_OHLCV_PARSE_FAILED",
                    e,
                    once_key="yfinance_live_ohlcv_parse",
                    ticker_symbol=ticker_symbol,
                    interval=str(interval),
                )

    return out
