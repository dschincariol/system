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
from typing import Any, Dict, cast
import requests
from requests.adapters import HTTPAdapter

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter, emit_gauge

try:
    import yfinance as yf
    _YFINANCE_IMPORT_ERROR = None
except Exception as _yfinance_import_error:
    yf = None  # type: ignore
    _YFINANCE_IMPORT_ERROR = _yfinance_import_error

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
        out = float(cast(Any, value))
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


_HTTP_SESSION: requests.Session | None = None
_HTTP_SESSION_LOCK = threading.Lock()
_HTTP_SESSION_POOL_SIZE: int | None = None
_YFINANCE_EXECUTOR: ThreadPoolExecutor | None = None
_YFINANCE_EXECUTOR_MAX_WORKERS: int | None = None
_YFINANCE_EXECUTOR_LOCK = threading.Lock()


def _configured_executor_max_workers() -> int:
    raw = os.environ.get("YFINANCE_LIVE_MAX_WORKERS", "12")
    try:
        parsed = int(float(str(raw).strip()))
    except Exception:
        parsed = 12
    return max(1, int(parsed))


def _configured_http_pool_size() -> int:
    return max(32, _configured_executor_max_workers())


def _get_http_session() -> requests.Session:
    global _HTTP_SESSION, _HTTP_SESSION_POOL_SIZE

    session = _HTTP_SESSION
    if isinstance(session, requests.Session):
        return session
    with _HTTP_SESSION_LOCK:
        session = _HTTP_SESSION
        if isinstance(session, requests.Session):
            return session
        session = requests.Session()
        session.trust_env = False
        session.headers.update({"User-Agent": "Mozilla/5.0"})
        pool_size = _configured_http_pool_size()
        try:
            adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        except Exception as e:
            _warn_nonfatal("YFINANCE_SESSION_POOL_CONFIG_FAILED", e, once_key="yfinance_session_pool_config")
        _HTTP_SESSION = session
        _HTTP_SESSION_POOL_SIZE = int(pool_size)
        return session


def _get_yfinance_executor(max_workers: int | None = None) -> ThreadPoolExecutor:
    global _YFINANCE_EXECUTOR, _YFINANCE_EXECUTOR_MAX_WORKERS

    configured_workers = _configured_executor_max_workers()
    desired_workers = int(max_workers) if max_workers is not None else configured_workers
    desired_workers = max(1, min(desired_workers, configured_workers))
    executor = _YFINANCE_EXECUTOR
    if executor is not None and int(_YFINANCE_EXECUTOR_MAX_WORKERS or 0) >= desired_workers:
        return executor

    with _YFINANCE_EXECUTOR_LOCK:
        executor = _YFINANCE_EXECUTOR
        if executor is not None and int(_YFINANCE_EXECUTOR_MAX_WORKERS or 0) >= desired_workers:
            return executor
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        _YFINANCE_EXECUTOR = ThreadPoolExecutor(max_workers=desired_workers, thread_name_prefix="yf-live")
        _YFINANCE_EXECUTOR_MAX_WORKERS = int(desired_workers)
        return _YFINANCE_EXECUTOR


def shutdown_yfinance_resources(*, wait: bool = True, cancel_futures: bool = True) -> None:
    global _HTTP_SESSION, _HTTP_SESSION_POOL_SIZE, _YFINANCE_EXECUTOR, _YFINANCE_EXECUTOR_MAX_WORKERS

    with _YFINANCE_EXECUTOR_LOCK:
        executor = _YFINANCE_EXECUTOR
        _YFINANCE_EXECUTOR = None
        _YFINANCE_EXECUTOR_MAX_WORKERS = None
    if executor is not None:
        executor.shutdown(wait=bool(wait), cancel_futures=bool(cancel_futures))

    with _HTTP_SESSION_LOCK:
        session = _HTTP_SESSION
        _HTTP_SESSION = None
        _HTTP_SESSION_POOL_SIZE = None
    if session is not None:
        session.close()


def reset_yfinance_resources_for_tests() -> None:
    global _YFINANCE_BATCH_CURSOR

    shutdown_yfinance_resources(wait=True, cancel_futures=True)
    with _YFINANCE_BATCH_LOCK:
        _YFINANCE_BATCH_CURSOR = 0


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
    return max(0.001, min(float(_YFINANCE_TIMEOUT_S), float(remaining_s)))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_positive_int(names: tuple[str, ...], default: int | None = None) -> int | None:
    for name in names:
        raw = os.environ.get(str(name))
        if raw is None or str(raw).strip() == "":
            continue
        try:
            parsed = int(float(str(raw).strip()))
        except Exception:
            parsed = 0
        return int(parsed) if parsed > 0 else None
    return default


def _yf_download_batch_enabled() -> bool:
    return bool(
        yf is not None
        and hasattr(yf, "download")
        and _env_bool("YFINANCE_LIVE_BATCH_ENABLED", True)
    )


def _configured_symbol_limit(*, batch_path_active: bool) -> int | None:
    if batch_path_active:
        return None
    return _env_positive_int(
        ("YFINANCE_LIVE_FALLBACK_SYMBOL_LIMIT", "YFINANCE_LIVE_BATCH_SIZE"),
        default=64,
    )


def _configured_batch_chunk_size() -> int:
    return max(
        1,
        int(
            _env_positive_int(
                ("YFINANCE_LIVE_BATCH_CHUNK_SIZE", "YFINANCE_LIVE_BATCH_SIZE"),
                default=128,
            )
            or 128
        ),
    )


def _eligible_ticker_items(ticker_map: Dict[str, str]) -> list[tuple[str, str]]:
    return [
        (str(sym), _normalize_ticker(tkr))
        for sym, tkr in (ticker_map or {}).items()
        if str(sym).strip() and _normalize_ticker(tkr)
    ]


def _iter_chunks(items: list[str], chunk_size: int) -> list[list[str]]:
    size = max(1, int(chunk_size))
    return [items[i : i + size] for i in range(0, len(items), size)]


def _log_partial_batch_failure(
    *,
    configured: int,
    requested: int,
    returned: int,
    missing: int,
    timed_out: bool,
    chunk_size: int,
    missing_symbols: list[str],
) -> None:
    LOG.log(
        logging.WARNING,
        "yfinance_download_batch_partial configured=%d requested=%d returned=%d missing=%d timed_out=%s chunk_size=%d missing_symbols=%s",
        int(configured),
        int(requested),
        int(returned),
        int(missing),
        bool(timed_out),
        int(chunk_size),
        ",".join(str(sym) for sym in missing_symbols[:20]),
        extra={
            "event": "yfinance_download_batch_partial",
            "configured": int(configured),
            "requested": int(requested),
            "returned": int(returned),
            "missing": int(missing),
            "timed_out": bool(timed_out),
            "chunk_size": int(chunk_size),
            "missing_symbols": list(missing_symbols[:20]),
        },
    )


def _emit_symbol_limit_metrics(
    *,
    skipped: int,
    limit: int | None,
    batch_path_active: bool,
) -> None:
    if skipped <= 0:
        return
    tags = {
        "batch_active": bool(batch_path_active),
        "degraded": True,
        "reason": "configured_fallback_symbol_limit",
    }
    if limit is not None:
        tags["limit"] = int(limit)
    emit_counter(
        "yfinance_live_symbol_limit_skipped",
        int(skipped),
        component="engine.data.live_prices.yfinance_live",
        provider="yfinance",
        extra_tags=tags,
    )
    emit_gauge(
        "yfinance_live_degraded",
        1,
        component="engine.data.live_prices.yfinance_live",
        provider="yfinance",
        extra_tags=tags,
    )


def _log_symbol_selection_limited(
    *,
    configured: int,
    requested: int,
    limit: int | None,
    batch_path_active: bool,
    skipped_symbols: list[str],
) -> None:
    skipped = int(len(skipped_symbols))
    if skipped <= 0:
        return
    LOG.log(
        logging.WARNING,
        "yfinance_live_symbol_selection_limited configured=%d requested=%d skipped=%d limit=%s batch_active=%s degraded=%s skipped_symbols=%s",
        int(configured),
        int(requested),
        int(skipped),
        "" if limit is None else str(int(limit)),
        bool(batch_path_active),
        True,
        ",".join(str(sym) for sym in skipped_symbols),
        extra={
            "event": "yfinance_live_symbol_selection_limited",
            "configured": int(configured),
            "requested": int(requested),
            "skipped": int(skipped),
            "limit": None if limit is None else int(limit),
            "batch_active": bool(batch_path_active),
            "degraded": True,
            "degraded_reason": "configured_fallback_symbol_limit",
            "skipped_symbols": list(skipped_symbols),
        },
    )
    _emit_symbol_limit_metrics(
        skipped=int(skipped),
        limit=limit,
        batch_path_active=bool(batch_path_active),
    )


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


def _field_matches(value: object, field: str) -> bool:
    return str(value or "").strip().lower().replace("_", " ") == str(field or "").strip().lower().replace("_", " ")


def _download_field_series(frame: Any, ticker_symbol: str, field: str) -> Any | None:
    columns = getattr(frame, "columns", None)
    if columns is None:
        return None

    ticker = _normalize_ticker(ticker_symbol)
    try:
        column_items = list(columns)
    except Exception:
        column_items = []

    for col in column_items:
        parts = col if isinstance(col, tuple) else (col,)
        if len(parts) < 2:
            continue
        first = parts[0]
        second = parts[1]
        try:
            if _field_matches(first, field) and _normalize_ticker(second) == ticker:
                return frame[col]
            if _normalize_ticker(first) == ticker and _field_matches(second, field):
                return frame[col]
        except Exception:
            continue

    for col in column_items:
        if isinstance(col, tuple):
            continue
        if not _field_matches(col, field):
            continue
        try:
            candidate = frame[col]
            if int(getattr(candidate, "ndim", 1) or 1) == 1:
                return candidate
        except Exception:
            continue
    return None


def _latest_finite_from_series(series: Any) -> float | None:
    if series is None:
        return None
    parsed = _finite_float_or_none(series)
    if parsed is not None:
        return parsed

    values: list[Any] = []
    try:
        clean = series.dropna()
        if bool(getattr(clean, "empty", False)):
            return None
        values = list(clean)
    except Exception:
        try:
            values = list(series)
        except Exception:
            values = []

    for value in reversed(values):
        parsed = _finite_float_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _extract_latest_from_download_frame(frame: Any, ticker_symbol: str) -> tuple[float | None, float | None]:
    close_series = _download_field_series(frame, ticker_symbol, "Close")
    px = _latest_finite_from_series(close_series)
    if px is None:
        adj_close_series = _download_field_series(frame, ticker_symbol, "Adj Close")
        px = _latest_finite_from_series(adj_close_series)

    volume_series = _download_field_series(frame, ticker_symbol, "Volume")
    vol = _latest_finite_from_series(volume_series)
    return px, vol


def _download_yf_frame(
    ticker_symbols: list[str],
    *,
    timeout_s: float,
    session: requests.Session | None = None,
) -> Any | None:
    if yf is None:
        return None
    kwargs: dict[str, Any] = {
        "tickers": list(ticker_symbols),
        "period": "1d",
        "interval": "1m",
        "threads": False,
        "progress": False,
        "auto_adjust": False,
    }
    if timeout_s > 0.0:
        kwargs["timeout"] = float(timeout_s)
    if session is not None:
        kwargs["session"] = session

    while True:
        try:
            return yf.download(**kwargs)
        except TypeError as e:
            message = str(e).lower()
            changed = False
            if "timeout" in kwargs and "timeout" in message:
                kwargs.pop("timeout", None)
                changed = True
            if "session" in kwargs and "session" in message:
                kwargs.pop("session", None)
                changed = True
            if not changed:
                raise
        except Exception as e:
            if "session" in kwargs and "session" in str(e).lower():
                kwargs.pop("session", None)
                continue
            raise


def _fetch_yf_download_batch_prices(
    selected_items: list[tuple[str, str]],
    *,
    now_ms: int,
    deadline_monotonic: float,
) -> tuple[Dict[str, dict], bool, bool]:
    out: Dict[str, dict] = {}
    if not selected_items or not _yf_download_batch_enabled():
        return out, False, False

    ticker_symbols: list[str] = []
    seen: set[str] = set()
    for _sym, ticker_symbol in selected_items:
        ticker = _normalize_ticker(ticker_symbol)
        if ticker and ticker not in seen:
            ticker_symbols.append(ticker)
            seen.add(ticker)
    if not ticker_symbols:
        return out, False, False

    chunk_size = _configured_batch_chunk_size()
    chunks = _iter_chunks(ticker_symbols, chunk_size)
    items_by_ticker: dict[str, list[tuple[str, str]]] = {}
    for sym, ticker_symbol in selected_items:
        items_by_ticker.setdefault(_normalize_ticker(ticker_symbol), []).append((str(sym), ticker_symbol))

    if _bounded_request_timeout_s(deadline_monotonic) <= 0.0:
        return out, True, True

    session = _get_http_session()
    pool = _get_yfinance_executor()
    for chunk_index, chunk_tickers in enumerate(chunks):
        timeout_s = _bounded_request_timeout_s(deadline_monotonic)
        if timeout_s <= 0.0:
            return out, True, True

        future = pool.submit(
            _download_yf_frame,
            list(chunk_tickers),
            timeout_s=float(timeout_s),
            session=session,
        )
        done, not_done = wait((future,), timeout=float(timeout_s))
        if not_done:
            future.cancel()
            _warn_nonfatal(
                "YFINANCE_DOWNLOAD_BATCH_CHUNK_TIMEOUT",
                TimeoutError("yfinance_download_batch_chunk_timeout"),
                requested_symbols=len(chunk_tickers),
                chunk_index=int(chunk_index),
                chunk_count=int(len(chunks)),
            )
            return out, True, True

        try:
            frame = next(iter(done)).result()
        except Exception as e:
            _warn_nonfatal(
                "YFINANCE_DOWNLOAD_BATCH_CHUNK_FAILED",
                e,
                requested_symbols=len(chunk_tickers),
                chunk_index=int(chunk_index),
                chunk_count=int(len(chunks)),
            )
            continue

        if frame is None or bool(getattr(frame, "empty", False)):
            _warn_nonfatal(
                "YFINANCE_DOWNLOAD_BATCH_CHUNK_EMPTY",
                RuntimeError("yfinance_download_batch_chunk_empty"),
                requested_symbols=len(chunk_tickers),
                chunk_index=int(chunk_index),
                chunk_count=int(len(chunks)),
            )
            continue

        for ticker in chunk_tickers:
            for sym, ticker_symbol in items_by_ticker.get(ticker, []):
                try:
                    px, vol = _extract_latest_from_download_frame(frame, ticker_symbol)
                except Exception as e:
                    _warn_nonfatal(
                        "YFINANCE_DOWNLOAD_BATCH_PARSE_FAILED",
                        e,
                        once_key="yfinance_download_batch_parse",
                        ticker_symbol=str(ticker_symbol),
                    )
                    continue
                if px is None:
                    continue
                out[str(sym)] = {
                    "ts_ms": int(now_ms),
                    "price": float(px),
                    "bid": None,
                    "ask": None,
                    "spread": None,
                    "volume": (float(vol) if vol is not None else None),
                    "source": "yfinance",
                }

    return out, False, True


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


def _select_batch_items(ticker_map: Dict[str, str], *, batch_path_active: bool = False) -> list[tuple[str, str]]:
    global _YFINANCE_BATCH_CURSOR

    items = _eligible_ticker_items(ticker_map)
    if not items:
        return []

    max_batch = _configured_symbol_limit(batch_path_active=bool(batch_path_active))
    if max_batch is None or len(items) <= max_batch:
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


def _skipped_symbols_for_selection(ticker_map: Dict[str, str], selected_items: list[tuple[str, str]]) -> list[str]:
    selected_symbols = {str(sym) for sym, _ticker in selected_items}
    return [
        str(sym)
        for sym, _ticker in _eligible_ticker_items(ticker_map)
        if str(sym) not in selected_symbols
    ]


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
        range_="1d",
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


def _fetch_chart_fallback_prices(
    selected_items: list[tuple[str, str]],
    *,
    now_ms: int,
    deadline_monotonic: float,
) -> tuple[Dict[str, dict], int, int]:
    out: Dict[str, dict] = {}
    if not selected_items:
        return out, 0, 0

    remaining_s = _remaining_budget_s(deadline_monotonic)
    if remaining_s is not None and remaining_s <= 0.0:
        return out, int(len(selected_items)), int(len(selected_items))

    requested = int(len(selected_items))
    max_workers = max(1, min(requested, _configured_executor_max_workers()))
    timeout_s = remaining_s
    if timeout_s is None:
        timeout_s = max(0.25, float(os.environ.get("YFINANCE_LIVE_BATCH_TIMEOUT_S", str(_YFINANCE_BATCH_TIMEOUT_S))))
    timeout_s = max(0.0, float(timeout_s))
    if timeout_s <= 0.0:
        return out, requested, requested

    skipped = 0
    pending = 0
    pool = _get_yfinance_executor(max_workers=max_workers)
    futures = {
        pool.submit(_fetch_symbol_last_price, sym, ticker_symbol, now_ms, deadline_monotonic): (sym, ticker_symbol)
        for sym, ticker_symbol in selected_items
    }
    done, not_done = wait(tuple(futures.keys()), timeout=float(timeout_s))
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

    return out, int(skipped), int(pending)


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
    batch_path_active = _yf_download_batch_enabled()
    selected_items = _select_batch_items(ticker_map, batch_path_active=bool(batch_path_active))
    if not selected_items:
        return out

    symbol_limit = _configured_symbol_limit(batch_path_active=bool(batch_path_active))
    skipped_symbols = _skipped_symbols_for_selection(ticker_map, selected_items)
    requested = int(len(selected_items))
    configured = int(requested + len(skipped_symbols))
    skipped = 0
    degraded_reasons: list[str] = []
    if skipped_symbols:
        skipped += int(len(skipped_symbols))
        degraded_reasons.append("configured_fallback_symbol_limit")
        _log_symbol_selection_limited(
            configured=int(configured),
            requested=int(requested),
            limit=symbol_limit,
            batch_path_active=bool(batch_path_active),
            skipped_symbols=list(skipped_symbols),
        )

    batch_timeout_s = max(0.25, float(os.environ.get("YFINANCE_LIVE_BATCH_TIMEOUT_S", str(_YFINANCE_BATCH_TIMEOUT_S))))
    deadline_monotonic = float(time.monotonic() + batch_timeout_s)
    pending = 0
    batch_attempted = False
    batch_timed_out = False

    if batch_path_active:
        batch_out, batch_timed_out, batch_attempted = _fetch_yf_download_batch_prices(
            selected_items,
            now_ms=int(now_ms),
            deadline_monotonic=float(deadline_monotonic),
        )
        out.update(batch_out)

    missing_items = [(sym, ticker_symbol) for sym, ticker_symbol in selected_items if str(sym) not in out]
    if batch_attempted and missing_items:
        _log_partial_batch_failure(
            configured=int(configured),
            requested=int(requested),
            returned=int(len(out)),
            missing=int(len(missing_items)),
            timed_out=bool(batch_timed_out),
            chunk_size=int(_configured_batch_chunk_size()),
            missing_symbols=[str(sym) for sym, _ticker_symbol in missing_items],
        )
    if missing_items:
        remaining_s = _remaining_budget_s(deadline_monotonic)
        if batch_timed_out or (remaining_s is not None and remaining_s <= 0.0):
            skipped += int(len(missing_items))
            pending += int(len(missing_items))
        else:
            fallback_out, fallback_skipped, fallback_pending = _fetch_chart_fallback_prices(
                missing_items,
                now_ms=int(now_ms),
                deadline_monotonic=float(deadline_monotonic),
            )
            out.update(fallback_out)
            skipped += int(fallback_skipped)
            pending += int(fallback_pending)

    if pending:
        degraded_reasons.append("fetch_timeout")
        _log_partial_fetch_timeout(
            configured=int(configured),
            requested=int(requested),
            returned=int(len(out)),
            pending=int(pending),
            batch_timeout_s=float(batch_timeout_s),
        )

    degraded_reasons = list(dict.fromkeys(degraded_reasons))
    LOG.info(
        "fetch_complete provider=yfinance configured=%d requested=%d returned=%d skipped=%d batch_active=%s batch_attempted=%s degraded=%s degraded_reasons=%s",
        configured,
        requested,
        len(out),
        skipped,
        bool(batch_path_active),
        bool(batch_attempted),
        bool(degraded_reasons),
        ",".join(degraded_reasons),
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
        chart = _fetch_chart_json(ticker_symbol, interval=interval, range_="1d")
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
