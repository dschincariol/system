"""
FILE: ccxt_live.py

Live price feed integration for `ccxt_live`.
"""

# dev_core/live_prices/ccxt_live.py
import json
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Mapping, cast

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter, emit_timing

try:
    import ccxt
    _CCXT_IMPORT_ERROR = None
except Exception as _ccxt_import_error:
    ccxt = None  # type: ignore
    _CCXT_IMPORT_ERROR = _ccxt_import_error

_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("data.live_prices.ccxt_live")
_METRIC_COMPONENT = "engine.data.live_prices.ccxt_live"
_EXCHANGE_CACHE_LOCK = threading.RLock()
_EXCHANGE_CACHE: Dict[str, "_CachedExchange"] = {}
_MAX_FAILURE_TELEMETRY_EVENTS = 8


@dataclass
class _CachedExchange:
    exchange_id: str
    cache_key: str
    exchange: Any
    created_monotonic: float = field(default_factory=time.monotonic)
    last_used_monotonic: float = field(default_factory=time.monotonic)
    markets_loaded: bool = False
    lock: Any = field(default_factory=threading.RLock, repr=False)


@dataclass(frozen=True)
class _BatchTickerResult:
    tickers: Dict[str, dict]
    used: bool
    stop: bool
    unsupported: bool = False
    failed: bool = False
    failure_reason: str = ""


class _FailureTelemetryBudget:
    def __init__(self, *, limit: int = _MAX_FAILURE_TELEMETRY_EVENTS) -> None:
        self.limit = max(1, int(limit))
        self.emitted = 0
        self.suppressed = 0

    def warn(self, code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
        if self.emitted < self.limit:
            self.emitted += 1
            _warn_nonfatal(code, error, once_key=once_key, **extra)
            return
        self.suppressed += 1

    def flush(self, *, exchange_id: str, requested: int) -> None:
        if self.suppressed <= 0:
            return
        _warn_nonfatal(
            "CCXT_LIVE_FAILURE_TELEMETRY_SUPPRESSED",
            RuntimeError("ccxt_live_failure_telemetry_suppressed"),
            exchange_id=str(exchange_id),
            requested=int(requested),
            emitted=int(self.emitted),
            suppressed=int(self.suppressed),
            limit=int(self.limit),
        )


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_ccxt_live_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.live_prices.ccxt_live",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _emit_ccxt_counter(metric: str, value: int | float = 1, *, exchange_id: str, **extra_tags: object) -> None:
    tags: Dict[str, object] = {
        "exchange": str(exchange_id or ""),
        "exchange_id": str(exchange_id or ""),
    }
    tags.update({str(key): str(val) for key, val in extra_tags.items() if val is not None})
    emit_counter(
        str(metric),
        value,
        component=_METRIC_COMPONENT,
        provider="ccxt",
        extra_tags=tags,
    )


def _emit_ccxt_timing(metric: str, latency_ms: int | float, *, exchange_id: str, **extra_tags: object) -> None:
    tags: Dict[str, object] = {"exchange_id": str(exchange_id or "")}
    tags.update({str(key): str(val) for key, val in extra_tags.items() if val is not None})
    emit_timing(
        str(metric),
        latency_ms,
        component=_METRIC_COMPONENT,
        provider="ccxt",
        extra_tags=tags,
    )


def _finite_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        out = float(cast(Any, value))
    except Exception as e:
        _warn_nonfatal(
            "CCXT_LIVE_FLOAT_PARSE_FAILED",
            e,
            once_key="ccxt_live_float_parse",
            value=repr(value)[:120],
        )
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _clean_exchange_id(exchange_id: str) -> str:
    return str(exchange_id or "").strip().lower()


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_positive_int(name: str) -> int | None:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = int(float(str(raw).strip()))
    except (TypeError, ValueError) as e:
        _warn_nonfatal(
            "CCXT_LIVE_CONFIG_INVALID",
            e,
            once_key=f"config_invalid:{name}",
            config_key=str(name),
            config_value=repr(raw)[:120],
        )
        return None
    if value <= 0:
        _warn_nonfatal(
            "CCXT_LIVE_CONFIG_INVALID",
            ValueError(f"{name} must be positive"),
            once_key=f"config_invalid:{name}",
            config_key=str(name),
            config_value=repr(raw)[:120],
        )
        return None
    return int(value)


def _exchange_options() -> dict[str, Any]:
    raw = os.environ.get("CCXT_OPTIONS_JSON")
    if raw is None or str(raw).strip() == "":
        return {}
    try:
        parsed = json.loads(str(raw))
    except json.JSONDecodeError as e:
        _warn_nonfatal(
            "CCXT_LIVE_CONFIG_INVALID",
            e,
            once_key="config_invalid:CCXT_OPTIONS_JSON",
            config_key="CCXT_OPTIONS_JSON",
        )
        return {}
    if not isinstance(parsed, dict):
        _warn_nonfatal(
            "CCXT_LIVE_CONFIG_INVALID",
            TypeError("CCXT_OPTIONS_JSON must decode to an object"),
            once_key="config_invalid:CCXT_OPTIONS_JSON:type",
            config_key="CCXT_OPTIONS_JSON",
            parsed_type=type(parsed).__name__,
        )
        return {}
    return dict(parsed)


def _exchange_config() -> Dict[str, Any]:
    config: Dict[str, Any] = {"enableRateLimit": True}

    timeout_ms = _env_positive_int("CCXT_TIMEOUT_MS")
    if timeout_ms is not None:
        config["timeout"] = int(timeout_ms)

    for env_name, config_key in (
        ("CCXT_API_KEY", "apiKey"),
        ("CCXT_SECRET", "secret"),
        ("CCXT_PASSWORD", "password"),
    ):
        value = str(os.environ.get(env_name) or "").strip()
        if value:
            config[str(config_key)] = value

    options = _exchange_options()
    if options:
        config["options"] = options

    return config


def _exchange_sandbox_enabled() -> bool:
    return _env_flag("CCXT_SANDBOX", False)


def _config_fingerprint_value(key: str, value: object) -> str:
    key_l = str(key or "").strip().lower()
    if any(token in key_l for token in ("key", "secret", "password", "token", "credential", "private")):
        import hashlib

        digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]
        return f"<redacted:{digest}>"
    return repr(value)


def _exchange_cache_key(exchange_id: str, config: Mapping[str, object], *, sandbox_enabled: bool = False) -> str:
    config_fingerprint = repr(
        tuple((str(key), _config_fingerprint_value(str(key), config[key])) for key in sorted(config))
    )
    return f"{str(exchange_id)}|sandbox={int(bool(sandbox_enabled))}|{config_fingerprint}"


def _ccxt_error_classes(*names: str) -> tuple[type[BaseException], ...]:
    if ccxt is None:
        return tuple()
    classes: list[type[BaseException]] = []
    for name in names:
        candidate = getattr(ccxt, str(name), None)
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            classes.append(candidate)
    return tuple(classes)


def _classify_ccxt_error(error: BaseException) -> str:
    rate_limit_errors = _ccxt_error_classes("RateLimitExceeded")
    if rate_limit_errors and isinstance(error, rate_limit_errors):
        return "rate_limited"

    symbol_errors = _ccxt_error_classes("BadSymbol")
    if symbol_errors and isinstance(error, symbol_errors):
        return "bad_symbol"

    fatal_errors = _ccxt_error_classes("AuthenticationError", "PermissionDenied", "AccountSuspended")
    if fatal_errors and isinstance(error, fatal_errors):
        return "fatal"

    stale_errors = _ccxt_error_classes(
        "NetworkError",
        "RequestTimeout",
        "ExchangeNotAvailable",
        "DDoSProtection",
        "InvalidNonce",
    )
    if stale_errors and isinstance(error, stale_errors):
        return "stale"

    return "nonfatal"


def _should_stop_after_error(error_classification: str) -> bool:
    return str(error_classification) in {"fatal", "rate_limited", "stale"}


def _close_exchange(exchange: Any, *, exchange_id: str, reason: str) -> None:
    close = getattr(exchange, "close", None)
    if not callable(close):
        return
    try:
        close()
    except Exception as e:
        _warn_nonfatal(
            "CCXT_LIVE_EXCHANGE_CLOSE_FAILED",
            e,
            once_key=f"exchange_close:{exchange_id}:{type(e).__name__}",
            exchange_id=str(exchange_id),
            reason=str(reason),
        )


def _evict_cached_exchange(exchange_id: str, cached: _CachedExchange, *, reason: str) -> None:
    removed: _CachedExchange | None = None
    with _EXCHANGE_CACHE_LOCK:
        current = _EXCHANGE_CACHE.get(str(cached.cache_key))
        if current is cached:
            removed = _EXCHANGE_CACHE.pop(str(cached.cache_key), None)
    if removed is None:
        return
    with removed.lock:
        _close_exchange(removed.exchange, exchange_id=str(exchange_id), reason=str(reason))
    _emit_ccxt_counter(
        "ccxt_live_exchange_cache_evictions",
        1,
        exchange_id=str(exchange_id),
        reason=str(reason),
    )


def _mark_markets_stale(cached: _CachedExchange, *, reason: str) -> None:
    with cached.lock:
        cached.markets_loaded = False
    _emit_ccxt_counter(
        "ccxt_live_market_cache_invalidations",
        1,
        exchange_id=str(cached.exchange_id),
        reason=str(reason),
    )


def _build_exchange(
    exchange_id: str,
    *,
    config: Mapping[str, object],
    cache_key: str,
    sandbox_enabled: bool,
) -> _CachedExchange | None:
    if ccxt is None:
        return None

    ex_class = getattr(ccxt, exchange_id, None)
    if ex_class is None:
        _warn_nonfatal(
            "CCXT_LIVE_EXCHANGE_NOT_FOUND",
            RuntimeError(f"ccxt_exchange_not_found:{exchange_id}"),
            once_key=f"exchange_not_found:{exchange_id}",
            exchange_id=str(exchange_id),
        )
        return None

    try:
        exchange = ex_class(dict(config))
    except Exception as e:
        _warn_nonfatal(
            "CCXT_LIVE_EXCHANGE_INIT_FAILED",
            e,
            once_key=f"exchange_init:{exchange_id}:{type(e).__name__}",
            exchange_id=str(exchange_id),
        )
        return None

    if sandbox_enabled:
        set_sandbox_mode = getattr(exchange, "set_sandbox_mode", None)
        if callable(set_sandbox_mode):
            try:
                set_sandbox_mode(True)
            except Exception as e:
                _warn_nonfatal(
                    "CCXT_LIVE_SANDBOX_ENABLE_FAILED",
                    e,
                    once_key=f"sandbox_enable:{exchange_id}:{type(e).__name__}",
                    exchange_id=str(exchange_id),
                )
                _close_exchange(exchange, exchange_id=str(exchange_id), reason="sandbox_enable_failed")
                return None
        else:
            _warn_nonfatal(
                "CCXT_LIVE_SANDBOX_UNSUPPORTED",
                RuntimeError("ccxt_live_sandbox_unsupported"),
                once_key=f"sandbox_unsupported:{exchange_id}",
                exchange_id=str(exchange_id),
            )
            _close_exchange(exchange, exchange_id=str(exchange_id), reason="sandbox_unsupported")
            return None

    return _CachedExchange(exchange_id=str(exchange_id), cache_key=str(cache_key), exchange=exchange)


def _ensure_markets_loaded(cached: _CachedExchange) -> bool:
    with cached.lock:
        cached.last_used_monotonic = time.monotonic()
        if cached.markets_loaded:
            _emit_ccxt_counter(
                "ccxt_live_markets_reuses",
                1,
                exchange_id=str(cached.exchange_id),
            )
            _emit_ccxt_counter(
                "ccxt_live_market_cache_hits",
                1,
                exchange_id=str(cached.exchange_id),
            )
            return True
        load_markets = getattr(cached.exchange, "load_markets", None)
        if callable(load_markets):
            started = time.perf_counter()
            try:
                load_markets()
            except Exception as e:
                latency_ms = (time.perf_counter() - started) * 1000.0
                _emit_ccxt_timing(
                    "ccxt_live_markets_load_latency_ms",
                    latency_ms,
                    exchange_id=str(cached.exchange_id),
                    ok=False,
                )
                classification = _classify_ccxt_error(e)
                _warn_nonfatal(
                    "CCXT_LIVE_MARKETS_LOAD_FAILED",
                    e,
                    once_key=f"markets_load:{cached.exchange_id}:{type(e).__name__}",
                    exchange_id=str(cached.exchange_id),
                    error_classification=str(classification),
                )
                if classification in {"fatal", "stale"}:
                    _evict_cached_exchange(
                        cached.exchange_id,
                        cached,
                        reason=f"markets_load_{classification}",
                    )
                return False
            latency_ms = (time.perf_counter() - started) * 1000.0
            _emit_ccxt_counter("ccxt_live_markets_loads", 1, exchange_id=str(cached.exchange_id))
            _emit_ccxt_counter("ccxt_live_market_cache_reloads", 1, exchange_id=str(cached.exchange_id))
            _emit_ccxt_timing(
                "ccxt_live_markets_load_latency_ms",
                latency_ms,
                exchange_id=str(cached.exchange_id),
                ok=True,
            )
        cached.markets_loaded = True
        return True


def _get_cached_exchange(exchange_id: str) -> _CachedExchange | None:
    clean_id = _clean_exchange_id(exchange_id)
    if not clean_id or ccxt is None:
        return None

    config = _exchange_config()
    sandbox_enabled = _exchange_sandbox_enabled()
    cache_key = _exchange_cache_key(clean_id, config, sandbox_enabled=sandbox_enabled)
    cache_metric = "hit"
    with _EXCHANGE_CACHE_LOCK:
        cached = _EXCHANGE_CACHE.get(cache_key)
        if cached is None:
            cache_metric = "miss"
            cached = _build_exchange(
                clean_id,
                config=config,
                cache_key=cache_key,
                sandbox_enabled=bool(sandbox_enabled),
            )
            if cached is None:
                _emit_ccxt_counter("ccxt_live_exchange_cache_misses", 1, exchange_id=clean_id)
                return None
            _EXCHANGE_CACHE[cache_key] = cached

    _emit_ccxt_counter(
        "ccxt_live_exchange_cache_hits" if cache_metric == "hit" else "ccxt_live_exchange_cache_misses",
        1,
        exchange_id=clean_id,
    )

    if not _ensure_markets_loaded(cached):
        return None
    return cached


def _clear_exchange_cache_for_tests() -> None:
    with _EXCHANGE_CACHE_LOCK:
        cached = list(_EXCHANGE_CACHE.values())
        _EXCHANGE_CACHE.clear()
    for entry in cached:
        with entry.lock:
            _close_exchange(entry.exchange, exchange_id=entry.exchange_id, reason="test_clear")


def _supports_fetch_tickers(exchange: Any) -> bool:
    has = getattr(exchange, "has", {}) or {}
    supported: object = None
    if isinstance(has, Mapping):
        supported = has.get("fetchTickers")
    else:
        supported = getattr(has, "fetchTickers", None)
    return supported is True or str(supported).strip().lower() == "true"


def _unique_markets(markets: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for market in markets:
        market_s = str(market or "").strip()
        if not market_s or market_s in seen:
            continue
        seen.add(market_s)
        out.append(market_s)
    return out


def _index_tickers_by_market(tickers: object) -> Dict[str, dict]:
    out: Dict[str, dict] = {}

    def _store(key: object, ticker: object) -> None:
        key_s = str(key or "").strip()
        if key_s and isinstance(ticker, dict):
            out.setdefault(key_s, dict(ticker))

    if isinstance(tickers, Mapping):
        for key, ticker in tickers.items():
            if not isinstance(ticker, dict):
                continue
            _store(key, ticker)
            _store(ticker.get("symbol"), ticker)
    elif isinstance(tickers, list):
        for ticker in tickers:
            if isinstance(ticker, dict):
                _store(ticker.get("symbol"), ticker)

    return out


def _lookup_ticker(indexed_tickers: Mapping[str, dict], market: str) -> dict | None:
    market_s = str(market or "").strip()
    if not market_s:
        return None
    direct = indexed_tickers.get(market_s)
    if isinstance(direct, dict):
        return direct

    market_fold = market_s.upper()
    for key, ticker in indexed_tickers.items():
        if str(key or "").strip().upper() == market_fold and isinstance(ticker, dict):
            return ticker
    return None


def _ticker_to_price_row(
    ticker: object,
    *,
    symbol: str,
    market: str,
    now_ms: int,
    telemetry: _FailureTelemetryBudget,
) -> dict | None:
    if not isinstance(ticker, dict):
        telemetry.warn(
            "CCXT_LIVE_TICKER_PARSE_FAILED",
            RuntimeError("ccxt_live_ticker_not_mapping"),
            once_key=f"ticker_parse:{symbol}",
            symbol=str(symbol),
            market=str(market),
            ticker_type=type(ticker).__name__,
        )
        return None

    last = _finite_float_or_none(ticker.get("last", None))
    if last is None:
        telemetry.warn(
            "CCXT_LIVE_SKIP_NO_PRICE",
            RuntimeError("ccxt_live_skip_no_price"),
            once_key=f"skip_no_price:{symbol}",
            symbol=str(symbol),
            market=str(market),
        )
        return None

    bid = _finite_float_or_none(ticker.get("bid"))
    ask = _finite_float_or_none(ticker.get("ask"))
    spread = None
    try:
        if bid is not None and ask is not None:
            spread = float(ask) - float(bid)
    except Exception:
        spread = None

    return {
        "ts_ms": int(now_ms),
        "price": float(last),
        "bid": (float(bid) if bid is not None else None),
        "ask": (float(ask) if ask is not None else None),
        "spread": spread,
        "volume": _finite_float_or_none(ticker.get("baseVolume")),
        "source": "ccxt",
    }


def _fetch_batch_tickers(
    cached: _CachedExchange,
    markets: list[str],
    *,
    telemetry: _FailureTelemetryBudget,
) -> _BatchTickerResult:
    if not markets:
        return _BatchTickerResult({}, False, False)
    if not _supports_fetch_tickers(cached.exchange):
        return _BatchTickerResult({}, False, False, unsupported=True)

    fetch_tickers = getattr(cached.exchange, "fetch_tickers", None)
    if not callable(fetch_tickers):
        return _BatchTickerResult({}, False, False, unsupported=True)

    try:
        started = time.perf_counter()
        with cached.lock:
            cached.last_used_monotonic = time.monotonic()
            _emit_ccxt_counter(
                "ccxt_live_fetch_tickers_attempts",
                1,
                exchange_id=str(cached.exchange_id),
                requested_markets=int(len(markets)),
            )
            tickers = fetch_tickers(list(markets))
        latency_ms = (time.perf_counter() - started) * 1000.0
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_successes",
            1,
            exchange_id=str(cached.exchange_id),
            requested_markets=int(len(markets)),
        )
        _emit_ccxt_timing(
            "ccxt_live_fetch_tickers_latency_ms",
            latency_ms,
            exchange_id=str(cached.exchange_id),
            ok=True,
            markets=int(len(markets)),
        )
        return _BatchTickerResult(_index_tickers_by_market(tickers), True, False)
    except Exception as e:
        latency_ms = (time.perf_counter() - started) * 1000.0
        _emit_ccxt_timing(
            "ccxt_live_fetch_tickers_latency_ms",
            latency_ms,
            exchange_id=str(cached.exchange_id),
            ok=False,
            markets=int(len(markets)),
        )
        classification = _classify_ccxt_error(e)
        stop_after_error = _should_stop_after_error(classification)
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_failures",
            1,
            exchange_id=str(cached.exchange_id),
            requested_markets=int(len(markets)),
            error_classification=str(classification),
        )
        telemetry.warn(
            "CCXT_LIVE_BATCH_TICKERS_FAILED",
            e,
            once_key=f"batch_tickers:{cached.exchange_id}:{type(e).__name__}",
            exchange_id=str(cached.exchange_id),
            requested_markets=int(len(markets)),
            error_classification=str(classification),
        )
        if classification in {"fatal", "stale"}:
            _evict_cached_exchange(
                cached.exchange_id,
                cached,
                reason=f"batch_tickers_{classification}",
            )
        elif classification == "bad_symbol":
            _mark_markets_stale(cached, reason="batch_tickers_bad_symbol")
    return _BatchTickerResult({}, True, stop_after_error, failed=True, failure_reason=str(classification))


def _fetch_single_ticker(
    cached: _CachedExchange,
    *,
    symbol: str,
    market: str,
    telemetry: _FailureTelemetryBudget,
) -> tuple[object | None, bool]:
    fetch_ticker = getattr(cached.exchange, "fetch_ticker", None)
    if not callable(fetch_ticker):
        telemetry.warn(
            "CCXT_LIVE_TICKER_FETCH_FAILED",
            RuntimeError("ccxt_live_fetch_ticker_unavailable"),
            once_key=f"fetch_ticker_unavailable:{cached.exchange_id}",
            exchange_id=str(cached.exchange_id),
            symbol=str(symbol),
            market=str(market),
        )
        return None, False

    started = time.perf_counter()
    try:
        with cached.lock:
            cached.last_used_monotonic = time.monotonic()
            ticker = fetch_ticker(market)
        latency_ms = (time.perf_counter() - started) * 1000.0
        _emit_ccxt_timing(
            "ccxt_live_fallback_fetch_latency_ms",
            latency_ms,
            exchange_id=str(cached.exchange_id),
            ok=True,
        )
        return (dict(ticker) if isinstance(ticker, dict) else ticker), False
    except Exception as e:
        latency_ms = (time.perf_counter() - started) * 1000.0
        _emit_ccxt_timing(
            "ccxt_live_fallback_fetch_latency_ms",
            latency_ms,
            exchange_id=str(cached.exchange_id),
            ok=False,
        )
        classification = _classify_ccxt_error(e)
        stop_after_error = _should_stop_after_error(classification)
        telemetry.warn(
            "CCXT_LIVE_TICKER_FETCH_FAILED",
            e,
            once_key=f"ticker_fetch:{cached.exchange_id}:{symbol}:{type(e).__name__}",
            exchange_id=str(cached.exchange_id),
            symbol=str(symbol),
            market=str(market),
            error_classification=str(classification),
        )
        if classification in {"fatal", "stale"}:
            _evict_cached_exchange(
                cached.exchange_id,
                cached,
                reason=f"ticker_fetch_{classification}",
            )
        elif classification == "bad_symbol":
            _mark_markets_stale(cached, reason="ticker_fetch_bad_symbol")
    return None, stop_after_error


def _emit_fetch_path_metrics(
    *,
    exchange_id: str,
    requested: int,
    returned: int,
    skipped: int,
    failed: int,
    batch_used: bool,
    batch_failed: bool,
    batch_failure_reason: str,
    batch_stop: bool,
    batch_markets: int,
    batch_rows: int,
    batch_unsupported: bool,
    batch_partial_misses: int,
    batch_missing_symbols: int,
    batch_invalid_rows: int,
    fallback_fetches: int,
    fallback_rows: int,
) -> None:
    if batch_stop:
        path = "batch_stopped"
    elif batch_failed:
        path = "batch_failed"
    elif bool(batch_used) and int(fallback_fetches) > 0:
        path = "batch_with_fallback"
    elif bool(batch_used):
        path = "batch_only"
    elif int(fallback_fetches) > 0:
        path = "fallback_only"
    else:
        path = "empty"

    common_tags = {
        "path": path,
        "batch_used": bool(batch_used),
        "supports_batch": "0" if batch_unsupported else "1",
        "batch_stop": bool(batch_stop),
        "batch_unsupported": bool(batch_unsupported),
        "batch_failed": bool(batch_failed),
        "batch_failure_reason": str(batch_failure_reason or ""),
    }
    _emit_ccxt_counter(
        "ccxt_live_fetch_cycles",
        1,
        exchange_id=exchange_id,
        **common_tags,
        requested=int(requested),
        returned=int(returned),
        skipped=int(skipped),
        failed=int(failed),
    )
    if batch_used:
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_calls",
            1,
            exchange_id=exchange_id,
            **common_tags,
        )
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_markets",
            int(batch_markets),
            exchange_id=exchange_id,
            **common_tags,
        )
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_rows",
            int(batch_rows),
            exchange_id=exchange_id,
            **common_tags,
        )
    if batch_unsupported:
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_unsupported",
            1,
            exchange_id=exchange_id,
            **common_tags,
            reason="unsupported",
        )
    if batch_failed:
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_cycle_failures",
            1,
            exchange_id=exchange_id,
            **common_tags,
            reason=str(batch_failure_reason or "exception"),
        )
    if batch_missing_symbols:
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_partial_misses",
            int(batch_missing_symbols),
            exchange_id=exchange_id,
            **common_tags,
            reason="missing_symbol",
        )
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_missing_symbols",
            int(batch_missing_symbols),
            exchange_id=exchange_id,
            **common_tags,
        )
    if batch_invalid_rows:
        _emit_ccxt_counter(
            "ccxt_live_fetch_tickers_partial_misses",
            int(batch_invalid_rows),
            exchange_id=exchange_id,
            **common_tags,
            reason="invalid_row",
        )
    if failed:
        _emit_ccxt_counter(
            "ccxt_live_failed_symbols",
            int(failed),
            exchange_id=exchange_id,
            **common_tags,
        )
    if fallback_fetches:
        _emit_ccxt_counter(
            "ccxt_live_fallback_fetches",
            int(fallback_fetches),
            exchange_id=exchange_id,
            **common_tags,
        )
        _emit_ccxt_counter(
            "ccxt_live_fallback_rows",
            int(fallback_rows),
            exchange_id=exchange_id,
            **common_tags,
        )
        fallback_failures = max(0, int(fallback_fetches) - int(fallback_rows))
        if fallback_rows:
            _emit_ccxt_counter(
                "ccxt_live_fallback_successes",
                int(fallback_rows),
                exchange_id=exchange_id,
                **common_tags,
            )
        if fallback_failures:
            _emit_ccxt_counter(
                "ccxt_live_fallback_failures",
                int(fallback_failures),
                exchange_id=exchange_id,
                **common_tags,
            )


def fetch_latest_ohlcv_ccxt(exchange_id: str, market_map: Dict[str, str], timeframe: str = "1m") -> Dict[str, dict]:
    """
    Returns latest OHLCV bar for each market via CCXT.
    Output:
      { "BTC": {"ts_ms":..., "tf_s":60, "o":..,"h":..,"l":..,"c":..,"v":..}, ... }
    """
    out: Dict[str, dict] = {}
    tf_s = 60 if timeframe == "1m" else 300 if timeframe == "5m" else 60

    cached = _get_cached_exchange(exchange_id)
    if cached is None:
        return out

    clean_map = [
        (str(sym).strip(), str(market).strip())
        for sym, market in (market_map or {}).items()
        if str(sym).strip() and str(market).strip()
    ]
    telemetry = _FailureTelemetryBudget()

    for sym, market in clean_map:
        stop_after_error = False
        try:
            with cached.lock:
                cached.last_used_monotonic = time.monotonic()
                bars = cached.exchange.fetch_ohlcv(market, timeframe=timeframe, limit=2)
            if not bars:
                continue
            ts, o, h, low, c, v = bars[-1]
            out[str(sym)] = {
                "ts_ms": int(ts),
                "tf_s": int(tf_s),
                "o": float(o),
                "h": float(h),
                "l": float(low),
                "c": float(c),
                "v": float(v) if v is not None else None,
            }
        except Exception as e:
            classification = _classify_ccxt_error(e)
            telemetry.warn(
                "CCXT_LIVE_OHLCV_FETCH_FAILED",
                e,
                once_key=f"ohlcv:{cached.exchange_id}:{sym}:{type(e).__name__}",
                exchange_id=str(cached.exchange_id),
                symbol=str(sym),
                market=str(market),
                error_classification=str(classification),
            )
            if classification in {"fatal", "stale"}:
                _evict_cached_exchange(
                    cached.exchange_id,
                    cached,
                    reason=f"ohlcv_fetch_{classification}",
                )
            elif classification == "bad_symbol":
                _mark_markets_stale(cached, reason="ohlcv_fetch_bad_symbol")
            if _should_stop_after_error(classification):
                stop_after_error = True
        if stop_after_error:
            break

    telemetry.flush(exchange_id=str(cached.exchange_id), requested=len(clean_map))
    return out


def fetch_last_prices_ccxt(exchange_id: str, market_map: Dict[str, str]) -> Dict[str, dict]:
    """
    exchange_id: "binance", "kraken", etc.
    market_map: { "BTC": "BTC/USDT", ... }
    Returns: { "BTC": {ts_ms, price}, ... }
    """
    out: Dict[str, dict] = {}
    now_ms = int(time.time() * 1000)

    cached = _get_cached_exchange(exchange_id)
    if cached is None:
        return out

    items = [
        (str(sym).strip(), str(market).strip())
        for sym, market in (market_map or {}).items()
        if str(sym).strip() and str(market).strip()
    ]
    requested = len(items)
    skipped = 0
    batch_rows = 0
    batch_partial_misses = 0
    batch_missing_symbols = 0
    batch_invalid_rows = 0
    fallback_fetches = 0
    fallback_rows = 0
    failed = 0
    telemetry = _FailureTelemetryBudget()
    batch_markets = _unique_markets(market for _, market in items)
    batch_result = _fetch_batch_tickers(
        cached,
        batch_markets,
        telemetry=telemetry,
    )

    for sym, market in items:
        ticker: object | None = _lookup_ticker(batch_result.tickers, market) if batch_result.tickers else None
        ticker_from_batch = ticker is not None
        if ticker is None:
            if batch_result.stop:
                failed += 1
                continue
            if batch_result.used and not batch_result.failed:
                batch_partial_misses += 1
                batch_missing_symbols += 1
            ticker, stop_after_error = _fetch_single_ticker(
                cached,
                symbol=sym,
                market=market,
                telemetry=telemetry,
            )
            fallback_fetches += 1
            if ticker is None:
                failed += 1
                if stop_after_error:
                    break
                continue

        row = _ticker_to_price_row(
            ticker,
            symbol=sym,
            market=market,
            now_ms=now_ms,
            telemetry=telemetry,
        )
        if row is None:
            if ticker_from_batch and not batch_result.stop:
                batch_partial_misses += 1
                batch_invalid_rows += 1
                ticker, stop_after_error = _fetch_single_ticker(
                    cached,
                    symbol=sym,
                    market=market,
                    telemetry=telemetry,
                )
                fallback_fetches += 1
                if ticker is None:
                    failed += 1
                    if stop_after_error:
                        break
                    continue
                row = _ticker_to_price_row(
                    ticker,
                    symbol=sym,
                    market=market,
                    now_ms=now_ms,
                    telemetry=telemetry,
                )
                if row is not None:
                    out[str(sym)] = row
                    fallback_rows += 1
                    continue
            skipped += 1
            continue
        out[str(sym)] = row
        if ticker_from_batch:
            batch_rows += 1
        else:
            fallback_rows += 1

    if batch_partial_misses:
        log_failure(
            LOG,
            event="ccxt_live_batch_partial",
            code="CCXT_LIVE_BATCH_PARTIAL",
            message="CCXT live batch response required per-symbol fallback",
            error=RuntimeError("ccxt_live_batch_partial"),
            level=logging.WARNING,
            component=_METRIC_COMPONENT,
            extra={
                "provider": "ccxt",
                "exchange_id": str(cached.exchange_id),
                "requested": int(requested),
                "batch_partial_misses": int(batch_partial_misses),
                "batch_missing_symbols": int(batch_missing_symbols),
                "batch_invalid_rows": int(batch_invalid_rows),
                "fallback_fetches": int(fallback_fetches),
                "failed": int(failed),
            },
            persist=False,
        )

    telemetry.flush(exchange_id=str(cached.exchange_id), requested=requested)
    _emit_fetch_path_metrics(
        exchange_id=str(cached.exchange_id),
        requested=int(requested),
        returned=int(len(out)),
        skipped=int(skipped),
        failed=int(failed),
        batch_used=bool(batch_result.used),
        batch_failed=bool(batch_result.failed),
        batch_failure_reason=str(batch_result.failure_reason),
        batch_stop=bool(batch_result.stop),
        batch_markets=int(len(batch_markets)),
        batch_rows=int(batch_rows),
        batch_unsupported=bool(batch_result.unsupported),
        batch_partial_misses=int(batch_partial_misses),
        batch_missing_symbols=int(batch_missing_symbols),
        batch_invalid_rows=int(batch_invalid_rows),
        fallback_fetches=int(fallback_fetches),
        fallback_rows=int(fallback_rows),
    )
    LOG.info(
        "fetch_complete provider=ccxt requested=%d returned=%d skipped=%d failed=%d batch_used=%s fallback_fetches=%d",
        requested,
        len(out),
        skipped,
        failed,
        str(bool(batch_result.used)),
        fallback_fetches,
    )
    return out


class CCXTPriceProvider:
    def __init__(self):
        self.exchange_id = str(os.environ.get("CCXT_EXCHANGE_ID", "kraken")).strip() or "kraken"

    def fetch_last_prices(self, ticker_map: Dict[str, str]) -> Dict[str, dict]:
        return fetch_last_prices_ccxt(self.exchange_id, ticker_map)
