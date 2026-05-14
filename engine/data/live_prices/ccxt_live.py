"""
FILE: ccxt_live.py

Live price feed integration for `ccxt_live`.
"""

# dev_core/live_prices/ccxt_live.py
import logging
import math
import os
import time
from typing import Dict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

try:
    import ccxt
    _CCXT_IMPORT_ERROR = None
except Exception as _ccxt_import_error:
    ccxt = None  # type: ignore
    _CCXT_IMPORT_ERROR = _ccxt_import_error

_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("data.live_prices.ccxt_live")


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


def _finite_float_or_none(value: object) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
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

def fetch_latest_ohlcv_ccxt(exchange_id: str, market_map: Dict[str, str], timeframe: str = "1m") -> Dict[str, dict]:
    """
    Returns latest OHLCV bar for each market via CCXT.
    Output:
      { "BTC": {"ts_ms":..., "tf_s":60, "o":..,"h":..,"l":..,"c":..,"v":..}, ... }
    """
    out: Dict[str, dict] = {}
    tf_s = 60 if timeframe == "1m" else 300 if timeframe == "5m" else 60

    if ccxt is None:
        return out

    ex_class = getattr(ccxt, exchange_id, None)
    if ex_class is None:
        return out

    ex = ex_class({"enableRateLimit": True})

    for sym, market in (market_map or {}).items():
        try:
            bars = ex.fetch_ohlcv(market, timeframe=timeframe, limit=2)
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
            _warn_nonfatal("CCXT_LIVE_OHLCV_PARSE_FAILED", e, once_key=f"ohlcv:{sym}", symbol=str(sym), market=str(market))
            continue

    return out


def fetch_last_prices_ccxt(exchange_id: str, market_map: Dict[str, str]) -> Dict[str, dict]:
    """
    exchange_id: "binance", "kraken", etc.
    market_map: { "BTC": "BTC/USDT", ... }
    Returns: { "BTC": {ts_ms, price}, ... }
    """
    out = {}
    now_ms = int(time.time() * 1000)

    if ccxt is None:
        return out

    ex_class = getattr(ccxt, exchange_id, None)
    if ex_class is None:
        return out

    ex = ex_class({"enableRateLimit": True})
    requested = 0
    skipped = 0

    for sym, market in market_map.items():
        requested += 1
        try:
            t = ex.fetch_ticker(market)
            last = _finite_float_or_none(t.get("last", None))
            if last is None:
                skipped += 1
                _warn_nonfatal(
                    "CCXT_LIVE_SKIP_NO_PRICE",
                    RuntimeError("ccxt_live_skip_no_price"),
                    once_key=f"skip_no_price:{sym}",
                    symbol=str(sym),
                    market=str(market),
                )
                continue
            bid = _finite_float_or_none(t.get("bid"))
            ask = _finite_float_or_none(t.get("ask"))
            spread = None
            try:
                if bid is not None and ask is not None:
                    spread = float(ask) - float(bid)
            except Exception:
                spread = None

            out[str(sym)] = {
                "ts_ms": now_ms,
                "price": float(last),
                "bid": (float(bid) if bid is not None else None),
                "ask": (float(ask) if ask is not None else None),
                "spread": spread,
                "volume": _finite_float_or_none(t.get("baseVolume")),
                "source": "ccxt",
            }
        except Exception as e:
            _warn_nonfatal("CCXT_LIVE_TICKER_PARSE_FAILED", e, once_key=f"ticker:{sym}", symbol=str(sym), market=str(market))
            continue

    LOG.info("fetch_complete provider=ccxt requested=%d returned=%d skipped=%d", requested, len(out), skipped)
    return out


class CCXTPriceProvider:
    def __init__(self):
        self.exchange_id = str(os.environ.get("CCXT_EXCHANGE_ID", "kraken")).strip() or "kraken"

    def fetch_last_prices(self, ticker_map: Dict[str, str]) -> Dict[str, dict]:
        return fetch_last_prices_ccxt(self.exchange_id, ticker_map)
