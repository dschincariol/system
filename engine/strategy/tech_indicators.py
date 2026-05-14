"""
FILE: tech_indicators.py

Computes lightweight price-only technical features such as realized vol, KAMA,
ATR proxies, and related diagnostics. These features are consumed by the
feature-expansion path rather than directly generating trades.
"""

import os
import time
import math
from typing import Any, List, Dict, Tuple

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

# --------------------------------------------
# Runtime controls
# --------------------------------------------

TECH_LOOKBACK = int(os.environ.get("TECH_LOOKBACK", "400"))          # price points
ATR_N = int(os.environ.get("TECH_ATR_N", "14"))
RV_N = int(os.environ.get("TECH_RV_N", "20"))
VOV_N = int(os.environ.get("TECH_VOV_N", "60"))

KAMA_ER_N = int(os.environ.get("TECH_KAMA_ER_N", "10"))
KAMA_FAST = int(os.environ.get("TECH_KAMA_FAST", "2"))
KAMA_SLOW = int(os.environ.get("TECH_KAMA_SLOW", "30"))
KAMA_SLOPE_N = int(os.environ.get("TECH_KAMA_SLOPE_N", "5"))

# Tiny in-process cache avoids repeated reads when several features are queried
# for the same symbol/timestamp during one decision cycle.
_CACHE_TTL_S = float(os.environ.get("TECH_CACHE_TTL_S", "3.0"))
_cache = {
    # (symbol, ts_ms) -> (ts_s_cached, features_dict)
    "items": {},
    "ts_s": 0.0,
}
LOG = get_logger("engine.strategy.tech_indicators")
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
        component="engine.strategy.tech_indicators",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _load_prices(
    symbol: str,
    ts_ms: int,
    lookback: int,
    con=None,
) -> List[Tuple[int, float]]:
    """
    Returns ascending list[(ts_ms, price)] up to ts_ms inclusive.
    """
    if con is None:
        con = connect()
    try:
        rows = con.execute(
            """
            SELECT ts_ms, price
            FROM prices
            WHERE symbol=? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(ts_ms), int(lookback)),
        ).fetchall()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "TECH_INDICATORS_CLOSE_FAILED",
                e,
                once_key="load_prices_close",
                symbol=str(symbol),
                ts_ms=int(ts_ms),
            )

    out = []
    for t, p in (rows or []):
        if t is None or p is None:
            continue
        try:
            out.append((int(t), float(p)))
        except Exception as e:
            _warn_nonfatal(
                "TECH_INDICATORS_PRICE_PARSE_FAILED",
                e,
                once_key="price_parse",
                symbol=str(symbol),
                ts_ms=int(ts_ms),
            )
            continue

    out.reverse()
    return out


def _log_returns(px: np.ndarray) -> np.ndarray:
    px = np.asarray(px, dtype=float)
    if px.size < 2:
        return np.asarray([], dtype=float)
    p0 = px[:-1]
    p1 = px[1:]
    good = (p0 > 0) & (p1 > 0)
    if not np.any(good):
        return np.asarray([], dtype=float)
    r = np.log(p1[good] / p0[good])
    return np.asarray(r, dtype=float)


def realized_vol(px: np.ndarray, n: int) -> float:
    r = _log_returns(px)
    if r.size < max(3, int(n)):
        return 0.0
    w = r[-int(n):]
    v = float(np.std(w, ddof=1))
    if not math.isfinite(v):
        return 0.0
    return max(0.0, v)


def vol_of_vol(px: np.ndarray, rv_n: int, vov_n: int) -> float:
    r = _log_returns(px)
    if r.size < max(10, int(rv_n) + int(vov_n)):
        return 0.0

    rvs = []
    for i in range(int(rv_n), r.size + 1):
        w = r[i - int(rv_n): i]
        v = float(np.std(w, ddof=1)) if w.size >= 3 else 0.0
        if math.isfinite(v):
            rvs.append(v)

    if len(rvs) < max(5, int(vov_n)):
        return 0.0

    w2 = np.asarray(rvs[-int(vov_n):], dtype=float)
    vv = float(np.std(w2, ddof=1)) if w2.size >= 3 else 0.0
    if not math.isfinite(vv):
        return 0.0
    return max(0.0, vv)


def kama(px: np.ndarray, er_n: int, fast: int, slow: int) -> float:
    """
    Kaufman Adaptive Moving Average (single pass; returns last value).
    """
    px = np.asarray(px, dtype=float)
    if px.size < max(20, int(er_n) + 2):
        return float(px[-1]) if px.size else 0.0

    er_n = int(er_n)
    fast = int(fast)
    slow = int(slow)

    fast_sc = 2.0 / (fast + 1.0)
    slow_sc = 2.0 / (slow + 1.0)

    k = float(np.mean(px[:er_n]))

    for i in range(er_n, px.size):
        change = abs(px[i] - px[i - er_n])
        volatility = float(np.sum(np.abs(np.diff(px[i - er_n:i + 1]))))
        er = float(change / volatility) if volatility > 1e-12 else 0.0
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
        k = k + sc * (px[i] - k)

    if not math.isfinite(k):
        return 0.0
    return float(k)


def atr_proxy(px: np.ndarray, n: int) -> float:
    """
    ATR proxy using abs log-return magnitude * price.
    """
    px = np.asarray(px, dtype=float)
    if px.size < max(4, int(n) + 1):
        return 0.0
    r = _log_returns(px)
    if r.size < int(n):
        return 0.0
    w = np.abs(r[-int(n):])
    a = float(np.mean(w)) if w.size else 0.0
    out = a * float(px[-1])
    if not math.isfinite(out):
        return 0.0
    return max(0.0, out)


def _zscore(x: float, xs: np.ndarray) -> float:
    xs = np.asarray(xs, dtype=float)
    if xs.size < 10:
        return 0.0
    m = float(np.mean(xs))
    s = float(np.std(xs, ddof=1)) if xs.size >= 3 else 0.0
    if s <= 1e-12:
        return 0.0
    z = (float(x) - m) / s
    if not math.isfinite(z):
        return 0.0
    return float(z)


def _trend_slope(px: np.ndarray, n: int) -> float:
    px = np.asarray(px, dtype=float)
    n_i = max(3, int(n))
    if px.size < n_i:
        return 0.0
    w = px[-n_i:]
    if np.any(~np.isfinite(w)) or np.any(w <= 0):
        return 0.0
    y = np.log(w)
    x = np.arange(w.size, dtype=float)
    try:
        slope, _ = np.polyfit(x, y, 1)
    except Exception as e:
        _warn_nonfatal("TECH_INDICATORS_TREND_SLOPE_FAILED", e, once_key="trend_slope_failed", n=int(n_i))
        return 0.0
    if not math.isfinite(float(slope)):
        return 0.0
    return float(slope)


def _market_regime_features(px: np.ndarray) -> Dict[str, Any]:
    px = np.asarray(px, dtype=float)
    out: Dict[str, Any] = {
        "market_regime_volatility": 0.0,
        "market_regime_volatility_baseline": 0.0,
        "market_regime_trend": 0.0,
        "market_regime_trend_strength": 0.0,
        "market_regime_label": "mean_reversion",
    }
    if px.size < max(40, int(RV_N) + 5):
        return out

    rets = _log_returns(px)
    if rets.size < max(10, int(RV_N)):
        return out

    recent = rets[-int(RV_N):]
    hist_n = min(rets.size, max(int(RV_N) * 3, 60))
    hist = rets[-hist_n:]

    vol = float(np.std(recent, ddof=1)) if recent.size >= 3 else 0.0
    vol_baseline = float(np.std(hist, ddof=1)) if hist.size >= 3 else 0.0
    trend = float(np.mean(recent)) if recent.size else 0.0
    slope = _trend_slope(px, max(int(RV_N), int(KAMA_SLOPE_N) * 2))

    trend_strength = 0.0
    denom = max(abs(vol), 1e-9)
    if math.isfinite(trend) and math.isfinite(slope):
        trend_strength = max(abs(trend), abs(slope)) / denom

    label = "mean_reversion"
    if math.isfinite(vol) and math.isfinite(vol_baseline) and vol_baseline > 1e-9 and vol >= (vol_baseline * 1.35):
        label = "high_vol"
    elif math.isfinite(trend_strength) and trend_strength >= 0.35:
        label = "trend"

    out["market_regime_volatility"] = float(vol) if math.isfinite(vol) else 0.0
    out["market_regime_volatility_baseline"] = float(vol_baseline) if math.isfinite(vol_baseline) else 0.0
    out["market_regime_trend"] = float(trend if abs(trend) >= abs(slope) else slope) if math.isfinite(trend) and math.isfinite(slope) else 0.0
    out["market_regime_trend_strength"] = float(trend_strength) if math.isfinite(trend_strength) else 0.0
    out["market_regime_label"] = str(label)
    return out


def compute_tech_features(symbol: str, ts_ms: int) -> Dict[str, float]:
    """
    Returns stable, leakage-safe features computed from prices up to ts_ms.
    """
    key = (str(symbol).upper(), int(ts_ms))
    now_s = time.monotonic()

    try:
        item = _cache["items"].get(key)
        if item is not None:
            ts_cached_s, feats = item
            if (now_s - float(ts_cached_s)) <= float(_CACHE_TTL_S):
                return dict(feats)
    except Exception as e:
        _warn_nonfatal(
            "TECH_INDICATORS_CACHE_READ_FAILED",
            e,
            once_key="cache_read",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )

    con = connect()
    try:
        series = _load_prices(
            str(symbol).upper(),
            int(ts_ms),
            int(TECH_LOOKBACK),
            con=con,
        )
        px = np.asarray([p for _, p in series], dtype=float)

        out: Dict[str, float] = {}

        last = float(px[-1]) if px.size else 0.0

        k = kama(px, KAMA_ER_N, KAMA_FAST, KAMA_SLOW)
        out["kama_level"] = float(k)

        if px.size >= max(50, KAMA_ER_N + KAMA_SLOPE_N + 2):
            k2 = kama(
                px[:-int(KAMA_SLOPE_N)],
                KAMA_ER_N,
                KAMA_FAST,
                KAMA_SLOW,
            )
            out["kama_slope"] = float(k - k2)
        else:
            out["kama_slope"] = 0.0

        a = atr_proxy(px, ATR_N)
        out["atr_14"] = float(a)
        out["atr_pct"] = float((a / last) if last > 0 else 0.0)

        rv = realized_vol(px, RV_N)
        out["rv_20"] = float(rv)

        vv = vol_of_vol(px, RV_N, VOV_N)
        out["vol_of_vol"] = float(vv)

        out.update(_market_regime_features(px))

        if a > 1e-12:
            out["price_kama_z"] = float((last - float(k)) / float(a))
        else:
            out["price_kama_z"] = 0.0

        try:
            vix_series = _load_prices("VIX", int(ts_ms), 200, con=con)
            vix_px = np.asarray([p for _, p in vix_series], dtype=float)
            if vix_px.size >= 5:
                vix_last = float(vix_px[-1])
                out["stress_vix_level"] = float(vix_last)
                out["stress_vix_z_60"] = float(_zscore(vix_last, vix_px[-60:]))
                out["stress_vix_change_1d"] = float(vix_last - float(vix_px[-2]))
            else:
                out["stress_vix_level"] = 0.0
                out["stress_vix_z_60"] = 0.0
                out["stress_vix_change_1d"] = 0.0
        except Exception:
            out["stress_vix_level"] = 0.0
            out["stress_vix_z_60"] = 0.0
            out["stress_vix_change_1d"] = 0.0

        try:
            _cache["items"][key] = (float(now_s), dict(out))
        except Exception as e:
            _warn_nonfatal(
                "TECH_INDICATORS_CACHE_WRITE_FAILED",
                e,
                once_key="cache_write",
                symbol=str(symbol),
                ts_ms=int(ts_ms),
            )

        return out

    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "TECH_INDICATORS_CLOSE_FAILED",
                e,
                once_key="compute_close",
                symbol=str(symbol),
                ts_ms=int(ts_ms),
            )


def get_market_regime_snapshot(symbol: str, ts_ms: int) -> Dict[str, Any]:
    feats = compute_tech_features(str(symbol), int(ts_ms)) or {}
    return {
        "volatility": float(feats.get("market_regime_volatility", 0.0) or 0.0),
        "volatility_baseline": float(feats.get("market_regime_volatility_baseline", 0.0) or 0.0),
        "trend": float(feats.get("market_regime_trend", 0.0) or 0.0),
        "trend_strength": float(feats.get("market_regime_trend_strength", 0.0) or 0.0),
        "label": str(feats.get("market_regime_label") or "mean_reversion"),
    }
