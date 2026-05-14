"""
FILE: confidence_adjust.py

Applies post-model confidence penalties based on stale or volatile market data.
This keeps the model output intact while reducing trust when market context is
less reliable.
"""

import os
import time
import math
import logging
from typing import Dict, Tuple, Any, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

STALE_AFTER_S = int(os.environ.get("CONF_STALE_AFTER_S", "120"))
STALE_HALF_LIFE_S = float(os.environ.get("CONF_STALE_HALF_LIFE_S", "120"))
STALE_MIN_MULT = float(os.environ.get("CONF_STALE_MIN_MULT", "0.25"))

VOL_LOOKBACK_MS = int(os.environ.get("CONF_VOL_LOOKBACK_MS", str(30 * 60 * 1000)))
VOL_MIN_POINTS = int(os.environ.get("CONF_VOL_MIN_POINTS", "8"))
VOL_MAX_PENALTY = float(os.environ.get("CONF_VOL_MAX_PENALTY", "0.20"))  # reduce conf by up to 20%
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("strategy.confidence_adjust")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_confidence_adjust_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.confidence_adjust",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _get_last_price_ts_ms(con, symbol: str) -> Optional[int]:
    row = con.execute(
        "SELECT MAX(ts_ms) FROM prices WHERE symbol=?",
        (symbol,),
    ).fetchone()
    if not row:
        return None
    try:
        v = row[0]
        return int(v) if v is not None else None
    except Exception as e:
        _warn_nonfatal(
            "CONFIDENCE_ADJUST_LAST_PRICE_TS_PARSE_FAILED",
            e,
            once_key="last_price_ts_parse",
            symbol=str(symbol),
        )
        return None


def _stale_multiplier(age_s: float) -> float:
    """
    Exponential decay: multiplier = exp(-age/half_life) after STALE_AFTER_S.
    Clamped to [STALE_MIN_MULT, 1.0].
    """
    if age_s <= float(STALE_AFTER_S):
        return 1.0
    hl = max(1.0, float(STALE_HALF_LIFE_S))
    x = max(0.0, (age_s - float(STALE_AFTER_S)) / hl)
    mult = math.exp(-x)
    if not (mult == mult):  # NaN guard
        mult = 0.0
    return float(max(float(STALE_MIN_MULT), min(1.0, mult)))


def _vol_proxy_multiplier(con, symbol: str, ts_ms: int) -> Tuple[float, Dict[str, Any]]:
    """
    Best-effort volatility proxy: (high-low)/last over recent window.
    Downweights confidence by up to VOL_MAX_PENALTY when proxy is high.
    """
    try:
        rows = con.execute(
            """
            SELECT price
            FROM prices
            WHERE symbol=?
              AND ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 400
            """,
            (symbol, int(ts_ms - VOL_LOOKBACK_MS)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "CONFIDENCE_ADJUST_VOL_QUERY_FAILED",
            e,
            once_key=f"vol_query:{symbol}",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return 1.0, {}

    prices = []
    for r in rows or []:
        try:
            if r and r[0] is not None:
                prices.append(float(r[0]))
        except Exception as e:
            _warn_nonfatal(
                "CONFIDENCE_ADJUST_VOL_PRICE_PARSE_FAILED",
                e,
                once_key="vol_price_parse",
                symbol=str(symbol),
                price_row=str(r)[:200],
            )
            continue

    if len(prices) < int(VOL_MIN_POINTS):
        return 1.0, {"vol_proxy": None, "n": len(prices)}

    hi = max(prices)
    lo = min(prices)
    last = prices[0]
    denom = max(1e-12, abs(last))
    vol_proxy = float((hi - lo) / denom)

    # This mapping is intentionally simple and monotonic so operators can reason
    # about the penalty without reverse-engineering a learned model.
    x = max(0.0, min(1.0, vol_proxy / 0.05))
    penalty = float(VOL_MAX_PENALTY) * x
    mult = float(max(0.0, 1.0 - penalty))

    return mult, {"vol_proxy": vol_proxy, "n": len(prices), "penalty": penalty}


def get_adjusted_confidence(
    con,
    symbol: str,
    horizon_s: int,
    base_conf: float,
) -> Tuple[float, Dict[str, Any]]:
    """
    Returns (adjusted_conf, explain_dict)
    """
    sym = (symbol or "").upper().strip()
    now_ms = _now_ms()

    explain: Dict[str, Any] = {
        "base_conf": float(base_conf),
        "symbol": sym,
        "horizon_s": int(horizon_s),
    }

    last_ts = _get_last_price_ts_ms(con, sym)
    if last_ts is None:
        # No price info -> treat as fully stale
        explain["price_last_ts_ms"] = None
        explain["price_age_s"] = None
        explain["stale_mult"] = float(STALE_MIN_MULT)
        adj = float(base_conf) * float(STALE_MIN_MULT)
        return float(max(0.0, min(1.0, adj))), explain

    age_s = max(0.0, (now_ms - int(last_ts)) / 1000.0)
    stale_mult = _stale_multiplier(age_s)

    explain["price_last_ts_ms"] = int(last_ts)
    explain["price_age_s"] = float(age_s)
    explain["stale_mult"] = float(stale_mult)

    # Optional vol proxy (best-effort)
    vol_mult, vol_ex = _vol_proxy_multiplier(con, sym, now_ms)
    explain["vol"] = vol_ex
    explain["vol_mult"] = float(vol_mult)

    mult = float(stale_mult) * float(vol_mult)
    adj = float(base_conf) * mult

    # Clamp
    adj = float(max(0.0, min(1.0, adj)))
    explain["mult_total"] = float(mult)
    explain["adjusted_conf"] = float(adj)
    return adj, explain
