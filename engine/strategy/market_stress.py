"""
FILE: market_stress.py

Computes a cross-asset market stress snapshot from local price series. The
output is a normalized, read-only diagnostic used by sizing and capital guards.
"""

import logging
import math
import time
from typing import Dict, Optional, Tuple, List

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect


_LOOKBACK = 240   # samples
_ZWIN = 120       # samples for zscore window
LOG = get_logger("engine.strategy.market_stress")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="market_stress_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.market_stress",
        extra=extra or None,
        persist=False,
    )

def _load_prices(con, symbol: str, ts_ms: int, n: int) -> List[Tuple[int, float]]:
    rows = con.execute(
        """
        SELECT ts_ms, price
        FROM prices
        WHERE symbol=? AND ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (str(symbol), int(ts_ms), int(n)),
    ).fetchall()
    out = []
    for t, p in (rows or []):
        if t is None or p is None:
            continue
        try:
            out.append((int(t), float(p)))
        except Exception as e:
            _warn_nonfatal("MARKET_STRESS_PRICE_PARSE_FAILED", e, symbol=str(symbol), ts_ms=int(ts_ms))
            continue
    out.reverse()
    return out


def _zscore_last(px: np.ndarray, win: int) -> float:
    px = np.asarray(px, dtype=float)
    if px.size < max(20, int(win)):
        return 0.0
    w = px[-int(win):]
    mu = float(np.mean(w))
    sd = float(np.std(w, ddof=1)) if w.size >= 3 else 0.0
    if sd <= 1e-12:
        return 0.0
    z = (float(w[-1]) - mu) / sd
    if not math.isfinite(z):
        return 0.0
    return float(z)


def _safe_last(px: np.ndarray) -> float:
    try:
        arr = np.asarray(px, dtype=float)
    except Exception:
        arr = np.asarray([], dtype=float)
    if arr.size <= 0:
        return 0.0
    try:
        v = float(arr[-1])
        return v if math.isfinite(v) else 0.0
    except Exception as e:
        _warn_nonfatal("MARKET_STRESS_LAST_VALUE_FAILED", e, size=int(arr.size))
        return 0.0


def _ratio(a: float, b: float) -> float:
    a = float(a)
    b = float(b)
    if b <= 1e-12:
        return 0.0
    r = a / b
    if not math.isfinite(r):
        return 0.0
    return float(r)

def get_market_stress_snapshot(con=None, ts_ms: Optional[int] = None) -> Dict[str, float]:
    """
    Pure read-only snapshot computed from the local `prices` table.
    Returns a stable dict suitable for /api exposure and for explain_json annotations.

    Stress score is 0..1 (higher = more stress).
    """
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)

        # ---- load series
        def load(sym: str) -> np.ndarray:
            s = _load_prices(con, sym, int(ts_ms), _LOOKBACK)
            return np.asarray([p for _, p in s], dtype=float)

        vix = load("VIX")
        vix1d = load("VIX1D")
        vix9d = load("VIX9D")
        vix3m = load("VIX3M")
        vvix = load("VVIX")
        move = load("MOVE")

        hyg = load("HYG")
        lqd = load("LQD")
        tlt = load("TLT")
        shy = load("SHY")

        # ---- levels
        vix_last = _safe_last(vix)
        vvix_last = _safe_last(vvix)
        move_last = _safe_last(move)

        # ---- term structure ratios (shape)
        vix1d_last = _safe_last(vix1d)
        vix9d_last = _safe_last(vix9d)
        vix3m_last = _safe_last(vix3m)

        ts_1d = _ratio(vix1d_last, vix_last)
        ts_9d = _ratio(vix9d_last, vix_last)
        ts_3m = _ratio(vix3m_last, vix_last)

        # ---- credit proxy: HY vs IG
        hyg_last = _safe_last(hyg)
        lqd_last = _safe_last(lqd)
        credit_ratio = _ratio(lqd_last, hyg_last)  # rises when HY underperforms IG

        # ---- rates proxy: long vs short treasury (risk-off)
        tlt_last = _safe_last(tlt)
        shy_last = _safe_last(shy)
        rates_ratio = _ratio(tlt_last, shy_last)

        # ---- zscores (comparable scale)
        z_vix = _zscore_last(vix, _ZWIN)
        z_vvix = _zscore_last(vvix, _ZWIN)
        z_move = _zscore_last(move, _ZWIN)

        # ---- term structure z-scores (vectorized, no extra DB reads)
        n_vix = int(min(vix.size, vix1d.size, vix9d.size, vix3m.size))
        if n_vix >= 20:
            v = np.maximum(vix[-n_vix:], 1e-12)
            ts1 = np.asarray([_ratio(float(vix1d[-n_vix + i]), float(v[i])) for i in range(n_vix)], dtype=float)
            ts9 = np.asarray([_ratio(float(vix9d[-n_vix + i]), float(v[i])) for i in range(n_vix)], dtype=float)
            ts3 = np.asarray([_ratio(float(vix3m[-n_vix + i]), float(v[i])) for i in range(n_vix)], dtype=float)

            z_ts_1d = _zscore_last(ts1, min(_ZWIN, ts1.size))
            z_ts_9d = _zscore_last(ts9, min(_ZWIN, ts9.size))
            z_ts_3m = _zscore_last(ts3, min(_ZWIN, ts3.size))
        else:
            z_ts_1d = 0.0
            z_ts_9d = 0.0
            z_ts_3m = 0.0

        # ---- credit + rates z-scores (vectorized, aligned lengths)
        n_credit = int(min(hyg.size, lqd.size))
        if n_credit >= 20:
            cr = np.asarray([_ratio(float(lqd[-n_credit + i]), float(hyg[-n_credit + i])) for i in range(n_credit)], dtype=float)
            z_credit = _zscore_last(cr, min(_ZWIN, cr.size))
        else:
            z_credit = 0.0

        n_rates = int(min(tlt.size, shy.size))
        if n_rates >= 20:
            rr = np.asarray([_ratio(float(tlt[-n_rates + i]), float(shy[-n_rates + i])) for i in range(n_rates)], dtype=float)
            z_rates = _zscore_last(rr, min(_ZWIN, rr.size))
        else:
            z_rates = 0.0

        # The stress score intentionally blends several asset classes so no
        # single series like VIX can dominate the entire signal.
        w = {
            "vix": 0.30,
            "vvix": 0.20,
            "move": 0.20,
            "term": 0.15,
            "credit": 0.10,
            "rates": 0.05,
        }
        term_z = (z_ts_1d + z_ts_9d + z_ts_3m) / 3.0
        raw = (
            w["vix"] * z_vix
            + w["vvix"] * z_vvix
            + w["move"] * z_move
            + w["term"] * term_z
            + w["credit"] * z_credit
            + w["rates"] * z_rates
        )
        # squash into 0..1
        score = 1.0 / (1.0 + math.exp(-float(raw) / 2.0)) if math.isfinite(raw) else 0.5

        out = {
            "ts_ms": int(ts_ms),

            "vix": float(vix_last),
            "vvix": float(vvix_last),
            "move": float(move_last),

            "vix1d_over_vix": float(ts_1d),
            "vix9d_over_vix": float(ts_9d),
            "vix3m_over_vix": float(ts_3m),

            "credit_lqd_over_hyg": float(credit_ratio),
            "rates_tlt_over_shy": float(rates_ratio),

            "z_vix": float(z_vix),
            "z_vvix": float(z_vvix),
            "z_move": float(z_move),
            "z_term": float(term_z),
            "z_credit": float(z_credit),
            "z_rates": float(z_rates),

            "stress_score": float(max(0.0, min(1.0, score))),
        }

        # Optional: macro narrative stress from GDELT (best-effort, no failures)
        try:
            from engine.data.gdelt_macro import get_gdelt_macro_snapshot
            gm = get_gdelt_macro_snapshot(ts_ms=int(ts_ms or int(time.time() * 1000))) or {}
            if gm:
                out["z_gdelt_doc"] = float(gm.get("z_doc_count", 0.0))
                out["z_gdelt_tone"] = float(gm.get("z_tone_mean", 0.0))
                out["z_gdelt_conflict"] = float(gm.get("z_conflict_share", 0.0))
                # Conservative bump: only conflict increases stress; tone is ambiguous
                out["stress_score"] = float(out["stress_score"]) + max(0.0, float(out["z_gdelt_conflict"]))
        except Exception as e:
            _warn_nonfatal("MARKET_STRESS_GDELT_MACRO_SNAPSHOT_FAILED", e, ts_ms=int(ts_ms))

        return out

    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("MARKET_STRESS_CLOSE_FAILED", e, operation="get_market_stress_snapshot")

