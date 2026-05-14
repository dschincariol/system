"""
FILE: regime_size.py

Computes a regime-aware capital scaling factor from realized volatility, VIX
stress, and drawdown context. This is a deterministic gross-exposure modifier
applied after base portfolio weights are built.
"""

import os
import math
import logging
from typing import Dict, Any, Tuple, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.strategy.model_v2 import get_current_regime, classify_regime
from engine.runtime.storage import connect

LOG = logging.getLogger("regime_size")


def _warn_nonfatal(event: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.regime_size",
        extra=extra,
        persist=False,
    )

USE = os.environ.get("PORTFOLIO_REGIME_SCALE_ENABLE", "1") == "1"

ANCHOR = os.environ.get("PORTFOLIO_REGIME_ANCHOR", "SPY").strip().upper()

M_LOW = float(os.environ.get("PORTFOLIO_REGIME_MULT_LOW", "1.10"))
M_MID = float(os.environ.get("PORTFOLIO_REGIME_MULT_MID", "1.00"))
M_HIGH = float(os.environ.get("PORTFOLIO_REGIME_MULT_HIGH", "0.70"))

CONF_EN = os.environ.get("PORTFOLIO_REGIME_CONF_ENABLE", "1") == "1"
CONF_FLOOR = float(os.environ.get("PORTFOLIO_REGIME_CONF_FLOOR", "0.65"))
CONF_BAND = float(os.environ.get("PORTFOLIO_REGIME_CONF_BAND", "0.002"))

VIX_EN = os.environ.get("PORTFOLIO_REGIME_VIX_ENABLE", "1") == "1"
VIX_Z_TH = float(os.environ.get("PORTFOLIO_REGIME_VIX_Z_TH", "2.0"))
VIX_Z_AT_MIN = float(os.environ.get("PORTFOLIO_REGIME_VIX_Z_AT_MIN", "4.0"))
VIX_MIN = float(os.environ.get("PORTFOLIO_REGIME_VIX_MIN", "0.60"))

DD_EN = os.environ.get("PORTFOLIO_REGIME_DD_ENABLE", "1") == "1"
DD_LOOKBACK_D = int(os.environ.get("PORTFOLIO_REGIME_DD_LOOKBACK_D", "14"))
DD_TH = float(os.environ.get("PORTFOLIO_REGIME_DD_TH", "0.06"))
DD_AT_MIN = float(os.environ.get("PORTFOLIO_REGIME_DD_AT_MIN", "0.15"))
DD_MIN = float(os.environ.get("PORTFOLIO_REGIME_DD_MIN", "0.55"))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _stdev(xs):
    xs = [float(x) for x in (xs or [])]
    n = len(xs)
    if n < 10:
        return None
    m = sum(xs) / n
    v = sum((x - m) * (x - m) for x in xs) / (n - 1)
    s = (v ** 0.5) if v > 0 else 0.0
    return float(s)


def _anchor_realized_vol(con, symbol: str, lookback: int = 120) -> Optional[float]:
    try:
        rows = con.execute(
            """
            SELECT price
            FROM prices
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(lookback)),
        ).fetchall() or []
        px = [float(r[0]) for r in rows if r and r[0] is not None]
        if len(px) < 30:
            return None
        rets = []
        for i in range(1, len(px)):
            if px[i - 1] <= 0:
                continue
            rets.append((px[i] / px[i - 1]) - 1.0)
        if len(rets) < 25:
            return None
        # This keeps the anchor-vol proxy aligned with the regime classifier
        # used elsewhere in the strategy stack.
        vol = float(math.sqrt(sum(r * r for r in rets) / len(rets)))
        if not math.isfinite(vol) or vol <= 0:
            return None
        return float(vol)
    except Exception as e:
        _warn_nonfatal("regime_size_volatility_parse_failed", e, symbol=str(symbol), lookback=int(lookback))
        return None


def _regime_confidence(vol: Optional[float]) -> float:
    """
    Confidence in current regime based on distance from nearest boundary.
    LOW/MID/HIGH boundaries per model_v2.classify_regime thresholds:
      LOW  if vol < 0.004
      HIGH if vol > 0.012
      MID  otherwise
    Near boundary => lower confidence => scale toward CONF_FLOOR.
    """
    if (vol is None) or (not math.isfinite(float(vol))) or (float(vol) <= 0):
        return 1.0

    v = float(vol)
    # boundaries
    low_hi = 0.004
    mid_hi = 0.012

    reg = classify_regime(v)

    if reg == "LOW":
        dist = abs(low_hi - v)
    elif reg == "HIGH":
        dist = abs(v - mid_hi)
    else:
        dist = min(abs(v - low_hi), abs(mid_hi - v))

    band = max(1e-9, float(CONF_BAND))
    conf = _clamp(dist / band, 0.0, 1.0)
    return float(conf)


def _vix_z(con, lookback: int = 180) -> Optional[float]:
    try:
        rows = con.execute(
            """
            SELECT price
            FROM prices
            WHERE symbol='VIX'
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(lookback),),
        ).fetchall() or []
        v = [float(r[0]) for r in rows if r and r[0] is not None]
        if len(v) < 40:
            return None
        cur = float(v[0])
        hist = list(reversed(v[1:]))
        mu = sum(hist) / len(hist)
        sd = _stdev(hist)
        if sd is None or sd <= 1e-9:
            return None
        z = (cur - mu) / sd
        if not math.isfinite(z):
            return None
        return float(z)
    except Exception as e:
        _warn_nonfatal("regime_size_momentum_parse_failed", e, once_key="momentum_parse", value=str(rows)[:200])
        return None


def _rolling_drawdown(con, lookback_days: int) -> Optional[float]:
    """
    Drawdown fraction over recent portfolio_bt_points.
    Uses equity curve: dd = (peak - last)/peak.
    """
    try:
        now_ms = int(con.execute("SELECT CAST(strftime('%s','now') AS INTEGER)*1000").fetchone()[0])
        min_ts = int(now_ms - int(lookback_days) * 86400000)

        rows = con.execute(
            """
            SELECT equity
            FROM portfolio_bt_points
            WHERE ts_ms >= ?
            ORDER BY ts_ms ASC
            """,
            (int(min_ts),),
        ).fetchall() or []
        eq = [float(r[0]) for r in rows if r and r[0] is not None]
        if len(eq) < 10:
            return None
        peak = max(eq)
        last = eq[-1]
        if peak <= 0:
            return None
        dd = (peak - last) / peak
        if not math.isfinite(dd):
            return None
        return float(max(0.0, dd))
    except Exception as e:
        _warn_nonfatal("regime_size_drawdown_parse_failed", e, once_key="drawdown_parse")
        return None


def regime_multiplier(con=None, anchor: Optional[str] = None) -> Tuple[str, float]:
    """
    Backward-compatible: returns (regime, base_multiplier) only.
    """
    if not USE:
        return "MID", 1.0

    a = str(anchor or ANCHOR).strip().upper()
    try:
        reg = str(get_current_regime(a) or "MID").upper().strip()
    except Exception:
        reg = "MID"

    if reg == "LOW":
        return reg, float(M_LOW)
    if reg == "HIGH":
        return reg, float(M_HIGH)
    return "MID", float(M_MID)


def regime_capital_scale(con=None, anchor: Optional[str] = None) -> Dict[str, Any]:
    """
    Full regime-adaptive capital scale factor + explain meta.
    Returns:
      {
        "ok": True,
        "anchor": "...",
        "regime": "LOW|MID|HIGH",
        "vol": float|None,
        "base_mult": float,
        "conf": float,
        "conf_mult": float,
        "vix_z": float|None,
        "vix_mult": float,
        "dd": float|None,
        "dd_mult": float,
        "final_mult": float
      }
    """
    if not USE:
        return {
            "ok": True,
            "anchor": str(anchor or ANCHOR),
            "regime": "MID",
            "vol": None,
            "base_mult": 1.0,
            "conf": 1.0,
            "conf_mult": 1.0,
            "vix_z": None,
            "vix_mult": 1.0,
            "dd": None,
            "dd_mult": 1.0,
            "final_mult": 1.0,
        }

    owns = False
    if con is None:
        con = connect()
        owns = True

    try:
        a = str(anchor or ANCHOR).strip().upper()

        # base regime
        try:
            reg = str(get_current_regime(a) or "MID").upper().strip()
        except Exception:
            reg = "MID"

        if reg == "LOW":
            base = float(M_LOW)
        elif reg == "HIGH":
            base = float(M_HIGH)
        else:
            reg = "MID"
            base = float(M_MID)

        # confidence from anchor vol distance to boundary
        vol = _anchor_realized_vol(con, a)
        conf = 1.0
        conf_mult = 1.0
        if CONF_EN:
            conf = _regime_confidence(vol)
            # interpolate from CONF_FLOOR..1.0
            conf_mult = float(CONF_FLOOR + (1.0 - CONF_FLOOR) * float(conf))

        # VIX stress scaling
        vz = None
        vix_mult = 1.0
        if VIX_EN:
            vz = _vix_z(con)
            if (vz is not None) and math.isfinite(float(vz)):
                z = float(vz)
                if z > float(VIX_Z_TH):
                    # map z in [th..at_min] -> factor in [1..VIX_MIN]
                    z0 = float(VIX_Z_TH)
                    z1 = max(z0 + 1e-9, float(VIX_Z_AT_MIN))
                    t = _clamp((z - z0) / (z1 - z0), 0.0, 1.0)
                    vix_mult = float(1.0 + (float(VIX_MIN) - 1.0) * t)

        # drawdown scaling
        dd = None
        dd_mult = 1.0
        if DD_EN:
            dd = _rolling_drawdown(con, lookback_days=int(DD_LOOKBACK_D))
            if (dd is not None) and math.isfinite(float(dd)):
                d = float(dd)
                if d > float(DD_TH):
                    d0 = float(DD_TH)
                    d1 = max(d0 + 1e-9, float(DD_AT_MIN))
                    t = _clamp((d - d0) / (d1 - d0), 0.0, 1.0)
                    dd_mult = float(1.0 + (float(DD_MIN) - 1.0) * t)

        final = float(base) * float(conf_mult) * float(vix_mult) * float(dd_mult)
        # hard clamp to prevent uncontrolled scaling
        final = _clamp(final, 0.10, 1.50)

        return {
            "ok": True,
            "anchor": str(a),
            "regime": str(reg),
            "vol": (None if vol is None else float(vol)),
            "base_mult": float(base),
            "conf": float(conf),
            "conf_mult": float(conf_mult),
            "vix_z": (None if vz is None else float(vz)),
            "vix_mult": float(vix_mult),
            "dd": (None if dd is None else float(dd)),
            "dd_mult": float(dd_mult),
            "final_mult": float(final),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("regime_size_db_close_failed", e)
