"""
FILE: regime_compat.py

Tracks how well a model performs in the current market regime and converts that
into a multiplier or suppression signal. The logic is fail-open when data is
missing or stale.
"""

import os
import time
from typing import Dict, Any

from engine.runtime.storage import connect
from engine.strategy.model_v2 import get_current_regime

ENABLE = os.environ.get("REGIME_COMPAT_ENABLE", "1") == "1"

DECAY = float(os.environ.get("REGIME_COMPAT_DECAY", "0.97"))
MIN_TRADES = int(os.environ.get("REGIME_COMPAT_MIN_TRADES", "25"))

MULT_MIN = float(os.environ.get("REGIME_COMPAT_MULT_MIN", "0.50"))
MULT_MAX = float(os.environ.get("REGIME_COMPAT_MULT_MAX", "1.50"))

# staleness (days). If stale => neutral (no impact).
MAX_STALE_DAYS = float(os.environ.get("REGIME_COMPAT_MAX_STALE_DAYS", "21"))

# suppression threshold. If computed mult < this AND enough trades AND not stale => suppress (mult=0).
SUPPRESS_BELOW = float(os.environ.get("REGIME_COMPAT_SUPPRESS_BELOW", "0.60"))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _ensure_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS regime_compat_scores (
            model_name TEXT NOT NULL,
            regime TEXT NOT NULL,
            decayed_sum REAL NOT NULL,
            decayed_weight REAL NOT NULL,
            trade_count INTEGER NOT NULL,
            last_update_ts_ms INTEGER NOT NULL,
            PRIMARY KEY (model_name, regime)
        )
        """
    )


def update_regime_compat(*, model_name: str, regime: str, net_return: float) -> None:
    if not ENABLE:
        return

    con = connect()
    try:
        _ensure_tables(con)
        now_ms = _now_ms()

        row = con.execute(
            """
            SELECT decayed_sum, decayed_weight, trade_count
            FROM regime_compat_scores
            WHERE model_name=? AND regime=?
            """,
            (str(model_name), str(regime)),
        ).fetchone()

        if row:
            ds, dw, n = row
            ds = float(ds) * float(DECAY) + float(net_return)
            dw = float(dw) * float(DECAY) + 1.0
            n = int(n) + 1
        else:
            ds, dw, n = float(net_return), 1.0, 1

        con.execute(
            """
            INSERT OR REPLACE INTO regime_compat_scores
            (model_name, regime, decayed_sum, decayed_weight, trade_count, last_update_ts_ms)
            VALUES (?,?,?,?,?,?)
            """,
            (str(model_name), str(regime), float(ds), float(dw), int(n), int(now_ms)),
        )
        con.commit()
    finally:
        con.close()


def regime_compat_multiplier(*, model_name: str, anchor: str = "SPY") -> Dict[str, Any]:
    """
    Returns:
      {
        "mult": float,
        "suppressed": bool,
        "reason": str,
        "regime": str|None,
        "mean_ret": float|None,
        "trade_count": int|None,
        "stale": bool
      }
    """
    if not ENABLE:
        return {"mult": 1.0, "suppressed": False, "reason": "disabled", "stale": False}

    con = connect()
    try:
        _ensure_tables(con)

        regime = str(get_current_regime(anchor) or "MID").upper()

        row = con.execute(
            """
            SELECT decayed_sum, decayed_weight, trade_count, last_update_ts_ms
            FROM regime_compat_scores
            WHERE model_name=? AND regime=?
            """,
            (str(model_name), str(regime)),
        ).fetchone()

        if not row:
            return {
                "mult": 1.0,
                "suppressed": False,
                "reason": "no_data",
                "regime": regime,
                "mean_ret": None,
                "trade_count": None,
                "stale": False,
            }

        ds, dw, n, last_ms = row
        n = int(n or 0)
        dw = float(dw or 0.0)
        last_ms = int(last_ms or 0)

        # Stale compatibility data is treated as neutral so old observations do
        # not keep distorting current sizing.
        stale = False
        if MAX_STALE_DAYS > 0:
            age_ms = _now_ms() - last_ms
            if age_ms > int(MAX_STALE_DAYS * 86400000.0):
                stale = True
                return {
                    "mult": 1.0,
                    "suppressed": False,
                    "reason": "stale",
                    "regime": regime,
                    "mean_ret": None,
                    "trade_count": n,
                    "stale": True,
                }

        if n < int(MIN_TRADES) or dw <= 1e-9:
            return {
                "mult": 1.0,
                "suppressed": False,
                "reason": "insufficient_data",
                "regime": regime,
                "mean_ret": None,
                "trade_count": n,
                "stale": stale,
            }

        mean_ret = float(ds) / float(dw)

        # The return-to-multiplier mapping is intentionally simple and clipped.
        mult = 1.0 + (float(mean_ret) * 10.0)
        mult = _clamp(mult, MULT_MIN, MULT_MAX)

        # suppression gate (only after enough data and not stale)
        if (SUPPRESS_BELOW > 0) and (float(mult) < float(SUPPRESS_BELOW)):
            return {
                "mult": 0.0,
                "suppressed": True,
                "reason": "suppressed_below_threshold",
                "regime": regime,
                "mean_ret": float(mean_ret),
                "trade_count": n,
                "stale": stale,
            }

        return {
            "mult": float(mult),
            "suppressed": False,
            "reason": "ok",
            "regime": regime,
            "mean_ret": float(mean_ret),
            "trade_count": n,
            "stale": stale,
        }
    finally:
        con.close()
