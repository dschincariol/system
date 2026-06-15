"""Opt-in Almgren-Chriss style impact estimates for simulation and validation."""

from __future__ import annotations

import math
import os
from typing import Any, Dict, Optional

from engine.execution.execution_liquidity_model import get_execution_liquidity_snapshot
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


LOG = get_logger("engine.execution.almgren_chriss")
_WARNED_NONFATAL_KEYS: set[str] = set()

_ENABLED = os.environ.get("ALMGREN_CHRISS_ENABLED", "0") == "1"
_TEMP_COEF = float(os.environ.get("ALMGREN_CHRISS_TEMP_COEF", "0.15"))
_PERM_COEF = float(os.environ.get("ALMGREN_CHRISS_PERM_COEF", "0.05"))
_RISK_AVERSION = float(os.environ.get("ALMGREN_CHRISS_RISK_AVERSION", "0.0"))
_EXEC_HORIZON_S = int(os.environ.get("ALMGREN_CHRISS_EXEC_HORIZON_S", "900"))
_FALLBACK_VOL_BPS = float(os.environ.get("ALMGREN_CHRISS_FALLBACK_VOL_BPS", "10.0"))
_MIN_PARTICIPATION = float(os.environ.get("ALMGREN_CHRISS_MIN_PARTICIPATION", "1e-6"))
_LIVE_PARTICIPATION_MULT = float(os.environ.get("ALMGREN_CHRISS_LIVE_PARTICIPATION_MULT", "0.20"))
_MAX_IMPACT_BPS = float(os.environ.get("ALMGREN_CHRISS_MAX_IMPACT_BPS", "75.0"))


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=code,
        message=code,
        error=error,
        level=30,
        component="engine.execution.almgren_chriss",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def almgren_chriss_enabled() -> bool:
    """Return whether Almgren-Chriss cost estimation is enabled."""
    return bool(_ENABLED)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        _warn_nonfatal(
            "ALMGREN_CHRISS_FLOAT_PARSE_FAILED",
            exc,
            once_key=f"safe_float:{repr(value)[:80]}",
            value=repr(value)[:240],
            default=float(default),
        )
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _clamp(value: Any, lo: float, hi: float) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        _warn_nonfatal(
            "ALMGREN_CHRISS_CLAMP_PARSE_FAILED",
            exc,
            once_key=f"clamp:{repr(value)[:80]}",
            value=repr(value)[:240],
            low=float(lo),
            high=float(hi),
        )
        out = float(lo)
    if not math.isfinite(out):
        out = float(lo)
    return float(max(float(lo), min(float(hi), float(out))))


def estimate_almgren_chriss_costs(
    *,
    symbol: str,
    qty: float,
    px: float,
    side: int | str,
    ts_ms: Optional[int] = None,
    liquidity_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Estimate bounded temporary, permanent, and risk costs for one order."""
    qty_abs = abs(_safe_float(qty, 0.0))
    px_f = _safe_float(px, 0.0)
    sym = str(symbol or "").upper().strip()
    side_text = str(side or "").upper().strip()
    side_sign = -1.0 if side_text in {"-1", "SELL", "SHORT"} or _safe_float(side, 1.0) < 0.0 else 1.0

    result: Dict[str, Any] = {
        "ok": False,
        "enabled": bool(_ENABLED),
        "model": "almgren_chriss",
        "symbol": sym,
        "side": ("SELL" if side_sign < 0.0 else "BUY"),
        "qty_abs": float(qty_abs),
        "px": float(px_f),
        "temporary_impact_bps": 0.0,
        "permanent_impact_bps": 0.0,
        "risk_term_bps": 0.0,
        "execution_cost_bps": 0.0,
        "participation_rate": 0.0,
        "rolling_adv": 0.0,
        "live_participation_rate": 0.0,
        "intraday_vol_bps": 0.0,
        "true_spread_bps": 0.0,
        "exec_horizon_s": int(max(1, _EXEC_HORIZON_S)),
    }
    if not _ENABLED or not sym or qty_abs <= 0.0 or px_f <= 0.0:
        return result

    snapshot = dict(liquidity_snapshot or {})
    if not snapshot:
        try:
            snapshot = dict(
                get_execution_liquidity_snapshot(
                    sym,
                    qty=float(qty_abs),
                    px=float(px_f),
                    ts_ms=(int(ts_ms) if ts_ms is not None else None),
                )
                or {}
            )
        except Exception as e:
            _warn_nonfatal(
                "ALMGREN_CHRISS_LIQUIDITY_SNAPSHOT_FAILED",
                e,
                once_key=f"ac_liquidity_snapshot:{sym}",
                symbol=str(sym),
            )
            return result

    adv = max(0.0, _safe_float(snapshot.get("rolling_adv"), 0.0))
    adv_participation = max(0.0, _safe_float(snapshot.get("adv_participation"), 0.0))
    live_participation = max(0.0, _safe_float(snapshot.get("live_participation_rate"), 0.0))
    spread_bps = max(0.0, _safe_float(snapshot.get("true_spread_bps"), 0.0))
    vol_bps = max(0.0, _safe_float(snapshot.get("intraday_vol_bps"), _FALLBACK_VOL_BPS))
    interval_mult = max(1.0, _safe_float(snapshot.get("interval_mult"), 1.0))

    vol_dec = max(1e-6, float(vol_bps) / 10000.0)
    participation = max(
        float(_MIN_PARTICIPATION),
        min(
            1.0,
            max(
                float(adv_participation),
                float(min(1.0, live_participation) * max(0.0, float(_LIVE_PARTICIPATION_MULT))),
            ),
        ),
    )

    # Use a bounded urgency proxy so more urgent slicing raises temporary cost
    # without making the model unstable when quote volume is sparse.
    urgency = math.sqrt(max(1.0, float(interval_mult)))
    temporary = max(0.0, float(_TEMP_COEF) * float(vol_bps) * math.sqrt(float(participation)) * float(urgency))
    permanent = max(0.0, float(_PERM_COEF) * float(vol_bps) * float(participation))
    risk_term = max(
        0.0,
        float(_RISK_AVERSION) * float(vol_dec) * math.sqrt(float(max(1, _EXEC_HORIZON_S)) / 86400.0) * 10000.0,
    )
    execution_cost_bps = min(float(_MAX_IMPACT_BPS), temporary + (0.5 * permanent) + risk_term)

    result.update(
        {
            "ok": True,
            "participation_rate": float(participation),
            "rolling_adv": float(adv),
            "live_participation_rate": float(live_participation),
            "intraday_vol_bps": float(vol_bps),
            "true_spread_bps": float(spread_bps),
            "temporary_impact_bps": float(temporary),
            "permanent_impact_bps": float(permanent),
            "risk_term_bps": float(risk_term),
            "execution_cost_bps": float(execution_cost_bps),
            "liquidity_snapshot": snapshot,
        }
    )
    return result


__all__ = [
    "almgren_chriss_enabled",
    "estimate_almgren_chriss_costs",
]
