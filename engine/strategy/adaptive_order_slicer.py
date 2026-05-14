"""
FILE: adaptive_order_slicer.py

Builds a simple execution slicing plan from recent slippage, spread, and
volatility context. This module is intentionally execution-only: it does not
generate alpha, it just decides how aggressively to work an order.
"""

import os
import time
import math
import logging
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("strategy.adaptive_order_slicer")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_adaptive_order_slicer_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.adaptive_order_slicer",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception as e:
        _warn_nonfatal(
            "ADAPTIVE_ORDER_SLICER_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value=repr(x)[:120],
        )
        return default


def _percentile(vals: List[float], p: float) -> Optional[float]:
    if not vals:
        return None
    p = max(0.0, min(1.0, float(p)))
    s = sorted([float(v) for v in vals if v is not None and math.isfinite(float(v))])
    if not s:
        return None
    if len(s) == 1:
        return float(s[0])
    idx = int(round((len(s) - 1) * p))
    idx = max(0, min(len(s) - 1, idx))
    return float(s[idx])


def _mad(vals: List[float]) -> Optional[float]:
    # Median absolute deviation is used instead of variance so a few bad fills
    # do not dominate the stress estimate.
    if not vals:
        return None
    med = _percentile(vals, 0.5)
    if med is None:
        return None
    abs_dev = [abs(float(v) - float(med)) for v in vals if v is not None]
    if not abs_dev:
        return None
    return _percentile(abs_dev, 0.5)


def _liquidity_regime_factor_utc() -> float:
    """
    Time-of-day liquidity regime proxy (UTC-based, exchange-agnostic).
    Caller can override by passing their own regime if needed later.

    Factor >1 => worse liquidity (shrink slices, slow down)
    """
    # Env overrides for quick tuning
    open_factor = _safe_float(os.environ.get("SLICE_REGIME_OPEN_FACTOR", "1.35"), 1.35)
    mid_factor = _safe_float(os.environ.get("SLICE_REGIME_MID_FACTOR", "1.00"), 1.0)
    close_factor = _safe_float(os.environ.get("SLICE_REGIME_CLOSE_FACTOR", "1.25"), 1.25)
    off_factor = _safe_float(os.environ.get("SLICE_REGIME_OFF_FACTOR", "1.50"), 1.5)

    h = int(time.gmtime().tm_hour)

    # This is only a coarse liquidity regime proxy; callers can still layer more
    # specific venue/session logic on top later.
    # 13–15 UTC ~ open (winter) / 14–16 (summer) — treat as "open band"
    # 19–21 UTC ~ close band
    if 13 <= h <= 15:
        return float(open_factor)
    if 19 <= h <= 21:
        return float(close_factor)
    if 16 <= h <= 18:
        return float(mid_factor)
    return float(off_factor)


class AdaptiveOrderSlicer:
    def __init__(
        self,
        *,
        recent_slippage_bps: Optional[List[float]] = None,
        spread_bps: float = 0.0,
        volatility_bps: float = 0.0,
        symbol: Optional[str] = None,
        broker: Optional[str] = None,
    ) -> None:
        self.recent = list(recent_slippage_bps or [])
        self.spread_bps = _safe_float(spread_bps, 0.0)
        self.vol_bps = _safe_float(volatility_bps, 0.0)
        self.symbol = symbol
        self.broker = broker

        # Controls
        self.min_slice_qty = _safe_float(os.environ.get("SLICE_MIN_QTY", "1"), 1.0)
        self.max_slice_pct = _safe_float(os.environ.get("SLICE_MAX_PCT", "0.25"), 0.25)
        self.min_slice_pct = _safe_float(os.environ.get("SLICE_MIN_PCT", "0.02"), 0.02)

        self.base_delay_ms = int(_safe_float(os.environ.get("SLICE_BASE_DELAY_MS", "120"), 120.0))
        self.max_delay_ms = int(_safe_float(os.environ.get("SLICE_MAX_DELAY_MS", "2000"), 2000.0))

        # Stress thresholds (variance/tails)
        self.var_stress_mad_bps = _safe_float(os.environ.get("SLICE_VAR_STRESS_MAD_BPS", "6.0"), 6.0)
        self.tail_stress_p95_bps = _safe_float(os.environ.get("SLICE_TAIL_STRESS_P95_BPS", "12.0"), 12.0)
        self.abort_p99_bps = _safe_float(os.environ.get("SLICE_ABORT_P99_BPS", "30.0"), 30.0)
        self.abort_spread_bps = _safe_float(os.environ.get("SLICE_ABORT_SPREAD_BPS", "25.0"), 25.0)

        # Spread/vol impact weights
        self.spread_weight = _safe_float(os.environ.get("SLICE_SPREAD_WEIGHT", "0.35"), 0.35)
        self.vol_weight = _safe_float(os.environ.get("SLICE_VOL_WEIGHT", "0.25"), 0.25)

    def compute_slice_plan(self, *, remaining_qty: float) -> Dict[str, Any]:
        rem = abs(_safe_float(remaining_qty, 0.0))
        if rem <= 0.0:
            return {
                "ok": True,
                "abort": False,
                "slow_down": False,
                "slice_qty": 0.0,
                "delay_ms": 0,
                "audit": {"ts_ms": _now_ms()},
            }

        # Robust distribution stats
        mad = _mad(self.recent) or 0.0
        p95 = _percentile(self.recent, 0.95) or 0.0
        p99 = _percentile(self.recent, 0.99) or 0.0

        regime = _liquidity_regime_factor_utc()

        # Stress score (variance + tails + spread + vol)
        stress = 0.0
        if mad >= self.var_stress_mad_bps:
            stress += 1.0
        if p95 >= self.tail_stress_p95_bps:
            stress += 1.0

        stress += min(2.0, max(0.0, self.spread_bps) / max(1e-9, self.abort_spread_bps)) * self.spread_weight
        stress += min(2.0, max(0.0, self.vol_bps) / 25.0) * self.vol_weight  # 25bps vol proxy scaling

        # Hard abort gates (no prediction)
        abort = False
        abort_reason = None
        if self.spread_bps >= self.abort_spread_bps:
            abort = True
            abort_reason = "spread_abort"
        if p99 >= self.abort_p99_bps:
            abort = True
            abort_reason = abort_reason or "p99_slippage_abort"

        # Base slice pct shrinks with stress & regime
        # Higher stress => smaller pct
        pct = self.max_slice_pct
        pct = pct / max(1.0, (1.0 + stress))
        pct = pct / max(1.0, float(regime))

        pct = max(self.min_slice_pct, min(self.max_slice_pct, pct))

        # Slice qty
        slice_qty = max(self.min_slice_qty, rem * pct)
        slice_qty = min(rem, slice_qty)

        # Timing: slow down when stressed
        slow_down = (stress >= 1.0) or (regime >= 1.25)
        delay = int(self.base_delay_ms * (1.0 + 0.65 * stress) * float(regime))
        delay = max(0, min(self.max_delay_ms, delay))

        return {
            "ok": True,
            "abort": bool(abort),
            "abort_reason": abort_reason,
            "slow_down": bool(slow_down),
            "slice_qty": float(slice_qty),
            "delay_ms": int(delay),
            "audit": {
                "ts_ms": _now_ms(),
                "symbol": self.symbol,
                "broker": self.broker,
                "recent_n": int(len(self.recent)),
                "slip_mad_bps": float(mad),
                "slip_p95_bps": float(p95),
                "slip_p99_bps": float(p99),
                "spread_bps": float(self.spread_bps),
                "vol_bps": float(self.vol_bps),
                "regime_factor": float(regime),
                "stress_score": float(stress),
                "slice_pct": float(pct),
                "delay_ms": int(delay),
            },
        }
