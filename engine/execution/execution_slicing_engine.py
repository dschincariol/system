"""
FILE: execution_slicing_engine.py

Execution subsystem module for `execution_slicing_engine`.
"""

import math
import os
import time
import logging
from typing import Any, Dict, List

from engine.execution.execution_liquidity_model import get_execution_liquidity_snapshot
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.execution.execution_slicing_engine")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="execution_slicing_engine_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.execution_slicing_engine",
        extra=extra or None,
        persist=False,
    )


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception as e:
        _warn_nonfatal("EXECUTION_SLICING_ENGINE_SAFE_FLOAT_FAILED", e, value=repr(v), default=default)
        return default


def _optional_int(v: Any) -> int | None:
    try:
        if v in (None, ""):
            return None
        return int(v)
    except Exception as e:
        _warn_nonfatal("EXECUTION_SLICING_ENGINE_OPTIONAL_INT_FAILED", e, value=repr(v))
        return None


def _style_from_order(order: Dict[str, Any]) -> str:
    # Allow several upstream aliases so intent producers can evolve without
    # breaking the slicer contract.
    style = str(
        order.get("slice_style")
        or order.get("execution_style")
        or order.get("algo")
        or order.get("algo_style")
        or ""
    ).strip().lower()
    if style in {"twap", "vwap", "pov", "adaptive"}:
        return style

    default_style = str(os.environ.get("EXEC_DEFAULT_SLICE_STYLE", "")).strip().lower()
    if default_style in {"twap", "vwap", "pov", "adaptive"}:
        return default_style
    return ""


def _base_slice_qty(remaining_qty: float, style: str, liquidity: Dict[str, Any], order: Dict[str, Any]) -> float:
    rem = abs(_safe_float(remaining_qty, 0.0))
    if rem <= 0.0:
        return 0.0

    configured = _safe_float(order.get("slice_qty") or order.get("slice_size") or 0.0, 0.0)
    if configured > 0.0:
        return min(rem, configured)

    pct = _safe_float(order.get("slice_pct") or 0.0, 0.0)
    if pct > 0.0:
        return min(rem, max(1.0, rem * pct))

    default_pct = _safe_float(os.environ.get("EXEC_SLICE_DEFAULT_PCT", "0.20"), 0.20)
    if style == "twap":
        default_pct = _safe_float(os.environ.get("EXEC_TWAP_SLICE_PCT", "0.12"), 0.12)
    elif style == "vwap":
        default_pct = _safe_float(os.environ.get("EXEC_VWAP_SLICE_PCT", "0.18"), 0.18)
    elif style == "pov":
        adv = float(liquidity.get("rolling_adv") or 0.0)
        pov = _safe_float(
            order.get("target_participation")
            or order.get("pov_participation")
            or os.environ.get("EXEC_POV_PARTICIPATION", "0.03"),
            0.03,
        )
        if adv > 0.0:
            return min(rem, max(1.0, adv * pov))
        default_pct = _safe_float(os.environ.get("EXEC_POV_FALLBACK_SLICE_PCT", "0.08"), 0.08)
    elif style == "adaptive":
        default_pct = _safe_float(os.environ.get("EXEC_ADAPTIVE_SLICE_PCT", "0.15"), 0.15)

    # Liquidity context acts as a multiplier on the configured/default slice
    # size, which makes slicing adaptive without changing order intent shape.
    mult = float(liquidity.get("slice_size_mult") or 1.0)
    qty = rem * default_pct * mult
    return min(rem, max(1.0, qty))


def _recent_volume_buckets(symbol: str, bucket_ms: int, bucket_count: int) -> List[float]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return []

    lookback_ms = int(max(1, bucket_ms) * max(1, bucket_count))
    now_ms = int(time.time() * 1000)
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT ts_ms, volume
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms >= ?
              AND volume IS NOT NULL
            ORDER BY ts_ms ASC
            """,
            (sym, int(now_ms - lookback_ms)),
        ).fetchall()
    except Exception:
        rows = []
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("EXECUTION_SLICING_ENGINE_CLOSE_FAILED", e, symbol=str(sym))

    if not rows:
        return []

    buckets = [0.0 for _ in range(int(bucket_count))]
    prev_volume = None
    for ts_ms, volume in rows:
        v = _safe_float(volume, 0.0)
        if v <= 0.0:
            continue
        idx = int((int(ts_ms) - int(now_ms - lookback_ms)) // int(bucket_ms))
        if idx < 0 or idx >= int(bucket_count):
            continue
        if prev_volume is None:
            delta_v = v
        else:
            delta_v = v - prev_volume
            if delta_v < 0.0:
                delta_v = v
        prev_volume = v
        if delta_v > 0.0:
            buckets[idx] += float(delta_v)
    return buckets


def _target_slice_count(total_qty: float, base_qty: float, interval_ms: int, order: Dict[str, Any]) -> int:
    configured = int(max(0.0, _safe_float(order.get("max_slices") or order.get("slice_count") or 0.0, 0.0)))
    if configured > 0:
        return max(1, min(configured, int(os.environ.get("EXEC_MAX_SLICES_PER_ORDER", "64"))))

    duration_ms = int(max(0.0, _safe_float(order.get("slice_duration_ms") or order.get("duration_ms") or 0.0, 0.0)))
    if duration_ms > 0 and interval_ms > 0:
        return max(1, min(int(math.ceil(float(duration_ms) / float(interval_ms))), int(os.environ.get("EXEC_MAX_SLICES_PER_ORDER", "64"))))

    if base_qty > 0.0:
        return max(1, min(int(math.ceil(abs(float(total_qty)) / float(base_qty))), int(os.environ.get("EXEC_MAX_SLICES_PER_ORDER", "64"))))

    return 1


def _normalize_weights(weights: List[float], count: int) -> List[float]:
    w = [max(0.0, _safe_float(v, 0.0)) for v in list(weights or [])[: int(count)]]
    while len(w) < int(count):
        w.append(0.0)
    s = sum(w)
    if s <= 0.0:
        return [1.0 / float(max(1, count)) for _ in range(int(count))]
    return [float(v) / float(s) for v in w]


def _slice_weights(style: str, symbol: str, qty_abs: float, base_qty: float, interval_ms: int, liquidity: Dict[str, Any], order: Dict[str, Any]) -> List[float]:
    count = _target_slice_count(total_qty=qty_abs, base_qty=base_qty, interval_ms=interval_ms, order=order)
    if count <= 1:
        return [1.0]

    if style == "twap":
        return [1.0 / float(count) for _ in range(int(count))]

    if style == "vwap":
        bucket_ms = int(max(interval_ms, int(os.environ.get("EXEC_VWAP_BUCKET_MS", str(interval_ms or 60000)))))
        lookback_buckets = int(max(count, int(os.environ.get("EXEC_VWAP_LOOKBACK_BUCKETS", str(max(count * 4, 8))))))
        buckets = _recent_volume_buckets(symbol=symbol, bucket_ms=bucket_ms, bucket_count=lookback_buckets)
        if buckets:
            if len(buckets) >= count:
                weights = buckets[-count:]
            else:
                weights = buckets + [0.0 for _ in range(int(count - len(buckets)))]
            return _normalize_weights(weights, count)
        return [1.0 / float(count) for _ in range(int(count))]

    if style == "pov":
        target_participation = _safe_float(
            order.get("target_participation")
            or order.get("pov_participation")
            or liquidity.get("live_participation_rate")
            or os.environ.get("EXEC_POV_PARTICIPATION", "0.03"),
            0.03,
        )
        recent_volume = max(
            _safe_float(liquidity.get("recent_volume_1m") or 0.0, 0.0),
            _safe_float(liquidity.get("recent_volume_5m") or 0.0, 0.0) / 5.0,
        )
        if recent_volume > 0.0 and interval_ms > 0:
            slice_qty = max(1.0, recent_volume * target_participation * max(1.0, float(interval_ms) / 60000.0))
            count = max(1, min(int(math.ceil(float(qty_abs) / float(slice_qty))), int(os.environ.get("EXEC_MAX_SLICES_PER_ORDER", "64"))))
        return [1.0 / float(count) for _ in range(int(count))]

    # Adaptive slicing front-loads when liquidity context says urgency/risk is
    # elevated, but still normalizes weights back to a full order quantity.
    front_load = max(0.0, _safe_float(liquidity.get("aggressiveness_bias") or 0.0, 0.0))
    weights = []
    for i in range(int(count)):
        decay = float(count - i)
        weights.append(max(0.1, decay * (1.0 + front_load)))
    return _normalize_weights(weights, count)


def build_order_slices(order: Dict[str, Any], broker_name: str = "") -> List[Dict[str, Any]]:
    src = dict(order or {})
    qty0 = _safe_float(src.get("qty"), 0.0)
    if qty0 == 0.0:
        return [src]

    symbol = str(src.get("symbol") or "").upper().strip()
    if not symbol:
        return [src]

    px = _safe_float(
        src.get("expected_px")
        or src.get("mid_px")
        or src.get("arrival_mid_px")
        or src.get("ref_px")
        or src.get("px")
        or src.get("price"),
        0.0,
    )

    style = _style_from_order(src)
    if style not in {"twap", "vwap", "pov", "adaptive"}:
        return [src]

    remaining = abs(qty0)
    side_sign = 1.0 if qty0 > 0 else -1.0
    liquidity = get_execution_liquidity_snapshot(symbol=symbol, qty=qty0, px=px or 0.0, ts_ms=src.get("submit_ts_ms"))
    interval_ms = int(
        max(
            0.0,
            _safe_float(
                src.get("slice_interval_ms")
                or src.get("interval_ms")
                or os.environ.get("EXEC_SLICE_INTERVAL_MS", "250"),
                250.0,
            )
            * float(liquidity.get("interval_mult") or 1.0)
        )
    )

    slices: List[Dict[str, Any]] = []
    parent_id = str(src.get("client_order_id") or src.get("portfolio_orders_id") or f"{symbol}_{int(time.time() * 1000)}")
    parent_order_id = _optional_int(src.get("parent_order_id") or src.get("portfolio_orders_id"))
    base_qty = _base_slice_qty(remaining, style, liquidity, src)
    weights = _slice_weights(
        style=style,
        symbol=symbol,
        qty_abs=abs(qty0),
        base_qty=base_qty,
        interval_ms=interval_ms,
        liquidity=liquidity,
        order=src,
    )

    slice_count = len(list(weights or [1.0]))
    for idx, weight in enumerate(list(weights or [1.0])):
        if remaining <= 1e-9:
            break
        qty = float(abs(qty0)) * float(weight)
        if idx == (len(weights) - 1):
            qty = float(remaining)
        qty = min(float(remaining), max(1.0, float(qty)))

        so = dict(src)
        so["qty"] = float(qty) * float(side_sign)
        so["slice_style"] = str(style)
        so["slice_index"] = int(idx)
        so["adaptive_slice_index"] = int(idx)
        so["slice_count"] = int(slice_count)
        so["adaptive_slice_count"] = int(slice_count)
        so["slice_parent_qty"] = float(qty0)
        so["slice_interval_ms"] = int(interval_ms)
        so["slice_parent_id"] = str(parent_id)
        if parent_order_id is not None:
            so["parent_order_id"] = int(parent_order_id)
            so["adaptive_parent_order_id"] = int(parent_order_id)
        so["slice_weight"] = float(weight)
        so["liquidity_snapshot"] = liquidity
        so["rolling_adv"] = float(liquidity.get("rolling_adv") or 0.0)
        so["adv_participation"] = float(liquidity.get("adv_participation") or 0.0)
        so["intraday_vol_bps"] = float(liquidity.get("intraday_vol_bps") or 0.0)
        so["true_spread_bps"] = float(liquidity.get("true_spread_bps") or 0.0)
        so["spread_regime"] = str(liquidity.get("spread_regime") or "")
        so["live_participation_rate"] = float(liquidity.get("live_participation_rate") or 0.0)
        so["recent_volume_1m"] = float(liquidity.get("recent_volume_1m") or 0.0)
        so["recent_volume_5m"] = float(liquidity.get("recent_volume_5m") or 0.0)
        so["slice_broker"] = str(broker_name or "")
        slices.append(so)
        remaining -= float(qty)

    if remaining > 1e-9 and slices:
        slices[-1]["qty"] = float(slices[-1]["qty"]) + (float(remaining) * float(side_sign))

    return slices or [src]
