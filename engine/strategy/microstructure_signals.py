"""
FILE: microstructure_signals.py

Reads live microstructure context and turns it into a bounded confidence
multiplier. The logic is intentionally sign-preserving: it adjusts trust, not
direction.
"""

import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

MICRO_MAX_AGE_MS = int(os.environ.get("MICRO_MAX_AGE_MS", "15000"))
MICRO_SPREAD_SOFT_Z = float(os.environ.get("MICRO_SPREAD_SOFT_Z", "1.25"))
MICRO_SPREAD_HARD_Z = float(os.environ.get("MICRO_SPREAD_HARD_Z", "4.00"))
MICRO_SPREAD_MAX_DECAY = float(os.environ.get("MICRO_SPREAD_MAX_DECAY", "0.60"))
MICRO_ALIGN_BOOST = float(os.environ.get("MICRO_ALIGN_BOOST", "0.12"))
MICRO_OPPOSE_DECAY = float(os.environ.get("MICRO_OPPOSE_DECAY", "0.20"))
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("strategy.microstructure_signals")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_microstructure_signals_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.microstructure_signals",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(x))))


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception as e:
        _warn_nonfatal("MICROSTRUCTURE_SIGNALS_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(x)[:120])
        return None


def _safe_json_loads(s: Any) -> Any:
    try:
        if not s:
            return None
        return json.loads(s)
    except Exception as e:
        _warn_nonfatal("MICROSTRUCTURE_SIGNALS_JSON_PARSE_FAILED", e, once_key="json_parse", payload=str(s)[:200])
        return None


def load_latest_microstructure_context(
    con,
    symbol: str,
    ts_ref_ms: Optional[int] = None,
    max_age_ms: int = MICRO_MAX_AGE_MS,
) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None

    ref_ms = int(ts_ref_ms or _now_ms())
    try:
        row = con.execute(
            """
            SELECT
              ts_ms,
              provider,
              mid_px,
              bid_px,
              ask_px,
              bid_sz,
              ask_sz,
              spread_bps,
              spread_z,
              spread_widening,
              order_book_imbalance,
              trade_buy_volume,
              trade_sell_volume,
              trade_aggressor_imbalance,
              composite_score,
              details_json
            FROM market_microstructure_signals
            WHERE symbol=?
              AND ts_ms <= ?
              AND ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (
                sym,
                int(ref_ms),
                int(ref_ms - max(1, int(max_age_ms))),
            ),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "MICROSTRUCTURE_SIGNALS_CONTEXT_LOAD_FAILED",
            e,
            once_key=f"context_load:{sym}",
            symbol=str(sym),
        )
        return None

    if not row:
        return None

    ctx = {
        "ts_ms": int(row[0] or 0),
        "provider": str(row[1] or ""),
        "mid_px": _safe_float(row[2]),
        "bid_px": _safe_float(row[3]),
        "ask_px": _safe_float(row[4]),
        "bid_sz": _safe_float(row[5]),
        "ask_sz": _safe_float(row[6]),
        "spread_bps": _safe_float(row[7]),
        "spread_z": _safe_float(row[8]),
        "spread_widening": _safe_float(row[9]),
        "order_book_imbalance": _safe_float(row[10]),
        "trade_buy_volume": _safe_float(row[11]),
        "trade_sell_volume": _safe_float(row[12]),
        "trade_aggressor_imbalance": _safe_float(row[13]),
        "composite_score": _safe_float(row[14]),
        "details": _safe_json_loads(row[15]),
    }
    ctx["age_ms"] = int(max(0, ref_ms - int(ctx["ts_ms"] or 0)))
    return ctx


def apply_microstructure_confidence(
    *,
    expected_z: float,
    base_conf: float,
    micro_ctx: Optional[Dict[str, Any]],
) -> Tuple[float, Dict[str, Any]]:
    conf = float(base_conf)
    explain: Dict[str, Any] = {
        "applied": False,
        "base_conf": float(base_conf),
    }

    if not isinstance(micro_ctx, dict):
        explain["reason"] = "no_microstructure_context"
        return float(conf), explain

    explain["applied"] = True
    explain["age_ms"] = int(micro_ctx.get("age_ms") or 0)
    explain["provider"] = str(micro_ctx.get("provider") or "")
    explain["spread_bps"] = _safe_float(micro_ctx.get("spread_bps"))
    explain["spread_z"] = _safe_float(micro_ctx.get("spread_z"))
    explain["spread_widening"] = _safe_float(micro_ctx.get("spread_widening"))
    explain["order_book_imbalance"] = _safe_float(micro_ctx.get("order_book_imbalance"))
    explain["trade_aggressor_imbalance"] = _safe_float(micro_ctx.get("trade_aggressor_imbalance"))
    explain["composite_score"] = _safe_float(micro_ctx.get("composite_score"))

    ob = float(_safe_float(micro_ctx.get("order_book_imbalance")) or 0.0)
    ta = float(_safe_float(micro_ctx.get("trade_aggressor_imbalance")) or 0.0)
    directional_pressure = 0.5 * float(ob) + 0.5 * float(ta)
    signal_side = 1.0 if float(expected_z) >= 0.0 else -1.0
    align_score = float(signal_side) * float(directional_pressure)

    spread_z = float(_safe_float(micro_ctx.get("spread_z")) or 0.0)
    spread_mult = 1.0
    if spread_z >= float(MICRO_SPREAD_HARD_Z):
        spread_mult = float(MICRO_SPREAD_MAX_DECAY)
    elif spread_z > float(MICRO_SPREAD_SOFT_Z):
        frac = (float(spread_z) - float(MICRO_SPREAD_SOFT_Z)) / max(
            1e-9, float(MICRO_SPREAD_HARD_Z) - float(MICRO_SPREAD_SOFT_Z)
        )
        frac = _clamp(frac, 0.0, 1.0)
        spread_mult = 1.0 - ((1.0 - float(MICRO_SPREAD_MAX_DECAY)) * frac)

    directional_mult = 1.0
    if align_score > 0.0:
        directional_mult += float(MICRO_ALIGN_BOOST) * _clamp(abs(float(align_score)), 0.0, 1.0)
    elif align_score < 0.0:
        directional_mult -= float(MICRO_OPPOSE_DECAY) * _clamp(abs(float(align_score)), 0.0, 1.0)

    final_mult = _clamp(float(spread_mult) * float(directional_mult), 0.25, 1.25)
    conf = _clamp(float(conf) * float(final_mult), 0.0, 1.0)

    explain["directional_pressure"] = float(directional_pressure)
    explain["align_score"] = float(align_score)
    explain["spread_mult"] = float(spread_mult)
    explain["directional_mult"] = float(directional_mult)
    explain["final_mult"] = float(final_mult)
    explain["adjusted_conf"] = float(conf)

    return float(conf), explain
