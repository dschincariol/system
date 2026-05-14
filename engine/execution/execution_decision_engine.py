"""
Execution-only decision layer.

Alpha upstream decides what to trade. This module decides how to work the
resulting intent using microstructure context and realized fill feedback.
"""

from __future__ import annotations

import math
import logging
import os
from typing import Any, Dict, Iterable, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

FEEDBACK_LOOKBACK_N = int(os.environ.get("EXECUTION_FEEDBACK_LOOKBACK_N", "80"))
PASSIVE_DELAY_MS = int(os.environ.get("EXECUTION_PASSIVE_DELAY_MS", "900"))
BALANCED_DELAY_MS = int(os.environ.get("EXECUTION_BALANCED_DELAY_MS", "250"))
MAX_ENTRY_DELAY_MS = int(os.environ.get("EXECUTION_MAX_ENTRY_DELAY_MS", "15000"))
MIN_SIZE_MULT = float(os.environ.get("EXECUTION_MIN_SIZE_MULT", "0.35"))
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("execution.execution_decision_engine")


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="execution_decision_engine_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.execution_decision_engine",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if not math.isfinite(out):
            return float(default)
        return float(out)
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_DECISION_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value=repr(value)[:120],
        )
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_DECISION_SAFE_INT_FAILED",
            e,
            once_key="safe_int",
            value=repr(value)[:120],
        )
        return int(default)


def _clamp(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    if not vals:
        return None
    return float(sum(vals) / float(len(vals)))


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_DECISION_TABLE_EXISTS_FAILED",
            e,
            once_key=f"table_exists:{table_name}",
            table_name=str(table_name),
        )
        return False


def build_alpha_handoff(
    order: Dict[str, Any],
    *,
    side: str,
    signal_ts_ms: int,
    alpha_remaining: float,
    ttl_ms: int,
    half_life_ms: int,
) -> Dict[str, Any]:
    return {
        "symbol": str(order.get("symbol") or "").strip().upper(),
        "side": str(side or "").upper().strip(),
        "action": str(order.get("action") or "").strip().upper(),
        "qty": _safe_float(order.get("qty"), 0.0),
        "to_weight": _safe_float(order.get("to_weight"), 0.0),
        "delta_weight": _safe_float(order.get("delta_weight"), 0.0),
        "signal_ts_ms": int(signal_ts_ms or 0),
        "confidence": _safe_float(order.get("confidence"), 0.0),
        "expected_z": _safe_float(order.get("expected_z") or order.get("zscore"), 0.0),
        "alpha_remaining": float(_clamp(alpha_remaining, 0.0, 1.0)),
        "alpha_ttl_ms": int(max(0, ttl_ms)),
        "alpha_half_life_ms": int(max(0, half_life_ms)),
        "model_id": str(order.get("model_id") or "baseline"),
        "model_name": str(order.get("model_name") or order.get("strategy_name") or ""),
        "source_alert_id": (
            _safe_int(order.get("source_alert_id"))
            if order.get("source_alert_id") is not None
            else None
        ),
    }


def load_execution_feedback_snapshot(
    con,
    *,
    symbol: str,
    broker: str,
    lookback_n: int = FEEDBACK_LOOKBACK_N,
) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    br = str(broker or "").strip().lower()
    limit_n = int(max(10, min(500, int(lookback_n or FEEDBACK_LOOKBACK_N))))
    out: Dict[str, Any] = {
        "symbol": sym,
        "broker": br,
        "sample_n": 0,
        "avg_expected_slippage_bps": None,
        "avg_realized_slippage_bps": None,
        "avg_slippage_error_bps": None,
        "avg_latency_ms": None,
        "avg_fill_quality_score": None,
        "source": None,
    }
    if not sym:
        return out

    if _table_exists(con, "execution_policy_feedback"):
        try:
            rows = con.execute(
                """
                SELECT
                  expected_slippage_bps,
                  realized_slippage_bps,
                  slippage_error_bps,
                  realized_fill_latency_ms,
                  fill_quality_score
                FROM execution_policy_feedback
                WHERE symbol = ?
                  AND (? = '' OR broker = ? OR broker IS NULL)
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (sym, br, br, limit_n),
            ).fetchall() or []
        except Exception:
            rows = []

        if rows:
            out.update(
                {
                    "sample_n": int(len(rows)),
                    "avg_expected_slippage_bps": _mean(row[0] for row in rows),
                    "avg_realized_slippage_bps": _mean(row[1] for row in rows),
                    "avg_slippage_error_bps": _mean(row[2] for row in rows),
                    "avg_latency_ms": _mean(row[3] for row in rows),
                    "avg_fill_quality_score": _mean(row[4] for row in rows),
                    "source": "execution_policy_feedback",
                }
            )
            return out

    if _table_exists(con, "execution_analytics"):
        try:
            rows = con.execute(
                """
                SELECT slippage_bps, time_to_fill_ms
                FROM execution_analytics
                WHERE symbol = ?
                  AND (? = '' OR broker = ? OR broker IS NULL)
                  AND slippage_bps IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (sym, br, br, limit_n),
            ).fetchall() or []
        except Exception:
            rows = []

        if rows:
            out.update(
                {
                    "sample_n": int(len(rows)),
                    "avg_realized_slippage_bps": _mean(row[0] for row in rows),
                    "avg_latency_ms": _mean(row[1] for row in rows),
                    "source": "execution_analytics",
                }
            )
            return out

    if _table_exists(con, "execution_fills"):
        try:
            rows = con.execute(
                """
                SELECT slippage_bps, fill_latency_ms
                FROM execution_fills
                WHERE symbol = ?
                  AND slippage_bps IS NOT NULL
                ORDER BY fill_ts_ms DESC, id DESC
                LIMIT ?
                """,
                (sym, limit_n),
            ).fetchall() or []
        except Exception:
            rows = []

        if rows:
            out.update(
                {
                    "sample_n": int(len(rows)),
                    "avg_realized_slippage_bps": _mean(row[0] for row in rows),
                    "avg_latency_ms": _mean(row[1] for row in rows),
                    "source": "execution_fills",
                }
            )
    return out


def decide_execution_strategy(
    *,
    alpha_intent: Dict[str, Any],
    order: Dict[str, Any],
    broker: str,
    base_order_type: str,
    base_aggressiveness: str,
    default_latency_ms: int,
    default_chunk_pct: float,
    default_extra_slippage_bps: float,
    feedback: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    feedback_norm = dict(feedback or {})

    alpha_remaining = _safe_float(alpha_intent.get("alpha_remaining"), 0.0)
    confidence = _safe_float(alpha_intent.get("confidence"), 0.0)
    expected_z = abs(_safe_float(alpha_intent.get("expected_z"), 0.0))

    spread_bps = max(
        0.0,
        _safe_float(
            order.get("true_spread_bps")
            or order.get("spread_bps")
            or order.get("entry_spread_bps"),
            0.0,
        ),
    )
    volatility_bps = max(
        0.0,
        _safe_float(order.get("intraday_vol_bps"), 0.0),
        _safe_float(order.get("vol_bps"), 0.0),
        _safe_float(order.get("volatility"), 0.0) * 10000.0,
    )
    adv_participation = max(
        0.0,
        _safe_float(order.get("adv_participation"), 0.0),
        _safe_float(order.get("live_participation_rate"), 0.0),
    )

    sample_n = _safe_int(feedback_norm.get("sample_n"), 0)
    avg_realized_slip = max(0.0, _safe_float(feedback_norm.get("avg_realized_slippage_bps"), 0.0))
    avg_slip_error = _safe_float(feedback_norm.get("avg_slippage_error_bps"), 0.0)
    avg_latency_ms = max(0.0, _safe_float(feedback_norm.get("avg_latency_ms"), 0.0))
    avg_fill_quality = _safe_float(feedback_norm.get("avg_fill_quality_score"), 0.65)
    if sample_n <= 0:
        avg_fill_quality = 0.65

    expected_slippage_bps = max(0.0, _safe_float(default_extra_slippage_bps, 0.0))
    expected_slippage_bps += spread_bps * 0.45
    expected_slippage_bps += min(6.0, volatility_bps * 0.08)
    expected_slippage_bps += min(3.0, adv_participation * 35.0)

    if str(base_order_type or "").upper().strip() == "MARKET":
        expected_slippage_bps += 1.0
    if str(base_aggressiveness or "").upper().strip() == "AGGRESSIVE":
        expected_slippage_bps += 0.8
    elif str(base_aggressiveness or "").upper().strip() == "NEUTRAL":
        expected_slippage_bps += 0.25

    if sample_n >= 3:
        expected_slippage_bps = max(
            expected_slippage_bps,
            avg_realized_slip + max(0.0, avg_slip_error) * 0.75,
        )

    edge_budget_bps = max(0.75, (expected_z * 2.5) + (confidence * 4.0) + (alpha_remaining * 3.0))
    slippage_pressure = expected_slippage_bps / max(edge_budget_bps, 0.5)

    execution_policy = "balanced"
    if (
        expected_slippage_bps >= 2.75
        or spread_bps >= 4.0
        or (sample_n >= 5 and avg_fill_quality <= 0.55)
        or slippage_pressure >= 0.55
    ):
        execution_policy = "passive"
    if (
        alpha_remaining <= 0.28
        or (slippage_pressure <= 0.35 and spread_bps <= 3.0 and expected_slippage_bps <= 3.25)
    ):
        execution_policy = "aggressive"
    if str(base_order_type or "").upper().strip() == "MARKET" and execution_policy != "passive":
        execution_policy = "aggressive"
    if str(base_aggressiveness or "").upper().strip() == "PASSIVE" and execution_policy != "aggressive":
        execution_policy = "passive"

    order_type = "LIMIT"
    aggressiveness = "NEUTRAL"
    entry_delay_ms = 0
    entry_style = "staged_limit"

    if execution_policy == "passive":
        order_type = "LIMIT"
        aggressiveness = "PASSIVE"
        entry_style = "delayed_limit"
        entry_delay_ms = PASSIVE_DELAY_MS
        entry_delay_ms += int(max(0.0, spread_bps - 1.0) * 80.0)
        entry_delay_ms += int(max(0.0, expected_slippage_bps - 1.0) * 120.0)
        entry_delay_ms += int(max(0.0, avg_slip_error) * 70.0)
    elif execution_policy == "aggressive":
        order_type = "MARKET"
        aggressiveness = "AGGRESSIVE"
        entry_style = "direct_market"
        entry_delay_ms = 0
    else:
        order_type = "LIMIT"
        aggressiveness = "NEUTRAL"
        entry_style = "working_limit"
        entry_delay_ms = BALANCED_DELAY_MS
        entry_delay_ms += int(max(0.0, expected_slippage_bps - 1.25) * 70.0)

    if alpha_remaining < 0.35:
        entry_delay_ms = int(float(entry_delay_ms) * 0.25)
    if sample_n < 3:
        entry_delay_ms = int(float(entry_delay_ms) * 0.50)
    entry_delay_ms = int(_clamp(entry_delay_ms, 0.0, float(MAX_ENTRY_DELAY_MS)))

    size_mult = 1.0
    if expected_slippage_bps >= 1.5:
        size_mult *= 0.92
    if expected_slippage_bps >= 3.0:
        size_mult *= 0.82
    if expected_slippage_bps >= 5.0:
        size_mult *= 0.72
    if sample_n >= 5 and avg_slip_error >= 1.0:
        size_mult *= 0.88
    if sample_n >= 5 and avg_fill_quality <= 0.50:
        size_mult *= 0.88
    if slippage_pressure >= 0.85:
        size_mult *= 0.78
    if execution_policy == "aggressive" and alpha_remaining <= 0.20 and confidence >= 0.75:
        size_mult = max(size_mult, 0.90)
    size_mult = _clamp(size_mult, MIN_SIZE_MULT, 1.0)

    latency_mult = 1.0
    chunk_mult = 1.0
    sim_extra_slippage_bps = expected_slippage_bps

    if execution_policy == "passive":
        latency_mult = 1.35
        chunk_mult = 0.80
        sim_extra_slippage_bps = max(0.0, expected_slippage_bps * 0.25)
    elif execution_policy == "aggressive":
        latency_mult = 0.70
        chunk_mult = 1.15
        sim_extra_slippage_bps = max(_safe_float(default_extra_slippage_bps, 0.0), expected_slippage_bps * 0.60)
    else:
        latency_mult = 1.0
        chunk_mult = 0.95
        sim_extra_slippage_bps = max(_safe_float(default_extra_slippage_bps, 0.0), expected_slippage_bps * 0.40)

    expected_fill_latency_ms = max(
        int(default_latency_ms),
        int(avg_latency_ms) if sample_n >= 3 else int(default_latency_ms),
    )
    expected_fill_latency_ms = int(max(50.0, float(expected_fill_latency_ms) * float(latency_mult)))
    limit_offset_bps = 0.0
    if order_type == "LIMIT":
        limit_offset_bps = _clamp(max(spread_bps * 0.45, expected_slippage_bps * 0.20), 0.0, 25.0)

    chunk_pct = _clamp(float(default_chunk_pct) * float(chunk_mult), 0.05, 1.0)

    rationale = [
        f"policy={execution_policy}",
        f"expected_slippage_bps={expected_slippage_bps:.2f}",
        f"alpha_remaining={alpha_remaining:.3f}",
        f"spread_bps={spread_bps:.2f}",
    ]
    if sample_n > 0:
        rationale.append(f"feedback_n={sample_n}")
        rationale.append(f"avg_fill_quality={avg_fill_quality:.2f}")

    return {
        "execution_policy": str(execution_policy),
        "entry_strategy": str(entry_style),
        "entry_delay_ms": int(entry_delay_ms),
        "order_type": str(order_type),
        "aggressiveness": str(aggressiveness),
        "expected_slippage_bps": round(float(expected_slippage_bps), 4),
        "expected_fill_latency_ms": int(expected_fill_latency_ms),
        "size_mult": round(float(size_mult), 6),
        "latency_mult": round(float(latency_mult), 6),
        "chunk_pct": round(float(chunk_pct), 6),
        "limit_offset_bps": round(float(limit_offset_bps), 4),
        "sim_extra_slippage_bps": round(float(sim_extra_slippage_bps), 4),
        "slippage_pressure": round(float(slippage_pressure), 6),
        "feedback_snapshot": feedback_norm,
        "rationale": rationale,
    }
