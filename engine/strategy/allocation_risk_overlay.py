"""
Deterministic allocation overlays for crowding, concentration, and execution capacity.

This extends the existing portfolio allocator without changing its base signal
selection logic.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

ALLOCATION_OVERLAY_ENABLED = os.environ.get("PORTFOLIO_RISK_OVERLAY_ENABLED", "1") == "1"
ALLOCATION_TOP2_CAP = float(os.environ.get("PORTFOLIO_TOP2_CONCENTRATION_CAP", "0.65"))
ALLOCATION_CROWDING_LOOKBACK_S = int(os.environ.get("PORTFOLIO_CROWDING_LOOKBACK_S", "14400"))
ALLOCATION_CROWDING_ALERTS_WARN = int(os.environ.get("PORTFOLIO_CROWDING_ALERTS_WARN", "4"))
ALLOCATION_CROWDING_ALERTS_HARD = int(os.environ.get("PORTFOLIO_CROWDING_ALERTS_HARD", "8"))
ALLOCATION_CROWDING_WARN_FACTOR = float(os.environ.get("PORTFOLIO_CROWDING_WARN_FACTOR", "0.80"))
ALLOCATION_CROWDING_HARD_FACTOR = float(os.environ.get("PORTFOLIO_CROWDING_HARD_FACTOR", "0.60"))
ALLOCATION_EXEC_CAP_WARN_BPS = float(os.environ.get("PORTFOLIO_EXEC_CAP_WARN_BPS", "12.0"))
ALLOCATION_EXEC_CAP_HARD_BPS = float(os.environ.get("PORTFOLIO_EXEC_CAP_HARD_BPS", "25.0"))
ALLOCATION_EXEC_CAP_WARN_FACTOR = float(os.environ.get("PORTFOLIO_EXEC_CAP_WARN_FACTOR", "0.85"))
ALLOCATION_EXEC_CAP_HARD_FACTOR = float(os.environ.get("PORTFOLIO_EXEC_CAP_HARD_FACTOR", "0.65"))
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("strategy.allocation_risk_overlay")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_allocation_risk_overlay_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.allocation_risk_overlay",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, str) and not value.strip():
        return float(default)
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal("ALLOCATION_RISK_OVERLAY_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(value)[:120])
        return float(default)


def _table_exists(con, name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("ALLOCATION_RISK_OVERLAY_TABLE_EXISTS_FAILED", e, once_key=f"table_exists:{name}", table_name=str(name))
        return False


def _gross(desired: Dict[str, Dict[str, Any]]) -> float:
    return sum(abs(_safe_float((row or {}).get("weight"), 0.0)) for row in (desired or {}).values())


def _renormalize_if_needed(desired: Dict[str, Dict[str, Any]], gross_cap: float) -> None:
    gross = _gross(desired)
    if gross <= 1e-9 or gross <= float(gross_cap):
        return
    scale = float(gross_cap) / float(gross)
    for symbol in list(desired.keys()):
        desired[symbol]["weight"] = _safe_float(desired[symbol].get("weight"), 0.0) * float(scale)
        desired[symbol].setdefault("reason", {})
        desired[symbol]["reason"]["overlay_gross_rescale"] = float(scale)


def _recent_alert_counts(con, since_ms: int) -> Dict[str, int]:
    if not _table_exists(con, "alerts"):
        return {}
    try:
        rows = con.execute(
            """
            SELECT symbol, COUNT(*)
            FROM alerts
            WHERE ts_ms >= ?
            GROUP BY symbol
            """,
            (int(since_ms),),
        ).fetchall() or []
    except Exception:
        rows = []
    out: Dict[str, int] = {}
    for symbol, count in rows:
        out[str(symbol or "").upper().strip()] = int(count or 0)
    return out


def _recent_slippage_bps(con, symbols: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    if not symbols:
        return {}
    if _table_exists(con, "execution_fills"):
        sql = """
            SELECT f.symbol, AVG(f.slippage_bps)
            FROM execution_fills f
            LEFT JOIN execution_orders o
              ON o.client_order_id = f.client_order_id
            WHERE f.slippage_bps IS NOT NULL
              AND COALESCE(json_extract(o.extra_json, '$.execution_target'), 'real') = 'real'
            GROUP BY f.symbol
        """
    elif _table_exists(con, "execution_analytics"):
        sql = """
            SELECT symbol, AVG(slippage_bps)
            FROM execution_analytics
            WHERE slippage_bps IS NOT NULL
            GROUP BY symbol
        """
    else:
        return {}
    try:
        rows = con.execute(sql).fetchall() or []
    except Exception:
        rows = []
    out: Dict[str, float] = {}
    for symbol, avg_slippage in rows:
        sym = str(symbol or "").upper().strip()
        if sym:
            out[sym] = _safe_float(avg_slippage, 0.0)
    return out


def apply_allocation_risk_overlays(
    con,
    desired: Dict[str, Dict[str, Any]],
    *,
    gross_cap: float,
    now_ms: int | None = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    snapshot = {
        "enabled": bool(ALLOCATION_OVERLAY_ENABLED),
        "top2_concentration_before": 0.0,
        "top2_concentration_after": 0.0,
        "crowding_counts": {},
        "slippage_bps": {},
        "adjusted_symbols": [],
    }
    if not ALLOCATION_OVERLAY_ENABLED or not desired:
        return desired, snapshot

    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    since_ms = int(ts_ms - max(60, int(ALLOCATION_CROWDING_LOOKBACK_S)) * 1000)
    alert_counts = _recent_alert_counts(con, since_ms)
    slippage = _recent_slippage_bps(con, desired)
    snapshot["crowding_counts"] = alert_counts
    snapshot["slippage_bps"] = slippage

    ranked_before = sorted(
        [abs(_safe_float((row or {}).get("weight"), 0.0)) for row in desired.values()],
        reverse=True,
    )
    snapshot["top2_concentration_before"] = float(sum(ranked_before[:2]))

    adjusted = []
    for symbol in list(desired.keys()):
        row = desired[symbol]
        factor = 1.0
        crowding_n = int(alert_counts.get(str(symbol).upper(), 0))
        slip_bps = _safe_float(slippage.get(str(symbol).upper()), 0.0)

        if crowding_n >= int(ALLOCATION_CROWDING_ALERTS_HARD):
            factor *= float(ALLOCATION_CROWDING_HARD_FACTOR)
        elif crowding_n >= int(ALLOCATION_CROWDING_ALERTS_WARN):
            factor *= float(ALLOCATION_CROWDING_WARN_FACTOR)

        if slip_bps >= float(ALLOCATION_EXEC_CAP_HARD_BPS):
            factor *= float(ALLOCATION_EXEC_CAP_HARD_FACTOR)
        elif slip_bps >= float(ALLOCATION_EXEC_CAP_WARN_BPS):
            factor *= float(ALLOCATION_EXEC_CAP_WARN_FACTOR)

        if factor < 0.999:
            row["weight"] = _safe_float(row.get("weight"), 0.0) * float(factor)
            row.setdefault("reason", {})
            row["reason"]["overlay_factor"] = float(factor)
            row["reason"]["overlay_crowding_alerts"] = int(crowding_n)
            row["reason"]["overlay_exec_capacity_slippage_bps"] = float(slip_bps)
            adjusted.append(str(symbol))

    ranked = sorted(
        [(symbol, abs(_safe_float((row or {}).get("weight"), 0.0))) for symbol, row in desired.items()],
        key=lambda item: item[1],
        reverse=True,
    )
    top2_total = float(sum(weight for _, weight in ranked[:2]))
    if top2_total > float(ALLOCATION_TOP2_CAP) and top2_total > 1e-9:
        scale = float(ALLOCATION_TOP2_CAP) / float(top2_total)
        for symbol, _ in ranked[:2]:
            desired[symbol]["weight"] = _safe_float(desired[symbol].get("weight"), 0.0) * float(scale)
            desired[symbol].setdefault("reason", {})
            desired[symbol]["reason"]["overlay_top2_concentration_scale"] = float(scale)
            if symbol not in adjusted:
                adjusted.append(str(symbol))

    _renormalize_if_needed(desired, gross_cap=float(gross_cap))

    ranked_after = sorted(
        [abs(_safe_float((row or {}).get("weight"), 0.0)) for row in desired.values()],
        reverse=True,
    )
    snapshot["top2_concentration_after"] = float(sum(ranked_after[:2]))
    snapshot["adjusted_symbols"] = adjusted
    return desired, snapshot
