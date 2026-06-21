"""
FILE: allocator_status.py

Runtime subsystem module for `allocator_status`.

This is a read-side diagnostics adapter for allocator state. It composes
freshness, status, and capacity signals from persisted runtime tables into a
single payload for APIs and operator dashboards.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect


FRESH_MAX_AGE_S = int(os.environ.get("ALLOCATOR_FRESH_MAX_AGE_S", "1800"))  # 30m
log = get_logger("runtime.allocator_status")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn(scope: str, err: Exception, **extra) -> None:
    once_key = None
    if not extra:
        once_key = str(scope)
    elif "table" in extra:
        once_key = f"{scope}:{extra.get('table')}"
    log_failure(
        log,
        event="runtime_allocator_status_nonfatal",
        code=str(scope).replace(".", "_"),
        message=str(scope),
        error=err,
        level=logging.WARNING,
        component="engine.runtime.allocator_status",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        v = float(x)
        if v != v or v == float("inf") or v == float("-inf"):
            return float(default)
        return float(v)
    except Exception as e:
        _warn("allocator_status.safe_float", e, value=repr(x))
        return float(default)


def _safe_json_obj(x: Any) -> Dict[str, Any]:
    try:
        obj = json.loads(x or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception as e:
        _warn("allocator_status.safe_json_obj", e)
        return {}


def _table_exists(con, name: str) -> bool:
    # Allocator status is fail-open with respect to schema evolution; missing
    # tables simply mean that allocator facet is unavailable.
    try:
        r = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(name),),
        ).fetchone()
        return bool(r)
    except Exception as e:
        _warn("allocator_status.table_exists", e, table=str(name))
        return False


def _latest_row(con, table: str, *, where: str = "", args: Tuple[Any, ...] = ()) -> Optional[tuple]:
    try:
        q = f"SELECT * FROM {table} {where} ORDER BY ts_ms DESC LIMIT 1"
        return con.execute(q, args).fetchone()
    except Exception as e:
        _warn("allocator_status.latest_row", e, table=str(table), where=str(where or ""))
        return None


def _count(con, table: str, *, where: str = "", args: Tuple[Any, ...] = ()) -> int:
    try:
        q = f"SELECT COUNT(1) FROM {table} {where}"
        r = con.execute(q, args).fetchone()
        return int(r[0] or 0) if r else 0
    except Exception as e:
        _warn("allocator_status.count", e, table=str(table), where=str(where or ""))
        return 0


def get_allocator_status(*, window_days: int = 0) -> Dict[str, Any]:
    # This is a synthesized status view over several allocator outputs. It is
    # diagnostic/advisory, not the allocator's source of truth itself.
    ts_ms = _now_ms()
    wd = int(window_days)

    con = connect()
    try:
        out: Dict[str, Any] = {
            "ok": False,
            "ts_ms": int(ts_ms),
            "window_days": int(wd),
            "fresh_max_age_s": int(FRESH_MAX_AGE_S),
            "allocator": {},
            "sleeves": {},
            "strategies": {},
            "alpha_decay": {},
            "reasons": [],
        }

        has_sleeve_alloc = _table_exists(con, "sleeve_allocations")
        has_strat_alloc = _table_exists(con, "strategy_allocations")
        has_sleeve_metrics = _table_exists(con, "sleeve_metrics")
        has_strat_metrics = _table_exists(con, "strategy_metrics")
        has_alpha_decay_strategy = _table_exists(con, "alpha_decay_strategy_metrics")
        has_alpha_decay_runtime = _table_exists(con, "alpha_decay_runtime_history")

        out["tables"] = {
            "sleeve_allocations": bool(has_sleeve_alloc),
            "strategy_allocations": bool(has_strat_alloc),
            "sleeve_metrics": bool(has_sleeve_metrics),
            "strategy_metrics": bool(has_strat_metrics),
            "alpha_decay_strategy_metrics": bool(has_alpha_decay_strategy),
            "alpha_decay_runtime_history": bool(has_alpha_decay_runtime),
        }

        # Sleeve and strategy rows are read independently because some installs
        # may have only one allocator layer enabled.
        if has_sleeve_alloc:
            r = _latest_row(con, "sleeve_allocations", where="WHERE window_days=?", args=(int(wd),))
            if r:
                out["sleeves"]["ts_ms"] = int(r[0] or 0)
                out["sleeves"]["weights"] = _safe_json_obj(r[2])
                out["sleeves"]["reason"] = _safe_json_obj(r[3])

        # strategies
        if has_strat_alloc:
            r = _latest_row(con, "strategy_allocations", where="WHERE window_days=?", args=(int(wd),))
            if r:
                out["strategies"]["ts_ms"] = int(r[0] or 0)
                out["strategies"]["weights"] = _safe_json_obj(r[2])
                out["strategies"]["reason"] = _safe_json_obj(r[3])

        if has_sleeve_metrics:
            out["sleeves"]["n_metrics"] = _count(con, "sleeve_metrics", where="WHERE window_days=?", args=(int(wd),))

        if has_strat_metrics:
            out["strategies"]["n_metrics"] = _count(con, "strategy_metrics", where="WHERE window_days=?", args=(int(wd),))

        # runtime alpha decay
        if has_alpha_decay_runtime:
            r = _latest_row(con, "alpha_decay_runtime_history")
            if r:
                out["alpha_decay"]["ts_ms"] = int(r[0] or 0)
                out["alpha_decay"]["status"] = str(r[1] or "ok")
                out["alpha_decay"]["min_throttle_mult"] = _safe_float(r[2], 1.0)
                out["alpha_decay"]["severe_count"] = int(r[3] or 0)
                out["alpha_decay"]["warn_count"] = int(r[4] or 0)

                try:
                    out["alpha_decay"]["runtime"] = json.loads(r[5] or "{}") if r[5] else {}
                except Exception as e:
                    _warn("allocator_status.alpha_decay.runtime_json", e)
                    out["alpha_decay"]["runtime"] = {}

                try:
                    runtime_obj = dict(out["alpha_decay"].get("runtime") or {})
                    portfolio_obj = dict(runtime_obj.get("portfolio") or {})
                    if portfolio_obj:
                        out["alpha_decay"]["portfolio"] = portfolio_obj
                except Exception as e:
                    _warn("allocator_status.alpha_decay.portfolio_extract", e)

        # strategy alpha decay metrics
        if has_alpha_decay_strategy:
            try:
                rows = con.execute(
                    """
                    SELECT m.strategy_name,
                           m.ts_ms,
                           m.rolling_sharpe,
                           m.half_life_buckets,
                           m.half_life_seconds,
                           m.structural_break_z,
                           m.severity,
                           m.severity_score,
                           m.throttle_mult,
                           m.n_obs,
                           m.detail_json
                    FROM alpha_decay_strategy_metrics m
                    JOIN (
                      SELECT strategy_name, MAX(ts_ms) AS ts_ms
                      FROM alpha_decay_strategy_metrics
                      WHERE window_days=?
                      GROUP BY strategy_name
                    ) t
                    ON t.strategy_name=m.strategy_name AND t.ts_ms=m.ts_ms
                    WHERE m.window_days=?
                    ORDER BY m.ts_ms DESC, m.strategy_name ASC
                    """,
                    (int(wd), int(wd)),
                ).fetchall() or []
            except Exception as e:
                _warn("allocator_status.alpha_decay.strategy_metrics", e, window_days=int(wd))
                rows = []

            latest_rows: List[Dict[str, Any]] = []

            for r in rows:
                detail = _safe_json_obj(r[10])
                latest_rows.append({
                    "strategy_name": str(r[0] or ""),
                    "ts_ms": int(r[1] or 0),
                    "rolling_sharpe": float(r[2] or 0.0),
                    "half_life_buckets": None if r[3] is None else float(r[3]),
                    "half_life_seconds": None if r[4] is None else float(r[4]),
                    "structural_break_z": float(r[5] or 0.0),
                    "severity": str(r[6] or "ok"),
                    "severity_score": float(r[7] or 0.0),
                    "throttle_mult": _safe_float(r[8], 1.0),
                    "n_obs": int(r[9] or 0),
                    "detail": detail,
                })

            out["alpha_decay"]["strategies"] = latest_rows
            out["alpha_decay"]["n_metrics"] = _count(
                con,
                "alpha_decay_strategy_metrics",
                where="WHERE window_days=?",
                args=(int(wd),),
            )

        # Freshness is the main status signal: allocator output that is too old
        # is treated as unavailable even if historical rows exist.
        freshest = max(
            int(out.get("sleeves", {}).get("ts_ms") or 0),
            int(out.get("strategies", {}).get("ts_ms") or 0),
            int(out.get("alpha_decay", {}).get("ts_ms") or 0),
        )

        age_s = (int(ts_ms) - int(freshest)) / 1000.0 if freshest > 0 else 1e18

        out["allocator"]["latest_ts_ms"] = int(freshest)
        out["allocator"]["age_s"] = float(age_s)

        if freshest <= 0:
            out["reasons"].append("allocator_never_ran")
            out["ok"] = False
        elif age_s > float(FRESH_MAX_AGE_S):
            out["reasons"].append("allocator_stale")
            out["ok"] = False
        else:
            out["ok"] = True

        return out

    finally:
        try:
            con.close()
        except Exception as e:
            _warn("allocator_status.close", e)
