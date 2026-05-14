"""
FILE: trade_suppression_engine.py

Execution subsystem module for `trade_suppression_engine`.
"""

import json
import logging
import math
import os
import time
from typing import Any, Dict, List

from engine.execution.exec_stats import get_false_positive_streak
from engine.execution.execution_analytics_engine import get_execution_degradation_snapshot
from engine.runtime.dbapi_compat import Error as DBAPIError, is_sqlite_error
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.observability import record_component_health
from engine.runtime.risk_state import set_state
from engine.runtime.storage import connect, init_db

LOG = get_logger("engine.execution.trade_suppression_engine")


def _is_db_cleanup_error(error: BaseException) -> bool:
    return isinstance(error, (DBAPIError, OSError)) or is_sqlite_error(error)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="trade_suppression_engine_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.trade_suppression_engine",
        extra=extra or None,
        persist=False,
    )


def _record_trade_suppression_degraded(reason: str, error: BaseException, **extra: object) -> None:
    health_extra = dict(extra or {})
    health_extra["reason"] = str(reason)
    health_extra["error_type"] = type(error).__name__
    record_component_health(
        "trade_suppression_engine",
        ok=False,
        status="degraded",
        detail=str(reason),
        extra=health_extra,
    )


FP_SIZE_THRESHOLD = int(os.environ.get("TSE_FP_SIZE_THRESHOLD", "2"))
FP_SOFT_THRESHOLD = int(os.environ.get("TSE_FP_SOFT_THRESHOLD", "3"))
FP_HARD_THRESHOLD = int(os.environ.get("TSE_FP_HARD_THRESHOLD", "5"))

SLIPVOL_Z_SIZE_THRESHOLD = float(os.environ.get("TSE_SLIPVOL_Z_SIZE_THRESHOLD", "1.25"))
SLIPVOL_Z_SOFT_THRESHOLD = float(os.environ.get("TSE_SLIPVOL_Z_SOFT_THRESHOLD", "2.00"))
SLIPVOL_Z_HARD_THRESHOLD = float(os.environ.get("TSE_SLIPVOL_Z_HARD_THRESHOLD", "3.00"))

LATVAR_Z_SIZE_THRESHOLD = float(os.environ.get("TSE_LATVAR_Z_SIZE_THRESHOLD", "1.25"))
LATVAR_Z_SOFT_THRESHOLD = float(os.environ.get("TSE_LATVAR_Z_SOFT_THRESHOLD", "2.00"))
LATVAR_Z_HARD_THRESHOLD = float(os.environ.get("TSE_LATVAR_Z_HARD_THRESHOLD", "3.00"))

DEGRADE_MEAN_SLIPPAGE_SOFT = float(os.environ.get("TSE_DEGRADE_MEAN_SLIPPAGE_SOFT", "12.0"))
DEGRADE_MEAN_SLIPPAGE_HARD = float(os.environ.get("TSE_DEGRADE_MEAN_SLIPPAGE_HARD", "20.0"))
DEGRADE_P95_SLIPPAGE_SOFT = float(os.environ.get("TSE_DEGRADE_P95_SLIPPAGE_SOFT", "20.0"))
DEGRADE_P95_SLIPPAGE_HARD = float(os.environ.get("TSE_DEGRADE_P95_SLIPPAGE_HARD", "35.0"))
DEGRADE_MEAN_LATENCY_SOFT = float(os.environ.get("TSE_DEGRADE_MEAN_LATENCY_SOFT", "900"))
DEGRADE_MEAN_LATENCY_HARD = float(os.environ.get("TSE_DEGRADE_MEAN_LATENCY_HARD", "1500"))
DEGRADE_P95_LATENCY_SOFT = float(os.environ.get("TSE_DEGRADE_P95_LATENCY_SOFT", "1500"))
DEGRADE_P95_LATENCY_HARD = float(os.environ.get("TSE_DEGRADE_P95_LATENCY_HARD", "2500"))

SIZE_COMPRESSION_MULT = float(os.environ.get("TSE_SIZE_COMPRESSION_MULT", "0.65"))
SOFT_THROTTLE_MULT = float(os.environ.get("TSE_SOFT_THROTTLE_MULT", "0.40"))
HARD_BLOCK_MULT = 0.0

EXEC_ANALYTICS_WINDOW = int(os.environ.get("TSE_EXEC_ANALYTICS_WINDOW", "120"))
PROVIDER_WINDOW = int(os.environ.get("TSE_PROVIDER_WINDOW", "120"))
BASELINE_MULT = float(os.environ.get("TSE_BASELINE_MULT", "4.0"))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "TRADE_SUPPRESSION_ENGINE_FLOAT_PARSE_FAILED",
            e,
            value_repr=repr(x),
        )
        return float(default)


def _mean(vals: List[float]) -> float:
    if not vals:
        return 0.0
    return float(sum(vals) / float(len(vals)))


def _stdev(vals: List[float]) -> float:
    n = len(vals)
    if n <= 1:
        return 0.0
    mu = _mean(vals)
    var = sum((float(v) - mu) ** 2 for v in vals) / float(max(1, n - 1))
    return float(math.sqrt(max(0.0, var)))


def _zscore(cur: float, hist: List[float]) -> float:
    if not hist:
        return 0.0
    mu = _mean(hist)
    sd = _stdev(hist)
    if sd <= 1e-9:
        return 0.0
    return float((float(cur) - float(mu)) / float(sd))


def _ensure_tables(con) -> None:
    # Suppression state is persisted so policy engines and dashboards can read
    # the latest throttle/block regime without recomputing it inline.
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS trade_suppression_state (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          ts_ms INTEGER NOT NULL,
          state TEXT NOT NULL,
          action TEXT NOT NULL,
          fp_streak INTEGER,
          execution_degradation_json TEXT,
          latency_mean_ms REAL,
          latency_var REAL,
          latency_var_z REAL,
          slippage_mean_bps REAL,
          slippage_vol_bps REAL,
          slippage_z REAL,
          size_mult REAL NOT NULL DEFAULT 1.0,
          throttle_mult REAL NOT NULL DEFAULT 1.0,
          hard_block INTEGER NOT NULL DEFAULT 0,
          reason TEXT,
          audit_json TEXT
        );

        CREATE TABLE IF NOT EXISTS trade_suppression_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          actor TEXT,
          mode TEXT,
          broker TEXT,
          state TEXT NOT NULL,
          action TEXT NOT NULL,
          fp_streak INTEGER,
          slippage_z REAL,
          latency_var_z REAL,
          execution_degradation_json TEXT,
          size_mult REAL NOT NULL,
          throttle_mult REAL NOT NULL,
          hard_block INTEGER NOT NULL,
          reason TEXT,
          audit_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_trade_suppression_audit_ts
          ON trade_suppression_audit(ts_ms);
        """
    )


def _latest_execution_rows(con, n: int) -> List[tuple]:
    try:
        return con.execute(
            """
            SELECT ts_ms, slippage_bps, age_ms
            FROM execution_analytics
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(max(20, n)),),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "TRADE_SUPPRESSION_ENGINE_EXECUTION_ROWS_FAILED",
            e,
            table="execution_analytics",
        )
        return []


def _latest_provider_rows(con, n: int) -> List[tuple]:
    try:
        return con.execute(
            """
            SELECT ts_ms, latency_ms
            FROM price_provider_health
            WHERE ok = 1
              AND latency_ms IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(max(20, n)),),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "TRADE_SUPPRESSION_ENGINE_PROVIDER_ROWS_FAILED",
            e,
            table="price_provider_health",
        )
        return []


def _compute_slippage_stats(con) -> Dict[str, Any]:
    # Compare recent slippage volatility against historical chunks so the
    # suppression decision reacts to regime shifts, not just high absolute cost.
    rows = _latest_execution_rows(con, EXEC_ANALYTICS_WINDOW)
    slips: List[float] = []
    for _, slippage_bps, _ in rows or []:
        try:
            slips.append(float(slippage_bps))
        except Exception as e:
            _warn_nonfatal(
                "TRADE_SUPPRESSION_ENGINE_SLIPPAGE_PARSE_FAILED",
                e,
                raw_value=repr(slippage_bps),
            )
            continue

    if len(slips) < 10:
        return {
            "n": int(len(slips)),
            "mean_bps": 0.0,
            "vol_bps": 0.0,
            "z": 0.0,
        }

    cur = slips[: max(10, min(30, len(slips) // 3))]
    hist = slips[max(10, min(30, len(slips) // 3)) :]
    cur_vol = _stdev(cur)
    hist_chunks: List[float] = []

    chunk = max(10, len(cur))
    for i in range(0, len(hist), chunk):
        sub = hist[i : i + chunk]
        if len(sub) >= 5:
            hist_chunks.append(_stdev(sub))

    return {
        "n": int(len(slips)),
        "mean_bps": float(_mean(cur)),
        "vol_bps": float(cur_vol),
        "z": float(_zscore(cur_vol, hist_chunks)),
    }


def _compute_latency_stats(con) -> Dict[str, Any]:
    # Provider latency is used as a proxy for market-data execution stress.
    rows = _latest_provider_rows(con, PROVIDER_WINDOW)
    lats: List[float] = []
    for _, latency_ms in rows or []:
        try:
            lats.append(float(latency_ms))
        except Exception as e:
            _warn_nonfatal(
                "TRADE_SUPPRESSION_ENGINE_LATENCY_PARSE_FAILED",
                e,
                raw_value=repr(latency_ms),
            )
            continue

    if len(lats) < 10:
        return {
            "n": int(len(lats)),
            "mean_ms": 0.0,
            "var_ms2": 0.0,
            "var_z": 0.0,
        }

    cur = lats[: max(10, min(30, len(lats) // 3))]
    hist = lats[max(10, min(30, len(lats) // 3)) :]
    cur_var = _stdev(cur) ** 2
    hist_chunks: List[float] = []

    chunk = max(10, len(cur))
    for i in range(0, len(hist), chunk):
        sub = hist[i : i + chunk]
        if len(sub) >= 5:
            hist_chunks.append(_stdev(sub) ** 2)

    return {
        "n": int(len(lats)),
        "mean_ms": float(_mean(cur)),
        "var_ms2": float(cur_var),
        "var_z": float(_zscore(cur_var, hist_chunks)),
    }


def _action_from_metrics(
    *,
    fp_streak: int,
    slip_z: float,
    lat_var_z: float,
    execution_degradation: Dict[str, Any],
) -> Dict[str, Any]:
    hard_hits: List[str] = []
    soft_hits: List[str] = []
    size_hits: List[str] = []

    mean_slip = _safe_float(execution_degradation.get("mean_slippage"), 0.0)
    p95_slip = _safe_float(execution_degradation.get("p95_slippage"), 0.0)
    mean_lat = _safe_float(execution_degradation.get("mean_latency"), 0.0)
    p95_lat = _safe_float(execution_degradation.get("p95_latency"), 0.0)

    if int(fp_streak) >= int(FP_HARD_THRESHOLD):
        hard_hits.append(f"fp_streak>={FP_HARD_THRESHOLD}")
    elif int(fp_streak) >= int(FP_SOFT_THRESHOLD):
        soft_hits.append(f"fp_streak>={FP_SOFT_THRESHOLD}")
    elif int(fp_streak) >= int(FP_SIZE_THRESHOLD):
        size_hits.append(f"fp_streak>={FP_SIZE_THRESHOLD}")

    if float(slip_z) >= float(SLIPVOL_Z_HARD_THRESHOLD):
        hard_hits.append(f"slippage_z>={SLIPVOL_Z_HARD_THRESHOLD}")
    elif float(slip_z) >= float(SLIPVOL_Z_SOFT_THRESHOLD):
        soft_hits.append(f"slippage_z>={SLIPVOL_Z_SOFT_THRESHOLD}")
    elif float(slip_z) >= float(SLIPVOL_Z_SIZE_THRESHOLD):
        size_hits.append(f"slippage_z>={SLIPVOL_Z_SIZE_THRESHOLD}")

    if float(lat_var_z) >= float(LATVAR_Z_HARD_THRESHOLD):
        hard_hits.append(f"latency_var_z>={LATVAR_Z_HARD_THRESHOLD}")
    elif float(lat_var_z) >= float(LATVAR_Z_SOFT_THRESHOLD):
        soft_hits.append(f"latency_var_z>={LATVAR_Z_SOFT_THRESHOLD}")
    elif float(lat_var_z) >= float(LATVAR_Z_SIZE_THRESHOLD):
        size_hits.append(f"latency_var_z>={LATVAR_Z_SIZE_THRESHOLD}")

    if mean_slip >= float(DEGRADE_MEAN_SLIPPAGE_HARD):
        hard_hits.append(f"mean_slippage>={DEGRADE_MEAN_SLIPPAGE_HARD}")
    elif mean_slip >= float(DEGRADE_MEAN_SLIPPAGE_SOFT):
        soft_hits.append(f"mean_slippage>={DEGRADE_MEAN_SLIPPAGE_SOFT}")

    if p95_slip >= float(DEGRADE_P95_SLIPPAGE_HARD):
        hard_hits.append(f"p95_slippage>={DEGRADE_P95_SLIPPAGE_HARD}")
    elif p95_slip >= float(DEGRADE_P95_SLIPPAGE_SOFT):
        soft_hits.append(f"p95_slippage>={DEGRADE_P95_SLIPPAGE_SOFT}")

    if mean_lat >= float(DEGRADE_MEAN_LATENCY_HARD):
        hard_hits.append(f"mean_latency>={DEGRADE_MEAN_LATENCY_HARD}")
    elif mean_lat >= float(DEGRADE_MEAN_LATENCY_SOFT):
        soft_hits.append(f"mean_latency>={DEGRADE_MEAN_LATENCY_SOFT}")

    if p95_lat >= float(DEGRADE_P95_LATENCY_HARD):
        hard_hits.append(f"p95_latency>={DEGRADE_P95_LATENCY_HARD}")
    elif p95_lat >= float(DEGRADE_P95_LATENCY_SOFT):
        soft_hits.append(f"p95_latency>={DEGRADE_P95_LATENCY_SOFT}")

    if hard_hits:
        return {
            "state": "HARD_BLOCK",
            "action": "HARD_BLOCK",
            "size_mult": float(HARD_BLOCK_MULT),
            "throttle_mult": 0.0,
            "hard_block": 1,
            "reason": "|".join(hard_hits),
            "hard_hits": hard_hits,
            "soft_hits": soft_hits,
            "size_hits": size_hits,
        }

    if len(soft_hits) >= 2 or (soft_hits and size_hits):
        return {
            "state": "SOFT_THROTTLE",
            "action": "SOFT_THROTTLE",
            "size_mult": float(SOFT_THROTTLE_MULT),
            "throttle_mult": float(SOFT_THROTTLE_MULT),
            "hard_block": 0,
            "reason": "|".join(soft_hits + size_hits),
            "hard_hits": hard_hits,
            "soft_hits": soft_hits,
            "size_hits": size_hits,
        }

    if soft_hits or size_hits:
        return {
            "state": "SIZE_COMPRESSION",
            "action": "SIZE_COMPRESSION",
            "size_mult": float(SIZE_COMPRESSION_MULT),
            "throttle_mult": 1.0,
            "hard_block": 0,
            "reason": "|".join(soft_hits + size_hits),
            "hard_hits": hard_hits,
            "soft_hits": soft_hits,
            "size_hits": size_hits,
        }

    return {
        "state": "NONE",
        "action": "NONE",
        "size_mult": 1.0,
        "throttle_mult": 1.0,
        "hard_block": 0,
        "reason": "normal",
        "hard_hits": [],
        "soft_hits": [],
        "size_hits": [],
    }


def evaluate_trade_suppression(
    *,
    con=None,
    actor: str = "system",
    mode: str = "unknown",
    broker: str = "unknown",
) -> Dict[str, Any]:
    init_db()

    owns = False
    if con is None:
        con = connect()
        owns = True

    try:
        _ensure_tables(con)

        fp_streak = 0
        try:
            fp_streak = int(get_false_positive_streak(con) or 0)
        except Exception:
            fp_streak = 0

        execution_degradation = get_execution_degradation_snapshot(con, lookback_n=int(BASELINE_MULT * EXEC_ANALYTICS_WINDOW)) or {}
        slip = _compute_slippage_stats(con)
        lat = _compute_latency_stats(con)

        action = _action_from_metrics(
            fp_streak=int(fp_streak),
            slip_z=float(slip.get("z") or 0.0),
            lat_var_z=float(lat.get("var_z") or 0.0),
            execution_degradation=execution_degradation,
        )

        ts_ms = _now_ms()
        audit = {
            "fp_streak": int(fp_streak),
            "slippage": slip,
            "latency": lat,
            "execution_degradation": execution_degradation,
            "hard_hits": list(action.get("hard_hits") or []),
            "soft_hits": list(action.get("soft_hits") or []),
            "size_hits": list(action.get("size_hits") or []),
            "actor": str(actor or "system"),
            "mode": str(mode or "unknown"),
            "broker": str(broker or "unknown"),
        }

        con.execute(
            """
            INSERT OR REPLACE INTO trade_suppression_state(
              id, ts_ms, state, action, fp_streak, execution_degradation_json,
              latency_mean_ms, latency_var, latency_var_z,
              slippage_mean_bps, slippage_vol_bps, slippage_z,
              size_mult, throttle_mult, hard_block, reason, audit_json
            )
            VALUES (1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                str(action.get("state") or "NONE"),
                str(action.get("action") or "NONE"),
                int(fp_streak),
                json.dumps(execution_degradation or {}, separators=(",", ":"), sort_keys=True),
                float(lat.get("mean_ms") or 0.0),
                float(lat.get("var_ms2") or 0.0),
                float(lat.get("var_z") or 0.0),
                float(slip.get("mean_bps") or 0.0),
                float(slip.get("vol_bps") or 0.0),
                float(slip.get("z") or 0.0),
                float(action.get("size_mult") or 1.0),
                float(action.get("throttle_mult") or 1.0),
                int(action.get("hard_block") or 0),
                str(action.get("reason") or ""),
                json.dumps(audit, separators=(",", ":"), sort_keys=True),
            ),
        )

        con.execute(
            """
            INSERT INTO trade_suppression_audit(
              ts_ms, actor, mode, broker, state, action, fp_streak,
              slippage_z, latency_var_z, execution_degradation_json,
              size_mult, throttle_mult, hard_block, reason, audit_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                str(actor or "system"),
                str(mode or "unknown"),
                str(broker or "unknown"),
                str(action.get("state") or "NONE"),
                str(action.get("action") or "NONE"),
                int(fp_streak),
                float(slip.get("z") or 0.0),
                float(lat.get("var_z") or 0.0),
                json.dumps(execution_degradation or {}, separators=(",", ":"), sort_keys=True),
                float(action.get("size_mult") or 1.0),
                float(action.get("throttle_mult") or 1.0),
                int(action.get("hard_block") or 0),
                str(action.get("reason") or ""),
                json.dumps(audit, separators=(",", ":"), sort_keys=True),
            ),
        )

        try:
            set_state("tse_state", str(action.get("state") or "NONE"))
            set_state("tse_action", str(action.get("action") or "NONE"))
            set_state("tse_reason", str(action.get("reason") or ""))
            set_state("tse_size_mult", str(float(action.get("size_mult") or 1.0)))
            set_state("tse_throttle_mult", str(float(action.get("throttle_mult") or 1.0)))
            set_state("execution_pause", "1" if int(action.get("hard_block") or 0) == 1 else "0")
        except Exception as e:
            _warn_nonfatal("TRADE_SUPPRESSION_ENGINE_STATE_WRITE_FAILED", e, state=str(action.get("state") or "NONE"))

        return {
            "ok": True,
            "ts_ms": int(ts_ms),
            "state": str(action.get("state") or "NONE"),
            "action": str(action.get("action") or "NONE"),
            "fp_streak": int(fp_streak),
            "size_mult": float(action.get("size_mult") or 1.0),
            "throttle_mult": float(action.get("throttle_mult") or 1.0),
            "hard_block": bool(int(action.get("hard_block") or 0)),
            "reason": str(action.get("reason") or ""),
            "slippage_mean_bps": float(slip.get("mean_bps") or 0.0),
            "slippage_vol_bps": float(slip.get("vol_bps") or 0.0),
            "slippage_z": float(slip.get("z") or 0.0),
            "latency_mean_ms": float(lat.get("mean_ms") or 0.0),
            "latency_var": float(lat.get("var_ms2") or 0.0),
            "latency_var_z": float(lat.get("var_z") or 0.0),
            "execution_degradation": execution_degradation,
            "audit": audit,
        }
    finally:
        try:
            con.commit()
        except Exception as e:
            if not _is_db_cleanup_error(e):
                raise
            _record_trade_suppression_degraded("commit_failed", e)
            try:
                con.rollback()
            except Exception as rollback_error:
                if not _is_db_cleanup_error(rollback_error):
                    raise
                _record_trade_suppression_degraded("rollback_after_commit_failed", rollback_error)
                LOG.exception("trade_suppression_rollback_failed")
            LOG.exception("trade_suppression_commit_failed")
            raise
        if owns:
            try:
                con.close()
            except Exception as e:
                if not _is_db_cleanup_error(e):
                    raise
                _record_trade_suppression_degraded("close_failed", e)
                LOG.exception("trade_suppression_close_failed")
