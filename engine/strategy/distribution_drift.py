"""
FILE: distribution_drift.py

Measures drift in both feature distributions and model residuals, then stores a
 simple normalized state used by downstream safety and monitoring logic.
"""

import json
import logging
import math
import os
import time
from typing import Any, Dict, Optional

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.strategy.distribution_drift")


FEATURE_RECENT_N = int(os.environ.get("FEATURE_DRIFT_RECENT_N", "200"))
FEATURE_BASELINE_N = int(os.environ.get("FEATURE_DRIFT_BASELINE_N", "1000"))
FEATURE_MIN_N = int(os.environ.get("FEATURE_DRIFT_MIN_N", "60"))

RESIDUAL_RECENT_N = int(os.environ.get("RESIDUAL_DRIFT_RECENT_N", "80"))
RESIDUAL_BASELINE_N = int(os.environ.get("RESIDUAL_DRIFT_BASELINE_N", "320"))
RESIDUAL_MIN_N = int(os.environ.get("RESIDUAL_DRIFT_MIN_N", "40"))

FEATURE_SHIFT_Z_THR = float(os.environ.get("FEATURE_SHIFT_Z_THR", "2.0"))
RESIDUAL_SHIFT_Z_THR = float(os.environ.get("RESIDUAL_SHIFT_Z_THR", "2.0"))
RESIDUAL_ABS_RATIO_THR = float(os.environ.get("RESIDUAL_ABS_RATIO_THR", "1.35"))

DRIFT_WARN_THR = float(os.environ.get("DISTRIBUTION_DRIFT_WARN_THR", "0.45"))
DRIFT_CRITICAL_THR = float(os.environ.get("DISTRIBUTION_DRIFT_CRITICAL_THR", "0.75"))
DRIFT_STALE_AFTER_MS = int(os.environ.get("DISTRIBUTION_DRIFT_STALE_AFTER_MS", str(6 * 60 * 60 * 1000)))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="distribution_drift_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.distribution_drift",
        extra=extra or None,
        persist=False,
    )


def _safe_float(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(d)
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "DISTRIBUTION_DRIFT_FLOAT_PARSE_FAILED",
            e,
            value_repr=repr(x),
        )
        return float(d)


def _safe_json_dumps(v: Any) -> str:
    try:
        return json.dumps(v, separators=(",", ":"), sort_keys=True)
    except Exception as e:
        _warn_nonfatal(
            "DISTRIBUTION_DRIFT_JSON_DUMPS_FAILED",
            e,
            value_type=type(v).__name__,
        )
        return "{}"


def _drift_score_from_shift(shift_z: float, thr: float) -> float:
    thr = max(1e-9, float(thr))
    x = max(0.0, float(shift_z)) / thr
    return float(max(0.0, min(1.0, x)))


def _compute_feature_shift_stats(vals_recent, vals_base) -> Optional[Dict[str, float]]:
    if len(vals_recent) < FEATURE_MIN_N or len(vals_base) < FEATURE_MIN_N:
        return None

    recent = np.asarray(vals_recent, dtype=float)
    base = np.asarray(vals_base, dtype=float)

    if recent.size < FEATURE_MIN_N or base.size < FEATURE_MIN_N:
        return None

    recent_mean = float(np.mean(recent))
    baseline_mean = float(np.mean(base))
    baseline_std = float(np.std(base))

    denom = max(1e-6, baseline_std)
    shift_z = float(abs(recent_mean - baseline_mean) / denom)
    drift_score = _drift_score_from_shift(shift_z, FEATURE_SHIFT_Z_THR)

    return {
        "recent_mean": recent_mean,
        "baseline_mean": baseline_mean,
        "baseline_std": baseline_std,
        "shift_z": shift_z,
        "drift_score": drift_score,
        "drift_flag": 1 if shift_z >= FEATURE_SHIFT_Z_THR else 0,
    }


def _compute_residual_shift_stats(vals_recent, vals_base) -> Optional[Dict[str, float]]:
    if len(vals_recent) < RESIDUAL_MIN_N or len(vals_base) < RESIDUAL_MIN_N:
        return None

    recent = np.asarray(vals_recent, dtype=float)
    base = np.asarray(vals_base, dtype=float)

    if recent.size < RESIDUAL_MIN_N or base.size < RESIDUAL_MIN_N:
        return None

    recent_mean = float(np.mean(recent))
    baseline_mean = float(np.mean(base))
    baseline_std = float(np.std(base))
    shift_z = float(abs(recent_mean - baseline_mean) / max(1e-6, baseline_std))

    abs_mean_recent = float(np.mean(np.abs(recent)))
    abs_mean_base = float(np.mean(np.abs(base)))
    abs_shift_ratio = float(abs_mean_recent / max(1e-6, abs_mean_base))

    score_z = _drift_score_from_shift(shift_z, RESIDUAL_SHIFT_Z_THR)
    score_abs = _drift_score_from_shift(abs_shift_ratio - 1.0, RESIDUAL_ABS_RATIO_THR - 1.0)
    drift_score = float(max(score_z, score_abs))
    drift_flag = 1 if (shift_z >= RESIDUAL_SHIFT_Z_THR or abs_shift_ratio >= RESIDUAL_ABS_RATIO_THR) else 0

    return {
        "recent_mean": recent_mean,
        "baseline_mean": baseline_mean,
        "baseline_std": baseline_std,
        "shift_z": shift_z,
        "abs_mean_recent": abs_mean_recent,
        "abs_mean_base": abs_mean_base,
        "abs_shift_ratio": abs_shift_ratio,
        "drift_score": drift_score,
        "drift_flag": int(drift_flag),
    }


def _drift_state_from_scores(feature_score: float, residual_score: float, ts_ms: int) -> str:
    # Staleness is treated as its own state so callers can distinguish "healthy"
    # from "no fresh drift measurement available".
    age_ms = max(0, _now_ms() - int(ts_ms or 0))
    worst = max(float(feature_score), float(residual_score))

    if ts_ms <= 0 or age_ms > DRIFT_STALE_AFTER_MS:
        return "STALE"
    if worst >= DRIFT_CRITICAL_THR:
        return "CRITICAL"
    if worst >= DRIFT_WARN_THR:
        return "WARN"
    return "NORMAL"


def compute_and_store_distribution_drift() -> Dict[str, Any]:
    now_ms = _now_ms()
    con = connect()

    try:
        out: Dict[str, Any] = {
            "ts_ms": int(now_ms),
            "feature_distribution_drift": {},
            "residual_distribution_drift": {},
        }

        # --------------------------------------------------
        # Feature distribution drift (factor_features)
        # --------------------------------------------------
        try:
            rows = con.execute(
                """
                SELECT feature_id, value
                FROM factor_features
                WHERE value IS NOT NULL
                ORDER BY asof_ts DESC, effective_ts DESC
                """
            ).fetchall()
        except Exception:
            rows = []

        feat_buckets: Dict[str, list] = {}
        for feature_id, value in rows or []:
            try:
                feat_buckets.setdefault(str(feature_id), []).append(float(value))
            except Exception as e:
                _warn_nonfatal(
                    "DISTRIBUTION_DRIFT_FEATURE_VALUE_PARSE_FAILED",
                    e,
                    feature_id=str(feature_id),
                    raw_value=repr(value),
                )
                continue

        for feature_id, vals in feat_buckets.items():
            if len(vals) < FEATURE_MIN_N:
                continue

            vals_recent = vals[:FEATURE_RECENT_N]
            vals_base = vals[FEATURE_RECENT_N : FEATURE_RECENT_N + FEATURE_BASELINE_N]

            stats = _compute_feature_shift_stats(vals_recent, vals_base)
            if not stats:
                continue

            con.execute(
                """
                INSERT INTO feature_distribution_drift(
                  feature_id, ts_ms, recent_n, baseline_n,
                  recent_mean, baseline_mean, baseline_std,
                  shift_z, drift_score, drift_flag, meta_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(feature_id) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  recent_n=excluded.recent_n,
                  baseline_n=excluded.baseline_n,
                  recent_mean=excluded.recent_mean,
                  baseline_mean=excluded.baseline_mean,
                  baseline_std=excluded.baseline_std,
                  shift_z=excluded.shift_z,
                  drift_score=excluded.drift_score,
                  drift_flag=excluded.drift_flag,
                  meta_json=excluded.meta_json
                """,
                (
                    str(feature_id),
                    int(now_ms),
                    int(len(vals_recent)),
                    int(len(vals_base)),
                    float(stats["recent_mean"]),
                    float(stats["baseline_mean"]),
                    float(stats["baseline_std"]),
                    float(stats["shift_z"]),
                    float(stats["drift_score"]),
                    int(stats["drift_flag"]),
                    _safe_json_dumps(
                        {
                            "feature_recent_n": int(len(vals_recent)),
                            "feature_baseline_n": int(len(vals_base)),
                            "feature_shift_z_thr": float(FEATURE_SHIFT_Z_THR),
                        }
                    ),
                ),
            )

            out["feature_distribution_drift"][str(feature_id)] = {
                "recent_n": int(len(vals_recent)),
                "baseline_n": int(len(vals_base)),
                **stats,
            }

        # --------------------------------------------------
        # Residual distribution drift (global + per-symbol)
        # --------------------------------------------------
        residual_rows_global = []
        try:
            residual_rows_global = con.execute(
                """
                SELECT symbol, residual_pnl
                FROM pnl_decomposition
                WHERE residual_pnl IS NOT NULL
                ORDER BY ts_ms DESC
                """
            ).fetchall()
        except Exception:
            residual_rows_global = []

        residual_buckets: Dict[tuple, list] = {("global", "__all__"): []}
        for symbol, residual_pnl in residual_rows_global or []:
            try:
                sym = str(symbol or "").upper().strip()
                val = float(residual_pnl)
            except Exception as e:
                _warn_nonfatal(
                    "DISTRIBUTION_DRIFT_RESIDUAL_VALUE_PARSE_FAILED",
                    e,
                    symbol=str(symbol or ""),
                    raw_value=repr(residual_pnl),
                )
                continue
            residual_buckets[("global", "__all__")].append(val)
            if sym:
                residual_buckets.setdefault(("symbol", sym), []).append(val)

        for (scope, symbol), vals in residual_buckets.items():
            min_n = RESIDUAL_MIN_N if scope == "global" else max(RESIDUAL_MIN_N, 20)
            if len(vals) < min_n:
                continue

            vals_recent = vals[:RESIDUAL_RECENT_N]
            vals_base = vals[RESIDUAL_RECENT_N : RESIDUAL_RECENT_N + RESIDUAL_BASELINE_N]

            stats = _compute_residual_shift_stats(vals_recent, vals_base)
            if not stats:
                continue

            con.execute(
                """
                INSERT INTO residual_distribution_drift(
                  scope, symbol, ts_ms, recent_n, baseline_n,
                  recent_mean, baseline_mean, baseline_std,
                  shift_z, abs_mean_recent, abs_mean_base, abs_shift_ratio,
                  drift_score, drift_flag, meta_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(scope, symbol) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  recent_n=excluded.recent_n,
                  baseline_n=excluded.baseline_n,
                  recent_mean=excluded.recent_mean,
                  baseline_mean=excluded.baseline_mean,
                  baseline_std=excluded.baseline_std,
                  shift_z=excluded.shift_z,
                  abs_mean_recent=excluded.abs_mean_recent,
                  abs_mean_base=excluded.abs_mean_base,
                  abs_shift_ratio=excluded.abs_shift_ratio,
                  drift_score=excluded.drift_score,
                  drift_flag=excluded.drift_flag,
                  meta_json=excluded.meta_json
                """,
                (
                    str(scope),
                    str(symbol),
                    int(now_ms),
                    int(len(vals_recent)),
                    int(len(vals_base)),
                    float(stats["recent_mean"]),
                    float(stats["baseline_mean"]),
                    float(stats["baseline_std"]),
                    float(stats["shift_z"]),
                    float(stats["abs_mean_recent"]),
                    float(stats["abs_mean_base"]),
                    float(stats["abs_shift_ratio"]),
                    float(stats["drift_score"]),
                    int(stats["drift_flag"]),
                    _safe_json_dumps(
                        {
                            "residual_recent_n": int(len(vals_recent)),
                            "residual_baseline_n": int(len(vals_base)),
                            "residual_shift_z_thr": float(RESIDUAL_SHIFT_Z_THR),
                            "residual_abs_ratio_thr": float(RESIDUAL_ABS_RATIO_THR),
                        }
                    ),
                ),
            )

            out["residual_distribution_drift"][f"{scope}:{symbol}"] = {
                "recent_n": int(len(vals_recent)),
                "baseline_n": int(len(vals_base)),
                **stats,
            }

        con.commit()
        return out

    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("DISTRIBUTION_DRIFT_CLOSE_FAILED", e, operation="compute_distribution_drift_snapshot")


def get_latest_distribution_drift_snapshot(symbol: Optional[str] = None, con=None) -> Dict[str, Any]:
    owns = False
    if con is None:
        con = connect()
        owns = True

    try:
        feature_rows = []
        try:
            feature_rows = con.execute(
                """
                SELECT feature_id, ts_ms, recent_n, baseline_n, shift_z, drift_score, drift_flag
                FROM feature_distribution_drift
                ORDER BY drift_score DESC, shift_z DESC, feature_id ASC
                """
            ).fetchall()
        except Exception:
            feature_rows = []

        feature_count = int(len(feature_rows))
        feature_max_shift_z = 0.0
        feature_max_score = 0.0
        feature_flagged = 0
        feature_top = []

        for row in feature_rows[:5]:
            try:
                feature_top.append(
                    {
                        "feature_id": str(row[0]),
                        "ts_ms": int(row[1] or 0),
                        "recent_n": int(row[2] or 0),
                        "baseline_n": int(row[3] or 0),
                        "shift_z": float(row[4] or 0.0),
                        "drift_score": float(row[5] or 0.0),
                        "drift_flag": int(row[6] or 0),
                    }
                )
            except Exception as e:
                _warn_nonfatal(
                    "DISTRIBUTION_DRIFT_FEATURE_ROW_PARSE_FAILED",
                    e,
                    row_repr=repr(row),
                )
                continue

        for row in feature_rows:
            try:
                feature_max_shift_z = max(feature_max_shift_z, float(row[4] or 0.0))
                feature_max_score = max(feature_max_score, float(row[5] or 0.0))
                feature_flagged += int(row[6] or 0)
            except Exception as e:
                _warn_nonfatal(
                    "DISTRIBUTION_DRIFT_FEATURE_SUMMARY_PARSE_FAILED",
                    e,
                    row_repr=repr(row),
                )
                continue

        sym = str(symbol or "").upper().strip()
        residual_row = None

        if sym:
            try:
                residual_row = con.execute(
                    """
                    SELECT scope, symbol, ts_ms, recent_n, baseline_n,
                           shift_z, abs_shift_ratio, drift_score, drift_flag
                    FROM residual_distribution_drift
                    WHERE scope='symbol' AND symbol=?
                    LIMIT 1
                    """,
                    (sym,),
                ).fetchone()
            except Exception:
                residual_row = None

        if not residual_row:
            try:
                residual_row = con.execute(
                    """
                    SELECT scope, symbol, ts_ms, recent_n, baseline_n,
                           shift_z, abs_shift_ratio, drift_score, drift_flag
                    FROM residual_distribution_drift
                    WHERE scope='global' AND symbol='__all__'
                    LIMIT 1
                    """
                ).fetchone()
            except Exception:
                residual_row = None

        residual = {
            "scope": "global",
            "symbol": "__all__",
            "ts_ms": 0,
            "recent_n": 0,
            "baseline_n": 0,
            "shift_z": 0.0,
            "abs_shift_ratio": 1.0,
            "drift_score": 0.0,
            "drift_flag": 0,
        }

        if residual_row:
            try:
                residual = {
                    "scope": str(residual_row[0]),
                    "symbol": str(residual_row[1]),
                    "ts_ms": int(residual_row[2] or 0),
                    "recent_n": int(residual_row[3] or 0),
                    "baseline_n": int(residual_row[4] or 0),
                    "shift_z": float(residual_row[5] or 0.0),
                    "abs_shift_ratio": float(residual_row[6] or 1.0),
                    "drift_score": float(residual_row[7] or 0.0),
                    "drift_flag": int(residual_row[8] or 0),
                }
            except Exception as e:
                _warn_nonfatal(
                    "DISTRIBUTION_DRIFT_RESIDUAL_ROW_PARSE_FAILED",
                    e,
                    symbol=str(symbol or ""),
                )

        latest_ts_ms = 0
        try:
            latest_ts_ms = max(
                max((int(r[1] or 0) for r in (feature_rows or [])), default=0),
                int(residual.get("ts_ms", 0) or 0),
            )
        except Exception:
            latest_ts_ms = int(residual.get("ts_ms", 0) or 0)

        state = _drift_state_from_scores(
            feature_max_score,
            float(residual.get("drift_score", 0.0) or 0.0),
            latest_ts_ms,
        )
        stable_score = max(
            0.0,
            min(
                1.0,
                1.0 - max(float(feature_max_score), float(residual.get("drift_score", 0.0) or 0.0)),
            ),
        )

        return {
            "feature_shift": {
                "count": feature_count,
                "flagged": int(feature_flagged),
                "max_shift_z": float(feature_max_shift_z),
                "max_drift_score": float(feature_max_score),
                "top": feature_top,
            },
            "residual_shift": residual,
            "state": str(state),
            "stable_score": float(stable_score),
            "ts_ms": int(latest_ts_ms),
        }

    finally:
        if owns:
            try:
                con.close()
            except Exception:
                LOG.exception("distribution_drift_close_failed")
