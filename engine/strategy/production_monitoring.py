"""Production drift, calibration, and shadow-vs-live monitoring.

The monitor composes existing production evidence tables into bounded latest
state metrics.  Threshold breaches emit retrain or shadow-review signals through
``drift_retrain_events`` only; this module never promotes a model or marks a
candidate live-ready.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any, Iterable, Mapping

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.strategy.conformal import extract_conformal_payload


LOG = get_logger("engine.strategy.production_monitoring")
MONITOR_SOURCE = "production_monitoring"
STATUS_META_KEY = "production_monitoring_status"

RECENT_N = int(os.environ.get("PRODUCTION_MONITOR_RECENT_N", "80"))
BASELINE_N = int(os.environ.get("PRODUCTION_MONITOR_BASELINE_N", "320"))
MIN_N = int(os.environ.get("PRODUCTION_MONITOR_MIN_N", "30"))
SIGNAL_COOLDOWN_S = int(os.environ.get("PRODUCTION_MONITOR_SIGNAL_COOLDOWN_S", str(60 * 60)))

FEATURE_DRIFT_WARN = float(os.environ.get("PRODUCTION_FEATURE_DRIFT_WARN", os.environ.get("DISTRIBUTION_DRIFT_WARN_THR", "0.45")))
FEATURE_DRIFT_CRIT = float(os.environ.get("PRODUCTION_FEATURE_DRIFT_CRIT", os.environ.get("DISTRIBUTION_DRIFT_CRITICAL_THR", "0.75")))
MISSING_FEATURE_WARN = float(os.environ.get("PRODUCTION_MISSING_FEATURE_RATE_WARN", "0.05"))
MISSING_FEATURE_CRIT = float(os.environ.get("PRODUCTION_MISSING_FEATURE_RATE_CRIT", "0.15"))
PREDICTION_SHIFT_Z_WARN = float(os.environ.get("PRODUCTION_PREDICTION_SHIFT_Z_WARN", "2.0"))
PREDICTION_SHIFT_Z_CRIT = float(os.environ.get("PRODUCTION_PREDICTION_SHIFT_Z_CRIT", "3.0"))
LABEL_SHIFT_Z_WARN = float(os.environ.get("PRODUCTION_LABEL_SHIFT_Z_WARN", "2.0"))
LABEL_SHIFT_Z_CRIT = float(os.environ.get("PRODUCTION_LABEL_SHIFT_Z_CRIT", "3.0"))
CALIBRATION_ECE_WARN = float(os.environ.get("PRODUCTION_CALIBRATION_ECE_WARN", "0.10"))
CALIBRATION_ECE_CRIT = float(os.environ.get("PRODUCTION_CALIBRATION_ECE_CRIT", "0.20"))
CONFORMAL_COVERAGE_WARN_GAP = float(os.environ.get("PRODUCTION_CONFORMAL_COVERAGE_WARN_GAP", "0.05"))
CONFORMAL_COVERAGE_CRIT_GAP = float(os.environ.get("PRODUCTION_CONFORMAL_COVERAGE_CRIT_GAP", "0.10"))
SHADOW_DISAGREE_WARN = float(os.environ.get("PRODUCTION_SHADOW_DISAGREE_RATE_WARN", "0.25"))
SHADOW_DISAGREE_CRIT = float(os.environ.get("PRODUCTION_SHADOW_DISAGREE_RATE_CRIT", "0.40"))
SHADOW_ABS_DELTA_WARN = float(os.environ.get("PRODUCTION_SHADOW_ABS_DELTA_WARN", "0.75"))
SHADOW_ABS_DELTA_CRIT = float(os.environ.get("PRODUCTION_SHADOW_ABS_DELTA_CRIT", "1.50"))
NET_PNL_DEGRADATION_WARN = float(os.environ.get("PRODUCTION_NET_PNL_DEGRADATION_WARN", "0.0025"))
NET_PNL_DEGRADATION_CRIT = float(os.environ.get("PRODUCTION_NET_PNL_DEGRADATION_CRIT", "0.0100"))

_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.production_monitoring",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _is_sqlite(con: Any) -> bool:
    module = str(getattr(type(con), "__module__", "") or "")
    return module.startswith("sqlite3")


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, "", b"", bytearray()):
        return []
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        out = float(value)
    except Exception:
        return default
    return float(out) if math.isfinite(out) else default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_ident(name: str) -> str:
    ident = str(name or "").strip()
    if not ident or not ident.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier: {name!r}")
    return ident


def _table_exists(con: Any, table: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {_safe_ident(table)} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _table_columns(con: Any, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({_safe_ident(table)})").fetchall()
        cols = {str(row[1]) for row in rows or [] if len(row) > 1}
        if cols:
            return cols
    except Exception as exc:
        LOG.debug(
            "production_monitoring_table_columns_pragma_failed table=%s error=%s",
            str(table),
            exc,
        )
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name=?
            """,
            (str(table),),
        ).fetchall()
        return {str(row[0]) for row in rows or []}
    except Exception:
        return set()


def _first_existing(cols: set[str], *names: str) -> str:
    for name in names:
        if str(name) in cols:
            return str(name)
    return ""


def ensure_production_monitoring_schema(con: Any) -> None:
    """Create the bounded latest-state production monitoring table."""

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS production_monitoring_metrics (
          metric_name TEXT NOT NULL,
          scope TEXT NOT NULL DEFAULT 'global',
          dimension TEXT NOT NULL DEFAULT '',
          ts_ms BIGINT NOT NULL,
          value DOUBLE PRECISION,
          baseline_value DOUBLE PRECISION,
          threshold_value DOUBLE PRECISION,
          severity TEXT NOT NULL DEFAULT 'UNKNOWN',
          state TEXT NOT NULL DEFAULT 'unavailable',
          action_signal TEXT NOT NULL DEFAULT '',
          labels_available BIGINT NOT NULL DEFAULT 0,
          sample_n BIGINT NOT NULL DEFAULT 0,
          details_json JSONB NOT NULL DEFAULT '{}',
          PRIMARY KEY(metric_name, scope, dimension)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_production_monitoring_metrics_ts
          ON production_monitoring_metrics(ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_production_monitoring_metrics_state
          ON production_monitoring_metrics(severity, state, ts_ms DESC)
        """
    )


def _ensure_signal_schema(con: Any) -> None:
    id_col = "INTEGER PRIMARY KEY AUTOINCREMENT" if _is_sqlite(con) else "BIGSERIAL PRIMARY KEY"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS drift_retrain_events (
          id {id_col},
          created_ts BIGINT NOT NULL,
          model_name TEXT NOT NULL DEFAULT '',
          family TEXT,
          trigger_type TEXT,
          trigger_metrics JSONB NOT NULL DEFAULT '{{}}',
          action_taken TEXT,
          cooldown_applied BIGINT NOT NULL DEFAULT 0,
          candidate_version TEXT,
          outcome_status TEXT,
          diagnostics JSONB NOT NULL DEFAULT '{{}}'
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_drift_retrain_events_model_created
          ON drift_retrain_events(model_name, created_ts DESC, id DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_drift_retrain_events_family_created
          ON drift_retrain_events(family, created_ts DESC, id DESC)
        """
    )


def _severity_from_value(value: float | None, warn: float, crit: float, *, higher_bad: bool = True) -> tuple[str, str]:
    if value is None:
        return "UNKNOWN", "unavailable"
    if higher_bad:
        if float(value) >= float(crit):
            return "CRIT", "crit"
        if float(value) >= float(warn):
            return "WARN", "warn"
    else:
        if float(value) <= float(crit):
            return "CRIT", "crit"
        if float(value) <= float(warn):
            return "WARN", "warn"
    return "OK", "ok"


def _metric(
    name: str,
    *,
    value: float | None,
    baseline_value: float | None = None,
    threshold_value: float | None = None,
    severity: str = "UNKNOWN",
    state: str = "unavailable",
    action_signal: str = "",
    labels_available: bool = False,
    sample_n: int = 0,
    scope: str = "global",
    dimension: str = "",
    details: Mapping[str, Any] | None = None,
    ts_ms: int | None = None,
) -> dict[str, Any]:
    return {
        "metric_name": str(name),
        "scope": str(scope or "global"),
        "dimension": str(dimension or ""),
        "ts_ms": int(ts_ms if ts_ms is not None else _now_ms()),
        "value": (None if value is None else float(value)),
        "baseline_value": (None if baseline_value is None else float(baseline_value)),
        "threshold_value": (None if threshold_value is None else float(threshold_value)),
        "severity": str(severity or "UNKNOWN").upper(),
        "state": str(state or "unavailable").lower(),
        "action_signal": str(action_signal or ""),
        "labels_available": bool(labels_available),
        "sample_n": int(sample_n or 0),
        "details": dict(details or {}),
    }


def _window_stats(values: Iterable[Any], *, recent_n: int = RECENT_N, baseline_n: int = BASELINE_N, min_n: int = MIN_N) -> dict[str, Any]:
    vals = [_safe_float(v) for v in list(values or [])]
    vals = [float(v) for v in vals if v is not None]
    recent = vals[: int(recent_n)]
    base = vals[int(recent_n) : int(recent_n) + int(baseline_n)]
    if len(recent) < int(min_n) or len(base) < int(min_n):
        return {"available": False, "n": len(recent), "baseline_n": len(base)}
    recent_arr = np.asarray(recent, dtype=np.float64)
    base_arr = np.asarray(base, dtype=np.float64)
    recent_mean = float(np.mean(recent_arr))
    baseline_mean = float(np.mean(base_arr))
    baseline_std = float(np.std(base_arr))
    shift_z = float(abs(recent_mean - baseline_mean) / max(1.0e-9, baseline_std))
    recent_abs = float(np.mean(np.abs(recent_arr)))
    baseline_abs = float(np.mean(np.abs(base_arr)))
    abs_ratio = float(recent_abs / max(1.0e-9, baseline_abs))
    return {
        "available": True,
        "n": int(len(recent)),
        "baseline_n": int(len(base)),
        "recent_mean": recent_mean,
        "baseline_mean": baseline_mean,
        "baseline_std": baseline_std,
        "shift_z": shift_z,
        "recent_abs_mean": recent_abs,
        "baseline_abs_mean": baseline_abs,
        "abs_ratio": abs_ratio,
    }


def _read_numeric_series(con: Any, table: str, value_columns: tuple[str, ...], *, order_columns: tuple[str, ...] = ("ts_ms",), limit: int | None = None) -> list[float]:
    if not _table_exists(con, table):
        return []
    cols = _table_columns(con, table)
    value_col = _first_existing(cols, *value_columns)
    if not value_col:
        return []
    order_col = _first_existing(cols, *order_columns) or value_col
    max_rows = int(limit or (RECENT_N + BASELINE_N))
    try:
        rows = con.execute(
            f"""
            SELECT {_safe_ident(value_col)}
            FROM {_safe_ident(table)}
            WHERE {_safe_ident(value_col)} IS NOT NULL
            ORDER BY {_safe_ident(order_col)} DESC
            LIMIT ?
            """,
            (max_rows,),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal(
            "PRODUCTION_MONITOR_SERIES_READ_FAILED",
            exc,
            once_key=f"series:{table}:{value_col}",
            table=table,
            value_col=value_col,
        )
        return []
    out: list[float] = []
    for row in rows or []:
        value = _safe_float(row[0])
        if value is not None:
            out.append(float(value))
    return out


def _feature_drift_metric(con: Any, now_ms: int) -> dict[str, Any]:
    if not _table_exists(con, "feature_distribution_drift"):
        return _metric(
            "feature_drift",
            value=None,
            state="unavailable",
            details={"reason": "feature_distribution_drift table unavailable"},
            ts_ms=now_ms,
        )
    rows = con.execute(
        """
        SELECT feature_id, ts_ms, shift_z, drift_score, drift_flag
        FROM feature_distribution_drift
        ORDER BY drift_score DESC, shift_z DESC, feature_id ASC
        LIMIT 25
        """
    ).fetchall()
    if not rows:
        return _metric(
            "feature_drift",
            value=None,
            state="insufficient_data",
            details={"reason": "no feature_distribution_drift rows"},
            ts_ms=now_ms,
        )
    max_score = max(float(_safe_float(row[3], 0.0) or 0.0) for row in rows)
    max_shift = max(float(_safe_float(row[2], 0.0) or 0.0) for row in rows)
    flagged = sum(1 for row in rows if _safe_int(row[4], 0) > 0)
    severity, state = _severity_from_value(max_score, FEATURE_DRIFT_WARN, FEATURE_DRIFT_CRIT)
    return _metric(
        "feature_drift",
        value=max_score,
        baseline_value=0.0,
        threshold_value=FEATURE_DRIFT_WARN,
        severity=severity,
        state=state,
        action_signal=("retrain" if severity in {"WARN", "CRIT"} else ""),
        sample_n=len(rows),
        details={
            "max_shift_z": float(max_shift),
            "flagged_features": int(flagged),
            "top_features": [
                {
                    "feature_id": str(row[0] or ""),
                    "ts_ms": _safe_int(row[1], 0),
                    "shift_z": _safe_float(row[2], 0.0),
                    "drift_score": _safe_float(row[3], 0.0),
                    "drift_flag": _safe_int(row[4], 0),
                }
                for row in rows[:8]
            ],
        },
        ts_ms=now_ms,
    )


def _missing_feature_metric(con: Any, now_ms: int) -> dict[str, Any]:
    if not _table_exists(con, "model_feature_snapshots"):
        return _metric(
            "missing_feature_rate",
            value=None,
            state="unavailable",
            details={"reason": "model_feature_snapshots table unavailable"},
            ts_ms=now_ms,
        )
    rows = con.execute(
        """
        SELECT symbol, ts_ms, feature_ids_json, features_json, vector_json, availability_json
        FROM model_feature_snapshots
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (int(RECENT_N),),
    ).fetchall()
    if not rows:
        return _metric(
            "missing_feature_rate",
            value=None,
            state="insufficient_data",
            details={"reason": "no model_feature_snapshots rows"},
            ts_ms=now_ms,
        )
    expected_total = 0
    missing_total = 0
    group_total = 0
    group_missing = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        feature_ids = [str(x) for x in _json_list(row[2]) if str(x)]
        features = _json_obj(row[3])
        vector = _json_list(row[4])
        availability = _json_obj(row[5])
        expected_total += len(feature_ids)
        for idx, feature_id in enumerate(feature_ids):
            value = features.get(feature_id)
            if value is None and idx < len(vector):
                value = vector[idx]
            parsed = _safe_float(value)
            if parsed is None:
                missing_total += 1
                if len(examples) < 8:
                    examples.append({"symbol": str(row[0] or ""), "ts_ms": _safe_int(row[1], 0), "feature_id": feature_id})
        for group, available in availability.items():
            if str(group).startswith("_"):
                continue
            group_total += 1
            if not bool(available):
                group_missing += 1
    feature_rate = float(missing_total / expected_total) if expected_total else 0.0
    group_rate = float(group_missing / group_total) if group_total else 0.0
    value = max(feature_rate, group_rate)
    severity, state = _severity_from_value(value, MISSING_FEATURE_WARN, MISSING_FEATURE_CRIT)
    return _metric(
        "missing_feature_rate",
        value=value,
        baseline_value=0.0,
        threshold_value=MISSING_FEATURE_WARN,
        severity=severity,
        state=state,
        action_signal=("retrain" if severity in {"WARN", "CRIT"} else ""),
        sample_n=len(rows),
        details={
            "feature_missing_rate": float(feature_rate),
            "group_unavailable_rate": float(group_rate),
            "missing_features": int(missing_total),
            "expected_features": int(expected_total),
            "unavailable_groups": int(group_missing),
            "observed_groups": int(group_total),
            "examples": examples,
        },
        ts_ms=now_ms,
    )


def _prediction_drift_metric(con: Any, now_ms: int) -> dict[str, Any]:
    values = _read_numeric_series(
        con,
        "predictions",
        ("predicted_z", "prediction"),
        order_columns=("ts_ms", "prediction_time", "time"),
    )
    if not values:
        values = _read_numeric_series(
            con,
            "tracked_predictions",
            ("prediction", "predicted_z"),
            order_columns=("ts_ms", "time"),
        )
    stats = _window_stats(values)
    if not stats.get("available"):
        return _metric(
            "prediction_drift",
            value=None,
            state="insufficient_data",
            sample_n=_safe_int(stats.get("n"), 0),
            details={"reason": "not enough prediction rows", **stats},
            ts_ms=now_ms,
        )
    shift_z = float(stats["shift_z"])
    severity, state = _severity_from_value(shift_z, PREDICTION_SHIFT_Z_WARN, PREDICTION_SHIFT_Z_CRIT)
    return _metric(
        "prediction_drift",
        value=shift_z,
        baseline_value=0.0,
        threshold_value=PREDICTION_SHIFT_Z_WARN,
        severity=severity,
        state=state,
        action_signal=("retrain" if severity in {"WARN", "CRIT"} else ""),
        sample_n=_safe_int(stats.get("n"), 0),
        details=stats,
        ts_ms=now_ms,
    )


def _label_values(con: Any, *, limit: int | None = None) -> list[float]:
    if not _table_exists(con, "labels"):
        return []
    cols = _table_columns(con, "labels")
    target_col = _first_existing(cols, "impact_z", "realized_z", "realized_ret", "label")
    if not target_col:
        return []
    order_col = _first_existing(cols, "created_at_ms", "ts_ms", "time") or target_col
    try:
        rows = con.execute(
            f"""
            SELECT {_safe_ident(target_col)}
            FROM labels
            WHERE {_safe_ident(target_col)} IS NOT NULL
            ORDER BY {_safe_ident(order_col)} DESC
            LIMIT ?
            """,
            (int(limit or (RECENT_N + BASELINE_N)),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("PRODUCTION_MONITOR_LABEL_VALUES_FAILED", exc, once_key="label_values")
        return []
    out: list[float] = []
    for row in rows or []:
        value = _safe_float(row[0])
        if value is not None:
            out.append(float(value))
    return out


def _label_drift_metric(con: Any, now_ms: int) -> dict[str, Any]:
    values = _label_values(con)
    if not values:
        return _metric(
            "target_label_drift",
            value=None,
            state="no_labels_yet",
            labels_available=False,
            details={"reason": "no matured labels available"},
            ts_ms=now_ms,
        )
    stats = _window_stats(values)
    if not stats.get("available"):
        return _metric(
            "target_label_drift",
            value=None,
            state="insufficient_labels",
            labels_available=True,
            sample_n=_safe_int(stats.get("n"), 0),
            details={"reason": "not enough matured labels", **stats},
            ts_ms=now_ms,
        )
    shift_z = float(stats["shift_z"])
    severity, state = _severity_from_value(shift_z, LABEL_SHIFT_Z_WARN, LABEL_SHIFT_Z_CRIT)
    return _metric(
        "target_label_drift",
        value=shift_z,
        baseline_value=0.0,
        threshold_value=LABEL_SHIFT_Z_WARN,
        severity=severity,
        state=state,
        action_signal=("retrain" if severity in {"WARN", "CRIT"} else ""),
        labels_available=True,
        sample_n=_safe_int(stats.get("n"), 0),
        details=stats,
        ts_ms=now_ms,
    )


def _labeled_prediction_rows(con: Any, *, limit: int | None = None) -> list[dict[str, Any]]:
    if not (_table_exists(con, "predictions") and _table_exists(con, "labels")):
        return []
    p_cols = _table_columns(con, "predictions")
    l_cols = _table_columns(con, "labels")
    pred_col = _first_existing(p_cols, "predicted_z", "prediction")
    conf_col = _first_existing(p_cols, "confidence", "probability")
    target_col = _first_existing(l_cols, "impact_z", "realized_z", "realized_ret", "label")
    if not pred_col or not conf_col or not target_col:
        return []
    label_ts = _first_existing(l_cols, "created_at_ms", "ts_ms") or "0"
    try:
        rows = con.execute(
            f"""
            SELECT p.ts_ms, p.event_id, p.symbol, p.horizon_s,
                   p.{_safe_ident(pred_col)}, p.{_safe_ident(conf_col)},
                   l.{_safe_ident(target_col)}, l.{_safe_ident(label_ts)}
            FROM predictions p
            JOIN labels l
              ON l.event_id=p.event_id
             AND upper(l.symbol)=upper(p.symbol)
             AND l.horizon_s=p.horizon_s
            WHERE p.{_safe_ident(pred_col)} IS NOT NULL
              AND p.{_safe_ident(conf_col)} IS NOT NULL
              AND l.{_safe_ident(target_col)} IS NOT NULL
            ORDER BY l.{_safe_ident(label_ts)} DESC, p.ts_ms DESC
            LIMIT ?
            """,
            (int(limit or (RECENT_N + BASELINE_N)),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("PRODUCTION_MONITOR_LABELED_PREDICTIONS_FAILED", exc, once_key="labeled_predictions")
        return []
    out: list[dict[str, Any]] = []
    for row in rows or []:
        pred = _safe_float(row[4])
        conf = _safe_float(row[5])
        target = _safe_float(row[6])
        if pred is None or conf is None or target is None:
            continue
        out.append(
            {
                "ts_ms": _safe_int(row[0], 0),
                "event_id": _safe_int(row[1], 0),
                "symbol": str(row[2] or "").upper(),
                "horizon_s": _safe_int(row[3], 0),
                "prediction": float(pred),
                "confidence": max(0.0, min(1.0, float(conf))),
                "target": float(target),
                "label_ts_ms": _safe_int(row[7], 0),
            }
        )
    return out


def _ece(rows: list[dict[str, Any]], *, bins: int = 10) -> float | None:
    if not rows:
        return None
    bucket_count = max(2, int(bins))
    total = len(rows)
    ece = 0.0
    for idx in range(bucket_count):
        lo = idx / bucket_count
        hi = (idx + 1) / bucket_count
        bucket = [
            row
            for row in rows
            if float(row.get("confidence") or 0.0) >= lo and (float(row.get("confidence") or 0.0) < hi or idx == bucket_count - 1)
        ]
        if not bucket:
            continue
        avg_conf = float(np.mean([float(row["confidence"]) for row in bucket]))
        acc = float(
            np.mean(
                [
                    1.0
                    if (float(row["prediction"]) == 0.0 and float(row["target"]) == 0.0)
                    or (float(row["prediction"]) * float(row["target"]) > 0.0)
                    else 0.0
                    for row in bucket
                ]
            )
        )
        ece += (len(bucket) / total) * abs(avg_conf - acc)
    return float(ece)


def _calibration_metric(con: Any, now_ms: int) -> dict[str, Any]:
    rows = _labeled_prediction_rows(con)
    if not rows and not _label_values(con, limit=1):
        return _metric(
            "calibration_ece",
            value=None,
            state="no_labels_yet",
            labels_available=False,
            details={"reason": "no matured labels available"},
            ts_ms=now_ms,
        )
    if len(rows) < MIN_N * 2:
        return _metric(
            "calibration_ece",
            value=None,
            state="insufficient_labels",
            labels_available=bool(rows),
            sample_n=len(rows),
            details={"reason": "not enough labeled prediction rows"},
            ts_ms=now_ms,
        )
    recent = rows[:RECENT_N]
    baseline = rows[RECENT_N : RECENT_N + BASELINE_N]
    if len(recent) < MIN_N or len(baseline) < MIN_N:
        return _metric(
            "calibration_ece",
            value=None,
            state="insufficient_labels",
            labels_available=True,
            sample_n=len(recent),
            details={"reason": "not enough recent or baseline calibration rows", "baseline_n": len(baseline)},
            ts_ms=now_ms,
        )
    recent_ece = _ece(recent)
    baseline_ece = _ece(baseline)
    if recent_ece is None:
        severity, state = "UNKNOWN", "unavailable"
    else:
        severity, state = _severity_from_value(recent_ece, CALIBRATION_ECE_WARN, CALIBRATION_ECE_CRIT)
        if baseline_ece is not None and recent_ece <= baseline_ece and severity == "WARN":
            severity, state = "OK", "ok"
    return _metric(
        "calibration_ece",
        value=recent_ece,
        baseline_value=baseline_ece,
        threshold_value=CALIBRATION_ECE_WARN,
        severity=severity,
        state=state,
        action_signal=("retrain" if severity in {"WARN", "CRIT"} else ""),
        labels_available=True,
        sample_n=len(recent),
        details={
            "baseline_n": len(baseline),
            "method": "directional_expected_calibration_error",
            "target": "directional_accuracy",
        },
        ts_ms=now_ms,
    )


def _conformal_rows(con: Any, *, limit: int | None = None) -> tuple[list[dict[str, Any]], bool]:
    labels_available = bool(_label_values(con, limit=1))
    if not (_table_exists(con, "decision_log") and _table_exists(con, "labels")):
        return [], labels_available
    d_cols = _table_columns(con, "decision_log")
    l_cols = _table_columns(con, "labels")
    target_col = _first_existing(l_cols, "impact_z", "realized_z", "realized_ret", "label")
    if not {"event_id", "symbol", "horizon_s"}.issubset(d_cols) or not target_col:
        return [], labels_available
    explain_col = _first_existing(d_cols, "explain_json", "reason_json", "payload_json")
    if not explain_col:
        return [], labels_available
    label_ts = _first_existing(l_cols, "created_at_ms", "ts_ms") or "0"
    try:
        rows = con.execute(
            f"""
            SELECT d.ts_ms, d.event_id, d.symbol, d.horizon_s, d.{_safe_ident(explain_col)},
                   l.{_safe_ident(target_col)}, l.{_safe_ident(label_ts)}
            FROM decision_log d
            JOIN labels l
              ON l.event_id=d.event_id
             AND upper(l.symbol)=upper(d.symbol)
             AND l.horizon_s=d.horizon_s
            WHERE l.{_safe_ident(target_col)} IS NOT NULL
              AND d.{_safe_ident(explain_col)} IS NOT NULL
            ORDER BY l.{_safe_ident(label_ts)} DESC, d.ts_ms DESC
            LIMIT ?
            """,
            (int(limit or RECENT_N),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("PRODUCTION_MONITOR_CONFORMAL_ROWS_FAILED", exc, once_key="conformal_rows")
        return [], labels_available
    out: list[dict[str, Any]] = []
    for row in rows or []:
        target = _safe_float(row[5])
        payload = extract_conformal_payload(_json_obj(row[4]))
        lower = _safe_float(payload.get("interval_lower", payload.get("lower")))
        upper = _safe_float(payload.get("interval_upper", payload.get("upper")))
        if target is None or lower is None or upper is None:
            continue
        alpha = _safe_float(payload.get("alpha_target"), None)
        target_coverage = 1.0 - float(alpha) if alpha is not None else 1.0 - float(os.environ.get("CONFORMAL_ALPHA", "0.2"))
        out.append(
            {
                "ts_ms": _safe_int(row[0], 0),
                "event_id": _safe_int(row[1], 0),
                "symbol": str(row[2] or "").upper(),
                "horizon_s": _safe_int(row[3], 0),
                "target": float(target),
                "lower": float(lower),
                "upper": float(upper),
                "covered": bool(float(lower) <= float(target) <= float(upper)),
                "target_coverage": max(0.0, min(1.0, float(target_coverage))),
            }
        )
    return out, labels_available


def _conformal_metric(con: Any, now_ms: int) -> dict[str, Any]:
    rows, labels_available = _conformal_rows(con)
    if not labels_available:
        return _metric(
            "conformal_coverage",
            value=None,
            state="no_labels_yet",
            labels_available=False,
            details={"reason": "no matured labels available"},
            ts_ms=now_ms,
        )
    if len(rows) < MIN_N:
        return _metric(
            "conformal_coverage",
            value=None,
            state="insufficient_labels",
            labels_available=True,
            sample_n=len(rows),
            details={"reason": "not enough labeled conformal intervals"},
            ts_ms=now_ms,
        )
    coverage = float(np.mean([1.0 if row["covered"] else 0.0 for row in rows]))
    target = float(np.mean([float(row["target_coverage"]) for row in rows]))
    gap = float(target - coverage)
    if gap >= CONFORMAL_COVERAGE_CRIT_GAP:
        severity, state = "CRIT", "crit"
    elif gap >= CONFORMAL_COVERAGE_WARN_GAP:
        severity, state = "WARN", "warn"
    else:
        severity, state = "OK", "ok"
    return _metric(
        "conformal_coverage",
        value=coverage,
        baseline_value=target,
        threshold_value=max(0.0, target - CONFORMAL_COVERAGE_WARN_GAP),
        severity=severity,
        state=state,
        action_signal=("retrain" if severity in {"WARN", "CRIT"} else ""),
        labels_available=True,
        sample_n=len(rows),
        details={"target_coverage": target, "coverage_gap": gap},
        ts_ms=now_ms,
    )


def _shadow_live_metric(con: Any, now_ms: int) -> dict[str, Any]:
    if not (_table_exists(con, "shadow_predictions") and _table_exists(con, "predictions")):
        return _metric(
            "shadow_live_disagreement",
            value=None,
            state="unavailable",
            details={"reason": "shadow_predictions or predictions table unavailable"},
            ts_ms=now_ms,
        )
    p_cols = _table_columns(con, "predictions")
    s_cols = _table_columns(con, "shadow_predictions")
    p_value = _first_existing(p_cols, "predicted_z", "prediction")
    s_value = _first_existing(s_cols, "predicted_z", "prediction", "net_pred_z")
    if not p_value or not s_value:
        return _metric(
            "shadow_live_disagreement",
            value=None,
            state="unavailable",
            details={"reason": "prediction value columns unavailable"},
            ts_ms=now_ms,
        )
    try:
        rows = con.execute(
            f"""
            SELECT p.ts_ms, p.event_id, p.symbol, p.horizon_s,
                   p.{_safe_ident(p_value)}, s.{_safe_ident(s_value)}, COALESCE(s.model_name, '')
            FROM predictions p
            JOIN shadow_predictions s
              ON s.event_id=p.event_id
             AND upper(s.symbol)=upper(p.symbol)
             AND s.horizon_s=p.horizon_s
            WHERE p.{_safe_ident(p_value)} IS NOT NULL
              AND s.{_safe_ident(s_value)} IS NOT NULL
            ORDER BY p.ts_ms DESC
            LIMIT ?
            """,
            (int(RECENT_N),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("PRODUCTION_MONITOR_SHADOW_LIVE_FAILED", exc, once_key="shadow_live")
        rows = []
    if not rows:
        return _metric(
            "shadow_live_disagreement",
            value=None,
            state="insufficient_data",
            details={"reason": "no comparable shadow/live prediction rows"},
            ts_ms=now_ms,
        )
    deltas: list[float] = []
    sign_disagree = 0
    examples: list[dict[str, Any]] = []
    for row in rows:
        live = _safe_float(row[4])
        shadow = _safe_float(row[5])
        if live is None or shadow is None:
            continue
        delta = abs(float(live) - float(shadow))
        deltas.append(float(delta))
        if (float(live) > 0.0) != (float(shadow) > 0.0):
            sign_disagree += 1
            if len(examples) < 8:
                examples.append(
                    {
                        "event_id": _safe_int(row[1], 0),
                        "symbol": str(row[2] or ""),
                        "horizon_s": _safe_int(row[3], 0),
                        "live": float(live),
                        "shadow": float(shadow),
                        "shadow_model": str(row[6] or ""),
                    }
                )
    if not deltas:
        return _metric("shadow_live_disagreement", value=None, state="insufficient_data", ts_ms=now_ms)
    rate = float(sign_disagree / len(deltas))
    mean_abs_delta = float(np.mean(deltas))
    if rate >= SHADOW_DISAGREE_CRIT or mean_abs_delta >= SHADOW_ABS_DELTA_CRIT:
        severity, state = "CRIT", "crit"
    elif rate >= SHADOW_DISAGREE_WARN or mean_abs_delta >= SHADOW_ABS_DELTA_WARN:
        severity, state = "WARN", "warn"
    else:
        severity, state = "OK", "ok"
    return _metric(
        "shadow_live_disagreement",
        value=rate,
        baseline_value=0.0,
        threshold_value=SHADOW_DISAGREE_WARN,
        severity=severity,
        state=state,
        action_signal=("shadow_review" if severity in {"WARN", "CRIT"} else ""),
        sample_n=len(deltas),
        details={
            "sign_disagreement_rate": rate,
            "mean_abs_delta": mean_abs_delta,
            "mean_abs_delta_warn": SHADOW_ABS_DELTA_WARN,
            "examples": examples,
        },
        ts_ms=now_ms,
    )


def _net_pnl_values(con: Any) -> tuple[list[float], str]:
    values = _read_numeric_series(con, "labels_exec", ("net_ret", "net_z"), order_columns=("ts_ms",))
    if values:
        return values, "labels_exec"
    values = _read_numeric_series(con, "model_performance", ("pnl_impact", "realized_return"), order_columns=("time", "ts_ms"))
    if values:
        return values, "model_performance"
    values = _read_numeric_series(con, "pnl_attribution", ("pnl", "net_pnl"), order_columns=("ts_ms",))
    if values:
        return values, "pnl_attribution"
    return [], ""


def _net_pnl_metric(con: Any, now_ms: int) -> dict[str, Any]:
    values, source = _net_pnl_values(con)
    stats = _window_stats(values)
    if not stats.get("available"):
        return _metric(
            "net_pnl_degradation",
            value=None,
            state="insufficient_data",
            sample_n=_safe_int(stats.get("n"), 0),
            details={"reason": "not enough net PnL rows", "source": source, **stats},
            ts_ms=now_ms,
        )
    recent_mean = float(stats["recent_mean"])
    baseline_mean = float(stats["baseline_mean"])
    degradation = float(baseline_mean - recent_mean)
    if degradation >= NET_PNL_DEGRADATION_CRIT and recent_mean < baseline_mean:
        severity, state = "CRIT", "crit"
    elif degradation >= NET_PNL_DEGRADATION_WARN and recent_mean < baseline_mean:
        severity, state = "WARN", "warn"
    else:
        severity, state = "OK", "ok"
    return _metric(
        "net_pnl_degradation",
        value=degradation,
        baseline_value=baseline_mean,
        threshold_value=NET_PNL_DEGRADATION_WARN,
        severity=severity,
        state=state,
        action_signal=("retrain" if severity in {"WARN", "CRIT"} else ""),
        labels_available=True,
        sample_n=_safe_int(stats.get("n"), 0),
        details={"source": source, **stats, "recent_mean": recent_mean, "degradation": degradation},
        ts_ms=now_ms,
    )


def compute_production_monitoring_metrics(con: Any, *, now_ms: int | None = None) -> list[dict[str, Any]]:
    """Compute latest production monitoring metrics without writing signals."""

    ts_value = int(now_ms if now_ms is not None else _now_ms())
    return [
        _feature_drift_metric(con, ts_value),
        _missing_feature_metric(con, ts_value),
        _prediction_drift_metric(con, ts_value),
        _label_drift_metric(con, ts_value),
        _calibration_metric(con, ts_value),
        _conformal_metric(con, ts_value),
        _shadow_live_metric(con, ts_value),
        _net_pnl_metric(con, ts_value),
    ]


def _store_metrics(con: Any, metrics: Iterable[Mapping[str, Any]]) -> None:
    ensure_production_monitoring_schema(con)
    for metric in metrics or []:
        con.execute(
            """
            INSERT INTO production_monitoring_metrics(
              metric_name, scope, dimension, ts_ms, value, baseline_value,
              threshold_value, severity, state, action_signal, labels_available,
              sample_n, details_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(metric_name, scope, dimension) DO UPDATE SET
              ts_ms=excluded.ts_ms,
              value=excluded.value,
              baseline_value=excluded.baseline_value,
              threshold_value=excluded.threshold_value,
              severity=excluded.severity,
              state=excluded.state,
              action_signal=excluded.action_signal,
              labels_available=excluded.labels_available,
              sample_n=excluded.sample_n,
              details_json=excluded.details_json
            """,
            (
                str(metric.get("metric_name") or ""),
                str(metric.get("scope") or "global"),
                str(metric.get("dimension") or ""),
                int(metric.get("ts_ms") or _now_ms()),
                metric.get("value"),
                metric.get("baseline_value"),
                metric.get("threshold_value"),
                str(metric.get("severity") or "UNKNOWN"),
                str(metric.get("state") or "unavailable"),
                str(metric.get("action_signal") or ""),
                1 if bool(metric.get("labels_available")) else 0,
                int(metric.get("sample_n") or 0),
                _json_dumps(dict(metric.get("details") or {})),
            ),
        )


def _recent_signal_exists(con: Any, metric: Mapping[str, Any], *, now_ms: int, cooldown_s: int) -> bool:
    if int(cooldown_s or 0) <= 0:
        return False
    if not _table_exists(con, "drift_retrain_events"):
        return False
    row = con.execute(
        """
        SELECT id
        FROM drift_retrain_events
        WHERE family=?
          AND trigger_type=?
          AND action_taken=?
          AND created_ts>=?
        ORDER BY created_ts DESC, id DESC
        LIMIT 1
        """,
        (
            MONITOR_SOURCE,
            str(metric.get("metric_name") or ""),
            "shadow_review_signal" if str(metric.get("action_signal") or "") == "shadow_review" else "retrain_signal",
            int(now_ms) - int(cooldown_s) * 1000,
        ),
    ).fetchone()
    return bool(row)


def _emit_signals(con: Any, metrics: Iterable[Mapping[str, Any]], *, now_ms: int, cooldown_s: int = SIGNAL_COOLDOWN_S) -> list[dict[str, Any]]:
    _ensure_signal_schema(con)
    signals: list[dict[str, Any]] = []
    for metric in metrics or []:
        action_signal = str(metric.get("action_signal") or "").strip()
        severity = str(metric.get("severity") or "").upper()
        if action_signal not in {"retrain", "shadow_review"} or severity not in {"WARN", "CRIT"}:
            continue
        if _recent_signal_exists(con, metric, now_ms=now_ms, cooldown_s=int(cooldown_s)):
            signals.append(
                {
                    "metric_name": str(metric.get("metric_name") or ""),
                    "action_taken": "shadow_review_signal" if action_signal == "shadow_review" else "retrain_signal",
                    "outcome_status": "cooldown",
                    "cooldown_applied": True,
                }
            )
            continue
        action_taken = "shadow_review_signal" if action_signal == "shadow_review" else "retrain_signal"
        trigger_metrics = {
            "source": MONITOR_SOURCE,
            "metric_name": str(metric.get("metric_name") or ""),
            "scope": str(metric.get("scope") or "global"),
            "dimension": str(metric.get("dimension") or ""),
            "severity": severity,
            "state": str(metric.get("state") or ""),
            "value": metric.get("value"),
            "baseline_value": metric.get("baseline_value"),
            "threshold_value": metric.get("threshold_value"),
            "labels_available": bool(metric.get("labels_available")),
            "sample_n": int(metric.get("sample_n") or 0),
            "action_signal": action_signal,
            "details": dict(metric.get("details") or {}),
        }
        diagnostics = {
            "source": MONITOR_SOURCE,
            "direct_promotion": False,
            "promotion_allowed": False,
            "signal_only": True,
        }
        cur = con.execute(
            """
            INSERT INTO drift_retrain_events(
              created_ts, model_name, family, trigger_type, trigger_metrics,
              action_taken, cooldown_applied, candidate_version, outcome_status, diagnostics
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                MONITOR_SOURCE,
                MONITOR_SOURCE,
                str(metric.get("metric_name") or ""),
                _json_dumps(trigger_metrics),
                action_taken,
                0,
                None,
                "signal_created",
                _json_dumps(diagnostics),
            ),
        )
        signals.append(
            {
                "event_id": _safe_int(getattr(cur, "lastrowid", 0), 0),
                "metric_name": str(metric.get("metric_name") or ""),
                "action_taken": action_taken,
                "outcome_status": "signal_created",
                "cooldown_applied": False,
            }
        )
    return signals


def _status(metrics: Iterable[Mapping[str, Any]], *, ts_ms: int) -> dict[str, Any]:
    rows = list(metrics or [])
    severities = [str(row.get("severity") or "UNKNOWN").upper() for row in rows]
    if "CRIT" in severities:
        state = "crit"
        severity = "CRIT"
    elif "WARN" in severities:
        state = "warn"
        severity = "WARN"
    elif rows:
        state = "normal"
        severity = "OK"
    else:
        state = "unavailable"
        severity = "UNKNOWN"
    active = severity in {"WARN", "CRIT"}
    if active:
        reason = "Production monitoring thresholds are breached."
    elif rows:
        reason = "Production monitoring metrics are within configured thresholds or waiting for mature labels."
    else:
        reason = "No production monitoring metrics are available."
    return {
        "state": state,
        "severity": severity,
        "active": bool(active),
        "reason": reason,
        "latest_ts_ms": int(ts_ms or 0),
        "metric_count": int(len(rows)),
        "alert_count": int(sum(1 for row in rows if str(row.get("severity") or "").upper() in {"WARN", "CRIT"})),
    }


def compute_and_store_production_monitoring(
    *,
    con: Any = None,
    now_ms: int | None = None,
    emit_signals: bool = True,
    signal_cooldown_s: int = SIGNAL_COOLDOWN_S,
) -> dict[str, Any]:
    """Compute, persist, and optionally signal production monitoring breaches."""

    owns = con is None
    if owns:
        init_db()
        con = connect()
    ts_value = int(now_ms if now_ms is not None else _now_ms())
    try:
        metrics = compute_production_monitoring_metrics(con, now_ms=ts_value)
        _store_metrics(con, metrics)
        signals = _emit_signals(con, metrics, now_ms=ts_value, cooldown_s=int(signal_cooldown_s)) if emit_signals else []
        status = _status(metrics, ts_ms=ts_value)
        if owns:
            con.commit()
        return {
            "ok": True,
            "ts_ms": int(ts_value),
            "status": status,
            "metrics": metrics,
            "signals": signals,
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("PRODUCTION_MONITOR_CLOSE_FAILED", exc, once_key="close")


def get_latest_production_monitoring_snapshot(*, con: Any = None, limit: int = 50) -> dict[str, Any]:
    """Read latest production monitoring metrics for API/dashboard composition."""

    owns = con is None
    if owns:
        con = connect(readonly=True)
    try:
        if not _table_exists(con, "production_monitoring_metrics"):
            return {
                "ok": True,
                "status": _status([], ts_ms=0),
                "metrics": [],
                "signals": [],
                "updated_ts_ms": 0,
            }
        rows = con.execute(
            """
            SELECT metric_name, scope, dimension, ts_ms, value, baseline_value,
                   threshold_value, severity, state, action_signal,
                   labels_available, sample_n, details_json
            FROM production_monitoring_metrics
            ORDER BY ts_ms DESC, metric_name ASC
            LIMIT ?
            """,
            (max(1, min(500, int(limit or 50))),),
        ).fetchall()
        metrics = [
            {
                "metric_name": str(row[0] or ""),
                "scope": str(row[1] or "global"),
                "dimension": str(row[2] or ""),
                "ts_ms": _safe_int(row[3], 0),
                "value": _safe_float(row[4]),
                "baseline_value": _safe_float(row[5]),
                "threshold_value": _safe_float(row[6]),
                "severity": str(row[7] or "UNKNOWN"),
                "state": str(row[8] or "unavailable"),
                "action_signal": str(row[9] or ""),
                "labels_available": bool(_safe_int(row[10], 0)),
                "sample_n": _safe_int(row[11], 0),
                "details": _json_obj(row[12]),
            }
            for row in rows or []
        ]
        latest_ts = max((_safe_int(row.get("ts_ms"), 0) for row in metrics), default=0)
        signals: list[dict[str, Any]] = []
        if _table_exists(con, "drift_retrain_events"):
            signal_rows = con.execute(
                """
                SELECT created_ts, trigger_type, action_taken, outcome_status, trigger_metrics, diagnostics
                FROM drift_retrain_events
                WHERE family=?
                ORDER BY created_ts DESC, id DESC
                LIMIT 20
                """,
                (MONITOR_SOURCE,),
            ).fetchall()
            signals = [
                {
                    "created_ts": _safe_int(row[0], 0),
                    "metric_name": str(row[1] or ""),
                    "action_taken": str(row[2] or ""),
                    "outcome_status": str(row[3] or ""),
                    "trigger_metrics": _json_obj(row[4]),
                    "diagnostics": _json_obj(row[5]),
                }
                for row in signal_rows or []
            ]
        return {
            "ok": True,
            "status": _status(metrics, ts_ms=latest_ts),
            "metrics": metrics,
            "signals": signals,
            "updated_ts_ms": int(latest_ts),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("PRODUCTION_MONITOR_READ_CLOSE_FAILED", exc, once_key="read_close")


__all__ = [
    "MONITOR_SOURCE",
    "STATUS_META_KEY",
    "compute_and_store_production_monitoring",
    "compute_production_monitoring_metrics",
    "ensure_production_monitoring_schema",
    "get_latest_production_monitoring_snapshot",
]
