"""Read-side lifecycle helpers for retraining, drift, and dataset provenance.

This module does not train models itself. It summarizes runtime evidence so the
lifecycle and governance layers can decide when a model family should retrain,
which dataset snapshot was used, and whether recent behavior suggests decay.
"""

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, Mapping, Optional

from engine.model_registry import get_stage_latest
from engine.runtime.dataset_store import materialize_dataset_snapshot, normalize_feature_schema, normalize_training_window
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.strategy.distribution_drift import get_latest_distribution_drift_snapshot


DRIFT_RATIO_TRIGGER = float(os.environ.get("MODEL_LIFECYCLE_DRIFT_RATIO_TRIGGER", "1.25"))
RUNTIME_MIN_TRADES = int(os.environ.get("MODEL_LIFECYCLE_RUNTIME_MIN_TRADES", "10"))
RUNTIME_MIN_WIN_RATE = float(os.environ.get("MODEL_LIFECYCLE_RUNTIME_MIN_WIN_RATE", "0.48"))
RUNTIME_NEG_PNL_TRIGGER = float(os.environ.get("MODEL_LIFECYCLE_RUNTIME_NEG_PNL_TRIGGER", "-0.01"))
SHADOW_MIN_POINTS = int(os.environ.get("MODEL_LIFECYCLE_SHADOW_MIN_POINTS", "1"))
SHADOW_MIN_DIR_ACC = float(os.environ.get("MODEL_LIFECYCLE_SHADOW_MIN_DIR_ACC", "0.48"))
SHADOW_NET_RMSE_MULT = float(os.environ.get("MODEL_LIFECYCLE_SHADOW_NET_RMSE_MULT", "1.10"))
TEMPORAL_FAIL_LOOKBACK_MS = int(
    os.environ.get("MODEL_LIFECYCLE_TEMPORAL_FAIL_LOOKBACK_MS", str(24 * 60 * 60 * 1000))
)
REGIME_SHIFT_STATES = {
    str(x).strip().upper()
    for x in str(os.environ.get("MODEL_LIFECYCLE_REGIME_SHIFT_STATES", "WARN,CRITICAL") or "WARN,CRITICAL").split(",")
    if str(x).strip()
}
LOG = get_logger("engine.strategy.learning_loop")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="learning_loop_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.learning_loop",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal("LEARNING_LOOP_SAFE_INT_FAILED", e, once_key="safe_int", value=repr(value)[:120])
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal("LEARNING_LOOP_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(value)[:120])
        return float(default)


def _table_exists(con, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("LEARNING_LOOP_TABLE_EXISTS_FAILED", e, once_key=f"table_exists:{table}", table_name=str(table))
        return False


def _fingerprint_payload(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload or {}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _labels_dataset_summary(
    con,
    *,
    lookback_days: int,
    symbols: Iterable[str],
    horizons: Iterable[int],
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "table": "labels",
        "row_count": 0,
        "latest_created_at_ms": 0,
        "latest_event_ts_ms": 0,
        "distinct_symbols": 0,
        "distinct_horizons": 0,
    }
    if not _table_exists(con, "labels"):
        return summary

    params = []
    has_events = _table_exists(con, "events")
    where = ["l.impact_z IS NOT NULL"]
    if int(lookback_days or 0) > 0 and has_events:
        cutoff_ms = _now_ms() - int(lookback_days) * 24 * 60 * 60 * 1000
        where.append("COALESCE(e.ts_ms, l.created_at_ms, 0) >= ?")
        params.append(int(cutoff_ms))

    symbol_list = [str(sym).upper().strip() for sym in (symbols or []) if str(sym).strip()]
    if symbol_list:
        where.append("l.symbol IN (" + ",".join("?" for _ in symbol_list) + ")")
        params.extend(symbol_list)

    horizon_list = [int(h) for h in (horizons or []) if _safe_int(h, 0) > 0]
    if horizon_list:
        where.append("l.horizon_s IN (" + ",".join("?" for _ in horizon_list) + ")")
        params.extend(horizon_list)

    join_events = "LEFT JOIN events e ON e.id = l.event_id" if has_events else ""
    event_expr = "COALESCE(MAX(e.ts_ms), 0)" if has_events else "0"
    row = con.execute(
        f"""
        SELECT
          COUNT(*),
          MAX(COALESCE(l.created_at_ms, 0)),
          {event_expr},
          COUNT(DISTINCT l.symbol),
          COUNT(DISTINCT l.horizon_s)
        FROM labels l
        {join_events}
        WHERE {" AND ".join(where)}
        """,
        tuple(params),
    ).fetchone()
    if not row:
        return summary

    summary.update(
        {
            "row_count": _safe_int(row[0], 0),
            "latest_created_at_ms": _safe_int(row[1], 0),
            "latest_event_ts_ms": _safe_int(row[2], 0),
            "distinct_symbols": _safe_int(row[3], 0),
            "distinct_horizons": _safe_int(row[4], 0),
        }
    )
    return summary


def _table_count_summary(con, table: str, *, ts_column: str = "ts_ms") -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "table": str(table),
        "row_count": 0,
        "latest_ts_ms": 0,
    }
    if not _table_exists(con, table):
        return summary

    cols = {
        str(row[1] or "").strip().lower()
        for row in (con.execute(f"PRAGMA table_info({table})").fetchall() or [])
    }
    if str(ts_column).strip().lower() not in cols:
        ts_expr = "0"
    else:
        ts_expr = f"MAX({ts_column})"

    row = con.execute(f"SELECT COUNT(*), {ts_expr} FROM {table}").fetchone()
    if not row:
        return summary

    summary.update(
        {
            "row_count": _safe_int(row[0], 0),
            "latest_ts_ms": _safe_int(row[1], 0),
        }
    )
    return summary


def build_dataset_snapshot(
    *,
    model_name: str,
    lookback_days: int = 0,
    symbols: Optional[Iterable[str]] = None,
    horizons: Optional[Iterable[int]] = None,
    feature_ids: Optional[Iterable[str]] = None,
    extra: Optional[Dict[str, Any]] = None,
    feature_schema: Optional[Mapping[str, Any]] = None,
    training_window: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    # The dataset snapshot is the lightweight provenance record shared across
    # lifecycle, training, and governance surfaces. It is how the repo answers
    # "which data and feature contract produced this model version?"
    init_db()
    model_key = str(model_name or "").strip()
    symbol_list = [str(sym).upper().strip() for sym in (symbols or []) if str(sym).strip()]
    horizon_list = [int(h) for h in (horizons or []) if _safe_int(h, 0) > 0]
    feature_list = [str(fid).strip() for fid in (feature_ids or []) if str(fid).strip()]

    con = connect(readonly=True)
    try:
        dataset_used: Dict[str, Any] = {
            "model_name": str(model_key),
            "lookback_days": int(lookback_days or 0),
            "symbols": list(symbol_list),
            "horizons": list(horizon_list),
            "feature_ids": list(feature_list),
            "captured_ts_ms": _now_ms(),
            "sources": {
                "labels": _labels_dataset_summary(
                    con,
                    lookback_days=int(lookback_days or 0),
                    symbols=list(symbol_list),
                    horizons=list(horizon_list),
                ),
            },
        }

        if model_key == "temporal_predictor":
            emb_table = "event_embeddings_seq" if _table_exists(con, "event_embeddings_seq") else "event_embeddings"
            dataset_used["sources"]["embeddings"] = _table_count_summary(con, emb_table, ts_column="event_id")

        if model_key == "embed_regressor":
            dataset_used["sources"]["embed_eval"] = _table_count_summary(con, "embed_model_eval")

        if model_key == "gbm_regressor" or str(model_key).startswith("gbm_regressor"):
            dataset_used["sources"]["gbm_models"] = _table_count_summary(con, "gbm_models")

        if model_key == "regime_stats_v2":
            dataset_used["sources"]["model_stats_versions"] = _table_count_summary(
                con,
                "model_stats_versions",
                ts_column="ts_ms",
            )

        if isinstance(extra, dict) and extra:
            dataset_used["extra"] = dict(extra)

        schema_dict = normalize_feature_schema(
            feature_ids=list(feature_list),
            feature_schema=feature_schema,
            feature_set_tag=(dict(extra or {}).get("feature_set_tag") if isinstance(extra, dict) else None),
        )
        if schema_dict:
            dataset_used["feature_schema"] = dict(schema_dict)

        training_window_dict = normalize_training_window(
            captured_ts_ms=int(dataset_used.get("captured_ts_ms") or 0),
            lookback_days=int(lookback_days or 0),
            training_window=training_window,
            symbols=list(symbol_list),
            horizons=list(horizon_list),
        )
        if training_window_dict:
            dataset_used["training_window"] = dict(training_window_dict)

        dataset_used["fingerprint"] = _fingerprint_payload(dataset_used)
        return materialize_dataset_snapshot(
            dataset_used,
            feature_schema=schema_dict,
            training_window=training_window_dict,
            extra_manifest={
                "job_name": str((dict(extra or {}).get("job_name") or "").strip()),
                "dataset_contract": "training_provenance_v1",
            },
        )
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("LEARNING_LOOP_CLOSE_FAILED", e, operation="build_dataset_snapshot", model_name=str(model_name))


def _runtime_performance_signal(model_name: str) -> Dict[str, Any]:
    champion = None
    try:
        champion = get_stage_latest(str(model_name), "champion", regime="global")
    except Exception:
        champion = None

    perf = dict((champion or {}).get("performance_metrics") or {})
    trade_count = _safe_int(perf.get("trade_count"), 0)
    rolling_total_pnl = _safe_float(perf.get("rolling_total_pnl"), 0.0)
    recent_total_pnl = _safe_float(perf.get("recent_total_pnl"), 0.0)
    win_rate = perf.get("win_rate")
    if win_rate is not None:
        win_rate = _safe_float(win_rate, 0.0)

    reasons = []
    if trade_count >= int(RUNTIME_MIN_TRADES):
        if rolling_total_pnl <= float(RUNTIME_NEG_PNL_TRIGGER):
            reasons.append("runtime_negative_pnl")
        if recent_total_pnl <= float(RUNTIME_NEG_PNL_TRIGGER):
            reasons.append("runtime_recent_negative_pnl")
        if win_rate is not None and float(win_rate) < float(RUNTIME_MIN_WIN_RATE):
            reasons.append("runtime_low_win_rate")

    return {
        "detected": bool(reasons),
        "reasons": reasons,
        "trade_count": int(trade_count),
        "rolling_total_pnl": float(rolling_total_pnl),
        "recent_total_pnl": float(recent_total_pnl),
        "win_rate": (float(win_rate) if win_rate is not None else None),
        "champion_model_version": str(((champion or {}).get("metrics") or {}).get("model_version") or ""),
    }


def _shadow_performance_signal(con, model_name: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "detected": False,
        "reasons": [],
        "points": 0,
        "avg_dir_acc": None,
        "avg_net_rmse": None,
        "avg_rmse": None,
        "window_end_ms": 0,
    }
    if not _table_exists(con, "shadow_metrics"):
        return out

    rows = con.execute(
        """
        SELECT window_end_ms, dir_acc, net_rmse, rmse, n
        FROM shadow_metrics
        WHERE model_name=?
        ORDER BY window_end_ms DESC
        LIMIT 8
        """,
        (str(model_name),),
    ).fetchall()
    if not rows:
        return out

    dir_vals = []
    net_rmse_vals = []
    rmse_vals = []
    sample_points = 0
    latest_window_end = 0
    for window_end_ms, dir_acc, net_rmse, rmse, n in rows:
        latest_window_end = max(int(window_end_ms or 0), latest_window_end)
        sample_points += int(n or 0)
        if dir_acc is not None:
            dir_vals.append(float(dir_acc))
        if net_rmse is not None:
            net_rmse_vals.append(float(net_rmse))
        if rmse is not None:
            rmse_vals.append(float(rmse))

    out["points"] = int(len(rows))
    out["window_end_ms"] = int(latest_window_end)
    out["avg_dir_acc"] = (sum(dir_vals) / len(dir_vals)) if dir_vals else None
    out["avg_net_rmse"] = (sum(net_rmse_vals) / len(net_rmse_vals)) if net_rmse_vals else None
    out["avg_rmse"] = (sum(rmse_vals) / len(rmse_vals)) if rmse_vals else None

    reasons = []
    if int(len(rows)) >= int(SHADOW_MIN_POINTS):
        if out["avg_dir_acc"] is not None and float(out["avg_dir_acc"]) < float(SHADOW_MIN_DIR_ACC):
            reasons.append("shadow_low_directional_accuracy")
        if (
            out["avg_net_rmse"] is not None
            and out["avg_rmse"] is not None
            and float(out["avg_net_rmse"]) > float(out["avg_rmse"]) * float(SHADOW_NET_RMSE_MULT)
        ):
            reasons.append("shadow_net_rmse_regression")

    out["detected"] = bool(reasons)
    out["reasons"] = reasons
    out["sample_points"] = int(sample_points)
    return out


def _temporal_shadow_signal(con) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "detected": False,
        "reasons": [],
        "failed_rows": 0,
        "latest_ts_ms": 0,
    }
    if not _table_exists(con, "temporal_shadow_eval"):
        return out

    min_ts = _now_ms() - int(TEMPORAL_FAIL_LOOKBACK_MS)
    rows = con.execute(
        """
        SELECT ts_ms, pass_all, rmse_improvement, diracc_delta, detail_json
        FROM temporal_shadow_eval
        WHERE ts_ms >= ?
        ORDER BY ts_ms DESC
        LIMIT 50
        """,
        (int(min_ts),),
    ).fetchall()
    if not rows:
        return out

    reasons = []
    failed_rows = 0
    latest_ts_ms = 0
    for ts_ms, pass_all, rmse_improvement, diracc_delta, detail_json in rows:
        latest_ts_ms = max(latest_ts_ms, _safe_int(ts_ms, 0))
        if int(pass_all or 0) == 1:
            continue
        failed_rows += 1
        reasons.append("temporal_shadow_failed")
        try:
            detail = json.loads(detail_json or "{}")
        except Exception:
            detail = {}
        for reason in list(detail.get("reasons") or []):
            reason_text = str(reason).strip()
            if reason_text:
                reasons.append(f"temporal_shadow:{reason_text}")
        if _safe_float(rmse_improvement, 0.0) <= 0.0:
            reasons.append("temporal_shadow_rmse_regression")
        if _safe_float(diracc_delta, 0.0) < 0.0:
            reasons.append("temporal_shadow_diracc_regression")

    out["detected"] = bool(failed_rows > 0)
    out["reasons"] = sorted(set(reasons))
    out["failed_rows"] = int(failed_rows)
    out["latest_ts_ms"] = int(latest_ts_ms)
    return out


def detect_learning_signals(model_name: str) -> Dict[str, Any]:
    init_db()
    con = connect(readonly=True)
    try:
        drift_ratio = 0.0
        drift_rows = 0
        if _table_exists(con, "model_drift"):
            row = con.execute(
                """
                SELECT MAX(drift_ratio), COUNT(*)
                FROM model_drift
                """
            ).fetchone()
            drift_ratio = _safe_float((row or [0.0, 0])[0], 0.0)
            drift_rows = _safe_int((row or [0.0, 0])[1], 0)

        distribution_snapshot = get_latest_distribution_drift_snapshot(con=con) or {}
        distribution_state = str(distribution_snapshot.get("state", "UNKNOWN") or "UNKNOWN").upper()

        runtime_signal = _runtime_performance_signal(str(model_name))
        shadow_signal = _shadow_performance_signal(con, str(model_name))
        temporal_signal = _temporal_shadow_signal(con) if str(model_name) == "temporal_predictor" else {
            "detected": False,
            "reasons": [],
            "failed_rows": 0,
            "latest_ts_ms": 0,
        }

        performance_reasons = []
        for signal in (runtime_signal, shadow_signal, temporal_signal):
            performance_reasons.extend(list(signal.get("reasons") or []))

        drift_detected = bool(drift_rows > 0 and float(drift_ratio) >= float(DRIFT_RATIO_TRIGGER))
        regime_shift = bool(distribution_state in REGIME_SHIFT_STATES)

        return {
            "model_name": str(model_name),
            "ts_ms": _now_ms(),
            "performance_drop": bool(performance_reasons),
            "performance_reasons": sorted(set(str(x) for x in performance_reasons if str(x).strip())),
            "drift_detected": bool(drift_detected),
            "regime_shift": bool(regime_shift),
            "drift_ratio": float(drift_ratio),
            "drift_ratio_trigger": float(DRIFT_RATIO_TRIGGER),
            "distribution_state": str(distribution_state),
            "distribution_snapshot": dict(distribution_snapshot),
            "runtime_signal": dict(runtime_signal),
            "shadow_signal": dict(shadow_signal),
            "temporal_shadow_signal": dict(temporal_signal),
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("LEARNING_LOOP_CLOSE_FAILED", e, operation="detect_learning_signals", model_name=str(model_name))
