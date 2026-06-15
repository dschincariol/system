"""Model lifecycle orchestration for versioning, retraining cadence, and retirement.

The lifecycle flow inspects learning-loop signals, advances model version
metadata, records active-version state, and decides when stale or weak models
should be retrained or retired.
"""

import hashlib
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.runtime.storage import connect, init_db, run_write_txn
from engine.runtime.dataset_store import materialize_dataset_snapshot, normalize_feature_schema, normalize_training_window
from engine.strategy.learning_loop import build_dataset_snapshot, detect_learning_signals

DEFAULT_MODEL_NAME = str(os.environ.get("MODEL_V2_NAME", "regime_stats_v2") or "regime_stats_v2").strip()
RETIRE_SCORE_THRESHOLD = float(os.environ.get("MODEL_LIFECYCLE_RETIRE_SCORE_THRESHOLD", "0.25"))
RETIRE_MIN_POINTS = int(os.environ.get("MODEL_LIFECYCLE_RETIRE_MIN_POINTS", "3"))
RETRAIN_INTERVAL_MS = int(
    os.environ.get("MODEL_LIFECYCLE_RETRAIN_INTERVAL_MS", str(6 * 60 * 60 * 1000))
)
VARIATION_FAST_FACTOR = float(os.environ.get("MODEL_LIFECYCLE_FAST_FACTOR", "0.75"))
VARIATION_SLOW_FACTOR = float(os.environ.get("MODEL_LIFECYCLE_SLOW_FACTOR", "1.25"))
LIFECYCLE_PLAN_ENV = "MODEL_LIFECYCLE_PLAN_JSON"
LOG = get_logger("engine.strategy.model_lifecycle")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="model_lifecycle_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.model_lifecycle",
        extra=extra or None,
        persist=False,
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, str) and not value.strip():
        return float(default)
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_SAFE_FLOAT_FAILED", e, value=repr(value), default=float(default))
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    if isinstance(value, str) and not value.strip():
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_SAFE_INT_FAILED", e, value=repr(value), default=int(default))
        return int(default)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
        except Exception as e:
            _warn_nonfatal("MODEL_LIFECYCLE_JSON_PARSE_FAILED", e, value=repr(value)[:512])
            return {}
        return dict(obj) if isinstance(obj, dict) else {}
    return {}


def load_lifecycle_plan(expected_model_name: Optional[str] = None) -> Dict[str, Any]:
    raw = str(os.environ.get(LIFECYCLE_PLAN_ENV, "") or "").strip()
    plan = _safe_json_dict(raw)
    if not plan:
        return {}
    if expected_model_name:
        expected = str(expected_model_name or "").strip()
        actual = str(plan.get("model_name") or "").strip()
        if expected and actual and actual != expected:
            return {}
    return plan


def _hmm_lifecycle_enabled() -> bool:
    return str(os.environ.get("HMM_REGIME_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}


def _default_lifecycle_model_names() -> List[str]:
    names = ["regime_stats_v2", "embed_regressor", "temporal_predictor"]
    if _hmm_lifecycle_enabled():
        names.append("hmm_regime")
    try:
        from engine.strategy.model_config import load_model_configs
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_MODEL_CONFIG_IMPORT_FAILED", e)
        return list(dict.fromkeys(names))

    try:
        configs = load_model_configs(include_disabled=False)
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_MODEL_CONFIG_LOAD_FAILED", e)
        return list(dict.fromkeys(names))

    for cfg in configs or []:
        candidate_name = str(cfg.get("model_name") or cfg.get("family") or "").strip()
        if not candidate_name:
            continue
        if not _family_training_job(candidate_name):
            continue
        if candidate_name not in names:
            names.append(candidate_name)
    return list(dict.fromkeys(names))


def _active_version_meta_key(model_name: str) -> str:
    return f"active_model_version:{str(model_name or DEFAULT_MODEL_NAME).strip()}"


def next_model_version(model_name: str = DEFAULT_MODEL_NAME) -> str:
    init_db()
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT model_version
            FROM model_versions
            WHERE model_name=?
            ORDER BY created_ts_ms DESC
            LIMIT 200
            """,
            (str(model_name),),
        ).fetchall()
    finally:
        con.close()

    max_seq = 0
    for row in rows or []:
        version = str((row or [None])[0] or "")
        if not version.startswith("v"):
            continue
        head = version.split("-", 1)[0][1:]
        try:
            max_seq = max(max_seq, int(head))
        except Exception as e:
            _warn_nonfatal("MODEL_LIFECYCLE_VERSION_PARSE_FAILED", e, version=str(version))
            continue
    return f"v{int(max_seq) + 1:06d}-{_now_ms()}"


def version_from_ts(model_name: str, ts_ms: int, *, prefix: Optional[str] = None) -> str:
    family = str(prefix or model_name or "model").strip().lower().replace(" ", "_")
    return f"{family}-{int(ts_ms)}"


def get_latest_version(model_name: str = DEFAULT_MODEL_NAME) -> Optional[Dict[str, Any]]:
    init_db()
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT model_version, model_kind, parent_version, mutation_kind, stage, status,
                   live_ready, training_job_name, train_scope_json, meta_json,
                   created_ts_ms, updated_ts_ms
            FROM model_versions
            WHERE model_name=?
            ORDER BY updated_ts_ms DESC, created_ts_ms DESC
            LIMIT 1
            """,
            (str(model_name),),
        ).fetchone()
        if not row:
            return None
        return {
            "model_name": str(model_name),
            "model_version": str(row[0] or ""),
            "model_kind": str(row[1] or ""),
            "parent_version": row[2],
            "mutation_kind": str(row[3] or "baseline_retrain"),
            "stage": str(row[4] or "shadow"),
            "status": str(row[5] or "candidate"),
            "live_ready": bool(int(row[6] or 0)),
            "training_job_name": str(row[7] or ""),
            "train_scope": _safe_json_dict(row[8]),
            "meta": _safe_json_dict(row[9]),
            "created_ts_ms": _safe_int(row[10], 0),
            "updated_ts_ms": _safe_int(row[11], 0),
        }
    finally:
        con.close()


def register_model_version(
    *,
    model_name: str,
    model_version: str,
    model_kind: str,
    parent_version: Optional[str] = None,
    mutation_kind: str = "baseline_retrain",
    stage: str = "shadow",
    status: str = "candidate",
    live_ready: bool = False,
    training_job_name: Optional[str] = None,
    train_scope: Optional[Dict[str, Any]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    init_db()
    now_ms = _now_ms()

    def _write(con) -> None:
        existing = con.execute(
            """
            SELECT train_scope_json, meta_json, created_ts_ms
            FROM model_versions
            WHERE model_name=? AND model_version=?
            LIMIT 1
            """,
            (str(model_name), str(model_version)),
        ).fetchone()
        existing_train_scope = _safe_json_dict(existing[0] if existing else None)
        existing_meta = _safe_json_dict(existing[1] if existing else None)
        created_ts_ms = _safe_int(existing[2] if existing else None, now_ms)
        merged_train_scope = dict(existing_train_scope)
        merged_train_scope.update(dict(train_scope or {}))
        merged_meta = dict(existing_meta)
        merged_meta.update(dict(meta or {}))
        con.execute(
            """
            INSERT INTO model_versions(
              model_name, model_version, model_kind, parent_version, mutation_kind,
              stage, status, live_ready, training_job_name, train_scope_json,
              meta_json, created_ts_ms, updated_ts_ms
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(model_name, model_version) DO UPDATE SET
              model_kind=excluded.model_kind,
              parent_version=excluded.parent_version,
              mutation_kind=excluded.mutation_kind,
              stage=excluded.stage,
              status=excluded.status,
              live_ready=excluded.live_ready,
              training_job_name=excluded.training_job_name,
              train_scope_json=excluded.train_scope_json,
              meta_json=excluded.meta_json,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (
                str(model_name),
                str(model_version),
                str(model_kind),
                (str(parent_version) if parent_version else None),
                str(mutation_kind or "baseline_retrain"),
                str(stage or "shadow"),
                str(status or "candidate"),
                1 if live_ready else 0,
                (str(training_job_name) if training_job_name else None),
                _json_dumps(merged_train_scope),
                _json_dumps(merged_meta),
                int(created_ts_ms),
                int(now_ms),
            ),
        )

    run_write_txn(_write)


def update_model_version_status(
    model_name: str,
    model_version: str,
    *,
    stage: Optional[str] = None,
    status: Optional[str] = None,
    live_ready: Optional[bool] = None,
    meta_patch: Optional[Dict[str, Any]] = None,
) -> None:
    init_db()
    now_ms = _now_ms()
    patch = dict(meta_patch or {})

    def _write(con) -> None:
        row = con.execute(
            """
            SELECT meta_json
            FROM model_versions
            WHERE model_name=? AND model_version=?
            """,
            (str(model_name), str(model_version)),
        ).fetchone()
        meta = _safe_json_dict(row[0] if row else None)
        meta.update(patch)

        sets = ["updated_ts_ms=?", "meta_json=?"]
        params: List[Any] = [int(now_ms), _json_dumps(meta)]
        if stage is not None:
            sets.append("stage=?")
            params.append(str(stage))
        if status is not None:
            sets.append("status=?")
            params.append(str(status))
        if live_ready is not None:
            sets.append("live_ready=?")
            params.append(1 if bool(live_ready) else 0)
        params.extend([str(model_name), str(model_version)])
        con.execute(
            f"""
            UPDATE model_versions
            SET {", ".join(sets)}
            WHERE model_name=? AND model_version=?
            """,
            tuple(params),
        )

    run_write_txn(_write)


def get_model_version(model_name: str, model_version: str) -> Optional[Dict[str, Any]]:
    init_db()
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT model_kind, parent_version, mutation_kind, stage, status,
                   live_ready, training_job_name, train_scope_json, meta_json,
                   created_ts_ms, updated_ts_ms
            FROM model_versions
            WHERE model_name=? AND model_version=?
            LIMIT 1
            """,
            (str(model_name), str(model_version)),
        ).fetchone()
        if not row:
            return None
        train_scope = _safe_json_dict(row[7])
        meta = _safe_json_dict(row[8])
        dataset_used = train_scope.get("dataset_used") or meta.get("dataset_used") or {}
        training_timestamp_ms = _safe_int(
            meta.get("training_completed_ts_ms") or meta.get("training_started_ts_ms"),
            _safe_int(row[9], 0),
        )
        return {
            "model_name": str(model_name),
            "model_version": str(model_version),
            "model_kind": str(row[0] or ""),
            "parent_version": row[1],
            "mutation_kind": str(row[2] or ""),
            "stage": str(row[3] or ""),
            "status": str(row[4] or ""),
            "live_ready": bool(int(row[5] or 0)),
            "training_job_name": str(row[6] or ""),
            "train_scope": train_scope,
            "meta": meta,
            "dataset_used": dataset_used,
            "training_timestamp_ms": int(training_timestamp_ms),
            "created_ts_ms": _safe_int(row[9], 0),
            "updated_ts_ms": _safe_int(row[10], 0),
        }
    finally:
        con.close()


def record_version_performance(
    *,
    model_name: str,
    model_version: str,
    metric_scope: str,
    metrics: Dict[str, Any],
    sample_n: int = 0,
    meta: Optional[Dict[str, Any]] = None,
) -> int:
    init_db()
    now_ms = _now_ms()
    rows: List[tuple[str, float]] = []
    for key, value in dict(metrics or {}).items():
        try:
            rows.append((str(key), float(value)))
        except Exception as e:
            _warn_nonfatal(
                "MODEL_LIFECYCLE_PERFORMANCE_METRIC_PARSE_FAILED",
                e,
                metric_name=str(key),
                metric_value=repr(value),
            )
            continue
    if not rows:
        return 0

    def _write(con) -> int:
        for metric_name, metric_value in rows:
            con.execute(
                """
                INSERT INTO model_version_performance(
                  model_name, model_version, metric_scope, metric_name,
                  metric_value, sample_n, recorded_ts_ms, meta_json
                )
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    str(model_name),
                    str(model_version),
                    str(metric_scope),
                    str(metric_name),
                    float(metric_value),
                    int(sample_n),
                    int(now_ms),
                    _json_dumps(meta or {}),
                ),
            )
        return len(rows)

    return int(run_write_txn(_write) or 0)


def summarize_version_performance(
    model_name: str,
    model_version: str,
    *,
    metric_names: Optional[Sequence[str]] = None,
    limit: int = 12,
) -> Dict[str, Any]:
    init_db()
    metric_filter = [str(name) for name in (metric_names or []) if str(name).strip()]
    con = connect(readonly=True)
    try:
        if metric_filter:
            placeholders = ",".join("?" for _ in metric_filter)
            rows = con.execute(
                f"""
                SELECT metric_name, metric_value, recorded_ts_ms
                FROM model_version_performance
                WHERE model_name=? AND model_version=? AND metric_name IN ({placeholders})
                ORDER BY recorded_ts_ms DESC
                LIMIT ?
                """,
                (str(model_name), str(model_version), *metric_filter, int(limit)),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT metric_name, metric_value, recorded_ts_ms
                FROM model_version_performance
                WHERE model_name=? AND model_version=?
                ORDER BY recorded_ts_ms DESC
                LIMIT ?
                """,
                (str(model_name), str(model_version), int(limit)),
            ).fetchall()
    finally:
        con.close()

    by_metric: Dict[str, List[float]] = {}
    for metric_name, metric_value, _ in rows or []:
        by_metric.setdefault(str(metric_name or ""), []).append(_safe_float(metric_value))

    summary: Dict[str, Any] = {"points": len(rows or []), "metrics": {}}
    for metric_name, values in by_metric.items():
        if not values:
            continue
        summary["metrics"][metric_name] = {
            "latest": float(values[0]),
            "avg": float(sum(values) / len(values)),
            "points": int(len(values)),
        }
    return summary


def start_lifecycle_run(
    *,
    model_name: str,
    action: str,
    status: str,
    triggered_by: Optional[str] = None,
    model_version: Optional[str] = None,
    parent_version: Optional[str] = None,
    mutation_kind: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> int:
    init_db()
    now_ms = _now_ms()

    def _write(con) -> int:
        cur = con.execute(
            """
            INSERT INTO model_lifecycle_runs(
              model_name, model_version, parent_version, action, status,
              triggered_by, mutation_kind, details_json, created_ts_ms, updated_ts_ms
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(model_name),
                (str(model_version) if model_version else None),
                (str(parent_version) if parent_version else None),
                str(action),
                str(status),
                (str(triggered_by) if triggered_by else None),
                (str(mutation_kind) if mutation_kind else None),
                _json_dumps(details or {}),
                int(now_ms),
                int(now_ms),
            ),
        )
        return int(cur.lastrowid)

    return int(run_write_txn(_write) or 0)


def finish_lifecycle_run(run_id: int, *, status: str, details: Optional[Dict[str, Any]] = None) -> None:
    init_db()
    now_ms = _now_ms()

    def _write(con) -> None:
        existing = con.execute(
            "SELECT details_json FROM model_lifecycle_runs WHERE id=?",
            (int(run_id),),
        ).fetchone()
        merged = _safe_json_dict(existing[0] if existing else None)
        merged.update(details or {})
        con.execute(
            """
            UPDATE model_lifecycle_runs
            SET status=?, details_json=?, updated_ts_ms=?
            WHERE id=?
            """,
            (str(status), _json_dumps(merged), int(now_ms), int(run_id)),
        )

    run_write_txn(_write)


def should_retrain(model_name: str = DEFAULT_MODEL_NAME) -> Dict[str, Any]:
    learning_signals = detect_learning_signals(model_name)
    if bool(learning_signals.get("performance_drop")):
        return {
            "should_retrain": True,
            "reason": "performance_drop",
            "trigger_reasons": list(learning_signals.get("performance_reasons") or []),
            "learning_signals": learning_signals,
        }

    if bool(learning_signals.get("drift_detected")):
        return {
            "should_retrain": True,
            "reason": "drift_detected",
            "drift_ratio": float(learning_signals.get("drift_ratio") or 0.0),
            "learning_signals": learning_signals,
        }

    if bool(learning_signals.get("regime_shift")):
        return {
            "should_retrain": True,
            "reason": "regime_shift",
            "distribution_state": str(learning_signals.get("distribution_state") or "UNKNOWN"),
            "learning_signals": learning_signals,
        }

    latest = get_latest_version(model_name)
    if not latest:
        return {
            "should_retrain": True,
            "reason": "missing_version",
            "learning_signals": learning_signals,
        }

    age_ms = max(0, _now_ms() - _safe_int(latest.get("updated_ts_ms"), 0))
    if age_ms >= int(RETRAIN_INTERVAL_MS):
        return {
            "should_retrain": True,
            "reason": "stale_version",
            "age_ms": int(age_ms),
            "latest_version": str(latest.get("model_version") or ""),
            "learning_signals": learning_signals,
        }

    summary = summarize_version_performance(
        model_name,
        str(latest.get("model_version") or ""),
        metric_names=("quality_score", "competition_score", "shadow_win_rate"),
        limit=max(RETIRE_MIN_POINTS, 6),
    )
    for metric_name, metric_summary in dict(summary.get("metrics") or {}).items():
        if (
            _safe_int(metric_summary.get("points"), 0) >= int(RETIRE_MIN_POINTS)
            and _safe_float(metric_summary.get("avg"), 1.0) < float(RETIRE_SCORE_THRESHOLD)
        ):
            return {
                "should_retrain": True,
                "reason": f"underperforming:{metric_name}",
                "latest_version": str(latest.get("model_version") or ""),
                "metric_avg": _safe_float(metric_summary.get("avg"), 0.0),
                "learning_signals": learning_signals,
            }

    return {
        "should_retrain": False,
        "reason": "healthy_recent_version",
        "latest_version": str(latest.get("model_version") or ""),
        "learning_signals": learning_signals,
    }


def plan_training_variation(
    *,
    model_name: str,
    base_lookback_days: int,
    symbols: Iterable[str],
    horizons: Iterable[int],
) -> Dict[str, Any]:
    latest = get_latest_version(model_name)
    latest_version = str((latest or {}).get("model_version") or "")
    next_version = next_model_version(model_name)
    retrain_reason = should_retrain(model_name)

    factors = [
        ("baseline_retrain", 1.0),
        ("fast_decay", max(0.4, float(VARIATION_FAST_FACTOR))),
        ("slow_decay", min(2.0, float(VARIATION_SLOW_FACTOR))),
    ]

    try:
        seq = int(next_version.split("-", 1)[0][1:])
    except Exception:
        seq = 1

    selected_kind, selected_factor = factors[seq % len(factors)]
    if str(retrain_reason.get("reason") or "").startswith("underperforming:"):
        selected_kind, selected_factor = ("slow_decay", min(2.0, float(VARIATION_SLOW_FACTOR)))

    tuned_lookback = max(30, int(round(float(base_lookback_days) * float(selected_factor))))
    return {
        "model_name": str(model_name),
        "model_version": str(next_version),
        "parent_version": (latest_version or None),
        "mutation_kind": str(selected_kind),
        "lookback_days": int(tuned_lookback),
        "trigger": retrain_reason,
        "train_scope": {
            "symbols": [str(sym) for sym in (symbols or []) if str(sym).strip()],
            "horizons": [int(h) for h in (horizons or [])],
            "base_lookback_days": int(base_lookback_days),
            "selected_lookback_days": int(tuned_lookback),
        },
    }


def _discover_training_scope() -> Tuple[List[str], List[int]]:
    init_db()
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT symbol, horizon_s
            FROM labels
            WHERE impact_z IS NOT NULL
            ORDER BY symbol ASC, horizon_s ASC
            """
        ).fetchall()
    finally:
        con.close()

    symbols: List[str] = []
    horizons: List[int] = []
    for symbol, horizon_s in rows or []:
        sym = str(symbol or "").upper().strip()
        if sym and sym not in symbols:
            symbols.append(sym)
        try:
            h = int(horizon_s)
        except Exception as e:
            _warn_nonfatal("MODEL_LIFECYCLE_HORIZON_PARSE_FAILED", e, horizon_s=repr(horizon_s))
            continue
        if h > 0 and h not in horizons:
            horizons.append(h)

    if not symbols:
        symbols = ["SPY", "BTC", "OIL"]
    if not horizons:
        horizons = [300, 3600]
    return symbols, horizons


def _discover_hmm_training_scope() -> Tuple[List[str], List[int]]:
    try:
        from engine.strategy.hmm_regime import hmm_model_symbol

        symbol = str(hmm_model_symbol() or "").upper().strip()
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_HMM_SYMBOL_RESOLVE_FAILED", e)
        symbol = ""

    if not symbol:
        symbol = str(os.environ.get("HMM_REGIME_MODEL_SYMBOL", "SPY") or "SPY").upper().strip()
    return ([symbol] if symbol else ["SPY"]), []


def _build_hmm_dataset_snapshot(*, symbol: str, lookback_rows: int) -> Dict[str, Any]:
    feature_names: List[str] = []
    try:
        from engine.strategy.hmm_regime import DEFAULT_HMM_FEATURE_NAMES

        feature_names = [str(name) for name in list(DEFAULT_HMM_FEATURE_NAMES or []) if str(name).strip()]
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_HMM_FEATURE_NAMES_FAILED", e)

    requested_symbol = str(symbol or "").upper().strip()
    effective_symbol = requested_symbol or "SPY"
    row_count = 0
    latest_ts_ms = 0

    init_db()
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT COUNT(*), MAX(ts_ms)
            FROM (
              SELECT ts_ms
              FROM prices
              WHERE symbol=?
                AND ts_ms IS NOT NULL
              ORDER BY ts_ms DESC
              LIMIT ?
            ) recent_prices
            """,
            (
                str(effective_symbol),
                int(max(1, lookback_rows)),
            ),
        ).fetchone()
        row_count = _safe_int((row[0] if row else 0), 0)
        latest_ts_ms = _safe_int((row[1] if row else 0), 0)
    except Exception as e:
        _warn_nonfatal(
            "MODEL_LIFECYCLE_HMM_PRICE_DATASET_FAILED",
            e,
            symbol=str(effective_symbol),
            lookback_rows=int(max(1, lookback_rows)),
        )
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("MODEL_LIFECYCLE_HMM_PRICE_DATASET_CLOSE_FAILED", e, symbol=str(effective_symbol))

    min_rows = max(1, _safe_int(os.environ.get("HMM_TRAIN_MIN_ROWS", "96"), 96))
    dataset_used: Dict[str, Any] = {
        "model_name": "hmm_regime",
        "lookback_rows": int(max(1, lookback_rows)),
        "symbols": [str(effective_symbol)],
        "horizons": [],
        "feature_ids": list(feature_names),
        "captured_ts_ms": _now_ms(),
        "sources": {
            "prices": {
                "table": "prices",
                "row_count": int(row_count),
                "latest_ts_ms": int(latest_ts_ms),
                "symbol": str(effective_symbol),
            },
            "regime_vectors": {
                "usable_rows": int(row_count),
                "required_min_rows": int(min_rows),
            },
        },
    }
    feature_schema = normalize_feature_schema(
        feature_ids=list(feature_names),
        feature_schema={
            "feature_ids": list(feature_names),
            "feature_set_tag": "hmm.regime.v1",
            "feature_count": int(len(feature_names)),
        },
    )
    training_window = normalize_training_window(
        captured_ts_ms=int(dataset_used.get("captured_ts_ms") or 0),
        lookback_rows=int(max(1, lookback_rows)),
        training_window={
            "lookback_rows": int(max(1, lookback_rows)),
            "end_ts_ms": int(latest_ts_ms),
        },
        symbols=[str(effective_symbol)],
        horizons=[],
    )
    dataset_used["feature_schema"] = dict(feature_schema)
    dataset_used["training_window"] = dict(training_window)
    dataset_used["fingerprint"] = hashlib.sha1(
        json.dumps(dataset_used or {}, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return materialize_dataset_snapshot(
        dataset_used,
        feature_schema=feature_schema,
        training_window=training_window,
        extra_manifest={"job_name": "train_hmm_regime", "dataset_contract": "training_provenance_v1"},
    )


def _family_training_job(model_name: str) -> Optional[Dict[str, Any]]:
    name = str(model_name or "").strip()
    if name == "regime_stats_v2":
        return {
            "job_name": "train_model_v2",
            "module_name": "engine.strategy.jobs.train_model_v2",
            "base_lookback_days": int(os.environ.get("MODEL_V2_LOOKBACK_DAYS", "180")),
        }
    if name == "embed_regressor":
        return {
            "job_name": "train_embed_models",
            "module_name": "engine.strategy.train_embed_models",
            "base_lookback_days": int(os.environ.get("EMBED_MODEL_LOOKBACK_DAYS", "365")),
        }
    if name == "temporal_predictor":
        return {
            "job_name": "train_temporal_predictor",
            "module_name": "engine.strategy.train_temporal_predictor",
            "base_lookback_days": int(os.environ.get("TEMPORAL_LOOKBACK_DAYS", "365")),
        }
    if name == "gbm_regressor" or name.startswith("gbm_regressor."):
        return {
            "job_name": "train_gbm_regressor",
            "module_name": "engine.strategy.gbm_regressor",
            "base_lookback_days": int(os.environ.get("GBM_LOOKBACK_DAYS", "365")),
        }
    if name == "lgbm_regressor" or name.startswith("lgbm_regressor."):
        return {
            "job_name": "train_lgbm_models",
            "module_name": "engine.strategy.models.lgbm_regressor",
            "base_lookback_days": int(os.environ.get("LGBM_LOOKBACK_DAYS", "365")),
        }
    if name == "lgbm_ranker" or name.startswith("lgbm_ranker."):
        return {
            "job_name": "train_lgbm_ranker_models",
            "module_name": "engine.strategy.models.lgbm_ranker",
            "base_lookback_days": int(os.environ.get("LGBM_RANKER_LOOKBACK_DAYS", "365")),
        }
    if name == "xgb_regressor" or name.startswith("xgb_regressor."):
        return {
            "job_name": "train_xgb_models",
            "module_name": "engine.strategy.models.xgb_regressor",
            "base_lookback_days": int(os.environ.get("XGB_LOOKBACK_DAYS", "365")),
        }
    if name == "patchtst" or name.startswith("patchtst."):
        return {
            "job_name": "train_patchtst_models",
            "module_name": "engine.strategy.models.patchtst",
            "base_lookback_days": int(os.environ.get("PATCHTST_LOOKBACK_DAYS", "365")),
        }
    if name == "hmm_regime":
        return {
            "job_name": "train_hmm_regime",
            "module_name": "engine.strategy.jobs.train_hmm_regime",
            "base_lookback_days": int(os.environ.get("HMM_TRAIN_LOOKBACK_DAYS", "180")),
        }
    return None


def _latest_open_lifecycle_run(model_name: str, action: str) -> Optional[Dict[str, Any]]:
    init_db()
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT id, model_version, parent_version, status, details_json, created_ts_ms
            FROM model_lifecycle_runs
            WHERE model_name=? AND action=? AND status IN ('queued','running')
            ORDER BY created_ts_ms DESC
            LIMIT 1
            """,
            (str(model_name), str(action)),
        ).fetchone()
    finally:
        con.close()

    if not row:
        return None
    return {
        "id": _safe_int(row[0], 0),
        "model_version": str(row[1] or ""),
        "parent_version": row[2],
        "status": str(row[3] or ""),
        "details": _safe_json_dict(row[4]),
        "created_ts_ms": _safe_int(row[5], 0),
    }


def create_training_plan(model_name: str) -> Dict[str, Any]:
    family = _family_training_job(model_name)
    if not family:
        return {}

    if str(model_name or "").strip() == "hmm_regime":
        symbols, horizons = _discover_hmm_training_scope()
    else:
        symbols, horizons = _discover_training_scope()
    variation = plan_training_variation(
        model_name=str(model_name),
        base_lookback_days=int(family.get("base_lookback_days") or 180),
        symbols=symbols,
        horizons=horizons,
    )
    variation["job_name"] = str(family.get("job_name") or "")
    variation["module_name"] = str(family.get("module_name") or "")
    variation["symbols"] = list(symbols)
    variation["horizons"] = list(horizons)
    if str(model_name or "").strip() == "hmm_regime":
        lookback_rows = max(1, _safe_int(os.environ.get("HMM_TRAIN_LOOKBACK_ROWS", "640"), 640))
        min_rows = max(1, _safe_int(os.environ.get("HMM_TRAIN_MIN_ROWS", "96"), 96))
        variation["dataset_used"] = _build_hmm_dataset_snapshot(
            symbol=(symbols[0] if symbols else ""),
            lookback_rows=int(lookback_rows),
        )
        train_scope = dict(variation.get("train_scope") or {})
        train_scope.update(
            {
                "symbols": list(symbols),
                "horizons": [],
                "lookback_rows": int(lookback_rows),
                "min_rows": int(min_rows),
                "dataset_used": dict(variation.get("dataset_used") or {}),
            }
        )
        variation["train_scope"] = train_scope
    else:
        variation["dataset_used"] = build_dataset_snapshot(
            model_name=str(model_name),
            lookback_days=int(variation.get("lookback_days") or family.get("base_lookback_days") or 0),
            symbols=list(symbols),
            horizons=list(horizons),
            extra={"job_name": str(family.get("job_name") or ""), "trigger": variation.get("trigger") or {}},
        )
    return variation


def dispatch_training_plan(plan: Dict[str, Any], *, triggered_by: str = "model_lifecycle_manager") -> Dict[str, Any]:
    payload = dict(plan or {})
    model_name = str(payload.get("model_name") or "").strip()
    model_version = str(payload.get("model_version") or "").strip()
    module_name = str(payload.get("module_name") or "").strip()
    job_name = str(payload.get("job_name") or "").strip()
    if not model_name or not model_version or not module_name or not job_name:
        return {"ok": False, "error": "invalid_training_plan", "plan": payload}

    existing = _latest_open_lifecycle_run(model_name, job_name)
    if existing:
        return {
            "ok": True,
            "skipped": True,
            "reason": "training_already_pending",
            "run_id": int(existing.get("id") or 0),
            "model_version": str(existing.get("model_version") or ""),
        }

    run_id = start_lifecycle_run(
        model_name=model_name,
        model_version=model_version,
        parent_version=payload.get("parent_version"),
        action=job_name,
        status="queued",
        triggered_by=str(triggered_by),
        mutation_kind=payload.get("mutation_kind"),
        details={"variation": payload},
    )
    payload["lifecycle_run_id"] = int(run_id)

    register_model_version(
        model_name=model_name,
        model_version=model_version,
        model_kind=str(payload.get("model_kind") or model_name),
        parent_version=payload.get("parent_version"),
        mutation_kind=str(payload.get("mutation_kind") or "baseline_retrain"),
        stage="shadow",
        status="queued",
        live_ready=False,
        training_job_name=job_name,
        train_scope=dict(payload.get("train_scope") or {}),
        meta={"trigger": payload.get("trigger") or {}, "dispatched_by": str(triggered_by)},
    )

    env = dict(os.environ)
    env[LIFECYCLE_PLAN_ENV] = _json_dumps(payload)
    project_root = Path(__file__).resolve().parents[2]
    proc = subprocess.run(
        [sys.executable, "-u", "-m", module_name],
        cwd=str(project_root),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    details = {
        "variation": payload,
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or "")[-4000:],
        "stderr": str(proc.stderr or "")[-4000:],
    }
    if int(proc.returncode) == 0:
        return {
            "ok": True,
            "run_id": int(run_id),
            "model_version": model_version,
            "returncode": int(proc.returncode),
            "stdout": str(proc.stdout or ""),
        }

    finish_lifecycle_run(int(run_id), status="error", details=details)
    update_model_version_status(
        model_name,
        model_version,
        stage="retired",
        status="error",
        live_ready=False,
        meta_patch={"dispatch_failed_ts_ms": _now_ms(), "dispatch_returncode": int(proc.returncode)},
    )
    return {
        "ok": False,
        "run_id": int(run_id),
        "model_version": model_version,
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or ""),
        "stderr": str(proc.stderr or ""),
    }


def publish_lifecycle_status(payload: Dict[str, Any]) -> None:
    try:
        meta_set("model_lifecycle_status", _json_dumps(payload or {}))
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_STATUS_META_SET_FAILED", e, payload_size=int(len(_json_dumps(payload or {}))))


def _registry_has_champion(model_name: str) -> bool:
    try:
        from engine.model_registry import get_stage_latest

        row = get_stage_latest(str(model_name), "champion", regime="global")
    except Exception as e:
        _warn_nonfatal("MODEL_LIFECYCLE_REGISTRY_CHAMPION_CHECK_FAILED", e, model_name=str(model_name))
        return False
    return bool(row)


def mark_version_live(
    model_name: str,
    model_version: str,
    *,
    stage: str = "champion",
    meta_patch: Optional[Dict[str, Any]] = None,
) -> None:
    if str(stage or "").strip().lower() == "champion" and not _registry_has_champion(str(model_name)):
        raise RuntimeError(
            f"cannot mark model_version live as champion before registry promotion model={model_name}"
        )
    update_model_version_status(
        str(model_name),
        str(model_version),
        stage=str(stage),
        status="live",
        live_ready=True,
        meta_patch=dict(meta_patch or {}),
    )
    try:
        meta_set(_active_version_meta_key(str(model_name)), str(model_version))
    except Exception as e:
        _warn_nonfatal(
            "MODEL_LIFECYCLE_ACTIVE_VERSION_META_SET_FAILED",
            e,
            model_name=str(model_name),
            model_version=str(model_version),
            stage=str(stage),
        )


def list_model_versions(model_name: str, *, limit: int = 20) -> List[Dict[str, Any]]:
    init_db()
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT model_version, model_kind, parent_version, mutation_kind, stage, status,
                   live_ready, training_job_name, train_scope_json, meta_json, created_ts_ms, updated_ts_ms
            FROM model_versions
            WHERE model_name=?
            ORDER BY updated_ts_ms DESC, created_ts_ms DESC
            LIMIT ?
            """,
            (str(model_name), int(max(1, min(500, int(limit or 20))))),
        ).fetchall()
    finally:
        con.close()

    out: List[Dict[str, Any]] = []
    for row in rows or []:
        train_scope = _safe_json_dict(row[8])
        meta = _safe_json_dict(row[9])
        dataset_used = train_scope.get("dataset_used") or meta.get("dataset_used") or {}
        training_timestamp_ms = _safe_int(
            meta.get("training_completed_ts_ms") or meta.get("training_started_ts_ms"),
            _safe_int(row[10], 0),
        )
        out.append(
            {
                "model_name": str(model_name),
                "model_version": str(row[0] or ""),
                "model_kind": str(row[1] or ""),
                "parent_version": row[2],
                "mutation_kind": str(row[3] or ""),
                "stage": str(row[4] or ""),
                "status": str(row[5] or ""),
                "live_ready": bool(int(row[6] or 0)),
                "training_job_name": str(row[7] or ""),
                "train_scope": train_scope,
                "meta": meta,
                "dataset_used": dataset_used,
                "training_timestamp_ms": int(training_timestamp_ms),
                "created_ts_ms": _safe_int(row[10], 0),
                "updated_ts_ms": _safe_int(row[11], 0),
                "performance": summarize_version_performance(str(model_name), str(row[0] or ""), limit=6),
            }
        )
    return out


def get_lifecycle_summary(model_names: Optional[Sequence[str]] = None, *, limit: int = 6) -> Dict[str, Any]:
    names = [str(name).strip() for name in (model_names or []) if str(name).strip()]
    if not names:
        names = _default_lifecycle_model_names()
    families: Dict[str, Any] = {}
    for model_name in names:
        versions = list_model_versions(model_name, limit=limit)
        latest = versions[0] if versions else None
        learning_signals = detect_learning_signals(model_name)
        families[model_name] = {
            "latest": latest,
            "versions": versions,
            "active_version": str(meta_get(_active_version_meta_key(model_name), "") or "").strip() or None,
            "learning_signals": learning_signals,
        }
    return {
        "ok": True,
        "ts_ms": _now_ms(),
        "families": families,
    }


def sync_registry_metrics(model_name: str = DEFAULT_MODEL_NAME, *, limit: int = 50) -> int:
    init_db()
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT metrics_json, stage, created_ts_ms
            FROM model_registry
            WHERE model_name=?
            ORDER BY created_ts_ms DESC
            LIMIT ?
            """,
            (str(model_name), int(limit)),
        ).fetchall()
    finally:
        con.close()

    inserted = 0
    seen: Set[str] = set()
    for metrics_json, stage, created_ts_ms in rows or []:
        metrics = _safe_json_dict(metrics_json)
        model_version = str(metrics.get("model_version") or "").strip()
        if not model_version:
            continue
        dedupe_key = "|".join([model_version, str(stage or ""), str(created_ts_ms or 0)])
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        numeric_metrics = {
            key: value
            for key, value in metrics.items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
        inserted += record_version_performance(
            model_name=str(model_name),
            model_version=str(model_version),
            metric_scope=f"registry:{str(stage or '').strip() or 'unknown'}",
            metrics=numeric_metrics,
            sample_n=_safe_int(metrics.get("train_rows") or metrics.get("rows_upserted"), 0),
            meta={"created_ts_ms": _safe_int(created_ts_ms, 0)},
        )
    return int(inserted)


def retire_underperforming_versions(
    model_name: str = DEFAULT_MODEL_NAME,
    *,
    protect_versions: Optional[Iterable[str]] = None,
) -> List[str]:
    protected = {str(v) for v in (protect_versions or []) if str(v).strip()}
    init_db()
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT model_version, live_ready
            FROM model_versions
            WHERE model_name=? AND status IN ('candidate','shadow','challenger')
            ORDER BY updated_ts_ms DESC
            """,
            (str(model_name),),
        ).fetchall()
    finally:
        con.close()

    retired: List[str] = []
    for model_version, live_ready in rows or []:
        version = str(model_version or "").strip()
        if not version or version in protected or bool(int(live_ready or 0)):
            continue
        summary = summarize_version_performance(
            model_name,
            version,
            metric_names=("quality_score", "competition_score", "shadow_win_rate"),
            limit=max(RETIRE_MIN_POINTS, 6),
        )
        metrics = dict(summary.get("metrics") or {})
        should_retire = False
        for metric_summary in metrics.values():
            if (
                _safe_int(metric_summary.get("points"), 0) >= int(RETIRE_MIN_POINTS)
                and _safe_float(metric_summary.get("avg"), 1.0) < float(RETIRE_SCORE_THRESHOLD)
            ):
                should_retire = True
                break
        if not should_retire:
            continue

        update_model_version_status(
            str(model_name),
            version,
            stage="retired",
            status="retired",
            live_ready=False,
            meta_patch={"retired_ts_ms": _now_ms(), "retired_reason": "persistent_underperformance"},
        )

        pattern = f'"model_version":"{version}"'

        def _write(con) -> None:
            con.execute(
                """
                UPDATE model_registry
                SET stage='retired'
                WHERE model_name=?
                  AND stage IN ('shadow','challenger')
                  AND instr(metrics_json, ?) > 0
                """,
                (str(model_name), str(pattern)),
            )

        run_write_txn(_write)
        retired.append(version)

    return retired


def run_model_lifecycle_job(model_name: str = DEFAULT_MODEL_NAME) -> Dict[str, Any]:
    init_db()
    model_names = [str(model_name)]
    if str(model_name) == str(DEFAULT_MODEL_NAME):
        model_names = _default_lifecycle_model_names()

    family_status: Dict[str, Any] = {}
    total_synced = 0
    all_retired: List[str] = []
    dispatched_plans: List[Dict[str, Any]] = []
    for name in model_names:
        synced = sync_registry_metrics(name)
        latest = get_latest_version(name)
        protected = []
        if latest and str(latest.get("status") or "") != "retired":
            protected.append(str(latest.get("model_version") or ""))
        retired = retire_underperforming_versions(name, protect_versions=protected)
        retrain = should_retrain(name)
        dispatch = None
        if bool(retrain.get("should_retrain")):
            plan = create_training_plan(name)
            if plan:
                dispatch = dispatch_training_plan(plan)
                if dispatch.get("ok") and not dispatch.get("skipped"):
                    dispatched_plans.append(
                        {
                            "model_name": str(name),
                            "job_name": str(plan.get("job_name") or ""),
                            "model_version": str(plan.get("model_version") or ""),
                            "mutation_kind": str(plan.get("mutation_kind") or ""),
                            "parent_version": plan.get("parent_version"),
                        }
                    )
        total_synced += int(synced)
        all_retired.extend([f"{name}:{version}" for version in retired])
        family_status[name] = {
            "synced_metrics": int(synced),
            "retired_versions": retired,
            "latest_version": str((latest or {}).get("model_version") or ""),
            "retrain": retrain,
            "dispatch": dispatch,
        }

    status = {
        "ok": True,
        "model_name": str(model_name),
        "synced_metrics": int(total_synced),
        "retired_versions": all_retired,
        "dispatched_plans": dispatched_plans,
        "families": family_status,
        "ts_ms": _now_ms(),
    }
    publish_lifecycle_status(status)
    return status
