"""
FILE: train_model_v2.py

Strategy job entrypoint for `train_model_v2`.
"""

# train_model_v2.py

import json
import time
import os
import logging
import socket

import numpy as np
from sklearn.model_selection import TimeSeriesSplit

from engine.backtest.cpcv import CombinatorialPurgedKFold
from engine.data.futures_roll import load_futures_roll_boundaries
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.training_guard import training_allowed
from engine.strategy.model_lifecycle import (
    finish_lifecycle_run,
    load_lifecycle_plan,
    plan_training_variation,
    publish_lifecycle_status,
    update_model_version_status,
    record_version_performance,
    register_model_version,
    retire_underperforming_versions,
    start_lifecycle_run,
)
from engine.strategy.learning_loop import build_dataset_snapshot
from engine.model_registry import register_model
from engine.strategy.model_v2 import train_regime_stats


# ----------------------------
# Job identity
# ----------------------------
JOB_NAME = "train_model_v2"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", socket.gethostname())),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [train_model_v2] %(message)s",
)
LOG = logging.getLogger("engine.strategy.jobs.train_model_v2")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.train_model_v2",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


# ----------------------------
# Training config
# ----------------------------
DEFAULT_SYMBOLS = ["SPY", "BTC", "OIL"]
DEFAULT_HORIZONS = [300, 3600]
LOOKBACK_DAYS = int(os.environ.get("MODEL_V2_LOOKBACK_DAYS", "180"))
MODEL_NAME = str(os.environ.get("MODEL_V2_NAME", "regime_stats_v2") or "regime_stats_v2").strip()
MODEL_KIND = str(os.environ.get("MODEL_V2_KIND", "regime_stats_versioned") or "regime_stats_versioned").strip()
MODEL_V2_CV_SPLITS = int(os.environ.get("MODEL_V2_CV_SPLITS", "6"))
MODEL_V2_CPCV_TEST_SPLITS = int(os.environ.get("MODEL_V2_CPCV_TEST_SPLITS", "2"))
MODEL_V2_CPCV_EMBARGO = float(os.environ.get("MODEL_V2_CPCV_EMBARGO", "0.01"))


def _infer_holding_horizon_bars(horizons) -> int:
    configured = os.environ.get("MODEL_V2_HOLDING_HORIZON_BARS")
    if configured not in (None, ""):
        try:
            return max(1, int(configured))
        except Exception:
            return 1
    values = sorted({int(h) for h in list(horizons or []) if int(h or 0) > 0})
    if len(values) >= 2 and values[0] > 0:
        return max(1, int(round(float(values[-1]) / float(values[0]))))
    return 1


def _select_cv_splitter(
    *,
    holding_horizon_bars: int,
    n_samples: int,
    roll_times=None,
    label_start_times=None,
    label_end_times=None,
):
    splits = max(2, min(int(MODEL_V2_CV_SPLITS), max(2, int(n_samples) // 2)))
    if int(holding_horizon_bars or 1) > 1:
        return CombinatorialPurgedKFold(
            n_splits=splits,
            n_test_splits=max(1, min(int(MODEL_V2_CPCV_TEST_SPLITS), splits - 1)),
            embargo=float(MODEL_V2_CPCV_EMBARGO),
            label_horizon=int(holding_horizon_bars),
            label_start_times=label_start_times,
            label_end_times=label_end_times,
            roll_times=roll_times,
        )
    return TimeSeriesSplit(n_splits=splits)


def _training_scope():
    # Training scope is inferred from actual labeled coverage so the job follows
    # the data that exists instead of hard-coding stale symbol/horizon lists.
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT DISTINCT symbol, horizon_s
            FROM labels
            WHERE impact_z IS NOT NULL
            ORDER BY symbol ASC, horizon_s ASC
            """
        ).fetchall()
    except Exception:
        rows = []
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("TRAIN_MODEL_V2_CLOSE_FAILED", e, once_key="training_scope_close")

    if not rows:
        return list(DEFAULT_SYMBOLS), list(DEFAULT_HORIZONS)

    symbols = []
    horizons = []

    for sym, horizon_s in rows:
        sym = str(sym or "").upper().strip()
        if sym and sym not in symbols:
            symbols.append(sym)
        try:
            h = int(horizon_s)
        except Exception as e:
            _warn_nonfatal(
                "TRAIN_MODEL_V2_HORIZON_PARSE_FAILED",
                e,
                once_key=f"horizon_parse:{horizon_s!r}",
                horizon_s=horizon_s,
            )
            continue
        if h not in horizons:
            horizons.append(h)

    return (symbols or list(DEFAULT_SYMBOLS)), (horizons or list(DEFAULT_HORIZONS))


def _load_oos_return_series(*, symbols, horizons, lookback_days: int, holdout_fraction: float = 0.20, holding_horizon_bars: int = 1) -> dict:
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - int(lookback_days) * 24 * 3600 * 1000
    symset = {str(symbol or "").upper().strip() for symbol in list(symbols or []) if str(symbol or "").strip()}
    hset = {int(horizon) for horizon in list(horizons or [])}
    con = connect()
    roll_times: list[int] = []
    try:
        rows = con.execute(
            """
            SELECT e.ts_ms, l.symbol, l.horizon_s, COALESCE(le.net_z, l.impact_z) AS impact_z
            FROM labels l
            JOIN events e ON e.id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            WHERE e.ts_ms >= ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            ORDER BY e.ts_ms ASC, l.symbol ASC, l.horizon_s ASC
            """,
            (int(cutoff_ms),),
        ).fetchall()
        if rows:
            row_symbols = [str(row[1] or "").upper().strip() for row in rows or []]
            starts = [int(row[0] or 0) for row in rows or []]
            ends = [int(row[0] or 0) + int(row[2] or 0) * 1000 for row in rows or []]
            roll_times = load_futures_roll_boundaries(
                con,
                symbols=row_symbols,
                start_ts_ms=min(starts) if starts else None,
                end_ts_ms=max(ends) if ends else None,
            )
    except Exception as e:
        _warn_nonfatal("TRAIN_MODEL_V2_OOS_RETURNS_LOAD_FAILED", e, once_key="oos_returns_load")
        return {"oos_returns": [], "oos_return_count": 0, "oos_holdout_fraction": float(holdout_fraction)}
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("TRAIN_MODEL_V2_CLOSE_FAILED", e, once_key="oos_returns_close")

    values = []
    label_starts = []
    label_ends = []
    for _ts_ms, symbol, horizon_s, impact_z in rows or []:
        sym = str(symbol or "").upper().strip()
        try:
            horizon = int(horizon_s)
            value = float(impact_z)
        except Exception:
            continue
        if symset and sym not in symset:
            continue
        if hset and horizon not in hset:
            continue
        values.append(float(value))
        label_starts.append(float(int(_ts_ms or 0)))
        label_ends.append(float(int(_ts_ms or 0) + int(horizon) * 1000))

    if not values:
        return {"oos_returns": [], "oos_return_count": 0, "oos_holdout_fraction": float(holdout_fraction)}

    cv_method = "time_series_holdout"
    if int(holding_horizon_bars or 1) > 1 and len(values) >= 6:
        splitter = _select_cv_splitter(
            holding_horizon_bars=int(holding_horizon_bars),
            n_samples=len(values),
            roll_times=roll_times,
            label_start_times=np.asarray(label_starts, dtype=float),
            label_end_times=np.asarray(label_ends, dtype=float),
        )
        arr = np.asarray(values, dtype=float)
        oos_values: list[float] = []
        for _train_idx, test_idx in splitter.split(np.arange(len(values), dtype=float)):
            oos_values.extend(float(v) for v in arr[np.asarray(test_idx, dtype=int)].tolist())
        oos_returns = oos_values
        cv_method = "combinatorial_purged_kfold"
    else:
        split_idx = int(max(0, min(len(values) - 1, round(len(values) * (1.0 - float(holdout_fraction))))))
        oos_returns = [float(value) for value in values[split_idx:]]
    return {
        "oos_returns": oos_returns,
        "oos_return_count": int(len(oos_returns)),
        "oos_holdout_fraction": float(holdout_fraction),
        "cv_method": str(cv_method),
        "holding_horizon_bars": int(holding_horizon_bars or 1),
        "oos_return_mean": float(sum(oos_returns) / len(oos_returns)) if oos_returns else 0.0,
    }


# ----------------------------
# Main logic
# ----------------------------
def _run_training() -> int:
    init_db()
    lifecycle_run_id = None
    variation = {}
    try:
        # `training_allowed()` is the operational gate. This script should be safe to
        # schedule repeatedly because the guard, not the caller, decides if training proceeds.
        if not training_allowed():
            log_failure(
                LOG,
                event="train_model_v2_training_guard_blocked",
                code="TRAIN_MODEL_V2_TRAINING_GUARD_BLOCKED",
                message="training blocked by training_guard",
                level=logging.WARNING,
                component="engine.strategy.jobs.train_model_v2",
                extra={"model_name": MODEL_NAME, "job_name": JOB_NAME},
                persist=False,
            )
            return 0

        symbols, horizons = _training_scope()
        holding_horizon_bars = _infer_holding_horizon_bars(horizons)
        variation = load_lifecycle_plan(MODEL_NAME) or plan_training_variation(
            model_name=MODEL_NAME,
            base_lookback_days=LOOKBACK_DAYS,
            symbols=symbols,
            horizons=horizons,
        )
        lifecycle_run_id = int(variation.get("lifecycle_run_id") or 0)
        if lifecycle_run_id <= 0:
            lifecycle_run_id = start_lifecycle_run(
                model_name=MODEL_NAME,
                model_version=str(variation.get("model_version") or ""),
                parent_version=variation.get("parent_version"),
                action="train_model_v2",
                status="running",
                triggered_by=JOB_NAME,
                mutation_kind=variation.get("mutation_kind"),
                details={"variation": variation},
            )

        training_started_ts_ms = int(time.time() * 1000)
        dataset_used = build_dataset_snapshot(
            model_name=MODEL_NAME,
            lookback_days=int(variation.get("lookback_days") or LOOKBACK_DAYS),
            symbols=symbols,
            horizons=horizons,
            extra={
                "job_name": JOB_NAME,
                "mutation_kind": str(variation.get("mutation_kind") or "baseline_retrain"),
                "holding_horizon_bars": int(holding_horizon_bars),
            },
        )

        register_model_version(
            model_name=MODEL_NAME,
            model_version=str(variation.get("model_version") or ""),
            model_kind=MODEL_KIND,
            parent_version=variation.get("parent_version"),
            mutation_kind=str(variation.get("mutation_kind") or "baseline_retrain"),
            stage="shadow",
            status="training",
            live_ready=False,
            training_job_name=JOB_NAME,
            train_scope={
                **dict(variation.get("train_scope") or {}),
                "dataset_used": dataset_used,
            },
            meta={
                "trigger": variation.get("trigger") or {},
                "dataset_used": dataset_used,
                "training_started_ts_ms": int(training_started_ts_ms),
            },
        )

        logging.info(
            "training v2 regime stats model=%s version=%s mutation=%s symbols=%s horizons=%s lookback_days=%s",
            MODEL_NAME,
            variation.get("model_version"),
            variation.get("mutation_kind"),
            symbols,
            horizons,
            variation.get("lookback_days"),
        )

        n = train_regime_stats(
            symbols,
            horizons,
            lookback_days=int(variation.get("lookback_days") or LOOKBACK_DAYS),
            model_version=str(variation.get("model_version") or ""),
            model_name=MODEL_NAME,
            publish_live=False,
        )

        bucket_count = max(1, len(symbols) * len(horizons))
        quality_score = max(0.0, min(1.0, float(n) / float(bucket_count)))
        metrics = {
            "rows_upserted": int(n),
            "train_rows": int(n),
            "quality_score": float(quality_score),
            "bucket_count": int(bucket_count),
            "symbol_count": int(len(symbols)),
            "horizon_count": int(len(horizons)),
            "lookback_days": int(variation.get("lookback_days") or LOOKBACK_DAYS),
            "model_version": str(variation.get("model_version") or ""),
            "parent_version": variation.get("parent_version"),
            "mutation_kind": str(variation.get("mutation_kind") or "baseline_retrain"),
            "dataset_fingerprint": str(dataset_used.get("fingerprint") or ""),
            "holding_horizon_bars": int(holding_horizon_bars),
        }
        oos_payload = _load_oos_return_series(
            symbols=symbols,
            horizons=horizons,
            lookback_days=int(variation.get("lookback_days") or LOOKBACK_DAYS),
            holding_horizon_bars=int(holding_horizon_bars),
        )
        metrics_with_oos = {
            **metrics,
            "oos_return_count": int(oos_payload.get("oos_return_count") or 0),
            "oos_return_mean": float(oos_payload.get("oos_return_mean") or 0.0),
            "oos_returns": list(oos_payload.get("oos_returns") or []),
        }
        record_version_performance(
            model_name=MODEL_NAME,
            model_version=str(variation.get("model_version") or ""),
            metric_scope="training",
            metrics=metrics,
            sample_n=int(n),
            meta={"job_name": JOB_NAME, **oos_payload},
        )
        register_model(
            model_name=MODEL_NAME,
            model_kind=MODEL_KIND,
            model_ts_ms=int(time.time() * 1000),
            stage="shadow",
            metrics=metrics_with_oos,
            note="train_model_v2_lifecycle",
            regime="global",
        )
        update_model_version_status(
            MODEL_NAME,
            str(variation.get("model_version") or ""),
            stage="shadow",
            status="trained",
            live_ready=False,
            meta_patch={
                "dataset_used": dataset_used,
                "training_started_ts_ms": int(training_started_ts_ms),
                "training_completed_ts_ms": int(time.time() * 1000),
                "rows_upserted": int(n),
                **oos_payload,
            },
        )
        retired = retire_underperforming_versions(
            MODEL_NAME,
            protect_versions=[
                str(variation.get("model_version") or ""),
                str(variation.get("parent_version") or ""),
            ],
        )
        finish_lifecycle_run(
            int(lifecycle_run_id or 0),
            status="ok",
            details={
                "rows_upserted": int(n),
                "retired_versions": retired,
                "metrics": metrics,
                "dataset_used": dataset_used,
            },
        )
        publish_lifecycle_status(
            {
                "ok": True,
                "model_name": MODEL_NAME,
                "active_job": JOB_NAME,
                "version": variation.get("model_version"),
                "mutation_kind": variation.get("mutation_kind"),
                "rows_upserted": int(n),
                "retired_versions": retired,
                "dataset_used": dataset_used,
                "ts_ms": int(time.time() * 1000),
            }
        )
        logging.info("trained v2 stats rows_upserted=%s", n)
        return 0
    except Exception:
        if variation:
            try:
                update_model_version_status(
                    MODEL_NAME,
                    str(variation.get("model_version") or ""),
                    stage="retired",
                    status="error",
                    live_ready=False,
                    meta_patch={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception:
                _warn_nonfatal(
                    "TRAIN_MODEL_V2_VERSION_STATUS_FAILED",
                    RuntimeError("update_model_version_status failed"),
                    once_key="version_status_error",
                    version=str(variation.get("model_version") or ""),
                )
        if lifecycle_run_id:
            try:
                finish_lifecycle_run(
                    int(lifecycle_run_id),
                    status="error",
                    details={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception:
                _warn_nonfatal("TRAIN_MODEL_V2_LIFECYCLE_STATUS_FAILED", RuntimeError("publish_lifecycle_status failed"), once_key="lifecycle_status_error", lifecycle_run_id=int(lifecycle_run_id))
        raise


def main() -> int:
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        return 0

    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        rc = int(_run_training() or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", "rc": rc}, separators=(",", ":"), sort_keys=True),
        )
        return rc
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


# ----------------------------
# Entrypoint
# ----------------------------
if __name__ == "__main__":
    raise SystemExit(main())
