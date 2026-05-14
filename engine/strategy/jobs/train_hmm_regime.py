"""Train and persist the opt-in HMM regime model from regime stack history."""

from __future__ import annotations

import json
import hashlib
import logging
import os
import time
from typing import Any, Dict, List

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.dataset_store import materialize_dataset_snapshot, normalize_feature_schema, normalize_training_window
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy.hmm_regime import (
    DEFAULT_HMM_FEATURE_NAMES,
    build_hmm_input_from_regime_vector,
    hmm_model_symbol,
    persist_hmm_model,
    train_hmm,
)
from engine.strategy.model_lifecycle import (
    finish_lifecycle_run,
    load_lifecycle_plan,
    publish_lifecycle_status,
    register_model_version,
    start_lifecycle_run,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.regime_stack import compute_regime_vector


JOB_NAME = "train_hmm_regime"
MODEL_NAME = "hmm_regime"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
TRAIN_SYMBOL = str(os.environ.get("HMM_TRAIN_SYMBOL", hmm_model_symbol()) or hmm_model_symbol()).upper().strip() or hmm_model_symbol()
LOOKBACK_ROWS = int(os.environ.get("HMM_TRAIN_LOOKBACK_ROWS", "640") or 640)
MIN_ROWS = int(os.environ.get("HMM_TRAIN_MIN_ROWS", "96") or 96)
LOG = get_logger("engine.strategy.jobs.train_hmm_regime")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="train_hmm_regime_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.train_hmm_regime",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def _load_training_timestamps(symbol: str, *, lookback_rows: int) -> List[int]:
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT ts_ms
            FROM prices
            WHERE symbol=?
              AND ts_ms IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(max(1, lookback_rows))),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal(
            "TRAIN_HMM_REGIME_TIMESTAMP_LOAD_FAILED",
            exc,
            once_key=f"train_hmm_regime_timestamp_load_failed:{str(symbol).upper().strip()}",
            symbol=str(symbol).upper().strip(),
            lookback_rows=int(max(1, lookback_rows)),
        )
        rows = []
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "TRAIN_HMM_REGIME_TIMESTAMP_CLOSE_FAILED",
                exc,
                once_key="train_hmm_regime_timestamp_close_failed",
            )

    timestamps = [int(row[0]) for row in list(rows or []) if row and row[0] is not None]
    timestamps.sort()
    return timestamps


def _build_training_rows(symbol: str, timestamps: List[int]) -> Dict[str, Any]:
    con = connect(readonly=True)
    feature_rows: List[Dict[str, float]] = []
    skipped = 0
    try:
        for ts_ms in list(timestamps or []):
            try:
                regime_vector = compute_regime_vector(
                    symbol=str(symbol),
                    ts_ms=int(ts_ms),
                    con=con,
                    include_hmm=False,
                )
                feature_row = build_hmm_input_from_regime_vector(regime_vector)
            except Exception as exc:
                _warn_nonfatal(
                    "TRAIN_HMM_REGIME_BUILD_ROW_FAILED",
                    exc,
                    once_key=f"train_hmm_regime_build_row_failed:{str(symbol).upper().strip()}",
                    symbol=str(symbol).upper().strip(),
                    ts_ms=int(ts_ms),
                )
                skipped += 1
                continue
            if not feature_row:
                skipped += 1
                continue
            feature_rows.append(dict(feature_row))
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "TRAIN_HMM_REGIME_BUILD_CLOSE_FAILED",
                exc,
                once_key="train_hmm_regime_build_close_failed",
            )
    return {
        "rows": feature_rows,
        "skipped": int(skipped),
    }


def _build_dataset_used(symbol: str, timestamps: List[int], build_result: Dict[str, Any], captured_ts_ms: int) -> Dict[str, Any]:
    usable_rows = int(len(list(build_result.get("rows") or [])))
    skipped_rows = int(build_result.get("skipped") or 0)
    latest_ts_ms = int(timestamps[-1]) if timestamps else 0
    dataset_used = {
        "model_name": MODEL_NAME,
        "lookback_rows": int(LOOKBACK_ROWS),
        "symbols": [str(symbol).upper().strip()],
        "horizons": [],
        "feature_ids": list(DEFAULT_HMM_FEATURE_NAMES),
        "captured_ts_ms": int(captured_ts_ms),
        "sources": {
            "prices": {
                "table": "prices",
                "row_count": int(len(timestamps)),
                "latest_ts_ms": int(latest_ts_ms),
                "symbol": str(symbol).upper().strip(),
            },
            "regime_vectors": {
                "usable_rows": int(usable_rows),
                "skipped_rows": int(skipped_rows),
                "min_rows": int(MIN_ROWS),
            },
        },
    }
    feature_schema = normalize_feature_schema(
        feature_ids=list(DEFAULT_HMM_FEATURE_NAMES),
        feature_schema={
            "feature_ids": list(DEFAULT_HMM_FEATURE_NAMES),
            "feature_set_tag": "hmm.regime.v1",
            "feature_count": int(len(DEFAULT_HMM_FEATURE_NAMES)),
        },
    )
    training_window = normalize_training_window(
        captured_ts_ms=int(captured_ts_ms),
        lookback_rows=int(LOOKBACK_ROWS),
        training_window={"lookback_rows": int(LOOKBACK_ROWS), "end_ts_ms": int(latest_ts_ms)},
        symbols=[str(symbol).upper().strip()],
        horizons=[],
    )
    dataset_used["feature_schema"] = dict(feature_schema)
    dataset_used["training_window"] = dict(training_window)
    dataset_used["fingerprint"] = hashlib.sha1(_json_dumps(dataset_used).encode("utf-8")).hexdigest()
    row_records = [
        {
            "ts_ms": int(dict(row).get("ts_ms") or 0),
            "symbol": str(dict(row).get("symbol") or ""),
            "vector_json": dict(row).get("vector"),
        }
        for row in list(build_result.get("rows") or [])
        if isinstance(row, dict)
    ]
    return materialize_dataset_snapshot(
        dataset_used,
        row_records=row_records,
        feature_schema=feature_schema,
        training_window=training_window,
        extra_manifest={"job_name": JOB_NAME, "dataset_contract": "training_provenance_v1"},
    )


def _mark_lifecycle_error(
    *,
    model_version: str,
    lifecycle_run_id: int,
    result: Dict[str, Any],
    dataset_used: Dict[str, Any],
) -> None:
    if str(model_version).strip():
        try:
            update_model_version_status(
                MODEL_NAME,
                str(model_version),
                stage="retired",
                status="error",
                live_ready=False,
                meta_patch={
                    "dataset_used": dict(dataset_used or {}),
                    "error_ts_ms": int(time.time() * 1000),
                    "job_result": dict(result or {}),
                },
            )
        except Exception as exc:
            _warn_nonfatal(
                "TRAIN_HMM_REGIME_VERSION_STATUS_FAILED",
                exc,
                once_key=f"train_hmm_regime_version_status_failed:{model_version}",
                model_version=str(model_version),
            )
    if int(lifecycle_run_id) > 0:
        try:
            finish_lifecycle_run(int(lifecycle_run_id), status="error", details=dict(result or {}))
        except Exception as exc:
            _warn_nonfatal(
                "TRAIN_HMM_REGIME_LIFECYCLE_STATUS_FAILED",
                exc,
                once_key=f"train_hmm_regime_lifecycle_status_failed:{int(lifecycle_run_id)}",
                lifecycle_run_id=int(lifecycle_run_id),
            )


def run() -> Dict[str, Any]:
    """Train, persist, and register one HMM regime model version."""
    init_db()
    plan = load_lifecycle_plan(MODEL_NAME)
    training_started_ts_ms = int(time.time() * 1000)
    lifecycle_run_id = int(plan.get("lifecycle_run_id") or 0)
    model_version = str(plan.get("model_version") or version_from_ts(MODEL_NAME, int(training_started_ts_ms), prefix="hmm"))
    if lifecycle_run_id <= 0:
        lifecycle_run_id = int(
            start_lifecycle_run(
                model_name=MODEL_NAME,
                model_version=str(model_version),
                parent_version=plan.get("parent_version"),
                action=JOB_NAME,
                status="running",
                triggered_by=JOB_NAME,
                mutation_kind=plan.get("mutation_kind"),
                details={"variation": dict(plan or {})},
            )
            or 0
        )
    timestamps = _load_training_timestamps(str(TRAIN_SYMBOL), lookback_rows=int(LOOKBACK_ROWS))
    build_result = _build_training_rows(str(TRAIN_SYMBOL), timestamps)
    feature_rows = list(build_result.get("rows") or [])
    dataset_used = _build_dataset_used(str(TRAIN_SYMBOL), timestamps, build_result, int(training_started_ts_ms))
    if len(feature_rows) < int(MIN_ROWS):
        result = {
            "ok": False,
            "status": "insufficient_history",
            "model_name": MODEL_NAME,
            "model_version": str(model_version),
            "symbol": str(TRAIN_SYMBOL),
            "row_count": int(len(feature_rows)),
            "min_rows": int(MIN_ROWS),
            "skipped_rows": int(build_result.get("skipped") or 0),
            "candidate_timestamps": int(len(timestamps)),
            "dataset_used": dict(dataset_used),
        }
        _mark_lifecycle_error(
            model_version=str(model_version),
            lifecycle_run_id=int(lifecycle_run_id),
            result=result,
            dataset_used=dataset_used,
        )
        publish_lifecycle_status(dict(result))
        return result

    model = train_hmm(feature_rows)
    if not bool((model or {}).get("available")):
        result = {
            "ok": False,
            "status": "training_unavailable",
            "model_name": MODEL_NAME,
            "model_version": str(model_version),
            "symbol": str(TRAIN_SYMBOL),
            "row_count": int(len(feature_rows)),
            "metrics": dict((model or {}).get("metrics") or {}),
            "dataset_used": dict(dataset_used),
        }
        _mark_lifecycle_error(
            model_version=str(model_version),
            lifecycle_run_id=int(lifecycle_run_id),
            result=result,
            dataset_used=dataset_used,
        )
        publish_lifecycle_status(dict(result))
        return result

    persist_result = persist_hmm_model(model, symbol=str(TRAIN_SYMBOL))
    if not bool(persist_result.get("ok")):
        result = {
            "ok": False,
            "status": "persist_failed",
            "model_name": MODEL_NAME,
            "model_version": str(model_version),
            "symbol": str(TRAIN_SYMBOL),
            "row_count": int(len(feature_rows)),
            "persist": dict(persist_result),
            "dataset_used": dict(dataset_used),
        }
        _mark_lifecycle_error(
            model_version=str(model_version),
            lifecycle_run_id=int(lifecycle_run_id),
            result=result,
            dataset_used=dataset_used,
        )
        publish_lifecycle_status(dict(result))
        return result

    register_model_version(
        model_name=MODEL_NAME,
        model_version=str(model_version),
        model_kind=MODEL_NAME,
        parent_version=plan.get("parent_version"),
        mutation_kind=str(plan.get("mutation_kind") or "baseline_retrain"),
        stage="shadow",
        status="trained",
        live_ready=False,
        training_job_name=JOB_NAME,
        train_scope={
            **dict(plan.get("train_scope") or {}),
            "symbols": [str(TRAIN_SYMBOL)],
            "horizons": [],
            "lookback_rows": int(LOOKBACK_ROWS),
            "min_rows": int(MIN_ROWS),
            "dataset_used": dict(dataset_used),
        },
        meta={
            "standalone_job": not bool(plan),
            "trigger": plan.get("trigger") or {},
            "dataset_used": dict(dataset_used),
            "training_started_ts_ms": int(training_started_ts_ms),
            "training_completed_ts_ms": int(time.time() * 1000),
            "symbol": str(TRAIN_SYMBOL),
        },
    )
    update_model_version_status(
        MODEL_NAME,
        str(model_version),
        stage="shadow",
        status="trained",
        live_ready=False,
        meta_patch={
            "dataset_used": dict(dataset_used),
            "training_started_ts_ms": int(training_started_ts_ms),
            "training_completed_ts_ms": int(time.time() * 1000),
            "num_states": int((model or {}).get("num_states") or 0),
        },
    )
    if int(lifecycle_run_id) > 0:
        finish_lifecycle_run(
            int(lifecycle_run_id),
            status="ok",
            details={
                "model_version": str(model_version),
                "symbol": str(TRAIN_SYMBOL),
                "row_count": int(len(feature_rows)),
                "dataset_used": dict(dataset_used),
            },
        )

    result = {
        "ok": True,
        "status": "trained",
        "model_name": MODEL_NAME,
        "model_version": str(model_version),
        "symbol": str(TRAIN_SYMBOL),
        "row_count": int(len(feature_rows)),
        "skipped_rows": int(build_result.get("skipped") or 0),
        "candidate_timestamps": int(len(timestamps)),
        "num_states": int((model or {}).get("num_states") or 0),
        "label_map": dict((model or {}).get("label_map") or {}),
        "metrics": dict((model or {}).get("metrics") or {}),
        "persist": dict(persist_result),
        "dataset_used": dict(dataset_used),
    }
    publish_lifecycle_status(dict(result))
    return result


def main() -> int:
    """CLI entrypoint for the HMM regime model training job."""
    result = run()
    print(_json_dumps(result))
    return 0 if bool(result.get("ok")) else 1


if __name__ == "__main__":
    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S):
        raise SystemExit(0)

    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=_json_dumps({"phase": "start", "symbol": str(TRAIN_SYMBOL)}),
        )
        rc = int(main() or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=_json_dumps({"phase": "done", "rc": rc, "symbol": str(TRAIN_SYMBOL)}),
        )
        raise SystemExit(rc)
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)
