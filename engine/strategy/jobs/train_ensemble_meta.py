"""Train and persist the opt-in stacked ensemble meta learner."""

from __future__ import annotations

import json
import logging
import math
import os
import pickle
import time
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.strategy.ensemble_blender import (
    ensemble_meta_retrain_s,
    persist_blend_weights,
    persist_family_performance,
    train_stacking_meta_learner,
)

JOB_NAME = "train_ensemble_meta"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
LOOKBACK_DAYS = int(os.environ.get("ENSEMBLE_META_LOOKBACK_DAYS", "30") or 30)
MIN_ROWS = int(os.environ.get("ENSEMBLE_META_MIN_ROWS", "25") or 25)
MIN_FAMILIES = int(os.environ.get("ENSEMBLE_META_MIN_FAMILIES", "2") or 2)
LOG = get_logger("engine.strategy.jobs.train_ensemble_meta")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="train_ensemble_meta_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.jobs.train_ensemble_meta",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        _warn_nonfatal(
            "TRAIN_ENSEMBLE_META_FLOAT_PARSE_FAILED",
            exc,
            once_key=f"safe_float:{repr(value)[:80]}",
            value=repr(value)[:240],
            default=float(default),
        )
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        _warn_nonfatal(
            "TRAIN_ENSEMBLE_META_INT_PARSE_FAILED",
            exc,
            once_key=f"safe_int:{repr(value)[:80]}",
            value=repr(value)[:240],
            default=int(default),
        )
        return int(default)


def _family_from_model_name(model_name: Any) -> str:
    name = str(model_name or "").strip().lower()
    if name == "gbm_regressor" or name.startswith("gbm_regressor"):
        return "gbm_regressor"
    if name == "temporal_predictor" or name.startswith("temporal_predictor"):
        return "temporal_predictor"
    if name.startswith("regime_stats_") or name == "regime_stats":
        return "regime_stats"
    return "embed_regressor"


def _load_latest_stacked_created_ts() -> int:
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT created_ts
            FROM ensemble_blend_weights
            WHERE mode='stacked'
            ORDER BY created_ts DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        return int(_safe_int((row[0] if row else 0), 0))
    except Exception as exc:
        _warn_nonfatal(
            "TRAIN_ENSEMBLE_META_LATEST_WEIGHT_LOOKUP_FAILED",
            exc,
            once_key="train_ensemble_meta_latest_weight_lookup_failed",
        )
        return 0
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "TRAIN_ENSEMBLE_META_LATEST_WEIGHT_CLOSE_FAILED",
                exc,
                once_key="train_ensemble_meta_latest_weight_close_failed",
            )


def _load_training_rows(*, lookback_days: int) -> Tuple[List[Dict[str, Any]], int, int]:
    cutoff_ts_ms = int(time.time() * 1000) - (max(1, int(lookback_days)) * 24 * 60 * 60 * 1000)
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT
              sp.event_id,
              sp.symbol,
              sp.horizon_s,
              sp.ts_ms,
              sp.model_name,
              sp.predicted_z,
              sp.confidence,
              le.net_z
            FROM shadow_predictions sp
            JOIN labels_exec le
              ON le.event_id=sp.event_id
             AND le.symbol=sp.symbol
             AND le.horizon_s=sp.horizon_s
            WHERE sp.ts_ms >= ?
            ORDER BY sp.ts_ms ASC, sp.event_id ASC, sp.symbol ASC, sp.horizon_s ASC, sp.id ASC
            """,
            (int(cutoff_ts_ms),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal(
            "TRAIN_ENSEMBLE_META_HISTORY_LOAD_FAILED",
            exc,
            once_key="train_ensemble_meta_history_load_failed",
            lookback_days=int(lookback_days),
        )
        rows = []
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "TRAIN_ENSEMBLE_META_HISTORY_CLOSE_FAILED",
                exc,
                once_key="train_ensemble_meta_history_close_failed",
            )

    grouped: Dict[Tuple[int, str, int, int], Dict[str, Any]] = {}
    for event_id, symbol, horizon_s, ts_ms, model_name, predicted_z, confidence, net_z in rows or []:
        group_key = (
            int(_safe_int(event_id, 0)),
            str(symbol or "").upper().strip(),
            int(_safe_int(horizon_s, 0)),
            int(_safe_int(ts_ms, 0)),
        )
        group = grouped.setdefault(
            group_key,
            {
                "event_id": int(_safe_int(event_id, 0)),
                "symbol": str(symbol or "").upper().strip(),
                "horizon_s": int(_safe_int(horizon_s, 0)),
                "ts_ms": int(_safe_int(ts_ms, 0)),
                "target": float(_safe_float(net_z, 0.0)),
                "family_preds": {},
            },
        )
        family = _family_from_model_name(model_name)
        if family in group["family_preds"]:
            continue
        group["family_preds"][str(family)] = {
            "prediction": float(_safe_float(predicted_z, 0.0)),
            "confidence": float(max(0.0, min(1.0, _safe_float(confidence, 0.0)))),
            "model_name": str(model_name or ""),
        }

    history_rows = [
        dict(group)
        for group in grouped.values()
        if len(dict(group.get("family_preds") or {})) >= 1
    ]
    distinct_families = {
        family
        for row in history_rows
        for family in dict(row.get("family_preds") or {}).keys()
    }
    return history_rows, len(history_rows), len(distinct_families)


def _compute_family_performance(history_rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    family_returns: Dict[str, List[float]] = defaultdict(list)
    family_hits: Dict[str, List[float]] = defaultdict(list)
    for row in list(history_rows or []):
        target = float(_safe_float((row or {}).get("target"), 0.0))
        for family, payload in dict((row or {}).get("family_preds") or {}).items():
            pred = float(_safe_float((payload or {}).get("prediction"), 0.0))
            family_returns[str(family)].append(float(pred * target))
            family_hits[str(family)].append(1.0 if ((pred >= 0.0 and target >= 0.0) or (pred < 0.0 and target < 0.0)) else 0.0)

    out: List[Dict[str, Any]] = []
    for family, returns in family_returns.items():
        values = list(returns or [])
        if not values:
            continue
        mean = float(sum(values) / len(values))
        variance = float(sum((value - mean) ** 2 for value in values) / max(1, len(values)))
        stdev = math.sqrt(max(variance, 0.0))
        sharpe = float((mean / stdev) * math.sqrt(len(values))) if stdev > 1e-9 else 0.0
        hits = list(family_hits.get(family) or [])
        hit_rate = float(sum(hits) / len(hits)) if hits else 0.0
        out.append(
            {
                "family": str(family),
                "n_predictions": int(len(values)),
                "realized_sharpe": float(sharpe),
                "hit_rate": float(hit_rate),
            }
        )
    out.sort(key=lambda row: (str(row.get("family") or "")))
    return out


def run() -> Dict[str, Any]:
    """Train and persist the stacked ensemble meta-learner from shadow history."""
    init_db()
    now_ts_ms = int(time.time() * 1000)
    latest_stacked_ts = _load_latest_stacked_created_ts()
    if latest_stacked_ts > 0 and (now_ts_ms - latest_stacked_ts) < int(ensemble_meta_retrain_s() * 1000):
        return {
            "ok": True,
            "status": "skipped_recent",
            "latest_stacked_ts": int(latest_stacked_ts),
            "retrain_after_s": int(ensemble_meta_retrain_s()),
        }

    history_rows, row_count, family_count = _load_training_rows(lookback_days=int(LOOKBACK_DAYS))
    if row_count < int(MIN_ROWS) or family_count < int(MIN_FAMILIES):
        return {
            "ok": False,
            "status": "insufficient_history",
            "row_count": int(row_count),
            "family_count": int(family_count),
            "min_rows": int(MIN_ROWS),
            "min_families": int(MIN_FAMILIES),
        }

    meta_blob = bytes(train_stacking_meta_learner(history_rows))
    try:
        payload = pickle.loads(meta_blob)
    except Exception as exc:
        _warn_nonfatal(
            "TRAIN_ENSEMBLE_META_PAYLOAD_UNPICKLE_FAILED",
            exc,
            once_key=f"train_ensemble_meta_payload_unpickle_failed:{row_count}:{family_count}",
            row_count=int(row_count),
            family_count=int(family_count),
        )
        payload = {}
    weights = dict(payload.get("weights") or {}) if isinstance(payload, Mapping) else {}
    if not weights:
        return {
            "ok": False,
            "status": "empty_weights",
            "row_count": int(row_count),
            "family_count": int(family_count),
        }

    persist_blend_weights(
        mode="stacked",
        regime=None,
        weights=dict(weights),
        meta_blob=meta_blob,
        force=True,
    )

    performance_rows = _compute_family_performance(history_rows)
    window_start_ts = int(min(int(_safe_int((row or {}).get("ts_ms"), now_ts_ms)) for row in history_rows))
    window_end_ts = int(max(int(_safe_int((row or {}).get("ts_ms"), now_ts_ms)) for row in history_rows))
    persisted_performance = persist_family_performance(
        window_start_ts=int(window_start_ts),
        window_end_ts=int(window_end_ts),
        rows=performance_rows,
    )
    return {
        "ok": True,
        "status": "trained",
        "row_count": int(row_count),
        "family_count": int(family_count),
        "weights": dict(weights),
        "meta_rows": int(_safe_int((payload or {}).get("row_count"), row_count)),
        "family_performance_rows": int(persisted_performance),
        "window_start_ts": int(window_start_ts),
        "window_end_ts": int(window_end_ts),
    }


def main() -> int:
    """CLI entrypoint for the stacked ensemble meta-learner training job."""
    result = run()
    LOG.info("train_ensemble_meta_result result=%s", _json_dumps(result))
    return 0 if bool(result.get("ok")) else 1


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


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
            extra_json=_json_dumps({"phase": "start"}),
        )
        rc = int(main() or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=_json_dumps({"phase": "done", "rc": rc}),
        )
        raise SystemExit(rc)
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)
