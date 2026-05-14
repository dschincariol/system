"""Bounded controller for drift-triggered model retraining.

The controller only dispatches into the existing lifecycle planner and never
promotes a candidate directly. Newly created versions remain on the existing
shadow/challenger path and must satisfy the normal CPCV/statistical promotion
gates before any replacement can occur.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.runtime_meta import meta_set
from engine.runtime.storage import (
    connect,
    fetch_recent_drift_retrain_events,
    init_db,
    record_drift_retrain_event,
)
from engine.strategy.model_lifecycle import (
    create_training_plan,
    dispatch_training_plan,
    get_lifecycle_summary,
)


LOG = get_logger("engine.strategy.drift_retrain_controller")
TRIGGERED_BY = "drift_triggered_retrain"
STATUS_META_KEY = "drift_retrain_status"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="drift_retrain_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.drift_retrain_controller",
        extra=extra or None,
        persist=False,
    )


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _family_name(model_name: str) -> str:
    name = str(model_name or "").strip()
    if not name:
        return ""
    if name == "regime_stats_v2":
        return "regime_stats_v2"
    if name == "embed_regressor":
        return "embed_regressor"
    if name == "temporal_predictor":
        return "temporal_predictor"
    if name == "hmm_regime":
        return "hmm_regime"
    if name == "gbm_regressor" or name.startswith("gbm_regressor."):
        return "gbm_regressor"
    prefix = name.split(".", 1)[0]
    return prefix or name


def drift_retrain_config_from_env() -> Dict[str, Any]:
    """Build drift-triggered retraining thresholds from the environment."""
    return {
        "enabled": _safe_bool(os.environ.get("DRIFT_RETRAIN_ENABLED", "0"), False),
        "cooldown_s": max(0, _safe_int(os.environ.get("DRIFT_RETRAIN_COOLDOWN_S", str(6 * 60 * 60)), 6 * 60 * 60)),
        "min_degradation": max(
            0.0,
            _safe_float(os.environ.get("DRIFT_RETRAIN_MIN_DEGRADATION", "0.25"), 0.25),
        ),
        "require_cpcv": _safe_bool(os.environ.get("DRIFT_RETRAIN_REQUIRE_CPCV", "1"), True),
        "require_stat_gate": _safe_bool(os.environ.get("DRIFT_RETRAIN_REQUIRE_STAT_GATE", "1"), True),
        "max_parallel_jobs": max(1, _safe_int(os.environ.get("DRIFT_RETRAIN_MAX_PARALLEL_JOBS", "1"), 1)),
    }


def _fingerprint_payload(payload: Dict[str, Any]) -> str:
    raw = _json_dumps(payload or {})
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _build_trigger_metrics(model_name: str, signals: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    runtime_signal = dict(signals.get("runtime_signal") or {})
    shadow_signal = dict(signals.get("shadow_signal") or {})
    temporal_signal = dict(signals.get("temporal_shadow_signal") or {})
    performance_reasons = [str(x) for x in (signals.get("performance_reasons") or []) if str(x).strip()]
    drift_ratio = _safe_float(signals.get("drift_ratio"), 0.0)
    drift_degradation = max(0.0, drift_ratio - 1.0)
    performance_degradation = 1.0 if bool(signals.get("performance_drop")) else 0.0
    regime_degradation = 1.0 if bool(signals.get("regime_shift")) else 0.0
    degradation_score = max(drift_degradation, performance_degradation, regime_degradation)
    min_degradation = float(cfg.get("min_degradation") or 0.0)
    trigger_types: List[str] = []
    if bool(signals.get("drift_detected")) and drift_degradation >= min_degradation:
        trigger_types.append("drift_ratio")
    if bool(signals.get("performance_drop")) and performance_degradation >= min_degradation:
        trigger_types.append("performance_drop")
    if bool(signals.get("regime_shift")) and regime_degradation >= min_degradation:
        trigger_types.append("regime_shift")

    metrics = {
        "model_name": str(model_name or "").strip(),
        "drift_ratio": float(drift_ratio),
        "drift_ratio_trigger": _safe_float(signals.get("drift_ratio_trigger"), 0.0),
        "drift_detected": bool(signals.get("drift_detected")),
        "performance_drop": bool(signals.get("performance_drop")),
        "performance_reasons": sorted(set(performance_reasons)),
        "regime_shift": bool(signals.get("regime_shift")),
        "distribution_state": str(signals.get("distribution_state") or "UNKNOWN"),
        "degradation_score": float(degradation_score),
        "min_degradation": float(min_degradation),
        "runtime_signal": {
            "detected": bool(runtime_signal.get("detected")),
            "trade_count": _safe_int(runtime_signal.get("trade_count"), 0),
            "rolling_total_pnl": _safe_float(runtime_signal.get("rolling_total_pnl"), 0.0),
            "recent_total_pnl": _safe_float(runtime_signal.get("recent_total_pnl"), 0.0),
            "win_rate": runtime_signal.get("win_rate"),
            "reasons": list(runtime_signal.get("reasons") or []),
        },
        "shadow_signal": {
            "detected": bool(shadow_signal.get("detected")),
            "points": _safe_int(shadow_signal.get("points"), 0),
            "avg_dir_acc": shadow_signal.get("avg_dir_acc"),
            "avg_net_rmse": shadow_signal.get("avg_net_rmse"),
            "reasons": list(shadow_signal.get("reasons") or []),
        },
        "temporal_shadow_signal": {
            "detected": bool(temporal_signal.get("detected")),
            "failed_rows": _safe_int(temporal_signal.get("failed_rows"), 0),
            "reasons": list(temporal_signal.get("reasons") or []),
        },
        "trigger_types": list(trigger_types),
    }
    metrics["fingerprint"] = _fingerprint_payload(
        {
            "model_name": metrics["model_name"],
            "drift_ratio": round(float(drift_ratio), 6),
            "performance_reasons": metrics["performance_reasons"],
            "distribution_state": metrics["distribution_state"],
            "trigger_types": metrics["trigger_types"],
        }
    )
    return metrics


def _latest_queue_event_ts(events: Iterable[Dict[str, Any]]) -> int:
    for event in list(events or []):
        if str(event.get("action_taken") or "").strip() == "queue_training":
            return _safe_int(event.get("created_ts"), 0)
    return 0


def _cooldown_state(model_name: str, family: str, cooldown_s: int) -> Dict[str, Any]:
    if int(cooldown_s or 0) <= 0:
        return {"active": False, "anchor_ts": 0, "remaining_ms": 0}
    model_events = fetch_recent_drift_retrain_events(limit=25, model_name=str(model_name or "").strip())
    family_events = fetch_recent_drift_retrain_events(limit=25, family=str(family or "").strip())
    anchor_ts = max(_latest_queue_event_ts(model_events), _latest_queue_event_ts(family_events))
    remaining_ms = max(0, int(anchor_ts) + int(cooldown_s) * 1000 - _now_ms()) if anchor_ts > 0 else 0
    return {
        "active": bool(anchor_ts > 0 and remaining_ms > 0),
        "anchor_ts": int(anchor_ts),
        "remaining_ms": int(remaining_ms),
    }


def _count_open_retrain_runs() -> int:
    init_db()
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM model_lifecycle_runs
            WHERE triggered_by=? AND status IN ('queued','running')
            """,
            (str(TRIGGERED_BY),),
        ).fetchone()
        return _safe_int((row or [0])[0], 0)
    except Exception as e:
        _warn_nonfatal("DRIFT_RETRAIN_OPEN_RUN_COUNT_FAILED", e)
        return 0
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("DRIFT_RETRAIN_OPEN_RUN_CLOSE_FAILED", e)


def _promotion_requirements(cfg: Dict[str, Any], trigger_metrics: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source": TRIGGERED_BY,
        "require_cpcv": bool(cfg.get("require_cpcv")),
        "require_stat_gate": bool(cfg.get("require_stat_gate")),
        "config": {
            "enabled": bool(cfg.get("require_stat_gate")),
            "cpcv": {"enabled": bool(cfg.get("require_cpcv"))},
        },
        "trigger_types": list(trigger_metrics.get("trigger_types") or []),
        "trigger_fingerprint": str(trigger_metrics.get("fingerprint") or ""),
        "updated_ts_ms": _now_ms(),
    }


def _plan_has_training_prerequisites(plan: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
    dataset_used = dict(plan.get("dataset_used") or {})
    train_scope = dict(plan.get("train_scope") or {})
    if not dataset_used:
        dataset_used = dict(train_scope.get("dataset_used") or {})
    model_name = str(plan.get("model_name") or dataset_used.get("model_name") or "").strip()
    sources = dict(dataset_used.get("sources") or {})
    if model_name == "hmm_regime":
        prices = dict(sources.get("prices") or {})
        regime_vectors = dict(sources.get("regime_vectors") or {})
        symbols = [str(sym).upper().strip() for sym in list(train_scope.get("symbols") or []) if str(sym).strip()]
        price_row_count = _safe_int(prices.get("row_count"), 0)
        usable_rows = _safe_int(regime_vectors.get("usable_rows"), 0)
        required_rows = max(
            1,
            _safe_int(
                regime_vectors.get("required_min_rows"),
                _safe_int(train_scope.get("min_rows"), 0),
            ),
        )
        diagnostics = {
            "dataset_used": dataset_used,
            "price_row_count": int(price_row_count),
            "usable_rows": int(usable_rows),
            "required_min_rows": int(required_rows),
            "symbols": list(symbols),
            "latest_ts_ms": _safe_int(prices.get("latest_ts_ms"), 0),
        }
        ready = bool(
            max(int(price_row_count), int(usable_rows)) >= int(required_rows)
            and (str(prices.get("symbol") or "").strip() or symbols)
        )
        return ready, diagnostics
    labels = dict(sources.get("labels") or {})
    diagnostics = {
        "dataset_used": dataset_used,
        "labels_row_count": _safe_int(labels.get("row_count"), 0),
        "distinct_symbols": _safe_int(labels.get("distinct_symbols"), 0),
        "distinct_horizons": _safe_int(labels.get("distinct_horizons"), 0),
    }
    ready = bool(
        _safe_int(labels.get("row_count"), 0) > 0
        and _safe_int(labels.get("distinct_symbols"), 0) > 0
        and _safe_int(labels.get("distinct_horizons"), 0) > 0
    )
    return ready, diagnostics


def _publish_status(payload: Dict[str, Any]) -> None:
    try:
        meta_set(STATUS_META_KEY, _json_dumps(payload or {}))
    except Exception as e:
        _warn_nonfatal("DRIFT_RETRAIN_STATUS_META_SET_FAILED", e, payload_size=len(_json_dumps(payload or {})))


def _record_governance_snapshot(payload: Dict[str, Any]) -> None:
    try:
        from engine.strategy.model_governance_ext import record_governance_snapshot
    except Exception:
        return

    try:
        record_governance_snapshot(
            source=TRIGGERED_BY,
            regime="global",
            status=str(payload.get("status") or "evaluated"),
            summary={
                "enabled": bool(payload.get("enabled")),
                "triggered_models": list(payload.get("triggered_models") or []),
                "skipped_models": list(payload.get("skipped_models") or []),
                "open_retrain_runs": _safe_int(payload.get("open_retrain_runs"), 0),
            },
        )
    except Exception as e:
        _warn_nonfatal("DRIFT_RETRAIN_GOVERNANCE_SNAPSHOT_FAILED", e)


def _event_payload(
    *,
    model_name: str,
    family: str,
    trigger_metrics: Dict[str, Any],
    action_taken: str,
    outcome_status: str,
    diagnostics: Dict[str, Any],
    cooldown_applied: bool = False,
    candidate_version: str | None = None,
) -> Dict[str, Any]:
    event_id = record_drift_retrain_event(
        model_name=str(model_name or "").strip(),
        family=str(family or "").strip(),
        trigger_type=",".join(list(trigger_metrics.get("trigger_types") or [])) or "none",
        trigger_metrics=trigger_metrics,
        action_taken=str(action_taken or "").strip(),
        cooldown_applied=bool(cooldown_applied),
        candidate_version=candidate_version,
        outcome_status=str(outcome_status or "").strip(),
        diagnostics=dict(diagnostics or {}),
    )
    return {
        "event_id": int(event_id),
        "model_name": str(model_name or "").strip(),
        "family": str(family or "").strip(),
        "action_taken": str(action_taken or "").strip(),
        "outcome_status": str(outcome_status or "").strip(),
        "candidate_version": (str(candidate_version) if candidate_version is not None else None),
        "cooldown_applied": bool(cooldown_applied),
    }


def run_drift_retrain_job(model_names: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """Evaluate drift signals and emit retraining plans when thresholds trip."""
    init_db()
    started_at = _now_ms()
    cfg = drift_retrain_config_from_env()

    if not bool(cfg.get("enabled")):
        result = {
            "ok": True,
            "enabled": False,
            "status": "disabled",
            "ts_ms": int(started_at),
            "duration_ms": 0,
            "config": dict(cfg),
            "evaluated_models": [],
            "triggered_models": [],
            "skipped_models": [],
            "open_retrain_runs": _count_open_retrain_runs(),
        }
        _publish_status(result)
        _record_governance_snapshot(result)
        return result

    summary = dict(
        get_lifecycle_summary(
            model_names=list(model_names) if model_names is not None else None,
            limit=3,
        )
        or {}
    )
    family_rows = dict(summary.get("families") or {})

    evaluations: List[Dict[str, Any]] = []
    triggered_models: List[str] = []
    skipped_models: List[str] = []

    for model_name, row in family_rows.items():
        model_key = str(model_name or "").strip()
        family = _family_name(model_key)
        latest = dict((row or {}).get("latest") or {})
        signals = dict((row or {}).get("learning_signals") or {})
        trigger_metrics = _build_trigger_metrics(model_key, signals, cfg)
        diagnostics = {
            "learning_signals": signals,
            "latest_version": {
                "model_version": str(latest.get("model_version") or ""),
                "stage": str(latest.get("stage") or ""),
                "status": str(latest.get("status") or ""),
                "live_ready": bool(latest.get("live_ready")),
            },
        }

        if not list(trigger_metrics.get("trigger_types") or []):
            evaluations.append(
                {
                    **_event_payload(
                        model_name=model_key,
                        family=family,
                        trigger_metrics=trigger_metrics,
                        action_taken="noop",
                        outcome_status="insufficient_evidence",
                        diagnostics=diagnostics,
                    ),
                    "reason": "insufficient_evidence",
                }
            )
            skipped_models.append(model_key)
            continue

        cooldown = _cooldown_state(model_key, family, _safe_int(cfg.get("cooldown_s"), 0))
        diagnostics["cooldown"] = dict(cooldown)
        if bool(cooldown.get("active")):
            evaluations.append(
                {
                    **_event_payload(
                        model_name=model_key,
                        family=family,
                        trigger_metrics=trigger_metrics,
                        action_taken="defer_training",
                        outcome_status="cooldown",
                        diagnostics=diagnostics,
                        cooldown_applied=True,
                    ),
                    "reason": "cooldown",
                }
            )
            skipped_models.append(model_key)
            continue

        open_runs = _count_open_retrain_runs()
        diagnostics["open_retrain_runs"] = int(open_runs)
        diagnostics["max_parallel_jobs"] = _safe_int(cfg.get("max_parallel_jobs"), 1)
        if open_runs >= _safe_int(cfg.get("max_parallel_jobs"), 1):
            evaluations.append(
                {
                    **_event_payload(
                        model_name=model_key,
                        family=family,
                        trigger_metrics=trigger_metrics,
                        action_taken="defer_training",
                        outcome_status="max_parallel_jobs_reached",
                        diagnostics=diagnostics,
                    ),
                    "reason": "max_parallel_jobs_reached",
                }
            )
            skipped_models.append(model_key)
            continue

        plan = dict(create_training_plan(model_key) or {})
        diagnostics["training_plan"] = {
            "model_version": str(plan.get("model_version") or ""),
            "job_name": str(plan.get("job_name") or ""),
            "module_name": str(plan.get("module_name") or ""),
            "parent_version": plan.get("parent_version"),
        }
        if not plan:
            evaluations.append(
                {
                    **_event_payload(
                        model_name=model_key,
                        family=family,
                        trigger_metrics=trigger_metrics,
                        action_taken="unsupported_family",
                        outcome_status="unsupported_model_family",
                        diagnostics=diagnostics,
                    ),
                    "reason": "unsupported_model_family",
                }
            )
            skipped_models.append(model_key)
            continue

        prerequisites_ready, prereq_diagnostics = _plan_has_training_prerequisites(plan)
        diagnostics["prerequisites"] = dict(prereq_diagnostics)
        if not bool(prerequisites_ready):
            evaluations.append(
                {
                    **_event_payload(
                        model_name=model_key,
                        family=family,
                        trigger_metrics=trigger_metrics,
                        action_taken="block_training",
                        outcome_status="missing_training_prerequisites",
                        diagnostics=diagnostics,
                    ),
                    "reason": "missing_training_prerequisites",
                }
            )
            skipped_models.append(model_key)
            continue

        promotion_requirements = _promotion_requirements(cfg, trigger_metrics)
        trigger_payload = dict(plan.get("trigger") or {})
        trigger_payload.update(
            {
                "source": TRIGGERED_BY,
                "ts_ms": _now_ms(),
                "trigger_types": list(trigger_metrics.get("trigger_types") or []),
                "metrics": dict(trigger_metrics),
                "promotion_requirements": dict(promotion_requirements),
            }
        )
        train_scope = dict(plan.get("train_scope") or {})
        if plan.get("dataset_used") and "dataset_used" not in train_scope:
            train_scope["dataset_used"] = dict(plan.get("dataset_used") or {})
        train_scope["promotion_requirements"] = dict(promotion_requirements)
        train_scope["drift_retrain"] = {
            "source": TRIGGERED_BY,
            "trigger_types": list(trigger_metrics.get("trigger_types") or []),
            "trigger_fingerprint": str(trigger_metrics.get("fingerprint") or ""),
            "triggered_ts_ms": _now_ms(),
        }
        plan["trigger"] = trigger_payload
        plan["train_scope"] = train_scope
        plan["mutation_kind"] = str(plan.get("mutation_kind") or "drift_retrain")

        dispatch = dict(dispatch_training_plan(plan, triggered_by=TRIGGERED_BY) or {})
        diagnostics["dispatch"] = dict(dispatch)

        if bool(dispatch.get("skipped")) and str(dispatch.get("reason") or "") == "training_already_pending":
            evaluations.append(
                {
                    **_event_payload(
                        model_name=model_key,
                        family=family,
                        trigger_metrics=trigger_metrics,
                        action_taken="duplicate_suppressed",
                        outcome_status="training_already_pending",
                        diagnostics=diagnostics,
                        candidate_version=(str(dispatch.get("model_version")) if dispatch.get("model_version") else None),
                    ),
                    "reason": "training_already_pending",
                }
            )
            skipped_models.append(model_key)
            continue

        if not bool(dispatch.get("ok")):
            evaluations.append(
                {
                    **_event_payload(
                        model_name=model_key,
                        family=family,
                        trigger_metrics=trigger_metrics,
                        action_taken="queue_training",
                        outcome_status="dispatch_failed",
                        diagnostics=diagnostics,
                        candidate_version=(str(dispatch.get("model_version")) if dispatch.get("model_version") else None),
                    ),
                    "reason": "dispatch_failed",
                }
            )
            skipped_models.append(model_key)
            continue

        candidate_version = str(dispatch.get("model_version") or plan.get("model_version") or "").strip() or None
        evaluations.append(
            {
                **_event_payload(
                    model_name=model_key,
                    family=family,
                    trigger_metrics=trigger_metrics,
                    action_taken="queue_training",
                    outcome_status="dispatched",
                    diagnostics=diagnostics,
                    candidate_version=candidate_version,
                ),
                "reason": "dispatched",
            }
        )
        triggered_models.append(model_key)

    result = {
        "ok": True,
        "enabled": True,
        "status": "evaluated",
        "ts_ms": _now_ms(),
        "duration_ms": max(0, _now_ms() - started_at),
        "config": dict(cfg),
        "evaluated_models": evaluations,
        "triggered_models": triggered_models,
        "skipped_models": skipped_models,
        "open_retrain_runs": _count_open_retrain_runs(),
    }
    _publish_status(result)
    _record_governance_snapshot(result)
    return result


__all__ = [
    "STATUS_META_KEY",
    "TRIGGERED_BY",
    "drift_retrain_config_from_env",
    "run_drift_retrain_job",
]
