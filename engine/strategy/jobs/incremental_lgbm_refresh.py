"""Nightly incremental LightGBM refresh job.

The job continues persisted LightGBM boosters with ``init_model`` on a recent
window and evaluates the refreshed candidate against the unchanged champion on
latest matured labels.  PatchTST and other sequence families remain full-retrain
only; this module never silently overwrites a served artifact.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Mapping

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
    touch_job_lock,
)
from engine.strategy import champion_manager
from engine.strategy.models.lgbm_regressor import (
    DEFAULT_MODEL_KIND,
    FAMILY,
    LGBMRegressorModel,
    _load_training_rows,
    continue_lgbm_regressor,
    evaluate_lgbm_regressor,
    load_model_from_artifact,
    register_shadow_model,
)
from engine.strategy.promotion_audit import audit as promotion_audit


JOB_NAME = "incremental_lgbm_refresh"
LOG = get_logger("engine.strategy.jobs.incremental_lgbm_refresh")
_WARNED_NONFATAL_KEYS: set[str] = set()
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))


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
        component="engine.strategy.jobs.incremental_lgbm_refresh",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, type(default)):
        return value
    if value in (None, "", b"", bytearray()):
        return default
    try:
        raw = value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value)
        out = json.loads(raw)
    except Exception:
        return default
    return out if isinstance(out, type(default)) else default


def incremental_refresh_mode(value: str | None = None) -> str:
    raw = str(value if value is not None else os.environ.get("INCREMENTAL_REFRESH_MODE", "off") or "off").strip().lower()
    return raw if raw in {"off", "shadow", "live"} else "off"


def compare_incremental_refresh(
    stale_model: LGBMRegressorModel,
    X_refresh: Any,
    y_refresh: Any,
    X_eval: Any,
    y_eval: Any,
    *,
    num_boost_round: int = 25,
) -> dict[str, Any]:
    refreshed = continue_lgbm_regressor(
        stale_model,
        X_refresh,
        y_refresh,
        num_boost_round=max(1, int(num_boost_round or 25)),
    )
    stale_metrics = evaluate_lgbm_regressor(stale_model, X_eval, y_eval)
    refreshed_metrics = evaluate_lgbm_regressor(refreshed, X_eval, y_eval)
    improved = float(refreshed_metrics.get("rmse") or 0.0) < float(stale_metrics.get("rmse") or 0.0)
    return {
        "ok": True,
        "improved": bool(improved),
        "stale_metrics": dict(stale_metrics),
        "refreshed_metrics": dict(refreshed_metrics),
        "refreshed_model": refreshed,
    }


def route_incremental_refresh_result(
    refreshed_model: LGBMRegressorModel,
    *,
    mode: str | None = None,
    symbol: str = "*",
    horizon_s: int = 0,
    version: str | None = None,
    metrics: Mapping[str, Any] | None = None,
    register_artifact: bool = True,
    set_assignment_fn: Any = None,
    audit_fn: Any = None,
) -> dict[str, Any]:
    """Route a winning refresh through the existing governance path."""

    mode_key = incremental_refresh_mode(mode)
    if mode_key == "off":
        return {"ok": True, "mode": "off", "serving_updated": False, "registered": False}

    version_s = str(version or f"lgbm-incremental-{_now_ms()}")
    registered: dict[str, Any] = {}
    if bool(register_artifact):
        registered = register_shadow_model(
            refreshed_model,
            symbol=str(symbol or "*").upper(),
            version=version_s,
            family=FAMILY,
            model_kind=DEFAULT_MODEL_KIND,
            performance_metrics=dict(metrics or {}),
        )

    if mode_key == "shadow":
        return {
            "ok": True,
            "mode": "shadow",
            "version": version_s,
            "registered": bool(register_artifact),
            "serving_updated": False,
            "registration": dict(registered or {}),
        }

    assignment_fn = set_assignment_fn or champion_manager.set_champion_assignment
    audit_callback = audit_fn or promotion_audit
    assignment_meta = {
        "source": JOB_NAME,
        "version": version_s,
        "metrics": dict(metrics or {}),
        "incremental_refresh": True,
    }
    assignment_fn(
        scope="symbol",
        symbol=str(symbol or "*").upper(),
        horizon_s=int(horizon_s or 0),
        model_name=str(refreshed_model.model_name),
        state="challenger",
        meta=assignment_meta,
    )
    audit_recorded = True
    audit_error: str | None = None
    try:
        audit_callback(
            actor=JOB_NAME,
            action="block_incremental_lgbm_refresh",
            model_name=str(refreshed_model.model_name),
            to_kind=DEFAULT_MODEL_KIND,
            reason={
                **dict(assignment_meta),
                "blocked_reason": "incremental_refresh_requires_competition_promotion",
                "required_path": "competition_replay_validation_and_model_registry_promotion",
            },
            regime="global",
        )
    except Exception as e:
        audit_recorded = False
        audit_error = type(e).__name__
        _warn_nonfatal(
            "INCREMENTAL_LGBM_REFRESH_PROMOTION_AUDIT_FAILED",
            e,
            once_key=f"promotion_audit:{refreshed_model.model_name}:{version_s}",
            model_name=str(refreshed_model.model_name),
            version=str(version_s),
            symbol=str(symbol or "*").upper(),
            horizon_s=int(horizon_s or 0),
        )
    return {
        "ok": False,
        "error": (
            "incremental_refresh_requires_competition_promotion"
            if audit_recorded
            else "promotion_audit_failed"
        ),
        "mode": "live",
        "version": version_s,
        "registered": bool(register_artifact),
        "serving_updated": False,
        "audit_recorded": bool(audit_recorded),
        "audit_error": audit_error,
        "assignment": dict(assignment_meta),
        "registration": dict(registered or {}),
    }


def _active_lgbm_champions(con: Any) -> list[dict[str, Any]]:
    try:
        rows = con.execute(
            """
            SELECT ca.symbol, ca.horizon_s, ca.model_name,
                   mv.model_version, mv.meta_json, mv.updated_ts_ms
            FROM champion_assignments ca
            JOIN model_versions mv ON mv.model_name=ca.model_name
            WHERE lower(COALESCE(ca.state, ''))='champion'
              AND lower(COALESCE(mv.model_kind, ''))='lightgbm'
            ORDER BY ca.symbol, ca.horizon_s, ca.model_name, mv.updated_ts_ms DESC
            """
        ).fetchall()
    except Exception:
        return []
    seen: set[tuple[str, int, str]] = set()
    out: list[dict[str, Any]] = []
    for row in rows or []:
        key = (str(row[0] or "*").upper(), _safe_int(row[1], 0), str(row[2] or ""))
        if key in seen or not key[2]:
            continue
        seen.add(key)
        meta = _json_loads(row[4], {})
        out.append(
            {
                "symbol": key[0],
                "horizon_s": key[1],
                "model_name": key[2],
                "model_version": str(row[3] or ""),
                "meta": dict(meta or {}),
            }
        )
    return out


def run_incremental_lgbm_refresh(*, con: Any = None, now_ms: int | None = None) -> dict[str, Any]:
    mode = incremental_refresh_mode()
    ts_value = int(now_ms if now_ms is not None else _now_ms())
    if mode == "off":
        return {"ok": True, "enabled": False, "mode": "off", "ts_ms": int(ts_value), "refreshed": [], "skipped": []}

    own = con is None
    if own:
        init_db()
        con = connect()
    try:
        champions = _active_lgbm_champions(con)
        refreshed_rows: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        min_rows = max(20, _safe_int(os.environ.get("INCREMENTAL_REFRESH_MIN_ROWS"), 60))
        eval_rows = max(5, _safe_int(os.environ.get("INCREMENTAL_REFRESH_EVAL_ROWS"), 20))
        window_days = max(1, _safe_int(os.environ.get("INCREMENTAL_REFRESH_WINDOW_DAYS"), 60))
        num_boost_round = max(1, _safe_int(os.environ.get("INCREMENTAL_REFRESH_BOOST_ROUNDS"), 25))
        cutoff_ms = int(ts_value) - int(window_days) * 86_400_000

        for champion in champions:
            meta = dict(champion.get("meta") or {})
            alias = str(meta.get("artifact_alias") or meta.get("artifact_uri") or "").strip()
            sha256 = str(meta.get("artifact_sha256") or "").strip()
            if not alias and not sha256:
                skipped.append({**champion, "reason": "missing_artifact_reference"})
                continue
            try:
                stale = load_model_from_artifact(alias=alias, sha256=sha256)
            except Exception as exc:
                skipped.append({**champion, "reason": "artifact_load_failed", "error": type(exc).__name__})
                continue
            X_rows, y_rows = _load_training_rows(
                cutoff_ms=int(cutoff_ms),
                horizon_s=int(champion.get("horizon_s") or 0),
                symbols=[str(champion.get("symbol") or "*")],
                feature_ids=list(stale.feature_ids),
            )
            if len(y_rows) < min_rows or len(y_rows) <= eval_rows:
                skipped.append({**champion, "reason": "insufficient_recent_rows", "rows": len(y_rows)})
                continue
            split = max(1, len(y_rows) - int(eval_rows))
            result = compare_incremental_refresh(
                stale,
                X_rows[:split],
                y_rows[:split],
                X_rows[split:],
                y_rows[split:],
                num_boost_round=int(num_boost_round),
            )
            metrics = {
                "stale_metrics": dict(result.get("stale_metrics") or {}),
                "refreshed_metrics": dict(result.get("refreshed_metrics") or {}),
                "eval_rows": int(len(y_rows) - split),
                "refresh_rows": int(split),
                "mode": mode,
            }
            if not bool(result.get("improved")):
                skipped.append({**champion, "reason": "refresh_not_better", "metrics": metrics})
                continue
            routing = route_incremental_refresh_result(
                result["refreshed_model"],
                mode=mode,
                symbol=str(champion.get("symbol") or "*"),
                horizon_s=int(champion.get("horizon_s") or 0),
                version=f"lgbm-incremental-{int(ts_value)}",
                metrics=metrics,
                register_artifact=True,
            )
            refreshed_rows.append({**champion, "metrics": metrics, "routing": routing})
        if own:
            con.commit()
        return {
            "ok": True,
            "enabled": True,
            "mode": mode,
            "ts_ms": int(ts_value),
            "refreshed": refreshed_rows,
            "skipped": skipped,
        }
    finally:
        if own:
            con.close()


def main() -> int:
    result = run_incremental_lgbm_refresh()
    print(json.dumps(result, separators=(",", ":"), sort_keys=True, default=str))
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
            extra_json=json.dumps({"phase": "start"}, separators=(",", ":"), sort_keys=True),
        )
        rc = int(main() or 0)
        touch_job_lock(JOB_NAME, OWNER, PID)
        put_job_heartbeat(
            JOB_NAME,
            OWNER,
            PID,
            extra_json=json.dumps({"phase": "done", "rc": rc}, separators=(",", ":"), sort_keys=True),
        )
        raise SystemExit(rc)
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


__all__ = [
    "compare_incremental_refresh",
    "incremental_refresh_mode",
    "route_incremental_refresh_result",
    "run_incremental_lgbm_refresh",
]
