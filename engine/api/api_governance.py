"""
FILE: api_governance.py

HTTP/API handlers for governance endpoints.

This module keeps governance-specific control surfaces out of the main
dashboard server and route registration files. It exposes rollback, promotion
status, calibration, and governance-summary reads for operator and UI callers.
"""

import json
import math
import time

from engine.api.internal_access import db_connect
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.api.api_governance")
_WARNED_NONFATAL_KEYS: set[str] = set()
ROLLBACK_CONFIRM_TOKEN = "ROLLBACK_CHAMPION"
ROLLBACK_JUSTIFICATION_MIN_LEN = 12


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.api.api_governance",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _body_dict(body) -> dict:
    return dict(body) if isinstance(body, dict) else {}


def _rollback_justification_error(body) -> dict | None:
    payload = _body_dict(body)
    justification = str(payload.get("justification") or payload.get("reason") or "").strip()
    if len(justification) >= ROLLBACK_JUSTIFICATION_MIN_LEN:
        return None
    return {
        "ok": False,
        "error": "justification_required",
        "min_length": ROLLBACK_JUSTIFICATION_MIN_LEN,
        "http_status": 422,
    }


def _rollback_confirmation_error(body) -> dict | None:
    confirm = str(_body_dict(body).get("confirm") or "").strip()
    if confirm == ROLLBACK_CONFIRM_TOKEN:
        return None
    return {
        "ok": False,
        "error": "confirmation_required",
        "required_confirm": ROLLBACK_CONFIRM_TOKEN,
        "http_status": 422,
    }


def validate_rollback_request(body) -> dict | None:
    """Shared rollback safety validation used by dashboard and ops handlers."""

    return _rollback_confirmation_error(body) or _rollback_justification_error(body)


def _safe_json_dict(value) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


def _compact_model_record(row) -> dict | None:
    if not isinstance(row, dict):
        return None
    metrics = row.get("metrics") if isinstance(row.get("metrics"), dict) else {}
    perf = row.get("performance_metrics") if isinstance(row.get("performance_metrics"), dict) else {}
    return {
        "model_name": str(row.get("model_name") or ""),
        "model_kind": str(row.get("model_kind") or ""),
        "model_ts_ms": _safe_int(row.get("model_ts_ms"), 0),
        "stage": str(row.get("stage") or ""),
        "regime": str(row.get("regime") or "global"),
        "status": str(row.get("status") or row.get("stage") or ""),
        "created_ts_ms": _safe_int(row.get("created_ts_ms"), 0),
        "updated_ts_ms": _safe_int(row.get("updated_ts_ms") or row.get("created_ts_ms"), 0),
        "last_promotion_ts_ms": _safe_int(row.get("last_promotion_ts_ms"), 0),
        "metrics": dict(metrics),
        "performance_metrics": dict(perf),
    }


def _metrics_for(row) -> dict:
    if not isinstance(row, dict):
        return {}
    out = {}
    for key in ("metrics", "performance_metrics", "meta"):
        value = row.get(key)
        if isinstance(value, dict):
            out.update(value)
    for key, value in row.items():
        if key not in out and isinstance(value, (int, float, str)):
            out[key] = value
    return out


def _first_metric(metrics: dict, keys: tuple[str, ...]):
    for key in keys:
        value = _safe_float(metrics.get(key))
        if value is not None:
            return value
    return None


_COMPARISON_METRICS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("rmse", "RMSE", "lower", ("rmse", "root_mean_square_error", "validation_rmse")),
    ("mae", "MAE", "lower", ("mae", "mean_absolute_error")),
    ("directional_acc", "Directional accuracy", "higher", ("directional_acc", "direction_acc", "dir_acc", "directional_accuracy")),
    ("sharpe", "Sharpe", "higher", ("sharpe", "sharpe_ratio", "deflated_sharpe")),
    ("drawdown", "Drawdown", "lower", ("max_drawdown", "drawdown", "drawdown_contribution")),
    ("turnover", "Turnover", "lower", ("turnover", "avg_turnover", "portfolio_turnover")),
    ("correlation", "Correlation", "lower", ("correlation", "corr", "max_correlation", "portfolio_correlation")),
    ("exposure", "Exposure", "lower", ("exposure", "gross_exposure", "net_exposure", "model_exposure")),
    ("n_eval", "Eval observations", "higher", ("n_eval", "n", "sample_n", "trades")),
)


def _comparison_metrics(champion: dict | None, challenger: dict | None) -> list[dict]:
    ch_metrics = _metrics_for(champion or {})
    challenger_metrics = _metrics_for(challenger or {})
    out: list[dict] = []
    for key, label, direction, candidates in _COMPARISON_METRICS:
        champion_value = _first_metric(ch_metrics, candidates)
        challenger_value = _first_metric(challenger_metrics, candidates)
        delta = None
        if champion_value is not None and challenger_value is not None:
            delta = float(challenger_value - champion_value)
        out.append({
            "key": key,
            "label": label,
            "direction": direction,
            "champion": champion_value,
            "challenger": challenger_value,
            "delta": delta,
            "state": "available" if champion_value is not None or challenger_value is not None else "unavailable",
        })
    return out


def _stage_latest(rows: list[dict], stage: str) -> dict | None:
    matches = [row for row in rows if str((row or {}).get("stage") or "").lower() == str(stage).lower()]
    if not matches:
        return None
    matches.sort(
        key=lambda row: (
            _safe_int(row.get("updated_ts_ms") or row.get("created_ts_ms"), 0),
            _safe_int(row.get("created_ts_ms"), 0),
        ),
        reverse=True,
    )
    return matches[0]


def _check_state(blockers: set[str], blocker_key: str, available: bool = True) -> str:
    if not available:
        return "unavailable"
    return "fail" if blocker_key in blockers else "pass"


def _gate_checklist(status: dict, reason: dict, replay_status: dict, shadow_scores: list) -> list[dict]:
    raw_blockers = reason.get("blockers") or []
    if not isinstance(raw_blockers, list):
        raw_blockers = [raw_blockers]
    blockers = {str(item) for item in raw_blockers if str(item or "").strip()}
    enabled_known = ("promotion_enabled_db" in reason) or ("promotion_enabled_env" in reason) or ("enabled" in status)
    last_promo = _safe_int(reason.get("last_promo_ts_ms"), 0)
    cooldown_s = _safe_int(reason.get("cooldown_s"), 0)
    cooldown_remaining_s = None
    if last_promo > 0 and cooldown_s > 0:
        cooldown_remaining_s = max(0, int(cooldown_s - ((int(time.time() * 1000) - last_promo) / 1000.0)))

    replay_available = bool(replay_status)
    replay_pass = None
    if replay_available:
        if "fresh" in replay_status:
            replay_pass = bool(replay_status.get("fresh"))
        elif str(replay_status.get("status") or "").lower() in {"pass", "passed", "fresh", "ok", "approved"}:
            replay_pass = True
        elif str(replay_status.get("status") or "").lower() in {"fail", "failed", "stale", "blocked", "rejected"}:
            replay_pass = False

    return [
        {
            "key": "promotion_enabled",
            "label": "Promotion switch",
            "state": "pass" if bool(status.get("enabled")) else ("fail" if enabled_known else "unavailable"),
            "observed": {
                "enabled": status.get("enabled"),
                "promotion_enabled_env": reason.get("promotion_enabled_env"),
                "promotion_enabled_db": reason.get("promotion_enabled_db"),
            },
            "expected": "enabled in environment and database",
        },
        {
            "key": "promotion_allowed",
            "label": "Promotion guard",
            "state": "pass" if bool(status.get("allowed")) else "fail",
            "observed": {"allowed": status.get("allowed"), "blockers": sorted(blockers)},
            "expected": "no active guard blockers",
        },
        {
            "key": "cooldown",
            "label": "Cooldown",
            "state": _check_state(blockers, "cooldown", available=("cooldown_s" in reason or "last_promo_ts_ms" in reason)),
            "observed": {
                "last_promo_ts_ms": last_promo or None,
                "cooldown_s": cooldown_s or None,
                "remaining_s": cooldown_remaining_s,
            },
            "expected": "cooldown elapsed",
        },
        {
            "key": "crit_alerts",
            "label": "Critical alerts",
            "state": _check_state(blockers, "crit_alerts", available=("crit_alerts" in reason)),
            "observed": reason.get("crit_alerts"),
            "expected": "within promotion guard threshold",
        },
        {
            "key": "equity_drift",
            "label": "Equity drift",
            "state": _check_state(blockers, "equity_drift_crit", available=bool(reason.get("equity_drift_available", False))),
            "observed": {
                "available": reason.get("equity_drift_available"),
                "crit_points": reason.get("equity_drift_crit_points"),
            },
            "expected": "no critical equity drift points",
        },
        {
            "key": "model_drift",
            "label": "Model drift",
            "state": _check_state(blockers, "drift_ratio", available=("max_drift_ratio" in reason)),
            "observed": reason.get("max_drift_ratio"),
            "expected": "within configured drift-ratio threshold",
        },
        {
            "key": "realized_pnl",
            "label": "Realized PnL",
            "state": _check_state(blockers, "negative_real_pnl_models", available=("model_pnl_snapshot" in reason)),
            "observed": reason.get("model_pnl_snapshot"),
            "expected": "no negative real-PnL model blockers",
        },
        {
            "key": "statistical_gate",
            "label": "Statistical gate",
            "state": "unavailable",
            "observed": reason.get("statistical_gate"),
            "expected": "latest persisted statistical evidence must pass",
        },
        {
            "key": "cpcv_gate",
            "label": "CPCV gate",
            "state": "unavailable",
            "observed": reason.get("cpcv_gate"),
            "expected": "CPCV evidence must pass when configured",
        },
        {
            "key": "replay_validation",
            "label": "Replay validation",
            "state": ("pass" if replay_pass is True else "fail" if replay_pass is False else "unavailable"),
            "observed": replay_status or None,
            "expected": "fresh approved replay or no backend-provided status",
        },
        {
            "key": "shadow_validation",
            "label": "Shadow validation",
            "state": "pass" if shadow_scores else "unavailable",
            "observed": shadow_scores[:3] if shadow_scores else None,
            "expected": "shadow score rows available",
        },
    ]


def build_promotion_gate_data(
    *,
    status: dict | None,
    registry_rows: list | None,
    model_name: str = "embed_regressor",
    regime: str = "global",
    replay_status: dict | None = None,
    replay_validation: dict | None = None,
    shadow_scores: list | None = None,
) -> dict:
    """Compose operator-facing promotion gate data without changing promotion policy."""

    safe_status = dict(status or {})
    reason = _safe_json_dict(safe_status.get("reason"))
    rows = [
        row for row in (_compact_model_record(item) for item in list(registry_rows or []))
        if row and str(row.get("model_name") or "") == str(model_name)
    ]
    champion = _stage_latest(rows, "champion")
    challenger = _stage_latest(rows, "challenger")
    rollback_target = _stage_latest(rows, "retired")
    safe_replay_status = dict(replay_status or {})
    safe_shadow_scores = list(shadow_scores or [])

    cooldown_check = next(
        (item for item in _gate_checklist(safe_status, reason, safe_replay_status, safe_shadow_scores) if item["key"] == "cooldown"),
        {},
    )
    cooldown_observed = cooldown_check.get("observed") if isinstance(cooldown_check, dict) else {}

    return {
        "ok": True,
        "model_name": str(model_name),
        "regime": str(regime or "global"),
        "status": {
            "enabled": safe_status.get("enabled"),
            "allowed": safe_status.get("allowed"),
            "updated_ts_ms": _safe_int(safe_status.get("updated_ts_ms"), 0),
            "blockers": list(reason.get("blockers") or []) if isinstance(reason.get("blockers"), list) else [],
        },
        "champion": champion,
        "challenger": challenger,
        "rollback_target": rollback_target,
        "comparison_metrics": _comparison_metrics(champion, challenger),
        "checklist": _gate_checklist(safe_status, reason, safe_replay_status, safe_shadow_scores),
        "cooldown": {
            "available": bool(cooldown_observed),
            "state": str(cooldown_check.get("state") or "unavailable") if isinstance(cooldown_check, dict) else "unavailable",
            **(cooldown_observed if isinstance(cooldown_observed, dict) else {}),
        },
        "validation": {
            "replay_status": safe_replay_status,
            "replay_validation": dict(replay_validation or {}),
            "shadow_scores": safe_shadow_scores[:10],
        },
        "actions": {
            "rollback": {
                "available": bool(champion and rollback_target),
                "endpoint": "/api/champion/rollback",
                "method": "POST",
                "required_confirm": ROLLBACK_CONFIRM_TOKEN,
                "requires_justification": True,
                "preview": {
                    "action": "rollback",
                    "model_name": str(model_name),
                    "regime": str(regime or "global"),
                    "current_champion": champion,
                    "rollback_target": rollback_target,
                    "consequence": (
                        "Current champion will be retired and the latest retired model "
                        "will become champion for this model/regime."
                    ),
                },
            },
            "force_promote": {
                "available": False,
                "reason": "no_audit_safe_force_promotion_endpoint_registered",
            },
        },
        "source": "api_governance.get_promotion_explain",
    }


# --------------------------------------------------
# ROLLBACK
# --------------------------------------------------

def api_post_rollback(_parsed=None, _body=None, _ctx=None):
    validation_error = validate_rollback_request(_body)
    if validation_error:
        return validation_error
    payload = _body_dict(_body)
    justification = str(payload.get("justification") or payload.get("reason") or "").strip()

    try:
        # Manual rollback is intentionally explicit and audited; this endpoint
        # is an operator control surface, not an automated policy loop.
        from engine.model_registry import rollback_champion as _rb
        from engine.strategy.promotion_audit import audit as _audit
        from engine.model_registry import get_stage_latest as _get

        ch_before = None
        try:
            ch_before = _get("embed_regressor", "champion")
        except Exception as e:
            _warn_nonfatal(
                "API_GOVERNANCE_PRE_ROLLBACK_CHAMPION_READ_FAILED",
                e,
                once_key="api_governance_pre_rollback_champion_read",
            )

        ch_after = _rb("embed_regressor")
        if not ch_after:
            return {"ok": False, "error": "no retired model available"}

        _audit(
            actor="manual",
            action="rollback",
            model_name="embed_regressor",
            from_kind=(ch_before.get("model_kind") if ch_before else None),
            from_ts_ms=(ch_before.get("model_ts_ms") if ch_before else None),
            to_kind=ch_after.get("model_kind"),
            to_ts_ms=ch_after.get("model_ts_ms"),
            reason={
                "note": "dashboard rollback",
                "justification": justification,
                "confirmed": True,
                "source": str(payload.get("source") or "dashboard"),
                "preview": payload.get("preview") if isinstance(payload.get("preview"), dict) else {},
            },
        )

        return {"ok": True, "champion": ch_after, "audit_justification_recorded": True}

    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_ROLLBACK_FAILED",
            e,
            once_key="api_governance_rollback_failed",
        )
        return {"ok": False, "error": str(e)}


# --------------------------------------------------
# PROMOTION STATUS
# --------------------------------------------------

def get_promotion_status():
    # Promotion status merges the static enable flag with the live guard result
    # so callers can distinguish "disabled" from "currently disallowed."
    reason = {}
    try:
        from engine.strategy.promotion_guard import promotion_allowed
        guard_result = promotion_allowed()
        if isinstance(guard_result, tuple) and len(guard_result) >= 2:
            allowed = bool(guard_result[0])
            reason = dict(guard_result[1] or {}) if isinstance(guard_result[1], dict) else {}
        else:
            allowed = bool(guard_result)
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_PROMOTION_ALLOWED_FAILED",
            e,
            once_key="api_governance_promotion_allowed_failed",
        )
        allowed = False

    try:
        con = db_connect()
        row = con.execute(
            """
            SELECT value, updated_ts_ms
            FROM risk_state
            WHERE key='promotion_enabled'
            """
        ).fetchone()
        con.close()

        enabled = (str(row[0]) == "1") if row else True
        ts_ms = int(row[1]) if row else 0
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_PROMOTION_STATUS_READ_FAILED",
            e,
            once_key="api_governance_promotion_status_read_failed",
        )
        enabled = True
        ts_ms = 0

    return {
        "enabled": bool(enabled),
        "allowed": bool(allowed),
        "updated_ts_ms": int(ts_ms),
        "reason": reason,
    }


def get_promotion_explain():
    promotion_status = get_promotion_status()
    out = {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "promotion_status": promotion_status,
        "registry": {},
        "lifecycle": {},
        "audit": [],
        "gate": {},
    }

    try:
        from engine.model_registry import list_recent
        out["registry"]["embed_regressor"] = list_recent("embed_regressor", limit=50) or []
        out["registry"]["temporal_predictor"] = list_recent("temporal_predictor", limit=50) or []
        out["registry"]["regime_stats_v2"] = list_recent("regime_stats_v2", limit=50) or []
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_REGISTRY_READ_FAILED",
            e,
            once_key="api_governance_registry_read_failed",
        )
        out["registry"]["embed_regressor"] = []
        out["registry"]["temporal_predictor"] = []
        out["registry"]["regime_stats_v2"] = []

    try:
        from engine.strategy.model_lifecycle import get_lifecycle_summary

        out["lifecycle"] = get_lifecycle_summary(limit=6) or {}
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_LIFECYCLE_READ_FAILED",
            e,
            once_key="api_governance_lifecycle_read_failed",
        )
        out["lifecycle"] = {}

    try:
        con = db_connect()
        rows = con.execute(
                """
                SELECT ts_ms, model_name, key, decision, reason, detail_json
                FROM model_promotion_audit
                ORDER BY ts_ms DESC
                LIMIT 50
                """
            ).fetchall()
        con.close()

        for r in rows or []:
            out["audit"].append({
                "ts_ms": int(r[0] or 0),
                "model_name": str(r[1] or ""),
                "key": str(r[2] or ""),
                "decision": str(r[3] or ""),
                "reason": str(r[4] or ""),
                "detail_json": r[5],
            })
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_AUDIT_READ_FAILED",
            e,
            once_key="api_governance_audit_read",
        )

    replay_status = {}
    replay_validation = {}
    shadow_scores = []
    try:
        from engine.runtime.runtime_meta import meta_get

        replay_status = _safe_json_dict(meta_get("competition_replay_validation_status", "") or "{}")
        replay_validation = _safe_json_dict(meta_get("competition_replay_validation", "") or "{}")
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_REPLAY_META_READ_FAILED",
            e,
            once_key="api_governance_replay_meta_read",
        )

    try:
        from engine.runtime.shadow_capital_allocator import get_shadow_capital_scores

        shadow_scores = list((get_shadow_capital_scores(limit=10, regime="global") or {}).get("rows") or [])
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_SHADOW_SCORE_READ_FAILED",
            e,
            once_key="api_governance_shadow_score_read",
        )
        shadow_scores = []

    out["gate"] = build_promotion_gate_data(
        status=promotion_status,
        registry_rows=list(out["registry"].get("embed_regressor") or []),
        model_name="embed_regressor",
        regime="global",
        replay_status=replay_status,
        replay_validation=replay_validation,
        shadow_scores=shadow_scores,
    )

    return out


def get_governance_summary():
    try:
        from engine.strategy.model_governance_ext import build_governance_summary

        return build_governance_summary(limit_audit=20)
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_SUMMARY_FAILED",
            e,
            once_key="api_governance_summary_failed",
        )
        return {
            "ok": False,
            "error": str(e),
            "promotion_status": {},
            "replay_status": {},
            "governance_alerts": [],
            "champions": [],
            "challengers": [],
            "shadow_scores": [],
            "audit": [],
            "logs": [],
        }


# --------------------------------------------------
# EXECUTION CONFIDENCE CALIBRATION
# --------------------------------------------------

def api_get_exec_conf_calib(_parsed=None, _ctx=None):
    try:
        from engine.execution.exec_conf_calibration import get_latest_exec_conf_calib
        return get_latest_exec_conf_calib()
    except Exception as e:
        _warn_nonfatal(
            "API_GOVERNANCE_EXEC_CONF_CALIB_FAILED",
            e,
            once_key="api_governance_exec_conf_calib_failed",
        )
        return {"ok": False, "error": str(e)}
