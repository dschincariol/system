"""Governance summary builders for promotion, lifecycle, and competition state.

This layer turns lower-level training, replay, self-critic, lifecycle, and
champion/challenger artifacts into one operator-facing governance summary
without introducing a separate control plane.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.runtime_meta import meta_get
from engine.runtime.storage import connect, fetch_recent_drift_retrain_events, init_db

LOG = get_logger("engine.strategy.model_governance_ext")
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
        component="engine.strategy.model_governance_ext",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal("MODEL_GOVERNANCE_EXT_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(value)[:120])
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal("MODEL_GOVERNANCE_EXT_SAFE_INT_FAILED", e, once_key="safe_int", value=repr(value)[:120])
        return int(default)


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
            return dict(obj) if isinstance(obj, dict) else {}
        except Exception as e:
            _warn_nonfatal("MODEL_GOVERNANCE_EXT_JSON_PARSE_FAILED", e, once_key="safe_json_dict", value=str(value)[:200])
            return {}
    return {}


def record_governance_snapshot(
    *,
    source: str,
    summary: Dict[str, Any],
    regime: str = "global",
    champion_name: str = "",
    challenger_name: str = "",
    status: str = "ok",
) -> Dict[str, Any]:
    init_db()
    ts_ms = _now_ms()
    con = connect()
    try:
        cur = con.execute(
            """
            INSERT INTO model_governance_log(
              ts_ms, source, regime, champion_name, challenger_name, status, summary_json
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                str(source or "unknown"),
                str(regime or "global"),
                (str(champion_name) if champion_name else None),
                (str(challenger_name) if challenger_name else None),
                str(status or "ok"),
                json.dumps(summary or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
        return {"ok": True, "id": int(cur.lastrowid or 0), "ts_ms": int(ts_ms)}
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "MODEL_GOVERNANCE_RECORD_CLOSE_FAILED",
                e,
                once_key="record_close",
                source=str(source or "unknown"),
                regime=str(regime or "global"),
            )


def build_governance_summary(limit_audit: int = 20) -> Dict[str, Any]:
    init_db()
    con = connect()
    try:
        promotion_status = {"enabled": True, "allowed": False, "updated_ts_ms": 0}
        try:
            from engine.api.api_governance import get_promotion_status

            promotion_status = dict(get_promotion_status() or promotion_status)
        except Exception as e:
            _warn_nonfatal(
                "MODEL_GOVERNANCE_PROMOTION_STATUS_FAILED",
                e,
                once_key="promotion_status",
            )

        replay_validation = _safe_json_dict(meta_get("competition_replay_validation", "") or "{}")
        replay_status = _safe_json_dict(meta_get("competition_replay_validation_status", "") or "{}")
        self_critic = _safe_json_dict(meta_get("competition_self_critic", "") or "{}")
        capital_plan = _safe_json_dict(meta_get("competition_capital_plan", "") or "{}")
        lifecycle_status = _safe_json_dict(meta_get("model_lifecycle_status", "") or "{}")
        drift_retrain_status = _safe_json_dict(meta_get("drift_retrain_status", "") or "{}")
        lifecycle_summary = {"ok": False, "families": {}}
        try:
            from engine.strategy.model_lifecycle import get_lifecycle_summary

            lifecycle_summary = dict(get_lifecycle_summary(limit=5) or lifecycle_summary)
        except Exception as e:
            _warn_nonfatal(
                "MODEL_GOVERNANCE_LIFECYCLE_SUMMARY_FAILED",
                e,
                once_key="lifecycle_summary",
            )

        champions = []
        try:
            rows = con.execute(
                """
                SELECT scope, symbol, horizon_s, model_name, challenger_name, regime, updated_ts_ms, meta_json
                FROM champion_assignments
                WHERE state='champion'
                ORDER BY updated_ts_ms DESC, symbol ASC, horizon_s ASC
                LIMIT 20
                """
            ).fetchall() or []
            for row in rows:
                champions.append(
                    {
                        "scope": str(row[0] or ""),
                        "symbol": str(row[1] or ""),
                        "horizon_s": _safe_int(row[2], 0),
                        "model_name": str(row[3] or ""),
                        "challenger_name": str(row[4] or ""),
                        "regime": str(row[5] or "global"),
                        "updated_ts_ms": _safe_int(row[6], 0),
                        "meta": _safe_json_dict(row[7]),
                    }
                )
        except Exception:
            champions = []

        challengers = []
        try:
            from engine.strategy.model_marketplace import top_challengers

            challengers = list(top_challengers(limit=10) or [])
        except Exception:
            challengers = []

        shadow_scores = []
        try:
            from engine.runtime.shadow_capital_allocator import get_shadow_capital_scores

            shadow_scores = list((get_shadow_capital_scores(limit=10, regime="global") or {}).get("rows") or [])
        except Exception:
            shadow_scores = []

        audit_rows = []
        try:
            rows = con.execute(
                """
                SELECT ts_ms, actor, action, model_name, from_model_kind, to_model_kind, reason_json, regime
                FROM model_promotion_audit
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(max(1, min(200, int(limit_audit or 20)))),),
            ).fetchall() or []
            for row in rows:
                audit_rows.append(
                    {
                        "ts_ms": _safe_int(row[0], 0),
                        "actor": str(row[1] or ""),
                        "action": str(row[2] or ""),
                        "model_name": str(row[3] or ""),
                        "from_model_kind": (str(row[4]) if row[4] is not None else None),
                        "to_model_kind": (str(row[5]) if row[5] is not None else None),
                        "reason": _safe_json_dict(row[6]),
                        "regime": str(row[7] or "global"),
                    }
                )
        except Exception:
            audit_rows = []

        logs = []
        try:
            rows = con.execute(
                """
                SELECT ts_ms, source, regime, champion_name, challenger_name, status, summary_json
                FROM model_governance_log
                ORDER BY ts_ms DESC, id DESC
                LIMIT 10
                """
            ).fetchall() or []
            for row in rows:
                logs.append(
                    {
                        "ts_ms": _safe_int(row[0], 0),
                        "source": str(row[1] or ""),
                        "regime": str(row[2] or "global"),
                        "champion_name": (str(row[3]) if row[3] is not None else None),
                        "challenger_name": (str(row[4]) if row[4] is not None else None),
                        "status": str(row[5] or ""),
                        "summary": _safe_json_dict(row[6]),
                    }
                )
        except Exception:
            logs = []

        drift_retrain_events = []
        try:
            drift_retrain_events = list(fetch_recent_drift_retrain_events(limit=10) or [])
        except Exception as e:
            _warn_nonfatal(
                "MODEL_GOVERNANCE_DRIFT_RETRAIN_EVENTS_FAILED",
                e,
                once_key="drift_retrain_events",
            )

        governance_alerts = []
        if not bool(replay_status.get("fresh", False)):
            governance_alerts.append("replay_stale")
        if bool(self_critic.get("blocked_keys")):
            governance_alerts.append("self_critic_blocks_present")
        if not bool(promotion_status.get("allowed")):
            governance_alerts.append("promotion_not_allowed")
        if bool(drift_retrain_status.get("enabled")) and bool(drift_retrain_status.get("triggered_models")):
            governance_alerts.append("drift_retrain_triggered")

        capital_allocations = dict(capital_plan.get("allocations") or {})
        concentration = 0.0
        for alloc in capital_allocations.values():
            concentration = max(concentration, _safe_float((alloc or {}).get("capital_multiplier"), 0.0))

        return {
            "ok": True,
            "ts_ms": _now_ms(),
            "promotion_status": promotion_status,
            "replay_status": replay_status,
            "self_critic": self_critic,
            "lifecycle_status": lifecycle_status,
            "drift_retrain_status": drift_retrain_status,
            "lifecycle_summary": lifecycle_summary,
            "governance_alerts": governance_alerts,
            "champions": champions,
            "challengers": challengers,
            "shadow_scores": shadow_scores,
            "capital_plan_summary": {
                "allocation_groups": int(len(capital_allocations)),
                "max_capital_multiplier": float(concentration),
            },
            "audit": audit_rows,
            "logs": logs,
            "drift_retrain_events": drift_retrain_events,
            "replay_models": int(len(dict(replay_validation.get("models") or {}))),
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "MODEL_GOVERNANCE_SUMMARY_CLOSE_FAILED",
                e,
                once_key="summary_close",
                limit_audit=int(limit_audit),
            )
