"""Read-only governance evidence aggregation for operator surfaces.

The functions in this module compose persisted governance artifacts into a
single evidence contract. They never promote a model, allocate capital, or
change risk state; existing strategy/runtime gates remain authoritative.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
import os
import time
from typing import Any, Mapping, Sequence

from engine.api.internal_access import db_connect
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


LOG = get_logger("engine.api.governance_evidence")
_WARNED_NONFATAL_KEYS: set[str] = set()

DEFAULT_LABEL_STALE_MS = 30 * 24 * 60 * 60 * 1000
DEFAULT_ALPHA_SHRINKAGE_STALE_MS = 24 * 60 * 60 * 1000
DEFAULT_PRODUCTION_MONITORING_STALE_MS = 24 * 60 * 60 * 1000
DEFAULT_SHADOW_CAPITAL_STALE_MS = 24 * 60 * 60 * 1000
DEFAULT_EXPERIMENT_LEDGER_STALE_MS = 90 * 24 * 60 * 60 * 1000
DEFAULT_RISK_VAR_BACKTEST_STALE_MS = 14 * 24 * 60 * 60 * 1000

PASS_DECISIONS = frozenset({"pass", "passed", "accepted", "approved", "promote", "promoted"})
SENSITIVE_COMPONENT_KEYS = frozenset(
    {
        "account",
        "account_id",
        "broker_account",
        "broker_account_id",
        "credential",
        "credentials",
        "password",
        "secret",
        "token",
    }
)


@dataclass(frozen=True)
class EvidenceItem:
    key: str
    label: str
    state: str
    freshness: str
    sample_count: int
    last_update_ts_ms: int
    source_artifact: str
    remediation: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": str(self.key),
            "label": str(self.label),
            "state": _state_key(self.state),
            "freshness": str(self.freshness or "unknown"),
            "sample_count": int(max(0, self.sample_count)),
            "last_update_ts_ms": int(max(0, self.last_update_ts_ms)),
            "source_artifact": str(self.source_artifact),
            "remediation": str(self.remediation),
            "details": dict(self.details or {}),
        }


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
        component="engine.api.governance_evidence",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(str(name), "")
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if value in (None, "", b"", bytearray()):
        return []
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return []
    return list(parsed) if isinstance(parsed, list) else []


def _state_key(value: Any) -> str:
    key = str(value or "").strip().lower()
    if key in {"pass", "block", "unknown"}:
        return key
    if key in {"ok", "normal", "fresh", "allowed", "true"}:
        return "pass"
    if key in {"fail", "failed", "crit", "warn", "stale", "missing", "blocked", "false"}:
        return "block"
    return "unknown"


def _freshness(last_update_ts_ms: int, *, now_ms: int, stale_after_ms: int) -> str:
    ts = _safe_int(last_update_ts_ms, 0)
    if ts <= 0:
        return "missing"
    if stale_after_ms > 0 and int(now_ms) - ts > int(stale_after_ms):
        return "stale"
    return "fresh"


def _fresh_state(
    *,
    passed: bool | None,
    last_update_ts_ms: int,
    now_ms: int,
    stale_after_ms: int,
    missing_blocks: bool = True,
) -> tuple[str, str]:
    freshness = _freshness(last_update_ts_ms, now_ms=now_ms, stale_after_ms=stale_after_ms)
    if freshness in {"missing", "stale"} and missing_blocks:
        return "block", freshness
    if passed is True:
        return "pass", freshness
    if passed is False:
        return "block", freshness
    return "unknown", freshness


def _table_exists(con: Any, table_name: str) -> bool:
    name = str(table_name)
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (name,),
        ).fetchone()
        if row:
            return True
    except Exception:
        row = None
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name=?
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        if row:
            return True
    except Exception:
        row = None
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _columns(con: Any, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({str(table_name)})").fetchall() or []
        return {str(row[1] or "").strip() for row in rows if row and len(row) > 1}
    except Exception:
        return set()


def _runtime_meta(con: Any, key: str) -> str:
    if not _table_exists(con, "runtime_meta"):
        return ""
    try:
        row = con.execute("SELECT value FROM runtime_meta WHERE key=? LIMIT 1", (str(key),)).fetchone()
    except Exception:
        return ""
    return str(row[0] or "") if row else ""


def _source_artifact(table_name: str, row_id: Any = None) -> str:
    if row_id in (None, "", 0):
        return str(table_name)
    return f"{table_name}#{row_id}"


def _blocker_from_item(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "key": str(item.get("key") or ""),
        "label": str(item.get("label") or item.get("key") or ""),
        "state": _state_key(item.get("state")),
        "freshness": str(item.get("freshness") or "unknown"),
        "source_artifact": str(item.get("source_artifact") or ""),
        "remediation": str(item.get("remediation") or ""),
        "details": dict(item.get("details") or {}),
    }


def _latest_ope_evidence(con: Any, *, now_ms: int) -> EvidenceItem:
    stale_after_ms = _env_int("PROMOTION_OPE_LOOKBACK_MS", 90 * 24 * 60 * 60 * 1000)
    remediation = (
        "Run the policy OPE job for the exact challenger and persist a passing "
        "policy_ope_evidence row with sufficient support, effective sample size, "
        "propensities, outcomes, and confidence bounds."
    )
    if not _table_exists(con, "policy_ope_evidence"):
        return EvidenceItem(
            key="ope_gate",
            label="OPE gate",
            state="block",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="policy_ope_evidence",
            remediation=remediation,
            details={"reason": "table_missing"},
        )
    row = con.execute(
        """
        SELECT id, ts_ms, candidate_key, model_id, model_name, candidate_type,
               candidate_version, policy_value, standard_error, ci_lower,
               ci_upper, n_obs, effective_n, support, max_importance_weight,
               decision, reason, diagnostics_json
        FROM policy_ope_evidence
        ORDER BY ts_ms DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return EvidenceItem(
            key="ope_gate",
            label="OPE gate",
            state="block",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="policy_ope_evidence",
            remediation=remediation,
            details={"reason": "no_rows"},
        )
    ts_ms = _safe_int(row[1], 0)
    decision = str(row[15] or "").strip().lower()
    diagnostics = _json_dict(row[17])
    passed = decision == "pass" and bool(diagnostics.get("passed", True))
    state, freshness = _fresh_state(
        passed=passed,
        last_update_ts_ms=ts_ms,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
    )
    return EvidenceItem(
        key="ope_gate",
        label="OPE gate",
        state=state,
        freshness=freshness,
        sample_count=_safe_int(row[11], 0),
        last_update_ts_ms=ts_ms,
        source_artifact=_source_artifact("policy_ope_evidence", row[0]),
        remediation=remediation,
        details={
            "candidate_key": str(row[2] or ""),
            "model_id": str(row[3] or ""),
            "model_name": str(row[4] or ""),
            "candidate_type": str(row[5] or ""),
            "candidate_version": str(row[6] or ""),
            "policy_value": _safe_float(row[7]),
            "standard_error": _safe_float(row[8]),
            "ci_lower": _safe_float(row[9]),
            "ci_upper": _safe_float(row[10]),
            "effective_n": _safe_float(row[12]),
            "support": _safe_float(row[13]),
            "max_importance_weight": _safe_float(row[14]),
            "decision": decision,
            "reason": str(row[16] or ""),
            "blockers": list(diagnostics.get("blockers") or []),
        },
    )


def _ledger_row_blockers(row: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    decision = str(row.get("promotion_decision") or "").strip().lower()
    if decision not in PASS_DECISIONS:
        blockers.append("ledger_decision_not_passing")
    trial_budget = _safe_int(row.get("trial_budget"), 0)
    trial_count = _safe_int(row.get("trial_count"), 0)
    if trial_budget <= 0:
        blockers.append("trial_budget_missing")
    if trial_count <= 0:
        blockers.append("trial_count_missing")
    if trial_budget > 0 and trial_count > trial_budget:
        blockers.append("trial_budget_exceeded")
    if not any(bool(row.get(key) or {}) for key in ("evidence_json", "cpcv_json", "fdr_json")):
        blockers.append("ledger_evidence_missing")
    if not row.get("redundancy_json"):
        blockers.append("redundancy_check_missing")
    return list(dict.fromkeys(blockers))


def _experiment_ledger_rows(con: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    if not _table_exists(con, "experiment_ledger"):
        return []
    rows = con.execute(
        """
        SELECT id, ts, candidate_key, candidate_name, candidate_version,
               candidate_type, source, parent_candidate_key, model_name,
               model_family, feature_ids_json, prompt_hash, model_hash,
               search_space_json, trial_budget, trial_count, cpcv_json, pbo,
               dsr, fdr_json, redundancy_json, evidence_json,
               promotion_decision, status, diagnostics_json
        FROM experiment_ledger
        ORDER BY ts DESC, id DESC
        LIMIT ?
        """,
        (max(1, min(250, int(limit or 20))),),
    ).fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = {
            "id": _safe_int(row[0], 0),
            "ts": _safe_int(row[1], 0),
            "candidate_key": str(row[2] or ""),
            "candidate_name": str(row[3] or ""),
            "candidate_version": str(row[4] or ""),
            "candidate_type": str(row[5] or ""),
            "source": str(row[6] or ""),
            "parent_candidate_key": str(row[7] or ""),
            "model_name": str(row[8] or ""),
            "model_family": str(row[9] or ""),
            "feature_ids": _json_list(row[10]),
            "prompt_hash": str(row[11] or ""),
            "model_hash": str(row[12] or ""),
            "search_space": _json_dict(row[13]),
            "trial_budget": _safe_int(row[14], 0),
            "trial_count": _safe_int(row[15], 0),
            "cpcv_json": _json_dict(row[16]),
            "pbo": _safe_float(row[17]),
            "dsr": _safe_float(row[18]),
            "fdr_json": _json_dict(row[19]),
            "redundancy_json": _json_dict(row[20]),
            "evidence_json": _json_dict(row[21]),
            "promotion_decision": str(row[22] or ""),
            "status": str(row[23] or ""),
            "diagnostics_json": _json_dict(row[24]),
        }
        blockers = _ledger_row_blockers(payload)
        payload["blockers"] = blockers
        payload["state"] = "pass" if not blockers else "block"
        payload["source_artifact"] = _source_artifact("experiment_ledger", payload["id"])
        payload["remediation"] = (
            "Record a passing experiment_ledger row for the exact candidate/version "
            "with feature lineage, trial budget/count, statistical evidence, FDR, "
            "CPCV/PBO/DSR, and redundancy checks."
        )
        out.append(payload)
    return out


def _experiment_ledger_evidence(con: Any, *, now_ms: int, limit: int) -> tuple[EvidenceItem, list[dict[str, Any]]]:
    stale_after_ms = _env_int("GOVERNANCE_EXPERIMENT_LEDGER_STALE_MS", DEFAULT_EXPERIMENT_LEDGER_STALE_MS)
    remediation = (
        "Record a passing experiment_ledger row for each generated candidate before "
        "trusting it for promotion."
    )
    if not _table_exists(con, "experiment_ledger"):
        return (
            EvidenceItem(
                key="experiment_ledger",
                label="Generated-candidate ledger",
                state="block",
                freshness="missing",
                sample_count=0,
                last_update_ts_ms=0,
                source_artifact="experiment_ledger",
                remediation=remediation,
                details={"reason": "table_missing"},
            ),
            [],
        )
    rows = _experiment_ledger_rows(con, limit=limit)
    latest_ts = max((_safe_int(row.get("ts"), 0) for row in rows), default=0)
    failing = [row for row in rows if row.get("state") == "block"]
    state, freshness = _fresh_state(
        passed=bool(rows) and not failing,
        last_update_ts_ms=latest_ts,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
    )
    if not rows:
        state, freshness = "block", "missing"
    return (
        EvidenceItem(
            key="experiment_ledger",
            label="Generated-candidate ledger",
            state=state,
            freshness=freshness,
            sample_count=len(rows),
            last_update_ts_ms=latest_ts,
            source_artifact="experiment_ledger",
            remediation=remediation,
            details={
                "failing_candidates": len(failing),
                "latest_candidate_key": str(rows[0].get("candidate_key") if rows else ""),
                "latest_decision": str(rows[0].get("promotion_decision") if rows else ""),
            },
        ),
        rows,
    )


def _net_after_cost_evidence(con: Any, *, now_ms: int) -> EvidenceItem:
    stale_after_ms = _env_int("GOVERNANCE_LABEL_STALE_MS", DEFAULT_LABEL_STALE_MS)
    remediation = (
        "Run label materialization until net_after_cost_labels contains realized "
        "net-return rows for the active promotion candidates."
    )
    if not _table_exists(con, "net_after_cost_labels"):
        return EvidenceItem(
            key="net_after_cost_labels",
            label="Net-after-cost labels",
            state="block",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="net_after_cost_labels",
            remediation=remediation,
            details={"reason": "table_missing"},
        )
    row = con.execute(
        """
        SELECT
          COUNT(1),
          COALESCE(SUM(CASE WHEN COALESCE(realized, 0) <> 0 THEN 1 ELSE 0 END), 0),
          MAX(COALESCE(computed_at_ts_ms, label_ts_ms, 0)),
          COUNT(DISTINCT COALESCE(NULLIF(model_name, ''), model_id, model_family, 'unknown')),
          COUNT(DISTINCT symbol)
        FROM net_after_cost_labels
        """
    ).fetchone()
    total_count = _safe_int(row[0] if row else 0, 0)
    realized_count = _safe_int(row[1] if row else 0, 0)
    latest_ts = _safe_int(row[2] if row else 0, 0)
    state, freshness = _fresh_state(
        passed=realized_count > 0,
        last_update_ts_ms=latest_ts,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
    )
    return EvidenceItem(
        key="net_after_cost_labels",
        label="Net-after-cost labels",
        state=state,
        freshness=freshness,
        sample_count=realized_count,
        last_update_ts_ms=latest_ts,
        source_artifact="net_after_cost_labels",
        remediation=remediation,
        details={
            "total_rows": total_count,
            "realized_rows": realized_count,
            "model_count": _safe_int(row[3] if row else 0, 0),
            "symbol_count": _safe_int(row[4] if row else 0, 0),
        },
    )


def _learned_alpha_evidence(con: Any, *, now_ms: int) -> EvidenceItem:
    stale_after_ms = _env_int("LEARNED_ALPHA_MAX_LOOKUP_AGE_MS", 7 * 24 * 60 * 60 * 1000)
    remediation = (
        "Run train_learned_alpha_decay so learned_alpha_decay_runs and "
        "learned_alpha_decay_estimates contain a fresh run with cohort estimates."
    )
    if not _table_exists(con, "learned_alpha_decay_runs"):
        return EvidenceItem(
            key="learned_alpha_decay",
            label="Learned alpha decay",
            state="block",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="learned_alpha_decay_runs",
            remediation=remediation,
            details={"reason": "table_missing"},
        )
    row = con.execute(
        """
        SELECT id, ts_ms, lookback_days, min_samples, metrics_json
        FROM learned_alpha_decay_runs
        ORDER BY ts_ms DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return EvidenceItem(
            key="learned_alpha_decay",
            label="Learned alpha decay",
            state="block",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="learned_alpha_decay_runs",
            remediation=remediation,
            details={"reason": "no_runs"},
        )
    run_id = _safe_int(row[0], 0)
    latest_ts = _safe_int(row[1], 0)
    estimates_count = 0
    if _table_exists(con, "learned_alpha_decay_estimates"):
        try:
            count_row = con.execute(
                "SELECT COUNT(1) FROM learned_alpha_decay_estimates WHERE run_id=?",
                (run_id,),
            ).fetchone()
            estimates_count = _safe_int(count_row[0] if count_row else 0, 0)
        except Exception:
            estimates_count = 0
    state, freshness = _fresh_state(
        passed=estimates_count > 0,
        last_update_ts_ms=latest_ts,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
    )
    return EvidenceItem(
        key="learned_alpha_decay",
        label="Learned alpha decay",
        state=state,
        freshness=freshness,
        sample_count=estimates_count,
        last_update_ts_ms=latest_ts,
        source_artifact=_source_artifact("learned_alpha_decay_runs", run_id),
        remediation=remediation,
        details={
            "run_id": run_id,
            "lookback_days": _safe_int(row[2], 0),
            "min_samples": _safe_int(row[3], 0),
            "metrics": _json_dict(row[4]),
        },
    )


def _production_like() -> bool:
    mode = str(os.environ.get("ENGINE_MODE") or os.environ.get("APP_ENV") or "").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE") or "").strip().lower()
    return mode in {"live", "prod", "production"} or execution_mode == "live"


def _alpha_shrinkage_evidence(con: Any, *, now_ms: int) -> EvidenceItem:
    stale_after_ms = _env_int("GOVERNANCE_ALPHA_SHRINKAGE_STALE_MS", DEFAULT_ALPHA_SHRINKAGE_STALE_MS)
    remediation = (
        "Run portfolio rebalance with alpha shrinkage enabled so runtime_meta."
        "last_alpha_shrinkage records applied shrinkage diagnostics."
    )
    try:
        from engine.strategy.alpha_shrinkage import config_from_env

        cfg = config_from_env()
        enabled = bool(cfg.enabled)
        cfg_payload = {
            "enabled": enabled,
            "prior_strength": float(cfg.prior_strength),
            "missing_prior_strength": float(cfg.missing_prior_strength),
            "min_prior_observations": float(cfg.min_prior_observations),
            "prior_levels": list(cfg.prior_levels),
        }
    except Exception as exc:
        _warn_nonfatal("GOVERNANCE_EVIDENCE_ALPHA_SHRINKAGE_CONFIG_FAILED", exc, once_key="alpha_shrinkage_config")
        enabled = False
        cfg_payload = {"enabled": False, "error": str(exc)}

    meta = _json_dict(_runtime_meta(con, "last_alpha_shrinkage"))
    applied = bool(meta.get("applied"))
    latest_ts = _safe_int(meta.get("ts_ms") or meta.get("run_ts_ms") or meta.get("updated_ts_ms"), 0)
    if latest_ts <= 0 and meta:
        latest_ts = now_ms
    if not enabled and not _production_like():
        state, freshness = "unknown", "unknown"
    else:
        state, freshness = _fresh_state(
            passed=enabled and applied,
            last_update_ts_ms=latest_ts,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
        )
    return EvidenceItem(
        key="alpha_shrinkage",
        label="Alpha shrinkage",
        state=state,
        freshness=freshness,
        sample_count=_safe_int(meta.get("observations"), 0),
        last_update_ts_ms=latest_ts,
        source_artifact="runtime_meta.last_alpha_shrinkage",
        remediation=remediation,
        details={
            "config": cfg_payload,
            "metadata": meta,
            "production_like": _production_like(),
        },
    )


def _production_monitoring_evidence(*, now_ms: int) -> tuple[EvidenceItem, EvidenceItem, dict[str, Any]]:
    stale_after_ms = _env_int("GOVERNANCE_PRODUCTION_MONITORING_STALE_MS", DEFAULT_PRODUCTION_MONITORING_STALE_MS)
    remediation = (
        "Run compute_drift/production monitoring until production_monitoring_metrics "
        "has fresh drift, calibration, conformal, shadow-live, and net-PnL rows."
    )
    try:
        from engine.strategy.production_monitoring import get_latest_production_monitoring_snapshot

        snapshot = get_latest_production_monitoring_snapshot(limit=80) or {}
    except Exception as exc:
        _warn_nonfatal("GOVERNANCE_EVIDENCE_PRODUCTION_MONITORING_FAILED", exc, once_key="production_monitoring")
        snapshot = {"ok": False, "error": str(exc), "metrics": [], "updated_ts_ms": 0}
    metrics = [dict(row) for row in list(snapshot.get("metrics") or []) if isinstance(row, Mapping)]
    status = dict(snapshot.get("status") or {})
    latest_ts = _safe_int(snapshot.get("updated_ts_ms") or status.get("latest_ts_ms"), 0)
    active_breach = bool(status.get("active"))
    state, freshness = _fresh_state(
        passed=bool(metrics) and not active_breach,
        last_update_ts_ms=latest_ts,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
    )
    monitoring_item = EvidenceItem(
        key="production_monitoring",
        label="Production monitoring",
        state=state,
        freshness=freshness,
        sample_count=len(metrics),
        last_update_ts_ms=latest_ts,
        source_artifact="production_monitoring_metrics",
        remediation=remediation,
        details={
            "status": status,
            "alert_count": _safe_int(status.get("alert_count"), 0),
            "signals": list(snapshot.get("signals") or [])[:10],
        },
    )

    shadow_metric = next((row for row in metrics if str(row.get("metric_name") or "") == "shadow_live_disagreement"), {})
    shadow_ts = _safe_int(shadow_metric.get("ts_ms"), latest_ts)
    shadow_state_text = str(shadow_metric.get("state") or "").strip().lower()
    shadow_ok = bool(shadow_metric) and shadow_state_text in {"ok", "normal"}
    shadow_state, shadow_freshness = _fresh_state(
        passed=shadow_ok,
        last_update_ts_ms=shadow_ts,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
    )
    shadow_live_item = EvidenceItem(
        key="shadow_live_monitoring",
        label="Shadow-vs-live monitoring",
        state=shadow_state,
        freshness=shadow_freshness,
        sample_count=_safe_int(shadow_metric.get("sample_n"), 0),
        last_update_ts_ms=shadow_ts,
        source_artifact="production_monitoring_metrics.shadow_live_disagreement",
        remediation=(
            "Run production monitoring with comparable live and shadow predictions "
            "until shadow_live_disagreement has enough samples and no threshold breach."
        ),
        details={"metric": shadow_metric or {"reason": "metric_missing"}},
    )
    return monitoring_item, shadow_live_item, snapshot


def _sanitize_components(components: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in dict(components or {}).items():
        text = str(key or "").strip()
        lowered = text.lower()
        if any(token in lowered for token in SENSITIVE_COMPONENT_KEYS):
            continue
        if isinstance(value, (int, float, str, bool)) or value is None:
            out[text] = value
    return out


def _shadow_capital_rows(con: Any, *, limit: int, regime: str) -> list[dict[str, Any]]:
    if not _table_exists(con, "shadow_capital_scores"):
        return []
    cols = _columns(con, "shadow_capital_scores")
    model_kind_expr = "model_kind" if "model_kind" in cols else "NULL"
    model_ts_expr = "model_ts_ms" if "model_ts_ms" in cols else "NULL"
    latency_mean_expr = "execution_latency_ms_mean" if "execution_latency_ms_mean" in cols else "NULL"
    latency_std_expr = "execution_latency_ms_std" if "execution_latency_ms_std" in cols else "NULL"
    rows = con.execute(
        """
        SELECT ts_ms, window_s, regime, model_name, model_kind, model_ts_ms, n,
               rmse, dir_acc, net_rmse, slippage_bps_mean, slippage_bps_std,
               execution_latency_ms_mean, execution_latency_ms_std,
               drawdown_proxy, cap_eff, realized_pnl, unrealized_pnl,
               total_pnl, score, components_json
        FROM shadow_capital_scores
        WHERE regime=?
        ORDER BY score DESC, ts_ms DESC
        LIMIT ?
        """.replace("model_kind", model_kind_expr, 1)
        .replace("model_ts_ms", model_ts_expr, 1)
        .replace("execution_latency_ms_mean", latency_mean_expr, 1)
        .replace("execution_latency_ms_std", latency_std_expr, 1),
        (str(regime), max(1, min(500, int(limit or 50)))),
    ).fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "ts_ms": _safe_int(row[0], 0),
                "window_s": _safe_int(row[1], 0),
                "regime": str(row[2] or "global"),
                "model_name": str(row[3] or ""),
                "model_kind": str(row[4] or ""),
                "model_ts_ms": _safe_int(row[5], 0),
                "n": _safe_int(row[6], 0),
                "rmse": _safe_float(row[7]),
                "dir_acc": _safe_float(row[8]),
                "net_rmse": _safe_float(row[9]),
                "slippage_bps_mean": _safe_float(row[10]),
                "slippage_bps_std": _safe_float(row[11]),
                "execution_latency_ms_mean": _safe_float(row[12]),
                "execution_latency_ms_std": _safe_float(row[13]),
                "drawdown_proxy": _safe_float(row[14]),
                "cap_eff": _safe_float(row[15]),
                "realized_pnl": _safe_float(row[16]),
                "unrealized_pnl": _safe_float(row[17]),
                "total_pnl": _safe_float(row[18]),
                "score": _safe_float(row[19]),
                "components": _sanitize_components(_json_dict(row[20])),
                "source_artifact": "shadow_capital_scores",
            }
        )
    return out


def _shadow_capital_evidence(
    con: Any,
    *,
    now_ms: int,
    limit: int,
    regime: str,
) -> tuple[EvidenceItem, dict[str, Any]]:
    remediation = (
        "Run the shadow capital scoring job after marketplace score refresh so "
        "shadow_capital_scores has fresh, sample-backed model scores."
    )
    if not _table_exists(con, "shadow_capital_scores"):
        return (
            EvidenceItem(
                key="shadow_capital_scores",
                label="Shadow capital scores",
                state="block",
                freshness="missing",
                sample_count=0,
                last_update_ts_ms=0,
                source_artifact="shadow_capital_scores",
                remediation=remediation,
                details={"reason": "table_missing"},
            ),
            _shadow_capital_payload([], regime=regime, evidence=None),
        )
    rows = _shadow_capital_rows(con, limit=limit, regime=regime)
    latest_ts = max((_safe_int(row.get("ts_ms"), 0) for row in rows), default=0)
    max_window_s = max((_safe_int(row.get("window_s"), 0) for row in rows), default=0)
    stale_after_ms = max(
        _env_int("GOVERNANCE_SHADOW_CAPITAL_STALE_MS", DEFAULT_SHADOW_CAPITAL_STALE_MS),
        int(max_window_s * 2 * 1000) if max_window_s > 0 else 0,
    )
    state, freshness = _fresh_state(
        passed=bool(rows) and any(_safe_int(row.get("n"), 0) > 0 for row in rows),
        last_update_ts_ms=latest_ts,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
    )
    sample_count = int(sum(max(0, _safe_int(row.get("n"), 0)) for row in rows))
    item = EvidenceItem(
        key="shadow_capital_scores",
        label="Shadow capital scores",
        state=state,
        freshness=freshness,
        sample_count=sample_count,
        last_update_ts_ms=latest_ts,
        source_artifact="shadow_capital_scores",
        remediation=remediation,
        details={
            "regime": str(regime),
            "row_count": len(rows),
            "top_model": str(rows[0].get("model_name") if rows else ""),
            "top_score": rows[0].get("score") if rows else None,
        },
    )
    return item, _shadow_capital_payload(rows, regime=regime, evidence=item)


def _shadow_capital_payload(
    rows: Sequence[Mapping[str, Any]],
    *,
    regime: str,
    evidence: EvidenceItem | None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "regime": str(regime),
        "rows": [dict(row) for row in rows],
        "masking": {
            "applied": True,
            "policy": "score_fields_only",
            "removed_component_keys": sorted(SENSITIVE_COMPONENT_KEYS),
        },
        "evidence": evidence.to_dict() if evidence else None,
        "authority": "read_only_governance_evidence",
    }


def _promotion_status() -> dict[str, Any]:
    reason: dict[str, Any] = {}
    try:
        from engine.strategy.promotion_guard import promotion_allowed

        guard_result = promotion_allowed()
        if isinstance(guard_result, tuple) and len(guard_result) >= 2:
            allowed = bool(guard_result[0])
            reason = dict(guard_result[1] or {}) if isinstance(guard_result[1], dict) else {}
        else:
            allowed = bool(guard_result)
    except Exception as exc:
        _warn_nonfatal("GOVERNANCE_EVIDENCE_PROMOTION_GUARD_FAILED", exc, once_key="promotion_guard")
        allowed = False
        reason = {"blockers": ["promotion_guard_error"], "detail": str(exc)}
    return {
        "allowed": bool(allowed),
        "reason": reason,
        "blockers": list(reason.get("blockers") or []) if isinstance(reason.get("blockers"), list) else [],
    }


def _promotion_gate_evidence(con: Any, *, now_ms: int) -> EvidenceItem:
    status = _promotion_status()
    updated_ts_ms = 0
    enabled = True
    if _table_exists(con, "risk_state"):
        try:
            row = con.execute(
                "SELECT value, updated_ts_ms FROM risk_state WHERE key='promotion_enabled' LIMIT 1"
            ).fetchone()
            if row:
                enabled = str(row[0] or "") == "1"
                updated_ts_ms = _safe_int(row[1], 0)
        except Exception:
            updated_ts_ms = 0
    blockers = list(status.get("blockers") or [])
    state = "pass" if enabled and status.get("allowed") is True else "block"
    freshness = "fresh" if updated_ts_ms > 0 else "unknown"
    return EvidenceItem(
        key="promotion_guard",
        label="Promotion guard",
        state=state,
        freshness=freshness,
        sample_count=0,
        last_update_ts_ms=updated_ts_ms,
        source_artifact="promotion_guard+risk_state.promotion_enabled",
        remediation="Clear the exact promotion_guard blockers; do not bypass the backend promotion gate.",
        details={
            "enabled": bool(enabled),
            "allowed": bool(status.get("allowed")),
            "blockers": blockers,
            "reason": dict(status.get("reason") or {}),
            "observed_ts_ms": int(now_ms),
        },
    )


def _overall_state(items: Sequence[Mapping[str, Any]]) -> str:
    states = [_state_key(item.get("state")) for item in items]
    if "block" in states:
        return "block"
    if states and all(state == "pass" for state in states):
        return "pass"
    return "unknown"


def _risk_var_backtest_evidence(con: Any, *, now_ms: int, limit: int) -> EvidenceItem:
    stale_after_ms = _env_int("RISK_VAR_BACKTEST_EVIDENCE_STALE_MS", DEFAULT_RISK_VAR_BACKTEST_STALE_MS)
    if not _table_exists(con, "risk_var_backtest_results"):
        return EvidenceItem(
            key="risk_var_backtesting",
            label="VaR/CVaR Backtesting",
            state="unknown",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="risk_var_backtest_results",
            remediation="Run migrations and schedule risk_var_backtest after Monte Carlo forecasts and realized equity history exist.",
            details={"source_route": "/api/risk/var_backtest", "reason": "table_missing"},
        )

    try:
        rows = con.execute(
            """
            SELECT forecast_ts_ms, confidence_level, exception, kupiec_pof_status,
                   christoffersen_ind_status, rolling_exception_rate, traffic_light_status,
                   traffic_light_reason, created_ts_ms
            FROM risk_var_backtest_results
            ORDER BY forecast_ts_ms DESC, confidence_level DESC, id DESC
            LIMIT ?
            """,
            (max(1, min(500, int(limit or 20))),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal("GOVERNANCE_EVIDENCE_RISK_VAR_BACKTEST_FAILED", exc, once_key="risk_var_backtest")
        return EvidenceItem(
            key="risk_var_backtesting",
            label="VaR/CVaR Backtesting",
            state="unknown",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="risk_var_backtest_results",
            remediation="Inspect risk_var_backtest_results readability and rerun the risk_var_backtest job.",
            details={"source_route": "/api/risk/var_backtest", "reason": f"query_failed:{type(exc).__name__}"},
        )

    sample_count = len(rows or [])
    if sample_count <= 0:
        return EvidenceItem(
            key="risk_var_backtesting",
            label="VaR/CVaR Backtesting",
            state="unknown",
            freshness="missing",
            sample_count=0,
            last_update_ts_ms=0,
            source_artifact="risk_var_backtest_results",
            remediation="Allow forecasts to mature, ensure equity_history is populated, then run risk_var_backtest.",
            details={"source_route": "/api/risk/var_backtest", "reason": "no_backtest_rows"},
        )

    latest_ts_ms = max(_safe_int(row[8], 0) or _safe_int(row[0], 0) for row in rows)
    freshness = _freshness(latest_ts_ms, now_ms=now_ms, stale_after_ms=stale_after_ms)
    failing = [
        row
        for row in rows
        if str(row[6] or "").strip().lower() == "red"
        or str(row[3] or "").strip().lower() == "fail"
        or str(row[4] or "").strip().lower() == "fail"
    ]
    if freshness == "stale":
        state = "block"
    elif failing:
        state = "block"
    else:
        state = "pass"
    return EvidenceItem(
        key="risk_var_backtesting",
        label="VaR/CVaR Backtesting",
        state=state,
        freshness=freshness,
        sample_count=sample_count,
        last_update_ts_ms=latest_ts_ms,
        source_artifact="risk_var_backtest_results",
        remediation=(
            "Review VaR model calibration, simulation method, and realized exception clustering before relying on live tail-risk forecasts."
            if state == "block"
            else "Continue scheduled risk_var_backtest evidence refreshes."
        ),
        details={
            "source_route": "/api/risk/var_backtest",
            "failing_count": int(len(failing)),
            "latest_traffic_light": str(rows[0][6] or ""),
            "latest_exception_rate": _safe_float(rows[0][5]),
        },
    )


def build_governance_evidence_summary(
    *,
    limit: int = 20,
    regime: str = "global",
    con: Any = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Return a read-only operator summary of governance evidence."""

    owns = con is None
    if owns:
        con = db_connect()
    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    try:
        promotion_item = _promotion_gate_evidence(con, now_ms=ts_ms)
        ope_item = _latest_ope_evidence(con, now_ms=ts_ms)
        ledger_item, ledger_rows = _experiment_ledger_evidence(con, now_ms=ts_ms, limit=limit)
        labels_item = _net_after_cost_evidence(con, now_ms=ts_ms)
        learned_item = _learned_alpha_evidence(con, now_ms=ts_ms)
        shrinkage_item = _alpha_shrinkage_evidence(con, now_ms=ts_ms)
        risk_var_item = _risk_var_backtest_evidence(con, now_ms=ts_ms, limit=limit)
        monitoring_item, shadow_live_item, monitoring_payload = _production_monitoring_evidence(now_ms=ts_ms)
        shadow_capital_item, shadow_capital_payload = _shadow_capital_evidence(
            con,
            now_ms=ts_ms,
            limit=limit,
            regime=regime,
        )
        evidence = [
            promotion_item.to_dict(),
            ope_item.to_dict(),
            ledger_item.to_dict(),
            labels_item.to_dict(),
            learned_item.to_dict(),
            shrinkage_item.to_dict(),
            risk_var_item.to_dict(),
            monitoring_item.to_dict(),
            shadow_live_item.to_dict(),
            shadow_capital_item.to_dict(),
        ]
        blockers = [_blocker_from_item(item) for item in evidence if _state_key(item.get("state")) == "block"]
        unknowns = [_blocker_from_item(item) for item in evidence if _state_key(item.get("state")) == "unknown"]
        generated_blockers = [
            {
                "candidate_key": str(row.get("candidate_key") or ""),
                "source_artifact": str(row.get("source_artifact") or ""),
                "blockers": list(row.get("blockers") or []),
                "remediation": str(row.get("remediation") or ""),
            }
            for row in ledger_rows
            if row.get("state") == "block"
        ]
        return {
            "ok": True,
            "ts_ms": ts_ms,
            "state": _overall_state(evidence),
            "authority": {
                "mode": "read_only_governance_evidence",
                "summary": (
                    "This surface explains evidence used by existing promotion, "
                    "generated-candidate, model-risk, and shadow-capital controls. "
                    "It does not authorize promotion, allocation, or execution."
                ),
                "authoritative_controls": [
                    "engine.strategy.promotion_guard",
                    "engine.strategy.champion_manager",
                    "engine.strategy.strategy_promotion_governance",
                    "engine.runtime.shadow_capital_allocator",
                    "engine.execution and risk gates",
                ],
            },
            "evidence": evidence,
            "blockers": blockers,
            "unknowns": unknowns,
            "promotion_blockers": build_promotion_blockers(
                limit=limit,
                regime=regime,
                con=con,
                now_ms=ts_ms,
                _summary_items=evidence,
            ),
            "generated_candidates": {
                "rows": ledger_rows,
                "blockers": generated_blockers,
            },
            "production_monitoring": monitoring_payload,
            "shadow_capital": shadow_capital_payload,
            "drilldowns": {
                "promotion_blockers": "/api/governance/evidence/promotion_blockers",
                "generated_candidates": "/api/governance/evidence/generated_candidates",
                "shadow_capital": "/api/governance/evidence/shadow_capital",
                "shadow_capital_scores": "/api/governance/shadow_capital/scores",
            },
        }
    finally:
        if owns and con is not None:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GOVERNANCE_EVIDENCE_CLOSE_FAILED", exc, once_key="summary_close")


def build_promotion_blockers(
    *,
    limit: int = 20,
    regime: str = "global",
    con: Any = None,
    now_ms: int | None = None,
    _summary_items: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return exact blocker rows for promotion and trust decisions."""

    owns = con is None
    if owns:
        con = db_connect()
    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    try:
        if _summary_items is None:
            summary = build_governance_evidence_summary(limit=limit, regime=regime, con=con, now_ms=ts_ms)
            items = list(summary.get("evidence") or [])
        else:
            items = [dict(item) for item in _summary_items]
        status = _promotion_status()
        guard_blockers = [
            {
                "key": str(blocker),
                "source_artifact": "engine.strategy.promotion_guard",
                "remediation": "Resolve this promotion_guard blocker before any challenger promotion attempt.",
            }
            for blocker in list(status.get("blockers") or [])
        ]
        evidence_blockers = [_blocker_from_item(item) for item in items if _state_key(item.get("state")) == "block"]
        return {
            "ok": True,
            "ts_ms": ts_ms,
            "state": "block" if guard_blockers or evidence_blockers else "pass",
            "guard": {
                "allowed": bool(status.get("allowed")),
                "blockers": guard_blockers,
                "reason": dict(status.get("reason") or {}),
            },
            "evidence_blockers": evidence_blockers,
            "remediation": (
                "Promotion remains controlled by backend gates. Use these blocker "
                "rows to refresh missing evidence or resolve failed controls."
            ),
        }
    finally:
        if owns and con is not None:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GOVERNANCE_BLOCKERS_CLOSE_FAILED", exc, once_key="blockers_close")


def build_generated_candidate_provenance(
    *,
    limit: int = 50,
    con: Any = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Return generated-candidate ledger rows with explainable blockers."""

    del now_ms
    owns = con is None
    if owns:
        con = db_connect()
    try:
        rows = _experiment_ledger_rows(con, limit=limit)
        blockers = [
            {
                "candidate_key": str(row.get("candidate_key") or ""),
                "candidate_name": str(row.get("candidate_name") or row.get("model_name") or ""),
                "candidate_version": str(row.get("candidate_version") or ""),
                "source_artifact": str(row.get("source_artifact") or ""),
                "blockers": list(row.get("blockers") or []),
                "remediation": str(row.get("remediation") or ""),
            }
            for row in rows
            if row.get("state") == "block"
        ]
        return {
            "ok": True,
            "state": "block" if blockers else ("pass" if rows else "block"),
            "rows": rows,
            "blockers": blockers
            or (
                []
                if rows
                else [
                    {
                        "candidate_key": "",
                        "source_artifact": "experiment_ledger",
                        "blockers": ["missing_experiment_ledger"],
                        "remediation": "Record generated-candidate provenance before trusting candidates.",
                    }
                ]
            ),
        }
    finally:
        if owns and con is not None:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GOVERNANCE_GENERATED_CLOSE_FAILED", exc, once_key="generated_close")


def build_shadow_capital_evidence(
    *,
    limit: int = 50,
    regime: str = "global",
    con: Any = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    owns = con is None
    if owns:
        con = db_connect()
    try:
        _, payload = _shadow_capital_evidence(
            con,
            now_ms=int(now_ms if now_ms is not None else _now_ms()),
            limit=limit,
            regime=regime,
        )
        return payload
    finally:
        if owns and con is not None:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GOVERNANCE_SHADOW_CLOSE_FAILED", exc, once_key="shadow_close")
