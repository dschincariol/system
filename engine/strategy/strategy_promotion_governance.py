"""Governed strategy promotion workflow.

Shadow strategy outperformance is evidence, not authority to mutate live state.
This module records shadow-to-live candidates and verifies the production
promotion evidence before `strategy_registry.stage` can become `live`.
"""

from __future__ import annotations

import json
import math
import os
import time
from typing import Any, Dict, Optional

from engine.runtime.dbapi_compat import is_sqlite_connection
from engine.runtime.storage import table_exists


PENDING_STATUSES = ("candidate", "approved")
REQUIRED_STATISTICAL_TESTS = frozenset({"white_reality_check", "deconfounded_signal_validation"})


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, separators=(",", ":"), sort_keys=True)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def strategy_candidate_model_id(strategy_name: str, candidate_version: Any) -> str:
    name = str(strategy_name or "").strip()
    version = str(candidate_version or "").strip()
    return f"strategy:{name}:{version}" if version else f"strategy:{name}"


def _operator_approval_required() -> bool:
    raw = str(os.environ.get("STRATEGY_PROMOTION_OPERATOR_APPROVAL_REQUIRED", "1") or "").strip().lower()
    return raw in {"", "1", "true", "yes", "y", "on"}


def _ope_evidence_required() -> bool:
    raw = str(os.environ.get("STRATEGY_PROMOTION_OPE_EVIDENCE_REQUIRED", "1") or "").strip().lower()
    return raw in {"", "1", "true", "yes", "y", "on"}


def _min_realized_pnl() -> float:
    return _safe_float(os.environ.get("STRATEGY_PROMOTION_MIN_REALIZED_PNL", "0.0"), 0.0)


def ensure_strategy_promotion_governance_schema(con) -> None:
    if is_sqlite_connection(con):
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_promotion_candidates (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              strategy_name TEXT NOT NULL,
              candidate_version TEXT NOT NULL,
              candidate_model_id TEXT NOT NULL,
              status TEXT NOT NULL,
              source TEXT NOT NULL,
              shadow_score REAL,
              best_live_score REAL,
              observed_shadow_runs INTEGER NOT NULL DEFAULT 0,
              min_shadow_runs INTEGER NOT NULL DEFAULT 0,
              evidence_json TEXT NOT NULL DEFAULT '{}',
              operator_approved_ts_ms INTEGER,
              operator_approved_by TEXT,
              operator_approval_reason TEXT,
              promoted_ts_ms INTEGER,
              blocked_reason TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_promotion_log (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              strategy_name TEXT NOT NULL,
              reason TEXT
            )
            """
        )
    else:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_promotion_candidates (
              id BIGSERIAL PRIMARY KEY,
              created_ts_ms BIGINT NOT NULL,
              updated_ts_ms BIGINT NOT NULL,
              strategy_name TEXT NOT NULL,
              candidate_version TEXT NOT NULL,
              candidate_model_id TEXT NOT NULL,
              status TEXT NOT NULL,
              source TEXT NOT NULL,
              shadow_score DOUBLE PRECISION,
              best_live_score DOUBLE PRECISION,
              observed_shadow_runs BIGINT NOT NULL DEFAULT 0,
              min_shadow_runs BIGINT NOT NULL DEFAULT 0,
              evidence_json TEXT NOT NULL DEFAULT '{}',
              operator_approved_ts_ms BIGINT,
              operator_approved_by TEXT,
              operator_approval_reason TEXT,
              promoted_ts_ms BIGINT,
              blocked_reason TEXT
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_promotion_log (
              id BIGSERIAL PRIMARY KEY,
              ts_ms BIGINT NOT NULL,
              strategy_name TEXT NOT NULL,
              reason TEXT
            )
            """
        )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_strategy_promotion_candidates_strategy_status
          ON strategy_promotion_candidates(strategy_name, status, updated_ts_ms)
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_strategy_promotion_candidates_model_id
          ON strategy_promotion_candidates(candidate_model_id)
        """
    )


def _append_strategy_audit(
    con,
    *,
    action: str,
    strategy_name: str,
    candidate_model_id: str,
    candidate_version: Any = None,
    reason: Optional[Dict[str, Any]] = None,
) -> None:
    from engine.model_registry import _append_model_promotion_audit_row

    _append_model_promotion_audit_row(
        con,
        actor="strategy_governance",
        action=str(action),
        model_name=str(candidate_model_id or strategy_name),
        to_kind="strategy",
        to_ts_ms=(_safe_int(candidate_version) if str(candidate_version or "").strip().isdigit() else None),
        regime="global",
        reason={
            "strategy_name": str(strategy_name),
            "candidate_model_id": str(candidate_model_id or ""),
            **dict(reason or {}),
        },
    )


def _candidate_from_row(row: Any) -> Dict[str, Any]:
    if not row:
        return {}
    evidence = _safe_json_dict(row[12] if len(row) > 12 else "{}")
    return {
        "id": _safe_int(row[0]),
        "created_ts_ms": _safe_int(row[1]),
        "updated_ts_ms": _safe_int(row[2]),
        "strategy_name": str(row[3] or ""),
        "candidate_version": str(row[4] or ""),
        "candidate_model_id": str(row[5] or ""),
        "status": str(row[6] or ""),
        "source": str(row[7] or ""),
        "shadow_score": _safe_float(row[8], 0.0),
        "best_live_score": _safe_float(row[9], 0.0),
        "observed_shadow_runs": _safe_int(row[10]),
        "min_shadow_runs": _safe_int(row[11]),
        "evidence": evidence,
        "operator_approved_ts_ms": _safe_int(row[13]),
        "operator_approved_by": str(row[14] or ""),
        "operator_approval_reason": str(row[15] or ""),
        "promoted_ts_ms": _safe_int(row[16]),
        "blocked_reason": str(row[17] or ""),
    }


def fetch_strategy_promotion_candidate(
    con,
    *,
    strategy_name: str,
    candidate_version: Any = None,
    statuses: tuple[str, ...] = PENDING_STATUSES,
) -> Dict[str, Any]:
    ensure_strategy_promotion_governance_schema(con)
    params: list[Any] = [str(strategy_name)]
    status_clause = ""
    if statuses:
        status_clause = "AND status IN (" + ",".join(["?"] * len(statuses)) + ")"
        params.extend([str(item) for item in statuses])
    version_clause = ""
    if candidate_version is not None:
        version_clause = "AND candidate_version=?"
        params.append(str(candidate_version))
    row = con.execute(
        f"""
        SELECT
          id, created_ts_ms, updated_ts_ms, strategy_name, candidate_version,
          candidate_model_id, status, source, shadow_score, best_live_score,
          observed_shadow_runs, min_shadow_runs, evidence_json,
          operator_approved_ts_ms, operator_approved_by, operator_approval_reason,
          promoted_ts_ms, blocked_reason
        FROM strategy_promotion_candidates
        WHERE strategy_name=?
          {status_clause}
          {version_clause}
        ORDER BY updated_ts_ms DESC, id DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    return _candidate_from_row(row)


def record_shadow_promotion_candidate(
    con,
    *,
    strategy_name: str,
    shadow_score: float,
    best_live_score: float,
    observed_shadow_runs: int,
    min_shadow_runs: int,
    evidence: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_strategy_promotion_governance_schema(con)
    now_ms = _now_ms()
    strategy = str(strategy_name or "").strip()
    existing = fetch_strategy_promotion_candidate(con, strategy_name=strategy)
    status = str(existing.get("status") or "candidate")
    payload = {
        **dict(existing.get("evidence") or {}),
        **dict(evidence or {}),
        "shadow_score": float(shadow_score),
        "best_live_score": float(best_live_score),
        "observed_shadow_runs": int(observed_shadow_runs),
        "min_shadow_runs": int(min_shadow_runs),
        "source": "portfolio_shadow_outperformance",
    }
    if existing:
        candidate_model_id = str(existing["candidate_model_id"])
        candidate_version = str(existing["candidate_version"])
        con.execute(
            """
            UPDATE strategy_promotion_candidates
            SET updated_ts_ms=?,
                shadow_score=?,
                best_live_score=?,
                observed_shadow_runs=?,
                min_shadow_runs=?,
                evidence_json=?,
                blocked_reason=NULL
            WHERE id=?
            """,
            (
                int(now_ms),
                float(shadow_score),
                float(best_live_score),
                int(observed_shadow_runs),
                int(min_shadow_runs),
                _json_dumps(payload),
                int(existing["id"]),
            ),
        )
    else:
        candidate_version = str(now_ms)
        candidate_model_id = strategy_candidate_model_id(strategy, candidate_version)
        con.execute(
            """
            INSERT INTO strategy_promotion_candidates(
              created_ts_ms, updated_ts_ms, strategy_name, candidate_version,
              candidate_model_id, status, source, shadow_score, best_live_score,
              observed_shadow_runs, min_shadow_runs, evidence_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                int(now_ms),
                strategy,
                str(candidate_version),
                str(candidate_model_id),
                "candidate",
                "portfolio_shadow_outperformance",
                float(shadow_score),
                float(best_live_score),
                int(observed_shadow_runs),
                int(min_shadow_runs),
                _json_dumps(payload),
            ),
        )
        _append_strategy_audit(
            con,
            action="candidate",
            strategy_name=strategy,
            candidate_model_id=str(candidate_model_id),
            candidate_version=candidate_version,
            reason={"source": "portfolio_shadow_outperformance", "evidence": payload},
        )

    con.execute(
        """
        INSERT INTO strategy_promotion_log(ts_ms, strategy_name, reason)
        VALUES(?,?,?)
        """,
        (
            int(now_ms),
            strategy,
            _json_dumps(
                {
                    "reason": "shadow_promotion_candidate_recorded",
                    "status": str(status),
                    "candidate_model_id": str(candidate_model_id),
                    "candidate_version": str(candidate_version),
                    "evidence": payload,
                }
            ),
        ),
    )
    return fetch_strategy_promotion_candidate(con, strategy_name=strategy, candidate_version=candidate_version, statuses=())


def approve_strategy_promotion_candidate(
    con,
    *,
    strategy_name: str,
    actor: str,
    reason: str,
    candidate_version: Any = None,
) -> Dict[str, Any]:
    ensure_strategy_promotion_governance_schema(con)
    candidate = fetch_strategy_promotion_candidate(
        con,
        strategy_name=strategy_name,
        candidate_version=candidate_version,
        statuses=PENDING_STATUSES,
    )
    if not candidate:
        raise RuntimeError(f"strategy promotion candidate not found for strategy={strategy_name}")
    now_ms = _now_ms()
    con.execute(
        """
        UPDATE strategy_promotion_candidates
        SET updated_ts_ms=?,
            status='approved',
            operator_approved_ts_ms=?,
            operator_approved_by=?,
            operator_approval_reason=?,
            blocked_reason=NULL
        WHERE id=?
        """,
        (int(now_ms), int(now_ms), str(actor), str(reason), int(candidate["id"])),
    )
    _append_strategy_audit(
        con,
        action="approve",
        strategy_name=str(strategy_name),
        candidate_model_id=str(candidate["candidate_model_id"]),
        candidate_version=candidate.get("candidate_version"),
        reason={"operator": str(actor), "approval_reason": str(reason)},
    )
    return fetch_strategy_promotion_candidate(
        con,
        strategy_name=strategy_name,
        candidate_version=candidate.get("candidate_version"),
        statuses=("approved",),
    )


def _realized_pnl_evidence(metrics: Dict[str, Any]) -> Dict[str, Any]:
    realized = _safe_float(
        metrics.get("rolling_realized_pnl", metrics.get("realized_pnl", metrics.get("net_realized_pnl", 0.0))),
        0.0,
    )
    total = _safe_float(
        metrics.get("rolling_total_pnl", metrics.get("total_pnl", metrics.get("net_pnl", realized))),
        realized,
    )
    min_realized = _min_realized_pnl()
    return {
        "realized_pnl": float(realized),
        "total_pnl": float(total),
        "min_realized_pnl": float(min_realized),
        "passed": bool(realized > float(min_realized)),
    }


def _statistical_evidence(con, *, candidate_model_id: str) -> Dict[str, Any]:
    try:
        from engine.strategy.promotion_audit import latest_statistical_evidence_decision

        decision = latest_statistical_evidence_decision(model_id=str(candidate_model_id), con=con)
    except Exception as exc:
        return {"passed": False, "status": "statistical_evidence_unavailable", "error": f"{type(exc).__name__}:{exc}"}
    rows = [dict(row or {}) for row in list(decision.get("rows") or [])]
    tests = {str(row.get("test_name") or "").strip() for row in rows}
    missing = sorted(REQUIRED_STATISTICAL_TESTS.difference(tests))
    passed = bool(decision.get("passed")) and not missing
    return {
        "passed": bool(passed),
        "decision": str(decision.get("decision") or "missing"),
        "rows": len(rows),
        "required_tests": sorted(REQUIRED_STATISTICAL_TESTS),
        "missing_tests": missing,
    }


def _replay_evidence(*, candidate_model_id: str, strategy_name: str, candidate_version: Any) -> Dict[str, Any]:
    try:
        from engine.strategy.model_marketplace import get_cached_replay_validation_snapshot

        state = get_cached_replay_validation_snapshot()
    except Exception as exc:
        return {"passed": False, "status": "replay_validation_unavailable", "error": f"{type(exc).__name__}:{exc}"}
    if not bool(state.get("ok")) or not bool(state.get("fresh")):
        return {
            "passed": False,
            "status": str(state.get("status") or "missing_or_stale"),
            "age_ms": _safe_int(state.get("age_ms")),
        }
    snapshot = _safe_json_dict(state.get("snapshot") or {})
    models = snapshot.get("models") if isinstance(snapshot, dict) else {}
    if isinstance(models, list):
        model_rows = [(str(idx), dict(row or {})) for idx, row in enumerate(models) if isinstance(row, dict)]
    elif isinstance(models, dict):
        model_rows = [(str(key), dict(row or {})) for key, row in models.items() if isinstance(row, dict)]
    else:
        model_rows = []
    wanted_names = {
        str(candidate_model_id),
        str(strategy_name),
        f"strategy:{strategy_name}",
    }
    version_text = str(candidate_version or "")
    for row_key, row in model_rows:
        row_names = {
            str(row_key),
            str(row_key).split("|", 1)[0],
            str(row.get("model_name") or ""),
            str(row.get("model_id") or ""),
        }
        if not bool(wanted_names.intersection(row_names)):
            continue
        if str(row.get("model_kind") or "strategy") != "strategy":
            continue
        if str(row.get("model_ts_ms") or row.get("candidate_version") or "") != version_text:
            continue
        if not bool(row.get("approved")):
            continue
        return {
            "passed": True,
            "model_key": str(row_key),
            "updated_ts_ms": _safe_int(state.get("updated_ts_ms")),
            "age_ms": _safe_int(state.get("age_ms")),
            "model_kind": str(row.get("model_kind") or "strategy"),
            "model_ts_ms": _safe_int(row.get("model_ts_ms")),
            "approved": True,
        }
    return {
        "passed": False,
        "status": "missing_fresh_approved_replay",
        "candidate_model_id": str(candidate_model_id),
        "candidate_version": version_text,
    }


def _ope_evidence(con, *, candidate_model_id: str, candidate_version: Any) -> Dict[str, Any]:
    if not _ope_evidence_required():
        return {"passed": True, "required": False, "status": "disabled"}
    try:
        exists = bool(table_exists(con, "policy_ope_evidence"))
    except Exception:
        exists = False
    if not exists:
        return {"passed": False, "required": True, "status": "missing_policy_ope_evidence_table"}
    row = con.execute(
        """
        SELECT
          ts_ms, candidate_key, model_id, model_name, candidate_type, candidate_version,
          policy_value, standard_error, ci_lower, ci_upper, n_obs, effective_n,
          support, decision, reason
        FROM policy_ope_evidence
        WHERE candidate_key=? OR model_id=?
        ORDER BY ts_ms DESC, id DESC
        LIMIT 1
        """,
        (str(candidate_model_id), str(candidate_model_id)),
    ).fetchone()
    if not row:
        return {"passed": False, "required": True, "status": "missing_policy_ope_evidence"}
    row_version = str(row[5] or "")
    if row_version and row_version != str(candidate_version or ""):
        return {
            "passed": False,
            "required": True,
            "status": "policy_ope_candidate_version_mismatch",
            "candidate_version": str(candidate_version or ""),
            "evidence_version": row_version,
        }
    decision = str(row[13] or "").strip().lower()
    return {
        "passed": decision == "pass",
        "required": True,
        "status": str(row[14] or decision or "missing"),
        "decision": decision,
        "ts_ms": _safe_int(row[0]),
        "candidate_key": str(row[1] or ""),
        "model_id": str(row[2] or ""),
        "policy_value": _safe_float(row[6], 0.0),
        "standard_error": _safe_float(row[7], 0.0),
        "ci_lower": _safe_float(row[8], 0.0),
        "ci_upper": _safe_float(row[9], 0.0),
        "n_obs": _safe_int(row[10]),
        "effective_n": _safe_float(row[11], 0.0),
        "support": _safe_float(row[12], 0.0),
    }


def _system_guard_evidence(precomputed: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if precomputed is not None:
        return dict(precomputed or {})
    try:
        from engine.strategy.promotion_guard import promotion_allowed

        allowed, reason = promotion_allowed()
        return {"passed": bool(allowed), "reason": dict(reason or {})}
    except Exception as exc:
        return {"passed": False, "reason": {"blockers": ["promotion_guard_unavailable"], "error": f"{type(exc).__name__}:{exc}"}}


def evaluate_strategy_promotion_governance(
    con,
    *,
    strategy_name: str,
    metrics: Optional[Dict[str, Any]] = None,
    audit_block: bool = False,
    system_guard: Optional[Dict[str, Any]] = None,
) -> tuple[bool, Dict[str, Any]]:
    ensure_strategy_promotion_governance_schema(con)
    strategy = str(strategy_name or "").strip()
    candidate = fetch_strategy_promotion_candidate(con, strategy_name=strategy)
    blockers: list[str] = []
    if not candidate:
        candidate_model_id = strategy_candidate_model_id(strategy, "")
        blockers.append("missing_strategy_promotion_candidate")
        result = {
            "passed": False,
            "strategy_name": strategy,
            "candidate_model_id": candidate_model_id,
            "blockers": blockers,
            "candidate": {},
            "evidence": {},
        }
        if audit_block:
            _append_strategy_audit(
                con,
                action="block",
                strategy_name=strategy,
                candidate_model_id=candidate_model_id,
                reason=result,
            )
        return False, result

    candidate_model_id = str(candidate["candidate_model_id"])
    candidate_version = str(candidate["candidate_version"])
    evidence: Dict[str, Any] = {}

    approval_required = _operator_approval_required()
    approval_passed = (not approval_required) or bool(candidate.get("operator_approved_ts_ms"))
    evidence["operator_approval"] = {
        "required": bool(approval_required),
        "passed": bool(approval_passed),
        "approved_ts_ms": _safe_int(candidate.get("operator_approved_ts_ms")),
        "approved_by": str(candidate.get("operator_approved_by") or ""),
    }
    if not approval_passed:
        blockers.append("operator_approval_required")

    evidence["realized_pnl"] = _realized_pnl_evidence(dict(metrics or {}))
    if not bool(evidence["realized_pnl"].get("passed")):
        blockers.append("realized_pnl_required")

    evidence["statistical"] = _statistical_evidence(con, candidate_model_id=candidate_model_id)
    if not bool(evidence["statistical"].get("passed")):
        blockers.append("statistical_evidence_required")

    evidence["replay_validation"] = _replay_evidence(
        candidate_model_id=candidate_model_id,
        strategy_name=strategy,
        candidate_version=candidate_version,
    )
    if not bool(evidence["replay_validation"].get("passed")):
        blockers.append("replay_validation_required")

    evidence["off_policy_evaluation"] = _ope_evidence(
        con,
        candidate_model_id=candidate_model_id,
        candidate_version=candidate_version,
    )
    if not bool(evidence["off_policy_evaluation"].get("passed")):
        blockers.append("ope_evidence_required")

    evidence["promotion_guard"] = _system_guard_evidence(system_guard)
    if not bool(evidence["promotion_guard"].get("passed")):
        blockers.append("promotion_guard_blocked")

    blockers = list(dict.fromkeys(blockers))
    result = {
        "passed": not blockers,
        "strategy_name": strategy,
        "candidate_model_id": candidate_model_id,
        "candidate_version": candidate_version,
        "candidate": dict(candidate),
        "evidence": evidence,
        "blockers": blockers,
        "gate_version": 1,
    }
    if blockers:
        con.execute(
            """
            UPDATE strategy_promotion_candidates
            SET updated_ts_ms=?, blocked_reason=?
            WHERE id=?
            """,
            (_now_ms(), ",".join(blockers), int(candidate["id"])),
        )
        if audit_block:
            _append_strategy_audit(
                con,
                action="block",
                strategy_name=strategy,
                candidate_model_id=candidate_model_id,
                candidate_version=candidate_version,
                reason=result,
            )
    return not blockers, result


def mark_strategy_promotion_promoted(
    con,
    *,
    strategy_name: str,
    governance: Dict[str, Any],
) -> Dict[str, Any]:
    ensure_strategy_promotion_governance_schema(con)
    strategy = str(strategy_name or "").strip()
    candidate_model_id = str(governance.get("candidate_model_id") or strategy_candidate_model_id(strategy, ""))
    candidate_version = str(governance.get("candidate_version") or "")
    now_ms = _now_ms()
    candidate = fetch_strategy_promotion_candidate(
        con,
        strategy_name=strategy,
        candidate_version=candidate_version or None,
        statuses=PENDING_STATUSES,
    )
    if candidate:
        con.execute(
            """
            UPDATE strategy_promotion_candidates
            SET updated_ts_ms=?,
                status='promoted',
                promoted_ts_ms=?,
                blocked_reason=NULL
            WHERE id=?
            """,
            (int(now_ms), int(now_ms), int(candidate["id"])),
        )
    _append_strategy_audit(
        con,
        action="promote",
        strategy_name=strategy,
        candidate_model_id=candidate_model_id,
        candidate_version=candidate_version,
        reason={"strategy_promotion_governance": dict(governance or {})},
    )
    return fetch_strategy_promotion_candidate(
        con,
        strategy_name=strategy,
        candidate_version=candidate_version or None,
        statuses=("promoted",),
    )


__all__ = [
    "approve_strategy_promotion_candidate",
    "ensure_strategy_promotion_governance_schema",
    "evaluate_strategy_promotion_governance",
    "fetch_strategy_promotion_candidate",
    "mark_strategy_promotion_promoted",
    "record_shadow_promotion_candidate",
    "strategy_candidate_model_id",
]
