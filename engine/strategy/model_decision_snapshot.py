"""Model promotion decision snapshot helpers.

These helpers build immutable, JSON-serializable model-card and gate-state
snapshots for promotion/rollback audit rows. They intentionally summarize the
decision evidence already supplied by callers; they do not re-run gates.
"""

from __future__ import annotations

import os
import time
from typing import Any, Mapping


DEFAULT_INTENDED_USE = (
    "Generate governed trading predictions for the configured model/regime; "
    "not standalone trading authority."
)
DEFAULT_OWNER = "model-governance"
DEFAULT_STALE_MS = 6 * 60 * 60 * 1000


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, (list, tuple)) else []


def _compact_model(
    *,
    model_kind: Any = None,
    model_ts_ms: Any = None,
    stage: str = "",
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    row = _as_dict(source)
    kind = str(row.get("model_kind") or row.get("kind") or model_kind or "").strip()
    ts_ms = _safe_int(row.get("model_ts_ms") or row.get("ts_ms") or model_ts_ms, 0)
    if not kind and ts_ms <= 0 and not row:
        return None
    metrics = row.get("metrics") if isinstance(row.get("metrics"), Mapping) else {}
    perf = row.get("performance_metrics") if isinstance(row.get("performance_metrics"), Mapping) else {}
    return {
        "model_name": str(row.get("model_name") or ""),
        "model_kind": kind,
        "model_ts_ms": ts_ms,
        "stage": str(row.get("stage") or stage or ""),
        "regime": str(row.get("regime") or ""),
        "status": str(row.get("status") or row.get("stage") or stage or ""),
        "created_ts_ms": _safe_int(row.get("created_ts_ms"), 0),
        "updated_ts_ms": _safe_int(row.get("updated_ts_ms") or row.get("created_ts_ms") or ts_ms, 0),
        "metrics": dict(metrics),
        "performance_metrics": dict(perf),
    }


def _citation(source: str, ts_ms: int = 0, label: str = "", detail: str = "") -> dict[str, Any]:
    return {
        "source": str(source or "unknown"),
        "label": str(label or source or "source"),
        "ts_ms": int(ts_ms or 0),
        "detail": str(detail or ""),
    }


def _model_ts_candidates(*models: dict[str, Any] | None) -> list[int]:
    out: list[int] = []
    for model in models:
        if not model:
            continue
        for key in ("updated_ts_ms", "created_ts_ms", "model_ts_ms"):
            ts_ms = _safe_int(model.get(key), 0)
            if ts_ms > 0:
                out.append(ts_ms)
    return out


def _decision_data_window(
    reason: Mapping[str, Any],
    *,
    decision_ts_ms: int,
    models: list[dict[str, Any] | None],
) -> dict[str, Any]:
    existing = reason.get("data_window")
    if isinstance(existing, Mapping):
        out = dict(existing)
        out.setdefault("source", "audit_reason.data_window")
        return out

    candidates = _model_ts_candidates(*models)
    return {
        "start_ts_ms": min(candidates) if candidates else None,
        "end_ts_ms": max(candidates) if candidates else int(decision_ts_ms),
        "as_of_ts_ms": int(decision_ts_ms),
        "source": "model_registry.metrics",
    }


def _stale_after_ms() -> int:
    return max(1, _safe_int(os.environ.get("MODEL_GOVERNANCE_EVIDENCE_STALE_MS"), DEFAULT_STALE_MS))


def _badge(
    key: str,
    label: str,
    state: str,
    *,
    severity: str = "warn",
    source: str = "",
    ts_ms: int = 0,
    detail: str = "",
) -> dict[str, Any]:
    return {
        "key": str(key),
        "label": str(label),
        "state": str(state),
        "severity": str(severity),
        "source": str(source or "governance"),
        "ts_ms": int(ts_ms or 0),
        "detail": str(detail or ""),
    }


def build_staleness_badges(
    *,
    gate_snapshot: Mapping[str, Any] | None,
    decision_ts_ms: int,
) -> list[dict[str, Any]]:
    gate = _as_dict(gate_snapshot)
    stale_after_ms = _stale_after_ms()
    badges: list[dict[str, Any]] = []

    for role in ("champion", "challenger", "rollback_target"):
        model = _as_dict(gate.get(role))
        if not model:
            continue
        ts_ms = _safe_int(model.get("updated_ts_ms") or model.get("model_ts_ms"), 0)
        if ts_ms <= 0:
            badges.append(_badge(
                f"{role}_metrics_timestamp_missing",
                f"{role.replace('_', ' ').title()} metrics timestamp missing",
                "unknown",
                source="model_registry",
            ))
        elif int(decision_ts_ms) - ts_ms > stale_after_ms:
            badges.append(_badge(
                f"{role}_metrics_stale",
                f"{role.replace('_', ' ').title()} metrics stale",
                "stale",
                source="model_registry",
                ts_ms=ts_ms,
                detail=f"older than {stale_after_ms} ms",
            ))

    validation = _as_dict(gate.get("validation"))
    replay_status = _as_dict(validation.get("replay_status"))
    replay_ts = _safe_int(replay_status.get("ts_ms") or replay_status.get("updated_ts_ms"), 0)
    replay_state = str(replay_status.get("status") or "").strip().lower()
    if replay_status:
        if replay_status.get("fresh") is False or replay_state in {"stale", "fail", "failed", "blocked", "rejected"}:
            badges.append(_badge(
                "replay_stale",
                "Replay evidence stale",
                "stale",
                source="competition_replay_validation_status",
                ts_ms=replay_ts,
                detail=replay_state or "fresh=false",
            ))
        elif replay_ts > 0 and int(decision_ts_ms) - replay_ts > stale_after_ms:
            badges.append(_badge(
                "replay_stale",
                "Replay evidence stale",
                "stale",
                source="competition_replay_validation_status",
                ts_ms=replay_ts,
                detail=f"older than {stale_after_ms} ms",
            ))
    else:
        badges.append(_badge(
            "replay_unavailable",
            "Replay evidence unavailable",
            "unavailable",
            source="competition_replay_validation_status",
            detail="no replay status in decision snapshot",
        ))

    checklist = [_as_dict(row) for row in _as_list(gate.get("checklist"))]
    checklist_by_key = {str(row.get("key") or ""): row for row in checklist}
    temporal = checklist_by_key.get("cpcv_gate") or checklist_by_key.get("temporal_eval")
    if temporal:
        temporal_state = str(temporal.get("state") or "").strip().lower()
        if temporal_state in {"fail", "unavailable", "unknown", ""}:
            badges.append(_badge(
                "temporal_eval_stale",
                "Temporal evaluation stale or unavailable",
                "stale" if temporal_state == "fail" else "unavailable",
                source="promotion_gate.checklist",
                detail=temporal_state or "unknown",
            ))
    else:
        badges.append(_badge(
            "temporal_eval_unavailable",
            "Temporal evaluation unavailable",
            "unavailable",
            source="promotion_gate.checklist",
        ))

    blockers = _as_list(_as_dict(gate.get("status")).get("blockers"))
    execution_tokens = ("execution", "degradation", "broker", "readiness", "latency", "stale")
    if any(any(token in str(blocker).lower() for token in execution_tokens) for blocker in blockers):
        badges.append(_badge(
            "execution_degradation",
            "Execution degradation blocks governance action",
            "conflict",
            severity="crit",
            source="promotion_guard.blockers",
            detail=", ".join(str(item) for item in blockers[:5]),
        ))

    return badges


def build_source_citations(
    *,
    gate_snapshot: Mapping[str, Any] | None,
    decision_ts_ms: int,
) -> list[dict[str, Any]]:
    gate = _as_dict(gate_snapshot)
    citations = [_citation(gate.get("source") or "promotion_audit", decision_ts_ms, "decision snapshot")]
    status = _as_dict(gate.get("status"))
    status_ts = _safe_int(status.get("updated_ts_ms"), 0)
    if status_ts > 0:
        citations.append(_citation("promotion_guard.status", status_ts, "promotion guard status"))
    for role in ("champion", "challenger", "rollback_target"):
        model = _as_dict(gate.get(role))
        if model:
            citations.append(_citation(
                "model_registry",
                _safe_int(model.get("updated_ts_ms") or model.get("model_ts_ms"), 0),
                role.replace("_", " "),
                str(model.get("model_kind") or ""),
            ))
    replay_status = _as_dict(_as_dict(gate.get("validation")).get("replay_status"))
    if replay_status:
        citations.append(_citation(
            "runtime_meta.competition_replay_validation_status",
            _safe_int(replay_status.get("ts_ms") or replay_status.get("updated_ts_ms"), 0),
            "replay validation",
            str(replay_status.get("status") or ""),
        ))
    return citations


def enrich_decision_reason(
    reason: Mapping[str, Any] | None,
    *,
    action: str,
    actor: str,
    model_name: str,
    from_kind: Any = None,
    from_ts_ms: Any = None,
    to_kind: Any = None,
    to_ts_ms: Any = None,
    regime: Any = None,
    gate_snapshot: Mapping[str, Any] | None = None,
    confirmation: Mapping[str, Any] | None = None,
    decision_ts_ms: int | None = None,
) -> dict[str, Any]:
    out = dict(reason or {})
    decision_ts = int(decision_ts_ms or _now_ms())
    gate = _as_dict(gate_snapshot or out.get("gate_snapshot") or out.get("gate_state_snapshot"))
    from_model = _compact_model(
        model_kind=from_kind,
        model_ts_ms=from_ts_ms,
        stage="prior_champion",
        source=gate.get("champion") if isinstance(gate.get("champion"), Mapping) else None,
    )
    to_source = None
    if str(action or "").lower() == "rollback" and isinstance(gate.get("rollback_target"), Mapping):
        to_source = gate.get("rollback_target")
    elif isinstance(gate.get("challenger"), Mapping):
        to_source = gate.get("challenger")
    to_model = _compact_model(
        model_kind=to_kind,
        model_ts_ms=to_ts_ms,
        stage="decision_target",
        source=to_source if isinstance(to_source, Mapping) else None,
    )
    comparison = _as_list(gate.get("comparison_metrics")) or _as_list(out.get("comparison_to_champion"))
    checklist = _as_list(gate.get("checklist")) or _as_list(out.get("gates"))
    validation = _as_dict(gate.get("validation")) or _as_dict(out.get("validation"))
    source_citations = build_source_citations(gate_snapshot=gate, decision_ts_ms=decision_ts)
    staleness_badges = build_staleness_badges(gate_snapshot=gate, decision_ts_ms=decision_ts)
    caveats = list(out.get("caveats") or []) if isinstance(out.get("caveats"), list) else []
    caveats.extend(str(badge.get("label")) for badge in staleness_badges if badge.get("state") != "fresh")

    owner = str(out.get("owner") or os.environ.get("MODEL_GOVERNANCE_OWNER") or actor or DEFAULT_OWNER)
    model_card = {
        "schema_version": 1,
        "action": str(action or ""),
        "model_name": str(model_name or ""),
        "regime": str(regime if regime is not None else out.get("regime") or "global"),
        "owner": owner,
        "intended_use": str(out.get("intended_use") or os.environ.get("MODEL_GOVERNANCE_INTENDED_USE") or DEFAULT_INTENDED_USE),
        "decision_ts_ms": decision_ts,
        "data_window": _decision_data_window(out, decision_ts_ms=decision_ts, models=[from_model, to_model]),
        "metrics": {
            "from_model": from_model,
            "to_model": to_model,
            "comparison_to_champion": comparison,
        },
        "gates": checklist,
        "caveats": sorted({str(item) for item in caveats if str(item).strip()}),
        "comparison_to_champion": comparison,
        "source_citations": source_citations,
    }
    gate_state = {
        "schema_version": 1,
        "action": str(action or ""),
        "model_name": str(model_name or ""),
        "regime": model_card["regime"],
        "decision_ts_ms": decision_ts,
        "status": _as_dict(gate.get("status")) or _as_dict(out.get("status")),
        "checklist": checklist,
        "validation": validation,
        "staleness_badges": staleness_badges,
        "source_citations": source_citations,
    }

    out.setdefault("model_card_snapshot", model_card)
    out.setdefault("gate_state_at_decision", gate_state)
    out.setdefault("source_citations", source_citations)
    out.setdefault("staleness_badges", staleness_badges)
    if confirmation:
        out.setdefault("confirmation", dict(confirmation))
    return out
