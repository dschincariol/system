"""
FILE: event_replay.py

Runtime subsystem module for `event_replay`.
"""

import copy
import hashlib
import json
import math
import os
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.event_log import flush_event_log_buffer, replay_events
from engine.runtime.storage import connect


REPLAY_RELEVANT_EVENT_TYPES = (
    "decision",
    "order_decision",
    "order_submit",
    "fill",
    "regime_change",
    "risk_block",
    "allocator_decision",
    "kill_switch_enabled",
    "kill_switch_cleared",
    "lifecycle_state_change",
    "engine_stop",
    "engine_start",
    "execution_block",
    "order_error",
    "order_reject",
)
LOG = get_logger("engine.runtime.event_replay")
_WARNED_NONFATAL_KEYS: set[str] = set()
_CAPITAL_RECON_BATCH_WINDOW_MS = int(os.environ.get("CAPITAL_RECON_BATCH_WINDOW_MS", "2500"))
_CAPITAL_RECON_STALE_MS = int(os.environ.get("CAPITAL_RECON_STALE_MS", "900000"))
_CAPITAL_RECON_WEIGHT_TOL = float(os.environ.get("CAPITAL_RECON_WEIGHT_TOL", "0.0005"))
_CAPITAL_RECON_MIN_ACTIONABLE_WEIGHT = float(os.environ.get("CAPITAL_RECON_MIN_ACTIONABLE_WEIGHT", "0.000001"))


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
        component="engine.runtime.event_replay",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _default_state() -> Dict[str, Any]:
    return {
        "last_event_id": 0,
        "decisions": [],
        "orders": {},
        "fills": {},
        "regimes": {},
        "risk_blocks": {},
        "allocator": {},
        "kill_switches": {},
        "lifecycle": {},
        "execution_blocks": [],
        "errors": [],
    }


def apply_event(state: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(state or _default_state())
    payload = dict(event.get("payload") or {})
    et = str(event.get("event_type") or "")
    entity_id = event.get("entity_id")
    eid = int(event.get("id") or 0)

    out["last_event_id"] = max(int(out.get("last_event_id", 0)), eid)

    # Replay builds a pragmatic operator-facing state view, not a perfect
    # event-sourced domain model. Unknown event types are ignored by design.
    if et == "decision":
        out["decisions"].append({"event": dict(event), "payload": payload})
    elif et == "order_decision":
        key = str(entity_id or payload.get("batch_id") or eid)
        out["orders"].setdefault(key, {})
        out["orders"][key]["decision"] = dict(payload)
    elif et == "order_submit":
        key = str(entity_id or payload.get("client_order_id") or eid)
        out["orders"].setdefault(key, {})
        out["orders"][key]["submit"] = dict(payload)
    elif et == "fill":
        key = str(entity_id or payload.get("client_order_id") or eid)
        out["fills"].setdefault(key, [])
        out["fills"][key].append(dict(payload))
    elif et == "regime_change":
        key = str(entity_id or payload.get("symbol") or "unknown")
        out["regimes"][key] = dict(payload)
    elif et == "risk_block":
        key = str(entity_id or payload.get("name") or "risk")
        out["risk_blocks"][key] = dict(payload)
    elif et == "allocator_decision":
        out["allocator"] = dict(payload)
    elif et in ("kill_switch_enabled", "kill_switch_cleared"):
        key = str(entity_id or "unknown")
        out["kill_switches"][key] = dict(payload)
    elif et in ("lifecycle_state_change", "engine_stop", "engine_start"):
        out["lifecycle"] = dict(payload)
    elif et == "execution_block":
        out["execution_blocks"].append({"event": dict(event), "payload": dict(payload)})
    elif et in ("order_error", "order_reject"):
        out["errors"].append({"event": dict(event), "payload": dict(payload)})

    return out


def replay_state(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
    initial_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    state = copy.deepcopy(initial_state or _default_state())
    cursor = int(after_event_id)
    remaining = int(limit)

    # Replay in bounded batches so large event logs do not require a single
    # giant query or unbounded memory spike.
    while remaining > 0:
        batch = replay_events(after_event_id=cursor, limit=min(5000, remaining))
        if not batch:
            break

        for event in batch:
            state = apply_event(state, event)
            cursor = int(event.get("id") or cursor)
            remaining -= 1
            if remaining <= 0:
                break

    return state


def replay_outputs(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
    event_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    selected_types = {
        str(t).strip()
        for t in (event_types or list(REPLAY_RELEVANT_EVENT_TYPES))
        if str(t).strip()
    }
    cursor = int(after_event_id)
    remaining = int(limit)
    outputs: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}

    while remaining > 0:
        batch = replay_events(after_event_id=cursor, limit=min(5000, remaining))
        if not batch:
            break

        for event in batch:
            cursor = int(event.get("id") or cursor)
            remaining -= 1
            et = str(event.get("event_type") or "")
            if et in selected_types:
                payload = dict(event.get("payload") or {})
                outputs.append(
                    {
                        "id": int(event.get("id") or 0),
                        "ts_ms": int(event.get("ts_ms") or 0),
                        "event_type": et,
                        "event_source": str(event.get("event_source") or ""),
                        "entity_type": event.get("entity_type"),
                        "entity_id": event.get("entity_id"),
                        "correlation_id": event.get("correlation_id"),
                        "payload": _canonicalize(payload),
                    }
                )
                counts[et] = int(counts.get(et, 0)) + 1
            if remaining <= 0:
                break

    return {
        "ok": True,
        "after_event_id": int(after_event_id),
        "last_event_id": int(cursor),
        "counts": {str(k): int(v) for k, v in sorted(counts.items())},
        "events": outputs,
        "digest": _stable_digest(outputs),
    }


def replay_determinism_snapshot(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
    initial_state: Optional[Dict[str, Any]] = None,
    event_types: Optional[List[str]] = None,
) -> Dict[str, Any]:
    state_a = replay_state(
        after_event_id=int(after_event_id),
        limit=int(limit),
        initial_state=initial_state,
    )
    state_b = replay_state(
        after_event_id=int(after_event_id),
        limit=int(limit),
        initial_state=initial_state,
    )
    outputs_a = replay_outputs(
        after_event_id=int(after_event_id),
        limit=int(limit),
        event_types=event_types,
    )
    outputs_b = replay_outputs(
        after_event_id=int(after_event_id),
        limit=int(limit),
        event_types=event_types,
    )

    state_digest_a = _stable_digest(state_a)
    state_digest_b = _stable_digest(state_b)
    outputs_digest_a = str(outputs_a.get("digest") or "")
    outputs_digest_b = str(outputs_b.get("digest") or "")
    deterministic = bool(
        state_digest_a == state_digest_b
        and outputs_digest_a == outputs_digest_b
    )

    return {
        "ok": True,
        "deterministic": bool(deterministic),
        "after_event_id": int(after_event_id),
        "limit": int(limit),
        "state_digest": str(state_digest_a),
        "outputs_digest": str(outputs_digest_a),
        "state_match": bool(state_digest_a == state_digest_b),
        "outputs_match": bool(outputs_digest_a == outputs_digest_b),
        "relevant_counts": dict(outputs_a.get("counts") or {}),
        "last_event_id": int(outputs_a.get("last_event_id") or state_a.get("last_event_id") or 0),
    }


def replay_persisted_outputs(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
) -> Dict[str, Any]:
    window = _event_window(after_event_id=int(after_event_id), limit=int(limit))
    if not window.get("ok"):
        return {
            "ok": False,
            "error": str(window.get("error") or "event_window_unavailable"),
            "after_event_id": int(after_event_id),
            "limit": int(limit),
        }

    min_ts_ms = int(window.get("min_ts_ms") or 0)
    max_ts_ms = int(window.get("max_ts_ms") or 0)
    last_event_id = int(window.get("last_event_id") or 0)
    if max_ts_ms <= 0:
        empty = {
            "ok": True,
            "after_event_id": int(after_event_id),
            "last_event_id": int(last_event_id),
            "min_ts_ms": int(min_ts_ms),
            "max_ts_ms": int(max_ts_ms),
            "tables": {},
            "counts": {},
            "digest": _stable_digest({}),
        }
        return empty

    con = connect(readonly=True)
    try:
        tables = {
            "predictions": _fetch_predictions(con, min_ts_ms=min_ts_ms, max_ts_ms=max_ts_ms),
            "alerts": _fetch_alerts(con, min_ts_ms=min_ts_ms, max_ts_ms=max_ts_ms),
            "portfolio_orders": _fetch_portfolio_orders(con, min_ts_ms=min_ts_ms, max_ts_ms=max_ts_ms),
            "execution_orders": _fetch_execution_orders(con, min_ts_ms=min_ts_ms, max_ts_ms=max_ts_ms),
            "execution_fills": _fetch_execution_fills(con, min_ts_ms=min_ts_ms, max_ts_ms=max_ts_ms),
            "pnl_attribution": _fetch_pnl_attribution(con, min_ts_ms=min_ts_ms, max_ts_ms=max_ts_ms),
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_CLOSE_FAILED",
                e,
                once_key="persisted_outputs_close",
                after_event_id=int(after_event_id),
            )

    counts = {str(name): int(len(rows)) for name, rows in tables.items()}
    digest_payload = {
        "window": {
            "after_event_id": int(after_event_id),
            "last_event_id": int(last_event_id),
            "min_ts_ms": int(min_ts_ms),
            "max_ts_ms": int(max_ts_ms),
        },
        "tables": tables,
    }
    return {
        "ok": True,
        "after_event_id": int(after_event_id),
        "last_event_id": int(last_event_id),
        "min_ts_ms": int(min_ts_ms),
        "max_ts_ms": int(max_ts_ms),
        "tables": tables,
        "counts": counts,
        "digest": _stable_digest(digest_payload),
    }


def replay_persisted_determinism_snapshot(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
) -> Dict[str, Any]:
    snap_a = replay_persisted_outputs(after_event_id=int(after_event_id), limit=int(limit))
    snap_b = replay_persisted_outputs(after_event_id=int(after_event_id), limit=int(limit))
    digest_a = str(snap_a.get("digest") or "")
    digest_b = str(snap_b.get("digest") or "")
    return {
        "ok": bool(snap_a.get("ok")) and bool(snap_b.get("ok")),
        "deterministic": bool(digest_a == digest_b),
        "after_event_id": int(after_event_id),
        "limit": int(limit),
        "digest": digest_a,
        "digest_match": bool(digest_a == digest_b),
        "counts": dict(snap_a.get("counts") or {}),
        "min_ts_ms": int(snap_a.get("min_ts_ms") or 0),
        "max_ts_ms": int(snap_a.get("max_ts_ms") or 0),
        "last_event_id": int(snap_a.get("last_event_id") or 0),
    }


def replay_pipeline_chain_snapshot(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
) -> Dict[str, Any]:
    snap = replay_persisted_outputs(after_event_id=int(after_event_id), limit=int(limit))
    if not snap.get("ok"):
        return {
            "ok": False,
            "error": str(snap.get("error") or "persisted_outputs_unavailable"),
            "after_event_id": int(after_event_id),
            "limit": int(limit),
        }

    tables = dict(snap.get("tables") or {})
    predictions = list(tables.get("predictions") or [])
    alerts = list(tables.get("alerts") or [])
    portfolio_orders = list(tables.get("portfolio_orders") or [])
    execution_orders = list(tables.get("execution_orders") or [])
    execution_fills = list(tables.get("execution_fills") or [])
    pnl_rows = list(tables.get("pnl_attribution") or [])

    prediction_event_ids = {
        int(row.get("event_id") or 0)
        for row in predictions
        if int(row.get("event_id") or 0) > 0
    }
    alert_event_ids = {
        int(_alert_event_id(row) or 0)
        for row in alerts
        if int(_alert_event_id(row) or 0) > 0
    }
    alert_ids = {
        int(_alert_id_from_row(row) or 0)
        for row in alerts
        if int(_alert_id_from_row(row) or 0) > 0
    }
    portfolio_alert_ids = {
        int(row.get("source_alert_id") or 0)
        for row in portfolio_orders
        if row.get("source_alert_id") is not None and int(row.get("source_alert_id") or 0) > 0
    }
    execution_alert_ids = {
        int(row.get("source_alert_id") or 0)
        for row in execution_orders
        if row.get("source_alert_id") is not None and int(row.get("source_alert_id") or 0) > 0
    }
    fill_client_ids = {
        str(row.get("client_order_id") or "").strip()
        for row in execution_fills
        if str(row.get("client_order_id") or "").strip()
    }
    execution_client_ids = {
        str(row.get("client_order_id") or "").strip()
        for row in execution_orders
        if str(row.get("client_order_id") or "").strip()
    }
    pnl_alert_ids = {
        int(row.get("source_alert_id") or 0)
        for row in pnl_rows
        if int(row.get("source_alert_id") or 0) > 0
    }

    summary = {
        "predictions": int(len(predictions)),
        "prediction_event_ids": int(len(prediction_event_ids)),
        "alerts": int(len(alerts)),
        "alert_event_ids": int(len(alert_event_ids)),
        "alert_ids": int(len(alert_ids)),
        "portfolio_orders": int(len(portfolio_orders)),
        "portfolio_alert_ids": int(len(portfolio_alert_ids)),
        "execution_orders": int(len(execution_orders)),
        "execution_alert_ids": int(len(execution_alert_ids)),
        "execution_client_ids": int(len(execution_client_ids)),
        "execution_fills": int(len(execution_fills)),
        "fill_client_ids": int(len(fill_client_ids)),
        "pnl_attribution_rows": int(len(pnl_rows)),
        "pnl_alert_ids": int(len(pnl_alert_ids)),
    }

    continuity = {
        "alerts_with_prediction_event": int(len(alert_event_ids & prediction_event_ids)),
        "portfolio_with_alert": int(len(portfolio_alert_ids & alert_ids)),
        "execution_with_portfolio_alert": int(len(execution_alert_ids & portfolio_alert_ids)),
        "fills_with_execution_order": int(len(fill_client_ids & execution_client_ids)),
        "pnl_with_execution_alert": int(len(pnl_alert_ids & execution_alert_ids)),
    }

    gaps = {
        "alert_event_ids_missing_predictions": sorted(alert_event_ids - prediction_event_ids)[:100],
        "portfolio_alert_ids_missing_alerts": sorted(portfolio_alert_ids - alert_ids)[:100],
        "execution_alert_ids_missing_portfolio_orders": sorted(execution_alert_ids - portfolio_alert_ids)[:100],
        "fill_client_ids_missing_execution_orders": sorted(fill_client_ids - execution_client_ids)[:100],
        "pnl_alert_ids_missing_execution_orders": sorted(pnl_alert_ids - execution_alert_ids)[:100],
    }

    payload = {
        "window": {
            "after_event_id": int(after_event_id),
            "last_event_id": int(snap.get("last_event_id") or 0),
            "min_ts_ms": int(snap.get("min_ts_ms") or 0),
            "max_ts_ms": int(snap.get("max_ts_ms") or 0),
        },
        "summary": summary,
        "continuity": continuity,
        "gaps": gaps,
    }
    return {
        "ok": True,
        "after_event_id": int(after_event_id),
        "last_event_id": int(snap.get("last_event_id") or 0),
        "min_ts_ms": int(snap.get("min_ts_ms") or 0),
        "max_ts_ms": int(snap.get("max_ts_ms") or 0),
        "summary": summary,
        "continuity": continuity,
        "gaps": gaps,
        "digest": _stable_digest(payload),
    }


def replay_pipeline_chain_determinism_snapshot(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
) -> Dict[str, Any]:
    snap_a = replay_pipeline_chain_snapshot(after_event_id=int(after_event_id), limit=int(limit))
    snap_b = replay_pipeline_chain_snapshot(after_event_id=int(after_event_id), limit=int(limit))
    digest_a = str(snap_a.get("digest") or "")
    digest_b = str(snap_b.get("digest") or "")
    return {
        "ok": bool(snap_a.get("ok")) and bool(snap_b.get("ok")),
        "deterministic": bool(digest_a == digest_b),
        "after_event_id": int(after_event_id),
        "limit": int(limit),
        "digest": digest_a,
        "digest_match": bool(digest_a == digest_b),
        "summary": dict(snap_a.get("summary") or {}),
        "continuity": dict(snap_a.get("continuity") or {}),
        "last_event_id": int(snap_a.get("last_event_id") or 0),
    }


def replay_capital_reconciliation_snapshot(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
    batch_window_ms: int = _CAPITAL_RECON_BATCH_WINDOW_MS,
) -> Dict[str, Any]:
    snap = replay_persisted_outputs(after_event_id=int(after_event_id), limit=int(limit))
    if not snap.get("ok"):
        return {
            "ok": False,
            "error": str(snap.get("error") or "persisted_outputs_unavailable"),
            "after_event_id": int(after_event_id),
            "limit": int(limit),
        }

    findings: List[Dict[str, Any]] = []

    def _add_finding(severity: str, code: str, message: str, **meta: Any) -> None:
        findings.append(
            {
                "severity": str(severity),
                "code": str(code),
                "message": str(message),
                "meta": _canonicalize(dict(meta or {})),
            }
        )

    tables = dict(snap.get("tables") or {})
    all_orders = list(tables.get("portfolio_orders") or [])
    latest_order_ts_ms = max((int((row or {}).get("ts_ms") or 0) for row in all_orders), default=0)
    batch_lo_ts_ms = int(latest_order_ts_ms) - int(max(250, int(batch_window_ms)))
    latest_orders = [
        dict(row or {})
        for row in all_orders
        if int((row or {}).get("ts_ms") or 0) >= int(batch_lo_ts_ms)
    ] if latest_order_ts_ms > 0 else []
    actionable_orders = [
        dict(row or {})
        for row in latest_orders
        if abs(_safe_float((row or {}).get("to_weight"), 0.0)) > float(_CAPITAL_RECON_MIN_ACTIONABLE_WEIGHT)
        and str((row or {}).get("to_side") or "").upper() != "FLAT"
    ]
    reference_ts_ms = int(
        latest_order_ts_ms
        or snap.get("max_ts_ms")
        or 0
    )

    con = connect(readonly=True)
    try:
        strategy_row = _load_latest_strategy_allocations_row(con, reference_ts_ms=int(reference_ts_ms))
        competition_plan = _load_runtime_meta_json(con, "competition_capital_plan")
        risk_row = _load_latest_portfolio_risk_snapshot(con, reference_ts_ms=int(reference_ts_ms))
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_CLOSE_FAILED",
                e,
                once_key="capital_reconciliation_close",
                after_event_id=int(after_event_id),
            )

    try:
        from engine.strategy.portfolio_execution_intents import load_latest_execution_intents

        intents_con = connect()
        try:
            intents_res = load_latest_execution_intents(
                intents_con,
                window_ms=int(max(250, int(batch_window_ms))),
                max_rows=max(5000, int(len(latest_orders) or 0) + 100),
            )
        finally:
            try:
                intents_con.close()
            except Exception as e:
                _warn_nonfatal(
                    "EVENT_REPLAY_CLOSE_FAILED",
                    e,
                    once_key="capital_reconciliation_intents_close",
                    after_event_id=int(after_event_id),
                )
    except Exception as e:
        intents_res = {"ok": False, "error": f"load_latest_execution_intents_failed:{e}"}
        _warn_nonfatal(
            "EVENT_REPLAY_EXECUTION_INTENTS_LOAD_FAILED",
            e,
            once_key="capital_reconciliation_execution_intents_load",
        )

    strategy_summary = _summarize_strategy_allocator_row(strategy_row, reference_ts_ms=int(reference_ts_ms))
    if actionable_orders and not bool(strategy_summary.get("present")):
        _add_finding("error", "strategy_allocation_missing", "missing strategy allocation row for actionable batch")
    if bool(strategy_summary.get("present")):
        if not bool(strategy_summary.get("normalized", False)):
            _add_finding(
                "error",
                "strategy_allocation_not_normalized",
                "strategy allocation weights are not normalized",
                allocation_sum=strategy_summary.get("allocation_sum"),
            )
        if list(strategy_summary.get("invalid_weight_keys") or []):
            _add_finding(
                "error",
                "strategy_allocation_invalid_weights",
                "strategy allocation row contains invalid weights",
                invalid_weight_keys=list(strategy_summary.get("invalid_weight_keys") or []),
            )
        if actionable_orders and bool(strategy_summary.get("stale")):
            _add_finding(
                "error",
                "strategy_allocation_stale",
                "strategy allocation row is older than allowed for actionable batch",
                age_ms=strategy_summary.get("age_ms"),
            )

    competition_summary = _summarize_competition_capital_plan(competition_plan, reference_ts_ms=int(reference_ts_ms))
    if actionable_orders and not bool(competition_summary.get("present")):
        _add_finding("error", "competition_capital_plan_missing", "missing competition capital plan for actionable batch")
    if bool(competition_summary.get("present")):
        if bool(competition_summary.get("stale")) and actionable_orders:
            _add_finding(
                "error",
                "competition_capital_plan_stale",
                "competition capital plan is stale for actionable batch",
                age_ms=competition_summary.get("age_ms"),
                max_age_ms=competition_summary.get("max_age_ms"),
            )
        if not bool(competition_summary.get("normalized", False)):
            _add_finding(
                "error",
                "competition_capital_plan_not_normalized",
                "competition capital plan budgets are not normalized",
                total_group_budget_fraction=competition_summary.get("total_group_budget_fraction"),
                total_capital_fraction=competition_summary.get("competition_total_capital_fraction"),
            )
        if list(competition_summary.get("invalid_groups") or []):
            _add_finding(
                "error",
                "competition_capital_plan_invalid_groups",
                "competition capital plan contains invalid group/model allocations",
                invalid_groups=list(competition_summary.get("invalid_groups") or []),
            )

    latest_orders_summary = _summarize_latest_portfolio_orders(latest_orders)
    if actionable_orders and int(latest_orders_summary.get("missing_strategy_count") or 0) > 0:
        _add_finding(
            "error",
            "portfolio_orders_missing_strategy_attribution",
            "latest actionable portfolio orders are missing strategy attribution",
            missing_strategy_count=int(latest_orders_summary.get("missing_strategy_count") or 0),
        )
    if actionable_orders and int(latest_orders_summary.get("missing_model_count") or 0) > 0:
        _add_finding(
            "error",
            "portfolio_orders_missing_model_attribution",
            "latest actionable portfolio orders are missing model attribution",
            missing_model_count=int(latest_orders_summary.get("missing_model_count") or 0),
        )
    if actionable_orders and int(latest_orders_summary.get("missing_competition_count") or 0) > 0:
        _add_finding(
            "error",
            "portfolio_orders_missing_competition_attribution",
            "latest actionable portfolio orders are missing competition attribution",
            missing_competition_count=int(latest_orders_summary.get("missing_competition_count") or 0),
        )
    if actionable_orders and int(latest_orders_summary.get("unauthoritative_competition_count") or 0) > 0:
        _add_finding(
            "error",
            "portfolio_orders_without_authoritative_competition_capital",
            "latest actionable portfolio orders do not carry authoritative upstream competition capital",
            unauthoritative_competition_count=int(latest_orders_summary.get("unauthoritative_competition_count") or 0),
        )
    if list(latest_orders_summary.get("model_budget_overages") or []):
        _add_finding(
            "error",
            "portfolio_orders_model_budget_overage",
            "latest actionable portfolio orders exceed model capital budgets",
            overages=list(latest_orders_summary.get("model_budget_overages") or []),
        )
    if list(latest_orders_summary.get("group_budget_overages") or []):
        _add_finding(
            "error",
            "portfolio_orders_group_budget_overage",
            "latest actionable portfolio orders exceed group capital budgets",
            overages=list(latest_orders_summary.get("group_budget_overages") or []),
        )

    intents_summary = _summarize_execution_intents_result(intents_res)
    if actionable_orders and not bool(intents_summary.get("ok", False)):
        _add_finding(
            "error",
            "execution_intents_unavailable",
            "latest execution intents could not be loaded",
            error=intents_summary.get("error"),
        )
    if actionable_orders and int(intents_summary.get("real_count") or 0) <= 0:
        _add_finding("error", "execution_intents_missing_real_batch", "latest actionable batch produced no real execution intents")
    if list(intents_summary.get("model_budget_overages") or []):
        _add_finding(
            "error",
            "execution_intents_model_budget_overage",
            "execution intents exceed model budgets after enforcement",
            overages=list(intents_summary.get("model_budget_overages") or []),
        )
    if list(intents_summary.get("group_budget_overages") or []):
        _add_finding(
            "error",
            "execution_intents_group_budget_overage",
            "execution intents exceed group budgets after enforcement",
            overages=list(intents_summary.get("group_budget_overages") or []),
        )
    if list(intents_summary.get("negative_remaining_budget") or []):
        _add_finding(
            "error",
            "execution_intents_negative_remaining_budget",
            "execution intents reported negative remaining model/group budget",
            rows=list(intents_summary.get("negative_remaining_budget") or []),
        )

    risk_summary = _summarize_portfolio_risk_snapshot(risk_row, reference_ts_ms=int(reference_ts_ms))
    if actionable_orders and not bool(risk_summary.get("present")):
        _add_finding("error", "portfolio_risk_snapshot_missing", "missing portfolio risk snapshot for actionable batch")
    if bool(risk_summary.get("present")):
        if actionable_orders and bool(risk_summary.get("stale")):
            _add_finding(
                "warning",
                "portfolio_risk_snapshot_stale",
                "portfolio risk snapshot is older than allowed for actionable batch",
                age_ms=risk_summary.get("age_ms"),
            )
        if actionable_orders and not bool(risk_summary.get("reconciliation_present")):
            _add_finding(
                "error",
                "portfolio_risk_reconciliation_missing",
                "portfolio risk snapshot is missing allocation reconciliation",
            )

    severity_counts: Dict[str, int] = {}
    for finding in findings:
        sev = str((finding or {}).get("severity") or "warning")
        severity_counts[sev] = int(severity_counts.get(sev, 0)) + 1

    payload = {
        "window": {
            "after_event_id": int(after_event_id),
            "last_event_id": int(snap.get("last_event_id") or 0),
            "min_ts_ms": int(snap.get("min_ts_ms") or 0),
            "max_ts_ms": int(snap.get("max_ts_ms") or 0),
            "reference_ts_ms": int(reference_ts_ms),
            "latest_order_ts_ms": int(latest_order_ts_ms),
            "batch_lo_ts_ms": int(batch_lo_ts_ms),
        },
        "summary": {
            "persisted_counts": dict(snap.get("counts") or {}),
            "latest_order_count": int(len(latest_orders)),
            "actionable_order_count": int(len(actionable_orders)),
            "strategy_allocator": strategy_summary,
            "competition_capital_plan": competition_summary,
            "latest_portfolio_orders": latest_orders_summary,
            "execution_intents": intents_summary,
            "portfolio_risk": risk_summary,
        },
        "findings": findings,
    }
    return {
        "ok": True,
        "passed": bool(severity_counts.get("error", 0) == 0),
        "after_event_id": int(after_event_id),
        "last_event_id": int(snap.get("last_event_id") or 0),
        "min_ts_ms": int(snap.get("min_ts_ms") or 0),
        "max_ts_ms": int(snap.get("max_ts_ms") or 0),
        "summary": payload["summary"],
        "findings": findings,
        "severity_counts": severity_counts,
        "digest": _stable_digest(payload),
    }


def replay_capital_reconciliation_determinism_snapshot(
    *,
    after_event_id: int = 0,
    limit: int = 100000,
    batch_window_ms: int = _CAPITAL_RECON_BATCH_WINDOW_MS,
) -> Dict[str, Any]:
    snap_a = replay_capital_reconciliation_snapshot(
        after_event_id=int(after_event_id),
        limit=int(limit),
        batch_window_ms=int(batch_window_ms),
    )
    snap_b = replay_capital_reconciliation_snapshot(
        after_event_id=int(after_event_id),
        limit=int(limit),
        batch_window_ms=int(batch_window_ms),
    )
    digest_a = str(snap_a.get("digest") or "")
    digest_b = str(snap_b.get("digest") or "")
    return {
        "ok": bool(snap_a.get("ok")) and bool(snap_b.get("ok")),
        "deterministic": bool(digest_a == digest_b),
        "after_event_id": int(after_event_id),
        "limit": int(limit),
        "digest": digest_a,
        "digest_match": bool(digest_a == digest_b),
        "passed": bool(snap_a.get("passed", False)),
        "severity_counts": dict(snap_a.get("severity_counts") or {}),
        "last_event_id": int(snap_a.get("last_event_id") or 0),
    }


def replay_model_predictions_snapshot(
    *,
    after_event_id: int = 0,
    limit_events: int = 5,
    symbol_limit: int = 12,
    horizons: Optional[List[int]] = None,
    top_k: int = 8,
) -> Dict[str, Any]:
    try:
        from engine.strategy.predictor import predict_event
        from engine.data.universe import get_active_symbols
    except Exception as e:
        _warn_nonfatal(
            "EVENT_REPLAY_PREDICTOR_IMPORT_FAILED",
            e,
            once_key="event_replay_predictor_import_failed",
        )
        return {"ok": False, "error": f"predictor_import_failed:{type(e).__name__}:{e}"}

    con = connect(readonly=True)
    try:
        try:
            symbols = get_active_symbols(con, limit=int(symbol_limit)) or []
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_SYMBOLS_LOAD_FAILED",
                e,
                once_key="event_replay_symbols_load_failed",
                symbol_limit=int(symbol_limit),
            )
            symbols = []
        if not symbols:
            symbols = ["SPY", "BTC", "OIL"][: max(1, int(symbol_limit))]

        rows = con.execute(
            """
            SELECT e.id, e.ts_ms, e.source, e.title, e.body, e.url, e.meta_json, emb.dim, emb.vec
            FROM events e
            JOIN event_embeddings emb ON emb.event_id = e.id
            WHERE e.id > ?
            ORDER BY e.id ASC
            LIMIT ?
            """,
            (int(after_event_id), int(max(1, limit_events))),
        ).fetchall()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_CLOSE_FAILED",
                e,
                once_key="model_predictions_close",
                after_event_id=int(after_event_id),
            )

    hs = [int(h) for h in (horizons or [300, 3600]) if int(h) > 0]
    out_rows: List[Dict[str, Any]] = []
    last_event_id = int(after_event_id)

    for row in rows or []:
        event_id = int(row[0] or 0)
        last_event_id = int(max(last_event_id, event_id))
        ts_ms = int(row[1] or 0)
        source = str(row[2] or "")
        title = str(row[3] or "")
        body = str(row[4] or "")
        url = str(row[5] or "")
        meta = _safe_json_loads(row[6])
        dim = int(row[7] or 0)
        blob = row[8]
        if dim <= 0 or not blob:
            continue
        try:
            import numpy as np

            vec = np.frombuffer(blob, dtype=np.float32)
            if int(vec.size) != dim:
                continue
            vec = vec.astype(np.float32, copy=False)
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_EMBED_VECTOR_PARSE_FAILED",
                e,
                once_key=f"event_replay_embed_vector:{event_id}",
                event_id=int(event_id),
                embedding_dim=int(dim),
            )
            continue

        event_ctx = {
            "event_id": int(event_id),
            "ts_ms": int(ts_ms),
            "source": source,
            "title": title,
            "body": body,
            "url": url,
            "meta": meta if isinstance(meta, dict) else {},
        }
        preds = predict_event(
            vec,
            list(symbols),
            list(hs),
            top_k=int(top_k),
            event=event_ctx,
        )
        pred_rows = []
        for sym in symbols:
            for h in hs:
                z, conf, explain = preds.get((str(sym), int(h)), (0.0, 0.0, {}))
                ex = dict(explain or {})
                pred_rows.append(
                    {
                        "symbol": str(sym),
                        "horizon_s": int(h),
                        "predicted_z": float(z),
                        "confidence": float(conf),
                        "model_name": str(ex.get("model_name") or ex.get("model") or ""),
                        "model_id": str(ex.get("model_id") or ""),
                        "model_version": str(ex.get("model_version") or ""),
                        "regime": str(ex.get("regime_at_trade") or ex.get("regime") or ""),
                    }
                )
        out_rows.append(
            {
                "event_id": int(event_id),
                "ts_ms": int(ts_ms),
                "title": str(title)[:120],
                "predictions": pred_rows,
            }
        )

    payload = {
        "after_event_id": int(after_event_id),
        "last_event_id": int(last_event_id),
        "symbols": list(symbols),
        "horizons": list(hs),
        "events": out_rows,
    }
    return {
        "ok": True,
        "after_event_id": int(after_event_id),
        "last_event_id": int(last_event_id),
        "symbol_count": int(len(symbols)),
        "event_count": int(len(out_rows)),
        "horizons": list(hs),
        "digest": _stable_digest(payload),
        "events": out_rows,
    }


def replay_model_predictions_determinism_snapshot(
    *,
    after_event_id: int = 0,
    limit_events: int = 5,
    symbol_limit: int = 12,
    horizons: Optional[List[int]] = None,
    top_k: int = 8,
) -> Dict[str, Any]:
    snap_a = replay_model_predictions_snapshot(
        after_event_id=int(after_event_id),
        limit_events=int(limit_events),
        symbol_limit=int(symbol_limit),
        horizons=horizons,
        top_k=int(top_k),
    )
    snap_b = replay_model_predictions_snapshot(
        after_event_id=int(after_event_id),
        limit_events=int(limit_events),
        symbol_limit=int(symbol_limit),
        horizons=horizons,
        top_k=int(top_k),
    )
    digest_a = str(snap_a.get("digest") or "")
    digest_b = str(snap_b.get("digest") or "")
    return {
        "ok": bool(snap_a.get("ok")) and bool(snap_b.get("ok")),
        "deterministic": bool(digest_a == digest_b),
        "after_event_id": int(after_event_id),
        "limit_events": int(limit_events),
        "symbol_limit": int(symbol_limit),
        "event_count": int(snap_a.get("event_count") or 0),
        "last_event_id": int(snap_a.get("last_event_id") or 0),
        "digest": digest_a,
        "digest_match": bool(digest_a == digest_b),
    }


def replay_competition_window(
    *,
    lookback_events: int = 5000,
) -> Dict[str, Any]:
    flush_event_log_buffer(max_batches=64)
    con = connect()
    try:
        max_event_id = 0
        try:
            row = con.execute("SELECT MAX(id) FROM event_log").fetchone()
            max_event_id = int((row or [0])[0] or 0)
        except Exception:
            max_event_id = 0
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_CLOSE_FAILED",
                e,
                once_key="competition_window_close",
            )

    after_event_id = max(0, int(max_event_id) - int(max(1, lookback_events)))
    state = replay_state(after_event_id=after_event_id, limit=int(lookback_events))
    return {
        "ok": True,
        "after_event_id": int(after_event_id),
        "last_event_id": int(state.get("last_event_id") or 0),
        "decision_count": int(len(state.get("decisions") or [])),
        "order_count": int(len(state.get("orders") or {})),
        "fill_count": int(len(state.get("fills") or {})),
        "risk_block_count": int(len(state.get("risk_blocks") or {})),
        "error_count": int(len(state.get("errors") or [])),
        "state": state,
    }


def _event_window(*, after_event_id: int, limit: int) -> Dict[str, Any]:
    flush_event_log_buffer(max_batches=64)
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT id, ts_ms
            FROM event_log
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(after_event_id), int(limit)),
        ).fetchall()
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_CLOSE_FAILED",
                e,
                once_key="event_window_close",
                after_event_id=int(after_event_id),
            )

    if not rows:
        return {
            "ok": True,
            "after_event_id": int(after_event_id),
            "last_event_id": int(after_event_id),
            "min_ts_ms": 0,
            "max_ts_ms": 0,
            "event_count": 0,
        }

    min_ts_ms = int(rows[0][1] or 0)
    max_ts_ms = int(rows[-1][1] or 0)
    last_event_id = int(rows[-1][0] or after_event_id)
    return {
        "ok": True,
        "after_event_id": int(after_event_id),
        "last_event_id": int(last_event_id),
        "min_ts_ms": int(min_ts_ms),
        "max_ts_ms": int(max_ts_ms),
        "event_count": int(len(rows)),
    }


def _fetch_predictions(con, *, min_ts_ms: int, max_ts_ms: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
               model_name, model_id, model_version
        FROM predictions
        WHERE ts_ms BETWEEN ? AND ?
        ORDER BY ts_ms ASC, event_id ASC, symbol ASC, horizon_s ASC, id ASC
        """
        ,
        (int(min_ts_ms), int(max_ts_ms)),
    ).fetchall()
    return [
        {
            "ts_ms": int(r[0] or 0),
            "event_id": int(r[1] or 0),
            "symbol": str(r[2] or ""),
            "horizon_s": int(r[3] or 0),
            "predicted_z": float(r[4] or 0.0),
            "confidence": float(r[5] or 0.0),
            "model_name": str(r[6] or ""),
            "model_id": str(r[7] or ""),
            "model_version": str(r[8] or ""),
        }
        for r in (rows or [])
    ]


def _fetch_alerts(con, *, min_ts_ms: int, max_ts_ms: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT id, ts_ms, symbol, horizon_s, expected_z, confidence, severity,
               rule_id, model_name, model_id, model_version, dedupe_key, explain_json
        FROM alerts
        WHERE ts_ms BETWEEN ? AND ?
        ORDER BY ts_ms ASC, symbol ASC, horizon_s ASC, id ASC
        """,
        (int(min_ts_ms), int(max_ts_ms)),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        event_id = _alert_event_id({"id": r[0], "explain_json": r[12]})
        out.append(
            {
                "id": int(r[0] or 0),
                "ts_ms": int(r[1] or 0),
                "symbol": str(r[2] or ""),
                "horizon_s": int(r[3] or 0),
                "expected_z": float(r[4] or 0.0),
                "confidence": float(r[5] or 0.0),
                "severity": str(r[6] or ""),
                "rule_id": str(r[7] or ""),
                "model_name": str(r[8] or ""),
                "model_id": str(r[9] or ""),
                "model_version": str(r[10] or ""),
                "dedupe_key": str(r[11] or ""),
                "event_id": int(event_id) if event_id is not None else None,
            }
        )
    return out


def _fetch_portfolio_orders(con, *, min_ts_ms: int, max_ts_ms: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT id, ts_ms, model_id, symbol, action, from_side, to_side,
               from_weight, to_weight, delta_weight, source_alert_id, explain_json
        FROM portfolio_orders
        WHERE ts_ms BETWEEN ? AND ?
        ORDER BY ts_ms ASC, id ASC
        """,
        (int(min_ts_ms), int(max_ts_ms)),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        explain = _safe_json_loads(r[11])
        reason_obj = explain.get("reason")
        strategy_data = explain.get("strategy")
        execution_data = explain.get("execution")
        reason = dict(reason_obj) if isinstance(reason_obj, dict) else {}
        strategy_obj = dict(strategy_data) if isinstance(strategy_data, dict) else {}
        execution_obj = dict(execution_data) if isinstance(execution_data, dict) else {}
        competition_obj = reason.get("competition")
        competition = dict(competition_obj) if isinstance(competition_obj, dict) else {}
        competition_policy_obj = competition.get("policy")
        competition_policy = dict(competition_policy_obj) if isinstance(competition_policy_obj, dict) else {}
        execution_alloc_obj = execution_obj.get("strategy_alloc")
        reason_alloc_obj = reason.get("strategy_alloc")
        strategy_alloc = (
            dict(execution_alloc_obj)
            if isinstance(execution_alloc_obj, dict)
            else (dict(reason_alloc_obj) if isinstance(reason_alloc_obj, dict) else {})
        )
        out.append(
            {
                "id": int(r[0] or 0),
                "ts_ms": int(r[1] or 0),
                "model_id": str(r[2] or ""),
                "symbol": str(r[3] or ""),
                "action": str(r[4] or ""),
                "from_side": str(r[5] or ""),
                "to_side": str(r[6] or ""),
                "from_weight": float(r[7] or 0.0),
                "to_weight": float(r[8] or 0.0),
                "delta_weight": float(r[9] or 0.0),
                "source_alert_id": int(r[10] or 0) if r[10] is not None else None,
                "model_version": str(explain.get("model_version") or ""),
                "model_name": str(explain.get("model_name") or competition.get("model_name") or ""),
                "regime": str(explain.get("regime") or competition.get("regime") or competition_policy.get("regime") or "global"),
                "horizon_s": _safe_int(
                    competition.get("horizon_s")
                    if competition.get("horizon_s") is not None
                    else explain.get("horizon_s"),
                    0,
                ),
                "strategy_name": str(strategy_obj.get("name") or reason.get("strategy") or ""),
                "strategy_alloc": dict(strategy_alloc or {}),
                "competition_reason_code": str(competition.get("reason_code") or ""),
                "competition_capital_applied_upstream": bool(competition.get("capital_applied_upstream")),
                "competition_policy": dict(competition_policy or {}),
            }
        )
    return out


def _fetch_execution_orders(con, *, min_ts_ms: int, max_ts_ms: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT client_order_id, portfolio_orders_id, source_alert_id, model_id, model_version,
               symbol, qty, submit_ts_ms, broker, status
        FROM execution_orders
        WHERE submit_ts_ms BETWEEN ? AND ?
        ORDER BY submit_ts_ms ASC, client_order_id ASC
        """,
        (int(min_ts_ms), int(max_ts_ms)),
    ).fetchall()
    return [
        {
            "client_order_id": str(r[0] or ""),
            "portfolio_orders_id": int(r[1] or 0) if r[1] is not None else None,
            "source_alert_id": int(r[2] or 0) if r[2] is not None else None,
            "model_id": str(r[3] or ""),
            "model_version": str(r[4] or ""),
            "symbol": str(r[5] or ""),
            "qty": float(r[6] or 0.0),
            "submit_ts_ms": int(r[7] or 0),
            "broker": str(r[8] or ""),
            "status": str(r[9] or ""),
        }
        for r in (rows or [])
    ]


def _fetch_execution_fills(con, *, min_ts_ms: int, max_ts_ms: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT client_order_id, fill_id, broker, model_id, model_version, symbol,
               fill_ts_ms, fill_qty, fill_px, expected_px, slippage_bps, fees
        FROM execution_fills
        WHERE fill_ts_ms BETWEEN ? AND ?
        ORDER BY fill_ts_ms ASC, client_order_id ASC, id ASC
        """,
        (int(min_ts_ms), int(max_ts_ms)),
    ).fetchall()
    return [
        {
            "client_order_id": str(r[0] or ""),
            "fill_id": str(r[1] or ""),
            "broker": str(r[2] or ""),
            "model_id": str(r[3] or ""),
            "model_version": str(r[4] or ""),
            "symbol": str(r[5] or ""),
            "fill_ts_ms": int(r[6] or 0),
            "fill_qty": float(r[7] or 0.0),
            "fill_px": float(r[8] or 0.0),
            "expected_px": float(r[9] or 0.0) if r[9] is not None else None,
            "slippage_bps": float(r[10] or 0.0) if r[10] is not None else None,
            "fees": float(r[11] or 0.0) if r[11] is not None else None,
        }
        for r in (rows or [])
    ]


def _fetch_pnl_attribution(con, *, min_ts_ms: int, max_ts_ms: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
               slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl
        FROM pnl_attribution
        WHERE ts_ms BETWEEN ? AND ?
        ORDER BY ts_ms ASC, source_alert_id ASC, model_id ASC, symbol ASC
        """,
        (int(min_ts_ms), int(max_ts_ms)),
    ).fetchall()
    return [
        {
            "ts_ms": int(r[0] or 0),
            "source_alert_id": int(r[1] or 0),
            "model_id": str(r[2] or ""),
            "model_version": str(r[3] or ""),
            "symbol": str(r[4] or ""),
            "pnl": float(r[5] or 0.0),
            "fees": float(r[6] or 0.0),
            "slippage_bps": float(r[7] or 0.0) if r[7] is not None else None,
            "position_size": float(r[8] or 0.0) if r[8] is not None else None,
            "avg_price": float(r[9] or 0.0) if r[9] is not None else None,
            "realized_pnl": float(r[10] or 0.0) if r[10] is not None else None,
            "unrealized_pnl": float(r[11] or 0.0) if r[11] is not None else None,
        }
        for r in (rows or [])
    ]


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "EVENT_REPLAY_SAFE_INT_FAILED",
            e,
            once_key="safe_int_failed",
            value_type=type(value).__name__,
        )
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "EVENT_REPLAY_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float_failed",
            value_type=type(value).__name__,
        )
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _is_finite_nonnegative(value: Any) -> bool:
    if value is None:
        return False
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "EVENT_REPLAY_FINITE_NONNEGATIVE_PARSE_FAILED",
            e,
            once_key="finite_nonnegative_parse_failed",
            value_type=type(value).__name__,
        )
        return False
    return bool(math.isfinite(out) and out >= 0.0)


def _approx_le(lhs: float, rhs: float, tol: float = _CAPITAL_RECON_WEIGHT_TOL) -> bool:
    return float(lhs) <= float(rhs) + float(tol)


def _load_runtime_meta_json(con, key: str) -> Dict[str, Any]:
    try:
        row = con.execute(
            "SELECT value, updated_ts_ms FROM runtime_meta WHERE key=? LIMIT 1",
            (str(key),),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return {}
    obj = _safe_json_loads(row[0])
    if not isinstance(obj, dict):
        obj = {}
    obj.setdefault("_runtime_meta_updated_ts_ms", _safe_int(row[1], 0))
    return obj


def _load_latest_strategy_allocations_row(con, *, reference_ts_ms: int) -> Dict[str, Any]:
    row = None
    try:
        row = con.execute(
            """
            SELECT ts_ms, window_days, allocations_json, reason_json
            FROM strategy_allocations
            WHERE ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (int(reference_ts_ms),),
        ).fetchone()
    except Exception:
        row = None
    if row is None:
        try:
            row = con.execute(
                """
                SELECT ts_ms, window_days, allocations_json, reason_json
                FROM strategy_allocations
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        except Exception:
            row = None
    if not row:
        return {}
    return {
        "ts_ms": _safe_int(row[0], 0),
        "window_days": _safe_int(row[1], 0),
        "allocations": _safe_json_loads(row[2]),
        "reason": _safe_json_loads(row[3]),
    }


def _load_latest_portfolio_risk_snapshot(con, *, reference_ts_ms: int) -> Dict[str, Any]:
    row = None
    try:
        row = con.execute(
            """
            SELECT ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json
            FROM portfolio_risk_snapshots
            WHERE ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (int(reference_ts_ms),),
        ).fetchone()
    except Exception:
        row = None
    if row is None:
        try:
            row = con.execute(
                """
                SELECT ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json
                FROM portfolio_risk_snapshots
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        except Exception:
            row = None
    if not row:
        return {}
    return {
        "ts_ms": _safe_int(row[0], 0),
        "gross": _safe_float(row[1], 0.0),
        "net": _safe_float(row[2], 0.0),
        "vol_proxy": (_safe_float(row[3], 0.0) if row[3] is not None else None),
        "drawdown": (_safe_float(row[4], 0.0) if row[4] is not None else None),
        "blocked": bool(_safe_int(row[5], 0)),
        "info": _safe_json_loads(row[6]),
    }


def _summarize_strategy_allocator_row(row: Dict[str, Any], *, reference_ts_ms: int) -> Dict[str, Any]:
    if not isinstance(row, dict) or not row:
        return {"present": False}
    allocations_obj = row.get("allocations")
    allocations = dict(allocations_obj) if isinstance(allocations_obj, dict) else {}
    invalid_weight_keys = [
        str(name)
        for name, value in allocations.items()
        if not _is_finite_nonnegative(value)
    ]
    allocation_sum = sum(_safe_float(value, 0.0) for value in allocations.values())
    age_ms = max(0, int(reference_ts_ms) - int(_safe_int(row.get("ts_ms"), 0))) if int(reference_ts_ms) > 0 else 0
    return {
        "present": True,
        "ts_ms": _safe_int(row.get("ts_ms"), 0),
        "window_days": _safe_int(row.get("window_days"), 0),
        "strategy_count": int(len(allocations)),
        "allocation_sum": float(allocation_sum),
        "normalized": bool(abs(float(allocation_sum) - 1.0) <= float(_CAPITAL_RECON_WEIGHT_TOL) if allocations else True),
        "invalid_weight_keys": invalid_weight_keys,
        "age_ms": int(age_ms),
        "stale": bool(age_ms > int(_CAPITAL_RECON_STALE_MS)),
    }


def _summarize_competition_capital_plan(plan: Dict[str, Any], *, reference_ts_ms: int) -> Dict[str, Any]:
    if not isinstance(plan, dict) or not plan:
        return {"present": False}
    allocations_obj = plan.get("allocations")
    allocations = dict(allocations_obj) if isinstance(allocations_obj, dict) else {}
    total_capital_fraction = _safe_float(
        plan.get("competition_total_capital_fraction"),
        1.0,
    )
    total_group_budget_fraction = _safe_float(
        plan.get("total_group_budget_fraction_post"),
        sum(_safe_float((alloc or {}).get("group_budget_fraction"), 0.0) for alloc in allocations.values() if isinstance(alloc, dict)),
    )
    updated_ts_ms = _safe_int(plan.get("updated_ts_ms"), _safe_int(plan.get("_runtime_meta_updated_ts_ms"), 0))
    max_age_ms = max(
        0,
        _safe_int(plan.get("max_age_ms"), _safe_int(plan.get("capital_plan_max_age_ms"), 15000)),
    )
    age_ms = max(0, int(reference_ts_ms) - int(updated_ts_ms)) if int(reference_ts_ms) > 0 and updated_ts_ms > 0 else 0
    invalid_groups: List[Dict[str, Any]] = []
    model_count = 0
    for group_key, alloc in allocations.items():
        if not isinstance(alloc, dict):
            invalid_groups.append({"group_key": str(group_key), "reason": "group_not_dict"})
            continue
        group_budget = _safe_float(alloc.get("group_budget_fraction"), 0.0)
        models = list(alloc.get("models") or [])
        alloc_sum = sum(_safe_float((row or {}).get("allocation_fraction"), 0.0) for row in models)
        eff_sum = sum(_safe_float((row or {}).get("effective_allocation_fraction"), 0.0) for row in models)
        model_count += int(len(models))
        if (models and abs(float(alloc_sum) - 1.0) > float(_CAPITAL_RECON_WEIGHT_TOL)) or (
            models and abs(float(eff_sum) - float(group_budget)) > float(_CAPITAL_RECON_WEIGHT_TOL)
        ):
            invalid_groups.append(
                {
                    "group_key": str(group_key),
                    "allocation_sum": float(alloc_sum),
                    "effective_sum": float(eff_sum),
                    "group_budget_fraction": float(group_budget),
                }
            )
    return {
        "present": True,
        "updated_ts_ms": int(updated_ts_ms),
        "age_ms": int(age_ms),
        "max_age_ms": int(max_age_ms),
        "stale": bool(max_age_ms > 0 and age_ms > max_age_ms),
        "group_count": int(len(allocations)),
        "model_count": int(model_count),
        "competition_total_capital_fraction": float(total_capital_fraction),
        "total_group_budget_fraction": float(total_group_budget_fraction),
        "normalized": bool(_approx_le(float(total_group_budget_fraction), float(total_capital_fraction))),
        "invalid_groups": invalid_groups,
    }


def _summarize_latest_portfolio_orders(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    actionable = [
        dict(row or {})
        for row in (orders or [])
        if abs(_safe_float((row or {}).get("to_weight"), 0.0)) > float(_CAPITAL_RECON_MIN_ACTIONABLE_WEIGHT)
        and str((row or {}).get("to_side") or "").upper() != "FLAT"
    ]
    missing_strategy_count = 0
    missing_model_count = 0
    missing_competition_count = 0
    unauthoritative_competition_count = 0
    model_exposure: Dict[Tuple[str, str], float] = {}
    model_budget: Dict[Tuple[str, str], float] = {}
    group_exposure: Dict[str, float] = {}
    group_budget: Dict[str, float] = {}
    for row in actionable:
        strategy_name = str((row or {}).get("strategy_name") or "").strip()
        model_name = str((row or {}).get("model_name") or "").strip()
        regime = str((row or {}).get("regime") or "global").strip() or "global"
        policy = dict((row or {}).get("competition_policy") or {})
        reason_code = str((row or {}).get("competition_reason_code") or "")
        group_key = str(
            policy.get("group_key")
            or "|".join(
                [
                    str((row or {}).get("symbol") or "").upper().strip(),
                    str(_safe_int((row or {}).get("horizon_s"), 0)),
                    str(regime),
                ]
            )
        ).strip()
        to_weight = abs(_safe_float((row or {}).get("to_weight"), 0.0))
        if not strategy_name:
            missing_strategy_count += 1
        if not model_name:
            missing_model_count += 1
        if model_name and (not isinstance(policy, dict) or not policy or not str(group_key).strip()):
            missing_competition_count += 1
        if model_name and to_weight > float(_CAPITAL_RECON_MIN_ACTIONABLE_WEIGHT) and reason_code != "competition_capital_applied":
            unauthoritative_competition_count += 1
        if model_name:
            mk = (str(model_name), str(regime))
            model_exposure[mk] = float(model_exposure.get(mk, 0.0) + to_weight)
            if _is_finite_nonnegative(policy.get("model_budget_fraction")):
                model_budget[mk] = max(float(model_budget.get(mk, 0.0)), _safe_float(policy.get("model_budget_fraction"), 0.0))
        if group_key:
            group_exposure[group_key] = float(group_exposure.get(group_key, 0.0) + to_weight)
            if _is_finite_nonnegative(policy.get("group_budget_fraction")):
                group_budget[group_key] = max(float(group_budget.get(group_key, 0.0)), _safe_float(policy.get("group_budget_fraction"), 0.0))

    model_budget_overages = [
        {
            "model_name": str(key[0]),
            "regime": str(key[1]),
            "exposure": float(exposure),
            "budget": float(model_budget.get(key, 0.0)),
        }
        for key, exposure in sorted(model_exposure.items())
        if key in model_budget and not _approx_le(float(exposure), float(model_budget.get(key, 0.0)))
    ]
    group_budget_overages = [
        {
            "group_key": str(key),
            "exposure": float(exposure),
            "budget": float(group_budget.get(key, 0.0)),
        }
        for key, exposure in sorted(group_exposure.items())
        if key in group_budget and not _approx_le(float(exposure), float(group_budget.get(key, 0.0)))
    ]
    return {
        "order_count": int(len(orders or [])),
        "actionable_order_count": int(len(actionable)),
        "missing_strategy_count": int(missing_strategy_count),
        "missing_model_count": int(missing_model_count),
        "missing_competition_count": int(missing_competition_count),
        "unauthoritative_competition_count": int(unauthoritative_competition_count),
        "model_budget_overages": model_budget_overages,
        "group_budget_overages": group_budget_overages,
    }


def _summarize_execution_intents_result(intents_res: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(intents_res, dict) or not intents_res:
        return {"ok": False, "error": "execution_intents_unavailable"}
    if not bool(intents_res.get("ok")):
        return {"ok": False, "error": str(intents_res.get("error") or "execution_intents_error")}
    intents = list(intents_res.get("intents") or [])
    real_intents = [dict(row or {}) for row in intents if str((row or {}).get("execution_target") or "real") == "real"]
    shadow_intents = [dict(row or {}) for row in intents if str((row or {}).get("execution_target") or "real") == "shadow"]
    model_exposure: Dict[Tuple[str, str], float] = {}
    model_budget: Dict[Tuple[str, str], float] = {}
    group_exposure: Dict[str, float] = {}
    group_budget: Dict[str, float] = {}
    negative_remaining_budget: List[Dict[str, Any]] = []
    resize_count = 0
    blocked_count = 0
    for row in real_intents:
        competition = dict((row or {}).get("competition") or {})
        model_name = str((row or {}).get("model_name") or "").strip()
        regime = str(competition.get("regime") or (row or {}).get("regime") or "global").strip() or "global"
        group_key = str(competition.get("group_key") or "").strip()
        to_weight = abs(_safe_float((row or {}).get("to_weight"), 0.0))
        if competition.get("resize_reason"):
            resize_count += 1
        if bool(competition.get("blocked")) or str((row or {}).get("competition_capital_block_reason") or "").strip():
            blocked_count += 1
        remaining_budget = competition.get("remaining_budget_fraction")
        remaining_group_budget = competition.get("remaining_group_budget_fraction")
        if remaining_budget is not None and _safe_float(remaining_budget, 0.0) < -float(_CAPITAL_RECON_WEIGHT_TOL):
            negative_remaining_budget.append(
                {"scope": "model", "model_name": str(model_name), "remaining_budget_fraction": _safe_float(remaining_budget, 0.0)}
            )
        if remaining_group_budget is not None and _safe_float(remaining_group_budget, 0.0) < -float(_CAPITAL_RECON_WEIGHT_TOL):
            negative_remaining_budget.append(
                {"scope": "group", "group_key": str(group_key), "remaining_group_budget_fraction": _safe_float(remaining_group_budget, 0.0)}
            )
        if model_name:
            mk = (str(model_name), str(regime))
            model_exposure[mk] = float(model_exposure.get(mk, 0.0) + to_weight)
            if _is_finite_nonnegative(competition.get("model_budget_fraction")):
                model_budget[mk] = max(float(model_budget.get(mk, 0.0)), _safe_float(competition.get("model_budget_fraction"), 0.0))
        if group_key:
            group_exposure[group_key] = float(group_exposure.get(group_key, 0.0) + to_weight)
            if _is_finite_nonnegative(competition.get("group_budget_fraction")):
                group_budget[group_key] = max(float(group_budget.get(group_key, 0.0)), _safe_float(competition.get("group_budget_fraction"), 0.0))

    model_budget_overages = [
        {
            "model_name": str(key[0]),
            "regime": str(key[1]),
            "exposure": float(exposure),
            "budget": float(model_budget.get(key, 0.0)),
        }
        for key, exposure in sorted(model_exposure.items())
        if key in model_budget and not _approx_le(float(exposure), float(model_budget.get(key, 0.0)))
    ]
    group_budget_overages = [
        {
            "group_key": str(key),
            "exposure": float(exposure),
            "budget": float(group_budget.get(key, 0.0)),
        }
        for key, exposure in sorted(group_exposure.items())
        if key in group_budget and not _approx_le(float(exposure), float(group_budget.get(key, 0.0)))
    ]
    return {
        "ok": True,
        "batch_id": _safe_int(intents_res.get("batch_id"), 0),
        "batch_ts_ms": _safe_int(intents_res.get("batch_ts_ms"), 0),
        "intent_count": int(len(intents)),
        "real_count": int(len(real_intents)),
        "shadow_count": int(len(shadow_intents)),
        "resize_count": int(resize_count),
        "blocked_count": int(blocked_count),
        "model_budget_overages": model_budget_overages,
        "group_budget_overages": group_budget_overages,
        "negative_remaining_budget": negative_remaining_budget,
    }


def _summarize_portfolio_risk_snapshot(row: Dict[str, Any], *, reference_ts_ms: int) -> Dict[str, Any]:
    if not isinstance(row, dict) or not row:
        return {"present": False}
    info_obj = row.get("info")
    info = dict(info_obj) if isinstance(info_obj, dict) else {}
    reconciliation_obj = info.get("allocation_reconciliation")
    reconciliation = dict(reconciliation_obj) if isinstance(reconciliation_obj, dict) else {}
    by_strategy_obj = reconciliation.get("by_strategy")
    by_model_obj = reconciliation.get("by_model")
    by_strategy = dict(by_strategy_obj) if isinstance(by_strategy_obj, dict) else {}
    by_model = dict(by_model_obj) if isinstance(by_model_obj, dict) else {}
    ts_ms = _safe_int(row.get("ts_ms"), 0)
    age_ms = max(0, int(reference_ts_ms) - int(ts_ms)) if int(reference_ts_ms) > 0 and ts_ms > 0 else 0
    return {
        "present": True,
        "ts_ms": int(ts_ms),
        "age_ms": int(age_ms),
        "stale": bool(age_ms > int(_CAPITAL_RECON_STALE_MS)),
        "blocked": bool(row.get("blocked")),
        "final_gross": _safe_float((info or {}).get("final_gross"), _safe_float(row.get("gross"), 0.0)),
        "final_net": _safe_float((info or {}).get("final_net"), _safe_float(row.get("net"), 0.0)),
        "reconciliation_present": bool(reconciliation),
        "strategy_reconciliation_count": int(len(by_strategy)),
        "model_reconciliation_count": int(len(by_model)),
    }


def _safe_json_loads(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        obj = json.loads(value)
    except Exception as e:
        _warn_nonfatal(
            "EVENT_REPLAY_JSON_LOAD_FAILED",
            e,
            once_key=f"event_replay_json_load:{str(value)[:96]}",
        )
        return {}
    return dict(obj) if isinstance(obj, dict) else {}


def _alert_event_id(row: Dict[str, Any]) -> Optional[int]:
    explain = _safe_json_loads(row.get("explain_json"))
    event_data = explain.get("event")
    event_obj = dict(event_data) if isinstance(event_data, dict) else {}
    for candidate in (
        row.get("event_id"),
        event_obj.get("event_id"),
        explain.get("event_id"),
    ):
        if candidate in (None, "", "None"):
            continue
        try:
            val = int(candidate)
        except Exception as e:
            _warn_nonfatal(
                "EVENT_REPLAY_ALERT_EVENT_ID_PARSE_FAILED",
                e,
                once_key=f"event_replay_alert_event_id:{candidate!r}",
                candidate=repr(candidate),
            )
            continue
        if val > 0:
            return val
    return None


def _alert_id_from_row(row: Dict[str, Any]) -> Optional[int]:
    try:
        val = int(row.get("id") or 0)
    except Exception as e:
        _warn_nonfatal(
            "EVENT_REPLAY_ALERT_ID_PARSE_FAILED",
            e,
            once_key=f"event_replay_alert_id:{row.get('id')!r}",
            alert_id=repr(row.get("id")),
        )
        return None
    return val if val > 0 else None


def _canonicalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _canonicalize(value[k]) for k in sorted(value.keys(), key=lambda x: str(x))}
    if isinstance(value, list):
        return [_canonicalize(v) for v in value]
    return value


def _stable_json(value: Any) -> str:
    return json.dumps(_canonicalize(value), separators=(",", ":"), sort_keys=True, default=str)


def _stable_digest(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()
