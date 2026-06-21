"""Readiness payload assembly for runtime health."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Mapping, Optional


def get_readiness_snapshot(
    health: Optional[Dict[str, Any]] = None,
    preflight: Optional[Dict[str, Any]] = None,
    system_state: Optional[Dict[str, Any]] = None,
    graph: Optional[Dict[str, Any]] = None,
    *,
    environ: Mapping[str, str] = os.environ,
    live_state: str = "LIVE",
) -> Dict[str, Any]:
    ts_ms = int(time.time() * 1000)

    health = dict(health or {})
    preflight = dict(preflight or {})
    system_state = dict(system_state or {})
    graph = dict(graph or {})

    prices = health.get("prices") or {}
    providers = health.get("providers") or {}
    provider_readiness = dict(health.get("provider_readiness") or {}) if isinstance(health.get("provider_readiness"), dict) else {}
    labels = health.get("labels") or {}
    model = health.get("model") or {}
    execution_barrier = health.get("execution_barrier") or {}
    broker_connection = health.get("broker_connection") or {}
    db = health.get("db") or {}
    job_summary = health.get("job_summary") or {}
    startup_validation = dict(health.get("startup_validation") or {}) if isinstance(health.get("startup_validation"), dict) else {}
    timeseries_storage = dict(health.get("timeseries_storage") or {}) if isinstance(health.get("timeseries_storage"), dict) else {}
    feature_store = dict(health.get("feature_store") or {}) if isinstance(health.get("feature_store"), dict) else dict(timeseries_storage.get("feature_store") or {})
    portfolio_runtime = dict(health.get("portfolio_runtime") or {}) if isinstance(health.get("portfolio_runtime"), dict) else {}
    position_reconcile = dict(health.get("position_reconcile") or {}) if isinstance(health.get("position_reconcile"), dict) else {}
    execution_degraded = dict(health.get("execution_degraded") or {}) if isinstance(health.get("execution_degraded"), dict) else {}
    execution_supervisor = dict(health.get("execution_supervisor") or {}) if isinstance(health.get("execution_supervisor"), dict) else {}

    mode_name = str(
        (system_state.get("mode") or system_state.get("execution_mode") or environ.get("ENGINE_MODE") or "safe")
    ).strip().lower() or "safe"

    require_models = mode_name in ("shadow", "live")
    require_risk = mode_name in ("shadow", "live")
    require_broker = mode_name == "live"

    provider_readiness_required = bool(provider_readiness.get("required"))
    provider_readiness_ok = bool((not provider_readiness_required) or provider_readiness.get("ok"))
    data_feed_ok = bool(prices.get("ok")) and bool(providers.get("ok")) and provider_readiness_ok
    models_ok = bool(labels.get("ok")) and bool(model.get("ok"))
    risk_ok = bool(execution_barrier.get("allowed"))
    db_ok = bool(db.get("ok"))
    db_initialized = db_ok and bool(db.get("initialized"))
    jobs_ok = bool(job_summary.get("ok"))
    jobs_running = bool(job_summary.get("total")) and jobs_ok
    require_timeseries = bool(timeseries_storage.get("enabled")) or bool(feature_store.get("enabled"))
    timeseries_ok = (not require_timeseries) or bool(timeseries_storage.get("ok"))
    portfolio_runtime_ok = not bool(portfolio_runtime.get("degraded"))
    position_reconcile_required = mode_name in ("paper", "live")
    position_reconcile_ok = bool((not position_reconcile_required) or position_reconcile.get("ok"))
    position_reconcile_blocking = bool(position_reconcile_required and not position_reconcile_ok)
    execution_degraded_active = bool(execution_degraded.get("active"))
    execution_degraded_severity = str(execution_degraded.get("severity") or "WARNING").strip().upper() or "WARNING"
    execution_degraded_critical = bool(execution_degraded_active and execution_degraded_severity == "CRITICAL")
    execution_supervisor_ok = bool(execution_supervisor.get("ok"))
    execution_supervisor_state = str(execution_supervisor.get("state") or "unknown").strip().lower() or "unknown"
    execution_supervisor_critical = execution_supervisor_state == "critical"
    execution_supervisor_failed_gates = [
        str(name)
        for name in list(execution_supervisor.get("failed_gates") or [])
        if str(name).strip()
    ]

    broker_state = str(broker_connection.get("state") or "").strip().lower()
    broker_ok = bool(broker_connection.get("ok")) and broker_state not in (
        "disconnected",
        "connect_failed",
        "reconnect_failed",
    )

    preflight_ok = bool(preflight.get("ok")) if preflight else True
    graph_ok = bool(graph.get("ok")) if graph else True
    startup_validation_ok = bool(startup_validation.get("ok")) if startup_validation else True
    startup_blocking_gates = [
        str(name)
        for name in list(startup_validation.get("blocking_gates") or startup_validation.get("blocking_checks") or [])
        if str(name).strip()
    ]

    state_name = str(system_state.get("state") or "").strip().upper()
    system_live = state_name == live_state if system_state else False

    issues: List[Dict[str, Any]] = []

    if not db_initialized:
        issues.append({
            "code": "db_not_initialized",
            "level": "error",
            "message": "Database is not initialized.",
            "detail": f"db_ok={db_ok} exists={bool(db.get('exists'))} initialized={bool(db.get('initialized'))} path={db.get('db_path')}",
        })

    if not jobs_running:
        issues.append({
            "code": "jobs_not_running",
            "level": "error",
            "message": "Required startup jobs are not running.",
            "detail": f"total={job_summary.get('total')} stale={job_summary.get('stale')} stale_jobs={job_summary.get('stale_jobs')}",
        })

    if not bool(prices.get("ok")):
        issues.append({
            "code": "no_prices",
            "level": "error",
            "message": "No fresh prices are available.",
            "detail": f"last_ts_ms={prices.get('last_ts_ms')} age_s={prices.get('age_s')} max_age_s={prices.get('max_age_s')}",
        })

    if not data_feed_ok:
        issues.append({
            "code": "data_feed_not_ready",
            "level": "error",
            "message": "Data feed readiness failed.",
            "detail": (
                f"prices_ok={bool(prices.get('ok'))} providers_ok={bool(providers.get('ok'))} "
                f"provider_readiness_ok={provider_readiness_ok} age_s={prices.get('age_s')} "
                f"healthy={providers.get('healthy')}/{providers.get('total')}"
            ),
        })

    if provider_readiness_required and not provider_readiness_ok:
        issues.append({
            "code": "provider_readiness_failed",
            "level": "error",
            "message": "Required provider readiness failed.",
            "detail": (
                f"required_providers={list(provider_readiness.get('required_providers') or [])} "
                f"blockers={list(provider_readiness.get('blockers') or [])}"
            ),
        })

    if require_models and not models_ok:
        issues.append({
            "code": "models_not_ready",
            "level": "error",
            "message": "Models readiness failed.",
            "detail": f"labels_ok={bool(labels.get('ok'))} label_count={labels.get('count')} model_ok={bool(model.get('ok'))} support_n={model.get('support_n')}",
        })

    if require_risk and not risk_ok:
        issues.append({
            "code": "risk_not_ready",
            "level": "error",
            "message": "Risk gate is blocking trading.",
            "detail": str(execution_barrier.get("reason") or "blocked"),
        })

    if require_broker and not broker_ok:
        issues.append({
            "code": "broker_not_ready",
            "level": "error",
            "message": "Broker connection is not ready.",
            "detail": f"state={broker_connection.get('state')} broker={broker_connection.get('broker')}",
        })

    if require_timeseries and not timeseries_ok:
        issues.append({
            "code": "timeseries_storage_not_ready",
            "level": "error",
            "message": "Timeseries storage sidecars are not ready.",
            "detail": (
                f"timeseries_ok={bool(timeseries_storage.get('ok'))} detail={timeseries_storage.get('detail') or 'timeseries_storage_not_ready'} "
                f"timescale_enabled={bool(timeseries_storage.get('enabled'))} feature_store_enabled={bool(feature_store.get('enabled'))}"
            ),
        })

    if not portfolio_runtime_ok:
        issues.append({
            "code": "portfolio_runtime_degraded",
            "level": "error",
            "message": "Portfolio runtime is degraded.",
            "detail": (
                f"detail={portfolio_runtime.get('detail') or 'portfolio_runtime_degraded'} "
                f"codes={list(portfolio_runtime.get('degraded_codes') or [])}"
            ),
        })

    if position_reconcile_blocking:
        issues.append({
            "code": "position_reconcile_failed",
            "level": "error",
            "message": "Persisted position reconcile gate failed.",
            "detail": (
                f"status={position_reconcile.get('status') or 'failed'} "
                f"broker={position_reconcile.get('broker') or 'unknown'} "
                f"mismatched_n={position_reconcile.get('mismatched_n')} "
                f"blockers={list(position_reconcile.get('blockers') or [])} "
                f"detail={position_reconcile.get('detail') or 'position_reconcile_failed'}"
            ),
        })

    if execution_degraded_active:
        issues.append({
            "code": "execution_degraded",
            "level": ("error" if execution_degraded_critical else "warn"),
            "message": "Execution runtime is degraded.",
            "detail": (
                f"severity={execution_degraded_severity} reason={execution_degraded.get('reason') or 'execution_degraded'} "
                f"codes={list(execution_degraded.get('reason_codes') or [])}"
            ),
        })

    if mode_name in ("shadow", "live") and not execution_supervisor_ok:
        issues.append({
            "code": "execution_supervisor_unavailable",
            "level": "error",
            "message": "Execution supervisor snapshot is unavailable.",
            "detail": str(execution_supervisor.get("detail") or "execution_supervisor_unavailable"),
        })

    if execution_supervisor_critical or execution_supervisor_failed_gates:
        issues.append({
            "code": "execution_health_gate_failed",
            "level": "error",
            "message": "Execution safety gates failed.",
            "detail": (
                f"state={execution_supervisor.get('state') or 'unknown'} "
                f"failed_gates={execution_supervisor_failed_gates}"
            ),
        })

    if not preflight_ok:
        issues.append({
            "code": "preflight_failed",
            "level": "error",
            "message": "Startup preflight failed.",
            "detail": "; ".join(str(x) for x in (preflight.get("notes") or [])) or "preflight_failed",
        })

    if not startup_validation_ok:
        issues.append({
            "code": "startup_gates_failed",
            "level": "error",
            "message": "Startup gate validation failed.",
            "detail": (
                f"blocking_gates={startup_blocking_gates}"
                if startup_blocking_gates
                else "; ".join(str(x) for x in (startup_validation.get("reasons") or []))
            ),
        })

    if not graph_ok:
        issues.append({
            "code": "graph_invalid",
            "level": "warn",
            "message": "Runtime dependency graph validation failed.",
            "detail": str(graph.get("error") or "graph_invalid"),
        })

    if system_state and not system_live:
        issues.append({
            "code": "system_state_not_live",
            "level": "warn",
            "message": "System state is not LIVE.",
            "detail": state_name or "UNKNOWN",
        })

    startup_ok = (
        db_ok
        and data_feed_ok
        and jobs_ok
        and timeseries_ok
        and portfolio_runtime_ok
        and position_reconcile_ok
        and (execution_supervisor_ok if mode_name in ("shadow", "live") else True)
        and (not execution_supervisor_critical)
        and (not execution_degraded_critical)
    )

    steps = [
        {
            "id": "database",
            "label": "Verify Database",
            "ok": db_ok,
            "blocked": not db_ok,
            "detail": f"db_ok={db_ok} path={db.get('db_path')} error={db.get('error')}",
        },
        {
            "id": "data_feed",
            "label": "Verify Data Feed",
            "ok": data_feed_ok,
            "blocked": not data_feed_ok,
            "detail": (
                f"prices_ok={bool(prices.get('ok'))} providers={providers.get('healthy')}/{providers.get('total')} "
                f"provider_readiness_ok={provider_readiness_ok} age_s={prices.get('age_s')}"
            ),
        },
        {
            "id": "provider_readiness",
            "label": "Verify Providers",
            "ok": provider_readiness_ok,
            "blocked": bool(provider_readiness_required and not provider_readiness_ok),
            "detail": (
                "not_required"
                if not provider_readiness_required
                else f"required={list(provider_readiness.get('required_providers') or [])} blockers={list(provider_readiness.get('blockers') or [])}"
            ),
        },
        {
            "id": "jobs",
            "label": "Verify Jobs",
            "ok": jobs_ok,
            "blocked": not jobs_ok,
            "detail": f"total={job_summary.get('total')} stale={job_summary.get('stale')} stale_jobs={job_summary.get('stale_jobs')}",
        },
        {
            "id": "models",
            "label": "Verify Models",
            "ok": models_ok,
            "blocked": bool(require_models and not models_ok),
            "detail": f"required={require_models} labels_ok={bool(labels.get('ok'))} label_count={labels.get('count')} model_ok={bool(model.get('ok'))} support_n={model.get('support_n')}",
        },
        {
            "id": "risk",
            "label": "Verify Risk",
            "ok": risk_ok,
            "blocked": bool(require_risk and not risk_ok),
            "detail": f"required={require_risk} execution_allowed={risk_ok} reason={execution_barrier.get('reason') or 'ok'}",
        },
        {
            "id": "broker",
            "label": "Verify Broker",
            "ok": broker_ok,
            "blocked": bool(require_broker and not broker_ok),
            "detail": f"required={require_broker} broker_ok={bool(broker_connection.get('ok'))} state={broker_connection.get('state') or 'unknown'} broker={broker_connection.get('broker') or 'unknown'}",
        },
        {
            "id": "timeseries_storage",
            "label": "Verify Sidecars",
            "ok": timeseries_ok,
            "blocked": bool(require_timeseries and not timeseries_ok),
            "detail": (
                f"required={require_timeseries} timeseries_ok={bool(timeseries_storage.get('ok'))} "
                f"detail={timeseries_storage.get('detail') or 'optional_disabled'}"
            ),
        },
        {
            "id": "portfolio_runtime",
            "label": "Verify Portfolio Runtime",
            "ok": portfolio_runtime_ok,
            "blocked": not portfolio_runtime_ok,
            "detail": (
                f"degraded={bool(portfolio_runtime.get('degraded'))} "
                f"detail={portfolio_runtime.get('detail') or 'ok'}"
            ),
        },
        {
            "id": "position_reconcile",
            "label": "Verify Positions",
            "ok": position_reconcile_ok,
            "blocked": position_reconcile_blocking,
            "detail": (
                f"available={bool(position_reconcile.get('available'))} "
                f"status={position_reconcile.get('status') or 'unavailable'} "
                f"broker={position_reconcile.get('broker') or 'unknown'}"
            ),
        },
        {
            "id": "execution_health",
            "label": "Verify Execution Health",
            "ok": (not execution_degraded_active) and execution_supervisor_ok and (not execution_supervisor_critical),
            "blocked": execution_degraded_critical or execution_supervisor_critical or (mode_name in ("shadow", "live") and not execution_supervisor_ok),
            "detail": (
                f"degraded_active={execution_degraded_active} severity={execution_degraded_severity} "
                f"reason={execution_degraded.get('reason') or 'ok'} "
                f"exec_supervisor_ok={execution_supervisor_ok} "
                f"exec_supervisor_state={execution_supervisor.get('state') or 'unknown'} "
                f"failed_gates={execution_supervisor_failed_gates}"
            ),
        },
        {
            "id": "startup_gates",
            "label": "Verify Startup Gates",
            "ok": startup_validation_ok,
            "blocked": not startup_validation_ok,
            "detail": (
                "All startup gates passed."
                if startup_validation_ok
                else f"blocking_gates={startup_blocking_gates or list(startup_validation.get('reasons') or [])}"
            ),
        },
    ]

    ready_without_state = (
        startup_ok
        and startup_validation_ok
        and preflight_ok
        and graph_ok
        and (models_ok if require_models else True)
        and (risk_ok if require_risk else True)
        and (broker_ok if require_broker else True)
        and (timeseries_ok if require_timeseries else True)
        and portfolio_runtime_ok
        and position_reconcile_ok
        and (execution_supervisor_ok if mode_name in ("shadow", "live") else True)
        and (not execution_supervisor_critical)
        and (not execution_degraded_critical)
    )
    final_ready = ready_without_state and (system_live if system_state else True)

    waiting_on = []
    if not db_ok:
        waiting_on.append("database")
    if not data_feed_ok:
        waiting_on.append("data_feed")
    if provider_readiness_required and not provider_readiness_ok:
        waiting_on.append("provider_readiness")
    if not jobs_ok:
        waiting_on.append("jobs")
    if require_models and not models_ok:
        waiting_on.append("models")
    if require_risk and not risk_ok:
        waiting_on.append("risk")
    if require_broker and not broker_ok:
        waiting_on.append("broker")
    if require_timeseries and not timeseries_ok:
        waiting_on.append("timeseries_storage")
    if not portfolio_runtime_ok:
        waiting_on.append("portfolio_runtime")
    if position_reconcile_blocking:
        waiting_on.append("position_reconcile")
    if execution_degraded_critical:
        waiting_on.append("execution_degraded")
    if mode_name in ("shadow", "live") and not execution_supervisor_ok:
        waiting_on.append("execution_supervisor")
    if execution_supervisor_critical or execution_supervisor_failed_gates:
        waiting_on.append("execution_health")
    if not startup_validation_ok:
        waiting_on.extend(startup_blocking_gates or ["startup_gates"])
    if not preflight_ok:
        waiting_on.append("preflight")
    if not graph_ok:
        waiting_on.append("graph")
    if system_state and not system_live:
        waiting_on.append("system_state")

    steps.append({
        "id": "enable_trading",
        "label": "Enable Trading",
        "ok": final_ready,
        "blocked": not final_ready,
        "detail": "All startup gates passed." if final_ready else f"waiting_on={','.join(waiting_on) or 'system_state'}",
    })

    reasons = []
    for item in issues:
        if str(item.get("level") or "").lower() != "error":
            continue
        reasons.append({
            "code": str(item.get("code") or "unknown"),
            "message": str(item.get("message") or ""),
            "detail": str(item.get("detail") or ""),
        })

    return {
        "ok": final_ready,
        "ready": final_ready,
        "degraded": not final_ready,
        "status": ("READY" if final_ready else "DEGRADED"),
        "ts_ms": ts_ms,
        "mode": mode_name,
        "data_feed_ok": data_feed_ok,
        "provider_readiness_ok": provider_readiness_ok,
        "models_ok": models_ok,
        "risk_ok": risk_ok,
        "broker_ok": broker_ok,
        "timeseries_ok": timeseries_ok,
        "portfolio_runtime_ok": portfolio_runtime_ok,
        "position_reconcile_ok": position_reconcile_ok,
        "preflight_ok": preflight_ok,
        "graph_ok": graph_ok,
        "startup_validation_ok": startup_validation_ok,
        "system_live": system_live,
        "system_state": state_name or "UNKNOWN",
        "position_reconcile": dict(position_reconcile or {}),
        "provider_readiness": provider_readiness,
        "execution_degraded": dict(execution_degraded or {}),
        "startup_validation": startup_validation,
        "waiting_on": waiting_on,
        "issues": issues,
        "reasons": reasons,
        "steps": steps,
    }
