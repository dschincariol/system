"""Serve grounded system, health, and operator-diagnostics HTTP endpoints.

This module owns the dashboard server handlers that expose runtime state,
readiness, execution barriers, and operator support snapshots.
"""

# engine/api/api_system.py
# Route specs for system/health/telemetry endpoints.
# This file contains route metadata + handler implementations.
# It does NOT import dashboard_server.py.

import json
import logging
import os
import time
import threading
import copy
from pathlib import Path

from engine.api.http_parsing import qs as _qs
from engine.api.sql_identifiers import require_allowed_table_name, sql_identifier
from engine.runtime.health import get_health_snapshot, run_preflight, get_readiness_snapshot, get_schema_audit
from engine.runtime.ingestion_status import pipeline_health_summary
from engine.runtime.runtime_meta import meta_get
from engine.runtime.system_state import compute_system_state
from engine.runtime.gates import execution_gate_snapshot
from engine.runtime.ipc import market_data_status
from engine.runtime.storage import connect as _db_connect, connect_ro_direct, DB_PATH, run_write_txn, get_db_debug_snapshot
from engine.runtime.telemetry_read_router import fetch_recent_runtime_failure_events
from engine.runtime.jobs.repair_schema import run as repair_schema
from engine.runtime.job_registry import validate_runtime_architecture
from engine.runtime.failure_diagnostics import failure_response, log_failure, normalize_root_cause_code
from engine.strategy.champion_manager import current_competition_snapshot


log = logging.getLogger(__name__)


_HEALTH_CACHE_LOCK = threading.Lock()
_HEALTH_CACHE = {
    "ts_ms": 0,
    "payload": None,
}
_HEALTH_CACHE_TTL_MS = int(float(os.environ.get("API_HEALTH_CACHE_TTL_S", "3.0")) * 1000.0)
_HEALTH_CACHE_MAX_STALE_MS = int(float(os.environ.get("API_HEALTH_CACHE_MAX_STALE_S", "60.0")) * 1000.0)
_HEALTH_CACHE_REFRESH_LOCK = threading.Lock()
_HEALTH_CACHE_REFRESH_IN_FLIGHT = False
_SYSTEM_SNAPSHOT_CACHE_LOCK = threading.Lock()
_SYSTEM_SNAPSHOT_CACHE = {
    "ts_ms": 0,
    "payload": None,
}
_SYSTEM_SNAPSHOT_CACHE_TTL_MS = int(float(os.environ.get("API_SYSTEM_SNAPSHOT_CACHE_TTL_S", "1.5")) * 1000.0)


def _warn(scope: str, err: Exception, **extra) -> None:
    log_failure(
        log,
        event=str(scope),
        code=normalize_root_cause_code(str(scope)),
        message=str(err),
        error=err,
        level=logging.WARNING,
        component="engine.api.api_system",
        extra=extra or None,
        include_health=False,
        persist=True,
    )


def _failure_out(event: str, code: str, error: BaseException, **extra) -> dict:
    payload = failure_response(
        log,
        event=event,
        code=code,
        message=str(error),
        error=error,
        component="engine.api.api_system",
        extra=extra or None,
    )
    payload.setdefault("error", str(error))
    payload.update(extra or {})
    return payload


# ----------------------------------------------------------------------
# ROUTE DEFINITIONS
# ----------------------------------------------------------------------

ROUTE_SPECS_SYSTEM = [
    ("POST", "/api/system/self_repair", "api_post_self_repair"),
    ("GET",  "/api/system/kill_switches",          "api_get_kill_switches"),
    ("GET",  "/api/system/state",                  "api_get_system_state"),
    ("GET",  "/api/system/health",                 "api_get_health"),
    ("GET",  "/api/system/liveness",               "api_get_liveness"),
    ("GET",  "/api/system/competition",            "api_get_competition_view"),
    ("GET",  "/api/system/replay_freshness",       "api_get_replay_freshness"),
    ("GET",  "/api/system/attribution_quality",    "api_get_attribution_quality"),
    ("GET",  "/api/system/mode",                   "api_get_status"),
    ("GET",  "/api/system/config",                 "api_get_runtime_config"),
    ("GET",  "/api/supervisor/status",             "api_get_supervisor_status"),
    ("GET",  "/api/ingestion/status",              "api_get_ingestion_status"),

    ("GET",  "/api/health",                        "api_get_health"),
    ("GET",  "/api/liveness",                      "api_get_liveness"),
    ("GET",  "/api/status",                        "api_get_status"),
    ("GET",  "/api/readiness",                     "api_get_readiness"),
    ("GET",  "/api/system/trading_readiness",      "api_get_trading_readiness"),
    ("GET",  "/api/operator/trading_readiness",    "api_get_trading_readiness"),
    ("GET",  "/api/operator/preflight_report",     "api_get_preflight_report"),
    ("GET",  "/api/operator/runtime_watchdogs",    "api_get_runtime_watchdogs"),
    ("GET",  "/api/operator/service_status",       "api_get_service_status"),
    ("GET",  "/api/operator/support_snapshot",     "api_get_support_snapshot"),
    ("GET",  "/api/operator/snapshot",             "api_get_support_snapshot"),
    ("GET",  "/api/operator/competition",          "api_get_competition_view"),
    ("GET",  "/api/operator/replay_freshness",     "api_get_replay_freshness"),
    ("GET",  "/api/operator/attribution_quality",  "api_get_attribution_quality"),
    ("GET",  "/api/operator/provider_telemetry",   "api_get_provider_telemetry"),
    ("GET",  "/api/operator/supervisor_diagnostics", "api_get_supervisor_diagnostics"),

    ("POST", "/api/system/self_repair",            "api_post_self_repair"),
    ("POST", "/api/operator/self_repair",          "api_post_self_repair"),
    ("POST", "/api/system/repair_schema",          "api_post_repair_schema"),

    ("GET",  "/api/telemetry",                     "api_get_telemetry"),
    ("GET",  "/api/telemetry/history",             "api_get_telemetry_history"),
    ("GET",  "/api/execution/barrier",             "api_get_execution_barrier"),
    ("GET",  "/api/risk/portfolio",                "api_get_portfolio_risk"),
    ("GET",  "/api/risk/monte_carlo",              "api_get_monte_carlo_risk"),
    ("GET",  "/api/drift/explainer",               "api_get_drift_explainer"),

    ("GET",  "/api/allocator/status",              "api_get_allocator_status"),
    ("GET",  "/api/training_status",               "api_get_training_status"),
    ("GET",  "/api/server/status",                 "api_get_server_status"),
    ("POST", "/api/server/shutdown",               "api_post_server_shutdown"),
]

# ----------------------------------------------------------------------
# SYSTEM STATE
# ----------------------------------------------------------------------
from dataclasses import asdict
from engine.runtime.config_schema import load_runtime_config, ConfigError

def _ts_ms() -> int:
    return int(time.time() * 1000)


def _get_cached_system_snapshot():
    now_ms = _ts_ms()
    ttl_ms = max(0, int(_SYSTEM_SNAPSHOT_CACHE_TTL_MS))
    if ttl_ms <= 0:
        return None

    try:
        with _SYSTEM_SNAPSHOT_CACHE_LOCK:
            cached_ts_ms = int(_SYSTEM_SNAPSHOT_CACHE.get("ts_ms") or 0)
            cached_payload = _SYSTEM_SNAPSHOT_CACHE.get("payload")
            if (
                isinstance(cached_payload, dict)
                and cached_ts_ms > 0
                and (now_ms - cached_ts_ms) <= ttl_ms
            ):
                payload = copy.deepcopy(cached_payload)
                payload["cache_age_ms"] = max(0, now_ms - cached_ts_ms)
                return payload
    except Exception as e:
        _warn("api_system.system_snapshot_cache.read", e)
    return None


def _store_cached_system_snapshot(payload: dict) -> None:
    ttl_ms = max(0, int(_SYSTEM_SNAPSHOT_CACHE_TTL_MS))
    if ttl_ms <= 0 or not isinstance(payload, dict):
        return
    try:
        with _SYSTEM_SNAPSHOT_CACHE_LOCK:
            _SYSTEM_SNAPSHOT_CACHE["ts_ms"] = _ts_ms()
            _SYSTEM_SNAPSHOT_CACHE["payload"] = copy.deepcopy(payload)
    except Exception as e:
        _warn("api_system.system_snapshot_cache.write", e)


def _health_cache_refresh_in_flight() -> bool:
    try:
        with _HEALTH_CACHE_REFRESH_LOCK:
            return bool(_HEALTH_CACHE_REFRESH_IN_FLIGHT)
    except Exception as e:
        _warn("api_system.health_cache.refresh_state", e)
    return False


def _health_cache_placeholder(*, now_ms: int, cache_age_ms: int, cache_populated: bool) -> dict:
    refresh_in_flight = _health_cache_refresh_in_flight()
    reasons = ["health_snapshot_pending"]
    if refresh_in_flight:
        reasons.append("health_snapshot_refresh_in_flight")
    return {
        "ok": False,
        "status": "BOOTING",
        "warming_up": True,
        "error": "health_snapshot_pending",
        "reasons": reasons,
        "ts_ms": int(now_ms),
        "db": {
            "ok": False,
            "status": "UNKNOWN",
            "warming_up": True,
            "detail": "health_snapshot_pending",
        },
        "lifecycle": {
            "state": "WARMING_UP",
            "detail": "health_snapshot_pending",
            "ts_ms": int(now_ms),
        },
        "cache": {
            "source": "api_system_cache",
            "stale": True,
            "age_ms": int(max(0, cache_age_ms)),
            "populated": bool(cache_populated),
            "refresh_in_flight": bool(refresh_in_flight),
        },
    }


def _cached_health_snapshot(*, allow_sync_on_miss: bool = True) -> dict:
    now_ms = _ts_ms()
    cached_payload = None
    cached_ts_ms = 0
    try:
        with _HEALTH_CACHE_LOCK:
            cached_ts_ms = int(_HEALTH_CACHE.get("ts_ms") or 0)
            cached_payload = _HEALTH_CACHE.get("payload")
            if (
                isinstance(cached_payload, dict)
                and cached_ts_ms > 0
                and (now_ms - cached_ts_ms) <= max(100, int(_HEALTH_CACHE_TTL_MS))
            ):
                return dict(cached_payload)
    except Exception as e:
        _warn("api_system.health_cache.read", e)

    cache_age_ms = max(0, now_ms - int(cached_ts_ms or 0)) if cached_ts_ms > 0 else 10**12
    if isinstance(cached_payload, dict) and cached_ts_ms > 0:
        if cache_age_ms <= max(int(_HEALTH_CACHE_TTL_MS), int(_HEALTH_CACHE_MAX_STALE_MS), 100):
            _schedule_health_cache_refresh()
            cached = dict(cached_payload)
            cached.setdefault("cache", {})
            if isinstance(cached.get("cache"), dict):
                cached["cache"] = dict(cached.get("cache") or {})
                cached["cache"]["stale"] = cache_age_ms > max(100, int(_HEALTH_CACHE_TTL_MS))
                cached["cache"]["age_ms"] = int(cache_age_ms)
                cached["cache"]["source"] = "api_system_cache"
            return cached

    if allow_sync_on_miss:
        fresh = _refresh_health_cache_sync()
        return dict(fresh)

    _schedule_health_cache_refresh()
    return _health_cache_placeholder(
        now_ms=now_ms,
        cache_age_ms=cache_age_ms,
        cache_populated=isinstance(cached_payload, dict) and cached_ts_ms > 0,
    )


def _refresh_health_cache_sync() -> dict:
    try:
        fresh = dict(get_health_snapshot() or {})
    except Exception as e:
        _warn("api_system.health_cache.refresh", e)
        fresh = {"ok": False, "error": str(e), "reasons": [f"health_snapshot_error:{e}"]}

    try:
        with _HEALTH_CACHE_LOCK:
            _HEALTH_CACHE["ts_ms"] = _ts_ms()
            _HEALTH_CACHE["payload"] = dict(fresh)
    except Exception as e:
        _warn("api_system.health_cache.write", e)
    return dict(fresh)


def _schedule_health_cache_refresh() -> None:
    global _HEALTH_CACHE_REFRESH_IN_FLIGHT
    try:
        with _HEALTH_CACHE_REFRESH_LOCK:
            if _HEALTH_CACHE_REFRESH_IN_FLIGHT:
                return
            _HEALTH_CACHE_REFRESH_IN_FLIGHT = True
    except Exception as e:
        _warn("api_system.health_cache.refresh_lock", e)
        return

    def _runner() -> None:
        global _HEALTH_CACHE_REFRESH_IN_FLIGHT
        try:
            _refresh_health_cache_sync()
        except Exception as refresh_error:
            _warn("api_system.health_cache.refresh_async", refresh_error)
        finally:
            try:
                with _HEALTH_CACHE_REFRESH_LOCK:
                    _HEALTH_CACHE_REFRESH_IN_FLIGHT = False
            except Exception as reset_error:
                _warn("api_system.health_cache.refresh_reset", reset_error)

    try:
        threading.Thread(
            target=_runner,
            name="api_health_cache_refresh",
            daemon=True,
        ).start()
    except Exception as e:
        try:
            with _HEALTH_CACHE_REFRESH_LOCK:
                _HEALTH_CACHE_REFRESH_IN_FLIGHT = False
        except Exception as reset_error:
            _warn("api_system.health_cache.refresh_spawn_reset", reset_error)
        _warn("api_system.health_cache.refresh_spawn", e)


def _build_readiness_snapshot(_parsed, ctx=None) -> dict:
    cached = _get_cached_system_snapshot()
    if isinstance(cached, dict):
        snapshot = dict(cached)
    else:
        snapshot = _build_system_state_snapshot(_parsed, ctx)
        snapshot["graph"] = dict(_get_supervisor_graph(ctx) or {})

        health = dict(snapshot.get("health") or {})
        startup_validation = (
            dict(health.get("startup_validation") or {})
            if isinstance(health.get("startup_validation"), dict)
            else {}
        )
        snapshot.setdefault("db_validation", dict(startup_validation.get("db_validation") or {}))
        snapshot.setdefault("database_debug", {"failure_classification": {"primary_cause": ""}})
        snapshot.setdefault("job_launch_trace", [])
        snapshot.setdefault("supervisor_analysis", {})

        try:
            runtime_watchdogs = api_get_runtime_watchdogs(_parsed, ctx)
        except Exception as e:
            _warn("api_system.readiness_snapshot.runtime_watchdogs", e)
            runtime_watchdogs = {"ok": False, "error": str(e), "ts_ms": _ts_ms()}

        snapshot["runtime_watchdogs"] = dict(runtime_watchdogs or {})
        snapshot["production_validation"] = _build_production_validation(
            snapshot,
            ctx=ctx,
            runtime_watchdogs=runtime_watchdogs,
        )
        _store_cached_system_snapshot(snapshot)

    if not isinstance(snapshot.get("production_validation"), dict):
        runtime_watchdogs = dict(snapshot.get("runtime_watchdogs") or {})
        if not runtime_watchdogs:
            try:
                runtime_watchdogs = api_get_runtime_watchdogs(_parsed, ctx)
            except Exception as e:
                _warn("api_system.readiness_snapshot.runtime_watchdogs", e)
                runtime_watchdogs = {"ok": False, "error": str(e), "ts_ms": _ts_ms()}
            snapshot["runtime_watchdogs"] = dict(runtime_watchdogs or {})
        snapshot["production_validation"] = _build_production_validation(
            snapshot,
            ctx=ctx,
            runtime_watchdogs=runtime_watchdogs,
        )
        _store_cached_system_snapshot(snapshot)

    snapshot.setdefault("graph", dict(snapshot.get("graph") or {}))
    return snapshot


def _dedupe_reasons(*groups):
    out = []
    seen = set()
    for group in groups:
        for item in (group or []):
            value = str(item or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
    return out


_REQUIRED_TABLE_ALIASES = {
    "prices": ("prices",),
    "trades": ("trades",),
    "portfolio_state": ("portfolio_state",),
    "alerts": ("alerts",),
    "jobs": ("job_locks", "job_history"),
}


def _required_tables_status(schema):
    # Map logical UI concepts onto one or more physical tables so health
    # reporting stays stable even if storage internals evolve.
    schema = dict(schema or {})
    have_tables = set(str(x or "") for x in (schema.get("have_tables") or []))
    missing_tables = set(str(x or "") for x in (schema.get("missing_tables") or []))

    tables = {}
    missing = []
    reasons = []

    for logical_name, physical_names in _REQUIRED_TABLE_ALIASES.items():
        present = False
        missing_physical = []
        for physical_name in physical_names:
            if physical_name in have_tables and physical_name not in missing_tables:
                present = True
                break
            missing_physical.append(str(physical_name))
        tables[str(logical_name)] = {
            "ok": bool(present),
            "logical_name": str(logical_name),
            "physical_names": [str(x) for x in physical_names],
            "missing_physical": missing_physical if not present else [],
        }
        if not present:
            missing.append(str(logical_name))
            reasons.append(f"required_table_missing:{logical_name}")

    return {
        "ok": len(missing) == 0,
        "tables": tables,
        "missing": missing,
        "reasons": reasons,
    }


def _snapshot_response(snapshot, ok=None, **extra):
    snapshot = dict(snapshot or {})
    # This helper standardizes the top-level status/readiness envelope shared by
    # several system endpoints.
    payload = dict(snapshot)
    if ok is not None:
        payload["ok"] = bool(ok)
    payload.update(extra or {})
    payload.setdefault("status", str(snapshot.get("status") or "STOPPED"))
    payload.setdefault("state", str(snapshot.get("state") or "UNKNOWN"))
    payload.setdefault("mode", str(snapshot.get("mode") or "unknown"))
    payload.setdefault("execution_mode", str(snapshot.get("execution_mode") or payload.get("mode") or "unknown"))
    payload.setdefault("execution_allowed", bool(snapshot.get("execution_allowed")))
    payload["reasons"] = _dedupe_reasons(snapshot.get("reasons"), payload.get("reasons"))
    payload.setdefault("health", dict(snapshot.get("health") or {}))
    payload.setdefault("ingestion", dict(snapshot.get("ingestion") or {}))
    payload.setdefault("services", dict(snapshot.get("services") or {}))
    payload.setdefault("readiness", dict(snapshot.get("readiness") or {}))
    payload.setdefault("timestamps", dict(snapshot.get("timestamps") or {}))
    return payload


def _safe_json_dict(raw):
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        parsed = {}
    return dict(parsed or {}) if isinstance(parsed, dict) else {}


def _dict_or_empty(value) -> dict:
    return dict(value) if isinstance(value, dict) else {}


def _list_or_empty(value) -> list:
    return list(value) if isinstance(value, list) else []


def _health_snapshot_dict() -> dict:
    try:
        return _dict_or_empty(get_health_snapshot())
    except Exception as e:
        _warn("api_system.health_snapshot", e)
        return {"ok": False}


def _meta_json(key: str) -> dict:
    return _safe_json_dict(meta_get(str(key or ""), "") or "{}")


def _normalized_health_from_snapshot(snapshot) -> dict:
    outer = dict(snapshot.get("health") or {})
    inner = outer.get("health")
    if isinstance(inner, dict):
        return dict(inner)
    return outer


_PRODUCTION_GATE_ORDER = [
    "config_valid",
    "startup_complete",
    "database_reachable",
    "schema_valid",
    "ingestion_active",
    "ingestion_not_stale",
    "critical_features_valid",
    "model_inputs_valid",
    "scoring_pipeline_operational",
    "execution_engine_initialized",
    "order_state_consistent",
    "position_state_consistent",
    "pnl_calculation_valid",
    "api_layer_healthy",
    "operator_server_healthy",
    "critical_ui_dependencies_available",
]

_PRODUCTION_CRITICAL_GATES = {
    "config_valid",
    "startup_complete",
    "database_reachable",
    "schema_valid",
    "ingestion_active",
    "ingestion_not_stale",
    "critical_features_valid",
    "model_inputs_valid",
    "scoring_pipeline_operational",
    "execution_engine_initialized",
    "order_state_consistent",
    "position_state_consistent",
    "pnl_calculation_valid",
}

_UI_CRITICAL_ENDPOINT_SPECS = [
    {
        "path": "/api/operator/status",
        "handlers": ("api_get_operator_status", "api_get_status"),
    },
    {
        "path": "/api/operator/readiness",
        "handlers": ("api_get_readiness",),
    },
    {
        "path": "/api/operator/health",
        "handlers": ("api_get_health",),
    },
    {
        "path": "/api/operator/service_status",
        "handlers": ("api_get_service_status",),
    },
    {
        "path": "/api/operator/runtime_watchdogs",
        "handlers": ("api_get_runtime_watchdogs",),
    },
    {
        "path": "/api/operator/provider_telemetry",
        "handlers": ("api_get_provider_telemetry",),
    },
    {
        "path": "/api/operator/supervisor_diagnostics",
        "handlers": ("api_get_supervisor_diagnostics",),
    },
    {
        "path": "/api/operator/snapshot",
        "handlers": ("api_get_support_snapshot",),
    },
]


def _int_or_zero(value) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return 0
    signless = text[1:] if text[:1] in {"+", "-"} else text
    if not signless.isdigit():
        return 0
    return int(text)


def _ts_or_none(value):
    current = _int_or_zero(value)
    return current if current > 0 else None


def _first_text(*values) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _latest_timestamp_entry(candidates) -> dict:
    best = {"ts_ms": None, "source": "", "detail": "unavailable"}
    best_ts = 0
    for source, ts_ms, detail in list(candidates or []):
        current = _int_or_zero(ts_ms)
        if current <= 0:
            continue
        if current >= best_ts:
            best_ts = current
            best = {
                "ts_ms": int(current),
                "source": str(source or ""),
                "detail": str(detail or "ok"),
            }
    return best


def _make_production_gate(
    name: str,
    ok: bool,
    *,
    reason: str,
    subsystem: str,
    ts_ms,
    critical: bool,
    source: str = "",
    extra: dict | None = None,
) -> dict:
    payload = {
        "name": str(name),
        "ok": bool(ok),
        "status": "ok" if bool(ok) else ("failed" if bool(critical) else "degraded"),
        "reason": str(reason or ("ok" if bool(ok) else "unreported")),
        "blocker_severity": "none" if bool(ok) else ("critical" if bool(critical) else "warning"),
        "affected_subsystem": str(subsystem or "runtime"),
        "last_evaluated_ts_ms": _ts_or_none(ts_ms),
        "critical": bool(critical),
        "source": str(source or ""),
    }
    if extra:
        payload.update(dict(extra))
    return payload


def _startup_gate_to_production(name: str, startup_validation: dict, *, snapshot_ts_ms: int, critical: bool) -> dict:
    gate = dict(((startup_validation or {}).get("gates") or {}).get(name) or {})
    reason = _first_text(gate.get("detail"), "startup_gate_unreported")
    subsystem = str(gate.get("component") or "startup")
    ts_ms = gate.get("ts_ms") or (startup_validation or {}).get("ts_ms") or snapshot_ts_ms
    return _make_production_gate(
        name,
        bool(gate.get("ok")),
        reason=reason,
        subsystem=subsystem,
        ts_ms=ts_ms,
        critical=critical,
        source="startup_validation",
        extra={"dependency": str(gate.get("dependency") or "")},
    )


def _data_pipeline_gate_to_production(name: str, data_pipeline: dict, *, snapshot_ts_ms: int, critical: bool) -> dict:
    gate = dict(((data_pipeline or {}).get("gates") or {}).get(name) or {})
    reason_codes = [str(code) for code in list(gate.get("reason_codes") or []) if str(code or "").strip()]
    reason = _first_text(
        gate.get("detail"),
        ",".join(reason_codes),
        "data_pipeline_gate_unreported",
    )
    ts_ms = (data_pipeline or {}).get("updated_ts_ms") or snapshot_ts_ms
    return _make_production_gate(
        name,
        bool(gate.get("ok")),
        reason=reason,
        subsystem="data_pipeline",
        ts_ms=ts_ms,
        critical=critical,
        source="data_pipeline_gates",
        extra={"reason_codes": reason_codes},
    )


def _execution_gate_to_production(name: str, execution_supervisor: dict, *, snapshot_ts_ms: int, critical: bool) -> dict:
    gate = dict(((execution_supervisor or {}).get("gates") or {}).get(name) or {})
    alerts = [
        str((alert or {}).get("alert_type") or "")
        for alert in list((execution_supervisor or {}).get("alerts") or [])
        if str((alert or {}).get("alert_type") or "").strip()
    ]
    reason = _first_text(
        gate.get("detail"),
        ",".join(alerts),
        "execution_gate_unreported",
    )
    ts_ms = (execution_supervisor or {}).get("ts_ms") or snapshot_ts_ms
    return _make_production_gate(
        name,
        bool(gate.get("ok")),
        reason=reason,
        subsystem="execution",
        ts_ms=ts_ms,
        critical=critical,
        source="execution_supervisor",
        extra={"supervisor_state": str((execution_supervisor or {}).get("state") or "unknown")},
    )


def _ctx_handlers(ctx=None) -> dict:
    handlers = {}
    if isinstance(ctx, dict):
        raw = ctx.get("API_HANDLERS")
        if isinstance(raw, dict):
            handlers = dict(raw)
    return handlers


def _handler_present(ctx, *handler_names: str) -> bool:
    handlers = _ctx_handlers(ctx)
    if not handlers:
        return True
    return any(callable(handlers.get(str(name or ""))) for name in handler_names if str(name or "").strip())


def _build_ui_critical_endpoint_status(snapshot, *, ctx=None, runtime_watchdogs=None) -> list[dict]:
    snapshot = dict(snapshot or {})
    health = _normalized_health_from_snapshot(snapshot)
    production_validation = dict(snapshot.get("production_validation") or {})
    services = dict(snapshot.get("services") or {})
    provider_telemetry_ok = isinstance(health.get("providers"), dict) and isinstance(health.get("ingestion_runtime"), dict)
    runtime_watchdogs_ok = isinstance(runtime_watchdogs, dict) and isinstance(runtime_watchdogs.get("pipeline_watchdog_state"), dict)
    ts_ms = _ts_or_none(snapshot.get("ts_ms")) or _ts_ms()

    out = []
    for spec in _UI_CRITICAL_ENDPOINT_SPECS:
        path = str(spec.get("path") or "")
        handlers = tuple(spec.get("handlers") or ())
        handler_ok = _handler_present(ctx, *handlers)
        partial = False
        reason = "ok"

        if path == "/api/operator/status":
            partial = not (
                str(snapshot.get("status") or "").strip()
                and isinstance(snapshot.get("health"), dict)
                and isinstance(snapshot.get("ingestion"), dict)
                and isinstance(services.get("engine"), dict)
                and isinstance(snapshot.get("readiness"), dict)
            )
            reason = "operator_status_partial_payload" if partial else "ok"
        elif path == "/api/operator/readiness":
            partial = not (
                isinstance(production_validation.get("gates"), dict)
                and all(name in dict(production_validation.get("gates") or {}) for name in _PRODUCTION_GATE_ORDER)
            )
            reason = "operator_readiness_partial_payload" if partial else "ok"
        elif path == "/api/operator/health":
            partial = not (
                isinstance(health.get("db"), dict)
                and isinstance(health.get("prices"), dict)
                and isinstance(health.get("providers"), dict)
            )
            reason = "operator_health_partial_payload" if partial else "ok"
        elif path == "/api/operator/service_status":
            partial = not isinstance(services.get("engine"), dict)
            reason = "operator_service_status_partial_payload" if partial else "ok"
        elif path == "/api/operator/runtime_watchdogs":
            partial = not runtime_watchdogs_ok
            reason = "operator_runtime_watchdogs_partial_payload" if partial else "ok"
        elif path == "/api/operator/provider_telemetry":
            partial = not provider_telemetry_ok
            reason = "operator_provider_telemetry_partial_payload" if partial else "ok"
        elif path == "/api/operator/supervisor_diagnostics":
            partial = not (
                isinstance(snapshot.get("graph"), dict)
                and isinstance(snapshot.get("ingestion"), dict)
                and isinstance(snapshot.get("jobs"), list)
                and isinstance(services.get("engine"), dict)
            )
            reason = "operator_supervisor_diagnostics_partial_payload" if partial else "ok"
        elif path == "/api/operator/snapshot":
            partial = not (
                isinstance(snapshot.get("database_debug"), dict)
                and isinstance(production_validation.get("gates"), dict)
            )
            reason = "operator_snapshot_partial_payload" if partial else "ok"

        if not handler_ok:
            reason = "handler_missing"

        out.append(
            {
                "path": path,
                "ok": bool(handler_ok and not partial),
                "status": "ok" if bool(handler_ok and not partial) else ("missing" if not handler_ok else "partial"),
                "reason": reason,
                "handler_names": list(handlers),
                "last_checked_ts_ms": int(ts_ms),
            }
        )
    return out


def _build_restart_retry_loop_indicators(snapshot) -> dict:
    snapshot = dict(snapshot or {})
    supervisor_analysis = dict(snapshot.get("supervisor_analysis") or {})
    job_launch_trace = [dict(row or {}) for row in list(snapshot.get("job_launch_trace") or [])]
    jobs = [dict(row or {}) for row in list(snapshot.get("jobs") or [])]

    jobs_with_restarts = [
        {
            "job": str(row.get("name") or ""),
            "restart_count": int(row.get("restart_count") or 0),
            "running": bool(row.get("running")),
            "stale": bool(row.get("stale")),
            "last_error": str(row.get("last_error") or ""),
        }
        for row in jobs
        if int(row.get("restart_count") or 0) > 0 and str(row.get("name") or "").strip()
    ]
    recent_failed_launches = [
        {
            "job": str(row.get("job") or ""),
            "error": str(row.get("error") or ""),
            "ts_ms": _ts_or_none(row.get("ts_ms")),
        }
        for row in job_launch_trace[-25:]
        if bool(row.get("failed"))
    ]
    restart_loops = [dict(row or {}) for row in list(supervisor_analysis.get("restart_loops") or [])]
    detected = bool(supervisor_analysis.get("restart_loops_detected")) or bool(jobs_with_restarts) or bool(recent_failed_launches)
    reasons = _dedupe_reasons(
        ["restart_loops_detected" if bool(supervisor_analysis.get("restart_loops_detected")) else None],
        [f"job_restart_count:{row['job']}={row['restart_count']}" for row in jobs_with_restarts[:10]],
        [f"job_launch_failed:{row['job']}" for row in recent_failed_launches[:10]],
    )
    return {
        "detected": bool(detected),
        "restart_loops_detected": bool(supervisor_analysis.get("restart_loops_detected")),
        "crash_cause": str(supervisor_analysis.get("crash_cause") or ""),
        "restart_loops": restart_loops[:10],
        "jobs_with_restarts": jobs_with_restarts[:10],
        "recent_failed_launches": recent_failed_launches[:10],
        "reasons": reasons,
        "last_checked_ts_ms": _ts_or_none(snapshot.get("ts_ms")) or _ts_ms(),
    }


def _build_stale_data_indicators(snapshot) -> list[dict]:
    health = _normalized_health_from_snapshot(snapshot)
    checks = [
        ("prices", dict(health.get("prices") or {}), "age_s", "max_age_s"),
        ("ingestion_freshness", dict(health.get("ingestion_freshness") or {}), "max_observed_age_s", "max_allowed_age_s"),
        ("feature_runtime", dict(health.get("feature_runtime") or {}), "age_s", "max_age_s"),
        ("model_input_runtime", dict(health.get("model_input_runtime") or {}), "age_s", "max_age_s"),
        ("scoring_runtime", dict(health.get("scoring_runtime") or {}), "age_s", "max_age_s"),
        ("predictions", dict(health.get("predictions") or {}), "age_s", "max_age_s"),
    ]
    out = []
    for subsystem, payload, age_key, max_age_key in checks:
        if not payload:
            continue
        age_s = payload.get(age_key)
        max_age_s = payload.get(max_age_key)
        stale = bool(payload.get("stale"))
        if subsystem == "ingestion_freshness":
            stale = stale or not bool(payload.get("critical_ok", True))
        if age_s is not None and max_age_s is not None:
            try:
                stale = stale or float(age_s) > float(max_age_s)
            except Exception as e:
                _warn(
                    "api_system.stale_data_indicators.compare",
                    e,
                    subsystem=subsystem,
                    age_s=repr(age_s),
                    max_age_s=repr(max_age_s),
                )
        if not stale and bool(payload.get("ok", True)):
            continue
        out.append(
            {
                "subsystem": subsystem,
                "reason": _first_text(
                    payload.get("detail"),
                    ",".join(str(code) for code in list(payload.get("reason_codes") or []) if str(code or "").strip()),
                    f"{subsystem}_not_ok",
                ),
                "age_s": age_s,
                "max_age_s": max_age_s,
                "last_ts_ms": _ts_or_none(payload.get("last_ts_ms") or payload.get("validated_ts_ms") or payload.get("last_success_ts_ms")),
            }
        )
    return out


def _build_production_validation(snapshot, *, ctx=None, runtime_watchdogs=None) -> dict:
    snapshot = dict(snapshot or {})
    snapshot_ts_ms = _ts_or_none(snapshot.get("ts_ms")) or _ts_ms()
    health = _normalized_health_from_snapshot(snapshot)
    startup_validation = dict(health.get("startup_validation") or snapshot.get("startup_validation") or {})
    startup_gates = dict(startup_validation.get("gates") or {})
    data_pipeline = dict(health.get("data_pipeline_gates") or {})
    execution_supervisor = dict(health.get("execution_supervisor") or {})
    system_state_detail = dict(snapshot.get("system_state_detail") or {})
    lifecycle = dict(health.get("lifecycle") or {})
    database_debug = dict(snapshot.get("database_debug") or {})
    db_validation = dict(snapshot.get("db_validation") or (database_debug.get("db_validation") or {}) or {})
    services = dict(snapshot.get("services") or {})

    runtime_state = str(
        system_state_detail.get("state")
        or lifecycle.get("state")
        or snapshot.get("state")
        or snapshot.get("status")
        or "UNKNOWN"
    ).strip().upper()
    runtime_detail = _first_text(system_state_detail.get("detail"), lifecycle.get("detail"))

    gates = {
        "config_valid": _startup_gate_to_production(
            "config_valid",
            startup_validation,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "database_reachable": _startup_gate_to_production(
            "database_reachable",
            startup_validation,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "schema_valid": _startup_gate_to_production(
            "schema_valid",
            startup_validation,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "ingestion_active": _data_pipeline_gate_to_production(
            "ingestion_active",
            data_pipeline,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "ingestion_not_stale": _data_pipeline_gate_to_production(
            "ingestion_not_stale",
            data_pipeline,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "critical_features_valid": _data_pipeline_gate_to_production(
            "critical_features_valid",
            data_pipeline,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "model_inputs_valid": _data_pipeline_gate_to_production(
            "model_inputs_valid",
            data_pipeline,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "scoring_pipeline_operational": _data_pipeline_gate_to_production(
            "scoring_pipeline_operational",
            data_pipeline,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "execution_engine_initialized": _execution_gate_to_production(
            "execution_engine_initialized",
            execution_supervisor,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "order_state_consistent": _execution_gate_to_production(
            "order_state_consistent",
            execution_supervisor,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "position_state_consistent": _execution_gate_to_production(
            "position_state_consistent",
            execution_supervisor,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
        "pnl_calculation_valid": _execution_gate_to_production(
            "pnl_calculation_valid",
            execution_supervisor,
            snapshot_ts_ms=snapshot_ts_ms,
            critical=True,
        ),
    }

    startup_complete_ok = (
        bool((startup_gates.get("core_services_initialized") or {}).get("ok"))
        and bool(startup_validation.get("ok"))
        and runtime_state not in {"BOOTING", "WARMING_UP", "STARTING", "STOPPED", "SHUTDOWN", "UNKNOWN"}
    )
    gates["startup_complete"] = _make_production_gate(
        "startup_complete",
        startup_complete_ok,
        reason=(
            "ok"
            if startup_complete_ok
            else _first_text(
                (startup_gates.get("core_services_initialized") or {}).get("detail"),
                f"runtime_state={runtime_state.lower() or 'unknown'}",
                runtime_detail,
                "startup_incomplete",
            )
        ),
        subsystem="startup",
        ts_ms=(
            (startup_gates.get("core_services_initialized") or {}).get("ts_ms")
            or startup_validation.get("ts_ms")
            or snapshot_ts_ms
        ),
        critical=True,
        source="startup_validation",
    )

    provisional_snapshot = dict(snapshot)
    provisional_snapshot["production_validation"] = {"gates": dict(gates)}
    ui_critical_endpoint_status = _build_ui_critical_endpoint_status(
        provisional_snapshot,
        ctx=ctx,
        runtime_watchdogs=runtime_watchdogs,
    )
    missing_ui_handlers = [
        str(row.get("path") or "")
        for row in ui_critical_endpoint_status
        if str(row.get("status") or "") == "missing"
    ]
    bad_ui_endpoints = [
        dict(row or {})
        for row in ui_critical_endpoint_status
        if not bool(row.get("ok"))
    ]

    api_gate = dict(startup_gates.get("required_api_dependencies_available") or {})
    gates["api_layer_healthy"] = _make_production_gate(
        "api_layer_healthy",
        bool(api_gate.get("ok")) and len(missing_ui_handlers) == 0,
        reason=(
            "ok"
            if bool(api_gate.get("ok")) and len(missing_ui_handlers) == 0
            else _first_text(
                api_gate.get("detail"),
                (
                    f"missing_handlers={','.join(missing_ui_handlers)}"
                    if missing_ui_handlers
                    else ""
                ),
                "api_layer_unhealthy",
            )
        ),
        subsystem="api",
        ts_ms=api_gate.get("ts_ms") or startup_validation.get("ts_ms") or snapshot_ts_ms,
        critical=False,
        source="startup_validation",
    )

    port_gate = dict(startup_gates.get("no_port_binding_conflict") or {})
    operator_server_ok = (
        bool(port_gate.get("ok"))
        and runtime_state not in {"STOPPED", "SHUTDOWN", "UNKNOWN"}
        and str(snapshot.get("status") or "").strip().upper() != "STOPPED"
    )
    gates["operator_server_healthy"] = _make_production_gate(
        "operator_server_healthy",
        operator_server_ok,
        reason=(
            "ok"
            if operator_server_ok
            else _first_text(
                port_gate.get("detail"),
                runtime_detail,
                f"runtime_state={runtime_state.lower() or 'unknown'}",
                "operator_server_unavailable",
            )
        ),
        subsystem="operator_server",
        ts_ms=port_gate.get("ts_ms") or startup_validation.get("ts_ms") or snapshot_ts_ms,
        critical=False,
        source="startup_validation",
    )

    ui_gate = dict(startup_gates.get("ui_static_assets_present") or {})
    gates["critical_ui_dependencies_available"] = _make_production_gate(
        "critical_ui_dependencies_available",
        bool(ui_gate.get("ok")) and len(bad_ui_endpoints) == 0,
        reason=(
            "ok"
            if bool(ui_gate.get("ok")) and len(bad_ui_endpoints) == 0
            else _first_text(
                ui_gate.get("detail"),
                (
                    f"{bad_ui_endpoints[0].get('path')}:{bad_ui_endpoints[0].get('reason')}"
                    if bad_ui_endpoints
                    else ""
                ),
                "critical_ui_dependency_missing",
            )
        ),
        subsystem="ui",
        ts_ms=ui_gate.get("ts_ms") or startup_validation.get("ts_ms") or snapshot_ts_ms,
        critical=False,
        source="startup_validation",
    )

    critical_failures = [name for name in _PRODUCTION_GATE_ORDER if not bool((gates.get(name) or {}).get("ok")) and name in _PRODUCTION_CRITICAL_GATES]
    warning_failures = [name for name in _PRODUCTION_GATE_ORDER if not bool((gates.get(name) or {}).get("ok")) and name not in _PRODUCTION_CRITICAL_GATES]
    status = "healthy"
    if critical_failures:
        status = "failed"
    elif warning_failures:
        status = "degraded"

    ingestion_runtime = dict(health.get("ingestion_runtime") or {})
    predictions = dict(health.get("predictions") or {})
    scoring_runtime = dict(health.get("scoring_runtime") or {})
    execution = dict(health.get("execution") or {})
    last_write_timestamps = dict(db_validation.get("last_write_timestamps") or {})
    restart_retry_loop_indicators = _build_restart_retry_loop_indicators(snapshot)
    stale_data_indicators = _build_stale_data_indicators(snapshot)
    failing_components = sorted(
        {
            str((gates.get(name) or {}).get("affected_subsystem") or "")
            for name in _PRODUCTION_GATE_ORDER
            if not bool((gates.get(name) or {}).get("ok")) and str((gates.get(name) or {}).get("affected_subsystem") or "").strip()
        }
    )
    degraded_reasons = [
        str((gates.get(name) or {}).get("reason") or "")
        for name in _PRODUCTION_GATE_ORDER
        if not bool((gates.get(name) or {}).get("ok")) and str((gates.get(name) or {}).get("reason") or "").strip()
    ]

    return {
        "status": status,
        "healthy": status == "healthy",
        "degraded": status == "degraded",
        "failed": status == "failed",
        "safe_to_operate": status == "healthy",
        "unsafe_to_operate": status != "healthy",
        "lifecycle_state": {
            "state": runtime_state or "UNKNOWN",
            "detail": runtime_detail,
            "status": str(snapshot.get("status") or ""),
            "ts_ms": _ts_or_none(lifecycle.get("ts_ms")) or snapshot_ts_ms,
        },
        "gate_order": list(_PRODUCTION_GATE_ORDER),
        "gates": {name: dict(gates.get(name) or {}) for name in _PRODUCTION_GATE_ORDER},
        "gate_rows": [dict(gates.get(name) or {}) for name in _PRODUCTION_GATE_ORDER],
        "critical_failures": critical_failures,
        "warning_failures": warning_failures,
        "failing_components": failing_components,
        "current_degraded_reasons": degraded_reasons,
        "last_successful_ingestion_event": _latest_timestamp_entry(
            [
                ("ingestion_runtime.last_publish_ts_ms", ingestion_runtime.get("last_publish_ts_ms"), str(ingestion_runtime.get("detail") or "ok")),
                ("data_pipeline.ingestion_active", ((data_pipeline.get("gates") or {}).get("ingestion_active") or {}).get("freshest_activity_ts_ms"), str((((data_pipeline.get("gates") or {}).get("ingestion_active") or {}).get("detail") or "ok"))),
                ("ingestion.last_price_ts_ms", (snapshot.get("ingestion") or {}).get("last_price_ts_ms"), str((snapshot.get("ingestion") or {}).get("status") or "ok")),
            ]
        ),
        "last_successful_db_write": _latest_timestamp_entry(
            [
                ("db_validation.prices_last_ts_ms", last_write_timestamps.get("prices_last_ts_ms"), "prices"),
                ("db_validation.event_log_last_ts_ms", last_write_timestamps.get("event_log_last_ts_ms"), "event_log"),
                ("db_validation.job_heartbeats_last_ts_ms", last_write_timestamps.get("job_heartbeats_last_ts_ms"), "job_heartbeats"),
            ]
        ),
        "last_successful_score_or_model_output": _latest_timestamp_entry(
            [
                ("data_pipeline.scoring_pipeline_operational", ((data_pipeline.get("gates") or {}).get("scoring_pipeline_operational") or {}).get("last_success_ts_ms"), str((((data_pipeline.get("gates") or {}).get("scoring_pipeline_operational") or {}).get("detail") or "ok"))),
                ("scoring_runtime.last_success_ts_ms", scoring_runtime.get("last_success_ts_ms"), str(scoring_runtime.get("detail") or "ok")),
                ("predictions.last_ts_ms", predictions.get("last_ts_ms"), str(predictions.get("detail") or "ok")),
            ]
        ),
        "last_successful_execution_event": _latest_timestamp_entry(
            [
                ("execution.last_fill_ts_ms", execution.get("last_fill_ts_ms"), str(execution.get("fills_table") or "fills")),
            ]
        ),
        "restart_retry_loop_indicators": restart_retry_loop_indicators,
        "stale_data_indicators": stale_data_indicators,
        "ui_critical_endpoint_status": ui_critical_endpoint_status,
        "summary_reason": _first_text(
            (gates.get(critical_failures[0] if critical_failures else "") or {}).get("reason"),
            (gates.get(warning_failures[0] if warning_failures else "") or {}).get("reason"),
            str((database_debug.get("failure_classification") or {}).get("primary_cause") or ""),
            "production_validation_ok",
        ),
        "ts_ms": int(snapshot_ts_ms),
    }


def _get_jobs_payload(ctx=None):
    if not isinstance(ctx, dict):
        return [], ["jobs_ctx_missing"]

    jobs_ref = ctx.get("JOBS")
    if jobs_ref is None:
        return [], ["jobs_manager_missing"]

    try:
        rows = jobs_ref.list_jobs() or []
        if not isinstance(rows, list):
            return [], ["jobs_list_invalid"]
        return rows, []
    except Exception as e:
        _warn("api_system.get_jobs_payload.list_jobs", e)
        errors = [f"jobs_list_error:{e}"]
        return [], errors


def _get_supervisor_graph(ctx=None):
    if not isinstance(ctx, dict):
        return {"ok": False, "error": "supervisor_ctx_missing"}

    sup = ctx.get("SUPERVISOR")
    if sup is None:
        return {"ok": False, "error": "supervisor_missing"}

    try:
        graph = sup.validate_graph(strict=True)
        if isinstance(graph, dict):
            return graph
        return {"ok": False, "error": "supervisor_graph_invalid"}
    except Exception as e:
        _warn("api_system.get_supervisor_graph", e)
        graph = {"ok": False, "error": str(e)}
        return graph


def _get_kill_switch_data(_parsed, ctx=None):
    try:
        from engine.execution.kill_switch import snapshot as _snapshot_kill_switches

        payload = _snapshot_kill_switches()
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), dict):
                return dict(payload.get("data") or {}), []
            return dict(payload), []
    except Exception as e:
        _warn("api_system.get_kill_switch_data.direct", e)

    if not isinstance(ctx, dict):
        return {}, ["kill_switch_ctx_missing"]

    handlers = ctx.get("API_HANDLERS") or {}
    get_ks = handlers.get("api_get_kill_switches")
    if not callable(get_ks):
        return {}, ["kill_switch_handler_missing"]

    try:
        payload = get_ks(_parsed, ctx)
    except Exception as e:
        _warn("api_system.get_kill_switch_data", e)
        errors = [f"kill_switch_error:{e}"]
        return {}, errors

    if not isinstance(payload, dict):
        return {}, ["kill_switch_payload_invalid"]

    if isinstance(payload.get("data"), dict):
        return dict(payload.get("data") or {}), []
    return dict(payload), []


def _build_services_snapshot(jobs):
    # Services snapshot turns raw job rows into operator-facing service health
    # with stale/zombie/dead classifications.
    services = {}
    running_count = 0
    stale_count = 0
    failed_count = 0
    zombie_count = 0
    locked_count = 0
    dead_count = 0

    for row in (jobs or []):
        name = str(row.get("name") or "").strip()
        if not name:
            continue

        running = bool(row.get("running"))
        stale = bool(row.get("stale"))
        exit_code = row.get("exit_code")
        pid = row.get("pid")
        heartbeat_age_s = row.get("heartbeat_age_s")
        restart_count = int(row.get("restart_count") or 0)
        lock_owner = row.get("lock_owner")
        stop_requested = bool(row.get("stop_requested"))
        zombie = bool((not running) and pid and exit_code in (None, 0))
        locked = bool((not running) and lock_owner and not stop_requested)
        dead = bool((not running) and exit_code not in (None, 0))

        if running:
            running_count += 1
        if stale:
            stale_count += 1
        if exit_code not in (None, 0):
            failed_count += 1
        if zombie:
            zombie_count += 1
        if locked:
            locked_count += 1
        if dead:
            dead_count += 1

        services[name] = {
            "running": running,
            "pid": pid,
            "group": row.get("group"),
            "mode": row.get("mode"),
            "heartbeat_age_s": heartbeat_age_s,
            "heartbeat_ts_ms": row.get("heartbeat_ts_ms"),
            "restart_count": restart_count,
            "stale": stale,
            "exit_code": exit_code,
            "zombie": zombie,
            "locked": locked,
            "dead": dead,
            "stop_requested": stop_requested,
            "lock_owner": lock_owner,
            "last_error": row.get("last_error") if "last_error" in row else None,
            "last_success_ts": row.get("last_success_ts") if "last_success_ts" in row else None,
        }

    engine_running = running_count > 0
    ok = bool(engine_running) and stale_count == 0 and failed_count == 0 and zombie_count == 0 and locked_count == 0 and dead_count == 0

    if engine_running and ok:
        engine_state = "RUNNING"
    elif engine_running:
        engine_state = "DEGRADED"
    else:
        engine_state = "STOPPED"

    reasons = []
    if not engine_running:
        reasons.append("services_not_running")
    if stale_count > 0:
        reasons.append("services_stale")
    if failed_count > 0:
        reasons.append("services_failed")
    if zombie_count > 0:
        reasons.append("services_zombie")
    if locked_count > 0:
        reasons.append("services_locked")
    if dead_count > 0:
        reasons.append("services_dead")

    return {
        "ok": ok,
        "engine": {
            "running": engine_running,
            "job_count": len(jobs or []),
            "running_count": running_count,
            "stale_count": stale_count,
            "failed_count": failed_count,
            "zombie_count": zombie_count,
            "locked_count": locked_count,
            "dead_count": dead_count,
            "state": engine_state,
        },
        "services": services,
        "reasons": reasons,
    }


def _build_ingestion_snapshot(jobs, health):
    try:
        max_age_ms = int(float(os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120")) * 1000.0)
    except Exception:
        max_age_ms = 120000

    try:
        market = market_data_status(max_age_ms=max_age_ms)
    except Exception as e:
        market = {"ok": False, "error": str(e)}

    if not isinstance(market, dict):
        market = {"ok": False, "error": "invalid_market_data_status"}

    jobs_by_name = {
        str(row.get("name") or ""): row
        for row in (jobs or [])
        if str(row.get("name") or "").strip()
    }

    ingestion_runtime = dict(jobs_by_name.get("ingestion_runtime") or {})
    price_daemons = {
        name: dict(jobs_by_name.get(name) or {})
        for name in (
            "ingestion_runtime",
            "poll_prices",
            "stream_prices_polygon_ws",
        )
        if jobs_by_name.get(name)
    }

    prices = dict((health or {}).get("prices") or {})
    providers = dict((health or {}).get("providers") or {})
    db = dict((health or {}).get("db") or {})
    schema = dict((health or {}).get("schema") or {})
    required_tables = dict((health or {}).get("required_tables") or {})

    running = bool(market.get("running")) or any(bool((row or {}).get("running")) for row in price_daemons.values())
    healthy_providers = int(market.get("healthy_providers") or 0)
    fresh_rows = int(market.get("fresh_rows") or 0)
    fresh_symbols = int(market.get("fresh_symbols") or 0)
    price_age_ms = int(market.get("price_age_ms") or 0)
    visible_jobs_running = sum(1 for row in price_daemons.values() if bool((row or {}).get("running")))
    stale_jobs = [
        name
        for name, row in price_daemons.items()
        if bool((row or {}).get("stale"))
    ]

    reasons = []
    if not running:
        reasons.append("ingestion_not_running")
    if not bool(prices.get("ok")):
        reasons.append("prices_not_ok")
    if not bool(providers.get("ok")):
        reasons.append("providers_not_ok")
    if not bool(db.get("ok")):
        reasons.append("db_not_ok")
    if not bool(schema.get("ok")):
        reasons.append("schema_not_ok")
    if not bool(required_tables.get("ok", False)):
        reasons.extend(list(required_tables.get("reasons") or []))
    if healthy_providers <= 0:
        reasons.append("healthy_providers_zero")
    if fresh_rows <= 0:
        reasons.append("fresh_rows_zero")
    if fresh_symbols <= 0:
        reasons.append("fresh_symbols_zero")
    if price_age_ms <= 0 or price_age_ms > max_age_ms:
        reasons.append("prices_stale")
    if visible_jobs_running <= 0:
        reasons.append("ingestion_jobs_not_running")
    if stale_jobs:
        reasons.append("ingestion_jobs_stale")

    ok = (
        running
        and bool(prices.get("ok"))
        and bool(providers.get("ok"))
        and bool(db.get("ok"))
        and bool(schema.get("ok"))
        and bool(required_tables.get("ok", False))
        and healthy_providers > 0
        and fresh_rows > 0
        and fresh_symbols > 0
        and price_age_ms > 0
        and price_age_ms <= max_age_ms
        and visible_jobs_running > 0
        and len(stale_jobs) == 0
    )

    if ok:
        status = "RUNNING"
    elif running or bool(ingestion_runtime.get("running")):
        status = "DEGRADED"
    elif len(price_daemons) > 0:
        status = "STARTING"
    else:
        status = "STOPPED"

    return {
        "ok": ok,
        "running": running,
        "status": status,
        "job_visible": bool(ingestion_runtime),
        "healthy_providers": healthy_providers,
        "fresh_rows": fresh_rows,
        "fresh_symbols": fresh_symbols,
        "last_price_ts_ms": int(market.get("last_price_ts_ms") or 0),
        "price_age_ms": price_age_ms,
        "active_child": str(market.get("active_child") or ""),
        "child_pid": int(market.get("child_pid") or 0),
        "providers": market.get("providers") or {},
        "updated_ts_ms": int(market.get("updated_ts_ms") or 0),
        "age_ms": int(market.get("age_ms") or 0),
        "owner": str(market.get("owner") or ""),
        "last_seq": int(market.get("last_seq") or 0),
        "price_jobs": price_daemons,
        "summary": {
            "active_child": str(market.get("active_child") or ""),
            "child_pid": int(market.get("child_pid") or 0),
            "healthy_providers": healthy_providers,
            "fresh_rows": fresh_rows,
            "fresh_symbols": fresh_symbols,
            "price_age_ms": price_age_ms,
            "status": status,
            "visible_jobs_running": visible_jobs_running,
            "stale_jobs": stale_jobs,
        },
        "visible_jobs_running": visible_jobs_running,
        "stale_jobs": stale_jobs,
        "required_tables": required_tables,
        "reasons": reasons,
    }


def _normalize_health_payload(health, schema, preflight, graph, execution_barrier):
    out = dict(health or {})
    out["schema"] = dict(schema or {})
    out["preflight"] = dict(preflight or {})
    out["graph"] = dict(graph or {})
    out["execution_barrier"] = dict(execution_barrier or {})
    out["required_tables"] = _required_tables_status(schema)
    timeseries_storage = _dict_or_empty(out.get("timeseries_storage"))
    feature_store = _dict_or_empty(out.get("feature_store")) or _dict_or_empty(timeseries_storage.get("feature_store"))
    portfolio_runtime = _dict_or_empty(out.get("portfolio_runtime"))
    execution_degraded = _dict_or_empty(out.get("execution_degraded"))
    timeseries_required = bool(timeseries_storage.get("enabled")) or bool(feature_store.get("enabled"))
    timeseries_ok = (not timeseries_required) or bool(timeseries_storage.get("ok"))
    portfolio_runtime_ok = not bool(portfolio_runtime.get("degraded"))
    execution_degraded_critical = bool(
        execution_degraded.get("active")
        and str(execution_degraded.get("severity") or "").strip().upper() == "CRITICAL"
    )
    out["reasons"] = _dedupe_reasons(
        out.get("reasons") or [],
        (out.get("required_tables") or {}).get("reasons") or [],
        [
            (out.get("schema") or {}).get("error"),
            (out.get("preflight") or {}).get("error"),
            (out.get("graph") or {}).get("error"),
            (out.get("execution_barrier") or {}).get("reason"),
        ],
    )
    out.setdefault("ok", False)
    out["ok"] = (
        bool((out.get("db") or {}).get("ok"))
        and bool((out.get("schema") or {}).get("ok"))
        and bool((out.get("required_tables") or {}).get("ok"))
        and bool((out.get("prices") or {}).get("ok"))
        and bool((out.get("events") or {}).get("ok"))
        and bool((out.get("providers") or {}).get("ok"))
        and bool((out.get("job_summary") or {}).get("ok"))
        and bool((out.get("execution_barrier") or {}).get("allowed"))
        and timeseries_ok
        and portfolio_runtime_ok
        and (not execution_degraded_critical)
    )
    return out


def _kill_switch_active(kill_switches):
    try:
        if not isinstance(kill_switches, dict):
            return False
        if kill_switches.get("enabled") is True:
            return True
        if str(kill_switches.get("state") or "").strip().upper() in ("KILL", "KILL_SWITCH"):
            return True
        if isinstance(kill_switches.get("kill_switches"), dict):
            for value in (kill_switches.get("kill_switches") or {}).values():
                if isinstance(value, dict) and (value.get("enabled") is True or value.get("active") is True):
                    return True
                if value is True:
                    return True
        if isinstance(kill_switches.get("data"), dict):
            return _kill_switch_active(kill_switches.get("data"))
        if isinstance(kill_switches.get("state"), list):
            for row in (kill_switches.get("state") or []):
                if int((row or {}).get("enabled") or 0) == 1:
                    return True
    except Exception as e:
        _warn("api_system.kill_switch_active", e)
        active = False
        return active
    return False


def _compute_status_name(state_name, health_ok, services_ok, ingestion_ok, execution_allowed, kill_switches):
    ks_enabled = _kill_switch_active(kill_switches)

    if ks_enabled or state_name == "KILL_SWITCH":
        return "KILL_SWITCH"
    if health_ok and services_ok and ingestion_ok and execution_allowed:
        return "RUNNING"
    if state_name in ("BOOTING", "WARMING_UP"):
        return "STARTING"
    if state_name == "SHUTDOWN":
        return "STOPPED"
    if state_name == "LIVE":
        return "DEGRADED"
    if state_name == "DEGRADED":
        return "DEGRADED"
    return "STOPPED"


def _build_system_snapshot(_parsed, ctx=None):
    cached = _get_cached_system_snapshot()
    if isinstance(cached, dict):
        return cached

    ts_ms = _ts_ms()
    timestamps = {"ts_ms": ts_ms, "snapshot_ts_ms": ts_ms}

    jobs, job_errors = _get_jobs_payload(ctx)

    try:
        health_raw = _cached_health_snapshot()
    except Exception as e:
        health_raw = {"ok": False, "reasons": [f"health_snapshot_error:{e}"], "error": str(e)}

    if not isinstance(health_raw, dict):
        health_raw = {"ok": False, "reasons": ["health_snapshot_invalid"]}

    try:
        schema = get_schema_audit()
    except Exception as e:
        _warn("api_system.build_system_snapshot.schema", e)
        schema = {"ok": False, "error": str(e), "missing_tables": [], "missing_cols": {}}

    graph = _get_supervisor_graph(ctx)

    try:
        preflight = run_preflight()
    except Exception as e:
        _warn("api_system.build_system_snapshot.preflight", e)
        preflight = {"ok": False, "notes": [f"preflight_error:{e}"], "error": str(e)}

    kill_switches, kill_switch_errors = _get_kill_switch_data(_parsed, ctx)

    get_execution_mode_fn = None
    try:
        from engine.api.internal_access import get_execution_mode as _get_execution_mode
        get_execution_mode_fn = _get_execution_mode
    except Exception:
        get_execution_mode_fn = None

    try:
        readiness_seed = get_readiness_snapshot(
            health=dict(health_raw or {}),
            preflight=dict(preflight or {}),
            graph=dict(graph or {}),
        )
    except Exception as e:
        _warn("api_system.build_system_snapshot.readiness_seed", e)
        readiness_seed = {"ok": False, "issues": [{"code": "readiness_seed_error", "message": str(e), "detail": str(e)}]}

    try:
        system_state_detail = compute_system_state(
            health=dict(health_raw or {}),
            jobs=list(jobs or []),
            kill_switches=dict(kill_switches or {}),
            readiness=dict(readiness_seed or {}),
        )
    except Exception as e:
        _warn("api_system.build_system_snapshot.system_state", e)
        system_state_detail = {
            "ok": False,
            "ts_ms": ts_ms,
            "state": "UNKNOWN",
            "reasons": [f"system_state_error:{e}"],
        }

    try:
        execution_barrier = execution_gate_snapshot(
            get_execution_mode_fn=get_execution_mode_fn,
            system_state=system_state_detail,
            kill_switches=kill_switches,
            execution_degraded=dict((health_raw or {}).get("execution_degraded") or {}),
            portfolio_risk_gate=None,
        )
    except Exception as e:
        _warn("api_system.build_system_snapshot.execution_barrier", e)
        execution_barrier = {
            "ok": False,
            "allowed": False,
            "mode": "unknown",
            "reason": f"execution_barrier_error:{e}",
        }

    if not isinstance(execution_barrier, dict):
        execution_barrier = {
            "ok": False,
            "allowed": False,
            "mode": "unknown",
            "reason": "execution_barrier_invalid",
        }

    health = _normalize_health_payload(
        health=health_raw,
        schema=schema,
        preflight=preflight,
        graph=graph,
        execution_barrier=execution_barrier,
    )

    services = _build_services_snapshot(jobs)
    ingestion = _build_ingestion_snapshot(jobs=jobs, health=health)

    try:
        readiness = get_readiness_snapshot(
            health=dict(health or {}),
            preflight=dict(preflight or {}),
            system_state=dict(system_state_detail or {}),
            graph=dict(graph or {}),
        )
    except Exception as e:
        _warn("api_system.build_system_snapshot.readiness", e)
        readiness = {
            "ok": False,
            "ready": False,
            "issues": [{"code": "readiness_error", "message": str(e), "detail": str(e)}],
        }

    mode = str((execution_barrier or {}).get("mode") or "unknown").strip().lower() or "unknown"
    kill_switch_active = _kill_switch_active(kill_switches)
    execution_allowed = bool((execution_barrier or {}).get("allowed")) and not kill_switch_active
    state_name = "KILL_SWITCH" if kill_switch_active else str((system_state_detail or {}).get("state") or "UNKNOWN")
    reasons = _dedupe_reasons(
        job_errors,
        kill_switch_errors,
        (system_state_detail or {}).get("reasons") or [],
        (health or {}).get("reasons") or [],
        ((health or {}).get("required_tables") or {}).get("reasons") or [],
        (services or {}).get("reasons") or [],
        (ingestion or {}).get("reasons") or [],
        ["kill_switch_active" if kill_switch_active else None],
        [
            (graph or {}).get("error"),
            (schema or {}).get("error"),
            (preflight or {}).get("error"),
            (health or {}).get("error"),
            (execution_barrier or {}).get("reason"),
        ],
    )

    status = _compute_status_name(
        state_name=state_name,
        health_ok=bool(health.get("ok")),
        services_ok=bool(services.get("ok")),
        ingestion_ok=bool(ingestion.get("ok")),
        execution_allowed=execution_allowed,
        kill_switches=kill_switches,
    )

    ok = (
        status == "RUNNING"
        and bool(health.get("ok"))
        and bool(services.get("ok"))
        and bool(ingestion.get("ok"))
        and execution_allowed
        and not kill_switch_active
    )

    base = {
        "ok": ok,
        "status": status,
        "state": state_name,
        "mode": mode,
        "execution_mode": mode,
        "execution_allowed": execution_allowed,
        "reasons": reasons,
        "timestamps": timestamps,
        "ts_ms": ts_ms,
    }

    health_payload = {
        **base,
        "ok": bool(health.get("ok")),
        "health": dict(health or {}),
        "ingestion": ingestion,
        "services": services,
        "readiness": readiness,
        "timestamps": timestamps,
    }

    root_cause_candidates = _dedupe_reasons(
        reasons,
        (execution_barrier or {}).get("reason"),
        (schema or {}).get("error"),
        (preflight or {}).get("error"),
        (graph or {}).get("error"),
    )

    critical_blockers = []
    if not health.get("ok"):
        critical_blockers.append("health_not_ok")
    if not services.get("ok"):
        critical_blockers.append("services_not_ok")
    if not ingestion.get("ok"):
        critical_blockers.append("ingestion_not_ok")
    if not execution_allowed:
        critical_blockers.append("execution_blocked")

    if not schema.get("ok"):
        system_stage = "BOOT"
    elif not ingestion.get("ok"):
        system_stage = "INGESTION"
    elif not health.get("ok"):
        system_stage = "FEATURES"
    else:
        system_stage = "EXECUTION"

    data_flow_ok = bool(
        health.get("ok")
        and ingestion.get("ok")
        and services.get("ok")
        and execution_allowed
    )

    db_info = {
        "path": str(DB_PATH),
        "exists": bool(Path(DB_PATH).exists()),
    }

    recent_errors = []
    try:
        recent_errors = _recent_runtime_errors(limit=10)
    except Exception as e:
        _warn("api_system.build_system_snapshot.recent_errors", e)
        recent_errors = []

    db_debug = {}
    try:
        db_debug = get_db_debug_snapshot()
    except Exception as e:
        db_debug = {"ok": False, "error": str(e)}

    runtime_watchdogs = {}
    try:
        runtime_watchdogs = api_get_runtime_watchdogs(_parsed, ctx)
    except Exception as e:
        _warn("api_system.build_system_snapshot.runtime_watchdogs", e)
        runtime_watchdogs = {"ok": False, "error": str(e), "ts_ms": _ts_ms()}

    payload = {
        **base,
        "db": db_info,
        "health": health_payload,
        "recent_errors": recent_errors,
        "ingestion": ingestion,
        "services": services,
        "readiness": readiness,
        "timestamps": timestamps,
        "jobs": jobs,
        "graph": graph,
        "preflight": preflight,
        "schema": schema,
        "kill_switches": kill_switches,
        "execution_barrier": execution_barrier,
        "system_state_detail": system_state_detail,
        "root_cause_candidates": root_cause_candidates,
        "critical_blockers": critical_blockers,
        "system_stage": system_stage,
        "data_flow_ok": data_flow_ok,
        "database_debug": db_debug,
        "startup_trace": dict((db_debug or {}).get("startup_trace") or {}),
        "import_smoke": dict((db_debug or {}).get("import_smoke") or {}),
        "job_launch_trace": list((db_debug or {}).get("job_launch_trace") or []),
        "db_validation": dict((db_debug or {}).get("db_validation") or {}),
        "ingestion_state": dict((db_debug or {}).get("ingestion_state") or {}),
        "supervisor_analysis": dict((db_debug or {}).get("supervisor_analysis") or {}),
        "failure_classification": dict((db_debug or {}).get("failure_classification") or {}),
        "runtime_watchdogs": runtime_watchdogs,
    }
    production_validation = _build_production_validation(
        payload,
        ctx=ctx,
        runtime_watchdogs=runtime_watchdogs,
    )
    payload["production_validation"] = production_validation
    _store_cached_system_snapshot(payload)
    return payload


def _build_system_state_snapshot(_parsed, ctx=None):
    ts_ms = _ts_ms()
    timestamps = {"ts_ms": ts_ms, "snapshot_ts_ms": ts_ms}

    jobs, job_errors = _get_jobs_payload(ctx)

    try:
        health_raw = _cached_health_snapshot(allow_sync_on_miss=False)
    except Exception as e:
        _warn("api_system.build_system_snapshot.health", e)
        health_raw = {"ok": False, "reasons": [f"health_snapshot_error:{e}"], "error": str(e)}

    if not isinstance(health_raw, dict):
        health_raw = {"ok": False, "reasons": ["health_snapshot_invalid"]}

    kill_switches, kill_switch_errors = _get_kill_switch_data(_parsed, ctx)

    try:
        readiness = get_readiness_snapshot(health=dict(health_raw or {}))
    except Exception as e:
        readiness = {
            "ok": False,
            "ready": False,
            "issues": [{"code": "readiness_error", "message": str(e), "detail": str(e)}],
        }

    try:
        system_state_detail = compute_system_state(
            health=dict(health_raw or {}),
            jobs=list(jobs or []),
            kill_switches=dict(kill_switches or {}),
            readiness=dict(readiness or {}),
        )
    except Exception as e:
        system_state_detail = {
            "ok": False,
            "ts_ms": ts_ms,
            "state": "UNKNOWN",
            "reasons": [f"system_state_error:{e}"],
        }

    try:
        get_execution_mode_fn = None
        try:
            from engine.api.internal_access import get_execution_mode as _get_execution_mode
            get_execution_mode_fn = _get_execution_mode
        except Exception:
            get_execution_mode_fn = None

        execution_barrier = execution_gate_snapshot(
            get_execution_mode_fn=get_execution_mode_fn,
            system_state=system_state_detail,
            kill_switches=kill_switches,
            execution_degraded=dict((health_raw or {}).get("execution_degraded") or {}),
            portfolio_risk_gate=None,
        )
    except Exception as e:
        execution_barrier = {
            "ok": False,
            "allowed": False,
            "mode": "unknown",
            "reason": f"execution_barrier_error:{e}",
        }

    services = _build_services_snapshot(jobs)
    ingestion = _build_ingestion_snapshot(jobs=jobs, health=health_raw)

    mode = str((execution_barrier or {}).get("mode") or "unknown").strip().lower() or "unknown"
    kill_switch_active = _kill_switch_active(kill_switches)
    execution_allowed = bool((execution_barrier or {}).get("allowed")) and not kill_switch_active
    state_name = "KILL_SWITCH" if kill_switch_active else str((system_state_detail or {}).get("state") or "UNKNOWN")
    reasons = _dedupe_reasons(
        job_errors,
        kill_switch_errors,
        (system_state_detail or {}).get("reasons") or [],
        (health_raw or {}).get("reasons") or [],
        (services or {}).get("reasons") or [],
        (ingestion or {}).get("reasons") or [],
        ["kill_switch_active" if kill_switch_active else None],
        [(execution_barrier or {}).get("reason")],
    )

    status = _compute_status_name(
        state_name=state_name,
        health_ok=bool((health_raw or {}).get("ok")),
        services_ok=bool((services or {}).get("ok")),
        ingestion_ok=bool((ingestion or {}).get("ok")),
        execution_allowed=execution_allowed,
        kill_switches=kill_switches,
    )

    return {
        "ok": bool((system_state_detail or {}).get("ok")),
        "status": status,
        "state": state_name,
        "mode": mode,
        "execution_mode": mode,
        "execution_allowed": execution_allowed,
        "reasons": reasons,
        "timestamps": timestamps,
        "ts_ms": ts_ms,
        "health": dict(health_raw or {}),
        "ingestion": dict(ingestion or {}),
        "services": dict(services or {}),
        "readiness": dict(readiness or {}),
        "jobs": list(jobs or []),
        "kill_switches": dict(kill_switches or {}),
        "execution_barrier": dict(execution_barrier or {}),
        "system_state_detail": dict(system_state_detail or {}),
    }


def api_get_runtime_config(_parsed, ctx=None):
    """Return the validated runtime configuration alongside a system snapshot.

    Parameters
    ----------
    _parsed : Any
        Parsed request/query container supplied by the HTTP transport.
    ctx : dict[str, Any] | None, optional
        Request context from the dashboard server.

    Returns
    -------
    dict[str, Any]
        System snapshot enriched with a serialized `RuntimeConfig` under the
        `config` key. On configuration validation failure, the payload remains
        structured but reports `ok=False` and includes the validation error.
    """

    snapshot = _build_system_snapshot(_parsed, ctx)
    try:
        cfg = load_runtime_config()
        return {
            **snapshot,
            "config": asdict(cfg),
        }
    except ConfigError as e:
        _warn("api_system.runtime_config", e)
        payload = {
            **snapshot,
            "ok": False,
            "status": "DEGRADED" if snapshot.get("status") == "RUNNING" else snapshot.get("status"),
            "reasons": _dedupe_reasons(snapshot.get("reasons"), [f"config_error:{e}"]),
            "config": None,
            "error": str(e),
        }
        return payload


def api_get_system_state(_parsed, ctx=None):
    ts_ms = _ts_ms()
    timestamps = {"ts_ms": ts_ms, "snapshot_ts_ms": ts_ms}

    health = _cached_health_snapshot()
    if not isinstance(health, dict):
        health = {"ok": False, "reasons": ["health_snapshot_invalid"]}

    services = {
        "ok": True,
        "engine": {"running": True, "job_count": 0, "running_count": 0, "stale_count": 0, "failed_count": 0, "zombie_count": 0, "locked_count": 0, "dead_count": 0, "state": "RUNNING"},
        "services": {},
        "reasons": [],
    }
    ingestion = dict((health or {}).get("ingestion_runtime") or {})
    if ingestion:
        ingestion.setdefault("ok", bool(not ingestion.get("stale", False)))
        ingestion.setdefault("status", "RUNNING" if ingestion.get("running") else "DEGRADED")
    else:
        ingestion = {"ok": False, "status": "UNKNOWN", "running": False, "reasons": ["ingestion_snapshot_missing"]}

    execution_barrier = dict((health or {}).get("execution_barrier") or {})
    mode = str((execution_barrier or {}).get("mode") or os.environ.get("ENGINE_MODE") or "unknown").strip().lower() or "unknown"
    execution_allowed = bool((execution_barrier or {}).get("allowed"))

    lifecycle = dict((health or {}).get("execution_barrier") or {})
    state_name = str((lifecycle or {}).get("runtime_state") or "UNKNOWN").strip().upper() or "UNKNOWN"
    status = _compute_status_name(
        state_name=state_name,
        health_ok=bool((health or {}).get("ok")),
        services_ok=bool((services or {}).get("ok")),
        ingestion_ok=bool((ingestion or {}).get("ok")),
        execution_allowed=execution_allowed,
        kill_switches={},
    )

    snapshot = {
        "ok": bool((health or {}).get("ok")),
        "status": status,
        "state": state_name,
        "mode": mode,
        "execution_mode": mode,
        "execution_allowed": execution_allowed,
        "reasons": list((health or {}).get("reasons") or []),
        "timestamps": timestamps,
        "ts_ms": ts_ms,
        "health": dict(health or {}),
        "model_serving": dict((health or {}).get("model_serving") or {}),
        "alert_lifecycle": dict((health or {}).get("alert_lifecycle") or {}),
        "event_bus": dict((health or {}).get("event_bus") or {}),
        "attribution_quality": dict((health or {}).get("attribution") or {}),
        "ingestion": dict(ingestion or {}),
        "services": dict(services or {}),
        "readiness": {
            "ok": bool((health or {}).get("ok")),
            "ready": bool((health or {}).get("ok")),
        },
        "jobs": [],
        "execution_barrier": execution_barrier,
        "system_state_detail": {
            "ok": bool((health or {}).get("ok")),
            "state": state_name,
            "reasons": list((health or {}).get("reasons") or []),
            "ts_ms": ts_ms,
            "model_serving": dict((health or {}).get("model_serving") or {}),
            "alert_lifecycle": dict((health or {}).get("alert_lifecycle") or {}),
            "event_bus": dict((health or {}).get("event_bus") or {}),
            "attribution_quality": dict((health or {}).get("attribution") or {}),
        },
    }
    return _snapshot_response(snapshot)

# ----------------------------------------------------------------------
# SUPERVISOR
# ----------------------------------------------------------------------
def api_get_supervisor_status(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    sup = None
    try:
        sup = ctx.get("SUPERVISOR") if ctx else None
    except Exception:
        sup = None

    if not sup:
        return _snapshot_response(
            snapshot,
            ok=False,
            status="DEGRADED" if snapshot.get("status") == "RUNNING" else snapshot.get("status"),
            reasons=_dedupe_reasons(snapshot.get("reasons"), ["supervisor_missing"]),
            supervisor={"ok": False, "error": "no supervisor"},
            error="no supervisor",
        )

    try:
        snap = sup.status()
        return _snapshot_response(snapshot, ok=bool(snap.get("ok", True)), supervisor=snap)
    except Exception as e:
        _warn("api_system.supervisor_status", e)
        payload = _snapshot_response(
            snapshot,
            ok=False,
            status="DEGRADED" if snapshot.get("status") == "RUNNING" else snapshot.get("status"),
            reasons=_dedupe_reasons(snapshot.get("reasons"), [f"supervisor_error:{e}"]),
            supervisor={"ok": False, "error": str(e)},
            error=str(e),
        )
        return payload


def api_get_ingestion_status(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    return {
        **snapshot,
        "ok": bool((snapshot.get("ingestion") or {}).get("ok")),
        "ingestion": dict(snapshot.get("ingestion") or {}),
    }

# ----------------------------------------------------------------------
# HEALTH
# ----------------------------------------------------------------------

def _db_table_counts():
    counts = {}
    con = None
    try:
        con = _db_connect(readonly=True)
        for table in ("prices", "events", "labels", "alerts", "job_locks", "job_history", "portfolio_state"):
            table_name = require_allowed_table_name(table)
            table_sql = sql_identifier(table_name)
            try:
                row = con.execute(f"SELECT COUNT(*) FROM {table_sql}").fetchone()
                counts[str(table_name)] = int((row or [0])[0] or 0)
            except Exception as e:
                _warn("api_system.db_table_counts.query", e, table=table_name)
                counts[str(table_name)] = None
        return counts
    except Exception as e:
        _warn("api_system.db_table_counts.connect", e)
        return counts
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn("api_system.db_table_counts.close", e)


def _recent_runtime_errors(limit: int = 10):
    rows_out = []
    con = None
    try:
        con = connect_ro_direct(timeout_s=0.25, busy_timeout_ms=250)
        alert_columns = set()
        try:
            info_rows = con.execute("PRAGMA table_info(alerts)").fetchall() or []
            alert_columns = {
                str(row[1] or "").strip().lower()
                for row in info_rows
                if len(row) > 1 and str(row[1] or "").strip()
            }
        except Exception as e:
            _warn("api_system.recent_runtime_errors.alerts_schema", e, limit=int(limit))
            alert_columns = set()

        if alert_columns:
            severity_expr = "severity" if "severity" in alert_columns else "'WARN'"
            code_expr = "code" if "code" in alert_columns else ("event" if "event" in alert_columns else "''")
            title_expr = "title" if "title" in alert_columns else ("event" if "event" in alert_columns else "severity")
            message_expr = "message" if "message" in alert_columns else ("detail" if "detail" in alert_columns else "severity")
            detail_expr = "detail" if "detail" in alert_columns else ("payload_json" if "payload_json" in alert_columns else "NULL")
            rows = con.execute(
                f"""
                SELECT ts_ms, {severity_expr}, {code_expr}, {title_expr}, {message_expr}, {detail_expr}
                FROM alerts
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall() or []
            for row in rows:
                rows_out.append({
                    "ts_ms": int(row[0] or 0),
                    "severity": str(row[1] or ""),
                    "code": str(row[2] or ""),
                    "title": str(row[3] or ""),
                    "message": str(row[4] or ""),
                    "detail": (None if row[5] is None else str(row[5])),
                })
        else:
            _warn(
                "api_system.recent_runtime_errors.alerts_schema_missing",
                RuntimeError("alerts_table_schema_unavailable"),
                limit=int(limit),
            )

        if not rows_out:
            rows = con.execute(
                """
                SELECT ts_ms, job_name, event, detail, exit_code
                FROM job_history
                WHERE event IN ('exit', 'start_failed', 'stop_hard_kill', 'autorestart_failed', 'autorestart_stall_detected')
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall() or []
            for row in rows:
                rows_out.append({
                    "ts_ms": int(row[0] or 0),
                    "severity": "WARN",
                    "code": str(row[2] or ""),
                    "title": str(row[1] or ""),
                    "message": str(row[3] or ""),
                    "detail": (None if row[4] is None else str(row[4])),
                })
        try:
            seen = {
                (
                    int(item.get("ts_ms") or 0),
                    str(item.get("code") or ""),
                    str(item.get("message") or ""),
                )
                for item in rows_out
                if isinstance(item, dict)
            }
            for row in fetch_recent_runtime_failure_events(limit=int(limit)):
                payload = dict(row.get("payload") or {})
                item = {
                    "ts_ms": int(row.get("ts_ms") or 0),
                    "severity": "ERROR",
                    "code": str(payload.get("root_cause_code") or ""),
                    "title": str(payload.get("failure_scope") or row.get("event_source") or ""),
                    "message": str(payload.get("error_message") or ""),
                    "detail": str(payload.get("error_type") or ""),
                }
                dedupe_key = (item["ts_ms"], item["code"], item["message"])
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                rows_out.append(item)
            rows_out.sort(key=lambda item: int((item or {}).get("ts_ms") or 0), reverse=True)
            rows_out = rows_out[: int(limit)]
        except Exception as e:
            _warn("api_system.recent_runtime_errors.event_log_query", e, limit=int(limit))
        return rows_out
    except Exception as e:
        _warn("api_system.recent_runtime_errors.fetch", e, limit=int(limit))
        return rows_out
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn("api_system.recent_runtime_errors.close", e)


def _build_runtime_health(_parsed, ctx):
    try:
        health = _cached_health_snapshot()
    except Exception as e:
        health = {"ok": False, "error": str(e), "reasons": [f"health_exception:{e}"]}

    if not isinstance(health, dict):
        health = {"ok": False, "reasons": ["invalid_health_snapshot"]}

    health.setdefault("ok", False)
    health.setdefault("reasons", [])

    try:
        preflight = run_preflight()
    except Exception as e:
        preflight = {"ok": False, "notes": [f"preflight_exception:{e}"], "error": str(e)}

    try:
        sys_state = api_get_system_state(_parsed, ctx)
    except Exception as e:
        sys_state = {"ok": False, "state": "UNKNOWN", "error": str(e)}

    try:
        sup = ctx.get("SUPERVISOR") if ctx else None
        graph = sup.validate_graph(strict=True) if sup else {"ok": False, "error": "supervisor_missing"}
    except Exception as e:
        graph = {"ok": False, "error": str(e)}

    jobs = health.get("job_summary") or {}
    providers = health.get("providers") or {}
    prices = health.get("prices") or {}
    events = health.get("events") or {}
    labels = health.get("labels") or {}
    model = health.get("model") or {}
    execution_barrier = health.get("execution_barrier") or {}
    training = health.get("training") or {}
    db = health.get("db") or {}
    schema = health.get("schema") or {}

    panels = [
        {
            "id": "system_status",
            "label": "System status",
            "ok": bool(sys_state.get("ok")),
            "detail": str(sys_state.get("state") or "UNKNOWN"),
        },
        {
            "id": "database_health",
            "label": "Database health",
            "ok": bool(db.get("ok")),
            "detail": f"quick_check={db.get('quick_check')} size={db.get('size_bytes')}",
        },
        {
            "id": "schema_health",
            "label": "Schema health",
            "ok": bool(schema.get("ok", True)),
            "detail": f"missing_tables={len(schema.get('missing_tables') or [])} missing_cols={len(schema.get('missing_cols') or [])}",
        },
        {
            "id": "ingestion_status",
            "label": "Data ingestion status",
            "ok": bool(prices.get("ok")) and bool(providers.get("ok")),
            "detail": f"prices_age_s={prices.get('age_s')} providers={providers.get('healthy')}/{providers.get('total')}",
        },
        {
            "id": "event_pipeline",
            "label": "Event pipeline status",
            "ok": bool(events.get("ok")),
            "detail": f"events_age_s={events.get('age_s')}",
        },
        {
            "id": "training_status",
            "label": "Model training status",
            "ok": bool(model.get("ok")) and bool(labels.get("ok")),
            "detail": f"labels={labels.get('count')} support_n={model.get('support_n')} training_allowed={training.get('allowed')}",
        },
        {
            "id": "trading_readiness",
            "label": "Trading readiness",
            "ok": bool(execution_barrier.get("allowed")),
            "detail": str(execution_barrier.get("reason") or "not_ready"),
        },
        {
            "id": "watchdogs",
            "label": "Watchdogs",
            "ok": bool(jobs.get("ok")),
            "detail": f"stale_jobs={jobs.get('stale')} total_jobs={jobs.get('total')}",
        },
        {
            "id": "preflight",
            "label": "Startup preflight",
            "ok": bool(preflight.get("ok")),
            "detail": "; ".join(str(x) for x in (preflight.get("notes") or [])) or "preflight_ok",
        },
    ]

    ok = all(bool(p.get("ok")) for p in panels)

    database_health = dict(db or {})
    database_health["schema"] = dict(schema or {})

    price_ingestion_status = {
        "ok": bool(prices.get("ok")) and bool(providers.get("ok")),
        "prices": dict(prices or {}),
        "providers": dict(providers or {}),
    }

    event_pipeline_status = dict(events or {})
    label_pipeline_status = dict(labels or {})
    model_availability = {
        "ok": bool(model.get("ok")),
        "model": dict(model or {}),
        "training": dict(training or {}),
    }

    try:
        jobs_payload = ctx["JOBS"].list_jobs() if ctx and ctx.get("JOBS") else []
    except Exception as e:
        _warn("api_system.runtime_health.jobs_payload", e)
        jobs_payload = []

    job_supervisor_state = {
        "ok": bool(graph.get("ok")) and bool(jobs.get("ok", True)),
        "graph": graph,
        "jobs": jobs_payload,
        "job_summary": dict(jobs or {}),
    }

    storage_summary = {
        "db_path": str(DB_PATH),
        "db_exists": bool(Path(DB_PATH).exists()),
        "db_size_bytes": int(database_health.get("size_bytes") or 0),
        "wal_bytes": int(database_health.get("wal_bytes") or 0),
    }

    return {
        "ok": bool(ok),
        "ts_ms": int(time.time() * 1000),
        "health": health,
        "preflight": preflight,
        "system_state": sys_state,
        "graph": graph,
        "panels": panels,
        "database_health": database_health,
        "price_ingestion_status": price_ingestion_status,
        "event_pipeline_status": event_pipeline_status,
        "label_pipeline_status": label_pipeline_status,
        "model_availability": model_availability,
        "job_supervisor_state": job_supervisor_state,
        "storage": storage_summary,
    }


def _build_trading_readiness(_parsed, ctx):
    runtime = _build_runtime_health(_parsed, ctx)
    health = runtime.get("health") or {}
    sys_state = runtime.get("system_state") or {}
    graph = runtime.get("graph") or {}
    preflight = runtime.get("preflight") or {}

    readiness = get_readiness_snapshot(
        health=health,
        preflight=preflight,
        system_state=sys_state,
        graph=graph,
    )

    execution_barrier = health.get("execution_barrier") or {}

    readiness["health_ok"] = bool(health.get("ok"))
    readiness["graph_valid"] = bool(graph.get("ok"))
    readiness["execution_allowed"] = bool(execution_barrier.get("allowed"))
    readiness["runtime"] = runtime

    return readiness


def api_get_health(_parsed, ctx=None):
    """Return the cached system health snapshot used by the operator UI.

    Parameters
    ----------
    _parsed : Any
        Accepted for handler signature compatibility and ignored.
    ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        Cached health payload when available. If the cache payload is invalid,
        returns a degraded fail-closed response with ``ok=False`` and
        ``ts_ms`` in epoch milliseconds.
    """
    snapshot = _cached_health_snapshot(allow_sync_on_miss=False)
    if isinstance(snapshot, dict):
        return snapshot
    return {
        "ok": False,
        "status": "DEGRADED",
        "error": "health_snapshot_invalid",
        "ts_ms": int(time.time() * 1000),
    }


def api_get_liveness(_parsed, ctx=None):
    """Return a pure liveness payload for process-level probing only."""

    lifecycle = {}
    try:
        health_snapshot = _cached_health_snapshot(allow_sync_on_miss=False)
        lifecycle = _dict_or_empty((health_snapshot or {}).get("lifecycle"))
    except Exception as e:
        _warn("api_system.liveness", e)
        lifecycle = {}

    return {
        "ok": True,
        "alive": True,
        "status": "ALIVE",
        "ts_ms": int(time.time() * 1000),
        "pid": int(os.getpid()),
        "lifecycle": lifecycle,
        "note": "Liveness only; use /api/readiness for serve/trade readiness.",
    }


def api_get_competition_view(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    health = _normalized_health_from_snapshot(snapshot)
    competition_health = dict((health or {}).get("competition") or {})
    system_state_detail = dict(snapshot.get("system_state_detail") or {})
    active_symbols = list(((system_state_detail.get("competition") or {}).get("active_symbols")) or [])
    runtime = current_competition_snapshot(active_symbols=active_symbols)

    champions = list(runtime.get("champions") or [])
    challengers = list(runtime.get("challengers") or [])
    champion = dict(runtime.get("champion") or {})
    capital_plan = dict(runtime.get("capital_plan") or {})
    replay_status = dict(runtime.get("replay_validation_status") or {})
    self_critic = dict(runtime.get("self_critic") or {})
    cycle_status = dict(runtime.get("cycle_status") or {})

    summary = {
        "champion_model_name": str(champion.get("model_name") or ""),
        "champion_symbol": str(champion.get("symbol") or ""),
        "champion_horizon_s": int(champion.get("horizon_s") or 0),
        "champions": int(len(champions)),
        "challengers": int(len(challengers)),
        "active_symbols": int(len(list(runtime.get("active_symbols") or []))),
        "allocation_groups": int(len(dict(capital_plan.get("allocations") or {}))),
        "critic_blocked_keys": int(len(list(self_critic.get("blocked_keys") or []))),
        "replay_ready": str(replay_status.get("status") or "") == "ready",
        "cycle_status": str(cycle_status.get("status") or "missing"),
    }

    return _snapshot_response(
        snapshot,
        ok=bool(competition_health.get("ok")),
        competition={
            "summary": summary,
            "runtime": runtime,
            "health": competition_health,
        },
    )


def api_get_replay_freshness(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    health = _normalized_health_from_snapshot(snapshot)
    competition_health = dict((health or {}).get("competition") or {})
    replay = _meta_json("competition_replay_validation")
    replay_status = _meta_json("competition_replay_validation_status")
    models = dict(replay.get("models") or {})

    approved = 0
    source_counts = {}
    latest_window_end_ms = 0
    for row in models.values():
        if not isinstance(row, dict):
            continue
        if bool(row.get("approved")):
            approved += 1
        source = str(row.get("source") or "unknown")
        source_counts[source] = int(source_counts.get(source) or 0) + 1
        try:
            latest_window_end_ms = max(latest_window_end_ms, int(row.get("window_end_ms") or 0))
        except Exception as e:
            _warn("api_system.replay_freshness.window_end_parse", e, row=row)

    updated_ts_ms = int(replay_status.get("updated_ts_ms") or 0)
    now_ms = _ts_ms()
    age_ms = max(0, now_ms - updated_ts_ms) if updated_ts_ms > 0 else None
    fresh = bool(replay_status.get("fresh"))
    stale = bool(replay_status.get("stale")) or (fresh is False and updated_ts_ms > 0)

    return _snapshot_response(
        snapshot,
        ok=bool(str(replay_status.get("status") or "") == "ready" and not stale),
        replay_freshness={
            "summary": {
                "status": str(replay_status.get("status") or "missing"),
                "fresh": fresh,
                "stale": stale,
                "updated_ts_ms": updated_ts_ms or None,
                "age_ms": age_ms,
                "model_count": int(replay_status.get("model_count") or len(models)),
                "approved_model_count": int(approved),
                "latest_window_end_ms": latest_window_end_ms or None,
            },
            "status": replay_status,
            "models": models,
            "sources": source_counts,
            "health": {
                "replay_status": str(competition_health.get("replay_status") or "missing"),
                "replay_age_s": competition_health.get("replay_age_s"),
                "reasons": list(competition_health.get("reasons") or []),
            },
        },
    )


def api_get_attribution_quality(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    health = _normalized_health_from_snapshot(snapshot)
    attribution_health = dict((health or {}).get("attribution") or {})
    completeness = _meta_json("attribution_completeness")
    repair = _meta_json("execution_order_model_identity_repair")
    historical_repair = _meta_json("trade_attribution_historical_repair")
    poll_state = _meta_json("execution_poll_and_attrib_last")

    summary = {
        "rows": int(completeness.get("rows") or 0),
        "authoritative_model_present": int(completeness.get("authoritative_model_present") or 0),
        "authoritative_model_present_ratio": float(completeness.get("authoritative_model_present_ratio") or 0.0),
        "regime_present_ratio": float(completeness.get("regime_present_ratio") or 0.0),
        "policy_present_ratio": float(completeness.get("policy_present_ratio") or 0.0),
        "historical_repair_ok": bool(historical_repair.get("ok")) if historical_repair else None,
        "recent_identity_repair_scanned": int(repair.get("rows_scanned") or 0),
        "recent_identity_repair_updated": int(repair.get("rows_updated") or 0),
        "updated_ts_ms": int(poll_state.get("ts_ms") or historical_repair.get("ts_ms") or 0) or None,
        "warning_row_count": int(attribution_health.get("warning_row_count") or 0),
        "max_residual_share": float(attribution_health.get("max_residual_share") or 0.0),
        "latest_warning_ts_ms": attribution_health.get("latest_warning_ts_ms"),
        "latest_reconstruction_error": dict(attribution_health.get("latest_reconstruction_error") or {}),
        "quality_status": str(attribution_health.get("quality_status") or "ok"),
    }

    return _snapshot_response(
        snapshot,
        ok=bool(attribution_health.get("ok")),
        attribution_quality={
            "summary": summary,
            "completeness": completeness,
            "recent_repair": repair,
            "historical_repair": historical_repair,
            "health": attribution_health,
        },
    )


def api_get_runtime_health(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    try:
        runtime = _build_runtime_health(_parsed, ctx)
        return _snapshot_response(
            snapshot,
            ok=bool((snapshot.get("health") or {}).get("ok")),
            runtime_health=runtime,
            panels=list((runtime or {}).get("panels") or []),
        )
    except Exception as e:
        failure = failure_response(
            log,
            event="api_system_runtime_health_failed",
            code="API_SYSTEM_RUNTIME_HEALTH_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_system",
            ctx=ctx,
            extra={"status": str(snapshot.get("status") or "")},
        )
        return _snapshot_response(
            snapshot,
            ok=False,
            status="DEGRADED" if snapshot.get("status") == "RUNNING" else snapshot.get("status"),
            reasons=_dedupe_reasons(snapshot.get("reasons"), [f"runtime_health_error:{e}"]),
            runtime_health={"ok": False, "error": str(e), "panels": []},
            panels=[],
            error=str(e),
            root_cause_code=failure.get("root_cause_code"),
            failure_scope=failure.get("failure_scope"),
            failure_type=failure.get("failure_type"),
            system_state_snapshot=failure.get("system_state_snapshot"),
        )


def api_get_status(_parsed, ctx=None):
    """Return the full aggregated runtime status snapshot.

    Parameters
    ----------
    _parsed : Any
        Parsed request object forwarded into snapshot construction.
    ctx : Any, optional
        Optional request context forwarded into snapshot construction.

    Returns
    -------
    dict
        Aggregated system snapshot from ``_build_system_snapshot`` wrapped by
        ``_snapshot_response``.
    """
    snapshot = _build_readiness_snapshot(_parsed, ctx)
    return _snapshot_response(snapshot)


# ----------------------------------------------------------------------
# READINESS
# ----------------------------------------------------------------------
def api_get_readiness(_parsed, ctx=None):
    """Return a condensed readiness view derived from the system snapshot.

    Parameters
    ----------
    _parsed : Any
        Parsed request object forwarded into snapshot construction.
    ctx : Any, optional
        Optional request context forwarded into snapshot construction.

    Returns
    -------
    dict
        Snapshot payload augmented with boolean summary fields including
        ``ok``, ``health_ok``, ``graph_valid``, and ``system_state``.

    Notes
    -----
    ``ok`` is driven by the nested ``readiness`` payload rather than the
    broader status snapshot.
    """
    boot_diagnostics = {}
    if isinstance(ctx, dict):
        boot_fn = ctx.get("_boot_diagnostics")
        if callable(boot_fn):
            try:
                boot_diagnostics = dict(boot_fn() or {})
            except Exception as e:
                _warn("api_system.readiness.boot_diagnostics", e)
                boot_diagnostics = {}
    storage = dict(boot_diagnostics.get("storage") or {})
    if not storage:
        try:
            from engine.runtime.storage_pool import storage_readiness_snapshot

            storage = dict(storage_readiness_snapshot() or {})
        except Exception as e:
            _warn("api_system.readiness.storage_snapshot", e)
            storage = {}

    storage_checked = bool(storage.get("checked"))
    storage_ok = storage.get("ok")
    if storage and storage_ok is not True:
        reason = "storage_unavailable" if storage_checked and storage_ok is False else "storage_not_checked"
        status = "FAILED" if reason == "storage_unavailable" else "STARTING"
        ts_ms = _ts_ms()
        issue = {
            "code": reason,
            "level": "error" if reason == "storage_unavailable" else "warn",
            "message": "Runtime storage is unavailable." if reason == "storage_unavailable" else "Runtime storage has not been checked yet.",
            "detail": str(storage.get("error") or storage.get("detail") or reason),
        }
        production_validation = {
            "status": status.lower(),
            "safe_to_operate": False,
            "unsafe_to_operate": True,
            "summary_reason": reason,
            "current_degraded_reasons": [reason],
            "storage": dict(storage),
        }
        return {
            "ok": False,
            "ready": False,
            "degraded": reason == "storage_unavailable",
            "failed": reason == "storage_unavailable",
            "safe_to_operate": False,
            "unsafe_to_operate": True,
            "status": status,
            "storage": dict(storage),
            "storage_degraded": reason == "storage_unavailable",
            "state": "DEGRADED" if reason == "storage_unavailable" else "STARTING",
            "mode": str(os.environ.get("ENGINE_MODE") or "unknown"),
            "execution_mode": str(os.environ.get("ENGINE_MODE") or "unknown"),
            "execution_allowed": False,
            "reasons": [reason],
            "ts_ms": ts_ms,
            "timestamps": {"ts_ms": ts_ms, "snapshot_ts_ms": ts_ms},
            "readiness": {"ok": False, "ready": False, "issues": [issue], "steps": []},
            "production_validation": production_validation,
            "health_ok": False,
            "graph_valid": False,
            "system_state": "DEGRADED" if reason == "storage_unavailable" else "STARTING",
            "system_state_detail": {"state": "DEGRADED" if reason == "storage_unavailable" else "STARTING", "reason": reason},
        }

    snapshot = _build_readiness_snapshot(_parsed, ctx)
    production_validation = dict(snapshot.get("production_validation") or {})
    storage_degraded = bool(storage.get("checked") and storage.get("ok") is False)
    if storage:
        production_validation.setdefault("storage", dict(storage))
    production_status = str(production_validation.get("status") or "failed").strip().lower() or "failed"
    reasons = _dedupe_reasons(
        snapshot.get("reasons"),
        production_validation.get("current_degraded_reasons"),
        [production_validation.get("summary_reason")],
        ["storage_unavailable"] if storage_degraded else [],
    )
    return {
        "ok": production_status == "healthy",
        "ready": bool(production_validation.get("safe_to_operate")),
        "degraded": production_status == "degraded" or storage_degraded,
        "failed": production_status == "failed",
        "safe_to_operate": bool(production_validation.get("safe_to_operate")),
        "unsafe_to_operate": bool(production_validation.get("unsafe_to_operate")),
        "status": production_status.upper(),
        "storage": storage,
        "storage_degraded": storage_degraded,
        "state": str(snapshot.get("state") or "UNKNOWN"),
        "mode": str(snapshot.get("mode") or "unknown"),
        "execution_mode": str(snapshot.get("execution_mode") or snapshot.get("mode") or "unknown"),
        "execution_allowed": bool(snapshot.get("execution_allowed")),
        "reasons": reasons,
        "ts_ms": int(snapshot.get("ts_ms") or _ts_ms()),
        "timestamps": dict(snapshot.get("timestamps") or {}),
        "readiness": dict(snapshot.get("readiness") or {}),
        "production_validation": production_validation,
        "health_ok": bool((snapshot.get("health") or {}).get("ok")),
        "graph_valid": bool((snapshot.get("graph") or {}).get("ok")),
        "system_state": str(snapshot.get("state") or "UNKNOWN"),
        "ingestion": dict(snapshot.get("ingestion") or {}),
        "services": dict(snapshot.get("services") or {}),
        "critical_blockers": list(snapshot.get("critical_blockers") or production_validation.get("critical_failures") or []),
    }


def api_get_trading_readiness(_parsed, ctx=None):
    """Alias for :func:`api_get_readiness` used by trading-specific clients."""
    return api_get_readiness(_parsed, ctx)


def api_get_preflight_report(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    try:
        out = run_preflight()
        out["report_generated_ts_ms"] = int(time.time() * 1000)
        out.setdefault("ok", False)
        return _snapshot_response(snapshot, ok=bool(out.get("ok")), preflight_report=out)
    except Exception as e:
        failure = failure_response(
            log,
            event="api_system_preflight_report_failed",
            code="API_SYSTEM_PREFLIGHT_REPORT_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_system",
            ctx=ctx,
            extra={"status": str(snapshot.get("status") or "")},
        )
        return _snapshot_response(
            snapshot,
            ok=False,
            status="DEGRADED" if snapshot.get("status") == "RUNNING" else snapshot.get("status"),
            reasons=_dedupe_reasons(snapshot.get("reasons"), [f"preflight_report_error:{e}"]),
            preflight_report={"ok": False, "error": str(e), "notes": [str(e)]},
            error=str(e),
            root_cause_code=failure.get("root_cause_code"),
            failure_scope=failure.get("failure_scope"),
            failure_type=failure.get("failure_type"),
            system_state_snapshot=failure.get("system_state_snapshot"),
        )


def api_get_runtime_watchdogs(_parsed, ctx=None):
    """Return heartbeat and freshness watchdog state for critical runtime jobs.

    Parameters
    ----------
    _parsed : Any
        Accepted for handler signature compatibility and ignored.
    ctx : Any, optional
        Optional request context used to resolve job payloads.

    Returns
    -------
    dict
        Watchdog payload containing per-job heartbeat timestamps, heartbeat ages
        in seconds, restart counters, and freshness summaries for ingestion and
        price-feed pipelines.
    """
    ts_ms = int(time.time() * 1000)
    try:
        health = _cached_health_snapshot()
    except Exception as e:
        _warn("api_system.runtime_watchdogs.health", e)
        health = {"ok": False, "error": str(e), "reasons": [f"health_exception:{e}"]}
    jobs_payload, _ = _get_jobs_payload(ctx)

    jobs_by_name = {str(j.get("name") or ""): j for j in jobs_payload if str(j.get("name") or "")}

    provider_monitor = jobs_by_name.get("provider_monitor") or {}
    metrics_collector = jobs_by_name.get("metrics_collector") or {}
    ingestion_runtime = jobs_by_name.get("ingestion_runtime") or {}

    restart_counters = {}
    for row in jobs_payload:
        name = str(row.get("name") or "")
        if not name:
            continue
        restart_counters[name] = int(row.get("restart_count") or 0)

    out = {
        "ok": bool(health.get("ok")),
        "ts_ms": ts_ms,
        "provider_monitor": {
            "running": bool(provider_monitor.get("running")),
            "heartbeat_ts_ms": provider_monitor.get("heartbeat_ts_ms"),
            "heartbeat_age_s": provider_monitor.get("heartbeat_age_s"),
            "restart_count": int(provider_monitor.get("restart_count") or 0),
            "stale": bool(provider_monitor.get("stale")),
        },
        "metrics_collector": {
            "running": bool(metrics_collector.get("running")),
            "heartbeat_ts_ms": metrics_collector.get("heartbeat_ts_ms"),
            "heartbeat_age_s": metrics_collector.get("heartbeat_age_s"),
            "restart_count": int(metrics_collector.get("restart_count") or 0),
            "stale": bool(metrics_collector.get("stale")),
        },
        "price_feed_freshness": dict(health.get("prices") or {}),
        "pipeline_watchdog_state": {
            "ingestion_runtime": {
                "running": bool(ingestion_runtime.get("running")),
                "heartbeat_ts_ms": ingestion_runtime.get("heartbeat_ts_ms"),
                "heartbeat_age_s": ingestion_runtime.get("heartbeat_age_s"),
                "restart_count": int(ingestion_runtime.get("restart_count") or 0),
                "stale": bool(ingestion_runtime.get("stale")),
            },
            "ingestion_freshness": dict(health.get("ingestion_freshness") or {}),
            "events": dict(health.get("events") or {}),
            "labels": dict(health.get("labels") or {}),
            "model": dict(health.get("model") or {}),
        },
        "ingestion_freshness": dict(health.get("ingestion_freshness") or {}),
        "job_restart_counters": restart_counters,
        "job_summary": dict(health.get("job_summary") or {}),
    }
    out["ok"] = bool(
        out["price_feed_freshness"].get("ok")
        and not out["provider_monitor"].get("stale")
        and not out["metrics_collector"].get("stale")
    )
    return out


def api_get_service_status(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    return {
        **snapshot,
        "ok": bool((snapshot.get("services") or {}).get("ok")),
        "services": dict(snapshot.get("services") or {}),
    }


def _support_snapshot_schema(mode: str):
    return {
        "name": "operator_repair_snapshot",
        "version": 3,
        "producer": "engine.api.api_system",
        "mode": str(mode or "repair"),
        "stable_sections": [
            "snapshot_schema",
            "snapshot_mode",
            "status",
            "state",
            "mode",
            "execution_mode",
            "execution_allowed",
            "system_stage",
            "critical_blockers",
            "root_cause_candidates",
            "diagnostics",
            "production_validation",
            "system_health",
            "trading_readiness",
            "preflight_report",
            "database_counts",
            "database_debug",
            "job_registry_validation",
            "job_status",
            "daemon_status",
            "runtime_watchdogs",
            "recent_errors",
            "evidence",
            "timestamps",
        ],
    }


def _support_snapshot_inspect_files_first(snapshot, architecture):
    reasons = [str(x or "") for x in _dedupe_reasons(
        snapshot.get("critical_blockers"),
        snapshot.get("root_cause_candidates"),
        snapshot.get("reasons"),
        (architecture or {}).get("errors"),
    )]

    files = []

    def _add(path: str):
        path = str(path or "").strip()
        if path and path not in files:
            files.append(path)

    _add("start_system.py")
    _add("dashboard_server.py")
    _add("engine/api/api_system.py")
    _add("engine/runtime/health.py")
    _add("engine/runtime/storage.py")

    if any("invalid_python_entry:" in r or "missing_callable_entrypoint:" in r for r in reasons):
        _add("engine/runtime/job_registry.py")

    if any("required_table_missing:" in r or "schema" in r or "db" in r for r in reasons):
        _add("engine/runtime/jobs/repair_schema.py")
        _add("engine/runtime/storage.py")

    if any("jobs_" in r or "supervisor" in r or "restart" in r or "stale" in r for r in reasons):
        _add("engine/runtime/supervisor.py")
        _add("engine/runtime/jobs_manager.py")
        _add("engine/runtime/job_registry.py")

    if any("prices" in r or "provider" in r or "ingestion" in r or "events" in r for r in reasons):
        _add("start_ingestion.py")
        _add("engine/runtime/ingestion_runtime.py")

    return files[:12]


def _support_snapshot_top_failures(snapshot, recent_errors, architecture):
    out = []

    for item in (snapshot.get("critical_blockers") or []):
        value = str(item or "").strip()
        if value:
            out.append(value)

    for item in (snapshot.get("root_cause_candidates") or []):
        value = str(item or "").strip()
        if value:
            out.append(value)

    for item in (architecture or {}).get("errors") or []:
        value = str(item or "").strip()
        if value:
            out.append(value)

    for row in (recent_errors or []):
        code = str((row or {}).get("code") or "").strip()
        title = str((row or {}).get("title") or "").strip()
        if code:
            out.append(code)
        elif title:
            out.append(title)

    return _dedupe_reasons(out)[:20]


def _support_snapshot_suspected_root_causes(snapshot, architecture, db_debug):
    out = []

    for item in (snapshot.get("root_cause_candidates") or []):
        value = str(item or "").strip()
        if value:
            out.append(value)

    for item in (architecture or {}).get("errors") or []:
        value = str(item or "").strip()
        if value:
            out.append(value)

    for item in ((snapshot.get("schema") or {}).get("missing_tables") or []):
        value = str(item or "").strip()
        if value:
            out.append(f"missing_table:{value}")

    for item in ((snapshot.get("critical_blockers") or [])):
        value = str(item or "").strip()
        if value:
            out.append(value)

    for row in ((db_debug or {}).get("long_lived_readers") or []):
        owner = str((row or {}).get("owner") or "").strip()
        age_ms = int((row or {}).get("age_ms") or 0)
        out.append(f"sqlite_long_lived_reader:{owner or 'unknown'}:{age_ms}")

    return _dedupe_reasons(out)[:20]


def _build_support_snapshot(snapshot, _parsed, ctx=None, mode: str = "repair"):
    mode = str(mode or "repair").strip().lower() or "repair"
    if mode not in ("quick", "repair", "deep"):
        mode = "repair"

    recent_errors_limit = 5 if mode == "quick" else (25 if mode == "deep" else 10)
    job_limit = 20 if mode == "quick" else (200 if mode == "deep" else 60)

    try:
        db_debug = get_db_debug_snapshot()
    except Exception as e:
        db_debug = {"ok": False, "error": str(e)}

    try:
        architecture = validate_runtime_architecture()
    except Exception as e:
        architecture = {"ok": False, "errors": [f"validate_runtime_architecture_error:{e}"]}

    recent_errors = _recent_runtime_errors(limit=recent_errors_limit)
    job_status = list(snapshot.get("jobs") or [])[:job_limit]
    daemon_status = dict(snapshot.get("graph") or {})
    runtime_watchdogs = dict(snapshot.get("runtime_watchdogs") or {})
    if not runtime_watchdogs:
        runtime_watchdogs = api_get_runtime_watchdogs(_parsed, ctx)
    database_counts = _db_table_counts()
    production_validation = dict(snapshot.get("production_validation") or {})

    diagnostics = {
        "top_failures": _support_snapshot_top_failures(snapshot, recent_errors, architecture),
        "blocking_issues": list(snapshot.get("critical_blockers") or []),
        "suspected_root_causes": _support_snapshot_suspected_root_causes(snapshot, architecture, db_debug),
        "inspect_files_first": _support_snapshot_inspect_files_first(snapshot, architecture),
        "startup_trace": dict((db_debug or {}).get("startup_trace") or {}),
        "import_smoke": dict((db_debug or {}).get("import_smoke") or {}),
        "job_launch_trace": list((db_debug or {}).get("job_launch_trace") or []),
        "db_validation": dict((db_debug or {}).get("db_validation") or {}),
        "ingestion_state": dict((db_debug or {}).get("ingestion_state") or {}),
        "supervisor_analysis": dict((db_debug or {}).get("supervisor_analysis") or {}),
        "failure_classification": dict((db_debug or {}).get("failure_classification") or {}),
        "production_status": str(production_validation.get("status") or "failed"),
        "safe_to_operate": bool(production_validation.get("safe_to_operate")),
        "failing_components": list(production_validation.get("failing_components") or []),
        "current_degraded_reasons": list(production_validation.get("current_degraded_reasons") or []),
        "last_successful_ingestion_event": dict(production_validation.get("last_successful_ingestion_event") or {}),
        "last_successful_db_write": dict(production_validation.get("last_successful_db_write") or {}),
        "last_successful_score_or_model_output": dict(production_validation.get("last_successful_score_or_model_output") or {}),
        "last_successful_execution_event": dict(production_validation.get("last_successful_execution_event") or {}),
        "restart_retry_loop_indicators": dict(production_validation.get("restart_retry_loop_indicators") or {}),
        "stale_data_indicators": list(production_validation.get("stale_data_indicators") or []),
        "ui_critical_endpoint_status": list(production_validation.get("ui_critical_endpoint_status") or []),
    }

    evidence = {
        "system_snapshot": snapshot,
        "system_health": dict(snapshot.get("health") or {}),
        "trading_readiness": dict(snapshot.get("readiness") or {}),
        "production_validation": production_validation,
        "preflight_report": dict(snapshot.get("preflight") or {}),
        "database_counts": database_counts,
        "database_debug": db_debug,
        "job_registry_validation": architecture,
        "job_status": job_status,
        "daemon_status": daemon_status,
        "runtime_watchdogs": runtime_watchdogs,
        "recent_errors": recent_errors,
        "execution_barrier": dict(snapshot.get("execution_barrier") or {}),
        "schema": dict(snapshot.get("schema") or {}),
        "system_state_detail": dict(snapshot.get("system_state_detail") or {}),
        "startup_trace": dict((db_debug or {}).get("startup_trace") or {}),
        "import_smoke": dict((db_debug or {}).get("import_smoke") or {}),
        "job_launch_trace": list((db_debug or {}).get("job_launch_trace") or []),
        "db_validation": dict((db_debug or {}).get("db_validation") or {}),
        "ingestion_state": dict((db_debug or {}).get("ingestion_state") or {}),
        "supervisor_analysis": dict((db_debug or {}).get("supervisor_analysis") or {}),
        "failure_classification": dict((db_debug or {}).get("failure_classification") or {}),
    }

    return {
        **snapshot,
        "snapshot_schema": _support_snapshot_schema(mode),
        "snapshot_mode": mode,
        "production_validation": production_validation,
        "system_health": dict(snapshot.get("health") or {}),
        "trading_readiness": dict(snapshot.get("readiness") or {}),
        "preflight_report": dict(snapshot.get("preflight") or {}),
        "database_counts": database_counts,
        "database_debug": db_debug,
        "job_registry_validation": architecture,
        "job_status": job_status,
        "daemon_status": daemon_status,
        "runtime_watchdogs": runtime_watchdogs,
        "recent_errors": recent_errors,
        "startup_trace": dict((db_debug or {}).get("startup_trace") or {}),
        "import_smoke": dict((db_debug or {}).get("import_smoke") or {}),
        "job_launch_trace": list((db_debug or {}).get("job_launch_trace") or []),
        "db_validation": dict((db_debug or {}).get("db_validation") or {}),
        "ingestion_state": dict((db_debug or {}).get("ingestion_state") or {}),
        "supervisor_analysis": dict((db_debug or {}).get("supervisor_analysis") or {}),
        "failure_classification": dict((db_debug or {}).get("failure_classification") or {}),
        "diagnostics": diagnostics,
        "evidence": evidence,
    }


def api_get_support_snapshot(_parsed, ctx=None):
    """Return the operator support snapshot used for guided diagnostics.

    Parameters
    ----------
    _parsed : Any
        Parsed request/query container. The optional `mode` query parameter is
        forwarded to the support-snapshot builder.
    ctx : dict[str, Any] | None, optional
        Request context from the dashboard server.

    Returns
    -------
    dict[str, Any]
        Support payload containing the current system snapshot plus the
        diagnostics, evidence, and schema sections assembled by
        `_build_support_snapshot`.
    """

    mode = str(_qs(_parsed, "mode", "repair") or "repair").strip().lower() or "repair"
    snapshot = _build_system_snapshot(_parsed, ctx)
    return _build_support_snapshot(snapshot, _parsed, ctx, mode=mode)

# ----------------------------------------------------------------------
# PROVIDER TELEMETRY
# ----------------------------------------------------------------------

def api_get_provider_telemetry(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    try:
        status = market_data_status()
        lifecycle = {}
        try:
            lifecycle = ((snapshot.get("health") or {}).get("health") or {}).get("lifecycle") or {}
        except Exception as e:
            _warn("api_system.provider_telemetry.lifecycle_extract", e)
            lifecycle = {}
        provider_telemetry = {
            "ok": bool(status.get("ok")),
            "running": bool(status.get("running")),
            "active_child": str(status.get("active_child") or ""),
            "child_pid": int(status.get("child_pid") or 0),
            "healthy_providers": int(status.get("healthy_providers") or 0),
            "fresh_rows": int(status.get("fresh_rows") or 0),
            "fresh_symbols": int(status.get("fresh_symbols") or 0),
            "last_price_ts_ms": int(status.get("last_price_ts_ms") or 0),
            "price_age_ms": int(status.get("price_age_ms") or 0),
            "providers": status.get("providers") or {},
            "updated_ts_ms": int(status.get("updated_ts_ms") or 0),
            "owner": str(status.get("owner") or ""),
            "last_seq": int(status.get("last_seq") or 0),
            "lifecycle": lifecycle,
            "runtime_health_providers": (((snapshot.get("health") or {}).get("health") or {}).get("providers") or {}),
            "ingestion_pipelines": pipeline_health_summary(),
        }
        return _snapshot_response(snapshot, ok=bool(provider_telemetry.get("ok")), provider_telemetry=provider_telemetry)
    except Exception as e:
        failure = failure_response(
            log,
            event="api_system_provider_telemetry_failed",
            code="API_SYSTEM_PROVIDER_TELEMETRY_FAILED",
            message=str(e),
            error=e,
            component="engine.api.api_system",
            ctx=ctx,
            extra={"status": str(snapshot.get("status") or "")},
        )
        return _snapshot_response(
            snapshot,
            ok=False,
            status="DEGRADED" if snapshot.get("status") == "RUNNING" else snapshot.get("status"),
            reasons=_dedupe_reasons(snapshot.get("reasons"), [f"provider_telemetry_error:{e}"]),
            provider_telemetry={"ok": False, "error": str(e)},
            error=str(e),
            root_cause_code=failure.get("root_cause_code"),
            failure_scope=failure.get("failure_scope"),
            failure_type=failure.get("failure_type"),
            system_state_snapshot=failure.get("system_state_snapshot"),
        )


# ----------------------------------------------------------------------
# SUPERVISOR DIAGNOSTICS
# ----------------------------------------------------------------------

def api_get_supervisor_diagnostics(_parsed, ctx=None):
    snapshot = _build_system_snapshot(_parsed, ctx)
    services = dict(snapshot.get("services") or {})
    graph = dict(snapshot.get("graph") or {})
    jobs = list(snapshot.get("jobs") or [])
    ingestion = dict(snapshot.get("ingestion") or {})

    counts = {
        "total": len(jobs),
        "running": sum(1 for row in jobs if bool((row or {}).get("running"))),
        "stale": sum(1 for row in jobs if bool((row or {}).get("stale"))),
        "failed": sum(1 for row in jobs if (row or {}).get("exit_code") not in (None, 0)),
        "locked": sum(1 for row in jobs if bool((row or {}).get("lock_owner")) and not bool((row or {}).get("running"))),
    }

    diagnostics = {
        "ok": bool(graph.get("ok")) and bool(services.get("ok")) and bool(ingestion.get("ok", True)),
        "enabled": True,
        "state": str(snapshot.get("state") or services.get("state") or "UNKNOWN"),
        "status": str(snapshot.get("status") or services.get("engine", {}).get("state") or "UNKNOWN"),
        "counts": counts,
        "graph": graph,
        "ingestion": ingestion,
        "services": services,
        "jobs": jobs,
        "reasons": _dedupe_reasons(snapshot.get("reasons"), graph.get("reasons"), services.get("reasons"), ingestion.get("reasons")),
        "ts_ms": int(time.time() * 1000),
    }
    return _snapshot_response(snapshot, ok=bool(diagnostics.get("ok")), supervisor_diagnostics=diagnostics, **diagnostics)

# ----------------------------------------------------------------------
# TELEMETRY
# ----------------------------------------------------------------------
def api_get_telemetry(_parsed, ctx=None):
    import os
    import time

    from engine.runtime.metrics_store import write_runtime_snapshot
    from engine.runtime.storage import connect as _db_connect

    ts = int(time.time() * 1000)

    from engine.runtime.platform import default_data_root

    db_path = os.environ.get("DB_PATH", str(default_data_root()))
    db_size = 0
    try:
        if os.path.exists(db_path):
            db_size = os.path.getsize(db_path)
    except Exception as e:
        _warn("api_system.telemetry.db_size", e, db_path=db_path)

    health = _health_snapshot_dict()

    lifecycle = _dict_or_empty(health.get("lifecycle"))
    job_summary = _dict_or_empty(health.get("job_summary"))
    system_state = {
        "ok": bool(health.get("ok")),
        "state": str(
            lifecycle.get("state")
            or health.get("status")
            or "UNKNOWN"
        ).strip().upper(),
    }
    supervisor = {
        "ok": bool(job_summary),
        "delegated": False,
        "jobs": [],
    }

    alert_counts = {
        "last_hour": 0,
        "critical_open": 0,
    }
    execution = {
        "n_fills": 0,
        "last_fill_ts_ms": None,
    }
    crash_analytics = {
        "rows": 0,
    }
    vol_target = {
        "enabled": False,
        "target_vol": None,
        "pre_realized_vol": None,
        "post_realized_vol": None,
        "scale": None,
        "ts_ms": None,
    }

    con = None
    try:
        con = _db_connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM alerts
                WHERE ts_ms >= ?
                """,
                (int(ts - 3600_000),),
            ).fetchone()
            alert_counts["last_hour"] = int((row or [0])[0] or 0)
        except Exception as e:
            _warn("api_system.telemetry.alert_counts.last_hour", e)

        try:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM alerts
                WHERE severity = 'CRIT'
                """
            ).fetchone()
            alert_counts["critical_open"] = int((row or [0])[0] or 0)
        except Exception as e:
            _warn("api_system.telemetry.alert_counts.critical_open", e)

        fills_table = None
        try:
            row = con.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_fills_v2' LIMIT 1"
            ).fetchone()
            if row:
                fills_table = require_allowed_table_name("broker_fills_v2")
            else:
                row = con.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='broker_fills' LIMIT 1"
                ).fetchone()
                if row:
                    fills_table = require_allowed_table_name("broker_fills")
        except Exception as e:
            _warn("api_system.telemetry.fills_table_probe", e)
            fills_table = None

        if fills_table:
            try:
                fills_sql = sql_identifier(fills_table)
                row = con.execute(
                    f"SELECT COUNT(*), MAX(ts_ms) FROM {fills_sql}"
                ).fetchone()
                execution = {
                    "n_fills": int((row or [0, None])[0] or 0),
                    "last_fill_ts_ms": int((row or [0, None])[1] or 0) or None,
                    "fills_table": fills_table,
                }
            except Exception as e:
                _warn("api_system.telemetry.execution_fills", e, fills_table=fills_table)
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                _warn("api_system.telemetry.close", e)
    try:
        if ctx and isinstance(ctx, dict):
            handlers = ctx.get("API_HANDLERS") or {}
            get_crash = handlers.get("api_get_crash_analytics")
            if callable(get_crash):
                crash = get_crash(_parsed, ctx)
                if isinstance(crash, dict):
                    crash_analytics["rows"] = len(crash.get("rows") or [])
    except Exception as e:
        _warn("api_system.telemetry.crash_analytics", e)

    if str(os.environ.get("TELEMETRY_INCLUDE_VOL_TARGET", "0")).strip().lower() in ("1", "true", "yes", "on"):
        try:
            from engine.runtime.risk_state import get_state

            enabled_raw = str(get_state("portfolio_vol_target_enabled", "0") or "0").strip().lower()
            vol_target = {
                "enabled": enabled_raw in ("1", "true", "yes", "on"),
                "target_vol": (
                    float(get_state("portfolio_target_vol", "0") or 0.0)
                    if str(get_state("portfolio_target_vol", "") or "").strip() != ""
                    else None
                ),
                "pre_realized_vol": (
                    float(get_state("portfolio_realized_vol_pre_target", "0") or 0.0)
                    if str(get_state("portfolio_realized_vol_pre_target", "") or "").strip() != ""
                    else None
                ),
                "post_realized_vol": (
                    float(get_state("portfolio_realized_vol_post_target", "0") or 0.0)
                    if str(get_state("portfolio_realized_vol_post_target", "") or "").strip() != ""
                    else None
                ),
                "scale": (
                    float(get_state("portfolio_vol_target_scale", "0") or 0.0)
                    if str(get_state("portfolio_vol_target_scale", "") or "").strip() != ""
                    else None
                ),
                "ts_ms": (
                    int(get_state("portfolio_vol_target_ts_ms", "0") or 0)
                    if str(get_state("portfolio_vol_target_ts_ms", "") or "").strip() != ""
                    else None
                ),
            }
        except Exception as e:
            _warn("api_system.telemetry.vol_target", e)

    try:
        import psutil
        p = psutil.Process(os.getpid())

        payload = {
            "ok": True,
            "ts_ms": ts,
            "cpu_percent": psutil.cpu_percent(interval=0.2),
            "memory_percent": psutil.virtual_memory().percent,
            "process_rss_mb": round(p.memory_info().rss / (1024 * 1024), 2),
            "thread_count": p.num_threads(),
            "db_size_mb": round(db_size / (1024 * 1024), 2),
            "health": {
                "ok": bool(health.get("ok")),
                "reasons": _list_or_empty(health.get("reasons")),
            },
            "jobs": _dict_or_empty(health.get("job_summary")),
            "providers": _dict_or_empty(health.get("providers")),
            "system_state": system_state.get("state"),
            "vol_target": vol_target,
            "supervisor": {
                "ok": bool(supervisor.get("ok")),
                "delegated": bool(supervisor.get("delegated")),
                "n_jobs": len(supervisor.get("jobs") or []),
            },
            "alerts": alert_counts,
            "execution": execution,
            "crash_analytics": crash_analytics,
        }

        if str(os.environ.get("TELEMETRY_WRITE_RUNTIME_SNAPSHOT", "0")).strip().lower() in ("1", "true", "yes", "on"):
            try:
                write_runtime_snapshot(
                    {
                        "ts_ms": ts,
                        "metrics": {
                            "cpu_percent": payload.get("cpu_percent"),
                            "memory_percent": payload.get("memory_percent"),
                            "process_rss_mb": payload.get("process_rss_mb"),
                            "thread_count": payload.get("thread_count"),
                            "db_size_mb": payload.get("db_size_mb"),
                            "health_ok": 1.0 if bool((payload.get("health") or {}).get("ok")) else 0.0,
                            "stale_jobs": float((payload.get("jobs") or {}).get("stale") or 0.0),
                            "job_total": float((payload.get("jobs") or {}).get("total") or 0.0),
                            "providers_healthy": float((payload.get("providers") or {}).get("healthy") or 0.0),
                            "providers_total": float((payload.get("providers") or {}).get("total") or 0.0),
                            "alerts_last_hour": float((payload.get("alerts") or {}).get("last_hour") or 0.0),
                            "critical_alerts_open": float((payload.get("alerts") or {}).get("critical_open") or 0.0),
                            "execution_fills": float((payload.get("execution") or {}).get("n_fills") or 0.0),
                            "portfolio_vol_target_enabled": 1.0 if bool((payload.get("vol_target") or {}).get("enabled")) else 0.0,
                            "portfolio_target_vol": float((payload.get("vol_target") or {}).get("target_vol") or 0.0),
                            "portfolio_realized_vol_pre_target": float((payload.get("vol_target") or {}).get("pre_realized_vol") or 0.0),
                            "portfolio_realized_vol_post_target": float((payload.get("vol_target") or {}).get("post_realized_vol") or 0.0),
                            "portfolio_vol_target_scale": float((payload.get("vol_target") or {}).get("scale") or 0.0),
                        },
                        "tags": {
                            "source": "api_get_telemetry",
                        },
                    }
                )
            except Exception as e:
                _warn("api_system.telemetry.write_runtime_snapshot", e)

        return payload
    except Exception as e:
        _warn("api_system.telemetry.fallback", e)
        payload = {
            "ok": True,
            "ts_ms": ts,
            "thread_count": 0,
            "db_size_mb": round(db_size / (1024 * 1024), 2),
            "psutil": False,
            "health": {
                "ok": bool(health.get("ok")),
                "reasons": _list_or_empty(health.get("reasons")),
            },
            "jobs": _dict_or_empty(health.get("job_summary")),
            "providers": _dict_or_empty(health.get("providers")),
            "system_state": system_state.get("state"),
            "vol_target": vol_target,
            "supervisor": {
                "ok": bool(supervisor.get("ok")),
                "delegated": bool(supervisor.get("delegated")),
                "n_jobs": len(supervisor.get("jobs") or []),
            },
            "alerts": alert_counts,
            "execution": execution,
            "crash_analytics": crash_analytics,
        }

        if str(os.environ.get("TELEMETRY_WRITE_RUNTIME_SNAPSHOT", "0")).strip().lower() in ("1", "true", "yes", "on"):
            try:
                write_runtime_snapshot(
                    {
                        "ts_ms": ts,
                        "metrics": {
                            "db_size_mb": payload.get("db_size_mb"),
                            "health_ok": 1.0 if bool((payload.get("health") or {}).get("ok")) else 0.0,
                            "stale_jobs": float((payload.get("jobs") or {}).get("stale") or 0.0),
                            "job_total": float((payload.get("jobs") or {}).get("total") or 0.0),
                            "providers_healthy": float((payload.get("providers") or {}).get("healthy") or 0.0),
                            "providers_total": float((payload.get("providers") or {}).get("total") or 0.0),
                            "alerts_last_hour": float((payload.get("alerts") or {}).get("last_hour") or 0.0),
                            "critical_alerts_open": float((payload.get("alerts") or {}).get("critical_open") or 0.0),
                            "execution_fills": float((payload.get("execution") or {}).get("n_fills") or 0.0),
                        },
                        "tags": {
                            "source": "api_get_telemetry",
                            "psutil": "0",
                        },
                    }
                )
            except Exception as write_err:
                _warn("api_system.telemetry.write_runtime_snapshot_fallback", write_err)

        return payload

def api_get_telemetry_history(parsed, ctx=None):
    from engine.runtime.metrics_store import get_runtime_metrics

    q = _qs(parsed) or {}
    metric = str(q.get("metric") or "").strip() or None

    try:
        limit = max(1, min(5000, int(q.get("limit", "500") or "500")))
    except Exception:
        limit = 500

    since_ms = None
    try:
        raw_since_ms = str(q.get("since_ms") or "").strip()
        if raw_since_ms:
            since_ms = int(raw_since_ms)
    except Exception:
        since_ms = None

    return get_runtime_metrics(metric=metric, since_ms=since_ms, limit=limit)


def api_get_allocator_status(_parsed=None, ctx=None):
    try:
        q = _qs(_parsed) or {}

        try:
            window_days = max(0, int(q.get("window_days", "0") or "0"))
        except Exception:
            window_days = 0

        from engine.runtime.allocator_status import get_allocator_status

        return get_allocator_status(window_days=window_days)
    except Exception as e:
        payload = _failure_out("api_system_allocator_status_failed", "API_SYSTEM_ALLOCATOR_STATUS_FAILED", e)
        return payload


def api_get_monte_carlo_risk(_parsed, ctx=None):
    """Return the latest persisted Monte Carlo portfolio-risk summary.

    Parameters
    ----------
    _parsed : Any
        Accepted for handler signature compatibility and ignored.
    ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        Persisted Monte Carlo risk payload augmented with ``ok``, ``pending``,
        ``status``, and ``ts_ms``. If no Monte Carlo result has been stored yet,
        returns ``ready=False`` with the latest status metadata.
    """
    try:
        import json

        from engine.runtime.risk_state import get_state, get_state_row

        raw, ts_ms = get_state_row("monte_carlo_risk_info", "")
        status = str(get_state("monte_carlo_risk_status", "idle") or "idle")
        pending = str(get_state("monte_carlo_risk_pending", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
        risk_ts_ms = int(get_state("monte_carlo_risk_ts_ms", str(ts_ms or 0)) or (ts_ms or 0) or 0)

        if not raw:
            return {
                "ok": True,
                "enabled": True,
                "ready": False,
                "pending": pending,
                "status": status,
                "ts_ms": int(risk_ts_ms),
            }

        data = json.loads(raw)
        if not isinstance(data, dict):
            data = {"value": data}

        data["ok"] = True
        data.setdefault("enabled", True)
        data.setdefault("ready", False)
        data["pending"] = bool(pending)
        data["status"] = str(status)
        data["ts_ms"] = int(data.get("ts_ms") or risk_ts_ms)
        return data
    except Exception as e:
        payload = _failure_out("api_system_monte_carlo_risk_failed", "API_SYSTEM_MONTE_CARLO_RISK_FAILED", e)
        return payload


def api_get_drift_explainer(_parsed, ctx=None):
    """Return read-only attribution for currently persisted drift outputs."""
    try:
        from engine.api.drift_explainer import DEFAULT_TOP_N, build_drift_explainer_snapshot

        q = _qs(_parsed) or {}
        try:
            top_n = max(1, min(25, int(q.get("top_n", DEFAULT_TOP_N) or DEFAULT_TOP_N)))
        except Exception:
            top_n = DEFAULT_TOP_N

        return build_drift_explainer_snapshot(top_n=top_n)
    except Exception as e:
        payload = _failure_out("api_system_drift_explainer_failed", "API_SYSTEM_DRIFT_EXPLAINER_FAILED", e)
        payload.setdefault("schema_version", 1)
        payload.setdefault(
            "status",
            {
                "state": "unavailable",
                "severity": "UNKNOWN",
                "active": False,
                "stale": False,
                "reason": str(e),
            },
        )
        payload.setdefault("contributors", [])
        payload.setdefault("affected", {"symbols": [], "models": [], "regimes": [], "time_slices": []})
        payload.setdefault("unavailable", [{"field": "drift_explainer", "reason": str(e)}])
        return payload


def api_get_execution_barrier(_parsed, ctx=None):
    """Return the current execution barrier extracted from the system snapshot.

    Parameters
    ----------
    _parsed : Any
        Parsed request object forwarded into snapshot construction.
    ctx : Any, optional
        Optional request context forwarded into snapshot construction.

    Returns
    -------
    dict
        Snapshot payload augmented with ``execution_barrier``, boolean
        ``allowed``, and the selected block ``reason``. ``ok`` mirrors
        ``allowed`` to keep the endpoint fail-closed.
    """
    snapshot = _build_system_snapshot(_parsed, ctx)
    barrier = dict(snapshot.get("execution_barrier") or {})
    return {
        **snapshot,
        "ok": bool(barrier.get("allowed")),
        "execution_barrier": barrier,
        "allowed": bool(barrier.get("allowed")),
        "reason": str(barrier.get("reason") or ""),
    }
    
def api_get_portfolio_risk(_parsed, ctx=None):
    """Return persisted portfolio-risk state plus recent history.

    Parameters
    ----------
    _parsed : Any
        Accepted for handler signature compatibility and ignored.
    ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        Payload with ``enabled``, ``ready``, ``blocked``, ``status``, ``ts_ms``,
        ``summary``, ``info``, and ``history``. ``history`` contains up to
        ``200`` snapshot rows ordered newest-first, with timestamps in epoch
        milliseconds and exposure metrics expressed as unit fractions.
    """
    try:
        import json
        from engine.runtime.risk_state import get_state, get_state_row
        from engine.runtime.storage import connect, _table_exists

        raw, ts_ms = get_state_row("portfolio_risk_info", "")
        summary_raw, summary_ts_ms = get_state_row("portfolio_risk_summary", "")
        status = str(get_state("portfolio_risk_status", "unknown") or "unknown")
        blocked = str(get_state("portfolio_risk_block", "0") or "0").strip() == "1"
        risk_ts_ms = int(get_state("portfolio_risk_ts_ms", str(ts_ms or summary_ts_ms or 0)) or (ts_ms or summary_ts_ms or 0) or 0)

        info = {}
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    info = parsed
                else:
                    info = {"value": parsed}
            except Exception:
                info = {"raw": raw}

        summary = {}
        if summary_raw:
            try:
                parsed = json.loads(summary_raw)
                if isinstance(parsed, dict):
                    summary = parsed
                else:
                    summary = {"value": parsed}
            except Exception:
                summary = {"raw": summary_raw}

        history = []
        con = connect(readonly=True)
        try:
            if _table_exists(con, "portfolio_risk_snapshots"):
                rows = con.execute(
                    """
                    SELECT ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json
                    FROM portfolio_risk_snapshots
                    ORDER BY ts_ms DESC
                    LIMIT 200
                    """
                ).fetchall() or []
                for r in rows:
                    info_json = {}
                    try:
                        info_json = json.loads(r[6] or "{}")
                        if not isinstance(info_json, dict):
                            info_json = {"value": info_json}
                    except Exception:
                        info_json = {}
                    history.append(
                        {
                            "ts_ms": int(r[0] or 0),
                            "gross": float(r[1] or 0.0),
                            "net": float(r[2] or 0.0),
                            "vol_proxy": (float(r[3]) if r[3] is not None else None),
                            "drawdown": (float(r[4]) if r[4] is not None else None),
                            "blocked": bool(int(r[5] or 0)),
                            "info": info_json,
                        }
                    )
        finally:
            con.close()

        return {
            "ok": True,
            "enabled": True,
            "ready": bool(info or summary),
            "blocked": bool(blocked),
            "status": str(status),
            "ts_ms": int(risk_ts_ms),
            "summary": summary,
            "info": info,
            "history": history,
        }
    except Exception as e:
        payload = _failure_out("api_system_portfolio_risk_failed", "API_SYSTEM_PORTFOLIO_RISK_FAILED", e)
        return payload

# ----------------------------------------------------------------------
# SELF REPAIR
# ----------------------------------------------------------------------

def api_post_repair_schema(_parsed=None, body=None, ctx=None):
    try:
        result = repair_schema()
        if isinstance(result, dict):
            return result
        return {"ok": True}
    except Exception as e:
        payload = _failure_out("api_system_repair_schema_failed", "API_SYSTEM_REPAIR_SCHEMA_FAILED", e)
        return payload

def api_post_self_repair(_parsed=None, body=None, ctx=None):
    try:
        steps = []
        started_ts_ms = int(time.time() * 1000)
        body_payload = body if isinstance(body, dict) else {}
        mode = str(
            body_payload.get("mode")
            or os.environ.get("ENGINE_MODE")
            or "paper"
        ).strip().lower() or "paper"
        log.info("api_system_self_repair_begin mode=%s", mode)

        def _add_step(step_id, ok, detail=None, **extra):
            item = {
                "step": str(step_id),
                "ok": bool(ok),
                "detail": detail,
            }
            item.update(extra or {})
            steps.append(item)
            return item

        try:
            initial = run_preflight()
            _add_step("preflight_before", bool(initial.get("ok")), initial)
        except Exception as e:
            initial = {"ok": False, "error": str(e)}
            _add_step("preflight_before", False, initial)

        try:
            from engine.runtime.jobs.repair_schema import run as repair_schema
            repair = repair_schema()
            _add_step("repair_schema", bool(repair.get("ok")), repair)
        except Exception as e:
            _add_step("repair_schema", False, {"error": str(e)})

        try:
            from engine.runtime.first_run import bootstrap_first_run
            boot = bootstrap_first_run(mode=mode)
            _add_step("bootstrap_first_run", bool(boot.get("ok")), boot)
        except Exception as e:
            _add_step("bootstrap_first_run", False, {"error": str(e)})

        try:
            stale_after_s = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
            threshold_ms = int(time.time() * 1000) - (stale_after_s * 1000)

            from engine.runtime.storage import _pid_is_running

            con = _db_connect(readonly=True)
            try:
                rows = con.execute(
                    """
                    SELECT job_name, owner, pid, heartbeat_ts_ms
                    FROM job_locks
                    WHERE heartbeat_ts_ms IS NULL OR heartbeat_ts_ms < ?
                    ORDER BY job_name ASC
                    """,
                    (int(threshold_ms),),
                ).fetchall() or []
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn("api_system.telemetry_history.close", e)

            cleared = []
            skipped = []

            for row in rows:
                job_name = str(row[0] or "")
                owner = str(row[1] or "")
                pid = int(row[2] or 0)
                if not job_name:
                    continue
                if _pid_is_running(pid):
                    skipped.append({"job_name": job_name, "owner": owner, "pid": pid})
                    continue

                def _write(con, _job_name=job_name):
                    con.execute("DELETE FROM job_locks WHERE job_name=?", (_job_name,))
                    con.execute("DELETE FROM job_heartbeats WHERE job_name=?", (_job_name,))

                run_write_txn(_write)
                cleared.append(job_name)

            _add_step(
                "clear_stale_job_locks",
                True,
                {"cleared": cleared, "count": len(cleared), "skipped_running": skipped},
            )
        except Exception as e:
            _add_step("clear_stale_job_locks", False, {"error": str(e)})

        try:
            sup = ctx.get("SUPERVISOR") if ctx else None
            started = []
            results = {}

            for name in [
                "ingestion_runtime",
                "stream_prices_polygon_ws",
                "poll_prices",
                "provider_monitor",
                "metrics_collector",
            ]:
                try:
                    if sup:
                        result = sup.deterministic_start([name], include_deps=True, strict=False)
                    else:
                        result = {"ok": False, "error": "supervisor_missing"}
                    results[name] = result
                    if bool(result.get("ok")):
                        started.append(name)
                except Exception as e:
                    results[name] = {"ok": False, "error": str(e)}

            _add_step("restart_runtime_daemons", len(started) > 0, {"started": started, "results": results})
        except Exception as e:
            _add_step("restart_runtime_daemons", False, {"error": str(e)})

        try:
            sup = ctx.get("SUPERVISOR") if ctx else None
            if sup:
                universe = sup.deterministic_start(["update_universe"], include_deps=True, strict=False)
            else:
                universe = {"ok": False, "error": "supervisor_missing"}
            _add_step("update_universe", bool(universe.get("ok")), universe)
        except Exception as e:
            _add_step("update_universe", False, {"error": str(e)})

        try:
            final_preflight = run_preflight()
            _add_step("preflight_after", bool(final_preflight.get("ok")), final_preflight)
        except Exception as e:
            final_preflight = {"ok": False, "error": str(e)}
            _add_step("preflight_after", False, final_preflight)

        runtime_health = api_get_runtime_health(_parsed, ctx)
        trading_readiness = api_get_trading_readiness(_parsed, ctx)

        ok = bool(final_preflight.get("ok")) or bool(trading_readiness.get("ready"))

        finished_ts_ms = int(time.time() * 1000)
        result = {
            "ok": bool(ok),
            "mode": mode,
            "steps": steps,
            "runtime_health": runtime_health,
            "trading_readiness": trading_readiness,
            "started_ts_ms": started_ts_ms,
            "finished_ts_ms": finished_ts_ms,
            "duration_ms": int(max(0, finished_ts_ms - started_ts_ms)),
        }
        log.info(
            "api_system_self_repair_done mode=%s ok=%s duration_ms=%s steps=%s",
            mode,
            bool(result.get("ok")),
            int(result.get("duration_ms") or 0),
            len(steps),
        )
        return result

    except Exception as e:
        payload = _failure_out("api_system_self_repair_failed", "API_SYSTEM_SELF_REPAIR_FAILED", e, steps=[])
        return payload
