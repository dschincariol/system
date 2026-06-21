"""Shared response helpers for the system API facade."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any


REQUIRED_TABLE_ALIASES = {
    "prices": ("prices",),
    "trades": ("trades",),
    "portfolio_state": ("portfolio_state",),
    "alerts": ("alerts",),
    "jobs": ("job_locks", "job_history"),
}


PRODUCTION_GATE_ORDER = [
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
    "live_trading_preflight",
    "api_layer_healthy",
    "operator_server_healthy",
    "critical_ui_dependencies_available",
]


PRODUCTION_CRITICAL_GATES = {
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
    "live_trading_preflight",
}


SAFE_NO_CREDENTIAL_SERVICE_READY_GATES = {
    "critical_features_valid",
    "model_inputs_valid",
    "scoring_pipeline_operational",
    "execution_engine_initialized",
    "order_state_consistent",
    "position_state_consistent",
    "pnl_calculation_valid",
}


SAFE_NO_CREDENTIAL_SKIPPABLE_GATE_REASONS = {
    "critical_features_valid": {"feature_validation_missing", "data_pipeline_gate_unreported"},
    "model_inputs_valid": {"model_input_validation_missing", "data_pipeline_gate_unreported"},
    "scoring_pipeline_operational": {"scoring_pipeline_unreported", "data_pipeline_gate_unreported"},
    "execution_engine_initialized": {"execution_gate_unreported"},
    "order_state_consistent": {"execution_gate_unreported"},
    "position_state_consistent": {"execution_gate_unreported"},
    "pnl_calculation_valid": {"execution_gate_unreported"},
}


UI_CRITICAL_ENDPOINT_SPECS = [
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


def dedupe_reasons(*groups: Any) -> list[str]:
    out = []
    seen = set()
    for group in groups:
        for item in group or []:
            value = str(item or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
    return out


def required_tables_status(schema: Any) -> dict[str, Any]:
    schema = dict(schema or {})
    have_tables = set(str(x or "") for x in (schema.get("have_tables") or []))
    missing_tables = set(str(x or "") for x in (schema.get("missing_tables") or []))

    tables = {}
    missing = []
    reasons = []

    for logical_name, physical_names in REQUIRED_TABLE_ALIASES.items():
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


def snapshot_response(snapshot: Any, ok: bool | None = None, **extra: Any) -> dict[str, Any]:
    snapshot = dict(snapshot or {})
    payload = dict(snapshot)
    if ok is not None:
        payload["ok"] = bool(ok)
    payload.update(extra or {})
    payload.setdefault("status", str(snapshot.get("status") or "STOPPED"))
    payload.setdefault("state", str(snapshot.get("state") or "UNKNOWN"))
    payload.setdefault("mode", str(snapshot.get("mode") or "unknown"))
    payload.setdefault("execution_mode", str(snapshot.get("execution_mode") or payload.get("mode") or "unknown"))
    payload.setdefault("execution_allowed", bool(snapshot.get("execution_allowed")))
    payload["reasons"] = dedupe_reasons(snapshot.get("reasons"), payload.get("reasons"))
    payload.setdefault("health", dict(snapshot.get("health") or {}))
    payload.setdefault("ingestion", dict(snapshot.get("ingestion") or {}))
    payload.setdefault("services", dict(snapshot.get("services") or {}))
    payload.setdefault("readiness", dict(snapshot.get("readiness") or {}))
    payload.setdefault("timestamps", dict(snapshot.get("timestamps") or {}))
    return payload


def storage_readiness_from_health(health: Any) -> dict[str, Any]:
    health = dict(health or {})
    startup_validation = dict(health.get("startup_validation") or {})
    candidates = [
        startup_validation.get("storage"),
        ((startup_validation.get("checks") or {}).get("core_services_initialized") or {}).get("storage"),
        (((startup_validation.get("checks") or {}).get("core_services_initialized") or {}).get("boot_diagnostics") or {}).get("storage"),
    ]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        storage = dict(candidate)
        if "ok" not in storage and str(storage.get("status") or "").strip().lower() in {"ready", "ok", "healthy"}:
            storage["ok"] = True
        if "checked" not in storage:
            storage["checked"] = "ok" in storage or bool(storage.get("status"))
        return storage
    return {}


def safe_json_dict(raw: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(raw or "{}")
    except Exception:
        parsed = {}
    return dict(parsed or {}) if isinstance(parsed, dict) else {}


def float_or_none(value: Any, *, warn: Callable[..., None] | None = None) -> float | None:
    try:
        if value is None or value == "":
            return None
        out = float(value)
        return out if out == out else None
    except Exception as e:
        if warn is not None:
            warn("api_system.float_or_none_parse", e)
        return None


def dict_or_empty(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def list_or_empty(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def normalized_health_from_snapshot(snapshot: Any) -> dict[str, Any]:
    outer = dict((snapshot or {}).get("health") or {})
    inner = outer.get("health")
    if isinstance(inner, dict):
        return dict(inner)
    return outer


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name), "")
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}
