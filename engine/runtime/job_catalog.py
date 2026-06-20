"""Runtime-owned job catalog serialization and safety policy.

The allowlist in :mod:`engine.runtime.job_registry` remains the source of
runnable jobs. This module turns that compact registry shape into the
operator-facing contract used by the API and dashboard, and keeps job action
safety decisions server-side.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from engine.runtime import job_registry


SAFETY_READ_ONLY = "read_only"
SAFETY_DATA_REFRESH = "data_refresh"
SAFETY_TRAINING_RESEARCH = "training_research"
SAFETY_EXECUTION_SENSITIVE = "execution_sensitive"
SAFETY_DESTRUCTIVE_ADMIN = "destructive_admin"
SAFETY_UNAVAILABLE = "unavailable"

CONFIRM_JOB_ACTION = "JOB_ACTION"

SAFETY_LABELS = {
    SAFETY_READ_ONLY: "Read-only",
    SAFETY_DATA_REFRESH: "Data refresh",
    SAFETY_TRAINING_RESEARCH: "Training/research",
    SAFETY_EXECUTION_SENSITIVE: "Execution-sensitive",
    SAFETY_DESTRUCTIVE_ADMIN: "Destructive/admin",
    SAFETY_UNAVAILABLE: "Unavailable",
}

SAFETY_REQUIRES_START_CONFIRMATION = {
    SAFETY_EXECUTION_SENSITIVE,
    SAFETY_DESTRUCTIVE_ADMIN,
}

DESTRUCTIVE_ADMIN_JOBS = frozenset(
    {
        "artifacts_fsck",
        "blacklist_update_job",
        "model_lifecycle_manager",
        "monthly_restore_drill",
        "promote_temporal_models",
        "repair_schema",
        "strategy_governance",
    }
)

READ_ONLY_JOB_NAMES = frozenset(
    {
        "audit_chain_verify",
        "check_alerts",
        "check_events",
        "check_labels",
        "check_predictions",
        "execution_quality_job",
        "inference_health_probe",
        "kill_drift_monitor",
        "kill_health_monitor",
        "kill_slippage_monitor",
        "live_stability_guard_job",
        "metrics_collector",
        "observability_snapshot",
        "post_promotion_monitor",
        "prod_preflight",
        "provider_monitor",
        "snapshot_equity",
        "strategy_kill_drift_monitor",
        "strategy_kill_health_monitor",
        "strategy_kill_slippage_monitor",
        "trade_attribution_audit_job",
        "trade_lifecycle_audit_job",
    }
)

SECRET_PROVIDER_HINTS = {
    "ALPACA": "alpaca",
    "ANTHROPIC": "anthropic",
    "QUIVER": "quiver",
    "SHARADAR": "sharadar",
    "SIMFIN": "simfin",
}

TEXT_PROVIDER_HINTS = (
    ("polygon", "polygon"),
    ("ibkr", "ibkr"),
    ("reddit", "reddit"),
    ("stocktwits", "stocktwits"),
    ("weather", "weather"),
    ("gdelt", "gdelt"),
    ("sec", "sec"),
    ("finra", "finra"),
    ("cftc", "cftc"),
    ("crypto", "crypto"),
    ("etf", "etf"),
    ("form4", "sec-form4"),
    ("congress", "congressional-trades"),
)


@dataclass(frozen=True)
class JobCatalogEntry:
    """Typed operator-facing representation of one registered job."""

    id: str
    name: str
    label: str
    group: str
    workflow: str
    script: str
    module: str
    mode: str
    schedule: str
    cadence_seconds: int | None
    stage: str
    owner_subsystem: str
    dependencies: tuple[str, ...]
    required_secrets: tuple[str, ...]
    required_secret_any: tuple[str, ...]
    required_providers: tuple[str, ...]
    missing_prerequisites: tuple[dict[str, Any], ...]
    execution: bool
    execution_sensitivity: str
    resource_class: str
    resource_priority: int | None
    slot_cost: int | None
    purpose: str
    base_safety: str
    safety: str
    safety_label: str
    disabled_reason: str
    quick_label: str

    def to_dict(self) -> dict[str, Any]:
        """Return the stable JSON shape used by the API layer."""

        prereq_ok = not bool(self.missing_prerequisites)
        start_enabled = prereq_ok
        stop_enabled = prereq_ok
        start_safety_confirmation = self.base_safety in SAFETY_REQUIRES_START_CONFIRMATION
        start_reason = "" if start_enabled else self.disabled_reason
        return {
            "id": self.id,
            "name": self.name,
            "label": self.label,
            "quick_label": self.quick_label,
            "group": self.group,
            "workflow": self.workflow,
            "script": self.script,
            "module": self.module,
            "mode": self.mode,
            "schedule": self.schedule,
            "cadence_seconds": self.cadence_seconds,
            "stage": self.stage,
            "owner_subsystem": self.owner_subsystem,
            "dependencies": list(self.dependencies),
            "required_secrets": list(self.required_secrets),
            "required_secret_any": list(self.required_secret_any),
            "required_providers": list(self.required_providers),
            "missing_prerequisites": [dict(item) for item in self.missing_prerequisites],
            "prerequisites": {
                "ok": prereq_ok,
                "required_secrets": list(self.required_secrets),
                "required_secret_any": list(self.required_secret_any),
                "required_providers": list(self.required_providers),
                "missing": [dict(item) for item in self.missing_prerequisites],
            },
            "execution": self.execution,
            "execution_sensitivity": self.execution_sensitivity,
            "resource_class": self.resource_class,
            "resource_priority": self.resource_priority,
            "slot_cost": self.slot_cost,
            "purpose": self.purpose,
            "base_safety": self.base_safety,
            "safety": self.safety,
            "safety_label": self.safety_label,
            "disabled_reason": self.disabled_reason,
            "log_url": f"/api/jobs/log?name={self.id}&tail=800",
            "history_url": f"/api/jobs/history?name={self.id}&limit=200",
            "last_output_url": f"/api/jobs/log?name={self.id}&tail=800",
            "action_policy": {
                "start": {
                    "enabled": start_enabled,
                    "disabled_reason": start_reason,
                    "confirmation_required": True,
                    "safety_confirmation_required": start_safety_confirmation,
                    "required_confirm": CONFIRM_JOB_ACTION,
                    "backend_guarded": start_safety_confirmation or self.safety == SAFETY_UNAVAILABLE,
                },
                "stop": {
                    "enabled": stop_enabled,
                    "disabled_reason": "" if stop_enabled else self.disabled_reason,
                    "confirmation_required": True,
                    "safety_confirmation_required": False,
                    "required_confirm": CONFIRM_JOB_ACTION,
                    "backend_guarded": self.safety == SAFETY_UNAVAILABLE,
                },
            },
        }


def _humanize(value: str) -> str:
    text = re.sub(r"[_\-]+", " ", str(value or "").strip())
    return re.sub(r"\s+", " ", text).strip()


def _module_from_script(script: str) -> str:
    path = Path(str(script or "").strip())
    if path.suffix == ".py":
        path = path.with_suffix("")
    return ".".join(part for part in path.parts if part and part != ".")


def _owner_from_script(script: str) -> str:
    parts = Path(str(script or "")).parts
    if not parts:
        return "unknown"
    if parts[0] == "engine" and len(parts) >= 2:
        if parts[1] == "jobs":
            return "data"
        return str(parts[1])
    return str(parts[0])


def _list_from_value(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return (str(value).strip(),) if str(value).strip() else ()


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _required_providers(name: str, script: str, secrets: tuple[str, ...]) -> tuple[str, ...]:
    providers: set[str] = set()
    for secret in secrets:
        upper = str(secret or "").upper()
        for token, provider in SECRET_PROVIDER_HINTS.items():
            if token in upper:
                providers.add(provider)
    haystack = f"{name} {script}".lower()
    for token, provider in TEXT_PROVIDER_HINTS:
        if token in haystack:
            providers.add(provider)
    return tuple(sorted(providers))


def _missing_prerequisites(
    *,
    required_secrets: tuple[str, ...],
    required_secret_any: tuple[str, ...],
    environ: Mapping[str, str],
) -> tuple[dict[str, Any], ...]:
    missing: list[dict[str, Any]] = []
    for name in required_secrets:
        if not str(environ.get(name, "") or "").strip():
            missing.append({"type": "secret", "name": name})
    if required_secret_any and not any(str(environ.get(name, "") or "").strip() for name in required_secret_any):
        missing.append({"type": "secret_any", "names": list(required_secret_any)})
    return tuple(missing)


def _dependencies_from_meta(meta: Mapping[str, Any]) -> tuple[str, ...]:
    deps: list[str] = []
    for key in ("dependencies", "depends_on", "requires_jobs"):
        deps.extend(_list_from_value(meta.get(key)))
    deps.extend(_list_from_value(meta.get("gap_recovery_job")))
    return tuple(dict.fromkeys(dep for dep in deps if dep))


def _resource_class(name: str, script: str, meta: Mapping[str, Any], *, execution: bool) -> str:
    explicit = str(meta.get("resource_class") or "").strip()
    if explicit:
        return explicit
    if execution:
        return "execution"
    if job_registry.is_offline_workload_job(name):
        if "backtest" in name or "replay" in name:
            return "replay"
        return "training"
    if job_registry.is_market_data_job(name):
        return "market_data"
    owner = _owner_from_script(script)
    if owner == "runtime":
        return "control_plane"
    if owner == "data":
        return "data"
    return "general"


def _workflow(name: str, group: str, owner: str, stage: str, resource_class: str) -> str:
    if group:
        return group
    if stage:
        if any(token in stage for token in ("train", "tune", "model", "ensemble")):
            return "model_training"
        if any(token in stage for token in ("ingest", "process", "label", "news", "macro", "universe")):
            return "data_pipeline"
        return stage
    if resource_class in {"execution", "replay", "training", "inference", "maintenance", "market_data"}:
        return resource_class
    if name.startswith(("poll_", "ingest_", "backfill_", "compute_")):
        return "data_pipeline"
    return owner or "general"


def _purpose(
    *,
    name: str,
    mode: str,
    owner: str,
    stage: str,
    resource_class: str,
    schedule: str,
    meta: Mapping[str, Any],
) -> str:
    explicit = str(meta.get("purpose") or meta.get("description") or "").strip()
    if explicit:
        return explicit
    job_label = _humanize(name)
    parts = [f"Runs {job_label}"]
    if stage:
        parts.append(f"for the {stage} stage")
    elif resource_class and resource_class != "general":
        parts.append(f"for {resource_class} work")
    if owner and owner != "unknown":
        parts.append(f"in the {owner} subsystem")
    if mode == "daemon":
        parts.append("as a supervised daemon")
    else:
        parts.append("as a one-shot job")
    if schedule:
        parts.append(f"on {schedule}")
    return " ".join(parts) + "."


def _base_safety(name: str, script: str, meta: Mapping[str, Any], resource_class: str) -> str:
    if name in DESTRUCTIVE_ADMIN_JOBS:
        return SAFETY_DESTRUCTIVE_ADMIN
    if bool(meta.get("execution") is True):
        return SAFETY_EXECUTION_SENSITIVE
    if job_registry.is_offline_workload_job(name):
        return SAFETY_TRAINING_RESEARCH
    if name in READ_ONLY_JOB_NAMES:
        return SAFETY_READ_ONLY
    if resource_class in {"maintenance"}:
        return SAFETY_DESTRUCTIVE_ADMIN
    if resource_class in {"training", "replay"}:
        return SAFETY_TRAINING_RESEARCH
    if resource_class in {"control_plane"} and any(token in name for token in ("monitor", "probe", "snapshot", "check")):
        return SAFETY_READ_ONLY
    return SAFETY_DATA_REFRESH


def build_job_catalog_entry(
    job_name: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> JobCatalogEntry | None:
    """Build one catalog entry from the canonical job registry."""

    name = str(job_name or "").strip()
    spec = job_registry.get_job_spec(name)
    if not name or not isinstance(spec, (tuple, list)) or len(spec) < 2:
        return None
    script = str(spec[0] or "").strip()
    mode = str(spec[1] or "").strip().lower()
    group = str(spec[2] or "").strip() if len(spec) >= 3 else ""
    meta = dict(spec[3] or {}) if len(spec) >= 4 and isinstance(spec[3], dict) else {}
    env = os.environ if environ is None else environ

    required_secrets = _list_from_value(meta.get("requires_secret"))
    required_secret_any = _list_from_value(meta.get("requires_secret_any"))
    missing = _missing_prerequisites(
        required_secrets=required_secrets,
        required_secret_any=required_secret_any,
        environ=env,
    )
    owner = _owner_from_script(script)
    execution = bool(meta.get("execution") is True)
    resource_class = _resource_class(name, script, meta, execution=execution)
    stage = str(meta.get("pipeline_stage") or meta.get("default_stage") or "").strip()
    schedule = str(meta.get("schedule") or "").strip()
    base_safety = _base_safety(name, script, meta, resource_class)
    safety = SAFETY_UNAVAILABLE if missing else base_safety
    disabled_reason = ""
    if missing:
        missing_bits = []
        for item in missing:
            if item.get("type") == "secret_any":
                missing_bits.append("one of " + ", ".join(item.get("names") or []))
            else:
                missing_bits.append(str(item.get("name") or "secret"))
        disabled_reason = "Missing prerequisite: " + "; ".join(missing_bits)

    return JobCatalogEntry(
        id=name,
        name=name,
        label=_humanize(name),
        group=group,
        workflow=_workflow(name, group, owner, stage, resource_class),
        script=script,
        module=_module_from_script(script),
        mode=mode,
        schedule=schedule,
        cadence_seconds=_optional_int(meta.get("cadence_seconds")),
        stage=stage,
        owner_subsystem=owner,
        dependencies=_dependencies_from_meta(meta),
        required_secrets=required_secrets,
        required_secret_any=required_secret_any,
        required_providers=_required_providers(
            name,
            script,
            required_secrets + required_secret_any,
        ),
        missing_prerequisites=missing,
        execution=execution,
        execution_sensitivity=(
            "live_execution"
            if execution
            else ("admin_destructive" if base_safety == SAFETY_DESTRUCTIVE_ADMIN else "none")
        ),
        resource_class=resource_class,
        resource_priority=_optional_int(meta.get("resource_priority")),
        slot_cost=_optional_int(meta.get("slot_cost")),
        purpose=_purpose(
            name=name,
            mode=mode,
            owner=owner,
            stage=stage,
            resource_class=resource_class,
            schedule=schedule,
            meta=meta,
        ),
        base_safety=base_safety,
        safety=safety,
        safety_label=SAFETY_LABELS.get(safety, _humanize(safety)),
        disabled_reason=disabled_reason,
        quick_label=("Start " if mode == "daemon" else "Run ") + name,
    )


def build_job_catalog(
    *,
    environ: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Return all registered jobs as operator-facing dictionaries."""

    rows: list[dict[str, Any]] = []
    for name in job_registry.ALLOWED_JOBS.keys():
        entry = build_job_catalog_entry(str(name), environ=environ)
        if entry is not None:
            rows.append(entry.to_dict())

    order = list(job_registry.JOB_ORDER or [])
    pipeline = list(job_registry.PIPELINE_ORDER or [])

    def sort_key(row: Mapping[str, Any]) -> tuple[int, int, str, str]:
        job_id = str(row.get("id") or row.get("name") or "")
        if job_id in order:
            return (0, order.index(job_id), str(row.get("workflow") or ""), job_id)
        if job_id in pipeline:
            return (1, pipeline.index(job_id), str(row.get("workflow") or ""), job_id)
        return (2, 99999, str(row.get("workflow") or ""), job_id)

    return sorted(rows, key=sort_key)


def enrich_job_runtime_row(
    row: Mapping[str, Any],
    *,
    environ: Mapping[str, str] | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Merge live/persisted job state with the catalog contract."""

    name = str(row.get("name") or row.get("id") or "").strip()
    entry = build_job_catalog_entry(name, environ=environ)
    out = dict(row or {})
    if entry is not None:
        catalog = entry.to_dict()
        catalog.update(out)
        out = catalog
    out.setdefault("id", name)
    out.setdefault("name", name)
    out.setdefault("log_url", f"/api/jobs/log?name={name}&tail=800")
    out.setdefault("history_url", f"/api/jobs/history?name={name}&limit=200")
    out.setdefault("last_output_url", f"/api/jobs/log?name={name}&tail=800")
    running = bool(out.get("running"))
    exit_code = out.get("exit_code")
    last_exit_code = out.get("last_exit_code")
    last_event_ts_ms = out.get("last_event_ts_ms")
    state = "running" if running else "idle"
    if not running:
        code = exit_code if exit_code is not None else last_exit_code
        if code is not None:
            try:
                state = "succeeded" if int(code) == 0 else "failed"
            except Exception:
                state = "exited"
    out["latest_run"] = {
        "state": state,
        "running": running,
        "exit_code": exit_code,
        "last_exit_code": last_exit_code,
        "last_event_ts_ms": last_event_ts_ms,
        "heartbeat_ts_ms": out.get("heartbeat_ts_ms"),
        "heartbeat_age_s": out.get("heartbeat_age_s"),
        "stale": bool(out.get("stale")),
    }
    if now_ms is not None:
        out["catalog_ts_ms"] = int(now_ms)
    return out


def dangerous_job_start_confirmation_error(
    job_name: str,
    body: Mapping[str, Any] | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any] | None:
    """Return a policy error for unavailable or dangerous unconfirmed starts."""

    entry = build_job_catalog_entry(job_name, environ=environ)
    if entry is None:
        return None
    payload = dict(body or {}) if isinstance(body, Mapping) else {}
    row = entry.to_dict()
    if row.get("safety") == SAFETY_UNAVAILABLE:
        return {
            "ok": False,
            "error": "job_unavailable",
            "job": str(job_name or "").strip(),
            "safety": row.get("safety"),
            "disabled_reason": row.get("disabled_reason") or "Job prerequisites are not satisfied.",
            "missing_prerequisites": list(row.get("missing_prerequisites") or []),
            "meta": {"status": 409},
        }
    if row.get("base_safety") not in SAFETY_REQUIRES_START_CONFIRMATION:
        return None
    actual = str(payload.get("confirmation") or payload.get("confirm") or "").strip()
    consequence_ack = payload.get("consequence_ack")
    ack_ok = bool(consequence_ack) if isinstance(consequence_ack, bool) else str(consequence_ack or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
        "ack",
        "confirmed",
    }
    missing = []
    if actual != CONFIRM_JOB_ACTION:
        missing.append("confirmation")
    if not ack_ok:
        missing.append("consequence_ack")
    if not missing:
        return None
    return {
        "ok": False,
        "error": "confirmation_required",
        "job": str(job_name or "").strip(),
        "safety": row.get("base_safety"),
        "required_confirm": CONFIRM_JOB_ACTION,
        "required_token": CONFIRM_JOB_ACTION,
        "required_fields": missing,
        "action_id": "jobs.start",
        "severity": "high",
        "consequence": "Starts an execution-sensitive or administrative runtime job.",
        "meta": {"status": 422},
    }


__all__ = [
    "CONFIRM_JOB_ACTION",
    "SAFETY_DATA_REFRESH",
    "SAFETY_DESTRUCTIVE_ADMIN",
    "SAFETY_EXECUTION_SENSITIVE",
    "SAFETY_READ_ONLY",
    "SAFETY_TRAINING_RESEARCH",
    "SAFETY_UNAVAILABLE",
    "build_job_catalog",
    "build_job_catalog_entry",
    "dangerous_job_start_confirmation_error",
    "enrich_job_runtime_row",
]
