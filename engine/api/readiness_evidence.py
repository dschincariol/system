from __future__ import annotations

"""Normalized live/paper readiness evidence for operator surfaces.

This module intentionally serializes evidence produced by runtime, execution,
broker, data-source, and governance subsystems. It does not arm execution,
promote models, or bypass existing gates.
"""

from dataclasses import dataclass, field
import os
import time
from typing import Any, Mapping, Sequence


PASSING = "passing"
WARNING = "warning"
BLOCKED = "blocked"
UNAVAILABLE = "unavailable"

INFO = "info"
WARN = "warning"
CRITICAL = "critical"

DEFAULT_BROKER_TEST_MAX_AGE_MS = 24 * 60 * 60 * 1000
DEFAULT_PRODUCTION_MONITORING_MAX_AGE_MS = 24 * 60 * 60 * 1000

CRITICAL_CONTEXT_MODES = {"paper", "live"}


@dataclass(frozen=True)
class Freshness:
    last_update_ts_ms: int = 0
    age_ms: int | None = None
    max_age_ms: int | None = None
    stale: bool = False
    label: str = "unavailable"

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_update_ts_ms": int(max(0, self.last_update_ts_ms)),
            "age_ms": self.age_ms,
            "max_age_ms": self.max_age_ms,
            "stale": bool(self.stale),
            "label": str(self.label),
        }


@dataclass(frozen=True)
class ReadinessEvidenceItem:
    id: str
    title: str
    status: str
    severity: str
    blocking: bool
    source_subsystem: str
    source_route: str = ""
    source_config_key: str = ""
    freshness: Freshness = field(default_factory=Freshness)
    detail: str = ""
    remediation: str = ""
    category: str = "runtime"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": str(self.id),
            "title": str(self.title),
            "status": _normalize_status(self.status),
            "severity": _normalize_severity(self.severity),
            "blocking": bool(self.blocking),
            "source_subsystem": str(self.source_subsystem),
            "source_route": str(self.source_route),
            "source_config_key": str(self.source_config_key),
            "freshness": self.freshness.to_dict(),
            "detail": str(self.detail),
            "remediation": str(self.remediation),
            "category": str(self.category or self.source_subsystem or "runtime"),
        }


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_int_ms(name: str, default_ms: int) -> int:
    raw = str(os.environ.get(str(name), "") or "").strip()
    if not raw:
        return int(default_ms)
    try:
        value = float(raw)
    except Exception:
        return int(default_ms)
    return int(max(0.0, value) * 1000.0)


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_mode(value: Any, default: str = "safe") -> str:
    text = str(value if value not in (None, "") else default).strip().lower() or default
    if text in {"sim-paper", "sim_paper"}:
        return "paper"
    if text in {"dev", "development", "test"}:
        return "safe"
    return text


def _critical_context(mode: str, execution_mode: str) -> bool:
    return _normalize_mode(mode) in CRITICAL_CONTEXT_MODES or _normalize_mode(execution_mode) in CRITICAL_CONTEXT_MODES


def _normalize_status(status: str) -> str:
    value = str(status or "").strip().lower()
    if value in {PASSING, "ok", "ready", "healthy", "pass", "passed"}:
        return PASSING
    if value in {WARNING, "warn", "degraded", "stale"}:
        return WARNING
    if value in {BLOCKED, "block", "failed", "failure", "critical"}:
        return BLOCKED
    return UNAVAILABLE


def _normalize_severity(severity: str) -> str:
    value = str(severity or "").strip().lower()
    if value in {CRITICAL, "crit", "blocker", "error"}:
        return CRITICAL
    if value in {WARN, "warn", "degraded"}:
        return WARN
    return INFO


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _dedupe(values: Sequence[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _freshness(
    ts_ms: Any,
    *,
    now_ms: int,
    max_age_ms: int | None = None,
    stale: bool | None = None,
) -> Freshness:
    ts = _safe_int(ts_ms, 0)
    age_ms = max(0, int(now_ms) - ts) if ts > 0 else None
    computed_stale = bool(stale) if stale is not None else bool(ts <= 0)
    if ts > 0 and max_age_ms is not None:
        computed_stale = computed_stale or age_ms is None or age_ms > int(max_age_ms)
    label = "unavailable"
    if ts > 0:
        label = "stale" if computed_stale else "fresh"
    return Freshness(
        last_update_ts_ms=ts,
        age_ms=age_ms,
        max_age_ms=max_age_ms,
        stale=bool(computed_stale),
        label=label,
    )


def _status_from_ok(
    ok: Any,
    *,
    required: bool,
    stale: bool = False,
    missing: bool = False,
) -> str:
    if missing:
        return UNAVAILABLE if required else WARNING
    if ok is True and not stale:
        return PASSING
    if ok is True and stale:
        return WARNING if not required else BLOCKED
    if ok is False:
        return BLOCKED if required else WARNING
    return UNAVAILABLE if required else WARNING


def _severity_for(status: str, *, required: bool) -> str:
    normalized = _normalize_status(status)
    if normalized == BLOCKED and required:
        return CRITICAL
    if normalized in {BLOCKED, WARNING, UNAVAILABLE}:
        return WARN if not required else CRITICAL
    return INFO


def _blocking_for(status: str, *, required: bool) -> bool:
    return bool(required and _normalize_status(status) in {BLOCKED, UNAVAILABLE})


def _item_from_payload(
    *,
    item_id: str,
    title: str,
    payload: Mapping[str, Any] | None,
    required: bool,
    source_subsystem: str,
    source_route: str = "",
    source_config_key: str = "",
    category: str = "",
    remediation: str = "",
    now_ms: int,
    max_age_ms: int | None = None,
    detail_keys: Sequence[str] = ("reason", "detail", "status", "state"),
) -> ReadinessEvidenceItem:
    data = _dict(payload)
    blockers = [str(x) for x in _list(data.get("blockers")) if str(x or "").strip()]
    stale = bool(data.get("stale"))
    if max_age_ms is not None:
        ts_value = _safe_int(
            data.get("ts_ms")
            or data.get("updated_ts_ms")
            or data.get("last_update_ts_ms")
            or data.get("tested_ts_ms")
            or data.get("last_ts_ms"),
            0,
        )
        if ts_value <= 0:
            stale = True
        else:
            stale = stale or (int(now_ms) - ts_value) > int(max_age_ms)
    status = _status_from_ok(data.get("ok"), required=required, stale=stale, missing=not bool(data))
    detail = _first_text(
        blockers[0] if blockers else "",
        *[data.get(key) for key in detail_keys],
        "evidence_unavailable" if not data else "",
    )
    ts_ms = (
        data.get("ts_ms")
        or data.get("updated_ts_ms")
        or data.get("last_update_ts_ms")
        or data.get("tested_ts_ms")
        or data.get("last_ts_ms")
    )
    return ReadinessEvidenceItem(
        id=item_id,
        title=title,
        status=status,
        severity=_severity_for(status, required=required),
        blocking=_blocking_for(status, required=required),
        source_subsystem=source_subsystem,
        source_route=source_route,
        source_config_key=source_config_key,
        freshness=_freshness(ts_ms, now_ms=now_ms, max_age_ms=max_age_ms, stale=stale),
        detail=detail or ("passing" if status == PASSING else "evidence unavailable"),
        remediation=remediation,
        category=category or source_subsystem,
    )


def _gate_to_item(gate: Mapping[str, Any], *, now_ms: int, critical_context: bool) -> ReadinessEvidenceItem:
    data = _dict(gate)
    gate_id = str(data.get("name") or "production_gate")
    critical = bool(data.get("critical")) and critical_context
    ok = bool(data.get("ok"))
    status = PASSING if ok else (BLOCKED if critical else WARNING)
    source = str(data.get("source") or "production_validation")
    subsystem = str(data.get("affected_subsystem") or "runtime")
    return ReadinessEvidenceItem(
        id=f"production_gate.{gate_id}",
        title=gate_id.replace("_", " ").title(),
        status=status,
        severity=_severity_for(status, required=critical),
        blocking=bool(critical and status == BLOCKED),
        source_subsystem=subsystem,
        source_route="/api/readiness",
        source_config_key=source,
        freshness=_freshness(data.get("last_evaluated_ts_ms"), now_ms=now_ms),
        detail=str(data.get("reason") or ("ok" if ok else "production gate failed")),
        remediation=_remediation_for_gate(gate_id),
        category=_category_for_gate(gate_id, subsystem),
    )


def _category_for_gate(gate_id: str, subsystem: str) -> str:
    name = str(gate_id)
    if name in {"database_reachable", "schema_valid"}:
        return "storage"
    if "ingestion" in name or "features" in name or "model_inputs" in name or "scoring" in name:
        return "data"
    if "execution" in name or "order" in name or "position" in name or "pnl" in name:
        return "execution"
    if name == "live_trading_preflight":
        return "live_trading"
    if "ui" in name or "api" in name or "operator" in name:
        return "operator"
    return str(subsystem or "runtime")


def _remediation_for_gate(gate_id: str) -> str:
    remediations = {
        "config_valid": "Fix invalid runtime configuration and restart the supervised process.",
        "startup_complete": "Wait for startup to complete or inspect runtime watchdogs for boot failures.",
        "database_reachable": "Restore Postgres/Timescale connectivity and rerun readiness.",
        "schema_valid": "Run the schema repair or migration workflow before operating.",
        "ingestion_active": "Start the required ingestion jobs and confirm provider sessions are healthy.",
        "ingestion_not_stale": "Repair stale market-data ingestion before trading.",
        "critical_features_valid": "Recompute required feature snapshots before serving decisions.",
        "model_inputs_valid": "Restore model input generation and verify freshness.",
        "scoring_pipeline_operational": "Restart scoring jobs and verify latest predictions.",
        "execution_engine_initialized": "Start execution services and inspect execution supervisor state.",
        "order_state_consistent": "Reconcile order state before enabling broker-facing actions.",
        "position_state_consistent": "Reconcile broker and portfolio positions before operation.",
        "pnl_calculation_valid": "Repair PnL/accounting inputs before operation.",
        "live_trading_preflight": "Open the live preflight detail and clear every listed blocker.",
        "api_layer_healthy": "Restore required API handlers and re-run startup validation.",
        "operator_server_healthy": "Restart or reconnect the operator server.",
        "critical_ui_dependencies_available": "Restore required dashboard assets and API routes.",
    }
    return remediations.get(str(gate_id), "Inspect the owning subsystem and clear the reported blocker.")


def _health_items(
    *,
    health_payload: Mapping[str, Any] | None,
    readiness_payload: Mapping[str, Any] | None,
    liveness_payload: Mapping[str, Any] | None,
    now_ms: int,
    critical_context: bool,
) -> list[ReadinessEvidenceItem]:
    health = _dict(health_payload)
    readiness = _dict(readiness_payload)
    liveness = _dict(liveness_payload)
    return [
        _item_from_payload(
            item_id="probe.liveness",
            title="Liveness probe",
            payload=(
                {**liveness, "ok": bool(liveness.get("alive", liveness.get("ok")))}
                if liveness
                else {"ok": health.get("ok"), "ts_ms": health.get("ts_ms"), "reason": health.get("status")}
            ),
            required=False,
            source_subsystem="runtime",
            source_route="/api/liveness",
            category="runtime",
            remediation="If liveness is unavailable, restart the dashboard process or inspect process supervision.",
            now_ms=now_ms,
        ),
        _item_from_payload(
            item_id="probe.health",
            title="Health snapshot",
            payload=health,
            required=critical_context,
            source_subsystem="runtime",
            source_route="/api/health",
            category="runtime",
            remediation="Open system health details and repair the first failing subsystem.",
            now_ms=now_ms,
        ),
        _item_from_payload(
            item_id="probe.readiness",
            title="Runtime readiness",
            payload=readiness,
            required=critical_context,
            source_subsystem="runtime",
            source_route="/api/readiness",
            category="runtime",
            remediation="Use the production validation gates below to clear readiness blockers.",
            now_ms=now_ms,
        ),
    ]


def _live_preflight_items(
    live_preflight: Mapping[str, Any] | None,
    *,
    now_ms: int,
    critical_context: bool,
    target_live: bool,
) -> list[ReadinessEvidenceItem]:
    data = _dict(live_preflight)
    required = bool(target_live)
    items = [
        _item_from_payload(
            item_id="live_preflight.aggregate",
            title="Live trading preflight",
            payload=data,
            required=required,
            source_subsystem="runtime.live_trading_preflight",
            source_route="/api/operator/readiness_evidence",
            category="live_trading",
            remediation="Clear every live preflight blocker before enabling live execution.",
            now_ms=now_ms,
        )
    ]
    nested_specs = [
        ("live_preflight.deployment_contract", "Deployment contract", "deployment_contract", "runtime.live_trading_preflight", "LIVE_TRADING_CONFIRM"),
        ("broker.environment_contract", "Broker environment contract", "broker_contract", "execution.broker_failover_policy", "BROKER/LIVE_BROKER"),
        ("broker.startup_preflight", "Broker startup preflight", "broker_preflight", "execution.broker_failover_policy", "BROKER_STARTUP_PREFLIGHT"),
        ("kill_switch.initial_hold", "Initial kill-switch hold", "initial_kill_switch_hold", "execution.kill_switch", "KILL_SWITCH_GLOBAL"),
        ("backup.restore_evidence", "Backup and restore evidence", "backup_restore_evidence", "runtime.backup_evidence", "BACKUP_EVIDENCE_PATH"),
        ("live_ai.safety", "Live AI serving safety", "live_ai_safety", "runtime.live_ai_safety", "DECISION_MIN_CONFIDENCE"),
        ("execution.arming_audit", "Execution arming audit", "execution_arming_audit", "runtime.live_trading_preflight", "execution_mode_audit"),
        ("reconcile.position_evidence", "Pre-live position reconciliation", "position_reconcile_evidence", "execution.position_reconcile", "position_reconcile_audit"),
        ("options.readiness", "Options instrument readiness", "options_instruments", "execution.options_readiness", "OPTIONS_INSTRUMENTS_MODE"),
        ("lob.deeplob_shadow", "LOB DeepLOB shadow readiness", "lob_deeplob_shadow", "execution.lob_simulation", "EXEC_LOB_DEEPLOB_SHADOW_ENABLED"),
    ]
    for item_id, title, key, subsystem, config_key in nested_specs:
        payload = _dict(data.get(key))
        nested_required = bool(payload.get("required")) or required
        if key == "lob_deeplob_shadow":
            nested_required = bool(payload.get("enabled")) and required
        items.append(
            _item_from_payload(
                item_id=item_id,
                title=title,
                payload=payload,
                required=bool(nested_required),
                source_subsystem=subsystem,
                source_route="/api/operator/readiness_evidence",
                source_config_key=config_key,
                category="live_trading" if item_id.startswith("live") else item_id.split(".", 1)[0],
                remediation=_remediation_for_nested_preflight(item_id),
                now_ms=now_ms,
            )
        )
    return items


def _remediation_for_nested_preflight(item_id: str) -> str:
    mapping = {
        "live_preflight.deployment_contract": "Set dashboard token, live confirmation, operator sidecar, and execution-mode deployment settings.",
        "broker.environment_contract": "Align BROKER, BROKER_NAME, LIVE_BROKER, and failover settings for one intended broker.",
        "broker.startup_preflight": "Repair broker credentials and startup connectivity evidence.",
        "kill_switch.initial_hold": "Keep the initial global kill switch hold until live arming is fully audited.",
        "backup.restore_evidence": "Generate signed backup, WAL, and restore-drill evidence within policy freshness limits.",
        "live_ai.safety": "Configure live AI thresholds, model artifacts, feature contracts, and fallback policy explicitly.",
        "execution.arming_audit": "Arm execution only through the audited execution-mode path.",
        "reconcile.position_evidence": "Run pre-live broker/portfolio reconciliation and resolve mismatches.",
        "options.readiness": "Keep options in shadow/paper or implement the missing live options controls.",
        "lob.deeplob_shadow": "Refresh L2, latency, and simulated-fill calibration evidence or disable the shadow path.",
    }
    return mapping.get(str(item_id), "Open live preflight details and clear the owning subsystem blocker.")


def _execution_barrier_item(
    execution_barrier: Mapping[str, Any] | None,
    *,
    now_ms: int,
    critical_context: bool,
) -> ReadinessEvidenceItem:
    barrier = _dict(execution_barrier)
    ok = bool(barrier.get("allowed") or barrier.get("allow_execution") or barrier.get("real_trading_allowed"))
    payload = dict(barrier)
    payload["ok"] = ok
    payload.setdefault("ts_ms", barrier.get("ts_ms") or now_ms)
    required = critical_context
    return _item_from_payload(
        item_id="execution.barrier",
        title="Execution barrier",
        payload=payload,
        required=required,
        source_subsystem="runtime.gates",
        source_route="/api/execution/barrier",
        category="execution",
        remediation="Clear the execution barrier reason before starting broker-facing execution.",
        now_ms=now_ms,
    )


def _kill_switch_item(
    kill_switches: Mapping[str, Any] | None,
    *,
    now_ms: int,
    critical_context: bool,
) -> ReadinessEvidenceItem:
    data = _dict(kill_switches)
    rows = [_dict(row) for row in _list(data.get("state"))]
    active_rows = [row for row in rows if bool(_safe_int(row.get("enabled"), 0))]
    activation_failure = _dict(data.get("activation_failure"))
    active = bool(active_rows or activation_failure.get("active") or data.get("enabled") is True)
    ts_ms = max([_safe_int(row.get("updated_ts_ms"), 0) for row in rows] + [_safe_int(data.get("loaded_ts_ms"), 0)])
    reason = _first_text(
        activation_failure.get("reason") if activation_failure.get("active") else "",
        active_rows[0].get("reason") if active_rows else "",
        data.get("reason"),
        "kill_switch_active" if active else "ok",
    )
    status = BLOCKED if active and critical_context else (WARNING if active else PASSING)
    return ReadinessEvidenceItem(
        id="execution.kill_switch",
        title="Kill-switch state",
        status=status,
        severity=_severity_for(status, required=critical_context),
        blocking=bool(critical_context and active),
        source_subsystem="execution.kill_switch",
        source_route="/api/system/kill_switches",
        source_config_key="kill_switch_state/KILL_SWITCH_GLOBAL",
        freshness=_freshness(ts_ms, now_ms=now_ms, stale=bool(data.get("cache_fresh") is False)),
        detail=reason,
        remediation="Clear or audit the active kill switch before allowing high-risk actions.",
        category="execution",
    )


def _broker_config_item(
    broker_config: Mapping[str, Any] | None,
    *,
    target_broker: str,
    now_ms: int,
    critical_context: bool,
) -> ReadinessEvidenceItem:
    payload = _dict(broker_config)
    cfg = _dict(payload.get("config"))
    broker = str(target_broker or cfg.get("active_broker") or "sim").strip().lower() or "sim"
    last_test = _dict(cfg.get("last_test_result"))
    test_max_age_ms = _env_int_ms("BROKER_CONNECTION_TEST_MAX_AGE_S", DEFAULT_BROKER_TEST_MAX_AGE_MS)
    required = bool(critical_context and broker != "sim")
    test_ts = _safe_int(last_test.get("tested_ts_ms"), 0)
    stale = bool(test_ts <= 0 or (now_ms - test_ts) > test_max_age_ms)
    ok = bool(broker == "sim" or (last_test.get("ok") is True and str(last_test.get("broker") or "") == broker and not stale))
    detail = "sim broker does not require connection test"
    if broker != "sim":
        if not last_test:
            detail = "broker connection test missing"
        elif str(last_test.get("broker") or "") != broker:
            detail = f"last test was for {last_test.get('broker') or 'unknown'}, not {broker}"
        elif stale:
            detail = "broker connection test is stale"
        else:
            detail = _first_text(last_test.get("state"), "broker connection test passed")
    status = _status_from_ok(ok, required=required, stale=stale and broker != "sim", missing=not bool(payload))
    return ReadinessEvidenceItem(
        id="broker.config_test",
        title="Broker config and connection test",
        status=status,
        severity=_severity_for(status, required=required),
        blocking=_blocking_for(status, required=required),
        source_subsystem="api.api_broker_config",
        source_route="/api/broker/config",
        source_config_key="broker.config/broker.last_test",
        freshness=_freshness(test_ts or payload.get("ts_ms"), now_ms=now_ms, max_age_ms=test_max_age_ms if broker != "sim" else None, stale=stale if broker != "sim" else False),
        detail=detail,
        remediation="Run a passing broker connection test for the target broker before activation.",
        category="broker",
    )


def _provider_items(
    provider_readiness: Mapping[str, Any] | None,
    provider_telemetry: Mapping[str, Any] | None,
    *,
    now_ms: int,
    critical_context: bool,
) -> list[ReadinessEvidenceItem]:
    readiness = _dict(provider_readiness)
    telemetry = _dict(provider_telemetry)
    required = bool(critical_context and readiness.get("required", critical_context))
    items = [
        _item_from_payload(
            item_id="providers.readiness",
            title="Required provider readiness",
            payload=readiness,
            required=required,
            source_subsystem="runtime.health",
            source_route="/api/operator/provider_telemetry",
            source_config_key="LIVE_DATA_REQUIRED_PROVIDERS",
            category="data",
            remediation="Configure required providers, credentials, and fresh telemetry in the Data Sources screen.",
            now_ms=now_ms,
        )
    ]
    by_provider = _dict(readiness.get("by_provider"))
    for provider_name, row in sorted(by_provider.items()):
        data = _dict(row)
        max_age_s = _safe_float(data.get("max_age_s"))
        max_age_ms = int(max_age_s * 1000.0) if max_age_s is not None else None
        data.setdefault("ts_ms", data.get("last_ts_ms"))
        items.append(
            _item_from_payload(
                item_id=f"providers.{provider_name}",
                title=f"Provider {provider_name}",
                payload=data,
                required=required,
                source_subsystem="runtime.health",
                source_route="/api/operator/provider_telemetry",
                source_config_key=str(data.get("source_key") or "data_sources"),
                category="data",
                remediation="Fix provider credentials/session health, then wait for fresh telemetry.",
                now_ms=now_ms,
                max_age_ms=max_age_ms,
            )
        )
    if not by_provider and telemetry:
        items.append(
            _item_from_payload(
                item_id="providers.telemetry",
                title="Provider telemetry",
                payload=telemetry,
                required=required,
                source_subsystem="runtime.ipc",
                source_route="/api/operator/provider_telemetry",
                source_config_key="provider_health",
                category="data",
                remediation="Restart market data ingestion and confirm provider telemetry rows are fresh.",
                now_ms=now_ms,
            )
        )
    return items


def _data_source_item(
    health_payload: Mapping[str, Any] | None,
    *,
    now_ms: int,
    critical_context: bool,
) -> ReadinessEvidenceItem:
    health = _dict(health_payload)
    sources = _dict(health.get("ingestion_sources"))
    freshness = _dict(health.get("ingestion_freshness"))
    ok = True
    stale = False
    detail = "data-source health reported passing"
    if sources:
        failed = []
        for name, raw in sorted(sources.items()):
            row = _dict(raw)
            if not bool(row.get("ok", True)):
                failed.append(str(name))
            stale = stale or bool(row.get("stale"))
        ok = not failed and not stale
        if failed:
            detail = "degraded sources=" + ",".join(failed[:8])
        elif stale:
            detail = "one or more data sources are stale"
    elif freshness:
        ok = bool(freshness.get("ok", freshness.get("critical_ok", True)))
        stale = bool(freshness.get("stale") or freshness.get("critical_ok") is False)
        detail = _first_text(
            ",".join(str(code) for code in _list(freshness.get("reason_codes"))),
            freshness.get("detail"),
            "ingestion freshness unavailable" if not ok else "ingestion freshness ok",
        )
    else:
        ok = False
        detail = "data-source health evidence missing"

    status = _status_from_ok(ok, required=critical_context, stale=stale, missing=not bool(sources or freshness))
    return ReadinessEvidenceItem(
        id="data_sources.health",
        title="Data-source health",
        status=status,
        severity=_severity_for(status, required=critical_context),
        blocking=_blocking_for(status, required=critical_context),
        source_subsystem="runtime.health",
        source_route="/api/data_sources",
        source_config_key="data_sources",
        freshness=_freshness(
            freshness.get("updated_ts_ms") or freshness.get("last_ts_ms") or health.get("ts_ms"),
            now_ms=now_ms,
            stale=stale,
        ),
        detail=detail,
        remediation="Open Data Sources, fix degraded sources, and verify fresh ingestion rows.",
        category="data",
    )


def _governance_items(
    governance_evidence: Mapping[str, Any] | None,
    *,
    now_ms: int,
    critical_context: bool,
) -> list[ReadinessEvidenceItem]:
    payload = _dict(governance_evidence)
    evidence = [_dict(item) for item in _list(payload.get("evidence"))]
    wanted = {
        "ope_gate": "OPE evidence",
        "experiment_ledger": "Experiment ledger",
        "production_monitoring": "Production monitoring freshness",
    }
    items: list[ReadinessEvidenceItem] = []
    seen: set[str] = set()
    for item in evidence:
        key = str(item.get("key") or "").strip()
        if key not in wanted:
            continue
        seen.add(key)
        state = str(item.get("state") or "").strip().lower()
        freshness_state = str(item.get("freshness") or "").strip().lower()
        stale = freshness_state in {"stale", "missing", "unknown", "unavailable"}
        ok = state not in {"block", "unknown"} and not stale
        status = _status_from_ok(ok, required=critical_context, stale=stale, missing=False)
        items.append(
            ReadinessEvidenceItem(
                id=f"governance.{key}",
                title=wanted[key],
                status=status,
                severity=_severity_for(status, required=critical_context),
                blocking=_blocking_for(status, required=critical_context),
                source_subsystem="api.governance_evidence",
                source_route="/api/governance/evidence",
                source_config_key=str(item.get("source_artifact") or key),
                freshness=_freshness(
                    item.get("last_update_ts_ms"),
                    now_ms=now_ms,
                    max_age_ms=DEFAULT_PRODUCTION_MONITORING_MAX_AGE_MS if key == "production_monitoring" else None,
                    stale=stale,
                ),
                detail=_first_text(item.get("label"), state, freshness_state),
                remediation=str(item.get("remediation") or "Refresh governance evidence before live or paper operation."),
                category="governance",
            )
        )
    for key, title in wanted.items():
        if key in seen:
            continue
        status = UNAVAILABLE if critical_context else WARNING
        items.append(
            ReadinessEvidenceItem(
                id=f"governance.{key}",
                title=title,
                status=status,
                severity=_severity_for(status, required=critical_context),
                blocking=_blocking_for(status, required=critical_context),
                source_subsystem="api.governance_evidence",
                source_route="/api/governance/evidence",
                source_config_key=key,
                freshness=_freshness(0, now_ms=now_ms, stale=True),
                detail=f"{key} evidence missing",
                remediation="Run the owning governance job and persist fresh evidence.",
                category="governance",
            )
        )
    return items


def _group_items(items: Sequence[ReadinessEvidenceItem]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in items:
        category = str(item.category or "runtime")
        row = grouped.setdefault(
            category,
            {
                "category": category,
                "total": 0,
                "blocked": 0,
                "warning": 0,
                "unavailable": 0,
                "passing": 0,
                "blocking": 0,
            },
        )
        status = _normalize_status(item.status)
        row["total"] += 1
        row[status] = int(row.get(status, 0)) + 1
        if item.blocking:
            row["blocking"] += 1
    return grouped


def _action_guards(
    *,
    items: Sequence[ReadinessEvidenceItem],
    target_mode: str,
    target_broker: str,
) -> dict[str, Any]:
    mode = _normalize_mode(target_mode)
    broker = str(target_broker or "sim").strip().lower() or "sim"
    activation_relevant = [
        item
        for item in items
        if item.category in {"broker", "data", "runtime", "live_trading", "execution"}
        and (item.blocking or _normalize_status(item.status) in {UNAVAILABLE, BLOCKED})
    ]
    warnings = [
        item
        for item in items
        if item.category in {"broker", "data", "runtime", "live_trading", "execution"}
        and _normalize_status(item.status) == WARNING
    ]
    activation_blocking = [
        item
        for item in activation_relevant
        if item.blocking
        and (
            mode == "live"
            or item.category in {"broker", "runtime"}
            or (mode == "paper" and item.category in {"broker", "data"})
        )
    ]
    return {
        "broker_activation": {
            "allowed": not activation_blocking,
            "requires_confirmation": bool(warnings or activation_relevant),
            "target_mode": mode,
            "target_broker": broker,
            "blockers": [item.to_dict() for item in activation_blocking],
            "warnings": [item.to_dict() for item in warnings[:12]],
            "source_route": "/api/operator/readiness_evidence",
        }
    }


def build_readiness_evidence(
    *,
    readiness_payload: Mapping[str, Any] | None = None,
    health_payload: Mapping[str, Any] | None = None,
    liveness_payload: Mapping[str, Any] | None = None,
    execution_barrier: Mapping[str, Any] | None = None,
    kill_switches: Mapping[str, Any] | None = None,
    live_preflight: Mapping[str, Any] | None = None,
    broker_config: Mapping[str, Any] | None = None,
    provider_telemetry: Mapping[str, Any] | None = None,
    governance_evidence: Mapping[str, Any] | None = None,
    mode: str | None = None,
    execution_mode: str | None = None,
    target_broker: str | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Return normalized readiness evidence grouped by owning subsystem."""

    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    readiness = _dict(readiness_payload)
    health = _dict(health_payload)
    health_inner = _dict(health.get("health")) if isinstance(health.get("health"), Mapping) else health
    mode_name = _normalize_mode(mode or readiness.get("mode") or health_inner.get("mode") or os.environ.get("ENGINE_MODE"), "safe")
    execution_mode_name = _normalize_mode(
        execution_mode
        or readiness.get("execution_mode")
        or health_inner.get("execution_mode")
        or os.environ.get("EXECUTION_MODE")
        or mode_name,
        mode_name,
    )
    broker = str(target_broker or os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or "sim").strip().lower() or "sim"
    critical_context = _critical_context(mode_name, execution_mode_name)
    target_live = mode_name == "live" or execution_mode_name == "live"

    production_validation = _dict(readiness.get("production_validation"))
    gates = _dict(production_validation.get("gates"))

    items: list[ReadinessEvidenceItem] = []
    items.extend(
        _health_items(
            health_payload=health_inner,
            readiness_payload=readiness,
            liveness_payload=liveness_payload,
            now_ms=ts_ms,
            critical_context=critical_context,
        )
    )
    for gate_name in list(production_validation.get("gate_order") or gates.keys()):
        gate = gates.get(gate_name)
        if isinstance(gate, Mapping):
            items.append(_gate_to_item(gate, now_ms=ts_ms, critical_context=critical_context))

    items.append(_execution_barrier_item(execution_barrier or readiness.get("execution_barrier"), now_ms=ts_ms, critical_context=critical_context))
    items.append(_kill_switch_item(kill_switches or health_inner.get("kill_switches"), now_ms=ts_ms, critical_context=critical_context))
    items.extend(
        _live_preflight_items(
            live_preflight,
            now_ms=ts_ms,
            critical_context=critical_context,
            target_live=target_live,
        )
    )
    items.append(_broker_config_item(broker_config, target_broker=broker, now_ms=ts_ms, critical_context=critical_context))
    items.extend(
        _provider_items(
            health_inner.get("provider_readiness"),
            provider_telemetry,
            now_ms=ts_ms,
            critical_context=critical_context,
        )
    )
    items.append(_data_source_item(health_inner, now_ms=ts_ms, critical_context=critical_context))
    items.extend(_governance_items(governance_evidence, now_ms=ts_ms, critical_context=critical_context))

    item_dicts = [item.to_dict() for item in items]
    blockers = [item.to_dict() for item in items if item.blocking]
    warnings = [
        item.to_dict()
        for item in items
        if not item.blocking and _normalize_status(item.status) in {WARNING, BLOCKED}
    ]
    unavailable = [item.to_dict() for item in items if _normalize_status(item.status) == UNAVAILABLE]
    grouped = _group_items(items)
    status = PASSING
    if blockers:
        status = BLOCKED
    elif unavailable:
        status = UNAVAILABLE
    elif warnings:
        status = WARNING

    return {
        "ok": not blockers and not unavailable,
        "status": status,
        "mode": mode_name,
        "execution_mode": execution_mode_name,
        "target_broker": broker,
        "critical_context": bool(critical_context),
        "ts_ms": ts_ms,
        "items": item_dicts,
        "categories": grouped,
        "blockers": blockers,
        "warnings": warnings,
        "unavailable": unavailable,
        "summary": {
            "total": len(items),
            "blocking": len(blockers),
            "warnings": len(warnings),
            "unavailable": len(unavailable),
            "passing": sum(1 for item in items if _normalize_status(item.status) == PASSING),
        },
        "action_guards": _action_guards(items=items, target_mode=mode_name, target_broker=broker),
        "source_routes": [
            "/api/readiness",
            "/api/health",
            "/api/liveness",
            "/api/execution/barrier",
            "/api/system/kill_switches",
            "/api/broker/config",
            "/api/operator/provider_telemetry",
            "/api/governance/evidence",
        ],
        "authority": {
            "read_only": True,
            "summary": "Evidence is aggregated here; runtime, execution, broker, and governance gates remain authoritative.",
        },
    }


__all__ = [
    "BLOCKED",
    "CRITICAL",
    "PASSING",
    "UNAVAILABLE",
    "WARNING",
    "Freshness",
    "ReadinessEvidenceItem",
    "build_readiness_evidence",
]
