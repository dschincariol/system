from __future__ import annotations
"""Compute fail-closed execution-gating snapshots for runtime callers.

The gate snapshot is consumed by API handlers, job orchestration, and terminal
order-entry paths that need one stable view of execution eligibility.
"""

import os
import logging
import time
from typing import Any, Dict, List, Optional

from engine.runtime.data_quality import build_data_pipeline_gate_snapshot
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.job_registry import ALLOWED_JOBS
from engine.runtime.live_trading_preflight import live_trading_preflight
from engine.runtime.logging import get_logger

import json


LOG = get_logger("runtime.gates")
_WARNED_NONFATAL_KEYS: set[str] = set()
_EXECUTION_BLOCKED_DEGRADED_CODES_ENV = (
    "PORTFOLIO_STRATEGY_ALLOCATOR_FAILED,"
    "PORTFOLIO_RISK_ENGINE_FAILED,"
    "PORTFOLIO_RISK_GATE_FAILED,"
    "PORTFOLIO_TOTAL_RISK_FAILED"
)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="runtime_gates_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.gates",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

try:
    from engine.runtime.lifecycle_state import get_state as _get_lifecycle_state  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "RUNTIME_GATES_LIFECYCLE_STATE_IMPORT_FAILED",
        e,
        once_key="runtime_gates_lifecycle_state_import_failed",
    )
    _get_lifecycle_state = None  # type: ignore

try:
    from engine.runtime.risk_state import get_state as _get_risk_state  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "RUNTIME_GATES_RISK_STATE_IMPORT_FAILED",
        e,
        once_key="runtime_gates_risk_state_import_failed",
    )
    _get_risk_state = None  # type: ignore


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_csv(name: str, default: str) -> tuple[str, ...]:
    raw = str(os.environ.get(name, default) or default)
    values = [
        str(part or "").strip()
        for part in raw.split(",")
        if str(part or "").strip()
    ]
    return tuple(values)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


_EXECUTION_BLOCKED_DEGRADED_CODES = {
    str(code or "").strip()
    for code in _env_csv("EXECUTION_BLOCKED_DEGRADED_CODES", _EXECUTION_BLOCKED_DEGRADED_CODES_ENV)
    if str(code or "").strip()
}
_EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE = _env_bool(
    "EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE",
    True,
)


def _json_dict_or_empty(raw: Any) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_GATES_JSON_DICT_PARSE_FAILED",
            e,
            once_key="runtime_gates_json_dict_parse_failed",
            raw_preview=text[:120],
        )
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _json_list_or_empty(raw: Any) -> List[Any]:
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_GATES_JSON_LIST_PARSE_FAILED",
            e,
            once_key="runtime_gates_json_list_parse_failed",
            raw_preview=text[:120],
        )
        return []
    return list(payload) if isinstance(payload, list) else []


def _dedupe_strs(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _severity_rank(value: str) -> int:
    severity = _normalize_severity(value, "WARNING")
    if severity == "CRITICAL":
        return 3
    if severity == "DEGRADED":
        return 2
    return 1


def _normalize_severity(value: Any, default: str = "WARNING") -> str:
    sev = str(value or "").strip().upper()
    if sev in ("", "OK", "INFO", "WARN", "WARNING"):
        return "WARNING"
    if sev in ("DEGRADED", "SEVERE"):
        return "DEGRADED"
    if sev in ("CRIT", "CRITICAL", "ERROR", "FATAL", "BLOCK"):
        return "CRITICAL"
    return str(default or "WARNING").strip().upper() or "WARNING"


def _env_mode() -> str:
    # Operator intent comes from env first; later reconciliation with DB state
    # chooses the most restrictive result rather than trusting one source blindly.
    m = (
        os.environ.get("EXECUTION_MODE")
        or os.environ.get("ENGINE_MODE")
        or os.environ.get("OPERATOR_MODE")
        or os.environ.get("MODE")
        or "safe"
    )
    return str(m).strip().lower() or "safe"


def is_execution_job(job_name: str) -> bool:
    """Return whether a registered job is tagged as execution-related.

    Parameters
    ----------
    job_name : str
        Job registry key to inspect.

    Returns
    -------
    bool
        `True` when the registry metadata marks the job as execution-related;
        otherwise `False`.

    Notes
    -----
    The function is intentionally fail-closed. Registry lookup or metadata
    errors are logged and treated as non-execution jobs.
    """

    try:
        spec = ALLOWED_JOBS.get(job_name)
        if not spec:
            return False
        meta = spec[3] if len(spec) > 3 else {}
        return bool(isinstance(meta, dict) and meta.get("execution") is True)
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_GATES_IS_EXECUTION_JOB_FAILED",
            e,
            once_key=f"runtime_gates_is_execution_job:{job_name}",
            job_name=job_name,
        )
        return False


def _explicit_execution_degraded_snapshot(execution_degraded: Any) -> Dict[str, Any]:
    if isinstance(execution_degraded, dict):
        severity = _normalize_severity(
            execution_degraded.get("severity") or execution_degraded.get("level"),
            "CRITICAL",
        )
        reason = str(execution_degraded.get("reason") or execution_degraded.get("detail") or "").strip()
        return {
            "source": "explicit",
            "active": bool(execution_degraded.get("active", True)),
            "severity": severity,
            "reason": reason or "execution_degraded",
            "reason_codes": _dedupe_strs(
                list(execution_degraded.get("reason_codes") or [])
                + [reason or str(execution_degraded.get("detail") or "execution_degraded")]
            ),
            "detail": dict(execution_degraded),
        }

    if isinstance(execution_degraded, str):
        return {
            "source": "explicit",
            "active": True,
            "severity": _normalize_severity(execution_degraded, "CRITICAL"),
            "reason": "execution_degraded",
            "reason_codes": [str(execution_degraded)],
            "detail": {"raw": str(execution_degraded)},
        }

    return {
        "source": "explicit",
        "active": bool(execution_degraded),
        "severity": "CRITICAL" if bool(execution_degraded) else "WARNING",
        "reason": "execution_degraded" if bool(execution_degraded) else "",
        "reason_codes": ["execution_degraded"] if bool(execution_degraded) else [],
        "detail": {},
    }


def _portfolio_execution_degraded_snapshot(risk_state_getter=None) -> Dict[str, Any]:
    get_risk_state = risk_state_getter if callable(risk_state_getter) else _get_risk_state
    if not callable(get_risk_state):
        return {"source": "portfolio_runtime", "active": False, "severity": "WARNING", "reason": "", "reason_codes": [], "detail": {}}
    try:
        payload = _json_dict_or_empty(get_risk_state("portfolio_runtime_health", ""))
        reasons = list(payload.get("degraded_reasons") or [])
        reason_codes = [
            str((row or {}).get("code") or "").strip()
            for row in reasons
            if isinstance(row, dict) and str((row or {}).get("code") or "").strip()
        ]
        blocking_codes = [
            code for code in reason_codes
            if code in _EXECUTION_BLOCKED_DEGRADED_CODES
        ]
        degraded = bool(payload.get("degraded")) or bool(reasons)
        if not degraded:
            return {
                "source": "portfolio_runtime",
                "active": False,
                "severity": "WARNING",
                "reason": "",
                "reason_codes": [],
                "detail": dict(payload),
            }
        severity = "CRITICAL" if blocking_codes else "DEGRADED"
        return {
            "source": "portfolio_runtime",
            "active": bool(blocking_codes),
            "severity": severity,
            "reason": (
                "portfolio_runtime_critical_degraded"
                if blocking_codes
                else "portfolio_runtime_degraded"
            ),
            "reason_codes": _dedupe_strs(blocking_codes or reason_codes),
            "detail": {
                **dict(payload),
                "blocking_codes": list(blocking_codes),
            },
        }
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_GATES_PORTFOLIO_RUNTIME_HEALTH_LOAD_FAILED",
            e,
            once_key="runtime_gates_portfolio_runtime_health_load_failed",
        )
        return {
            "source": "portfolio_runtime",
            "active": True,
            "severity": "CRITICAL",
            "reason": "portfolio_runtime_health_error",
            "reason_codes": ["portfolio_runtime_health_error"],
            "detail": {"error": f"{type(e).__name__}: {e}"},
        }


def _event_bus_execution_degraded_snapshot() -> Dict[str, Any]:
    if not _EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE:
        return {"source": "event_bus", "active": False, "severity": "WARNING", "reason": "", "reason_codes": [], "detail": {}}
    try:
        from engine.runtime.event_bus import get_event_bus  # type: ignore

        stats = dict(get_event_bus().get_stats() or {})
        active = bool(stats.get("critical_backpressure_active"))
        if not active:
            return {
                "source": "event_bus",
                "active": False,
                "severity": "WARNING",
                "reason": "",
                "reason_codes": [],
                "detail": {
                    "critical_backpressure_active": False,
                    "critical_backpressure_count": int(stats.get("critical_backpressure_count") or 0),
                    "critical_queue_size": int(stats.get("critical_queue_size") or 0),
                },
            }
        return {
            "source": "event_bus",
            "active": True,
            "severity": "CRITICAL",
            "reason": "event_bus_critical_backpressure",
            "reason_codes": ["event_bus_critical_backpressure"],
            "detail": {
                "critical_backpressure_active": True,
                "critical_backpressure_count": int(stats.get("critical_backpressure_count") or 0),
                "critical_queue_size": int(stats.get("critical_queue_size") or 0),
                "critical_queue_max_size": int(stats.get("critical_queue_max_size") or 0),
                "last_critical_backpressure_ts_ms": stats.get("last_critical_backpressure_ts_ms"),
            },
        }
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_GATES_EVENT_BUS_HEALTH_LOAD_FAILED",
            e,
            once_key="runtime_gates_event_bus_health_load_failed",
        )
        return {
            "source": "event_bus",
            "active": True,
            "severity": "CRITICAL",
            "reason": "event_bus_health_error",
            "reason_codes": ["event_bus_health_error"],
            "detail": {"error": f"{type(e).__name__}: {e}"},
        }


def get_execution_degraded_snapshot(execution_degraded: Any = False, *, risk_state_getter=None) -> Dict[str, Any]:
    sources = [
        _explicit_execution_degraded_snapshot(execution_degraded),
        _portfolio_execution_degraded_snapshot(risk_state_getter=risk_state_getter),
        _event_bus_execution_degraded_snapshot(),
    ]
    active_sources = [dict(source) for source in sources if bool(source.get("active"))]
    if not active_sources:
        return {
            "active": False,
            "severity": "WARNING",
            "reason": "",
            "reason_codes": [],
            "sources": [dict(source) for source in sources if source.get("reason") or source.get("detail")],
        }
    primary = max(
        active_sources,
        key=lambda row: (_severity_rank(str(row.get("severity") or "WARNING")), len(list(row.get("reason_codes") or []))),
    )
    return {
        "active": True,
        "severity": _normalize_severity(primary.get("severity"), "CRITICAL"),
        "reason": str(primary.get("reason") or "execution_degraded"),
        "reason_codes": _dedupe_strs(
            [code for source in active_sources for code in list(source.get("reason_codes") or [])]
        ),
        "sources": active_sources,
    }


def data_pipeline_gate_snapshot(
    *,
    now_ms: Optional[int] = None,
    ingestion_runtime: Optional[Dict[str, Any]] = None,
    ingestion_freshness: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return dict(
        build_data_pipeline_gate_snapshot(
            now_ms=now_ms,
            ingestion_runtime=dict(ingestion_runtime or {}),
            ingestion_freshness=dict(ingestion_freshness or {}),
        )
        or {}
    )


def execution_gate_snapshot(
    get_execution_mode_fn=None,
    system_state: Optional[Dict[str, Any]] = None,
    kill_switches: Optional[Dict[str, Any]] = None,
    execution_degraded: bool = False,
    portfolio_risk_gate: Optional[Dict[str, Any]] = None,
    risk_state_getter=None,
) -> Dict[str, Any]:
    """Build the current execution barrier snapshot.

    Parameters
    ----------
    get_execution_mode_fn : callable | None, optional
        Legacy callback that returns persisted execution-mode state when the
        caller does not provide `system_state`.
    system_state : dict[str, Any] | None, optional
        Precomputed system-state snapshot that may contain execution mode and
        arming information.
    kill_switches : dict[str, Any] | None, optional
        Kill-switch snapshot used to block execution when any stop condition is
        active.
    execution_degraded : bool | dict[str, Any], optional
        Execution degradation signal. A mapping may include severity and reason
        fields; a truthy scalar is treated as critical.
    portfolio_risk_gate : dict[str, Any] | None, optional
        Portfolio-risk gate snapshot that can independently block order flow.
    risk_state_getter : callable | None, optional
        Read-only risk-state accessor used by callers that already own a DB
        handle and must avoid opening nested runtime connections.

    Returns
    -------
    dict[str, Any]
        Stable payload describing the selected mode, runtime state, block
        reason, and the booleans consumed by the API layer and execution paths,
        including `allowed`, `allow_execution_pipeline`, `allow_simulation`,
        and `real_trading_allowed`.

    Notes
    -----
    The snapshot is compatible with both legacy callers that pass only
    `get_execution_mode_fn` and newer callers that provide richer runtime
    context directly. Unknown errors, ambiguous states, active kill switches,
    and mode mismatches all fail closed.
    """

    ts = _now_ms()

    mode: str = _env_mode()
    env_mode: str = mode
    db_mode: Optional[str] = None
    armed: Optional[int] = None
    source = "env"
    reason = "ok"
    allow_execution_pipeline = False
    allow_simulation = False
    real_trading_allowed = False

    runtime_state = "UNKNOWN"
    runtime_detail = ""
    runtime_source = "unknown"
    gate_severity = "WARNING"
    severity_reasons: List[str] = []
    live_preflight_state: Optional[Dict[str, Any]] = None

    def _normalize_mode(value: Any, fallback: str) -> str:
        m = str(value or fallback).strip().lower() or fallback
        if m in ("safe", "paper", "shadow", "live"):
            return m
        return fallback

    def _normalize_runtime_state(value: Any) -> str:
        s = str(value or "").strip().upper()
        if s in ("WARMING", "WARMING_UP"):
            return "WARMING_UP"
        if s in ("SHUTDOWN", "SHUTTING_DOWN"):
            return "SHUTDOWN"
        if s in ("KILL", "KILL_SWITCH"):
            return "KILL_SWITCH"
        if s == "SCHEMA_REPAIR":
            return "SCHEMA_REPAIR"
        if s in ("BOOTING", "LIVE", "DEGRADED"):
            return s
        return "UNKNOWN"

    def _dedupe_strs(values: List[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for value in values or []:
            item = str(value or "").strip()
            if not item:
                continue
            if item in seen:
                continue
            seen.add(item)
            out.append(item)
        return out

    def _load_ingestion_source_health() -> Dict[str, Any]:
        try:
            from engine.runtime.runtime_meta import meta_get  # type: ignore

            raw = str(meta_get("ingestion_state", "") or "").strip()
            if not raw:
                return {}
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                return {}
            source_health = payload.get("source_health")
            if isinstance(source_health, dict):
                return source_health
        except Exception as e:
            _warn_nonfatal(
                "RUNTIME_GATES_LOAD_INGESTION_SOURCE_HEALTH_FAILED",
                e,
                once_key="runtime_gates_load_ingestion_source_health_failed",
            )
            return {}
        return {}

    def _collect_reason_codes() -> List[str]:
        values: List[str] = []
        if isinstance(system_state, dict):
            for key in ("detail", "reason", "runtime_detail"):
                raw = system_state.get(key)
                if raw not in (None, ""):
                    values.append(str(raw))

            for key in (
                "reasons",
                "critical_blockers",
                "runtime_reason_codes",
                "advisory_reason_codes",
                "reason_codes",
            ):
                raw = system_state.get(key)
                if isinstance(raw, list):
                    values.extend(str(item) for item in raw if str(item or "").strip())

        if runtime_detail:
            values.append(str(runtime_detail))

        source_health = _load_ingestion_source_health()
        if isinstance(source_health, dict):
            for key in ("runtime_reason_codes", "advisory_reason_codes", "stale_critical_sources", "stale_sources"):
                raw = source_health.get(key)
                if isinstance(raw, list):
                    values.extend(str(item) for item in raw if str(item or "").strip())
            if bool(source_health.get("degraded")):
                values.append("critical_ingestion_degraded")
            elif list(source_health.get("advisory_reason_codes") or []):
                values.append("advisory_ingestion_degraded")

        return _dedupe_strs(values)

    def _classify_runtime_severity() -> tuple[str, List[str]]:
        explicit = None
        if isinstance(system_state, dict):
            explicit = (
                system_state.get("execution_gate_severity")
                or system_state.get("gate_severity")
                or system_state.get("severity")
            )
        if explicit not in (None, ""):
            return _normalize_severity(explicit), _dedupe_strs([str(explicit)])

        if runtime_state in ("BOOTING", "WARMING_UP", "SCHEMA_REPAIR", "SHUTDOWN", "KILL_SWITCH", "UNKNOWN"):
            return "CRITICAL", [f"runtime_state_{str(runtime_state or 'unknown').lower()}"]

        if runtime_state == "LIVE":
            return "WARNING", []

        reason_codes = _collect_reason_codes()
        critical_markers = (
            "critical_",
            "prices_not_ok",
            "prices_stale",
            "no_prices",
            "ingestion_not_running",
            "ingestion_stale",
            "kill_switch",
            "portfolio_risk",
            "broker_connection",
            "execution_supervisor_critical",
            "execution_not_allowed",
            "runtime_exit",
            "permanent_failure",
            "spawn_failed",
            "start_failed",
            "lifecycle_monitor_error",
            "db_not_",
            "schema_not_",
            "providers_not_ok",
            "jobs_not_running",
            "all_ingestion_children_restart_disabled",
            "polygon_ws_",
            "ibkr_",
        )
        degraded_markers = (
            "source_degraded:",
            "ingestion_source_degraded:",
            "advisory_ingestion_degraded",
            "alpha_decay_monitor:",
            "labels_not_ok",
            "model_not_ok",
            "competition_not_ok",
            "graph_invalid",
        )

        critical_hits = [
            code for code in reason_codes
            if any(marker in str(code).lower() for marker in critical_markers)
        ]
        if critical_hits:
            return "CRITICAL", _dedupe_strs(critical_hits)

        degraded_hits = [
            code for code in reason_codes
            if any(marker in str(code).lower() for marker in degraded_markers)
        ]
        if degraded_hits or runtime_state == "DEGRADED":
            return "DEGRADED", _dedupe_strs(degraded_hits or reason_codes or ["runtime_state_degraded"])

        return "WARNING", []

    def _apply_mode_state(r: Any, source_name: str) -> None:
        nonlocal mode, db_mode, armed, source

        if isinstance(r, dict):
            if "mode" in r:
                db_mode = _normalize_mode(r.get("mode"), mode)
                mode = db_mode
            if "armed" in r:
                try:
                    armed = int(r.get("armed") or 0)
                except Exception as e:
                    _warn_nonfatal(
                        "RUNTIME_GATES_ARMED_PARSE_FAILED",
                        e,
                        once_key=f"runtime_gates_armed_parse:{source_name}",
                        source=source_name,
                    )
                    armed = None
            source = source_name
        elif isinstance(r, str):
            db_mode = _normalize_mode(r, mode)
            mode = db_mode
            source = source_name + ":str"

    def _apply_runtime_state(r: Any, source_name: str) -> None:
        nonlocal runtime_state, runtime_detail, runtime_source

        if not isinstance(r, dict):
            return

        if "state" in r:
            runtime_state = _normalize_runtime_state(r.get("state"))
            runtime_detail = str(r.get("detail") or "")
            runtime_source = source_name
            return

        lifecycle = r.get("lifecycle")
        if isinstance(lifecycle, dict) and "state" in lifecycle:
            runtime_state = _normalize_runtime_state(lifecycle.get("state"))
            runtime_detail = str(lifecycle.get("detail") or "")
            runtime_source = source_name + ".lifecycle"

    if isinstance(system_state, dict):
        _apply_runtime_state(system_state, "system_state")
        _apply_mode_state(system_state, "system_state")

    if callable(get_execution_mode_fn):
        try:
            _apply_mode_state(get_execution_mode_fn(), "get_execution_mode_fn")
        except Exception as e:
            _warn_nonfatal(
                "RUNTIME_GATES_EXECUTION_MODE_FN_FAILED",
                e,
                once_key="runtime_gates_execution_mode_fn_failed",
            )
            return {
                "ok": False,
                "ts_ms": ts,
                "mode": mode,
                "armed": armed,
                "allow_execution": False,
                "allow_execution_pipeline": False,
                "allow_simulation": False,
                "real_trading_allowed": False,
                "allowed": False,
                "reason": f"execmode_error:{type(e).__name__}",
                "source": "get_execution_mode_fn:error",
                "runtime_state": runtime_state,
                "runtime_detail": runtime_detail,
                "runtime_source": runtime_source,
            }

    if runtime_state == "UNKNOWN" and callable(_get_lifecycle_state):
        try:
            _apply_runtime_state(_get_lifecycle_state(), "lifecycle_state")
        except Exception as e:
            _warn_nonfatal(
                "RUNTIME_GATES_LIFECYCLE_STATE_LOAD_FAILED",
                e,
                once_key="runtime_gates_lifecycle_state_load_failed",
            )
            runtime_state = "UNKNOWN"
            runtime_detail = ""
            runtime_source = "lifecycle_state:error"

    # Most restrictive mode wins when env and DB differ. This prevents stale DB
    # state from silently enabling execution after an operator explicitly chose
    # a safer mode for the current run.
    # Ordering: safe > shadow > live
    order = {
        "safe": 3,
        "paper": 3,
        "shadow": 2,
        "live": 1,
    }
    if db_mode:
        if order.get(env_mode, 3) >= order.get(db_mode, 3):
            mode = _normalize_mode(env_mode, "safe")
            source = f"{source}+env_restrictive"
        else:
            mode = _normalize_mode(db_mode, "safe")

    gate_severity, severity_reasons = _classify_runtime_severity()

    # Boot/warmup/shutdown/unknown states remain hard-blocked. DEGRADED is only
    # blocked when the degradation is classified as critical.
    if runtime_state != "LIVE" and gate_severity == "CRITICAL":
        blocked_reason = {
            "BOOTING": "runtime_state_booting",
            "SCHEMA_REPAIR": "runtime_state_schema_repair",
            "WARMING_UP": "runtime_state_warming_up",
            "DEGRADED": "runtime_state_degraded",
            "SHUTDOWN": "runtime_state_shutdown",
            "KILL_SWITCH": "runtime_state_kill_switch",
            "UNKNOWN": "runtime_state_unknown",
        }.get(runtime_state, f"runtime_state_{str(runtime_state or 'unknown').lower()}")

        return {
            "ok": True,
            "ts_ms": ts,
            "mode": mode,
            "armed": armed,
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "allow_simulation": False,
            "real_trading_allowed": False,
            "allowed": False,
            "reason": blocked_reason,
            "source": source,
            "runtime_state": runtime_state,
            "runtime_detail": runtime_detail,
            "runtime_source": runtime_source,
            "severity": gate_severity,
            "severity_reasons": severity_reasons,
        }

    execution_degraded_state = get_execution_degraded_snapshot(
        execution_degraded,
        risk_state_getter=risk_state_getter,
    )
    if execution_degraded_state.get("active") and execution_degraded_state.get("severity") == "CRITICAL":
        return {
            "ok": True,
            "ts_ms": ts,
            "mode": mode,
            "armed": armed,
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "allow_simulation": False,
            "real_trading_allowed": False,
            "allowed": False,
            "reason": str(execution_degraded_state.get("reason") or "execution_degraded"),
            "source": source,
            "runtime_state": runtime_state,
            "runtime_detail": runtime_detail,
            "runtime_source": runtime_source,
            "severity": "CRITICAL",
            "severity_reasons": _dedupe_strs(
                severity_reasons
                + list(execution_degraded_state.get("reason_codes") or [])
                + [str(execution_degraded_state.get("reason") or "execution_degraded")]
            ),
            "execution_degraded": dict(execution_degraded_state),
        }

    portfolio_risk_state = None
    get_risk_state = risk_state_getter if callable(risk_state_getter) else _get_risk_state
    if callable(get_risk_state):
        try:
            portfolio_risk_block = str(get_risk_state("portfolio_risk_block", "0") or "0").strip()
            portfolio_risk_info_raw = str(get_risk_state("portfolio_risk_info", "") or "")
            portfolio_risk_summary_raw = str(get_risk_state("portfolio_risk_summary", "") or "")
            portfolio_risk_status = str(get_risk_state("portfolio_risk_status", "") or "").strip()
            portfolio_risk_ts_ms = str(get_risk_state("portfolio_risk_ts_ms", "0") or "0").strip()

            portfolio_risk_info = {}
            if portfolio_risk_info_raw:
                try:
                    parsed = json.loads(portfolio_risk_info_raw)
                    if isinstance(parsed, dict):
                        portfolio_risk_info = parsed
                except Exception as e:
                    _warn_nonfatal(
                        "RUNTIME_GATES_PORTFOLIO_RISK_INFO_PARSE_FAILED",
                        e,
                        once_key="runtime_gates_portfolio_risk_info_parse_failed",
                    )
                    portfolio_risk_info = {"raw": portfolio_risk_info_raw}

            portfolio_risk_summary = {}
            if portfolio_risk_summary_raw:
                try:
                    parsed = json.loads(portfolio_risk_summary_raw)
                    if isinstance(parsed, dict):
                        portfolio_risk_summary = parsed
                except Exception as e:
                    _warn_nonfatal(
                        "RUNTIME_GATES_PORTFOLIO_RISK_SUMMARY_PARSE_FAILED",
                        e,
                        once_key="runtime_gates_portfolio_risk_summary_parse_failed",
                    )
                    portfolio_risk_summary = {"raw": portfolio_risk_summary_raw}

            portfolio_risk_state = {
                "blocked": (portfolio_risk_block == "1"),
                "status": str(portfolio_risk_status),
                "ts_ms": int(portfolio_risk_ts_ms or "0"),
                "info": portfolio_risk_info,
                "summary": portfolio_risk_summary,
            }
        except Exception as e:
            _warn_nonfatal(
                "RUNTIME_GATES_PORTFOLIO_RISK_STATE_LOAD_FAILED",
                e,
                once_key="runtime_gates_portfolio_risk_state_load_failed",
            )
            portfolio_risk_state = None

    if isinstance(portfolio_risk_gate, dict) and portfolio_risk_gate.get("blocked"):
        return {
            "ok": True,
            "ts_ms": ts,
            "mode": mode,
            "armed": armed,
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "allow_simulation": False,
            "real_trading_allowed": False,
            "allowed": False,
            "reason": "portfolio_risk_gate_block",
            "source": source,
            "runtime_state": runtime_state,
            "runtime_detail": runtime_detail,
            "runtime_source": runtime_source,
            "portfolio_risk": portfolio_risk_gate,
            "severity": "CRITICAL",
            "severity_reasons": _dedupe_strs(severity_reasons + ["portfolio_risk_gate_block"]),
        }

    if isinstance(portfolio_risk_state, dict) and portfolio_risk_state.get("blocked"):
        return {
            "ok": True,
            "ts_ms": ts,
            "mode": mode,
            "armed": armed,
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "allow_simulation": False,
            "real_trading_allowed": False,
            "allowed": False,
            "reason": "portfolio_risk_block",
            "source": source,
            "runtime_state": runtime_state,
            "runtime_detail": runtime_detail,
            "runtime_source": runtime_source,
            "portfolio_risk": portfolio_risk_state,
            "severity": "CRITICAL",
            "severity_reasons": _dedupe_strs(severity_reasons + ["portfolio_risk_block"]),
        }

    ks = kill_switches
    if isinstance(ks, dict) and isinstance(ks.get("data"), dict):
        ks = ks.get("data")

    active_kill = []

    if isinstance(ks, dict):
        if isinstance(ks.get("state"), list):
            for row in ks.get("state") or []:
                try:
                    if int(row.get("enabled") or 0) == 1:
                        active_kill.append(
                            f"{row.get('scope') or 'global'}:{row.get('key') or 'global'}"
                        )
                except Exception as e:
                    _warn_nonfatal(
                        "RUNTIME_GATES_KILL_SWITCH_ROW_PARSE_FAILED",
                        e,
                        once_key="runtime_gates_kill_switch_row_parse_failed",
                    )
                    continue

        for k, v in ks.items():
            if k == "state":
                continue
            if isinstance(v, dict) and (v.get("enabled") is True or v.get("active") is True):
                active_kill.append(str(k))
            elif v is True:
                active_kill.append(str(k))

    if active_kill:
        return {
            "ok": True,
            "ts_ms": ts,
            "mode": mode,
            "armed": armed,
            "allow_execution": False,
            "allow_execution_pipeline": False,
            "allow_simulation": False,
            "real_trading_allowed": False,
            "allowed": False,
            "reason": "kill_switch_active",
            "source": source,
            "active": active_kill,
            "runtime_state": runtime_state,
            "runtime_detail": runtime_detail,
            "runtime_source": runtime_source,
            "severity": "CRITICAL",
            "severity_reasons": _dedupe_strs(severity_reasons + ["kill_switch_active"]),
        }

    # Final mode mapping happens only after all global blocks have been checked,
    # so shadow/live never override kill switches or degraded runtime state.
    if mode == "safe":
        reason = "mode_safe"
    elif mode == "paper":
        allow_execution_pipeline = True
        allow_simulation = True
        reason = "mode_paper"
    elif mode == "shadow":
        allow_execution_pipeline = True
        reason = "mode_shadow_live_runtime" if runtime_state == "LIVE" else "mode_shadow_degraded_runtime"
    elif mode == "live":
        live_preflight_state = live_trading_preflight(engine_mode=mode)
        if armed is None:
            reason = "mode_live_unarmed_unknown"
        elif armed != 1:
            reason = "mode_live_unarmed"
        elif not bool(live_preflight_state.get("ok")):
            reason = str(live_preflight_state.get("reason") or "live_trading_preflight_failed")
        else:
            real_trading_allowed = True
            allow_execution_pipeline = True
            allow_simulation = True
            reason = "mode_live_armed" if runtime_state == "LIVE" else "mode_live_armed_degraded_runtime"
    else:
        reason = f"mode_unknown:{mode}"

    allow_execution = bool(real_trading_allowed)

    return {
        "ok": True,
        "ts_ms": ts,
        "mode": mode,
        "armed": armed,
        "allow_execution": bool(allow_execution),
        "allow_execution_pipeline": bool(allow_execution_pipeline),
        "allow_simulation": bool(allow_simulation),
        "real_trading_allowed": bool(real_trading_allowed),
        "allowed": bool(allow_execution_pipeline),
        "reason": reason,
        "source": source,
        "runtime_state": runtime_state,
        "runtime_detail": runtime_detail,
        "runtime_source": runtime_source,
        "severity": gate_severity,
        "severity_reasons": severity_reasons,
        "conditional_allow": bool(runtime_state == "DEGRADED" and gate_severity == "DEGRADED"),
        "execution_degraded": dict(execution_degraded_state),
        "live_trading_preflight": dict(live_preflight_state or {}),
    }
