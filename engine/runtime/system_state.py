"""
FILE: system_state.py

Runtime subsystem module for `system_state`.
"""

import json
import logging
import os
import time
from typing import Dict, Any, List, Optional

from engine.runtime.config_schema import load_runtime_config, ConfigError
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.metrics import emit_gauge
from engine.runtime.tracing import trace_event
from engine.runtime.runtime_meta import meta_get


STATE_BOOTING = "BOOTING"
STATE_WARMING_UP = "WARMING_UP"
STATE_LIVE = "LIVE"
STATE_DEGRADED = "DEGRADED"
STATE_KILL_SWITCH = "KILL_SWITCH"
STATE_SHUTDOWN = "SHUTDOWN"


LOG = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception as e:
        log_failure(
            LOG,
            event="system_state_safe_float",
            code="SYSTEM_STATE_SAFE_FLOAT_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.runtime.system_state",
            extra={"value": repr(v), "default": float(default)},
            include_health=False,
            persist=False,
        )
        return float(default)


def compute_system_state(
    health: Dict[str, Any],
    jobs: List[Dict[str, Any]],
    kill_switches: Optional[Dict[str, Any]] = None,
    readiness: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Pure function computing global state from health, jobs, and kill switches.
    It is intentionally fail-closed: uncertainty should never look LIVE.
    """

    out: Dict[str, Any] = {
        "ok": True,
        "ts_ms": _now_ms(),
        "state": STATE_BOOTING,
        "reasons": [],
        "diagnostics": {"failures": []},
        "jobs": {"running_daemons": [], "running_oneshots": []},
        "monte_carlo_risk": {"enabled": False, "ready": False},
        "competition": {
            "ok": False,
            "champion": {},
            "challengers": [],
            "active_symbols": [],
            "updated_ts_ms": 0,
        },
        "readiness": {
            "ok": False,
            "ready": False,
            "data_feed_ok": False,
            "models_ok": False,
            "risk_ok": False,
            "broker_ok": False,
            "preflight_ok": False,
            "graph_ok": True,
            "startup_validation_ok": False,
        },
    }

    def _record_nonfatal(code: str, exc: Exception, *, stage: str) -> None:
        out["diagnostics"]["failures"].append(
            {
                "code": str(code),
                "stage": str(stage),
                "error": str(exc),
                "error_type": type(exc).__name__,
            }
        )
        log_failure(
            LOG,
            event=str(stage),
            code=str(code),
            message=str(exc),
            error=exc,
            level=logging.WARNING,
            component="engine.runtime.system_state",
            extra={"stage": str(stage)},
            include_health=False,
            persist=True,
        )

    kill_switches = kill_switches or {}
    health = health or {}
    readiness = readiness or {}

    out["readiness"] = {
        "ok": bool(readiness.get("ok")),
        "ready": bool(readiness.get("ready")),
        "data_feed_ok": bool(readiness.get("data_feed_ok")),
        "models_ok": bool(readiness.get("models_ok")),
        "risk_ok": bool(readiness.get("risk_ok")),
        "broker_ok": bool(readiness.get("broker_ok")),
        "preflight_ok": bool(readiness.get("preflight_ok")),
        "graph_ok": bool(readiness.get("graph_ok", True)),
        "startup_validation_ok": bool(readiness.get("startup_validation_ok", readiness.get("ok"))),
    }

    try:
        from engine.runtime.risk_state import get_state_row

        raw, _ts = get_state_row("monte_carlo_risk_info", "")
        if raw:
            out["monte_carlo_risk"] = json.loads(raw)
    except Exception as e:
        _record_nonfatal("SYSTEM_STATE_MONTE_CARLO_SNAPSHOT_FAILED", e, stage="system_state.monte_carlo")

    # Competition/champion state is optional and sourced from runtime_meta so it
    # can be surfaced even when owned by another subsystem.
    try:
        raw_comp = meta_get("competition_runtime", "")
        if raw_comp:
            comp = json.loads(raw_comp)
            if isinstance(comp, dict):
                out["competition"] = {
                    "ok": bool(comp.get("ok")),
                    "champion": dict(comp.get("champion") or {}),
                    "challengers": list(comp.get("challengers") or []),
                    "active_symbols": list(comp.get("active_symbols") or []),
                    "updated_ts_ms": int(comp.get("updated_ts_ms") or 0),
                }
    except Exception as e:
        _record_nonfatal("SYSTEM_STATE_COMPETITION_RUNTIME_META_FAILED", e, stage="system_state.competition_runtime")

    try:
        raw_comp = meta_get("competition_runtime", "")
        if raw_comp:
            comp = json.loads(raw_comp)
            if isinstance(comp, dict):
                out["competition"] = {
                    "ok": bool(comp.get("ok")),
                    "champion": dict(comp.get("champion") or {}),
                    "challengers": list(comp.get("challengers") or []),
                    "active_symbols": list(comp.get("active_symbols") or []),
                    "updated_ts_ms": int(comp.get("updated_ts_ms") or 0),
                }
    except Exception as e:
        _record_nonfatal("SYSTEM_STATE_COMPETITION_RUNTIME_META_DUPLICATE_FAILED", e, stage="system_state.competition_runtime_duplicate")

    try:
        load_runtime_config()
    except ConfigError as e:
        _record_nonfatal("SYSTEM_STATE_CONFIG_LOAD_FAILED", e, stage="system_state.config")
        out["state"] = STATE_DEGRADED
        out["reasons"].append(f"config_error:{e}")
        out["ok"] = False
        return out

    try:
        lifecycle = health.get("lifecycle") or {}
        if lifecycle.get("shutdown") is True:
            out["state"] = STATE_SHUTDOWN
            out["reasons"].append("lifecycle_shutdown")
            out["ok"] = False
            return out
    except Exception as e:
        _record_nonfatal("SYSTEM_STATE_LIFECYCLE_READ_FAILED", e, stage="system_state.lifecycle")

    running_daemons = []
    running_oneshots = []

    if isinstance(jobs, dict):
        jobs_list = list(jobs.values())
    else:
        jobs_list = list(jobs or [])

    for j in jobs_list:
        try:
            if j.get("running"):
                if str(j.get("mode") or "") == "daemon":
                    running_daemons.append(str(j.get("name") or ""))
                else:
                    running_oneshots.append(str(j.get("name") or ""))
        except Exception as e:
            _record_nonfatal("SYSTEM_STATE_JOB_ENTRY_PARSE_FAILED", e, stage="system_state.jobs")
            continue

    out["jobs"]["running_daemons"] = running_daemons
    out["jobs"]["running_oneshots"] = running_oneshots

    prices = health.get("prices") or {}
    labels = health.get("labels") or {}
    model = health.get("model") or {}

    prices_ok = bool(prices.get("ok"))
    labels_ok = bool(labels.get("ok"))
    model_ok = bool(model.get("ok"))

    prices_age_s = _safe_float(prices.get("age_s"), 1e9)

    ks_enabled = False
    try:
        if kill_switches.get("enabled") is True:
            ks_enabled = True
        elif kill_switches.get("state") == "KILL":
            ks_enabled = True
        elif isinstance(kill_switches.get("kill_switches"), dict):
            for v in kill_switches["kill_switches"].values():
                if isinstance(v, dict) and v.get("enabled") is True:
                    ks_enabled = True
                    break
        elif isinstance(kill_switches.get("state"), list):
            for r in kill_switches.get("state") or []:
                try:
                    if isinstance(r, dict) and int(r.get("enabled") or 0) == 1:
                        ks_enabled = True
                        break
                except Exception as e:
                    _record_nonfatal("SYSTEM_STATE_KILL_SWITCH_ROW_PARSE_FAILED", e, stage="system_state.kill_switch_row")
                    continue
    except Exception as e:
        _record_nonfatal("SYSTEM_STATE_KILL_SWITCH_EVAL_FAILED", e, stage="system_state.kill_switch_eval")

    # Kill switches outrank every other readiness or health signal.
    if ks_enabled:
        out["state"] = STATE_KILL_SWITCH
        out["reasons"].append("kill_switch_enabled")
        out["ok"] = False
        return out

    health_reasons = [str(x) for x in (health.get("reasons") or []) if str(x or "").strip()]
    # These reasons imply structural runtime failure rather than ordinary warmup.
    hard_failure_reasons = [
        r for r in health_reasons
        if (
            r.startswith("db_not_ok")
            or r.startswith("schema_not_ok")
            or r.startswith("providers_not_ok")
            or r.startswith("execution_barrier")
            or r.startswith("broker_connection")
        )
    ]

    if not jobs_list:
        out["reasons"].append("no_jobs_visible")
        if not prices_ok:
            out["reasons"].append("prices_not_ok")
        if not labels_ok:
            out["reasons"].append("labels_not_ok")
        if not model_ok:
            out["reasons"].append("model_not_ok")
        for reason in hard_failure_reasons:
            if reason not in out["reasons"]:
                out["reasons"].append(reason)
        out["state"] = STATE_DEGRADED if hard_failure_reasons else STATE_BOOTING
        out["ok"] = False
        return out

    if not prices_ok:
        out["state"] = STATE_DEGRADED if hard_failure_reasons else STATE_WARMING_UP
        out["reasons"].append("prices_not_ok")
        if not labels_ok:
            out["reasons"].append("labels_not_ok")
        if not model_ok:
            out["reasons"].append("model_not_ok")
        for reason in hard_failure_reasons:
            if reason not in out["reasons"]:
                out["reasons"].append(reason)
        out["ok"] = False
        return out

    try:
        max_age_s = float(os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120"))
    except Exception:
        max_age_s = 120.0

    has_price_daemon = (
        "ingestion_runtime" in running_daemons
        or "poll_prices" in running_daemons
        or "stream_prices_polygon_ws" in running_daemons
    )

    # Fresh prices with missing labels/models means the system is alive but not
    # fully ready, so classify that as degraded instead of booting.
    if has_price_daemon and prices_age_s <= max_age_s:
        if not (labels_ok and model_ok):
            out["state"] = STATE_DEGRADED
            out["ok"] = True
            if not labels_ok:
                out["reasons"].append("labels_not_ok")
            if not model_ok:
                out["reasons"].append("model_not_ok")
            return out

        out["state"] = STATE_LIVE
        out["ok"] = True

        if readiness:
            if not bool(readiness.get("startup_validation_ok", readiness.get("ok"))):
                out["state"] = STATE_DEGRADED
                out["ok"] = False
                out["reasons"].append("startup_validation_failed")
                for gate_name in list(readiness.get("waiting_on") or []):
                    gate_text = str(gate_name or "").strip()
                    if gate_text and gate_text not in out["reasons"]:
                        out["reasons"].append(gate_text)

            if not bool(readiness.get("risk_ok")):
                out["state"] = STATE_DEGRADED
                out["ok"] = False
                out["reasons"].append("execution_not_allowed")

            if not bool(readiness.get("broker_ok")):
                out["state"] = STATE_DEGRADED
                out["ok"] = False
                out["reasons"].append("broker_not_ready")

            if not bool(readiness.get("preflight_ok")):
                out["state"] = STATE_DEGRADED
                out["ok"] = False
                out["reasons"].append("preflight_failed")

            if not bool(readiness.get("graph_ok", True)):
                out["state"] = STATE_DEGRADED
                out["ok"] = False
                out["reasons"].append("graph_invalid")

        return out

    out["state"] = STATE_DEGRADED
    out["ok"] = False

    if not has_price_daemon:
        out["reasons"].append("no_price_daemon_running")

    if prices_age_s > max_age_s:
        out["reasons"].append(f"prices_stale_age_s={prices_age_s:.1f}")

    try:
        emit_gauge(
            "supervisor_state",
            1.0,
            component="engine.runtime.system_state",
            extra_tags={
                "state": str(out.get("state") or ""),
                "ok": str(bool(out.get("ok"))).lower(),
            },
        )
        emit_gauge(
            "job_health",
            1.0 if bool(out.get("ok")) else 0.0,
            component="engine.runtime.system_state",
            extra_tags={"metric_scope": "system_state"},
        )
        trace_event(
            "supervisor_state",
            component="engine.runtime.system_state",
            entity_type="system_state",
            entity_id="runtime",
            payload={
                "state": str(out.get("state") or ""),
                "ok": bool(out.get("ok")),
                "reasons": list(out.get("reasons") or []),
                "running_daemons": list((out.get("jobs") or {}).get("running_daemons") or []),
                "running_oneshots": list((out.get("jobs") or {}).get("running_oneshots") or []),
            },
        )
    except Exception as e:
        _record_nonfatal("SYSTEM_STATE_METRICS_EMIT_FAILED", e, stage="system_state.metrics_emit")

    return out
