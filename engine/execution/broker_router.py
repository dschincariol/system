# FILE: dev_core/broker_router.py
# REPLACE ENTIRE FILE WITH THIS (copy/paste)

"""
Unified Broker Router

Supports:
- sim
- alpaca
- ibkr

Failover:
  BROKER_FAILOVER="ibkr"
  Retries on exception OR ok=False
  Returns structured failover_attempts

Supports:
- override_orders
- override_order_id
- override_ts_ms

Adds:
- Pre-Live Position Reconciliation Gate
  (blocks LIVE execution if broker positions mismatch baseline)
"""

import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, cast

from engine.execution.execution_slicing_engine import build_order_slices
from engine.execution.contextual_bandit_slicer import validate_routed_learned_orders
from engine.execution.kill_switch_reactivity import wait_with_kill_interrupt
from engine.execution.options_readiness import live_options_order_block
from engine.execution.mode_safety import env_execution_mode_snapshot, live_broker_mode_boundary_block
from engine.execution.broker_failover_policy import (
    LIVE_BROKERS,
    broker_exception_terminal_failure,
    canonical_broker_name,
    configured_failover_chain,
    is_non_retryable_broker_result,
    live_broker_environment_contract,
    validate_live_failover_chain,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.live_execution_control import (
    disabled_live_execution_gate,
    live_execution_disabled,
    prelive_reconcile_policy_gate,
)
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_timing
from engine.runtime.observability import backoff_delay_s, record_component_health, record_rolling_rate
from engine.runtime.tracing import trace_event

_IMPORT_LOG = logging.getLogger("execution.broker_router")


def _import_nonfatal(code: str, error: BaseException) -> None:
    log_failure(
        _IMPORT_LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.broker_router",
        persist=False,
    )


try:
    from engine.runtime.gates import execution_gate_snapshot as _execution_gate_snapshot  # type: ignore
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_EXECUTION_GATE_IMPORT_FAILED", e)
    _execution_gate_snapshot = None  # type: ignore

try:
    from engine.runtime.job_registry import ALLOWED_JOBS as _ALLOWED_JOBS  # type: ignore
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_JOB_REGISTRY_IMPORT_FAILED", e)
    _ALLOWED_JOBS = {}  # type: ignore


try:
    from engine.cache.wrappers.kill_switch import read_kill_switch as _kill_switch_snapshot  # type: ignore
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_KILL_SWITCH_WRAPPER_IMPORT_FAILED", e)
    _kill_switch_snapshot = None  # type: ignore

try:
    from engine.cache.wrappers.execution_mode import read_execution_mode as _get_execution_mode  # type: ignore
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_EXECUTION_MODE_WRAPPER_IMPORT_FAILED", e)
    _get_execution_mode = None  # type: ignore

_read_execution_health = None  # type: ignore

# Best-effort DB access for realized slippage distribution
try:
    from engine.runtime.storage import connect  # type: ignore
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_RUNTIME_STORAGE_IMPORT_FAILED", e)
    connect = None  # type: ignore

# Adaptive slicing (best-effort; router remains loadable)
try:
    from engine.strategy.adaptive_order_slicer import AdaptiveOrderSlicer  # type: ignore
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_ADAPTIVE_ORDER_SLICER_IMPORT_FAILED", e)
    AdaptiveOrderSlicer = None  # type: ignore


# ============================================================
# Adapter imports (best-effort; router remains loadable)
# ============================================================

_sim_apply = None
_alpaca_apply = None
_ibkr_apply = None
_tradier_options_apply = None

# Pre-live reconciliation gate (hard block on mismatch)
try:
    from engine.execution.position_reconcile import pre_live_position_reconcile as _prelive_reconcile
except Exception as e:
    _import_nonfatal("BROKER_ROUTER_POSITION_RECONCILE_IMPORT_FAILED", e)
    _prelive_reconcile = None


LOG = get_logger("execution.broker_router")
_WARNED_NONFATAL_KEYS: set[str] = set()
_SUCCESS_TRACE_MIN_MS = int(os.environ.get("BROKER_ROUTER_SUCCESS_TRACE_MIN_MS", "250"))
BROKER_ROUTER_RETRY_ATTEMPTS = max(1, int(os.environ.get("BROKER_ROUTER_RETRY_ATTEMPTS", "2")))
BROKER_ROUTER_RETRY_BASE_S = max(0.0, float(os.environ.get("BROKER_ROUTER_RETRY_BASE_S", "0.1")))
BROKER_ROUTER_RETRY_MAX_S = max(0.0, float(os.environ.get("BROKER_ROUTER_RETRY_MAX_S", "1.0")))
_PRELIVE_RECONCILE_FALSEY = {"0", "false", "f", "no", "n", "off", "none", "null"}
_TRUTHY = {"1", "true", "t", "yes", "y", "on"}
_FUTURES_MONTH_TO_NUM = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.broker_router",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _resolve_sim_apply():
    global _sim_apply
    if _sim_apply is not None:
        return _sim_apply
    try:
        from engine.execution.broker_sim import apply_new_portfolio_orders

        _sim_apply = apply_new_portfolio_orders
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_SIM_IMPORT_FAILED", e, once_key="adapter_import:sim")
        _sim_apply = None
    return _sim_apply


def _resolve_alpaca_apply():
    global _alpaca_apply
    if _alpaca_apply is not None:
        return _alpaca_apply
    try:
        from engine.execution.broker_alpaca_rest import apply_latest_portfolio_orders_live

        _alpaca_apply = apply_latest_portfolio_orders_live
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_ALPACA_IMPORT_FAILED", e, once_key="adapter_import:alpaca")
        _alpaca_apply = None
    return _alpaca_apply


def _resolve_ibkr_apply():
    global _ibkr_apply
    if _ibkr_apply is not None:
        return _ibkr_apply
    try:
        from engine.execution.broker_ibkr_gateway import apply_latest_portfolio_orders_live

        _ibkr_apply = apply_latest_portfolio_orders_live
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_IBKR_IMPORT_FAILED", e, once_key="adapter_import:ibkr")
        _ibkr_apply = None
    return _ibkr_apply


def _resolve_tradier_options_apply():
    global _tradier_options_apply
    if _tradier_options_apply is not None:
        return _tradier_options_apply
    try:
        from engine.execution.broker_tradier_options import apply_latest_portfolio_orders_live

        _tradier_options_apply = apply_latest_portfolio_orders_live
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_TRADIER_OPTIONS_IMPORT_FAILED", e, once_key="adapter_import:tradier_options")
        _tradier_options_apply = None
    return _tradier_options_apply


def _safe_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ROUTER_SAFE_INT_FAILED",
            e,
            once_key=f"safe_int:{type(value).__name__}:{str(value)[:64]}",
            value_type=type(value).__name__,
        )
        return int(default)


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ROUTER_OPTIONAL_INT_FAILED",
            e,
            once_key=f"optional_int:{type(value).__name__}:{str(value)[:64]}",
            value_type=type(value).__name__,
        )
        return None


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in _TRUTHY


def _stamp_parent_slice_identity(
    order: Dict[str, Any],
    *,
    parent_order_id: Optional[int],
    parent_ts_ms: Optional[int],
    order_index: int,
    slice_index: int,
    slice_count: int,
) -> Dict[str, Any]:
    out = dict(order or {})
    out["slice_index"] = int(slice_index)
    out["adaptive_slice_index"] = int(slice_index)
    out["slice_count"] = int(slice_count)
    out["adaptive_slice_count"] = int(slice_count)
    out["parent_order_index"] = int(order_index)
    if parent_order_id is not None:
        out["parent_order_id"] = int(parent_order_id)
        out["adaptive_parent_order_id"] = int(parent_order_id)
    if parent_ts_ms is not None:
        out["parent_ts_ms"] = int(parent_ts_ms)
        out["adaptive_parent_ts_ms"] = int(parent_ts_ms)
    return out


def _lineage_summary(orders: Optional[List[dict]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "order_count": 0,
        "execution_targets": [],
        "symbols": [],
        "model_ids": [],
        "model_names": [],
        "source_alert_ids": [],
    }
    if not orders:
        return out

    targets = set()
    symbols = set()
    model_ids = set()
    model_names = set()
    source_alert_ids = set()
    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        out["order_count"] += 1
        targets.add(str(order.get("execution_target") or "real").strip().lower() or "real")
        if order.get("symbol") not in (None, ""):
            symbols.add(str(order.get("symbol")).strip().upper())
        if order.get("model_id") not in (None, ""):
            model_ids.add(str(order.get("model_id")).strip() or "baseline")
        if order.get("model_name") not in (None, ""):
            model_names.add(str(order.get("model_name")).strip())
        elif order.get("strategy_name") not in (None, ""):
            model_names.add(str(order.get("strategy_name")).strip())
        if order.get("source_alert_id") is not None:
            try:
                source_alert_ids.add(_safe_int(order.get("source_alert_id")))
            except Exception as e:
                _warn_nonfatal(
                    "BROKER_ROUTER_SOURCE_ALERT_ID_PARSE_FAILED",
                    e,
                    once_key="lineage_summary_source_alert_id_parse",
                    raw_value=order.get("source_alert_id"),
                )

    out["execution_targets"] = sorted(x for x in targets if x)
    out["symbols"] = sorted(x for x in symbols if x)
    out["model_ids"] = sorted(x for x in model_ids if x)
    out["model_names"] = sorted(x for x in model_names if x)
    out["source_alert_ids"] = sorted(source_alert_ids)
    return out


def _normalized_order_source(order: Dict[str, Any]) -> str:
    for key in ("source", "order_source"):
        raw = order.get(key)
        if raw is None:
            continue
        source = str(raw).strip().lower()
        if source:
            return source
    return ""


def _rl_source_block(orders: Optional[List[dict]]) -> Optional[Dict[str, Any]]:
    """Reject shadow RL-originated orders before any broker adapter is reached."""
    blocked: List[Dict[str, Any]] = []
    for idx, order in enumerate(list(orders or [])):
        if not isinstance(order, dict):
            continue
        source = _normalized_order_source(order)
        if source.startswith(("rl.", "rl_")):
            blocked.append(
                {
                    "index": int(idx),
                    "symbol": str(order.get("symbol") or "").strip().upper(),
                    "source": source,
                }
            )
    if not blocked:
        return None
    return {
        "ok": False,
        "status": "rl_source_forbidden",
        "reason": "RL portfolio policies are shadow-only and may not enter the live broker router.",
        "blocked_orders": blocked,
        "stop_failover": True,
    }

# ============================================================
# Helpers
# ============================================================

def _jobs_from_db_snapshot() -> List[Dict[str, Any]]:
    """
    Minimal job list for compute_system_state() when routing execution outside JobManager.
    Uses job_locks.heartbeat_ts_ms to infer running-ness.
    This keeps execution gating consistent even when the router is invoked
    outside the normal supervised job loop.
    """
    connect_fn = connect
    if connect_fn is None:
        return []

    try:
        max_stale_s = float(os.environ.get("HEALTH_JOBS_MAX_STALE_S", "180"))
    except ValueError as e:
        _warn_nonfatal(
            "BROKER_ROUTER_HEALTH_MAX_STALE_PARSE_FAILED",
            e,
            once_key="jobs_from_db_snapshot_max_stale",
            value=os.environ.get("HEALTH_JOBS_MAX_STALE_S", ""),
        )
        max_stale_s = 180.0

    now_ms = int(time.time() * 1000)

    def _load() -> List[Dict[str, Any]]:
        try:
            con = connect_fn(readonly=True)
            try:
                rows = con.execute(
                        "SELECT job_name, heartbeat_ts_ms FROM job_locks"
                    ).fetchall()
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn_nonfatal(
                        "BROKER_ROUTER_JOB_SNAPSHOT_CLOSE_FAILED",
                        e,
                        once_key="jobs_from_db_snapshot_close",
                    )
        except Exception as e:
            _warn_nonfatal(
                "BROKER_ROUTER_JOB_SNAPSHOT_LOAD_FAILED",
                e,
                once_key="jobs_from_db_snapshot_load",
            )
            rows = []

        out: List[Dict[str, Any]] = []
        for r in rows or []:
            name = "<unknown>"
            try:
                name = str(r[0] or "")
                hb = int(r[1] or 0)
                running = (now_ms - hb) <= int(max_stale_s * 1000.0)

                mode = ""
                try:
                    spec = _ALLOWED_JOBS.get(name)
                    if isinstance(spec, (list, tuple)) and len(spec) >= 2:
                        mode = str(spec[1] or "")
                except Exception as e:
                    _warn_nonfatal(
                        "BROKER_ROUTER_ALLOWED_JOB_LOOKUP_FAILED",
                        e,
                        once_key=f"allowed_job_lookup:{name}",
                        job_name=str(name),
                    )
                    mode = ""

                out.append({"name": name, "running": bool(running), "mode": mode})
            except Exception as e:
                _warn_nonfatal(
                    "BROKER_ROUTER_JOB_SNAPSHOT_ROW_FAILED",
                    e,
                    once_key=f"job_snapshot_row:{name}",
                    job_name=str(name),
                )
                continue

        return out

    from engine.runtime.state_cache import cache_get_or_load
    return cache_get_or_load("job_locks", "broker_router_snapshot", _load, ttl_s=1.0)


def _execution_degraded_from_cache() -> Dict[str, Any]:
    if _read_execution_health is None:
        return {"active": False, "detail": {}}
    try:
        health = _read_execution_health() or {}
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ROUTER_EXECUTION_HEALTH_CACHE_READ_FAILED",
            e,
            once_key="execution_health_cache_read",
        )
        return {"active": False, "detail": {}}
    state = str(health.get("state") or health.get("status") or "").strip().lower()
    if state not in {"critical", "degraded", "down", "unhealthy"}:
        return {"active": False, "detail": dict(health or {})}
    severity = "CRITICAL" if state in {"critical", "down", "unhealthy"} else "WARNING"
    return {
        "active": True,
        "severity": severity,
        "reason": f"execution_health_{state}",
        "reason_codes": [f"execution_health_{state}"],
        "detail": dict(health or {}),
    }


def _paper_sim_broker_contract(
    chain: Optional[List[str]] = None,
    *,
    env: Optional[dict] = None,
) -> Dict[str, Any]:
    source: Dict[str, Any] = dict(env) if env is not None else dict(os.environ)
    mode_snapshot = env_execution_mode_snapshot(source)
    invalid = mode_snapshot.get("invalid")
    mode = str(mode_snapshot.get("mode") or "safe").strip().lower() or "safe"
    normalized_chain = [canonical_broker_name(item) for item in list(chain if chain is not None else effective_broker_chain(source))]

    broker_env = {
        "BROKER": str(source.get("BROKER", "") or "").strip(),
        "BROKER_NAME": str(source.get("BROKER_NAME", "") or "").strip(),
        "LIVE_BROKER": str(source.get("LIVE_BROKER", "") or "").strip(),
        "INTENDED_LIVE_BROKER": str(source.get("INTENDED_LIVE_BROKER", "") or "").strip(),
        "BROKER_FAILOVER": str(source.get("BROKER_FAILOVER", "") or "").strip(),
    }
    canonical_env = {
        key: canonical_broker_name(value)
        for key, value in broker_env.items()
        if key != "BROKER_FAILOVER" and value
    }
    blockers: List[str] = []
    if invalid:
        blockers.append("invalid_execution_mode")
    if mode == "paper":
        if normalized_chain != ["sim"]:
            blockers.append("paper_requires_sim_failover_chain")
        live_env_keys = [key for key, value in canonical_env.items() if value in LIVE_BROKERS]
        if live_env_keys:
            blockers.append("paper_live_broker_env_forbidden")
        non_sim_env_keys = [key for key, value in canonical_env.items() if value != "sim"]
        if non_sim_env_keys:
            blockers.append("paper_requires_sim_broker_env")

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "status": "ok" if not blockers else "paper_sim_broker_contract_invalid",
        "reason": "ok" if not blockers else blockers[0],
        "mode": mode,
        "mode_snapshot": dict(mode_snapshot or {}),
        "chain": normalized_chain,
        "broker_env": broker_env,
        "canonical_broker_env": canonical_env,
        "live_adapter_import_reachable": bool(
            any(item in LIVE_BROKERS for item in normalized_chain)
            or any(value in LIVE_BROKERS for value in canonical_env.values())
        ),
        "blockers": blockers,
    }


def _paper_simulation_route_enabled_with_live_disabled(chain: Optional[List[str]]) -> bool:
    if not live_execution_disabled():
        return False
    contract = _paper_sim_broker_contract(chain)
    return bool(contract.get("ok")) and str(contract.get("mode") or "") == "paper"


def _execution_gate_or_block(dry_run: bool, *, chain: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """
    Returns None if allowed, else a structured block response.
    Fail-closed if providers missing.
    """
    if bool(dry_run):
        return None

    allow_paper_sim_with_live_disabled = _paper_simulation_route_enabled_with_live_disabled(chain)
    if live_execution_disabled() and not allow_paper_sim_with_live_disabled:
        return {
            "ok": False,
            "status": "execution_blocked",
            "gate": disabled_live_execution_gate(source="engine.execution.broker_router"),
        }

    if _execution_gate_snapshot is None:
        return {"ok": False, "status": "execution_blocked_gate_unavailable"}

    if _kill_switch_snapshot is None or _get_execution_mode is None:
        return {"ok": False, "status": "execution_blocked_gate_providers_missing"}

    gate = _execution_gate_snapshot(
        get_execution_mode_fn=_get_execution_mode,
        execution_degraded=cast(Any, _execution_degraded_from_cache()),
        kill_switches=(_kill_switch_snapshot() or {}),
    )

    if allow_paper_sim_with_live_disabled:
        mode = str(gate.get("mode") or "").strip().lower()
        if (
            bool(gate.get("ok"))
            and bool(gate.get("allowed"))
            and bool(gate.get("allow_simulation"))
            and not bool(gate.get("real_trading_allowed"))
            and mode == "paper"
        ):
            return None
        return {
            "ok": False,
            "status": "execution_blocked",
            "gate": gate,
            "paper_sim_broker_contract": _paper_sim_broker_contract(chain),
        }

    if (not bool(gate.get("ok"))) or (not bool(gate.get("allowed"))):
        return {"ok": False, "status": "execution_blocked", "gate": gate}

    return None


def _real_trading_gate_or_block(broker_name: str, dry_run: bool) -> Optional[Dict[str, Any]]:
    if bool(dry_run):
        return None

    if live_execution_disabled():
        return {
            "ok": False,
            "status": "real_trading_blocked",
            "broker": str(broker_name),
            "gate": disabled_live_execution_gate(
                source="engine.execution.broker_router",
                extra={"broker": str(broker_name)},
            ),
        }

    if _execution_gate_snapshot is None or _kill_switch_snapshot is None or _get_execution_mode is None:
        return {
            "ok": False,
            "status": "execution_blocked_gate_providers_missing",
            "broker": str(broker_name),
        }

    gate = _execution_gate_snapshot(
        get_execution_mode_fn=_get_execution_mode,
        execution_degraded=cast(Any, _execution_degraded_from_cache()),
        kill_switches=(_kill_switch_snapshot() or {}),
    )
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {
            "ok": False,
            "status": "real_trading_blocked",
            "broker": str(broker_name),
            "gate": gate,
        }
    return None


def _prelive_reconcile_explicitly_disabled() -> bool:
    raw = os.environ.get("EXECUTION_PRELIVE_RECONCILE")
    if raw is None:
        return False
    return str(raw).strip().lower() in _PRELIVE_RECONCILE_FALSEY


def _prelive_reconcile_or_block(
    broker_name: str,
    *,
    correlation_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return a fatal block when live pre-live reconcile cannot run cleanly."""

    broker = str(broker_name or "").lower().strip()
    policy_block = prelive_reconcile_policy_gate(
        source="engine.execution.broker_router",
        engine_mode="live",
        broker=broker,
        audit_override=True,
        correlation_id=correlation_id,
    )
    if policy_block is not None:
        return policy_block

    if _prelive_reconcile_explicitly_disabled():
        return None

    prelive_reconcile = _prelive_reconcile
    if not callable(prelive_reconcile):
        return {
            "ok": False,
            "status": "prelive_reconcile_unavailable",
            "reason": "prelive_reconcile_provider_missing",
            "broker": broker,
            "fatal_reconcile": True,
            "reconcile_provider_available": False,
        }

    try:
        gate = prelive_reconcile(broker=broker) or {}
    except Exception as exc:
        _warn_nonfatal(
            "BROKER_ROUTER_PRELIVE_RECONCILE_FAILED",
            exc,
            once_key="prelive_reconcile",
            broker=broker,
        )
        return {
            "ok": False,
            "status": "prelive_reconcile_exception",
            "reason": "prelive_reconcile_provider_exception",
            "broker": broker,
            "fatal_reconcile": True,
            "error": str(exc),
        }

    if bool(gate.get("ok", False)):
        return None
    return {
        "ok": False,
        "status": str(gate.get("status") or "prelive_reconcile_block"),
        "broker": broker,
        "fatal_reconcile": True,
        "reconcile": dict(gate or {}),
    }


def _load_recent_slippage_bps(symbol: str, broker: str, n: int = 80) -> List[float]:
    """
    Loads recent realized slippage_bps for (symbol, broker) from execution_analytics.
    Falls back to empty on any error.
    """
    if connect is None:
        return []

    sym = str(symbol or "").upper().strip()
    br = str(broker or "").lower().strip()
    n = int(max(10, min(500, int(n))))

    try:
        con = connect()
        try:
            rows = con.execute(
                """
                SELECT slippage_bps
                FROM execution_analytics
                WHERE symbol = ?
                  AND (broker = ? OR broker IS NULL)
                  AND slippage_bps IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (sym, br, n),
            ).fetchall()
            out = []
            for (v,) in rows or []:
                try:
                    out.append(float(v))
                except Exception as e:
                    _warn_nonfatal(
                        "BROKER_ROUTER_SLIPPAGE_PARSE_FAILED",
                        e,
                        once_key="load_recent_slippage_parse",
                        symbol=sym,
                        broker=br,
                        raw_value=v,
                    )
            return out
        finally:
            con.close()
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ROUTER_SLIPPAGE_HISTORY_FAILED",
            e,
            once_key=f"slippage_history:{sym}:{br}",
            symbol=str(sym),
            broker=str(br),
        )
        return []


def _entry_delay_ms(order: Optional[Dict[str, Any]]) -> int:
    src = dict(order or {})
    for key in ("entry_delay_ms", "execution_entry_delay_ms", "entry_wait_ms"):
        try:
            val = src.get(key)
            if val not in (None, ""):
                return max(0, int(float(val)))
        except Exception as e:
            _warn_nonfatal(
                "BROKER_ROUTER_ENTRY_DELAY_PARSE_FAILED",
                e,
                once_key=f"entry_delay:{key}",
                key=str(key),
            )
            continue
    return 0


def _apply_entry_delay(
    *,
    order: Optional[Dict[str, Any]],
    dry_run: bool,
    broker_name: str = "",
    slice_audit: Optional[List[Dict[str, Any]]] = None,
    order_index: Optional[int] = None,
) -> int:
    delay_ms = _entry_delay_ms(order)
    if delay_ms <= 0:
        return 0

    if isinstance(slice_audit, list):
        slice_audit.append(
            {
                "symbol": str((order or {}).get("symbol") or "").upper().strip(),
                "order_index": (int(order_index) if order_index is not None else None),
                "stage": "entry_delay",
                "delay_ms": int(delay_ms),
                "applied": not bool(dry_run),
            }
        )

    if (not bool(dry_run)) and delay_ms > 0:
        ok, reason, meta = wait_with_kill_interrupt(
            delay_s=float(delay_ms) / 1000.0,
            symbol=str((order or {}).get("symbol") or "").upper().strip(),
            model_id=str((order or {}).get("model_id") or ""),
            broker=str(broker_name or ""),
            component="engine.execution.broker_router",
            stage="entry_delay",
        )
        if not bool(ok):
            if isinstance(slice_audit, list):
                slice_audit.append(
                    {
                        "symbol": str((order or {}).get("symbol") or "").upper().strip(),
                        "order_index": (int(order_index) if order_index is not None else None),
                        "stage": "entry_delay",
                        "delay_ms": int(delay_ms),
                        "interrupted": True,
                        "reason": str(reason or "kill_switch_block"),
                        "meta": dict(meta or {}),
                    }
                )
            return -int(delay_ms)

    return int(delay_ms)


def _apply_batch_entry_delay(*, orders: Optional[List[dict]], dry_run: bool, broker_name: str = "") -> int:
    if not orders:
        return 0
    return _apply_entry_delay(order=dict((orders or [None])[0] or {}), dry_run=bool(dry_run), broker_name=str(broker_name or ""))


def _parse_failover_chain() -> List[str]:
    return configured_failover_chain()


def effective_broker_chain(env: Optional[dict] = None) -> List[str]:
    """Return the canonical broker chain the router will use for this process."""

    return configured_failover_chain(env)


def _is_fx_order_symbol(symbol: str) -> bool:
    try:
        from engine.data.fx_instrument import parse_fx_symbol

        parsed = parse_fx_symbol(symbol)
        return bool(parsed is not None and parsed.base_ccy and parsed.quote_ccy and str(parsed.instrument_kind or "") == "fx_spot")
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_FX_PARSE_FAILED", e, once_key=f"fx_parse:{symbol}", symbol=str(symbol))

    try:
        from engine.data.asset_map import asset_class_for_symbol

        text = str(symbol or "").upper().strip().replace("/", "").replace("_", "")
        return bool(asset_class_for_symbol(text) == "FX" and len(text) == 6 and text.isalpha() and text[:3] != text[3:])
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_FX_FALLBACK_FAILED", e, once_key=f"fx_fallback:{symbol}", symbol=str(symbol))
    return False


def _batch_has_fx(orders: Optional[List[dict]]) -> bool:
    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        if _is_fx_order_symbol(str(order.get("symbol") or "")):
            return True
    return False


def _fx_capable_broker(chain: List[str]) -> Optional[str]:
    from engine.execution.broker_failover_policy import IBKR_BROKERS, canonical_broker_name

    for name in list(chain or []):
        if canonical_broker_name(name) in IBKR_BROKERS:
            return "ibkr"
    return None


def _prefer_fx_capable_broker(chain: List[str], orders: Optional[List[dict]]) -> List[str]:
    from engine.execution.broker_failover_policy import canonical_broker_name

    ordered = [canonical_broker_name(name) for name in list(chain or [])]
    if not _batch_has_fx(orders):
        return ordered
    fx_broker = _fx_capable_broker(ordered)
    if not fx_broker or (ordered and ordered[0] == fx_broker):
        return ordered
    return [fx_broker] + [name for name in ordered if canonical_broker_name(name) != fx_broker]


def _fx_order_safety_block(chain: List[str], orders: Optional[List[dict]]) -> Optional[Dict[str, Any]]:
    if not _batch_has_fx(orders):
        return None
    fx_broker = _fx_capable_broker(chain)
    if fx_broker:
        return None
    return {
        "ok": False,
        "status": "fx_broker_unavailable",
        "reason": "fx_execution_requires_ibkr_cash_idealpro",
        "broker": "failover_chain",
        "stop_failover": True,
        "retryable": False,
        "failover_attempts": [],
        "required_broker": "ibkr",
        "supported_fx_route": "IBKR CASH/IDEALPRO",
    }


def normalize_crypto_symbol(symbol: str) -> str:
    """Return the local bare-root crypto symbol used by asset_map/storage.

    Local fallback only: the canonical ``crypto_instrument.py`` owner is still
    absent, so keep this behavior aligned with the other crypto fallbacks.
    """

    text = str(symbol or "").upper().strip()
    if not text:
        return ""
    for separator in ("/", "-", "_", ":"):
        if separator in text:
            text = text.split(separator, 1)[0]
            break
    for quote in ("USDT", "USDC", "USD"):
        if text.endswith(quote) and len(text) > len(quote):
            text = text[: -len(quote)]
            break
    return {"XBT": "BTC"}.get(text, text)


def _is_crypto_order_symbol(symbol: str) -> bool:
    try:
        from engine.data.asset_map import asset_class_for_symbol

        return bool(asset_class_for_symbol(normalize_crypto_symbol(symbol)) == "CRYPTO")
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_CRYPTO_CLASSIFY_FAILED", e, once_key=f"crypto_classify:{symbol}", symbol=str(symbol))
    return False


def _batch_has_crypto(orders: Optional[List[dict]]) -> bool:
    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        if _is_crypto_order_symbol(str(order.get("symbol") or "")):
            return True
    return False


def _crypto_capable_broker(chain: List[str]) -> Optional[str]:
    from engine.execution.broker_failover_policy import IBKR_BROKERS, canonical_broker_name

    for name in list(chain or []):
        if canonical_broker_name(name) in IBKR_BROKERS:
            return "ibkr"
    return None


def _prefer_crypto_capable_broker(chain: List[str], orders: Optional[List[dict]]) -> List[str]:
    from engine.execution.broker_failover_policy import canonical_broker_name

    ordered = [canonical_broker_name(name) for name in list(chain or [])]
    if not _batch_has_crypto(orders):
        return ordered
    crypto_broker = _crypto_capable_broker(ordered)
    if not crypto_broker or (ordered and ordered[0] == crypto_broker):
        return ordered
    return [crypto_broker] + [name for name in ordered if canonical_broker_name(name) != crypto_broker]


def _parse_futures_order_metadata(symbol: str):
    try:
        from engine.data.futures_instrument import parse_futures_symbol

        return parse_futures_symbol(symbol)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ROUTER_FUTURES_PARSE_FAILED",
            e,
            once_key=f"futures_parse:{symbol}",
            symbol=str(symbol),
        )
        return None


def _is_futures_order_symbol(symbol: str) -> bool:
    return _parse_futures_order_metadata(symbol) is not None


def _batch_has_futures(orders: Optional[List[dict]]) -> bool:
    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        if _is_futures_order_symbol(str(order.get("symbol") or "")):
            return True
    return False


def _futures_capable_broker(chain: List[str]) -> Optional[str]:
    from engine.execution.broker_failover_policy import IBKR_BROKERS, canonical_broker_name

    for name in list(chain or []):
        if canonical_broker_name(name) in IBKR_BROKERS:
            return "ibkr"
    return None


def _prefer_futures_capable_broker(chain: List[str], orders: Optional[List[dict]]) -> List[str]:
    from engine.execution.broker_failover_policy import canonical_broker_name

    ordered = [canonical_broker_name(name) for name in list(chain or [])]
    if not _batch_has_futures(orders):
        return ordered
    futures_broker = _futures_capable_broker(ordered)
    if not futures_broker or (ordered and ordered[0] == futures_broker):
        return ordered
    return [futures_broker] + [name for name in ordered if canonical_broker_name(name) != futures_broker]


def _futures_delivery_block_window_ms() -> int:
    default = 7 * 24 * 60 * 60 * 1000
    try:
        return max(0, int(str(os.environ.get("FUTURES_DELIVERY_BLOCK_WINDOW_MS", str(default)) or str(default)).strip()))
    except Exception as e:
        _warn_nonfatal(
            "BROKER_ROUTER_FUTURES_DELIVERY_BLOCK_WINDOW_PARSE_FAILED",
            e,
            once_key="futures_delivery_block_window",
        )
        return int(default)


def _futures_contract_month(symbol: str) -> tuple[int, int] | None:
    text = str(symbol or "").upper().strip()
    match = re.match(r"^([A-Z0-9]+)([FGHJKMNQUVXZ])(\d{2})$", text)
    if match is None:
        return None
    _root, month_code, year_text = match.groups()
    month = _FUTURES_MONTH_TO_NUM.get(month_code)
    if month is None:
        return None
    return 2000 + int(year_text), int(month)


def _futures_order_safety_block(
    orders: Optional[List[dict]],
    *,
    dry_run: bool,
    chain: Optional[List[str]],
    ts_ms: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    if bool(dry_run) or not _batch_has_futures(orders):
        return None

    normalized_chain = [canonical_broker_name(name) for name in list(chain or [])]
    if any(name in LIVE_BROKERS for name in normalized_chain) and not _env_bool("FUTURES_LIVE_TRADING_ENABLED", False):
        return {
            "ok": False,
            "status": "futures_live_disabled",
            "reason": "futures_live_trading_disabled_by_default",
            "broker": "failover_chain",
            "stop_failover": True,
            "retryable": False,
            "env": {"FUTURES_LIVE_TRADING_ENABLED": str(os.environ.get("FUTURES_LIVE_TRADING_ENABLED", "0") or "0")},
        }

    ts = int(ts_ms if ts_ms is not None else time.time() * 1000)
    delivery_window = _futures_delivery_block_window_ms()
    try:
        from engine.data.calendar.futures_sessions import futures_market_closed, is_maintenance_break
    except Exception as e:
        _warn_nonfatal("BROKER_ROUTER_FUTURES_SESSION_IMPORT_FAILED", e, once_key="futures_session_import")
        return {
            "ok": False,
            "status": "futures_session_check_unavailable",
            "reason": "futures_session_check_failed_closed",
            "broker": "failover_chain",
            "stop_failover": True,
            "retryable": False,
            "error": str(e),
        }

    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        symbol = str(order.get("symbol") or "").upper().strip()
        metadata = _parse_futures_order_metadata(symbol)
        if metadata is None:
            continue
        root = str(getattr(metadata, "root", "") or "").upper().strip()
        if is_maintenance_break(ts):
            return {
                "ok": False,
                "status": "futures_maintenance_break_blocked",
                "reason": "futures_order_during_maintenance_break",
                "broker": "failover_chain",
                "symbol": str(getattr(metadata, "symbol", symbol) or symbol),
                "root": root,
                "stop_failover": True,
                "retryable": False,
            }
        if futures_market_closed(ts, session_calendar=str(getattr(metadata, "session_calendar", "") or "CME_GLOBEX_24x5")):
            return {
                "ok": False,
                "status": "futures_closed_session_blocked",
                "reason": "futures_order_during_closed_session",
                "broker": "failover_chain",
                "symbol": str(getattr(metadata, "symbol", symbol) or symbol),
                "root": root,
                "stop_failover": True,
                "retryable": False,
            }
        for key in ("first_notice_ts_ms", "expiry_ts_ms", "last_trade_ts_ms"):
            boundary = _optional_int(order.get(key))
            if boundary is not None and ts >= int(boundary) - int(delivery_window):
                return {
                    "ok": False,
                    "status": "futures_delivery_window_blocked",
                    "reason": f"futures_order_inside_{key}_window",
                    "broker": "failover_chain",
                    "symbol": str(getattr(metadata, "symbol", symbol) or symbol),
                    "root": root,
                    "boundary_ts_ms": int(boundary),
                    "window_ms": int(delivery_window),
                    "stop_failover": True,
                    "retryable": False,
                }
        contract_month = _futures_contract_month(str(getattr(metadata, "symbol", symbol) or symbol))
        if contract_month is not None:
            year, month = contract_month
            month_start = int(datetime(int(year), int(month), 1, tzinfo=timezone.utc).timestamp() * 1000)
            if ts >= int(month_start) - int(delivery_window):
                return {
                    "ok": False,
                    "status": "futures_delivery_window_blocked",
                    "reason": "futures_order_inside_contract_month_window",
                    "broker": "failover_chain",
                    "symbol": str(getattr(metadata, "symbol", symbol) or symbol),
                    "root": root,
                    "boundary_ts_ms": int(month_start),
                    "window_ms": int(delivery_window),
                    "stop_failover": True,
                    "retryable": False,
                }
    return None


def list_open_orders_router(*, broker: Optional[str] = None, timeout_s: float = 10.0) -> Dict[str, Any]:
    from engine.execution.broker_shutdown_risk import list_open_orders_for_broker

    selected = str(broker or (_parse_failover_chain()[0] if _parse_failover_chain() else "sim"))
    return list_open_orders_for_broker(broker=selected, timeout_s=float(timeout_s))


def cancel_open_orders_router(
    *,
    broker: Optional[str] = None,
    timeout_s: float = 10.0,
    command_id: str = "",
) -> Dict[str, Any]:
    from engine.execution.broker_shutdown_risk import cancel_open_orders_for_broker

    selected = str(broker or (_parse_failover_chain()[0] if _parse_failover_chain() else "sim"))
    return cancel_open_orders_for_broker(
        broker=selected,
        timeout_s=float(timeout_s),
        command_id=str(command_id or "broker-router-cancel"),
    )


def flatten_positions_router(
    *,
    broker: Optional[str] = None,
    timeout_s: float = 10.0,
    command_id: str = "",
) -> Dict[str, Any]:
    from engine.execution.broker_shutdown_risk import flatten_positions_for_broker

    selected = str(broker or (_parse_failover_chain()[0] if _parse_failover_chain() else "sim"))
    return flatten_positions_for_broker(
        broker=selected,
        timeout_s=float(timeout_s),
        command_id=str(command_id or "broker-router-flatten"),
    )


def _adaptive_execute_orders(
    *,
    broker_name: str,
    fn,
    dry_run: bool,
    override_orders: List[dict],
    override_order_id: Optional[int],
    override_ts_ms: Optional[int],
) -> Dict[str, Any]:
    """
    Executes override_orders using configurable slicing:
    - TWAP
    - VWAP
    - POV
    - adaptive
    - preserves 1-order direct path when slicing disabled
    """
    if not override_orders:
        return {"ok": True, "status": "no_orders", "broker": broker_name}

    if os.environ.get("EXEC_ADAPTIVE_SLICING", "1") != "1":
        return {"ok": False, "status": "adaptive_disabled"}

    slice_audit: List[Dict[str, Any]] = []
    all_ok = True
    last_res: Dict[str, Any] = {}

    for oidx, order in enumerate(list(override_orders or [])):
        try:
            symbol = str(order.get("symbol") or "").upper().strip()
            if not symbol:
                continue

            qty0 = float(order.get("qty") or 0.0)
            if qty0 == 0.0:
                continue

            recent_slip = _load_recent_slippage_bps(symbol, broker_name, n=80)
            sliced_orders = build_order_slices(order=dict(order), broker_name=str(broker_name))

            if (
                len(sliced_orders) == 1
                and str((order or {}).get("slice_style") or (order or {}).get("execution_style") or "").strip() == ""
                and AdaptiveOrderSlicer is not None
            ):
                spread_bps = float(order.get("spread_bps") or order.get("spread") or order.get("true_spread_bps") or 0.0)
                vol_bps = float(order.get("vol_bps") or order.get("intraday_vol_bps") or order.get("volatility") or 0.0)
                slicer = AdaptiveOrderSlicer(
                    recent_slippage_bps=recent_slip,
                    spread_bps=spread_bps,
                    volatility_bps=vol_bps,
                    symbol=symbol,
                    broker=broker_name,
                )
                plan = slicer.compute_slice_plan(remaining_qty=abs(qty0))
                if bool(plan.get("abort")):
                    slice_audit.append(
                        {
                            "symbol": symbol,
                            "order_index": int(oidx),
                            "slice_index": 0,
                            "abort": True,
                            "abort_reason": plan.get("abort_reason"),
                            "audit": plan.get("audit") or {},
                        }
                    )
                    all_ok = False
                    break

                if float(plan.get("slice_qty") or 0.0) > 0.0 and float(plan.get("slice_qty") or 0.0) < abs(qty0):
                    side_sign = 1.0 if qty0 > 0 else -1.0
                    sliced_orders = []
                    remaining = abs(qty0)
                    slice_index = 0
                    while remaining > 0.0:
                        plan_i = slicer.compute_slice_plan(remaining_qty=remaining)
                        slice_qty = float(plan_i.get("slice_qty") or 0.0)
                        if slice_qty <= 0.0:
                            break
                        so = dict(order)
                        so["qty"] = float(slice_qty) * float(side_sign)
                        so["slice_style"] = "adaptive"
                        so["slice_index"] = int(slice_index)
                        so["slice_parent_qty"] = float(qty0)
                        so["slice_interval_ms"] = int(plan_i.get("delay_ms") or 0)
                        so["adaptive_slice_plan"] = plan_i.get("audit") or {}
                        sliced_orders.append(so)
                        remaining -= float(slice_qty)
                        slice_index += 1

            _apply_entry_delay(
                order=order,
                dry_run=bool(dry_run),
                broker_name=str(broker_name),
                slice_audit=slice_audit,
                order_index=int(oidx),
            )
            if slice_audit and bool(slice_audit[-1].get("interrupted")):
                all_ok = False
                last_res = {
                    "ok": False,
                    "status": "blocked_kill_switch_during_entry_delay",
                    "broker": str(broker_name),
                    "reason": str(slice_audit[-1].get("reason") or "kill_switch_block"),
                    "kill_meta": dict(slice_audit[-1].get("meta") or {}),
                }
                break

            slice_batch = list(sliced_orders or [dict(order)])
            slice_count = len(slice_batch)
            parent_order_id = _optional_int(override_order_id)
            parent_ts_ms = _optional_int(override_ts_ms)

            for sidx, raw_slice in enumerate(slice_batch):
                so = dict(raw_slice or {})
                if slice_count > 1:
                    so = _stamp_parent_slice_identity(
                        so,
                        parent_order_id=parent_order_id,
                        parent_ts_ms=parent_ts_ms,
                        order_index=int(oidx),
                        slice_index=int(sidx),
                        slice_count=int(slice_count),
                    )
                interval_ms = int(so.get("slice_interval_ms") or 0)
                style = str(so.get("slice_style") or "single").strip().lower()
                gate_ok, gate_reason, gate_meta = wait_with_kill_interrupt(
                    delay_s=0.0,
                    symbol=str(so.get("symbol") or symbol).upper().strip(),
                    model_id=str(so.get("model_id") or ""),
                    broker=str(broker_name),
                    component="engine.execution.broker_router",
                    stage="slice_boundary",
                )
                if not bool(gate_ok):
                    all_ok = False
                    last_res = {
                        "ok": False,
                        "status": "blocked_kill_switch_mid_slice",
                        "broker": str(broker_name),
                        "reason": str(gate_reason or "kill_switch_block"),
                        "kill_meta": dict(gate_meta or {}),
                    }
                    slice_audit.append(
                        {
                            "symbol": symbol,
                            "order_index": int(oidx),
                            "slice_index": int(sidx),
                            "slice_count": int(slice_count),
                            "parent_order_id": parent_order_id,
                            "slice_qty": float(so.get("qty") or 0.0),
                            "delay_ms": int(interval_ms),
                            "style": str(style),
                            "ok": False,
                            "status": "blocked_kill_switch_mid_slice",
                            "reason": str(gate_reason or "kill_switch_block"),
                        }
                    )
                    break

                r = _call_adapter(
                    fn,
                    dry_run=bool(dry_run),
                    override_orders=[so],
                    override_order_id=override_order_id,
                    override_ts_ms=override_ts_ms,
                ) or {}

                last_res = dict(r) if isinstance(r, dict) else {"raw": r}
                ok = bool(last_res.get("ok", False))

                slice_audit.append(
                    {
                        "symbol": symbol,
                        "order_index": int(oidx),
                        "slice_index": int(sidx),
                        "slice_count": int(slice_count),
                        "parent_order_id": parent_order_id,
                        "slice_qty": float(so.get("qty") or 0.0),
                        "delay_ms": int(interval_ms),
                        "style": str(style),
                        "ok": ok,
                        "status": last_res.get("status"),
                    }
                )

                if not ok:
                    all_ok = False
                    break

                if interval_ms > 0 and sidx < (len(slice_batch) - 1) and (not bool(dry_run)):
                    wait_ok, wait_reason, wait_meta = wait_with_kill_interrupt(
                        delay_s=float(interval_ms) / 1000.0,
                        symbol=str(so.get("symbol") or symbol).upper().strip(),
                        model_id=str(so.get("model_id") or ""),
                        broker=str(broker_name),
                        component="engine.execution.broker_router",
                        stage="slice_sleep",
                    )
                    if not bool(wait_ok):
                        all_ok = False
                        last_res = {
                            "ok": False,
                            "status": "blocked_kill_switch_mid_slice",
                            "broker": str(broker_name),
                            "reason": str(wait_reason or "kill_switch_block"),
                            "kill_meta": dict(wait_meta or {}),
                        }
                        slice_audit.append(
                            {
                                "symbol": symbol,
                                "order_index": int(oidx),
                                "slice_index": int(sidx),
                                "slice_count": int(slice_count),
                                "parent_order_id": parent_order_id,
                                "slice_qty": float(so.get("qty") or 0.0),
                                "delay_ms": int(interval_ms),
                                "style": str(style),
                                "ok": False,
                                "status": "blocked_kill_switch_mid_slice",
                                "reason": str(wait_reason or "kill_switch_block"),
                            }
                        )
                        break

            if not all_ok:
                break

        except Exception as e:
            all_ok = False
            slice_audit.append({"order_index": int(oidx), "exception": str(e)})

    out: Dict[str, Any] = {}
    out.update(last_res if isinstance(last_res, dict) else {})
    out["ok"] = bool(all_ok)
    out.setdefault("status", "adaptive_sliced" if all_ok else "adaptive_failed")
    out["adaptive_slices"] = slice_audit
    return out


def _is_retryable_result(res: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(res, dict):
        return True
    if bool(res.get("ok")) or bool(res.get("fatal_reconcile")) or bool(res.get("stop_failover")):
        return False
    status = str(res.get("status") or "").strip().lower()
    return status not in {
        "unknown_broker",
        "sim_adapter_missing",
        "alpaca_adapter_missing",
        "ibkr_adapter_missing",
        "tradier_options_adapter_missing",
        "execution_blocked",
        "execution_blocked_gate_unavailable",
        "execution_blocked_gate_providers_missing",
        "real_trading_blocked",
        "execution_mode_blocked",
        "execution_mode_invalid",
        "execution_mode_provider_missing",
        "execution_mode_unavailable",
        "prelive_reconcile_unavailable",
        "prelive_reconcile_exception",
        "prelive_reconcile_block",
        "needs_reconcile",
        "submit_inflight_unknown",
        "submission_reconcile_gate_unavailable",
        "submission_unknown",
        "submission_unrecorded",
        "missing_credentials",
        "auth_failed",
        "authentication_failed",
        "authorization_failed",
        "invalid_credentials",
        "credentials_invalid",
        "ibkr_configuration_invalid",
        "invalid_order_ref",
    }


def _apply_terminal_broker_policy(res: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(res or {})
    if is_non_retryable_broker_result(out):
        out["retryable"] = False
        out["stop_failover"] = True
    return out


def _call_adapter(

    fn,
    *,
    dry_run: bool,
    override_orders: Optional[List[dict]],
    override_order_id: Optional[int],
    override_ts_ms: Optional[int],
):
    """
    Backward-compatible adapter wrapper.
    Allows older backends that may not accept new kwargs.
    """
    try:
        return fn(
            dry_run=bool(dry_run),
            override_orders=override_orders,
            override_order_id=override_order_id,
            override_ts_ms=override_ts_ms,
        )
    except TypeError as e:
        _warn_nonfatal(
            "BROKER_ROUTER_APPLY_SIGNATURE_RETRY_FAILED",
            e,
            once_key=f"apply_signature:{getattr(fn, '__name__', 'unknown')}:override_ts",
            fn_name=getattr(fn, "__name__", "unknown"),
        )
        try:
            return fn(
                dry_run=bool(dry_run),
                override_orders=override_orders,
            )
        except TypeError as inner:
            _warn_nonfatal(
                "BROKER_ROUTER_APPLY_SIGNATURE_MINIMAL_RETRY_FAILED",
                inner,
                once_key=f"apply_signature:{getattr(fn, '__name__', 'unknown')}:override_orders",
                fn_name=getattr(fn, "__name__", "unknown"),
            )
            return fn(dry_run=bool(dry_run))


def _apply_one(
    name: str,
    *,
    dry_run: bool,
    override_orders: Optional[List[dict]] = None,
    override_order_id: Optional[int] = None,
    override_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:

    name = (name or "").lower().strip()
    trace_event(
        "order_router",
        component="engine.execution.broker_router",
        entity_type="broker",
        entity_id=str(name),
        payload={
            "dry_run": bool(dry_run),
            "override_order_id": override_order_id,
            "override_ts_ms": override_ts_ms,
            "order_count": int(len(override_orders or [])),
        },
        broker=str(name),
    )

    # ---------------- SIM (never reconciled) ----------------
    if name in ("sim", "paper", "sandbox"):
        sim_apply = _resolve_sim_apply()
        if sim_apply is None:
            return {"ok": False, "status": "sim_adapter_missing", "broker": name}
        if (not bool(dry_run)) and override_orders:
            ad = _adaptive_execute_orders(
                broker_name="sim",
                fn=sim_apply,
                dry_run=dry_run,
                override_orders=list(override_orders or []),
                override_order_id=override_order_id,
                override_ts_ms=override_ts_ms,
            )
            if isinstance(ad, dict) and ad.get("status") != "adaptive_disabled":
                res = ad or {}
            else:
                delay_res = _apply_batch_entry_delay(orders=override_orders, dry_run=bool(dry_run), broker_name="sim")
                if int(delay_res) < 0:
                    return {"ok": False, "status": "blocked_kill_switch_during_entry_delay", "broker": "sim"}
                res = _call_adapter(
                    sim_apply,
                    dry_run=dry_run,
                    override_orders=override_orders,
                    override_order_id=override_order_id,
                    override_ts_ms=override_ts_ms,
                ) or {}
        else:
            delay_res = _apply_batch_entry_delay(orders=override_orders, dry_run=bool(dry_run), broker_name="sim")
            if int(delay_res) < 0:
                return {"ok": False, "status": "blocked_kill_switch_during_entry_delay", "broker": "sim"}
            res = _call_adapter(
                sim_apply,
                dry_run=dry_run,
                override_orders=override_orders,
                override_order_id=override_order_id,
                override_ts_ms=override_ts_ms,
            ) or {}

        if isinstance(res, dict) and "broker" not in res:
            res["broker"] = "sim"
        return res

    # ---------------- LIVE BROKERS ----------------
    is_live = name in (
        "alpaca", "alpaca_rest",
        "ibkr", "interactivebrokers", "interactive_brokers",
        "ib_gateway", "ibgateway", "tws",
        "tradier_options",
    )

    if is_live:
        gate_block = _real_trading_gate_or_block(broker_name=name, dry_run=bool(dry_run))
        if gate_block is not None:
            return gate_block
        if not bool(dry_run):
            mode_block = live_broker_mode_boundary_block(
                broker=name,
                get_execution_mode_fn=_get_execution_mode,
                environ=os.environ,
            )
            if mode_block is not None:
                return mode_block
        options_block = live_options_order_block(
            override_orders,
            broker=name,
            dry_run=bool(dry_run),
            engine_mode=os.environ.get("ENGINE_MODE", ""),
            execution_mode=os.environ.get("EXECUTION_MODE", ""),
        )
        if options_block is not None:
            return options_block

        futures_block = _futures_order_safety_block(
            override_orders,
            dry_run=bool(dry_run),
            chain=[name],
            ts_ms=override_ts_ms,
        )
        if futures_block is not None:
            futures_block["broker"] = name
            return futures_block

    # Pre-live reconcile gate (never blocks dry_run)
    if is_live and (not bool(dry_run)):
        reconcile_block = _prelive_reconcile_or_block(
            name,
            correlation_id=(str(override_order_id) if override_order_id is not None else None),
        )
        if reconcile_block is not None:
            return reconcile_block

    # ---------------- TRADIER OPTIONS ----------------
    if name == "tradier_options":
        tradier_apply = _resolve_tradier_options_apply()
        if tradier_apply is None:
            return {"ok": False, "status": "tradier_options_adapter_missing", "broker": name}
        delay_res = _apply_batch_entry_delay(orders=override_orders, dry_run=bool(dry_run), broker_name="tradier_options")
        if int(delay_res) < 0:
            return {"ok": False, "status": "blocked_kill_switch_during_entry_delay", "broker": "tradier_options"}
        res = _call_adapter(
            tradier_apply,
            dry_run=dry_run,
            override_orders=override_orders,
            override_order_id=override_order_id,
            override_ts_ms=override_ts_ms,
        ) or {}
        if isinstance(res, dict) and "broker" not in res:
            res["broker"] = "tradier_options"
        return res

    # ---------------- ALPACA ----------------
    if name in ("alpaca", "alpaca_rest"):
        alpaca_apply = _resolve_alpaca_apply()
        if alpaca_apply is None:
            return {"ok": False, "status": "alpaca_adapter_missing", "broker": name}
        if (not bool(dry_run)) and override_orders:
            ad = _adaptive_execute_orders(
                broker_name="alpaca",
                fn=alpaca_apply,
                dry_run=dry_run,
                override_orders=list(override_orders or []),
                override_order_id=override_order_id,
                override_ts_ms=override_ts_ms,
            )
            if isinstance(ad, dict) and ad.get("status") != "adaptive_disabled":
                res = ad or {}
            else:
                delay_res = _apply_batch_entry_delay(orders=override_orders, dry_run=bool(dry_run), broker_name="alpaca")
                if int(delay_res) < 0:
                    return {"ok": False, "status": "blocked_kill_switch_during_entry_delay", "broker": "alpaca"}
                res = _call_adapter(
                    alpaca_apply,
                    dry_run=dry_run,
                    override_orders=override_orders,
                    override_order_id=override_order_id,
                    override_ts_ms=override_ts_ms,
                ) or {}
        else:
            delay_res = _apply_batch_entry_delay(orders=override_orders, dry_run=bool(dry_run), broker_name="alpaca")
            if int(delay_res) < 0:
                return {"ok": False, "status": "blocked_kill_switch_during_entry_delay", "broker": "alpaca"}
            res = _call_adapter(
                alpaca_apply,
                dry_run=dry_run,
                override_orders=override_orders,
                override_order_id=override_order_id,
                override_ts_ms=override_ts_ms,
            ) or {}

        if isinstance(res, dict) and "broker" not in res:
            res["broker"] = "alpaca"
        return res

    # ---------------- IBKR ----------------
    if name in ("ibkr", "interactivebrokers", "interactive_brokers", "ib_gateway", "ibgateway", "tws"):
        ibkr_apply = _resolve_ibkr_apply()
        if ibkr_apply is None:
            return {"ok": False, "status": "ibkr_adapter_missing", "broker": name}
        if (not bool(dry_run)) and override_orders:
            ad = _adaptive_execute_orders(
                broker_name="ibkr",
                fn=ibkr_apply,
                dry_run=dry_run,
                override_orders=list(override_orders or []),
                override_order_id=override_order_id,
                override_ts_ms=override_ts_ms,
            )
            if isinstance(ad, dict) and ad.get("status") != "adaptive_disabled":
                res = ad or {}
            else:
                delay_res = _apply_batch_entry_delay(orders=override_orders, dry_run=bool(dry_run), broker_name="ibkr")
                if int(delay_res) < 0:
                    return {"ok": False, "status": "blocked_kill_switch_during_entry_delay", "broker": "ibkr"}
                res = _call_adapter(
                    ibkr_apply,
                    dry_run=dry_run,
                    override_orders=override_orders,
                    override_order_id=override_order_id,
                    override_ts_ms=override_ts_ms,
                ) or {}
        else:
            delay_res = _apply_batch_entry_delay(orders=override_orders, dry_run=bool(dry_run), broker_name="ibkr")
            if int(delay_res) < 0:
                return {"ok": False, "status": "blocked_kill_switch_during_entry_delay", "broker": "ibkr"}
            res = _call_adapter(
                ibkr_apply,
                dry_run=dry_run,
                override_orders=override_orders,
                override_order_id=override_order_id,
                override_ts_ms=override_ts_ms,
            ) or {}

        if isinstance(res, dict) and "broker" not in res:
            res["broker"] = "ibkr"
        return res

    return {"ok": False, "status": "unknown_broker", "broker": name}


# ============================================================
# Public Router
# ============================================================

def apply_new_portfolio_orders_router(
    dry_run: bool = False,
    override_orders: Optional[List[dict]] = None,
    override_order_id: Optional[int] = None,
    override_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Route portfolio orders through the configured broker failover chain.

    Parameters
    ----------
    dry_run : bool, default=False
        When ``True``, downstream adapters should avoid live order placement.
    override_orders : list of dict, optional
        Explicit order batch to route instead of loading the latest stored
        portfolio-order batch.
    override_order_id : int, optional
        Persisted order or batch identifier used for lineage and tracing.
    override_ts_ms : int, optional
        Epoch milliseconds associated with the override batch.

    Returns
    -------
    dict
        Structured adapter result. Successful and failed outputs include a
        ``failover_attempts`` list, and successful outputs also include the
        selected ``broker``.

    Notes
    -----
    Execution is gated before any broker call. Each broker is retried up to
    ``BROKER_ROUTER_RETRY_ATTEMPTS`` times and only fails over on adapter
    errors or ``ok=False`` results. ``fatal_reconcile`` responses stop the
    failover chain immediately.

    Side Effects
    ------------
    May place live orders unless ``dry_run`` is true and always emits metrics,
    health signals, traces, and structured logs describing the routing attempt.
    """
    rl_block = _rl_source_block(override_orders)
    if rl_block is not None:
        return rl_block

    learned_policy_block = validate_routed_learned_orders(override_orders)
    if learned_policy_block is not None:
        return learned_policy_block

    lineage = _lineage_summary(override_orders)

    chain = effective_broker_chain()
    if not chain:
        chain = ["sim"]

    engine_mode = os.environ.get("ENGINE_MODE", "safe")
    paper_contract = _paper_sim_broker_contract(chain)
    if (
        str(paper_contract.get("mode") or "") == "paper"
        and not bool(paper_contract.get("ok"))
        and live_execution_disabled()
    ):
        return {
            "ok": False,
            "status": "paper_sim_broker_contract_invalid",
            "reason": str(paper_contract.get("reason") or "paper_requires_sim_broker"),
            "broker": "failover_chain",
            "stop_failover": True,
            "retryable": False,
            "failover_attempts": [],
            "paper_sim_broker_contract": paper_contract,
        }

    chain_policy = validate_live_failover_chain(
        chain,
        engine_mode=engine_mode,
        dry_run=bool(dry_run),
    )
    if not bool(chain_policy.get("ok")):
        return {
            "ok": False,
            "status": "live_failover_chain_invalid",
            "reason": str(chain_policy.get("reason") or "sim_after_live_broker_forbidden"),
            "broker": "failover_chain",
            "stop_failover": True,
            "retryable": False,
            "failover_attempts": [],
            "failover_policy": chain_policy,
        }

    if str(engine_mode or "").strip().lower() == "live" and not bool(dry_run):
        broker_contract = live_broker_environment_contract(engine_mode=engine_mode, chain=chain)
        if not bool(broker_contract.get("ok")):
            return {
                "ok": False,
                "status": "live_broker_contract_invalid",
                "reason": str(broker_contract.get("reason") or "live_broker_contract_invalid"),
                "broker": "failover_chain",
                "stop_failover": True,
                "retryable": False,
                "failover_attempts": [],
                "broker_contract": broker_contract,
                "failover_policy": dict(broker_contract.get("chain_policy") or chain_policy),
            }

    blocked = _execution_gate_or_block(dry_run=bool(dry_run), chain=chain)
    if blocked is not None:
        return blocked

    if str(paper_contract.get("mode") or "") == "paper" and not bool(paper_contract.get("ok")):
        return {
            "ok": False,
            "status": "paper_sim_broker_contract_invalid",
            "reason": str(paper_contract.get("reason") or "paper_requires_sim_broker"),
            "broker": "failover_chain",
            "stop_failover": True,
            "retryable": False,
            "failover_attempts": [],
            "paper_sim_broker_contract": paper_contract,
        }

    options_block = live_options_order_block(
        override_orders,
        broker=",".join(chain),
        dry_run=bool(dry_run),
        engine_mode=engine_mode,
        execution_mode=os.environ.get("EXECUTION_MODE", ""),
    )
    if options_block is not None:
        return options_block

    futures_block = _futures_order_safety_block(
        override_orders,
        dry_run=bool(dry_run),
        chain=chain,
        ts_ms=override_ts_ms,
    )
    if futures_block is not None:
        futures_block["failover_attempts"] = []
        return futures_block

    fx_block = _fx_order_safety_block(chain, override_orders)
    if fx_block is not None:
        return fx_block

    chain = _prefer_crypto_capable_broker(
        _prefer_futures_capable_broker(_prefer_fx_capable_broker(chain, override_orders), override_orders),
        override_orders,
    )
    attempts: List[Dict[str, Any]] = []

    for name in chain:
        for attempt_num in range(1, int(BROKER_ROUTER_RETRY_ATTEMPTS) + 1):
            t0 = time.time()
            try:
                res = _apply_one(
                    name,
                    dry_run=bool(dry_run),
                    override_orders=override_orders,
                    override_order_id=override_order_id,
                    override_ts_ms=override_ts_ms,
                ) or {}
                res = _apply_terminal_broker_policy(res)

                dur_ms = int((time.time() - t0) * 1000)

                # If reconciliation gate trips → DO NOT failover
                if bool(res.get("fatal_reconcile", False)):
                    attempts.append(
                        {
                            "broker": name,
                            "attempt": int(attempt_num),
                            "ok": False,
                            "status": res.get("status"),
                            "dur_ms": int(dur_ms),
                        }
                    )
                    record_rolling_rate(
                        "execution_success_rate",
                        success=False,
                        component="engine.execution.broker_router",
                        extra_tags={"broker": str(name)},
                    )
                    record_component_health(
                        "execution",
                        ok=False,
                        status=str(res.get("status") or "prelive_reconcile_block"),
                        detail=str(res.get("status") or "fatal_reconcile"),
                        latency_ms=float(dur_ms),
                        extra={
                            "broker": str(name),
                            "attempts": int(len(attempts)),
                            "order_count": int(lineage.get("order_count") or 0),
                            "symbols": list(lineage.get("symbols") or []),
                        },
                    )
                    res.setdefault("broker", name)
                    res["failover_attempts"] = attempts
                    return res

                if bool(res.get("stop_failover", False)):
                    status = str(res.get("status") or "submit_inflight_unknown")
                    attempts.append(
                        {
                            "broker": name,
                            "attempt": int(attempt_num),
                            "ok": False,
                            "status": status,
                            "dur_ms": int(dur_ms),
                        }
                    )
                    record_rolling_rate(
                        "execution_success_rate",
                        success=False,
                        component="engine.execution.broker_router",
                        extra_tags={"broker": str(name)},
                    )
                    record_component_health(
                        "execution",
                        ok=False,
                        status=str(status),
                        detail=str(res.get("detail") or status),
                        latency_ms=float(dur_ms),
                        extra={
                            "broker": str(name),
                            "attempts": int(len(attempts)),
                            "order_count": int(lineage.get("order_count") or 0),
                            "symbols": list(lineage.get("symbols") or []),
                            "execution_targets": list(lineage.get("execution_targets") or []),
                            "stop_failover": True,
                        },
                    )
                    res.setdefault("broker", name)
                    res["failover_attempts"] = attempts
                    return res

                ok = bool(res.get("ok", False))
                status = str(res.get("status") or ("ok" if ok else "failed"))
                attempts.append(
                    {
                        "broker": name,
                        "attempt": int(attempt_num),
                        "ok": ok,
                        "status": status,
                        "dur_ms": int(dur_ms),
                    }
                )

                if ok:
                    res.setdefault("broker", name)
                    res["failover_attempts"] = attempts
                    emit_counter(
                        "order_throughput",
                        int(len(override_orders or [])) if override_orders else 1,
                        component="engine.execution.broker_router",
                        broker=str(name),
                        extra_tags={"throughput_type": "router_success"},
                    )
                    emit_timing(
                        "execution_latency_ms",
                        int(dur_ms),
                        component="engine.execution.broker_router",
                        broker=str(name),
                    )
                    record_rolling_rate(
                        "execution_success_rate",
                        success=True,
                        component="engine.execution.broker_router",
                        extra_tags={"broker": str(name)},
                    )
                    record_component_health(
                        "execution",
                        ok=True,
                        status=str(status),
                        detail="ok",
                        latency_ms=float(dur_ms),
                        extra={
                            "broker": str(name),
                            "attempts": int(len(attempts)),
                            "order_count": int(lineage.get("order_count") or 0),
                            "symbols": list(lineage.get("symbols") or []),
                            "execution_targets": list(lineage.get("execution_targets") or []),
                        },
                    )
                    if len(attempts) > 1 or int(dur_ms) >= int(_SUCCESS_TRACE_MIN_MS):
                        trace_event(
                            "order_router",
                            component="engine.execution.broker_router",
                            entity_type="broker",
                            entity_id=str(name),
                            payload={
                                "ok": True,
                                "status": res.get("status"),
                                "attempts": attempts,
                                "order_count": int(len(override_orders or [])),
                                "lineage": lineage,
                                "override_order_id": (int(override_order_id) if override_order_id is not None else None),
                                "override_ts_ms": (int(override_ts_ms) if override_ts_ms is not None else None),
                                "latency_ms": int(dur_ms),
                            },
                            broker=str(name),
                        )
                        log_event(
                            LOG,
                            20,
                            "order_router_success",
                            component="engine.execution.broker_router",
                            extra={
                                "broker": str(name),
                                "status": res.get("status"),
                                "attempts": attempts,
                                "order_count": int(len(override_orders or [])),
                                "lineage": lineage,
                                "override_order_id": (int(override_order_id) if override_order_id is not None else None),
                                "override_ts_ms": (int(override_ts_ms) if override_ts_ms is not None else None),
                                "latency_ms": int(dur_ms),
                            },
                        )
                    return res

                retryable = bool(
                    attempt_num < int(BROKER_ROUTER_RETRY_ATTEMPTS)
                    and _is_retryable_result(res)
                )
                if retryable:
                    emit_counter(
                        "retry_attempt",
                        1,
                        component="engine.execution.broker_router",
                        broker=str(name),
                        extra_tags={
                            "operation": "order_router",
                            "attempt": int(attempt_num),
                            "status": str(status),
                        },
                    )
                    log_event(
                        LOG,
                        logging.WARNING,
                        "order_router_retry_scheduled",
                        component="engine.execution.broker_router",
                        extra={
                            "broker": str(name),
                            "attempt": int(attempt_num),
                            "status": str(status),
                            "latency_ms": int(dur_ms),
                            "lineage": lineage,
                        },
                    )
                    retry_delay_s = backoff_delay_s(
                        int(attempt_num),
                        base_s=float(BROKER_ROUTER_RETRY_BASE_S),
                        max_s=float(BROKER_ROUTER_RETRY_MAX_S),
                    )
                    if float(retry_delay_s) > 0.0:
                        retry_wait_ok, retry_wait_reason, retry_wait_meta = wait_with_kill_interrupt(
                            delay_s=float(retry_delay_s),
                            symbol=str((lineage.get("symbols") or [""])[0] or ""),
                            broker=str(name),
                            component="engine.execution.broker_router",
                            stage="broker_retry_backoff",
                        )
                        if not bool(retry_wait_ok):
                            return {
                                "ok": False,
                                "status": "blocked_kill_switch_during_retry_backoff",
                                "broker": str(name),
                                "reason": str(retry_wait_reason or "kill_switch_block"),
                                "kill_meta": dict(retry_wait_meta or {}),
                                "failover_attempts": attempts,
                            }
                    continue
                break

            except Exception as e:
                terminal_res = broker_exception_terminal_failure(broker=name, error=e)
                if terminal_res is not None:
                    _warn_nonfatal(
                        "BROKER_ROUTER_TERMINAL_BROKER_FAILURE",
                        e,
                        once_key=f"broker_terminal:{name}:{terminal_res.get('status')}",
                        broker=str(name),
                        status=str(terminal_res.get("status") or ""),
                        stop_failover=True,
                    )
                    dur_ms = int((time.time() - t0) * 1000)
                    attempts.append(
                        {
                            "broker": name,
                            "attempt": int(attempt_num),
                            "ok": False,
                            "status": str(terminal_res.get("status") or "broker_terminal_failure"),
                            "error": str(e),
                            "dur_ms": int(dur_ms),
                        }
                    )
                    record_rolling_rate(
                        "execution_success_rate",
                        success=False,
                        component="engine.execution.broker_router",
                        extra_tags={"broker": str(name)},
                    )
                    record_component_health(
                        "execution",
                        ok=False,
                        status=str(terminal_res.get("status") or "broker_terminal_failure"),
                        detail=str(terminal_res.get("detail") or "broker_credentials_or_auth_failed"),
                        latency_ms=float(dur_ms),
                        extra={
                            "broker": str(name),
                            "attempts": int(len(attempts)),
                            "order_count": int(lineage.get("order_count") or 0),
                            "symbols": list(lineage.get("symbols") or []),
                            "execution_targets": list(lineage.get("execution_targets") or []),
                            "stop_failover": True,
                        },
                    )
                    terminal_res["failover_attempts"] = attempts
                    return terminal_res
                _warn_nonfatal(
                    "BROKER_ROUTER_BROKER_ATTEMPT_FAILED",
                    e,
                    once_key=f"broker_attempt:{name}",
                    broker=str(name),
                )
                dur_ms = int((time.time() - t0) * 1000)
                attempts.append(
                    {
                        "broker": name,
                        "attempt": int(attempt_num),
                        "ok": False,
                        "status": "exception",
                        "error": str(e),
                        "dur_ms": int(dur_ms),
                    }
                )
                retryable = attempt_num < int(BROKER_ROUTER_RETRY_ATTEMPTS)
                if retryable:
                    emit_counter(
                        "retry_attempt",
                        1,
                        component="engine.execution.broker_router",
                        broker=str(name),
                        extra_tags={
                            "operation": "order_router",
                            "attempt": int(attempt_num),
                            "status": "exception",
                        },
                    )
                    log_event(
                        LOG,
                        logging.WARNING,
                        "order_router_retry_scheduled",
                        component="engine.execution.broker_router",
                        extra={
                            "broker": str(name),
                            "attempt": int(attempt_num),
                            "status": "exception",
                            "error": str(e),
                            "latency_ms": int(dur_ms),
                            "lineage": lineage,
                        },
                    )
                    retry_delay_s = backoff_delay_s(
                        int(attempt_num),
                        base_s=float(BROKER_ROUTER_RETRY_BASE_S),
                        max_s=float(BROKER_ROUTER_RETRY_MAX_S),
                    )
                    if float(retry_delay_s) > 0.0:
                        retry_wait_ok, retry_wait_reason, retry_wait_meta = wait_with_kill_interrupt(
                            delay_s=float(retry_delay_s),
                            symbol=str((lineage.get("symbols") or [""])[0] or ""),
                            broker=str(name),
                            component="engine.execution.broker_router",
                            stage="broker_retry_backoff",
                        )
                        if not bool(retry_wait_ok):
                            return {
                                "ok": False,
                                "status": "blocked_kill_switch_during_retry_backoff",
                                "broker": str(name),
                                "reason": str(retry_wait_reason or "kill_switch_block"),
                                "kill_meta": dict(retry_wait_meta or {}),
                                "failover_attempts": attempts,
                            }
                    continue
                break

    emit_counter(
        "job_failure",
        1,
        component="engine.execution.broker_router",
        job="order_router",
        extra_tags={"failure_type": "all_brokers_failed"},
    )
    record_rolling_rate(
        "execution_success_rate",
        success=False,
        component="engine.execution.broker_router",
        extra_tags={"broker": "failover_chain"},
    )
    record_component_health(
        "execution",
        ok=False,
        status="all_brokers_failed",
        detail="execution_failover_exhausted",
        extra={
            "broker": "failover_chain",
            "attempts": int(len(attempts)),
            "order_count": int(lineage.get("order_count") or 0),
            "symbols": list(lineage.get("symbols") or []),
            "execution_targets": list(lineage.get("execution_targets") or []),
        },
    )
    trace_event(
        "order_router",
        component="engine.execution.broker_router",
        entity_type="broker",
        entity_id="failover_chain",
        payload={
            "ok": False,
            "status": "all_brokers_failed",
            "attempts": attempts,
            "lineage": lineage,
            "override_order_id": (int(override_order_id) if override_order_id is not None else None),
            "override_ts_ms": (int(override_ts_ms) if override_ts_ms is not None else None),
        },
        job="order_router",
    )
    log_event(
        LOG,
        40,
        "order_router_failed",
        component="engine.execution.broker_router",
        extra={
            "status": "all_brokers_failed",
            "attempts": attempts,
            "lineage": lineage,
            "override_order_id": (int(override_order_id) if override_order_id is not None else None),
            "override_ts_ms": (int(override_ts_ms) if override_ts_ms is not None else None),
        },
    )
    return {"ok": False, "status": "all_brokers_failed", "failover_attempts": attempts}


def submit_order(order: dict, *, dry_run: bool = False, broker: Optional[str] = None) -> Dict[str, Any]:
    """Compatibility wrapper for single-order submission through the router."""
    previous = None
    if broker:
        previous = os.environ.get("BROKER_FAILOVER")
        os.environ["BROKER_FAILOVER"] = str(broker)
    try:
        return apply_new_portfolio_orders_router(
            dry_run=bool(dry_run),
            override_orders=[dict(order or {})],
            override_order_id=None,
            override_ts_ms=int(time.time() * 1000),
        )
    finally:
        if broker:
            if previous is None:
                os.environ.pop("BROKER_FAILOVER", None)
            else:
                os.environ["BROKER_FAILOVER"] = previous
