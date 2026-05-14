"""
FILE: broker_apply_orders.py

Execution subsystem module for `broker_apply_orders`.

This is the main live order-application path. It loads the latest execution
intents, applies safety gates and execution shaping, routes approved orders to
the configured broker path, and records audit/telemetry side effects.
"""

# broker_apply_orders.py
"""
Unified broker_apply_orders

Preserves:
- Job lock enforcement
- Kill switch
- Execution mode enforcement
- Execution Policy Engine shaping (EPE)
- Dual IBKR execution (optional)
- Shadow logging
- Execution meta tracking

Adds:
- Reads latest row-per-order portfolio_orders batch (no orders_json dependency) when available
- Hard TTL enforcement via EPE (fail-closed, when supported by EPE)
- ALE registration on-demand (via EPE, when supported by EPE)
"""

import json
import logging
import os
import re
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db, acquire_job_lock, release_job_lock, touch_job_lock, run_write_txn
from engine.runtime.event_log import append_event, record_execution_block, record_order_decision
from engine.runtime.logging import get_logger, log_event
from engine.runtime.metrics import emit_counter, emit_snapshot, emit_timing
from engine.runtime.tracing import trace_event
from engine.execution.kill_switch import execution_allowed
from engine.execution.position_reconcile import pre_live_position_reconcile
from engine.strategy.portfolio_risk_gate import apply_execution_risk_governor
from engine.runtime.risk_state import get_state
from engine.strategy.rules_engine import evaluate_rules
from engine.strategy.champion_manager import get_competition_policy_for_intent
from engine.cache.wrappers.execution_mode import read_execution_mode as get_execution_mode
from engine.execution.broker_router import apply_new_portfolio_orders_router as apply_new_portfolio_orders
from engine.execution.broker_sim import apply_new_portfolio_orders as apply_shadow_portfolio_orders
from engine.execution.execution_quality_supervisor import refresh_execution_quality_supervisor
from engine.execution.execution_broker_watchdog import refresh_broker_connection_health
from engine.cache.wrappers.kill_switch import read_kill_switch as kill_switch_snapshot

# ------------------------------------------------------------
# HARD EXECUTION BARRIER
# ------------------------------------------------------------
from engine.runtime.gates import execution_gate_snapshot
from engine.execution.order_command_boundary import (
    record_order_command as record_execution_command,
    record_order_event as record_execution_event,
)


def _execution_gate_snapshot() -> Dict[str, Any]:
    return execution_gate_snapshot(get_execution_mode_fn=get_execution_mode)

# Newer path (preferred)
try:
    from engine.strategy.portfolio_execution_intents import load_latest_execution_intents  # type: ignore
except Exception:
    load_latest_execution_intents = None  # type: ignore

# EPE import
try:
    from engine.execution.execution_policy_engine import apply_execution_policy  # type: ignore
except Exception as e:
    raise RuntimeError(f"apply_execution_policy import failed: {e}")

try:
    from engine.execution.execution_ai_advisor import persist_execution_advisories  # type: ignore
except Exception:
    persist_execution_advisories = None  # type: ignore

# Optional dual execution (IBKR)
try:
    from engine.execution.dual_execution import apply_portfolio_orders_dual_ibkr  # type: ignore
except Exception:
    apply_portfolio_orders_dual_ibkr = None  # type: ignore


JOB_NAME = "broker_apply_orders"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "120"))
BROKER_NAME = os.environ.get("BROKER_NAME", "sim")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper().strip()
LOG = get_logger("execution.broker_apply_orders")
_WARNED_NONFATAL_KEYS: set[str] = set()
_NON_PRODUCTION_MODEL_TOKENS = frozenset(
    {
        "rl",
        "reinforcement",
        "llm",
        "gpt",
        "openai",
        "advisor",
        "advisory",
        "operatorai",
        "operator_ai",
    }
)


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.broker_apply_orders",
        extra={"job": JOB_NAME, "broker": str(BROKER_NAME), **(extra or {})},
        include_health=False,
        persist=False,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_APPLY_ORDERS_SAFE_INT_FAILED",
            e,
            once_key="safe_int",
            value=repr(value)[:120],
        )
        return int(default)


def _latency_summary(values: List[int]) -> Dict[str, float]:
    cleaned = sorted(int(max(0, int(v))) for v in values if v is not None)
    if not cleaned:
        return {}
    count = len(cleaned)
    p95_index = min(count - 1, max(0, int(round((count - 1) * 0.95))))
    return {
        "count": float(count),
        "avg_ms": float(round(sum(cleaned) / count, 3)),
        "p95_ms": float(cleaned[p95_index]),
        "max_ms": float(cleaned[-1]),
    }


def _decision_ts_ms(order: Dict[str, Any]) -> int:
    if not isinstance(order, dict):
        return 0
    for key in ("decision_ts_ms", "prediction_ts_ms", "signal_ts_ms", "ts_ms"):
        if order.get(key) is not None:
            return _safe_int(order.get(key), 0)
    explain = order.get("explain")
    if isinstance(explain, dict):
        pipeline_timing = explain.get("pipeline_timing")
        if isinstance(pipeline_timing, dict):
            for key in ("decision_ts_ms", "prediction_ts_ms", "db_observed_ts_ms", "source_event_ts_ms"):
                if pipeline_timing.get(key) is not None:
                    return _safe_int(pipeline_timing.get(key), 0)
    return 0


def _emit_decision_execution_snapshot(
    orders: List[dict],
    *,
    mode: str,
    payload_source: str,
    broker: str,
) -> None:
    now_ms = _now_ms()
    latencies: List[int] = []
    for order in list(orders or []):
        ts_ms = _decision_ts_ms(order)
        if ts_ms <= 0:
            continue
        latencies.append(int(max(0, now_ms - ts_ms)))

    summary = _latency_summary(latencies)
    if not summary:
        return

    emit_snapshot(
        {
            "pipeline_latency.decision_to_execution.count": float(summary["count"]),
            "pipeline_latency.decision_to_execution.avg_ms": float(summary["avg_ms"]),
            "pipeline_latency.decision_to_execution.p95_ms": float(summary["p95_ms"]),
            "pipeline_latency.decision_to_execution.max_ms": float(summary["max_ms"]),
        },
        tags={
            "component": "engine.execution.broker_apply_orders",
            "job": JOB_NAME,
            "broker": str(broker),
            "mode": str(mode),
            "payload_source": str(payload_source),
        },
    )


def _print(out: Dict[str, Any]) -> None:
    try:
        sys.stdout.write(json.dumps(out, separators=(",", ":"), sort_keys=True) + "\n")
        sys.stdout.flush()
    except Exception:
        print(out)


def _summarize_order_lineage(orders: Optional[List[dict]]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "order_count": 0,
        "execution_targets": [],
        "symbols": [],
        "model_ids": [],
        "model_names": [],
        "client_order_ids": [],
        "broker_order_ids": [],
        "source_alert_ids": [],
        "execution_modes": [],
        "batch_ids": [],
    }
    if not orders:
        return summary

    targets = set()
    symbols = set()
    model_ids = set()
    model_names = set()
    client_order_ids = set()
    broker_order_ids = set()
    source_alert_ids = set()
    execution_modes = set()
    batch_ids = set()

    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        summary["order_count"] += 1
        targets.add(str(order.get("execution_target") or "real").strip().lower() or "real")
        if order.get("symbol") not in (None, ""):
            symbols.add(str(order.get("symbol")).strip().upper())
        if order.get("model_id") not in (None, ""):
            model_ids.add(str(order.get("model_id")).strip() or "baseline")
        if order.get("model_name") not in (None, ""):
            model_names.add(str(order.get("model_name")).strip())
        elif order.get("strategy_name") not in (None, ""):
            model_names.add(str(order.get("strategy_name")).strip())
        if order.get("client_order_id") not in (None, ""):
            client_order_ids.add(str(order.get("client_order_id")).strip())
        if order.get("broker_order_id") not in (None, ""):
            broker_order_ids.add(str(order.get("broker_order_id")).strip())
        if order.get("source_alert_id") is not None:
            try:
                source_alert_ids.add(_safe_int(order.get("source_alert_id")))
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_SOURCE_ALERT_ID_PARSE_FAILED", e, once_key="source_alert_id_parse")
        if order.get("execution_mode") not in (None, ""):
            execution_modes.add(str(order.get("execution_mode")).strip())
        if order.get("batch_id") is not None:
            try:
                batch_ids.add(_safe_int(order.get("batch_id")))
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_BATCH_ID_PARSE_FAILED", e, once_key="batch_id_parse")

    summary["execution_targets"] = sorted(x for x in targets if x)
    summary["symbols"] = sorted(x for x in symbols if x)
    summary["model_ids"] = sorted(x for x in model_ids if x)
    summary["model_names"] = sorted(x for x in model_names if x)
    summary["client_order_ids"] = sorted(x for x in client_order_ids if x)
    summary["broker_order_ids"] = sorted(x for x in broker_order_ids if x)
    summary["source_alert_ids"] = sorted(source_alert_ids)
    summary["execution_modes"] = sorted(x for x in execution_modes if x)
    summary["batch_ids"] = sorted(batch_ids)
    return summary


def _execution_boundary_correlation_id(
    batch_id: Optional[int],
    payload_ts_ms: Optional[int],
    fallback_ts_ms: Optional[int] = None,
) -> str:
    if batch_id is not None:
        return str(int(batch_id))
    if payload_ts_ms is not None:
        return str(int(payload_ts_ms))
    if fallback_ts_ms is not None:
        return str(int(fallback_ts_ms))
    return str(int(_now_ms()))


def _record_execution_command_boundary(
    *,
    ts_ms: int,
    batch_id: Optional[int],
    payload_ts_ms: Optional[int],
    payload_source: str,
    mode: str,
    broker: str,
    raw_payload: List[dict],
    shaped_payload: List[dict],
    real_payload: List[dict],
    shadow_payload_by_model: Dict[str, List[dict]],
    blocked_orders: Optional[List[dict]] = None,
) -> Optional[str]:
    shadow_payload = {
        str(model_id): list(orders or [])
        for model_id, orders in dict(shadow_payload_by_model or {}).items()
        if orders
    }
    blocked_list = list(blocked_orders or [])
    correlation_id = _execution_boundary_correlation_id(
        batch_id,
        payload_ts_ms,
        ts_ms,
    )
    try:
        return record_execution_command(
            ts_ms=int(ts_ms),
            batch_id=(int(batch_id) if batch_id is not None else None),
            payload_ts_ms=(int(payload_ts_ms) if payload_ts_ms is not None else None),
            correlation_id=str(correlation_id),
            mode=str(mode),
            broker=str(broker),
            payload_source=str(payload_source),
            real_order_count=int(len(real_payload or [])),
            shadow_order_count=int(sum(len(v) for v in shadow_payload.values())),
            blocked_order_count=int(len(blocked_list)),
            payload={
                "ts_ms": int(ts_ms),
                "batch_id": (int(batch_id) if batch_id is not None else None),
                "payload_ts_ms": (int(payload_ts_ms) if payload_ts_ms is not None else None),
                "payload_source": str(payload_source),
                "mode": str(mode),
                "broker": str(broker),
                "raw_count": int(len(raw_payload or [])),
                "shaped_count": int(len(shaped_payload or [])),
                "real_count": int(len(real_payload or [])),
                "shadow_count": int(sum(len(v) for v in shadow_payload.values())),
                "blocked_count": int(len(blocked_list)),
                "raw_payload": list(raw_payload or []),
                "shaped_payload": list(shaped_payload or []),
                "real_payload": list(real_payload or []),
                "shadow_payload_by_model": shadow_payload,
                "blocked_orders": blocked_list,
            },
        )
    except Exception as e:
        _warn_nonfatal(
            "BROKER_APPLY_ORDERS_COMMAND_BOUNDARY_WRITE_FAILED",
            e,
            once_key=f"command_boundary:{mode}",
            mode=str(mode),
            broker=str(broker),
        )
        return None


def _record_execution_event_boundary(
    *,
    event_type: str,
    status: str,
    mode: str,
    broker: str,
    payload: Optional[Dict[str, Any]],
    ts_ms: Optional[int] = None,
    command_id: Optional[str] = None,
    batch_id: Optional[int] = None,
    payload_ts_ms: Optional[int] = None,
    correlation_id: Optional[str] = None,
) -> None:
    event_ts_ms = int(ts_ms if ts_ms is not None else _now_ms())
    resolved_correlation_id = (
        str(correlation_id)
        if correlation_id is not None
        else _execution_boundary_correlation_id(batch_id, payload_ts_ms, event_ts_ms)
    )
    payload_norm = {"ts_ms": int(event_ts_ms), **dict(payload or {})}
    try:
        record_execution_event(
            ts_ms=int(event_ts_ms),
            event_type=str(event_type),
            mode=str(mode),
            broker=str(broker),
            status=str(status),
            payload=payload_norm,
            command_id=(str(command_id) if command_id else None),
            batch_id=(int(batch_id) if batch_id is not None else None),
            correlation_id=str(resolved_correlation_id),
        )
    except Exception as e:
        _warn_nonfatal(
            "BROKER_APPLY_ORDERS_EVENT_BOUNDARY_WRITE_FAILED",
            e,
            once_key=f"event_boundary:{event_type}:{mode}:{status}",
            event_type=str(event_type),
            mode=str(mode),
            status=str(status),
            broker=str(broker),
        )


def _paper_sim_result_issue(result: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(result, dict):
        return {
            "reason": "paper_broker_invalid_result",
            "result_status": "invalid",
            "result_broker": "",
        }
    broker = str(result.get("broker") or "").strip().lower()
    status = str(result.get("status") or "")
    if not broker:
        return {
            "reason": "paper_broker_missing_broker",
            "result_status": status,
            "result_broker": "",
        }
    if broker != "sim":
        return {
            "reason": "paper_broker_unexpected_broker",
            "result_status": status,
            "result_broker": broker,
        }
    return None


def _blocked(
    *,
    started_ms: int,
    layer: str,
    mode: str,
    broker: str,
    reason: str,
    payload: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
    command_id: Optional[str] = None,
) -> int:
    now_ms = _now_ms()
    payload_norm = dict(payload or {})
    lineage = _summarize_order_lineage(
        list(payload_norm.get("orders") or [])
        if isinstance(payload_norm.get("orders"), list)
        else []
    )
    blocked_lineage = _summarize_order_lineage(
        list(payload_norm.get("blocked_orders") or [])
        if isinstance(payload_norm.get("blocked_orders"), list)
        else []
    )
    if any(lineage.get(key) for key in ("execution_targets", "symbols", "model_ids", "model_names", "client_order_ids", "broker_order_ids", "source_alert_ids", "execution_modes", "batch_ids")):
        payload_norm["lineage"] = lineage
    if any(blocked_lineage.get(key) for key in ("execution_targets", "symbols", "model_ids", "model_names", "client_order_ids", "broker_order_ids", "source_alert_ids", "execution_modes", "batch_ids")):
        payload_norm["blocked_lineage"] = blocked_lineage
    out = {
        "status": "blocked",
        "layer": str(layer),
        "mode": str(mode),
        "broker": str(broker),
        "reason": str(reason),
        "ts_ms": int(now_ms),
        "dur_ms": int(now_ms - started_ms),
    }
    if payload_norm:
        out.update(payload_norm)

    # Blocked execution attempts still emit telemetry/audit data so operators
    # can tell the difference between "no intents" and "explicitly denied".
    emit_counter(
        "job_health",
        1,
        component="engine.execution.broker_apply_orders",
        job=JOB_NAME,
        broker=str(broker),
        extra_tags={"metric_scope": "execution_blocked"},
    )

    trace_event(
        "risk_validation",
        component="engine.execution.broker_apply_orders",
        entity_type="execution_block",
        entity_id=str(layer),
        payload={"mode": str(mode), "broker": str(broker), "reason": str(reason), **payload_norm},
        job=JOB_NAME,
        broker=str(broker),
    )

    try:
        record_execution_block(
            ts_ms=int(now_ms),
            layer=str(layer),
            reason=str(reason),
            mode=str(mode),
            broker=str(broker),
            payload=payload_norm,
            correlation_id=(str(correlation_id) if correlation_id is not None else None),
        )
    except Exception as e:
        _warn_nonfatal("BROKER_APPLY_ORDERS_RECORD_BLOCK_FAILED", e, once_key=f"record_block:{layer}", layer=str(layer), mode=str(mode), reason=str(reason))

    _record_execution_event_boundary(
        event_type="execution_block",
        status="blocked",
        mode=str(mode),
        broker=str(broker),
        payload={
            "layer": str(layer),
            "reason": str(reason),
            **payload_norm,
        },
        ts_ms=int(now_ms),
        command_id=(str(command_id) if command_id else None),
        correlation_id=(str(correlation_id) if correlation_id is not None else None),
    )

    _print(out)
    return 0


def _ensure_shadow_table(con) -> None:
    # Create a superset schema to maximize compatibility.
    # NOTE: CREATE TABLE IF NOT EXISTS does not alter existing tables.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS shadow_order_intents (
          ts_ms INTEGER NOT NULL,
          actor TEXT NOT NULL,
          broker TEXT NOT NULL,
          orders_json TEXT,
          intents_json TEXT,
          mode_json TEXT NOT NULL
        )
        """
    )


def _log_shadow_intents(payload: List[Dict[str, Any]], actor: str, mode_state: dict) -> None:
    # Shadow intent persistence is best-effort compatibility plumbing for audit
    # and UI visibility; live execution should not depend on this write.
    try:
        con = connect()
        try:
            _ensure_shadow_table(con)

            payload_json = json.dumps(payload or [], separators=(",", ":"), sort_keys=True)
            mode_json = json.dumps(mode_state or {}, separators=(",", ":"), sort_keys=True)

            try:
                con.execute(
                    """
                    INSERT INTO shadow_order_intents(
                      ts_ms, actor, broker, intents_json, mode_json
                    )
                    VALUES (?,?,?,?,?)
                    """,
                    (
                        _now_ms(),
                        str(actor),
                        str(BROKER_NAME),
                        payload_json,
                        mode_json,
                    ),
                )
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_SHADOW_INTENT_WRITE_FAILED", e, once_key="shadow_intents_primary", actor=str(actor), schema="intents_json")
                try:
                    con.execute(
                        """
                        INSERT INTO shadow_order_intents(
                          ts_ms, actor, broker, orders_json, mode_json
                        )
                        VALUES (?,?,?,?,?)
                        """,
                        (
                            _now_ms(),
                            str(actor),
                            str(BROKER_NAME),
                            payload_json,
                            mode_json,
                        ),
                    )
                except Exception as fallback_err:
                    _warn_nonfatal("BROKER_APPLY_ORDERS_SHADOW_INTENT_WRITE_FAILED", fallback_err, once_key="shadow_intents_legacy", actor=str(actor), schema="orders_json")
                    con.execute(
                        """
                        INSERT INTO shadow_order_intents(
                          ts_ms, actor, broker, mode_json
                        )
                        VALUES (?,?,?,?)
                        """,
                        (
                            _now_ms(),
                            str(actor),
                            str(BROKER_NAME),
                            mode_json,
                        ),
                    )

            con.commit()
        finally:
            con.close()
    except Exception as e:
        _warn_nonfatal("BROKER_APPLY_ORDERS_SHADOW_INTENT_LOG_FAILED", e, once_key="shadow_intents_log", actor=str(actor))


def _write_execution_meta_last(broker: str, source: str) -> None:
    try:
        # execution_meta is a lightweight breadcrumb store for the last source
        # that drove execution without forcing readers to scan ledgers.
        con = connect()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_meta (
                  key TEXT PRIMARY KEY,
                  value TEXT NOT NULL
                )
                """
            )
            con.execute(
                """
                INSERT INTO execution_meta(key, value)
                VALUES(?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                ("last_execution_source", str(source)),
            )
            con.execute(
                """
                INSERT INTO execution_meta(key, value)
                VALUES(?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                ("last_execution_broker", str(broker)),
            )
            con.commit()
        finally:
            con.close()
    except Exception as e:
        _warn_nonfatal("BROKER_APPLY_ORDERS_EXECUTION_META_WRITE_FAILED", e, once_key="execution_meta_last", source=str(source), broker=str(broker))


def _extract_preview_meta(preview: dict) -> Tuple[Optional[int], Optional[int], List[dict]]:
    oid = None
    ts_ms = None
    orders: List[dict] = []

    if isinstance(preview, dict):
        for k in ("order_id", "portfolio_orders_id", "id"):
            try:
                if preview.get(k) is not None:
                    oid = _safe_int(preview.get(k))
                    break
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_PREVIEW_ID_PARSE_FAILED", e, once_key=f"preview_id:{k}", field=str(k))

        for k in ("ts_ms", "portfolio_orders_ts_ms"):
            try:
                if preview.get(k) is not None:
                    ts_ms = _safe_int(preview.get(k))
                    break
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_PREVIEW_TS_PARSE_FAILED", e, once_key=f"preview_ts:{k}", field=str(k))

        try:
            orders = list(preview.get("orders") or [])
        except Exception:
            orders = []

    return oid, ts_ms, orders


def _acquire_lock_compat() -> bool:
    # Support both signatures:
    # - acquire_job_lock(name, owner, pid, stale_after_s=...)
    # - acquire_job_lock(name, owner, pid, ttl_s=...)
    try:
        return bool(acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S))
    except TypeError as e:
        _warn_nonfatal("BROKER_APPLY_ORDERS_LOCK_SIGNATURE_FALLBACK", e, once_key="lock_signature_fallback")
        return bool(acquire_job_lock(JOB_NAME, OWNER, PID, stale_after_s=LOCK_STALE_AFTER_S))


def _touch_lock_compat() -> None:
    try:
        touch_job_lock(JOB_NAME, OWNER, PID)
    except Exception as e:
        _warn_nonfatal("BROKER_APPLY_ORDERS_LOCK_TOUCH_FAILED", e, once_key="touch_lock")


def _mark_challenger_shadow_orders_status(row_ids: List[int], status: str) -> None:
    ids = sorted({int(x) for x in (row_ids or []) if x is not None})
    if not ids:
        return
    def _write(con) -> None:
        placeholders = ",".join("?" for _ in ids)
        con.execute(
            f"""
            UPDATE challenger_shadow_orders
            SET status=?
            WHERE id IN ({placeholders})
            """,
            [str(status or "shadow"), *ids],
        )
    try:
        run_write_txn(
            _write,
            table="challenger_shadow_orders",
            operation="mark_challenger_shadow_orders_status",
            context={"row_count": len(ids), "status": str(status or "shadow")},
        )
    except Exception as e:
        _warn_nonfatal("BROKER_APPLY_ORDERS_SHADOW_STATUS_WRITE_FAILED", e, once_key="shadow_status_write", status=str(status))


def _start_lock_heartbeat() -> threading.Event:
    stop_evt = threading.Event()
    interval_s = max(5.0, min(30.0, float(max(30, int(LOCK_STALE_AFTER_S))) / 3.0))

    def _runner() -> None:
        while not stop_evt.wait(interval_s):
            _touch_lock_compat()

    threading.Thread(
        target=_runner,
        name="broker_apply_orders_lock_heartbeat",
        daemon=True,
    ).start()
    return stop_evt


def _apply_epe_compat(
    *,
    con,
    raw_payload: List[dict],
    actor: str,
    mode: str,
    broker: str,
    portfolio_orders_id: Optional[int],
    portfolio_orders_batch_id: Optional[int],
    default_signal_ts_ms: Optional[int],
) -> List[dict]:
    # Support both EPE signatures:
    # Newer:
    #   apply_execution_policy(con=..., intents=..., actor=..., mode=..., broker=...,
    #                          portfolio_orders_batch_id=..., default_signal_ts_ms=...)
    # Older:
    #   apply_execution_policy(orders, actor=..., mode=..., broker=..., portfolio_orders_id=...,
    #                          default_signal_ts_ms=...)
    try:
        shaped = apply_execution_policy(
            con=con,
            intents=raw_payload,
            actor=str(actor),
            mode=str(mode),
            broker=str(broker),
            portfolio_orders_batch_id=(int(portfolio_orders_batch_id) if portfolio_orders_batch_id is not None else None),
            portfolio_orders_id=(int(portfolio_orders_id) if portfolio_orders_id is not None else None),
            default_signal_ts_ms=(int(default_signal_ts_ms) if default_signal_ts_ms is not None else None),
        )
        return list(shaped or [])
    except TypeError as e:
        _warn_nonfatal("BROKER_APPLY_ORDERS_EPE_SIGNATURE_FALLBACK", e, once_key="epe_signature_fallback")
        shaped = apply_execution_policy(
            raw_payload,
            actor=str(actor),
            mode=str(mode),
            broker=str(broker),
            portfolio_orders_id=(int(portfolio_orders_id) if portfolio_orders_id is not None else None),
            default_signal_ts_ms=(int(default_signal_ts_ms) if default_signal_ts_ms is not None else None),
        )
        return list(shaped or [])


def _load_latest_payload() -> Tuple[Optional[int], Optional[int], List[dict], str]:
    """
    Returns: (batch_or_order_id, ts_ms, payload_list, source)
    payload_list is either intents or orders; broker router receives as override_orders.
    """
    # Preferred: row-per-order intents table
    if callable(load_latest_execution_intents):
        try:
            con = connect()
            try:
                batch = load_latest_execution_intents(con) or {}
                batch_id = batch.get("batch_id")
                batch_ts_ms = batch.get("batch_ts_ms")
                intents = list(batch.get("intents") or [])
            finally:
                con.close()

            if (batch_id is not None) or intents:
                return (
                    (int(batch_id) if batch_id is not None else None),
                    (int(batch_ts_ms) if batch_ts_ms is not None else None),
                    intents,
                    "execution_intents",
                )
        except Exception:
            _warn_nonfatal("BROKER_APPLY_ORDERS_APPEND_EVENT_FAILED", Exception("append_event failed"), once_key="append_order_submit_result")

    # Fallback: legacy broker_router dry_run preview
    preview = apply_new_portfolio_orders(dry_run=True)
    oid, ts_ms, orders = _extract_preview_meta(preview)
    return oid, ts_ms, orders, "broker_router_preview"


def _execution_target(order: Dict[str, Any]) -> str:
    target = str((order or {}).get("execution_target") or "").strip().lower()
    if target in ("real", "shadow"):
        return target
    comp = (order or {}).get("competition") or {}
    if bool(comp.get("blocked")) and str(comp.get("reason") or "").strip() == "champion_mismatch":
        return "shadow"
    return "real"


def _split_execution_payload(orders: List[dict]) -> Tuple[List[dict], Dict[str, List[dict]]]:
    real_orders: List[dict] = []
    shadow_groups: Dict[str, List[dict]] = {}
    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        if _execution_target(order) != "shadow":
            real_orders.append(order)
            continue
        model_id = str(order.get("model_id") or "baseline").strip() or "baseline"
        shadow_groups.setdefault(model_id, []).append(order)
    return real_orders, shadow_groups


def _policy_float(policy: Optional[Dict[str, Any]], *keys: str) -> float:
    obj = dict(policy or {})
    for key in keys:
        try:
            value = obj.get(key)
            if value is None or value == "":
                continue
            return float(value)
        except Exception as e:
            _warn_nonfatal(
                "BROKER_APPLY_ORDERS_POLICY_FLOAT_PARSE_FAILED",
                e,
                once_key=f"policy_float:{key}",
                key=str(key),
            )
            continue
    return 0.0


def _contains_non_production_model_token(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if not text:
        return False
    normalized = text.replace("-", " ").replace("_", " ")
    tokens = [tok for tok in re.split(r"[^a-z0-9]+", normalized) if tok]
    return any(tok in _NON_PRODUCTION_MODEL_TOKENS for tok in tokens)


def _non_production_model_reason(order: Dict[str, Any]) -> Optional[str]:
    if not isinstance(order, dict):
        return None

    direct_fields = (
        ("model_id", order.get("model_id")),
        ("model_name", order.get("model_name")),
        ("model_kind", order.get("model_kind")),
        ("strategy_name", order.get("strategy_name")),
    )
    for field_name, value in direct_fields:
        if _contains_non_production_model_token(value):
            return f"{field_name}:{value}"

    explain = order.get("explain")
    if isinstance(explain, dict):
        strategy_meta = explain.get("strategy")
        if isinstance(strategy_meta, dict):
            if _contains_non_production_model_token(strategy_meta.get("name")):
                return f"explain.strategy.name:{strategy_meta.get('name')}"
        model_meta = explain.get("model")
        if isinstance(model_meta, dict):
            for field_name in ("model_id", "model_name", "model_kind", "name", "id", "type"):
                if _contains_non_production_model_token(model_meta.get(field_name)):
                    return f"explain.model.{field_name}:{model_meta.get(field_name)}"
        selector_meta = explain.get("selector")
        if isinstance(selector_meta, dict):
            for field_name in ("rl_choice", "llm_choice", "advisor_choice"):
                if selector_meta.get(field_name) not in (None, "", "multi_strategy", "baseline", "conservative"):
                    return f"explain.selector.{field_name}:{selector_meta.get(field_name)}"
            if any(key in selector_meta for key in ("rl_choice", "rl_score", "llm_choice", "advisor_choice")):
                return "explain.selector:non_production_selector_metadata"

    return None


def _enforce_production_model_sources(
    orders: List[dict],
) -> Tuple[List[dict], List[Dict[str, Any]]]:
    allowed_orders: List[dict] = []
    blocked_orders: List[Dict[str, Any]] = []
    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        reason = _non_production_model_reason(order)
        if not reason:
            allowed_orders.append(order)
            continue
        blocked_orders.append(
            {
                "symbol": str(order.get("symbol") or "").strip().upper() or None,
                "model_id": str(order.get("model_id") or "baseline").strip() or "baseline",
                "model_name": str(order.get("model_name") or order.get("strategy_name") or "").strip() or None,
                "reason": "non_production_model_blocked",
                "meta": {"source": str(reason)},
            }
        )
    return allowed_orders, blocked_orders


def _pre_submit_revalidate_competition(
    orders: List[dict],
) -> Tuple[List[dict], List[Dict[str, Any]]]:
    updated_orders: List[dict] = []
    rerouted_orders: List[Dict[str, Any]] = []

    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        if _execution_target(order) != "real":
            updated_orders.append(order)
            continue

        symbol = str(order.get("symbol") or "").strip().upper()
        horizon_s = int(order.get("horizon_s") or 0)
        model_name = str(order.get("model_name") or order.get("strategy_name") or "").strip()
        regime = str(order.get("regime") or order.get("market_regime") or "global").strip() or "global"
        previous_policy = dict(order.get("competition") or {})
        current_policy = get_competition_policy_for_intent(
            symbol=symbol,
            horizon_s=int(horizon_s),
            model_name=str(model_name),
            regime=str(regime),
        )

        reasons: List[str] = []
        if not model_name:
            reasons.append("model_identity_missing")
        if bool((current_policy or {}).get("blocked")):
            reasons.append(str((current_policy or {}).get("reason") or "competition_blocked"))

        prev_champion = str(previous_policy.get("champion_model_name") or "").strip()
        curr_champion = str((current_policy or {}).get("champion_model_name") or "").strip()
        if (prev_champion or curr_champion) and prev_champion != curr_champion:
            reasons.append("champion_mismatch")

        prev_alloc = _policy_float(previous_policy, "allocation_fraction", "model_weight", "capital_multiplier")
        curr_alloc = _policy_float(current_policy, "allocation_fraction", "model_weight", "capital_multiplier")
        if abs(float(prev_alloc) - float(curr_alloc)) > 1e-9:
            reasons.append("allocation_changed")

        prev_group_budget = _policy_float(previous_policy, "group_budget_fraction")
        curr_group_budget = _policy_float(current_policy, "group_budget_fraction")
        if abs(float(prev_group_budget) - float(curr_group_budget)) > 1e-9:
            reasons.append("group_budget_changed")

        prev_model_budget = _policy_float(previous_policy, "model_budget_fraction")
        curr_model_budget = _policy_float(current_policy, "model_budget_fraction")
        if abs(float(prev_model_budget) - float(curr_model_budget)) > 1e-9:
            reasons.append("model_budget_changed")

        prev_risk_limit = _policy_float(previous_policy, "risk_limit_multiplier")
        curr_risk_limit = _policy_float(current_policy, "risk_limit_multiplier")
        if abs(float(prev_risk_limit) - float(curr_risk_limit)) > 1e-9:
            reasons.append("risk_limit_changed")

        if reasons:
            rerouted_order = dict(order)
            rerouted_order["execution_target"] = "shadow"
            rerouted_order["competition"] = {
                **dict(current_policy or {}),
                "allowed": False,
                "blocked": True,
                "reason": str(reasons[0]),
            }
            rerouted_order["competition_pre_submit_reasons"] = list(dict.fromkeys(reasons))
            updated_orders.append(rerouted_order)
            rerouted_orders.append(
                {
                    "symbol": str(symbol),
                    "model_id": str(order.get("model_id") or "baseline").strip() or "baseline",
                    "model_name": str(model_name),
                    "reason": str(reasons[0]),
                    "reasons": list(dict.fromkeys(reasons)),
                    "previous_competition": dict(previous_policy or {}),
                    "current_competition": dict(current_policy or {}),
                }
            )
            continue

        refreshed_order = dict(order)
        refreshed_order["competition"] = dict(current_policy or previous_policy or {})
        updated_orders.append(refreshed_order)

    return updated_orders, rerouted_orders


def _apply_model_kill_switch(
    *,
    con,
    orders: List[dict],
) -> Tuple[List[dict], List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    allowed_orders: List[dict] = []
    blocked_orders: List[Dict[str, Any]] = []

    for order in list(orders or []):
        if not isinstance(order, dict):
            continue
        symbol = str(order.get("symbol") or "").strip().upper() or None
        regime = str(order.get("regime") or order.get("market_regime") or "global").strip() or None
        model_id = str(order.get("model_id") or "baseline").strip() or "baseline"

        allow, reason, meta = execution_allowed(
            con=con,
            symbol=symbol,
            regime=regime,
            model_id=model_id,
        )
        if allow:
            allowed_orders.append(order)
            continue

        scope = str((meta or {}).get("scope") or "").strip().lower()
        blocked_orders.append(
            {
                "symbol": symbol,
                "model_id": model_id,
                "reason": str(reason or "kill_switch_block"),
                "meta": dict(meta or {}),
            }
        )
        if scope == "global":
            return [], blocked_orders, {"reason": str(reason or "kill_switch_block"), "meta": dict(meta or {})}

    return allowed_orders, blocked_orders, None


def _execute_shadow_groups(
    *,
    shadow_groups: Dict[str, List[dict]],
    batch_or_oid: Optional[int],
    payload_ts_ms: Optional[int],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": True, "groups": [], "submitted_models": 0, "fills_written": 0}
    for model_id, orders in sorted((shadow_groups or {}).items()):
        if not orders:
            continue
        row_ids = []
        for order in list(orders or []):
            try:
                if order.get("shadow_order_row_id") is not None:
                    row_ids.append(_safe_int(order.get("shadow_order_row_id")))
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_SHADOW_ROW_ID_PARSE_FAILED", e, once_key="shadow_row_id_parse")
                continue
        res = apply_shadow_portfolio_orders(
            dry_run=False,
            override_orders=list(orders),
            override_order_id=(int(batch_or_oid) if batch_or_oid is not None else None),
            override_ts_ms=(int(payload_ts_ms) if payload_ts_ms is not None else None),
            book_key=f"shadow:{str(model_id)}",
        ) or {}
        if bool((res or {}).get("ok", False)):
            _mark_challenger_shadow_orders_status(row_ids, "executed")
        out["groups"].append({"model_id": str(model_id), "result": dict(res or {})})
        out["submitted_models"] += 1
        out["fills_written"] += int((res or {}).get("fills_written") or 0)
        if not bool((res or {}).get("ok", True)):
            out["ok"] = False
    return out


def main() -> int:
    init_db()

    if not _acquire_lock_compat():
        _print({"status": "locked_out", "job": JOB_NAME})
        return 0
    lock_heartbeat_stop = _start_lock_heartbeat()

    started_ms = _now_ms()
    batch_or_oid: Optional[int] = None
    payload_ts_ms: Optional[int] = None
    payload_source = "uninitialized"
    raw_payload: List[dict] = []
    shaped_payload: List[dict] = []
    real_payload: List[dict] = []
    shadow_payload_by_model: Dict[str, List[dict]] = {}
    command_id: Optional[str] = None

# ------------------------------------------------------------
# Signal freshness is enforced later using payload_ts_ms
# from the loaded execution payload.
# ------------------------------------------------------------

    try:
        con = connect()
        try:
            allow, ks_reason, ks_meta = execution_allowed(con=con, symbol=None, regime=None)
        finally:
            con.close()

        if not allow:
            blocked_ts_ms = _now_ms()
            _record_execution_event_boundary(
                event_type="execution_block",
                status="blocked",
                mode=str(get_execution_mode().get("mode") if isinstance(get_execution_mode(), dict) else "unknown"),
                broker=str(BROKER_NAME),
                payload={
                    "layer": "kill_switch",
                    "reason": str(ks_reason),
                    "meta": dict(ks_meta or {}),
                    "dur_ms": int(blocked_ts_ms - started_ms),
                },
                ts_ms=int(blocked_ts_ms),
            )
            _print(
                {
                    "status": "blocked",
                    "layer": "kill_switch",
                    "reason": ks_reason,
                    "meta": ks_meta,
                    "ts_ms": _now_ms(),
                    "dur_ms": _now_ms() - started_ms,
                }
            )
            return 0

        # Best-effort rules eval (never blocks)
        try:
            evaluate_rules()
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_RULES_EVALUATION_FAILED", e, once_key="evaluate_rules")

        # Portfolio Risk Engine hard-block (fail-closed)
        try:
            if str(get_state("portfolio_risk_block", "0") or "0").strip() == "1":
                details = str(get_state("portfolio_risk_info", "") or "")
                blocked_ts_ms = _now_ms()
                _record_execution_event_boundary(
                    event_type="execution_block",
                    status="blocked",
                    mode=str(get_execution_mode().get("mode") if isinstance(get_execution_mode(), dict) else "unknown"),
                    broker=str(BROKER_NAME),
                    payload={
                        "layer": "portfolio_risk_engine",
                        "reason": "portfolio_risk_block",
                        "portfolio_risk_info": details,
                        "dur_ms": int(blocked_ts_ms - started_ms),
                    },
                    ts_ms=int(blocked_ts_ms),
                )
                _print(
                    {
                        "status": "blocked",
                        "layer": "portfolio_risk_engine",
                        "reason": "portfolio_risk_block",
                        "portfolio_risk_info": details,
                        "ts_ms": _now_ms(),
                        "dur_ms": _now_ms() - started_ms,
                    }
                )
                return 0
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_PORTFOLIO_RISK_EXCEPTION", e, once_key="portfolio_risk_exception")
            blocked_ts_ms = _now_ms()
            _record_execution_event_boundary(
                event_type="execution_block",
                status="blocked",
                mode=str(get_execution_mode().get("mode") if isinstance(get_execution_mode(), dict) else "unknown"),
                broker=str(BROKER_NAME),
                payload={
                    "layer": "portfolio_risk_engine_exception",
                    "reason": str(e),
                    "dur_ms": int(blocked_ts_ms - started_ms),
                },
                ts_ms=int(blocked_ts_ms),
            )
            _print(
                {
                    "status": "blocked",
                    "layer": "portfolio_risk_engine_exception",
                    "reason": str(e),
                    "ts_ms": _now_ms(),
                    "dur_ms": _now_ms() - started_ms,
                }
            )
            return 0

        mode_state = get_execution_mode() or {}
        gate = execution_gate_snapshot(
            get_execution_mode_fn=get_execution_mode,
            kill_switches=(kill_switch_snapshot() or {}),
        )
        if (not bool(gate.get("ok"))) or (not bool(gate.get("allow_execution_pipeline"))):
            return _blocked(
                started_ms=int(started_ms),
                layer="execution_gate",
                mode=str(gate.get("mode") or mode_state.get("mode") or "unknown"),
                broker=str(BROKER_NAME),
                reason=str(gate.get("reason") or "execution_gate_blocked"),
                payload={"gate": gate},
            )

        mode = str(gate.get("mode") or mode_state.get("mode") or "").lower().strip()

        # Load latest payload
        batch_or_oid, payload_ts_ms, raw_payload, payload_source = _load_latest_payload()

        # Shape via EPE (TTL hard wall / ALE registration when supported)
        con = connect()
        try:
            shaped_payload = _apply_epe_compat(
                con=con,
                raw_payload=raw_payload,
                actor=str(OWNER),
                mode=str(mode),
                broker=str(BROKER_NAME),
                portfolio_orders_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                portfolio_orders_batch_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                default_signal_ts_ms=(int(payload_ts_ms) if payload_ts_ms is not None else None),
            )

            try:
                record_order_decision(
                    ts_ms=int(_now_ms()),
                    batch_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                    payload_source=str(payload_source),
                    raw_payload=list(raw_payload or []),
                    shaped_payload=list(shaped_payload or []),
                    mode=str(mode),
                    broker=str(BROKER_NAME),
                    con=con,
                )
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_DECISION_LOG_FAILED", e, once_key="record_order_decision")

            if callable(persist_execution_advisories):
                try:
                    persist_execution_advisories(
                        shaped_payload=list(shaped_payload or []),
                        batch_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                        portfolio_orders_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                        payload_source=str(payload_source),
                        execution_mode=str(mode),
                        broker=str(BROKER_NAME),
                        ts_ms=int(_now_ms()),
                    )
                except Exception as e:
                    _warn_nonfatal("BROKER_APPLY_ORDERS_ADVISORY_PERSIST_FAILED", e, once_key="persist_execution_advisories")

            try:
                con.commit()
            except Exception as e:
                _warn_nonfatal("BROKER_APPLY_ORDERS_COMMIT_FAILED", e, once_key="epe_commit")
        finally:
            con.close()

        production_ready_payload, production_model_blocked_orders = _enforce_production_model_sources(
            shaped_payload,
        )
        if (not production_ready_payload) and production_model_blocked_orders:
            return _blocked(
                started_ms=int(started_ms),
                layer="production_model_guard",
                mode=str(mode),
                broker=str(BROKER_NAME),
                reason="all_orders_blocked_non_production_models",
                payload={"blocked_orders": list(production_model_blocked_orders or [])},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        con = connect()
        try:
            gated_payload, model_blocked_orders, global_model_block = _apply_model_kill_switch(
                con=con,
                orders=production_ready_payload,
            )
        finally:
            con.close()

        if global_model_block is not None:
            return _blocked(
                started_ms=int(started_ms),
                layer="kill_switch",
                mode=str(mode),
                broker=str(BROKER_NAME),
                reason=str((global_model_block or {}).get("reason") or "kill_switch_block"),
                payload={
                    "meta": dict((global_model_block or {}).get("meta") or {}),
                    "blocked_orders": list(model_blocked_orders or []),
                },
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        shaped_payload = list(gated_payload or [])
        real_payload, shadow_payload_by_model = _split_execution_payload(shaped_payload)

        if (not shaped_payload) and model_blocked_orders:
            return _blocked(
                started_ms=int(started_ms),
                layer="kill_switch",
                mode=str(mode),
                broker=str(BROKER_NAME),
                reason="all_orders_blocked_model_kill_switch",
                payload={"blocked_orders": list(model_blocked_orders or [])},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        if mode == "shadow":
            shadow_all = dict(shadow_payload_by_model)
            if real_payload:
                shadow_all.setdefault("champion", []).extend(list(real_payload))
            command_id = _record_execution_command_boundary(
                ts_ms=int(_now_ms()),
                batch_id=batch_or_oid,
                payload_ts_ms=payload_ts_ms,
                payload_source=str(payload_source),
                mode="shadow",
                broker=str(BROKER_NAME),
                raw_payload=list(raw_payload or []),
                shaped_payload=list(shaped_payload or []),
                real_payload=[],
                shadow_payload_by_model=shadow_all,
                blocked_orders=list(model_blocked_orders) + list(production_model_blocked_orders),
            ) if shadow_all else None
            _emit_decision_execution_snapshot(
                list(shaped_payload or []),
                mode="shadow",
                payload_source=str(payload_source),
                broker=str(BROKER_NAME),
            )
            shadow_res = _execute_shadow_groups(
                shadow_groups=shadow_all,
                batch_or_oid=batch_or_oid,
                payload_ts_ms=payload_ts_ms,
            )
            _log_shadow_intents(shaped_payload, OWNER, mode_state)
            _record_execution_event_boundary(
                event_type="command_result",
                status=("executed" if bool((shadow_res or {}).get("ok", True)) else "failed"),
                mode="shadow",
                broker=str(BROKER_NAME),
                payload={
                    "payload_source": str(payload_source),
                    "batch_id": (int(batch_or_oid) if batch_or_oid is not None else None),
                    "payload_ts_ms": (int(payload_ts_ms) if payload_ts_ms is not None else None),
                    "shadow_result": dict(shadow_res or {}),
                    "shaped_count": int(len(shaped_payload or [])),
                    "blocked_count": int(len(model_blocked_orders or [])) + int(len(production_model_blocked_orders or [])),
                    "blocked_orders": list(model_blocked_orders) + list(production_model_blocked_orders),
                },
                command_id=command_id,
                batch_id=batch_or_oid,
                payload_ts_ms=payload_ts_ms,
            )
            _print(
                {
                    "status": "ok",
                    "mode": "shadow",
                    "broker": BROKER_NAME,
                    "payload_source": payload_source,
                    "batch_id": batch_or_oid,
                    "raw_count": len(raw_payload),
                    "shaped_count": len(shaped_payload),
                    "blocked_count": len(model_blocked_orders),
                    "production_model_blocked_count": len(production_model_blocked_orders),
                    "real_count": len(real_payload),
                    "shadow_count": sum(len(v) for v in shadow_payload_by_model.values()),
                    "blocked_orders": list(model_blocked_orders) + list(production_model_blocked_orders),
                    "shadow_result": shadow_res,
                    "executed": True,
                    "ts_ms": _now_ms(),
                    "dur_ms": _now_ms() - started_ms,
                }
            )
            return 0

        if mode == "paper":
            paper_broker = "sim"
            command_id = _record_execution_command_boundary(
                ts_ms=int(_now_ms()),
                batch_id=batch_or_oid,
                payload_ts_ms=payload_ts_ms,
                payload_source=str(payload_source),
                mode="paper",
                broker=paper_broker,
                raw_payload=list(raw_payload or []),
                shaped_payload=list(shaped_payload or []),
                real_payload=list(real_payload or []),
                shadow_payload_by_model=shadow_payload_by_model,
                blocked_orders=list(model_blocked_orders) + list(production_model_blocked_orders),
            ) if (real_payload or shadow_payload_by_model) else None
            _emit_decision_execution_snapshot(
                list(real_payload or []),
                mode="paper",
                payload_source=str(payload_source),
                broker=paper_broker,
            )
            res = apply_shadow_portfolio_orders(
                dry_run=False,
                override_orders=real_payload,
                override_order_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                override_ts_ms=(int(payload_ts_ms) if payload_ts_ms is not None else None),
            ) if real_payload else {"ok": True, "status": "no_real_orders", "broker": "sim"}
            paper_result_issue = _paper_sim_result_issue(res)
            if paper_result_issue is not None:
                return _blocked(
                    started_ms=int(started_ms),
                    layer="paper_broker_result",
                    mode="paper",
                    broker=paper_broker,
                    reason=str(paper_result_issue.get("reason") or "paper_broker_result_invalid"),
                    payload={
                        "result_ok": bool((res or {}).get("ok")) if isinstance(res, dict) else False,
                        "result_status": str(paper_result_issue.get("result_status") or ""),
                        "result_broker": str(paper_result_issue.get("result_broker") or ""),
                    },
                    command_id=command_id,
                    correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
                )
            shadow_res = _execute_shadow_groups(
                shadow_groups=shadow_payload_by_model,
                batch_or_oid=batch_or_oid,
                payload_ts_ms=payload_ts_ms,
            ) if shadow_payload_by_model else {"ok": True, "groups": [], "submitted_models": 0, "fills_written": 0}
            _write_execution_meta_last(paper_broker, "paper_broker_sim")
            _record_execution_event_boundary(
                event_type="command_result",
                status=("executed" if bool((res or {}).get("ok", True)) else "failed"),
                mode="paper",
                broker=paper_broker,
                payload={
                    "payload_source": str(payload_source),
                    "batch_id": (int(batch_or_oid) if batch_or_oid is not None else None),
                    "payload_ts_ms": (int(payload_ts_ms) if payload_ts_ms is not None else None),
                    "result": dict(res or {}),
                    "shadow_result": dict(shadow_res or {}),
                    "blocked_count": int(len(model_blocked_orders or [])) + int(len(production_model_blocked_orders or [])),
                    "blocked_orders": list(model_blocked_orders) + list(production_model_blocked_orders),
                    "real_count": int(len(real_payload or [])),
                    "shadow_count": int(sum(len(v) for v in shadow_payload_by_model.values())),
                },
                command_id=command_id,
                batch_id=batch_or_oid,
                payload_ts_ms=payload_ts_ms,
            )
            _print(
                {
                    "status": "ok",
                    "mode": "paper",
                    "broker": paper_broker,
                    "payload_source": payload_source,
                    "batch_id": batch_or_oid,
                    "result": res,
                    "shadow_result": shadow_res,
                    "blocked_count": len(model_blocked_orders),
                    "production_model_blocked_count": len(production_model_blocked_orders),
                    "blocked_orders": list(model_blocked_orders) + list(production_model_blocked_orders),
                    "real_count": len(real_payload),
                    "shadow_count": sum(len(v) for v in shadow_payload_by_model.values()),
                    "ts_ms": _now_ms(),
                    "dur_ms": _now_ms() - started_ms,
                }
            )
            return 0

        if mode != "live":
            return _blocked(
                started_ms=int(started_ms),
                layer="execution_mode",
                mode=str(mode or "unknown"),
                broker=str(BROKER_NAME),
                reason="mode_not_executable",
                payload={"gate": gate},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        armed = int(mode_state.get("armed", 0)) == 1
        if not armed:
            return _blocked(
                started_ms=int(started_ms),
                layer="execution_mode",
                mode="live",
                broker=str(BROKER_NAME),
                reason="live_not_armed",
                payload={},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        if not bool(gate.get("real_trading_allowed")):
            blocked_ts_ms = _now_ms()
            _record_execution_event_boundary(
                event_type="execution_block",
                status="blocked",
                mode=str(mode or "live"),
                broker=str(BROKER_NAME),
                payload={
                    "layer": "execution_gate",
                    "reason": str(gate.get("reason") or "real_trading_not_allowed"),
                    "gate": gate,
                    "dur_ms": int(blocked_ts_ms - started_ms),
                },
                ts_ms=int(blocked_ts_ms),
                batch_id=batch_or_oid,
                payload_ts_ms=payload_ts_ms,
            )
            _print(
                {
                    "status": "blocked",
                    "layer": "execution_gate",
                    "mode": str(mode or "live"),
                    "broker": BROKER_NAME,
                    "reason": str(gate.get("reason") or "real_trading_not_allowed"),
                    "gate": gate,
                    "ts_ms": _now_ms(),
                    "dur_ms": _now_ms() - started_ms,
                }
            )
            return 0

        max_signal_age_s = int(os.environ.get("EXECUTION_MAX_SIGNAL_AGE_S", "300"))
        if payload_ts_ms is None or int(payload_ts_ms) <= 0:
            return _blocked(
                started_ms=int(started_ms),
                layer="payload_freshness",
                mode="live",
                broker=str(BROKER_NAME),
                reason="payload_ts_missing",
                payload={},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        payload_age_ms = _now_ms() - int(payload_ts_ms)
        if payload_age_ms > (max_signal_age_s * 1000):
            return _blocked(
                started_ms=int(started_ms),
                layer="payload_freshness",
                mode="live",
                broker=str(BROKER_NAME),
                reason="payload_stale",
                payload={
                    "payload_ts_ms": int(payload_ts_ms),
                    "payload_age_ms": int(payload_age_ms),
                    "max_signal_age_s": int(max_signal_age_s),
                },
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        try:
            from engine.runtime.health import run_preflight
            pre = run_preflight()
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_PREFLIGHT_EXCEPTION", e, once_key="preflight_exception")
            return _blocked(
                started_ms=int(started_ms),
                layer="preflight_exception",
                mode="live",
                broker=str(BROKER_NAME),
                reason="preflight_exception",
                payload={"error": str(e)},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        if not bool((pre or {}).get("ok")):
            return _blocked(
                started_ms=int(started_ms),
                layer="preflight",
                mode="live",
                broker=str(BROKER_NAME),
                reason="preflight_failed",
                payload={"preflight": pre},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        # ------------------------------------------------------------
        # Institutional completion layer (pre-trade):
        # 1) Position reconciliation (live brokers)
        # 2) Execution risk governor (defense in depth)
        # ------------------------------------------------------------
        try:
            con2 = connect()
            try:
                rec = pre_live_position_reconcile(
                    broker=str(BROKER_NAME or ""),
                    con=con2,
                )
                if isinstance(rec, dict) and rec.get("fatal_reconcile"):
                    return _blocked(
                        started_ms=int(started_ms),
                        layer="position_reconcile",
                        mode="live",
                        broker=str(BROKER_NAME),
                        reason="fatal_reconcile",
                        payload={"reconcile": rec},
                        correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
                    )
            finally:
                con2.close()
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_POSITION_RECONCILE_EXCEPTION", e, once_key="position_reconcile_exception")
            return _blocked(
                started_ms=int(started_ms),
                layer="position_reconcile_exception",
                mode="live",
                broker=str(BROKER_NAME),
                reason="position_reconcile_exception",
                payload={"error": str(e)},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        try:
            con3 = connect()
            try:
                governed, gov_info = apply_execution_risk_governor(
                    con3,
                    list(shaped_payload or []),
                    broker=str(BROKER_NAME or ""),
                    mode="live",
                    equity_usd=None,
                )
            finally:
                con3.close()
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_RISK_GOVERNOR_EXCEPTION", e, once_key="risk_governor_exception")
            return _blocked(
                started_ms=int(started_ms),
                layer="risk_governor_exception",
                mode="live",
                broker=str(BROKER_NAME),
                reason="risk_governor_exception",
                payload={"error": str(e)},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        if isinstance(gov_info, dict) and (not gov_info.get("ok")):
            return _blocked(
                started_ms=int(started_ms),
                layer="risk_governor",
                mode="live",
                broker=str(BROKER_NAME),
                reason="risk_governor_block",
                payload={"governor": gov_info},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
            )

        shaped_payload = list(governed or [])
        shaped_payload, pre_submit_competition_blocks = _pre_submit_revalidate_competition(
            shaped_payload,
        )
        real_payload, shadow_payload_by_model = _split_execution_payload(shaped_payload)
        live_blocked_orders = (
            list(production_model_blocked_orders or [])
            + list(model_blocked_orders or [])
            + list(pre_submit_competition_blocks or [])
        )

        dual_enable = os.environ.get("EXECUTION_DUAL_ENABLE", "0") == "1"
        command_id = _record_execution_command_boundary(
            ts_ms=int(_now_ms()),
            batch_id=batch_or_oid,
            payload_ts_ms=payload_ts_ms,
            payload_source=str(payload_source),
            mode="live",
            broker=str(BROKER_NAME),
            raw_payload=list(raw_payload or []),
            shaped_payload=list(shaped_payload or []),
            real_payload=list(real_payload or []),
            shadow_payload_by_model=shadow_payload_by_model,
            blocked_orders=list(live_blocked_orders or []),
        ) if (real_payload or shadow_payload_by_model) else None
        shadow_res = _execute_shadow_groups(
            shadow_groups=shadow_payload_by_model,
            batch_or_oid=batch_or_oid,
            payload_ts_ms=payload_ts_ms,
        ) if shadow_payload_by_model else {"ok": True, "groups": [], "submitted_models": 0, "fills_written": 0}
        _emit_decision_execution_snapshot(
            list(real_payload or []),
            mode="live",
            payload_source=str(payload_source),
            broker=str(BROKER_NAME),
        )

        try:
            broker_connection = refresh_broker_connection_health(broker=str(BROKER_NAME))
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_BROKER_HEALTH_REFRESH_FAILED", e, once_key="broker_health_refresh")
            broker_connection = {"ok": False, "state": "unknown"}

        if not bool((broker_connection or {}).get("ok")) or str((broker_connection or {}).get("state") or "").lower().strip() in ("disconnected", "connect_failed", "reconnect_failed"):
            return _blocked(
                started_ms=int(started_ms),
                layer="broker_connection_watchdog",
                mode="live",
                broker=str(BROKER_NAME),
                reason="broker_connection_unavailable",
                payload={"broker_connection": broker_connection, "shadow_result": shadow_res},
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
                command_id=command_id,
            )

        try:
            pre_exec_supervisor = refresh_execution_quality_supervisor(lookback_n=500)
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_EXECUTION_SUPERVISOR_REFRESH_FAILED", e, once_key="pre_exec_supervisor_refresh")
            pre_exec_supervisor = {"ok": False, "state": "unknown"}

        if str((pre_exec_supervisor or {}).get("state") or "").lower().strip() == "critical":
            return _blocked(
                started_ms=int(started_ms),
                layer="execution_quality_supervisor",
                mode="live",
                broker=str(BROKER_NAME),
                reason="execution_quality_critical",
                payload={
                    "execution_supervisor": pre_exec_supervisor,
                    "broker_connection": broker_connection,
                    "shadow_result": shadow_res,
                },
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
                command_id=command_id,
            )

        if dual_enable and str(BROKER_NAME).lower() == "ibkr" and callable(apply_portfolio_orders_dual_ibkr):
            res = apply_portfolio_orders_dual_ibkr(
                dry_run_live=False,
                override_orders=real_payload,
                override_order_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                override_ts_ms=(int(payload_ts_ms) if payload_ts_ms is not None else None),
            )
        else:
            res = apply_new_portfolio_orders(
                dry_run=False,
                override_orders=real_payload,
                override_order_id=(int(batch_or_oid) if batch_or_oid is not None else None),
                override_ts_ms=(int(payload_ts_ms) if payload_ts_ms is not None else None),
            ) if real_payload else {"ok": True, "status": "no_real_orders", "broker": BROKER_NAME}

        broker_used = str((res or {}).get("broker") or BROKER_NAME)
        _write_execution_meta_last(broker_used, "live_broker")
        _record_execution_event_boundary(
            event_type="command_result",
            status=("submitted" if bool((res or {}).get("ok", True)) else "failed"),
            mode="live",
            broker=str(BROKER_NAME),
            payload={
                "payload_source": str(payload_source),
                "batch_id": (int(batch_or_oid) if batch_or_oid is not None else None),
                "payload_ts_ms": (int(payload_ts_ms) if payload_ts_ms is not None else None),
                "broker_used": str(broker_used),
                "result": dict(res or {}),
                "shadow_result": dict(shadow_res or {}),
                "blocked_count": int(len(live_blocked_orders or [])),
                "blocked_orders": list(live_blocked_orders or []),
                "real_count": int(len(real_payload or [])),
                "shadow_count": int(sum(len(v) for v in shadow_payload_by_model.values())),
            },
            command_id=command_id,
            batch_id=batch_or_oid,
            payload_ts_ms=payload_ts_ms,
        )

        try:
            append_event(
                event_type=("order_submit_result" if bool((res or {}).get("ok", True)) else "order_error"),
                event_source="engine.execution.broker_apply_orders",
                event_version=1,
                entity_type="order_batch",
                entity_id=(str(batch_or_oid) if batch_or_oid is not None else str(int(_now_ms()))),
                correlation_id=(str(batch_or_oid) if batch_or_oid is not None else None),
                payload={
                    "ts_ms": int(_now_ms()),
                    "mode": "live",
                    "broker": str(BROKER_NAME),
                    "broker_used": str(broker_used),
                    "payload_source": str(payload_source),
                    "batch_id": (int(batch_or_oid) if batch_or_oid is not None else None),
                    "result": dict(res or {}),
                    "raw_count": int(len(raw_payload or [])),
                    "shaped_count": int(len(shaped_payload or [])),
                    "blocked_count": int(len(live_blocked_orders or [])),
                    "blocked_orders": list(live_blocked_orders or []),
                    "real_count": int(len(real_payload or [])),
                    "shadow_count": int(sum(len(v) for v in shadow_payload_by_model.values())),
                    "shadow_result": dict(shadow_res or {}),
                },
            )
        except Exception:
            _warn_nonfatal("BROKER_APPLY_ORDERS_ERROR_EVENT_APPEND_FAILED", Exception("append_event failed"), once_key="append_order_error")

        # ------------------------------------------------------------
        # Institutional completion layer (post-trade):
        # ------------------------------------------------------------
        try:
            from engine.execution.execution_analytics_engine import build_execution_analytics
            build_execution_analytics(limit=2000)
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_ANALYTICS_BUILD_FAILED", e, once_key="build_execution_analytics")

        try:
            refresh_execution_quality_supervisor(lookback_n=500)
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_EXECUTION_SUPERVISOR_REFRESH_FAILED", e, once_key="post_exec_supervisor_refresh")

        emit_counter(
            "order_throughput",
            int(len(shaped_payload or [])),
            component="engine.execution.broker_apply_orders",
            job=JOB_NAME,
            broker=str(BROKER_NAME),
            extra_tags={"throughput_type": "orders_shaped"},
        )

        emit_timing(
            "execution_latency_ms",
            int(_now_ms() - started_ms),
            component="engine.execution.broker_apply_orders",
            job=JOB_NAME,
            broker=str(BROKER_NAME),
        )

        trace_event(
            "order_submission",
            component="engine.execution.broker_apply_orders",
            entity_type="order_batch",
            entity_id=(str(batch_or_oid) if batch_or_oid is not None else JOB_NAME),
            payload={
                "mode": "live",
                "broker": str(BROKER_NAME),
                "broker_used": str(broker_used),
                "payload_source": str(payload_source),
                "raw_count": int(len(raw_payload or [])),
                "shaped_count": int(len(shaped_payload or [])),
                "blocked_count": int(len(live_blocked_orders or [])),
                "real_count": int(len(real_payload or [])),
                "shadow_count": int(sum(len(v) for v in shadow_payload_by_model.values())),
                "latency_ms": int(_now_ms() - started_ms),
            },
            job=JOB_NAME,
            broker=str(BROKER_NAME),
        )

        _print(
            {
                "status": "ok",
                "mode": "live",
                "broker": BROKER_NAME,
                "broker_used": broker_used,
                "payload_source": payload_source,
                "batch_id": batch_or_oid,
                "result": res,
                "shadow_result": shadow_res,
                "blocked_count": len(live_blocked_orders),
                "blocked_orders": live_blocked_orders,
                "real_count": len(real_payload),
                "shadow_count": sum(len(v) for v in shadow_payload_by_model.values()),
                "ts_ms": _now_ms(),
                "dur_ms": _now_ms() - started_ms,
            }
        )
        return 0
    except Exception as e:
        lineage = _summarize_order_lineage(shaped_payload or raw_payload)
        _record_execution_event_boundary(
            event_type="execution_error",
            status="failed",
            mode=(str(mode) if "mode" in locals() and mode is not None else "unknown"),
            broker=str(BROKER_NAME),
            payload={
                "job": str(JOB_NAME),
                "payload_source": str(payload_source),
                "batch_id": (int(batch_or_oid) if batch_or_oid is not None else None),
                "payload_ts_ms": (int(payload_ts_ms) if payload_ts_ms is not None else None),
                "lineage": lineage,
                "error": str(e),
            },
            command_id=command_id,
            batch_id=batch_or_oid,
            payload_ts_ms=payload_ts_ms,
        )
        try:
            append_event(
                event_type="order_error",
                event_source="engine.execution.broker_apply_orders",
                event_version=1,
                entity_type="job",
                entity_id=JOB_NAME,
                correlation_id=str(int(_now_ms())),
                payload={
                    "ts_ms": int(_now_ms()),
                    "job": str(JOB_NAME),
                    "broker": str(BROKER_NAME),
                    "mode": (str(mode) if "mode" in locals() and mode is not None else None),
                    "payload_source": str(payload_source),
                    "batch_id": (int(batch_or_oid) if batch_or_oid is not None else None),
                    "payload_ts_ms": (int(payload_ts_ms) if payload_ts_ms is not None else None),
                    "execution_target": (
                        lineage["execution_targets"][0]
                        if len(lineage.get("execution_targets") or []) == 1
                        else None
                    ),
                    "lineage": lineage,
                    "error": str(e),
                },
            )
        except Exception:
            _warn_nonfatal("BROKER_APPLY_ORDERS_ERROR_EVENT_APPEND_FAILED", Exception("append_event failed"), once_key="append_order_error")
        log_event(
            LOG,
            40,
            "broker_apply_orders_error",
            component="engine.execution.broker_apply_orders",
            extra={
                "job": JOB_NAME,
                "broker": BROKER_NAME,
                "mode": (str(mode) if "mode" in locals() and mode is not None else None),
                "payload_source": str(payload_source),
                "batch_id": (int(batch_or_oid) if batch_or_oid is not None else None),
                "payload_ts_ms": (int(payload_ts_ms) if payload_ts_ms is not None else None),
                "lineage": lineage,
                "error": str(e),
            },
        )
        _warn_nonfatal("BROKER_APPLY_ORDERS_MAIN_FAILED", e, once_key="main_failed")
        return 2
    finally:
        try:
            lock_heartbeat_stop.set()
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_HEARTBEAT_STOP_FAILED", e, once_key="heartbeat_stop")
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("BROKER_APPLY_ORDERS_JOB_LOCK_RELEASE_FAILED", e, once_key="job_lock_release")


if __name__ == "__main__":
    raise SystemExit(main())
