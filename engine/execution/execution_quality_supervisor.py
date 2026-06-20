"""
FILE: execution_quality_supervisor.py

Execution subsystem module for `execution_quality_supervisor`.
"""

import logging
import json
import math
import os
import time
from typing import Any, Dict, List

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.execution.execution_analytics_engine import get_execution_degradation_snapshot
from engine.execution.execution_broker_watchdog import get_broker_connection_health

LOG = get_logger("engine.execution.execution_quality_supervisor")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception as e:
        _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_SAFE_FLOAT_FAILED", e, value_type=type(v).__name__)
        return default


def _truthy_env(name: str, default: str = "0") -> bool:
    raw = str(os.environ.get(name, default) or default).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _broker_required_for_execution() -> bool:
    execution_mode = str(os.environ.get("EXECUTION_MODE") or "").strip().lower()
    engine_mode = str(os.environ.get("ENGINE_MODE") or "").strip().lower()
    mode = execution_mode or engine_mode

    if execution_mode in ("live", "paper") or engine_mode in ("live", "paper", "shadow"):
        return True
    if mode == "shadow":
        return True
    if mode in ("safe", "sim", "dev", "test", "local", "") and _truthy_env("DISABLE_LIVE_EXECUTION", "1"):
        return False
    return True


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event="execution_quality_supervisor_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.execution_quality_supervisor",
        extra=extra or None,
        persist=False,
    )


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name=?
            LIMIT 1
            """,
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_TABLE_EXISTS_FAILED", e, table=str(table_name))
        return False


def _default_integrity_snapshot(detail: str, *, missing_tables: List[str] | None = None) -> Dict[str, Any]:
    return {
        "ok": False,
        "detail": str(detail),
        "missing_tables": list(missing_tables or []),
        "duplicate_order_count": 0,
        "duplicate_fill_count": 0,
        "missing_fill_count": 0,
        "stale_missing_fill_count": 0,
        "fills_without_order_count": 0,
        "unreconciled_order_reference_count": 0,
        "out_of_order_fill_count": 0,
        "inconsistent_position_count": 0,
        "pricing_unavailable_count": 0,
        "duplicate_orders": [],
        "duplicate_fills": [],
        "missing_fills": [],
        "stale_missing_fills": [],
        "fills_without_order": [],
        "unreconciled_order_references": [],
        "out_of_order_fills": [],
        "position_mismatches": [],
        "pricing_unavailable_positions": [],
    }


def _execution_table_state(con) -> Dict[str, Any]:
    required = [
        "execution_orders",
        "execution_fills",
        "model_position_state",
        "pnl_attribution",
    ]
    missing = [name for name in required if not _table_exists(con, name)]
    return {
        "ok": len(missing) == 0,
        "required_tables": list(required),
        "missing_tables": list(missing),
    }


def _has_execution_activity(con) -> bool:
    for table_name in ("execution_orders", "execution_fills"):
        if not _table_exists(con, table_name):
            continue
        try:
            row = con.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
            if row:
                return True
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_QUALITY_SUPERVISOR_ACTIVITY_CHECK_FAILED",
                e,
                table=str(table_name),
            )
    return False


def _account_state_snapshot(con, broker_conn: Dict[str, Any], *, broker_required: bool = True) -> Dict[str, Any]:
    broker_name = str((broker_conn or {}).get("broker") or os.environ.get("BROKER_NAME", os.environ.get("BROKER", "sim")) or "sim").lower().strip()
    if broker_name in ("sim", "paper", "sandbox", ""):
        has_activity = _has_execution_activity(con)
        if not _table_exists(con, "broker_account"):
            return {
                "ok": not has_activity,
                "broker": broker_name or "sim",
                "detail": ("broker_account_not_initialized" if not has_activity else "broker_account_missing"),
            }
        broker_account_cols = {
            str(col[1] or "").strip().lower()
            for col in list(con.execute("PRAGMA table_info(broker_account)").fetchall() or [])
            if len(col) > 1
        }
        if "id" in broker_account_cols:
            row = con.execute(
                """
                SELECT cash, equity, updated_ts_ms
                FROM broker_account
                WHERE id=1
                LIMIT 1
                """
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT cash, equity, COALESCE(updated_ts_ms, ts_ms, 0) AS updated_ts_ms
                FROM broker_account
                ORDER BY COALESCE(updated_ts_ms, ts_ms, 0) DESC
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return {
                "ok": not has_activity,
                "broker": broker_name or "sim",
                "detail": ("broker_account_not_initialized" if not has_activity else "broker_account_row_missing"),
            }
        try:
            cash = float(row[0] or 0.0)
            equity = float(row[1] or 0.0)
        except Exception as e:
            _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_ACCOUNT_PARSE_FAILED", e, broker=str(broker_name or "sim"))
            return {
                "ok": False,
                "broker": broker_name or "sim",
                "detail": "broker_account_parse_failed",
            }
        ok = math.isfinite(cash) and math.isfinite(equity) and float(equity) > 0.0
        return {
            "ok": bool(ok),
            "broker": broker_name or "sim",
            "cash": float(cash),
            "equity": float(equity),
            "updated_ts_ms": int(row[2] or 0),
            "detail": ("ok" if ok else "invalid_account_balance_state"),
        }

    broker_ok = bool((broker_conn or {}).get("ok"))
    if not broker_required and not broker_ok:
        return {
            "ok": True,
            "broker": broker_name or "unknown",
            "state": str((broker_conn or {}).get("state") or "unknown"),
            "detail": "broker_not_required_for_safe_execution_mode",
        }
    return {
        "ok": bool(broker_ok),
        "broker": broker_name or "unknown",
        "state": str((broker_conn or {}).get("state") or "unknown"),
        "detail": ("validated_via_broker_connection_only" if broker_ok else "broker_connection_unavailable"),
    }


def _ensure_tables(con) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS execution_health_state (
          ts_ms INTEGER NOT NULL,
          state TEXT NOT NULL,
          score REAL,
          n INTEGER,
          mean_slippage_bps REAL,
          p95_slippage_bps REAL,
          mean_latency_ms REAL,
          p95_latency_ms REAL,
          routing_error_rate REAL,
          open_due INTEGER,
          broker_failures INTEGER,
          extra_json TEXT,
          PRIMARY KEY (ts_ms)
        );

        CREATE INDEX IF NOT EXISTS idx_execution_health_state_ts
          ON execution_health_state(ts_ms);

        CREATE TABLE IF NOT EXISTS execution_alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          severity TEXT NOT NULL,
          alert_type TEXT NOT NULL,
          state TEXT NOT NULL,
          details_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_execution_alerts_ts
          ON execution_alerts(ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_alerts_type_ts
          ON execution_alerts(alert_type, ts_ms);
        """
    )


def _open_due(con) -> int:
    try:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM exec_open_orders
            WHERE status='open'
            """
        ).fetchone()
        return int((row or [0])[0] or 0)
    except Exception as e:
        _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_OPEN_DUE_FAILED", e)
        return 0


def _routing_failures(con, lookback_ms: int) -> int:
    try:
        row = con.execute(
            """
            SELECT COUNT(*)
            FROM exec_order_events
            WHERE ts_ms >= ?
              AND event IN (
                'limit_replace_submit_failed',
                'market_submit_failed',
                'broker_order_missing',
                'broker_lookup_failed',
                'broker_ack_timeout',
                'cancel_failed',
                'get_order_failed'
              )
            """
        , (int(_now_ms() - int(lookback_ms)),)).fetchone()
        return int((row or [0])[0] or 0)
    except Exception as e:
        _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_ROUTING_FAILURES_FAILED", e, lookback_ms=int(lookback_ms))
        return 0


def _last_fills_age_ms(con) -> int:
    try:
        row = con.execute(
            """
            SELECT MAX(fill_ts_ms)
            FROM execution_fills
            """
        ).fetchone()
        ts_ms = int((row or [None])[0] or 0)
        if ts_ms <= 0:
            return -1
        return int(_now_ms() - ts_ms)
    except Exception as e:
        _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_LAST_FILLS_AGE_FAILED", e)
        return -1


def _oldest_open_order_age_ms(con) -> int:
    try:
        row = con.execute(
            """
            SELECT MIN(ts_ms)
            FROM exec_open_orders
            WHERE status='open'
            """
        ).fetchone()
        ts_ms = int((row or [None])[0] or 0)
        if ts_ms <= 0:
            return -1
        return int(_now_ms() - ts_ms)
    except Exception as e:
        _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_OLDEST_OPEN_ORDER_AGE_FAILED", e)
        return -1


def refresh_execution_quality_supervisor(lookback_n: int = 500) -> Dict[str, Any]:
    con = connect()
    try:
        _ensure_tables(con)
        # This supervisor rolls several execution-quality signals into one
        # persisted health view for runtime gating and operator visibility.
        snap = get_execution_degradation_snapshot(con, lookback_n=int(max(50, lookback_n)))
        open_due = _open_due(con)
        routing_failures = _routing_failures(
            con,
            lookback_ms=int(os.environ.get("EXEC_SUPERVISOR_LOOKBACK_MS", str(6 * 3600000))),
        )
        last_fill_age_ms = _last_fills_age_ms(con)
        oldest_open_order_age_ms = _oldest_open_order_age_ms(con)

        broker_required = _broker_required_for_execution()
        try:
            broker_conn = get_broker_connection_health(readonly=not broker_required)
        except Exception:
            broker_conn = {"ok": False, "state": "unknown"}
        broker_state = str((broker_conn or {}).get("state") or "").lower().strip()
        broker_conn_valid = bool((broker_conn or {}).get("ok")) and broker_state not in (
            "disconnected",
            "connect_failed",
            "reconnect_failed",
            "unsupported_broker",
            "unknown",
        )
        broker_gate_ok = bool(broker_conn_valid or not broker_required)

        execution_tables = _execution_table_state(con)
        integrity = _default_integrity_snapshot(
            "execution_engine_tables_missing",
            missing_tables=list(execution_tables.get("missing_tables") or []),
        )
        if bool(execution_tables.get("ok")):
            try:
                from engine.execution.execution_ledger import audit_execution_integrity

                integrity.update(dict(audit_execution_integrity(con=con) or {}))
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_QUALITY_SUPERVISOR_INTEGRITY_AUDIT_FAILED",
                    e,
                )
                integrity = _default_integrity_snapshot(
                    f"execution_integrity_audit_failed:{type(e).__name__}",
                    missing_tables=list(execution_tables.get("missing_tables") or []),
                )

        account_state = _account_state_snapshot(con, dict(broker_conn or {}), broker_required=broker_required)
        duplicate_order_count = int(integrity.get("duplicate_order_count") or 0)
        duplicate_fill_count = int(integrity.get("duplicate_fill_count") or 0)
        stale_missing_fill_count = int(integrity.get("stale_missing_fill_count") or 0)
        fills_without_order_count = int(integrity.get("fills_without_order_count") or 0)
        unreconciled_order_reference_count = int(integrity.get("unreconciled_order_reference_count") or 0)
        submission_unrecorded_count = int(integrity.get("submission_unrecorded_count") or 0)
        inconsistent_position_count = int(integrity.get("inconsistent_position_count") or 0)
        pricing_unavailable_count = int(integrity.get("pricing_unavailable_count") or 0)

        gates = {
            "execution_engine_initialized": {
                "ok": bool(execution_tables.get("ok")),
                "detail": (
                    "execution_engine_ready"
                    if bool(execution_tables.get("ok"))
                    else f"missing_tables={list(execution_tables.get('missing_tables') or [])}"
                ),
            },
            "broker_or_sim_connection_valid": {
                "ok": bool(broker_gate_ok),
                "detail": (
                    f"broker={broker_conn.get('broker') or 'unknown'} "
                    f"state={broker_conn.get('state') or 'unknown'} "
                    f"detail={broker_conn.get('detail') or 'ok'} "
                    f"required={broker_required}"
                ),
            },
            "order_state_consistent": {
                "ok": bool(execution_tables.get("ok")) and duplicate_order_count == 0 and duplicate_fill_count == 0 and stale_missing_fill_count == 0 and fills_without_order_count == 0 and unreconciled_order_reference_count == 0 and submission_unrecorded_count == 0,
                "detail": (
                    f"duplicate_order_count={duplicate_order_count} "
                    f"duplicate_fill_count={duplicate_fill_count} "
                    f"fills_without_order_count={fills_without_order_count} "
                    f"unreconciled_order_reference_count={unreconciled_order_reference_count} "
                    f"submission_unrecorded_count={submission_unrecorded_count} "
                    f"stale_missing_fill_count={stale_missing_fill_count}"
                ),
            },
            "position_state_consistent": {
                "ok": bool(execution_tables.get("ok")) and inconsistent_position_count == 0,
                "detail": f"inconsistent_position_count={inconsistent_position_count}",
            },
            "pnl_calculation_valid": {
                "ok": bool(execution_tables.get("ok")) and pricing_unavailable_count == 0,
                "detail": f"pricing_unavailable_count={pricing_unavailable_count}",
            },
        }
        failed_gates = [str(name) for name, gate in gates.items() if not bool((gate or {}).get("ok"))]

        mean_slip = float(snap.get("mean_slippage") or 0.0)
        p95_slip = float(snap.get("p95_slippage") or 0.0)
        mean_lat = float(snap.get("mean_latency") or 0.0)
        p95_lat = float(snap.get("p95_latency") or 0.0)
        n = int(snap.get("n") or 0)

        # The score is intentionally coarse and additive so multiple weak
        # signals can escalate state even if none alone is catastrophic.
        score = 0.0
        alerts: List[Dict[str, Any]] = []

        if p95_slip >= float(os.environ.get("EXEC_SUPERVISOR_CRIT_SLIP_BPS", "25.0")):
            score += 3.0
            alerts.append({"severity": "critical", "alert_type": "abnormal_slippage", "value": p95_slip})
        elif p95_slip >= float(os.environ.get("EXEC_SUPERVISOR_WARN_SLIP_BPS", "12.0")):
            score += 1.5
            alerts.append({"severity": "warn", "alert_type": "abnormal_slippage", "value": p95_slip})

        if p95_lat >= float(os.environ.get("EXEC_SUPERVISOR_CRIT_LAT_MS", "30000")):
            score += 3.0
            alerts.append({"severity": "critical", "alert_type": "latency_spike", "value": p95_lat})
        elif p95_lat >= float(os.environ.get("EXEC_SUPERVISOR_WARN_LAT_MS", "10000")):
            score += 1.5
            alerts.append({"severity": "warn", "alert_type": "latency_spike", "value": p95_lat})

        if routing_failures >= int(os.environ.get("EXEC_SUPERVISOR_ROUTE_FAIL_CRIT", "5")):
            score += 3.0
            alerts.append({"severity": "critical", "alert_type": "broker_routing_problem", "value": routing_failures})
        elif routing_failures >= int(os.environ.get("EXEC_SUPERVISOR_ROUTE_FAIL_WARN", "2")):
            score += 1.5
            alerts.append({"severity": "warn", "alert_type": "broker_routing_problem", "value": routing_failures})

        if open_due >= int(os.environ.get("EXEC_SUPERVISOR_OPEN_DUE_CRIT", "20")):
            score += 2.0
            alerts.append({"severity": "critical", "alert_type": "stale_orders", "value": open_due})
        elif open_due >= int(os.environ.get("EXEC_SUPERVISOR_OPEN_DUE_WARN", "8")):
            score += 1.0
            alerts.append({"severity": "warn", "alert_type": "stale_orders", "value": open_due})

        if last_fill_age_ms >= int(os.environ.get("EXEC_SUPERVISOR_FILL_AGE_WARN_MS", "1800000")) and n > 0:
            score += 1.0
            alerts.append({"severity": "warn", "alert_type": "execution_stall", "value": last_fill_age_ms})

        if oldest_open_order_age_ms >= int(os.environ.get("EXEC_SUPERVISOR_OPEN_TIMEOUT_CRIT_MS", "180000")):
            score += 3.0
            alerts.append({"severity": "critical", "alert_type": "execution_timeout", "value": oldest_open_order_age_ms})
        elif oldest_open_order_age_ms >= int(os.environ.get("EXEC_SUPERVISOR_OPEN_TIMEOUT_WARN_MS", "60000")):
            score += 1.5
            alerts.append({"severity": "warn", "alert_type": "execution_timeout", "value": oldest_open_order_age_ms})

        if broker_required and broker_state in ("disconnected", "connect_failed", "reconnect_failed"):
            score += 3.0
            alerts.append({"severity": "critical", "alert_type": "broker_connection_failure", "value": broker_state})
        elif broker_required and broker_state in ("reconnecting", "unknown", "degraded"):
            score += 1.5
            alerts.append({"severity": "warn", "alert_type": "broker_connection_warning", "value": broker_state})

        if duplicate_order_count > 0 or duplicate_fill_count > 0:
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "duplicate_order_risk_detected",
                    "value": {
                        "duplicate_order_count": int(duplicate_order_count),
                        "duplicate_fill_count": int(duplicate_fill_count),
                    },
                }
            )
        if stale_missing_fill_count > 0:
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "missing_fills_detected",
                    "value": {
                        "missing_fill_count": int(integrity.get("missing_fill_count") or 0),
                        "stale_missing_fill_count": int(stale_missing_fill_count),
                    },
                }
            )
        if fills_without_order_count > 0 or unreconciled_order_reference_count > 0:
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "fill_missing_local_order_reference",
                    "value": {
                        "fills_without_order_count": int(fills_without_order_count),
                        "unreconciled_order_reference_count": int(unreconciled_order_reference_count),
                    },
                }
            )
        if submission_unrecorded_count > 0:
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "broker_submission_unrecorded_needs_reconcile",
                    "value": {
                        "submission_unrecorded_count": int(submission_unrecorded_count),
                        "submissions": list(integrity.get("submission_unrecorded") or [])[:20],
                    },
                }
            )
        if inconsistent_position_count > 0:
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "order_position_mismatch",
                    "value": int(inconsistent_position_count),
                }
            )
        if pricing_unavailable_count > 0:
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "pricing_unavailable_for_unrealized_pnl",
                    "value": int(pricing_unavailable_count),
                }
            )
        if not bool(account_state.get("ok", True)):
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "invalid_account_balance_state",
                    "value": dict(account_state),
                }
            )
        if not bool(execution_tables.get("ok")):
            alerts.append(
                {
                    "severity": "critical",
                    "alert_type": "execution_engine_not_initialized",
                    "value": list(execution_tables.get("missing_tables") or []),
                }
            )

        if failed_gates or (not bool(account_state.get("ok", True))):
            score = max(score, 6.0)

        state = "ok"
        if score >= 6.0:
            state = "critical"
        elif score >= 2.5:
            state = "degraded"

        payload = {
            "ts_ms": int(_now_ms()),
            "state": str(state),
            "score": float(score),
            "n": int(n),
            "mean_slippage_bps": float(mean_slip),
            "p95_slippage_bps": float(p95_slip),
            "mean_latency_ms": float(mean_lat),
            "p95_latency_ms": float(p95_lat),
            "routing_failures": int(routing_failures),
            "open_due": int(open_due),
            "last_fill_age_ms": int(last_fill_age_ms),
            "oldest_open_order_age_ms": int(oldest_open_order_age_ms),
            "broker_connection": dict(broker_conn or {}),
            "account_state": dict(account_state or {}),
            "gates": dict(gates),
            "failed_gates": list(failed_gates),
            "integrity": dict(integrity),
            "alerts": alerts,
        }

        broker_failures = 0 if bool(broker_gate_ok) else 1

        con.execute(
            """
            INSERT INTO execution_health_state(
              ts_ms, state, score, n, mean_slippage_bps, p95_slippage_bps,
              mean_latency_ms, p95_latency_ms, routing_error_rate, open_due,
              broker_failures, extra_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(payload["ts_ms"]),
                str(payload["state"]),
                float(payload["score"]),
                int(payload["n"]),
                float(payload["mean_slippage_bps"]),
                float(payload["p95_slippage_bps"]),
                float(payload["mean_latency_ms"]),
                float(payload["p95_latency_ms"]),
                float(payload["routing_failures"]),
                int(payload["open_due"]),
                int(broker_failures),
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )

        for alert in alerts:
            con.execute(
                """
                INSERT INTO execution_alerts(ts_ms, severity, alert_type, state, details_json)
                VALUES(?,?,?,?,?)
                """,
                (
                    int(payload["ts_ms"]),
                    str(alert.get("severity") or "warn"),
                    str(alert.get("alert_type") or "execution"),
                    str(state),
                    json.dumps(
                        {
                            **payload,
                            "alert": alert,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )

        con.commit()
        try:
            from engine.cache.wrappers.execution_health import prime_execution_health

            prime_execution_health(payload)
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_QUALITY_SUPERVISOR_CACHE_PRIME_FAILED",
                e,
                operation="refresh_execution_quality_supervisor",
            )
        return {"ok": True, **payload}
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_CLOSE_FAILED", e, operation="refresh_execution_quality_supervisor")


def _readonly_unavailable_snapshot(detail: str = "execution_quality_snapshot_unavailable") -> Dict[str, Any]:
    return {
        "ok": False,
        "state": "unknown",
        "score": 0.0,
        "n": 0,
        "mean_slippage_bps": 0.0,
        "p95_slippage_bps": 0.0,
        "mean_latency_ms": 0.0,
        "p95_latency_ms": 0.0,
        "routing_failures": 0,
        "open_due": 0,
        "broker_failures": 0,
        "last_fill_age_ms": -1,
        "oldest_open_order_age_ms": -1,
        "broker_connection": {},
        "account_state": {},
        "gates": {},
        "failed_gates": [],
        "integrity": _default_integrity_snapshot(str(detail)),
        "alerts": [],
        "detail": str(detail),
    }


def get_execution_quality_snapshot(*, readonly: bool = False) -> Dict[str, Any]:
    if bool(readonly):
        try:
            from engine.cache.wrappers.execution_health import read_execution_health

            cached = read_execution_health()
            if cached:
                return {"ok": True, **dict(cached)}
        except Exception as e:
            _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_CACHE_READ_FAILED", e, operation="get_execution_quality_snapshot")
    con = connect(readonly=bool(readonly))
    try:
        if not bool(readonly):
            _ensure_tables(con)
        row = con.execute(
            """
            SELECT ts_ms, state, score, n, mean_slippage_bps, p95_slippage_bps,
                   mean_latency_ms, p95_latency_ms, routing_error_rate, open_due,
                   broker_failures, extra_json
            FROM execution_health_state
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()

        if not row:
            if bool(readonly):
                return _readonly_unavailable_snapshot("execution_quality_snapshot_missing")
            return refresh_execution_quality_supervisor()

        extra = {}
        try:
            extra = json.loads(row[11] or "{}")
            if not isinstance(extra, dict):
                extra = {}
        except Exception:
            extra = {}

        ts_ms = int(row[0]) if row[0] is not None else 0
        max_age_ms = int(os.environ.get("EXEC_SUPERVISOR_MAX_AGE_MS", "30000"))
        if ts_ms <= 0 or (_now_ms() - ts_ms) > max_age_ms:
            if bool(readonly):
                snapshot = _readonly_unavailable_snapshot("execution_quality_snapshot_stale")
                snapshot.update(
                    {
                        "ts_ms": int(row[0]) if row[0] is not None else None,
                        "state": str(row[1] or "unknown"),
                        "score": float(row[2] or 0.0),
                        "n": int(row[3] or 0),
                        "mean_slippage_bps": float(row[4] or 0.0),
                        "p95_slippage_bps": float(row[5] or 0.0),
                        "mean_latency_ms": float(row[6] or 0.0),
                        "p95_latency_ms": float(row[7] or 0.0),
                        "routing_failures": int(row[8] or 0),
                        "open_due": int(row[9] or 0),
                        "broker_failures": int(row[10] or 0),
                        "last_fill_age_ms": int(extra.get("last_fill_age_ms") or -1),
                        "oldest_open_order_age_ms": int(extra.get("oldest_open_order_age_ms") or -1),
                        "broker_connection": dict(extra.get("broker_connection") or {}),
                        "account_state": dict(extra.get("account_state") or {}),
                        "gates": dict(extra.get("gates") or {}),
                        "failed_gates": list(extra.get("failed_gates") or []),
                        "integrity": dict(extra.get("integrity") or _default_integrity_snapshot("execution_quality_snapshot_stale")),
                        "alerts": list(extra.get("alerts") or []),
                    }
                )
                return snapshot
            return refresh_execution_quality_supervisor()

        return {
            "ok": True,
            "ts_ms": int(row[0]) if row[0] is not None else None,
            "state": str(row[1] or "unknown"),
            "score": float(row[2] or 0.0),
            "n": int(row[3] or 0),
            "mean_slippage_bps": float(row[4] or 0.0),
            "p95_slippage_bps": float(row[5] or 0.0),
            "mean_latency_ms": float(row[6] or 0.0),
            "p95_latency_ms": float(row[7] or 0.0),
            "routing_failures": int(row[8] or 0),
            "open_due": int(row[9] or 0),
            "broker_failures": int(row[10] or 0),
            "last_fill_age_ms": int(extra.get("last_fill_age_ms") or -1),
            "oldest_open_order_age_ms": int(extra.get("oldest_open_order_age_ms") or -1),
            "broker_connection": dict(extra.get("broker_connection") or {}),
            "account_state": dict(extra.get("account_state") or {}),
            "gates": dict(extra.get("gates") or {}),
            "failed_gates": list(extra.get("failed_gates") or []),
            "integrity": dict(extra.get("integrity") or _default_integrity_snapshot("execution_quality_snapshot_ok")),
            "alerts": list(extra.get("alerts") or []),
        }
    except Exception as e:
        if bool(readonly):
            _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_READONLY_SNAPSHOT_FAILED", e, operation="get_execution_quality_snapshot")
            return _readonly_unavailable_snapshot(f"execution_quality_snapshot_failed:{type(e).__name__}")
        raise
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("EXECUTION_QUALITY_SUPERVISOR_CLOSE_FAILED", e, operation="get_execution_quality_snapshot")
