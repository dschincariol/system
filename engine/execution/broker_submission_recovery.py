"""Recovery handling for broker-accepted orders that local bookkeeping missed."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable, Dict, Mapping, Optional

from engine.execution.broker_action_audit import record_broker_action_audit
from engine.execution.order_idempotency import (
    SUBMISSION_UNRECORDED_STATUS,
    mark_order_submission_unrecorded,
)
from engine.runtime import storage
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.metrics import emit_counter


LOG = logging.getLogger("engine.execution.broker_submission_recovery")
NEEDS_RECONCILE_STATUS = "needs_reconcile"
EXECUTION_ALERT_TYPE = "broker_submission_unrecorded_needs_reconcile"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _commit_if_outermost(con: Any, had_transaction: bool) -> None:
    if not bool(had_transaction):
        con.commit()


def _open_read_connection(connect_fn: Optional[Callable[..., Any]] = None) -> Any:
    fn = connect_fn or storage.connect
    try:
        return fn(readonly=True)
    except TypeError:
        return fn()


def _table_missing_error(exc: BaseException) -> bool:
    text = str(exc or "").lower()
    return "no such table" in text or "does not exist" in text or "undefinedtable" in text


def _json_dumps(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload or {}), separators=(",", ":"), sort_keys=True)


def _ensure_execution_alerts(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          severity TEXT NOT NULL,
          alert_type TEXT NOT NULL,
          state TEXT NOT NULL,
          details_json TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_alerts_type_ts
          ON execution_alerts(alert_type, ts_ms)
        """
    )


def _write_execution_alert(
    *,
    con: Any,
    broker: str,
    symbol: str,
    qty: float,
    client_order_id: str,
    broker_order_id: Optional[str],
    order_uid: str,
    portfolio_orders_id: Optional[int],
    submit_ts_ms: int,
    error: BaseException,
) -> Dict[str, Any]:
    if con is None:
        return {"ok": False, "status": "alert_not_written", "detail": "connection_unavailable"}
    payload = {
        "broker": str(broker or "").strip().lower(),
        "symbol": str(symbol or "").strip().upper(),
        "qty": float(qty or 0.0),
        "client_order_id": str(client_order_id or ""),
        "broker_order_id": (str(broker_order_id) if broker_order_id is not None else None),
        "order_uid": str(order_uid or ""),
        "portfolio_orders_id": (int(portfolio_orders_id) if portfolio_orders_id is not None else None),
        "submit_ts_ms": int(submit_ts_ms or 0),
        "status": SUBMISSION_UNRECORDED_STATUS,
        "reason": NEEDS_RECONCILE_STATUS,
        "error_type": type(error).__name__,
        "error": str(error),
    }
    try:
        had_transaction = bool(getattr(con, "in_transaction", False))
        _ensure_execution_alerts(con)
        con.execute(
            """
            INSERT INTO execution_alerts(ts_ms, severity, alert_type, state, details_json)
            VALUES(?,?,?,?,?)
            """,
            (
                _now_ms(),
                "critical",
                EXECUTION_ALERT_TYPE,
                NEEDS_RECONCILE_STATUS,
                _json_dumps(payload),
            ),
        )
        _commit_if_outermost(con, had_transaction)
        return {"ok": True, "alert_type": EXECUTION_ALERT_TYPE, "severity": "critical"}
    except Exception as exc:
        log_failure(
            LOG,
            event="broker_submission_unrecorded_alert_failed",
            code="BROKER_SUBMISSION_UNRECORDED_ALERT_FAILED",
            message=str(exc),
            error=exc,
            level=logging.WARNING,
            component="engine.execution.broker_submission_recovery",
            extra=payload,
            persist=False,
        )
        return {"ok": False, "status": "alert_write_failed", "error": f"{type(exc).__name__}: {exc}"}


def unrecorded_submission_gate(
    *,
    broker: str,
    con: Any = None,
    connect_fn: Optional[Callable[..., Any]] = None,
    limit: int = 20,
) -> Optional[Dict[str, Any]]:
    broker_name = str(broker or "").strip().lower() or "unknown"
    owns_con = con is None
    db = con
    try:
        if db is None:
            db = _open_read_connection(connect_fn)
        rows = db.execute(
            """
            SELECT order_uid, client_order_id, broker_order_id, symbol, updated_ts_ms, last_error
            FROM execution_order_idempotency
            WHERE LOWER(TRIM(status))=?
              AND LOWER(TRIM(broker))=?
            ORDER BY updated_ts_ms ASC, order_uid ASC
            LIMIT ?
            """,
            (SUBMISSION_UNRECORDED_STATUS, broker_name, max(1, int(limit or 20))),
        ).fetchall()
    except Exception as exc:
        if _table_missing_error(exc):
            return None
        return {
            "ok": False,
            "status": NEEDS_RECONCILE_STATUS,
            "reason": "submission_reconcile_gate_unavailable",
            "detail": "could_not_check_unrecorded_broker_submissions",
            "broker": broker_name,
            "fatal_reconcile": True,
            "stop_failover": True,
            "retryable": False,
            "needs_reconcile": True,
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        if owns_con and db is not None:
            try:
                db.close()
            except Exception as close_exc:
                log_failure(
                    LOG,
                    event="broker_submission_recovery_close_failed",
                    code="BROKER_SUBMISSION_RECOVERY_CLOSE_FAILED",
                    message=str(close_exc),
                    error=close_exc,
                    level=logging.WARNING,
                    component="engine.execution.broker_submission_recovery",
                    persist=False,
                )

    if not rows:
        return None
    submissions = [
        {
            "order_uid": str(row[0] or ""),
            "client_order_id": str(row[1] or ""),
            "broker_order_id": str(row[2] or ""),
            "symbol": str(row[3] or "").upper().strip(),
            "updated_ts_ms": int(row[4] or 0),
            "last_error": str(row[5] or ""),
        }
        for row in rows
    ]
    return {
        "ok": False,
        "status": NEEDS_RECONCILE_STATUS,
        "reason": SUBMISSION_UNRECORDED_STATUS,
        "detail": "broker_accepted_order_missing_durable_local_submission_record",
        "broker": broker_name,
        "fatal_reconcile": True,
        "stop_failover": True,
        "retryable": False,
        "needs_reconcile": True,
        "unrecorded_submission_count": int(len(submissions)),
        "unrecorded_submissions": submissions,
    }


def record_submission_unrecorded(
    *,
    con: Any,
    broker: str,
    symbol: str,
    qty: float,
    order_uid: str,
    client_order_id: str,
    broker_order_id: Optional[str],
    submit_ts_ms: int,
    portfolio_orders_id: Optional[int],
    portfolio_ts_ms: Optional[int],
    source_order_id: Optional[int],
    source_alert_id: Optional[int],
    payload: Optional[Mapping[str, Any]],
    error: BaseException,
    stage: str,
    submitted_n: int = 0,
) -> Dict[str, Any]:
    broker_name = str(broker or "").strip().lower() or "unknown"
    symbol_norm = str(symbol or "").strip().upper()
    error_text = f"{str(stage or 'bookkeeping')}:{type(error).__name__}:{error}"
    marker: Dict[str, Any]
    try:
        marker = mark_order_submission_unrecorded(
            con=con,
            broker=broker_name,
            order_uid=str(order_uid or ""),
            client_order_id=str(client_order_id or ""),
            broker_order_id=broker_order_id,
            submit_ts_ms=int(submit_ts_ms or _now_ms()),
            last_error=error_text,
            symbol=symbol_norm,
            portfolio_orders_id=portfolio_orders_id,
            portfolio_ts_ms=portfolio_ts_ms,
            source_order_id=source_order_id,
            source_alert_id=source_alert_id,
            payload=dict(payload or {}),
        )
    except Exception as marker_exc:
        log_failure(
            LOG,
            event="broker_submission_unrecorded_marker_failed",
            code="BROKER_SUBMISSION_UNRECORDED_MARKER_FAILED",
            message=str(marker_exc),
            error=marker_exc,
            level=logging.ERROR,
            component="engine.execution.broker_submission_recovery",
            extra={
                "broker": broker_name,
                "symbol": symbol_norm,
                "order_uid": str(order_uid or ""),
                "client_order_id": str(client_order_id or ""),
                "broker_order_id": str(broker_order_id or ""),
                "stage": str(stage or ""),
                "bookkeeping_error": str(error),
            },
            persist=True,
        )
        marker = {
            "ok": False,
            "marker_written": False,
            "status": "recovery_marker_failed",
            "error": f"{type(marker_exc).__name__}: {marker_exc}",
        }

    alert = _write_execution_alert(
        con=con,
        broker=broker_name,
        symbol=symbol_norm,
        qty=float(qty or 0.0),
        client_order_id=str(client_order_id or ""),
        broker_order_id=broker_order_id,
        order_uid=str(order_uid or ""),
        portfolio_orders_id=portfolio_orders_id,
        submit_ts_ms=int(submit_ts_ms or 0),
        error=error,
    )
    audit = record_broker_action_audit(
        broker=broker_name,
        action="order_submission_unrecorded",
        status=NEEDS_RECONCILE_STATUS,
        symbol=symbol_norm,
        qty=float(qty or 0.0),
        client_order_id=str(client_order_id or ""),
        broker_order_id=(str(broker_order_id) if broker_order_id is not None else None),
        portfolio_orders_id=portfolio_orders_id,
        payload={
            "order_uid": str(order_uid or ""),
            "stage": str(stage or ""),
            "needs_reconcile": True,
            "marker": dict(marker or {}),
            "alert": dict(alert or {}),
            "error_type": type(error).__name__,
            "error": str(error),
        },
        ts_ms=int(submit_ts_ms or _now_ms()),
    )
    log_failure(
        LOG,
        event="broker_submission_unrecorded",
        code="BROKER_SUBMISSION_UNRECORDED",
        message=error_text,
        error=error,
        level=logging.ERROR,
        component="engine.execution.broker_submission_recovery",
        extra={
            "broker": broker_name,
            "symbol": symbol_norm,
            "qty": float(qty or 0.0),
            "order_uid": str(order_uid or ""),
            "client_order_id": str(client_order_id or ""),
            "broker_order_id": str(broker_order_id or ""),
            "portfolio_orders_id": portfolio_orders_id,
            "stage": str(stage or ""),
            "marker_written": bool((marker or {}).get("marker_written")),
            "alert_written": bool((alert or {}).get("ok")),
            "audit_written": bool((audit or {}).get("ok")),
        },
        persist=True,
    )
    try:
        emit_counter(
            "broker_submission_unrecorded",
            1,
            component="engine.execution.broker_submission_recovery",
            broker=broker_name,
            symbol=symbol_norm,
            extra_tags={"stage": str(stage or "bookkeeping")},
        )
    except Exception as metric_exc:
        log_failure(
            LOG,
            event="broker_submission_recovery_metric_failed",
            code="BROKER_SUBMISSION_RECOVERY_METRIC_FAILED",
            message=str(metric_exc),
            error=metric_exc,
            level=logging.WARNING,
            component="engine.execution.broker_submission_recovery",
            persist=False,
        )

    return {
        "ok": False,
        "status": SUBMISSION_UNRECORDED_STATUS,
        "reason": NEEDS_RECONCILE_STATUS,
        "detail": "broker_accepted_order_local_bookkeeping_failed",
        "broker": broker_name,
        "symbol": symbol_norm,
        "order_uid": str(order_uid or ""),
        "client_order_id": str(client_order_id or ""),
        "broker_order_id": (str(broker_order_id) if broker_order_id is not None else None),
        "submitted_n": int(submitted_n or 0),
        "stop_failover": True,
        "fatal_reconcile": True,
        "retryable": False,
        "needs_reconcile": True,
        "recovery_marker": dict(marker or {}),
        "alert": dict(alert or {}),
        "audit": dict(audit or {}),
        "error": str(error),
    }
