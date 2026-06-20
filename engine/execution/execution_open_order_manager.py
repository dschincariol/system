"""
FILE: execution_open_order_manager.py

Execution subsystem module for `execution_open_order_manager`.
"""

import json
import logging
import os
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.execution.broker_submission_recovery import record_submission_unrecorded
from engine.execution.execution_ledger import log_submit
from engine.execution.order_idempotency import (
    claim_open_order_replacement_submission_durable,
    mark_order_submission_submitted_durable,
    mark_order_submission_unknown_durable,
)
from engine.execution.execution_microstructure import (
    MAX_OPEN_ORDERS_PER_PASS,
    EPS_QTY,
    _now_ms,
    _ensure_tables,
    _log_event,
    _adjust_limit_px,
    _next_aggressiveness,
    _next_action_ms,
    _remaining_qty,
    _is_open_like_status,
    verify_cancel_before_replace,
    try_native_limit_replace,
    mark_cancel_replace_needs_reconcile,
)

LOG = get_logger("engine.execution.execution_open_order_manager")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.execution.execution_open_order_manager",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


class _SubmissionUnrecorded(RuntimeError):
    def __init__(self, result: Dict[str, Any]) -> None:
        super().__init__(str((result or {}).get("status") or "submission_unrecorded"))
        self.result = dict(result or {})


class _SubmissionUnknown(RuntimeError):
    def __init__(self, result: Dict[str, Any]) -> None:
        super().__init__(str((result or {}).get("status") or "submit_inflight_unknown"))
        self.result = dict(result or {})


def manage_open_orders() -> Dict[str, Any]:
    """
    Called periodically (e.g. from execution_poll_and_attrib.py).
    - Supports Alpaca and IBKR.
    - Retries rejected / stale / missing-ack orders without duplicating known fills.
    - Fail-closed on acknowledgement failure by quarantining the order when broker state is unknown.
    """
    out = {"ok": True, "managed": 0, "updated": 0, "errors": 0}
    con = connect()
    try:
        _ensure_tables(con)
        now = _now_ms()

        rows = con.execute(
            """
            SELECT id, broker, symbol, qty, side, order_type, aggressiveness, limit_px,
                   client_order_id, broker_order_id, attempts, max_attempts,
                   next_action_ts_ms, portfolio_orders_id, source_alert_id, meta_json
            FROM exec_open_orders
            WHERE status='open' AND next_action_ts_ms <= ?
            ORDER BY next_action_ts_ms ASC
            LIMIT ?
            """,
            (int(now), int(MAX_OPEN_ORDERS_PER_PASS)),
        ).fetchall()

        if not rows:
            return {**out, "open_due": 0}

        try:
            from engine.execution.broker_alpaca_rest import get_order as alpaca_get_order
            from engine.execution.broker_alpaca_rest import cancel_order as alpaca_cancel_order
            from engine.execution.broker_alpaca_rest import replace_limit_order as alpaca_replace_limit_order
            from engine.execution.broker_alpaca_rest import submit_limit_order as alpaca_submit_limit_order
            from engine.execution.broker_alpaca_rest import submit_market_order as alpaca_submit_market_order
        except Exception:
            alpaca_get_order = None
            alpaca_cancel_order = None
            alpaca_replace_limit_order = None
            alpaca_submit_limit_order = None
            alpaca_submit_market_order = None

        try:
            from engine.execution.broker_ibkr_gateway import get_order as ibkr_get_order
            from engine.execution.broker_ibkr_gateway import cancel_order as ibkr_cancel_order
            from engine.execution.broker_ibkr_gateway import submit_limit_order as ibkr_submit_limit_order
            from engine.execution.broker_ibkr_gateway import submit_market_order as ibkr_submit_market_order
        except Exception:
            ibkr_get_order = None
            ibkr_cancel_order = None
            ibkr_submit_limit_order = None
            ibkr_submit_market_order = None

        def _handlers(broker: str):
            br = str(broker or "").lower().strip()
            if br == "alpaca":
                return alpaca_get_order, alpaca_cancel_order, alpaca_submit_limit_order, alpaca_submit_market_order
            if br == "ibkr":
                return ibkr_get_order, ibkr_cancel_order, ibkr_submit_limit_order, ibkr_submit_market_order
            return None, None, None, None

        def _native_replace_handler(broker: str):
            br = str(broker or "").lower().strip()
            if br == "alpaca":
                return alpaca_replace_limit_order
            return None

        def _has_known_fills(client_oid: str, broker_oid: str) -> bool:
            # Before retrying, check whether the order already produced fills so
            # recovery logic does not manufacture duplicates during uncertainty.
            try:
                row = con.execute(
                    """
                    SELECT 1
                    FROM execution_fills
                    WHERE client_order_id = ?
                       OR (raw_json IS NOT NULL AND INSTR(raw_json, ?) > 0)
                    LIMIT 1
                    """,
                    (str(client_oid or ""), str(broker_oid or "")),
                ).fetchone()
                return bool(row)
            except Exception as e:
                _warn_nonfatal(
                    "EXEC_OPEN_ORDER_KNOWN_FILL_LOOKUP_FAILED",
                    e,
                    once_key=f"known_fill_lookup:{client_oid}:{broker_oid}",
                    client_order_id=str(client_oid or ""),
                    broker_order_id=str(broker_oid or ""),
                )
                return False

        def _close_row(open_id: int, status: str, remaining_qty: float, details: Dict[str, Any]) -> None:
            con.execute(
                """
                UPDATE exec_open_orders
                SET updated_ts_ms=?, qty=?, status=?, next_action_ts_ms=0
                WHERE id=?
                """,
                (int(now), float(remaining_qty), str(status), int(open_id)),
            )
            _log_event(con, open_id, "closed", details)

        def _raise_submission_unrecorded(
            *,
            open_id: int,
            broker: str,
            symbol: str,
            qty: float,
            order_uid: str,
            client_order_id: str,
            broker_order_id: Optional[str],
            submit_ts_ms: int,
            attempts: int,
            portfolio_orders_id,
            source_alert_id,
            payload: Dict[str, Any],
            error: BaseException,
            stage: str,
        ) -> None:
            result = record_submission_unrecorded(
                con=con,
                broker=str(broker),
                symbol=str(symbol),
                qty=float(qty),
                order_uid=str(order_uid),
                client_order_id=str(client_order_id),
                broker_order_id=(str(broker_order_id) if broker_order_id is not None else None),
                submit_ts_ms=int(submit_ts_ms or _now_ms()),
                portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                portfolio_ts_ms=None,
                source_order_id=None,
                source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                payload={**dict(payload or {}), "open_order_id": int(open_id)},
                error=error,
                stage=str(stage or "log_submit"),
                submitted_n=0,
                durable_idempotency=True,
                connect_fn=connect,
            )
            try:
                con.execute(
                    """
                    UPDATE exec_open_orders
                    SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?,
                        attempts=?, status='submission_unrecorded', next_action_ts_ms=0
                    WHERE id=?
                    """,
                    (
                        int(now),
                        float(qty),
                        str(client_order_id),
                        str(broker_order_id) if broker_order_id is not None else None,
                        int(attempts),
                        int(open_id),
                    ),
                )
                _log_event(
                    con,
                    open_id,
                    "submission_unrecorded",
                    {
                        "client_order_id": str(client_order_id),
                        "broker_order_id": str(broker_order_id or ""),
                        "stage": str(stage or "log_submit"),
                        "needs_reconcile": True,
                    },
                )
            except Exception as row_exc:
                _warn_nonfatal(
                    "EXEC_OPEN_ORDER_SUBMISSION_UNRECORDED_ROW_UPDATE_FAILED",
                    row_exc,
                    once_key=f"submission_unrecorded_row:{open_id}:{client_order_id}",
                    broker=str(broker),
                    symbol=str(symbol),
                    client_order_id=str(client_order_id),
                    broker_order_id=str(broker_order_id or ""),
                    open_order_id=int(open_id),
                )
            raise _SubmissionUnrecorded(result)

        def _resubmit_order(*, open_id: int, broker: str, symbol: str, side: str, remaining_qty: float, order_type: str, limit_px: Optional[float], client_oid: str, broker_oid: str, attempts: int, max_attempts: int, portfolio_orders_id, source_alert_id, meta: Dict[str, Any], event_name: str, status_hint: str) -> None:
            # Retries are persisted as new submits so the ledger reflects the
            # actual broker-facing order lineage, not just the original intent.
            _get_order_fn, _cancel_order_fn, submit_limit_fn, submit_market_fn = _handlers(broker)
            next_attempt = int(attempts) + 1
            next_aggr = _next_aggressiveness(str(meta.get("aggressiveness") or ""), next_attempt, max_attempts)
            next_action_ts_ms = _next_action_ms(int(now), meta)
            venue = str(meta.get("venue") or meta.get("broker_venue") or broker)

            def _block_replacement(
                *,
                reason: str,
                order_uid: str,
                replacement_client_order_id: str,
                replacement_broker_order_id: Optional[str] = None,
                status: str = "submit_inflight_unknown",
                error: Optional[BaseException] = None,
                details: Optional[Dict[str, Any]] = None,
            ) -> None:
                mark_cancel_replace_needs_reconcile(
                    con,
                    open_id=int(open_id),
                    now_ms=int(now),
                    broker=str(broker),
                    symbol=str(symbol),
                    qty=float(remaining_qty),
                    client_order_id=str(client_oid),
                    broker_order_id=str(broker_oid or ""),
                    reason=str(reason),
                    details={
                        "replacement_order_uid": str(order_uid or ""),
                        "replacement_client_order_id": str(replacement_client_order_id or ""),
                        "replacement_broker_order_id": str(replacement_broker_order_id or ""),
                        "replacement_attempt": int(next_attempt),
                        "replacement_status": str(status),
                        "retry_event": str(event_name),
                        "retry_status": str(status_hint),
                        "error": f"{type(error).__name__}: {error}" if error is not None else "",
                        **dict(details or {}),
                    },
                    meta=meta,
                )
                raise _SubmissionUnknown(
                    {
                        "ok": False,
                        "status": str(status),
                        "broker": str(broker),
                        "detail": str(reason),
                        "order_uid": str(order_uid or ""),
                        "client_order_id": str(replacement_client_order_id or ""),
                        "broker_order_id": str(replacement_broker_order_id or ""),
                        "symbol": str(symbol),
                        "open_order_id": int(open_id),
                        "stop_failover": True,
                        "fatal_reconcile": True,
                        "retryable": False,
                        "needs_reconcile": True,
                        "error": str(error or ""),
                    }
                )

            def _recover_duplicate_claim(
                *,
                guard: Dict[str, Any],
                replacement_order_type: str,
                replacement_limit_px: Optional[float],
                replacement_aggressiveness: str,
            ) -> None:
                order_uid = str(guard.get("order_uid") or "")
                replacement_client_oid = str(guard.get("client_order_id") or "")
                replacement_broker_oid = str(guard.get("broker_order_id") or "")
                claim_status = str(guard.get("status") or "claimed").lower().strip()

                if claim_status == "submitted" and replacement_broker_oid:
                    if str(replacement_order_type).upper() == "MARKET":
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?, attempts=?,
                                order_type='MARKET', aggressiveness='MARKET', status='escalated_market',
                                next_action_ts_ms=0
                            WHERE id=?
                            """,
                            (
                                int(now),
                                float(remaining_qty),
                                str(replacement_client_oid),
                                str(replacement_broker_oid),
                                int(next_attempt),
                                int(open_id),
                            ),
                        )
                    else:
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?, attempts=?,
                                order_type='LIMIT', aggressiveness=?, limit_px=?, status='open',
                                next_action_ts_ms=?
                            WHERE id=?
                            """,
                            (
                                int(now),
                                float(remaining_qty),
                                str(replacement_client_oid),
                                str(replacement_broker_oid),
                                int(next_attempt),
                                str(replacement_aggressiveness),
                                float(replacement_limit_px or 0.0),
                                int(next_action_ts_ms),
                                int(open_id),
                            ),
                        )
                    _log_event(
                        con,
                        open_id,
                        "replacement_recovered_from_idempotency",
                        {
                            "attempt": int(next_attempt),
                            "order_uid": str(order_uid),
                            "client_order_id": str(replacement_client_oid),
                            "broker_order_id": str(replacement_broker_oid),
                            "idempotency_status": str(claim_status),
                            "event": str(event_name),
                            "status": str(status_hint),
                        },
                    )
                    return

                if claim_status == "submission_unrecorded":
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?,
                            attempts=?, status='submission_unrecorded', next_action_ts_ms=0
                        WHERE id=?
                        """,
                        (
                            int(now),
                            float(remaining_qty),
                            str(replacement_client_oid),
                            str(replacement_broker_oid or ""),
                            int(next_attempt),
                            int(open_id),
                        ),
                    )
                    _log_event(
                        con,
                        open_id,
                        "submission_unrecorded",
                        {
                            "client_order_id": str(replacement_client_oid),
                            "broker_order_id": str(replacement_broker_oid or ""),
                            "order_uid": str(order_uid),
                            "stage": "idempotency_recovery",
                            "needs_reconcile": True,
                        },
                    )
                    raise _SubmissionUnrecorded(
                        {
                            "ok": False,
                            "status": "submission_unrecorded",
                            "reason": "needs_reconcile",
                            "detail": "broker_accepted_order_local_bookkeeping_failed",
                            "broker": str(broker),
                            "symbol": str(symbol),
                            "order_uid": str(order_uid),
                            "client_order_id": str(replacement_client_oid),
                            "broker_order_id": str(replacement_broker_oid or ""),
                            "open_order_id": int(open_id),
                            "stop_failover": True,
                            "fatal_reconcile": True,
                            "retryable": False,
                            "needs_reconcile": True,
                        }
                    )

                if claim_status == "claimed":
                    try:
                        mark_order_submission_unknown_durable(
                            order_uid=str(order_uid),
                            last_error="replacement retry observed pre-submit claim; broker submit skipped",
                            connect_fn=connect,
                        )
                    except Exception as mark_err:
                        _warn_nonfatal(
                            "EXEC_OPEN_ORDER_REPLACEMENT_MARK_UNKNOWN_FAILED",
                            mark_err,
                            once_key=f"replacement_mark_unknown:{order_uid}",
                            broker=str(broker),
                            symbol=str(symbol),
                            order_uid=str(order_uid),
                            client_order_id=str(replacement_client_oid),
                            open_order_id=int(open_id),
                        )
                    _block_replacement(
                        reason="replacement_claim_already_exists",
                        order_uid=order_uid,
                        replacement_client_order_id=replacement_client_oid,
                        replacement_broker_order_id=replacement_broker_oid,
                        status="submit_inflight_unknown",
                        details={"idempotency_status": str(claim_status), "last_error": str(guard.get("last_error") or "")},
                    )

                _block_replacement(
                    reason="replacement_idempotency_duplicate",
                    order_uid=order_uid,
                    replacement_client_order_id=replacement_client_oid,
                    replacement_broker_order_id=replacement_broker_oid,
                    status="submit_inflight_unknown",
                    details={"idempotency_status": str(claim_status), "last_error": str(guard.get("last_error") or "")},
                )

            if order_type == "MARKET" or next_aggr == "MARKET" or (max_attempts > 0 and attempts >= max_attempts):
                if not submit_market_fn:
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, qty=?, status='gave_up', next_action_ts_ms=0
                        WHERE id=?
                        """,
                        (int(now), float(remaining_qty), int(open_id)),
                    )
                    _log_event(con, open_id, "gave_up", {"attempts": int(attempts), "max_attempts": int(max_attempts), "status": status_hint})
                    return

                new_client_oid = f"{client_oid}_m{next_attempt}"
                market_payload = {
                    "timeout_escalation": True,
                    "retry_event": str(event_name),
                    "retry_status": str(status_hint),
                    "escalated_from_client_order_id": client_oid,
                    "escalated_to_order_type": "MARKET",
                    "attempts": int(next_attempt),
                    "max_attempts": int(max_attempts),
                    "remaining_qty": float(remaining_qty),
                    "meta": meta,
                }
                replacement_order_type = "MARKET"
                replacement_limit_px = None
                replacement_aggr = "MARKET"
                ref_px = float(limit_px) if limit_px is not None else 0.0
                payload = market_payload
            else:
                if not submit_limit_fn:
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, qty=?, next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (int(now), float(remaining_qty), int(next_action_ts_ms), int(open_id)),
                    )
                    return

                next_limit = _adjust_limit_px(float(limit_px or 0.0), float(remaining_qty), next_attempt)
                new_client_oid = f"{client_oid}_r{next_attempt}"
                limit_payload = {
                    "timeout_escalation": True,
                    "retry_event": str(event_name),
                    "retry_status": str(status_hint),
                    "reprice_attempt": int(next_attempt),
                    "prev_client_order_id": client_oid,
                    "escalated_to_aggressiveness": str(next_aggr),
                    "remaining_qty": float(remaining_qty),
                    "meta": meta,
                }
                replacement_order_type = "LIMIT"
                replacement_limit_px = float(next_limit)
                replacement_aggr = str(next_aggr)
                ref_px = float(next_limit)
                payload = limit_payload

            replacement_payload = {
                **dict(payload or {}),
                "open_order_id": int(open_id),
                "replacement_client_order_id": str(new_client_oid),
                "replacement_attempt": int(next_attempt),
                "replacement_order_type": str(replacement_order_type),
                "replacement_limit_px": replacement_limit_px,
                "replacement_side": str(side),
                "replacement_venue": str(venue),
            }
            try:
                guard = claim_open_order_replacement_submission_durable(
                    broker=str(broker),
                    open_order_id=int(open_id),
                    original_client_order_id=str(client_oid),
                    replacement_attempt=int(next_attempt),
                    remaining_qty=float(remaining_qty),
                    side=str(side),
                    symbol=str(symbol),
                    client_order_id=str(new_client_oid),
                    venue=str(venue),
                    order_type=str(replacement_order_type),
                    limit_px=replacement_limit_px,
                    portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                    source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                    payload=replacement_payload,
                    connect_fn=connect,
                )
            except Exception as e:
                _warn_nonfatal(
                    "EXEC_OPEN_ORDER_REPLACEMENT_IDEMPOTENCY_CLAIM_FAILED",
                    e,
                    once_key=f"replacement_claim_failed:{open_id}:{new_client_oid}",
                    broker=str(broker),
                    symbol=str(symbol),
                    client_order_id=str(new_client_oid),
                    open_order_id=int(open_id),
                )
                _block_replacement(
                    reason="replacement_idempotency_claim_failed",
                    order_uid="",
                    replacement_client_order_id=str(new_client_oid),
                    status="order_idempotency_claim_failed",
                    error=e,
                )

            if not bool(guard.get("ok")):
                _block_replacement(
                    reason="replacement_idempotency_claim_failed",
                    order_uid=str(guard.get("order_uid") or ""),
                    replacement_client_order_id=str(guard.get("client_order_id") or new_client_oid),
                    status=str(guard.get("status") or "order_idempotency_claim_failed"),
                    details={"guard": dict(guard or {})},
                )
            if bool(guard.get("duplicate")):
                _recover_duplicate_claim(
                    guard=dict(guard or {}),
                    replacement_order_type=str(replacement_order_type),
                    replacement_limit_px=replacement_limit_px,
                    replacement_aggressiveness=str(replacement_aggr),
                )
                return

            order_uid = str(guard.get("order_uid") or "")
            try:
                if str(replacement_order_type).upper() == "MARKET":
                    res = submit_market_fn(symbol=symbol, qty=float(remaining_qty), client_oid=new_client_oid) or {}
                else:
                    res = submit_limit_fn(symbol=symbol, qty=float(remaining_qty), limit_price=float(replacement_limit_px or 0.0), client_oid=new_client_oid) or {}
            except Exception as e:
                try:
                    mark_order_submission_unknown_durable(
                        order_uid=str(order_uid),
                        last_error=str(e),
                        connect_fn=connect,
                    )
                except Exception as mark_err:
                    _warn_nonfatal(
                        "EXEC_OPEN_ORDER_REPLACEMENT_MARK_UNKNOWN_FAILED",
                        mark_err,
                        once_key=f"replacement_mark_unknown:{order_uid}",
                        broker=str(broker),
                        symbol=str(symbol),
                        order_uid=str(order_uid),
                        client_order_id=str(new_client_oid),
                        open_order_id=int(open_id),
                    )
                _block_replacement(
                    reason="replacement_submit_ambiguous",
                    order_uid=str(order_uid),
                    replacement_client_order_id=str(new_client_oid),
                    status="submit_inflight_unknown",
                    error=e,
                )

            new_broker_oid = str((res or {}).get("id") or "") or None
            submit_ts_ms = int(_now_ms())
            if not new_broker_oid:
                try:
                    mark_order_submission_unknown_durable(
                        order_uid=str(order_uid),
                        last_error="broker_submit_missing_broker_order_id",
                        connect_fn=connect,
                    )
                except Exception as mark_err:
                    _warn_nonfatal(
                        "EXEC_OPEN_ORDER_REPLACEMENT_MARK_UNKNOWN_FAILED",
                        mark_err,
                        once_key=f"replacement_mark_unknown:{order_uid}",
                        broker=str(broker),
                        symbol=str(symbol),
                        order_uid=str(order_uid),
                        client_order_id=str(new_client_oid),
                        open_order_id=int(open_id),
                    )
                _block_replacement(
                    reason="replacement_submit_missing_broker_order_id",
                    order_uid=str(order_uid),
                    replacement_client_order_id=str(new_client_oid),
                    status="submit_inflight_unknown",
                    details={"broker_response": dict(res or {})},
                )

            submitted_payload = {**dict(replacement_payload or {}), "order_uid": str(order_uid), "idempotency_status": "submitted"}
            try:
                log_submit(
                    client_order_id=new_client_oid,
                    broker=str(broker),
                    symbol=symbol,
                    qty=float(remaining_qty),
                    submit_ts_ms=int(submit_ts_ms),
                    ref_px=float(ref_px),
                    broker_order_id=str(new_broker_oid),
                    portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                    source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                    extra=submitted_payload,
                    order_uid=str(order_uid),
                    idempotency_status="submitted",
                )
            except Exception as e:
                _warn_nonfatal(
                    "EXEC_OPEN_ORDER_MANAGER_REPLACEMENT_LOG_SUBMIT_FAILED",
                    e,
                    once_key=f"manage_open_orders_replacement_log_submit:{open_id}:{new_client_oid}",
                    broker=str(broker),
                    symbol=str(symbol),
                    client_order_id=str(new_client_oid),
                    order_uid=str(order_uid),
                    open_order_id=int(open_id),
                )
                _raise_submission_unrecorded(
                    open_id=open_id,
                    broker=broker,
                    symbol=symbol,
                    qty=float(remaining_qty),
                    order_uid=str(order_uid),
                    client_order_id=new_client_oid,
                    broker_order_id=str(new_broker_oid),
                    submit_ts_ms=int(submit_ts_ms),
                    attempts=next_attempt,
                    portfolio_orders_id=portfolio_orders_id,
                    source_alert_id=source_alert_id,
                    payload={**dict(replacement_payload or {}), "order_uid": str(order_uid), "idempotency_status": "submission_unrecorded"},
                    error=e,
                    stage="log_submit",
                )

            try:
                mark_order_submission_submitted_durable(
                    order_uid=str(order_uid),
                    client_order_id=str(new_client_oid),
                    broker_order_id=str(new_broker_oid),
                    submit_ts_ms=int(submit_ts_ms),
                    connect_fn=connect,
                )
            except Exception as e:
                _warn_nonfatal(
                    "EXEC_OPEN_ORDER_MANAGER_REPLACEMENT_MARK_SUBMITTED_FAILED",
                    e,
                    once_key=f"manage_open_orders_replacement_mark_submitted:{open_id}:{new_client_oid}",
                    broker=str(broker),
                    symbol=str(symbol),
                    client_order_id=str(new_client_oid),
                    broker_order_id=str(new_broker_oid),
                    order_uid=str(order_uid),
                    open_order_id=int(open_id),
                )
                _raise_submission_unrecorded(
                    open_id=open_id,
                    broker=broker,
                    symbol=symbol,
                    qty=float(remaining_qty),
                    order_uid=str(order_uid),
                    client_order_id=new_client_oid,
                    broker_order_id=str(new_broker_oid),
                    submit_ts_ms=int(submit_ts_ms),
                    attempts=next_attempt,
                    portfolio_orders_id=portfolio_orders_id,
                    source_alert_id=source_alert_id,
                    payload={**dict(replacement_payload or {}), "order_uid": str(order_uid), "idempotency_status": "submission_unrecorded"},
                    error=e,
                    stage="mark_order_submission_submitted",
                )

            if str(replacement_order_type).upper() == "MARKET":
                try:
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?, attempts=?,
                            order_type='MARKET', aggressiveness='MARKET', status='escalated_market',
                            next_action_ts_ms=0
                        WHERE id=?
                        """,
                        (
                            int(now),
                            float(remaining_qty),
                            str(new_client_oid),
                            str(new_broker_oid),
                            int(next_attempt),
                            int(open_id),
                        ),
                    )
                    _log_event(
                        con,
                        open_id,
                        "escalated_market",
                        {
                            "attempt": int(next_attempt),
                            "broker_order_id": new_broker_oid,
                            "prev_client_order_id": client_oid,
                            "remaining_qty": float(remaining_qty),
                            "event": event_name,
                            "status": status_hint,
                            "order_uid": str(order_uid),
                            "idempotency_status": "submitted",
                        },
                    )
                except Exception as e:
                    _raise_submission_unrecorded(
                        open_id=open_id,
                        broker=broker,
                        symbol=symbol,
                        qty=float(remaining_qty),
                        order_uid=str(order_uid),
                        client_order_id=new_client_oid,
                        broker_order_id=str(new_broker_oid),
                        submit_ts_ms=int(submit_ts_ms),
                        attempts=next_attempt,
                        portfolio_orders_id=portfolio_orders_id,
                        source_alert_id=source_alert_id,
                        payload={**dict(replacement_payload or {}), "order_uid": str(order_uid), "idempotency_status": "submission_unrecorded"},
                        error=e,
                        stage="open_order_update",
                    )
                return

            try:
                con.execute(
                    """
                    UPDATE exec_open_orders
                    SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?, attempts=?,
                        aggressiveness=?, limit_px=?, status='open', next_action_ts_ms=?
                    WHERE id=?
                    """,
                    (
                        int(now),
                        float(remaining_qty),
                        str(new_client_oid),
                        str(new_broker_oid),
                        int(next_attempt),
                        str(replacement_aggr),
                        float(replacement_limit_px or 0.0),
                        int(next_action_ts_ms),
                        int(open_id),
                    ),
                )
                _log_event(
                    con,
                    open_id,
                    "replaced",
                    {
                        "attempt": int(next_attempt),
                        "limit_px": float(replacement_limit_px or 0.0),
                        "aggressiveness": str(replacement_aggr),
                        "broker_order_id": new_broker_oid,
                        "remaining_qty": float(remaining_qty),
                        "event": event_name,
                        "status": status_hint,
                        "order_uid": str(order_uid),
                        "idempotency_status": "submitted",
                    },
                )
            except Exception as e:
                _raise_submission_unrecorded(
                    open_id=open_id,
                    broker=broker,
                    symbol=symbol,
                    qty=float(remaining_qty),
                    order_uid=str(order_uid),
                    client_order_id=new_client_oid,
                    broker_order_id=str(new_broker_oid),
                    submit_ts_ms=int(submit_ts_ms),
                    attempts=next_attempt,
                    portfolio_orders_id=portfolio_orders_id,
                    source_alert_id=source_alert_id,
                    payload={**dict(replacement_payload or {}), "order_uid": str(order_uid), "idempotency_status": "submission_unrecorded"},
                    error=e,
                    stage="open_order_update",
                )

        for r in rows or []:
            out["managed"] += 1
            try:
                open_id = int(r[0])
                broker = str(r[1] or "").lower().strip()
                symbol = str(r[2] or "").upper().strip()
                qty = float(r[3] or 0.0)
                side = str(r[4] or ("BUY" if qty >= 0 else "SELL")).upper().strip()
                order_type = str(r[5] or "").upper().strip()
                aggressiveness = str(r[6] or "").upper().strip()
                limit_px = r[7]
                client_oid = str(r[8] or "")
                broker_oid = str(r[9] or "")
                attempts = int(r[10] or 0)
                max_attempts = int(r[11] or 0)
                portfolio_orders_id = r[13]
                source_alert_id = r[14]
                meta_json = str(r[15] or "{}")

                try:
                    meta = json.loads(meta_json or "{}")
                except Exception:
                    meta = {}
                meta.setdefault("aggressiveness", aggressiveness)
                next_action_ts_ms = _next_action_ms(int(now), meta)
                ack_timeout_ms = int(meta.get("ack_timeout_ms") or float(os.environ.get("EXEC_ACK_TIMEOUT_S", os.environ.get("IBKR_ACK_TIMEOUT_S", "15"))) * 1000.0)
                broker_submit_ts_ms = int(meta.get("broker_submit_ts_ms") or meta.get("submit_ts_ms") or 0)

                get_order_fn, cancel_order_fn, submit_limit_fn, submit_market_fn = _handlers(broker)
                if not get_order_fn:
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (int(now), int(next_action_ts_ms), int(open_id)),
                    )
                    out["updated"] += 1
                    continue

                if not broker_oid:
                    ack_age_ms = int(now - broker_submit_ts_ms) if broker_submit_ts_ms > 0 else int(now)
                    if ack_age_ms >= int(ack_timeout_ms):
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?, status='ack_timeout', next_action_ts_ms=0
                            WHERE id=?
                            """,
                            (int(now), int(open_id)),
                        )
                        _log_event(con, open_id, "broker_ack_timeout", {"ack_age_ms": int(ack_age_ms), "ack_timeout_ms": int(ack_timeout_ms), "client_order_id": client_oid})
                    else:
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?, next_action_ts_ms=?
                            WHERE id=?
                            """,
                            (int(now), int(next_action_ts_ms), int(open_id)),
                        )
                    out["updated"] += 1
                    continue

                try:
                    oinfo = get_order_fn(str(broker_oid)) or {}
                except Exception as e:
                    _warn_nonfatal(
                        "EXEC_OPEN_ORDER_GET_ORDER_FAILED",
                        e,
                        once_key=f"get_order:{broker}:{broker_oid}",
                        open_id=int(open_id),
                        broker=str(broker),
                        broker_order_id=str(broker_oid),
                    )
                    out["errors"] += 1
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (int(now), int(next_action_ts_ms), int(open_id)),
                    )
                    _log_event(con, open_id, "get_order_failed", {"broker_order_id": broker_oid})
                    out["updated"] += 1
                    continue

                if not oinfo:
                    if _has_known_fills(client_oid=client_oid, broker_oid=broker_oid):
                        _close_row(open_id, "filled", 0.0, {"status": "filled", "broker_order_id": broker_oid, "filled_qty": qty, "reconciled_from_fills": True})
                    else:
                        mark_cancel_replace_needs_reconcile(
                            con,
                            open_id=open_id,
                            now_ms=int(now),
                            broker=broker,
                            symbol=symbol,
                            qty=float(qty),
                            client_order_id=client_oid,
                            broker_order_id=broker_oid,
                            reason="broker_order_missing_unverified",
                            details={
                                "attempts": int(attempts),
                                "max_attempts": int(max_attempts),
                                "portfolio_orders_id": portfolio_orders_id,
                                "source_alert_id": source_alert_id,
                            },
                            meta=meta,
                        )
                        out["errors"] += 1
                    out["updated"] += 1
                    continue

                st = str(oinfo.get("status") or "").lower().strip()
                remaining_qty = _remaining_qty(float(qty), oinfo)

                if st in ("filled",) or abs(float(remaining_qty)) <= float(EPS_QTY):
                    _close_row(open_id, "filled", 0.0, {"status": "filled", "broker_order_id": broker_oid, "filled_qty": oinfo.get("filled_qty")})
                    out["updated"] += 1
                    continue

                if st in ("rejected", "inactive", "api_cancelled", "expired"):
                    if max_attempts > 0 and attempts >= max_attempts:
                        _close_row(open_id, str(st or "rejected"), float(remaining_qty), {"status": st, "broker_order_id": broker_oid})
                    else:
                        _resubmit_order(open_id=open_id, broker=broker, symbol=symbol, side=side, remaining_qty=float(remaining_qty or qty), order_type=order_type, limit_px=(float(limit_px) if limit_px is not None else None), client_oid=client_oid, broker_oid=broker_oid, attempts=attempts, max_attempts=max_attempts, portfolio_orders_id=portfolio_orders_id, source_alert_id=source_alert_id, meta=meta, event_name="rejected_retry", status_hint=st)
                    out["updated"] += 1
                    continue

                if st in ("cancelled", "canceled"):
                    if max_attempts > 0 and attempts >= max_attempts:
                        _close_row(open_id, str(st), float(remaining_qty), {"status": st, "broker_order_id": broker_oid})
                    else:
                        _resubmit_order(open_id=open_id, broker=broker, symbol=symbol, side=side, remaining_qty=float(remaining_qty or qty), order_type=order_type, limit_px=(float(limit_px) if limit_px is not None else None), client_oid=client_oid, broker_oid=broker_oid, attempts=attempts, max_attempts=max_attempts, portfolio_orders_id=portfolio_orders_id, source_alert_id=source_alert_id, meta=meta, event_name="cancel_retry", status_hint=st)
                    out["updated"] += 1
                    continue

                if not _is_open_like_status(st):
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (int(now), int(next_action_ts_ms), int(open_id)),
                    )
                    _log_event(con, open_id, "unexpected_status", {"status": st, "broker_order_id": broker_oid})
                    out["updated"] += 1
                    continue

                if order_type != "LIMIT" or limit_px is None:
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, qty=?, next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (int(now), float(remaining_qty), int(next_action_ts_ms), int(open_id)),
                    )
                    out["updated"] += 1
                    continue

                native_replace = try_native_limit_replace(
                    con,
                    open_id=open_id,
                    now_ms=int(now),
                    broker=broker,
                    symbol=symbol,
                    current_qty=float(qty),
                    remaining_qty=float(remaining_qty),
                    limit_px=float(limit_px),
                    client_order_id=client_oid,
                    broker_order_id=broker_oid,
                    attempts=attempts,
                    max_attempts=max_attempts,
                    aggressiveness=aggressiveness,
                    next_action_ts_ms=int(next_action_ts_ms),
                    replace_limit_fn=_native_replace_handler(broker),
                    meta=meta,
                )
                if bool(native_replace.get("replace_done")):
                    out["updated"] += 1
                    continue
                if bool(native_replace.get("attempted")) and not bool(native_replace.get("ok")):
                    out["errors"] += 1
                    out["updated"] += 1
                    continue

                cancel_gate = verify_cancel_before_replace(
                    con,
                    open_id=open_id,
                    now_ms=int(now),
                    broker=broker,
                    symbol=symbol,
                    open_qty=float(qty),
                    client_order_id=client_oid,
                    broker_order_id=broker_oid,
                    get_order_fn=get_order_fn,
                    cancel_order_fn=cancel_order_fn,
                    current_order=oinfo,
                    attempts=attempts,
                    max_attempts=max_attempts,
                    meta=meta,
                )
                if not bool(cancel_gate.get("ok")):
                    out["errors"] += 1
                    out["updated"] += 1
                    continue
                if not bool(cancel_gate.get("replace_allowed")):
                    out["updated"] += 1
                    continue

                verified_remaining_qty = float(cancel_gate.get("remaining_qty") or 0.0)
                if abs(float(verified_remaining_qty)) <= float(EPS_QTY):
                    out["updated"] += 1
                    continue

                _resubmit_order(open_id=open_id, broker=broker, symbol=symbol, side=side, remaining_qty=float(verified_remaining_qty), order_type=order_type, limit_px=(float(limit_px) if limit_px is not None else None), client_oid=client_oid, broker_oid=broker_oid, attempts=attempts, max_attempts=max_attempts, portfolio_orders_id=portfolio_orders_id, source_alert_id=source_alert_id, meta=meta, event_name="stale_retry", status_hint=str(cancel_gate.get("status") or st))
                out["updated"] += 1

            except _SubmissionUnrecorded as e:
                out["errors"] += 1
                try:
                    con.commit()
                except Exception as commit_exc:
                    _warn_nonfatal(
                        "EXEC_OPEN_ORDER_SUBMISSION_UNRECORDED_COMMIT_FAILED",
                        commit_exc,
                        once_key=f"submission_unrecorded_commit:{open_id}",
                        open_id=int(open_id),
                    )
                return {**out, "open_due": len(rows), **dict(e.result)}

            except _SubmissionUnknown as e:
                out["errors"] += 1
                try:
                    con.commit()
                except Exception as commit_exc:
                    _warn_nonfatal(
                        "EXEC_OPEN_ORDER_SUBMISSION_UNKNOWN_COMMIT_FAILED",
                        commit_exc,
                        once_key=f"submission_unknown_commit:{open_id}",
                        open_id=int(open_id),
                    )
                return {**out, "open_due": len(rows), **dict(e.result)}

            except Exception as e:
                _warn_nonfatal(
                    "EXEC_OPEN_ORDER_MANAGE_ROW_FAILED",
                    e,
                    once_key=f"manage_row:{open_id}",
                    open_id=int(open_id),
                    broker=str(broker),
                    broker_order_id=str(broker_oid or ""),
                )
                out["errors"] += 1
                continue

        con.commit()
        return {**out, "open_due": len(rows)}

    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EXEC_OPEN_ORDER_MANAGER_CLOSE_FAILED",
                e,
                once_key="manage_open_orders_close",
            )
