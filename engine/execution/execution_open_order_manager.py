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
from engine.execution.execution_ledger import log_submit
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
            SELECT id, broker, symbol, qty, order_type, aggressiveness, limit_px,
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
            from engine.execution.broker_alpaca_rest import submit_limit_order as alpaca_submit_limit_order
            from engine.execution.broker_alpaca_rest import submit_market_order as alpaca_submit_market_order
        except Exception:
            alpaca_get_order = None
            alpaca_cancel_order = None
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

        def _resubmit_order(*, open_id: int, broker: str, symbol: str, remaining_qty: float, order_type: str, limit_px: Optional[float], client_oid: str, attempts: int, max_attempts: int, portfolio_orders_id, source_alert_id, meta: Dict[str, Any], event_name: str, status_hint: str) -> None:
            # Retries are persisted as new submits so the ledger reflects the
            # actual broker-facing order lineage, not just the original intent.
            _get_order_fn, _cancel_order_fn, submit_limit_fn, submit_market_fn = _handlers(broker)
            next_attempt = int(attempts) + 1
            next_aggr = _next_aggressiveness(str(meta.get("aggressiveness") or ""), next_attempt, max_attempts)
            next_action_ts_ms = _next_action_ms(int(now), meta)

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
                res = submit_market_fn(symbol=symbol, qty=float(remaining_qty), client_oid=new_client_oid) or {}
                new_broker_oid = str(res.get("id") or "") or None
                if not new_broker_oid:
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=NULL,
                            attempts=?, order_type='MARKET', aggressiveness='MARKET', next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (int(now), float(remaining_qty), str(new_client_oid), int(next_attempt), int(next_action_ts_ms), int(open_id)),
                    )
                    _log_event(con, open_id, "market_submit_failed", {"attempt": int(next_attempt), "prev_client_order_id": client_oid, "event": event_name, "status": status_hint})
                    return

                try:
                    log_submit(
                        client_order_id=new_client_oid,
                        broker=str(broker),
                        symbol=symbol,
                        qty=float(remaining_qty),
                        submit_ts_ms=int(_now_ms()),
                        ref_px=float(limit_px) if limit_px is not None else 0.0,
                        broker_order_id=str(new_broker_oid),
                        portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                        source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                        extra={
                            "timeout_escalation": True,
                            "retry_event": str(event_name),
                            "retry_status": str(status_hint),
                            "escalated_from_client_order_id": client_oid,
                            "escalated_to_order_type": "MARKET",
                            "attempts": int(next_attempt),
                            "max_attempts": int(max_attempts),
                            "remaining_qty": float(remaining_qty),
                            "meta": meta,
                        },
                    )
                except Exception as e:
                    _warn_nonfatal(
                        "EXEC_OPEN_ORDER_MANAGER_MARKET_LOG_SUBMIT_FAILED",
                        e,
                        once_key="manage_open_orders_market_log_submit",
                        broker=str(broker),
                        symbol=str(symbol),
                        client_order_id=str(new_client_oid),
                        open_order_id=int(open_id),
                    )

                con.execute(
                    """
                    UPDATE exec_open_orders
                    SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?, attempts=?,
                        order_type='MARKET', aggressiveness='MARKET', status='escalated_market', next_action_ts_ms=0
                    WHERE id=?
                    """,
                    (int(now), float(remaining_qty), str(new_client_oid), str(new_broker_oid), int(next_attempt), int(open_id)),
                )
                _log_event(con, open_id, "escalated_market", {"attempt": int(next_attempt), "broker_order_id": new_broker_oid, "prev_client_order_id": client_oid, "remaining_qty": float(remaining_qty), "event": event_name, "status": status_hint})
                return

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
            res = submit_limit_fn(symbol=symbol, qty=float(remaining_qty), limit_price=float(next_limit), client_oid=new_client_oid) or {}
            new_broker_oid = str(res.get("id") or "") or None
            if not new_broker_oid:
                con.execute(
                    """
                    UPDATE exec_open_orders
                    SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=NULL, attempts=?,
                        aggressiveness=?, limit_px=?, next_action_ts_ms=?
                    WHERE id=?
                    """,
                    (int(now), float(remaining_qty), str(new_client_oid), int(next_attempt), str(next_aggr), float(next_limit), int(next_action_ts_ms), int(open_id)),
                )
                _log_event(con, open_id, "limit_replace_submit_failed", {"attempt": int(next_attempt), "limit_px": float(next_limit), "aggressiveness": str(next_aggr), "event": event_name, "status": status_hint})
                return

            try:
                log_submit(
                    client_order_id=new_client_oid,
                    broker=str(broker),
                    symbol=symbol,
                    qty=float(remaining_qty),
                    submit_ts_ms=int(_now_ms()),
                    ref_px=float(next_limit),
                    broker_order_id=str(new_broker_oid),
                    portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                    source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                    extra={
                        "timeout_escalation": True,
                        "retry_event": str(event_name),
                        "retry_status": str(status_hint),
                        "reprice_attempt": int(next_attempt),
                        "prev_client_order_id": client_oid,
                        "escalated_to_aggressiveness": str(next_aggr),
                        "remaining_qty": float(remaining_qty),
                        "meta": meta,
                    },
                )
            except Exception as e:
                _warn_nonfatal(
                    "EXEC_OPEN_ORDER_MANAGER_LIMIT_LOG_SUBMIT_FAILED",
                    e,
                    once_key="manage_open_orders_limit_log_submit",
                    broker=str(broker),
                    symbol=str(symbol),
                    client_order_id=str(new_client_oid),
                    open_order_id=int(open_id),
                )

            con.execute(
                """
                UPDATE exec_open_orders
                SET updated_ts_ms=?, qty=?, client_order_id=?, broker_order_id=?, attempts=?,
                    aggressiveness=?, limit_px=?, next_action_ts_ms=?
                WHERE id=?
                """,
                (int(now), float(remaining_qty), str(new_client_oid), str(new_broker_oid), int(next_attempt), str(next_aggr), float(next_limit), int(next_action_ts_ms), int(open_id)),
            )
            _log_event(con, open_id, "replaced", {"attempt": int(next_attempt), "limit_px": float(next_limit), "aggressiveness": str(next_aggr), "broker_order_id": new_broker_oid, "remaining_qty": float(remaining_qty), "event": event_name, "status": status_hint})

        for r in rows or []:
            out["managed"] += 1
            try:
                open_id = int(r[0])
                broker = str(r[1] or "").lower().strip()
                symbol = str(r[2] or "").upper().strip()
                qty = float(r[3] or 0.0)
                order_type = str(r[4] or "").upper().strip()
                aggressiveness = str(r[5] or "").upper().strip()
                limit_px = r[6]
                client_oid = str(r[7] or "")
                broker_oid = str(r[8] or "")
                attempts = int(r[9] or 0)
                max_attempts = int(r[10] or 0)
                portfolio_orders_id = r[12]
                source_alert_id = r[13]
                meta_json = str(r[14] or "{}")

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
                    elif max_attempts > 0 and attempts >= max_attempts:
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?, status='broker_missing', next_action_ts_ms=0
                            WHERE id=?
                            """,
                            (int(now), int(open_id)),
                        )
                        _log_event(con, open_id, "broker_order_missing", {"broker_order_id": broker_oid, "client_order_id": client_oid, "attempts": int(attempts), "max_attempts": int(max_attempts)})
                    else:
                        _log_event(con, open_id, "broker_order_missing", {"broker_order_id": broker_oid, "client_order_id": client_oid, "attempts": int(attempts), "max_attempts": int(max_attempts)})
                        _resubmit_order(open_id=open_id, broker=broker, symbol=symbol, remaining_qty=float(qty), order_type=order_type, limit_px=(float(limit_px) if limit_px is not None else None), client_oid=client_oid, attempts=attempts, max_attempts=max_attempts, portfolio_orders_id=portfolio_orders_id, source_alert_id=source_alert_id, meta=meta, event_name="broker_missing_retry", status_hint="missing")
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
                        _resubmit_order(open_id=open_id, broker=broker, symbol=symbol, remaining_qty=float(remaining_qty or qty), order_type=order_type, limit_px=(float(limit_px) if limit_px is not None else None), client_oid=client_oid, attempts=attempts, max_attempts=max_attempts, portfolio_orders_id=portfolio_orders_id, source_alert_id=source_alert_id, meta=meta, event_name="rejected_retry", status_hint=st)
                    out["updated"] += 1
                    continue

                if st in ("cancelled", "canceled"):
                    if max_attempts > 0 and attempts >= max_attempts:
                        _close_row(open_id, str(st), float(remaining_qty), {"status": st, "broker_order_id": broker_oid})
                    else:
                        _resubmit_order(open_id=open_id, broker=broker, symbol=symbol, remaining_qty=float(remaining_qty or qty), order_type=order_type, limit_px=(float(limit_px) if limit_px is not None else None), client_oid=client_oid, attempts=attempts, max_attempts=max_attempts, portfolio_orders_id=portfolio_orders_id, source_alert_id=source_alert_id, meta=meta, event_name="cancel_retry", status_hint=st)
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

                if cancel_order_fn:
                    try:
                        cancel_order_fn(str(broker_oid))
                    except Exception:
                        _log_event(con, open_id, "cancel_failed", {"broker_order_id": broker_oid})

                _resubmit_order(open_id=open_id, broker=broker, symbol=symbol, remaining_qty=float(remaining_qty), order_type=order_type, limit_px=(float(limit_px) if limit_px is not None else None), client_oid=client_oid, attempts=attempts, max_attempts=max_attempts, portfolio_orders_id=portfolio_orders_id, source_alert_id=source_alert_id, meta=meta, event_name="stale_retry", status_hint=st)
                out["updated"] += 1

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
