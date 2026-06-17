"""
FILE: execution_microstructure.py

Execution subsystem module for `execution_microstructure`.
"""

# dev_core/execution_microstructure.py
"""
Phase 2: Execution Microstructure Layer

Responsibilities:
- Maintain open order registry (cancel/replace state)
- Reprice limit orders based on attempts + aggressiveness
- Fail-soft: never throws; never blocks other jobs
"""

import json
import logging
import os
import time
from typing import Any, Dict, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect
from engine.execution.broker_submission_recovery import record_submission_unrecorded
from engine.execution.execution_ledger import log_submit

REPRICE_INTERVAL_S = float(os.environ.get("EPE_REPRICE_INTERVAL_S", "60"))
REPRICE_STEP_BPS = float(os.environ.get("EPE_REPRICE_STEP_BPS", "5.0"))
MAX_OPEN_ORDERS_PER_PASS = int(os.environ.get("EPE_MAX_OPEN_ORDERS_PER_PASS", "50"))
MIN_REPLACE_INTERVAL_S = float(os.environ.get("EPE_MIN_REPLACE_INTERVAL_S", "15"))
EPS_QTY = float(os.environ.get("EPE_QTY_EPS", "0.000001"))
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


class _SubmissionUnrecorded(RuntimeError):
    def __init__(self, result: Dict[str, Any]) -> None:
        super().__init__(str((result or {}).get("status") or "submission_unrecorded"))
        self.result = dict(result or {})


def _ensure_tables(con) -> None:
    # These tables are the persistent state machine for open-order management
    # and retry lineage across poll cycles and process restarts.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS exec_open_orders (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL,
          broker TEXT NOT NULL,
          symbol TEXT NOT NULL,
          qty REAL NOT NULL,
          side TEXT,
          order_type TEXT NOT NULL,
          aggressiveness TEXT,
          limit_px REAL,
          client_order_id TEXT,
          broker_order_id TEXT,
          status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 0,
          next_action_ts_ms INTEGER NOT NULL DEFAULT 0,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          meta_json TEXT NOT NULL
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_exec_open_orders_status_next ON exec_open_orders(status, next_action_ts_ms)"
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_exec_open_orders_broker ON exec_open_orders(broker)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_exec_open_orders_symbol ON exec_open_orders(symbol)")
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_exec_open_orders_client ON exec_open_orders(client_order_id)")

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS exec_order_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          open_order_id INTEGER,
          event TEXT NOT NULL,
          details_json TEXT NOT NULL
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_exec_order_events_ts ON exec_order_events(ts_ms)")


def record_open_order(
    *,
    broker: str,
    symbol: str,
    qty: float,
    order_type: str,
    aggressiveness: str,
    limit_px: Optional[float],
    client_order_id: str,
    broker_order_id: Optional[str],
    max_attempts: int,
    portfolio_orders_id: Optional[int] = None,
    source_alert_id: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Best-effort open-order registry insert."""
    con = connect()
    try:
        _ensure_tables(con)
        now = _now_ms()
        meta_obj = dict(meta or {})
        # Upsert by client_order_id so repeated submissions/recovery updates
        # converge on one open-order row instead of fragmenting state.
        con.execute(
            """
            INSERT INTO exec_open_orders(
              ts_ms, updated_ts_ms, broker, symbol, qty, side, order_type, aggressiveness,
              limit_px, client_order_id, broker_order_id, status, attempts, max_attempts,
              next_action_ts_ms, portfolio_orders_id, source_alert_id, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(client_order_id) DO UPDATE SET
              updated_ts_ms=excluded.updated_ts_ms,
              broker=excluded.broker,
              symbol=excluded.symbol,
              qty=excluded.qty,
              side=excluded.side,
              order_type=excluded.order_type,
              aggressiveness=excluded.aggressiveness,
              limit_px=excluded.limit_px,
              broker_order_id=COALESCE(excluded.broker_order_id, exec_open_orders.broker_order_id),
              status='open',
              max_attempts=excluded.max_attempts,
              next_action_ts_ms=excluded.next_action_ts_ms,
              portfolio_orders_id=COALESCE(excluded.portfolio_orders_id, exec_open_orders.portfolio_orders_id),
              source_alert_id=COALESCE(excluded.source_alert_id, exec_open_orders.source_alert_id),
              meta_json=excluded.meta_json
            """,
            (
                now,
                now,
                str(broker),
                str(symbol),
                float(qty),
                ("BUY" if float(qty) > 0 else "SELL"),
                str(order_type),
                str(aggressiveness or ""),
                float(limit_px) if limit_px is not None else None,
                str(client_order_id),
                str(broker_order_id) if broker_order_id else None,
                "open",
                0,
                int(max_attempts),
                _next_action_ms(int(now), meta_obj),
                int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                int(source_alert_id) if source_alert_id is not None else None,
                json.dumps(meta_obj, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    except Exception as exc:
        _warn_nonfatal(
            "execution_microstructure_record_open_order_failed",
            "EXECUTION_MICROSTRUCTURE_RECORD_OPEN_ORDER_FAILED",
            exc,
            broker=str(broker),
            symbol=str(symbol),
            client_order_id=str(client_order_id),
        )
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "execution_microstructure_record_open_order_close_failed",
                "EXECUTION_MICROSTRUCTURE_RECORD_OPEN_ORDER_CLOSE_FAILED",
                exc,
                warn_key="execution_microstructure_record_open_order_close_failed",
            )


def _log_event(con, open_order_id: int, event: str, details: Dict[str, Any]) -> None:
    try:
        con.execute(
            """
            INSERT INTO exec_order_events(ts_ms, open_order_id, event, details_json)
            VALUES (?,?,?,?)
            """,
            (
                _now_ms(),
                int(open_order_id),
                str(event),
                json.dumps(details or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
    except Exception as exc:
        _warn_nonfatal(
            "execution_microstructure_log_event_failed",
            "EXECUTION_MICROSTRUCTURE_LOG_EVENT_FAILED",
            exc,
            open_order_id=int(open_order_id),
            event_name=str(event),
        )


def _adjust_limit_px(limit_px: float, qty: float, attempt: int) -> float:
    # attempt=1 means first reprice (move further toward market)
    step = float(REPRICE_STEP_BPS) * float(max(1, attempt))
    if float(qty) > 0:
        # buy: increase limit to be more aggressive
        return float(limit_px) * (1.0 + (step / 10000.0))
    # sell: decrease limit to be more aggressive
    return float(limit_px) * (1.0 - (step / 10000.0))


def _next_aggressiveness(current: str, attempt: int, max_attempts: int) -> str:
    # Escalation is monotonic: passive -> neutral -> aggressive -> market.
    # The retry loop should never become less aggressive over time.
    cur = str(current or "").upper().strip()
    if cur in ("", "PASSIVE"):
        return "NEUTRAL"
    if cur == "NEUTRAL":
        return "AGGRESSIVE"
    if cur == "AGGRESSIVE":
        return "MARKET"
    if int(max_attempts) > 0 and int(attempt) >= int(max_attempts):
        return "MARKET"
    return cur or "MARKET"


def _meta_timeout_s(meta: Optional[Dict[str, Any]]) -> float:
    try:
        v = float((meta or {}).get("escalation_timeout_s") or REPRICE_INTERVAL_S)
    except Exception:
        v = float(REPRICE_INTERVAL_S)
    return float(max(float(MIN_REPLACE_INTERVAL_S), float(v)))


def _next_action_ms(now_ms: int, meta: Optional[Dict[str, Any]]) -> int:
    return int(now_ms + (_meta_timeout_s(meta) * 1000.0))


def _remaining_qty(open_qty: float, oinfo: Dict[str, Any]) -> float:
    try:
        broker_qty = float(oinfo.get("qty") or 0.0)
    except Exception:
        broker_qty = 0.0

    try:
        filled_qty = float(oinfo.get("filled_qty") or 0.0)
    except Exception:
        filled_qty = 0.0

    base_abs = abs(float(broker_qty)) if abs(float(broker_qty)) > 0.0 else abs(float(open_qty))
    rem_abs = max(0.0, float(base_abs) - abs(float(filled_qty)))
    if rem_abs <= float(EPS_QTY):
        return 0.0

    side = str(oinfo.get("side") or "").lower().strip()
    if side == "sell":
        return -float(rem_abs)
    if side == "buy":
        return float(rem_abs)
    return float(rem_abs) if float(open_qty) >= 0.0 else -float(rem_abs)


def _is_open_like_status(status: str) -> bool:
    s = str(status or "").lower().strip()
    return s in {
        "",
        "new",
        "accepted",
        "pending_new",
        "accepted_for_bidding",
        "partially_filled",
        "done_for_day",
        "calculated",
        "open",
    }


def manage_open_orders() -> Dict[str, Any]:
    """
    Called periodically (e.g. from execution_poll_and_attrib.py).
    - For Alpaca only (today): checks open orders; cancel/replace as needed.
    This module is fail-soft by design; errors should quarantine order state,
    not crash the broader execution maintenance loop.
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

        def _raise_submission_unrecorded(
            *,
            open_id: int,
            symbol: str,
            qty: float,
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
                broker="alpaca",
                symbol=str(symbol),
                qty=float(qty),
                order_uid=f"microstructure:alpaca:{str(client_order_id)}",
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
                    "execution_microstructure_submission_unrecorded_row_update_failed",
                    "EXECUTION_MICROSTRUCTURE_SUBMISSION_UNRECORDED_ROW_UPDATE_FAILED",
                    row_exc,
                    warn_key=f"execution_microstructure_submission_unrecorded_row:{open_id}:{client_order_id}",
                    symbol=str(symbol),
                    client_order_id=str(client_order_id),
                    broker_order_id=str(broker_order_id or ""),
                    open_order_id=int(open_id),
                )
            raise _SubmissionUnrecorded(result)

        # lazy import to avoid cycles
        try:
            from engine.execution.broker_alpaca_rest import get_order as alpaca_get_order
            from engine.execution.broker_alpaca_rest import cancel_order as alpaca_cancel_order
            from engine.execution.broker_alpaca_rest import submit_limit_order as alpaca_submit_limit_order
            from engine.execution.broker_alpaca_rest import submit_market_order as alpaca_submit_market_order
        except Exception as exc:
            _warn_nonfatal(
                "execution_microstructure_alpaca_import_failed",
                "EXECUTION_MICROSTRUCTURE_ALPACA_IMPORT_FAILED",
                exc,
                warn_key="execution_microstructure_alpaca_import_failed",
            )
            alpaca_get_order = None
            alpaca_cancel_order = None
            alpaca_submit_limit_order = None
            alpaca_submit_market_order = None

        for r in rows or []:
            out["managed"] += 1
            try:
                open_id = int(r[0])
                broker = str(r[1] or "").lower().strip()
                symbol = str(r[2] or "").upper().strip()
                qty = float(r[3] or 0.0)
                order_type = str(r[4] or "").upper().strip()
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
                next_action_ts_ms = _next_action_ms(int(now), meta)

                if broker != "alpaca":
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

                if not alpaca_get_order or not alpaca_cancel_order or not alpaca_submit_limit_order:
                    out["errors"] += 1
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (int(now), int(next_action_ts_ms), int(open_id)),
                    )
                    continue

                if not broker_oid:
                    new_client_oid = f"{client_oid}_retry{attempts + 1}"
                    new_broker_oid = None
                    submit_ok = False

                    try:
                        if order_type == "LIMIT" and limit_px is not None:
                            res = alpaca_submit_limit_order(
                                symbol=symbol,
                                qty=float(qty),
                                limit_price=float(limit_px),
                                client_oid=new_client_oid,
                            ) or {}
                            new_broker_oid = str(res.get("id") or "") or None
                            submit_ok = bool(new_broker_oid)
                        elif order_type == "MARKET" and alpaca_submit_market_order:
                            res = alpaca_submit_market_order(
                                symbol=symbol,
                                qty=float(qty),
                                client_oid=new_client_oid,
                            ) or {}
                            new_broker_oid = str(res.get("id") or "") or None
                            submit_ok = bool(new_broker_oid)
                    except Exception as exc:
                        _warn_nonfatal(
                            "execution_microstructure_missing_broker_order_resubmit_failed",
                            "EXECUTION_MICROSTRUCTURE_MISSING_BROKER_ORDER_RESUBMIT_FAILED",
                            exc,
                            symbol=symbol,
                            client_order_id=client_oid,
                            order_type=order_type,
                        )
                        submit_ok = False

                    if submit_ok:
                        submit_ts_ms = int(_now_ms())
                        retry_payload = {
                            "timeout_escalation": True,
                            "retry_missing_broker_order_id": True,
                            "prev_client_order_id": client_oid,
                            "meta": meta,
                        }
                        try:
                            log_submit(
                                client_order_id=new_client_oid,
                                broker="alpaca",
                                symbol=symbol,
                                qty=float(qty),
                                submit_ts_ms=int(submit_ts_ms),
                                ref_px=float(limit_px) if limit_px is not None else 0.0,
                                broker_order_id=str(new_broker_oid) if new_broker_oid else None,
                                portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                                source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                                extra=retry_payload,
                            )
                        except Exception as exc:
                            _warn_nonfatal(
                                "execution_microstructure_resubmit_log_submit_failed",
                                "EXECUTION_MICROSTRUCTURE_RESUBMIT_LOG_SUBMIT_FAILED",
                                exc,
                                symbol=symbol,
                                client_order_id=new_client_oid,
                                broker_order_id=new_broker_oid,
                            )
                            _raise_submission_unrecorded(
                                open_id=open_id,
                                symbol=symbol,
                                qty=float(qty),
                                client_order_id=new_client_oid,
                                broker_order_id=new_broker_oid,
                                submit_ts_ms=int(submit_ts_ms),
                                attempts=int(attempts + 1),
                                portfolio_orders_id=portfolio_orders_id,
                                source_alert_id=source_alert_id,
                                payload=retry_payload,
                                error=exc,
                                stage="log_submit",
                            )

                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?,
                                client_order_id=?,
                                broker_order_id=?,
                                attempts=?,
                                next_action_ts_ms=?
                            WHERE id=?
                            """,
                            (
                                int(now),
                                str(new_client_oid),
                                str(new_broker_oid),
                                int(attempts + 1),
                                int(next_action_ts_ms),
                                int(open_id),
                            ),
                        )
                        _log_event(
                            con,
                            open_id,
                            "resubmitted_missing_broker_order_id",
                            {"client_order_id": new_client_oid, "broker_order_id": new_broker_oid},
                        )
                    else:
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?, next_action_ts_ms=?
                            WHERE id=?
                            """,
                            (int(now), int(next_action_ts_ms), int(open_id)),
                        )
                        _log_event(con, open_id, "missing_broker_order_id", {"client_order_id": client_oid})

                    out["updated"] += 1
                    continue

                try:
                    oinfo = alpaca_get_order(str(broker_oid)) or {}
                except Exception as exc:
                    _warn_nonfatal(
                        "execution_microstructure_get_order_failed",
                        "EXECUTION_MICROSTRUCTURE_GET_ORDER_FAILED",
                        exc,
                        symbol=symbol,
                        broker_order_id=broker_oid,
                        open_order_id=int(open_id),
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

                st = str(oinfo.get("status") or "").lower().strip()
                remaining_qty = _remaining_qty(float(qty), oinfo)

                if st in ("rejected", "expired", "canceled"):
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, status=?, next_action_ts_ms=0
                        WHERE id=?
                        """,
                        (int(now), str(st), int(open_id)),
                    )
                    _log_event(con, open_id, "closed", {"status": st, "broker_order_id": broker_oid})
                    out["updated"] += 1
                    continue

                if st == "filled" or abs(float(remaining_qty)) <= float(EPS_QTY):
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?, qty=?, status='filled', next_action_ts_ms=0
                        WHERE id=?
                        """,
                        (int(now), 0.0, int(open_id)),
                    )
                    _log_event(
                        con,
                        open_id,
                        "closed",
                        {"status": "filled", "broker_order_id": broker_oid, "filled_qty": oinfo.get("filled_qty")},
                    )
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

                next_attempt = attempts + 1
                next_aggr = _next_aggressiveness(str(r[5] or ""), next_attempt, max_attempts)

                try:
                    alpaca_cancel_order(str(broker_oid))
                except Exception as exc:
                    _warn_nonfatal(
                        "execution_microstructure_cancel_failed",
                        "EXECUTION_MICROSTRUCTURE_CANCEL_FAILED",
                        exc,
                        symbol=symbol,
                        broker_order_id=broker_oid,
                        open_order_id=int(open_id),
                    )
                    _log_event(con, open_id, "cancel_failed", {"broker_order_id": broker_oid})

                if max_attempts > 0 and attempts >= max_attempts:
                    if not alpaca_submit_market_order:
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?, qty=?, status='gave_up', next_action_ts_ms=0
                            WHERE id=?
                            """,
                            (int(now), float(remaining_qty), int(open_id)),
                        )
                        _log_event(con, open_id, "gave_up", {"attempts": attempts, "max_attempts": max_attempts})
                        out["updated"] += 1
                        continue

                    new_client_oid = f"{client_oid}_mkt"
                    res = alpaca_submit_market_order(
                        symbol=symbol,
                        qty=float(remaining_qty),
                        client_oid=new_client_oid,
                    ) or {}
                    new_broker_oid = str(res.get("id") or "") or None

                    if not new_broker_oid:
                        con.execute(
                            """
                            UPDATE exec_open_orders
                            SET updated_ts_ms=?,
                                qty=?,
                                client_order_id=?,
                                broker_order_id=NULL,
                                attempts=?,
                                order_type='MARKET',
                                aggressiveness='MARKET',
                                next_action_ts_ms=?
                            WHERE id=?
                            """,
                            (
                                int(now),
                                float(remaining_qty),
                                str(new_client_oid),
                                int(next_attempt),
                                int(next_action_ts_ms),
                                int(open_id),
                            ),
                        )
                        _log_event(
                            con,
                            open_id,
                            "market_submit_failed",
                            {"attempt": int(next_attempt), "prev_client_order_id": client_oid},
                        )
                        out["updated"] += 1
                        continue

                    submit_ts_ms = int(_now_ms())
                    market_payload = {
                        "timeout_escalation": True,
                        "escalated_from_client_order_id": client_oid,
                        "escalated_from_order_type": "LIMIT",
                        "escalated_from_aggressiveness": str(r[5] or ""),
                        "escalated_to_order_type": "MARKET",
                        "attempts": int(attempts),
                        "max_attempts": int(max_attempts),
                        "remaining_qty": float(remaining_qty),
                        "meta": meta,
                    }
                    try:
                        log_submit(
                            client_order_id=new_client_oid,
                            broker="alpaca",
                            symbol=symbol,
                            qty=float(remaining_qty),
                            submit_ts_ms=int(submit_ts_ms),
                            ref_px=float(limit_px),
                            broker_order_id=str(new_broker_oid) if new_broker_oid else None,
                            portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                            source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                            extra=market_payload,
                        )
                    except Exception as exc:
                        _warn_nonfatal(
                            "execution_microstructure_market_escalation_log_submit_failed",
                            "EXECUTION_MICROSTRUCTURE_MARKET_ESCALATION_LOG_SUBMIT_FAILED",
                            exc,
                            symbol=symbol,
                            client_order_id=new_client_oid,
                            broker_order_id=new_broker_oid,
                        )
                        _raise_submission_unrecorded(
                            open_id=open_id,
                            symbol=symbol,
                            qty=float(remaining_qty),
                            client_order_id=new_client_oid,
                            broker_order_id=new_broker_oid,
                            submit_ts_ms=int(submit_ts_ms),
                            attempts=int(next_attempt),
                            portfolio_orders_id=portfolio_orders_id,
                            source_alert_id=source_alert_id,
                            payload=market_payload,
                            error=exc,
                            stage="log_submit",
                        )

                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?,
                            qty=?,
                            client_order_id=?,
                            broker_order_id=?,
                            attempts=?,
                            order_type='MARKET',
                            aggressiveness='MARKET',
                            status='escalated_market',
                            next_action_ts_ms=0
                        WHERE id=?
                        """,
                        (
                            int(now),
                            float(remaining_qty),
                            str(new_client_oid),
                            str(new_broker_oid) if new_broker_oid else None,
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
                        },
                    )
                    out["updated"] += 1
                    continue

                if not alpaca_submit_limit_order:
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

                new_limit = _adjust_limit_px(float(limit_px), float(remaining_qty), next_attempt)
                new_client_oid = f"{client_oid}_r{next_attempt}"

                res = alpaca_submit_limit_order(
                    symbol=symbol,
                    qty=float(remaining_qty),
                    limit_price=float(new_limit),
                    client_oid=new_client_oid,
                ) or {}
                new_broker_oid = str(res.get("id") or "") or None

                if not new_broker_oid:
                    con.execute(
                        """
                        UPDATE exec_open_orders
                        SET updated_ts_ms=?,
                            qty=?,
                            client_order_id=?,
                            broker_order_id=NULL,
                            attempts=?,
                            aggressiveness=?,
                            limit_px=?,
                            next_action_ts_ms=?
                        WHERE id=?
                        """,
                        (
                            int(now),
                            float(remaining_qty),
                            str(new_client_oid),
                            int(next_attempt),
                            str(next_aggr),
                            float(new_limit),
                            int(next_action_ts_ms),
                            int(open_id),
                        ),
                    )
                    _log_event(
                        con,
                        open_id,
                        "limit_replace_submit_failed",
                        {
                            "attempt": int(next_attempt),
                            "limit_px": float(new_limit),
                            "aggressiveness": str(next_aggr),
                        },
                    )
                    out["updated"] += 1
                    continue

                submit_ts_ms = int(_now_ms())
                limit_payload = {
                    "timeout_escalation": True,
                    "reprice_attempt": int(next_attempt),
                    "prev_client_order_id": client_oid,
                    "escalated_from_aggressiveness": str(r[5] or ""),
                    "escalated_to_aggressiveness": str(next_aggr),
                    "remaining_qty": float(remaining_qty),
                    "meta": meta,
                }
                try:
                    log_submit(
                        client_order_id=new_client_oid,
                        broker="alpaca",
                        symbol=symbol,
                        qty=float(remaining_qty),
                        submit_ts_ms=int(submit_ts_ms),
                        ref_px=float(new_limit),
                        broker_order_id=str(new_broker_oid) if new_broker_oid else None,
                        portfolio_orders_id=int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                        source_alert_id=int(source_alert_id) if source_alert_id is not None else None,
                        extra=limit_payload,
                    )
                except Exception as exc:
                    _warn_nonfatal(
                        "execution_microstructure_limit_replace_log_submit_failed",
                        "EXECUTION_MICROSTRUCTURE_LIMIT_REPLACE_LOG_SUBMIT_FAILED",
                        exc,
                        symbol=symbol,
                        client_order_id=new_client_oid,
                        broker_order_id=new_broker_oid,
                    )
                    _raise_submission_unrecorded(
                        open_id=open_id,
                        symbol=symbol,
                        qty=float(remaining_qty),
                        client_order_id=new_client_oid,
                        broker_order_id=new_broker_oid,
                        submit_ts_ms=int(submit_ts_ms),
                        attempts=int(next_attempt),
                        portfolio_orders_id=portfolio_orders_id,
                        source_alert_id=source_alert_id,
                        payload=limit_payload,
                        error=exc,
                        stage="log_submit",
                    )

                con.execute(
                    """
                    UPDATE exec_open_orders
                    SET updated_ts_ms=?,
                        qty=?,
                        client_order_id=?,
                        broker_order_id=?,
                        attempts=?,
                        aggressiveness=?,
                        limit_px=?,
                        next_action_ts_ms=?
                    WHERE id=?
                    """,
                    (
                        int(now),
                        float(remaining_qty),
                        str(new_client_oid),
                        str(new_broker_oid) if new_broker_oid else None,
                        int(next_attempt),
                        str(next_aggr),
                        float(new_limit),
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
                        "limit_px": float(new_limit),
                        "aggressiveness": str(next_aggr),
                        "broker_order_id": new_broker_oid,
                        "remaining_qty": float(remaining_qty),
                        "status": st,
                    },
                )
                out["updated"] += 1

            except _SubmissionUnrecorded as exc:
                out["errors"] += 1
                try:
                    con.commit()
                except Exception as commit_exc:
                    _warn_nonfatal(
                        "execution_microstructure_submission_unrecorded_commit_failed",
                        "EXECUTION_MICROSTRUCTURE_SUBMISSION_UNRECORDED_COMMIT_FAILED",
                        commit_exc,
                        warn_key=f"execution_microstructure_submission_unrecorded_commit:{r[0] if r else 'unknown'}",
                    )
                return {**out, "open_due": len(rows), **dict(exc.result)}

            except Exception as exc:
                _warn_nonfatal(
                    "execution_microstructure_manage_open_order_failed",
                    "EXECUTION_MICROSTRUCTURE_MANAGE_OPEN_ORDER_FAILED",
                    exc,
                    open_order_id=int(r[0]) if r and r[0] is not None else None,
                )
                out["errors"] += 1
                continue

        con.commit()
        return {**out, "open_due": len(rows)}

    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "execution_microstructure_manage_open_orders_close_failed",
                "EXECUTION_MICROSTRUCTURE_MANAGE_OPEN_ORDERS_CLOSE_FAILED",
                exc,
                warn_key="execution_microstructure_manage_open_orders_close_failed",
            )
