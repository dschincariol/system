"""
FILE: order_idempotency.py

Execution subsystem module for `order_idempotency`.
"""

import hashlib
import json
import logging
import threading
import time
from typing import Any, Dict, Optional

from engine.runtime import dbapi_compat as dbapi
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

SCHEMA = """
CREATE TABLE IF NOT EXISTS execution_order_idempotency (
  order_uid TEXT PRIMARY KEY,
  broker TEXT NOT NULL,
  portfolio_orders_id INTEGER,
  portfolio_ts_ms INTEGER,
  source_order_id INTEGER,
  source_alert_id INTEGER,
  symbol TEXT NOT NULL,
  client_order_id TEXT NOT NULL,
  broker_order_id TEXT,
  status TEXT NOT NULL,
  first_seen_ts_ms INTEGER NOT NULL,
  claimed_ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  submit_ts_ms INTEGER,
  last_error TEXT,
  payload_json TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_order_idempotency_client
  ON execution_order_idempotency(client_order_id);

CREATE INDEX IF NOT EXISTS idx_execution_order_idempotency_status
  ON execution_order_idempotency(status);

CREATE INDEX IF NOT EXISTS idx_execution_order_idempotency_symbol_ts
  ON execution_order_idempotency(symbol, updated_ts_ms);
"""
_WARNED_NONFATAL_KEYS: set[str] = set()
_WARNED_NONFATAL_LOCK = threading.Lock()
LOG = get_logger("execution.order_idempotency")


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key:
        with _WARNED_NONFATAL_LOCK:
            if once_key in _WARNED_NONFATAL_KEYS:
                return
    log_failure(
        LOG,
        event="execution_order_idempotency_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.order_idempotency",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        with _WARNED_NONFATAL_LOCK:
            _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_i(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception as e:
        _warn_nonfatal(
            "ORDER_IDEMPOTENCY_SAFE_INT_FAILED",
            e,
            once_key="safe_int",
            value=repr(v)[:120],
        )
        return None


def _safe_f(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "ORDER_IDEMPOTENCY_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value=repr(v)[:120],
        )
        return None


def _canon_str(v: Any) -> str:
    return str(v or "").strip()


def _canon_upper(v: Any) -> str:
    return str(v or "").strip().upper()


def _canon_float(v: Any, ndigits: int = 12) -> Optional[float]:
    x = _safe_f(v)
    if x is None:
        return None
    return round(float(x), int(ndigits))


def _commit_if_outermost(con, had_transaction: bool) -> None:
    if not bool(had_transaction):
        con.commit()


def init_order_idempotency(con) -> None:
    had_transaction = bool(getattr(con, "in_transaction", False))
    con.executescript(SCHEMA)
    _commit_if_outermost(con, had_transaction)


def _canonical_order_payload(
    *,
    broker: str,
    portfolio_orders_id: Optional[int],
    portfolio_ts_ms: Optional[int],
    order: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    o = dict(order or {})
    # Canonicalization removes presentation noise so semantically identical
    # orders hash to the same UID across retries/restarts.
    return {
        "broker": _canon_str(broker).lower(),
        "portfolio_orders_id": _safe_i(portfolio_orders_id),
        "portfolio_ts_ms": _safe_i(portfolio_ts_ms),
        "source_order_id": _safe_i(o.get("source_order_id")),
        "source_alert_id": _safe_i(o.get("source_alert_id")),
        "symbol": _canon_upper(o.get("symbol")),
        "action": _canon_upper(o.get("action")),
        "from_side": _canon_upper(o.get("from_side")),
        "to_side": _canon_upper(o.get("to_side")),
        "to_weight": _canon_float(o.get("to_weight")),
        "qty": _canon_float(o.get("qty")),
        "order_type": _canon_upper(o.get("order_type") or "MARKET"),
        "aggressiveness": _canon_upper(o.get("aggressiveness") or "AGGRESSIVE"),
        "limit_px": _canon_float(o.get("limit_px")),
        "ttl_ms": _safe_i(o.get("ttl_ms")),
        "strategy_name": _canon_str(o.get("strategy_name")),
    }


def compute_order_uid(
    *,
    broker: str,
    portfolio_orders_id: Optional[int],
    portfolio_ts_ms: Optional[int],
    order: Optional[Dict[str, Any]],
) -> str:
    payload = _canonical_order_payload(
        broker=str(broker or ""),
        portfolio_orders_id=portfolio_orders_id,
        portfolio_ts_ms=portfolio_ts_ms,
        order=order,
    )
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def make_client_order_id(order_uid: str, broker: str) -> str:
    prefix = _canon_str(broker).lower()[:3] or "ord"
    return f"{prefix}_{str(order_uid)[:40]}"


def claim_order_submission(
    *,
    con,
    broker: str,
    portfolio_orders_id: Optional[int],
    portfolio_ts_ms: Optional[int],
    order: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    init_order_idempotency(con)

    payload = _canonical_order_payload(
        broker=str(broker or ""),
        portfolio_orders_id=portfolio_orders_id,
        portfolio_ts_ms=portfolio_ts_ms,
        order=order,
    )
    order_uid = compute_order_uid(
        broker=str(broker or ""),
        portfolio_orders_id=portfolio_orders_id,
        portfolio_ts_ms=portfolio_ts_ms,
        order=order,
    )
    client_order_id = make_client_order_id(order_uid, broker=str(broker or ""))
    now_ms = _now_ms()
    had_transaction = bool(getattr(con, "in_transaction", False))

    try:
        insert_cur = con.execute(
            """
            INSERT INTO execution_order_idempotency(
              order_uid,
              broker,
              portfolio_orders_id,
              portfolio_ts_ms,
              source_order_id,
              source_alert_id,
              symbol,
              client_order_id,
              broker_order_id,
              status,
              first_seen_ts_ms,
              claimed_ts_ms,
              updated_ts_ms,
              submit_ts_ms,
              last_error,
              payload_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_uid) DO NOTHING
            """,
            (
                str(order_uid),
                str(broker or "").lower(),
                _safe_i(portfolio_orders_id),
                _safe_i(portfolio_ts_ms),
                _safe_i(payload.get("source_order_id")),
                _safe_i(payload.get("source_alert_id")),
                str(payload.get("symbol") or ""),
                str(client_order_id),
                None,
                "claimed",
                int(now_ms),
                int(now_ms),
                int(now_ms),
                None,
                None,
                json.dumps(payload, separators=(",", ":"), sort_keys=True),
            ),
        )
        if int(getattr(insert_cur, "rowcount", 0) or 0) > 0:
            _commit_if_outermost(con, had_transaction)
            return {
                "ok": True,
                "duplicate": False,
                "order_uid": str(order_uid),
                "client_order_id": str(client_order_id),
                "status": "claimed",
            }
        raise dbapi.IntegrityError("duplicate claim")
    except dbapi.IntegrityError:
        # Duplicate claims resolve to the previously stored row so callers can
        # resume idempotently instead of throwing on restart.
        _warn_nonfatal(
            "ORDER_IDEMPOTENCY_DUPLICATE_CLAIM",
            dbapi.IntegrityError("duplicate claim"),
            once_key=f"duplicate_claim:{order_uid}",
            order_uid=str(order_uid),
            client_order_id=str(client_order_id),
        )
        row = con.execute(
            """
            SELECT order_uid, client_order_id, broker_order_id, status, submit_ts_ms
            FROM execution_order_idempotency
            WHERE order_uid=?
            LIMIT 1
            """,
            (str(order_uid),),
        ).fetchone()
        if not row:
            return {
                "ok": False,
                "order_uid": str(order_uid),
                "client_order_id": str(client_order_id),
                "status": "duplicate_exists_but_unreadable",
            }
        return {
            "ok": True,
            "duplicate": True,
            "order_uid": str(row[0]) if row[0] is not None else str(order_uid),
            "client_order_id": str(row[1]) if row[1] is not None else str(client_order_id),
            "broker_order_id": str(row[2]) if row[2] is not None else None,
            "status": str(row[3]) if row[3] is not None else "claimed",
            "submit_ts_ms": (int(row[4]) if row[4] is not None else None),
        }


def mark_order_submission_submitted(
    *,
    con,
    order_uid: str,
    client_order_id: str,
    broker_order_id: Optional[str],
    submit_ts_ms: int,
) -> None:
    init_order_idempotency(con)
    now_ms = _now_ms()
    had_transaction = bool(getattr(con, "in_transaction", False))
    con.execute(
        """
        UPDATE execution_order_idempotency
        SET client_order_id=?,
            broker_order_id=?,
            status='submitted',
            submit_ts_ms=?,
            updated_ts_ms=?,
            last_error=NULL
        WHERE order_uid=?
        """,
        (
            str(client_order_id),
            (str(broker_order_id) if broker_order_id is not None else None),
            int(submit_ts_ms),
            int(now_ms),
            str(order_uid),
        ),
    )
    _commit_if_outermost(con, had_transaction)


def mark_order_submission_unknown(
    *,
    con,
    order_uid: str,
    last_error: str,
) -> None:
    init_order_idempotency(con)
    now_ms = _now_ms()
    had_transaction = bool(getattr(con, "in_transaction", False))
    con.execute(
        """
        UPDATE execution_order_idempotency
        SET status='submit_inflight_unknown',
            updated_ts_ms=?,
            last_error=?
        WHERE order_uid=?
        """,
        (
            int(now_ms),
            str(last_error or ""),
            str(order_uid),
        ),
    )
    _commit_if_outermost(con, had_transaction)
