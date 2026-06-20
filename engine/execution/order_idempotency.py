"""
FILE: order_idempotency.py

Execution subsystem module for `order_idempotency`.
"""

import hashlib
import inspect
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

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
  parent_order_id INTEGER,
  slice_index INTEGER,
  slice_count INTEGER,
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
SUBMISSION_UNRECORDED_STATUS = "submission_unrecorded"


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


def _first_present(o: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in o and o.get(key) not in (None, ""):
            return o.get(key)
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


def _connect_fn_accepts_readonly(connect_fn: Callable[..., Any]) -> bool:
    accepts_readonly = True
    try:
        signature = inspect.signature(connect_fn)
    except (TypeError, ValueError) as exc:
        _warn_nonfatal(
            "ORDER_IDEMPOTENCY_CONNECT_SIGNATURE_INSPECT_FAILED",
            exc,
            once_key="connect_signature_inspect_failed",
            connect_fn=repr(connect_fn)[:200],
        )
    else:
        accepts_readonly = False
        for param in signature.parameters.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                accepts_readonly = True
                break
            if param.name == "readonly" and param.kind in {
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            }:
                accepts_readonly = True
                break
    return bool(accepts_readonly)


def _open_dedicated_connection(connect_fn: Optional[Callable[..., Any]] = None):
    if connect_fn is None:
        from engine.runtime.storage import connect as storage_connect

        connect_fn = storage_connect
    if _connect_fn_accepts_readonly(connect_fn):
        return connect_fn(readonly=False)
    _warn_nonfatal(
        "ORDER_IDEMPOTENCY_CONNECT_READONLY_ARG_UNSUPPORTED",
        RuntimeError("connect_fn_readonly_arg_unsupported"),
        once_key="connect_readonly_arg_unsupported",
        connect_fn=repr(connect_fn)[:200],
    )
    return connect_fn()


def _run_durable_idempotency_write(
    fn: Callable[[Any], Any],
    *,
    connect_fn: Optional[Callable[..., Any]] = None,
) -> Any:
    con = _open_dedicated_connection(connect_fn)
    try:
        result = fn(con)
        con.commit()
        return result
    except Exception:
        try:
            con.rollback()
        except Exception as rollback_exc:
            _warn_nonfatal(
                "ORDER_IDEMPOTENCY_DURABLE_ROLLBACK_FAILED",
                rollback_exc,
                once_key="durable_rollback_failed",
            )
        raise
    finally:
        try:
            con.close()
        except Exception as close_exc:
            _warn_nonfatal(
                "ORDER_IDEMPOTENCY_DURABLE_CLOSE_FAILED",
                close_exc,
                once_key="durable_close_failed",
            )


def init_order_idempotency(con) -> None:
    had_transaction = bool(getattr(con, "in_transaction", False))
    con.executescript(SCHEMA)
    _ensure_order_idempotency_slice_columns(con)
    _commit_if_outermost(con, had_transaction)


def _ensure_order_idempotency_slice_columns(con) -> None:
    columns = {
        "parent_order_id": "INTEGER",
        "slice_index": "INTEGER",
        "slice_count": "INTEGER",
    }
    try:
        if dbapi.is_sqlite_connection(con):
            rows = con.execute("PRAGMA table_info(execution_order_idempotency)").fetchall()
            existing = {str(row[1]) for row in rows}
            for name, ddl in columns.items():
                if name not in existing:
                    con.execute(f"ALTER TABLE execution_order_idempotency ADD COLUMN {name} {ddl}")
            return

        for name, ddl in columns.items():
            con.execute(f"ALTER TABLE IF EXISTS execution_order_idempotency ADD COLUMN IF NOT EXISTS {name} {ddl}")
    except Exception as e:
        _warn_nonfatal(
            "ORDER_IDEMPOTENCY_SLICE_COLUMN_MIGRATION_FAILED",
            e,
            once_key="slice_column_migration_failed",
        )


def _canonical_order_payload(
    *,
    broker: str,
    portfolio_orders_id: Optional[int],
    portfolio_ts_ms: Optional[int],
    order: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    o = dict(order or {})
    parent_order_id = _first_present(
        o,
        "parent_order_id",
        "adaptive_parent_order_id",
        "slice_parent_order_id",
    )
    slice_index = _first_present(o, "slice_index", "adaptive_slice_index")
    slice_count = _first_present(o, "slice_count", "adaptive_slice_count")
    # Canonicalization removes presentation noise so semantically identical
    # orders hash to the same UID across retries/restarts.
    return {
        "broker": _canon_str(broker).lower(),
        "portfolio_orders_id": _safe_i(portfolio_orders_id),
        "portfolio_ts_ms": _safe_i(portfolio_ts_ms),
        "parent_order_id": _safe_i(parent_order_id),
        "slice_index": _safe_i(slice_index),
        "slice_count": _safe_i(slice_count),
        "slice_style": _canon_str(o.get("slice_style") or ""),
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
              parent_order_id,
              slice_index,
              slice_count,
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
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_uid) DO NOTHING
            """,
            (
                str(order_uid),
                str(broker or "").lower(),
                _safe_i(portfolio_orders_id),
                _safe_i(portfolio_ts_ms),
                _safe_i(payload.get("source_order_id")),
                _safe_i(payload.get("source_alert_id")),
                _safe_i(payload.get("parent_order_id")),
                _safe_i(payload.get("slice_index")),
                _safe_i(payload.get("slice_count")),
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


def claim_order_submission_durable(
    *,
    broker: str,
    portfolio_orders_id: Optional[int],
    portfolio_ts_ms: Optional[int],
    order: Optional[Dict[str, Any]],
    connect_fn: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Claim a live broker submission on a dedicated committed connection."""

    return _run_durable_idempotency_write(
        lambda con: claim_order_submission(
            con=con,
            broker=broker,
            portfolio_orders_id=portfolio_orders_id,
            portfolio_ts_ms=portfolio_ts_ms,
            order=order,
        ),
        connect_fn=connect_fn,
    )


def _canonical_open_order_replacement_payload(
    *,
    broker: str,
    open_order_id: int,
    original_client_order_id: str,
    replacement_attempt: int,
    remaining_qty: float,
    side: str,
    symbol: str,
    venue: Optional[str] = None,
    order_type: Optional[str] = None,
    limit_px: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "kind": "open_order_replacement",
        "broker": _canon_str(broker).lower(),
        "venue": _canon_str(venue or broker).lower(),
        "open_order_id": _safe_i(open_order_id),
        "original_client_order_id": _canon_str(original_client_order_id),
        "replacement_attempt": _safe_i(replacement_attempt),
        "remaining_qty": _canon_float(remaining_qty),
        "side": _canon_upper(side),
        "symbol": _canon_upper(symbol),
        "order_type": _canon_upper(order_type),
        "limit_px": _canon_float(limit_px),
    }


def compute_open_order_replacement_uid(
    *,
    broker: str,
    open_order_id: int,
    original_client_order_id: str,
    replacement_attempt: int,
    remaining_qty: float,
    side: str,
    symbol: str,
    venue: Optional[str] = None,
    order_type: Optional[str] = None,
    limit_px: Optional[float] = None,
) -> str:
    payload = _canonical_open_order_replacement_payload(
        broker=broker,
        open_order_id=int(open_order_id),
        original_client_order_id=original_client_order_id,
        replacement_attempt=int(replacement_attempt),
        remaining_qty=float(remaining_qty),
        side=side,
        symbol=symbol,
        venue=venue,
        order_type=order_type,
        limit_px=limit_px,
    )
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def claim_open_order_replacement_submission(
    *,
    con,
    broker: str,
    open_order_id: int,
    original_client_order_id: str,
    replacement_attempt: int,
    remaining_qty: float,
    side: str,
    symbol: str,
    client_order_id: str,
    venue: Optional[str] = None,
    order_type: Optional[str] = None,
    limit_px: Optional[float] = None,
    portfolio_orders_id: Optional[int] = None,
    source_alert_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    init_order_idempotency(con)
    canonical = _canonical_open_order_replacement_payload(
        broker=broker,
        open_order_id=int(open_order_id),
        original_client_order_id=original_client_order_id,
        replacement_attempt=int(replacement_attempt),
        remaining_qty=float(remaining_qty),
        side=side,
        symbol=symbol,
        venue=venue,
        order_type=order_type,
        limit_px=limit_px,
    )
    order_uid = compute_open_order_replacement_uid(
        broker=broker,
        open_order_id=int(open_order_id),
        original_client_order_id=original_client_order_id,
        replacement_attempt=int(replacement_attempt),
        remaining_qty=float(remaining_qty),
        side=side,
        symbol=symbol,
        venue=venue,
        order_type=order_type,
        limit_px=limit_px,
    )
    client_oid = _canon_str(client_order_id) or make_client_order_id(order_uid, broker=str(broker or ""))
    now_ms = _now_ms()
    had_transaction = bool(getattr(con, "in_transaction", False))
    payload_json = {
        **canonical,
        "portfolio_orders_id": _safe_i(portfolio_orders_id),
        "source_alert_id": _safe_i(source_alert_id),
        "client_order_id": str(client_oid),
        "payload": dict(payload or {}),
    }

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
              parent_order_id,
              slice_index,
              slice_count,
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
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_uid) DO NOTHING
            """,
            (
                str(order_uid),
                _canon_str(broker).lower(),
                _safe_i(portfolio_orders_id),
                None,
                _safe_i(open_order_id),
                _safe_i(source_alert_id),
                None,
                None,
                None,
                _canon_upper(symbol),
                str(client_oid),
                None,
                "claimed",
                int(now_ms),
                int(now_ms),
                int(now_ms),
                None,
                None,
                json.dumps(payload_json, separators=(",", ":"), sort_keys=True, default=str),
            ),
        )
        if int(getattr(insert_cur, "rowcount", 0) or 0) > 0:
            _commit_if_outermost(con, had_transaction)
            return {
                "ok": True,
                "duplicate": False,
                "order_uid": str(order_uid),
                "client_order_id": str(client_oid),
                "status": "claimed",
            }
        raise dbapi.IntegrityError("duplicate replacement claim")
    except dbapi.IntegrityError:
        _warn_nonfatal(
            "ORDER_IDEMPOTENCY_DUPLICATE_REPLACEMENT_CLAIM",
            dbapi.IntegrityError("duplicate replacement claim"),
            once_key=f"duplicate_replacement_claim:{order_uid}",
            order_uid=str(order_uid),
            client_order_id=str(client_oid),
        )
        row = con.execute(
            """
            SELECT order_uid, client_order_id, broker_order_id, status, submit_ts_ms, last_error
            FROM execution_order_idempotency
            WHERE order_uid=?
            LIMIT 1
            """,
            (str(order_uid),),
        ).fetchone()
        if not row:
            row = con.execute(
                """
                SELECT order_uid, client_order_id, broker_order_id, status, submit_ts_ms, last_error
                FROM execution_order_idempotency
                WHERE client_order_id=?
                LIMIT 1
                """,
                (str(client_oid),),
            ).fetchone()
        if not row:
            return {
                "ok": False,
                "order_uid": str(order_uid),
                "client_order_id": str(client_oid),
                "status": "duplicate_exists_but_unreadable",
            }
        return {
            "ok": True,
            "duplicate": True,
            "order_uid": str(row[0]) if row[0] is not None else str(order_uid),
            "client_order_id": str(row[1]) if row[1] is not None else str(client_oid),
            "broker_order_id": str(row[2]) if row[2] is not None else None,
            "status": str(row[3]) if row[3] is not None else "claimed",
            "submit_ts_ms": (int(row[4]) if row[4] is not None else None),
            "last_error": str(row[5]) if row[5] is not None else None,
        }


def claim_open_order_replacement_submission_durable(
    *,
    broker: str,
    open_order_id: int,
    original_client_order_id: str,
    replacement_attempt: int,
    remaining_qty: float,
    side: str,
    symbol: str,
    client_order_id: str,
    venue: Optional[str] = None,
    order_type: Optional[str] = None,
    limit_px: Optional[float] = None,
    portfolio_orders_id: Optional[int] = None,
    source_alert_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    connect_fn: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Durably claim an open-order replacement before a broker submit."""

    return _run_durable_idempotency_write(
        lambda con: claim_open_order_replacement_submission(
            con=con,
            broker=broker,
            open_order_id=int(open_order_id),
            original_client_order_id=original_client_order_id,
            replacement_attempt=int(replacement_attempt),
            remaining_qty=float(remaining_qty),
            side=side,
            symbol=symbol,
            client_order_id=client_order_id,
            venue=venue,
            order_type=order_type,
            limit_px=limit_px,
            portfolio_orders_id=portfolio_orders_id,
            source_alert_id=source_alert_id,
            payload=payload,
        ),
        connect_fn=connect_fn,
    )


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


def mark_order_submission_submitted_durable(
    *,
    order_uid: str,
    client_order_id: str,
    broker_order_id: Optional[str],
    submit_ts_ms: int,
    connect_fn: Optional[Callable[..., Any]] = None,
) -> None:
    """Persist a submitted marker independently of any ambient transaction."""

    _run_durable_idempotency_write(
        lambda con: mark_order_submission_submitted(
            con=con,
            order_uid=order_uid,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            submit_ts_ms=submit_ts_ms,
        ),
        connect_fn=connect_fn,
    )


def mark_order_submission_unrecorded(
    *,
    con,
    broker: str,
    order_uid: str,
    client_order_id: str,
    broker_order_id: Optional[str],
    submit_ts_ms: int,
    last_error: str,
    symbol: Optional[str] = None,
    portfolio_orders_id: Optional[int] = None,
    portfolio_ts_ms: Optional[int] = None,
    source_order_id: Optional[int] = None,
    source_alert_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    init_order_idempotency(con)
    now_ms = _now_ms()
    had_transaction = bool(getattr(con, "in_transaction", False))
    oid = str(order_uid or "").strip()
    cid = str(client_order_id or "").strip()
    broker_name = str(broker or "").strip().lower() or "unknown"
    symbol_norm = _canon_upper(symbol) or _canon_upper((payload or {}).get("symbol")) or "UNKNOWN"
    broker_oid = str(broker_order_id).strip() if broker_order_id is not None else None
    submit_ts = int(submit_ts_ms or now_ms)
    error_text = str(last_error or "accepted_broker_submission_unrecorded")
    marker_payload = {
        **dict(payload or {}),
        "broker": broker_name,
        "order_uid": oid,
        "client_order_id": cid,
        "broker_order_id": broker_oid,
        "status": SUBMISSION_UNRECORDED_STATUS,
        "needs_reconcile": True,
        "last_error": error_text,
    }

    updated = con.execute(
        """
        UPDATE execution_order_idempotency
        SET client_order_id=?,
            broker_order_id=?,
            status=?,
            submit_ts_ms=COALESCE(submit_ts_ms, ?),
            updated_ts_ms=?,
            last_error=?
        WHERE order_uid=?
        """,
        (
            cid,
            broker_oid,
            SUBMISSION_UNRECORDED_STATUS,
            int(submit_ts),
            int(now_ms),
            error_text,
            oid,
        ),
    )
    updated_n = int(getattr(updated, "rowcount", 0) or 0)
    if updated_n <= 0:
        con.execute(
            """
            INSERT INTO execution_order_idempotency(
              order_uid,
              broker,
              portfolio_orders_id,
              portfolio_ts_ms,
              source_order_id,
              source_alert_id,
              parent_order_id,
              slice_index,
              slice_count,
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
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_uid) DO UPDATE SET
              client_order_id=excluded.client_order_id,
              broker_order_id=excluded.broker_order_id,
              status=excluded.status,
              submit_ts_ms=COALESCE(execution_order_idempotency.submit_ts_ms, excluded.submit_ts_ms),
              updated_ts_ms=excluded.updated_ts_ms,
              last_error=excluded.last_error
            """,
            (
                oid,
                broker_name,
                _safe_i(portfolio_orders_id),
                _safe_i(portfolio_ts_ms),
                _safe_i(source_order_id if source_order_id is not None else marker_payload.get("source_order_id")),
                _safe_i(source_alert_id if source_alert_id is not None else marker_payload.get("source_alert_id")),
                _safe_i(_first_present(marker_payload, "parent_order_id", "adaptive_parent_order_id", "slice_parent_order_id")),
                _safe_i(_first_present(marker_payload, "slice_index", "adaptive_slice_index")),
                _safe_i(_first_present(marker_payload, "slice_count", "adaptive_slice_count")),
                symbol_norm,
                cid,
                broker_oid,
                SUBMISSION_UNRECORDED_STATUS,
                int(now_ms),
                int(now_ms),
                int(now_ms),
                int(submit_ts),
                error_text,
                json.dumps(marker_payload, separators=(",", ":"), sort_keys=True),
            ),
        )
    _commit_if_outermost(con, had_transaction)
    return {
        "ok": True,
        "marker_written": True,
        "status": SUBMISSION_UNRECORDED_STATUS,
        "broker": broker_name,
        "order_uid": oid,
        "client_order_id": cid,
        "broker_order_id": broker_oid,
        "submit_ts_ms": int(submit_ts),
        "updated_existing": bool(updated_n > 0),
    }


def mark_order_submission_unrecorded_durable(
    *,
    broker: str,
    order_uid: str,
    client_order_id: str,
    broker_order_id: Optional[str],
    submit_ts_ms: int,
    last_error: str,
    symbol: Optional[str] = None,
    portfolio_orders_id: Optional[int] = None,
    portfolio_ts_ms: Optional[int] = None,
    source_order_id: Optional[int] = None,
    source_alert_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    connect_fn: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    """Persist an unrecorded-submission marker on a dedicated connection."""

    return _run_durable_idempotency_write(
        lambda con: mark_order_submission_unrecorded(
            con=con,
            broker=broker,
            order_uid=order_uid,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            submit_ts_ms=submit_ts_ms,
            last_error=last_error,
            symbol=symbol,
            portfolio_orders_id=portfolio_orders_id,
            portfolio_ts_ms=portfolio_ts_ms,
            source_order_id=source_order_id,
            source_alert_id=source_alert_id,
            payload=payload,
        ),
        connect_fn=connect_fn,
    )


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


def mark_order_submission_unknown_durable(
    *,
    order_uid: str,
    last_error: str,
    connect_fn: Optional[Callable[..., Any]] = None,
) -> None:
    """Persist an ambiguous-submit marker independently of ambient rollback."""

    _run_durable_idempotency_write(
        lambda con: mark_order_submission_unknown(
            con=con,
            order_uid=order_uid,
            last_error=last_error,
        ),
        connect_fn=connect_fn,
    )
