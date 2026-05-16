"""
FILE: crash_recovery.py

Runtime subsystem module for `crash_recovery`.

This module rebuilds recent execution state after a restart by replaying broker
or ledger evidence into the execution ledger, open-order tracking, and runtime
event log.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

from engine.runtime.storage import connect
from engine.execution.execution_ledger import log_fill, log_submit
from engine.execution.execution_microstructure import record_open_order
from engine.runtime.event_log import append_event
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


log = get_logger("runtime.crash_recovery")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn(scope: str, err: Exception, **extra) -> None:
    log_failure(
        log,
        event="runtime_crash_recovery_nonfatal",
        code=str(scope).replace(".", "_"),
        message=str(scope),
        error=err,
        level=logging.WARNING,
        component="engine.runtime.crash_recovery",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _broker() -> str:
    return str(os.environ.get("BROKER_NAME", os.environ.get("BROKER", "sim"))).lower().strip()


def _enabled() -> bool:
    v = str(os.environ.get("CRASH_RECOVERY_ENABLED", "1")).strip().lower()
    return v not in ("0", "false", "no", "off")


def _lookback_hours() -> int:
    try:
        return max(1, int(float(os.environ.get("CRASH_RECOVERY_LOOKBACK_HOURS", "72"))))
    except Exception as e:
        _warn("crash_recovery.lookback_hours", e)
        return 72


def _max_ledger_restore() -> int:
    try:
        return max(1, int(float(os.environ.get("CRASH_RECOVERY_MAX_LEDGER_RESTORE", "250"))))
    except Exception as e:
        _warn("crash_recovery.max_ledger_restore", e)
        return 250


def _safe_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn("crash_recovery.safe_int", e, value_type=type(value).__name__)
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except Exception as e:
        _warn("crash_recovery.safe_float", e, value_type=type(value).__name__)
        return float(default)


def _reconcile_on_boot() -> bool:
    v = str(os.environ.get("CRASH_RECOVERY_RECONCILE_ON_BOOT", "1")).strip().lower()
    return v not in ("0", "false", "no", "off")


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn("crash_recovery.table_exists", e, table=str(table_name))
        return False


def _is_timescale_hypertable(con, table_name: str) -> bool:
    try:
        if not hasattr(con, "raw"):
            return False
        row = con.execute(
            """
            SELECT 1
            FROM timescaledb_information.hypertables
            WHERE hypertable_schema = ANY (current_schemas(false))
              AND hypertable_name = ?
            LIMIT 1
            """,
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _ensure_replay_key_index(con) -> None:
    if _is_timescale_hypertable(con, "crash_recovery_audit"):
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_crash_recovery_audit_replay_key
              ON crash_recovery_audit(replay_key)
            """
        )
        return
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_crash_recovery_audit_replay_key
          ON crash_recovery_audit(replay_key)
        """
    )


def _ensure_tables(con) -> None:
    # Recovery audit is append-only and idempotence-keyed so boot-time replay
    # can be retried without duplicating the same restoration action.
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS crash_recovery_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          event_type TEXT NOT NULL,
          client_order_id TEXT,
          broker_order_id TEXT,
          symbol TEXT,
          status TEXT,
          replay_key TEXT NOT NULL,
          detail_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_crash_recovery_audit_ts
          ON crash_recovery_audit(ts_ms);

        CREATE INDEX IF NOT EXISTS idx_crash_recovery_audit_type
          ON crash_recovery_audit(event_type);
        """
    )
    _ensure_replay_key_index(con)
    con.commit()

def _audit_event(
    con,
    *,
    event_type: str,
    replay_key: str,
    client_order_id: Optional[str] = None,
    broker_order_id: Optional[str] = None,
    symbol: Optional[str] = None,
    status: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> bool:
    try:
        existing = con.execute(
            "SELECT 1 FROM crash_recovery_audit WHERE replay_key=? LIMIT 1",
            (str(replay_key),),
        ).fetchone()
        if existing:
            return False
        cur = con.execute(
            """
            INSERT INTO crash_recovery_audit(
              ts_ms, event_type, client_order_id, broker_order_id, symbol, status, replay_key, detail_json
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                _now_ms(),
                str(event_type),
                str(client_order_id) if client_order_id is not None else None,
                str(broker_order_id) if broker_order_id is not None else None,
                str(symbol) if symbol is not None else None,
                str(status) if status is not None else None,
                str(replay_key),
                json.dumps(detail or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
        inserted = bool(getattr(cur, "rowcount", 0))
        if inserted:
            try:
                # Emit a matching event_log row so crash recovery activity
                # shows up in the same observability stream as live actions.
                append_event(
                    event_type=str(event_type),
                    event_source="engine.runtime.crash_recovery",
                    entity_type="crash_recovery",
                    entity_id=(str(client_order_id) if client_order_id is not None else str(replay_key)),
                    correlation_id=str(replay_key),
                    payload={
                        "ts_ms": _now_ms(),
                        "client_order_id": (str(client_order_id) if client_order_id is not None else None),
                        "broker_order_id": (str(broker_order_id) if broker_order_id is not None else None),
                        "symbol": (str(symbol) if symbol is not None else None),
                        "status": (str(status) if status is not None else None),
                        "replay_key": str(replay_key),
                        "detail": dict(detail or {}),
                    },
                    ts_ms=_now_ms(),
                    con=con,
                )
            except Exception as e:
                _warn("crash_recovery.audit_event.append_event", e, event_type=str(event_type), replay_key=str(replay_key))
        return inserted
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            try:
                con.rollback()
            except Exception:
                pass
            return False
        _warn("crash_recovery.audit_event", e, event_type=str(event_type), replay_key=str(replay_key))
        return False


def _parse_ts_ms(v: Any) -> int:
    if v is None:
        return _now_ms()
    try:
        if isinstance(v, (int, float)):
            x = float(v)
            if x > 1_000_000_000_000:
                return int(x)
            if x > 1_000_000_000:
                return int(x * 1000.0)
    except Exception as e:
        _warn("crash_recovery.parse_ts.numeric", e, value=v)

    s = str(v or "").strip()
    if not s:
        return _now_ms()

    # Broker/export timestamps are not always normalized, so accept a few
    # legacy shapes before falling back to "now".
    for fmt in ("%Y%m%d  %H:%M:%S", "%Y%m%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000.0)
        except Exception as e:
            _warn("crash_recovery.parse_ts_ms.legacy_format", e, raw=str(s), fmt=str(fmt))
            continue

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000.0)
    except Exception as e:
        _warn("crash_recovery.parse_ts_ms.isoformat", e, raw=str(s))
        now = _now_ms()
        return now


def _signed_qty(qty: Any, side: Any) -> float:
    q = abs(float(qty or 0.0))
    s = str(side or "").lower().strip()
    if s in ("sell", "s", "sl"):
        return -q
    return q


def _lookup_order_by_client(con, client_order_id: str):
    if not client_order_id or not _table_exists(con, "execution_orders"):
        return None
    return con.execute(
        """
        SELECT client_order_id, broker_order_id, broker, symbol, qty, submit_ts_ms, ref_px, extra_json
        FROM execution_orders
        WHERE client_order_id=?
        LIMIT 1
        """,
        (str(client_order_id),),
    ).fetchone()


def _lookup_order_by_broker(con, broker_order_id: str):
    if not broker_order_id or not _table_exists(con, "execution_orders"):
        return None
    return con.execute(
        """
        SELECT client_order_id, broker_order_id, broker, symbol, qty, submit_ts_ms, ref_px, extra_json
        FROM execution_orders
        WHERE broker_order_id=?
        ORDER BY submit_ts_ms DESC
        LIMIT 1
        """,
        (str(broker_order_id),),
    ).fetchone()


def _execution_fill_exists(con, client_order_id: str, fill_id: str) -> bool:
    # Fill replay is deduped against the execution ledger so a restart cannot
    # double-count fills that were already persisted before the crash.
    if not client_order_id or not fill_id or not _table_exists(con, "execution_fills"):
        return False
    row = con.execute(
        """
        SELECT 1
        FROM execution_fills
        WHERE client_order_id=? AND fill_id=?
        LIMIT 1
        """,
        (str(client_order_id), str(fill_id)),
    ).fetchone()
    return bool(row)


def _sum_fill_qty_by_symbol(con) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not _table_exists(con, "execution_fills"):
        return out
    try:
        rows = con.execute(
            """
            SELECT f.symbol, COALESCE(SUM(f.fill_qty), 0.0)
            FROM execution_fills f
            LEFT JOIN execution_orders o
              ON o.client_order_id = f.client_order_id
            WHERE COALESCE(f.symbol, '') <> ''
              AND COALESCE(json_extract(o.extra_json, '$.execution_target'), 'real') = 'real'
            GROUP BY f.symbol
            """
        ).fetchall()
    except Exception:
        rows = con.execute(
            """
            SELECT symbol, COALESCE(SUM(fill_qty), 0.0)
            FROM execution_fills
            WHERE COALESCE(symbol, '') <> ''
            GROUP BY symbol
            """
        ).fetchall()
    for r in rows or []:
        try:
            sym = str(r[0] or "").upper().strip()
            qty = float(r[1] or 0.0)
            if sym:
                out[sym] = qty
        except Exception as e:
            _warn("crash_recovery.sum_fill_qty_by_symbol.row", e, row=repr(r))
            continue
    return out


def _real_execution_model_ids(con) -> List[str]:
    out: List[str] = []
    if not _table_exists(con, "execution_orders"):
        return ["baseline"]
    try:
        rows = con.execute(
            """
            SELECT DISTINCT COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')
            FROM execution_orders
            WHERE COALESCE(json_extract(extra_json, '$.execution_target'), 'real') = 'real'
            """
        ).fetchall() or []
    except Exception:
        rows = []
    for row in rows:
        try:
            mid = str((row or [None])[0] or "").strip()
            if mid:
                out.append(mid)
        except Exception as e:
            _warn("crash_recovery.real_execution_model_ids.row", e, row=repr(row))
            continue
    return out or ["baseline"]


def _sum_portfolio_state_by_symbol(con) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not _table_exists(con, "portfolio_state"):
        return out
    real_model_ids = _real_execution_model_ids(con)
    placeholders = ",".join("?" for _ in real_model_ids)
    rows = con.execute(
        f"""
        SELECT symbol,
               SUM(CASE
                     WHEN UPPER(COALESCE(side, '')) IN ('LONG','BUY') THEN ABS(COALESCE(weight, 0.0))
                     WHEN UPPER(COALESCE(side, '')) IN ('SHORT','SELL') THEN -ABS(COALESCE(weight, 0.0))
                     ELSE COALESCE(weight, 0.0)
                   END) AS qty_like
        FROM portfolio_state
        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') IN ({placeholders})
        GROUP BY symbol
        """,
        tuple(real_model_ids),
    ).fetchall()
    for r in rows or []:
        try:
            sym = str(r[0] or "").upper().strip()
            qty = float(r[1] or 0.0)
            if sym:
                out[sym] = qty
        except Exception as e:
            _warn("crash_recovery.sum_portfolio_state_by_symbol.row", e, row=repr(r))
            continue
    return out


def _sum_execution_open_qty_by_symbol(con) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not _table_exists(con, "execution_orders"):
        return out

    rows = con.execute(
        """
        SELECT eo.symbol,
               COALESCE(SUM(eo.qty), 0.0) - COALESCE(SUM(ef.fill_qty), 0.0) AS remaining_qty
        FROM execution_orders eo
        LEFT JOIN execution_fills ef
          ON ef.client_order_id = eo.client_order_id
        WHERE COALESCE(eo.status, 'submitted') NOT IN ('filled','cancelled','canceled','rejected')
          AND COALESCE(json_extract(eo.extra_json, '$.execution_target'), 'real') = 'real'
        GROUP BY eo.client_order_id, eo.symbol, eo.qty
        """
    ).fetchall()

    for r in rows or []:
        try:
            sym = str(r[0] or "").upper().strip()
            qty = float(r[1] or 0.0)
            if sym and abs(qty) > 1e-9:
                out[sym] = float(out.get(sym) or 0.0) + qty
        except Exception as e:
            _warn("crash_recovery.sum_execution_open_qty_by_symbol.row", e, row=repr(r))
            continue
    return out


def _sum_registry_open_qty_by_symbol(con) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not _table_exists(con, "exec_open_orders"):
        return out
    rows = con.execute(
        """
        SELECT symbol, COALESCE(SUM(qty), 0.0)
        FROM exec_open_orders
        WHERE COALESCE(status, 'open')='open'
        GROUP BY symbol
        """
    ).fetchall()
    for r in rows or []:
        try:
            sym = str(r[0] or "").upper().strip()
            qty = float(r[1] or 0.0)
            if sym and abs(qty) > 1e-9:
                out[sym] = qty
        except Exception as e:
            _warn("crash_recovery.sum_registry_open_qty_by_symbol.row", e, row=repr(r))
            continue
    return out


def _reconcile_open_order_registry(
    con,
    *,
    broker_name: str,
    broker_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not _table_exists(con, "exec_open_orders"):
        return {"closed_stale_registry_orders": 0}

    broker_client_ids = set()
    broker_order_ids = set()

    for r in broker_rows or []:
        try:
            if str(broker_name) == "alpaca":
                cid = str(r.get("client_order_id") or r.get("id") or "").strip()
                boid = str(r.get("id") or "").strip()
            else:
                boid = str(r.get("orderId") or "").strip()
                local = _lookup_order_by_broker(con, boid) if boid else None
                cid = str(local[0]) if local and local[0] else str(f"ibkr_{boid or r.get('permId') or ''}").strip()
            if cid:
                broker_client_ids.add(cid)
            if boid:
                broker_order_ids.add(boid)
        except Exception as e:
            _warn("crash_recovery.reconcile_open_order_registry.broker_row", e, broker=str(broker_name), row=repr(r))
            continue

    ledger_pending_ids = set()
    if _table_exists(con, "execution_orders"):
        rows = con.execute(
            """
            SELECT eo.client_order_id
            FROM execution_orders eo
            LEFT JOIN execution_fills ef
              ON ef.client_order_id = eo.client_order_id
            WHERE COALESCE(eo.status, 'submitted') NOT IN ('filled','cancelled','canceled','rejected')
            GROUP BY eo.client_order_id, eo.qty
            HAVING ABS(COALESCE(SUM(ef.fill_qty), 0.0)) + 1e-9 < ABS(COALESCE(eo.qty, 0.0))
            """
        ).fetchall()
        for r in rows or []:
            try:
                cid = str(r[0] or "").strip()
                if cid:
                    ledger_pending_ids.add(cid)
            except Exception as e:
                _warn("crash_recovery.reconcile_open_order_registry.ledger_pending_row", e, row=repr(r))
                continue

    closed = 0
    now = _now_ms()
    rows = con.execute(
        """
        SELECT id, client_order_id, broker_order_id, symbol, status
        FROM exec_open_orders
        WHERE COALESCE(status, 'open')='open'
        """
    ).fetchall()

    for r in rows or []:
        open_id = int(r[0])
        client_order_id = str(r[1] or "").strip()
        broker_order_id = str(r[2] or "").strip()
        symbol = str(r[3] or "").upper().strip()
        status = str(r[4] or "open")

        if client_order_id in ledger_pending_ids:
            continue
        if client_order_id and client_order_id in broker_client_ids:
            continue
        if broker_order_id and broker_order_id in broker_order_ids:
            continue

        replay_key = f"reconcile_registry_close:{client_order_id}:{broker_order_id}"
        inserted = _audit_event(
            con,
            event_type="reconcile_open_order",
            replay_key=replay_key,
            client_order_id=client_order_id or None,
            broker_order_id=broker_order_id or None,
            symbol=symbol or None,
            status="closed_stale_registry_order",
            detail={
                "previous_status": status,
                "broker": broker_name,
                "reason": "missing_from_broker_and_ledger_pending_sets",
            },
        )
        if not inserted:
            continue

        con.execute(
            """
            UPDATE exec_open_orders
            SET updated_ts_ms=?,
                status='closed'
            WHERE id=?
            """,
            (int(now), int(open_id)),
        )
        closed += 1

    try:
        con.commit()
    except Exception as e:
        _warn("crash_recovery.reconcile_open_order_registry.commit", e, broker=str(broker_name))

    return {"closed_stale_registry_orders": int(closed)}


def _restore_open_orders(con) -> Dict[str, Any]:
    restored_local = 0
    restored_broker = 0
    limit_n = _max_ledger_restore()
    lookback_ms = int(_lookback_hours() * 3600 * 1000)
    after_ts_ms = _now_ms() - lookback_ms

    if _table_exists(con, "execution_orders"):
        rows = con.execute(
            """
            SELECT eo.client_order_id,
                   eo.broker_order_id,
                   eo.broker,
                   eo.symbol,
                   eo.qty,
                   eo.submit_ts_ms,
                   eo.ref_px,
                   eo.status,
                   eo.extra_json
            FROM execution_orders eo
            LEFT JOIN execution_fills ef
              ON ef.client_order_id = eo.client_order_id
            WHERE eo.submit_ts_ms >= ?
              AND COALESCE(eo.status, 'submitted') NOT IN ('filled','cancelled','canceled','rejected')
            GROUP BY eo.client_order_id, eo.broker_order_id, eo.broker, eo.symbol, eo.qty, eo.submit_ts_ms, eo.ref_px, eo.status, eo.extra_json
            HAVING COALESCE(SUM(ABS(ef.fill_qty)), 0.0) < ABS(eo.qty)
            ORDER BY eo.submit_ts_ms DESC
            LIMIT ?
            """,
            (int(after_ts_ms), int(limit_n)),
        ).fetchall()

        for r in rows or []:
            client_order_id = str(r[0] or "").strip()
            if not client_order_id:
                continue
            broker_order_id = str(r[1]) if r[1] is not None else None
            broker_name = str(r[2] or _broker()).lower().strip()
            symbol = str(r[3] or "").upper().strip()
            qty = float(r[4] or 0.0)
            submit_ts_ms = int(r[5] or _now_ms())
            ref_px = float(r[6]) if r[6] is not None else None
            status = str(r[7] or "submitted")
            extra_json = r[8]

            try:
                extra_obj = json.loads(extra_json or "{}")
                if not isinstance(extra_obj, dict):
                    extra_obj = {}
            except Exception as e:
                _warn("crash_recovery.restore_open_orders.extra_json", e, client_order_id=str(client_order_id))
                extra_obj = {}

            replay_key = f"restore_open_order:{client_order_id}"
            inserted = _audit_event(
                con,
                event_type="restore_open_order",
                replay_key=replay_key,
                client_order_id=client_order_id,
                broker_order_id=broker_order_id,
                symbol=symbol,
                status=status,
                detail={
                    "broker": broker_name,
                    "qty": qty,
                    "submit_ts_ms": submit_ts_ms,
                    "ref_px": ref_px,
                },
            )
            if not inserted:
                continue

            record_open_order(
                broker=broker_name,
                symbol=symbol,
                qty=qty,
                order_type=str(extra_obj.get("order_type") or "MARKET").upper().strip(),
                aggressiveness=str(extra_obj.get("aggressiveness") or "RECOVERY").upper().strip(),
                limit_px=ref_px,
                client_order_id=client_order_id,
                broker_order_id=broker_order_id,
                max_attempts=int(extra_obj.get("max_reprice_attempts") or 0),
                portfolio_orders_id=(
                    _safe_int(extra_obj.get("portfolio_orders_id"))
                    if extra_obj.get("portfolio_orders_id") is not None
                    else None
                ),
                source_alert_id=(
                    _safe_int(extra_obj.get("source_alert_id"))
                    if extra_obj.get("source_alert_id") is not None
                    else None
                ),
                meta={
                    **extra_obj,
                    "recovered": True,
                    "replay_key": replay_key,
                    "recovery_submit_ts_ms": submit_ts_ms,
                },
            )
            restored_local += 1

    broker_name = _broker()

    if broker_name == "alpaca":
        try:
            from engine.execution.broker_alpaca_rest import list_open_orders
            broker_rows = list_open_orders(limit=int(limit_n))
        except Exception as e:
            _warn("crash_recovery.restore_open_orders.alpaca", e)
            broker_rows = []
    elif broker_name == "ibkr":
        try:
            from engine.execution.broker_ibkr_gateway import list_open_orders_live
            broker_rows = list_open_orders_live()
        except Exception as e:
            _warn("crash_recovery.restore_open_orders.ibkr", e)
            broker_rows = []
    else:
        broker_rows = []

    for r in broker_rows or []:
        if broker_name == "alpaca":
            broker_order_id = str(r.get("id") or "").strip() or None
            client_order_id = str(r.get("client_order_id") or broker_order_id or "").strip()
            if not client_order_id:
                continue
            symbol = str(r.get("symbol") or "").upper().strip()
            qty = _signed_qty(r.get("qty") or r.get("remaining_qty") or 0.0, r.get("side"))
            submit_ts_ms = _parse_ts_ms(r.get("created_at"))
            ref_px = _safe_float(r.get("limit_price")) if r.get("limit_price") not in (None, "") else None
            status = str(r.get("status") or "open")
            order_type = str(r.get("type") or "MARKET").upper().strip()
            aggressiveness = "RECOVERY"
        else:
            broker_order_id = str(r.get("orderId") or "").strip() or None
            local = _lookup_order_by_broker(con, broker_order_id) if broker_order_id else None
            client_order_id = str(local[0]) if local and local[0] else str(f"ibkr_{broker_order_id or r.get('permId') or _now_ms()}")
            symbol = str(r.get("symbol") or (local[3] if local and local[3] is not None else "")).upper().strip()
            qty = _signed_qty(
                r.get("remaining") if r.get("remaining") not in (None, "") else r.get("totalQuantity"),
                r.get("action"),
            )
            submit_ts_ms = _now_ms()
            ref_px = _safe_float(r.get("lmtPrice")) if r.get("lmtPrice") not in (None, "") else None
            status = str(r.get("status") or "open")
            order_type = str(r.get("orderType") or "MARKET").upper().strip()
            aggressiveness = "RECOVERY"

        replay_key = f"restore_open_order:broker:{broker_name}:{client_order_id}:{broker_order_id or ''}"
        inserted = _audit_event(
            con,
            event_type="restore_open_order",
            replay_key=replay_key,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            symbol=symbol,
            status=status,
            detail=dict(r or {}),
        )
        if not inserted:
            continue

        local_order = _lookup_order_by_client(con, client_order_id)
        if not local_order:
            log_submit(
                client_order_id=client_order_id,
                broker=broker_name,
                symbol=symbol,
                qty=qty,
                submit_ts_ms=submit_ts_ms,
                ref_px=ref_px,
                broker_order_id=broker_order_id,
                extra={
                    "crash_recovery_restored": True,
                    "replay_key": replay_key,
                    "broker_payload": dict(r or {}),
                    "order_type": order_type,
                    "aggressiveness": aggressiveness,
                },
            )

        record_open_order(
            broker=broker_name,
            symbol=symbol,
            qty=qty,
            order_type=order_type,
            aggressiveness=aggressiveness,
            limit_px=ref_px,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            max_attempts=0,
            meta={
                "recovered": True,
                "broker_payload": dict(r or {}),
                "replay_key": replay_key,
            },
        )
        restored_broker += 1

    registry_rec = _reconcile_open_order_registry(
        con,
        broker_name=broker_name,
        broker_rows=list(broker_rows or []),
    )

    return {
        "restored_from_ledger": int(restored_local),
        "restored_from_broker": int(restored_broker),
        **dict(registry_rec or {}),
    }


def _recover_ibkr_fills(con) -> Dict[str, Any]:
    try:
        from engine.execution.broker_ibkr_gateway import list_recent_executions_live
    except Exception as e:
        _warn("crash_recovery.recover_ibkr_fills.import", e)
        return {"fills": 0}

    rows = list_recent_executions_live(_now_ms() - (_lookback_hours() * 3600 * 1000))

    logged = 0
    orphan = 0

    for r in rows or []:
        broker_order_id = str(r.get("orderId") or "").strip()
        local = _lookup_order_by_broker(con, broker_order_id) if broker_order_id else None
        client_order_id = str(local[0]) if local and local[0] else str(f"ibkr_{broker_order_id or r.get('permId') or _now_ms()}")
        fill_id = str(r.get("execId") or r.get("permId") or "").strip()
        symbol = str(r.get("symbol") or (local[3] if local and local[3] is not None else "")).upper().strip()
        qty = _signed_qty(r.get("shares") or 0.0, r.get("side"))
        px = float(r.get("price") or 0.0)
        fill_ts_ms = _parse_ts_ms(r.get("time"))

        if not client_order_id or not fill_id:
            continue

        existed_locally = bool(local)
        replay_key = f"recover_fill:ibkr:{client_order_id}:{fill_id}"
        inserted = _audit_event(
            con,
            event_type="recovered_fill",
            replay_key=replay_key,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            symbol=symbol,
            status="recovered",
            detail=dict(r or {}),
        )
        if not inserted:
            continue

        if not existed_locally:
            orphan += 1
            _audit_event(
                con,
                event_type="orphan_broker_fill",
                replay_key=f"orphan_broker_fill:ibkr:{client_order_id}:{fill_id}",
                client_order_id=client_order_id,
                broker_order_id=broker_order_id,
                symbol=symbol,
                status="orphan",
                detail=dict(r or {}),
            )
            log_submit(
                client_order_id=client_order_id,
                broker="ibkr",
                symbol=symbol,
                qty=qty,
                submit_ts_ms=fill_ts_ms,
                ref_px=px,
                broker_order_id=broker_order_id,
                extra={
                    "crash_recovery_submit_stub": True,
                    "broker_payload": dict(r or {}),
                },
            )

        if not _execution_fill_exists(con, client_order_id, fill_id):
            log_fill(
                client_order_id=client_order_id,
                fill_id=fill_id,
                broker="ibkr",
                symbol=symbol,
                qty=qty,
                fill_px=px,
                fill_ts_ms=fill_ts_ms,
                extra={
                    **dict(r or {}),
                    "broker_order_id": broker_order_id,
                    "replay_key": replay_key,
                },
            )
            logged += 1

    return {"fills": int(logged), "orphan_broker_fills": int(orphan)}


def _recover_alpaca_fills(con) -> Dict[str, Any]:
    try:
        from engine.execution.broker_alpaca_rest import list_orders_after
    except Exception as e:
        _warn("crash_recovery.recover_alpaca_fills.import", e)
        return {"fills": 0}

    after_ts_ms = _now_ms() - (_lookback_hours() * 3600 * 1000)
    rows = list_orders_after(after_ts_ms=after_ts_ms, status="all", limit=500)

    logged = 0
    orphan = 0

    for r in rows or []:
        filled_qty = float(r.get("filled_qty") or 0.0)
        filled_avg = r.get("filled_avg_price")
        if filled_qty <= 0.0 or filled_avg in (None, ""):
            continue

        broker_order_id = str(r.get("id") or "").strip() or None
        client_order_id = str(r.get("client_order_id") or broker_order_id or "").strip()
        if not client_order_id:
            continue

        local = _lookup_order_by_client(con, client_order_id)
        symbol = str(r.get("symbol") or (local[3] if local and local[3] is not None else "")).upper().strip()
        qty = _signed_qty(filled_qty, r.get("side"))
        fill_px = float(filled_avg or 0.0)
        fill_ts_ms = _parse_ts_ms(r.get("updated_at") or r.get("filled_at") or r.get("created_at"))
        fill_id = str(r.get("id") or client_order_id).strip()

        replay_key = f"recover_fill:alpaca:{client_order_id}:{fill_id}"
        inserted = _audit_event(
            con,
            event_type="recovered_fill",
            replay_key=replay_key,
            client_order_id=client_order_id,
            broker_order_id=broker_order_id,
            symbol=symbol,
            status=str(r.get("status") or "filled"),
            detail=dict(r or {}),
        )
        if not inserted:
            continue

        if not local:
            orphan += 1
            _audit_event(
                con,
                event_type="orphan_broker_fill",
                replay_key=f"orphan_broker_fill:alpaca:{client_order_id}:{fill_id}",
                client_order_id=client_order_id,
                broker_order_id=broker_order_id,
                symbol=symbol,
                status="orphan",
                detail=dict(r or {}),
            )
            log_submit(
                client_order_id=client_order_id,
                broker="alpaca",
                symbol=symbol,
                qty=qty,
                submit_ts_ms=_parse_ts_ms(r.get("created_at")),
                ref_px=_safe_float(r.get("limit_price"), fill_px) if r.get("limit_price") not in (None, "") else fill_px,
                broker_order_id=broker_order_id,
                extra={
                    "crash_recovery_submit_stub": True,
                    "broker_payload": dict(r or {}),
                    "order_type": str(r.get("type") or "MARKET").upper().strip(),
                    "aggressiveness": "RECOVERY",
                },
            )

        if not _execution_fill_exists(con, client_order_id, fill_id):
            log_fill(
                client_order_id=client_order_id,
                fill_id=fill_id,
                broker="alpaca",
                symbol=symbol,
                qty=qty,
                fill_px=fill_px,
                fill_ts_ms=fill_ts_ms,
                extra={
                    **dict(r or {}),
                    "broker_order_id": broker_order_id,
                    "replay_key": replay_key,
                },
            )
            logged += 1

    return {"fills": int(logged), "orphan_broker_fills": int(orphan)}


def _detect_orphan_fills(con) -> Dict[str, Any]:
    if not _table_exists(con, "execution_fills") or not _table_exists(con, "execution_orders"):
        return {"orphans": 0}

    rows = con.execute(
        """
        SELECT f.client_order_id, MAX(f.symbol)
        FROM execution_fills f
        LEFT JOIN execution_orders o
          ON o.client_order_id = f.client_order_id
        WHERE o.client_order_id IS NULL
        GROUP BY f.client_order_id
        """
    ).fetchall()

    n = 0
    for r in rows or []:
        client_order_id = str(r[0] or "").strip()
        symbol = str(r[1] or "").upper().strip()
        if not client_order_id:
            continue
        inserted = _audit_event(
            con,
            event_type="orphan_local_fill",
            replay_key=f"orphan_local_fill:{client_order_id}",
            client_order_id=client_order_id,
            symbol=symbol,
            status="orphan",
            detail={"client_order_id": client_order_id, "symbol": symbol},
        )
        if inserted:
            n += 1

    return {"orphans": int(len(rows or [])), "new_audits": int(n)}


def _reconcile_broker_state(con) -> Dict[str, Any]:
    broker_name = _broker()
    broker_positions: List[Dict[str, Any]]
    if broker_name == "alpaca":
        try:
            from engine.execution.broker_alpaca_rest import get_positions
            broker_positions = list(get_positions() or [])
        except Exception as e:
            _warn("crash_recovery.reconcile_broker_state.alpaca_positions", e)
            broker_positions = []
    elif broker_name == "ibkr":
        try:
            from engine.execution.broker_ibkr_gateway import get_positions_live
            broker_positions = list(get_positions_live() or [])
        except Exception as e:
            _warn("crash_recovery.reconcile_broker_state.ibkr_positions", e)
            broker_positions = []
    else:
        broker_positions = []

    broker_map: Dict[str, float] = {}
    for p in broker_positions or []:
        try:
            sym = str(p.get("symbol") or "").upper().strip()
            qty = float(p.get("qty") or 0.0)
            if sym:
                broker_map[sym] = qty
        except Exception as e:
            _warn("crash_recovery.reconcile_positions.broker_row", e, row=repr(p))
            continue

    portfolio_map: Dict[str, float] = _sum_portfolio_state_by_symbol(con)
    ledger_fill_map: Dict[str, float] = _sum_fill_qty_by_symbol(con)
    ledger_open_map: Dict[str, float] = _sum_execution_open_qty_by_symbol(con)
    registry_open_map: Dict[str, float] = _sum_registry_open_qty_by_symbol(con)

    mismatch_symbols = sorted(
        set(broker_map.keys())
        | set(portfolio_map.keys())
        | set(ledger_fill_map.keys())
        | set(ledger_open_map.keys())
        | set(registry_open_map.keys())
    )

    mismatches = []
    for sym in mismatch_symbols:
        broker_qty = float(broker_map.get(sym) or 0.0)
        portfolio_qty = float(portfolio_map.get(sym) or 0.0)
        ledger_filled_qty = float(ledger_fill_map.get(sym) or 0.0)
        ledger_open_qty = float(ledger_open_map.get(sym) or 0.0)
        registry_open_qty = float(registry_open_map.get(sym) or 0.0)

        max_abs_diff = max(
            abs(broker_qty - portfolio_qty),
            abs(broker_qty - ledger_filled_qty),
            abs(ledger_open_qty - registry_open_qty),
        )
        if max_abs_diff <= 1e-9:
            continue

        mismatches.append(
            {
                "symbol": sym,
                "broker_qty": broker_qty,
                "portfolio_qty": portfolio_qty,
                "ledger_filled_qty": ledger_filled_qty,
                "ledger_open_qty": ledger_open_qty,
                "registry_open_qty": registry_open_qty,
                "broker_vs_portfolio_abs_diff": abs(broker_qty - portfolio_qty),
                "broker_vs_ledger_abs_diff": abs(broker_qty - ledger_filled_qty),
                "open_orders_abs_diff": abs(ledger_open_qty - registry_open_qty),
            }
        )

    _audit_event(
        con,
        event_type="broker_state_reconcile",
        replay_key=f"broker_state_reconcile:{broker_name}",
        status="ok",
        detail={
            "broker": broker_name,
            "broker_positions": broker_map,
            "portfolio_state_positions": portfolio_map,
            "ledger_fill_positions": ledger_fill_map,
            "ledger_open_orders": ledger_open_map,
            "registry_open_orders": registry_open_map,
            "mismatches": mismatches,
        },
    )

    return {
        "broker": broker_name,
        "broker_positions_n": int(len(broker_map)),
        "portfolio_positions_n": int(len(portfolio_map)),
        "ledger_fill_positions_n": int(len(ledger_fill_map)),
        "ledger_open_positions_n": int(len(ledger_open_map)),
        "registry_open_positions_n": int(len(registry_open_map)),
        "mismatched_n": int(len(mismatches)),
    }


def replay_boot_recovery(log=None) -> Dict[str, Any]:
    out = {
        "ok": True,
        "enabled": _enabled(),
        "restore_open_orders": None,
        "broker_fills": None,
        "orphans": None,
        "reconcile": None,
    }

    if not _enabled():
        return out

    con = connect()

    try:
        _ensure_tables(con)

        out["restore_open_orders"] = _restore_open_orders(con)

        broker_name = _broker()
        if broker_name == "ibkr":
            out["broker_fills"] = _recover_ibkr_fills(con)
        elif broker_name == "alpaca":
            out["broker_fills"] = _recover_alpaca_fills(con)
        else:
            out["broker_fills"] = {"fills": 0}

        out["orphans"] = _detect_orphan_fills(con)

        if _reconcile_on_boot():
            reconcile_out: Dict[str, Any] = {
                "pre_live": None,
                "state": None,
            }
            try:
                from engine.execution.position_reconcile import pre_live_position_reconcile
                reconcile_out["pre_live"] = pre_live_position_reconcile(broker=broker_name)
            except Exception as e:
                _warn("crash_recovery.replay_boot_recovery.pre_live_reconcile", e, broker=broker_name)
                reconcile_out["pre_live"] = {"ok": False, "error": str(e)}
            reconcile_out["state"] = _reconcile_broker_state(con)
            out["reconcile"] = reconcile_out
        else:
            out["reconcile"] = {"status": "skipped_disabled"}

        return out

    except Exception as e:
        out["ok"] = False
        out["error"] = str(e)
        try:
            append_event(
                event_type="crash_recovery_failed",
                event_source="engine.runtime.crash_recovery",
                entity_type="crash_recovery",
                entity_id=str(_broker() or "unknown"),
                correlation_id=str(int(_now_ms())),
                payload={
                    "ts_ms": int(_now_ms()),
                    "broker": str(_broker() or "unknown"),
                    "status": "failed",
                    "reason": "crash_recovery_exception",
                    "restore_open_orders": dict(out.get("restore_open_orders") or {}),
                    "broker_fills": dict(out.get("broker_fills") or {}),
                    "orphans": dict(out.get("orphans") or {}),
                    "reconcile": dict(out.get("reconcile") or {}),
                    "error": str(e),
                },
                ts_ms=int(_now_ms()),
            )
        except Exception as event_error:
            _warn("crash_recovery.replay_boot_recovery.append_event", event_error, broker=_broker(), error=str(e))

        if log:
            try:
                log.exception("crash recovery replay failed")
            except Exception as log_error:
                _warn("crash_recovery.replay_boot_recovery.log_exception", log_error)

        return out
    finally:
        try:
            con.close()
        except Exception as e:
            _warn("crash_recovery.replay_boot_recovery.close", e)
