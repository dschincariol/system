"""
FILE: execution_ledger.py

Execution subsystem module for `execution_ledger`.
"""

"""
Execution ledger (SQLite):

- execution_orders: one row per submitted broker order (by client_order_id)
- execution_fills: fills captured later (polling or event-driven)
- execution_metrics: slippage + mark-to-market PnL snapshots
- pnl_attribution: grouped PnL per source_alert_id (signal)

Designed to be broker-agnostic.
"""

import hashlib
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.dbapi_compat import Error as DBAPIError, is_sqlite_error
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.observability import record_component_health
from engine.runtime.storage import connect, connect_rw_direct, get_timescale_client, register_after_commit, run_write_txn
from engine.runtime.event_log import append_event
from engine.runtime.metrics import emit_counter, emit_gauge, emit_timing
from engine.runtime.state_cache import cache_get_or_load, cache_set, cache_invalidate_namespace
from engine.runtime.tracing import trace_event
from engine.strategy.model_marketplace import record_live_fill_attribution
from engine.execution.order_command_boundary import record_order_event as record_execution_boundary_event
from engine.execution import execution_ledger_serialization as _ledger_serialization

# Back-compat: some deployments reference this import elsewhere.
# Keep the import optional so the ledger remains usable in narrower setups.
try:
    from engine.execution.trade_attribution_ledger import upsert_from_latest_pnl_attribution_snapshot  # noqa: F401
except Exception:
    upsert_from_latest_pnl_attribution_snapshot = None  # type: ignore


LOG = logging.getLogger("engine.execution.execution_ledger")
_WARNED_NONFATAL_KEYS: set[str] = set()
_WARNED_NONFATAL_LOCK = threading.Lock()
_EXECUTION_LEDGER_INIT_LOCK = threading.Lock()
_EXECUTION_LEDGER_SCHEMA_MARKER_KEY = "execution_ledger_schema_version"
_EXECUTION_LEDGER_SCHEMA_MARKER_VALUE = "1"
_EXECUTION_LEDGER_SCHEMA_MARKER_CHECKS = (
    ("execution_orders", "idx_execution_orders_model_submit_ts"),
    ("execution_fills", "idx_execution_fills_model_ts"),
    ("execution_metrics", "idx_execution_metrics_ts"),
    ("pnl_attribution", "idx_pnl_attribution_ts"),
)


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        with _WARNED_NONFATAL_LOCK:
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
        component="engine.execution.execution_ledger",
        extra=extra or {},
        include_health=False,
        persist=False,
    )


def _record_execution_ledger_degraded(reason: str, error: BaseException, **extra: Any) -> None:
    health_extra = dict(extra or {})
    health_extra["reason"] = str(reason)
    health_extra["error_type"] = type(error).__name__
    record_component_health(
        "execution_ledger",
        ok=False,
        status="degraded",
        detail=str(reason),
        extra=health_extra,
    )


def _index_exists(con, index_name: str) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='index' AND name=?
            LIMIT 1
            """,
            (str(index_name),),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _execution_ledger_schema_marker_ready() -> bool:
    try:
        from engine.runtime.runtime_meta import meta_get

        marker = str(meta_get(_EXECUTION_LEDGER_SCHEMA_MARKER_KEY, "") or "").strip()
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_LEDGER_INIT_MARKER_READ_FAILED",
            e,
            once_key="init_marker_read",
        )
        return False
    if marker != _EXECUTION_LEDGER_SCHEMA_MARKER_VALUE:
        return False

    con = connect(readonly=True)
    try:
        for table_name, index_name in _EXECUTION_LEDGER_SCHEMA_MARKER_CHECKS:
            if not _table_exists(con, table_name):
                return False
            if not _index_exists(con, index_name):
                return False
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_LEDGER_INIT_MARKER_VERIFY_FAILED",
            e,
            once_key="init_marker_verify",
        )
        return False
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_LEDGER_INIT_MARKER_VERIFY_CLOSE_FAILED",
                e,
                once_key="init_marker_verify_close",
            )
    return True


def _mark_execution_ledger_schema_ready(con) -> None:
    try:
        con.execute(
            """
            INSERT INTO runtime_meta(key, value, updated_ts_ms)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (
                _EXECUTION_LEDGER_SCHEMA_MARKER_KEY,
                _EXECUTION_LEDGER_SCHEMA_MARKER_VALUE,
                int(time.time() * 1000),
            ),
        )
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_LEDGER_INIT_MARKER_SET_FAILED",
            e,
            once_key="init_marker_set",
        )


def _trade_outcome_label(pnl_value: float) -> str:
    return _ledger_serialization.trade_outcome_label(pnl_value)


def _register_timescale_trade_outcomes_after_commit(con, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    client = get_timescale_client()
    if client is None or not bool(getattr(client, "enabled", False)):
        return

    payload = tuple(dict(row) for row in rows)

    def _enqueue() -> None:
        try:
            client.enqueue_trade_outcomes(payload)
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_LEDGER_TIMESCALE_ENQUEUE_FAILED",
                e,
                once_key="execution_ledger_timescale_enqueue",
                rows=int(len(payload)),
            )

    register_after_commit(con, _enqueue)


def _resolve_order_boundary_command_id(
    con,
    *,
    batch_id: Optional[int],
    correlation_id: Optional[str],
    broker: Optional[str],
) -> Optional[str]:
    if not _table_exists(con, "order_commands"):
        return None
    if batch_id is not None:
        row = con.execute(
            """
            SELECT command_id
            FROM order_commands
            WHERE batch_id=CAST(? AS INTEGER)
              AND (CAST(? AS TEXT) IS NULL OR broker=CAST(? AS TEXT))
            ORDER BY ts_ms DESC, command_id DESC
            LIMIT 1
            """,
            (
                int(batch_id),
                (str(broker) if broker not in (None, "") else None),
                (str(broker) if broker not in (None, "") else None),
            ),
        ).fetchone()
        if row and row[0] not in (None, ""):
            return str(row[0])
    if correlation_id not in (None, ""):
        row = con.execute(
            """
            SELECT command_id
            FROM order_commands
            WHERE correlation_id=?
              AND (CAST(? AS TEXT) IS NULL OR broker=CAST(? AS TEXT))
            ORDER BY ts_ms DESC, command_id DESC
            LIMIT 1
            """,
            (
                str(correlation_id),
                (str(broker) if broker not in (None, "") else None),
                (str(broker) if broker not in (None, "") else None),
            ),
        ).fetchone()
        if row and row[0] not in (None, ""):
            return str(row[0])
    return None


def _record_order_boundary_event(
    con,
    *,
    ts_ms: int,
    event_type: str,
    status: str,
    mode: Optional[str],
    broker: Optional[str],
    payload: Optional[Dict[str, Any]],
    batch_id: Optional[int] = None,
    correlation_id: Optional[str] = None,
    command_id: Optional[str] = None,
) -> None:
    resolved_command_id = str(command_id or "").strip() or _resolve_order_boundary_command_id(
        con,
        batch_id=batch_id,
        correlation_id=correlation_id,
        broker=broker,
    )
    record_execution_boundary_event(
        ts_ms=int(ts_ms),
        event_type=str(event_type),
        mode=str(mode or ""),
        broker=str(broker or ""),
        status=str(status),
        payload=dict(payload or {}),
        command_id=(resolved_command_id if resolved_command_id else None),
        batch_id=(int(batch_id) if batch_id is not None else None),
        correlation_id=(str(correlation_id) if correlation_id not in (None, "") else None),
        con=con,
    )


SCHEMA = """
-- ============================================================
-- execution_orders
-- ============================================================
CREATE TABLE IF NOT EXISTS execution_orders (
  client_order_id TEXT PRIMARY KEY,
  order_uid TEXT,
  idempotency_status TEXT,
  broker TEXT NOT NULL,
  portfolio_orders_id INTEGER,
  source_alert_id INTEGER,
  prediction_id INTEGER,
  model_id TEXT NOT NULL DEFAULT 'baseline',
  model_version TEXT,
  symbol TEXT NOT NULL,
  qty REAL NOT NULL,
  submit_ts_ms INTEGER NOT NULL,
  ref_px REAL,
  expected_px REAL,
  mid_px REAL,
  bid_px REAL,
  ask_px REAL,
  spread_bps REAL,
  broker_order_id TEXT,
  status TEXT NOT NULL DEFAULT 'submitted',
  extra_json TEXT
);

-- ============================================================
-- execution_fills
-- ============================================================
CREATE TABLE IF NOT EXISTS execution_fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_order_id TEXT NOT NULL,
  fill_id TEXT,
  broker TEXT,
  model_id TEXT NOT NULL DEFAULT 'baseline',
  model_version TEXT,
  symbol TEXT,
  portfolio_orders_id INTEGER,
  source_alert_id INTEGER,
  prediction_id INTEGER,
  ts_ms INTEGER,
  submit_ts_ms INTEGER,
  fill_ts_ms INTEGER NOT NULL,
  fill_qty REAL NOT NULL,
  fill_px REAL NOT NULL,
  expected_px REAL,
  mid_px REAL,
  bid_px REAL,
  ask_px REAL,
  spread_bps REAL,
  slippage_bps REAL,
  fill_latency_ms INTEGER,
  fees REAL,
  commission REAL,
  liquidity TEXT,
  raw_json TEXT,
  extra_json TEXT
);

-- ============================================================
-- model_position_state
-- ============================================================
CREATE TABLE IF NOT EXISTS model_position_state (
  model_id TEXT NOT NULL DEFAULT 'baseline',
  symbol TEXT NOT NULL,
  net_qty REAL NOT NULL DEFAULT 0,
  avg_entry_price REAL NOT NULL DEFAULT 0,
  realized_pnl REAL NOT NULL DEFAULT 0,
  last_update_ts_ms INTEGER NOT NULL,
  PRIMARY KEY (model_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_model_position_state_updated
  ON model_position_state(last_update_ts_ms);

-- ============================================================
-- execution_metrics
-- ============================================================
CREATE TABLE IF NOT EXISTS execution_metrics (
  ts_ms INTEGER NOT NULL,
  client_order_id TEXT NOT NULL,
  broker TEXT,
  symbol TEXT NOT NULL,
  submit_qty REAL,
  filled_qty REAL,
  ref_px REAL,
  expected_px REAL,
  mid_px REAL,
  fill_px REAL,
  fill_vwap REAL,
  spread_bps REAL,
  slippage_bps REAL,
  fill_latency_ms INTEGER,
  fees REAL,
  m2m_pnl REAL,
  last_px REAL,
  PRIMARY KEY (ts_ms, client_order_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_metrics_client
  ON execution_metrics(client_order_id);

CREATE INDEX IF NOT EXISTS idx_execution_metrics_ts
  ON execution_metrics(ts_ms);

-- ============================================================
-- pnl_attribution
-- ============================================================
CREATE TABLE IF NOT EXISTS pnl_attribution (
  ts_ms INTEGER NOT NULL,
  source_alert_id INTEGER NOT NULL,
  prediction_id INTEGER,
  model_id TEXT NOT NULL DEFAULT 'baseline',
  model_version TEXT,
  symbol TEXT NOT NULL,
  pnl REAL NOT NULL,
  fees REAL NOT NULL,
  slippage_bps REAL,
  position_size REAL,
  avg_price REAL,
  realized_pnl REAL,
  unrealized_pnl REAL,
  extra_json TEXT,
  PRIMARY KEY (ts_ms, source_alert_id, model_id, symbol)
);

-- ============================================================
-- capital_efficiency
-- ============================================================
CREATE TABLE IF NOT EXISTS capital_efficiency (
  ts_ms INTEGER NOT NULL,
  source_alert_id INTEGER NOT NULL,
  model_id TEXT NOT NULL DEFAULT 'baseline',
  model_version TEXT,
  symbol TEXT NOT NULL,
  capital_hours REAL NOT NULL,
  return_per_risk REAL,
  drawdown_contribution REAL,
  efficiency_score REAL,
  extra_json TEXT,
  PRIMARY KEY (ts_ms, source_alert_id, model_id, symbol)
);

-- ============================================================
-- execution_capital_efficiency
-- ============================================================
CREATE TABLE IF NOT EXISTS execution_capital_efficiency (
  ts_ms INTEGER NOT NULL,
  client_order_id TEXT NOT NULL,
  broker TEXT,
  portfolio_orders_id INTEGER,
  source_alert_id INTEGER,
  model_id TEXT NOT NULL DEFAULT 'baseline',
  model_version TEXT,
  strategy_name TEXT,
  symbol TEXT NOT NULL,
  submit_ts_ms INTEGER,
  filled_qty REAL,
  fill_vwap REAL,
  fees REAL,
  notional REAL,
  holding_hours REAL,
  capital_hours REAL,
  pnl_net REAL,
  return_per_risk REAL,
  drawdown_contrib REAL,
  efficiency_score REAL,
  extra_json TEXT,
  PRIMARY KEY (ts_ms, client_order_id)
);

CREATE INDEX IF NOT EXISTS idx_exec_cap_eff_ts
  ON execution_capital_efficiency(ts_ms);

CREATE INDEX IF NOT EXISTS idx_exec_cap_eff_strategy_ts
  ON execution_capital_efficiency(strategy_name, ts_ms);

-- ============================================================
-- execution_fill_quality
-- ============================================================
CREATE TABLE IF NOT EXISTS execution_fill_quality (
  ts_ms INTEGER NOT NULL,
  client_order_id TEXT NOT NULL,
  broker TEXT,
  symbol TEXT NOT NULL,
  order_type TEXT,
  aggressiveness TEXT,
  tod_bucket TEXT,
  spread_bps REAL,
  spread_capture_bps REAL,
  slippage_bps REAL,
  fee_bps REAL,
  total_cost_bps REAL,
  passive_flag INTEGER,
  aggressive_flag INTEGER,
  extra_json TEXT,
  PRIMARY KEY (ts_ms, client_order_id)
);

CREATE INDEX IF NOT EXISTS idx_exec_fill_quality_ts
  ON execution_fill_quality(ts_ms);

CREATE INDEX IF NOT EXISTS idx_exec_fill_quality_symbol_ts
  ON execution_fill_quality(symbol, ts_ms);
"""


def now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json_dict(v: Any) -> Dict[str, Any]:
    return _ledger_serialization.safe_json_dict(v, warn_nonfatal=_warn_nonfatal)


def _safe_json_obj(v: Any) -> Dict[str, Any]:
    return _ledger_serialization.safe_json_obj(v, warn_nonfatal=_warn_nonfatal)


def _safe_float(value: Any, default: float = 0.0) -> float:
    return _ledger_serialization.safe_float(value, default)


def _safe_int(value: Any, default: int = 0) -> int:
    return _ledger_serialization.safe_int(value, default)


def _pick_float(*vals: Any) -> Optional[float]:
    return _ledger_serialization.pick_float(*vals, warn_nonfatal=_warn_nonfatal)


def _extract_strategy_name(extra_payload: Any) -> Optional[str]:
    return _ledger_serialization.extract_strategy_name(
        extra_payload,
        warn_nonfatal=_warn_nonfatal,
    )


def _normalize_model_id(model_id: Any) -> str:
    return _ledger_serialization.normalize_model_id(model_id)


def _extract_model_identity(extra_payload: Any) -> Dict[str, Any]:
    return _ledger_serialization.extract_model_identity(
        extra_payload,
        warn_nonfatal=_warn_nonfatal,
    )


def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _table_columns(con, table_name: str) -> Dict[str, str]:
    rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    return {str(r[1]): str(r[2] or "") for r in rows}


def _safe_identifier_name(value: str) -> str:
    text = str(value or "").strip()
    if not text or not (text[0].isalpha() or text[0] == "_"):
        raise ValueError(f"invalid_identifier:{value}")
    if not all(ch.isalnum() or ch == "_" for ch in text):
        raise ValueError(f"invalid_identifier:{value}")
    return text


def _table_has_unique_key(con, table_name: str, columns: Tuple[str, ...]) -> bool:
    try:
        table = _safe_identifier_name(table_name)
        cols = tuple(_safe_identifier_name(col) for col in columns)
    except ValueError:
        return False
    if not cols:
        return False
    table_cols = _table_columns(con, table)
    if not set(cols).issubset(set(table_cols.keys())):
        return False

    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall() or []
        pk_cols = tuple(str(row[1]) for row in sorted(rows, key=lambda row: int(row[5] or 0)) if int(row[5] or 0) > 0)
        if pk_cols == cols:
            return True
    except Exception as e:
        if not (isinstance(e, (DBAPIError, TypeError, ValueError)) or is_sqlite_error(e)):
            raise
        reason = "unique_key_primary_key_lookup_failed"
        _record_execution_ledger_degraded(
            reason,
            e,
            table=str(table),
            columns=",".join(cols),
        )
        _warn_nonfatal(
            "EXECUTION_LEDGER_UNIQUE_KEY_PRIMARY_KEY_LOOKUP_FAILED",
            e,
            once_key=f"unique_key_primary_key_lookup:{table}:{','.join(cols)}",
            table=str(table),
            columns=",".join(cols),
        )

    if getattr(con, "raw", None) is not None:
        return False

    try:
        index_rows = con.execute(f"PRAGMA index_list({table})").fetchall() or []
        for index_row in index_rows:
            if int(index_row[2] or 0) != 1:
                continue
            index_name = _safe_identifier_name(str(index_row[1]))
            info_rows = con.execute(f"PRAGMA index_info({index_name})").fetchall() or []
            index_cols = tuple(str(row[2]) for row in info_rows)
            if index_cols == cols:
                return True
    except Exception as e:
        if not (isinstance(e, (DBAPIError, TypeError, ValueError)) or is_sqlite_error(e)):
            raise
        reason = "unique_key_index_lookup_failed"
        _record_execution_ledger_degraded(
            reason,
            e,
            table=str(table),
            columns=",".join(cols),
        )
        _warn_nonfatal(
            "EXECUTION_LEDGER_UNIQUE_KEY_INDEX_LOOKUP_FAILED",
            e,
            once_key=f"unique_key_index_lookup:{table}:{','.join(cols)}",
            table=str(table),
            columns=",".join(cols),
        )
        return False
    return False


def _ensure_column(con, table_name: str, column_name: str, column_def: str) -> None:
    cols = _table_columns(con, table_name)
    if column_name in cols:
        return
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def _prediction_exists(con, prediction_id: Optional[int]) -> bool:
    if prediction_id in (None, "") or not _table_exists(con, "predictions"):
        return False
    row = con.execute(
        "SELECT 1 FROM predictions WHERE id=? LIMIT 1",
        (int(prediction_id),),
    ).fetchone()
    return bool(row)


def _validated_prediction_id(con, prediction_id: Any) -> Optional[int]:
    if prediction_id in (None, ""):
        return None
    try:
        value = int(prediction_id)
    except Exception:
        return None
    return value if _prediction_exists(con, value) else None


def _alert_lineage(con, source_alert_id: Any) -> Dict[str, Any]:
    alert_id = _optional_int(source_alert_id)
    if alert_id is None:
        return {
            "source_alert_id": None,
            "prediction_id": None,
            "alert_found": False,
        }
    if not _table_exists(con, "alerts"):
        return {
            "source_alert_id": int(alert_id),
            "prediction_id": None,
            "alert_found": False,
        }
    alert_cols = _table_columns(con, "alerts")
    if "prediction_id" not in alert_cols:
        row = con.execute(
            "SELECT 1 FROM alerts WHERE id=? LIMIT 1",
            (int(alert_id),),
        ).fetchone()
        return {
            "source_alert_id": int(alert_id),
            "prediction_id": None,
            "alert_found": bool(row),
        }
    row = con.execute(
        "SELECT prediction_id FROM alerts WHERE id=? LIMIT 1",
        (int(alert_id),),
    ).fetchone()
    return {
        "source_alert_id": int(alert_id),
        "prediction_id": (_validated_prediction_id(con, row[0]) if row else None),
        "alert_found": bool(row),
    }


def _resolve_typed_lineage(
    con,
    *,
    portfolio_orders_id: Any,
    source_alert_id: Any,
    prediction_id: Any,
) -> Dict[str, Any]:
    portfolio_order_id = _optional_int(portfolio_orders_id)
    portfolio_lineage = _lineage_from_portfolio_order(con, portfolio_order_id)
    if portfolio_order_id is not None and not portfolio_lineage:
        portfolio_order_id = None
    if portfolio_lineage:
        return {
            "portfolio_orders_id": _optional_int(portfolio_lineage.get("portfolio_orders_id")),
            "source_alert_id": _optional_int(portfolio_lineage.get("source_alert_id")),
            "prediction_id": _validated_prediction_id(con, portfolio_lineage.get("prediction_id")),
            "model_id": portfolio_lineage.get("model_id"),
            "symbol": portfolio_lineage.get("symbol"),
            "alert_found": (
                _optional_int(portfolio_lineage.get("source_alert_id")) is not None
                and _table_exists(con, "alerts")
            ),
        }

    source_alert_id_resolved = _optional_int(source_alert_id)
    alert_lineage = _alert_lineage(con, source_alert_id_resolved)
    if bool(alert_lineage.get("alert_found")):
        prediction_id_resolved = _validated_prediction_id(con, alert_lineage.get("prediction_id"))
    elif source_alert_id_resolved is not None:
        prediction_id_resolved = None
    else:
        prediction_id_resolved = _validated_prediction_id(con, prediction_id)
    return {
        "portfolio_orders_id": portfolio_order_id,
        "source_alert_id": source_alert_id_resolved,
        "prediction_id": prediction_id_resolved,
        "model_id": None,
        "symbol": None,
        "alert_found": bool(alert_lineage.get("alert_found")),
    }


def _lineage_from_portfolio_order(con, portfolio_orders_id: Optional[int]) -> Dict[str, Any]:
    order_id = _optional_int(portfolio_orders_id)
    if order_id is None or not _table_exists(con, "portfolio_orders"):
        return {}
    portfolio_cols = _table_columns(con, "portfolio_orders")
    select_cols = [
        "source_alert_id" if "source_alert_id" in portfolio_cols else "NULL AS source_alert_id",
        "prediction_id" if "prediction_id" in portfolio_cols else "NULL AS prediction_id",
        "model_id" if "model_id" in portfolio_cols else "NULL AS model_id",
        "symbol" if "symbol" in portfolio_cols else "NULL AS symbol",
        "explain_json" if "explain_json" in portfolio_cols else "NULL AS explain_json",
    ]
    row = con.execute(
        f"""
        SELECT {", ".join(select_cols)}
        FROM portfolio_orders
        WHERE id=?
        LIMIT 1
        """,
        (int(order_id),),
    ).fetchone()
    if not row:
        return {}

    explain = _safe_json_obj(row[4])
    source_alert_id = _optional_int(row[0])
    prediction_id = _validated_prediction_id(con, row[1])
    alert_lineage = _alert_lineage(con, source_alert_id)

    if prediction_id is None and bool(alert_lineage.get("alert_found")):
        prediction_id = _validated_prediction_id(con, alert_lineage.get("prediction_id"))

    if prediction_id is None and not bool(alert_lineage.get("alert_found")):
        prediction_id = _validated_prediction_id(con, explain.get("prediction_id"))

    return {
        "portfolio_orders_id": int(order_id),
        "source_alert_id": source_alert_id,
        "prediction_id": prediction_id,
        "model_id": (_normalize_model_id(row[2]) if row[2] not in (None, "") else None),
        "symbol": (str(row[3]) if row[3] not in (None, "") else None),
    }


def _backfill_execution_order_lineage(con) -> None:
    if not _table_exists(con, "execution_orders"):
        return
    if _table_exists(con, "portfolio_orders"):
        portfolio_cols = _table_columns(con, "portfolio_orders")
        if "source_alert_id" in portfolio_cols:
            con.execute(
                """
                UPDATE execution_orders
                SET source_alert_id = (
                  SELECT p.source_alert_id
                  FROM portfolio_orders p
                  WHERE p.id = execution_orders.portfolio_orders_id
                  LIMIT 1
                )
                WHERE portfolio_orders_id IS NOT NULL
                  AND COALESCE(source_alert_id, -1) <> COALESCE(
                    (
                      SELECT p.source_alert_id
                      FROM portfolio_orders p
                      WHERE p.id = execution_orders.portfolio_orders_id
                      LIMIT 1
                    ),
                    -1
                  )
                  AND EXISTS(
                    SELECT 1
                    FROM portfolio_orders p
                    WHERE p.id = execution_orders.portfolio_orders_id
                  )
                """
            )
        if "prediction_id" in portfolio_cols:
            con.execute(
                """
                UPDATE execution_orders
                SET prediction_id = (
                  SELECT p.prediction_id
                  FROM portfolio_orders p
                  WHERE p.id = execution_orders.portfolio_orders_id
                  LIMIT 1
                )
                WHERE portfolio_orders_id IS NOT NULL
                  AND COALESCE(prediction_id, -1) <> COALESCE(
                    (
                      SELECT p.prediction_id
                      FROM portfolio_orders p
                      WHERE p.id = execution_orders.portfolio_orders_id
                      LIMIT 1
                    ),
                    -1
                  )
                  AND EXISTS(
                    SELECT 1
                    FROM portfolio_orders p
                    WHERE p.id = execution_orders.portfolio_orders_id
                  )
                """
            )
        con.execute(
            """
            UPDATE execution_orders
            SET portfolio_orders_id = NULL
            WHERE portfolio_orders_id IS NOT NULL
              AND NOT EXISTS(
                SELECT 1
                FROM portfolio_orders p
                WHERE p.id = execution_orders.portfolio_orders_id
              )
            """
        )
    if _table_exists(con, "alerts"):
        alert_cols = _table_columns(con, "alerts")
        if "prediction_id" in alert_cols:
            con.execute(
                """
                UPDATE execution_orders
                SET prediction_id = (
                  SELECT a.prediction_id
                  FROM alerts a
                  WHERE a.id = execution_orders.source_alert_id
                  LIMIT 1
                )
                WHERE portfolio_orders_id IS NULL
                  AND source_alert_id IS NOT NULL
                  AND COALESCE(prediction_id, -1) <> COALESCE(
                    (
                      SELECT a.prediction_id
                      FROM alerts a
                      WHERE a.id = execution_orders.source_alert_id
                      LIMIT 1
                    ),
                    -1
                  )
                  AND EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = execution_orders.source_alert_id
                  )
                """
            )
            con.execute(
                """
                UPDATE execution_orders
                SET prediction_id = NULL
                WHERE portfolio_orders_id IS NULL
                  AND source_alert_id IS NOT NULL
                  AND prediction_id IS NOT NULL
                  AND EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = execution_orders.source_alert_id
                      AND a.prediction_id IS NULL
                  )
                """
            )
            con.execute(
                """
                UPDATE execution_orders
                SET prediction_id = NULL
                WHERE portfolio_orders_id IS NULL
                  AND source_alert_id IS NOT NULL
                  AND prediction_id IS NOT NULL
                  AND NOT EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = execution_orders.source_alert_id
                  )
                """
            )
    if _table_exists(con, "predictions"):
        con.execute(
            """
            UPDATE execution_orders
            SET prediction_id = NULL
            WHERE prediction_id IS NOT NULL
              AND NOT EXISTS(
                SELECT 1
                FROM predictions p
                WHERE p.id = execution_orders.prediction_id
              )
            """
        )
    else:
        con.execute("UPDATE execution_orders SET prediction_id = NULL WHERE prediction_id IS NOT NULL")


def _backfill_execution_fill_lineage(con) -> None:
    if not _table_exists(con, "execution_fills"):
        return
    if _table_exists(con, "execution_orders"):
        con.execute(
            """
            UPDATE execution_fills
            SET portfolio_orders_id = (
                  SELECT o.portfolio_orders_id
                  FROM execution_orders o
                  WHERE o.client_order_id = execution_fills.client_order_id
                  LIMIT 1
                ),
                source_alert_id = (
                  SELECT o.source_alert_id
                  FROM execution_orders o
                  WHERE o.client_order_id = execution_fills.client_order_id
                  LIMIT 1
                ),
                prediction_id = (
                  SELECT o.prediction_id
                  FROM execution_orders o
                  WHERE o.client_order_id = execution_fills.client_order_id
                  LIMIT 1
                )
            WHERE client_order_id IS NOT NULL
              AND (
                COALESCE(portfolio_orders_id, -1) <> COALESCE(
                  (
                    SELECT o.portfolio_orders_id
                    FROM execution_orders o
                    WHERE o.client_order_id = execution_fills.client_order_id
                    LIMIT 1
                  ),
                  -1
                )
                OR COALESCE(source_alert_id, -1) <> COALESCE(
                  (
                    SELECT o.source_alert_id
                    FROM execution_orders o
                    WHERE o.client_order_id = execution_fills.client_order_id
                    LIMIT 1
                  ),
                  -1
                )
                OR COALESCE(prediction_id, -1) <> COALESCE(
                  (
                    SELECT o.prediction_id
                    FROM execution_orders o
                    WHERE o.client_order_id = execution_fills.client_order_id
                    LIMIT 1
                  ),
                  -1
                )
              )
              AND EXISTS(
                SELECT 1
                FROM execution_orders o
                WHERE o.client_order_id = execution_fills.client_order_id
              )
            """
        )
    if _table_exists(con, "portfolio_orders"):
        con.execute(
            """
            UPDATE execution_fills
            SET source_alert_id = (
                  SELECT p.source_alert_id
                  FROM portfolio_orders p
                  WHERE p.id = execution_fills.portfolio_orders_id
                  LIMIT 1
                ),
                prediction_id = (
                  SELECT p.prediction_id
                  FROM portfolio_orders p
                  WHERE p.id = execution_fills.portfolio_orders_id
                  LIMIT 1
                )
            WHERE portfolio_orders_id IS NOT NULL
              AND (
                COALESCE(source_alert_id, -1) <> COALESCE(
                  (
                    SELECT p.source_alert_id
                    FROM portfolio_orders p
                    WHERE p.id = execution_fills.portfolio_orders_id
                    LIMIT 1
                  ),
                  -1
                )
                OR COALESCE(prediction_id, -1) <> COALESCE(
                  (
                    SELECT p.prediction_id
                    FROM portfolio_orders p
                    WHERE p.id = execution_fills.portfolio_orders_id
                    LIMIT 1
                  ),
                  -1
                )
              )
              AND EXISTS(
                SELECT 1
                FROM portfolio_orders p
                WHERE p.id = execution_fills.portfolio_orders_id
              )
            """
        )
        con.execute(
            """
            UPDATE execution_fills
            SET portfolio_orders_id = NULL
            WHERE portfolio_orders_id IS NOT NULL
              AND NOT EXISTS(
                SELECT 1
                FROM portfolio_orders p
                WHERE p.id = execution_fills.portfolio_orders_id
              )
            """
        )
    if _table_exists(con, "alerts"):
        alert_cols = _table_columns(con, "alerts")
        if "prediction_id" in alert_cols:
            con.execute(
                """
                UPDATE execution_fills
                SET prediction_id = (
                  SELECT a.prediction_id
                  FROM alerts a
                  WHERE a.id = execution_fills.source_alert_id
                  LIMIT 1
                )
                WHERE portfolio_orders_id IS NULL
                  AND source_alert_id IS NOT NULL
                  AND COALESCE(prediction_id, -1) <> COALESCE(
                    (
                      SELECT a.prediction_id
                      FROM alerts a
                      WHERE a.id = execution_fills.source_alert_id
                      LIMIT 1
                    ),
                    -1
                  )
                  AND EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = execution_fills.source_alert_id
                  )
                """
            )
            con.execute(
                """
                UPDATE execution_fills
                SET prediction_id = NULL
                WHERE portfolio_orders_id IS NULL
                  AND source_alert_id IS NOT NULL
                  AND prediction_id IS NOT NULL
                  AND NOT EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = execution_fills.source_alert_id
                  )
                """
            )
    if _table_exists(con, "predictions"):
        con.execute(
            """
            UPDATE execution_fills
            SET prediction_id = NULL
            WHERE prediction_id IS NOT NULL
              AND NOT EXISTS(
                SELECT 1
                FROM predictions p
                WHERE p.id = execution_fills.prediction_id
              )
            """
        )
    else:
        con.execute("UPDATE execution_fills SET prediction_id = NULL WHERE prediction_id IS NOT NULL")


def _resolve_prediction_id_for_order(
    con,
    *,
    source_alert_id: Optional[int],
    symbol: str,
    model_id: str,
    extra: Dict[str, Any],
) -> Optional[int]:
    event_id: Optional[int] = None
    horizon_s: Optional[int] = None
    model_name = str(extra.get("model_name") or "").strip()
    if _safe_int(source_alert_id, 0) > 0 and _table_exists(con, "alerts"):
        alert_cols = _table_columns(con, "alerts")
        select_cols = ["event_id", "symbol", "horizon_s"]
        for optional in ("prediction_id", "model_name", "model_id"):
            if optional in alert_cols:
                select_cols.append(optional)
        row = con.execute(
            f"SELECT {', '.join(select_cols)} FROM alerts WHERE id=? LIMIT 1",
            (int(source_alert_id),),
        ).fetchone()
        if row:
            data = {name: row[idx] for idx, name in enumerate(select_cols)}
            if data.get("prediction_id") not in (None, ""):
                try:
                    validated = _validated_prediction_id(con, data["prediction_id"])
                    if validated is not None:
                        return validated
                except Exception as e:
                    _warn_nonfatal(
                        "EXECUTION_LEDGER_ALERT_PREDICTION_ID_PARSE_FAILED",
                        e,
                        once_key=f"execution_ledger_alert_prediction_id:{source_alert_id}",
                        source_alert_id=int(source_alert_id),
                        prediction_id=repr(data.get("prediction_id")),
                    )
            return None
        return None

    explicit_prediction_id = extra.get("prediction_id")
    if explicit_prediction_id not in (None, ""):
        try:
            validated = _validated_prediction_id(con, explicit_prediction_id)
            if validated is not None:
                return validated
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_LEDGER_PREDICTION_ID_PARSE_FAILED",
                e,
                once_key=f"prediction_id_parse:{explicit_prediction_id}",
                raw_value=explicit_prediction_id,
            )

    if not _table_exists(con, "predictions"):
        return None

    if event_id is None and extra.get("event_id") is not None:
        event_id = _safe_int(extra.get("event_id"), 0) or None
    if horizon_s is None and extra.get("horizon_s") is not None:
        horizon_s = _safe_int(extra.get("horizon_s"), 0) or None

    symbol_u = str(symbol or "").strip().upper()
    if event_id is None or horizon_s is None or not symbol_u:
        return None

    rows = con.execute(
        """
        SELECT id, model_name, model_id, ts_ms
        FROM predictions
        WHERE event_id=? AND UPPER(TRIM(symbol))=? AND horizon_s=?
        ORDER BY ts_ms DESC, id DESC
        """,
        (int(event_id), str(symbol_u), int(horizon_s)),
    ).fetchall()
    if not rows:
        return None

    resolved_model_id = _normalize_model_id(model_id)

    def _rank(row: Any) -> tuple[int, int, int]:
        row_model_name = str(row[1] or "").strip()
        row_model_id = _normalize_model_id(row[2])
        match = 2
        if resolved_model_id and row_model_id == resolved_model_id:
            match = 0
        elif model_name and row_model_name == model_name:
            match = 1
        return (match, -_safe_int(row[3], 0), -_safe_int(row[0], 0))

    best = min(rows, key=_rank)
    try:
        return _validated_prediction_id(con, best[0])
    except Exception:
        return None


def _rebuild_execution_orders_if_needed(con) -> None:
    if not _table_exists(con, "execution_orders"):
        return
    if hasattr(con, "raw"):
        return

    sql_row = con.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name='execution_orders'
        """
    ).fetchone()
    sql = str(sql_row[0] or "") if sql_row else ""

    portfolio_orders_available = _table_exists(con, "portfolio_orders")
    alerts_available = _table_exists(con, "alerts")
    predictions_available = _table_exists(con, "predictions")
    alert_cols = _table_columns(con, "alerts") if alerts_available else {}
    portfolio_cols = _table_columns(con, "portfolio_orders") if portfolio_orders_available else {}
    alert_prediction_lineage_available = alerts_available and ("prediction_id" in alert_cols)
    portfolio_prediction_lineage_available = portfolio_orders_available and {
        "source_alert_id",
        "prediction_id",
    }.issubset(set(portfolio_cols.keys()))
    if alert_prediction_lineage_available:
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_id_prediction_lineage
              ON alerts(id, prediction_id)
            """
        )
    if portfolio_prediction_lineage_available:
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_orders_id_source_prediction_lineage
              ON portfolio_orders(id, source_alert_id, prediction_id, ts_ms)
            """
        )
    portfolio_orders_fk_available = portfolio_orders_available and _table_has_unique_key(con, "portfolio_orders", ("id",))
    predictions_fk_available = predictions_available and _table_has_unique_key(con, "predictions", ("id",))
    alert_prediction_fk_available = alert_prediction_lineage_available and _table_has_unique_key(
        con,
        "alerts",
        ("id", "prediction_id"),
    )
    portfolio_prediction_fk_available = portfolio_prediction_lineage_available and _table_has_unique_key(
        con,
        "portfolio_orders",
        ("id", "source_alert_id", "prediction_id"),
    )
    cols = _table_columns(con, "execution_orders")
    needs_rebuild = False

    required_cols = {
        "order_uid",
        "idempotency_status",
        "portfolio_orders_id",
        "prediction_id",
        "model_id",
        "model_version",
        "expected_px",
        "mid_px",
        "bid_px",
        "ask_px",
        "spread_bps",
    }
    if not required_cols.issubset(set(cols.keys())):
        needs_rebuild = True

    sql_upper = sql.upper().replace(" ", "").replace("\n", "")
    if portfolio_orders_fk_available and "FOREIGNKEY(PORTFOLIO_ORDERS_ID)REFERENCESPORTFOLIO_ORDERS(ID)ONDELETESETNULL" not in sql_upper:
        needs_rebuild = True
    if predictions_fk_available and "FOREIGNKEY(PREDICTION_ID)REFERENCESPREDICTIONS(ID)ONDELETESETNULL" not in sql_upper:
        needs_rebuild = True
    if alert_prediction_fk_available and "FOREIGNKEY(SOURCE_ALERT_ID,PREDICTION_ID)REFERENCESALERTS(ID,PREDICTION_ID)ONDELETESETNULL" not in sql_upper:
        needs_rebuild = True
    if portfolio_prediction_fk_available and (
        "FOREIGNKEY(PORTFOLIO_ORDERS_ID,SOURCE_ALERT_ID,PREDICTION_ID)"
        "REFERENCESPORTFOLIO_ORDERS(ID,SOURCE_ALERT_ID,PREDICTION_ID)ONDELETESETNULL"
        not in sql_upper
    ):
        needs_rebuild = True

    if not needs_rebuild:
        return

    def _legacy_expr(column_name: str, fallback_sql: str) -> str:
        if column_name in cols:
            return column_name
        return fallback_sql

    raw_portfolio_order_expr = _legacy_expr("portfolio_orders_id", "NULL")
    if portfolio_orders_available:
        portfolio_order_expr = (
            "CASE "
            f"WHEN {raw_portfolio_order_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM portfolio_orders p WHERE p.id = {raw_portfolio_order_expr}) "
            f"THEN {raw_portfolio_order_expr} "
            "ELSE NULL END"
        )
    else:
        portfolio_order_expr = "NULL"

    legacy_source_alert_expr = _legacy_expr("source_alert_id", "NULL")
    if portfolio_orders_available:
        source_alert_expr = (
            "CASE "
            f"WHEN {portfolio_order_expr} IS NOT NULL THEN ("
            "SELECT p.source_alert_id "
            "FROM portfolio_orders p "
            f"WHERE p.id = {portfolio_order_expr} "
            "LIMIT 1"
            ") "
            f"ELSE {legacy_source_alert_expr} END"
        )
        portfolio_prediction_expr = (
            "("
            "SELECT p.prediction_id "
            "FROM portfolio_orders p "
            f"WHERE p.id = {portfolio_order_expr} "
            "LIMIT 1"
            ")"
        )
    else:
        source_alert_expr = legacy_source_alert_expr
        portfolio_prediction_expr = "NULL"

    legacy_prediction_value_expr = _legacy_expr("prediction_id", "NULL")
    prediction_without_portfolio_expr = "NULL"
    if alert_prediction_lineage_available and predictions_available:
        prediction_without_portfolio_expr = (
            "CASE "
            f"WHEN {source_alert_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM alerts a WHERE a.id = {source_alert_expr}) THEN ("
            "SELECT CASE "
            "WHEN a.prediction_id IS NOT NULL "
            "  AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = a.prediction_id) "
            "THEN a.prediction_id "
            "ELSE NULL END "
            "FROM alerts a "
            f"WHERE a.id = {source_alert_expr} "
            "LIMIT 1"
            ") "
            f"WHEN {source_alert_expr} IS NOT NULL THEN NULL "
            f"WHEN {legacy_prediction_value_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = {legacy_prediction_value_expr}) "
            f"THEN {legacy_prediction_value_expr} "
            "ELSE NULL END"
        )
    elif predictions_available:
        prediction_without_portfolio_expr = (
            "CASE "
            f"WHEN {legacy_prediction_value_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = {legacy_prediction_value_expr}) "
            f"THEN {legacy_prediction_value_expr} "
            "ELSE NULL END"
        )
    else:
        prediction_without_portfolio_expr = "NULL"

    if portfolio_orders_available:
        prediction_expr = (
            "CASE "
            f"WHEN {portfolio_order_expr} IS NOT NULL THEN {portfolio_prediction_expr} "
            f"ELSE {prediction_without_portfolio_expr} END"
        )
    else:
        prediction_expr = prediction_without_portfolio_expr

    fk_clauses = []
    if portfolio_orders_fk_available:
        fk_clauses.append("FOREIGN KEY(portfolio_orders_id) REFERENCES portfolio_orders(id) ON DELETE SET NULL")
    if predictions_fk_available:
        fk_clauses.append("FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL")
    if alert_prediction_fk_available:
        fk_clauses.append("FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL")
    if portfolio_prediction_fk_available:
        fk_clauses.append(
            "FOREIGN KEY(portfolio_orders_id, source_alert_id, prediction_id) "
            "REFERENCES portfolio_orders(id, source_alert_id, prediction_id) ON DELETE SET NULL"
        )
    fk_sql = ""
    if fk_clauses:
        fk_sql = ",\n          " + ",\n          ".join(fk_clauses)

    con.executescript(
        """
        DROP INDEX IF EXISTS uq_execution_orders_order_uid;
        DROP INDEX IF EXISTS idx_execution_orders_submit_ts;
        DROP INDEX IF EXISTS idx_execution_orders_source_alert;
        DROP INDEX IF EXISTS idx_execution_orders_portfolio_order_submit_ts;
        DROP INDEX IF EXISTS idx_execution_orders_prediction_submit_ts;
        DROP INDEX IF EXISTS idx_execution_orders_source_alert_prediction_submit_ts;
        DROP INDEX IF EXISTS idx_execution_orders_broker_order_id;
        DROP INDEX IF EXISTS idx_execution_orders_model_submit_ts;
        DROP INDEX IF EXISTS idx_execution_orders_symbol_submit_ts;
        DROP INDEX IF EXISTS idx_execution_orders_order_uid;
        ALTER TABLE execution_orders RENAME TO execution_orders_old;
        """
    )

    con.execute(
        f"""
        CREATE TABLE execution_orders (
          client_order_id TEXT PRIMARY KEY,
          order_uid TEXT,
          idempotency_status TEXT,
          broker TEXT NOT NULL,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          prediction_id INTEGER,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_version TEXT,
          symbol TEXT NOT NULL,
          qty REAL NOT NULL,
          submit_ts_ms INTEGER NOT NULL,
          ref_px REAL,
          expected_px REAL,
          mid_px REAL,
          bid_px REAL,
          ask_px REAL,
          spread_bps REAL,
          broker_order_id TEXT,
          status TEXT NOT NULL DEFAULT 'submitted',
          extra_json TEXT{fk_sql}
        )
        """
    )
    con.execute(
        f"""
        INSERT INTO execution_orders(
          client_order_id,
          order_uid,
          idempotency_status,
          broker,
          portfolio_orders_id,
          source_alert_id,
          prediction_id,
          model_id,
          model_version,
          symbol,
          qty,
          submit_ts_ms,
          ref_px,
          expected_px,
          mid_px,
          bid_px,
          ask_px,
          spread_bps,
          broker_order_id,
          status,
          extra_json
        )
        SELECT
          client_order_id,
          {_legacy_expr("order_uid", "NULL")},
          {_legacy_expr("idempotency_status", "NULL")},
          broker,
          {portfolio_order_expr},
          {source_alert_expr},
          {prediction_expr},
          COALESCE(NULLIF(TRIM({_legacy_expr("model_id", "'baseline'")}), ''), 'baseline'),
          {_legacy_expr("model_version", "NULL")},
          symbol,
          qty,
          submit_ts_ms,
          {_legacy_expr("ref_px", "NULL")},
          {_legacy_expr("expected_px", "NULL")},
          {_legacy_expr("mid_px", "NULL")},
          {_legacy_expr("bid_px", "NULL")},
          {_legacy_expr("ask_px", "NULL")},
          {_legacy_expr("spread_bps", "NULL")},
          {_legacy_expr("broker_order_id", "NULL")},
          COALESCE(NULLIF(TRIM({_legacy_expr("status", "'submitted'")}), ''), 'submitted'),
          {_legacy_expr("extra_json", "NULL")}
        FROM execution_orders_old
        """
    )
    con.execute("DROP TABLE execution_orders_old")
    con.executescript(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_orders_order_uid
          ON execution_orders(order_uid);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_submit_ts
          ON execution_orders(submit_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_source_alert
          ON execution_orders(source_alert_id);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_portfolio_order_submit_ts
          ON execution_orders(portfolio_orders_id, submit_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_prediction_submit_ts
          ON execution_orders(prediction_id, submit_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_source_alert_prediction_submit_ts
          ON execution_orders(source_alert_id, prediction_id, submit_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_broker_order_id
          ON execution_orders(broker_order_id);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_model_submit_ts
          ON execution_orders(model_id, submit_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_symbol_submit_ts
          ON execution_orders(symbol, submit_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_orders_order_uid
          ON execution_orders(order_uid);
        """
    )


def _rebuild_execution_fills_if_needed(con) -> None:
    if not _table_exists(con, "execution_fills"):
        return
    if hasattr(con, "raw"):
        return

    sql_row = con.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name='execution_fills'
        """
    ).fetchone()
    sql = str(sql_row[0] or "") if sql_row else ""

    portfolio_orders_available = _table_exists(con, "portfolio_orders")
    execution_orders_available = _table_exists(con, "execution_orders")
    alerts_available = _table_exists(con, "alerts")
    predictions_available = _table_exists(con, "predictions")
    alert_cols = _table_columns(con, "alerts") if alerts_available else {}
    portfolio_cols = _table_columns(con, "portfolio_orders") if portfolio_orders_available else {}
    alert_prediction_lineage_available = alerts_available and ("prediction_id" in alert_cols)
    portfolio_prediction_lineage_available = portfolio_orders_available and {
        "source_alert_id",
        "prediction_id",
    }.issubset(set(portfolio_cols.keys()))
    if alert_prediction_lineage_available:
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_id_prediction_lineage
              ON alerts(id, prediction_id)
            """
        )
    if portfolio_prediction_lineage_available:
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_orders_id_source_prediction_lineage
              ON portfolio_orders(id, source_alert_id, prediction_id, ts_ms)
            """
        )
    portfolio_orders_fk_available = portfolio_orders_available and _table_has_unique_key(con, "portfolio_orders", ("id",))
    predictions_fk_available = predictions_available and _table_has_unique_key(con, "predictions", ("id",))
    alert_prediction_fk_available = alert_prediction_lineage_available and _table_has_unique_key(
        con,
        "alerts",
        ("id", "prediction_id"),
    )
    portfolio_prediction_fk_available = portfolio_prediction_lineage_available and _table_has_unique_key(
        con,
        "portfolio_orders",
        ("id", "source_alert_id", "prediction_id"),
    )

    cols = _table_columns(con, "execution_fills")
    needs_rebuild = False

    if "client_order_id TEXT NOT NULL UNIQUE" in sql:
        needs_rebuild = True

    required_cols = {
        "model_id",
        "model_version",
        "portfolio_orders_id",
        "source_alert_id",
        "prediction_id",
        "ts_ms",
        "submit_ts_ms",
        "expected_px",
        "mid_px",
        "bid_px",
        "ask_px",
        "spread_bps",
        "slippage_bps",
        "fill_latency_ms",
        "commission",
    }
    if not required_cols.issubset(set(cols.keys())):
        needs_rebuild = True

    sql_upper = sql.upper().replace(" ", "").replace("\n", "")
    if portfolio_orders_fk_available and "FOREIGNKEY(PORTFOLIO_ORDERS_ID)REFERENCESPORTFOLIO_ORDERS(ID)ONDELETESETNULL" not in sql_upper:
        needs_rebuild = True
    if predictions_fk_available and "FOREIGNKEY(PREDICTION_ID)REFERENCESPREDICTIONS(ID)ONDELETESETNULL" not in sql_upper:
        needs_rebuild = True
    if alert_prediction_fk_available and "FOREIGNKEY(SOURCE_ALERT_ID,PREDICTION_ID)REFERENCESALERTS(ID,PREDICTION_ID)ONDELETESETNULL" not in sql_upper:
        needs_rebuild = True
    if portfolio_prediction_fk_available and (
        "FOREIGNKEY(PORTFOLIO_ORDERS_ID,SOURCE_ALERT_ID,PREDICTION_ID)"
        "REFERENCESPORTFOLIO_ORDERS(ID,SOURCE_ALERT_ID,PREDICTION_ID)ONDELETESETNULL"
        not in sql_upper
    ):
        needs_rebuild = True

    if not needs_rebuild:
        return

    def _legacy_expr(column_name: str, fallback_sql: str) -> str:
        if column_name in cols:
            return column_name
        return fallback_sql

    fees_expr = _legacy_expr("fees", "NULL")
    commission_expr = f"COALESCE({_legacy_expr('commission', 'NULL')}, {fees_expr})"
    raw_portfolio_order_expr = _legacy_expr("portfolio_orders_id", "NULL")
    if portfolio_orders_available:
        raw_valid_portfolio_order_expr = (
            "CASE "
            f"WHEN {raw_portfolio_order_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM portfolio_orders p WHERE p.id = {raw_portfolio_order_expr}) "
            f"THEN {raw_portfolio_order_expr} "
            "ELSE NULL END"
        )
    else:
        raw_valid_portfolio_order_expr = "NULL"

    if execution_orders_available:
        order_exists_expr = (
            "EXISTS("
            "SELECT 1 FROM execution_orders o "
            "WHERE o.client_order_id = execution_fills_old.client_order_id"
            ")"
        )
        order_portfolio_order_expr = (
            "("
            "SELECT o.portfolio_orders_id "
            "FROM execution_orders o "
            "WHERE o.client_order_id = execution_fills_old.client_order_id "
            "LIMIT 1"
            ")"
        )
        order_source_alert_expr = (
            "("
            "SELECT o.source_alert_id "
            "FROM execution_orders o "
            "WHERE o.client_order_id = execution_fills_old.client_order_id "
            "LIMIT 1"
            ")"
        )
        order_prediction_expr = (
            "("
            "SELECT o.prediction_id "
            "FROM execution_orders o "
            "WHERE o.client_order_id = execution_fills_old.client_order_id "
            "LIMIT 1"
            ")"
        )
        if portfolio_orders_available:
            order_valid_portfolio_order_expr = (
                "CASE "
                f"WHEN {order_portfolio_order_expr} IS NOT NULL "
                f"AND EXISTS(SELECT 1 FROM portfolio_orders p WHERE p.id = {order_portfolio_order_expr}) "
                f"THEN {order_portfolio_order_expr} "
                "ELSE NULL END"
            )
        else:
            order_valid_portfolio_order_expr = "NULL"
    else:
        order_exists_expr = "0"
        order_portfolio_order_expr = "NULL"
        order_source_alert_expr = "NULL"
        order_prediction_expr = "NULL"
        order_valid_portfolio_order_expr = "NULL"

    if portfolio_orders_available:
        portfolio_order_expr = (
            "CASE "
            f"WHEN {order_exists_expr} THEN {order_valid_portfolio_order_expr} "
            f"ELSE {raw_valid_portfolio_order_expr} END"
        )
        portfolio_source_alert_expr = (
            "("
            "SELECT p.source_alert_id "
            "FROM portfolio_orders p "
            f"WHERE p.id = {portfolio_order_expr} "
            "LIMIT 1"
            ")"
        )
        portfolio_prediction_expr = (
            "("
            "SELECT p.prediction_id "
            "FROM portfolio_orders p "
            f"WHERE p.id = {portfolio_order_expr} "
            "LIMIT 1"
            ")"
        )
    else:
        portfolio_order_expr = "NULL"
        portfolio_source_alert_expr = "NULL"
        portfolio_prediction_expr = "NULL"

    raw_source_alert_expr = _legacy_expr("source_alert_id", "NULL")
    if portfolio_orders_available:
        source_alert_expr = (
            "CASE "
            f"WHEN {portfolio_order_expr} IS NOT NULL THEN {portfolio_source_alert_expr} "
            f"WHEN {order_exists_expr} THEN {order_source_alert_expr} "
            f"ELSE {raw_source_alert_expr} END"
        )
    elif execution_orders_available:
        source_alert_expr = (
            "CASE "
            f"WHEN {order_exists_expr} THEN {order_source_alert_expr} "
            f"ELSE {raw_source_alert_expr} END"
        )
    else:
        source_alert_expr = raw_source_alert_expr

    raw_prediction_expr = _legacy_expr("prediction_id", "NULL")
    prediction_without_parents_expr = "NULL"
    if alert_prediction_lineage_available and predictions_available:
        prediction_without_parents_expr = (
            "CASE "
            f"WHEN {source_alert_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM alerts a WHERE a.id = {source_alert_expr}) THEN ("
            "SELECT CASE "
            "WHEN a.prediction_id IS NOT NULL "
            "  AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = a.prediction_id) "
            "THEN a.prediction_id "
            "ELSE NULL END "
            "FROM alerts a "
            f"WHERE a.id = {source_alert_expr} "
            "LIMIT 1"
            ") "
            f"WHEN {source_alert_expr} IS NOT NULL THEN NULL "
            f"WHEN {raw_prediction_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = {raw_prediction_expr}) "
            f"THEN {raw_prediction_expr} "
            "ELSE NULL END"
        )
    elif predictions_available:
        prediction_without_parents_expr = (
            "CASE "
            f"WHEN {raw_prediction_expr} IS NOT NULL "
            f"AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = {raw_prediction_expr}) "
            f"THEN {raw_prediction_expr} "
            "ELSE NULL END"
        )
    else:
        prediction_without_parents_expr = "NULL"

    if portfolio_orders_available:
        prediction_expr = (
            "CASE "
            f"WHEN {portfolio_order_expr} IS NOT NULL THEN {portfolio_prediction_expr} "
            f"WHEN {order_exists_expr} THEN {order_prediction_expr} "
            f"ELSE {prediction_without_parents_expr} END"
        )
    elif execution_orders_available:
        prediction_expr = (
            "CASE "
            f"WHEN {order_exists_expr} THEN {order_prediction_expr} "
            f"ELSE {prediction_without_parents_expr} END"
        )
    else:
        prediction_expr = prediction_without_parents_expr

    con.executescript(
        """
        DROP INDEX IF EXISTS idx_execution_fills_ts;
        DROP INDEX IF EXISTS idx_execution_fills_client;
        DROP INDEX IF EXISTS idx_execution_fills_model_ts;
        DROP INDEX IF EXISTS idx_execution_fills_model_symbol_ts;
        DROP INDEX IF EXISTS idx_execution_fills_portfolio_order_ts;
        DROP INDEX IF EXISTS idx_execution_fills_source_alert_ts;
        DROP INDEX IF EXISTS idx_execution_fills_prediction_ts;
        DROP INDEX IF EXISTS idx_execution_fills_source_alert_prediction_ts;
        DROP INDEX IF EXISTS idx_execution_fills_symbol_ts;
        DROP INDEX IF EXISTS idx_execution_fills_fill_id;
        DROP INDEX IF EXISTS uq_execution_fills_client_fillid;
        ALTER TABLE execution_fills RENAME TO execution_fills_old;
        """
    )
    fk_clauses = []
    if portfolio_orders_fk_available:
        fk_clauses.append("FOREIGN KEY(portfolio_orders_id) REFERENCES portfolio_orders(id) ON DELETE SET NULL")
    if predictions_fk_available:
        fk_clauses.append("FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL")
    if alert_prediction_fk_available:
        fk_clauses.append("FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL")
    if portfolio_prediction_fk_available:
        fk_clauses.append(
            "FOREIGN KEY(portfolio_orders_id, source_alert_id, prediction_id) "
            "REFERENCES portfolio_orders(id, source_alert_id, prediction_id) ON DELETE SET NULL"
        )
    fk_clause = ""
    if fk_clauses:
        fk_clause = ",\n          " + ",\n          ".join(fk_clauses)

    con.execute(
        f"""
        CREATE TABLE execution_fills (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          client_order_id TEXT NOT NULL,
          fill_id TEXT,
          broker TEXT,
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_version TEXT,
          symbol TEXT,
          portfolio_orders_id INTEGER,
          source_alert_id INTEGER,
          prediction_id INTEGER,
          ts_ms INTEGER,
          submit_ts_ms INTEGER,
          fill_ts_ms INTEGER NOT NULL,
          fill_qty REAL NOT NULL,
          fill_px REAL NOT NULL,
          expected_px REAL,
          mid_px REAL,
          bid_px REAL,
          ask_px REAL,
          spread_bps REAL,
          slippage_bps REAL,
          fill_latency_ms INTEGER,
          fees REAL,
          commission REAL,
          liquidity TEXT,
          raw_json TEXT,
          extra_json TEXT{fk_clause}
        )
        """
    )
    con.execute(
        f"""
        INSERT INTO execution_fills(
          id,
          client_order_id,
          fill_id,
          broker,
          model_id,
          model_version,
          symbol,
          portfolio_orders_id,
          source_alert_id,
          prediction_id,
          ts_ms,
          submit_ts_ms,
          fill_ts_ms,
          fill_qty,
          fill_px,
          expected_px,
          mid_px,
          bid_px,
          ask_px,
          spread_bps,
          slippage_bps,
          fill_latency_ms,
          fees,
          commission,
          liquidity,
          raw_json,
          extra_json
        )
        SELECT
          id,
          client_order_id,
          {_legacy_expr("fill_id", "NULL")},
          {_legacy_expr("broker", "NULL")},
          COALESCE(NULLIF(TRIM({_legacy_expr("model_id", "'baseline'")}), ''), 'baseline'),
          {_legacy_expr("model_version", "NULL")},
          {_legacy_expr("symbol", "NULL")},
          {portfolio_order_expr},
          {source_alert_expr},
          {prediction_expr},
          COALESCE({_legacy_expr("ts_ms", "NULL")}, {_legacy_expr("fill_ts_ms", "0")}),
          {_legacy_expr("submit_ts_ms", "NULL")},
          {_legacy_expr("fill_ts_ms", "0")},
          {_legacy_expr("fill_qty", "0.0")},
          {_legacy_expr("fill_px", "0.0")},
          {_legacy_expr("expected_px", "NULL")},
          {_legacy_expr("mid_px", "NULL")},
          {_legacy_expr("bid_px", "NULL")},
          {_legacy_expr("ask_px", "NULL")},
          {_legacy_expr("spread_bps", "NULL")},
          {_legacy_expr("slippage_bps", "NULL")},
          {_legacy_expr("fill_latency_ms", "NULL")},
          {fees_expr},
          {commission_expr},
          {_legacy_expr("liquidity", "NULL")},
          {_legacy_expr("raw_json", "NULL")},
          {_legacy_expr("extra_json", "NULL")}
        FROM execution_fills_old
        """
    )
    con.execute("DROP TABLE execution_fills_old")
    con.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_fills_ts
          ON execution_fills(fill_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_client
          ON execution_fills(client_order_id);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_model_ts
          ON execution_fills(model_id, fill_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_model_symbol_ts
          ON execution_fills(model_id, symbol, fill_ts_ms, id);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_portfolio_order_ts
          ON execution_fills(portfolio_orders_id, fill_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_source_alert_ts
          ON execution_fills(source_alert_id, fill_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_prediction_ts
          ON execution_fills(prediction_id, fill_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_source_alert_prediction_ts
          ON execution_fills(source_alert_id, prediction_id, fill_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_symbol_ts
          ON execution_fills(symbol, fill_ts_ms);

        CREATE INDEX IF NOT EXISTS idx_execution_fills_fill_id
          ON execution_fills(fill_id);

        CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_fills_client_fillid
        ON execution_fills(client_order_id, fill_id, ts_ms)
        WHERE fill_id IS NOT NULL;
        """
    )


def _rebuild_execution_metrics_if_needed(con) -> None:
    if not _table_exists(con, "execution_metrics"):
        return

    sql_row = con.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name='execution_metrics'
        """
    ).fetchone()
    sql = str(sql_row[0] or "") if sql_row else ""

    cols = _table_columns(con, "execution_metrics")
    needs_rebuild = False

    if "client_order_id TEXT NOT NULL UNIQUE" in sql:
        needs_rebuild = True

    required_cols = {
        "expected_px",
        "mid_px",
        "fill_px",
        "spread_bps",
        "fill_latency_ms",
    }
    if not required_cols.issubset(set(cols.keys())):
        needs_rebuild = True

    if not needs_rebuild:
        return

    con.executescript(
        """
        ALTER TABLE execution_metrics RENAME TO execution_metrics_old;

        CREATE TABLE execution_metrics (
          ts_ms INTEGER NOT NULL,
          client_order_id TEXT NOT NULL,
          broker TEXT,
          symbol TEXT NOT NULL,
          submit_qty REAL,
          filled_qty REAL,
          ref_px REAL,
          expected_px REAL,
          mid_px REAL,
          fill_px REAL,
          fill_vwap REAL,
          spread_bps REAL,
          slippage_bps REAL,
          fill_latency_ms INTEGER,
          fees REAL,
          m2m_pnl REAL,
          last_px REAL,
          PRIMARY KEY (ts_ms, client_order_id)
        );

        INSERT INTO execution_metrics(
          ts_ms,
          client_order_id,
          broker,
          symbol,
          submit_qty,
          filled_qty,
          ref_px,
          expected_px,
          mid_px,
          fill_px,
          fill_vwap,
          spread_bps,
          slippage_bps,
          fill_latency_ms,
          fees,
          m2m_pnl,
          last_px
        )
        SELECT
          ts_ms,
          client_order_id,
          broker,
          symbol,
          submit_qty,
          filled_qty,
          ref_px,
          expected_px,
          mid_px,
          COALESCE(fill_px, fill_vwap) AS fill_px,
          fill_vwap,
          spread_bps,
          slippage_bps,
          fill_latency_ms,
          fees,
          m2m_pnl,
          last_px
        FROM execution_metrics_old;

        DROP TABLE execution_metrics_old;

        CREATE INDEX IF NOT EXISTS idx_execution_metrics_client
          ON execution_metrics(client_order_id);

        CREATE INDEX IF NOT EXISTS idx_execution_metrics_ts
          ON execution_metrics(ts_ms);
        """
    )


def _init_execution_ledger_schema(con) -> None:
    alert_cols = {}
    portfolio_cols = {}
    if _table_exists(con, "alerts"):
        alert_cols = _table_columns(con, "alerts")
    if _table_exists(con, "portfolio_orders"):
        portfolio_cols = _table_columns(con, "portfolio_orders")
    if "prediction_id" in alert_cols:
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_id_prediction_lineage
              ON alerts(id, prediction_id)
            """
        )
    if {"source_alert_id", "prediction_id"}.issubset(set(portfolio_cols.keys())):
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_orders_id_source_prediction_lineage
              ON portfolio_orders(id, source_alert_id, prediction_id, ts_ms)
            """
        )
    if _table_exists(con, "execution_orders"):
        _ensure_column(con, "execution_orders", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")
        _ensure_column(con, "execution_orders", "prediction_id", "INTEGER")
    if _table_exists(con, "execution_fills"):
        _ensure_column(con, "execution_fills", "fill_id", "TEXT")
        _ensure_column(con, "execution_fills", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")
    if _table_exists(con, "pnl_attribution"):
        _ensure_column(con, "pnl_attribution", "prediction_id", "INTEGER")
        _ensure_column(con, "pnl_attribution", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")

    con.executescript(SCHEMA)

    _rebuild_execution_orders_if_needed(con)
    _ensure_column(con, "execution_orders", "order_uid", "TEXT")
    _ensure_column(con, "execution_orders", "idempotency_status", "TEXT")
    _ensure_column(con, "execution_orders", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")
    _ensure_column(con, "execution_orders", "model_version", "TEXT")
    _ensure_column(con, "execution_orders", "prediction_id", "INTEGER")
    _ensure_column(con, "execution_orders", "expected_px", "REAL")
    _ensure_column(con, "execution_orders", "mid_px", "REAL")
    _ensure_column(con, "execution_orders", "bid_px", "REAL")
    _ensure_column(con, "execution_orders", "ask_px", "REAL")
    _ensure_column(con, "execution_orders", "spread_bps", "REAL")
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_orders_order_uid
          ON execution_orders(order_uid)
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_orders_model_submit_ts ON execution_orders(model_id, submit_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_orders_portfolio_order_submit_ts ON execution_orders(portfolio_orders_id, submit_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_orders_prediction_submit_ts ON execution_orders(prediction_id, submit_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_orders_source_alert_prediction_submit_ts ON execution_orders(source_alert_id, prediction_id, submit_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_orders_symbol_submit_ts ON execution_orders(symbol, submit_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_orders_order_uid ON execution_orders(order_uid)"
    )

    _rebuild_execution_fills_if_needed(con)
    _ensure_column(con, "execution_fills", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")
    _ensure_column(con, "execution_fills", "model_version", "TEXT")
    _ensure_column(con, "execution_fills", "portfolio_orders_id", "INTEGER")
    _ensure_column(con, "execution_fills", "source_alert_id", "INTEGER")
    _ensure_column(con, "execution_fills", "prediction_id", "INTEGER")
    _ensure_column(con, "execution_fills", "ts_ms", "INTEGER")
    _ensure_column(con, "execution_fills", "submit_ts_ms", "INTEGER")
    _ensure_column(con, "execution_fills", "expected_px", "REAL")
    _ensure_column(con, "execution_fills", "mid_px", "REAL")
    _ensure_column(con, "execution_fills", "bid_px", "REAL")
    _ensure_column(con, "execution_fills", "ask_px", "REAL")
    _ensure_column(con, "execution_fills", "spread_bps", "REAL")
    _ensure_column(con, "execution_fills", "slippage_bps", "REAL")
    _ensure_column(con, "execution_fills", "fill_latency_ms", "INTEGER")
    _ensure_column(con, "execution_fills", "commission", "REAL")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_model_ts ON execution_fills(model_id, fill_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_model_symbol_ts ON execution_fills(model_id, symbol, fill_ts_ms, id)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_portfolio_order_ts ON execution_fills(portfolio_orders_id, fill_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_source_alert_ts ON execution_fills(source_alert_id, fill_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_prediction_ts ON execution_fills(prediction_id, fill_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_source_alert_prediction_ts ON execution_fills(source_alert_id, prediction_id, fill_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_symbol_ts ON execution_fills(symbol, fill_ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_fills_fill_id ON execution_fills(fill_id)"
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_fills_client_fillid
          ON execution_fills(client_order_id, fill_id, ts_ms)
          WHERE fill_id IS NOT NULL
        """
    )
    _backfill_execution_order_lineage(con)
    _backfill_execution_fill_lineage(con)
    _ensure_column(con, "model_position_state", "net_qty", "REAL NOT NULL DEFAULT 0")
    _ensure_column(con, "model_position_state", "avg_entry_price", "REAL NOT NULL DEFAULT 0")
    _ensure_column(con, "model_position_state", "realized_pnl", "REAL NOT NULL DEFAULT 0")
    _ensure_column(con, "model_position_state", "last_update_ts_ms", "INTEGER NOT NULL DEFAULT 0")

    _rebuild_execution_metrics_if_needed(con)
    _ensure_column(con, "execution_metrics", "expected_px", "REAL")
    _ensure_column(con, "execution_metrics", "mid_px", "REAL")
    _ensure_column(con, "execution_metrics", "fill_px", "REAL")
    _ensure_column(con, "execution_metrics", "spread_bps", "REAL")
    _ensure_column(con, "execution_metrics", "fill_latency_ms", "INTEGER")
    _ensure_column(con, "pnl_attribution", "position_size", "REAL")
    _ensure_column(con, "pnl_attribution", "prediction_id", "INTEGER")
    _ensure_column(con, "pnl_attribution", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")
    _ensure_column(con, "pnl_attribution", "model_version", "TEXT")
    _ensure_column(con, "pnl_attribution", "avg_price", "REAL")
    _ensure_column(con, "pnl_attribution", "realized_pnl", "REAL")
    _ensure_column(con, "pnl_attribution", "unrealized_pnl", "REAL")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_pnl_attribution_prediction_ts ON pnl_attribution(prediction_id, ts_ms DESC)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_pnl_attribution_ts ON pnl_attribution(ts_ms DESC)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_pnl_attribution_model_ts ON pnl_attribution(model_id, ts_ms DESC)"
    )
    _ensure_column(con, "capital_efficiency", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")
    _ensure_column(con, "capital_efficiency", "model_version", "TEXT")
    _ensure_column(con, "execution_capital_efficiency", "model_id", "TEXT NOT NULL DEFAULT 'baseline'")
    _ensure_column(con, "execution_capital_efficiency", "model_version", "TEXT")
    _mark_execution_ledger_schema_ready(con)


def _invalidate_execution_ledger_caches() -> None:
    invalidations = (
        ("execution_metrics", None),
        ("api_read", "execution_stats"),
        ("api_read", "execution_metrics"),
    )
    for namespace, prefix in invalidations:
        try:
            cache_invalidate_namespace(namespace, prefix=prefix)
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_LEDGER_INIT_CACHE_INVALIDATE_FAILED",
                e,
                once_key=f"init_cache_invalidate:{namespace}:{prefix or ''}",
                namespace=str(namespace),
                prefix=(str(prefix) if prefix is not None else None),
            )


def init_execution_ledger() -> None:
    if _execution_ledger_schema_marker_ready():
        _invalidate_execution_ledger_caches()
        return
    # Bootstrap must not route back through run_write_txn(), because init_db()
    # invokes this helper before marking the database as initialized.
    with _EXECUTION_LEDGER_INIT_LOCK:
        if _execution_ledger_schema_marker_ready():
            _invalidate_execution_ledger_caches()
            return
        con = connect_rw_direct()
        try:
            _init_execution_ledger_schema(con)
            con.commit()
        except Exception:
            try:
                con.rollback()
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_LEDGER_INIT_ROLLBACK_FAILED",
                    e,
                    once_key="init_rollback",
                )
            raise
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "EXECUTION_LEDGER_INIT_CLOSE_FAILED",
                    e,
                    once_key="init_close",
                )
    _invalidate_execution_ledger_caches()


def log_submit(
    client_order_id: str,
    broker: str,
    symbol: str,
    qty: float,
    submit_ts_ms: int,
    ref_px: Optional[float] = None,
    broker_order_id: Optional[str] = None,
    portfolio_orders_id: Optional[int] = None,
    source_alert_id: Optional[int] = None,
    extra: Optional[Dict[str, Any]] = None,
    expected_px: Optional[float] = None,
    mid_px: Optional[float] = None,
    bid_px: Optional[float] = None,
    ask_px: Optional[float] = None,
    spread_bps: Optional[float] = None,
    order_uid: Optional[str] = None,
    idempotency_status: Optional[str] = None,
    con=None,
) -> None:
    if con is None:
        init_execution_ledger()
        run_write_txn(
            lambda conw: log_submit(
                client_order_id=client_order_id,
                broker=broker,
                symbol=symbol,
                qty=qty,
                submit_ts_ms=submit_ts_ms,
                ref_px=ref_px,
                broker_order_id=broker_order_id,
                portfolio_orders_id=portfolio_orders_id,
                source_alert_id=source_alert_id,
                extra=extra,
                expected_px=expected_px,
                mid_px=mid_px,
                bid_px=bid_px,
                ask_px=ask_px,
                spread_bps=spread_bps,
                order_uid=order_uid,
                idempotency_status=idempotency_status,
                con=conw,
            ),
            table="execution_orders",
            operation="upsert_order_submit",
            context={
                "client_order_id": str(client_order_id),
                "broker": str(broker),
                "symbol": str(symbol),
            },
        )
        return
    try:
        can_emit_async_obs = not bool(
            con is not None and getattr(con, "in_transaction", False)
        )
        extra_norm = dict(extra or {})
        portfolio_orders_id_norm = _optional_int(portfolio_orders_id)
        source_alert_id_requested = _optional_int(source_alert_id if source_alert_id is not None else extra_norm.get("source_alert_id"))
        resolved_lineage = _resolve_typed_lineage(
            con,
            portfolio_orders_id=portfolio_orders_id_norm,
            source_alert_id=source_alert_id_requested,
            prediction_id=extra_norm.get("prediction_id"),
        )
        portfolio_orders_id_norm = _optional_int(resolved_lineage.get("portfolio_orders_id"))
        source_alert_id_resolved = _optional_int(resolved_lineage.get("source_alert_id"))
        if not str(extra_norm.get("model_id") or "").strip() and resolved_lineage.get("model_id") not in (None, ""):
            extra_norm["model_id"] = str(resolved_lineage.get("model_id"))

        ref_px_f = float(ref_px) if ref_px is not None else None
        expected_px_f = _pick_float(
            expected_px,
            extra_norm.get("expected_px"),
            extra_norm.get("ref_px"),
            extra_norm.get("arrival_px"),
            ref_px_f,
        )
        mid_px_f = _pick_float(
            mid_px,
            extra_norm.get("mid_px"),
            extra_norm.get("arrival_mid_px"),
            extra_norm.get("arrival_px"),
            ref_px_f,
        )
        bid_px_f = _pick_float(bid_px, extra_norm.get("bid_px"))
        ask_px_f = _pick_float(ask_px, extra_norm.get("ask_px"))
        spread_bps_f = _pick_float(
            spread_bps,
            extra_norm.get("spread_bps"),
            extra_norm.get("entry_spread_bps"),
            extra_norm.get("spread_at_entry_bps"),
        )

        if expected_px_f is None and ref_px_f is not None:
            expected_px_f = ref_px_f
        if mid_px_f is None and ref_px_f is not None:
            mid_px_f = ref_px_f
        if spread_bps_f is None and bid_px_f is not None and ask_px_f is not None and mid_px_f not in (None, 0.0):
            spread_bps_f = ((ask_px_f - bid_px_f) / mid_px_f) * 10000.0

        strategy_name = _extract_strategy_name(extra_norm)
        if strategy_name:
            extra_norm["strategy_name"] = str(strategy_name)
        model_identity = _extract_model_identity(extra_norm)
        if model_identity.get("model_name"):
            extra_norm["model_name"] = str(model_identity.get("model_name"))
        extra_norm["model_id"] = _normalize_model_id(model_identity.get("model_id"))
        if model_identity.get("model_kind"):
            extra_norm["model_kind"] = str(model_identity.get("model_kind"))
        model_ts_raw = model_identity.get("model_ts_ms")
        if model_ts_raw is not None:
            try:
                extra_norm["model_ts_ms"] = _safe_int(model_ts_raw)
            except Exception as e:
                _warn_nonfatal("EXECUTION_LEDGER_MODEL_TS_PARSE_FAILED", e, once_key="log_submit_model_ts", client_order_id=str(client_order_id))
        if model_identity.get("model_version"):
            extra_norm["model_version"] = str(model_identity.get("model_version"))
        if model_identity.get("regime"):
            extra_norm["regime"] = str(model_identity.get("regime"))
        if extra_norm.get("market_regime") is None and extra_norm.get("market_regime_label") is not None:
            extra_norm["market_regime"] = str(extra_norm.get("market_regime_label"))
        if extra_norm.get("market_regime_label") is None and extra_norm.get("market_regime") is not None:
            extra_norm["market_regime_label"] = str(extra_norm.get("market_regime"))
        horizon_raw = model_identity.get("horizon_s")
        if horizon_raw is not None:
            try:
                extra_norm["horizon_s"] = _safe_int(horizon_raw)
            except Exception as e:
                _warn_nonfatal("EXECUTION_LEDGER_HORIZON_PARSE_FAILED", e, once_key="log_submit_horizon", client_order_id=str(client_order_id))
        if any(k in model_identity for k in ("model_name", "model_kind", "model_ts_ms", "model_version")):
            extra_norm["source_model"] = {
                "model_name": extra_norm.get("model_name"),
                "model_kind": extra_norm.get("model_kind"),
                "model_ts_ms": extra_norm.get("model_ts_ms"),
                "model_version": extra_norm.get("model_version"),
                "regime": extra_norm.get("regime"),
                "market_regime": extra_norm.get("market_regime"),
                "horizon_s": extra_norm.get("horizon_s"),
            }

        if "mid_px" not in extra_norm and extra_norm.get("arrival_mid_px") is not None:
            extra_norm["mid_px"] = extra_norm.get("arrival_mid_px")

        prediction_id = _validated_prediction_id(con, resolved_lineage.get("prediction_id"))
        if prediction_id is None:
            prediction_id = _resolve_prediction_id_for_order(
                con,
                source_alert_id=source_alert_id_resolved,
                symbol=str(symbol),
                model_id=_normalize_model_id(extra_norm.get("model_id")),
                extra=extra_norm,
            )

        order_uid_norm = str(order_uid or extra_norm.get("order_uid") or "").strip() or None
        idempotency_status_norm = str(
            idempotency_status or extra_norm.get("idempotency_status") or ""
        ).strip() or None

        if portfolio_orders_id_norm is not None:
            extra_norm["portfolio_orders_id"] = int(portfolio_orders_id_norm)
        else:
            extra_norm.pop("portfolio_orders_id", None)
        if source_alert_id_resolved is not None:
            extra_norm["source_alert_id"] = int(source_alert_id_resolved)
        else:
            extra_norm.pop("source_alert_id", None)
        if prediction_id is not None:
            extra_norm["prediction_id"] = int(prediction_id)
        else:
            extra_norm.pop("prediction_id", None)

        extra_norm.setdefault("symbol", str(symbol))
        extra_norm.setdefault("submit_ts_ms", int(submit_ts_ms))
        extra_norm.setdefault("ref_px", ref_px_f)
        extra_norm.setdefault("expected_px", expected_px_f)
        extra_norm.setdefault("mid_px", mid_px_f)
        extra_norm.setdefault("bid_px", bid_px_f)
        extra_norm.setdefault("ask_px", ask_px_f)
        extra_norm.setdefault("spread_bps", spread_bps_f)
        extra_norm.setdefault("order_uid", order_uid_norm)
        extra_norm.setdefault("idempotency_status", idempotency_status_norm)
        extra_norm.setdefault("order_type", str(extra_norm.get("order_type") or "MARKET").upper().strip())
        extra_norm.setdefault("aggressiveness", str(extra_norm.get("aggressiveness") or "AGGRESSIVE").upper().strip())
        extra_norm.setdefault("passive_flag", 1 if str(extra_norm.get("aggressiveness") or "").upper().strip() == "PASSIVE" else 0)
        extra_norm.setdefault("aggressive_flag", 1 if str(extra_norm.get("aggressiveness") or "").upper().strip() == "AGGRESSIVE" else 0)

        order_row_cache = {
            "broker": str(broker),
            "symbol": str(symbol),
            "model_id": _normalize_model_id(extra_norm.get("model_id")),
            "model_version": (str(extra_norm.get("model_version")).strip() if extra_norm.get("model_version") not in (None, "") else None),
            "prediction_id": (int(prediction_id) if prediction_id is not None else None),
            "submit_ts_ms": int(submit_ts_ms),
            "ref_px": ref_px_f,
            "expected_px": expected_px_f,
            "mid_px": mid_px_f,
            "bid_px": bid_px_f,
            "ask_px": ask_px_f,
            "spread_bps": spread_bps_f,
        }

        con.execute(
            """
            INSERT INTO execution_orders(
            client_order_id,
            order_uid,
            idempotency_status,
            broker,
            portfolio_orders_id,
            source_alert_id,
            prediction_id,
            model_id,
            model_version,
            symbol,
            qty,
            submit_ts_ms,
            ref_px,
            expected_px,
            mid_px,
            bid_px,
            ask_px,
            spread_bps,
            broker_order_id,
            status,
            extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(client_order_id) DO UPDATE SET
              order_uid=COALESCE(excluded.order_uid, execution_orders.order_uid),
              idempotency_status=COALESCE(excluded.idempotency_status, execution_orders.idempotency_status),
              broker=excluded.broker,
              portfolio_orders_id=COALESCE(excluded.portfolio_orders_id, execution_orders.portfolio_orders_id),
              source_alert_id=CASE
                WHEN excluded.portfolio_orders_id IS NOT NULL THEN excluded.source_alert_id
                WHEN excluded.source_alert_id IS NOT NULL THEN excluded.source_alert_id
                ELSE execution_orders.source_alert_id
              END,
              prediction_id=CASE
                WHEN excluded.portfolio_orders_id IS NOT NULL THEN excluded.prediction_id
                WHEN excluded.source_alert_id IS NOT NULL THEN excluded.prediction_id
                ELSE COALESCE(excluded.prediction_id, execution_orders.prediction_id)
              END,
              model_id=excluded.model_id,
              model_version=COALESCE(excluded.model_version, execution_orders.model_version),
              symbol=excluded.symbol,
              qty=excluded.qty,
              submit_ts_ms=excluded.submit_ts_ms,
              ref_px=COALESCE(excluded.ref_px, execution_orders.ref_px),
              expected_px=COALESCE(excluded.expected_px, execution_orders.expected_px),
              mid_px=COALESCE(excluded.mid_px, execution_orders.mid_px),
              bid_px=COALESCE(excluded.bid_px, execution_orders.bid_px),
              ask_px=COALESCE(excluded.ask_px, execution_orders.ask_px),
              spread_bps=COALESCE(excluded.spread_bps, execution_orders.spread_bps),
              broker_order_id=COALESCE(excluded.broker_order_id, execution_orders.broker_order_id),
              status=excluded.status,
              extra_json=excluded.extra_json
            """,
            (
                str(client_order_id),
                order_uid_norm,
                idempotency_status_norm,
                str(broker),
                portfolio_orders_id_norm,
                source_alert_id_resolved,
                int(prediction_id) if prediction_id is not None else None,
                _normalize_model_id(extra_norm.get("model_id")),
                (str(extra_norm.get("model_version")).strip() if extra_norm.get("model_version") not in (None, "") else None),
                str(symbol),
                float(qty),
                int(submit_ts_ms),
                ref_px_f,
                expected_px_f,
                mid_px_f,
                bid_px_f,
                ask_px_f,
                spread_bps_f,
                str(broker_order_id) if broker_order_id is not None else None,
                "submitted",
                json.dumps(extra_norm, separators=(",", ":"), sort_keys=True),
            ),
        )
        if portfolio_orders_id_norm is not None or source_alert_id_resolved is not None or prediction_id is not None:
            con.execute(
                """
                UPDATE execution_fills
                SET portfolio_orders_id = COALESCE(?, execution_fills.portfolio_orders_id),
                    source_alert_id = CASE
                      WHEN ? IS NOT NULL THEN ?
                      WHEN ? IS NOT NULL THEN ?
                      ELSE execution_fills.source_alert_id
                    END,
                    prediction_id = CASE
                      WHEN ? IS NOT NULL THEN ?
                      WHEN ? IS NOT NULL THEN ?
                      ELSE COALESCE(execution_fills.prediction_id, ?)
                    END
                WHERE client_order_id=?
                """,
                (
                    portfolio_orders_id_norm,
                    portfolio_orders_id_norm,
                    source_alert_id_resolved,
                    source_alert_id_resolved,
                    source_alert_id_resolved,
                    portfolio_orders_id_norm,
                    int(prediction_id) if prediction_id is not None else None,
                    source_alert_id_resolved,
                    int(prediction_id) if prediction_id is not None else None,
                    int(prediction_id) if prediction_id is not None else None,
                    str(client_order_id),
                ),
            )
        cache_set("execution_orders", str(client_order_id), order_row_cache, ttl_s=3600.0)
        append_event(
            event_type="order_submit",
            event_source="engine.execution.execution_ledger",
            entity_type="order",
            entity_id=str(client_order_id),
            correlation_id=str(portfolio_orders_id_norm) if portfolio_orders_id_norm else str(client_order_id),
            payload={
                "client_order_id": str(client_order_id),
                "order_uid": order_uid_norm,
                "idempotency_status": idempotency_status_norm,
                "broker": str(broker),
                "symbol": str(symbol),
                "qty": float(qty),
                "submit_ts_ms": int(submit_ts_ms),
                "ref_px": ref_px_f,
                "expected_px": expected_px_f,
                "mid_px": mid_px_f,
                "bid_px": bid_px_f,
                "ask_px": ask_px_f,
                "spread_bps": spread_bps_f,
                "source_alert_id": source_alert_id_resolved,
                "prediction_id": (int(prediction_id) if prediction_id is not None else None),
                "portfolio_orders_id": portfolio_orders_id_norm,
            },
            ts_ms=int(submit_ts_ms),
            con=con,
        )
        _record_order_boundary_event(
            con,
            ts_ms=int(submit_ts_ms),
            event_type="order_submit",
            status="submitted",
            mode=(str(extra_norm.get("execution_mode")) if extra_norm.get("execution_mode") not in (None, "") else None),
            broker=str(broker),
            batch_id=portfolio_orders_id_norm,
            correlation_id=str(client_order_id),
            payload={
                "client_order_id": str(client_order_id),
                "order_uid": order_uid_norm,
                "idempotency_status": idempotency_status_norm,
                "broker": str(broker),
                "symbol": str(symbol),
                "qty": float(qty),
                "submit_ts_ms": int(submit_ts_ms),
                "ref_px": ref_px_f,
                "expected_px": expected_px_f,
                "mid_px": mid_px_f,
                "bid_px": bid_px_f,
                "ask_px": ask_px_f,
                "spread_bps": spread_bps_f,
                "source_alert_id": source_alert_id_resolved,
                "prediction_id": (int(prediction_id) if prediction_id is not None else None),
                "portfolio_orders_id": portfolio_orders_id_norm,
                "model_id": _normalize_model_id(extra_norm.get("model_id")),
                "model_version": (str(extra_norm.get("model_version")).strip() if extra_norm.get("model_version") not in (None, "") else None),
                "execution_mode": (str(extra_norm.get("execution_mode")) if extra_norm.get("execution_mode") not in (None, "") else None),
            },
        )
        if can_emit_async_obs:
            emit_counter(
                "order_throughput",
                1,
                component="engine.execution.execution_ledger",
                broker=str(broker),
                symbol=str(symbol),
                strategy=extra_norm.get("strategy_name"),
                extra_tags={"throughput_type": "submitted_orders"},
            )
            trace_event(
                "order_submission",
                component="engine.execution.execution_ledger",
                entity_type="order",
                entity_id=str(client_order_id),
                payload={
                    "client_order_id": str(client_order_id),
                    "broker": str(broker),
                    "symbol": str(symbol),
                    "qty": float(qty),
                    "submit_ts_ms": int(submit_ts_ms),
                    "strategy": extra_norm.get("strategy_name"),
                },
                symbol=str(symbol),
                strategy=extra_norm.get("strategy_name"),
                broker=str(broker),
                ts_ms=int(submit_ts_ms),
                con=con,
            )
    finally:
        cache_invalidate_namespace("execution_metrics")
        cache_invalidate_namespace("api_read", prefix="execution_stats")
        cache_invalidate_namespace("api_read", prefix="execution_metrics")


def repair_execution_order_model_identity(limit: int = 5000) -> Dict[str, Any]:
    init_execution_ledger()
    def _write(con):
        rows = con.execute(
            """
            SELECT client_order_id, extra_json
            FROM execution_orders
            ORDER BY submit_ts_ms DESC, client_order_id DESC
            LIMIT ?
            """,
            (max(1, int(limit or 5000)),),
        ).fetchall()

        scanned = 0
        repaired = 0
        already_ok = 0

        for client_order_id, extra_json_raw in rows or []:
            scanned += 1
            extra_norm = _safe_json_obj(extra_json_raw)
            if not isinstance(extra_norm, dict):
                extra_norm = {}

            source_model_obj = extra_norm.get("source_model")
            source_model_before = dict(source_model_obj) if isinstance(source_model_obj, dict) else {}
            needs_repair = not isinstance(source_model_before, dict)
            if not needs_repair:
                for key in ("model_name", "model_kind", "model_ts_ms", "model_version"):
                    if source_model_before.get(key) in (None, ""):
                        needs_repair = True
                        break

            strategy_name = _extract_strategy_name(extra_norm)
            if strategy_name and not str(extra_norm.get("strategy_name") or "").strip():
                extra_norm["strategy_name"] = str(strategy_name)
                needs_repair = True

            model_identity = _extract_model_identity(extra_norm)
            extra_norm["model_id"] = _normalize_model_id(model_identity.get("model_id"))
            if model_identity.get("model_name"):
                extra_norm["model_name"] = str(model_identity.get("model_name"))
            if model_identity.get("model_kind"):
                extra_norm["model_kind"] = str(model_identity.get("model_kind"))
            model_ts_raw = model_identity.get("model_ts_ms")
            if model_ts_raw is not None:
                try:
                    extra_norm["model_ts_ms"] = _safe_int(model_ts_raw)
                except Exception as e:
                    _warn_nonfatal("EXECUTION_LEDGER_MODEL_TS_PARSE_FAILED", e, once_key="repair_model_ts", client_order_id=str(client_order_id))
            if model_identity.get("model_version"):
                extra_norm["model_version"] = str(model_identity.get("model_version"))
            if model_identity.get("regime"):
                extra_norm["regime"] = str(model_identity.get("regime"))
            if extra_norm.get("market_regime") is None and extra_norm.get("market_regime_label") is not None:
                extra_norm["market_regime"] = str(extra_norm.get("market_regime_label"))
            if extra_norm.get("market_regime_label") is None and extra_norm.get("market_regime") is not None:
                extra_norm["market_regime_label"] = str(extra_norm.get("market_regime"))
            horizon_raw = model_identity.get("horizon_s")
            if horizon_raw is not None:
                try:
                    extra_norm["horizon_s"] = _safe_int(horizon_raw)
                except Exception as e:
                    _warn_nonfatal("EXECUTION_LEDGER_HORIZON_PARSE_FAILED", e, once_key="repair_horizon", client_order_id=str(client_order_id))

            if any(extra_norm.get(k) not in (None, "") for k in ("model_name", "model_kind", "model_ts_ms", "model_version")):
                extra_norm["source_model"] = {
                    "model_name": extra_norm.get("model_name"),
                    "model_kind": extra_norm.get("model_kind"),
                    "model_ts_ms": extra_norm.get("model_ts_ms"),
                    "model_version": extra_norm.get("model_version"),
                    "regime": extra_norm.get("regime"),
                    "market_regime": extra_norm.get("market_regime"),
                    "horizon_s": extra_norm.get("horizon_s"),
                }
                needs_repair = True

            if needs_repair:
                con.execute(
                    """
                    UPDATE execution_orders
                    SET extra_json=?, model_id=?, model_version=?
                    WHERE client_order_id=?
                    """,
                    (
                        json.dumps(extra_norm, separators=(",", ":"), sort_keys=True),
                        _normalize_model_id(extra_norm.get("model_id")),
                        (str(extra_norm.get("model_version")).strip() if extra_norm.get("model_version") not in (None, "") else None),
                        str(client_order_id),
                    ),
                )
                repaired += 1
            else:
                already_ok += 1

        cache_invalidate_namespace("execution_orders")
        return {
            "ok": True,
            "scanned": int(scanned),
            "repaired": int(repaired),
            "already_ok": int(already_ok),
        }
    return run_write_txn(
        _write,
        table="execution_orders",
        operation="repair_model_identity",
        context={"limit": int(limit or 5000)},
    )


def log_fill(*args, **kwargs) -> None:
    """
    Back/forward compatible wrapper.

    Old signature:
      log_fill(client_order_id, fill_ts_ms, fill_qty, fill_px, fees=None, liquidity=None, raw=None)

    New signature:
      log_fill(client_order_id, fill_id, broker, symbol, qty, fill_px, fill_ts_ms, fees=None, extra=None)
    """
    if "fill_id" in kwargs or (len(args) >= 2 and isinstance(args[1], str)):
        _log_fill_v2(*args, **kwargs)
        return
    _log_fill_v1(*args, **kwargs)


def _apply_execution_order_lineage(
    con,
    *,
    client_order_id: str,
    lineage: Dict[str, Any],
) -> None:
    portfolio_orders_id = _optional_int(lineage.get("portfolio_orders_id"))
    source_alert_id = _optional_int(lineage.get("source_alert_id"))
    prediction_id = _validated_prediction_id(con, lineage.get("prediction_id"))
    con.execute(
        """
        UPDATE execution_orders
        SET portfolio_orders_id = COALESCE(CAST(? AS INTEGER), execution_orders.portfolio_orders_id),
            source_alert_id = CASE
              WHEN CAST(? AS INTEGER) IS NOT NULL THEN CAST(? AS INTEGER)
              WHEN CAST(? AS INTEGER) IS NOT NULL THEN CAST(? AS INTEGER)
              ELSE execution_orders.source_alert_id
            END,
            prediction_id = CASE
              WHEN CAST(? AS INTEGER) IS NOT NULL THEN CAST(? AS INTEGER)
              WHEN CAST(? AS INTEGER) IS NOT NULL THEN CAST(? AS INTEGER)
              ELSE COALESCE(execution_orders.prediction_id, CAST(? AS INTEGER))
            END
        WHERE client_order_id=?
        """,
        (
            portfolio_orders_id,
            portfolio_orders_id,
            source_alert_id,
            source_alert_id,
            source_alert_id,
            portfolio_orders_id,
            prediction_id,
            source_alert_id,
            prediction_id,
            prediction_id,
            str(client_order_id),
        ),
    )


def _lineage_from_execution_order(con, client_order_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT
          portfolio_orders_id,
          source_alert_id,
          prediction_id,
          broker_order_id,
          model_id,
          model_version,
          symbol,
          extra_json
        FROM execution_orders
        WHERE client_order_id=?
        LIMIT 1
        """,
        (str(client_order_id),),
    ).fetchone()
    if not row:
        return {
            "client_order_id": str(client_order_id),
            "model_id": "baseline",
        }

    extra = _safe_json_obj(row[7])
    resolved_lineage = _resolve_typed_lineage(
        con,
        portfolio_orders_id=(row[0] if row[0] not in (None, "") else extra.get("portfolio_orders_id")),
        source_alert_id=(row[1] if row[1] not in (None, "") else extra.get("source_alert_id")),
        prediction_id=(row[2] if row[2] not in (None, "") else extra.get("prediction_id")),
    )
    portfolio_orders_id = _optional_int(resolved_lineage.get("portfolio_orders_id"))
    source_alert_id = _optional_int(resolved_lineage.get("source_alert_id"))
    prediction_id = _validated_prediction_id(con, resolved_lineage.get("prediction_id"))
    out: Dict[str, Any] = {
        "client_order_id": str(client_order_id),
        "portfolio_orders_id": portfolio_orders_id,
        "source_alert_id": source_alert_id,
        "prediction_id": prediction_id,
        "broker_order_id": (str(row[3]) if row[3] not in (None, "") else None),
        "model_id": _normalize_model_id(row[4] or resolved_lineage.get("model_id")),
        "model_version": (str(row[5]).strip() if row[5] not in (None, "") else None),
        "symbol": (
            str(row[6])
            if row[6] not in (None, "")
            else (
                str(resolved_lineage.get("symbol"))
                if resolved_lineage.get("symbol") not in (None, "")
                else None
            )
        ),
        "execution_target": str(extra.get("execution_target") or "real").strip().lower() or "real",
        "execution_mode": (str(extra.get("execution_mode")).strip() if extra.get("execution_mode") not in (None, "") else None),
        "model_name": (str(extra.get("model_name")).strip() if extra.get("model_name") not in (None, "") else None),
        "model_kind": (str(extra.get("model_kind")).strip() if extra.get("model_kind") not in (None, "") else None),
    }
    try:
        out["model_ts_ms"] = (
            _safe_int(extra.get("model_ts_ms"))
            if extra.get("model_ts_ms") is not None and str(extra.get("model_ts_ms")).strip() != ""
            else None
        )
    except Exception:
        out["model_ts_ms"] = None
    try:
        out["batch_id"] = (
            _safe_int(extra.get("batch_id"))
            if extra.get("batch_id") is not None and str(extra.get("batch_id")).strip() != ""
            else None
        )
    except Exception:
        out["batch_id"] = None
    try:
        out["portfolio_orders_id"] = _optional_int(out.get("portfolio_orders_id"))
    except Exception:
        out["portfolio_orders_id"] = None
    return out


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return _safe_int(value)
    except Exception:
        return None


def _ensure_fill_order_reference(
    con,
    *,
    client_order_id: str,
    broker: Optional[str],
    symbol: Optional[str],
    qty: float,
    event_ts_ms: int,
    extra: Optional[Dict[str, Any]] = None,
) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM execution_orders
        WHERE client_order_id=?
        LIMIT 1
        """,
        (str(client_order_id),),
    ).fetchone()
    if row:
        return False

    extra_norm = dict(extra or {})
    portfolio_orders_id = _optional_int(extra_norm.get("portfolio_orders_id"))
    requested_source_alert_id = _optional_int(extra_norm.get("source_alert_id"))
    resolved_lineage = _resolve_typed_lineage(
        con,
        portfolio_orders_id=portfolio_orders_id,
        source_alert_id=requested_source_alert_id,
        prediction_id=extra_norm.get("prediction_id"),
    )
    portfolio_orders_id = _optional_int(resolved_lineage.get("portfolio_orders_id"))
    source_alert_id = _optional_int(resolved_lineage.get("source_alert_id"))
    prediction_id = _validated_prediction_id(con, resolved_lineage.get("prediction_id"))
    sym = str(symbol or resolved_lineage.get("symbol") or extra_norm.get("symbol") or "").strip()
    broker_name = str(broker or extra_norm.get("broker") or "unknown").strip() or "unknown"
    if not sym:
        append_event(
            event_type="fill_missing_local_order_reference",
            event_source="engine.execution.execution_ledger",
            entity_type="order",
            entity_id=str(client_order_id),
            correlation_id=str(client_order_id),
            payload={
                "client_order_id": str(client_order_id),
                "broker": str(broker_name),
                "fill_ts_ms": int(event_ts_ms),
                "fill_qty": float(qty or 0.0),
                "placeholder_inserted": False,
                "reason": "symbol_unavailable",
            },
            ts_ms=int(event_ts_ms),
            con=con,
        )
        return False

    model_identity = _extract_model_identity(extra_norm)
    model_id = _normalize_model_id(model_identity.get("model_id") or extra_norm.get("model_id") or resolved_lineage.get("model_id"))
    model_version = (
        str(model_identity.get("model_version") or extra_norm.get("model_version")).strip()
        if (model_identity.get("model_version") or extra_norm.get("model_version")) not in (None, "")
        else None
    )
    ref_px = _pick_float(extra_norm.get("ref_px"), extra_norm.get("expected_px"), extra_norm.get("mid_px"))
    expected_px = _pick_float(extra_norm.get("expected_px"), ref_px)
    mid_px = _pick_float(extra_norm.get("mid_px"), expected_px, ref_px)
    bid_px = _pick_float(extra_norm.get("bid_px"))
    ask_px = _pick_float(extra_norm.get("ask_px"))
    spread_bps = _pick_float(extra_norm.get("spread_bps"))
    if spread_bps is None and bid_px is not None and ask_px is not None and mid_px not in (None, 0.0):
        spread_bps = ((float(ask_px) - float(bid_px)) / float(mid_px)) * 10000.0

    placeholder_extra = dict(extra_norm)
    placeholder_extra["missing_local_order_reference"] = True
    placeholder_extra["placeholder_reason"] = "fill_arrived_before_order"
    placeholder_extra["fill_arrived_before_submit"] = True
    placeholder_extra.setdefault("symbol", str(sym))
    placeholder_extra.setdefault("broker", str(broker_name))
    placeholder_extra.setdefault("submit_ts_ms", int(event_ts_ms))
    placeholder_extra.setdefault("model_id", str(model_id))
    if portfolio_orders_id is not None:
        placeholder_extra["portfolio_orders_id"] = int(portfolio_orders_id)
    else:
        placeholder_extra.pop("portfolio_orders_id", None)
    if source_alert_id is not None:
        placeholder_extra["source_alert_id"] = int(source_alert_id)
    else:
        placeholder_extra.pop("source_alert_id", None)
    if prediction_id is not None:
        placeholder_extra["prediction_id"] = int(prediction_id)
    else:
        placeholder_extra.pop("prediction_id", None)
    if model_version is not None:
        placeholder_extra.setdefault("model_version", str(model_version))

    order_uid = str(extra_norm.get("order_uid") or "").strip() or None
    idempotency_status = str(extra_norm.get("idempotency_status") or "fill_before_submit").strip() or "fill_before_submit"

    con.execute(
        """
        INSERT OR IGNORE INTO execution_orders(
          client_order_id,
          order_uid,
          idempotency_status,
          broker,
          portfolio_orders_id,
          source_alert_id,
          prediction_id,
          model_id,
          model_version,
          symbol,
          qty,
          submit_ts_ms,
          ref_px,
          expected_px,
          mid_px,
          bid_px,
          ask_px,
          spread_bps,
          broker_order_id,
          status,
          extra_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(client_order_id),
            order_uid,
            idempotency_status,
            str(broker_name),
            portfolio_orders_id,
            source_alert_id,
            prediction_id,
            str(model_id),
            model_version,
            str(sym),
            float(qty or 0.0),
            int(event_ts_ms),
            ref_px,
            expected_px,
            mid_px,
            bid_px,
            ask_px,
            spread_bps,
            (str(extra_norm.get("broker_order_id")).strip() if extra_norm.get("broker_order_id") not in (None, "") else None),
            "fill_pending_submit",
            json.dumps(placeholder_extra, separators=(",", ":"), sort_keys=True),
        ),
    )
    cache_set(
        "execution_orders",
        str(client_order_id),
        {
            "broker": str(broker_name),
            "symbol": str(sym),
            "model_id": str(model_id),
            "model_version": model_version,
            "prediction_id": prediction_id,
            "submit_ts_ms": int(event_ts_ms),
            "ref_px": ref_px,
            "expected_px": expected_px,
            "mid_px": mid_px,
            "bid_px": bid_px,
            "ask_px": ask_px,
            "spread_bps": spread_bps,
        },
        ttl_s=3600.0,
    )
    append_event(
        event_type="fill_missing_local_order_reference",
        event_source="engine.execution.execution_ledger",
        entity_type="order",
        entity_id=str(client_order_id),
        correlation_id=str(client_order_id),
        payload={
            "client_order_id": str(client_order_id),
            "broker": str(broker_name),
            "symbol": str(sym),
            "fill_ts_ms": int(event_ts_ms),
            "fill_qty": float(qty or 0.0),
            "placeholder_inserted": True,
            "placeholder_status": "fill_pending_submit",
        },
        ts_ms=int(event_ts_ms),
        con=con,
    )
    return True


def _normalize_fill_id(
    *,
    client_order_id: str,
    fill_id: Optional[str],
    broker: Optional[str],
    symbol: Optional[str],
    qty: float,
    fill_px: float,
    fill_ts_ms: int,
    fees: Optional[float],
    liquidity: Optional[str],
    payload: Optional[Dict[str, Any]],
) -> str:
    fill_id_norm = str(fill_id or "").strip()
    if fill_id_norm:
        return fill_id_norm

    payload_norm = payload if isinstance(payload, dict) else {}
    digest_payload = {
        "broker": (str(broker or "").strip().lower() or None),
        "client_order_id": str(client_order_id or "").strip(),
        "symbol": (str(symbol or "").strip().upper() or None),
        "qty": round(float(qty or 0.0), 12),
        "fill_px": round(float(fill_px or 0.0), 12),
        "fill_ts_ms": int(fill_ts_ms or 0),
        "fees": (round(float(fees), 12) if fees is not None else None),
        "liquidity": (str(liquidity).strip() if liquidity not in (None, "") else None),
        "payload": payload_norm,
    }
    digest = hashlib.sha256(
        json.dumps(digest_payload, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"synthetic:{digest}"


def _apply_fill_attribution_side_effects(
    *,
    con,
    client_order_id: str,
    fill_qty: float,
    fill_px: float,
    fill_ts_ms: int,
    fees: Optional[float],
    slippage_bps: Optional[float],
    broker: Optional[str],
    fill_id: Optional[str],
    reason: str,
) -> Dict[str, Any]:
    lineage = _lineage_from_execution_order(con, str(client_order_id))

    def _emit_failure_event(payload: Dict[str, Any]) -> None:
        append_event(
            event_type="fill_attribution_failed",
            event_source="engine.execution.execution_ledger",
            entity_type="order",
            entity_id=str(client_order_id),
            correlation_id=(str(fill_id) if fill_id else str(client_order_id)),
            payload=dict(payload or {}),
            ts_ms=int(fill_ts_ms),
            con=con,
        )

    try:
        result = record_live_fill_attribution(
            client_order_id=str(client_order_id),
            fill_qty=float(fill_qty),
            fill_px=float(fill_px),
            fill_ts_ms=int(fill_ts_ms),
            fees=float(fees or 0.0),
            slippage_bps=(float(slippage_bps) if slippage_bps is not None else None),
            con=con,
            commit=False,
        ) or {}
    except Exception as e:
        _emit_failure_event(
            {
                **dict(lineage or {}),
                "fill_id": (str(fill_id) if fill_id else None),
                "broker": (str(broker) if broker else None),
                "fill_ts_ms": int(fill_ts_ms),
                "fill_qty": float(fill_qty),
                "fill_px": float(fill_px),
                "fees": (float(fees) if fees is not None else None),
                "slippage_bps": (float(slippage_bps) if slippage_bps is not None else None),
                "reason": str(reason),
                "error": str(e),
            }
        )
        raise

    if not bool(result.get("ok")):
        _emit_failure_event(
            {
                **dict(lineage or {}),
                "fill_id": (str(fill_id) if fill_id else None),
                "broker": (str(broker) if broker else None),
                "fill_ts_ms": int(fill_ts_ms),
                "fill_qty": float(fill_qty),
                "fill_px": float(fill_px),
                "fees": (float(fees) if fees is not None else None),
                "slippage_bps": (float(slippage_bps) if slippage_bps is not None else None),
                "reason": str(reason),
                "status": str(result.get("status") or "fill_attribution_failed"),
            }
        )
        raise RuntimeError(str(result.get("status") or "fill_attribution_failed"))

    return lineage


def _log_fill_v1(
    client_order_id: str,
    fill_ts_ms: int,
    fill_qty: float,
    fill_px: float,
    fees: Optional[float] = None,
    liquidity: Optional[str] = None,
    raw: Optional[Dict[str, Any]] = None,
    con=None,
) -> None:
    if con is None:
        init_execution_ledger()
        run_write_txn(
            lambda conw: _log_fill_v1(
                client_order_id=client_order_id,
                fill_ts_ms=fill_ts_ms,
                fill_qty=fill_qty,
                fill_px=fill_px,
                fees=fees,
                liquidity=liquidity,
                raw=raw,
                con=conw,
            ),
            table="execution_fills",
            operation="insert_fill_v1",
            context={"client_order_id": str(client_order_id)},
        )
        return
    try:
        can_emit_async_obs = not bool(
            con is not None and getattr(con, "in_transaction", False)
        )
        raw_payload = dict(raw or {})
        _ensure_fill_order_reference(
            con,
            client_order_id=str(client_order_id),
            broker=(str(raw_payload.get("broker")) if raw_payload.get("broker") not in (None, "") else None),
            symbol=(str(raw_payload.get("symbol")) if raw_payload.get("symbol") not in (None, "") else None),
            qty=float(fill_qty),
            event_ts_ms=int(fill_ts_ms),
            extra=raw_payload,
        )
        lineage = _lineage_from_execution_order(con, str(client_order_id))
        _apply_execution_order_lineage(
            con,
            client_order_id=str(client_order_id),
            lineage=lineage,
        )

        order_state = cache_get_or_load(
            "execution_orders",
            str(client_order_id),
            lambda: (
                lambda row: {
                    "broker": (str(row[0]) if row and row[0] is not None else None),
                    "model_id": (_normalize_model_id(row[1]) if row and row[1] is not None else "baseline"),
                    "model_version": (str(row[2]).strip() if row and row[2] not in (None, "") else None),
                    "symbol": (str(row[3]) if row and row[3] is not None else None),
                    "submit_ts_ms": (int(row[4]) if row and row[4] is not None else None),
                    "ref_px": (float(row[5]) if row and row[5] is not None else None),
                    "expected_px": (float(row[6]) if row and row[6] is not None else None),
                    "mid_px": (float(row[7]) if row and row[7] is not None else None),
                    "bid_px": (float(row[8]) if row and row[8] is not None else None),
                    "ask_px": (float(row[9]) if row and row[9] is not None else None),
                    "spread_bps": (float(row[10]) if row and row[10] is not None else None),
                }
            )(
                con.execute(
                    """
                    SELECT broker, model_id, model_version, symbol, submit_ts_ms, ref_px, expected_px, mid_px, bid_px, ask_px, spread_bps
                    FROM execution_orders
                    WHERE client_order_id=?
                    LIMIT 1
                    """,
                    (str(client_order_id),),
                ).fetchone()
            ),
            ttl_s=3600.0,
        )

        broker = order_state.get("broker")
        model_id = _normalize_model_id(order_state.get("model_id"))
        model_version = order_state.get("model_version")
        symbol = order_state.get("symbol")
        submit_ts_ms_val = order_state.get("submit_ts_ms")
        expected_px_val = order_state.get("expected_px")
        mid_px_val = order_state.get("mid_px")
        bid_px_val = order_state.get("bid_px")
        ask_px_val = order_state.get("ask_px")
        spread_bps_val = order_state.get("spread_bps")

        if expected_px_val is None:
            expected_px_val = order_state.get("ref_px")
        if mid_px_val is None:
            mid_px_val = order_state.get("ref_px")

        if symbol is None and raw_payload.get("symbol") is not None:
            symbol = str(raw_payload.get("symbol"))
        if expected_px_val is None and raw_payload.get("expected_px") is not None:
            expected_px_val = _safe_float(raw_payload.get("expected_px"))
        if mid_px_val is None and raw_payload.get("mid_px") is not None:
            mid_px_val = _safe_float(raw_payload.get("mid_px"))
        if bid_px_val is None and raw_payload.get("bid_px") is not None:
            bid_px_val = _safe_float(raw_payload.get("bid_px"))
        if ask_px_val is None and raw_payload.get("ask_px") is not None:
            ask_px_val = _safe_float(raw_payload.get("ask_px"))
        if spread_bps_val is None and raw_payload.get("spread_bps") is not None:
            spread_bps_val = _safe_float(raw_payload.get("spread_bps"))

        if mid_px_val is None and raw_payload.get("explain_json"):
            try:
                explain = json.loads(raw_payload.get("explain_json") or "{}")
                if isinstance(explain, dict):
                    if explain.get("mid_px") is not None:
                        mid_px_val = _safe_float(explain.get("mid_px"))
                    if spread_bps_val is None and explain.get("spread_bps") is not None:
                        spread_bps_val = _safe_float(explain.get("spread_bps"))
                    if raw_payload.get("latency_ms") is None and explain.get("latency_ms") is not None:
                        raw_payload["latency_ms"] = _safe_int(explain.get("latency_ms"))
            except Exception as e:
                _warn_nonfatal("EXECUTION_LEDGER_FILL_EXPLAIN_PARSE_FAILED", e, once_key="fill_explain_parse", client_order_id=str(client_order_id))

        if expected_px_val is None:
            expected_px_val = mid_px_val
        if mid_px_val is None:
            mid_px_val = expected_px_val

        fill_latency_ms = None
        if submit_ts_ms_val is not None:
            fill_latency_ms = max(0, int(fill_ts_ms) - int(submit_ts_ms_val))
        elif raw_payload.get("latency_ms") is not None:
            fill_latency_ms = max(0, _safe_int(raw_payload.get("latency_ms")))
            submit_ts_ms_val = int(fill_ts_ms) - int(fill_latency_ms)

        slippage_bps = None
        side_sign = 1.0 if float(fill_qty) > 0 else -1.0
        if expected_px_val is not None and float(expected_px_val) > 0:
            slippage_bps = ((float(fill_px) - float(expected_px_val)) / float(expected_px_val)) * 10000.0 * side_sign

        synthetic_fill_id = _normalize_fill_id(
            client_order_id=str(client_order_id),
            fill_id=None,
            broker=broker,
            symbol=symbol,
            qty=float(fill_qty),
            fill_px=float(fill_px),
            fill_ts_ms=int(fill_ts_ms),
            fees=fees,
            liquidity=liquidity,
            payload=raw_payload,
        )

        cur = con.execute(
            """
            INSERT OR IGNORE INTO execution_fills(
              client_order_id,
              fill_id,
              broker,
              model_id,
              model_version,
              symbol,
              portfolio_orders_id,
              source_alert_id,
              prediction_id,
              ts_ms,
              submit_ts_ms,
              fill_ts_ms,
              fill_qty,
              fill_px,
              expected_px,
              mid_px,
              bid_px,
              ask_px,
              spread_bps,
              slippage_bps,
              fill_latency_ms,
              fees,
              commission,
              liquidity,
              raw_json,
              extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(client_order_id),
                str(synthetic_fill_id),
                broker,
                model_id,
                (str(model_version).strip() if model_version not in (None, "") else None),
                symbol,
                _optional_int(lineage.get("portfolio_orders_id")),
                _optional_int(lineage.get("source_alert_id")),
                _validated_prediction_id(con, lineage.get("prediction_id")),
                int(fill_ts_ms),
                int(submit_ts_ms_val) if submit_ts_ms_val is not None else None,
                int(fill_ts_ms),
                float(fill_qty),
                float(fill_px),
                float(expected_px_val) if expected_px_val is not None else None,
                float(mid_px_val) if mid_px_val is not None else None,
                float(bid_px_val) if bid_px_val is not None else None,
                float(ask_px_val) if ask_px_val is not None else None,
                float(spread_bps_val) if spread_bps_val is not None else None,
                float(slippage_bps) if slippage_bps is not None else None,
                int(fill_latency_ms) if fill_latency_ms is not None else None,
                float(fees) if fees is not None else None,
                float(fees) if fees is not None else None,
                str(liquidity) if liquidity is not None else None,
                json.dumps(raw_payload, separators=(",", ":"), sort_keys=True),
                json.dumps(
                    {
                        "expected_px": expected_px_val,
                        "mid_px": mid_px_val,
                        "bid_px": bid_px_val,
                        "ask_px": ask_px_val,
                        "spread_bps": spread_bps_val,
                        "fill_latency_ms": fill_latency_ms,
                        "slippage_bps": slippage_bps,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )
        inserted = bool(getattr(cur, "rowcount", 0))
        if not inserted:
            append_event(
                event_type="fill_duplicate_ignored",
                event_source="engine.execution.execution_ledger",
                entity_type="order",
                entity_id=str(client_order_id),
                correlation_id=str(synthetic_fill_id),
                payload={
                    "client_order_id": str(client_order_id),
                    "fill_id": str(synthetic_fill_id),
                    "broker": broker,
                    "symbol": symbol,
                    "fill_ts_ms": int(fill_ts_ms),
                    "fill_qty": float(fill_qty),
                    "fill_px": float(fill_px),
                },
                ts_ms=int(fill_ts_ms),
                con=con,
            )
            return
        _upsert_model_position_state(
            con,
            model_id=model_id,
            symbol=str(symbol),
            qty_signed=float(fill_qty),
            fill_px=float(fill_px),
            fill_ts_ms=int(fill_ts_ms),
        )
        lineage = _apply_fill_attribution_side_effects(
            con=con,
            client_order_id=str(client_order_id),
            fill_qty=float(fill_qty),
            fill_px=float(fill_px),
            fill_ts_ms=int(fill_ts_ms),
            fees=fees,
            slippage_bps=slippage_bps,
            broker=broker,
            fill_id=None,
            reason="fill_inserted_v1",
        )
        append_event(
            event_type="fill",
            event_source="engine.execution.execution_ledger",
            entity_type="order",
            entity_id=str(client_order_id),
            correlation_id=str(client_order_id),
            payload={
                "client_order_id": str(client_order_id),
                "fill_id": str(synthetic_fill_id),
                "broker_order_id": lineage.get("broker_order_id"),
                "source_alert_id": lineage.get("source_alert_id"),
                "prediction_id": lineage.get("prediction_id"),
                "model_id": lineage.get("model_id"),
                "model_version": lineage.get("model_version"),
                "model_name": lineage.get("model_name"),
                "model_kind": lineage.get("model_kind"),
                "model_ts_ms": lineage.get("model_ts_ms"),
                "symbol": symbol,
                "execution_target": lineage.get("execution_target"),
                "execution_mode": lineage.get("execution_mode"),
                "batch_id": lineage.get("batch_id"),
                "portfolio_orders_id": lineage.get("portfolio_orders_id"),
                "fill_ts_ms": int(fill_ts_ms),
                "fill_qty": float(fill_qty),
                "fill_px": float(fill_px),
                "expected_px": expected_px_val,
                "mid_px": mid_px_val,
                "spread_bps": spread_bps_val,
                "slippage_bps": slippage_bps,
                "latency_ms": fill_latency_ms,
                "fees": fees,
                "liquidity": liquidity,
            },
            ts_ms=int(fill_ts_ms),
            con=con,
        )
        _record_order_boundary_event(
            con,
            ts_ms=int(fill_ts_ms),
            event_type="fill",
            status="filled",
            mode=(str(lineage.get("execution_mode")) if lineage.get("execution_mode") not in (None, "") else None),
            broker=broker,
            batch_id=_optional_int(lineage.get("portfolio_orders_id")),
            correlation_id=str(client_order_id),
            payload={
                "client_order_id": str(client_order_id),
                "fill_id": str(synthetic_fill_id),
                "broker_order_id": lineage.get("broker_order_id"),
                "source_alert_id": lineage.get("source_alert_id"),
                "prediction_id": lineage.get("prediction_id"),
                "model_id": lineage.get("model_id"),
                "model_version": lineage.get("model_version"),
                "model_name": lineage.get("model_name"),
                "model_kind": lineage.get("model_kind"),
                "model_ts_ms": lineage.get("model_ts_ms"),
                "symbol": symbol,
                "execution_target": lineage.get("execution_target"),
                "execution_mode": lineage.get("execution_mode"),
                "batch_id": lineage.get("batch_id"),
                "portfolio_orders_id": lineage.get("portfolio_orders_id"),
                "fill_ts_ms": int(fill_ts_ms),
                "fill_qty": float(fill_qty),
                "fill_px": float(fill_px),
                "expected_px": expected_px_val,
                "mid_px": mid_px_val,
                "bid_px": bid_px_val,
                "ask_px": ask_px_val,
                "spread_bps": spread_bps_val,
                "slippage_bps": slippage_bps,
                "fill_latency_ms": fill_latency_ms,
                "fees": fees,
                "liquidity": liquidity,
            },
        )
        if can_emit_async_obs:
            emit_counter(
                "broker_fill",
                1,
                component="engine.execution.execution_ledger",
                broker=(str(broker) if broker else None),
                symbol=(str(symbol) if symbol else None),
            )
            if fill_latency_ms is not None:
                emit_timing(
                    "execution_latency_ms",
                    int(fill_latency_ms),
                    component="engine.execution.execution_ledger",
                    broker=(str(broker) if broker else None),
                    symbol=(str(symbol) if symbol else None),
                )
            if slippage_bps is not None:
                emit_gauge(
                    "strategy_latency",
                    float(slippage_bps),
                    component="engine.execution.execution_ledger",
                    broker=(str(broker) if broker else None),
                    symbol=(str(symbol) if symbol else None),
                    extra_tags={"metric_scope": "slippage_bps"},
                )
            trace_event(
                "broker_fill",
                component="engine.execution.execution_ledger",
                entity_type="order",
                entity_id=str(client_order_id),
                payload={
                    "client_order_id": str(client_order_id),
                    "symbol": (str(symbol) if symbol else None),
                    "fill_latency_ms": fill_latency_ms,
                    "slippage_bps": slippage_bps,
                    "fill_qty": float(fill_qty),
                    "fill_px": float(fill_px),
                },
                symbol=(str(symbol) if symbol else None),
                broker=(str(broker) if broker else None),
                ts_ms=int(fill_ts_ms),
                con=con,
            )
    finally:
        cache_invalidate_namespace("execution_metrics")
        cache_invalidate_namespace("api_read", prefix="execution_stats")
        cache_invalidate_namespace("api_read", prefix="execution_metrics")


def _log_fill_v2(
    client_order_id: str,
    fill_id: str,
    broker: str,
    symbol: str,
    qty: float,
    fill_px: float,
    fill_ts_ms: int,
    fees: Optional[float] = None,
    extra: Optional[Dict[str, Any]] = None,
    con=None,
) -> None:
    if con is None:
        init_execution_ledger()
        run_write_txn(
            lambda conw: _log_fill_v2(
                client_order_id=client_order_id,
                fill_id=fill_id,
                broker=broker,
                symbol=symbol,
                qty=qty,
                fill_px=fill_px,
                fill_ts_ms=fill_ts_ms,
                fees=fees,
                extra=extra,
                con=conw,
            ),
            table="execution_fills",
            operation="insert_fill_v2",
            context={"client_order_id": str(client_order_id), "fill_id": str(fill_id or "")},
        )
        return
    try:
        can_emit_async_obs = not bool(
            con is not None and getattr(con, "in_transaction", False)
        )
        extra_payload = dict(extra or {})
        _ensure_fill_order_reference(
            con,
            client_order_id=str(client_order_id),
            broker=(str(broker) if broker not in (None, "") else None),
            symbol=(str(symbol) if symbol not in (None, "") else None),
            qty=float(qty),
            event_ts_ms=int(fill_ts_ms),
            extra=extra_payload,
        )
        lineage = _lineage_from_execution_order(con, str(client_order_id))
        _apply_execution_order_lineage(
            con,
            client_order_id=str(client_order_id),
            lineage=lineage,
        )

        order_state = cache_get_or_load(
            "execution_orders",
            str(client_order_id),
            lambda: (
                lambda row: {
                    "submit_ts_ms": (int(row[0]) if row and row[0] is not None else None),
                    "model_id": (_normalize_model_id(row[1]) if row and row[1] is not None else "baseline"),
                    "model_version": (str(row[2]).strip() if row and row[2] not in (None, "") else None),
                    "ref_px": (float(row[3]) if row and row[3] is not None else None),
                    "expected_px": (float(row[4]) if row and row[4] is not None else None),
                    "mid_px": (float(row[5]) if row and row[5] is not None else None),
                    "bid_px": (float(row[6]) if row and row[6] is not None else None),
                    "ask_px": (float(row[7]) if row and row[7] is not None else None),
                    "spread_bps": (float(row[8]) if row and row[8] is not None else None),
                }
            )(
                con.execute(
                    """
                    SELECT submit_ts_ms, model_id, model_version, ref_px, expected_px, mid_px, bid_px, ask_px, spread_bps
                    FROM execution_orders
                    WHERE client_order_id=?
                    LIMIT 1
                    """,
                    (str(client_order_id),),
                ).fetchone()
            ),
            ttl_s=3600.0,
        )

        submit_ts_ms_val = order_state.get("submit_ts_ms")
        model_id = _normalize_model_id(order_state.get("model_id"))
        model_version = order_state.get("model_version")
        ref_px_val = order_state.get("ref_px")
        expected_px_val = order_state.get("expected_px")
        mid_px_val = order_state.get("mid_px")
        bid_px_val = order_state.get("bid_px")
        ask_px_val = order_state.get("ask_px")
        spread_bps_val = order_state.get("spread_bps")

        if expected_px_val is None:
            if extra_payload.get("expected_px") is not None:
                expected_px_val = _safe_float(extra_payload.get("expected_px"))
            elif ref_px_val is not None:
                expected_px_val = ref_px_val

        if mid_px_val is None:
            if extra_payload.get("mid_px") is not None:
                mid_px_val = _safe_float(extra_payload.get("mid_px"))
            elif ref_px_val is not None:
                mid_px_val = ref_px_val

        if bid_px_val is None and extra_payload.get("bid_px") is not None:
            bid_px_val = _safe_float(extra_payload.get("bid_px"))

        if ask_px_val is None and extra_payload.get("ask_px") is not None:
            ask_px_val = _safe_float(extra_payload.get("ask_px"))

        if spread_bps_val is None:
            if extra_payload.get("spread_bps") is not None:
                spread_bps_val = _safe_float(extra_payload.get("spread_bps"))
            elif bid_px_val is not None and ask_px_val is not None and mid_px_val not in (None, 0.0):
                spread_bps_val = ((ask_px_val - bid_px_val) / mid_px_val) * 10000.0

        fill_latency_ms = None
        if extra_payload.get("fill_latency_ms") is not None:
            fill_latency_ms = max(0, _safe_int(extra_payload.get("fill_latency_ms")))
        elif submit_ts_ms_val is not None:
            fill_latency_ms = max(0, int(fill_ts_ms) - int(submit_ts_ms_val))

        slippage_bps = None
        side_sign = 1.0 if float(qty) > 0 else -1.0
        if expected_px_val is not None and float(expected_px_val) > 0:
            slippage_bps = ((float(fill_px) - float(expected_px_val)) / float(expected_px_val)) * 10000.0 * side_sign

        liquidity = extra_payload.get("liquidity")
        if liquidity is not None:
            liquidity = str(liquidity)

        fill_id_norm = _normalize_fill_id(
            client_order_id=str(client_order_id),
            fill_id=fill_id,
            broker=broker,
            symbol=symbol,
            qty=float(qty),
            fill_px=float(fill_px),
            fill_ts_ms=int(fill_ts_ms),
            fees=fees,
            liquidity=liquidity,
            payload=extra_payload,
        )
        raw_json = json.dumps(extra_payload, separators=(",", ":"), sort_keys=True)
        extra_json = json.dumps(
            {
                **extra_payload,
                "fill_id": str(fill_id_norm),
                "expected_px": expected_px_val,
                "mid_px": mid_px_val,
                "bid_px": bid_px_val,
                "ask_px": ask_px_val,
                "spread_bps": spread_bps_val,
                "fill_latency_ms": fill_latency_ms,
                "slippage_bps": slippage_bps,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        cur = con.execute(
            """
            INSERT OR IGNORE INTO execution_fills(
              client_order_id,
              fill_id,
              broker,
              model_id,
              model_version,
              symbol,
              portfolio_orders_id,
              source_alert_id,
              prediction_id,
              ts_ms,
              submit_ts_ms,
              fill_ts_ms,
              fill_qty,
              fill_px,
              expected_px,
              mid_px,
              bid_px,
              ask_px,
              spread_bps,
              slippage_bps,
              fill_latency_ms,
              fees,
              commission,
              liquidity,
              raw_json,
              extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(client_order_id),
                str(fill_id_norm),
                str(broker),
                model_id,
                (str(model_version).strip() if model_version not in (None, "") else None),
                str(symbol),
                _optional_int(lineage.get("portfolio_orders_id")),
                _optional_int(lineage.get("source_alert_id")),
                _validated_prediction_id(con, lineage.get("prediction_id")),
                int(fill_ts_ms),
                int(submit_ts_ms_val) if submit_ts_ms_val is not None else None,
                int(fill_ts_ms),
                float(qty),
                float(fill_px),
                float(expected_px_val) if expected_px_val is not None else None,
                float(mid_px_val) if mid_px_val is not None else None,
                float(bid_px_val) if bid_px_val is not None else None,
                float(ask_px_val) if ask_px_val is not None else None,
                float(spread_bps_val) if spread_bps_val is not None else None,
                float(slippage_bps) if slippage_bps is not None else None,
                int(fill_latency_ms) if fill_latency_ms is not None else None,
                float(fees) if fees is not None else None,
                float(fees) if fees is not None else None,
                liquidity,
                raw_json,
                extra_json,
            ),
        )
        inserted = bool(getattr(cur, "rowcount", 0))
        cache_invalidate_namespace("execution_metrics")
        cache_invalidate_namespace("api_read", prefix="execution_stats")
        cache_invalidate_namespace("api_read", prefix="execution_metrics")

        if not inserted:
            append_event(
                event_type="fill_duplicate_ignored",
                event_source="engine.execution.execution_ledger",
                entity_type="order",
                entity_id=str(client_order_id),
                correlation_id=str(fill_id_norm),
                payload={
                    "client_order_id": str(client_order_id),
                    "fill_id": str(fill_id_norm),
                    "broker": str(broker),
                    "symbol": str(symbol),
                    "fill_ts_ms": int(fill_ts_ms),
                    "fill_qty": float(qty),
                    "fill_px": float(fill_px),
                },
                ts_ms=int(fill_ts_ms),
                con=con,
            )
            return

        if inserted:
            _upsert_model_position_state(
                con,
                model_id=model_id,
                symbol=str(symbol),
                qty_signed=float(qty),
                fill_px=float(fill_px),
                fill_ts_ms=int(fill_ts_ms),
            )
            lineage = _apply_fill_attribution_side_effects(
                con=con,
                client_order_id=str(client_order_id),
                fill_qty=float(qty),
                fill_px=float(fill_px),
                fill_ts_ms=int(fill_ts_ms),
                fees=fees,
                slippage_bps=slippage_bps,
                broker=broker,
                fill_id=fill_id,
                reason="fill_inserted_v2",
            )

        append_event(
            event_type="fill",
            event_source="engine.execution.execution_ledger",
            entity_type="order",
            entity_id=str(client_order_id),
            correlation_id=str(fill_id) if fill_id else str(client_order_id),
            payload={
                "client_order_id": str(client_order_id),
                "fill_id": str(fill_id_norm),
                "broker": broker,
                "broker_order_id": (
                    str(extra_payload.get("broker_order_id"))
                    if extra_payload.get("broker_order_id") not in (None, "")
                    else lineage.get("broker_order_id")
                ),
                "source_alert_id": lineage.get("source_alert_id"),
                "prediction_id": lineage.get("prediction_id"),
                "model_id": lineage.get("model_id"),
                "model_version": lineage.get("model_version"),
                "model_name": lineage.get("model_name"),
                "model_kind": lineage.get("model_kind"),
                "model_ts_ms": lineage.get("model_ts_ms"),
                "symbol": symbol,
                "execution_target": lineage.get("execution_target"),
                "execution_mode": lineage.get("execution_mode"),
                "batch_id": lineage.get("batch_id"),
                "portfolio_orders_id": lineage.get("portfolio_orders_id"),
                "qty": float(qty),
                "fill_px": float(fill_px),
                "fill_ts_ms": int(fill_ts_ms),
                "expected_px": expected_px_val,
                "mid_px": mid_px_val,
                "spread_bps": spread_bps_val,
                "slippage_bps": slippage_bps,
                "latency_ms": fill_latency_ms,
                "fees": fees,
                "liquidity": liquidity,
            },
            ts_ms=int(fill_ts_ms),
            con=con,
        )
        _record_order_boundary_event(
            con,
            ts_ms=int(fill_ts_ms),
            event_type="fill",
            status="filled",
            mode=(str(lineage.get("execution_mode")) if lineage.get("execution_mode") not in (None, "") else None),
            broker=(str(broker) if broker not in (None, "") else None),
            batch_id=_optional_int(lineage.get("portfolio_orders_id")),
            correlation_id=str(client_order_id),
            payload={
                "client_order_id": str(client_order_id),
                "fill_id": str(fill_id_norm),
                "broker": broker,
                "broker_order_id": (
                    str(extra_payload.get("broker_order_id"))
                    if extra_payload.get("broker_order_id") not in (None, "")
                    else lineage.get("broker_order_id")
                ),
                "source_alert_id": lineage.get("source_alert_id"),
                "prediction_id": lineage.get("prediction_id"),
                "model_id": lineage.get("model_id"),
                "model_version": lineage.get("model_version"),
                "model_name": lineage.get("model_name"),
                "model_kind": lineage.get("model_kind"),
                "model_ts_ms": lineage.get("model_ts_ms"),
                "symbol": symbol,
                "execution_target": lineage.get("execution_target"),
                "execution_mode": lineage.get("execution_mode"),
                "batch_id": lineage.get("batch_id"),
                "portfolio_orders_id": lineage.get("portfolio_orders_id"),
                "qty": float(qty),
                "fill_qty": float(qty),
                "fill_px": float(fill_px),
                "fill_ts_ms": int(fill_ts_ms),
                "expected_px": expected_px_val,
                "mid_px": mid_px_val,
                "bid_px": bid_px_val,
                "ask_px": ask_px_val,
                "spread_bps": spread_bps_val,
                "slippage_bps": slippage_bps,
                "fill_latency_ms": fill_latency_ms,
                "fees": fees,
                "liquidity": liquidity,
            },
        )

        if can_emit_async_obs:
            emit_counter(
                "broker_fill",
                1,
                component="engine.execution.execution_ledger",
                broker=(str(broker) if broker else None),
                symbol=str(symbol),
            )
            if fill_latency_ms is not None:
                emit_timing(
                    "execution_latency_ms",
                    int(fill_latency_ms),
                    component="engine.execution.execution_ledger",
                    broker=(str(broker) if broker else None),
                    symbol=str(symbol),
                )
            if slippage_bps is not None:
                emit_gauge(
                    "strategy_latency",
                    float(slippage_bps),
                    component="engine.execution.execution_ledger",
                    broker=(str(broker) if broker else None),
                    symbol=str(symbol),
                    extra_tags={"metric_scope": "slippage_bps"},
                )
            trace_event(
                "broker_fill",
                component="engine.execution.execution_ledger",
                entity_type="order",
                entity_id=str(client_order_id),
                payload={
                    "client_order_id": str(client_order_id),
                    "fill_id": str(fill_id_norm),
                    "symbol": str(symbol),
                    "fill_latency_ms": fill_latency_ms,
                    "slippage_bps": slippage_bps,
                    "fill_qty": float(qty),
                    "fill_px": float(fill_px),
                },
                symbol=str(symbol),
                broker=(str(broker) if broker else None),
                ts_ms=int(fill_ts_ms),
                con=con,
            )
    finally:
        cache_invalidate_namespace("execution_metrics")
        cache_invalidate_namespace("api_read", prefix="execution_stats")
        cache_invalidate_namespace("api_read", prefix="execution_metrics")


def _vwap_for_client(
    con,
    client_order_id: str,
) -> Tuple[Optional[float], float, float, Optional[float], Optional[float], Optional[float], Optional[int]]:
    """
    Returns:
      (vwap, filled_qty_signed, fees_sum, expected_px_avg, mid_px_avg, spread_bps_avg, fill_latency_ms_max)

    Uses signed sum of fill_qty to preserve direction.
    """
    rows = con.execute(
        """
        SELECT fill_qty, fill_px, COALESCE(fees,0), expected_px, mid_px, spread_bps, fill_latency_ms
        FROM execution_fills
        WHERE client_order_id=?
        """,
        (str(client_order_id),),
    ).fetchall()

    qty_sum = 0.0
    notional = 0.0
    fees = 0.0
    expected_notional = 0.0
    expected_qty = 0.0
    mid_notional = 0.0
    mid_qty = 0.0
    spread_sum = 0.0
    spread_n = 0
    latency_max = None

    for q, px, f, expected_px, mid_px, spread_bps, fill_latency_ms in rows or []:
        qf = float(q or 0.0)
        pxf = float(px or 0.0)
        abs_qf = abs(qf)

        qty_sum += qf
        notional += qf * pxf
        fees += float(f or 0.0)

        if expected_px is not None and abs_qf > 0.0:
            expected_notional += abs_qf * float(expected_px)
            expected_qty += abs_qf

        if mid_px is not None and abs_qf > 0.0:
            mid_notional += abs_qf * float(mid_px)
            mid_qty += abs_qf

        if spread_bps is not None:
            spread_sum += float(spread_bps)
            spread_n += 1

        if fill_latency_ms is not None:
            latency_val = int(fill_latency_ms)
            latency_max = latency_val if latency_max is None else max(latency_max, latency_val)

    if abs(qty_sum) < 1e-12:
        return None, 0.0, float(fees), None, None, None, latency_max

    vwap = notional / qty_sum
    expected_px_avg = (expected_notional / expected_qty) if expected_qty > 0.0 else None
    mid_px_avg = (mid_notional / mid_qty) if mid_qty > 0.0 else None
    spread_bps_avg = (spread_sum / spread_n) if spread_n > 0 else None

    return float(vwap), float(qty_sum), float(fees), expected_px_avg, mid_px_avg, spread_bps_avg, latency_max


def _last_price(con, symbol: str) -> Optional[float]:
    """
    Uses existing prices table if present (fail-soft).
    Supports both column names: px (old) and price (some deployments).
    """
    try:
        r = con.execute(
            "SELECT px FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
        if r and r[0] is not None:
            px = float(r[0])
            return px if px > 0 else None
    except Exception as e:
        _warn_nonfatal("EXECUTION_LEDGER_LAST_PRICE_LOOKUP_FAILED", e, once_key=f"last_price_px:{symbol}", symbol=str(symbol), column="px")

    try:
        r = con.execute(
            "SELECT price FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
        if r and r[0] is not None:
            px = float(r[0])
            return px if px > 0 else None
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_LEDGER_LAST_PRICE_LOOKUP_FAILED",
            e,
            once_key=f"last_price_price:{symbol}",
            symbol=str(symbol),
            column="price",
        )
        return None

    return None


def _price_at_or_before(con, symbol: str, ts_ms: int) -> Optional[float]:
    try:
        r = con.execute(
            """
            SELECT px
            FROM prices
            WHERE symbol=? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
        if r and r[0] is not None:
            px = float(r[0])
            return px if px > 0 else None
    except Exception as e:
        _warn_nonfatal("EXECUTION_LEDGER_PRICE_AT_OR_BEFORE_LOOKUP_FAILED", e, once_key=f"price_at_or_before_px:{symbol}", symbol=str(symbol), column="px", ts_ms=int(ts_ms))

    try:
        r = con.execute(
            """
            SELECT price
            FROM prices
            WHERE symbol=? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
        if r and r[0] is not None:
            px = float(r[0])
            return px if px > 0 else None
    except Exception as e:
        _warn_nonfatal(
            "EXECUTION_LEDGER_PRICE_AT_OR_BEFORE_LOOKUP_FAILED",
            e,
            once_key=f"price_at_or_before_price:{symbol}",
            symbol=str(symbol),
            column="price",
            ts_ms=int(ts_ms),
        )
        return None

    return None


def _fill_cost_from_components(
    *,
    qty_signed: float,
    fill_px: float,
    fees: float,
    slippage_bps: Optional[float],
) -> float:
    slippage_cost = 0.0
    if slippage_bps is not None:
        # Slippage is signed: favorable fills reduce total execution cost.
        slippage_cost = (
            abs(float(qty_signed or 0.0))
            * abs(float(fill_px or 0.0))
            * float(slippage_bps)
            / 10000.0
        )
    return float(fees or 0.0) + float(slippage_cost)


def _apply_position_fill_state(
    state: Dict[str, Any],
    *,
    qty_signed: float,
    fill_px: float,
    fill_cost: float,
    client_order_id: Optional[str] = None,
    fill_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    out = {
        "gross_realized_pnl": 0.0,
        "net_realized_pnl": 0.0,
        "close_qty": 0.0,
        "opening_cost": 0.0,
    }
    open_qty = float(state.get("position_size") or 0.0)
    avg_price = float(state.get("avg_price") or 0.0)
    open_cost_basis = float(state.get("open_cost_basis") or 0.0)
    fill_cost_total = float(fill_cost or 0.0)
    fill_abs_qty = abs(float(qty_signed or 0.0))

    if fill_abs_qty <= 1e-12:
        return out

    if abs(open_qty) <= 1e-12:
        state["position_size"] = float(qty_signed)
        state["avg_price"] = float(fill_px)
        state["open_cost_basis"] = float(fill_cost_total)
        out["opening_cost"] = float(fill_cost_total)
        return out

    if open_qty * float(qty_signed) > 0.0:
        new_qty = float(open_qty) + float(qty_signed)
        state["avg_price"] = (
            (abs(float(open_qty)) * float(avg_price))
            + (abs(float(qty_signed)) * float(fill_px))
        ) / max(1e-12, abs(float(new_qty)))
        state["position_size"] = float(new_qty)
        state["open_cost_basis"] = float(open_cost_basis) + float(fill_cost_total)
        out["opening_cost"] = float(fill_cost_total)
        return out

    close_qty = min(abs(float(open_qty)), fill_abs_qty)
    close_ratio = close_qty / max(1e-12, fill_abs_qty)
    carry_ratio = close_qty / max(1e-12, abs(float(open_qty)))
    close_fill_cost = float(fill_cost_total) * float(close_ratio)
    carry_cost = float(open_cost_basis) * float(carry_ratio)
    gross_realized = (
        (float(fill_px) - float(avg_price))
        * float(close_qty)
        * (1.0 if float(open_qty) > 0.0 else -1.0)
    )
    net_realized = float(gross_realized) - float(close_fill_cost) - float(carry_cost)

    state["realized_pnl"] = float(state.get("realized_pnl") or 0.0) + float(gross_realized)

    remaining_qty = float(open_qty) + float(qty_signed)
    residual_fill_cost = float(fill_cost_total) - float(close_fill_cost)
    remaining_cost_basis = float(open_cost_basis) - float(carry_cost)
    if abs(remaining_qty) <= 1e-12:
        state["position_size"] = 0.0
        state["avg_price"] = 0.0
        state["open_cost_basis"] = 0.0
    elif float(open_qty) * float(remaining_qty) < 0.0:
        state["position_size"] = float(remaining_qty)
        state["avg_price"] = float(fill_px)
        state["open_cost_basis"] = float(residual_fill_cost)
    else:
        state["position_size"] = float(remaining_qty)
        state["avg_price"] = float(avg_price)
        state["open_cost_basis"] = float(remaining_cost_basis)

    trade_events = state.setdefault("realized_trade_events", [])
    trade_index = state.setdefault("realized_trade_index", {})
    exit_key = str(client_order_id or "").strip() or f"close:{int(fill_ts_ms or 0)}"
    trade_event = trade_index.get(exit_key)
    if trade_event is None:
        trade_event = {
            "client_order_id": exit_key,
            "gross_realized_pnl": 0.0,
            "net_realized_pnl": 0.0,
            "close_qty": 0.0,
            "fill_ts_ms": int(fill_ts_ms or 0),
        }
        trade_events.append(trade_event)
        trade_index[exit_key] = trade_event
    trade_event["gross_realized_pnl"] = float(trade_event.get("gross_realized_pnl") or 0.0) + float(gross_realized)
    trade_event["net_realized_pnl"] = float(trade_event.get("net_realized_pnl") or 0.0) + float(net_realized)
    trade_event["close_qty"] = float(trade_event.get("close_qty") or 0.0) + float(close_qty)
    trade_event["fill_ts_ms"] = max(int(trade_event.get("fill_ts_ms") or 0), int(fill_ts_ms or 0))

    out["gross_realized_pnl"] = float(gross_realized)
    out["net_realized_pnl"] = float(net_realized)
    out["close_qty"] = float(close_qty)
    out["opening_cost"] = float(residual_fill_cost) if abs(remaining_qty) > 1e-12 and float(open_qty) * float(remaining_qty) < 0.0 else 0.0
    return out


def _load_model_position_state(
    con,
    *,
    model_id: str,
    symbol: str,
) -> Dict[str, float]:
    row = con.execute(
        """
        SELECT net_qty, avg_entry_price, realized_pnl, last_update_ts_ms
        FROM model_position_state
        WHERE model_id=? AND symbol=?
        LIMIT 1
        """,
        (_normalize_model_id(model_id), str(symbol).upper().strip()),
    ).fetchone()
    if not row:
        return {
            "net_qty": 0.0,
            "avg_entry_price": 0.0,
            "realized_pnl": 0.0,
            "last_update_ts_ms": 0.0,
        }
    return {
        "net_qty": float(row[0] or 0.0),
        "avg_entry_price": float(row[1] or 0.0),
        "realized_pnl": float(row[2] or 0.0),
        "last_update_ts_ms": float(row[3] or 0.0),
    }


def _apply_model_position_fill_state(
    state: Dict[str, float],
    *,
    qty_signed: float,
    fill_px: float,
    fill_ts_ms: int,
) -> None:
    open_qty = float(state.get("net_qty") or 0.0)
    avg_entry_price = float(state.get("avg_entry_price") or 0.0)

    if abs(open_qty) <= 1e-12:
        state["net_qty"] = float(qty_signed)
        state["avg_entry_price"] = float(fill_px)
        state["last_update_ts_ms"] = float(fill_ts_ms)
        return

    if open_qty * float(qty_signed) > 0.0:
        new_qty = float(open_qty) + float(qty_signed)
        state["avg_entry_price"] = (
            (abs(float(open_qty)) * float(avg_entry_price))
            + (abs(float(qty_signed)) * float(fill_px))
        ) / max(1e-12, abs(float(new_qty)))
        state["net_qty"] = float(new_qty)
        state["last_update_ts_ms"] = float(fill_ts_ms)
        return

    close_qty = min(abs(float(open_qty)), abs(float(qty_signed)))
    state["realized_pnl"] = float(state.get("realized_pnl") or 0.0) + (
        (float(fill_px) - float(avg_entry_price))
        * float(close_qty)
        * (1.0 if float(open_qty) > 0.0 else -1.0)
    )

    remaining_qty = float(open_qty) + float(qty_signed)
    if abs(remaining_qty) <= 1e-12:
        state["net_qty"] = 0.0
        state["avg_entry_price"] = 0.0
        state["last_update_ts_ms"] = float(fill_ts_ms)
        return

    if float(open_qty) * float(remaining_qty) < 0.0:
        state["net_qty"] = float(remaining_qty)
        state["avg_entry_price"] = float(fill_px)
        state["last_update_ts_ms"] = float(fill_ts_ms)
        return

    state["net_qty"] = float(remaining_qty)
    state["avg_entry_price"] = float(avg_entry_price)
    state["last_update_ts_ms"] = float(fill_ts_ms)


def _upsert_model_position_state(
    con,
    *,
    model_id: str,
    symbol: str,
    qty_signed: float,
    fill_px: float,
    fill_ts_ms: int,
) -> Dict[str, float]:
    mid = _normalize_model_id(model_id)
    sym = str(symbol or "").upper().strip()
    if not sym or abs(float(qty_signed)) <= 1e-12 or float(fill_px) <= 0.0:
        return {}

    rows = con.execute(
        """
        SELECT fill_qty, fill_px, fill_ts_ms
        FROM execution_fills
        WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
          AND UPPER(TRIM(symbol)) = ?
        ORDER BY fill_ts_ms ASC, id ASC
        """,
        (str(mid), str(sym)),
    ).fetchall()
    state = {
        "net_qty": 0.0,
        "avg_entry_price": 0.0,
        "realized_pnl": 0.0,
        "last_update_ts_ms": 0.0,
    }
    for row_qty, row_fill_px, row_fill_ts_ms in rows or []:
        qty_f = float(row_qty or 0.0)
        fill_px_f = float(row_fill_px or 0.0)
        if abs(qty_f) <= 1e-12 or fill_px_f <= 0.0:
            continue
        _apply_model_position_fill_state(
            state,
            qty_signed=float(qty_f),
            fill_px=float(fill_px_f),
            fill_ts_ms=int(row_fill_ts_ms or 0),
        )
    con.execute(
        """
        INSERT INTO model_position_state(
          model_id,
          symbol,
          net_qty,
          avg_entry_price,
          realized_pnl,
          last_update_ts_ms
        )
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(model_id, symbol) DO UPDATE SET
          net_qty=excluded.net_qty,
          avg_entry_price=excluded.avg_entry_price,
          realized_pnl=excluded.realized_pnl,
          last_update_ts_ms=excluded.last_update_ts_ms
        """,
        (
            str(mid),
            str(sym),
            float(state.get("net_qty") or 0.0),
            float(state.get("avg_entry_price") or 0.0),
            float(state.get("realized_pnl") or 0.0),
            int(state.get("last_update_ts_ms") or 0),
        ),
    )
    cache_invalidate_namespace("api_read", prefix="pnl")
    return state


def audit_execution_integrity(
    *,
    model_id: Optional[str] = None,
    con=None,
) -> Dict[str, Any]:
    owns_con = con is None
    if owns_con:
        init_execution_ledger()
        con = connect(readonly=True)

    try:
        mid = _normalize_model_id(model_id) if model_id else None
        now_ms = int(time.time() * 1000)
        stale_missing_fill_ms = int(os.environ.get("EXEC_INTEGRITY_MISSING_FILL_STALE_MS", "300000"))
        pending_order_reconcile_ms = int(os.environ.get("EXEC_INTEGRITY_PENDING_ORDER_RECONCILE_MS", "60000"))
        unrealized_price_max_age_ms = int(os.environ.get("EXEC_INTEGRITY_UNREALIZED_PRICE_MAX_AGE_MS", "60000"))
        order_params: tuple[Any, ...] = tuple()
        fill_params: tuple[Any, ...] = tuple()
        state_params: tuple[Any, ...] = tuple()
        order_filter = "1=1"
        order_filter_o = "1=1"
        fill_filter = "1=1"
        fill_filter_f = "1=1"
        state_filter = "1=1"
        if mid:
            order_params = (str(mid),)
            fill_params = (str(mid),)
            state_params = (str(mid),)
            order_filter = "COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?"
            order_filter_o = "COALESCE(NULLIF(TRIM(o.model_id), ''), 'baseline') = ?"
            fill_filter = "COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?"
            fill_filter_f = "COALESCE(NULLIF(TRIM(f.model_id), ''), 'baseline') = ?"
            state_filter = "COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?"

        duplicate_order_rows = con.execute(
            """
            SELECT order_uid, COUNT(DISTINCT client_order_id) AS order_count
            FROM execution_orders
            WHERE """ + order_filter + """
              AND order_uid IS NOT NULL
              AND TRIM(order_uid) <> ''
            GROUP BY order_uid
            HAVING COUNT(DISTINCT client_order_id) > 1
            ORDER BY order_count DESC, order_uid ASC
            """,
            order_params,
        ).fetchall()

        duplicate_fill_rows = con.execute(
            """
            SELECT client_order_id, fill_id, COUNT(*) AS fill_count
            FROM execution_fills
            WHERE """ + fill_filter + """
              AND fill_id IS NOT NULL
              AND TRIM(fill_id) <> ''
            GROUP BY client_order_id, fill_id
            HAVING COUNT(*) > 1
            ORDER BY fill_count DESC, client_order_id ASC, fill_id ASC
            """,
            fill_params,
        ).fetchall()

        missing_fill_rows = con.execute(
            """
            SELECT o.client_order_id
            FROM execution_orders o
            LEFT JOIN execution_fills f
              ON f.client_order_id = o.client_order_id
            WHERE """ + order_filter_o + """
            GROUP BY o.client_order_id
            HAVING COUNT(f.id) = 0
            ORDER BY o.client_order_id ASC
            """,
            order_params,
        ).fetchall()

        stale_missing_fill_rows = con.execute(
            """
            SELECT o.client_order_id, o.status, o.submit_ts_ms
            FROM execution_orders o
            LEFT JOIN execution_fills f
              ON f.client_order_id = o.client_order_id
            WHERE """ + order_filter_o + """
              AND LOWER(COALESCE(TRIM(o.status), 'submitted')) NOT IN (
                'cancelled',
                'canceled',
                'rejected',
                'expired',
                'filled'
              )
              AND o.submit_ts_ms <= ?
            GROUP BY o.client_order_id, o.status, o.submit_ts_ms
            HAVING COUNT(f.id) = 0
            ORDER BY o.submit_ts_ms ASC, o.client_order_id ASC
            """,
            order_params + (int(now_ms - stale_missing_fill_ms),),
        ).fetchall()

        fills_without_order_rows = con.execute(
            """
            SELECT f.client_order_id, f.fill_id, f.symbol, f.fill_ts_ms
            FROM execution_fills f
            LEFT JOIN execution_orders o
              ON o.client_order_id = f.client_order_id
            WHERE """ + fill_filter_f + """
              AND o.client_order_id IS NULL
            ORDER BY f.fill_ts_ms ASC, f.id ASC
            """,
            fill_params,
        ).fetchall()

        unreconciled_order_reference_rows = con.execute(
            """
            SELECT client_order_id, symbol, submit_ts_ms
            FROM execution_orders
            WHERE """ + order_filter + """
              AND LOWER(COALESCE(TRIM(status), '')) = 'fill_pending_submit'
              AND submit_ts_ms <= ?
            ORDER BY submit_ts_ms ASC, client_order_id ASC
            """,
            order_params + (int(now_ms - pending_order_reconcile_ms),),
        ).fetchall()

        submission_unrecorded_rows = []
        if _table_exists(con, "execution_order_idempotency"):
            submission_unrecorded_rows = con.execute(
                """
                SELECT order_uid, client_order_id, broker_order_id, symbol, updated_ts_ms, last_error
                FROM execution_order_idempotency
                WHERE LOWER(COALESCE(TRIM(status), '')) = 'submission_unrecorded'
                ORDER BY updated_ts_ms ASC, order_uid ASC
                """
            ).fetchall()

        out_of_order_rows = con.execute(
            """
            SELECT f.client_order_id, f.fill_id, f.fill_ts_ms, f.id
            FROM execution_fills f
            WHERE """ + fill_filter_f + """
              AND EXISTS (
              SELECT 1
              FROM execution_fills prev
              WHERE COALESCE(NULLIF(TRIM(prev.model_id), ''), 'baseline')
                      = COALESCE(NULLIF(TRIM(f.model_id), ''), 'baseline')
                AND UPPER(TRIM(prev.symbol)) = UPPER(TRIM(f.symbol))
                AND prev.id < f.id
                AND prev.fill_ts_ms > f.fill_ts_ms
            )
            ORDER BY f.client_order_id ASC, f.id ASC
            """,
            fill_params,
        ).fetchall()

        key_rows = con.execute(
            """
            SELECT model_id, symbol
            FROM model_position_state
            WHERE """ + state_filter + """
            UNION
            SELECT COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') AS model_id, UPPER(TRIM(symbol)) AS symbol
            FROM execution_fills
            WHERE """ + fill_filter + """
            """,
            state_params + fill_params,
        ).fetchall()
        position_mismatches = []
        for row_model_id, row_symbol in key_rows or []:
            row_mid = _normalize_model_id(row_model_id)
            row_sym = str(row_symbol or "").upper().strip()
            if not row_sym:
                continue
            current = _load_model_position_state(con, model_id=row_mid, symbol=row_sym)
            recomputed = {
                "net_qty": 0.0,
                "avg_entry_price": 0.0,
                "realized_pnl": 0.0,
                "last_update_ts_ms": 0.0,
            }
            fill_rows = con.execute(
                """
                SELECT fill_qty, fill_px, fill_ts_ms
                FROM execution_fills
                WHERE COALESCE(NULLIF(TRIM(model_id), ''), 'baseline') = ?
                  AND UPPER(TRIM(symbol)) = ?
                ORDER BY fill_ts_ms ASC, id ASC
                """,
                (str(row_mid), str(row_sym)),
            ).fetchall()
            for qty_signed, fill_px, fill_ts_ms in fill_rows or []:
                qty_f = float(qty_signed or 0.0)
                fill_px_f = float(fill_px or 0.0)
                if abs(qty_f) <= 1e-12 or fill_px_f <= 0.0:
                    continue
                _apply_model_position_fill_state(
                    recomputed,
                    qty_signed=float(qty_f),
                    fill_px=float(fill_px_f),
                    fill_ts_ms=int(fill_ts_ms or 0),
                )
            mismatch = (
                abs(float(current.get("net_qty") or 0.0) - float(recomputed.get("net_qty") or 0.0)) > 1e-9
                or abs(float(current.get("avg_entry_price") or 0.0) - float(recomputed.get("avg_entry_price") or 0.0)) > 1e-9
                or abs(float(current.get("realized_pnl") or 0.0) - float(recomputed.get("realized_pnl") or 0.0)) > 1e-9
                or int(current.get("last_update_ts_ms") or 0) != int(recomputed.get("last_update_ts_ms") or 0)
            )
            if mismatch:
                position_mismatches.append(
                    {
                        "model_id": str(row_mid),
                        "symbol": str(row_sym),
                        "current": {
                            "net_qty": float(current.get("net_qty") or 0.0),
                            "avg_entry_price": float(current.get("avg_entry_price") or 0.0),
                            "realized_pnl": float(current.get("realized_pnl") or 0.0),
                            "last_update_ts_ms": int(current.get("last_update_ts_ms") or 0),
                        },
                        "expected": {
                            "net_qty": float(recomputed.get("net_qty") or 0.0),
                            "avg_entry_price": float(recomputed.get("avg_entry_price") or 0.0),
                            "realized_pnl": float(recomputed.get("realized_pnl") or 0.0),
                            "last_update_ts_ms": int(recomputed.get("last_update_ts_ms") or 0),
                        },
                    }
                )

        pricing_unavailable_positions = []
        open_position_rows = con.execute(
            """
            SELECT model_id, symbol, net_qty, last_update_ts_ms
            FROM model_position_state
            WHERE """ + state_filter + """
              AND ABS(COALESCE(net_qty, 0)) > 1e-12
            ORDER BY last_update_ts_ms ASC, symbol ASC
            """,
            state_params,
        ).fetchall()
        prices_available = _table_exists(con, "prices")
        for row_model_id, row_symbol, net_qty, last_update_ts_ms in open_position_rows or []:
            row_mid = _normalize_model_id(row_model_id)
            row_sym = str(row_symbol or "").upper().strip()
            if not row_sym:
                continue
            if not prices_available:
                pricing_unavailable_positions.append(
                    {
                        "model_id": str(row_mid),
                        "symbol": str(row_sym),
                        "net_qty": float(net_qty or 0.0),
                        "last_update_ts_ms": int(last_update_ts_ms or 0),
                        "detail": "prices_table_missing",
                    }
                )
                continue
            price_row = con.execute(
                """
                SELECT COALESCE(price, px) AS last_px, ts_ms
                FROM prices
                WHERE UPPER(TRIM(symbol)) = ?
                  AND COALESCE(price, px) IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (str(row_sym),),
            ).fetchone()
            if not price_row:
                pricing_unavailable_positions.append(
                    {
                        "model_id": str(row_mid),
                        "symbol": str(row_sym),
                        "net_qty": float(net_qty or 0.0),
                        "last_update_ts_ms": int(last_update_ts_ms or 0),
                        "detail": "price_missing",
                    }
                )
                continue
            last_px = float(price_row[0] or 0.0)
            price_ts_ms = int(price_row[1] or 0)
            price_age_ms = int(now_ms - price_ts_ms) if price_ts_ms > 0 else -1
            if last_px <= 0.0 or price_age_ms < 0 or price_age_ms > unrealized_price_max_age_ms:
                pricing_unavailable_positions.append(
                    {
                        "model_id": str(row_mid),
                        "symbol": str(row_sym),
                        "net_qty": float(net_qty or 0.0),
                        "last_update_ts_ms": int(last_update_ts_ms or 0),
                        "price_ts_ms": int(price_ts_ms),
                        "price_age_ms": int(price_age_ms),
                        "detail": ("price_nonpositive" if last_px <= 0.0 else "price_stale"),
                    }
                )

        duplicate_order_count = sum(max(0, int(row[1] or 0) - 1) for row in duplicate_order_rows or [])
        duplicate_fill_count = sum(max(0, int(row[2] or 0) - 1) for row in duplicate_fill_rows or [])
        missing_fill_count = int(len(missing_fill_rows or []))
        stale_missing_fill_count = int(len(stale_missing_fill_rows or []))
        fills_without_order_count = int(len(fills_without_order_rows or []))
        unreconciled_order_reference_count = int(len(unreconciled_order_reference_rows or []))
        submission_unrecorded_count = int(len(submission_unrecorded_rows or []))
        out_of_order_fill_count = int(len(out_of_order_rows or []))
        inconsistent_position_count = int(len(position_mismatches))
        pricing_unavailable_count = int(len(pricing_unavailable_positions))
        return {
            "ok": (
                duplicate_order_count == 0
                and duplicate_fill_count == 0
                and fills_without_order_count == 0
                and unreconciled_order_reference_count == 0
                and submission_unrecorded_count == 0
                and stale_missing_fill_count == 0
                and inconsistent_position_count == 0
            ),
            "model_id": str(mid or ""),
            "duplicate_order_count": int(duplicate_order_count),
            "duplicate_fill_count": int(duplicate_fill_count),
            "missing_fill_count": int(missing_fill_count),
            "stale_missing_fill_count": int(stale_missing_fill_count),
            "fills_without_order_count": int(fills_without_order_count),
            "unreconciled_order_reference_count": int(unreconciled_order_reference_count),
            "submission_unrecorded_count": int(submission_unrecorded_count),
            "out_of_order_fill_count": int(out_of_order_fill_count),
            "inconsistent_position_count": int(inconsistent_position_count),
            "pricing_unavailable_count": int(pricing_unavailable_count),
            "stale_missing_fill_threshold_ms": int(stale_missing_fill_ms),
            "pending_order_reconcile_threshold_ms": int(pending_order_reconcile_ms),
            "unrealized_price_max_age_ms": int(unrealized_price_max_age_ms),
            "duplicate_orders": [
                {"order_uid": str(order_uid), "order_count": int(order_count or 0)}
                for order_uid, order_count in (duplicate_order_rows or [])[:20]
            ],
            "duplicate_fills": [
                {
                    "client_order_id": str(client_order_id),
                    "fill_id": str(fill_id),
                    "fill_count": int(fill_count or 0),
                }
                for client_order_id, fill_id, fill_count in (duplicate_fill_rows or [])[:20]
            ],
            "missing_fills": [
                {"client_order_id": str(client_order_id)}
                for (client_order_id,) in (missing_fill_rows or [])[:20]
            ],
            "stale_missing_fills": [
                {
                    "client_order_id": str(client_order_id),
                    "status": str(status or ""),
                    "submit_ts_ms": int(submit_ts_ms or 0),
                }
                for client_order_id, status, submit_ts_ms in (stale_missing_fill_rows or [])[:20]
            ],
            "fills_without_order": [
                {
                    "client_order_id": str(client_order_id),
                    "fill_id": str(fill_id or ""),
                    "symbol": str(symbol or ""),
                    "fill_ts_ms": int(fill_ts_ms or 0),
                }
                for client_order_id, fill_id, symbol, fill_ts_ms in (fills_without_order_rows or [])[:20]
            ],
            "unreconciled_order_references": [
                {
                    "client_order_id": str(client_order_id),
                    "symbol": str(symbol or ""),
                    "submit_ts_ms": int(submit_ts_ms or 0),
                }
                for client_order_id, symbol, submit_ts_ms in (unreconciled_order_reference_rows or [])[:20]
            ],
            "submission_unrecorded": [
                {
                    "order_uid": str(order_uid or ""),
                    "client_order_id": str(client_order_id or ""),
                    "broker_order_id": str(broker_order_id or ""),
                    "symbol": str(symbol or "").upper().strip(),
                    "updated_ts_ms": int(updated_ts_ms or 0),
                    "last_error": str(last_error or ""),
                }
                for order_uid, client_order_id, broker_order_id, symbol, updated_ts_ms, last_error in (submission_unrecorded_rows or [])[:20]
            ],
            "out_of_order_fills": [
                {
                    "client_order_id": str(client_order_id),
                    "fill_id": str(fill_id),
                    "fill_ts_ms": int(fill_ts_ms or 0),
                    "row_id": int(row_id or 0),
                }
                for client_order_id, fill_id, fill_ts_ms, row_id in (out_of_order_rows or [])[:20]
            ],
            "position_mismatches": list(position_mismatches[:20]),
            "pricing_unavailable_positions": list(pricing_unavailable_positions[:20]),
        }
    finally:
        if owns_con:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("EXECUTION_LEDGER_AUDIT_CLOSE_FAILED", e, once_key="audit_close")


def compute_metrics_snapshot(limit_orders: int = 500) -> Dict[str, Any]:
    """
    Computes:
      - slippage_bps vs expected_px/ref_px for each executed client_order_id
      - mark-to-market pnl (signed) using latest price (from prices table)
      - mid, spread, fill latency snapshots

    Stores into execution_metrics at current ts_ms.
    """
    init_execution_ledger()
    def _write(con):
        ts = now_ms()
        ts = ts - (ts % 1000)

        rows = con.execute(
            """
            SELECT client_order_id, broker, symbol, qty, ref_px, expected_px, mid_px, submit_ts_ms
            FROM execution_orders
            ORDER BY submit_ts_ms DESC
            LIMIT ?
            """,
            (int(max(1, min(20000, int(limit_orders)))),),
        ).fetchall()

        n = 0
        for cid, broker, sym, qty, ref_px, expected_px, mid_px, submit_ts_ms in rows or []:
            cid = str(cid)
            broker = str(broker) if broker is not None else None
            sym = str(sym)
            qty = float(qty or 0.0)
            ref_px_f = float(ref_px) if ref_px is not None else None
            expected_px_f = float(expected_px) if expected_px is not None else ref_px_f
            mid_px_f = float(mid_px) if mid_px is not None else expected_px_f

            vwap, filled_qty_signed, fees, expected_fill_px, mid_fill_px, spread_bps_avg, fill_latency_ms = _vwap_for_client(con, cid)
            if vwap is None:
                continue

            effective_expected_px = expected_fill_px if expected_fill_px is not None else expected_px_f
            effective_mid_px = mid_fill_px if mid_fill_px is not None else mid_px_f

            sl_bps = None
            if effective_expected_px is not None and effective_expected_px > 0:
                sign = 1.0 if qty > 0 else -1.0
                sl_bps = ((float(vwap) - float(effective_expected_px)) / float(effective_expected_px)) * 10000.0 * sign

            last_px = _last_price(con, sym)

            m2m = None
            if last_px is not None:
                m2m = (float(last_px) - float(vwap)) * float(filled_qty_signed)

            con.execute(
                """
                INSERT OR REPLACE INTO execution_metrics(
                  ts_ms,
                  client_order_id,
                  broker,
                  symbol,
                  submit_qty,
                  filled_qty,
                  ref_px,
                  expected_px,
                  mid_px,
                  fill_px,
                  fill_vwap,
                  spread_bps,
                  slippage_bps,
                  fill_latency_ms,
                  fees,
                  m2m_pnl,
                  last_px
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(ts),
                    cid,
                    broker,
                    sym,
                    float(qty),
                    float(filled_qty_signed),
                    float(ref_px_f) if ref_px_f is not None else None,
                    float(effective_expected_px) if effective_expected_px is not None else None,
                    float(effective_mid_px) if effective_mid_px is not None else None,
                    float(vwap),
                    float(vwap),
                    float(spread_bps_avg) if spread_bps_avg is not None else None,
                    float(sl_bps) if sl_bps is not None else None,
                    int(fill_latency_ms) if fill_latency_ms is not None else None,
                    float(fees) if fees is not None else 0.0,
                    float(m2m) if m2m is not None else None,
                    float(last_px) if last_px is not None else None,
                ),
            )
            n += 1

        cache_invalidate_namespace("api_read", prefix="execution_metrics")
        cache_invalidate_namespace("api_read", prefix="execution_stats")

        return {"ok": True, "metrics_written": int(n), "ts_ms": int(ts)}
    return run_write_txn(
        _write,
        table="execution_metrics",
        operation="compute_metrics_snapshot",
        context={"limit_orders": int(limit_orders)},
    )


def _recompute_pnl_attribution_snapshot(
    con,
    *,
    snapshot_ts_ms: int,
    lookback_orders: int,
    historical: bool,
) -> Dict[str, Any]:
    ts = int(snapshot_ts_ms or 0)
    if ts <= 0:
        return {"ok": False, "status": "no_execution_metrics"}
    limit_n = int(max(1, min(20000, int(lookback_orders))))
    selected_keys: set[Tuple[int, str, str]] = set()
    key_sql = """
        SELECT
          o.source_alert_id,
          COALESCE(NULLIF(TRIM(o.model_id), ''), 'baseline') AS model_id,
          UPPER(TRIM(o.symbol)) AS symbol,
          MAX(f.fill_ts_ms) AS last_fill_ts_ms
        FROM execution_orders o
        JOIN execution_fills f
          ON f.client_order_id = o.client_order_id
        WHERE o.source_alert_id IS NOT NULL
          {fill_time_filter}
        GROUP BY o.source_alert_id, COALESCE(NULLIF(TRIM(o.model_id), ''), 'baseline'), UPPER(TRIM(o.symbol))
        ORDER BY last_fill_ts_ms DESC, o.source_alert_id DESC
        LIMIT ?
    """
    fill_time_filter = "AND f.fill_ts_ms <= ?" if historical else ""
    key_params = (int(ts), int(limit_n)) if historical else (int(limit_n),)
    key_rows = con.execute(
        key_sql.format(fill_time_filter=fill_time_filter),
        key_params,
    ).fetchall()
    for sid, model_id, symbol, _last_fill_ts_ms in key_rows or []:
        if sid is None or symbol in (None, ""):
            continue
        selected_keys.add((int(sid), _normalize_model_id(model_id), str(symbol).upper().strip()))

    if not historical and _table_exists(con, "pnl_attribution"):
        try:
            prev_rows = con.execute(
                """
                SELECT source_alert_id, model_id, symbol
                FROM pnl_attribution
                WHERE ts_ms = (SELECT MAX(ts_ms) FROM pnl_attribution)
                  AND ABS(COALESCE(position_size, 0.0)) > 1e-12
                """
            ).fetchall()
        except Exception:
            prev_rows = []
        for sid, model_id, symbol in prev_rows or []:
            if sid is None or symbol in (None, ""):
                continue
            selected_keys.add((int(sid), _normalize_model_id(model_id), str(symbol).upper().strip()))

    if not selected_keys:
        return {
            "ok": True,
            "attribution_written": 0,
            "ts_ms": int(ts),
            "metrics_ts_ms": int(ts),
            "historical": bool(historical),
        }

    rows = con.execute(
        f"""
        SELECT
          o.source_alert_id,
          o.prediction_id,
          COALESCE(NULLIF(TRIM(o.model_id), ''), 'baseline') AS model_id,
          o.model_version,
          o.symbol,
          o.qty,
          o.submit_ts_ms,
          o.client_order_id,
          o.extra_json,
          f.fill_qty,
          f.fill_px,
          f.fill_ts_ms,
          f.fill_latency_ms,
          COALESCE(f.fees,0),
          f.slippage_bps,
          COALESCE(f.mid_px, o.mid_px),
          COALESCE(f.expected_px, o.expected_px)
        FROM execution_orders o
        JOIN execution_fills f
          ON f.client_order_id = o.client_order_id
        WHERE o.source_alert_id IS NOT NULL
          {fill_time_filter}
        ORDER BY
          COALESCE(o.submit_ts_ms, f.fill_ts_ms) ASC,
          f.fill_ts_ms ASC,
          f.id ASC,
          o.client_order_id ASC
        """,
        ((int(ts),) if historical else tuple()),
    ).fetchall()

    agg: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    for (
        sid,
        prediction_id,
        model_id,
        model_version,
        sym,
        order_qty,
        submit_ts_ms,
        client_order_id,
        order_extra_json,
        fill_qty,
        fill_px,
        fill_ts_ms,
        fill_latency_ms,
        fees,
        slippage_bps,
        mid_px,
        expected_px,
    ) in rows or []:
        if sid is None:
            continue

        sym_u = str(sym or "").strip().upper()
        if not sym_u:
            continue

        qty_signed = float(fill_qty or 0.0)
        if abs(qty_signed) <= 1e-12:
            qty_signed = float(order_qty or 0.0)
        if abs(qty_signed) <= 1e-12:
            continue

        fill_px_f = float(fill_px or 0.0)
        if fill_px_f <= 0.0:
            continue

        k = (int(sid), _normalize_model_id(model_id), sym_u)
        if k not in selected_keys:
            continue
        cur = agg.get(k) or {
            "prediction_id": (_safe_int(prediction_id, 0) if prediction_id is not None else None),
            "model_version": (str(model_version).strip() if model_version not in (None, "") else None),
            "position_size": 0.0,
            "avg_price": 0.0,
            "realized_pnl": 0.0,
            "unrealized_pnl": 0.0,
            "open_cost_basis": 0.0,
            "fees": 0.0,
            "slippage_bps_weighted": 0.0,
            "slippage_weight": 0.0,
            "slippage_cost": 0.0,
            "fill_price_notional": 0.0,
            "fill_price_weight": 0.0,
            "expected_price_notional": 0.0,
            "expected_price_weight": 0.0,
            "slippage_price_weighted": 0.0,
            "abs_slippage_price_weighted": 0.0,
            "execution_latency_ms_sum": 0.0,
            "execution_latency_ms_n": 0.0,
            "execution_latency_ms_max": 0.0,
            "notional_traded": 0.0,
            "fill_count": 0.0,
            "min_submit_ts": 0.0,
            "market_regime": None,
            "market_regime_snapshot": None,
            "realized_trade_events": [],
            "realized_trade_index": {},
        }
        if cur.get("prediction_id") in (None, 0) and prediction_id not in (None, ""):
            cur["prediction_id"] = _safe_int(prediction_id, 0)
        if cur.get("model_version") in (None, "") and model_version not in (None, ""):
            cur["model_version"] = str(model_version).strip()

        abs_qty = abs(float(qty_signed))
        fill_cost_total = _fill_cost_from_components(
            qty_signed=float(qty_signed),
            fill_px=float(fill_px_f),
            fees=float(fees or 0.0),
            slippage_bps=(float(slippage_bps) if slippage_bps is not None else None),
        )
        cur["fees"] += float(fees or 0.0)
        cur["notional_traded"] += abs_qty * float(fill_px_f)
        cur["fill_count"] += 1.0
        cur["fill_price_notional"] += abs_qty * float(fill_px_f)
        cur["fill_price_weight"] += abs_qty

        if slippage_bps is not None:
            sl_bps = float(slippage_bps)
            cur["slippage_bps_weighted"] += abs_qty * float(sl_bps)
            cur["slippage_weight"] += abs_qty
            cur["slippage_cost"] += abs_qty * float(fill_px_f) * float(sl_bps) / 10000.0

        if expected_px is not None:
            expected_px_f = float(expected_px or 0.0)
            if expected_px_f > 0.0:
                cur["expected_price_notional"] += abs_qty * float(expected_px_f)
                cur["expected_price_weight"] += abs_qty
                px_slippage = float(fill_px_f) - float(expected_px_f)
                cur["slippage_price_weighted"] += abs_qty * float(px_slippage)
                cur["abs_slippage_price_weighted"] += abs_qty * abs(float(px_slippage))
        if fill_latency_ms is not None:
            lat_ms = max(0.0, float(fill_latency_ms))
            cur["execution_latency_ms_sum"] += lat_ms
            cur["execution_latency_ms_n"] += 1.0
            cur["execution_latency_ms_max"] = max(float(cur.get("execution_latency_ms_max") or 0.0), lat_ms)

        extra_obj = _safe_json_dict(order_extra_json)
        if cur.get("market_regime") is None:
            market_regime = extra_obj.get("market_regime")
            if market_regime is None:
                market_regime = extra_obj.get("market_regime_label")
            if market_regime is not None:
                cur["market_regime"] = str(market_regime)
        if cur.get("market_regime_snapshot") is None:
            snap = extra_obj.get("market_regime_snapshot")
            if isinstance(snap, dict):
                cur["market_regime_snapshot"] = {
                    "label": str(snap.get("label") or cur.get("market_regime") or "mean_reversion"),
                    "volatility": float(snap.get("volatility", 0.0) or 0.0),
                    "volatility_baseline": float(snap.get("volatility_baseline", 0.0) or 0.0),
                    "trend": float(snap.get("trend", 0.0) or 0.0),
                    "trend_strength": float(snap.get("trend_strength", 0.0) or 0.0),
                }

        st = float(submit_ts_ms or 0.0)
        if cur["min_submit_ts"] <= 0.0 or (st > 0.0 and st < cur["min_submit_ts"]):
            cur["min_submit_ts"] = st

        _apply_position_fill_state(
            cur,
            qty_signed=float(qty_signed),
            fill_px=float(fill_px_f),
            fill_cost=float(fill_cost_total),
            client_order_id=str(client_order_id or ""),
            fill_ts_ms=int(fill_ts_ms or 0),
        )
        agg[k] = cur

    n = 0
    timescale_trade_outcome_rows: List[Dict[str, Any]] = []
    for (sid, model_id, sym), v in agg.items():
        last_px = (
            _price_at_or_before(con, str(sym), int(ts))
            if historical
            else _last_price(con, str(sym))
        )
        position_size = float(v.get("position_size") or 0.0)
        avg_price = float(v.get("avg_price") or 0.0)
        unrealized_pnl = 0.0
        if last_px is not None and abs(position_size) > 1e-12 and avg_price > 0.0:
            unrealized_pnl = (float(last_px) - float(avg_price)) * float(position_size)

        avg_sl = (
            float(v["slippage_bps_weighted"]) / max(1.0, float(v["slippage_weight"]))
            if float(v.get("slippage_weight") or 0.0) > 0.0
            else None
        )
        avg_expected_price = (
            float(v["expected_price_notional"]) / max(1.0, float(v["expected_price_weight"]))
            if float(v.get("expected_price_weight") or 0.0) > 0.0
            else None
        )
        avg_fill_price = (
            float(v["fill_price_notional"]) / max(1.0, float(v["fill_price_weight"]))
            if float(v.get("fill_price_weight") or 0.0) > 0.0
            else None
        )
        avg_exec_slippage = (
            float(v["slippage_price_weighted"]) / max(1.0, float(v["expected_price_weight"]))
            if float(v.get("expected_price_weight") or 0.0) > 0.0
            else None
        )
        avg_abs_exec_slippage = (
            float(v["abs_slippage_price_weighted"]) / max(1.0, float(v["expected_price_weight"]))
            if float(v.get("expected_price_weight") or 0.0) > 0.0
            else None
        )
        avg_exec_latency_ms = (
            float(v["execution_latency_ms_sum"]) / max(1.0, float(v["execution_latency_ms_n"]))
            if float(v.get("execution_latency_ms_n") or 0.0) > 0.0
            else None
        )
        realized_pnl = float(v.get("realized_pnl") or 0.0)
        fees_sum = float(v["fees"])
        slippage_cost = float(v.get("slippage_cost") or 0.0)
        total_cost = float(fees_sum) + float(slippage_cost)
        total_pnl = float(realized_pnl) + float(unrealized_pnl) - float(total_cost)
        realized_trade_events = list(v.get("realized_trade_events") or [])
        realized_trade_events.sort(
            key=lambda row: (
                int((row or {}).get("fill_ts_ms") or 0),
                str((row or {}).get("client_order_id") or ""),
            )
        )
        realized_trade_net_pnls = [
            float((row or {}).get("net_realized_pnl") or 0.0)
            for row in realized_trade_events
        ]
        realized_trade_gross_pnls = [
            float((row or {}).get("gross_realized_pnl") or 0.0)
            for row in realized_trade_events
        ]
        realized_trade_client_order_ids = [
            str((row or {}).get("client_order_id") or "")
            for row in realized_trade_events
            if str((row or {}).get("client_order_id") or "").strip()
        ]
        for row in realized_trade_events:
            trade_id = str((row or {}).get("client_order_id") or "").strip()
            fill_ts_ms = int((row or {}).get("fill_ts_ms") or 0)
            net_realized_pnl = float((row or {}).get("net_realized_pnl") or 0.0)
            if not trade_id or fill_ts_ms <= 0:
                continue
            timescale_trade_outcome_rows.append(
                {
                    "trade_id": trade_id,
                    "timestamp": int(fill_ts_ms),
                    "pnl": float(net_realized_pnl),
                    "outcome": _trade_outcome_label(net_realized_pnl),
                }
            )

        first_submit = int(v.get("min_submit_ts") or 0.0) or None
        capital_hours = 0.0
        if first_submit:
            capital_hours = max(0.0, (int(ts) - int(first_submit)) / 3600000.0)

        risk_unit = abs(total_pnl) if abs(total_pnl) > 1e-9 else 1.0
        return_per_risk = total_pnl / risk_unit
        drawdown = -min(0.0, total_pnl)

        efficiency_score = 0.0
        if capital_hours > 0:
            efficiency_score = total_pnl / capital_hours

        con.execute(
            """
            INSERT OR REPLACE INTO pnl_attribution(
              ts_ms,
              source_alert_id,
              prediction_id,
              model_id,
              model_version,
              symbol,
              pnl,
              fees,
              slippage_bps,
              position_size,
              avg_price,
              realized_pnl,
              unrealized_pnl,
              extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(ts),
                int(sid),
                (int(v.get("prediction_id")) if v.get("prediction_id") not in (None, 0, "") else None),
                _normalize_model_id(model_id),
                (str(v.get("model_version")).strip() if v.get("model_version") not in (None, "") else None),
                str(sym),
                float(total_pnl),
                float(fees_sum),
                (float(avg_sl) if avg_sl is not None else None),
                float(position_size),
                (float(avg_price) if abs(position_size) > 1e-12 and avg_price > 0.0 else None),
                float(realized_pnl),
                float(unrealized_pnl),
                json.dumps(
                    {
                        "metrics_ts_ms": int(ts),
                        "fill_count": int(v["fill_count"]),
                        "prediction_id": (int(v.get("prediction_id")) if v.get("prediction_id") not in (None, 0, "") else None),
                        "model_id": _normalize_model_id(model_id),
                        "model_version": (str(v.get("model_version")).strip() if v.get("model_version") not in (None, "") else None),
                        "position_size": float(position_size),
                        "avg_price": (
                            float(avg_price)
                            if abs(position_size) > 1e-12 and avg_price > 0.0
                            else None
                        ),
                        "last_px": (float(last_px) if last_px is not None else None),
                        "realized_pnl": float(realized_pnl),
                        "unrealized_pnl": float(unrealized_pnl),
                        "fee_cost": float(fees_sum),
                        "slippage_cost": float(slippage_cost),
                        "total_cost": float(total_cost),
                        "total_pnl": float(total_pnl),
                        "notional_traded": float(v.get("notional_traded") or 0.0),
                        "open_cost_basis": float(v.get("open_cost_basis") or 0.0),
                        "realized_trade_count": int(len(realized_trade_events)),
                        "realized_trade_pnls": list(realized_trade_net_pnls),
                        "realized_trade_gross_pnls": list(realized_trade_gross_pnls),
                        "realized_trade_client_order_ids": list(realized_trade_client_order_ids),
                        "execution_quality": {
                            "expected_price": avg_expected_price,
                            "fill_price": avg_fill_price,
                            "slippage": avg_exec_slippage,
                            "abs_slippage": avg_abs_exec_slippage,
                            "avg_latency_ms": avg_exec_latency_ms,
                            "max_latency_ms": float(v.get("execution_latency_ms_max") or 0.0),
                        },
                        "market_regime": (
                            str(v.get("market_regime"))
                            if v.get("market_regime") is not None
                            else None
                        ),
                        "market_regime_snapshot": (
                            dict(v.get("market_regime_snapshot") or {})
                            if isinstance(v.get("market_regime_snapshot"), dict)
                            else None
                        ),
                        "historical_recompute": bool(historical),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

        con.execute(
            """
            INSERT OR REPLACE INTO capital_efficiency(
              ts_ms,
              source_alert_id,
              model_id,
              model_version,
              symbol,
              capital_hours,
              return_per_risk,
              drawdown_contribution,
              efficiency_score,
              extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(ts),
                int(sid),
                _normalize_model_id(model_id),
                (str(v.get("model_version")).strip() if v.get("model_version") not in (None, "") else None),
                str(sym),
                float(capital_hours),
                float(return_per_risk),
                float(drawdown),
                float(efficiency_score),
                json.dumps(
                    {
                        "pnl": float(total_pnl),
                        "fees": float(fees_sum),
                        "realized_pnl": float(realized_pnl),
                        "unrealized_pnl": float(unrealized_pnl),
                        "historical_recompute": bool(historical),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

        n += 1

    _register_timescale_trade_outcomes_after_commit(con, timescale_trade_outcome_rows)

    return {
        "ok": True,
        "attribution_written": int(n),
        "ts_ms": int(ts),
        "metrics_ts_ms": int(ts),
        "historical": bool(historical),
    }


def compute_pnl_attribution_snapshot(lookback_orders: int = 500) -> Dict[str, Any]:
    """
    Groups execution_metrics (latest snapshot) by source_alert_id + symbol.

    Writes:
      - pnl_attribution (uses latest execution_metrics snapshot ts_ms)
      - capital_efficiency (old table; source_alert_id+symbol)
    """
    init_execution_ledger()
    def _write(con):
        r = con.execute("SELECT MAX(ts_ms) FROM execution_metrics").fetchone()
        mts = int(r[0]) if r and r[0] is not None else None
        if mts is None:
            return {"ok": False, "status": "no_execution_metrics"}
        return _recompute_pnl_attribution_snapshot(
            con,
            snapshot_ts_ms=int(mts),
            lookback_orders=int(lookback_orders),
            historical=False,
        )
    result = run_write_txn(
        _write,
        table="pnl_attribution",
        operation="compute_pnl_attribution_snapshot",
        context={"lookback_orders": int(lookback_orders)},
    )
    if bool(result.get("ok")) and _safe_int(result.get("attribution_written"), 0) > 0:
        try:
            from engine.metrics_engine import refresh_feedback_loop

            result["feedback_loop"] = refresh_feedback_loop(
                snapshot_ts_ms=_safe_int(result.get("ts_ms"), 0) or None,
            )
        except Exception as e:
            _warn_nonfatal(
                "EXECUTION_LEDGER_FEEDBACK_LOOP_FAILED",
                e,
                once_key="execution_feedback_loop",
                snapshot_ts_ms=_safe_int(result.get("ts_ms"), 0),
            )
    return result





def compute_capital_efficiency_snapshot(limit_orders: int = 5000) -> Dict[str, Any]:
    """
    Computes capital efficiency metrics at the latest execution_metrics snapshot:

      - capital_hours: notional * holding_hours
      - return_per_risk: pnl_net / notional
      - drawdown_contrib: min(0, pnl_net) / notional
      - efficiency_score: pnl_net / capital_hours

    Writes:
      - execution_capital_efficiency rows (order-level)
      - strategy_metrics window_days=0 (strategy-level aggregates) if available
    """
    init_execution_ledger()

    try:
        from engine.runtime.storage import init_db

        init_db()
    except Exception as e:
        _warn_nonfatal("EXECUTION_LEDGER_INIT_DB_FAILED", e, once_key="compute_cap_eff_init_db")

    def _write(con):
        r = con.execute("SELECT MAX(ts_ms) FROM execution_metrics").fetchone()
        mts = int(r[0]) if r and r[0] is not None else None
        if mts is None:
            return {"ok": False, "status": "no_execution_metrics"}

        attribution_rows = con.execute(
            """
            SELECT
              source_alert_id,
              model_id,
              model_version,
              symbol,
              COALESCE(realized_pnl, 0.0),
              COALESCE(unrealized_pnl, 0.0),
              COALESCE(fees, 0.0),
              extra_json
            FROM pnl_attribution
            WHERE ts_ms = ?
            """,
            (int(mts),),
        ).fetchall()
        pnl_by_key: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
        for sid, model_id, model_version, sym, realized_pnl, unrealized_pnl, fees_row, extra_json in attribution_rows or []:
            if sid is None:
                continue
            extra = _safe_json_dict(extra_json)
            slippage_cost = _safe_float(extra.get("slippage_cost"), 0.0)
            total_pnl = (
                float(realized_pnl or 0.0)
                + float(unrealized_pnl or 0.0)
                - float(fees_row or 0.0)
                - float(slippage_cost)
            )
            pnl_by_key[(int(sid), _normalize_model_id(model_id), str(sym or "").strip().upper())] = {
                "pnl_net": float(total_pnl),
                "model_version": (str(model_version).strip() if model_version not in (None, "") else None),
            }

        rows = con.execute(
            """
            SELECT o.client_order_id,
                   o.broker,
                   o.portfolio_orders_id,
                   o.source_alert_id,
                   o.model_id,
                   o.model_version,
                   o.symbol,
                   o.qty,
                   o.submit_ts_ms,
                   o.extra_json,
                   m.fill_vwap,
                   COALESCE(m.fees,0)
            FROM execution_orders o
            JOIN execution_metrics m
              ON m.client_order_id = o.client_order_id
            WHERE m.ts_ms = ?
            ORDER BY o.submit_ts_ms DESC
            LIMIT ?
            """,
            (int(mts), int(max(1, min(20000, int(limit_orders))))),
        ).fetchall()

        order_rows = []
        key_notional: Dict[Tuple[int, str, str], float] = {}
        wrote = 0
        agg: Dict[str, Dict[str, float]] = {}

        for (
            cid,
            broker,
            portfolio_orders_id,
            source_alert_id,
            model_id,
            model_version,
            sym,
            submit_qty,
            submit_ts_ms,
            extra_json,
            _fill_vwap,
            fees_m,
        ) in (rows or []):
            cid = str(cid)
            sym = str(sym or "").strip().upper()
            broker_s = str(broker) if broker is not None else None

            vwap, filled_qty_signed, fees_fills, _, _, _, _ = _vwap_for_client(con, cid)
            if vwap is None:
                continue

            vwap = float(vwap)
            filled_qty_signed = float(filled_qty_signed)

            notional = abs(float(filled_qty_signed) * float(vwap))
            key = None
            if source_alert_id is not None and sym:
                key = (int(source_alert_id), _normalize_model_id(model_id), sym)
                key_notional[key] = float(key_notional.get(key, 0.0)) + float(notional)
            order_rows.append(
                (
                    cid,
                    broker_s,
                    portfolio_orders_id,
                    source_alert_id,
                    _normalize_model_id(model_id),
                    (str(model_version).strip() if model_version not in (None, "") else None),
                    sym,
                    submit_ts_ms,
                    extra_json,
                    float(filled_qty_signed),
                    float(vwap),
                    float(fees_fills or 0.0),
                    (float(fees_m) if fees_m is not None else None),
                    float(notional),
                    key,
                )
            )

        for (
            cid,
            broker_s,
            portfolio_orders_id,
            source_alert_id,
            model_id,
            model_version,
            sym,
            submit_ts_ms,
            extra_json,
            filled_qty_signed,
            vwap,
            fees_fills,
            fees_m,
            notional,
            key,
        ) in order_rows:
            fees_total = float(fees_fills or 0.0)
            if fees_m is not None:
                try:
                    fees_total = max(fees_total, float(fees_m))
                except Exception as e:
                    _warn_nonfatal("EXECUTION_LEDGER_FEES_PARSE_FAILED", e, once_key=f"capital_eff_fees:{cid}", client_order_id=str(cid), raw_value=fees_m)
            holding_hours = 0.0
            try:
                holding_ms = max(0, int(mts) - int(submit_ts_ms or 0))
                holding_hours = float(holding_ms) / 3_600_000.0
            except Exception:
                holding_hours = 0.0

            capital_hours = float(notional) * float(holding_hours)
            pnl_net = -float(fees_total or 0.0)
            if key is not None and key in pnl_by_key:
                key_total_notional = float(key_notional.get(key) or 0.0)
                alloc_share = (
                    float(notional) / float(key_total_notional)
                    if key_total_notional > 1e-12
                    else 1.0
                )
                pnl_net = float((pnl_by_key.get(key) or {}).get("pnl_net") or 0.0) * float(alloc_share)
            return_per_risk = float(pnl_net) / max(1e-9, float(notional))
            dd_contrib = min(0.0, float(pnl_net)) / max(1e-9, float(notional))
            efficiency_score = float(pnl_net) / max(1e-9, float(capital_hours))
            strategy_name = _extract_strategy_name(extra_json)

            con.execute(
                """
                INSERT OR REPLACE INTO execution_capital_efficiency(
                  ts_ms,
                  client_order_id,
                  broker,
                  portfolio_orders_id,
                  source_alert_id,
                  model_id,
                  model_version,
                  strategy_name,
                  symbol,
                  submit_ts_ms,
                  filled_qty,
                  fill_vwap,
                  fees,
                  notional,
                  holding_hours,
                  capital_hours,
                  pnl_net,
                  return_per_risk,
                  drawdown_contrib,
                  efficiency_score,
                  extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(mts),
                    cid,
                    broker_s,
                    int(portfolio_orders_id) if portfolio_orders_id is not None else None,
                    int(source_alert_id) if source_alert_id is not None else None,
                    _normalize_model_id(model_id),
                    (str(model_version).strip() if model_version not in (None, "") else None),
                    str(strategy_name) if strategy_name else None,
                    sym,
                    int(submit_ts_ms) if submit_ts_ms is not None else None,
                    float(filled_qty_signed),
                    float(vwap),
                    float(fees_total),
                    float(notional),
                    float(holding_hours),
                    float(capital_hours),
                    float(pnl_net),
                    float(return_per_risk),
                    float(dd_contrib),
                    float(efficiency_score),
                    extra_json,
                ),
            )
            wrote += 1

            if strategy_name:
                cur = agg.get(str(strategy_name)) or {
                    "capital_hours": 0.0,
                    "pnl_net": 0.0,
                    "notional": 0.0,
                    "dd_sum": 0.0,
                    "n": 0.0,
                }
                cur["capital_hours"] += float(capital_hours)
                cur["pnl_net"] += float(pnl_net)
                cur["notional"] += float(notional)
                cur["dd_sum"] += float(min(0.0, pnl_net))
                cur["n"] += 1.0
                agg[str(strategy_name)] = cur

        wrote_strat = 0
        for sname, v in (agg or {}).items():
            cap_h = float(v.get("capital_hours") or 0.0)
            pnl = float(v.get("pnl_net") or 0.0)
            notional_sum = float(v.get("notional") or 0.0)
            dd_sum = float(v.get("dd_sum") or 0.0)
            n_orders = int(v.get("n") or 0.0)

            metrics = {
                "ts_ms": int(mts),
                "capital_hours": float(cap_h),
                "notional": float(notional_sum),
                "pnl_net": float(pnl),
                "return_per_risk_unit": float(pnl) / max(1e-9, float(notional_sum)),
                "drawdown_contribution": float(dd_sum) / max(1e-9, float(notional_sum)),
                "efficiency_score": float(pnl) / max(1e-9, float(cap_h)),
                "capital_efficiency": float(pnl) / max(1e-9, float(cap_h)),
                "return_per_capital_hour": float(pnl) / max(1e-9, float(cap_h)),
                "n_orders": int(n_orders),
            }

            try:
                con.execute(
                    """
                    INSERT INTO strategy_metrics(strategy_name, window_days, ts_ms, metrics_json)
                    VALUES(?,?,?,?)
                    ON CONFLICT(strategy_name, window_days) DO UPDATE SET
                      ts_ms=excluded.ts_ms,
                      metrics_json=excluded.metrics_json
                    """,
                    (
                        str(sname),
                        0,
                        int(mts),
                        json.dumps(metrics, separators=(",", ":"), sort_keys=True),
                    ),
                )
                wrote_strat += 1
            except Exception as e:
                _warn_nonfatal("EXECUTION_LEDGER_STRATEGY_METRICS_WRITE_FAILED", e, once_key=f"strategy_metrics:{sname}", strategy_name=str(sname), ts_ms=int(mts))

        return {
            "ok": True,
            "ts_ms": int(mts),
            "orders_written": int(wrote),
            "strategies_written": int(wrote_strat),
        }
    return run_write_txn(
        _write,
        table="execution_capital_efficiency",
        operation="compute_capital_efficiency_snapshot",
        context={"limit_orders": int(limit_orders)},
    )


def rebuild_historical_pnl_attribution(
    *,
    limit_snapshots: int = 200,
    max_snapshot_age_ms: Optional[int] = None,
    lookback_orders: int = 20000,
) -> Dict[str, Any]:
    init_execution_ledger()
    def _write(con):
        where = ""
        params = []
        if max_snapshot_age_ms is not None and int(max_snapshot_age_ms) > 0:
            cutoff_ts_ms = now_ms() - int(max_snapshot_age_ms)
            where = "WHERE ts_ms >= ?"
            params.append(int(cutoff_ts_ms))

        rows = con.execute(
            f"""
            SELECT ts_ms
            FROM (
              SELECT DISTINCT ts_ms FROM execution_metrics
              UNION
              SELECT DISTINCT ts_ms FROM pnl_attribution
            )
            {where}
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            tuple(params + [max(1, int(limit_snapshots))]),
        ).fetchall()

        snapshots = [int(r[0]) for r in rows or [] if r and r[0] is not None]
        rebuilt = 0
        rows_written = 0
        last_snapshot_ts_ms = 0

        for snapshot_ts_ms in sorted(snapshots):
            result = _recompute_pnl_attribution_snapshot(
                con,
                snapshot_ts_ms=int(snapshot_ts_ms),
                lookback_orders=int(lookback_orders),
                historical=True,
            )
            if not bool(result.get("ok")):
                continue
            rebuilt += 1
            rows_written += int(result.get("attribution_written") or 0)
            last_snapshot_ts_ms = max(
                last_snapshot_ts_ms, int(result.get("ts_ms") or 0)
            )

        return {
            "ok": True,
            "snapshots_rebuilt": int(rebuilt),
            "rows_written": int(rows_written),
            "last_snapshot_ts_ms": int(last_snapshot_ts_ms),
        }
    return run_write_txn(
        _write,
        table="pnl_attribution",
        operation="rebuild_historical_pnl_attribution",
        context={
            "limit_snapshots": int(limit_snapshots),
            "max_snapshot_age_ms": (int(max_snapshot_age_ms) if max_snapshot_age_ms is not None else None),
            "lookback_orders": int(lookback_orders),
        },
    )
