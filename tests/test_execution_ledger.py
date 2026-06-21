from __future__ import annotations

import ast
import inspect
import sqlite3
from unittest.mock import patch

from engine.execution import execution_ledger


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)


class _PrimaryKeyLookupFailureConnection:
    raw = object()

    def __init__(self) -> None:
        self._table_info_calls = 0

    def execute(self, sql, params=()):
        del params
        if "PRAGMA table_info" not in str(sql):
            raise AssertionError(f"unexpected sql: {sql!r}")
        self._table_info_calls += 1
        if self._table_info_calls == 1:
            return _Cursor([(0, "id", "INTEGER", 0, None, 1)])
        raise sqlite3.DatabaseError("catalog lookup failed")


def test_unique_key_primary_key_lookup_failure_records_degraded_health() -> None:
    con = _PrimaryKeyLookupFailureConnection()

    with patch.object(execution_ledger, "record_component_health") as health:
        with patch.object(execution_ledger, "_warn_nonfatal") as warn_nonfatal:
            result = execution_ledger._table_has_unique_key(con, "portfolio_orders", ("id",))

    assert result is False
    warn_nonfatal.assert_called_once()
    health.assert_called_once()
    args, kwargs = health.call_args
    assert args == ("execution_ledger",)
    assert kwargs["ok"] is False
    assert kwargs["status"] == "degraded"
    assert kwargs["detail"] == "unique_key_primary_key_lookup_failed"
    assert kwargs["extra"]["reason"] == "unique_key_primary_key_lookup_failed"
    assert kwargs["extra"]["table"] == "portfolio_orders"


def test_execution_ledger_has_single_table_exists_helper() -> None:
    tree = ast.parse(inspect.getsource(execution_ledger))
    helpers = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_table_exists"
    ]

    assert len(helpers) == 1


def test_audit_execution_integrity_public_output_shape_golden(monkeypatch) -> None:
    monkeypatch.setenv("EXEC_INTEGRITY_MISSING_FILL_STALE_MS", "111")
    monkeypatch.setenv("EXEC_INTEGRITY_PENDING_ORDER_RECONCILE_MS", "222")
    monkeypatch.setenv("EXEC_INTEGRITY_UNREALIZED_PRICE_MAX_AGE_MS", "333")

    con = sqlite3.connect(":memory:")
    try:
        con.executescript(execution_ledger.SCHEMA)
        con.execute(
            """
            INSERT INTO execution_orders(
              client_order_id, order_uid, broker, source_alert_id, model_id,
              symbol, qty, submit_ts_ms, ref_px, status, extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("cid-1", "uid-1", "sim", 101, "m1", "AAPL", 10.0, 1000, 100.0, "filled", "{}"),
        )
        con.execute(
            """
            INSERT INTO execution_fills(
              client_order_id, fill_id, broker, model_id, symbol, source_alert_id,
              ts_ms, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, fees, extra_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            ("cid-1", "fill-1", "sim", "m1", "AAPL", 101, 1000, 1000, 1000, 10.0, 100.0, 0.25, "{}"),
        )
        con.execute(
            """
            INSERT INTO model_position_state(
              model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms
            )
            VALUES (?,?,?,?,?,?)
            """,
            ("m1", "AAPL", 10.0, 100.0, 0.0, 1000),
        )
        con.commit()

        audit = execution_ledger.audit_execution_integrity(model_id="m1", con=con)
    finally:
        con.close()

    assert audit == {
        "ok": True,
        "model_id": "m1",
        "duplicate_order_count": 0,
        "duplicate_fill_count": 0,
        "missing_fill_count": 0,
        "stale_missing_fill_count": 0,
        "fills_without_order_count": 0,
        "unreconciled_order_reference_count": 0,
        "submission_unrecorded_count": 0,
        "out_of_order_fill_count": 0,
        "inconsistent_position_count": 0,
        "pricing_unavailable_count": 1,
        "stale_missing_fill_threshold_ms": 111,
        "pending_order_reconcile_threshold_ms": 222,
        "unrealized_price_max_age_ms": 333,
        "duplicate_orders": [],
        "duplicate_fills": [],
        "missing_fills": [],
        "stale_missing_fills": [],
        "fills_without_order": [],
        "unreconciled_order_references": [],
        "submission_unrecorded": [],
        "out_of_order_fills": [],
        "position_mismatches": [],
        "pricing_unavailable_positions": [
            {
                "model_id": "m1",
                "symbol": "AAPL",
                "net_qty": 10.0,
                "last_update_ts_ms": 1000,
                "detail": "prices_table_missing",
            }
        ],
    }
