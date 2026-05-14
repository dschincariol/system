from __future__ import annotations

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
