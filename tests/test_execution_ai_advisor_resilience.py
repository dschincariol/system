from __future__ import annotations

import importlib
import sqlite3
from unittest.mock import patch


def _reload_advisor():
    import engine.execution.execution_ai_advisor as advisor

    return importlib.reload(advisor)


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _HistoricalSnapshotConnection:
    def __init__(self, *, analytics_missing_on_query: bool = False) -> None:
        self.analytics_missing_on_query = bool(analytics_missing_on_query)
        self.closed = False

    def execute(self, sql: str, params=()):
        text = " ".join(str(sql).split()).lower()
        if "from execution_analytics" in text:
            if self.analytics_missing_on_query:
                raise sqlite3.OperationalError("no such table: execution_analytics")
            raise AssertionError("execution_analytics should be skipped when unavailable")
        if "from execution_fills" in text:
            return _Cursor([])
        raise AssertionError(f"unexpected SQL: {sql!r} params={params!r}")

    def close(self) -> None:
        self.closed = True


def test_safe_float_none_defaults_without_warning() -> None:
    advisor = _reload_advisor()

    with patch.object(advisor, "_warn_nonfatal") as warn_nonfatal:
        assert advisor._safe_float(None, 12.5) == 12.5

    warn_nonfatal.assert_not_called()


def test_missing_execution_analytics_table_falls_back_without_warning() -> None:
    advisor = _reload_advisor()
    con = _HistoricalSnapshotConnection()

    with patch.object(advisor, "connect_ro", return_value=con):
        with patch.object(advisor, "_table_exists", return_value=False):
            with patch.object(advisor, "_warn_nonfatal") as warn_nonfatal:
                snapshot = advisor._historical_execution_snapshot("AAPL", "sim")

    assert snapshot["sample_n"] == 0
    assert snapshot["source"] is None
    assert con.closed is True
    warn_nonfatal.assert_not_called()


def test_execution_analytics_missing_during_query_falls_back_without_warning() -> None:
    advisor = _reload_advisor()
    con = _HistoricalSnapshotConnection(analytics_missing_on_query=True)

    with patch.object(advisor, "connect_ro", return_value=con):
        with patch.object(advisor, "_table_exists", return_value=True):
            with patch.object(advisor, "_warn_nonfatal") as warn_nonfatal:
                snapshot = advisor._historical_execution_snapshot("AAPL", "sim")

    assert snapshot["sample_n"] == 0
    assert snapshot["source"] is None
    assert con.closed is True
    warn_nonfatal.assert_not_called()
