from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.execution.execution_ledger as execution_ledger

    return importlib.reload(execution_ledger)


class ExecutionLedgerInitContractTests(unittest.TestCase):
    def test_init_execution_ledger_skips_write_txn_when_schema_marker_is_ready(self) -> None:
        execution_ledger = _reload_module()
        with patch.object(execution_ledger, "_execution_ledger_schema_marker_ready", return_value=True):
            with patch.object(execution_ledger, "run_write_txn", side_effect=AssertionError("write txn should be skipped")):
                with patch.object(execution_ledger, "_invalidate_execution_ledger_caches") as invalidate:
                    execution_ledger.init_execution_ledger()

        invalidate.assert_called_once_with()

    def test_init_execution_ledger_bootstraps_with_dedicated_connection_when_marker_missing(self) -> None:
        execution_ledger = _reload_module()

        class _FakeConnection:
            def __init__(self) -> None:
                self.commit_calls = 0
                self.close_calls = 0

            def commit(self):
                self.commit_calls += 1

            def close(self):
                self.close_calls += 1

            def rollback(self):
                raise AssertionError("rollback should not be called")

        fake_con = _FakeConnection()
        with patch.object(execution_ledger, "_execution_ledger_schema_marker_ready", return_value=False):
            with patch.object(execution_ledger, "_invalidate_execution_ledger_caches") as invalidate:
                with patch.object(execution_ledger, "connect_rw_direct", return_value=fake_con):
                    with patch.object(execution_ledger, "_init_execution_ledger_schema") as init_schema:
                        execution_ledger.init_execution_ledger()

        init_schema.assert_called_once_with(fake_con)
        self.assertEqual(fake_con.commit_calls, 1)
        self.assertEqual(fake_con.close_calls, 1)
        invalidate.assert_called_once_with()

    def test_marker_ready_requires_meta_marker_and_schema_probe(self) -> None:
        execution_ledger = _reload_module()

        class _FakeConnection:
            def __init__(self) -> None:
                self.closed = False

            def execute(self, sql, params=()):
                text = " ".join(str(sql).split()).lower()
                if "where type='table'" in text:
                    return _FakeCursor([(1,)])
                if "where type='index'" in text:
                    return _FakeCursor([(1,)])
                raise AssertionError(f"unexpected query: {sql!r}")

            def close(self):
                self.closed = True

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

        fake_con = _FakeConnection()
        with patch("engine.runtime.runtime_meta.meta_get", return_value=execution_ledger._EXECUTION_LEDGER_SCHEMA_MARKER_VALUE):
            with patch.object(execution_ledger, "connect", return_value=fake_con):
                self.assertTrue(execution_ledger._execution_ledger_schema_marker_ready())

        self.assertTrue(fake_con.closed)

    def test_mark_execution_ledger_schema_ready_writes_via_existing_connection(self) -> None:
        execution_ledger = _reload_module()

        class _FakeConnection:
            def __init__(self) -> None:
                self.calls = []

            def execute(self, sql, params=()):
                self.calls.append((sql, params))
                return None

        fake_con = _FakeConnection()
        execution_ledger._mark_execution_ledger_schema_ready(fake_con)

        self.assertEqual(len(fake_con.calls), 1)
        _sql, params = fake_con.calls[0]
        self.assertEqual(
            params[:2],
            (
                execution_ledger._EXECUTION_LEDGER_SCHEMA_MARKER_KEY,
                execution_ledger._EXECUTION_LEDGER_SCHEMA_MARKER_VALUE,
            ),
        )


if __name__ == "__main__":
    unittest.main()
