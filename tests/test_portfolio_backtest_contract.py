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
    import engine.strategy.portfolio_backtest as portfolio_backtest

    return importlib.reload(portfolio_backtest)


class PortfolioBacktestContractTests(unittest.TestCase):
    def test_init_portfolio_backtest_schema_skips_write_txn_when_ready(self) -> None:
        portfolio_backtest = _reload_module()
        with patch.object(portfolio_backtest, "_portfolio_backtest_schema_ready", return_value=True):
            with patch.object(portfolio_backtest, "run_write_txn", side_effect=AssertionError("write txn should be skipped")):
                portfolio_backtest.init_portfolio_backtest_schema()

    def test_init_portfolio_backtest_schema_uses_retrying_direct_write_txn_when_missing(self) -> None:
        portfolio_backtest = _reload_module()
        with patch.object(portfolio_backtest, "_portfolio_backtest_schema_ready", return_value=False):
            with patch.object(portfolio_backtest, "run_write_txn") as run_write_txn:
                portfolio_backtest.init_portfolio_backtest_schema()

        run_write_txn.assert_called_once_with(
            portfolio_backtest._init_portfolio_backtest_schema,
            table="portfolio_bt_runs",
            operation="init_portfolio_backtest_schema",
            direct=True,
        )

    def test_persist_backtest_results_writes_run_and_curve_points(self) -> None:
        portfolio_backtest = _reload_module()

        class _FakeCursor:
            def __init__(self, lastrowid=None):
                self.lastrowid = lastrowid

        class _FakeConnection:
            def __init__(self) -> None:
                self.calls = []

            def execute(self, sql, params=()):
                self.calls.append((sql, params))
                text = " ".join(str(sql).split()).lower()
                if "insert into portfolio_bt_runs" in text:
                    return _FakeCursor(lastrowid=17)
                return _FakeCursor()

        fake_con = _FakeConnection()
        run_id = portfolio_backtest._persist_backtest_results(
            fake_con,
            now_ms=2000,
            start_ms=1000,
            curve=[
                (1100, 0.1, 1.1, 0.0, {"exec_cost": 0.01, "slippage": 0.02, "fees": 0.03}),
                (1200, -0.2, 0.9, 0.1, {"exec_cost": 0.04, "slippage": 0.05, "fees": 0.06}),
            ],
            metrics={"final_equity": 0.9},
        )

        point_inserts = [
            call
            for call in fake_con.calls
            if "insert or replace into portfolio_bt_points" in " ".join(str(call[0]).split()).lower()
        ]
        updates = [
            call
            for call in fake_con.calls
            if "update portfolio_bt_runs" in " ".join(str(call[0]).split()).lower()
        ]

        self.assertEqual(run_id, 17)
        self.assertEqual(len(point_inserts), 2)
        self.assertEqual(len(updates), 1)

    def test_run_backtest_skips_write_txn_when_no_curve_points_exist(self) -> None:
        portfolio_backtest = _reload_module()

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return list(self._rows)

        class _FakeConnection:
            def execute(self, sql, params=()):
                return _FakeCursor([])

            def close(self):
                return None

        with patch.object(portfolio_backtest, "init_portfolio_db"):
            with patch.object(portfolio_backtest, "init_portfolio_backtest_schema"):
                with patch.object(portfolio_backtest, "connect", return_value=_FakeConnection()):
                    with patch.object(portfolio_backtest, "run_write_txn", side_effect=AssertionError("write txn should be skipped")):
                        with patch.object(portfolio_backtest, "_now_ms", return_value=2_000):
                            result = portfolio_backtest.run_backtest()

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "no_points")
        self.assertIsNone(result["run_id"])


if __name__ == "__main__":
    unittest.main()
