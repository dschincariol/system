from __future__ import annotations

import importlib
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.execution.broker_sim as broker_sim

    return importlib.reload(broker_sim)


class BrokerSimContractTests(unittest.TestCase):
    def test_init_broker_db_skips_write_txn_when_schema_is_ready(self) -> None:
        broker_sim = _reload_module()

        class _FakeConnection:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        fake_con = _FakeConnection()
        with patch.object(broker_sim, "connect", return_value=fake_con):
            with patch.object(broker_sim, "_broker_schema_ready", return_value=True):
                with patch.object(broker_sim, "run_write_txn", side_effect=AssertionError("write txn should be skipped")):
                    broker_sim.init_broker_db()

        self.assertTrue(fake_con.closed)

    def test_init_broker_db_uses_retrying_direct_write_txn_when_schema_missing(self) -> None:
        broker_sim = _reload_module()

        class _FakeConnection:
            def close(self) -> None:
                return None

        with patch.object(broker_sim, "connect", return_value=_FakeConnection()):
            with patch.object(broker_sim, "_broker_schema_ready", return_value=False):
                with patch.object(broker_sim, "run_write_txn") as run_write_txn:
                    broker_sim.init_broker_db()

        run_write_txn.assert_called_once_with(
            broker_sim._ensure_tables,
            table="broker_account",
            operation="init_broker_db",
            direct=True,
        )

    def test_mark_to_market_best_effort_returns_snapshot_when_persist_is_locked(self) -> None:
        broker_sim = _reload_module()

        class _FakeCursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchall(self):
                return list(self._rows)

        class _FakeConnection:
            def execute(self, sql, params=()):
                text = " ".join(str(sql).split()).lower()
                if "select symbol, qty from broker_positions" in text:
                    return _FakeCursor([("AAPL", 2.0)])
                raise AssertionError(f"unexpected query: {sql!r} params={params!r}")

        with patch.object(
            broker_sim,
            "_read_account",
            return_value={"cash": 100.0, "equity": 100.0, "updated_ts_ms": 0},
        ):
            with patch.object(broker_sim, "_get_price_at_or_before", return_value=(50.0, 1234)):
                with patch.object(
                    broker_sim,
                    "_persist_account_snapshot",
                    side_effect=sqlite3.OperationalError("database is locked"),
                ):
                    with patch.object(broker_sim, "_warn_nonfatal") as warn_nonfatal:
                        result = broker_sim._mark_to_market(_FakeConnection(), 1234, best_effort=True)

        self.assertEqual(result["cash"], 100.0)
        self.assertEqual(result["equity"], 200.0)
        self.assertEqual(result["storage_status"], "best_effort_deferred_lock_contention")
        warn_nonfatal.assert_called_once()

    def test_apply_new_portfolio_orders_no_orders_uses_best_effort_mark_to_market(self) -> None:
        broker_sim = _reload_module()

        class _FakeConnection:
            def close(self) -> None:
                return None

        fake_con = _FakeConnection()
        with patch.object(broker_sim, "init_broker_db"):
            with patch.object(broker_sim, "connect", return_value=fake_con):
                with patch("engine.strategy.portfolio_execution_intents.load_latest_execution_intents", return_value={}):
                    with patch.object(broker_sim, "_now_ms", return_value=1234):
                        with patch.object(
                            broker_sim,
                            "_mark_to_market",
                            return_value={
                                "cash": 1.0,
                                "equity": 2.0,
                                "updated_ts_ms": 1234,
                                "storage_status": "best_effort_deferred_lock_contention",
                            },
                        ) as mark_to_market:
                            result = broker_sim.apply_new_portfolio_orders()

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "no_orders")
        mark_to_market.assert_called_once_with(fake_con, 1234, book_key=None, best_effort=True)


if __name__ == "__main__":
    unittest.main()
