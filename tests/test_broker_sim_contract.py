from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_module():
    import engine.execution.broker_sim as broker_sim

    return importlib.reload(broker_sim)


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


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


class BrokerSimOverrideOrderPipelineTests(unittest.TestCase):
    ENV_KEYS = (
        "DB_PATH",
        "BROKER_START_CASH",
        "BROKER_LATENCY_SLEEP",
        "TS_TESTING",
        "TS_STORAGE_BACKEND",
    )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "broker_sim_override_pipeline.db"
        self._env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["BROKER_START_CASH"] = "100000"
        os.environ["BROKER_LATENCY_SLEEP"] = "0"
        os.environ["TS_TESTING"] = "1"
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        _reload_modules("engine.runtime.db_guard", "engine.runtime.storage")

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        finally:
            for key, value in self._env_backup.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
            self.tmp.cleanup()

    def _init_runtime(self):
        storage, broker_sim, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.broker_sim",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        broker_sim.init_broker_db()
        execution_ledger.init_execution_ledger()
        return storage, broker_sim

    def _seed_price(self, storage, *, ts_ms: int) -> None:
        con = storage.connect()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS prices (
                  ts_ms INTEGER NOT NULL,
                  symbol TEXT NOT NULL,
                  price REAL,
                  px REAL,
                  source TEXT,
                  PRIMARY KEY(symbol, ts_ms)
                )
                """
            )
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (int(ts_ms), "AAPL", 100.0, 100.0, "test"),
            )
            con.commit()
        finally:
            con.close()

    def test_override_orders_dry_run_preserves_preview_without_writes(self) -> None:
        storage, broker_sim = self._init_runtime()
        now_ms = int(time.time() * 1000)
        self._seed_price(storage, ts_ms=now_ms)

        orders = [{"source_order_id": 7001, "symbol": "AAPL", "to_side": "LONG", "qty": 1.0}]
        result = broker_sim.apply_new_portfolio_orders(
            dry_run=True,
            override_orders=[dict(orders[0])],
            override_order_id=77,
            override_ts_ms=now_ms,
        )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("status"), "dry_run_preview")
        self.assertEqual(result.get("broker"), "sim")
        self.assertEqual(result.get("order_id"), 77)
        self.assertEqual(result.get("orders"), orders)

        con = storage.connect(readonly=True)
        try:
            self.assertEqual(int(con.execute("SELECT COUNT(*) FROM broker_fills").fetchone()[0] or 0), 0)
            self.assertEqual(int(con.execute("SELECT COUNT(*) FROM broker_positions").fetchone()[0] or 0), 0)
            self.assertEqual(int(con.execute("SELECT COUNT(*) FROM broker_order_state").fetchone()[0] or 0), 0)
            self.assertEqual(int(con.execute("SELECT COUNT(*) FROM execution_orders").fetchone()[0] or 0), 0)
            self.assertEqual(int(con.execute("SELECT COUNT(*) FROM execution_fills").fetchone()[0] or 0), 0)
            last_applied = con.execute(
                "SELECT value FROM broker_meta WHERE key='last_portfolio_orders_id'"
            ).fetchone()
        finally:
            con.close()
        self.assertIsNone(last_applied)

    def test_null_cash_account_snapshot_repairs_to_start_cash(self) -> None:
        storage, broker_sim = self._init_runtime()
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO broker_account(id, cash, equity, updated_ts_ms)
                VALUES(1, NULL, 0, NULL)
                ON CONFLICT(id) DO UPDATE SET
                  cash=excluded.cash,
                  equity=excluded.equity,
                  updated_ts_ms=excluded.updated_ts_ms
                """
            )
            con.commit()

            account = broker_sim._read_account(con)
            self.assertEqual(float(account["cash"]), 100000.0)
            self.assertEqual(float(account["equity"]), 100000.0)
            self.assertGreater(int(account["updated_ts_ms"]), 0)
            con.commit()
        finally:
            con.close()

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT cash, equity, updated_ts_ms FROM broker_account WHERE id=1").fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(float(row[0]), 100000.0)
        self.assertEqual(float(row[1]), 100000.0)
        self.assertGreater(int(row[2]), 0)

    def test_override_orders_live_sim_persists_state_and_ledger_effects(self) -> None:
        storage, broker_sim = self._init_runtime()
        now_ms = int(time.time() * 1000)
        self._seed_price(storage, ts_ms=now_ms)

        orders = [
            {
                "source_order_id": 7002,
                "symbol": "AAPL",
                "to_side": "LONG",
                "qty": 1.0,
                "source_alert_id": 42,
                "event_id": 99,
                "horizon_s": 300,
                "model_id": "override-test",
            }
        ]
        with patch("engine.execution.kill_switch.execution_allowed", return_value=(True, None, None)):
            with patch.object(broker_sim, "get_execution_liquidity_snapshot", return_value={}):
                with patch.object(broker_sim, "_earnings_proximity_decay", return_value=0.0):
                    with patch.object(broker_sim, "_get_factor_feature_asof", return_value=0.0):
                        with patch.object(broker_sim, "_prime_broker_order_state_after_commit", return_value=None):
                            result = broker_sim.apply_new_portfolio_orders(
                                dry_run=False,
                                override_orders=[dict(orders[0])],
                                override_order_id=78,
                                override_ts_ms=now_ms,
                            )

        self.assertTrue(result.get("ok"), result)
        self.assertEqual(result.get("status"), "applied")
        self.assertEqual(result.get("broker"), "sim")
        self.assertEqual(result.get("order_id"), 78)
        self.assertGreaterEqual(int(result.get("fills_written") or 0), 1)

        con = storage.connect(readonly=True)
        try:
            position = con.execute("SELECT qty FROM broker_positions WHERE symbol='AAPL'").fetchone()
            fill_count = int(con.execute("SELECT COUNT(*) FROM broker_fills WHERE symbol='AAPL'").fetchone()[0] or 0)
            order_state = con.execute(
                "SELECT state FROM broker_order_state WHERE source_order_id=7002 AND symbol='AAPL'"
            ).fetchone()
            execution_orders = int(
                con.execute("SELECT COUNT(*) FROM execution_orders WHERE symbol='AAPL'").fetchone()[0] or 0
            )
            execution_fills = int(
                con.execute("SELECT COUNT(*) FROM execution_fills WHERE symbol='AAPL'").fetchone()[0] or 0
            )
            last_applied = con.execute(
                "SELECT value FROM broker_meta WHERE key='last_portfolio_orders_id'"
            ).fetchone()
            account_columns = {str(row[1]) for row in con.execute("PRAGMA table_info(broker_account)").fetchall()}
            if "id" in account_columns:
                cash = float(con.execute("SELECT cash FROM broker_account WHERE id=1").fetchone()[0])
            else:
                cash = float(
                    con.execute(
                        """
                        SELECT cash
                        FROM broker_account
                        ORDER BY COALESCE(updated_ts_ms, ts_ms, 0) DESC, ts_ms DESC
                        LIMIT 1
                        """
                    ).fetchone()[0]
                )
        finally:
            con.close()

        self.assertIsNotNone(position)
        self.assertAlmostEqual(float(position[0]), 1.0, places=6)
        self.assertGreaterEqual(fill_count, 1)
        self.assertEqual(tuple(order_state or ()), ("FILLED",))
        self.assertEqual(execution_orders, 1)
        self.assertGreaterEqual(execution_fills, 1)
        self.assertEqual(tuple(last_applied or ()), ("78",))
        self.assertLess(cash, 100000.0)


if __name__ == "__main__":
    unittest.main()
