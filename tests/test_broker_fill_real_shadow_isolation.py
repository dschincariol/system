from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class BrokerFillRealShadowIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "broker_fill_isolation.db"
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["BROKER_START_CASH"] = "100000"
        os.environ["TS_TESTING"] = "1"
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        _reload_modules("engine.runtime.db_guard", "engine.runtime.storage")

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        finally:
            self.tmp.cleanup()

    def test_get_realized_trade_excludes_sim_shadow_and_training_fills_at_same_timestamps(self) -> None:
        storage, broker_fill_utils = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.broker_fill_utils",
        )
        storage.init_db()

        entry_ts = int(time.time() * 1000)
        exit_ts = entry_ts + 120_000
        con = storage.connect()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS labels (id INTEGER PRIMARY KEY)")
            con.execute("DROP TABLE IF EXISTS broker_fills")
            con.execute(
                """
                CREATE TABLE broker_fills (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ts_ms INTEGER NOT NULL,
                  symbol TEXT NOT NULL,
                  price REAL NOT NULL,
                  qty REAL NOT NULL,
                  side TEXT NOT NULL,
                  fees REAL,
                  source TEXT NOT NULL DEFAULT 'real',
                  book_key TEXT,
                  explain_json TEXT
                )
                """
            )
            rows = [
                (entry_ts, "AAPL", 1.0, 10.0, "BUY", 9.0, "rl_shadow", "shadow:rl", {"shadow_book_key": "shadow:rl"}),
                (exit_ts, "AAPL", 999.0, -10.0, "SELL", 9.0, "rl_shadow", "shadow:rl", {"shadow_book_key": "shadow:rl"}),
                (entry_ts, "AAPL", 40.0, 10.0, "BUY", 2.0, "sim", None, {"source": "sim"}),
                (exit_ts, "AAPL", 70.0, -10.0, "SELL", 2.0, "sim", None, {"source": "sim"}),
                (entry_ts, "AAPL", 50.0, 10.0, "BUY", 3.0, "sim_training", None, {"source": "sim_training"}),
                (exit_ts, "AAPL", 60.0, -10.0, "SELL", 3.0, "sim_training", None, {"source": "sim_training"}),
                (entry_ts, "AAPL", 100.0, 10.0, "BUY", 0.10, "alpaca", None, {}),
                (exit_ts, "AAPL", 110.0, -10.0, "SELL", 0.20, "alpaca", None, {}),
            ]
            con.executemany(
                """
                INSERT INTO broker_fills(ts_ms, symbol, price, qty, side, fees, source, book_key, explain_json)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        int(ts_ms),
                        symbol,
                        float(price),
                        float(qty),
                        side,
                        float(fees),
                        source,
                        book_key,
                        json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    )
                    for ts_ms, symbol, price, qty, side, fees, source, book_key, explain in rows
                ],
            )
            con.commit()
        finally:
            con.close()

        trade = broker_fill_utils.get_realized_trade(
            symbol="AAPL",
            entry_ts_ms=entry_ts,
            exit_ts_ms=exit_ts,
        )

        self.assertIsNotNone(trade)
        assert trade is not None
        self.assertEqual(trade["side"], 1)
        self.assertAlmostEqual(float(trade["px_in"]), 100.0)
        self.assertAlmostEqual(float(trade["px_out"]), 110.0)
        self.assertAlmostEqual(float(trade["fees_total"]), 0.30)

    def test_shadow_broker_sim_fill_does_not_mirror_to_labels_or_execution_ledger(self) -> None:
        storage, broker_sim, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.broker_sim",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        execution_ledger.init_execution_ledger()

        now_ms = int(time.time() * 1000)
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
                (int(now_ms), "AAPL", 100.0, 100.0, "test"),
            )
            con.commit()
        finally:
            con.close()

        with patch("engine.execution.kill_switch.execution_allowed", return_value=(True, None, None)):
            result = broker_sim.apply_new_portfolio_orders(
                override_orders=[
                    {
                        "source_order_id": 501,
                        "symbol": "AAPL",
                        "to_side": "LONG",
                        "qty": 1.0,
                        "source_alert_id": 101,
                        "event_id": 44,
                        "horizon_s": 300,
                        "model_id": "rl_shadow_model",
                    }
                ],
                override_order_id=9001,
                override_ts_ms=now_ms,
                book_key="shadow:rl_shadow_model",
            )

        self.assertTrue(result.get("ok"), result)
        con = storage.connect(readonly=True)
        try:
            fill_row = con.execute(
                """
                SELECT source, book_key
                FROM broker_fills
                WHERE symbol='AAPL'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            execution_orders = con.execute("SELECT COUNT(*) FROM execution_orders").fetchone()[0]
            execution_fills = con.execute("SELECT COUNT(*) FROM execution_fills").fetchone()[0]
            placeholder_labels = con.execute(
                "SELECT COUNT(*) FROM labels_exec WHERE source='broker_sim_placeholder'"
            ).fetchone()[0]
        finally:
            con.close()

        self.assertEqual(tuple(fill_row or ()), ("shadow", "shadow:rl_shadow_model"))
        self.assertEqual(int(execution_orders or 0), 0)
        self.assertEqual(int(execution_fills or 0), 0)
        self.assertEqual(int(placeholder_labels or 0), 0)


if __name__ == "__main__":
    unittest.main()
