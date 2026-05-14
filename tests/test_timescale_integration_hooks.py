from __future__ import annotations

import importlib
import os
import sys
import tempfile
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


class _FakeTimescaleClient:
    enabled = True

    def __init__(self) -> None:
        self.price_rows = []
        self.feature_rows = []
        self.prediction_rows = []
        self.trade_rows = []

    def enqueue_price_data(self, rows, *, timeout_s=None) -> int:
        batch = [dict(row) for row in (rows or [])]
        self.price_rows.extend(batch)
        return int(len(batch))

    def enqueue_feature_data(self, rows, *, timeout_s=None) -> int:
        batch = [dict(row) for row in (rows or [])]
        self.feature_rows.extend(batch)
        return int(len(batch))

    def enqueue_model_predictions(self, rows, *, timeout_s=None) -> int:
        batch = [dict(row) for row in (rows or [])]
        self.prediction_rows.extend(batch)
        return int(len(batch))

    def enqueue_trade_outcomes(self, rows, *, timeout_s=None) -> int:
        batch = [dict(row) for row in (rows or [])]
        self.trade_rows.extend(batch)
        return int(len(batch))


class TimescaleIntegrationHookTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "timescale_hooks.db")
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ.pop("TIMESCALE_ENABLED", None)
        (
            self.storage,
            self.validation,
            self.feature_snapshots,
            self.execution_ledger,
            self.poll_prices,
        ) = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.validation",
            "engine.strategy.model_feature_snapshots",
            "engine.execution.execution_ledger",
            "engine.data.poll_prices",
        )
        self.storage.init_db()
        self.execution_ledger.init_execution_ledger()

    def tearDown(self) -> None:
        try:
            self.storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_register_after_commit_runs_on_commit_and_clears_on_rollback(self) -> None:
        con = self.storage.connect_rw_direct()
        calls: list[str] = []
        try:
            con.execute("CREATE TABLE IF NOT EXISTS after_commit_probe(id INTEGER PRIMARY KEY)")
            con.commit()

            con.execute("INSERT INTO after_commit_probe(id) VALUES (1)")
            self.storage.register_after_commit(con, lambda: calls.append("commit"))
            self.assertEqual(calls, [])
            con.commit()
            self.assertEqual(calls, ["commit"])

            con.execute("INSERT INTO after_commit_probe(id) VALUES (2)")
            self.storage.register_after_commit(con, lambda: calls.append("rollback"))
            con.rollback()
            self.assertEqual(calls, ["commit"])
        finally:
            con.close()

    def test_store_prediction_enqueues_timescale_after_commit(self) -> None:
        con = self.storage.connect_rw_direct()
        client = _FakeTimescaleClient()
        try:
            with patch.object(self.validation, "get_timescale_client", return_value=client):
                self.validation.store_prediction(
                    101,
                    "SPY",
                    300,
                    1.25,
                    0.82,
                    model_id="model-alpha",
                    con=con,
                )
                self.assertEqual(client.prediction_rows, [])
                con.commit()
        finally:
            con.close()

        self.assertEqual(len(client.prediction_rows), 1)
        self.assertEqual(client.prediction_rows[0]["model_id"], "model-alpha")
        self.assertEqual(client.prediction_rows[0]["symbol"], "SPY")
        self.assertEqual(client.prediction_rows[0]["prediction"], 1.25)
        self.assertEqual(client.prediction_rows[0]["confidence"], 0.82)

    def test_store_model_feature_snapshots_enqueues_timescale_after_commit(self) -> None:
        con = self.storage.connect_rw_direct()
        client = _FakeTimescaleClient()
        snapshot = {
            "symbol": "SPY",
            "ts_ms": 1_700_000_000_000,
            "feature_set_tag": "unified_symbol_v1",
            "snapshot_version": 1,
            "feature_ids": ["f1"],
            "vector": [1.0],
            "features": {"f1": 1.0},
            "source_timestamps": {"price": {"quote_ts_ms": 1_700_000_000_000}},
            "availability": {"price": True},
        }
        try:
            with patch.object(self.feature_snapshots, "get_timescale_client", return_value=client):
                wrote = self.feature_snapshots.store_model_feature_snapshots([snapshot], con=con)
                self.assertEqual(wrote, 1)
                self.assertEqual(client.feature_rows, [])
                con.commit()
        finally:
            con.close()

        self.assertEqual(len(client.feature_rows), 1)
        self.assertEqual(client.feature_rows[0]["symbol"], "SPY")
        self.assertEqual(client.feature_rows[0]["timestamp"], 1_700_000_000_000)
        self.assertEqual(client.feature_rows[0]["feature_vector"]["features"], {"f1": 1.0})

    def test_trade_outcome_helper_enqueues_after_commit(self) -> None:
        con = self.storage.connect_rw_direct()
        client = _FakeTimescaleClient()
        rows = [
            {
                "trade_id": "cid-1",
                "timestamp": 1_700_000_100_000,
                "pnl": 12.5,
                "outcome": "win",
            }
        ]
        try:
            con.execute("CREATE TABLE IF NOT EXISTS trade_outcome_probe(id INTEGER PRIMARY KEY)")
            con.commit()
            con.execute("INSERT INTO trade_outcome_probe(id) VALUES (1)")
            with patch.object(self.execution_ledger, "get_timescale_client", return_value=client):
                self.execution_ledger._register_timescale_trade_outcomes_after_commit(con, rows)
                self.assertEqual(client.trade_rows, [])
                con.commit()
        finally:
            con.close()

        self.assertEqual(client.trade_rows, rows)
        self.assertEqual(self.execution_ledger._trade_outcome_label(5.0), "win")
        self.assertEqual(self.execution_ledger._trade_outcome_label(-1.0), "loss")
        self.assertEqual(self.execution_ledger._trade_outcome_label(0.0), "flat")

    def test_price_rows_enqueue_after_commit(self) -> None:
        con = self.storage.connect_rw_direct()
        client = _FakeTimescaleClient()
        price_rows = [(1_700_000_200_000, "SPY", 502.25)]
        quote_rows = [(1_700_000_200_000, "SPY", 502.25, 502.2, 502.3, 0.1, 1_250.0, "yfinance")]
        try:
            con.execute("CREATE TABLE IF NOT EXISTS price_probe(id INTEGER PRIMARY KEY)")
            con.commit()
            con.execute("INSERT INTO price_probe(id) VALUES (1)")
            with patch.object(self.poll_prices, "get_timescale_client", return_value=client):
                self.poll_prices._register_timescale_price_rows_after_commit(con, price_rows, quote_rows)
                self.assertEqual(client.price_rows, [])
                con.commit()
        finally:
            con.close()

        self.assertEqual(len(client.price_rows), 1)
        self.assertEqual(
            client.price_rows[0],
            {
                "symbol": "SPY",
                "timestamp": 1_700_000_200_000,
                "open": 502.25,
                "high": 502.25,
                "low": 502.25,
                "close": 502.25,
                "volume": 1250.0,
            },
        )

    def test_market_feature_store_enqueues_timescale_after_commit_when_sqlite_is_disabled(self) -> None:
        with patch.dict(os.environ, {"FEATURE_STORE_SQLITE_WRITE_ENABLED": "0"}, clear=False):
            storage, price_cache, feature_store = _reload_modules(
                "engine.runtime.storage",
                "engine.data.price_cache",
                "engine.data.feature_store",
            )
            storage.init_db()
            con = storage.connect_rw_direct()
            client = _FakeTimescaleClient()
            rows = [
                {
                    "symbol": "SPY",
                    "ts_ms": 1_700_000_300_000 + (idx * 60_000),
                    "price": 500.0 + idx,
                    "volume": 1_000.0 + (idx * 25.0),
                    "source": "unit_test",
                }
                for idx in range(25)
            ]
            snapshot = price_cache.snapshot_from_rows("SPY", rows)
            features = feature_store.compute_features("SPY", snapshot)
            try:
                con.execute("CREATE TABLE IF NOT EXISTS feature_after_commit_probe(id INTEGER PRIMARY KEY)")
                con.commit()
                con.execute("INSERT INTO feature_after_commit_probe(id) VALUES (1)")
                with patch.object(feature_store, "get_timescale_client", return_value=client):
                    stored = feature_store.store_features("SPY", features, con=con)
                    self.assertEqual(client.feature_rows, [])
                    self.assertEqual(int(stored["ts_ms"]), int(features["ts_ms"]))
                    con.commit()
            finally:
                con.close()

            con = storage.connect(readonly=True)
            try:
                row = con.execute(
                    "SELECT COUNT(*) FROM market_features WHERE symbol = ?",
                    ("SPY",),
                ).fetchone()
            finally:
                con.close()

        self.assertEqual(int(row[0] or 0), 0)
        self.assertEqual(len(client.feature_rows), 1)
        self.assertEqual(client.feature_rows[0]["symbol"], "SPY")
        self.assertEqual(client.feature_rows[0]["timestamp"], int(features["ts_ms"]))
        self.assertEqual(client.feature_rows[0]["feature_vector"]["features"], dict(features["features"]))


if __name__ == "__main__":
    unittest.main()
