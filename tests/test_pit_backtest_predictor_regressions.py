from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.requires_postgres


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class PortfolioBacktestPointInTimeRegressionTests(unittest.TestCase):
    ENV_KEYS = (
        "DB_PATH",
        "ENGINE_SUPERVISED",
        "BT_DAYS",
        "BT_LOOKBACK_S",
        "BT_START_EQUITY",
    )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "portfolio_backtest_pit.db"
        self._env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["ENGINE_SUPERVISED"] = "1"
        self.storage, self.portfolio_backtest = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio_backtest",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)
        self.tmp.cleanup()

    def test_price_lookup_does_not_fallback_to_future_prices(self) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (2_000, "AAPL", 101.0, 101.0, "test"),
            )
            con.commit()
        finally:
            con.close()

        con = self.storage.connect(readonly=True)
        try:
            px = self.portfolio_backtest._price_at_or_before(con, "AAPL", 1_000)
        finally:
            con.close()

        self.assertIsNone(px)

    def test_cost_bps_uses_trade_notional_for_fees(self) -> None:
        trade = {
            "qty": 10.0,
            "fees_total": 5.0,
        }
        result = self.portfolio_backtest._cost_bps_from_trade(trade, 100.0, 101.0, 1)
        self.assertAlmostEqual(float(result.get("fees_bps") or 0.0), 50.0, places=6)

    def test_run_backtest_uses_full_window_and_chronological_alert_order(self) -> None:
        fixed_now_ms = 1_710_000_100_000
        older_ts_ms = int(fixed_now_ms - (5 * 86400 * 1000))
        recent_ts_ms = int(fixed_now_ms - 30_000)

        os.environ["BT_DAYS"] = "10"
        os.environ["BT_LOOKBACK_S"] = "60"
        os.environ["BT_START_EQUITY"] = "100.0"

        con = self.storage.connect()
        try:
            for ts_ms, dedupe_key in (
                (older_ts_ms, "older-alert"),
                (recent_ts_ms, "recent-alert"),
            ):
                con.execute(
                    """
                    INSERT INTO alerts(
                      ts_ms, event_title, symbol, horizon_s, expected_z, confidence,
                      severity, rule_id, explain_json, dedupe_key
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(ts_ms),
                        str(dedupe_key),
                        "AAPL",
                        300,
                        1.0,
                        0.8,
                        "medium",
                        "unit_test",
                        "{}",
                        str(dedupe_key),
                    ),
                )
            con.commit()
        finally:
            con.close()

        seen_ts: list[int] = []

        def _record_targets(con, now_ms, lookback_s):
            seen_ts.append(int(now_ms))
            return []

        with patch.object(self.portfolio_backtest, "_now_ms", return_value=int(fixed_now_ms)):
            with patch.object(self.portfolio_backtest, "init_portfolio_db", return_value=None):
                with patch.object(self.portfolio_backtest, "_targets_from_recent_alerts", side_effect=_record_targets):
                    with patch.object(self.portfolio_backtest, "apply_execution_policy", return_value=[]):
                        result = self.portfolio_backtest.run_backtest()

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(seen_ts, [int(older_ts_ms), int(recent_ts_ms)])


class PredictorPointInTimeRegressionTests(unittest.TestCase):
    ENV_KEYS = (
        "DB_PATH",
        "ENGINE_SUPERVISED",
    )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "predictor_pit.db"
        self._env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["ENGINE_SUPERVISED"] = "1"
        self.storage, self.predictor = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.predictor",
        )
        self.storage.init_db()

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)
        self.tmp.cleanup()

    def _insert_event_embedding(self, event_id: int, ts_ms: int, vec: np.ndarray, *, symbol: str) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO events(
                  id, ts_ms, timestamp, event_type, symbol, source, title, body, url,
                  importance_score, raw_payload, derived_features, meta_json,
                  source_id, dedupe_hash, event_key
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(event_id),
                    int(ts_ms),
                    int(ts_ms),
                    "test_event",
                    str(symbol),
                    "test",
                    f"title-{event_id}",
                    "body",
                    None,
                    0.1,
                    "{}",
                    "{}",
                    "{}",
                    f"source-{event_id}",
                    f"hash-{event_id}",
                    f"event-key-{event_id}",
                ),
            )
            con.execute(
                "INSERT INTO event_embeddings(event_id, dim, vec) VALUES (?,?,?)",
                (int(event_id), int(vec.size), vec.astype(np.float32, copy=False).tobytes()),
            )
            con.commit()
        finally:
            con.close()

    def _insert_label(
        self,
        *,
        event_id: int,
        symbol: str,
        horizon_s: int,
        impact_z: float,
        created_at_ms: int,
    ) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO labels(
                  event_id, symbol, horizon_s, baseline_ret, realized_ret,
                  impact_z, created_at_ms, vol_proxy, regime
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(event_id),
                    str(symbol),
                    int(horizon_s),
                    0.0,
                    0.0,
                    float(impact_z),
                    int(created_at_ms),
                    1.0,
                    "MID",
                ),
            )
            con.commit()
        finally:
            con.close()

    def test_load_labeled_event_vectors_filters_labels_by_as_of_timestamp(self) -> None:
        self._insert_event_embedding(1, 1_000, np.asarray([1.0, 0.0], dtype=np.float32), symbol="AAPL")
        self._insert_event_embedding(2, 2_000, np.asarray([0.0, 1.0], dtype=np.float32), symbol="AAPL")
        self._insert_label(event_id=1, symbol="AAPL", horizon_s=300, impact_z=0.5, created_at_ms=1_500)
        self._insert_label(event_id=2, symbol="AAPL", horizon_s=300, impact_z=9.9, created_at_ms=4_500)

        events, labels = self.predictor.load_labeled_event_vectors(as_of_ts_ms=3_000)

        self.assertEqual(sorted(int(event_id) for event_id, _, _ in events), [1, 2])
        self.assertEqual(set(labels.keys()), {(1, "AAPL", 300)})

    def test_knn_raw_excludes_future_neighbors_for_requested_as_of(self) -> None:
        query_vec = np.asarray([1.0, 0.0], dtype=np.float32)
        events = [
            (1, 100, np.asarray([1.0, 0.0], dtype=np.float32)),
            (2, 300, np.asarray([1.0, 0.0], dtype=np.float32)),
        ]
        labels = {
            (1, "AAPL", 300): 0.5,
            (2, "AAPL", 300): 10.0,
        }
        vecs = np.stack([row[2] for row in events]).astype(np.float32, copy=False)
        self.predictor._cached["label_created_at"] = {
            (1, "AAPL", 300): 150,
            (2, "AAPL", 300): 350,
        }

        with patch.object(
            self.predictor,
            "_load_labeled_event_vectors_cached",
            return_value=(events, labels, vecs, [1, 2], [100, 300]),
        ):
            knn_z, _, explain = self.predictor._knn_raw(
                query_vec,
                "AAPL",
                300,
                4,
                as_of_ts_ms=200,
            )

        self.assertAlmostEqual(float(knn_z), 0.5, places=6)
        self.assertEqual(int(explain.get("used") or 0), 1)
        self.assertEqual(int((explain.get("neighbors") or [])[0].get("event_id") or 0), 1)


if __name__ == "__main__":
    unittest.main()
