from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

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


class AuditFixRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "audit_fix_regressions.db")
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_fill_cost_components_preserve_favorable_slippage_sign(self) -> None:
        (execution_ledger,) = _reload_modules("engine.execution.execution_ledger")

        total_cost = execution_ledger._fill_cost_from_components(
            qty_signed=10.0,
            fill_px=100.0,
            fees=0.10,
            slippage_bps=-5.0,
        )

        self.assertAlmostEqual(float(total_cost), -0.40, places=6)

    def test_position_fill_state_carries_negative_opening_cost_into_net_realized(self) -> None:
        (execution_ledger,) = _reload_modules("engine.execution.execution_ledger")
        state = {}

        execution_ledger._apply_position_fill_state(
            state,
            qty_signed=10.0,
            fill_px=100.0,
            fill_cost=-0.50,
            client_order_id="open-order",
            fill_ts_ms=1,
        )
        close_result = execution_ledger._apply_position_fill_state(
            state,
            qty_signed=-10.0,
            fill_px=101.0,
            fill_cost=0.0,
            client_order_id="close-order",
            fill_ts_ms=2,
        )

        self.assertAlmostEqual(float(close_result["gross_realized_pnl"]), 10.0, places=6)
        self.assertAlmostEqual(float(close_result["net_realized_pnl"]), 10.5, places=6)

    def test_recompute_snapshot_treats_favorable_slippage_as_negative_cost(self) -> None:
        storage, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        execution_ledger.init_execution_ledger()

        now_ms = 1_760_000_000_000
        con = storage.connect()
        try:
            con.execute(
                "INSERT INTO prices(ts_ms, symbol, price, px, source) VALUES (?,?,?,?,?)",
                (int(now_ms), "AAPL", 110.0, 110.0, "test"),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, portfolio_orders_id, source_alert_id, model_id,
                  model_version, symbol, qty, submit_ts_ms, ref_px, expected_px, mid_px,
                  bid_px, ask_px, spread_bps, broker_order_id, status, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "oid-favorable",
                    "paper",
                    None,
                    101,
                    "m1",
                    None,
                    "AAPL",
                    10.0,
                    int(now_ms),
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    None,
                    None,
                    "submitted",
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, model_version, symbol,
                  ts_ms, submit_ts_ms, fill_ts_ms, fill_qty, fill_px, expected_px, mid_px,
                  bid_px, ask_px, spread_bps, slippage_bps, fill_latency_ms, fees,
                  commission, liquidity, raw_json, extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "oid-favorable",
                    "fill-favorable",
                    "paper",
                    "m1",
                    None,
                    "AAPL",
                    int(now_ms),
                    int(now_ms),
                    int(now_ms),
                    10.0,
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    None,
                    -10.0,
                    0,
                    5.0,
                    5.0,
                    "maker",
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_metrics(
                  ts_ms, client_order_id, broker, symbol, submit_qty, filled_qty,
                  ref_px, expected_px, mid_px, fill_px, fill_vwap, spread_bps,
                  slippage_bps, fill_latency_ms, fees, m2m_pnl, last_px
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "oid-favorable",
                    "paper",
                    "AAPL",
                    10.0,
                    10.0,
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    100.0,
                    None,
                    -10.0,
                    0,
                    5.0,
                    100.0,
                    110.0,
                ),
            )
            con.commit()
        finally:
            con.close()

        con = storage.connect()
        try:
            result = execution_ledger._recompute_pnl_attribution_snapshot(
                con,
                snapshot_ts_ms=int(now_ms),
                lookback_orders=100,
                historical=False,
            )
            con.commit()
        finally:
            con.close()

        self.assertTrue(bool(result.get("ok")), result)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT pnl, realized_pnl, unrealized_pnl, fees, extra_json
                FROM pnl_attribution
                ORDER BY ts_ms DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        extra = json.loads(row[4] or "{}")
        self.assertAlmostEqual(float(extra.get("slippage_cost") or 0.0), -1.0, places=6)
        self.assertAlmostEqual(float(row[0] or 0.0), 96.0, places=6)
        self.assertAlmostEqual(
            float(row[0] or 0.0),
            float(row[1] or 0.0) + float(row[2] or 0.0) - float(row[3] or 0.0) - float(extra.get("slippage_cost") or 0.0),
            places=6,
        )

    def test_validate_model_feature_snapshots_raises_on_lookahead(self) -> None:
        (feature_snapshots,) = _reload_modules("engine.strategy.model_feature_snapshots")

        with self.assertRaisesRegex(ValueError, "lookahead_detected"):
            feature_snapshots.validate_model_feature_snapshots_or_raise(
                [
                    {
                        "symbol": "AAPL",
                        "ts_ms": 1_700_000_000_000,
                        "availability": {"price": True},
                        "source_timestamps": {
                            "price": {
                                "quote_ts_ms": 1_700_000_000_001,
                            }
                        },
                    }
                ],
                context="unit_test_snapshots",
            )

    def test_event_bus_overflow_tracks_drop_details_and_logs(self) -> None:
        (event_bus,) = _reload_modules("engine.runtime.event_bus")
        bus = event_bus.EventBus(max_queue_size=32, handler_workers=1)

        with patch.object(event_bus, "_warn_nonfatal") as warn_nonfatal:
            for idx in range(33):
                event_type = "alpha" if idx == 0 else f"evt-{idx}"
                bus.publish({"type": event_type, "ts_ms": idx + 1})

        stats = bus.get_stats()
        self.assertEqual(int(stats["dropped_count"]), 1)
        self.assertEqual(int(stats["normal_dropped_count"]), 1)
        self.assertEqual(str(stats["last_dropped_event_type"]), "alpha")
        self.assertEqual(int(stats["queue_size"]), 32)
        warn_nonfatal.assert_called_once()

    def test_event_bus_keeps_critical_events_out_of_normal_drop_path(self) -> None:
        prev_critical_queue_size = os.environ.get("EVENT_BUS_CRITICAL_QUEUE_MAX_SIZE")
        os.environ["EVENT_BUS_CRITICAL_QUEUE_MAX_SIZE"] = "1"
        try:
            (event_bus,) = _reload_modules("engine.runtime.event_bus")
            bus = event_bus.EventBus(max_queue_size=32, handler_workers=1)
            handled: list[int] = []

            bus.subscribe("execution_update", lambda event: handled.append(int((event.get("payload") or {}).get("seq") or 0)))
            for seq in range(1, 9):
                bus.publish({"type": "execution_update", "payload": {"seq": seq}, "ts_ms": seq})
            for idx in range(33):
                event_type = "alpha" if idx == 0 else f"evt-{idx}"
                bus.publish({"type": event_type, "payload": {"seq": idx}, "ts_ms": idx + 100})
            bus.publish({"type": "execution_update", "payload": {"seq": 9}, "ts_ms": 9})

            stats = bus.get_stats()
        finally:
            if prev_critical_queue_size is None:
                os.environ.pop("EVENT_BUS_CRITICAL_QUEUE_MAX_SIZE", None)
            else:
                os.environ["EVENT_BUS_CRITICAL_QUEUE_MAX_SIZE"] = prev_critical_queue_size

        self.assertEqual(int(stats["critical_queue_size"] or 0), 8)
        self.assertEqual(int(stats["normal_queue_size"] or 0), 32)
        self.assertEqual(int(stats["normal_dropped_count"] or 0), 1)
        self.assertEqual(int(stats["critical_inline_dispatch_count"] or 0), 1)
        self.assertEqual(str(stats["last_dropped_event_type"] or ""), "alpha")
        self.assertEqual(handled, [9])

    def test_event_bus_tracks_critical_handler_failures_separately(self) -> None:
        (event_bus,) = _reload_modules("engine.runtime.event_bus")
        bus = event_bus.EventBus(max_queue_size=8, handler_workers=1)

        def _boom(_event):
            raise RuntimeError("boom")

        bus._invoke_handler(_boom, {"type": "execution_fill", "_critical": True})
        stats = bus.get_stats()
        self.assertEqual(int(stats["handler_failures"] or 0), 1)
        self.assertEqual(int(stats["critical_handler_failures"] or 0), 1)
        self.assertEqual(str(stats["last_failed_event_type"] or ""), "execution_fill")

    def test_temporal_shadow_db_init_runs_schema_once_under_concurrency(self) -> None:
        (temporal_predictor,) = _reload_modules("engine.strategy.temporal_predictor")

        class _FakeConnection:
            def __init__(self) -> None:
                self.executescript_calls = 0
                self.execute_calls = 0
                self.commit_calls = 0
                self._lock = threading.Lock()

            def execute(self, _sql: str):
                with self._lock:
                    self.execute_calls += 1
                return None

            def executescript(self, _sql: str):
                with self._lock:
                    self.executescript_calls += 1
                return None

            def commit(self):
                with self._lock:
                    self.commit_calls += 1
                return None

        con = _FakeConnection()
        temporal_predictor._SHADOW_DB_READY = False
        threads = [
            threading.Thread(target=temporal_predictor.init_temporal_shadow_db, args=(con,))
            for _ in range(12)
        ]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2.0)
        finally:
            temporal_predictor._SHADOW_DB_READY = False

        self.assertEqual(int(con.executescript_calls), 1)
        self.assertEqual(int(con.commit_calls), 1)


if __name__ == "__main__":
    unittest.main()
