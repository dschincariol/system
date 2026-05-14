"""Regression tests for trade lifecycle, prediction history, and fill-audit paths."""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from datetime import datetime, timezone
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


def _warn_cleanup_issue(scope: str, error: BaseException) -> None:
    sys.stderr.write(f"[{scope}] {type(error).__name__}: {error}\n")
    sys.stderr.flush()


class TradeLifecycleRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "trade_lifecycle_test.db"
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["BROKER_START_CASH"] = "100000"

        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
        )

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            _warn_cleanup_issue("test_trade_lifecycle_regressions.close_pooled_connections", e)
        try:
            self.tmp.cleanup()
        except PermissionError as e:
            _warn_cleanup_issue("test_trade_lifecycle_regressions.tempdir_cleanup", e)

    def _executescript(self, script: str) -> None:
        con = sqlite3.connect(str(self.db_path))
        try:
            con.executescript(script)
            con.commit()
        finally:
            con.close()

    def test_compute_model_metrics_filters_requested_model(self) -> None:
        storage, validation = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.validation",
        )
        storage.init_db()

        con = storage.connect()
        try:
            con.execute(
                "INSERT INTO labels(event_id, symbol, horizon_s, baseline_ret, realized_ret, impact_z, created_at_ms, vol_proxy, regime) VALUES (?,?,?,?,?,?,?,?,?)",
                (1, "AAPL", 300, None, 1.25, 1.25, 1, None, "global"),
            )
            con.execute(
                "INSERT INTO labels(event_id, symbol, horizon_s, baseline_ret, realized_ret, impact_z, created_at_ms, vol_proxy, regime) VALUES (?,?,?,?,?,?,?,?,?)",
                (2, "AAPL", 300, None, -0.75, -0.75, 2, None, "global"),
            )
            con.commit()
        finally:
            con.close()

        validation.store_prediction(1, "AAPL", 300, 1.0, 0.8, model_name="model_one")
        validation.store_prediction(2, "AAPL", 300, -1.0, 0.7, model_name="model_two")

        count = validation.compute_model_metrics(model_name="model_one")
        self.assertEqual(count, 1)

        rows = validation.get_model_metrics(model_name="model_one")
        self.assertEqual(len(rows), 1)
        self.assertEqual(int(rows[0]["n"]), 1)

    def test_store_prediction_keeps_append_only_history(self) -> None:
        storage, validation = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.validation",
        )
        storage.init_db()

        validation.store_prediction(7, "MSFT", 600, 0.5, 0.6, model_name="model_one", model_id="m1")
        validation.store_prediction(7, "MSFT", 600, 0.8, 0.9, model_name="model_one", model_id="m1")

        con = storage.connect(readonly=True)
        try:
            snap = con.execute(
                """
                SELECT predicted_z, confidence
                FROM predictions
                WHERE event_id=7 AND symbol='MSFT' AND horizon_s=600
                LIMIT 1
                """
            ).fetchone()
            hist = con.execute(
                """
                SELECT predicted_z, confidence
                FROM prediction_history
                WHERE event_id=7 AND symbol='MSFT' AND horizon_s=600
                ORDER BY ts_ms ASC, id ASC
                """
            ).fetchall()
        finally:
            con.close()

        self.assertIsNotNone(snap)
        self.assertEqual((float(snap[0]), float(snap[1])), (0.8, 0.9))
        self.assertEqual(len(hist), 2)
        self.assertEqual((float(hist[0][0]), float(hist[0][1])), (0.5, 0.6))
        self.assertEqual((float(hist[1][0]), float(hist[1][1])), (0.8, 0.9))

    def test_alert_and_checkpoint_can_share_transaction(self) -> None:
        storage, alerts = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.alerts",
        )
        storage.init_db()

        con = storage.connect()
        try:
            alerts.emit_alert(
                event_id=11,
                event_title="Test alert",
                symbol="AAPL",
                horizon_s=300,
                expected_z=1.2,
                confidence=0.9,
                explain={"model_name": "model_one", "model_id": "m1"},
                con=con,
            )
            storage.put_job_checkpoint("process_events", 11, 12345, con=con)
            con.rollback()
        finally:
            con.close()

        con = storage.connect(readonly=True)
        try:
            alert_n = int(con.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] or 0)
            checkpoint_n = int(
                con.execute("SELECT COUNT(*) FROM job_checkpoints WHERE job_name='process_events'").fetchone()[0] or 0
            )
        finally:
            con.close()

        self.assertEqual(alert_n, 0)
        self.assertEqual(checkpoint_n, 0)

    def test_fill_attribution_failure_event_respects_caller_transaction(self) -> None:
        storage, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        execution_ledger.init_execution_ledger()

        con = storage.connect()
        try:
            con.begin_managed_write()
            with patch.object(
                execution_ledger,
                "record_live_fill_attribution",
                side_effect=RuntimeError("attribution_boom"),
            ):
                with self.assertRaises(RuntimeError):
                    execution_ledger._apply_fill_attribution_side_effects(
                        con=con,
                        client_order_id="cid-1",
                        fill_qty=1.0,
                        fill_px=100.0,
                        fill_ts_ms=1234567890,
                        fees=0.1,
                        slippage_bps=2.5,
                        broker="sim",
                        fill_id="fill-1",
                        reason="test_failure",
                    )

            row = con.execute(
                "SELECT COUNT(*) FROM event_log WHERE event_type='fill_attribution_failed'"
            ).fetchone()
            self.assertEqual(int(row[0] or 0), 1)
            self.assertTrue(bool(con.in_transaction))
            con.rollback()
        finally:
            con.close()

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM event_log WHERE event_type='fill_attribution_failed'"
            ).fetchone()
            self.assertEqual(int(row[0] or 0), 0)
        finally:
            con.close()

    def test_parse_broker_timestamp_ms_handles_ibkr_and_iso_inputs(self) -> None:
        (broker_fill_utils,) = _reload_modules("engine.execution.broker_fill_utils")
        parse = broker_fill_utils.parse_broker_timestamp_ms

        ibkr_expected = int(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp() * 1000.0)
        iso_expected = int(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp() * 1000.0)

        self.assertEqual(parse("20260102  03:04:05"), ibkr_expected)
        self.assertEqual(parse("2026-01-02T03:04:05Z"), iso_expected)

    def test_broker_sim_labels_exec_are_marked_placeholder(self) -> None:
        storage, broker_sim = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.broker_sim",
        )
        storage.init_db()

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                "CREATE TABLE IF NOT EXISTS prices (ts_ms INTEGER NOT NULL, symbol TEXT NOT NULL, price REAL, px REAL, source TEXT, PRIMARY KEY(symbol, ts_ms))"
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
                        "symbol": "AAPL",
                        "to_side": "LONG",
                        "qty": 1.0,
                        "source_alert_id": 101,
                        "event_id": 44,
                        "horizon_s": 300,
                        "model_id": "m1",
                    }
                ],
                override_order_id=1,
                override_ts_ms=int(now_ms),
            )
        self.assertTrue(result.get("ok"), result)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT source, realized, extra_json
                FROM labels_exec
                WHERE event_id=44 AND symbol='AAPL' AND horizon_s=300
                LIMIT 1
                """
            ).fetchone()
            order_row = con.execute(
                """
                SELECT client_order_id, source_alert_id, model_id, symbol
                FROM execution_orders
                WHERE source_alert_id=101 AND model_id='m1' AND symbol='AAPL'
                LIMIT 1
                """
            ).fetchone()
            fill_row = con.execute(
                """
                SELECT client_order_id, model_id, symbol
                FROM execution_fills
                WHERE model_id='m1' AND symbol='AAPL'
                LIMIT 1
                """
            ).fetchone()
            fill_failure_count = int(
                con.execute(
                    """
                    SELECT COUNT(*)
                    FROM event_log
                    WHERE event_type='fill_attribution_failed'
                    """
                ).fetchone()[0]
                or 0
            )
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertIsNotNone(order_row)
        self.assertIsNotNone(fill_row)
        self.assertEqual(fill_failure_count, 0)
        extra = json.loads(row[2] or "{}")
        self.assertEqual(str(row[0]), "broker_sim_placeholder")
        self.assertEqual(int(row[1]), 0)
        self.assertTrue(bool(extra.get("placeholder_exec_label")))
        self.assertEqual(str(fill_row[0]), str(order_row[0]))

    def test_favorable_slippage_reduces_execution_cost(self) -> None:
        (execution_ledger,) = _reload_modules("engine.execution.execution_ledger")

        favorable_cost = execution_ledger._fill_cost_from_components(
            qty_signed=10.0,
            fill_px=100.0,
            fees=1.0,
            slippage_bps=-5.0,
        )
        adverse_cost = execution_ledger._fill_cost_from_components(
            qty_signed=10.0,
            fill_px=100.0,
            fees=1.0,
            slippage_bps=5.0,
        )

        self.assertAlmostEqual(favorable_cost, 0.5)
        self.assertAlmostEqual(adverse_cost, 1.5)

    def test_fill_before_submit_creates_reconcilable_placeholder_order(self) -> None:
        storage, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        execution_ledger.init_execution_ledger()

        execution_ledger.log_fill(
            client_order_id="cid-missing",
            fill_id="fill-1",
            broker="sim",
            symbol="AAPL",
            qty=1.0,
            fill_px=100.0,
            fill_ts_ms=2_000,
            fees=0.25,
            extra={"model_id": "m1"},
        )

        con = storage.connect(readonly=True)
        try:
            placeholder = con.execute(
                """
                SELECT order_uid, idempotency_status, status, extra_json
                FROM execution_orders
                WHERE client_order_id='cid-missing'
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(placeholder)
        self.assertEqual(str(placeholder[0] or ""), "")
        self.assertEqual(str(placeholder[1] or ""), "fill_before_submit")
        self.assertEqual(str(placeholder[2] or ""), "fill_pending_submit")
        placeholder_extra = json.loads(placeholder[3] or "{}")
        self.assertTrue(bool(placeholder_extra.get("missing_local_order_reference")))

        execution_ledger.log_submit(
            client_order_id="cid-missing",
            broker="sim",
            symbol="AAPL",
            qty=1.0,
            submit_ts_ms=1_500,
            order_uid="order-123",
            idempotency_status="submitted",
            extra={"model_id": "m1"},
        )

        con = storage.connect(readonly=True)
        try:
            reconciled = con.execute(
                """
                SELECT order_uid, idempotency_status, status
                FROM execution_orders
                WHERE client_order_id='cid-missing'
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(reconciled)
        self.assertEqual(str(reconciled[0] or ""), "order-123")
        self.assertEqual(str(reconciled[1] or ""), "submitted")
        self.assertEqual(str(reconciled[2] or ""), "submitted")

    def test_log_submit_backfills_fill_lineage_from_portfolio_order(self) -> None:
        storage, portfolio, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        self._executescript(portfolio.SCHEMA + execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            prediction_id = int(
                con.execute(
                    """
                    INSERT INTO predictions(
                      ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                      confidence_raw, prediction_strength, model_name, model_id, model_version
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (int(now_ms), 44, "AAPL", 300, 1.1, 0.82, 0.82, 0.91, "model_one", "m1", "v1"),
                ).lastrowid
                or 0
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    101,
                    int(now_ms),
                    44,
                    int(prediction_id),
                    "Lifecycle test",
                    "AAPL",
                    300,
                    1.1,
                    0.82,
                    "HIGH",
                    "high_z15_conf60",
                    json.dumps({"model_name": "model_one", "model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    "AAPL:300:high:test",
                    "model_one",
                    "m1",
                    "v1",
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side, from_weight,
                  to_weight, delta_weight, source_alert_id, prediction_id, explain_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "m1",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.25,
                    0.25,
                    101,
                    int(prediction_id),
                    json.dumps({"source": "test"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        execution_ledger.log_fill(
            client_order_id="cid-before-submit",
            fill_id="fill-1",
            broker="sim",
            symbol="AAPL",
            qty=1.0,
            fill_px=100.0,
            fill_ts_ms=int(now_ms),
            fees=0.25,
            extra={"model_id": "m1"},
        )
        execution_ledger.log_submit(
            client_order_id="cid-before-submit",
            broker="sim",
            symbol="AAPL",
            qty=1.0,
            submit_ts_ms=int(now_ms - 500),
            portfolio_orders_id=1,
            extra={"model_id": "m1"},
        )

        con = storage.connect(readonly=True)
        try:
            order_row = con.execute(
                """
                SELECT portfolio_orders_id, source_alert_id, prediction_id
                FROM execution_orders
                WHERE client_order_id='cid-before-submit'
                """
            ).fetchone()
            fill_row = con.execute(
                """
                SELECT portfolio_orders_id, source_alert_id, prediction_id
                FROM execution_fills
                WHERE client_order_id='cid-before-submit'
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(tuple(order_row or ()), (1, 101, prediction_id))
        self.assertEqual(tuple(fill_row or ()), (1, 101, prediction_id))

    def test_broker_sim_propagates_fees_to_execution_ledger(self) -> None:
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
            broker_sim.apply_new_portfolio_orders(
                override_orders=[
                    {
                        "symbol": "AAPL",
                        "to_side": "LONG",
                        "qty": 1.0,
                        "source_alert_id": 101,
                        "event_id": 44,
                        "horizon_s": 300,
                        "model_id": "m1",
                    }
                ]
            )

        con = storage.connect(readonly=True)
        try:
            fee_sum = float(
                con.execute(
                    "SELECT COALESCE(SUM(fees), 0.0) FROM execution_fills WHERE client_order_id IS NOT NULL"
                ).fetchone()[0]
                or 0.0
            )
        finally:
            con.close()

        self.assertGreater(fee_sum, 0.0)

    def test_execution_quality_supervisor_surfaces_execution_safety_gates(self) -> None:
        storage, execution_ledger, execution_quality_supervisor = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
            "engine.execution.execution_quality_supervisor",
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
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, order_uid, broker, source_alert_id, model_id, symbol, qty,
                  submit_ts_ms, ref_px, expected_px, mid_px, spread_bps, status, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("co-1", "ord-1", "sim", 101, "m1", "AAPL", 1.0, int(now_ms - 600_000), 100.0, 100.0, 100.0, 2.0, "submitted", json.dumps({})),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, source_alert_id, model_id, symbol, qty,
                  submit_ts_ms, ref_px, expected_px, mid_px, spread_bps, status, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                ("co-pos", "sim", 201, "m1", "MSFT", 1.0, int(now_ms - 10_000), 50.0, 50.0, 50.0, 2.0, "filled", json.dumps({})),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, symbol, ts_ms, submit_ts_ms, fill_ts_ms,
                  fill_qty, fill_px, expected_px, mid_px, spread_bps, slippage_bps, fill_latency_ms,
                  fees, commission, liquidity, raw_json, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "co-pos",
                    "fill-pos-1",
                    "sim",
                    "m1",
                    "MSFT",
                    int(now_ms - 5_000),
                    int(now_ms - 10_000),
                    int(now_ms - 5_000),
                    1.0,
                    50.0,
                    50.0,
                    50.0,
                    2.0,
                    0.0,
                    5_000,
                    0.0,
                    0.0,
                    "sim",
                    json.dumps({}),
                    json.dumps({}),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, symbol, ts_ms, submit_ts_ms, fill_ts_ms,
                  fill_qty, fill_px, expected_px, mid_px, spread_bps, slippage_bps, fill_latency_ms,
                  fees, commission, liquidity, raw_json, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "co-orphan",
                    "fill-orphan-1",
                    "sim",
                    "m1",
                    "AAPL",
                    int(now_ms - 2_000),
                    None,
                    int(now_ms - 2_000),
                    1.0,
                    100.0,
                    100.0,
                    100.0,
                    2.0,
                    0.0,
                    None,
                    0.0,
                    0.0,
                    "sim",
                    json.dumps({}),
                    json.dumps({}),
                ),
            )
            con.execute(
                """
                INSERT INTO model_position_state(model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms)
                VALUES (?,?,?,?,?,?)
                """,
                ("m1", "MSFT", 2.0, 50.0, 0.0, int(now_ms)),
            )
            con.commit()
        finally:
            con.close()

        snapshot = execution_quality_supervisor.refresh_execution_quality_supervisor()

        self.assertEqual(str(snapshot.get("state") or ""), "critical")
        self.assertIn("order_state_consistent", list(snapshot.get("failed_gates") or []))
        self.assertIn("position_state_consistent", list(snapshot.get("failed_gates") or []))
        self.assertIn("pnl_calculation_valid", list(snapshot.get("failed_gates") or []))
        integrity = dict(snapshot.get("integrity") or {})
        self.assertGreater(int(integrity.get("stale_missing_fill_count") or 0), 0)
        self.assertGreater(int(integrity.get("fills_without_order_count") or 0), 0)
        self.assertGreater(int(integrity.get("inconsistent_position_count") or 0), 0)
        self.assertGreater(int(integrity.get("pricing_unavailable_count") or 0), 0)

    def test_broker_sim_numeric_guards_and_missing_earnings_calendar_degrade_quietly(self) -> None:
        storage, broker_sim = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.broker_sim",
        )
        storage.init_db()

        con = storage.connect()
        try:
            with patch.object(broker_sim, "_warn_nonfatal") as warn_nonfatal:
                self.assertEqual(broker_sim._safe_f(None, 1.25), 1.25)
                self.assertEqual(broker_sim._safe_f("", 2.5), 2.5)
                self.assertEqual(broker_sim._safe_i(None, 7), 7)
                self.assertEqual(broker_sim._safe_i("   ", 9), 9)
                self.assertEqual(broker_sim._earnings_proximity_decay(con, "AAPL", int(time.time() * 1000)), 0.0)
            warn_nonfatal.assert_not_called()
        finally:
            con.close()

    def test_trace_trade_lifecycle_walks_full_chain(self) -> None:
        storage, portfolio, execution_ledger, trade_lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.execution.execution_ledger",
            "engine.runtime.trade_lifecycle",
        )
        storage.init_db()
        self._executescript(portfolio.SCHEMA + execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            prediction_id = int(
                con.execute(
                    """
                    INSERT INTO predictions(
                      ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                      confidence_raw, prediction_strength, model_name, model_id, model_version
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (int(now_ms), 44, "AAPL", 300, 1.1, 0.82, 0.82, 0.91, "model_one", "m1", "v1"),
                ).lastrowid
                or 0
            )
            con.execute(
                """
                INSERT INTO prediction_history(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(now_ms - 5), 44, "AAPL", 300, 0.9, 0.7, 0.7, 0.8, "model_one", "m1", "v0"),
            )
            con.execute(
                """
                INSERT INTO prediction_history(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), 44, "AAPL", 300, 1.1, 0.82, 0.82, 0.91, "model_one", "m1", "v1"),
            )
            con.execute(
                """
                INSERT INTO decision_log(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  model_name, model_kind, model_ts_ms, features_hash, features_json, explain_json, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    44,
                    "AAPL",
                    300,
                    1.1,
                    0.82,
                    "model_one",
                    "xgb",
                    int(now_ms),
                    "hash",
                    json.dumps({"f": 1}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"note": "decision"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    101,
                    int(now_ms),
                    44,
                    int(prediction_id),
                    "Lifecycle test",
                    "AAPL",
                    300,
                    1.1,
                    0.82,
                    "HIGH",
                    "high_z15_conf60",
                    json.dumps({"model_name": "model_one", "model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    "AAPL:300:high:test",
                    "model_one",
                    "m1",
                    "v1",
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side, from_weight,
                  to_weight, delta_weight, source_alert_id, prediction_id, explain_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "m1",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.25,
                    0.25,
                    101,
                    int(prediction_id),
                    json.dumps({"source": "test"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, portfolio_orders_id, source_alert_id, prediction_id, model_id,
                  model_version, symbol, qty, submit_ts_ms, ref_px, expected_px, mid_px,
                  bid_px, ask_px, spread_bps, broker_order_id, status, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "co1",
                    "paper",
                    1,
                    101,
                    int(prediction_id),
                    "m1",
                    "v1",
                    "AAPL",
                    10.0,
                    int(now_ms),
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    2.0,
                    "bo1",
                    "filled",
                    json.dumps({"model_name": "model_one"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, model_version, symbol,
                  portfolio_orders_id, source_alert_id, prediction_id, ts_ms, submit_ts_ms, fill_ts_ms,
                  fill_qty, fill_px, expected_px, mid_px, bid_px, ask_px, spread_bps, slippage_bps,
                  fill_latency_ms, fees, commission, liquidity, raw_json, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "co1",
                    "fill1",
                    "paper",
                    "m1",
                    "v1",
                    "AAPL",
                    1,
                    101,
                    int(prediction_id),
                    int(now_ms),
                    int(now_ms),
                    int(now_ms),
                    10.0,
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    2.0,
                    0.0,
                    0,
                    1.0,
                    1.0,
                    "maker",
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO model_position_state(model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms)
                VALUES (?,?,?,?,?,?)
                """,
                ("m1", "AAPL", 10.0, 100.0, 12.0, int(now_ms)),
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    101,
                    "m1",
                    "v1",
                    "AAPL",
                    25.0,
                    1.0,
                    0.0,
                    10.0,
                    100.0,
                    12.0,
                    14.0,
                    json.dumps({"total_pnl": 25.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO model_marketplace_scores(
                  model_id, model_name, symbol, horizon_s, regime, stage, score, trades, wins,
                  losses, gross_pnl, net_pnl, avg_confidence, last_signal_ts_ms, updated_ts_ms, meta_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "m1",
                    "model_one",
                    "AAPL",
                    300,
                    "global",
                    "champion",
                    3.2,
                    1,
                    1,
                    0,
                    26.0,
                    25.0,
                    0.82,
                    int(now_ms),
                    int(now_ms),
                    json.dumps({"score_source": "pnl_attribution"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO model_metrics(model_name, symbol, horizon_s, n, ts_ms, metrics_json)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    "model_one",
                    "AAPL",
                    300,
                    5,
                    int(now_ms),
                    json.dumps({"rmse": 0.5}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        report = trade_lifecycle.trace_trade_lifecycle(client_order_id="co1")
        self.assertTrue(report.get("ok"), report)
        self.assertEqual(report["anchor"]["source_alert_id"], 101)
        self.assertFalse(report.get("breaks"))
        self.assertEqual(int(report["steps"]["alert"]["event_id"]), 44)
        self.assertEqual(str(report["steps"]["prediction"]["model_id"]), "m1")
        self.assertEqual(int(report["steps"]["prediction"]["id"]), prediction_id)
        self.assertEqual(int(report["steps"]["portfolio_orders"][0]["prediction_id"]), prediction_id)
        self.assertEqual(int(report["steps"]["fills"][0]["prediction_id"]), prediction_id)
        self.assertEqual(len(report["steps"]["prediction_history"]), 2)
        self.assertEqual(len(report["steps"]["fills"]), 1)
        self.assertEqual(str(report["steps"]["position"]["symbol"]), "AAPL")

    def test_trace_trade_lifecycle_projects_order_events_without_execution_tables(self) -> None:
        storage, portfolio, execution_ledger, order_command_boundary, trade_lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.execution.execution_ledger",
            "engine.execution.order_command_boundary",
            "engine.runtime.trade_lifecycle",
        )
        storage.init_db()
        self._executescript(portfolio.SCHEMA + execution_ledger.SCHEMA + order_command_boundary.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            prediction_id = int(
                con.execute(
                    """
                    INSERT INTO predictions(
                      ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                      confidence_raw, prediction_strength, model_name, model_id, model_version
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (int(now_ms), 66, "AAPL", 300, 1.4, 0.91, 0.91, 0.94, "model_one", "m1", "v1"),
                ).lastrowid
                or 0
            )
            con.execute(
                """
                INSERT INTO prediction_history(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(now_ms - 25), 66, "AAPL", 300, 1.1, 0.80, 0.80, 0.86, "model_one", "m1", "v0"),
            )
            con.execute(
                """
                INSERT INTO prediction_history(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), 66, "AAPL", 300, 1.4, 0.91, 0.91, 0.94, "model_one", "m1", "v1"),
            )
            con.execute(
                """
                INSERT INTO decision_log(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  model_name, model_kind, model_ts_ms, features_hash, features_json, explain_json, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    66,
                    "AAPL",
                    300,
                    1.4,
                    0.91,
                    "model_one",
                    "xgb",
                    int(now_ms),
                    "hash",
                    json.dumps({"f": 1}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"note": "projected-order-events"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    606,
                    int(now_ms),
                    66,
                    int(prediction_id),
                    "Projected lifecycle",
                    "AAPL",
                    300,
                    1.4,
                    0.91,
                    "HIGH",
                    "rule.projected",
                    json.dumps({"model_name": "model_one", "model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    "AAPL:300:projected:606",
                    "model_one",
                    "m1",
                    "v1",
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side, from_weight,
                  to_weight, delta_weight, source_alert_id, prediction_id, explain_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "m1",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.25,
                    0.25,
                    606,
                    int(prediction_id),
                    json.dumps({"source": "projected"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute("DROP TABLE execution_orders")
            con.execute("DROP TABLE execution_fills")
            con.execute(
                """
                INSERT INTO model_position_state(model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms)
                VALUES (?,?,?,?,?,?)
                """,
                ("m1", "AAPL", 5.0, 100.1, 0.0, int(now_ms + 60)),
            )
            command_id = order_command_boundary.record_order_command(
                ts_ms=int(now_ms + 10),
                batch_id=1,
                payload_ts_ms=int(now_ms + 10),
                correlation_id="co-event-only",
                mode="paper",
                broker="paper",
                payload_source="unit_test",
                real_order_count=1,
                shadow_order_count=0,
                blocked_order_count=0,
                payload={"source_alert_id": 606, "prediction_id": int(prediction_id), "model_id": "m1", "symbol": "AAPL"},
                con=con,
            )
            order_command_boundary.record_order_event(
                ts_ms=int(now_ms + 25),
                event_type="order_submit",
                mode="paper",
                broker="paper",
                status="submitted",
                command_id=str(command_id),
                batch_id=1,
                correlation_id="co-event-only",
                payload={
                    "client_order_id": "co-event-only",
                    "portfolio_orders_id": 1,
                    "source_alert_id": 606,
                    "prediction_id": int(prediction_id),
                    "model_id": "m1",
                    "model_version": "v1",
                    "symbol": "AAPL",
                    "qty": 5.0,
                    "submit_ts_ms": int(now_ms + 25),
                    "broker": "paper",
                    "execution_mode": "paper",
                },
                con=con,
            )
            order_command_boundary.record_order_event(
                ts_ms=int(now_ms + 50),
                event_type="fill",
                mode="paper",
                broker="paper",
                status="filled",
                command_id=str(command_id),
                batch_id=1,
                correlation_id="co-event-only",
                payload={
                    "client_order_id": "co-event-only",
                    "fill_id": "fill-event-only",
                    "portfolio_orders_id": 1,
                    "source_alert_id": 606,
                    "prediction_id": int(prediction_id),
                    "model_id": "m1",
                    "model_version": "v1",
                    "symbol": "AAPL",
                    "fill_qty": 5.0,
                    "fill_px": 100.1,
                    "expected_px": 100.0,
                    "mid_px": 100.05,
                    "spread_bps": 2.0,
                    "slippage_bps": 10.0,
                    "fill_latency_ms": 25,
                    "fill_ts_ms": int(now_ms + 50),
                    "submit_ts_ms": int(now_ms + 25),
                    "fees": 0.15,
                    "liquidity": "maker",
                },
                con=con,
            )
            con.commit()
        finally:
            con.close()

        report = trade_lifecycle.trace_trade_lifecycle(client_order_id="co-event-only")

        self.assertTrue(report.get("ok"), report)
        self.assertEqual(int(report["anchor"]["source_alert_id"]), 606)
        self.assertEqual(str(report["steps"]["execution_order"]["client_order_id"]), "co-event-only")
        self.assertEqual(len(report["steps"]["execution_orders"]), 1)
        self.assertEqual(len(report["steps"]["order_commands"]), 1)
        self.assertEqual([str(item.get("event_type")) for item in report["steps"]["order_events"]], ["order_submit", "fill"])
        self.assertEqual(len(report["steps"]["fills"]), 1)
        self.assertEqual(int(report["steps"]["fills"][0]["prediction_id"]), prediction_id)
        self.assertEqual(int(report["steps"]["prediction"]["id"]), prediction_id)
        self.assertEqual(int(report["steps"]["portfolio_orders"][0]["id"]), 1)

    def test_trace_trade_lifecycle_prefers_typed_prediction_lineage_without_alert_event_id(self) -> None:
        storage, portfolio, execution_ledger, trade_lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.execution.execution_ledger",
            "engine.runtime.trade_lifecycle",
        )
        storage.init_db()
        self._executescript(portfolio.SCHEMA + execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            prediction_id = int(
                con.execute(
                    """
                    INSERT INTO predictions(
                      ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                      confidence_raw, prediction_strength, model_name, model_id, model_version
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (int(now_ms), 55, "AAPL", 300, 1.2, 0.9, 0.9, 0.95, "model_one", "m1", "v1"),
                ).lastrowid
                or 0
            )
            con.execute(
                """
                INSERT INTO prediction_history(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  confidence_raw, prediction_strength, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), 55, "AAPL", 300, 1.2, 0.9, 0.9, 0.95, "model_one", "m1", "v1"),
            )
            con.execute(
                """
                INSERT INTO decision_log(
                  ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                  model_name, model_kind, model_ts_ms, features_hash, features_json, explain_json, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    55,
                    "AAPL",
                    300,
                    1.2,
                    0.9,
                    "model_one",
                    "xgb",
                    int(now_ms),
                    "hash",
                    json.dumps({"f": 1}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    json.dumps({"note": "decision"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    202,
                    int(now_ms),
                    None,
                    int(prediction_id),
                    "Lifecycle typed lineage",
                    "AAPL",
                    300,
                    1.2,
                    0.9,
                    "HIGH",
                    "rule.typed",
                    json.dumps({"model_name": "model_one", "model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    "AAPL:300:typed",
                    "model_one",
                    "m1",
                    "v1",
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side, from_weight,
                  to_weight, delta_weight, source_alert_id, prediction_id, explain_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "m1",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.20,
                    0.20,
                    202,
                    int(prediction_id),
                    json.dumps({"source": "typed"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, portfolio_orders_id, source_alert_id, prediction_id, model_id,
                  model_version, symbol, qty, submit_ts_ms, ref_px, expected_px, mid_px,
                  bid_px, ask_px, spread_bps, broker_order_id, status, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "co-typed",
                    "paper",
                    1,
                    202,
                    int(prediction_id),
                    "m1",
                    "v1",
                    "AAPL",
                    10.0,
                    int(now_ms),
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    2.0,
                    "bo-typed",
                    "filled",
                    json.dumps({"model_name": "model_one"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_fills(
                  client_order_id, fill_id, broker, model_id, model_version, symbol,
                  portfolio_orders_id, source_alert_id, prediction_id, ts_ms, submit_ts_ms, fill_ts_ms,
                  fill_qty, fill_px, expected_px, mid_px, bid_px, ask_px, spread_bps, slippage_bps,
                  fill_latency_ms, fees, commission, liquidity, raw_json, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "co-typed",
                    "fill-typed",
                    "paper",
                    "m1",
                    "v1",
                    "AAPL",
                    1,
                    202,
                    int(prediction_id),
                    int(now_ms),
                    int(now_ms),
                    int(now_ms),
                    10.0,
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    2.0,
                    0.0,
                    0,
                    1.0,
                    1.0,
                    "maker",
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO model_position_state(model_id, symbol, net_qty, avg_entry_price, realized_pnl, last_update_ts_ms)
                VALUES (?,?,?,?,?,?)
                """,
                ("m1", "AAPL", 10.0, 100.0, 0.0, int(now_ms)),
            )
            con.commit()
        finally:
            con.close()

        report = trade_lifecycle.trace_trade_lifecycle(client_order_id="co-typed")

        self.assertEqual(int(report["steps"]["prediction"]["id"]), prediction_id)
        self.assertEqual(int(report["steps"]["decision"]["event_id"]), 55)
        self.assertNotIn("prediction_resolved_without_alert_event_id", list(report.get("approximations") or []))
        self.assertNotIn("decision_resolved_without_alert_event_id", list(report.get("approximations") or []))

    def test_emit_order_and_execution_writes_populate_typed_lineage_from_alert(self) -> None:
        storage, portfolio, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        self._executescript(portfolio.SCHEMA + execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            prediction_id = int(
                con.execute(
                    """
                    INSERT INTO predictions(
                      ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                      confidence_raw, prediction_strength, model_name, model_id, model_version
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (int(now_ms), 77, "AAPL", 300, 1.3, 0.88, 0.88, 0.92, "model_one", "m1", "v1"),
                ).lastrowid
                or 0
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    303,
                    int(now_ms),
                    77,
                    int(prediction_id),
                    "Emit order lineage",
                    "AAPL",
                    300,
                    1.3,
                    0.88,
                    "HIGH",
                    "rule.emit",
                    json.dumps({"model_name": "model_one", "model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    "AAPL:300:rule.emit:303",
                    "model_one",
                    "m1",
                    "v1",
                ),
            )
            portfolio._emit_order(
                con,
                sym="AAPL",
                action="OPEN",
                from_side="FLAT",
                to_side="LONG",
                from_w=0.0,
                to_w=0.20,
                source_alert_id=303,
                prediction_id=None,
                explain={"model_id": "m1", "reason": {"strategy": "unit-test"}},
            )
            portfolio_row = con.execute(
                """
                SELECT id, source_alert_id, prediction_id
                FROM portfolio_orders
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            con.commit()
        finally:
            con.close()

        portfolio_order_id = int((portfolio_row or (0, 0, 0))[0] or 0)
        self.assertGreater(portfolio_order_id, 0)
        self.assertEqual(tuple(portfolio_row or ()), (portfolio_order_id, 303, prediction_id))

        execution_ledger.log_submit(
            client_order_id="cid-typed-lineage",
            broker="paper",
            symbol="AAPL",
            qty=5.0,
            submit_ts_ms=int(now_ms + 25),
            portfolio_orders_id=portfolio_order_id,
            extra={"model_id": "m1"},
        )
        execution_ledger.log_fill(
            client_order_id="cid-typed-lineage",
            fill_id="fill-typed-lineage",
            broker="paper",
            symbol="AAPL",
            qty=5.0,
            fill_px=100.0,
            fill_ts_ms=int(now_ms + 50),
            fees=0.10,
            extra={"model_id": "m1"},
        )

        con = storage.connect(readonly=True)
        try:
            order_row = con.execute(
                """
                SELECT portfolio_orders_id, source_alert_id, prediction_id
                FROM execution_orders
                WHERE client_order_id='cid-typed-lineage'
                """
            ).fetchone()
            fill_row = con.execute(
                """
                SELECT portfolio_orders_id, source_alert_id, prediction_id
                FROM execution_fills
                WHERE client_order_id='cid-typed-lineage'
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(tuple(order_row or ()), (portfolio_order_id, 303, prediction_id))
        self.assertEqual(tuple(fill_row or ()), (portfolio_order_id, 303, prediction_id))

    def test_trace_trade_lifecycle_uses_portfolio_lineage_when_execution_order_is_sparse(self) -> None:
        storage, portfolio, execution_ledger, trade_lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.execution.execution_ledger",
            "engine.runtime.trade_lifecycle",
        )
        storage.init_db()
        self._executescript(portfolio.SCHEMA + execution_ledger.SCHEMA)

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            prediction_id = int(
                con.execute(
                    """
                    INSERT INTO predictions(
                      ts_ms, event_id, symbol, horizon_s, predicted_z, confidence,
                      confidence_raw, prediction_strength, model_name, model_id, model_version
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (int(now_ms), 88, "AAPL", 300, 1.0, 0.8, 0.8, 0.85, "model_one", "m1", "v1"),
                ).lastrowid
                or 0
            )
            con.execute(
                """
                INSERT INTO alerts(
                  id, ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    404,
                    int(now_ms),
                    88,
                    int(prediction_id),
                    "Sparse order lineage",
                    "AAPL",
                    300,
                    1.0,
                    0.8,
                    "HIGH",
                    "rule.sparse",
                    json.dumps({"model_name": "model_one", "model_id": "m1"}, separators=(",", ":"), sort_keys=True),
                    "AAPL:300:rule.sparse:404",
                    "model_one",
                    "m1",
                    "v1",
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side, from_weight,
                  to_weight, delta_weight, source_alert_id, prediction_id, explain_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "m1",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.10,
                    0.10,
                    404,
                    int(prediction_id),
                    json.dumps({"source": "portfolio"}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO execution_orders(
                  client_order_id, broker, portfolio_orders_id, source_alert_id, prediction_id, model_id,
                  model_version, symbol, qty, submit_ts_ms, ref_px, expected_px, mid_px,
                  bid_px, ask_px, spread_bps, broker_order_id, status, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    "co-sparse-lineage",
                    "paper",
                    1,
                    None,
                    None,
                    "m1",
                    "v1",
                    "AAPL",
                    2.0,
                    int(now_ms),
                    100.0,
                    100.0,
                    100.0,
                    None,
                    None,
                    1.0,
                    "bo-sparse",
                    "submitted",
                    json.dumps({}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        report = trade_lifecycle.trace_trade_lifecycle(client_order_id="co-sparse-lineage")

        self.assertEqual(int(report["anchor"]["source_alert_id"]), 404)
        self.assertEqual(int(report["steps"]["alert"]["id"]), 404)
        self.assertEqual(int(report["steps"]["prediction"]["id"]), prediction_id)
        self.assertEqual(int(report["steps"]["portfolio_orders"][0]["prediction_id"]), prediction_id)
        self.assertNotIn("missing_alert", list(report.get("breaks") or []))


if __name__ == "__main__":
    unittest.main()
