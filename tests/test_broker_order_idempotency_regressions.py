from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _mute_router_side_effects(stack: ExitStack, broker_router) -> None:
    for attr in (
        "emit_counter",
        "emit_timing",
        "record_rolling_rate",
        "record_component_health",
        "trace_event",
        "log_event",
    ):
        stack.enter_context(patch.object(broker_router, attr, return_value=None))


class _FakeCursor:
    def __init__(self, *, rowcount: int = 0, row=None) -> None:
        self.rowcount = int(rowcount)
        self._row = row

    def fetchone(self):
        return self._row


class _DuplicateUnreadableConnection:
    in_transaction = False

    def __init__(self) -> None:
        self.commits = 0
        self.selects = 0

    def executescript(self, _schema: str) -> None:
        return None

    def commit(self) -> None:
        self.commits += 1

    def execute(self, sql: str, _params=()):
        if "INSERT INTO execution_order_idempotency" in sql:
            return _FakeCursor(rowcount=0)
        if "SELECT order_uid, client_order_id, broker_order_id, status, submit_ts_ms" in sql:
            self.selects += 1
            return _FakeCursor(row=None)
        raise AssertionError(f"unexpected SQL: {sql}")


class _CloseOnlyConnection:
    def close(self) -> None:
        return None


class _ManagedSqliteConnection(sqlite3.Connection):
    def begin_managed_write(self) -> None:
        self.execute("BEGIN IMMEDIATE")

    def close(self) -> None:
        return None

    def real_close(self) -> None:
        super().close()


_ManagedSqliteConnection.__module__ = "sqlite3"


def _run_sqlite_write_txn(con):
    def _run(fn, *args, **kwargs):
        result = fn(con)
        con.commit()
        return result

    return _run


class BrokerRouterIdempotencyRegressionTests(unittest.TestCase):
    def test_duplicate_claim_fails_closed_when_existing_row_cannot_be_reread(self) -> None:
        (order_idempotency,) = _reload_modules("engine.execution.order_idempotency")
        con = _DuplicateUnreadableConnection()

        with patch.object(order_idempotency, "_warn_nonfatal", return_value=None):
            result = order_idempotency.claim_order_submission(
                con=con,
                broker="sim",
                portfolio_orders_id=77,
                portfolio_ts_ms=1234567890,
                order={"symbol": "AAPL", "action": "BUY", "qty": 1.0},
            )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "duplicate_exists_but_unreadable")
        self.assertTrue(str(result.get("order_uid") or ""))
        self.assertEqual(con.selects, 1)

    def test_ambiguous_submit_stops_failover_chain(self) -> None:
        env = {
            "BROKER_FAILOVER": "alpaca,sim",
            "BROKER_ROUTER_RETRY_ATTEMPTS": "2",
            "EXEC_ADAPTIVE_SLICING": "0",
        }
        orders = [{"symbol": "AAPL", "qty": 1.0, "action": "BUY"}]

        with patch.dict(os.environ, env, clear=False):
            (broker_router,) = _reload_modules("engine.execution.broker_router")
            ambiguous = {
                "ok": False,
                "status": "submit_inflight_unknown",
                "broker": "alpaca",
                "stop_failover": True,
                "detail": "broker_submit_ambiguous",
                "client_order_id": "alp_test",
            }
            sim_adapter = Mock(side_effect=AssertionError("failover should stop after ambiguous submit"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", return_value=None))
                stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", return_value=None))
                stack.enter_context(patch.object(broker_router, "_prelive_reconcile", return_value={"ok": True}))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", Mock(return_value=ambiguous)))
                stack.enter_context(patch.object(broker_router, "_sim_apply", sim_adapter))

                result = broker_router.apply_new_portfolio_orders_router(
                    dry_run=False,
                    override_orders=orders,
                    override_order_id=77,
                    override_ts_ms=1234,
                )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "submit_inflight_unknown")
        self.assertEqual(str(result.get("broker") or ""), "alpaca")
        self.assertTrue(bool(result.get("stop_failover")))
        self.assertEqual(len(result.get("failover_attempts") or []), 1)
        self.assertEqual(str((result.get("failover_attempts") or [])[0].get("broker") or ""), "alpaca")
        sim_adapter.assert_not_called()


class BrokerAdapterLineageRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_backup = {
            "ENGINE_SUPERVISED": os.environ.get("ENGINE_SUPERVISED"),
        }
        os.environ["ENGINE_SUPERVISED"] = "1"

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)

    def test_alpaca_override_orders_use_stable_batch_lineage_for_idempotency(self) -> None:
        (alpaca,) = _reload_modules(
            "engine.execution.broker_alpaca_rest",
        )

        guard = {
            "ok": True,
            "duplicate": True,
            "order_uid": "uid-1",
            "client_order_id": "cid-1",
            "status": "claimed",
        }

        with patch.object(alpaca, "KEY_ID", "key"):
            with patch.object(alpaca, "SECRET", "secret"):
                with patch.object(alpaca, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}):
                    with patch.object(alpaca, "connect", return_value=_CloseOnlyConnection()):
                        with patch.object(alpaca, "get_state", return_value="0"):
                            with patch.object(alpaca, "set_state", return_value=None):
                                with patch.object(alpaca, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})):
                                    with patch.object(alpaca, "execution_allowed", return_value=(True, None, None)):
                                        with patch.object(alpaca, "get_account", return_value={"equity": 100000.0, "buying_power": 100000.0, "cash": 100000.0}):
                                            with patch.object(alpaca, "get_positions", return_value=[]):
                                                with patch.object(alpaca, "_alpaca_pos_map", return_value={}):
                                                    with patch.object(alpaca, "_load_latest_prices", return_value={"AAPL": 100.0}):
                                                        with patch.object(alpaca, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})):
                                                            with patch.object(alpaca, "_price_at_or_before", return_value=100.0):
                                                                with patch.object(alpaca, "claim_order_submission", return_value=guard) as claim_mock:
                                                                    result = alpaca.apply_latest_portfolio_orders_live(
                                                                        dry_run=False,
                                                                        override_orders=[{"symbol": "AAPL", "to_side": "LONG", "to_weight": 0.10}],
                                                                        override_order_id=88,
                                                                        override_ts_ms=1_710_000_000_000,
                                                                    )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(int(result.get("submitted_n") or 0), 0)
        self.assertEqual(claim_mock.call_count, 1)
        self.assertEqual(int(claim_mock.call_args.kwargs.get("portfolio_orders_id") or 0), 88)
        self.assertEqual(int(claim_mock.call_args.kwargs.get("portfolio_ts_ms") or 0), 1_710_000_000_000)


class BrokerSimIdempotencyRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_backup = {
            "ENGINE_SUPERVISED": os.environ.get("ENGINE_SUPERVISED"),
            "BROKER_LATENCY_SLEEP": os.environ.get("BROKER_LATENCY_SLEEP"),
            "BROKER_START_CASH": os.environ.get("BROKER_START_CASH"),
        }
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ["BROKER_LATENCY_SLEEP"] = "0"
        os.environ["BROKER_START_CASH"] = "100000"
        self.order_idempotency, self.kill_switch, self.broker_sim = _reload_modules(
            "engine.execution.order_idempotency",
            "engine.execution.kill_switch",
            "engine.execution.broker_sim",
        )
        self.con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        self._patchers = [
            patch.object(self.broker_sim, "connect", side_effect=lambda *args, **kwargs: self.con),
            patch.object(self.broker_sim, "connect_rw_direct", side_effect=lambda *args, **kwargs: self.con),
            patch.object(self.broker_sim, "run_write_txn", side_effect=_run_sqlite_write_txn(self.con)),
            patch.object(self.broker_sim, "_broker_schema_ready", return_value=False),
            patch.object(self.broker_sim, "_get_factor_feature_asof", return_value=0.0),
            patch.object(self.broker_sim, "_warn_nonfatal", return_value=None),
            patch.object(self.order_idempotency, "_warn_nonfatal", return_value=None),
        ]
        for patcher in self._patchers:
            patcher.start()
        self.broker_sim.init_broker_db()

    def tearDown(self) -> None:
        try:
            for patcher in reversed(self._patchers):
                patcher.stop()
        except Exception:
            pass
        try:
            self.con.real_close()
        except Exception:
            pass
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)

    def test_duplicate_override_batch_does_not_duplicate_broker_order_state(self) -> None:
        override_order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "source_order_id": 123,
            "order_type": "MARKET",
        }
        ts_ms = 1_710_000_100_000

        with patch.object(self.broker_sim, "_get_price_at_or_before", return_value=(100.0, ts_ms)):
            with patch.object(self.broker_sim, "get_execution_liquidity_snapshot", return_value={}):
                with patch.object(self.broker_sim, "estimate_almgren_chriss_costs", return_value={"execution_cost_bps": 0.0}):
                    with patch.object(self.broker_sim, "_earnings_proximity_decay", return_value=0.0):
                        with patch.object(self.kill_switch, "execution_allowed", return_value=(True, None, None)):
                            with patch.object(self.broker_sim.time, "sleep", return_value=None):
                                first = self.broker_sim.apply_new_portfolio_orders(
                                    override_orders=[dict(override_order)],
                                    override_order_id=None,
                                    override_ts_ms=ts_ms,
                                )
                                second = self.broker_sim.apply_new_portfolio_orders(
                                    override_orders=[dict(override_order)],
                                    override_order_id=None,
                                    override_ts_ms=ts_ms,
                                )

        self.assertTrue(bool(first.get("ok")))
        self.assertTrue(bool(second.get("ok")))
        self.assertEqual(str(second.get("status") or ""), "no_changes")

        state_count = self.con.execute(
            "SELECT COUNT(*) FROM broker_order_state WHERE source_order_id=? AND symbol=?",
            (123, "AAPL"),
        ).fetchone()
        claim_count = self.con.execute(
            "SELECT COUNT(*) FROM execution_order_idempotency WHERE symbol=? AND broker=?",
            ("AAPL", "sim"),
        ).fetchone()

        self.assertEqual(int(state_count[0] or 0), 1)
        self.assertEqual(int(claim_count[0] or 0), 1)

    def test_read_account_normalizes_legacy_null_timeseries_snapshot(self) -> None:
        bad_ts_ms = 1_710_000_200_000
        con = self.con
        try:
            con.execute("DROP TABLE IF EXISTS broker_account")
            con.execute(
                """
                CREATE TABLE broker_account (
                    ts_ms INTEGER PRIMARY KEY,
                    updated_ts_ms INTEGER,
                    broker TEXT,
                    account_id TEXT,
                    equity REAL,
                    cash REAL,
                    buying_power REAL,
                    maintenance_margin REAL,
                    day_pnl REAL,
                    unrealized_pnl REAL,
                    realized_pnl REAL,
                    currency TEXT,
                    extra_json TEXT
                )
                """
            )
            con.execute(
                """
                INSERT INTO broker_account(
                    ts_ms, updated_ts_ms, broker, account_id, equity, cash,
                    buying_power, maintenance_margin, day_pnl, unrealized_pnl,
                    realized_pnl, currency, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(bad_ts_ms),
                    None,
                    "sim",
                    "paper",
                    0.0,
                    None,
                    0.0,
                    None,
                    None,
                    None,
                    None,
                    "USD",
                    None,
                ),
            )
            con.commit()

            with patch.object(self.broker_sim, "_warn_nonfatal") as warn_nonfatal:
                account = self.broker_sim._read_account(con)
        finally:
            pass

        self.assertEqual(float(account.get("cash") or 0.0), 100000.0)
        self.assertEqual(float(account.get("equity") or 0.0), 100000.0)
        self.assertEqual(int(account.get("updated_ts_ms") or 0), int(bad_ts_ms))
        warn_nonfatal.assert_called_once()


if __name__ == "__main__":
    unittest.main()
