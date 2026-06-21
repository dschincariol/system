from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
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


class _RollbackOnCloseConnection:
    in_transaction = True

    def __init__(self, db_path: Path) -> None:
        self._con = sqlite3.connect(str(db_path))
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, sql: str, params=()):
        return self._con.execute(sql, params)

    def executescript(self, sql: str):
        return self._con.executescript(sql)

    def commit(self) -> None:
        self.commits += 1
        self._con.commit()

    def rollback(self) -> None:
        self.rollbacks += 1
        self._con.rollback()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.rollback()
        self._con.close()


class _AmbientThenDurableSqliteConnect:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.ambient = _RollbackOnCloseConnection(db_path)
        self.calls = 0

    def __call__(self, *args, **kwargs):
        del args, kwargs
        self.calls += 1
        if self.calls == 1:
            return self.ambient
        return sqlite3.connect(str(self.db_path))


def _run_sqlite_write_txn(con):
    def _run(fn, *args, **kwargs):
        result = fn(con)
        con.commit()
        return result

    return _run


def _sqlite_connect_factory(db_path: Path):
    def _connect(*args, **kwargs):
        del args, kwargs
        return sqlite3.connect(str(db_path))

    return _connect


def _idempotency_row(con, client_order_id: str):
    return con.execute(
        """
        SELECT status, broker_order_id, last_error
        FROM execution_order_idempotency
        WHERE client_order_id=?
        LIMIT 1
        """,
        (str(client_order_id),),
    ).fetchone()


def _idempotency_row_from_db(db_path: Path, client_order_id: str):
    con = sqlite3.connect(str(db_path))
    try:
        return _idempotency_row(con, client_order_id)
    finally:
        con.close()


def _patch_common_alpaca_live(stack: ExitStack, alpaca, connector: _AmbientThenDurableSqliteConnect, *, set_state=None) -> None:
    stack.enter_context(patch.object(alpaca, "KEY_ID", "key"))
    stack.enter_context(patch.object(alpaca, "SECRET", "secret"))
    stack.enter_context(patch.object(alpaca, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
    stack.enter_context(patch.object(alpaca, "_prelive_reconcile_or_block", return_value=None))
    stack.enter_context(patch.object(alpaca, "connect", side_effect=connector))
    stack.enter_context(patch.object(alpaca, "get_state", return_value="0"))
    stack.enter_context(patch.object(alpaca, "set_state", set_state or Mock()))
    stack.enter_context(patch.object(alpaca, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})))
    stack.enter_context(patch.object(alpaca, "execution_allowed", return_value=(True, None, None)))
    stack.enter_context(patch.object(alpaca, "get_account", return_value={"equity": 100000.0, "buying_power": 100000.0, "cash": 100000.0}))
    stack.enter_context(patch.object(alpaca, "get_positions", return_value=[]))
    stack.enter_context(patch.object(alpaca, "_alpaca_pos_map", return_value={}))
    stack.enter_context(patch.object(alpaca, "_load_latest_prices", return_value={"AAPL": 100.0}))
    stack.enter_context(patch.object(alpaca, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})))
    stack.enter_context(patch.object(alpaca, "_price_at_or_before", return_value=100.0))
    stack.enter_context(patch.object(alpaca, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))
    stack.enter_context(patch.object(alpaca, "wait_with_kill_interrupt", return_value=(True, None, {})))


def _patch_common_ibkr_live(stack: ExitStack, ibkr, connector: _AmbientThenDurableSqliteConnect, app=None, *, set_state=None) -> None:
    stack.enter_context(patch.object(ibkr, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
    stack.enter_context(patch.object(ibkr, "_ibkr_credentials_block", return_value=None))
    stack.enter_context(patch.object(ibkr, "_prelive_reconcile_or_block", return_value=None))
    stack.enter_context(patch.object(ibkr, "connect", side_effect=connector))
    stack.enter_context(patch.object(ibkr, "get_state", return_value="0"))
    stack.enter_context(patch.object(ibkr, "set_state", set_state or Mock()))
    stack.enter_context(patch.object(ibkr, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})))
    stack.enter_context(patch.object(ibkr, "execution_allowed", return_value=(True, None, None)))
    stack.enter_context(patch.object(ibkr, "compute_deployable_equity_from_env", return_value=100000.0))
    stack.enter_context(patch.object(ibkr, "get_positions_live", return_value=[]))
    stack.enter_context(patch.object(ibkr, "_load_latest_prices", return_value={"AAPL": 100.0}))
    stack.enter_context(patch.object(ibkr, "_price_at_or_before", return_value=100.0))
    stack.enter_context(patch.object(ibkr, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})))
    stack.enter_context(patch.object(ibkr, "_adaptive_aggressiveness", return_value=("MARKET", "AGGRESSIVE", 0.0, 100.0)))
    stack.enter_context(patch.object(ibkr, "_connect_ib", return_value=app or Mock()))
    stack.enter_context(patch.object(ibkr, "_mk_stock_contract", return_value=object()))
    stack.enter_context(patch.object(ibkr, "_mk_market_order", return_value=SimpleNamespace()))
    stack.enter_context(patch.object(ibkr, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))
    stack.enter_context(patch.object(ibkr, "wait_with_kill_interrupt", return_value=(True, None, {})))


def _insert_open_order(
    con,
    *,
    broker: str = "alpaca",
    symbol: str = "AAPL",
    qty: float = 1.0,
    order_type: str = "LIMIT",
    aggressiveness: str = "PASSIVE",
    limit_px: float = 100.0,
    client_order_id: str = "cid-open-1",
    broker_order_id: str = "alp-open-1",
    attempts: int = 0,
    max_attempts: int = 2,
) -> int:
    con.execute(
        """
        INSERT INTO exec_open_orders(
          ts_ms, updated_ts_ms, broker, symbol, qty, side, order_type, aggressiveness,
          limit_px, client_order_id, broker_order_id, status, attempts, max_attempts,
          next_action_ts_ms, portfolio_orders_id, source_alert_id, meta_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            1_710_000_000_000,
            1_710_000_000_000,
            str(broker),
            str(symbol),
            float(qty),
            "BUY" if float(qty) > 0 else "SELL",
            str(order_type),
            str(aggressiveness),
            float(limit_px),
            str(client_order_id),
            str(broker_order_id),
            "open",
            int(attempts),
            int(max_attempts),
            0,
            88,
            12,
            "{}",
        ),
    )
    con.commit()
    return int(con.execute("SELECT last_insert_rowid()").fetchone()[0])


def _order_row(con, open_id: int):
    return con.execute(
        """
        SELECT status, qty, client_order_id, broker_order_id, attempts, order_type, aggressiveness, limit_px, meta_json
        FROM exec_open_orders
        WHERE id=?
        """,
        (int(open_id),),
    ).fetchone()


def _latest_order_event(con, open_id: int):
    row = con.execute(
        """
        SELECT event, details_json
        FROM exec_order_events
        WHERE open_order_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(open_id),),
    ).fetchone()
    if not row:
        return None, {}
    import json

    return str(row[0]), json.loads(str(row[1] or "{}"))


def _assert_cancel_replace_blocked(testcase: unittest.TestCase, con, open_id: int, reason: str) -> None:
    row = _order_row(con, open_id)
    testcase.assertIsNotNone(row)
    testcase.assertEqual(str(row[0]), "needs_reconcile")
    event_name, details = _latest_order_event(con, open_id)
    testcase.assertEqual(event_name, "cancel_replace_needs_reconcile")
    testcase.assertEqual(str(details.get("reason") or ""), str(reason))
    alert = con.execute(
        """
        SELECT severity, alert_type, state
        FROM execution_alerts
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    testcase.assertEqual(tuple(alert), ("critical", "limit_cancel_replace_needs_reconcile", "needs_reconcile"))


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

    def test_equal_sized_parent_slices_have_distinct_durable_order_ids(self) -> None:
        (order_idempotency,) = _reload_modules("engine.execution.order_idempotency")
        con = sqlite3.connect(":memory:")
        try:
            base = {
                "symbol": "AAPL",
                "action": "BUY",
                "qty": 1.0,
                "parent_order_id": 77,
                "slice_count": 2,
                "slice_style": "adaptive",
            }
            first = order_idempotency.claim_order_submission(
                con=con,
                broker="alpaca",
                portfolio_orders_id=77,
                portfolio_ts_ms=1_710_000_000_000,
                order={**base, "slice_index": 0},
            )
            second = order_idempotency.claim_order_submission(
                con=con,
                broker="alpaca",
                portfolio_orders_id=77,
                portfolio_ts_ms=1_710_000_000_000,
                order={**base, "slice_index": 1},
            )
            rows = con.execute(
                """
                SELECT parent_order_id, slice_index, slice_count, client_order_id
                FROM execution_order_idempotency
                ORDER BY slice_index
                """
            ).fetchall()
        finally:
            con.close()

        self.assertTrue(bool(first.get("ok")))
        self.assertTrue(bool(second.get("ok")))
        self.assertFalse(bool(second.get("duplicate")))
        self.assertNotEqual(str(first.get("client_order_id")), str(second.get("client_order_id")))
        self.assertEqual([(int(r[0]), int(r[1]), int(r[2])) for r in rows], [(77, 0, 2), (77, 1, 2)])

    def test_durable_connection_supports_legacy_no_readonly_connect_fn(self) -> None:
        (order_idempotency,) = _reload_modules("engine.execution.order_idempotency")
        token = object()

        class LegacyConnect:
            def __init__(self) -> None:
                self.calls = 0

            def __call__(self):
                self.calls += 1
                return token

        connector = LegacyConnect()
        with patch.object(order_idempotency, "_warn_nonfatal", return_value=None) as warn:
            result = order_idempotency._open_dedicated_connection(connector)

        self.assertIs(result, token)
        self.assertEqual(connector.calls, 1)
        warn.assert_called_once()
        self.assertEqual(warn.call_args.args[0], "ORDER_IDEMPOTENCY_CONNECT_READONLY_ARG_UNSUPPORTED")

    def test_durable_connection_does_not_mask_internal_type_error(self) -> None:
        (order_idempotency,) = _reload_modules("engine.execution.order_idempotency")

        def broken_connect(*, readonly=False):
            raise TypeError("internal connection bug")

        with patch.object(order_idempotency, "_warn_nonfatal", return_value=None) as warn:
            with self.assertRaisesRegex(TypeError, "internal connection bug"):
                order_idempotency._open_dedicated_connection(broken_connect)

        warn.assert_not_called()

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

    def test_submission_unrecorded_status_stops_failover_chain(self) -> None:
        env = {
            "BROKER_FAILOVER": "alpaca,sim",
            "BROKER_ROUTER_RETRY_ATTEMPTS": "2",
            "EXEC_ADAPTIVE_SLICING": "0",
        }
        orders = [{"symbol": "AAPL", "qty": 1.0, "action": "BUY"}]

        with patch.dict(os.environ, env, clear=False):
            (broker_router,) = _reload_modules("engine.execution.broker_router")
            unrecorded = {
                "ok": False,
                "status": "submission_unrecorded",
                "broker": "alpaca",
                "detail": "broker_accepted_order_local_bookkeeping_failed",
                "client_order_id": "alp_test",
            }
            sim_adapter = Mock(side_effect=AssertionError("failover should stop after accepted unrecorded submit"))

            with ExitStack() as stack:
                _mute_router_side_effects(stack, broker_router)
                stack.enter_context(patch.object(broker_router, "_execution_gate_or_block", return_value=None))
                stack.enter_context(patch.object(broker_router, "_real_trading_gate_or_block", return_value=None))
                stack.enter_context(patch.object(broker_router, "_prelive_reconcile", return_value={"ok": True}))
                stack.enter_context(patch.object(broker_router, "_alpaca_apply", Mock(return_value=unrecorded)))
                stack.enter_context(patch.object(broker_router, "_sim_apply", sim_adapter))

                result = broker_router.apply_new_portfolio_orders_router(
                    dry_run=False,
                    override_orders=orders,
                    override_order_id=77,
                    override_ts_ms=1234,
                )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "submission_unrecorded")
        self.assertTrue(bool(result.get("stop_failover")))
        self.assertEqual(len(result.get("failover_attempts") or []), 1)
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
                                                                with patch.object(alpaca, "_prelive_reconcile_or_block", return_value=None):
                                                                    with patch.object(alpaca, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}):
                                                                        with patch.object(alpaca, "claim_order_submission_durable", return_value=guard) as claim_mock:
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

    def test_router_two_slice_parent_calls_adapter_for_both_slices_with_parent_identity(self) -> None:
        (broker_router,) = _reload_modules("engine.execution.broker_router")
        calls = []

        def adapter(**kwargs):
            calls.append(dict((kwargs.get("override_orders") or [{}])[0]))
            return {"ok": True, "status": "applied", "submitted_n": 1, "broker": "alpaca"}

        slices = [
            {"symbol": "AAPL", "qty": 1.0, "slice_style": "adaptive", "slice_interval_ms": 0},
            {"symbol": "AAPL", "qty": 1.0, "slice_style": "adaptive", "slice_interval_ms": 0},
        ]
        with patch.dict(os.environ, {"EXEC_ADAPTIVE_SLICING": "1"}, clear=False):
            with ExitStack() as stack:
                stack.enter_context(patch.object(broker_router, "build_order_slices", return_value=slices))
                stack.enter_context(patch.object(broker_router, "_load_recent_slippage_bps", return_value=[]))
                stack.enter_context(patch.object(broker_router, "wait_with_kill_interrupt", return_value=(True, None, {})))
                result = broker_router._adaptive_execute_orders(
                    broker_name="alpaca",
                    fn=adapter,
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 2.0}],
                    override_order_id=88,
                    override_ts_ms=1_710_000_000_000,
                )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(len(calls), 2)
        self.assertEqual([int(c["slice_index"]) for c in calls], [0, 1])
        self.assertEqual([int(c["slice_count"]) for c in calls], [2, 2])
        self.assertEqual([int(c["parent_order_id"]) for c in calls], [88, 88])

    def test_alpaca_multi_slice_override_does_not_use_completed_parent_cursor(self) -> None:
        (alpaca,) = _reload_modules("engine.execution.broker_alpaca_rest")
        set_state = Mock()
        guard = {
            "ok": True,
            "duplicate": False,
            "order_uid": "uid-slice-1",
            "client_order_id": "cid-slice-1",
            "status": "claimed",
        }
        order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "order_type": "MARKET",
            "parent_order_id": 88,
            "slice_index": 1,
            "slice_count": 2,
            "slice_style": "adaptive",
        }

        with ExitStack() as stack:
            _patch_common_alpaca_live(stack, alpaca, lambda *args, **kwargs: _CloseOnlyConnection(), set_state=set_state)
            stack.enter_context(patch.object(alpaca, "get_state", return_value="88"))
            submit = stack.enter_context(patch.object(alpaca, "_submit_market_order", return_value={"id": "alp-slice-1"}))
            stack.enter_context(patch.object(alpaca, "claim_order_submission_durable", return_value=guard))
            stack.enter_context(patch.object(alpaca, "log_submit", return_value=None))
            stack.enter_context(patch.object(alpaca, "mark_order_submission_submitted_durable", return_value=None))

            result = alpaca.apply_latest_portfolio_orders_live(
                dry_run=False,
                override_orders=[order],
                override_order_id=88,
                override_ts_ms=1_710_000_000_000,
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(int(result.get("submitted_n") or 0), 1)
        self.assertTrue(bool(result.get("parent_cursor_deferred")))
        submit.assert_called_once()
        set_state.assert_not_called()

    def test_ibkr_multi_slice_override_does_not_use_completed_parent_cursor(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")
        app = Mock()
        app.disconnect = Mock()
        set_state = Mock()
        order = {
            "symbol": "AAPL",
            "qty": 1.0,
            "parent_order_id": 88,
            "slice_index": 1,
            "slice_count": 2,
            "slice_style": "adaptive",
        }
        expected_uid, expected_cid = ibkr._client_order_identity_for_ibkr_order(
            portfolio_orders_id=88,
            portfolio_ts_ms=1_710_000_000_000,
            order=order,
        )
        guard = {
            "ok": True,
            "duplicate": False,
            "order_uid": expected_uid,
            "client_order_id": expected_cid,
            "status": "claimed",
        }

        with ExitStack() as stack:
            _patch_common_ibkr_live(stack, ibkr, lambda *args, **kwargs: _CloseOnlyConnection(), app, set_state=set_state)
            stack.enter_context(patch.object(ibkr, "get_state", return_value="88"))
            stack.enter_context(patch.object(ibkr, "claim_order_submission_durable", return_value=guard))
            stack.enter_context(patch.object(ibkr, "_consume_next_order_id", return_value=45678))
            place = stack.enter_context(patch.object(ibkr, "_place_order_with_order_ref", return_value=str(expected_cid)))
            stack.enter_context(patch.object(ibkr, "log_submit", return_value=None))
            stack.enter_context(patch.object(ibkr, "mark_order_submission_submitted_durable", return_value=None))

            result = ibkr.apply_latest_portfolio_orders_live(
                dry_run=False,
                override_orders=[order],
                override_order_id=88,
                override_ts_ms=1_710_000_000_000,
            )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(int(result.get("submitted_n") or 0), 1)
        self.assertTrue(bool(result.get("parent_cursor_deferred")))
        place.assert_called_once()
        set_state.assert_not_called()

    def test_adaptive_restart_skips_submitted_slice_and_submits_unsubmitted_slice(self) -> None:
        order_idempotency, broker_router, alpaca = _reload_modules(
            "engine.execution.order_idempotency",
            "engine.execution.broker_router",
            "engine.execution.broker_alpaca_rest",
        )
        slice0 = {
            "symbol": "AAPL",
            "qty": 1.0,
            "parent_order_id": 88,
            "slice_index": 0,
            "slice_count": 2,
            "slice_style": "adaptive",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            con = sqlite3.connect(str(db_path))
            try:
                guard = order_idempotency.claim_order_submission(
                    con=con,
                    broker="alpaca",
                    portfolio_orders_id=88,
                    portfolio_ts_ms=1_710_000_000_000,
                    order=slice0,
                )
                order_idempotency.mark_order_submission_submitted(
                    con=con,
                    order_uid=str(guard["order_uid"]),
                    client_order_id=str(guard["client_order_id"]),
                    broker_order_id="alp-slice-0",
                    submit_ts_ms=1_710_000_000_001,
                )
                con.commit()
            finally:
                con.close()

            submitted_client_ids = []
            set_state = Mock()

            def submit_order(_symbol, _qty, client_order_id):
                submitted_client_ids.append(str(client_order_id))
                return {"id": "alp-slice-1"}

            with ExitStack() as stack:
                _patch_common_alpaca_live(stack, alpaca, _sqlite_connect_factory(db_path), set_state=set_state)
                stack.enter_context(patch.object(alpaca, "_submit_market_order", side_effect=submit_order))
                stack.enter_context(patch.object(alpaca, "log_submit", return_value=None))
                stack.enter_context(patch.object(broker_router, "build_order_slices", return_value=[
                    {"symbol": "AAPL", "qty": 1.0, "slice_style": "adaptive", "slice_interval_ms": 0},
                    {"symbol": "AAPL", "qty": 1.0, "slice_style": "adaptive", "slice_interval_ms": 0},
                ]))
                stack.enter_context(patch.object(broker_router, "_load_recent_slippage_bps", return_value=[]))
                stack.enter_context(patch.object(broker_router, "wait_with_kill_interrupt", return_value=(True, None, {})))

                result = broker_router._adaptive_execute_orders(
                    broker_name="alpaca",
                    fn=alpaca.apply_latest_portfolio_orders_live,
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 2.0}],
                    override_order_id=88,
                    override_ts_ms=1_710_000_000_000,
                )

            readback = sqlite3.connect(str(db_path))
            try:
                rows = readback.execute(
                    """
                    SELECT slice_index, status, broker_order_id
                    FROM execution_order_idempotency
                    ORDER BY slice_index
                    """
                ).fetchall()
            finally:
                readback.close()

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(len(submitted_client_ids), 1)
        self.assertEqual([(int(r[0]), str(r[1]), str(r[2])) for r in rows], [
            (0, "submitted", "alp-slice-0"),
            (1, "submitted", "alp-slice-1"),
        ])
        set_state.assert_not_called()

    def test_adaptive_mid_parent_failure_does_not_advance_parent_cursor(self) -> None:
        _, broker_router, alpaca = _reload_modules(
            "engine.execution.order_idempotency",
            "engine.execution.broker_router",
            "engine.execution.broker_alpaca_rest",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            set_state = Mock()
            submit_calls = []

            def submit_order(_symbol, _qty, client_order_id):
                submit_calls.append(str(client_order_id))
                if len(submit_calls) == 1:
                    return {"id": "alp-slice-0"}
                raise RuntimeError("broker response ambiguous")

            with ExitStack() as stack:
                _patch_common_alpaca_live(stack, alpaca, _sqlite_connect_factory(db_path), set_state=set_state)
                stack.enter_context(patch.object(alpaca, "_submit_market_order", side_effect=submit_order))
                stack.enter_context(patch.object(alpaca, "log_submit", return_value=None))
                stack.enter_context(patch.object(broker_router, "build_order_slices", return_value=[
                    {"symbol": "AAPL", "qty": 1.0, "slice_style": "adaptive", "slice_interval_ms": 0},
                    {"symbol": "AAPL", "qty": 1.0, "slice_style": "adaptive", "slice_interval_ms": 0},
                ]))
                stack.enter_context(patch.object(broker_router, "_load_recent_slippage_bps", return_value=[]))
                stack.enter_context(patch.object(broker_router, "wait_with_kill_interrupt", return_value=(True, None, {})))

                result = broker_router._adaptive_execute_orders(
                    broker_name="alpaca",
                    fn=alpaca.apply_latest_portfolio_orders_live,
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 2.0}],
                    override_order_id=88,
                    override_ts_ms=1_710_000_000_000,
                )

            readback = sqlite3.connect(str(db_path))
            try:
                rows = readback.execute(
                    """
                    SELECT slice_index, status, broker_order_id
                    FROM execution_order_idempotency
                    ORDER BY slice_index
                    """
                ).fetchall()
            finally:
                readback.close()

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "submit_inflight_unknown")
        self.assertEqual(len(submit_calls), 2)
        self.assertEqual([(int(r[0]), str(r[1]), None if r[2] is None else str(r[2])) for r in rows], [
            (0, "submitted", "alp-slice-0"),
            (1, "submit_inflight_unknown", None),
        ])
        set_state.assert_not_called()

    def test_alpaca_direct_live_adapter_blocks_on_prelive_reconcile_before_broker_reads(self) -> None:
        (alpaca,) = _reload_modules("engine.execution.broker_alpaca_rest")
        reconcile_block = {
            "ok": False,
            "status": "mismatch",
            "broker": "alpaca",
            "fatal_reconcile": True,
        }

        with patch.object(alpaca, "KEY_ID", "key"):
            with patch.object(alpaca, "SECRET", "secret"):
                with patch.object(alpaca, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}):
                    with patch.object(alpaca, "_prelive_reconcile_or_block", return_value=reconcile_block):
                        with patch.object(alpaca, "connect", side_effect=AssertionError("storage should not open after reconcile block")):
                            with patch.object(alpaca, "get_account", side_effect=AssertionError("account read should not happen")):
                                with patch.object(alpaca, "_submit_market_order", side_effect=AssertionError("broker submit should not happen")):
                                    result = alpaca.apply_latest_portfolio_orders_live(
                                        dry_run=False,
                                        override_orders=[{"symbol": "AAPL", "qty": 1.0}],
                                        override_order_id=88,
                                        override_ts_ms=1_710_000_000_000,
                                    )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "mismatch")
        self.assertTrue(bool(result.get("fatal_reconcile")))

    def test_alpaca_submit_helper_blocks_when_pre_submit_audit_fails(self) -> None:
        (alpaca,) = _reload_modules("engine.execution.broker_alpaca_rest")

        with patch.object(alpaca, "KEY_ID", "key"):
            with patch.object(alpaca, "SECRET", "secret"):
                with patch.object(alpaca, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}):
                    with patch.object(alpaca, "_prelive_reconcile_or_block", return_value=None):
                        with patch.object(
                            alpaca,
                            "record_broker_action_audit",
                            return_value={"ok": False, "status": "broker_action_audit_failed", "broker": "alpaca"},
                        ):
                            with patch.object(alpaca, "_submit_market_order", side_effect=AssertionError("broker submit should not happen")):
                                result = alpaca.submit_market_order("AAPL", 1.0, "cid-1")

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "broker_action_audit_failed")

    def test_unrecorded_submission_gate_blocks_before_position_reconcile(self) -> None:
        order_idempotency, recovery, alpaca = _reload_modules(
            "engine.execution.order_idempotency",
            "engine.execution.broker_submission_recovery",
            "engine.execution.broker_alpaca_rest",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            guard = order_idempotency.claim_order_submission(
                con=con,
                broker="alpaca",
                portfolio_orders_id=88,
                portfolio_ts_ms=1_710_000_000_000,
                order={"symbol": "AAPL", "qty": 1.0, "source_order_id": 12},
            )
            order_idempotency.mark_order_submission_unrecorded(
                con=con,
                broker="alpaca",
                order_uid=str(guard["order_uid"]),
                client_order_id=str(guard["client_order_id"]),
                broker_order_id="alp-accepted-1",
                submit_ts_ms=1_710_000_000_001,
                last_error="log_submit:RuntimeError:ledger down",
                symbol="AAPL",
                portfolio_orders_id=88,
                portfolio_ts_ms=1_710_000_000_000,
                source_order_id=12,
                payload={"symbol": "AAPL", "qty": 1.0},
            )
            prelive = Mock(return_value={"ok": True})

            with patch.object(alpaca, "prelive_reconcile_policy_gate", return_value=None):
                with patch.object(alpaca, "connect", side_effect=lambda *args, **kwargs: con):
                    with patch.object(alpaca, "_prelive_reconcile", prelive):
                        result = alpaca._prelive_reconcile_or_block("alpaca")

            self.assertIsNotNone(result)
            self.assertFalse(bool((result or {}).get("ok")))
            self.assertEqual(str((result or {}).get("status") or ""), "needs_reconcile")
            self.assertEqual(str((result or {}).get("reason") or ""), "submission_unrecorded")
            self.assertTrue(bool((result or {}).get("fatal_reconcile")))
            prelive.assert_not_called()
            self.assertIs(recovery.unrecorded_submission_gate(broker="sim", con=con), None)
        finally:
            con.real_close()

    def test_alpaca_accepted_order_log_submit_failure_returns_submission_unrecorded(self) -> None:
        recovery, alpaca = _reload_modules(
            "engine.execution.broker_submission_recovery",
            "engine.execution.broker_alpaca_rest",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        set_state = Mock()
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(alpaca, "KEY_ID", "key"))
                stack.enter_context(patch.object(alpaca, "SECRET", "secret"))
                stack.enter_context(patch.object(alpaca, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
                stack.enter_context(patch.object(alpaca, "_prelive_reconcile_or_block", return_value=None))
                stack.enter_context(patch.object(alpaca, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(patch.object(alpaca, "get_state", return_value="0"))
                stack.enter_context(patch.object(alpaca, "set_state", set_state))
                stack.enter_context(patch.object(alpaca, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})))
                stack.enter_context(patch.object(alpaca, "execution_allowed", return_value=(True, None, None)))
                stack.enter_context(patch.object(alpaca, "get_account", return_value={"equity": 100000.0, "buying_power": 100000.0, "cash": 100000.0}))
                stack.enter_context(patch.object(alpaca, "get_positions", return_value=[]))
                stack.enter_context(patch.object(alpaca, "_alpaca_pos_map", return_value={}))
                stack.enter_context(patch.object(alpaca, "_load_latest_prices", return_value={"AAPL": 100.0}))
                stack.enter_context(patch.object(alpaca, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})))
                stack.enter_context(patch.object(alpaca, "_price_at_or_before", return_value=100.0))
                stack.enter_context(patch.object(alpaca, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))
                stack.enter_context(patch.object(recovery, "record_broker_action_audit", return_value={"ok": True, "event_id": 2}))
                stack.enter_context(patch.object(recovery, "log_failure", return_value={}))
                stack.enter_context(patch.object(recovery, "emit_counter", return_value=None))
                submit = stack.enter_context(patch.object(alpaca, "_submit_market_order", return_value={"id": "alp-accepted-1"}))
                log_submit = stack.enter_context(patch.object(alpaca, "log_submit", side_effect=RuntimeError("ledger down")))
                mark_submitted = stack.enter_context(patch.object(alpaca, "mark_order_submission_submitted_durable", side_effect=AssertionError("mark should not run after log_submit failure")))

                result = alpaca.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 1.0, "source_order_id": 12}],
                    override_order_id=88,
                    override_ts_ms=1_710_000_000_000,
                )

            self.assertFalse(bool(result.get("ok")))
            self.assertEqual(str(result.get("status") or ""), "submission_unrecorded")
            self.assertEqual(str(result.get("reason") or ""), "needs_reconcile")
            self.assertTrue(bool(result.get("stop_failover")))
            self.assertTrue(bool(result.get("fatal_reconcile")))
            self.assertTrue(bool((result.get("recovery_marker") or {}).get("marker_written")))
            self.assertEqual(str(result.get("broker_order_id") or ""), "alp-accepted-1")
            submit.assert_called_once()
            log_submit.assert_called_once()
            mark_submitted.assert_not_called()
            set_state.assert_not_called()

            row = _idempotency_row(con, str(result.get("client_order_id") or ""))
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submission_unrecorded")
            self.assertEqual(str(row[1]), "alp-accepted-1")
            self.assertIn("ledger down", str(row[2]))
            alert = con.execute("SELECT severity, alert_type, state FROM execution_alerts").fetchone()
            self.assertEqual(tuple(alert), ("critical", "broker_submission_unrecorded_needs_reconcile", "needs_reconcile"))
        finally:
            con.real_close()

    def test_alpaca_accepted_order_mark_submitted_failure_returns_submission_unrecorded(self) -> None:
        recovery, alpaca = _reload_modules(
            "engine.execution.broker_submission_recovery",
            "engine.execution.broker_alpaca_rest",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(alpaca, "KEY_ID", "key"))
                stack.enter_context(patch.object(alpaca, "SECRET", "secret"))
                stack.enter_context(patch.object(alpaca, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
                stack.enter_context(patch.object(alpaca, "_prelive_reconcile_or_block", return_value=None))
                stack.enter_context(patch.object(alpaca, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(patch.object(alpaca, "get_state", return_value="0"))
                stack.enter_context(patch.object(alpaca, "set_state", return_value=None))
                stack.enter_context(patch.object(alpaca, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})))
                stack.enter_context(patch.object(alpaca, "execution_allowed", return_value=(True, None, None)))
                stack.enter_context(patch.object(alpaca, "get_account", return_value={"equity": 100000.0, "buying_power": 100000.0, "cash": 100000.0}))
                stack.enter_context(patch.object(alpaca, "get_positions", return_value=[]))
                stack.enter_context(patch.object(alpaca, "_alpaca_pos_map", return_value={}))
                stack.enter_context(patch.object(alpaca, "_load_latest_prices", return_value={"AAPL": 100.0}))
                stack.enter_context(patch.object(alpaca, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})))
                stack.enter_context(patch.object(alpaca, "_price_at_or_before", return_value=100.0))
                stack.enter_context(patch.object(alpaca, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))
                stack.enter_context(patch.object(recovery, "record_broker_action_audit", return_value={"ok": True, "event_id": 2}))
                stack.enter_context(patch.object(recovery, "log_failure", return_value={}))
                stack.enter_context(patch.object(recovery, "emit_counter", return_value=None))
                stack.enter_context(patch.object(alpaca, "_submit_market_order", return_value={"id": "alp-accepted-2"}))
                log_submit = stack.enter_context(patch.object(alpaca, "log_submit", return_value=None))
                mark_submitted = stack.enter_context(patch.object(alpaca, "mark_order_submission_submitted_durable", side_effect=RuntimeError("idempotency write down")))

                result = alpaca.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 1.0, "source_order_id": 13}],
                    override_order_id=89,
                    override_ts_ms=1_710_000_000_100,
                )

            self.assertFalse(bool(result.get("ok")))
            self.assertEqual(str(result.get("status") or ""), "submission_unrecorded")
            self.assertTrue(bool(result.get("stop_failover")))
            log_submit.assert_called_once()
            mark_submitted.assert_called_once()
            row = _idempotency_row(con, str(result.get("client_order_id") or ""))
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submission_unrecorded")
            self.assertEqual(str(row[1]), "alp-accepted-2")
            self.assertIn("idempotency write down", str(row[2]))
        finally:
            con.real_close()

    def test_alpaca_live_submitted_marker_survives_ambient_rollback(self) -> None:
        (alpaca,) = _reload_modules("engine.execution.broker_alpaca_rest")

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            connector = _AmbientThenDurableSqliteConnect(db_path)

            def submit_order(_symbol, _qty, client_order_id):
                row = _idempotency_row_from_db(db_path, str(client_order_id))
                self.assertIsNotNone(row)
                self.assertEqual(str(row[0]), "claimed")
                return {"id": "alp-durable-1"}

            with ExitStack() as stack:
                _patch_common_alpaca_live(stack, alpaca, connector)
                submit = stack.enter_context(patch.object(alpaca, "_submit_market_order", side_effect=submit_order))
                stack.enter_context(patch.object(alpaca, "log_submit", return_value=None))

                result = alpaca.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 1.0, "source_order_id": 21, "order_type": "MARKET"}],
                    override_order_id=121,
                    override_ts_ms=1_710_000_001_000,
                )

            self.assertTrue(bool(result.get("ok")))
            self.assertTrue(connector.ambient.closed)
            self.assertEqual(connector.ambient.commits, 0)
            client_order_id = str(submit.call_args.args[2])
            row = _idempotency_row_from_db(db_path, client_order_id)
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submitted")
            self.assertEqual(str(row[1]), "alp-durable-1")

    def test_alpaca_live_unknown_marker_survives_ambient_rollback(self) -> None:
        (alpaca,) = _reload_modules("engine.execution.broker_alpaca_rest")

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            connector = _AmbientThenDurableSqliteConnect(db_path)

            def submit_order(_symbol, _qty, client_order_id):
                row = _idempotency_row_from_db(db_path, str(client_order_id))
                self.assertIsNotNone(row)
                self.assertEqual(str(row[0]), "claimed")
                raise RuntimeError("broker response ambiguous")

            with ExitStack() as stack:
                _patch_common_alpaca_live(stack, alpaca, connector)
                submit = stack.enter_context(patch.object(alpaca, "_submit_market_order", side_effect=submit_order))
                stack.enter_context(patch.object(alpaca, "log_submit", side_effect=AssertionError("ledger should not run")))

                result = alpaca.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 1.0, "source_order_id": 22, "order_type": "MARKET"}],
                    override_order_id=122,
                    override_ts_ms=1_710_000_001_100,
                )

            self.assertFalse(bool(result.get("ok")))
            self.assertEqual(str(result.get("status") or ""), "submit_inflight_unknown")
            self.assertTrue(connector.ambient.closed)
            self.assertEqual(connector.ambient.commits, 0)
            client_order_id = str(submit.call_args.args[2])
            row = _idempotency_row_from_db(db_path, client_order_id)
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submit_inflight_unknown")
            self.assertIn("broker response ambiguous", str(row[2]))

    def test_ibkr_live_submission_unrecorded_marker_survives_ambient_rollback(self) -> None:
        recovery, ibkr = _reload_modules(
            "engine.execution.broker_submission_recovery",
            "engine.execution.broker_ibkr_gateway",
        )
        app = Mock()
        app.disconnect = Mock()

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            connector = _AmbientThenDurableSqliteConnect(db_path)

            def place_order(_order_id, _contract, order):
                client_order_id = str(getattr(order, "orderRef", "") or "")
                row = _idempotency_row_from_db(db_path, client_order_id)
                self.assertIsNotNone(row)
                self.assertEqual(str(row[0]), "claimed")

            with ExitStack() as stack:
                _patch_common_ibkr_live(stack, ibkr, connector, app)
                stack.enter_context(patch.object(ibkr, "_consume_next_order_id", return_value=45678))
                stack.enter_context(patch.object(app, "placeOrder", side_effect=place_order))
                stack.enter_context(patch.object(ibkr, "log_submit", side_effect=RuntimeError("ledger down")))
                stack.enter_context(patch.object(recovery, "record_broker_action_audit", return_value={"ok": True, "event_id": 2}))
                stack.enter_context(patch.object(recovery, "log_failure", return_value={}))
                stack.enter_context(patch.object(recovery, "emit_counter", return_value=None))

                result = ibkr.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 1.0, "source_order_id": 23}],
                    override_order_id=123,
                    override_ts_ms=1_710_000_001_200,
                )

            self.assertFalse(bool(result.get("ok")))
            self.assertEqual(str(result.get("status") or ""), "submission_unrecorded")
            self.assertTrue(connector.ambient.closed)
            self.assertEqual(connector.ambient.commits, 0)
            row = _idempotency_row_from_db(db_path, str(result.get("client_order_id") or ""))
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submission_unrecorded")
            self.assertEqual(str(row[1]), "45678")
            self.assertIn("ledger down", str(row[2]))

    def test_ibkr_duplicate_claim_retry_does_not_consume_or_place_fresh_order_id(self) -> None:
        order_idempotency, ibkr = _reload_modules(
            "engine.execution.order_idempotency",
            "engine.execution.broker_ibkr_gateway",
        )
        app = Mock()
        app.disconnect = Mock()
        order = {"symbol": "AAPL", "qty": 1.0, "source_order_id": 24}

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            seed = sqlite3.connect(str(db_path))
            try:
                guard = order_idempotency.claim_order_submission(
                    con=seed,
                    broker="ibkr",
                    portfolio_orders_id=124,
                    portfolio_ts_ms=1_710_000_001_300,
                    order=order,
                )
                seed.commit()
            finally:
                seed.close()

            connector = _AmbientThenDurableSqliteConnect(db_path)

            with ExitStack() as stack:
                _patch_common_ibkr_live(stack, ibkr, connector, app)
                consume = stack.enter_context(patch.object(ibkr, "_consume_next_order_id", side_effect=AssertionError("duplicate claim must not consume a new IBKR order id")))
                place = stack.enter_context(patch.object(app, "placeOrder", side_effect=AssertionError("duplicate claim must not place a fresh broker order")))
                stack.enter_context(patch.object(ibkr, "log_submit", side_effect=AssertionError("duplicate claim should not log a fresh submission")))

                result = ibkr.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[order],
                    override_order_id=124,
                    override_ts_ms=1_710_000_001_300,
                )

            self.assertTrue(bool(result.get("ok")))
            self.assertEqual(int(result.get("submitted_n") or 0), 0)
            self.assertEqual(str(guard.get("status") or ""), "claimed")
            consume.assert_not_called()
            place.assert_not_called()

    def test_ibkr_accepted_order_log_submit_failure_returns_submission_unrecorded(self) -> None:
        recovery, ibkr = _reload_modules(
            "engine.execution.broker_submission_recovery",
            "engine.execution.broker_ibkr_gateway",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        app = Mock()
        set_state = Mock()
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(ibkr, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
                stack.enter_context(patch.object(ibkr, "_ibkr_credentials_block", return_value=None))
                stack.enter_context(patch.object(ibkr, "_prelive_reconcile_or_block", return_value=None))
                stack.enter_context(patch.object(ibkr, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(patch.object(ibkr, "get_state", return_value="0"))
                stack.enter_context(patch.object(ibkr, "set_state", set_state))
                stack.enter_context(patch.object(ibkr, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})))
                stack.enter_context(patch.object(ibkr, "execution_allowed", return_value=(True, None, None)))
                stack.enter_context(patch.object(ibkr, "compute_deployable_equity_from_env", return_value=100000.0))
                stack.enter_context(patch.object(ibkr, "get_positions_live", return_value=[]))
                stack.enter_context(patch.object(ibkr, "_load_latest_prices", return_value={"AAPL": 100.0}))
                stack.enter_context(patch.object(ibkr, "_price_at_or_before", return_value=100.0))
                stack.enter_context(patch.object(ibkr, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})))
                stack.enter_context(patch.object(ibkr, "_adaptive_aggressiveness", return_value=("MARKET", "AGGRESSIVE", 0.0, 100.0)))
                stack.enter_context(patch.object(ibkr, "_connect_ib", return_value=app))
                stack.enter_context(patch.object(ibkr, "_consume_next_order_id", return_value=12345))
                stack.enter_context(patch.object(ibkr, "_mk_stock_contract", return_value=object()))
                stack.enter_context(patch.object(ibkr, "_mk_market_order", return_value=SimpleNamespace()))
                stack.enter_context(patch.object(ibkr, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))
                stack.enter_context(patch.object(recovery, "record_broker_action_audit", return_value={"ok": True, "event_id": 2}))
                stack.enter_context(patch.object(recovery, "log_failure", return_value={}))
                stack.enter_context(patch.object(recovery, "emit_counter", return_value=None))
                log_submit = stack.enter_context(patch.object(ibkr, "log_submit", side_effect=RuntimeError("ledger down")))
                mark_submitted = stack.enter_context(patch.object(ibkr, "mark_order_submission_submitted_durable", side_effect=AssertionError("mark should not run after log_submit failure")))

                result = ibkr.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 1.0, "source_order_id": 14}],
                    override_order_id=90,
                    override_ts_ms=1_710_000_000_200,
                )

            self.assertFalse(bool(result.get("ok")))
            self.assertEqual(str(result.get("status") or ""), "submission_unrecorded")
            self.assertEqual(str(result.get("reason") or ""), "needs_reconcile")
            self.assertTrue(bool(result.get("stop_failover")))
            self.assertTrue(bool(result.get("fatal_reconcile")))
            self.assertEqual(str(result.get("broker_order_id") or ""), "12345")
            app.placeOrder.assert_called_once()
            log_submit.assert_called_once()
            mark_submitted.assert_not_called()
            set_state.assert_not_called()
            row = _idempotency_row(con, str(result.get("client_order_id") or ""))
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submission_unrecorded")
            self.assertEqual(str(row[1]), "12345")
            self.assertIn("ledger down", str(row[2]))
        finally:
            con.real_close()

    def test_ibkr_market_apply_sets_order_ref_from_idempotency_client_order_id(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        app = Mock()
        app.disconnect = Mock()
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(ibkr, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
                stack.enter_context(patch.object(ibkr, "_ibkr_credentials_block", return_value=None))
                stack.enter_context(patch.object(ibkr, "_prelive_reconcile_or_block", return_value=None))
                stack.enter_context(patch.object(ibkr, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(patch.object(ibkr, "get_state", return_value="0"))
                stack.enter_context(patch.object(ibkr, "set_state", return_value=None))
                stack.enter_context(patch.object(ibkr, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})))
                stack.enter_context(patch.object(ibkr, "execution_allowed", return_value=(True, None, None)))
                stack.enter_context(patch.object(ibkr, "compute_deployable_equity_from_env", return_value=100000.0))
                stack.enter_context(patch.object(ibkr, "get_positions_live", return_value=[]))
                stack.enter_context(patch.object(ibkr, "_load_latest_prices", return_value={"AAPL": 100.0}))
                stack.enter_context(patch.object(ibkr, "_price_at_or_before", return_value=100.0))
                stack.enter_context(patch.object(ibkr, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})))
                stack.enter_context(patch.object(ibkr, "_adaptive_aggressiveness", return_value=("MARKET", "AGGRESSIVE", 0.0, 100.0)))
                stack.enter_context(patch.object(ibkr, "_connect_ib", return_value=app))
                stack.enter_context(patch.object(ibkr, "_consume_next_order_id", return_value=22345))
                stack.enter_context(patch.object(ibkr, "_mk_stock_contract", return_value=object()))
                stack.enter_context(patch.object(ibkr, "_mk_market_order", return_value=SimpleNamespace()))
                stack.enter_context(patch.object(ibkr, "wait_with_kill_interrupt", return_value=(True, None, {})))
                audit = stack.enter_context(patch.object(ibkr, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))
                log_submit = stack.enter_context(patch.object(ibkr, "log_submit", return_value=None))

                result = ibkr.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "AAPL", "qty": 1.0, "source_order_id": 15}],
                    override_order_id=91,
                    override_ts_ms=1_710_000_000_300,
                )

            self.assertTrue(bool(result.get("ok")))
            app.placeOrder.assert_called_once()
            log_submit.assert_called_once()
            client_order_id = str(log_submit.call_args.kwargs["client_order_id"])
            submitted_order = app.placeOrder.call_args.args[2]
            self.assertEqual(getattr(submitted_order, "orderRef"), client_order_id)
            self.assertEqual(audit.call_args.kwargs["client_order_id"], client_order_id)
            self.assertEqual(audit.call_args.kwargs["payload"]["ibkr_order_ref"], client_order_id)
            self.assertEqual(log_submit.call_args.kwargs["extra"]["ibkr_order_ref"], client_order_id)
            row = _idempotency_row(con, client_order_id)
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submitted")
            self.assertEqual(str(row[1]), "22345")
        finally:
            con.real_close()

    def test_ibkr_limit_apply_sets_order_ref_from_idempotency_client_order_id(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        app = Mock()
        app.disconnect = Mock()
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(ibkr, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
                stack.enter_context(patch.object(ibkr, "_ibkr_credentials_block", return_value=None))
                stack.enter_context(patch.object(ibkr, "_prelive_reconcile_or_block", return_value=None))
                stack.enter_context(patch.object(ibkr, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(patch.object(ibkr, "get_state", return_value="0"))
                stack.enter_context(patch.object(ibkr, "set_state", return_value=None))
                stack.enter_context(patch.object(ibkr, "apply_alpha_lifecycle", side_effect=lambda **kwargs: (list(kwargs.get("orders") or []), {"ok": True})))
                stack.enter_context(patch.object(ibkr, "execution_allowed", return_value=(True, None, None)))
                stack.enter_context(patch.object(ibkr, "compute_deployable_equity_from_env", return_value=100000.0))
                stack.enter_context(patch.object(ibkr, "get_positions_live", return_value=[]))
                stack.enter_context(patch.object(ibkr, "_load_latest_prices", return_value={"MSFT": 100.0}))
                stack.enter_context(patch.object(ibkr, "_price_at_or_before", return_value=100.0))
                stack.enter_context(patch.object(ibkr, "_apply_execution_risk_caps", side_effect=lambda **kwargs: (kwargs["delta_qty"], {})))
                stack.enter_context(patch.object(ibkr, "_adaptive_aggressiveness", return_value=("LIMIT", "PASSIVE", 4.0, 100.0)))
                stack.enter_context(patch.object(ibkr, "_connect_ib", return_value=app))
                stack.enter_context(patch.object(ibkr, "_consume_next_order_id", return_value=22346))
                stack.enter_context(patch.object(ibkr, "_mk_stock_contract", return_value=object()))
                stack.enter_context(patch.object(ibkr, "_mk_limit_order", return_value=SimpleNamespace()))
                stack.enter_context(patch.object(ibkr, "wait_with_kill_interrupt", return_value=(True, None, {})))
                stack.enter_context(patch("engine.execution.execution_microstructure.record_open_order", return_value=None))
                audit = stack.enter_context(patch.object(ibkr, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))
                log_submit = stack.enter_context(patch.object(ibkr, "log_submit", return_value=None))

                result = ibkr.apply_latest_portfolio_orders_live(
                    dry_run=False,
                    override_orders=[{"symbol": "MSFT", "qty": -1.0, "source_order_id": 16}],
                    override_order_id=92,
                    override_ts_ms=1_710_000_000_400,
                )

            self.assertTrue(bool(result.get("ok")))
            app.placeOrder.assert_called_once()
            log_submit.assert_called_once()
            client_order_id = str(log_submit.call_args.kwargs["client_order_id"])
            submitted_order = app.placeOrder.call_args.args[2]
            self.assertEqual(getattr(submitted_order, "orderRef"), client_order_id)
            self.assertEqual(audit.call_args.kwargs["client_order_id"], client_order_id)
            self.assertEqual(audit.call_args.kwargs["payload"]["broker_native_client_order_ref"], client_order_id)
            self.assertEqual(log_submit.call_args.kwargs["extra"]["broker_native_client_order_ref"], client_order_id)
            row = _idempotency_row(con, client_order_id)
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submitted")
            self.assertEqual(str(row[1]), "22346")
        finally:
            con.real_close()

    def test_ibkr_direct_submit_helpers_set_order_ref_before_place_order(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")
        app = Mock()
        app.disconnect = Mock()

        with ExitStack() as stack:
            stack.enter_context(patch.object(ibkr, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
            stack.enter_context(patch.object(ibkr, "_prelive_reconcile_or_block", return_value=None))
            stack.enter_context(patch.object(ibkr, "_ibkr_credentials_block", return_value=None))
            stack.enter_context(patch.object(ibkr, "_connect_ib", return_value=app))
            stack.enter_context(patch.object(ibkr, "_consume_next_order_id", side_effect=[9001, 9002]))
            stack.enter_context(patch.object(ibkr, "_mk_stock_contract", return_value=object()))
            stack.enter_context(patch.object(ibkr, "_mk_market_order", return_value=SimpleNamespace()))
            stack.enter_context(patch.object(ibkr, "_mk_limit_order", return_value=SimpleNamespace()))
            audit = stack.enter_context(patch.object(ibkr, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}))

            market = ibkr.submit_market_order("AAPL", 1.0, "cid.IBKR_123-ABC")
            limit = ibkr.submit_limit_order("MSFT", -1.0, 101.25, "cid.IBKR_456-DEF")

        self.assertEqual(market["orderRef"], "cid.IBKR_123-ABC")
        self.assertEqual(limit["orderRef"], "cid.IBKR_456-DEF")
        self.assertEqual(app.placeOrder.call_count, 2)
        self.assertEqual(getattr(app.placeOrder.call_args_list[0].args[2], "orderRef"), "cid.IBKR_123-ABC")
        self.assertEqual(getattr(app.placeOrder.call_args_list[1].args[2], "orderRef"), "cid.IBKR_456-DEF")
        self.assertEqual(audit.call_args_list[0].kwargs["payload"]["ibkr_order_ref"], "cid.IBKR_123-ABC")
        self.assertEqual(audit.call_args_list[1].kwargs["payload"]["ibkr_order_ref"], "cid.IBKR_456-DEF")

    def test_ibkr_direct_submit_rejects_unsafe_order_ref_before_connect(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")

        with ExitStack() as stack:
            stack.enter_context(patch.object(ibkr, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}))
            stack.enter_context(patch.object(ibkr, "_prelive_reconcile_or_block", return_value=None))
            stack.enter_context(patch.object(ibkr, "_ibkr_credentials_block", return_value=None))
            connect_ib = stack.enter_context(patch.object(ibkr, "_connect_ib", side_effect=AssertionError("broker connect should not happen")))
            audit = stack.enter_context(patch.object(ibkr, "record_broker_action_audit", side_effect=AssertionError("audit should not run for invalid local orderRef")))

            unsafe = ibkr.submit_market_order("AAPL", 1.0, "cid with spaces")
            too_long = ibkr.submit_limit_order("MSFT", 1.0, 101.25, "x" * (ibkr.IBKR_ORDER_REF_MAX_LEN + 1))

        self.assertFalse(bool(unsafe.get("ok")))
        self.assertEqual(str(unsafe.get("status") or ""), "invalid_order_ref")
        self.assertTrue(bool(unsafe.get("stop_failover")))
        self.assertEqual(str(too_long.get("status") or ""), "invalid_order_ref")
        connect_ib.assert_not_called()
        audit.assert_not_called()

    def test_ibkr_direct_live_adapter_blocks_on_prelive_reconcile_before_connect(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")
        reconcile_block = {
            "ok": False,
            "status": "baseline_missing",
            "broker": "ibkr",
            "fatal_reconcile": True,
        }

        with patch.object(ibkr, "_real_trading_gate", return_value={"ok": True, "real_trading_allowed": True}):
            with patch.object(ibkr, "ibkr_credentials_status", return_value={"ok": True, "missing": [], "invalid": []}):
                with patch.object(ibkr, "_prelive_reconcile_or_block", return_value=reconcile_block):
                    with patch.object(ibkr, "connect", side_effect=AssertionError("storage should not open after reconcile block")):
                        with patch.object(ibkr, "_connect_ib", side_effect=AssertionError("broker connect should not happen")):
                            result = ibkr.apply_latest_portfolio_orders_live(
                                dry_run=False,
                                override_orders=[{"symbol": "AAPL", "qty": 1.0}],
                                override_order_id=88,
                                override_ts_ms=1_710_000_000_000,
                            )

        self.assertFalse(bool(result.get("ok")))
        self.assertEqual(str(result.get("status") or ""), "baseline_missing")
        self.assertTrue(bool(result.get("fatal_reconcile")))

    def test_alpaca_cancel_order_reports_unverified_when_order_remains_open(self) -> None:
        (alpaca,) = _reload_modules("engine.execution.broker_alpaca_rest")

        def fake_req(method, path, payload=None, **_kwargs):
            del payload
            if str(method).upper() == "DELETE":
                return {}
            if str(method).upper() == "GET" and str(path).endswith("/alp-open-1"):
                return {"id": "alp-open-1", "status": "new", "qty": "1", "filled_qty": "0", "side": "buy"}
            raise AssertionError(f"unexpected request {method} {path}")

        with patch.object(alpaca, "KEY_ID", "key"):
            with patch.object(alpaca, "SECRET", "secret"):
                with patch.object(alpaca, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}):
                    with patch.object(alpaca, "_req", side_effect=fake_req):
                        result = alpaca.cancel_order("alp-open-1", timeout_s=0.0)

        self.assertFalse(bool(result.get("ok")))
        self.assertFalse(bool(result.get("cancel_verified")))
        self.assertEqual(str(result.get("status") or ""), "cancel_not_verified")

    def test_alpaca_cancel_order_reports_verified_terminal_cancel(self) -> None:
        (alpaca,) = _reload_modules("engine.execution.broker_alpaca_rest")

        def fake_req(method, path, payload=None, **_kwargs):
            del payload
            if str(method).upper() == "DELETE":
                return {}
            if str(method).upper() == "GET" and str(path).endswith("/alp-cancel-1"):
                return {"id": "alp-cancel-1", "status": "canceled", "qty": "10", "filled_qty": "4", "side": "buy"}
            raise AssertionError(f"unexpected request {method} {path}")

        with patch.object(alpaca, "KEY_ID", "key"):
            with patch.object(alpaca, "SECRET", "secret"):
                with patch.object(alpaca, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}):
                    with patch.object(alpaca, "_req", side_effect=fake_req):
                        result = alpaca.cancel_order("alp-cancel-1", timeout_s=0.0)

        self.assertTrue(bool(result.get("ok")))
        self.assertTrue(bool(result.get("cancel_verified")))
        self.assertTrue(bool(result.get("terminal_cancel_verified")))
        self.assertAlmostEqual(float(result.get("remaining_qty") or 0.0), 6.0)

    def test_ibkr_cancel_order_reports_unverified_when_order_remains_open(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")

        class _FakeIB:
            def cancelOrder(self, _oid):
                return None

            def disconnect(self):
                return None

        with patch.object(ibkr, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}):
            with patch.object(ibkr, "_connect_ib", return_value=_FakeIB()):
                with patch.object(ibkr.time, "sleep", return_value=None):
                    with patch.object(
                        ibkr,
                        "get_order",
                        return_value={"orderId": 101, "status": "Submitted", "totalQuantity": 1, "filled": 0, "remaining": 1},
                    ):
                        result = ibkr.cancel_order("101", timeout_s=0.0)

        self.assertFalse(bool(result.get("ok")))
        self.assertFalse(bool(result.get("cancel_verified")))
        self.assertEqual(str(result.get("status") or ""), "cancel_not_verified")

    def test_ibkr_cancel_order_reports_verified_when_open_order_absent(self) -> None:
        (ibkr,) = _reload_modules("engine.execution.broker_ibkr_gateway")

        class _FakeIB:
            def cancelOrder(self, _oid):
                return None

            def disconnect(self):
                return None

        with patch.object(ibkr, "record_broker_action_audit", return_value={"ok": True, "event_id": 1}):
            with patch.object(ibkr, "_connect_ib", return_value=_FakeIB()):
                with patch.object(ibkr.time, "sleep", return_value=None):
                    with patch.object(ibkr, "get_order", return_value={}):
                        result = ibkr.cancel_order("101", timeout_s=0.0)

        self.assertTrue(bool(result.get("ok")))
        self.assertTrue(bool(result.get("cancel_verified")))
        self.assertTrue(bool(result.get("terminal_cancel_verified")))


class OpenOrderSubmissionRecoveryRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._native_replace_env = os.environ.get("EXEC_NATIVE_LIMIT_REPLACE_ENABLED")
        os.environ["EXEC_NATIVE_LIMIT_REPLACE_ENABLED"] = "0"

    def tearDown(self) -> None:
        if self._native_replace_env is None:
            os.environ.pop("EXEC_NATIVE_LIMIT_REPLACE_ENABLED", None)
        else:
            os.environ["EXEC_NATIVE_LIMIT_REPLACE_ENABLED"] = str(self._native_replace_env)

    def test_open_order_manager_prefers_native_limit_replace_when_available(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con)

            with patch.dict(os.environ, {"EXEC_NATIVE_LIMIT_REPLACE_ENABLED": "1"}, clear=False):
                with ExitStack() as stack:
                    stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                    stack.enter_context(patch.object(alpaca, "get_order", return_value={"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"}))
                    replace = stack.enter_context(
                        patch.object(
                            alpaca,
                            "replace_limit_order",
                            return_value={
                                "ok": True,
                                "replace_verified": True,
                                "broker_order_id": "alp-open-1",
                                "broker_status": "new",
                            },
                        )
                    )
                    cancel = stack.enter_context(patch.object(alpaca, "cancel_order", side_effect=AssertionError("native replace should avoid cancel")))
                    submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("native replace should avoid second submit")))

                    result = open_manager.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 0)
            replace.assert_called_once()
            cancel.assert_not_called()
            submit.assert_not_called()
            row = _order_row(con, open_id)
            self.assertEqual(str(row[0]), "open")
            self.assertEqual(str(row[2]), "cid-open-1")
            self.assertEqual(str(row[3]), "alp-open-1")
            self.assertEqual(int(row[4] or 0), 1)
            event_name, details = _latest_order_event(con, open_id)
            self.assertEqual(event_name, "native_replaced")
            self.assertGreater(float(details.get("limit_px") or 0.0), 100.0)
        finally:
            con.real_close()

    def test_open_order_manager_accepted_replace_log_failure_returns_submission_unrecorded(self) -> None:
        recovery, alpaca, open_manager = _reload_modules(
            "engine.execution.broker_submission_recovery",
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con)

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {"status": "canceled", "qty": "1", "filled_qty": "0", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "cancel_order",
                        return_value={
                            "ok": True,
                            "cancel_verified": True,
                            "terminal_cancel_verified": True,
                            "broker_status": "canceled",
                            "remaining_qty": 1.0,
                        },
                    )
                )
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", return_value={"id": "alp-replace-accepted-1"}))
                stack.enter_context(patch.object(open_manager, "log_submit", side_effect=RuntimeError("ledger down")))
                stack.enter_context(patch.object(recovery, "record_broker_action_audit", return_value={"ok": True, "event_id": 3}))
                stack.enter_context(patch.object(recovery, "log_failure", return_value={}))
                stack.enter_context(patch.object(recovery, "emit_counter", return_value=None))

                result = open_manager.manage_open_orders()

            self.assertFalse(bool(result.get("ok")))
            self.assertEqual(str(result.get("status") or ""), "submission_unrecorded")
            self.assertEqual(str(result.get("reason") or ""), "needs_reconcile")
            self.assertTrue(bool(result.get("stop_failover")))
            self.assertTrue(bool(result.get("fatal_reconcile")))
            self.assertEqual(str(result.get("client_order_id") or ""), "cid-open-1_r1")
            self.assertEqual(str(result.get("broker_order_id") or ""), "alp-replace-accepted-1")
            submit.assert_called_once()

            row = _idempotency_row(con, "cid-open-1_r1")
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submission_unrecorded")
            self.assertEqual(str(row[1]), "alp-replace-accepted-1")
            self.assertIn("ledger down", str(row[2]))
            open_row = con.execute("SELECT status, broker_order_id FROM exec_open_orders WHERE id=?", (open_id,)).fetchone()
            self.assertEqual(tuple(open_row), ("submission_unrecorded", "alp-replace-accepted-1"))
            alert = con.execute("SELECT severity, alert_type, state FROM execution_alerts").fetchone()
            self.assertEqual(tuple(alert), ("critical", "broker_submission_unrecorded_needs_reconcile", "needs_reconcile"))
        finally:
            con.real_close()

    def test_open_order_manager_crash_after_replacement_submit_blocks_retry_submit(self) -> None:
        _order_idempotency, alpaca, open_manager = _reload_modules(
            "engine.execution.order_idempotency",
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            seed = sqlite3.connect(str(db_path), factory=_ManagedSqliteConnection)
            try:
                open_manager._ensure_tables(seed)
                open_id = _insert_open_order(seed)
            finally:
                seed.real_close()

            first_connector = _AmbientThenDurableSqliteConnect(db_path)

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=first_connector))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {"status": "canceled", "qty": "1", "filled_qty": "0", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "cancel_order",
                        return_value={
                            "ok": True,
                            "cancel_verified": True,
                            "terminal_cancel_verified": True,
                            "broker_status": "canceled",
                            "remaining_qty": 1.0,
                        },
                    )
                )
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", return_value={"id": "alp-crash-before-mark"}))
                stack.enter_context(patch.object(open_manager, "log_submit", side_effect=KeyboardInterrupt("crash after broker submit")))

                with self.assertRaisesRegex(KeyboardInterrupt, "crash after broker submit"):
                    open_manager.manage_open_orders()

            submit.assert_called_once()
            self.assertTrue(first_connector.ambient.closed)
            self.assertEqual(first_connector.ambient.commits, 0)
            claimed = _idempotency_row_from_db(db_path, "cid-open-1_r1")
            self.assertIsNotNone(claimed)
            self.assertEqual(str(claimed[0]), "claimed")

            retry_connector = _AmbientThenDurableSqliteConnect(db_path)
            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=retry_connector))
                stack.enter_context(patch.object(alpaca, "get_order", return_value={"status": "canceled", "qty": "1", "filled_qty": "0", "side": "buy"}))
                stack.enter_context(patch.object(alpaca, "cancel_order", side_effect=AssertionError("retry should not cancel again")))
                retry_submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("retry must not submit a second replacement")))
                stack.enter_context(patch.object(open_manager, "log_submit", side_effect=AssertionError("retry should not log a fresh submission")))

                retry = open_manager.manage_open_orders()

            self.assertFalse(bool(retry.get("ok")))
            self.assertEqual(str(retry.get("status") or ""), "submit_inflight_unknown")
            retry_submit.assert_not_called()
            unknown = _idempotency_row_from_db(db_path, "cid-open-1_r1")
            self.assertIsNotNone(unknown)
            self.assertEqual(str(unknown[0]), "submit_inflight_unknown")
            con = sqlite3.connect(str(db_path))
            try:
                row = _order_row(con, open_id)
                self.assertEqual(str(row[0]), "needs_reconcile")
                event_name, details = _latest_order_event(con, open_id)
                self.assertEqual(event_name, "cancel_replace_needs_reconcile")
                self.assertEqual(str(details.get("replacement_client_order_id") or ""), "cid-open-1_r1")
            finally:
                con.close()

    def test_open_order_manager_retry_recovers_submitted_replacement_after_open_order_rollback(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "orders.db"
            seed = sqlite3.connect(str(db_path), factory=_ManagedSqliteConnection)
            try:
                open_manager._ensure_tables(seed)
                open_id = _insert_open_order(seed)
            finally:
                seed.real_close()

            real_mark_submitted = open_manager.mark_order_submission_submitted_durable

            def mark_submitted_then_crash(**kwargs):
                real_mark_submitted(**kwargs)
                raise KeyboardInterrupt("crash before open-order commit")

            first_connector = _AmbientThenDurableSqliteConnect(db_path)
            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=first_connector))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {"status": "canceled", "qty": "1", "filled_qty": "0", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "cancel_order",
                        return_value={
                            "ok": True,
                            "cancel_verified": True,
                            "terminal_cancel_verified": True,
                            "broker_status": "canceled",
                            "remaining_qty": 1.0,
                        },
                    )
                )
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", return_value={"id": "alp-submitted-before-rollback"}))
                stack.enter_context(patch.object(open_manager, "log_submit", return_value=None))
                stack.enter_context(patch.object(open_manager, "mark_order_submission_submitted_durable", side_effect=mark_submitted_then_crash))

                with self.assertRaisesRegex(KeyboardInterrupt, "crash before open-order commit"):
                    open_manager.manage_open_orders()

            submit.assert_called_once()
            submitted = _idempotency_row_from_db(db_path, "cid-open-1_r1")
            self.assertIsNotNone(submitted)
            self.assertEqual(str(submitted[0]), "submitted")
            self.assertEqual(str(submitted[1]), "alp-submitted-before-rollback")
            con = sqlite3.connect(str(db_path))
            try:
                row = _order_row(con, open_id)
                self.assertEqual(str(row[2]), "cid-open-1")
                self.assertEqual(str(row[3]), "alp-open-1")
            finally:
                con.close()

            retry_connector = _AmbientThenDurableSqliteConnect(db_path)
            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=retry_connector))
                stack.enter_context(patch.object(alpaca, "get_order", return_value={"status": "canceled", "qty": "1", "filled_qty": "0", "side": "buy"}))
                stack.enter_context(patch.object(alpaca, "cancel_order", side_effect=AssertionError("retry should not cancel again")))
                retry_submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("retry must not submit a second replacement")))
                stack.enter_context(patch.object(open_manager, "log_submit", side_effect=AssertionError("retry should not log a fresh submission")))

                retry = open_manager.manage_open_orders()

            self.assertTrue(bool(retry.get("ok")))
            self.assertEqual(int(retry.get("errors") or 0), 0)
            retry_submit.assert_not_called()
            con = sqlite3.connect(str(db_path))
            try:
                row = _order_row(con, open_id)
                self.assertEqual(str(row[0]), "open")
                self.assertEqual(str(row[2]), "cid-open-1_r1")
                self.assertEqual(str(row[3]), "alp-submitted-before-rollback")
                event_name, details = _latest_order_event(con, open_id)
                self.assertEqual(event_name, "replacement_recovered_from_idempotency")
                self.assertEqual(str(details.get("idempotency_status") or ""), "submitted")
            finally:
                con.close()

    def test_microstructure_accepted_replace_log_failure_returns_submission_unrecorded(self) -> None:
        recovery, alpaca, micro = _reload_modules(
            "engine.execution.broker_submission_recovery",
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_microstructure",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            micro._ensure_tables(con)
            open_id = _insert_open_order(con, client_order_id="cid-micro-1", broker_order_id="alp-open-2")

            with ExitStack() as stack:
                stack.enter_context(patch.object(micro, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {"status": "canceled", "qty": "1", "filled_qty": "0", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "cancel_order",
                        return_value={
                            "ok": True,
                            "cancel_verified": True,
                            "terminal_cancel_verified": True,
                            "broker_status": "canceled",
                            "remaining_qty": 1.0,
                        },
                    )
                )
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", return_value={"id": "alp-micro-accepted-1"}))
                stack.enter_context(patch.object(micro, "log_submit", side_effect=RuntimeError("ledger down")))
                stack.enter_context(patch.object(recovery, "record_broker_action_audit", return_value={"ok": True, "event_id": 4}))
                stack.enter_context(patch.object(recovery, "log_failure", return_value={}))
                stack.enter_context(patch.object(recovery, "emit_counter", return_value=None))

                result = micro.manage_open_orders()

            self.assertFalse(bool(result.get("ok")))
            self.assertEqual(str(result.get("status") or ""), "submission_unrecorded")
            self.assertEqual(str(result.get("reason") or ""), "needs_reconcile")
            self.assertTrue(bool(result.get("stop_failover")))
            self.assertTrue(bool(result.get("fatal_reconcile")))
            self.assertEqual(str(result.get("client_order_id") or ""), "cid-micro-1_r1")
            self.assertEqual(str(result.get("broker_order_id") or ""), "alp-micro-accepted-1")
            submit.assert_called_once()

            row = _idempotency_row(con, "cid-micro-1_r1")
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "submission_unrecorded")
            self.assertEqual(str(row[1]), "alp-micro-accepted-1")
            self.assertIn("ledger down", str(row[2]))
            open_row = con.execute("SELECT status, broker_order_id FROM exec_open_orders WHERE id=?", (open_id,)).fetchone()
            self.assertEqual(tuple(open_row), ("submission_unrecorded", "alp-micro-accepted-1"))
            alert = con.execute("SELECT severity, alert_type, state FROM execution_alerts").fetchone()
            self.assertEqual(tuple(alert), ("critical", "broker_submission_unrecorded_needs_reconcile", "needs_reconcile"))
        finally:
            con.real_close()

    def test_open_order_manager_cancel_exception_blocks_replacement(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con)

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(patch.object(alpaca, "get_order", return_value={"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"}))
                stack.enter_context(patch.object(alpaca, "cancel_order", side_effect=RuntimeError("broker timeout")))
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("replacement must not submit")))

                result = open_manager.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 1)
            submit.assert_not_called()
            _assert_cancel_replace_blocked(self, con, open_id, "cancel_exception")
        finally:
            con.real_close()

    def test_open_order_manager_cancel_without_terminal_verification_blocks_replacement(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con)

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {},
                        ],
                    )
                )
                stack.enter_context(patch.object(alpaca, "cancel_order", return_value={"ok": True, "status": "accepted"}))
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("replacement must not submit")))

                result = open_manager.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 1)
            submit.assert_not_called()
            _assert_cancel_replace_blocked(self, con, open_id, "cancel_no_terminal_verification")
        finally:
            con.real_close()

    def test_open_order_manager_cancel_accepted_but_broker_still_open_blocks_replacement(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con)

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(patch.object(alpaca, "cancel_order", return_value={"ok": True, "status": "accepted"}))
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("replacement must not submit")))

                result = open_manager.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 1)
            submit.assert_not_called()
            _assert_cancel_replace_blocked(self, con, open_id, "broker_order_still_open_after_cancel")
        finally:
            con.real_close()

    def test_open_order_manager_confirmed_cancel_submits_replacement(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con)

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {"status": "canceled", "qty": "1", "filled_qty": "0", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(patch.object(alpaca, "cancel_order", return_value={"ok": True, "status": "accepted"}))

                def submit_after_claim(**_kwargs):
                    row = _idempotency_row(con, "cid-open-1_r1")
                    self.assertIsNotNone(row)
                    self.assertEqual(str(row[0]), "claimed")
                    return {"id": "alp-replace-ok"}

                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=submit_after_claim))
                log_submit = stack.enter_context(patch.object(open_manager, "log_submit", return_value=None))

                result = open_manager.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 0)
            submit.assert_called_once()
            log_submit.assert_called_once()
            row = _order_row(con, open_id)
            self.assertEqual(str(row[0]), "open")
            self.assertEqual(str(row[2]), "cid-open-1_r1")
            self.assertEqual(str(row[3]), "alp-replace-ok")
        finally:
            con.real_close()

    def test_open_order_manager_partial_fill_cancel_replaces_remaining_only(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con, qty=10.0, broker_order_id="alp-partial-1")

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "partially_filled", "qty": "10", "filled_qty": "4", "side": "buy"},
                            {"status": "canceled", "qty": "10", "filled_qty": "4", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "cancel_order",
                        return_value={
                            "ok": True,
                            "cancel_verified": True,
                            "terminal_cancel_verified": True,
                            "broker_status": "canceled",
                            "remaining_qty": 6.0,
                        },
                    )
                )
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", return_value={"id": "alp-partial-replace"}))
                stack.enter_context(patch.object(open_manager, "log_submit", return_value=None))

                result = open_manager.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 0)
            submit.assert_called_once()
            self.assertAlmostEqual(float(submit.call_args.kwargs["qty"]), 6.0)
            row = _order_row(con, open_id)
            self.assertAlmostEqual(float(row[1]), 6.0)
            self.assertEqual(str(row[2]), "cid-open-1_r1")
        finally:
            con.real_close()

    def test_open_order_manager_max_attempt_ambiguous_cancel_blocks_market_escalation(self) -> None:
        alpaca, open_manager = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_open_order_manager",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            open_manager._ensure_tables(con)
            open_id = _insert_open_order(con, attempts=2, max_attempts=2)

            with ExitStack() as stack:
                stack.enter_context(patch.object(open_manager, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                        ],
                    )
                )
                stack.enter_context(patch.object(alpaca, "cancel_order", return_value={"ok": True, "status": "accepted"}))
                submit_market = stack.enter_context(patch.object(alpaca, "submit_market_order", side_effect=AssertionError("market escalation must not submit")))
                submit_limit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("limit replacement must not submit")))

                result = open_manager.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 1)
            submit_market.assert_not_called()
            submit_limit.assert_not_called()
            _assert_cancel_replace_blocked(self, con, open_id, "broker_order_still_open_after_cancel")
        finally:
            con.real_close()

    def test_microstructure_cancel_without_terminal_verification_blocks_replacement(self) -> None:
        alpaca, micro = _reload_modules(
            "engine.execution.broker_alpaca_rest",
            "engine.execution.execution_microstructure",
        )
        con = sqlite3.connect(":memory:", factory=_ManagedSqliteConnection)
        try:
            micro._ensure_tables(con)
            open_id = _insert_open_order(con, client_order_id="cid-micro-block", broker_order_id="alp-open-block")

            with ExitStack() as stack:
                stack.enter_context(patch.object(micro, "connect", side_effect=lambda *args, **kwargs: con))
                stack.enter_context(
                    patch.object(
                        alpaca,
                        "get_order",
                        side_effect=[
                            {"status": "new", "qty": "1", "filled_qty": "0", "side": "buy"},
                            {},
                        ],
                    )
                )
                stack.enter_context(patch.object(alpaca, "cancel_order", return_value={"ok": True, "status": "accepted"}))
                submit = stack.enter_context(patch.object(alpaca, "submit_limit_order", side_effect=AssertionError("replacement must not submit")))

                result = micro.manage_open_orders()

            self.assertEqual(int(result.get("errors") or 0), 1)
            submit.assert_not_called()
            _assert_cancel_replace_blocked(self, con, open_id, "cancel_no_terminal_verification")
        finally:
            con.real_close()


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
