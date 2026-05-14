from __future__ import annotations

import importlib
import json
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


def _restore_env(snapshot: dict[str, str | None]) -> None:
    for key, value in snapshot.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)


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


class ManagedSqliteConnection(sqlite3.Connection):
    def begin_managed_write(self) -> None:
        self.execute("BEGIN IMMEDIATE")


ManagedSqliteConnection.__module__ = "sqlite3"


def _load_baseline_from_db(con, broker: str) -> dict[str, float] | None:
    row = con.execute(
        "SELECT positions_json FROM position_reconcile_baseline WHERE broker=?",
        (str(broker),),
    ).fetchone()
    if not row:
        return None
    raw = json.loads(str(row[0] or "{}"))
    return {str(symbol).upper(): float(qty) for symbol, qty in dict(raw).items()}


def _save_baseline_to_db(con, broker: str, ts_ms: int, pos_map: dict[str, float]) -> None:
    con.execute(
        """
        INSERT INTO position_reconcile_baseline(broker, ts_ms, positions_json)
        VALUES(?,?,?)
        ON CONFLICT(broker) DO UPDATE SET
          ts_ms=excluded.ts_ms,
          positions_json=excluded.positions_json
        """,
        (
            str(broker),
            int(ts_ms),
            json.dumps(dict(pos_map or {}), separators=(",", ":"), sort_keys=True),
        ),
    )


class PositionReconcileSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._env_keys = [
            "ENGINE_SUPERVISED",
            "EXECUTION_PRELIVE_RECONCILE",
            "EXECUTION_RECONCILE_REQUIRE_BASELINE",
            "EXECUTION_RECONCILE_ALLOW_BOOTSTRAP",
            "TS_RECONCILE_BOOTSTRAP_TOKEN",
            "TS_RECONCILE_BOOTSTRAP_CONFIRM",
            "TS_RECONCILE_BOOTSTRAP_ACTOR",
            "POSITION_RECONCILE_QTY_TOL",
            "POSITION_RECONCILE_IGNORE_QTY_LT",
            "BROKER_FAILOVER",
            "BROKER_ROUTER_RETRY_ATTEMPTS",
            "EXEC_ADAPTIVE_SLICING",
        ]
        self._env_backup = {key: os.environ.get(key) for key in self._env_keys}
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ["EXECUTION_PRELIVE_RECONCILE"] = "1"
        os.environ["EXECUTION_RECONCILE_REQUIRE_BASELINE"] = "1"
        os.environ["EXECUTION_RECONCILE_ALLOW_BOOTSTRAP"] = "1"
        os.environ["TS_RECONCILE_BOOTSTRAP_TOKEN"] = "bootstrap-token"
        os.environ["TS_RECONCILE_BOOTSTRAP_CONFIRM"] = "bootstrap-token"
        os.environ["TS_RECONCILE_BOOTSTRAP_ACTOR"] = "test-operator"
        os.environ["POSITION_RECONCILE_QTY_TOL"] = "0.01"
        os.environ["POSITION_RECONCILE_IGNORE_QTY_LT"] = "0"
        os.environ["BROKER_FAILOVER"] = "alpaca"
        os.environ["BROKER_ROUTER_RETRY_ATTEMPTS"] = "1"
        os.environ["EXEC_ADAPTIVE_SLICING"] = "0"
        self.position_reconcile, self.broker_router = _reload_modules(
            "engine.execution.position_reconcile",
            "engine.execution.broker_router",
        )
        self.con = sqlite3.connect(":memory:", factory=ManagedSqliteConnection)

    def tearDown(self) -> None:
        try:
            self.con.close()
        except Exception:
            pass
        _restore_env(self._env_backup)

    def test_bootstrap_sets_pending_and_next_live_trade_re_reconciles_baseline(self) -> None:
        broker_positions = Mock(
            side_effect=[
                (True, "ok", [{"symbol": "AAPL", "qty": 1.0}]),
                (True, "ok", [{"symbol": "AAPL", "qty": 2.0}]),
            ]
        )
        adapter = Mock(return_value={"ok": True, "status": "submitted", "submitted_n": 1})

        def _prelive_with_shared_connection(*, broker: str):
            return self.position_reconcile.pre_live_position_reconcile(broker, con=self.con)

        with ExitStack() as stack:
            stack.enter_context(patch.object(self.position_reconcile, "_broker_positions", broker_positions))
            stack.enter_context(patch.object(self.position_reconcile, "_load_baseline", _load_baseline_from_db))
            stack.enter_context(patch.object(self.position_reconcile, "_save_baseline", _save_baseline_to_db))
            stack.enter_context(
                patch.object(
                    self.position_reconcile,
                    "_now_ms",
                    side_effect=[1_710_000_000_000, 1_710_000_000_001],
                )
            )
            bootstrap = self.position_reconcile.pre_live_position_reconcile("alpaca", con=self.con)
            self.assertFalse(bool(bootstrap.get("ok")))
            self.assertEqual(str(bootstrap.get("status") or ""), "baseline_bootstrapped_re_reconcile_pending")
            self.assertTrue(bool(bootstrap.get("re_reconcile_pending")))

            _mute_router_side_effects(stack, self.broker_router)
            stack.enter_context(patch.object(self.broker_router, "_execution_gate_or_block", return_value=None))
            stack.enter_context(patch.object(self.broker_router, "_real_trading_gate_or_block", return_value=None))
            stack.enter_context(patch.object(self.broker_router, "_prelive_reconcile", _prelive_with_shared_connection))
            stack.enter_context(patch.object(self.broker_router, "_alpaca_apply", adapter))

            result = self.broker_router.apply_new_portfolio_orders_router(
                dry_run=False,
                override_orders=[{"symbol": "AAPL", "action": "BUY", "qty": 1.0}],
                override_order_id=101,
                override_ts_ms=1_710_000_000_000,
            )

        self.assertTrue(bool(result.get("ok")))
        adapter.assert_called_once()
        self.assertEqual(broker_positions.call_count, 2)

        baseline_row = self.con.execute(
            "SELECT positions_json FROM position_reconcile_baseline WHERE broker=?",
            ("alpaca",),
        ).fetchone()
        state_row = self.con.execute(
            "SELECT re_reconcile_pending FROM position_reconcile_state WHERE broker=?",
            ("alpaca",),
        ).fetchone()
        audit_rows = self.con.execute(
            """
            SELECT actor, status
            FROM position_reconcile_bootstrap_audit
            WHERE broker=?
            ORDER BY id ASC
            """,
            ("alpaca",),
        ).fetchall()

        self.assertIsNotNone(baseline_row)
        baseline = json.loads(str(baseline_row[0] or "{}"))
        self.assertAlmostEqual(float(baseline.get("AAPL") or 0.0), 2.0)
        self.assertIsNotNone(state_row)
        self.assertEqual(int(state_row[0] or 0), 0)
        self.assertEqual(
            [(str(actor), str(status)) for actor, status in audit_rows],
            [
                ("test-operator", "baseline_bootstrapped"),
                ("position_reconcile", "re_reconcile_completed"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
