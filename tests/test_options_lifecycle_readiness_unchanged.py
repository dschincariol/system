from __future__ import annotations

from datetime import datetime, timezone
import importlib
import os
from pathlib import Path
import sys
import tempfile
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _ms(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


def _reload(name: str):
    module = importlib.import_module(name)
    return importlib.reload(module)


def _option_order() -> dict[str, object]:
    return {
        "symbol": "SPY270115C00500000",
        "option_contract": "SPY270115C00500000",
        "instrument_type": "option",
        "qty": 1,
    }


class OptionsLifecycleReadinessUnchangedTest(unittest.TestCase):
    ENV_KEYS = (
        "OPTIONS_INSTRUMENTS_MODE",
        "OPTIONS_LIVE_ORDERS_ENABLED",
        "OPTIONS_LIVE_ASSIGNMENT_EXERCISE_READY",
        "OPTIONS_ASSIGNMENT_EXERCISE_HANDLING_ENABLED",
        "OPTIONS_LIVE_EXPIRATION_RISK_READY",
        "OPTIONS_EXPIRATION_RISK_ENABLED",
        "OPTIONS_LIFECYCLE_ENABLED",
        "DB_PATH",
        "TS_TESTING",
        "TS_STORAGE_BACKEND",
        "BROKER_START_CASH",
        "BROKER_START_EQUITY",
    )

    def setUp(self) -> None:
        self.env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        try:
            if "engine.runtime.storage" in sys.modules:
                _reload("engine.runtime.storage").close_pooled_connections()
        except Exception:
            pass
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
            if self.env_backup[key] is not None:
                os.environ[key] = str(self.env_backup[key])

    def test_live_readiness_gates_and_shadow_intent_stay_unchanged(self) -> None:
        os.environ["OPTIONS_INSTRUMENTS_MODE"] = "live"
        os.environ["OPTIONS_LIVE_ORDERS_ENABLED"] = "1"
        readiness = _reload("engine.execution.options_readiness")
        lifecycle = _reload("engine.execution.options_lifecycle")

        state = readiness.live_options_readiness_snapshot(
            engine_mode="live",
            execution_mode="live",
            broker="alpaca",
            orders=[_option_order()],
        )
        shadow_intent = readiness.force_options_shadow_intent(_option_order())
        evidence = lifecycle.lifecycle_readiness_evidence({"OPTIONS_LIFECYCLE_ENABLED": "1"})

        self.assertEqual(readiness.LIVE_OPTIONS_BROKER_ADAPTERS, frozenset({"tradier_options"}))
        self.assertFalse(state["ok"])
        self.assertIn("options_live_assignment_exercise_missing", state["blockers"])
        self.assertIn("options_live_expiration_risk_missing", state["blockers"])
        self.assertIn("options_live_broker_adapter_missing:alpaca", state["blockers"])
        self.assertTrue(readiness.is_options_order(_option_order()))
        self.assertEqual(shadow_intent["execution_target"], "shadow")
        self.assertTrue(evidence["implemented"])
        self.assertFalse(evidence["live_order_authority"])
        self.assertTrue(evidence["shadow_only"])

    def test_non_option_position_is_skipped_by_lifecycle_applier(self) -> None:
        tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(tmp.cleanup)
        os.environ["DB_PATH"] = str(Path(tmp.name) / "non_option_lifecycle.db")
        os.environ["TS_TESTING"] = "1"
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["BROKER_START_CASH"] = "100000"
        os.environ["BROKER_START_EQUITY"] = "100000"
        os.environ["OPTIONS_LIFECYCLE_ENABLED"] = "1"
        storage = _reload("engine.runtime.storage")
        broker_sim = _reload("engine.execution.broker_sim")
        storage.init_db()
        broker_sim.init_broker_db()
        now_ms = _ms(2027, 1, 16)

        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO broker_shadow_account(book_key, cash, equity, updated_ts_ms)
                VALUES(?,?,?,?)
                """,
                ("non_option", 100000.0, 100000.0, now_ms - 1),
            )
            con.execute(
                """
                INSERT INTO broker_shadow_positions(book_key, symbol, qty, avg_px, updated_ts_ms)
                VALUES(?,?,?,?,?)
                """,
                ("non_option", "AAPL", 5.0, 100.0, now_ms - 1),
            )
            con.commit()
            summary = broker_sim.apply_option_lifecycle(con, book_key="non_option", now_ms=now_ms)
        finally:
            con.close()

        read_con = storage.connect(readonly=True)
        try:
            qty = read_con.execute(
                "SELECT qty FROM broker_shadow_positions WHERE book_key=? AND symbol=?",
                ("non_option", "AAPL"),
            ).fetchone()[0]
            fills = read_con.execute(
                "SELECT COUNT(*) FROM broker_fills WHERE book_key=?",
                ("non_option",),
            ).fetchone()[0]
        finally:
            read_con.close()

        self.assertTrue(summary["ok"])
        self.assertEqual(summary["processed"], 0)
        self.assertEqual(float(qty), 5.0)
        self.assertEqual(int(fills), 0)


if __name__ == "__main__":
    unittest.main()
