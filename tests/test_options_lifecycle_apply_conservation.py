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


CALL = "SPY270115C00500000"
BOOK = "lifecycle_shadow"


def _ms(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


def _reload(name: str):
    module = importlib.import_module(name)
    return importlib.reload(module)


class OptionsLifecycleApplyConservationTest(unittest.TestCase):
    ENV_KEYS = (
        "DB_PATH",
        "TS_TESTING",
        "TS_STORAGE_BACKEND",
        "BROKER_START_CASH",
        "BROKER_START_EQUITY",
        "BROKER_MAX_PRICE_AGE_MS",
        "OPTIONS_LIFECYCLE_ENABLED",
        "OPTIONS_PIN_RISK_BAND_ABS",
        "OPTIONS_MIN_DTE_DAYS",
    )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "options_lifecycle.db"
        self.env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["TS_TESTING"] = "1"
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["BROKER_START_CASH"] = "100000"
        os.environ["BROKER_START_EQUITY"] = "100000"
        os.environ["BROKER_MAX_PRICE_AGE_MS"] = "864000000"
        os.environ["OPTIONS_PIN_RISK_BAND_ABS"] = "0"
        os.environ["OPTIONS_MIN_DTE_DAYS"] = "0"

        self.storage = _reload("engine.runtime.storage")
        self.broker_sim = _reload("engine.execution.broker_sim")
        self.storage.init_db()
        self.broker_sim.init_broker_db()
        self.now_ms = _ms(2027, 1, 16)

    def tearDown(self) -> None:
        try:
            _reload("engine.runtime.storage").close_pooled_connections()
        except Exception:
            pass
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _seed_price(self, symbol: str, px: float) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO prices(ts_ms, symbol, price, px, source)
                VALUES(?,?,?,?,?)
                ON CONFLICT(symbol, ts_ms) DO UPDATE SET
                  price=excluded.price,
                  px=excluded.px,
                  source=excluded.source
                """,
                (int(self.now_ms), str(symbol), float(px), float(px), "test"),
            )
            con.commit()
        finally:
            con.close()

    def _seed_shadow(self, *, qty: float, cash: float = 100000.0, equity: float = 100000.0) -> None:
        con = self.storage.connect()
        try:
            con.execute(
                """
                INSERT INTO broker_shadow_account(book_key, cash, equity, updated_ts_ms)
                VALUES(?,?,?,?)
                ON CONFLICT(book_key) DO UPDATE SET
                  cash=excluded.cash,
                  equity=excluded.equity,
                  updated_ts_ms=excluded.updated_ts_ms
                """,
                (BOOK, float(cash), float(equity), int(self.now_ms - 1)),
            )
            con.execute(
                """
                INSERT INTO broker_shadow_positions(book_key, symbol, qty, avg_px, updated_ts_ms)
                VALUES(?,?,?,?,?)
                ON CONFLICT(book_key, symbol) DO UPDATE SET
                  qty=excluded.qty,
                  avg_px=excluded.avg_px,
                  updated_ts_ms=excluded.updated_ts_ms
                """,
                (BOOK, CALL, float(qty), 4.0, int(self.now_ms - 1)),
            )
            con.commit()
        finally:
            con.close()

    def _snapshot(self) -> dict[str, object]:
        con = self.storage.connect(readonly=True)
        try:
            account = con.execute(
                "SELECT cash, equity, updated_ts_ms FROM broker_shadow_account WHERE book_key=?",
                (BOOK,),
            ).fetchone()
            positions = con.execute(
                """
                SELECT symbol, qty, avg_px, updated_ts_ms
                FROM broker_shadow_positions
                WHERE book_key=?
                ORDER BY symbol
                """,
                (BOOK,),
            ).fetchall()
            fills = con.execute(
                """
                SELECT symbol, qty, px, note, book_key, source, contract_multiplier
                FROM broker_fills
                WHERE book_key=?
                ORDER BY id
                """,
                (BOOK,),
            ).fetchall()
            return {"account": account, "positions": positions, "fills": fills}
        finally:
            con.close()

    def test_long_call_exercise_cash_settles_and_flattens_shadow_position(self) -> None:
        os.environ["OPTIONS_LIFECYCLE_ENABLED"] = "1"
        self._seed_price("SPY", 512.0)
        self._seed_shadow(qty=2.0)

        con = self.storage.connect()
        try:
            summary = self.broker_sim.apply_option_lifecycle(con, book_key=BOOK, now_ms=self.now_ms)
        finally:
            con.close()

        snap = self._snapshot()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["processed"], 1)
        self.assertAlmostEqual(snap["account"][0], 102400.0)
        self.assertAlmostEqual(snap["account"][1], 102400.0)
        self.assertAlmostEqual(snap["positions"][0][1], 0.0)
        self.assertAlmostEqual(snap["fills"][0][1], -2.0)
        self.assertAlmostEqual(snap["fills"][0][2], 12.0)
        self.assertEqual(snap["fills"][0][3], "options_lifecycle:EXERCISE")
        self.assertAlmostEqual(snap["fills"][0][6], 100.0)

    def test_short_call_assignment_cash_settles_per_stated_model(self) -> None:
        os.environ["OPTIONS_LIFECYCLE_ENABLED"] = "1"
        self._seed_price("SPY", 510.0)
        self._seed_shadow(qty=-1.0)

        con = self.storage.connect()
        try:
            summary = self.broker_sim.apply_option_lifecycle(con, book_key=BOOK, now_ms=self.now_ms)
        finally:
            con.close()

        snap = self._snapshot()
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["processed"], 1)
        self.assertAlmostEqual(snap["account"][0], 99000.0)
        self.assertAlmostEqual(snap["account"][1], 99000.0)
        self.assertAlmostEqual(snap["positions"][0][1], 0.0)
        self.assertAlmostEqual(snap["fills"][0][1], 1.0)
        self.assertAlmostEqual(snap["fills"][0][2], 10.0)
        self.assertEqual(snap["fills"][0][3], "options_lifecycle:ASSIGN")

    def test_disabled_lifecycle_is_byte_identical_noop(self) -> None:
        os.environ.pop("OPTIONS_LIFECYCLE_ENABLED", None)
        self._seed_price("SPY", 512.0)
        self._seed_shadow(qty=2.0)
        before = self._snapshot()

        con = self.storage.connect()
        try:
            summary = self.broker_sim.apply_option_lifecycle(con, book_key=BOOK, now_ms=self.now_ms)
        finally:
            con.close()

        after = self._snapshot()
        self.assertEqual(summary, {"ok": True, "processed": 0, "skipped_disabled": True})
        self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
