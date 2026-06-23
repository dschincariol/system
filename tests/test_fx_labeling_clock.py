from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import unittest
import uuid
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


NY = ZoneInfo("America/New_York")


def _ms_et(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=NY).timestamp() * 1000)


def _series(event_ts: int) -> list[dict]:
    return [
        {"ts_ms": event_ts + 1, "price": 1.0},
        {"ts_ms": event_ts + 300_000 + 1, "price": 1.01},
        {"ts_ms": event_ts + 3_600_000 + 1, "price": 1.02},
        {"ts_ms": _ms_et(2026, 1, 11, 17, 30) + 1, "price": 1.03},
    ]


def _init_db(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(
            """
            CREATE TABLE labels(
              event_id INTEGER,
              horizon_s INTEGER,
              symbol TEXT,
              baseline_ret REAL,
              realized_ret REAL,
              impact_z REAL,
              created_at_ms INTEGER,
              vol_proxy REAL,
              regime TEXT,
              PRIMARY KEY(event_id, horizon_s, symbol)
            )
            """
        )
        con.commit()
    finally:
        con.close()


class FxLabelingClockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "labels.db"
        _init_db(self.db_path)
        self.labeling = importlib.reload(importlib.import_module("engine.strategy.labeling"))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _rows(self) -> list[tuple]:
        con = sqlite3.connect(self.db_path)
        try:
            return con.execute(
                "SELECT symbol, horizon_s, realized_ret, regime FROM labels ORDER BY symbol, horizon_s"
            ).fetchall()
        finally:
            con.close()

    def test_fx_gap_window_skips_while_equity_keeps_naive_behavior(self) -> None:
        canary = "CANARY-" + uuid.uuid4().hex
        os.environ["FX04_SECRET_SHAPED_VALUE"] = canary
        self.addCleanup(os.environ.pop, "FX04_SECRET_SHAPED_VALUE", None)
        event_ts = _ms_et(2026, 1, 9, 16, 30)

        with mock.patch.object(self.labeling, "connect", side_effect=lambda: sqlite3.connect(self.db_path)):
            self.labeling.label_event(101, event_ts, {"EURUSD": _series(event_ts)})
            self.labeling.label_event(102, event_ts, {"SPY": _series(event_ts)})

        rows = self._rows()
        horizons_by_symbol = {}
        for symbol, horizon_s, *_ in rows:
            horizons_by_symbol.setdefault(symbol, []).append(int(horizon_s))

        self.assertEqual(horizons_by_symbol["EURUSD"], [300])
        self.assertEqual(horizons_by_symbol["SPY"], [300, 3600])
        self.assertNotIn(canary, repr(rows))


if __name__ == "__main__":
    unittest.main()
