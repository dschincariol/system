from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os
import sqlite3
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _ms(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=timezone.utc).timestamp() * 1000)


def _seed_chain(con: sqlite3.Connection, *, ts_ms: int) -> None:
    con.execute(
        """
        CREATE TABLE options_chain_v2(
          ts_ms INTEGER,
          underlying TEXT,
          contract TEXT,
          expiration TEXT,
          contract_type TEXT,
          strike REAL,
          iv REAL,
          open_interest REAL,
          volume REAL,
          delta REAL,
          gamma REAL
        )
        """
    )
    rows = [
        ("SPY270220P00495000", "2027-02-20", "put", 495.0, 0.35, 500, 100, -0.30, 0.01),
        ("SPY270220P00485000", "2027-02-20", "put", 485.0, 0.33, 450, 90, -0.12, 0.01),
        ("SPY270220C00510000", "2027-02-20", "call", 510.0, 0.34, 500, 100, 0.30, 0.01),
        ("SPY270220C00520000", "2027-02-20", "call", 520.0, 0.32, 400, 80, 0.11, 0.01),
    ]
    for contract, expiration, contract_type, strike, iv, oi, volume, delta, gamma in rows:
        con.execute(
            """
            INSERT INTO options_chain_v2(
              ts_ms, underlying, contract, expiration, contract_type, strike,
              iv, open_interest, volume, delta, gamma
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(ts_ms), "SPY", contract, expiration, contract_type, strike, iv, oi, volume, delta, gamma),
        )


class OptionsPredictorSelectionTest(unittest.TestCase):
    ENV_KEYS = ("OPTIONS_MIN_DTE_DAYS", "OPTIONS_MAX_DTE_DAYS", "OPTIONS_PRED_TARGET_DELTA")

    def setUp(self) -> None:
        self.env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}
        os.environ["OPTIONS_MIN_DTE_DAYS"] = "20"
        os.environ["OPTIONS_MAX_DTE_DAYS"] = "60"
        os.environ["OPTIONS_PRED_TARGET_DELTA"] = "0.30"

    def tearDown(self) -> None:
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_credit_structure_selects_delta_band_and_occ_contracts(self) -> None:
        from engine.data.options_instrument import parse_option_symbol
        from engine.strategy.options_predictor import select_option_structure

        con = sqlite3.connect(":memory:")
        now_ms = _ms(2027, 1, 15)
        _seed_chain(con, ts_ms=now_ms)

        structure = select_option_structure(
            con,
            underlying="SPY",
            vrp_signal=0.75,
            directional_view=0.25,
            ts_ms=now_ms,
        )

        self.assertIsNotNone(structure)
        assert structure is not None
        self.assertEqual(structure["structure_type"], "put_credit_vertical")
        self.assertEqual([leg["side"] for leg in structure["legs"]], ["SELL", "BUY"])
        self.assertLessEqual(abs(abs(structure["legs"][0]["delta"]) - 0.30), 0.05)
        for leg in structure["legs"]:
            self.assertGreaterEqual(leg["dte"], 20.0)
            self.assertLessEqual(leg["dte"], 60.0)
            parsed = parse_option_symbol(leg["contract_symbol"])
            self.assertIsNotNone(parsed)
            assert parsed is not None
            self.assertEqual(parsed.underlying, "SPY")

    def test_empty_chain_returns_none(self) -> None:
        from engine.strategy.options_predictor import select_option_structure

        con = sqlite3.connect(":memory:")
        con.execute(
            """
            CREATE TABLE options_chain_v2(
              ts_ms INTEGER,
              underlying TEXT,
              contract TEXT,
              expiration TEXT,
              contract_type TEXT,
              strike REAL,
              iv REAL,
              open_interest REAL,
              volume REAL,
              delta REAL,
              gamma REAL
            )
            """
        )

        self.assertIsNone(
            select_option_structure(
                con,
                underlying="SPY",
                vrp_signal=0.75,
                directional_view=0.25,
                ts_ms=_ms(2027, 1, 15),
            )
        )


if __name__ == "__main__":
    unittest.main()
