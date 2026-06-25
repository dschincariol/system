from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_storage_sqlite(db_path: Path):
    env = {
        "DB_PATH": str(db_path),
        "TS_STORAGE_BACKEND": "sqlite",
        "TS_TESTING": "1",
        "TIMESCALE_ENABLED": "0",
        "SQLITE_LIVENESS_DB_ENABLED": "0",
        "SQLITE_LIVENESS_QUEUE_ENABLED": "0",
    }
    with patch.dict(os.environ, env, clear=False):
        return importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))


class FuturesInstrumentMetadataStorageTests(unittest.TestCase):
    def test_futures_instrument_metadata_persists_with_sqlite_affinity(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage_sqlite = _reload_storage_sqlite(Path(tmpdir) / "futures_instrument_metadata.db")
            storage_sqlite.init_db()

            import engine.data.universe as universe

            universe = importlib.reload(universe)
            con = storage_sqlite.connect_rw_direct()
            try:
                universe.upsert_symbol(con, "ES.c.0", status="ACTIVE", score_delta=1.0)
                universe.upsert_symbol(con, "EURUSD", status="ACTIVE", score_delta=1.0)
                universe.upsert_symbol(con, "SPY")
                con.commit()

                row = con.execute(
                    """
                    SELECT symbol, asset_class, instrument_kind, fut_root, fut_exchange,
                           fut_multiplier, fut_tick_size, fut_tick_value, fut_price_ccy,
                           fut_margin_ref, fut_expiry_rule, fut_roll_method,
                           fut_continuous_alias, session_calendar, instrument_meta_source
                    FROM symbols
                    WHERE symbol='ES.C.0'
                    """
                ).fetchone()

                self.assertIsNone(row)

                row = con.execute(
                    """
                    SELECT symbol, asset_class, instrument_kind, fut_root, fut_exchange,
                           fut_multiplier, fut_tick_size, fut_tick_value, fut_price_ccy,
                           fut_margin_ref, fut_expiry_rule, fut_roll_method,
                           fut_continuous_alias, session_calendar, instrument_meta_source
                    FROM symbols
                    WHERE symbol='ES.c.0'
                    """
                ).fetchone()

                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row[0], "ES.c.0")
                self.assertEqual(row[1], "FUTURES")
                self.assertEqual(row[2], "fut_continuous")
                self.assertIsInstance(row[3], str)
                self.assertEqual(row[3], "ES")
                self.assertEqual(row[4], "CME")
                self.assertIsInstance(row[5], float)
                self.assertEqual(row[5], 50.0)
                self.assertIsInstance(row[6], float)
                self.assertEqual(row[6], 0.25)
                self.assertIsInstance(row[7], float)
                self.assertEqual(row[7], 12.5)
                self.assertEqual(row[8], "USD")
                self.assertIsInstance(row[9], float)
                self.assertTrue(row[10])
                self.assertEqual(row[11], "oi_volume")
                self.assertEqual(row[12], "ES.c.0")
                self.assertEqual(row[13], "CME_EQUITY")
                self.assertEqual(row[14], "parser")

                table_info = {
                    item[1]: item[2].upper() for item in con.execute("PRAGMA table_info(symbols)").fetchall()
                }
                self.assertEqual(table_info["fut_multiplier"], "REAL")
                self.assertEqual(table_info["fut_tick_size"], "REAL")
                self.assertEqual(table_info["fut_tick_value"], "REAL")
                self.assertEqual(table_info["fut_margin_ref"], "REAL")
                self.assertEqual(table_info["fut_root"], "TEXT")

                futures_metadata = universe.get_instrument_metadata(con, "ES.c.0")
                self.assertIsNotNone(futures_metadata)
                assert futures_metadata is not None
                self.assertEqual(futures_metadata["asset_class"], "FUTURES")
                self.assertEqual(futures_metadata["symbol"], "ES.c.0")
                self.assertEqual(futures_metadata["root"], "ES")
                self.assertEqual(futures_metadata["multiplier"], 50.0)
                self.assertEqual(futures_metadata["tick_value"], 12.5)

                fx_metadata = universe.get_instrument_metadata(con, "EURUSD")
                self.assertIsNotNone(fx_metadata)
                assert fx_metadata is not None
                self.assertEqual(fx_metadata["asset_class"], "FX")
                self.assertEqual(fx_metadata["symbol"], "EURUSD")
                self.assertEqual(fx_metadata["base_ccy"], "EUR")
                self.assertEqual(fx_metadata["quote_ccy"], "USD")

                self.assertIsNone(universe.get_instrument_metadata(con, "SPY"))

                spy_row = con.execute(
                    """
                    SELECT fut_root, fut_exchange, fut_multiplier, fut_tick_size,
                           fut_tick_value, fut_price_ccy, fut_margin_ref, fut_expiry_rule,
                           fut_roll_method, fut_continuous_alias
                    FROM symbols
                    WHERE symbol='SPY'
                    """
                ).fetchone()
                self.assertIsNotNone(spy_row)
                self.assertEqual(tuple(spy_row), (None, None, None, None, None, None, None, None, None, None))

                snapshot = universe.get_universe_snapshot(con)
                self.assertTrue(any(item["symbol"] == "ES.c.0" for item in snapshot))
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
