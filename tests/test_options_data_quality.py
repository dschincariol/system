import importlib
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.data import options_data_quality as odq


NOW_MS = 1_700_000_000_000


def _connect():
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE options_symbol_ingestion_state (
          symbol TEXT NOT NULL PRIMARY KEY,
          provider TEXT NOT NULL DEFAULT '',
          consecutive_failures INTEGER NOT NULL DEFAULT 0,
          total_failures INTEGER NOT NULL DEFAULT 0,
          last_failure_ts_ms INTEGER,
          last_failure_error TEXT,
          last_success_ts_ms INTEGER,
          last_fresh_snapshot_ts_ms INTEGER,
          last_cached_snapshot_ts_ms INTEGER,
          last_fallback_ts_ms INTEGER,
          last_row_count INTEGER NOT NULL DEFAULT 0,
          disabled_until_ts_ms INTEGER NOT NULL DEFAULT 0,
          updated_ts_ms INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    con.execute(
        """
        CREATE TABLE options_chain_v2 (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          underlying TEXT NOT NULL,
          contract TEXT NOT NULL,
          expiration TEXT,
          contract_type TEXT,
          strike REAL,
          iv REAL,
          open_interest REAL,
          volume REAL,
          bid REAL,
          ask REAL,
          delta REAL,
          gamma REAL,
          theta REAL,
          vega REAL,
          source TEXT,
          payload_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE options_chain (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          expiry TEXT,
          strike REAL,
          call_put TEXT,
          iv REAL,
          open_interest REAL,
          volume REAL,
          source TEXT,
          payload_json TEXT
        )
        """
    )
    return con


def _seed_state(con, symbol, *, ts_ms=NOW_MS - 1_000, provider="polygon"):
    con.execute(
        """
        INSERT INTO options_symbol_ingestion_state (
          symbol, provider, last_success_ts_ms, last_fresh_snapshot_ts_ms,
          last_cached_snapshot_ts_ms, last_row_count, updated_ts_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (symbol, provider, ts_ms, ts_ms, ts_ms, 1, ts_ms),
    )


def _seed_v2(
    con,
    symbol,
    *,
    ts_ms=NOW_MS - 1_000,
    contract="O:SPY260116C00500000",
    iv=0.25,
    bid=1.20,
    ask=1.30,
    delta=0.50,
    gamma=0.03,
    theta=-0.01,
    vega=0.15,
    source="polygon",
):
    con.execute(
        """
        INSERT INTO options_chain_v2 (
          ts_ms, underlying, contract, expiration, contract_type, strike,
          iv, open_interest, volume, bid, ask, delta, gamma, theta, vega, source
        )
        VALUES (?, ?, ?, '2026-01-16', 'call', 500.0, ?, 100.0, 25.0, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ts_ms, symbol, contract, iv, bid, ask, delta, gamma, theta, vega, source),
    )


def _seed_legacy(con, symbol, *, ts_ms=NOW_MS - 1_000, source="tradier"):
    con.execute(
        """
        INSERT INTO options_chain (
          ts_ms, symbol, expiry, strike, call_put, iv, open_interest, volume, source
        )
        VALUES (?, ?, '2026-01-16', 500.0, 'C', 0.31, 100.0, 20.0, ?)
        """,
        (ts_ms, symbol, source),
    )


class RecordingConnection:
    def __init__(self, con):
        self.con = con
        self.protected_writes = []

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).lower().split())
        if normalized.startswith(("insert", "update", "delete", "replace")):
            for table in ("options_chain_v2", "options_chain", "options_symbol_ingestion_state"):
                if table in normalized:
                    self.protected_writes.append(normalized)
        return self.con.execute(sql, params)


class OptionsDataQualityTests(unittest.TestCase):
    def test_polygon_full_snapshot_is_available_and_ok(self):
        con = _connect()
        _seed_state(con, "SPY")
        _seed_v2(con, "SPY")

        report = odq.compute_options_data_quality(con, now_ms=NOW_MS, symbols=["SPY"])

        self.assertTrue(report["available"])
        self.assertTrue(report["ok"])
        self.assertFalse(report["degraded"])
        self.assertEqual(report["coverage_fraction"], 1.0)
        polygon = report["providers"]["polygon"]
        self.assertEqual(polygon["greeks_complete_fraction"], 1.0)
        self.assertEqual(polygon["bid_ask_complete_fraction"], 1.0)

    def test_legacy_snapshot_surfaces_missing_bid_ask_and_greeks(self):
        con = _connect()
        _seed_state(con, "QQQ", provider="tradier")
        _seed_legacy(con, "QQQ", source="tradier")

        report = odq.compute_options_data_quality(con, now_ms=NOW_MS, symbols=["QQQ"])

        tradier = report["providers"]["tradier"]
        self.assertEqual(tradier["iv_complete_fraction"], 1.0)
        self.assertEqual(tradier["greeks_complete_fraction"], 0.0)
        self.assertEqual(tradier["bid_ask_complete_fraction"], 0.0)
        self.assertTrue(tradier["legacy_missing_bid_ask_greeks"])

    def test_iv_sanity_counts_negative_absurd_and_zero_greeks_rows(self):
        con = _connect()
        _seed_state(con, "SPY")
        _seed_v2(con, "SPY", contract="O:SPY260116C00400000", iv=-1.0)
        _seed_v2(con, "SPY", contract="O:SPY260116C00450000", iv=99.0)
        _seed_v2(
            con,
            "SPY",
            contract="O:SPY260116C00500000",
            iv=0.20,
            delta=0.0,
            gamma=0.0,
            theta=0.0,
            vega=0.0,
        )

        report = odq.compute_options_data_quality(con, now_ms=NOW_MS, symbols=["SPY"])

        self.assertEqual(report["iv_sanity"]["iv_negative_rows"], 1)
        self.assertEqual(report["iv_sanity"]["iv_absurd_rows"], 1)
        self.assertEqual(report["iv_sanity"]["zero_greeks_rows"], 1)
        self.assertTrue(report["degraded"])

    def test_empty_tables_are_unavailable_not_green(self):
        con = _connect()

        report = odq.compute_options_data_quality(con, now_ms=NOW_MS, symbols=[])

        self.assertFalse(report["available"])
        self.assertFalse(report["ok"])
        self.assertTrue(report["degraded"])
        self.assertEqual(report["coverage_fraction"], 0.0)

    def test_coverage_fraction_uses_configured_universe(self):
        con = _connect()
        _seed_state(con, "SPY")
        _seed_v2(con, "SPY")

        report = odq.compute_options_data_quality(
            con,
            now_ms=NOW_MS,
            symbols=["SPY", "QQQ", "IWM", "DIA"],
        )

        self.assertEqual(report["fresh_underlyings"], 1)
        self.assertEqual(report["universe_underlyings"], 4)
        self.assertEqual(report["coverage_fraction"], 0.25)
        self.assertTrue(report["degraded"])

    def test_options_data_quality_ok_helper(self):
        self.assertTrue(odq.options_data_quality_ok({"available": True, "ok": True, "degraded": False}))
        self.assertFalse(odq.options_data_quality_ok({"available": True, "ok": False, "degraded": True}))
        self.assertFalse(odq.options_data_quality_ok({"available": False, "ok": True, "degraded": False}))

    def test_degradation_event_uses_normalized_options_event_path_once(self):
        con = _connect()
        _seed_state(con, "SPY")
        _seed_v2(con, "SPY")
        report = odq.compute_options_data_quality(
            con,
            now_ms=NOW_MS,
            symbols=["SPY", "QQQ", "IWM", "DIA"],
        )
        recording = RecordingConnection(con)
        emitted_payloads = []
        odq._LAST_DQ_DEGRADATION_EVENT_TS_MS = 0

        from engine.data import options_features

        def fake_put_normalized_event(payload, con=None):
            emitted_payloads.append(payload)
            return 123

        def fake_run_write_txn(fn, *args, **kwargs):
            self.assertEqual(kwargs.get("table"), "events")
            return fn(recording)

        with mock.patch.object(options_features, "put_normalized_event", side_effect=fake_put_normalized_event):
            with mock.patch.object(options_features, "run_write_txn", side_effect=fake_run_write_txn):
                first = odq.emit_options_data_quality_degradation_event(report, now_ms=NOW_MS)
                second = odq.emit_options_data_quality_degradation_event(report, now_ms=NOW_MS + 1)

        self.assertEqual(first["events"], 1)
        self.assertEqual(second["events"], 0)
        self.assertTrue(second["throttled"])
        self.assertEqual(len(emitted_payloads), 1)
        payload = emitted_payloads[0]
        self.assertEqual(payload["event_type"], "options")
        self.assertEqual(payload["raw_payload"]["event_kind"], "options_data_quality_degraded")
        self.assertEqual(payload["derived_features"]["options_event_kind"], "options_data_quality_degraded")
        self.assertEqual(recording.protected_writes, [])

    def test_degradation_event_writes_against_runtime_events_schema(self):
        report = {
            "ts_ms": NOW_MS,
            "available": True,
            "ok": False,
            "degraded": True,
            "coverage_fraction": 0.25,
            "fresh_underlyings": 1,
            "universe_underlyings": 4,
            "reason_codes": ["coverage_below_min"],
            "thresholds": {"min_coverage": 0.75},
            "iv_sanity": {"iv_negative_rows": 0, "iv_absurd_rows": 0, "zero_greeks_rows": 0},
            "completeness_failures": [],
        }
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "options_dq.sqlite"
            env = {
                "DB_PATH": str(db_path),
                "TS_STORAGE_BACKEND": "sqlite",
                "TIMESCALE_ENABLED": "0",
                "SQLITE_LIVENESS_DB_ENABLED": "0",
                "SQLITE_LIVENESS_QUEUE_ENABLED": "0",
                "FEATURE_STORE_ENABLED": "0",
                "FEATURE_STORE_INIT_ON_STARTUP": "0",
            }
            with mock.patch.dict(os.environ, env):
                storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
                importlib.reload(importlib.import_module("engine.data.options_features"))
                storage.init_db()
                odq._LAST_DQ_DEGRADATION_EVENT_TS_MS = 0

                result = odq.emit_options_data_quality_degradation_event(report, now_ms=NOW_MS)

                self.assertEqual(result.get("events"), 1, result)
                con = sqlite3.connect(str(db_path))
                try:
                    row = con.execute(
                        """
                        SELECT raw_payload, derived_features, source_id, event_key
                        FROM events
                        WHERE event_type='options'
                        """
                    ).fetchone()
                finally:
                    con.close()
                storage.close_pooled_connections()

            importlib.reload(importlib.import_module("engine.runtime.storage"))
            importlib.reload(importlib.import_module("engine.data.options_features"))

        self.assertIsNotNone(row)
        raw_payload = json.loads(str(row[0] or "{}"))
        derived_features = json.loads(str(row[1] or "{}"))
        self.assertEqual(raw_payload["event_kind"], "options_data_quality_degraded")
        self.assertEqual(derived_features["options_event_kind"], "options_data_quality_degraded")
        self.assertTrue(str(row[2]).startswith("options_data_quality:options_data_quality_degraded:"))
        self.assertTrue(str(row[3]).startswith("options:options_data_quality_degraded:"))


if __name__ == "__main__":
    unittest.main()
