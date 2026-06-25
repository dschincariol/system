"""Regression tests for hardened options ingestion reliability paths."""

from __future__ import annotations

import importlib
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
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


class _FakeResponse:
    def __init__(self, status_code: int, payload=None, *, headers=None, text: str = "") -> None:
        self.status_code = int(status_code)
        self._payload = payload
        self.headers = dict(headers or {})
        self.text = str(text)

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses) -> None:
        self._responses = list(responses)

    def get(self, *_args, **_kwargs):
        if not self._responses:
            raise AssertionError("unexpected extra request")
        return self._responses.pop(0)

    def close(self) -> None:
        return None


class _CountingConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.closed = False
        self.executemany_batches = []

    def commit(self) -> None:
        self.commits += 1

    def executemany(self, sql, seq_of_params):
        params = [tuple(row) for row in list(seq_of_params or [])]
        self.executemany_batches.append({"sql": str(sql or ""), "count": len(params), "rows": params})
        return _FakeCursor([])

    def close(self) -> None:
        self.closed = True
        return None


class _FakeCursor:
    def __init__(self, rows=None) -> None:
        self._rows = list(rows or [])

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _InstrumentedOptionsConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.execute_calls = []
        self.executemany_batches = []
        self.state_load_queries = 0
        self.state_load_param_counts = []

    def execute(self, sql, params=None):
        text = str(sql or "")
        normalized = " ".join(text.lower().split())
        param_tuple = tuple(params or ())
        self.execute_calls.append((text, param_tuple))
        if "from options_symbol_ingestion_state" in normalized and "where symbol in" in normalized:
            self.state_load_queries += 1
            self.state_load_param_counts.append(len(param_tuple))
            return _FakeCursor([])
        if "select max(ts_ms)" in normalized and "from options_chain" in normalized:
            return _FakeCursor([(None,)])
        return _FakeCursor([])

    def executemany(self, sql, seq_of_params):
        params = [tuple(row) for row in list(seq_of_params or [])]
        self.executemany_batches.append({"sql": str(sql or ""), "count": len(params), "rows": params})
        return _FakeCursor([])

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1
        return None

    def close(self) -> None:
        self.closed = True


def _tradier_success_rows():
    return {
        "rows": [
            {
                "expiry": "2026-04-17",
                "strike": 500.0,
                "call_put": "C",
                "iv": 0.2,
                "open_interest": 10,
                "volume": 5,
            }
        ]
    }


def _polygon_success_contracts(symbol: str):
    return (
        [
            {
                "ts_ms": 1_000_000,
                "underlying": str(symbol),
                "contract": f"O:{symbol}260417C00500000",
                "expiration": "2026-04-17",
                "contract_type": "call",
                "strike": 500.0,
                "iv": 0.2,
                "open_interest": 10,
                "volume": 5,
                "bid": 1.0,
                "ask": 1.1,
                "source": "polygon",
            }
        ],
        None,
    )


def _bulk_write_counts(_con, *, polygon_rows=None, tradier_rows=None):
    polygon_n = len(list(polygon_rows or []))
    tradier_n = len(list(tradier_rows or []))
    return {"polygon_rows": polygon_n, "tradier_rows": tradier_n, "raw_rows": polygon_n + tradier_n}


class OptionsIngestionReliabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "options_reliability.db"
        self._env_backup = {
            "DB_PATH": os.environ.get("DB_PATH"),
            "TS_STORAGE_BACKEND": os.environ.get("TS_STORAGE_BACKEND"),
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ["TRADIER_API_TOKEN"] = "token"
        os.environ["OPTIONS_PROVIDER_CHAIN"] = "tradier"
        os.environ["OPTIONS_CRITICAL_SYMBOLS"] = "SPY"
        os.environ["OPTIONS_SYMBOL_FAILURE_THRESHOLD"] = "2"
        os.environ["OPTIONS_SYMBOL_DISABLE_S"] = "300"
        os.environ["OPTIONS_CACHE_MAX_AGE_S"] = "3600"

        _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
        )

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            _warn_cleanup_issue("test_options_ingestion_reliability.close_pooled_connections", e)
        try:
            self.tmp.cleanup()
        except PermissionError as e:
            _warn_cleanup_issue("test_options_ingestion_reliability.tempdir_cleanup", e)
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            _reload_modules(
                "engine.runtime.storage",
                "engine.data.options_poll",
            )
        except Exception as e:
            _warn_cleanup_issue("test_options_ingestion_reliability.restore_backend", e)

    def test_tradier_retries_rate_limit_and_validates_payload(self) -> None:
        (tradier_live,) = _reload_modules("engine.data.options.tradier_live")
        session = _FakeSession(
            [
                _FakeResponse(429, {}, headers={"Retry-After": "0"}),
                _FakeResponse(200, {"expirations": {"date": ["2026-04-17"]}}),
                _FakeResponse(
                    200,
                    {
                        "options": {
                            "option": [
                                {
                                    "strike": "500",
                                    "option_type": "call",
                                    "implied_volatility": "0.2",
                                    "open_interest": "10",
                                    "volume": "5",
                                }
                            ]
                        }
                    },
                ),
            ]
        )

        with patch("engine.data.options.tradier_live.time.sleep", return_value=None):
            result = tradier_live.fetch_options_chain("SPY", session=session)

        self.assertEqual(result["rows"][0]["call_put"], "C")
        self.assertEqual(float(result["rows"][0]["strike"]), 500.0)

    def test_run_once_uses_cached_snapshot_and_then_disables_symbol(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        cached_ts_ms = int(time.time() * 1000) - 60_000
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO options_chain(
                  ts_ms, symbol, expiry, strike, call_put, iv, open_interest, volume, source
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (int(cached_ts_ms), "SPY", "2026-04-17", 500.0, "C", 0.2, 10, 5, "tradier"),
            )
            con.commit()
        finally:
            con.close()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("rate_limited", kind="rate_limit"),
            ):
                first = options_poll._run_once(["tradier"])
                second = options_poll._run_once(["tradier"])

        self.assertEqual(first["meta"]["cached_symbols"], ["SPY"])
        self.assertEqual(int(first["last_ingested_ts_ms"]), int(cached_ts_ms))
        self.assertEqual(second["meta"]["cached_symbols"], ["SPY"])

        con = storage.connect(readonly=True)
        try:
            state = con.execute(
                """
                SELECT consecutive_failures, disabled_until_ts_ms
                FROM options_symbol_ingestion_state
                WHERE symbol='SPY'
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(state)
        self.assertEqual(int(state[0]), 2)
        self.assertGreater(int(state[1]), int(time.time() * 1000))

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]):
            with patch("engine.data.options_poll.fetch_options_chain", side_effect=AssertionError("should not fetch disabled symbol")):
                third = options_poll._run_once(["tradier"])

        self.assertEqual(third["meta"]["cached_symbols"], ["SPY"])
        self.assertEqual(third["meta"]["symbol_status"]["SPY"]["status"], "disabled_cached")

    def test_run_once_short_circuits_provider_config_errors_across_symbols(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY", "QQQ", "IWM"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("tradier_api_token_missing", kind="config_error"),
            ) as fetch_mock:
                result = options_poll._run_once(["tradier"])

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertEqual(int(result["meta"]["symbols_attempted"]), 3)
        self.assertEqual(int(result["meta"]["symbols_succeeded"]), 0)
        self.assertEqual(int(result["meta"]["symbols_failed"]), 3)
        self.assertFalse(bool(result["pipeline_ok"]))
        self.assertEqual(int(result["provider_status"]["tradier"]["failed_symbols"]), 3)

    def test_run_once_bulk_loads_state_and_fetches_remaining_symbols_in_parallel(self) -> None:
        previous_concurrency = os.environ.get("OPTIONS_POLL_FETCH_CONCURRENCY")
        try:
            os.environ["OPTIONS_POLL_FETCH_CONCURRENCY"] = "2"
            storage, options_poll = _reload_modules(
                "engine.runtime.storage",
                "engine.data.options_poll",
            )
            storage.init_db()

            active = {"current": 0, "max": 0}
            lock = threading.Lock()

            def _fetch(symbol):
                if str(symbol) != "SPY":
                    with lock:
                        active["current"] += 1
                        active["max"] = max(active["max"], active["current"])
                    try:
                        time.sleep(0.05)
                    finally:
                        with lock:
                            active["current"] -= 1
                return _tradier_success_rows()

            with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY", "QQQ", "IWM"]):
                with patch("engine.data.options_poll.fetch_options_chain", side_effect=_fetch):
                    with patch(
                        "engine.data.options_poll._load_symbol_state",
                        side_effect=AssertionError("state should be bulk-loaded once per run"),
                    ):
                        result = options_poll._run_once(["tradier"])

            self.assertEqual(int(result["meta"]["symbols_succeeded"]), 3)
            self.assertGreaterEqual(int(active["max"]), 2)
        finally:
            if previous_concurrency is None:
                os.environ.pop("OPTIONS_POLL_FETCH_CONCURRENCY", None)
            else:
                os.environ["OPTIONS_POLL_FETCH_CONCURRENCY"] = previous_concurrency

    def test_run_once_defaults_to_batched_commit(self) -> None:
        previous_batch = os.environ.get("OPTIONS_POLL_COMMIT_BATCH_SYMBOLS")
        previous_legacy_batch = os.environ.get("OPTIONS_POLL_COMMIT_EVERY_SYMBOLS")
        try:
            os.environ.pop("OPTIONS_POLL_COMMIT_BATCH_SYMBOLS", None)
            os.environ.pop("OPTIONS_POLL_COMMIT_EVERY_SYMBOLS", None)
            (options_poll,) = _reload_modules("engine.data.options_poll")

            read_con = _CountingConnection()
            write_con = _CountingConnection()
            states = {
                "SPY": {"disabled_until_ts_ms": 0},
                "QQQ": {"disabled_until_ts_ms": 0},
                "IWM": {"disabled_until_ts_ms": 0},
            }
            with patch("engine.data.options_poll.connect", side_effect=[read_con, write_con]):
                with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY", "QQQ", "IWM"]):
                    with patch("engine.data.options_poll._load_symbol_states", return_value=states):
                        with patch("engine.data.options_poll.fetch_options_chain", return_value=_tradier_success_rows()):
                            with patch("engine.data.options_poll._write_options_bulk_rows", side_effect=_bulk_write_counts):
                                with patch("engine.data.options_poll._record_symbol_success", return_value={}):
                                    with patch("engine.data.options_poll._write_options_snapshot_event", return_value=None):
                                        with patch("engine.data.options_poll.checkpoint_if_due", return_value=None):
                                            result = options_poll._run_once(["tradier"])

            self.assertEqual(int(result["meta"]["symbols_succeeded"]), 3)
            self.assertEqual(int(write_con.commits), 1)
        finally:
            if previous_batch is None:
                os.environ.pop("OPTIONS_POLL_COMMIT_BATCH_SYMBOLS", None)
            else:
                os.environ["OPTIONS_POLL_COMMIT_BATCH_SYMBOLS"] = previous_batch
            if previous_legacy_batch is None:
                os.environ.pop("OPTIONS_POLL_COMMIT_EVERY_SYMBOLS", None)
            else:
                os.environ["OPTIONS_POLL_COMMIT_EVERY_SYMBOLS"] = previous_legacy_batch

    def test_options_write_buffer_outage_replays_after_reload_and_deletes_after_commit(self) -> None:
        previous_spool_path = os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_PATH")
        previous_spool_rows = os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS")
        previous_spool_bytes = os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES")
        try:
            os.environ["OPTIONS_POLL_DURABLE_BUFFER_PATH"] = str(Path(self.tmp.name) / "options_poll_spool.sqlite")
            os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS"] = "100"
            os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES"] = str(1024 * 1024)
            (options_poll,) = _reload_modules("engine.data.options_poll")

            first_con = _CountingConnection()
            buffer = options_poll._OptionsWriteBuffer(first_con, batch_symbols=1)
            written = buffer.add_tradier_rows("SPY", _tradier_success_rows()["rows"], ts_ms=1_000, source="tradier")
            buffer.stage_snapshot_event(
                ts_ms=1_000,
                source="options_tradier",
                symbol="SPY",
                provider="tradier",
                row_count=written,
            )
            buffer.stage_symbol_success(
                "SPY",
                provider="tradier",
                now_ms=1_000,
                snapshot_ts_ms=1_000,
                row_count=written,
                state_cache={},
            )

            with patch("engine.data.options_poll._write_options_bulk_rows", side_effect=RuntimeError("db down")):
                with self.assertRaises(RuntimeError):
                    buffer.mark_symbol()

            outage_snapshot = buffer.snapshot()
            self.assertEqual(int(outage_snapshot["durable_buffer_pending_rows"]), 3)
            self.assertEqual(int(outage_snapshot["durable_buffer_dropped_rows"]), 0)
            self.assertEqual(int(outage_snapshot["pending_tradier_rows"]), 1)
            self.assertEqual(int(outage_snapshot["pending_event_rows"]), 1)
            self.assertEqual(int(outage_snapshot["pending_state_rows"]), 1)

            (options_poll,) = _reload_modules("engine.data.options_poll")
            failed_replay = options_poll._OptionsWriteBuffer(_CountingConnection(), batch_symbols=1)
            with patch("engine.data.options_poll._write_options_bulk_rows", side_effect=RuntimeError("still down")):
                failed_replay.replay_spooled(max_rows=100)
            failed_snapshot = failed_replay.snapshot()
            self.assertEqual(int(failed_snapshot["durable_buffer_pending_rows"]), 3)
            self.assertEqual(int(failed_snapshot["durable_buffer_replay_failures"]), 1)

            replay_con = _CountingConnection()
            replayed = options_poll._OptionsWriteBuffer(replay_con, batch_symbols=1)
            with patch("engine.data.options_poll._write_options_bulk_rows", side_effect=_bulk_write_counts):
                replayed.replay_spooled(max_rows=100)
            replay_snapshot = replayed.snapshot()
            self.assertEqual(int(replay_snapshot["durable_buffer_pending_rows"]), 0)
            self.assertEqual(int(replay_snapshot["durable_buffer_replayed_rows"]), 3)
            self.assertEqual(int(replay_snapshot["durable_buffer_deleted_rows"]), 3)
            self.assertEqual(int(replay_snapshot["durable_buffer_dropped_rows"]), 0)
            self.assertEqual(int(replay_snapshot["event_rows_written"]), 1)
            self.assertEqual(int(replay_snapshot["state_rows_written"]), 1)
            self.assertEqual(int(replay_con.commits), 1)
        finally:
            if previous_spool_path is None:
                os.environ.pop("OPTIONS_POLL_DURABLE_BUFFER_PATH", None)
            else:
                os.environ["OPTIONS_POLL_DURABLE_BUFFER_PATH"] = previous_spool_path
            if previous_spool_rows is None:
                os.environ.pop("OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS", None)
            else:
                os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS"] = previous_spool_rows
            if previous_spool_bytes is None:
                os.environ.pop("OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES", None)
            else:
                os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES"] = previous_spool_bytes

    def test_options_write_buffer_spool_full_backpressures_before_db_write(self) -> None:
        previous_spool_path = os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_PATH")
        previous_spool_rows = os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS")
        previous_spool_bytes = os.environ.get("OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES")
        try:
            os.environ["OPTIONS_POLL_DURABLE_BUFFER_PATH"] = str(Path(self.tmp.name) / "options_poll_full.sqlite")
            os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS"] = "2"
            os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES"] = str(1024 * 1024)
            (options_poll,) = _reload_modules("engine.data.options_poll")

            buffer = options_poll._OptionsWriteBuffer(_CountingConnection(), batch_symbols=1)
            written = buffer.add_tradier_rows("SPY", _tradier_success_rows()["rows"], ts_ms=1_000, source="tradier")
            buffer.stage_snapshot_event(
                ts_ms=1_000,
                source="options_tradier",
                symbol="SPY",
                provider="tradier",
                row_count=written,
            )
            buffer.stage_symbol_success(
                "SPY",
                provider="tradier",
                now_ms=1_000,
                snapshot_ts_ms=1_000,
                row_count=written,
                state_cache={},
            )

            with patch("engine.data.options_poll._write_options_bulk_rows") as bulk_write:
                with self.assertRaises(options_poll.NonPriceIngestionSpoolFullError):
                    buffer.mark_symbol()

            bulk_write.assert_not_called()
            snapshot = buffer.snapshot()
            self.assertTrue(bool(snapshot["durable_buffer_backpressure_active"]))
            self.assertEqual(int(snapshot["durable_buffer_backpressure_events"]), 1)
            self.assertEqual(int(snapshot["durable_buffer_rejected_rows"]), 3)
            self.assertEqual(int(snapshot["durable_buffer_pending_rows"]), 0)
            self.assertEqual(int(snapshot["pending_tradier_rows"]), 1)
            self.assertEqual(int(snapshot["pending_event_rows"]), 1)
            self.assertEqual(int(snapshot["pending_state_rows"]), 1)
        finally:
            if previous_spool_path is None:
                os.environ.pop("OPTIONS_POLL_DURABLE_BUFFER_PATH", None)
            else:
                os.environ["OPTIONS_POLL_DURABLE_BUFFER_PATH"] = previous_spool_path
            if previous_spool_rows is None:
                os.environ.pop("OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS", None)
            else:
                os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_ROWS"] = previous_spool_rows
            if previous_spool_bytes is None:
                os.environ.pop("OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES", None)
            else:
                os.environ["OPTIONS_POLL_DURABLE_BUFFER_MAX_BYTES"] = previous_spool_bytes

    def test_options_bulk_rows_use_copy_staging_when_raw_postgres_copy_is_available(self) -> None:
        (options_poll,) = _reload_modules("engine.data.options_poll")

        class _FakeCopy:
            def __init__(self, cursor, sql) -> None:
                self.cursor = cursor
                self.sql = str(sql or "")

            def __enter__(self):
                self.cursor.copy_sql.append(self.sql)
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def write_row(self, row) -> None:
                self.cursor.copy_rows.append({"sql": self.sql, "row": tuple(row)})

        class _FakeCopyCursor:
            def __init__(self) -> None:
                self.executed = []
                self.copy_sql = []
                self.copy_rows = []
                self.closed = False

            def execute(self, sql, params=None) -> None:
                self.executed.append({"sql": str(sql or ""), "params": tuple(params or ())})

            def copy(self, sql):
                return _FakeCopy(self, sql)

            def close(self) -> None:
                self.closed = True

        class _FakeRawPostgres:
            def __init__(self) -> None:
                self.cursor_obj = _FakeCopyCursor()

            def cursor(self):
                return self.cursor_obj

        class _FakePostgresConnection:
            def __init__(self) -> None:
                self.raw = _FakeRawPostgres()

            def executemany(self, *_args, **_kwargs):
                raise AssertionError("copy path should not fall back to executemany")

        con = _FakePostgresConnection()
        options_poll.OPTIONS_POLL_COPY_STAGING_ENABLED = True
        options_poll.OPTIONS_POLL_COPY_STAGING_FALLBACK_ENABLED = True

        polygon_rows = options_poll._polygon_contract_value_rows(_polygon_success_contracts("SPY")[0])
        tradier_rows = options_poll._tradier_value_rows(
            "SPY",
            _tradier_success_rows()["rows"],
            ts_ms=1_900_000_000_000,
            source="tradier",
        )
        result = options_poll._write_options_bulk_rows(
            con,
            polygon_rows=polygon_rows,
            tradier_rows=tradier_rows,
        )

        cursor = con.raw.cursor_obj
        self.assertEqual(result["write_path"], "copy_staging")
        self.assertEqual(int(result["polygon_rows"]), 1)
        self.assertEqual(int(result["tradier_rows"]), 1)
        self.assertEqual(len(cursor.copy_rows), 2)
        self.assertTrue(any("options_chain_v2_write_staging" in sql for sql in cursor.copy_sql))
        self.assertTrue(any("options_chain_write_staging" in sql for sql in cursor.copy_sql))
        self.assertTrue(any("DISTINCT ON (contract, ts_ms)" in row["sql"] for row in cursor.executed))
        self.assertTrue(any("DISTINCT ON (symbol, expiry, strike, call_put, ts_ms)" in row["sql"] for row in cursor.executed))
        self.assertTrue(cursor.closed)

    def test_options_bulk_rows_replay_preserves_idempotent_upserts(self) -> None:
        (options_poll,) = _reload_modules("engine.data.options_poll")
        con = sqlite3.connect(":memory:")
        con.execute(
            """
            CREATE TABLE options_chain_v2(
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
              UNIQUE(contract, ts_ms)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE options_chain(
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              expiry TEXT NOT NULL,
              strike REAL NOT NULL,
              call_put TEXT NOT NULL,
              iv REAL,
              open_interest INTEGER,
              volume INTEGER,
              source TEXT,
              UNIQUE(symbol, expiry, strike, call_put, ts_ms)
            )
            """
        )
        polygon_rows = options_poll._polygon_contract_value_rows(_polygon_success_contracts("SPY")[0])
        tradier_rows = options_poll._tradier_value_rows(
            "SPY",
            _tradier_success_rows()["rows"],
            ts_ms=1_900_000_000_000,
            source="tradier",
        )

        first = options_poll._write_options_bulk_rows(con, polygon_rows=polygon_rows, tradier_rows=tradier_rows)
        second = options_poll._write_options_bulk_rows(con, polygon_rows=polygon_rows, tradier_rows=tradier_rows)

        self.assertEqual(first["raw_rows"], 2)
        self.assertEqual(second["raw_rows"], 2)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM options_chain_v2").fetchone()[0], 1)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM options_chain").fetchone()[0], 1)
        con.close()

    def test_ingest_options_snapshot_job_is_bounded_retry_idempotent(self) -> None:
        (ingest_options,) = _reload_modules("engine.data.jobs.ingest_options")
        con = sqlite3.connect(":memory:")
        con.execute(
            """
            CREATE TABLE options_chain_v2(
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
              UNIQUE(contract, ts_ms)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE options_chain(
              ts_ms INTEGER NOT NULL,
              symbol TEXT NOT NULL,
              expiry TEXT NOT NULL,
              strike REAL NOT NULL,
              call_put TEXT NOT NULL,
              iv REAL,
              open_interest INTEGER,
              volume INTEGER,
              source TEXT,
              UNIQUE(symbol, expiry, strike, call_put, ts_ms)
            )
            """
        )
        rows = _polygon_success_contracts("SPY")[0]

        con.execute("BEGIN")
        ingest_options._put_options_rows(con, rows)
        con.rollback()
        self.assertEqual(con.execute("SELECT COUNT(*) FROM options_chain_v2").fetchone()[0], 0)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM options_chain").fetchone()[0], 0)

        ingest_options._put_options_rows(con, rows)
        con.commit()
        ingest_options._put_options_rows(con, rows)
        con.commit()

        self.assertEqual(con.execute("SELECT COUNT(*) FROM options_chain_v2").fetchone()[0], 1)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM options_chain").fetchone()[0], 1)
        con.close()

    def test_600_symbol_cycle_uses_bounded_fetches_single_state_load_and_bulk_writes(self) -> None:
        (options_poll,) = _reload_modules("engine.data.options_poll")
        symbols = [f"S{i:03d}" for i in range(600)]

        for provider in ("polygon", "tradier"):
            with self.subTest(provider=provider):
                read_con = _InstrumentedOptionsConnection()
                write_con = _InstrumentedOptionsConnection()
                active = {"current": 0, "max": 0}
                fetch_count = {"count": 0}
                lock = threading.Lock()

                options_poll.provider_cooldowns.clear()
                options_poll.provider_rate_limit_counts.clear()
                options_poll.provider_cooldown_reasons.clear()
                options_poll.OPTIONS_POLL_FETCH_CONCURRENCY = 8
                options_poll.OPTIONS_POLL_COMMIT_BATCH_SYMBOLS = 50
                options_poll.OPTIONS_POLL_COMMIT_EVERY_SYMBOLS = 50

                def _tracked_fetch(symbol, *_args, **_kwargs):
                    with lock:
                        fetch_count["count"] += 1
                        active["current"] += 1
                        active["max"] = max(active["max"], active["current"])
                    try:
                        time.sleep(0.002)
                    finally:
                        with lock:
                            active["current"] -= 1
                    if provider == "polygon":
                        return _polygon_success_contracts(str(symbol))
                    return _tradier_success_rows()

                def _connect(readonly=False, **_kwargs):
                    return read_con if readonly else write_con

                fetch_patch = (
                    patch("engine.data.options_poll.fetch_options_chain_snapshot", side_effect=_tracked_fetch)
                    if provider == "polygon"
                    else patch("engine.data.options_poll.fetch_options_chain", side_effect=_tracked_fetch)
                )

                with patch("engine.data.options_poll.connect", side_effect=_connect):
                    with patch("engine.data.options_poll.get_active_symbols", return_value=symbols):
                        with patch(
                            "engine.data.options_poll._load_symbol_state",
                            side_effect=AssertionError("state should be bulk-loaded once per run"),
                        ):
                            with patch(
                                "engine.data.options_poll._write_polygon_contracts",
                                side_effect=AssertionError("per-symbol polygon writer should not run"),
                            ) as polygon_writer:
                                with patch(
                                    "engine.data.options_poll._write_tradier_rows",
                                    side_effect=AssertionError("per-symbol tradier writer should not run"),
                                ) as tradier_writer:
                                    with patch("engine.data.options_poll.checkpoint_if_due", return_value=None):
                                        with patch("engine.data.options_poll.logging.info", return_value=None):
                                            with fetch_patch:
                                                result = options_poll._run_once([provider])

                self.assertEqual(int(result["meta"]["symbols_succeeded"]), 600)
                self.assertEqual(int(result["raw_rows"]), 600)
                self.assertEqual(int(result["meta"]["state_load_queries"]), 1)
                self.assertEqual(int(result["meta"]["state_load_symbols"]), 600)
                self.assertEqual(int(result["meta"]["provider_fetch_symbols"]), 600)
                self.assertEqual(int(result["meta"]["provider_fetch_max_workers"]), 8)
                self.assertEqual(int(result["meta"]["provider_fetch_concurrency_limit"]), 8)
                self.assertEqual(int(result["meta"]["commit_batches"]), 12)
                self.assertEqual(int(result["meta"]["commit_batch_symbols"]), 50)
                self.assertEqual(int(result["meta"]["max_symbols_per_commit"]), 50)
                self.assertEqual(int(result["meta"]["rows_written"]), 600)
                self.assertEqual(int(result["meta"]["bulk_write_failures"]), 0)
                self.assertEqual(int(fetch_count["count"]), 600)
                self.assertGreater(int(active["max"]), 1)
                self.assertLessEqual(int(active["max"]), 8)
                self.assertEqual(int(write_con.state_load_queries), 1)
                self.assertEqual(write_con.state_load_param_counts, [600])
                self.assertEqual(int(write_con.commits), 12)
                self.assertEqual(int(result["meta"]["write_buffer"]["commit_batches"]), 12)
                self.assertEqual(int(result["meta"]["write_buffer"]["max_symbols_per_commit"]), 50)
                self.assertEqual(int(result["meta"]["write_buffer"]["rows_written"]), 600)
                self.assertEqual(result["meta"]["write_buffer"]["write_paths"], {"executemany_copy_fallback": 12})
                self.assertEqual(int(result["meta"]["copy_staging_batches"]), 0)
                self.assertEqual(int(result["meta"]["executemany_batches"]), 12)
                self.assertEqual(int(result["meta"]["copy_fallbacks"]), 12)
                expected_table = "options_chain_v2" if provider == "polygon" else "options_chain"
                option_batches = [
                    batch
                    for batch in write_con.executemany_batches
                    if expected_table in str(batch["sql"])
                ]
                state_batches = [
                    batch
                    for batch in write_con.executemany_batches
                    if "options_symbol_ingestion_state" in str(batch["sql"])
                ]
                self.assertEqual(len(option_batches), 12)
                self.assertEqual(sum(int(batch["count"]) for batch in option_batches), 600)
                self.assertTrue(all(int(batch["count"]) == 50 for batch in option_batches))
                self.assertEqual(len(state_batches), 12)
                self.assertEqual(sum(int(batch["count"]) for batch in state_batches), 600)
                self.assertTrue(all(int(batch["count"]) == 50 for batch in state_batches))
                self.assertFalse(polygon_writer.called)
                self.assertFalse(tradier_writer.called)

    def test_600_symbol_provider_entitlement_failure_falls_back_to_bounded_pool(self) -> None:
        (options_poll,) = _reload_modules("engine.data.options_poll")
        symbols = [f"S{i:03d}" for i in range(600)]
        read_con = _InstrumentedOptionsConnection()
        write_con = _InstrumentedOptionsConnection()
        active = {"current": 0, "max": 0}
        tradier_fetch_count = {"count": 0}
        lock = threading.Lock()

        class EntitlementError(Exception):
            status_code = 403

        entitlement_error = EntitlementError("polygon_options_entitlement_required")
        options_poll.provider_cooldowns.clear()
        options_poll.provider_rate_limit_counts.clear()
        options_poll.provider_cooldown_reasons.clear()
        options_poll.OPTIONS_POLL_FETCH_CONCURRENCY = 8
        options_poll.OPTIONS_POLL_COMMIT_BATCH_SYMBOLS = 50
        options_poll.OPTIONS_POLL_COMMIT_EVERY_SYMBOLS = 50

        def _connect(readonly=False, **_kwargs):
            return read_con if readonly else write_con

        def _tradier_fetch(symbol, *_args, **_kwargs):
            with lock:
                tradier_fetch_count["count"] += 1
                active["current"] += 1
                active["max"] = max(active["max"], active["current"])
            try:
                time.sleep(0.002)
            finally:
                with lock:
                    active["current"] -= 1
            return _tradier_success_rows()

        with patch("engine.data.options_poll.connect", side_effect=_connect):
            with patch("engine.data.options_poll.get_active_symbols", return_value=symbols):
                with patch("engine.data.options_poll.fetch_options_chain_snapshot", return_value=([], entitlement_error)) as polygon_fetch:
                    with patch("engine.data.options_poll.fetch_options_chain", side_effect=_tradier_fetch):
                        with patch("engine.data.options_poll.checkpoint_if_due", return_value=None):
                            with patch("engine.data.options_poll.logging.info", return_value=None):
                                result = options_poll._run_once(["polygon", "tradier"])

        self.assertEqual(polygon_fetch.call_count, 1)
        self.assertEqual(int(tradier_fetch_count["count"]), 600)
        self.assertGreater(int(active["max"]), 1)
        self.assertLessEqual(int(active["max"]), 8)
        self.assertEqual(int(result["meta"]["symbols_succeeded"]), 600)
        self.assertEqual(int(result["meta"]["provider_fetch_symbols"]), 601)
        self.assertEqual(int(result["meta"]["provider_fetch_max_workers"]), 8)
        self.assertEqual(int(result["provider_status"]["polygon"]["failed_symbols"]), 600)
        self.assertEqual(int(result["provider_status"]["tradier"]["fresh_symbols"]), 600)
        self.assertEqual(result["meta"]["symbol_status"]["S000"]["provider"], "tradier")
        self.assertEqual(int(write_con.state_load_queries), 1)
        self.assertEqual(write_con.state_load_param_counts, [600])
        self.assertEqual(int(write_con.commits), 12)
        self.assertEqual(int(result["meta"]["commit_batches"]), 12)
        self.assertEqual(int(result["meta"]["rows_written"]), 600)
        self.assertEqual(result["meta"]["write_buffer"]["write_paths"], {"executemany_copy_fallback": 12})
        self.assertEqual(int(result["meta"]["executemany_batches"]), 12)
        self.assertEqual(int(result["meta"]["copy_fallbacks"]), 12)
        option_batches = [
            batch
            for batch in write_con.executemany_batches
            if "options_chain" in str(batch["sql"])
        ]
        state_batches = [
            batch
            for batch in write_con.executemany_batches
            if "options_symbol_ingestion_state" in str(batch["sql"])
        ]
        self.assertEqual(len(option_batches), 12)
        self.assertEqual(sum(int(batch["count"]) for batch in option_batches), 600)
        self.assertEqual(len(state_batches), 12)
        self.assertEqual(sum(int(batch["count"]) for batch in state_batches), 600)

    def test_600_symbol_all_failure_cycle_commits_state_at_batch_boundaries(self) -> None:
        (options_poll,) = _reload_modules("engine.data.options_poll")
        symbols = [f"S{i:03d}" for i in range(600)]
        read_con = _InstrumentedOptionsConnection()
        write_con = _InstrumentedOptionsConnection()
        fetch_count = {"count": 0}

        options_poll.provider_cooldowns.clear()
        options_poll.provider_rate_limit_counts.clear()
        options_poll.provider_cooldown_reasons.clear()
        options_poll.OPTIONS_POLL_FETCH_CONCURRENCY = 8
        options_poll.OPTIONS_POLL_COMMIT_BATCH_SYMBOLS = 50
        options_poll.OPTIONS_POLL_COMMIT_EVERY_SYMBOLS = 50

        def _connect(readonly=False, **_kwargs):
            return read_con if readonly else write_con

        def _disabled_provider_fetch(_symbol):
            fetch_count["count"] += 1
            raise options_poll.TradierFetchError("tradier_api_token_missing", kind="config_error")

        with patch("engine.data.options_poll.connect", side_effect=_connect):
            with patch("engine.data.options_poll.get_active_symbols", return_value=symbols):
                with patch("engine.data.options_poll.fetch_options_chain", side_effect=_disabled_provider_fetch):
                    with patch("engine.data.options_poll.checkpoint_if_due", return_value=None):
                        with patch("engine.data.options_poll.emit_alert", return_value=None):
                            with patch("engine.data.options_poll.logging.info", return_value=None):
                                result = options_poll._run_once(["tradier"])

        self.assertEqual(int(fetch_count["count"]), 1)
        self.assertEqual(int(result["meta"]["symbols_succeeded"]), 0)
        self.assertEqual(int(result["meta"]["symbols_failed"]), 600)
        self.assertEqual(int(result["meta"]["state_load_queries"]), 1)
        self.assertEqual(int(write_con.state_load_queries), 1)
        self.assertEqual(write_con.state_load_param_counts, [600])
        self.assertEqual(int(write_con.commits), 12)
        self.assertEqual(int(write_con.rollbacks), 0)
        self.assertEqual(int(result["meta"]["commit_batches"]), 12)
        self.assertEqual(int(result["meta"]["max_symbols_per_commit"]), 50)
        self.assertEqual(int(result["meta"]["rows_written"]), 0)
        self.assertEqual(int(result["meta"]["bulk_write_failures"]), 0)
        self.assertEqual(result["meta"]["write_buffer"]["write_paths"], {"none": 12})
        self.assertEqual(int(result["meta"]["executemany_batches"]), 0)
        self.assertEqual(int(result["meta"]["copy_fallbacks"]), 0)
        self.assertEqual(int(result["meta"]["state_only_batches"]), 12)
        option_batches = [
            batch
            for batch in write_con.executemany_batches
            if "options_chain" in str(batch["sql"])
        ]
        state_batches = [
            batch
            for batch in write_con.executemany_batches
            if "options_symbol_ingestion_state" in str(batch["sql"])
        ]
        self.assertEqual(len(option_batches), 0)
        self.assertEqual(len(state_batches), 12)
        self.assertEqual(sum(int(batch["count"]) for batch in state_batches), 600)

    def test_partial_cycle_failure_preserves_committed_batches_and_reports_failure(self) -> None:
        (options_poll,) = _reload_modules("engine.data.options_poll")
        symbols = ["SPY", "QQQ", "IWM", "DIA"]
        read_con = _CountingConnection()
        write_con = _CountingConnection()
        flushes = []

        options_poll.provider_cooldowns.clear()
        options_poll.provider_rate_limit_counts.clear()
        options_poll.provider_cooldown_reasons.clear()
        options_poll.OPTIONS_POLL_FETCH_CONCURRENCY = 4
        options_poll.OPTIONS_POLL_COMMIT_BATCH_SYMBOLS = 2
        options_poll.OPTIONS_POLL_COMMIT_EVERY_SYMBOLS = 2

        def _connect(readonly=False, **_kwargs):
            return read_con if readonly else write_con

        def _bulk_write_once_then_fail(_con, *, polygon_rows=None, tradier_rows=None):
            polygon_n = len(list(polygon_rows or []))
            tradier_n = len(list(tradier_rows or []))
            flushes.append({"polygon": polygon_n, "tradier": tradier_n})
            if len(flushes) == 2:
                raise RuntimeError("bulk write failed")
            return {"polygon_rows": polygon_n, "tradier_rows": tradier_n, "raw_rows": polygon_n + tradier_n}

        with patch("engine.data.options_poll.connect", side_effect=_connect):
            with patch("engine.data.options_poll.get_active_symbols", return_value=symbols):
                with patch(
                    "engine.data.options_poll._load_symbol_states",
                    return_value={symbol: {"disabled_until_ts_ms": 0} for symbol in symbols},
                ):
                    with patch("engine.data.options_poll.fetch_options_chain", return_value=_tradier_success_rows()):
                        with patch("engine.data.options_poll._write_options_bulk_rows", side_effect=_bulk_write_once_then_fail):
                            with patch("engine.data.options_poll._record_symbol_success", return_value={}):
                                with patch("engine.data.options_poll._write_options_snapshot_event", return_value=None):
                                    with patch("engine.data.options_poll._write_options_poll_metric", return_value=None):
                                        with patch("engine.data.options_poll._warn_nonfatal") as warn_nonfatal:
                                            with self.assertRaises(RuntimeError, msg="bulk write failed"):
                                                options_poll._run_once(["tradier"])

        self.assertEqual(int(write_con.commits), 1)
        self.assertTrue(bool(write_con.closed))
        self.assertEqual(flushes, [{"polygon": 0, "tradier": 2}, {"polygon": 0, "tradier": 2}])
        codes = [str(call.args[0]) for call in warn_nonfatal.call_args_list if call.args]
        self.assertIn("OPTIONS_POLL_BULK_WRITE_FAILED", codes)
        self.assertIn("OPTIONS_POLL_RUN_FAILED", codes)

    def test_run_once_suppresses_per_symbol_fetch_failed_after_provider_disable(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY", "QQQ", "IWM"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("tradier_api_token_missing", kind="config_error"),
            ):
                with patch("engine.data.options_poll._warn_state") as warn_state:
                    options_poll._run_once(["tradier"])

        codes = [str(call.args[0]) for call in warn_state.call_args_list if call.args]
        self.assertEqual(codes.count("OPTIONS_POLL_PROVIDER_DISABLED"), 1)
        self.assertNotIn("OPTIONS_POLL_FETCH_FAILED", codes)

    def test_run_once_survives_symbol_failure_state_lock(self) -> None:
        storage, options_poll = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
        )
        storage.init_db()

        with patch("engine.data.options_poll.get_active_symbols", return_value=["SPY"]):
            with patch(
                "engine.data.options_poll.fetch_options_chain",
                side_effect=options_poll.TradierFetchError("tradier_api_token_missing", kind="config_error"),
            ):
                with patch(
                    "engine.data.options_poll._write_symbol_state_rows",
                    side_effect=sqlite3.OperationalError("database is locked"),
                ):
                    with patch("engine.data.options_poll._warn_nonfatal") as warn_nonfatal:
                        result = options_poll._run_once(["tradier"])

        self.assertFalse(bool(result["pipeline_ok"]))
        self.assertEqual(result["meta"]["symbol_status"]["SPY"]["status"], "failed")
        self.assertEqual(int(result["meta"]["symbol_status"]["SPY"]["disabled_until_ts_ms"] or 0), 0)
        self.assertEqual(int(result["meta"]["state_write_failures"]), 1)
        codes = [str(call.args[0]) for call in warn_nonfatal.call_args_list if call.args]
        self.assertIn("OPTIONS_POLL_SYMBOL_STATE_BATCH_RECORD_FAILED", codes)
        failure_record_calls = [
            call
            for call in warn_nonfatal.call_args_list
            if call.args and call.args[0] == "OPTIONS_POLL_SYMBOL_STATE_BATCH_RECORD_FAILED"
        ]
        self.assertEqual(len(failure_record_calls), 1)
        self.assertNotIn("error", failure_record_calls[0].kwargs)
        self.assertEqual(int(failure_record_calls[0].kwargs["state_rows"]), 1)

    def test_lifecycle_degrades_when_options_ingestion_is_degraded(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: {
                "prices": {"ok": True},
                "ingestion_runtime": {"running": True, "stale": False},
                "ingestion_freshness": {
                    "degraded": True,
                    "runtime_reason_codes": ["critical_source_stale:options"],
                },
            },
            get_jobs=lambda: [{"name": "ingestion_runtime", "running": True}],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.2)
            state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(state["state"], lifecycle_state.DEGRADED)
        self.assertEqual(state["detail"], "critical_source_stale:options")

    def test_lifecycle_warmup_timeout_degrades_feedless_safe_runtime(self) -> None:
        prev_timeout = os.environ.get("WARMUP_TIMEOUT_S")
        prev_engine_mode = os.environ.get("ENGINE_MODE")
        prev_execution_mode = os.environ.get("EXECUTION_MODE")
        try:
            os.environ["WARMUP_TIMEOUT_S"] = "1"
            os.environ["ENGINE_MODE"] = "safe"
            os.environ["EXECUTION_MODE"] = "safe"
            storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.runtime_meta",
                "engine.runtime.lifecycle_state",
                "engine.runtime.lifecycle",
            )
            storage.init_db()
            lifecycle_state.set_state(lifecycle_state.WARMING_UP, "awaiting_first_price_tick")
            runtime_meta.meta_set("first_price_ts_ms", "")

            stop_event = threading.Event()
            thread = lifecycle.start_lifecycle_monitor(
                get_health=lambda: {
                    "prices": {"ok": False},
                    "ingestion_runtime": {"running": False, "stale": False},
                    "ingestion_freshness": {
                        "degraded": False,
                        "runtime_reason_codes": [],
                    },
                },
                get_jobs=lambda: [],
                get_kill_switches=lambda: {},
                interval_s=0.05,
                stop_event=stop_event,
                claim_booting=False,
            )
            try:
                time.sleep(1.35)
                state = lifecycle_state.get_state()
            finally:
                stop_event.set()
                thread.join(timeout=1.0)

            self.assertEqual(state["state"], lifecycle_state.DEGRADED)
            self.assertEqual(state["detail"], "warmup_timeout_awaiting_first_price_tick")

            lifecycle_state.set_state(
                lifecycle_state.WARMING_UP,
                "ingestion_runtime_running_awaiting_first_price_tick",
            )
            self.assertEqual(lifecycle_state.get_state()["state"], lifecycle_state.DEGRADED)
        finally:
            if prev_timeout is None:
                os.environ.pop("WARMUP_TIMEOUT_S", None)
            else:
                os.environ["WARMUP_TIMEOUT_S"] = prev_timeout
            if prev_engine_mode is None:
                os.environ.pop("ENGINE_MODE", None)
            else:
                os.environ["ENGINE_MODE"] = prev_engine_mode
            if prev_execution_mode is None:
                os.environ.pop("EXECUTION_MODE", None)
            else:
                os.environ["EXECUTION_MODE"] = prev_execution_mode

    def test_options_source_not_critical_when_options_job_disabled(self) -> None:
        prev_tradier_enabled = os.environ.get("TRADIER_ENABLED")
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        try:
            os.environ["TRADIER_ENABLED"] = "0"
            os.environ["CRITICAL_INGESTION_SOURCES"] = "prices,options"
            (health,) = _reload_modules("engine.runtime.health")

            definitions = health._ingestion_source_definitions()

            self.assertFalse(bool((definitions.get("options") or {}).get("critical")))
        finally:
            if prev_tradier_enabled is None:
                os.environ.pop("TRADIER_ENABLED", None)
            else:
                os.environ["TRADIER_ENABLED"] = prev_tradier_enabled
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources

    def test_lifecycle_ignores_noncritical_source_staleness(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: {
                "prices": {"ok": True},
                "ingestion_runtime": {"running": True, "stale": False},
                "ingestion_freshness": {
                    "degraded": False,
                    "runtime_reason_codes": [],
                    "advisory_reason_codes": ["source_stale:news", "source_stale:social"],
                    "stale_sources": ["news", "social"],
                    "stale_critical_sources": [],
                },
            },
            get_jobs=lambda: [{"name": "ingestion_runtime", "running": True}],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.2)
            state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(state["state"], lifecycle_state.LIVE)

    def test_lifecycle_does_not_degrade_for_nonfreshness_ingestion_flags(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: {
                "prices": {"ok": True},
                "ingestion_runtime": {"running": False, "stale": True},
                "options_ingestion": {"degraded": True, "detail": "options_ingestion_failed"},
                "ingestion_freshness": {
                    "degraded": False,
                    "runtime_reason_codes": [],
                    "advisory_reason_codes": ["source_degraded:options"],
                },
            },
            get_jobs=lambda: [],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.2)
            state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(state["state"], lifecycle_state.LIVE)

    def test_lifecycle_recovers_when_critical_freshness_recovers(self) -> None:
        storage, runtime_meta, lifecycle_state, lifecycle = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
            "engine.runtime.lifecycle",
        )
        storage.init_db()
        runtime_meta.meta_set("first_price_ts_ms", "123")
        lifecycle_state.set_state(lifecycle_state.LIVE, "first_market_data_tick")

        health_state = {
            "prices": {"ok": True},
            "ingestion_runtime": {"running": True, "stale": False},
            "ingestion_freshness": {
                "degraded": True,
                "runtime_reason_codes": ["critical_source_stale:prices"],
            },
        }

        stop_event = threading.Event()
        thread = lifecycle.start_lifecycle_monitor(
            get_health=lambda: dict(health_state),
            get_jobs=lambda: [{"name": "ingestion_runtime", "running": True}],
            get_kill_switches=lambda: {},
            interval_s=0.05,
            stop_event=stop_event,
            claim_booting=False,
        )
        try:
            time.sleep(0.15)
            first_state = lifecycle_state.get_state()
            health_state["ingestion_freshness"] = {
                "degraded": False,
                "runtime_reason_codes": [],
            }
            time.sleep(1.3)
            recovered_state = lifecycle_state.get_state()
        finally:
            stop_event.set()
            thread.join(timeout=1.0)

        self.assertEqual(first_state["state"], lifecycle_state.DEGRADED)
        self.assertEqual(first_state["detail"], "critical_source_stale:prices")
        self.assertEqual(recovered_state["state"], lifecycle_state.LIVE)

    def test_health_freshness_tracker_keeps_runtime_stable_for_noncritical_stale_sources(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        now_ms = int(time.time() * 1000)
        snapshot = health._build_ingestion_freshness_snapshot(
            now_ms=int(now_ms),
            prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
            options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
            ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
            pipeline_statuses={
                "ingest_now": {"ok": True, "updated_ts_ms": int(now_ms - 1_000_000)},
                "poll_social_reddit": {"ok": True, "updated_ts_ms": int(now_ms - 1_000_000)},
                "poll_social_stocktwits": {"ok": True, "updated_ts_ms": int(now_ms)},
                "poll_macro": {"ok": True, "updated_ts_ms": int(now_ms)},
                "poll_weather_forecasts": {"ok": True, "updated_ts_ms": int(now_ms)},
                "poll_weather_alerts": {"ok": True, "updated_ts_ms": int(now_ms)},
            },
        )

        self.assertFalse(snapshot["degraded"])
        self.assertEqual(snapshot["stale_critical_sources"], [])
        self.assertIn("news", snapshot["stale_sources"])
        self.assertNotIn("critical_source_stale:news", list(snapshot.get("runtime_reason_codes") or []))

    def test_health_freshness_marks_fresh_failed_options_as_degraded(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        now_ms = int(time.time() * 1000)

        snapshot = health._build_ingestion_freshness_snapshot(
            now_ms=int(now_ms),
            prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
            options_snapshot={
                "ok": False,
                "available": True,
                "degraded": False,
                "failed": True,
                "stale": False,
                "status": "failed",
                "detail": "options_ingestion_failed",
                "last_ingested_ts_ms": None,
            },
            ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
            pipeline_statuses={
                "options_poll": {
                    "ok": False,
                    "updated_ts_ms": int(now_ms),
                    "last_error": "polygon 403",
                },
            },
        )

        options_source = dict(snapshot.get("sources", {}).get("options") or {})
        self.assertEqual(options_source.get("status"), "degraded")
        self.assertFalse(bool(options_source.get("ok")))
        self.assertFalse(bool(options_source.get("stale")))
        self.assertIn("source_degraded:options", list(snapshot.get("advisory_reason_codes") or []))

    def test_health_freshness_escalates_fresh_failed_critical_source(self) -> None:
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        try:
            os.environ["CRITICAL_INGESTION_SOURCES"] = "prices,macro"
            (health,) = _reload_modules("engine.runtime.health")
            now_ms = int(time.time() * 1000)

            snapshot = health._build_ingestion_freshness_snapshot(
                now_ms=int(now_ms),
                prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
                options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
                ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
                pipeline_statuses={
                    "poll_macro": {
                        "ok": False,
                        "updated_ts_ms": int(now_ms),
                        "last_ingested_ts_ms": int(now_ms),
                        "last_error": "fred_http_403",
                    },
                },
            )
        finally:
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources

        macro_source = dict(snapshot.get("sources", {}).get("macro") or {})
        self.assertEqual(macro_source.get("status"), "degraded")
        self.assertTrue(bool(macro_source.get("critical")))
        self.assertFalse(bool(snapshot.get("critical_ok")))
        self.assertIn("macro", list(snapshot.get("failed_critical_sources") or []))
        self.assertIn("critical_source_failed:macro", list(snapshot.get("runtime_reason_codes") or []))
        self.assertIn("source_degraded:macro", list(snapshot.get("advisory_reason_codes") or []))

    def test_health_freshness_exposes_sec_and_form4_failures(self) -> None:
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        prev_child_jobs = os.environ.get("INGESTION_CHILD_JOBS")
        try:
            os.environ["CRITICAL_INGESTION_SOURCES"] = "sec,form4"
            os.environ["INGESTION_CHILD_JOBS"] = "poll_sec_filings,ingest_form4"
            (health,) = _reload_modules("engine.runtime.health")
            now_ms = int(time.time() * 1000)

            snapshot = health._build_ingestion_freshness_snapshot(
                now_ms=int(now_ms),
                prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
                options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
                ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
                pipeline_statuses={
                    "poll_sec_filings": {
                        "ok": True,
                        "updated_ts_ms": int(now_ms),
                        "last_ingested_ts_ms": int(now_ms),
                    },
                    "ingest_form4": {
                        "ok": False,
                        "updated_ts_ms": int(now_ms),
                        "last_ingested_ts_ms": int(now_ms),
                        "last_error": "edgar_rate_limited",
                    },
                },
            )
        finally:
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources
            if prev_child_jobs is None:
                os.environ.pop("INGESTION_CHILD_JOBS", None)
            else:
                os.environ["INGESTION_CHILD_JOBS"] = prev_child_jobs

        sec_source = dict(snapshot.get("sources", {}).get("sec") or {})
        self.assertTrue(bool(sec_source.get("critical")))
        self.assertIn("poll_sec_filings", list(sec_source.get("pipeline_names") or []))
        self.assertIn("ingest_form4", list(sec_source.get("pipeline_names") or []))
        self.assertEqual(sec_source.get("status"), "degraded")
        self.assertIn("sec", list(snapshot.get("failed_critical_sources") or []))
        self.assertIn("critical_source_failed:sec", list(snapshot.get("runtime_reason_codes") or []))

    def test_health_freshness_exposes_alt_data_failures(self) -> None:
        prev_critical_sources = os.environ.get("CRITICAL_INGESTION_SOURCES")
        prev_child_jobs = os.environ.get("INGESTION_CHILD_JOBS")
        try:
            os.environ["CRITICAL_INGESTION_SOURCES"] = "etf_flows"
            os.environ["INGESTION_CHILD_JOBS"] = "ingest_etf_flows"
            data_source_manager, health = _reload_modules(
                "services.data_source_manager",
                "engine.runtime.health",
            )
            now_ms = int(time.time() * 1000)

            with patch.object(data_source_manager, "desired_ingestion_jobs", return_value=["ingest_etf_flows"]):
                snapshot = health._build_ingestion_freshness_snapshot(
                    now_ms=int(now_ms),
                    prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
                    options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
                    ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
                    pipeline_statuses={
                        "ingest_etf_flows": {
                            "ok": False,
                            "updated_ts_ms": int(now_ms),
                            "last_ingested_ts_ms": int(now_ms),
                            "last_error": "etf_flows_provider_503",
                        },
                    },
                )
        finally:
            if prev_critical_sources is None:
                os.environ.pop("CRITICAL_INGESTION_SOURCES", None)
            else:
                os.environ["CRITICAL_INGESTION_SOURCES"] = prev_critical_sources
            if prev_child_jobs is None:
                os.environ.pop("INGESTION_CHILD_JOBS", None)
            else:
                os.environ["INGESTION_CHILD_JOBS"] = prev_child_jobs

        alt_source = dict(snapshot.get("sources", {}).get("alt_data") or {})
        self.assertTrue(bool(alt_source.get("critical")))
        self.assertEqual(alt_source.get("status"), "degraded")
        self.assertIn("ingest_etf_flows", list(alt_source.get("pipeline_names") or []))
        self.assertIn("alt_data", list(snapshot.get("failed_critical_sources") or []))
        self.assertIn("critical_source_failed:alt_data", list(snapshot.get("runtime_reason_codes") or []))

    def test_health_freshness_marks_fresh_failed_social_as_degraded(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        now_ms = int(time.time() * 1000)

        snapshot = health._build_ingestion_freshness_snapshot(
            now_ms=int(now_ms),
            prices_snapshot={"ok": True, "last_ts_ms": int(now_ms)},
            options_snapshot={"ok": True, "last_ingested_ts_ms": int(now_ms)},
            ingestion_runtime_snapshot={"last_publish_ts_ms": int(now_ms)},
            pipeline_statuses={
                "poll_social_reddit": {
                    "ok": False,
                    "updated_ts_ms": int(now_ms - 1_000_000),
                    "last_error": "reddit_credentials_missing",
                },
                "poll_social_stocktwits": {
                    "ok": False,
                    "updated_ts_ms": int(now_ms),
                    "last_error": "stocktwits_http_403",
                },
            },
        )

        social_source = dict(snapshot.get("sources", {}).get("social") or {})
        self.assertEqual(social_source.get("status"), "degraded")
        self.assertFalse(bool(social_source.get("ok")))
        self.assertFalse(bool(social_source.get("stale")))
        self.assertIn("source_degraded:social", list(snapshot.get("advisory_reason_codes") or []))
