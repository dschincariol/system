"""Regression tests for ingestion runtime freshness handling."""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
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


def _reset_sqlite_trace(storage) -> None:
    with storage._SQLITE_TRACE_LOCK:
        storage._SQLITE_TRACE_HISTORY.clear()
        storage._SQLITE_TRACE_LONGEST_LOCKS.clear()
        storage._SQLITE_TRACE_BY_TABLE.clear()
        storage._SQLITE_TRACE_BY_PATH.clear()
        for key in list(storage._SQLITE_TRACE_TOTALS.keys()):
            value = storage._SQLITE_TRACE_TOTALS.get(key)
            storage._SQLITE_TRACE_TOTALS[key] = 0.0 if isinstance(value, float) else 0


def _table_stats(snapshot: dict[str, object], table_name: str) -> dict[str, object]:
    for row in list(snapshot.get("top_write_tables") or []):
        if str((row or {}).get("table") or "") == str(table_name):
            return dict(row or {})
    return {}


def _path_stats(snapshot: dict[str, object], path_name: str) -> dict[str, object]:
    for row in list(snapshot.get("top_contention_paths") or []):
        if str((row or {}).get("path") or "") == str(path_name):
            return dict(row or {})
    return {}


def test_safe_no_credential_child_candidates_only_run_yfinance_polling() -> None:
    with patch.dict(
        os.environ,
        {
            "ENGINE_MODE": "safe",
            "EXECUTION_MODE": "safe",
            "INGESTION_CHILD_JOBS": "",
            "POLYGON_WS_ENABLED": "0",
            "POLYGON_REST_ENABLED": "0",
            "IBKR_ENABLED": "0",
            "CCXT_ENABLED": "0",
            "TRADIER_ENABLED": "0",
            "YFINANCE_ENABLED": "1",
        },
        clear=False,
    ):
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")
        with patch.object(
            ingestion_runtime,
            "desired_ingestion_jobs",
            return_value=[
                "stream_prices_polygon_ws",
                "options_poll",
                "poll_prices",
                "ingest_now",
                "poll_macro",
            ],
        ):
            candidates = ingestion_runtime._child_candidates()

    assert candidates == ["poll_prices"]


def test_safe_no_credential_child_spawn_sanitizes_provider_credentials() -> None:
    with patch.dict(
        os.environ,
        {
            "ENGINE_MODE": "safe",
            "EXECUTION_MODE": "safe",
            "BROKER": "sim",
            "BROKER_NAME": "sim",
            "DISABLE_LIVE_EXECUTION": "1",
            "KILL_SWITCH_GLOBAL": "1",
            "POLYGON_API_KEY": "dummy",
            "TRADIER_API_TOKEN": "dummy",
            "IBKR_HOST": "dummy",
            "FMP_API_KEY": "dummy",
            "ALPACA_KEY_ID": "dummy",
            "ALPACA_SECRET_KEY": "dummy",
            "OPENAI_API_KEY": "dummy",
            "POLYGON_WS_ENABLED": "1",
            "TRADIER_ENABLED": "1",
            "YFINANCE_ENABLED": "1",
        },
        clear=False,
    ):
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")
        captured: dict[str, dict[str, str]] = {}

        def _fake_popen(*_args, **kwargs):
            captured["env"] = dict(kwargs.get("env") or {})
            return SimpleNamespace(pid=12345, poll=lambda: None)

        with patch.object(ingestion_runtime.subprocess, "Popen", side_effect=_fake_popen):
            ingestion_runtime._spawn_child_once("poll_prices")

    env = captured["env"]
    assert env["YFINANCE_ENABLED"] == "1"
    assert env["POLYGON_WS_ENABLED"] == "0"
    assert env["TRADIER_ENABLED"] == "0"
    assert "POLYGON_API_KEY" not in env
    assert "TRADIER_API_TOKEN" not in env
    assert "IBKR_HOST" not in env
    assert "FMP_API_KEY" not in env
    assert "ALPACA_KEY_ID" not in env
    assert "ALPACA_SECRET_KEY" not in env
    assert "OPENAI_API_KEY" not in env


def test_safe_no_credential_child_spawn_sanitizes_control_plane_overlay() -> None:
    with patch.dict(
        os.environ,
        {
            "ENGINE_MODE": "safe",
            "EXECUTION_MODE": "safe",
            "BROKER": "sim",
            "BROKER_NAME": "sim",
            "DISABLE_LIVE_EXECUTION": "1",
            "KILL_SWITCH_GLOBAL": "1",
            "YFINANCE_ENABLED": "1",
        },
        clear=False,
    ):
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")
        captured: dict[str, dict[str, str]] = {}

        def _fake_popen(*_args, **kwargs):
            captured["env"] = dict(kwargs.get("env") or {})
            return SimpleNamespace(pid=12345, poll=lambda: None)

        fake_manager = SimpleNamespace(
            build_job_environment=lambda _job_name: {
                "POLYGON_API_KEY": "dummy",
                "ALPACA_KEY_ID": "dummy",
                "ALPACA_SECRET_KEY": "dummy",
                "OPENAI_API_KEY": "dummy",
                "POLYGON_REST_ENABLED": "1",
                "LIVE_PRICE_PROVIDER_CHAIN": "polygon,yfinance",
            }
        )

        with patch.object(ingestion_runtime, "get_manager", return_value=fake_manager):
            with patch.object(ingestion_runtime.subprocess, "Popen", side_effect=_fake_popen):
                ingestion_runtime._spawn_child_once("poll_prices")

    env = captured["env"]
    assert env["YFINANCE_ENABLED"] == "1"
    assert env["POLYGON_REST_ENABLED"] == "0"
    assert env["LIVE_PRICE_PROVIDER_CHAIN"] == "yfinance"
    assert "POLYGON_API_KEY" not in env
    assert "ALPACA_KEY_ID" not in env
    assert "ALPACA_SECRET_KEY" not in env
    assert "OPENAI_API_KEY" not in env


class IngestionRuntimeReliabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_liveness_queue_enabled = os.environ.get("SQLITE_LIVENESS_QUEUE_ENABLED")
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "ingestion_runtime.db")
        os.environ["SQLITE_LIVENESS_QUEUE_ENABLED"] = "0"
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(
                f"[test_ingestion_runtime_reliability] close_pooled_connections_failed: {type(e).__name__}: {e}\n"
            )
        try:
            telemetry_append_buffer = importlib.import_module("engine.runtime.telemetry_append_buffer")
            telemetry_append_buffer.shutdown_telemetry_append_buffers(timeout_s=1.0)
        except Exception as e:
            sys.stderr.write(
                f"[test_ingestion_runtime_reliability] shutdown_telemetry_append_buffers_failed: "
                f"{type(e).__name__}: {e}\n"
            )
        if self.prev_liveness_queue_enabled is None:
            os.environ.pop("SQLITE_LIVENESS_QUEUE_ENABLED", None)
        else:
            os.environ["SQLITE_LIVENESS_QUEUE_ENABLED"] = str(self.prev_liveness_queue_enabled)
        self.tmp.cleanup()

    def test_polling_price_max_age_reads_sqlite_row_heartbeat_payloads(self) -> None:
        (storage, ingestion_runtime) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )

        heartbeat_extra = json.dumps(
            {
                "poll_seconds": 30,
                "providers": {
                    "yfinance": {
                        "connected": True,
                        "manager_state": "healthy",
                        "last_msg_age_ms": 1000,
                        "capabilities": {
                            "streaming": False,
                            "polling": True,
                        },
                    }
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        storage.put_job_heartbeat("poll_prices", "test-owner", 1234, heartbeat_extra)

        # Sanity check that the helper is reading sqlite3.Row-backed query results.
        con = storage.connect_ro()
        try:
            row = con.execute(
                "SELECT extra_json FROM job_heartbeats WHERE job_name='poll_prices'"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertIsInstance(row, sqlite3.Row)
        finally:
            con.close()

        self.assertEqual(ingestion_runtime._polling_price_max_age_ms(), 75000)

    def test_job_heartbeat_hot_path_failing_before_locked_flush_visibility_fixed_after_trace_and_requeue_flush(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SQLITE_LIVENESS_QUEUE_ENABLED": "1",
                "SQLITE_LIVENESS_DB_ENABLED": "0",
                "SQLITE_LIVENESS_FLUSH_INTERVAL_S": "60",
                "SQLITE_LIVENESS_BUSY_TIMEOUT_MS": "50",
                "SQLITE_LIVENESS_CONNECT_TIMEOUT_S": "0.05",
                "SQLITE_TRACE_REPORT_EVERY_S": "0",
            },
            clear=False,
        ):
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.init_db()

            con = storage.connect_rw_direct()
            try:
                con.execute(
                    """
                    INSERT INTO job_locks(job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms, expires_ms)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("poll_prices", "test-owner", 1234, 1, 1, 1),
                )
                con.commit()
            finally:
                con.close()

            _reset_sqlite_trace(storage)

            lock_con = sqlite3.connect(os.environ["DB_PATH"], timeout=0.05, isolation_level=None)
            try:
                lock_con.execute("PRAGMA journal_mode=WAL;")
                lock_con.execute("BEGIN IMMEDIATE")
                lock_con.execute(
                    "UPDATE job_locks SET heartbeat_ts_ms=? WHERE job_name=?",
                    (2, "poll_prices"),
                )

                with patch.object(storage, "_ensure_job_liveness_writer_started", return_value=None):
                    storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"phase":"a"}')
                batch = storage._drain_job_liveness_batch(force=True)

                self.assertEqual(len(batch), 1)
                with self.assertRaises(sqlite3.OperationalError):
                    storage._flush_job_liveness_batch(batch)
                storage._requeue_job_liveness_batch(batch)

                locked_trace = dict((storage.get_connection_debug_snapshot().get("sqlite_trace") or {}))
                job_heartbeats = _table_stats(locked_trace, "job_heartbeats")
                flush_path = _path_stats(locked_trace, "storage.py:_flush_job_liveness_batch")

                self.assertGreaterEqual(
                    int((job_heartbeats.get("lock_errors") or 0)),
                    1,
                    "failing_before_locked_job_heartbeat_visibility would have missed the contended job_heartbeats write table",
                )
                self.assertGreaterEqual(
                    int((flush_path.get("lock_errors") or 0)),
                    1,
                    "failing_before_locked_job_heartbeat_visibility would have hidden storage.py:_flush_job_liveness_batch as the hot contention path",
                )
                self.assertIn(
                    "database is locked",
                    str(flush_path.get("last_error") or ""),
                )
                self.assertGreaterEqual(
                    int((storage._job_liveness_queue_snapshot().get("pending_count") or 0)),
                    1,
                    "fixed_after_requeue should keep the pending heartbeat available for a follow-up flush after the write lock clears",
                )
            finally:
                try:
                    lock_con.rollback()
                finally:
                    lock_con.close()

            flushed = storage.flush_job_liveness_queue(max_batches=4, force=True)
            self.assertEqual(int(flushed.get("flushed") or 0), 1)

            con = storage.connect_ro_direct()
            try:
                row = con.execute(
                    "SELECT extra_json FROM job_heartbeats WHERE job_name=?",
                    ("poll_prices",),
                ).fetchone()
            finally:
                con.close()

            self.assertIsNotNone(row)
            self.assertEqual(str(row[0] or ""), '{"phase":"a"}')

    def test_poll_prices_post_commit_cleanup_resets_leaked_pooled_writer(self) -> None:
        storage, poll_prices, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.data.poll_prices",
            "engine.runtime.runtime_meta",
        )

        def _leaky_emit_alert(**_kwargs):
            con = storage.connect()
            con.begin_managed_write()
            con.execute("CREATE TABLE IF NOT EXISTS _tmp_alert_probe(id INTEGER PRIMARY KEY)")
            return 1

        with patch.object(poll_prices, "emit_alert", side_effect=_leaky_emit_alert):
            with patch.object(poll_prices, "set_state") as set_state_mock:
                poll_prices._finalize_post_commit_price_cycle(
                    [
                        {
                            "event_title": "probe",
                            "symbol": "AAPL",
                            "horizon_s": 300,
                            "expected_z": 2.0,
                            "confidence": 0.9,
                            "explain": {"model_name": "probe_model", "model_id": "probe_model"},
                        }
                    ],
                    {"provider": "polygon", "first_ts_ms": 1234567890},
                )
                set_state_mock.assert_called_once_with(poll_prices.LIVE, "first_market_data_tick")

        runtime_meta.flush_best_effort_runtime_meta_buffer(max_batches=4)

        self.assertFalse(bool(storage.connect().in_transaction))

        con = storage.connect_ro_direct()
        try:
            rows = con.execute(
                "SELECT key, value FROM runtime_meta WHERE key IN ('price_provider_active', 'first_price_ts_ms')"
            ).fetchall()
        finally:
            con.close()

        values = {str(row[0]): str(row[1]) for row in rows}
        self.assertEqual(values.get("price_provider_active"), "polygon")
        self.assertEqual(values.get("first_price_ts_ms"), "1234567890")

    def test_provider_health_rejection_stays_off_sync_sqlite_path(self) -> None:
        storage, poll_prices = _reload_modules(
            "engine.runtime.storage",
            "engine.data.poll_prices",
        )
        storage.init_db()

        class _FakeManager:
            def __init__(self) -> None:
                self.calls: list[dict[str, object]] = []

            def record_source_status(self, provider, **kwargs) -> None:
                payload = dict(kwargs)
                payload["provider"] = str(provider)
                self.calls.append(payload)

        manager = _FakeManager()
        nonfatal_calls: list[dict[str, object]] = []

        def _capture_nonfatal(event, exc, **context):
            nonfatal_calls.append(
                {
                    "event": str(event),
                    "error": f"{type(exc).__name__}:{exc}",
                    "context": dict(context),
                }
            )

        with patch.object(poll_prices, "enqueue_price_provider_health", return_value=False):
            with patch.object(
                poll_prices,
                "get_telemetry_append_buffer_snapshot",
                return_value={
                    "enabled": True,
                    "buffered_rows": 1,
                    "dropped_by_table": {"price_provider_health": 1},
                    "last_rejected_reason": "buffer_overflow",
                    "last_rejected_table": "price_provider_health",
                    "pending_by_table": {"price_provider_health": 1},
                },
            ):
                with patch.object(
                    poll_prices,
                    "run_write_txn",
                    side_effect=AssertionError("unexpected_sync_provider_health_write"),
                ):
                    with patch.object(poll_prices, "_log_nonfatal", side_effect=_capture_nonfatal):
                        buffered = poll_prices._record_provider_health_telemetry(
                            manager,
                            provider="polygon_ws",
                            ok=False,
                            latency_ms=25,
                            n_symbols=4,
                            error="sqlite busy",
                            ts_ms=1_700_000_000_000,
                        )

        self.assertFalse(bool(buffered))
        self.assertEqual(len(manager.calls), 1)
        self.assertEqual(str(manager.calls[0].get("provider") or ""), "polygon_ws")
        self.assertTrue(bool(manager.calls[0].get("best_effort")))
        self.assertTrue(
            any(str(call.get("event") or "") == "poll_prices_provider_health_buffer_rejected" for call in nonfatal_calls)
        )

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM price_provider_health WHERE provider=?", ("polygon_ws",)).fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 0)

    def test_poll_prices_reclaims_feed_lock_from_dead_owner(self) -> None:
        storage, poll_prices = _reload_modules(
            "engine.runtime.storage",
            "engine.data.poll_prices",
        )
        storage.init_db()

        con = storage.connect()
        try:
            poll_prices._ensure_price_feed_lock_table(con)
            con.execute(
                "INSERT OR REPLACE INTO price_feed_lock(id, owner, pid, ts_ms) VALUES(1, ?, ?, ?)",
                ("poll_prices", 999999, int(time.time() * 1000)),
            )
            con.commit()
        finally:
            con.close()

        self.assertTrue(poll_prices._acquire_price_feed_lock())

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT owner, pid FROM price_feed_lock WHERE id=1").fetchone()
        finally:
            con.close()

        self.assertEqual(str(row[0]), "poll_prices")
        self.assertEqual(int(row[1]), int(poll_prices.PID))

    def test_latest_prices_state_filters_disabled_and_non_price_providers(self) -> None:
        storage, ingestion_runtime, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
            "services.data_source_manager",
        )
        storage.init_db()
        data_source_manager.get_manager().initialize()

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.execute(
                "UPDATE data_sources SET enabled=1 WHERE source_key='polygon'"
            )
            con.execute(
                "UPDATE data_sources SET enabled=0 WHERE source_key IN ('polygon_ws','tradier')"
            )
            con.execute(
                """
                INSERT INTO price_provider_health(
                  ts_ms, provider, ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), "polygon", 1, 25, 4, None, int(now_ms), 0),
            )
            con.execute(
                """
                INSERT INTO price_provider_health(
                  ts_ms, provider, ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), "polygon_ws", 0, 25, 4, "stale", int(now_ms - 1000), 3),
            )
            con.execute(
                """
                INSERT INTO price_provider_health(
                  ts_ms, provider, ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (int(now_ms), "tradier", 0, 25, 4, "options provider", int(now_ms - 1000), 3),
            )
            con.commit()
        finally:
            con.close()

        snapshot = ingestion_runtime._latest_prices_state()
        providers = dict(snapshot.get("providers") or {})

        self.assertIn("polygon", providers)
        self.assertNotIn("polygon_ws", providers)
        self.assertNotIn("tradier", providers)

    def test_feed_stall_restart_waits_for_first_price_tick_latch(self) -> None:
        storage, ingestion_runtime, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()

        now = 1_700_000_000.0
        heartbeat_extra = json.dumps(
            {
                "heartbeat_every_s": 2,
                "poll_seconds": 30,
                "telemetry": {
                    "connected": True,
                    "last_msg_age_ms": 1000,
                    "manager_state": "healthy",
                    "capabilities": {"polling": True, "streaming": False},
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        storage.put_job_heartbeat("poll_prices", "test-owner", 4321, heartbeat_extra)

        class _FakeProc:
            def __init__(self, pid: int):
                self.pid = int(pid)

            def poll(self):
                return None

        def _children() -> dict[str, dict[str, object]]:
            return {
                "poll_prices": {
                    "job": "poll_prices",
                    "pid": 4321,
                    "proc": _FakeProc(4321),
                    "running": True,
                    "restart_disabled": False,
                    "last_start_ts": now - float(ingestion_runtime.CHILD_STARTUP_GRACE_S) - 5.0,
                    "restart_delay_s": float(ingestion_runtime.RESTART_BASE_S),
                }
            }

        latest_state = {
            "healthy_providers": 0,
            "price_age_ms": int(10**12),
            "provider_errors": {},
            "providers": {},
        }

        with patch.object(ingestion_runtime.subprocess, "Popen", _FakeProc):
            with patch.object(ingestion_runtime, "_terminate_child") as terminate_mock:
                with patch.object(ingestion_runtime, "publish_message") as publish_mock:
                    children = _children()
                    ingestion_runtime._restart_children_for_feed_stall(children, latest_state, now)

        terminate_mock.assert_not_called()
        publish_mock.assert_not_called()
        self.assertTrue(bool(children["poll_prices"]["running"]))
        self.assertEqual(int(children["poll_prices"]["pid"] or 0), 4321)

        runtime_meta.meta_set("first_price_ts_ms", str(int((now - 10.0) * 1000.0)))

        with patch.object(ingestion_runtime.subprocess, "Popen", _FakeProc):
            with patch.object(ingestion_runtime, "_terminate_child") as terminate_mock:
                with patch.object(ingestion_runtime, "publish_message") as publish_mock:
                    children = _children()
                    ingestion_runtime._restart_children_for_feed_stall(children, latest_state, now)

        terminate_mock.assert_called_once()
        publish_mock.assert_called_once()
        self.assertFalse(bool(children["poll_prices"]["running"]))
        self.assertEqual(int(children["poll_prices"]["pid"] or 0), 0)

    def test_manager_backfills_missing_builtin_credentials_from_env(self) -> None:
        prev_polygon_key = os.environ.get("POLYGON_API_KEY")
        try:
            os.environ.pop("POLYGON_API_KEY", None)
            storage, data_source_manager = _reload_modules(
                "engine.runtime.storage",
                "services.data_source_manager",
            )
            storage.init_db()

            cold_manager = data_source_manager.DataSourceManager()
            cold_manager.initialize()
            self.assertEqual(
                dict((cold_manager.get_source("polygon_ws", include_credentials=True) or {}).get("credentials") or {}),
                {},
            )

            os.environ["POLYGON_API_KEY"] = "polygon-test-key"
            warm_manager = data_source_manager.DataSourceManager()
            warm_manager.initialize()

            polygon_ws = warm_manager.get_source("polygon_ws", include_credentials=True) or {}
            polygon_rest = warm_manager.get_source("polygon", include_credentials=True) or {}

            self.assertEqual(str((polygon_ws.get("credentials") or {}).get("api_key") or ""), "polygon-test-key")
            self.assertEqual(str((polygon_rest.get("credentials") or {}).get("api_key") or ""), "polygon-test-key")
        finally:
            if prev_polygon_key is None:
                os.environ.pop("POLYGON_API_KEY", None)
            else:
                os.environ["POLYGON_API_KEY"] = str(prev_polygon_key)

    def test_options_poll_can_be_desired_from_polygon_provider(self) -> None:
        prev_polygon_key = os.environ.get("POLYGON_API_KEY")
        prev_tradier_enabled = os.environ.get("TRADIER_ENABLED")
        try:
            os.environ["POLYGON_API_KEY"] = "polygon-test-key"
            os.environ["TRADIER_ENABLED"] = "0"
            storage, data_source_manager = _reload_modules(
                "engine.runtime.storage",
                "services.data_source_manager",
            )
            storage.init_db()

            manager = data_source_manager.DataSourceManager()
            manager.initialize()

            con = storage.connect()
            try:
                con.execute("UPDATE data_sources SET enabled=0 WHERE source_key='tradier'")
                con.commit()
            finally:
                con.close()

            desired_jobs = set(manager.get_desired_ingestion_jobs())
            options_env = manager.build_job_environment("options_poll")

            self.assertIn("options_poll", desired_jobs)
            self.assertEqual(str(options_env.get("OPTIONS_PROVIDER_CHAIN") or ""), "polygon")
            self.assertEqual(str(options_env.get("TRADIER_ENABLED") or ""), "0")
        finally:
            if prev_polygon_key is None:
                os.environ.pop("POLYGON_API_KEY", None)
            else:
                os.environ["POLYGON_API_KEY"] = str(prev_polygon_key)
            if prev_tradier_enabled is None:
                os.environ.pop("TRADIER_ENABLED", None)
            else:
                os.environ["TRADIER_ENABLED"] = str(prev_tradier_enabled)

    def test_stocktwits_poll_raises_on_http_block(self) -> None:
        storage, stocktwits = _reload_modules(
            "engine.runtime.storage",
            "engine.data.jobs.poll_social_stocktwits",
        )
        storage.init_db()

        class _BlockedResponse:
            status_code = 403
            text = "blocked by upstream"

            def json(self):
                return {}

        con = storage.connect()
        try:
            with patch("engine.data.jobs.poll_social_stocktwits.requests.get", return_value=_BlockedResponse()):
                with self.assertRaisesRegex(RuntimeError, "stocktwits_http_403"):
                    stocktwits._poll_once(con)
        finally:
            con.close()

    def test_write_ingestion_state_includes_per_source_health_timestamps(self) -> None:
        storage, ingestion_runtime, ingestion_status, runtime_meta, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
            "engine.runtime.ingestion_status",
            "engine.runtime.runtime_meta",
            "services.data_source_manager",
        )
        storage.init_db()
        data_source_manager.get_manager().initialize()

        now_ms = int(time.time() * 1000)
        ingestion_status.record_pipeline_status(
            "ingest_now",
            ok=True,
            raw_rows=3,
            event_rows=2,
            last_ingested_ts_ms=int(now_ms - 1000),
        )
        ingestion_status.record_pipeline_status(
            "poll_social_stocktwits",
            ok=True,
            raw_rows=2,
            event_rows=2,
            last_ingested_ts_ms=int(now_ms - 2000),
        )
        ingestion_status.record_pipeline_status(
            "poll_macro",
            ok=True,
            raw_rows=1,
            event_rows=1,
            last_ingested_ts_ms=int(now_ms - 3000),
        )
        ingestion_status.record_pipeline_status(
            "poll_weather_forecasts",
            ok=True,
            raw_rows=1,
            event_rows=1,
            last_ingested_ts_ms=int(now_ms - 4000),
        )
        ingestion_status.record_pipeline_status(
            "poll_weather_alerts",
            ok=True,
            raw_rows=1,
            event_rows=1,
            last_ingested_ts_ms=int(now_ms - 5000),
        )

        ingestion_runtime._INGESTION_STATE["last_publish_ts_ms"] = int(now_ms)
        ingestion_runtime._INGESTION_STATE["last_tick_ts_ms"] = int(now_ms)
        ingestion_runtime._INGESTION_STATE["healthy_providers"] = 1
        ingestion_runtime._INGESTION_STATE["running"] = True
        ingestion_runtime._INGESTION_STATE["stale"] = False

        with patch.object(
            ingestion_runtime,
            "_latest_prices_state",
            return_value={
                "last_price_ts_ms": int(now_ms),
                "price_age_ms": 0,
                "healthy_providers": 1,
                "providers": {"polygon": {"ok": True}},
            },
        ):
            ingestion_runtime._write_ingestion_state({}, provider_status="running")

        payload = json.loads(str(runtime_meta.meta_get("ingestion_state", "") or "{}"))
        source_health = dict(payload.get("source_health") or {})
        sources = dict(source_health.get("sources") or {})

        self.assertIn("prices", sources)
        self.assertIn("news", sources)
        self.assertIn("social", sources)
        self.assertIn("macro", sources)
        self.assertIn("weather", sources)
        self.assertGreater(int((sources.get("prices") or {}).get("last_update_ts_ms") or 0), 0)
        self.assertGreater(int((sources.get("news") or {}).get("last_update_ts_ms") or 0), 0)
        self.assertGreater(int((sources.get("social") or {}).get("last_update_ts_ms") or 0), 0)

    def test_builtin_sources_cannot_be_deleted(self) -> None:
        storage, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "services.data_source_manager",
        )
        storage.init_db()

        manager = data_source_manager.DataSourceManager()
        manager.initialize()

        with self.assertRaisesRegex(ValueError, "builtin_source_delete_not_allowed:polygon"):
            manager.delete_source("polygon")

    def test_custom_creation_is_limited_to_rss_feeds(self) -> None:
        storage, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "services.data_source_manager",
        )
        storage.init_db()

        manager = data_source_manager.DataSourceManager()
        manager.initialize()

        with self.assertRaisesRegex(ValueError, "unsupported_custom_source:custom_price"):
            manager.create_source(
                {
                    "source_key": "custom_price",
                    "display_name": "Custom Price Feed",
                    "source_type": "price_provider",
                    "provider_name": "custom_price",
                    "job_name": "poll_prices",
                }
            )

        rss = manager.create_source(
            {
                "source_key": "rss:custom_market_watch",
                "display_name": "Custom Market Watch",
                "source_type": "rss_feed",
                "settings": {
                    "name": "Custom Market Watch",
                    "url": "https://example.com/feed.xml",
                },
            }
        )

        self.assertEqual(str(rss.get("source_type") or ""), "rss_feed")
        self.assertEqual(str(rss.get("provider_name") or ""), "rss")
        self.assertEqual(str(rss.get("job_name") or ""), "ingest_now")

    def test_update_source_can_clear_credentials_explicitly(self) -> None:
        storage, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "services.data_source_manager",
        )
        storage.init_db()

        manager = data_source_manager.DataSourceManager()
        manager.initialize()
        manager.update_source(
            {
                "source_key": "polygon",
                "credentials": {"api_key": "alpha-secret"},
                "replace_credentials": True,
                "actor": "tester",
            }
        )

        before = manager.get_source("polygon", include_credentials=True) or {}
        self.assertEqual(str((before.get("credentials") or {}).get("api_key") or ""), "alpha-secret")

        manager.update_source(
            {
                "source_key": "polygon",
                "clear_credential_fields": ["api_key"],
                "actor": "tester",
            }
        )

        after = manager.get_source("polygon", include_credentials=True) or {}
        self.assertEqual(dict(after.get("credentials") or {}), {})

    def test_source_templates_expose_truthful_mutation_rules(self) -> None:
        storage, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "services.data_source_manager",
        )
        storage.init_db()

        manager = data_source_manager.DataSourceManager()
        manager.initialize()
        templates = {str(item.get("template_key") or ""): item for item in manager.list_source_templates()}

        polygon = templates.get("polygon") or {}
        rss = templates.get("rss_feed") or {}

        self.assertFalse(bool(polygon.get("allow_delete")))
        self.assertFalse(bool(polygon.get("allow_create")))
        self.assertEqual(str(polygon.get("job_name") or ""), "poll_prices")
        self.assertTrue(bool(rss.get("allow_create")))
        self.assertTrue(bool(rss.get("allow_delete")))
        self.assertEqual(str(rss.get("provider_name") or ""), "rss")


if __name__ == "__main__":
    unittest.main()
