"""Regression tests for ingestion runtime freshness handling."""

from __future__ import annotations

import importlib
import base64
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


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


def test_ingestion_child_spawn_uses_bounded_child_pool_profile() -> None:
    with patch.dict(
        os.environ,
        {
            "ENGINE_MODE": "safe",
            "EXECUTION_MODE": "safe",
            "YFINANCE_ENABLED": "1",
            "INGESTION_TUNING_PROFILE": "host_32t_123g",
            "TRADING_CPU_THREAD_POLICY": "auto",
            "RUNTIME_CPUS": "32",
            "TS_PG_POOL_SIZE": "12",
            "TIMESCALE_POOL_MAX_SIZE": "8",
            "TIMESCALE_PRICES_POOL_MAX_SIZE": "8",
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
    assert env["ENGINE_PROCESS_ROLE"] == "ingestion_child"
    assert env["TS_PG_POOL_PROFILE"] == "jobs"
    assert env["TS_PG_POOL_SIZE"] == "3"
    assert env["TS_PG_POOL_MIN_SIZE"] == "1"
    assert env["TIMESCALE_POOL_MAX_SIZE"] == "4"
    assert env["TIMESCALE_PRICES_POOL_MAX_SIZE"] == "4"
    assert env["ASYNC_PRICE_WRITER_WORKERS"] == "4"
    assert env["ENGINE_CPU_THREAD_POLICY_ROLE"] == "ingestion_child"
    assert env["OMP_NUM_THREADS"] == "1"
    assert env["MKL_NUM_THREADS"] == "1"
    assert env["OPENBLAS_NUM_THREADS"] == "1"
    assert env["NUMEXPR_NUM_THREADS"] == "1"
    assert env["TORCH_CPU_THREADS"] == "1"
    assert env["TORCH_INTEROP_THREADS"] == "1"


class IngestionRuntimeReliabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_db_path = os.environ.get("DB_PATH")
        self.prev_storage_backend = os.environ.get("TS_STORAGE_BACKEND")
        self.prev_liveness_queue_enabled = os.environ.get("SQLITE_LIVENESS_QUEUE_ENABLED")
        self.prev_data_source_master_key = os.environ.get("DATA_SOURCE_MASTER_KEY")
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "ingestion_runtime.db")
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["SQLITE_LIVENESS_QUEUE_ENABLED"] = "0"
        os.environ["DATA_SOURCE_MASTER_KEY"] = VALID_DATA_SOURCE_MASTER_KEY
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
        if self.prev_data_source_master_key is None:
            os.environ.pop("DATA_SOURCE_MASTER_KEY", None)
        else:
            os.environ["DATA_SOURCE_MASTER_KEY"] = str(self.prev_data_source_master_key)
        if self.prev_db_path is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = str(self.prev_db_path)
        if self.prev_storage_backend is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = str(self.prev_storage_backend)
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(
                f"[test_ingestion_runtime_reliability] restore_storage_failed: {type(e).__name__}: {e}\n"
            )
        self.tmp.cleanup()

    def test_provider_router_opens_circuit_after_repeated_health_failures(self) -> None:
        storage, live_schema, provider_router = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.storage_live_ingestion_schema",
            "engine.data.provider_router",
        )
        storage.init_db()
        now_ms = int(time.time() * 1000)
        schema_con = storage.connect()
        try:
            live_schema.ensure_price_provider_health_schema(
                schema_con,
                warn_nonfatal=lambda *_args, **_kwargs: None,
            )
            schema_con.commit()
        finally:
            schema_con.close()
        con = sqlite3.connect(os.environ["DB_PATH"])
        try:
            con.execute(
                """
                INSERT INTO price_provider_health(
                  provider, ts_ms, ok, latency_ms, n_symbols, error,
                  last_success_ts_ms, error_count
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    "polygon",
                    int(now_ms),
                    0,
                    250,
                    0,
                    "credential_error:403",
                    int(now_ms - 60_000),
                    3,
                ),
            )
            con.commit()
        finally:
            con.close()

        health = provider_router.compute_provider_health()
        polygon = dict(health.get("polygon") or {})

        self.assertFalse(bool(polygon.get("ok")))
        self.assertTrue(bool(polygon.get("circuit_open")))
        self.assertEqual(str(polygon.get("status")), "CIRCUIT_OPEN")
        self.assertEqual(int(polygon.get("error_count") or 0), 3)
        self.assertEqual(float(polygon.get("score") or 0.0), 0.0)

    def test_weather_forecast_unsupported_provider_records_failure(self) -> None:
        (weather_forecasts,) = _reload_modules("engine.data.jobs.poll_weather_forecasts")
        provider_health: dict[str, object] = {}
        pipeline_status: dict[str, object] = {}
        manager_status: dict[str, object] = {}

        class _Manager:
            def record_job_status(self, job_name, ok, message="", error="", meta=None):
                manager_status.update(
                    {
                        "job_name": job_name,
                        "ok": bool(ok),
                        "message": str(message),
                        "error": str(error or ""),
                        "meta": dict(meta or {}),
                    }
                )

        def _record_pipeline_status(job_name, **kwargs):
            pipeline_status.update({"job_name": job_name, **kwargs})
            return {"job_name": job_name, **kwargs}

        def _append_weather_provider_health(**kwargs):
            provider_health.update(dict(kwargs))

        with patch.object(weather_forecasts, "WEATHER_PROVIDER", "unsupported_weather"):
            with patch.object(weather_forecasts, "get_manager", return_value=_Manager()):
                with patch.object(
                    weather_forecasts,
                    "_load_region_map",
                    return_value={"regions": {"midwest": {"lat": 41.88, "lon": -87.63}}},
                ):
                    with patch.object(weather_forecasts, "put_job_heartbeat", return_value=None):
                        with patch.object(weather_forecasts, "touch_job_lock", return_value=None):
                            with patch.object(
                                weather_forecasts,
                                "append_weather_provider_health",
                                side_effect=_append_weather_provider_health,
                            ):
                                with patch.object(
                                    weather_forecasts,
                                    "record_pipeline_status",
                                    side_effect=_record_pipeline_status,
                                ):
                                    weather_forecasts._run_once()

        self.assertFalse(bool(provider_health.get("ok")))
        self.assertIn("unsupported_weather_provider", str(provider_health.get("error") or ""))
        self.assertFalse(bool(pipeline_status.get("ok")))
        self.assertIn("unsupported_weather_provider", str(pipeline_status.get("error") or ""))
        self.assertFalse(bool(manager_status.get("ok")))
        self.assertIn("unsupported_weather_provider", str(manager_status.get("error") or ""))

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

    def test_poll_prices_post_commit_updates_runtime_meta_without_pool_reset(self) -> None:
        storage, poll_prices, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.data.poll_prices",
            "engine.runtime.runtime_meta",
        )

        close_calls: list[object] = []

        with patch.object(storage, "close_pooled_connections", side_effect=lambda: close_calls.append(True)):
            with patch.object(poll_prices, "emit_alert", return_value=1):
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

        self.assertFalse(hasattr(poll_prices, "close_pooled_connections"))
        self.assertEqual(close_calls, [])

        runtime_meta.flush_best_effort_runtime_meta_buffer(max_batches=4)

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

    def test_poll_prices_status_records_provider_snapshot_observability(self) -> None:
        _storage, poll_prices = _reload_modules(
            "engine.runtime.storage",
            "engine.data.poll_prices",
        )

        recorded: dict[str, object] = {}
        job_status: dict[str, object] = {}

        class _FakeManager:
            def record_job_status(self, *args, **kwargs):
                job_status["args"] = args
                job_status["kwargs"] = kwargs

        def _record_pipeline_status(*args, **kwargs):
            recorded["args"] = args
            recorded["kwargs"] = kwargs
            return {"ok": kwargs["ok"], "meta": kwargs["meta"]}

        with patch.object(poll_prices, "record_pipeline_status", side_effect=_record_pipeline_status):
            status = poll_prices._record_poll_prices_status(
                _FakeManager(),
                ok=True,
                providers=["yfinance", "polygon"],
                provider_errors={"polygon": "timeout"},
                provider_latencies_ms={"yfinance": 12, "polygon": 503},
                provider_result_counts={"yfinance": 4, "polygon": 0},
                message="unit test",
            )

        self.assertEqual(status["meta"]["provider_errors"], {"polygon": "timeout"})
        self.assertEqual(status["meta"]["provider_latencies_ms"], {"yfinance": 12, "polygon": 503})
        self.assertEqual(status["meta"]["provider_result_counts"], {"yfinance": 4, "polygon": 0})
        self.assertEqual(recorded["kwargs"]["meta"]["provider_errors"], {"polygon": "timeout"})
        self.assertEqual(job_status["kwargs"]["meta"]["provider_latencies_ms"]["polygon"], 503)

    def test_poll_prices_collects_rest_provider_snapshots_concurrently_and_isolates_failures(self) -> None:
        _storage, poll_prices = _reload_modules(
            "engine.runtime.storage",
            "engine.data.poll_prices",
        )

        entered: list[str] = []
        entered_lock = threading.Lock()
        both_started = threading.Event()

        class _FakeSession:
            def __init__(self) -> None:
                self.errors: list[str] = []

            def latency_ms(self) -> int:
                return 0

            def note_error(self, error) -> None:
                self.errors.append(str(error))

        class _FakeManager:
            def __init__(self, name, snapshot_fn, *, ok=True) -> None:
                self.name = str(name)
                self._snapshot_fn = snapshot_fn
                self._ok = bool(ok)

            def snapshot(self):
                return self._snapshot_fn(self.name)

            def provider_telemetry(self):
                return {"last_error": ""}

            def ok(self):
                return self._ok

        def good_snapshot(name):
            with entered_lock:
                entered.append(str(name))
                if len(entered) == 2:
                    both_started.set()
            if not both_started.wait(timeout=1.0):
                raise AssertionError("provider snapshots did not overlap")
            return {str(name).upper(): {"price": 101.0, "ts_ms": 123, "source": str(name)}}

        bad_session = _FakeSession()

        def bad_snapshot(_name):
            raise RuntimeError("upstream timeout")

        with patch.object(poll_prices, "POLL_PRICES_PROVIDER_MAX_WORKERS", 2):
            parallel_results = poll_prices._collect_rest_provider_snapshots(
                [
                    ("yfinance", _FakeManager("yfinance", good_snapshot), _FakeSession()),
                    ("polygon", _FakeManager("polygon", good_snapshot), _FakeSession()),
                ]
            )
            failure_results = poll_prices._collect_rest_provider_snapshots(
                [
                    ("yfinance", _FakeManager("yfinance", lambda name: {name: {"price": 1.0}}), _FakeSession()),
                    ("polygon", _FakeManager("polygon", bad_snapshot, ok=False), bad_session),
                ]
            )

        self.assertEqual(set(entered), {"yfinance", "polygon"})
        self.assertTrue(all(result["ok"] for result in parallel_results.values()))
        self.assertTrue(failure_results["yfinance"]["ok"])
        self.assertFalse(failure_results["polygon"]["ok"])
        self.assertIn("RuntimeError: upstream timeout", str(failure_results["polygon"]["error"]))
        self.assertEqual(bad_session.errors, ["RuntimeError: upstream timeout"])

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

    def test_writer_diagnostics_degrades_on_options_durable_buffer_pressure(self) -> None:
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")

        options_status = {
            "ok": False,
            "updated_ts_ms": 1_700_000_000_000,
            "meta": {
                "durable_buffer_pending_rows": 10,
                "durable_buffer_pending_bytes": 2048,
                "durable_buffer_oldest_age_ms": 30_000,
                "durable_buffer_rows_fill_ratio": 0.85,
                "durable_buffer_bytes_fill_ratio": 0.10,
                "durable_buffer_backpressure_active": True,
                "durable_buffer_backpressure_events": 1,
                "durable_buffer_rejected_rows": 4,
                "durable_buffer_dropped_rows": 0,
                "durable_buffer_enqueue_failures": 1,
                "durable_buffer_replay_failures": 0,
                "durable_buffer_delete_failures": 0,
                "durable_buffer_corrupt_payload_rows": 0,
                "durable_buffer_last_error": "spool_row_limit:2",
            },
        }

        with patch("engine.runtime.ingestion_tuning.ingestion_tuning_snapshot", return_value={"ok": True}):
            with patch(
                "engine.runtime.async_writer.get_async_writer",
                return_value=SimpleNamespace(get_snapshot=lambda: {"enabled": False}),
            ):
                with patch(
                    "engine.runtime.storage_pg_prices.get_price_storage",
                    return_value=SimpleNamespace(get_snapshot=lambda: {"enabled": False, "ok": True}),
                ):
                    with patch(
                        "engine.runtime.telemetry_append_buffer.get_telemetry_append_buffer_snapshot",
                        return_value={"enabled": False},
                    ):
                        with patch("engine.runtime.ingestion_status.get_pipeline_status", return_value=options_status):
                            with patch(
                                "engine.runtime.timescale_client.get_timescale_snapshot",
                                return_value={"enabled": False},
                            ):
                                diagnostics = ingestion_runtime._ingestion_writer_diagnostics()

        reasons = set(diagnostics.get("degraded_reasons") or [])
        self.assertIn("options_poll_durable_buffer_pressure", reasons)
        self.assertIn("options_poll_durable_buffer_backpressure", reasons)
        self.assertIn("options_poll_durable_buffer_rejected_rows", reasons)
        self.assertIn("options_poll_durable_buffer_enqueue_failures", reasons)
        options_buffer = dict(diagnostics.get("options_poll_durable_buffer") or {})
        self.assertEqual(int(options_buffer.get("pending_rows") or 0), 10)
        self.assertTrue(bool(options_buffer.get("backpressure_active")))

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

    def test_latest_prices_state_reuses_ttl_snapshot_and_recomputes_ages(self) -> None:
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")
        ingestion_runtime.invalidate_supervisor_snapshot_cache()
        old_ttl = float(ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S)
        base_ms = 1_700_000_000_000
        connect_calls: list[object] = []

        class _Cursor:
            def __init__(self, value):
                self._value = value

            def fetchone(self):
                return self._value

            def fetchall(self):
                return self._value

        class _Connection:
            def execute(self, sql, _params=()):
                text = str(sql)
                if "FROM prices" in text:
                    return _Cursor((1, 1, int(base_ms - 1000)))
                if "FROM price_provider_health" in text:
                    return _Cursor([("polygon", int(base_ms - 1000), 1, 25, 4, None)])
                raise AssertionError(f"unexpected query: {text}")

            def close(self):
                return None

        try:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = 30.0
            with patch.object(
                ingestion_runtime,
                "connect_ro",
                side_effect=lambda: connect_calls.append(True) or _Connection(),
            ):
                with patch.object(ingestion_runtime, "_enabled_price_providers", return_value={"polygon"}):
                    with patch.object(ingestion_runtime, "_polling_price_max_age_ms", return_value=15000):
                        with patch.object(ingestion_runtime, "meta_get", return_value=""):
                            with patch.object(
                                ingestion_runtime.time,
                                "time",
                                side_effect=[
                                    float(base_ms) / 1000.0,
                                    float(base_ms) / 1000.0,
                                    float(base_ms + 5000) / 1000.0,
                                ],
                            ):
                                first = ingestion_runtime._latest_prices_state()
                                second = ingestion_runtime._latest_prices_state()
        finally:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = old_ttl
            ingestion_runtime.invalidate_supervisor_snapshot_cache()

        self.assertEqual(len(connect_calls), 1)
        self.assertEqual(int(first["price_age_ms"]), 1000)
        self.assertEqual(int(second["price_age_ms"]), 6000)
        self.assertEqual(int((second["providers"]["polygon"] or {}).get("age_ms") or 0), 6000)

    def test_latest_prices_state_refreshes_snapshot_after_ttl(self) -> None:
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")
        ingestion_runtime.invalidate_supervisor_snapshot_cache()
        old_ttl = float(ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S)
        base_ms = 1_700_000_000_000
        connect_calls: list[int] = []

        class _Cursor:
            def __init__(self, value):
                self._value = value

            def fetchone(self):
                return self._value

            def fetchall(self):
                return self._value

        class _Connection:
            def __init__(self, call_index: int):
                self._call_index = int(call_index)

            def execute(self, sql, _params=()):
                text = str(sql)
                ts_ms = int(base_ms - (1000 * self._call_index))
                if "FROM prices" in text:
                    return _Cursor((self._call_index, self._call_index, ts_ms))
                if "FROM price_provider_health" in text:
                    return _Cursor([("polygon", ts_ms, 1, 25, 4, None)])
                raise AssertionError(f"unexpected query: {text}")

            def close(self):
                return None

        def _connect():
            call_index = len(connect_calls) + 1
            connect_calls.append(call_index)
            return _Connection(call_index)

        try:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = 1.0
            with patch.object(ingestion_runtime.time, "monotonic", side_effect=[0.0, 0.0, 0.5, 0.5, 1.5, 1.5]):
                with patch.object(ingestion_runtime.time, "time", return_value=float(base_ms) / 1000.0):
                    with patch.object(ingestion_runtime, "connect_ro", side_effect=_connect):
                        with patch.object(ingestion_runtime, "_enabled_price_providers", return_value={"polygon"}):
                            with patch.object(ingestion_runtime, "_polling_price_max_age_ms", return_value=15000):
                                with patch.object(ingestion_runtime, "meta_get", return_value=""):
                                    first = ingestion_runtime._latest_prices_state()
                                    second = ingestion_runtime._latest_prices_state()
                                    third = ingestion_runtime._latest_prices_state()
        finally:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = old_ttl
            ingestion_runtime.invalidate_supervisor_snapshot_cache()

        self.assertEqual(connect_calls, [1, 2])
        self.assertEqual(int(first["fresh_rows"]), 1)
        self.assertEqual(int(second["fresh_rows"]), 1)
        self.assertEqual(int(third["fresh_rows"]), 2)
        self.assertEqual(int(third["price_age_ms"]), 2000)

    def test_child_control_plane_snapshot_reduces_config_reads_until_invalidated(self) -> None:
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")
        ingestion_runtime.invalidate_supervisor_snapshot_cache()
        old_ttl = float(ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S)
        old_marker = ingestion_runtime._LAST_DATA_SOURCES_RELOAD_TS_MS
        hash_calls: list[str] = []

        class _Manager:
            def config_hash_for_job(self, job_name):
                hash_calls.append(str(job_name))
                return f"hash-{len(hash_calls)}"

        try:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = 30.0
            ingestion_runtime._LAST_DATA_SOURCES_RELOAD_TS_MS = None
            with patch.object(ingestion_runtime, "_read_data_sources_reload_ts_ms_uncached", return_value=0):
                with patch.object(ingestion_runtime, "_compute_child_candidates", return_value=["poll_prices"]) as candidates_mock:
                    with patch.object(ingestion_runtime, "get_manager", return_value=_Manager()):
                        children: dict[str, dict[str, object]] = {}
                        first = ingestion_runtime._reconcile_child_control_plane(children, 100.0)
                        second = ingestion_runtime._reconcile_child_control_plane(children, 101.0)

            self.assertEqual(first, ["poll_prices"])
            self.assertEqual(second, ["poll_prices"])
            self.assertEqual(candidates_mock.call_count, 1)
            self.assertEqual(hash_calls, ["poll_prices"])

            ingestion_runtime._LAST_DATA_SOURCES_RELOAD_TS_MS = 100
            with patch.object(ingestion_runtime, "_read_data_sources_reload_ts_ms_uncached", return_value=200):
                ingestion_runtime._invalidate_supervisor_snapshots_if_data_sources_changed()
            with patch.object(ingestion_runtime, "_compute_child_candidates", return_value=["poll_prices"]) as candidates_after:
                with patch.object(ingestion_runtime, "get_manager", return_value=_Manager()):
                    refreshed = ingestion_runtime._reconcile_child_control_plane(children, 102.0)

            self.assertEqual(refreshed, ["poll_prices"])
            self.assertEqual(candidates_after.call_count, 1)
            self.assertEqual(hash_calls, ["poll_prices", "poll_prices"])
        finally:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = old_ttl
            ingestion_runtime._LAST_DATA_SOURCES_RELOAD_TS_MS = old_marker
            ingestion_runtime.invalidate_supervisor_snapshot_cache()

    def test_data_source_dirty_marker_invalidates_supervisor_config_cache_in_process(self) -> None:
        storage, ingestion_runtime, data_source_manager, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
            "services.data_source_manager",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()
        data_source_manager.get_manager().initialize()
        ingestion_runtime.invalidate_supervisor_snapshot_cache()
        ingestion_runtime._SUPERVISOR_SNAPSHOT_CACHE["child_control_plane"] = {
            "expires_at": time.monotonic() + 30.0,
            "value": {"desired": ["poll_prices"], "config_hashes": {"poll_prices": "old"}},
        }
        ingestion_runtime._SUPERVISOR_SNAPSHOT_CACHE["enabled_price_providers"] = {
            "expires_at": time.monotonic() + 30.0,
            "value": {"polygon"},
        }

        data_source_manager.get_manager().mark_runtime_dirty(reason="unit_test_config_change")

        self.assertFalse(
            ingestion_runtime._supervisor_snapshot_cache_has_any(
                ingestion_runtime._DATA_SOURCE_CONFIG_SNAPSHOT_NAMES
            )
        )
        self.assertGreater(int(runtime_meta.meta_get("data_sources_reload_ts_ms", "0") or 0), 0)

    def test_cached_child_heartbeat_snapshot_still_detects_stale_liveness(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        ingestion_runtime.invalidate_supervisor_snapshot_cache()
        old_ttl = float(ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S)
        base_s = 1_700_000_000.0
        heartbeat_extra = json.dumps(
            {
                "heartbeat_every_s": 10,
                "telemetry": {
                    "connected": True,
                    "last_msg_age_ms": 1000,
                    "manager_state": "healthy",
                    "capabilities": {"polling": False, "streaming": True},
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO job_heartbeats(job_name, owner, pid, ts_ms, extra_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("poll_prices", "test-owner", 4321, int(base_s * 1000), heartbeat_extra),
            )
            con.commit()
        finally:
            con.close()

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
                    "last_start_ts": base_s - float(ingestion_runtime.CHILD_STARTUP_GRACE_S) - 5.0,
                    "restart_delay_s": float(ingestion_runtime.RESTART_BASE_S),
                }
            }

        latest_state = {
            "healthy_providers": 1,
            "price_age_ms": 0,
            "provider_errors": {},
            "providers": {"polygon": {"ok": True}},
        }

        try:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = 30.0
            with patch.object(ingestion_runtime.subprocess, "Popen", _FakeProc):
                with patch.object(ingestion_runtime, "_polling_price_max_age_ms", return_value=15000):
                    with patch.object(ingestion_runtime, "_terminate_child") as terminate_mock:
                        with patch.object(ingestion_runtime, "publish_message") as publish_mock:
                            children = _children()
                            ingestion_runtime._restart_children_for_feed_stall(children, latest_state, base_s + 20.0)
                            ingestion_runtime._restart_children_for_feed_stall(children, latest_state, base_s + 35.0)
        finally:
            ingestion_runtime.SUPERVISOR_SNAPSHOT_CACHE_TTL_S = old_ttl
            ingestion_runtime.invalidate_supervisor_snapshot_cache()

        terminate_mock.assert_called_once()
        publish_mock.assert_called_once()
        self.assertFalse(bool(children["poll_prices"]["running"]))
        self.assertEqual(int(children["poll_prices"]["pid"] or 0), 0)

    def test_child_restart_guard_accounting_survives_supervisor_module_reload(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 3
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                first = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s,
                    reason="exit",
                )
                second = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s + 1.0,
                    reason="exit",
                )

            self.assertEqual(int(first["count"]), 1)
            self.assertEqual(int(second["count"]), 2)
            self.assertFalse(bool(second["suppressed"]))

            storage, ingestion_runtime = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.ingestion_runtime",
            )
            ingestion_runtime.CHILD_MAX_RESTARTS = 3
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0

            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                third = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s + 2.0,
                    reason="exit",
                )

            self.assertEqual(int(third["count"]), 3)
            self.assertTrue(bool(third["suppressed"]))

            con = storage.connect_ro()
            try:
                prefix = ingestion_runtime._restart_guard_row_prefix("poll_prices")
                row = con.execute(
                    "SELECT COUNT(*) FROM job_locks WHERE job_name LIKE ?",
                    (prefix + "%",),
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(int((row or [0])[0] or 0), 3)
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

    def test_child_restart_guard_stale_window_expires(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 2
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 1.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                suppressed = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s,
                    reason="start_failed",
                )
                suppressed = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s + 0.1,
                    reason="start_failed",
                )

            self.assertEqual(int(suppressed["count"]), 2)
            self.assertTrue(bool(suppressed["suppressed"]))

            expired = ingestion_runtime._restart_guard_snapshot("poll_prices", now=base_s + 1.2)
            self.assertEqual(int(expired["count"]), 0)
            self.assertFalse(bool(expired["suppressed"]))

            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                after_expiry = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s + 1.2,
                    reason="start_failed",
                )
            self.assertEqual(int(after_expiry["count"]), 1)
            self.assertFalse(bool(after_expiry["suppressed"]))
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

    def test_child_restart_guard_survives_supervisor_restart_reconcile(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 3
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s, reason="exit")
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s + 1.0, reason="exit")

            storage, ingestion_runtime = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.ingestion_runtime",
            )
            ingestion_runtime.CHILD_MAX_RESTARTS = 3
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            children = {"poll_prices": ingestion_runtime._new_child_info("poll_prices")}

            with patch.object(
                ingestion_runtime,
                "_child_control_plane_snapshot",
                return_value={"desired": ["poll_prices"], "config_hashes": {"poll_prices": "hash-v1"}},
            ):
                with patch.object(ingestion_runtime, "_invalidate_supervisor_snapshots_if_data_sources_changed"):
                    desired = ingestion_runtime._reconcile_child_control_plane(children, base_s + 2.0)

            snapshot = ingestion_runtime._restart_guard_snapshot("poll_prices", now=base_s + 2.0)
            self.assertEqual(desired, ["poll_prices"])
            self.assertEqual(str(children["poll_prices"].get("config_hash") or ""), "hash-v1")
            self.assertEqual(int(snapshot["count"]), 2)
            self.assertFalse(bool(children["poll_prices"].get("restart_disabled")))
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

    def test_child_restart_guard_active_window_suppresses_spawn_after_restart(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 2
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s, reason="exit")
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s + 1.0, reason="exit")

            storage, ingestion_runtime = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.ingestion_runtime",
            )
            ingestion_runtime.CHILD_MAX_RESTARTS = 2
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            child = ingestion_runtime._new_child_info("poll_prices")

            with patch.object(ingestion_runtime, "emit_counter") as counter_mock:
                with patch.object(ingestion_runtime, "publish_message") as publish_mock:
                    with patch("engine.runtime.event_log.append_event") as append_mock:
                        suppressed = ingestion_runtime._suppress_inactive_child_if_restart_guard_active(
                            "poll_prices",
                            child,
                            base_s + 2.0,
                        )

            self.assertTrue(suppressed)
            self.assertTrue(bool(child.get("restart_disabled")))
            self.assertTrue(str(child.get("last_error") or "").startswith("restart_guard_triggered"))
            self.assertTrue(bool((child.get("restart_guard") or {}).get("suppressed")))
            self.assertTrue(
                any(
                    call_args.args[:2] == ("ingestion_child_restart_suppressed_total", 1)
                    for call_args in counter_mock.call_args_list
                )
            )
            append_mock.assert_called_once()
            publish_mock.assert_called_once()
            self.assertEqual(publish_mock.call_args.args[:2], ("market_data", "child_restart_guard_triggered"))
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

    def test_child_restart_guard_disabled_child_recovers_after_window_expiry(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 1
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 1.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                suppressed = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s,
                    reason="exit",
                )
            self.assertTrue(bool(suppressed["suppressed"]))

            child = ingestion_runtime._new_child_info("poll_prices")
            child["restart_disabled"] = True
            child["last_error"] = "restart_guard_triggered rc=1"
            child["next_spawn_ts"] = base_s + 600.0
            still_suppressed = ingestion_runtime._restart_guard_disabled_child_still_suppressed(
                "poll_prices",
                child,
                base_s + 1.2,
            )

            self.assertFalse(still_suppressed)
            self.assertFalse(bool(child.get("restart_disabled")))
            self.assertIsNone(child.get("last_error"))
            self.assertEqual(float(child.get("next_spawn_ts") or 0.0), base_s + 1.2)
            self.assertFalse(bool((child.get("restart_guard") or {}).get("suppressed")))
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

    def test_child_restart_guard_config_change_clears_disabled_window(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 2
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s, reason="exit")
                suppressed = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s + 1.0,
                    reason="exit",
                )
            self.assertTrue(bool(suppressed["suppressed"]))

            children = {"poll_prices": ingestion_runtime._new_child_info("poll_prices")}
            children["poll_prices"]["config_hash"] = "hash-old"
            children["poll_prices"]["restart_disabled"] = True
            children["poll_prices"]["restart_guard"] = dict(suppressed)
            children["poll_prices"]["next_spawn_ts"] = base_s + 600.0
            children["poll_prices"]["last_error"] = "restart_guard_triggered rc=1"

            with patch.object(
                ingestion_runtime,
                "_child_control_plane_snapshot",
                return_value={"desired": ["poll_prices"], "config_hashes": {"poll_prices": "hash-new"}},
            ):
                with patch.object(ingestion_runtime, "_invalidate_supervisor_snapshots_if_data_sources_changed"):
                    with patch.object(ingestion_runtime, "emit_counter"):
                        ingestion_runtime._reconcile_child_control_plane(children, base_s + 2.0)

            snapshot = ingestion_runtime._restart_guard_snapshot("poll_prices", now=base_s + 2.0)
            self.assertEqual(int(snapshot["count"]), 0)
            self.assertFalse(bool(children["poll_prices"].get("restart_disabled")))
            self.assertEqual(float(children["poll_prices"].get("next_spawn_ts") or 0.0), base_s + 2.0)
            self.assertEqual(str(children["poll_prices"].get("last_error") or ""), "config_changed_restart_requested")
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

    def test_child_restart_guard_config_reload_marker_clears_after_supervisor_restart(self) -> None:
        storage, ingestion_runtime, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        old_marker = ingestion_runtime._LAST_DATA_SOURCES_RELOAD_TS_MS
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 2
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s, reason="exit")
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s + 1.0, reason="exit")

            storage, ingestion_runtime, runtime_meta = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.ingestion_runtime",
                "engine.runtime.runtime_meta",
            )
            ingestion_runtime.CHILD_MAX_RESTARTS = 2
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            ingestion_runtime._LAST_DATA_SOURCES_RELOAD_TS_MS = None
            runtime_meta.meta_set("data_sources_reload_ts_ms", str(int((base_s + 10.0) * 1000.0)))
            children = {"poll_prices": ingestion_runtime._new_child_info("poll_prices")}
            children["poll_prices"]["restart_disabled"] = True
            children["poll_prices"]["last_error"] = "restart_guard_triggered rc=1"

            with patch.object(
                ingestion_runtime,
                "_child_control_plane_snapshot",
                return_value={"desired": ["poll_prices"], "config_hashes": {"poll_prices": "hash-after-restart"}},
            ):
                with patch.object(ingestion_runtime, "emit_counter"):
                    ingestion_runtime._reconcile_child_control_plane(children, base_s + 11.0)

            snapshot = ingestion_runtime._restart_guard_snapshot("poll_prices", now=base_s + 11.0)
            self.assertEqual(int(snapshot["count"]), 0)
            self.assertFalse(bool(children["poll_prices"].get("restart_disabled")))
            self.assertEqual(float(children["poll_prices"].get("next_spawn_ts") or 0.0), base_s + 11.0)
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window
            ingestion_runtime._LAST_DATA_SOURCES_RELOAD_TS_MS = old_marker

    def test_child_restart_guard_manual_clear_preserves_operator_override(self) -> None:
        storage, ingestion_runtime = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        base_s = 1_700_000_000.0

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 2
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            with patch.object(ingestion_runtime, "emit_counter"), patch.object(ingestion_runtime, "emit_gauge"):
                ingestion_runtime._record_child_restart_attempt("poll_prices", now=base_s, reason="exit")
                suppressed = ingestion_runtime._record_child_restart_attempt(
                    "poll_prices",
                    now=base_s + 1.0,
                    reason="exit",
                )
            self.assertTrue(bool(suppressed["suppressed"]))

            with patch.object(ingestion_runtime, "emit_counter"):
                cleared = ingestion_runtime.clear_child_restart_accounting(
                    ["poll_prices"],
                    reason="operator_restart_feeds",
                )
            snapshot = ingestion_runtime._restart_guard_snapshot("poll_prices", now=base_s + 2.0)

            self.assertTrue(bool(cleared["ok"]))
            self.assertGreaterEqual(int(cleared["deleted_rows"]), 2)
            self.assertEqual(int(snapshot["count"]), 0)
            self.assertFalse(bool(snapshot["suppressed"]))
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

    def test_child_restart_guard_suppression_emits_operator_metrics_and_event(self) -> None:
        (ingestion_runtime,) = _reload_modules("engine.runtime.ingestion_runtime")
        status = {
            "liveness_job": "poll_prices",
            "count": 3,
            "limit": 3,
            "window_s": 60.0,
            "suppressed_until_ts_ms": 1_700_000_060_000,
        }

        with patch.object(ingestion_runtime, "emit_counter") as counter_mock:
            with patch.object(ingestion_runtime, "publish_message") as publish_mock:
                with patch("engine.runtime.event_log.append_event") as append_mock:
                    ingestion_runtime._publish_child_restart_suppressed(
                        "poll_prices",
                        status,
                        reason="exit",
                        detail={"rc": 1, "age_s": 0.25},
                    )

        counter_mock.assert_called_once()
        self.assertEqual(counter_mock.call_args.args[:2], ("ingestion_child_restart_suppressed_total", 1))
        append_mock.assert_called_once()
        self.assertEqual(append_mock.call_args.kwargs["event_type"], "ingestion_child_restart_suppressed")
        publish_mock.assert_called_once()
        self.assertEqual(publish_mock.call_args.args[:2], ("market_data", "child_restart_guard_triggered"))

    def test_feed_stall_restart_guard_suppresses_with_persisted_accounting(self) -> None:
        storage, ingestion_runtime, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_runtime",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()
        old_max = int(ingestion_runtime.CHILD_MAX_RESTARTS)
        old_window = float(ingestion_runtime.CHILD_RESTART_WINDOW_S)
        now = 1_700_000_000.0
        heartbeat_extra = json.dumps(
            {
                "heartbeat_every_s": 2,
                "telemetry": {
                    "connected": True,
                    "last_msg_age_ms": 1000,
                    "manager_state": "healthy",
                    "capabilities": {"polling": False, "streaming": True},
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO job_heartbeats(job_name, owner, pid, ts_ms, extra_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("poll_prices", "test-owner", 4321, int((now - 60.0) * 1000.0), heartbeat_extra),
            )
            con.commit()
        finally:
            con.close()

        class _FakeProc:
            def __init__(self, pid: int):
                self.pid = int(pid)

            def poll(self):
                return None

        children = {
            "poll_prices": {
                "job": "poll_prices",
                "pid": 4321,
                "proc": _FakeProc(4321),
                "running": True,
                "restart_disabled": False,
                "last_start_ts": now - float(ingestion_runtime.CHILD_STARTUP_GRACE_S) - 30.0,
                "restart_delay_s": float(ingestion_runtime.RESTART_BASE_S),
            }
        }
        latest_state = {
            "healthy_providers": 1,
            "price_age_ms": 0,
            "provider_errors": {},
            "providers": {"polygon": {"ok": True}},
        }

        try:
            ingestion_runtime.CHILD_MAX_RESTARTS = 1
            ingestion_runtime.CHILD_RESTART_WINDOW_S = 300.0
            runtime_meta.meta_set("first_price_ts_ms", str(int((now - 10.0) * 1000.0)))

            with patch.object(ingestion_runtime.subprocess, "Popen", _FakeProc):
                with patch.object(ingestion_runtime, "_terminate_child") as terminate_mock:
                    with patch.object(ingestion_runtime, "emit_counter") as counter_mock:
                        with patch.object(ingestion_runtime, "emit_gauge"):
                            with patch.object(ingestion_runtime, "publish_message") as publish_mock:
                                with patch("engine.runtime.event_log.append_event") as append_mock:
                                    ingestion_runtime._restart_children_for_feed_stall(children, latest_state, now)

            terminate_mock.assert_called_once()
            self.assertFalse(bool(children["poll_prices"]["running"]))
            self.assertTrue(bool(children["poll_prices"]["restart_disabled"]))
            self.assertTrue(bool((children["poll_prices"].get("restart_guard") or {}).get("suppressed")))
            self.assertEqual(int((children["poll_prices"].get("restart_guard") or {}).get("count") or 0), 1)
            self.assertTrue(
                any(
                    call_args.args[:2] == ("ingestion_child_restart_suppressed_total", 1)
                    for call_args in counter_mock.call_args_list
                )
            )
            append_mock.assert_called_once()
            self.assertEqual(append_mock.call_args.kwargs["event_type"], "ingestion_child_restart_suppressed")
            self.assertTrue(
                any(
                    call_args.args[:2] == ("market_data", "child_restart_guard_triggered")
                    for call_args in publish_mock.call_args_list
                )
            )

            prefix = ingestion_runtime._restart_guard_row_prefix("poll_prices")
            con = storage.connect_ro()
            try:
                row = con.execute(
                    "SELECT COUNT(*) FROM job_locks WHERE job_name LIKE ?",
                    (prefix + "%",),
                ).fetchone()
            finally:
                con.close()
            self.assertEqual(int((row or [0])[0] or 0), 1)
        finally:
            ingestion_runtime.CHILD_MAX_RESTARTS = old_max
            ingestion_runtime.CHILD_RESTART_WINDOW_S = old_window

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
