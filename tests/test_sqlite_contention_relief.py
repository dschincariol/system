from __future__ import annotations

import importlib
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

VALID_DATA_SOURCE_MASTER_KEY = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="


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


def _path_stats(snapshot: dict[str, object], path_prefix: str) -> dict[str, object]:
    prefix = str(path_prefix)
    for row in list(snapshot.get("top_contention_paths") or []):
        path = str((row or {}).get("path") or "")
        if path.startswith(prefix):
            return dict(row or {})
    return {}


class SQLiteContentionReliefTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env = {}
        self._set_env("DB_PATH", str(Path(self.tmp.name) / "contention_relief.db"))
        self._set_env("TS_STORAGE_BACKEND", "sqlite")
        self._set_env("ENGINE_SUPERVISED", "1")
        self._set_env("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
        self._set_env("DATA_SOURCE_MASTER_KEY_FILE", None)

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        try:
            (_, metrics_store) = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.metrics_store",
            )
            metrics_store.shutdown_runtime_metrics_buffer(timeout_s=1.0)
        except Exception:
            pass
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.shutdown_job_liveness_queue(timeout_s=1.0)
        except Exception:
            pass
        try:
            (_, runtime_meta) = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.runtime_meta",
            )
            runtime_meta.shutdown_best_effort_runtime_meta_buffer(timeout_s=1.0)
        except Exception:
            pass
        try:
            (_, event_log) = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.event_log",
            )
            event_log.shutdown_event_log_buffer(timeout_s=1.0)
        except Exception:
            pass
        try:
            telemetry_append_buffer = importlib.import_module("engine.runtime.telemetry_append_buffer")
            telemetry_append_buffer.shutdown_telemetry_append_buffers(timeout_s=1.0)
        except Exception:
            pass
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _set_env(self, key: str, value: str | None) -> None:
        if key not in self.prev_env:
            self.prev_env[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)

    def test_live_provider_status_runtime_meta_writes_are_best_effort(self) -> None:
        provider_files = [
            REPO_ROOT / "engine" / "data" / "poll_prices.py",
            REPO_ROOT / "engine" / "data" / "providers" / "ibkr" / "daemon_stream.py",
            REPO_ROOT / "engine" / "jobs" / "stream_prices_polygon_ws.py",
        ]

        for path in provider_files:
            with self.subTest(path=str(path)):
                matches = [
                    line.strip()
                    for line in path.read_text(encoding="utf-8").splitlines()
                    if 'meta_set("price_provider_active"' in line
                ]
                self.assertTrue(matches, f"expected price_provider_active writes in {path}")
                for line in matches:
                    normalized = line.replace(" ", "")
                    self.assertIn(
                        'best_effort=True',
                        normalized,
                        f"price_provider_active should be best_effort in {path}: {line}",
                    )

    def test_runtime_metrics_buffer_flushes_batched_rows(self) -> None:
        self._set_env("RUNTIME_METRICS_BUFFER_ENABLED", "1")
        self._set_env("RUNTIME_METRICS_BUFFER_MAX_BATCH", "64")
        storage, metrics_store = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.metrics_store",
        )
        storage.init_db()

        metrics_store.write_runtime_metric("queue_depth", value_num=5, tags={"job": "unit"})
        metrics_store.write_runtime_metric("queue_depth", value_num=7, tags={"job": "unit"})
        snapshot = metrics_store.flush_runtime_metrics_buffer(max_batches=8)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM runtime_metrics WHERE metric='queue_depth'").fetchone()
        finally:
            con.close()

        self.assertEqual(int(snapshot.get("buffered_rows") or 0), 0)
        self.assertEqual(int(row[0] or 0), 2)

    def test_runtime_metrics_buffer_is_enabled_by_default(self) -> None:
        self._set_env("RUNTIME_METRICS_BUFFER_ENABLED", None)
        storage, metrics_store = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.metrics_store",
        )
        storage.init_db()

        row = metrics_store._runtime_metric_row("default_buffer_probe", value_num=1, tags={"job": "unit"})
        with patch.object(metrics_store, "_ensure_runtime_metrics_writer_started", return_value=None):
            enqueued = metrics_store._enqueue_runtime_metric_rows([row])

        buffered = metrics_store.get_runtime_metrics_buffer_snapshot()

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM runtime_metrics WHERE metric='default_buffer_probe'").fetchone()
        finally:
            con.close()

        self.assertTrue(bool(enqueued))
        self.assertTrue(bool(buffered.get("enabled")))
        self.assertGreaterEqual(int(buffered.get("buffered_rows") or 0), 1)
        self.assertEqual(int(row[0] or 0), 0)

        flushed = metrics_store.flush_runtime_metrics_buffer(max_batches=8)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM runtime_metrics WHERE metric='default_buffer_probe'").fetchone()
        finally:
            con.close()

        self.assertEqual(int(flushed.get("buffered_rows") or 0), 0)
        self.assertEqual(int(row[0] or 0), 1)

    def test_runtime_metrics_flush_interval_is_staggered_per_process(self) -> None:
        self._set_env("RUNTIME_METRICS_FLUSH_INTERVAL_S", "2.0")
        self._set_env("RUNTIME_METRICS_FLUSH_JITTER_RATIO", "0.5")
        with patch("os.getpid", return_value=16):
            storage, metrics_store = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.metrics_store",
            )
        storage.init_db()

        snapshot = metrics_store.get_runtime_metrics_buffer_snapshot()

        self.assertEqual(float(snapshot.get("flush_interval_base_s") or 0.0), 2.0)
        self.assertEqual(float(snapshot.get("flush_jitter_ratio") or 0.0), 0.5)
        self.assertEqual(float(snapshot.get("flush_interval_s") or 0.0), 3.0)

    def test_runtime_meta_best_effort_same_value_is_debounced(self) -> None:
        self._set_env("RUNTIME_META_BEST_EFFORT_MIN_INTERVAL_S", "60")
        storage, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()
        real_run_write_txn = runtime_meta.run_write_txn
        calls = []

        def _capture_run_write_txn(fn, *args, **kwargs):
            calls.append(dict(kwargs))
            return real_run_write_txn(fn, *args, **kwargs)

        with patch.object(runtime_meta, "run_write_txn", side_effect=_capture_run_write_txn):
            runtime_meta.meta_set("debounce_probe", "same", best_effort=True)
            runtime_meta.meta_set("debounce_probe", "same", best_effort=True)

        self.assertEqual(len(calls), 1)
        self.assertEqual(runtime_meta.meta_get("debounce_probe"), "same")

    def test_data_quality_runtime_meta_keys_bypass_long_lived_cache_and_buffer(self) -> None:
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", "1")
        storage, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()
        real_run_write_txn = runtime_meta.run_write_txn
        calls = []

        def _capture_run_write_txn(fn, *args, **kwargs):
            calls.append(dict(kwargs))
            return real_run_write_txn(fn, *args, **kwargs)

        key = "data_quality::feature_validation"
        payload = '{"ok":true,"validated_ts_ms":1700000000000}'
        with patch.object(runtime_meta, "_ensure_best_effort_writer_thread", side_effect=AssertionError("unexpected buffer")):
            with patch.object(runtime_meta, "run_write_txn", side_effect=_capture_run_write_txn):
                runtime_meta.meta_set(key, payload, best_effort=True)

        self.assertTrue(runtime_meta._is_volatile_key(key))
        self.assertFalse(runtime_meta._should_buffer_best_effort_key(key))
        self.assertEqual(int(runtime_meta.runtime_meta_best_effort_buffer_snapshot().get("buffered_keys") or 0), 0)
        self.assertEqual(len(calls), 1)
        self.assertEqual(runtime_meta.meta_get(key), payload)

    def test_startup_write_gate_releases_after_first_price_even_if_lifecycle_meta_lags(self) -> None:
        (startup_write_gate,) = _reload_modules(
            "engine.runtime.startup_write_gate",
        )

        class FakeConnection:
            def execute(self, _sql, _params=()):
                class Cursor:
                    def fetchall(self):
                        return [
                            ("warmup_started_ts_ms", "1700000000000"),
                            ("first_price_ts_ms", "1700000001234"),
                            ("lifecycle_state", "WARMING_UP"),
                        ]

                return Cursor()

            def close(self):
                return None

        with patch.object(startup_write_gate, "connect_ro", return_value=FakeConnection()):
            state = startup_write_gate.startup_noncritical_write_gate_state(force_refresh=True)

        self.assertFalse(bool(state.get("defer")))
        self.assertEqual(str(state.get("reason") or ""), "first_price_seen")

    def test_startup_write_gate_releases_once_runtime_is_live_after_first_price(self) -> None:
        storage, runtime_meta, startup_write_gate = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.startup_write_gate",
        )
        storage.init_db()

        runtime_meta.meta_set("warmup_started_ts_ms", "1700000000000")
        runtime_meta.meta_set("first_price_ts_ms", "1700000001234")
        runtime_meta.meta_set("lifecycle_state", "LIVE")

        state = startup_write_gate.startup_noncritical_write_gate_state(force_refresh=True)

        self.assertFalse(bool(state.get("defer")))
        self.assertEqual(str(state.get("reason") or ""), "first_price_seen")

    def test_runtime_meta_best_effort_buffer_coalesces_latest_value(self) -> None:
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", "1")
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S", "60")
        storage, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()
        real_run_write_txn = runtime_meta.run_write_txn
        calls = []

        def _capture_run_write_txn(fn, *args, **kwargs):
            calls.append(dict(kwargs))
            return real_run_write_txn(fn, *args, **kwargs)

        with patch.object(runtime_meta, "_ensure_best_effort_writer_thread", return_value=None):
            with patch.object(runtime_meta, "run_write_txn", side_effect=_capture_run_write_txn):
                runtime_meta.meta_set("dashboard_boot_diagnostics", '{"seq":1}', best_effort=True)
                runtime_meta.meta_set("dashboard_boot_diagnostics", '{"seq":2}', best_effort=True)

                buffered = runtime_meta.runtime_meta_best_effort_buffer_snapshot()
                self.assertEqual(int(buffered.get("buffered_keys") or 0), 1)
                self.assertEqual(runtime_meta.meta_get("dashboard_boot_diagnostics"), '{"seq":2}')

                con = storage.connect_ro_direct()
                try:
                    row = con.execute("SELECT COUNT(*) FROM runtime_meta WHERE key=?", ("dashboard_boot_diagnostics",)).fetchone()
                finally:
                    con.close()
                self.assertEqual(int(row[0] or 0), 0)

                flushed = runtime_meta.flush_best_effort_runtime_meta_buffer(max_batches=4)

        self.assertEqual(int(flushed.get("manual_flush_batches") or 0), 1)
        self.assertEqual(int(flushed.get("manual_flushed_keys") or 0), 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(int(calls[0].get("attempts") or 0), 1)
        self.assertTrue(bool(calls[0].get("direct")))
        self.assertTrue(calls[0].get("maintenance") is False)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT value FROM runtime_meta WHERE key=?", ("dashboard_boot_diagnostics",)).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), '{"seq":2}')

    def test_lifecycle_state_updates_use_buffered_runtime_meta_writes(self) -> None:
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", "1")
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S", "60")
        storage, runtime_meta, lifecycle_state = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.lifecycle_state",
        )
        storage.init_db()

        with patch.object(runtime_meta, "run_write_txn", side_effect=AssertionError("unexpected sync runtime_meta write")) as run_mock:
            with patch.object(runtime_meta, "_ensure_best_effort_writer_thread", return_value=None):
                with patch.object(lifecycle_state, "record_lifecycle_event", return_value=None):
                    with patch.object(lifecycle_state, "emit_counter", return_value=None):
                        with patch.object(lifecycle_state, "emit_gauge", return_value=None):
                            with patch.object(lifecycle_state, "trace_event", return_value=None):
                                lifecycle_state.set_state(
                                    lifecycle_state.WARMING_UP,
                                    "awaiting_first_price_tick",
                                )
                                lifecycle_state.mark_dashboard_bound("http://127.0.0.1:8000")

        self.assertEqual(run_mock.call_count, 0)
        self.assertEqual(runtime_meta.meta_get("lifecycle_state"), lifecycle_state.WARMING_UP)
        self.assertEqual(
            runtime_meta.meta_get("lifecycle_detail"),
            "awaiting_first_price_tick",
        )
        self.assertEqual(
            runtime_meta.meta_get("dashboard_bound_detail"),
            "http://127.0.0.1:8000",
        )

    def test_startup_diagnostics_keys_use_buffered_runtime_meta_writes(self) -> None:
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_ENABLED", "1")
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S", "60")
        storage, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()

        with patch.object(runtime_meta, "run_write_txn", side_effect=AssertionError("unexpected sync runtime_meta write")) as run_mock:
            with patch.object(runtime_meta, "_ensure_best_effort_writer_thread", return_value=None):
                runtime_meta.meta_set("startup_trace", '{"phase":"RUNNING"}', best_effort=True)
                runtime_meta.meta_set("startup_health_validation", '{"ok":false}', best_effort=True)
                runtime_meta.meta_set("dashboard_boot_diagnostics", '{"post_bind_boot":{"ok":true}}', best_effort=True)

        self.assertEqual(run_mock.call_count, 0)
        self.assertEqual(runtime_meta.meta_get("startup_trace"), '{"phase":"RUNNING"}')
        self.assertEqual(runtime_meta.meta_get("startup_health_validation"), '{"ok":false}')
        self.assertEqual(
            runtime_meta.meta_get("dashboard_boot_diagnostics"),
            '{"post_bind_boot":{"ok":true}}',
        )

    def test_runtime_meta_buffer_flush_interval_is_staggered_per_process(self) -> None:
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_INTERVAL_S", "2.0")
        self._set_env("RUNTIME_META_BEST_EFFORT_BUFFER_FLUSH_JITTER_RATIO", "0.5")
        with patch("os.getpid", return_value=16):
            storage, runtime_meta = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.runtime_meta",
            )
        storage.init_db()

        snapshot = runtime_meta.runtime_meta_best_effort_buffer_snapshot()

        self.assertEqual(float(snapshot.get("flush_interval_base_s") or 0.0), 2.0)
        self.assertEqual(float(snapshot.get("flush_jitter_ratio") or 0.0), 0.5)
        self.assertEqual(float(snapshot.get("flush_interval_s") or 0.0), 3.0)

    def test_event_log_buffer_batches_standalone_appends(self) -> None:
        self._set_env("EVENT_LOG_BUFFER_ENABLED", "1")
        self._set_env("EVENT_LOG_BUFFER_FLUSH_INTERVAL_S", "60")
        storage, event_log = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_log",
        )
        storage.init_db()
        real_run_write_txn = event_log.run_write_txn
        calls = []

        def _capture_run_write_txn(fn, *args, **kwargs):
            calls.append(dict(kwargs))
            return real_run_write_txn(fn, *args, **kwargs)

        with patch.object(event_log, "_ensure_event_log_writer_started", return_value=None):
            with patch.object(event_log, "run_write_txn", side_effect=_capture_run_write_txn):
                self.assertIsNone(
                    event_log.append_event(
                        event_type="buffer_probe",
                        event_source="tests.test_sqlite_contention_relief",
                        entity_type="probe",
                        entity_id="one",
                        payload={"seq": 1},
                    )
                )
                self.assertIsNone(
                    event_log.append_event(
                        event_type="buffer_probe",
                        event_source="tests.test_sqlite_contention_relief",
                        entity_type="probe",
                        entity_id="two",
                        payload={"seq": 2},
                    )
                )

                snapshot = event_log.get_event_log_buffer_snapshot()
                self.assertTrue(bool(snapshot.get("enabled")))
                self.assertEqual(int(snapshot.get("buffered_rows") or 0), 2)

                con = storage.connect_ro_direct()
                try:
                    row = con.execute("SELECT COUNT(*) FROM event_log WHERE event_type='buffer_probe'").fetchone()
                finally:
                    con.close()
                self.assertEqual(int(row[0] or 0), 0)

                flushed = event_log.flush_event_log_buffer(max_batches=4)

        self.assertEqual(int(flushed.get("flushed") or 0), 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(int(calls[0].get("attempts") or 0), 1)
        self.assertTrue(bool(calls[0].get("direct")))
        self.assertTrue(calls[0].get("maintenance") is False)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM event_log WHERE event_type='buffer_probe'").fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 2)

    def test_event_log_buffer_preserves_transactional_appends(self) -> None:
        self._set_env("EVENT_LOG_BUFFER_ENABLED", "1")
        storage, event_log = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_log",
        )
        storage.init_db()

        con = storage.connect()
        try:
            event_id = event_log.append_event(
                event_type="txn_probe",
                event_source="tests.test_sqlite_contention_relief",
                entity_type="probe",
                entity_id="txn",
                payload={"ok": True},
                con=con,
            )
            con.commit()
        finally:
            con.close()

        self.assertIsNotNone(event_id)
        self.assertGreater(int(event_id or 0), 0)
        snapshot = event_log.get_event_log_buffer_snapshot()
        self.assertEqual(int(snapshot.get("buffered_rows") or 0), 0)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM event_log WHERE event_type='txn_probe'").fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 1)

    def test_job_liveness_flush_interval_is_staggered_per_process(self) -> None:
        self._set_env("SQLITE_LIVENESS_FLUSH_INTERVAL_S", "2.0")
        self._set_env("SQLITE_LIVENESS_FLUSH_JITTER_RATIO", "0.5")
        with patch("os.getpid", return_value=16):
            (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        snapshot = storage._job_liveness_queue_snapshot()

        self.assertEqual(float(snapshot.get("flush_interval_base_s") or 0.0), 2.0)
        self.assertEqual(float(snapshot.get("flush_jitter_ratio") or 0.0), 0.5)
        self.assertEqual(float(snapshot.get("flush_interval_s") or 0.0), 3.0)

    def test_job_liveness_writer_backs_off_after_flush_failure(self) -> None:
        self._set_env("SQLITE_LIVENESS_FLUSH_INTERVAL_S", "2.0")
        self._set_env("SQLITE_LIVENESS_FLUSH_JITTER_RATIO", "0.0")
        (storage,) = _reload_modules("engine.runtime.storage")

        waits = []

        def _wait(timeout: float) -> bool:
            waits.append(float(timeout))
            return len(waits) >= 2

        with patch.object(storage._SQLITE_LIVENESS_STOP, "wait", side_effect=_wait):
            with patch.object(storage, "_drain_job_liveness_batch", return_value=[{"job_name": "poll_prices"}]):
                with patch.object(storage, "_flush_job_liveness_batch", side_effect=RuntimeError("locked")):
                    with patch.object(storage, "_requeue_job_liveness_batch", return_value=None) as requeue_mock:
                        storage._job_liveness_writer_loop()

        self.assertEqual(len(waits), 2)
        self.assertEqual(float(waits[0]), 2.0)
        self.assertEqual(float(waits[1]), 4.0)
        requeue_mock.assert_called_once()

    def test_ingestion_status_best_effort_health_rows_are_throttled(self) -> None:
        self._set_env("INGESTION_PIPELINE_HEALTH_MIN_INTERVAL_S", "60")
        storage, ingestion_status = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_status",
        )
        storage.init_db()

        ingestion_status.record_pipeline_status("poll_prices", ok=True, raw_rows=1, event_rows=0, best_effort=True)
        ingestion_status.record_pipeline_status("poll_prices", ok=True, raw_rows=2, event_rows=0, best_effort=True)

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM ingestion_pipeline_health WHERE pipeline=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(int(row[0] or 0), 1)

    def test_ingestion_status_best_effort_buffer_batches_pipeline_rows(self) -> None:
        self._set_env("INGESTION_PIPELINE_HEALTH_MIN_INTERVAL_S", "0")
        self._set_env("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
        self._set_env("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
        storage, ingestion_status, telemetry_append_buffer = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_status",
            "engine.runtime.telemetry_append_buffer",
        )
        storage.init_db()

        with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
            ingestion_status.record_pipeline_status("poll_prices", ok=True, raw_rows=1, event_rows=0, best_effort=True)
            ingestion_status.record_pipeline_status("poll_macro", ok=True, raw_rows=2, event_rows=0, best_effort=True)

        snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        self.assertEqual(
            int(((snapshot.get("pending_by_table") or {}).get("ingestion_pipeline_health") or 0)),
            2,
        )

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM ingestion_pipeline_health").fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 0)

        flushed = telemetry_append_buffer.flush_telemetry_append_buffers(
            max_batches=4,
            tables=("ingestion_pipeline_health",),
        )
        self.assertEqual(int(flushed.get("flushed") or 0), 2)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM ingestion_pipeline_health").fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 2)

    def test_job_liveness_queue_throttles_repeated_persisted_heartbeats(self) -> None:
        self._set_env("SQLITE_LIVENESS_QUEUE_ENABLED", "1")
        self._set_env("SQLITE_LIVENESS_FLUSH_INTERVAL_S", "60")
        self._set_env("SQLITE_LIVENESS_MIN_PERSIST_INTERVAL_S", "60")
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

        storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"phase":"a"}')
        first = storage.flush_job_liveness_queue(max_batches=4)
        self.assertEqual(int(first.get("flushed") or 0), 1)

        storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"phase":"b"}')
        throttled = storage.flush_job_liveness_queue(max_batches=4, force=False)
        self.assertEqual(int(throttled.get("flushed") or 0), 0)
        self.assertEqual(int((throttled.get("pending_count") or 0)), 1)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT extra_json FROM job_heartbeats WHERE job_name=?", ("poll_prices",)).fetchone()
        finally:
            con.close()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0] or ""), '{"phase":"a"}')

        forced = storage.flush_job_liveness_queue(max_batches=4, force=True)
        self.assertEqual(int(forced.get("flushed") or 0), 1)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT extra_json FROM job_heartbeats WHERE job_name=?", ("poll_prices",)).fetchone()
        finally:
            con.close()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0] or ""), '{"phase":"b"}')

    def test_job_liveness_can_use_separate_sqlite_db_with_transparent_reads(self) -> None:
        self._set_env("SQLITE_LIVENESS_DB_ENABLED", "1")
        self._set_env("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
        storage, locks = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.locks",
        )
        storage.init_db()

        self.assertTrue(locks.acquire_lock("poll_prices", ttl_ms=2_000))
        storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"phase":"isolated"}')

        liveness_path = Path(str(storage._SQLITE_LIVENESS_DB_PATH))
        self.assertTrue(liveness_path.exists())

        main_con = sqlite3.connect(os.environ["DB_PATH"])
        try:
            row = main_con.execute(
                "SELECT COUNT(*) FROM job_heartbeats WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            main_con.close()
        self.assertEqual(int((row or [0])[0] or 0), 0)

        liveness_con = sqlite3.connect(str(liveness_path))
        try:
            row = liveness_con.execute(
                "SELECT COUNT(*) FROM job_heartbeats WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            liveness_con.close()
        self.assertEqual(int((row or [0])[0] or 0), 1)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT ts_ms, extra_json FROM job_heartbeats WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertGreater(int(row[0] or 0), 0)
        self.assertEqual(str(row[1] or ""), '{"phase":"isolated"}')

    def test_job_liveness_separate_sqlite_db_is_enabled_by_default(self) -> None:
        self._set_env("SQLITE_LIVENESS_DB_ENABLED", None)
        self._set_env("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
        storage, locks = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.locks",
        )
        storage.init_db()

        self.assertTrue(bool(storage._SQLITE_LIVENESS_DB_ENABLED))

        self.assertTrue(locks.acquire_lock("poll_prices", ttl_ms=2_000))
        storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"phase":"default"}')

        liveness_path = Path(str(storage._SQLITE_LIVENESS_DB_PATH))
        self.assertTrue(liveness_path.exists())

        main_con = sqlite3.connect(os.environ["DB_PATH"])
        try:
            row = main_con.execute(
                "SELECT COUNT(*) FROM job_heartbeats WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            main_con.close()
        self.assertEqual(int((row or [0])[0] or 0), 0)

        liveness_con = sqlite3.connect(str(liveness_path))
        try:
            row = liveness_con.execute(
                "SELECT COUNT(*) FROM job_heartbeats WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            liveness_con.close()
        self.assertEqual(int((row or [0])[0] or 0), 1)

    def test_job_liveness_repairs_legacy_duplicate_heartbeats_before_upsert(self) -> None:
        self._set_env("SQLITE_LIVENESS_DB_ENABLED", "1")
        self._set_env("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
        legacy_liveness_path = Path(self.tmp.name) / "legacy_liveness.sqlite"
        self._set_env("SQLITE_LIVENESS_DB_PATH", str(legacy_liveness_path))

        legacy_con = sqlite3.connect(str(legacy_liveness_path))
        try:
            legacy_con.execute(
                """
                CREATE TABLE job_heartbeats (
                  job_name TEXT,
                  owner TEXT,
                  pid INTEGER,
                  ts_ms INTEGER,
                  extra_json TEXT
                )
                """
            )
            legacy_con.executemany(
                """
                INSERT INTO job_heartbeats(job_name, owner, pid, ts_ms, extra_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    ("poll_prices", "old-owner", 111, 10, '{"phase":"old"}'),
                    ("poll_prices", "newer-owner", 222, 20, '{"phase":"newer"}'),
                ],
            )
            legacy_con.commit()
        finally:
            legacy_con.close()

        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT owner, pid, ts_ms, extra_json FROM job_heartbeats WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), "newer-owner")
        self.assertEqual(int(row[1] or 0), 222)
        self.assertEqual(int(row[2] or 0), 20)
        self.assertEqual(str(row[3] or ""), '{"phase":"newer"}')

        storage.put_job_heartbeat("poll_prices", "upsert-owner", 333, '{"phase":"upserted"}')

        liveness_con = sqlite3.connect(str(legacy_liveness_path))
        try:
            rows = liveness_con.execute(
                """
                SELECT owner, pid, extra_json
                FROM job_heartbeats
                WHERE job_name=?
                """,
                ("poll_prices",),
            ).fetchall()
            pk_info = {
                str(row[1]): int(row[5] or 0)
                for row in liveness_con.execute("PRAGMA table_info(job_heartbeats)").fetchall()
            }
        finally:
            liveness_con.close()

        self.assertEqual(pk_info.get("job_name"), 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0][0]), "upsert-owner")
        self.assertEqual(int(rows[0][1] or 0), 333)
        self.assertEqual(str(rows[0][2] or ""), '{"phase":"upserted"}')

    def test_ipc_best_effort_channel_state_is_debounced(self) -> None:
        self._set_env("IPC_CHANNEL_STATE_BEST_EFFORT_MIN_INTERVAL_S", "60")
        storage, ipc = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ipc",
        )
        storage.init_db()

        first = ipc.publish_channel_state("market_data", {"ok": True}, owner="unit", best_effort=True)
        second = ipc.publish_channel_state("market_data", {"ok": True}, owner="unit", best_effort=True)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM ipc_channels WHERE channel=?", ("market_data",)).fetchone()
        finally:
            con.close()

        self.assertTrue(bool(first.get("ok")))
        self.assertTrue(bool(second.get("ok")))
        self.assertTrue(bool(second.get("skipped")))
        self.assertEqual(int(row[0] or 0), 1)

    def test_feature_store_can_skip_sqlite_writes_during_cutover(self) -> None:
        self._set_env("FEATURE_STORE_SQLITE_WRITE_ENABLED", "0")
        storage, price_cache, feature_store = _reload_modules(
            "engine.runtime.storage",
            "engine.data.price_cache",
            "engine.data.feature_store",
        )
        storage.init_db()
        price_cache.record_price_rows(
            [
                {"symbol": "SPY", "ts_ms": 1_700_000_000_000, "price": 500.25, "volume": 1000, "source": "unit"},
                {"symbol": "SPY", "ts_ms": 1_700_000_060_000, "price": 500.5, "volume": 1200, "source": "unit"},
            ]
        )

        with patch.object(
            feature_store,
            "run_write_txn",
            side_effect=AssertionError("feature_store_should_not_write_sqlite"),
        ):
            refreshed = feature_store.refresh_symbols(["SPY"], price_cache=price_cache)

        self.assertIn("SPY", refreshed)
        self.assertGreater(int((refreshed["SPY"] or {}).get("ts_ms") or 0), 0)
        live_snapshot = feature_store.get_live_features("SPY", price_cache=price_cache, persist=False)
        self.assertEqual(str(live_snapshot.get("symbol") or ""), "SPY")
        self.assertGreater(int(live_snapshot.get("ts_ms") or 0), 0)

        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM market_features WHERE symbol=?", ("SPY",)).fetchone()
        finally:
            con.close()

        self.assertEqual(int(row[0] or 0), 0)

    def test_provider_health_buffer_batches_rows_and_status_updates_are_debounced(self) -> None:
        self._set_env("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
        self._set_env("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
        self._set_env("DATA_SOURCE_STATUS_BEST_EFFORT_MIN_INTERVAL_S", "60")
        storage, options_poll, telemetry_append_buffer, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.data.options_poll",
            "engine.runtime.telemetry_append_buffer",
            "services.data_source_manager",
        )
        storage.init_db()
        data_source_manager.get_manager().initialize()

        with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
            with patch.object(
                telemetry_append_buffer,
                "_read_price_provider_state_from_db",
                return_value={"last_success_ts_ms": 0, "error_count": 0},
            ):
                fake_times = iter([1.0, 1.002])
                with patch.object(options_poll, "time", SimpleNamespace(time=lambda: next(fake_times))):
                    options_poll._put_provider_health("tradier", ok=True, n_symbols=5)
                    options_poll._put_provider_health("tradier", ok=True, n_symbols=5)

        snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        self.assertEqual(
            int(((snapshot.get("pending_by_table") or {}).get("price_provider_health") or 0)),
            2,
        )

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM data_source_logs WHERE source_key=? AND event_type='status'",
                ("tradier",),
            ).fetchone()
            pending_row = con.execute(
                "SELECT COUNT(*) FROM price_provider_health WHERE provider=?",
                ("tradier",),
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 1)
        self.assertEqual(int(pending_row[0] or 0), 0)

        flushed = telemetry_append_buffer.flush_telemetry_append_buffers(
            max_batches=4,
            tables=("price_provider_health",),
        )
        self.assertEqual(int(flushed.get("flushed") or 0), 2)

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM price_provider_health WHERE provider=?",
                ("tradier",),
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 2)

    def test_price_provider_health_append_falls_back_to_immediate_write_when_buffer_is_unavailable(self) -> None:
        self._set_env("TELEMETRY_APPEND_BUFFER_ENABLED", "0")
        storage, telemetry_append_buffer = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.telemetry_append_buffer",
        )
        storage.init_db()

        with patch.object(
            telemetry_append_buffer,
            "_read_price_provider_state_from_db",
            return_value={"last_success_ts_ms": 0, "error_count": 0},
        ):
            written = telemetry_append_buffer.append_price_provider_health(
                provider="polygon_ws",
                ok=False,
                latency_ms=42,
                n_symbols=7,
                error="downstream timeout",
                ts_ms=1_700_000_000_123,
            )

        self.assertTrue(bool(written))
        snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        self.assertEqual(
            int(((snapshot.get("pending_by_table") or {}).get("price_provider_health") or 0)),
            0,
        )

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                """
                SELECT ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count
                FROM price_provider_health
                WHERE provider=?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                ("polygon_ws",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(int(row[0] or 0), 0)
        self.assertEqual(int(row[1] or 0), 42)
        self.assertEqual(int(row[2] or 0), 7)
        self.assertEqual(str(row[3] or ""), "downstream timeout")
        self.assertEqual(int(row[4] or 0), 0)
        self.assertEqual(int(row[5] or 0), 1)

    def test_pipeline_health_append_falls_back_to_immediate_write_when_buffer_is_unavailable(self) -> None:
        self._set_env("TELEMETRY_APPEND_BUFFER_ENABLED", "0")
        self._set_env("INGESTION_PIPELINE_HEALTH_MIN_INTERVAL_S", "0")
        storage, ingestion_status, telemetry_append_buffer = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_status",
            "engine.runtime.telemetry_append_buffer",
        )
        storage.init_db()

        status = ingestion_status.record_pipeline_status(
            "poll_prices",
            ok=True,
            raw_rows=3,
            event_rows=2,
            last_ingested_ts_ms=1_700_000_001_000,
            meta={"provider": "polygon"},
            latency_ms=15,
            best_effort=True,
        )

        self.assertEqual(str(status.get("pipeline") or ""), "poll_prices")
        snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        self.assertEqual(
            int(((snapshot.get("pending_by_table") or {}).get("ingestion_pipeline_health") or 0)),
            0,
        )

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                """
                SELECT ok, latency_ms, raw_rows, event_rows, last_ingested_ts_ms, error
                FROM ingestion_pipeline_health
                WHERE pipeline=?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(int(row[0] or 0), 1)
        self.assertEqual(int(row[1] or 0), 15)
        self.assertEqual(int(row[2] or 0), 3)
        self.assertEqual(int(row[3] or 0), 2)
        self.assertEqual(int(row[4] or 0), 1_700_000_001_000)
        self.assertEqual(row[5], None)

    def test_live_stream_hot_path_failing_before_sync_health_writes_fixed_after_buffer_flush_defers_provider_and_pipeline_appends(self) -> None:
        self._set_env("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
        self._set_env("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
        self._set_env("INGESTION_PIPELINE_HEALTH_MIN_INTERVAL_S", "0")
        self._set_env("SQLITE_TRACE_REPORT_EVERY_S", "0")
        self._set_env("FEATURE_STORE_SQLITE_WRITE_ENABLED", "0")
        storage, runtime_meta, ingestion_status, poll_prices, telemetry_append_buffer, price_router, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
            "engine.runtime.ingestion_status",
            "engine.data.poll_prices",
            "engine.runtime.telemetry_append_buffer",
            "engine.runtime.price_router",
            "services.data_source_manager",
        )
        storage.init_db()
        manager = data_source_manager.get_manager()
        manager.initialize()
        _reset_sqlite_trace(storage)

        now_ms = int(time.time() * 1000)
        with patch.object(price_router, "publish_event"):
            with patch.object(price_router, "emit_counter"):
                with patch.object(price_router, "record_component_health"):
                    with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
                        with patch.object(
                            telemetry_append_buffer,
                            "_read_price_provider_state_from_db",
                            return_value={"last_success_ts_ms": 0, "error_count": 0},
                        ):
                            price_router.publish_price_events(
                                [
                                    {
                                        "symbol": "SPY",
                                        "provider": "polygon_ws",
                                        "source": "polygon_ws",
                                        "timestamp": now_ms,
                                        "last": 500.25,
                                        "bid": 500.2,
                                        "ask": 500.3,
                                        "volume": 1000,
                                    }
                                ],
                                write_prices=True,
                                write_quotes=True,
                                write_raw=True,
                                emit_telemetry=False,
                                update_symbols=False,
                            )
                            poll_prices._record_poll_prices_status(
                                manager,
                                ok=True,
                                raw_rows=1,
                                price_rows=1,
                                quote_rows=1,
                                last_ingested_ts_ms=now_ms,
                                providers=["polygon_ws"],
                                latency_ms=15,
                                message="stream ok",
                            )
                            telemetry_append_buffer.enqueue_price_provider_health(
                                provider="polygon_ws",
                                ok=True,
                                latency_ms=12,
                                n_symbols=1,
                                ts_ms=now_ms,
                            )

        buffer_snapshot_before = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        timeseries_snapshot_before = storage.get_timeseries_storage_snapshot()
        before_flush = dict((storage.get_connection_debug_snapshot().get("sqlite_trace") or {}))
        before_price_table = _table_stats(before_flush, "price_provider_health")
        before_pipeline_table = _table_stats(before_flush, "ingestion_pipeline_health")
        before_raw_table = _table_stats(before_flush, "price_quotes_raw")
        price_path = _path_stats(before_flush, "price_router.py:publish_price_events")
        status_path = _path_stats(before_flush, "data_source_manager.py:record_source_status")
        meta_path = _path_stats(before_flush, "runtime_meta.py:_run_meta_write")
        buffer_path_before = _path_stats(before_flush, "telemetry_append_buffer.py:_flush_rows")

        self.assertGreater(int((_table_stats(before_flush, "prices").get("writes") or 0)), 0)
        self.assertGreater(int((_table_stats(before_flush, "price_quotes").get("writes") or 0)), 0)
        self.assertEqual(
            int((before_raw_table.get("writes") or 0)),
            0,
            "raw quote evidence should stay off the immediate live-stream write path until the deferred buffer flush",
        )
        self.assertGreater(int((_table_stats(before_flush, "runtime_meta").get("writes") or 0)), 0)
        self.assertGreater(
            int((price_path.get("writes") or 0)),
            0,
            "failing_before_live_stream_relief would have hidden the immediate prices/quotes write path",
        )
        self.assertGreater(
            int((status_path.get("writes") or 0)),
            0,
            "failing_before_status_write_relief would have hidden the synchronous source-status writes on the poll_prices path",
        )
        self.assertGreater(
            int((meta_path.get("writes") or 0)),
            0,
            "failing_before_runtime_meta_relief would have hidden the best-effort runtime_meta write pressure on poll_prices status updates",
        )
        self.assertEqual(
            int((before_price_table.get("writes") or 0)),
            0,
            "failing_before_buffered_provider_health would have written price_provider_health on the immediate live-stream path before explicit flush",
        )
        self.assertEqual(
            int((before_pipeline_table.get("writes") or 0)),
            0,
            "failing_before_buffered_pipeline_health would have written ingestion_pipeline_health on the immediate poll_prices status path before explicit flush",
        )
        self.assertEqual(
            int((buffer_path_before.get("writes") or 0)),
            0,
            "failing_before_buffer_flush should leave telemetry buffer flush writes absent until an explicit flush happens",
        )
        self.assertEqual(
            int(((buffer_snapshot_before.get("accepted_by_table") or {}).get("price_quotes_raw") or 0)),
            1,
        )
        self.assertEqual(
            int(((buffer_snapshot_before.get("accepted_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )
        self.assertEqual(
            int(((buffer_snapshot_before.get("accepted_by_table") or {}).get("ingestion_pipeline_health") or 0)),
            1,
        )
        self.assertEqual(
            int(((buffer_snapshot_before.get("pending_by_table") or {}).get("price_quotes_raw") or 0)),
            1,
        )
        self.assertEqual(
            int(((buffer_snapshot_before.get("pending_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )
        self.assertEqual(
            int(((buffer_snapshot_before.get("pending_by_table") or {}).get("ingestion_pipeline_health") or 0)),
            1,
        )
        self.assertEqual(
            int(((buffer_snapshot_before.get("flushed_by_table") or {}).get("price_provider_health") or 0)),
            0,
        )
        self.assertEqual(
            int((((timeseries_snapshot_before.get("telemetry_append_buffer") or {}).get("pending_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )

        con = storage.connect_ro_direct()
        try:
            provider_row = con.execute(
                "SELECT COUNT(*) FROM price_provider_health WHERE provider=?",
                ("polygon_ws",),
            ).fetchone()
            pipeline_row = con.execute(
                "SELECT COUNT(*) FROM ingestion_pipeline_health WHERE pipeline=?",
                ("poll_prices",),
            ).fetchone()
            raw_row = con.execute(
                "SELECT COUNT(*) FROM price_quotes_raw WHERE symbol=? AND provider=?",
                ("SPY", "polygon_ws"),
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(int(provider_row[0] or 0), 0)
        self.assertEqual(int(pipeline_row[0] or 0), 0)
        self.assertEqual(int(raw_row[0] or 0), 0)

        flushed = telemetry_append_buffer.flush_telemetry_append_buffers(
            max_batches=8,
            tables=("price_quotes_raw", "ingestion_pipeline_health", "price_provider_health"),
        )
        self.assertGreaterEqual(int(flushed.get("flushed") or 0), 3)
        self.assertEqual(
            int(((flushed.get("pending_by_table") or {}).get("price_quotes_raw") or 0)),
            0,
        )
        self.assertEqual(
            int(((flushed.get("pending_by_table") or {}).get("price_provider_health") or 0)),
            0,
        )
        self.assertEqual(
            int(((flushed.get("pending_by_table") or {}).get("ingestion_pipeline_health") or 0)),
            0,
        )
        self.assertEqual(
            int(((flushed.get("flushed_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )
        self.assertEqual(
            int(((flushed.get("flushed_by_table") or {}).get("ingestion_pipeline_health") or 0)),
            1,
        )

        after_flush = dict((storage.get_connection_debug_snapshot().get("sqlite_trace") or {}))
        after_price_table = _table_stats(after_flush, "price_provider_health")
        after_pipeline_table = _table_stats(after_flush, "ingestion_pipeline_health")
        after_raw_table = _table_stats(after_flush, "price_quotes_raw")

        self.assertGreater(
            int((after_raw_table.get("writes") or 0)),
            0,
            "fixed_after_explicit_flush should persist the buffered price_quotes_raw rows",
        )
        self.assertGreater(
            int((after_price_table.get("writes") or 0)),
            0,
            "fixed_after_explicit_flush should persist the buffered price_provider_health rows",
        )
        self.assertGreater(
            int((after_pipeline_table.get("writes") or 0)),
            0,
            "fixed_after_explicit_flush should persist the buffered ingestion_pipeline_health rows",
        )
        self.assertGreaterEqual(
            int(flushed.get("manual_flush_batches") or 0),
            1,
            "fixed_after_explicit_flush should flush through the explicit telemetry buffer flush path",
        )

        con = storage.connect_ro_direct()
        try:
            provider_row = con.execute(
                "SELECT COUNT(*) FROM price_provider_health WHERE provider=?",
                ("polygon_ws",),
            ).fetchone()
            pipeline_row = con.execute(
                "SELECT COUNT(*) FROM ingestion_pipeline_health WHERE pipeline=?",
                ("poll_prices",),
            ).fetchone()
            raw_row = con.execute(
                "SELECT COUNT(*) FROM price_quotes_raw WHERE symbol=? AND provider=?",
                ("SPY", "polygon_ws"),
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(int(provider_row[0] or 0), 1)
        self.assertEqual(int(pipeline_row[0] or 0), 1)
        self.assertEqual(int(raw_row[0] or 0), 1)

    def test_provider_health_buffer_backpressure_reports_rejection_counters(self) -> None:
        self._set_env("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
        self._set_env("TELEMETRY_APPEND_BUFFER_MAX_BATCH", "1")
        self._set_env("TELEMETRY_APPEND_BUFFER_MAX_ROWS", "1")
        self._set_env("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
        storage, telemetry_append_buffer = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.telemetry_append_buffer",
        )
        storage.init_db()

        with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
            with patch.object(
                telemetry_append_buffer,
                "_read_price_provider_state_from_db",
                return_value={"last_success_ts_ms": 0, "error_count": 0},
            ):
                self.assertTrue(
                    telemetry_append_buffer.enqueue_price_provider_health(
                        provider="polygon_ws",
                        ok=True,
                        latency_ms=10,
                        n_symbols=1,
                        ts_ms=1_700_000_000_000,
                    )
                )
                self.assertFalse(
                    telemetry_append_buffer.enqueue_price_provider_health(
                        provider="polygon_ws",
                        ok=False,
                        latency_ms=11,
                        n_symbols=1,
                        error="buffer full",
                        ts_ms=1_700_000_000_001,
                    )
                )

        snapshot = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        self.assertEqual(str(snapshot.get("last_rejected_table") or ""), "price_provider_health")
        self.assertEqual(str(snapshot.get("last_rejected_reason") or ""), "buffer_overflow")
        self.assertEqual(int(snapshot.get("buffered_rows") or 0), 1)
        self.assertEqual(
            int(((snapshot.get("accepted_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )
        self.assertEqual(
            int(((snapshot.get("dropped_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )

    def test_data_source_manager_best_effort_status_writes_use_short_sqlite_budget(self) -> None:
        storage, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "services.data_source_manager",
        )
        storage.init_db()
        manager = data_source_manager.get_manager()
        manager.initialize()
        calls = []

        def _capture_run_write_txn(fn, *args, **kwargs):
            calls.append(dict(kwargs))
            return None

        with patch.object(data_source_manager, "run_write_txn", side_effect=_capture_run_write_txn):
            manager.record_source_status("polygon_ws", ok=True, message="ok", best_effort=True)

        self.assertEqual(len(calls), 1)
        self.assertEqual(int(calls[0].get("attempts") or 0), 1)
        self.assertTrue(bool(calls[0].get("direct")))
        self.assertTrue(calls[0].get("maintenance") is False)
        self.assertEqual(float(calls[0].get("timeout_s") or 0.0), 0.25)
        self.assertEqual(int(calls[0].get("busy_timeout_ms") or 0), 250)

    def test_data_source_manager_best_effort_status_writes_defer_during_startup_gate(self) -> None:
        storage, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "services.data_source_manager",
        )
        storage.init_db()
        manager = data_source_manager.get_manager()
        manager.initialize()

        with patch.object(data_source_manager, "should_defer_noncritical_startup_write", return_value=True):
            with patch.object(data_source_manager, "run_write_txn", side_effect=AssertionError("unexpected sqlite write")):
                manager.record_source_status("polygon_ws", ok=True, message="ok", best_effort=True)

    def test_data_source_manager_log_event_uses_short_direct_write_budget(self) -> None:
        storage, data_source_log_store, data_source_manager = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.data_source_log_store",
            "services.data_source_manager",
        )
        storage.init_db()
        manager = data_source_manager.get_manager()
        manager.initialize()
        calls = []

        def _capture_run_write_txn(fn, *args, **kwargs):
            calls.append(dict(kwargs))
            return None

        with patch.object(data_source_log_store, "run_write_txn", side_effect=_capture_run_write_txn):
            manager.log_event("polygon_ws", event_type="probe", message="probe")

        self.assertEqual(len(calls), 1)
        self.assertEqual(int(calls[0].get("attempts") or 0), 1)
        self.assertEqual(str(calls[0].get("table") or ""), "data_source_logs")
        self.assertEqual(str(calls[0].get("operation") or ""), "log_event")
        self.assertTrue(bool(calls[0].get("direct")))
        self.assertTrue(calls[0].get("maintenance") is False)
        self.assertEqual(float(calls[0].get("timeout_s") or 0.0), 0.25)
        self.assertEqual(int(calls[0].get("busy_timeout_ms") or 0), 250)

    def test_price_router_can_cut_over_writes_to_async_sidecar(self) -> None:
        self._set_env("PRICE_ROUTER_SQLITE_WRITE_ENABLED", "0")
        self._set_env("ASYNC_PRICE_WRITER_ENABLED", "1")
        self._set_env("FEATURE_STORE_SQLITE_WRITE_ENABLED", "0")
        (storage, _, feature_store, price_router) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.async_writer",
            "engine.data.feature_store",
            "engine.runtime.price_router",
        )
        storage.init_db()
        captured = []

        with patch.object(feature_store, "run_write_txn", side_effect=AssertionError("feature_store_should_not_write_sqlite")):
            with patch.object(price_router, "enqueue_price_persistence", side_effect=lambda **kwargs: captured.append(kwargs) or True):
                with patch.object(price_router, "enqueue_price_quotes_raw_rows", return_value=True):
                    with patch.object(price_router, "run_write_txn") as run_write_txn_mock:
                        with patch.object(price_router, "publish_event"):
                            with patch.object(price_router, "emit_counter"):
                                with patch.object(price_router, "record_component_health"):
                                    counts = price_router.publish_price_events(
                                        [
                                            {
                                                "symbol": "SPY",
                                                "provider": "polygon",
                                                "source": "polygon",
                                                "timestamp": 1_700_000_000_000,
                                                "last": 500.25,
                                                "bid": 500.2,
                                                "ask": 500.3,
                                                "volume": 1000,
                                            }
                                        ],
                                        write_prices=True,
                                        write_quotes=True,
                                        write_raw=True,
                                    )

        run_write_txn_mock.assert_not_called()
        self.assertEqual(int(counts.get("prices") or 0), 1)
        self.assertEqual(int(counts.get("quotes") or 0), 1)
        self.assertEqual(int(counts.get("raw") or 0), 1)
        self.assertEqual(len(captured), 1)
        con = storage.connect_ro_direct()
        try:
            row = con.execute("SELECT COUNT(*) FROM market_features WHERE symbol=?", ("SPY",)).fetchone()
        finally:
            con.close()
        self.assertEqual(int(row[0] or 0), 0)

    def test_price_router_surfaces_async_enqueue_rejection_for_producer_backoff(self) -> None:
        self._set_env("PRICE_ROUTER_SQLITE_WRITE_ENABLED", "0")
        self._set_env("ASYNC_PRICE_WRITER_ENABLED", "1")
        self._set_env("FEATURE_STORE_SQLITE_WRITE_ENABLED", "0")
        (storage, _, feature_store, price_router) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.async_writer",
            "engine.data.feature_store",
            "engine.runtime.price_router",
        )
        storage.init_db()
        emitted_counters = []
        health_updates = []
        fake_writer = SimpleNamespace(
            enabled=True,
            get_snapshot=lambda: {
                "backpressure_active": True,
                "last_backpressure_reason": "queue_full",
                "queue_depth": 8,
                "queue_maxsize": 8,
            }
        )

        with patch.object(feature_store, "run_write_txn", side_effect=AssertionError("feature_store_should_not_write_sqlite")):
            with patch.object(price_router, "enqueue_price_persistence", return_value=False):
                with patch.object(price_router, "get_async_writer", return_value=fake_writer):
                    with patch.object(price_router, "publish_event"):
                        with patch.object(
                            price_router,
                            "emit_counter",
                            side_effect=lambda *args, **kwargs: emitted_counters.append((args, kwargs)),
                        ):
                            with patch.object(
                                price_router,
                                "record_component_health",
                                side_effect=lambda *args, **kwargs: health_updates.append((args, kwargs)),
                            ):
                                counts = price_router.publish_price_events(
                                    [
                                        {
                                            "symbol": "SPY",
                                            "provider": "polygon",
                                            "source": "polygon",
                                            "timestamp": 1_700_000_000_000,
                                            "last": 500.25,
                                            "bid": 500.2,
                                            "ask": 500.3,
                                            "volume": 1000,
                                        }
                                    ],
                                    write_prices=True,
                                    write_quotes=True,
                                    write_raw=False,
                                )

        async_status = price_router.price_persistence_backpressure_status(counts)
        self.assertFalse(bool(async_status.get("accepted")))
        self.assertTrue(bool(async_status.get("backpressure")))
        self.assertEqual(str(async_status.get("reason") or ""), "enqueue_rejected")
        self.assertTrue(
            any(
                args and args[0] == "price_router_async_persistence_backpressure_rows"
                for args, _kwargs in emitted_counters
            )
        )
        self.assertTrue(
            any(args and args[0] == "price_router_async_persistence" for args, _kwargs in health_updates)
        )

    def test_ipc_best_effort_message_drops_before_sqlite_when_startup_gate_is_active(self) -> None:
        storage, ipc = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ipc",
        )
        storage.init_db()

        with patch.object(ipc, "should_defer_noncritical_startup_write", return_value=True):
            with patch.object(ipc, "run_write_txn", side_effect=AssertionError("unexpected sqlite write")):
                out = ipc.publish_message(
                    "runtime.boundary.startup_gate",
                    "state",
                    {"ok": True},
                    sender="test",
                    best_effort=True,
                )

        self.assertFalse(bool(out.get("ok")))
        self.assertTrue(bool(out.get("dropped")))
        self.assertEqual(str(out.get("detail") or ""), "sqlite_busy_best_effort_drop")

    def test_best_effort_writer_backoff_grows_under_repeated_lock_failures(self) -> None:
        runtime_meta, metrics_store, event_log, telemetry_append_buffer = _reload_modules(
            "engine.runtime.runtime_meta",
            "engine.runtime.metrics_store",
            "engine.runtime.event_log",
            "engine.runtime.telemetry_append_buffer",
        )

        self.assertGreaterEqual(float(runtime_meta._best_effort_flush_backoff_s(1)), 1.0)
        self.assertGreater(
            float(runtime_meta._best_effort_flush_backoff_s(2)),
            float(runtime_meta._best_effort_flush_backoff_s(1)),
        )
        self.assertLessEqual(float(runtime_meta._best_effort_flush_backoff_s(5)), 10.0)

        self.assertGreaterEqual(float(metrics_store._runtime_metrics_flush_backoff_s(1)), 1.0)
        self.assertGreater(
            float(metrics_store._runtime_metrics_flush_backoff_s(2)),
            float(metrics_store._runtime_metrics_flush_backoff_s(1)),
        )
        self.assertLessEqual(float(metrics_store._runtime_metrics_flush_backoff_s(5)), 10.0)

        self.assertGreaterEqual(float(event_log._event_log_flush_backoff_s(1)), 1.0)
        self.assertGreater(
            float(event_log._event_log_flush_backoff_s(2)),
            float(event_log._event_log_flush_backoff_s(1)),
        )
        self.assertLessEqual(float(event_log._event_log_flush_backoff_s(5)), 10.0)

        self.assertGreaterEqual(float(telemetry_append_buffer._telemetry_append_flush_backoff_s(1)), 1.0)
        self.assertGreater(
            float(telemetry_append_buffer._telemetry_append_flush_backoff_s(2)),
            float(telemetry_append_buffer._telemetry_append_flush_backoff_s(1)),
        )
        self.assertLessEqual(float(telemetry_append_buffer._telemetry_append_flush_backoff_s(5)), 10.0)

    def test_runtime_meta_best_effort_writer_waits_while_startup_gate_is_active(self) -> None:
        (_, runtime_meta) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        runtime_meta._BEST_EFFORT_BUFFER_PENDING.clear()
        runtime_meta._BEST_EFFORT_BUFFER_INFLIGHT.clear()
        runtime_meta._BEST_EFFORT_BUFFER_PENDING["lifecycle_state"] = {
            "value": "WARMING_UP",
            "enqueued_ts_ms": 1,
        }

        with patch.object(runtime_meta, "should_defer_noncritical_startup_write", return_value=True):
            with patch.object(runtime_meta, "_drain_best_effort_rows", side_effect=AssertionError("unexpected drain")):
                with patch.object(runtime_meta._BEST_EFFORT_BUFFER_STOP, "wait", side_effect=SystemExit):
                    with self.assertRaises(SystemExit):
                        runtime_meta._best_effort_writer_loop()

    def test_runtime_metrics_writer_waits_while_startup_gate_is_active(self) -> None:
        (_, metrics_store) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.metrics_store",
        )
        buffer = metrics_store._RUNTIME_METRICS_BUFFER
        buffer._pending[:] = [
            metrics_store._runtime_metric_row("startup.metric", value_num=1.0),
        ]

        with patch.object(metrics_store, "should_defer_noncritical_startup_write", return_value=True):
            with patch.object(metrics_store, "_drain_runtime_metrics_buffer", side_effect=AssertionError("unexpected drain")):
                with patch.object(buffer._stop, "wait", side_effect=SystemExit):
                    with self.assertRaises(SystemExit):
                        metrics_store._runtime_metrics_writer_loop()

    def test_runtime_metrics_writer_coalesces_burst_before_first_flush(self) -> None:
        self._set_env("RUNTIME_METRICS_FLUSH_INTERVAL_S", "0.1")
        self._set_env("RUNTIME_METRICS_FLUSH_JITTER_RATIO", "0")
        (_, metrics_store) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.metrics_store",
        )
        buffer = metrics_store._RUNTIME_METRICS_BUFFER
        buffer._pending[:] = [
            metrics_store._runtime_metric_row("burst.metric", value_num=1.0),
        ]
        wait_calls: list[float] = []
        monotonic_calls = {"count": 0}

        def _fake_monotonic() -> float:
            monotonic_calls["count"] += 1
            return 100.0 if int(monotonic_calls["count"]) == 1 else 100.11

        with patch.object(metrics_store, "should_defer_noncritical_startup_write", return_value=False):
            with patch.object(metrics_store.time, "monotonic", side_effect=_fake_monotonic):
                with patch.object(
                    buffer._condition,
                    "wait",
                    side_effect=lambda timeout=None: wait_calls.append(float(timeout or 0.0)),
                ):
                    with patch.object(metrics_store, "_flush_runtime_metric_rows", side_effect=SystemExit):
                        with self.assertRaises(SystemExit):
                            metrics_store._runtime_metrics_writer_loop()

        self.assertGreaterEqual(len(wait_calls), 1)
        self.assertGreaterEqual(float(wait_calls[0]), 0.09)

    def test_event_log_writer_waits_while_startup_gate_is_active(self) -> None:
        (_, event_log) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_log",
        )
        event_log._EVENT_LOG_BUFFER_PENDING[:] = [
            (
                1,
                "startup_probe",
                "tests.test_sqlite_contention_relief",
                1,
                "probe",
                "startup",
                None,
                "{}",
            ),
        ]

        with patch.object(event_log, "should_defer_noncritical_startup_write", return_value=True):
            with patch.object(event_log, "_drain_event_log_buffer", side_effect=AssertionError("unexpected drain")):
                with patch.object(event_log._EVENT_LOG_BUFFER_STOP, "wait", side_effect=SystemExit):
                    with self.assertRaises(SystemExit):
                        event_log._event_log_writer_loop()

    def test_event_log_writer_coalesces_burst_before_first_flush(self) -> None:
        self._set_env("EVENT_LOG_BUFFER_FLUSH_INTERVAL_S", "0.1")
        self._set_env("EVENT_LOG_BUFFER_FLUSH_JITTER_RATIO", "0")
        (_, event_log) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_log",
        )
        event_log._EVENT_LOG_BUFFER_PENDING[:] = [
            (
                1,
                "burst_probe",
                "tests.test_sqlite_contention_relief",
                1,
                "probe",
                "one",
                None,
                "{}",
            )
        ]
        wait_calls: list[float] = []
        monotonic_calls = {"count": 0}

        def _fake_monotonic() -> float:
            monotonic_calls["count"] += 1
            return 100.0 if int(monotonic_calls["count"]) == 1 else 100.11

        with patch.object(event_log, "should_defer_noncritical_startup_write", return_value=False):
            with patch.object(event_log.time, "monotonic", side_effect=_fake_monotonic):
                with patch.object(
                    event_log._EVENT_LOG_BUFFER_LOCK,
                    "wait",
                    side_effect=lambda timeout=None: wait_calls.append(float(timeout or 0.0)),
                ):
                    with patch.object(event_log, "_flush_event_log_rows", side_effect=SystemExit):
                        with self.assertRaises(SystemExit):
                            event_log._event_log_writer_loop()

        self.assertGreaterEqual(len(wait_calls), 1)
        self.assertGreaterEqual(float(wait_calls[0]), 0.09)

    def test_telemetry_append_writer_waits_while_startup_gate_is_active(self) -> None:
        (_, _, telemetry_append_buffer) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_status",
            "engine.runtime.telemetry_append_buffer",
        )
        for rows in telemetry_append_buffer._BUFFER_PENDING.values():
            rows.clear()
        telemetry_append_buffer._BUFFER_PENDING["price_provider_health"].append(
            (1, "polygon", 1, 12, 5, None, 1, 0)
        )

        with patch.object(telemetry_append_buffer, "should_defer_noncritical_startup_write", return_value=True):
            with patch.object(telemetry_append_buffer, "_drain_rows_locked", side_effect=AssertionError("unexpected drain")):
                with patch.object(telemetry_append_buffer._BUFFER_STOP, "wait", side_effect=SystemExit):
                    with self.assertRaises(SystemExit):
                        telemetry_append_buffer._buffer_writer_loop()

    def test_telemetry_append_buffer_count_uses_memory_pending_without_spool_stat(self) -> None:
        (_, _, telemetry_append_buffer) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_status",
            "engine.runtime.telemetry_append_buffer",
        )
        for rows in telemetry_append_buffer._BUFFER_PENDING.values():
            rows.clear()
        telemetry_append_buffer._BUFFER_STATE["spooled_rows"] = 0
        telemetry_append_buffer._BUFFER_PENDING["price_provider_health"].append(
            (1, "polygon", 1, 12, 5, None, 1, 0)
        )

        with telemetry_append_buffer._BUFFER_LOCK:
            with patch.object(
                telemetry_append_buffer,
                "_spool_stats",
                side_effect=AssertionError("memory-pending counts must not stat durable spool"),
            ):
                self.assertEqual(telemetry_append_buffer._buffered_row_count_locked(), 1)

    def test_telemetry_append_writer_coalesces_burst_before_first_flush(self) -> None:
        self._set_env("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "0.1")
        self._set_env("TELEMETRY_APPEND_BUFFER_FLUSH_JITTER_RATIO", "0")
        (_, _, telemetry_append_buffer) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ingestion_status",
            "engine.runtime.telemetry_append_buffer",
        )
        for rows in telemetry_append_buffer._BUFFER_PENDING.values():
            rows.clear()
        telemetry_append_buffer._BUFFER_PENDING["price_provider_health"].append(
            (1, "polygon", 1, 12, 5, None, 1, 0)
        )
        wait_calls: list[float] = []
        monotonic_calls = {"count": 0}

        def _fake_monotonic() -> float:
            monotonic_calls["count"] += 1
            return 100.0 if int(monotonic_calls["count"]) == 1 else 100.11

        with patch.object(telemetry_append_buffer, "should_defer_noncritical_startup_write", return_value=False):
            with patch.object(telemetry_append_buffer.time, "monotonic", side_effect=_fake_monotonic):
                with patch.object(
                    telemetry_append_buffer._BUFFER_LOCK,
                    "wait",
                    side_effect=lambda timeout=None: wait_calls.append(float(timeout or 0.0)),
                ):
                    with patch.object(telemetry_append_buffer, "_flush_rows", side_effect=SystemExit):
                        with self.assertRaises(SystemExit):
                            telemetry_append_buffer._buffer_writer_loop()

        self.assertGreaterEqual(len(wait_calls), 1)
        self.assertGreaterEqual(float(wait_calls[0]), 0.09)


if __name__ == "__main__":
    unittest.main()
