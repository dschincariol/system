from __future__ import annotations

import importlib
import os
import sys
import tempfile
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


class TelemetryReadRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env: dict[str, str | None] = {}
        self._set_env("DB_PATH", str(Path(self.tmp.name) / "telemetry_read_routing.db"))
        self._set_env("ENGINE_MODE", "safe")
        self._set_env("ENGINE_SUPERVISED", "1")
        self._set_env("TIMESCALE_ENABLED", "0")
        self._set_env("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")
        self._set_env("TIMESCALE_TELEMETRY_VALIDATION_ENABLED", "0")
        self._set_env("FEATURE_STORE_ENABLED", "0")
        self._set_env("HEALTH_SNAPSHOT_CACHE_TTL_S", "0")
        self._set_env("TELEMETRY_READ_BACKEND", None)
        self._set_env("TELEMETRY_READ_REQUIRE_VALIDATION", "1")
        self._set_env("TELEMETRY_READ_FALLBACK_TO_SQLITE", "1")
        self.storage = None

    def tearDown(self) -> None:
        try:
            telemetry_append_buffer = importlib.import_module("engine.runtime.telemetry_append_buffer")
            telemetry_append_buffer.shutdown_telemetry_append_buffers(timeout_s=1.0)
        except Exception:
            pass
        if self.storage is not None:
            try:
                self.storage.shutdown_timeseries_storage(timeout_s=0.1)
            except Exception:
                pass
            try:
                self.storage.close_pooled_connections()
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

    def _init_storage(self):
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()
        self.storage = storage
        return storage

    def test_router_auto_mode_falls_back_to_sqlite_when_timescale_is_unavailable(self) -> None:
        storage = self._init_storage()
        (router,) = _reload_modules("engine.runtime.telemetry_read_router")

        now_ms = int(time.time() * 1000)
        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                INSERT INTO runtime_metrics(ts_ms, metric, value_num, value_text, tags_json)
                VALUES (?,?,?,?,?)
                """,
                (now_ms, "unit.queue_depth", 7.0, None, '{"job":"unit"}'),
            )
            con.execute(
                """
                INSERT INTO event_log(
                  ts_ms, event_type, event_source, event_version, entity_type, entity_id, correlation_id, payload_json
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    now_ms,
                    "runtime_failure",
                    "unit_test",
                    1,
                    "job",
                    "poll_prices",
                    "corr-1",
                    '{"root_cause_code":"LOCK","failure_scope":"router","error_message":"db busy","error_type":"OperationalError"}',
                ),
            )
            con.execute(
                """
                INSERT INTO price_provider_health(
                  ts_ms, provider, ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (now_ms, "unit_provider", 1, 42, 12, None, now_ms, 0),
            )
            con.execute(
                """
                INSERT INTO data_source_logs(ts_ms, source_key, level, event_type, message, detail_json)
                VALUES (?,?,?,?,?,?)
                """,
                (now_ms, "polygon_ws", "INFO", "unit_test", "router path", '{"ok":true}'),
            )
            con.commit()
        finally:
            con.close()

        metrics = router.fetch_runtime_metrics(metric="unit.queue_depth", limit=10)
        self.assertTrue(bool(metrics.get("ok")))
        self.assertTrue(any(str(row.get("metric") or "") == "unit.queue_depth" for row in (metrics.get("rows") or [])))

        summary = router.fetch_event_log_summary()
        self.assertTrue(bool(summary.get("ok")))
        self.assertGreaterEqual(int(summary.get("count") or 0), 1)

        failures = router.fetch_recent_runtime_failure_events(limit=5)
        self.assertTrue(any(str(row.get("event_source") or "") == "unit_test" for row in failures))

        providers = router.fetch_provider_health_rows()
        self.assertTrue(any(str(row.get("provider") or "") == "unit_provider" for row in providers))

        logs = router.fetch_data_source_logs(source_key="polygon_ws", limit=10)
        self.assertTrue(any(str(row.get("message") or "") == "router path" for row in logs))

    def test_router_auto_mode_prefers_timescale_when_validation_passes(self) -> None:
        self._set_env("TIMESCALE_ENABLED", "1")
        self._set_env("TIMESCALE_DSN", "postgres://unit-test")
        self._set_env("TELEMETRY_READ_BACKEND", None)
        self._set_env("TELEMETRY_READ_REQUIRE_VALIDATION", "1")
        (router,) = _reload_modules("engine.runtime.telemetry_read_router")

        with patch.object(router, "_timescale_enabled", return_value=True):
            with patch.object(
                router,
                "get_telemetry_migration_validation_snapshot",
                return_value={"enabled": True, "ok": True, "detail": "validation_ok"},
            ):
                backend = router.get_telemetry_read_backend()

        self.assertEqual(backend, "timescale")

    def test_timescale_reader_reuses_pool_until_dsn_or_schema_changes(self) -> None:
        self._set_env("TIMESCALE_ENABLED", "1")
        self._set_env("TIMESCALE_DSN", "postgres://unit-test")
        self._set_env("TIMESCALE_SCHEMA", "public")
        self._set_env("TIMESCALE_POOL_MIN_SIZE", "1")
        self._set_env("TIMESCALE_POOL_MAX_SIZE", "3")
        self._set_env("TIMESCALE_CONNECT_TIMEOUT_S", "0.2")
        (router,) = _reload_modules("engine.runtime.telemetry_read_router")
        created = []

        class FakeCursor:
            def __init__(self) -> None:
                self.executed = []

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                return None

            def execute(self, sql, params=None):
                self.executed.append((str(sql), params))

        class FakeConnection:
            def __init__(self) -> None:
                self.cursor_obj = FakeCursor()
                self.autocommit = False
                self.closed = False

            def cursor(self):
                return self.cursor_obj

            def rollback(self):
                return None

            def close(self):
                self.closed = True

        class FakePool:
            def __init__(self, **kwargs) -> None:
                self.kwargs = dict(kwargs)
                self.open_calls = []
                self.close_calls = []
                self.getconn_calls = []
                self.putconn_calls = []
                self.connection = FakeConnection()
                created.append(self)

            def open(self, *, wait, timeout):
                self.open_calls.append((bool(wait), float(timeout)))

            def close(self, *, timeout):
                self.close_calls.append(float(timeout))

            def getconn(self, *, timeout):
                self.getconn_calls.append(float(timeout))
                return self.connection

            def putconn(self, con):
                self.putconn_calls.append(con)

        with patch.object(router, "psycopg", object()):
            with patch.object(router, "ConnectionPool", FakePool):
                real_from_env = router.TimescaleConfig.from_env
                with patch.object(router.TimescaleConfig, "from_env", side_effect=real_from_env) as from_env_mock:
                    with router._timescale_connection() as (_con, schema):
                        self.assertEqual(schema, "public")
                    with router._timescale_connection() as (_con, schema):
                        self.assertEqual(schema, "public")
                    self.assertEqual(from_env_mock.call_count, 1)
                    self.assertTrue(all(pool_key[0] == "telemetry_read" for pool_key in router._POOLS))

                    self._set_env("TIMESCALE_SCHEMA", "alt")
                    with router._timescale_connection() as (_con, schema):
                        self.assertEqual(schema, "alt")
                    self.assertEqual(from_env_mock.call_count, 2)
                    self.assertTrue(all(pool_key[0] == "telemetry_read" for pool_key in router._POOLS))
                    router.close_timescale_read_pool()

        self.assertEqual(len(created), 2)
        self.assertIn("unit-test", str(created[0].kwargs["conninfo"]))
        self.assertEqual(created[0].kwargs["min_size"], 1)
        self.assertEqual(created[0].kwargs["max_size"], 3)
        self.assertEqual(created[0].open_calls, [(True, 0.2)])
        self.assertEqual(created[0].getconn_calls, [0.2, 0.2])
        self.assertEqual(len(created[0].putconn_calls), 2)
        self.assertEqual(created[0].close_calls, [0.2])
        self.assertEqual(created[1].getconn_calls, [0.2])
        self.assertEqual(created[1].close_calls, [0.2])

    def test_telemetry_fetches_use_short_state_cache(self) -> None:
        self._set_env("TELEMETRY_READ_BACKEND", "sqlite")
        (router,) = _reload_modules("engine.runtime.telemetry_read_router")
        calls = {"count": 0}

        def fake_fetch(*, metric, since_ms, limit):
            calls["count"] += 1
            return {
                "ok": True,
                "metric": metric,
                "since_ms": since_ms,
                "rows": [{"ts_ms": calls["count"], "metric": metric}],
            }

        with patch.object(router, "_fetch_sqlite_runtime_metrics", side_effect=fake_fetch):
            first = router.fetch_runtime_metrics(metric="unit.cached", since_ms=123, limit=5)
            second = router.fetch_runtime_metrics(metric="unit.cached", since_ms=123, limit=5)

        self.assertEqual(calls["count"], 1)
        self.assertEqual(first, second)

    def test_router_sqlite_override_keeps_sqlite_primary(self) -> None:
        self._set_env("TIMESCALE_ENABLED", "1")
        self._set_env("TIMESCALE_DSN", "postgres://unit-test")
        self._set_env("TELEMETRY_READ_BACKEND", "sqlite")
        self._set_env("TELEMETRY_READ_REQUIRE_VALIDATION", "1")
        (router,) = _reload_modules("engine.runtime.telemetry_read_router")

        with patch.object(router, "_timescale_enabled", return_value=True):
            with patch.object(
                router,
                "get_telemetry_migration_validation_snapshot",
                return_value={"enabled": True, "ok": True, "detail": "validation_ok"},
            ):
                backend = router.get_telemetry_read_backend()

        self.assertEqual(backend, "sqlite")

    def test_provider_health_router_flushes_buffered_rows(self) -> None:
        self._set_env("TELEMETRY_APPEND_BUFFER_ENABLED", "1")
        self._set_env("TELEMETRY_APPEND_BUFFER_FLUSH_INTERVAL_S", "60")
        self._init_storage()
        router, telemetry_append_buffer = _reload_modules(
            "engine.runtime.telemetry_read_router",
            "engine.runtime.telemetry_append_buffer",
        )

        with patch.object(telemetry_append_buffer, "_ensure_buffer_thread_started", return_value=None):
            self.assertTrue(
                telemetry_append_buffer.enqueue_price_provider_health(
                    provider="buffered_provider",
                    ok=True,
                    latency_ms=12,
                    n_symbols=4,
                    error=None,
                    ts_ms=1_700_000_000_000,
                )
            )

        before = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        self.assertEqual(
            int(((before.get("pending_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )
        self.assertEqual(
            int(((before.get("flushed_by_table") or {}).get("price_provider_health") or 0)),
            0,
        )
        providers = router.fetch_provider_health_rows()
        after = telemetry_append_buffer.get_telemetry_append_buffer_snapshot()
        self.assertTrue(any(str(row.get("provider") or "") == "buffered_provider" for row in providers))
        self.assertEqual(
            int(((after.get("pending_by_table") or {}).get("price_provider_health") or 0)),
            0,
        )
        self.assertEqual(
            int(((after.get("flushed_by_table") or {}).get("price_provider_health") or 0)),
            1,
        )

    def test_router_falls_back_to_sqlite_when_timescale_fetch_fails(self) -> None:
        self._set_env("TIMESCALE_ENABLED", "1")
        self._set_env("TIMESCALE_DSN", "postgres://unit-test")
        self._set_env("TELEMETRY_READ_BACKEND", "timescale")
        self._set_env("TELEMETRY_READ_REQUIRE_VALIDATION", "0")
        storage = self._init_storage()
        (router,) = _reload_modules("engine.runtime.telemetry_read_router")

        now_ms = int(time.time() * 1000)
        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                INSERT INTO runtime_metrics(ts_ms, metric, value_num, value_text, tags_json)
                VALUES (?,?,?,?,?)
                """,
                (now_ms, "unit.fallback", 3.0, None, '{"source":"sqlite"}'),
            )
            con.commit()
        finally:
            con.close()

        with patch.object(router, "_timescale_enabled", return_value=True):
            with patch.object(router, "_fetch_timescale_runtime_metrics", side_effect=RuntimeError("boom")):
                metrics = router.fetch_runtime_metrics(metric="unit.fallback", limit=5)

        self.assertTrue(bool(metrics.get("ok")))
        self.assertTrue(any(str(row.get("metric") or "") == "unit.fallback" for row in (metrics.get("rows") or [])))

    def test_metrics_store_runtime_history_uses_router(self) -> None:
        self._init_storage()
        metrics_store, router = _reload_modules(
            "engine.runtime.metrics_store",
            "engine.runtime.telemetry_read_router",
        )

        expected = {
            "ok": True,
            "metric": "unit.metric",
            "since_ms": 123,
            "rows": [{"ts_ms": 456, "metric": "unit.metric", "value_num": 1.0, "value_text": None, "tags": {"path": "router"}}],
        }
        with patch.object(router, "fetch_runtime_metrics", return_value=expected) as fetch_mock:
            out = metrics_store.get_runtime_metrics(metric="unit.metric", since_ms=123, limit=7)

        fetch_mock.assert_called_once_with(metric="unit.metric", since_ms=123, limit=7)
        self.assertEqual(out, expected)

    def test_api_and_health_surfaces_use_router_helpers(self) -> None:
        self._init_storage()
        api_read, api_system, health = _reload_modules(
            "engine.api.api_read",
            "engine.api.api_system",
            "engine.runtime.health",
        )

        now_ms = int(time.time() * 1000)
        provider_rows = [
            {
                "provider": "unit_provider",
                "ts_ms": now_ms,
                "ok": True,
                "latency_ms": 25.0,
                "n_symbols": 3,
                "error": None,
            }
        ]
        failure_rows = [
            {
                "ts_ms": now_ms,
                "event_source": "unit_test",
                "payload": {
                    "root_cause_code": "LOCK",
                    "failure_scope": "runtime_loop",
                    "error_message": "db busy",
                    "error_type": "OperationalError",
                },
            }
        ]
        event_summary = {"ok": True, "count": 9, "last_ts_ms": now_ms}

        with patch.object(api_read, "fetch_provider_health_rows", return_value=provider_rows):
            feed = api_read.get_feed_status()
        self.assertTrue(bool(feed.get("ok")))
        self.assertEqual(int((feed.get("summary") or {}).get("providers_total") or 0), 1)
        self.assertEqual(str(((feed.get("providers") or [{}])[0]).get("provider") or ""), "unit_provider")

        with patch.object(api_system, "fetch_recent_runtime_failure_events", return_value=failure_rows):
            errors = api_system._recent_runtime_errors(limit=5)
        self.assertTrue(any(str(item.get("code") or "") == "LOCK" for item in errors))
        self.assertTrue(any(str(item.get("title") or "") == "runtime_loop" for item in errors))

        with patch.object(health, "fetch_event_log_summary", return_value=event_summary):
            with patch.object(health, "fetch_provider_health_rows", return_value=provider_rows):
                snapshot = health.get_health_snapshot()
        self.assertEqual(int((snapshot.get("event_log") or {}).get("count") or 0), 9)
        self.assertEqual(int((snapshot.get("providers") or {}).get("healthy") or 0), 1)
        self.assertIn("unit_provider", dict((snapshot.get("providers") or {}).get("by_provider") or {}))

    def test_data_source_manager_list_logs_uses_router(self) -> None:
        self._init_storage()
        (data_source_manager,) = _reload_modules("services.data_source_manager")
        manager = data_source_manager.get_manager()

        expected_logs = [
            {
                "ts_ms": 1_700_000_000_000,
                "source_key": "polygon_ws",
                "level": "INFO",
                "event_type": "unit_test",
                "message": "mirrored",
                "detail": {"ok": True},
            }
        ]
        with patch.object(data_source_manager, "fetch_data_source_logs", return_value=expected_logs) as fetch_mock:
            logs = manager.list_logs("polygon_ws", limit=25)

        fetch_mock.assert_called_once_with(source_key="polygon_ws", limit=25)
        self.assertEqual(logs, expected_logs)


if __name__ == "__main__":
    unittest.main()
