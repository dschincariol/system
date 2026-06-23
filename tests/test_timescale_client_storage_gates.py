from __future__ import annotations

import asyncio
import importlib
import os
import sys
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeTimescalePool:
    def __init__(self, connections):
        self._connections = list(connections)
        self.acquire_count = 0

    def acquire(self):
        if self.acquire_count >= len(self._connections):
            conn = self._connections[-1]
        else:
            conn = self._connections[self.acquire_count]
        self.acquire_count += 1
        return _AsyncContext(conn)


class _FakeTimescaleConnection:
    def __init__(self, *, copy_available: bool = True):
        self.copy_available = bool(copy_available)
        self.executed: list[str] = []
        self.executemany_calls: list[tuple[str, list[tuple]]] = []
        self.copy_calls: list[dict] = []

    def transaction(self):
        return _AsyncContext(self)

    async def execute(self, sql: str, *args):
        self.executed.append(str(sql))
        return "OK"

    async def executemany(self, sql: str, rows):
        self.executemany_calls.append((str(sql), list(rows)))
        return "OK"

    def __getattribute__(self, name: str):
        if name == "copy_records_to_table" and not object.__getattribute__(self, "copy_available"):
            raise AttributeError(name)
        return object.__getattribute__(self, name)

    async def copy_records_to_table(self, table_name: str, *, records, columns, timeout=None):
        self.copy_calls.append(
            {
                "table_name": table_name,
                "records": list(records),
                "columns": tuple(columns),
                "timeout": timeout,
            }
        )
        return "COPY 0"


class _FakeTimescaleConnectionProxy:
    def __init__(self, connection):
        self._con = connection

    def __getattr__(self, name: str):
        return getattr(self._con, name)


def _config(timescale_client, *, enabled: bool = True, batch_size: int = 10, **overrides):
    values = {
        "enabled": enabled,
        "dsn": "postgres://example",
        "schema_name": "public",
        "pool_min_size": 1,
        "pool_max_size": 1,
        "batch_size": batch_size,
        "flush_interval_s": 0.5,
        "queue_maxsize": 32,
        "retry_attempts": 2,
        "retry_base_s": 0.1,
        "retry_max_s": 1.0,
        "backpressure_timeout_s": 1.0,
        "start_timeout_s": 1.0,
        "connect_timeout_s": 1.0,
        "lock_timeout_s": 1.0,
        "command_timeout_s": 5.0,
        "idle_in_txn_timeout_s": 30.0,
        "application_name": "unit-test",
    }
    values.update(overrides)
    return timescale_client.TimescaleConfig(**values)


class TimescaleClientStorageGateTests(unittest.TestCase):
    def test_from_env_uses_safe_timescale_batch_and_queue_defaults(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")

        with mock.patch.dict(os.environ, {}, clear=True):
            config = timescale_client.TimescaleConfig.from_env()

        self.assertEqual(2000, config.batch_size)
        self.assertEqual(256, config.queue_maxsize)

    def test_from_env_clamps_timescale_batch_to_hard_upper_bound(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")

        with mock.patch.dict(os.environ, {"TIMESCALE_BATCH_SIZE": "9999"}, clear=True):
            config = timescale_client.TimescaleConfig.from_env()

        self.assertEqual(5000, config.batch_size)

    def test_snapshot_requires_schema_ready_before_reporting_ok(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        client = timescale_client.TimescaleClient(config=_config(timescale_client))
        client._thread = threading.current_thread()

        snapshot = client.get_snapshot()

        self.assertFalse(bool(snapshot.get("ok")))
        self.assertIn("schema_not_ready", list(snapshot.get("degraded_reasons") or []))
        self.assertFalse(bool(snapshot.get("schema_ready")))

    def test_start_returns_snapshot_for_live_writer_without_lock_deadlock(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        client = timescale_client.TimescaleClient(config=_config(timescale_client))
        client._thread = threading.current_thread()
        timescale_client.asyncpg = object()
        result: list[dict] = []
        errors: list[BaseException] = []

        def _call_start() -> None:
            try:
                result.append(client.start())
            except BaseException as exc:
                errors.append(exc)

        worker = threading.Thread(target=_call_start, name="timescale-start-regression", daemon=True)
        worker.start()
        worker.join(timeout=1.0)

        self.assertFalse(worker.is_alive(), "TimescaleClient.start() deadlocked while snapshotting an active writer")
        self.assertEqual([], errors)
        self.assertEqual(1, len(result))
        self.assertTrue(bool(result[0].get("enabled")))

    def test_table_policies_include_compress_orderby_for_real_time_columns(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        client = timescale_client.TimescaleClient(
            config=_config(timescale_client, compression_after_days=7)
        )
        conn = _FakeTimescaleConnection()

        asyncio.run(client._apply_table_policies(conn, "price_data"))
        asyncio.run(client._apply_table_policies(conn, "runtime_metrics"))

        rendered_sql = "\n".join(conn.executed)
        self.assertIn('timescaledb.compress_orderby = \'"timestamp" DESC\'', rendered_sql)
        self.assertIn('timescaledb.compress_orderby = \'"time" DESC\'', rendered_sql)
        self.assertIn("timescaledb.compress_segmentby = 'symbol'", rendered_sql)
        self.assertIn("timescaledb.compress_segmentby = 'metric'", rendered_sql)

    def test_flush_uses_copy_staging_upsert_and_dedupes_last_row(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        client = timescale_client.TimescaleClient(config=_config(timescale_client))
        client._schema_ready = True
        conn = _FakeTimescaleConnection(copy_available=True)
        client._pool = _FakeTimescalePool(
            [
                _FakeTimescaleConnectionProxy(conn),
                _FakeTimescaleConnectionProxy(conn),
            ]
        )
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [
            ("SPY", ts, 1.0, 2.0, 0.5, 1.5, 100.0),
            ("SPY", ts, 1.1, 2.1, 0.6, 1.6, 101.0),
            ("QQQ", ts, 3.0, 4.0, 2.5, 3.5, 200.0),
        ]

        ok = asyncio.run(client._flush_with_retry("price_data", rows))

        self.assertTrue(ok)
        self.assertEqual([], conn.executemany_calls)
        self.assertEqual(1, len(conn.copy_calls))
        self.assertEqual(
            ("_ordinal", "symbol", "timestamp", "open", "high", "low", "close", "volume"),
            conn.copy_calls[0]["columns"],
        )
        self.assertEqual("__ts_stage_price_data", conn.copy_calls[0]["table_name"])
        self.assertEqual([0, 1, 2], [record[0] for record in conn.copy_calls[0]["records"]])
        upsert_sql = "\n".join(conn.executed)
        self.assertIn('CREATE TEMP TABLE IF NOT EXISTS "__ts_stage_price_data"', upsert_sql)
        self.assertIn("ON COMMIT DELETE ROWS", upsert_sql)
        self.assertIn('SELECT DISTINCT ON ("symbol", "timestamp")', upsert_sql)
        self.assertIn('ORDER BY "symbol", "timestamp", "_ordinal" DESC', upsert_sql)
        self.assertIn('ON CONFLICT ("symbol", "timestamp") DO UPDATE SET', upsert_sql)
        metrics = client.get_snapshot()["metrics"]
        self.assertEqual(1, metrics["copy_batches"])
        self.assertEqual(3, metrics["copy_rows"])
        self.assertEqual(1, metrics["deduped_rows"])
        self.assertEqual("copy_staging", metrics["table_stats"]["price_data"]["last_write_path"])

    def test_flush_falls_back_to_executemany_when_asyncpg_copy_is_unavailable(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        client = timescale_client.TimescaleClient(config=_config(timescale_client))
        client._schema_ready = True
        conn = _FakeTimescaleConnection(copy_available=False)
        client._pool = _FakeTimescalePool([conn])
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [("SPY", ts, 1.0, 2.0, 0.5, 1.5, 100.0)]

        ok = asyncio.run(client._flush_with_retry("price_data", rows))

        self.assertTrue(ok)
        self.assertEqual(1, len(conn.executemany_calls))
        self.assertEqual([], conn.copy_calls)
        metrics = client.get_snapshot()["metrics"]
        self.assertEqual(1, metrics["copy_fallback_count"])
        self.assertEqual("copy_records_to_table_unavailable", metrics["last_copy_fallback_reason"])
        self.assertEqual("executemany_fallback", metrics["table_stats"]["price_data"]["last_write_path"])

    def test_optimized_path_reuses_session_staging_table_without_per_flush_ddl(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        client = timescale_client.TimescaleClient(config=_config(timescale_client))
        client._schema_ready = True
        conn = _FakeTimescaleConnection(copy_available=True)
        client._pool = _FakeTimescalePool(
            [
                _FakeTimescaleConnectionProxy(conn),
                _FakeTimescaleConnectionProxy(conn),
            ]
        )
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [("SPY", ts, 1.0, 2.0, 0.5, 1.5, 100.0)]

        first_ok = asyncio.run(client._flush_with_retry("price_data", rows))
        second_ok = asyncio.run(client._flush_with_retry("price_data", rows))

        self.assertTrue(first_ok)
        self.assertTrue(second_ok)
        self.assertEqual(2, len(conn.copy_calls))
        create_temp_statements = [
            sql
            for sql in conn.executed
            if "CREATE TEMP TABLE" in sql and "__ts_stage_price_data" in sql
        ]
        self.assertEqual(1, len(create_temp_statements))

    def test_unsupported_table_stays_on_existing_executemany_upsert_path_with_fallback_metrics(self) -> None:
        (timescale_client,) = _reload_modules("engine.runtime.timescale_client")
        client = timescale_client.TimescaleClient(config=_config(timescale_client))
        client._schema_ready = True
        conn = _FakeTimescaleConnection(copy_available=True)
        client._pool = _FakeTimescalePool([conn])
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [("model-a", "v1", ts, '{"kind":"unit"}')]

        ok = asyncio.run(client._flush_with_retry("model_registry", rows))

        self.assertTrue(ok)
        self.assertEqual(1, len(conn.executemany_calls))
        self.assertEqual([], conn.copy_calls)
        self.assertIn("ON CONFLICT", conn.executemany_calls[0][0])
        metrics = client.get_snapshot()["metrics"]
        self.assertEqual(0, metrics["copy_batches"])
        self.assertEqual(1, metrics["copy_fallback_count"])
        self.assertEqual("unsupported_table", metrics["last_copy_fallback_reason"])
        self.assertEqual("executemany_unsupported", metrics["table_stats"]["model_registry"]["last_write_path"])


if __name__ == "__main__":
    unittest.main()
