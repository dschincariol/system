from __future__ import annotations

import importlib
import os
import sys
import tempfile
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


class _FakeTimescaleClient:
    enabled = True

    def __init__(self) -> None:
        self.runtime_metrics_rows: list[dict[str, object]] = []
        self.event_log_rows: list[dict[str, object]] = []

    def enqueue_runtime_metrics(self, rows, *, timeout_s=None) -> int:
        batch = [dict(row) for row in (rows or ())]
        self.runtime_metrics_rows.extend(batch)
        return len(batch)

    def enqueue_event_log(self, rows, *, timeout_s=None) -> int:
        batch = [dict(row) for row in (rows or ())]
        self.event_log_rows.extend(batch)
        return len(batch)


class _FakeAliveThread:
    def __init__(self) -> None:
        self.started = False

    def start(self) -> None:
        self.started = True

    def is_alive(self) -> bool:
        return True


class TelemetryMirrorValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env: dict[str, str | None] = {}
        self._set_env("DB_PATH", str(Path(self.tmp.name) / "telemetry_mirror.db"))
        self._set_env("ENGINE_SUPERVISED", "1")

    def tearDown(self) -> None:
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

    def test_telemetry_mirror_polls_committed_rows_into_timescale_client(self) -> None:
        self._set_env("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "1")
        storage, telemetry_mirror = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.telemetry_mirror",
        )
        storage.init_db()

        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                INSERT INTO runtime_metrics(ts_ms, metric, value_num, value_text, tags_json)
                VALUES (?,?,?,?,?)
                """,
                (1_700_000_000_000, "queue_depth", 7.0, None, '{"job":"unit"}'),
            )
            con.execute(
                """
                INSERT INTO event_log(
                  ts_ms, event_type, event_source, event_version, entity_type, entity_id, correlation_id, payload_json
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    1_700_000_000_100,
                    "runtime.unit",
                    "unit_test",
                    1,
                    "job",
                    "poll_prices",
                    "corr-1",
                    '{"ok":true}',
                ),
            )
            con.commit()
        finally:
            con.close()

        fake_client = _FakeTimescaleClient()
        config = telemetry_mirror.TelemetryMirrorConfig(
            enabled=True,
            poll_interval_s=0.1,
            batch_size=100,
            start_mode="beginning",
        )
        mirror = telemetry_mirror.TelemetryMirror(config=config)
        mirror._initialize_cursors()

        with patch.object(telemetry_mirror, "get_timescale_client", return_value=fake_client):
            mirrored = mirror._poll_once()

        self.assertTrue(bool(mirrored))
        self.assertTrue(
            any(str(row.get("metric") or "") == "queue_depth" for row in fake_client.runtime_metrics_rows)
        )
        self.assertTrue(
            any(str(row.get("event_type") or "") == "runtime.unit" for row in fake_client.event_log_rows)
        )

    def test_telemetry_validation_detects_parity_and_health_problems(self) -> None:
        self._set_env("TIMESCALE_ENABLED", "1")
        self._set_env("TIMESCALE_DSN", "postgres://unit-test")
        (validation,) = _reload_modules("engine.runtime.telemetry_migration_validation")

        sqlite_summary = {
            "runtime_metrics": {"count": 10, "max_ts_ms": 1000, "max_rowid": 10},
            "event_log": {"count": 8, "max_ts_ms": 1001, "max_rowid": 8},
            "ingestion_pipeline_health": {"count": 4, "max_ts_ms": 1002, "max_rowid": 4},
            "price_provider_health": {"count": 3, "max_ts_ms": 1003, "max_rowid": 3},
            "weather_provider_health": {"count": 2, "max_ts_ms": 1004, "max_rowid": 2},
            "data_source_logs": {"count": 5, "max_ts_ms": 1005, "max_rowid": 5},
        }
        timescale_summary = {
            "runtime_metrics": {"count": 9, "max_ts_ms": 990, "max_rowid": 9},
            "event_log": {"count": 8, "max_ts_ms": 1001, "max_rowid": 8},
            "ingestion_pipeline_health": {"count": 4, "max_ts_ms": 1002, "max_rowid": 4},
            "price_provider_health": {"count": 1, "max_ts_ms": 995, "max_rowid": 1},
            "weather_provider_health": {"count": 2, "max_ts_ms": 1004, "max_rowid": 2},
            "data_source_logs": {"count": 5, "max_ts_ms": 1005, "max_rowid": 5},
        }
        storage_snapshot = {
            "enabled": True,
            "ok": False,
            "telemetry_mirror": {"enabled": True, "ok": False},
        }

        with patch.object(validation, "asyncpg", object()):
            with patch.object(validation, "get_timeseries_storage_snapshot", return_value=storage_snapshot):
                with patch.object(validation, "_sqlite_summary", return_value=sqlite_summary):
                    with patch.object(validation, "_timescale_summary", return_value=timescale_summary):
                        snapshot = validation.build_telemetry_migration_validation_snapshot(
                            lookback_minutes=5,
                            max_count_delta=0,
                            max_last_ts_lag_ms=5,
                            require_healthy_mirror=True,
                            require_healthy_timescale=True,
                        )

        self.assertFalse(bool(snapshot.get("ok")))
        self.assertIn("telemetry_mirror_not_ok", list(snapshot.get("reasons") or []))
        self.assertIn("timescale_not_ok", list(snapshot.get("reasons") or []))
        self.assertIn("runtime_metrics_parity_out_of_bounds", list(snapshot.get("reasons") or []))
        self.assertIn("price_provider_health_parity_out_of_bounds", list(snapshot.get("reasons") or []))

    def test_health_snapshot_exposes_telemetry_migration_validation(self) -> None:
        storage, health, validation = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.runtime.telemetry_migration_validation",
        )
        storage.init_db()

        with patch.object(
            validation,
            "get_telemetry_migration_validation_snapshot",
            return_value={"ok": False, "enabled": True, "detail": "unit_test", "reasons": ["mismatch"], "ts_ms": 1},
        ):
            snapshot = health.get_health_snapshot()

        self.assertIn("telemetry_migration_validation", snapshot)
        self.assertFalse(bool((snapshot.get("telemetry_migration_validation") or {}).get("ok")))
        self.assertEqual(str((snapshot.get("telemetry_migration_validation") or {}).get("detail") or ""), "unit_test")

    def test_timeseries_storage_snapshot_gates_on_enabled_telemetry_mirror(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")

        fake_timescale = SimpleNamespace(
            get_timescale_snapshot=lambda: {"ok": True, "enabled": False, "degraded": False, "ts_ms": 1},
        )
        fake_telemetry_mirror = SimpleNamespace(
            get_telemetry_mirror_snapshot=lambda: {
                "ok": False,
                "enabled": True,
                "thread_alive": False,
                "error": "mirror_failed",
                "ts_ms": 2,
            },
        )
        fake_telemetry_append_buffer = SimpleNamespace(
            get_telemetry_append_buffer_snapshot=lambda: {"ok": True, "enabled": True, "ts_ms": 3},
        )
        fake_feature_store = SimpleNamespace(
            get_feature_store_snapshot=lambda: {"ok": True, "enabled": False, "ts_ms": 4},
        )
        fake_market_feature_store = SimpleNamespace(
            get_feature_store_snapshot=lambda timescale_snapshot=None: {
                "ok": True,
                "write_mode": "sqlite",
                "timescale_enabled": False,
                "ts_ms": 5,
            },
        )

        with patch.object(storage, "_load_timescale_module", return_value=fake_timescale):
            with patch.object(storage, "_load_telemetry_mirror_module", return_value=fake_telemetry_mirror):
                with patch.object(storage, "_load_telemetry_append_buffer_module", return_value=fake_telemetry_append_buffer):
                    with patch.object(storage, "_load_feature_store_module", return_value=fake_feature_store):
                        with patch.object(storage, "_load_market_feature_store_module", return_value=fake_market_feature_store):
                            snapshot = storage.get_timeseries_storage_snapshot()

        self.assertTrue(bool(snapshot.get("enabled")))
        self.assertFalse(bool(snapshot.get("ok")))
        self.assertTrue(bool(snapshot.get("degraded")))
        self.assertIn("telemetry_mirror_not_ok", list(snapshot.get("degraded_reasons") or []))
        self.assertEqual(str((snapshot.get("telemetry_mirror") or {}).get("error") or ""), "mirror_failed")

    def test_telemetry_mirror_snapshot_reports_missing_timescale_client_as_degraded(self) -> None:
        (_, telemetry_mirror) = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.telemetry_mirror",
        )
        mirror = telemetry_mirror.TelemetryMirror(
            config=telemetry_mirror.TelemetryMirrorConfig(
                enabled=True,
                poll_interval_s=0.1,
                batch_size=100,
                start_mode="current",
            )
        )

        with patch.object(telemetry_mirror, "get_timescale_client", return_value=None):
            snapshot = mirror.start()

        self.assertFalse(bool(snapshot.get("ok")))
        self.assertFalse(bool(snapshot.get("started")))
        self.assertTrue(bool(snapshot.get("degraded")))
        self.assertIn("mirror_stopped", list(snapshot.get("degraded_reasons") or []))
        self.assertIn("timescale_client_unavailable", list(snapshot.get("degraded_reasons") or []))
        self.assertEqual(
            str(((snapshot.get("metrics") or {}).get("last_error")) or ""),
            "timescale_client_not_enabled_for_telemetry_mirror",
        )

    def test_telemetry_mirror_recovery_clears_stale_error_before_and_after_idle_poll(self) -> None:
        storage, telemetry_mirror = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.telemetry_mirror",
        )
        storage.init_db()
        mirror = telemetry_mirror.TelemetryMirror(
            config=telemetry_mirror.TelemetryMirrorConfig(
                enabled=True,
                poll_interval_s=0.1,
                batch_size=100,
                start_mode="beginning",
            )
        )
        mirror._metrics["last_error"] = "timescale_client_not_enabled_for_telemetry_mirror"
        mirror._metrics["last_error_ts_ms"] = 123
        fake_client = _FakeTimescaleClient()
        fake_thread = _FakeAliveThread()

        with patch.object(telemetry_mirror, "get_timescale_client", return_value=fake_client):
            with patch.object(mirror, "_initialize_cursors", return_value=None):
                with patch.object(telemetry_mirror.threading, "Thread", return_value=fake_thread):
                    started = mirror.start()

            self.assertTrue(bool(started.get("ok")))
            self.assertEqual(str(((started.get("metrics") or {}).get("last_error")) or ""), "")

            mirrored = mirror._poll_once()
            snapshot = mirror.get_snapshot()

        self.assertFalse(bool(mirrored))
        self.assertTrue(bool(snapshot.get("ok")))
        self.assertEqual(str(((snapshot.get("metrics") or {}).get("last_error")) or ""), "")
        self.assertEqual(str(snapshot.get("detail") or ""), "ok")


if __name__ == "__main__":
    unittest.main()
