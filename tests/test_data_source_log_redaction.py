from __future__ import annotations

import base64
import importlib
import json
import logging
import os
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
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


class _WarningCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(self.format(record))


class DataSourceLogRedactionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env: dict[str, str | None] = {}
        self._set_env("DB_PATH", str(Path(self.tmp.name) / "data_source_log_redaction.db"))
        self._set_env("TS_STORAGE_BACKEND", "sqlite")
        self._set_env("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
        self._set_env("DATA_SOURCE_MASTER_KEY_FILE", None)
        self._set_env("ENGINE_MODE", "safe")
        self._set_env("ENGINE_SUPERVISED", "0")
        self._set_env("TIMESCALE_ENABLED", "0")
        self._set_env("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")
        self._set_env("TELEMETRY_READ_BACKEND", "sqlite")
        self._set_env("TELEMETRY_READ_REQUIRE_VALIDATION", "0")
        self.storage = None

    def tearDown(self) -> None:
        try:
            telemetry_append_buffer = importlib.import_module("engine.runtime.telemetry_append_buffer")
            telemetry_append_buffer.shutdown_telemetry_append_buffers(timeout_s=1.0)
        except Exception:
            pass
        if self.storage is not None:
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

    def _all_raw_detail_text(self, table_name: str) -> str:
        con = self.storage.connect_ro_direct()
        try:
            rows = con.execute(f"SELECT detail_json FROM {table_name}").fetchall() or []
        finally:
            con.close()
        return "\n".join("" if row[0] is None else str(row[0]) for row in rows)

    def test_upsert_redacts_canary_from_logs_api_router_audit_and_warnings(self) -> None:
        storage = self._init_storage()
        data_source_manager, routes = _reload_modules(
            "services.data_source_manager",
            "routes.data_sources_routes",
        )
        manager = data_source_manager.DataSourceManager()
        canary = f"codex-canary-secret-{uuid.uuid4().hex}"

        capture = _WarningCapture()
        capture.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
        root_logger = logging.getLogger()
        root_logger.addHandler(capture)
        try:
            manager.update_source(
                {
                    "source_key": "polygon",
                    "credentials": {"api_key": canary},
                    "replace_credentials": True,
                    "actor": "unit-test",
                }
            )
        finally:
            root_logger.removeHandler(capture)

        raw_logs = self._all_raw_detail_text("data_source_logs")
        raw_audit = self._all_raw_detail_text("data_source_audit")
        self.assertNotIn(canary, raw_logs)
        self.assertNotIn(canary, raw_audit)

        con = storage.connect_ro_direct()
        try:
            log_details = [
                json.loads(str(row[0] or "{}"))
                for row in con.execute(
                    "SELECT detail_json FROM data_source_logs WHERE source_key = ?",
                    ("polygon",),
                ).fetchall()
            ]
            audit_details = [
                json.loads(str(row[0] or "{}"))
                for row in con.execute(
                    "SELECT detail_json FROM data_source_audit WHERE source_key = ?",
                    ("polygon",),
                ).fetchall()
            ]
        finally:
            con.close()
        self.assertTrue(log_details)
        self.assertTrue(audit_details)
        self.assertTrue(any(item.get("credentials") == "[REDACTED]" for item in log_details))
        self.assertFalse(any("credentials" in item for item in audit_details))
        self.assertTrue(any(item.get("status") in {"seeded", "configured"} for item in log_details))

        router = importlib.reload(importlib.import_module("engine.runtime.telemetry_read_router"))
        router_logs = router.fetch_data_source_logs(source_key="polygon", limit=20)
        route_payload = routes.api_get_data_source_logs({"source_key": "polygon", "limit": "20"})
        combined_response_text = json.dumps(
            {"router": router_logs, "route": route_payload},
            sort_keys=True,
            default=str,
        )
        self.assertNotIn(canary, combined_response_text)
        self.assertIn("[REDACTED]", combined_response_text)

        warning_text = "\n".join(capture.messages)
        self.assertNotIn(canary, warning_text)

    def test_existing_sqlite_log_cleanup_is_idempotent_and_preserves_status_fields(self) -> None:
        storage = self._init_storage()
        data_source_manager = importlib.reload(importlib.import_module("services.data_source_manager"))
        canary = f"codex-cleanup-secret-{uuid.uuid4().hex}"

        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                INSERT INTO data_source_logs(ts_ms, source_key, level, event_type, message, detail_json)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    int(time.time() * 1000),
                    "polygon",
                    "INFO",
                    "legacy_leak",
                    "legacy row",
                    json.dumps(
                        {
                            "status": "configured",
                            "credentials": {"api_key": canary},
                            "nested": {"client_secret": canary, "token_required": True},
                        },
                        separators=(",", ":"),
                    ),
                ),
            )
            con.commit()
        finally:
            con.close()

        router = importlib.reload(importlib.import_module("engine.runtime.telemetry_read_router"))
        routed_before_cleanup = router.fetch_data_source_logs(source_key="polygon", limit=10)
        routed_before_cleanup_text = json.dumps(routed_before_cleanup, sort_keys=True, default=str)
        self.assertNotIn(canary, routed_before_cleanup_text)
        self.assertIn("[REDACTED]", routed_before_cleanup_text)

        manager = data_source_manager.DataSourceManager()
        manager.initialize()
        manager._initialized = False
        manager.initialize()

        raw_logs = self._all_raw_detail_text("data_source_logs")
        self.assertNotIn(canary, raw_logs)
        con = storage.connect_ro_direct()
        try:
            detail_raw = con.execute(
                "SELECT detail_json FROM data_source_logs WHERE event_type = ? LIMIT 1",
                ("legacy_leak",),
            ).fetchone()[0]
        finally:
            con.close()
        detail = json.loads(str(detail_raw or "{}"))
        self.assertEqual(detail.get("status"), "configured")
        self.assertEqual(detail.get("credentials"), "[REDACTED]")
        self.assertEqual((detail.get("nested") or {}).get("client_secret"), "[REDACTED]")
        self.assertTrue(bool((detail.get("nested") or {}).get("token_required")))

    def test_telemetry_mirror_sanitizes_legacy_log_rows_before_timescale_enqueue(self) -> None:
        self._set_env("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "1")
        storage = self._init_storage()
        telemetry_mirror = importlib.reload(importlib.import_module("engine.runtime.telemetry_mirror"))
        canary = f"codex-mirror-secret-{uuid.uuid4().hex}"

        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                INSERT INTO data_source_logs(ts_ms, source_key, level, event_type, message, detail_json)
                VALUES (?,?,?,?,?,?)
                """,
                (
                    int(time.time() * 1000),
                    "polygon",
                    "INFO",
                    "legacy_mirror",
                    "legacy mirror row",
                    json.dumps({"api_token": canary, "status": "queued"}, separators=(",", ":")),
                ),
            )
            con.commit()
        finally:
            con.close()

        class FakeTimescaleClient:
            def __init__(self) -> None:
                self.enabled = True
                self.data_source_logs_rows: list[dict[str, object]] = []

            def enqueue_data_source_logs(self, rows):
                self.data_source_logs_rows.extend(dict(row) for row in rows)
                return len(rows)

            def __getattr__(self, name: str):
                if name.startswith("enqueue_"):
                    return lambda rows: len(rows)
                raise AttributeError(name)

        fake_client = FakeTimescaleClient()
        mirror = telemetry_mirror.TelemetryMirror(
            config=telemetry_mirror.TelemetryMirrorConfig(
                enabled=True,
                poll_interval_s=0.1,
                batch_size=100,
                start_mode="beginning",
            )
        )
        mirror._initialize_cursors()

        with patch.object(telemetry_mirror, "get_timescale_client", return_value=fake_client):
            mirrored = mirror._poll_once()

        self.assertTrue(bool(mirrored))
        rows_text = json.dumps(fake_client.data_source_logs_rows, sort_keys=True, default=str)
        self.assertNotIn(canary, rows_text)
        self.assertIn("[REDACTED]", rows_text)
        self.assertIn("queued", rows_text)


if __name__ == "__main__":
    unittest.main()
