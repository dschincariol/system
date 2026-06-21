"""Regression tests for fail-closed startup health validation."""

from __future__ import annotations

import importlib
import json
import os
import socket
import sqlite3
import stat
import sys
import tempfile
import threading
import time
import types
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


def _reserve_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _FakeJobs:
    def __init__(self, *, start_delay_s: float = 0.0) -> None:
        self.start_delay_s = float(start_delay_s)

    def list_jobs(self):
        return []

    def get(self, _name: str):
        return None

    def start(self, _name: str):
        if self.start_delay_s > 0:
            time.sleep(self.start_delay_s)
        return {"ok": True}


class _RecordingJobs(_FakeJobs):
    def __init__(self) -> None:
        super().__init__(start_delay_s=0.0)
        self.started: list[str] = []

    def start(self, name: str):
        self.started.append(str(name))
        return {"ok": True}


class StartupHealthValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._prev_allow_training = os.environ.get("ALLOW_TRAINING")
        self._prev_schema_per_db_path = os.environ.get("TS_PG_SCHEMA_PER_DB_PATH")
        tmp_root = Path(self.tmp.name)
        (tmp_root / "logs").mkdir(parents=True, exist_ok=True)
        (tmp_root / "data").mkdir(parents=True, exist_ok=True)
        os.environ["TRADING_LOGS"] = str(tmp_root / "logs")
        os.environ["TRADING_DATA"] = str(tmp_root / "data")
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "startup_validation.db")
        os.environ["ENV"] = "dev"
        os.environ["ENGINE_MODE"] = "safe"
        os.environ["ALLOW_TRAINING"] = "0"
        os.environ["DASHBOARD_HOST"] = "127.0.0.1"
        os.environ["DASHBOARD_PORT"] = str(_reserve_free_port())
        os.environ["TRADING_STARTUP_HEALTH_ASYNC_BIND"] = "1"
        os.environ["TRADING_STARTUP_HEALTH_FAIL_OPEN"] = "0"
        os.environ["TS_PG_SCHEMA_PER_DB_PATH"] = "1"
        _reload_modules("engine.runtime.db_guard", "engine.runtime.storage")

    def test_startup_db_repair_retries_transient_sqlite_lock(self) -> None:
        (start_system,) = _reload_modules("start_system")
        attempts = {"count": 0}

        def _repair(*, startup_fast_path: bool):
            self.assertTrue(startup_fast_path)
            attempts["count"] += 1
            if attempts["count"] == 1:
                return {"ok": False, "error": "database is locked"}
            return {"ok": True}

        start_system._STARTUP_DB_REPAIR_LOCK_RETRIES = 2
        start_system._STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S = 0.01

        with patch("engine.runtime.db_repair.repair", side_effect=_repair):
            with patch.object(start_system.time, "sleep") as sleep:
                result = start_system._run_startup_db_repair()

        self.assertEqual(result, {"ok": True})
        self.assertEqual(attempts["count"], 2)
        sleep.assert_called_once()

    def test_ingestion_storage_ready_accepts_postgres_runtime_without_sqlite_file(self) -> None:
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "missing_runtime.sqlite")
        os.environ["TS_STORAGE_BACKEND"] = "postgres"
        os.environ["TS_PG_DSN"] = "host=timescaledb dbname=trading user=trading password=test"
        try:
            (start_system,) = _reload_modules("start_system")

            self.assertEqual(start_system._ingestion_storage_ready(), (True, "postgres"))
        finally:
            os.environ.pop("TS_STORAGE_BACKEND", None)
            os.environ.pop("TS_PG_DSN", None)

    def test_ingestion_storage_ready_requires_sqlite_file_for_sqlite_runtime(self) -> None:
        missing_db = Path(self.tmp.name) / "missing_runtime.sqlite"
        os.environ["DB_PATH"] = str(missing_db)
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ.pop("TS_PG_DSN", None)

        (start_system,) = _reload_modules("start_system")

        self.assertEqual(start_system._ingestion_storage_ready(), (False, str(missing_db)))

    def test_startup_import_smoke_compiles_jobs_without_importing_them_by_default(self) -> None:
        (start_system,) = _reload_modules("start_system")
        tmp_root = Path(self.tmp.name) / "import_smoke_default"
        (tmp_root / "engine" / "runtime").mkdir(parents=True)
        (tmp_root / "jobs").mkdir(parents=True)
        for path in [
            tmp_root / "engine" / "__init__.py",
            tmp_root / "engine" / "runtime" / "__init__.py",
            tmp_root / "jobs" / "__init__.py",
        ]:
            path.write_text("", encoding="utf-8")
        (tmp_root / "dashboard_server.py").write_text("VALUE = 1\n", encoding="utf-8")
        (tmp_root / "start_ingestion.py").write_text("VALUE = 1\n", encoding="utf-8")
        (tmp_root / "engine" / "runtime" / "ingestion_runtime.py").write_text("VALUE = 1\n", encoding="utf-8")
        (tmp_root / "jobs" / "slow_job.py").write_text(
            "raise RuntimeError('job import should not run during startup smoke')\n",
            encoding="utf-8",
        )
        fake_registry = types.ModuleType("engine.runtime.job_registry")
        fake_registry.ALLOWED_JOBS = {"slow_job": ("jobs/slow_job.py",)}

        start_system._BASE_DIR = str(tmp_root)
        start_system._IMPORT_SMOKE_IMPORT_JOBS = False
        start_system._IMPORT_SMOKE_TIMEOUT_S = 2.0
        start_system._IMPORT_SMOKE["ok"] = False
        start_system._IMPORT_SMOKE["failures"] = [{"stale": True}]

        with patch.dict(sys.modules, {"engine.runtime.job_registry": fake_registry}):
            with patch.object(start_system, "_persist_import_smoke"):
                with patch.object(start_system, "_persist_startup_trace"):
                    start_system._run_import_smoke()

        self.assertTrue(bool(start_system._IMPORT_SMOKE["ok"]))
        self.assertEqual(start_system._IMPORT_SMOKE["failures"], [])

    def test_startup_import_smoke_can_opt_into_full_job_imports(self) -> None:
        (start_system,) = _reload_modules("start_system")
        tmp_root = Path(self.tmp.name) / "import_smoke_full"
        (tmp_root / "engine" / "runtime").mkdir(parents=True)
        (tmp_root / "jobs").mkdir(parents=True)
        for path in [
            tmp_root / "engine" / "__init__.py",
            tmp_root / "engine" / "runtime" / "__init__.py",
            tmp_root / "jobs" / "__init__.py",
        ]:
            path.write_text("", encoding="utf-8")
        (tmp_root / "dashboard_server.py").write_text("VALUE = 1\n", encoding="utf-8")
        (tmp_root / "start_ingestion.py").write_text("VALUE = 1\n", encoding="utf-8")
        (tmp_root / "engine" / "runtime" / "ingestion_runtime.py").write_text("VALUE = 1\n", encoding="utf-8")
        (tmp_root / "jobs" / "slow_job.py").write_text(
            "raise RuntimeError('full job import failed')\n",
            encoding="utf-8",
        )
        fake_registry = types.ModuleType("engine.runtime.job_registry")
        fake_registry.ALLOWED_JOBS = {"slow_job": ("jobs/slow_job.py",)}

        start_system._BASE_DIR = str(tmp_root)
        start_system._IMPORT_SMOKE_IMPORT_JOBS = True
        start_system._IMPORT_SMOKE_TIMEOUT_S = 2.0
        start_system._IMPORT_SMOKE["ok"] = True
        start_system._IMPORT_SMOKE["failures"] = []

        with patch.dict(sys.modules, {"engine.runtime.job_registry": fake_registry}):
            with patch.object(start_system, "_persist_import_smoke"):
                with patch.object(start_system, "_persist_startup_trace"):
                    start_system._run_import_smoke()

        self.assertFalse(bool(start_system._IMPORT_SMOKE["ok"]))
        failures = list(start_system._IMPORT_SMOKE["failures"])
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["module"], "jobs.slow_job")
        self.assertIn("full job import failed", str(failures[0].get("stderr") or ""))

    def test_startup_validation_log_redacts_connection_and_token_material(self) -> None:
        (start_system,) = _reload_modules("start_system")
        dsn = "postgresql://unit:super-secret@127.0.0.1/trading"
        snapshot = {
            "ok": False,
            "mode": "safe",
            "gates": {
                "database_reachable": {
                    "ok": False,
                    "detail": "password=super-secret token=clear-token",
                    "dsn": dsn,
                }
            },
            "db_validation": {
                "dsn": dsn,
                "password": "super-secret",
                "api_key": "clear-key",
            },
            "reasons": ["token=clear-token"],
        }

        with self.assertLogs(start_system.LOG.name, level="WARNING") as records:
            start_system._log_startup_validation("unit", snapshot, level="warning", attempt=1, timeout_s=1.0)

        text = "\n".join(records.output)
        self.assertIn("<redacted>", text)
        self.assertNotIn("super-secret", text)
        self.assertNotIn("clear-token", text)
        self.assertNotIn("clear-key", text)
        self.assertNotIn(dsn, text)

    def test_local_env_bootstrap_uses_secret_file_pointer_for_master_key(self) -> None:
        (start_system,) = _reload_modules("start_system")
        tmp_root = Path(self.tmp.name) / "local_env_bootstrap"
        tmp_root.mkdir(parents=True)
        (tmp_root / ".env.example").write_text("# local template\n", encoding="utf-8")
        start_system._BASE_DIR = str(tmp_root)

        start_system._ensure_local_env_file()
        start_system._ensure_local_env_file()

        env_text = (tmp_root / ".env").read_text(encoding="utf-8")
        self.assertIn("DATA_SOURCE_MASTER_KEY_FILE=data/secrets/data_source_master_key", env_text)
        self.assertEqual(env_text.count("DATA_SOURCE_MASTER_KEY_FILE="), 1)
        self.assertNotIn("DATA_SOURCE_MASTER_KEY=", env_text)
        secret_path = tmp_root / "data" / "secrets" / "data_source_master_key"
        self.assertTrue(secret_path.is_file())
        self.assertTrue(bool(secret_path.read_text(encoding="utf-8").strip()))
        mode = stat.S_IMODE(secret_path.stat().st_mode)
        self.assertFalse(mode & (stat.S_IRWXG | stat.S_IRWXO), oct(mode))

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception as e:
            sys.stderr.write(
                f"[test_startup_health_validation] close_pooled_connections_failed: {type(e).__name__}: {e}\n"
            )
        if self._prev_allow_training is None:
            os.environ.pop("ALLOW_TRAINING", None)
        else:
            os.environ["ALLOW_TRAINING"] = str(self._prev_allow_training)
        if self._prev_schema_per_db_path is None:
            os.environ.pop("TS_PG_SCHEMA_PER_DB_PATH", None)
        else:
            os.environ["TS_PG_SCHEMA_PER_DB_PATH"] = str(self._prev_schema_per_db_path)
        self.tmp.cleanup()

    def test_startup_validation_accepts_healthy_runtime(self) -> None:
        storage, health = _reload_modules("engine.runtime.storage", "engine.runtime.health")
        storage.init_db()

        snapshot = health.get_startup_validation_snapshot(
            health={
                "db": {"ok": True, "initialized": True, "db_path": os.environ["DB_PATH"]},
                "ingestion_runtime": {"running": True, "stale": False, "last_publish_ts_ms": 123},
                "ingestion_freshness": {
                    "critical_ok": True,
                    "stale_critical_sources": [],
                    "runtime_reason_codes": [],
                },
                "job_summary": {"ok": True, "required_missing": [], "required_stale": []},
                "predictions": {
                    "ok": True,
                    "detail": "ok",
                    "count": 5,
                    "recent_count": 2,
                    "history_count": 8,
                    "history_recent_count": 2,
                    "last_ts_ms": 123,
                    "age_s": 1.0,
                    "max_age_s": 600.0,
                },
                "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": False},
                "execution_supervisor": {"state": "ok"},
                "broker_connection": {"ok": False, "state": "disconnected", "broker": "sim"},
            },
            db_validation={
                "ok": True,
                "quick_check": "ok",
                "missing_tables": [],
                "schema_version": 1,
                "schema_status": "applied",
            },
        )

        self.assertTrue(snapshot["ok"])
        self.assertEqual(snapshot["blocking_gates"], [])
        self.assertEqual(snapshot["blocking_checks"], [])
        self.assertEqual(snapshot["critical_systems_missing"], [])
        self.assertTrue(bool(snapshot["gates"]["config_valid"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["database_reachable"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["schema_valid"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["log_path_writable"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["required_directories_present"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["disk_headroom_available"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["ui_static_assets_present"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["no_port_binding_conflict"]["ok"]))

    def test_startup_validation_blocks_critical_disk_pressure(self) -> None:
        storage, health = _reload_modules("engine.runtime.storage", "engine.runtime.health")
        storage.init_db()

        snapshot = health.get_startup_validation_snapshot(
            health={
                "db": {"ok": True, "initialized": True, "db_path": os.environ["DB_PATH"]},
                "disk_pressure": {
                    "ok": False,
                    "status": "critical",
                    "critical": ["root:disk_critical:free_bytes=1024:free_pct=0.01"],
                    "warnings": [],
                    "paths": [],
                },
                "ingestion_runtime": {"running": True, "stale": False, "last_publish_ts_ms": 123},
                "ingestion_freshness": {
                    "critical_ok": True,
                    "stale_critical_sources": [],
                    "runtime_reason_codes": [],
                },
                "job_summary": {"ok": True, "required_missing": [], "required_stale": []},
                "predictions": {
                    "ok": True,
                    "detail": "ok",
                    "count": 5,
                    "recent_count": 2,
                    "history_count": 8,
                    "history_recent_count": 2,
                    "last_ts_ms": 123,
                    "age_s": 1.0,
                    "max_age_s": 600.0,
                },
                "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": False},
                "execution_supervisor": {"state": "ok"},
                "broker_connection": {"ok": False, "state": "disconnected", "broker": "sim"},
            },
            db_validation={
                "ok": True,
                "quick_check": "ok",
                "missing_tables": [],
                "schema_version": 1,
                "schema_status": "applied",
            },
        )

        self.assertFalse(snapshot["ok"])
        self.assertIn("disk_headroom_available", snapshot["blocking_gates"])
        self.assertIn("filesystem", snapshot["critical_systems_missing"])
        self.assertIn("disk_pressure_critical", snapshot["gates"]["disk_headroom_available"]["detail"])

    def test_startup_validation_accepts_structural_db_validation_when_quick_check_skipped_or_not_applicable(self) -> None:
        health, startup_gates = _reload_modules("engine.runtime.health", "engine.runtime.startup_gates")

        health_payload = {
            "db": {"ok": True, "initialized": True, "db_path": os.environ["DB_PATH"]},
            "ingestion_runtime": {"running": True, "stale": False, "last_publish_ts_ms": 123},
            "ingestion_freshness": {
                "critical_ok": True,
                "stale_critical_sources": [],
                "runtime_reason_codes": [],
            },
            "job_summary": {"ok": True, "required_missing": [], "required_stale": []},
            "predictions": {
                "ok": True,
                "detail": "ok",
                "count": 5,
                "recent_count": 2,
                "history_count": 8,
                "history_recent_count": 2,
                "last_ts_ms": 123,
                "age_s": 1.0,
                "max_age_s": 600.0,
            },
            "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": False},
            "execution_supervisor": {"state": "ok"},
            "broker_connection": {"ok": False, "state": "disconnected", "broker": "sim"},
        }

        for quick_check, explicit_skip in [("skipped", True), ("not_applicable", False)]:
            with self.subTest(quick_check=quick_check):
                with patch.object(startup_gates, "_db_reachable", return_value={"ok": True, "detail": "ok"}):
                    with patch.object(startup_gates, "_json_meta_get", return_value={}):
                        with patch.object(startup_gates, "meta_get", side_effect=lambda _key, default="": default):
                            with patch.object(startup_gates, "default_pg_dsn", return_value="postgresql://unit-test"):
                                snapshot = health.get_startup_validation_snapshot(
                                    health=health_payload,
                                    db_validation={
                                        "ok": True,
                                        "quick_check": quick_check,
                                        "quick_check_skipped": explicit_skip,
                                        "missing_tables": [],
                                        "missing_columns": {},
                                        "missing_indexes": [],
                                        "schema_version": 1,
                                        "expected_schema_version": 1,
                                        "schema_version_ok": True,
                                        "schema_status": "applied",
                                    },
                                )

                self.assertTrue(snapshot["ok"])
                self.assertEqual(snapshot["blocking_gates"], [])
                self.assertTrue(bool(snapshot["gates"]["schema_valid"]["ok"]))
                self.assertTrue(bool(snapshot["gates"]["schema_valid"]["quick_check_skipped"]))

    def test_health_wal_path_handles_directory_style_db_path(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")

        self.assertEqual(health._sqlite_wal_path(Path("data")), Path("data-wal"))
        self.assertEqual(health._sqlite_wal_path(Path("trading.db")), Path("trading.db-wal"))

    def test_db_validation_tracks_price_provider_health_contract_and_indexes(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        validation = storage.get_db_validation_snapshot(include_quick_check=False)
        self.assertIn("price_provider_health", list(validation.get("required_tables") or []))
        self.assertEqual(
            list((validation.get("required_columns") or {}).get("price_provider_health") or []),
            [
                "ts_ms",
                "provider",
                "ok",
                "latency_ms",
                "n_symbols",
                "error",
                "last_success_ts_ms",
                "error_count",
            ],
        )
        self.assertIn("idx_price_provider_health_ts", list(validation.get("required_indexes") or []))
        self.assertIn("idx_price_provider_health_provider", list(validation.get("required_indexes") or []))
        self.assertNotIn("idx_price_provider_health_ts", list(validation.get("missing_indexes") or []))

        con = storage.connect_rw_direct()
        try:
            con.execute("DROP INDEX IF EXISTS idx_price_provider_health_ts")
            con.commit()
        finally:
            con.close()

        degraded = storage.get_db_validation_snapshot(include_quick_check=False)
        self.assertFalse(bool(degraded.get("ok")))
        self.assertIn("idx_price_provider_health_ts", list(degraded.get("missing_indexes") or []))

    def test_db_validation_tracks_live_ingestion_coordination_tables(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        validation = storage.get_db_validation_snapshot(include_quick_check=False)
        required_tables = list(validation.get("required_tables") or [])
        required_columns = dict(validation.get("required_columns") or {})

        self.assertIn("price_feed_lock", required_tables)
        self.assertEqual(
            list(required_columns.get("price_feed_lock") or []),
            ["id", "owner", "pid", "ts_ms"],
        )
        self.assertNotIn("price_feed_lock", list(validation.get("missing_tables") or []))
        self.assertIn("options_symbol_ingestion_state", required_tables)
        self.assertEqual(
            list(required_columns.get("options_symbol_ingestion_state") or []),
            [
                "symbol",
                "provider",
                "consecutive_failures",
                "total_failures",
                "last_failure_ts_ms",
                "last_failure_error",
                "last_success_ts_ms",
                "last_fresh_snapshot_ts_ms",
                "last_cached_snapshot_ts_ms",
                "last_fallback_ts_ms",
                "last_row_count",
                "disabled_until_ts_ms",
                "updated_ts_ms",
            ],
        )
        self.assertNotIn("options_symbol_ingestion_state", list(validation.get("missing_tables") or []))

    def test_runtime_startup_gates_ignore_stale_dashboard_bound_without_listener(self) -> None:
        storage, startup_gates = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.startup_gates",
        )
        storage.init_db()

        with patch.object(startup_gates, "_json_meta_get", return_value={}):
            with patch.object(
                startup_gates,
                "meta_get",
                side_effect=lambda key, default="": "123" if str(key) == "dashboard_bound_ts_ms" else default,
            ):
                snapshot = startup_gates.evaluate_runtime_startup_gates(
                    repo_root=REPO_ROOT,
                    health={
                        "db": {"ok": True, "initialized": True, "db_path": os.environ["DB_PATH"]},
                        "lifecycle": {"dashboard_bound_ts_ms": "123"},
                    },
                    db_validation={
                        "ok": True,
                        "quick_check": "ok",
                        "missing_tables": [],
                        "schema_version": 1,
                        "schema_status": "applied",
                    },
                )

        self.assertTrue(bool(snapshot["ok"]))
        self.assertTrue(bool(snapshot["gates"]["core_services_initialized"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["required_api_dependencies_available"]["ok"]))
        self.assertTrue(bool(snapshot["gates"]["no_port_binding_conflict"]["ok"]))
        self.assertNotEqual(
            str(snapshot["gates"]["no_port_binding_conflict"]["detail"] or ""),
            "dashboard_listener_bound",
        )

    def test_prebind_startup_gates_fail_on_live_port_conflict(self) -> None:
        (startup_gates,) = _reload_modules("engine.runtime.startup_gates")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            sock.bind(("127.0.0.1", 0))
            sock.listen(1)
            conflict_port = int(sock.getsockname()[1])

            with patch.dict(
                os.environ,
                {"DASHBOARD_HOST": "127.0.0.1", "DASHBOARD_PORT": str(conflict_port)},
                clear=False,
            ):
                snapshot = startup_gates.evaluate_prebind_startup_gates(repo_root=REPO_ROOT)

        self.assertFalse(bool(snapshot["ok"]))
        self.assertIn("no_port_binding_conflict", list(snapshot.get("blocking_gates") or []))
        self.assertFalse(bool(snapshot["gates"]["no_port_binding_conflict"]["ok"]))
        self.assertIn(
            "port",
            str(snapshot["gates"]["no_port_binding_conflict"]["detail"] or "").lower(),
        )

    def test_startup_config_accepts_dashboard_token_file_for_remote_bind(self) -> None:
        (startup_gates,) = _reload_modules("engine.runtime.startup_gates")
        token_file = Path(self.tmp.name) / "dashboard_api_token"
        token_file.write_text("production-token-1234567890\n", encoding="utf-8")

        with patch.dict(
            os.environ,
            {
                "DASHBOARD_HOST": "0.0.0.0",
                "DASHBOARD_API_TOKEN": "",
                "DASHBOARD_API_TOKEN_FILE": str(token_file),
            },
            clear=False,
        ):
            snapshot = startup_gates.get_startup_config_snapshot(REPO_ROOT)

        self.assertTrue(bool(snapshot["ok"]))
        self.assertTrue(bool(snapshot["parsed"]["dashboard_api_token_present"]))
        self.assertNotIn(
            "DASHBOARD_API_TOKEN",
            {str(item.get("key") or "") for item in list(snapshot.get("errors") or [])},
        )

    def test_health_snapshot_exposes_component_observability_sections(self) -> None:
        storage, health, observability = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.runtime.observability",
        )
        storage.init_db()
        observability.record_component_health("inference", ok=True, status="ok", detail="ok")
        observability.record_component_health("execution", ok=False, status="all_brokers_failed", detail="router_failed")

        snapshot = health.get_health_snapshot()

        self.assertIn("component_health", snapshot)
        self.assertTrue(bool(snapshot["inference_runtime"]["ok"]))
        self.assertEqual(str(snapshot["inference_runtime"]["status"] or ""), "ok")
        self.assertFalse(bool(snapshot["execution_runtime"]["ok"]))
        self.assertEqual(str(snapshot["execution_runtime"]["status"] or ""), "all_brokers_failed")

    def test_health_snapshot_exposes_data_pipeline_gates_and_diagnostics(self) -> None:
        storage, health, data_quality, ingestion_status = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.runtime.data_quality",
            "engine.runtime.ingestion_status",
        )
        storage.init_db()
        now_ms = int(time.time() * 1000)
        ingestion_status.record_pipeline_status(
            "poll_prices",
            ok=True,
            raw_rows=5,
            event_rows=5,
            last_ingested_ts_ms=now_ms,
            meta={"source_connected": True},
        )
        data_quality.record_feature_validation(
            {
                "ok": True,
                "status": "ok",
                "detail": "ok",
                "symbol": "AAPL",
                "validated_ts_ms": now_ms,
                "feature_ts_ms": now_ms,
                "feature_set_tag": "unit_test_feature_set",
                "schema_version": 1,
                "point_count": 64,
                "feature_count": 12,
                "vector_size": 12,
                "reason_codes": [],
            }
        )
        data_quality.record_model_input_validation(
            {
                "ok": True,
                "status": "ok",
                "detail": "ok",
                "symbol": "AAPL",
                "validated_ts_ms": now_ms,
                "model_name": "rt_linear",
                "model_version": "v1",
                "model_kind": "linear",
                "feature_ts_ms": now_ms,
                "feature_set_tag": "unit_test_feature_set",
                "expected_feature_count": 12,
                "actual_feature_count": 12,
                "feature_coverage": 1.0,
                "missing_feature_ids": [],
                "schema_mismatch": False,
                "shape_valid": True,
                "stale": False,
                "reason_codes": [],
            }
        )
        data_quality.record_scoring_pipeline(
            {
                "ok": True,
                "status": "ok",
                "detail": "ok",
                "symbol": "AAPL",
                "attempt_ts_ms": now_ms,
                "model_name": "rt_linear",
                "model_version": "v1",
                "model_kind": "linear",
                "model_loaded": True,
                "prediction": 0.42,
                "confidence": 0.88,
                "feature_ts_ms": now_ms,
                "prediction_ts_ms": now_ms,
                "safe_output": False,
                "fallback_reason": "",
                "config_variant": "rt_linear",
                "reason_codes": [],
                "invalid_input_delta": 0,
            }
        )

        snapshot = health.get_health_snapshot()

        self.assertEqual(str(snapshot["feature_validation"]["symbol"] or ""), "AAPL")
        self.assertEqual(str(snapshot["model_input_validation"]["model_name"] or ""), "rt_linear")
        self.assertTrue(bool(snapshot["scoring_pipeline"]["model_loaded"]))
        self.assertTrue(bool(snapshot["feature_runtime"]["ok"]))
        self.assertTrue(bool(snapshot["model_input_runtime"]["ok"]))
        self.assertTrue(bool(snapshot["scoring_runtime"]["ok"]))

        gates = dict(snapshot["data_pipeline_gates"]["gates"])
        expected_gates = {
            "ingestion_active",
            "ingestion_not_stale",
            "critical_features_valid",
            "model_inputs_valid",
            "scoring_pipeline_operational",
        }
        self.assertTrue(expected_gates.issubset(set(gates.keys())))
        self.assertTrue(all(str((gates[name] or {}).get("criteria") or "").strip() for name in expected_gates))

    def test_health_snapshot_warms_model_cache_for_standalone_validation(self) -> None:
        storage, model_registry, model_cache = _reload_modules(
            "engine.runtime.storage",
            "engine.model_registry",
            "engine.runtime.model_cache",
        )
        storage.init_db()
        model_registry.register_model(
            model_name="gbm_regressor",
            model_kind="regressor",
            symbol="AAPL",
            version="v1",
            performance_metrics={"score": 0.91},
            metadata={"source": "unit-test"},
            is_active=True,
        )
        model_cache.invalidate_model_catalog()
        (health,) = _reload_modules("engine.runtime.health")

        snapshot = health.get_health_snapshot()

        self.assertTrue(bool(snapshot["model_cache"]["ok"]))
        self.assertTrue(bool(snapshot["model_cache"]["loaded"]))
        self.assertGreaterEqual(int(snapshot["model_cache"]["rows"] or 0), 1)
        self.assertTrue(bool(snapshot["startup_validation"]["checks"]["core_services_initialized"]["ok"]))

    def test_health_snapshot_warms_model_cache_readonly(self) -> None:
        storage, health, model_cache = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.runtime.model_cache",
        )
        storage.init_db()
        model_cache.invalidate_model_catalog()

        warm_calls: list[dict[str, object]] = []

        def _capture_warm(*args, **kwargs):
            warm_calls.append(dict(kwargs))
            return {
                "ok": True,
                "loaded": True,
                "rows": 0,
                "last_error": "",
                "ts_ms": int(time.time() * 1000),
            }

        with patch.object(model_cache, "warm_model_catalog", side_effect=_capture_warm):
            snapshot = health.get_health_snapshot()

        self.assertIn("model_cache", snapshot)
        self.assertEqual(len(warm_calls), 1)
        self.assertTrue(bool(warm_calls[0].get("readonly")))

    def test_health_snapshot_uses_readonly_execution_supervisor_snapshot(self) -> None:
        storage, health, execution_quality_supervisor = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.execution.execution_quality_supervisor",
        )
        storage.init_db()

        readonly_calls: list[dict[str, object]] = []

        def _capture_snapshot(*args, **kwargs):
            readonly_calls.append(dict(kwargs))
            return {
                "ok": False,
                "state": "unknown",
                "score": 0.0,
                "alerts": [],
                "detail": "unit_test_readonly",
            }

        with patch.object(
            execution_quality_supervisor,
            "refresh_execution_quality_supervisor",
            side_effect=AssertionError("health snapshot must not refresh execution supervisor state"),
        ):
            with patch.object(
                execution_quality_supervisor,
                "get_execution_quality_snapshot",
                side_effect=_capture_snapshot,
            ):
                snapshot = health.get_health_snapshot()

        self.assertIn("execution_supervisor", snapshot)
        self.assertEqual(len(readonly_calls), 1)
        self.assertTrue(bool(readonly_calls[0].get("readonly")))

    def test_execution_barrier_refresh_reads_kill_switch_snapshot_without_schema_writes(self) -> None:
        storage, health, gates, kill_switch = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.runtime.gates",
            "engine.execution.kill_switch",
        )
        storage.init_db()

        con = storage.connect()
        try:
            now_ms = int(time.time() * 1000)
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS kill_switch_state (
                  scope TEXT NOT NULL,
                  key TEXT NOT NULL,
                  enabled INTEGER NOT NULL DEFAULT 0,
                  reason TEXT,
                  actor TEXT NOT NULL DEFAULT 'system',
                  meta_json TEXT,
                  created_ts_ms INTEGER NOT NULL,
                  updated_ts_ms INTEGER NOT NULL,
                  PRIMARY KEY (scope, key)
                );
                """
            )
            con.execute(
                """
                INSERT OR REPLACE INTO kill_switch_state(
                  scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ("global", "global", 1, "unit_test", "tester", '{"source":"unit"}', now_ms, now_ms),
            )
            con.commit()
        finally:
            con.close()

        gate_calls: list[dict[str, object]] = []

        def _capture_gate(**kwargs):
            gate_calls.append(dict(kwargs))
            return {
                "ok": True,
                "allowed": False,
                "mode": "safe",
                "reason": "unit_test_barrier",
            }

        with patch.object(
            kill_switch,
            "snapshot",
            side_effect=AssertionError("health barrier refresh must not invoke kill_switch.snapshot()"),
        ):
            with patch.object(gates, "execution_gate_snapshot", side_effect=_capture_gate):
                ro_con = storage.connect_ro_direct(timeout_s=1.0, busy_timeout_ms=250)
                try:
                    barrier = health._refresh_execution_barrier_snapshot_with_con(
                        {
                            "active": True,
                            "severity": "CRITICAL",
                            "reason": "unit_test",
                            "reason_codes": ["unit_test"],
                        },
                        con=ro_con,
                    )
                finally:
                    ro_con.close()

        self.assertTrue(gate_calls)
        self.assertEqual(str(barrier.get("reason") or ""), "unit_test_barrier")
        self.assertEqual(int(((gate_calls[0].get("kill_switches") or {}).get("state") or [{}])[0].get("enabled") or 0), 1)
        self.assertEqual(str(((gate_calls[0].get("kill_switches") or {}).get("state") or [{}])[0].get("reason") or ""), "unit_test")

    def test_shared_ingestion_runtime_snapshot_prefers_fresh_prices_table_timestamp(self) -> None:
        storage, health = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
        )
        storage.init_db()

        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO prices(ts_ms, symbol, price, px, source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (4_000, "SPY", 500.25, 500.25, "unit"),
            )
            con.commit()
        finally:
            con.close()

        meta_state = {
            "running": True,
            "last_event_ts_ms": 1_000,
            "market_state": {
                "last_price_ts_ms": 1_000,
                "price_age_ms": 4_000,
            },
        }

        with patch.object(health, "_json_meta_get", return_value=meta_state):
            with patch.object(
                health,
                "market_data_status",
                return_value={
                    "running": True,
                    "last_price_ts_ms": 1_000,
                    "price_age_ms": 4_000,
                    "healthy_providers": 0,
                    "providers": {},
                },
            ):
                ro_con = storage.connect_ro_direct(timeout_s=1.0, busy_timeout_ms=250)
                try:
                    snapshot = health._shared_ingestion_runtime_snapshot(
                        ro_con,
                        now_ms=5_000,
                        effective_prices_max_age_s=2.5,
                    )
                finally:
                    ro_con.close()

        self.assertEqual(int(snapshot["last_tick_ts_ms"] or 0), 4_000)
        self.assertEqual(int(snapshot["price_age_ms"] or 0), 1_000)
        self.assertFalse(bool(snapshot["stale"]))

    def test_shared_ingestion_runtime_fixed_after_provider_and_heartbeat_query_failures_uses_prices_table_and_fail_closed_provider_state(self) -> None:
        storage, health = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
        )
        storage.init_db()

        con = storage.connect()
        try:
            con.execute(
                """
                INSERT INTO prices(ts_ms, symbol, price, px, source)
                VALUES (?, ?, ?, ?, ?)
                """,
                (4_000, "SPY", 500.25, 500.25, "unit"),
            )
            con.commit()
        finally:
            con.close()

        class _FailingSharedIngestionReads:
            def __init__(self, inner) -> None:
                self._inner = inner

            def execute(self, sql, params=()):
                stmt = " ".join(str(sql).split())
                if "FROM job_heartbeats" in stmt:
                    raise sqlite3.OperationalError("database is locked")
                if "FROM price_provider_health" in stmt:
                    raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
                return self._inner.execute(sql, params)

        meta_state = {
            "running": False,
            "last_event_ts_ms": 1_000,
            "market_state": {
                "last_price_ts_ms": 1_000,
                "price_age_ms": 4_000,
                "healthy_providers": 0,
                "providers": {},
            },
        }

        with patch.object(health, "_json_meta_get", return_value=meta_state):
            with patch.object(
                health,
                "market_data_status",
                return_value={
                    "running": False,
                    "last_price_ts_ms": 1_000,
                    "price_age_ms": 4_000,
                    "healthy_providers": 0,
                    "providers": {},
                },
            ):
                ro_con = storage.connect_ro_direct(timeout_s=1.0, busy_timeout_ms=250)
                try:
                    snapshot = health._shared_ingestion_runtime_snapshot(
                        _FailingSharedIngestionReads(ro_con),
                        now_ms=5_000,
                        effective_prices_max_age_s=2.5,
                    )
                finally:
                    ro_con.close()

        self.assertEqual(
            int(snapshot["last_tick_ts_ms"] or 0),
            4_000,
            "fixed_after_locked_health_reads should still derive the latest tick from prices when job_heartbeats and price_provider_health reads fail",
        )
        self.assertEqual(int(snapshot["price_age_ms"] or 0), 1_000)
        self.assertFalse(bool(snapshot["stale"]))
        self.assertFalse(
            bool(snapshot["running"]),
            "failing_before_job_heartbeat_query_relief would have fabricated a running ingestion_runtime from a failed heartbeat read",
        )
        self.assertEqual(
            int(snapshot["healthy_providers"] or 0),
            0,
            "failing_before_provider_health_query_relief would have left stale provider health behind instead of failing closed to zero healthy providers",
        )
        self.assertEqual(
            dict(snapshot.get("providers") or {}),
            {},
            "fixed_after_provider_health_query_failure should leave provider state empty when price_provider_health cannot be read",
        )
        self.assertEqual(int(snapshot["last_publish_ts_ms"] or 0), 0)

    def test_health_snapshot_surfaces_execution_supervisor_failed_gates(self) -> None:
        storage, health, execution_quality_supervisor = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.execution.execution_quality_supervisor",
        )
        storage.init_db()
        health._HEALTH_SNAPSHOT_CACHE["ts_ms"] = 0
        health._HEALTH_SNAPSHOT_CACHE["payload"] = None

        with patch.object(
            execution_quality_supervisor,
            "get_execution_quality_snapshot",
            return_value={
                "ok": True,
                "state": "critical",
                "score": 6.0,
                "failed_gates": ["order_state_consistent", "pnl_calculation_valid"],
                "alerts": [
                    {"alert_type": "duplicate_order_risk_detected"},
                    {"alert_type": "pricing_unavailable_for_unrealized_pnl"},
                ],
                "integrity": {"pricing_unavailable_count": 1},
                "account_state": {"ok": False, "detail": "invalid_account_balance_state"},
            },
        ):
            snapshot = health.get_health_snapshot()

        self.assertFalse(bool(snapshot["ok"]))
        self.assertIn("execution_gate:order_state_consistent", list(snapshot.get("reasons") or []))
        self.assertIn("execution_gate:pnl_calculation_valid", list(snapshot.get("reasons") or []))
        self.assertIn("duplicate_order_risk_detected", list(snapshot.get("reasons") or []))
        self.assertIn("pricing_unavailable_for_unrealized_pnl", list(snapshot.get("reasons") or []))
        self.assertIn("execution_supervisor_critical", list(snapshot.get("reasons") or []))

    def test_health_snapshot_uses_readonly_broker_connection_snapshot(self) -> None:
        storage, health, execution_broker_watchdog = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.execution.execution_broker_watchdog",
        )
        storage.init_db()

        readonly_calls: list[dict[str, object]] = []

        def _capture_snapshot(*args, **kwargs):
            readonly_calls.append(dict(kwargs))
            return {
                "ok": False,
                "state": "unknown",
                "broker": "sim",
                "detail": "unit_test_readonly",
            }

        with patch.object(
            execution_broker_watchdog,
            "refresh_broker_connection_health",
            side_effect=AssertionError("health snapshot must not refresh broker connection state"),
        ):
            with patch.object(
                execution_broker_watchdog,
                "get_broker_connection_health",
                side_effect=_capture_snapshot,
            ):
                snapshot = health.get_health_snapshot()

        self.assertIn("broker_connection", snapshot)
        self.assertEqual(len(readonly_calls), 1)
        self.assertTrue(bool(readonly_calls[0].get("readonly")))

    def test_health_snapshot_reports_model_serving_and_alert_lifecycle_summaries(self) -> None:
        prev_model_min_sample = os.environ.get("HEALTH_MODEL_SERVING_MIN_SAMPLE")
        prev_model_max_rate = os.environ.get("HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE")
        prev_alert_window = os.environ.get("HEALTH_ALERT_LIFECYCLE_WINDOW_S")
        os.environ["HEALTH_MODEL_SERVING_MIN_SAMPLE"] = "1"
        os.environ["HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE"] = "0.25"
        os.environ["HEALTH_ALERT_LIFECYCLE_WINDOW_S"] = "86400"
        try:
            storage, health = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.health",
            )
            storage.init_db()
            now_ms = int(time.time() * 1000)
            con = storage.connect()
            try:
                con.execute(
                    """
                    INSERT INTO tracked_predictions(
                      ts_ms, symbol, model_name, model_version, prediction, confidence,
                      features_version, metadata_json
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(now_ms - 2_000),
                        "SPY",
                        "embed_regressor.live",
                        "v1",
                        0.5,
                        0.7,
                        "price_feature_store_v1",
                        '{"requested_model_name":"temporal_predictor.live","resolved_model_name":"embed_regressor.live","requested_model_family":"temporal_predictor","served_model_family":"embed_regressor","serve_fallback_active":true,"fallback_reason":"resolved_to_registry"}',
                    ),
                )
                con.execute(
                    """
                    INSERT INTO tracked_predictions(
                      ts_ms, symbol, model_name, model_version, prediction, confidence,
                      features_version, metadata_json
                    ) VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(now_ms - 1_000),
                        "QQQ",
                        "embed_regressor.live",
                        "v1",
                        0.2,
                        0.8,
                        "price_feature_store_v1",
                        '{"requested_model_name":"embed_regressor.live","resolved_model_name":"embed_regressor.live","requested_model_family":"embed_regressor","served_model_family":"embed_regressor","serve_fallback_active":false}',
                    ),
                )
                for offset_ms, symbol, dedupe_key, status, first_seen, last_seen, consumed_ts, expired_ts in (
                    (1_000, "AAPL", "seen-alert", "seen", now_ms - 900, now_ms - 500, 0, 0),
                    (2_000, "MSFT", "consumed-alert", "consumed", now_ms - 1_900, now_ms - 1_500, now_ms - 1_000, 0),
                    (3_000, "NVDA", "expired-alert", "expired", now_ms - 2_900, now_ms - 2_400, 0, now_ms - 100),
                ):
                    con.execute(
                        """
                        INSERT INTO alerts(
                          ts_ms, event_title, symbol, horizon_s, expected_z, confidence, severity,
                          rule_id, explain_json, dedupe_key, portfolio_first_seen_ts_ms,
                          portfolio_last_seen_ts_ms, portfolio_consumed_ts_ms, portfolio_expired_ts_ms,
                          portfolio_status
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(now_ms - offset_ms),
                            f"alert:{symbol}",
                            str(symbol),
                            300,
                            1.0,
                            0.8,
                            "medium",
                            "unit_test",
                            "{}",
                            str(dedupe_key),
                            int(first_seen),
                            int(last_seen),
                            int(consumed_ts),
                            int(expired_ts),
                            str(status),
                        ),
                    )
                con.commit()
            finally:
                con.close()

            snapshot = health.get_health_snapshot()
        finally:
            if prev_model_min_sample is None:
                os.environ.pop("HEALTH_MODEL_SERVING_MIN_SAMPLE", None)
            else:
                os.environ["HEALTH_MODEL_SERVING_MIN_SAMPLE"] = prev_model_min_sample
            if prev_model_max_rate is None:
                os.environ.pop("HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE", None)
            else:
                os.environ["HEALTH_MODEL_SERVING_MAX_FALLBACK_RATE"] = prev_model_max_rate
            if prev_alert_window is None:
                os.environ.pop("HEALTH_ALERT_LIFECYCLE_WINDOW_S", None)
            else:
                os.environ["HEALTH_ALERT_LIFECYCLE_WINDOW_S"] = prev_alert_window

        self.assertIn("model_serving", snapshot)
        self.assertEqual(int(snapshot["model_serving"]["sample_count"] or 0), 2)
        self.assertEqual(int(snapshot["model_serving"]["fallback_count"] or 0), 1)
        self.assertAlmostEqual(float(snapshot["model_serving"]["fallback_rate"] or 0.0), 0.5, places=6)
        self.assertTrue(bool(snapshot["model_serving"]["degraded"]))
        self.assertEqual(str(snapshot["model_serving"]["top_fallback_reasons"][0]["reason"]), "resolved_to_registry")

        self.assertIn("alert_lifecycle", snapshot)
        self.assertEqual(int(snapshot["alert_lifecycle"]["recent_alerts"] or 0), 3)
        self.assertEqual(int(snapshot["alert_lifecycle"]["seen_count"] or 0), 1)
        self.assertEqual(int(snapshot["alert_lifecycle"]["consumed_count"] or 0), 1)
        self.assertEqual(int(snapshot["alert_lifecycle"]["expired_unconsumed_count"] or 0), 1)
        self.assertTrue(bool(snapshot["alert_lifecycle"]["warning"]))

    def test_health_snapshot_surfaces_timeseries_portfolio_and_execution_degradation(self) -> None:
        storage, health = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
        )
        storage.init_db()
        health._HEALTH_SNAPSHOT_CACHE["ts_ms"] = 0
        health._HEALTH_SNAPSHOT_CACHE["payload"] = None

        with patch.object(
            health,
            "get_timeseries_storage_snapshot",
            return_value={
                "ok": False,
                "enabled": True,
                "detail": "timeseries_storage_not_ready",
                "degraded_reasons": ["timescale_flush_failures"],
                "feature_store": {
                    "ok": False,
                    "enabled": True,
                    "degraded": True,
                    "degraded_reasons": ["feature_store_queue_backpressure_active"],
                },
            },
        ):
            with patch.object(
                health,
                "_portfolio_runtime_snapshot",
                return_value={
                    "ok": False,
                    "available": True,
                    "degraded": True,
                    "detail": "portfolio_runtime_degraded",
                    "degraded_reasons": [{"code": "PORTFOLIO_RISK_GATE_FAILED"}],
                    "degraded_codes": ["PORTFOLIO_RISK_GATE_FAILED"],
                },
            ):
                with patch.object(
                    health,
                    "_execution_degraded_snapshot",
                    return_value={
                        "active": True,
                        "severity": "CRITICAL",
                        "reason": "event_bus_critical_backpressure",
                        "reason_codes": ["event_bus_critical_backpressure"],
                        "sources": [{"source": "event_bus"}],
                    },
                ):
                    with patch.object(
                        health,
                        "_refresh_execution_barrier_snapshot",
                        return_value={
                            "ok": True,
                            "allowed": False,
                            "reason": "event_bus_critical_backpressure",
                            "mode": "live",
                        },
                    ):
                        snapshot = health.get_health_snapshot()

        self.assertIn("timeseries_storage", snapshot)
        self.assertIn("portfolio_runtime", snapshot)
        self.assertIn("execution_degraded", snapshot)
        self.assertFalse(bool(snapshot["timeseries_storage"]["ok"]))
        self.assertTrue(bool(snapshot["portfolio_runtime"]["degraded"]))
        self.assertTrue(bool(snapshot["execution_degraded"]["active"]))
        self.assertEqual(str(snapshot["execution_barrier"]["reason"] or ""), "event_bus_critical_backpressure")

    def test_health_snapshot_surfaces_attribution_orphans_and_position_reconcile_failure(self) -> None:
        storage, health, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        health._HEALTH_SNAPSHOT_CACHE["ts_ms"] = 0
        health._HEALTH_SNAPSHOT_CACHE["payload"] = None

        now_ms = int(time.time() * 1000)
        con = storage.connect()
        try:
            con.executescript(execution_ledger.SCHEMA)
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS position_reconcile_audit (
                    ts_ms INTEGER PRIMARY KEY,
                    broker TEXT NOT NULL,
                    ok INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    mismatched_n INTEGER NOT NULL DEFAULT 0,
                    max_abs_qty_diff REAL NOT NULL DEFAULT 0,
                    total_abs_qty_diff REAL NOT NULL DEFAULT 0,
                    detail_json TEXT
                )
                """
            )
            con.execute(
                """
                INSERT INTO pnl_attribution(
                  ts_ms, source_alert_id, prediction_id, model_id, model_version, symbol, pnl, fees,
                  slippage_bps, position_size, avg_price, realized_pnl, unrealized_pnl, extra_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    5001,
                    7001,
                    "m1",
                    "v1",
                    "AAPL",
                    5.0,
                    1.0,
                    0.0,
                    0.0,
                    None,
                    2.0,
                    4.0,
                    json.dumps({"slippage_cost": 1.0}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO position_reconcile_audit(
                  ts_ms, broker, ok, status, mismatched_n, max_abs_qty_diff, total_abs_qty_diff, detail_json
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "paper",
                    0,
                    "mismatch",
                    2,
                    3.0,
                    4.5,
                    json.dumps({"fatal_reconcile": True, "mismatched": ["AAPL"]}, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()
        finally:
            con.close()

        snapshot = health.get_health_snapshot()

        attribution = dict(snapshot.get("attribution") or {})
        orphan_snapshot = dict(attribution.get("orphans") or {})
        position_reconcile = dict(snapshot.get("position_reconcile") or {})

        self.assertFalse(bool(attribution.get("ok")))
        self.assertEqual(int(orphan_snapshot.get("orphan_row_count") or 0), 1)
        self.assertEqual(str(orphan_snapshot.get("detail") or ""), "orphan_pnl_rows_detected")
        self.assertIn("pnl_attribution_orphans_detected", list(snapshot.get("reasons") or []))
        self.assertTrue(bool(position_reconcile.get("available")))
        self.assertTrue(bool(position_reconcile.get("fatal_reconcile")))
        self.assertEqual(str(position_reconcile.get("status") or ""), "mismatch")
        self.assertEqual(int(position_reconcile.get("mismatched_n") or 0), 2)

    def test_readiness_snapshot_blocks_timeseries_and_portfolio_runtime_degradation(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        os.environ["ENGINE_MODE"] = "live"

        snapshot = health.get_readiness_snapshot(
            health={
                "prices": {"ok": True, "last_ts_ms": 123, "age_s": 1, "max_age_s": 60},
                "providers": {"ok": True, "healthy": 1, "total": 1},
                "labels": {"ok": True, "count": 10},
                "model": {"ok": True, "support_n": 10},
                "execution_barrier": {"allowed": False, "reason": "event_bus_critical_backpressure"},
                "broker_connection": {"ok": True, "state": "connected", "broker": "sim"},
                "db": {"ok": True, "initialized": True, "exists": True, "db_path": os.environ["DB_PATH"]},
                "job_summary": {"ok": True, "total": 1, "stale": 0, "stale_jobs": []},
                "timeseries_storage": {"ok": False, "enabled": True, "detail": "timeseries_storage_not_ready"},
                "feature_store": {"ok": False, "enabled": True, "degraded_reasons": ["feature_store_queue_backpressure_active"]},
                "portfolio_runtime": {
                    "degraded": True,
                    "detail": "portfolio_runtime_degraded",
                    "degraded_codes": ["PORTFOLIO_RISK_GATE_FAILED"],
                },
                "execution_supervisor": {"ok": True, "state": "ok", "failed_gates": []},
                "execution_degraded": {
                    "active": True,
                    "severity": "CRITICAL",
                    "reason": "event_bus_critical_backpressure",
                    "reason_codes": ["event_bus_critical_backpressure"],
                },
            },
            preflight={"ok": True},
            system_state={"state": "LIVE", "mode": "live"},
            graph={"ok": True},
        )

        issue_codes = {str(item.get("code") or "") for item in list(snapshot.get("issues") or [])}

        self.assertFalse(bool(snapshot["ok"]))
        self.assertFalse(bool(snapshot["timeseries_ok"]))
        self.assertFalse(bool(snapshot["portfolio_runtime_ok"]))
        self.assertIn("timeseries_storage_not_ready", issue_codes)
        self.assertIn("portfolio_runtime_degraded", issue_codes)
        self.assertIn("execution_degraded", issue_codes)
        self.assertIn("timeseries_storage", list(snapshot.get("waiting_on") or []))
        self.assertIn("portfolio_runtime", list(snapshot.get("waiting_on") or []))

    def test_readiness_snapshot_blocks_live_position_reconcile_failure(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        os.environ["ENGINE_MODE"] = "live"

        snapshot = health.get_readiness_snapshot(
            health={
                "prices": {"ok": True, "last_ts_ms": 123, "age_s": 1, "max_age_s": 60},
                "providers": {"ok": True, "healthy": 1, "total": 1},
                "labels": {"ok": True, "count": 10},
                "model": {"ok": True, "support_n": 10},
                "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": True},
                "broker_connection": {"ok": True, "state": "connected", "broker": "paper"},
                "db": {"ok": True, "initialized": True, "exists": True, "db_path": os.environ["DB_PATH"]},
                "job_summary": {"ok": True, "total": 1, "stale": 0, "stale_jobs": []},
                "timeseries_storage": {"ok": True, "enabled": False},
                "feature_store": {"ok": True, "enabled": False},
                "portfolio_runtime": {"degraded": False, "detail": "ok"},
                "execution_supervisor": {"ok": True, "state": "ok", "failed_gates": []},
                "position_reconcile": {
                    "available": True,
                    "fatal_reconcile": True,
                    "status": "mismatch",
                    "broker": "paper",
                    "mismatched_n": 2,
                    "detail": "mismatch",
                },
                "execution_degraded": {"active": False, "severity": "WARNING"},
                "startup_validation": {"ok": True},
            },
            preflight={"ok": True},
            system_state={"state": "LIVE", "mode": "live"},
            graph={"ok": True},
        )

        issue_codes = {str(item.get("code") or "") for item in list(snapshot.get("issues") or [])}

        self.assertFalse(bool(snapshot.get("ok")))
        self.assertFalse(bool(snapshot.get("position_reconcile_ok")))
        self.assertIn("position_reconcile_failed", issue_codes)
        self.assertIn("position_reconcile", list(snapshot.get("waiting_on") or []))

    def test_readiness_snapshot_blocks_live_missing_position_reconcile_evidence(self) -> None:
        (health,) = _reload_modules("engine.runtime.health")
        os.environ["ENGINE_MODE"] = "live"

        snapshot = health.get_readiness_snapshot(
            health={
                "prices": {"ok": True, "last_ts_ms": 123, "age_s": 1, "max_age_s": 60},
                "providers": {"ok": True, "healthy": 1, "total": 1},
                "labels": {"ok": True, "count": 10},
                "model": {"ok": True, "support_n": 10},
                "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": True},
                "broker_connection": {"ok": True, "state": "connected", "broker": "alpaca"},
                "db": {"ok": True, "initialized": True, "exists": True, "db_path": os.environ["DB_PATH"]},
                "job_summary": {"ok": True, "total": 1, "stale": 0, "stale_jobs": []},
                "timeseries_storage": {"ok": True, "enabled": False},
                "feature_store": {"ok": True, "enabled": False},
                "portfolio_runtime": {"degraded": False, "detail": "ok"},
                "execution_supervisor": {"ok": True, "state": "ok", "failed_gates": []},
                "position_reconcile": {
                    "required": True,
                    "ok": False,
                    "available": False,
                    "exercised": False,
                    "status": "empty",
                    "broker": "alpaca",
                    "blockers": ["position_reconcile_not_exercised"],
                    "detail": "position_reconcile_audit_empty",
                },
                "execution_degraded": {"active": False, "severity": "WARNING"},
                "startup_validation": {"ok": True},
            },
            preflight={"ok": True},
            system_state={"state": "LIVE", "mode": "live"},
            graph={"ok": True},
        )

        issue_codes = {str(item.get("code") or "") for item in list(snapshot.get("issues") or [])}
        issue_details = "\n".join(str(item.get("detail") or "") for item in list(snapshot.get("issues") or []))

        self.assertFalse(bool(snapshot.get("ok")))
        self.assertFalse(bool(snapshot.get("position_reconcile_ok")))
        self.assertIn("position_reconcile_failed", issue_codes)
        self.assertIn("position_reconcile_not_exercised", issue_details)
        self.assertIn("position_reconcile", list(snapshot.get("waiting_on") or []))

    def test_startup_validation_allows_safe_mode_prediction_cold_start(self) -> None:
        storage, health = _reload_modules("engine.runtime.storage", "engine.runtime.health")
        storage.init_db()
        os.environ["ENGINE_MODE"] = "safe"

        snapshot = health.get_startup_validation_snapshot(
            health={
                "db": {"ok": True, "initialized": True, "db_path": os.environ["DB_PATH"]},
                "ingestion_runtime": {"running": True, "stale": False, "last_publish_ts_ms": 123},
                "ingestion_freshness": {
                    "critical_ok": True,
                    "stale_critical_sources": [],
                    "runtime_reason_codes": [],
                },
                "job_summary": {"ok": True, "required_missing": [], "required_stale": []},
                "predictions": {
                    "ok": False,
                    "detail": "predictions_empty",
                    "count": 0,
                    "recent_count": 0,
                    "history_count": 0,
                    "history_recent_count": 0,
                    "last_ts_ms": None,
                    "age_s": None,
                    "max_age_s": 600.0,
                },
                "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": False},
                "execution_supervisor": {"state": "ok"},
                "broker_connection": {"ok": False, "state": "disconnected", "broker": "sim"},
            },
            db_validation={
                "ok": True,
                "quick_check": "ok",
                "missing_tables": [],
                "schema_version": 1,
                "schema_status": "applied",
            },
        )

        self.assertTrue(snapshot["ok"])
        self.assertEqual(snapshot["blocking_gates"], [])
        self.assertEqual(snapshot["blocking_checks"], [])
        self.assertNotIn("predictions_flowing", snapshot["checks"])
        self.assertTrue(bool(snapshot["checks"]["database_reachable"]["ok"]))
        self.assertTrue(bool(snapshot["checks"]["schema_valid"]["ok"]))

    def test_startup_validation_blocks_missing_critical_systems(self) -> None:
        storage, health = _reload_modules("engine.runtime.storage", "engine.runtime.health")
        storage.init_db()
        os.environ["ENGINE_MODE"] = "live"

        snapshot = health.get_startup_validation_snapshot(
            health={
                "db": {"ok": False, "initialized": False, "db_path": os.environ["DB_PATH"]},
                "ingestion_runtime": {"running": False, "stale": True, "last_error": "spawn_failed"},
                "ingestion_freshness": {
                    "critical_ok": False,
                    "stale_critical_sources": ["prices"],
                    "runtime_reason_codes": ["critical_source_stale:prices"],
                },
                "job_summary": {
                    "ok": False,
                    "required_missing": ["ingestion_runtime"],
                    "required_stale": [],
                },
                "predictions": {
                    "ok": False,
                    "detail": "predictions_stale",
                    "count": 0,
                    "recent_count": 0,
                    "history_count": 0,
                    "history_recent_count": 0,
                    "last_ts_ms": None,
                    "age_s": None,
                    "max_age_s": 600.0,
                },
                "execution_barrier": {
                    "ok": False,
                    "reason": "execution_barrier_error:missing_system_state",
                    "allowed": False,
                },
                "execution_supervisor": {"state": "critical"},
                "broker_connection": {"ok": False, "state": "disconnected", "broker": "ibkr"},
            },
            db_validation={
                "ok": False,
                "quick_check": "error",
                "missing_tables": ["runtime_meta", "event_log"],
                "schema_version": None,
                "schema_status": "missing",
            },
        )

        self.assertFalse(snapshot["ok"])
        self.assertEqual(
            snapshot["blocking_gates"],
            [
                "database_reachable",
                "schema_valid",
            ],
        )
        self.assertEqual(
            snapshot["blocking_checks"],
            [
                "database_reachable",
                "schema_valid",
            ],
        )
        self.assertIn("database", snapshot["critical_systems_missing"])
        failed_gate_details = {
            str(item.get("id") or ""): dict(item)
            for item in list(snapshot.get("failed_gate_details") or [])
        }
        self.assertIn("database_reachable", failed_gate_details)
        self.assertIn("schema_valid", failed_gate_details)
        self.assertEqual(str(failed_gate_details["database_reachable"]["component"] or ""), "database")
        self.assertEqual(str(failed_gate_details["schema_valid"]["component"] or ""), "database")

    def test_startup_validation_blocks_partial_schema_contracts(self) -> None:
        storage, health = _reload_modules("engine.runtime.storage", "engine.runtime.health")
        storage.init_db()
        os.environ["ENGINE_MODE"] = "live"

        snapshot = health.get_startup_validation_snapshot(
            health={
                "db": {"ok": True, "initialized": True, "db_path": os.environ["DB_PATH"]},
                "ingestion_runtime": {"running": True, "stale": False, "last_publish_ts_ms": 123},
                "ingestion_freshness": {
                    "critical_ok": True,
                    "stale_critical_sources": [],
                    "runtime_reason_codes": [],
                },
                "job_summary": {"ok": True, "required_missing": [], "required_stale": []},
                "predictions": {
                    "ok": True,
                    "detail": "ok",
                    "count": 5,
                    "recent_count": 2,
                    "history_count": 8,
                    "history_recent_count": 2,
                    "last_ts_ms": 123,
                    "age_s": 1.0,
                    "max_age_s": 600.0,
                },
                "execution_barrier": {"ok": True, "reason": "health_fast_path", "allowed": False},
                "execution_supervisor": {"state": "ok"},
                "broker_connection": {"ok": False, "state": "disconnected", "broker": "sim"},
            },
            db_validation={
                "ok": False,
                "quick_check": "ok",
                "missing_tables": [],
                "missing_columns": {"prices": ["source"]},
                "missing_indexes": ["idx_prices_symbol_ts"],
                "schema_version": 10,
                "expected_schema_version": 11,
                "schema_version_ok": False,
                "schema_status": "applied",
            },
        )

        self.assertFalse(snapshot["ok"])
        self.assertIn("schema_valid", snapshot["blocking_gates"])
        schema_gate = dict(snapshot["gates"]["schema_valid"])
        self.assertFalse(bool(schema_gate["ok"]))
        self.assertIn("missing_columns={'prices': ['source']}", str(schema_gate.get("detail") or ""))
        self.assertIn("missing_indexes=['idx_prices_symbol_ts']", str(schema_gate.get("detail") or ""))

    def test_startup_config_rejects_fail_open_and_requires_async_bind(self) -> None:
        (startup_gates,) = _reload_modules("engine.runtime.startup_gates")

        with patch.dict(
            os.environ,
            {
                "TRADING_STARTUP_HEALTH_FAIL_OPEN": "1",
                "TRADING_STARTUP_HEALTH_ASYNC_BIND": "0",
            },
            clear=False,
        ):
            snapshot = startup_gates.get_startup_config_snapshot(REPO_ROOT)

        self.assertFalse(bool(snapshot["ok"]))
        error_keys = {str(item.get("key") or "") for item in list(snapshot.get("errors") or [])}
        self.assertIn("TRADING_STARTUP_HEALTH_FAIL_OPEN", error_keys)
        self.assertIn("TRADING_STARTUP_HEALTH_ASYNC_BIND", error_keys)

    def test_startup_orchestrator_health_timeout_stays_blocking(self) -> None:
        (startup_orchestrator,) = _reload_modules("engine.runtime.startup_orchestrator")
        os.environ["STARTUP_HEALTH_TIMEOUT_S"] = "1"

        def _slow_health():
            time.sleep(1.5)
            return {"ok": True}

        orchestrator = startup_orchestrator.StartupOrchestrator(
            jobs=_FakeJobs(),
            health_fn=_slow_health,
        )

        snap = orchestrator._health_snapshot()
        self.assertFalse(snap["ok"])
        self.assertTrue(snap["timed_out"])
        self.assertIn("health_timeout_after_1.0s", str(snap["error"]))
        self.assertFalse(orchestrator._backend_ready())

    def test_startup_orchestrator_does_not_treat_oneshot_timeout_as_success(self) -> None:
        (startup_orchestrator,) = _reload_modules("engine.runtime.startup_orchestrator")
        os.environ["STARTUP_ONESHOT_START_TIMEOUT_S"] = "1"

        orchestrator = startup_orchestrator.StartupOrchestrator(
            jobs=_FakeJobs(start_delay_s=1.5),
            health_fn=lambda: {"ok": False},
        )

        with patch.object(orchestrator, "_clear_stale_lock", return_value={"ok": True}):
            out = orchestrator._start_oneshot("process_events")

        self.assertFalse(out["ok"])
        self.assertTrue(out["timed_out"])
        self.assertEqual(out["job"], "process_events")

    def test_startup_orchestrator_skips_poll_prices_fallback_when_isolated_ingestion_is_enabled(self) -> None:
        prev_start_ingestion = os.environ.get("START_INGESTION_WITH_SERVER")
        os.environ["START_INGESTION_WITH_SERVER"] = "1"
        try:
            (startup_orchestrator,) = _reload_modules("engine.runtime.startup_orchestrator")
            orchestrator = startup_orchestrator.StartupOrchestrator(
                jobs=_FakeJobs(),
                health_fn=lambda: {"ok": False},
            )

            counts = {
                "symbols": 10,
                "symbol_universe": 10,
                "fresh_price_provider_health": 0,
                "prices": 0,
                "events": 0,
                "labels": 0,
                "model_registry": 0,
                "model_metrics": 0,
            }

            wait_results = iter([True, False, False])

            with patch.object(startup_orchestrator, "repair_db", return_value={"ok": True}):
                with patch.object(orchestrator, "_db_counts", side_effect=lambda: dict(counts)):
                    with patch.object(orchestrator, "_health_snapshot", return_value={"ok": False}):
                        with patch.object(orchestrator, "_wait_until", side_effect=lambda *args, **kwargs: next(wait_results)):
                            with patch.object(orchestrator, "_start_daemon") as start_daemon_mock:
                                result = orchestrator.run("safe")

            self.assertFalse(bool(result.get("ok")))
            start_daemon_mock.assert_not_called()
        finally:
            if prev_start_ingestion is None:
                os.environ.pop("START_INGESTION_WITH_SERVER", None)
            else:
                os.environ["START_INGESTION_WITH_SERVER"] = str(prev_start_ingestion)

    def test_auto_pipeline_loop_skips_poll_prices_when_isolated_ingestion_is_enabled(self) -> None:
        prev_start_ingestion = os.environ.get("START_INGESTION_WITH_SERVER")
        os.environ["START_INGESTION_WITH_SERVER"] = "1"
        try:
            (orchestrator_mod,) = _reload_modules("engine.runtime.orchestrator")
            jobs = _RecordingJobs()
            orchestrator = orchestrator_mod.RuntimeOrchestrator(
                jobs=jobs,
                acquire_lock=lambda *_args, **_kwargs: True,
                release_lock=lambda *_args, **_kwargs: None,
                auto_pipeline_include_execution=False,
                auto_pipeline_log=False,
                auto_pipeline_interval_s=60.0,
                auto_pipeline_start_delay_s=0.0,
                auto_challenger_log=False,
                auto_challenger_interval_s=60.0,
                auto_challenger_start_delay_s=0.0,
                auto_challenger_min_drift=0.0,
                auto_size_policy_log=False,
                auto_size_policy_interval_s=60.0,
                auto_size_policy_start_delay_s=0.0,
            )

            with patch.object(orchestrator, "_any_price_feed_running", return_value=False):
                with patch.object(orchestrator, "run_pipeline", side_effect=SystemExit):
                    with self.assertRaises(SystemExit):
                        orchestrator.auto_pipeline_loop()

            self.assertEqual(jobs.started, [])
        finally:
            if prev_start_ingestion is None:
                os.environ.pop("START_INGESTION_WITH_SERVER", None)
            else:
                os.environ["START_INGESTION_WITH_SERVER"] = str(prev_start_ingestion)

    def test_start_system_hardened_startup_runs_health_validation_synchronously(self) -> None:
        os.environ["TRADING_STARTUP_HEALTH_ASYNC_BIND"] = "1"
        (start_system,) = _reload_modules("start_system")

        events: list[str] = []

        def _fake_async(*, mode: str):
            events.append(f"async:{mode}")
            return None

        def _fake_sync(*, mode: str):
            events.append(f"sync:{mode}")

        def _fake_run_server():
            events.append("run_server")

        with patch.object(start_system, "_start_startup_health_validation_async", side_effect=_fake_async):
            with patch.object(start_system, "_perform_startup_health_validation", side_effect=_fake_sync):
                start_system._run_dashboard_server(_fake_run_server, mode="safe")

        self.assertEqual(events, ["sync:safe", "run_server"])

    def test_start_system_dashboard_return_requires_current_clean_shutdown(self) -> None:
        (start_system,) = _reload_modules("start_system")

        self.assertFalse(
            start_system._dashboard_returned_after_clean_shutdown(
                {"state": "LIVE", "detail": "market_data_healthy", "last_clean_shutdown_ts_ms": "99"},
                run_enter_ts_ms=100,
            )
        )
        self.assertTrue(
            start_system._dashboard_returned_after_clean_shutdown(
                {"state": "LIVE", "detail": "market_data_healthy", "last_clean_shutdown_ts_ms": "101"},
                run_enter_ts_ms=100,
            )
        )
        self.assertTrue(
            start_system._dashboard_returned_after_clean_shutdown(
                {"state": "SHUTTING_DOWN", "detail": "clean_shutdown"},
                run_enter_ts_ms=100,
            )
        )

    def test_start_system_dashboard_return_accepts_server_stop_event(self) -> None:
        (start_system,) = _reload_modules("start_system")

        stop_event = threading.Event()
        stop_event.set()
        previous_dashboard = sys.modules.get("dashboard_server")
        sys.modules["dashboard_server"] = types.SimpleNamespace(_SERVER_STOP_EVENT=stop_event)
        try:
            self.assertTrue(
                start_system._dashboard_returned_after_clean_shutdown(
                    {"state": "LIVE", "detail": "market_data_healthy", "last_clean_shutdown_ts_ms": "0"},
                    run_enter_ts_ms=100,
                )
            )
            self.assertFalse(
                start_system._dashboard_returned_after_clean_shutdown(
                    {"state": "LIVE", "detail": "market_data_healthy", "last_clean_shutdown_ts_ms": "0"},
                    run_enter_ts_ms=100,
                    stop_requested_at_enter=True,
                )
            )
        finally:
            if previous_dashboard is None:
                sys.modules.pop("dashboard_server", None)
            else:
                sys.modules["dashboard_server"] = previous_dashboard

    def test_start_system_post_bind_validation_waits_for_dashboard_bind(self) -> None:
        os.environ["TRADING_STARTUP_HEALTH_ASYNC_BIND"] = "1"
        (start_system,) = _reload_modules("start_system")

        events: list[str] = []
        dashboard_bound = threading.Event()
        async_called = threading.Event()

        def _fake_wait_for_bind(*, host: str, port: int, timeout_s: float) -> bool:
            events.append(f"wait:{host}:{port}")
            return bool(dashboard_bound.wait(timeout=timeout_s))

        def _fake_async(*, mode: str):
            events.append(f"async:{mode}")
            async_called.set()
            return None

        def _fake_run_server():
            events.append("run_server")
            dashboard_bound.set()
            time.sleep(0.05)

        with patch.object(start_system, "_wait_for_dashboard_bind", side_effect=_fake_wait_for_bind):
            with patch.object(start_system, "_start_startup_health_validation_async", side_effect=_fake_async):
                start_system._run_dashboard_server_post_bind_validation(
                    _fake_run_server,
                    mode="safe",
                    host="127.0.0.1",
                    port=8000,
                )
                self.assertTrue(async_called.wait(timeout=1.0))

        self.assertIn("wait:127.0.0.1:8000", events)
        self.assertIn("run_server", events)
        self.assertIn("async:safe", events)
        self.assertLess(events.index("run_server"), events.index("async:safe"))

    def test_start_system_late_validation_failure_requests_full_runtime_stop(self) -> None:
        with patch.dict(
            os.environ,
            {
                "EXECUTION_MODE": "live",
                "ENGINE_MODE": "live",
                "DISABLE_LIVE_EXECUTION": "0",
                "KILL_SWITCH_GLOBAL": "0",
                "EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE": "0",
            },
            clear=False,
        ):
            storage, lifecycle_state, start_system = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.lifecycle_state",
                "start_system",
            )
            storage.init_db()
            lifecycle_state.set_state(lifecycle_state.LIVE, "unit_test_live")
            start_system._INGESTION_WATCHDOG_STOP.clear()

            calls: list[str] = []
            fake_dashboard = types.ModuleType("dashboard_server")
            fake_dashboard.stop_server = lambda: calls.append("stop_server")
            previous_dashboard = sys.modules.get("dashboard_server")
            sys.modules["dashboard_server"] = fake_dashboard
            try:
                shutdown_reasons: list[str] = []

                def _runtime_shutdown(**kwargs) -> None:
                    calls.append("runtime_shutdown")
                    shutdown_reasons.append(str(kwargs.get("shutdown_reason") or ""))

                with patch.object(start_system, "runtime_shutdown", side_effect=_runtime_shutdown):
                    with patch.object(start_system, "_terminate_ingestion", side_effect=lambda: calls.append("terminate_ingestion")):
                        start_system._handle_late_startup_health_validation_failure(
                            RuntimeError("unit_late_validation_failure"),
                            mode="live",
                            scope="unit_post_bind",
                        )
            finally:
                if previous_dashboard is None:
                    sys.modules.pop("dashboard_server", None)
                else:
                    sys.modules["dashboard_server"] = previous_dashboard

            self.assertEqual(calls, ["stop_server", "runtime_shutdown", "terminate_ingestion"])
            self.assertEqual(shutdown_reasons, ["late_startup_health_validation_failed:RuntimeError:unit_late_validation_failure"])
            self.assertTrue(start_system._INGESTION_WATCHDOG_STOP.is_set())
            self.assertEqual(os.environ.get("DISABLE_LIVE_EXECUTION"), "1")
            self.assertEqual(os.environ.get("KILL_SWITCH_GLOBAL"), "1")
            self.assertIn("unit_late_validation_failure", os.environ.get("STARTUP_HEALTH_LATE_FAILURE", ""))
            self.assertEqual(str(lifecycle_state.get_state().get("state") or ""), lifecycle_state.KILL_SWITCH)

    def test_start_system_async_late_validation_failure_blocks_live_execution_gate(self) -> None:
        with patch.dict(
            os.environ,
            {
                "EXECUTION_MODE": "live",
                "ENGINE_MODE": "live",
                "ALLOW_TRAINING": "0",
                "DASHBOARD_API_TOKEN": "live-token-1234567890",
                "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_LIVE_TRADING",
                "DISABLE_LIVE_EXECUTION": "0",
                "KILL_SWITCH_GLOBAL": "0",
                "EXECUTION_BLOCK_EVENT_BUS_CRITICAL_BACKPRESSURE": "0",
            },
            clear=False,
        ):
            storage, lifecycle_state, gates, start_system = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.lifecycle_state",
                "engine.runtime.gates",
                "start_system",
            )
            storage.init_db()
            lifecycle_state.set_state(lifecycle_state.LIVE, "unit_test_live")

            def _live_mode():
                return {"mode": "live", "armed": 1}

            with patch.object(gates, "live_trading_preflight", return_value={"ok": True, "reason": "ok"}):
                before = gates.execution_gate_snapshot(
                    system_state={"state": "LIVE", "mode": "live", "armed": 1},
                    get_execution_mode_fn=_live_mode,
                    kill_switches={},
                    risk_state_getter=lambda _key, default=None: default,
                )

            self.assertTrue(bool(before["allowed"]))
            self.assertTrue(bool(before["allow_execution_pipeline"]))
            self.assertTrue(bool(before["real_trading_allowed"]))

            stop_reasons: list[str] = []

            def _fake_stop(reason: str) -> None:
                stop_reasons.append(str(reason))

            def _fail_validation(*, mode: str) -> None:
                self.assertEqual(mode, "live")
                raise RuntimeError("unit_late_validation_failure")

            with patch.object(start_system, "_perform_startup_health_validation", side_effect=_fail_validation):
                with patch.object(start_system, "_request_dashboard_runtime_stop", side_effect=_fake_stop):
                    thread = start_system._start_startup_health_validation_async(mode="live")
                    thread.join(timeout=2.0)

            self.assertFalse(thread.is_alive())
            self.assertEqual(len(stop_reasons), 1)
            self.assertIn("late_startup_health_validation_failed", stop_reasons[0])
            self.assertEqual(os.environ.get("DISABLE_LIVE_EXECUTION"), "1")
            self.assertEqual(os.environ.get("KILL_SWITCH_GLOBAL"), "1")
            self.assertIn("unit_late_validation_failure", os.environ.get("STARTUP_HEALTH_LATE_FAILURE", ""))
            self.assertEqual(str(lifecycle_state.get_state().get("state") or ""), lifecycle_state.KILL_SWITCH)

            stale_live_after_failure = gates.execution_gate_snapshot(
                system_state={"state": "LIVE", "mode": "live", "armed": 1},
                get_execution_mode_fn=_live_mode,
                kill_switches={},
                risk_state_getter=lambda _key, default=None: default,
            )
            self.assertFalse(bool(stale_live_after_failure["allowed"]))
            self.assertFalse(bool(stale_live_after_failure["allow_execution_pipeline"]))
            self.assertFalse(bool(stale_live_after_failure["real_trading_allowed"]))
            self.assertEqual(str(stale_live_after_failure["reason"]), "disable_live_execution_env")

            lifecycle_after_failure = gates.execution_gate_snapshot(
                get_execution_mode_fn=_live_mode,
                kill_switches={},
                risk_state_getter=lambda _key, default=None: default,
            )
            self.assertFalse(bool(lifecycle_after_failure["allowed"]))
            self.assertFalse(bool(lifecycle_after_failure["allow_execution_pipeline"]))
            self.assertFalse(bool(lifecycle_after_failure["real_trading_allowed"]))
            self.assertEqual(str(lifecycle_after_failure["runtime_state"]), "KILL_SWITCH")
            self.assertEqual(str(lifecycle_after_failure["reason"]), "runtime_state_kill_switch")

    def test_start_system_stale_ingestion_markers_match_daemon_modules(self) -> None:
        (start_system,) = _reload_modules("start_system")

        markers = start_system._build_repo_ingestion_process_markers({"sec_poll", "poll_macro"})

        self.assertIn("engine.data.jobs.sec_poll", markers)
        self.assertIn("engine.data.jobs.poll_macro", markers)

        repo_root = str(Path(start_system._BASE_DIR))
        venv_python = str(Path(repo_root) / ".venv" / "Scripts" / "python.exe")
        self.assertTrue(
            start_system._looks_like_repo_ingestion_process(
                f'"{venv_python}" -u -m engine.data.jobs.sec_poll'
            )
        )
        self.assertTrue(
            start_system._looks_like_repo_ingestion_process(
                f'"{venv_python}" -u -m engine.data.jobs.poll_macro'
            )
        )

    def test_first_run_seed_handles_legacy_portfolio_equity_state_schema(self) -> None:
        db_path = Path(os.environ["DB_PATH"])
        con = sqlite3.connect(db_path)
        try:
            con.execute("DROP TABLE IF EXISTS portfolio_equity_state")
            con.execute("DROP TABLE IF EXISTS broker_account")
            con.execute("DROP TABLE IF EXISTS symbols")
            con.execute(
                """
                CREATE TABLE portfolio_equity_state (
                  ts_ms INTEGER PRIMARY KEY,
                  equity REAL NOT NULL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE broker_account (
                  ts_ms INTEGER PRIMARY KEY,
                  equity REAL,
                  buying_power REAL
                )
                """
            )
            con.execute(
                """
                CREATE TABLE symbols (
                  symbol TEXT PRIMARY KEY,
                  asset_class TEXT,
                  status TEXT,
                  score REAL,
                  created_ts_ms INTEGER,
                  updated_ts_ms INTEGER,
                  meta_json TEXT
                )
                """
            )
            con.commit()
        finally:
            con.close()

        (first_run,) = _reload_modules("engine.runtime.first_run")
        out = first_run._seed_minimum_rows(str(db_path))

        self.assertTrue(out["ok"])
        self.assertIn("portfolio_equity_state", out["seeded"])

        con = sqlite3.connect(db_path)
        try:
            row = con.execute("SELECT ts_ms, equity FROM portfolio_equity_state").fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(float(row[1]), 0.0)
