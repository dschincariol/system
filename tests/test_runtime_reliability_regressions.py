from __future__ import annotations

import importlib
import json
import os
import re
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


class _SessionProbe:
    provider_name = "probe"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._desired = set()
        self._subscribed = set()
        self._connected = False
        self._authenticated = False
        self.preconnect_subscribe_calls = 0

    def replace_desired_symbols(self, symbols):
        with self._lock:
            self._desired = {str(x) for x in (symbols or [])}

    def desired_symbols(self):
        with self._lock:
            return set(self._desired)

    def subscribed_symbols(self):
        with self._lock:
            return set(self._subscribed)

    def telemetry_snapshot(self):
        with self._lock:
            return {
                "connected": bool(self._connected),
                "authenticated": bool(self._authenticated),
                "desired_symbol_count": len(self._desired),
                "subscribed_symbol_count": len(self._subscribed),
                "last_msg_age_ms": 0,
                "last_connect_ts_ms": int(time.time() * 1000),
                "last_heartbeat_ts_ms": int(time.time() * 1000),
                "capabilities": {"authentication": "api_key", "streaming": True, "polling": False},
            }

    def note_reconnecting(self, _reason=None):
        return None

    def note_error(self, _error):
        return None

    def increment_reconnect_count(self):
        return None

    def close(self):
        with self._lock:
            self._connected = False
            self._authenticated = False

    def connect(self):
        time.sleep(0.05)
        with self._lock:
            self._connected = True

    def authenticate(self):
        with self._lock:
            self._authenticated = True

    def detect_capabilities(self):
        return self.telemetry_snapshot()["capabilities"]

    def subscribe(self, symbols):
        with self._lock:
            if not (self._connected and self._authenticated):
                self.preconnect_subscribe_calls += 1
                return
            self._subscribed |= {str(x) for x in (symbols or [])}

    def unsubscribe(self, symbols):
        with self._lock:
            self._subscribed -= {str(x) for x in (symbols or [])}

    def apply_rate_limit(self, _operation="request"):
        return None

    def heartbeat(self):
        return self.telemetry_snapshot()

    def snapshot(self):
        return {}

    def perform_gap_fill(self, *_args, **_kwargs):
        return {}


class _FakeCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _PragmaProbeConnection:
    def __init__(self, *, journal_mode: str = "wal", busy_timeout_ms: int = 60000) -> None:
        self.commands = []
        self.row_factory = None
        self._journal_mode = str(journal_mode)
        self._busy_timeout_ms = int(busy_timeout_ms)

    def execute(self, sql: str):
        statement = str(sql)
        self.commands.append(statement)
        normalized = statement.strip().rstrip(";").lower()
        if normalized == "pragma journal_mode":
            return _FakeCursor((self._journal_mode,))
        if normalized == "pragma busy_timeout":
            return _FakeCursor((self._busy_timeout_ms,))
        if normalized == "pragma journal_mode=wal":
            self._journal_mode = "wal"
            return _FakeCursor((self._journal_mode,))
        return _FakeCursor(None)


class RuntimeReliabilityRegressionTests(unittest.TestCase):
    RUNTIME_ENV_KEYS = (
        "EXECUTION_MODE",
        "ENGINE_MODE",
        "BROKER",
        "BROKER_NAME",
        "LIVE_BROKER",
        "BROKER_FAILOVER",
        "DECISION_ENGINE_ENABLED",
        "DECISION_MIN_CONFIDENCE",
        "DECISION_MIN_ABS_PREDICTION",
        "UNCERTAINTY_SIZING_PRODUCTION_POLICY",
        "UNCERTAINTY_HIGH_THRESHOLD",
        "UNCERTAINTY_HARD_THRESHOLD",
        "UNCERTAINTY_MAX_AGE_MS",
        "OOD_SUPPRESS_THRESHOLD",
        "OOD_HARD_THRESHOLD",
        "RL_ALLOW_FALLBACK_AGENT",
    )

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._prev_allow_training = os.environ.get("ALLOW_TRAINING")
        self._prev_storage_backend = os.environ.get("TS_STORAGE_BACKEND")
        self._prev_sqlite_liveness_queue_enabled = os.environ.get("SQLITE_LIVENESS_QUEUE_ENABLED")
        self._prev_sqlite_trace_report_every_s = os.environ.get("SQLITE_TRACE_REPORT_EVERY_S")
        self._prev_runtime_env = {key: os.environ.get(key) for key in self.RUNTIME_ENV_KEYS}
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "runtime_reliability.db")
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["ENGINE_SUPERVISED"] = "1"
        os.environ["ALLOW_TRAINING"] = "0"
        os.environ["SQLITE_LIVENESS_QUEUE_ENABLED"] = "0"
        os.environ["SQLITE_TRACE_REPORT_EVERY_S"] = "0"
        os.environ["EXECUTION_MODE"] = "paper"
        os.environ["ENGINE_MODE"] = "paper"
        os.environ["BROKER"] = "sim"
        os.environ["BROKER_NAME"] = "sim"
        os.environ["LIVE_BROKER"] = "sim"
        os.environ["BROKER_FAILOVER"] = "sim"
        for key in self.RUNTIME_ENV_KEYS:
            if key.startswith(("DECISION_", "UNCERTAINTY_", "OOD_", "RL_")):
                os.environ.pop(key, None)
        _reload_modules("engine.runtime.db_guard", "engine.runtime.storage")

    def tearDown(self) -> None:
        try:
            storage = importlib.import_module("engine.runtime.storage")
            try:
                storage.shutdown_job_liveness_queue(timeout_s=1.0)
            except Exception:
                pass
            storage.close_pooled_connections()
        except Exception as e:
            _warn_cleanup_issue("test_runtime_reliability_regressions.close_pooled_connections", e)
        try:
            event_log = importlib.import_module("engine.runtime.event_log")
            event_log.shutdown_event_log_buffer(timeout_s=1.0)
        except Exception:
            pass
        try:
            telemetry_append_buffer = importlib.import_module("engine.runtime.telemetry_append_buffer")
            telemetry_append_buffer.shutdown_telemetry_append_buffers(timeout_s=1.0)
        except Exception:
            pass
        if self._prev_allow_training is None:
            os.environ.pop("ALLOW_TRAINING", None)
        else:
            os.environ["ALLOW_TRAINING"] = str(self._prev_allow_training)
        if self._prev_storage_backend is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = str(self._prev_storage_backend)
        if self._prev_sqlite_liveness_queue_enabled is None:
            os.environ.pop("SQLITE_LIVENESS_QUEUE_ENABLED", None)
        else:
            os.environ["SQLITE_LIVENESS_QUEUE_ENABLED"] = str(self._prev_sqlite_liveness_queue_enabled)
        if self._prev_sqlite_trace_report_every_s is None:
            os.environ.pop("SQLITE_TRACE_REPORT_EVERY_S", None)
        else:
            os.environ["SQLITE_TRACE_REPORT_EVERY_S"] = str(self._prev_sqlite_trace_report_every_s)
        for key, value in self._prev_runtime_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[str(key)] = str(value)
        self.tmp.cleanup()

    def _set_shadow_env(self):
        prev = {
            "EXECUTION_MODE": os.environ.get("EXECUTION_MODE"),
            "ENGINE_MODE": os.environ.get("ENGINE_MODE"),
            "ALLOW_TRAINING": os.environ.get("ALLOW_TRAINING"),
            "OPERATOR_MODE": os.environ.get("OPERATOR_MODE"),
            "MODE": os.environ.get("MODE"),
        }
        os.environ["EXECUTION_MODE"] = "shadow"
        os.environ["ENGINE_MODE"] = "shadow"
        os.environ["ALLOW_TRAINING"] = "0"
        os.environ.pop("OPERATOR_MODE", None)
        os.environ.pop("MODE", None)
        return prev

    def _restore_shadow_env(self, prev: dict) -> None:
        for key, value in (prev or {}).items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[str(key)] = str(value)

    def _set_live_env(self):
        prev = {
            "EXECUTION_MODE": os.environ.get("EXECUTION_MODE"),
            "ENGINE_MODE": os.environ.get("ENGINE_MODE"),
            "ALLOW_TRAINING": os.environ.get("ALLOW_TRAINING"),
            "OPERATOR_MODE": os.environ.get("OPERATOR_MODE"),
            "MODE": os.environ.get("MODE"),
            "DASHBOARD_API_TOKEN": os.environ.get("DASHBOARD_API_TOKEN"),
            "LIVE_TRADING_CONFIRM": os.environ.get("LIVE_TRADING_CONFIRM"),
            "DISABLE_LIVE_EXECUTION": os.environ.get("DISABLE_LIVE_EXECUTION"),
            "KILL_SWITCH_GLOBAL": os.environ.get("KILL_SWITCH_GLOBAL"),
            "BROKER": os.environ.get("BROKER"),
            "BROKER_NAME": os.environ.get("BROKER_NAME"),
            "LIVE_BROKER": os.environ.get("LIVE_BROKER"),
            "BROKER_FAILOVER": os.environ.get("BROKER_FAILOVER"),
            "ALPACA_BASE_URL": os.environ.get("ALPACA_BASE_URL"),
            "ALPACA_KEY_ID": os.environ.get("ALPACA_KEY_ID"),
            "ALPACA_SECRET_KEY": os.environ.get("ALPACA_SECRET_KEY"),
        }
        os.environ["EXECUTION_MODE"] = "live"
        os.environ["ENGINE_MODE"] = "live"
        os.environ["ALLOW_TRAINING"] = "0"
        os.environ["DASHBOARD_API_TOKEN"] = "live-token-1234567890"
        os.environ["LIVE_TRADING_CONFIRM"] = "I_UNDERSTAND_LIVE_TRADING"
        os.environ["DISABLE_LIVE_EXECUTION"] = "0"
        os.environ["KILL_SWITCH_GLOBAL"] = "0"
        os.environ["BROKER"] = "alpaca"
        os.environ["BROKER_NAME"] = "alpaca"
        os.environ["LIVE_BROKER"] = "alpaca"
        os.environ["BROKER_FAILOVER"] = "alpaca"
        os.environ["ALPACA_BASE_URL"] = "https://api.alpaca.markets"
        os.environ["ALPACA_KEY_ID"] = "alpaca-key"
        os.environ["ALPACA_SECRET_KEY"] = "alpaca-secret"
        os.environ.pop("OPERATOR_MODE", None)
        os.environ.pop("MODE", None)
        return prev

    def test_manager_does_not_subscribe_before_session_ready(self) -> None:
        (session_manager,) = _reload_modules("engine.data.provider_sessions.session_manager")
        probe = _SessionProbe()
        manager = session_manager.ProviderSessionManager(
            probe,
            provider_name="probe",
            heartbeat_interval_s=0.01,
        )
        try:
            manager.ensure_subscriptions({"SPY"})
            deadline = time.time() + 1.0
            while time.time() < deadline:
                if probe.subscribed_symbols() == {"SPY"}:
                    break
                time.sleep(0.02)
            self.assertEqual(probe.preconnect_subscribe_calls, 0)
            self.assertEqual(probe.subscribed_symbols(), {"SPY"})
        finally:
            manager.close()

    def test_lock_helpers_use_direct_connections(self) -> None:
        (locks,) = _reload_modules("engine.runtime.locks")

        with patch.object(locks, "_db_connect_ro_direct", return_value="ro_conn") as ro_mock:
            self.assertEqual(locks._get_conn(readonly=True), "ro_conn")
            ro_mock.assert_called_once_with()

        with patch.object(locks, "_db_connect_rw_direct", return_value="rw_conn") as rw_mock:
            self.assertEqual(locks._get_conn(readonly=False), "rw_conn")
            rw_mock.assert_called_once_with()

        with patch.object(locks, "_db_connect_runtime_ro_direct", return_value="runtime_ro_conn") as ro_mock:
            self.assertEqual(locks._get_history_conn(), "runtime_ro_conn")
            ro_mock.assert_called_once_with()

    def test_job_history_reads_use_runtime_db_when_liveness_db_is_split(self) -> None:
        os.environ["SQLITE_LIVENESS_DB_ENABLED"] = "1"
        storage, locks, api_handlers = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.locks",
            "engine.api.api_handlers",
        )
        storage.init_db()

        job_name = "job_history_runtime_db_probe"
        event_name = "history_probe_written"
        detail = "split-liveness-db-regression"
        locks.write_job_history(job_name, event_name, detail, exit_code=7, ts_ms=1_900_000_000_123)

        main_con = sqlite3.connect(os.environ["DB_PATH"])
        try:
            row = main_con.execute(
                "SELECT COUNT(*) FROM job_history WHERE job_name=? AND event=?",
                (job_name, event_name),
            ).fetchone()
        finally:
            main_con.close()
        self.assertEqual(int((row or [0])[0] or 0), 1)

        liveness_path = Path(str(storage._SQLITE_LIVENESS_DB_PATH))
        self.assertTrue(liveness_path.exists())
        liveness_con = sqlite3.connect(str(liveness_path))
        try:
            row = liveness_con.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type='table' AND name='job_history'
                """
            ).fetchone()
        finally:
            liveness_con.close()
        self.assertEqual(int((row or [0])[0] or 0), 0)

        history = locks.read_job_history(job_name, limit=5)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["event"], event_name)
        self.assertEqual(history[0]["detail"], detail)
        self.assertEqual(history[0]["exit_code"], 7)

        class _Jobs:
            def get_job_history(self, *, name: str, limit: int = 200):
                rows = locks.read_job_history(name, limit=limit)
                return {"ok": True, "job": str(name), "rows": rows}

            def get_job_log(self, *, name: str, tail: int = 200):
                return {"ok": True, "job": str(name), "tail": int(tail), "lines": ["still works"]}

        history_response = api_handlers.api_get_job_history(
            {"name": job_name, "limit": "5"},
            ctx={"JOBS": _Jobs()},
        )
        self.assertTrue(bool(history_response.get("ok")), history_response)
        self.assertEqual(history_response.get("job"), job_name)
        self.assertEqual((history_response.get("rows") or [])[0]["event"], event_name)

        log_response = api_handlers.api_get_job_log(
            {"name": job_name, "tail": "25"},
            ctx={"JOBS": _Jobs()},
        )
        self.assertTrue(bool(log_response.get("ok")), log_response)
        self.assertEqual(log_response.get("job"), job_name)
        self.assertEqual(log_response.get("lines"), ["still works"])

    def test_touch_lock_uses_write_connection_and_advances_expiry(self) -> None:
        storage, locks = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.locks",
        )
        storage.init_db()

        self.assertTrue(locks.acquire_lock("poll_prices", ttl_ms=25))

        con = storage.connect_ro_direct()
        try:
            before = con.execute(
                "SELECT expires_ms FROM job_locks WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(before)
        time.sleep(0.02)

        real_connect_rw_direct = locks._db_connect_rw_direct
        with patch.object(
            locks,
            "_db_connect_ro_direct",
            side_effect=AssertionError("touch_lock_should_not_use_readonly_connection"),
        ):
            with patch.object(locks, "_db_connect_rw_direct", wraps=real_connect_rw_direct) as rw_mock:
                locks.touch_lock("poll_prices", ttl_ms=2_000)

        self.assertGreaterEqual(int(rw_mock.call_count), 1)

        con = storage.connect_ro_direct()
        try:
            after = con.execute(
                "SELECT expires_ms FROM job_locks WHERE job_name=?",
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(after)
        self.assertGreater(int(after["expires_ms"]), int(before["expires_ms"]))

    def test_acquire_lock_refreshes_existing_same_owner_row(self) -> None:
        storage, locks = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.locks",
        )
        storage.init_db()

        now_ms = int(time.time() * 1000)
        owner = f"{os.getpid()}:{threading.get_ident()}"
        con = storage.connect_liveness_rw_direct()
        try:
            con.execute(
                """
                INSERT OR REPLACE INTO job_locks(
                    job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms, expires_ms
                ) VALUES (?,?,?,?,?,?)
                """,
                (
                    "poll_prices",
                    owner,
                    int(os.getpid()),
                    int(now_ms - 100),
                    int(now_ms - 100),
                    int(now_ms + 30_000),
                ),
            )
            con.commit()
        finally:
            con.close()

        self.assertTrue(locks.acquire_lock("poll_prices", ttl_ms=2_000))

        con = storage.connect_liveness_ro_direct()
        try:
            row = con.execute(
                """
                SELECT owner, pid, heartbeat_ts_ms, expires_ms
                FROM job_locks
                WHERE job_name=?
                """,
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(str(row["owner"]), owner)
        self.assertEqual(int(row["pid"]), int(os.getpid()))
        self.assertGreaterEqual(int(row["heartbeat_ts_ms"]), int(now_ms))
        self.assertLessEqual(int(row["expires_ms"]), int(now_ms + 2_500))

    def test_heartbeat_lock_extends_expiry_and_updates_owner_pid(self) -> None:
        storage, locks = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.locks",
        )
        storage.init_db()

        self.assertTrue(locks.acquire_lock("poll_prices", ttl_ms=25))

        con = storage.connect_ro_direct()
        try:
            before = con.execute(
                """
                SELECT owner, pid, expires_ms, heartbeat_ts_ms
                FROM job_locks
                WHERE job_name=?
                """,
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(before)
        time.sleep(0.02)

        with patch.object(
            locks,
            "_db_connect_ro_direct",
            side_effect=AssertionError("heartbeat_touch_should_not_use_readonly_connection"),
        ):
            locks.heartbeat_lock("poll_prices", ttl_ms=2_000)

        con = storage.connect_ro_direct()
        try:
            after = con.execute(
                """
                SELECT owner, pid, expires_ms, heartbeat_ts_ms
                FROM job_locks
                WHERE job_name=?
                """,
                ("poll_prices",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(after)
        self.assertGreater(int(after["expires_ms"]), int(before["expires_ms"]))
        self.assertGreaterEqual(int(after["heartbeat_ts_ms"]), int(before["heartbeat_ts_ms"]))
        self.assertEqual(int(after["pid"]), int(os.getpid()))
        self.assertTrue(str(after["owner"]).startswith(f"{os.getpid()}:"))

    def test_execution_gate_blocks_on_persisted_portfolio_runtime_critical_degraded(self) -> None:
        prev = self._set_live_env()
        try:
            storage, risk_state, gates = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.risk_state",
                "engine.runtime.gates",
            )
            storage.init_db()
            risk_state.set_state(
                "portfolio_runtime_health",
                json.dumps(
                    {
                        "updated_ts_ms": int(time.time() * 1000),
                        "degraded": True,
                        "degraded_reasons": [
                            {
                                "code": "PORTFOLIO_RISK_GATE_FAILED",
                                "detail": "unit_test_portfolio_runtime_failure",
                            }
                        ],
                    }
                ),
            )

            degraded = gates.get_execution_degraded_snapshot()
            self.assertTrue(bool(degraded.get("active")))
            self.assertEqual(str(degraded.get("severity") or ""), "CRITICAL")
            self.assertEqual(str(degraded.get("reason") or ""), "portfolio_runtime_critical_degraded")
            self.assertIn("PORTFOLIO_RISK_GATE_FAILED", list(degraded.get("reason_codes") or []))

            barrier = gates.execution_gate_snapshot(
                system_state={"state": "LIVE", "mode": "live"},
                kill_switches={},
            )

            self.assertFalse(bool(barrier.get("allowed")))
            self.assertEqual(str(barrier.get("reason") or ""), "portfolio_runtime_critical_degraded")
            self.assertEqual(
                str(((barrier.get("execution_degraded") or {}).get("reason")) or ""),
                "portfolio_runtime_critical_degraded",
            )
        finally:
            self._restore_shadow_env(prev)

    def test_execution_gate_blocks_on_event_bus_critical_backpressure(self) -> None:
        prev = self._set_live_env()
        try:
            storage, event_bus, gates = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.event_bus",
                "engine.runtime.gates",
            )
            storage.init_db()

            class _BackpressuredBus:
                def get_stats(self):
                    return {
                        "started": True,
                        "critical_backpressure_active": True,
                        "critical_backpressure_count": 3,
                        "critical_queue_size": 17,
                        "critical_queue_max_size": 16,
                        "last_critical_backpressure_ts_ms": 123456,
                    }

            with patch.object(event_bus, "get_event_bus", return_value=_BackpressuredBus()):
                degraded = gates.get_execution_degraded_snapshot()
                self.assertTrue(bool(degraded.get("active")))
                self.assertEqual(str(degraded.get("reason") or ""), "event_bus_critical_backpressure")

                barrier = gates.execution_gate_snapshot(
                    system_state={"state": "LIVE", "mode": "live"},
                    kill_switches={},
                )

            self.assertFalse(bool(barrier.get("allowed")))
            self.assertEqual(str(barrier.get("reason") or ""), "event_bus_critical_backpressure")
            self.assertEqual(
                int((((barrier.get("execution_degraded") or {}).get("sources") or [])[0].get("detail") or {}).get("critical_queue_size") or 0),
                17,
            )
        finally:
            self._restore_shadow_env(prev)

    def test_storage_heartbeat_helpers_use_direct_write_connections(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                INSERT INTO job_locks(job_name, owner, pid, acquired_ts_ms, heartbeat_ts_ms)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("poll_prices", "test-owner", 1234, 1, 1),
            )
            con.commit()
        finally:
            con.close()

        real_connect_rw_direct = storage.connect_rw_direct

        with patch.object(
            storage,
            "connect",
            side_effect=AssertionError("unexpected_pooled_write_connection"),
        ):
            with patch.object(storage, "connect_rw_direct", wraps=real_connect_rw_direct) as direct_mock:
                with patch.object(storage, "_maybe_quick_check") as quick_check_mock:
                    with patch.object(storage, "_maybe_wal_checkpoint") as wal_ckpt_mock:
                        storage.touch_job_lock("poll_prices", "test-owner", 1234)
                        storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"poll_seconds":30}')

        self.assertGreaterEqual(int(direct_mock.call_count), 2)
        quick_check_mock.assert_not_called()
        wal_ckpt_mock.assert_not_called()

    def test_storage_heartbeat_helpers_fail_fast_on_busy(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        class BusyConnection:
            def __init__(self) -> None:
                self.in_transaction = False
                self.begin_calls = 0

            def begin_managed_write(self):
                self.begin_calls += 1
                err = sqlite3.OperationalError("database is locked")
                err.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise err

            def rollback(self):
                self.in_transaction = False

            def close(self):
                return None

        touch_attempts = []
        heartbeat_attempts = []

        def _busy_touch(*args, **kwargs):
            con = BusyConnection()
            touch_attempts.append(con)
            return con

        def _busy_heartbeat(*args, **kwargs):
            con = BusyConnection()
            heartbeat_attempts.append(con)
            return con

        with patch.object(storage, "connect_rw_direct", side_effect=_busy_touch):
            with self.assertRaises(sqlite3.OperationalError):
                storage.touch_job_lock("poll_prices", "test-owner", 1234)

        with patch.object(storage, "connect_rw_direct", side_effect=_busy_heartbeat):
            with self.assertRaises(sqlite3.OperationalError):
                storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"poll_seconds":30}')

        self.assertEqual(len(touch_attempts), 1)
        self.assertEqual(int(touch_attempts[0].begin_calls), 1)
        self.assertEqual(len(heartbeat_attempts), 1)
        self.assertEqual(int(heartbeat_attempts[0].begin_calls), 1)

    def test_storage_heartbeat_helpers_best_effort_drop_busy(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        class BusyConnection:
            def __init__(self) -> None:
                self.in_transaction = False
                self.begin_calls = 0

            def begin_managed_write(self):
                self.begin_calls += 1
                err = sqlite3.OperationalError("database is locked")
                err.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise err

            def rollback(self):
                self.in_transaction = False

            def close(self):
                return None

        touch_attempts = []
        heartbeat_attempts = []

        def _busy_touch(*args, **kwargs):
            con = BusyConnection()
            touch_attempts.append(con)
            return con

        def _busy_heartbeat(*args, **kwargs):
            con = BusyConnection()
            heartbeat_attempts.append(con)
            return con

        with patch.object(storage, "connect_rw_direct", side_effect=_busy_touch):
            storage.touch_job_lock("poll_prices", "test-owner", 1234, best_effort=True)

        with patch.object(storage, "connect_rw_direct", side_effect=_busy_heartbeat):
            storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"poll_seconds":30}', best_effort=True)

        self.assertEqual(len(touch_attempts), 1)
        self.assertEqual(int(touch_attempts[0].begin_calls), 1)
        self.assertEqual(len(heartbeat_attempts), 1)
        self.assertEqual(int(heartbeat_attempts[0].begin_calls), 1)

    def test_run_write_txn_maintenance_false_skips_post_write_maintenance(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        with patch.object(storage, "_maybe_quick_check") as quick_check_mock:
            with patch.object(storage, "_maybe_wal_checkpoint") as wal_ckpt_mock:
                storage.run_write_txn(
                    lambda con: con.execute(
                        """
                        INSERT INTO runtime_meta(key, value, updated_ts_ms)
                        VALUES (?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                          value=excluded.value,
                          updated_ts_ms=excluded.updated_ts_ms
                        """,
                        ("maintenance_skip_probe", "1", 1),
                    ),
                    table="runtime_meta",
                    operation="maintenance_skip_probe",
                    direct=True,
                    maintenance=False,
                )

        quick_check_mock.assert_not_called()
        wal_ckpt_mock.assert_not_called()

    def test_run_write_txn_direct_uses_dedicated_write_connection(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        with patch.object(
            storage,
            "connect",
            side_effect=AssertionError("unexpected_pooled_write_connection"),
        ):
            storage.run_write_txn(
                lambda con: con.execute(
                    """
                    INSERT INTO runtime_meta(key, value, updated_ts_ms)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                      value=excluded.value,
                      updated_ts_ms=excluded.updated_ts_ms
                    """,
                    ("direct_write_probe", "1", 1),
                ),
                table="runtime_meta",
                operation="direct_write_probe",
                direct=True,
            )

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                "SELECT value FROM runtime_meta WHERE key=?",
                ("direct_write_probe",),
            ).fetchone()
        finally:
            con.close()
        self.assertEqual(str(row[0] or ""), "1")

    def test_readonly_connections_do_not_apply_write_pragmas(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = _PragmaProbeConnection()

        storage._apply_pragmas(con, readonly=True)

        commands = list(con.commands)
        expected_busy_timeout = int(storage._SQLITE_BUSY_TIMEOUT_MS)
        self.assertIs(con.row_factory, sqlite3.Row)
        self.assertIn(f"PRAGMA busy_timeout={expected_busy_timeout};", commands)
        self.assertIn("PRAGMA journal_mode;", commands)
        self.assertIn("PRAGMA busy_timeout;", commands)
        self.assertIn("PRAGMA query_only=ON;", commands)
        self.assertNotIn("PRAGMA journal_mode=WAL;", commands)
        self.assertFalse(any("synchronous=" in command for command in commands))
        self.assertFalse(any("locking_mode=" in command for command in commands))
        self.assertFalse(any("wal_autocheckpoint=" in command for command in commands))
        self.assertFalse(any("journal_size_limit=" in command for command in commands))
        self.assertFalse(any("defer_foreign_keys=ON" in command for command in commands))
        self.assertFalse(any("wal_checkpoint(" in command for command in commands))
        self.assertFalse(any("trusted_schema=OFF" in command for command in commands))
        self.assertFalse(any("recursive_triggers=ON" in command for command in commands))

    def test_writer_connections_still_apply_wal_and_safety_pragmas(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = _PragmaProbeConnection(journal_mode="delete")

        storage._apply_pragmas(con, readonly=False)

        commands = list(con.commands)
        expected_busy_timeout = int(storage._SQLITE_BUSY_TIMEOUT_MS)
        self.assertIs(con.row_factory, sqlite3.Row)
        self.assertIn("PRAGMA journal_mode=WAL;", commands)
        self.assertIn(f"PRAGMA busy_timeout={expected_busy_timeout};", commands)
        self.assertIn("PRAGMA journal_mode;", commands)
        self.assertIn("PRAGMA busy_timeout;", commands)
        self.assertIn("PRAGMA defer_foreign_keys=ON;", commands)
        self.assertIn("PRAGMA wal_checkpoint(PASSIVE);", commands)
        self.assertIn("PRAGMA trusted_schema=OFF;", commands)
        self.assertIn("PRAGMA recursive_triggers=ON;", commands)

    def test_storage_liveness_queue_flushes_and_preserves_pending_metadata(self) -> None:
        with patch.dict(
            os.environ,
            {
                "SQLITE_LIVENESS_QUEUE_ENABLED": "1",
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

            storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"poll_seconds":30}')
            storage.put_job_heartbeat("poll_prices", "test-owner", 1234, '{"providers":{"yfinance":{"connected":true}}}')

            queued = storage.get_connection_debug_snapshot()
            self.assertTrue(bool(((queued.get("liveness_queue") or {}).get("enabled"))))
            self.assertEqual(int(((queued.get("liveness_queue") or {}).get("pending_count"))), 1)

            flush_result = storage.flush_job_liveness_queue()
            self.assertGreaterEqual(int(flush_result.get("flushed") or 0), 1)

            con = storage.connect_ro_direct()
            try:
                row = con.execute(
                    "SELECT extra_json FROM job_heartbeats WHERE job_name=?",
                    ("poll_prices",),
                ).fetchone()
            finally:
                con.close()

            self.assertIsNotNone(row)
            payload = json.loads(str(row["extra_json"] or "{}"))
            self.assertEqual(int(payload["poll_seconds"]), 30)
            self.assertTrue(bool((((payload.get("providers") or {}).get("yfinance") or {}).get("connected"))))

    def test_metrics_store_reinitializes_when_db_path_cache_is_stale(self) -> None:
        storage, metrics_store = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.metrics_store",
        )
        metrics_store._METRICS_DB_READY = True
        metrics_store._METRICS_DB_READY_PATH = "stale-db-path"

        metrics_store.write_runtime_metric("runtime.boundary.metric", value_num=1.0, tags={"case": "stale_path"})
        metrics_store.flush_runtime_metrics_buffer(max_batches=8)

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM runtime_metrics WHERE metric=?",
                ("runtime.boundary.metric",),
            ).fetchone()
            self.assertEqual(int(row[0]), 1)
        finally:
            con.close()

    def test_put_normalized_event_records_pipeline_timing_metadata(self) -> None:
        storage, = _reload_modules("engine.runtime.storage")
        storage.init_db()

        source_ts_ms = int(time.time() * 1000) - 250
        event_id = storage.put_normalized_event(
            {
                "timestamp": int(source_ts_ms),
                "source": "unit_test",
                "title": "latency probe",
                "body": "body",
                "url": "https://example.com/test",
                "event_key": "unit-test-latency-probe",
            }
        )

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT meta_json FROM events WHERE id=?", (int(event_id),)).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        meta = json.loads(str(row[0] or "{}"))
        timing = dict(meta.get("pipeline_timing") or {})
        self.assertEqual(int(timing.get("source_event_ts_ms") or 0), int(source_ts_ms))
        self.assertGreaterEqual(int(timing.get("db_observed_ts_ms") or 0), int(source_ts_ms))
        self.assertGreaterEqual(int(timing.get("ingestion_to_db_latency_ms") or 0), 0)

    def test_load_latest_execution_intents_propagates_pipeline_timing_from_alerts(self) -> None:
        storage, portfolio, intents = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.strategy.portfolio_execution_intents",
        )
        storage.init_db()

        con = storage.connect()
        try:
            con.executescript(portfolio.SCHEMA)
            now_ms = int(time.time() * 1000)
            explain = {
                "model_name": "baseline",
                "model_id": "baseline",
                "pipeline_timing": {
                    "source_event_ts_ms": now_ms - 4000,
                    "db_observed_ts_ms": now_ms - 3000,
                    "prediction_ts_ms": now_ms - 2000,
                    "decision_ts_ms": now_ms - 1000,
                },
            }
            cur = con.execute(
                """
                INSERT INTO alerts(
                  ts_ms, event_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms - 1500),
                    1,
                    "probe",
                    "AAPL",
                    300,
                    1.5,
                    0.9,
                    "HIGH",
                    "r1",
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    "intent-latency-probe",
                ),
            )
            alert_id = int(cur.lastrowid)
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side,
                  from_weight, to_weight, delta_weight, source_alert_id, explain_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "baseline",
                    "AAPL",
                    "OPEN",
                    "FLAT",
                    "LONG",
                    0.0,
                    0.15,
                    0.15,
                    int(alert_id),
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()

            with patch.object(intents, "get_competition_policy_for_intent", return_value={}):
                batch = intents.load_latest_execution_intents(con)
        finally:
            con.close()

        self.assertTrue(bool(batch.get("ok")))
        self.assertEqual(len(batch.get("intents") or []), 1)
        intent = dict((batch.get("intents") or [])[0] or {})
        self.assertEqual(int(intent.get("decision_ts_ms") or 0), int(now_ms - 1000))
        self.assertEqual(int(intent.get("prediction_ts_ms") or 0), int(now_ms - 2000))
        self.assertEqual(int(intent.get("db_observed_ts_ms") or 0), int(now_ms - 3000))
        self.assertEqual(int(intent.get("source_event_ts_ms") or 0), int(now_ms - 4000))

    def test_load_latest_execution_intents_clamps_to_remaining_competition_budget(self) -> None:
        storage, portfolio, intents = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio",
            "engine.strategy.portfolio_execution_intents",
        )
        storage.init_db()

        con = storage.connect()
        try:
            con.executescript(portfolio.SCHEMA)
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS earnings_calendar (
                  symbol TEXT NOT NULL,
                  earnings_date TEXT NOT NULL,
                  time_of_day TEXT,
                  eps_est REAL,
                  eps_act REAL,
                  revenue_est REAL,
                  revenue_act REAL,
                  source TEXT,
                  updated_ts_ms INTEGER NOT NULL,
                  PRIMARY KEY(symbol, earnings_date)
                );
                """
            )
            now_ms = int(time.time() * 1000)
            explain = {
                "model_name": "carry_v1",
                "model_id": "carry_v1",
                "regime": "global",
                "horizon_s": 300,
            }
            cur = con.execute(
                """
                INSERT INTO alerts(
                  ts_ms, event_id, event_title, symbol, horizon_s, expected_z, confidence,
                  severity, rule_id, explain_json, dedupe_key
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms - 1500),
                    1,
                    "budget probe",
                    "AAPL",
                    300,
                    1.2,
                    0.8,
                    "HIGH",
                    "budget-r1",
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    "intent-budget-probe",
                ),
            )
            alert_id = int(cur.lastrowid)
            con.execute(
                """
                INSERT INTO portfolio_state(
                  model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                )
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    "carry_v1",
                    "AAPL",
                    "LONG",
                    0.08,
                    int(now_ms - 4000),
                    int(now_ms - 1000),
                    int(alert_id),
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.execute(
                """
                INSERT INTO portfolio_orders(
                  ts_ms, model_id, symbol, action, from_side, to_side,
                  from_weight, to_weight, delta_weight, source_alert_id, explain_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(now_ms),
                    "carry_v1",
                    "AAPL",
                    "INCREASE",
                    "LONG",
                    "LONG",
                    0.04,
                    0.15,
                    0.11,
                    int(alert_id),
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                ),
            )
            con.commit()

            with patch.object(
                intents,
                "get_competition_policy_for_intent",
                return_value={
                    "allowed": True,
                    "blocked": False,
                    "reason": "",
                    "group_key": "AAPL|300|global",
                    "group_budget_fraction": 0.12,
                    "model_budget_fraction": 0.10,
                    "capital_multiplier": 1.0,
                    "effective_allocation_fraction": 0.10,
                    "risk_limit_multiplier": 1.0,
                    "regime": "global",
                },
            ):
                batch = intents.load_latest_execution_intents(con)
        finally:
            con.close()

        self.assertTrue(bool(batch.get("ok")))
        self.assertEqual(len(batch.get("intents") or []), 1)
        intent = dict((batch.get("intents") or [])[0] or {})
        competition = dict(intent.get("competition") or {})
        self.assertAlmostEqual(float(intent.get("to_weight") or 0.0), 0.06, places=6)
        self.assertAlmostEqual(float(intent.get("delta_weight") or 0.0), 0.02, places=6)
        self.assertAlmostEqual(float(competition.get("current_model_exposure_fraction") or 0.0), 0.08, places=6)
        self.assertAlmostEqual(float(competition.get("current_group_exposure_fraction") or 0.0), 0.08, places=6)
        self.assertAlmostEqual(float(competition.get("remaining_budget_fraction") or 0.0), 0.06, places=6)
        self.assertAlmostEqual(float(competition.get("remaining_group_budget_fraction") or 0.0), 0.08, places=6)
        self.assertIn("model_budget_remaining", str(competition.get("resize_reason") or ""))

    def test_load_latest_execution_intents_decision_gate_downgrades_real_orders_to_shadow(self) -> None:
        prev_env = {
            "DECISION_ENGINE_ENABLED": os.environ.get("DECISION_ENGINE_ENABLED"),
            "DECISION_MIN_CONFIDENCE": os.environ.get("DECISION_MIN_CONFIDENCE"),
            "DECISION_MIN_ABS_PREDICTION": os.environ.get("DECISION_MIN_ABS_PREDICTION"),
        }
        os.environ["DECISION_ENGINE_ENABLED"] = "1"
        os.environ["DECISION_MIN_CONFIDENCE"] = "0.80"
        os.environ["DECISION_MIN_ABS_PREDICTION"] = "0.75"
        try:
            _, storage, portfolio, intents = _reload_modules(
                "engine.decision_engine",
                "engine.runtime.storage",
                "engine.strategy.portfolio",
                "engine.strategy.portfolio_execution_intents",
            )
            storage.init_db()

            con = storage.connect()
            try:
                con.executescript(portfolio.SCHEMA)
                now_ms = int(time.time() * 1000)
                explain = {
                    "model_name": "baseline",
                    "model_id": "baseline",
                    "model_intent": {
                        "expected_z": 1.1,
                        "confidence": 0.62,
                    },
                }
                cur = con.execute(
                    """
                    INSERT INTO alerts(
                      ts_ms, event_id, event_title, symbol, horizon_s, expected_z, confidence,
                      severity, rule_id, explain_json, dedupe_key
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(now_ms - 1000),
                        1,
                        "decision probe",
                        "AAPL",
                        300,
                        1.1,
                        0.62,
                        "HIGH",
                        "decision-r1",
                        json.dumps(explain, separators=(",", ":"), sort_keys=True),
                        "intent-decision-probe",
                    ),
                )
                alert_id = int(cur.lastrowid)
                con.execute(
                    """
                    INSERT INTO portfolio_orders(
                      ts_ms, model_id, symbol, action, from_side, to_side,
                      from_weight, to_weight, delta_weight, source_alert_id, explain_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        int(now_ms),
                        "baseline",
                        "AAPL",
                        "OPEN",
                        "FLAT",
                        "LONG",
                        0.0,
                        0.15,
                        0.15,
                        int(alert_id),
                        json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    ),
                )
                con.commit()

                with patch.object(intents, "get_competition_policy_for_intent", return_value={}):
                    batch = intents.load_latest_execution_intents(con)
            finally:
                con.close()
        finally:
            for key, value in prev_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[str(key)] = str(value)

        self.assertTrue(bool(batch.get("ok")))
        self.assertEqual(len(batch.get("intents") or []), 1)
        self.assertEqual(len(batch.get("shadowed_intents") or []), 1)
        intent = dict((batch.get("intents") or [])[0] or {})
        decision = dict(intent.get("decision") or {})
        self.assertEqual(str(intent.get("execution_target") or ""), "shadow")
        self.assertFalse(bool(decision.get("execute")))
        self.assertIn("confidence_below_threshold", list(decision.get("reasons") or []))
        self.assertEqual(int((batch.get("decision_summary") or {}).get("shadowed") or 0), 1)

    def test_execution_intent_optional_table_lookups_degrade_quietly_when_missing(self) -> None:
        storage, intents = _reload_modules(
            "engine.runtime.storage",
            "engine.strategy.portfolio_execution_intents",
        )
        storage.init_db()

        con = storage.connect()
        try:
            with patch.object(intents, "_warn_nonfatal") as warn_nonfatal:
                self.assertEqual(float(intents._get_factor_feature_asof(con, "options.skew_25d_z", int(time.time() * 1000))), 0.0)
                self.assertEqual(float(intents._earnings_proximity_decay(con, "AAPL", int(time.time() * 1000))), 0.0)
            warn_nonfatal.assert_not_called()
        finally:
            con.close()

    def test_market_stress_safe_last_ignores_empty_series_without_warning(self) -> None:
        (market_stress,) = _reload_modules("engine.strategy.market_stress")

        with patch.object(market_stress, "_warn_nonfatal") as warn_nonfatal:
            self.assertEqual(float(market_stress._safe_last([])), 0.0)
        warn_nonfatal.assert_not_called()

    def test_market_stress_preserves_post_gdelt_scores_above_one(self) -> None:
        (market_stress,) = _reload_modules("engine.strategy.market_stress")

        def fake_prices(_con, _symbol, ts_ms, _n):
            return [(int(ts_ms) - (idx * 60_000), 100.0) for idx in range(120)]

        with patch.object(market_stress, "_load_prices", side_effect=fake_prices):
            with patch(
                "engine.data.gdelt_macro.get_gdelt_macro_snapshot",
                return_value={
                    "z_doc_count": 0.0,
                    "z_tone_mean": 0.0,
                    "z_conflict_share": 0.8,
                },
            ):
                snapshot = market_stress.get_market_stress_snapshot(con=object(), ts_ms=1_700_000_000_000)

        self.assertGreater(float(snapshot["stress_score"]), 1.0)
        self.assertAlmostEqual(float(snapshot["stress_score"]), 1.3, places=9)
        self.assertEqual(
            market_stress.market_stress_thresholds(),
            {"warning": 0.55, "critical": 0.75},
        )

    def test_broker_router_fast_success_suppresses_success_trace_logging(self) -> None:
        prev = os.environ.get("BROKER_ROUTER_SUCCESS_TRACE_MIN_MS")
        os.environ["BROKER_ROUTER_SUCCESS_TRACE_MIN_MS"] = "999999"
        try:
            (broker_router,) = _reload_modules("engine.execution.broker_router")

            with patch.object(broker_router, "_execution_gate_or_block", return_value=None):
                with patch.object(broker_router, "_apply_one", return_value={"ok": True, "status": "ok"}):
                    with patch.object(broker_router, "trace_event") as trace_mock:
                        with patch.object(broker_router, "log_event") as log_mock:
                            with patch.object(broker_router, "emit_counter"):
                                with patch.object(broker_router, "emit_timing"):
                                    result = broker_router.apply_new_portfolio_orders_router(
                                        dry_run=False,
                                        override_orders=[{"symbol": "AAPL", "execution_target": "real"}],
                                    )

            self.assertTrue(bool(result.get("ok")))
            trace_mock.assert_not_called()
            log_mock.assert_not_called()
        finally:
            if prev is None:
                os.environ.pop("BROKER_ROUTER_SUCCESS_TRACE_MIN_MS", None)
            else:
                os.environ["BROKER_ROUTER_SUCCESS_TRACE_MIN_MS"] = prev

    def test_broker_router_retries_transient_failures_and_updates_execution_health(self) -> None:
        prev = {
            "BROKER_ROUTER_RETRY_ATTEMPTS": os.environ.get("BROKER_ROUTER_RETRY_ATTEMPTS"),
            "BROKER_ROUTER_RETRY_BASE_S": os.environ.get("BROKER_ROUTER_RETRY_BASE_S"),
            "BROKER_ROUTER_RETRY_MAX_S": os.environ.get("BROKER_ROUTER_RETRY_MAX_S"),
            "BROKER_FAILOVER": os.environ.get("BROKER_FAILOVER"),
        }
        os.environ["BROKER_ROUTER_RETRY_ATTEMPTS"] = "2"
        os.environ["BROKER_ROUTER_RETRY_BASE_S"] = "0"
        os.environ["BROKER_ROUTER_RETRY_MAX_S"] = "0"
        os.environ["BROKER_FAILOVER"] = "sim"
        try:
            storage, broker_router, metrics_store, observability = _reload_modules(
                "engine.runtime.storage",
                "engine.execution.broker_router",
                "engine.runtime.metrics_store",
                "engine.runtime.observability",
            )
            storage.init_db()

            with patch.object(broker_router, "_execution_gate_or_block", return_value=None):
                with patch.object(
                    broker_router,
                    "_apply_one",
                    side_effect=[
                        {"ok": False, "status": "temporary_failure"},
                        {"ok": True, "status": "ok"},
                    ],
                ) as apply_mock:
                    with patch.object(broker_router.time, "sleep", return_value=None):
                        result = broker_router.apply_new_portfolio_orders_router(
                            dry_run=False,
                            override_orders=[{"symbol": "AAPL", "execution_target": "real"}],
                        )

            self.assertTrue(bool(result.get("ok")))
            self.assertEqual(apply_mock.call_count, 2)
            self.assertEqual(len(result.get("failover_attempts") or []), 2)

            metrics = metrics_store.get_runtime_metrics(metric="execution_success_rate")
            self.assertTrue(bool(metrics["ok"]))
            self.assertTrue(
                any(
                    str(row["tags"].get("broker") or "") == "sim"
                    and float(row["value_num"] or 0.0) >= 0.5
                    for row in (metrics.get("rows") or [])
                )
            )

            health = observability.get_component_health_snapshot("execution")
            self.assertTrue(bool(health.get("ok")))
            self.assertEqual(str(health.get("status") or ""), "ok")
            self.assertEqual(str(health.get("broker") or ""), "sim")
        finally:
            for key, value in prev.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = str(value)

    def test_runtime_meta_recovers_from_stale_pooled_writer_on_retry(self) -> None:
        storage, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()

        real_run_write_txn = runtime_meta.run_write_txn
        attempts = {"count": 0}
        kwargs_seen = []

        def _flaky_run_write_txn(fn, *args, **kwargs):
            attempts["count"] += 1
            kwargs_seen.append(dict(kwargs))
            if attempts["count"] == 1:
                raise sqlite3.OperationalError("write_transaction_already_active")
            return real_run_write_txn(fn, *args, **kwargs)

        with patch.object(runtime_meta, "run_write_txn", side_effect=_flaky_run_write_txn) as run_mock:
            with patch.object(runtime_meta, "close_pooled_connections") as close_mock:
                runtime_meta.meta_set("retry_probe_key", "retry_probe_value")

        self.assertEqual(run_mock.call_count, 2)
        close_mock.assert_called_once_with()
        self.assertTrue(all(bool(kwargs.get("direct")) for kwargs in kwargs_seen))
        self.assertTrue(all(kwargs.get("maintenance") is False for kwargs in kwargs_seen))
        self.assertEqual(runtime_meta.meta_get("retry_probe_key"), "retry_probe_value")

    def test_ingestion_state_best_effort_write_bypasses_async_runtime_meta_buffer(self) -> None:
        storage, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        storage.init_db()

        payload = json.dumps({"running": True, "pid": 1234}, separators=(",", ":"), sort_keys=True)
        with runtime_meta._BEST_EFFORT_BUFFER_LOCK:
            runtime_meta._BEST_EFFORT_BUFFER_PENDING.clear()
            runtime_meta._BEST_EFFORT_BUFFER_INFLIGHT.clear()

        runtime_meta.meta_set("ingestion_state", payload, best_effort=True)

        with runtime_meta._BEST_EFFORT_BUFFER_LOCK:
            self.assertNotIn("ingestion_state", runtime_meta._BEST_EFFORT_BUFFER_PENDING)
            self.assertNotIn("ingestion_state", runtime_meta._BEST_EFFORT_BUFFER_INFLIGHT)

        con = storage.connect_ro_direct()
        try:
            row = con.execute(
                "SELECT value FROM runtime_meta WHERE key=?",
                ("ingestion_state",),
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)
        self.assertEqual(str(row[0] or ""), payload)

    def test_provider_session_meta_writes_are_best_effort(self) -> None:
        (session_manager,) = _reload_modules("engine.data.provider_sessions.session_manager")
        probe = _SessionProbe()
        manager = session_manager.ProviderSessionManager(
            probe,
            provider_name="probe",
            heartbeat_interval_s=0.01,
        )
        try:
            with patch.object(session_manager, "meta_set") as meta_set_mock:
                manager._write_meta()
        finally:
            manager.close()

        meta_set_mock.assert_called_once()
        self.assertTrue(bool(meta_set_mock.call_args.kwargs.get("best_effort")))

    def test_provider_session_meta_loop_writes_are_throttled(self) -> None:
        (session_manager,) = _reload_modules("engine.data.provider_sessions.session_manager")
        probe = _SessionProbe()
        manager = session_manager.ProviderSessionManager(
            probe,
            provider_name="probe",
            heartbeat_interval_s=0.01,
        )
        manager.meta_write_interval_s = 60.0
        try:
            with patch.object(session_manager, "meta_set") as meta_set_mock:
                manager._write_meta()
                manager._write_meta()
                manager._write_meta(force=True)
        finally:
            manager.close()

        self.assertEqual(meta_set_mock.call_count, 2)

    def test_provider_session_loop_metrics_are_throttled(self) -> None:
        (session_manager,) = _reload_modules("engine.data.provider_sessions.session_manager")
        probe = _SessionProbe()
        manager = session_manager.ProviderSessionManager(
            probe,
            provider_name="probe",
            heartbeat_interval_s=0.01,
        )
        manager.metric_emit_interval_s = 60.0
        heartbeat = probe.telemetry_snapshot()
        try:
            with patch.object(session_manager, "emit_gauge") as emit_gauge_mock:
                manager._emit_loop_metrics(heartbeat, 25)
                manager._emit_loop_metrics(heartbeat, 25)
        finally:
            manager.close()

        self.assertEqual(emit_gauge_mock.call_count, 3)

    def test_closed_connection_registry_is_bounded_under_churn(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()
        storage.close_pooled_connections()

        for _ in range(128):
            con = storage.connect_ro_direct()
            try:
                con.execute("SELECT 1").fetchone()
            finally:
                con.close()

        storage.close_pooled_connections()
        debug = storage.get_connection_debug_snapshot()
        connections = list(debug.get("connections") or [])
        active = [row for row in connections if not bool(row.get("closed"))]
        closed = [row for row in connections if bool(row.get("closed"))]

        self.assertEqual(active, [])
        self.assertLessEqual(len(closed), 64)

    def test_state_cache_releases_load_locks_after_cache_miss_resolution(self) -> None:
        (state_cache,) = _reload_modules("engine.runtime.state_cache")
        load_calls = []
        results = []
        errors = []

        def _loader() -> dict:
            load_calls.append(time.time())
            time.sleep(0.05)
            return {"ok": True}

        def _worker() -> None:
            try:
                results.append(
                    state_cache.cache_get_or_load("runtime_reliability", "shared_key", _loader, ttl_s=0.25)
                )
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5.0)

        self.assertEqual(errors, [])
        self.assertEqual(len(load_calls), 1)
        self.assertEqual(len(results), 6)
        self.assertEqual(len(state_cache._CACHE._load_locks), 0)

        time.sleep(0.30)
        self.assertIsNone(state_cache.cache_get("runtime_reliability", "shared_key"))
        self.assertEqual(len(state_cache._CACHE._load_locks), 0)

    def test_execution_gate_allows_shadow_pipeline_only_when_runtime_live(self) -> None:
        prev_env = self._set_shadow_env()
        try:
            (gates,) = _reload_modules("engine.runtime.gates")

            shadow_live = gates.execution_gate_snapshot(
                system_state={"state": "LIVE"},
                get_execution_mode_fn=lambda: {"mode": "shadow", "armed": 0},
            )
            self.assertTrue(bool(shadow_live["allow_execution_pipeline"]))
            self.assertFalse(bool(shadow_live["real_trading_allowed"]))
            self.assertEqual(str(shadow_live["reason"]), "mode_shadow_live_runtime")

            shadow_warming = gates.execution_gate_snapshot(
                system_state={"state": "WARMING_UP"},
                get_execution_mode_fn=lambda: {"mode": "shadow", "armed": 0},
            )
            self.assertFalse(bool(shadow_warming["allow_execution_pipeline"]))
            self.assertEqual(str(shadow_warming["reason"]), "runtime_state_warming_up")
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_ignores_risk_state_cache_from_different_db(self) -> None:
        prev_env = self._set_shadow_env()
        alt_tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        try:
            risk_state, gates = _reload_modules(
                "engine.runtime.risk_state",
                "engine.runtime.gates",
            )
            risk_state.set_state("portfolio_risk_block", "1")

            os.environ["DB_PATH"] = str(Path(alt_tmp.name) / "runtime_reliability_alt.db")
            _reload_modules("engine.runtime.db_guard", "engine.runtime.storage")
            (gates,) = _reload_modules("engine.runtime.gates")

            shadow_live = gates.execution_gate_snapshot(
                system_state={"state": "LIVE"},
                get_execution_mode_fn=lambda: {"mode": "shadow", "armed": 0},
            )

            self.assertTrue(bool(shadow_live["allow_execution_pipeline"]))
            self.assertEqual(str(shadow_live["reason"]), "mode_shadow_live_runtime")
        finally:
            alt_tmp.cleanup()
            self._restore_shadow_env(prev_env)

    def test_execution_gate_blocks_live_trading_when_runtime_degraded(self) -> None:
        prev_env = self._set_live_env()
        try:
            storage, runtime_meta, gates = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.runtime_meta",
                "engine.runtime.gates",
            )
            storage.init_db()
            runtime_meta.meta_set(
                "ingestion_state",
                json.dumps(
                    {
                        "source_health": {
                            "degraded": False,
                            "runtime_reason_codes": [],
                            "advisory_reason_codes": ["source_degraded:news"],
                        }
                    }
                ),
            )

            live_degraded = gates.execution_gate_snapshot(
                system_state={"state": "DEGRADED", "detail": "alpha_decay_monitor:warn"},
                get_execution_mode_fn=lambda: {"mode": "live", "armed": 1},
            )

            self.assertFalse(bool(live_degraded["allow_execution_pipeline"]))
            self.assertFalse(bool(live_degraded["allow_execution"]))
            self.assertFalse(bool(live_degraded["real_trading_allowed"]))
            self.assertEqual(str(live_degraded["severity"]), "DEGRADED")
            self.assertTrue(bool(live_degraded["conditional_allow"]))
            self.assertEqual(str(live_degraded["reason"]), "runtime_state_degraded")
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_reblocks_live_on_critical_health_reason_codes(self) -> None:
        prev_env = self._set_live_env()
        try:
            (gates,) = _reload_modules("engine.runtime.gates")
            critical_reason_codes = (
                "ingestion_not_running",
                "prices_stale_age_s=999.0",
                "no_prices",
                "providers_not_ok",
                "broker_connection_unavailable",
                "jobs_not_running",
                "jobs_not_ok",
                "execution_supervisor_critical",
                "execution_supervisor_unavailable",
            )

            for reason_code in critical_reason_codes:
                with self.subTest(reason_code=reason_code):
                    with patch.object(
                        gates,
                        "live_trading_preflight",
                        return_value={"ok": True, "reason": "ok"},
                    ) as preflight:
                        blocked = gates.execution_gate_snapshot(
                            system_state={
                                "state": "LIVE",
                                "mode": "live",
                                "armed": 1,
                                "reasons": [reason_code],
                                "critical_blockers": [reason_code],
                            },
                            kill_switches={},
                            risk_state_getter=lambda _key, default=None: default,
                        )

                    self.assertFalse(bool(blocked["allow_execution_pipeline"]))
                    self.assertFalse(bool(blocked["allow_execution"]))
                    self.assertFalse(bool(blocked["real_trading_allowed"]))
                    self.assertFalse(bool(blocked["allowed"]))
                    self.assertEqual(str(blocked["severity"]), "CRITICAL")
                    self.assertEqual(str(blocked["reason"]), reason_code)
                    self.assertIn(reason_code, list(blocked["severity_reasons"] or []))
                    preflight.assert_not_called()
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_blocks_live_armed_runtime_when_disable_live_execution_set(self) -> None:
        prev_env = self._set_live_env()
        try:
            os.environ["DISABLE_LIVE_EXECUTION"] = "true"
            (gates,) = _reload_modules("engine.runtime.gates")

            blocked = gates.execution_gate_snapshot(
                system_state={"state": "LIVE"},
                get_execution_mode_fn=lambda: {"mode": "live", "armed": 1},
                kill_switches={},
                risk_state_getter=lambda _key, default=None: default,
            )

            self.assertFalse(bool(blocked["allow_execution_pipeline"]))
            self.assertFalse(bool(blocked["allow_execution"]))
            self.assertFalse(bool(blocked["real_trading_allowed"]))
            self.assertFalse(bool(blocked["allowed"]))
            self.assertEqual(str(blocked["reason"]), "disable_live_execution_env")
            self.assertTrue(bool(blocked["disable_live_execution"]))
            self.assertIn("disable_live_execution_env", list(blocked["severity_reasons"] or []))
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_blocks_env_global_kill_switch(self) -> None:
        prev_env = self._set_live_env()
        try:
            os.environ["KILL_SWITCH_GLOBAL"] = "1"
            (gates,) = _reload_modules("engine.runtime.gates")

            with patch.object(gates, "live_trading_preflight", return_value={"ok": True, "reason": "ok"}) as preflight:
                blocked = gates.execution_gate_snapshot(
                    system_state={"state": "LIVE"},
                    get_execution_mode_fn=lambda: {"mode": "live", "armed": 1},
                    kill_switches={},
                    risk_state_getter=lambda _key, default=None: default,
                )

            self.assertFalse(bool(blocked["allow_execution_pipeline"]))
            self.assertFalse(bool(blocked["real_trading_allowed"]))
            self.assertEqual(str(blocked["reason"]), "kill_switch_env_global")
            self.assertIn("KILL_SWITCH_GLOBAL", list(blocked["active"] or []))
            preflight.assert_not_called()
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_requires_audited_db_arming_source(self) -> None:
        prev_env = self._set_live_env()
        try:
            (gates,) = _reload_modules("engine.runtime.gates")

            with patch.object(gates, "live_trading_preflight", return_value={"ok": True, "reason": "ok"}) as preflight:
                blocked = gates.execution_gate_snapshot(
                    system_state={"state": "LIVE", "mode": "live", "armed": 1},
                    kill_switches={},
                    risk_state_getter=lambda _key, default=None: default,
                )

            self.assertFalse(bool(blocked["allow_execution_pipeline"]))
            self.assertFalse(bool(blocked["real_trading_allowed"]))
            self.assertEqual(str(blocked["reason"]), "mode_live_armed_not_from_audited_db")
            self.assertEqual(str(blocked["armed_source"]), "system_state")
            preflight.assert_not_called()
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_loads_default_db_execution_mode_without_callback(self) -> None:
        prev_env = {
            "EXECUTION_MODE": os.environ.get("EXECUTION_MODE"),
            "ENGINE_MODE": os.environ.get("ENGINE_MODE"),
            "OPERATOR_MODE": os.environ.get("OPERATOR_MODE"),
            "MODE": os.environ.get("MODE"),
        }
        try:
            for key in prev_env:
                os.environ.pop(key, None)
            storage, execution_mode, gates = _reload_modules(
                "engine.runtime.storage",
                "engine.execution.execution_mode",
                "engine.runtime.gates",
            )
            storage.init_db()
            execution_mode.set_execution_mode("paper", actor="test", reason="db_default")

            allowed = gates.execution_gate_snapshot(
                system_state={"state": "LIVE"},
                kill_switches={},
                risk_state_getter=lambda _key, default=None: default,
            )

            self.assertTrue(bool(allowed["allow_execution_pipeline"]))
            self.assertEqual(str(allowed["mode"]), "paper")
            self.assertEqual(str(allowed["reason"]), "mode_paper")
            self.assertEqual(str(allowed["source"]), "default_execution_mode_db")
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_does_not_treat_env_live_as_db_arming(self) -> None:
        prev_env = self._set_live_env()
        try:
            storage, execution_mode, gates = _reload_modules(
                "engine.runtime.storage",
                "engine.execution.execution_mode",
                "engine.runtime.gates",
            )
            storage.init_db()
            execution_mode.set_execution_mode("paper", actor="test", reason="db_paper")

            blocked = gates.execution_gate_snapshot(
                system_state={"state": "LIVE"},
                kill_switches={},
                risk_state_getter=lambda _key, default=None: default,
            )

            self.assertFalse(bool(blocked["real_trading_allowed"]))
            self.assertEqual(str(blocked["mode"]), "paper")
            self.assertEqual(str(blocked["reason"]), "mode_paper")
            self.assertNotEqual(int(blocked.get("armed") or 0), 1)
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_gate_blocks_critical_ingestion_degradation(self) -> None:
        prev_env = self._set_shadow_env()
        try:
            storage, runtime_meta, gates = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.runtime_meta",
                "engine.runtime.gates",
            )
            storage.init_db()
            runtime_meta.meta_set(
                "ingestion_state",
                json.dumps(
                    {
                        "source_health": {
                            "degraded": True,
                            "runtime_reason_codes": ["critical_source_stale:prices"],
                            "advisory_reason_codes": [],
                        }
                    }
                ),
            )

            blocked = gates.execution_gate_snapshot(
                system_state={"state": "DEGRADED", "detail": "critical_source_stale:prices"},
                get_execution_mode_fn=lambda: {"mode": "shadow", "armed": 0},
            )

            self.assertFalse(bool(blocked["allow_execution_pipeline"]))
            self.assertFalse(bool(blocked["allowed"]))
            self.assertEqual(str(blocked["severity"]), "CRITICAL")
            self.assertEqual(str(blocked["reason"]), "runtime_state_degraded")
        finally:
            self._restore_shadow_env(prev_env)

    def test_write_connections_disable_implicit_sqlite_transactions(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect_rw_direct()
        try:
            self.assertIsNone(con.isolation_level)
            con.execute("CREATE TABLE IF NOT EXISTS txn_probe(id INTEGER PRIMARY KEY, value TEXT)")
            con.commit()
        finally:
            con.close()

    def test_manual_write_path_auto_begins_and_commits_single_transaction(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect_rw_direct()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS txn_probe(id INTEGER PRIMARY KEY, value TEXT)")
            con.commit()

            self.assertFalse(bool(con.in_transaction))
            con.execute("INSERT INTO txn_probe(value) VALUES (?)", ("a",))
            self.assertTrue(bool(con.in_transaction))
            con.execute("INSERT INTO txn_probe(value) VALUES (?)", ("b",))
            con.commit()
            self.assertFalse(bool(con.in_transaction))

            rows = con.execute("SELECT value FROM txn_probe ORDER BY id").fetchall()
            self.assertEqual([str(r[0]) for r in rows], ["a", "b"])
        finally:
            con.close()

    def test_nested_split_connection_close_does_not_abort_active_transaction(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS txn_probe(id INTEGER PRIMARY KEY, value TEXT)")
            con.commit()
            con.begin_managed_write()
            con.execute("INSERT INTO txn_probe(value) VALUES (?)", ("a",))

            nested = storage.connect()
            nested.execute("SELECT COUNT(*) FROM txn_probe").fetchone()
            nested.close()

            self.assertTrue(bool(con.in_transaction))
            con.execute("INSERT INTO txn_probe(value) VALUES (?)", ("b",))
            con.commit()

            rows = con.execute("SELECT value FROM txn_probe ORDER BY id").fetchall()
            self.assertEqual([str(r[0]) for r in rows], ["a", "b"])
        finally:
            con.close()

    def test_runtime_meta_writes_reuse_active_transaction(self) -> None:
        storage, runtime_meta = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.runtime_meta",
        )
        con = storage.connect()
        try:
            con.begin_managed_write()
            runtime_meta.meta_set("txn_runtime_meta_probe", "a")
            self.assertTrue(bool(con.in_transaction))
            did_set = runtime_meta.meta_set_if_missing("txn_runtime_meta_probe_once", "b")
            self.assertTrue(did_set)
            con.commit()

            row = con.execute(
                """
                SELECT value FROM runtime_meta
                WHERE key='txn_runtime_meta_probe'
                """
            ).fetchone()
            self.assertEqual(str(row[0]), "a")

            row = con.execute(
                """
                SELECT value FROM runtime_meta
                WHERE key='txn_runtime_meta_probe_once'
                """
            ).fetchone()
            self.assertEqual(str(row[0]), "b")
        finally:
            con.close()

    def test_run_write_txn_suppresses_inner_manual_commit(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect_rw_direct()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS txn_probe(id INTEGER PRIMARY KEY, value TEXT)")
            con.commit()
        finally:
            con.close()

        def _write(db):
            db.execute("INSERT INTO txn_probe(value) VALUES (?)", ("a",))
            db.execute("COMMIT;")
            db.execute("INSERT INTO txn_probe(value) VALUES (?)", ("b",))

        storage.run_write_txn(_write, table="txn_probe", operation="inner_commit_regression")

        con = storage.connect(readonly=True)
        try:
            rows = con.execute("SELECT value FROM txn_probe ORDER BY id").fetchall()
            self.assertEqual([str(r[0]) for r in rows], ["a", "b"])
        finally:
            con.close()

    def test_executescript_stays_inside_transaction_and_rolls_back_on_error(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect_rw_direct()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS txn_probe(id INTEGER PRIMARY KEY, value TEXT)")
            con.commit()
        finally:
            con.close()

        def _write(db):
            db.executescript(
                """
                INSERT INTO txn_probe(value) VALUES ('a');
                INSERT INTO txn_probe(value) VALUES ('b');
                """
            )
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            storage.run_write_txn(_write, table="txn_probe", operation="executescript_rollback")

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT COUNT(*) FROM txn_probe").fetchone()
            self.assertEqual(int(row[0]), 0)
        finally:
            con.close()

    def test_read_error_inside_auto_write_transaction_does_not_rollback_prior_writes(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect_rw_direct()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS txn_probe(id INTEGER PRIMARY KEY, value TEXT)")
            con.commit()

            con.execute("INSERT INTO txn_probe(value) VALUES (?)", ("a",))
            self.assertTrue(bool(con.in_transaction))

            with self.assertRaises(sqlite3.OperationalError):
                con.execute("SELECT value FROM missing_probe")

            self.assertTrue(bool(con.in_transaction))
            con.execute("INSERT INTO txn_probe(value) VALUES (?)", ("b",))
            con.commit()

            rows = con.execute("SELECT value FROM txn_probe ORDER BY id").fetchall()
            self.assertEqual([str(r[0]) for r in rows], ["a", "b"])
        finally:
            con.close()

    def test_run_write_txn_rolls_back_and_does_not_retry_non_busy_errors(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect_rw_direct()
        try:
            con.execute("CREATE TABLE IF NOT EXISTS txn_probe(id INTEGER PRIMARY KEY, value TEXT)")
            con.commit()
        finally:
            con.close()

        attempts = {"count": 0}

        def _write(db):
            attempts["count"] += 1
            db.execute("INSERT INTO txn_probe(value) VALUES (?)", ("x",))
            raise ValueError("non_busy_failure")

        with self.assertRaises(ValueError):
            storage.run_write_txn(_write, attempts=5, table="txn_probe", operation="non_busy_failure")

        self.assertEqual(attempts["count"], 1)

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT COUNT(*) FROM txn_probe").fetchone()
            self.assertEqual(int(row[0]), 0)
        finally:
            con.close()

    def test_run_write_txn_retries_busy_errors_with_bounded_attempts(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")

        attempts = {"count": 0}

        class BusyConnection:
            def __init__(self) -> None:
                self.in_transaction = False

            def begin_managed_write(self):
                attempts["count"] += 1
                err = sqlite3.OperationalError("database is locked")
                err.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise err

            def rollback(self):
                self.in_transaction = False

            def close(self):
                return None

        with patch.object(storage, "connect", return_value=BusyConnection()), patch.object(storage.time, "sleep", return_value=None):
            with self.assertRaises(sqlite3.OperationalError):
                storage.run_write_txn(attempts=3, fn=lambda _db: None, table="txn_probe", operation="busy_retry_bound")

        self.assertEqual(attempts["count"], 3)

    def test_runtime_metrics_drop_busy_writes_without_retries(self) -> None:
        with patch.dict(os.environ, {"RUNTIME_METRICS_BUFFER_ENABLED": "0"}, clear=False):
            metrics_store, metrics = _reload_modules(
                "engine.runtime.metrics_store",
                "engine.runtime.metrics",
            )

            calls = []

            def _busy(_fn, **kwargs):
                calls.append(dict(kwargs))
                err = sqlite3.OperationalError("database is locked")
                err.sqlite_errorcode = sqlite3.SQLITE_BUSY
                raise err

            with patch.object(metrics_store, "init_runtime_metrics_db", return_value=True):
                with patch.object(metrics_store, "run_write_txn", side_effect=_busy):
                    metrics.emit_counter("runtime_reliability_probe", 1)

        self.assertEqual(len(calls), 1)
        self.assertEqual(int(calls[0].get("attempts") or 0), 1)
        self.assertEqual(str(calls[0].get("operation") or ""), "write_runtime_metric")
        self.assertTrue(bool(calls[0].get("direct")))
        self.assertTrue(calls[0].get("maintenance") is False)

    def test_metrics_store_init_uses_direct_lightweight_write(self) -> None:
        (metrics_store,) = _reload_modules("engine.runtime.metrics_store")
        metrics_store._METRICS_DB_READY = False
        metrics_store._METRICS_DB_READY_PATH = ""

        calls = []

        def _capture_run_write_txn(_fn, **kwargs):
            calls.append(dict(kwargs))
            return None

        with patch.object(metrics_store, "_init_db", return_value=None):
            with patch.object(metrics_store, "_metrics_schema_present", return_value=False):
                with patch.object(metrics_store, "run_write_txn", side_effect=_capture_run_write_txn):
                    self.assertTrue(metrics_store.init_runtime_metrics_db())

        self.assertEqual(len(calls), 1)
        self.assertEqual(int(calls[0].get("attempts") or 0), 1)
        self.assertTrue(bool(calls[0].get("direct")))
        self.assertTrue(calls[0].get("maintenance") is False)

    def test_execution_mode_respects_caller_transaction_boundaries(self) -> None:
        storage, execution_mode = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_mode",
        )
        baseline = execution_mode.set_execution_mode("paper", actor="test", reason="baseline")
        self.assertEqual(str(baseline["mode"]), "paper")

        con = storage.connect()
        try:
            con.begin_managed_write()
            execution_mode.set_execution_mode("live", actor="test", reason="outer_txn", con=con)
            self.assertTrue(bool(con.in_transaction))
            con.rollback()
        finally:
            con.close()

        state = execution_mode.get_execution_mode()
        self.assertEqual(str(state["mode"]), "paper")

    def test_execution_mode_getter_skips_schema_write_after_schema_exists(self) -> None:
        _storage, execution_mode = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_mode",
        )
        baseline = execution_mode.set_execution_mode("paper", actor="test", reason="baseline")
        self.assertEqual(str(baseline["mode"]), "paper")

        execution_mode.cache_invalidate_namespace("execution_mode")
        execution_mode._EXECUTION_MODE_SCHEMA_READY_PATH = ""

        with patch.object(
            execution_mode,
            "_ensure_schema",
            side_effect=AssertionError("get_execution_mode must not rewrite schema on a warm database"),
        ):
            state = execution_mode.get_execution_mode()

        self.assertEqual(str(state["mode"]), "paper")

    def test_execution_allowed_for_real_trading_requires_live_runtime_state(self) -> None:
        prev_env = self._set_live_env()
        try:
            storage, lifecycle_state, execution_mode = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.lifecycle_state",
                "engine.execution.execution_mode",
            )
            storage.init_db()
            execution_mode.set_execution_mode("live", actor="test", reason="live_mode")
            with patch.object(
                execution_mode,
                "_assert_live_arming_preflight",
                return_value=None,
            ):
                execution_mode.set_execution_armed(1, actor="test", reason="armed")
            lifecycle_state.set_state(lifecycle_state.DEGRADED, "critical_source_failed:prices")

            allowed, reason, detail = execution_mode.execution_allowed_for_real_trading()

            self.assertFalse(bool(allowed))
            self.assertEqual(str(reason), "runtime_state_degraded")
            self.assertEqual(str((detail.get("runtime_state") or {}).get("state")), lifecycle_state.DEGRADED)
        finally:
            self._restore_shadow_env(prev_env)

    def test_execution_allowed_for_real_trading_blocks_disable_live_execution_truthy(self) -> None:
        prev_env = self._set_live_env()
        try:
            os.environ["DISABLE_LIVE_EXECUTION"] = "on"
            storage, lifecycle_state, execution_mode = _reload_modules(
                "engine.runtime.storage",
                "engine.runtime.lifecycle_state",
                "engine.execution.execution_mode",
            )
            storage.init_db()
            execution_mode.set_execution_mode("live", actor="test", reason="live_mode")
            with patch.object(
                execution_mode,
                "_assert_live_arming_preflight",
                return_value=None,
            ):
                execution_mode.set_execution_armed(1, actor="test", reason="armed")
            lifecycle_state.set_state(lifecycle_state.LIVE, "unit_test_live")

            allowed, reason, detail = execution_mode.execution_allowed_for_real_trading()

            self.assertFalse(bool(allowed))
            self.assertEqual(str(reason), "disable_live_execution_env")
            self.assertEqual(str(detail.get("mode")), "live")
            self.assertEqual(int(detail.get("armed") or 0), 1)
            self.assertEqual(str((detail.get("runtime_state") or {}).get("state")), lifecycle_state.LIVE)
        finally:
            self._restore_shadow_env(prev_env)

    def test_kill_switch_respects_caller_transaction_boundaries(self) -> None:
        storage, kill_switch = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.kill_switch",
        )
        schema_con = storage.connect()
        try:
            kill_switch._ensure_schema(schema_con)
            schema_con.commit()
        finally:
            schema_con.close()

        con = storage.connect()
        try:
            con.begin_managed_write()
            kill_switch.set_kill_switch(
                "global",
                "global",
                1,
                reason="outer_txn",
                actor="test",
                con=con,
            )
            self.assertTrue(bool(con.in_transaction))
            con.rollback()
        finally:
            con.close()

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT enabled FROM kill_switch_state WHERE scope=? AND key=?",
                ("global", "global"),
            ).fetchone()
            self.assertTrue(row is None or int(row[0] or 0) == 0)
        finally:
            con.close()

    def test_kill_switch_snapshot_skips_schema_write_after_schema_exists(self) -> None:
        storage, kill_switch = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.kill_switch",
        )
        schema_con = storage.connect()
        try:
            kill_switch._ensure_schema(schema_con)
            schema_con.commit()
        finally:
            schema_con.close()

        kill_switch._KILL_SWITCH_SCHEMA_READY_PATH = ""

        with patch.object(
            kill_switch,
            "_ensure_schema",
            side_effect=AssertionError("kill_switch.snapshot must not rewrite schema on a warm database"),
        ):
            snapshot = kill_switch.snapshot()

        self.assertIn("state", snapshot)
        self.assertIsInstance(snapshot.get("state"), list)

    def test_alerts_respect_caller_transaction_boundaries(self) -> None:
        storage, alerts = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.alerts",
        )
        alerts.init_alerts_db()

        con = storage.connect()
        try:
            con.begin_managed_write()
            with patch.object(alerts, "publish_event") as publish_mock:
                result = alerts.emit_alert(
                    event_title="txn boundary test",
                    symbol="AAPL",
                    horizon_s=300,
                    expected_z=2.0,
                    confidence=0.9,
                    explain={"model_name": "boundary_model", "model_id": "boundary_model"},
                    con=con,
                    return_details=True,
                )
                self.assertTrue(bool(result["inserted"]))
                self.assertTrue(bool(con.in_transaction))
                publish_mock.assert_not_called()
            con.rollback()
        finally:
            con.close()

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT COUNT(*) FROM alerts").fetchone()
            self.assertEqual(int(row[0]), 0)
        finally:
            con.close()

    def test_standalone_alert_write_does_not_leave_active_transaction(self) -> None:
        storage, alerts = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.alerts",
        )
        result = alerts.emit_alert(
            event_title="standalone boundary test",
            symbol="AAPL",
            horizon_s=300,
            expected_z=2.0,
            confidence=0.9,
            explain={"model_name": "boundary_model", "model_id": "boundary_model"},
            return_details=True,
        )
        self.assertTrue(bool(result["inserted"]))
        self.assertFalse(bool(storage.connect().in_transaction))

    def test_alerts_use_explain_regime_without_recomputing_runtime_regime(self) -> None:
        storage, alerts = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.alerts",
        )
        alerts.init_alerts_db()

        con = storage.connect()
        try:
            con.begin_managed_write()
            with patch.object(alerts, "get_current_regime", side_effect=AssertionError("unexpected regime lookup")):
                with patch.object(alerts, "publish_event"):
                    result = alerts.emit_alert(
                        event_title="regime reuse test",
                        symbol="AAPL",
                        horizon_s=300,
                        expected_z=2.0,
                        confidence=0.9,
                        explain={
                            "model_name": "boundary_model",
                            "model_id": "boundary_model",
                            "regime": "global",
                        },
                        con=con,
                        return_details=True,
                    )
            self.assertTrue(bool(result["inserted"]))
            self.assertTrue(bool(con.in_transaction))
            con.rollback()
        finally:
            con.close()

    def test_position_reconcile_respects_caller_transaction_boundaries(self) -> None:
        storage, position_reconcile = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.position_reconcile",
        )
        schema_con = storage.connect()
        try:
            position_reconcile._ensure_schema(schema_con)
            schema_con.commit()
        finally:
            schema_con.close()

        con = storage.connect()
        try:
            con.begin_managed_write()
            with patch.object(position_reconcile, "_broker_positions", return_value=(False, "boom", [])):
                result = position_reconcile.pre_live_position_reconcile("sim", con=con)
            self.assertFalse(bool(result["ok"]))
            self.assertTrue(bool(con.in_transaction))
            con.rollback()
        finally:
            con.close()

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT COUNT(*) FROM position_reconcile_audit").fetchone()
            self.assertEqual(int(row[0]), 0)
        finally:
            con.close()

    def test_ipc_respects_caller_transaction_boundaries(self) -> None:
        storage, ipc = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ipc",
        )
        schema_con = storage.connect()
        try:
            ipc._ensure_ipc_tables(schema_con)
            schema_con.commit()
        finally:
            schema_con.close()

        con = storage.connect()
        try:
            con.begin_managed_write()
            result = ipc.publish_channel_state("runtime.boundary", {"ok": True}, owner="test", con=con)
            self.assertTrue(bool(result["ok"]))
            self.assertTrue(bool(con.in_transaction))
            con.rollback()
        finally:
            con.close()

    def test_ipc_owned_writes_use_direct_lightweight_transactions(self) -> None:
        storage, ipc = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ipc",
        )
        storage.init_db()

        real_run_write_txn = ipc.run_write_txn
        calls = []

        def _capture_run_write_txn(fn, *args, **kwargs):
            calls.append(dict(kwargs))
            return real_run_write_txn(fn, *args, **kwargs)

        with patch.object(ipc, "run_write_txn", side_effect=_capture_run_write_txn):
            with patch.object(ipc, "emit_counter"):
                with patch.object(ipc, "emit_gauge"):
                    with patch.object(ipc, "trace_event"):
                        state_result = ipc.publish_channel_state("runtime.boundary.owned", {"ok": True}, owner="test")
                        msg_result = ipc.publish_message("runtime.boundary.owned", "state", {"ok": True}, sender="test")

        self.assertTrue(bool(state_result.get("ok")))
        self.assertTrue(bool(msg_result.get("ok")))
        self.assertEqual(len(calls), 2)
        self.assertTrue(all(int(kwargs.get("attempts") or 0) == 1 for kwargs in calls))
        self.assertTrue(all(bool(kwargs.get("direct")) for kwargs in calls))
        self.assertTrue(all(kwargs.get("maintenance") is False for kwargs in calls))

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT COUNT(*) FROM ipc_channels WHERE channel=?", ("runtime.boundary",)).fetchone()
            self.assertEqual(int(row[0]), 0)
        finally:
            con.close()

    def test_ipc_best_effort_busy_drop_does_not_raise(self) -> None:
        storage, ipc = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.ipc",
        )
        storage.init_db()
        busy = sqlite3.OperationalError("database is locked")

        with patch.object(ipc, "run_write_txn", side_effect=busy):
            state_result = ipc.publish_channel_state(
                "runtime.boundary.busy",
                {"ok": True},
                owner="test",
                best_effort=True,
            )
            msg_result = ipc.publish_message(
                "runtime.boundary.busy",
                "state",
                {"ok": True},
                sender="test",
                best_effort=True,
            )

        self.assertFalse(bool(state_result.get("ok")))
        self.assertTrue(bool(state_result.get("dropped")))
        self.assertEqual(str(state_result.get("detail") or ""), "sqlite_busy_best_effort_drop")
        self.assertFalse(bool(msg_result.get("ok")))
        self.assertTrue(bool(msg_result.get("dropped")))
        self.assertEqual(str(msg_result.get("detail") or ""), "sqlite_busy_best_effort_drop")

    def test_high_risk_modules_do_not_issue_raw_transaction_sql(self) -> None:
        pattern = re.compile(r'execute\(\s*["\'](?:BEGIN(?:\s+IMMEDIATE)?|COMMIT|ROLLBACK|END);?["\']')
        targets = [
            REPO_ROOT / "engine" / "execution" / "execution_mode.py",
            REPO_ROOT / "engine" / "execution" / "kill_switch.py",
            REPO_ROOT / "engine" / "execution" / "position_reconcile.py",
            REPO_ROOT / "engine" / "execution" / "broker_apply_orders.py",
            REPO_ROOT / "engine" / "runtime" / "ipc.py",
            REPO_ROOT / "engine" / "strategy" / "clustering.py",
            REPO_ROOT / "engine" / "strategy" / "portfolio.py",
            REPO_ROOT / "engine" / "strategy" / "champion_manager.py",
            REPO_ROOT / "engine" / "strategy" / "model_marketplace.py",
            REPO_ROOT / "engine" / "runtime" / "alerts.py",
        ]
        offenders = []
        for path in targets:
            text = path.read_text(encoding="utf-8")
            if pattern.search(text):
                offenders.append(str(path.relative_to(REPO_ROOT)))
        self.assertEqual(offenders, [])

    def test_concurrent_write_load_has_zero_db_errors(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS load_probe(
                  worker_id INTEGER NOT NULL,
                  seq INTEGER NOT NULL,
                  mode TEXT NOT NULL,
                  PRIMARY KEY(worker_id, seq, mode)
                )
                """
            )
            con.commit()
        finally:
            con.close()

        errors = []
        errors_lock = threading.Lock()

        def _record_error(exc: Exception) -> None:
            with errors_lock:
                errors.append(f"{type(exc).__name__}:{exc}")

        def _manual_worker(worker_id: int) -> None:
            con = storage.connect()
            try:
                for seq in range(25):
                    con.execute(
                        "INSERT INTO load_probe(worker_id, seq, mode) VALUES (?,?,?)",
                        (int(worker_id), int(seq), "manual"),
                    )
                    con.commit()
            except Exception as exc:
                _record_error(exc)
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn_cleanup_issue("test_runtime_reliability_regressions.txn_worker_close", e)

        def _txn_worker(worker_id: int) -> None:
            try:
                for seq in range(25):
                    storage.run_write_txn(
                        lambda db, seq=seq: db.execute(
                            "INSERT INTO load_probe(worker_id, seq, mode) VALUES (?,?,?)",
                            (int(worker_id), int(seq), "txn"),
                        ),
                        table="load_probe",
                        operation="concurrent_load_probe",
                    )
            except Exception as exc:
                _record_error(exc)

        threads = [
            threading.Thread(target=_manual_worker, args=(1,)),
            threading.Thread(target=_manual_worker, args=(2,)),
            threading.Thread(target=_txn_worker, args=(3,)),
            threading.Thread(target=_txn_worker, args=(4,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)

        self.assertEqual(errors, [])

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT COUNT(*) FROM load_probe").fetchone()
            self.assertEqual(int(row[0]), 100)
        finally:
            con.close()

    def test_concurrent_order_claims_use_single_idempotency_row(self) -> None:
        storage, order_idempotency = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.order_idempotency",
        )
        storage.init_db()

        results = []
        errors = []
        result_lock = threading.Lock()
        order = {
            "symbol": "AAPL",
            "action": "BUY",
            "qty": 5.0,
            "order_type": "MARKET",
            "source_alert_id": 101,
        }

        def _worker() -> None:
            con = storage.connect()
            try:
                res = order_idempotency.claim_order_submission(
                    con=con,
                    broker="sim",
                    portfolio_orders_id=77,
                    portfolio_ts_ms=1234567890,
                    order=order,
                )
                with result_lock:
                    results.append(dict(res or {}))
            except Exception as exc:
                with result_lock:
                    errors.append(f"{type(exc).__name__}:{exc}")
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn_cleanup_issue("test_runtime_reliability_regressions.order_claim_close", e)

        threads = [threading.Thread(target=_worker) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10.0)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 12)
        self.assertEqual(sum(1 for row in results if not bool(row.get("duplicate"))), 1)
        self.assertEqual(len({str(row.get("order_uid") or "") for row in results}), 1)
        self.assertEqual(len({str(row.get("client_order_id") or "") for row in results}), 1)

        con = storage.connect(readonly=True)
        try:
            row = con.execute("SELECT COUNT(*) FROM execution_order_idempotency").fetchone()
            self.assertEqual(int(row[0] or 0), 1)
        finally:
            con.close()

    def test_fill_processing_without_fill_id_is_idempotent_under_load(self) -> None:
        storage, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        execution_ledger.init_execution_ledger()

        execution_ledger.log_submit(
            client_order_id="cid-legacy",
            broker="sim",
            symbol="AAPL",
            qty=10.0,
            submit_ts_ms=1000,
            ref_px=100.0,
            source_alert_id=101,
            extra={"model_id": "m1"},
        )

        apply_calls = []
        apply_lock = threading.Lock()

        def _record_fill(*_args, **_kwargs):
            with apply_lock:
                apply_calls.append(1)
            return {"ok": True}

        with patch.object(execution_ledger, "record_live_fill_attribution", side_effect=_record_fill):
            threads = [
                threading.Thread(
                    target=lambda: execution_ledger.log_fill(
                        client_order_id="cid-legacy",
                        fill_ts_ms=1000,
                        fill_qty=10.0,
                        fill_px=100.0,
                        fees=0.25,
                        liquidity="maker",
                        raw={"source": "legacy_probe"},
                    )
                )
                for _ in range(12)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10.0)

        con = storage.connect(readonly=True)
        try:
            fill_row = con.execute(
                """
                SELECT COUNT(*), MIN(fill_id), MAX(fill_id)
                FROM execution_fills
                WHERE client_order_id='cid-legacy'
                """
            ).fetchone()
            state_row = con.execute(
                """
                SELECT net_qty, avg_entry_price, realized_pnl, last_update_ts_ms
                FROM model_position_state
                WHERE model_id='m1' AND symbol='AAPL'
                """
            ).fetchone()
            duplicate_event_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM event_log WHERE event_type='fill_duplicate_ignored'"
                ).fetchone()[0]
                or 0
            )
        finally:
            con.close()

        self.assertEqual(int(fill_row[0] or 0), 1)
        self.assertTrue(str(fill_row[1] or "").startswith("synthetic:"))
        self.assertEqual(str(fill_row[1] or ""), str(fill_row[2] or ""))
        self.assertEqual(len(apply_calls), 1)
        self.assertGreaterEqual(duplicate_event_count, 1)
        self.assertIsNotNone(state_row)
        self.assertAlmostEqual(float(state_row[0] or 0.0), 10.0)
        self.assertAlmostEqual(float(state_row[1] or 0.0), 100.0)
        self.assertAlmostEqual(float(state_row[2] or 0.0), 0.0)
        self.assertEqual(int(state_row[3] or 0), 1000)

        audit = execution_ledger.audit_execution_integrity(model_id="m1")
        self.assertTrue(audit.get("ok"), audit)
        self.assertEqual(int(audit.get("duplicate_fill_count") or 0), 0)
        self.assertEqual(int(audit.get("missing_fill_count") or 0), 0)
        self.assertEqual(int(audit.get("inconsistent_position_count") or 0), 0)

    def test_out_of_order_duplicate_fill_load_preserves_exactly_once_state(self) -> None:
        storage, execution_ledger = _reload_modules(
            "engine.runtime.storage",
            "engine.execution.execution_ledger",
        )
        storage.init_db()
        execution_ledger.init_execution_ledger()

        execution_ledger.log_submit(
            client_order_id="cid-open",
            broker="sim",
            symbol="AAPL",
            qty=10.0,
            submit_ts_ms=1000,
            ref_px=100.0,
            source_alert_id=201,
            extra={"model_id": "m1"},
        )
        execution_ledger.log_submit(
            client_order_id="cid-close",
            broker="sim",
            symbol="AAPL",
            qty=-4.0,
            submit_ts_ms=2000,
            ref_px=110.0,
            source_alert_id=202,
            extra={"model_id": "m1"},
        )

        apply_calls = []
        apply_lock = threading.Lock()

        def _record_fill(*_args, **_kwargs):
            with apply_lock:
                apply_calls.append(1)
            return {"ok": True}

        def _close_fill_worker() -> None:
            execution_ledger.log_fill(
                client_order_id="cid-close",
                fill_id="fill-close",
                broker="sim",
                symbol="AAPL",
                qty=-4.0,
                fill_px=110.0,
                fill_ts_ms=2000,
                fees=0.1,
                extra={"liquidity": "taker"},
            )

        def _open_fill_worker() -> None:
            execution_ledger.log_fill(
                client_order_id="cid-open",
                fill_id="fill-open",
                broker="sim",
                symbol="AAPL",
                qty=10.0,
                fill_px=100.0,
                fill_ts_ms=1000,
                fees=0.1,
                extra={"liquidity": "maker"},
            )

        with patch.object(execution_ledger, "record_live_fill_attribution", side_effect=_record_fill):
            _close_fill_worker()
            threads = [threading.Thread(target=_close_fill_worker) for _ in range(9)]
            threads.append(threading.Thread(target=_open_fill_worker))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10.0)

        con = storage.connect(readonly=True)
        try:
            fill_count = int(
                con.execute("SELECT COUNT(*) FROM execution_fills").fetchone()[0] or 0
            )
            state_row = con.execute(
                """
                SELECT net_qty, avg_entry_price, realized_pnl, last_update_ts_ms
                FROM model_position_state
                WHERE model_id='m1' AND symbol='AAPL'
                """
            ).fetchone()
            duplicate_event_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM event_log WHERE event_type='fill_duplicate_ignored'"
                ).fetchone()[0]
                or 0
            )
        finally:
            con.close()

        self.assertEqual(fill_count, 2)
        self.assertEqual(len(apply_calls), 2)
        self.assertGreaterEqual(duplicate_event_count, 1)
        self.assertIsNotNone(state_row)
        self.assertAlmostEqual(float(state_row[0] or 0.0), 6.0)
        self.assertAlmostEqual(float(state_row[1] or 0.0), 100.0)
        self.assertAlmostEqual(float(state_row[2] or 0.0), 40.0)
        self.assertEqual(int(state_row[3] or 0), 2000)

        audit = execution_ledger.audit_execution_integrity(model_id="m1")
        self.assertTrue(audit.get("ok"), audit)
        self.assertEqual(int(audit.get("duplicate_fill_count") or 0), 0)
        self.assertEqual(int(audit.get("missing_fill_count") or 0), 0)
        self.assertEqual(int(audit.get("inconsistent_position_count") or 0), 0)
        self.assertGreaterEqual(int(audit.get("out_of_order_fill_count") or 0), 1)

    def test_event_log_and_redundant_init_under_load_have_zero_db_errors(self) -> None:
        storage, event_log = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.event_log",
        )
        storage.init_db()

        con = storage.connect_rw_direct()
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS load_probe_batch(
                  worker_id INTEGER NOT NULL,
                  seq INTEGER NOT NULL,
                  batch_no INTEGER NOT NULL,
                  payload TEXT,
                  PRIMARY KEY(worker_id, seq)
                )
                """
            )
            con.commit()
        finally:
            con.close()

        errors = []
        errors_lock = threading.Lock()

        def _record_error(exc: Exception) -> None:
            with errors_lock:
                errors.append(f"{type(exc).__name__}:{exc}")

        def _init_spammer() -> None:
            try:
                for _ in range(60):
                    storage.init_db()
            except Exception as exc:
                _record_error(exc)

        def _event_writer() -> None:
            try:
                for seq in range(120):
                    event_log.append_event(
                        event_type="runtime.load_probe",
                        event_source="tests.test_runtime_reliability_regressions",
                        entity_type="worker",
                        entity_id="event-writer",
                        payload={"seq": int(seq)},
                    )
            except Exception as exc:
                _record_error(exc)

        def _batch_worker(worker_id: int, mode: str) -> None:
            try:
                for batch_no in range(20):
                    rows = [
                        (int(worker_id), int(batch_no * 10 + offset), int(batch_no), str(mode))
                        for offset in range(10)
                    ]

                    if mode == "txn":
                        storage.run_write_txn(
                            lambda db, rows=rows: db.executemany(
                                """
                                INSERT INTO load_probe_batch(worker_id, seq, batch_no, payload)
                                VALUES (?,?,?,?)
                                """,
                                rows,
                            ),
                            table="load_probe_batch",
                            operation=f"batch_worker_{mode}",
                        )
                        continue

                    con_local = storage.connect()
                    try:
                        con_local.executemany(
                            """
                            INSERT INTO load_probe_batch(worker_id, seq, batch_no, payload)
                            VALUES (?,?,?,?)
                            """,
                            rows,
                        )
                        con_local.commit()
                    finally:
                        con_local.close()
            except Exception as exc:
                _record_error(exc)

        def _reader() -> None:
            try:
                for _ in range(120):
                    con_local = storage.connect(readonly=True)
                    try:
                        con_local.execute("SELECT COUNT(*) FROM event_log").fetchone()
                        con_local.execute("SELECT COUNT(*) FROM load_probe_batch").fetchone()
                    finally:
                        con_local.close()
            except Exception as exc:
                _record_error(exc)

        threads = [
            threading.Thread(target=_init_spammer),
            threading.Thread(target=_event_writer),
            threading.Thread(target=_batch_worker, args=(1, "txn")),
            threading.Thread(target=_batch_worker, args=(2, "manual")),
            threading.Thread(target=_reader),
            threading.Thread(target=_reader),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20.0)

        self.assertEqual(errors, [])
        event_log.flush_event_log_buffer(max_batches=64)

        con = storage.connect(readonly=True)
        try:
            event_row = con.execute("SELECT COUNT(*) FROM event_log WHERE event_type=?", ("runtime.load_probe",)).fetchone()
            batch_row = con.execute("SELECT COUNT(*) FROM load_probe_batch").fetchone()
        finally:
            con.close()

        self.assertEqual(int(event_row[0]), 120)
        self.assertEqual(int(batch_row[0]), 400)

        debug = storage.get_connection_debug_snapshot()
        txn_stats = dict(debug.get("txn_stats") or {})
        self.assertEqual(int(txn_stats.get("cannot_commit_count") or 0), 0)
        self.assertIn("busy_retry_count", txn_stats)
        self.assertIn("slow_write_count", txn_stats)

    def test_hot_runtime_tables_have_required_indexes(self) -> None:
        (storage,) = _reload_modules("engine.runtime.storage")
        storage.init_db()

        con = storage.connect(readonly=True)
        try:
            expected = {
                "predictions": {
                    "idx_predictions_ts",
                    "idx_predictions_symbol_ts",
                    "idx_predictions_model_ts",
                },
                "decision_log": {
                    "idx_decision_log_ts",
                    "idx_decision_log_symbol_ts",
                    "idx_decision_log_model_ts",
                },
                "portfolio_orders": {
                    "idx_portfolio_orders_ts",
                    "idx_portfolio_orders_symbol_ts",
                    "idx_portfolio_orders_model_ts",
                },
                "execution_orders": {
                    "idx_execution_orders_submit_ts",
                    "idx_execution_orders_symbol_submit_ts",
                    "idx_execution_orders_model_submit_ts",
                    "idx_execution_orders_broker_order_id",
                    "idx_execution_orders_order_uid",
                },
                "execution_fills": {
                    "idx_execution_fills_ts",
                    "idx_execution_fills_client",
                    "idx_execution_fills_model_ts",
                    "idx_execution_fills_symbol_ts",
                    "idx_execution_fills_fill_id",
                },
                "pnl_attribution": {
                    "idx_pnl_attribution_prediction_ts",
                    "idx_pnl_attribution_ts",
                    "idx_pnl_attribution_model_ts",
                },
            }

            for table, expected_indexes in expected.items():
                rows = con.execute(f"PRAGMA index_list({table})").fetchall()
                names = {str(row[1]) for row in rows}
                self.assertTrue(
                    expected_indexes.issubset(names),
                    msg=f"{table} missing indexes: {sorted(expected_indexes - names)}",
                )
        finally:
            con.close()
