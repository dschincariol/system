"""Process-local defaults for Python test runners.

These helpers are intentionally inert outside pytest/unittest.  They keep unit
tests from accidentally bootstrapping production Postgres, PgBouncer, metrics,
or system credential paths before pytest fixtures have a chance to run.
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable

_HOOK_INSTALLED = False
_CLEANUP_ACTIVE = False
_BASE_TEST_ENV: dict[str, str] | None = None
_DEFAULT_ENV_KEYS = {
    "TS_TESTING": "1",
    "TS_STORAGE_BACKEND": "sqlite",
    "TS_CREDENTIAL_AUDIT_ENABLED": "0",
    "TS_CREDENTIAL_AUDIT_TIMEOUT_S": "0.05",
    "TS_PG_POOL_TIMEOUT": "0.1",
    "TS_PG_CONNECT_TIMEOUT": "1",
}
_PRODUCTION_BACKEND_TEST_FLAGS = (
    "TS_PRODUCTION_BACKEND_TESTS",
    "TRADING_PRODUCTION_BACKEND_TESTS",
)
_PRODUCTION_BACKEND_ENV_PRESERVE = {
    "LIVE_CACHE_BACKEND",
    "LIVE_CACHE_REDIS_URL",
    "REDIS_URL",
    "TS_PG_DSN",
    "TS_PG_PASSWORD",
    "TS_PG_PASSWORD_APP",
    "TS_PG_APP_PASSWORD",
    "TS_REDIS_URL",
    "TS_STORAGE_BACKEND",
}
_SCRUB_ENV_NAMES = {
    "ALLOW_TRAINING",
    "ASYNC_PRICE_WRITER_ENABLED",
    "CREDENTIALS_DIRECTORY",
    "DASHBOARD_API_TOKEN",
    "DATA_SOURCE_MASTER_KEY",
    "DATA_SOURCE_MASTER_KEY_FILE",
    "DISABLE_LIVE_EXECUTION",
    "ENGINE_MODE",
    "ENGINE_RUNTIME_MODE",
    "ENGINE_SUPERVISED",
    "ENV",
    "EXECUTION_MODE",
    "LIVE_CACHE_BACKEND",
    "LIVE_CACHE_REDIS_URL",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_BUCKET",
    "OBJECT_STORE_ENDPOINT",
    "OBJECT_STORE_SECRET_KEY",
    "OBJECT_STORE_SESSION_TOKEN",
    "PG_DSN",
    "PGPASSWORD",
    "POLYGON_API_KEY",
    "POLYGON_KEY",
    "PRICE_READ_BACKEND",
    "PRICE_ROUTER_SQLITE_PRICES_ENABLED",
    "PRICE_ROUTER_SQLITE_QUOTES_ENABLED",
    "PRICE_ROUTER_SQLITE_RAW_ENABLED",
    "PRICE_ROUTER_SQLITE_WRITE_ENABLED",
    "PROD_LOCK",
    "REDIS_CACHE_URL",
    "REDIS_URL",
    "RUNTIME_MODE",
    "SQLITE_LIVENESS_DB_ENABLED",
    "SQLITE_LIVENESS_DB_PATH",
    "SQLITE_LIVENESS_QUEUE_ENABLED",
    "TELEMETRY_APPEND_BUFFER_ENABLED",
    "TELEMETRY_READ_BACKEND",
    "TIMESCALE_DSN",
    "TIMESCALE_ENABLED",
    "TIMESCALE_PRICES_DSN",
    "TIMESCALE_PRICES_ENABLED",
    "TRADIER_API_TOKEN",
    "TRADING_DATA",
    "TRADING_LOGS",
    "TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN",
    "TS_DEV_SECRETS_DIR",
    "TS_ENV",
    "TS_PG_DSN",
    "TS_PG_PASSWORD",
    "TS_PG_PASSWORD_APP",
    "TS_PG_APP_PASSWORD",
    "TS_PG_PASSWORD_INGEST",
    "TS_PG_INGEST_PASSWORD",
    "TS_PG_PASSWORD_READER",
    "TS_PG_READER_PASSWORD",
    "TS_REDIS_URL",
    "TS_RUNTIME_MODE",
    "TS_SECRETS_PROVIDER",
}
_SCRUB_ENV_PREFIXES = (
    "ALPACA_",
    "ANTHROPIC_",
    "AWS_",
    "AZURE_",
    "FINNHUB_",
    "FRED_",
    "GOOGLE_API_",
    "IBKR_",
    "KILL_SWITCH",
    "NEWSAPI_",
    "OPENAI_",
    "OPENWEATHER_",
    "PREFLIGHT_REQUIRE_",
    "QUANDL_",
    "STOCKTWITS_",
    "TWITTER_",
    "X_API_",
)


def running_python_tests() -> bool:
    argv = " ".join(str(part or "") for part in sys.argv).lower()
    return bool(
        "pytest" in argv
        or "unittest" in argv
        or "discover" in argv
        or "tests/" in argv
        or " tests" in argv
    )


def _test_root() -> Path:
    root = Path(tempfile.gettempdir()) / f"trading-system-tests-{os.getpid()}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_default_test_secrets(root: Path) -> Path:
    secrets = root / "secrets"
    secrets.mkdir(parents=True, exist_ok=True)
    for name, value in {
        "data_source_master_key": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
        "master_key": "test-master-key",
        "pg_password_app": "test-app-password",
        "pg_password_ingest": "test-ingest-password",
        "pg_password_reader": "test-reader-password",
    }.items():
        (secrets / name).write_text(value, encoding="utf-8")
    return secrets


def _should_scrub_env_key(key: str) -> bool:
    key_s = str(key or "").strip()
    return bool(key_s in _SCRUB_ENV_NAMES or any(key_s.startswith(prefix) for prefix in _SCRUB_ENV_PREFIXES))


def _env_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _production_backend_tests_enabled() -> bool:
    return any(_env_truthy(os.environ.get(name)) for name in _PRODUCTION_BACKEND_TEST_FLAGS)


def _build_base_test_env() -> dict[str, str]:
    root = _test_root()
    production_backend_tests = _production_backend_tests_enabled()
    env = {
        key: value
        for key, value in os.environ.items()
        if (production_backend_tests and key in _PRODUCTION_BACKEND_ENV_PRESERVE)
        or not _should_scrub_env_key(key)
    }
    defaults = dict(_DEFAULT_ENV_KEYS)
    if production_backend_tests:
        defaults["TS_STORAGE_BACKEND"] = "postgres"
        for key in ("TS_PG_POOL_TIMEOUT", "TS_PG_CONNECT_TIMEOUT"):
            if os.environ.get(key):
                defaults[key] = str(os.environ[key])
    env.update(defaults)
    env["DB_PATH"] = str(root / "runtime-test.sqlite")
    env["TS_SECRETS_PROVIDER"] = "plaintext"
    env["TS_DEV_SECRETS_DIR"] = str(_write_default_test_secrets(root))
    env.pop("TS_ENV", None)
    return {str(key): str(value) for key, value in env.items()}


def _restore_env(env: dict[str, str]) -> None:
    preserve = {key: value for key, value in os.environ.items() if key == "PYTEST_CURRENT_TEST"}
    os.environ.clear()
    os.environ.update({str(key): str(value) for key, value in dict(env or {}).items()})
    os.environ.update(preserve)


def _call_loaded(module_name: str, func_name: str, *args: Any, **kwargs: Any) -> None:
    module = sys.modules.get(str(module_name))
    if module is None:
        return
    func = getattr(module, str(func_name), None)
    if not callable(func):
        return
    try:
        func(*args, **kwargs)
    except Exception:
        return


def _call_loaded_custom(module_name: str, callback: Callable[[Any], None]) -> None:
    module = sys.modules.get(str(module_name))
    if module is None:
        return
    try:
        callback(module)
    except Exception:
        return


def _reset_storage_sqlite_state() -> None:
    module = sys.modules.get("engine.runtime.storage_sqlite")
    if module is None:
        return
    try:
        stop = getattr(module, "_SQLITE_LIVENESS_STOP", None)
        lock = getattr(module, "_SQLITE_LIVENESS_LOCK", None)
        thread = getattr(module, "_SQLITE_LIVENESS_THREAD", None)
        if stop is not None:
            stop.set()
        if lock is not None:
            with lock:
                lock.notify_all()
        if (
            thread is not None
            and getattr(thread, "is_alive", lambda: False)()
            and thread is not threading.current_thread()
        ):
            thread.join(timeout=0.5)
        if thread is None or not getattr(thread, "is_alive", lambda: False)():
            setattr(module, "_SQLITE_LIVENESS_THREAD", None)
    except Exception:
        pass  # no-op-guard: allow - test cleanup must not mask the next case.
    try:
        module.close_pooled_connections()
    except Exception:
        pass  # no-op-guard: allow - test cleanup must not mask the next case.
    lock = getattr(module, "_SQLITE_LIVENESS_LOCK", None)
    try:
        if lock is not None:
            with lock:
                pending = getattr(module, "_SQLITE_LIVENESS_PENDING", None)
                if isinstance(pending, dict):
                    pending.clear()
                last = getattr(module, "_SQLITE_LIVENESS_LAST_PERSIST_MS", None)
                if isinstance(last, dict):
                    last.clear()
                state = getattr(module, "_SQLITE_LIVENESS_STATE", None)
                if isinstance(state, dict):
                    state.update(
                        {
                            "pending_count": 0,
                            "pending": 0,
                            "last_error": "",
                            "last_error_ts_ms": 0,
                        }
                    )
    except Exception:
        pass  # no-op-guard: allow - stale liveness state is best-effort cleanup.
    try:
        setattr(module, "_THREAD_LOCAL", threading.local())
        setattr(module, "_WRITE_LOCK", threading.RLock())
        initialized = getattr(module, "_INITIALIZED_PATHS", None)
        if isinstance(initialized, set):
            initialized.clear()
        current_db_path = getattr(module, "_current_db_path", None)
        if callable(current_db_path):
            current_db_path()
    except Exception:
        pass  # no-op-guard: allow - reload-era lock reset is defensive only.


def _sync_loaded_storage_paths() -> None:
    sqlite_module = sys.modules.get("engine.runtime.storage_sqlite")
    if sqlite_module is not None:
        try:
            current_db_path = getattr(sqlite_module, "_current_db_path", None)
            if callable(current_db_path):
                current_db_path()
        # system-audit: ignore[silent_except] no-op-guard: allow test cleanup must not mask the next case.
        except Exception:
            pass
    storage_module = sys.modules.get("engine.runtime.storage")
    if storage_module is not None and sqlite_module is not None:
        try:
            setattr(storage_module, "DB_PATH", getattr(sqlite_module, "DB_PATH"))
            setattr(storage_module, "PG_LIVENESS_DB_PATH", getattr(sqlite_module, "PG_LIVENESS_DB_PATH"))
            setattr(
                storage_module,
                "_SQLITE_LIVENESS_DB_PATH",
                getattr(sqlite_module, "_SQLITE_LIVENESS_DB_PATH"),
            )
            setattr(
                storage_module,
                "_SQLITE_LIVENESS_DB_ENABLED",
                getattr(sqlite_module, "_SQLITE_LIVENESS_DB_ENABLED"),
            )
        # system-audit: ignore[silent_except] no-op-guard: allow test cleanup must not mask the next case.
        except Exception:
            pass


def _clear_state_cache() -> None:
    module = sys.modules.get("engine.runtime.state_cache")
    cache = getattr(module, "_CACHE", None) if module is not None else None
    if cache is None:
        return
    lock = getattr(cache, "_lock", None)
    try:
        if lock is not None:
            with lock:
                data = getattr(cache, "_data", None)
                load_locks = getattr(cache, "_load_locks", None)
                if isinstance(data, dict):
                    data.clear()
                if isinstance(load_locks, dict):
                    load_locks.clear()
        else:
            data = getattr(cache, "_data", None)
            load_locks = getattr(cache, "_load_locks", None)
            if isinstance(data, dict):
                data.clear()
            if isinstance(load_locks, dict):
                load_locks.clear()
    except Exception:
        return


def _clear_mapping_attr(module_name: str, attr_name: str, replacement: dict[str, Any]) -> None:
    module = sys.modules.get(str(module_name))
    if module is None:
        return
    mapping = getattr(module, str(attr_name), None)
    if not isinstance(mapping, dict):
        return
    lock = getattr(module, f"{attr_name}_LOCK", None)
    try:
        if lock is not None:
            with lock:
                mapping.clear()
                mapping.update(dict(replacement))
        else:
            mapping.clear()
            mapping.update(dict(replacement))
    except Exception:
        return


def _reset_telemetry_append_buffer_state() -> None:
    module = sys.modules.get("engine.runtime.telemetry_append_buffer")
    if module is None:
        return
    try:
        lock = getattr(module, "_BUFFER_LOCK", None)
        table_order = tuple(getattr(module, "_TABLE_ORDER", ()) or ())
        if lock is not None:
            with lock:
                pending = getattr(module, "_BUFFER_PENDING", None)
                if isinstance(pending, dict):
                    for table in table_order:
                        pending[str(table)] = []
                state = getattr(module, "_BUFFER_STATE", None)
                empty_counters = getattr(module, "_empty_table_counters", lambda: {})()
                if isinstance(state, dict):
                    state.clear()
                    state.update(
                        {
                            "accepted_rows": 0,
                            "buffered_rows": 0,
                            "dropped_rows": 0,
                            "flush_batches": 0,
                            "flushed_rows": 0,
                            "last_enqueue_ts_ms": 0,
                            "last_flush_ts_ms": 0,
                            "last_error": "",
                            "last_error_ts_ms": 0,
                            "last_rejected_reason": "",
                            "last_rejected_table": "",
                            "last_rejected_ts_ms": 0,
                            "accepted_by_table": dict(empty_counters),
                            "dropped_by_table": dict(empty_counters),
                            "flushed_by_table": dict(empty_counters),
                        }
                    )
                stop = getattr(module, "_BUFFER_STOP", None)
                if stop is not None:
                    stop.clear()
        price_state = getattr(module, "_PRICE_PROVIDER_STATE", None)
        price_lock = getattr(module, "_PRICE_PROVIDER_STATE_LOCK", None)
        if isinstance(price_state, dict):
            if price_lock is not None:
                with price_lock:
                    price_state.clear()
            else:
                price_state.clear()
        current_key = getattr(module, "_current_buffer_db_path_key", None)
        if callable(current_key):
            setattr(module, "_BUFFER_DB_PATH_KEY", str(current_key()))
    except Exception:
        return


def _clear_loaded_caches() -> None:
    _call_loaded("engine.data._credentials", "clear_data_credential_cache")
    _clear_state_cache()
    _call_loaded("engine.data.price_cache", "clear_price_cache")
    _call_loaded("engine.data.feature_store", "clear_feature_cache")
    _call_loaded("engine.runtime.live_cache", "close_live_cache")
    _call_loaded("engine.runtime.model_cache", "invalidate_model_catalog")
    _call_loaded("engine.api.api_jobs", "_clear_jobs_cache")
    _clear_mapping_attr("engine.api.api_system", "_HEALTH_CACHE", {"ts_ms": 0, "payload": None})
    _clear_mapping_attr("engine.api.api_system", "_SYSTEM_SNAPSHOT_CACHE", {"ts_ms": 0, "payload": None})
    _clear_mapping_attr("engine.runtime.health", "_HEALTH_SNAPSHOT_CACHE", {"ts_ms": 0, "payload": None})
    _clear_mapping_attr("engine.runtime.health", "_PREFLIGHT_CACHE", {})
    _clear_mapping_attr("engine.runtime.price_migration_validation", "_VALIDATION_CACHE", {"ts_ms": 0, "snapshot": None})
    _clear_mapping_attr(
        "engine.runtime.telemetry_migration_validation",
        "_VALIDATION_CACHE",
        {"ts_ms": 0, "snapshot": None},
    )
    _call_loaded_custom(
        "engine.inference_engine",
        lambda module: (
            getattr(module, "_ARTIFACT_CACHE").clear(),
            setattr(module, "_ARTIFACT_CACHE_DB_PATH", ""),
        ),
    )
    _call_loaded_custom(
        "engine.model_registry",
        lambda module: (
            getattr(module, "_TRACKING_MODEL_CACHE").clear(),
            setattr(module, "_MODEL_REGISTRY_READY_PATH", ""),
        ),
    )
    _call_loaded_custom(
        "engine.runtime.startup_gates",
        lambda module: getattr(module, "_DIR_PROBE_CACHE").clear(),
    )


def cleanup_runtime_test_state(*, timeout_s: float = 0.5) -> None:
    global _CLEANUP_ACTIVE
    if not running_python_tests() or _CLEANUP_ACTIVE:
        return
    _CLEANUP_ACTIVE = True
    try:
        _call_loaded_custom(
            "engine.prediction_logger",
            lambda module: getattr(module, "_TRACKING_SINK").abort(timeout_s=timeout_s),
        )
        _call_loaded("engine.regime_detector", "shutdown_regime_detector", timeout_s=timeout_s)
        _call_loaded("engine.model_scoring", "stop_model_scoring_service", timeout_s=timeout_s)
        _call_loaded("engine.runtime.async_writer", "shutdown_async_writer", timeout_s=timeout_s)
        _call_loaded("engine.runtime.event_bus", "shutdown_event_bus")
        _call_loaded("engine.runtime.event_log", "shutdown_event_log_buffer", timeout_s=timeout_s)
        _call_loaded("engine.runtime.telemetry_append_buffer", "shutdown_telemetry_append_buffers", timeout_s=timeout_s)
        _reset_telemetry_append_buffer_state()
        _call_loaded("engine.runtime.metrics_store", "shutdown_runtime_metrics_buffer", timeout_s=timeout_s)
        _call_loaded("engine.runtime.runtime_meta", "shutdown_best_effort_runtime_meta_buffer", timeout_s=timeout_s)
        _call_loaded("engine.strategy.feature_store", "close_feature_store", timeout_s=timeout_s)
        _call_loaded("engine.runtime.storage", "shutdown_job_liveness_queue", timeout_s=timeout_s)
        _call_loaded("engine.runtime.storage", "close_pooled_connections")
        _reset_storage_sqlite_state()
        _clear_loaded_caches()
    finally:
        _CLEANUP_ACTIVE = False


def _base_env() -> dict[str, str]:
    global _BASE_TEST_ENV
    if _BASE_TEST_ENV is None:
        _BASE_TEST_ENV = _build_base_test_env()
    return dict(_BASE_TEST_ENV)


def reset_runtime_test_env() -> None:
    if not running_python_tests():
        return
    _restore_env(_base_env())
    _sync_loaded_storage_paths()


def install_unittest_test_isolation() -> None:
    global _HOOK_INSTALLED
    if _HOOK_INSTALLED or not running_python_tests():
        return
    try:
        import unittest
    except Exception:
        return
    original_run = unittest.TestCase.run
    if getattr(original_run, "_ts_isolated", False):
        _HOOK_INSTALLED = True
        return

    def _isolated_run(self, result=None):  # type: ignore[no-untyped-def]
        cleanup_runtime_test_state(timeout_s=0.5)
        reset_runtime_test_env()
        try:
            return original_run(self, result)
        finally:
            cleanup_runtime_test_state(timeout_s=0.5)
            reset_runtime_test_env()

    setattr(_isolated_run, "_ts_isolated", True)
    unittest.TestCase.run = _isolated_run
    _HOOK_INSTALLED = True


def apply_runtime_test_defaults() -> None:
    if not running_python_tests():
        return
    for key, value in _DEFAULT_ENV_KEYS.items():
        os.environ.setdefault(key, value)

    root = _test_root()
    os.environ.setdefault("DB_PATH", str(root / "runtime-test.sqlite"))

    if not os.environ.get("TS_SECRETS_PROVIDER"):
        secrets = _write_default_test_secrets(root)
        os.environ["TS_SECRETS_PROVIDER"] = "plaintext"
        os.environ["TS_DEV_SECRETS_DIR"] = str(secrets)
    os.environ.pop("TS_ENV", None)
    install_unittest_test_isolation()
