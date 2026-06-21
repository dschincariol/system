"""Public runtime storage facade.

Production and production-like modes use configured Postgres storage.  Python
unit tests may opt into the local SQLite backend through ``TS_STORAGE_BACKEND``
so they do not accidentally probe ambient PgBouncer/Postgres.
"""

from __future__ import annotations

import os
import importlib
import logging
from types import ModuleType
from typing import Any, Protocol

from engine.runtime import dbapi_compat as dbapi
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.test_isolation import apply_runtime_test_defaults, running_python_tests


LOGGER = logging.getLogger(__name__)
apply_runtime_test_defaults()


class StorageBackend(Protocol):
    """Runtime storage module contract enforced by the public facade."""

    STORAGE_BACKEND_NAME: str
    SCHEMA_VERSION: int
    DB_PATH: Any

    def init_db(self, schema: str | None = None) -> Any: ...

    def connect(self, readonly: bool = False, **kwargs: Any) -> Any: ...

    def connect_ro(self) -> Any: ...

    def connect_ro_direct(self, **kwargs: Any) -> Any: ...

    def connect_rw_direct(self, **kwargs: Any) -> Any: ...

    def run_write_txn(self, fn, *args: Any, **kwargs: Any) -> Any: ...

    def get_db_validation_snapshot(
        self,
        *,
        include_quick_check: bool = True,
        strict: bool = False,
    ) -> dict[str, Any]: ...

    def get_db_debug_snapshot(self, *, include_quick_check: bool = True) -> dict[str, Any]: ...


_REQUIRED_BACKEND_SYMBOLS: tuple[str, ...] = (
    "STORAGE_BACKEND_NAME",
    "SCHEMA_VERSION",
    "DB_PATH",
    "init_db",
    "connect",
    "connect_ro",
    "connect_ro_direct",
    "connect_rw_direct",
    "connection",
    "execute",
    "fetch_one",
    "fetch_all",
    "run_write_txn",
    "get_db_validation_snapshot",
    "get_db_debug_snapshot",
    "close_pooled_connections",
    "_table_exists",
)


def _validate_backend_module(module: ModuleType, *, expected_name: str) -> StorageBackend:
    missing = [name for name in _REQUIRED_BACKEND_SYMBOLS if not hasattr(module, name)]
    if missing:
        raise RuntimeError(
            "runtime storage backend contract violation: "
            f"{module.__name__} missing {', '.join(sorted(missing))}"
        )
    backend_name = str(getattr(module, "STORAGE_BACKEND_NAME", "") or "").strip().lower()
    if backend_name != str(expected_name).strip().lower():
        raise RuntimeError(
            "runtime storage backend contract violation: "
            f"{module.__name__} declared backend={backend_name or '<unset>'}, expected={expected_name}"
        )
    for name in _REQUIRED_BACKEND_SYMBOLS:
        if name in {"STORAGE_BACKEND_NAME", "SCHEMA_VERSION", "DB_PATH"}:
            continue
        if not callable(getattr(module, name)):
            raise RuntimeError(
                "runtime storage backend contract violation: "
                f"{module.__name__}.{name} must be callable"
            )
    return module  # type: ignore[return-value]


def _publish_backend_symbols(module: ModuleType) -> None:
    exported = tuple(getattr(module, "__all__", ()) or ())
    for name in exported:
        if name.startswith("__"):
            continue
        globals()[str(name)] = getattr(module, str(name))


def _log_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.storage",
        extra=extra or None,
        persist=False,
    )


def _use_sqlite_test_backend() -> bool:
    backend = str(os.environ.get("TS_STORAGE_BACKEND") or "").strip().lower()
    if backend in {"postgres", "pg"}:
        return False
    explicit_sqlite = backend in {"sqlite", "sqlite-test", "test"}
    explicit_test_mode = str(os.environ.get("TS_TESTING") or "").strip().lower() in {"1", "true", "yes", "on"}
    validation_harness = (
        str(os.environ.get("TRADING_VALIDATION_MODE") or "").strip().lower() == "startup"
        and str(os.environ.get("DATA_SOURCE_MANAGER_READ_ONLY") or "").strip().lower() in {"1", "true", "yes", "on"}
        and str(os.environ.get("ENGINE_PRIMARY_BOOTSTRAP_DONE") or "").strip().lower() in {"1", "true", "yes", "on"}
    )
    if explicit_sqlite or explicit_test_mode:
        try:
            from engine.runtime.config_schema import get_runtime_safety_context

            safety = get_runtime_safety_context()
        except Exception:
            safety = {}
        if bool((safety or {}).get("strict_runtime")) and not running_python_tests() and not validation_harness:
            mode = "TS_STORAGE_BACKEND=" + (backend or "<unset>")
            if explicit_test_mode and not explicit_sqlite:
                mode = "TS_TESTING=1"
            raise RuntimeError(
                "SQLite runtime storage backend is forbidden in supervised/prod/live runtime; "
                f"{mode} is test-only and production control-plane state requires Postgres"
            )
    if backend in {"sqlite", "sqlite-test", "test"}:
        return True
    return explicit_test_mode or running_python_tests()


_SQLITE_TEST_BACKEND = _use_sqlite_test_backend()
_BACKEND_NAME = "sqlite" if _SQLITE_TEST_BACKEND else "postgres"

if _SQLITE_TEST_BACKEND:
    from engine.runtime import storage_sqlite as _sqlite_backend
    _sqlite_backend = importlib.reload(_sqlite_backend)
    _BACKEND_MODULE = _validate_backend_module(_sqlite_backend, expected_name="sqlite")
    _publish_backend_symbols(_BACKEND_MODULE)
    _facade_init_db = _BACKEND_MODULE.init_db
    DB_PATH = _sqlite_backend._current_db_path()
else:
    _sqlite_backend = None
    from engine.runtime import storage_pg as _pg_backend
    _BACKEND_MODULE = _validate_backend_module(_pg_backend, expected_name="postgres")
    _publish_backend_symbols(_BACKEND_MODULE)
    _facade_init_db = _BACKEND_MODULE.init_db


def get_active_backend() -> StorageBackend:
    """Return the concrete backend module selected by the facade."""

    return _BACKEND_MODULE


def get_active_backend_name() -> str:
    """Return ``postgres`` or ``sqlite`` for the selected backend."""

    return _BACKEND_NAME


if _SQLITE_TEST_BACKEND:
    _TIMESCALE_MODULE = None
    _FEATURE_STORE_MODULE = None
    _TELEMETRY_MIRROR_MODULE = None
    _TELEMETRY_APPEND_BUFFER_MODULE = None
    _MARKET_FEATURE_STORE_MODULE = None

    def _load_timescale_module():
        global _TIMESCALE_MODULE
        if _TIMESCALE_MODULE is None:
            import engine.runtime.timescale_client as module

            _TIMESCALE_MODULE = module
        return _TIMESCALE_MODULE

    def _load_feature_store_module():
        global _FEATURE_STORE_MODULE
        if _FEATURE_STORE_MODULE is None:
            import engine.strategy.feature_store as module

            _FEATURE_STORE_MODULE = module
        return _FEATURE_STORE_MODULE

    def _load_telemetry_mirror_module():
        global _TELEMETRY_MIRROR_MODULE
        if _TELEMETRY_MIRROR_MODULE is None:
            import engine.runtime.telemetry_mirror as module

            _TELEMETRY_MIRROR_MODULE = module
        return _TELEMETRY_MIRROR_MODULE

    def _load_telemetry_append_buffer_module():
        global _TELEMETRY_APPEND_BUFFER_MODULE
        if _TELEMETRY_APPEND_BUFFER_MODULE is None:
            import engine.runtime.telemetry_append_buffer as module

            _TELEMETRY_APPEND_BUFFER_MODULE = module
        return _TELEMETRY_APPEND_BUFFER_MODULE

    def _load_market_feature_store_module():
        global _MARKET_FEATURE_STORE_MODULE
        if _MARKET_FEATURE_STORE_MODULE is None:
            import engine.data.feature_store as module

            _MARKET_FEATURE_STORE_MODULE = module
        return _MARKET_FEATURE_STORE_MODULE

    def init_timeseries_storage() -> dict:
        timescale = _load_timescale_module().init_timescale_client()
        feature_store = _load_feature_store_module().init_feature_store()
        telemetry_mirror = _load_telemetry_mirror_module().init_telemetry_mirror()
        snapshot = get_timeseries_storage_snapshot()
        snapshot["timescale"] = dict(timescale or {})
        snapshot["feature_store"] = dict(feature_store or {})
        snapshot["telemetry_mirror"] = dict(telemetry_mirror or {})
        return snapshot

    def shutdown_timeseries_storage(timeout_s: float | None = None) -> dict:
        timescale = _load_timescale_module().shutdown_timescale_client(timeout_s=timeout_s)
        try:
            _load_feature_store_module().close_feature_store(timeout_s=timeout_s)
        except Exception:
            LOGGER.debug("sqlite_facade_feature_store_shutdown_failed", exc_info=True)
        try:
            _load_telemetry_mirror_module().shutdown_telemetry_mirror(timeout_s=timeout_s or 2.0)
        except Exception:
            LOGGER.debug("sqlite_facade_telemetry_mirror_shutdown_failed", exc_info=True)
        return {"ok": bool((timescale or {}).get("ok", True)), "timescale": dict(timescale or {})}

    def get_timeseries_storage_snapshot() -> dict:
        timescale = _load_timescale_module().get_timescale_snapshot()
        feature_store = _load_feature_store_module().get_feature_store_snapshot()
        try:
            market_feature_store = _load_market_feature_store_module().get_feature_store_snapshot(
                timescale_snapshot=timescale
            )
        except Exception:
            LOGGER.debug("sqlite_facade_market_feature_snapshot_failed", exc_info=True)
            market_feature_store = {}
        try:
            telemetry_snapshot = _load_telemetry_append_buffer_module().get_telemetry_append_buffer_snapshot()
        except Exception:
            LOGGER.debug("sqlite_facade_telemetry_snapshot_failed", exc_info=True)
            telemetry_snapshot = {}
        try:
            telemetry_mirror = _load_telemetry_mirror_module().get_telemetry_mirror_snapshot()
        except Exception:
            LOGGER.debug("sqlite_facade_telemetry_mirror_snapshot_failed", exc_info=True)
            telemetry_mirror = {}
        components = {
            "timescale": dict(timescale or {}),
            "feature_store": dict(feature_store or {}),
            "market_feature_store": dict(market_feature_store or {}),
            "telemetry_append_buffer": dict(telemetry_snapshot or {}),
            "telemetry_mirror": dict(telemetry_mirror or {}),
        }
        enabled = any(bool(component.get("enabled")) for component in components.values())
        degraded_reasons = [
            f"{name}_not_ok"
            for name, component in components.items()
            if bool(component.get("enabled")) and not bool(component.get("ok", True))
        ]
        return {
            "ok": not bool(degraded_reasons),
            "enabled": bool(enabled),
            "degraded": bool(degraded_reasons),
            "degraded_reasons": degraded_reasons,
            **components,
        }


def init_rl_portfolio_tables(con=None) -> None:
    """Compatibility shim; RL tables are owned by schema migrations."""
    del con
    _facade_init_db()


def init_db(schema: str | None = None):
    result = _facade_init_db(schema)
    if _SQLITE_TEST_BACKEND and _sqlite_backend is not None:
        globals()["DB_PATH"] = _sqlite_backend.DB_PATH
        globals()["PG_LIVENESS_DB_PATH"] = _sqlite_backend.PG_LIVENESS_DB_PATH
        globals()["_SQLITE_LIVENESS_DB_ENABLED"] = _sqlite_backend._SQLITE_LIVENESS_DB_ENABLED
        globals()["_SQLITE_LIVENESS_DB_PATH"] = _sqlite_backend._SQLITE_LIVENESS_DB_PATH
    return result


def table_exists(con, table_name: str) -> bool:
    """Return whether a runtime storage table exists for the active backend."""

    helper = globals().get("_table_exists")
    if not callable(helper):
        raise RuntimeError("runtime storage backend does not expose table existence checks")
    return bool(helper(con, str(table_name)))


if _SQLITE_TEST_BACKEND and _sqlite_backend is not None:
    def _with_facade_sqlite_bindings(fn, *args, **kwargs):
        names = (
            "connect",
            "connect_ro_direct",
            "connect_rw_direct",
            "connect_liveness_ro_direct",
            "connect_liveness_rw_direct",
            "_maybe_quick_check",
            "_maybe_wal_checkpoint",
            "_drain_job_liveness_batch",
            "_flush_job_liveness_batch",
            "_requeue_job_liveness_batch",
            "_ensure_job_liveness_writer_started",
        )
        originals = {name: getattr(_sqlite_backend, name, None) for name in names}
        try:
            for name in names:
                if name in globals():
                    value = globals()[name]
                    if value is globals().get(f"_FACADE_SELF_WRAPPER_{name}"):
                        continue
                    setattr(_sqlite_backend, name, value)
            return fn(*args, **kwargs)
        finally:
            for name, value in originals.items():
                if value is not None:
                    setattr(_sqlite_backend, name, value)

    def run_write_txn(fn, *args, **kwargs):
        return _with_facade_sqlite_bindings(_sqlite_backend.run_write_txn, fn, *args, **kwargs)

    def _sqlite_busy(exc: BaseException) -> bool:
        return dbapi.is_sqlite_error(exc, "OperationalError") and (
            "locked" in str(exc).lower() or "busy" in str(exc).lower()
        )

    def _probe_patched_direct_writer(*, best_effort: bool = False) -> bool:
        connector = globals().get("connect_rw_direct")
        if connector is _sqlite_backend.connect_rw_direct:
            return True
        con = connector()
        try:
            con.begin_managed_write()
            try:
                con.rollback()
            except Exception:
                LOGGER.debug("sqlite_facade_probe_rollback_failed", exc_info=True)
            return True
        except Exception as exc:
            try:
                con.rollback()
            except Exception:
                LOGGER.debug("sqlite_facade_probe_error_rollback_failed", exc_info=True)
            if bool(best_effort) and _sqlite_busy(exc):
                _log_nonfatal("SQLITE_FACADE_PROBE_BUSY_DEGRADED", exc, best_effort=True)
                return False
            raise
        finally:
            try:
                con.close()
            except Exception:
                LOGGER.debug("sqlite_facade_probe_close_failed", exc_info=True)

    def touch_job_lock(job_name: str, owner: str, pid: int, *, best_effort: bool = False) -> None:
        return _with_facade_sqlite_bindings(
            _sqlite_backend.touch_job_lock,
            job_name,
            owner,
            pid,
            best_effort=best_effort,
        )

    def _drain_job_liveness_batch(*args, **kwargs):
        return _sqlite_backend._drain_job_liveness_batch(*args, **kwargs)

    _FACADE_SELF_WRAPPER__drain_job_liveness_batch = _drain_job_liveness_batch

    def _flush_job_liveness_batch(*args, **kwargs):
        previous = getattr(_sqlite_backend._THREAD_LOCAL, "trace_call_path", None)
        _sqlite_backend._THREAD_LOCAL.trace_call_path = "storage.py:_flush_job_liveness_batch"
        try:
            return _sqlite_backend._flush_job_liveness_batch(*args, **kwargs)
        finally:
            if previous is None:
                try:
                    delattr(_sqlite_backend._THREAD_LOCAL, "trace_call_path")
                except AttributeError:
                    LOGGER.debug("sqlite_facade_trace_override_clear_missing", exc_info=True)
            else:
                _sqlite_backend._THREAD_LOCAL.trace_call_path = previous

    _FACADE_SELF_WRAPPER__flush_job_liveness_batch = _flush_job_liveness_batch

    def _requeue_job_liveness_batch(*args, **kwargs):
        return _sqlite_backend._requeue_job_liveness_batch(*args, **kwargs)

    _FACADE_SELF_WRAPPER__requeue_job_liveness_batch = _requeue_job_liveness_batch

    def put_job_heartbeat(
        job_name: str,
        owner: str,
        pid: int,
        extra_json: str | None = None,
        *,
        best_effort: bool = False,
    ) -> None:
        if not _probe_patched_direct_writer(best_effort=best_effort):
            return None
        return _with_facade_sqlite_bindings(
            _sqlite_backend.put_job_heartbeat,
            job_name,
            owner,
            pid,
            extra_json,
            best_effort=best_effort,
        )

    def flush_job_liveness_queue(*, max_batches: int = 8, force: bool = True) -> dict:
        return _with_facade_sqlite_bindings(
            _sqlite_backend.flush_job_liveness_queue,
            max_batches=max_batches,
            force=force,
        )

    def shutdown_job_liveness_queue(*, timeout_s: float = 2.0) -> dict:
        return _with_facade_sqlite_bindings(
            _sqlite_backend.shutdown_job_liveness_queue,
            timeout_s=timeout_s,
        )

    def _job_liveness_writer_loop() -> None:
        return _with_facade_sqlite_bindings(_sqlite_backend._job_liveness_writer_loop)

    def _ensure_job_liveness_writer_started() -> None:
        return _with_facade_sqlite_bindings(_sqlite_backend._ensure_job_liveness_writer_started)

    _FACADE_SELF_WRAPPER__ensure_job_liveness_writer_started = _ensure_job_liveness_writer_started

    def _job_liveness_queue_snapshot() -> dict:
        return _with_facade_sqlite_bindings(_sqlite_backend._job_liveness_queue_snapshot)

    def get_connection_debug_snapshot() -> dict:
        return _with_facade_sqlite_bindings(_sqlite_backend.get_connection_debug_snapshot)


def __getattr__(name: str):
    return getattr(_BACKEND_MODULE, name)


__all__ = sorted(
    {
        *[str(name) for name in getattr(_BACKEND_MODULE, "__all__", ()) if not str(name).startswith("__")],
        "StorageBackend",
        "get_active_backend",
        "get_active_backend_name",
        "init_rl_portfolio_tables",
        "init_db",
        "table_exists",
    }
)
