"""Process-local psycopg connection pool for runtime storage."""

from __future__ import annotations

import atexit
import hashlib
import logging
import time

from contextlib import contextmanager
from contextvars import ContextVar
import os
import re
import sys
import threading
from collections.abc import Iterator
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg_pool import ConnectionPool, PoolTimeout

from engine.runtime.platform import default_pg_dsn, dsn_with_pg_password


class StoragePoolTimeout(TimeoutError):
    """Raised when a storage connection cannot be acquired before timeout."""


class StorageReadinessError(ConnectionError):
    """Raised when runtime storage is required but unavailable."""

    def __init__(self, message: str, *, snapshot: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.snapshot = dict(snapshot or {})


_POOL_LOCK = threading.Lock()
_POOL: ConnectionPool | None = None
_POOL_TRANSACTION_MODE = False
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCHEMA_OVERRIDE: ContextVar[str | None] = ContextVar("ts_pg_schema_override", default=None)
_ACQUIRE_TIMEOUT_OVERRIDE: ContextVar[float | None] = ContextVar(
    "ts_pg_acquire_timeout_override",
    default=None,
)
_READINESS_LOCK = threading.Lock()
_READINESS_STATE: dict[str, Any] = {
    "checked": False,
    "ok": None,
    "status": "unknown",
    "storage": "postgres",
    "backend": "postgres",
    "degraded": False,
    "detail": "storage_not_checked",
    "error": "",
    "error_type": "",
    "timeout_s": None,
    "ts_ms": 0,
    "last_ok_ts_ms": 0,
    "last_failure_ts_ms": 0,
}


def _pool_timeout_s() -> float:
    return max(0.05, float(os.environ.get("TS_PG_POOL_TIMEOUT", "5") or 5.0))


def _connect_timeout_s(timeout_s: float | None = None) -> int:
    raw = os.environ.get("TS_PG_CONNECT_TIMEOUT")
    if raw is not None and str(raw).strip():
        value = _coerce_timeout_s(raw, 1.0)
    else:
        value = current_acquire_timeout_s(timeout_s)
    # libpq connect_timeout is integer seconds. Use a minimum of one second so
    # blackholed endpoints still fail promptly instead of using the OS default.
    whole = int(value)
    if float(whole) < float(value):
        whole += 1
    return max(1, whole)


def _failure_cooldown_s() -> float:
    raw = (
        os.environ.get("TS_PG_FAILURE_COOLDOWN_S")
        or os.environ.get("DASHBOARD_STORAGE_FAILURE_COOLDOWN_S")
        or "2.0"
    )
    try:
        return max(0.0, float(raw))
    except Exception:
        return 2.0


def _recent_unavailable_error() -> StoragePoolTimeout | None:
    cooldown_s = _failure_cooldown_s()
    if cooldown_s <= 0:
        return None
    with _READINESS_LOCK:
        snapshot = dict(_READINESS_STATE)
    if not bool(snapshot.get("checked")) or snapshot.get("ok") is not False:
        return None
    failed_ms = int(snapshot.get("last_failure_ts_ms") or snapshot.get("ts_ms") or 0)
    if failed_ms <= 0:
        return None
    age_s = max(0.0, (_now_ms() - failed_ms) / 1000.0)
    if age_s > cooldown_s:
        return None
    detail = str(snapshot.get("error") or snapshot.get("detail") or "postgres_recently_unavailable")
    return StoragePoolTimeout(
        f"postgres_recently_unavailable; retry_after_s={max(0.0, cooldown_s - age_s):.3f}; {detail}"
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _coerce_timeout_s(value: Any, default: float) -> float:
    try:
        return max(0.05, float(value))
    except Exception:
        return max(0.05, float(default))


def current_acquire_timeout_s(timeout_s: float | None = None) -> float:
    if timeout_s is not None:
        return _coerce_timeout_s(timeout_s, _pool_timeout_s())
    override = _ACQUIRE_TIMEOUT_OVERRIDE.get()
    if override is not None:
        return _coerce_timeout_s(override, _pool_timeout_s())
    return _pool_timeout_s()


@contextmanager
def storage_acquire_timeout_override(timeout_s: float | None) -> Iterator[None]:
    if timeout_s is None:
        yield
        return
    token = _ACQUIRE_TIMEOUT_OVERRIDE.set(current_acquire_timeout_s(timeout_s))
    try:
        yield
    finally:
        _ACQUIRE_TIMEOUT_OVERRIDE.reset(token)


def _pool_profile() -> str:
    raw = str(
        os.environ.get("TS_PG_POOL_PROFILE")
        or os.environ.get("TS_PROCESS_ROLE")
        or os.environ.get("ENGINE_PROCESS_ROLE")
        or ""
    ).strip().lower()
    if raw in {"ingest", "ingestion", "market-data", "market_data"}:
        return "ingestion"
    if raw in {"job", "jobs", "worker"}:
        return "jobs"

    job_name = str(os.environ.get("JOB_NAME") or os.environ.get("ENGINE_JOB_NAME") or "").lower()
    if any(part in job_name for part in ("ingest", "stream", "poll_prices", "market")):
        return "ingestion"
    if job_name:
        return "jobs"
    return "application"


def default_pool_size() -> int:
    explicit = str(os.environ.get("TS_PG_POOL_SIZE") or "").strip()
    if explicit:
        return max(1, int(explicit))
    profile = _pool_profile()
    if profile == "ingestion":
        return 8
    if profile == "jobs":
        return 2
    return 4


def _dsn() -> str:
    configured = str(os.environ.get("TS_PG_DSN") or "").strip()
    if configured:
        return dsn_with_pg_password(configured)
    return default_pg_dsn()


def _transaction_pool_mode(conninfo: str | None = None) -> bool:
    explicit = str(os.environ.get("TS_PG_POOL_MODE") or "").strip().lower()
    if explicit:
        return explicit == "transaction"
    dsn = str(conninfo or "").lower()
    if not dsn:
        dsn = str(os.environ.get("TS_PG_DSN") or "").strip().lower()
    if not dsn:
        default_port = "6432" if sys.platform.startswith("linux") else "5432"
        dsn = f"port={os.environ.get('TS_PG_PORT') or default_port}"
    return "port=6432" in dsn or "port='6432'" in dsn


def _redact_dsn(conninfo: str) -> str:
    parts = []
    for part in str(conninfo or "").split():
        if part.lower().startswith("password="):
            parts.append("password=***")
        else:
            parts.append(part)
    return " ".join(parts)


def _env_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _storage_required_by_runtime() -> bool:
    try:
        from engine.runtime.config_schema import get_runtime_safety_context

        safety = get_runtime_safety_context()
        return bool((safety or {}).get("strict_runtime"))
    except Exception:
        env = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip().lower()
        mode = str(os.environ.get("ENGINE_MODE") or "").strip().lower()
        supervised = _env_truthy(os.environ.get("ENGINE_SUPERVISED"))
        return bool(supervised or env in {"prod", "production"} or mode in {"live", "shadow", "paper"})


def _format_storage_error(error: BaseException | None) -> tuple[str, str]:
    if error is None:
        return "", ""
    return type(error).__name__, f"{type(error).__name__}: {error}"


def _set_storage_readiness(
    *,
    ok: bool,
    error: BaseException | None = None,
    timeout_s: float | None = None,
    detail: str = "",
) -> dict[str, Any]:
    now_ms = _now_ms()
    error_type, error_text = _format_storage_error(error)
    with _READINESS_LOCK:
        _READINESS_STATE.update(
            {
                "checked": True,
                "ok": bool(ok),
                "status": "ready" if ok else "unavailable",
                "storage": "postgres",
                "backend": "postgres",
                "degraded": not bool(ok),
                "detail": str(detail or ("ok" if ok else "storage_unavailable")),
                "error": "" if ok else error_text,
                "error_type": "" if ok else error_type,
                "timeout_s": None if timeout_s is None else current_acquire_timeout_s(timeout_s),
                "ts_ms": now_ms,
            }
        )
        if ok:
            _READINESS_STATE["last_ok_ts_ms"] = now_ms
        else:
            _READINESS_STATE["last_failure_ts_ms"] = now_ms
        return dict(_READINESS_STATE)


def storage_readiness_snapshot() -> dict[str, Any]:
    with _READINESS_LOCK:
        state = dict(_READINESS_STATE)
    state["required"] = bool(_storage_required_by_runtime())
    state["ts_ms"] = int(state.get("ts_ms") or 0)
    state["checked"] = bool(state.get("checked"))
    state["ok"] = (bool(state.get("ok")) if state.get("ok") is not None else None)
    state["degraded"] = bool(state.get("degraded") or state.get("ok") is False)
    return state


def is_storage_acquisition_error(error: BaseException) -> bool:
    if isinstance(error, (StoragePoolTimeout, StorageReadinessError, PoolTimeout, psycopg.OperationalError)):
        return True
    name = type(error).__name__.lower()
    text = str(error or "").lower()
    return bool(
        "pooltimeout" in name
        or "storagepooltimeout" in name
        or "couldn't get a connection" in text
        or "connection refused" in text
        or "could not connect" in text
        or "postgres_connect_failed" in text
        or "pg_password" in text
        or "ts_pg_" in text
    )


def storage_unavailable_payload(
    *,
    endpoint: str = "",
    error: BaseException | None = None,
    readiness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = dict(readiness or storage_readiness_snapshot())
    error_type, error_text = _format_storage_error(error)
    detail = str(
        error_text
        or snapshot.get("error")
        or snapshot.get("detail")
        or "runtime_storage_unavailable"
    )
    now_ms = _now_ms()
    return {
        "ok": False,
        "error": "storage_unavailable",
        "detail": detail,
        "endpoint": str(endpoint or ""),
        "storage": {
            "backend": str(snapshot.get("backend") or snapshot.get("storage") or "postgres"),
            "status": str(snapshot.get("status") or "unavailable"),
            "degraded": True,
            "required": bool(snapshot.get("required")),
            "checked": bool(snapshot.get("checked")),
            "detail": str(snapshot.get("detail") or detail),
            "error_type": str(error_type or snapshot.get("error_type") or ""),
            "timeout_s": snapshot.get("timeout_s"),
            "last_checked_ts_ms": int(snapshot.get("ts_ms") or 0),
        },
        "meta": {
            "status": 503,
            "retryable": True,
            "ts_ms": now_ms,
        },
    }


def validate_schema_name(value: str) -> str:
    candidate = str(value or "").strip()
    if not _IDENT_RE.match(candidate):
        raise ValueError(f"invalid_pg_schema:{candidate}")
    return candidate


@contextmanager
def schema_name_override(value: str | None) -> Iterator[None]:
    if value is None:
        yield
        return
    token = _SCHEMA_OVERRIDE.set(validate_schema_name(value))
    try:
        yield
    finally:
        _SCHEMA_OVERRIDE.reset(token)


def schema_name() -> str:
    override = _SCHEMA_OVERRIDE.get()
    if override:
        candidate = override
    elif explicit := str(os.environ.get("TS_PG_SCHEMA") or "").strip():
        candidate = explicit
    elif _env_truthy(os.environ.get("TS_PG_SCHEMA_PER_DB_PATH")) and str(os.environ.get("DB_PATH") or "").strip():
        db_path = os.path.abspath(str(os.environ.get("DB_PATH") or ""))
        digest = hashlib.sha1(db_path.encode("utf-8", errors="ignore")).hexdigest()[:16]
        candidate = f"trading_{digest}"
    else:
        candidate = "trading"
    return validate_schema_name(candidate)


def quote_ident(value: str) -> str:
    text = str(value or "")
    if not _IDENT_RE.match(text):
        raise ValueError(f"invalid_pg_identifier:{text}")
    return '"' + text.replace('"', '""') + '"'


def _rollback_if_in_transaction(conn: psycopg.Connection[Any]) -> None:
    try:
        if conn.info.transaction_status != TransactionStatus.IDLE:
            conn.rollback()
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def _configure_connection(conn: psycopg.Connection[Any]) -> None:
    if _POOL_TRANSACTION_MODE:
        return
    _rollback_if_in_transaction(conn)
    previous_autocommit = bool(conn.autocommit)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET search_path = {quote_ident(schema_name())}, public")
    finally:
        conn.autocommit = previous_autocommit


def get_pool(timeout_s: float | None = None, *, bypass_failure_cooldown: bool = False) -> ConnectionPool:
    global _POOL, _POOL_TRANSACTION_MODE
    conninfo = _dsn()
    transaction_mode = _transaction_pool_mode(conninfo)
    pool_timeout_s = current_acquire_timeout_s(timeout_s)

    with _POOL_LOCK:
        if _POOL is not None:
            return _POOL
        if not bool(bypass_failure_cooldown):
            recent_error = _recent_unavailable_error()
            if recent_error is not None:
                raise recent_error

        _POOL_TRANSACTION_MODE = bool(transaction_mode)

    max_size = default_pool_size()
    min_size = min(max_size, max(1, int(os.environ.get("TS_PG_POOL_MIN_SIZE", "2") or 2)))
    pool = ConnectionPool(
        conninfo=conninfo,
        min_size=min_size,
        max_size=max_size,
        timeout=pool_timeout_s,
        kwargs={
            "autocommit": False,
            "connect_timeout": _connect_timeout_s(pool_timeout_s),
            "prepare_threshold": int(os.environ.get("TS_PG_PREPARE_THRESHOLD", "5") or 5),
        },
        configure=_configure_connection,
        open=False,
    )
    try:
        pool.open(wait=True, timeout=pool_timeout_s)
    except Exception:
        try:
            pool.close(timeout=pool_timeout_s)
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
        raise

    with _POOL_LOCK:
        if _POOL is not None:
            try:
                pool.close(timeout=pool_timeout_s)
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
            return _POOL
        _POOL = pool
        return pool


def acquire(timeout_s: float | None = None, *, bypass_failure_cooldown: bool = False):
    conn = None
    effective_timeout_s = current_acquire_timeout_s(timeout_s)
    try:
        conn = get_pool(
            timeout_s=effective_timeout_s,
            bypass_failure_cooldown=bool(bypass_failure_cooldown),
        ).getconn(timeout=float(effective_timeout_s))
        _configure_connection(conn)
        _set_storage_readiness(ok=True, timeout_s=effective_timeout_s, detail="ok")
        return conn
    except StoragePoolTimeout as exc:
        if "postgres_recently_unavailable" not in str(exc):
            _set_storage_readiness(
                ok=False,
                error=exc,
                timeout_s=effective_timeout_s,
                detail="postgres_pool_timeout",
            )
        raise
    except PoolTimeout as exc:
        wrapped = StoragePoolTimeout(str(exc))
        _set_storage_readiness(
            ok=False,
            error=wrapped,
            timeout_s=effective_timeout_s,
            detail="postgres_pool_timeout",
        )
        raise wrapped from exc
    except Exception as exc:
        _set_storage_readiness(
            ok=False,
            error=exc,
            timeout_s=effective_timeout_s,
            detail="postgres_acquire_failed",
        )
        if conn is not None:
            try:
                get_pool().putconn(conn)
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
        raise


def probe_storage_readiness(
    *,
    timeout_s: float | None = None,
    max_age_s: float | None = None,
    force: bool = False,
) -> dict[str, Any]:
    if not force and max_age_s is not None:
        snapshot = storage_readiness_snapshot()
        ts_ms = int(snapshot.get("ts_ms") or 0)
        if bool(snapshot.get("checked")) and ts_ms > 0 and (_now_ms() - ts_ms) <= int(float(max_age_s) * 1000):
            return snapshot

    effective_timeout_s = current_acquire_timeout_s(timeout_s)
    conn = None
    try:
        conn = acquire(timeout_s=effective_timeout_s, bypass_failure_cooldown=bool(force))
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return _set_storage_readiness(ok=True, timeout_s=effective_timeout_s, detail="ok")
    except Exception as exc:
        return _set_storage_readiness(
            ok=False,
            error=exc,
            timeout_s=effective_timeout_s,
            detail="postgres_readiness_probe_failed",
        )
    finally:
        if conn is not None:
            try:
                release(conn)
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def assert_storage_ready(timeout_s: float | None = None) -> dict[str, Any]:
    snapshot = probe_storage_readiness(timeout_s=timeout_s, force=True)
    if not bool(snapshot.get("ok")):
        detail = str(snapshot.get("error") or snapshot.get("detail") or "runtime_storage_unavailable")
        raise StorageReadinessError(f"runtime_storage_unavailable:{detail}", snapshot=snapshot)
    return snapshot


def release(conn) -> None:
    pool = _POOL
    if pool is None:
        try:
            conn.close()
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
        return
    try:
        _rollback_if_in_transaction(conn)
        pool.putconn(conn)
    except ValueError:
        try:
            conn.close()
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def close_pool() -> None:
    global _POOL
    with _POOL_LOCK:
        pool = _POOL
        _POOL = None
    if pool is not None:
        pool.close(timeout=_pool_timeout_s())


def close_pooled_connections() -> None:
    close_pool()


def pool_snapshot() -> dict[str, Any]:
    pool = _POOL
    return {
        "configured": bool(pool is not None),
        "dsn": _redact_dsn(_dsn()),
        "schema": schema_name(),
        "profile": _pool_profile(),
        "max_size": int(default_pool_size()),
        "timeout_s": float(_pool_timeout_s()),
        "readiness": storage_readiness_snapshot(),
    }


atexit.register(close_pooled_connections)
