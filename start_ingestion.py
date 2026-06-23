"""Supervisor-owned ingestion bootstrap wrapper.

This entrypoint normalizes the ingestion process environment, performs a
blocking database repair gate, records basic runtime state, and then delegates
to ``engine.runtime.ingestion_runtime.main`` so ingestion behavior stays
centralized in the canonical runtime module.
"""

import atexit
import json
import logging
import os
import sys
import time
from pathlib import Path

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_local_db_dir, default_local_db_path, default_local_log_dir

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG_DIR = os.path.abspath(
    os.environ.get("TRADING_LOGS") or
    os.environ.get("LOG_DIR") or
    str(default_local_log_dir())
)
_DATA_DIR = os.path.abspath(
    os.environ.get("TRADING_DATA") or
    os.environ.get("DATA_DIR") or
    str(default_local_db_dir())
)


def _current_ingestion_shard():
    from engine.runtime.ingestion_shards import current_ingestion_shard

    return current_ingestion_shard()


def _ingestion_shard_slug() -> str:
    shard = _current_ingestion_shard()
    if not bool(shard.enabled):
        return ""
    return str(shard.label).replace(":", "-")


def _ingestion_pid_path(log_dir: str) -> str:
    slug = _ingestion_shard_slug()
    filename = "ingestion.pid" if not slug else f"ingestion.{slug}.pid"
    return os.path.join(log_dir, filename)


_PID_PATH = _ingestion_pid_path(_LOG_DIR)
LOG = get_logger("start_ingestion")


def _warn_nonfatal(event: str, error: BaseException, **extra) -> None:
    log_failure(
        LOG,
        event=str(event).lower(),
        code=str(event),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="start_ingestion",
        include_health=False,
        persist=True,
        extra=extra or None,
    )


def _strict_runtime_requires_explicit_db_path() -> bool:
    try:
        from engine.runtime.config_schema import get_runtime_safety_context

        return bool(get_runtime_safety_context().get("strict_runtime"))
    except Exception:
        env_raw = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "dev").strip().lower()
        env = "prod" if env_raw in {"prod", "production"} else env_raw
        engine_mode = str(os.environ.get("ENGINE_MODE") or "safe").strip().lower()
        supervised = str(os.environ.get("ENGINE_SUPERVISED") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        explicit_dev_env = bool(str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip()) and env in {
            "dev",
            "test",
        }
        return bool(supervised or env == "prod" or (engine_mode in {"live", "shadow", "paper"} and not explicit_dev_env))


def _bootstrap_ingestion_env() -> None:
    sys.dont_write_bytecode = True
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    os.environ.setdefault("ENGINE_SUPERVISED", "1")
    os.environ.setdefault("ENGINE_LAUNCHED_BY_SUPERVISOR", "1")
    # This entrypoint is always treated as supervisor-owned ingestion, even when
    # launched directly, so downstream jobs inherit consistent runtime identity.
    os.environ.setdefault("ENGINE_JOB_NAME", "ingestion_runtime")
    os.environ.setdefault("ENGINE_PROCESS_ROLE", "ingestion")
    os.environ.setdefault("TS_PG_POOL_PROFILE", "ingestion")

    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(_BASE_DIR, ".env"))
    except Exception as e:
        log_failure(
            LOG,
            event="start_ingestion_dotenv_load_failed",
            code="START_INGESTION_DOTENV_LOAD_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="start_ingestion",
            include_health=False,
            persist=True,
        )

    # Ignore the known-dead local proxy sentinel if present so polling providers can reach upstream APIs.
    dead_local_proxy = "http://127.0.0.1:9"
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        if str(os.environ.get(key, "")).strip().lower() == dead_local_proxy:
            os.environ.pop(key, None)
            os.environ.pop(key.lower(), None)

    os.makedirs(_LOG_DIR, exist_ok=True)
    os.makedirs(_DATA_DIR, exist_ok=True)
    os.environ.setdefault("TRADING_LOGS", _LOG_DIR)
    os.environ.setdefault("TRADING_DATA", _DATA_DIR)
    if not _strict_runtime_requires_explicit_db_path():
        os.environ.setdefault("DB_PATH", str(default_local_db_path().resolve()))
    try:
        from engine.runtime.hardware import apply_cpu_first_runtime_defaults

        apply_cpu_first_runtime_defaults(role="ingestion")
    except Exception as e:
        _warn_nonfatal("START_INGESTION_HARDWARE_DEFAULTS_FAILED", e)

    try:
        from services.data_source_manager import (
            apply_safe_no_credential_runtime_environment,
            safe_no_credential_market_data_mode,
        )

        if safe_no_credential_market_data_mode():
            apply_safe_no_credential_runtime_environment()
    except Exception as e:
        _warn_nonfatal("START_INGESTION_SAFE_ENV_SANITIZE_FAILED", e)


def _read_pid_file_record() -> dict:
    try:
        raw = Path(_PID_PATH).read_text(encoding="utf-8").strip()
        if not raw:
            return {"pid": 0, "owner_pid": 0, "raw": ""}
        if raw.startswith("{"):
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("pid_record_not_object")
            return {
                "pid": int(data.get("pid") or 0),
                "owner_pid": int(data.get("owner_pid") or 0),
                "label": str(data.get("label") or "ingestion"),
                "entry": str(data.get("entry") or ""),
                "base_dir": str(data.get("base_dir") or ""),
                "created_ts_ms": int(data.get("created_ts_ms") or 0),
                "raw": raw,
            }
        return {"pid": int(raw), "owner_pid": 0, "raw": raw}
    except FileNotFoundError:
        sys.stderr.write(f"[start_ingestion] pid_file_missing path={_PID_PATH}\n")
        sys.stderr.flush()
        return {"pid": 0, "owner_pid": 0, "raw": ""}
    except Exception as e:
        _warn_nonfatal("START_INGESTION_PID_READ_FAILED", e, pid_path=str(_PID_PATH))
        return {"pid": 0, "owner_pid": 0, "raw": ""}


def _pid_is_running(pid: int) -> bool:
    try:
        pid = int(pid or 0)
    except Exception:
        pid = 0

    if pid <= 0:
        return False

    try:
        if os.name == "nt":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, 0, int(pid))
            if handle:
                kernel32.CloseHandle(handle)
                return True
            return False

        os.kill(int(pid), 0)
        return True
    except Exception as e:
        _warn_nonfatal("START_INGESTION_PID_RUNNING_CHECK_FAILED", e, pid=int(pid))
        return False


def _parent_pid() -> int:
    try:
        return int(os.getppid() or 0)
    except Exception as e:
        _warn_nonfatal("START_INGESTION_PARENT_PID_READ_FAILED", e)
        return 0


def _write_pid_file() -> None:
    os.makedirs(_LOG_DIR, exist_ok=True)

    record = _read_pid_file_record()
    existing_pid = int(record.get("pid") or 0)
    owner_pid = int(record.get("owner_pid") or 0)
    current_pid = int(os.getpid())
    parent_pid = _parent_pid()
    launched_by_supervisor = str(os.environ.get("ENGINE_LAUNCHED_BY_SUPERVISOR", "0")).strip().lower() in ("1", "true", "yes", "on")

    # Under `start_system` supervision the parent already controls process
    # lifetime and `ingestion_runtime` still enforces a DB-backed job lock. The
    # child pid file is only advisory in that mode, so always allow the current
    # supervised child to replace it instead of fighting sibling restart races.
    if not launched_by_supervisor and existing_pid > 0 and existing_pid != current_pid:
        if owner_pid > 0 and not _pid_is_running(owner_pid):
            pass
        elif _pid_is_running(existing_pid):
            raise RuntimeError(f"ingestion_pid_already_active:{_PID_PATH}:{existing_pid}")

    payload = {
        "pid": current_pid,
        "owner_pid": parent_pid if launched_by_supervisor and parent_pid > 0 else current_pid,
        "label": "ingestion",
        "entry": "start_ingestion.py",
        "base_dir": str(_BASE_DIR),
        "created_ts_ms": int(time.time() * 1000),
        "shard": _current_ingestion_shard().as_dict(),
    }
    Path(_PID_PATH).write_text(
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )


def _cleanup_pid_file() -> None:
    try:
        record = _read_pid_file_record()
        owner_pid = int(record.get("owner_pid") or 0)
        current_pid = int(os.getpid())

        if owner_pid > 0 and owner_pid != current_pid:
            return
        if os.path.exists(_PID_PATH):
            os.remove(_PID_PATH)
    except Exception as e:
        log_failure(
            LOG,
            event="start_ingestion_cleanup_pid_failed",
            code="START_INGESTION_CLEANUP_PID_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="start_ingestion",
            include_health=False,
            persist=True,
            extra={"pid_path": str(_PID_PATH)},
        )


def _write_ingestion_state(payload: dict) -> None:
    try:
        from engine.runtime.ingestion_shards import current_ingestion_shard, ingestion_state_key
        from engine.runtime.runtime_meta import meta_set

        shard = current_ingestion_shard()
        enriched = dict(payload or {})
        enriched.setdefault("shard", shard.as_dict())
        meta_set(
            ingestion_state_key("ingestion_state", shard),
            json.dumps(enriched, separators=(",", ":"), sort_keys=True),
        )
    except Exception as e:
        log_failure(
            LOG,
            event="start_ingestion_write_state_failed",
            code="START_INGESTION_WRITE_STATE_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="start_ingestion",
            include_health=False,
            persist=True,
            extra={"payload": dict(payload or {})},
        )


def _assert_ingestion_tuning_safe() -> dict:
    try:
        from engine.runtime.ingestion_tuning import assert_ingestion_tuning_safe

        return assert_ingestion_tuning_safe(pg_pool_role="ingestion")
    except Exception as e:
        log_failure(
            LOG,
            event="start_ingestion_tuning_unsafe",
            code="START_INGESTION_TUNING_UNSAFE",
            message=str(e),
            error=e,
            level=logging.ERROR,
            component="start_ingestion",
            include_health=True,
            persist=True,
        )
        raise


def _log_runtime_hardware_bootstrap() -> None:
    try:
        from engine.runtime.hardware import log_runtime_hardware_diagnostics

        log_runtime_hardware_diagnostics(LOG, component="start_ingestion")
    except Exception as e:
        if e.__class__.__name__ == "AccelerationProfileError":
            raise
        _warn_nonfatal("START_INGESTION_RUNTIME_HARDWARE_DIAGNOSTICS_FAILED", e)


def main():
    """Bootstrap supervised ingestion and transfer control to the runtime.

    Returns
    -------
    object
        Whatever ``engine.runtime.ingestion_runtime.main`` returns.

    Raises
    ------
    Exception
        Propagates database repair or runtime import failures so supervisors do
        not continue with a partially initialized ingestion process.

    Notes
    -----
    Database repair is a hard gate. If the repair step reports ``ok=False`` or
    raises, the wrapper aborts before marking ingestion as running.

    Side Effects
    ------------
    Sets supervisor-oriented environment variables, writes an ingestion PID
    file, persists a boot-state snapshot, and registers process-exit cleanup.
    """
    _bootstrap_ingestion_env()
    _log_runtime_hardware_bootstrap()
    _assert_ingestion_tuning_safe()
    atexit.register(_cleanup_pid_file)
    _write_pid_file()

    try:
        from engine.runtime.db_repair import repair
        repair_result = repair(startup_fast_path=True)
        if isinstance(repair_result, dict) and not repair_result.get("ok"):
            raise RuntimeError(repair_result)
    except Exception as e:
        print("DB_REPAIR_FAILED:", e)
        raise

    _write_ingestion_state({
        "running": False,
        "entry": "start_ingestion.py",
        "pid": int(os.getpid()),
        "last_event_ts_ms": int(time.time() * 1000),
        "provider_status": "booting",
        "last_error": "",
        "lag_ms": 0,
        "ts_ms": int(time.time() * 1000),
    })

    # After env/bootstrap/DB repair, hand off to the canonical ingestion runtime.
    # This wrapper should stay thin so ingestion behavior remains centralized.
    from engine.runtime.ingestion_runtime import main as _run
    return _run()


if __name__ == "__main__":
    raise SystemExit(main())
