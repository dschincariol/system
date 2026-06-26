"""Top-level supervised runtime bootstrap for the trading system.

This entrypoint owns environment bootstrapping, startup validation, dashboard
binding, ingestion supervision, shutdown hooks, and the main process lifecycle
for the local/service runtime.
"""

import atexit
import hashlib
import json
import logging
import os
import py_compile
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

from engine.runtime.platform import (
    default_local_db_dir,
    default_local_db_path,
    default_local_log_dir,
    resolve_runtime_paths,
)
from engine.startup.env import (
    append_env_line as _startup_append_env_line,
    ensure_local_env_file as _startup_ensure_local_env_file,
    ensure_local_secret_file as _startup_ensure_local_secret_file,
    env_bool as _startup_env_bool,
    env_file_has_nonempty_value as _startup_env_file_has_nonempty_value,
    env_float as _startup_env_float,
    env_int as _startup_env_int,
    strict_runtime_requires_explicit_db_path as _startup_strict_runtime_requires_explicit_db_path,
)
from engine.startup.mode import pick_mode_from_argv_or_env as _startup_pick_mode_from_argv_or_env
from engine.startup.phase import record_first_failure as _startup_record_first_failure
from engine.startup.phase import record_phase as _startup_record_phase
from engine.startup.subprocesses import import_smoke_subprocess as _startup_import_smoke_subprocess
from engine.startup.subprocesses import module_name_from_path as _startup_module_name_from_path
from engine.startup.subprocesses import run_runtime_graph_validation as _startup_run_runtime_graph_validation
from engine.startup.validation import persist_startup_validation as _startup_persist_startup_validation
from engine.startup.validation import redact_for_log as _startup_redact_for_log
from engine.startup.validation import redact_log_string as _startup_redact_log_string
from engine.startup.validation import startup_validation_summary as _startup_validation_summary_impl
from engine.startup.validation import validation_gate_payload as _startup_validation_gate_payload
from engine.startup.dashboard import coerce_ts_ms as _startup_coerce_ts_ms
from engine.startup.dashboard import dashboard_returned_after_clean_shutdown as _startup_dashboard_returned_after_clean_shutdown
from engine.startup.dashboard import dashboard_stop_requested as _startup_dashboard_stop_requested
from engine.startup.dashboard import run_dashboard_server as _startup_run_dashboard_server
from engine.startup.dashboard import run_dashboard_server_post_bind_validation as _startup_run_dashboard_server_post_bind_validation
from engine.startup.dashboard import wait_for_dashboard_bind as _startup_wait_for_dashboard_bind
from engine.startup.shutdown import bootstrap_runtime_side_effects as _startup_bootstrap_runtime_side_effects
from engine.startup.shutdown import handle_signal as _startup_handle_signal
from engine.startup.shutdown import request_dashboard_runtime_stop as _startup_request_dashboard_runtime_stop
from engine.runtime.sd_notify import notify_ready as _systemd_notify_ready
from engine.runtime.sd_notify import notify_watchdog as _systemd_notify_watchdog

warnings.filterwarnings(
    "ignore",
    message=r"The pynvml package is deprecated\.",
    category=FutureWarning,
)


def _early_log_nonfatal(event: str, error: BaseException, **extra: Any) -> None:
    try:
        logging.getLogger("start_system.bootstrap").log(
            logging.WARNING,
            str(event),
            exc_info=(type(error), error, error.__traceback__),
            extra={
                "event": str(event),
                "component": "start_system",
                "extra_json": dict(extra or {}),
            },
        )
    except Exception:
        _logging_unavailable = True


try:
    import psutil  # type: ignore
except Exception as e:
    psutil = None  # type: ignore
    _early_log_nonfatal("START_SYSTEM_PSUTIL_IMPORT_FAILED", e)

# ------------------------------------------------------------------
# Absolute base directory for systemd-managed Linux execution.
# ------------------------------------------------------------------
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_LOCAL_DATA_SOURCE_MASTER_KEY_FILE = Path("data") / "secrets" / "data_source_master_key"

# HARD ENFORCE: ensure no shadow copies of start_system are executed
EXPECTED_PATH = os.path.join(_BASE_DIR, "start_system.py")
if os.path.abspath(__file__) != os.path.abspath(EXPECTED_PATH):
    raise RuntimeError(f"invalid_entrypoint:{__file__}")


def _env_file_has_nonempty_value(env_path: Path, key: str) -> bool:
    return _startup_env_file_has_nonempty_value(env_path, key, warn=_early_log_nonfatal)


def _append_env_line(env_path: Path, line: str) -> None:
    _startup_append_env_line(env_path, line)


def _ensure_local_secret_file(path: Path) -> None:
    _startup_ensure_local_secret_file(path, warn=_early_log_nonfatal)


def _strict_runtime_requires_explicit_db_path() -> bool:
    return _startup_strict_runtime_requires_explicit_db_path()


def _ensure_local_env_file() -> None:
    _startup_ensure_local_env_file(
        Path(_BASE_DIR),
        _LOCAL_DATA_SOURCE_MASTER_KEY_FILE,
        warn=_early_log_nonfatal,
    )


def _env_int(name: str, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    return _startup_env_int(os.environ, name, default, minimum=minimum, maximum=maximum)


def _env_float(name: str, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    return _startup_env_float(os.environ, name, default, minimum=minimum, maximum=maximum)


def _env_bool(name: str, default: bool) -> bool:
    return _startup_env_bool(os.environ, name, default)


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
_PID_PATH = os.path.join(_LOG_DIR, "runtime.pid")
_INGESTION_PID_PATH = os.path.join(_LOG_DIR, "ingestion.pid")
_INGESTION_STDOUT_PATH = os.path.join(_LOG_DIR, "ingestion.stdout.log")
_INGESTION_STDERR_PATH = os.path.join(_LOG_DIR, "ingestion.stderr.log")
_INGESTION_ENTRY = os.path.join(_BASE_DIR, "start_ingestion.py")
_VALIDATION_TIMEOUT_S = _env_int("TRADING_VALIDATION_TIMEOUT_S", 180, minimum=30, maximum=3600)
_STARTUP_HEALTH_TIMEOUT_S = _env_int("TRADING_STARTUP_HEALTH_TIMEOUT_S", 180, minimum=15, maximum=3600)
_STARTUP_HEALTH_POLL_S = _env_float("TRADING_STARTUP_HEALTH_POLL_S", 2.0, minimum=0.5, maximum=30.0)
_STALE_INGESTION_CLEANUP_TIMEOUT_S = _env_float(
    "TRADING_STALE_INGESTION_CLEANUP_TIMEOUT_S",
    5.0,
    minimum=0.5,
    maximum=60.0,
)
_CHALLENGER_RUNTIME_START_TIMEOUT_S = _env_float(
    "TRADING_CHALLENGER_RUNTIME_START_TIMEOUT_S",
    2.0,
    minimum=0.5,
    maximum=60.0,
)
_STARTUP_DB_REPAIR_LOCK_RETRIES = _env_int("STARTUP_DB_REPAIR_LOCK_RETRIES", 8, minimum=0, maximum=100)
_STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S = _env_float(
    "STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S",
    1.0,
    minimum=0.1,
    maximum=30.0,
)
# Hardened startup remains fail-closed, but the dashboard listener must bind
# before the heavyweight health-validation thread starts spawning ingestion.
# The async path still stops the server if validation later fails.
_STARTUP_HEALTH_FAIL_OPEN = _env_bool("TRADING_STARTUP_HEALTH_FAIL_OPEN", False)
_STARTUP_HEALTH_ASYNC_BIND = _env_bool("TRADING_STARTUP_HEALTH_ASYNC_BIND", True)
_IMPORT_SMOKE_IMPORT_JOBS = _env_bool("TRADING_IMPORT_SMOKE_IMPORT_JOBS", False)
_IMPORT_SMOKE_TIMEOUT_S = _env_float("TRADING_IMPORT_SMOKE_TIMEOUT_S", 12.0, minimum=1.0, maximum=120.0)
_SKIP_STALE_INGESTION_CLEANUP = str(
    os.environ.get("TRADING_SKIP_STALE_INGESTION_CLEANUP", "0")
).strip().lower() in ("1", "true", "yes", "on")
_SKIP_RUNTIME_GRAPH_CHECK = str(
    os.environ.get("TRADING_SKIP_RUNTIME_GRAPH_CHECK", "0")
).strip().lower() in ("1", "true", "yes", "on")
_SHARD_AWARE_INGESTION_JOBS = {"ingestion_runtime", "poll_prices", "options_poll"}
_RUNTIME_OWNER_PID_ENV_KEYS = ("ENGINE_RUNTIME_OWNER_PID", "TRADING_RUNTIME_OWNER_PID")


def _current_ingestion_shard():
    from engine.runtime.ingestion_shards import current_ingestion_shard

    return current_ingestion_shard()


def _ingestion_shard_slug() -> str:
    shard = _current_ingestion_shard()
    if not bool(shard.enabled):
        return ""
    return str(shard.label).replace(":", "-")


def _ingestion_artifact_paths(log_dir: str) -> tuple[str, str, str]:
    slug = _ingestion_shard_slug()
    prefix = "ingestion" if not slug else f"ingestion.{slug}"
    return (
        os.path.join(log_dir, f"{prefix}.pid"),
        os.path.join(log_dir, f"{prefix}.stdout.log"),
        os.path.join(log_dir, f"{prefix}.stderr.log"),
    )


def _ingestion_runtime_liveness_job_name() -> str:
    from engine.runtime.ingestion_shards import ingestion_shard_job_name

    return ingestion_shard_job_name("ingestion_runtime", _current_ingestion_shard())


def _current_shard_liveness_job_names(job_names: set[str]) -> set[str]:
    from engine.runtime.ingestion_shards import ingestion_shard_job_name

    shard = _current_ingestion_shard()
    names: set[str] = set()
    for raw_name in job_names or set():
        name = str(raw_name or "").strip()
        if not name:
            continue
        if bool(shard.enabled) and name in _SHARD_AWARE_INGESTION_JOBS:
            names.add(ingestion_shard_job_name(name, shard))
        elif not bool(shard.enabled) or int(shard.index) == 0:
            names.add(name)
    return names


def _safe_print(*args, **kwargs) -> None:
    kwargs.setdefault("flush", True)
    try:
        print(*args, **kwargs)
    except OSError as e:
        _early_log_nonfatal("START_SYSTEM_SAFE_PRINT_FAILED", e)
    except Exception as e:
        _early_log_nonfatal("START_SYSTEM_SAFE_PRINT_FAILED", e)


def _bootstrap_start_system_env() -> None:
    sys.dont_write_bytecode = True
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

    if _BASE_DIR not in sys.path:
        sys.path.insert(0, _BASE_DIR)

    try:
        from dotenv import load_dotenv
        env_file_raw = str(os.environ.get("TRADING_ENV_FILE") or ".env").strip() or ".env"
        env_file = Path(env_file_raw).expanduser()
        if not env_file.is_absolute():
            env_file = Path(_BASE_DIR) / env_file
        if env_file == Path(_BASE_DIR) / ".env":
            _ensure_local_env_file()
        load_dotenv(env_file, override=False)
    except Exception as e:
        sys.stderr.write(f"[start_system] dotenv_load_failed: {type(e).__name__}: {e}\n")
        sys.stderr.flush()

    try:
        resolve_runtime_paths(os.environ, project_root=Path(_BASE_DIR))
    except Exception as e:
        sys.stderr.write(f"[start_system] runtime_path_resolve_failed: {type(e).__name__}: {e}\n")
        sys.stderr.flush()

    # Ignore the known-dead local proxy sentinel if present so outbound market-data HTTP works.
    dead_local_proxy = "http://127.0.0.1:9"
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        if str(os.environ.get(key, "")).strip().lower() == dead_local_proxy:
            os.environ.pop(key, None)
            os.environ.pop(key.lower(), None)

    resolved_log_dir = os.path.abspath(
        os.environ.get("TRADING_LOGS")
        or os.environ.get("LOG_DIR")
        or str(default_local_log_dir())
    )
    resolved_data_dir = os.path.abspath(
        os.environ.get("TRADING_DATA")
        or os.environ.get("DATA_DIR")
        or str(default_local_db_dir())
    )

    os.makedirs(resolved_log_dir, exist_ok=True)
    os.makedirs(resolved_data_dir, exist_ok=True)
    os.environ.setdefault("TRADING_LOGS", resolved_log_dir)
    os.environ.setdefault("TRADING_DATA", resolved_data_dir)
    if not _strict_runtime_requires_explicit_db_path():
        os.environ.setdefault("DB_PATH", str(default_local_db_path().resolve()))
    try:
        from engine.runtime.hardware import apply_cpu_first_runtime_defaults

        apply_cpu_first_runtime_defaults(role="runtime")
    except Exception as e:
        sys.stderr.write(f"[start_system] hardware_defaults_failed: {type(e).__name__}: {e}\n")
        sys.stderr.flush()
    os.environ["PYTHONPATH"] = _BASE_DIR + os.pathsep + str(os.environ.get("PYTHONPATH", ""))


def _refresh_startup_settings() -> None:
    global _LOG_DIR
    global _DATA_DIR
    global _PID_PATH
    global _INGESTION_PID_PATH
    global _INGESTION_STDOUT_PATH
    global _INGESTION_STDERR_PATH
    global _INGESTION_ENTRY
    global _VALIDATION_TIMEOUT_S
    global _STARTUP_HEALTH_TIMEOUT_S
    global _STARTUP_HEALTH_POLL_S
    global _STALE_INGESTION_CLEANUP_TIMEOUT_S
    global _CHALLENGER_RUNTIME_START_TIMEOUT_S
    global _STARTUP_DB_REPAIR_LOCK_RETRIES
    global _STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S
    global _STARTUP_HEALTH_FAIL_OPEN
    global _STARTUP_HEALTH_ASYNC_BIND
    global _IMPORT_SMOKE_IMPORT_JOBS
    global _IMPORT_SMOKE_TIMEOUT_S
    global _SKIP_STALE_INGESTION_CLEANUP
    global _SKIP_RUNTIME_GRAPH_CHECK

    _LOG_DIR = os.path.abspath(
        os.environ.get("TRADING_LOGS")
        or os.environ.get("LOG_DIR")
        or str(default_local_log_dir())
    )
    _DATA_DIR = os.path.abspath(
        os.environ.get("TRADING_DATA")
        or os.environ.get("DATA_DIR")
        or str(default_local_db_dir())
    )
    _PID_PATH = os.path.join(_LOG_DIR, "runtime.pid")
    _INGESTION_PID_PATH, _INGESTION_STDOUT_PATH, _INGESTION_STDERR_PATH = _ingestion_artifact_paths(_LOG_DIR)
    _INGESTION_ENTRY = os.path.join(_BASE_DIR, "start_ingestion.py")
    _VALIDATION_TIMEOUT_S = _env_int("TRADING_VALIDATION_TIMEOUT_S", 180, minimum=30, maximum=3600)
    _STARTUP_HEALTH_TIMEOUT_S = _env_int("TRADING_STARTUP_HEALTH_TIMEOUT_S", 180, minimum=15, maximum=3600)
    _STARTUP_HEALTH_POLL_S = _env_float("TRADING_STARTUP_HEALTH_POLL_S", 2.0, minimum=0.5, maximum=30.0)
    _STALE_INGESTION_CLEANUP_TIMEOUT_S = _env_float(
        "TRADING_STALE_INGESTION_CLEANUP_TIMEOUT_S",
        5.0,
        minimum=0.5,
        maximum=60.0,
    )
    _CHALLENGER_RUNTIME_START_TIMEOUT_S = _env_float(
        "TRADING_CHALLENGER_RUNTIME_START_TIMEOUT_S",
        2.0,
        minimum=0.5,
        maximum=60.0,
    )
    _STARTUP_DB_REPAIR_LOCK_RETRIES = _env_int("STARTUP_DB_REPAIR_LOCK_RETRIES", 8, minimum=0, maximum=100)
    _STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S = _env_float(
        "STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S",
        1.0,
        minimum=0.1,
        maximum=30.0,
    )
    _STARTUP_HEALTH_FAIL_OPEN = _env_bool("TRADING_STARTUP_HEALTH_FAIL_OPEN", False)
    _STARTUP_HEALTH_ASYNC_BIND = _env_bool("TRADING_STARTUP_HEALTH_ASYNC_BIND", True)
    _IMPORT_SMOKE_IMPORT_JOBS = _env_bool("TRADING_IMPORT_SMOKE_IMPORT_JOBS", False)
    _IMPORT_SMOKE_TIMEOUT_S = _env_float(
        "TRADING_IMPORT_SMOKE_TIMEOUT_S",
        12.0,
        minimum=1.0,
        maximum=120.0,
    )
    _SKIP_STALE_INGESTION_CLEANUP = str(
        os.environ.get("TRADING_SKIP_STALE_INGESTION_CLEANUP", "0")
    ).strip().lower() in ("1", "true", "yes", "on")
    _SKIP_RUNTIME_GRAPH_CHECK = str(
        os.environ.get("TRADING_SKIP_RUNTIME_GRAPH_CHECK", "0")
    ).strip().lower() in ("1", "true", "yes", "on")

_bootstrap_start_system_env()
_refresh_startup_settings()

from engine.runtime.log_retention import rotate_log_if_needed
from engine.runtime.logging import get_logger, flush_logging_handlers
from engine.runtime.failure_diagnostics import log_failure, normalize_root_cause_code
from engine.runtime.shutdown import runtime_shutdown
from engine.strategy.challenger_runtime import start_challenger_runtime

LOG = get_logger("start_system")
_safe_print("START_SYSTEM_RUNNING_FROM:", __file__)
_safe_print("SYS.PATH:", sys.path)

# HARD GUARD — detect corrupted file versions
try:
    with open(__file__, "r", encoding="utf-8") as f:
        _self_text = f.read()
except Exception:
    raise

_STARTUP_TRACE = {
    "phase": "BOOT",
    "phases": [],
    "first_failure": {},
    "import_errors": [],
    "ts_ms": int(time.time() * 1000),
}
_IMPORT_SMOKE = {
    "ok": True,
    "failures": [],
    "ts_ms": int(time.time() * 1000),
}


def _initialize_data_source_manager_env() -> None:
    try:
        from services.data_source_manager import get_manager

        get_manager().initialize()
        get_manager().apply_runtime_environment()
    except Exception as e:
        _early_log_nonfatal("START_SYSTEM_DATA_SOURCE_MANAGER_INIT_FAILED", e)

def _json_default(value):
    try:
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, BaseException):
            return f"{type(value).__name__}: {value}"
        return str(value)
    except Exception as e:
        _early_log_nonfatal("START_SYSTEM_JSON_DEFAULT_FAILED", e, value_type=type(value).__name__)
        return repr(value)

def _meta_set_json(key: str, payload) -> None:
    try:
        from engine.runtime.runtime_meta import meta_set
        meta_set(
            str(key),
            json.dumps(payload, default=_json_default, separators=(",", ":"), sort_keys=True),
            best_effort=True,
        )
    except Exception as e:
        sys.stderr.write(
            f"[start_system] meta_set_json_failed key={key}: {type(e).__name__}: {e}\n"
        )
        sys.stderr.flush()

def _persist_startup_trace() -> None:
    _STARTUP_TRACE["ts_ms"] = int(time.time() * 1000)
    _meta_set_json("startup_trace", _STARTUP_TRACE)

def _persist_import_smoke() -> None:
    _IMPORT_SMOKE["ts_ms"] = int(time.time() * 1000)
    _meta_set_json("import_smoke", _IMPORT_SMOKE)


def _run_nonfatal_with_timeout(label: str, fn, *, timeout_s: float = 2.0) -> bool:
    done = threading.Event()
    error_box: Dict[str, Optional[Exception]] = {"error": None}

    def _runner() -> None:
        try:
            fn()
        except Exception as e:
            error_box["error"] = e
        finally:
            done.set()

    t = threading.Thread(
        target=_runner,
        name=f"nonfatal_{label}",
        daemon=True,
    )
    t.start()
    finished = done.wait(max(0.1, float(timeout_s)))
    if not finished:
        _log_swallowed(f"{label.upper()}_TIMEOUT", timeout_s=float(timeout_s))
        return False
    if error_box["error"] is not None:
        raise error_box["error"]
    return True


def _startup_validation_summary(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return _startup_validation_summary_impl(snapshot, now_ms=int(time.time() * 1000))


def _redact_log_string(value: str) -> str:
    return _startup_redact_log_string(value)


def _redact_for_log(value: Any, *, key: str = "") -> Any:
    return _startup_redact_for_log(value, key=key)


def _persist_startup_validation(snapshot: Optional[Dict[str, Any]], *, stage: str, attempt: int, timeout_s: float) -> None:
    _startup_persist_startup_validation(
        _STARTUP_TRACE,
        snapshot,
        stage=stage,
        attempt=attempt,
        timeout_s=timeout_s,
        persist_startup_trace=_persist_startup_trace,
        meta_set_json=_meta_set_json,
        now_ms=int(time.time() * 1000),
    )


def _log_startup_validation(stage: str, snapshot: Optional[Dict[str, Any]], *, level: str = "warning", attempt: int = 0, timeout_s: float = 0.0) -> None:
    payload = _startup_validation_summary(snapshot)
    payload["stage"] = str(stage)
    payload["attempt"] = int(attempt)
    payload["timeout_s"] = float(timeout_s)
    safe_payload = _redact_for_log(payload)
    message = "STARTUP_HEALTH_VALIDATION " + json.dumps(safe_payload, default=_json_default, separators=(",", ":"), sort_keys=True)
    log_fn = getattr(LOG, str(level).lower(), LOG.warning)
    log_fn(message)
    try:
        flush_logging_handlers()
    except Exception as e:
        _log_swallowed("STARTUP_HEALTH_VALIDATION_FLUSH_FAILED", error=str(e), stage=str(stage), attempt=int(attempt))


_FALSE_STARTUP_VALUES = {"0", "false", "no", "off", "disabled"}
_TRUE_STARTUP_VALUES = {"1", "true", "yes", "on", "enabled"}
_FEEDLESS_SAFE_HEALTH_MARKERS = (
    "awaiting_first_price_tick",
    "first_price",
    "health_not_ok",
    "market_data",
    "price",
    "prices",
    "warmup",
    "warming_up",
)


def _env_value(name: str) -> str:
    return str(os.environ.get(str(name), "") or "").strip()


def _is_false_env(name: str) -> bool:
    return _env_value(name).lower() in _FALSE_STARTUP_VALUES


def _is_true_env(name: str) -> bool:
    return _env_value(name).lower() in _TRUE_STARTUP_VALUES


def _safe_no_credential_startup_mode(mode: str) -> bool:
    """Return true only for the local safe posture where feedless serving is allowed."""

    normalized_mode = str(mode or "").strip().lower() or _env_value("ENGINE_MODE").lower() or "safe"
    if normalized_mode != "safe":
        return False
    for key in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
        value = _env_value(key).lower()
        if value and value not in {"safe", "paper"}:
            return False
    if _is_false_env("DISABLE_LIVE_EXECUTION"):
        return False
    broker = (_env_value("BROKER") or _env_value("BROKER_NAME")).lower()
    if broker and broker not in {"sim", "safe", "paper", "mock"}:
        return False
    live_provider_flags = (
        "IBKR_ENABLED",
        "ALPACA_ENABLED",
        "TRADIER_ENABLED",
        "POLYGON_REST_ENABLED",
        "POLYGON_WS_ENABLED",
    )
    return not any(_is_true_env(key) for key in live_provider_flags)


def _safe_feedless_startup_health_allowed(
    *,
    mode: str,
    validation: Optional[Dict[str, Any]] = None,
    exc: Optional[BaseException] = None,
) -> bool:
    if not _safe_no_credential_startup_mode(str(mode)):
        return False

    snap = dict(validation or {})
    blocking = [
        str(item or "").strip()
        for item in list(snap.get("blocking_gates") or snap.get("blocking_checks") or [])
        if str(item or "").strip()
    ]
    critical_missing = [
        str(item or "").strip()
        for item in list(snap.get("critical_systems_missing") or [])
        if str(item or "").strip()
    ]
    if blocking or critical_missing:
        return False

    lifecycle_state = str(snap.get("lifecycle_state") or snap.get("status") or "").strip().upper()
    first_tick = str(snap.get("first_price_ts_ms") or "").strip()
    prices_known_bad = snap.get("prices_ok") is False
    if lifecycle_state in {"WARMING_UP", "DEGRADED"} and not first_tick:
        return True
    if prices_known_bad and not first_tick:
        return True

    text_parts: List[str] = []
    for key in ("reasons", "health_reasons", "notes"):
        text_parts.extend(str(item or "") for item in list(snap.get(key) or []))
    if exc is not None:
        text_parts.append(str(exc))
    text = " ".join(text_parts).lower()
    return bool(text and any(marker in text for marker in _FEEDLESS_SAFE_HEALTH_MARKERS))


def _await_startup_health(*, mode: str, timeout_s: float) -> Dict[str, Any]:
    from engine.runtime.health import get_health_snapshot

    deadline = time.time() + max(1.0, float(timeout_s))
    attempt = 0
    last_validation: Dict[str, Any] = {}
    last_signature = ""

    while True:
        attempt += 1
        health = dict(get_health_snapshot() or {})
        validation = dict((health.get("startup_validation") or {}))
        if not validation:
            missing_gates = [
                "config_valid",
                "database_reachable",
                "schema_valid",
                "log_path_writable",
                "required_directories_present",
                "core_services_initialized",
                "required_api_dependencies_available",
                "ui_static_assets_present",
                "no_port_binding_conflict",
            ]
            validation = {
                "ok": False,
                "blocking_checks": list(missing_gates),
                "blocking_gates": list(missing_gates),
                "critical_systems_missing": [],
                "reasons": ["startup_validation_missing_from_health_snapshot"],
                "health_reasons": list(health.get("reasons") or []),
                "checks": {},
                "gates": {},
                "db_validation": {},
                "mode": str(mode),
                "ts_ms": int(time.time() * 1000),
            }
        validation.setdefault("health_reasons", list(health.get("reasons") or []))
        validation["health_ok"] = bool(health.get("ok"))
        validation["health_snapshot_ts_ms"] = int(health.get("ts_ms") or 0)
        validation["system_stage"] = str(health.get("system_stage") or "")
        validation["data_flow_ok"] = bool(health.get("data_flow_ok"))
        lifecycle = dict(health.get("lifecycle") or {})
        validation["health_status"] = str(health.get("status") or "")
        validation["lifecycle_state"] = str(lifecycle.get("state") or "")
        validation["first_price_ts_ms"] = str(lifecycle.get("first_price_ts_ms") or "")
        validation["prices_ok"] = bool((health.get("prices") or {}).get("ok"))

        _persist_startup_validation(validation, stage="poll", attempt=attempt, timeout_s=timeout_s)

        signature = json.dumps(
            {
                "ok": bool(validation.get("ok")),
                "blocking_gates": list(validation.get("blocking_gates") or validation.get("blocking_checks") or []),
                "reasons": list(validation.get("reasons") or []),
                "critical_systems_missing": list(validation.get("critical_systems_missing") or []),
            },
            default=_json_default,
            sort_keys=True,
        )
        if attempt == 1 or signature != last_signature:
            _log_startup_validation("poll", validation, level="warning", attempt=attempt, timeout_s=timeout_s)
            last_signature = signature

        if bool(validation.get("ok")):
            _persist_startup_validation(validation, stage="passed", attempt=attempt, timeout_s=timeout_s)
            _log_startup_validation("passed", validation, level="warning", attempt=attempt, timeout_s=timeout_s)
            return validation

        last_validation = validation
        if time.time() >= deadline:
            break
        time.sleep(float(_STARTUP_HEALTH_POLL_S))

    if _safe_feedless_startup_health_allowed(mode=str(mode), validation=last_validation):
        degraded_validation = dict(last_validation)
        degraded_validation["ok"] = True
        degraded_validation["safe_mode_feedless_degraded"] = True
        reasons = list(degraded_validation.get("reasons") or [])
        if "safe_mode_feedless_degraded_serving" not in reasons:
            reasons.append("safe_mode_feedless_degraded_serving")
        degraded_validation["reasons"] = reasons
        _persist_startup_validation(degraded_validation, stage="safe_degraded", attempt=attempt, timeout_s=timeout_s)
        _log_startup_validation("safe_degraded", degraded_validation, level="warning", attempt=attempt, timeout_s=timeout_s)
        try:
            from engine.runtime.lifecycle_state import DEGRADED, set_state

            set_state(DEGRADED, "safe_mode_feedless_startup_health_degraded")
        except Exception as degraded_err:
            _log_swallowed(
                "STARTUP_HEALTH_SAFE_DEGRADED_STATE_FAILED",
                error=str(degraded_err),
                mode=str(mode),
            )
        return degraded_validation

    _persist_startup_validation(last_validation, stage="failed", attempt=attempt, timeout_s=timeout_s)
    _log_startup_validation("failed", last_validation, level="error", attempt=attempt, timeout_s=timeout_s)
    raise RuntimeError(
        "startup_health_validation_failed:"
        + ",".join(
            str(x)
            for x in (last_validation.get("blocking_gates") or last_validation.get("blocking_checks") or [])
        )
    )


def _record_phase(phase: str, *, status: str = "started", detail: str = "", extra: Optional[dict] = None) -> None:
    _startup_record_phase(
        _STARTUP_TRACE,
        phase,
        status=status,
        detail=detail,
        extra=extra,
        now_ms=int(time.time() * 1000),
    )
    _persist_startup_trace()


def _record_first_failure(phase: str, exc: BaseException, *, file_path: str = "", line_no: Optional[int] = None, module: str = "") -> None:
    before = _STARTUP_TRACE.get("first_failure")
    _startup_record_first_failure(
        _STARTUP_TRACE,
        phase,
        exc,
        file_path=file_path,
        line_no=line_no,
        module=module,
        now_ms=int(time.time() * 1000),
    )
    if not before and _STARTUP_TRACE.get("first_failure"):
        _persist_startup_trace()


def _probe_runtime_acceleration() -> None:
    def _run_probe() -> None:
        from engine.runtime.acceleration import probe_torch_acceleration

        snapshot = probe_torch_acceleration(logger=LOG)
        _STARTUP_TRACE["runtime_acceleration"] = dict(snapshot)
        _meta_set_json("runtime_acceleration", snapshot)

    try:
        finished = _run_nonfatal_with_timeout(
            "runtime_acceleration_probe",
            _run_probe,
            timeout_s=5.0,
        )
        snapshot = dict(_STARTUP_TRACE.get("runtime_acceleration") or {})
        if not finished:
            snapshot = {
                "ok": False,
                "torch_imported": False,
                "effective_device": "cpu",
                "fallback_reason": "probe_timeout",
                "ts_ms": int(time.time() * 1000),
            }
            _STARTUP_TRACE["runtime_acceleration"] = snapshot
        _record_phase(
            "ACCELERATION",
            status="ok",
            detail=str(snapshot.get("effective_device") or "cpu"),
            extra=snapshot,
        )
    except Exception as e:
        snapshot = {
            "ok": False,
            "torch_imported": False,
            "effective_device": "cpu",
            "fallback_reason": f"probe_failed:{type(e).__name__}",
            "error": str(e),
            "ts_ms": int(time.time() * 1000),
        }
        _STARTUP_TRACE["runtime_acceleration"] = snapshot
        if e.__class__.__name__ == "AccelerationProfileError":
            _record_phase("ACCELERATION", status="error", detail=str(e), extra=snapshot)
            raise
        _record_phase("ACCELERATION", status="ok", detail="cpu", extra=snapshot)
        _log_swallowed("RUNTIME_ACCELERATION_PROBE_FAILED", error=f"{type(e).__name__}: {e}")


def _evaluate_startup_prebind_gates(*, mode: str) -> Dict[str, Any]:
    from engine.runtime.startup_gates import evaluate_prebind_startup_gates

    payload = evaluate_prebind_startup_gates(
        repo_root=_BASE_DIR,
        require_ui_assets=True,
    )
    payload["mode"] = str(mode)
    _STARTUP_TRACE["startup_prebind_gates"] = dict(payload)
    _persist_startup_trace()
    _meta_set_json("startup_prebind_gates", payload)
    return payload

def _module_name_from_path(path_value: str) -> str:
    return _startup_module_name_from_path(path_value)


def _import_smoke_subprocess(module_name: str, abs_path: str, *, timeout_s: float) -> Dict[str, Any]:
    return _startup_import_smoke_subprocess(
        module_name,
        abs_path,
        timeout_s=timeout_s,
        base_dir=_BASE_DIR,
        executable=sys.executable,
        environ=os.environ,
        run=subprocess.run,
        log_swallowed=_log_swallowed,
    )


def _run_import_smoke() -> None:
    failures = []
    seen = set()

    try:
        from engine.runtime.job_registry import ALLOWED_JOBS
    except Exception as e:
        _log_swallowed(
            "IMPORT_SMOKE_JOB_REGISTRY_IMPORT_FAILED",
            module="engine.runtime.job_registry",
            path="engine/runtime/job_registry.py",
            error=str(e),
        )
        _IMPORT_SMOKE["ok"] = False
        _IMPORT_SMOKE["failures"] = [{
            "module": "engine.runtime.job_registry",
            "path": "engine/runtime/job_registry.py",
            "error_type": type(e).__name__,
            "error": str(e),
            "line": 0,
        }]
        _STARTUP_TRACE["import_errors"] = list(_IMPORT_SMOKE["failures"])
        _persist_import_smoke()
        _persist_startup_trace()
        return

    bootstrap_targets = [
        ("dashboard_server", "dashboard_server.py"),
        ("start_ingestion", "start_ingestion.py"),
        ("engine.runtime.ingestion_runtime", "engine/runtime/ingestion_runtime.py"),
    ]
    targets = list(bootstrap_targets)
    bootstrap_keys = {(str(name), str(path)) for name, path in bootstrap_targets}

    for job_name, spec in sorted((ALLOWED_JOBS or {}).items()):
        try:
            script_rel = str((spec or ("",))[0] or "").strip()
        except Exception:
            script_rel = ""
        if not script_rel:
            continue
        targets.append((_module_name_from_path(script_rel), script_rel))

    with tempfile.TemporaryDirectory(prefix="startup_import_smoke_pycompile_") as compile_dir:
        for module_name, rel_path in targets:
            key = (str(module_name), str(rel_path))
            if key in seen:
                continue
            seen.add(key)

            abs_path = os.path.join(_BASE_DIR, rel_path)
            if not os.path.exists(abs_path):
                failures.append({
                    "module": str(module_name),
                    "path": str(rel_path),
                    "error_type": "FileNotFoundError",
                    "error": f"module_path_missing:{abs_path}",
                    "line": 0,
                })
                continue

            try:
                digest = hashlib.sha256(os.path.abspath(abs_path).encode("utf-8", "surrogatepass")).hexdigest()
                py_compile.compile(abs_path, cfile=os.path.join(compile_dir, f"{digest}.pyc"), doraise=True)
            except (SyntaxError, IndentationError, py_compile.PyCompileError) as e:
                err = getattr(e, "exc_value", e)
                line_no = int(getattr(err, "lineno", 0) or 0)
                _log_swallowed(
                    "IMPORT_SMOKE_COMPILE_FAILED",
                    module=str(module_name),
                    path=str(rel_path),
                    error=str(err),
                    line=int(line_no),
                )
                failures.append({
                    "module": str(module_name),
                    "path": str(rel_path),
                    "error_type": type(err).__name__,
                    "error": str(err),
                    "line": line_no,
                })
                continue
            except Exception as e:
                _log_swallowed(
                    "IMPORT_SMOKE_COMPILE_FAILED",
                    module=str(module_name),
                    path=str(rel_path),
                    error=str(e),
                    line=int(getattr(e, "lineno", 0) or 0),
                )
                failures.append({
                    "module": str(module_name),
                    "path": str(rel_path),
                    "error_type": type(e).__name__,
                    "error": str(e),
                    "line": int(getattr(e, "lineno", 0) or 0),
                })
                continue

            should_import = key in bootstrap_keys or bool(_IMPORT_SMOKE_IMPORT_JOBS)
            if not should_import:
                continue

            import_result = _import_smoke_subprocess(
                str(module_name),
                str(abs_path),
                timeout_s=float(_IMPORT_SMOKE_TIMEOUT_S),
            )
            if not bool(import_result.get("ok")):
                failures.append({
                    "module": str(module_name),
                    "path": str(rel_path),
                    "error_type": str(import_result.get("error_type") or "ImportError"),
                    "error": str(import_result.get("error") or "import_failed"),
                    "line": 0,
                    "stdout": str(import_result.get("stdout") or ""),
                    "stderr": str(import_result.get("stderr") or ""),
                })

    _IMPORT_SMOKE["ok"] = len(failures) == 0
    _IMPORT_SMOKE["failures"] = failures
    _STARTUP_TRACE["import_errors"] = list(failures)
    _persist_import_smoke()
    _persist_startup_trace()


def _run_production_validation_gate() -> None:
    checks = ["import_smoke"]
    check_name = "runtime_graph_check"
    script_path = os.path.join(_BASE_DIR, "tools", "runtime_graph_check.py")
    failures = []

    try:
        _run_import_smoke()
    except Exception as e:
        failures.append({
            "name": "import_smoke",
            "script": "",
            "error": f"import_smoke_runner_failed:{type(e).__name__}:{e}",
            "exit_code": None,
            "stdout": "",
            "stderr": "".join(traceback.format_exception(type(e), e, e.__traceback__))[-12000:],
        })

    if not bool(_IMPORT_SMOKE.get("ok")):
        failures.append({
            "name": "import_smoke",
            "script": "",
            "error": "import_smoke_failed",
            "exit_code": None,
            "stdout": "",
            "stderr": json.dumps(_IMPORT_SMOKE.get("failures") or [], default=_json_default)[-12000:],
        })

    if _SKIP_RUNTIME_GRAPH_CHECK:
        _STARTUP_TRACE["validation_gate_skip_reason"] = "env:TRADING_SKIP_RUNTIME_GRAPH_CHECK"
    elif not os.path.exists(script_path):
        failures.append({
            "name": str(check_name),
            "script": str(script_path),
            "error": f"validation_script_missing:{script_path}",
            "exit_code": None,
            "stdout": "",
            "stderr": "",
        })
    else:
        checks.append(check_name)
        failure = _startup_run_runtime_graph_validation(
            script_path,
            base_dir=_BASE_DIR,
            executable=sys.executable,
            environ=os.environ,
            timeout_s=_VALIDATION_TIMEOUT_S,
            run=subprocess.run,
        )
        if failure is not None:
            failures.append(failure)

    _STARTUP_TRACE["validation_gate"] = _startup_validation_gate_payload(
        checks,
        failures,
        now_ms=int(time.time() * 1000),
    )
    _persist_startup_trace()

    if failures:
        err = RuntimeError(
            "production_validation_gate_failed: "
            + "; ".join(
                f"{row.get('name')}[exit={row.get('exit_code')}]"
                for row in failures
            )
        )
        _record_first_failure("IMPORTS", err, file_path=__file__, module="validation_gate")
        raise err

def _log_swallowed(event: str, **extra) -> None:
    try:
        log_failure(
            LOG,
            event=str(event),
            code=normalize_root_cause_code(str(event)),
            message=str(extra.get("error") or event),
            level=logging.WARNING,
            component="start_system",
            extra=extra or None,
            include_health=False,
            include_quick_check=False,
            persist=False,
            flush=False,
        )
    except Exception as e:
        _early_log_nonfatal(
            "START_SYSTEM_STRUCTURED_LOG_FAILURE_FAILED",
            e,
            original_event=str(event),
            extra_keys=sorted(str(key) for key in list((extra or {}).keys())[:20]) if isinstance(extra, dict) else [],
        )


def _log_runtime_hardware_bootstrap() -> None:
    try:
        from engine.runtime.hardware import log_runtime_hardware_diagnostics

        snapshot = log_runtime_hardware_diagnostics(LOG, component="start_system")
        _STARTUP_TRACE["runtime_hardware"] = {
            "profile": str(snapshot.get("profile") or ""),
            "dependency_profile": str(snapshot.get("dependency_profile") or ""),
            "disabled_accelerator_reason": str(snapshot.get("disabled_accelerator_reason") or ""),
            "accelerator_profile_error": str(snapshot.get("accelerator_profile_error") or ""),
            "nvidia_telemetry_enabled": bool(snapshot.get("nvidia_telemetry_enabled")),
            "amd_profile_selected": bool(snapshot.get("amd_profile_selected")),
            "ok": bool(snapshot.get("ok")),
        }
        _persist_startup_trace()
    except Exception as e:
        if e.__class__.__name__ == "AccelerationProfileError":
            raise
        _log_swallowed("START_SYSTEM_RUNTIME_HARDWARE_DIAGNOSTICS_FAILED", error=str(e))

try:
    from engine.runtime.event_log import append_event
except Exception:
    append_event = None
    _log_swallowed("EVENT_LOG_IMPORT_FAILED")

_INGESTION_PROC = None
_INGESTION_RESTART_TIMES: List[float] = []
_INGESTION_MAX_RESTARTS = _env_int("INGESTION_MAX_RESTARTS", 5, minimum=1, maximum=100)
_INGESTION_RESTART_WINDOW_S = _env_float("INGESTION_RESTART_WINDOW_S", 60.0, minimum=5.0, maximum=3600.0)
_INGESTION_WATCHDOG_SLEEP_S = _env_float("INGESTION_WATCHDOG_SLEEP_S", 2.0, minimum=0.25, maximum=60.0)
_INGESTION_WATCHDOG_STOP = threading.Event()
_INGESTION_RESTART_BLOCKED = False
_INGESTION_WATCHDOG_THREAD: Optional[threading.Thread] = None
_SYSTEMD_WATCHDOG_THREAD: Optional[threading.Thread] = None
_SYSTEMD_READY_SENT = False
_STARTUP_HEALTH_THREAD: Optional[threading.Thread] = None


def _default_systemd_watchdog_ping_seconds() -> float:
    raw_watchdog_usec = str(os.environ.get("WATCHDOG_USEC") or "0").strip() or "0"
    try:
        watchdog_usec = int(raw_watchdog_usec)
    except (TypeError, ValueError) as e:
        _log_swallowed("SYSTEMD_WATCHDOG_BUDGET_PARSE_FAILED", error=e, raw_watchdog_usec=raw_watchdog_usec)
        return 15.0
    if watchdog_usec > 0:
        return max(1.0, min(15.0, (watchdog_usec / 1_000_000.0) / 2.0))
    return 15.0


_SYSTEMD_WATCHDOG_PING_SECONDS = _env_float(
    "WATCHDOG_PING_SECONDS",
    _default_systemd_watchdog_ping_seconds(),
    minimum=1.0,
    maximum=30.0,
)


def _start_ingestion_with_server_enabled() -> bool:
    enabled = str(os.environ.get("START_INGESTION_WITH_SERVER", "1")).strip().lower()
    return enabled in ("1", "true", "yes", "on")


def _systemd_watchdog_liveness_ok() -> bool:
    try:
        from engine.runtime.lifecycle_state import LIVE, WARMING_UP, get_state

        state = str((get_state() or {}).get("state") or "").strip().upper()
        return state in {LIVE, WARMING_UP}
    except Exception as e:
        _log_swallowed("SYSTEMD_WATCHDOG_LIVENESS_CHECK_FAILED", error=e)
        return False


def _notify_systemd_ready() -> bool:
    global _SYSTEMD_READY_SENT
    if _SYSTEMD_READY_SENT:
        return False
    sent = _systemd_notify_ready()
    _SYSTEMD_READY_SENT = True
    return bool(sent)


def _notify_systemd_watchdog_if_live() -> bool:
    if not _systemd_watchdog_liveness_ok():
        return False
    return bool(_systemd_notify_watchdog())


def _systemd_watchdog_loop() -> None:
    while not _INGESTION_WATCHDOG_STOP.is_set():
        _notify_systemd_watchdog_if_live()
        _INGESTION_WATCHDOG_STOP.wait(max(1.0, float(_SYSTEMD_WATCHDOG_PING_SECONDS)))


def _ensure_systemd_watchdog_started() -> None:
    global _SYSTEMD_WATCHDOG_THREAD
    if not str(os.environ.get("NOTIFY_SOCKET") or "").strip():
        return
    if _start_ingestion_with_server_enabled():
        return
    thread = _SYSTEMD_WATCHDOG_THREAD
    if thread is not None and thread.is_alive():
        return
    thread = threading.Thread(target=_systemd_watchdog_loop, name="systemd_watchdog", daemon=True)
    _SYSTEMD_WATCHDOG_THREAD = thread
    thread.start()


def _watch_ingestion() -> None:
    global _INGESTION_PROC, _INGESTION_RESTART_TIMES, _INGESTION_RESTART_BLOCKED

    while not _INGESTION_WATCHDOG_STOP.is_set():
        try:
            if not _start_ingestion_with_server_enabled():
                return

            if _INGESTION_RESTART_BLOCKED:
                _INGESTION_WATCHDOG_STOP.wait(max(0.25, float(_INGESTION_WATCHDOG_SLEEP_S)))
                continue

            proc = _INGESTION_PROC

            if proc is not None and proc.poll() is not None:
                now = time.time()

                _INGESTION_RESTART_TIMES = [
                    t for t in _INGESTION_RESTART_TIMES
                    if (now - t) < float(_INGESTION_RESTART_WINDOW_S)
                ]
                _INGESTION_RESTART_TIMES.append(now)

                if len(_INGESTION_RESTART_TIMES) >= int(_INGESTION_MAX_RESTARTS):
                    LOG.error(
                        "INGESTION_RESTART_GUARD_TRIGGERED crashes=%s window_s=%s stderr_log=%s",
                        len(_INGESTION_RESTART_TIMES),
                        _INGESTION_RESTART_WINDOW_S,
                        _INGESTION_STDERR_PATH,
                    )
                    LOG.error("INGESTION_DISABLED_UNTIL_MANUAL_RESTART")
                    _INGESTION_PROC = None
                    _INGESTION_RESTART_BLOCKED = True
                    _cleanup_ingestion_pid()
                    return

                exit_code = proc.poll()
                LOG.warning(
                    "INGESTION_PROCESS_EXITED_RESTARTING exit_code=%s stderr_log=%s",
                    exit_code,
                    _INGESTION_STDERR_PATH,
                )
                _INGESTION_PROC = None
                _cleanup_ingestion_pid()
                try:
                    _spawn_ingestion_if_enabled()
                except Exception as e:
                    _log_swallowed(
                        "INGESTION_FATAL_START_FAILED",
                        error=str(e),
                        entry=str(_INGESTION_ENTRY),
                    )

            if _INGESTION_PROC is None:
                if _existing_ingestion_runtime_active():
                    _notify_systemd_watchdog_if_live()
                    _INGESTION_WATCHDOG_STOP.wait(max(0.25, float(_INGESTION_WATCHDOG_SLEEP_S)))
                    continue
                try:
                    _spawn_ingestion_if_enabled()
                except Exception as e:
                    now = time.time()
                    _INGESTION_RESTART_TIMES = [
                        t for t in _INGESTION_RESTART_TIMES
                        if (now - t) < float(_INGESTION_RESTART_WINDOW_S)
                    ]
                    _INGESTION_RESTART_TIMES.append(now)
                    if len(_INGESTION_RESTART_TIMES) >= int(_INGESTION_MAX_RESTARTS):
                        _INGESTION_RESTART_BLOCKED = True
                    _log_swallowed(
                        "INGESTION_WATCHDOG_RESPAWN_FAILED",
                        error=str(e),
                        entry=str(_INGESTION_ENTRY),
                    )
                if _INGESTION_RESTART_BLOCKED:
                    LOG.error(
                        "INGESTION_RESTART_GUARD_TRIGGERED crashes=%s window_s=%s stderr_log=%s",
                        len(_INGESTION_RESTART_TIMES),
                        _INGESTION_RESTART_WINDOW_S,
                        _INGESTION_STDERR_PATH,
                    )
                    LOG.error("INGESTION_DISABLED_UNTIL_MANUAL_RESTART")
                    _cleanup_ingestion_pid()
                    return
                if _INGESTION_PROC is None and not _existing_ingestion_runtime_active():
                    raise RuntimeError("INGESTION_FAILED_TO_START")

        except Exception:
            _log_swallowed(
                "INGESTION_WATCHDOG_ERROR",
                stderr_log=str(_INGESTION_STDERR_PATH),
                stdout_log=str(_INGESTION_STDOUT_PATH),
            )

        _notify_systemd_watchdog_if_live()
        _INGESTION_WATCHDOG_STOP.wait(max(0.25, float(_INGESTION_WATCHDOG_SLEEP_S)))

def _read_pid_file_record(pid_path: str, *, label: str = "runtime") -> dict:
    try:
        raw = Path(pid_path).read_text(encoding="utf-8").strip()
        if not raw:
            return {"pid": 0, "label": str(label), "raw": ""}
        if raw.startswith("{"):
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("pid_record_not_object")
            pid = int(data.get("pid") or 0)
            return {
                "pid": pid if pid > 0 else 0,
                "label": str(data.get("label") or label),
                "entry": str(data.get("entry") or ""),
                "base_dir": str(data.get("base_dir") or ""),
                "owner_pid": int(data.get("owner_pid") or 0),
                "created_ts_ms": int(data.get("created_ts_ms") or 0),
                "raw": raw,
            }
        pid = int(raw)
        return {
            "pid": pid if pid > 0 else 0,
            "label": str(label),
            "entry": "",
            "base_dir": "",
            "owner_pid": 0,
            "created_ts_ms": 0,
            "raw": raw,
        }
    except FileNotFoundError:
        _log_swallowed("RUNTIME_PID_FILE_MISSING", pid_path=str(pid_path), label=str(label))
        return {"pid": 0, "label": str(label), "raw": ""}
    except Exception as e:
        _log_swallowed("RUNTIME_PID_READ_FAILED", pid_path=str(pid_path), error=str(e))
        return {"pid": 0, "label": str(label), "raw": ""}


def _read_pid_file_int(pid_path: str) -> int:
    try:
        return int((_read_pid_file_record(pid_path).get("pid") or 0))
    except Exception as e:
        _log_swallowed("RUNTIME_PID_INT_PARSE_FAILED", pid_path=str(pid_path), error=str(e))
        return 0


def _write_pid_file_record(
    pid_path: str,
    *,
    pid: int,
    label: str,
    entry: str,
    owner_pid: int = 0,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    payload = {
        "pid": int(pid),
        "label": str(label),
        "entry": str(entry or ""),
        "base_dir": str(_BASE_DIR),
        "owner_pid": int(owner_pid or 0),
        "created_ts_ms": int(time.time() * 1000),
    }
    if isinstance(extra, dict):
        payload.update(dict(extra))
    Path(pid_path).write_text(
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )


def _pid_record_belongs_here(record: dict, *, label: str) -> bool:
    try:
        record_label = str((record or {}).get("label") or "").strip().lower()
        record_base_dir = os.path.abspath(str((record or {}).get("base_dir") or "")).strip()
        if record_label and record_label != str(label).strip().lower():
            return False
        if record_base_dir and record_base_dir != os.path.abspath(_BASE_DIR):
            return False
        return True
    except Exception as e:
        _log_swallowed("RUNTIME_PID_RECORD_VALIDATE_FAILED", label=str(label), error=str(e))
        return False


def _write_pid_file() -> None:
    os.makedirs(_LOG_DIR, exist_ok=True)

    record = _read_pid_file_record(_PID_PATH, label="runtime")
    existing_pid = int(record.get("pid") or 0)
    current_pid = int(os.getpid())

    if existing_pid > 0 and existing_pid != current_pid:
        if _pid_is_running_cross_platform(existing_pid) and _pid_record_belongs_here(record, label="runtime"):
            raise RuntimeError(f"runtime_pid_already_active:{_PID_PATH}:{existing_pid}")
        try:
            os.remove(_PID_PATH)
        except FileNotFoundError as e:
            _log_swallowed("RUNTIME_PID_STALE_FILE_ALREADY_REMOVED", pid_path=str(_PID_PATH), error=str(e))
        except Exception as e:
            raise RuntimeError(
                f"runtime_pid_stale_cleanup_failed:{_PID_PATH}:{type(e).__name__}:{e}"
            ) from e

    _write_pid_file_record(
        _PID_PATH,
        pid=current_pid,
        label="runtime",
        entry="start_system.py",
        owner_pid=current_pid,
    )


def _cleanup_pid_file() -> None:
    try:
        record = _read_pid_file_record(_PID_PATH, label="runtime")
        existing_pid = int(record.get("pid") or 0)
        owner_pid = int(record.get("owner_pid") or 0)
        current_pid = int(os.getpid())

        if owner_pid > 0 and owner_pid != current_pid:
            return
        if existing_pid > 0 and existing_pid != current_pid and _pid_is_running_cross_platform(existing_pid):
            return
        if os.path.exists(_PID_PATH):
            os.remove(_PID_PATH)
    except Exception as e:
        _log_swallowed("RUNTIME_PID_CLEANUP_FAILED", pid_path=str(_PID_PATH), error=str(e))


def _write_ingestion_pid(pid: int) -> None:
    try:
        shard = _current_ingestion_shard()
        _write_pid_file_record(
            _INGESTION_PID_PATH,
            pid=int(pid),
            label="ingestion",
            entry="start_ingestion.py",
            owner_pid=int(os.getpid()),
            extra={"shard": shard.as_dict()},
        )
    except Exception as e:
        raise RuntimeError(
            f"write_ingestion_pid_failed:{_INGESTION_PID_PATH}:{type(e).__name__}:{e}"
        ) from e


def _cleanup_ingestion_pid() -> None:
    try:
        record = _read_pid_file_record(_INGESTION_PID_PATH, label="ingestion")
        owner_pid = int(record.get("owner_pid") or 0)
        current_pid = int(os.getpid())
        if owner_pid > 0 and owner_pid != current_pid:
            return
        if os.path.exists(_INGESTION_PID_PATH):
            os.remove(_INGESTION_PID_PATH)
    except Exception as e:
        _log_swallowed("INGESTION_PID_CLEANUP_FAILED", pid_path=str(_INGESTION_PID_PATH), error=str(e))


def _pid_is_running_cross_platform(pid: int) -> bool:
    try:
        pid = int(pid or 0)
    except Exception:
        pid = 0

    if pid <= 0:
        return False

    try:
        os.kill(int(pid), 0)
        return True
    except Exception as e:
        _log_swallowed("RUNTIME_PID_RUNNING_CHECK_FAILED", pid=int(pid), error=str(e))
        return False


def _pid_exists_quiet(pid: int) -> bool:
    try:
        pid = int(pid or 0)
    except (TypeError, ValueError) as e:
        _log_swallowed("PID_EXISTS_INT_PARSE_FAILED", error=str(e))
        pid = 0
    if pid <= 0:
        return False
    exists = False
    try:
        os.kill(int(pid), 0)
        exists = True
    except ProcessLookupError as e:
        _log_swallowed("PID_EXISTS_PROCESS_MISSING", pid=int(pid), error=str(e))
        exists = False
    except PermissionError as e:
        _log_swallowed("PID_EXISTS_PERMISSION_DENIED", pid=int(pid), error=str(e))
        exists = True
    except OSError as e:
        _log_swallowed("PID_EXISTS_CHECK_FAILED", pid=int(pid), error=str(e))
        exists = False
    return bool(exists)


def _terminate_pid_tree_cross_platform(pid: int, *, timeout_s: float = 15.0) -> bool:
    try:
        pid = int(pid or 0)
    except Exception:
        pid = 0

    if pid <= 0 or pid == os.getpid():
        return False

    if not _pid_is_running_cross_platform(pid):
        return False

    try:
        os.kill(int(pid), signal.SIGTERM)
        return True
    except Exception as e:
        _log_swallowed("TERMINATE_PID_TREE_FAILED", pid=int(pid), error=str(e))
        return False


def _repo_process_cmdline(proc) -> str:
    try:
        parts = proc.info.get("cmdline") or []
    except Exception:
        parts = []
    try:
        return " ".join(str(part or "") for part in parts)
    except Exception as e:
        _log_swallowed("REPO_PROCESS_CMDLINE_BUILD_FAILED", error=str(e))
        return ""


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None or str(value).strip() == "":
        return int(default)
    result = int(default)
    try:
        result = int(value)
    except (TypeError, ValueError) as e:
        _log_swallowed("SAFE_INT_PARSE_FAILED", error=str(e), value_type=type(value).__name__)
        result = int(default)
    return int(result)


def _process_env_owner_pid(proc) -> int:
    try:
        environ = proc.environ()
    except Exception as e:
        _log_swallowed(
            "STALE_INGESTION_PROCESS_ENV_READ_FAILED",
            error=str(e),
            pid=getattr(proc, "pid", None),
        )
        return 0
    if not isinstance(environ, dict):
        return 0
    for key in _RUNTIME_OWNER_PID_ENV_KEYS:
        owner_pid = _safe_int(environ.get(key), 0)
        if owner_pid > 0:
            return int(owner_pid)
    return 0


def _process_parent_chain_contains_pid(proc, owner_pid: int) -> bool:
    owner_pid = _safe_int(owner_pid, 0)
    if owner_pid <= 0:
        return False
    try:
        if _safe_int(proc.ppid(), 0) == owner_pid:
            return True
    except Exception as e:
        _log_swallowed(
            "STALE_INGESTION_PROCESS_PARENT_READ_FAILED",
            error=str(e),
            pid=getattr(proc, "pid", None),
        )
    try:
        for parent in proc.parents():
            try:
                if _safe_int(getattr(parent, "pid", 0), 0) == owner_pid:
                    return True
            except Exception as e:
                _log_swallowed(
                    "STALE_INGESTION_PROCESS_PARENT_PID_READ_FAILED",
                    error=str(e),
                    pid=getattr(proc, "pid", None),
                    owner_pid=int(owner_pid),
                )
                continue
    except Exception as e:
        _log_swallowed(
            "STALE_INGESTION_PROCESS_PARENT_CHAIN_READ_FAILED",
            error=str(e),
            pid=getattr(proc, "pid", None),
        )
    return False


def _process_owned_by_current_runtime(proc) -> bool:
    current_pid = int(os.getpid())
    env_owner_pid = _process_env_owner_pid(proc)
    if env_owner_pid > 0:
        return env_owner_pid == current_pid
    return _process_parent_chain_contains_pid(proc, current_pid)


def _pid_record_current_or_orphaned(record: dict, *, label: str) -> bool:
    if not _pid_record_belongs_here(record, label=label):
        return False
    current_pid = int(os.getpid())
    owner_pid = _safe_int((record or {}).get("owner_pid"), 0)
    if owner_pid <= 0 or owner_pid == current_pid:
        return True
    if _pid_exists_quiet(owner_pid):
        return False
    return True


def _row_value(row: Any, key: str, index: int, default: Any = None) -> Any:
    try:
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
    except Exception as e:
        _log_swallowed("ROW_VALUE_KEY_READ_FAILED", error=str(e), key=str(key), index=int(index))
    try:
        return row[index]
    except Exception as e:
        _log_swallowed("ROW_VALUE_INDEX_READ_FAILED", error=str(e), key=str(key), index=int(index))
        return default


def _json_dict_from_text(raw: Any) -> dict:
    try:
        text = str(raw or "").strip()
        if not text:
            return {}
        data = json.loads(text)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        _log_swallowed("JSON_DICT_FROM_TEXT_FAILED", error=str(e))
        return {}


def _liveness_row_current_or_orphaned(row: Any, *, now_ms: int, max_age_ms: int) -> tuple[bool, int, str, str]:
    job_name = str(_row_value(row, "job_name", 0, "") or "").strip()
    pid = _safe_int(_row_value(row, "pid", 1, 0), 0)
    ts_ms = _safe_int(_row_value(row, "ts_ms", 2, 0), 0)
    extra_json = _row_value(row, "extra_json", 4, "")
    extra = _json_dict_from_text(extra_json)
    current_pid = int(os.getpid())
    owner_pid = 0
    for key in ("supervisor_owner_pid", "runtime_owner_pid", "owner_pid"):
        owner_pid = _safe_int(extra.get(key), 0)
        if owner_pid > 0:
            break
    stale = ts_ms <= 0 or (int(now_ms) - int(ts_ms)) > int(max_age_ms)
    pid_running = pid > 0 and _pid_exists_quiet(pid)
    if pid > 0 and not pid_running:
        return True, pid, job_name, "dead_liveness_pid"
    if owner_pid == current_pid:
        return True, pid, job_name, "current_owner"
    if owner_pid > 0:
        if _pid_exists_quiet(owner_pid):
            return False, pid, job_name, "active_other_owner"
        return True, pid, job_name, "orphaned_owner"
    if stale:
        return True, pid, job_name, "stale_unowned_liveness"
    return False, pid, job_name, "fresh_unowned_liveness"


def _build_repo_ingestion_process_markers(job_names: Optional[set[str]] = None) -> set[str]:
    markers = {
        "start_ingestion.py",
        "engine.runtime.ingestion_runtime",
        "engine.data.poll_prices",
        "stream_prices_polygon_ws",
    }
    candidate_jobs = {
        str(name).strip()
        for name in (job_names or set())
        if str(name).strip()
    }
    allowed_jobs: Dict[str, Any] = {}
    try:
        from engine.runtime.job_registry import ALLOWED_JOBS, INGESTION_DAEMON_JOBS

        allowed_jobs = dict(ALLOWED_JOBS or {})
        candidate_jobs.update(
            str(name).strip()
            for name in (INGESTION_DAEMON_JOBS or [])
            if str(name).strip()
        )
    except Exception as e:
        _log_swallowed("STALE_INGESTION_MARKER_DISCOVERY_FAILED", error=str(e))

    for job_name in candidate_jobs:
        job_key = str(job_name).strip().lower()
        if not job_key:
            continue
        markers.add(job_key)
        spec = allowed_jobs.get(str(job_name).strip())
        if not isinstance(spec, (tuple, list)) or not spec:
            continue
        script_rel = str(spec[0] or "").strip().replace("\\", "/").lower()
        if not script_rel:
            continue
        markers.add(os.path.basename(script_rel))
        if script_rel.endswith(".py"):
            markers.add(script_rel[:-3].replace("/", "."))

    return {str(marker).strip().lower() for marker in markers if str(marker).strip()}


def _looks_like_repo_ingestion_process(cmdline: str, *, markers: Optional[set[str]] = None) -> bool:
    text = str(cmdline or "").strip().lower()
    if not text:
        return False
    base = str(os.path.abspath(_BASE_DIR)).strip().lower()
    if base and base not in text:
        return False
    marker_set = set(markers or _build_repo_ingestion_process_markers())
    return any(marker in text for marker in marker_set)


def _process_matches_current_ingestion_shard(proc) -> bool:
    shard = _current_ingestion_shard()
    if not bool(shard.enabled):
        return True
    try:
        environ = proc.environ()
    except Exception as e:
        _log_swallowed("STALE_INGESTION_PROCESS_ENV_READ_FAILED", error=str(e))
        return False
    try:
        proc_index = int(str((environ or {}).get("INGESTION_SHARD_INDEX", "")).strip())
        proc_count = int(str((environ or {}).get("INGESTION_SHARD_COUNT", "")).strip())
    except Exception as e:
        _log_swallowed(
            "STALE_INGESTION_PROCESS_SHARD_ENV_PARSE_FAILED",
            error=str(e),
            pid=getattr(proc, "pid", None),
        )
        return False
    return proc_index == int(shard.index) and proc_count == int(shard.count)


def _discover_repo_ingestion_process_pids(*, known_jobs: Optional[set[str]] = None) -> set[int]:
    stale_pids: set[int] = set()
    if psutil is None:
        return stale_pids
    marker_set = _build_repo_ingestion_process_markers(known_jobs)

    try:
        current_pid = int(os.getpid())
    except Exception:
        current_pid = 0

    try:
        current_proc = psutil.Process(current_pid) if current_pid > 0 else None
        current_parent_pid = int(current_proc.ppid()) if current_proc is not None else 0
    except Exception:
        current_parent_pid = 0

    try:
        for proc in psutil.process_iter(attrs=["pid", "cmdline"]):
            try:
                pid = int(proc.info.get("pid") or 0)
            except Exception:
                pid = 0
            if pid <= 0 or pid == current_pid or pid == current_parent_pid:
                continue
            cmdline = _repo_process_cmdline(proc)
            if (
                _looks_like_repo_ingestion_process(cmdline, markers=marker_set)
                and _process_matches_current_ingestion_shard(proc)
                and _process_owned_by_current_runtime(proc)
            ):
                stale_pids.add(pid)
        return stale_pids
    except Exception as e:
        _log_swallowed("STALE_INGESTION_PROCESS_SCAN_FAILED", error=str(e))
        return stale_pids


def _terminate_stale_ingestion_processes(*, time_budget_s: Optional[float] = None) -> None:
    cleanup_started_ms = int(time.time() * 1000)
    stale_pids = set()
    cleanup_liveness_jobs: set[str] = set()
    stale_jobs = {"ingestion_runtime", "poll_prices", "options_poll"}
    deadline = None
    if time_budget_s is not None:
        try:
            deadline = time.monotonic() + max(0.1, float(time_budget_s))
        except Exception:
            deadline = None

    def _budget_exhausted() -> bool:
        return deadline is not None and time.monotonic() >= deadline

    raw_child_jobs = [
        str(name).strip()
        for name in str(os.environ.get("INGESTION_CHILD_JOBS", "") or "").split(",")
        if str(name).strip()
    ]
    if raw_child_jobs:
        stale_jobs.update(raw_child_jobs)
    else:
        try:
            from engine.runtime.job_registry import INGESTION_DAEMON_JOBS, get_price_feed_jobs

            stale_jobs.update(
                str(name).strip()
                for name in (INGESTION_DAEMON_JOBS or [])
                if str(name).strip()
            )
            stale_jobs.update(
                str(name).strip()
                for name in (get_price_feed_jobs() or [])
                if str(name).strip()
            )
        except Exception as e:
            _log_swallowed("STALE_INGESTION_CHILD_JOB_DISCOVERY_FAILED", error=str(e))

    stale_liveness_jobs = _current_shard_liveness_job_names(stale_jobs)

    try:
        record = _read_pid_file_record(_INGESTION_PID_PATH, label="ingestion")
        pid_value = int(record.get("pid") or 0)
        if pid_value > 0 and _pid_record_current_or_orphaned(record, label="ingestion"):
            stale_pids.add(pid_value)
        elif pid_value > 0:
            LOG.info(
                "STALE_INGESTION_PID_FILE_SKIPPED_ACTIVE_OWNER pid=%s owner_pid=%s",
                pid_value,
                int(record.get("owner_pid") or 0),
            )
    except FileNotFoundError as e:
        _log_swallowed(
            "READ_INGESTION_PID_FILE_MISSING",
            pid_path=str(_INGESTION_PID_PATH),
            error=str(e),
        )
    except Exception as e:
        _log_swallowed(
            "READ_INGESTION_PID_FAILED",
            pid_path=str(_INGESTION_PID_PATH),
            error=str(e),
        )

    stale_pids.update(_discover_repo_ingestion_process_pids(known_jobs=stale_jobs))
    LOG.info(
        "STALE_INGESTION_CLEANUP_DISCOVERED jobs=%s liveness_jobs=%s pid_count=%s budget_s=%s",
        sorted(stale_jobs),
        sorted(stale_liveness_jobs),
        len(stale_pids),
        float(time_budget_s) if time_budget_s is not None else None,
    )

    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if db_path and os.path.exists(db_path) and stale_liveness_jobs:
        con = None
        try:
            from engine.runtime.storage import connect_ro_direct

            now_ms = int(time.time() * 1000)
            max_age_ms = int(
                max(
                    30.0,
                    float(_INGESTION_RESTART_WINDOW_S),
                    float(_INGESTION_WATCHDOG_SLEEP_S) * 4.0,
                )
                * 1000
            )
            con = connect_ro_direct(timeout_s=2.0, busy_timeout_ms=5000)
            placeholders = ",".join("?" for _ in stale_liveness_jobs)
            rows = con.execute(
                f"""
                SELECT job_name, pid, ts_ms, owner, extra_json
                FROM job_heartbeats
                WHERE job_name IN ({placeholders})
                """,
                tuple(sorted(stale_liveness_jobs)),
            ).fetchall()
            for row in rows or []:
                try:
                    eligible, pid_value, job_name, reason = _liveness_row_current_or_orphaned(
                        row,
                        now_ms=now_ms,
                        max_age_ms=max_age_ms,
                    )
                    if eligible:
                        if pid_value > 0:
                            stale_pids.add(int(pid_value))
                        if job_name:
                            cleanup_liveness_jobs.add(str(job_name))
                    else:
                        LOG.info(
                            "STALE_INGESTION_LIVENESS_ROW_SKIPPED job=%s pid=%s reason=%s",
                            job_name,
                            pid_value,
                            reason,
                        )
                except Exception as e:
                    _log_swallowed(
                        "STALE_INGESTION_HEARTBEAT_ROW_PARSE_FAILED",
                        row=repr(row),
                        error=str(e),
                    )
                    continue
            lock_rows = con.execute(
                f"""
                SELECT job_name, pid, heartbeat_ts_ms AS ts_ms, owner, NULL AS extra_json
                FROM job_locks
                WHERE job_name IN ({placeholders})
                """,
                tuple(sorted(stale_liveness_jobs)),
            ).fetchall()
            for row in lock_rows or []:
                try:
                    eligible, pid_value, job_name, reason = _liveness_row_current_or_orphaned(
                        row,
                        now_ms=now_ms,
                        max_age_ms=max_age_ms,
                    )
                    if eligible:
                        if pid_value > 0:
                            stale_pids.add(int(pid_value))
                        if job_name:
                            cleanup_liveness_jobs.add(str(job_name))
                    else:
                        LOG.info(
                            "STALE_INGESTION_LOCK_ROW_SKIPPED job=%s pid=%s reason=%s",
                            job_name,
                            pid_value,
                            reason,
                        )
                except Exception as e:
                    _log_swallowed(
                        "STALE_INGESTION_LOCK_ROW_PARSE_FAILED",
                        row=repr(row),
                        error=str(e),
                    )
                    continue
        except Exception as e:
            _log_swallowed(
                "STALE_INGESTION_HEARTBEAT_QUERY_FAILED",
                db_path=str(db_path),
                error=str(e),
            )
        finally:
            try:
                if con is not None:
                    con.close()
            except Exception as e:
                _log_swallowed(
                    "STALE_INGESTION_DB_CLOSE_FAILED",
                    db_path=str(db_path),
                    error=str(e),
                )

    running_stale_pids: set[int] = set()
    unhandled_running_stale_pids: set[int] = set()
    for pid in sorted(stale_pids):
        if pid <= 0 or pid == os.getpid():
            continue
        if not _pid_is_running_cross_platform(pid):
            continue
        running_stale_pids.add(int(pid))
        if _budget_exhausted():
            unhandled_running_stale_pids.add(int(pid))
            _log_swallowed(
                "STALE_INGESTION_CLEANUP_BUDGET_EXHAUSTED",
                stage="terminate_pids",
                remaining_pids=sorted(int(x) for x in stale_pids if int(x) >= int(pid)),
            )
            break

        try:
            terminate_timeout_s = 15.0
            if deadline is not None:
                terminate_timeout_s = max(0.25, min(5.0, deadline - time.monotonic()))
            terminated = _terminate_pid_tree_cross_platform(int(pid), timeout_s=terminate_timeout_s)
            if terminated:
                LOG.warning("TERMINATING_STALE_INGESTION_PROCESS pid=%s", pid)
            else:
                unhandled_running_stale_pids.add(int(pid))
                _log_swallowed("TERMINATE_STALE_INGESTION_SKIPPED", pid=int(pid))
        except Exception as e:
            unhandled_running_stale_pids.add(int(pid))
            _log_swallowed("TERMINATE_STALE_INGESTION_FAILED", pid=int(pid), error=str(e))

    try:
        record = _read_pid_file_record(_INGESTION_PID_PATH, label="ingestion")
        pid_value = int(record.get("pid") or 0)
        if pid_value <= 0 or not _pid_is_running_cross_platform(pid_value):
            _cleanup_ingestion_pid()
        LOG.info(
            "STALE_INGESTION_FINAL_PID_CLEANUP_DONE pid=%s duration_ms=%s",
            pid_value,
            max(0, int(time.time() * 1000) - cleanup_started_ms),
        )
    except Exception as e:
        _log_swallowed("FINAL_INGESTION_PID_CLEANUP_FAILED", error=str(e))

    if db_path and os.path.exists(db_path) and cleanup_liveness_jobs:
        if _budget_exhausted() and unhandled_running_stale_pids:
            _log_swallowed(
                "STALE_INGESTION_CLEANUP_BUDGET_EXHAUSTED",
                stage="lock_cleanup",
                db_path=str(db_path),
                unhandled_running_pids=sorted(unhandled_running_stale_pids),
            )
            return
        if _budget_exhausted() and not running_stale_pids:
            _log_swallowed(
                "STALE_INGESTION_LOCK_CLEANUP_AFTER_BUDGET_EXHAUSTED",
                db_path=str(db_path),
                stale_pid_count=len(stale_pids),
            )
        try:
            cleanup_jobs = tuple(sorted(cleanup_liveness_jobs))
            placeholders = ",".join("?" for _ in cleanup_jobs)

            from engine.runtime.storage import run_write_txn

            def _cleanup_txn(con) -> None:
                con.execute(
                    f"DELETE FROM job_heartbeats WHERE job_name IN ({placeholders})",
                    cleanup_jobs,
                )
                con.execute(
                    f"DELETE FROM job_locks WHERE job_name IN ({placeholders})",
                    cleanup_jobs,
                )
                shard = _current_ingestion_shard()
                if (not bool(shard.enabled)) or int(shard.index) == 0:
                    try:
                        con.execute("DELETE FROM price_feed_lock")
                    except Exception as e:
                        if "no such table" not in str(e or "").lower():
                            LOG.warning(
                                "STALE_INGESTION_PRICE_FEED_LOCK_DELETE_FAILED db_path=%s error=%s",
                                db_path,
                                e,
                            )

            run_write_txn(
                _cleanup_txn,
                table="job_heartbeats",
                operation="stale_ingestion_cleanup",
                context={"db_path": str(db_path), "jobs": list(cleanup_jobs)},
            )
            LOG.info(
                "STALE_INGESTION_LOCK_CLEANUP_DONE jobs=%s duration_ms=%s",
                list(cleanup_jobs),
                max(0, int(time.time() * 1000) - cleanup_started_ms),
            )
        except Exception as e:
            LOG.warning(
                "STALE_INGESTION_LOCK_CLEANUP_FAILED db_path=%s liveness_jobs=%s error=%s",
                db_path,
                sorted(stale_liveness_jobs),
                e,
            )


def _existing_ingestion_runtime_active() -> bool:
    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if not db_path or not os.path.exists(db_path):
        return False

    now_ms = int(time.time() * 1000)
    max_age_ms = int(max(30.0, float(_INGESTION_RESTART_WINDOW_S), float(_INGESTION_WATCHDOG_SLEEP_S) * 4.0) * 1000)
    liveness_job_name = _ingestion_runtime_liveness_job_name()

    con = None
    try:
        from engine.runtime.storage import connect_rw_direct

        con = connect_rw_direct(timeout_s=2.0, busy_timeout_ms=5000)
        row = con.execute(
            """
            SELECT pid, ts_ms
            FROM job_heartbeats
            WHERE job_name = ?
            """,
            (liveness_job_name,),
        ).fetchone()

        if not row:
            return False

        pid = int(row[0] or 0)
        ts_ms = int(row[1] or 0)
        fresh = ts_ms > 0 and (now_ms - ts_ms) <= max_age_ms

        # 🔴 VERIFY PID IS ACTUALLY RUNNING
        pid_running = False
        try:
            if int(pid) > 0:
                if os.name == "nt":
                    import ctypes
                    kernel32 = ctypes.windll.kernel32
                    handle = kernel32.OpenProcess(0x1000, 0, int(pid))
                    if handle:
                        kernel32.CloseHandle(handle)
                        pid_running = True
                else:
                    os.kill(int(pid), 0)
                    pid_running = True
        except Exception:
            pid_running = False

        if fresh and pid_running:
            LOG.info(
                "INGESTION_ALREADY_ACTIVE_SKIP_SPAWN job=%s pid=%s heartbeat_age_ms=%s",
                liveness_job_name,
                pid,
                max(0, now_ms - ts_ms),
            )
            return True

        # 🔴 STALE HEARTBEAT → CLEAN IT
        if fresh and not pid_running:
            LOG.warning(
                "INGESTION_HEARTBEAT_WITH_DEAD_PID pid=%s heartbeat_age_ms=%s -> forcing respawn",
                pid,
                max(0, now_ms - ts_ms),
            )

        if (not fresh) and pid_running:
            LOG.warning(
                "INGESTION_PID_RUNNING_BUT_HEARTBEAT_STALE pid=%s heartbeat_age_ms=%s -> allowing supervisor recovery",
                pid,
                max(0, now_ms - ts_ms),
            )

        try:
            con.execute("DELETE FROM job_heartbeats WHERE job_name = ?", (liveness_job_name,))
            con.execute("DELETE FROM job_locks WHERE job_name = ?", (liveness_job_name,))
            con.commit()
        except Exception as e:
            raise RuntimeError(
                f"stale_ingestion_heartbeat_cleanup_failed:{db_path}:{type(e).__name__}:{e}"
            ) from e

        return False
    except Exception as e:
        _log_swallowed("INGESTION_RUNTIME_CHECK_FAILED", db_path=str(db_path), error=str(e))
        return False
    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _log_swallowed(
                "INGESTION_RUNTIME_CHECK_DB_CLOSE_FAILED",
                db_path=str(db_path),
                error=str(e),
            )



def _terminate_ingestion() -> None:
    global _INGESTION_PROC

    proc = _INGESTION_PROC
    _INGESTION_PROC = None

    if proc is None:
        _cleanup_ingestion_pid()
        return

    try:
        if proc.poll() is None:
            if os.name == "nt":
                if not _terminate_pid_tree_cross_platform(int(proc.pid)):
                    proc.terminate()
            else:
                try:
                    os.killpg(int(proc.pid), signal.SIGTERM)
                except Exception:
                    proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception as e:
                _log_swallowed("INGESTION_TERMINATE_WAIT_FAILED", error=str(e))
                if os.name == "nt":
                    if not _terminate_pid_tree_cross_platform(int(proc.pid)):
                        proc.kill()
                else:
                    try:
                        os.killpg(int(proc.pid), signal.SIGKILL)
                    except Exception:
                        proc.kill()
                try:
                    proc.wait(timeout=5)
                except Exception as wait_err:
                    _log_swallowed("INGESTION_KILL_WAIT_FAILED", error=str(wait_err))
    except Exception as e:
        _log_swallowed("INGESTION_TERMINATE_FAILED", error=str(e))
    finally:
        _cleanup_ingestion_pid()


def _ensure_ingestion_watchdog_started() -> None:
    global _INGESTION_WATCHDOG_THREAD

    if not _start_ingestion_with_server_enabled():
        _INGESTION_WATCHDOG_STOP.set()
        return

    thread = _INGESTION_WATCHDOG_THREAD
    if thread is not None and thread.is_alive():
        return

    thread = threading.Thread(target=_watch_ingestion, name="ingestion_watchdog", daemon=True)
    thread.start()
    _INGESTION_WATCHDOG_THREAD = thread


def _postgres_runtime_storage_configured() -> bool:
    backend = str(os.environ.get("TS_STORAGE_BACKEND") or "").strip().lower()
    if backend in ("postgres", "postgresql", "pg"):
        return True
    if backend in ("sqlite", "sqlite-test", "test"):
        return False
    return bool(str(os.environ.get("TS_PG_DSN") or "").strip())


def _ingestion_storage_ready() -> tuple[bool, str]:
    if _postgres_runtime_storage_configured():
        return True, "postgres"
    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if not db_path:
        return False, db_path
    path = Path(db_path)
    if not path.is_file():
        return False, db_path
    try:
        from engine.runtime.storage_sqlite import _sqlite_schema_sentinels_ready

        return bool(_sqlite_schema_sentinels_ready(path)), db_path
    except Exception as e:
        _log_swallowed("INGESTION_STORAGE_READY_SQLITE_CHECK_FAILED", db_path=db_path, error=str(e))
        return False, db_path


def _spawn_ingestion_if_enabled() -> None:
    global _INGESTION_PROC, _INGESTION_RESTART_BLOCKED, _INGESTION_RESTART_TIMES

    if not _start_ingestion_with_server_enabled():
        _INGESTION_RESTART_BLOCKED = False
        _terminate_ingestion()
        return

    if not os.path.exists(_INGESTION_ENTRY):
        raise RuntimeError(f"missing_ingestion_entry:{_INGESTION_ENTRY}")

    try:
        import runpy
        runpy.run_path(_INGESTION_ENTRY, run_name="__ingestion_probe__")
    except Exception as e:
        raise RuntimeError(f"ingestion_entry_import_failed:{_INGESTION_ENTRY}:{e}")

    if _INGESTION_PROC is not None and _INGESTION_PROC.poll() is None:
        return

    if _existing_ingestion_runtime_active():
        return

    storage_ready, storage_detail = _ingestion_storage_ready()
    if not storage_ready:
        raise RuntimeError(f"DB_NOT_INITIALIZED_BEFORE_INGESTION:{storage_detail}")

    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    env["ENGINE_SUPERVISED"] = "1"
    env["ENGINE_LAUNCHED_BY_SUPERVISOR"] = "1"
    env["ENGINE_JOB_NAME"] = "ingestion_runtime"
    env["ENGINE_RUNTIME_OWNER_PID"] = str(int(os.getpid()))
    env["TRADING_RUNTIME_OWNER_PID"] = str(int(os.getpid()))
    env.setdefault("ENGINE_PROCESS_ROLE", "ingestion")
    env.setdefault("TS_PG_POOL_PROFILE", "ingestion")
    env["PYTHONPATH"] = _BASE_DIR + os.pathsep + env.get("PYTHONPATH", "")
    try:
        from engine.runtime.ingestion_shards import canonical_shard_env

        env.update(canonical_shard_env(_current_ingestion_shard()))
    except Exception:
        raise
    try:
        from services.data_source_manager import (
            apply_safe_no_credential_runtime_environment,
            safe_no_credential_market_data_mode,
        )

        if safe_no_credential_market_data_mode():
            apply_safe_no_credential_runtime_environment(env)
    except Exception as e:
        _log_swallowed("INGESTION_SAFE_ENV_SANITIZE_FAILED", error=str(e))

    try:
        from engine.runtime.thread_policy import apply_cpu_thread_policy_to_env

        apply_cpu_thread_policy_to_env(env, role="ingestion")
    except Exception as e:
        _log_swallowed("INGESTION_CPU_THREAD_POLICY_FAILED", error=str(e))

    creationflags = 0
    start_new_session = False
    if sys.platform.startswith("win"):
        creationflags = subprocess.CREATE_NO_WINDOW | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        start_new_session = True

    os.makedirs(_LOG_DIR, exist_ok=True)

    rotate_log_if_needed(_INGESTION_STDOUT_PATH)
    rotate_log_if_needed(_INGESTION_STDERR_PATH)
    with open(_INGESTION_STDOUT_PATH, "ab") as stdout_fh, open(_INGESTION_STDERR_PATH, "ab") as stderr_fh:
        _INGESTION_PROC = subprocess.Popen(
            [sys.executable, "-u", _INGESTION_ENTRY],
            cwd=str(_BASE_DIR),
            env=env,
            creationflags=creationflags,
            start_new_session=start_new_session,
            stdout=stdout_fh,
            stderr=stderr_fh,
        )

    if _INGESTION_PROC is None or _INGESTION_PROC.poll() is not None:
        raise RuntimeError(
            f"ingestion_spawn_failed:{_INGESTION_ENTRY}:"
            f"{None if _INGESTION_PROC is None else _INGESTION_PROC.poll()}"
        )

    _write_ingestion_pid(_INGESTION_PROC.pid)
    _INGESTION_RESTART_TIMES = []
    _INGESTION_RESTART_BLOCKED = False

    try:
        if append_event:
            append_event(
                event_type="ingestion_spawned",
                event_source="start_system",
                entity_type="process",
                entity_id="ingestion_runtime",
                payload={
                    "pid": int(_INGESTION_PROC.pid),
                    "entry": str(_INGESTION_ENTRY),
                    "stdout_log": str(_INGESTION_STDOUT_PATH),
                    "stderr_log": str(_INGESTION_STDERR_PATH),
                    "ts_ms": int(__import__("time").time() * 1000),
                },
                ts_ms=int(__import__("time").time() * 1000),
            )
    except Exception:
        _log_swallowed(
            "INGESTION_APPEND_EVENT_FAILED",
            pid=int(_INGESTION_PROC.pid),
            entry=str(_INGESTION_ENTRY),
        )


def _perform_startup_health_validation(*, mode: str) -> None:
    try:
        _record_phase(
            "STARTUP_HEALTH",
            status="started",
            detail="spawn_ingestion_and_validate_runtime_health",
            extra={"timeout_s": float(_STARTUP_HEALTH_TIMEOUT_S)},
        )
        LOG.warning(
            "INGESTION_POSTBIND_START entry=%s stdout_log=%s stderr_log=%s",
            _INGESTION_ENTRY,
            _INGESTION_STDOUT_PATH,
            _INGESTION_STDERR_PATH,
        )
        _spawn_ingestion_if_enabled()
        _ensure_ingestion_watchdog_started()

        startup_validation = _await_startup_health(
            mode=str(mode),
            timeout_s=float(_STARTUP_HEALTH_TIMEOUT_S),
        )
        _record_phase(
            "STARTUP_HEALTH",
            status="ok",
            detail="startup_health_validation_passed",
            extra=_startup_validation_summary(startup_validation),
        )
    except Exception as e:
        _record_first_failure("STARTUP_HEALTH", e, file_path=__file__, module="start_system.startup_health")
        _record_phase(
            "STARTUP_HEALTH",
            status="failed",
            detail=str(e),
            extra=dict(_STARTUP_TRACE.get("startup_health_validation") or {}),
        )
        LOG.exception(
            "STARTUP_HEALTH_VALIDATION_FAILED",
            extra={"mode": str(mode), "entry": str(_INGESTION_ENTRY)},
        )
        raise


def _request_dashboard_runtime_stop(reason: str) -> None:
    def _stop_server_loader():
        from dashboard_server import stop_server

        return stop_server

    _startup_request_dashboard_runtime_stop(
        reason,
        watchdog_stop=_INGESTION_WATCHDOG_STOP,
        stop_server_loader=_stop_server_loader,
        runtime_shutdown=runtime_shutdown,
        terminate_ingestion=_terminate_ingestion,
        log_swallowed=_log_swallowed,
    )


def _handle_late_startup_health_validation_failure(exc: BaseException, *, mode: str, scope: str) -> None:
    reason = f"late_startup_health_validation_failed:{type(exc).__name__}:{exc}"
    latest_validation = dict(_STARTUP_TRACE.get("startup_health_validation") or {})
    if _safe_feedless_startup_health_allowed(mode=str(mode), validation=latest_validation, exc=exc):
        os.environ["DISABLE_LIVE_EXECUTION"] = "1"
        os.environ["STARTUP_HEALTH_SAFE_MODE_FEEDLESS_DEGRADED"] = reason[:2000]
        _record_phase(
            "STARTUP_HEALTH",
            status="degraded",
            detail="safe_mode_feedless_degraded_serving",
            extra={
                "mode": str(mode),
                "scope": str(scope),
                "execution_disabled": True,
                "safe_mode_feedless_degraded": True,
                "reason": reason[:2000],
            },
        )
        _meta_set_json(
            "startup_health_safe_mode_feedless_degraded",
            {
                "ok": True,
                "mode": str(mode),
                "scope": str(scope),
                "reason": reason,
                "ts_ms": int(time.time() * 1000),
            },
        )
        try:
            from engine.runtime.lifecycle_state import DEGRADED, set_state

            set_state(DEGRADED, "safe_mode_feedless_startup_health_degraded")
        except Exception as degraded_err:
            _log_swallowed("STARTUP_HEALTH_SAFE_DEGRADED_STATE_FAILED", error=str(degraded_err), reason=reason)
        return

    os.environ["DISABLE_LIVE_EXECUTION"] = "1"
    os.environ["KILL_SWITCH_GLOBAL"] = "1"
    os.environ["STARTUP_HEALTH_LATE_FAILURE"] = reason[:2000]
    _record_phase(
        "STARTUP_HEALTH",
        status="failed",
        detail=reason,
        extra={
            "mode": str(mode),
            "scope": str(scope),
            "execution_disabled": True,
            "kill_switch_global": True,
        },
    )
    _meta_set_json(
        "startup_health_late_failure",
        {
            "ok": False,
            "mode": str(mode),
            "scope": str(scope),
            "reason": reason,
            "ts_ms": int(time.time() * 1000),
        },
    )

    try:
        from engine.runtime.lifecycle_state import KILL_SWITCH, set_state

        set_state(KILL_SWITCH, reason[:2000])
    except Exception as kill_err:
        _log_swallowed("STARTUP_HEALTH_KILL_SWITCH_STATE_FAILED", error=str(kill_err), reason=reason)
        try:
            from engine.runtime.lifecycle_state import DEGRADED, set_state

            set_state(DEGRADED, reason[:2000])
        except Exception as degraded_err:
            _log_swallowed("STARTUP_HEALTH_DEGRADED_STATE_FAILED", error=str(degraded_err), reason=reason)

    _request_dashboard_runtime_stop(reason)


def _start_startup_health_validation_async(*, mode: str) -> threading.Thread:
    global _STARTUP_HEALTH_THREAD

    thread = _STARTUP_HEALTH_THREAD
    if thread is not None and thread.is_alive():
        return thread

    def _runner() -> None:
        try:
            _perform_startup_health_validation(mode=str(mode))
            _notify_systemd_ready()
            _ensure_systemd_watchdog_started()
        except Exception as e:
            _log_swallowed("STARTUP_HEALTH_ASYNC_FATAL", mode=str(mode), error=str(e))
            _handle_late_startup_health_validation_failure(
                e,
                mode=str(mode),
                scope="async_post_bind_validation",
            )

    thread = threading.Thread(
        target=_runner,
        name="startup_health_validation",
        daemon=True,
    )
    thread.start()
    _STARTUP_HEALTH_THREAD = thread
    return thread


def _start_systemd_ready_after_dashboard_bind(*, host: str, port: int) -> None:
    if not str(os.environ.get("NOTIFY_SOCKET") or "").strip():
        return

    def _runner() -> None:
        try:
            bind_wait_timeout_s = max(5.0, min(120.0, float(_STARTUP_HEALTH_TIMEOUT_S)))
            if not _wait_for_dashboard_bind(
                host=str(host),
                port=int(port),
                timeout_s=float(bind_wait_timeout_s),
            ):
                raise TimeoutError(f"dashboard_bind_timeout:{host}:{port}")
            _notify_systemd_ready()
            _ensure_systemd_watchdog_started()
        except Exception as e:
            _log_swallowed(
                "SYSTEMD_READY_AFTER_BIND_FAILED",
                host=str(host),
                port=int(port),
                error=str(e),
            )

    threading.Thread(
        target=_runner,
        name="systemd_ready_after_bind",
        daemon=True,
    ).start()


def _wait_for_dashboard_bind(*, host: str, port: int, timeout_s: float) -> bool:
    return _startup_wait_for_dashboard_bind(
        host=host,
        port=port,
        timeout_s=timeout_s,
        create_connection=socket.create_connection,
        monotonic=time.monotonic,
        sleep=time.sleep,
    )


def _run_dashboard_server_post_bind_validation(
    run_server,
    *,
    mode: str,
    host: str,
    port: int,
) -> None:
    bind_wait_timeout_s = max(5.0, min(120.0, float(_STARTUP_HEALTH_TIMEOUT_S)))
    _startup_run_dashboard_server_post_bind_validation(
        run_server,
        mode=mode,
        host=host,
        port=port,
        bind_wait_timeout_s=bind_wait_timeout_s,
        wait_for_bind=_wait_for_dashboard_bind,
        start_startup_health_validation_async=_start_startup_health_validation_async,
        record_phase=_record_phase,
        record_first_failure=_record_first_failure,
        log_warning=LOG.warning,
        log_swallowed=_log_swallowed,
        handle_late_startup_health_validation_failure=_handle_late_startup_health_validation_failure,
        file_path=__file__,
        thread_factory=threading.Thread,
    )


def _run_dashboard_server(run_server, *, mode: str) -> None:
    _startup_run_dashboard_server(
        run_server,
        mode=mode,
        perform_startup_health_validation=_perform_startup_health_validation,
    )


def _run_dashboard_server_with_systemd_ready_after_bind(
    run_server,
    *,
    mode: str,
    host: str,
    port: int,
) -> None:
    _perform_startup_health_validation(mode=str(mode))
    _start_systemd_ready_after_dashboard_bind(host=str(host), port=int(port))
    run_server()


def _coerce_ts_ms(value: Any) -> int:
    try:
        return _startup_coerce_ts_ms(value)
    except Exception as e:
        _log_swallowed("COERCE_TS_MS_FAILED", error=str(e), value_type=type(value).__name__)
        return 0


def _dashboard_stop_requested() -> bool:
    return _startup_dashboard_stop_requested(
        dashboard_module=sys.modules.get("dashboard_server"),
        log_swallowed=_log_swallowed,
    )


def _dashboard_returned_after_clean_shutdown(
    lifecycle: Dict[str, Any],
    *,
    run_enter_ts_ms: int,
    stop_requested_at_enter: bool = False,
) -> bool:
    try:
        from engine.runtime.lifecycle_state import SHUTTING_DOWN

        shutdown_states = (str(SHUTTING_DOWN),)
    except Exception as e:
        _log_swallowed("DASHBOARD_SHUTTING_DOWN_STATE_IMPORT_FAILED", error=str(e))
        shutdown_states = ("SHUTTING_DOWN", "SHUTDOWN", "SHUTTING")

    return _startup_dashboard_returned_after_clean_shutdown(
        lifecycle,
        run_enter_ts_ms=run_enter_ts_ms,
        stop_requested_at_enter=stop_requested_at_enter,
        stop_requested_now=_dashboard_stop_requested(),
        shutdown_states=shutdown_states,
        coerce_ts_ms_fn=_coerce_ts_ms,
    )


def _handle_signal(signum, _frame) -> None:
    def _mark_clean_shutdown_loader():
        from engine.runtime.lifecycle_state import mark_clean_shutdown

        return mark_clean_shutdown

    _startup_handle_signal(
        int(signum),
        watchdog_stop=_INGESTION_WATCHDOG_STOP,
        mark_clean_shutdown_loader=_mark_clean_shutdown_loader,
        terminate_ingestion=_terminate_ingestion,
        runtime_shutdown=runtime_shutdown,
        log_swallowed=_log_swallowed,
        flush_logging_handlers=flush_logging_handlers,
    )


def _db_repair_lock_contention(value: Any) -> bool:
    if isinstance(value, dict):
        parts = [value.get("error"), value.get("detail"), value.get("message")]
        text = " ".join(str(part) for part in parts if part is not None)
    else:
        text = str(value)
    lowered = text.lower()
    return (
        "database is locked" in lowered
        or "sqlite_busy" in lowered
        or "sqlite_locked" in lowered
        or "deadlock detected" in lowered
        or "deadlockdetected" in lowered
        or "locknotavailable" in lowered
        or "could not obtain lock" in lowered
        or "canceling statement due to lock timeout" in lowered
        or "canceling statement due to statement timeout" in lowered
    )


def _run_startup_db_repair() -> Any:
    from engine.runtime.db_repair import repair

    max_attempts = max(1, int(_STARTUP_DB_REPAIR_LOCK_RETRIES) + 1)
    for attempt in range(1, max_attempts + 1):
        try:
            repair_result = repair(startup_fast_path=True)
        except Exception as exc:
            if _db_repair_lock_contention(exc) and attempt < max_attempts:
                delay_s = min(30.0, float(_STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S) * attempt)
                LOG.warning(
                    "DB_REPAIR_LOCKED_RETRY attempt=%s/%s delay_s=%.2f error=%s",
                    attempt,
                    max_attempts,
                    delay_s,
                    str(exc),
                )
                time.sleep(delay_s)
                continue
            raise

        if isinstance(repair_result, dict) and not repair_result.get("ok"):
            if _db_repair_lock_contention(repair_result) and attempt < max_attempts:
                delay_s = min(30.0, float(_STARTUP_DB_REPAIR_LOCK_RETRY_SLEEP_S) * attempt)
                LOG.warning(
                    "DB_REPAIR_LOCKED_RETRY attempt=%s/%s delay_s=%.2f result=%s",
                    attempt,
                    max_attempts,
                    delay_s,
                    repair_result,
                )
                time.sleep(delay_s)
                continue
            raise RuntimeError(repair_result)
        return repair_result
    raise RuntimeError("db_repair_retry_exhausted")


def _bootstrap_runtime_side_effects() -> None:
    _startup_bootstrap_runtime_side_effects(
        watchdog_stop=_INGESTION_WATCHDOG_STOP,
        register_atexit=atexit.register,
        register_signal=signal.signal,
        sigterm=signal.SIGTERM,
        sigint=signal.SIGINT,
        handle_signal_fn=_handle_signal,
        terminate_ingestion=_terminate_ingestion,
        cleanup_pid_file=_cleanup_pid_file,
        write_pid_file=_write_pid_file,
        run_startup_db_repair=_run_startup_db_repair,
        log_exception=LOG.exception,
    )


def _pick_mode_from_argv_or_env() -> str:
    return _startup_pick_mode_from_argv_or_env(sys.argv, os.environ)


def main():
    """Boot the supervised trading runtime and hand off to the dashboard server.

    The startup sequence is intentionally ordered: environment bootstrap,
    architecture validation, deterministic first-run/database bootstrap,
    non-fatal warmers, dashboard import, and finally the long-lived HTTP
    server. Validation failures abort the process before the operator surface
    is exposed.

    Returns
    -------
    None
        This function blocks until ``dashboard_server.run_server`` exits.

    Raises
    ------
    Exception
        Propagates bootstrap, validation, import, or runtime failures so the
        supervising process can treat startup as failed.

    Notes
    -----
    Startup is fail-closed. The production validation gate runs before the
    dashboard module is imported, and unexpected dashboard returns are treated
    as fatal unless the lifecycle state is already shutting down.

    Side Effects
    ------------
    Sets process environment variables, writes startup trace events, may repair
    or initialize runtime storage, starts auxiliary runtime helpers, and always
    tears down ingestion/runtime resources during shutdown.
    """
    if _BASE_DIR not in sys.path:
        sys.path.insert(0, _BASE_DIR)

    _record_phase("BOOT", status="started", detail="start_system_main_enter")
    _bootstrap_start_system_env()
    _refresh_startup_settings()
    mode = _pick_mode_from_argv_or_env()
    os.environ["ENGINE_MODE"] = mode
    os.environ["ENGINE_PRIMARY_BOOTSTRAP_DONE"] = "1"
    # Resolve TRADING_NETWORK_MODE=lan -> DASHBOARD_HOST/OPERATOR_BIND_HOST
    # wildcard defaults before the prebind gates inspect them. Doing this here
    # (rather than only in dashboard_server) ensures the startup token gate sees
    # the same non-loopback host the server will actually bind to.
    try:
        from engine.runtime.platform import apply_network_mode_bind_defaults

        _net_applied = apply_network_mode_bind_defaults(os.environ)
        if _net_applied:
            LOG.warning("NETWORK_MODE_BIND_DEFAULTS_APPLIED %s", _net_applied)
    except Exception as e:  # pragma: no cover - defensive
        _early_log_nonfatal("NETWORK_MODE_BIND_DEFAULTS_FAILED", e)
    _log_runtime_hardware_bootstrap()
    _record_phase("BOOT", status="ok", detail=f"mode={mode}", extra={"pid": int(os.getpid()), "db_path": str(os.environ.get("DB_PATH") or "")})
    _probe_runtime_acceleration()

    _record_phase("CONFIG", status="started", detail="startup_prebind_gates")
    try:
        prebind_gates = _evaluate_startup_prebind_gates(mode=str(mode))
        if not bool(prebind_gates.get("ok")):
            raise RuntimeError(
                "startup_prebind_gates_failed:"
                + ",".join(str(item) for item in list(prebind_gates.get("blocking_gates") or []))
            )
        _record_phase(
            "CONFIG",
            status="ok",
            detail="startup_prebind_gates_passed",
            extra={
                "blocking_gates": list(prebind_gates.get("blocking_gates") or []),
                "config_contract": dict(prebind_gates.get("gates", {}).get("config_valid", {}).get("config_contract") or {}),
            },
        )
    except Exception as e:
        _record_first_failure("CONFIG", e, file_path=__file__, module="start_system.startup_prebind_gates")
        _record_phase(
            "CONFIG",
            status="failed",
            detail=str(e),
            extra=dict(_STARTUP_TRACE.get("startup_prebind_gates") or {}),
        )
        LOG.exception("STARTUP_PREBIND_GATES_FAILED")
        raise

    _bootstrap_runtime_side_effects()

    try:
        if append_event:
            append_event(
                event_type="start_system_begin",
                event_source="start_system",
                entity_type="runtime",
                entity_id="start_system",
                payload={
                    "mode": str(mode),
                    "pid": int(os.getpid()),
                    "db_path": str(os.environ.get("DB_PATH") or ""),
                },
                ts_ms=int(__import__("time").time() * 1000),
            )
    except Exception:
        _log_swallowed("START_SYSTEM_APPEND_EVENT_FAILED", mode=str(mode))

    # --------------------------------------------------------------
    # Architecture integrity validation (fail closed before server import)
    # --------------------------------------------------------------
    _record_phase("IMPORTS", status="started", detail="production_validation_gate")
    try:
        _run_production_validation_gate()
        _record_phase(
            "IMPORTS",
            status="ok",
            detail="canonical_runtime_validation_passed",
            extra={
                "validation_gate": dict(_STARTUP_TRACE.get("validation_gate") or {}),
            },
        )
    except Exception as e:
        _record_first_failure("IMPORTS", e, file_path=__file__, module="start_system.main")
        _record_phase("IMPORTS", status="failed", detail=str(e), extra={"validation_gate": dict(_STARTUP_TRACE.get("validation_gate") or {})})
        LOG.exception("ARCHITECTURE_VALIDATION_FAILED")
        raise

    # --------------------------------------------------------------
    # Lifecycle state boot marker (non-fatal if unavailable)
    # --------------------------------------------------------------
    try:
        from engine.runtime.lifecycle_state import set_state, BOOTING
        set_state(BOOTING, f"mode={mode}")
    except Exception:
        _log_swallowed("LIFECYCLE_BOOT_MARK_FAILED", mode=str(mode))

    # --------------------------------------------------------------
    # Deterministic first-run bootstrap (schema + db guard + seeding)
    # --------------------------------------------------------------
    _record_phase("DB_INIT", status="started", detail="bootstrap_first_run")
    if _SKIP_STALE_INGESTION_CLEANUP:
        _log_swallowed(
            "STALE_INGESTION_CLEANUP_SKIPPED",
            reason="env_override",
            timeout_s=float(_STALE_INGESTION_CLEANUP_TIMEOUT_S),
        )
    else:
        _terminate_stale_ingestion_processes(
            time_budget_s=_STALE_INGESTION_CLEANUP_TIMEOUT_S,
        )

    try:
        LOG.info("FIRST_RUN_IMPORT_BEGIN mode=%s", mode)
        from engine.runtime.first_run import bootstrap_first_run
        LOG.info("FIRST_RUN_IMPORT_OK mode=%s", mode)
        first_run_started_ms = int(time.time() * 1000)
        LOG.info("FIRST_RUN_CALL_BEGIN mode=%s", mode)
        first_run_result = bootstrap_first_run(
            mode=mode,
            assume_prior_db_repair=True,
        )
        LOG.info(
            "FIRST_RUN_CALL_OK mode=%s duration_ms=%s",
            mode,
            max(0, int(time.time() * 1000) - first_run_started_ms),
        )
        if not isinstance(first_run_result, dict) or not first_run_result.get("ok"):
            raise RuntimeError(first_run_result)
        _record_phase("DB_INIT", status="ok", detail="bootstrap_first_run_ok", extra={"result": dict(first_run_result or {})})
    except Exception as e:
        _record_first_failure("DB_INIT", e, file_path=__file__, module="engine.runtime.first_run.bootstrap_first_run")
        _record_phase("DB_INIT", status="failed", detail=str(e))
        LOG.exception("FIRST_RUN_BOOTSTRAP_FAILED")
        raise

    _initialize_data_source_manager_env()

    try:
        from engine.runtime.cache_warm import warm_cache_async
        warm_cache_async()
    except Exception:
        _log_swallowed("CACHE_WARM_START_FAILED", mode=str(mode))

    try:
        _run_nonfatal_with_timeout(
            "challenger_runtime_start",
            start_challenger_runtime,
            timeout_s=_CHALLENGER_RUNTIME_START_TIMEOUT_S,
        )
    except Exception as e:
        _log_swallowed("CHALLENGER_RUNTIME_START_FAILED", error=str(e), mode=str(mode))

    try:
        from engine.runtime.storage_pg_prices import init_pg_price_storage
        from engine.runtime.async_writer import init_async_writer
        from engine.runtime.model_cache import warm_model_catalog

        _run_nonfatal_with_timeout(
            "pg_price_storage_start",
            init_pg_price_storage,
            timeout_s=_CHALLENGER_RUNTIME_START_TIMEOUT_S,
        )
        _run_nonfatal_with_timeout(
            "async_price_writer_start",
            init_async_writer,
            timeout_s=_CHALLENGER_RUNTIME_START_TIMEOUT_S,
        )
        _run_nonfatal_with_timeout(
            "model_cache_warm",
            warm_model_catalog,
            timeout_s=_CHALLENGER_RUNTIME_START_TIMEOUT_S,
        )
        event_runtime_enabled = str(os.environ.get("EVENT_RUNTIME_ENABLED", "0") or "0").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if event_runtime_enabled:
            from engine.runtime.event_runtime import start_event_runtime

            _run_nonfatal_with_timeout(
                "event_runtime_start",
                start_event_runtime,
                timeout_s=_CHALLENGER_RUNTIME_START_TIMEOUT_S,
            )
    except Exception as e:
        _log_swallowed("EVENT_RUNTIME_START_FAILED", error=str(e), mode=str(mode))

    # --------------------------------------------------------------
    # Defer ingestion sibling process until after dashboard server bind
    # --------------------------------------------------------------
    try:
        LOG.warning(
            "INGESTION_PREBIND_DEFERRED entry=%s stdout_log=%s stderr_log=%s",
            _INGESTION_ENTRY,
            _INGESTION_STDOUT_PATH,
            _INGESTION_STDERR_PATH,
        )
        if append_event:
            append_event(
                event_type="ingestion_prebind_deferred",
                event_source="start_system",
                entity_type="process",
                entity_id="ingestion_runtime",
                payload={
                    "entry": str(_INGESTION_ENTRY),
                    "stdout_log": str(_INGESTION_STDOUT_PATH),
                    "stderr_log": str(_INGESTION_STDERR_PATH),
                    "ts_ms": int(__import__("time").time() * 1000),
                },
                ts_ms=int(__import__("time").time() * 1000),
                best_effort=True,
            )
    except Exception as e:
        _log_swallowed("INGESTION_PREBIND_DEFER_MARK_FAILED", error=str(e))

    # --------------------------------------------------------------
    # Start dashboard server
    # --------------------------------------------------------------
    _record_phase("JOB_REGISTRATION", status="started", detail="import_dashboard_server")

    try:
        _run_server_ref: Dict[str, Any] = {"fn": None, "err": None}

        def _import_dashboard():
            try:
                _safe_print("[start_system] api_server_import_begin")
                from engine.api.server import run_server as _rs
                _run_server_ref["fn"] = _rs
                _safe_print("[start_system] api_server_import_ok")
            except Exception as e:
                _run_server_ref["err"] = e
                _safe_print(f"[start_system] api_server_import_error: {e}")

        try:
            t_import = threading.Thread(target=_import_dashboard, daemon=True)
            t_import.start()
            t_import.join(timeout=20)

            if _run_server_ref["fn"] is None:
                raise RuntimeError(
                    f"dashboard_server_import_timeout_or_failed: {_run_server_ref['err']}"
                )

            run_server = _run_server_ref["fn"]

        except Exception as e:
            _safe_print(f"[start_system] dashboard_import_wrapper_failed: {e}")
            raise

        _record_phase("JOB_REGISTRATION", status="ok", detail="dashboard_server_imported")
        LOG.warning(
            "DASHBOARD_SERVER_IMPORTED host=%s port=%s",
            os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
            os.environ.get("DASHBOARD_PORT", "8000"),
        )

        try:
            open_browser = str(os.environ.get("OPEN_DASHBOARD_BROWSER_ON_START", "0")).strip().lower()
            if open_browser in ("1", "true", "yes", "on"):
                import webbrowser
                _safe_print("[start_system] dashboard_browser_open_begin")
                webbrowser.open("http://127.0.0.1:8000/ui/dashboard.html")
                _safe_print("[start_system] dashboard_browser_open_ok")
            else:
                _safe_print("[start_system] dashboard_browser_open_skipped")
        except Exception:
            _log_swallowed("DASHBOARD_BROWSER_OPEN_FAILED")

        try:
            _record_phase("RUNNING", status="started", detail="dashboard_server_run_server_enter")
            LOG.warning("DASHBOARD_SERVER_RUN_SERVER_ENTER")
            _safe_print("[start_system] dashboard_server_run_server_begin")

            dashboard_host = str(
                os.environ.get("DASHBOARD_HOST", "127.0.0.1") or "127.0.0.1"
            ).strip() or "127.0.0.1"
            dashboard_port = _env_int(
                "DASHBOARD_PORT",
                8000,
                minimum=1,
                maximum=65535,
            )
            dashboard_run_enter_ts_ms = int(time.time() * 1000)
            dashboard_stop_requested_at_enter = _dashboard_stop_requested()
            if _STARTUP_HEALTH_ASYNC_BIND:
                _run_dashboard_server_post_bind_validation(
                    run_server,
                    mode=str(mode),
                    host=str(dashboard_host),
                    port=int(dashboard_port),
                )
            else:
                _run_dashboard_server_with_systemd_ready_after_bind(
                    run_server,
                    mode=str(mode),
                    host=str(dashboard_host),
                    port=int(dashboard_port),
                )
            _safe_print("[start_system] dashboard_server_run_server_returned")

            try:
                from engine.runtime.lifecycle_state import get_state
                _lc = get_state() or {}
                if not _dashboard_returned_after_clean_shutdown(
                    dict(_lc),
                    run_enter_ts_ms=int(dashboard_run_enter_ts_ms),
                    stop_requested_at_enter=bool(dashboard_stop_requested_at_enter),
                ):
                    _lc_state = str(_lc.get("state") or "").strip().upper()
                    raise RuntimeError(
                        "dashboard_server_returned_without_clean_shutdown:"
                        f"{_lc_state or 'UNKNOWN'}:"
                        f"{str(_lc.get('detail') or '')}"
                    )
            except Exception as e:
                _record_first_failure("RUNNING", e, file_path=str(Path(_BASE_DIR) / "dashboard_server.py"), module="dashboard_server.run_server")
                _record_phase("RUNNING", status="failed", detail=str(e))
                LOG.exception("DASHBOARD_SERVER_UNEXPECTED_RETURN", extra={"mode": str(mode)})
                try:
                    flush_logging_handlers()
                except Exception as flush_err:
                    sys.stderr.write(
                        f"[start_system] flush_logging_handlers_failed_after_unexpected_return: {type(flush_err).__name__}: {flush_err}\n"
                    )
                    sys.stderr.flush()
                raise
        except Exception as e:
            _record_first_failure("RUNNING", e, file_path=str(Path(_BASE_DIR) / "dashboard_server.py"), module="dashboard_server.run_server")
            _record_phase("RUNNING", status="failed", detail=str(e))
            LOG.exception("DASHBOARD_SERVER_FATAL", extra={"mode": str(mode)})
            try:
                flush_logging_handlers()
            except Exception as flush_err:
                sys.stderr.write(
                    f"[start_system] flush_logging_handlers_failed_after_dashboard_fatal: {type(flush_err).__name__}: {flush_err}\n"
                )
                sys.stderr.flush()
            raise
    finally:
        _INGESTION_WATCHDOG_STOP.set()
        _terminate_ingestion()
        try:
            runtime_shutdown()
        except Exception:
            _log_swallowed("RUNTIME_SHUTDOWN_FAILED_IN_MAIN")
        try:
            flush_logging_handlers()
        except Exception as e:
            sys.stderr.write(f"[start_system] flush_logging_handlers_failed: {type(e).__name__}: {e}\n")
            sys.stderr.flush()


if __name__ == "__main__":
    _mode_for_fatal = str(os.environ.get("ENGINE_MODE", "") or "").strip().lower() or "safe"

    try:
        raise SystemExit(main())
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else 0
        if int(code or 0) != 0:
            try:
                failure = RuntimeError(f"start_system_exit_code:{code}")
                _record_first_failure(str(_STARTUP_TRACE.get("phase") or "RUNNING"), failure, file_path=__file__, module="start_system")
                _record_phase(str(_STARTUP_TRACE.get("phase") or "RUNNING"), status="failed", detail=f"exit_code={code}")
            except Exception as trace_err:
                sys.stderr.write(
                    f"[start_system] startup_trace_record_failed_for_exit_code: {type(trace_err).__name__}: {trace_err}\n"
                )
                sys.stderr.flush()
            try:
                from engine.runtime.lifecycle_state import mark_crash_shutdown
                mark_crash_shutdown(f"start_system_exit_code:{code}")
            except Exception:
                _log_swallowed("MARK_CRASH_SHUTDOWN_FAILED", error=f"start_system_exit_code:{code}")
            try:
                flush_logging_handlers()
            except Exception as flush_err:
                sys.stderr.write(
                    f"[start_system] flush_logging_handlers_failed_after_dashboard_fatal: {type(flush_err).__name__}: {flush_err}\n"
                )
                sys.stderr.flush()
        raise
    except Exception as e:
        _record_first_failure(str(_STARTUP_TRACE.get("phase") or "RUNNING"), e, file_path=__file__, module="start_system")
        _record_phase(str(_STARTUP_TRACE.get("phase") or "RUNNING"), status="failed", detail=str(e))
        LOG.exception("START_SYSTEM_FATAL", extra={"mode": _mode_for_fatal})
        try:
            flush_logging_handlers()
        except Exception as flush_err:
            sys.stderr.write(
                f"[start_system] flush_logging_handlers_failed_on_fatal: {type(flush_err).__name__}: {flush_err}\n"
            )
            sys.stderr.flush()
        try:
            from engine.runtime.lifecycle_state import mark_crash_shutdown
            mark_crash_shutdown(str(e))
        except Exception:
            _log_swallowed("MARK_CRASH_SHUTDOWN_FAILED", error=str(e))
        try:
            flush_logging_handlers()
        except Exception as flush_err:
            sys.stderr.write(
                f"[start_system] flush_logging_handlers_failed_on_fatal: {type(flush_err).__name__}: {flush_err}\n"
            )
            sys.stderr.flush()
        raise
