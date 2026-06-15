"""Top-level supervised runtime bootstrap for the trading system.

This entrypoint owns environment bootstrapping, startup validation, dashboard
binding, ingestion supervision, shutdown hooks, and the main process lifecycle
for the local/service runtime.
"""

import atexit
import base64
import importlib
import importlib.util
import json
import logging
import os
import py_compile
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

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
# Absolute base directory (works under systemd + Windows)
# ------------------------------------------------------------------
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# HARD ENFORCE: ensure no shadow copies of start_system are executed
EXPECTED_PATH = os.path.join(_BASE_DIR, "start_system.py")
if os.path.abspath(__file__) != os.path.abspath(EXPECTED_PATH):
    raise RuntimeError(f"invalid_entrypoint:{__file__}")


def _env_file_has_nonempty_value(env_path: Path, key: str) -> bool:
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() == key and value.strip():
                return True
    except FileNotFoundError:
        missing = True
    except Exception as e:
        _early_log_nonfatal("START_SYSTEM_ENV_FILE_READ_FAILED", e, path=str(env_path), key=str(key))
    return False


def _append_env_line(env_path: Path, line: str) -> None:
    existing = ""
    try:
        existing = env_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        existing = ""
    with env_path.open("a", encoding="utf-8", newline="") as fh:
        if existing and not existing.endswith(("\n", "\r")):
            fh.write("\n")
        fh.write(str(line).rstrip("\r\n") + "\n")


def _ensure_local_env_file() -> None:
    env_path = Path(_BASE_DIR) / ".env"
    example_path = Path(_BASE_DIR) / ".env.example"

    if not env_path.exists():
        if example_path.exists():
            env_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            env_path.write_text("", encoding="utf-8")

    if not _env_file_has_nonempty_value(env_path, "DATA_SOURCE_MASTER_KEY"):
        key = base64.b64encode(os.urandom(32)).decode("ascii")
        _append_env_line(env_path, f"DATA_SOURCE_MASTER_KEY={key}")

def _env_int(name: str, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    raw = os.environ.get(name)
    try:
        value = int(float(str(raw if raw is not None else default).strip()))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value

def _env_float(name: str, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    raw = os.environ.get(name)
    try:
        value = float(str(raw if raw is not None else default).strip())
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return bool(default)


_LOG_DIR = os.path.abspath(
    os.environ.get("TRADING_LOGS") or
    os.environ.get("LOG_DIR") or
    os.path.join(_BASE_DIR, "logs")
)
_DATA_DIR = os.path.abspath(
    os.environ.get("TRADING_DATA") or
    os.environ.get("DATA_DIR") or
    os.path.join(_BASE_DIR, "data")
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
        _ensure_local_env_file()
        from dotenv import load_dotenv
        load_dotenv(os.path.join(_BASE_DIR, ".env"))
    except Exception as e:
        sys.stderr.write(f"[start_system] dotenv_load_failed: {type(e).__name__}: {e}\n")
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
        or os.path.join(_BASE_DIR, "logs")
    )
    resolved_data_dir = os.path.abspath(
        os.environ.get("TRADING_DATA")
        or os.environ.get("DATA_DIR")
        or os.path.join(_BASE_DIR, "data")
    )

    os.makedirs(resolved_log_dir, exist_ok=True)
    os.makedirs(resolved_data_dir, exist_ok=True)
    os.environ.setdefault("TRADING_LOGS", resolved_log_dir)
    os.environ.setdefault("TRADING_DATA", resolved_data_dir)
    os.environ.setdefault("DB_PATH", str((Path(resolved_data_dir) / "trading.db").resolve()))
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
        or os.path.join(_BASE_DIR, "logs")
    )
    _DATA_DIR = os.path.abspath(
        os.environ.get("TRADING_DATA")
        or os.environ.get("DATA_DIR")
        or os.path.join(_BASE_DIR, "data")
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
    snap = dict(snapshot or {})
    gates = dict(snap.get("gates") or snap.get("checks") or {})
    blocking_gates = list(snap.get("blocking_gates") or snap.get("blocking_checks") or [])
    return {
        "ok": bool(snap.get("ok")),
        "mode": str(snap.get("mode") or ""),
        "blocking_checks": list(blocking_gates),
        "blocking_gates": list(blocking_gates),
        "critical_systems_missing": list(snap.get("critical_systems_missing") or []),
        "reasons": list(snap.get("reasons") or []),
        "health_reasons": list(snap.get("health_reasons") or []),
        "checks": gates,
        "gates": gates,
        "db_validation": dict(snap.get("db_validation") or {}),
        "ts_ms": int(snap.get("ts_ms") or int(time.time() * 1000)),
    }


_SENSITIVE_LOG_KEYS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "dsn",
    "password",
    "secret",
    "token",
)


def _redact_log_string(value: str) -> str:
    text = str(value)
    text = re.sub(r"(?i)(password\s*=\s*)[^\s,;}\"]+", r"\1<redacted>", text)
    text = re.sub(r"(?i)(://[^:/@\s]+:)[^@/\s]+@", r"\1<redacted>@", text)
    text = re.sub(r"(?i)((?:api[_-]?key|token|secret|password)\s*[=:]\s*)[^&\s,;}\"]+", r"\1<redacted>", text)
    return text


def _redact_for_log(value: Any, *, key: str = "") -> Any:
    key_l = str(key or "").strip().lower()
    sensitive_key = any(marker in key_l for marker in _SENSITIVE_LOG_KEYS)
    if isinstance(value, dict):
        return {str(k): _redact_for_log(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_for_log(v, key=key) for v in value]
    if isinstance(value, tuple):
        return [_redact_for_log(v, key=key) for v in value]
    if sensitive_key and value not in (None, "", False, True) and not isinstance(value, (int, float)):
        return "<redacted>"
    if isinstance(value, str):
        return _redact_log_string(value)
    return value


def _persist_startup_validation(snapshot: Optional[Dict[str, Any]], *, stage: str, attempt: int, timeout_s: float) -> None:
    payload = _startup_validation_summary(snapshot)
    payload["stage"] = str(stage)
    payload["attempt"] = int(attempt)
    payload["timeout_s"] = float(timeout_s)
    _STARTUP_TRACE["startup_health_validation"] = payload
    _persist_startup_trace()
    _meta_set_json("startup_health_validation", payload)


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
    now_ms = int(time.time() * 1000)
    entry = {
        "phase": str(phase),
        "status": str(status),
        "detail": str(detail or ""),
        "ts_ms": now_ms,
    }
    if isinstance(extra, dict) and extra:
        entry["extra"] = dict(extra)
    _STARTUP_TRACE["phase"] = str(phase)
    _STARTUP_TRACE.setdefault("phases", []).append(entry)
    _persist_startup_trace()

def _record_first_failure(phase: str, exc: BaseException, *, file_path: str = "", line_no: Optional[int] = None, module: str = "") -> None:
    if _STARTUP_TRACE.get("first_failure"):
        return
    tb = traceback.extract_tb(exc.__traceback__) if getattr(exc, "__traceback__", None) else []
    leaf = tb[-1] if tb else None
    _STARTUP_TRACE["first_failure"] = {
        "phase": str(phase),
        "type": type(exc).__name__,
        "error": str(exc),
        "module": str(module or (leaf.name if leaf else "")),
        "file": str(file_path or (leaf.filename if leaf else "")),
        "line": int(line_no or (leaf.lineno if leaf else 0) or 0),
        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-12000:],
        "ts_ms": int(time.time() * 1000),
    }
    _persist_startup_trace()


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
    rel = str(path_value or "").replace("\\", "/").strip()
    if not rel:
        return ""
    if rel.endswith(".py"):
        rel = rel[:-3]
    return rel.replace("/", ".").strip(".")


def _import_smoke_subprocess(module_name: str, abs_path: str, *, timeout_s: float) -> Dict[str, Any]:
    code = (
        "import importlib, importlib.util, sys\n"
        "module_name = sys.argv[1]\n"
        "abs_path = sys.argv[2]\n"
        "if module_name:\n"
        "    importlib.import_module(module_name)\n"
        "else:\n"
        "    spec = importlib.util.spec_from_file_location('startup_import_smoke_target', abs_path)\n"
        "    if spec is None or spec.loader is None:\n"
        "        raise ImportError(f'module_spec_unavailable:{abs_path}')\n"
        "    module = importlib.util.module_from_spec(spec)\n"
        "    spec.loader.exec_module(module)\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = _BASE_DIR + os.pathsep + str(env.get("PYTHONPATH", ""))
    env["TRADING_IMPORT_SMOKE_CHILD"] = "1"
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code, str(module_name or ""), str(abs_path)],
            cwd=_BASE_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_s)),
        )
    except subprocess.TimeoutExpired as e:
        _log_swallowed(
            "IMPORT_SMOKE_SUBPROCESS_TIMEOUT",
            module=str(module_name or ""),
            path=str(abs_path),
            timeout_s=float(timeout_s),
        )
        return {
            "ok": False,
            "error_type": "TimeoutError",
            "error": f"import_timeout_after_{float(timeout_s):.1f}s",
            "stdout": str(e.stdout or "").strip()[-4000:],
            "stderr": str(e.stderr or "").strip()[-4000:],
        }
    except Exception as e:
        _log_swallowed(
            "IMPORT_SMOKE_SUBPROCESS_FAILED",
            module=str(module_name or ""),
            path=str(abs_path),
            error=str(e),
        )
        return {
            "ok": False,
            "error_type": type(e).__name__,
            "error": str(e),
            "stdout": "",
            "stderr": "".join(traceback.format_exception(type(e), e, e.__traceback__))[-4000:],
        }

    if int(proc.returncode or 0) != 0:
        stderr_text = str(proc.stderr or "").strip()
        stdout_text = str(proc.stdout or "").strip()
        return {
            "ok": False,
            "error_type": "ImportError",
            "error": f"import_process_exit_{int(proc.returncode)}",
            "stdout": stdout_text[-4000:],
            "stderr": stderr_text[-4000:],
        }

    return {"ok": True}


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
            py_compile.compile(abs_path, doraise=True)
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
        try:
            env = dict(os.environ)
            env.setdefault("PYTHONPATH", _BASE_DIR)
            env["TRADING_VALIDATION_MODE"] = "startup"

            proc = subprocess.run(
                [sys.executable, script_path],
                cwd=_BASE_DIR,
                env=env,
                capture_output=True,
                text=True,
                timeout=_VALIDATION_TIMEOUT_S,
            )

            stdout_text = str(proc.stdout or "").strip()
            stderr_text = str(proc.stderr or "").strip()

            if int(proc.returncode or 0) != 0:
                failures.append({
                    "name": str(check_name),
                    "script": str(script_path),
                    "error": f"validation_failed:{check_name}",
                    "exit_code": int(proc.returncode),
                    "stdout": stdout_text[-12000:],
                    "stderr": stderr_text[-12000:],
                })
        except subprocess.TimeoutExpired as e:
            failures.append({
                "name": str(check_name),
                "script": str(script_path),
                "error": f"validation_timeout:{check_name}",
                "exit_code": None,
                "stdout": str((e.stdout or "")).strip()[-12000:],
                "stderr": str((e.stderr or "")).strip()[-12000:],
            })

    _STARTUP_TRACE["validation_gate"] = {
        "ok": len(failures) == 0,
        "checks": checks,
        "failures": failures,
        "ts_ms": int(time.time() * 1000),
    }
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
_STARTUP_HEALTH_THREAD: Optional[threading.Thread] = None


def _start_ingestion_with_server_enabled() -> bool:
    enabled = str(os.environ.get("START_INGESTION_WITH_SERVER", "1")).strip().lower()
    return enabled in ("1", "true", "yes", "on")


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


def _write_pid_file_record(pid_path: str, *, pid: int, label: str, entry: str, owner_pid: int = 0) -> None:
    payload = {
        "pid": int(pid),
        "label": str(label),
        "entry": str(entry or ""),
        "base_dir": str(_BASE_DIR),
        "owner_pid": int(owner_pid or 0),
        "created_ts_ms": int(time.time() * 1000),
    }
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
        _write_pid_file_record(
            _INGESTION_PID_PATH,
            pid=int(pid),
            label="ingestion",
            entry="start_ingestion.py",
            owner_pid=int(os.getpid()),
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
        _log_swallowed("RUNTIME_PID_RUNNING_CHECK_FAILED", pid=int(pid), error=str(e))
        return False


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
        if os.name == "nt":
            timeout_s = max(1.0, min(15.0, float(timeout_s or 15.0)))
            result = subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                timeout=timeout_s,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            return int(result.returncode or 0) == 0

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
            if _looks_like_repo_ingestion_process(cmdline, markers=marker_set):
                stale_pids.add(pid)
        return stale_pids
    except Exception as e:
        _log_swallowed("STALE_INGESTION_PROCESS_SCAN_FAILED", error=str(e))
        return stale_pids


def _terminate_stale_ingestion_processes(*, time_budget_s: Optional[float] = None) -> None:
    cleanup_started_ms = int(time.time() * 1000)
    stale_pids = set()
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

    try:
        record = _read_pid_file_record(_INGESTION_PID_PATH, label="ingestion")
        pid_value = int(record.get("pid") or 0)
        if pid_value > 0:
            stale_pids.add(pid_value)
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
        "STALE_INGESTION_CLEANUP_DISCOVERED jobs=%s pid_count=%s budget_s=%s",
        sorted(stale_jobs),
        len(stale_pids),
        float(time_budget_s) if time_budget_s is not None else None,
    )

    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if db_path and os.path.exists(db_path):
        con = None
        try:
            from engine.runtime.storage import connect_ro_direct

            con = connect_ro_direct(timeout_s=2.0, busy_timeout_ms=5000)
            placeholders = ",".join("?" for _ in stale_jobs)
            rows = con.execute(
                f"""
                SELECT job_name, pid
                FROM job_heartbeats
                WHERE job_name IN ({placeholders})
                """,
                tuple(sorted(stale_jobs)),
            ).fetchall()
            for row in rows or []:
                try:
                    if row and row[1]:
                        stale_pids.add(int(row[1]))
                except Exception as e:
                    _log_swallowed(
                        "STALE_INGESTION_HEARTBEAT_ROW_PARSE_FAILED",
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

    for pid in sorted(stale_pids):
        if _budget_exhausted():
            _log_swallowed(
                "STALE_INGESTION_CLEANUP_BUDGET_EXHAUSTED",
                stage="terminate_pids",
                remaining_pids=sorted(int(x) for x in stale_pids if int(x) >= int(pid)),
            )
            break
        if pid <= 0 or pid == os.getpid():
            continue
        if not _pid_is_running_cross_platform(pid):
            continue

        try:
            terminate_timeout_s = 15.0
            if deadline is not None:
                terminate_timeout_s = max(0.25, min(5.0, deadline - time.monotonic()))
            terminated = _terminate_pid_tree_cross_platform(int(pid), timeout_s=terminate_timeout_s)
            if terminated:
                LOG.warning("TERMINATING_STALE_INGESTION_PROCESS pid=%s", pid)
            else:
                _log_swallowed("TERMINATE_STALE_INGESTION_SKIPPED", pid=int(pid))
        except Exception as e:
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

    if db_path and os.path.exists(db_path):
        if _budget_exhausted():
            _log_swallowed(
                "STALE_INGESTION_CLEANUP_BUDGET_EXHAUSTED",
                stage="lock_cleanup",
                db_path=str(db_path),
            )
            return
        try:
            placeholders = ",".join("?" for _ in stale_jobs)
            cleanup_jobs = tuple(sorted(stale_jobs))

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
                "STALE_INGESTION_LOCK_CLEANUP_FAILED db_path=%s jobs=%s error=%s",
                db_path,
                sorted(stale_jobs),
                e,
            )


def _existing_ingestion_runtime_active() -> bool:
    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if not db_path or not os.path.exists(db_path):
        return False

    now_ms = int(time.time() * 1000)
    max_age_ms = int(max(30.0, float(_INGESTION_RESTART_WINDOW_S), float(_INGESTION_WATCHDOG_SLEEP_S) * 4.0) * 1000)

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
            ("ingestion_runtime",),
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
                "INGESTION_ALREADY_ACTIVE_SKIP_SPAWN pid=%s heartbeat_age_ms=%s",
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
            con.execute("DELETE FROM job_heartbeats WHERE job_name = ?", ("ingestion_runtime",))
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

    db_path = str(os.environ.get("DB_PATH") or "").strip()
    if not db_path or not os.path.exists(db_path):
        raise RuntimeError(f"DB_NOT_INITIALIZED_BEFORE_INGESTION:{db_path}")

    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    env["ENGINE_SUPERVISED"] = "1"
    env["ENGINE_LAUNCHED_BY_SUPERVISOR"] = "1"
    env["ENGINE_JOB_NAME"] = "ingestion_runtime"
    env["PYTHONPATH"] = _BASE_DIR + os.pathsep + env.get("PYTHONPATH", "")
    try:
        from services.data_source_manager import (
            apply_safe_no_credential_runtime_environment,
            safe_no_credential_market_data_mode,
        )

        if safe_no_credential_market_data_mode():
            apply_safe_no_credential_runtime_environment(env)
    except Exception as e:
        _log_swallowed("INGESTION_SAFE_ENV_SANITIZE_FAILED", error=str(e))

    creationflags = 0
    start_new_session = False
    if sys.platform.startswith("win"):
        creationflags = subprocess.CREATE_NO_WINDOW | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    else:
        start_new_session = True

    os.makedirs(_LOG_DIR, exist_ok=True)

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


def _start_startup_health_validation_async(*, mode: str) -> threading.Thread:
    global _STARTUP_HEALTH_THREAD

    thread = _STARTUP_HEALTH_THREAD
    if thread is not None and thread.is_alive():
        return thread

    def _runner() -> None:
        try:
            _perform_startup_health_validation(mode=str(mode))
        except Exception as e:
            _log_swallowed("STARTUP_HEALTH_ASYNC_FATAL", mode=str(mode), error=str(e))
            try:
                from dashboard_server import stop_server

                stop_server()
            except Exception as stop_err:
                _log_swallowed("STARTUP_HEALTH_ASYNC_STOP_SERVER_FAILED", error=str(stop_err))

    thread = threading.Thread(
        target=_runner,
        name="startup_health_validation",
        daemon=True,
    )
    thread.start()
    _STARTUP_HEALTH_THREAD = thread
    return thread


def _wait_for_dashboard_bind(*, host: str, port: int, timeout_s: float) -> bool:
    deadline = time.monotonic() + max(0.5, float(timeout_s))
    address = (str(host), int(port))
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(address, timeout=0.25):
                return True
        except OSError:
            time.sleep(0.25)
    return False


def _run_dashboard_server_post_bind_validation(
    run_server,
    *,
    mode: str,
    host: str,
    port: int,
) -> None:
    bind_wait_timeout_s = max(5.0, min(120.0, float(_STARTUP_HEALTH_TIMEOUT_S)))

    def _runner() -> None:
        try:
            _record_phase(
                "STARTUP_HEALTH",
                status="started",
                detail="await_dashboard_bind_before_async_validation",
                extra={
                    "host": str(host),
                    "port": int(port),
                    "timeout_s": float(bind_wait_timeout_s),
                },
            )
            LOG.warning(
                "STARTUP_HEALTH_AWAIT_DASHBOARD_BIND host=%s port=%s timeout_s=%s",
                host,
                port,
                bind_wait_timeout_s,
            )
            if not _wait_for_dashboard_bind(
                host=str(host),
                port=int(port),
                timeout_s=float(bind_wait_timeout_s),
            ):
                raise TimeoutError(f"dashboard_bind_timeout:{host}:{port}")
            _record_phase(
                "STARTUP_HEALTH",
                status="started",
                detail="dashboard_bound_async_validation_scheduled",
                extra={"host": str(host), "port": int(port)},
            )
            LOG.warning(
                "STARTUP_HEALTH_DASHBOARD_BOUND host=%s port=%s starting_async_validation",
                host,
                port,
            )
            _start_startup_health_validation_async(mode=str(mode))
        except Exception as e:
            _record_first_failure(
                "STARTUP_HEALTH",
                e,
                file_path=__file__,
                module="start_system.bind_wait",
            )
            _record_phase(
                "STARTUP_HEALTH",
                status="failed",
                detail=str(e),
                extra={"host": str(host), "port": int(port)},
            )
            _log_swallowed(
                "STARTUP_HEALTH_BIND_WAIT_FAILED",
                mode=str(mode),
                host=str(host),
                port=int(port),
                error=str(e),
            )
            try:
                from dashboard_server import stop_server

                stop_server()
            except Exception as stop_err:
                _log_swallowed(
                    "STARTUP_HEALTH_BIND_WAIT_STOP_SERVER_FAILED",
                    error=str(stop_err),
                )

    threading.Thread(
        target=_runner,
        name="startup_health_bind_wait",
        daemon=True,
    ).start()
    run_server()


def _run_dashboard_server(run_server, *, mode: str) -> None:
    _perform_startup_health_validation(mode=str(mode))
    run_server()


def _coerce_ts_ms(value: Any) -> int:
    try:
        return int(str(value or "0").strip() or "0")
    except Exception as e:
        _log_swallowed("COERCE_TS_MS_FAILED", error=str(e), value_type=type(value).__name__)
        return 0


def _dashboard_stop_requested() -> bool:
    try:
        module = sys.modules.get("dashboard_server")
        event = getattr(module, "_SERVER_STOP_EVENT", None) if module is not None else None
        return bool(callable(getattr(event, "is_set", None)) and event.is_set())
    except Exception as e:
        _log_swallowed("DASHBOARD_STOP_REQUEST_CHECK_FAILED", error=str(e))
        return False


def _dashboard_returned_after_clean_shutdown(
    lifecycle: Dict[str, Any],
    *,
    run_enter_ts_ms: int,
    stop_requested_at_enter: bool = False,
) -> bool:
    try:
        from engine.runtime.lifecycle_state import SHUTTING_DOWN

        if str(lifecycle.get("state") or "").strip().upper() == str(SHUTTING_DOWN):
            return True
    except Exception as e:
        _log_swallowed("DASHBOARD_SHUTTING_DOWN_STATE_IMPORT_FAILED", error=str(e))
        if str(lifecycle.get("state") or "").strip().upper() in {"SHUTTING_DOWN", "SHUTDOWN", "SHUTTING"}:
            return True

    if _dashboard_stop_requested() and not bool(stop_requested_at_enter):
        return True

    clean_ts_ms = _coerce_ts_ms(lifecycle.get("last_clean_shutdown_ts_ms"))
    return bool(clean_ts_ms > 0 and clean_ts_ms >= int(run_enter_ts_ms or 0))


def _handle_signal(signum, _frame) -> None:
    _INGESTION_WATCHDOG_STOP.set()
    try:
        from engine.runtime.lifecycle_state import mark_clean_shutdown
        mark_clean_shutdown()
    except Exception:
        _log_swallowed("MARK_CLEAN_SHUTDOWN_FAILED", signal=int(signum))
    _terminate_ingestion()
    try:
        runtime_shutdown()
    except Exception:
        _log_swallowed("RUNTIME_SHUTDOWN_FAILED", signal=int(signum))
    raise SystemExit(0)


def _db_repair_lock_contention(value: Any) -> bool:
    if isinstance(value, dict):
        parts = [value.get("error"), value.get("detail"), value.get("message")]
        text = " ".join(str(part) for part in parts if part is not None)
    else:
        text = str(value)
    lowered = text.lower()
    return "database is locked" in lowered or "sqlite_busy" in lowered or "sqlite_locked" in lowered


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
    _INGESTION_WATCHDOG_STOP.clear()
    atexit.register(_terminate_ingestion)
    atexit.register(_cleanup_pid_file)
    _write_pid_file()

    try:
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
    except Exception as e:
        LOG.exception("SIGNAL_HANDLER_REGISTRATION_FAILED")
        raise RuntimeError(f"signal_handler_registration_failed:{type(e).__name__}:{e}") from e

    try:
        _run_startup_db_repair()
    except Exception:
        LOG.exception("DB_REPAIR_FAILED")
        raise


def _pick_mode_from_argv_or_env() -> str:
    # Prefer explicit argv, else env, else SAFE
    if len(sys.argv) >= 2 and str(sys.argv[1] or "").strip():
        mode = str(sys.argv[1]).strip().lower()
    else:
        mode = str(os.environ.get("ENGINE_MODE", "") or "").strip().lower() or "safe"

    allowed = {"safe", "shadow", "live"}
    if mode not in allowed:
        raise RuntimeError(f"invalid ENGINE_MODE: {mode}")

    return mode


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
    _record_phase("BOOT", status="ok", detail=f"mode={mode}", extra={"pid": int(os.getpid()), "db_path": str(os.environ.get("DB_PATH") or "")})

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
                _run_dashboard_server(run_server, mode=str(mode))
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
