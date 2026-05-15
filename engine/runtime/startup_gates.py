from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from engine.runtime.config_schema import ConfigError, load_runtime_config
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.platform import default_data_root
from engine.runtime.runtime_meta import meta_get


LOG = get_logger("runtime.startup_gates")
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DIR_PROBE_CACHE_LOCK = threading.Lock()
_DIR_PROBE_CACHE: Dict[str, Dict[str, Any]] = {}
_DIR_PROBE_CACHE_TTL_MS = 30_000
_TRUE_VALUES = {"1", "true", "yes", "y", "on"}
_FALSE_VALUES = {"0", "false", "no", "n", "off"}


def _redacted_pg_dsn() -> str:
    return "<redacted>"


_STARTUP_CONFIG_CONTRACT = {
    "required": [
        "ENGINE_MODE",
        "DB_PATH",
        "DASHBOARD_HOST",
        "DASHBOARD_PORT",
    ],
    "optional": [
        "TRADING_LOGS|LOG_DIR",
        "TRADING_DATA|DATA_DIR",
        "ENGINE_LOG_FILE",
        "START_INGESTION_WITH_SERVER",
        "DASHBOARD_API_TOKEN",
        "TRADING_VALIDATION_TIMEOUT_S",
        "TRADING_STARTUP_HEALTH_TIMEOUT_S",
        "TRADING_STARTUP_HEALTH_POLL_S",
        "TRADING_STARTUP_HEALTH_ASYNC_BIND",
        "TRADING_STALE_INGESTION_CLEANUP_TIMEOUT_S",
        "TRADING_CHALLENGER_RUNTIME_START_TIMEOUT_S",
    ],
    "dev_only": [
        "TRADING_SKIP_RUNTIME_GRAPH_CHECK",
        "TRADING_SKIP_STALE_INGESTION_CLEANUP",
    ],
    "production_only": [
        "DASHBOARD_API_TOKEN (when DASHBOARD_HOST is not loopback)",
    ],
}


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.startup_gates",
        extra=extra or None,
        persist=False,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _repo_root(repo_root: str | Path | None = None) -> Path:
    if repo_root is None:
        return _PROJECT_ROOT
    return Path(repo_root).expanduser().resolve()


def _json_meta_get(key: str) -> Dict[str, Any]:
    try:
        raw = str(meta_get(str(key or ""), "") or "").strip()
        if not raw:
            return {}
        payload = json.loads(raw)
        return dict(payload) if isinstance(payload, dict) else {}
    except Exception as e:
        _warn_nonfatal("STARTUP_GATES_META_GET_FAILED", e, key=str(key))
        return {}


def _compact_text(value: Any, *, limit: int = 240) -> str:
    text = str(value or "")
    if len(text) <= int(limit):
        return text
    return text[: max(0, int(limit) - 3)] + "..."


def _compact_mapping(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    keep = {
        "ok",
        "status",
        "state",
        "detail",
        "reason",
        "error",
        "error_type",
        "started",
        "skipped",
        "enabled",
        "degraded",
        "handler_count",
        "pid",
        "exit_code",
        "ts_ms",
        "started_ts_ms",
        "finished_ts_ms",
        "last_ok_ts_ms",
    }
    out: Dict[str, Any] = {}
    for key, raw in value.items():
        key_s = str(key)
        if key_s not in keep:
            continue
        if isinstance(raw, dict):
            out[key_s] = _compact_mapping(raw)
        elif isinstance(raw, list):
            out[key_s] = {"count": len(raw)}
        elif isinstance(raw, str):
            out[key_s] = _compact_text(raw)
        else:
            out[key_s] = raw
    return out


def _compact_boot_diagnostics(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, raw in value.items():
        key_s = str(key)
        if isinstance(raw, dict):
            out[key_s] = _compact_mapping(raw)
        elif isinstance(raw, list):
            out[key_s] = {"count": len(raw)}
        elif isinstance(raw, str):
            out[key_s] = _compact_text(raw)
        else:
            out[key_s] = raw
    return out


def _normalize_host(host: str) -> str:
    text = str(host or "").strip()
    if not text:
        return "127.0.0.1"
    if text == "localhost":
        return "127.0.0.1"
    return text


def _is_loopback_host(host: str) -> bool:
    text = _normalize_host(host).lower()
    return text in {"127.0.0.1", "::1"}


def _parse_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    text = str(raw).strip().lower()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    raise ConfigError(f"Invalid bool for {name}: {raw}")


def _parse_int(name: str, default: int, *, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
    raw = os.environ.get(name)
    text = str(raw if raw is not None else default).strip()
    try:
        value = int(text)
    except Exception as e:
        raise ConfigError(f"Invalid int for {name}: {raw}") from e
    if minimum is not None and value < int(minimum):
        raise ConfigError(f"{name} must be >= {int(minimum)}")
    if maximum is not None and value > int(maximum):
        raise ConfigError(f"{name} must be <= {int(maximum)}")
    return value


def _parse_float(name: str, default: float, *, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
    raw = os.environ.get(name)
    text = str(raw if raw is not None else default).strip()
    try:
        value = float(text)
    except Exception as e:
        raise ConfigError(f"Invalid float for {name}: {raw}") from e
    if minimum is not None and value < float(minimum):
        raise ConfigError(f"{name} must be >= {float(minimum)}")
    if maximum is not None and value > float(maximum):
        raise ConfigError(f"{name} must be <= {float(maximum)}")
    return value


def _resolve_startup_paths(repo_root: str | Path | None = None) -> Dict[str, Path]:
    root = _repo_root(repo_root)
    log_dir = Path(
        os.environ.get("TRADING_LOGS")
        or os.environ.get("LOG_DIR")
        or str((root / "logs").resolve())
    ).expanduser().resolve()
    data_dir = Path(
        os.environ.get("TRADING_DATA")
        or os.environ.get("DATA_DIR")
        or str((root / "data").resolve())
    ).expanduser().resolve()
    db_path = Path(str(os.environ.get("DB_PATH") or default_data_root())).expanduser().resolve()
    log_file = Path(
        os.environ.get("ENGINE_LOG_FILE")
        or str((log_dir / "engine.log").resolve())
    ).expanduser().resolve()
    return {
        "repo_root": root,
        "log_dir": log_dir,
        "data_dir": data_dir,
        "db_path": db_path,
        "db_parent": db_path.parent.resolve(),
        "log_file": log_file,
        "ui_dir": (root / "ui").resolve(),
    }


def get_startup_config_contract() -> Dict[str, list[str]]:
    return {
        key: list(values)
        for key, values in _STARTUP_CONFIG_CONTRACT.items()
    }


def get_startup_config_snapshot(repo_root: str | Path | None = None) -> Dict[str, Any]:
    errors: list[Dict[str, str]] = []
    parsed: Dict[str, Any] = {}
    paths = _resolve_startup_paths(repo_root)

    try:
        runtime_cfg = load_runtime_config()
        parsed["runtime_config_loaded"] = True
        parsed["runtime_env"] = str(getattr(runtime_cfg, "env", ""))
    except ConfigError as e:
        errors.append(
            {
                "key": "runtime_config",
                "detail": str(e),
                "component": "engine.runtime.config_schema",
            }
        )
        parsed["runtime_config_loaded"] = False

    try:
        engine_mode = str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"
        if engine_mode not in {"safe", "shadow", "live", "dev", "development", "paper"}:
            raise ConfigError(f"Invalid ENGINE_MODE: {engine_mode}")
        parsed["engine_mode"] = engine_mode
    except ConfigError as e:
        errors.append({"key": "ENGINE_MODE", "detail": str(e), "component": "runtime"})

    try:
        host = _normalize_host(str(os.environ.get("DASHBOARD_HOST", "127.0.0.1") or "127.0.0.1"))
        if not host:
            raise ConfigError("DASHBOARD_HOST must be non-empty")
        parsed["dashboard_host"] = host
    except ConfigError as e:
        errors.append({"key": "DASHBOARD_HOST", "detail": str(e), "component": "dashboard_server"})

    try:
        parsed["dashboard_port"] = _parse_int("DASHBOARD_PORT", 8000, minimum=1, maximum=65535)
    except ConfigError as e:
        errors.append({"key": "DASHBOARD_PORT", "detail": str(e), "component": "dashboard_server"})

    try:
        parsed["startup_health_timeout_s"] = _parse_float(
            "TRADING_STARTUP_HEALTH_TIMEOUT_S",
            180.0,
            minimum=1.0,
            maximum=3600.0,
        )
    except ConfigError as e:
        errors.append(
            {
                "key": "TRADING_STARTUP_HEALTH_TIMEOUT_S",
                "detail": str(e),
                "component": "start_system",
            }
        )

    try:
        parsed["validation_timeout_s"] = _parse_int(
            "TRADING_VALIDATION_TIMEOUT_S",
            180,
            minimum=30,
            maximum=3600,
        )
    except ConfigError as e:
        errors.append(
            {
                "key": "TRADING_VALIDATION_TIMEOUT_S",
                "detail": str(e),
                "component": "start_system",
            }
        )

    try:
        parsed["startup_health_poll_s"] = _parse_float(
            "TRADING_STARTUP_HEALTH_POLL_S",
            2.0,
            minimum=0.1,
            maximum=60.0,
        )
    except ConfigError as e:
        errors.append(
            {
                "key": "TRADING_STARTUP_HEALTH_POLL_S",
                "detail": str(e),
                "component": "start_system",
            }
        )

    try:
        parsed["startup_health_async_bind"] = _parse_bool("TRADING_STARTUP_HEALTH_ASYNC_BIND", True)
    except ConfigError as e:
        errors.append(
            {
                "key": "TRADING_STARTUP_HEALTH_ASYNC_BIND",
                "detail": str(e),
                "component": "start_system",
            }
        )

    try:
        parsed["startup_health_fail_open"] = _parse_bool("TRADING_STARTUP_HEALTH_FAIL_OPEN", False)
    except ConfigError as e:
        errors.append(
            {
                "key": "TRADING_STARTUP_HEALTH_FAIL_OPEN",
                "detail": str(e),
                "component": "start_system",
            }
        )

    try:
        parsed["start_ingestion_with_server"] = _parse_bool("START_INGESTION_WITH_SERVER", True)
    except ConfigError as e:
        errors.append(
            {
                "key": "START_INGESTION_WITH_SERVER",
                "detail": str(e),
                "component": "start_system",
            }
        )

    try:
        parsed["stale_ingestion_cleanup_timeout_s"] = _parse_float(
            "TRADING_STALE_INGESTION_CLEANUP_TIMEOUT_S",
            5.0,
            minimum=0.5,
            maximum=60.0,
        )
    except ConfigError as e:
        errors.append(
            {
                "key": "TRADING_STALE_INGESTION_CLEANUP_TIMEOUT_S",
                "detail": str(e),
                "component": "start_system",
            }
        )

    try:
        parsed["challenger_runtime_start_timeout_s"] = _parse_float(
            "TRADING_CHALLENGER_RUNTIME_START_TIMEOUT_S",
            2.0,
            minimum=0.5,
            maximum=60.0,
        )
    except ConfigError as e:
        errors.append(
            {
                "key": "TRADING_CHALLENGER_RUNTIME_START_TIMEOUT_S",
                "detail": str(e),
                "component": "start_system",
            }
        )

    if bool(parsed.get("startup_health_fail_open")):
        errors.append(
            {
                "key": "TRADING_STARTUP_HEALTH_FAIL_OPEN",
                "detail": "TRADING_STARTUP_HEALTH_FAIL_OPEN is not allowed in hardened startup mode",
                "component": "start_system",
            }
        )

    if not bool(parsed.get("startup_health_async_bind", True)):
        errors.append(
            {
                "key": "TRADING_STARTUP_HEALTH_ASYNC_BIND",
                "detail": "TRADING_STARTUP_HEALTH_ASYNC_BIND must be enabled for hardened startup post-bind validation",
                "component": "start_system",
            }
        )

    parsed["dashboard_api_token_present"] = bool(str(os.environ.get("DASHBOARD_API_TOKEN", "") or "").strip())
    if not _is_loopback_host(str(parsed.get("dashboard_host") or "")) and not parsed["dashboard_api_token_present"]:
        errors.append(
            {
                "key": "DASHBOARD_API_TOKEN",
                "detail": "DASHBOARD_API_TOKEN is required when DASHBOARD_HOST is not loopback",
                "component": "dashboard_server",
            }
        )

    parsed["paths"] = {
        key: str(value)
        for key, value in paths.items()
    }
    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "parsed": parsed,
        "config_contract": get_startup_config_contract(),
    }


def _directory_present(path: Path) -> Dict[str, Any]:
    return {
        "path": str(path),
        "exists": bool(path.exists()),
        "is_dir": bool(path.is_dir()),
    }


def _probe_directory_writable(path: Path) -> Dict[str, Any]:
    try:
        resolved = str(path.resolve())
    except Exception:
        resolved = str(path)
    now_ms = _now_ms()
    with _DIR_PROBE_CACHE_LOCK:
        cached = dict(_DIR_PROBE_CACHE.get(resolved) or {})
        if cached and (now_ms - int(cached.get("ts_ms") or 0)) <= _DIR_PROBE_CACHE_TTL_MS:
            return cached

    result = {
        "ok": False,
        "path": resolved,
        "detail": "",
        "ts_ms": now_ms,
    }
    try:
        path.mkdir(parents=True, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(prefix="startup-gate-", suffix=".tmp", dir=str(path))
        os.close(fd)
        os.remove(temp_path)
        result["ok"] = True
        result["detail"] = "ok"
    except Exception as e:
        result["ok"] = False
        result["detail"] = f"log_path_not_writable:{type(e).__name__}:{e}"
    with _DIR_PROBE_CACHE_LOCK:
        _DIR_PROBE_CACHE[resolved] = dict(result)
    return result


def _required_ui_assets(repo_root: str | Path | None = None) -> tuple[Path, ...]:
    root = _repo_root(repo_root)
    return (
        (root / "ui" / "dashboard.html").resolve(),
        (root / "ui" / "dashboard.js").resolve(),
        (root / "ui" / "styles.tech.css").resolve(),
    )


def _probe_port_available(host: str, port: int) -> Dict[str, Any]:
    bind_host = _normalize_host(host)
    family = socket.AF_INET6 if ":" in bind_host and bind_host != "127.0.0.1" else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((bind_host, int(port)))
    except OSError as e:
        return {
            "ok": False,
            "host": bind_host,
            "port": int(port),
            "detail": f"port_in_use:{type(e).__name__}:{e}",
        }
    finally:
        try:
            sock.close()
        except Exception as e:
            _warn_nonfatal("STARTUP_GATES_SOCKET_CLOSE_FAILED", e, host=bind_host, port=int(port))
    return {
        "ok": True,
        "host": bind_host,
        "port": int(port),
        "detail": "ok",
    }


def _db_reachable(db_path: Path) -> Dict[str, Any]:
    del db_path
    try:
        from engine.runtime.storage import connect_ro_direct

        con = connect_ro_direct(timeout_s=1.0)
        try:
            con.execute("SELECT 1").fetchone()
        finally:
            con.close()
        return {"ok": True, "detail": "ok", "dsn": _redacted_pg_dsn()}
    except Exception as e:
        return {
            "ok": False,
            "detail": f"database_unreachable:{type(e).__name__}:{e}",
            "dsn": _redacted_pg_dsn(),
        }


def _gate(
    name: str,
    ok: bool,
    *,
    blocking: bool,
    component: str,
    detail: str,
    config_keys: Optional[Iterable[str]] = None,
    dependency: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "id": str(name),
        "ok": bool(ok),
        "blocking": bool(blocking),
        "level": "ok" if bool(ok) else ("blocking" if bool(blocking) else "degraded"),
        "component": str(component),
        "detail": str(detail or ("ok" if ok else "failed")),
        "dependency": str(dependency or ""),
        "config_keys": [str(key) for key in (config_keys or []) if str(key).strip()],
        "ts_ms": _now_ms(),
    }
    if extra:
        payload.update(dict(extra))
    return payload


def _summarize_gate_failures(gates: Dict[str, Dict[str, Any]]) -> tuple[list[str], list[str]]:
    blocking: list[str] = []
    reasons: list[str] = []
    for name, payload in gates.items():
        if bool(payload.get("ok")):
            continue
        if bool(payload.get("blocking")):
            blocking.append(str(name))
        reasons.append(f"{name}:{payload.get('detail')}")
    return blocking, reasons


def evaluate_prebind_startup_gates(
    *,
    repo_root: str | Path | None = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    require_ui_assets: bool = True,
    api_dependencies: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    config = get_startup_config_snapshot(repo_root)
    paths = _resolve_startup_paths(repo_root)
    resolved_host = _normalize_host(str(host or config.get("parsed", {}).get("dashboard_host") or "127.0.0.1"))
    resolved_port = int(port or config.get("parsed", {}).get("dashboard_port") or 8000)
    ui_assets = _required_ui_assets(repo_root)
    log_probe = _probe_directory_writable(paths["log_dir"])
    port_probe = _probe_port_available(resolved_host, resolved_port)

    missing_dirs = [
        entry["path"]
        for entry in (
            _directory_present(paths["log_dir"]),
            _directory_present(paths["data_dir"]),
            _directory_present(paths["db_parent"]),
        )
        if not bool(entry["exists"] and entry["is_dir"])
    ]
    missing_ui_assets = [str(path) for path in ui_assets if not path.exists()]

    gates = {
        "config_valid": _gate(
            "config_valid",
            bool(config.get("ok")),
            blocking=not bool(config.get("ok")),
            component="runtime_config",
            detail=(
                "ok"
                if bool(config.get("ok"))
                else "; ".join(str(item.get("detail") or "") for item in list(config.get("errors") or [])[:5])
            ),
            config_keys=[str(item.get("key") or "") for item in list(config.get("errors") or [])],
            extra={"config_contract": config.get("config_contract"), "errors": list(config.get("errors") or [])},
        ),
        "log_path_writable": _gate(
            "log_path_writable",
            bool(log_probe.get("ok")),
            blocking=not bool(log_probe.get("ok")),
            component="logging",
            detail=str(log_probe.get("detail") or "log_path_not_writable"),
            config_keys=["TRADING_LOGS", "LOG_DIR", "ENGINE_LOG_FILE"],
            extra={"path": str(paths["log_dir"])},
        ),
        "required_directories_present": _gate(
            "required_directories_present",
            len(missing_dirs) == 0,
            blocking=len(missing_dirs) > 0,
            component="filesystem",
            detail="ok" if not missing_dirs else f"missing_directories:{','.join(missing_dirs)}",
            config_keys=["TRADING_LOGS", "LOG_DIR", "TRADING_DATA", "DATA_DIR", "DB_PATH"],
            extra={"paths": {key: str(value) for key, value in paths.items() if key in {"log_dir", "data_dir", "db_parent"}}},
        ),
        "ui_static_assets_present": _gate(
            "ui_static_assets_present",
            (not require_ui_assets) or len(missing_ui_assets) == 0,
            blocking=bool(require_ui_assets and missing_ui_assets),
            component="ui",
            detail="ok" if not missing_ui_assets else f"missing_ui_assets:{','.join(missing_ui_assets)}",
            extra={"required": bool(require_ui_assets), "assets": [str(path) for path in ui_assets]},
        ),
        "no_port_binding_conflict": _gate(
            "no_port_binding_conflict",
            bool(port_probe.get("ok")),
            blocking=not bool(port_probe.get("ok")),
            component="listener",
            detail=str(port_probe.get("detail") or "port_conflict"),
            config_keys=["DASHBOARD_HOST", "DASHBOARD_PORT"],
            dependency="dashboard_listener",
            extra={"host": resolved_host, "port": resolved_port},
        ),
    }

    if api_dependencies is not None:
        api_ok = bool(api_dependencies.get("ok"))
        gates["required_api_dependencies_available"] = _gate(
            "required_api_dependencies_available",
            api_ok,
            blocking=not api_ok,
            component="api",
            detail=(
                "ok"
                if api_ok
                else str(api_dependencies.get("detail") or api_dependencies.get("error") or "api_dependencies_invalid")
            ),
            dependency="dashboard_routes",
            extra=dict(api_dependencies or {}),
        )

    blocking_gates, reasons = _summarize_gate_failures(gates)
    return {
        "ok": len(blocking_gates) == 0,
        "phase": "prebind",
        "ts_ms": _now_ms(),
        "gates": gates,
        "blocking_gates": blocking_gates,
        "reasons": reasons,
    }


def evaluate_runtime_startup_gates(
    *,
    repo_root: str | Path | None = None,
    health: Optional[Dict[str, Any]] = None,
    db_validation: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    repo = _repo_root(repo_root)
    health = dict(health or {})
    db_validation = dict(db_validation or {})
    config = get_startup_config_snapshot(repo)
    paths = _resolve_startup_paths(repo)
    boot_diagnostics = _json_meta_get("dashboard_boot_diagnostics")
    lifecycle = dict(health.get("lifecycle") or {})
    resolved_host = str(config.get("parsed", {}).get("dashboard_host") or "127.0.0.1")
    resolved_port = int(config.get("parsed", {}).get("dashboard_port") or 8000)
    port_probe = _probe_port_available(resolved_host, resolved_port)
    dashboard_bound_meta = bool(str(meta_get("dashboard_bound_ts_ms", "") or "").strip()) or bool(
        str(lifecycle.get("dashboard_bound_ts_ms") or "").strip()
    )
    # Persisted lifecycle state can survive a prior process crash or shutdown.
    # Only treat dashboard bind as active when boot diagnostics are present or
    # the listener is still actually occupying the configured socket.
    dashboard_bound = bool(boot_diagnostics) or (dashboard_bound_meta and not bool(port_probe.get("ok")))
    log_probe = _probe_directory_writable(paths["log_dir"])
    db_probe = _db_reachable(paths["db_path"])
    ui_assets = _required_ui_assets(repo)
    missing_ui_assets = [str(path) for path in ui_assets if not path.exists()]
    missing_dirs = [
        entry["path"]
        for entry in (
            _directory_present(paths["log_dir"]),
            _directory_present(paths["data_dir"]),
            _directory_present(paths["db_parent"]),
        )
        if not bool(entry["exists"] and entry["is_dir"])
    ]

    db_health = dict(health.get("db") or {})
    model_cache = dict(health.get("model_cache") or {})
    runtime_price_cache = dict(health.get("runtime_price_cache") or {})
    event_bus = dict(health.get("event_bus") or {})

    post_bind_boot = dict(boot_diagnostics.get("post_bind_boot") or {})
    runtime_bootstrap = dict(boot_diagnostics.get("runtime_bootstrap") or {})
    startup_orchestrator = dict(boot_diagnostics.get("startup_orchestrator") or {})
    api_dependencies = dict(boot_diagnostics.get("api_dependencies") or {})
    validation_boot_mode = (
        str(os.environ.get("ENGINE_PRIMARY_BOOTSTRAP_DONE", "")).strip().lower() in _TRUE_VALUES
        and str(os.environ.get("AUTO_BOOT_DAEMONS", "")).strip().lower() in _FALSE_VALUES
    )

    core_service_problems: list[str] = []
    if dashboard_bound or boot_diagnostics:
        if bool(post_bind_boot.get("started")):
            if post_bind_boot.get("ok") is not True:
                detail = str(post_bind_boot.get("error") or "post_bind_boot_pending")
                core_service_problems.append(f"post_bind_boot:{detail}")
        elif dashboard_bound and not validation_boot_mode:
            core_service_problems.append("post_bind_boot:not_started")

        if runtime_bootstrap and runtime_bootstrap.get("ok") is False:
            core_service_problems.append(
                f"runtime_bootstrap:{runtime_bootstrap.get('error') or runtime_bootstrap.get('errors') or 'failed'}"
            )
        if startup_orchestrator and startup_orchestrator.get("ok") is False:
            core_service_problems.append(
                f"startup_orchestrator:{startup_orchestrator.get('error') or startup_orchestrator.get('detail') or 'failed'}"
            )

    if model_cache and bool(model_cache.get("loaded")) is False:
        core_service_problems.append(str(model_cache.get("last_error") or "model_cache_not_loaded"))
    if runtime_price_cache and bool(runtime_price_cache.get("initialized")) is False:
        core_service_problems.append(str(runtime_price_cache.get("detail") or "runtime_price_cache_uninitialized"))
    if event_bus and not (bool(event_bus.get("started")) and bool(event_bus.get("ok", True))):
        core_service_problems.append(str(event_bus.get("detail") or "event_bus_not_started"))

    if dashboard_bound:
        port_gate = _gate(
            "no_port_binding_conflict",
            True,
            blocking=False,
            component="listener",
            detail="dashboard_listener_bound",
            config_keys=["DASHBOARD_HOST", "DASHBOARD_PORT"],
        )
    else:
        port_gate = _gate(
            "no_port_binding_conflict",
            bool(port_probe.get("ok")),
            blocking=not bool(port_probe.get("ok")),
            component="listener",
            detail=str(port_probe.get("detail") or "port_conflict"),
            config_keys=["DASHBOARD_HOST", "DASHBOARD_PORT"],
            dependency="dashboard_listener",
            extra={"host": port_probe.get("host"), "port": port_probe.get("port")},
        )

    api_gate_ok = True
    api_gate_detail = "unreported"
    api_gate_blocking = False
    if dashboard_bound or api_dependencies:
        api_gate_ok = bool(api_dependencies.get("ok"))
        api_gate_blocking = not api_gate_ok
        api_gate_detail = (
            "ok"
            if api_gate_ok
            else str(api_dependencies.get("detail") or api_dependencies.get("error") or "api_dependencies_pending")
        )

    schema_missing = [str(item) for item in list(db_validation.get("missing_tables") or []) if str(item).strip()]
    schema_missing_columns = {
        str(table): [str(column) for column in list(columns or []) if str(column).strip()]
        for table, columns in dict(
            db_validation.get("missing_columns")
            or db_validation.get("missing_cols")
            or {}
        ).items()
        if str(table).strip()
    }
    schema_missing_indexes = [
        str(item)
        for item in list(db_validation.get("missing_indexes") or [])
        if str(item).strip()
    ]
    schema_version_ok = bool(db_validation.get("schema_version_ok", True))
    quick_check_value = str(db_validation.get("quick_check") or "").strip().lower()
    quick_check_skipped = bool(db_validation.get("quick_check_skipped")) or quick_check_value in {
        "not_applicable",
        "skipped",
        "reused_prior_db_repair",
    }
    schema_ok = (
        bool(db_validation.get("ok"))
        and not schema_missing
        and not schema_missing_columns
        and not schema_missing_indexes
        and bool(schema_version_ok)
        and (quick_check_value == "ok" or quick_check_skipped)
    )

    gates = {
        "config_valid": _gate(
            "config_valid",
            bool(config.get("ok")),
            blocking=not bool(config.get("ok")),
            component="runtime_config",
            detail=(
                "ok"
                if bool(config.get("ok"))
                else "; ".join(str(item.get("detail") or "") for item in list(config.get("errors") or [])[:5])
            ),
            config_keys=[str(item.get("key") or "") for item in list(config.get("errors") or [])],
            extra={"config_contract": config.get("config_contract"), "errors": list(config.get("errors") or [])},
        ),
        "database_reachable": _gate(
            "database_reachable",
            bool(db_probe.get("ok")) and bool(db_health.get("ok", True)),
            blocking=not (bool(db_probe.get("ok")) and bool(db_health.get("ok", True))),
            component="database",
            detail=(
                "ok"
                if bool(db_probe.get("ok")) and bool(db_health.get("ok", True))
                else str(db_health.get("error") or db_probe.get("detail") or "database_unreachable")
            ),
            config_keys=["DB_PATH"],
            dependency="postgres",
            extra={"dsn": _redacted_pg_dsn()},
        ),
        "schema_valid": _gate(
            "schema_valid",
            schema_ok,
            blocking=not schema_ok,
            component="database",
            detail=(
                "ok"
                if schema_ok
                else (
                    f"schema_invalid:quick_check={db_validation.get('quick_check')} "
                    f"missing_tables={schema_missing or []} "
                    f"missing_columns={schema_missing_columns or {}} "
                    f"missing_indexes={schema_missing_indexes or []} "
                    f"schema_version={db_validation.get('schema_version')} "
                    f"expected_schema_version={db_validation.get('expected_schema_version')} "
                    f"schema_status={db_validation.get('schema_status')}"
                )
            ),
            dependency="postgres_schema",
            extra={
                "db_validation": dict(db_validation or {}),
                "quick_check_skipped": bool(quick_check_skipped),
            },
        ),
        "log_path_writable": _gate(
            "log_path_writable",
            bool(log_probe.get("ok")),
            blocking=not bool(log_probe.get("ok")),
            component="logging",
            detail=str(log_probe.get("detail") or "log_path_not_writable"),
            config_keys=["TRADING_LOGS", "LOG_DIR", "ENGINE_LOG_FILE"],
            extra={"path": str(paths["log_dir"])},
        ),
        "required_directories_present": _gate(
            "required_directories_present",
            len(missing_dirs) == 0,
            blocking=len(missing_dirs) > 0,
            component="filesystem",
            detail="ok" if not missing_dirs else f"missing_directories:{','.join(missing_dirs)}",
            config_keys=["TRADING_LOGS", "LOG_DIR", "TRADING_DATA", "DATA_DIR", "DB_PATH"],
            extra={"paths": {key: str(value) for key, value in paths.items() if key in {"log_dir", "data_dir", "db_parent"}}},
        ),
        "core_services_initialized": _gate(
            "core_services_initialized",
            len(core_service_problems) == 0,
            blocking=len(core_service_problems) > 0,
            component="runtime",
            detail="ok" if not core_service_problems else "; ".join(core_service_problems[:5]),
            dependency="dashboard_bootstrap",
            extra={"boot_diagnostics": _compact_boot_diagnostics(boot_diagnostics)},
        ),
        "required_api_dependencies_available": _gate(
            "required_api_dependencies_available",
            api_gate_ok,
            blocking=api_gate_blocking,
            component="api",
            detail=api_gate_detail,
            dependency="dashboard_routes",
            extra={"boot_diagnostics": _compact_boot_diagnostics(api_dependencies)},
        ),
        "ui_static_assets_present": _gate(
            "ui_static_assets_present",
            len(missing_ui_assets) == 0,
            blocking=len(missing_ui_assets) > 0,
            component="ui",
            detail="ok" if not missing_ui_assets else f"missing_ui_assets:{','.join(missing_ui_assets)}",
            extra={"assets": [str(path) for path in ui_assets]},
        ),
        "no_port_binding_conflict": port_gate,
    }

    blocking_gates, reasons = _summarize_gate_failures(gates)
    impacted_components = sorted(
        {
            str(payload.get("component") or "")
            for payload in gates.values()
            if not bool(payload.get("ok")) and str(payload.get("component") or "").strip()
        }
    )

    return {
        "ok": len(blocking_gates) == 0,
        "phase": "runtime",
        "ts_ms": _now_ms(),
        "gates": gates,
        "blocking_gates": blocking_gates,
        "reasons": reasons,
        "impacted_components": impacted_components,
        "config_contract": config.get("config_contract"),
        "config_errors": list(config.get("errors") or []),
    }


def assert_prebind_startup_gates(
    *,
    repo_root: str | Path | None = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    require_ui_assets: bool = True,
    api_dependencies: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = evaluate_prebind_startup_gates(
        repo_root=repo_root,
        host=host,
        port=port,
        require_ui_assets=require_ui_assets,
        api_dependencies=api_dependencies,
    )
    if not bool(payload.get("ok")):
        raise RuntimeError(
            "startup_prebind_gates_failed:"
            + ",".join(str(item) for item in list(payload.get("blocking_gates") or []))
            + ":"
            + "; ".join(str(item) for item in list(payload.get("reasons") or []))
        )
    return payload
