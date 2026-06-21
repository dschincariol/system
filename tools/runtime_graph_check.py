"""
FILE: runtime_graph_check.py

Tooling or validation script for `runtime_graph_check`.
"""

import argparse
import base64
import importlib
import os
import pkgutil
import py_compile
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Dict, List, MutableMapping, Optional, Set, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENGINE_DIR = os.path.join(ROOT, "engine")
RUNTIME_DIR = os.path.join(ENGINE_DIR, "runtime")
LOG_DIR = os.path.join(ROOT, "var", "log")
DATA_DIR = os.path.join(ROOT, "var", "db")
_VALIDATION_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")
_LOCAL_EXTERNAL_SERVICE_POP_KEYS = (
    "TS_PG_DSN",
    "TS_PG_DSN_FILE",
    "PG_DSN",
    "TIMESCALE_DSN",
    "TIMESCALE_URL",
    "TIMESCALE_DATABASE_URL",
    "TIMESCALE_PRICES_DSN",
    "TIMESCALE_PRICES_URL",
    "TIMESCALE_PRICES_DATABASE_URL",
    "LIVE_CACHE_REDIS_URL",
    "REDIS_URL",
    "REDIS_CACHE_URL",
    "TS_REDIS_URL",
    "OBJECT_STORE_ENDPOINT",
    "OBJECT_STORE_BUCKET",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_SECRET_KEY",
    "OBJECT_STORE_SESSION_TOKEN",
    "MINIO_ENDPOINT",
    "MINIO_BUCKET",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)
_LOCAL_EXTERNAL_SERVICE_DEFAULTS = {
    "TS_STORAGE_BACKEND": "sqlite",
    "TIMESCALE_ENABLED": "0",
    "TIMESCALE_PRICES_ENABLED": "0",
    "TELEMETRY_READ_BACKEND": "sqlite",
    "PRICE_READ_BACKEND": "sqlite",
    "LIVE_CACHE_BACKEND": "memory",
    "PREFLIGHT_REQUIRE_TIMESCALE": "0",
    "PREFLIGHT_REQUIRE_REDIS": "0",
    "PREFLIGHT_REQUIRE_OBJECT_STORAGE": "0",
}
_LOCAL_SECRET_INLINE_POP_KEYS = (
    "DASHBOARD_API_TOKEN",
    "OPERATOR_API_TOKEN",
    "DATA_SOURCE_MASTER_KEY",
    "TRADING_MASTER_KEY",
    "APP_MASTER_KEY",
    "BACKUP_EVIDENCE_HMAC_KEY",
    "ALPACA_KEY_ID",
    "ALPACA_SECRET_KEY",
    "POLYGON_API_KEY",
    "POLYGON_KEY",
    "TRADIER_API_TOKEN",
    "OPENAI_API_KEY",
    "TIMESCALE_PASSWORD",
    "TS_PG_PASSWORD",
    "PGPASSWORD",
    "TS_PG_PASSWORD_APP",
    "TS_PG_APP_PASSWORD",
    "TS_PG_PASSWORD_INGEST",
    "TS_PG_INGEST_PASSWORD",
    "TS_PG_PASSWORD_READER",
    "TS_PG_READER_PASSWORD",
    "REDIS_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_SECRET_KEY",
    "OBJECT_STORE_SESSION_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)
_LOCAL_VALIDATION_FILE_SECRETS = {
    "DATA_SOURCE_MASTER_KEY_FILE": _VALIDATION_DATA_SOURCE_MASTER_KEY,
    "DASHBOARD_API_TOKEN_FILE": "validation-dashboard-token-0000000000000000",
    "OPERATOR_API_TOKEN_FILE": "validation-operator-token-0000000000000000",
}

if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _env_truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _production_validation_profile(env: Dict[str, str]) -> bool:
    if _env_truthy(env.get("TRADING_VALIDATION_REQUIRE_PROD_DEPS")):
        return True
    if _startup_validation_mode(env):
        return False
    profile_values = (
        env.get("TS_ENV"),
        env.get("ENV"),
        env.get("NODE_ENV"),
        env.get("ENGINE_MODE"),
        env.get("OPERATOR_MODE"),
    )
    return any(str(value or "").strip().lower() in {"prod", "production", "live"} for value in profile_values)


def _startup_validation_mode(env: MutableMapping[str, str]) -> bool:
    return str(env.get("TRADING_VALIDATION_MODE", "") or "").strip().lower() == "startup"


def _local_startup_validation_profile(env: MutableMapping[str, str]) -> bool:
    return bool(_startup_validation_mode(env) and not _production_validation_profile(dict(env)))


def _scrub_local_external_service_env(env: MutableMapping[str, str]) -> None:
    for key in _LOCAL_EXTERNAL_SERVICE_POP_KEYS:
        env[key] = ""
    env.update(_LOCAL_EXTERNAL_SERVICE_DEFAULTS)


def _write_validation_secret_file(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(value), encoding="utf-8")
    path.chmod(0o600)


def _local_secret_source_env_keys() -> Tuple[str, ...]:
    try:
        from engine.runtime.secret_sources import SECRET_ENV_SPECS
    except Exception:
        keys: Set[str] = set(_LOCAL_SECRET_INLINE_POP_KEYS)
        for key in tuple(_LOCAL_SECRET_INLINE_POP_KEYS):
            keys.add(f"{key}_FILE")
            keys.add(f"{key}_SECRET")
        keys.update(_LOCAL_VALIDATION_FILE_SECRETS.keys())
        return tuple(sorted(keys))

    keys: Set[str] = set()
    for spec in SECRET_ENV_SPECS:
        keys.add(str(spec.key))
        keys.update(str(item) for item in spec.file_envs)
        keys.update(str(item) for item in spec.secret_envs)
    return tuple(sorted(keys))


def _configure_local_secret_source_policy(env: MutableMapping[str, str], root_dir: Path) -> None:
    if _production_validation_profile(dict(env)):
        return
    for key in _local_secret_source_env_keys():
        env[key] = ""
    for key in ("TS_SECRETS_PROVIDER", "CREDENTIALS_DIRECTORY", "TS_DEV_SECRETS_DIR"):
        env[key] = ""
    root_dir.mkdir(parents=True, exist_ok=True)
    policy_root = root_dir / "empty_secret_policy_repo"
    policy_root.mkdir(parents=True, exist_ok=True)
    env["TRADING_SECRET_POLICY_REPO_ROOT"] = str(policy_root)
    secret_dir = root_dir / "secrets"
    for env_name, value in _LOCAL_VALIDATION_FILE_SECRETS.items():
        path = secret_dir / env_name.lower()
        _write_validation_secret_file(path, value)
        env[env_name] = str(path)


def _is_missing_optional_module(error: ModuleNotFoundError, module_name: str) -> bool:
    return getattr(error, "name", "") == module_name or f"No module named '{module_name}'" in str(error)


def bootstrap_validation_env(extra_env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    if extra_env:
        for key, value in dict(extra_env).items():
            if value is None:
                continue
            os.environ[str(key)] = str(value)

    os.environ.setdefault("ENGINE_SUPERVISED", "1")
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    os.environ.setdefault("TRADING_LOGS", LOG_DIR)
    os.environ.setdefault("LOG_DIR", LOG_DIR)
    os.environ.setdefault("TRADING_DATA", DATA_DIR)
    os.environ.setdefault("DATA_DIR", DATA_DIR)
    os.environ.setdefault("DB_PATH", os.path.join(DATA_DIR, "trading.db"))
    # Validation imports should stay read-only. Import smoke for entrypoints like
    # dashboard_server must not contend with a live ingestion process by trying to
    # bootstrap the control plane or auto-start background daemons.
    os.environ["DATA_SOURCE_MANAGER_READ_ONLY"] = "1"
    os.environ["ENGINE_PRIMARY_BOOTSTRAP_DONE"] = "1"
    os.environ["AUTO_BOOT_DAEMONS"] = "0"
    if not str(os.environ.get("TRADING_VALIDATION_IMPORT_DASHBOARD", "") or "").strip():
        os.environ["TRADING_VALIDATION_IMPORT_DASHBOARD"] = "0"
    if not str(os.environ.get("TRADING_VALIDATION_IMPORT_HEAVY_ENTRYPOINTS", "") or "").strip():
        os.environ["TRADING_VALIDATION_IMPORT_HEAVY_ENTRYPOINTS"] = "0"
    if (
        _local_startup_validation_profile(os.environ)
        and not str(os.environ.get("TS_STORAGE_BACKEND", "") or "").strip()
    ):
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"

    current_pythonpath = str(os.environ.get("PYTHONPATH", "") or "")
    pythonpath_parts = [ROOT]
    if current_pythonpath:
        pythonpath_parts.append(current_pythonpath)
    os.environ["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(ROOT, ".env"), override=False)
    except ModuleNotFoundError as e:
        if not _is_missing_optional_module(e, "dotenv"):
            raise
    except Exception as e:
        sys.stderr.write(f"[runtime_graph_check.bootstrap_validation_env] {type(e).__name__}: {e}\n")
        sys.stderr.flush()

    if _local_startup_validation_profile(os.environ):
        _scrub_local_external_service_env(os.environ)

    # Startup graph validation runs supervised imports, which require a
    # production-shaped key. Use deterministic file-backed validation material
    # unless the caller explicitly opts into validating real production
    # dependencies.
    if not _production_validation_profile(dict(os.environ)):
        _configure_local_secret_source_policy(os.environ, Path(DATA_DIR) / "runtime_graph_validation")

    return dict(os.environ)


def _record_error(errors: List[Tuple[str, str, str]], name: str, error: Exception, tb: str = "") -> None:
    msg = f"{type(error).__name__}: {error}"
    errors.append((name, msg, tb))
    print("FAIL", name, "->", msg)
    if tb:
        print(tb)


def _record_message(errors: List[Tuple[str, str, str]], name: str, message: str, tb: str = "") -> None:
    errors.append((name, str(message), tb))
    print("FAIL", name, "->", message)
    if tb:
        print(tb)


def _embedded_startup_validation_error(preflight_result: object) -> Optional[str]:
    if not isinstance(preflight_result, dict):
        return None

    startup_validation = preflight_result.get("startup_validation")
    if not isinstance(startup_validation, dict):
        health = preflight_result.get("health")
        if isinstance(health, dict):
            startup_validation = health.get("startup_validation")
    if not isinstance(startup_validation, dict):
        return None
    if bool(startup_validation.get("ok", False)):
        return None

    blocking_checks = [
        str(name)
        for name in (startup_validation.get("blocking_gates") or startup_validation.get("blocking_checks") or [])
        if str(name).strip()
    ]
    reasons = [
        str(reason)
        for reason in (startup_validation.get("reasons") or [])
        if str(reason).strip()
    ]
    if blocking_checks:
        return f"startup_validation_failed:blocking_checks={','.join(blocking_checks)}"
    if reasons:
        return f"startup_validation_failed:reasons={'; '.join(reasons[:4])}"
    return "startup_validation_failed"


def _check_import(name: str, errors: List[Tuple[str, str, str]]) -> None:
    try:
        importlib.import_module(name)
        print("OK  ", name)
    except Exception as e:
        _record_error(errors, name, e, traceback.format_exc())


def _check_python_file(label: str, file_path: str, errors: List[Tuple[str, str, str]]) -> None:
    try:
        py_compile.compile(file_path, doraise=True)
        print("OK  ", label)
    except Exception as e:
        _record_error(errors, label, e, traceback.format_exc())


def _module_name_from_script(script_rel: str) -> str:
    return ".".join(Path(script_rel).with_suffix("").parts)


def _collect_entrypoint_imports() -> List[str]:
    modules = [
        "engine.api.internal_access",
        "engine.api.api_jobs",
        "engine.api.api_system",
        "engine.runtime.job_registry",
        "engine.runtime.supervisor",
        "engine.runtime.jobs_manager",
        "engine.runtime.orchestrator",
        "engine.runtime.runtime_bootstrap",
        "engine.runtime.health",
        "engine.runtime.lifecycle",
        "engine.runtime.lifecycle_state",
        "engine.runtime.startup_orchestrator",
    ]
    if str(os.environ.get("TRADING_VALIDATION_IMPORT_HEAVY_ENTRYPOINTS", "") or "").strip().lower() in {"1", "true", "yes", "on"}:
        modules[:0] = [
            "start_ingestion",
            "start_system",
            "engine.app",
        ]
    if str(os.environ.get("TRADING_VALIDATION_IMPORT_DASHBOARD", "") or "").strip().lower() in {"1", "true", "yes", "on"}:
        modules.insert(0, "dashboard_server")
    return modules


def _validate_supervisor_graph() -> Dict[str, object]:
    from engine.runtime.supervisor import RuntimeSupervisor

    supervisor = RuntimeSupervisor()
    if not hasattr(supervisor, "validate_graph"):
        return {"ok": False, "errors": ["not_implemented"], "not_implemented": True}
    result = supervisor.validate_graph(strict=True)
    return result if isinstance(result, dict) else {"ok": False, "errors": [str(result)]}


def _run_cold_boot_db_bootstrap_check(*, timeout_s: float = 180.0) -> Dict[str, object]:
    timeout_value = max(5.0, float(timeout_s or 60.0))
    with tempfile.TemporaryDirectory(prefix="runtime-graph-check-boot-") as tmpdir:
        tmp_root = Path(tmpdir)
        data_dir = tmp_root / "data"
        log_dir = tmp_root / "logs"
        data_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        db_path = data_dir / "cold_boot_validation.db"

        env = dict(os.environ)
        env.update(
            {
                "DB_PATH": str(db_path),
                "TRADING_DATA": str(data_dir),
                "DATA_DIR": str(data_dir),
                "TRADING_LOGS": str(log_dir),
                "LOG_DIR": str(log_dir),
                "ENGINE_SUPERVISED": "1",
                "AUTO_BOOT_DAEMONS": "0",
                "SQLITE_TRACE_REPORT_EVERY_S": "0",
                "TRADING_VALIDATION_MODE": "startup",
                "TS_PG_SCHEMA_PER_DB_PATH": "1",
            }
        )
        if not _production_validation_profile(env):
            _scrub_local_external_service_env(env)
            _configure_local_secret_source_policy(env, tmp_root / "validation_secret_policy")

        check_code = (
            "import sys\n"
            f"sys.path.insert(0, {ROOT!r})\n"
            "from engine.runtime.db_repair import repair\n"
            "result = repair(startup_fast_path=True)\n"
            "print(result)\n"
            "raise SystemExit(0 if isinstance(result, dict) and result.get('ok') else 1)\n"
        )

        try:
            proc = subprocess.run(
                [sys.executable, "-c", check_code],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout_value,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False,
                "error": "timeout",
                "timeout_s": float(timeout_value),
                "stdout": str((exc.stdout or "")).strip(),
                "stderr": str((exc.stderr or "")).strip(),
            }

        return {
            "ok": int(proc.returncode or 0) == 0,
            "exit_code": int(proc.returncode or 0),
            "db_path": str(db_path),
            "stdout": str(proc.stdout or "").strip(),
            "stderr": str(proc.stderr or "").strip(),
        }


def _run_timeseries_sidecar_startup_check(*, timeout_s: float = 180.0) -> Dict[str, object]:
    timeout_value = max(5.0, float(timeout_s or 60.0))
    with tempfile.TemporaryDirectory(prefix="runtime-graph-check-timeseries-") as tmpdir:
        tmp_root = Path(tmpdir)
        data_dir = tmp_root / "data"
        log_dir = tmp_root / "logs"
        data_dir.mkdir(parents=True, exist_ok=True)
        log_dir.mkdir(parents=True, exist_ok=True)
        db_path = data_dir / "timeseries_validation.db"

        env = dict(os.environ)
        env.update(
            {
                "DB_PATH": str(db_path),
                "TRADING_DATA": str(data_dir),
                "DATA_DIR": str(data_dir),
                "TRADING_LOGS": str(log_dir),
                "LOG_DIR": str(log_dir),
                "ENGINE_SUPERVISED": "1",
                "AUTO_BOOT_DAEMONS": "0",
                "SQLITE_TRACE_REPORT_EVERY_S": "0",
                "TRADING_VALIDATION_MODE": "startup",
                "TS_PG_SCHEMA_PER_DB_PATH": "1",
            }
        )
        if not _production_validation_profile(env):
            _scrub_local_external_service_env(env)
            _configure_local_secret_source_policy(env, tmp_root / "validation_secret_policy")

        check_code = (
            "import json\n"
            "import os\n"
            "import sys\n"
            f"sys.path.insert(0, {ROOT!r})\n"
            "def _schema_timeout_s():\n"
            "    for name in ('RUNTIME_GRAPH_TIMESERIES_SCHEMA_TIMEOUT_S', 'TIMESCALE_COMMAND_TIMEOUT_S'):\n"
            "        raw = str(os.environ.get(name, '') or '').strip()\n"
            "        if not raw:\n"
            "            continue\n"
            "        try:\n"
            "            return max(1.0, float(raw))\n"
            "        except ValueError:\n"
            "            continue\n"
            "    return 30.0\n"
            "from engine.runtime import storage\n"
            "storage.init_db()\n"
            "snapshot = storage.init_timeseries_storage()\n"
            "if bool(snapshot.get('enabled')) and not bool(snapshot.get('ok', True)):\n"
            "    client = storage.get_timescale_client()\n"
            "    if client is not None and bool(getattr(client, 'enabled', False)):\n"
            "        try:\n"
            "            client.ensure_schema(timeout_s=_schema_timeout_s())\n"
            "            snapshot = storage.get_timeseries_storage_snapshot()\n"
            "        except Exception as exc:\n"
            "            snapshot = storage.get_timeseries_storage_snapshot()\n"
            "            snapshot['startup_check_error'] = f'{type(exc).__name__}: {exc}'\n"
            "enabled = bool(snapshot.get('enabled'))\n"
            "ok = bool(snapshot.get('ok', True))\n"
            "print(json.dumps(snapshot, sort_keys=True, default=str))\n"
            "storage.shutdown_timeseries_storage(timeout_s=0.5)\n"
            "raise SystemExit(0 if ((not enabled) or ok) else 1)\n"
        )

        try:
            proc = subprocess.run(
                [sys.executable, "-c", check_code],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                timeout=timeout_value,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False,
                "error": "timeout",
                "timeout_s": float(timeout_value),
                "stdout": str((exc.stdout or "")).strip(),
                "stderr": str((exc.stderr or "")).strip(),
            }

        return {
            "ok": int(proc.returncode or 0) == 0,
            "exit_code": int(proc.returncode or 0),
            "db_path": str(db_path),
            "stdout": str(proc.stdout or "").strip(),
            "stderr": str(proc.stderr or "").strip(),
        }


def _cleanup_validation_schema_for_result(label: str, result: Dict[str, object]) -> None:
    if not _production_validation_profile(dict(os.environ)):
        return
    db_path = str((result or {}).get("db_path") or "").strip()
    if not db_path:
        return
    if _env_truthy(os.environ.get("TRADING_KEEP_VALIDATION_PG_SCHEMAS")):
        return
    try:
        from tools.validation_pg_cleanup import cleanup_schema_for_db_path

        cleanup = cleanup_schema_for_db_path(db_path)
        dropped = list(cleanup.get("dropped") or [])
        if dropped:
            print(f"OK   {label}_schema_cleanup dropped={','.join(str(item) for item in dropped)}")
    except Exception as exc:
        print(f"WARN {label}_schema_cleanup -> {type(exc).__name__}: {exc}")


def _walk_engine_modules() -> List[str]:
    modules: List[str] = []
    if not os.path.isdir(ENGINE_DIR):
        return modules
    for mod in pkgutil.walk_packages([ENGINE_DIR], prefix="engine."):
        modules.append(str(mod.name))
    return modules


def _walk_python_files(root_dir: str) -> List[str]:
    files: List[str] = []
    if not os.path.isdir(root_dir):
        return files
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for filename in filenames:
            if not filename.endswith(".py"):
                continue
            files.append(os.path.join(dirpath, filename))
    files.sort()
    return files


def run_canonical_validation(mode: str = "full", extra_env: Optional[Dict[str, str]] = None) -> int:
    mode_name = str(mode or "full").strip().lower() or "full"
    bootstrap_env = dict(extra_env or {})
    bootstrap_env.setdefault("TRADING_VALIDATION_MODE", mode_name)
    bootstrap_validation_env(extra_env=bootstrap_env)

    if not os.path.isdir(ENGINE_DIR):
        print(f"FAIL engine_dir -> missing engine directory: {ENGINE_DIR}")
        return 1

    full_mode = mode_name == "full"

    print("\nCANONICAL VALIDATION\n")
    print("mode:", mode_name)
    print("root:", ROOT)
    print("db_path:", os.environ.get("DB_PATH"))
    print("logs:", os.environ.get("TRADING_LOGS"))
    print("data:", os.environ.get("TRADING_DATA"))

    errors: List[Tuple[str, str, str]] = []
    imported: Set[str] = set()

    print("\nENTRYPOINT FILES\n")
    entry_files = [
        ("dashboard_server.py", os.path.join(ROOT, "dashboard_server.py")),
        ("start_ingestion.py", os.path.join(ROOT, "start_ingestion.py")),
        ("start_system.py", os.path.join(ROOT, "start_system.py")),
    ]
    for label, file_path in entry_files:
        if not os.path.exists(file_path):
            _record_message(errors, label, f"missing_file:{file_path}")
            continue
        _check_python_file(label, file_path, errors)

    print("\nENTRYPOINT IMPORTS\n")
    for module_name in _collect_entrypoint_imports():
        if module_name in imported:
            continue
        imported.add(module_name)
        _check_import(module_name, errors)

    if full_mode:
        print("\nENGINE FILE GRAPH\n")
        for file_path in _walk_python_files(ENGINE_DIR):
            rel_path = os.path.relpath(file_path, ROOT)
            _check_python_file(rel_path, file_path, errors)
    else:
        print("\nENGINE FILE GRAPH\n")
        print("SKIP startup mode: full engine py_compile sweep disabled")

    print("\nJOB REGISTRY\n")
    try:
        job_registry = importlib.reload(importlib.import_module("engine.runtime.job_registry"))
        ALLOWED_JOBS = job_registry.ALLOWED_JOBS
        validate_job_registry_paths = job_registry.validate_job_registry_paths
        validate_runtime_architecture = job_registry.validate_runtime_architecture

        print("Registered jobs:", len(ALLOWED_JOBS))
        for name in sorted(ALLOWED_JOBS.keys()):
            print(" -", name)

        registry_import_check = bool(full_mode)
        registry_result = validate_job_registry_paths(
            repo_root=ROOT,
            import_check=registry_import_check,
        )
        print("validate_job_registry_paths:", registry_result)
        if not registry_result.get("ok"):
            _record_message(errors, "validate_job_registry_paths", str(registry_result))

        arch_result = validate_runtime_architecture(
            repo_root=ROOT,
            import_check=registry_import_check,
        )
        print("validate_runtime_architecture:", arch_result)
        if not arch_result.get("ok"):
            _record_message(errors, "validate_runtime_architecture", str(arch_result))

        if full_mode:
            print("\nJOB MODULE FILES\n")
            repo_root_abs = os.path.abspath(ROOT)

            for job_name, spec in sorted(ALLOWED_JOBS.items()):
                if not isinstance(spec, (tuple, list)) or len(spec) < 2:
                    _record_message(errors, f"job:{job_name}", "invalid_spec")
                    continue

                script_rel = str((spec[0] or "")).strip()
                if not script_rel:
                    _record_message(errors, f"job:{job_name}", "missing_script_path")
                    continue

                script_abs = os.path.normpath(os.path.abspath(os.path.join(ROOT, script_rel)))
                if not script_abs.startswith(repo_root_abs + os.path.sep) and script_abs != repo_root_abs:
                    _record_message(errors, f"job_file:{job_name}", f"script_outside_repo:{script_rel}")
                    continue
                if not os.path.exists(script_abs):
                    _record_message(errors, f"job_file:{job_name}", f"missing_file:{script_rel}")
                    continue
                if not os.path.isfile(script_abs):
                    _record_message(errors, f"job_file:{job_name}", f"invalid_file_type:{script_rel}")
                    continue

                _check_python_file(f"job_file:{job_name}", script_abs, errors)
        else:
            print("\nJOB MODULE FILES\n")
            print("SKIP startup mode: full job-file py_compile sweep disabled")
    except Exception as e:
        _record_error(errors, "job_registry", e, traceback.format_exc())

    if full_mode:
        print("\nSUPERVISOR GRAPH\n")
        try:
            result = _validate_supervisor_graph()
            print("validate_graph:", result)
            if not result.get("ok"):
                if bool(result.get("not_implemented")):
                    _record_message(errors, "validate_graph", "not_implemented")
                else:
                    _record_message(errors, "validate_graph", str(result))
            else:
                print("OK   validate_graph")
        except Exception as e:
            _record_error(errors, "supervisor_init", e, traceback.format_exc())
    else:
        print("\nSUPERVISOR GRAPH\n")
        print("SKIP startup mode: supervisor graph instantiation disabled")

    print("\nCOLD BOOT DB CHECK\n")
    cold_boot_db = _run_cold_boot_db_bootstrap_check()
    print("cold_boot_db:", cold_boot_db)
    if not bool(cold_boot_db.get("ok")):
        _record_message(errors, "cold_boot_db", str(cold_boot_db))
    else:
        print("OK   cold_boot_db")
    _cleanup_validation_schema_for_result("cold_boot_db", cold_boot_db)

    print("\nTIMESERIES SIDECAR CHECK\n")
    timeseries_sidecars = _run_timeseries_sidecar_startup_check()
    print("timeseries_sidecars:", timeseries_sidecars)
    if not bool(timeseries_sidecars.get("ok")):
        _record_message(errors, "timeseries_sidecars", str(timeseries_sidecars))
    else:
        print("OK   timeseries_sidecars")
    _cleanup_validation_schema_for_result("timeseries_sidecars", timeseries_sidecars)

    if full_mode:
        print("\nPREFLIGHT CHECK\n")
        try:
            from engine.runtime.health import run_preflight
            pf = run_preflight()
            print("preflight result:", pf)
            if isinstance(pf, dict) and not pf.get("ok", False):
                _record_message(errors, "preflight", str(pf))
            startup_validation_error = _embedded_startup_validation_error(pf)
            if startup_validation_error:
                _record_message(errors, "startup_validation", startup_validation_error)
        except Exception as e:
            _record_error(errors, "preflight", e, traceback.format_exc())

    print("\nSUMMARY\n")
    if not errors:
        print("SYSTEM GRAPH VALID")
        return 0

    print("ERRORS DETECTED:", len(errors))
    for name, msg, tb in errors:
        print(name, "->", msg)
        if tb:
            print(tb)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate runtime imports, graph, and startup dependencies.")
    parser.add_argument(
        "--mode",
        choices=("startup", "full"),
        default=None,
        help="Validation depth. Defaults to TRADING_VALIDATION_MODE or full.",
    )
    args = parser.parse_args()

    mode = str(
        args.mode
        or os.environ.get("TRADING_VALIDATION_MODE")
        or "full"
    ).strip().lower() or "full"
    return run_canonical_validation(mode=mode)


if __name__ == "__main__":
    raise SystemExit(main())
