"""
FILE: runtime_graph_check.py

Tooling or validation script for `runtime_graph_check`.
"""

import argparse
import importlib
import os
import pkgutil
import py_compile
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENGINE_DIR = os.path.join(ROOT, "engine")
RUNTIME_DIR = os.path.join(ENGINE_DIR, "runtime")
LOG_DIR = os.path.join(ROOT, "logs")
DATA_DIR = os.path.join(ROOT, "data")

if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


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

    current_pythonpath = str(os.environ.get("PYTHONPATH", "") or "")
    pythonpath_parts = [ROOT]
    if current_pythonpath:
        pythonpath_parts.append(current_pythonpath)
    os.environ["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(ROOT, ".env"), override=False)
    except Exception as e:
        sys.stderr.write(f"[runtime_graph_check.bootstrap_validation_env] {type(e).__name__}: {e}\n")
        sys.stderr.flush()

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
    bootstrap_validation_env(extra_env=extra_env)

    if not os.path.isdir(ENGINE_DIR):
        print(f"FAIL engine_dir -> missing engine directory: {ENGINE_DIR}")
        return 1

    mode_name = str(mode or "full").strip().lower()
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
        from engine.runtime.job_registry import ALLOWED_JOBS, validate_job_registry_paths, validate_runtime_architecture

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

    print("\nTIMESERIES SIDECAR CHECK\n")
    timeseries_sidecars = _run_timeseries_sidecar_startup_check()
    print("timeseries_sidecars:", timeseries_sidecars)
    if not bool(timeseries_sidecars.get("ok")):
        _record_message(errors, "timeseries_sidecars", str(timeseries_sidecars))
    else:
        print("OK   timeseries_sidecars")

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
