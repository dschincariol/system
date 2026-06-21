"""Subprocess helpers for startup validation and import smoke checks."""

import os
import subprocess
import traceback
from collections.abc import Mapping
from typing import Any, Callable, Dict, Optional

IMPORT_SMOKE_CODE = (
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


def module_name_from_path(path_value: str) -> str:
    """Convert a repository-relative Python file path into a module name."""
    rel = str(path_value or "").replace("\\", "/").strip()
    if not rel:
        return ""
    if rel.endswith(".py"):
        rel = rel[:-3]
    return rel.replace("/", ".").strip(".")


def import_smoke_subprocess(
    module_name: str,
    abs_path: str,
    *,
    timeout_s: float,
    base_dir: str,
    executable: str,
    environ: Mapping[str, str],
    run: Callable[..., Any] = subprocess.run,
    log_swallowed: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    """Run one import-smoke target in a child Python process."""
    env = dict(environ)
    env["PYTHONPATH"] = str(base_dir) + os.pathsep + str(env.get("PYTHONPATH", ""))
    env["TRADING_IMPORT_SMOKE_CHILD"] = "1"
    try:
        proc = run(
            [executable, "-c", IMPORT_SMOKE_CODE, str(module_name or ""), str(abs_path)],
            cwd=base_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_s)),
        )
    except subprocess.TimeoutExpired as e:
        if log_swallowed is not None:
            log_swallowed(
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
        if log_swallowed is not None:
            log_swallowed(
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


def run_runtime_graph_validation(
    script_path: str,
    *,
    base_dir: str,
    executable: str,
    environ: Mapping[str, str],
    timeout_s: float,
    run: Callable[..., Any] = subprocess.run,
) -> Optional[Dict[str, Any]]:
    """Run the startup runtime-graph validator and return a failure row, if any."""
    check_name = "runtime_graph_check"
    env = dict(environ)
    env.setdefault("PYTHONPATH", base_dir)
    env["TRADING_VALIDATION_MODE"] = "startup"

    try:
        proc = run(
            [executable, script_path],
            cwd=base_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )

        stdout_text = str(proc.stdout or "").strip()
        stderr_text = str(proc.stderr or "").strip()

        if int(proc.returncode or 0) != 0:
            return {
                "name": str(check_name),
                "script": str(script_path),
                "error": f"validation_failed:{check_name}",
                "exit_code": int(proc.returncode),
                "stdout": stdout_text[-12000:],
                "stderr": stderr_text[-12000:],
            }
    except subprocess.TimeoutExpired as e:
        return {
            "name": str(check_name),
            "script": str(script_path),
            "error": f"validation_timeout:{check_name}",
            "exit_code": None,
            "stdout": str((e.stdout or "")).strip()[-12000:],
            "stderr": str((e.stderr or "")).strip()[-12000:],
        }
    return None
