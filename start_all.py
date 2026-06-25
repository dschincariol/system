"""
FILE: start_all.py

Top-level entrypoint or configuration module for `start_all`.
"""

from __future__ import annotations

import os
import shutil
import signal
import logging
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.log_retention import rotate_log_if_needed
from engine.runtime.logging import get_logger
from engine.runtime.platform import (
    default_local_db_dir,
    default_local_db_path,
    default_local_log_dir,
    default_local_tmp_dir,
    resolve_runtime_paths,
)
from engine.startup.env import strict_runtime_requires_explicit_db_path


ROOT = Path(__file__).resolve().parent


def _launcher_env_file_path() -> Path:
    raw = str(os.environ.get("TRADING_ENV_FILE") or ".env").strip() or ".env"
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path


def _bootstrap_launcher_env() -> None:
    env_path = _launcher_env_file_path()
    if env_path.exists():
        try:
            from dotenv import load_dotenv

            load_dotenv(env_path, override=False)
        except Exception as e:
            print(f"[startup] dotenv_load_failed: {type(e).__name__}: {e}", flush=True)

    if str(os.environ.get("TRADING_ENV_FILE") or "").strip() and not str(os.environ.get("OPERATOR_ENV_PATH") or "").strip():
        try:
            operator_env_path = default_local_tmp_dir() / "start_all.env"
            operator_env_path.parent.mkdir(parents=True, exist_ok=True)
            operator_env_path.write_text(env_path.read_text(encoding="utf-8"), encoding="utf-8")
            os.environ["OPERATOR_ENV_PATH"] = str(operator_env_path)
        except Exception as e:
            print(f"[startup] operator_env_copy_failed: {type(e).__name__}: {e}", flush=True)

    os.environ.setdefault("TRADING_LOGS", str(default_local_log_dir().resolve()))
    os.environ.setdefault("TRADING_DATA", str(default_local_db_dir().resolve()))
    if not strict_runtime_requires_explicit_db_path(os.environ):
        os.environ.setdefault("DB_PATH", str(default_local_db_path().resolve()))
    try:
        resolve_runtime_paths(os.environ, project_root=ROOT)
    except Exception as e:
        print(f"[startup] runtime_path_resolve_failed: {type(e).__name__}: {e}", flush=True)


_bootstrap_launcher_env()

OPERATOR_URL = os.environ.get("OPERATOR_URL", "http://127.0.0.1:4001/")
NODE_ENTRY = ROOT / "boot" / "operator_server.js"
EXPRESS_PKG = ROOT / "node_modules" / "express" / "package.json"
OPERATOR_STDOUT_LOG = default_local_log_dir() / "operator.stdout.log"
OPERATOR_STDERR_LOG = default_local_log_dir() / "operator.stderr.log"
LOG = get_logger("engine.start_all")


def _pick_python() -> str:
    override = str(os.environ.get("OPERATOR_PYTHON", "")).strip()
    candidates = [override] if override else []
    candidates.extend(["python3", "python"])
    seen = set()
    for cmd in candidates:
        if not cmd or cmd in seen:
            continue
        seen.add(cmd)
        exe = shutil.which(cmd)
        if exe:
            return exe
    return sys.executable


def _pick_required(name: str) -> str:
    exe = shutil.which(name)
    if exe:
        return exe
    raise RuntimeError(f"missing_required_executable:{name}")


def _ensure_dirs() -> None:
    default_local_db_dir().mkdir(parents=True, exist_ok=True)
    default_local_log_dir().mkdir(parents=True, exist_ok=True)
    (default_local_tmp_dir() / "operator").mkdir(parents=True, exist_ok=True)


def _ensure_node_modules(npm_exe: str) -> None:
    if EXPRESS_PKG.exists():
        return
    print("[startup] node_modules missing; running npm install...", flush=True)
    try:
        r = subprocess.run(
            [npm_exe, "install"],
            cwd=str(ROOT),
            timeout=900,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError("npm_install_timeout") from e
    if r.returncode != 0:
        raise RuntimeError(f"npm_install_failed:{r.returncode}")


def _open_browser() -> None:
    try:
        webbrowser.open(OPERATOR_URL)
    except Exception as e:
        print(f"[startup] browser_open_failed: {e}", flush=True)


def _warn_nonfatal(event: str, code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="start_all",
        extra=extra or None,
        persist=False,
    )


def _spawn_operator(node_exe: str) -> subprocess.Popen:
    env = os.environ.copy()
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("OPERATOR_PYTHON", _pick_python())
    # `start_all.py` is the convenience dev launcher: it starts the operator and
    # lets the Node control plane manage engine startup from there.
    env.setdefault("OPERATOR_AUTO_START", "1")
    env.setdefault("ENGINE_MODE", "safe")

    OPERATOR_STDOUT_LOG.parent.mkdir(parents=True, exist_ok=True)

    rotate_log_if_needed(OPERATOR_STDOUT_LOG)
    rotate_log_if_needed(OPERATOR_STDERR_LOG)
    stdout_fh = open(OPERATOR_STDOUT_LOG, "ab")
    stderr_fh = open(OPERATOR_STDERR_LOG, "ab")

    try:
        return subprocess.Popen(
            [node_exe, str(NODE_ENTRY)],
            cwd=str(ROOT),
            env=env,
            stdout=stdout_fh,
            stderr=stderr_fh,
        )
    except Exception:
        stdout_fh.close()
        stderr_fh.close()
        raise


def _terminate_operator(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    try:
        proc.send_signal(signal.SIGTERM)
    except Exception as e:
        print(f"[startup] operator_terminate_failed: {e}", flush=True)

    deadline = time.time() + 5.0
    while time.time() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.1)

    try:
        proc.kill()
    except Exception as e:
        print(f"[startup] operator_terminate_failed: {e}", flush=True)


def main() -> int:
    os.chdir(ROOT)
    _ensure_dirs()

    node_exe = _pick_required("node")
    npm_exe = _pick_required("npm")

    if not NODE_ENTRY.exists():
        raise RuntimeError(f"missing_operator_server:{NODE_ENTRY}")

    _ensure_node_modules(npm_exe)

    # ------------------------------------------------------------------
    # Ensure operator port 4001 is free (prevents EADDRINUSE crash)
    # ------------------------------------------------------------------
    try:
        import socket
        import subprocess

        def _port_in_use(port: int) -> bool:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                return s.connect_ex(("127.0.0.1", port)) == 0
            finally:
                s.close()

        if _port_in_use(4001):
            print("[startup] port 4001 already in use, attempting cleanup...", flush=True)

            subprocess.run(
                ["pkill", "-f", "operator_server.js"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
                check=False,
            )

            time.sleep(1)

    except Exception as e:
        print(f"[startup] operator_terminate_failed: {e}", flush=True)

    proc = _spawn_operator(node_exe)

    time.sleep(2.0)
    early_exit = proc.poll()
    if early_exit is not None:
        raise RuntimeError(
            f"operator_exited_early:{early_exit}:stderr_log={OPERATOR_STDERR_LOG}:stdout_log={OPERATOR_STDOUT_LOG}"
        )

    # Browser open is best-effort convenience only; launcher success is defined
    # by the operator process, not by whether the local desktop opens a tab.
    _open_browser()

    def _shutdown(_sig=None, _frame=None):
        print("\n[startup] shutting down operator...", flush=True)
        _terminate_operator(proc)

    try:
        signal.signal(signal.SIGINT, _shutdown)
    except Exception as e:
        _warn_nonfatal("start_all_sigint_handler_register_failed", "START_ALL_SIGINT_HANDLER_REGISTER_FAILED", e)
    try:
        signal.signal(signal.SIGTERM, _shutdown)
    except Exception as e:
        _warn_nonfatal("start_all_sigterm_handler_register_failed", "START_ALL_SIGTERM_HANDLER_REGISTER_FAILED", e)

    try:
        return proc.wait()
    except KeyboardInterrupt:
        print("\n[startup] shutting down operator...", flush=True)
        _terminate_operator(proc)
        return 0
    finally:
        _terminate_operator(proc)


if __name__ == "__main__":
    raise SystemExit(main())
