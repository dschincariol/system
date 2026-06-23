from __future__ import annotations

import os
import sys
import threading
from typing import Any, Dict

from engine.runtime.hardware import DEFAULT_CPU_THREADS, DEFAULT_INTEROP_THREADS
from engine.runtime.thread_policy import apply_cpu_thread_policy_to_env

_CONFIG_LOCK = threading.Lock()
_CONFIG_STATE: Dict[str, Any] = {
    "attempted": False,
    "configured": False,
    "cpu_threads": None,
    "interop_threads": None,
    "error_type": "",
    "error_message": "",
}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(str(name))
    try:
        return int(str(default if raw is None or str(raw).strip() == "" else raw).strip())
    except Exception:
        return int(default)


def configure_torch_thread_pools(
    torch_module: Any,
    *,
    default_cpu_threads: int = DEFAULT_CPU_THREADS,
    default_interop_threads: int = DEFAULT_INTEROP_THREADS,
) -> Dict[str, Any]:
    policy = apply_cpu_thread_policy_to_env(os.environ)
    cpu_threads = max(1, _env_int("TORCH_CPU_THREADS", int(default_cpu_threads)))
    interop_threads = max(1, _env_int("TORCH_INTEROP_THREADS", int(default_interop_threads)))

    with _CONFIG_LOCK:
        if _CONFIG_STATE["attempted"]:
            return {
                "applied": False,
                "configured": bool(_CONFIG_STATE["configured"]),
                "reason": "already_attempted",
                "cpu_threads": _CONFIG_STATE["cpu_threads"],
                "interop_threads": _CONFIG_STATE["interop_threads"],
                "thread_policy": policy,
                "error_type": _CONFIG_STATE["error_type"],
                "error_message": _CONFIG_STATE["error_message"],
            }
        _CONFIG_STATE["attempted"] = True
        _CONFIG_STATE["cpu_threads"] = int(cpu_threads)
        _CONFIG_STATE["interop_threads"] = int(interop_threads)

    try:
        torch_module.set_num_threads(int(cpu_threads))
        torch_module.set_num_interop_threads(int(interop_threads))
    except Exception as exc:
        with _CONFIG_LOCK:
            _CONFIG_STATE["configured"] = False
            _CONFIG_STATE["error_type"] = type(exc).__name__
            _CONFIG_STATE["error_message"] = str(exc)
        sys.stderr.write(f"[torch_threads] configure_failed: {type(exc).__name__}: {exc}\n")
        sys.stderr.flush()
        return {
            "applied": False,
            "configured": False,
            "reason": "failed",
            "cpu_threads": int(cpu_threads),
            "interop_threads": int(interop_threads),
            "thread_policy": policy,
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "error": exc,
        }

    with _CONFIG_LOCK:
        _CONFIG_STATE["configured"] = True
        _CONFIG_STATE["error_type"] = ""
        _CONFIG_STATE["error_message"] = ""

    return {
        "applied": True,
        "configured": True,
        "reason": "configured",
        "cpu_threads": int(cpu_threads),
        "interop_threads": int(interop_threads),
        "thread_policy": policy,
        "error_type": "",
        "error_message": "",
    }
