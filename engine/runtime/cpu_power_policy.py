from __future__ import annotations

"""Read-only CPU power-policy verification helpers."""

import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCRIPT = REPO_ROOT / "ops" / "server" / "cpu_power_policy.sh"
DEFAULT_TIMEOUT_S = 3.0
_TRUE_VALUES = {"1", "true", "yes", "on"}
_LIVE_VALUES = {"live"}


def _truthy(raw: Any) -> bool:
    return str(raw or "").strip().lower() in _TRUE_VALUES


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    try:
        return max(0.1, float(str(env.get(name, "") or "").strip() or default))
    except (TypeError, ValueError):
        return float(default)


def cpu_power_policy_required(env: Mapping[str, str] | None = None) -> bool:
    env_map = dict(env or os.environ)
    if _truthy(env_map.get("PREFLIGHT_REQUIRE_CPU_POWER_POLICY")):
        return True
    if _truthy(env_map.get("TRADING_CPU_POWER_POLICY_REQUIRED")):
        return True
    for key in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE", "MODE"):
        if str(env_map.get(key, "") or "").strip().lower() in _LIVE_VALUES:
            return True
    return False


def _policy_script_path(env: Mapping[str, str]) -> Path:
    raw = str(env.get("TRADING_CPU_POWER_POLICY_SCRIPT") or "").strip()
    return Path(raw).expanduser() if raw else DEFAULT_SCRIPT


def _parse_key_values(stdout: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_line in str(stdout or "").splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[str(key).strip()] = str(value).strip()
    return parsed


def _is_unavailable(parsed: Mapping[str, str]) -> bool:
    return (
        str(parsed.get("power_profile") or "") == "unavailable"
        and str(parsed.get("scaling_governor") or "") == "missing"
        and str(parsed.get("energy_performance_preference") or "") == "missing"
    )


def _failure_reason(parsed: Mapping[str, str], returncode: int) -> str:
    intended = str(parsed.get("intended_state") or "").strip()
    if _is_unavailable(parsed):
        return "cpu_power_policy_unavailable"
    if intended.startswith("FAIL"):
        return "cpu_power_policy_drift"
    if returncode != 0:
        return "cpu_power_policy_verify_failed"
    return ""


def _summary(parsed: Mapping[str, str]) -> str:
    parts = []
    for key in (
        "power_profile",
        "power_profile_degraded",
        "amd_pstate_status",
        "scaling_governor",
        "energy_performance_preference",
        "intended_state",
    ):
        value = str(parsed.get(key) or "").strip()
        if value:
            parts.append(f"{key}={value}")
    return " ".join(parts)


def verify_cpu_power_policy(
    *,
    env: Mapping[str, str] | None = None,
    timeout_s: float | None = None,
    script_path: Path | str | None = None,
) -> dict[str, Any]:
    """Run ``cpu_power_policy.sh verify`` and return a structured snapshot.

    This function never applies or repairs policy; it only invokes the read-only
    verifier and classifies its output for preflight and observability callers.
    """

    env_map = dict(env or os.environ)
    required = cpu_power_policy_required(env_map)
    timeout = float(
        timeout_s
        if timeout_s is not None
        else _env_float(env_map, "PREFLIGHT_CPU_POWER_POLICY_TIMEOUT_S", DEFAULT_TIMEOUT_S)
    )
    script = Path(script_path).expanduser() if script_path is not None else _policy_script_path(env_map)
    started = time.perf_counter()
    snapshot: dict[str, Any] = {
        "required": bool(required),
        "ok": False,
        "status": "error",
        "reason": "",
        "script": str(script),
        "timeout_s": float(timeout),
        "returncode": None,
        "duration_ms": 0,
        "parsed": {},
        "summary": "",
        "stdout": "",
        "stderr": "",
    }

    if not script.exists():
        snapshot.update({"status": "missing", "reason": "cpu_power_policy_script_missing"})
        return snapshot
    if not script.is_file():
        snapshot.update({"status": "missing", "reason": "cpu_power_policy_script_not_regular_file"})
        return snapshot
    if not os.access(script, os.X_OK):
        snapshot.update({"status": "missing", "reason": "cpu_power_policy_script_not_executable"})
        return snapshot

    bash = shutil.which("bash") or "/usr/bin/bash"
    child_env = dict(env_map)
    child_env.setdefault("LC_ALL", "C")
    try:
        proc = subprocess.run(
            [bash, str(script), "verify"],
            cwd=str(REPO_ROOT),
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
            text=True,
        )
    except subprocess.TimeoutExpired as exc:
        snapshot.update(
            {
                "status": "timeout",
                "reason": "cpu_power_policy_verify_timeout",
                "stdout": str(exc.stdout or "")[-4000:],
                "stderr": str(exc.stderr or "")[-4000:],
            }
        )
        return snapshot
    except OSError as exc:
        snapshot.update(
            {
                "status": "error",
                "reason": f"cpu_power_policy_verify_exec_failed:{type(exc).__name__}:{exc}",
            }
        )
        return snapshot
    finally:
        snapshot["duration_ms"] = int((time.perf_counter() - started) * 1000.0)

    stdout = str(proc.stdout or "")
    stderr = str(proc.stderr or "")
    parsed = _parse_key_values(stdout)
    ok = int(proc.returncode) == 0 and str(parsed.get("intended_state") or "").strip() == "PASS"
    reason = "" if ok else _failure_reason(parsed, int(proc.returncode))
    if ok:
        status = "ok"
    elif reason == "cpu_power_policy_unavailable":
        status = "unavailable"
    elif reason == "cpu_power_policy_drift":
        status = "drift"
    else:
        status = "error"
    snapshot.update(
        {
            "ok": bool(ok),
            "status": status,
            "reason": reason,
            "returncode": int(proc.returncode),
            "parsed": parsed,
            "summary": _summary(parsed),
            "stdout": stdout[-4000:],
            "stderr": stderr[-4000:],
        }
    )
    return snapshot
