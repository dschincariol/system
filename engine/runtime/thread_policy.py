"""Concurrency-aware CPU thread-pool policy for supervised runtime processes."""

from __future__ import annotations

import math
import os
from typing import Any, MutableMapping

BLAS_THREAD_ENV_KEYS = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
)
EXTRA_THREAD_ENV_KEYS = (
    "NUMEXPR_MAX_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)
TORCH_THREAD_ENV_KEYS = (
    "TORCH_CPU_THREADS",
    "TORCH_INTEROP_THREADS",
)
ALL_THREAD_ENV_KEYS = BLAS_THREAD_ENV_KEYS + EXTRA_THREAD_ENV_KEYS + TORCH_THREAD_ENV_KEYS

DEFAULT_CPU_CAPACITY = 8
DEFAULT_INGESTION_CHILD_COUNT = 12

_AUTO_POLICY_VALUES = {"", "auto", "managed", "on", "1", "true", "yes"}
_MANUAL_POLICY_VALUES = {"manual", "preserve", "operator", "disabled", "off", "0", "false", "no"}

_ROLE_MAX_THREADS = {
    "runtime": 4,
    "ingestion": 2,
    "ingestion_child": 1,
    "inference": 2,
    "training": 4,
    "offline": 8,
    "execution": 2,
    "replay": 2,
    "background": 2,
    "jobs": 2,
}
_ROLE_INTEROP_MAX_THREADS = {
    "runtime": 2,
    "ingestion": 1,
    "ingestion_child": 1,
    "inference": 1,
    "training": 2,
    "offline": 4,
    "execution": 1,
    "replay": 1,
    "background": 1,
    "jobs": 1,
}
_INFERENCE_JOB_MARKERS = (
    "inference",
    "process_events",
    "finbert",
    "embed_",
    "predict",
)
_TRAINING_JOB_MARKERS = (
    "train",
    "tune",
    "fit",
    "tsfresh",
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        text = _clean(value)
        if not text:
            return int(default)
        parsed = int(float(text))
        return int(parsed)
    except Exception:
        return int(default)


def _parse_cpu(value: Any) -> int:
    try:
        text = _clean(value).lower()
        if text in {"", "0", "none", "unbounded", "unlimited"}:
            return 0
        parsed = float(text)
        if not math.isfinite(parsed) or parsed <= 0:
            return 0
        return max(1, int(math.floor(parsed)))
    except Exception:
        return 0


def _env_first(env: MutableMapping[str, str] | dict[str, str], *names: str) -> str:
    for name in names:
        raw = _clean(env.get(str(name)))
        if raw:
            return raw
    return ""


def normalize_process_role(role: Any = None, env: MutableMapping[str, str] | dict[str, str] | None = None) -> str:
    env_map = os.environ if env is None else env
    raw = (
        _clean(role)
        or _clean(env_map.get("ENGINE_PROCESS_ROLE"))
        or _clean(env_map.get("TS_PROCESS_ROLE"))
        or _clean(env_map.get("ENGINE_CPU_THREAD_POLICY_ROLE"))
    )
    job_name = _clean(env_map.get("ENGINE_JOB_NAME") or env_map.get("JOB_NAME")).lower()
    value = raw.lower().replace("-", "_")
    if value in {"ingestion_child", "child"} or _truthy(env_map.get("ENGINE_INGESTION_CHILD")):
        return "ingestion_child"
    if value in _ROLE_MAX_THREADS:
        return value
    if job_name == "ingestion_runtime":
        return "ingestion"
    if any(marker in job_name for marker in _INFERENCE_JOB_MARKERS):
        return "inference"
    if any(marker in job_name for marker in _TRAINING_JOB_MARKERS):
        return "training"
    if value:
        return value
    return "runtime"


def cpu_capacity(env: MutableMapping[str, str] | dict[str, str] | None = None) -> int:
    env_map = os.environ if env is None else env
    raw = _env_first(
        env_map,
        "TRADING_CPU_THREAD_TOTAL",
        "RUNTIME_CPUS",
        "TRADING_RESOURCE_HOST_CPUS",
        "RESOURCE_HOST_CPUS",
    )
    parsed = _parse_cpu(raw)
    if parsed > 0:
        return int(parsed)
    try:
        return max(1, int(os.cpu_count() or DEFAULT_CPU_CAPACITY))
    except Exception:
        return int(DEFAULT_CPU_CAPACITY)


def _explicit_process_count(env: MutableMapping[str, str] | dict[str, str]) -> int:
    return _parse_int(
        _env_first(
            env,
            "ENGINE_SUPERVISED_PROCESS_COUNT",
            "TRADING_CPU_THREAD_PROCESS_COUNT",
            "TRADING_SUPERVISED_PROCESS_COUNT",
            "SUPERVISED_PROCESS_COUNT",
        ),
        0,
    )


def _ingestion_child_count(env: MutableMapping[str, str] | dict[str, str]) -> int:
    raw_jobs = _clean(env.get("INGESTION_CHILD_JOBS"))
    if raw_jobs:
        jobs = [part.strip() for part in raw_jobs.split(",") if part.strip()]
        if jobs:
            return max(1, len(jobs))
    return max(
        1,
        _parse_int(env.get("TRADING_DEFAULT_INGESTION_CHILD_COUNT"), DEFAULT_INGESTION_CHILD_COUNT),
    )


def infer_supervised_process_count(
    env: MutableMapping[str, str] | dict[str, str] | None = None,
    *,
    role: Any = None,
) -> int:
    env_map = os.environ if env is None else env
    explicit = _explicit_process_count(env_map)
    if explicit > 0:
        return int(explicit)

    normalized_role = normalize_process_role(role, env_map)
    child_count = _ingestion_child_count(env_map)
    launched_by_supervisor = _truthy(env_map.get("ENGINE_SUPERVISED")) or _truthy(
        env_map.get("ENGINE_LAUNCHED_BY_SUPERVISOR")
    )
    start_ingestion = _clean(env_map.get("START_INGESTION_WITH_SERVER") or "1").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }

    if normalized_role == "runtime":
        return max(1, 1 + (1 + child_count if start_ingestion else 0))
    if normalized_role in {"ingestion", "ingestion_child"}:
        return max(1, 1 + child_count + (1 if launched_by_supervisor and start_ingestion else 0))
    if launched_by_supervisor:
        return max(1, _parse_int(env_map.get("RESOURCE_SCHEDULER_GLOBAL_MAX"), 1))
    return 1


def _policy_mode(env: MutableMapping[str, str] | dict[str, str]) -> str:
    raw = _clean(env.get("TRADING_CPU_THREAD_POLICY") or "auto").lower()
    if raw in _MANUAL_POLICY_VALUES:
        return "manual"
    if raw in _AUTO_POLICY_VALUES:
        return "auto"
    return "auto"


def _role_override_threads(env: MutableMapping[str, str] | dict[str, str], role: str) -> int:
    role_key = role.upper().replace("-", "_")
    return _parse_int(
        _env_first(
            env,
            f"TRADING_{role_key}_CPU_THREADS_PER_PROCESS",
            "TRADING_CPU_THREADS_PER_PROCESS",
            "TRADING_CPU_THREAD_CAP",
            "CPU_THREADS_PER_PROCESS",
        ),
        0,
    )


def _interop_override_threads(env: MutableMapping[str, str] | dict[str, str], role: str) -> int:
    role_key = role.upper().replace("-", "_")
    return _parse_int(
        _env_first(
            env,
            f"TRADING_{role_key}_TORCH_INTEROP_THREADS_PER_PROCESS",
            "TRADING_TORCH_INTEROP_THREADS_PER_PROCESS",
        ),
        0,
    )


def resolve_cpu_thread_policy(
    env: MutableMapping[str, str] | dict[str, str] | None = None,
    *,
    role: Any = None,
    supervised_process_count: int | None = None,
) -> dict[str, Any]:
    env_map = os.environ if env is None else env
    normalized_role = normalize_process_role(role, env_map)
    mode = _policy_mode(env_map)
    capacity = cpu_capacity(env_map)
    process_count = max(
        1,
        int(supervised_process_count)
        if supervised_process_count is not None and int(supervised_process_count) > 0
        else infer_supervised_process_count(env_map, role=normalized_role),
    )
    per_process_budget = max(1, int(math.floor(float(capacity) / float(process_count))))
    role_max = max(1, int(_ROLE_MAX_THREADS.get(normalized_role, _ROLE_MAX_THREADS["background"])))
    interop_max = max(1, int(_ROLE_INTEROP_MAX_THREADS.get(normalized_role, 1)))
    override_threads = _role_override_threads(env_map, normalized_role)
    override_interop = _interop_override_threads(env_map, normalized_role)
    operator_override = override_threads > 0 or override_interop > 0 or mode == "manual"

    if override_threads > 0:
        cpu_threads = int(override_threads)
        source = "operator_override"
    else:
        cpu_threads = max(1, min(int(role_max), int(per_process_budget)))
        source = "role_concurrency_default"

    if override_interop > 0:
        interop_threads = int(override_interop)
    else:
        interop_threads = max(1, min(int(interop_max), int(cpu_threads)))

    env_values: dict[str, str] = {}
    for key in BLAS_THREAD_ENV_KEYS + EXTRA_THREAD_ENV_KEYS:
        env_values[key] = str(int(cpu_threads))
    env_values["TORCH_CPU_THREADS"] = str(int(cpu_threads))
    env_values["TORCH_INTEROP_THREADS"] = str(int(interop_threads))

    current = {key: _parse_int(env_map.get(key), 0) or None for key in ALL_THREAD_ENV_KEYS}
    return {
        "ok": True,
        "mode": mode,
        "role": normalized_role,
        "source": source if mode == "auto" else "manual",
        "operator_override": bool(operator_override),
        "cpu_capacity": int(capacity),
        "supervised_process_count": int(process_count),
        "per_process_budget": int(per_process_budget),
        "role_max_threads": int(role_max),
        "role_interop_max_threads": int(interop_max),
        "cpu_threads": int(cpu_threads),
        "interop_threads": int(interop_threads),
        "env": env_values,
        "current_env": current,
        "oversubscription_guarded": bool(int(cpu_threads) == 1 or int(cpu_threads) * int(process_count) <= int(capacity)),
    }


def apply_cpu_thread_policy_to_env(
    env: MutableMapping[str, str] | None = None,
    *,
    role: Any = None,
    supervised_process_count: int | None = None,
) -> dict[str, Any]:
    env_map: MutableMapping[str, str] = os.environ if env is None else env
    policy = resolve_cpu_thread_policy(
        env_map,
        role=role,
        supervised_process_count=supervised_process_count,
    )
    if str(policy.get("mode") or "auto") == "auto":
        for key, value in dict(policy.get("env") or {}).items():
            env_map[str(key)] = str(value)
    if int(policy.get("supervised_process_count") or 0) > 0:
        env_map["ENGINE_SUPERVISED_PROCESS_COUNT"] = str(int(policy["supervised_process_count"]))
    env_map["ENGINE_CPU_THREAD_POLICY_ROLE"] = str(policy.get("role") or "")
    return policy


def cpu_thread_policy_snapshot(
    env: MutableMapping[str, str] | dict[str, str] | None = None,
    *,
    role: Any = None,
    supervised_process_count: int | None = None,
) -> dict[str, Any]:
    return resolve_cpu_thread_policy(
        os.environ if env is None else env,
        role=role,
        supervised_process_count=supervised_process_count,
    )
