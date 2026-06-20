from __future__ import annotations

"""Production resource isolation checks for Docker-backed services."""

import math
import os
import re
from typing import Any, Mapping

_BYTE_UNITS = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": 1024**3,
    "gb": 1024**3,
    "gib": 1024**3,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
}

_RESOURCE_SERVICES = (
    ("runtime", "RUNTIME_CPUS", "RUNTIME_MEM_LIMIT", "RUNTIME_SHM_SIZE"),
    ("timescaledb", "TIMESCALE_CPUS", "TIMESCALE_MEM_LIMIT", "TIMESCALE_SHM_SIZE"),
    ("redis", "REDIS_CPUS", "REDIS_MEM_LIMIT", ""),
    ("minio", "MINIO_CPUS", "MINIO_MEM_LIMIT", ""),
    ("operator", "OPERATOR_CPUS", "OPERATOR_MEM_LIMIT", ""),
)

_RUNTIME_THREAD_KEYS = (
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "NUMEXPR_MAX_THREADS",
    "VECLIB_MAXIMUM_THREADS",
    "TORCH_CPU_THREADS",
    "TORCH_INTEROP_THREADS",
)

_RUNTIME_WORKER_KEYS = (
    "RESOURCE_SCHEDULER_GLOBAL_MAX",
    "RESOURCE_SCHEDULER_EXECUTION_MAX",
    "RESOURCE_SCHEDULER_INFERENCE_MAX",
    "RESOURCE_SCHEDULER_TRAINING_MAX",
    "RESOURCE_SCHEDULER_REPLAY_MAX",
    "RESOURCE_SCHEDULER_BACKGROUND_MAX",
    "MODEL_TRAIN_N_JOBS",
    "MODEL_TRAIN_MAX_N_JOBS",
    "LGBM_N_JOBS",
    "LGBM_RANKER_N_JOBS",
    "XGB_N_JOBS",
    "META_LABEL_N_JOBS",
    "TSFRESH_N_JOBS",
    "TSFRESH_MAX_N_JOBS",
)

_RUNTIME_BATCH_KEYS = (
    "TSFRESH_SNAPSHOT_SYMBOL_LIMIT",
    "TSFRESH_SNAPSHOT_MAX_SYMBOLS",
    "TSFRESH_SNAPSHOT_BATCH_SIZE",
    "TSFRESH_SNAPSHOT_MAX_BATCH_SIZE",
    "TUNE_N_TRIALS",
    "TUNE_MAX_N_TRIALS",
)

_RUNTIME_ZERO_ALLOWED_WORKER_KEYS = {
    "RESOURCE_SCHEDULER_EXECUTION_MAX",
    "RESOURCE_SCHEDULER_INFERENCE_MAX",
    "RESOURCE_SCHEDULER_TRAINING_MAX",
    "RESOURCE_SCHEDULER_REPLAY_MAX",
    "RESOURCE_SCHEDULER_BACKGROUND_MAX",
    "TSFRESH_N_JOBS",
}

_PRODUCTION_VALUES = {"prod", "production", "live"}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "on"}


def _env_first(env: Mapping[str, str], *names: str) -> str:
    for name in names:
        raw = _clean(env.get(name))
        if raw:
            return raw
    return ""


def _parse_cpu(value: Any) -> float | None:
    text = _clean(value).lower()
    if text in {"", "0", "none", "unbounded", "unlimited"}:
        return None
    try:
        parsed = float(text)
    except Exception:
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _parse_int(value: Any) -> int | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = int(float(text))
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _parse_bytes(value: Any) -> int | None:
    text = _clean(value).lower().replace(" ", "")
    if text in {"", "none", "unbounded", "unlimited"}:
        return None
    match = re.fullmatch(r"(?P<num>\d+(?:\.\d+)?)(?P<unit>[a-z]*)", text)
    if not match:
        return None
    unit = match.group("unit")
    multiplier = _BYTE_UNITS.get(unit)
    if multiplier is None:
        return None
    parsed = int(float(match.group("num")) * multiplier)
    return parsed


def _gib(value: int | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / float(1024**3), 2)


def _production_like(env: Mapping[str, str]) -> bool:
    for name in ("ENV", "APP_ENV", "ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
        if _clean(env.get(name)).lower() in _PRODUCTION_VALUES:
            return True
    return False


def _checks_enabled(env: Mapping[str, str]) -> bool:
    raw = env.get("PREFLIGHT_CHECK_RESOURCE_LIMITS")
    if raw is not None and _clean(raw) != "":
        return _truthy(raw)
    return _production_like(env)


def _service_limits(env: Mapping[str, str]) -> dict[str, dict[str, Any]]:
    services: dict[str, dict[str, Any]] = {}
    for service, cpu_key, mem_key, shm_key in _RESOURCE_SERVICES:
        cpu_raw = _clean(env.get(cpu_key))
        mem_raw = _clean(env.get(mem_key))
        shm_raw = _clean(env.get(shm_key)) if shm_key else ""
        services[service] = {
            "cpu_env": cpu_key,
            "cpu_limit": cpu_raw,
            "cpu_limit_value": _parse_cpu(cpu_raw),
            "memory_env": mem_key,
            "memory_limit": mem_raw,
            "memory_limit_bytes": _parse_bytes(mem_raw),
            "memory_limit_gib": _gib(_parse_bytes(mem_raw)),
            "shm_env": shm_key or None,
            "shm_size": shm_raw or None,
            "shm_size_bytes": _parse_bytes(shm_raw) if shm_key else None,
            "shm_size_gib": _gib(_parse_bytes(shm_raw)) if shm_key else None,
        }
    return services


def _add_limit_warnings(
    env: Mapping[str, str],
    services: dict[str, dict[str, Any]],
    warnings: list[str],
) -> None:
    for service, limits in services.items():
        if limits.get("cpu_limit_value") is None:
            warnings.append(
                "resource isolation unbounded "
                f"service={service} cpu_limit_env={limits.get('cpu_env')}"
            )
        if limits.get("memory_limit_bytes") is None or int(limits.get("memory_limit_bytes") or 0) <= 0:
            warnings.append(
                "resource isolation unbounded "
                f"service={service} memory_limit_env={limits.get('memory_env')}"
            )

    runtime = services.get("runtime") or {}
    runtime_shm = runtime.get("shm_size_bytes")
    min_runtime_shm = _parse_bytes(env.get("TRADING_RUNTIME_MIN_SHM_SIZE") or "4g") or (4 * 1024**3)
    if runtime_shm is None or int(runtime_shm) < int(min_runtime_shm):
        warnings.append(
            "resource isolation insufficient shm "
            f"service=runtime shm_size={runtime.get('shm_size') or 'missing'} min={_gib(min_runtime_shm)}GiB"
        )

    timescale = services.get("timescaledb") or {}
    timescale_shm = timescale.get("shm_size_bytes")
    if timescale_shm is None or int(timescale_shm) <= 0:
        warnings.append("resource isolation missing shm service=timescaledb shm_env=TIMESCALE_SHM_SIZE")


def _add_headroom_warnings(
    env: Mapping[str, str],
    services: dict[str, dict[str, Any]],
    warnings: list[str],
    state: dict[str, Any],
) -> None:
    host_cpu = _parse_cpu(_env_first(env, "TRADING_RESOURCE_HOST_CPUS", "RESOURCE_HOST_CPUS"))
    host_mem = _parse_bytes(_env_first(env, "TRADING_RESOURCE_HOST_MEMORY", "RESOURCE_HOST_MEMORY"))
    min_cpu = _parse_cpu(_env_first(env, "TRADING_RESOURCE_MIN_HEADROOM_CPUS", "RESOURCE_MIN_HEADROOM_CPUS")) or 0.0
    min_mem = _parse_bytes(_env_first(env, "TRADING_RESOURCE_MIN_HEADROOM_MEMORY", "RESOURCE_MIN_HEADROOM_MEMORY")) or 0
    total_cpu = sum(float(item.get("cpu_limit_value") or 0.0) for item in services.values())
    total_mem = sum(int(item.get("memory_limit_bytes") or 0) for item in services.values())
    cpu_bounded = all(item.get("cpu_limit_value") is not None for item in services.values())
    mem_bounded = all((item.get("memory_limit_bytes") or 0) > 0 for item in services.values())

    host_state = {
        "host_cpus": host_cpu,
        "host_memory_gib": _gib(host_mem),
        "min_headroom_cpus": min_cpu,
        "min_headroom_memory_gib": _gib(min_mem),
        "total_limited_cpus": round(total_cpu, 3),
        "total_limited_memory_gib": _gib(total_mem),
    }
    state["host"] = host_state

    if host_cpu is None:
        warnings.append("resource isolation host headroom unchecked: TRADING_RESOURCE_HOST_CPUS is missing")
    elif cpu_bounded:
        cpu_headroom = float(host_cpu) - float(total_cpu)
        host_state["cpu_headroom"] = round(cpu_headroom, 3)
        if cpu_headroom < float(min_cpu):
            warnings.append(
                "resource isolation host CPU headroom below minimum "
                f"headroom={round(cpu_headroom, 3)} min={round(float(min_cpu), 3)}"
            )

    if host_mem is None:
        warnings.append("resource isolation host headroom unchecked: TRADING_RESOURCE_HOST_MEMORY is missing")
    elif mem_bounded:
        mem_headroom = int(host_mem) - int(total_mem)
        host_state["memory_headroom_gib"] = _gib(mem_headroom)
        if mem_headroom < int(min_mem):
            warnings.append(
                "resource isolation host memory headroom below minimum "
                f"headroom={_gib(mem_headroom)}GiB min={_gib(min_mem)}GiB"
            )


def _add_postgres_warnings(
    env: Mapping[str, str],
    services: dict[str, dict[str, Any]],
    warnings: list[str],
    state: dict[str, Any],
) -> None:
    timescale_mem = int((services.get("timescaledb") or {}).get("memory_limit_bytes") or 0)
    shared = _parse_bytes(_env_first(env, "POSTGRES_SHARED_BUFFERS", "TIMESCALE_SHARED_BUFFERS"))
    effective_cache = _parse_bytes(_env_first(env, "POSTGRES_EFFECTIVE_CACHE_SIZE", "TIMESCALE_EFFECTIVE_CACHE_SIZE"))
    work_mem = _parse_bytes(_env_first(env, "POSTGRES_WORK_MEM", "TIMESCALE_WORK_MEM"))
    maintenance = _parse_bytes(_env_first(env, "POSTGRES_MAINTENANCE_WORK_MEM", "TIMESCALE_MAINTENANCE_WORK_MEM"))
    max_connections = _parse_int(_env_first(env, "POSTGRES_MAX_CONNECTIONS", "TIMESCALE_MAX_CONNECTIONS"))
    pg_state = {
        "shared_buffers_gib": _gib(shared),
        "effective_cache_size_gib": _gib(effective_cache),
        "work_mem_mib": round(float(work_mem or 0) / float(1024**2), 2) if work_mem is not None else None,
        "maintenance_work_mem_gib": _gib(maintenance),
        "max_connections": max_connections,
    }
    state["postgres"] = pg_state

    required = {
        "POSTGRES_SHARED_BUFFERS/TIMESCALE_SHARED_BUFFERS": shared,
        "POSTGRES_EFFECTIVE_CACHE_SIZE/TIMESCALE_EFFECTIVE_CACHE_SIZE": effective_cache,
        "POSTGRES_WORK_MEM/TIMESCALE_WORK_MEM": work_mem,
        "POSTGRES_MAINTENANCE_WORK_MEM/TIMESCALE_MAINTENANCE_WORK_MEM": maintenance,
        "POSTGRES_MAX_CONNECTIONS/TIMESCALE_MAX_CONNECTIONS": max_connections,
    }
    for key, parsed in required.items():
        if parsed is None:
            warnings.append(f"resource consistency postgres setting missing_or_invalid env={key}")

    if timescale_mem <= 0 or None in {shared, effective_cache, work_mem, maintenance, max_connections}:
        return

    assert shared is not None
    assert effective_cache is not None
    assert work_mem is not None
    assert maintenance is not None
    assert max_connections is not None
    estimated_peak = int(shared) + int(maintenance) + int(work_mem) * int(max_connections)
    pg_state["estimated_peak_gib"] = _gib(estimated_peak)
    pg_state["timescale_memory_limit_gib"] = _gib(timescale_mem)

    if int(shared) > int(timescale_mem * 0.40):
        warnings.append(
            "resource consistency postgres shared_buffers high "
            f"shared_buffers={_gib(shared)}GiB timescale_mem_limit={_gib(timescale_mem)}GiB"
        )
    if int(effective_cache) > timescale_mem:
        warnings.append(
            "resource consistency postgres effective_cache_size exceeds container memory "
            f"effective_cache_size={_gib(effective_cache)}GiB timescale_mem_limit={_gib(timescale_mem)}GiB"
        )
    if estimated_peak > int(timescale_mem * 0.80):
        warnings.append(
            "resource consistency postgres peak memory estimate high "
            f"estimated={_gib(estimated_peak)}GiB timescale_mem_limit={_gib(timescale_mem)}GiB"
        )


def _add_redis_warnings(
    env: Mapping[str, str],
    services: dict[str, dict[str, Any]],
    warnings: list[str],
    state: dict[str, Any],
) -> None:
    redis_mem = int((services.get("redis") or {}).get("memory_limit_bytes") or 0)
    maxmemory = _parse_bytes(env.get("REDIS_MAXMEMORY"))
    redis_state = {
        "maxmemory_gib": _gib(maxmemory),
        "memory_limit_gib": _gib(redis_mem if redis_mem > 0 else None),
        "policy": _clean(env.get("REDIS_MAXMEMORY_POLICY")) or None,
    }
    state["redis"] = redis_state

    if maxmemory is None or int(maxmemory) <= 0:
        warnings.append("resource consistency redis maxmemory unbounded env=REDIS_MAXMEMORY")
        return
    if redis_mem <= 0:
        return
    if int(maxmemory) > int(redis_mem * 0.85):
        warnings.append(
            "resource consistency redis maxmemory too close to container memory "
            f"maxmemory={_gib(maxmemory)}GiB redis_mem_limit={_gib(redis_mem)}GiB"
        )


def _add_runtime_thread_warnings(
    env: Mapping[str, str],
    services: dict[str, dict[str, Any]],
    warnings: list[str],
    state: dict[str, Any],
) -> None:
    runtime_cpus = (services.get("runtime") or {}).get("cpu_limit_value")
    thread_state: dict[str, int | None] = {}
    state["runtime_threads"] = thread_state
    if runtime_cpus is None:
        return
    max_threads = max(1, int(math.floor(float(runtime_cpus))))
    for key in _RUNTIME_THREAD_KEYS:
        parsed = _parse_int(env.get(key))
        thread_state[key] = parsed
        if parsed is None:
            warnings.append(f"resource consistency runtime thread default missing_or_invalid env={key}")
        elif int(parsed) > max_threads:
            warnings.append(
                "resource consistency runtime thread default exceeds CPU limit "
                f"env={key} value={parsed} runtime_cpus={runtime_cpus}"
            )


def _add_runtime_worker_warnings(
    env: Mapping[str, str],
    services: dict[str, dict[str, Any]],
    warnings: list[str],
    state: dict[str, Any],
) -> None:
    runtime_cpus = (services.get("runtime") or {}).get("cpu_limit_value")
    worker_state: dict[str, int | None] = {}
    state["runtime_workers"] = worker_state
    if runtime_cpus is None:
        return

    runtime_cpu_floor = max(1, int(math.floor(float(runtime_cpus))))
    for key in _RUNTIME_WORKER_KEYS:
        if key in _RUNTIME_ZERO_ALLOWED_WORKER_KEYS:
            raw = _clean(env.get(key))
            try:
                parsed_zero_allowed = int(float(raw)) if raw else None
            except Exception:
                parsed_zero_allowed = None
            worker_state[key] = parsed_zero_allowed
            if raw == "" or parsed_zero_allowed is None:
                warnings.append(f"resource consistency runtime worker default missing_or_invalid env={key}")
            elif int(parsed_zero_allowed) < 0:
                warnings.append(f"resource consistency runtime worker default below minimum env={key} value={raw}")
            elif int(parsed_zero_allowed) > runtime_cpu_floor:
                warnings.append(
                    "resource consistency runtime worker default exceeds CPU limit "
                    f"env={key} value={parsed_zero_allowed} runtime_cpus={runtime_cpus}"
                )
            continue

        parsed = _parse_int(env.get(key))
        worker_state[key] = parsed
        if parsed is None:
            warnings.append(f"resource consistency runtime worker default missing_or_invalid env={key}")
        elif int(parsed) > runtime_cpu_floor:
            warnings.append(
                "resource consistency runtime worker default exceeds CPU limit "
                f"env={key} value={parsed} runtime_cpus={runtime_cpus}"
            )

    global_max = worker_state.get("RESOURCE_SCHEDULER_GLOBAL_MAX")
    scoped_sum = sum(
        int(worker_state.get(key) or 0)
        for key in (
            "RESOURCE_SCHEDULER_EXECUTION_MAX",
            "RESOURCE_SCHEDULER_INFERENCE_MAX",
            "RESOURCE_SCHEDULER_TRAINING_MAX",
            "RESOURCE_SCHEDULER_REPLAY_MAX",
            "RESOURCE_SCHEDULER_BACKGROUND_MAX",
        )
    )
    if global_max is not None and scoped_sum > int(global_max) * 4:
        warnings.append(
            "resource consistency scheduler scoped worker budget high "
            f"scoped_sum={scoped_sum} global_max={global_max}"
        )

    model_n_jobs = worker_state.get("MODEL_TRAIN_N_JOBS")
    model_max_n_jobs = worker_state.get("MODEL_TRAIN_MAX_N_JOBS")
    if model_n_jobs is not None and model_max_n_jobs is not None and int(model_n_jobs) > int(model_max_n_jobs):
        warnings.append(
            "resource consistency model worker default exceeds max "
            f"MODEL_TRAIN_N_JOBS={model_n_jobs} MODEL_TRAIN_MAX_N_JOBS={model_max_n_jobs}"
        )

    tsfresh_n_jobs = worker_state.get("TSFRESH_N_JOBS")
    tsfresh_max_n_jobs = worker_state.get("TSFRESH_MAX_N_JOBS")
    if (
        tsfresh_n_jobs is not None
        and tsfresh_max_n_jobs is not None
        and int(tsfresh_n_jobs) > int(tsfresh_max_n_jobs)
    ):
        warnings.append(
            "resource consistency tsfresh worker default exceeds max "
            f"TSFRESH_N_JOBS={tsfresh_n_jobs} TSFRESH_MAX_N_JOBS={tsfresh_max_n_jobs}"
        )


def _add_runtime_batch_warnings(
    env: Mapping[str, str],
    warnings: list[str],
    state: dict[str, Any],
) -> None:
    batch_state: dict[str, int | None] = {}
    state["runtime_batches"] = batch_state
    for key in _RUNTIME_BATCH_KEYS:
        parsed = _parse_int(env.get(key))
        batch_state[key] = parsed
        if parsed is None:
            warnings.append(f"resource consistency runtime batch default missing_or_invalid env={key}")

    pairs = (
        ("TSFRESH_SNAPSHOT_SYMBOL_LIMIT", "TSFRESH_SNAPSHOT_MAX_SYMBOLS"),
        ("TSFRESH_SNAPSHOT_BATCH_SIZE", "TSFRESH_SNAPSHOT_MAX_BATCH_SIZE"),
        ("TUNE_N_TRIALS", "TUNE_MAX_N_TRIALS"),
    )
    for value_key, max_key in pairs:
        value = batch_state.get(value_key)
        max_value = batch_state.get(max_key)
        if value is not None and max_value is not None and int(value) > int(max_value):
            warnings.append(
                "resource consistency runtime batch default exceeds max "
                f"{value_key}={value} {max_key}={max_value}"
            )


def check_resource_isolation(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    source_env: Mapping[str, str] = env if env is not None else os.environ
    state: dict[str, Any] = {
        "checked": False,
        "production_like": _production_like(source_env),
        "ok": True,
        "notes": [],
        "warnings": [],
        "services": {},
    }
    if not _checks_enabled(source_env):
        state["notes"].append("resource isolation check skipped")
        return state

    warnings: list[str] = []
    services = _service_limits(source_env)
    state["checked"] = True
    state["services"] = services

    _add_limit_warnings(source_env, services, warnings)
    _add_headroom_warnings(source_env, services, warnings, state)
    _add_postgres_warnings(source_env, services, warnings, state)
    _add_redis_warnings(source_env, services, warnings, state)
    _add_runtime_thread_warnings(source_env, services, warnings, state)
    _add_runtime_worker_warnings(source_env, services, warnings, state)
    _add_runtime_batch_warnings(source_env, warnings, state)

    if warnings:
        state["ok"] = False
        state["warnings"] = warnings
        return state

    host = dict(state.get("host") or {})
    state["notes"].append(
        "resource isolation ok "
        f"limited_cpus={host.get('total_limited_cpus')} "
        f"cpu_headroom={host.get('cpu_headroom')} "
        f"limited_memory_gib={host.get('total_limited_memory_gib')} "
        f"memory_headroom_gib={host.get('memory_headroom_gib')}"
    )
    return state


__all__ = ["check_resource_isolation"]
