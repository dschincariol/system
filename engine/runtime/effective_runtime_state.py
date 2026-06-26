"""Effective Docker/Postgres/Redis runtime evidence validation.

The resource-isolation and Postgres tuning modules validate the intended
compose/env contract.  This module validates operator-collected runtime evidence
from Docker and Redis so production preflight can prove those limits actually
took effect without mounting the Docker socket into the trading container.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from engine.api.redaction import redact_api_payload, redact_string
from engine.runtime.postgres_tuning import format_bytes, parse_size_bytes

BYTES_IN_GIB = 1024**3


@dataclass(frozen=True)
class RuntimeServiceSpec:
    service: str
    container: str
    cpu_env: str
    memory_env: str
    memswap_env: str
    shm_env: str = ""
    ports: tuple[tuple[str, str, str, str], ...] = ()
    mounts: tuple[tuple[str, str], ...] = ()
    allow_port_bindings: bool = True


def _system_path(*parts: str) -> str:
    return str(Path(os.sep).joinpath(*parts))


SERVICE_SPECS: tuple[RuntimeServiceSpec, ...] = (
    RuntimeServiceSpec(
        service="runtime",
        container="trading-runtime",
        cpu_env="RUNTIME_CPUS",
        memory_env="RUNTIME_MEM_LIMIT",
        memswap_env="RUNTIME_MEMSWAP_LIMIT",
        shm_env="RUNTIME_SHM_SIZE",
        ports=(("8000/tcp", "DASHBOARD_DANGEROUS_PUBLIC_BIND_HOST", "DASHBOARD_PUBLIC_PORT", "8000"),),
        mounts=(
            ("/app/data", "TRADING_RUNTIME_DATA"),
            ("/app/logs", "TRADING_RUNTIME_LOGS"),
            ("/var/backups/trading", "TRADING_BACKUP_ROOT"),
        ),
    ),
    RuntimeServiceSpec(
        service="timescaledb",
        container="trading-timescaledb",
        cpu_env="TIMESCALE_CPUS",
        memory_env="TIMESCALE_MEM_LIMIT",
        memswap_env="TIMESCALE_MEMSWAP_LIMIT",
        shm_env="TIMESCALE_SHM_SIZE",
        ports=(("5432/tcp", "TIMESCALE_DANGEROUS_PUBLIC_BIND_HOST", "TIMESCALE_PORT", "5432"),),
        mounts=(
            (_system_path("var", "lib", "postgresql", "data"), "TRADING_TIMESCALE_DATA"),
            ("/var/backups/trading", "TRADING_BACKUP_ROOT"),
        ),
    ),
    RuntimeServiceSpec(
        service="redis",
        container="trading-redis",
        cpu_env="REDIS_CPUS",
        memory_env="REDIS_MEM_LIMIT",
        memswap_env="REDIS_MEMSWAP_LIMIT",
        ports=(("6379/tcp", "REDIS_DANGEROUS_PUBLIC_BIND_HOST", "REDIS_PORT", "6379"),),
        mounts=(("/data", "TRADING_REDIS_DATA"),),
    ),
    RuntimeServiceSpec(
        service="minio",
        container="trading-minio",
        cpu_env="MINIO_CPUS",
        memory_env="MINIO_MEM_LIMIT",
        memswap_env="MINIO_MEMSWAP_LIMIT",
        ports=(
            ("9000/tcp", "MINIO_DANGEROUS_PUBLIC_BIND_HOST", "MINIO_PORT", "9000"),
            ("9001/tcp", "MINIO_CONSOLE_DANGEROUS_PUBLIC_BIND_HOST", "MINIO_CONSOLE_PORT", "9001"),
        ),
        mounts=(("/data", "TRADING_MINIO_DATA"),),
    ),
    RuntimeServiceSpec(
        service="operator",
        container="trading-operator",
        cpu_env="OPERATOR_CPUS",
        memory_env="OPERATOR_MEM_LIMIT",
        memswap_env="OPERATOR_MEMSWAP_LIMIT",
        mounts=(
            ("/app/data", "TRADING_RUNTIME_DATA"),
            ("/app/logs", "TRADING_RUNTIME_LOGS"),
        ),
        allow_port_bindings=False,
    ),
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in {"1", "true", "yes", "on"}


def _parse_float(raw: Any) -> float | None:
    text = _clean(raw)
    if not text:
        return None
    try:
        parsed = float(text)
    except Exception:
        return None
    return parsed if parsed > 0 else None


def _parse_size_optional(raw: Any) -> int | None:
    text = _clean(raw)
    if not text or text.lower() in {"0", "none", "unbounded", "unlimited"}:
        return None
    try:
        return parse_size_bytes(text)
    except Exception:
        return None


def _gib(value: int | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / float(BYTES_IN_GIB), 2)


def _redact_string(value: str) -> str:
    return redact_string(value)


def redact_evidence(value: Any, *, key: str = "") -> Any:
    return redact_api_payload(value, key=key)


def _default_docker_evidence_path(env: Mapping[str, str]) -> str:
    return _clean(
        env.get("PREFLIGHT_DOCKER_INSPECT_JSON")
        or env.get("TRADING_DOCKER_INSPECT_JSON")
        or env.get("DOCKER_RUNTIME_INSPECT_JSON")
        or "/var/backups/trading/evidence/docker_runtime_inspect.json"
    )


def _default_redis_evidence_path(env: Mapping[str, str]) -> str:
    return _clean(
        env.get("PREFLIGHT_REDIS_CONFIG_EVIDENCE")
        or env.get("PREFLIGHT_REDIS_CONFIG_JSON")
        or env.get("TRADING_REDIS_CONFIG_EVIDENCE")
        or "/var/backups/trading/evidence/redis_config_get.txt"
    )


def _operator_commands(env: Mapping[str, str]) -> list[dict[str, str]]:
    docker_path = _default_docker_evidence_path(env)
    redis_path = _default_redis_evidence_path(env)
    pg_path = _clean(
        env.get("PREFLIGHT_POSTGRES_SETTINGS_JSON")
        or env.get("TRADING_POSTGRES_SETTINGS_JSON")
        or "/var/backups/trading/evidence/postgres_pg_settings.json"
    )
    containers = " ".join(spec.container for spec in SERVICE_SPECS)
    return [
        {
            "command": f"sudo install -d -m 0750 -o root -g trading {Path(docker_path).parent}",
            "proves": "Creates the protected evidence directory used by production preflight.",
        },
        {
            "command": f"sudo docker inspect {containers} > {docker_path}",
            "proves": "Captures actual Docker CPU, memory, memswap, shm, log, port, and mount state.",
        },
        {
            "command": (
                "sudo docker stats --no-stream --format '{{json .}}' "
                f"{containers} > {Path(docker_path).with_name('docker_runtime_stats.json')}"
            ),
            "proves": "Captures current runtime resource use for operator capacity review.",
        },
        {
            "command": (
                "sudo docker exec trading-redis sh -lc "
                "'redis-cli -a \"$(cat /run/secrets/redis_password)\" --raw "
                f"CONFIG GET maxmemory maxmemory-policy' > {redis_path}"
            ),
            "proves": "Captures effective Redis maxmemory and eviction policy without printing the password.",
        },
        {
            "command": (
                "sudo docker exec -u postgres trading-timescaledb psql "
                "-d \"${TIMESCALE_DB:-trading}\" -tA -c "
                "'SELECT jsonb_object_agg(name, jsonb_build_object('\"'\"'setting'\"'\"', setting, '\"'\"'unit'\"'\"', unit)) "
                "FROM pg_catalog.pg_settings;' "
                f"> {pg_path}"
            ),
            "proves": "Captures effective pg_settings for comparison with TIMESCALE_* env.",
        },
    ]


def _load_json_file(path: str) -> Any | None:
    if not path:
        return None
    evidence_path = Path(path)
    if not evidence_path.exists():
        return None
    return json.loads(evidence_path.read_text(encoding="utf-8"))


def _query_docker_inspect(containers: Sequence[str], *, timeout_s: float) -> tuple[Any | None, str | None]:
    if shutil.which("docker") is None:
        return None, "docker_not_available"
    try:
        proc = subprocess.run(
            ["docker", "inspect", *containers],
            check=False,
            capture_output=True,
            text=True,
            timeout=float(timeout_s),
        )
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        return None, _redact_string((proc.stderr or proc.stdout or "").strip() or f"exit={proc.returncode}")
    try:
        return json.loads(proc.stdout or "[]"), None
    except Exception as exc:
        return None, f"json_decode_failed:{type(exc).__name__}: {exc}"


def _inspect_items(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, Mapping):
        for key in ("docker_inspect", "inspect", "containers"):
            if isinstance(raw.get(key), list):
                return [dict(item) for item in raw.get(key) if isinstance(item, Mapping)]
        if all(isinstance(value, Mapping) for value in raw.values()):
            return [dict(value) for value in raw.values()]
        return [dict(raw)]
    if isinstance(raw, list):
        return [dict(item) for item in raw if isinstance(item, Mapping)]
    return []


def _container_name(item: Mapping[str, Any]) -> str:
    raw = _clean(item.get("Name")).lstrip("/")
    if raw:
        return raw
    config = item.get("Config") if isinstance(item.get("Config"), Mapping) else {}
    hostname = _clean((config or {}).get("Hostname"))
    return hostname


def _compose_service(item: Mapping[str, Any]) -> str:
    config = item.get("Config") if isinstance(item.get("Config"), Mapping) else {}
    labels = (config or {}).get("Labels") if isinstance((config or {}).get("Labels"), Mapping) else {}
    return _clean((labels or {}).get("com.docker.compose.service"))


def _find_container(items: Sequence[Mapping[str, Any]], spec: RuntimeServiceSpec) -> Mapping[str, Any] | None:
    for item in items:
        if _container_name(item) == spec.container:
            return item
    for item in items:
        if _compose_service(item) == spec.service:
            return item
    return None


def _host_config(item: Mapping[str, Any]) -> Mapping[str, Any]:
    return item.get("HostConfig") if isinstance(item.get("HostConfig"), Mapping) else {}


def _actual_cpus(host_config: Mapping[str, Any]) -> float | None:
    try:
        nano = int(host_config.get("NanoCpus") or 0)
    except Exception:
        nano = 0
    if nano > 0:
        return round(float(nano) / 1_000_000_000.0, 6)
    try:
        quota = int(host_config.get("CpuQuota") or 0)
        period = int(host_config.get("CpuPeriod") or 0)
    except Exception:
        quota = period = 0
    if quota > 0 and period > 0:
        return round(float(quota) / float(period), 6)
    return None


def _container_ports(item: Mapping[str, Any]) -> Mapping[str, Any]:
    host_config = _host_config(item)
    port_bindings = host_config.get("PortBindings")
    if isinstance(port_bindings, Mapping):
        return port_bindings
    network = item.get("NetworkSettings") if isinstance(item.get("NetworkSettings"), Mapping) else {}
    ports = (network or {}).get("Ports")
    return ports if isinstance(ports, Mapping) else {}


def _binding_pairs(bindings: Any) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    if not isinstance(bindings, list):
        return pairs
    for binding in bindings:
        if not isinstance(binding, Mapping):
            continue
        host_ip = _clean(binding.get("HostIp")) or "0.0.0.0"
        host_port = _clean(binding.get("HostPort"))
        pairs.append((host_ip, host_port))
    return pairs


def _mounts_by_destination(item: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    mounts = item.get("Mounts")
    if isinstance(mounts, list):
        for mount in mounts:
            if not isinstance(mount, Mapping):
                continue
            destination = _clean(mount.get("Destination"))
            if destination:
                out[destination] = mount
    return out


def _docker_service_snapshot(
    *,
    spec: RuntimeServiceSpec,
    item: Mapping[str, Any] | None,
    env: Mapping[str, str],
    errors: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    expected_cpu = _parse_float(env.get(spec.cpu_env))
    expected_memory = _parse_size_optional(env.get(spec.memory_env))
    expected_memswap = _parse_size_optional(env.get(spec.memswap_env))
    expected_shm = _parse_size_optional(env.get(spec.shm_env)) if spec.shm_env else None
    state: dict[str, Any] = {
        "container": spec.container,
        "expected": {
            "cpus": expected_cpu,
            "memory_bytes": expected_memory,
            "memory_gib": _gib(expected_memory),
            "memswap_bytes": expected_memswap,
            "memswap_gib": _gib(expected_memswap),
            "shm_bytes": expected_shm,
            "shm_gib": _gib(expected_shm),
        },
        "actual": {},
        "ports": {},
        "mounts": {},
        "ok": True,
    }
    if item is None:
        errors.append(f"docker runtime evidence missing container service={spec.service} container={spec.container}")
        state["ok"] = False
        state["missing"] = True
        return state

    host_config = _host_config(item)
    actual_cpu = _actual_cpus(host_config)
    actual_memory = int(host_config.get("Memory") or 0)
    actual_memswap = int(host_config.get("MemorySwap") or 0)
    actual_shm = int(host_config.get("ShmSize") or 0)
    log_config = host_config.get("LogConfig") if isinstance(host_config.get("LogConfig"), Mapping) else {}
    actual = {
        "cpus": actual_cpu,
        "memory_bytes": actual_memory,
        "memory_gib": _gib(actual_memory if actual_memory > 0 else None),
        "memswap_bytes": actual_memswap,
        "memswap_gib": _gib(actual_memswap if actual_memswap > 0 else None),
        "shm_bytes": actual_shm,
        "shm_gib": _gib(actual_shm if actual_shm > 0 else None),
        "log_driver": _clean((log_config or {}).get("Type")),
        "log_options": dict((log_config or {}).get("Config") or {}) if isinstance((log_config or {}).get("Config"), Mapping) else {},
    }
    state["actual"] = redact_evidence(actual)

    if expected_cpu is None:
        errors.append(f"docker runtime expected CPU limit missing env={spec.cpu_env}")
    elif actual_cpu is None or abs(float(actual_cpu) - float(expected_cpu)) > 0.001:
        errors.append(
            "docker runtime CPU limit drift "
            f"service={spec.service} expected={expected_cpu} actual={actual_cpu if actual_cpu is not None else 'unbounded'}"
        )

    if expected_memory is None:
        errors.append(f"docker runtime expected memory limit missing env={spec.memory_env}")
    elif actual_memory <= 0 or actual_memory != expected_memory:
        errors.append(
            "docker runtime memory limit drift "
            f"service={spec.service} expected={format_bytes(expected_memory)} actual={format_bytes(actual_memory or None)}"
        )

    if expected_memswap is None:
        errors.append(f"docker runtime expected memswap limit missing env={spec.memswap_env}")
    elif actual_memswap <= 0 or actual_memswap != expected_memswap:
        errors.append(
            "docker runtime memswap limit drift "
            f"service={spec.service} expected={format_bytes(expected_memswap)} actual={format_bytes(actual_memswap if actual_memswap > 0 else None)}"
        )

    if expected_shm is not None and actual_shm < expected_shm:
        errors.append(
            "docker runtime shm below expected "
            f"service={spec.service} expected={format_bytes(expected_shm)} actual={format_bytes(actual_shm or None)}"
        )
    elif spec.shm_env and expected_shm is None:
        errors.append(f"docker runtime expected shm missing env={spec.shm_env}")

    expected_log_driver = _clean(env.get("DOCKER_LOG_DRIVER") or "local")
    expected_log_size = _clean(env.get("DOCKER_LOG_MAX_SIZE") or "50m")
    expected_log_file = _clean(env.get("DOCKER_LOG_MAX_FILE") or "5")
    log_options = actual.get("log_options") if isinstance(actual.get("log_options"), Mapping) else {}
    if actual.get("log_driver") != expected_log_driver:
        errors.append(
            "docker runtime log driver drift "
            f"service={spec.service} expected={expected_log_driver} actual={actual.get('log_driver') or 'missing'}"
        )
    if _clean((log_options or {}).get("max-size")) != expected_log_size:
        errors.append(
            "docker runtime log max-size drift "
            f"service={spec.service} expected={expected_log_size} actual={_clean((log_options or {}).get('max-size')) or 'missing'}"
        )
    if _clean((log_options or {}).get("max-file")) != expected_log_file:
        errors.append(
            "docker runtime log max-file drift "
            f"service={spec.service} expected={expected_log_file} actual={_clean((log_options or {}).get('max-file')) or 'missing'}"
        )

    port_state: dict[str, Any] = {}
    ports = _container_ports(item)
    if not spec.allow_port_bindings:
        published = {key: _binding_pairs(value) for key, value in ports.items() if _binding_pairs(value)}
        port_state["published"] = published
        if published:
            errors.append(f"docker runtime unexpected published port service={spec.service} bindings={published}")
    else:
        for container_port, host_env, port_env, default_port in spec.ports:
            expected_host = _clean(env.get(host_env) or "127.0.0.1")
            expected_port = _clean(env.get(port_env) or default_port)
            pairs = _binding_pairs(ports.get(container_port))
            port_state[container_port] = {
                "expected_host": expected_host,
                "expected_port": expected_port,
                "actual": pairs,
            }
            if (expected_host, expected_port) not in pairs:
                errors.append(
                    "docker runtime port binding drift "
                    f"service={spec.service} port={container_port} expected={expected_host}:{expected_port} actual={pairs or 'missing'}"
                )
    state["ports"] = port_state

    mount_state: dict[str, Any] = {}
    by_dest = _mounts_by_destination(item)
    for destination, env_name in spec.mounts:
        expected_source = _clean(env.get(env_name))
        mount = by_dest.get(destination)
        source = _clean((mount or {}).get("Source")) if mount else ""
        mount_type = _clean((mount or {}).get("Type")) if mount else ""
        rw = bool((mount or {}).get("RW")) if mount else None
        mount_state[destination] = {
            "expected_source": expected_source or None,
            "actual_source": source or None,
            "type": mount_type or None,
            "rw": rw,
        }
        if not mount:
            errors.append(f"docker runtime mount missing service={spec.service} destination={destination}")
        elif mount_type != "bind":
            errors.append(f"docker runtime mount not bind service={spec.service} destination={destination} type={mount_type}")
        elif expected_source and source != expected_source:
            errors.append(
                "docker runtime mount source drift "
                f"service={spec.service} destination={destination} expected={expected_source} actual={source or 'missing'}"
            )
        elif not expected_source:
            warnings.append(f"docker runtime mount expected source missing env={env_name} service={spec.service}")
    state["mounts"] = mount_state

    service_errors = [error for error in errors if f"service={spec.service}" in error or f"container={spec.container}" in error]
    state["ok"] = not service_errors
    return state


def _docker_runtime_snapshot(
    env: Mapping[str, str],
    *,
    required: bool,
    inspect_data: Any | None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    source = "provided"
    if inspect_data is None:
        evidence_path = _default_docker_evidence_path(env)
        try:
            inspect_data = _load_json_file(evidence_path)
            source = f"file:{evidence_path}" if inspect_data is not None else "missing"
        except Exception as exc:
            source = f"file_error:{evidence_path}"
            errors.append(f"docker runtime inspect evidence read failed: {type(exc).__name__}: {exc}")
        if inspect_data is None and _truthy(env.get("PREFLIGHT_DOCKER_RUNTIME_QUERY")):
            timeout = float(_clean(env.get("PREFLIGHT_DOCKER_RUNTIME_TIMEOUT_S") or "2.0"))
            inspect_data, query_error = _query_docker_inspect([spec.container for spec in SERVICE_SPECS], timeout_s=timeout)
            source = "docker"
            if query_error:
                warnings.append(f"docker runtime inspect query unavailable: {query_error}")

    items = _inspect_items(inspect_data)
    if required and not items:
        errors.append("docker_runtime_evidence_missing")

    services: dict[str, Any] = {}
    for spec in SERVICE_SPECS:
        item = _find_container(items, spec)
        services[spec.service] = _docker_service_snapshot(
            spec=spec,
            item=item,
            env=env,
            errors=errors,
            warnings=warnings,
        )

    return {
        "ok": not errors,
        "required": bool(required),
        "source": source,
        "errors": errors,
        "warnings": warnings,
        "services": services,
    }


def _parse_redis_config(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        if isinstance(parsed, Mapping):
            return dict(parsed)
        if isinstance(parsed, list):
            return {str(item.get("key")): item.get("value") for item in parsed if isinstance(item, Mapping)}
    except Exception:
        # no-op-guard: allow - raw redis-cli CONFIG GET text is the fallback format.
        pass
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    pairs: dict[str, Any] = {}
    for idx in range(0, len(lines) - 1, 2):
        pairs[lines[idx]] = lines[idx + 1]
    return pairs


def _redis_effective_snapshot(
    env: Mapping[str, str],
    *,
    required: bool,
    redis_config: Mapping[str, Any] | str | None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    source = "provided"
    parsed: dict[str, Any]
    if redis_config is None:
        path = _default_redis_evidence_path(env)
        evidence_path = Path(path)
        if evidence_path.exists():
            parsed = _parse_redis_config(evidence_path.read_text(encoding="utf-8"))
            source = f"file:{path}"
        else:
            parsed = {}
            source = "missing"
    elif isinstance(redis_config, str):
        parsed = _parse_redis_config(redis_config)
    else:
        parsed = dict(redis_config)

    expected_maxmemory = _parse_size_optional(env.get("REDIS_MAXMEMORY"))
    expected_policy = _clean(env.get("REDIS_MAXMEMORY_POLICY") or "allkeys-lru")
    raw_actual_maxmemory = parsed.get("maxmemory")
    raw_actual_policy = parsed.get("maxmemory-policy") or parsed.get("maxmemory_policy")
    actual_maxmemory: int | None
    try:
        actual_maxmemory = int(str(raw_actual_maxmemory).strip()) if raw_actual_maxmemory is not None else None
    except Exception:
        actual_maxmemory = None
    actual_policy = _clean(raw_actual_policy)

    if required and not parsed:
        errors.append("redis_effective_config_evidence_missing")
    if expected_maxmemory is None:
        errors.append("redis effective expected maxmemory missing env=REDIS_MAXMEMORY")
    elif actual_maxmemory is not None and actual_maxmemory != expected_maxmemory:
        errors.append(
            "redis effective maxmemory drift "
            f"expected={format_bytes(expected_maxmemory)} actual={format_bytes(actual_maxmemory)}"
        )
    elif required and actual_maxmemory is None:
        errors.append("redis effective maxmemory missing")
    if expected_policy and actual_policy and actual_policy != expected_policy:
        errors.append(f"redis effective maxmemory-policy drift expected={expected_policy} actual={actual_policy}")
    elif required and expected_policy and not actual_policy:
        errors.append("redis effective maxmemory-policy missing")

    return {
        "ok": not errors,
        "required": bool(required),
        "source": source,
        "errors": errors,
        "warnings": warnings,
        "expected": {
            "maxmemory_bytes": expected_maxmemory,
            "maxmemory_gib": _gib(expected_maxmemory),
            "maxmemory_policy": expected_policy,
        },
        "actual": {
            "maxmemory_bytes": actual_maxmemory,
            "maxmemory_gib": _gib(actual_maxmemory),
            "maxmemory_policy": actual_policy or None,
        },
    }


def effective_runtime_state_snapshot(
    env: Mapping[str, str] | None = None,
    *,
    required: bool | None = None,
    docker_inspect: Any | None = None,
    redis_config: Mapping[str, Any] | str | None = None,
) -> dict[str, Any]:
    source_env = {str(k): str(v) for k, v in (env or os.environ).items()}
    is_required = bool(required) if required is not None else _truthy(source_env.get("PREFLIGHT_REQUIRE_DOCKER_RUNTIME_EVIDENCE"))

    docker_state = _docker_runtime_snapshot(source_env, required=is_required, inspect_data=docker_inspect)
    redis_state = _redis_effective_snapshot(source_env, required=is_required, redis_config=redis_config)

    errors = list(docker_state.get("errors") or []) + list(redis_state.get("errors") or [])
    warnings = list(docker_state.get("warnings") or []) + list(redis_state.get("warnings") or [])
    return {
        "ok": not errors,
        "required": is_required,
        "errors": errors,
        "warnings": warnings,
        "docker": docker_state,
        "redis": redis_state,
        "operator_commands": _operator_commands(source_env),
        "redacted": True,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Emit JSON.")
    parser.add_argument("--required", action="store_true", help="Require evidence and fail when missing or drifted.")
    args = parser.parse_args(None if argv is None else list(argv))
    snapshot = effective_runtime_state_snapshot(required=bool(args.required))
    if args.json:
        print(json.dumps(redact_evidence(snapshot), separators=(",", ":"), sort_keys=True))
    else:
        status = "PASS" if snapshot.get("ok") else "FAIL"
        print(f"effective runtime state: {status}")
        for item in list(snapshot.get("errors") or []):
            print(f"ERROR: {item}")
        for item in list(snapshot.get("warnings") or []):
            print(f"WARN: {item}")
        if not snapshot.get("ok"):
            print("operator commands:")
            for row in list(snapshot.get("operator_commands") or []):
                print(f"- {row.get('command')} # {row.get('proves')}")
    return 0 if snapshot.get("ok") else 3


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "SERVICE_SPECS",
    "effective_runtime_state_snapshot",
    "redact_evidence",
]
