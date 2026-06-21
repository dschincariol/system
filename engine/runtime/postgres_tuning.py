"""Docker Postgres/Timescale tuning validation helpers.

The compose deployment owns Docker service limits and Postgres settings through
``TIMESCALE_*`` environment variables.  This module keeps production preflight
validation independent of Docker itself while still validating the same single
source of truth operators edit in ``deploy/compose/.env``.
"""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

BYTES_IN_KIB = 1024
BYTES_IN_MIB = BYTES_IN_KIB**2
BYTES_IN_GIB = BYTES_IN_KIB**3
BYTES_IN_TIB = BYTES_IN_KIB**4

_SIZE_RE = re.compile(
    r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>b|bytes?|k|kb|kib|m|mb|mib|g|gb|gib|t|tb|tib)?\s*$",
    re.IGNORECASE,
)
_PG_UNIT_RE = re.compile(r"^\s*(?P<count>\d+(?:\.\d+)?)?\s*(?P<unit>b|bytes?|kb|kB|MB|GB|TB|ms|s|min|h)?\s*$")
_DURATION_RE = re.compile(r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>ms|s|sec|secs|second|seconds|min|mins|minute|minutes|h|hr|hour|hours)?\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class SettingSpec:
    env: str
    pg_name: str
    kind: str
    required: bool = True


PG_SETTING_SPECS: tuple[SettingSpec, ...] = (
    SettingSpec("TIMESCALE_MAX_CONNECTIONS", "max_connections", "int"),
    SettingSpec("TIMESCALE_SHARED_BUFFERS", "shared_buffers", "bytes"),
    SettingSpec("TIMESCALE_EFFECTIVE_CACHE_SIZE", "effective_cache_size", "bytes"),
    SettingSpec("TIMESCALE_WORK_MEM", "work_mem", "bytes"),
    SettingSpec("TIMESCALE_MAINTENANCE_WORK_MEM", "maintenance_work_mem", "bytes"),
    SettingSpec("TIMESCALE_AUTOVACUUM_WORK_MEM", "autovacuum_work_mem", "bytes"),
    SettingSpec("TIMESCALE_WAL_BUFFERS", "wal_buffers", "bytes"),
    SettingSpec("TIMESCALE_MIN_WAL_SIZE", "min_wal_size", "bytes"),
    SettingSpec("TIMESCALE_MAX_WAL_SIZE", "max_wal_size", "bytes"),
    SettingSpec("TIMESCALE_WAL_KEEP_SIZE", "wal_keep_size", "bytes"),
    SettingSpec("TIMESCALE_MAX_SLOT_WAL_KEEP_SIZE", "max_slot_wal_keep_size", "bytes"),
    SettingSpec("TIMESCALE_CHECKPOINT_TIMEOUT", "checkpoint_timeout", "duration"),
    SettingSpec("TIMESCALE_CHECKPOINT_COMPLETION_TARGET", "checkpoint_completion_target", "float"),
    SettingSpec("TIMESCALE_MAX_WORKER_PROCESSES", "max_worker_processes", "int"),
    SettingSpec("TIMESCALE_MAX_PARALLEL_WORKERS", "max_parallel_workers", "int"),
    SettingSpec("TIMESCALE_MAX_PARALLEL_WORKERS_PER_GATHER", "max_parallel_workers_per_gather", "int"),
    SettingSpec("TIMESCALE_MAX_PARALLEL_MAINTENANCE_WORKERS", "max_parallel_maintenance_workers", "int"),
    SettingSpec("TIMESCALE_TIMESCALEDB_MAX_BACKGROUND_WORKERS", "timescaledb.max_background_workers", "int"),
    SettingSpec("TIMESCALE_AUTOVACUUM", "autovacuum", "bool"),
    SettingSpec("TIMESCALE_AUTOVACUUM_MAX_WORKERS", "autovacuum_max_workers", "int"),
    SettingSpec("TIMESCALE_AUTOVACUUM_NAPTIME", "autovacuum_naptime", "duration"),
    SettingSpec("TIMESCALE_AUTOVACUUM_VACUUM_COST_LIMIT", "autovacuum_vacuum_cost_limit", "int"),
    SettingSpec("TIMESCALE_AUTOVACUUM_VACUUM_COST_DELAY", "autovacuum_vacuum_cost_delay", "duration"),
    SettingSpec("TIMESCALE_RANDOM_PAGE_COST", "random_page_cost", "float"),
    SettingSpec("TIMESCALE_EFFECTIVE_IO_CONCURRENCY", "effective_io_concurrency", "int"),
    SettingSpec("TIMESCALE_MAINTENANCE_IO_CONCURRENCY", "maintenance_io_concurrency", "int"),
    SettingSpec("TIMESCALE_ARCHIVE_MODE", "archive_mode", "bool"),
    SettingSpec("TIMESCALE_ARCHIVE_COMMAND", "archive_command", "archive_command"),
    SettingSpec("TIMESCALE_ARCHIVE_TIMEOUT", "archive_timeout", "duration"),
)

VALIDATION_ENV_KEYS = (
    "TIMESCALE_MEMORY_LIMIT",
    "TIMESCALE_MEM_LIMIT",
    "TIMESCALE_CPUS",
    "TIMESCALE_HOST_MEMORY_RESERVE",
    "TRADING_RESOURCE_HOST_MEMORY",
    "TRADING_RESOURCE_MIN_HEADROOM_MEMORY",
    "TIMESCALE_WAL_DISK_BUDGET",
    "TIMESCALE_WORK_MEM_ACTIVE_CONNECTIONS",
    "TIMESCALE_WORK_MEM_NODE_FACTOR",
)


def env_truthy(env: Mapping[str, str] | None, name: str, default: bool = False) -> bool:
    raw = (env or os.environ).get(str(name))
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def parse_size_bytes(raw: Any) -> int:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty size")
    match = _SIZE_RE.match(text)
    if not match:
        raise ValueError(f"invalid size {text!r}")
    value = float(match.group("value"))
    unit = (match.group("unit") or "b").lower()
    multiplier = {
        "b": 1,
        "byte": 1,
        "bytes": 1,
        "k": BYTES_IN_KIB,
        "kb": BYTES_IN_KIB,
        "kib": BYTES_IN_KIB,
        "m": BYTES_IN_MIB,
        "mb": BYTES_IN_MIB,
        "mib": BYTES_IN_MIB,
        "g": BYTES_IN_GIB,
        "gb": BYTES_IN_GIB,
        "gib": BYTES_IN_GIB,
        "t": BYTES_IN_TIB,
        "tb": BYTES_IN_TIB,
        "tib": BYTES_IN_TIB,
    }[unit]
    return int(value * multiplier)


def parse_duration_seconds(raw: Any) -> float:
    text = str(raw or "").strip()
    if not text:
        raise ValueError("empty duration")
    match = _DURATION_RE.match(text)
    if not match:
        raise ValueError(f"invalid duration {text!r}")
    value = float(match.group("value"))
    unit = (match.group("unit") or "s").lower()
    if unit == "ms":
        return value / 1000.0
    if unit.startswith("min"):
        return value * 60.0
    if unit in {"h", "hr", "hour", "hours"}:
        return value * 3600.0
    return value


def format_bytes(value: int | float | None) -> str:
    if value is None:
        return "unknown"
    amount = float(value)
    for suffix, scale in (("TiB", BYTES_IN_TIB), ("GiB", BYTES_IN_GIB), ("MiB", BYTES_IN_MIB)):
        if amount >= scale:
            rendered = amount / scale
            return f"{rendered:.1f}{suffix}" if rendered % 1 else f"{int(rendered)}{suffix}"
    return f"{int(amount)}B"


def _proc_mem_total_bytes() -> int | None:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                return int(parts[1]) * BYTES_IN_KIB
    except Exception:
        return None
    return None


def _cgroup_memory_limit_bytes() -> int | None:
    candidates = (
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    )
    for path in candidates:
        try:
            raw = path.read_text(encoding="utf-8").strip()
        except Exception:
            continue
        if not raw or raw == "max":
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        if value > 0 and value < 2**60:
            return value
    return None


def _host_memory_bytes(env: Mapping[str, str]) -> int | None:
    override = str(
        env.get("PREFLIGHT_HOST_MEMORY_BYTES")
        or env.get("TIMESCALE_HOST_MEMORY_BYTES")
        or env.get("TRADING_RESOURCE_HOST_MEMORY")
        or ""
    ).strip()
    if override:
        return parse_size_bytes(override)
    return _proc_mem_total_bytes()


def _configured_memory_limit_bytes(env: Mapping[str, str]) -> tuple[int | None, str]:
    raw = str(env.get("TIMESCALE_MEMORY_LIMIT") or env.get("TIMESCALE_MEM_LIMIT") or "").strip()
    if raw:
        source = "TIMESCALE_MEMORY_LIMIT" if str(env.get("TIMESCALE_MEMORY_LIMIT") or "").strip() else "TIMESCALE_MEM_LIMIT"
        return parse_size_bytes(raw), source

    host = _host_memory_bytes(env)
    cgroup = _cgroup_memory_limit_bytes()
    if cgroup and (not host or cgroup < host):
        return cgroup, "runtime_cgroup_limit"
    if host:
        return host, "host_memtotal"
    return None, "unknown"


def _setting_value(spec: SettingSpec, raw: str) -> Any:
    if spec.kind == "bytes":
        return parse_size_bytes(raw)
    if spec.kind == "duration":
        return parse_duration_seconds(raw)
    if spec.kind == "int":
        return int(str(raw).strip())
    if spec.kind == "float":
        return float(str(raw).strip())
    if spec.kind == "bool":
        value = str(raw).strip().lower()
        if value in {"1", "true", "yes", "on"}:
            return True
        if value in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"invalid boolean {raw!r}")
    return str(raw)


def archive_command_uses_audited_script(command: Any) -> bool:
    text = str(command or "").strip()
    if not text:
        return False
    required_fragment = str(os.environ.get("TIMESCALE_ARCHIVE_COMMAND_REQUIRED_FRAGMENT") or "wal_archive.sh").strip()
    if required_fragment and required_fragment not in text:
        return False
    if "%p" not in text or "%f" not in text:
        return False
    padded = f" {text} "
    inline_fragments = (" cp %p ", " cp \"%p\" ", "mkdir -p ", "test ! -f ")
    return not any(fragment in padded for fragment in inline_fragments)


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, int(value)))


def derive_recommended_settings(memory_limit_bytes: int, cpus: int, max_connections: int = 100) -> dict[str, Any]:
    """Return conservative recommendations for an ingest-heavy TimescaleDB."""

    cpu_count = max(1, int(cpus or 1))
    connections = max(1, int(max_connections or 100))
    shared_buffers = _clamp(memory_limit_bytes // 4, 1 * BYTES_IN_GIB, 32 * BYTES_IN_GIB)
    effective_cache_size = int(memory_limit_bytes * 0.75)
    work_mem = _clamp(int((memory_limit_bytes * 0.05) / connections), 4 * BYTES_IN_MIB, 64 * BYTES_IN_MIB)
    maintenance_work_mem = _clamp(memory_limit_bytes // 32, 256 * BYTES_IN_MIB, 2 * BYTES_IN_GIB)
    autovacuum_work_mem = _clamp(memory_limit_bytes // 128, 128 * BYTES_IN_MIB, 1 * BYTES_IN_GIB)
    max_parallel_maintenance_workers = _clamp(cpu_count // 2, 2, 4)
    autovacuum_max_workers = _clamp(cpu_count // 2, 3, 8)
    max_wal_size = _clamp(memory_limit_bytes // 4, 8 * BYTES_IN_GIB, 32 * BYTES_IN_GIB)

    return {
        "max_connections": connections,
        "shared_buffers": shared_buffers,
        "effective_cache_size": effective_cache_size,
        "work_mem": work_mem,
        "maintenance_work_mem": maintenance_work_mem,
        "autovacuum_work_mem": autovacuum_work_mem,
        "wal_buffers": 64 * BYTES_IN_MIB if memory_limit_bytes >= 16 * BYTES_IN_GIB else 16 * BYTES_IN_MIB,
        "min_wal_size": max(2 * BYTES_IN_GIB, max_wal_size // 4),
        "max_wal_size": max_wal_size,
        "wal_keep_size": 1 * BYTES_IN_GIB,
        "max_slot_wal_keep_size": min(8 * BYTES_IN_GIB, max_wal_size // 2),
        "checkpoint_timeout": 900.0,
        "checkpoint_completion_target": 0.9,
        "max_worker_processes": max(8, cpu_count * 2),
        "max_parallel_workers": max(2, cpu_count),
        "max_parallel_workers_per_gather": _clamp(cpu_count // 2, 1, 4),
        "max_parallel_maintenance_workers": max_parallel_maintenance_workers,
        "timescaledb.max_background_workers": max(4, min(16, cpu_count)),
        "autovacuum": True,
        "autovacuum_max_workers": autovacuum_max_workers,
        "autovacuum_naptime": 10.0,
        "autovacuum_vacuum_cost_limit": 4000,
        "autovacuum_vacuum_cost_delay": 0.002,
        "random_page_cost": 1.1,
        "effective_io_concurrency": 200,
        "maintenance_io_concurrency": 200,
        "archive_mode": True,
        "archive_timeout": 60.0,
    }


def _pg_unit_multiplier(unit: str) -> float:
    match = _PG_UNIT_RE.match(str(unit or ""))
    if not match:
        return 1.0
    count = float(match.group("count") or 1.0)
    suffix = str(match.group("unit") or "").lower()
    if suffix in {"b", "byte", "bytes"}:
        return count
    if suffix == "kb":
        return count * BYTES_IN_KIB
    if suffix == "mb":
        return count * BYTES_IN_MIB
    if suffix == "gb":
        return count * BYTES_IN_GIB
    if suffix == "tb":
        return count * BYTES_IN_TIB
    if suffix == "ms":
        return count / 1000.0
    if suffix == "s":
        return count
    if suffix == "min":
        return count * 60.0
    if suffix == "h":
        return count * 3600.0
    return count


def normalize_pg_setting_value(setting: Any, unit: Any, kind: str) -> Any:
    text = str(setting if setting is not None else "").strip()
    if kind == "bytes":
        try:
            return int(float(text) * _pg_unit_multiplier(str(unit or "")))
        except Exception:
            return parse_size_bytes(text)
    if kind == "duration":
        try:
            return float(text) * _pg_unit_multiplier(str(unit or "s"))
        except Exception:
            return parse_duration_seconds(text)
    if kind == "int":
        return int(float(text))
    if kind == "float":
        return float(text)
    if kind == "bool":
        return text.lower() in {"on", "1", "true", "yes"}
    return text


def _values_match(expected: Any, actual: Any, kind: str) -> bool:
    if kind in {"bytes", "int"}:
        return int(expected) == int(actual)
    if kind in {"duration", "float"}:
        return math.isclose(float(expected), float(actual), rel_tol=0.0001, abs_tol=0.001)
    if kind == "bool":
        return bool(expected) == bool(actual)
    return str(expected) == str(actual)


def docker_postgres_tuning_snapshot(
    env: Mapping[str, str] | None = None,
    *,
    required: bool = False,
    effective_settings: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    source_env = {str(k): str(v) for k, v in (env or os.environ).items()}
    errors: list[str] = []
    warnings: list[str] = []
    configured: dict[str, Any] = {}
    raw_settings: dict[str, str] = {}

    for spec in PG_SETTING_SPECS:
        raw = str(source_env.get(spec.env) or "").strip()
        if not raw:
            if required and spec.required:
                errors.append(f"postgres tuning missing {spec.env}")
            continue
        raw_settings[spec.pg_name] = raw
        try:
            configured[spec.pg_name] = _setting_value(spec, raw)
        except Exception as exc:
            errors.append(f"postgres tuning invalid {spec.env}={raw!r}: {type(exc).__name__}: {exc}")

    memory_limit_bytes: int | None = None
    host_memory_bytes: int | None = None
    try:
        memory_limit_bytes, memory_source = _configured_memory_limit_bytes(source_env)
    except Exception as exc:
        memory_source = "TIMESCALE_MEM_LIMIT"
        errors.append(f"postgres tuning invalid Timescale memory limit: {type(exc).__name__}: {exc}")

    try:
        host_memory_bytes = _host_memory_bytes(source_env)
    except Exception as exc:
        errors.append(f"postgres tuning invalid host memory override: {type(exc).__name__}: {exc}")

    cpus_raw = str(source_env.get("TIMESCALE_CPUS") or "").strip()
    try:
        cpus = max(1, int(float(cpus_raw))) if cpus_raw else (os.cpu_count() or 1)
    except Exception as exc:
        cpus = os.cpu_count() or 1
        errors.append(f"postgres tuning invalid TIMESCALE_CPUS={cpus_raw!r}: {type(exc).__name__}: {exc}")

    if required and not str(source_env.get("TIMESCALE_MEMORY_LIMIT") or source_env.get("TIMESCALE_MEM_LIMIT") or "").strip():
        errors.append("postgres tuning missing TIMESCALE_MEM_LIMIT")
    if required and not cpus_raw:
        errors.append("postgres tuning missing TIMESCALE_CPUS")

    reserve_raw = str(
        source_env.get("TIMESCALE_HOST_MEMORY_RESERVE")
        or source_env.get("TRADING_RESOURCE_MIN_HEADROOM_MEMORY")
        or ""
    ).strip()
    host_reserve_bytes = 0
    if host_memory_bytes:
        host_reserve_bytes = max(8 * BYTES_IN_GIB, int(host_memory_bytes * 0.10))
    if reserve_raw:
        try:
            host_reserve_bytes = parse_size_bytes(reserve_raw)
        except Exception as exc:
            errors.append(f"postgres tuning invalid TIMESCALE_HOST_MEMORY_RESERVE={reserve_raw!r}: {type(exc).__name__}: {exc}")

    max_connections = int(configured.get("max_connections") or 100)
    recommended = (
        derive_recommended_settings(memory_limit_bytes, cpus, max_connections)
        if memory_limit_bytes
        else {}
    )

    memory_budget: dict[str, Any] = {}
    wal_budget: dict[str, Any] = {}
    if memory_limit_bytes:
        if host_memory_bytes and memory_limit_bytes + host_reserve_bytes > host_memory_bytes:
            errors.append(
                "postgres tuning exceeds host headroom: "
                f"timescale_limit={format_bytes(memory_limit_bytes)} "
                f"reserve={format_bytes(host_reserve_bytes)} "
                f"host={format_bytes(host_memory_bytes)}"
            )

        shared_buffers = int(configured.get("shared_buffers") or 0)
        effective_cache_size = int(configured.get("effective_cache_size") or 0)
        work_mem = int(configured.get("work_mem") or 0)
        maintenance_work_mem = int(configured.get("maintenance_work_mem") or 0)
        autovacuum_work_mem = int(configured.get("autovacuum_work_mem") or 0)
        autovacuum_workers = int(configured.get("autovacuum_max_workers") or 0)
        maintenance_workers = int(configured.get("max_parallel_maintenance_workers") or 0)

        active_connections_raw = str(source_env.get("TIMESCALE_WORK_MEM_ACTIVE_CONNECTIONS") or "").strip()
        node_factor_raw = str(source_env.get("TIMESCALE_WORK_MEM_NODE_FACTOR") or "").strip()
        try:
            active_connections = int(active_connections_raw) if active_connections_raw else min(max_connections, 64)
        except Exception as exc:
            active_connections = min(max_connections, 64)
            errors.append(
                "postgres tuning invalid TIMESCALE_WORK_MEM_ACTIVE_CONNECTIONS="
                f"{active_connections_raw!r}: {type(exc).__name__}: {exc}"
            )
        try:
            node_factor = float(node_factor_raw) if node_factor_raw else 2.0
        except Exception as exc:
            node_factor = 2.0
            errors.append(f"postgres tuning invalid TIMESCALE_WORK_MEM_NODE_FACTOR={node_factor_raw!r}: {type(exc).__name__}: {exc}")

        fixed_overhead = max(1 * BYTES_IN_GIB, int(memory_limit_bytes * 0.03))
        work_mem_budget = int(work_mem * active_connections * node_factor)
        maintenance_budget = maintenance_work_mem * max(1, maintenance_workers)
        autovacuum_budget = autovacuum_work_mem * max(1, autovacuum_workers)
        estimated_peak = shared_buffers + work_mem_budget + maintenance_budget + autovacuum_budget + fixed_overhead
        allowed_peak = int(memory_limit_bytes * 0.85)
        memory_budget = {
            "memory_limit_bytes": memory_limit_bytes,
            "shared_buffers_bytes": shared_buffers,
            "work_mem_budget_bytes": work_mem_budget,
            "maintenance_budget_bytes": maintenance_budget,
            "autovacuum_budget_bytes": autovacuum_budget,
            "fixed_overhead_bytes": fixed_overhead,
            "estimated_peak_bytes": estimated_peak,
            "allowed_peak_bytes": allowed_peak,
            "active_connections": active_connections,
            "work_mem_node_factor": node_factor,
        }

        if shared_buffers > int(memory_limit_bytes * 0.40):
            errors.append(
                "postgres tuning shared_buffers too large for service limit: "
                f"shared_buffers={format_bytes(shared_buffers)} limit={format_bytes(memory_limit_bytes)}"
            )
        if effective_cache_size > memory_limit_bytes:
            errors.append(
                "postgres tuning effective_cache_size exceeds service memory limit: "
                f"effective_cache_size={format_bytes(effective_cache_size)} limit={format_bytes(memory_limit_bytes)}"
            )
        if maintenance_work_mem > int(memory_limit_bytes * 0.10):
            errors.append(
                "postgres tuning maintenance_work_mem too large for service limit: "
                f"maintenance_work_mem={format_bytes(maintenance_work_mem)} limit={format_bytes(memory_limit_bytes)}"
            )
        if estimated_peak > allowed_peak:
            errors.append(
                "postgres tuning memory budget exceeds 85% of service limit: "
                f"estimated={format_bytes(estimated_peak)} allowed={format_bytes(allowed_peak)}"
            )

    wal_budget_raw = str(source_env.get("TIMESCALE_WAL_DISK_BUDGET") or "").strip()
    wal_disk_budget_bytes = 0
    if wal_budget_raw:
        try:
            wal_disk_budget_bytes = parse_size_bytes(wal_budget_raw)
        except Exception as exc:
            errors.append(f"postgres tuning invalid TIMESCALE_WAL_DISK_BUDGET={wal_budget_raw!r}: {type(exc).__name__}: {exc}")
    elif required:
        errors.append("postgres tuning missing TIMESCALE_WAL_DISK_BUDGET")

    max_wal_size = int(configured.get("max_wal_size") or 0)
    min_wal_size = int(configured.get("min_wal_size") or 0)
    wal_keep_size = int(configured.get("wal_keep_size") or 0)
    max_slot_wal_keep_size = int(configured.get("max_slot_wal_keep_size") or 0)
    if min_wal_size and max_wal_size and min_wal_size > max_wal_size:
        errors.append("postgres tuning min_wal_size exceeds max_wal_size")
    if max_wal_size and max_wal_size < 8 * BYTES_IN_GIB:
        errors.append(
            "postgres tuning max_wal_size leaves too little ingestion checkpoint headroom: "
            f"max_wal_size={format_bytes(max_wal_size)} minimum=8GiB"
        )
    if max_slot_wal_keep_size <= 0 and required:
        errors.append("postgres tuning max_slot_wal_keep_size must be finite and positive")
    if wal_disk_budget_bytes:
        retained_wal_ceiling = max_wal_size + wal_keep_size + max_slot_wal_keep_size
        wal_budget = {
            "wal_disk_budget_bytes": wal_disk_budget_bytes,
            "max_wal_size_bytes": max_wal_size,
            "wal_keep_size_bytes": wal_keep_size,
            "max_slot_wal_keep_size_bytes": max_slot_wal_keep_size,
            "configured_retained_wal_ceiling_bytes": retained_wal_ceiling,
        }
        if retained_wal_ceiling > wal_disk_budget_bytes:
            errors.append(
                "postgres tuning WAL retention budget exceeds configured disk budget: "
                f"retention={format_bytes(retained_wal_ceiling)} budget={format_bytes(wal_disk_budget_bytes)}"
            )

    checkpoint_timeout = float(configured.get("checkpoint_timeout") or 0.0)
    checkpoint_target = float(configured.get("checkpoint_completion_target") or 0.0)
    if checkpoint_timeout and checkpoint_timeout < 300:
        errors.append("postgres tuning checkpoint_timeout must be at least 300s for ingestion headroom")
    if checkpoint_target and checkpoint_target < 0.8:
        errors.append("postgres tuning checkpoint_completion_target must be at least 0.8")
    if configured.get("archive_mode") is False and required:
        errors.append("postgres tuning archive_mode must be on")
    archive_command = str(configured.get("archive_command") or "").strip()
    if required and not archive_command:
        errors.append("postgres tuning archive_command must invoke wal_archive.sh")
    elif archive_command and not archive_command_uses_audited_script(archive_command):
        errors.append("postgres tuning archive_command must invoke audited wal_archive.sh with %p and %f")
    archive_timeout = float(configured.get("archive_timeout") or 0.0)
    if archive_timeout and archive_timeout > 120:
        warnings.append("postgres tuning archive_timeout above 120s weakens WAL RPO")

    if configured.get("autovacuum") is False and required:
        errors.append("postgres tuning autovacuum must be on")
    if int(configured.get("autovacuum_max_workers") or 0) < 3 and required:
        errors.append("postgres tuning autovacuum_max_workers must be at least 3")
    if int(configured.get("effective_io_concurrency") or 0) < 1 and required:
        errors.append("postgres tuning effective_io_concurrency must be positive")
    if float(configured.get("random_page_cost") or 0.0) <= 0 and required:
        errors.append("postgres tuning random_page_cost must be positive")

    mismatches: list[dict[str, Any]] = []
    effective_normalized: dict[str, Any] = {}
    if effective_settings:
        by_name = {str(k): dict(v or {}) for k, v in effective_settings.items()}
        for spec in PG_SETTING_SPECS:
            if spec.pg_name not in configured:
                continue
            actual_row = by_name.get(spec.pg_name)
            if not actual_row:
                mismatches.append({"name": spec.pg_name, "reason": "missing_pg_settings_row"})
                continue
            try:
                actual = normalize_pg_setting_value(actual_row.get("setting"), actual_row.get("unit"), spec.kind)
                effective_normalized[spec.pg_name] = actual
                if not _values_match(configured[spec.pg_name], actual, spec.kind):
                    mismatches.append(
                        {
                            "name": spec.pg_name,
                            "expected": configured[spec.pg_name],
                            "actual": actual,
                            "unit": actual_row.get("unit"),
                        }
                    )
            except Exception as exc:
                mismatches.append({"name": spec.pg_name, "reason": f"normalization_failed:{type(exc).__name__}:{exc}"})
        for mismatch in mismatches:
            errors.append(
                "postgres effective setting mismatch: "
                f"{mismatch.get('name')} expected={mismatch.get('expected', 'configured')} "
                f"actual={mismatch.get('actual', mismatch.get('reason', 'unknown'))}"
            )

    return {
        "ok": not errors,
        "required": bool(required),
        "errors": errors,
        "warnings": warnings,
        "raw_settings": raw_settings,
        "configured": configured,
        "recommended": recommended,
        "derivation": {
            "memory_source": memory_source,
            "memory_limit_bytes": memory_limit_bytes,
            "memory_limit_human": format_bytes(memory_limit_bytes),
            "host_memory_bytes": host_memory_bytes,
            "host_memory_human": format_bytes(host_memory_bytes),
            "host_reserve_bytes": host_reserve_bytes,
            "host_reserve_human": format_bytes(host_reserve_bytes),
            "cpus": cpus,
        },
        "memory_budget": memory_budget,
        "wal_budget": wal_budget,
        "effective_settings": effective_settings or {},
        "effective_normalized": effective_normalized,
        "effective_mismatches": mismatches,
    }
