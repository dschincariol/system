from __future__ import annotations

"""Read-only host memory pressure policy checks for production readiness."""

import argparse
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any, Callable, Mapping


BYTES_IN_GIB = 1024**3
_PRODUCTION_VALUES = {"prod", "production", "live"}
_TRUTHY = {"1", "true", "yes", "on", "y"}
_FALSY = {"0", "false", "no", "off", "n"}
_BYTE_UNITS = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024**2,
    "mb": 1024**2,
    "mib": 1024**2,
    "g": BYTES_IN_GIB,
    "gb": BYTES_IN_GIB,
    "gib": BYTES_IN_GIB,
    "t": 1024**4,
    "tb": 1024**4,
    "tib": 1024**4,
}
_SERVICE_MEMORY_KEYS = (
    ("runtime", "RUNTIME_MEM_LIMIT"),
    ("timescaledb", "TIMESCALE_MEM_LIMIT"),
    ("redis", "REDIS_MEM_LIMIT"),
    ("minio", "MINIO_MEM_LIMIT"),
    ("operator", "OPERATOR_MEM_LIMIT"),
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in _TRUTHY


def _falsey(value: Any) -> bool:
    return _clean(value).lower() in _FALSY


def _production_like(env: Mapping[str, str]) -> bool:
    for name in ("PROD_LOCK", "ENGINE_SUPERVISED"):
        if _truthy(env.get(name)):
            return True
    for name in ("ENV", "APP_ENV", "TS_ENV", "ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
        if _clean(env.get(name)).lower() in _PRODUCTION_VALUES:
            return True
    return False


def _required(env: Mapping[str, str], required: bool | None) -> bool:
    if required is not None:
        return bool(required)
    raw = env.get("PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY")
    if raw is not None and _clean(raw):
        return _truthy(raw)
    return _production_like(env)


def _parse_bytes(value: Any) -> int | None:
    text = _clean(value).lower().replace(" ", "")
    if text in {"", "none", "unbounded", "unlimited"}:
        return None
    match = re.fullmatch(r"(?P<num>\d+(?:\.\d+)?)(?P<unit>[a-z]*)", text)
    if not match:
        return None
    multiplier = _BYTE_UNITS.get(match.group("unit"))
    if multiplier is None:
        return None
    parsed = int(float(match.group("num")) * multiplier)
    return parsed if parsed >= 0 else None


def _env_gib(env: Mapping[str, str], name: str, default: int) -> int:
    raw = _clean(env.get(name))
    if not raw:
        return int(default)
    parsed = _parse_bytes(raw if re.search(r"[a-zA-Z]", raw) else f"{raw}g")
    if parsed is None or parsed <= 0:
        return int(default)
    return max(1, int(round(float(parsed) / float(BYTES_IN_GIB))))


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = _clean(env.get(name))
    if not raw:
        return int(default)
    try:
        parsed = int(float(raw))
    except Exception:
        return int(default)
    return parsed


def _gib(value: int | None) -> float | None:
    if value is None:
        return None
    return round(float(value) / float(BYTES_IN_GIB), 2)


def _policy(env: Mapping[str, str]) -> dict[str, Any]:
    zram_gib = _env_gib(env, "TRADING_ZRAM_SIZE_GIB", 32)
    swapfile_gib = _env_gib(env, "TRADING_SWAPFILE_SIZE_GIB", 16)
    total_swap_default = zram_gib + swapfile_gib
    return {
        "min_host_ram_bytes": _env_gib(env, "TRADING_MEMORY_MIN_HOST_RAM_GIB", 120) * BYTES_IN_GIB,
        "swappiness": _env_int(env, "TRADING_SWAPPINESS", 10),
        "min_zram_bytes": zram_gib * BYTES_IN_GIB,
        "zram_priority": _env_int(env, "TRADING_ZRAM_PRIORITY", 100),
        "min_swapfile_bytes": swapfile_gib * BYTES_IN_GIB,
        "swapfile_priority": _env_int(env, "TRADING_SWAPFILE_PRIORITY", 10),
        "swapfile_path": _clean(env.get("TRADING_SWAPFILE_PATH")) or "/swapfile-trading",
        "zfs_arc_max_bytes": _env_gib(env, "TRADING_ZFS_ARC_MAX_GIB", 48) * BYTES_IN_GIB,
        "min_total_swap_bytes": _env_gib(env, "TRADING_MEMORY_MIN_TOTAL_SWAP_GIB", total_swap_default)
        * BYTES_IN_GIB,
        "min_headroom_bytes": _parse_bytes(env.get("TRADING_RESOURCE_MIN_HEADROOM_MEMORY") or "24g"),
    }


def _read_text(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def _read_meminfo(meminfo_text: str | None = None) -> dict[str, int]:
    text = meminfo_text if meminfo_text is not None else _read_text("/proc/meminfo")
    values: dict[str, int] = {}
    for line in str(text or "").splitlines():
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        parts = raw.strip().split()
        if not parts:
            continue
        try:
            number = int(float(parts[0]))
        except Exception:
            continue
        unit = parts[1].lower() if len(parts) > 1 else ""
        values[key] = number * (1024 if unit == "kb" else 1)
    return values


def _run_swapon(timeout_s: float = 1.0) -> tuple[str, str]:
    try:
        proc = subprocess.run(
            ["swapon", "--noheadings", "--bytes", "--show=NAME,TYPE,SIZE,USED,PRIO"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=max(0.1, float(timeout_s)),
            check=False,
        )
    except Exception as exc:
        return "", f"{type(exc).__name__}: {exc}"
    if int(proc.returncode) != 0:
        return "", (proc.stderr or proc.stdout or f"rc={proc.returncode}").strip()[:240]
    return proc.stdout or "", ""


def _read_proc_swaps(proc_swaps_text: str | None = None) -> str:
    text = proc_swaps_text if proc_swaps_text is not None else _read_text("/proc/swaps")
    rows: list[str] = []
    for line in str(text or "").splitlines()[1:]:
        parts = line.split()
        if len(parts) < 5:
            continue
        name, swap_type, size_kib, used_kib, prio = parts[:5]
        try:
            rows.append(f"{name} {swap_type} {int(size_kib) * 1024} {int(used_kib) * 1024} {prio}")
        except Exception:
            continue
    return "\n".join(rows)


def _parse_swapon(text: str) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    for line in str(text or "").splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        name, swap_type, size, used, prio = parts[:5]
        size_bytes = _parse_bytes(size)
        used_bytes = _parse_bytes(used)
        try:
            priority = int(float(prio))
        except Exception:
            priority = None
        devices.append(
            {
                "name": name,
                "type": swap_type,
                "size_bytes": size_bytes,
                "size_gib": _gib(size_bytes),
                "used_bytes": used_bytes,
                "used_gib": _gib(used_bytes),
                "priority": priority,
            }
        )
    return devices


def _swap_devices(
    *,
    swapon_text: str | None = None,
    proc_swaps_text: str | None = None,
    swapon_runner: Callable[[], tuple[str, str]] | None = None,
) -> tuple[list[dict[str, Any]], str, str]:
    if swapon_text is not None:
        return _parse_swapon(swapon_text), "provided", ""
    text, error = swapon_runner() if swapon_runner is not None else _run_swapon()
    source = "swapon"
    if not text:
        text = _read_proc_swaps(proc_swaps_text)
        source = "proc_swaps"
    return _parse_swapon(text), source, error


def _read_int_text(text: str | None, path: str) -> int | None:
    raw = text if text is not None else _read_text(path)
    raw = _clean(raw)
    if not raw:
        return None
    try:
        return int(float(raw))
    except Exception:
        return None


def _read_arcstats(arcstats_text: str | None = None) -> dict[str, int]:
    text = arcstats_text if arcstats_text is not None else _read_text("/proc/spl/kstat/zfs/arcstats")
    values: dict[str, int] = {}
    for line in str(text or "").splitlines():
        parts = line.split()
        if len(parts) != 3 or parts[0] in {"name", "data"}:
            continue
        try:
            values[parts[0]] = int(float(parts[2]))
        except Exception:
            continue
    return values


def _resource_memory_snapshot(env: Mapping[str, str], mem_total_bytes: int | None, policy: Mapping[str, Any]) -> dict[str, Any]:
    services: dict[str, Any] = {}
    total = 0
    bounded = True
    for service, env_name in _SERVICE_MEMORY_KEYS:
        parsed = _parse_bytes(env.get(env_name))
        services[service] = {"env": env_name, "limit_bytes": parsed, "limit_gib": _gib(parsed)}
        if parsed is None or parsed <= 0:
            bounded = False
        else:
            total += int(parsed)
    headroom = None if mem_total_bytes is None or not bounded else int(mem_total_bytes) - int(total)
    return {
        "services": services,
        "all_bounded": bool(bounded),
        "total_container_limit_bytes": total if bounded else None,
        "total_container_limit_gib": _gib(total if bounded else None),
        "min_headroom_bytes": policy.get("min_headroom_bytes"),
        "min_headroom_gib": _gib(policy.get("min_headroom_bytes")),
        "headroom_after_container_limits_bytes": headroom,
        "headroom_after_container_limits_gib": _gib(headroom),
    }


def host_memory_pressure_snapshot(
    env: Mapping[str, str] | None = None,
    *,
    required: bool | None = None,
    meminfo_text: str | None = None,
    swapon_text: str | None = None,
    proc_swaps_text: str | None = None,
    swappiness_text: str | None = None,
    zfs_arc_max_text: str | None = None,
    arcstats_text: str | None = None,
    swapon_runner: Callable[[], tuple[str, str]] | None = None,
) -> dict[str, Any]:
    source_env: Mapping[str, str] = env if env is not None else os.environ
    required_flag = _required(source_env, required)
    policy = _policy(source_env)
    meminfo = _read_meminfo(meminfo_text)
    mem_total = meminfo.get("MemTotal")
    mem_available = meminfo.get("MemAvailable")
    swap_total = meminfo.get("SwapTotal")
    swap_free = meminfo.get("SwapFree")
    devices, swap_source, swap_error = _swap_devices(
        swapon_text=swapon_text,
        proc_swaps_text=proc_swaps_text,
        swapon_runner=swapon_runner,
    )
    zram_devices = [item for item in devices if str(item.get("name") or "").startswith("/dev/zram")]
    zram_bytes = sum(int(item.get("size_bytes") or 0) for item in zram_devices)
    zram_priority = max([int(item.get("priority")) for item in zram_devices if item.get("priority") is not None] or [None])
    swapfile_path = str(policy["swapfile_path"])
    managed_swapfiles = [item for item in devices if str(item.get("name") or "") == swapfile_path]
    managed_swapfile_bytes = sum(int(item.get("size_bytes") or 0) for item in managed_swapfiles)
    managed_swapfile_priority = max(
        [int(item.get("priority")) for item in managed_swapfiles if item.get("priority") is not None] or [None]
    )
    swappiness = _read_int_text(swappiness_text, "/proc/sys/vm/swappiness")
    zfs_arc_max = _read_int_text(zfs_arc_max_text, "/sys/module/zfs/parameters/zfs_arc_max")
    arcstats = _read_arcstats(arcstats_text)
    resource_memory = _resource_memory_snapshot(source_env, mem_total, policy)

    violations: list[str] = []
    advisory: list[str] = []
    if mem_total is None:
        violations.append("memory_pressure_memtotal_unavailable")
    elif int(mem_total) < int(policy["min_host_ram_bytes"]):
        violations.append("memory_pressure_host_ram_below_policy")
    if swap_total is None:
        violations.append("memory_pressure_swaptotal_unavailable")
    elif int(swap_total) < int(policy["min_total_swap_bytes"]):
        violations.append("memory_pressure_total_swap_below_policy")
    if swap_error:
        advisory.append(f"memory_pressure_swapon_unavailable:{swap_error}")
    if zram_bytes < int(policy["min_zram_bytes"]):
        violations.append("memory_pressure_zram_below_policy")
    if zram_priority is None or int(zram_priority) < int(policy["zram_priority"]):
        violations.append("memory_pressure_zram_priority_below_policy")
    if managed_swapfile_bytes < int(policy["min_swapfile_bytes"]):
        violations.append("memory_pressure_swapfile_below_policy")
    if managed_swapfile_priority is None or int(managed_swapfile_priority) < int(policy["swapfile_priority"]):
        violations.append("memory_pressure_swapfile_priority_below_policy")
    if swappiness is None:
        violations.append("memory_pressure_swappiness_unavailable")
    elif int(swappiness) != int(policy["swappiness"]):
        violations.append("memory_pressure_swappiness_mismatch")
    if zfs_arc_max is None:
        violations.append("memory_pressure_zfs_arc_max_unavailable")
    elif int(zfs_arc_max) != int(policy["zfs_arc_max_bytes"]):
        violations.append("memory_pressure_zfs_arc_max_mismatch")

    min_headroom = policy.get("min_headroom_bytes")
    headroom = resource_memory.get("headroom_after_container_limits_bytes")
    if min_headroom is not None and headroom is not None and int(headroom) < int(min_headroom):
        violations.append("memory_pressure_container_headroom_below_policy")
    elif not bool(resource_memory.get("all_bounded")):
        advisory.append("memory_pressure_container_memory_limits_incomplete")

    errors = list(violations) if required_flag else []
    warnings = list(advisory)
    if not required_flag:
        warnings.extend(violations)
    meets_policy = not violations
    status = "pass" if meets_policy else ("fail" if required_flag else "warn")
    return {
        "ok": bool(not errors),
        "meets_policy": bool(meets_policy),
        "required": bool(required_flag),
        "production_like": _production_like(source_env),
        "status": status,
        "severity": "P1" if status == "fail" else ("P2" if status == "warn" else "OK"),
        "reason": "ok" if meets_policy else violations[0],
        "errors": errors,
        "warnings": warnings,
        "policy": {
            **policy,
            "min_host_ram_gib": _gib(policy.get("min_host_ram_bytes")),
            "min_total_swap_gib": _gib(policy.get("min_total_swap_bytes")),
            "min_zram_gib": _gib(policy.get("min_zram_bytes")),
            "min_swapfile_gib": _gib(policy.get("min_swapfile_bytes")),
            "zfs_arc_max_gib": _gib(policy.get("zfs_arc_max_bytes")),
        },
        "memory": {
            "mem_total_bytes": mem_total,
            "mem_total_gib": _gib(mem_total),
            "mem_available_bytes": mem_available,
            "mem_available_gib": _gib(mem_available),
            "swap_total_bytes": swap_total,
            "swap_total_gib": _gib(swap_total),
            "swap_free_bytes": swap_free,
            "swap_free_gib": _gib(swap_free),
        },
        "swap": {
            "source": swap_source,
            "devices": devices,
            "zram_total_bytes": zram_bytes,
            "zram_total_gib": _gib(zram_bytes),
            "zram_priority": zram_priority,
            "managed_swapfile_path": swapfile_path,
            "managed_swapfile_bytes": managed_swapfile_bytes,
            "managed_swapfile_gib": _gib(managed_swapfile_bytes),
            "managed_swapfile_priority": managed_swapfile_priority,
        },
        "vm": {"swappiness": swappiness},
        "zfs": {
            "arc_max_bytes": zfs_arc_max,
            "arc_max_gib": _gib(zfs_arc_max),
            "arc_size_bytes": arcstats.get("size"),
            "arc_size_gib": _gib(arcstats.get("size")),
            "arc_c_max_bytes": arcstats.get("c_max"),
            "arc_c_max_gib": _gib(arcstats.get("c_max")),
        },
        "resource_memory": resource_memory,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate host memory pressure policy.")
    parser.add_argument("--json", action="store_true", help="Print the full memory pressure snapshot as JSON.")
    parser.add_argument("--required", action="store_true", help="Treat policy drift as a failing check.")
    args = parser.parse_args(argv)

    state = host_memory_pressure_snapshot(required=True if args.required else None)
    if args.json:
        print(json.dumps(state, separators=(",", ":"), sort_keys=True))
    else:
        print(
            "[memory-pressure] "
            f"status={state.get('status')} reason={state.get('reason')} "
            f"ram_gib={(state.get('memory') or {}).get('mem_total_gib')} "
            f"swap_gib={(state.get('memory') or {}).get('swap_total_gib')} "
            f"zram_gib={(state.get('swap') or {}).get('zram_total_gib')} "
            f"swapfile_gib={(state.get('swap') or {}).get('managed_swapfile_gib')} "
            f"swappiness={(state.get('vm') or {}).get('swappiness')} "
            f"arc_max_gib={(state.get('zfs') or {}).get('arc_max_gib')}"
        )
        for warning in list(state.get("warnings") or []):
            print(f"[memory-pressure][warning] {warning}")
        for error in list(state.get("errors") or []):
            print(f"[memory-pressure][error] {error}")
    return 0 if bool(state.get("ok")) else 3


__all__ = ["BYTES_IN_GIB", "host_memory_pressure_snapshot", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
