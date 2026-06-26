"""Host-only readiness checks for the live validation gate."""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass


MIN_SWAP_BYTES = 16 * 1024 * 1024 * 1024
SYSTEMD_UNITS = {
    "trading-engine.service": {
        "type": "notify",
        "memory_max": str(32 * 1024 * 1024 * 1024),
        "oom_score_adjust": "600",
    },
    "trading-operator.service": {
        "type": "notify",
        "memory_max": str(6 * 1024 * 1024 * 1024),
        "oom_score_adjust": "700",
    },
}


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


def _run(args: list[str]) -> CommandResult:
    try:
        proc = subprocess.run(args, check=False, capture_output=True, text=True)
    except FileNotFoundError:
        return CommandResult(127, "", f"missing executable: {args[0]}")
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


def _parse_systemctl_show(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        if "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _swap_total_bytes() -> tuple[int | None, str | None]:
    if shutil.which("swapon") is None:
        return None, "swapon_missing"
    result = _run(["swapon", "--show=SIZE", "--bytes", "--noheadings", "--raw"])
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        return None, f"swapon_failed:{detail or result.returncode}"
    total = 0
    for raw_line in result.stdout.splitlines():
        raw = raw_line.strip()
        if not raw:
            continue
        tokens = raw.split()
        candidate = tokens[0]
        if len(tokens) >= 3 and tokens[2].isdigit():
            candidate = tokens[2]
        try:
            total += int(candidate)
        except ValueError:
            return None, f"swapon_unparseable_size:{raw}"
    return total, None


def _systemd_unit_errors(unit: str, expected: dict[str, str]) -> list[str]:
    result = _run(
        [
            "systemctl",
            "show",
            unit,
            "-p",
            "LoadState",
            "-p",
            "ActiveState",
            "-p",
            "Type",
            "-p",
            "WatchdogUSec",
            "-p",
            "MemoryMax",
            "-p",
            "OOMScoreAdjust",
        ]
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        return [f"{unit}:systemctl_show_failed:{detail or result.returncode}"]

    values = _parse_systemctl_show(result.stdout)
    if values.get("LoadState") in {"", "not-found"}:
        return []

    errors: list[str] = []
    if values.get("Type") != expected["type"]:
        errors.append(f"{unit}:type_invalid:{values.get('Type') or '<missing>'}")

    watchdog = values.get("WatchdogUSec") or ""
    if watchdog in {"", "0", "infinity"}:
        state = values.get("ActiveState") or "unknown"
        errors.append(
            f"{unit}:watchdog_not_applied:WatchdogUSec={watchdog or '<missing>'}:"
            f"ActiveState={state}:run sudo systemctl daemon-reload && "
            f"sudo systemctl restart {unit}"
        )

    if values.get("MemoryMax") != expected["memory_max"]:
        errors.append(
            f"{unit}:memory_max_invalid:{values.get('MemoryMax') or '<missing>'}:"
            f"expected={expected['memory_max']}"
        )

    if values.get("OOMScoreAdjust") != expected["oom_score_adjust"]:
        errors.append(
            f"{unit}:oom_score_adjust_invalid:{values.get('OOMScoreAdjust') or '<missing>'}:"
            f"expected={expected['oom_score_adjust']}"
        )
    return errors


def live_host_readiness_errors() -> list[str]:
    errors: list[str] = []

    swap_total, swap_error = _swap_total_bytes()
    if swap_error:
        errors.append(f"swap_check_failed:{swap_error}")
    elif swap_total is None or swap_total < MIN_SWAP_BYTES:
        errors.append(f"swap_capacity_below_policy:{swap_total or 0}:expected_at_least={MIN_SWAP_BYTES}")

    if shutil.which("systemctl") is None:
        return errors

    for unit, expected in SYSTEMD_UNITS.items():
        errors.extend(_systemd_unit_errors(unit, expected))
    return errors


def main() -> int:
    errors = live_host_readiness_errors()
    if errors:
        print("Live host readiness failed:")
        for error in errors:
            print(f"- {error}")
        print(
            "Host-only remediation: install the repo systemd units, run "
            "`sudo systemctl daemon-reload`, restart the active services, and "
            "rerun this check. Inactive systemd services report "
            "`WatchdogUSec=infinity` even when the unit file contains "
            "`WatchdogSec=60s`."
        )
        return 1
    print("Live host readiness passed: swap and installed systemd service properties are applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
