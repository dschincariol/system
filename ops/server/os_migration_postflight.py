#!/usr/bin/env python3
"""Post-upgrade OS migration verifier for the bart LTS cutover."""

from __future__ import annotations

import argparse
import datetime as dt
import getpass
import glob
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "var" / "os_migration"
GATE_VERSION = "1.0"

DEFAULT_CONTAINERS = [
    "trading-timescaledb",
    "trading-redis",
    "trading-minio",
    "trading-runtime",
    "trading-operator",
]

BACKUP_TIMERS = [
    "trading-base-backup.timer",
    "trading-backup-evidence.timer",
    "trading-backup-prune.timer",
    "trading-restore-drill.timer",
]


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_os_release(path: Path = Path("/etc/os-release")) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return values
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value.strip().strip('"')
    return values


def command_result(args: list[str], *, timeout: int = 30) -> dict[str, Any]:
    executable = shutil.which(args[0])
    result: dict[str, Any] = {
        "command": args,
        "executable": executable,
        "exit_code": None,
        "stdout": "",
        "stderr": "",
        "timed_out": False,
    }
    if executable is None:
        result["stderr"] = f"missing command: {args[0]}"
        return result
    try:
        completed = subprocess.run(
            args,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        result["timed_out"] = True
        result["stdout"] = exc.stdout or ""
        result["stderr"] = exc.stderr or f"command timed out after {timeout}s"
        return result
    result["exit_code"] = completed.returncode
    result["stdout"] = completed.stdout
    result["stderr"] = completed.stderr
    return result


def command_ok(result: dict[str, Any]) -> bool:
    return result.get("exit_code") == 0 and not result.get("timed_out")


def pass_check(name: str, detail: str, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"name": name, "status": "PASS", "detail": detail, "evidence": evidence or {}}


def fail_check(name: str, detail: str, evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"name": name, "status": "FAIL", "detail": detail, "evidence": evidence or {}}


def parse_kernel_major_minor(release: str) -> tuple[int, int] | None:
    match = re.match(r"^(\d+)\.(\d+)", release)
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def check_os_and_kernel(target_codename: str) -> dict[str, Any]:
    os_release = read_os_release()
    codename = os_release.get("VERSION_CODENAME", "")
    kernel = platform.release()
    parsed = parse_kernel_major_minor(kernel)
    evidence = {"os_release": os_release, "kernel_release": kernel}

    if codename != target_codename:
        return fail_check(
            "os_lts_target",
            f"VERSION_CODENAME={codename or 'unknown'}; expected {target_codename}",
            evidence,
        )
    if target_codename == "resolute" and (parsed is None or parsed[0] < 7):
        return fail_check("os_lts_target", f"resolute target requires Linux 7.x or newer; observed {kernel}", evidence)
    if target_codename == "noble" and (parsed is None or parsed < (6, 8)):
        return fail_check("os_lts_target", f"noble fallback requires Linux 6.8 or newer; observed {kernel}", evidence)
    return pass_check("os_lts_target", f"{codename} on kernel {kernel}", evidence)


def check_zfs(pool: str | None) -> dict[str, Any]:
    list_args = ["zpool", "list", "-H", "-o", "name,health"]
    list_result = command_result(list_args, timeout=20)
    status_args = ["zpool", "status", "-v"]
    if pool:
        status_args.append(pool)
    status_result = command_result(status_args, timeout=30)
    version_result = command_result(["zfs", "version"], timeout=20)
    evidence = {"zpool_list": list_result, "zpool_status": status_result, "zfs_version": version_result}

    if not command_ok(list_result) or not command_ok(status_result) or not command_ok(version_result):
        return fail_check("zfs_import_and_health", "zfs/zpool commands must succeed without sudo", evidence)
    pool_rows = [line.split("\t") for line in list_result["stdout"].splitlines() if line.strip()]
    if pool and not any(row and row[0] == pool for row in pool_rows):
        return fail_check("zfs_import_and_health", f"required pool {pool} is not imported", evidence)
    bad_pools = [row for row in pool_rows if len(row) > 1 and row[1] not in {"ONLINE"}]
    if bad_pools:
        return fail_check("zfs_import_and_health", f"unhealthy pools: {bad_pools}", evidence)
    status_text = status_result["stdout"]
    for marker in ("FAULTED", "DEGRADED", "UNAVAIL", "corrupt"):
        if marker in status_text:
            return fail_check("zfs_import_and_health", f"zpool status contains {marker}", evidence)
    return pass_check("zfs_import_and_health", f"imported pools healthy: {[row[0] for row in pool_rows if row]}", evidence)


def parse_json_lines(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            rows.append({"raw": line, "parse_error": "invalid_json"})
    return rows


def load_preflight(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def check_docker(expected_containers: list[str], preflight: dict[str, Any] | None) -> dict[str, Any]:
    info_result = command_result(["docker", "info", "--format", "{{json .}}"], timeout=20)
    ps_result = command_result(["docker", "ps", "-a", "--format", "{{json .}}"], timeout=30)
    evidence: dict[str, Any] = {"docker_info": info_result, "docker_ps": ps_result}
    if not command_ok(info_result) or not command_ok(ps_result):
        return fail_check("docker_data_and_container_health", "docker daemon and container inventory must be readable", evidence)

    containers = parse_json_lines(ps_result["stdout"])
    evidence["containers"] = containers
    by_name = {str(row.get("Names") or row.get("Name")): row for row in containers}
    missing = [name for name in expected_containers if name not in by_name]
    bad: list[str] = []
    for name in expected_containers:
        row = by_name.get(name)
        if not row:
            continue
        state = str(row.get("State", "")).lower()
        status = str(row.get("Status", "")).lower()
        if state != "running" or "unhealthy" in status or "exited" in status:
            bad.append(f"{name}: state={row.get('State')} status={row.get('Status')}")
    if missing or bad:
        return fail_check(
            "docker_data_and_container_health",
            f"missing={missing}; unhealthy={bad}",
            evidence,
        )

    if preflight:
        previous_images = {
            image.get("Id")
            for image in preflight.get("docker", {}).get("image_inspect", [])
            if isinstance(image, dict) and image.get("Id")
        }
        if previous_images:
            inspect_result = command_result(["docker", "image", "inspect", *sorted(previous_images)], timeout=60)
            evidence["preflight_image_inspect"] = inspect_result
            if not command_ok(inspect_result):
                return fail_check(
                    "docker_data_and_container_health",
                    "one or more preflight image IDs are missing after the OS migration",
                    evidence,
                )

    return pass_check("docker_data_and_container_health", f"containers healthy: {expected_containers}", evidence)


def check_backup_timers(timers: list[str]) -> dict[str, Any]:
    evidence: dict[str, Any] = {"timers": {}}
    failures: list[str] = []
    for timer in timers:
        active = command_result(["systemctl", "is-active", timer], timeout=10)
        enabled = command_result(["systemctl", "is-enabled", timer], timeout=10)
        evidence["timers"][timer] = {"active": active, "enabled": enabled}
        if active["stdout"].strip() != "active":
            failures.append(f"{timer} active={active['stdout'].strip() or active['stderr'].strip()}")
        if enabled["stdout"].strip() not in {"enabled", "static"}:
            failures.append(f"{timer} enabled={enabled['stdout'].strip() or enabled['stderr'].strip()}")
    if failures:
        return fail_check("backup_timers", "; ".join(failures), evidence)
    return pass_check("backup_timers", f"active timers: {timers}", evidence)


def check_backup_evidence(path: Path) -> dict[str, Any]:
    evidence: dict[str, Any] = {"path": str(path)}
    try:
        with path.open("r", encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        return fail_check("backup_evidence", f"cannot read backup evidence JSON: {exc}", evidence)
    evidence["report"] = report

    component_statuses: list[str] = []
    for key in ("base_backup", "wal_archive", "wal_archiver", "restore_drill"):
        value = report.get(key, {})
        if isinstance(value, dict):
            component_statuses.append(str(value.get("status", "")).lower())
    signature = report.get("signature", {})
    signature_status = str(signature.get("status", "")).lower() if isinstance(signature, dict) else ""

    if not component_statuses or any(status not in {"pass", "passed"} for status in component_statuses):
        return fail_check("backup_evidence", f"backup component statuses are not all pass: {component_statuses}", evidence)
    if signature_status != "signed":
        return fail_check("backup_evidence", f"backup evidence signature status is {signature_status or 'missing'}", evidence)
    return pass_check("backup_evidence", "latest backup/WAL/restore evidence is signed and passing", evidence)


def rocm_marker_present() -> bool:
    return bool(shutil.which("rocminfo") or shutil.which("rocm-smi") or Path("/opt/rocm").exists() or Path("/dev/kfd").exists())


def path_rw(path: Path) -> bool:
    return os.access(path, os.R_OK | os.W_OK)


def check_rocm(require_rocm: bool, expected_gfx: str) -> dict[str, Any]:
    marker_present = rocm_marker_present()
    evidence: dict[str, Any] = {
        "required": require_rocm,
        "marker_present": marker_present,
        "dev_kfd_exists": Path("/dev/kfd").exists(),
        "dev_kfd_rw": path_rw(Path("/dev/kfd")) if Path("/dev/kfd").exists() else False,
        "render_nodes": glob.glob("/dev/dri/renderD*"),
    }
    if not require_rocm and not marker_present:
        return pass_check("rocm_device_access", "ROCm not required and no ROCm marker detected", evidence)
    failures: list[str] = []
    if not Path("/dev/kfd").exists() or not path_rw(Path("/dev/kfd")):
        failures.append("/dev/kfd is missing or not readable/writable by this user")
    render_nodes = [Path(path) for path in evidence["render_nodes"]]
    if not render_nodes or not any(path_rw(path) for path in render_nodes):
        failures.append("/dev/dri/renderD* is missing or not readable/writable by this user")
    rocminfo = command_result(["rocminfo"], timeout=45)
    evidence["rocminfo"] = rocminfo
    if not command_ok(rocminfo):
        failures.append("rocminfo failed")
    elif expected_gfx and expected_gfx not in rocminfo["stdout"]:
        failures.append(f"rocminfo did not report expected {expected_gfx}")
    if failures:
        return fail_check("rocm_device_access", "; ".join(failures), evidence)
    return pass_check("rocm_device_access", f"ROCm device access verified for {expected_gfx}", evidence)


def check_runtime_preflight(enabled: bool) -> dict[str, Any]:
    if not enabled:
        return pass_check("runtime_prod_preflight", "not requested; run with --run-runtime-preflight to enforce this gate")
    result = command_result([sys.executable, str(REPO_ROOT / "engine" / "runtime" / "prod_preflight.py"), "--json"], timeout=120)
    evidence = {"command": result}
    if not command_ok(result):
        return fail_check("runtime_prod_preflight", "prod_preflight.py --json failed", evidence)
    try:
        payload = json.loads(result["stdout"])
    except json.JSONDecodeError:
        return fail_check("runtime_prod_preflight", "prod_preflight.py output is not JSON", evidence)
    evidence["payload"] = payload
    if payload.get("ok") is not True or payload.get("status") not in {None, "passed"}:
        return fail_check("runtime_prod_preflight", "production preflight did not pass", evidence)
    return pass_check("runtime_prod_preflight", "production preflight passed", evidence)


def write_report(report: dict[str, Any], output: Path | None) -> Path | None:
    if output is None:
        return None
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent, delete=False) as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
        tmp_name = handle.name
    Path(tmp_name).replace(output)
    return output


def default_output_path(hostname: str) -> Path:
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    safe_host = re.sub(r"[^A-Za-z0-9_.-]+", "_", hostname)
    return DEFAULT_OUTPUT_DIR / f"postflight_{safe_host}_{stamp}.json"


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    hostname = socket.gethostname()
    preflight = load_preflight(args.preflight_report)
    checks = [
        check_os_and_kernel(args.target_codename),
        check_zfs(args.zfs_pool),
        check_docker(args.expected_container, preflight),
        check_backup_timers(args.backup_timer),
        check_backup_evidence(args.backup_evidence),
        check_rocm(args.require_rocm, args.rocm_gfx),
        check_runtime_preflight(args.run_runtime_preflight),
    ]
    return {
        "schema": "trading.os_migration.postflight.v1",
        "gate_version": GATE_VERSION,
        "generated_at": utc_now(),
        "target_codename": args.target_codename,
        "read_only": True,
        "mutating_actions": [],
        "host": {
            "hostname": hostname,
            "fqdn": socket.getfqdn(),
            "user": getpass.getuser(),
            "uid": os.getuid() if hasattr(os, "getuid") else None,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": sys.version,
        },
        "preflight_report": str(args.preflight_report) if args.preflight_report else None,
        "checks": checks,
        "gate_status": "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL",
    }


def print_summary(report: dict[str, Any], output: Path | None) -> None:
    print(f"OS migration postflight gate: {report['gate_status']}")
    for check in report["checks"]:
        print(f"{check['status']}: {check['name']} - {check['detail']}")
    if output is not None:
        print(f"report: {output}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-codename",
        choices=["resolute", "noble"],
        default="resolute",
        help="Expected LTS codename after migration.",
    )
    parser.add_argument("--zfs-pool", help="Required imported ZFS pool name, for example zpool.")
    parser.add_argument(
        "--expected-container",
        action="append",
        default=list(DEFAULT_CONTAINERS),
        help="Container name that must be running and healthy. Repeat to add names.",
    )
    parser.add_argument(
        "--backup-timer",
        action="append",
        default=list(BACKUP_TIMERS),
        help="Systemd backup timer that must be enabled and active. Repeat to add timers.",
    )
    parser.add_argument(
        "--backup-evidence",
        type=Path,
        default=Path("/var/backups/trading/evidence/latest_backup_restore_evidence.json"),
        help="Signed backup evidence JSON path.",
    )
    parser.add_argument("--preflight-report", type=Path, help="Pre-upgrade preflight JSON report to compare Docker images against.")
    parser.add_argument("--require-rocm", action="store_true", help="Fail unless ROCm device access is available.")
    parser.add_argument("--rocm-gfx", default="gfx1151", help="Expected ROCm GFX target when ROCm is required or detected.")
    parser.add_argument("--run-runtime-preflight", action="store_true", help="Also run engine/runtime/prod_preflight.py --json.")
    parser.add_argument(
        "--output",
        type=Path,
        help="JSON report path. Defaults to var/os_migration/postflight_<host>_<timestamp>.json. Use '-' for stdout only.",
    )
    parser.add_argument("--json", action="store_true", help="Print the complete JSON report to stdout.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    report = build_report(args)
    if args.output and str(args.output) == "-":
        output = None
    else:
        output = args.output or default_output_path(report["host"]["hostname"])
        write_report(report, output)

    if args.json or (args.output and str(args.output) == "-"):
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_summary(report, output)
    return 0 if report["gate_status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
