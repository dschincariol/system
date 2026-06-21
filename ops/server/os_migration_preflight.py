#!/usr/bin/env python3
"""Read-only OS migration evidence collector for the bart LTS cutover."""

from __future__ import annotations

import argparse
import datetime as dt
import getpass
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

UBUNTU_APT_HOST_RE = re.compile(
    r"(^|\.)(ubuntu\.com|canonical\.com)$|"
    r"(^|\.)(archive\.ubuntu\.com|security\.ubuntu\.com|ports\.ubuntu\.com|old-releases\.ubuntu\.com)$"
)

INTERESTING_PACKAGES = [
    "ubuntu-release-upgrader-core",
    "update-manager-core",
    "linux-generic",
    "linux-image-generic",
    "linux-headers-generic",
    "zfsutils-linux",
    "zfs-zed",
    "zfs-dkms",
    "spl-dkms",
    "docker.io",
    "docker-ce",
    "docker-ce-cli",
    "containerd",
    "containerd.io",
    "runc",
    "rocm",
    "rocm-core",
    "rocminfo",
    "rocm-smi-lib",
    "amdgpu-dkms",
    "amdgpu-install",
]

TRADING_UNITS = [
    "trading.target",
    "trading-api.service",
    "trading-jobs.service",
    "trading-stream-prices.service",
    "trading-ingest.service",
    "trading-prod-preflight.service",
    "trading-operator.service",
    "trading-engine.service",
    "trading-upgrade.service",
    "trading-backup.service",
    "trading-backup.timer",
    "trading-base-backup.service",
    "trading-base-backup.timer",
    "trading-backup-evidence.service",
    "trading-backup-evidence.timer",
    "trading-backup-prune.service",
    "trading-backup-prune.timer",
    "trading-state-snapshot.service",
    "trading-state-snapshot.timer",
    "trading-artifact-snapshot.service",
    "trading-artifact-snapshot.timer",
    "trading-restore-drill.service",
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


def parse_json_lines(text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            rows.append(json.loads(stripped))
        except json.JSONDecodeError:
            rows.append({"raw": stripped, "parse_error": "invalid_json"})
    return rows


def parse_json_object(text: str) -> dict[str, Any] | None:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else {"value": value}


def classify_apt_uri(uri: str) -> str:
    from urllib.parse import urlparse

    parsed = urlparse(uri)
    host = parsed.netloc.lower()
    if not host:
        return "local"
    if UBUNTU_APT_HOST_RE.search(host):
        return "ubuntu"
    return "third_party"


def apt_source_files() -> list[Path]:
    paths = [Path("/etc/apt/sources.list")]
    sources_dir = Path("/etc/apt/sources.list.d")
    if sources_dir.exists():
        paths.extend(sorted(sources_dir.glob("*.list")))
        paths.extend(sorted(sources_dir.glob("*.sources")))
    return paths


def _parse_deb_line(path: Path, line_number: int, line: str) -> dict[str, Any] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    tokens = stripped.split()
    if not tokens or tokens[0] not in {"deb", "deb-src"}:
        return None
    index = 1
    if index < len(tokens) and tokens[index].startswith("["):
        while index < len(tokens) and not tokens[index].endswith("]"):
            index += 1
        index += 1
    if index >= len(tokens):
        return {
            "path": str(path),
            "line": line_number,
            "raw": stripped,
            "parse_error": "missing_uri",
        }
    uri = tokens[index]
    suite = tokens[index + 1] if index + 1 < len(tokens) else ""
    components = tokens[index + 2 :]
    return {
        "path": str(path),
        "line": line_number,
        "type": tokens[0],
        "uri": uri,
        "suite": suite,
        "components": components,
        "classification": classify_apt_uri(uri),
        "raw": stripped,
    }


def _parse_sources_file(path: Path, text: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    stanza: dict[str, str] = {}
    start_line = 1
    for line_number, raw_line in enumerate(text.splitlines() + [""], start=1):
        line = raw_line.rstrip()
        if not line:
            if stanza:
                uris = stanza.get("URIs", "").split()
                suites = stanza.get("Suites", "").split()
                components = stanza.get("Components", "").split()
                types = stanza.get("Types", "").split()
                for uri in uris:
                    entries.append(
                        {
                            "path": str(path),
                            "line": start_line,
                            "types": types,
                            "uri": uri,
                            "suites": suites,
                            "components": components,
                            "signed_by": stanza.get("Signed-By", ""),
                            "classification": classify_apt_uri(uri),
                            "format": "deb822",
                        }
                    )
                stanza = {}
            start_line = line_number + 1
            continue
        if line.startswith("#"):
            continue
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        stanza[key.strip()] = value.strip()
    return entries


def collect_apt_sources(paths: list[Path] | None = None) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    unreadable: list[dict[str, str]] = []
    for path in paths or apt_source_files():
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            unreadable.append({"path": str(path), "error": str(exc)})
            continue
        if path.suffix == ".sources":
            entries.extend(_parse_sources_file(path, text))
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            entry = _parse_deb_line(path, line_number, line)
            if entry:
                entries.append(entry)
    third_party = [entry for entry in entries if entry.get("classification") == "third_party"]
    return {
        "entries": entries,
        "third_party": third_party,
        "unreadable": unreadable,
        "third_party_count": len(third_party),
    }


def parse_dpkg_inventory(text: str) -> list[dict[str, str]]:
    packages: list[dict[str, str]] = []
    for line in text.splitlines():
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        packages.append(
            {
                "package": parts[0],
                "version": parts[1],
                "architecture": parts[2],
                "status": parts[3],
            }
        )
    return packages


def package_lookup(packages: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {pkg["package"]: pkg for pkg in packages}


def collect_packages() -> dict[str, Any]:
    inventory_cmd = [
        "dpkg-query",
        "-W",
        "-f=${binary:Package}\\t${Version}\\t${Architecture}\\t${db:Status-Abbrev}\\n",
    ]
    inventory_result = command_result(inventory_cmd, timeout=60)
    packages = parse_dpkg_inventory(inventory_result["stdout"]) if command_ok(inventory_result) else []
    lookup = package_lookup(packages)

    manual_result = command_result(["apt-mark", "showmanual"], timeout=60)
    policies: dict[str, dict[str, Any]] = {}
    for package in INTERESTING_PACKAGES:
        policies[package] = {
            "installed": lookup.get(package),
            "apt_policy": command_result(["apt-cache", "policy", package], timeout=20),
        }
    return {
        "inventory_command": inventory_result,
        "manual_packages_command": manual_result,
        "inventory": packages,
        "manual_packages": sorted(manual_result["stdout"].splitlines()) if command_ok(manual_result) else [],
        "interesting_packages": policies,
    }


def collect_kernel_and_zfs() -> dict[str, Any]:
    return {
        "uname": command_result(["uname", "-a"]),
        "kernel_release": platform.release(),
        "kernel_version": platform.version(),
        "zfs_version": command_result(["zfs", "version"], timeout=20),
        "zpool_version": command_result(["zpool", "version"], timeout=20),
        "zpool_list": command_result(["zpool", "list", "-H", "-o", "name,size,alloc,free,health,version"], timeout=20),
        "zpool_status": command_result(["zpool", "status", "-v"], timeout=30),
        "zpool_import": command_result(["zpool", "import"], timeout=30),
        "zfs_list": command_result(["zfs", "list", "-H", "-o", "name,mountpoint,used,avail,refer,compressratio"], timeout=30),
        "zfs_snapshots": command_result(["zfs", "list", "-t", "snapshot", "-H", "-o", "name,creation,used"], timeout=30),
        "modinfo_zfs": command_result(["modinfo", "zfs"], timeout=20),
        "lsmod_zfs": command_result(["lsmod"], timeout=20),
    }


def sanitize_container_inspect(container: dict[str, Any]) -> dict[str, Any]:
    """Keep migration evidence from docker inspect without copying Env values."""

    config = container.get("Config") if isinstance(container.get("Config"), dict) else {}
    state = container.get("State") if isinstance(container.get("State"), dict) else {}
    host_config = container.get("HostConfig") if isinstance(container.get("HostConfig"), dict) else {}
    network = container.get("NetworkSettings") if isinstance(container.get("NetworkSettings"), dict) else {}
    mounts = container.get("Mounts") if isinstance(container.get("Mounts"), list) else []
    safe_mounts = []
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        safe_mounts.append(
            {
                "Type": mount.get("Type"),
                "Name": mount.get("Name"),
                "Source": mount.get("Source"),
                "Destination": mount.get("Destination"),
                "Driver": mount.get("Driver"),
                "Mode": mount.get("Mode"),
                "RW": mount.get("RW"),
                "Propagation": mount.get("Propagation"),
            }
        )
    return {
        "Id": container.get("Id"),
        "Name": container.get("Name"),
        "Image": container.get("Image"),
        "Created": container.get("Created"),
        "Path": container.get("Path"),
        "Args": container.get("Args"),
        "Config": {
            "Image": config.get("Image"),
            "Labels": config.get("Labels") or {},
        },
        "State": {
            "Status": state.get("Status"),
            "Running": state.get("Running"),
            "Paused": state.get("Paused"),
            "Restarting": state.get("Restarting"),
            "OOMKilled": state.get("OOMKilled"),
            "Dead": state.get("Dead"),
            "ExitCode": state.get("ExitCode"),
            "Error": state.get("Error"),
            "StartedAt": state.get("StartedAt"),
            "FinishedAt": state.get("FinishedAt"),
            "Health": state.get("Health"),
        },
        "HostConfig": {
            "RestartPolicy": host_config.get("RestartPolicy"),
            "LogConfig": host_config.get("LogConfig"),
            "ReadonlyRootfs": host_config.get("ReadonlyRootfs"),
            "ShmSize": host_config.get("ShmSize"),
            "Memory": host_config.get("Memory"),
            "NanoCpus": host_config.get("NanoCpus"),
        },
        "Mounts": safe_mounts,
        "NetworkSettings": {
            "Ports": network.get("Ports"),
            "Networks": {
                name: {"IPAddress": value.get("IPAddress"), "Gateway": value.get("Gateway")}
                for name, value in (network.get("Networks") or {}).items()
                if isinstance(value, dict)
            },
        },
    }


def collect_docker() -> dict[str, Any]:
    version_result = command_result(["docker", "version", "--format", "{{json .}}"], timeout=20)
    info_result = command_result(["docker", "info", "--format", "{{json .}}"], timeout=20)
    ps_result = command_result(["docker", "ps", "-a", "--format", "{{json .}}"], timeout=30)
    image_ls_result = command_result(["docker", "image", "ls", "--digests", "--format", "{{json .}}"], timeout=60)
    compose_version = command_result(["docker", "compose", "version"], timeout=20)

    containers = parse_json_lines(ps_result["stdout"]) if command_ok(ps_result) else []
    container_ids = [row.get("ID") for row in containers if row.get("ID")]
    inspect_containers: list[dict[str, Any]] = []
    image_ids: set[str] = set()
    if container_ids:
        inspect_result = command_result(["docker", "inspect", *container_ids], timeout=60)
        if command_ok(inspect_result):
            try:
                inspected = json.loads(inspect_result["stdout"])
            except json.JSONDecodeError:
                inspected = []
            if isinstance(inspected, list):
                for container in inspected:
                    if isinstance(container, dict):
                        inspect_containers.append(sanitize_container_inspect(container))
                        image_id = container.get("Image")
                        if isinstance(image_id, str) and image_id:
                            image_ids.add(image_id)
        else:
            inspect_containers.append({"inspect_error": inspect_result})

    inspected_images: list[dict[str, Any]] = []
    if image_ids:
        image_result = command_result(["docker", "image", "inspect", *sorted(image_ids)], timeout=60)
        if command_ok(image_result):
            try:
                images = json.loads(image_result["stdout"])
            except json.JSONDecodeError:
                images = []
            if isinstance(images, list):
                inspected_images = [image for image in images if isinstance(image, dict)]
        else:
            inspected_images = [{"inspect_error": image_result}]

    return {
        "version": parse_json_object(version_result["stdout"]) if command_ok(version_result) else None,
        "version_command": version_result,
        "info": parse_json_object(info_result["stdout"]) if command_ok(info_result) else None,
        "info_command": info_result,
        "compose_version": compose_version,
        "containers": containers,
        "containers_command": ps_result,
        "container_inspect": inspect_containers,
        "images": parse_json_lines(image_ls_result["stdout"]) if command_ok(image_ls_result) else [],
        "images_command": image_ls_result,
        "image_inspect": inspected_images,
    }


def parse_systemctl_show(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key] = value
    return values


def collect_systemd() -> dict[str, Any]:
    unit_health: dict[str, Any] = {}
    show_props = [
        "Id",
        "Names",
        "LoadState",
        "ActiveState",
        "SubState",
        "UnitFileState",
        "Result",
        "FragmentPath",
        "ActiveEnterTimestamp",
        "InactiveEnterTimestamp",
    ]
    for unit in TRADING_UNITS:
        args = ["systemctl", "show", unit, "--no-pager"]
        for prop in show_props:
            args.extend(["-p", prop])
        result = command_result(args, timeout=15)
        unit_health[unit] = {
            "show": parse_systemctl_show(result["stdout"]) if command_ok(result) else {},
            "command": result,
        }
    return {
        "unit_health": unit_health,
        "list_units": command_result(["systemctl", "list-units", "trading*", "--all", "--plain", "--no-legend", "--no-pager"], timeout=20),
        "list_unit_files": command_result(["systemctl", "list-unit-files", "trading*", "--no-legend", "--no-pager"], timeout=20),
        "list_timers": command_result(["systemctl", "list-timers", "trading*", "--all", "--no-pager"], timeout=20),
        "failed_units": command_result(["systemctl", "--failed", "--no-pager", "--plain", "--no-legend"], timeout=20),
    }


def build_checks(report: dict[str, Any]) -> list[dict[str, Any]]:
    os_release = report["host"]["os_release"]
    codename = os_release.get("VERSION_CODENAME", "")
    packages = report["packages"]
    zfs = report["kernel_zfs"]
    docker = report["docker"]
    systemd = report["systemd"]

    checks = [
        {
            "name": "source_os_supported_for_migration",
            "status": "PASS" if codename in {"questing", "resolute", "noble"} else "FAIL",
            "detail": f"VERSION_CODENAME={codename or 'unknown'}; expected questing before cutover or an LTS after cutover",
        },
        {
            "name": "target_lts_documented",
            "status": "PASS" if report["target_lts"] in {"resolute", "noble"} else "FAIL",
            "detail": f"target_lts={report['target_lts']}",
        },
        {
            "name": "package_inventory_collected",
            "status": "PASS" if command_ok(packages["inventory_command"]) and packages["inventory"] else "FAIL",
            "detail": f"packages={len(packages['inventory'])}",
        },
        {
            "name": "apt_sources_collected",
            "status": "PASS" if not report["apt_sources"]["unreadable"] else "FAIL",
            "detail": f"third_party_sources={report['apt_sources']['third_party_count']}",
        },
        {
            "name": "zfs_status_collected",
            "status": "PASS" if command_ok(zfs["zpool_status"]) and command_ok(zfs["zfs_list"]) else "FAIL",
            "detail": "zpool status and zfs list must be readable without sudo",
        },
        {
            "name": "docker_inventory_collected",
            "status": "PASS" if command_ok(docker["info_command"]) and command_ok(docker["containers_command"]) else "FAIL",
            "detail": f"containers={len(docker['containers'])}; images={len(docker['images'])}",
        },
        {
            "name": "systemd_health_collected",
            "status": "PASS" if command_ok(systemd["list_units"]) or command_ok(systemd["list_unit_files"]) else "FAIL",
            "detail": "trading unit health must be readable through systemctl",
        },
    ]
    return checks


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
    return DEFAULT_OUTPUT_DIR / f"preflight_{safe_host}_{stamp}.json"


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    hostname = socket.gethostname()
    report: dict[str, Any] = {
        "schema": "trading.os_migration.preflight.v1",
        "gate_version": GATE_VERSION,
        "generated_at": utc_now(),
        "target_lts": args.target_lts,
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
            "os_release": read_os_release(),
        },
        "packages": collect_packages(),
        "apt_sources": collect_apt_sources(),
        "kernel_zfs": collect_kernel_and_zfs(),
        "docker": collect_docker(),
        "systemd": collect_systemd(),
    }
    report["checks"] = build_checks(report)
    report["gate_status"] = "PASS" if all(check["status"] == "PASS" for check in report["checks"]) else "FAIL"
    return report


def print_summary(report: dict[str, Any], output: Path | None) -> None:
    print(f"OS migration preflight gate: {report['gate_status']}")
    for check in report["checks"]:
        print(f"{check['status']}: {check['name']} - {check['detail']}")
    if output is not None:
        print(f"report: {output}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target-lts",
        choices=["resolute", "noble"],
        default="resolute",
        help="Target LTS codename. Use noble only for the documented conservative reinstall fallback.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="JSON report path. Defaults to var/os_migration/preflight_<host>_<timestamp>.json. Use '-' for stdout only.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the complete JSON report to stdout instead of the short PASS/FAIL summary.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    report = build_report(args)

    output: Path | None
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
