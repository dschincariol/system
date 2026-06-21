#!/usr/bin/env python3
from __future__ import annotations

"""Read-only assessment for the idle Windows/BitLocker NVMe on bart."""

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


DEFAULT_DEVICE = "nvme0n1"
WINDOWS_EFI_PARTTYPE = "c12a7328-f81f-11d2-ba4b-00a0c93ec93b"
WINDOWS_MSR_PARTTYPE = "e3c9e316-0b5c-4db8-817d-f92df00215ae"
WINDOWS_BASIC_DATA_PARTTYPE = "ebd0a0a2-b9e5-4433-87c0-68b6b72699c7"
WINDOWS_RECOVERY_PARTTYPE = "de94bba4-06d1-4d40-a16a-bfd50179d6ac"
LSBLK_OUTPUT_COLUMNS = (
    "NAME,KNAME,PATH,TYPE,SIZE,FSTYPE,FSVER,LABEL,UUID,PARTUUID,PARTTYPE,PARTLABEL,"
    "MOUNTPOINTS,PKNAME,ROTA,TRAN,MODEL,SERIAL,STATE,RM,RO"
)
LINUX_FILESYSTEMS = {
    "ext2",
    "ext3",
    "ext4",
    "xfs",
    "btrfs",
    "zfs_member",
    "linux_raid_member",
    "lvm2_member",
    "crypto_luks",
    "swap",
}
CRITICAL_LINUX_MOUNTPOINTS = {
    "/",
    "/boot",
    "/boot/efi",
    "/home",
    "/var",
    "/var/lib",
    "/var/lib/docker",
    "/var/lib/postgresql",
    "/var/backups/trading",
}


@dataclass(frozen=True)
class CommandResult:
    args: tuple[str, ...]
    returncode: int
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    missing: bool = False

    def to_json(self) -> dict[str, Any]:
        return {
            "args": list(self.args),
            "returncode": self.returncode,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "timed_out": self.timed_out,
            "missing": self.missing,
        }


Runner = Callable[[Sequence[str]], CommandResult]


def _run_command(args: Sequence[str], *, timeout_s: float) -> CommandResult:
    try:
        proc = subprocess.run(
            list(args),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except FileNotFoundError as exc:
        return CommandResult(tuple(args), 127, stderr=str(exc), missing=True)
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        return CommandResult(tuple(args), 124, stdout=stdout, stderr=stderr, timed_out=True)
    return CommandResult(tuple(args), int(proc.returncode), proc.stdout or "", proc.stderr or "")


def _node_children(node: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = node.get("children") or []
    return [dict(child) for child in raw if isinstance(child, Mapping)]


def _flatten_lsblk(nodes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for node in nodes:
        item = dict(node)
        flattened.append(item)
        flattened.extend(_flatten_lsblk(_node_children(item)))
    return flattened


def _normalize_device_name(device: str, *, dev_root: Path = Path("/dev")) -> str:
    raw = str(device or "").strip()
    if raw.startswith("/dev/disk/") or str(Path(raw).parent).startswith(str(dev_root / "disk")):
        try:
            resolved = Path(raw).resolve()
        except OSError:
            resolved = Path(raw)
        if resolved.name:
            return resolved.name
    if raw.startswith("/dev/") or raw.startswith(str(dev_root)):
        return Path(raw).name
    return raw


def _device_path(device_name: str) -> str:
    return f"/dev/{device_name}"


def _target_matches(node: Mapping[str, Any], device_name: str) -> bool:
    names = {
        str(node.get("name") or ""),
        str(node.get("kname") or ""),
        Path(str(node.get("path") or "")).name,
    }
    return device_name in names


def _find_target(nodes: Iterable[Mapping[str, Any]], device_name: str) -> dict[str, Any] | None:
    for node in nodes:
        if _target_matches(node, device_name):
            return dict(node)
        child = _find_target(_node_children(node), device_name)
        if child is not None:
            return child
    return None


def _target_nodes(target: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(item) for item in _flatten_lsblk([target])]


def _as_mountpoints(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    text = str(value).strip()
    if not text:
        return []
    return [item for item in text.splitlines() if item]


def _lower(value: Any) -> str:
    return str(value if value is not None else "").strip().lower()


def _partition_summary(nodes: Iterable[Mapping[str, Any]], device_name: str) -> list[dict[str, Any]]:
    partitions: list[dict[str, Any]] = []
    for node in nodes:
        if _lower(node.get("type")) != "part":
            continue
        partitions.append(
            {
                "name": node.get("name") or node.get("kname"),
                "path": node.get("path") or _device_path(str(node.get("name") or "")),
                "size_bytes": node.get("size"),
                "fstype": node.get("fstype"),
                "fsver": node.get("fsver"),
                "label": node.get("label"),
                "uuid": node.get("uuid"),
                "partuuid": node.get("partuuid"),
                "parttype": node.get("parttype"),
                "partlabel": node.get("partlabel"),
                "mountpoints": _as_mountpoints(node.get("mountpoints")),
                "parent_disk": device_name,
            }
        )
    return partitions


def _windows_layout_flags(partitions: Iterable[Mapping[str, Any]]) -> dict[str, bool]:
    flags = {
        "has_efi": False,
        "has_msr": False,
        "has_bitlocker": False,
        "has_ntfs": False,
        "has_recovery": False,
    }
    for part in partitions:
        fstype = _lower(part.get("fstype"))
        parttype = _lower(part.get("parttype"))
        partlabel = _lower(part.get("partlabel"))
        label = _lower(part.get("label"))
        flags["has_efi"] = flags["has_efi"] or fstype == "vfat" or parttype == WINDOWS_EFI_PARTTYPE
        flags["has_msr"] = flags["has_msr"] or parttype == WINDOWS_MSR_PARTTYPE or "reserved" in partlabel
        flags["has_bitlocker"] = flags["has_bitlocker"] or "bitlocker" in fstype or "bitlocker" in label
        flags["has_ntfs"] = flags["has_ntfs"] or fstype == "ntfs"
        flags["has_recovery"] = (
            flags["has_recovery"]
            or parttype == WINDOWS_RECOVERY_PARTTYPE
            or "recovery" in label
            or "recovery" in partlabel
        )
    flags["windows_bitlocker_layout_likely"] = bool(
        flags["has_bitlocker"] and (flags["has_efi"] or flags["has_msr"] or flags["has_recovery"] or flags["has_ntfs"])
    )
    return flags


def _identifier_tokens(nodes: Iterable[Mapping[str, Any]], device_name: str, *, dev_root: Path) -> dict[str, list[str]]:
    tokens: dict[str, set[str]] = {
        "device_names": {device_name},
        "device_paths": {_device_path(device_name)},
        "uuids": set(),
        "partuuids": set(),
        "labels": set(),
        "by_id_links": set(),
        "by_disk_links": set(),
    }
    for node in nodes:
        name = str(node.get("name") or node.get("kname") or "").strip()
        path = str(node.get("path") or "").strip()
        if name:
            tokens["device_names"].add(name)
            tokens["device_paths"].add(_device_path(name))
        if path:
            tokens["device_paths"].add(path)
        for key, bucket in (("uuid", "uuids"), ("partuuid", "partuuids"), ("label", "labels")):
            value = str(node.get(key) or "").strip()
            if value:
                tokens[bucket].add(value)
    disk_root = dev_root / "disk"
    target_paths = {Path(path).name for path in tokens["device_paths"] if path.startswith("/dev/")}
    try:
        for link in disk_root.glob("by-*/*"):
            try:
                resolved = link.resolve()
            except OSError:
                continue
            if resolved.name in target_paths:
                tokens["by_disk_links"].add(str(link))
                if link.parent.name == "by-id":
                    tokens["by_id_links"].add(str(link))
    except OSError:
        pass
    return {key: sorted(value) for key, value in sorted(tokens.items())}


def _stable_paths(identifiers: Mapping[str, Iterable[str]], device_name: str) -> list[str]:
    by_id = [str(value) for value in identifiers.get("by_id_links", []) if str(value).strip()]
    if by_id:
        return sorted(by_id)
    by_disk = [str(value) for value in identifiers.get("by_disk_links", []) if str(value).strip()]
    if by_disk:
        return sorted(by_disk)
    return [_device_path(device_name)]


def _plain_tokens(identifiers: Mapping[str, Iterable[str]]) -> set[str]:
    tokens: set[str] = set()
    for bucket, values in identifiers.items():
        for raw in values:
            value = str(raw or "").strip()
            if not value:
                continue
            if bucket == "labels":
                tokens.add(f"LABEL={value}")
                tokens.add(f"/dev/disk/by-label/{value}")
                continue
            if bucket == "uuids":
                tokens.add(value)
                tokens.add(f"UUID={value}")
                tokens.add(f"/dev/disk/by-uuid/{value}")
                continue
            if bucket == "partuuids":
                tokens.add(value)
                tokens.add(f"PARTUUID={value}")
                tokens.add(f"/dev/disk/by-partuuid/{value}")
                continue
            tokens.add(value)
            if bucket == "device_paths" and value.startswith("/dev/"):
                tokens.add(Path(value).name)
    return tokens


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _line_references(text: str, tokens: set[str], *, source: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        found = sorted(token for token in tokens if token and token in stripped)
        if found:
            matches.append({"source": source, "line": number, "tokens": found, "text": stripped})
    return matches


def _config_references(identifiers: Mapping[str, Iterable[str]], *, etc_root: Path) -> list[dict[str, Any]]:
    tokens = _plain_tokens(identifiers)
    candidates = [
        etc_root / "fstab",
        etc_root / "crypttab",
        etc_root / "docker" / "daemon.json",
    ]
    references: list[dict[str, Any]] = []
    for path in candidates:
        if path.exists():
            references.extend(_line_references(_read_text(path), tokens, source=str(path)))
    systemd_root = etc_root / "systemd" / "system"
    try:
        for unit_path in sorted(systemd_root.glob("*.service")) + sorted(systemd_root.glob("*.mount")):
            references.extend(_line_references(_read_text(unit_path), tokens, source=str(unit_path)))
    except OSError:
        pass
    return references


def _active_mounts_from_lsblk(nodes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    mounts: list[dict[str, Any]] = []
    for node in nodes:
        mountpoints = _as_mountpoints(node.get("mountpoints"))
        if not mountpoints:
            continue
        mounts.append(
            {
                "path": node.get("path") or _device_path(str(node.get("name") or "")),
                "name": node.get("name") or node.get("kname"),
                "mountpoints": mountpoints,
            }
        )
    return mounts


def _proc_references(identifiers: Mapping[str, Iterable[str]], *, proc_root: Path) -> dict[str, list[dict[str, Any]]]:
    tokens = _plain_tokens(identifiers)
    return {
        "mounts": _line_references(_read_text(proc_root / "mounts"), tokens, source=str(proc_root / "mounts")),
        "swaps": _line_references(_read_text(proc_root / "swaps"), tokens, source=str(proc_root / "swaps")),
        "mdstat": _line_references(_read_text(proc_root / "mdstat"), tokens, source=str(proc_root / "mdstat")),
    }


def _parse_json(stdout: str) -> dict[str, Any]:
    try:
        parsed = json.loads(stdout)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _command_search_references(
    commands: Mapping[str, CommandResult],
    identifiers: Mapping[str, Iterable[str]],
) -> list[dict[str, Any]]:
    tokens = _plain_tokens(identifiers)
    references: list[dict[str, Any]] = []
    for name, result in commands.items():
        if result.returncode not in (0, 1):
            continue
        for stream_name, text in (("stdout", result.stdout), ("stderr", result.stderr)):
            for match in _line_references(text, tokens, source=f"{name}:{stream_name}"):
                references.append(match)
    return references


def _target_command_paths(nodes: Iterable[Mapping[str, Any]]) -> list[str]:
    paths: list[str] = []
    for node in nodes:
        path = str(node.get("path") or "").strip()
        if path:
            paths.append(path)
    return paths


def _is_truthy_block_value(value: Any) -> bool:
    return str(value if value is not None else "").strip().lower() in {"1", "true", "yes", "on"}


def _local_disk_nodes(blockdevices: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    disks: list[dict[str, Any]] = []
    for node in blockdevices:
        if _lower(node.get("type")) != "disk":
            continue
        name = str(node.get("name") or node.get("kname") or "").strip()
        path = str(node.get("path") or "").strip()
        if not name or name.startswith(("loop", "ram", "zram", "sr")):
            continue
        if path and not path.startswith("/dev/"):
            continue
        if _is_truthy_block_value(node.get("rm")):
            continue
        disks.append(dict(node))
    return disks


def _mounted_on_critical_linux_path(partitions: Iterable[Mapping[str, Any]]) -> list[str]:
    mountpoints: list[str] = []
    for part in partitions:
        for mountpoint in _as_mountpoints(part.get("mountpoints")):
            normalized = str(mountpoint or "").rstrip("/") or "/"
            if normalized in CRITICAL_LINUX_MOUNTPOINTS:
                mountpoints.append(normalized)
    return sorted(set(mountpoints))


def _linux_fstypes(partitions: Iterable[Mapping[str, Any]]) -> list[str]:
    values: list[str] = []
    for part in partitions:
        fstype = _lower(part.get("fstype"))
        if fstype in LINUX_FILESYSTEMS:
            values.append(fstype)
    return sorted(set(values))


def classify_assessment(report: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a disk as GO candidate, RETAIN candidate, or NO-GO."""

    device = dict(report.get("device") or {})
    if not bool(device.get("found", True)):
        return {
            "classification": "no_go",
            "candidate": False,
            "reason": "target_device_not_found",
            "reasons": ["target_device_not_found"],
        }

    contents = dict(report.get("contents") or {})
    partitions = [dict(item) for item in contents.get("partitions") or [] if isinstance(item, Mapping)]
    windows = dict(contents.get("windows_layout") or {})
    references = dict(report.get("references") or {})
    active_ref_count = len(references.get("active") or [])
    config_ref_count = len(references.get("config") or [])
    critical_mounts = _mounted_on_critical_linux_path(partitions)
    linux_fstypes = _linux_fstypes(partitions)
    reasons: list[str] = []

    if critical_mounts:
        reasons.append("critical_linux_mountpoint")
    if "zfs_member" in linux_fstypes:
        reasons.append("zfs_member_detected")
    non_zfs_linux = [fstype for fstype in linux_fstypes if fstype != "zfs_member"]
    if non_zfs_linux:
        reasons.append("linux_filesystem_detected")

    if reasons:
        return {
            "classification": "no_go",
            "candidate": False,
            "reason": reasons[0],
            "reasons": reasons,
            "critical_mountpoints": critical_mounts,
            "linux_fstypes": linux_fstypes,
            "active_reference_count": active_ref_count,
            "config_reference_count": config_ref_count,
        }

    windows_likely = bool(windows.get("windows_bitlocker_layout_likely"))
    unused = bool(report.get("unused_by_linux"))
    if windows_likely and unused:
        return {
            "classification": "go_candidate",
            "candidate": True,
            "reason": "idle_windows_bitlocker_disk",
            "reasons": ["idle_windows_bitlocker_disk"],
            "active_reference_count": active_ref_count,
            "config_reference_count": config_ref_count,
        }
    if windows_likely:
        if active_ref_count:
            reasons.append("windows_bitlocker_layout_has_active_linux_references")
        if config_ref_count:
            reasons.append("windows_bitlocker_layout_has_config_references")
        if not reasons:
            reasons.append("windows_bitlocker_layout_requires_operator_retain_or_reclaim_decision")
        return {
            "classification": "retain_candidate",
            "candidate": False,
            "reason": reasons[0],
            "reasons": reasons,
            "active_reference_count": active_ref_count,
            "config_reference_count": config_ref_count,
        }
    return {
        "classification": "no_go",
        "candidate": False,
        "reason": "not_windows_bitlocker_layout",
        "reasons": ["not_windows_bitlocker_layout"],
        "active_reference_count": active_ref_count,
        "config_reference_count": config_ref_count,
        "linux_fstypes": linux_fstypes,
    }


def build_assessment(
    *,
    device: str = DEFAULT_DEVICE,
    runner: Runner | None = None,
    timeout_s: float = 6.0,
    proc_root: Path = Path("/proc"),
    etc_root: Path = Path("/etc"),
    dev_root: Path = Path("/dev"),
) -> dict[str, Any]:
    requested_device = str(device or "").strip()
    device_name = _normalize_device_name(requested_device, dev_root=dev_root)
    command_runner = runner or (lambda args: _run_command(args, timeout_s=timeout_s))
    commands: dict[str, CommandResult] = {}

    commands["lsblk"] = command_runner(
        [
            "lsblk",
            "--json",
            "--bytes",
            "--output",
            LSBLK_OUTPUT_COLUMNS,
        ]
    )
    lsblk_json = _parse_json(commands["lsblk"].stdout)
    blockdevices = [dict(item) for item in lsblk_json.get("blockdevices") or [] if isinstance(item, Mapping)]
    target = _find_target(blockdevices, device_name)
    if target is None:
        return {
            "ok": False,
            "generated_at_epoch": time.time(),
            "device": {
                "requested": requested_device,
                "name": device_name,
                "path": _device_path(device_name),
                "stable_paths": [_device_path(device_name)],
                "stable_path": _device_path(device_name),
                "found": False,
            },
            "contents": {"partitions": [], "windows_layout": {}},
            "references": {"active": [], "config": [], "proc": {}, "command": []},
            "unused_by_linux": False,
            "reason": "target_device_not_found",
            "warnings": ["lsblk_did_not_report_target_device"],
            "commands": {name: result.to_json() for name, result in sorted(commands.items())},
        }

    nodes = _target_nodes(target)
    paths = _target_command_paths(nodes)
    partitions = _partition_summary(nodes, device_name)
    identifiers = _identifier_tokens(nodes, device_name, dev_root=dev_root)
    stable_paths = _stable_paths(identifiers, device_name)
    active_lsblk = _active_mounts_from_lsblk(nodes)
    proc_refs = _proc_references(identifiers, proc_root=proc_root)
    config_refs = _config_references(identifiers, etc_root=etc_root)

    for index, path in enumerate(paths):
        commands[f"blkid:{index}"] = command_runner(["blkid", "-o", "export", path])
    commands["findmnt"] = command_runner(["findmnt", "--json"])
    commands["zpool_status"] = command_runner(["zpool", "status", "-P"])
    commands["pvs"] = command_runner(["pvs", "--reportformat", "json", "-o", "pv_name,vg_name,pv_uuid,pv_used,pv_size"])
    commands["docker_info"] = command_runner(["docker", "info", "--format", "{{json .DockerRootDir}}"])
    reference_commands = {
        name: result
        for name, result in commands.items()
        if name != "lsblk" and not name.startswith("blkid:")
    }
    command_refs = _command_search_references(reference_commands, identifiers)

    active_refs: list[dict[str, Any]] = []
    active_refs.extend({"kind": "lsblk_mountpoint", **item} for item in active_lsblk)
    for kind, rows in proc_refs.items():
        if kind in {"mounts", "swaps", "mdstat"}:
            active_refs.extend({"kind": f"proc_{kind}", **row} for row in rows)
    active_refs.extend({"kind": "command_reference", **row} for row in command_refs)

    warnings: list[str] = []
    for name, result in sorted(commands.items()):
        if result.missing:
            warnings.append(f"{name}_command_missing")
        elif result.timed_out:
            warnings.append(f"{name}_command_timed_out")
        elif result.returncode not in (0, 1, 2):
            warnings.append(f"{name}_command_failed_rc_{result.returncode}")

    windows_layout = _windows_layout_flags(partitions)
    unused = not active_refs and not config_refs
    reason = "unused_by_linux" if unused else "linux_references_found"
    report: dict[str, Any] = {
        "ok": bool(unused),
        "generated_at_epoch": time.time(),
        "device": {
            "requested": requested_device,
            "name": device_name,
            "path": _device_path(device_name),
            "stable_path": stable_paths[0],
            "stable_paths": stable_paths,
            "found": True,
            "size_bytes": target.get("size"),
            "model": target.get("model"),
            "serial": target.get("serial"),
            "transport": target.get("tran"),
            "readonly": target.get("ro"),
        },
        "contents": {
            "partition_count": len(partitions),
            "partitions": partitions,
            "windows_layout": windows_layout,
        },
        "identifiers": identifiers,
        "references": {
            "active": active_refs,
            "config": config_refs,
            "proc": proc_refs,
            "command": command_refs,
        },
        "unused_by_linux": bool(unused),
        "reason": reason,
        "warnings": warnings,
        "commands": {name: result.to_json() for name, result in sorted(commands.items())},
    }
    report["classification"] = classify_assessment(report)
    return report


def build_discovery(
    *,
    runner: Runner | None = None,
    timeout_s: float = 6.0,
    proc_root: Path = Path("/proc"),
    etc_root: Path = Path("/etc"),
    dev_root: Path = Path("/dev"),
) -> dict[str, Any]:
    command_runner = runner or (lambda args: _run_command(args, timeout_s=timeout_s))
    lsblk = command_runner(
        [
            "lsblk",
            "--json",
            "--bytes",
            "--output",
            LSBLK_OUTPUT_COLUMNS,
        ]
    )
    lsblk_json = _parse_json(lsblk.stdout)
    blockdevices = [dict(item) for item in lsblk_json.get("blockdevices") or [] if isinstance(item, Mapping)]
    disks = _local_disk_nodes(blockdevices)
    assessments: list[dict[str, Any]] = []
    for disk in disks:
        disk_name = str(disk.get("name") or disk.get("kname") or "").strip()
        if not disk_name:
            continue
        assessments.append(
            build_assessment(
                device=disk_name,
                runner=runner,
                timeout_s=timeout_s,
                proc_root=proc_root,
                etc_root=etc_root,
                dev_root=dev_root,
            )
        )

    go_candidates = [
        report
        for report in assessments
        if dict(report.get("classification") or {}).get("classification") == "go_candidate"
    ]
    retain_candidates = [
        report
        for report in assessments
        if dict(report.get("classification") or {}).get("classification") == "retain_candidate"
    ]
    if len(go_candidates) == 1:
        status = "go_candidate_found"
        ok = True
    elif len(go_candidates) > 1:
        status = "multiple_go_candidates_require_explicit_target"
        ok = False
    else:
        status = "no_idle_windows_bitlocker_disk_found"
        ok = False
    return {
        "ok": ok,
        "generated_at_epoch": time.time(),
        "mode": "discovery",
        "status": status,
        "disk_count": len(assessments),
        "go_candidate_count": len(go_candidates),
        "retain_candidate_count": len(retain_candidates),
        "requires_explicit_target": len(go_candidates) != 1,
        "candidates": [
            {
                "name": dict(report.get("device") or {}).get("name"),
                "path": dict(report.get("device") or {}).get("path"),
                "stable_path": dict(report.get("device") or {}).get("stable_path"),
                "stable_paths": dict(report.get("device") or {}).get("stable_paths") or [],
                "classification": dict(report.get("classification") or {}).get("classification"),
                "reason": dict(report.get("classification") or {}).get("reason"),
                "reasons": dict(report.get("classification") or {}).get("reasons") or [],
            }
            for report in go_candidates
        ],
        "disks": [
            {
                "device": report.get("device"),
                "contents": report.get("contents"),
                "unused_by_linux": report.get("unused_by_linux"),
                "classification": report.get("classification"),
            }
            for report in assessments
        ],
        "commands": {"lsblk": lsblk.to_json()},
    }


def _print_text(report: Mapping[str, Any]) -> None:
    device = dict(report.get("device") or {})
    contents = dict(report.get("contents") or {})
    windows = dict(contents.get("windows_layout") or {})
    classification = dict(report.get("classification") or {})
    print(f"idle_nvme_assessment device={device.get('path')} ok={str(report.get('ok')).lower()} reason={report.get('reason')}")
    print(
        "classification="
        f"{classification.get('classification', 'unknown')} "
        f"candidate={str(bool(classification.get('candidate'))).lower()} "
        f"classification_reason={classification.get('reason')}"
    )
    print(f"stable_path={device.get('stable_path') or device.get('path')}")
    print(f"unused_by_linux={str(report.get('unused_by_linux')).lower()}")
    print(f"windows_bitlocker_layout_likely={str(windows.get('windows_bitlocker_layout_likely', False)).lower()}")
    print(f"partition_count={contents.get('partition_count', 0)}")
    for part in contents.get("partitions") or []:
        if not isinstance(part, Mapping):
            continue
        mountpoints = ",".join(_as_mountpoints(part.get("mountpoints"))) or "-"
        print(
            "partition "
            f"path={part.get('path')} size_bytes={part.get('size_bytes')} "
            f"fstype={part.get('fstype') or '-'} label={part.get('label') or '-'} "
            f"partlabel={shlex.quote(str(part.get('partlabel') or '-'))} mountpoints={mountpoints}"
        )
    references = dict(report.get("references") or {})
    print(f"active_reference_count={len(references.get('active') or [])}")
    print(f"config_reference_count={len(references.get('config') or [])}")
    warnings = list(report.get("warnings") or [])
    if warnings:
        print(f"warnings={','.join(str(item) for item in warnings)}")


def _print_discovery_text(report: Mapping[str, Any]) -> None:
    print(
        "idle_nvme_discovery "
        f"ok={str(bool(report.get('ok'))).lower()} "
        f"status={report.get('status')} "
        f"disk_count={report.get('disk_count')} "
        f"go_candidate_count={report.get('go_candidate_count')} "
        f"retain_candidate_count={report.get('retain_candidate_count')}"
    )
    for disk in report.get("disks") or []:
        if not isinstance(disk, Mapping):
            continue
        device = dict(disk.get("device") or {})
        classification = dict(disk.get("classification") or {})
        contents = dict(disk.get("contents") or {})
        windows = dict(contents.get("windows_layout") or {})
        print(
            "disk "
            f"name={device.get('name')} "
            f"path={device.get('path')} "
            f"stable_path={device.get('stable_path') or device.get('path')} "
            f"classification={classification.get('classification')} "
            f"reason={classification.get('reason')} "
            f"windows_bitlocker_layout_likely={str(bool(windows.get('windows_bitlocker_layout_likely'))).lower()} "
            f"unused_by_linux={str(bool(disk.get('unused_by_linux'))).lower()}"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only assessment for an idle NVMe candidate.")
    parser.add_argument("--device", default=os.environ.get("IDLE_NVME_DEVICE", DEFAULT_DEVICE))
    parser.add_argument("--discover", action="store_true", help="Inventory local disks and classify idle Windows/BitLocker candidates.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON only.")
    parser.add_argument("--timeout-s", type=float, default=float(os.environ.get("IDLE_NVME_TIMEOUT_S", "6")))
    parser.add_argument("--proc-root", default=os.environ.get("IDLE_NVME_PROC_ROOT", "/proc"))
    parser.add_argument("--etc-root", default=os.environ.get("IDLE_NVME_ETC_ROOT", "/etc"))
    parser.add_argument("--dev-root", default=os.environ.get("IDLE_NVME_DEV_ROOT", "/dev"))
    args = parser.parse_args(argv)

    if args.discover:
        discovery = build_discovery(
            timeout_s=max(0.5, float(args.timeout_s)),
            proc_root=Path(args.proc_root),
            etc_root=Path(args.etc_root),
            dev_root=Path(args.dev_root),
        )
        if args.json:
            print(json.dumps(discovery, indent=2, sort_keys=True))
        else:
            _print_discovery_text(discovery)
        if int(discovery.get("go_candidate_count") or 0) > 1:
            return 2
        return 0 if discovery.get("ok") else 1

    report = build_assessment(
        device=args.device,
        timeout_s=max(0.5, float(args.timeout_s)),
        proc_root=Path(args.proc_root),
        etc_root=Path(args.etc_root),
        dev_root=Path(args.dev_root),
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        _print_text(report)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
