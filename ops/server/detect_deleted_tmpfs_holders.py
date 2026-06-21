#!/usr/bin/env python3
from __future__ import annotations

"""Read-only detector for deleted-but-open files under tmpfs paths."""

import argparse
import json
import os
import pwd
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class DeletedFileHolder:
    pid: int
    user: str
    command: str
    fd: str
    size_bytes: int
    target: str
    source: str


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def _user_for_pid(pid_dir: Path) -> str:
    try:
        uid = pid_dir.stat().st_uid
    except OSError:
        return "?"
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def _command_for_pid(pid_dir: Path) -> str:
    comm = _read_text(pid_dir / "comm")
    if comm:
        return comm
    raw = (pid_dir / "cmdline")
    try:
        data = raw.read_bytes().replace(b"\x00", b" ").strip()
    except OSError:
        return "?"
    return data.decode("utf-8", errors="replace") or "?"


def _clean_deleted_target(target: str) -> str:
    suffix = " (deleted)"
    if target.endswith(suffix):
        return target[: -len(suffix)]
    return target.replace(suffix, "")


def _is_under_path(target: str, root: Path) -> bool:
    clean = _clean_deleted_target(target)
    try:
        clean_path = Path(clean).resolve(strict=False)
    except OSError:
        return False
    root_path = root.resolve(strict=False)
    return clean_path == root_path or root_path in clean_path.parents


def scan_proc_deleted_tmp(path: str | os.PathLike[str] = "/tmp", proc_root: str | os.PathLike[str] = "/proc") -> list[DeletedFileHolder]:
    """Return deleted open file descriptors whose original path is under path."""

    root = Path(path)
    proc = Path(proc_root)
    holders: list[DeletedFileHolder] = []
    try:
        pid_dirs = list(proc.iterdir())
    except OSError:
        return holders

    for pid_dir in pid_dirs:
        if not pid_dir.name.isdigit():
            continue
        fd_dir = pid_dir / "fd"
        try:
            fd_entries = list(fd_dir.iterdir())
        except OSError:
            continue

        pid = int(pid_dir.name)
        user = _user_for_pid(pid_dir)
        command = _command_for_pid(pid_dir)
        for fd_entry in fd_entries:
            try:
                target = os.readlink(fd_entry)
            except OSError:
                continue
            if " (deleted)" not in target or not _is_under_path(target, root):
                continue
            try:
                size = fd_entry.stat().st_size
            except OSError:
                size = 0
            holders.append(
                DeletedFileHolder(
                    pid=pid,
                    user=user,
                    command=command,
                    fd=fd_entry.name,
                    size_bytes=int(size),
                    target=target,
                    source="proc-fd",
                )
            )
    return sorted(holders, key=lambda item: (-item.size_bytes, item.pid, item.fd))


def run_lsof(path: str) -> tuple[int | None, str, str]:
    """Run lsof +L1 for operator evidence when lsof is installed."""

    try:
        proc = subprocess.run(
            ["lsof", "-nP", "+L1", "--", path],
            text=True,
            capture_output=True,
            check=False,
        )
    except FileNotFoundError:
        return None, "", "lsof not installed"
    return proc.returncode, proc.stdout, proc.stderr


def format_table(holders: list[DeletedFileHolder]) -> str:
    if not holders:
        return "No deleted open files found under the requested path."

    rows = [
        ("PID", "USER", "COMMAND", "FD", "SIZE_BYTES", "TARGET"),
        *[
            (
                str(item.pid),
                item.user,
                item.command,
                item.fd,
                str(item.size_bytes),
                item.target,
            )
            for item in holders
        ],
    ]
    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    lines: list[str] = []
    for index, row in enumerate(rows):
        line = "  ".join(value.ljust(widths[col]) for col, value in enumerate(row))
        lines.append(line.rstrip())
        if index == 0:
            lines.append("  ".join("-" * width for width in widths))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Find deleted-but-open files under /tmp or another tmpfs path without modifying processes."
    )
    parser.add_argument("--path", default="/tmp", help="Path to inspect, default: /tmp")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    parser.add_argument("--skip-lsof", action="store_true", help="Skip the lsof +L1 evidence section")
    parser.add_argument("--fail-if-found", action="store_true", help="Return exit code 2 when holders are found")
    args = parser.parse_args(argv)

    holders = scan_proc_deleted_tmp(args.path)
    if args.json:
        payload = {"path": args.path, "holders": [asdict(item) for item in holders]}
        if not args.skip_lsof:
            rc, stdout, stderr = run_lsof(args.path)
            payload["lsof"] = {"returncode": rc, "stdout": stdout, "stderr": stderr}
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        if not args.skip_lsof:
            rc, stdout, stderr = run_lsof(args.path)
            print("lsof +L1 evidence:")
            if rc is None:
                print(f"  {stderr}")
            elif stdout.strip():
                print(stdout.rstrip())
            elif stderr.strip():
                print(stderr.rstrip())
            else:
                print(f"  no lsof rows reported (exit {rc})")
            print()
        print("proc fd scan:")
        print(format_table(holders))

    if args.fail_if_found and holders:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
