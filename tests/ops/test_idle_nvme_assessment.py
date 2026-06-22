from __future__ import annotations

import json
from pathlib import Path

from tools.idle_nvme_assessment import CommandResult, build_assessment, build_discovery


def _lsblk(stdout: dict) -> CommandResult:
    return CommandResult(("lsblk",), 0, stdout=json.dumps(stdout))


def _runner(stdout: dict):
    def run(args):
        if args and args[0] == "lsblk":
            return _lsblk(stdout)
        if args and args[0] in {"blkid", "findmnt", "zpool", "pvs", "docker"}:
            return CommandResult(tuple(args), 1, stdout="", stderr="")
        return CommandResult(tuple(args), 127, stderr="missing", missing=True)

    return run


def _windows_disk(name: str = "nvme0n1", *, mountpoints=None) -> dict:
    part_prefix = f"{name}p" if name[-1].isdigit() else name
    return {
        "name": name,
        "kname": name,
        "path": f"/dev/{name}",
        "type": "disk",
        "size": 2000398934016,
        "tran": "nvme",
        "children": [
            {
                "name": f"{part_prefix}1",
                "kname": f"{part_prefix}1",
                "path": f"/dev/{part_prefix}1",
                "type": "part",
                "size": 104857600,
                "fstype": "vfat",
                "parttype": "c12a7328-f81f-11d2-ba4b-00a0c93ec93b",
                "partlabel": "EFI system partition",
                "mountpoints": [],
            },
            {
                "name": f"{part_prefix}2",
                "kname": f"{part_prefix}2",
                "path": f"/dev/{part_prefix}2",
                "type": "part",
                "size": 16777216,
                "parttype": "e3c9e316-0b5c-4db8-817d-f92df00215ae",
                "partlabel": "Microsoft reserved partition",
                "mountpoints": [],
            },
            {
                "name": f"{part_prefix}3",
                "kname": f"{part_prefix}3",
                "path": f"/dev/{part_prefix}3",
                "type": "part",
                "size": 1999000000000,
                "fstype": "BitLocker",
                "label": "Windows",
                "uuid": f"BITLOCKER-UUID-{name}",
                "partuuid": f"PART-UUID-{name}-3",
                "parttype": "ebd0a0a2-b9e5-4433-87c0-68b6b72699c7",
                "partlabel": "Basic data partition",
                "mountpoints": mountpoints or [],
            },
            {
                "name": f"{part_prefix}4",
                "kname": f"{part_prefix}4",
                "path": f"/dev/{part_prefix}4",
                "type": "part",
                "size": 800000000,
                "fstype": "ntfs",
                "label": "Recovery",
                "parttype": "de94bba4-06d1-4d40-a16a-bfd50179d6ac",
                "mountpoints": [],
            },
        ],
    }


def _windows_bitlocker_lsblk(*, mountpoints=None) -> dict:
    return {"blockdevices": [_windows_disk(mountpoints=mountpoints)]}


def _linux_root_zfs_disk(name: str = "nvme0n1") -> dict:
    return {
        "name": name,
        "kname": name,
        "path": f"/dev/{name}",
        "type": "disk",
        "size": 4000000000000,
        "tran": "nvme",
        "children": [
            {
                "name": f"{name}p1",
                "kname": f"{name}p1",
                "path": f"/dev/{name}p1",
                "type": "part",
                "size": 1048576000,
                "fstype": "vfat",
                "label": "efi",
                "mountpoints": ["/boot/efi"],
            },
            {
                "name": f"{name}p2",
                "kname": f"{name}p2",
                "path": f"/dev/{name}p2",
                "type": "part",
                "size": 524288000000,
                "fstype": "ext4",
                "label": "root",
                "mountpoints": ["/"],
            },
            {
                "name": f"{name}p3",
                "kname": f"{name}p3",
                "path": f"/dev/{name}p3",
                "type": "part",
                "size": 262144000000,
                "fstype": "ext4",
                "label": "home",
                "mountpoints": ["/home"],
            },
            {
                "name": f"{name}p4",
                "kname": f"{name}p4",
                "path": f"/dev/{name}p4",
                "type": "part",
                "size": 3213304528896,
                "fstype": "zfs_member",
                "label": "zpool",
                "mountpoints": [],
            },
        ],
    }


def _write_proc_defaults(tmp_path: Path) -> tuple[Path, Path]:
    proc_root = tmp_path / "proc"
    etc_root = tmp_path / "etc"
    proc_root.mkdir()
    etc_root.mkdir()
    (proc_root / "mounts").write_text("/dev/nvme9n1p1 / ext4 rw 0 0\n", encoding="utf-8")
    (proc_root / "swaps").write_text("Filename Type Size Used Priority\n", encoding="utf-8")
    (proc_root / "mdstat").write_text("", encoding="utf-8")
    (etc_root / "fstab").write_text("UUID=root / ext4 defaults 0 1\n", encoding="utf-8")
    (etc_root / "crypttab").write_text("", encoding="utf-8")
    return proc_root, etc_root


def test_assessment_confirms_unused_windows_bitlocker_layout(tmp_path: Path) -> None:
    proc_root, etc_root = _write_proc_defaults(tmp_path)
    dev_root = tmp_path / "dev"
    by_id = dev_root / "disk" / "by-id"
    by_id.mkdir(parents=True)
    (by_id / "nvme-Windows_Device").symlink_to("../../nvme0n1")

    report = build_assessment(
        device="nvme0n1",
        runner=_runner(_windows_bitlocker_lsblk()),
        proc_root=proc_root,
        etc_root=etc_root,
        dev_root=dev_root,
    )

    assert report["ok"] is True
    assert report["unused_by_linux"] is True
    assert report["reason"] == "unused_by_linux"
    assert report["contents"]["partition_count"] == 4
    assert report["contents"]["windows_layout"]["windows_bitlocker_layout_likely"] is True
    assert report["classification"]["classification"] == "go_candidate"
    assert report["device"]["stable_path"].endswith("/disk/by-id/nvme-Windows_Device")


def test_assessment_blocks_when_partition_is_mounted(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    etc_root = tmp_path / "etc"
    proc_root.mkdir()
    etc_root.mkdir()
    (proc_root / "mounts").write_text("/dev/nvme0n1p3 /mnt/windows fuseblk rw 0 0\n", encoding="utf-8")
    (proc_root / "swaps").write_text("Filename Type Size Used Priority\n", encoding="utf-8")
    (proc_root / "mdstat").write_text("", encoding="utf-8")

    report = build_assessment(
        device="nvme0n1",
        runner=_runner(_windows_bitlocker_lsblk(mountpoints=["/mnt/windows"])),
        proc_root=proc_root,
        etc_root=etc_root,
        dev_root=tmp_path / "dev",
    )

    assert report["ok"] is False
    assert report["unused_by_linux"] is False
    assert report["reason"] == "linux_references_found"
    assert report["references"]["active"]


def test_assessment_blocks_when_config_references_partition_uuid(tmp_path: Path) -> None:
    proc_root = tmp_path / "proc"
    etc_root = tmp_path / "etc"
    proc_root.mkdir()
    etc_root.mkdir()
    (proc_root / "mounts").write_text("", encoding="utf-8")
    (proc_root / "swaps").write_text("Filename Type Size Used Priority\n", encoding="utf-8")
    (proc_root / "mdstat").write_text("", encoding="utf-8")
    (etc_root / "fstab").write_text("PARTUUID=PART-UUID-nvme0n1-3 /mnt/old ntfs defaults 0 0\n", encoding="utf-8")

    report = build_assessment(
        device="nvme0n1",
        runner=_runner(_windows_bitlocker_lsblk()),
        proc_root=proc_root,
        etc_root=etc_root,
        dev_root=tmp_path / "dev",
    )

    assert report["ok"] is False
    assert report["unused_by_linux"] is False
    assert report["references"]["config"][0]["source"].endswith("fstab")
    assert report["classification"]["classification"] == "retain_candidate"


def test_discovery_rejects_root_home_and_zfs_nvme(tmp_path: Path) -> None:
    proc_root, etc_root = _write_proc_defaults(tmp_path)
    report = build_discovery(
        runner=_runner({"blockdevices": [_linux_root_zfs_disk()]}),
        proc_root=proc_root,
        etc_root=etc_root,
        dev_root=tmp_path / "dev",
    )

    assert report["ok"] is False
    assert report["go_candidate_count"] == 0
    disk = report["disks"][0]
    assert disk["classification"]["classification"] == "no_go"
    assert disk["classification"]["reason"] == "critical_linux_mountpoint"
    assert "zfs_member" in disk["classification"]["linux_fstypes"]


def test_discovery_selects_idle_windows_bitlocker_candidate(tmp_path: Path) -> None:
    proc_root, etc_root = _write_proc_defaults(tmp_path)
    dev_root = tmp_path / "dev"
    by_id = dev_root / "disk" / "by-id"
    by_id.mkdir(parents=True)
    (by_id / "nvme-idle-windows").symlink_to("../../nvme1n1")

    report = build_discovery(
        runner=_runner({"blockdevices": [_linux_root_zfs_disk("nvme0n1"), _windows_disk("nvme1n1")]}),
        proc_root=proc_root,
        etc_root=etc_root,
        dev_root=dev_root,
    )

    assert report["ok"] is True
    assert report["go_candidate_count"] == 1
    assert report["candidates"][0]["name"] == "nvme1n1"
    assert report["candidates"][0]["stable_path"].endswith("/disk/by-id/nvme-idle-windows")


def test_discovery_multiple_candidates_require_explicit_selection(tmp_path: Path) -> None:
    proc_root, etc_root = _write_proc_defaults(tmp_path)
    report = build_discovery(
        runner=_runner({"blockdevices": [_windows_disk("nvme1n1"), _windows_disk("nvme2n1")]}),
        proc_root=proc_root,
        etc_root=etc_root,
        dev_root=tmp_path / "dev",
    )

    assert report["ok"] is False
    assert report["status"] == "multiple_go_candidates_require_explicit_target"
    assert report["requires_explicit_target"] is True
    assert report["go_candidate_count"] == 2
