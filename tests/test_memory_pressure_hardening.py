from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops" / "server" / "memory_pressure_hardening.sh"


def _run_script(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    run_env = dict(os.environ)
    if env:
        run_env.update(env)
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        cwd=str(ROOT),
        env=run_env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_memory_pressure_script_is_valid_bash() -> None:
    proc = subprocess.run(["bash", "-n", str(SCRIPT)], text=True, capture_output=True, check=False)

    assert proc.returncode == 0, proc.stderr


def test_memory_pressure_install_renders_idempotent_persistent_config(tmp_path: Path) -> None:
    env = {
        "TRADING_SWAPPINESS": "10",
        "TRADING_ZRAM_SIZE_GIB": "32",
        "TRADING_SWAPFILE_SIZE_GIB": "16",
        "TRADING_ZFS_ARC_MAX_GIB": "48",
    }

    first = _run_script("install", "--root", str(tmp_path), "--no-apply", env=env)
    second = _run_script("install", "--root", str(tmp_path), "--no-apply", env=env)
    verify = _run_script("verify", "--root", str(tmp_path), env=env)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert "unchanged /etc/sysctl.d/zz-trading-memory-pressure.conf" in second.stdout
    assert verify.returncode == 0, verify.stderr
    assert "persisted memory-pressure config verified" in verify.stdout

    sysctl_conf = tmp_path / "etc" / "sysctl.d" / "zz-trading-memory-pressure.conf"
    arc_conf = tmp_path / "etc" / "modprobe.d" / "trading-zfs-arc.conf"
    zram_unit = tmp_path / "etc" / "systemd" / "system" / "trading-zram-swap.service"
    swapfile_unit = tmp_path / "etc" / "systemd" / "system" / "trading-swapfile.service"
    managed_script = tmp_path / "usr" / "local" / "sbin" / "trading-memory-pressure"

    assert "vm.swappiness = 10" in sysctl_conf.read_text(encoding="utf-8")
    assert "zfs_arc_max=51539607552" in arc_conf.read_text(encoding="utf-8")
    assert "TRADING_ZRAM_SIZE_GIB=32" in zram_unit.read_text(encoding="utf-8")
    assert "TRADING_SWAPFILE_SIZE_GIB=16" in swapfile_unit.read_text(encoding="utf-8")
    assert managed_script.exists()
    assert os.access(managed_script, os.X_OK)


def test_memory_pressure_snapshot_accepts_bart_policy() -> None:
    from engine.runtime.memory_pressure import BYTES_IN_GIB, host_memory_pressure_snapshot

    meminfo = "\n".join(
        [
            "MemTotal:       128974848 kB",
            "MemAvailable:   83886080 kB",
            "SwapTotal:      50331648 kB",
            "SwapFree:       50331648 kB",
        ]
    )
    swapon = "\n".join(
        [
            f"/dev/zram0 partition {32 * BYTES_IN_GIB} 0 100",
            f"/swapfile-trading file {16 * BYTES_IN_GIB} 0 10",
        ]
    )

    state = host_memory_pressure_snapshot(
        {"PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY": "1"},
        meminfo_text=meminfo,
        swapon_text=swapon,
        swappiness_text="10",
        zfs_arc_max_text=str(48 * BYTES_IN_GIB),
    )

    assert state["ok"] is True
    assert state["meets_policy"] is True
    assert state["status"] == "pass"
    assert state["swap"]["zram_total_gib"] == 32.0
    assert state["swap"]["managed_swapfile_gib"] == 16.0


def test_memory_pressure_snapshot_accepts_swap_metadata_page_overhead() -> None:
    from engine.runtime.memory_pressure import BYTES_IN_GIB, host_memory_pressure_snapshot

    page = 4096
    meminfo = "\n".join(
        [
            "MemTotal:       128974848 kB",
            "MemAvailable:   83886080 kB",
            "SwapTotal:      50331640 kB",
            "SwapFree:       50331640 kB",
        ]
    )
    swapon = "\n".join(
        [
            f"/dev/zram0 partition {32 * BYTES_IN_GIB - page} 0 100",
            f"/swapfile-trading file {16 * BYTES_IN_GIB - page} 0 10",
        ]
    )

    state = host_memory_pressure_snapshot(
        {"PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY": "1"},
        meminfo_text=meminfo,
        swapon_text=swapon,
        swappiness_text="10",
        zfs_arc_max_text=str(48 * BYTES_IN_GIB),
    )

    assert state["ok"] is True
    assert state["meets_policy"] is True


def test_memory_pressure_snapshot_rejects_512m_swapfile_host() -> None:
    from engine.runtime.memory_pressure import BYTES_IN_GIB, host_memory_pressure_snapshot

    meminfo = "\n".join(
        [
            "MemTotal:       128974848 kB",
            "MemAvailable:   83886080 kB",
            "SwapTotal:      524284 kB",
            "SwapFree:       524284 kB",
        ]
    )
    swapon = "/swapfile file 536866816 0 -2"

    state = host_memory_pressure_snapshot(
        {"PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY": "1"},
        meminfo_text=meminfo,
        swapon_text=swapon,
        swappiness_text="60",
        zfs_arc_max_text=str(48 * BYTES_IN_GIB),
    )

    assert state["ok"] is False
    assert state["status"] == "fail"
    assert "memory_pressure_total_swap_below_policy" in state["errors"]
    assert "memory_pressure_zram_below_policy" in state["errors"]
    assert "memory_pressure_swapfile_below_policy" in state["errors"]
    assert state["memory"]["swap_total_gib"] < 1
