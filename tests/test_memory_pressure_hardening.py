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
