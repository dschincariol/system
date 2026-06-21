from __future__ import annotations

import os
from pathlib import Path

import pytest

from ops.server.detect_deleted_tmpfs_holders import format_table, scan_proc_deleted_tmp


@pytest.mark.linux_only
def test_proc_scan_finds_current_process_deleted_file(tmp_path: Path) -> None:
    held_path = tmp_path / "held-open.bin"
    handle = held_path.open("wb")
    try:
        handle.write(b"x" * 4096)
        handle.flush()
        os.unlink(held_path)

        holders = scan_proc_deleted_tmp(tmp_path)
    finally:
        handle.close()

    assert any(
        item.pid == os.getpid()
        and item.size_bytes >= 4096
        and "held-open.bin" in item.target
        for item in holders
    )


def test_format_table_names_holding_pid(tmp_path: Path) -> None:
    held_path = tmp_path / "held-table.bin"
    handle = held_path.open("wb")
    try:
        handle.write(b"x")
        handle.flush()
        os.unlink(held_path)
        rows = [item for item in scan_proc_deleted_tmp(tmp_path) if item.pid == os.getpid()]
        table = format_table(rows)
    finally:
        handle.close()

    assert "PID" in table
    assert str(os.getpid()) in table
    assert "held-table.bin" in table
