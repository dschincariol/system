from __future__ import annotations

from pathlib import Path


def test_rotate_log_if_needed_uses_local_size_and_backup_defaults(monkeypatch, tmp_path: Path) -> None:
    from engine.runtime.log_retention import rotate_log_if_needed

    log_path = tmp_path / "ingestion_runtime.combined.log"
    log_path.write_text("0123456789", encoding="utf-8")

    monkeypatch.setenv("TRADING_LOCAL_LOG_MAX_BYTES", "8")
    monkeypatch.setenv("TRADING_LOCAL_LOG_BACKUP_COUNT", "2")

    assert rotate_log_if_needed(log_path)
    assert not log_path.exists()
    assert (tmp_path / "ingestion_runtime.combined.log.1").read_text(encoding="utf-8") == "0123456789"

    log_path.write_text("abcdefghij", encoding="utf-8")
    assert rotate_log_if_needed(log_path)
    assert (tmp_path / "ingestion_runtime.combined.log.1").read_text(encoding="utf-8") == "abcdefghij"
    assert (tmp_path / "ingestion_runtime.combined.log.2").read_text(encoding="utf-8") == "0123456789"


def test_rotate_log_if_needed_is_noop_under_size(monkeypatch, tmp_path: Path) -> None:
    from engine.runtime.log_retention import rotate_log_if_needed

    log_path = tmp_path / "engine.log"
    log_path.write_text("small", encoding="utf-8")

    monkeypatch.setenv("TRADING_LOCAL_LOG_MAX_BYTES", "100")
    monkeypatch.setenv("TRADING_LOCAL_LOG_BACKUP_COUNT", "2")

    assert not rotate_log_if_needed(log_path)
    assert log_path.read_text(encoding="utf-8") == "small"
