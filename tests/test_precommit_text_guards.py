from pathlib import Path

from tools.precommit_text_guards import check_path


def test_text_guard_accepts_clean_text(tmp_path: Path) -> None:
    path = tmp_path / "clean.txt"
    path.write_text("alpha\nbeta\n", encoding="utf-8")

    assert check_path(path) == []


def test_text_guard_reports_trailing_whitespace_and_missing_newline(tmp_path: Path) -> None:
    path = tmp_path / "bad.txt"
    path.write_bytes(b"alpha \nbeta")

    assert check_path(path) == [
        f"{path}: EOF: missing final newline",
        f"{path}:1: trailing whitespace",
    ]


def test_text_guard_skips_binary_files(tmp_path: Path) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"\x00alpha ")

    assert check_path(path) == []
