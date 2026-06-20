"""Move ignored local runtime outputs into the repository ``var/`` layout."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
VAR = ROOT / "var"


def _tracked_paths() -> set[Path]:
    try:
        proc = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
        )
    except Exception:
        return set()
    out: set[Path] = set()
    for raw in proc.stdout.split(b"\0"):
        if not raw:
            continue
        out.add((ROOT / raw.decode("utf-8", "ignore")).resolve())
    return out


def _contains_tracked(path: Path, tracked: set[Path]) -> bool:
    resolved = path.resolve()
    if resolved in tracked:
        return True
    if not path.is_dir():
        return False
    return any(str(item).startswith(str(resolved) + "/") for item in tracked)


def _iter_root_dbs() -> Iterable[Path]:
    for pattern in ("*.db", "*.sqlite", "*.sqlite3"):
        yield from ROOT.glob(pattern)


def _iter_legacy_data_dbs() -> Iterable[Path]:
    data_root = ROOT / "data"
    for pattern in ("*.db", "*.sqlite", "*.sqlite3"):
        yield from data_root.glob(pattern)


def _available_conflict_dest(dest: Path) -> Path | None:
    for idx in range(1, 10_000):
        candidate = dest.with_name(f"{dest.name}.legacy{idx}")
        if not candidate.exists():
            return candidate
    return None


def _move_one(src: Path, dest: Path, *, dry_run: bool, tracked: set[Path]) -> tuple[str, Path]:
    if not src.exists():
        return "missing", dest
    if _contains_tracked(src, tracked):
        return "tracked_skip", dest
    status = "moved"
    target = dest
    if dest.exists():
        target = _available_conflict_dest(dest)
        if target is None:
            return "exists_skip", dest
        status = "moved_conflict"
    if not dry_run:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(target))
    return status, target


def _move_dir_contents(src: Path, dest: Path, *, dry_run: bool, tracked: set[Path]) -> list[tuple[str, Path, Path]]:
    rows: list[tuple[str, Path, Path]] = []
    if not src.exists():
        return rows
    if _contains_tracked(src, tracked):
        rows.append(("tracked_skip", src, dest))
        return rows
    if not dry_run:
        dest.mkdir(parents=True, exist_ok=True)
    for child in sorted(src.iterdir(), key=lambda p: p.name):
        target = dest / child.name
        status, moved_to = _move_one(child, target, dry_run=dry_run, tracked=tracked)
        rows.append((status, child, moved_to))
    if not dry_run:
        try:
            src.rmdir()
        except OSError:
            pass
    return rows


def migrate(*, dry_run: bool = False) -> list[tuple[str, Path, Path]]:
    tracked = _tracked_paths()
    rows: list[tuple[str, Path, Path]] = []
    dir_moves = (
        (ROOT / "logs", VAR / "log"),
        (ROOT / "tmp", VAR / "tmp"),
        (ROOT / ".run-audit", VAR / "audit"),
        (ROOT / "artifacts", VAR / "artifacts"),
        (ROOT / "models", VAR / "artifacts" / "models"),
        (ROOT / "data" / "operator", VAR / "tmp" / "operator"),
        (ROOT / "data" / "runtime", VAR / "db" / "runtime"),
        (ROOT / "data" / "artifacts", VAR / "artifacts"),
        (ROOT / "data" / "retraining", VAR / "artifacts" / "retraining"),
    )
    for src, dest in dir_moves:
        rows.extend(_move_dir_contents(src, dest, dry_run=dry_run, tracked=tracked))
    for src in sorted(_iter_root_dbs(), key=lambda p: p.name):
        dest = VAR / "db" / src.name
        status, moved_to = _move_one(src, dest, dry_run=dry_run, tracked=tracked)
        rows.append((status, src, moved_to))
    for src in sorted(_iter_legacy_data_dbs(), key=lambda p: p.name):
        dest = VAR / "db" / src.name
        status, moved_to = _move_one(src, dest, dry_run=dry_run, tracked=tracked)
        rows.append((status, src, moved_to))
    secret_path = ROOT / "data" / ".data_source_master_key"
    status, moved_to = _move_one(
        secret_path,
        VAR / "db" / ".data_source_master_key",
        dry_run=dry_run,
        tracked=tracked,
    )
    rows.append((status, secret_path, moved_to))
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print planned moves without changing files.")
    args = parser.parse_args(argv)
    rows = migrate(dry_run=bool(args.dry_run))
    for status, src, dest in rows:
        if status == "missing":
            continue
        print(f"{status}: {src.relative_to(ROOT)} -> {dest.relative_to(ROOT)}")
    return 0 if all(status != "tracked_skip" for status, _src, _dest in rows) else 2


if __name__ == "__main__":
    raise SystemExit(main())
