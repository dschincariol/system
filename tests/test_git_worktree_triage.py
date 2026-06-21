from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from tools import git_worktree_triage


def _git(root: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=str(root),
        check=True,
        text=True,
        capture_output=True,
    )
    return proc.stdout.strip()


def _init_repo(root: Path) -> None:
    root.mkdir(parents=True)
    _git(root, "init")
    _git(root, "config", "user.name", "Test")
    _git(root, "config", "user.email", "test@example.invalid")
    (root / "README.md").write_text("canonical\n", encoding="utf-8")
    _git(root, "add", "README.md")
    _git(root, "commit", "-m", "init")


class GitWorktreeTriageTests(unittest.TestCase):
    def test_layout_violations_block_loose_duplicate_directory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            root = parent / "system"
            duplicate = parent / "system-disk-retention-hardening"
            _init_repo(root)
            duplicate.mkdir()
            (duplicate / "README.md").write_text("loose copy\n", encoding="utf-8")

            violations = git_worktree_triage.layout_violations(
                canonical_root=root,
                duplicate_path=duplicate,
            )

        self.assertEqual(
            violations,
            [f"{duplicate} exists but is not a registered git worktree"],
        )

    def test_registered_clean_duplicate_worktree_passes_layout_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            root = parent / "system"
            duplicate = parent / "system-disk-retention-hardening"
            _init_repo(root)
            _git(root, "worktree", "add", "-b", "codex/duplicate", str(duplicate))

            violations = git_worktree_triage.layout_violations(
                canonical_root=root,
                duplicate_path=duplicate,
            )
            report = git_worktree_triage.build_report(
                canonical_root=root,
                duplicate_path=duplicate,
                base_ref=_git(root, "branch", "--show-current"),
            )

        self.assertEqual(violations, [])
        self.assertTrue(report["ok"])
        self.assertTrue(report["duplicate"]["registered_worktree"])
        self.assertEqual(report["duplicate"]["status"]["total"], 0)

    def test_dirty_duplicate_worktree_blocks_removal_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            parent = Path(tmp)
            root = parent / "system"
            duplicate = parent / "system-disk-retention-hardening"
            _init_repo(root)
            _git(root, "worktree", "add", "-b", "codex/duplicate", str(duplicate))
            (duplicate / "README.md").write_text("dirty\n", encoding="utf-8")

            report = git_worktree_triage.build_report(
                canonical_root=root,
                duplicate_path=duplicate,
                base_ref=_git(root, "branch", "--show-current"),
            )

        self.assertFalse(report["ok"])
        self.assertEqual(report["duplicate"]["status"]["total"], 1)
        self.assertIn(
            "duplicate worktree still has Git-visible tracked or untracked changes",
            report["removal"]["blockers"],
        )


if __name__ == "__main__":
    unittest.main()
