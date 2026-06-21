from __future__ import annotations

"""Non-destructive git worktree triage for large dirty workspaces.

The repository root is expected to be the canonical checkout.  The default
duplicate target is the historical sibling
``../system-disk-retention-hardening``.
"""

import argparse
import json
import shlex
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DUPLICATE = ROOT.parent / "system-disk-retention-hardening"
GENERATED_MARKERS = (
    "__pycache__/",
    ".pytest_cache/",
    ".claude/",
    ".venv/",
    ".venv-",
    "node_modules/",
    "var/",
    "logs/",
    "tmp/",
    ".run-audit/",
    "data/retraining/",
    "docs/system_audit_layer1",
    "docs/job_migration_l",
    "trading_system.egg-info/",
    ".coverage",
)
RUNTIME_SUFFIXES = (
    ".db",
    ".sqlite",
    ".sqlite3",
    ".db-wal",
    ".db-shm",
    ".log",
    ".pid",
    ".tmp",
)
SOURCE_PREFIXES = (
    ".dockerignore",
    ".gitignore",
    ".github/",
    "boot/",
    "dashboard_server.py",
    "engine/",
    "ops/",
    "routes/",
    "services/",
    "scripts/",
    "start_",
    "tests/",
    "tools/",
    "ui/",
    "ruff.toml",
)
DOC_PREFIXES = ("README", "docs/", "deploy/", "boot/README")
LOCAL_CONFIG_NAMES = {".env", ".npmrc"}


def _run(
    args: list[str],
    *,
    cwd: Path,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=str(cwd), check=check, text=True, capture_output=True)


def _git(args: list[str], *, cwd: Path = ROOT, check: bool = True) -> str:
    proc = _run(["git", *args], cwd=cwd, check=check)
    return proc.stdout.strip()


def _shell_join(args: Iterable[str]) -> str:
    return " ".join(shlex.quote(str(arg)) for arg in args)


def _safe_git(args: list[str], *, cwd: Path) -> str:
    try:
        return _git(args, cwd=cwd)
    except Exception:
        return ""


def _normalize_path(path: Path) -> str:
    try:
        return str(path.resolve())
    except OSError:
        return str(path.absolute())


def _worktree_entries(root: Path = ROOT) -> list[dict[str, str]]:
    try:
        raw = _git(["worktree", "list", "--porcelain"], cwd=root)
    except Exception:
        return []
    entries: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in raw.splitlines():
        if line.startswith("worktree "):
            if current:
                entries.append(current)
            current = {"path": line.split(" ", 1)[1].strip()}
            continue
        if not current:
            continue
        if line.startswith("HEAD "):
            current["head"] = line.split(" ", 1)[1].strip()
        elif line.startswith("branch "):
            branch = line.split(" ", 1)[1].strip()
            current["branch_ref"] = branch
            current["branch"] = branch.removeprefix("refs/heads/")
        elif line == "detached":
            current["detached"] = "true"
        elif line == "bare":
            current["bare"] = "true"
    if current:
        entries.append(current)
    return entries


def _registered_worktree(path: Path, root: Path = ROOT) -> dict[str, str] | None:
    target = _normalize_path(path)
    for entry in _worktree_entries(root):
        if _normalize_path(Path(entry.get("path", ""))) == target:
            return entry
    return None


def _git_status(root: Path) -> List[Dict[str, str]]:
    try:
        proc = _run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=root,
            check=True,
        )
    except Exception:
        return []
    rows: List[Dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if not line:
            continue
        status = line[:2]
        path = line[3:].strip()
        if " -> " in path:
            path = path.split(" -> ", 1)[1].strip()
        rows.append({"status": status, "path": path.replace("\\", "/")})
    return rows


def _ignored_rows(root: Path) -> list[dict[str, str]]:
    try:
        proc = _run(["git", "clean", "-ndX"], cwd=root, check=True)
    except Exception:
        return []
    rows: list[dict[str, str]] = []
    for raw in proc.stdout.splitlines():
        prefix = "Would remove "
        if raw.startswith(prefix):
            path = raw[len(prefix) :].strip().replace("\\", "/")
            rows.append({"status": "ignored", "path": path})
    return rows


def _category(path: str) -> str:
    lower = path.lower()
    name = lower.rstrip("/")
    if lower == ".env.example":
        return "docs"
    if name in LOCAL_CONFIG_NAMES:
        return "secrets_or_local_config"
    if any(marker in lower for marker in GENERATED_MARKERS) or lower.endswith(RUNTIME_SUFFIXES):
        return "generated_or_runtime"
    if any(lower.startswith(prefix.lower()) for prefix in DOC_PREFIXES) or lower.endswith((".md", ".txt")):
        return "docs"
    if lower.startswith(".env") or "secret" in lower or "credential" in lower:
        return "secrets_or_local_config"
    if any(lower.startswith(prefix.lower()) for prefix in SOURCE_PREFIXES) or lower.endswith(
        (".py", ".js", ".css", ".html", ".json", ".toml", ".yml", ".yaml", ".sh")
    ):
        return "source_or_tests"
    return "needs_review"


def _status_report(rows: list[dict[str, str]], *, sample_limit: int = 12) -> dict[str, Any]:
    by_status = Counter(row["status"] for row in rows)
    by_category = Counter(_category(row["path"]) for row in rows)
    samples: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        cat = _category(row["path"])
        if len(samples[cat]) < sample_limit:
            samples[cat].append(row)
    return {
        "total": len(rows),
        "by_status": dict(sorted(by_status.items())),
        "by_category": dict(sorted(by_category.items())),
        "samples": {key: value for key, value in sorted(samples.items())},
    }


def _du_sh(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        proc = _run(["du", "-sh", str(path)], cwd=path.parent, check=True)
    except Exception:
        return None
    return proc.stdout.splitlines()[0].split()[0] if proc.stdout.strip() else None


def _commit_lines(rev_range: str, *, root: Path, limit: int = 20) -> list[str]:
    output = _safe_git(["log", "--oneline", "--decorate", "--no-merges", f"-n{limit}", rev_range], cwd=root)
    return [line.strip() for line in output.splitlines() if line.strip()]


def _ahead_behind(left: str, right: str, *, root: Path) -> dict[str, Any]:
    output = _safe_git(["rev-list", "--left-right", "--count", f"{left}...{right}"], cwd=root)
    if not output:
        return {"left_only": None, "right_only": None}
    parts = output.split()
    if len(parts) < 2:
        return {"left_only": None, "right_only": None}
    return {"left_only": int(parts[0]), "right_only": int(parts[1])}


def _branch_name(root: Path) -> str:
    return _safe_git(["branch", "--show-current"], cwd=root)


def _head(root: Path) -> str:
    return _safe_git(["rev-parse", "--verify", "HEAD"], cwd=root)


def _relationship_report(
    *,
    canonical_root: Path,
    canonical_branch: str,
    duplicate_branch: str,
) -> dict[str, Any]:
    if not canonical_branch or not duplicate_branch:
        return {
            "base_ref": canonical_branch,
            "duplicate_ref": duplicate_branch,
            "available": False,
            "reason": "missing branch name",
        }
    counts = _ahead_behind(canonical_branch, duplicate_branch, root=canonical_root)
    left_only = counts.get("left_only")
    right_only = counts.get("right_only")
    return {
        "base_ref": canonical_branch,
        "duplicate_ref": duplicate_branch,
        "available": left_only is not None and right_only is not None,
        "canonical_only_commit_count": left_only,
        "duplicate_only_commit_count": right_only,
        "already_merged_into_canonical": right_only == 0,
        "stale_relative_to_canonical": bool(left_only),
        "duplicate_unique_commits": _commit_lines(f"{canonical_branch}..{duplicate_branch}", root=canonical_root),
        "canonical_unique_commits": _commit_lines(f"{duplicate_branch}..{canonical_branch}", root=canonical_root),
    }


def _removal_report(
    *,
    duplicate: Path,
    registered: bool,
    status_rows: list[dict[str, str]],
    ignored_rows: list[dict[str, str]],
) -> dict[str, Any]:
    blockers: list[str] = []
    ignored_categories = Counter(_category(row["path"]) for row in ignored_rows)
    if not duplicate.exists():
        blockers.append("duplicate path does not exist")
    if duplicate.exists() and not registered:
        blockers.append("duplicate path is not a registered git worktree")
    if status_rows:
        blockers.append("duplicate worktree still has Git-visible tracked or untracked changes")
    if ignored_categories.get("secrets_or_local_config"):
        blockers.append("ignored local config or secret-shaped files would be deleted")
    if ignored_categories.get("needs_review"):
        blockers.append("ignored files need manual review before deletion")

    dry_run = ["python", "tools/git_worktree_triage.py", "--remove-duplicate", "--duplicate", str(duplicate)]
    execute = ["git", "worktree", "remove", "--force", str(duplicate)]
    return {
        "ready": not blockers,
        "blockers": blockers,
        "dry_run_command": _shell_join(dry_run),
        "execute_command_after_owner_confirmation": _shell_join(execute),
        "ignored_summary": dict(sorted(ignored_categories.items())),
        "note": (
            "Default to the dry-run command. Use the execute command only after "
            "the report has no blockers or the owner explicitly confirms ignored "
            "local/runtime state is disposable."
        ),
    }


def build_report(
    *,
    canonical_root: Path = ROOT,
    duplicate_path: Path = DEFAULT_DUPLICATE,
    base_ref: str | None = None,
) -> Dict[str, object]:
    canonical_root = canonical_root.resolve()
    duplicate_path = duplicate_path.resolve()
    canonical_rows = _git_status(canonical_root)
    duplicate_rows = _git_status(duplicate_path) if duplicate_path.exists() else []
    ignored = _ignored_rows(duplicate_path) if duplicate_path.exists() else []
    registered_entry = _registered_worktree(duplicate_path, canonical_root)
    duplicate_branch = ""
    duplicate_head = ""
    if duplicate_path.exists() and (duplicate_path / ".git").exists():
        duplicate_branch = _branch_name(duplicate_path)
        duplicate_head = _head(duplicate_path)
    if registered_entry:
        duplicate_branch = duplicate_branch or registered_entry.get("branch", "")
        duplicate_head = duplicate_head or registered_entry.get("head", "")
    canonical_branch = base_ref or _branch_name(canonical_root)

    relationship = _relationship_report(
        canonical_root=canonical_root,
        canonical_branch=canonical_branch,
        duplicate_branch=duplicate_branch,
    )
    removal = _removal_report(
        duplicate=duplicate_path,
        registered=bool(registered_entry),
        status_rows=duplicate_rows,
        ignored_rows=ignored,
    )
    violations = layout_violations(canonical_root=canonical_root, duplicate_path=duplicate_path)
    return {
        "ok": not violations,
        "canonical": {
            "path": str(canonical_root),
            "branch": _branch_name(canonical_root),
            "head": _head(canonical_root),
            "status": _status_report(canonical_rows),
        },
        "duplicate": {
            "path": str(duplicate_path),
            "exists": duplicate_path.exists(),
            "registered_worktree": bool(registered_entry),
            "registered_entry": registered_entry,
            "branch": duplicate_branch,
            "head": duplicate_head,
            "disk_usage": _du_sh(duplicate_path),
            "status": _status_report(duplicate_rows),
            "ignored": _status_report(ignored),
            "relationship_to_canonical": relationship,
        },
        "layout_violations": violations,
        "removal": removal,
        "recommended_action": (
            "Preserve Git-visible duplicate work on a named branch before removal. "
            "Run the dry-run removal command first; delete only after blockers are "
            "resolved or explicitly accepted by the owner."
        ),
    }


def layout_violations(
    *,
    canonical_root: Path = ROOT,
    duplicate_path: Path = DEFAULT_DUPLICATE,
) -> list[str]:
    """Return release-blocking canonical worktree layout violations."""

    duplicate_path = duplicate_path.resolve()
    if not duplicate_path.exists():
        return []
    entry = _registered_worktree(duplicate_path, canonical_root)
    if not entry:
        return [f"{duplicate_path} exists but is not a registered git worktree"]
    rows = _git_status(duplicate_path)
    if rows:
        return [f"{duplicate_path} has {len(rows)} Git-visible tracked/untracked change(s)"]
    return []


def _remove_duplicate(
    *,
    canonical_root: Path,
    duplicate_path: Path,
    execute: bool,
    allow_ignored_local_state: bool,
) -> int:
    report = build_report(
        canonical_root=canonical_root,
        duplicate_path=duplicate_path,
    )
    removal = dict(report.get("removal") or {})
    blockers = list(removal.get("blockers") or [])
    if allow_ignored_local_state:
        blockers = [
            item
            for item in blockers
            if item
            not in {
                "ignored local config or secret-shaped files would be deleted",
                "ignored files need manual review before deletion",
            }
        ]
    command = ["git", "worktree", "remove", "--force", str(duplicate_path.resolve())]
    if not execute:
        result = {
            "dry_run": True,
            "would_execute": _shell_join(command),
            "blockers": blockers,
            "message": "No files were removed. Re-run with --execute-removal only after blockers are resolved or explicitly accepted.",
        }
        report["removal"] = {**removal, "ready": not blockers, "blockers": blockers}
        print(json.dumps({"ok": not blockers, "removal_dry_run": result, "report": report}, indent=2, sort_keys=True))
        return 0

    if blockers:
        report["removal"] = {**removal, "ready": False, "blockers": blockers}
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2

    proc = _run(command, cwd=canonical_root, check=False)
    result = {
        "command": _shell_join(command),
        "exit_code": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "executed": execute,
    }
    print(json.dumps({"ok": proc.returncode == 0, "removal_result": result}, indent=2, sort_keys=True))
    return proc.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duplicate",
        type=Path,
        default=DEFAULT_DUPLICATE,
        help="Sibling duplicate worktree/directory to triage.",
    )
    parser.add_argument(
        "--base-ref",
        default=None,
        help="Canonical branch/ref to compare against; defaults to the current branch.",
    )
    parser.add_argument(
        "--remove-duplicate",
        action="store_true",
        help="Run the guarded worktree removal path. Defaults to a dry run.",
    )
    parser.add_argument(
        "--execute-removal",
        action="store_true",
        help="Actually remove the duplicate worktree. Requires --remove-duplicate.",
    )
    parser.add_argument(
        "--allow-ignored-local-state",
        action="store_true",
        help="Allow actual removal even when ignored local/runtime files are present.",
    )
    args = parser.parse_args(argv)

    if args.execute_removal and not args.remove_duplicate:
        parser.error("--execute-removal requires --remove-duplicate")

    duplicate = args.duplicate
    if not duplicate.is_absolute():
        duplicate = (ROOT / duplicate).resolve()
    if args.remove_duplicate:
        return _remove_duplicate(
            canonical_root=ROOT,
            duplicate_path=duplicate,
            execute=args.execute_removal,
            allow_ignored_local_state=args.allow_ignored_local_state,
        )

    report = build_report(
        canonical_root=ROOT,
        duplicate_path=duplicate,
        base_ref=args.base_ref,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
