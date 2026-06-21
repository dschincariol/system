"""
Fail if generated, runtime, or local-secret artifacts are tracked by git.

The check intentionally inspects tracked files only. Ignored local state may
exist on disk for development and runtime use, but it must not enter commits.
"""

from __future__ import annotations

import argparse
import fnmatch
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

ALLOWED_TRACKED_GLOBS = (
    ".env.example",
    "*.env.example",
    "deploy/compose/.env.example",
    "deploy/env/*.env.example",
)

FORBIDDEN_ANY_COMPONENTS = {
    "__pycache__": "python bytecode cache",
    ".pytest_cache": "pytest cache",
    ".ruff_cache": "ruff cache",
    ".mypy_cache": "mypy cache",
    ".pyre": "pyre cache",
    ".tox": "tox environment",
    ".nox": "nox environment",
    "node_modules": "node dependency directory",
}

FORBIDDEN_ROOT_COMPONENTS = {
    ".venv": "local Python virtual environment",
    "venv": "local Python virtual environment",
    "env": "local Python virtual environment",
    "ENV": "local Python virtual environment",
    "var": "runtime state directory",
    "logs": "runtime log directory",
    "tmp": "temporary runtime directory",
    "temp": "temporary runtime directory",
    ".tmp": "temporary runtime directory",
    ".cache": "local cache directory",
    ".claude": "local assistant configuration",
    ".repo-audit": "local repository audit output",
    "artifacts": "generated artifact output",
    "artifacts-cache": "generated artifact cache",
    "model-artifacts": "generated model artifact output",
    "model_cache": "generated model cache",
    "models": "generated model output",
}

FORBIDDEN_PATH_GLOBS = (
    ".env",
    ".env.*",
    "*.env",
    "*.env.*",
    "**/.env",
    "**/.env.*",
    "**/*.env",
    "**/*.env.*",
    "deploy/env/*.env",
    "deploy/env/*.local",
    "deploy/compose/.env",
    "deploy/compose/.env.*",
    "data/secrets/**",
    "data/.data_source_master_key",
    "data/operator/**",
    "data/runtime/**",
    "data/retraining/**",
    "data/artifacts/**",
    "data/cache/**",
    "data/feature_cache/**",
    "data/model_cache/**",
    "data/models/**",
    "data/processed/**",
    "data/raw/**",
    "data/*.jsonl",
)

FORBIDDEN_SUFFIXES = (
    ".pyc",
    ".pyo",
    ".pyd",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".db-wal",
    ".db-shm",
    ".db-journal",
    ".sqlite-wal",
    ".sqlite-shm",
    ".sqlite-journal",
    ".log",
    ".pid",
    ".tmp",
    ".dump",
    ".prof",
    ".seed",
    ".out",
    ".err",
    ".lock",
    ".orig",
    ".rej",
    ".bak",
)


@dataclass(frozen=True)
class ArtifactViolation:
    path: str
    reason: str


def _normalize(path: str) -> str:
    return path.replace("\\", "/").strip("/")


def _matches_any(path: str, patterns: tuple[str, ...]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def _is_allowed_tracked_path(path: str) -> bool:
    return _matches_any(path, ALLOWED_TRACKED_GLOBS)


def artifact_violation_for_path(path: str) -> ArtifactViolation | None:
    normalized = _normalize(path)
    if not normalized or _is_allowed_tracked_path(normalized):
        return None

    parts = normalized.split("/")
    for part in parts:
        reason = FORBIDDEN_ANY_COMPONENTS.get(part)
        if reason:
            return ArtifactViolation(normalized, reason)

    root_reason = FORBIDDEN_ROOT_COMPONENTS.get(parts[0])
    if root_reason:
        return ArtifactViolation(normalized, root_reason)

    if _matches_any(normalized, FORBIDDEN_PATH_GLOBS):
        return ArtifactViolation(normalized, "local environment, secret, or runtime data path")

    if normalized.endswith(FORBIDDEN_SUFFIXES) or ".log." in normalized:
        return ArtifactViolation(normalized, "generated runtime/cache file suffix")

    return None


def tracked_artifact_violations(paths: list[str]) -> list[ArtifactViolation]:
    violations: list[ArtifactViolation] = []
    for path in paths:
        violation = artifact_violation_for_path(path)
        if violation is not None:
            violations.append(violation)
    return violations


def _git_lines(root: Path, args: list[str], *, nul: bool = False) -> list[str]:
    completed = subprocess.run(
        ["git", *args],
        check=True,
        cwd=str(root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=not nul,
    )
    if nul:
        raw = completed.stdout
        if isinstance(raw, bytes):
            return [item.decode("utf-8", errors="replace") for item in raw.split(b"\0") if item]
        return [item for item in raw.split("\0") if item]
    return [line for line in str(completed.stdout).splitlines() if line]


def tracked_files(root: Path = ROOT) -> list[str]:
    return _git_lines(root, ["ls-files", "-z"], nul=True)


def ignored_status_lines(root: Path = ROOT) -> list[str]:
    return _git_lines(root, ["status", "--ignored", "--short", "--untracked-files=all"])


def _print_report(root: Path, violations: list[ArtifactViolation], *, include_ignored: bool) -> None:
    print("Repository artifact hygiene report")
    print(f"root: {root}")

    tracked = tracked_files(root)
    env_like = [
        path
        for path in tracked
        if _matches_any(_normalize(path), (".env", ".env.*", "*.env", "*.env.*", "**/.env", "**/.env.*", "**/*.env", "**/*.env.*"))
    ]
    print(f"tracked files scanned: {len(tracked)}")
    print(f"tracked env-like files: {len(env_like)}")
    for path in env_like:
        print(f"  tracked env template: {path}")

    if violations:
        print(f"tracked artifact violations: {len(violations)}")
        for violation in violations:
            print(f"  {violation.path}: {violation.reason}")
    else:
        print("tracked artifact violations: 0")

    if include_ignored:
        ignored = [line for line in ignored_status_lines(root) if line.startswith("!! ")]
        print(f"ignored working-tree entries reported by git status --ignored: {len(ignored)}")
        for line in ignored[:40]:
            print(f"  {line}")
        if len(ignored) > 40:
            print(f"  ... {len(ignored) - 40} more ignored entries")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Block tracked generated/runtime/local-secret artifacts.")
    parser.add_argument(
        "--report",
        action="store_true",
        help="Print the tracked-file audit summary, including allowed env templates.",
    )
    parser.add_argument(
        "--include-ignored",
        action="store_true",
        help="Include a bounded git status --ignored summary in the report.",
    )
    args = parser.parse_args(argv)

    try:
        violations = tracked_artifact_violations(tracked_files(ROOT))
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr if isinstance(exc.stderr, str) else str(exc.stderr))
        return exc.returncode or 1

    if args.report or args.include_ignored:
        _print_report(ROOT, violations, include_ignored=args.include_ignored)

    if violations:
        if not args.report and not args.include_ignored:
            print("Tracked generated/runtime/local-secret artifacts are not allowed:")
            for violation in violations:
                print(f"  {violation.path}: {violation.reason}")
        print("\nRemove these from the index with `git rm --cached -- <path>` and keep them ignored.")
        return 1

    if not args.report and not args.include_ignored:
        print("Repo artifact hygiene check passed: no forbidden tracked artifacts.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
