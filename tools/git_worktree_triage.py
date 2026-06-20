from __future__ import annotations

"""Non-destructive git worktree triage for large dirty workspaces."""

import json
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
GENERATED_MARKERS = (
    "__pycache__/",
    ".pytest_cache/",
    ".claude/",
    "node_modules/",
    "var/",
    "logs/",
    "tmp/",
    ".run-audit/",
    "data/retraining/",
    "docs/system_audit_layer1",
    "docs/job_migration_l",
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


def _git_status() -> List[Dict[str, str]]:
    proc = subprocess.run(
        ["git", "status", "--porcelain=v1"],
        cwd=str(ROOT),
        check=True,
        text=True,
        capture_output=True,
    )
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


def _category(path: str) -> str:
    lower = path.lower()
    if any(marker in lower for marker in GENERATED_MARKERS) or lower.endswith(RUNTIME_SUFFIXES):
        return "generated_or_runtime"
    if lower == ".npmrc":
        return "secrets_or_local_config"
    if lower == ".env.example":
        return "docs"
    if lower.startswith(".env") or "secret" in lower or "credential" in lower:
        return "secrets_or_local_config"
    if any(lower.startswith(prefix.lower()) for prefix in DOC_PREFIXES) or lower.endswith((".md", ".txt")):
        return "docs"
    if any(lower.startswith(prefix.lower()) for prefix in SOURCE_PREFIXES) or lower.endswith((".py", ".js", ".css", ".html", ".json", ".yml", ".yaml")):
        return "source_or_tests"
    return "needs_review"


def build_report() -> Dict[str, object]:
    rows = _git_status()
    by_status = Counter(row["status"] for row in rows)
    by_category = Counter(_category(row["path"]) for row in rows)
    samples = defaultdict(list)
    for row in rows:
        cat = _category(row["path"])
        if len(samples[cat]) < 12:
            samples[cat].append(row)
    return {
        "ok": True,
        "total": len(rows),
        "by_status": dict(sorted(by_status.items())),
        "by_category": dict(sorted(by_category.items())),
        "samples": {key: value for key, value in sorted(samples.items())},
        "recommended_action": (
            "Review generated_or_runtime and secrets_or_local_config first; keep intentional "
            "source_or_tests/docs changes, then commit in small cohesive groups."
        ),
    }


def main() -> int:
    print(json.dumps(build_report(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
