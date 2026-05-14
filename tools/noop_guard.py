"""
FILE: noop_guard.py

Guardrail for silent no-op error handling patterns.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = REPO_ROOT / "tools" / "noop_guard_baseline.json"
ALLOW_MARKER = "no-op-guard: allow"

SCAN_DIRS = [
    "boot",
    "engine",
    "ops",
    "scripts",
    "services",
    "tools",
    "ui",
    "dashboard_server.py",
    "start_system.py",
    "start_ingestion.py",
    "start_all.py",
    "run_dev.py",
]

SKIP_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
    "logs",
    "logs-staging",
    "logs-staging-cleanrepro",
    "logs-staging-faulthandler",
    "logs-staging-faulthandler-clean",
    "logs-staging-faulthandler-postmetrics",
    "logs-staging-faulthandler2",
    "logs-staging-mini",
    "logs-staging-repro",
    "data",
    "data-isolation",
    "data-staging",
}

TEXT_EXTENSIONS = {".py", ".js", ".html"}


PY_EXCEPT_RE = re.compile(r"^(?P<indent>\s*)except\s+(?:Exception|BaseException)(?:\s+as\s+\w+)?\s*:\s*$")
PY_PASS_RE = re.compile(r"^\s*pass\s*(?:#.*)?$")
JS_EMPTY_CATCH_RE = re.compile(r"catch\s*\(\s*[^)]*\s*\)\s*\{\s*\}")
JS_PROMISE_EMPTY_CATCH_RE = re.compile(r"\.catch\(\s*\(\s*[^)]*\s*\)\s*=>\s*\{\s*\}\s*\)")


def iter_scan_files() -> Iterable[Path]:
    seen = set()
    for entry in SCAN_DIRS:
        root = REPO_ROOT / entry
        if not root.exists():
            continue
        if root.is_file():
            if root.suffix.lower() in TEXT_EXTENSIONS:
                yield root
            continue
        for path in root.rglob("*"):
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            if not path.is_file() or path.suffix.lower() not in TEXT_EXTENSIONS:
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            yield path


def load_text(path: Path) -> List[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        sys.stderr.write(f"[noop_guard] utf8_decode_failed path={path}\n")
        sys.stderr.flush()
        return path.read_text(encoding="utf-8", errors="ignore").splitlines()


def rel_path(path: Path) -> str:
    return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()


def make_fingerprint(rule: str, path: Path, snippet: str) -> str:
    normalized = " ".join(snippet.strip().split())
    return f"{rule}|{rel_path(path)}|{normalized}"


def is_allowed(lines: List[str], index: int) -> bool:
    start = max(0, index - 1)
    end = min(len(lines), index + 3)
    for i in range(start, end):
        if ALLOW_MARKER in lines[i]:
            return True
    return False


def scan_python(path: Path, lines: List[str]) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for idx, line in enumerate(lines):
        match = PY_EXCEPT_RE.match(line)
        if not match:
            continue
        if idx + 1 >= len(lines):
            continue
        next_line = lines[idx + 1]
        if not PY_PASS_RE.match(next_line):
            continue
        if is_allowed(lines, idx):
            continue
        snippet = f"{line}\n{next_line}"
        findings.append(
            {
                "rule": "python_except_pass",
                "path": rel_path(path),
                "line": idx + 1,
                "snippet": snippet,
                "fingerprint": make_fingerprint("python_except_pass", path, snippet),
            }
        )
    return findings


def scan_js_like(path: Path, lines: List[str]) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for idx, line in enumerate(lines):
        if is_allowed(lines, idx):
            continue
        stripped = line.strip()
        if JS_EMPTY_CATCH_RE.search(stripped):
            findings.append(
                {
                    "rule": "js_empty_catch_block",
                    "path": rel_path(path),
                    "line": idx + 1,
                    "snippet": stripped,
                    "fingerprint": make_fingerprint("js_empty_catch_block", path, stripped),
                }
            )
        if JS_PROMISE_EMPTY_CATCH_RE.search(stripped):
            findings.append(
                {
                    "rule": "js_empty_promise_catch",
                    "path": rel_path(path),
                    "line": idx + 1,
                    "snippet": stripped,
                    "fingerprint": make_fingerprint("js_empty_promise_catch", path, stripped),
                }
            )
    return findings


def scan_repo() -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for path in iter_scan_files():
        lines = load_text(path)
        if path.suffix.lower() == ".py":
            findings.extend(scan_python(path, lines))
        elif path.suffix.lower() in {".js", ".html"}:
            findings.extend(scan_js_like(path, lines))
    return findings


def load_baseline() -> Dict[str, List[str]]:
    if not BASELINE_PATH.exists():
        return {"allowed_fingerprints": []}
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def write_baseline(findings: List[Dict[str, Any]]) -> None:
    payload = {
        "comment": (
            "Grandfathered silent no-op patterns. "
            "Prefer removing entries by fixing code or annotating intentional cases with "
            f"'{ALLOW_MARKER}'."
        ),
        "allowed_fingerprints": sorted({item["fingerprint"] for item in findings}),
    }
    BASELINE_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Detect silent no-op error handling patterns.")
    parser.add_argument("--write-baseline", action="store_true", help="Write the current findings as the baseline.")
    args = parser.parse_args()

    findings = scan_repo()

    if args.write_baseline:
        write_baseline(findings)
        print(f"Wrote baseline with {len(findings)} finding(s) to {BASELINE_PATH}")
        return 0

    baseline = load_baseline()
    allowed = set(baseline.get("allowed_fingerprints") or [])
    current = [item for item in findings if item["fingerprint"] not in allowed]

    if not current:
        print(f"No new no-op findings. Scanned {len(findings)} known pattern(s).")
        return 0

    print("New no-op findings detected:", file=sys.stderr)
    for item in current:
        print(
            f"{item['path']}:{item['line']} [{item['rule']}] {item['snippet']}",
            file=sys.stderr,
        )
    print(
        "\nEither remove the silent no-op, log it explicitly, or annotate the code with "
        f"'{ALLOW_MARKER}' if the behavior is intentional.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
