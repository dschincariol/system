"""Run backend-required pytest selections and fail on unexpected skips."""

from __future__ import annotations

import argparse
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MARKER_ARGS = ["-q", "-m", "requires_postgres or requires_redis", "-rs"]


def _local_name(tag: str) -> str:
    return str(tag).rsplit("}", 1)[-1]


def inspect_junit(path: Path) -> tuple[int, list[str]]:
    """Return selected testcase count and rendered skipped-test entries."""

    root = ET.parse(path).getroot()
    testcases = [elem for elem in root.iter() if _local_name(elem.tag) == "testcase"]
    skipped: list[str] = []
    for case in testcases:
        skipped_children = [child for child in list(case) if _local_name(child.tag) == "skipped"]
        if not skipped_children:
            continue
        classname = str(case.attrib.get("classname") or "").strip()
        name = str(case.attrib.get("name") or "").strip()
        label = f"{classname}::{name}".strip(":")
        messages = [
            str(child.attrib.get("message") or child.text or "").strip()
            for child in skipped_children
        ]
        reason = "; ".join(message for message in messages if message) or "skipped"
        skipped.append(f"{label}: {reason}")
    return len(testcases), skipped


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run pytest for production-backend coverage and fail if selected "
            "tests are skipped. Extra pytest args go after '--'."
        )
    )
    parser.add_argument(
        "--junitxml",
        default="var/artifacts/pytest-required-backends.xml",
        help="JUnit XML path used for skip inspection.",
    )
    parser.add_argument(
        "--label",
        default="required-backend-tests",
        help="Human-readable label included in summary output.",
    )
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    pytest_args = list(args.pytest_args or [])
    if pytest_args and pytest_args[0] == "--":
        pytest_args = pytest_args[1:]
    if not pytest_args:
        pytest_args = list(DEFAULT_MARKER_ARGS)

    junit_path = Path(args.junitxml)
    if not junit_path.is_absolute():
        junit_path = ROOT / junit_path
    junit_path.parent.mkdir(parents=True, exist_ok=True)

    command = [sys.executable, "-m", "pytest", *pytest_args, f"--junitxml={junit_path}"]
    print(f"[{args.label}] running: {' '.join(command)}")
    proc = subprocess.run(command, cwd=str(ROOT), check=False)

    if not junit_path.exists():
        print(f"[{args.label}] ERROR: pytest did not write JUnit XML: {junit_path}")
        return int(proc.returncode or 1)

    try:
        test_count, skipped = inspect_junit(junit_path)
    except Exception as exc:
        print(f"[{args.label}] ERROR: failed to inspect JUnit XML: {type(exc).__name__}: {exc}")
        return int(proc.returncode or 1)

    print(f"[{args.label}] selected_tests={test_count} skipped={len(skipped)} junit={junit_path}")
    if test_count <= 0:
        print(f"[{args.label}] ERROR: pytest selection collected zero tests")
        return int(proc.returncode or 1)
    if skipped:
        print(f"[{args.label}] ERROR: selected backend tests skipped unexpectedly:")
        for item in skipped:
            print(f"- {item}")
        return int(proc.returncode or 1)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
