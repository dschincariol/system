"""Run backend-required pytest selections and fail on unexpected skips."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MARKER_ARGS = ["-q", "-m", "requires_postgres or requires_redis", "-rs"]


@dataclass(frozen=True)
class JUnitInspection:
    selected_tests: int
    skipped: list[str]
    source_modules: frozenset[str]

    def __iter__(self) -> Iterator[int | list[str]]:
        yield self.selected_tests
        yield self.skipped


def _local_name(tag: str) -> str:
    return str(tag).rsplit("}", 1)[-1]


def _module_from_source(source: str) -> str:
    normalized = str(source).strip().replace("\\", "/").strip("/")
    if normalized.endswith(".py"):
        normalized = normalized[:-3]
    return normalized.replace("/", ".")


def _expected_source_present(expected_module: str, source_modules: frozenset[str]) -> bool:
    return any(
        module == expected_module or module.startswith(f"{expected_module}.")
        for module in source_modules
    )


def _compile_skip_allowlist(patterns: list[str]) -> list[re.Pattern[str]]:
    return [re.compile(pattern) for pattern in patterns]


def inspect_junit(
    path: Path,
    *,
    allow_skip_message_regexes: list[str] | None = None,
) -> JUnitInspection:
    """Return selected testcase count and rendered skipped-test entries."""

    root = ET.parse(path).getroot()
    testcases = [elem for elem in root.iter() if _local_name(elem.tag) == "testcase"]
    allow_skip_patterns = _compile_skip_allowlist(list(allow_skip_message_regexes or []))
    skipped: list[str] = []
    source_modules: set[str] = set()
    for case in testcases:
        classname = str(case.attrib.get("classname") or "").strip()
        if classname:
            source_modules.add(classname)
        source_file = str(case.attrib.get("file") or "").strip()
        if source_file:
            source_modules.add(_module_from_source(source_file))
        skipped_children = [child for child in list(case) if _local_name(child.tag) == "skipped"]
        if not skipped_children:
            continue
        name = str(case.attrib.get("name") or "").strip()
        label = f"{classname}::{name}".strip(":")
        messages = [
            str(child.attrib.get("message") or child.text or "").strip()
            for child in skipped_children
        ]
        reason = "; ".join(message for message in messages if message) or "skipped"
        rendered = f"{label}: {reason}"
        if any(pattern.search(rendered) for pattern in allow_skip_patterns):
            continue
        skipped.append(rendered)
    return JUnitInspection(
        selected_tests=len(testcases),
        skipped=skipped,
        source_modules=frozenset(source_modules),
    )


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
    parser.add_argument(
        "--min-selected",
        type=int,
        default=1,
        help="Minimum selected testcase count required for the gate to pass.",
    )
    parser.add_argument(
        "--expected-source",
        action="append",
        default=[],
        help=(
            "Expected pytest source file or module that must contribute at least "
            "one selected testcase. Repeat for each required file."
        ),
    )
    parser.add_argument(
        "--allow-skip-message-regex",
        action="append",
        default=[],
        help=(
            "Regex for an expected skip message in broad full-suite runs. "
            "Omit to fail on every selected skip."
        ),
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
        inspection = inspect_junit(
            junit_path,
            allow_skip_message_regexes=list(args.allow_skip_message_regex or []),
        )
    except Exception as exc:
        print(f"[{args.label}] ERROR: failed to inspect JUnit XML: {type(exc).__name__}: {exc}")
        return int(proc.returncode or 1)

    expected_modules = [_module_from_source(source) for source in list(args.expected_source or [])]
    missing_sources = [
        source
        for source, module in zip(list(args.expected_source or []), expected_modules)
        if not _expected_source_present(module, inspection.source_modules)
    ]

    print(
        f"[{args.label}] selected_tests={inspection.selected_tests} "
        f"skipped={len(inspection.skipped)} "
        f"expected_sources={len(expected_modules)} missing_sources={len(missing_sources)} "
        f"junit={junit_path}"
    )
    if inspection.selected_tests < int(args.min_selected):
        print(
            f"[{args.label}] ERROR: pytest selection collected "
            f"{inspection.selected_tests} tests, below required minimum {args.min_selected}"
        )
        return int(proc.returncode or 1)
    if missing_sources:
        print(f"[{args.label}] ERROR: expected sources selected no tests:")
        for source in missing_sources:
            print(f"- {source}")
        return int(proc.returncode or 1)
    if inspection.skipped:
        print(f"[{args.label}] ERROR: selected tests skipped unexpectedly:")
        for item in inspection.skipped:
            print(f"- {item}")
        return int(proc.returncode or 1)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
