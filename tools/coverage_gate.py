"""Run and enforce branch coverage for money-path packages."""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PYPROJECT = ROOT / "pyproject.toml"


@dataclass(frozen=True)
class CoverageGateConfig:
    minimum_percent: float
    package_roots: tuple[str, ...]
    package_minimums: dict[str, float]
    zero_covered_module_roots: tuple[str, ...]
    zero_covered_module_allowlist: frozenset[str]
    report_dir: Path


@dataclass(frozen=True)
class PackageCoverage:
    root: str
    covered_lines: int
    total_lines: int
    covered_branches: int
    total_branches: int

    @property
    def covered_total(self) -> int:
        return self.covered_lines + self.covered_branches

    @property
    def measured_total(self) -> int:
        return self.total_lines + self.total_branches

    @property
    def total_percent(self) -> float:
        return _percent(self.covered_total, self.measured_total)

    @property
    def line_percent(self) -> float:
        return _percent(self.covered_lines, self.total_lines)

    @property
    def branch_percent(self) -> float:
        return _percent(self.covered_branches, self.total_branches)


def _percent(covered: int, total: int) -> float:
    if total <= 0:
        return 100.0
    return (float(covered) / float(total)) * 100.0


def load_config(pyproject_path: Path = DEFAULT_PYPROJECT) -> CoverageGateConfig:
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    tool = data.get("tool") or {}
    trading_system = tool.get("trading_system") or {}
    raw_config = trading_system.get("coverage_gate") or {}
    roots = tuple(str(item) for item in raw_config.get("package_roots") or ())
    if not roots:
        raise ValueError("tool.trading_system.coverage_gate.package_roots is required")

    minimum = float(raw_config.get("minimum_percent"))
    raw_package_minimums = raw_config.get("package_minimums") or {}
    package_minimums = {
        _normalize_root(root): float(value)
        for root, value in dict(raw_package_minimums).items()
    }
    zero_roots = tuple(
        _normalize_root(str(item))
        for item in raw_config.get("zero_covered_module_roots") or ()
    )
    zero_allowlist = frozenset(
        _normalize_root(str(item))
        for item in raw_config.get("zero_covered_module_allowlist") or ()
    )
    report_dir = Path(str(raw_config.get("report_dir") or "artifacts/coverage"))
    if not report_dir.is_absolute():
        report_dir = ROOT / report_dir
    return CoverageGateConfig(
        minimum_percent=minimum,
        package_roots=roots,
        package_minimums=package_minimums,
        zero_covered_module_roots=zero_roots,
        zero_covered_module_allowlist=zero_allowlist,
        report_dir=report_dir,
    )


def _normalize_root(value: str) -> str:
    return str(value).strip().strip("/").replace("\\", "/")


def _path_matches_root(filename: str, root: str) -> bool:
    normalized = _normalize_root(filename)
    normalized_root = _normalize_root(root)
    return normalized == normalized_root or normalized.startswith(f"{normalized_root}/")


def _summary_int(summary: dict[str, Any], key: str) -> int:
    return int(summary.get(key) or 0)


def package_summaries(
    coverage_payload: dict[str, Any],
    roots: tuple[str, ...],
) -> list[PackageCoverage]:
    normalized_roots = tuple(_normalize_root(root) for root in roots)
    by_root: dict[str, dict[str, int]] = {
        root: {
            "covered_lines": 0,
            "num_statements": 0,
            "covered_branches": 0,
            "num_branches": 0,
        }
        for root in normalized_roots
    }

    for filename, file_payload in dict(coverage_payload.get("files") or {}).items():
        normalized_filename = _normalize_root(str(filename))
        if not normalized_filename:
            continue
        for root in normalized_roots:
            if not _path_matches_root(normalized_filename, root):
                continue
            summary = dict(file_payload.get("summary") or {})
            aggregate = by_root[root]
            aggregate["covered_lines"] += _summary_int(summary, "covered_lines")
            aggregate["num_statements"] += _summary_int(summary, "num_statements")
            aggregate["covered_branches"] += _summary_int(summary, "covered_branches")
            aggregate["num_branches"] += _summary_int(summary, "num_branches")

    return [
        PackageCoverage(
            root=root,
            covered_lines=values["covered_lines"],
            total_lines=values["num_statements"],
            covered_branches=values["covered_branches"],
            total_branches=values["num_branches"],
        )
        for root, values in by_root.items()
    ]


def total_coverage_percent(coverage_payload: dict[str, Any]) -> float:
    totals = dict(coverage_payload.get("totals") or {})
    covered = _summary_int(totals, "covered_lines") + _summary_int(totals, "covered_branches")
    measured = _summary_int(totals, "num_statements") + _summary_int(totals, "num_branches")
    if measured <= 0 and "percent_covered" in totals:
        return float(totals["percent_covered"])
    return _percent(covered, measured)


def _package_summary_map(
    coverage_payload: dict[str, Any],
    roots: tuple[str, ...],
) -> dict[str, PackageCoverage]:
    return {summary.root: summary for summary in package_summaries(coverage_payload, roots)}


def zero_covered_modules(
    coverage_payload: dict[str, Any],
    roots: tuple[str, ...],
) -> list[str]:
    modules: list[str] = []
    for filename, file_payload in dict(coverage_payload.get("files") or {}).items():
        normalized_filename = _normalize_root(str(filename))
        if not normalized_filename.endswith(".py"):
            continue
        if not any(_path_matches_root(normalized_filename, root) for root in roots):
            continue
        summary = dict(file_payload.get("summary") or {})
        total_lines = _summary_int(summary, "num_statements")
        covered_lines = _summary_int(summary, "covered_lines")
        if total_lines > 0 and covered_lines <= 0:
            modules.append(normalized_filename)
    return sorted(set(modules))


def _format_delta(measured: float, floor: float) -> str:
    delta = measured - floor
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.2f}%"


def _module_name_for_path(path: str) -> str:
    normalized = _normalize_root(path)
    if normalized.endswith(".py"):
        normalized = normalized[:-3]
    return normalized.replace("/", ".")


def _iter_python_files(roots: tuple[str, ...]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        root_path = ROOT / _normalize_root(root)
        if root_path.is_file() and root_path.suffix == ".py":
            files.append(root_path)
        elif root_path.is_dir():
            files.extend(path for path in root_path.rglob("*.py") if path.is_file())
    return sorted(set(files))


def _imported_modules(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return set()
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.add(str(alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(str(node.module))
            for alias in node.names:
                if alias.name != "*":
                    imports.add(f"{node.module}.{alias.name}")
    return imports


def production_import_counts(
    modules: list[str],
    roots: tuple[str, ...],
) -> dict[str, int]:
    module_names = {module: _module_name_for_path(module) for module in modules}
    counts = {module: 0 for module in modules}
    for path in _iter_python_files(roots):
        rel_path = _normalize_root(str(path.relative_to(ROOT)))
        importer_module = _module_name_for_path(rel_path)
        imported = _imported_modules(path)
        for module, module_name in module_names.items():
            if importer_module == module_name:
                continue
            if any(
                imported_name == module_name or imported_name.startswith(f"{module_name}.")
                for imported_name in imported
            ):
                counts[module] += 1
    return counts


def print_package_summary(
    coverage_payload: dict[str, Any],
    config: CoverageGateConfig,
) -> float:
    total_percent = total_coverage_percent(coverage_payload)
    print("\nCoverage gate summary (line + branch)")
    print(f"Minimum required: {config.minimum_percent:.2f}%")
    print(f"Measured total:   {total_percent:.2f}%")
    print("")
    print("Package        Total     Lines  Branches  Covered/Measured")
    print("-------------  ------  -------  --------  ----------------")
    for summary in package_summaries(coverage_payload, config.package_roots):
        print(
            f"{summary.root:<13}  "
            f"{summary.total_percent:6.2f}%  "
            f"{summary.line_percent:6.2f}%  "
            f"{summary.branch_percent:7.2f}%  "
            f"{summary.covered_total}/{summary.measured_total}"
        )
    if config.package_minimums:
        print("")
        print("Required package floors")
        print("Package           Result  Measured   Floor    Delta  Covered/Measured")
        print("----------------  ------  --------  ------  -------  ----------------")
        floor_summaries = _package_summary_map(
            coverage_payload,
            tuple(config.package_minimums),
        )
        for root, floor in config.package_minimums.items():
            summary = floor_summaries.get(root) or PackageCoverage(root, 0, 0, 0, 0)
            result = "PASS" if summary.total_percent + 1e-9 >= floor else "FAIL"
            print(
                f"{root:<16}  "
                f"{result:<6}  "
                f"{summary.total_percent:7.2f}%  "
                f"{floor:5.2f}%  "
                f"{_format_delta(summary.total_percent, floor):>7}  "
                f"{summary.covered_total}/{summary.measured_total}"
            )
    if config.zero_covered_module_roots:
        remaining = zero_covered_modules(
            coverage_payload,
            config.zero_covered_module_roots,
        )
        new_modules = [
            module
            for module in remaining
            if module not in config.zero_covered_module_allowlist
        ]
        print("")
        print(
            "Zero-covered module burndown: "
            f"remaining={len(remaining)} allowlisted={len(config.zero_covered_module_allowlist)} "
            f"new={len(new_modules)}"
        )
        if remaining:
            importer_counts = production_import_counts(remaining, config.package_roots)
            prioritized = sorted(
                remaining,
                key=lambda module: (-importer_counts.get(module, 0), module),
            )
            print("Zero-covered burndown priority (production importers)")
            for module in prioritized[:10]:
                print(f"- {module}: importers={importer_counts.get(module, 0)}")
    return total_percent


def check_coverage(coverage_json: Path, config: CoverageGateConfig) -> int:
    if not coverage_json.exists():
        print(f"Coverage gate FAILED: missing coverage JSON at {coverage_json}", file=sys.stderr)
        return 2

    coverage_payload = json.loads(coverage_json.read_text(encoding="utf-8"))
    total_percent = print_package_summary(coverage_payload, config)
    failures: list[str] = []
    if total_percent + 1e-9 < config.minimum_percent:
        failures.append(
            f"total {total_percent:.2f}% is below {config.minimum_percent:.2f}%"
        )

    if config.package_minimums:
        floor_summaries = _package_summary_map(
            coverage_payload,
            tuple(config.package_minimums),
        )
        for root, floor in config.package_minimums.items():
            summary = floor_summaries.get(root) or PackageCoverage(root, 0, 0, 0, 0)
            if summary.total_percent + 1e-9 < floor:
                failures.append(
                    f"{root} {summary.total_percent:.2f}% is below {floor:.2f}%"
                )

    if config.zero_covered_module_roots:
        remaining = zero_covered_modules(
            coverage_payload,
            config.zero_covered_module_roots,
        )
        new_zero_modules = [
            module
            for module in remaining
            if module not in config.zero_covered_module_allowlist
        ]
        if new_zero_modules:
            failures.append(
                "new zero-covered modules under critical roots: "
                + ", ".join(new_zero_modules)
            )

    if failures:
        print("Coverage gate FAILED:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print(
        "Coverage gate PASSED: "
        f"{total_percent:.2f}% >= {config.minimum_percent:.2f}%"
    )
    return 0


def build_pytest_command(
    config: CoverageGateConfig,
    pytest_args: list[str],
) -> tuple[list[str], Path]:
    config.report_dir.mkdir(parents=True, exist_ok=True)
    coverage_json = config.report_dir / "coverage.json"
    coverage_xml = config.report_dir / "coverage.xml"
    selected_pytest_args = list(pytest_args) if pytest_args else ["tests/"]

    command = [
        sys.executable,
        "-m",
        "pytest",
        *selected_pytest_args,
        "--tb=short",
        "--cov-branch",
        "--cov-fail-under=0",
        "--cov-report=",
        f"--cov-report=xml:{coverage_xml}",
        f"--cov-report=json:{coverage_json}",
    ]
    for root in config.package_roots:
        command.append(f"--cov={root}")
    return command, coverage_json


def run_coverage(config: CoverageGateConfig, pytest_args: list[str]) -> int:
    command, coverage_json = build_pytest_command(config, pytest_args)
    print("Running coverage command:")
    print(" ".join(command))
    pytest_result = subprocess.run(command, cwd=str(ROOT), check=False)
    if coverage_json.exists():
        gate_result = check_coverage(coverage_json, config)
    else:
        gate_result = 2
        print(f"Coverage gate FAILED: {coverage_json} was not generated", file=sys.stderr)
    if pytest_result.returncode:
        return int(pytest_result.returncode)
    return gate_result


def _strip_remainder_separator(values: list[str]) -> list[str]:
    if values and values[0] == "--":
        return values[1:]
    return values


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=DEFAULT_PYPROJECT,
        help="Path to the canonical pyproject.toml coverage gate config.",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run pytest with coverage, then enforce the gate.")
    run_parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Optional pytest args after --, for example: -- tests/test_file.py -q",
    )

    check_parser = subparsers.add_parser("check", help="Enforce the gate against an existing JSON report.")
    check_parser.add_argument(
        "coverage_json",
        nargs="?",
        type=Path,
        help="Coverage JSON path. Defaults to report_dir/coverage.json from pyproject.",
    )

    args = parser.parse_args(argv)
    config = load_config(args.pyproject)
    if args.command == "run":
        return run_coverage(config, _strip_remainder_separator(args.pytest_args))
    if args.command == "check":
        coverage_json = args.coverage_json or (config.report_dir / "coverage.json")
        if not coverage_json.is_absolute():
            coverage_json = ROOT / coverage_json
        return check_coverage(coverage_json, config)

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
