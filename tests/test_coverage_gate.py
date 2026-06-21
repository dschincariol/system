from __future__ import annotations

import json
from pathlib import Path

from tools import coverage_gate


def _write_pyproject(path: Path, *, minimum: float = 52.0) -> None:
    path.write_text(
        "\n".join(
            [
                "[tool.trading_system.coverage_gate]",
                f"minimum_percent = {minimum}",
                'package_roots = ["engine", "services", "routes", "ops"]',
                'report_dir = "artifacts/coverage"',
                "",
            ]
        ),
        encoding="utf-8",
    )


def _coverage_payload() -> dict:
    return {
        "files": {
            "engine/runtime/storage.py": {
                "summary": {
                    "covered_lines": 8,
                    "num_statements": 10,
                    "covered_branches": 2,
                    "num_branches": 5,
                }
            },
            "services/data_source_manager.py": {
                "summary": {
                    "covered_lines": 6,
                    "num_statements": 10,
                    "covered_branches": 3,
                    "num_branches": 5,
                }
            },
            "routes/data_sources_routes.py": {
                "summary": {
                    "covered_lines": 2,
                    "num_statements": 10,
                    "covered_branches": 0,
                    "num_branches": 5,
                }
            },
            "ops/check_alerts.py": {
                "summary": {
                    "covered_lines": 1,
                    "num_statements": 10,
                    "covered_branches": 0,
                    "num_branches": 5,
                }
            },
        },
        "totals": {
            "covered_lines": 17,
            "num_statements": 40,
            "covered_branches": 5,
            "num_branches": 20,
        },
    }


def test_package_summaries_use_line_and_branch_totals() -> None:
    summaries = coverage_gate.package_summaries(
        _coverage_payload(),
        ("engine", "services", "routes", "ops"),
    )

    by_root = {summary.root: summary for summary in summaries}

    assert by_root["engine"].covered_total == 10
    assert by_root["engine"].measured_total == 15
    assert by_root["engine"].total_percent == (10 / 15) * 100
    assert by_root["routes"].branch_percent == 0.0
    assert by_root["ops"].line_percent == 10.0


def test_check_coverage_fails_below_configured_threshold(
    tmp_path: Path,
    capsys,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    coverage_json = tmp_path / "coverage.json"
    _write_pyproject(pyproject, minimum=40.0)
    coverage_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")

    config = coverage_gate.load_config(pyproject)
    exit_code = coverage_gate.check_coverage(coverage_json, config)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Measured total:   36.67%" in captured.out
    assert "engine" in captured.out
    assert "services" in captured.out
    assert "routes" in captured.out
    assert "ops" in captured.out
    assert "Coverage gate FAILED" in captured.err


def test_check_coverage_passes_at_or_above_threshold(
    tmp_path: Path,
    capsys,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    coverage_json = tmp_path / "coverage.json"
    _write_pyproject(pyproject, minimum=36.0)
    coverage_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")

    config = coverage_gate.load_config(pyproject)
    exit_code = coverage_gate.check_coverage(coverage_json, config)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Coverage gate PASSED" in captured.out


def test_build_pytest_command_enables_branch_coverage_and_reports(
    tmp_path: Path,
) -> None:
    config = coverage_gate.CoverageGateConfig(
        minimum_percent=52.0,
        package_roots=("engine", "services", "routes", "ops"),
        report_dir=tmp_path / "coverage",
    )

    command, coverage_json = coverage_gate.build_pytest_command(
        config,
        ["tests/test_coverage_gate.py", "-q"],
    )

    assert coverage_json == tmp_path / "coverage" / "coverage.json"
    assert "--cov-branch" in command
    assert "--cov-fail-under=0" in command
    assert "--cov=engine" in command
    assert "--cov=services" in command
    assert "--cov=routes" in command
    assert "--cov=ops" in command
    assert f"--cov-report=xml:{tmp_path / 'coverage' / 'coverage.xml'}" in command
    assert f"--cov-report=json:{coverage_json}" in command
    assert "tests/test_coverage_gate.py" in command
