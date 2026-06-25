from __future__ import annotations

import json
from pathlib import Path

from tools import coverage_gate


def _write_pyproject(
    path: Path,
    *,
    minimum: float = 52.0,
    package_minimums: dict[str, float] | None = None,
    zero_roots: list[str] | None = None,
    zero_allowlist: list[str] | None = None,
) -> None:
    lines = [
        "[tool.trading_system.coverage_gate]",
        f"minimum_percent = {minimum}",
        'package_roots = ["engine", "services", "routes", "ops"]',
        'report_dir = "artifacts/coverage"',
    ]
    if zero_roots is not None:
        rendered = ", ".join(json.dumps(root) for root in zero_roots)
        lines.append(f"zero_covered_module_roots = [{rendered}]")
    if zero_allowlist is not None:
        rendered = ", ".join(json.dumps(path) for path in zero_allowlist)
        lines.append(f"zero_covered_module_allowlist = [{rendered}]")
    if package_minimums:
        lines.extend(["", "[tool.trading_system.coverage_gate.package_minimums]"])
        for root, floor in package_minimums.items():
            lines.append(f"{json.dumps(root)} = {floor}")
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


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


def test_package_summaries_support_nested_subroots() -> None:
    summaries = coverage_gate.package_summaries(
        _coverage_payload(),
        ("engine/runtime", "engine/execution"),
    )

    by_root = {summary.root: summary for summary in summaries}

    assert by_root["engine/runtime"].covered_total == 10
    assert by_root["engine/runtime"].measured_total == 15
    assert by_root["engine/execution"].measured_total == 0


def test_check_coverage_fails_below_configured_threshold(
    tmp_path: Path,
    capsys,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    coverage_json = tmp_path / "coverage.json"
    _write_pyproject(pyproject, minimum=40.0)
    coverage_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")

    config = coverage_gate.load_config(pyproject)
    exit_code = coverage_gate.check_coverage(coverage_json, config, require_run_metadata=False)

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
    exit_code = coverage_gate.check_coverage(coverage_json, config, require_run_metadata=False)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Coverage gate PASSED" in captured.out


def test_check_coverage_enforces_nested_package_floor(
    tmp_path: Path,
    capsys,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    coverage_json = tmp_path / "coverage.json"
    _write_pyproject(
        pyproject,
        minimum=0.0,
        package_minimums={"engine/runtime": 80.0},
    )
    coverage_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")

    config = coverage_gate.load_config(pyproject)
    exit_code = coverage_gate.check_coverage(coverage_json, config, require_run_metadata=False)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "engine/runtime" in captured.out
    assert "FAIL" in captured.out
    assert "engine/runtime 66.67% is below 80.00%" in captured.err


def test_zero_covered_module_ratchet_fails_for_new_critical_module(
    tmp_path: Path,
    capsys,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    coverage_json = tmp_path / "coverage.json"
    payload = _coverage_payload()
    payload["files"]["engine/runtime/new_live_gate.py"] = {
        "summary": {
            "covered_lines": 0,
            "num_statements": 4,
            "covered_branches": 0,
            "num_branches": 0,
        }
    }
    _write_pyproject(
        pyproject,
        minimum=0.0,
        zero_roots=["engine/runtime"],
        zero_allowlist=["engine/runtime/old_allowlisted_gap.py"],
    )
    coverage_json.write_text(json.dumps(payload), encoding="utf-8")

    config = coverage_gate.load_config(pyproject)
    exit_code = coverage_gate.check_coverage(coverage_json, config, require_run_metadata=False)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Zero-covered module burndown: remaining=1 allowlisted=1 new=1" in captured.out
    assert "engine/runtime/new_live_gate.py" in captured.err


def test_zero_covered_module_allowlist_preserves_existing_burndown(
    tmp_path: Path,
    capsys,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    coverage_json = tmp_path / "coverage.json"
    payload = _coverage_payload()
    payload["files"]["engine/runtime/old_allowlisted_gap.py"] = {
        "summary": {
            "covered_lines": 0,
            "num_statements": 4,
            "covered_branches": 0,
            "num_branches": 0,
        }
    }
    _write_pyproject(
        pyproject,
        minimum=0.0,
        zero_roots=["engine/runtime"],
        zero_allowlist=["engine/runtime/old_allowlisted_gap.py"],
    )
    coverage_json.write_text(json.dumps(payload), encoding="utf-8")

    config = coverage_gate.load_config(pyproject)
    exit_code = coverage_gate.check_coverage(coverage_json, config, require_run_metadata=False)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Zero-covered module burndown: remaining=1 allowlisted=1 new=0" in captured.out


def test_check_coverage_rejects_unstamped_report_by_default(
    tmp_path: Path,
    capsys,
) -> None:
    config = coverage_gate.CoverageGateConfig(
        minimum_percent=0.0,
        package_roots=("engine", "services", "routes", "ops"),
        package_minimums={},
        zero_covered_module_roots=(),
        zero_covered_module_allowlist=frozenset(),
        report_dir=tmp_path,
    )
    coverage_json = tmp_path / "coverage.json"
    coverage_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")

    exit_code = coverage_gate.check_coverage(coverage_json, config)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "stale or partial coverage report" in captured.err
    assert "not stamped by tools/coverage_gate.py run" in captured.err


def test_check_coverage_accepts_stamped_full_gate_report(
    tmp_path: Path,
    capsys,
) -> None:
    config = coverage_gate.CoverageGateConfig(
        minimum_percent=0.0,
        package_roots=("engine", "services", "routes", "ops"),
        package_minimums={},
        zero_covered_module_roots=(),
        zero_covered_module_allowlist=frozenset(),
        report_dir=tmp_path,
    )
    coverage_json = tmp_path / "coverage.json"
    coverage_xml = tmp_path / "coverage.xml"
    coverage_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")
    coverage_xml.write_text("<coverage />\n", encoding="utf-8")
    coverage_gate.write_run_metadata(
        coverage_json=coverage_json,
        coverage_xml=coverage_xml,
        config=config,
        pytest_args=["tests/"],
        pytest_exit_code=0,
    )

    exit_code = coverage_gate.check_coverage(coverage_json, config)

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Coverage gate PASSED" in captured.out


def test_check_coverage_rejects_focused_run_metadata(
    tmp_path: Path,
    capsys,
) -> None:
    config = coverage_gate.CoverageGateConfig(
        minimum_percent=0.0,
        package_roots=("engine", "services", "routes", "ops"),
        package_minimums={},
        zero_covered_module_roots=(),
        zero_covered_module_allowlist=frozenset(),
        report_dir=tmp_path,
    )
    coverage_json = tmp_path / "coverage.json"
    coverage_xml = tmp_path / "coverage.xml"
    coverage_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")
    coverage_xml.write_text("<coverage />\n", encoding="utf-8")
    coverage_gate.write_run_metadata(
        coverage_json=coverage_json,
        coverage_xml=coverage_xml,
        config=config,
        pytest_args=["tests/test_coverage_gate.py", "-q"],
        pytest_exit_code=0,
    )

    exit_code = coverage_gate.check_coverage(coverage_json, config)

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "focused/partial pytest run" in captured.err


def test_run_coverage_rejects_existing_report_when_pytest_does_not_refresh(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    config = coverage_gate.CoverageGateConfig(
        minimum_percent=0.0,
        package_roots=("engine",),
        package_minimums={},
        zero_covered_module_roots=(),
        zero_covered_module_allowlist=frozenset(),
        report_dir=tmp_path,
    )
    stale_json = tmp_path / "coverage.json"
    stale_json.write_text(json.dumps(_coverage_payload()), encoding="utf-8")

    class Result:
        returncode = 0

    def fake_run(*args, **kwargs):
        assert "COVERAGE_PROCESS_START" in dict(kwargs.get("env") or {})
        return Result()

    monkeypatch.setattr(coverage_gate.subprocess, "run", fake_run)

    exit_code = coverage_gate.run_coverage(config, [])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert not stale_json.exists()
    assert "was not generated by this run" in captured.err


def test_build_pytest_command_enables_branch_coverage_and_reports(
    tmp_path: Path,
) -> None:
    config = coverage_gate.CoverageGateConfig(
        minimum_percent=52.0,
        package_roots=("engine", "services", "routes", "ops"),
        package_minimums={},
        zero_covered_module_roots=(),
        zero_covered_module_allowlist=frozenset(),
        report_dir=tmp_path / "coverage",
    )

    command, coverage_json = coverage_gate.build_pytest_command(
        config,
        ["tests/test_coverage_gate.py", "-q"],
    )

    assert coverage_json == tmp_path / "coverage" / "coverage.json"
    assert "--cov-branch" in command
    assert "--cov-fail-under=0" in command
    assert "--cov-report=" in command
    assert "--cov-report=term-missing:skip-covered" not in command
    assert "--cov=engine" in command
    assert "--cov=services" in command
    assert "--cov=routes" in command
    assert "--cov=ops" in command
    assert f"--cov-report=xml:{tmp_path / 'coverage' / 'coverage.xml'}" in command
    assert f"--cov-report=json:{coverage_json}" in command
    assert "tests/test_coverage_gate.py" in command
