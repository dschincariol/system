from __future__ import annotations

from pathlib import Path

from tools import run_required_backend_tests


def _write_junit(path: Path) -> None:
    path.write_text(
        """<?xml version="1.0" encoding="utf-8"?>
<testsuite name="pytest" tests="2">
  <testcase classname="tests.test_money_path" name="test_ok" />
  <testcase classname="tests.test_optional_capability" name="test_skipped">
    <skipped message="optional runtime is not installed" />
  </testcase>
</testsuite>
""",
        encoding="utf-8",
    )


def test_inspect_junit_reports_selected_sources_and_unexpected_skips(tmp_path: Path) -> None:
    junit = tmp_path / "pytest.xml"
    _write_junit(junit)

    inspection = run_required_backend_tests.inspect_junit(junit)

    assert inspection.selected_tests == 2
    assert run_required_backend_tests._expected_source_present(
        "tests.test_money_path",
        inspection.source_modules,
    )
    assert inspection.skipped == [
        "tests.test_optional_capability::test_skipped: optional runtime is not installed"
    ]


def test_inspect_junit_allows_explicit_optional_skip_regex(tmp_path: Path) -> None:
    junit = tmp_path / "pytest.xml"
    _write_junit(junit)

    inspection = run_required_backend_tests.inspect_junit(
        junit,
        allow_skip_message_regexes=["optional runtime"],
    )

    assert inspection.selected_tests == 2
    assert inspection.skipped == []
