from __future__ import annotations

import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fx_ui_dom_contract_and_modules_exist() -> None:
    for path, ids in {
        "ui/data_sources.html": ["sourceCards", "detailPane", "sourcesBody"],
        "ui/dashboard.html": ["positionsExposureSummaryCard", "positionsExposureGrid", "positionsExposureNotes"],
        "ui/terminal/terminal.html": ["symInput", "terminalStatusBanner", "chartTitle", "chartHealth", "posTbl"],
    }.items():
        text = _text(path)
        for dom_id in ids:
            assert f'id="{dom_id}"' in text

    assert (ROOT / "ui/fx_format.js").is_file()
    assert (ROOT / "ui/fx_session.js").is_file()
    tracked = subprocess.run(
        ["git", "ls-files", "ui/fx_format.js", "ui/fx_session.js"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    assert "ui/fx_format.js" in tracked
    assert "ui/fx_session.js" in tracked


def test_fx_modules_are_pure_and_imported_by_entrypoints() -> None:
    fx_format = _text("ui/fx_format.js")
    fx_session = _text("ui/fx_session.js")
    for text in (fx_format, fx_session):
        assert "fetch(" not in text
        assert "document." not in text
        assert "window." not in text
        assert "process.env" not in text

    dashboard = _text("ui/dashboard.js")
    terminal = _text("ui/terminal/terminal.js")
    assert './fx_format.js' in dashboard
    assert '../fx_format.js' in terminal
    assert '../fx_session.js' in terminal
    assert "formatFxPrice" in dashboard
    assert "formatLotQty" in dashboard
    assert "fxSessionStatus" in terminal


def test_fx_ui_tests_are_registered_in_ui_gate() -> None:
    runner = _text("tools/run_ui_checks.mjs")
    for path in [
        "tests/test_fx_format.mjs",
        "tests/test_fx_session.mjs",
        "tests/test_fx_ui_contract.py",
        "tests/test_fx_ui_no_secret_leak.py",
    ]:
        assert path in runner

    node_array = re.search(r"const NODE_TESTS = \[(.*?)\];", runner, flags=re.S)
    pytest_array = re.search(r"const PYTEST_UI_TESTS = \[(.*?)\];", runner, flags=re.S)
    assert node_array and "tests/test_fx_format.mjs" in node_array.group(1)
    assert node_array and "tests/test_fx_session.mjs" in node_array.group(1)
    assert pytest_array and "tests/test_fx_ui_contract.py" in pytest_array.group(1)
    assert pytest_array and "tests/test_fx_ui_no_secret_leak.py" in pytest_array.group(1)


def test_fx_data_source_test_result_uses_whitelist() -> None:
    data_sources = _text("ui/data_sources.js")
    match = re.search(r"function renderFxTestResultPanel\(result\) \{(?P<body>.*?)\n\}", data_sources, flags=re.S)
    assert match, "renderFxTestResultPanel missing"
    body = match.group("body")
    for key in ["status", "ok", "latency_ms", "latency", "detail", "message"]:
        assert f'"{key}"' in body
    for forbidden in ["api_key", "secret", "token", "password", "credential"]:
        assert forbidden not in body.lower()
