from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UI_PATH = ROOT / "boot" / "operator_ui.html"


def _extract_block(text: str, marker: str, next_marker: str) -> str:
    start = text.index(marker)
    end = text.index(next_marker, start)
    return text[start:end]


def test_operator_ui_disables_auto_actions_by_default():
    text = UI_PATH.read_text(encoding="utf-8")

    assert "const OPERATOR_AUTO_ACTIONS_ENABLED = false;" in text
    assert "setInterval(autoRemediationCheck, 5000)" not in text


def test_auto_remediation_check_is_observation_only():
    text = UI_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        "async function autoRemediationCheck(){",
        "let _refreshAllBusy = false;"
    )

    forbidden_calls = [
        '/api/operator/set_mode',
        '/api/operator/pause_training',
        '/api/operator/resume_training',
        '/api/operator/run_job',
        '/api/operator/restart',
        '/api/operator/ensure_polygon_stream',
        '/api/operator/repairSchema',
        '/api/operator/restart_feeds',
        '/api/operator/promote_model',
        '/api/operator/emergency_stop',
    ]
    for call in forbidden_calls:
        assert call not in block


def test_operator_ui_uses_staged_refresh_cadence():
    text = UI_PATH.read_text(encoding="utf-8")

    assert "const CORE_REFRESH_MS = 5000;" in text
    assert "const PANEL_REFRESH_MS = 15000;" in text
    assert "const DETAIL_REFRESH_MS = 30000;" in text
    assert "await refreshAll(force);" in text


def test_operator_ui_uses_quick_snapshot_for_clipboard_copy():
    text = UI_PATH.read_text(encoding="utf-8")

    assert 'onclick="copySnapshot(\'quick\')"' in text
    assert 'href="/api/operator/snapshot?mode=quick"' in text
    assert 'async function buildSnapshotBundle(mode = "quick")' in text
    assert 'async function copySnapshot(mode = "quick")' in text
    assert "execCommand_copy_failed" in text


def test_operator_ui_supports_dashboard_origin_bridge():
    text = UI_PATH.read_text(encoding="utf-8")

    assert "const OPERATOR_BRIDGE_PREFIX" in text
    assert 'path === "/operator/"' in text
    assert "function operatorBridgeUrl(url)" in text
    assert "fetch(operatorBridgeUrl(url), init)" in text
    assert "function operatorTelemetryWsUrl()" in text
    assert 'direct.pathname = "/ws/operator";' in text
