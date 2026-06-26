from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UI_PATH = ROOT / "boot" / "operator_ui.html"


def test_operator_ui_uses_shared_dark_visual_contract():
    html = UI_PATH.read_text(encoding="utf-8")

    assert '<link rel="stylesheet" href="/ui/base.css"/>' in html
    assert '<body class="operatorConsole' in html
    assert "theme-operator" in html
    assert "var(--surface-chip)" in html
    assert "var(--surface-page-deep)" in html
    assert "var(--status-ok" in html
    assert "var(--status-warn" in html
    assert "var(--status-crit" in html
    assert "var(--status-info" in html

    stale_light_tokens = [
        "--bg:#eef1f6",
        "--card:#ffffff",
        "background:#ffffff",
        "#f1f5f9",
        "#fbfdff",
        "#2563eb",
        "#dc2626",
        "rgba(2,6,23",
    ]
    for token in stale_light_tokens:
        assert token not in html


def test_operator_ui_keeps_keyboard_focus_and_structured_error_surface():
    html = UI_PATH.read_text(encoding="utf-8")

    assert "button:focus-visible" in html
    assert "select:focus-visible" in html
    assert "summary:focus-visible" in html
    assert 'from "/ui/state_presenter.js"' in html
    assert "stateBlockHtml" in html
    assert "operatorPrimaryText" in html
    assert "operatorTechnicalDetailsHtml" in html
    assert "technicalDetailsHtml" in html
