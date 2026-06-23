from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_PATH = ROOT / "dashboard_server.py"


def test_dashboard_boot_logs_app_init_bind_and_serve_phases() -> None:
    text = DASHBOARD_PATH.read_text(encoding="utf-8")
    run_block = text[
        text.index("def _run_dashboard_control_plane():") :
        text.index("def run_server():")
    ]

    assert '[dashboard_server] app_init_begin' in run_block
    assert '[dashboard_server] build_handler_ok' in run_block
    assert '[dashboard_server] app_init_ok' in run_block
    assert '[dashboard_server] bind_begin host=' in run_block
    assert '[dashboard_server] bind_ok host=' in run_block
    assert '[dashboard_server] serve_forever_enter host=' in run_block
    assert "dashboard_serve_forever_enter" in run_block
    assert "_HTTPD.serve_forever()" in run_block
