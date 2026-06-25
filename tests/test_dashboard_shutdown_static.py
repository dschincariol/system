from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT / "dashboard_server.py"
RUNTIME_SHUTDOWN_PATH = ROOT / "engine" / "runtime" / "shutdown.py"


def _extract_block(text: str, marker: str, next_marker: str) -> str:
    start = text.index(marker)
    end = text.index(next_marker, start)
    return text[start:end]


def test_dashboard_sigterm_requests_httpd_shutdown_from_background_thread():
    text = SERVER_PATH.read_text(encoding="utf-8")
    helper_block = _extract_block(
        text,
        "def _request_httpd_shutdown",
        "def _tail_text_file",
    )
    signal_block = _extract_block(
        text,
        "        def _shutdown(_sig=None, _frame=None):",
        "        try:\n            signal.signal(signal.SIGINT, _shutdown)",
    )

    assert "_start_background_thread" in helper_block
    assert "_HTTPD.shutdown()" in helper_block
    assert "_request_httpd_shutdown" in signal_block
    assert "_HTTPD.shutdown()" not in signal_block
    assert "handle_signal as _handle_bounded_signal" in signal_block
    assert "_handle_bounded_signal(" in signal_block


def test_runtime_shutdown_skips_sqlite_pragmas_for_postgres_storage():
    text = RUNTIME_SHUTDOWN_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        'connect_module = str(getattr(connect, "__module__", ""))',
        "if close_pooled_connections is not None:",
    )

    assert '"storage_pg" not in connect_module' in block
    assert "PRAGMA synchronous=FULL;" in block
    assert "PRAGMA wal_checkpoint(RESTART);" in block
