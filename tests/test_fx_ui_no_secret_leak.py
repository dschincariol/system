from __future__ import annotations

import re
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FX_UI_FILES = [
    "ui/fx_format.js",
    "ui/fx_session.js",
    "ui/data_sources.js",
    "ui/dashboard.js",
    "ui/terminal/terminal.js",
]


def _text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_fx_ui_files_do_not_embed_generated_canary() -> None:
    canary = f"CANARY-{uuid.uuid4().hex}"
    for path in FX_UI_FILES:
        assert canary not in _text(path)


def test_pure_fx_modules_do_not_contain_credential_literals() -> None:
    forbidden = re.compile(r"(api[_-]?key|secret|password|access[_-]?token|credential)", re.I)
    for path in ["ui/fx_format.js", "ui/fx_session.js"]:
        assert not forbidden.search(_text(path)), path


def test_fx_data_source_result_path_omits_sensitive_result_fields() -> None:
    data_sources = _text("ui/data_sources.js")
    match = re.search(r"function renderFxTestResultPanel\(result\) \{(?P<body>.*?function renderDetail)", data_sources, flags=re.S)
    assert match, "FX test-result renderer not found"
    body = match.group("body")
    assert "renderEvidence(safe)" in body
    assert "result && result.evidence" in body
    assert '["status", "ok", "latency_ms", "latency", "detail", "message"]' in body
    for forbidden in ["api_key", "secret", "token", "password", "credential"]:
        assert forbidden not in body.lower()
