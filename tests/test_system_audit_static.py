from __future__ import annotations

import ast

from tools import system_audit


def _run_stub_detector(source: str) -> system_audit.AuditState:
    state = system_audit.AuditState()
    tree = ast.parse(source)
    system_audit.detect_stubs_and_not_impl(tree, source, "engine/example.py", state)
    return state


def _run_exception_detector(source: str) -> system_audit.AuditState:
    state = system_audit.AuditState()
    tree = ast.parse(source)
    system_audit.detect_silent_exceptions(tree, source, "engine/example.py", state)
    return state


def test_protocol_ellipsis_methods_are_not_stub_findings() -> None:
    source = """
from typing import Protocol

class Store(Protocol):
    def put(self, value: bytes) -> str:
        ...
"""

    assert _run_stub_detector(source).findings == []


def test_system_audit_suppression_is_category_scoped() -> None:
    source = """
def cleanup() -> None:
    try:
        work()
    # system-audit: ignore[silent_except] cleanup is best-effort
    except Exception:
        pass
"""

    assert _run_exception_detector(source).findings == []

    unsuppressed = source.replace("ignore[silent_except]", "ignore[stub]")
    findings = _run_exception_detector(unsuppressed).findings
    assert len(findings) == 1
    assert findings[0].category == "silent_except"
