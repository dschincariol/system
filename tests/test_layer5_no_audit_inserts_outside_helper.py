"""Layer 5 negative test: no audit-table INSERTs outside `append_chain_row`.

The eight chained audit tables (decision_log, execution_mode_audit,
execution_policy_audit, kill_switch_audit, model_promotion_audit,
position_reconcile_audit, promotion_statistical_evidence,
trade_attribution_ledger) carry a tamper-evident hash chain. Every
write must go through `engine.audit.chain.append_chain_row()` so the
chain stays linked.

This test AST-scans `engine/` for direct INSERT statements against
those tables and fails the build if any module outside
`engine/audit/` writes one.

Catching this at lint time prevents a future contributor from
silently breaking the chain; the runtime verifier would still detect
the breach (see L4-AUDIT-01 fix), but a regression test caught here
is cheaper than a chain-broken finding.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
ENGINE = ROOT / "engine"
SERVICES = ROOT / "services"

# The eight chained audit tables, taken from the L4 audit's spec.
_CHAINED_AUDIT_TABLES = frozenset({
    "decision_log",
    "execution_mode_audit",
    "execution_policy_audit",
    "kill_switch_audit",
    "model_promotion_audit",
    "position_reconcile_audit",
    "promotion_statistical_evidence",
    "trade_attribution_ledger",
})

# Files allowed to issue raw INSERTs (the helper itself, plus the
# verifier which only ever writes to audit_chain_findings — a
# non-chained sibling table).
_ALLOWLISTED = (
    ROOT / "engine" / "audit" / "chain.py",
    ROOT / "engine" / "audit" / "verifier.py",
    ROOT / "engine" / "audit" / "cli.py",  # benchmark uses an in-mem table
    ROOT / "engine" / "runtime" / "schema",  # migrations may CREATE/INSERT
)

# Match an INSERT INTO <name> capturing the table name.
_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+([\"']?)(?P<table>[A-Za-z_][A-Za-z0-9_]*)\1",
    re.IGNORECASE,
)


def _is_allowlisted(path: Path) -> bool:
    for allow in _ALLOWLISTED:
        try:
            path.relative_to(allow)
        except ValueError:
            if path == allow:
                return True
            continue
        else:
            return True
    return False


def _scan_module_for_audit_inserts(path: Path) -> list[tuple[int, str]]:
    """Return [(line, table_name), …] for INSERT INTO <chained-table>
    occurrences in string literals inside the module."""
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError:
        return []
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            for m in _INSERT_RE.finditer(node.value):
                table = m.group("table").lower()
                if table in _CHAINED_AUDIT_TABLES:
                    hits.append((node.lineno, table))
    return hits


def test_no_module_under_engine_inserts_into_chained_audit_tables() -> None:
    offenders: list[str] = []
    for root in (ENGINE, SERVICES):
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if _is_allowlisted(path):
                continue
            hits = _scan_module_for_audit_inserts(path)
            for line, table in hits:
                rel = path.relative_to(ROOT).as_posix()
                offenders.append(
                    f"{rel}:{line} — direct INSERT INTO {table!r} bypasses "
                    "engine.audit.chain.append_chain_row()"
                )
    assert not offenders, (
        "Found raw INSERT statements against chained audit tables in "
        "non-allowlisted modules. Route these writes through "
        "engine.audit.chain.append_chain_row() instead, or add the "
        "module to the allowlist if it is genuinely test/diagnostic "
        "code:\n  " + "\n  ".join(offenders)
    )
