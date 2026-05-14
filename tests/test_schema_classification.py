from __future__ import annotations

import ast
import importlib.util
import io
import re
import sys
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.schema.table_classification import (
    Hypertable,
    Regular,
    SOURCE_DECLARED_TABLES,
    TABLE_CLASS,
)


CREATE_TABLE_RE = re.compile(
    r"""
    CREATE\s+TABLE\s+
    (?:IF\s+NOT\s+EXISTS\s+)?
    (?:(?:"[A-Za-z_][A-Za-z0-9_]*"|[A-Za-z_][A-Za-z0-9_]*)\.)?
    (?P<name>(?!IF\b)"?[A-Za-z_][A-Za-z0-9_]*"?)
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _baseline_tables() -> set[str]:
    path = ROOT / "engine/runtime/schema/migrations/0001_baseline.py"
    spec = importlib.util.spec_from_file_location("baseline_schema", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {str(name) for name, _ in module.TABLE_DEFS}


def _string_literals(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    out: list[str] = []
    try:
        tokens = tokenize.generate_tokens(io.StringIO(text).readline)
        for token in tokens:
            if token.type != tokenize.STRING:
                continue
            try:
                value = ast.literal_eval(token.string)
            except Exception:
                value = token.string
            if isinstance(value, str):
                out.append(value)
    except tokenize.TokenError:
        pass
    return out


def _source_create_table_names() -> set[str]:
    names: set[str] = set()
    for folder in (ROOT / "engine", ROOT / "ops"):
        if not folder.exists():
            continue
        for path in folder.rglob("*.py"):
            for literal in _string_literals(path):
                if "CREATE TABLE" not in literal.upper():
                    continue
                for match in CREATE_TABLE_RE.finditer(literal):
                    name = match.group("name").strip('"')
                    if name and "{" not in name and "}" not in name:
                        names.add(name)
    return names


def test_every_create_table_has_classification() -> None:
    discovered = _baseline_tables() | _source_create_table_names() | set(SOURCE_DECLARED_TABLES)
    missing = sorted(discovered - set(TABLE_CLASS))
    assert not missing, "Unclassified CREATE TABLE definitions: " + ", ".join(missing)


def test_classification_entries_are_typed() -> None:
    invalid = sorted(
        name for name, classification in TABLE_CLASS.items() if not isinstance(classification, (Hypertable, Regular))
    )
    assert not invalid, "Invalid table classifications: " + ", ".join(invalid)


def test_compliance_ledger_is_never_compressed_or_retained() -> None:
    ledger = TABLE_CLASS["trade_attribution_ledger"]
    assert isinstance(ledger, Hypertable)
    assert ledger.compress_after is None
    assert ledger.retain is None


def test_database_schema_doc_lists_every_classified_table() -> None:
    doc = (ROOT / "docs/Database_Schema.md").read_text(encoding="utf-8")
    missing = sorted(name for name in TABLE_CLASS if f"`{name}`" not in doc)
    assert not missing, "docs/Database_Schema.md missing table entries: " + ", ".join(missing)
