from __future__ import annotations

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest

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
CREATE_TABLE_HEAD_RE = re.compile(
    r"""
    CREATE\s+TABLE\s+
    (?:IF\s+NOT\s+EXISTS\s+)?
    (?:(?:"[A-Za-z_][A-Za-z0-9_]*"|[A-Za-z_][A-Za-z0-9_]*)\.)?
    (?P<name>"?[A-Za-z_][A-Za-z0-9_]*"?)\s*\(
    """,
    re.IGNORECASE | re.VERBOSE,
)
CREATE_TABLE_UNRESOLVED_NAME_RE = re.compile(
    r"""
    CREATE\s+TABLE\s+
    (?:IF\s+NOT\s+EXISTS\s+)?
    \{(?P<expr>_?[A-Za-z_][A-Za-z0-9_]*)\}
    """,
    re.IGNORECASE | re.VERBOSE,
)
ALTER_ADD_COLUMN_RE = re.compile(
    r"""
    ALTER\s+TABLE\s+
    (?:(?:"[A-Za-z_][A-Za-z0-9_]*"|[A-Za-z_][A-Za-z0-9_]*)\.)?
    (?P<name>"?[A-Za-z_][A-Za-z0-9_]*"?)\s+
    ADD\s+COLUMN\s+
    (?:IF\s+NOT\s+EXISTS\s+)?
    (?P<column>"?[A-Za-z_][A-Za-z0-9_]*"?)
    """,
    re.IGNORECASE | re.VERBOSE,
)
STATIC_TABLE_CONSTANT_RE = re.compile(r"_?[A-Z][A-Z0-9_]*")
TABLE_CONSTRAINT_KEYWORDS = frozenset({"CHECK", "CONSTRAINT", "EXCLUDE", "FOREIGN", "PRIMARY", "UNIQUE"})


def _baseline_tables() -> set[str]:
    path = ROOT / "engine/runtime/schema/migrations/0001_baseline.py"
    spec = importlib.util.spec_from_file_location("baseline_schema", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {str(name) for name, _ in module.TABLE_DEFS}


def _module_string_constants(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return {}
    constants: dict[str, str] = {}
    for node in tree.body:
        targets: list[ast.expr]
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value = node.value
        else:
            continue
        if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                constants[str(target.id)] = str(value.value)
    return constants


def _expression_text(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:
        return type(node).__name__


def _resolve_joined_string(node: ast.JoinedStr, constants: dict[str, str]) -> tuple[str, tuple[str, ...]]:
    parts: list[str] = []
    unresolved: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(str(value.value))
            continue
        if isinstance(value, ast.FormattedValue):
            expr = value.value
            if isinstance(expr, ast.Name) and expr.id in constants:
                parts.append(constants[expr.id])
                continue
            expr_text = _expression_text(expr)
            unresolved.append(expr_text)
            parts.append("{" + expr_text + "}")
    return "".join(parts), tuple(unresolved)


def _string_expressions(path: Path) -> list[tuple[str, tuple[str, ...]]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    constants = _module_string_constants(path)
    out: list[tuple[str, tuple[str, ...]]] = []

    class Visitor(ast.NodeVisitor):
        def visit_Constant(self, node: ast.Constant) -> None:  # noqa: N802 - ast visitor API.
            if isinstance(node.value, str):
                out.append((str(node.value), ()))

        def visit_JoinedStr(self, node: ast.JoinedStr) -> None:  # noqa: N802 - ast visitor API.
            out.append(_resolve_joined_string(node, constants))

    Visitor().visit(tree)
    return out


def _looks_like_static_table_constant(expr: str) -> bool:
    return bool(STATIC_TABLE_CONSTANT_RE.fullmatch(str(expr))) and "TABLE" in str(expr)


def _source_create_table_names(folders: tuple[Path, ...] | None = None) -> set[str]:
    names: set[str] = set()
    unresolved_static_table_names: list[str] = []
    for folder in folders or (ROOT / "engine", ROOT / "ops"):
        if not folder.exists():
            continue
        for path in folder.rglob("*.py"):
            for literal, _unresolved in _string_expressions(path):
                if "CREATE TABLE" not in literal.upper():
                    continue
                for match in CREATE_TABLE_UNRESOLVED_NAME_RE.finditer(literal):
                    expr = str(match.group("expr"))
                    if _looks_like_static_table_constant(expr):
                        try:
                            display_path = path.relative_to(ROOT)
                        except ValueError:
                            display_path = path
                        unresolved_static_table_names.append(f"{display_path}:{expr}")
                for match in CREATE_TABLE_RE.finditer(literal):
                    name = match.group("name").strip('"')
                    if name and "{" not in name and "}" not in name:
                        names.add(name)
    assert not unresolved_static_table_names, (
        "Unresolved static CREATE TABLE name constants: " + ", ".join(sorted(unresolved_static_table_names))
    )
    return names


def _baseline_table_columns() -> dict[str, set[str]]:
    path = ROOT / "engine/runtime/schema/migrations/0001_baseline.py"
    spec = importlib.util.spec_from_file_location("baseline_schema_columns", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {str(name): _column_names_from_table_body(str(body)) for name, body in module.TABLE_DEFS}


def _matching_parenthesized_body(sql: str, start: int) -> str:
    depth = 1
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = 0
    index = int(start)
    while index < len(sql):
        char = sql[index]
        next_char = sql[index + 1] if index + 1 < len(sql) else ""
        if in_line_comment:
            if char == "\n":
                in_line_comment = False
        elif in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment -= 1
                index += 1
        elif in_single_quote:
            if char == "'":
                if next_char == "'":
                    index += 1
                else:
                    in_single_quote = False
        elif in_double_quote:
            if char == '"':
                if next_char == '"':
                    index += 1
                else:
                    in_double_quote = False
        elif char == "-" and next_char == "-":
            in_line_comment = True
            index += 1
        elif char == "/" and next_char == "*":
            in_block_comment += 1
            index += 1
        elif char == "'":
            in_single_quote = True
        elif char == '"':
            in_double_quote = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return sql[start:index]
        index += 1
    return sql[start:]


def _split_top_level_sql_items(body: str) -> list[str]:
    items: list[str] = []
    start = 0
    depth = 0
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = 0
    index = 0
    while index < len(body):
        char = body[index]
        next_char = body[index + 1] if index + 1 < len(body) else ""
        if in_line_comment:
            if char == "\n":
                in_line_comment = False
        elif in_block_comment:
            if char == "*" and next_char == "/":
                in_block_comment -= 1
                index += 1
        elif in_single_quote:
            if char == "'":
                if next_char == "'":
                    index += 1
                else:
                    in_single_quote = False
        elif in_double_quote:
            if char == '"':
                if next_char == '"':
                    index += 1
                else:
                    in_double_quote = False
        elif char == "-" and next_char == "-":
            in_line_comment = True
            index += 1
        elif char == "/" and next_char == "*":
            in_block_comment += 1
            index += 1
        elif char == "'":
            in_single_quote = True
        elif char == '"':
            in_double_quote = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            items.append(body[start:index])
            start = index + 1
        index += 1
    items.append(body[start:])
    return items


def _strip_sql_line_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines()).strip()


def _column_names_from_table_body(body: str) -> set[str]:
    columns: set[str] = set()
    for item in _split_top_level_sql_items(body):
        item = _strip_sql_line_comments(item)
        if not item:
            continue
        first = item.split(None, 1)[0].strip('"')
        if not first or first.upper() in TABLE_CONSTRAINT_KEYWORDS:
            continue
        columns.add(first)
    return columns


def _create_table_columns_from_sql(sql: str) -> dict[str, set[str]]:
    columns_by_table: dict[str, set[str]] = {}
    for match in CREATE_TABLE_HEAD_RE.finditer(sql):
        table_name = match.group("name").strip('"')
        body = _matching_parenthesized_body(sql, match.end())
        columns_by_table.setdefault(table_name, set()).update(_column_names_from_table_body(body))
    return columns_by_table


def _alter_add_columns_from_sql(sql: str) -> dict[str, set[str]]:
    columns_by_table: dict[str, set[str]] = {}
    for match in ALTER_ADD_COLUMN_RE.finditer(sql):
        table_name = match.group("name").strip('"')
        column_name = match.group("column").strip('"')
        columns_by_table.setdefault(table_name, set()).add(column_name)
    return columns_by_table


def _literal_add_column_calls(path: Path) -> dict[str, set[str]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return {}
    columns_by_table: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function_name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if function_name != "_add_column" or len(node.args) < 3:
            continue
        table_arg = node.args[1]
        column_arg = node.args[2]
        if (
            isinstance(table_arg, ast.Constant)
            and isinstance(table_arg.value, str)
            and isinstance(column_arg, ast.Constant)
            and isinstance(column_arg.value, str)
        ):
            columns_by_table.setdefault(str(table_arg.value), set()).add(str(column_arg.value))
    return columns_by_table


def _merge_table_columns(target: dict[str, set[str]], source: dict[str, set[str]]) -> None:
    for table_name, columns in source.items():
        target.setdefault(table_name, set()).update(columns)


def _migration_materialized_columns() -> dict[str, set[str]]:
    columns_by_table = _baseline_table_columns()
    for path in (ROOT / "engine/runtime/schema/migrations").glob("*.py"):
        if path.name == "0001_baseline.py":
            continue
        for literal, _unresolved in _string_expressions(path):
            _merge_table_columns(columns_by_table, _create_table_columns_from_sql(literal))
            _merge_table_columns(columns_by_table, _alter_add_columns_from_sql(literal))
        _merge_table_columns(columns_by_table, _literal_add_column_calls(path))
    return columns_by_table


def test_every_create_table_has_classification() -> None:
    discovered = _baseline_tables() | _source_create_table_names() | set(SOURCE_DECLARED_TABLES)
    missing = sorted(discovered - set(TABLE_CLASS))
    assert not missing, "Unclassified CREATE TABLE definitions: " + ", ".join(missing)


def test_classification_entries_are_typed() -> None:
    invalid = sorted(
        name for name, classification in TABLE_CLASS.items() if not isinstance(classification, (Hypertable, Regular))
    )
    assert not invalid, "Invalid table classifications: " + ", ".join(invalid)


def test_source_scanner_resolves_static_fstring_table_names() -> None:
    discovered = _source_create_table_names()

    assert {
        "learned_alpha_decay_runs",
        "learned_alpha_decay_estimates",
        "learned_alpha_decay_age_edges",
    }.issubset(discovered)


def test_source_scanner_discovers_resolved_static_table_constant(tmp_path: Path) -> None:
    module = tmp_path / "ddl.py"
    module.write_text(
        """
RUNTIME_TABLE = "new_runtime_table"

def ensure_schema(con):
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {RUNTIME_TABLE} (
          id INTEGER PRIMARY KEY
        )
    ''')
""",
        encoding="utf-8",
    )

    assert _source_create_table_names((tmp_path,)) == {"new_runtime_table"}


def test_source_scanner_fails_on_unresolved_static_table_constant(tmp_path: Path) -> None:
    module = tmp_path / "ddl.py"
    module.write_text(
        """
def ensure_schema(con):
    con.execute(f'''
        CREATE TABLE IF NOT EXISTS {MISSING_TABLE} (
          id INTEGER PRIMARY KEY
        )
    ''')
""",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="MISSING_TABLE"):
        _source_create_table_names((tmp_path,))


def test_classified_hypertable_time_columns_exist_in_migration_schema() -> None:
    columns_by_table = _migration_materialized_columns()
    missing = sorted(
        f"{table_name}.{classification.time_column}"
        for table_name, classification in TABLE_CLASS.items()
        if isinstance(classification, Hypertable)
        and table_name in columns_by_table
        and classification.time_column not in columns_by_table[table_name]
    )
    assert not missing, "Hypertable time_column missing from migration materialized schema: " + ", ".join(missing)


def test_learned_alpha_decay_tables_are_regular() -> None:
    expected = {
        "learned_alpha_decay_runs": "latest run lookup and training audit",
        "learned_alpha_decay_estimates": "latest cohort lookup from execution, portfolio, and champion paths",
        "learned_alpha_decay_age_edges": "run/cohort drill-down and estimator audit",
    }

    for table_name, read_pattern in expected.items():
        classification = TABLE_CLASS[table_name]
        assert isinstance(classification, Regular)
        assert classification.write_rate == "low"
        assert classification.cleanup is None
        assert classification.read_pattern == read_pattern


def test_compliance_ledger_is_never_compressed_or_retained() -> None:
    ledger = TABLE_CLASS["trade_attribution_ledger"]
    assert isinstance(ledger, Hypertable)
    assert ledger.compress_after is None
    assert ledger.retain is None


def test_database_schema_doc_lists_every_classified_table() -> None:
    doc = (ROOT / "docs/Database_Schema.md").read_text(encoding="utf-8")
    missing = sorted(name for name in TABLE_CLASS if f"`{name}`" not in doc)
    assert not missing, "docs/Database_Schema.md missing table entries: " + ", ".join(missing)
