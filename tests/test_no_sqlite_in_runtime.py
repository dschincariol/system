import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _looks_like_db_path(text: str) -> bool:
    lower = text.lower()
    if ".db" not in lower:
        return False
    return (
        lower.endswith(".db")
        or ".db?" in lower
        or ".db/" in lower
        or ".db\\" in lower
        or "/" in lower
        or "\\" in lower
    )


def test_engine_does_not_import_sqlite3_or_open_db_files():
    offenders = []
    for path in (ROOT / "engine").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "sqlite3":
                        offenders.append(f"{path}: import sqlite3")
            elif isinstance(node, ast.ImportFrom):
                if node.module == "sqlite3":
                    offenders.append(f"{path}: from sqlite3")
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                text = node.value.lower()
                if _looks_like_db_path(text) and "db_guard" not in str(path):
                    offenders.append(f"{path}: .db path literal")
    assert offenders == []
