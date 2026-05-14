from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEGACY = "/etc/trading/" + "secrets/"


def _python_modules() -> list[Path]:
    roots = [ROOT / "engine", ROOT / "services", ROOT / "routes"]
    return [path for root in roots for path in root.rglob("*.py")]


def test_no_python_module_references_legacy_secret_path():
    offenders: list[str] = []
    for path in _python_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and LEGACY in node.value:
                offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert offenders == []
