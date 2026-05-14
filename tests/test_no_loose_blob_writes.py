from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _py_files():
    for base in (ROOT / "engine", ROOT / "tools"):
        for path in base.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            yield path


def _dotted_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted_name(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    return ""


def _looks_like_blob_target(node: ast.AST) -> bool:
    try:
        text = ast.unparse(node).lower()
    except Exception:
        text = ""
    markers = ("model", "blob", "artifact", "checkpoint", "weight", "joblib", "pickle", ".pkl", ".pt", ".pth")
    return any(marker in text for marker in markers)


def test_no_serializer_blob_writes_outside_artifact_layer() -> None:
    offenders: list[str] = []
    for path in _py_files():
        rel = path.relative_to(ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _dotted_name(node.func)
            if name not in {"joblib.dump", "torch.save", "pickle.dump", "pickle.dumps"}:
                continue
            if not rel.startswith("engine/artifacts/"):
                offenders.append(f"{rel}:{node.lineno}:{name}")
    assert offenders == []


def test_no_binary_file_writes_outside_artifact_layer() -> None:
    offenders: list[str] = []
    for path in _py_files():
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith("engine/artifacts/"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            name = _dotted_name(node.func)
            if name.endswith(".write_bytes") and _looks_like_blob_target(node.func):
                offenders.append(f"{rel}:{node.lineno}:{name}")
                continue
            if name != "open" or len(node.args) < 2:
                continue
            mode = node.args[1]
            if isinstance(mode, ast.Constant) and isinstance(mode.value, str) and "b" in mode.value and any(
                flag in mode.value for flag in ("w", "a", "x")
            ) and _looks_like_blob_target(node.args[0]):
                offenders.append(f"{rel}:{node.lineno}:open({mode.value!r})")
    assert offenders == []


def test_artifact_package_has_no_hardcoded_linux_artifact_root() -> None:
    offenders = []
    for path in (ROOT / "engine" / "artifacts").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "/var/lib/trading" in text:
            offenders.append(path.relative_to(ROOT).as_posix())
    assert offenders == []
