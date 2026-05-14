import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ALLOWED = {
    ROOT / "engine" / "runtime" / "platform.py",
}


def test_no_hardcoded_platform_paths_in_engine_or_services():
    offenders = []
    for root_name in ("engine", "services"):
        for path in (ROOT / root_name).rglob("*.py"):
            if path in ALLOWED:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    text = node.value
                    if "/var/lib/" in text or "/etc/" in text or "\\Trading\\" in text or "C:\\" in text:
                        offenders.append(f"{path}: {text!r}")
    assert offenders == []
