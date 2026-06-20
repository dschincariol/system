"""
FILE: syntax_check_workspace.py

Tooling or validation script for `syntax_check_workspace`.
"""

from pathlib import Path
import sys


ROOTS = [
    "start_system.py",
    "start_ingestion.py",
    "start_all.py",
    "run_dev.py",
    "dashboard_server.py",
    "engine",
    "ops",
    "scripts",
    "services",
    "tools",
]


def iter_python_files():
    files = []
    for root in ROOTS:
        path = Path(root)
        if not path.exists():
            continue
        if path.is_file():
            files.append(path)
        else:
            files.extend(sorted(path.rglob("*.py")))

    seen = set()
    for file_path in files:
        key = str(file_path)
        if key in seen:
            continue
        seen.add(key)
        yield file_path


def main() -> int:
    errors = []
    checked = 0

    for file_path in iter_python_files():
        checked += 1
        try:
            compile(file_path.read_bytes(), str(file_path), "exec", dont_inherit=True)
            print(f"OK {file_path}")
        except SyntaxError as err:
            line = getattr(err, "lineno", 1) or 1
            print(f"{file_path}:{line}: {err}", file=sys.stderr)
            errors.append(str(file_path))
        except Exception as err:
            print(f"{file_path}:1: {err}", file=sys.stderr)
            errors.append(str(file_path))

    if errors:
        print(f"Failed: {len(errors)} file(s)", file=sys.stderr)
        return 1

    print(f"Syntax OK: {checked} file(s) checked")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
