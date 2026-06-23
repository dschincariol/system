"""Check text files for trailing whitespace and missing final newlines."""

from __future__ import annotations

import argparse
from pathlib import Path


def _is_binary(data: bytes) -> bool:
    return b"\0" in data


def _line_body(line: bytes) -> bytes:
    body = line.rstrip(b"\n")
    if body.endswith(b"\r"):
        body = body[:-1]
    return body


def check_path(path: Path) -> list[str]:
    if not path.exists() or not path.is_file():
        return []

    data = path.read_bytes()
    if _is_binary(data):
        return []

    errors: list[str] = []
    if data and not data.endswith(b"\n"):
        errors.append(f"{path}: EOF: missing final newline")

    for lineno, line in enumerate(data.splitlines(keepends=True), start=1):
        body = _line_body(line)
        if body.endswith((b" ", b"\t")):
            errors.append(f"{path}:{lineno}: trailing whitespace")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check text files for trailing whitespace and missing final newlines."
    )
    parser.add_argument("paths", nargs="*", help="Paths passed by pre-commit.")
    args = parser.parse_args(argv)

    errors: list[str] = []
    for raw_path in args.paths:
        errors.extend(check_path(Path(raw_path)))

    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
