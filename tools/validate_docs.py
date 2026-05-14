"""Lightweight validation for repository documentation."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ADR_DIR = ROOT / "docs" / "adr"
OPENAPI_PATH = ROOT / "docs" / "openapi" / "openapi.yaml"
CHANGELOG_PATH = ROOT / "CHANGELOG.md"
REQUIRED_FILES = [
    ROOT / "CONTRIBUTING.md",
    ROOT / "CHANGELOG.md",
    ROOT / "docs" / "DOCSTRING_STYLE.md",
    ROOT / "docs" / "LICENSING_NOTE.md",
    ROOT / "docs" / "DOCUMENTATION_INDEX.md",
    ROOT / "docs" / "adr" / "README.md",
    ROOT / "docs" / "openapi" / "README.md",
    OPENAPI_PATH,
]
SKIP_PREFIXES = ("http://", "https://", "mailto:", "tel:")
LINK_RE = re.compile(r"(?<!\!)\[[^\]]+\]\(([^)]+)\)")
ADR_NAME_RE = re.compile(r"^(\d{4})-[a-z0-9-]+\.md$")


def _markdown_files() -> list[Path]:
    files: set[Path] = set()
    for path in (ROOT / "README.md", ROOT / "CONTRIBUTING.md", ROOT / "CHANGELOG.md"):
        if path.exists():
            files.add(path)
    for directory in ("docs", "engine", "boot", "services", "ui", "ops", "deploy"):
        base = ROOT / directory
        if not base.exists():
            continue
        files.update(base.rglob("*.md"))
    return sorted(files)


def _normalize_target(raw_target: str) -> str:
    target = raw_target.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1].strip()
    return target


def _split_target(target: str) -> tuple[str, str]:
    if "#" in target:
        path_part, anchor = target.split("#", 1)
        return path_part, anchor
    return target, ""


def _validate_links(files: list[Path]) -> list[str]:
    errors: list[str] = []
    for doc_path in files:
        text = doc_path.read_text(encoding="utf-8")
        for raw_target in LINK_RE.findall(text):
            target = _normalize_target(raw_target)
            if not target or target.startswith("#") or target.startswith(SKIP_PREFIXES):
                continue

            path_part, _anchor = _split_target(target)
            if not path_part:
                continue

            resolved = (doc_path.parent / path_part).resolve()
            if resolved.exists():
                continue

            errors.append(f"{doc_path.relative_to(ROOT)} -> missing link target: {target}")
    return errors


def _validate_required_files() -> list[str]:
    return [f"Missing required documentation file: {path.relative_to(ROOT)}" for path in REQUIRED_FILES if not path.exists()]


def _validate_adrs() -> list[str]:
    errors: list[str] = []
    if not ADR_DIR.exists():
        return ["Missing ADR directory: docs/adr"]

    adr_files = sorted(path for path in ADR_DIR.glob("*.md") if path.name != "README.md")
    if not adr_files:
        return ["No ADR files found in docs/adr"]

    numbers: list[int] = []
    for path in adr_files:
        match = ADR_NAME_RE.match(path.name)
        if not match:
            errors.append(f"ADR filename does not match NNNN-slug.md: {path.relative_to(ROOT)}")
            continue
        numbers.append(int(match.group(1)))

    if numbers:
        expected = list(range(1, len(numbers) + 1))
        observed = sorted(numbers)
        if observed != expected:
            errors.append(
                "ADR numbering must be sequential starting at 0001: "
                + ", ".join(f"{number:04d}" for number in observed)
            )
    return errors


def _validate_openapi_baseline() -> list[str]:
    errors: list[str] = []
    if not OPENAPI_PATH.exists():
        return [f"Missing OpenAPI baseline: {OPENAPI_PATH.relative_to(ROOT)}"]
    text = OPENAPI_PATH.read_text(encoding="utf-8")
    if "openapi:" not in text:
        errors.append("docs/openapi/openapi.yaml must declare an OpenAPI version")
    return errors


def _validate_changelog() -> list[str]:
    if not CHANGELOG_PATH.exists():
        return [f"Missing changelog: {CHANGELOG_PATH.relative_to(ROOT)}"]
    text = CHANGELOG_PATH.read_text(encoding="utf-8")
    if "## [Unreleased]" not in text:
        return ["CHANGELOG.md must contain an [Unreleased] section"]
    return []


def main() -> int:
    """Run repository documentation validation and return a process exit code."""
    errors: list[str] = []
    files = _markdown_files()
    errors.extend(_validate_required_files())
    errors.extend(_validate_links(files))
    errors.extend(_validate_adrs())
    errors.extend(_validate_openapi_baseline())
    errors.extend(_validate_changelog())

    if errors:
        print("Documentation validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Documentation validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
