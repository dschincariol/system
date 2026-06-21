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

# --- Subsystem-README coverage (engine/*/) ---
ENGINE_DIR = ROOT / "engine"
INDEX_PATH = ROOT / "docs" / "DOCUMENTATION_INDEX.md"

# --- Env-var coverage gate ---
# ENV_READ_RE mirrors the documented grep ERE for env reads in code: it matches
# os.getenv / os.environ.get / os.environ[...] applied to an UPPER_SNAKE string
# literal. (Examples are intentionally not spelled out as matching literals here so
# this gate does not flag its own source.)
ENV_READ_RE = re.compile(
    r"""(?:os\.getenv|os\.environ\.get)\(\s*['"]([A-Z][A-Z0-9_]+)['"]"""
    r"""|os\.environ\[\s*['"]([A-Z][A-Z0-9_]+)['"]\s*\]"""
)
ENV_TOKEN_RE = re.compile(r"[A-Z][A-Z0-9_]+")
ENV_SCAN_DIRS = ("engine", "services", "routes", "tools", "boot", "ops", "scripts")
ENV_DOC_PATHS = (
    ROOT / ".env.example",
    ROOT / "docs" / "REFERENCE_CONFIGURATION_GLOSSARY.md",
)
ENV_ALLOWLIST_PATH = ROOT / "docs" / "config_env_allowlist.txt"

# --- Staleness sentinels ---
STALENESS_MARKER = "Last verified against code"
STALENESS_DOCS = (
    ROOT / "CLAUDE.md",
    ROOT / "docs" / "MAINTAINER_INDEX.md",
    ROOT / "docs" / "README_DEVELOPER_MAP.md",
    ROOT / "docs" / "README_ARCHITECTURE.md",
    ROOT / "docs" / "README_FUNCTION_MAP.md",
    ROOT / "docs" / "Database_Schema.md",
)


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


def _validate_subsystem_readmes() -> list[str]:
    """Require every engine/<sub>/ package to ship a README and an index link row.

    Excludes ``__pycache__`` and other dunder/hidden directories. This closes the
    historical gap where new engine subsystems shipped with no discoverable docs.
    """
    errors: list[str] = []
    if not ENGINE_DIR.exists():
        return errors
    index_text = INDEX_PATH.read_text(encoding="utf-8") if INDEX_PATH.exists() else ""
    for child in sorted(ENGINE_DIR.iterdir()):
        if not child.is_dir() or child.name.startswith((".", "_")):
            continue
        if not (child / "README.md").exists():
            errors.append(f"Subsystem missing README: engine/{child.name}/README.md")
        if f"engine/{child.name}/README.md" not in index_text:
            errors.append(
                f"docs/DOCUMENTATION_INDEX.md missing a link row for engine/{child.name}/README.md"
            )
    return errors


def _iter_code_py_files() -> list[Path]:
    seen: set[Path] = set()
    for directory in ENV_SCAN_DIRS:
        base = ROOT / directory
        if base.exists():
            seen.update(base.rglob("*.py"))
    seen.update(ROOT.glob("*.py"))  # root-level *.py only (matches the documented grep)
    return sorted(seen)


def _collect_code_env_vars() -> set[str]:
    found: set[str] = set()
    for path in _iter_code_py_files():
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for match in ENV_READ_RE.finditer(text):
            found.add(match.group(1) or match.group(2))
    return found


def _collect_documented_env_vars() -> set[str]:
    documented: set[str] = set()
    for path in ENV_DOC_PATHS:
        if path.exists():
            documented.update(ENV_TOKEN_RE.findall(path.read_text(encoding="utf-8")))
    if ENV_ALLOWLIST_PATH.exists():
        for line in ENV_ALLOWLIST_PATH.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                documented.add(stripped)
    return documented


def _validate_env_coverage() -> list[str]:
    """Fail when code reads an env var documented nowhere.

    A var counts as documented if it appears in ``.env.example``, the configuration
    glossary, or ``docs/config_env_allowlist.txt`` (the frozen legacy backlog). This
    blocks NEW undocumented vars without forcing the existing backlog to be cleared.
    """
    if not ENV_ALLOWLIST_PATH.exists():
        return [f"Missing env-var allowlist: {ENV_ALLOWLIST_PATH.relative_to(ROOT)}"]
    undocumented = sorted(_collect_code_env_vars() - _collect_documented_env_vars())
    if not undocumented:
        return []
    preview = ", ".join(undocumented[:20])
    extra = "" if len(undocumented) <= 20 else f" (+{len(undocumented) - 20} more)"
    return [
        f"{len(undocumented)} env var(s) read in code but undocumented in .env.example, "
        f"docs/REFERENCE_CONFIGURATION_GLOSSARY.md, or docs/config_env_allowlist.txt: "
        f"{preview}{extra}"
    ]


def _validate_staleness_sentinels() -> list[str]:
    """Require the map/index docs to carry a 'Last verified against code' line."""
    errors: list[str] = []
    for path in STALENESS_DOCS:
        if not path.exists():
            errors.append(f"Missing staleness-tracked doc: {path.relative_to(ROOT)}")
        elif STALENESS_MARKER not in path.read_text(encoding="utf-8"):
            errors.append(f"{path.relative_to(ROOT)} must contain a '{STALENESS_MARKER}' line")
    return errors


def main() -> int:
    """Run repository documentation validation and return a process exit code."""
    errors: list[str] = []
    files = _markdown_files()
    errors.extend(_validate_required_files())
    errors.extend(_validate_links(files))
    errors.extend(_validate_adrs())
    errors.extend(_validate_openapi_baseline())
    errors.extend(_validate_changelog())
    errors.extend(_validate_subsystem_readmes())
    errors.extend(_validate_env_coverage())
    errors.extend(_validate_staleness_sentinels())

    if errors:
        print("Documentation validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Documentation validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
