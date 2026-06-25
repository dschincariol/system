"""
Validate that tracked local UI asset references resolve to tracked repo files.

This catches clone-time breakage where tracked HTML/JS/CSS points at files that
exist only in a local worktree or are missing entirely.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCANNABLE_SUFFIXES = {".html", ".js", ".mjs", ".cjs", ".css"}
EXTERNAL_PREFIXES = ("http://", "https://", "//", "data:", "blob:", "mailto:", "tel:", "javascript:", "node:")
IGNORED_LOCAL_PREFIXES = ("/api/", "/ws/", "/socket/")
TEMPLATE_TOKENS = ("${", "{{", "}}", "<%", "%>")
CHARTJS_VENDOR_BUNDLE_RE = re.compile(
    r"""(?:^chart(?:\.(?:umd|esm|cjs))?(?:\.min)?\.js$|(?:^|[-_.])chartjs(?:[-_.]|$))""",
    re.IGNORECASE,
)

HTML_REF_RE = re.compile(
    r"""<(?:script|link|img|source|audio|video)\b[^>]*?\b(?:src|href)=["']([^"']+)["']""",
    re.IGNORECASE | re.DOTALL,
)
CSS_IMPORT_RE = re.compile(
    r"""@import\s+(?:url\(\s*)?["']([^"']+)["']\s*\)?""",
    re.IGNORECASE,
)
ESM_IMPORT_RE = re.compile(
    r"""\b(?:import|export)\s+(?:(?:[^;'"`]*?\bfrom\s*)?["']([^"']+)["'])""",
    re.DOTALL,
)
DYNAMIC_IMPORT_RE = re.compile(
    r"""\bimport\(\s*["']([^"']+)["']\s*\)""",
    re.DOTALL,
)


@dataclass(frozen=True)
class AssetReferenceIssue:
    source_path: str
    line: int
    raw_ref: str
    resolved_path: str
    reason: str
    kind: str


def _normalize_repo_rel(path: str | Path) -> str:
    return Path(path).as_posix().lstrip("./")


def load_tracked_paths(root: Path = ROOT) -> set[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=str(root),
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"git_ls_files_failed: {exc}") from exc
    return {
        _normalize_repo_rel(line.strip())
        for line in result.stdout.splitlines()
        if str(line or "").strip()
    }


def iter_scannable_paths(tracked_paths: set[str]) -> list[str]:
    return sorted(
        rel
        for rel in tracked_paths
        if Path(rel).suffix.lower() in SCANNABLE_SUFFIXES
    )


def is_disallowed_vendor_asset(rel_path: str | Path) -> bool:
    rel = _normalize_repo_rel(rel_path)
    path = Path(rel)
    if path.parent.as_posix() != "ui/vendor":
        return False
    return bool(CHARTJS_VENDOR_BUNDLE_RE.search(path.name))


def find_disallowed_vendor_assets(tracked_paths: set[str] | None = None, root: Path = ROOT) -> list[str]:
    tracked = {_normalize_repo_rel(item) for item in (tracked_paths or load_tracked_paths(root))}
    return sorted(rel_path for rel_path in tracked if is_disallowed_vendor_asset(rel_path))


def _match_line(text: str, start_index: int) -> int:
    return int(text.count("\n", 0, start_index) + 1)


def iter_local_asset_refs(source_rel: str | Path, text: str) -> list[tuple[str, int, str]]:
    suffix = Path(source_rel).suffix.lower()
    patterns: list[tuple[re.Pattern[str], str]] = []
    if suffix == ".html":
        patterns.append((HTML_REF_RE, "html"))
    elif suffix in {".js", ".mjs", ".cjs"}:
        patterns.extend(((ESM_IMPORT_RE, "js-import"), (DYNAMIC_IMPORT_RE, "js-import")))
    elif suffix == ".css":
        patterns.append((CSS_IMPORT_RE, "css-import"))
    else:
        return []

    refs: list[tuple[str, int, str]] = []
    seen: set[tuple[str, int, str]] = set()
    for pattern, kind in patterns:
        for match in pattern.finditer(text):
            raw_ref = str(match.group(1) or "").strip()
            if not raw_ref:
                continue
            line = _match_line(text, match.start(1))
            key = (raw_ref, line, kind)
            if key in seen:
                continue
            seen.add(key)
            refs.append((raw_ref, line, kind))
    refs.sort(key=lambda item: (item[1], item[0], item[2]))
    return refs


def _strip_ref_suffix(raw_ref: str) -> str:
    ref = str(raw_ref or "").strip()
    if not ref:
        return ""
    ref = ref.split("#", 1)[0].strip()
    ref = ref.split("?", 1)[0].strip()
    return ref


def resolve_local_asset_ref(
    raw_ref: str,
    source_path: str | Path,
    *,
    root: Path = ROOT,
) -> str | None:
    ref = _strip_ref_suffix(raw_ref)
    if not ref or ref.startswith("#"):
        return None
    if any(ref.startswith(prefix) for prefix in EXTERNAL_PREFIXES):
        return None
    if any(ref.startswith(prefix) for prefix in IGNORED_LOCAL_PREFIXES):
        return None
    if any(token in ref for token in TEMPLATE_TOKENS) or "*" in ref:
        return None

    root_resolved = root.resolve()
    source_rel = Path(_normalize_repo_rel(source_path))
    if ref.startswith("/"):
        target = (root_resolved / ref.lstrip("/")).resolve(strict=False)
    else:
        target = (root_resolved / source_rel.parent / ref).resolve(strict=False)

    try:
        return _normalize_repo_rel(target.relative_to(root_resolved))
    except ValueError:
        return None


def find_local_asset_reference_issues(
    *,
    root: Path = ROOT,
    tracked_paths: set[str] | None = None,
) -> list[AssetReferenceIssue]:
    root = root.resolve()
    tracked = {_normalize_repo_rel(item) for item in (tracked_paths or load_tracked_paths(root))}
    issues: list[AssetReferenceIssue] = []

    for rel_path in iter_scannable_paths(tracked):
        file_path = root / rel_path
        if not file_path.exists():
            continue
        text = file_path.read_text(encoding="utf-8", errors="ignore")
        for raw_ref, line, kind in iter_local_asset_refs(rel_path, text):
            resolved_path = resolve_local_asset_ref(raw_ref, rel_path, root=root)
            if not resolved_path:
                continue

            target_path = root / resolved_path
            if not target_path.exists():
                issues.append(
                    AssetReferenceIssue(
                        source_path=rel_path,
                        line=int(line),
                        raw_ref=raw_ref,
                        resolved_path=resolved_path,
                        reason="missing",
                        kind=kind,
                    )
                )
                continue

            if resolved_path not in tracked:
                issues.append(
                    AssetReferenceIssue(
                        source_path=rel_path,
                        line=int(line),
                        raw_ref=raw_ref,
                        resolved_path=resolved_path,
                        reason="untracked",
                        kind=kind,
                    )
                )

    return issues


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate tracked local UI asset references.")
    parser.add_argument(
        "--root",
        default=str(ROOT),
        help="Repository root to scan. Defaults to the current repo.",
    )
    args = parser.parse_args(argv)

    root = Path(str(args.root)).resolve()
    issues = find_local_asset_reference_issues(root=root)
    disallowed_assets = find_disallowed_vendor_assets(root=root)
    if disallowed_assets:
        print("Disallowed vendored asset validation failed.")
        for rel_path in disallowed_assets:
            print(f"{rel_path}: Chart.js is not a supported vendored charting runtime")
        return 1

    if issues:
        print("Local asset reference validation failed.")
        for issue in issues:
            print(
                f"{issue.source_path}:{issue.line}: {issue.reason}:{issue.kind}: "
                f"{issue.raw_ref} -> {issue.resolved_path}"
            )
        return 1

    tracked_paths = load_tracked_paths(root)
    print(
        "Local asset reference validation passed. "
        f"Scanned {len(iter_scannable_paths(tracked_paths))} tracked source files."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
