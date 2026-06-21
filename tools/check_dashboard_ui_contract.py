"""
Focused dashboard UI contract helpers.

The checks here are intentionally static. They do not start the dashboard, open
broker connections, or require market-data credentials.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.check_local_asset_refs import (
    AssetReferenceIssue,
    iter_local_asset_refs,
    resolve_local_asset_ref,
)


DASHBOARD_HTML_SURFACES = (
    "ui/dashboard.html",
    "ui/data_sources.html",
    "ui/terminal/terminal.html",
    "ui/mobile/index.html",
)

LOCAL_API_PREFIXES = ("/api/",)
LOCAL_WEBSOCKET_PREFIXES = ("/ws/", "/socket/")
JS_SOURCE_SUFFIXES = {".js", ".mjs", ".cjs"}
SCANNABLE_ASSET_SUFFIXES = {".html", ".js", ".mjs", ".cjs", ".css"}
REQUIRED_NODE_VERSION = ">=20.17.0 <21"

ROUTE_TEMPLATE_SEGMENT_RE = re.compile(r"^\{[^{}]+\}$")
TEMPLATE_EXPR_RE = re.compile(r"\$\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}")


@dataclass(frozen=True)
class EndpointReference:
    source_path: str
    line: int
    raw_ref: str
    path: str
    transport: str


@dataclass(frozen=True)
class JsSyntaxIssue:
    source_path: str
    detail: str


@dataclass(frozen=True)
class RouteContractIssue:
    source_path: str
    line: int
    path: str
    transport: str
    reason: str


@dataclass(frozen=True)
class ScreenModuleBoundaryIssue:
    screen: str
    source_path: str
    reason: str
    detail: str


@dataclass(frozen=True)
class _StringLiteral:
    value: str
    line: int
    start: int
    end: int
    quote: str


def _normalize_repo_rel(path: str | Path) -> str:
    return Path(path).as_posix().lstrip("./")


def _read_text(root: Path, rel_path: str) -> str:
    return (root / rel_path).read_text(encoding="utf-8", errors="ignore")


def _line_for_offset(text: str, offset: int) -> int:
    return int(text.count("\n", 0, max(0, offset)) + 1)


def collect_dashboard_asset_graph(
    *,
    root: Path = ROOT,
    html_surfaces: Iterable[str] = DASHBOARD_HTML_SURFACES,
) -> tuple[set[str], list[AssetReferenceIssue]]:
    """Return local dashboard assets reachable from HTML plus missing refs."""

    root = root.resolve()
    seen: set[str] = set()
    queue = [_normalize_repo_rel(item) for item in html_surfaces]
    local_assets: set[str] = set(queue)
    issues: list[AssetReferenceIssue] = []

    while queue:
        rel_path = _normalize_repo_rel(queue.pop(0))
        if rel_path in seen:
            continue
        seen.add(rel_path)

        file_path = root / rel_path
        if not file_path.exists():
            issues.append(
                AssetReferenceIssue(
                    source_path=rel_path,
                    line=1,
                    raw_ref=rel_path,
                    resolved_path=rel_path,
                    reason="missing",
                    kind="entrypoint",
                )
            )
            continue

        suffix = file_path.suffix.lower()
        if suffix not in SCANNABLE_ASSET_SUFFIXES:
            continue

        text = _read_text(root, rel_path)
        for raw_ref, line, kind in iter_local_asset_refs(rel_path, text):
            resolved_path = resolve_local_asset_ref(raw_ref, rel_path, root=root)
            if not resolved_path:
                continue

            local_assets.add(resolved_path)
            target = root / resolved_path
            if not target.exists():
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

            if target.suffix.lower() in SCANNABLE_ASSET_SUFFIXES and resolved_path not in seen:
                queue.append(resolved_path)

    return local_assets, issues


def collect_dashboard_js_modules(
    *,
    root: Path = ROOT,
    html_surfaces: Iterable[str] = DASHBOARD_HTML_SURFACES,
) -> list[str]:
    assets, _issues = collect_dashboard_asset_graph(root=root, html_surfaces=html_surfaces)
    return sorted(path for path in assets if Path(path).suffix.lower() in JS_SOURCE_SUFFIXES)


def _iter_js_string_literals(text: str) -> list[_StringLiteral]:
    literals: list[_StringLiteral] = []
    i = 0
    line = 1
    length = len(text)

    while i < length:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < length else ""

        if ch == "\n":
            line += 1
            i += 1
            continue

        if ch == "/" and nxt == "/":
            i += 2
            while i < length and text[i] != "\n":
                i += 1
            continue

        if ch == "/" and nxt == "*":
            i += 2
            while i < length:
                if text[i] == "\n":
                    line += 1
                    i += 1
                    continue
                if text[i] == "*" and i + 1 < length and text[i + 1] == "/":
                    i += 2
                    break
                i += 1
            continue

        if ch not in ("'", '"', "`"):
            i += 1
            continue

        quote = ch
        start = i
        start_line = line
        i += 1
        value_chars: list[str] = []
        while i < length:
            cur = text[i]
            if cur == "\\":
                if i + 1 < length:
                    escaped = text[i + 1]
                    value_chars.append(escaped)
                    if escaped == "\n":
                        line += 1
                    i += 2
                    continue
                i += 1
                continue
            if cur == quote:
                i += 1
                literals.append(
                    _StringLiteral(
                        value="".join(value_chars),
                        line=start_line,
                        start=start,
                        end=i,
                        quote=quote,
                    )
                )
                break
            value_chars.append(cur)
            if cur == "\n":
                line += 1
            i += 1
        else:
            break

    return literals


def _strip_ref_suffix(raw_ref: str) -> str:
    ref = str(raw_ref or "").strip()
    if not ref:
        return ""
    ref = ref.split("#", 1)[0].strip()
    ref = ref.split("?", 1)[0].strip()
    return ref


def normalize_endpoint_path(raw_ref: str) -> str:
    ref = _strip_ref_suffix(raw_ref)
    if not ref:
        return ""
    ref = TEMPLATE_EXPR_RE.sub("{param}", ref)
    if ref.startswith("http://") or ref.startswith("https://"):
        return ""
    return ref


def _endpoint_transport(path: str) -> str | None:
    if path.startswith(LOCAL_WEBSOCKET_PREFIXES):
        return "websocket"
    if path.startswith(LOCAL_API_PREFIXES):
        return "http"
    return None


def _collect_eventsource_paths(text: str, literals: list[_StringLiteral]) -> set[tuple[int, str]]:
    assignments: list[tuple[str, int, int, str]] = []
    assignment_re = re.compile(
        r"\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*([`'\"])(/api/.*?)(?<!\\)\2",
        re.DOTALL,
    )
    for match in assignment_re.finditer(text):
        path = normalize_endpoint_path(match.group(3))
        if path:
            assignments.append((match.group(1), match.start(), _line_for_offset(text, match.start(3)), path))

    out: set[tuple[int, str]] = set()
    direct_re = re.compile(r"\bnew\s+EventSource\s*\(\s*([`'\"])(/api/.*?)(?<!\\)\1", re.DOTALL)
    for match in direct_re.finditer(text):
        path = normalize_endpoint_path(match.group(2))
        if path:
            out.add((_line_for_offset(text, match.start(2)), path))

    indirect_re = re.compile(r"\bnew\s+EventSource\s*\(\s*([A-Za-z_$][\w$]*)")
    for match in indirect_re.finditer(text):
        name = match.group(1)
        prior = [row for row in assignments if row[0] == name and row[1] < match.start()]
        if prior:
            _var_name, _offset, line, path = prior[-1]
            out.add((line, path))

    return out


def iter_endpoint_references(js_path: str, text: str) -> list[EndpointReference]:
    literals = _iter_js_string_literals(text)
    eventsource_paths = _collect_eventsource_paths(text, literals)
    refs: list[EndpointReference] = []
    seen: set[tuple[str, int, str, str]] = set()

    for literal in literals:
        call_context = text[max(0, literal.start - 48) : literal.start]
        if re.search(r"\.(?:startsWith|endsWith|includes|match|test)\s*\(\s*$", call_context):
            continue
        path = normalize_endpoint_path(literal.value)
        transport = _endpoint_transport(path)
        if not transport:
            continue
        if (literal.line, path) in eventsource_paths:
            transport = "eventsource"
        key = (js_path, literal.line, path, transport)
        if key in seen:
            continue
        seen.add(key)
        refs.append(
            EndpointReference(
                source_path=js_path,
                line=int(literal.line),
                raw_ref=literal.value,
                path=path,
                transport=transport,
            )
        )

    refs.sort(key=lambda item: (item.source_path, item.line, item.path, item.transport))
    return refs


def collect_dashboard_endpoint_references(
    *,
    root: Path = ROOT,
    html_surfaces: Iterable[str] = DASHBOARD_HTML_SURFACES,
) -> list[EndpointReference]:
    refs: list[EndpointReference] = []
    for js_path in collect_dashboard_js_modules(root=root, html_surfaces=html_surfaces):
        refs.extend(iter_endpoint_references(js_path, _read_text(root, js_path)))
    return refs


def route_key(route: object) -> tuple[str, str]:
    if isinstance(route, dict):
        return (str(route.get("method") or "").upper(), str(route.get("path") or ""))
    if isinstance(route, tuple) and len(route) >= 2:
        return (str(route[0] or "").upper(), str(route[1] or ""))
    return ("", "")


def route_handler(route: object) -> str:
    if isinstance(route, dict):
        return str(route.get("handler") or "")
    if isinstance(route, tuple) and len(route) >= 3:
        return str(route[2] or "")
    return ""


def _path_segments(path: str) -> list[str]:
    clean = _strip_ref_suffix(path)
    return [segment for segment in clean.split("/") if segment]


def route_path_matches(route_path: str, endpoint_path: str) -> bool:
    route_segments = _path_segments(route_path)
    endpoint_segments = _path_segments(endpoint_path)
    if len(route_segments) != len(endpoint_segments):
        return False

    for route_segment, endpoint_segment in zip(route_segments, endpoint_segments):
        if ROUTE_TEMPLATE_SEGMENT_RE.match(route_segment):
            continue
        if ROUTE_TEMPLATE_SEGMENT_RE.match(endpoint_segment):
            continue
        if route_segment != endpoint_segment:
            return False
    return True


def route_path_registered(route_specs: Iterable[object], endpoint_path: str) -> bool:
    return any(route_path_matches(route_key(route)[1], endpoint_path) for route in route_specs)


def find_unregistered_endpoint_references(
    endpoint_refs: Iterable[EndpointReference],
    route_specs: Iterable[object],
    *,
    optional_allowlist: dict[str, str] | None = None,
) -> list[RouteContractIssue]:
    optional = dict(optional_allowlist or {})
    issues: list[RouteContractIssue] = []
    for ref in endpoint_refs:
        if ref.transport == "websocket":
            continue
        if ref.path in optional:
            continue
        if not route_path_registered(route_specs, ref.path):
            issues.append(
                RouteContractIssue(
                    source_path=ref.source_path,
                    line=ref.line,
                    path=ref.path,
                    transport=ref.transport,
                    reason="route_not_registered",
                )
            )
    return issues


def find_js_syntax_issues(
    js_paths: Iterable[str],
    *,
    root: Path = ROOT,
    node_executable: str | None = None,
) -> list[JsSyntaxIssue]:
    node = node_executable or shutil.which("node")
    if not node:
        return [
            JsSyntaxIssue(
                source_path="node",
                detail=(
                    "Node.js executable not found on PATH. Install Node.js 20 LTS "
                    f"({REQUIRED_NODE_VERSION}) with npm 10, run npm ci, then rerun npm run check:ui."
                ),
            )
        ]

    version_result = subprocess.run(
        [node, "-p", "process.versions.node"],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )
    version_text = (version_result.stdout or version_result.stderr or "").strip()
    version_match = re.search(r"(\d+)\.(\d+)\.(\d+)", version_text)
    if version_result.returncode != 0 or not version_match:
        return [
            JsSyntaxIssue(
                source_path="node",
                detail=(
                    "Unable to determine Node.js version. Install Node.js 20 LTS "
                    f"({REQUIRED_NODE_VERSION}) with npm 10, run npm ci, then rerun npm run check:ui."
                ),
            )
        ]
    major, minor, patch = (int(part) for part in version_match.groups())
    if major != 20 or (major, minor, patch) < (20, 17, 0):
        return [
            JsSyntaxIssue(
                source_path="node",
                detail=(
                    f"Unsupported Node.js version {version_text}. Dashboard UI validation requires "
                    f"Node.js {REQUIRED_NODE_VERSION}; install Node.js 20 LTS, run npm ci, "
                    "then rerun npm run check:ui."
                ),
            )
        ]

    issues: list[JsSyntaxIssue] = []
    for rel_path in sorted({_normalize_repo_rel(path) for path in js_paths}):
        result = subprocess.run(
            [node, "--check", str(root / rel_path)],
            cwd=str(root),
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "node_check_failed").strip()
            issues.append(JsSyntaxIssue(source_path=rel_path, detail=detail))
    return issues


def find_screen_module_boundary_issues(*, root: Path = ROOT) -> list[ScreenModuleBoundaryIssue]:
    """Keep large dashboard screen controllers in dedicated modules."""

    boundaries = [
        {
            "screen": "Data Health",
            "dashboard_path": "ui/dashboard.js",
            "module_path": "ui/data_health.js",
            "required_dashboard_snippets": [
                'from "./data_health.js"',
                "loadDataHealthScreenModule({",
            ],
            "forbidden_dashboard_snippets": [
                "/api/operator/provider_telemetry",
                "/api/data/feature_visibility?limit=12",
                '"dataProvidersBody"',
                '"dataHealthNotes"',
                '"dataRuntimeGrid"',
            ],
            "required_module_snippets": [
                "export async function fetchDataHealthScreen",
                "export function normalizeDataHealthScreen",
                "export function renderDataHealthScreen",
                "export async function loadDataHealthScreen",
                "/api/operator/provider_telemetry",
                "/api/data/feature_visibility?limit=12",
                '"dataProvidersBody"',
                '"dataHealthNotes"',
                '"dataRuntimeGrid"',
            ],
        },
    ]

    issues: list[ScreenModuleBoundaryIssue] = []
    for boundary in boundaries:
        dashboard_path = str(boundary["dashboard_path"])
        module_path = str(boundary["module_path"])
        dashboard_file = root / dashboard_path
        module_file = root / module_path
        screen = str(boundary["screen"])

        if not module_file.exists():
            issues.append(
                ScreenModuleBoundaryIssue(
                    screen=screen,
                    source_path=module_path,
                    reason="missing_module",
                    detail=f"{module_path} must own the {screen} screen controller.",
                )
            )
            continue
        if not dashboard_file.exists():
            issues.append(
                ScreenModuleBoundaryIssue(
                    screen=screen,
                    source_path=dashboard_path,
                    reason="missing_dashboard",
                    detail=f"{dashboard_path} was not found.",
                )
            )
            continue

        dashboard_text = _read_text(root, dashboard_path)
        module_text = _read_text(root, module_path)

        for snippet in boundary["required_dashboard_snippets"]:
            if str(snippet) not in dashboard_text:
                issues.append(
                    ScreenModuleBoundaryIssue(
                        screen=screen,
                        source_path=dashboard_path,
                        reason="missing_dashboard_delegation",
                        detail=f"dashboard.js must delegate {screen} through {module_path}: missing {snippet!r}.",
                    )
                )

        for snippet in boundary["forbidden_dashboard_snippets"]:
            if str(snippet) in dashboard_text:
                issues.append(
                    ScreenModuleBoundaryIssue(
                        screen=screen,
                        source_path=dashboard_path,
                        reason="centralized_screen_logic",
                        detail=f"{screen} screen-specific contract {snippet!r} belongs in {module_path}, not dashboard.js.",
                    )
                )

        for snippet in boundary["required_module_snippets"]:
            if str(snippet) not in module_text:
                issues.append(
                    ScreenModuleBoundaryIssue(
                        screen=screen,
                        source_path=module_path,
                        reason="missing_module_contract",
                        detail=f"{module_path} is missing {screen} screen contract snippet {snippet!r}.",
                    )
                )

    return issues


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report static dashboard UI contract refs.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument(
        "--node-executable",
        default=None,
        help="Node.js executable to use for JS syntax checks. Defaults to node on PATH.",
    )
    parser.add_argument("--list-endpoints", action="store_true")
    parser.add_argument("--list-assets", action="store_true")
    args = parser.parse_args(argv)

    root = Path(args.root).resolve()
    assets, asset_issues = collect_dashboard_asset_graph(root=root)
    endpoint_refs = collect_dashboard_endpoint_references(root=root)
    syntax_issues = find_js_syntax_issues(
        collect_dashboard_js_modules(root=root),
        root=root,
        node_executable=args.node_executable,
    )
    boundary_issues = find_screen_module_boundary_issues(root=root)

    if args.list_assets:
        for path in sorted(assets):
            print(path)

    if args.list_endpoints:
        for ref in endpoint_refs:
            print(f"{ref.source_path}:{ref.line}: {ref.transport}: {ref.path}")

    if asset_issues:
        print("Dashboard asset contract failed.")
        for issue in asset_issues:
            print(f"{issue.source_path}:{issue.line}: {issue.reason}: {issue.raw_ref} -> {issue.resolved_path}")
        return 1

    if syntax_issues:
        print("Dashboard JS syntax contract failed.")
        for issue in syntax_issues:
            print(f"{issue.source_path}: {issue.detail}")
        return 1

    if boundary_issues:
        print("Dashboard screen module boundary contract failed.")
        for issue in boundary_issues:
            print(f"{issue.source_path}: {issue.reason}: {issue.detail}")
        return 1

    print(
        "Dashboard UI static contract passed. "
        f"Assets={len(assets)} endpoints={len(endpoint_refs)} js_modules={len(collect_dashboard_js_modules(root=root))}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
