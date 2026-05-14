"""System audit — Layer 1.

Pure-static AST + regex sweep over engine/, services/, ops/server/ to
flag stub bodies, silent exception swallows, NotImplementedError
raises, suspicious literal returns, mock-shaped names in production
code, magic localhost / 127.0.0.1 outside the platform helpers, print
statements in engine code, bare excepts, and TODO/FIXME/XXX/HACK
markers.

Outputs JSON (machine-readable) + a markdown table grouped by
severity and subsystem, each finding ending with a self-contained
Codex prompt the operator can paste verbatim.

Run from the repo root:

    python tools/system_audit.py

Outputs:

    docs/System_Audit_Layer1.json
    docs/System_Audit_Layer1.md
"""

from __future__ import annotations

import ast
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable

REPO = Path(__file__).resolve().parents[1]

INCLUDE_DIRS = ["engine", "services"]
EXCLUDE_DIRS = {
    "__pycache__", ".venv", ".git", "node_modules", ".pytest_cache",
    ".ruff_cache", ".mypy_cache", "tests", "test", "build", "dist",
}
EXCLUDE_FILE_GLOBS = {
    "**/__init__.py",
    "**/_version.py",
}

CLI_ENTRYPOINT_RE = re.compile(r'if\s+__name__\s*==\s*[\'\"]\__main__\__[\'\"]', re.MULTILINE)
PRIVATE_OK_PRINT_FILES = {
    "engine/audit/cli.py",  # CLI tool, prints are output
}

SAFETY_SUBSYSTEMS = {
    "audit", "secrets", "risk", "execution", "broker", "kill_switch",
    "promotion", "credential",
}


@dataclass
class Finding:
    id: str
    subsystem: str
    file: str
    line: int
    end_line: int
    category: str
    severity: str
    summary: str
    evidence: str
    recommended_prompt: str


@dataclass
class AuditState:
    findings: list[Finding] = field(default_factory=list)
    counter: dict[str, int] = field(default_factory=dict)

    def emit(self, **kwargs) -> None:
        category = kwargs["category"]
        rel_file = kwargs["file"]
        slug_path = (
            Path(rel_file).with_suffix("").as_posix().replace("/", ".")
        )
        key = f"{category.upper()}-{slug_path}"
        self.counter.setdefault(key, 0)
        self.counter[key] += 1
        kwargs["id"] = f"{key}-{self.counter[key]:04d}"
        self.findings.append(Finding(**kwargs))


def subsystem_of(rel: str) -> str:
    parts = rel.split("/")
    if len(parts) < 2:
        return parts[0]
    if parts[0] == "engine":
        if len(parts) >= 3 and parts[1] == "strategy":
            if parts[2] in {"models", "statistics", "ensemble", "tuning",
                            "discovery", "jobs"}:
                return f"strategy.{parts[2]}"
            return "strategy"
        return f"engine.{parts[1]}"
    if parts[0] == "services":
        if len(parts) >= 2 and parts[1] == "secrets":
            return "secrets"
        return f"services.{parts[1]}"
    return parts[0]


def severity_for(category: str, subsystem: str, summary: str) -> str:
    safety = any(s in subsystem for s in SAFETY_SUBSYSTEMS)
    if category in {"not_impl", "stub"} and safety:
        return "P0"
    if category in {"silent_except", "bare_except"} and safety:
        return "P0"
    if category in {"suspicious_literal_return"} and safety:
        return "P1"
    if category in {"not_impl", "stub"}:
        return "P1"
    if category in {"silent_except", "bare_except"}:
        return "P1"
    if category == "spec_magic":
        return "P1"
    if category in {"todo_marker", "print_in_engine", "mock_in_prod"}:
        return "P2"
    return "P2"


def excerpt(source: str, line: int, context: int = 2) -> str:
    lines = source.splitlines()
    a = max(0, line - 1 - context)
    b = min(len(lines), line + context)
    out = []
    for i in range(a, b):
        marker = ">>>" if (i + 1) == line else "   "
        out.append(f"{marker} {i + 1:5d}  {lines[i]}")
    return "\n".join(out)


# ---------- AST detectors ----------

class _BodyAnalyzer:
    """Classify a function body for stub-likeness."""

    def __init__(self, body: list[ast.stmt]) -> None:
        # Drop a leading docstring if present.
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
            self.body = body[1:]
        else:
            self.body = body

    def is_pass_only(self) -> bool:
        return len(self.body) == 1 and isinstance(self.body[0], ast.Pass)

    def is_ellipsis_only(self) -> bool:
        if len(self.body) != 1:
            return False
        s = self.body[0]
        return isinstance(s, ast.Expr) and isinstance(s.value, ast.Constant) and s.value.value is Ellipsis

    def is_return_none_only(self) -> bool:
        if len(self.body) != 1:
            return False
        s = self.body[0]
        if not isinstance(s, ast.Return):
            return False
        if s.value is None:
            return True
        return isinstance(s.value, ast.Constant) and s.value.value is None

    def is_return_empty_collection(self) -> bool:
        if len(self.body) != 1:
            return False
        s = self.body[0]
        if not isinstance(s, ast.Return) or s.value is None:
            return False
        v = s.value
        if isinstance(v, ast.Dict) and not v.keys:
            return True
        if isinstance(v, ast.List) and not v.elts:
            return True
        if isinstance(v, ast.Tuple) and not v.elts:
            return True
        if isinstance(v, ast.Set) and not v.elts:
            return True
        return False

    def is_raise_not_implemented_only(self) -> bool:
        if len(self.body) != 1:
            return False
        s = self.body[0]
        if not isinstance(s, ast.Raise) or s.exc is None:
            return False
        target = s.exc.func if isinstance(s.exc, ast.Call) else s.exc
        return isinstance(target, ast.Name) and target.id == "NotImplementedError"

    def stub_kind(self) -> str | None:
        if self.is_pass_only():
            return "pass-only"
        if self.is_ellipsis_only():
            return "ellipsis-only"
        if self.is_return_none_only():
            return "return-None-only"
        if self.is_return_empty_collection():
            return "return-empty-collection-only"
        if self.is_raise_not_implemented_only():
            return "NotImplementedError-only"
        return None


def detect_stubs_and_not_impl(tree: ast.AST, src: str, rel: str, st: AuditState) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if any(isinstance(d, ast.Name) and d.id in {"abstractmethod", "abstractproperty"} for d in node.decorator_list):
            continue
        if any(isinstance(d, ast.Attribute) and d.attr in {"abstractmethod", "abstractproperty"} for d in node.decorator_list):
            continue
        if any(isinstance(d, ast.Name) and d.id == "overload" for d in node.decorator_list):
            continue
        analyzer = _BodyAnalyzer(node.body)
        kind = analyzer.stub_kind()
        if kind is None:
            continue
        category = "not_impl" if kind == "NotImplementedError-only" else "stub"
        sub = subsystem_of(rel)
        st.emit(
            subsystem=sub,
            file=rel,
            line=node.lineno,
            end_line=getattr(node, "end_lineno", node.lineno),
            category=category,
            severity=severity_for(category, sub, ""),
            summary=f"Function `{node.name}` body is {kind}",
            evidence=excerpt(src, node.lineno, context=3),
            recommended_prompt=(
                f"Open `{rel}:{node.lineno}`. The function `{node.name}` has a "
                f"{kind} body. Either implement the contract documented in its "
                f"docstring (read the surrounding module to infer the expected "
                f"behaviour), or delete the function plus every caller that "
                f"depends on it. If it is part of an interface that subclasses "
                f"override, decorate it `@abstractmethod` and ensure every "
                f"subclass provides an implementation. Add a unit test that "
                f"exercises the new behaviour and fails before the change."
            ),
        )


# ---------- Exception swallow detectors ----------

def _handler_is_silent(handler: ast.ExceptHandler) -> tuple[bool, str]:
    body = handler.body
    if len(body) == 1 and isinstance(body[0], ast.Pass):
        return True, "except: pass"
    # except: log.debug(...)  with no re-raise
    only_logs = all(_is_logging_call(stmt) for stmt in body)
    if only_logs and not any(isinstance(s, ast.Raise) for s in body):
        return True, "except: <log only, no re-raise>"
    return False, ""


def _is_logging_call(stmt: ast.stmt) -> bool:
    if not isinstance(stmt, ast.Expr):
        return False
    call = stmt.value
    if not isinstance(call, ast.Call):
        return False
    func = call.func
    if isinstance(func, ast.Attribute):
        return func.attr in {"debug", "info", "warning", "error", "exception", "warn"}
    return False


def detect_silent_exceptions(tree: ast.AST, src: str, rel: str, st: AuditState) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Try):
            continue
        for h in node.handlers:
            silent, label = _handler_is_silent(h)
            is_bare = h.type is None
            if not (silent or is_bare):
                continue
            sub = subsystem_of(rel)
            cat = "bare_except" if is_bare and not silent else ("silent_except" if silent else "bare_except")
            label_text = "except <bare>: ..." if is_bare else label
            st.emit(
                subsystem=sub,
                file=rel,
                line=h.lineno,
                end_line=getattr(h, "end_lineno", h.lineno),
                category=cat,
                severity=severity_for(cat, sub, ""),
                summary=f"`{label_text}` swallows or hides exceptions",
                evidence=excerpt(src, h.lineno, context=3),
                recommended_prompt=(
                    f"Open `{rel}:{h.lineno}`. The except clause `{label_text}` "
                    f"hides exceptions. Audit each operation in the protected "
                    f"`try` block and identify which exception types are "
                    f"genuinely recoverable. Catch only those, log them with "
                    f"context, and let everything else propagate. Never use "
                    f"`except: pass` in production code; if the intent is "
                    f"\"best-effort, ignore failures,\" record a structured "
                    f"observation via `record_component_health` or "
                    f"`runtime_metrics` so the silence is observable."
                ),
            )


# ---------- Suspicious literal returns ----------

def detect_suspicious_literal_returns(tree: ast.AST, src: str, rel: str, st: AuditState) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith("_"):
            continue
        body = node.body
        if not body:
            continue
        # Skip clear non-stubs
        if len(body) > 12:
            continue
        # Look for functions whose ONLY return statement is a literal
        returns = [s for s in ast.walk(node) if isinstance(s, ast.Return)]
        if not returns:
            continue
        all_literal = all(
            r.value is not None and isinstance(r.value, ast.Constant) for r in returns
        )
        all_simple_literal = (
            all_literal
            and len(returns) >= 1
            and len(body) >= 3  # not a one-liner
        )
        if not all_simple_literal:
            continue
        sub = subsystem_of(rel)
        cat = "suspicious_literal_return"
        sev = severity_for(cat, sub, "")
        st.emit(
            subsystem=sub,
            file=rel,
            line=node.lineno,
            end_line=getattr(node, "end_lineno", node.lineno),
            category=cat,
            severity=sev,
            summary=f"Function `{node.name}` only returns hardcoded literals across all paths",
            evidence=excerpt(src, node.lineno, context=4),
            recommended_prompt=(
                f"Open `{rel}:{node.lineno}`. Function `{node.name}` always returns "
                f"a hardcoded literal regardless of input. Confirm that this is "
                f"intentional (e.g. a Protocol stub, a feature flag default, a "
                f"version constant). If not, implement the real logic. If it is "
                f"intentional, consider replacing the function with a module-level "
                f"constant or a `@property` for clarity."
            ),
        )


# ---------- Mock-shaped names in production ----------

MOCK_PREFIXES = ("fake_", "stub_", "placeholder_", "demo_", "dummy_", "mock_")


def detect_mock_in_prod(tree: ast.AST, src: str, rel: str, st: AuditState) -> None:
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        name = node.name.lower()
        if not any(name.startswith(p) for p in MOCK_PREFIXES):
            continue
        sub = subsystem_of(rel)
        cat = "mock_in_prod"
        st.emit(
            subsystem=sub,
            file=rel,
            line=node.lineno,
            end_line=getattr(node, "end_lineno", node.lineno),
            category=cat,
            severity=severity_for(cat, sub, ""),
            summary=f"Symbol `{node.name}` looks like a stand-in (prefix `{[p for p in MOCK_PREFIXES if name.startswith(p)][0]}`)",
            evidence=excerpt(src, node.lineno, context=3),
            recommended_prompt=(
                f"Open `{rel}:{node.lineno}`. The symbol `{node.name}` is named "
                f"like a placeholder. Confirm whether it ships in production. "
                f"If it is genuinely test-only, move it to `tests/`. If it is a "
                f"production fallback, rename it to reflect the real role and "
                f"document the contract."
            ),
        )


# ---------- Magic 127.0.0.1 / localhost in production ----------

LOCALHOST_RE = re.compile(r'\b(?:127\.0\.0\.1|localhost)\b')
ALLOW_LOCALHOST_FILES = {
    "engine/runtime/platform.py",
}


def detect_magic_localhost(src: str, rel: str, st: AuditState) -> None:
    if rel in ALLOW_LOCALHOST_FILES:
        return
    for m in LOCALHOST_RE.finditer(src):
        # find line number
        line = src.count("\n", 0, m.start()) + 1
        # exclude obvious docstrings/comments — heuristic
        line_text = src.splitlines()[line - 1] if line - 1 < len(src.splitlines()) else ""
        if line_text.lstrip().startswith("#"):
            continue
        sub = subsystem_of(rel)
        cat = "spec_magic"
        st.emit(
            subsystem=sub,
            file=rel,
            line=line,
            end_line=line,
            category=cat,
            severity=severity_for(cat, sub, ""),
            summary=f"Hardcoded `{m.group(0)}` outside `engine/runtime/platform.py`",
            evidence=excerpt(src, line, context=2),
            recommended_prompt=(
                f"Open `{rel}:{line}`. The literal `{m.group(0)}` is hardcoded. "
                f"Cross-platform routing must go through env vars resolved by "
                f"`engine/runtime/platform.py`. Replace this literal with a call "
                f"to the appropriate `default_*` helper (or read the env var "
                f"directly) so dev (Windows TCP) and prod (Linux Unix socket) "
                f"can both work without source changes."
            ),
        )


# ---------- Print in engine/services ----------

PRINT_RE = re.compile(r'(?<![\w.])print\s*\(')


def detect_print_in_engine(src: str, rel: str, st: AuditState) -> None:
    if not (rel.startswith("engine/") or rel.startswith("services/")):
        return
    if rel in PRIVATE_OK_PRINT_FILES:
        return
    has_main = bool(CLI_ENTRYPOINT_RE.search(src))
    for m in PRINT_RE.finditer(src):
        line = src.count("\n", 0, m.start()) + 1
        line_text = src.splitlines()[line - 1] if line - 1 < len(src.splitlines()) else ""
        if line_text.lstrip().startswith("#"):
            continue
        # If file has __main__ entry, prints below it are likely CLI output
        if has_main:
            main_match = CLI_ENTRYPOINT_RE.search(src)
            if main_match and m.start() > main_match.start():
                continue
        sub = subsystem_of(rel)
        cat = "print_in_engine"
        st.emit(
            subsystem=sub,
            file=rel,
            line=line,
            end_line=line,
            category=cat,
            severity=severity_for(cat, sub, ""),
            summary="`print()` call in production module (use logging or telemetry)",
            evidence=excerpt(src, line, context=2),
            recommended_prompt=(
                f"Open `{rel}:{line}`. Replace `print(...)` with a logging "
                f"call (`logging.getLogger(__name__).info(...)`) or with a "
                f"`runtime_metrics` / `record_component_health` observation, so "
                f"the message is captured by journald and observable through "
                f"`/api/operator/support_snapshot`."
            ),
        )


# ---------- TODO / FIXME / XXX / HACK markers ----------

MARKER_RE = re.compile(r'(?i)(?<![\w])(TODO|FIXME|XXX|HACK)\b[: ]?(.{0,140})')


def detect_markers(src: str, rel: str, st: AuditState) -> None:
    for m in MARKER_RE.finditer(src):
        line = src.count("\n", 0, m.start()) + 1
        line_text = src.splitlines()[line - 1] if line - 1 < len(src.splitlines()) else ""
        if not (line_text.lstrip().startswith("#") or '"""' in line_text or "'''" in line_text):
            # Probably a string literal that mentions TODO; only flag in comments / docstrings
            if not line_text.lstrip().startswith(('"', "'", "#")):
                continue
        marker = m.group(1).upper()
        rest = m.group(2).strip().rstrip("'\"") or "(no detail)"
        sub = subsystem_of(rel)
        cat = "todo_marker"
        st.emit(
            subsystem=sub,
            file=rel,
            line=line,
            end_line=line,
            category=cat,
            severity=severity_for(cat, sub, ""),
            summary=f"{marker}: {rest[:80]}",
            evidence=excerpt(src, line, context=2),
            recommended_prompt=(
                f"Open `{rel}:{line}`. The {marker} marker reads: \"{rest[:140]}\". "
                f"Investigate. Decide whether to (a) implement it now and remove "
                f"the marker, (b) convert to a tracked issue and remove the marker, "
                f"or (c) reword the marker so the contract and follow-up owner are "
                f"explicit. Stale TODOs accumulate technical debt and obscure real "
                f"work."
            ),
        )


# ---------- File walker ----------

def iter_python_files() -> Iterable[Path]:
    for top in INCLUDE_DIRS:
        root = REPO / top
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if any(part in EXCLUDE_DIRS for part in path.parts):
                continue
            yield path


def audit_file(path: Path, st: AuditState) -> None:
    rel = path.relative_to(REPO).as_posix()
    try:
        src = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return
    try:
        tree = ast.parse(src, filename=rel)
    except SyntaxError:
        st.emit(
            subsystem=subsystem_of(rel),
            file=rel,
            line=1,
            end_line=1,
            category="syntax_error",
            severity="P0",
            summary="File does not parse — SyntaxError",
            evidence="(file failed ast.parse)",
            recommended_prompt=(
                f"Open `{rel}`. The file fails `ast.parse`. Run "
                f"`python -m py_compile {rel}` to see the exact error and fix the "
                f"syntax. A non-parsing module is dead code at best and an "
                f"import-time crash at worst."
            ),
        )
        return
    detect_stubs_and_not_impl(tree, src, rel, st)
    detect_silent_exceptions(tree, src, rel, st)
    detect_suspicious_literal_returns(tree, src, rel, st)
    detect_mock_in_prod(tree, src, rel, st)
    detect_magic_localhost(src, rel, st)
    detect_print_in_engine(src, rel, st)
    detect_markers(src, rel, st)


# ---------- Output formatting ----------

def write_outputs(st: AuditState, json_path: Path, md_path: Path) -> None:
    findings = sorted(
        st.findings,
        key=lambda f: ({"P0": 0, "P1": 1, "P2": 2}.get(f.severity, 3), f.subsystem, f.file, f.line),
    )

    json_path.write_text(json.dumps([asdict(f) for f in findings], indent=2), encoding="utf-8")

    by_sev: dict[str, int] = {}
    by_cat: dict[str, int] = {}
    by_sub: dict[str, int] = {}
    for f in findings:
        by_sev[f.severity] = by_sev.get(f.severity, 0) + 1
        by_cat[f.category] = by_cat.get(f.category, 0) + 1
        by_sub[f.subsystem] = by_sub.get(f.subsystem, 0) + 1

    lines: list[str] = []
    lines.append("# System Audit — Layer 1 (Static Detectors)\n")
    lines.append(f"Generated by `tools/system_audit.py` against `{', '.join(INCLUDE_DIRS)}`.\n\n")
    lines.append(f"Total findings: **{len(findings)}**\n\n")

    lines.append("## Counts by severity\n\n")
    lines.append("| Severity | Count |\n|---|---:|\n")
    for sev in ("P0", "P1", "P2"):
        lines.append(f"| {sev} | {by_sev.get(sev, 0)} |\n")
    lines.append("\n")

    lines.append("## Counts by category\n\n")
    lines.append("| Category | Count |\n|---|---:|\n")
    for cat in sorted(by_cat, key=lambda k: -by_cat[k]):
        lines.append(f"| {cat} | {by_cat[cat]} |\n")
    lines.append("\n")

    lines.append("## Counts by subsystem\n\n")
    lines.append("| Subsystem | Count |\n|---|---:|\n")
    for sub in sorted(by_sub, key=lambda k: -by_sub[k]):
        lines.append(f"| {sub} | {by_sub[sub]} |\n")
    lines.append("\n")

    lines.append("## Findings\n\n")
    last_sev = None
    for f in findings:
        if f.severity != last_sev:
            lines.append(f"### {f.severity}\n\n")
            last_sev = f.severity
        lines.append(f"#### `{f.id}` — {f.summary}\n\n")
        lines.append(f"- **File**: `{f.file}:{f.line}`\n")
        lines.append(f"- **Subsystem**: `{f.subsystem}`\n")
        lines.append(f"- **Category**: `{f.category}`\n\n")
        lines.append("```\n" + f.evidence + "\n```\n\n")
        lines.append("**Codex prompt**:\n\n")
        lines.append("> " + f.recommended_prompt.replace("\n", "\n> ") + "\n\n")

    md_path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    st = AuditState()
    n = 0
    for p in iter_python_files():
        audit_file(p, st)
        n += 1
    json_path = REPO / "docs" / "System_Audit_Layer1.json"
    md_path = REPO / "docs" / "System_Audit_Layer1.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    write_outputs(st, json_path, md_path)
    print(f"audited {n} files, emitted {len(st.findings)} findings")
    print(f"  json: {json_path}")
    print(f"  md:   {md_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
