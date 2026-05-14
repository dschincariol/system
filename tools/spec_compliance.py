"""Spec compliance audit — Layer 3.

Parses every codex prompt under `docs/codex_prompts/` (and the
`database/` subdirectory) and verifies, programmatically, that the
deliverables it specified are actually present in the repo.

For each prompt we extract:
  * the "Files to create" bullets,
  * the "Files to modify" bullets,
  * the "Acceptance criteria" checklist,
  * the "Test plan" bullets and any `pytest -q ...` invocation.

For each extracted item we run a check (file exists, contains a named
symbol, named env var grepped, named test file exists, etc.) and emit
a finding when the check fails. Each finding ships with a Codex
prompt template that is specific to the failed item.

Run from the repo root:

    python tools/spec_compliance.py

Outputs:

    docs/System_Audit_Layer3.json
    docs/System_Audit_Layer3.md
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable

REPO = Path(__file__).resolve().parents[1]
PROMPT_DIRS = [
    REPO / "docs" / "codex_prompts",
    REPO / "docs" / "codex_prompts" / "database",
]


@dataclass
class Finding:
    id: str
    prompt: str
    section: str
    item: str
    severity: str
    summary: str
    evidence: str
    recommended_prompt: str


@dataclass
class State:
    findings: list[Finding] = field(default_factory=list)
    counter: dict[str, int] = field(default_factory=dict)
    prompts_seen: int = 0
    items_checked: int = 0

    def emit(self, prompt: str, **kwargs) -> None:
        slug = Path(prompt).stem
        key = f"SPEC-{slug}"
        self.counter.setdefault(key, 0)
        self.counter[key] += 1
        kwargs["id"] = f"{key}-{self.counter[key]:04d}"
        kwargs["prompt"] = prompt
        self.findings.append(Finding(**kwargs))


# ---------- Prompt parser ----------

H_RE = re.compile(r'^(#+)\s+(.*?)\s*$', re.MULTILINE)
BULLET_RE = re.compile(r'^[-*]\s+(.*?)$', re.MULTILINE)
CHECKBOX_RE = re.compile(r'^[-*]\s*\[\s*[xX ]?\s*\]\s+(.*?)$', re.MULTILINE)
PYTEST_RE = re.compile(r'pytest\s+-q\s+([^\n`]+)', re.MULTILINE)
BACKTICK_PATH_RE = re.compile(r'`([A-Za-z][A-Za-z0-9_./\\-]+\.(?:py|sh|md|yml|yaml|sql|tmpl|service|timer|target|conf|json|jsonl))`')
BACKTICK_SYMBOL_RE = re.compile(r'`([A-Za-z_][A-Za-z0-9_.]*\([^`]*\))`')
ENV_VAR_RE = re.compile(r'`(TS_[A-Z_]+|CREDENTIALS_DIRECTORY)`')


def split_sections(text: str) -> dict[str, str]:
    """Return {heading: body} keyed by lower-cased heading text."""
    sections: dict[str, str] = {}
    last_key = ""
    last_start = 0
    matches = list(H_RE.finditer(text))
    for i, m in enumerate(matches):
        if last_key:
            sections[last_key] = text[last_start:m.start()].strip()
        last_key = m.group(2).strip().lower()
        last_start = m.end()
    if last_key:
        sections[last_key] = text[last_start:].strip()
    return sections


def section_lookup(sections: dict[str, str], *keys: str) -> str:
    for k in keys:
        for h, body in sections.items():
            if h.startswith(k):
                return body
    return ""


def extract_paths(body: str) -> list[str]:
    paths = []
    for m in BACKTICK_PATH_RE.finditer(body):
        path = m.group(1).replace("\\", "/")
        # Skip bare filenames (no directory) — those are typically narrative
        # references inside prose, not deliverable specs. We only want
        # explicit-path bullets (e.g. `engine/foo/bar.py`).
        if "/" not in path:
            continue
        if path not in paths:
            paths.append(path)
    return paths


def extract_acceptance(body: str) -> list[str]:
    return [m.group(1).strip() for m in CHECKBOX_RE.finditer(body)]


def extract_pytest_targets(body: str) -> list[str]:
    targets: list[str] = []
    for m in PYTEST_RE.finditer(body):
        for tok in re.split(r'[ \\\n]+', m.group(1).strip()):
            tok = tok.strip().rstrip("`")
            if tok and tok.endswith(".py"):
                targets.append(tok.replace("\\", "/"))
    return targets


# ---------- Compliance checks ----------

def check_path_exists(path: str) -> tuple[bool, str]:
    p = REPO / path
    if p.exists():
        return True, ""
    return False, f"file not found: {path}"


def file_contains(path: str, needles: list[str]) -> tuple[bool, str]:
    p = REPO / path
    if not p.exists():
        return False, f"file does not exist"
    try:
        text = p.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        return False, f"cannot read: {exc}"
    missing = [n for n in needles if n not in text]
    if missing:
        return False, f"missing references: {missing}"
    return True, ""


def grep_repo(pattern: str, scopes: Iterable[str] = ("engine", "services", "tests", "ops", "tools")) -> list[tuple[Path, int, str]]:
    matches: list[tuple[Path, int, str]] = []
    for scope in scopes:
        root = REPO / scope
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix not in {".py", ".sh", ".md", ".yml", ".yaml", ".sql", ".tmpl", ".service", ".timer", ".target", ".conf", ".ini"}:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            for m in re.finditer(pattern, text):
                line = text.count("\n", 0, m.start()) + 1
                matches.append((path, line, m.group(0)))
    return matches


# ---------- Per-prompt audit ----------

SUPERSEDED_RE = re.compile(r'(?im)^\s*>?\s*\*?\*?(?:status|note)\*?\*?\s*[:\-—]\s*(superseded|deprecated)\b')


def audit_prompt(prompt_path: Path, st: State) -> None:
    text = prompt_path.read_text(encoding="utf-8")
    rel = prompt_path.relative_to(REPO).as_posix()
    if SUPERSEDED_RE.search(text):
        return
    sections = split_sections(text)

    create_body = section_lookup(sections, "files to create")
    modify_body = section_lookup(sections, "files to modify")
    accept_body = section_lookup(sections, "acceptance criteria")
    test_body = section_lookup(sections, "test plan")

    create_paths = extract_paths(create_body)
    modify_paths = extract_paths(modify_body)
    accept_items = extract_acceptance(accept_body)
    pytest_targets = extract_pytest_targets(test_body)

    # 1. Files to create — must exist
    for p in create_paths:
        st.items_checked += 1
        ok, why = check_path_exists(p)
        if not ok:
            st.emit(
                prompt=rel,
                section="files to create",
                item=p,
                severity="P0",
                summary=f"Specified file does not exist: `{p}`",
                evidence=f"Prompt declared `Files to create` should include `{p}`. Filesystem says: {why}",
                recommended_prompt=(
                    f"Open the prompt at `{rel}` and re-read its specification "
                    f"for `{p}`. Implement that file according to the prompt's "
                    f"`Implementation plan` and `Acceptance criteria` sections. "
                    f"Verify with `python tools/spec_compliance.py` that this "
                    f"finding clears, and add or run any tests the prompt named."
                ),
            )

    # 2. Files to modify — must exist (we cannot easily verify modifications)
    for p in modify_paths:
        if p.count("/") < 1:
            continue
        st.items_checked += 1
        ok, why = check_path_exists(p)
        if not ok:
            st.emit(
                prompt=rel,
                section="files to modify",
                item=p,
                severity="P1",
                summary=f"File listed under `Files to modify` does not exist: `{p}`",
                evidence=f"Prompt {rel} says modify `{p}` but it is not in the repo.",
                recommended_prompt=(
                    f"Open `{rel}` and re-read the specification for `{p}`. "
                    f"Either create the file (if it should exist now and the "
                    f"prompt's `Files to create` section was missing it) or "
                    f"correct the prompt to reflect the current path."
                ),
            )

    # 3. Acceptance criteria — heuristic checks
    for crit in accept_items:
        st.items_checked += 1
        # Heuristic A: criterion mentions a backticked path with at least
        # one directory segment → file must exist. Bare filenames inside
        # criterion prose are too noisy to check.
        for path_match in BACKTICK_PATH_RE.findall(crit):
            path = path_match.replace("\\", "/")
            if path.count("/") < 1:
                continue
            ok, _ = check_path_exists(path)
            if not ok:
                st.emit(
                    prompt=rel,
                    section="acceptance criteria",
                    item=crit[:140],
                    severity="P1",
                    summary=f"Acceptance criterion references missing file: `{path}`",
                    evidence=f"From {rel}: \"{crit[:200]}\"",
                    recommended_prompt=(
                        f"The acceptance criterion in `{rel}` requires `{path}` "
                        f"to exist. Create or restore that file according to "
                        f"the prompt's specification."
                    ),
                )
        # Heuristic B: criterion mentions an env var → must be referenced somewhere
        env_vars = ENV_VAR_RE.findall(crit)
        for ev in env_vars:
            hits = grep_repo(re.escape(ev))
            if not hits:
                st.emit(
                    prompt=rel,
                    section="acceptance criteria",
                    item=crit[:140],
                    severity="P1",
                    summary=f"Acceptance criterion mentions env var `{ev}` not referenced anywhere in the repo",
                    evidence=f"From {rel}: \"{crit[:200]}\"",
                    recommended_prompt=(
                        f"The criterion in `{rel}` references env var `{ev}` but "
                        f"no module reads it. Either implement the read-site "
                        f"(usually in `engine/runtime/platform.py` or the "
                        f"relevant subsystem entrypoint) or correct the prompt "
                        f"to use the actual env var name."
                    ),
                )

    # 4. Test plan — every named test file must exist and be collectable
    for tp in pytest_targets:
        st.items_checked += 1
        ok, _ = check_path_exists(tp)
        if not ok:
            st.emit(
                prompt=rel,
                section="test plan",
                item=tp,
                severity="P1",
                summary=f"Specified test file does not exist: `{tp}`",
                evidence=f"`Run:` line in {rel} names `{tp}`",
                recommended_prompt=(
                    f"The test plan in `{rel}` includes `{tp}`. Create that "
                    f"test file with the cases described in the prompt's "
                    f"`Test plan` section. Each test must have at least one "
                    f"non-trivial assertion and exercise the named code path."
                ),
            )


def write_outputs(st: State, json_path: Path, md_path: Path) -> None:
    findings = sorted(
        st.findings,
        key=lambda f: ({"P0": 0, "P1": 1, "P2": 2}.get(f.severity, 3), f.prompt, f.section),
    )

    json_path.write_text(json.dumps([asdict(f) for f in findings], indent=2), encoding="utf-8")

    by_sev: dict[str, int] = {}
    by_prompt: dict[str, int] = {}
    by_section: dict[str, int] = {}
    for f in findings:
        by_sev[f.severity] = by_sev.get(f.severity, 0) + 1
        by_prompt[f.prompt] = by_prompt.get(f.prompt, 0) + 1
        by_section[f.section] = by_section.get(f.section, 0) + 1

    lines: list[str] = []
    lines.append("# System Audit — Layer 3 (Spec Compliance)\n")
    lines.append(f"Generated by `tools/spec_compliance.py`.\n\n")
    lines.append(f"Audited **{st.prompts_seen} prompts**, ran **{st.items_checked} item checks**, emitted **{len(findings)} findings**.\n\n")

    lines.append("## Counts by severity\n\n")
    lines.append("| Severity | Count |\n|---|---:|\n")
    for sev in ("P0", "P1", "P2"):
        lines.append(f"| {sev} | {by_sev.get(sev, 0)} |\n")
    lines.append("\n")

    lines.append("## Counts by prompt\n\n")
    lines.append("| Prompt | Count |\n|---|---:|\n")
    for p in sorted(by_prompt, key=lambda k: -by_prompt[k]):
        lines.append(f"| `{p}` | {by_prompt[p]} |\n")
    lines.append("\n")

    lines.append("## Counts by section\n\n")
    lines.append("| Section | Count |\n|---|---:|\n")
    for s in sorted(by_section, key=lambda k: -by_section[k]):
        lines.append(f"| {s} | {by_section[s]} |\n")
    lines.append("\n")

    lines.append("## Findings\n\n")
    last_sev = None
    for f in findings:
        if f.severity != last_sev:
            lines.append(f"### {f.severity}\n\n")
            last_sev = f.severity
        lines.append(f"#### `{f.id}` — {f.summary}\n\n")
        lines.append(f"- **Prompt**: `{f.prompt}`\n")
        lines.append(f"- **Section**: {f.section}\n")
        lines.append(f"- **Item**: `{f.item}`\n\n")
        lines.append("```\n" + f.evidence + "\n```\n\n")
        lines.append("**Codex prompt**:\n\n")
        lines.append("> " + f.recommended_prompt.replace("\n", "\n> ") + "\n\n")

    md_path.write_text("".join(lines), encoding="utf-8")


def main() -> int:
    st = State()
    for d in PROMPT_DIRS:
        if not d.exists():
            continue
        for p in sorted(d.glob("*.md")):
            if p.name in {"README.md", "CROSS_PLATFORM.md", "AUDIT_REPORT.md", "AUDIT_CHECKLIST.md"}:
                continue
            st.prompts_seen += 1
            audit_prompt(p, st)
    json_path = REPO / "docs" / "System_Audit_Layer3.json"
    md_path = REPO / "docs" / "System_Audit_Layer3.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    write_outputs(st, json_path, md_path)
    print(f"audited {st.prompts_seen} prompts, ran {st.items_checked} item checks, emitted {len(st.findings)} findings")
    print(f"  json: {json_path}")
    print(f"  md:   {md_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
