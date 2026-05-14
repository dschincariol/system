"""
Guardrail for unmanaged writes to migration-scoped hot storage tables.

This tool intentionally focuses on production/runtime Python code, not tests.
It detects direct SQLite write paths against the hot tables slated for
Postgres/Timescale cutover and supports a checked-in baseline so the repo can
freeze the current surface area before cleanup phases retire it.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = REPO_ROOT / "tools" / "storage_route_audit_baseline.json"
ALLOW_MARKER = "storage-route-audit: allow"

SCAN_DIRS = (
    "boot",
    "engine",
    "services",
    "dashboard_server.py",
    "start_system.py",
    "start_ingestion.py",
    "start_all.py",
    "run_dev.py",
)

SKIP_ANYWHERE_PARTS = {
    ".git",
    ".venv",
    "__pycache__",
    "node_modules",
}

SKIP_TOP_LEVEL_PARTS = {
    "logs",
    "logs-staging",
    "logs-staging-cleanrepro",
    "logs-staging-faulthandler",
    "logs-staging-faulthandler-clean",
    "logs-staging-faulthandler-postmetrics",
    "logs-staging-faulthandler2",
    "logs-staging-mini",
    "logs-staging-repro",
    "data",
    "data-isolation",
    "data-staging",
    "tests",
}

SCOPED_TABLES = {
    "prices",
    "price_quotes",
    "price_quotes_raw",
    "runtime_metrics",
    "event_log",
    "ingestion_pipeline_health",
    "price_provider_health",
    "weather_provider_health",
    "data_source_logs",
    "feature_data",
    "model_predictions",
    "trade_outcomes",
}

APPROVED_OWNER_PATHS = {
    "engine/runtime/async_writer.py",
    "engine/runtime/data_source_log_store.py",
    "engine/runtime/event_log.py",
    "engine/runtime/metrics_store.py",
    "engine/runtime/storage_pg_prices.py",
    "engine/runtime/timescale_client.py",
    "engine/runtime/telemetry_append_buffer.py",
    "engine/runtime/price_router.py",
    "engine/runtime/price_migration_validation.py",
    "engine/runtime/telemetry_migration_validation.py",
    "engine/runtime/storage.py",
    "engine/runtime/storage_live_ingestion_schema.py",
}
APPROVED_OWNER_PREFIXES = {
    "engine/runtime/schema/migrations/",
}

WRITE_CALL_NAMES = {"execute", "executemany", "executescript"}
WRITE_SQL_RE = re.compile(
    r"\b(?P<verb>INSERT\s+INTO|UPDATE|DELETE\s+FROM|REPLACE\s+INTO|ALTER\s+TABLE|CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?)\s+"
    r"(?:[`\"']?(?P<table>[A-Za-z_][A-Za-z0-9_]*)[`\"']?)",
    flags=re.IGNORECASE | re.MULTILINE,
)


@dataclass(frozen=True)
class Finding:
    rule: str
    path: str
    line: int
    table: str
    snippet: str
    fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "path": self.path,
            "line": self.line,
            "table": self.table,
            "snippet": self.snippet,
            "fingerprint": self.fingerprint,
        }


def rel_path(path: Path, *, repo_root: Path = REPO_ROOT) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def iter_scan_files(
    *,
    repo_root: Path = REPO_ROOT,
    scan_dirs: Sequence[str] = SCAN_DIRS,
) -> Iterable[Path]:
    seen: set[str] = set()
    for entry in scan_dirs:
        root = (repo_root / entry).resolve()
        if not root.exists():
            continue
        if root.is_file():
            if root.suffix.lower() == ".py":
                yield root
            continue
        for path in root.rglob("*.py"):
            if should_skip_path(path, repo_root=repo_root):
                continue
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            yield path


def load_text(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        sys.stderr.write(f"[storage_route_audit] utf8_decode_failed path={path}\n")
        sys.stderr.flush()
        return path.read_text(encoding="utf-8", errors="ignore").splitlines()


def is_allowed(lines: Sequence[str], index: int) -> bool:
    start = max(0, index - 2)
    end = min(len(lines), index + 3)
    for i in range(start, end):
        if ALLOW_MARKER in lines[i]:
            return True
    return False


def make_fingerprint(rule: str, path: str, table: str, snippet: str) -> str:
    normalized = " ".join(str(snippet).strip().split())
    return f"{rule}|{path}|{table}|{normalized}"


def should_skip_path(path: Path, *, repo_root: Path = REPO_ROOT) -> bool:
    try:
        relative_parts = path.resolve().relative_to(repo_root.resolve()).parts
    except ValueError:
        relative_parts = path.parts
    if any(part in SKIP_ANYWHERE_PARTS for part in relative_parts):
        return True
    if relative_parts and relative_parts[0] in SKIP_TOP_LEVEL_PARTS:
        return True
    return False


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return str(node.id)
    if isinstance(node, ast.Attribute):
        base = _call_name(node.value)
        return f"{base}.{node.attr}" if base else str(node.attr)
    return ""


def _literal_str(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return str(node.value)
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(str(value.value))
            elif isinstance(value, ast.FormattedValue):
                parts.append("{}")
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _literal_str(node.left)
        right = _literal_str(node.right)
        if left is not None and right is not None:
            return left + right
    return None


def _sql_write_matches(sql: str) -> list[tuple[str, str]]:
    matches: list[tuple[str, str]] = []
    for match in WRITE_SQL_RE.finditer(sql or ""):
        table = str(match.group("table") or "").strip().lower()
        if table in SCOPED_TABLES:
            matches.append((table, match.group(0).strip()))
    return matches


class _StorageRouteAnalyzer(ast.NodeVisitor):
    def __init__(self, *, path: Path, repo_root: Path, approved_paths: set[str], lines: Sequence[str]):
        self._path = path
        self._repo_root = repo_root
        self._approved_paths = set(approved_paths)
        self._lines = list(lines)
        self._rel_path = rel_path(path, repo_root=repo_root)
        self.findings: list[Finding] = []

    @property
    def _is_approved(self) -> bool:
        return self._rel_path in self._approved_paths or any(
            self._rel_path.startswith(prefix) for prefix in APPROVED_OWNER_PREFIXES
        )

    def _record(self, *, rule: str, line: int, table: str, snippet: str) -> None:
        if self._is_approved:
            return
        line_index = max(0, int(line) - 1)
        if is_allowed(self._lines, line_index):
            return
        self.findings.append(
            Finding(
                rule=rule,
                path=self._rel_path,
                line=int(line),
                table=str(table),
                snippet=str(snippet).strip(),
                fingerprint=make_fingerprint(rule, self._rel_path, table, snippet),
            )
        )

    def visit_Call(self, node: ast.Call) -> Any:
        call_name = _call_name(node.func)
        short_name = call_name.rsplit(".", 1)[-1]
        if short_name == "run_write_txn":
            self._visit_run_write_txn(node)
        if short_name in WRITE_CALL_NAMES:
            self._visit_sql_write_call(node)
        self.generic_visit(node)

    def _visit_run_write_txn(self, node: ast.Call) -> None:
        for keyword in node.keywords:
            if keyword.arg != "table":
                continue
            table_name = _literal_str(keyword.value)
            if not table_name:
                continue
            table_key = str(table_name).strip().lower()
            if table_key not in SCOPED_TABLES:
                continue
            snippet = ast.get_source_segment("\n".join(self._lines), node) or f"run_write_txn(..., table={table_name!r})"
            self._record(
                rule="run_write_txn_scoped_table",
                line=int(getattr(node, "lineno", 1)),
                table=table_key,
                snippet=snippet,
            )

    def _visit_sql_write_call(self, node: ast.Call) -> None:
        sql_arg: ast.AST | None = None
        if node.args:
            sql_arg = node.args[0]
        else:
            for keyword in node.keywords:
                if keyword.arg == "sql":
                    sql_arg = keyword.value
                    break
        sql_text = _literal_str(sql_arg)
        if not sql_text:
            return
        for table_key, matched_sql in _sql_write_matches(sql_text):
            self._record(
                rule="sql_write_scoped_table",
                line=int(getattr(sql_arg, "lineno", getattr(node, "lineno", 1))),
                table=table_key,
                snippet=matched_sql,
            )


def scan_file(
    path: Path,
    *,
    repo_root: Path = REPO_ROOT,
    approved_paths: set[str] | None = None,
) -> list[Finding]:
    lines = load_text(path)
    source = "\n".join(lines)
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        sys.stderr.write(f"[storage_route_audit] parse_failed path={path} error={exc}\n")
        sys.stderr.flush()
        return []
    analyzer = _StorageRouteAnalyzer(
        path=path,
        repo_root=repo_root,
        approved_paths=approved_paths or APPROVED_OWNER_PATHS,
        lines=lines,
    )
    analyzer.visit(tree)
    return analyzer.findings


def scan_repo(
    *,
    repo_root: Path = REPO_ROOT,
    scan_dirs: Sequence[str] = SCAN_DIRS,
    approved_paths: set[str] | None = None,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for path in iter_scan_files(repo_root=repo_root, scan_dirs=scan_dirs):
        findings.extend(item.to_dict() for item in scan_file(path, repo_root=repo_root, approved_paths=approved_paths))
    findings.sort(key=lambda item: (str(item["path"]), int(item["line"]), str(item["rule"]), str(item["table"])))
    return findings


def load_baseline(path: Path = BASELINE_PATH) -> dict[str, list[str]]:
    if not path.exists():
        return {"allowed_fingerprints": []}
    return json.loads(path.read_text(encoding="utf-8"))


def write_baseline(findings: Sequence[dict[str, Any]], path: Path = BASELINE_PATH) -> None:
    payload = {
        "comment": (
            "Grandfathered storage route findings for migration-scoped hot tables. "
            "Prefer removing entries by routing the write through an approved owner "
            f"module or annotating intentional cases with '{ALLOW_MARKER}'."
        ),
        "allowed_fingerprints": sorted({str(item["fingerprint"]) for item in findings}),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Detect unmanaged writes to migration-scoped hot tables.")
    parser.add_argument("--write-baseline", action="store_true", help="Write the current findings as the baseline.")
    parser.add_argument("--json", action="store_true", help="Emit findings as JSON.")
    args = parser.parse_args(argv)

    findings = scan_repo()

    if args.write_baseline:
        write_baseline(findings)
        print(f"Wrote baseline with {len(findings)} finding(s) to {BASELINE_PATH}")
        return 0

    baseline = load_baseline()
    allowed = set(baseline.get("allowed_fingerprints") or [])
    current = [item for item in findings if str(item["fingerprint"]) not in allowed]

    if args.json:
        payload = {
            "current": current,
            "known": len(findings) - len(current),
            "total": len(findings),
        }
        print(json.dumps(payload, indent=2))
        return 0 if not current else 1

    if not current:
        print(
            "No new storage route findings. "
            f"Scanned {len(findings)} known pattern(s) across migration-scoped hot tables."
        )
        return 0

    print("New storage route findings detected:", file=sys.stderr)
    for item in current:
        print(
            f"{item['path']}:{item['line']} [{item['rule']}] table={item['table']} {item['snippet']}",
            file=sys.stderr,
        )
    print(
        "\nRoute the write through an approved owner module, move it behind a buffered/best-effort path, "
        f"or annotate the code with '{ALLOW_MARKER}' if the direct write is intentional.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
