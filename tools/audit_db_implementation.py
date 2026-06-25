#!/usr/bin/env python3
"""Audit script for the database implementation prompts (DB-01 through DB-09).

Verifies that every prompt's deliverables are present and correct. Designed
to be run from the repo root after all nine database prompts have been
implemented by Codex (or by hand).

Usage:
    python tools/audit_db_implementation.py                  # fast structural audit
    python tools/audit_db_implementation.py --run-tests      # also run pytest groups
    python tools/audit_db_implementation.py --db-dsn DSN     # also query live DB state
    python tools/audit_db_implementation.py --full --json    # everything, JSON output

Exit code:
    0 if every check passed, 1 otherwise.

Linux-only: pure Python stdlib + optional psycopg for --db-dsn.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
EXCLUDE_DIRS = {".git", ".venv", "venv", "node_modules", "__pycache__",
                ".claude", ".pytest_cache", "dist", "build"}


# ---------- Result types ----------

@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str = ""


@dataclass
class PromptResult:
    prompt: str
    title: str
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def pass_count(self) -> int:
        return sum(1 for c in self.checks if c.passed)


# ---------- Reusable check primitives ----------

def file_exists(path: str) -> CheckResult:
    p = REPO_ROOT / path
    return CheckResult(
        name=f"file exists: {path}",
        passed=p.exists(),
        detail="" if p.exists() else f"missing: {p}",
    )


def _iter_py_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.py"):
        if any(part in EXCLUDE_DIRS for part in path.parts):
            continue
        yield path


def _iter_files(root: Path, glob: str = "**/*") -> Iterable[Path]:
    for path in root.glob(glob):
        if not path.is_file():
            continue
        if any(part in EXCLUDE_DIRS for part in path.parts):
            continue
        yield path


def grep_absent(pattern: str, where: str, name: str | None = None,
                exclude_files: list[str] | None = None,
                exclude_dirs: list[str] | None = None) -> CheckResult:
    """Pass when `pattern` is NOT found in any file under `where` (a directory)."""
    base = REPO_ROOT / where
    label = name or f"no /{pattern}/ under {where}/"
    if not base.exists():
        return CheckResult(name=label, passed=False, detail=f"directory missing: {base}")
    excl_files = {(REPO_ROOT / f).resolve() for f in (exclude_files or [])}
    excl_dirs = set(exclude_dirs or [])
    pat = re.compile(pattern)
    matches: list[str] = []
    for path in _iter_files(base, "**/*.py"):
        if path.resolve() in excl_files:
            continue
        if any(part in excl_dirs for part in path.parts):
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if pat.search(content):
            matches.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))
    return CheckResult(
        name=label,
        passed=not matches,
        detail="" if not matches else f"found in: {matches[:8]}",
    )


def ast_no_import(module_root: str, forbidden: str,
                  exclude_files: list[str] | None = None) -> CheckResult:
    """Pass when no .py file under `module_root` imports `forbidden`."""
    base = REPO_ROOT / module_root
    label = f"no `import {forbidden}` under {module_root}/"
    if not base.exists():
        return CheckResult(name=label, passed=False, detail=f"missing dir: {base}")
    excl = {(REPO_ROOT / f).resolve() for f in (exclude_files or [])}
    matches: list[str] = []
    for path in _iter_py_files(base):
        if path.resolve() in excl:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == forbidden or alias.name.startswith(forbidden + "."):
                        matches.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))
            elif isinstance(node, ast.ImportFrom):
                m = node.module or ""
                if m == forbidden or m.startswith(forbidden + "."):
                    matches.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))
    matches = sorted(set(matches))
    return CheckResult(
        name=label,
        passed=not matches,
        detail="" if not matches else f"found in: {matches[:8]}",
    )


def ast_no_call(module_root: str, callable_pattern: str,
                exclude_files: list[str] | None = None) -> CheckResult:
    """Pass when no .py file under `module_root` calls anything matching `callable_pattern`."""
    base = REPO_ROOT / module_root
    label = f"no call to /{callable_pattern}/ under {module_root}/"
    if not base.exists():
        return CheckResult(name=label, passed=False, detail=f"missing dir: {base}")
    excl = {(REPO_ROOT / f).resolve() for f in (exclude_files or [])}
    pat = re.compile(callable_pattern)
    matches: list[str] = []
    for path in _iter_py_files(base):
        if path.resolve() in excl:
            continue
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if pat.search(content):
            matches.append(str(path.relative_to(REPO_ROOT)).replace("\\", "/"))
    matches = sorted(set(matches))
    return CheckResult(
        name=label,
        passed=not matches,
        detail="" if not matches else f"found in: {matches[:8]}",
    )


def run_pytest(test_paths: list[str], timeout: int = 600) -> CheckResult:
    label = f"pytest -q {test_paths[0]}" + ("" if len(test_paths) == 1 else f" (+{len(test_paths)-1} more)")
    missing = [p for p in test_paths if not (REPO_ROOT / p).exists()]
    if missing:
        return CheckResult(name=label, passed=False, detail=f"missing test files: {missing[:5]}")
    try:
        r = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "--no-header", *test_paths],
            cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=timeout,
        )
    except FileNotFoundError:
        return CheckResult(name=label, passed=False, detail="pytest not installed")
    except subprocess.TimeoutExpired:
        return CheckResult(name=label, passed=False, detail=f"timeout after {timeout}s")
    except Exception as e:
        return CheckResult(name=label, passed=False, detail=f"{type(e).__name__}: {e}")
    return CheckResult(
        name=label,
        passed=r.returncode == 0,
        detail="" if r.returncode == 0 else (r.stdout[-1500:] + r.stderr[-1500:]).strip(),
    )


# ---------- Database state checks (require psycopg + DSN) ----------

def db_query(dsn: str, sql: str) -> tuple[bool, list[tuple] | str]:
    try:
        import psycopg  # noqa
    except ImportError:
        return False, "psycopg not installed"
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return True, rows
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def check_extensions_present(dsn: str) -> CheckResult:
    expected = {"timescaledb", "pg_stat_statements", "pg_trgm", "pgcrypto"}
    ok, rows = db_query(dsn, "SELECT extname FROM pg_extension")
    if not ok:
        return CheckResult(name="DB extensions present", passed=False, detail=str(rows))
    found = {r[0] for r in rows}
    missing = expected - found
    return CheckResult(
        name="DB extensions present",
        passed=not missing,
        detail="" if not missing else f"missing: {sorted(missing)}",
    )


def check_hypertables_present(dsn: str, expected_min: int = 20) -> CheckResult:
    ok, rows = db_query(dsn,
        "SELECT hypertable_name FROM timescaledb_information.hypertables")
    if not ok:
        return CheckResult(name=f"DB hypertables (≥ {expected_min})", passed=False, detail=str(rows))
    return CheckResult(
        name=f"DB hypertables (≥ {expected_min})",
        passed=len(rows) >= expected_min,
        detail=f"found {len(rows)}: {sorted([r[0] for r in rows])[:10]}{'…' if len(rows) > 10 else ''}",
    )


def check_compression_policies(dsn: str) -> CheckResult:
    ok, rows = db_query(dsn,
        "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_compression'")
    if not ok:
        return CheckResult(name="compression policies armed", passed=False, detail=str(rows))
    n = rows[0][0] if rows else 0
    return CheckResult(
        name="compression policies armed",
        passed=n > 0,
        detail=f"count={n}",
    )


def check_retention_policies(dsn: str) -> CheckResult:
    ok, rows = db_query(dsn,
        "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name = 'policy_retention'")
    if not ok:
        return CheckResult(name="retention policies armed", passed=False, detail=str(rows))
    n = rows[0][0] if rows else 0
    return CheckResult(
        name="retention policies armed",
        passed=n > 0,
        detail=f"count={n}",
    )


def check_continuous_aggregates(dsn: str) -> CheckResult:
    ok, rows = db_query(dsn,
        "SELECT view_name FROM timescaledb_information.continuous_aggregates")
    if not ok:
        return CheckResult(name="continuous aggregates present", passed=False, detail=str(rows))
    return CheckResult(
        name="continuous aggregates present",
        passed=len(rows) >= 4,
        detail=f"found {len(rows)}: {sorted([r[0] for r in rows])}",
    )


def check_audit_columns(dsn: str) -> CheckResult:
    ok, rows = db_query(dsn, """
        SELECT table_name FROM information_schema.columns
        WHERE column_name = 'row_hash'
        GROUP BY table_name
    """)
    if not ok:
        return CheckResult(name="audit tables have row_hash column", passed=False, detail=str(rows))
    return CheckResult(
        name="audit tables have row_hash column",
        passed=len(rows) >= 5,
        detail=f"tables with row_hash: {sorted([r[0] for r in rows])}",
    )


def check_schema_migrations_applied(dsn: str, expected_at_least: int = 9) -> CheckResult:
    ok, rows = db_query(dsn, "SELECT max(id) FROM schema_migrations")
    if not ok:
        return CheckResult(name=f"migrations ≥ {expected_at_least:04d}", passed=False, detail=str(rows))
    n = rows[0][0] if rows and rows[0][0] is not None else 0
    return CheckResult(
        name=f"migrations applied (max id ≥ {expected_at_least:04d})",
        passed=n >= expected_at_least,
        detail=f"max id={n}",
    )


# ---------- Per-prompt audits ----------

def audit_db01() -> PromptResult:
    p = PromptResult(prompt="DB-01", title="Server bootstrap")
    for f in [
        "ops/server/bootstrap.sh",
        "ops/server/verify.sh",
        "ops/server/config/postgres.conf.tmpl",
        "ops/server/config/pgbouncer.ini.tmpl",
        "ops/server/config/redis.conf.tmpl",
        "ops/server/systemd/trading-api.service",
        "ops/server/systemd/trading-jobs.service",
        "ops/server/systemd/trading-stream-prices.service",
        "ops/server/systemd/trading-ingest.service",
        "ops/server/systemd/trading.target",
        "ops/server/README.md",
        "tests/ops/test_bootstrap_idempotent.sh",
        "tests/ops/test_systemd_units_lint.sh",
    ]:
        p.checks.append(file_exists(f))
    return p


def audit_db02(run_tests: bool) -> PromptResult:
    p = PromptResult(prompt="DB-02", title="Postgres storage layer")
    for f in [
        "engine/runtime/storage.py",
        "engine/runtime/storage_pg.py",
        "engine/runtime/storage_pool.py",
        "engine/runtime/storage_dialect.py",
        "engine/runtime/locks_pg.py",
        "engine/runtime/platform.py",
        "engine/runtime/schema/__init__.py",
        "engine/runtime/schema/migrator.py",
        "engine/runtime/schema/migrations/__init__.py",
        "engine/runtime/schema/migrations/0001_baseline.py",
        "tests/test_storage_pg_smoke.py",
        "tests/test_storage_param_rewrite.py",
        "tests/test_storage_migrator.py",
        "tests/test_storage_locks_pg.py",
        "tests/test_no_sqlite_in_runtime.py",
        "tests/test_platform_defaults.py",
        "tests/test_no_string_paths.py",
    ]:
        p.checks.append(file_exists(f))
    p.checks.append(ast_no_import("engine", "sqlite3"))
    p.checks.append(ast_no_import("services", "sqlite3"))
    p.checks.append(grep_absent(r"sqlite3\.connect", "engine"))
    p.checks.append(grep_absent(
        r"['\"]/(var|etc)/", "engine",
        name="no Linux-only path literals under engine/",
        exclude_files=["engine/runtime/platform.py"],
    ))
    if run_tests:
        p.checks.append(run_pytest([
            "tests/test_storage_pg_smoke.py",
            "tests/test_storage_param_rewrite.py",
            "tests/test_storage_migrator.py",
            "tests/test_storage_locks_pg.py",
            "tests/test_no_sqlite_in_runtime.py",
            "tests/test_platform_defaults.py",
            "tests/test_no_string_paths.py",
        ]))
    return p


def audit_db03(run_tests: bool) -> PromptResult:
    p = PromptResult(prompt="DB-03", title="Schema with hypertables")
    for f in [
        "engine/runtime/schema/migrations/0002_hypertables.py",
        "engine/runtime/schema/migrations/0003_indexes.py",
        "engine/runtime/schema/migrations/0004_continuous_aggregates.py",
        "engine/runtime/schema/table_classification.py",
        "docs/Database_Schema.md",
        "tests/test_schema_classification.py",
        "tests/test_schema_hypertable_creation.py",
        "tests/test_schema_compression_policy.py",
        "tests/test_schema_retention_policy.py",
        "tests/test_schema_indexes_present.py",
        "tests/test_schema_caggs_present.py",
    ]:
        p.checks.append(file_exists(f))
    if run_tests:
        p.checks.append(run_pytest([
            "tests/test_schema_classification.py",
            "tests/test_schema_hypertable_creation.py",
            "tests/test_schema_compression_policy.py",
            "tests/test_schema_retention_policy.py",
            "tests/test_schema_indexes_present.py",
            "tests/test_schema_caggs_present.py",
        ]))
    return p


def audit_db04(run_tests: bool) -> PromptResult:
    p = PromptResult(prompt="DB-04", title="Redis hot-path cache")
    for f in [
        "engine/cache/__init__.py",
        "engine/cache/redis_pool.py",
        "engine/cache/circuit.py",
        "engine/cache/codec.py",
        "engine/cache/keys.py",
        "engine/cache/store.py",
        "engine/cache/wrappers/__init__.py",
        "engine/cache/wrappers/kill_switch.py",
        "engine/cache/wrappers/execution_mode.py",
        "engine/cache/wrappers/execution_health.py",
        "engine/cache/wrappers/broker_order_state.py",
        "engine/cache/wrappers/position_baseline.py",
        "engine/cache/wrappers/strategy_allocations.py",
        "engine/cache/wrappers/feature_snapshots.py",
        "tests/test_cache_redis_pool.py",
        "tests/test_cache_circuit.py",
        "tests/test_cache_codec.py",
        "tests/test_cache_write_through.py",
        "tests/test_cache_fail_open.py",
        "tests/test_cache_wrappers_integration.py",
    ]:
        p.checks.append(file_exists(f))
    p.checks.append(ast_no_import(
        "engine", "redis",
        exclude_files=[f"engine/cache/{n}" for n in
                       ("redis_pool.py", "store.py", "circuit.py")],
    ))
    if run_tests:
        p.checks.append(run_pytest([
            "tests/test_cache_redis_pool.py",
            "tests/test_cache_circuit.py",
            "tests/test_cache_codec.py",
            "tests/test_cache_write_through.py",
            "tests/test_cache_fail_open.py",
            "tests/test_cache_wrappers_integration.py",
        ]))
    return p


def audit_db05(run_tests: bool) -> PromptResult:
    p = PromptResult(prompt="DB-05", title="Object storage for artifacts")
    for f in [
        "engine/artifacts/__init__.py",
        "engine/artifacts/store.py",
        "engine/artifacts/refs.py",
        "engine/artifacts/paths.py",
        "engine/artifacts/fsck.py",
        "engine/strategy/jobs/artifacts_fsck.py",
        "engine/runtime/schema/migrations/0005_artifacts.py",
        "tools/migrate_artifacts.py",
        "tests/test_artifact_store_local.py",
        "tests/test_artifact_store_alias_history.py",
        "tests/test_artifact_fsck.py",
        "tests/test_artifact_migrations.py",
        "tests/test_no_loose_blob_writes.py",
    ]:
        p.checks.append(file_exists(f))
    p.checks.append(ast_no_call(
        "engine", r"\bjoblib\.dump\b",
        exclude_files=["engine/artifacts/store.py"],
    ))
    p.checks.append(ast_no_call(
        "engine", r"\btorch\.save\b",
        exclude_files=["engine/artifacts/store.py"],
    ))
    if run_tests:
        p.checks.append(run_pytest([
            "tests/test_artifact_store_local.py",
            "tests/test_artifact_store_alias_history.py",
            "tests/test_artifact_fsck.py",
            "tests/test_artifact_migrations.py",
            "tests/test_no_loose_blob_writes.py",
        ]))
    return p


def audit_db06(run_tests: bool) -> PromptResult:
    p = PromptResult(prompt="DB-06", title="PgBouncer + observability")
    for f in [
        "ops/server/config/pgbouncer.userlist.txt.tmpl",
        "ops/server/config/pgbouncer.ini",
        "ops/server/systemd/pgbouncer.service",
        "engine/runtime/observability/__init__.py",
        "engine/runtime/observability/pg_stats.py",
        "engine/runtime/observability/slow_log.py",
        "engine/strategy/jobs/observability_snapshot.py",
        "engine/runtime/schema/migrations/0006_observability.py",
        "tools/grafana/trading-overview.json",
        "tests/test_pgbouncer_routing.py",
        "tests/test_observability_pg_stats.py",
        "tests/test_observability_slow_log.py",
        "tests/test_pgbouncer_userlist_render.py",
    ]:
        p.checks.append(file_exists(f))
    if run_tests:
        p.checks.append(run_pytest([
            "tests/test_pgbouncer_routing.py",
            "tests/test_observability_pg_stats.py",
            "tests/test_observability_slow_log.py",
            "tests/test_pgbouncer_userlist_render.py",
        ]))
    return p


def audit_db07() -> PromptResult:
    p = PromptResult(prompt="DB-07", title="Backup / WAL / restore drill")
    for f in [
        "ops/backup/wal_archive.sh",
        "ops/backup/base_backup.sh",
        "ops/backup/state_snapshot.sh",
        "ops/backup/artifact_snapshot.sh",
        "ops/backup/prune.sh",
        "ops/backup/restore.sh",
        "ops/backup/restore_drill.sh",
        "ops/server/systemd/trading-base-backup.service",
        "ops/server/systemd/trading-base-backup.timer",
        "ops/server/systemd/trading-state-snapshot.service",
        "ops/server/systemd/trading-state-snapshot.timer",
        "ops/server/systemd/trading-artifact-snapshot.service",
        "ops/server/systemd/trading-artifact-snapshot.timer",
        "ops/server/systemd/trading-backup-prune.service",
        "ops/server/systemd/trading-backup-prune.timer",
        "ops/server/systemd/trading-restore-drill.service",
        "ops/server/systemd/trading-restore-drill.timer",
        "tools/restore_sanity.sql",
        "tests/ops/test_wal_archive_script.sh",
        "tests/ops/test_base_backup_verify.sh",
        "tests/ops/test_restore_drill_dry.sh",
    ]:
        p.checks.append(file_exists(f))
    return p


def audit_db08(run_tests: bool) -> PromptResult:
    p = PromptResult(prompt="DB-08", title="Audit hash chain")
    for f in [
        "engine/audit/__init__.py",
        "engine/audit/canonical.py",
        "engine/audit/hashing.py",
        "engine/audit/chain.py",
        "engine/audit/verifier.py",
        "engine/audit/cli.py",
        "engine/runtime/schema/migrations/0007_audit_chain.py",
        "engine/runtime/schema/migrations/0008_audit_findings.py",
        "engine/strategy/jobs/audit_chain_verify.py",
        "docs/Audit_Chain_Spec.md",
        "tests/test_audit_canonical.py",
        "tests/test_audit_hashing.py",
        "tests/test_audit_chain_append.py",
        "tests/test_audit_chain_verifier.py",
        "tests/test_audit_chain_tamper_detection.py",
        "tests/test_audit_chain_concurrent_writers.py",
    ]:
        p.checks.append(file_exists(f))
    if run_tests:
        p.checks.append(run_pytest([
            "tests/test_audit_canonical.py",
            "tests/test_audit_hashing.py",
            "tests/test_audit_chain_append.py",
            "tests/test_audit_chain_verifier.py",
            "tests/test_audit_chain_tamper_detection.py",
            "tests/test_audit_chain_concurrent_writers.py",
        ]))
    return p


def audit_db09(run_tests: bool) -> PromptResult:
    p = PromptResult(prompt="DB-09", title="Secrets via systemd-creds (+ plaintext dev fallback)")
    for f in [
        "services/secrets/__init__.py",
        "services/secrets/loader.py",
        "services/secrets/rotation.py",
        "services/secrets/providers/__init__.py",
        "services/secrets/providers/systemd_creds.py",
        "services/secrets/providers/plaintext.py",
        "ops/server/credstore/install.sh",
        "ops/server/credstore/rotate_master_key.sh",
        "ops/server/credstore/rotate_pg_role.sh",
        "engine/runtime/schema/migrations/0009_credential_access_log.py",
        "docs/Secrets_Rotation_Runbook.md",
        "tests/test_secrets_loader.py",
        "tests/test_secrets_provider_systemd.py",
        "tests/test_secrets_provider_plaintext.py",
        "tests/test_secrets_rotation.py",
        "tests/test_no_legacy_secret_paths.py",
    ]:
        p.checks.append(file_exists(f))
    p.checks.append(grep_absent(
        r"/etc/trading/secrets/", "engine",
        name="no /etc/trading/secrets/ literal under engine/",
    ))
    p.checks.append(grep_absent(
        r"/etc/trading/secrets/", "services",
        name="no /etc/trading/secrets/ literal under services/",
        exclude_files=["services/secrets/providers/plaintext.py"],
    ))
    if run_tests:
        p.checks.append(run_pytest([
            "tests/test_secrets_loader.py",
            "tests/test_secrets_provider_plaintext.py",
            "tests/test_secrets_rotation.py",
            "tests/test_no_legacy_secret_paths.py",
        ]))
    return p


def audit_db_state(dsn: str) -> PromptResult:
    p = PromptResult(prompt="DB-STATE", title="Live database state")
    p.checks.append(check_extensions_present(dsn))
    p.checks.append(check_schema_migrations_applied(dsn))
    p.checks.append(check_hypertables_present(dsn))
    p.checks.append(check_compression_policies(dsn))
    p.checks.append(check_retention_policies(dsn))
    p.checks.append(check_continuous_aggregates(dsn))
    p.checks.append(check_audit_columns(dsn))
    return p


# ---------- Reporting ----------

def render_text(audits: list[PromptResult], verbose: bool) -> str:
    out: list[str] = []
    for p in audits:
        header = f"=== {p.prompt} {p.title}  ({p.pass_count}/{len(p.checks)}) ==="
        out.append("")
        out.append(header)
        for c in p.checks:
            mark = "OK  " if c.passed else "FAIL"
            out.append(f"  [{mark}] {c.name}")
            if (verbose or not c.passed) and c.detail:
                for line in c.detail.splitlines():
                    out.append(f"         {line}")
    total_pass = sum(p.pass_count for p in audits)
    total = sum(len(p.checks) for p in audits)
    fails = total - total_pass
    out.append("")
    out.append("=" * 60)
    out.append(f"Summary: {total_pass}/{total} checks pass, {fails} failures")
    if fails:
        out.append("Failures by prompt:")
        for p in audits:
            failed = [c for c in p.checks if not c.passed]
            if failed:
                out.append(f"  {p.prompt}: {len(failed)} failure(s)")
                for c in failed:
                    out.append(f"    - {c.name}")
    out.append("")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description="Audit database implementation prompts")
    ap.add_argument("--run-tests", action="store_true",
                    help="Also run the relevant pytest groups (slow)")
    ap.add_argument("--db-dsn", help="Postgres DSN for live DB state checks")
    ap.add_argument("--full", action="store_true",
                    help="Implies --run-tests; uses TS_PG_DSN env if --db-dsn omitted")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    ap.add_argument("--verbose", action="store_true",
                    help="Print detail for passing checks too")
    args = ap.parse_args()

    run_tests = args.run_tests or args.full
    dsn = args.db_dsn or (os.environ.get("TS_PG_DSN") if args.full else None)

    audits: list[PromptResult] = [
        audit_db01(),
        audit_db02(run_tests),
        audit_db03(run_tests),
        audit_db04(run_tests),
        audit_db05(run_tests),
        audit_db06(run_tests),
        audit_db07(),
        audit_db08(run_tests),
        audit_db09(run_tests),
    ]
    if dsn:
        audits.append(audit_db_state(dsn))

    if args.json:
        print(json.dumps([asdict(p) for p in audits], indent=2, default=str))
    else:
        print(render_text(audits, verbose=args.verbose))

    sys.exit(0 if all(p.passed for p in audits) else 1)


if __name__ == "__main__":
    main()
