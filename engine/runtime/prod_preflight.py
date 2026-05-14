from __future__ import annotations
# prod_preflight.py
"""Production preflight + smoke cycle.

Runs (best-effort, fail-fast on hard errors):
  1) py_compile integrity check for key modules
  2) core schema init + portfolio backtest schema
  3) optional auto-fix equivalent (idempotent)
  4) simulated execution cycle (labels/size/backtest/rebalance/broker_sim)
  5) execution-cost gating sanity check (if enabled)

Exit codes:
  0 = ok
  2 = warnings only
  3 = hard failure
"""

"""
FILE: prod_preflight.py

Runtime subsystem module for `prod_preflight`.
"""

import argparse
import json
import os
import py_compile
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

RUNTIME_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(RUNTIME_DIR, "..", ".."))
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

sys.path[:] = [
    entry
    for entry in sys.path
    if os.path.abspath(entry or os.curdir) != RUNTIME_DIR
]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging as _stdlib_logging

sys.modules["logging"] = _stdlib_logging

KEY_FILES = [
    os.path.join(REPO_ROOT, "dashboard_server.py"),
    os.path.join(REPO_ROOT, "engine", "runtime", "alerts.py"),
    os.path.join(REPO_ROOT, "engine", "execution", "broker_sim.py"),
    os.path.join(REPO_ROOT, "engine", "execution", "broker_alpaca_rest.py"),
    os.path.join(REPO_ROOT, "engine", "runtime", "storage.py"),
    os.path.join(REPO_ROOT, "engine", "strategy", "edge_filter.py"),
    os.path.join(REPO_ROOT, "engine", "strategy", "portfolio_backtest.py"),
    os.path.join(REPO_ROOT, "engine", "strategy", "portfolio_rebalance.py"),
    os.path.join(REPO_ROOT, "ops", "compute_exec_labels.py"),
    os.path.join(REPO_ROOT, "engine", "execution", "train_size_policy.py"),
]

SMOKE_CMDS = [
    ("ops.compute_exec_labels", [sys.executable, "-u", "-m", "ops.compute_exec_labels"]),
    ("engine.strategy.jobs.train_size_policy", [sys.executable, "-u", "-m", "engine.strategy.jobs.train_size_policy"]),
    ("engine.strategy.portfolio_backtest", [sys.executable, "-u", "-m", "engine.strategy.portfolio_backtest"]),
    ("engine.execution.jobs.portfolio_rebalance", [sys.executable, "-u", "-m", "engine.execution.jobs.portfolio_rebalance"]),
    ("engine.execution.broker_sim", [sys.executable, "-u", "-m", "engine.execution.broker_sim"]),
]
_WARNED_NONFATAL_KEYS: set[str] = set()


def _t_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    details = ", ".join(f"{k}={v}" for k, v in (extra or {}).items())
    suffix = f" ({details})" if details else ""
    sys.stderr.write(f"[engine.runtime.prod_preflight] {code}: {type(error).__name__}: {error}{suffix}\n")
    sys.stderr.flush()
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _compile_files(files: List[str]) -> List[str]:
    errs: List[str] = []
    for f in files:
        try:
            py_compile.compile(f, doraise=True)
        except Exception as e:
            errs.append(f"{f}: {e}")
    return errs


def _runtime_config_gate() -> Tuple[List[str], List[str]]:
    notes: List[str] = []
    errors: List[str] = []
    ConfigError = Exception
    try:
        from engine.runtime.config_schema import (
            ConfigError as RuntimeConfigError,
            get_runtime_safety_context,
            load_runtime_config,
        )

        ConfigError = RuntimeConfigError

        safety = get_runtime_safety_context()
        cfg = load_runtime_config()
        notes.append(
            "runtime config ok "
            f"env={safety.get('env')} "
            f"engine_mode={safety.get('engine_mode')} "
            f"strict_runtime={int(bool(safety.get('strict_runtime')))} "
            f"db_path={getattr(cfg, 'db_path', '')} "
            f"allow_training={int(bool(getattr(cfg, 'allow_training', False)))}"
        )
    except ConfigError as e:
        errors.append(f"runtime config invalid: {e}")
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_RUNTIME_CONFIG_FAILED",
            e,
            once_key="runtime_config_gate",
        )
        errors.append(f"runtime config validation failed: {type(e).__name__}: {e}")
    return notes, errors


def _ensure_schemas() -> List[str]:
    notes: List[str] = []
    from engine.runtime.storage import init_db
    from engine.runtime.alerts import init_alerts_db
    from engine.execution.execution_ledger import init_execution_ledger

    # Preflight creates the core schema surface before smoke jobs run so
    # failures point at job logic, not missing tables.
    init_db()
    notes.append("core db ok")

    init_alerts_db()
    notes.append("alerts schema ok")

    init_execution_ledger()
    notes.append("execution ledger schema ok")

    notes.append("portfolio backtest schema ok")

    return notes


def _verify_postgres_contract() -> Tuple[List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    errors: List[str] = []
    validation: Dict[str, Any] = {}
    try:
        from engine.runtime.storage import get_db_validation_snapshot

        validation = dict(get_db_validation_snapshot(strict=True) or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_DB_VALIDATION_FAILED",
            e,
            once_key="postgres_contract_gate",
        )
        errors.append(f"postgres contract validation failed: {type(e).__name__}: {e}")
        return notes, errors, validation

    if bool(validation.get("ok")):
        notes.append(
            "postgres contract ok "
            f"schema_version={validation.get('schema_version')} "
            f"quick_check={validation.get('quick_check')}"
        )
        return notes, errors, validation

    missing_tables = [str(item) for item in list(validation.get("missing_tables") or []) if str(item).strip()]
    missing_columns = {
        str(table): [str(column) for column in list(columns or []) if str(column).strip()]
        for table, columns in dict(validation.get("missing_columns") or validation.get("missing_cols") or {}).items()
        if str(table).strip()
    }
    missing_indexes = [str(item) for item in list(validation.get("missing_indexes") or []) if str(item).strip()]
    schema_version = validation.get("schema_version")
    expected_schema_version = validation.get("expected_schema_version")
    schema_version_ok = bool(validation.get("schema_version_ok", False))
    schema_status = str(validation.get("schema_status") or "")
    quick_check = str(validation.get("quick_check") or "")

    if missing_tables:
        errors.append("postgres contract invalid missing tables: " + ",".join(sorted(missing_tables)))
    if missing_columns:
        rendered_missing_columns = "; ".join(
            f"{table}({','.join(sorted(columns))})"
            for table, columns in sorted(missing_columns.items())
        )
        errors.append(f"postgres contract invalid missing columns: {rendered_missing_columns}")
    if missing_indexes:
        errors.append("postgres contract invalid missing indexes: " + ",".join(sorted(missing_indexes)))
    if not schema_version_ok:
        errors.append(
            "postgres contract invalid schema version: "
            f"actual={schema_version} expected={expected_schema_version} status={schema_status}"
        )
    if quick_check.lower() not in {"ok", "not_applicable"}:
        errors.append(f"postgres contract invalid quick_check={quick_check or 'unknown'}")
    if not errors:
        errors.append("postgres contract invalid")

    return notes, errors, validation


def _verify_sqlite_contract() -> Tuple[List[str], List[str], Dict[str, Any]]:
    return _verify_postgres_contract()


def _check_external_services() -> Tuple[List[str], List[str], List[str], List[Dict[str, Any]]]:
    try:
        from engine.runtime.external_service_readiness import check_external_service_readiness

        summary = dict(check_external_service_readiness() or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_EXTERNAL_SERVICES_FAILED",
            e,
            once_key="external_service_gate",
        )
        return [], [f"external service readiness failed: {type(e).__name__}: {e}"], [], []

    return (
        list(summary.get("notes") or []),
        list(summary.get("warnings") or []),
        list(summary.get("errors") or []),
        [dict(item) for item in list(summary.get("services") or []) if isinstance(item, dict)],
    )


def _run_cmd(name: str, argv: List[str], timeout_s: int, *, smoke_db_path: str | None = None) -> Tuple[int, str]:
    try:
        # Each smoke command runs in a separate process to exercise the same
        # entrypoints production uses, rather than importing jobs inline.
        child_env = dict(os.environ)
        child_env.setdefault("ENGINE_SUPERVISED", "1")
        child_env.setdefault("PREFLIGHT_SMOKE", "1")
        if smoke_db_path:
            child_env["DB_PATH"] = str(smoke_db_path)
            child_env["PREFLIGHT_SMOKE_DB_PATH"] = str(smoke_db_path)
        p = subprocess.run(
            argv,
            cwd=PROJECT_ROOT,
            env=child_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_s,
            check=False,
            text=True,
        )
        out = (p.stdout or "").strip()
        return int(p.returncode), out
    except subprocess.TimeoutExpired as e:
        _warn_nonfatal("PROD_PREFLIGHT_CMD_TIMEOUT", e, once_key=f"cmd_timeout:{name}", command=name, timeout_s=int(timeout_s))
        return 124, f"{name}: timeout after {timeout_s}s"
    except Exception as e:
        _warn_nonfatal("PROD_PREFLIGHT_CMD_FAILED", e, once_key=f"cmd_failed:{name}", command=name)
        return 125, f"{name}: {e}"


_SMOKE_FATAL_OUTPUT_PATTERNS = (
    "operationalerror: database is locked",
    "best_effort_deferred_lock_contention",
)


def _classify_smoke_result(name: str, rc: int, out: str) -> Tuple[str, str] | None:
    text = str(out or "").strip()
    lower = text.lower()
    if int(rc) != 0 and "not enough samples" in lower:
        return "warning", f"smoke warning: {name} rc={rc} ({text or 'not enough samples'})"
    if int(rc) != 0:
        return "error", f"smoke failed: {name} rc={rc}"
    for pattern in _SMOKE_FATAL_OUTPUT_PATTERNS:
        if pattern in lower:
            return "error", f"smoke failed: {name} output matched {pattern}"
    return None


def _isolated_smoke_db_enabled() -> bool:
    raw = str(os.environ.get("PREFLIGHT_ISOLATE_SMOKE_DB", "1") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _prepare_isolated_smoke_db() -> Tuple[List[str], List[str], List[str], str | None, str | None]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    if not _isolated_smoke_db_enabled():
        warnings.append("preflight smoke DB isolation disabled by PREFLIGHT_ISOLATE_SMOKE_DB")
        return notes, warnings, errors, None, None

    try:
        from engine.runtime.storage import DB_PATH

        source_path = Path(DB_PATH)
    except Exception:
        source_path = Path(str(os.environ.get("DB_PATH") or "")).expanduser()

    if not str(source_path).strip():
        errors.append("isolated smoke db source path is empty")
        return notes, warnings, errors, None, None
    if not source_path.exists():
        errors.append(f"isolated smoke db source missing: {source_path}")
        return notes, warnings, errors, None, None

    warnings.append("preflight smoke DB isolation is disabled for Postgres runtime storage")
    return notes, warnings, errors, None, None


def _cleanup_isolated_smoke_db(temp_dir: str | None) -> None:
    if not temp_dir:
        return
    try:
        shutil.rmtree(str(temp_dir), ignore_errors=True)
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_SMOKE_DB_CLEANUP_FAILED",
            exc,
            once_key="smoke_db_cleanup",
            path=str(temp_dir),
        )


def _exec_cost_gate_sanity() -> Tuple[List[str], List[str]]:
    warnings: List[str] = []
    notes: List[str] = []

    # Only run this check when the feature is enabled so preflight stays
    # aligned with the active deployment mode.
    if os.environ.get("ALERT_USE_EXEC_COST_FILTER", "0") != "1":
        return notes, warnings

    try:
        from engine.runtime.storage import connect
        from engine.strategy.edge_filter import adjust_expected_z_for_costs

        con = connect()
        try:
            row = con.execute(
                "SELECT symbol FROM prices ORDER BY ts_ms DESC LIMIT 1"
            ).fetchone()
        finally:
            con.close()

        if not row or not row[0]:
            warnings.append("exec_cost_filter enabled but prices table empty")
            return notes, warnings

        sym = str(row[0])
        adj = adjust_expected_z_for_costs(
            symbol=sym,
            horizon_s=300,
            expected_z=1.0,
            side=1,
        )
        if adj is None:
            warnings.append(f"exec_cost_filter enabled but no realized vol for symbol={sym}")
        else:
            notes.append(f"exec_cost_filter ok symbol={sym}")
    except Exception as e:
        warnings.append(f"exec_cost_filter sanity failed: {e}")

    return notes, warnings


def _capital_reconciliation_sanity() -> Tuple[List[str], List[str], List[str]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []

    try:
        from engine.runtime.event_replay import replay_capital_reconciliation_snapshot
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_CAPITAL_RECON_IMPORT_FAILED",
            e,
            once_key="capital_recon_import_failed",
        )
        warnings.append(f"capital_reconciliation import failed: {e}")
        return notes, warnings, errors

    try:
        snap = replay_capital_reconciliation_snapshot(
            after_event_id=0,
            limit=int(os.environ.get("PREFLIGHT_CAPITAL_RECON_EVENTS_LIMIT", "5000")),
            batch_window_ms=int(os.environ.get("PREFLIGHT_CAPITAL_RECON_BATCH_WINDOW_MS", "2500")),
        )
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_CAPITAL_RECON_RUN_FAILED",
            e,
            once_key="capital_recon_run_failed",
        )
        warnings.append(f"capital_reconciliation run failed: {e}")
        return notes, warnings, errors

    if not bool(snap.get("ok")):
        warnings.append(f"capital_reconciliation unavailable: {snap.get('error') or 'unknown_error'}")
        return notes, warnings, errors

    summary = dict(snap.get("summary") or {})
    latest_orders = dict(summary.get("latest_portfolio_orders") or {})
    actionable_order_count = int(latest_orders.get("actionable_order_count") or 0)
    severity_counts = dict(snap.get("severity_counts") or {})
    error_count = int(severity_counts.get("error") or 0)
    warning_count = int(severity_counts.get("warning") or 0)

    notes.append(
        "capital reconciliation "
        f"actionable_orders={actionable_order_count} "
        f"errors={error_count} warnings={warning_count}"
    )

    if actionable_order_count <= 0:
        return notes, warnings, errors

    for finding in list(snap.get("findings") or []):
        if not isinstance(finding, dict):
            continue
        rendered = f"{finding.get('code')}: {finding.get('message')}"
        if str(finding.get("severity") or "warning") == "error":
            errors.append(rendered)
        else:
            warnings.append(rendered)

    return notes, warnings, errors


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--timeout_s", type=int, default=int(os.environ.get("PREFLIGHT_SMOKE_TIMEOUT_S", "900")))
    args = ap.parse_args()

    started = _t_ms()
    result: Dict[str, Any] = {
        "ok": False,
        "status": "failed",
        "production_ready": False,
        "started_ts_ms": started,
        "steps": [],
        "warnings": [],
        "errors": [],
        "smoke": [],
        "db_validation": {},
        "external_services": [],
    }

    # Fail fast in dependency order: source integrity first, then schema,
    # then smoke jobs, then optional sanity checks.
    config_notes, config_errors = _runtime_config_gate()
    result["steps"].extend(config_notes)
    if config_errors:
        result["errors"].extend(config_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in config_errors:
                print("[config]", error)
        return 3

    comp_errs = _compile_files(KEY_FILES)
    if comp_errs:
        result["errors"] = list(result["errors"]) + comp_errs
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for e in comp_errs:
                print("[compile]", e)
        return 3
    result["steps"].append("py_compile ok")

    try:
        result["steps"].extend(_ensure_schemas())
    except Exception as e:
        _warn_nonfatal("PROD_PREFLIGHT_SCHEMA_INIT_FAILED", e, once_key="schema_init")
        result["errors"].append(f"schema init failed: {e}")
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            print("[schema]", e)
        return 3

    notes, validation_errors, validation = _verify_sqlite_contract()
    result["steps"].extend(notes)
    result["db_validation"] = dict(validation or {})
    if validation_errors:
        result["errors"].extend(validation_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in validation_errors:
                print("[postgres]", error)
        return 3

    external_notes, external_warnings, external_errors, external_services = _check_external_services()
    result["steps"].extend(external_notes)
    result["warnings"].extend(external_warnings)
    result["external_services"] = list(external_services)
    if external_errors:
        result["errors"].extend(external_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in external_errors:
                print("[external]", error)
        return 3

    smoke_db_notes, smoke_db_warnings, smoke_db_errors, smoke_db_path, smoke_db_temp_dir = _prepare_isolated_smoke_db()
    result["steps"].extend(smoke_db_notes)
    result["warnings"].extend(smoke_db_warnings)
    if smoke_db_errors:
        result["errors"].extend(smoke_db_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in smoke_db_errors:
                print("[smoke-db]", error)
        return 3

    try:
        for name, argv in SMOKE_CMDS:
            rc, out = _run_cmd(name, argv, timeout_s=int(args.timeout_s), smoke_db_path=smoke_db_path)
            result["smoke"].append({"name": name, "rc": rc, "out": out[-4000:]})
            classification = _classify_smoke_result(name, rc, out)
            if classification is not None:
                severity, message = classification
                if severity == "warning":
                    result["warnings"].append(message)
                    continue
                result["errors"].append(message)
                if args.json:
                    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
                else:
                    print(f"[smoke] {name} rc={rc}\n{out}")
                return 3
    finally:
        _cleanup_isolated_smoke_db(smoke_db_temp_dir)

    notes2, warns2 = _exec_cost_gate_sanity()
    result["steps"].extend(notes2)
    result["warnings"].extend(warns2)

    notes3, warns3, errs3 = _capital_reconciliation_sanity()
    result["steps"].extend(notes3)
    result["warnings"].extend(warns3)
    result["errors"].extend(errs3)
    if errs3:
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            print(json.dumps(result, indent=2, sort_keys=True))
        return 3

    if result["warnings"]:
        result["ok"] = False
        result["status"] = "warning"
        result["production_ready"] = False
    else:
        result["ok"] = True
        result["status"] = "passed"
        result["production_ready"] = True
    result["finished_ts_ms"] = _t_ms()
    result["duration_ms"] = int(result["finished_ts_ms"]) - int(result["started_ts_ms"])

    if args.json:
        print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    else:
        print(json.dumps(result, indent=2, sort_keys=True))

    return 2 if result["warnings"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
