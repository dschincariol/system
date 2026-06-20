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
import re
import shutil
import stat
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
_PG_PASSWORD_RE = re.compile(r"(?:^|\s)password=", re.IGNORECASE)
_DSN_USER_RE = re.compile(r"(?:^|\s)user=(?P<value>'(?:\\'|[^'])*'|\S+)")


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _looks_like_file_path(path: Path) -> bool:
    return bool(path.suffix)


def _split_names(raw: str) -> List[str]:
    return [part.strip() for part in re.split(r"[\s,]+", str(raw or "")) if part.strip()]


def _short_list(values: List[str], *, limit: int = 6) -> str:
    items = [str(item) for item in values if str(item).strip()]
    if len(items) <= limit:
        return ",".join(items)
    return ",".join(items[:limit]) + f",+{len(items) - limit}_more"


def _short_mapping(values: Dict[str, List[str]], *, table_limit: int = 4, value_limit: int = 4) -> str:
    rendered: List[str] = []
    for idx, (table, columns) in enumerate(sorted(values.items())):
        if idx >= table_limit:
            rendered.append(f"+{len(values) - table_limit}_more_tables")
            break
        column_values = [str(column) for column in list(columns or []) if str(column).strip()]
        if len(column_values) > value_limit:
            column_text = ",".join(column_values[:value_limit]) + f",+{len(column_values) - value_limit}_more"
        else:
            column_text = ",".join(column_values)
        rendered.append(f"{table}({column_text})")
    return ";".join(rendered)


def _schema_validation_failure_summary(validation: Dict[str, Any]) -> str:
    missing_tables = [str(item) for item in list(validation.get("missing_tables") or []) if str(item).strip()]
    missing_columns = {
        str(table): [str(column) for column in list(columns or []) if str(column).strip()]
        for table, columns in dict(validation.get("missing_columns") or validation.get("missing_cols") or {}).items()
        if str(table).strip()
    }
    missing_indexes = [str(item) for item in list(validation.get("missing_indexes") or []) if str(item).strip()]
    missing_migration_ids = [
        str(item)
        for item in list(validation.get("schema_migration_missing_ids") or [])
        if str(item).strip()
    ]
    unexpected_migration_ids = [
        str(item)
        for item in list(validation.get("schema_migration_unexpected_ids") or [])
        if str(item).strip()
    ]
    samples: List[str] = []
    if missing_tables:
        samples.append(f"missing_tables={_short_list(sorted(missing_tables))}")
    if missing_columns:
        samples.append(f"missing_columns={_short_mapping(missing_columns)}")
    if missing_indexes:
        samples.append(f"missing_indexes={_short_list(sorted(missing_indexes))}")
    if missing_migration_ids:
        samples.append(f"missing_migrations={_short_list(sorted(missing_migration_ids))}")
    if unexpected_migration_ids:
        samples.append(f"unexpected_migrations={_short_list(sorted(unexpected_migration_ids))}")

    return (
        "postgres_schema_validation_failed "
        "migration_required=1 "
        f"actual_schema_version={validation.get('schema_version')} "
        f"expected_schema_version={validation.get('expected_schema_version')} "
        f"schema_status={validation.get('schema_status') or 'unknown'} "
        f"schema_version_ok={int(bool(validation.get('schema_version_ok', False)))} "
        f"quick_check={validation.get('quick_check') or 'unknown'} "
        f"missing_tables_count={len(missing_tables)} "
        f"missing_columns_count={sum(len(v) for v in missing_columns.values())} "
        f"missing_indexes_count={len(missing_indexes)} "
        f"missing_migrations_count={len(missing_migration_ids)} "
        f"unexpected_migrations_count={len(unexpected_migration_ids)} "
        f"samples={('|'.join(samples) if samples else 'none')}"
    )


def _schema_validation_backoff_s() -> float:
    raw = str(
        os.environ.get("PROD_PREFLIGHT_SCHEMA_FAILURE_BACKOFF_S")
        or os.environ.get("TRADING_SCHEMA_VALIDATION_BACKOFF_S")
        or "0"
    ).strip()
    try:
        return max(0.0, min(600.0, float(raw)))
    except Exception:
        return 0.0


def _sleep_schema_validation_backoff() -> None:
    backoff_s = _schema_validation_backoff_s()
    if backoff_s > 0:
        time.sleep(backoff_s)


def _dsn_user(conninfo: str) -> str:
    match = _DSN_USER_RE.search(str(conninfo or ""))
    if not match:
        fallback = "ts_app"
        try:
            from engine.runtime.platform import default_pg_user

            fallback = str(default_pg_user())
        except Exception:
            fallback = "ts_app"
        return fallback
    value = str(match.group("value") or "").strip()
    if value.startswith("'") and value.endswith("'"):
        value = value[1:-1].replace("\\'", "'").replace("\\\\", "\\")
    return value or "ts_app"


def _pg_password_env_present(user: str) -> bool:
    role = str(user or "ts_app").removeprefix("ts_").upper()
    for name in (
        "TS_PG_PASSWORD",
        f"TS_PG_PASSWORD_{role}",
        f"TS_PG_{role}_PASSWORD",
        "PGPASSWORD",
    ):
        if str(os.environ.get(name) or "").strip():
            return True
    return False


def _required_pg_credential_names() -> List[str]:
    raw = str(
        os.environ.get("PROD_PREFLIGHT_REQUIRED_CREDENTIALS")
        or os.environ.get("PREFLIGHT_REQUIRED_CREDENTIALS")
        or ""
    ).strip()
    if raw:
        return _split_names(raw)

    configured_dsn = str(os.environ.get("TS_PG_DSN") or "").strip()
    if configured_dsn and _PG_PASSWORD_RE.search(configured_dsn):
        return []

    user = _dsn_user(configured_dsn)
    if _pg_password_env_present(user):
        return []

    try:
        from engine.runtime.platform import pg_password_secret_name

        names = [str(pg_password_secret_name(user))]
    except Exception:
        names = ["pg_password_app"]

    if _env_truthy("PROD_PREFLIGHT_REQUIRE_MASTER_KEY") or _env_truthy("PREFLIGHT_REQUIRE_MASTER_KEY"):
        names.append("master_key")
    return list(dict.fromkeys(names))


def _runtime_data_root() -> Path:
    raw = str(os.environ.get("DB_PATH") or "").strip()
    if raw:
        raw_path = Path(raw).expanduser()
    else:
        from engine.runtime.platform import default_data_root

        raw_path = default_data_root()
    return raw_path.parent if raw and _looks_like_file_path(raw_path) else raw_path


def _credential_file_snapshot(path: Path) -> Dict[str, Any]:
    early_state: Dict[str, Any] | None = None
    try:
        st = path.stat()
    except FileNotFoundError:
        early_state = {"ok": False, "reason": "missing", "path": str(path)}
    except OSError as exc:
        early_state = {"ok": False, "reason": f"stat_failed:{type(exc).__name__}:{exc}", "path": str(path)}
    if early_state is not None:
        return early_state
    mode = stat.S_IMODE(st.st_mode)
    ok = bool(path.is_file() and st.st_size > 0 and os.access(path, os.R_OK))
    reason = "ok"
    if not path.is_file():
        reason = "not_regular_file"
    elif st.st_size <= 0:
        reason = "empty"
    elif not os.access(path, os.R_OK):
        reason = "not_readable"
    return {
        "ok": ok,
        "reason": reason,
        "path": str(path),
        "mode": oct(mode),
        "size": int(st.st_size),
    }


def _production_provisioning_gate() -> Tuple[List[str], List[str], Dict[str, Any]]:
    """Validate production host prerequisites before storage/schema work."""

    notes: List[str] = []
    errors: List[str] = []
    snapshot: Dict[str, Any] = {"credentials": {}, "data_root": {}}

    required_names = _required_pg_credential_names()
    try:
        from services.secrets.loader import selected_provider_name, validate_secret_name

        provider = selected_provider_name()
        validated_names = [validate_secret_name(name) for name in required_names]
    except Exception as exc:
        provider = str(os.environ.get("TS_SECRETS_PROVIDER") or "systemd-creds").strip().lower()
        validated_names = list(required_names)
        errors.append(f"credential validation failed: {type(exc).__name__}: {exc}")

    credential_state: Dict[str, Any] = {
        "provider": provider,
        "required_names": list(validated_names),
    }
    snapshot["credentials"] = credential_state

    if validated_names:
        if provider in {"systemd-creds", "systemd_creds"}:
            cred_dir_raw = str(os.environ.get("CREDENTIALS_DIRECTORY") or "").strip()
            credential_state["credentials_directory"] = cred_dir_raw
            if not sys.platform.startswith("linux"):
                errors.append("systemd credentials require Linux: TS_SECRETS_PROVIDER=systemd-creds")
            elif not cred_dir_raw:
                errors.append(
                    "systemd credentials unavailable: CREDENTIALS_DIRECTORY is unset; "
                    "run under a systemd unit with LoadCredentialEncrypted= for "
                    + ",".join(validated_names)
                )
            else:
                cred_dir = Path(cred_dir_raw)
                if not cred_dir.exists():
                    errors.append(f"systemd credentials directory missing: {cred_dir}")
                elif not cred_dir.is_dir():
                    errors.append(f"systemd credentials path is not a directory: {cred_dir}")
                elif not os.access(cred_dir, os.R_OK | os.X_OK):
                    errors.append(f"systemd credentials directory not readable/searchable: {cred_dir}")
                else:
                    files = {}
                    for name in validated_names:
                        state = _credential_file_snapshot(cred_dir / name)
                        files[name] = state
                        if not bool(state.get("ok")):
                            errors.append(
                                "systemd credential invalid: "
                                f"{name} reason={state.get('reason')} path={state.get('path')}"
                            )
                    credential_state["files"] = files
        elif provider == "plaintext":
            secret_dir_raw = str(os.environ.get("TS_DEV_SECRETS_DIR") or "").strip()
            credential_state["dev_secrets_dir"] = secret_dir_raw
            if _env_truthy("PROD_PREFLIGHT_FORBID_PLAINTEXT_SECRETS", default=False):
                errors.append("plaintext secrets provider forbidden by PROD_PREFLIGHT_FORBID_PLAINTEXT_SECRETS")
            if not secret_dir_raw:
                errors.append("plaintext credential directory missing: TS_DEV_SECRETS_DIR is unset")
            else:
                secret_dir = Path(secret_dir_raw)
                files = {}
                for name in validated_names:
                    state = _credential_file_snapshot(secret_dir / name)
                    files[name] = state
                    if not bool(state.get("ok")):
                        errors.append(
                            "plaintext credential invalid: "
                            f"{name} reason={state.get('reason')} path={state.get('path')}"
                        )
                credential_state["files"] = files
        else:
            errors.append(
                "unsupported secrets provider for production preflight: "
                f"{provider}; expected systemd-creds or an inline TS_PG_DSN password"
            )
    else:
        credential_state["source"] = "inline_or_env_password"

    try:
        data_root = _runtime_data_root()
        data_root_display = str(data_root)
        data_state: Dict[str, Any] = {"path": data_root_display}
        snapshot["data_root"] = data_state
        if not data_root.is_absolute():
            errors.append(f"runtime data root must be absolute: {data_root_display}")
        elif not data_root.exists():
            errors.append(
                "runtime data root missing: "
                f"{data_root_display}; create it with the trading service owner before schema init"
            )
        elif not data_root.is_dir():
            errors.append(f"runtime data root is not a directory: {data_root_display}")
        else:
            st = data_root.stat()
            mode = stat.S_IMODE(st.st_mode)
            data_state.update(
                {
                    "mode": oct(mode),
                    "uid": int(st.st_uid),
                    "gid": int(st.st_gid),
                    "current_uid": int(os.geteuid()) if hasattr(os, "geteuid") else None,
                    "readable": bool(os.access(data_root, os.R_OK)),
                    "writable": bool(os.access(data_root, os.W_OK)),
                    "searchable": bool(os.access(data_root, os.X_OK)),
                }
            )
            if not bool(data_state["readable"]):
                errors.append(f"runtime data root not readable: {data_root_display}")
            if not bool(data_state["writable"]):
                errors.append(f"runtime data root not writable: {data_root_display}")
            if not bool(data_state["searchable"]):
                errors.append(f"runtime data root not searchable: {data_root_display}")
    except Exception as exc:
        errors.append(f"runtime data root validation failed: {type(exc).__name__}: {exc}")

    if not errors:
        if validated_names:
            notes.append(f"credential source ok provider={provider} names={','.join(validated_names)}")
        else:
            notes.append("credential source ok provider=inline_or_env_password")
        data_state = dict(snapshot.get("data_root") or {})
        notes.append(
            "runtime data root ok "
            f"path={data_state.get('path')} "
            f"mode={data_state.get('mode')} "
            f"uid={data_state.get('uid')} gid={data_state.get('gid')}"
        )

    return notes, errors, snapshot


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
            live_risk_threshold_validation_snapshot,
            load_runtime_config,
        )
        from engine.runtime.live_trading_preflight import live_trading_preflight

        ConfigError = RuntimeConfigError

        safety = get_runtime_safety_context()
        cfg = load_runtime_config()
        live_risk = live_risk_threshold_validation_snapshot(safety)
        notes.append(
            "runtime config ok "
            f"env={safety.get('env')} "
            f"engine_mode={safety.get('engine_mode')} "
            f"strict_runtime={int(bool(safety.get('strict_runtime')))} "
            f"db_path={getattr(cfg, 'db_path', '')} "
            f"allow_training={int(bool(getattr(cfg, 'allow_training', False)))}"
        )
        if bool(live_risk.get("required")):
            if bool(live_risk.get("override")):
                audit = dict(live_risk.get("audit") or {})
                notes.append(
                    "live risk thresholds override accepted "
                    f"id={audit.get('LIVE_RISK_THRESHOLD_ACCEPTANCE_ID')} "
                    f"owner={audit.get('LIVE_RISK_THRESHOLD_ACCEPTANCE_OWNER')} "
                    f"issues={len(list(live_risk.get('issues') or []))}"
                )
            else:
                notes.append(
                    "live risk thresholds ok "
                    f"thresholds={len(list(live_risk.get('required_thresholds') or []))} "
                    f"enabled_flags={len(list(live_risk.get('required_enabled_flags') or []))}"
                )
        live_preflight = live_trading_preflight(
            engine_mode=safety.get("engine_mode"),
            execution_mode=os.environ.get("EXECUTION_MODE", ""),
        )
        if bool(live_preflight.get("required")):
            live_env = dict(live_preflight.get("deployment_contract") or {})
            prelive_reconcile = dict(live_preflight.get("prelive_reconcile") or {})
            backup_restore_evidence = dict(live_preflight.get("backup_restore_evidence") or {})
            execution_arming_audit = dict(live_preflight.get("execution_arming_audit") or {})
            classified_blockers: set[str] = set()

            if not bool(live_env.get("ok")):
                env_blockers = [str(item) for item in list(live_env.get("blockers") or [])]
                classified_blockers.update(env_blockers)
                issues = "; ".join(env_blockers)
                errors.append(f"live environment contract invalid: {issues or live_env.get('reason')}")

            if bool(prelive_reconcile.get("required")):
                reconcile_blockers = [str(item) for item in list(prelive_reconcile.get("blockers") or [])]
                classified_blockers.update(reconcile_blockers)
                if not bool(prelive_reconcile.get("ok")):
                    issues = "; ".join(reconcile_blockers)
                    errors.append(f"pre-live reconcile invalid: {issues or prelive_reconcile.get('reason')}")
                elif bool(prelive_reconcile.get("override")):
                    audit = dict(prelive_reconcile.get("audit") or {})
                    notes.append(
                        "pre-live reconcile break-glass accepted "
                        f"actor={audit.get('actor')} "
                        f"broker={audit.get('broker') or 'all'}"
                    )

            if bool(backup_restore_evidence.get("required")) and not bool(backup_restore_evidence.get("ok")):
                backup_blockers = [str(item) for item in list(backup_restore_evidence.get("blockers") or [])]
                classified_blockers.update(backup_blockers)
                issues = "; ".join(backup_blockers)
                errors.append(f"backup restore evidence invalid: {issues or backup_restore_evidence.get('reason')}")

            if bool(execution_arming_audit.get("required")) and not bool(execution_arming_audit.get("ok")):
                arming_blockers = [str(item) for item in list(execution_arming_audit.get("blockers") or [])]
                classified_blockers.update(arming_blockers)
                issues = "; ".join(arming_blockers)
                errors.append(f"execution arming audit invalid: {issues or execution_arming_audit.get('reason')}")

            other_blockers = [
                str(item)
                for item in list(live_preflight.get("blockers") or [])
                if str(item) not in classified_blockers
            ]
            if other_blockers:
                errors.append(f"live trading preflight invalid: {'; '.join(other_blockers)}")

            if bool(live_preflight.get("ok")):
                broker_contract = dict(live_env.get("broker_contract") or {})
                initial_hold = dict(live_env.get("initial_kill_switch_hold") or {})
                arming_audit = dict(live_preflight.get("execution_arming_audit") or {})
                notes.append(
                    "live environment contract ok "
                    f"execution_mode={live_env.get('execution_mode')} "
                    f"broker={broker_contract.get('broker')} "
                    f"chain={','.join(str(item) for item in list(broker_contract.get('chain') or []))} "
                    f"initial_kill_switch_armed={int(bool(initial_hold.get('armed')))} "
                    f"execution_arming_audited={int(bool(arming_audit.get('ok')))}"
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


def _api_mutation_auth_gate() -> Tuple[List[str], List[str]]:
    notes: List[str] = []
    errors: List[str] = []
    try:
        from engine.api.auth_config import (
            format_mutation_auth_config_error,
            safe_dev_localhost_fallback_enabled,
            validate_mutation_auth_config,
        )

        state = validate_mutation_auth_config()
        if bool(state.get("ok")):
            notes.append(
                "api mutation auth ok "
                f"strict={int(bool(state.get('strict')))} "
                f"token_configured={int(bool(state.get('dashboard_api_token_configured')))} "
                f"localhost_dev_fallback={int(bool(safe_dev_localhost_fallback_enabled()))}"
            )
            return notes, errors

        errors.append(format_mutation_auth_config_error(state))
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_API_MUTATION_AUTH_FAILED",
            e,
            once_key="api_mutation_auth_gate",
        )
        errors.append(f"api mutation auth validation failed: {type(e).__name__}: {e}")
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

    errors.append(_schema_validation_failure_summary(validation))

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


def _docker_log_cap_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    state: Dict[str, Any] = {"checked": False, "containers": []}

    explicit_check = str(os.environ.get("PREFLIGHT_CHECK_DOCKER_LOG_CAPS") or "").strip()
    strict_runtime = str(os.environ.get("ENV") or "").strip().lower() in {"prod", "production"} or str(
        os.environ.get("ENGINE_MODE") or ""
    ).strip().lower() == "live"
    if explicit_check:
        check_enabled = _env_truthy("PREFLIGHT_CHECK_DOCKER_LOG_CAPS", True)
    else:
        check_enabled = bool(strict_runtime)
    if not check_enabled:
        notes.append("docker log cap check skipped")
        state["reason"] = "disabled"
        return notes, warnings, errors, state

    docker_bin = shutil.which("docker")
    if not docker_bin:
        warnings.append("docker log cap check skipped: docker CLI unavailable")
        state["reason"] = "docker_cli_unavailable"
        return notes, warnings, errors, state

    try:
        timeout_s = max(0.5, min(30.0, float(os.environ.get("PREFLIGHT_DOCKER_LOG_CAP_TIMEOUT_S", "5") or "5")))
    except Exception:
        timeout_s = 5.0
    try:
        ps = subprocess.run(
            [docker_bin, "ps", "-q"],
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except Exception as exc:
        warnings.append(f"docker log cap check skipped: docker ps failed: {type(exc).__name__}: {exc}")
        state["reason"] = "docker_ps_failed"
        return notes, warnings, errors, state

    if int(ps.returncode or 0) != 0:
        warnings.append(f"docker log cap check skipped: docker ps rc={ps.returncode}")
        state["reason"] = "docker_ps_nonzero"
        state["stderr"] = str(ps.stderr or "")[-1000:]
        return notes, warnings, errors, state

    container_ids = [line.strip() for line in str(ps.stdout or "").splitlines() if line.strip()]
    state["checked"] = True
    if not container_ids:
        notes.append("docker log caps ok running_containers=0")
        return notes, warnings, errors, state

    try:
        inspected = subprocess.run(
            [docker_bin, "inspect", *container_ids],
            cwd=PROJECT_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_s,
            check=False,
        )
    except Exception as exc:
        errors.append(f"docker log cap validation failed: docker inspect failed: {type(exc).__name__}: {exc}")
        state["reason"] = "docker_inspect_failed"
        return notes, warnings, errors, state

    if int(inspected.returncode or 0) != 0:
        errors.append(f"docker log cap validation failed: docker inspect rc={inspected.returncode}")
        state["reason"] = "docker_inspect_nonzero"
        state["stderr"] = str(inspected.stderr or "")[-1000:]
        return notes, warnings, errors, state

    try:
        objects = list(json.loads(inspected.stdout or "[]") or [])
    except Exception as exc:
        errors.append(f"docker log cap validation failed: inspect JSON invalid: {type(exc).__name__}: {exc}")
        state["reason"] = "docker_inspect_invalid_json"
        return notes, warnings, errors, state

    uncapped: List[str] = []
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        name = str(obj.get("Name") or obj.get("Id") or "unknown").lstrip("/")
        log_config = dict(dict(obj.get("HostConfig") or {}).get("LogConfig") or {})
        driver = str(log_config.get("Type") or "").strip()
        config = dict(log_config.get("Config") or {})
        max_size = str(config.get("max-size") or "").strip()
        max_file = str(config.get("max-file") or "").strip()
        capped = bool(driver in {"local", "json-file"} and max_size and max_file)
        state["containers"].append(
            {
                "name": name,
                "driver": driver,
                "max_size": max_size,
                "max_file": max_file,
                "capped": capped,
            }
        )
        if not capped:
            uncapped.append(f"{name}:driver={driver or 'unknown'}")

    if uncapped:
        errors.append("docker log caps invalid uncapped_containers=" + ",".join(uncapped))
    else:
        notes.append(f"docker log caps ok running_containers={len(state['containers'])}")
    return notes, warnings, errors, state


def _backup_restore_evidence_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    try:
        from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

        state = dict(backup_restore_evidence_snapshot(engine_mode=os.environ.get("ENGINE_MODE", "safe")) or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_BACKUP_EVIDENCE_FAILED",
            e,
            once_key="backup_evidence_gate",
        )
        return [], [], [f"backup restore evidence validation failed: {type(e).__name__}: {e}"], {}

    policy = dict(state.get("policy") or {})
    base = dict(state.get("base_backup") or {})
    wal = dict(state.get("wal_archive") or {})
    drill = dict(state.get("restore_drill") or {})
    if bool(state.get("fresh")):
        notes.append(
            "backup restore evidence ok "
            f"base_age_s={base.get('age_s')} "
            f"wal_age_s={wal.get('age_s')} "
            f"restore_drill_age_s={drill.get('age_s')} "
            f"restore_rto_s={policy.get('restore_rto_s')}"
        )
    elif bool(state.get("required")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        errors.append(f"backup restore evidence invalid: {blockers or state.get('reason') or 'unknown'}")
    else:
        notes.append(
            "backup restore evidence not required "
            f"blockers={len(list(state.get('blockers') or []))} "
            f"path={state.get('evidence_path')}"
        )
    return notes, warnings, errors, state


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
    "sqlite3.operationalerror: database is locked",
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
        from engine.runtime.storage import get_db_validation_snapshot

        validation = dict(get_db_validation_snapshot(include_quick_check=False) or {})
        if str(validation.get("storage") or "").strip().lower() == "postgres":
            notes.append("preflight smoke DB isolation not required for Postgres runtime storage")
            return notes, warnings, errors, None, None
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_SMOKE_DB_STORAGE_CHECK_FAILED",
            e,
            once_key="smoke_db_storage_check",
        )

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
        "backup_restore_evidence": {},
        "docker_log_caps": {},
        "provisioning": {},
    }

    # Fail fast in dependency order: source integrity first, then schema,
    # then smoke jobs, then optional sanity checks.
    provisioning_notes, provisioning_errors, provisioning = _production_provisioning_gate()
    result["steps"].extend(provisioning_notes)
    result["provisioning"] = dict(provisioning or {})
    if provisioning_errors:
        result["errors"].extend(provisioning_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in provisioning_errors:
                print("[provisioning]", error)
        return 3

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

    auth_notes, auth_errors = _api_mutation_auth_gate()
    result["steps"].extend(auth_notes)
    if auth_errors:
        result["errors"].extend(auth_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in auth_errors:
                print("[api-auth]", error)
        return 3

    docker_notes, docker_warnings, docker_errors, docker_state = _docker_log_cap_gate()
    result["steps"].extend(docker_notes)
    result["warnings"].extend(docker_warnings)
    result["docker_log_caps"] = dict(docker_state or {})
    if docker_errors:
        result["errors"].extend(docker_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in docker_errors:
                print("[docker-log]", error)
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
        _sleep_schema_validation_backoff()
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

    backup_notes, backup_warnings, backup_errors, backup_evidence = _backup_restore_evidence_gate()
    result["steps"].extend(backup_notes)
    result["warnings"].extend(backup_warnings)
    result["backup_restore_evidence"] = dict(backup_evidence or {})
    if backup_errors:
        result["errors"].extend(backup_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in backup_errors:
                print("[backup]", error)
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
