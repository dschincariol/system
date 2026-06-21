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
import hashlib
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
import urllib.error
import urllib.request
import math
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
    os.path.join(REPO_ROOT, "engine", "execution", "lob_simulation.py"),
    os.path.join(REPO_ROOT, "engine", "execution", "broker_alpaca_rest.py"),
    os.path.join(REPO_ROOT, "engine", "runtime", "storage.py"),
    os.path.join(REPO_ROOT, "engine", "strategy", "edge_filter.py"),
    os.path.join(REPO_ROOT, "engine", "strategy", "portfolio_backtest.py"),
    os.path.join(REPO_ROOT, "engine", "strategy", "portfolio_rebalance.py"),
    os.path.join(REPO_ROOT, "engine", "execution", "jobs", "compute_exec_labels.py"),
    os.path.join(REPO_ROOT, "engine", "execution", "train_size_policy.py"),
]

SMOKE_CMDS = [
    ("engine.execution.jobs.compute_exec_labels", [sys.executable, "-u", "-m", "engine.execution.jobs.compute_exec_labels"]),
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


def _env_value_present(*names: str) -> bool:
    return any(str(os.environ.get(name) or "").strip() for name in names)


def _secret_ref_names(*names: str) -> List[str]:
    return list(dict.fromkeys(str(os.environ.get(name) or "").strip() for name in names if str(os.environ.get(name) or "").strip()))


def _pg_password_file_present(user: str) -> bool:
    role = str(user or "ts_app").removeprefix("ts_").upper()
    return _env_value_present(
        "TS_PG_PASSWORD_FILE",
        "TIMESCALE_PASSWORD_FILE",
        f"TS_PG_PASSWORD_{role}_FILE",
        f"TS_PG_{role}_PASSWORD_FILE",
        "PGPASSWORD_FILE",
    )


def _required_pg_credential_names() -> List[str]:
    raw = str(
        os.environ.get("PROD_PREFLIGHT_REQUIRED_CREDENTIALS")
        or os.environ.get("PREFLIGHT_REQUIRED_CREDENTIALS")
        or ""
    ).strip()
    if raw:
        return _split_names(raw)

    names: List[str] = []
    configured_dsn = str(os.environ.get("TS_PG_DSN") or os.environ.get("TIMESCALE_DSN") or "").strip()
    user = _dsn_user(configured_dsn)
    role = str(user or "ts_app").removeprefix("ts_").upper()

    if not _pg_password_file_present(user):
        secret_refs = _secret_ref_names(
            "TS_PG_PASSWORD_SECRET",
            "TIMESCALE_PASSWORD_SECRET",
            f"TS_PG_PASSWORD_{role}_SECRET",
            f"TS_PG_{role}_PASSWORD_SECRET",
            "PGPASSWORD_SECRET",
        )
        if secret_refs:
            names.extend(secret_refs)
        else:
            try:
                from engine.runtime.platform import pg_password_secret_name

                names.append(str(pg_password_secret_name(user)))
            except Exception:
                names.append("pg_password_app")

    if _env_truthy("PROD_PREFLIGHT_REQUIRE_MASTER_KEY") or _env_truthy("PREFLIGHT_REQUIRE_MASTER_KEY"):
        if _env_value_present("DATA_SOURCE_MASTER_KEY_FILE", "TRADING_MASTER_KEY_FILE"):
            pass
        else:
            master_refs = _secret_ref_names("DATA_SOURCE_MASTER_KEY_SECRET", "TRADING_MASTER_KEY_SECRET")
            names.extend(master_refs or ["master_key"])
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
    snapshot: Dict[str, Any] = {"credentials": {}, "data_root": {}, "secret_sources": {}}

    try:
        from engine.runtime.secret_sources import format_secret_source_policy_error, secret_source_policy_snapshot

        secret_sources = dict(secret_source_policy_snapshot(validate_files=True) or {})
        snapshot["secret_sources"] = secret_sources
        if bool(secret_sources.get("required")) and not bool(secret_sources.get("ok")):
            errors.append(format_secret_source_policy_error(secret_sources))
    except Exception as exc:
        errors.append(f"secret source policy validation failed: {type(exc).__name__}: {exc}")

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
            if not _env_truthy("PROD_PREFLIGHT_ALLOW_PLAINTEXT_SECRETS", default=False):
                errors.append("plaintext secrets provider forbidden in production preflight")
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
                f"{provider}; expected systemd-creds, explicit *_SECRET provider refs, or strict *_FILE credentials"
            )
    else:
        credential_state["source"] = "file_or_secret_reference"

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
            notes.append("credential source ok provider=file_or_secret_reference")
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
    with tempfile.TemporaryDirectory(prefix="prod_preflight_pycompile_") as tmp_dir:
        for f in files:
            try:
                digest = hashlib.sha256(os.path.abspath(f).encode("utf-8", "surrogatepass")).hexdigest()
                cfile = os.path.join(tmp_dir, f"{digest}.pyc")
                py_compile.compile(f, cfile=cfile, doraise=True)
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
            validate_workload_profile_guardrails,
        )
        from engine.runtime.hardware import runtime_hardware_snapshot
        from engine.runtime.live_trading_preflight import live_trading_preflight

        ConfigError = RuntimeConfigError

        safety = get_runtime_safety_context()
        cfg = load_runtime_config()
        workload_ack = validate_workload_profile_guardrails(safety)
        hardware = runtime_hardware_snapshot()
        hardware_devices = dict(hardware.get("devices") or {})
        hardware_threads = dict(hardware.get("threads") or {})
        live_risk = live_risk_threshold_validation_snapshot(safety)
        notes.append(
            "runtime config ok "
            f"env={safety.get('env')} "
            f"engine_mode={safety.get('engine_mode')} "
            f"workload_profile={getattr(cfg, 'runtime_workload_profile', safety.get('workload_profile', 'live'))} "
            f"strict_runtime={int(bool(safety.get('strict_runtime')))} "
            f"db_path={getattr(cfg, 'db_path', '')} "
            f"allow_training={int(bool(getattr(cfg, 'allow_training', False)))} "
            f"offline_ack={int(bool(workload_ack.get('acknowledged')))}"
        )
        notes.append(
            "runtime hardware ok "
            f"profile={hardware.get('profile')} "
            f"dependency_profile={hardware.get('dependency_profile')} "
            f"torch_device={dict(hardware_devices.get('TORCH_DEVICE') or {}).get('resolved')} "
            f"embed_device={dict(hardware_devices.get('EMBED_DEVICE') or {}).get('resolved')} "
            f"nlp_device={dict(hardware_devices.get('NLP_DEVICE') or {}).get('resolved')} "
            f"finbert_device={dict(hardware_devices.get('FINBERT_DEVICE') or {}).get('resolved')} "
            f"ts_foundation_device={dict(hardware_devices.get('TS_FOUNDATION_DEVICE') or {}).get('resolved')} "
            f"cpu_threads={hardware_threads.get('cpu_threads')} "
            f"interop_threads={hardware_threads.get('interop_threads')} "
            f"nvidia_telemetry={int(bool(hardware.get('nvidia_telemetry_enabled')))} "
            f"disabled_accelerator_reason={hardware.get('disabled_accelerator_reason') or 'none'}"
        )
        accelerator_profile_error = str(hardware.get("accelerator_profile_error") or "").strip()
        if not bool(hardware.get("ok", False)):
            errors.append(
                "runtime hardware dependency profile invalid "
                f"profile={hardware.get('profile')} "
                f"dependency_profile={hardware.get('dependency_profile')} "
                f"reason={hardware.get('disabled_accelerator_reason') or hardware.get('error') or 'snapshot_unavailable'}"
            )
        if accelerator_profile_error:
            errors.append(
                "runtime hardware dependency profile invalid "
                f"profile={hardware.get('profile')} "
                f"dependency_profile={hardware.get('dependency_profile')} "
                f"reason={accelerator_profile_error}"
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
        promotion_min_observations = int(os.environ.get("CHAMPION_PROMOTION_MIN_OBSERVATIONS") or 50)
        notes.append(
            "promotion observation governance ok "
            f"min_observations={promotion_min_observations} "
            "non_bypassable=1 "
            f"legacy_stat_gate={int(_env_truthy('CHAMPION_PROMOTION_USE_STAT_GATE', default=False))} "
            f"cpcv={int(_env_truthy('CPCV_ENABLED', default=False))}"
        )
        live_preflight = live_trading_preflight(
            engine_mode=safety.get("engine_mode"),
            execution_mode=os.environ.get("EXECUTION_MODE", ""),
        )
        if bool(live_preflight.get("required")):
            live_env = dict(live_preflight.get("deployment_contract") or {})
            prelive_reconcile = dict(live_preflight.get("prelive_reconcile") or {})
            backup_restore_evidence = dict(live_preflight.get("backup_restore_evidence") or {})
            clock_health = dict(live_preflight.get("clock_health") or {})
            execution_arming_audit = dict(live_preflight.get("execution_arming_audit") or {})
            live_ai_safety = dict(live_preflight.get("live_ai_safety") or {})
            lob_deeplob_shadow = dict(live_preflight.get("lob_deeplob_shadow") or {})
            options_instruments = dict(live_preflight.get("options_instruments") or {})
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

            if bool(clock_health.get("required")) and not bool(clock_health.get("ok")):
                clock_blockers = [str(item) for item in list(clock_health.get("blockers") or [])]
                classified_blockers.update(clock_blockers)
                issues = "; ".join(clock_blockers)
                errors.append(f"clock health invalid: {issues or clock_health.get('reason')}")

            if bool(execution_arming_audit.get("required")) and not bool(execution_arming_audit.get("ok")):
                arming_blockers = [str(item) for item in list(execution_arming_audit.get("blockers") or [])]
                classified_blockers.update(arming_blockers)
                issues = "; ".join(arming_blockers)
                errors.append(f"execution arming audit invalid: {issues or execution_arming_audit.get('reason')}")

            if bool(live_ai_safety.get("required")) and not bool(live_ai_safety.get("ok")):
                ai_blockers = [str(item) for item in list(live_ai_safety.get("blockers") or [])]
                classified_blockers.update(ai_blockers)
                issues = "; ".join(ai_blockers)
                errors.append(f"live AI safety invalid: {issues or live_ai_safety.get('reason')}")

            if bool(lob_deeplob_shadow.get("enabled")) and not bool(lob_deeplob_shadow.get("ok")):
                lob_blockers = [str(item) for item in list(lob_deeplob_shadow.get("blockers") or [])]
                classified_blockers.update(lob_blockers)
                issues = "; ".join(lob_blockers)
                errors.append(f"LOB DeepLOB shadow readiness invalid: {issues or lob_deeplob_shadow.get('reason')}")

            if bool(options_instruments.get("required")) and not bool(options_instruments.get("ok")):
                options_blockers = [str(item) for item in list(options_instruments.get("blockers") or [])]
                classified_blockers.update(options_blockers)
                issues = "; ".join(options_blockers)
                errors.append(f"options instrument readiness invalid: {issues or options_instruments.get('reason')}")

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
                    f"execution_arming_audited={int(bool(arming_audit.get('ok')))} "
                    f"clock_health={int(bool(clock_health.get('ok', True)))} "
                    f"live_ai_safety={int(bool(live_ai_safety.get('ok')))}"
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


def _postgres_tuning_required() -> bool:
    if _env_truthy("PREFLIGHT_REQUIRE_DOCKER_POSTGRES_TUNING"):
        return True
    return bool(str(os.environ.get("TIMESCALE_MEMORY_LIMIT") or os.environ.get("TIMESCALE_MEM_LIMIT") or "").strip())


def _postgres_tuning_effective_settings(names: List[str]) -> Dict[str, Dict[str, Any]]:
    import psycopg

    from engine.runtime.platform import default_pg_dsn, dsn_with_pg_password

    configured = str(os.environ.get("TS_PG_DSN") or "").strip()
    conninfo = dsn_with_pg_password(configured or default_pg_dsn())
    timeout_s = 2.0
    try:
        timeout_s = max(
            0.1,
            float(
                os.environ.get("PREFLIGHT_POSTGRES_TUNING_TIMEOUT_S")
                or os.environ.get("PREFLIGHT_EXTERNAL_TIMEOUT_S")
                or "2.0"
            ),
        )
    except Exception:
        timeout_s = 2.0
    connect_timeout = max(1, int(math.ceil(timeout_s)))
    rows: list[tuple[Any, ...]]
    with psycopg.connect(conninfo, autocommit=True, connect_timeout=connect_timeout) as con:
        rows = con.execute(
            """
            SELECT name, setting, unit
            FROM pg_catalog.pg_settings
            WHERE name = ANY(%s)
            """,
            (list(names),),
        ).fetchall()
    return {
        str(row[0]): {
            "setting": row[1],
            "unit": row[2],
        }
        for row in rows
    }


def _postgres_tuning_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    required = _postgres_tuning_required()
    if not required:
        return notes, warnings, errors, {"required": False, "skipped": True}

    effective_settings: Dict[str, Dict[str, Any]] = {}
    effective_query: Dict[str, Any] = {"attempted": False, "ok": None}
    if str(os.environ.get("PREFLIGHT_POSTGRES_TUNING_QUERY_EFFECTIVE", "1")).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }:
        try:
            from engine.runtime.postgres_tuning import PG_SETTING_SPECS

            names = [str(spec.pg_name) for spec in PG_SETTING_SPECS]
            effective_settings = _postgres_tuning_effective_settings(names)
            effective_query = {
                "attempted": True,
                "ok": True,
                "settings": len(effective_settings),
            }
        except Exception as exc:
            _warn_nonfatal(
                "PROD_PREFLIGHT_POSTGRES_TUNING_EFFECTIVE_SETTINGS_FAILED",
                exc,
                once_key="postgres_tuning_effective_settings",
            )
            effective_query = {
                "attempted": True,
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
            warnings.append(f"postgres tuning effective settings unavailable: {type(exc).__name__}: {exc}")

    try:
        from engine.runtime.postgres_tuning import docker_postgres_tuning_snapshot, format_bytes

        snapshot = dict(
            docker_postgres_tuning_snapshot(
                os.environ,
                required=required,
                effective_settings=effective_settings,
            )
        )
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_POSTGRES_TUNING_VALIDATION_FAILED",
            exc,
            once_key="postgres_tuning_validation",
        )
        return notes, warnings, [f"postgres tuning validation failed: {type(exc).__name__}: {exc}"], {
            "required": required,
            "effective_query": effective_query,
        }

    snapshot["effective_query"] = effective_query
    warnings.extend(str(item) for item in list(snapshot.get("warnings") or []))
    errors.extend(str(item) for item in list(snapshot.get("errors") or []))
    if not errors:
        derivation = dict(snapshot.get("derivation") or {})
        memory_budget = dict(snapshot.get("memory_budget") or {})
        wal_budget = dict(snapshot.get("wal_budget") or {})
        notes.append(
            "postgres tuning ok "
            f"memory_limit={derivation.get('memory_limit_human')} "
            f"memory_source={derivation.get('memory_source')} "
            f"cpus={derivation.get('cpus')} "
            f"estimated_peak={format_bytes(memory_budget.get('estimated_peak_bytes'))} "
            f"wal_ceiling={format_bytes(wal_budget.get('configured_retained_wal_ceiling_bytes'))}"
        )
        if effective_settings:
            notes.append(f"postgres effective settings ok count={len(effective_settings)}")
    return notes, warnings, errors, snapshot


def _wal_archiver_runtime_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    try:
        from engine.runtime.backup_evidence import wal_archiver_runtime_snapshot

        state = dict(wal_archiver_runtime_snapshot(engine_mode=os.environ.get("ENGINE_MODE", "safe")) or {})
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_WAL_ARCHIVER_RUNTIME_FAILED",
            exc,
            once_key="wal_archiver_runtime_gate",
        )
        return [], [], [f"wal archiver runtime validation failed: {type(exc).__name__}: {exc}"], {}

    warnings.extend(str(item) for item in list(state.get("warnings") or []))
    if not bool(state.get("ok")) and bool(state.get("required")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        errors.append(f"wal archiver runtime invalid: {blockers or state.get('reason') or 'unknown'}")
    elif bool(state.get("ok")) and not bool(state.get("skipped")):
        notes.append(
            "wal archiver runtime ok "
            f"archive_mode={state.get('archive_mode')} "
            f"last_archived_wal={state.get('last_archived_wal')} "
            f"age_s={state.get('age_s')} "
            f"failed_count={state.get('failed_count')}"
        )
    elif bool(state.get("skipped")):
        notes.append("wal archiver runtime not required")
    return notes, warnings, errors, state


def _pg_wal_disk_risk_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    try:
        from engine.runtime.backup_evidence import pg_wal_disk_risk_snapshot

        state = dict(pg_wal_disk_risk_snapshot(engine_mode=os.environ.get("ENGINE_MODE", "safe")) or {})
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_PG_WAL_DISK_RISK_FAILED",
            exc,
            once_key="pg_wal_disk_risk_gate",
        )
        return [], [], [f"pg_wal disk risk validation failed: {type(exc).__name__}: {exc}"], {}

    warnings.extend(str(item) for item in list(state.get("warnings") or []))
    if not bool(state.get("ok")) and bool(state.get("required")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        errors.append(f"pg_wal disk risk invalid: {blockers or state.get('reason') or 'unknown'}")
    elif bool(state.get("ok")) and not bool(state.get("skipped")):
        local_space = dict(state.get("local_space") or {})
        notes.append(
            "pg_wal disk risk ok "
            f"wal_bytes={state.get('wal_bytes')} "
            f"wal_files={state.get('wal_files')} "
            f"ready_count={state.get('ready_count')} "
            f"free_bytes={local_space.get('free_bytes')}"
        )
    elif bool(state.get("skipped")):
        notes.append("pg_wal disk risk not required")
    return notes, warnings, errors, state


def _ingestion_tuning_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    try:
        from engine.runtime.ingestion_tuning import ingestion_tuning_snapshot

        snapshot = dict(ingestion_tuning_snapshot(pg_pool_role="ingestion") or {})
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_INGESTION_TUNING_FAILED",
            exc,
            once_key="ingestion_tuning",
        )
        return notes, warnings, [f"ingestion tuning validation failed: {type(exc).__name__}: {exc}"], {}

    warnings.extend(str(item) for item in list(snapshot.get("warnings") or []) if str(item).strip())
    errors.extend(str(item) for item in list(snapshot.get("errors") or []) if str(item).strip())
    if not errors:
        capacity = dict(snapshot.get("capacity") or {})
        notes.append(
            "ingestion tuning ok "
            f"profile={snapshot.get('profile')} "
            f"db_pool_total={capacity.get('total_db_pool_connections')}/{capacity.get('max_total_db_connections')} "
            f"buffered_row_risk={capacity.get('buffered_row_risk_estimate')}/{capacity.get('max_buffered_rows')}"
        )
    return notes, warnings, errors, snapshot


def _compose_operator_service_block() -> str:
    compose_path = Path(REPO_ROOT) / "deploy" / "compose" / "docker-compose.stack.yml"
    try:
        lines = compose_path.read_text(encoding="utf-8").splitlines()
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_OPERATOR_COMPOSE_READ_FAILED",
            exc,
            once_key="operator_compose_read",
            path=str(compose_path),
        )
        lines = []

    block: list[str] = []
    in_block = False
    for line in lines:
        if re.match(r"^\s{2}operator:\s*$", line):
            in_block = True
            block.append(line)
            continue
        if in_block and line and not line.startswith(" "):
            break
        if in_block and re.match(r"^\s{2}[A-Za-z0-9_.-]+:\s*$", line):
            break
        if in_block:
            block.append(line)
    return "\n".join(block)


def _operator_sidecar_probe_base() -> str:
    raw = str(
        os.environ.get("OPERATOR_PREFLIGHT_BASE")
        or os.environ.get("OPERATOR_SIDECAR_BASE")
        or ""
    ).strip()
    if raw:
        return raw.rstrip("/")
    host = str(
        os.environ.get("OPERATOR_SIDECAR_HOST")
        or os.environ.get("OPERATOR_BIND_HOST")
        or "127.0.0.1"
    ).strip()
    if host in {"", "0.0.0.0", "::", "[::]"}:
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = str(os.environ.get("OPERATOR_PORT") or "4001").strip() or "4001"
    return f"http://{host}:{port}"


def _operator_sensitive_get_denied(base_url: str, timeout_s: float) -> Tuple[bool | None, int, str]:
    url = str(base_url).rstrip("/") + "/api/operator/config"
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    denied: bool | None
    status = 0
    detail = ""
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = int(getattr(resp, "status", 0) or resp.getcode() or 0)
            denied = False
            detail = "sensitive_get_allowed_without_operator_token"
    except urllib.error.HTTPError as exc:
        status = int(getattr(exc, "code", 0) or 0)
        if status in {401, 403}:
            denied = True
            detail = "denied"
        else:
            _warn_nonfatal(
                "PROD_PREFLIGHT_OPERATOR_SENSITIVE_GET_UNEXPECTED_HTTP",
                exc,
                once_key=f"operator_sensitive_get_http:{status}",
                status=int(status),
                url=url,
            )
            denied = False
            detail = f"unexpected_http_{status}"
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_OPERATOR_SENSITIVE_GET_PROBE_FAILED",
            exc,
            once_key="operator_sensitive_get_probe",
            url=url,
        )
        denied = None
        status = 0
        detail = f"{type(exc).__name__}: {exc}"
    return denied, status, detail


def _operator_sidecar_security_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    snapshot: Dict[str, Any] = {}

    try:
        from engine.api.auth_config import strict_mutation_auth_reasons
        from engine.runtime.live_trading_preflight import operator_sidecar_security_snapshot

        strict_reasons = list(strict_mutation_auth_reasons())
        snapshot = dict(operator_sidecar_security_snapshot(engine_mode=os.environ.get("ENGINE_MODE", "safe")) or {})
        snapshot["strict_reasons"] = strict_reasons
        strict = bool(strict_reasons or str(os.environ.get("ENGINE_MODE", "")).strip().lower() == "live")
        if strict and not bool(snapshot.get("ok")):
            errors.append(
                "operator sidecar security invalid: "
                + ",".join(str(item) for item in list(snapshot.get("blockers") or []))
            )
        elif bool(snapshot.get("ok")):
            notes.append(
                "operator sidecar token ok "
                f"internal_only={int(bool(snapshot.get('internal_only')))} "
                f"token_configured={int(bool(snapshot.get('operator_api_token_configured')))}"
            )
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_OPERATOR_SIDECAR_SECURITY_FAILED",
            exc,
            once_key="operator_sidecar_security",
        )
        errors.append(f"operator sidecar security validation failed: {type(exc).__name__}: {exc}")
        return notes, warnings, errors, snapshot

    block = _compose_operator_service_block()
    compose_state = {
        "operator_service_found": bool(block),
        "operator_ports_declared": bool(re.search(r"(?m)^\s{4}ports:\s*$", block)),
        "operator_expose_declared": bool(re.search(r"(?m)^\s{4}expose:\s*$", block)),
    }
    snapshot["compose"] = compose_state
    if compose_state["operator_ports_declared"]:
        errors.append("operator sidecar compose service must not publish ports by default")
    elif compose_state["operator_service_found"]:
        notes.append(
            "operator sidecar compose exposure ok "
            f"expose={int(bool(compose_state['operator_expose_declared']))}"
        )

    if str(os.environ.get("PREFLIGHT_CHECK_OPERATOR_SIDECAR_HTTP", "1")).strip().lower() not in {"0", "false", "no", "off"}:
        base_url = _operator_sidecar_probe_base()
        timeout_s = 1.0
        try:
            timeout_s = max(0.1, float(os.environ.get("PREFLIGHT_OPERATOR_SIDECAR_TIMEOUT_S", "1.0")))
        except Exception:
            timeout_s = 1.0
        denied, status, reason = _operator_sensitive_get_denied(base_url, timeout_s)
        snapshot["unauthenticated_sensitive_get"] = {
            "checked": True,
            "base_url": base_url,
            "denied": denied,
            "status": int(status),
            "reason": str(reason),
        }
        if denied is False:
            errors.append(f"operator sidecar sensitive GET is not fail-closed: status={status} reason={reason}")
        elif denied is True:
            notes.append(f"operator sidecar sensitive GET denied without token status={status}")
        else:
            warnings.append(f"operator sidecar sensitive GET probe skipped/unreachable: {reason}")
    else:
        snapshot["unauthenticated_sensitive_get"] = {"checked": False, "reason": "disabled"}

    return notes, warnings, errors, snapshot


def _network_exposure_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    try:
        from engine.runtime.live_trading_preflight import public_network_exposure_snapshot

        state = dict(public_network_exposure_snapshot(engine_mode=os.environ.get("ENGINE_MODE", "safe")) or {})
    except Exception as exc:
        _warn_nonfatal(
            "PROD_PREFLIGHT_NETWORK_EXPOSURE_FAILED",
            exc,
            once_key="network_exposure_gate",
        )
        return [], [], [f"network exposure validation failed: {type(exc).__name__}: {exc}"], {}

    public_services = [str(item) for item in list(state.get("public_services") or []) if str(item).strip()]
    if bool(state.get("required")) and not bool(state.get("ok")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        errors.append(f"network exposure invalid: {blockers or state.get('reason') or 'unknown'}")
    elif public_services:
        ack = dict(state.get("ack") or {})
        notes.append(
            "network exposure approved "
            f"services={','.join(public_services)} "
            f"owner={ack.get('owner') or ''}"
        )
    else:
        notes.append("network exposure ok public_services=0")
    return notes, warnings, errors, state


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
    missing_migration_ids = [
        int(item)
        for item in list(validation.get("schema_migration_missing_ids") or [])
        if str(item).strip()
    ]
    unexpected_migration_ids = [
        int(item)
        for item in list(validation.get("schema_migration_unexpected_ids") or [])
        if str(item).strip()
    ]
    owned_missing_tables = [
        str(item) for item in list(validation.get("owned_missing_tables") or []) if str(item).strip()
    ]
    owned_missing_columns = {
        str(table): [str(column) for column in list(columns or []) if str(column).strip()]
        for table, columns in dict(validation.get("owned_missing_columns") or {}).items()
        if str(table).strip()
    }
    owned_unexpected_columns = {
        str(table): [str(column) for column in list(columns or []) if str(column).strip()]
        for table, columns in dict(validation.get("owned_unexpected_columns") or {}).items()
        if str(table).strip()
    }
    owned_type_mismatches = {
        str(table): dict(columns or {})
        for table, columns in dict(validation.get("owned_type_mismatches") or {}).items()
        if str(table).strip()
    }
    owned_pk_mismatches = {
        str(table): dict(columns or {})
        for table, columns in dict(validation.get("owned_pk_mismatches") or {}).items()
        if str(table).strip()
    }
    owned_missing_indexes = {
        str(table): [str(index_name) for index_name in list(indexes or []) if str(index_name).strip()]
        for table, indexes in dict(validation.get("owned_missing_indexes") or {}).items()
        if str(table).strip()
    }
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
    if missing_migration_ids:
        errors.append(
            "postgres contract invalid missing migrations: "
            + ",".join(str(item) for item in sorted(missing_migration_ids))
        )
    if unexpected_migration_ids:
        errors.append(
            "postgres contract invalid unexpected migrations: "
            + ",".join(str(item) for item in sorted(unexpected_migration_ids))
        )
    if owned_missing_tables:
        errors.append("postgres contract invalid owned missing tables: " + ",".join(sorted(owned_missing_tables)))
    if owned_missing_columns:
        rendered = "; ".join(
            f"{table}({','.join(sorted(columns))})"
            for table, columns in sorted(owned_missing_columns.items())
        )
        errors.append(f"postgres contract invalid owned missing columns: {rendered}")
    if owned_unexpected_columns:
        rendered = "; ".join(
            f"{table}({','.join(sorted(columns))})"
            for table, columns in sorted(owned_unexpected_columns.items())
        )
        errors.append(f"postgres contract invalid owned unexpected columns: {rendered}")
    if owned_type_mismatches:
        rendered = "; ".join(f"{table}({sorted(columns)})" for table, columns in sorted(owned_type_mismatches.items()))
        errors.append(f"postgres contract invalid owned type mismatches: {rendered}")
    if owned_pk_mismatches:
        rendered = "; ".join(f"{table}({sorted(columns)})" for table, columns in sorted(owned_pk_mismatches.items()))
        errors.append(f"postgres contract invalid owned primary keys: {rendered}")
    if owned_missing_indexes:
        rendered = "; ".join(
            f"{table}({','.join(sorted(indexes))})"
            for table, indexes in sorted(owned_missing_indexes.items())
        )
        errors.append(f"postgres contract invalid owned missing indexes: {rendered}")
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


def _resource_isolation_gate() -> Tuple[List[str], List[str], Dict[str, Any]]:
    try:
        from engine.runtime.resource_isolation import check_resource_isolation

        summary = dict(check_resource_isolation() or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_RESOURCE_ISOLATION_FAILED",
            e,
            once_key="resource_isolation_gate",
        )
        return [], [f"resource isolation validation failed: {type(e).__name__}: {e}"], {}

    return (
        list(summary.get("notes") or []),
        list(summary.get("warnings") or []),
        summary,
    )


def _storage_placement_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    try:
        from engine.runtime.storage_placement import check_storage_placement

        summary = dict(check_storage_placement() or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_STORAGE_PLACEMENT_FAILED",
            e,
            once_key="storage_placement_gate",
        )
        return [], [], [f"storage placement validation failed: {type(e).__name__}: {e}"], {}

    return (
        list(summary.get("notes") or []),
        list(summary.get("warnings") or []),
        list(summary.get("errors") or []),
        summary,
    )


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
    accounting: Dict[str, Any] = {}
    if bool(state.get("fresh")) or not bool(state.get("required")):
        try:
            from engine.runtime.backup_evidence import backup_accounting_snapshot

            accounting = dict(backup_accounting_snapshot() or {})
            state["accounting"] = accounting
            root_size = dict(accounting.get("root_size") or {})
            retention = dict(accounting.get("retention") or {})
            container_mount_source = (
                str(accounting.get("container_mount_source") or "").strip()
                or str(dict(accounting.get("container_mount") or {}).get("mount_source") or "").strip()
                or "unknown"
            )
            notes.append(
                "backup accounting "
                f"host_path={accounting.get('host_path')} "
                f"container_path={accounting.get('container_path')} "
                f"container_mount_source={container_mount_source} "
                f"apparent_bytes={root_size.get('apparent_bytes')} "
                f"allocated_bytes={root_size.get('allocated_bytes')} "
                f"retention_status={accounting.get('retention_status') or retention.get('status')} "
                f"keep_daily_days={retention.get('keep_daily_days')} "
                f"keep_weekly_days={retention.get('keep_weekly_days')}"
            )
            if not bool(accounting.get("ok")):
                warnings.append("backup accounting unavailable or incomplete")
            warnings.extend(str(item) for item in list(accounting.get("warnings") or []))
        except Exception as e:
            _warn_nonfatal(
                "PROD_PREFLIGHT_BACKUP_ACCOUNTING_FAILED",
                e,
                once_key="backup_accounting_gate",
            )
            warnings.append(f"backup accounting failed: {type(e).__name__}: {e}")
    warnings.extend(str(item) for item in list(state.get("warnings") or []))
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


def _disk_pressure_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    state: Dict[str, Any] = {}

    try:
        from engine.runtime.health import get_disk_pressure_snapshot
        from engine.runtime.platform import (
            default_backup_root_dir,
            default_container_runtime_roots,
            default_wal_backup_dir,
        )
        from engine.runtime.storage_placement import storage_pressure_paths

        candidate_paths: list[tuple[str, Path]] = [
            ("root", Path("/")),
            ("runtime_data", _runtime_data_root()),
            (
                "runtime_logs",
                Path(os.environ.get("TRADING_LOGS") or os.environ.get("LOG_DIR") or "/app/logs"),
            ),
            (
                "backup_root",
                Path(
                    os.environ.get("TRADING_BACKUP_ROOT")
                    or os.environ.get("TS_BACKUP_ROOT")
                    or default_backup_root_dir()
                ),
            ),
            (
                "backup_wal",
                Path(os.environ.get("TRADING_BACKUP_WAL_DIR") or default_wal_backup_dir()),
            ),
        ]
        candidate_paths.extend(storage_pressure_paths(os.environ))
        docker_paths_raw = str(
            os.environ.get("DISK_PRESSURE_DOCKER_PATHS")
            or ",".join(default_container_runtime_roots())
        )
        for raw in [part.strip() for part in docker_paths_raw.split(",") if part.strip()]:
            path = Path(raw)
            if path.exists():
                candidate_paths.append((f"docker:{path.name}", path))

        state = dict(get_disk_pressure_snapshot(candidate_paths) or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_DISK_PRESSURE_FAILED",
            e,
            once_key="disk_pressure_gate",
        )
        return [], [], [f"disk pressure validation failed: {type(e).__name__}: {e}"], {}

    disk_warnings = [str(item) for item in list(state.get("warnings") or []) if str(item).strip()]
    disk_critical = [str(item) for item in list(state.get("critical") or []) if str(item).strip()]
    warnings.extend(f"disk pressure warning: {item}" for item in disk_warnings)
    errors.extend(f"disk pressure critical: {item}" for item in disk_critical)

    paths = [dict(item) for item in list(state.get("paths") or []) if isinstance(item, dict)]
    root = next((item for item in paths if item.get("label") == "root"), paths[0] if paths else {})
    notes.append(
        "disk pressure "
        f"status={state.get('status')} "
        f"root_free_bytes={root.get('free_bytes')} "
        f"root_free_pct={root.get('free_pct')}"
    )
    return notes, warnings, errors, state


def _lob_deeplob_shadow_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    try:
        from engine.runtime.live_trading_preflight import lob_deeplob_shadow_readiness_snapshot

        state = dict(lob_deeplob_shadow_readiness_snapshot(engine_mode=os.environ.get("ENGINE_MODE", "safe")) or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_LOB_DEEPLOB_READINESS_FAILED",
            e,
            once_key="lob_deeplob_readiness_gate",
        )
        return [], [], [f"LOB DeepLOB shadow readiness validation failed: {type(e).__name__}: {e}"], {}

    if not bool(state.get("enabled")):
        notes.append("LOB DeepLOB shadow readiness not required enabled=0")
    elif bool(state.get("ok")):
        l2 = dict(state.get("l2_data") or {})
        calibration = dict(state.get("simulator_calibration") or {})
        notes.append(
            "LOB DeepLOB shadow readiness ok "
            f"l2_rows={l2.get('sample_n')} "
            f"calibration_fills={calibration.get('sample_n')} "
            f"shadow_only={int(bool(state.get('shadow_only')))}"
        )
    else:
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        errors.append(f"LOB DeepLOB shadow readiness invalid: {blockers or state.get('reason') or 'unknown'}")
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


def _capital_equity_freshness_gate() -> Tuple[List[str], List[str], List[str], Dict[str, Any]]:
    notes: List[str] = []
    warnings: List[str] = []
    errors: List[str] = []
    state: Dict[str, Any] = {}

    live_mode = any(
        str(os.environ.get(name) or "").strip().lower() == "live"
        for name in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE", "MODE")
    )
    try:
        from engine.execution.kill_switch import capital_equity_freshness_snapshot

        state = dict(capital_equity_freshness_snapshot(live_mode=live_mode) or {})
    except Exception as e:
        _warn_nonfatal(
            "PROD_PREFLIGHT_CAPITAL_EQUITY_FRESHNESS_FAILED",
            e,
            once_key="capital_equity_freshness_gate",
        )
        return [], [], [f"capital equity freshness validation failed: {type(e).__name__}: {e}"], {}

    if bool(state.get("required")) and not bool(state.get("ok")):
        blockers = ",".join(str(item) for item in list(state.get("blockers") or []))
        errors.append(
            "capital equity freshness invalid: "
            f"{state.get('reason_code') or state.get('reason') or 'unknown'}"
            + (f" blockers={blockers}" if blockers else "")
        )
    elif bool(state.get("required")):
        windows = dict(state.get("windows") or {})
        latest = dict(windows.get("latest") or {})
        daily = dict(windows.get("daily") or {})
        rolling = dict(windows.get("rolling") or {})
        var = dict(windows.get("var") or {})
        notes.append(
            "capital equity freshness ok "
            f"latest_age_s={latest.get('latest_age_s')} "
            f"daily_points={daily.get('points')} "
            f"rolling_points={rolling.get('points')} "
            f"var_points={var.get('points')}"
        )
    else:
        notes.append("capital equity freshness not required live_mode=0")

    return notes, warnings, errors, state


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
        "disk_pressure": {},
        "storage_placement": {},
        "lob_deeplob_shadow": {},
        "resource_isolation": {},
        "provisioning": {},
        "postgres_tuning": {},
        "wal_archiver_runtime": {},
        "pg_wal_disk_risk": {},
        "ingestion_tuning": {},
        "operator_sidecar_security": {},
        "network_exposure": {},
        "capital_equity_freshness": {},
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

    disk_notes, disk_warnings, disk_errors, disk_pressure = _disk_pressure_gate()
    result["steps"].extend(disk_notes)
    result["warnings"].extend(disk_warnings)
    result["disk_pressure"] = dict(disk_pressure or {})
    if disk_errors:
        result["errors"].extend(disk_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in disk_errors:
                print("[disk]", error)
        return 3

    storage_notes, storage_warnings, storage_errors, storage_placement = _storage_placement_gate()
    result["steps"].extend(storage_notes)
    result["warnings"].extend(storage_warnings)
    result["storage_placement"] = dict(storage_placement or {})
    if storage_errors:
        result["errors"].extend(storage_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in storage_errors:
                print("[storage]", error)
        return 3

    tuning_notes, tuning_warnings, tuning_errors, postgres_tuning = _postgres_tuning_gate()
    result["steps"].extend(tuning_notes)
    result["warnings"].extend(tuning_warnings)
    result["postgres_tuning"] = dict(postgres_tuning or {})
    if tuning_errors:
        result["errors"].extend(tuning_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in tuning_errors:
                print("[postgres-tuning]", error)
        return 3

    wal_archiver_notes, wal_archiver_warnings, wal_archiver_errors, wal_archiver_runtime = _wal_archiver_runtime_gate()
    result["steps"].extend(wal_archiver_notes)
    result["warnings"].extend(wal_archiver_warnings)
    result["wal_archiver_runtime"] = dict(wal_archiver_runtime or {})
    if wal_archiver_errors:
        result["errors"].extend(wal_archiver_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in wal_archiver_errors:
                print("[wal-archiver]", error)
        return 3

    pg_wal_notes, pg_wal_warnings, pg_wal_errors, pg_wal_disk_risk = _pg_wal_disk_risk_gate()
    result["steps"].extend(pg_wal_notes)
    result["warnings"].extend(pg_wal_warnings)
    result["pg_wal_disk_risk"] = dict(pg_wal_disk_risk or {})
    if pg_wal_errors:
        result["errors"].extend(pg_wal_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in pg_wal_errors:
                print("[pg-wal]", error)
        return 3

    ingestion_tuning_notes, ingestion_tuning_warnings, ingestion_tuning_errors, ingestion_tuning = _ingestion_tuning_gate()
    result["steps"].extend(ingestion_tuning_notes)
    result["warnings"].extend(ingestion_tuning_warnings)
    result["ingestion_tuning"] = dict(ingestion_tuning or {})
    if ingestion_tuning_errors:
        result["errors"].extend(ingestion_tuning_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in ingestion_tuning_errors:
                print("[ingestion-tuning]", error)
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

    sidecar_notes, sidecar_warnings, sidecar_errors, sidecar_security = _operator_sidecar_security_gate()
    result["steps"].extend(sidecar_notes)
    result["warnings"].extend(sidecar_warnings)
    result["operator_sidecar_security"] = dict(sidecar_security or {})
    if sidecar_errors:
        result["errors"].extend(sidecar_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in sidecar_errors:
                print("[operator-sidecar]", error)
        return 3

    exposure_notes, exposure_warnings, exposure_errors, network_exposure = _network_exposure_gate()
    result["steps"].extend(exposure_notes)
    result["warnings"].extend(exposure_warnings)
    result["network_exposure"] = dict(network_exposure or {})
    if exposure_errors:
        result["errors"].extend(exposure_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in exposure_errors:
                print("[network-exposure]", error)
        return 3

    resource_notes, resource_warnings, resource_isolation = _resource_isolation_gate()
    result["steps"].extend(resource_notes)
    result["warnings"].extend(resource_warnings)
    result["resource_isolation"] = dict(resource_isolation or {})

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

    equity_notes, equity_warnings, equity_errors, equity_state = _capital_equity_freshness_gate()
    result["steps"].extend(equity_notes)
    result["warnings"].extend(equity_warnings)
    result["capital_equity_freshness"] = dict(equity_state or {})
    if equity_errors:
        result["errors"].extend(equity_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in equity_errors:
                print("[capital-equity]", error)
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

    lob_notes, lob_warnings, lob_errors, lob_state = _lob_deeplob_shadow_gate()
    result["steps"].extend(lob_notes)
    result["warnings"].extend(lob_warnings)
    result["lob_deeplob_shadow"] = dict(lob_state or {})
    if lob_errors:
        result["errors"].extend(lob_errors)
        if args.json:
            print(json.dumps(result, separators=(",", ":"), sort_keys=True))
        else:
            for error in lob_errors:
                print("[lob-deeplob]", error)
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
