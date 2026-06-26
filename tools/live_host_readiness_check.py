"""Host-only readiness checks for the live validation gate."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MIN_SWAP_BYTES = 16 * 1024 * 1024 * 1024
SYSTEMD_UNITS = {
    "trading-engine.service": {
        "type": "notify",
        "memory_max": str(32 * 1024 * 1024 * 1024),
        "oom_score_adjust": "600",
    },
    "trading-operator.service": {
        "type": "notify",
        "memory_max": str(6 * 1024 * 1024 * 1024),
        "oom_score_adjust": "700",
    },
}
BACKUP_SYSTEMD_UNITS = (
    "trading-backup.service",
    "trading-backup.timer",
    "trading-restore-drill.service",
    "trading-restore-drill.timer",
)
INLINE_SECRET_ENV_NAMES = (
    "TRADING_MASTER_KEY",
    "APP_MASTER_KEY",
    "DASHBOARD_API_TOKEN",
    "DATA_SOURCE_MASTER_KEY",
    "BACKUP_EVIDENCE_HMAC_KEY",
)
DEFAULT_BACKUP_KEY_PATH = Path("/etc/trading/backup_evidence.hmac.key")
PAID_FEED_CREDENTIAL_SOURCES = {
    "polygon": ("any", ("POLYGON_API_KEY", "POLYGON_KEY")),
    "polygon_ws": ("any", ("POLYGON_API_KEY", "POLYGON_KEY")),
    "ibkr": ("all", ("IBKR_HOST", "IBKR_PORT", "IBKR_CLIENT_ID")),
}


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


def _run(args: list[str]) -> CommandResult:
    try:
        proc = subprocess.run(args, check=False, capture_output=True, text=True)
    except FileNotFoundError:
        return CommandResult(127, "", f"missing executable: {args[0]}")
    return CommandResult(proc.returncode, proc.stdout, proc.stderr)


def _parse_systemctl_show(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        if "=" not in raw_line:
            continue
        key, value = raw_line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _mode_octal(mode: int) -> str:
    return f"0o{mode:03o}"


def _swap_total_bytes() -> tuple[int | None, str | None]:
    if shutil.which("swapon") is None:
        return None, "swapon_missing"
    result = _run(["swapon", "--show=SIZE", "--bytes", "--noheadings", "--raw"])
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        return None, f"swapon_failed:{detail or result.returncode}"
    total = 0
    for raw_line in result.stdout.splitlines():
        raw = raw_line.strip()
        if not raw:
            continue
        tokens = raw.split()
        candidate = tokens[0]
        if len(tokens) >= 3 and tokens[2].isdigit():
            candidate = tokens[2]
        try:
            total += int(candidate)
        except ValueError:
            return None, f"swapon_unparseable_size:{raw}"
    return total, None


def _systemd_unit_errors(unit: str, expected: dict[str, str], *, require_active: bool = False) -> list[str]:
    result = _run(
        [
            "systemctl",
            "show",
            unit,
            "-p",
            "LoadState",
            "-p",
            "ActiveState",
            "-p",
            "Type",
            "-p",
            "WatchdogUSec",
            "-p",
            "MemoryMax",
            "-p",
            "OOMScoreAdjust",
        ]
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        return [f"{unit}:systemctl_show_failed:{detail or result.returncode}"]

    values = _parse_systemctl_show(result.stdout)
    if values.get("LoadState") in {"", "not-found"}:
        if require_active:
            return [f"{unit}:not_installed"]
        return []

    errors: list[str] = []
    if require_active and values.get("ActiveState") != "active":
        errors.append(f"{unit}:active_state_invalid:{values.get('ActiveState') or '<missing>'}")

    if values.get("Type") != expected["type"]:
        errors.append(f"{unit}:type_invalid:{values.get('Type') or '<missing>'}")

    watchdog = values.get("WatchdogUSec") or ""
    if watchdog in {"", "0", "infinity"}:
        state = values.get("ActiveState") or "unknown"
        errors.append(
            f"{unit}:watchdog_not_applied:WatchdogUSec={watchdog or '<missing>'}:"
            f"ActiveState={state}:run sudo systemctl daemon-reload && "
            f"sudo systemctl restart {unit}"
        )

    if values.get("MemoryMax") != expected["memory_max"]:
        errors.append(
            f"{unit}:memory_max_invalid:{values.get('MemoryMax') or '<missing>'}:"
            f"expected={expected['memory_max']}"
        )

    if values.get("OOMScoreAdjust") != expected["oom_score_adjust"]:
        errors.append(
            f"{unit}:oom_score_adjust_invalid:{values.get('OOMScoreAdjust') or '<missing>'}:"
            f"expected={expected['oom_score_adjust']}"
        )
    return errors


def _backup_systemd_unit_errors(unit: str, *, require_active: bool = False) -> list[str]:
    result = _run(
        [
            "systemctl",
            "show",
            unit,
            "-p",
            "LoadState",
            "-p",
            "ActiveState",
            "-p",
            "UnitFileState",
        ]
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        return [f"{unit}:systemctl_show_failed:{detail or result.returncode}"]

    values = _parse_systemctl_show(result.stdout)
    if values.get("LoadState") in {"", "not-found"}:
        if require_active:
            return [f"{unit}:not_installed"]
        return []

    errors: list[str] = []
    if require_active and unit.endswith(".timer"):
        if values.get("ActiveState") != "active":
            errors.append(f"{unit}:active_state_invalid:{values.get('ActiveState') or '<missing>'}")
        if values.get("UnitFileState") not in {"enabled", "enabled-runtime"}:
            errors.append(f"{unit}:unit_file_state_invalid:{values.get('UnitFileState') or '<missing>'}")
    return errors


def _backup_key_mode_errors(environ: Mapping[str, str]) -> list[str]:
    raw_path = str(environ.get("BACKUP_EVIDENCE_HMAC_KEY_FILE") or "").strip()
    path = Path(raw_path).expanduser() if raw_path else DEFAULT_BACKUP_KEY_PATH
    try:
        st = path.stat()
    except FileNotFoundError:
        return ["backup_key_missing"]
    except OSError as exc:
        return [f"backup_key_stat_failed:{type(exc).__name__}"]
    if not path.is_file():
        return ["backup_key_not_regular_file"]
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o600:
        return [f"backup_key_mode_insecure:{_mode_octal(mode)}"]
    return []


def _inline_secret_errors(environ: Mapping[str, str]) -> list[str]:
    errors: list[str] = []
    for name in INLINE_SECRET_ENV_NAMES:
        if str(environ.get(name) or "").strip():
            errors.append(f"inline_secret_present:{name}")
    return errors


def _secret_file_reference_ok(raw_path: str) -> bool:
    path_text = str(raw_path or "").strip()
    if not path_text or os.path.normpath(path_text) == "/dev/null":
        return False
    path = Path(path_text).expanduser()
    try:
        st = path.stat()
    except OSError:
        return False
    if not path.is_file() or st.st_size <= 0 or not os.access(path, os.R_OK):
        return False
    mode = stat.S_IMODE(st.st_mode)
    return not bool(mode & (stat.S_IRWXG | stat.S_IRWXO))


def _approved_secret_source_configured(key: str, environ: Mapping[str, str]) -> bool:
    try:
        from engine.runtime.secret_sources import SECRET_ENV_SPEC_BY_KEY

        spec = SECRET_ENV_SPEC_BY_KEY.get(str(key))
    except Exception:
        spec = None

    file_envs = tuple(getattr(spec, "file_envs", ()) or (f"{key}_FILE",))
    secret_envs = tuple(getattr(spec, "secret_envs", ()) or (f"{key}_SECRET",))
    for env_name in file_envs:
        if _secret_file_reference_ok(str(environ.get(env_name) or "")):
            return True
    for env_name in secret_envs:
        if str(environ.get(env_name) or "").strip():
            return True
    return False


def _paid_feed_credential_present(
    environ: Mapping[str, str],
    provider_names: Iterable[str] | None,
) -> bool:
    providers = [str(provider or "").strip().lower() for provider in (provider_names or ())]
    if not providers:
        providers = list(PAID_FEED_CREDENTIAL_SOURCES)

    for provider in providers:
        policy = PAID_FEED_CREDENTIAL_SOURCES.get(provider)
        if policy is None:
            continue
        mode, keys = policy
        configured = [_approved_secret_source_configured(key, environ) for key in keys]
        if mode == "all" and all(configured):
            return True
        if mode != "all" and any(configured):
            return True
    return False


def _paid_feed_credential_errors(
    environ: Mapping[str, str],
    provider_names: Iterable[str] | None,
) -> list[str]:
    if _paid_feed_credential_present(environ, provider_names):
        return []
    return ["no_paid_feed_credential"]


def _offsite_destination_errors(environ: Mapping[str, str]) -> list[str]:
    if str(environ.get("TS_BASE_BACKUP_OFFSITE_CMD") or "").strip():
        return []

    dest = str(environ.get("TS_OFFSITE_BACKUP_DEST") or "").strip()
    if not dest:
        return ["offsite_dest_unreachable:missing"]
    if dest.startswith("s3://"):
        return []
    if not dest.startswith("/"):
        return ["offsite_dest_unreachable:unsupported_destination"]

    path = Path(dest).expanduser()
    try:
        if not path.exists():
            return ["offsite_dest_unreachable:path_missing"]
        if not path.is_dir():
            return ["offsite_dest_unreachable:not_directory"]
        if not os.access(path, os.W_OK):
            return ["offsite_dest_unreachable:not_writable"]
    except OSError as exc:
        return [f"offsite_dest_unreachable:stat_failed:{type(exc).__name__}"]
    return []


def live_host_readiness_errors(
    *,
    require_active: bool = False,
    paid_equity_provider_names: Iterable[str] | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[str]:
    env = os.environ if environ is None else environ
    errors: list[str] = []

    swap_total, swap_error = _swap_total_bytes()
    if swap_error:
        errors.append(f"swap_check_failed:{swap_error}")
    elif swap_total is None or swap_total < MIN_SWAP_BYTES:
        errors.append(f"swap_capacity_below_policy:{swap_total or 0}:expected_at_least={MIN_SWAP_BYTES}")

    if shutil.which("systemctl") is None:
        if require_active:
            errors.append("systemd_unavailable:systemctl_missing")
    else:
        for unit, expected in SYSTEMD_UNITS.items():
            errors.extend(_systemd_unit_errors(unit, expected, require_active=require_active))
        for unit in BACKUP_SYSTEMD_UNITS:
            errors.extend(_backup_systemd_unit_errors(unit, require_active=require_active))

    errors.extend(_backup_key_mode_errors(env))
    errors.extend(_inline_secret_errors(env))
    errors.extend(_paid_feed_credential_errors(env, paid_equity_provider_names))
    errors.extend(_offsite_destination_errors(env))
    return errors


def main() -> int:
    errors = live_host_readiness_errors()
    if errors:
        for error in errors:
            print(error)
        return 1
    print("live_host_readiness_passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
