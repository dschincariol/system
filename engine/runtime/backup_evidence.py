from __future__ import annotations

"""Backup, WAL archive, and restore-drill evidence freshness checks."""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple


DEFAULT_EVIDENCE_PATH = "/var/backups/trading/evidence/latest_backup_restore_evidence.json"
DEFAULT_BASE_BACKUP_MAX_AGE_S = 26 * 60 * 60
DEFAULT_WAL_EVIDENCE_RPO_S = 120
DEFAULT_RESTORE_DRILL_MAX_AGE_S = 90 * 24 * 60 * 60
DEFAULT_RESTORE_RTO_S = 30 * 60

_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSY = {"0", "false", "no", "n", "off"}
_PASS_STATUSES = {"ok", "pass", "passed", "success", "succeeded", "true"}


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


def _env_float(name: str, default: float, errors: list[str]) -> float:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        value = float(raw)
    except Exception:
        errors.append(f"{name}_invalid")
        return float(default)
    if value < 0:
        errors.append(f"{name}_negative")
        return float(default)
    return float(value)


def _first_env_float(names: Iterable[str], default: float, errors: list[str]) -> float:
    for name in names:
        if str(os.environ.get(name, "") or "").strip():
            return _env_float(name, default, errors)
    return float(default)


def _iso_from_ts(ts: float | None) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_ts(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw <= 0:
            return None
        return raw / 1000.0 if raw > 10_000_000_000 else raw
    text = str(value).strip()
    if not text:
        return None
    try:
        raw = float(text)
    except Exception:
        raw = None
    if raw is not None:
        return raw / 1000.0 if raw > 10_000_000_000 else raw
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).timestamp()
    except Exception:
        return None


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _status_passed(value: Any) -> bool:
    return str(value if value is not None else "").strip().lower() in _PASS_STATUSES


def _read_json(path: Path) -> Tuple[Dict[str, Any], str]:
    try:
        if not path.exists():
            return {}, "missing"
        return dict(json.loads(path.read_text(encoding="utf-8")) or {}), "json"
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}, "invalid_json"


def _kv_report(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    try:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if not line or line.startswith("[") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    except Exception:
        return {}
    return values


def _latest_dir(parent: Path) -> Path | None:
    try:
        if (parent / "latest").exists():
            latest = (parent / "latest").resolve()
            if latest.is_dir():
                return latest
        dirs = [
            item
            for item in parent.iterdir()
            if item.is_dir()
            and item.name != "latest"
            and not item.name.startswith(".")
            and not item.name.endswith(".in_progress")
        ]
    except Exception:
        return None
    if not dirs:
        return None
    return max(dirs, key=lambda p: p.stat().st_mtime)


def _latest_file(parent: Path, pattern: str = "*") -> Path | None:
    try:
        files = [
            item
            for item in parent.glob(pattern)
            if item.is_file() and ".tmp" not in item.parts and not item.name.startswith(".")
        ]
    except Exception:
        return None
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def _component_from_json(raw: Mapping[str, Any], key: str) -> Dict[str, Any]:
    value = raw.get(key)
    return dict(value) if isinstance(value, Mapping) else {}


def _component_ts(component: Mapping[str, Any], keys: Iterable[str]) -> float | None:
    for key in keys:
        ts = _parse_ts(component.get(key))
        if ts is not None:
            return ts
    return None


def _base_backup_fallback(base_dir: Path) -> Dict[str, Any]:
    backup_dir = _latest_dir(base_dir)
    if backup_dir is None:
        return {"status": "missing", "source": "filesystem", "base_dir": str(base_dir)}
    manifest = backup_dir / "backup_manifest"
    verify_log = backup_dir / "pg_verifybackup.out"
    passed = manifest.exists() and verify_log.exists() and verify_log.stat().st_size > 0
    ts_candidates = [backup_dir.stat().st_mtime]
    for item in (verify_log, manifest):
        if item.exists():
            ts_candidates.append(item.stat().st_mtime)
    ts = max(ts_candidates)
    return {
        "status": "pass" if passed else "failed",
        "source": "filesystem",
        "backup_dir": str(backup_dir),
        "verify_log": str(verify_log),
        "verified_at_ts": ts,
        "verified_at": _iso_from_ts(ts),
    }


def _wal_archive_fallback(wal_dir: Path) -> Dict[str, Any]:
    wal_file = _latest_file(wal_dir)
    if wal_file is None:
        return {"status": "missing", "source": "filesystem", "wal_dir": str(wal_dir)}
    ts = wal_file.stat().st_mtime
    return {
        "status": "pass" if wal_file.stat().st_size > 0 else "failed",
        "source": "filesystem",
        "wal_dir": str(wal_dir),
        "wal_file": str(wal_file),
        "verified_at_ts": ts,
        "verified_at": _iso_from_ts(ts),
    }


def _restore_drill_fallback(drill_dir: Path) -> Dict[str, Any]:
    report = _latest_file(drill_dir, "restore_drill_*.txt")
    if report is None:
        return {"status": "missing", "source": "filesystem", "drill_dir": str(drill_dir)}
    values = _kv_report(report)
    ts = _parse_ts(values.get("generated_at")) or report.stat().st_mtime
    return {
        "status": values.get("status", "missing"),
        "source": "filesystem",
        "report": str(report),
        "verified_at_ts": ts,
        "verified_at": _iso_from_ts(ts),
        "time_to_recover_s": values.get("time_to_recover_s"),
        "exit_code": values.get("exit_code"),
    }


def _normalize_component(
    *,
    raw: Mapping[str, Any],
    key: str,
    fallback: Dict[str, Any],
    timestamp_keys: Iterable[str],
) -> Dict[str, Any]:
    component = _component_from_json(raw, key)
    if not component:
        component = dict(fallback)
    ts = _component_ts(component, timestamp_keys)
    if ts is not None:
        component["verified_at_ts"] = ts
        component["verified_at"] = _iso_from_ts(ts)
    component.setdefault("status", "missing")
    return component


def _policy() -> Tuple[Dict[str, float], list[str]]:
    errors: list[str] = []
    policy = {
        "base_backup_max_age_s": _first_env_float(
            ("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S", "BACKUP_MAX_AGE_S"),
            DEFAULT_BASE_BACKUP_MAX_AGE_S,
            errors,
        ),
        "wal_archive_max_age_s": _first_env_float(
            ("BACKUP_EVIDENCE_WAL_RPO_S", "BACKUP_EVIDENCE_RPO_S", "BACKUP_RPO_S"),
            DEFAULT_WAL_EVIDENCE_RPO_S,
            errors,
        ),
        "restore_drill_max_age_s": _first_env_float(
            ("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S", "RESTORE_DRILL_MAX_AGE_S"),
            DEFAULT_RESTORE_DRILL_MAX_AGE_S,
            errors,
        ),
        "restore_rto_s": _first_env_float(
            ("BACKUP_EVIDENCE_RESTORE_RTO_S", "BACKUP_EVIDENCE_RTO_S", "RESTORE_RTO_S"),
            DEFAULT_RESTORE_RTO_S,
            errors,
        ),
    }
    return policy, errors


def _assess_age(
    blockers: list[str],
    component: Mapping[str, Any],
    *,
    name: str,
    max_age_s: float,
    now_ts: float,
) -> float | None:
    status = str(component.get("status") or "missing").strip().lower()
    ts = _parse_ts(component.get("verified_at_ts")) or _parse_ts(component.get("verified_at"))
    if not status or status == "missing" or ts is None:
        blockers.append(f"backup_evidence_{name}_missing")
        return None
    if not _status_passed(status):
        blockers.append(f"backup_evidence_{name}_failed")
    age_s = max(0.0, float(now_ts) - float(ts))
    if age_s > float(max_age_s):
        blockers.append(f"backup_evidence_{name}_stale")
    return age_s


def backup_restore_evidence_snapshot(
    *,
    engine_mode: str | None = None,
    required: bool | None = None,
    now_ts: float | None = None,
) -> Dict[str, Any]:
    """Return backup evidence readiness with fail-closed blockers when required."""

    mode = str(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE", "safe")).strip().lower()
    required_flag = bool(mode == "live") if required is None else bool(required)
    if _env_bool("PREFLIGHT_REQUIRE_BACKUP_EVIDENCE", False):
        required_flag = True
    now = float(time.time() if now_ts is None else now_ts)
    evidence_path = Path(os.environ.get("BACKUP_EVIDENCE_PATH") or DEFAULT_EVIDENCE_PATH)
    base_dir = Path(os.environ.get("TS_BACKUP_BASE_DIR") or "/var/backups/trading/base")
    wal_dir = Path(os.environ.get("TS_BACKUP_WAL_DIR") or "/var/backups/trading/wal")
    drill_dir = Path(os.environ.get("TS_RESTORE_DRILL_DIR") or "/var/backups/trading/drills")

    raw, raw_source = _read_json(evidence_path)
    policy, policy_errors = _policy()
    blockers = list(policy_errors)
    warnings: list[str] = []
    if raw_source == "invalid_json":
        blockers.append("backup_evidence_json_invalid")
    elif raw_source == "missing":
        warnings.append("backup_evidence_json_missing")
    if raw_source == "json":
        report_status = str(raw.get("status") or "").strip()
        if report_status and not _status_passed(report_status):
            blockers.append("backup_evidence_report_failed")
        for check_key in ("script_checks", "systemd_checks"):
            check = raw.get(check_key)
            if isinstance(check, Mapping):
                check_status = str(check.get("status") or "").strip()
                if check_status and not _status_passed(check_status):
                    blockers.append(f"backup_evidence_{check_key}_failed")

    base_backup = _normalize_component(
        raw=raw,
        key="base_backup",
        fallback=_base_backup_fallback(base_dir),
        timestamp_keys=("verified_at_ts", "completed_at_ts", "generated_at_ts", "verified_at", "completed_at"),
    )
    wal_archive = _normalize_component(
        raw=raw,
        key="wal_archive",
        fallback=_wal_archive_fallback(wal_dir),
        timestamp_keys=("verified_at_ts", "archived_at_ts", "generated_at_ts", "verified_at", "archived_at"),
    )
    restore_drill = _normalize_component(
        raw=raw,
        key="restore_drill",
        fallback=_restore_drill_fallback(drill_dir),
        timestamp_keys=("verified_at_ts", "completed_at_ts", "generated_at_ts", "verified_at", "generated_at"),
    )

    base_age = _assess_age(
        blockers,
        base_backup,
        name="base_backup",
        max_age_s=policy["base_backup_max_age_s"],
        now_ts=now,
    )
    wal_age = _assess_age(
        blockers,
        wal_archive,
        name="wal_archive",
        max_age_s=policy["wal_archive_max_age_s"],
        now_ts=now,
    )
    drill_age = _assess_age(
        blockers,
        restore_drill,
        name="restore_drill",
        max_age_s=policy["restore_drill_max_age_s"],
        now_ts=now,
    )

    restore_elapsed = _safe_float(restore_drill.get("time_to_recover_s"))
    if restore_elapsed is None:
        blockers.append("backup_evidence_restore_rto_missing")
    elif restore_elapsed > float(policy["restore_rto_s"]):
        blockers.append("backup_evidence_restore_rto_exceeded")

    fresh = not blockers
    return {
        "ok": bool(fresh if required_flag else True),
        "fresh": bool(fresh),
        "required": bool(required_flag),
        "reason": "ok" if fresh else blockers[0],
        "blockers": blockers,
        "warnings": warnings,
        "mode": mode,
        "evidence_path": str(evidence_path),
        "evidence_source": raw_source,
        "generated_at": raw.get("generated_at") if isinstance(raw, Mapping) else None,
        "policy": policy,
        "base_backup": dict(base_backup, age_s=base_age),
        "wal_archive": dict(wal_archive, age_s=wal_age),
        "restore_drill": dict(restore_drill, age_s=drill_age, time_to_recover_s=restore_elapsed),
    }


__all__ = [
    "DEFAULT_BASE_BACKUP_MAX_AGE_S",
    "DEFAULT_EVIDENCE_PATH",
    "DEFAULT_RESTORE_DRILL_MAX_AGE_S",
    "DEFAULT_RESTORE_RTO_S",
    "DEFAULT_WAL_EVIDENCE_RPO_S",
    "backup_restore_evidence_snapshot",
]
