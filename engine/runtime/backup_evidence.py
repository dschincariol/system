from __future__ import annotations

"""Backup, WAL archive, and restore-drill evidence freshness checks."""

import json
import hashlib
import hmac
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Tuple

from engine.runtime.platform import (
    default_backup_evidence_path,
    default_backup_root_dir,
    default_base_backup_dir,
    default_restore_drill_dir,
    default_wal_backup_dir,
)

DEFAULT_EVIDENCE_PATH = default_backup_evidence_path()
DEFAULT_BASE_BACKUP_MAX_AGE_S = 26 * 60 * 60
DEFAULT_WAL_EVIDENCE_RPO_S = 120
DEFAULT_RESTORE_DRILL_MAX_AGE_S = 90 * 24 * 60 * 60
DEFAULT_RESTORE_RTO_S = 30 * 60
DEFAULT_SIGNATURE_MAX_AGE_S = DEFAULT_WAL_EVIDENCE_RPO_S
DEFAULT_WAL_ARCHIVE_COMMAND_FRAGMENT = "wal_archive.sh"
BYTES_IN_GIB = 1024 * 1024 * 1024

_TRUTHY = {"1", "true", "yes", "y", "on"}
_FALSY = {"0", "false", "no", "n", "off"}
_PASS_STATUSES = {"ok", "pass", "passed", "success", "succeeded", "true"}
_SIGNATURE_ALGORITHMS = {"hmac-sha256", "sha256-hmac"}
_SIZE_RE = re.compile(r"^\s*(?P<count>\d+(?:\.\d+)?)\s*(?P<unit>[kmgt]?i?b?|bytes?)?\s*$", re.IGNORECASE)


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


def _first_env_bool(names: Iterable[str], default: bool) -> bool:
    for name in names:
        raw = str(os.environ.get(name, "") or "").strip().lower()
        if not raw:
            continue
        if raw in _TRUTHY:
            return True
        if raw in _FALSY:
            return False
        return bool(default)
    return bool(default)


def _parse_size_bytes(value: Any, default: int) -> int:
    text = str(value if value is not None else "").strip()
    if not text:
        return int(default)
    match = _SIZE_RE.match(text)
    if not match:
        return int(default)
    count = float(match.group("count") or 0.0)
    unit = str(match.group("unit") or "b").strip().lower()
    factors = {
        "": 1,
        "b": 1,
        "byte": 1,
        "bytes": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "gib": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
        "tib": 1024**4,
    }
    return int(count * factors.get(unit, 1))


def _first_env_size_bytes(names: Iterable[str], default: int) -> int:
    for name in names:
        raw = str(os.environ.get(name, "") or "").strip()
        if raw:
            return _parse_size_bytes(raw, default)
    return int(default)


def _first_env_int(names: Iterable[str], default: int) -> int:
    for name in names:
        raw = str(os.environ.get(name, "") or "").strip()
        if not raw:
            continue
        try:
            return max(0, int(raw))
        except Exception:
            return int(default)
    return int(default)


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


def _archive_mode_enabled(value: Any) -> bool:
    return str(value if value is not None else "").strip().lower() in {"on", "always", "true", "1"}


def _expected_wal_archive_command_fragment() -> str:
    return str(
        os.environ.get("BACKUP_EVIDENCE_WAL_ARCHIVE_COMMAND_FRAGMENT")
        or os.environ.get("TIMESCALE_ARCHIVE_COMMAND_REQUIRED_FRAGMENT")
        or DEFAULT_WAL_ARCHIVE_COMMAND_FRAGMENT
    ).strip()


def _archive_command_audited(command: Any) -> bool:
    text = str(command or "").strip()
    if not text:
        return False
    required_fragment = _expected_wal_archive_command_fragment()
    if required_fragment and required_fragment not in text:
        return False
    if "%p" not in text or "%f" not in text:
        return False
    inline_fragments = (" cp %p ", " cp \"%p\" ", "mkdir -p ", "test ! -f ")
    padded = f" {text} "
    return not any(fragment in padded for fragment in inline_fragments)


def _read_json(path: Path) -> Tuple[Dict[str, Any], str]:
    try:
        if not path.exists():
            return {}, "missing"
        return dict(json.loads(path.read_text(encoding="utf-8")) or {}), "json"
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}, "invalid_json"


def _canonical_payload_bytes(raw: Mapping[str, Any]) -> bytes:
    payload = dict(raw)
    payload.pop("signature", None)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _signature_input_bytes(
    payload_bytes: bytes,
    *,
    algorithm: str,
    key_id: str,
    signed_at: str,
    payload_sha256: str,
) -> bytes:
    metadata_bytes = json.dumps(
        {
            "algorithm": str(algorithm),
            "key_id": str(key_id),
            "payload_sha256": str(payload_sha256),
            "signed_at": str(signed_at),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return payload_bytes + b"\n" + metadata_bytes


def _signature_key() -> Tuple[bytes | None, str]:
    for name in ("BACKUP_EVIDENCE_HMAC_KEY", "BACKUP_EVIDENCE_SIGNING_KEY"):
        raw = str(os.environ.get(name, "") or "")
        if raw.strip():
            return raw.encode("utf-8"), f"env:{name}"
    for name in ("BACKUP_EVIDENCE_HMAC_KEY_FILE", "BACKUP_EVIDENCE_SIGNING_KEY_FILE"):
        raw_path = str(os.environ.get(name, "") or "").strip()
        if not raw_path:
            continue
        try:
            raw = Path(raw_path).read_text(encoding="utf-8").strip()
        except Exception:
            return None, f"unreadable:{name}"
        if raw:
            return raw.encode("utf-8"), f"file:{name}"
        return None, f"empty:{name}"
    return None, "missing"


def _signature_snapshot(
    raw: Mapping[str, Any],
    *,
    raw_source: str,
    required: bool,
    max_age_s: float,
    now_ts: float,
) -> Dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    state: Dict[str, Any] = {
        "required": bool(required),
        "status": "not_required",
        "algorithm": "",
        "key_id": "",
        "signed_at": "",
        "signed_at_ts": None,
        "age_s": None,
        "payload_sha256": "",
    }
    if raw_source != "json":
        state["status"] = "unavailable" if required else "not_checked"
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state

    signature = raw.get("signature")
    if not isinstance(signature, Mapping):
        state["status"] = "unsigned"
        if required:
            blockers.append("backup_evidence_unsigned")
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state

    state.update(
        {
            "status": str(signature.get("status") or "signed").strip().lower() or "signed",
            "algorithm": str(signature.get("algorithm") or "").strip().lower(),
            "key_id": str(signature.get("key_id") or "").strip(),
            "signed_at": str(signature.get("signed_at") or "").strip(),
            "payload_sha256": str(signature.get("payload_sha256") or "").strip().lower(),
        }
    )
    value = str(signature.get("value") or signature.get("signature") or "").strip().lower()
    if state["status"] == "unsigned" or not value:
        if state["status"] in {"key_missing", "key_unreadable", "key_empty"}:
            if state["status"] == "key_unreadable":
                blocker = "backup_evidence_signature_key_unreadable"
            elif state["status"] == "key_empty":
                blocker = "backup_evidence_signature_key_empty"
            else:
                blocker = "backup_evidence_signature_key_missing"
            if required:
                blockers.append(blocker)
            else:
                warnings.append(blocker)
        else:
            state["status"] = "unsigned"
            if required:
                blockers.append("backup_evidence_unsigned")
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state

    if state["algorithm"] not in _SIGNATURE_ALGORITHMS:
        state["status"] = "invalid"
        if required:
            blockers.append("backup_evidence_signature_invalid")
        else:
            warnings.append("backup_evidence_signature_invalid")
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state
    signed_at_ts = _parse_ts(state["signed_at"])
    state["signed_at_ts"] = signed_at_ts
    if (
        not state["key_id"]
        or signed_at_ts is None
        or not state["payload_sha256"]
        or len(str(state["payload_sha256"])) != 64
    ):
        state["status"] = "invalid"
        if required:
            blockers.append("backup_evidence_signature_malformed")
        else:
            warnings.append("backup_evidence_signature_malformed")
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state
    if state["payload_sha256"]:
        try:
            int(str(state["payload_sha256"]), 16)
        except Exception:
            state["status"] = "invalid"
            if required:
                blockers.append("backup_evidence_signature_malformed")
            else:
                warnings.append("backup_evidence_signature_malformed")
            state["blockers"] = blockers
            state["warnings"] = warnings
            return state
    if signed_at_ts is not None:
        age_s = max(0.0, float(now_ts) - float(signed_at_ts))
        state["age_s"] = age_s
        if age_s > float(max_age_s):
            state["status"] = "stale"
            if required:
                blockers.append("backup_evidence_signature_stale")
            else:
                warnings.append("backup_evidence_signature_stale")
            state["blockers"] = blockers
            state["warnings"] = warnings
            return state

    key, key_source = _signature_key()
    state["key_source"] = key_source
    if key is None:
        if str(key_source).startswith("unreadable:"):
            state["status"] = "key_unreadable"
            blocker = "backup_evidence_signature_key_unreadable"
        elif str(key_source).startswith("empty:"):
            state["status"] = "key_empty"
            blocker = "backup_evidence_signature_key_empty"
        else:
            state["status"] = "unverified"
            blocker = "backup_evidence_signature_key_missing"
        if required:
            blockers.append(blocker)
        else:
            warnings.append(blocker)
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state

    payload_bytes = _canonical_payload_bytes(raw)
    payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
    if state["payload_sha256"] and state["payload_sha256"] != payload_sha256:
        state["status"] = "invalid"
        if required:
            blockers.append("backup_evidence_signature_invalid")
        else:
            warnings.append("backup_evidence_signature_invalid")
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state

    signature_input = _signature_input_bytes(
        payload_bytes,
        algorithm=str(state["algorithm"]),
        key_id=str(state["key_id"]),
        signed_at=str(state["signed_at"]),
        payload_sha256=str(state["payload_sha256"]),
    )
    expected = hmac.new(key, signature_input, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(value, expected):
        state["status"] = "invalid"
        if required:
            blockers.append("backup_evidence_signature_invalid")
        else:
            warnings.append("backup_evidence_signature_invalid")
        state["blockers"] = blockers
        state["warnings"] = warnings
        return state

    state["status"] = "verified"
    state["payload_sha256"] = payload_sha256
    state["blockers"] = blockers
    state["warnings"] = warnings
    return state


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


def _backup_root_from_env(base_dir: Path) -> Path:
    raw = str(
        os.environ.get("TRADING_BACKUP_ROOT")
        or os.environ.get("TS_BACKUP_ROOT")
        or ""
    ).strip()
    if raw:
        return Path(raw).expanduser()
    try:
        return base_dir.parent
    except Exception:
        return Path(default_backup_root_dir()).expanduser()


def _mountinfo_for(path: Path) -> Dict[str, Any]:
    target = path.expanduser()
    try:
        target_resolved = target.resolve(strict=False)
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return {"path": str(target), "available": False}

    best: Dict[str, Any] = {}
    best_len = -1
    for line in lines:
        left, sep, right = line.partition(" - ")
        if not sep:
            continue
        left_fields = left.split()
        right_fields = right.split()
        if len(left_fields) < 5 or len(right_fields) < 3:
            continue
        mount_point = left_fields[4].replace("\\040", " ")
        try:
            mount_path = Path(mount_point).resolve(strict=False)
        except Exception:
            mount_path = Path(mount_point)
        try:
            target_resolved.relative_to(mount_path)
        except Exception:
            continue
        mount_len = len(str(mount_path))
        if mount_len <= best_len:
            continue
        best_len = mount_len
        best = {
            "available": True,
            "mount_point": mount_point,
            "mount_root": left_fields[3].replace("\\040", " "),
            "device": left_fields[2],
            "filesystem_type": right_fields[0],
            "mount_source": right_fields[1].replace("\\040", " "),
            "super_options": right_fields[2],
        }
    if not best:
        return {"path": str(target), "available": False}
    best["path"] = str(target)
    return best


def _du_size(path: Path, *flags: str, timeout_s: float) -> Tuple[int | None, str]:
    try:
        proc = subprocess.run(
            ["du", *flags, str(path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=max(0.1, float(timeout_s)),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, "timeout"
    except Exception as exc:
        return None, f"{type(exc).__name__}:{exc}"
    if int(proc.returncode) != 0:
        return None, (proc.stderr or proc.stdout or f"rc={proc.returncode}").strip()[:240]
    first = (proc.stdout or "").strip().splitlines()[0] if (proc.stdout or "").strip() else ""
    try:
        return int(first.split()[0]), "ok"
    except Exception:
        return None, "parse_failed"


def _path_size_snapshot(path: Path, *, timeout_s: float) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "path": str(path),
        "exists": bool(path.exists()),
        "apparent_bytes": None,
        "allocated_bytes": None,
        "status": "missing" if not path.exists() else "unknown",
    }
    if not path.exists():
        return out
    apparent, apparent_status = _du_size(path, "-sb", timeout_s=timeout_s)
    allocated, allocated_status = _du_size(path, "-sB1", timeout_s=timeout_s)
    out.update(
        {
            "apparent_bytes": apparent,
            "allocated_bytes": allocated,
            "status": "ok" if apparent_status == "ok" and allocated_status == "ok" else "partial",
            "apparent_status": apparent_status,
            "allocated_status": allocated_status,
        }
    )
    return out


def _backup_dir_inventory(base_dir: Path, wal_dir: Path) -> Dict[str, Any]:
    inventory: Dict[str, Any] = {
        "base_backup_count": 0,
        "in_progress_count": 0,
        "latest_base_backup": "",
        "oldest_base_backup": "",
        "wal_file_count": 0,
        "latest_wal_file": "",
    }
    try:
        base_dirs = [
            item
            for item in base_dir.iterdir()
            if item.is_dir() and item.name != "latest" and not item.name.startswith(".")
        ]
        complete = [item for item in base_dirs if not item.name.endswith(".in_progress")]
        in_progress = [item for item in base_dirs if item.name.endswith(".in_progress")]
        inventory["base_backup_count"] = len(complete)
        inventory["in_progress_count"] = len(in_progress)
        if complete:
            latest = max(complete, key=lambda p: p.stat().st_mtime)
            oldest = min(complete, key=lambda p: p.stat().st_mtime)
            inventory["latest_base_backup"] = str(latest)
            inventory["oldest_base_backup"] = str(oldest)
    except Exception as exc:
        inventory["base_error"] = f"{type(exc).__name__}:{exc}"
    try:
        wal_files = [item for item in wal_dir.iterdir() if item.is_file() and not item.name.startswith(".")]
        inventory["wal_file_count"] = len(wal_files)
        if wal_files:
            inventory["latest_wal_file"] = str(max(wal_files, key=lambda p: p.stat().st_mtime))
    except Exception as exc:
        inventory["wal_error"] = f"{type(exc).__name__}:{exc}"
    return inventory


def _env_int_for_accounting(name: str, default: int, warnings: list[str]) -> int:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        value = int(float(raw))
    except Exception:
        warnings.append(f"backup_accounting_{name.lower()}_invalid")
        return int(default)
    if value < 0:
        warnings.append(f"backup_accounting_{name.lower()}_negative")
        return int(default)
    return int(value)


def _backup_retention_status(retention: Mapping[str, Any], warnings: list[str]) -> str:
    keep_daily = int(retention.get("keep_daily_days") or 0)
    keep_weekly = int(retention.get("keep_weekly_days") or 0)
    wal_cushion = int(retention.get("wal_cushion_days") or 0)
    if keep_daily <= 0 and keep_weekly <= 0:
        warnings.append("backup_accounting_retention_disabled")
        return "disabled"
    if keep_weekly > 0 and keep_weekly < keep_daily:
        warnings.append("backup_accounting_retention_review")
        return "review"
    if wal_cushion < 0:
        warnings.append("backup_accounting_wal_cushion_invalid")
        return "review"
    return "configured"


def backup_accounting_snapshot(*, timeout_s: float | None = None) -> Dict[str, Any]:
    """Return operator-facing size and retention accounting for the backup root."""

    timeout = float(timeout_s if timeout_s is not None else os.environ.get("BACKUP_ACCOUNTING_DU_TIMEOUT_S", "8"))
    base_dir = Path(os.environ.get("TS_BACKUP_BASE_DIR") or default_base_backup_dir()).expanduser()
    wal_dir = Path(os.environ.get("TS_BACKUP_WAL_DIR") or default_wal_backup_dir()).expanduser()
    drill_dir = Path(os.environ.get("TS_RESTORE_DRILL_DIR") or default_restore_drill_dir()).expanduser()
    backup_root = _backup_root_from_env(base_dir)
    subdirs = {
        "base": base_dir,
        "wal": wal_dir,
        "drills": drill_dir,
        "evidence": backup_root / "evidence",
        "state": backup_root / "state",
        "artifacts": backup_root / "artifacts",
    }
    warnings: list[str] = []
    root_size = _path_size_snapshot(backup_root, timeout_s=timeout)
    if root_size.get("status") != "ok":
        warnings.append(f"backup_accounting_root_size_{root_size.get('status')}")
    subdir_sizes = {name: _path_size_snapshot(path, timeout_s=timeout) for name, path in subdirs.items()}
    for name, snap in subdir_sizes.items():
        if snap.get("exists") and snap.get("status") != "ok":
            warnings.append(f"backup_accounting_{name}_size_{snap.get('status')}")

    filesystem: Dict[str, Any] = {"path": str(backup_root), "exists": bool(backup_root.exists())}
    if backup_root.exists():
        try:
            usage = shutil.disk_usage(str(backup_root))
            filesystem.update(
                {
                    "total_bytes": int(usage.total),
                    "used_bytes": int(usage.used),
                    "free_bytes": int(usage.free),
                    "free_pct": round((float(usage.free) / float(usage.total) * 100.0) if usage.total else 0.0, 2),
                }
            )
        except Exception as exc:
            warnings.append(f"backup_accounting_filesystem_error:{type(exc).__name__}")
            filesystem["error"] = f"{type(exc).__name__}:{exc}"
    else:
        warnings.append("backup_accounting_root_missing")

    retention = {
        "keep_daily_days": _env_int_for_accounting("TS_BACKUP_KEEP_DAILY_DAYS", 14, warnings),
        "keep_weekly_days": _env_int_for_accounting("TS_BACKUP_KEEP_WEEKLY_DAYS", 365, warnings),
        "wal_cushion_days": _env_int_for_accounting("TS_BACKUP_WAL_CUSHION_DAYS", 7, warnings),
        "prune_script": "ops/backup/prune.sh",
    }
    retention_status = _backup_retention_status(retention, warnings)
    retention["status"] = retention_status
    container_path = Path(os.environ.get("BACKUP_ACCOUNTING_CONTAINER_PATH") or default_backup_root_dir()).expanduser()
    container_mount = _mountinfo_for(backup_root)
    container_mount_source = str(container_mount.get("mount_source") or "").strip()
    return {
        "ok": bool(backup_root.exists() and root_size.get("status") in {"ok", "partial"}),
        "host_path": str(backup_root),
        "container_path": str(container_path),
        "container_mount_source": container_mount_source,
        "container_mount": container_mount,
        "filesystem": filesystem,
        "root_size": root_size,
        "subdir_sizes": subdir_sizes,
        "inventory": _backup_dir_inventory(base_dir, wal_dir),
        "retention_status": retention_status,
        "retention": retention,
        "warnings": warnings,
    }


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
    allow_fallback: bool = True,
) -> Dict[str, Any]:
    component = _component_from_json(raw, key)
    if not component and allow_fallback:
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
    policy["signature_max_age_s"] = _first_env_float(
        ("BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S",),
        policy["wal_archive_max_age_s"],
        errors,
    )
    return policy, errors


def _wal_archiver_component_age(component: Mapping[str, Any], now_ts: float) -> float | None:
    ts = (
        _parse_ts(component.get("last_archived_at_ts"))
        or _parse_ts(component.get("last_archived_ts"))
        or _parse_ts(component.get("verified_at_ts"))
        or _parse_ts(component.get("last_archived_at"))
        or _parse_ts(component.get("verified_at"))
    )
    if ts is None:
        return None
    return max(0.0, float(now_ts) - float(ts))


def _assess_wal_archiver_component(
    blockers: list[str],
    component: Mapping[str, Any],
    *,
    prefix: str,
    max_age_s: float,
    now_ts: float,
) -> float | None:
    status = str(component.get("status") or "missing").strip().lower()
    age_s = _wal_archiver_component_age(component, now_ts)
    if not status or status == "missing":
        blockers.append(f"{prefix}_missing")
        return age_s
    if not _status_passed(status):
        if status == "stale":
            blockers.append(f"{prefix}_stale")
        elif status == "timeout":
            blockers.append(f"{prefix}_timeout")
        else:
            blockers.append(f"{prefix}_failed")
        if age_s is None:
            return age_s
    elif age_s is None:
        blockers.append(f"{prefix}_missing")
        return age_s
    if not _archive_mode_enabled(component.get("archive_mode")):
        blockers.append(f"{prefix}_archive_mode_disabled")
    if not _archive_command_audited(component.get("archive_command")):
        blockers.append(f"{prefix}_archive_command_unaudited")
    last_archived_ts = (
        _parse_ts(component.get("last_archived_at_ts"))
        or _parse_ts(component.get("last_archived_ts"))
        or _parse_ts(component.get("last_archived_at"))
    )
    last_failed_ts = (
        _parse_ts(component.get("last_failed_at_ts"))
        or _parse_ts(component.get("last_failed_ts"))
        or _parse_ts(component.get("last_failed_at"))
    )
    if last_failed_ts is not None and (last_archived_ts is None or last_failed_ts > last_archived_ts):
        blockers.append(f"{prefix}_failure_unrecovered")
    if age_s > float(max_age_s) and f"{prefix}_stale" not in blockers:
        blockers.append(f"{prefix}_stale")
    return age_s


def _runtime_wal_archiver_required(engine_mode: str | None, required: bool | None) -> bool:
    if required is not None:
        return bool(required)
    mode = str(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE", "safe")).strip().lower()
    return bool(
        mode == "live"
        or _env_bool("PREFLIGHT_REQUIRE_BACKUP_EVIDENCE", False)
        or _env_bool("PREFLIGHT_REQUIRE_WAL_ARCHIVER_STATS", False)
    )


def wal_archiver_runtime_snapshot(
    *,
    engine_mode: str | None = None,
    required: bool | None = None,
    now_ts: float | None = None,
) -> Dict[str, Any]:
    """Return read-only pg_stat_archiver readiness without forcing a WAL switch."""

    mode = str(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE", "safe")).strip().lower()
    required_flag = _runtime_wal_archiver_required(mode, required)
    now = float(time.time() if now_ts is None else now_ts)
    policy, policy_errors = _policy()
    blockers = list(policy_errors)
    warnings: list[str] = []
    if (
        not required_flag
        and str(os.environ.get("PREFLIGHT_QUERY_WAL_ARCHIVER_STATS", "0")).strip().lower()
        not in {"1", "true", "yes", "on"}
    ):
        return {
            "ok": True,
            "required": False,
            "mode": mode,
            "reason": "skipped",
            "blockers": [],
            "warnings": [],
            "skipped": True,
            "source": "pg_stat_archiver",
        }

    try:
        import math

        import psycopg

        from engine.runtime.platform import default_pg_dsn, dsn_with_pg_password

        configured = str(os.environ.get("TS_PG_DSN") or "").strip()
        conninfo = dsn_with_pg_password(configured or default_pg_dsn())
        timeout_s = 2.0
        try:
            timeout_s = max(
                0.1,
                float(
                    os.environ.get("PREFLIGHT_WAL_ARCHIVER_STATS_TIMEOUT_S")
                    or os.environ.get("PREFLIGHT_EXTERNAL_TIMEOUT_S")
                    or "2.0"
                ),
            )
        except Exception:
            timeout_s = 2.0
        connect_timeout = max(1, int(math.ceil(timeout_s)))
        with psycopg.connect(conninfo, autocommit=True, connect_timeout=connect_timeout) as con:
            row = con.execute(
                """
                SELECT
                  current_setting('archive_mode', true) AS archive_mode,
                  current_setting('archive_command', true) AS archive_command,
                  archived_count::bigint AS archived_count,
                  COALESCE(last_archived_wal, '') AS last_archived_wal,
                  EXTRACT(EPOCH FROM last_archived_time) AS last_archived_at_ts,
                  failed_count::bigint AS failed_count,
                  COALESCE(last_failed_wal, '') AS last_failed_wal,
                  EXTRACT(EPOCH FROM last_failed_time) AS last_failed_at_ts,
                  EXTRACT(EPOCH FROM stats_reset) AS stats_reset_ts
                FROM pg_stat_archiver
                """
            ).fetchone()
    except Exception as exc:
        blockers.append("wal_archiver_stats_unavailable")
        return {
            "ok": False if required_flag else True,
            "required": bool(required_flag),
            "mode": mode,
            "reason": blockers[0],
            "blockers": blockers if required_flag else [],
            "warnings": warnings + ([f"wal_archiver_stats_unavailable:{type(exc).__name__}: {exc}"] if not required_flag else []),
            "source": "pg_stat_archiver",
            "error": f"{type(exc).__name__}: {exc}",
        }

    if not row:
        blockers.append("wal_archiver_stats_missing")
        return {
            "ok": False if required_flag else True,
            "required": bool(required_flag),
            "mode": mode,
            "reason": blockers[0],
            "blockers": blockers if required_flag else [],
            "warnings": warnings + ([] if required_flag else ["wal_archiver_stats_missing"]),
            "source": "pg_stat_archiver",
        }

    last_archived_ts = _safe_float(row[4])
    last_failed_ts = _safe_float(row[7])
    stats_reset_ts = _safe_float(row[8])
    component: Dict[str, Any] = {
        "status": "pass",
        "source": "pg_stat_archiver",
        "archive_mode": str(row[0] or ""),
        "archive_command": str(row[1] or ""),
        "archived_count": int(row[2] or 0),
        "last_archived_wal": str(row[3] or ""),
        "last_archived_at_ts": last_archived_ts,
        "last_archived_at": _iso_from_ts(last_archived_ts),
        "failed_count": int(row[5] or 0),
        "last_failed_wal": str(row[6] or ""),
        "last_failed_at_ts": last_failed_ts,
        "last_failed_at": _iso_from_ts(last_failed_ts),
        "stats_reset_ts": stats_reset_ts,
        "stats_reset": _iso_from_ts(stats_reset_ts),
    }
    if int(component["archived_count"]) <= 0 or last_archived_ts is None:
        component["status"] = "failed"
        blockers.append("wal_archiver_last_archive_missing")
    age_s = _assess_wal_archiver_component(
        blockers,
        component,
        prefix="wal_archiver",
        max_age_s=policy["wal_archive_max_age_s"],
        now_ts=now,
    )
    if not required_flag:
        warnings.extend(blockers)
        blockers = []
    fresh = not blockers
    return {
        "ok": bool(fresh),
        "required": bool(required_flag),
        "mode": mode,
        "reason": "ok" if fresh else blockers[0],
        "blockers": blockers,
        "warnings": warnings,
        "source": "pg_stat_archiver",
        "policy": policy,
        "age_s": age_s,
        **component,
    }


def _runtime_pg_wal_risk_required(engine_mode: str | None, required: bool | None) -> bool:
    if required is not None:
        return bool(required)
    mode = str(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE", "safe")).strip().lower()
    return bool(
        mode == "live"
        or _env_bool("PREFLIGHT_REQUIRE_BACKUP_EVIDENCE", False)
        or _env_bool("PREFLIGHT_REQUIRE_PG_WAL_RISK", False)
    )


def _pg_wal_risk_policy() -> Dict[str, Any]:
    critical_bytes = _first_env_size_bytes(
        (
            "PREFLIGHT_PG_WAL_CRITICAL_BYTES",
            "PREFLIGHT_PG_WAL_MAX_BYTES",
            "TIMESCALE_WAL_DISK_BUDGET",
        ),
        40 * BYTES_IN_GIB,
    )
    warning_bytes = _first_env_size_bytes(
        ("PREFLIGHT_PG_WAL_WARN_BYTES",),
        max(1, int(float(critical_bytes) * 0.75)),
    )
    critical_free_bytes = _first_env_size_bytes(
        ("PREFLIGHT_PG_WAL_CRITICAL_FREE_BYTES", "DISK_PRESSURE_CRITICAL_FREE_BYTES"),
        5 * BYTES_IN_GIB,
    )
    warning_free_bytes = _first_env_size_bytes(
        ("PREFLIGHT_PG_WAL_WARN_FREE_BYTES", "DISK_PRESSURE_WARN_FREE_BYTES"),
        20 * BYTES_IN_GIB,
    )
    return {
        "warning_bytes": int(warning_bytes),
        "critical_bytes": int(critical_bytes),
        "warning_free_bytes": int(warning_free_bytes),
        "critical_free_bytes": int(critical_free_bytes),
        "warning_ready_count": _first_env_int(("PREFLIGHT_PG_WAL_READY_WARN_COUNT",), 4),
        "critical_ready_count": _first_env_int(("PREFLIGHT_PG_WAL_READY_CRITICAL_COUNT",), 16),
    }


def _local_pg_wal_path() -> Path:
    raw = str(os.environ.get("TS_PG_WAL_DIR") or "").strip()
    if raw:
        return Path(raw).expanduser()
    pgdata = str(os.environ.get("PGDATA") or "").strip()
    if pgdata:
        return Path(pgdata).expanduser() / "pg_wal"
    return Path(os.sep).joinpath("var", "lib", "postgresql", "data", "pg_wal")


def _pg_wal_local_space(path: Path, policy: Mapping[str, Any]) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "path": str(path),
        "visible": False,
        "free_bytes": None,
        "total_bytes": None,
        "used_bytes": None,
        "status": "not_visible",
    }
    try:
        if not path.exists():
            return state
        usage = shutil.disk_usage(str(path))
    except Exception as exc:
        state["status"] = "error"
        state["error"] = f"{type(exc).__name__}: {exc}"
        return state
    free_bytes = int(usage.free)
    state.update(
        {
            "visible": True,
            "free_bytes": free_bytes,
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
            "status": "ok",
        }
    )
    if free_bytes <= int(policy.get("critical_free_bytes") or 0):
        state["status"] = "critical"
    elif free_bytes <= int(policy.get("warning_free_bytes") or 0):
        state["status"] = "warning"
    return state


def pg_wal_disk_risk_snapshot(
    *,
    engine_mode: str | None = None,
    required: bool | None = None,
) -> Dict[str, Any]:
    """Return pg_wal growth/backlog/free-space risk without mutating WAL state."""

    mode = str(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE", "safe")).strip().lower()
    required_flag = _runtime_pg_wal_risk_required(mode, required)
    if (
        not required_flag
        and str(os.environ.get("PREFLIGHT_QUERY_PG_WAL_RISK", "0")).strip().lower()
        not in {"1", "true", "yes", "on"}
    ):
        return {
            "ok": True,
            "required": False,
            "mode": mode,
            "reason": "skipped",
            "blockers": [],
            "warnings": [],
            "skipped": True,
            "source": "pg_ls_waldir",
        }

    policy = _pg_wal_risk_policy()
    blockers: list[str] = []
    warnings: list[str] = []
    try:
        import math

        import psycopg

        from engine.runtime.platform import default_pg_dsn, dsn_with_pg_password

        configured = str(os.environ.get("TS_PG_DSN") or "").strip()
        conninfo = dsn_with_pg_password(configured or default_pg_dsn())
        timeout_s = 2.0
        try:
            timeout_s = max(
                0.1,
                float(
                    os.environ.get("PREFLIGHT_PG_WAL_RISK_TIMEOUT_S")
                    or os.environ.get("PREFLIGHT_EXTERNAL_TIMEOUT_S")
                    or "2.0"
                ),
            )
        except Exception:
            timeout_s = 2.0
        connect_timeout = max(1, int(math.ceil(timeout_s)))
        with psycopg.connect(conninfo, autocommit=True, connect_timeout=connect_timeout) as con:
            wal_row = con.execute(
                """
                SELECT
                  COALESCE(SUM(size), 0)::bigint AS wal_bytes,
                  COUNT(*)::bigint AS wal_files
                FROM pg_ls_waldir()
                WHERE name ~ '^[0-9A-F]{24}(\\.[A-Za-z0-9]+)?$'
                """
            ).fetchone()
            try:
                ready_row = con.execute(
                    """
                    SELECT COUNT(*)::bigint AS ready_count
                    FROM pg_ls_dir('pg_wal/archive_status') AS status(name)
                    WHERE name LIKE '%.ready'
                    """
                ).fetchone()
                ready_count = int((ready_row[0] if ready_row else 0) or 0)
            except Exception as exc:
                ready_count = 0
                warnings.append(f"pg_wal_archive_status_unavailable:{type(exc).__name__}: {exc}")
    except Exception as exc:
        blockers.append("pg_wal_stats_unavailable")
        return {
            "ok": False if required_flag else True,
            "required": bool(required_flag),
            "mode": mode,
            "reason": blockers[0],
            "blockers": blockers if required_flag else [],
            "warnings": warnings + ([] if required_flag else [f"pg_wal_stats_unavailable:{type(exc).__name__}: {exc}"]),
            "source": "pg_ls_waldir",
            "policy": policy,
            "error": f"{type(exc).__name__}: {exc}",
        }

    wal_bytes = int((wal_row[0] if wal_row else 0) or 0)
    wal_files = int((wal_row[1] if wal_row else 0) or 0)
    if wal_bytes >= int(policy["critical_bytes"]):
        blockers.append("pg_wal_bytes_exceeds_budget")
    elif wal_bytes >= int(policy["warning_bytes"]):
        warnings.append("pg_wal_bytes_near_budget")
    if ready_count >= int(policy["critical_ready_count"]):
        blockers.append("pg_wal_ready_backlog_critical")
    elif ready_count >= int(policy["warning_ready_count"]):
        warnings.append("pg_wal_ready_backlog_warning")

    local_space = _pg_wal_local_space(_local_pg_wal_path(), policy)
    if local_space.get("status") == "critical":
        blockers.append("pg_wal_free_space_critical")
    elif local_space.get("status") == "warning":
        warnings.append("pg_wal_free_space_warning")
    elif not bool(local_space.get("visible")):
        warnings.append("pg_wal_free_space_not_visible")

    if not required_flag:
        warnings.extend(blockers)
        blockers = []
    return {
        "ok": not blockers,
        "required": bool(required_flag),
        "mode": mode,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "warnings": warnings,
        "source": "pg_ls_waldir",
        "policy": policy,
        "wal_bytes": wal_bytes,
        "wal_files": wal_files,
        "ready_count": ready_count,
        "local_space": local_space,
    }


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
    if not status or status == "missing":
        blockers.append(f"backup_evidence_{name}_missing")
        return None
    if not _status_passed(status):
        if status == "stale":
            blockers.append(f"backup_evidence_{name}_stale")
        elif status == "timeout":
            blockers.append(f"backup_evidence_{name}_timeout")
        else:
            blockers.append(f"backup_evidence_{name}_failed")
        if ts is None:
            return None
    elif ts is None:
        blockers.append(f"backup_evidence_{name}_missing")
        return None
    age_s = max(0.0, float(now_ts) - float(ts))
    if age_s > float(max_age_s) and f"backup_evidence_{name}_stale" not in blockers:
        blockers.append(f"backup_evidence_{name}_stale")
    return age_s


def _text_present(value: Any) -> bool:
    return bool(str(value if value is not None else "").strip())


def _assess_required_json_shape(
    blockers: list[str],
    *,
    raw: Mapping[str, Any],
    base_backup: Mapping[str, Any],
    wal_archive: Mapping[str, Any],
    wal_archiver: Mapping[str, Any],
    restore_drill: Mapping[str, Any],
) -> None:
    schema_version = raw.get("schema_version")
    if schema_version not in {1, "1"}:
        blockers.append("backup_evidence_schema_version_invalid")
    if _parse_ts(raw.get("generated_at_ts")) is None and _parse_ts(raw.get("generated_at")) is None:
        blockers.append("backup_evidence_generated_at_missing")

    if _status_passed(base_backup.get("status")):
        if not _text_present(base_backup.get("backup_dir")):
            blockers.append("backup_evidence_base_backup_dir_missing")
        if not _text_present(base_backup.get("verify_log")):
            blockers.append("backup_evidence_base_verify_log_missing")

    if _status_passed(wal_archive.get("status")) and not _text_present(wal_archive.get("wal_file")):
        blockers.append("backup_evidence_wal_file_missing")

    if _status_passed(wal_archiver.get("status")):
        archived_count = _safe_float(wal_archiver.get("archived_count"))
        if archived_count is None or archived_count <= 0:
            blockers.append("backup_evidence_wal_archiver_count_missing")
        if not _text_present(wal_archiver.get("last_archived_wal")):
            blockers.append("backup_evidence_wal_archiver_last_wal_missing")

    if _status_passed(restore_drill.get("status")) and not _text_present(restore_drill.get("report")):
        blockers.append("backup_evidence_restore_drill_report_missing")


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
    base_dir = Path(os.environ.get("TS_BACKUP_BASE_DIR") or default_base_backup_dir())
    wal_dir = Path(os.environ.get("TS_BACKUP_WAL_DIR") or default_wal_backup_dir())
    drill_dir = Path(os.environ.get("TS_RESTORE_DRILL_DIR") or default_restore_drill_dir())

    raw, raw_source = _read_json(evidence_path)
    policy, policy_errors = _policy()
    signature_required = required_flag or _first_env_bool(
        ("BACKUP_EVIDENCE_REQUIRE_SIGNATURE", "BACKUP_EVIDENCE_SIGNATURE_REQUIRED"),
        False,
    )
    signature = _signature_snapshot(
        raw,
        raw_source=raw_source,
        required=signature_required,
        max_age_s=policy["signature_max_age_s"],
        now_ts=now,
    )
    blockers = list(policy_errors)
    warnings: list[str] = [str(item) for item in list(signature.get("warnings") or [])]
    if raw_source == "invalid_json":
        blockers.append("backup_evidence_json_invalid")
    elif raw_source == "missing":
        warnings.append("backup_evidence_json_missing")
    blockers.extend(str(item) for item in list(signature.get("blockers") or []))
    if raw_source == "json":
        report_status = str(raw.get("status") or "").strip()
        if not report_status:
            blockers.append("backup_evidence_report_status_missing")
        elif not _status_passed(report_status):
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
        allow_fallback=raw_source != "json",
    )
    wal_archive = _normalize_component(
        raw=raw,
        key="wal_archive",
        fallback=_wal_archive_fallback(wal_dir),
        timestamp_keys=("verified_at_ts", "archived_at_ts", "generated_at_ts", "verified_at", "archived_at"),
        allow_fallback=raw_source != "json",
    )
    wal_archiver = _normalize_component(
        raw=raw,
        key="wal_archiver",
        fallback={"status": "missing", "source": "pg_stat_archiver"},
        timestamp_keys=(
            "last_archived_at_ts",
            "last_archived_ts",
            "verified_at_ts",
            "last_archived_at",
            "verified_at",
        ),
        allow_fallback=False,
    )
    restore_drill = _normalize_component(
        raw=raw,
        key="restore_drill",
        fallback=_restore_drill_fallback(drill_dir),
        timestamp_keys=("verified_at_ts", "completed_at_ts", "generated_at_ts", "verified_at", "generated_at"),
        allow_fallback=raw_source != "json",
    )

    if raw_source == "json":
        _assess_required_json_shape(
            blockers,
            raw=raw,
            base_backup=base_backup,
            wal_archive=wal_archive,
            wal_archiver=wal_archiver,
            restore_drill=restore_drill,
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
    wal_archiver_age = _assess_wal_archiver_component(
        blockers,
        wal_archiver,
        prefix="backup_evidence_wal_archiver",
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
    if required_flag and raw_source == "missing":
        blockers.append("backup_evidence_json_missing")

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
        "signature": {
            key: value
            for key, value in dict(signature or {}).items()
            if key not in {"warnings", "blockers"}
        },
        "base_backup": dict(base_backup, age_s=base_age),
        "wal_archive": dict(wal_archive, age_s=wal_age),
        "wal_archiver": dict(wal_archiver, age_s=wal_archiver_age),
        "restore_drill": dict(restore_drill, age_s=drill_age, time_to_recover_s=restore_elapsed),
    }


__all__ = [
    "DEFAULT_BASE_BACKUP_MAX_AGE_S",
    "DEFAULT_EVIDENCE_PATH",
    "DEFAULT_RESTORE_DRILL_MAX_AGE_S",
    "DEFAULT_RESTORE_RTO_S",
    "DEFAULT_SIGNATURE_MAX_AGE_S",
    "DEFAULT_WAL_EVIDENCE_RPO_S",
    "DEFAULT_WAL_ARCHIVE_COMMAND_FRAGMENT",
    "backup_accounting_snapshot",
    "backup_restore_evidence_snapshot",
    "pg_wal_disk_risk_snapshot",
    "wal_archiver_runtime_snapshot",
]
