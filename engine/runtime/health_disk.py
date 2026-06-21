"""Disk pressure probes for runtime health."""

from __future__ import annotations

import os
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Callable, Dict, Optional


WarnFn = Callable[..., None]


def nearest_existing_path(path: Path, *, warn: WarnFn) -> Path:
    candidate = path.expanduser()
    for item in (candidate, *candidate.parents):
        try:
            if item.exists():
                return item
        except Exception as exc:
            warn("health.disk_path_exists", exc, path=str(item))
    return Path("/")


def disk_path_snapshot(
    label: str,
    path: Path,
    *,
    warn_free_pct: float,
    critical_free_pct: float,
    warn_free_bytes: int,
    critical_free_bytes: int,
    warn: WarnFn,
    disk_usage: Callable[[str], Any] = shutil.disk_usage,
) -> Dict[str, Any]:
    requested = path.expanduser()
    check_path = nearest_existing_path(requested, warn=warn)
    out: Dict[str, Any] = {
        "label": str(label),
        "path": str(requested),
        "exists": bool(requested.exists()),
        "checked_path": str(check_path),
        "ok": False,
        "status": "unknown",
        "warning": False,
        "critical": False,
        "detail": "",
    }
    try:
        usage = disk_usage(str(check_path))
        total = int(usage.total)
        free = int(usage.free)
        used = int(usage.used)
        free_pct = (float(free) / float(total) * 100.0) if total > 0 else 0.0
        critical = free <= critical_free_bytes or free_pct <= critical_free_pct
        warning = not critical and (free <= warn_free_bytes or free_pct <= warn_free_pct)
        status = "critical" if critical else ("warning" if warning else "ok")
        out.update(
            {
                "ok": not critical,
                "status": status,
                "warning": bool(warning),
                "critical": bool(critical),
                "total_bytes": total,
                "used_bytes": used,
                "free_bytes": free,
                "free_pct": round(free_pct, 2),
                "used_pct": round(100.0 - free_pct, 2),
                "detail": (
                    "ok"
                    if status == "ok"
                    else f"disk_{status}:free_bytes={free}:free_pct={free_pct:.2f}"
                ),
            }
        )
    except Exception as e:
        out.update(
            {
                "ok": False,
                "status": "error",
                "critical": True,
                "detail": f"disk_usage_error:{type(e).__name__}:{e}",
            }
        )
    return out


def default_disk_pressure_paths(
    *,
    environ: os._Environ[str],
    db_path: Path,
    default_log_dir: Callable[[], Path],
    default_backup_dir: Callable[[], Path],
) -> list[tuple[str, Path]]:
    log_root = Path(
        environ.get("TRADING_LOGS")
        or environ.get("LOG_DIR")
        or str(default_log_dir().resolve())
    ).expanduser()
    backup_root = Path(
        environ.get("TRADING_BACKUP_ROOT")
        or environ.get("TS_BACKUP_ROOT")
        or default_backup_dir()
    ).expanduser()
    paths: list[tuple[str, Path]] = [
        ("root", Path("/")),
        ("runtime_data", db_path.expanduser().parent if db_path.suffix else db_path.expanduser()),
        ("runtime_logs", log_root),
        ("backup_root", backup_root),
    ]
    extra_raw = str(environ.get("DISK_PRESSURE_EXTRA_PATHS") or "").strip()
    for idx, raw in enumerate(part.strip() for part in extra_raw.split(",") if part.strip()):
        paths.append((f"extra_{idx}", Path(raw).expanduser()))
    return paths


def disk_pressure_snapshot(
    paths: Optional[Iterable[tuple[str, str | Path]]] = None,
    *,
    default_paths: Callable[[], list[tuple[str, Path]]],
    warn_free_pct: float,
    critical_free_pct: float,
    warn_free_bytes: int,
    critical_free_bytes: int,
    warn: WarnFn,
) -> Dict[str, Any]:
    requested_paths = list(paths) if paths is not None else default_paths()
    snapshots: list[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for label, raw_path in requested_paths:
        path = Path(raw_path).expanduser()
        key = (str(label), str(path))
        if key in seen:
            continue
        seen.add(key)
        snapshots.append(
            disk_path_snapshot(
                str(label),
                path,
                warn_free_pct=warn_free_pct,
                critical_free_pct=critical_free_pct,
                warn_free_bytes=warn_free_bytes,
                critical_free_bytes=critical_free_bytes,
                warn=warn,
            )
        )

    critical = [
        f"{item.get('label')}:{item.get('detail')}"
        for item in snapshots
        if bool(item.get("critical"))
    ]
    warnings = [
        f"{item.get('label')}:{item.get('detail')}"
        for item in snapshots
        if bool(item.get("warning"))
    ]
    return {
        "ok": not critical,
        "degraded": bool(warnings),
        "status": "critical" if critical else ("warning" if warnings else "ok"),
        "warnings": warnings,
        "critical": critical,
        "paths": snapshots,
        "thresholds": {
            "warn_free_pct": warn_free_pct,
            "critical_free_pct": critical_free_pct,
            "warn_free_bytes": warn_free_bytes,
            "critical_free_bytes": critical_free_bytes,
        },
    }
