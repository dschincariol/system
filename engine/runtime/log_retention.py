"""Local file-log retention helpers."""

from __future__ import annotations

import os
import sys
from pathlib import Path

DEFAULT_LOCAL_LOG_MAX_BYTES = 50 * 1024 * 1024
DEFAULT_LOCAL_LOG_BACKUP_COUNT = 5
DEFAULT_LOCAL_LOG_MAX_AGE_DAYS = 14


def _env_int(name: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    raw = os.environ.get(name)
    try:
        value = int(float(str(raw if raw is not None else default).strip()))
    except Exception:
        value = int(default)
    value = max(int(minimum), int(value))
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def local_log_max_bytes() -> int:
    return _env_int(
        "TRADING_LOCAL_LOG_MAX_BYTES",
        DEFAULT_LOCAL_LOG_MAX_BYTES,
        minimum=0,
        maximum=1024 * 1024 * 1024 * 1024,
    )


def local_log_backup_count() -> int:
    return _env_int(
        "TRADING_LOCAL_LOG_BACKUP_COUNT",
        DEFAULT_LOCAL_LOG_BACKUP_COUNT,
        minimum=0,
        maximum=1000,
    )


def local_log_max_age_days() -> int:
    return _env_int(
        "TRADING_LOCAL_LOG_MAX_AGE_DAYS",
        DEFAULT_LOCAL_LOG_MAX_AGE_DAYS,
        minimum=0,
        maximum=3650,
    )


def rotate_log_if_needed(
    path: str | os.PathLike[str],
    *,
    max_bytes: int | None = None,
    backup_count: int | None = None,
) -> bool:
    """Rotate ``path`` to ``path.1`` when it exceeds the local size cap.

    The active file is renamed, not truncated. Only numbered backups beyond the
    configured backup count are removed.
    """

    try:
        log_path = Path(path)
        effective_max = local_log_max_bytes() if max_bytes is None else int(max_bytes)
        effective_backups = local_log_backup_count() if backup_count is None else int(backup_count)

        if effective_max <= 0 or effective_backups <= 0:
            return False
        if not log_path.exists() or not log_path.is_file():
            return False
        if log_path.stat().st_size < effective_max:
            return False

        log_path.parent.mkdir(parents=True, exist_ok=True)
        oldest = Path(f"{log_path}.{effective_backups}")
        if oldest.exists():
            oldest.unlink()

        for idx in range(effective_backups - 1, 0, -1):
            src = Path(f"{log_path}.{idx}")
            if src.exists():
                os.replace(src, Path(f"{log_path}.{idx + 1}"))

        os.replace(log_path, Path(f"{log_path}.1"))
        return True
    except Exception as exc:
        try:
            sys.stderr.write(
                f"[log_retention] rotate skipped path={path!r}: {type(exc).__name__}: {exc}\n"
            )
            sys.stderr.flush()
        except Exception:
            return False
        return False
