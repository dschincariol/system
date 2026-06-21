"""Environment parsing helpers for the dashboard boundary."""

from __future__ import annotations


def env_int(key: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    import os

    raw = os.environ.get(key)
    try:
        value = int(float(str(raw if raw is not None else default).strip()))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def env_float(
    key: str,
    default: float,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    import os

    raw = os.environ.get(key)
    try:
        value = float(str(raw if raw is not None else default).strip())
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


def env_bool(key: str, default: bool = False) -> bool:
    import os

    v = os.environ.get(key)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")
