"""Startup launch-mode selection helpers."""

from collections.abc import Mapping, Sequence


def pick_mode_from_argv_or_env(argv: Sequence[str], environ: Mapping[str, str]) -> str:
    """Select startup mode with argv taking precedence over ENGINE_MODE."""
    if len(argv) >= 2 and str(argv[1] or "").strip():
        mode = str(argv[1]).strip().lower()
    else:
        mode = str(environ.get("ENGINE_MODE", "") or "").strip().lower() or "safe"

    allowed = {"safe", "shadow", "live"}
    if mode not in allowed:
        raise RuntimeError(f"invalid ENGINE_MODE: {mode}")

    return mode
