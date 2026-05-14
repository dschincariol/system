from __future__ import annotations
# engine/runtime/run_root_script.py
"""
Run legacy root-level scripts via engine/* wrappers (migration-safe).

Purpose:
- Preserve behavior (no rewrites)
- Stop supervisor from launching root scripts directly
- Keep root scripts present until final cleanup
"""

"""
FILE: run_root_script.py

Runtime subsystem module for `run_root_script`.
"""

import runpy
from pathlib import Path


def repo_root() -> Path:
    # .../engine/runtime/run_root_script.py -> parents[2] == repo root
    return Path(__file__).resolve().parents[2]


def resolve_root_script(script_rel_path: str) -> Path:
    root = repo_root()
    rel = str(script_rel_path or "").replace("\\", "/").lstrip("./")

    # Search known legacy locations so engine/* wrapper jobs can preserve old
    # entrypoint behavior while the repo is migrated toward canonical modules.
    candidates = [
        root / rel,
        root / "ops" / rel,
        root / "engine" / "strategy" / rel,
        root / "engine" / "execution" / rel,
        root / "engine" / "data" / rel,
        root / "engine" / "jobs" / rel,
        root / "engine" / "runtime" / rel,
        root / "engine" / "data" / "jobs" / rel,
        root / "engine" / "strategy" / "jobs" / rel,
        root / "engine" / "execution" / "jobs" / rel,
        root / "engine" / "runtime" / "jobs" / rel,
    ]

    seen = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        if resolved.exists() and resolved.is_file():
            return resolved

    raise FileNotFoundError(
        f"Could not resolve legacy script path: {script_rel_path}. "
        f"Checked: {', '.join(sorted(seen))}"
    )


def run_root_script(script_rel_path: str) -> None:
    p = resolve_root_script(script_rel_path)
    # runpy preserves script semantics for modules that still expect to execute
    # as __main__ rather than exposing a clean function entrypoint yet.
    runpy.run_path(str(p), run_name="__main__")
