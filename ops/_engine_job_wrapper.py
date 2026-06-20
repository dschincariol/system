"""Compatibility helpers for ops wrappers around canonical engine jobs."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType
from typing import MutableMapping


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run_engine_module(module_name: str, argv: list[str] | None = None) -> int:
    """Run the canonical engine job as ``python -m`` and return its exit code."""
    root = _repo_root()
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    parts = [str(root), *[part for part in existing.split(os.pathsep) if part]]
    env["PYTHONPATH"] = os.pathsep.join(dict.fromkeys(parts))
    cmd = [sys.executable, "-u", "-m", str(module_name), *(argv if argv is not None else sys.argv[1:])]
    return int(subprocess.call(cmd, cwd=str(root), env=env))


def import_engine_module(module_name: str) -> ModuleType:
    """Import and return a canonical engine job module."""
    return importlib.import_module(str(module_name))


def export_engine_module(module_name: str, namespace: MutableMapping[str, object]) -> ModuleType:
    """Expose canonical module attributes from an ops compatibility module."""
    module = importlib.import_module(str(module_name))
    for name, value in vars(module).items():
        if name.startswith("__") and name.endswith("__"):
            continue
        namespace.setdefault(name, value)
    namespace["__all__"] = [name for name in vars(module) if not (name.startswith("__") and name.endswith("__"))]
    return module
