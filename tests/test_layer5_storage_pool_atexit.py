"""Layer 5 negative test: storage pool cleanup is registered with atexit.

The Postgres connection pool is created globally at module load.
Without an atexit hook, worker processes hit by SIGTERM leak
connections — backends do not close cleanly, file descriptors leak,
and idle Postgres backends accumulate.

This test scans `engine/runtime/storage_pool.py` for the
`atexit.register(close_pooled_connections)` line at module scope
(an AST scan) and additionally captures the registration at runtime
by patching `atexit.register` before re-importing the module.

Two checks are intentional: the AST scan is stable and unambiguous;
the runtime check confirms the line is actually executed at module
load (not gated behind an `if False:` or similar).
"""

from __future__ import annotations

import ast
import atexit as _atexit_mod
import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
STORAGE_POOL = ROOT / "engine" / "runtime" / "storage_pool.py"


def test_storage_pool_source_has_module_level_atexit_register() -> None:
    """AST check: the module body contains
    `atexit.register(close_pooled_connections)` at top level."""
    source = STORAGE_POOL.read_text(encoding="utf-8")
    tree = ast.parse(source)

    found = False
    for node in tree.body:
        if not isinstance(node, ast.Expr):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        call = node.value
        # Match `atexit.register(...)` calls.
        if not isinstance(call.func, ast.Attribute):
            continue
        if call.func.attr != "register":
            continue
        if not isinstance(call.func.value, ast.Name) or call.func.value.id != "atexit":
            continue
        if not call.args:
            continue
        first_arg = call.args[0]
        if isinstance(first_arg, ast.Name) and first_arg.id == "close_pooled_connections":
            found = True
            break

    assert found, (
        "engine/runtime/storage_pool.py does not contain a top-level "
        "`atexit.register(close_pooled_connections)` call. Without it, "
        "process exit (SIGTERM, unhandled exception in workers) will "
        "leak Postgres connections and file descriptors."
    )


def test_atexit_register_is_called_during_module_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """Runtime check: importing the module triggers the
    atexit.register call. Patching atexit.register and re-importing
    confirms the registration runs (i.e., is not gated behind an
    `if False:` or wrapped in a `try` that silently swallows)."""
    captured: list = []

    def _capturing_register(func, *args, **kwargs):
        captured.append(func)
        # Still call the real register so process shutdown is
        # unaffected by the test.
        return _real_register(func, *args, **kwargs)

    _real_register = _atexit_mod.register
    monkeypatch.setattr(_atexit_mod, "register", _capturing_register)

    # Drop any cached module so the re-import re-runs module body.
    runtime_pkg = importlib.import_module("engine.runtime")
    old_module = sys.modules.get("engine.runtime.storage_pool")
    old_attr = getattr(runtime_pkg, "storage_pool", None)
    sys.modules.pop("engine.runtime.storage_pool", None)
    try:
        storage_pool = importlib.import_module("engine.runtime.storage_pool")
    finally:
        if old_module is not None:
            sys.modules["engine.runtime.storage_pool"] = old_module
            setattr(runtime_pkg, "storage_pool", old_module)
        elif old_attr is not None:
            setattr(runtime_pkg, "storage_pool", old_attr)

    target = storage_pool.close_pooled_connections
    assert target in captured, (
        "Importing engine.runtime.storage_pool did NOT call "
        "atexit.register(close_pooled_connections) — the registration "
        "is either guarded behind a runtime condition or never runs. "
        "Inspect the module body for an unconditional "
        "`atexit.register(close_pooled_connections)` at module scope."
    )


def test_close_pooled_connections_is_idempotent() -> None:
    """Calling the cleanup twice must be safe; both shutdown.py paths
    and the atexit handler may invoke it."""
    from engine.runtime import storage_pool

    storage_pool.close_pooled_connections()
    storage_pool.close_pooled_connections()
