"""Early test-process defaults.

Python imports ``sitecustomize`` before test modules are collected.  Keep the
settings here tightly scoped to pytest/unittest so normal application launches
continue to use the production Postgres/secrets/metrics defaults.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _truthy_env(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _early_supervised_thread_policy_enabled() -> bool:
    return bool(
        _truthy_env("ENGINE_SUPERVISED")
        or _truthy_env("ENGINE_LAUNCHED_BY_SUPERVISOR")
        or _truthy_env("TRADING_CPU_THREAD_POLICY_EARLY")
    )


def _running_python_tests() -> bool:
    argv = " ".join(str(part or "") for part in sys.argv).lower()
    executable = Path(str(sys.argv[0] or "")).name.lower()
    return bool(
        "pytest" in argv
        or "unittest" in argv
        or "discover" in argv
        or "tests/" in argv
        or " tests" in argv
        or executable.startswith("pytest")
    )


if os.environ.get("COVERAGE_PROCESS_START") and not _running_python_tests():
    try:
        import coverage

        coverage.process_startup()
    except Exception:
        # Coverage startup must never change application startup semantics.
        pass

if _running_python_tests():
    try:
        from engine.runtime.test_isolation import apply_runtime_test_defaults, reset_runtime_test_env

        apply_runtime_test_defaults()
        reset_runtime_test_env()
    except Exception:
        os.environ.setdefault("TS_TESTING", "1")
        os.environ.setdefault("TS_STORAGE_BACKEND", "sqlite")
        os.environ.setdefault("TS_CREDENTIAL_AUDIT_ENABLED", "0")
        os.environ.setdefault("TS_PG_POOL_TIMEOUT", "0.1")
        os.environ.setdefault("TS_PG_CONNECT_TIMEOUT", "1")
        os.environ.setdefault("TS_CREDENTIAL_AUDIT_TIMEOUT_S", "0.05")

if _early_supervised_thread_policy_enabled():
    try:
        from engine.runtime.thread_policy import apply_cpu_thread_policy_to_env

        apply_cpu_thread_policy_to_env(os.environ)
    except Exception:
        # sitecustomize must never make Python startup fail. Entry-point
        # bootstrap applies the same policy again and can report diagnostics.
        pass
