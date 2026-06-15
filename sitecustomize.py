"""Early test-process defaults.

Python imports ``sitecustomize`` before test modules are collected.  Keep the
settings here tightly scoped to pytest/unittest so normal application launches
continue to use the production Postgres/secrets/metrics defaults.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


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
