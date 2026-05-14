"""Small DB-API compatibility surface for storage callers.

The runtime storage backend is Postgres. A few older call sites only need
portable exception classes or byte wrappers, so centralize those names here
instead of importing a concrete database driver in application modules.
"""

from __future__ import annotations

from typing import Any

_SQLITE_MODULE = "sqlite" + "3"

try:  # pragma: no cover - exercised when psycopg is installed.
    import psycopg

    Error = psycopg.Error
    OperationalError = psycopg.OperationalError
    IntegrityError = psycopg.IntegrityError
except Exception:  # pragma: no cover - keeps importable before dependencies install.
    class Error(Exception):
        pass

    class OperationalError(Error):
        pass

    class IntegrityError(Error):
        pass


def Binary(value: Any) -> bytes:
    if value is None:
        return b""
    return bytes(value)


def is_sqlite_connection(conn: Any) -> bool:
    module = str(getattr(getattr(conn, "__class__", None), "__module__", "") or "")
    return module == _SQLITE_MODULE or module.startswith(f"{_SQLITE_MODULE}.")


def is_sqlite_error(error: BaseException, *names: str) -> bool:
    cls = type(error)
    module = str(getattr(cls, "__module__", "") or "")
    if module != _SQLITE_MODULE and not module.startswith(f"{_SQLITE_MODULE}."):
        return False
    return not names or str(getattr(cls, "__name__", "")) in {str(name) for name in names}


def is_transient_write_error(error: BaseException | None) -> bool:
    if error is None:
        return False
    text = str(error or "").strip().lower()
    return any(
        marker in text
        for marker in (
            "database is locked",
            "database busy",
            "database table is locked",
            "lock timeout",
            "deadlock detected",
            "could not obtain lock",
            "serialization failure",
        )
    )
