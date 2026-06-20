"""Pluggable secret loader with best-effort credential access auditing."""

from __future__ import annotations

import importlib
import logging
import os
import socket
import sys
import threading
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Callable

import psycopg

from engine.runtime.test_isolation import apply_runtime_test_defaults, running_python_tests
from engine.runtime.metrics import emit_counter
from engine.runtime.observability import record_component_health

apply_runtime_test_defaults()

LOG = logging.getLogger(__name__)

_PROVIDER_MODULES = {
    "systemd-creds": "services.secrets.providers.systemd_creds",
    "systemd_creds": "services.secrets.providers.systemd_creds",
    "plaintext": "services.secrets.providers.plaintext",
}
_AUDIT_LOCAL = threading.local()


class SecretNotAvailable(RuntimeError):
    """Raised when a named secret cannot be loaded from the selected provider."""


def validate_secret_name(name: str) -> str:
    """Return a normalized secret name or raise when it could escape a provider root."""
    secret_name = str(name or "").strip()
    if not secret_name:
        raise SecretNotAvailable("secret_name_required")

    posix_path = PurePosixPath(secret_name)
    windows_path = PureWindowsPath(secret_name)
    path_parts = tuple(posix_path.parts) + tuple(windows_path.parts)
    if (
        "/" in secret_name
        or "\\" in secret_name
        or ":" in secret_name
        or secret_name in {".", ".."}
        or posix_path.is_absolute()
        or windows_path.is_absolute()
        or windows_path.drive
        or windows_path.root
        or any(part in {".", ".."} for part in path_parts)
    ):
        raise SecretNotAvailable(f"invalid_secret_name:{secret_name}")
    return secret_name


def _default_provider_name() -> str:
    return "systemd-creds"


def selected_provider_name() -> str:
    return str(os.environ.get("TS_SECRETS_PROVIDER") or _default_provider_name()).strip().lower()


def _credential_audit_enabled() -> bool:
    configured = os.environ.get("TS_CREDENTIAL_AUDIT_ENABLED")
    if configured is None and running_python_tests():
        return False
    return str(configured if configured is not None else "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _provider_module(provider_name: str):
    module_name = _PROVIDER_MODULES.get(str(provider_name or "").strip().lower())
    if not module_name:
        raise SecretNotAvailable(f"unknown_secrets_provider:{provider_name}")
    try:
        return importlib.import_module(module_name)
    except RuntimeError:
        raise
    except Exception as exc:
        raise SecretNotAvailable(
            f"secrets_provider_unavailable:{provider_name}:{type(exc).__name__}:{exc}"
        ) from exc


def _provider_loader(provider_name: str) -> Callable[[str], bytes]:
    module = _provider_module(provider_name)
    load = getattr(module, "load", None)
    if not callable(load):
        raise SecretNotAvailable(f"secrets_provider_missing_load:{provider_name}")
    return load


def _provider_deleter(provider_name: str) -> Callable[[str], bool]:
    module = _provider_module(provider_name)
    delete = getattr(module, "delete", None)
    if not callable(delete):
        raise SecretNotAvailable(f"secrets_provider_missing_delete:{provider_name}")
    return delete


def _service_name() -> str:
    for env_name in ("TS_SERVICE_NAME", "SYSTEMD_UNIT", "ENGINE_JOB_NAME", "JOB_NAME"):
        value = str(os.environ.get(env_name) or "").strip()
        if value:
            return value[:200]
    argv0 = str(sys.argv[0] or "").strip()
    return (Path(argv0).name or "unknown")[:200]


def _insert_access_log(
    *,
    name: str,
    provider: str,
    ok: bool,
    error: str = "",
) -> None:
    timeout_s = float(os.environ.get("TS_CREDENTIAL_AUDIT_TIMEOUT_S", "0.25") or 0.25)
    import psycopg
    from engine.runtime.platform import default_pg_dsn, dsn_with_pg_password

    configured_dsn = str(os.environ.get("TS_PG_DSN") or "").strip()
    try:
        conninfo = dsn_with_pg_password(configured_dsn) if configured_dsn else default_pg_dsn()
    except SecretNotAvailable as exc:
        raise OSError(f"credential_access_log_pg_password_unavailable:{exc}") from exc
    with psycopg.connect(conninfo, connect_timeout=max(1, int(timeout_s))) as con:
        con.execute("SET search_path = trading, public")
        con.execute(
            """
            INSERT INTO credential_access_log(name, pid, service_name, host, provider, ok, error)
            VALUES(%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                str(name),
                int(os.getpid()),
                _service_name(),
                socket.gethostname(),
                str(provider),
                bool(ok),
                str(error or "")[:1000] or None,
            ),
        )
        con.commit()


def _record_access(*, name: str, provider: str, ok: bool, error: str = "") -> None:
    if not _credential_audit_enabled():
        return
    if bool(getattr(_AUDIT_LOCAL, "active", False)):
        return
    _AUDIT_LOCAL.active = True
    try:
        _insert_access_log(name=name, provider=provider, ok=ok, error=error)
    except (psycopg.Error, OSError) as exc:
        reason = f"{type(exc).__name__}: {exc}"
        record_component_health(
            "credential_access_log",
            ok=False,
            status="degraded",
            detail=reason,
            extra={
                "reason": reason,
                "secret_name": str(name),
                "provider": str(provider),
                "access_ok": bool(ok),
            },
        )
        emit_counter(
            "credential_access_log_write_failures",
            1,
            component="services.secrets.loader",
            extra_tags={
                "provider": str(provider),
                "ok": bool(ok),
                "error_class": type(exc).__name__,
            },
        )
        LOG.warning(
            "credential_access_log_write_failed name=%s provider=%s ok=%s error=%s",
            str(name),
            str(provider),
            bool(ok),
            reason,
        )
    finally:
        _AUDIT_LOCAL.active = False


def load_secret(name: str) -> bytes:
    """Load a named secret as bytes from the configured provider.

    Callers must consume the returned bytes immediately and avoid retaining
    plaintext secret material longer than the operation requires.
    """
    secret_name = validate_secret_name(name)
    provider = selected_provider_name()
    try:
        data = _provider_loader(provider)(secret_name)
        if not isinstance(data, (bytes, bytearray)):
            raise SecretNotAvailable(f"secret_provider_returned_non_bytes:{secret_name}")
        out = bytes(data)
        if not out:
            raise SecretNotAvailable(f"secret_empty:{secret_name}")
        _record_access(name=secret_name, provider=provider, ok=True)
        return out
    except SecretNotAvailable as exc:
        _record_access(name=secret_name, provider=provider, ok=False, error=str(exc))
        raise
    except RuntimeError as exc:
        _record_access(name=secret_name, provider=provider, ok=False, error=str(exc))
        raise
    except Exception as exc:
        wrapped = SecretNotAvailable(f"secret_unavailable:{secret_name}:{type(exc).__name__}:{exc}")
        _record_access(name=secret_name, provider=provider, ok=False, error=str(wrapped))
        raise wrapped from exc


def delete_secret(name: str) -> bool:
    """Delete a named secret from the configured provider.

    This is intended for post-rotation cleanup after all data has been
    verified under the replacement key. Providers return ``False`` when the
    named secret is already absent.
    """
    secret_name = validate_secret_name(name)
    provider = selected_provider_name()
    try:
        deleted = bool(_provider_deleter(provider)(secret_name))
        _record_access(name=secret_name, provider=provider, ok=True)
        return deleted
    except SecretNotAvailable as exc:
        _record_access(name=secret_name, provider=provider, ok=False, error=str(exc))
        raise
    except RuntimeError as exc:
        _record_access(name=secret_name, provider=provider, ok=False, error=str(exc))
        raise
    except Exception as exc:
        wrapped = SecretNotAvailable(f"secret_delete_unavailable:{secret_name}:{type(exc).__name__}:{exc}")
        _record_access(name=secret_name, provider=provider, ok=False, error=str(wrapped))
        raise wrapped from exc


__all__ = [
    "SecretNotAvailable",
    "delete_secret",
    "load_secret",
    "selected_provider_name",
    "validate_secret_name",
]
