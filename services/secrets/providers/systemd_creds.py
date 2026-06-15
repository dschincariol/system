"""Linux systemd-creds provider."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from services.secrets.loader import SecretNotAvailable, validate_secret_name


def _credential_path(name: str) -> Path:
    if not sys.platform.startswith("linux"):
        raise SecretNotAvailable("systemd_creds_linux_only")
    directory = str(os.environ.get("CREDENTIALS_DIRECTORY") or "").strip()
    if not directory:
        raise SecretNotAvailable("credentials_directory_missing")
    secret_name = validate_secret_name(name)
    return Path(directory) / secret_name


def load(name: str) -> bytes:
    path = _credential_path(name)
    try:
        return path.read_bytes()
    except FileNotFoundError as exc:
        raise SecretNotAvailable(f"secret_missing:{name}") from exc
    except OSError as exc:
        raise SecretNotAvailable(f"secret_read_failed:{name}:{type(exc).__name__}:{exc}") from exc


def delete(name: str) -> bool:
    path = _credential_path(name)
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise SecretNotAvailable(f"secret_delete_failed:{name}:{type(exc).__name__}:{exc}") from exc


__all__ = ["delete", "load"]
