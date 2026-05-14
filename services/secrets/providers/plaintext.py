"""Plaintext development secret provider."""

from __future__ import annotations

import os
import time
import warnings
from pathlib import Path

from services.secrets.loader import SecretNotAvailable

_PRODUCTION_CHECK_TTL_S = 60.0
_production_check_at = 0.0
_production_forbidden: bool | None = None

warnings.warn(
    "TS_SECRETS_PROVIDER=plaintext reads raw secret files and is for local development only.",
    RuntimeWarning,
    stacklevel=2,
)


def _is_production_env() -> bool:
    return str(os.environ.get("TS_ENV") or "").strip().lower() == "production"


def _plaintext_forbidden_cached() -> bool:
    global _production_check_at, _production_forbidden
    now = time.monotonic()
    if _production_forbidden is None or now - _production_check_at >= _PRODUCTION_CHECK_TTL_S:
        _production_forbidden = _is_production_env()
        _production_check_at = now
    return bool(_production_forbidden)


def _ensure_not_production() -> None:
    if _plaintext_forbidden_cached():
        raise RuntimeError("plaintext_secrets_provider_forbidden_in_production")


if _is_production_env():
    raise RuntimeError("plaintext_secrets_provider_forbidden_in_production")


def _secrets_dir() -> Path:
    configured = str(os.environ.get("TS_DEV_SECRETS_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    local_app_data = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if local_app_data:
        return Path(local_app_data) / "Trading" / "secrets"
    return Path.home() / ".trading" / "secrets"


def load(name: str) -> bytes:
    _ensure_not_production()
    secret_name = str(name or "").strip()
    if not secret_name or secret_name != Path(secret_name).name:
        raise SecretNotAvailable(f"invalid_secret_name:{secret_name}")
    path = _secrets_dir() / secret_name
    try:
        return path.read_bytes()
    except FileNotFoundError as exc:
        raise SecretNotAvailable(f"secret_missing:{name}") from exc
    except OSError as exc:
        raise SecretNotAvailable(f"secret_read_failed:{name}:{type(exc).__name__}:{exc}") from exc


__all__ = ["load"]
