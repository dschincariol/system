"""Plaintext development secret provider."""

from __future__ import annotations

import os
import time
import warnings
from pathlib import Path

from services.secrets.loader import SecretNotAvailable, validate_secret_name

_PRODUCTION_CHECK_TTL_S = 60.0
_production_check_at = 0.0
_production_forbidden: bool | None = None
_production_check_signature: tuple[str, ...] | None = None

warnings.warn(
    "TS_SECRETS_PROVIDER=plaintext reads raw secret files and is for local development only.",
    RuntimeWarning,
    stacklevel=2,
)


def _is_production_env() -> bool:
    try:
        from engine.runtime.secret_sources import _policy_suppressed_for_tests, strict_secret_source_policy_required

        return bool(strict_secret_source_policy_required() and not _policy_suppressed_for_tests(os.environ))
    except ImportError:
        truthy = {"1", "true", "yes", "on", "y"}
        if str(os.environ.get("PROD_LOCK") or "").strip().lower() in truthy:
            return True
        if str(os.environ.get("ENGINE_SUPERVISED") or "").strip().lower() in truthy:
            return True
        for name in ("ENV", "APP_ENV", "TS_ENV", "NODE_ENV"):
            if str(os.environ.get(name) or "").strip().lower() in {"prod", "production"}:
                return True
        for name in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
            if str(os.environ.get(name) or "").strip().lower() == "live":
                return True
        return False


def _production_env_signature() -> tuple[str, ...]:
    keys = (
        "PROD_LOCK",
        "ENGINE_SUPERVISED",
        "ENV",
        "APP_ENV",
        "TS_ENV",
        "NODE_ENV",
        "ENGINE_MODE",
        "EXECUTION_MODE",
        "OPERATOR_MODE",
        "TRADING_ENFORCE_SECRET_SOURCE_POLICY",
        "PYTEST_CURRENT_TEST",
    )
    return tuple(str(os.environ.get(key) or "") for key in keys)


def _plaintext_forbidden_cached() -> bool:
    global _production_check_at, _production_forbidden, _production_check_signature
    now = time.monotonic()
    signature = _production_env_signature()
    if (
        _production_forbidden is None
        or signature != _production_check_signature
        or now - _production_check_at >= _PRODUCTION_CHECK_TTL_S
    ):
        _production_forbidden = _is_production_env()
        _production_check_signature = signature
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
    return Path.home() / ".trading" / "secrets"


def _secret_path(name: str) -> Path:
    secret_name = validate_secret_name(name)
    return _secrets_dir() / secret_name


def load(name: str) -> bytes:
    _ensure_not_production()
    path = _secret_path(name)
    try:
        return path.read_bytes()
    except FileNotFoundError as exc:
        raise SecretNotAvailable(f"secret_missing:{name}") from exc
    except OSError as exc:
        raise SecretNotAvailable(f"secret_read_failed:{name}:{type(exc).__name__}:{exc}") from exc


def delete(name: str) -> bool:
    _ensure_not_production()
    path = _secret_path(name)
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise SecretNotAvailable(f"secret_delete_failed:{name}:{type(exc).__name__}:{exc}") from exc


__all__ = ["delete", "load"]
