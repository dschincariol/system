"""Shared API mutation authentication configuration checks."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


TRUTHY_VALUES = {"1", "true", "yes", "y", "on"}
PRODUCTION_ENV_VALUES = {"prod", "production"}
SAFE_DEV_ENV_VALUES = {"dev", "development", "test", "local"}
SAFE_DEV_ENGINE_MODES = {"safe", "dev", "development"}
SAFE_DEV_EXECUTION_MODES = {"safe", "dev", "development"}
PLACEHOLDER_DASHBOARD_API_TOKENS = {
    "change-me",
    "changeme",
    "change_me",
    "replace-me",
    "replace_me",
    "default",
    "token",
    "secret",
    "password",
    "test-token",
    "dev-token",
}
DEFAULT_PRODUCTION_TOKEN_MIN_LENGTH = 16


def _env(environ: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
    return environ if environ is not None else os.environ


def env_text(
    name: str,
    default: str = "",
    *,
    environ: Mapping[str, Any] | None = None,
) -> str:
    return str(_env(environ).get(name, default) or "").strip()


def env_flag(name: str, *, environ: Mapping[str, Any] | None = None) -> str:
    return env_text(name, environ=environ).lower()


def env_truthy(
    name: str,
    default: bool = False,
    *,
    environ: Mapping[str, Any] | None = None,
) -> bool:
    raw = env_text(name, environ=environ)
    if raw == "":
        return bool(default)
    return raw.lower() in TRUTHY_VALUES


def _env_int(
    name: str,
    default: int,
    *,
    minimum: int = 1,
    environ: Mapping[str, Any] | None = None,
) -> int:
    try:
        value = int(env_text(name, environ=environ))
    except Exception:
        return int(default)
    return max(int(minimum), int(value))


def strict_mutation_auth_reasons(
    *, environ: Mapping[str, Any] | None = None
) -> tuple[str, ...]:
    env = _env(environ)
    reasons: list[str] = []

    for key in ("TS_ENV", "ENV", "NODE_ENV", "APP_ENV"):
        value = str(env.get(key, "") or "").strip().lower()
        if value in PRODUCTION_ENV_VALUES:
            reasons.append(f"{key.lower()}={value}")

    if env_truthy("PROD_LOCK", environ=env):
        reasons.append("prod_lock=1")

    for key in ("ENGINE_MODE", "EXECUTION_MODE"):
        value = str(env.get(key, "") or "").strip().lower()
        if value == "live":
            reasons.append(f"{key.lower()}=live")

    return tuple(reasons)


def is_placeholder_dashboard_api_token(token: str | None) -> bool:
    normalized = str(token or "").strip().lower()
    return normalized in PLACEHOLDER_DASHBOARD_API_TOKENS


def dashboard_api_token_issue(
    token: str | None,
    *,
    strict: bool = False,
    environ: Mapping[str, Any] | None = None,
) -> str:
    value = str(token or "").strip()
    if not value:
        return "missing_dashboard_api_token"
    if is_placeholder_dashboard_api_token(value):
        return "default_dashboard_api_token"
    if strict:
        min_length = _env_int(
            "DASHBOARD_API_TOKEN_MIN_LENGTH",
            DEFAULT_PRODUCTION_TOKEN_MIN_LENGTH,
            minimum=8,
            environ=environ,
        )
        if len(value) < int(min_length):
            return "weak_dashboard_api_token"
    return ""


def validate_mutation_auth_config(
    dashboard_api_token: str | None = None,
    *,
    environ: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    env = _env(environ)
    token = (
        env_text("DASHBOARD_API_TOKEN", environ=env)
        if dashboard_api_token is None
        else str(dashboard_api_token or "").strip()
    )
    strict_reasons = strict_mutation_auth_reasons(environ=env)
    issue = dashboard_api_token_issue(
        token,
        strict=bool(strict_reasons),
        environ=env,
    )
    ok = not (strict_reasons and issue)
    return {
        "ok": bool(ok),
        "dashboard_api_token_configured": bool(token),
        "dashboard_api_token_issue": issue,
        "strict": bool(strict_reasons),
        "strict_reasons": list(strict_reasons),
        "min_length": _env_int(
            "DASHBOARD_API_TOKEN_MIN_LENGTH",
            DEFAULT_PRODUCTION_TOKEN_MIN_LENGTH,
            minimum=8,
            environ=env,
        ),
    }


def format_mutation_auth_config_error(state: Mapping[str, Any]) -> str:
    issue = str(state.get("dashboard_api_token_issue") or "invalid_dashboard_api_token")
    reasons = ",".join(str(item) for item in list(state.get("strict_reasons") or []))
    if issue == "missing_dashboard_api_token":
        detail = "DASHBOARD_API_TOKEN must be set"
    elif issue == "default_dashboard_api_token":
        detail = "DASHBOARD_API_TOKEN must not use a placeholder/default value"
    elif issue == "weak_dashboard_api_token":
        detail = (
            "DASHBOARD_API_TOKEN must be at least "
            f"{int(state.get('min_length') or DEFAULT_PRODUCTION_TOKEN_MIN_LENGTH)} characters"
        )
    else:
        detail = f"DASHBOARD_API_TOKEN is invalid: {issue}"
    return f"{detail} when production/live mutation auth is required ({reasons})"


def safe_dev_localhost_fallback_enabled(
    *, environ: Mapping[str, Any] | None = None
) -> bool:
    env = _env(environ)
    if not env_truthy("TS_API_ALLOW_LOCALHOST_MUTATIONS_WITHOUT_TOKEN", environ=env):
        return False

    env_explicit_safe_dev = any(
        str(env.get(key, "") or "").strip().lower() in SAFE_DEV_ENV_VALUES
        for key in ("TS_ENV", "ENV", "NODE_ENV", "APP_ENV")
        if str(env.get(key, "") or "").strip()
    )
    if not env_explicit_safe_dev:
        return False

    engine_mode = str(env.get("ENGINE_MODE", "") or "").strip().lower()
    execution_mode = str(env.get("EXECUTION_MODE", "") or "").strip().lower()
    if engine_mode not in SAFE_DEV_ENGINE_MODES:
        return False
    if execution_mode not in SAFE_DEV_EXECUTION_MODES:
        return False

    return not bool(strict_mutation_auth_reasons(environ=env))


__all__ = [
    "DEFAULT_PRODUCTION_TOKEN_MIN_LENGTH",
    "PLACEHOLDER_DASHBOARD_API_TOKENS",
    "dashboard_api_token_issue",
    "env_flag",
    "env_text",
    "env_truthy",
    "format_mutation_auth_config_error",
    "is_placeholder_dashboard_api_token",
    "safe_dev_localhost_fallback_enabled",
    "strict_mutation_auth_reasons",
    "validate_mutation_auth_config",
]
