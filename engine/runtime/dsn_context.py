"""Context-aware DSN hostname validation for startup preflight.

The checks in this module intentionally report only env key names, hostnames,
ports, and failure classes. They never return raw DSNs because those can carry
credentials in legacy deployments.
"""

from __future__ import annotations

import ipaddress
import os
import shlex
import socket
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlsplit

from engine.runtime.secret_sources import strict_secret_source_policy_required

_TRUTHY = {"1", "true", "yes", "on", "y"}
_CONTAINER_CONTEXTS = {"container", "container_internal", "docker", "docker_internal", "compose"}
_HOST_CONTEXTS = {"host", "local", "localhost"}
_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}
_COMPOSE_SERVICE_HOSTS = {
    "minio",
    "operator",
    "redis",
    "runtime",
    "timescaledb",
    "ts_app",
    "ts_ingest",
    "ts_reader",
}


DSN_CONTEXT_ENV_SPECS: tuple[dict[str, Any], ...] = (
    {"key": "TS_PG_DSN", "kind": "postgres", "default_scheme": "postgresql", "default_port": 5432},
    {"key": "TIMESCALE_DSN", "kind": "postgres", "default_scheme": "postgresql", "default_port": 5432},
    {"key": "TIMESCALE_PRICES_DSN", "kind": "postgres", "default_scheme": "postgresql", "default_port": 5432},
    {"key": "OFFLINE_TS_PG_DSN", "kind": "postgres", "default_scheme": "postgresql", "default_port": 5432},
    {"key": "REDIS_URL", "kind": "redis", "default_scheme": "redis", "default_port": 6379},
    {"key": "TS_REDIS_URL", "kind": "redis", "default_scheme": "redis", "default_port": 6379},
    {"key": "LIVE_CACHE_REDIS_URL", "kind": "redis", "default_scheme": "redis", "default_port": 6379},
    {"key": "OBJECT_STORE_ENDPOINT", "kind": "http", "default_scheme": "http", "default_port": 9000},
)


def _env_text(environ: Mapping[str, Any], name: str) -> str:
    return str(environ.get(name, "") or "").strip()


def _env_bool(environ: Mapping[str, Any], name: str, *, default: bool = False) -> bool:
    raw = _env_text(environ, name)
    if not raw:
        return bool(default)
    return raw.lower() in _TRUTHY


def runtime_dsn_context(environ: Mapping[str, Any] | None = None) -> str:
    env = os.environ if environ is None else environ
    for key in ("TRADING_DSN_CONTEXT", "TRADING_RUNTIME_CONTEXT", "DASHBOARD_BIND_CONTEXT"):
        value = _env_text(env, key).lower()
        if value in _CONTAINER_CONTEXTS:
            return "container"
        if value in _HOST_CONTEXTS:
            return "host"
    if _env_bool(env, "TRADING_IN_CONTAINER") or _env_bool(env, "RUNNING_IN_CONTAINER"):
        return "container"
    if os.path.exists("/.dockerenv") or os.path.exists("/run/.containerenv"):
        return "container"
    return "host"


def _parse_keyword_conninfo(text: str) -> tuple[str | None, int | None]:
    values: dict[str, str] = {}
    try:
        tokens = shlex.split(str(text or ""))
    except ValueError:
        tokens = str(text or "").split()
    for token in tokens:
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        values[str(key).strip().lower()] = str(value).strip()
    host = values.get("host") or values.get("hostaddr")
    port = values.get("port")
    parsed_port: int | None = None
    if port:
        try:
            parsed_port = int(port)
        except ValueError:
            parsed_port = None
    return host, parsed_port


def _parse_network_target(raw: str, *, default_scheme: str, default_port: int) -> tuple[str | None, int | None]:
    text = str(raw or "").strip()
    if not text:
        return None, None
    if "://" not in text and ("host=" in text or "hostaddr=" in text):
        host, port = _parse_keyword_conninfo(text)
        return host, port or int(default_port)
    try:
        parsed = urlsplit(text if "://" in text else f"{default_scheme}://{text}")
    except ValueError:
        return None, None
    host = str(parsed.hostname or "").strip()
    if not host:
        return None, None
    try:
        port = int(parsed.port or default_port)
    except ValueError:
        port = int(default_port)
    return host, port


def _is_ip_or_unix_socket(host: str) -> bool:
    value = str(host or "").strip()
    if not value:
        return False
    if value.startswith("/"):
        return True
    try:
        ipaddress.ip_address(value.strip("[]"))
        return True
    except ValueError:
        return False


def _is_loopback_host(host: str) -> bool:
    value = str(host or "").strip().lower()
    if value in _LOOPBACK_HOSTS:
        return True
    try:
        return bool(ipaddress.ip_address(value.strip("[]")).is_loopback)
    except ValueError:
        return False


def _resolve_host(
    host: str,
    port: int | None,
    *,
    resolver: Callable[..., object] | None,
) -> tuple[bool, str]:
    if _is_ip_or_unix_socket(host):
        return True, ""
    try:
        lookup = resolver or socket.getaddrinfo
        lookup(str(host), int(port or 0), type=socket.SOCK_STREAM)
        return True, ""
    except Exception as exc:
        return False, f"{type(exc).__name__}"


def dsn_context_snapshot(
    *,
    environ: Mapping[str, Any] | None = None,
    resolver: Callable[..., object] | None = None,
) -> dict[str, Any]:
    """Return a redacted startup DSN hostname validation snapshot."""

    env = os.environ if environ is None else environ
    context = runtime_dsn_context(env)
    required = _env_bool(
        env,
        "TRADING_DSN_PREFLIGHT_REQUIRED",
        default=strict_secret_source_policy_required(env),
    )
    entries: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []

    for spec in DSN_CONTEXT_ENV_SPECS:
        key = str(spec["key"])
        raw = _env_text(env, key)
        if not raw:
            continue
        host, port = _parse_network_target(
            raw,
            default_scheme=str(spec["default_scheme"]),
            default_port=int(spec["default_port"]),
        )
        entry: dict[str, Any] = {
            "key": key,
            "kind": str(spec["kind"]),
            "context": context,
            "host": host or "",
            "port": int(port) if port is not None else None,
            "ok": True,
            "reason": "ok",
        }
        issue = ""
        if not host:
            issue = "missing_hostname"
        elif context == "host" and str(host).lower() in _COMPOSE_SERVICE_HOSTS:
            issue = "container_hostname_in_host_context"
        elif context == "container" and _is_loopback_host(str(host)):
            issue = "loopback_hostname_in_container_context"
        else:
            resolved, resolve_issue = _resolve_host(str(host), port, resolver=resolver)
            if not resolved:
                issue = f"hostname_unresolvable:{resolve_issue or 'unknown'}"

        if issue:
            entry["ok"] = False
            entry["reason"] = issue
            message = f"dsn_context_invalid:{key}:{issue}"
            if required:
                blockers.append(message)
            else:
                warnings.append(message)
        entries.append(entry)

    blockers = list(dict.fromkeys(blockers))
    warnings = list(dict.fromkeys(warnings))
    return {
        "ok": not blockers,
        "required": bool(required),
        "context": context,
        "blockers": blockers,
        "warnings": warnings,
        "entries": entries,
    }


__all__ = [
    "DSN_CONTEXT_ENV_SPECS",
    "dsn_context_snapshot",
    "runtime_dsn_context",
]
