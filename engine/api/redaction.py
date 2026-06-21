"""Shared redaction helpers for API operational payloads."""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit, urlunsplit


_SENSITIVE_KEY_EXACT = frozenset(
    {
        "authorization",
        "auth_header",
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "key_id",
        "access_key",
        "secret_key",
        "session_key",
        "session_token",
        "refresh_token",
        "client_secret",
        "private_key",
        "master_key",
        "hmac_key",
        "credential",
        "credentials",
    }
)
_SENSITIVE_KEY_SUFFIXES = (
    "_authorization",
    "_auth_header",
    "_password",
    "_passwd",
    "_pwd",
    "_secret",
    "_token",
    "_api_key",
    "_apikey",
    "_key_id",
    "_access_key",
    "_secret_key",
    "_session_key",
    "_session_token",
    "_refresh_token",
    "_client_secret",
    "_private_key",
    "_master_key",
    "_hmac_key",
    "_credential",
    "_credentials",
    "_key_file",
)
_DSN_KEY_EXACT = frozenset({"dsn", "url", "uri", "conninfo", "connection_string"})
_DSN_KEY_SUFFIXES = ("_dsn", "_url", "_uri", "_conninfo", "_connection_string")
_IDENTIFIER_KEY_EXACT = frozenset(
    {
        "account_id",
        "account_number",
        "broker_account_id",
        "broker_account_number",
        "broker_order_id",
        "broker_native_order_id",
        "external_account_id",
        "ibkr_account",
        "alpaca_account",
    }
)
_IDENTIFIER_KEY_SUFFIXES = (
    "_account_id",
    "_account_number",
    "_broker_account_id",
    "_broker_account_number",
    "_broker_order_id",
    "_broker_native_order_id",
    "_external_account_id",
)

_URL_WITH_USERINFO_RE = re.compile(
    r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?P<userinfo>[^/\s:@]+(?::[^@\s/]*)?@)",
    re.IGNORECASE,
)
_KEY_VALUE_SECRET_RE = re.compile(
    r"(?P<key>\b[A-Za-z0-9_.-]*(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|secret[_-]?key|"
    r"key[_-]?id|session[_-]?token|refresh[_-]?token|client[_-]?secret|private[_-]?key|master[_-]?key|"
    r"hmac[_-]?key|account[_-]?id|account[_-]?number|broker[_-]?account[_-]?id|"
    r"broker[_-]?order[_-]?id|authorization)[A-Za-z0-9_.-]*\b)"
    r"(?P<sep>\s*[:=]\s*)"
    r"(?P<quote>['\"]?)"
    r"(?P<value>[^'\"\s,;]+)"
    r"(?P=quote)",
    re.IGNORECASE,
)
_JSON_STRING_SECRET_RE = re.compile(
    r'(?P<prefix>"[^"]*(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|secret[_-]?key|'
    r'key[_-]?id|session[_-]?token|refresh[_-]?token|client[_-]?secret|private[_-]?key|master[_-]?key|'
    r'hmac[_-]?key|authorization)[^"]*"\s*:\s*")'
    r'(?P<value>[^"]*)'
    r'(?P<suffix>")',
    re.IGNORECASE,
)
_AUTHORIZATION_RE = re.compile(
    r"(?P<prefix>\bauthorization\s*[:=]\s*)(?P<value>[^\r\n,;]+)",
    re.IGNORECASE,
)


def _digest(value: object) -> str:
    text = str(value if value is not None else "")
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()[:12]


def redacted_marker(value: object) -> str:
    return f"<redacted:{_digest(value)}>"


def _redacted_identifier_marker(value: object) -> str:
    return f"<redacted-id:{_digest(value)}>"


def _key_name(key: str | None) -> str:
    return str(key or "").strip().lower().replace("-", "_")


def is_sensitive_key(key: str | None) -> bool:
    name = _key_name(key)
    return name in _SENSITIVE_KEY_EXACT or any(name.endswith(suffix) for suffix in _SENSITIVE_KEY_SUFFIXES)


def is_dsn_key(key: str | None) -> bool:
    name = _key_name(key)
    return name in _DSN_KEY_EXACT or any(name.endswith(suffix) for suffix in _DSN_KEY_SUFFIXES)


def is_identifier_key(key: str | None) -> bool:
    name = _key_name(key)
    return name in _IDENTIFIER_KEY_EXACT or any(name.endswith(suffix) for suffix in _IDENTIFIER_KEY_SUFFIXES)


def _redact_url(value: str) -> str:
    text = str(value or "")
    try:
        parts = urlsplit(text)
    except ValueError:
        return text
    if not parts.scheme or not parts.netloc or "@" not in parts.netloc:
        return text
    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, f"<redacted>@{host}", parts.path, parts.query, parts.fragment))


def _redact_url_userinfo_in_text(value: str) -> str:
    text = _redact_url(value)
    return _URL_WITH_USERINFO_RE.sub(lambda match: f"{match.group('scheme')}<redacted>@", text)


def collect_sensitive_values(environ: Mapping[str, str] | None = None) -> set[str]:
    env = environ if environ is not None else os.environ
    values: set[str] = set()
    for key, raw in env.items():
        value = str(raw or "")
        if not value:
            continue
        if is_sensitive_key(key):
            values.add(value)
        elif is_dsn_key(key) and _redact_url_userinfo_in_text(value) != value:
            values.add(value)
            for match in _KEY_VALUE_SECRET_RE.finditer(value):
                values.add(str(match.group("value") or ""))
    return values


def redact_string(value: str, known_sensitive_values: set[str] | None = None) -> str:
    text = str(value or "")
    text = _redact_url_userinfo_in_text(text)
    text = _AUTHORIZATION_RE.sub(lambda match: f"{match.group('prefix')}{redacted_marker(match.group('value').strip())}", text)

    def _replace_key_value(match: re.Match[str]) -> str:
        key = str(match.group("key") or "")
        sep = str(match.group("sep") or "=")
        quote = str(match.group("quote") or "")
        raw_value = str(match.group("value") or "")
        marker = _redacted_identifier_marker(raw_value) if is_identifier_key(key) else redacted_marker(raw_value)
        return f"{key}{sep}{quote}{marker}{quote}"

    text = _KEY_VALUE_SECRET_RE.sub(_replace_key_value, text)
    text = _JSON_STRING_SECRET_RE.sub(
        lambda match: f"{match.group('prefix')}{redacted_marker(match.group('value'))}{match.group('suffix')}",
        text,
    )
    for secret in sorted(known_sensitive_values or set(), key=len, reverse=True):
        if secret and len(secret) >= 4:
            text = text.replace(secret, redacted_marker(secret))
    return text


def redact_api_payload(
    value: Any,
    known_sensitive_values: set[str] | None = None,
    *,
    key: str = "",
    redact_identifiers: bool = True,
) -> Any:
    known = collect_sensitive_values() if known_sensitive_values is None else known_sensitive_values

    if isinstance(value, Mapping):
        if is_sensitive_key(key):
            return redacted_marker(value) if value else value
        if redact_identifiers and is_identifier_key(key):
            return _redacted_identifier_marker(value) if value else value
        return {
            str(item_key): redact_api_payload(
                item_value,
                known,
                key=str(item_key),
                redact_identifiers=redact_identifiers,
            )
            for item_key, item_value in value.items()
        }

    if isinstance(value, list):
        return [redact_api_payload(item, known, key=key, redact_identifiers=redact_identifiers) for item in value]
    if isinstance(value, tuple):
        return [redact_api_payload(item, known, key=key, redact_identifiers=redact_identifiers) for item in value]

    if isinstance(value, str):
        if is_sensitive_key(key):
            return redacted_marker(value) if value else ""
        if redact_identifiers and is_identifier_key(key):
            return _redacted_identifier_marker(value) if value else ""
        return redact_string(value, known)

    if redact_identifiers and is_identifier_key(key) and value not in (None, ""):
        return _redacted_identifier_marker(value)

    return value


__all__ = [
    "collect_sensitive_values",
    "is_dsn_key",
    "is_identifier_key",
    "is_sensitive_key",
    "redact_api_payload",
    "redact_string",
    "redacted_marker",
]
