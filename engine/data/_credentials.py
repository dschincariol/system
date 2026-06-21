"""Credential access helpers for data ingesters."""

from __future__ import annotations

import os
import threading
import time
from typing import Dict, Tuple

from engine.runtime.secret_sources import read_secret_text_file
from engine.runtime.test_isolation import running_python_tests
from services.secrets.loader import SecretNotAvailable, load_secret

_CACHE: Dict[Tuple[str, int], str] = {}
_CACHE_LOCK = threading.Lock()

_ALIASES = {
    "POLYGON_API_KEY": ("POLYGON_API_KEY", "POLYGON_KEY"),
}


def _ttl_bucket(ttl_s: int) -> int:
    ttl = int(ttl_s or 0)
    if ttl <= 0:
        return time.monotonic_ns()
    return int(time.time() // ttl)


def _decode_secret(data: bytes) -> str:
    return bytes(data or b"").decode("utf-8", "ignore").strip()


def _candidate_names(name: str) -> Tuple[str, ...]:
    secret_name = str(name or "").strip()
    if not secret_name:
        return tuple()
    return tuple(dict.fromkeys(_ALIASES.get(secret_name, (secret_name,))))


def _load_from_secret_provider(name: str) -> str:
    for candidate in _candidate_names(name):
        try:
            value = _decode_secret(load_secret(candidate))
        except SecretNotAvailable:
            continue
        if value:
            return value
    return ""


def _load_from_file(name: str) -> str:
    for candidate in _candidate_names(name):
        path = str(os.environ.get(f"{candidate}_FILE") or "").strip()
        if not path:
            continue
        value = read_secret_text_file(path)
        if value:
            return value
    return ""


def _load_from_explicit_secret(name: str) -> str:
    for candidate in _candidate_names(name):
        secret_name = str(os.environ.get(f"{candidate}_SECRET") or "").strip()
        if not secret_name:
            continue
        try:
            value = _decode_secret(load_secret(secret_name))
        except SecretNotAvailable:
            continue
        if value:
            return value
    return ""


def _load_from_env_compat(name: str) -> str:
    if str(os.environ.get("TS_SECRETS_PROVIDER") or "").strip() and not running_python_tests():
        return ""
    if str(os.environ.get("TS_ENV") or "").strip().lower() == "production":
        return ""
    for candidate in _candidate_names(name):
        value = str(os.environ.get(candidate) or "").strip()
        if value:
            return value
    return ""


def clear_data_credential_cache() -> None:
    with _CACHE_LOCK:
        _CACHE.clear()


def get_data_credential(name: str, ttl_s: int = 300) -> str:
    """Return a data-ingestion credential through the configured secret loader."""
    secret_name = str(name or "").strip()
    if not secret_name:
        return ""
    cache_key = (secret_name, _ttl_bucket(int(ttl_s or 0)))
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
        if cached is not None:
            return cached

    value = (
        _load_from_file(secret_name)
        or _load_from_explicit_secret(secret_name)
        or _load_from_secret_provider(secret_name)
        or _load_from_env_compat(secret_name)
    )

    with _CACHE_LOCK:
        _CACHE[cache_key] = str(value or "")
    return str(value or "")


__all__ = ["clear_data_credential_cache", "get_data_credential"]
