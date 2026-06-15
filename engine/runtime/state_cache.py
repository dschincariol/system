"""
FILE: state_cache.py

Runtime subsystem module for `state_cache`.
"""

import copy
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple


_DEFAULT_TTL_S = float(os.environ.get("STATE_CACHE_DEFAULT_TTL_S", "1.0"))
LOG = logging.getLogger(__name__)


class _StateCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: Dict[Tuple[str, str], Tuple[float, Any]] = {}
        self._load_locks: Dict[Tuple[str, str], threading.Lock] = {}

    def get(self, namespace: str, key: str) -> Any:
        ns = str(namespace or "").strip()
        kk = str(key or "").strip()
        now = time.time()

        with self._lock:
            item = self._data.get((ns, kk))
            if not item:
                return None

            expires_at, value = item
            if expires_at > 0.0 and now >= expires_at:
                self._data.pop((ns, kk), None)
                self._load_locks.pop((ns, kk), None)
                return None

            return copy.deepcopy(value)

    def set(self, namespace: str, key: str, value: Any, ttl_s: Optional[float] = None) -> Any:
        ns = str(namespace or "").strip()
        kk = str(key or "").strip()

        try:
            ttl = float(_DEFAULT_TTL_S if ttl_s is None else ttl_s)
        except (TypeError, ValueError):
            LOG.warning(
                "state_cache_ttl_parse_failed namespace=%s key=%s ttl_s=%r default=%s",
                ns,
                kk,
                ttl_s,
                _DEFAULT_TTL_S,
                exc_info=True,
            )
            ttl = float(_DEFAULT_TTL_S)

        expires_at = 0.0 if ttl <= 0.0 else (time.time() + ttl)

        with self._lock:
            self._data[(ns, kk)] = (expires_at, copy.deepcopy(value))

        return copy.deepcopy(value)

    def invalidate_key(self, namespace: str, key: str) -> None:
        ns = str(namespace or "").strip()
        kk = str(key or "").strip()
        with self._lock:
            self._data.pop((ns, kk), None)
            self._load_locks.pop((ns, kk), None)

    def invalidate_namespace(self, namespace: str, prefix: Optional[str] = None) -> None:
        ns = str(namespace or "").strip()
        pref = None if prefix is None else str(prefix)

        with self._lock:
            doomed = []
            for cache_ns, cache_key in list(self._data.keys()):
                if cache_ns != ns:
                    continue
                if pref is not None and not cache_key.startswith(pref):
                    continue
                doomed.append((cache_ns, cache_key))

            for k in doomed:
                self._data.pop(k, None)
                self._load_locks.pop(k, None)

    def _get_load_lock(self, namespace: str, key: str) -> threading.Lock:
        nk = (str(namespace or "").strip(), str(key or "").strip())
        with self._lock:
            lk = self._load_locks.get(nk)
            if lk is None:
                lk = threading.Lock()
                self._load_locks[nk] = lk
            return lk

    def _release_load_lock(self, namespace: str, key: str, lock: threading.Lock) -> None:
        nk = (str(namespace or "").strip(), str(key or "").strip())
        with self._lock:
            current = self._load_locks.get(nk)
            if current is lock:
                self._load_locks.pop(nk, None)

    def get_or_load(
        self,
        namespace: str,
        key: str,
        loader: Callable[[], Any],
        ttl_s: Optional[float] = None,
    ) -> Any:
        cached = self.get(namespace, key)
        if cached is not None:
            return cached

        lk = self._get_load_lock(namespace, key)
        try:
            with lk:
                cached = self.get(namespace, key)
                if cached is not None:
                    return cached

                value = loader()
                return self.set(namespace, key, value, ttl_s=ttl_s)
        finally:
            # Per-key load locks only need to exist while a miss is being
            # resolved. Dropping them here prevents unique cache keys from
            # accumulating permanent synchronization objects over time.
            self._release_load_lock(namespace, key, lk)


_CACHE = _StateCache()


def cache_get(namespace: str, key: str) -> Any:
    return _CACHE.get(namespace, key)


def cache_set(namespace: str, key: str, value: Any, ttl_s: Optional[float] = None) -> Any:
    return _CACHE.set(namespace, key, value, ttl_s=ttl_s)


def cache_invalidate_key(namespace: str, key: str) -> None:
    _CACHE.invalidate_key(namespace, key)


def cache_invalidate_namespace(namespace: str, prefix: Optional[str] = None) -> None:
    _CACHE.invalidate_namespace(namespace, prefix=prefix)


def cache_get_or_load(
    namespace: str,
    key: str,
    loader: Callable[[], Any],
    ttl_s: Optional[float] = None,
) -> Any:
    return _CACHE.get_or_load(namespace, key, loader, ttl_s=ttl_s)
