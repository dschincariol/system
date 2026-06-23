"""Runtime live-cache boundary for price and feature snapshots.

The live market-data path still exposes price/feature cache helpers through
`engine.data.price_cache` and `engine.data.feature_store`, but ownership now
flows through this runtime interface so the backing store can move from local
memory to Redis without changing callers.
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import quote, urlparse, urlunparse

from engine.cache import codec as _cache_codec
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.metrics import emit_counter

try:
    from engine.cache.redis_pool import (
        redis_no_script_error_type as _redis_no_script_error_type,
        redis_dependency_available as _redis_dependency_available,
        redis_from_url as _redis_from_url,
    )
except Exception as exc:  # pragma: no cover - optional dependency at runtime
    def _redis_dependency_available() -> bool:
        return False

    _redis_from_url = None  # type: ignore[assignment]

    def _redis_no_script_error_type() -> None:
        return None

    _REDIS_IMPORT_ERROR = f"{type(exc).__name__}:{exc}"
else:  # pragma: no cover - exercised only when redis is installed
    _REDIS_IMPORT_ERROR = "" if _redis_dependency_available() else "ModuleNotFoundError:redis"
_redis = object() if _redis_dependency_available() else None

LOG = get_logger("engine.runtime.live_cache")
_WARNED_NONFATAL_KEYS: set[str] = set()
_LIVE_CACHE_LOCK = threading.RLock()
_LIVE_CACHE: "_BaseLiveCache | None" = None
_LIVE_CACHE_CONFIG_KEY = ""
_REDIS_CLEAR_SCAN_COUNT = 1024
_REDIS_CLEAR_BATCH_SIZE = 512
_REDIS_HEALTH_CHECK_INTERVAL_S = 5.0

_REDIS_SET_IF_NEWER_LUA = """
local ttl_s = tonumber(ARGV[1]) or 1
local results = {}
for idx = 1, #KEYS do
    local key = KEYS[idx]
    local incoming_ts = tonumber(ARGV[2 + ((idx - 1) * 2)]) or 0
    local payload = ARGV[3 + ((idx - 1) * 2)]
    local current = redis.call("GET", key)
    if current then
        local stored_ts = 0
        local ok, envelope = pcall(cmsgpack.unpack, current)
        if ok and type(envelope) == "table" then
            local data = envelope["data"]
            if type(data) == "table" then
                stored_ts = tonumber(data["snapshot_ts_ms"] or 0) or 0
                if stored_ts <= 0 then
                    local nested = data["payload"]
                    if type(nested) == "table" then
                        stored_ts = tonumber(nested["ts_ms"] or 0) or 0
                    end
                end
            end
        else
            local json_ok, legacy = pcall(cjson.decode, current)
            if json_ok and type(legacy) == "table" then
                stored_ts = tonumber(legacy["snapshot_ts_ms"] or 0) or 0
                if stored_ts <= 0 and type(legacy["payload"]) == "table" then
                    stored_ts = tonumber(legacy["payload"]["ts_ms"] or 0) or 0
                end
            end
        end
        if stored_ts > incoming_ts then
            results[idx] = 0
        else
            redis.call("SET", key, payload, "EX", ttl_s)
            results[idx] = 1
        end
    else
        redis.call("SET", key, payload, "EX", ttl_s)
        results[idx] = 1
    end
end
return results
""".strip()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.live_cache",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _emit_write_path_counter(kind: str, result: str, count: int) -> None:
    if int(count) <= 0:
        return
    try:
        emit_counter(
            "live_cache_redis_write_path_total",
            int(count),
            component="engine.runtime.live_cache",
            extra_tags={
                "kind": str(kind),
                "mode": "evalsha_lua_msgpack",
                "result": str(result),
            },
        )
    except Exception as exc:
        LOG.debug("live cache write-path metric emit failed kind=%s result=%s error=%s", kind, result, exc)


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except ValueError as exc:
        _warn_nonfatal(
            "LIVE_CACHE_ENV_FLOAT_PARSE_FAILED",
            exc,
            once_key=f"env_float:{name}:{raw}",
            env=name,
            value=raw,
            default=float(default),
        )
        return float(default)


def _secret_text_from_env(*env_names: str) -> str:
    secret_name = ""
    for env_name in env_names:
        name = str(env_name or "").strip()
        file_env_names = [name] if name.endswith("_FILE") else []
        if name.endswith("_SECRET"):
            file_env_names.append(f"{name.removesuffix('_SECRET')}_FILE")
        for file_env_name in file_env_names:
            path = str(os.environ.get(file_env_name) or "").strip()
            if path:
                from engine.runtime.secret_sources import read_secret_text_file

                return read_secret_text_file(path)
        if name.endswith("_FILE"):
            continue
        secret_name = str(os.environ.get(name) or "").strip()
        if secret_name:
            break
    if not secret_name:
        return ""
    from services.secrets.loader import load_secret

    return load_secret(secret_name).decode("utf-8", "ignore").rstrip("\r\n")


def _url_with_password(url: str, password: str) -> str:
    text = str(url or "").strip()
    if not text or not password:
        return text
    parsed = urlparse(text)
    if parsed.password or not parsed.scheme or not parsed.hostname:
        return text
    user = quote(str(parsed.username or ""), safe="")
    host = str(parsed.hostname or "")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    auth = f"{user}:{quote(password, safe='')}@" if user else f":{quote(password, safe='')}@"
    return urlunparse((parsed.scheme, auth + host, parsed.path, parsed.params, parsed.query, parsed.fragment))


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


@dataclass(frozen=True)
class LiveCacheConfig:
    backend: str
    redis_url: str
    redis_key_prefix: str
    connect_timeout_s: float
    socket_timeout_s: float

    @classmethod
    def from_env(cls) -> "LiveCacheConfig":
        backend = str(os.environ.get("LIVE_CACHE_BACKEND", "auto") or "auto").strip().lower()
        if backend not in {"auto", "memory", "redis"}:
            backend = "auto"
        redis_url = str(
            os.environ.get("LIVE_CACHE_REDIS_URL")
            or os.environ.get("REDIS_URL")
            or os.environ.get("REDIS_CACHE_URL")
            or ""
        ).strip()
        redis_url = _url_with_password(
            redis_url,
            _secret_text_from_env(
                "LIVE_CACHE_REDIS_PASSWORD_SECRET",
                "TS_REDIS_PASSWORD_SECRET",
                "REDIS_PASSWORD_SECRET",
            ),
        )
        return cls(
            backend=str(backend),
            redis_url=str(redis_url),
            redis_key_prefix=str(os.environ.get("LIVE_CACHE_REDIS_KEY_PREFIX", "trading-system:live-cache") or "trading-system:live-cache").strip()
            or "trading-system:live-cache",
            connect_timeout_s=max(0.1, _env_float("LIVE_CACHE_REDIS_CONNECT_TIMEOUT_S", 1.0)),
            socket_timeout_s=max(0.1, _env_float("LIVE_CACHE_REDIS_SOCKET_TIMEOUT_S", 1.0)),
        )


class _BaseLiveCache:
    requested_backend = "memory"
    resolved_backend = "memory"
    fallback_reason = ""

    # system-audit: ignore[stub] Base backend is an intentional safe no-op.
    def close(self) -> None:
        return None

    # system-audit: ignore[stub] Base backend is an intentional safe no-op.
    def clear_price(self, symbol: str | None = None) -> None:
        return None

    # system-audit: ignore[stub] Base backend exposes cache-miss semantics.
    def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return None

    def get_price_snapshots(self, symbols: Iterable[str]) -> dict[str, dict[str, Any] | None]:
        return {str(symbol): self.get_price_snapshot(str(symbol)) for symbol in list(symbols or [])}

    def set_price_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return False

    def set_price_snapshots(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        *,
        ttl_s: float,
        snapshot_ts_ms_by_symbol: Mapping[str, int],
    ) -> dict[str, bool]:
        return {
            str(symbol): self.set_price_snapshot(
                str(symbol),
                payload,
                ttl_s=ttl_s,
                snapshot_ts_ms=int(snapshot_ts_ms_by_symbol.get(str(symbol)) or 0),
            )
            for symbol, payload in dict(snapshots or {}).items()
        }

    # system-audit: ignore[stub] Base backend is an intentional safe no-op.
    def clear_feature(self, symbol: str | None = None) -> None:
        return None

    # system-audit: ignore[stub] Base backend exposes cache-miss semantics.
    def get_feature_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return None

    def set_feature_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return False

    def get_snapshot(self) -> dict[str, Any]:
        return {
            "ok": False,
            "backend": str(self.resolved_backend),
            "requested_backend": str(self.requested_backend),
            "resolved_backend": str(self.resolved_backend),
            "degraded": True,
            "fallback_reason": str(self.fallback_reason or "base_live_cache_no_backend"),
            "price_symbols": 0,
            "price_points": 0,
            "feature_symbols": 0,
            "price_write_count": 0,
            "feature_write_count": 0,
            "last_price_write_ts_ms": None,
            "last_feature_write_ts_ms": None,
            "last_price_snapshot_ts_ms": None,
            "last_feature_snapshot_ts_ms": None,
            "write_path": "none",
            "fallback_count": 1,
            "redis_configured": False,
            "redis_available": bool(_redis is not None and _redis_dependency_available()),
            "redis_write_path": None,
            "redis_script_load_count": 0,
            "redis_evalsha_attempts": 0,
            "redis_evalsha_results": 0,
            "redis_evalsha_noscript_reloads": 0,
            "redis_write_rejected_older_count": 0,
            "redis_write_failure_count": 0,
            "ts_ms": int(time.time() * 1000),
        }


class _MemoryCacheEntry:
    __slots__ = ("expires_at_monotonic", "snapshot_ts_ms", "payload")

    def __init__(self, *, expires_at_monotonic: float, snapshot_ts_ms: int, payload: Mapping[str, Any]) -> None:
        self.expires_at_monotonic = float(expires_at_monotonic)
        self.snapshot_ts_ms = int(snapshot_ts_ms)
        self.payload = copy.deepcopy(dict(payload or {}))


class MemoryLiveCache(_BaseLiveCache):
    resolved_backend = "memory"

    def __init__(self, *, requested_backend: str, fallback_reason: str = "") -> None:
        self.requested_backend = str(requested_backend or "memory")
        self.fallback_reason = str(fallback_reason or "")
        self._lock = threading.RLock()
        self._price: dict[str, _MemoryCacheEntry] = {}
        self._feature: dict[str, _MemoryCacheEntry] = {}
        self._metrics = {
            "price_write_count": 0,
            "feature_write_count": 0,
            "last_price_write_ts_ms": 0,
            "last_feature_write_ts_ms": 0,
            "last_price_snapshot_ts_ms": 0,
            "last_feature_snapshot_ts_ms": 0,
        }

    def _purge_expired_locked(self, bucket: dict[str, _MemoryCacheEntry]) -> None:
        now_monotonic = time.monotonic()
        expired = [key for key, entry in list(bucket.items()) if float(entry.expires_at_monotonic) <= float(now_monotonic)]
        for key in expired:
            bucket.pop(str(key), None)

    def _clear_locked(self, bucket: dict[str, _MemoryCacheEntry], symbol: str | None = None) -> None:
        self._purge_expired_locked(bucket)
        if symbol is None:
            bucket.clear()
            return
        bucket.pop(_normalize_symbol(symbol), None)

    def _get_locked(self, bucket: dict[str, _MemoryCacheEntry], symbol: str) -> dict[str, Any] | None:
        self._purge_expired_locked(bucket)
        entry = bucket.get(_normalize_symbol(symbol))
        if entry is None:
            return None
        return copy.deepcopy(dict(entry.payload or {}))

    def _set_locked(
        self,
        bucket: dict[str, _MemoryCacheEntry],
        symbol: str,
        payload: Mapping[str, Any],
        *,
        ttl_s: float,
        snapshot_ts_ms: int,
        metric_key: str,
        metric_ts_key: str,
    ) -> bool:
        self._purge_expired_locked(bucket)
        symbol_key = _normalize_symbol(symbol)
        existing = bucket.get(symbol_key)
        if existing is not None and int(existing.snapshot_ts_ms) > int(snapshot_ts_ms):
            return False
        bucket[symbol_key] = _MemoryCacheEntry(
            expires_at_monotonic=(time.monotonic() + max(0.05, float(ttl_s))),
            snapshot_ts_ms=int(snapshot_ts_ms),
            payload=payload,
        )
        self._metrics[metric_key] = int(self._metrics.get(metric_key) or 0) + 1
        self._metrics[metric_ts_key] = int(time.time() * 1000)
        if metric_key == "price_write_count":
            self._metrics["last_price_snapshot_ts_ms"] = int(snapshot_ts_ms)
        if metric_key == "feature_write_count":
            self._metrics["last_feature_snapshot_ts_ms"] = int(snapshot_ts_ms)
        return True

    def clear_price(self, symbol: str | None = None) -> None:
        with self._lock:
            self._clear_locked(self._price, symbol=symbol)

    def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            return self._get_locked(self._price, symbol)

    def get_price_snapshots(self, symbols: Iterable[str]) -> dict[str, dict[str, Any] | None]:
        symbol_keys = [_normalize_symbol(symbol) for symbol in list(symbols or [])]
        with self._lock:
            return {symbol: self._get_locked(self._price, symbol) for symbol in symbol_keys if symbol}

    def set_price_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        with self._lock:
            return self._set_locked(
                self._price,
                symbol,
                payload,
                ttl_s=ttl_s,
                snapshot_ts_ms=int(snapshot_ts_ms),
                metric_key="price_write_count",
                metric_ts_key="last_price_write_ts_ms",
            )

    def set_price_snapshots(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        *,
        ttl_s: float,
        snapshot_ts_ms_by_symbol: Mapping[str, int],
    ) -> dict[str, bool]:
        results: dict[str, bool] = {}
        with self._lock:
            for symbol, payload in dict(snapshots or {}).items():
                symbol_key = _normalize_symbol(symbol)
                if not symbol_key:
                    continue
                results[symbol_key] = self._set_locked(
                    self._price,
                    symbol_key,
                    payload,
                    ttl_s=ttl_s,
                    snapshot_ts_ms=int(snapshot_ts_ms_by_symbol.get(symbol_key) or snapshot_ts_ms_by_symbol.get(str(symbol)) or 0),
                    metric_key="price_write_count",
                    metric_ts_key="last_price_write_ts_ms",
                )
        return results

    def clear_feature(self, symbol: str | None = None) -> None:
        with self._lock:
            self._clear_locked(self._feature, symbol=symbol)

    def get_feature_snapshot(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            return self._get_locked(self._feature, symbol)

    def set_feature_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        with self._lock:
            return self._set_locked(
                self._feature,
                symbol,
                payload,
                ttl_s=ttl_s,
                snapshot_ts_ms=int(snapshot_ts_ms),
                metric_key="feature_write_count",
                metric_ts_key="last_feature_write_ts_ms",
            )

    def get_snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._purge_expired_locked(self._price)
            self._purge_expired_locked(self._feature)
            price_symbols = int(len(self._price))
            feature_symbols = int(len(self._feature))
            price_points = int(
                sum(len(list((entry.payload or {}).get("points") or [])) for entry in list(self._price.values()))
            )
            metrics = dict(self._metrics)
        return {
            "ok": True,
            "backend": "memory",
            "requested_backend": str(self.requested_backend),
            "resolved_backend": "memory",
            "degraded": bool(self.fallback_reason),
            "fallback_reason": (str(self.fallback_reason) if self.fallback_reason else None),
            "price_symbols": int(price_symbols),
            "price_points": int(price_points),
            "feature_symbols": int(feature_symbols),
            "price_write_count": int(metrics.get("price_write_count") or 0),
            "feature_write_count": int(metrics.get("feature_write_count") or 0),
            "last_price_write_ts_ms": int(metrics.get("last_price_write_ts_ms") or 0) or None,
            "last_feature_write_ts_ms": int(metrics.get("last_feature_write_ts_ms") or 0) or None,
            "last_price_snapshot_ts_ms": int(metrics.get("last_price_snapshot_ts_ms") or 0) or None,
            "last_feature_snapshot_ts_ms": int(metrics.get("last_feature_snapshot_ts_ms") or 0) or None,
            "write_path": "memory",
            "fallback_count": int(1 if self.fallback_reason else 0),
            "redis_configured": False,
            "redis_available": bool(_redis is not None and _redis_dependency_available()),
            "redis_write_path": None,
            "redis_script_load_count": 0,
            "redis_evalsha_attempts": 0,
            "redis_evalsha_results": 0,
            "redis_evalsha_noscript_reloads": 0,
            "redis_write_rejected_older_count": 0,
            "redis_write_failure_count": 0,
            "ts_ms": int(time.time() * 1000),
        }


class RedisLiveCache(_BaseLiveCache):
    resolved_backend = "redis"

    def __init__(self, config: LiveCacheConfig, *, requested_backend: str) -> None:
        if _redis is None or not _redis_dependency_available() or _redis_from_url is None:  # pragma: no cover - guarded by builder
            raise RuntimeError("redis_dependency_unavailable")
        _cache_codec.require_msgpack()
        self.requested_backend = str(requested_backend or "redis")
        self.fallback_reason = ""
        self._config = config
        self._last_error = ""
        self._last_error_ts_ms = 0
        self._set_if_newer_sha = ""
        self._last_health_check_monotonic = 0.0
        self._last_health_check_ts_ms = 0
        self._last_health_ok = False
        self._lock = threading.RLock()
        self._metrics: dict[str, Any] = {
            "price_write_count": 0,
            "feature_write_count": 0,
            "last_price_write_ts_ms": 0,
            "last_feature_write_ts_ms": 0,
            "last_price_snapshot_ts_ms": 0,
            "last_feature_snapshot_ts_ms": 0,
            "redis_script_load_count": 0,
            "redis_evalsha_attempts": 0,
            "redis_evalsha_results": 0,
            "redis_evalsha_noscript_reloads": 0,
            "redis_write_rejected_older_count": 0,
            "redis_write_failure_count": 0,
        }
        self._client = _redis_from_url(
            str(config.redis_url),
            socket_connect_timeout=float(config.connect_timeout_s),
            socket_timeout=float(config.socket_timeout_s),
            decode_responses=False,
        )
        self._set_if_newer_sha = self._load_set_if_newer_script()

    def close(self) -> None:
        try:
            close = getattr(self._client, "close", None)
            if callable(close):
                close()
        except Exception as exc:  # pragma: no cover - best effort shutdown
            self._record_error(exc, once_key="redis_live_cache_close_failed")

    def _record_error(self, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
        self._last_error = f"{type(error).__name__}:{error}"
        self._last_error_ts_ms = int(time.time() * 1000)
        _warn_nonfatal(
            "LIVE_CACHE_REDIS_OPERATION_FAILED",
            error,
            once_key=once_key,
            backend="redis",
            **extra,
        )

    def _key(self, kind: str, symbol: str) -> str:
        return f"{self._config.redis_key_prefix}:{str(kind)}:{_normalize_symbol(symbol)}"

    def _load_set_if_newer_script(self) -> str:
        sha = str(self._client.script_load(_REDIS_SET_IF_NEWER_LUA))
        with self._lock:
            self._metrics["redis_script_load_count"] = int(self._metrics.get("redis_script_load_count") or 0) + 1
        return sha

    def _metric_increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._metrics[str(key)] = int(self._metrics.get(str(key)) or 0) + int(amount)

    def _record_write_results(self, *, kind: str, results: Mapping[str, tuple[bool, int]]) -> None:
        now_ms = int(time.time() * 1000)
        accepted_count = 0
        rejected = 0
        with self._lock:
            self._metrics["redis_evalsha_results"] = int(self._metrics.get("redis_evalsha_results") or 0) + int(len(results))
            for accepted, snapshot_ts_ms in list(results.values()):
                if not bool(accepted):
                    rejected += 1
                    continue
                accepted_count += 1
                if str(kind) == "price":
                    self._metrics["price_write_count"] = int(self._metrics.get("price_write_count") or 0) + 1
                    self._metrics["last_price_write_ts_ms"] = int(now_ms)
                    self._metrics["last_price_snapshot_ts_ms"] = max(
                        int(self._metrics.get("last_price_snapshot_ts_ms") or 0),
                        int(snapshot_ts_ms),
                    )
                elif str(kind) == "feature":
                    self._metrics["feature_write_count"] = int(self._metrics.get("feature_write_count") or 0) + 1
                    self._metrics["last_feature_write_ts_ms"] = int(now_ms)
                    self._metrics["last_feature_snapshot_ts_ms"] = max(
                        int(self._metrics.get("last_feature_snapshot_ts_ms") or 0),
                        int(snapshot_ts_ms),
                    )
            if rejected:
                self._metrics["redis_write_rejected_older_count"] = int(
                    self._metrics.get("redis_write_rejected_older_count") or 0
                ) + int(rejected)
        _emit_write_path_counter(str(kind), "accepted", int(accepted_count))
        _emit_write_path_counter(str(kind), "rejected_older", int(rejected))

    def _record_write_result(self, *, kind: str, accepted: bool, snapshot_ts_ms: int) -> None:
        self._record_write_results(kind=str(kind), results={"": (bool(accepted), int(snapshot_ts_ms))})

    def _serialize(self, payload: Mapping[str, Any], snapshot_ts_ms: int) -> bytes:
        return _cache_codec.encode(
            {
                "snapshot_ts_ms": int(snapshot_ts_ms),
                "payload": dict(payload or {}),
            },
            ts_ms=int(snapshot_ts_ms),
        )

    def _decode_legacy_json(self, raw: Any) -> tuple[int, dict[str, Any] | None]:
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray, memoryview)) else str(raw)
        parsed = json.loads(str(text))
        if not isinstance(parsed, dict):
            return 0, None
        payload = dict(parsed.get("payload") or {})
        snapshot_ts_ms = int(parsed.get("snapshot_ts_ms") or payload.get("ts_ms") or 0)
        return int(snapshot_ts_ms), payload

    def _deserialize(self, raw: Any) -> tuple[int, dict[str, Any] | None]:
        if raw in (None, "", b""):
            return 0, None
        try:
            decoded = _cache_codec.decode(raw)
        except Exception as codec_exc:
            try:
                return self._decode_legacy_json(raw)
            except Exception:
                self._record_error(codec_exc, once_key="redis_live_cache_deserialize_failed")
                return 0, None
        if not isinstance(decoded, dict):
            return 0, None
        payload = dict(decoded.get("payload") or {})
        snapshot_ts_ms = int(decoded.get("snapshot_ts_ms") or payload.get("ts_ms") or 0)
        return int(snapshot_ts_ms), payload

    def _is_no_script_error(self, error: BaseException) -> bool:
        no_script_error = _redis_no_script_error_type()
        if no_script_error is not None and isinstance(error, no_script_error):
            return True
        text = str(error or "").lower()
        return "noscript" in text or "no matching script" in text

    def _set_many_if_newer(
        self,
        kind: str,
        payloads_by_symbol: Mapping[str, Mapping[str, Any]],
        *,
        ttl_s: float,
        snapshot_ts_ms_by_symbol: Mapping[str, int],
    ) -> dict[str, bool]:
        normalized_payloads: dict[str, Mapping[str, Any]] = {}
        for symbol, payload in dict(payloads_by_symbol or {}).items():
            symbol_key = _normalize_symbol(symbol)
            if symbol_key:
                normalized_payloads[symbol_key] = payload
        if not normalized_payloads:
            return {}
        try:
            ttl_seconds = max(1, int(round(float(ttl_s))))
            symbols = list(normalized_payloads.keys())
            keys = [self._key(kind, symbol) for symbol in symbols]
            args: list[Any] = [int(ttl_seconds)]
            ts_by_symbol: dict[str, int] = {}
            for symbol in symbols:
                snapshot_ts_ms = int(snapshot_ts_ms_by_symbol.get(symbol) or 0)
                ts_by_symbol[symbol] = int(snapshot_ts_ms)
                args.append(int(snapshot_ts_ms))
                args.append(self._serialize(normalized_payloads[symbol], int(snapshot_ts_ms)))
            try:
                self._metric_increment("redis_evalsha_attempts")
                result = self._client.evalsha(
                    str(self._set_if_newer_sha),
                    int(len(keys)),
                    *keys,
                    *args,
                )
            except Exception as exc:
                if not self._is_no_script_error(exc):
                    raise
                self._metric_increment("redis_evalsha_noscript_reloads")
                self._set_if_newer_sha = self._load_set_if_newer_script()
                self._metric_increment("redis_evalsha_attempts")
                result = self._client.evalsha(
                    str(self._set_if_newer_sha),
                    int(len(keys)),
                    *keys,
                    *args,
                )
            result_values = list(result if isinstance(result, (list, tuple)) else [result])
            accepted_by_symbol = {
                symbol: bool(int(result_values[idx] if idx < len(result_values) else 0) == 1)
                for idx, symbol in enumerate(symbols)
            }
            self._record_write_results(
                kind=str(kind),
                results={symbol: (bool(accepted), int(ts_by_symbol.get(symbol) or 0)) for symbol, accepted in accepted_by_symbol.items()},
            )
            return accepted_by_symbol
        except Exception as exc:
            self._metric_increment("redis_write_failure_count", amount=max(1, int(len(normalized_payloads))))
            _emit_write_path_counter(str(kind), "failure", max(1, int(len(normalized_payloads))))
            self._record_error(exc, once_key="redis_live_cache_set_failed", kind=str(kind))
            return {symbol: False for symbol in normalized_payloads}

    def _set_if_newer(
        self,
        kind: str,
        key: str,
        payload: Mapping[str, Any],
        *,
        ttl_s: float,
        snapshot_ts_ms: int,
    ) -> bool:
        symbol = str(key).rsplit(":", 1)[-1]
        results = self._set_many_if_newer(
            str(kind),
            {symbol: payload},
            ttl_s=ttl_s,
            snapshot_ts_ms_by_symbol={symbol: int(snapshot_ts_ms)},
        )
        return bool(results.get(_normalize_symbol(symbol)))

    def _get(self, key: str) -> dict[str, Any] | None:
        try:
            _snapshot_ts_ms, payload = self._deserialize(self._client.get(key))
            return payload
        except Exception as exc:
            self._record_error(exc, once_key=f"redis_live_cache_get_failed:{key}", key=str(key))
            return None

    def _pipeline(self):
        pipeline_fn = getattr(self._client, "pipeline", None)
        if not callable(pipeline_fn):
            return None
        try:
            return pipeline_fn(transaction=False)
        except TypeError:
            return pipeline_fn()

    def _delete_keys_direct(self, keys: Iterable[Any]) -> None:
        key_list = list(keys or [])
        if not key_list:
            return
        unlink = getattr(self._client, "unlink", None)
        if callable(unlink):
            try:
                unlink(*key_list)
                return
            except Exception as exc:
                LOG.debug("redis live cache direct UNLINK failed; retrying DEL count=%s error=%s", len(key_list), exc)
        self._client.delete(*key_list)

    def _delete_keys_batch(self, keys: Iterable[Any]) -> None:
        key_list = list(keys or [])
        if not key_list:
            return
        pipe = self._pipeline()
        if pipe is None:
            self._delete_keys_direct(key_list)
            return
        used_unlink = False
        try:
            unlink = getattr(pipe, "unlink", None)
            if callable(unlink):
                unlink(*key_list)
                used_unlink = True
            else:
                pipe.delete(*key_list)
            pipe.execute()
        except Exception:
            if not used_unlink:
                raise
            fallback = self._pipeline()
            if fallback is None:
                self._client.delete(*key_list)
                return
            fallback.delete(*key_list)
            fallback.execute()

    def _delete_kind(self, kind: str, symbol: str | None = None) -> None:
        try:
            if symbol is not None:
                self._delete_keys_batch([self._key(kind, symbol)])
                return
            pattern = f"{self._config.redis_key_prefix}:{str(kind)}:*"
            batch: list[Any] = []
            for key in self._client.scan_iter(match=pattern, count=_REDIS_CLEAR_SCAN_COUNT):
                batch.append(key)
                if len(batch) >= _REDIS_CLEAR_BATCH_SIZE:
                    self._delete_keys_batch(batch)
                    batch.clear()
            if batch:
                self._delete_keys_batch(batch)
        except Exception as exc:
            self._record_error(exc, once_key=f"redis_live_cache_delete_failed:{kind}", kind=str(kind))

    def clear_price(self, symbol: str | None = None) -> None:
        self._delete_kind("price", symbol=symbol)

    def get_price_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return self._get(self._key("price", symbol))

    def get_price_snapshots(self, symbols: Iterable[str]) -> dict[str, dict[str, Any] | None]:
        symbol_keys = [_normalize_symbol(symbol) for symbol in list(symbols or [])]
        symbol_keys = [symbol for symbol in symbol_keys if symbol]
        if not symbol_keys:
            return {}
        keys = [self._key("price", symbol) for symbol in symbol_keys]
        try:
            values = list(self._client.mget(keys))
            return {
                symbol: self._deserialize(values[idx])[1] if idx < len(values) else None
                for idx, symbol in enumerate(symbol_keys)
            }
        except Exception as exc:
            self._record_error(exc, once_key="redis_live_cache_mget_failed:price", kind="price")
            return {symbol: None for symbol in symbol_keys}

    def set_price_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return self._set_if_newer(
            "price",
            self._key("price", symbol),
            payload,
            ttl_s=ttl_s,
            snapshot_ts_ms=int(snapshot_ts_ms),
        )

    def set_price_snapshots(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        *,
        ttl_s: float,
        snapshot_ts_ms_by_symbol: Mapping[str, int],
    ) -> dict[str, bool]:
        return self._set_many_if_newer(
            "price",
            snapshots,
            ttl_s=ttl_s,
            snapshot_ts_ms_by_symbol=snapshot_ts_ms_by_symbol,
        )

    def clear_feature(self, symbol: str | None = None) -> None:
        self._delete_kind("feature", symbol=symbol)

    def get_feature_snapshot(self, symbol: str) -> dict[str, Any] | None:
        return self._get(self._key("feature", symbol))

    def set_feature_snapshot(self, symbol: str, payload: Mapping[str, Any], *, ttl_s: float, snapshot_ts_ms: int) -> bool:
        return self._set_if_newer(
            "feature",
            self._key("feature", symbol),
            payload,
            ttl_s=ttl_s,
            snapshot_ts_ms=int(snapshot_ts_ms),
        )

    def _health_ok(self) -> bool:
        now = time.monotonic()
        with self._lock:
            if (
                self._last_health_check_monotonic > 0.0
                and (float(now) - float(self._last_health_check_monotonic)) < _REDIS_HEALTH_CHECK_INTERVAL_S
            ):
                return bool(self._last_health_ok)
            self._last_health_check_monotonic = float(now)
            self._last_health_check_ts_ms = int(time.time() * 1000)
        ok = False
        try:
            ok = bool(self._client.ping())
        except Exception as exc:
            self._record_error(exc, once_key="redis_live_cache_ping_failed")
        with self._lock:
            self._last_health_ok = bool(ok)
        return bool(ok)

    def get_snapshot(self) -> dict[str, Any]:
        ok = self._health_ok()
        with self._lock:
            metrics = dict(self._metrics)
            last_health_check_ts_ms = int(self._last_health_check_ts_ms)
        return {
            "ok": bool(ok),
            "backend": "redis",
            "requested_backend": str(self.requested_backend),
            "resolved_backend": "redis",
            "degraded": not bool(ok),
            "fallback_reason": None,
            "price_symbols": None,
            "price_points": None,
            "feature_symbols": None,
            "price_write_count": int(metrics.get("price_write_count") or 0),
            "feature_write_count": int(metrics.get("feature_write_count") or 0),
            "last_price_write_ts_ms": int(metrics.get("last_price_write_ts_ms") or 0) or None,
            "last_feature_write_ts_ms": int(metrics.get("last_feature_write_ts_ms") or 0) or None,
            "last_price_snapshot_ts_ms": int(metrics.get("last_price_snapshot_ts_ms") or 0) or None,
            "last_feature_snapshot_ts_ms": int(metrics.get("last_feature_snapshot_ts_ms") or 0) or None,
            "write_path": "redis_evalsha_lua_msgpack",
            "fallback_count": 0,
            "redis_configured": bool(self._config.redis_url),
            "redis_available": True,
            "redis_url_present": bool(self._config.redis_url),
            "redis_codec": _cache_codec.codec_name(),
            "redis_write_path": "evalsha_lua_msgpack",
            "redis_script_load_count": int(metrics.get("redis_script_load_count") or 0),
            "redis_evalsha_attempts": int(metrics.get("redis_evalsha_attempts") or 0),
            "redis_evalsha_results": int(metrics.get("redis_evalsha_results") or 0),
            "redis_evalsha_noscript_reloads": int(metrics.get("redis_evalsha_noscript_reloads") or 0),
            "redis_write_rejected_older_count": int(metrics.get("redis_write_rejected_older_count") or 0),
            "redis_write_failure_count": int(metrics.get("redis_write_failure_count") or 0),
            "last_error": (str(self._last_error) if self._last_error else None),
            "last_error_ts_ms": (int(self._last_error_ts_ms) if self._last_error_ts_ms > 0 else None),
            "redis_health_check_interval_s": float(_REDIS_HEALTH_CHECK_INTERVAL_S),
            "redis_last_health_check_ts_ms": int(last_health_check_ts_ms) if last_health_check_ts_ms > 0 else None,
            "ts_ms": int(time.time() * 1000),
        }


def _config_key(config: LiveCacheConfig) -> str:
    return "|".join(
        [
            str(config.backend),
            str(config.redis_url),
            str(config.redis_key_prefix),
            str(config.connect_timeout_s),
            str(config.socket_timeout_s),
        ]
    )


def _build_live_cache(config: LiveCacheConfig) -> _BaseLiveCache:
    if str(config.backend) == "memory":
        return MemoryLiveCache(requested_backend="memory")
    wants_redis = str(config.backend) == "redis"
    auto_mode = str(config.backend) == "auto"
    if _redis is None or not _redis_dependency_available():
        if wants_redis:
            return MemoryLiveCache(requested_backend="redis", fallback_reason="redis_dependency_unavailable")
        return MemoryLiveCache(requested_backend=("auto" if auto_mode else str(config.backend)))
    if not _cache_codec.msgpack_available():
        if wants_redis:
            return MemoryLiveCache(requested_backend="redis", fallback_reason="msgpack_dependency_unavailable")
        return MemoryLiveCache(requested_backend=("auto" if auto_mode else str(config.backend)))
    if not str(config.redis_url or "").strip():
        if wants_redis:
            return MemoryLiveCache(requested_backend="redis", fallback_reason="redis_url_missing")
        return MemoryLiveCache(requested_backend=("auto" if auto_mode else str(config.backend)))
    try:
        backend = RedisLiveCache(config, requested_backend=("auto" if auto_mode else str(config.backend)))
        snapshot = backend.get_snapshot()
        if bool(snapshot.get("ok")):
            return backend
        if wants_redis:
            return MemoryLiveCache(
                requested_backend="redis",
                fallback_reason=str(snapshot.get("last_error") or "redis_ping_failed"),
            )
    except Exception as exc:  # pragma: no cover - runtime defensive guard
        if wants_redis:
            return MemoryLiveCache(requested_backend="redis", fallback_reason=f"redis_init_failed:{type(exc).__name__}")
        _warn_nonfatal(
            "LIVE_CACHE_REDIS_INIT_FAILED",
            exc,
            once_key="live_cache_redis_init_failed",
        )
    return MemoryLiveCache(requested_backend=("auto" if auto_mode else str(config.backend)))


def get_live_cache() -> _BaseLiveCache:
    global _LIVE_CACHE, _LIVE_CACHE_CONFIG_KEY
    config = LiveCacheConfig.from_env()
    config_key = _config_key(config)
    with _LIVE_CACHE_LOCK:
        if _LIVE_CACHE is not None and str(_LIVE_CACHE_CONFIG_KEY) == str(config_key):
            return _LIVE_CACHE
        previous = _LIVE_CACHE
        _LIVE_CACHE = _build_live_cache(config)
        _LIVE_CACHE_CONFIG_KEY = str(config_key)
    if previous is not None:
        try:
            previous.close()
        except Exception as exc:
            _warn_nonfatal(
                "LIVE_CACHE_PREVIOUS_CLOSE_FAILED",
                exc,
                once_key="live_cache_previous_close_failed",
            )
    return _LIVE_CACHE


def get_live_cache_snapshot() -> dict[str, Any]:
    return dict(get_live_cache().get_snapshot() or {})


def close_live_cache() -> None:
    global _LIVE_CACHE, _LIVE_CACHE_CONFIG_KEY
    with _LIVE_CACHE_LOCK:
        backend = _LIVE_CACHE
        _LIVE_CACHE = None
        _LIVE_CACHE_CONFIG_KEY = ""
    if backend is not None:
        try:
            backend.close()
        except Exception as exc:
            _warn_nonfatal(
                "LIVE_CACHE_CLOSE_FAILED",
                exc,
                once_key="live_cache_close_failed",
            )


__all__ = [
    "LiveCacheConfig",
    "close_live_cache",
    "get_live_cache",
    "get_live_cache_snapshot",
]
