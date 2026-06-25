"""Redis-cached wrapper for ``kill_switch_state``."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import Any

from engine.audit.chain import append_chain_row
from engine.cache import codec, keys, store
from engine.cache.wrappers._common import (
    after_commit_or_now,
    dumps_json,
    l1_get,
    l1_invalidate,
    l1_set,
    now_ms,
    parse_json,
    reload_after_codec_version_mismatch,
)
from engine.runtime import storage
from engine.runtime.metrics import emit_counter, emit_gauge

KILL_SWITCH_CODEC_VERSION = 1
KILL_SWITCH_CACHE_TTL_ENV = "KILL_SWITCH_CACHE_TTL_S"
KILL_SWITCH_TTL_S = 30
KILL_SWITCH_MAX_TTL_S = 300
ENV_GLOBAL_KEYS = ("KILL_SWITCH_GLOBAL", "TRADING_KILL_SWITCH", "KILL_SWITCH")
ENV_SCOPED_KEYS = (
    ("symbol", "KILL_SWITCH_SYMBOLS"),
    ("regime", "KILL_SWITCH_REGIMES"),
    ("model", "KILL_SWITCH_MODELS"),
)

LOG = logging.getLogger(__name__)


def _configured_ttl_s() -> int:
    fallback = int(KILL_SWITCH_TTL_S or 30)
    raw = str(os.environ.get(KILL_SWITCH_CACHE_TTL_ENV, "") or "").strip()
    try:
        value = int(float(raw)) if raw else fallback
    except Exception:
        value = fallback
    return max(1, min(int(KILL_SWITCH_MAX_TTL_S), int(value)))


def _max_age_ms() -> int:
    return int(_configured_ttl_s() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _env_truthy(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _csv_values(value: Any) -> list[str]:
    return [
        str(part or "").strip()
        for part in str(value or "").split(",")
        if str(part or "").strip()
    ]


def _env_kill_switch_entries(environ: Mapping[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for key in ENV_GLOBAL_KEYS:
        raw = str(environ.get(key, "") or "").strip()
        if _env_truthy(raw):
            entries.append(
                {
                    "source": "env",
                    "scope": "global",
                    "key": key,
                    "env_key": key,
                    "reason": "env_flag_truthy",
                }
            )
    for scope, env_key in ENV_SCOPED_KEYS:
        for value in _csv_values(environ.get(env_key, "")):
            entries.append(
                {
                    "source": "env",
                    "scope": scope,
                    "key": value,
                    "env_key": env_key,
                    "reason": "env_scope_listed",
                }
            )
    return entries


def _row_enabled(row: Mapping[str, Any]) -> bool:
    try:
        return int(row.get("enabled") or 0) == 1
    except Exception:
        return False


def _persisted_active_entries(rows: list[Any]) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping) or not _row_enabled(row):
            continue
        active.append(
            {
                "source": "persisted",
                "scope": str(row.get("scope") or "global"),
                "key": str(row.get("key") or "global"),
                "reason": str(row.get("reason") or ""),
                "actor": str(row.get("actor") or ""),
            }
        )
    return active


def _persisted_source_kind(snapshot: Mapping[str, Any], persisted_read_source: str | None) -> str:
    read_source = str(persisted_read_source or snapshot.get("persisted_read_source") or snapshot.get("read_source") or "").strip().lower()
    source = str(snapshot.get("source") or "").strip().lower()
    cache_status = str(snapshot.get("cache_status") or "").strip().lower()

    if read_source in {"redis", "l1"} or "redis" in read_source or "l1" in read_source:
        return "redis"
    if read_source in {"db", "direct_db", "db_load", "db_reload", "db_refresh", "db_readonly"} or "db" in read_source:
        return "db"
    if "redis" in source:
        return "redis"
    if source.endswith(":db") or ":db" in source or "provider_unavailable" in source:
        return "db"
    if "loaded" in cache_status and "db" in source:
        return "db"
    return "unknown"


def _source_summary(sources: list[str]) -> str:
    if not sources:
        return "disarmed"
    return "armed via " + "+".join(sources)


def annotate_effective_state(
    snapshot: dict[str, Any] | None,
    *,
    environ: Mapping[str, Any] | None = None,
    persisted_read_source: str | None = None,
) -> dict[str, Any]:
    """Add an operator-facing effective kill-switch state to a snapshot.

    The effective state is reporting-only. It is the fail-safe OR of environment
    kill-switch flags and any enabled persisted row, and it does not clear or
    otherwise mutate persisted rows.
    """

    out = dict(snapshot or {"state": []})
    state = out.get("state")
    rows = list(state) if isinstance(state, list) else []
    out["state"] = rows

    env_entries = _env_kill_switch_entries(environ or os.environ)
    persisted_entries = _persisted_active_entries(rows)
    persisted_source = _persisted_source_kind(out, persisted_read_source)

    env_armed = bool(env_entries)
    persisted_armed = bool(persisted_entries)
    armed = bool(env_armed or persisted_armed)

    sources: list[str] = []
    if env_armed:
        sources.append("env")
    if persisted_armed:
        sources.append(persisted_source if persisted_source in {"db", "redis"} else "persisted")

    persisted_summary = (
        f"persisted armed via {persisted_source}"
        if persisted_armed and persisted_source != "unknown"
        else ("persisted armed" if persisted_armed else "persisted disarmed")
    )
    summary = f"{_source_summary(sources)}; {persisted_summary}"
    active = [
        *env_entries,
        *[
            {
                **entry,
                "source": persisted_source if persisted_source in {"db", "redis"} else "persisted",
                "read_source": str(out.get("read_source") or persisted_source or "unknown"),
            }
            for entry in persisted_entries
        ],
    ]

    source_entries: dict[str, list[dict[str, Any]]] = {"env": env_entries, "db": [], "redis": []}
    if persisted_source == "db":
        source_entries["db"] = [
            {**entry, "source": "db", "read_source": str(out.get("read_source") or "db")}
            for entry in persisted_entries
        ]
    elif persisted_source == "redis":
        source_entries["redis"] = [
            {**entry, "source": "redis", "read_source": str(out.get("read_source") or "redis")}
            for entry in persisted_entries
        ]

    out["effective"] = {
        "armed": armed,
        "state": "armed" if armed else "disarmed",
        "sources": sources,
        "summary": summary,
        "env_armed": env_armed,
        "persisted_armed": persisted_armed,
        "persisted_state": "armed" if persisted_armed else "disarmed",
        "persisted_read_source": persisted_source,
        "active": active,
        "provenance": {
            "env": {
                "armed": env_armed,
                "keys": sorted({str(entry.get("env_key") or "") for entry in env_entries if entry.get("env_key")}),
                "active": env_entries,
            },
            "db": {
                "read": persisted_source == "db",
                "armed": bool(persisted_armed and persisted_source == "db"),
                "active": source_entries["db"],
            },
            "redis": {
                "read": persisted_source == "redis",
                "armed": bool(persisted_armed and persisted_source == "redis"),
                "active": source_entries["redis"],
            },
            "persisted": {
                "armed": persisted_armed,
                "source": persisted_source,
                "active": persisted_entries,
                "rows": len(rows),
            },
        },
    }
    return out


def _normalize_scope(scope: str) -> str:
    value = str(scope or "").strip().lower()
    if value not in {"global", "symbol", "regime", "model"}:
        raise ValueError(f"invalid_kill_switch_scope:{scope}")
    return value


def _normalize_key(scope: str, key: str) -> str:
    value = str(key or "").strip()
    if not value:
        raise ValueError("kill_switch_key_required")
    return value


def _row_to_state(row: Any) -> dict[str, Any]:
    return {
        "scope": str(row[0] or ""),
        "key": str(row[1] or ""),
        "enabled": int(row[2] or 0),
        "reason": str(row[3] or ""),
        "actor": str(row[4] or ""),
        "meta": parse_json(row[5], {}),
        "created_ts_ms": int(row[6] or 0),
        "updated_ts_ms": int(row[7] or 0),
    }


def _provider_unavailable_snapshot(error: BaseException | str | None = None) -> dict[str, Any]:
    error_text = "" if error is None else str(error)
    return _snapshot_for_cache(
        {
            "state": [
                {
                    "scope": "global",
                    "key": "provider_unavailable",
                    "enabled": 1,
                    "reason": "kill_switch_provider_unavailable",
                    "actor": "engine.cache.wrappers.kill_switch",
                    "meta": {"error": error_text} if error_text else {},
                    "created_ts_ms": 0,
                    "updated_ts_ms": now_ms(),
                }
            ],
            "source": "engine.cache.wrappers.kill_switch:provider_unavailable",
        }
    )


def _snapshot_for_cache(snapshot: dict[str, Any] | None, *, source: str = "engine.cache.wrappers.kill_switch:db") -> dict[str, Any]:
    out = dict(snapshot or {})
    state = out.get("state")
    out["state"] = list(state) if isinstance(state, list) else []
    out["loaded_ts_ms"] = int(now_ms())
    out["source"] = str(out.get("source") or source)
    out["max_age_ms"] = int(_max_age_ms())
    return out


def _snapshot_age_ms(snapshot: dict[str, Any], *, now: int | None = None) -> int | None:
    loaded_ts_ms = _safe_int(snapshot.get("loaded_ts_ms"), 0)
    if loaded_ts_ms <= 0:
        return None
    return max(0, int(now_ms() if now is None else now) - int(loaded_ts_ms))


def _freshness_budget_ms(snapshot: dict[str, Any]) -> int:
    stored = _safe_int(snapshot.get("max_age_ms"), 0)
    current = int(_max_age_ms())
    if stored <= 0:
        return 0
    return max(1, min(int(stored), int(current)))


def _is_fresh_snapshot(snapshot: dict[str, Any], *, now: int | None = None) -> bool:
    age_ms = _snapshot_age_ms(snapshot, now=now)
    if age_ms is None:
        return False
    return int(age_ms) <= int(_freshness_budget_ms(snapshot))


def _snapshot_is_provider_unavailable(snapshot: dict[str, Any] | None) -> bool:
    if not isinstance(snapshot, dict):
        return False
    if "provider_unavailable" in str(snapshot.get("source") or ""):
        return True
    for row in list(snapshot.get("state") or []):
        if not isinstance(row, dict):
            continue
        try:
            enabled = int(row.get("enabled") or 0) == 1
        except Exception:
            enabled = False
        if not enabled:
            continue
        scope = str(row.get("scope") or "").strip().lower()
        key = str(row.get("key") or "").strip().lower()
        reason = str(row.get("reason") or "").strip().lower()
        if scope == "global" and (key == "provider_unavailable" or reason == "kill_switch_provider_unavailable"):
            return True
    return False


def _snapshot_has_enabled_switch(snapshot: dict[str, Any] | None) -> bool:
    if not isinstance(snapshot, dict):
        return False
    if _snapshot_is_provider_unavailable(snapshot):
        return True
    for row in list(snapshot.get("state") or []):
        if not isinstance(row, dict):
            continue
        try:
            if int(row.get("enabled") or 0) == 1:
                return True
        except Exception:
            continue
    return False


def _non_live_mode_explicit() -> bool:
    mode = str(os.environ.get("ENGINE_MODE") or os.environ.get("EXECUTION_MODE") or "").strip().lower()
    return mode in {"safe", "paper", "shadow", "backtest", "test", "testing"}


def _l1_cacheable_snapshot(snapshot: dict[str, Any] | None) -> bool:
    # In live-possible contexts only fail-closed/blocking snapshots enter L1.
    # A stale blocking snapshot can only over-block; a stale clear snapshot could
    # miss a fresh operator hold, so clear snapshots require explicit non-live mode.
    return bool(_snapshot_has_enabled_switch(snapshot) or _non_live_mode_explicit())


def _l1_store_snapshot(key: str, snapshot: dict[str, Any] | None) -> None:
    if _l1_cacheable_snapshot(snapshot):
        l1_set(key, dict(snapshot or {"state": []}))
    else:
        l1_invalidate(key)


def _finalize_return_snapshot(
    snapshot: dict[str, Any] | None,
    *,
    read_source: str,
    cache_status: str,
) -> dict[str, Any]:
    out = dict(snapshot or {"state": []})
    state = out.get("state")
    out["state"] = list(state) if isinstance(state, list) else []
    if _snapshot_is_provider_unavailable(out):
        out["source"] = "engine.cache.wrappers.kill_switch:provider_unavailable"
    out["max_age_ms"] = int(_freshness_budget_ms(out) or _max_age_ms())
    age_ms = _snapshot_age_ms(out)
    out["cache_age_ms"] = age_ms
    out["cache_fresh"] = bool(age_ms is not None and int(age_ms) <= int(out["max_age_ms"]))
    out["read_source"] = str(read_source)
    out["cache_status"] = str(cache_status)
    out = annotate_effective_state(out, persisted_read_source=str(read_source))

    source = str(out.get("source") or "unknown")
    try:
        emit_gauge(
            "kill_switch_cache_age_ms",
            int(age_ms) if age_ms is not None else -1,
            component="engine.cache.wrappers.kill_switch",
            extra_tags={
                "source": source,
                "read_source": str(read_source),
                "cache_status": str(cache_status),
                "fresh": str(bool(out["cache_fresh"])).lower(),
            },
        )
        emit_counter(
            "kill_switch_cache_read_total",
            1,
            component="engine.cache.wrappers.kill_switch",
            extra_tags={
                "source": source,
                "read_source": str(read_source),
                "cache_status": str(cache_status),
            },
        )
    except Exception as exc:
        LOG.debug("KILL_SWITCH_CACHE_METRIC_EMIT_FAILED: %s", exc, exc_info=True)
    return out


def _encode_loaded_snapshot() -> bytes:
    return codec.encode(_snapshot_for_cache(_load_snapshot()), version=KILL_SWITCH_CODEC_VERSION)


def _reload_stale_snapshot(key: str, *, reason: str) -> dict[str, Any]:
    emit_counter(
        "kill_switch_cache_stale_reload_total",
        1,
        component="engine.cache.wrappers.kill_switch",
        extra_tags={"reason": str(reason)},
    )
    LOG.warning("KILL_SWITCH_CACHE_STALE: reloading key=%s reason=%s", key, reason)
    store.invalidate(key)
    snapshot = _snapshot_for_cache(_load_snapshot())
    try:
        store.prime(
            key,
            codec.encode(snapshot, version=KILL_SWITCH_CODEC_VERSION),
            ttl_s=_configured_ttl_s(),
        )
    except Exception as exc:
        LOG.warning("KILL_SWITCH_CACHE_PRIME_FAILED: key=%s reason=%s error=%s", key, reason, exc, exc_info=True)
    _l1_store_snapshot(key, snapshot)
    status = "stale_fail_closed" if _snapshot_is_provider_unavailable(snapshot) else "stale_reloaded"
    return _finalize_return_snapshot(snapshot, read_source="db_reload", cache_status=status)


def fail_closed_snapshot(error: BaseException | str | None = None) -> dict[str, Any]:
    """Public helper for callers that must materialize an unavailable provider block."""
    return _provider_unavailable_snapshot(error)


def kill_switch_cache_diagnostics() -> dict[str, Any]:
    """Return the current cached kill-switch snapshot with freshness metadata."""
    return read_kill_switch()


def refresh_kill_switch_cache() -> dict[str, Any]:
    """Reload the kill-switch snapshot from storage and re-prime Redis."""
    snapshot = _snapshot_for_cache(_load_snapshot())
    prime_kill_switch(snapshot)
    return _finalize_return_snapshot(
        snapshot,
        read_source="db_refresh",
        cache_status=("refresh_fail_closed" if _snapshot_is_provider_unavailable(snapshot) else "refreshed"),
    )


def _load_snapshot() -> dict[str, Any]:
    con = None
    try:
        con = storage.connect(readonly=True)
        rows = con.execute(
            """
            SELECT scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms
            FROM kill_switch_state
            ORDER BY scope, key
            """
        ).fetchall()
        return {"state": [_row_to_state(row) for row in rows or []], "source": "engine.cache.wrappers.kill_switch:db"}
    except Exception as exc:
        return _provider_unavailable_snapshot(exc)
    finally:
        if con is not None:
            con.close()


def read_kill_switch() -> dict[str, Any]:
    key = keys.kill_switch("snapshot")
    cached_l1 = l1_get(key)
    if isinstance(cached_l1, dict) and _is_fresh_snapshot(cached_l1):
        return _finalize_return_snapshot(cached_l1, read_source="l1", cache_status="fresh_l1")
    if cached_l1 is not None:
        l1_invalidate(key)

    loaded_from_loader = False

    def _loader() -> bytes:
        nonlocal loaded_from_loader
        loaded_from_loader = True
        return _encode_loaded_snapshot()

    ttl_s = _configured_ttl_s()
    raw = store.read(key, _loader, ttl_s=ttl_s)
    if raw is None:
        return _finalize_return_snapshot(
            _provider_unavailable_snapshot("kill_switch_cache_loader_unavailable"),
            read_source="loader_unavailable",
            cache_status="fail_closed",
        )
    try:
        data = codec.decode(raw, expected_version=KILL_SWITCH_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=ttl_s,
            wrapper=__name__,
            expected_version=KILL_SWITCH_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return _finalize_return_snapshot(
                _provider_unavailable_snapshot("kill_switch_cache_reload_unavailable"),
                read_source="codec_reload_unavailable",
                cache_status="fail_closed",
            )
        try:
            data = codec.decode(raw, expected_version=KILL_SWITCH_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            return _reload_stale_snapshot(key, reason="codec_reload_decode_failed")
    except codec.CacheCodecError:
        store.invalidate(key)
        return _reload_stale_snapshot(key, reason="codec_decode_failed")

    snapshot = dict(data or {"state": []})
    if not _is_fresh_snapshot(snapshot):
        reason = "missing_loaded_ts_ms" if _snapshot_age_ms(snapshot) is None else "expired_loaded_ts_ms"
        return _reload_stale_snapshot(key, reason=reason)
    _l1_store_snapshot(key, snapshot)
    return _finalize_return_snapshot(
        snapshot,
        read_source=("db_load" if loaded_from_loader else "redis"),
        cache_status=("loaded" if loaded_from_loader else "fresh"),
    )


def prime_kill_switch(snapshot: dict[str, Any] | None = None) -> None:
    key = keys.kill_switch("snapshot")
    payload = _snapshot_for_cache(snapshot or _load_snapshot())
    l1_invalidate(key)
    store.prime(
        key,
        codec.encode(payload, version=KILL_SWITCH_CODEC_VERSION),
        ttl_s=_configured_ttl_s(),
    )
    _l1_store_snapshot(key, payload)


def invalidate_kill_switch() -> None:
    key = keys.kill_switch("snapshot")
    l1_invalidate(key)
    store.invalidate(key)


def _ensure_schema(con: Any) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS kill_switch_state (
          scope TEXT NOT NULL,
          key TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 0,
          reason TEXT,
          actor TEXT NOT NULL DEFAULT 'system',
          meta_json TEXT,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL,
          PRIMARY KEY (scope, key)
        );

        CREATE TABLE IF NOT EXISTS kill_switch_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          action TEXT NOT NULL,
          scope TEXT NOT NULL,
          key TEXT NOT NULL,
          enabled INTEGER NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT,
          meta_json TEXT,
          prev_hash BLOB,
          row_hash BLOB NOT NULL
        );
        """
    )


def set_kill_switch(
    state: bool | int | dict[str, Any],
    reason: str = "",
    actor: str = "system",
    *,
    scope: str = "global",
    key: str = "global",
    meta: dict[str, Any] | None = None,
    action: str = "SET",
    con: Any | None = None,
) -> dict[str, Any]:
    payload = dict(state) if isinstance(state, dict) else {}
    scope_n = _normalize_scope(str(payload.get("scope") or scope))
    key_n = _normalize_key(scope_n, str(payload.get("key") or key))
    enabled = int(payload.get("enabled") if "enabled" in payload else (1 if bool(state) else 0))
    enabled = 1 if enabled else 0
    ts_ms = int(payload.get("updated_ts_ms") or now_ms())
    reason_s = str(payload.get("reason") if "reason" in payload else reason or "")
    actor_s = str(payload.get("actor") if "actor" in payload else actor or "system")
    meta_payload = dict(payload.get("meta") or meta or {})
    meta_json = dumps_json(meta_payload)
    row = {
        "scope": scope_n,
        "key": key_n,
        "enabled": int(enabled),
        "reason": reason_s,
        "actor": actor_s,
        "meta": meta_payload,
        "created_ts_ms": int(ts_ms),
        "updated_ts_ms": int(ts_ms),
    }

    def _persist(db: Any) -> None:
        _ensure_schema(db)
        current = db.execute(
            "SELECT created_ts_ms FROM kill_switch_state WHERE scope=? AND key=?",
            (scope_n, key_n),
        ).fetchone()
        created_ts_ms = int((current[0] if current else ts_ms) or ts_ms)
        db.execute(
            """
            INSERT INTO kill_switch_state(scope, key, enabled, reason, actor, meta_json, created_ts_ms, updated_ts_ms)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(scope, key) DO UPDATE SET
              enabled=excluded.enabled,
              reason=excluded.reason,
              actor=excluded.actor,
              meta_json=excluded.meta_json,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (scope_n, key_n, enabled, reason_s, actor_s, meta_json, created_ts_ms, ts_ms),
        )
        append_chain_row(
            "kill_switch_audit",
            {
                "ts_ms": int(ts_ms),
                "action": str(action or "SET").upper(),
                "scope": scope_n,
                "key": key_n,
                "enabled": int(enabled),
                "actor": actor_s,
                "reason": reason_s,
                "meta_json": meta_json,
            },
            db,
        )

    if con is not None:
        _persist(con)
        after_commit_or_now(con, prime_kill_switch)
    else:
        key = keys.kill_switch("snapshot")
        l1_invalidate(key)
        store.write_through(
            key,
            _encode_loaded_snapshot,
            persist=_persist,
            ttl_s=_configured_ttl_s(),
        )
        l1_invalidate(key)
    return row
