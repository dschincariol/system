"""Redis-cached wrapper for ``kill_switch_state``."""

from __future__ import annotations

from typing import Any

from engine.audit.chain import append_chain_row
from engine.cache import codec, keys, store
from engine.cache.wrappers._common import (
    after_commit_or_now,
    dumps_json,
    now_ms,
    parse_json,
    reload_after_codec_version_mismatch,
)
from engine.runtime import storage

KILL_SWITCH_CODEC_VERSION = 1
KILL_SWITCH_TTL_S: int | None = None


def _normalize_scope(scope: str) -> str:
    value = str(scope or "").strip().lower()
    if value not in {"global", "symbol", "regime", "model"}:
        raise ValueError(f"invalid_kill_switch_scope:{scope}")
    return value


def _normalize_key(scope: str, key: str) -> str:
    value = str(key or "").strip()
    if not value:
        raise ValueError("kill_switch_key_required")
    return "global" if scope == "global" else value


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
        return {"state": [_row_to_state(row) for row in rows or []]}
    except Exception:
        return {"state": []}
    finally:
        if con is not None:
            con.close()


def read_kill_switch() -> dict[str, Any]:
    key = keys.kill_switch("snapshot")

    def _loader() -> bytes:
        return codec.encode(_load_snapshot(), version=KILL_SWITCH_CODEC_VERSION)

    raw = store.read(key, _loader, ttl_s=KILL_SWITCH_TTL_S)
    if raw is None:
        return {"state": []}
    try:
        data = codec.decode(raw, expected_version=KILL_SWITCH_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=KILL_SWITCH_TTL_S,
            wrapper=__name__,
            expected_version=KILL_SWITCH_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return {"state": []}
        try:
            data = codec.decode(raw, expected_version=KILL_SWITCH_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            return _load_snapshot()
    except codec.CacheCodecError:
        store.invalidate(key)
        return _load_snapshot()
    return dict(data or {"state": []})


def prime_kill_switch(snapshot: dict[str, Any] | None = None) -> None:
    store.prime(
        keys.kill_switch("snapshot"),
        codec.encode(snapshot or _load_snapshot(), version=KILL_SWITCH_CODEC_VERSION),
        ttl_s=KILL_SWITCH_TTL_S,
    )


def invalidate_kill_switch() -> None:
    store.invalidate(keys.kill_switch("snapshot"))


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
        store.write_through(
            keys.kill_switch("snapshot"),
            lambda: codec.encode(_load_snapshot(), version=KILL_SWITCH_CODEC_VERSION),
            persist=_persist,
            ttl_s=KILL_SWITCH_TTL_S,
        )
    return row
