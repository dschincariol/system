"""Redis-cached wrapper for ``strategy_allocations``."""

from __future__ import annotations

from typing import Any

from engine.cache import codec, keys, store
from engine.cache.wrappers._common import (
    after_commit_or_now,
    dumps_json,
    now_ms,
    parse_json,
    reload_after_codec_version_mismatch,
)
from engine.runtime import storage

STRATEGY_ALLOCATIONS_CODEC_VERSION = 1
STRATEGY_ALLOCATIONS_TTL_S = 300


def _row_to_alloc(row: Any) -> dict[str, Any]:
    return {
        "ts_ms": int(row[0] or 0),
        "window_days": int(row[1] or 0),
        "allocations": parse_json(row[2], {}),
        "reason": parse_json(row[3], {}),
    }


def _load_latest(window_days: int = 0) -> dict[str, Any] | None:
    con = None
    try:
        con = storage.connect(readonly=True)
        row = con.execute(
            """
            SELECT ts_ms, window_days, allocations_json, reason_json
            FROM strategy_allocations
            WHERE window_days=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (int(window_days),),
        ).fetchone()
        return _row_to_alloc(row) if row else None
    except Exception:
        return None
    finally:
        if con is not None:
            con.close()


def read_strategy_allocations(window_days: int = 0) -> dict[str, Any] | None:
    wd = int(window_days)
    key = keys.strategy_allocations(wd)

    def _loader() -> bytes | None:
        row = _load_latest(wd)
        return codec.encode(row, version=STRATEGY_ALLOCATIONS_CODEC_VERSION) if row is not None else None

    raw = store.read(key, _loader, ttl_s=STRATEGY_ALLOCATIONS_TTL_S)
    if raw is None:
        return None
    try:
        data = codec.decode(raw, expected_version=STRATEGY_ALLOCATIONS_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=STRATEGY_ALLOCATIONS_TTL_S,
            wrapper=__name__,
            expected_version=STRATEGY_ALLOCATIONS_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return None
        try:
            data = codec.decode(raw, expected_version=STRATEGY_ALLOCATIONS_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            return _load_latest(wd)
    except codec.CacheCodecError:
        store.invalidate(key)
        return _load_latest(wd)
    return dict(data or {})


def prime_strategy_allocations(row: dict[str, Any]) -> None:
    window_days = int((row or {}).get("window_days") or 0)
    store.prime(
        keys.strategy_allocations(window_days),
        codec.encode(row, version=STRATEGY_ALLOCATIONS_CODEC_VERSION),
        ttl_s=STRATEGY_ALLOCATIONS_TTL_S,
    )


def _ensure_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_allocations (
          ts_ms INTEGER NOT NULL,
          window_days INTEGER NOT NULL,
          allocations_json TEXT NOT NULL,
          reason_json TEXT,
          PRIMARY KEY(ts_ms, window_days)
        )
        """
    )


def set_strategy_allocations(
    allocations: dict[str, Any],
    *,
    reason: dict[str, Any] | None = None,
    ts_ms: int | None = None,
    window_days: int = 0,
    con: Any | None = None,
) -> dict[str, Any]:
    payload = {
        "ts_ms": int(ts_ms or now_ms()),
        "window_days": int(window_days),
        "allocations": dict(allocations or {}),
        "reason": dict(reason or {}),
    }

    def _persist(db: Any) -> None:
        _ensure_schema(db)
        db.execute(
            """
            INSERT INTO strategy_allocations(ts_ms, window_days, allocations_json, reason_json)
            VALUES(?,?,?,?)
            ON CONFLICT(ts_ms, window_days) DO UPDATE SET
              allocations_json=excluded.allocations_json,
              reason_json=excluded.reason_json
            """,
            (
                int(payload["ts_ms"]),
                int(payload["window_days"]),
                dumps_json(payload["allocations"]),
                dumps_json(payload["reason"]),
            ),
        )

    if con is not None:
        _persist(con)
        after_commit_or_now(con, lambda: prime_strategy_allocations(payload))
    else:
        store.write_through(
            keys.strategy_allocations(int(window_days)),
            codec.encode(payload, version=STRATEGY_ALLOCATIONS_CODEC_VERSION),
            persist=_persist,
            ttl_s=STRATEGY_ALLOCATIONS_TTL_S,
        )
    return payload
