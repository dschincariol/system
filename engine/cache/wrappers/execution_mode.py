"""Redis-cached wrapper for ``execution_mode``."""

from __future__ import annotations

from typing import Any

from engine.audit.chain import append_chain_row
from engine.cache import codec, keys, store
from engine.cache.wrappers._common import (
    after_commit_or_now,
    l1_get,
    l1_invalidate,
    l1_set,
    now_ms,
    reload_after_codec_version_mismatch,
)
from engine.execution.mode_safety import CANONICAL_EXECUTION_MODES, coerce_execution_mode
from engine.runtime import storage

MODES = set(CANONICAL_EXECUTION_MODES)
EXECUTION_MODE_CODEC_VERSION = 1
EXECUTION_MODE_TTL_S = 3600


def _row_to_mode(row: Any) -> dict[str, Any]:
    return {
        "mode": str(row[0] or "paper"),
        "armed": int(row[1] or 0),
        "updated_ts_ms": int(row[2] or 0),
        "actor": str(row[3] or ""),
        "reason": str(row[4] or ""),
    }


def _load_mode() -> dict[str, Any]:
    con = None
    try:
        con = storage.connect(readonly=True)
        row = con.execute(
            "SELECT mode, armed, updated_ts_ms, actor, reason FROM execution_mode WHERE id=1"
        ).fetchone()
        if not row:
            return {"mode": "paper", "armed": 0, "updated_ts_ms": 0, "actor": "system", "reason": "missing"}
        return _row_to_mode(row)
    except Exception:
        return {"mode": "paper", "armed": 0, "updated_ts_ms": 0, "actor": "system", "reason": "unavailable"}
    finally:
        if con is not None:
            con.close()


def _l1_cacheable_mode(state: dict[str, Any] | None) -> bool:
    payload = dict(state or {})
    mode = str(payload.get("mode") or "paper").strip().lower()
    armed = int(payload.get("armed") or 0)
    return not (mode == "live" and armed == 1)


def _l1_store_mode(key: str, state: dict[str, Any] | None) -> None:
    if _l1_cacheable_mode(state):
        l1_set(key, dict(state or {}))
    else:
        l1_invalidate(key)


def read_execution_mode() -> dict[str, Any]:
    key = keys.execution_mode()
    cached_l1 = l1_get(key)
    if isinstance(cached_l1, dict):
        return dict(cached_l1)

    def _loader() -> bytes:
        return codec.encode(_load_mode(), version=EXECUTION_MODE_CODEC_VERSION)

    raw = store.read(key, _loader, ttl_s=EXECUTION_MODE_TTL_S)
    if raw is None:
        state = _load_mode()
        _l1_store_mode(key, state)
        return state
    try:
        state = dict(codec.decode(raw, expected_version=EXECUTION_MODE_CODEC_VERSION) or {})
        _l1_store_mode(key, state)
        return state
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=EXECUTION_MODE_TTL_S,
            wrapper=__name__,
            expected_version=EXECUTION_MODE_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            state = _load_mode()
            _l1_store_mode(key, state)
            return state
        try:
            state = dict(codec.decode(raw, expected_version=EXECUTION_MODE_CODEC_VERSION) or {})
            _l1_store_mode(key, state)
            return state
        except codec.CacheCodecError:
            store.invalidate(key)
            state = _load_mode()
            _l1_store_mode(key, state)
            return state
    except codec.CacheCodecError:
        store.invalidate(key)
        state = _load_mode()
        _l1_store_mode(key, state)
        return state


def prime_execution_mode(state: dict[str, Any] | None = None) -> None:
    key = keys.execution_mode()
    payload = dict(state or _load_mode())
    l1_invalidate(key)
    store.prime(
        key,
        codec.encode(payload, version=EXECUTION_MODE_CODEC_VERSION),
        ttl_s=EXECUTION_MODE_TTL_S,
    )
    _l1_store_mode(key, payload)


def _ensure_schema(con: Any) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS execution_mode (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          mode TEXT NOT NULL,
          armed INTEGER NOT NULL DEFAULT 0,
          updated_ts_ms INTEGER NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT
        );

        CREATE TABLE IF NOT EXISTS execution_mode_audit (
          ts_ms INTEGER NOT NULL,
          prev_mode TEXT NOT NULL,
          new_mode TEXT NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT,
          prev_armed INTEGER,
          new_armed INTEGER,
          prev_hash BLOB,
          row_hash BLOB NOT NULL
        );
        """
    )


def _assert_live_arming_confirmation(mode: str, armed: int) -> None:
    if str(mode or "").strip().lower() == "live" and int(armed or 0) == 1:
        from engine.runtime.live_trading_preflight import assert_live_execution_arming_preflight

        assert_live_execution_arming_preflight(engine_mode="live")


def set_execution_mode(
    mode: str,
    *,
    actor: str = "operator",
    reason: str = "",
    armed: int | None = None,
    con: Any | None = None,
) -> dict[str, Any]:
    mode_n = coerce_execution_mode(mode, source="execution_mode_cache")
    actor_s = str(actor or "operator")
    reason_s = str(reason or "")
    ts_ms = now_ms()
    current = read_execution_mode()
    new_armed = int(current.get("armed") or 0) if armed is None else int(armed or 0)
    if mode_n != "live":
        new_armed = 0
    _assert_live_arming_confirmation(mode_n, new_armed)
    state = {
        "mode": mode_n,
        "armed": int(new_armed),
        "updated_ts_ms": int(ts_ms),
        "actor": actor_s,
        "reason": reason_s,
    }

    def _persist(db: Any) -> None:
        _ensure_schema(db)
        prev = db.execute("SELECT mode, armed FROM execution_mode WHERE id=1").fetchone()
        prev_mode = str(prev[0] or "paper") if prev else "paper"
        prev_armed = int(prev[1] or 0) if prev else 0
        db.execute(
            """
            INSERT INTO execution_mode(id, mode, armed, updated_ts_ms, actor, reason)
            VALUES(1,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              mode=excluded.mode,
              armed=excluded.armed,
              updated_ts_ms=excluded.updated_ts_ms,
              actor=excluded.actor,
              reason=excluded.reason
            """,
            (mode_n, int(new_armed), int(ts_ms), actor_s, reason_s),
        )
        append_chain_row(
            "execution_mode_audit",
            {
                "ts_ms": int(ts_ms),
                "prev_mode": prev_mode,
                "new_mode": mode_n,
                "actor": actor_s,
                "reason": reason_s,
                "prev_armed": int(prev_armed),
                "new_armed": int(new_armed),
            },
            db,
        )

    if con is not None:
        _persist(con)
        after_commit_or_now(con, lambda: prime_execution_mode(state))
    else:
        key = keys.execution_mode()
        l1_invalidate(key)
        store.write_through(
            key,
            codec.encode(state, version=EXECUTION_MODE_CODEC_VERSION),
            persist=_persist,
            ttl_s=EXECUTION_MODE_TTL_S,
        )
        _l1_store_mode(key, state)
    return state


def set_execution_armed(armed: int, *, actor: str = "operator", reason: str = "", con: Any | None = None) -> dict[str, Any]:
    current = read_execution_mode()
    return set_execution_mode(
        str(current.get("mode") or "paper"),
        actor=actor,
        reason=reason,
        armed=(1 if int(armed) == 1 else 0),
        con=con,
    )
