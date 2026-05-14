"""Redis-cached wrapper for ``broker_order_state``."""

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

BROKER_ORDER_STATE_CODEC_VERSION = 1
BROKER_ORDER_STATE_TTL_S = 300


def _identifier(source_order_id: Any = None, symbol: Any = None, row_id: Any = None) -> str:
    if row_id not in (None, ""):
        return f"id:{int(row_id)}"
    if source_order_id not in (None, "") and str(symbol or "").strip():
        return f"source:{int(source_order_id)}:{str(symbol).upper().strip()}"
    if str(symbol or "").strip():
        return f"latest:{str(symbol).upper().strip()}"
    raise ValueError("broker_order_state_identifier_required")


def _row_to_order(row: Any) -> dict[str, Any]:
    return {
        "id": int(row[0] or 0),
        "source_order_id": int(row[1] or 0) if row[1] is not None else None,
        "symbol": str(row[2] or "").upper().strip(),
        "state": str(row[3] or ""),
        "created_ts_ms": int(row[4] or 0),
        "updated_ts_ms": int(row[5] or 0),
        "ttl_ms": int(row[6] or 0) if row[6] is not None else None,
        "meta": parse_json(row[7], {}),
    }


def _load_order(*, source_order_id: Any = None, symbol: Any = None, row_id: Any = None) -> dict[str, Any] | None:
    con = None
    try:
        con = storage.connect(readonly=True)
        if row_id not in (None, ""):
            row = con.execute(
                """
                SELECT id, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json
                FROM broker_order_state
                WHERE id=?
                LIMIT 1
                """,
                (int(row_id),),
            ).fetchone()
        elif source_order_id not in (None, "") and str(symbol or "").strip():
            row = con.execute(
                """
                SELECT id, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json
                FROM broker_order_state
                WHERE source_order_id=? AND symbol=?
                ORDER BY updated_ts_ms DESC, id DESC
                LIMIT 1
                """,
                (int(source_order_id), str(symbol).upper().strip()),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT id, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json
                FROM broker_order_state
                WHERE symbol=?
                ORDER BY updated_ts_ms DESC, id DESC
                LIMIT 1
                """,
                (str(symbol).upper().strip(),),
            ).fetchone()
        return _row_to_order(row) if row else None
    except Exception:
        return None
    finally:
        if con is not None:
            con.close()


def read_broker_order_state(
    *,
    source_order_id: Any = None,
    symbol: Any = None,
    row_id: Any = None,
) -> dict[str, Any] | None:
    ident = _identifier(source_order_id=source_order_id, symbol=symbol, row_id=row_id)
    key = keys.broker_order_state(ident)

    def _loader() -> bytes | None:
        row = _load_order(source_order_id=source_order_id, symbol=symbol, row_id=row_id)
        return codec.encode(row, version=BROKER_ORDER_STATE_CODEC_VERSION) if row is not None else None

    raw = store.read(key, _loader, ttl_s=BROKER_ORDER_STATE_TTL_S)
    if raw is None:
        return None
    try:
        data = codec.decode(raw, expected_version=BROKER_ORDER_STATE_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=BROKER_ORDER_STATE_TTL_S,
            wrapper=__name__,
            expected_version=BROKER_ORDER_STATE_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return None
        try:
            data = codec.decode(raw, expected_version=BROKER_ORDER_STATE_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            return _load_order(source_order_id=source_order_id, symbol=symbol, row_id=row_id)
    except codec.CacheCodecError:
        store.invalidate(key)
        return _load_order(source_order_id=source_order_id, symbol=symbol, row_id=row_id)
    return dict(data or {})


def prime_broker_order_state(row: dict[str, Any]) -> None:
    if not row:
        return
    for key, encoded in _encoded_entries(row).items():
        store.prime(key, encoded, ttl_s=BROKER_ORDER_STATE_TTL_S)


def _encoded_entries(row: dict[str, Any]) -> dict[str, bytes]:
    if not row:
        return {}
    identifiers = []
    if row.get("id") not in (None, ""):
        identifiers.append(_identifier(row_id=row.get("id")))
    if row.get("source_order_id") not in (None, "") and row.get("symbol"):
        identifiers.append(_identifier(source_order_id=row.get("source_order_id"), symbol=row.get("symbol")))
    if row.get("symbol"):
        identifiers.append(_identifier(symbol=row.get("symbol")))
    encoded = codec.encode(row, version=BROKER_ORDER_STATE_CODEC_VERSION)
    return {keys.broker_order_state(ident): encoded for ident in identifiers}


def _ensure_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_order_state (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_order_id INTEGER,
          symbol TEXT NOT NULL,
          state TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL,
          ttl_ms INTEGER,
          meta_json TEXT
        )
        """
    )


def set_broker_order_state(
    *,
    symbol: str,
    state: str,
    source_order_id: int | None = None,
    row_id: int | None = None,
    ttl_ms: int | None = None,
    meta: dict[str, Any] | None = None,
    con: Any | None = None,
) -> dict[str, Any]:
    ts_ms = now_ms()
    payload = {
        "id": row_id,
        "source_order_id": source_order_id,
        "symbol": str(symbol or "").upper().strip(),
        "state": str(state or ""),
        "created_ts_ms": int(ts_ms),
        "updated_ts_ms": int(ts_ms),
        "ttl_ms": (int(ttl_ms) if ttl_ms is not None else None),
        "meta": dict(meta or {}),
    }
    def _persist(db: Any) -> None:
        _ensure_schema(db)
        if row_id is not None:
            existing = db.execute("SELECT created_ts_ms FROM broker_order_state WHERE id=?", (int(row_id),)).fetchone()
            created = int((existing[0] if existing else ts_ms) or ts_ms)
            db.execute(
                """
                INSERT INTO broker_order_state(id, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(id) DO UPDATE SET
                  source_order_id=excluded.source_order_id,
                  symbol=excluded.symbol,
                  state=excluded.state,
                  updated_ts_ms=excluded.updated_ts_ms,
                  ttl_ms=excluded.ttl_ms,
                  meta_json=excluded.meta_json
                """,
                (
                    int(row_id),
                    source_order_id,
                    payload["symbol"],
                    payload["state"],
                    int(created),
                    int(ts_ms),
                    ttl_ms,
                    dumps_json(payload["meta"]),
                ),
            )
            payload["created_ts_ms"] = int(created)
            return
        cur = db.execute(
            """
            INSERT INTO broker_order_state(source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json)
            VALUES(?,?,?,?,?,?,?)
            """,
            (
                source_order_id,
                payload["symbol"],
                payload["state"],
                int(ts_ms),
                int(ts_ms),
                ttl_ms,
                dumps_json(payload["meta"]),
            ),
        )
        lastrowid = getattr(cur, "lastrowid", None)
        if lastrowid not in (None, ""):
            try:
                payload["id"] = int(lastrowid)
            except Exception:
                payload["id"] = lastrowid
        if payload.get("id") in (None, ""):
            source_clause = "source_order_id IS NULL" if source_order_id is None else "source_order_id=?"
            args = (payload["symbol"],) if source_order_id is None else (source_order_id, payload["symbol"])
            row = db.execute(
                f"""
                SELECT id, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json
                FROM broker_order_state
                WHERE {source_clause} AND symbol=?
                ORDER BY updated_ts_ms DESC, id DESC
                LIMIT 1
                """,
                args,
            ).fetchone()
            if row:
                payload.update(_row_to_order(row))

    if con is not None:
        _persist(con)
        after_commit_or_now(con, lambda: prime_broker_order_state(payload))
    else:
        store.write_through_many(lambda: _encoded_entries(payload), persist=_persist, ttl_s=BROKER_ORDER_STATE_TTL_S)
    return payload
