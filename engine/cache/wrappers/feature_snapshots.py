"""Redis-cached wrapper for latest ``model_feature_snapshots`` rows."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from engine.cache import codec, keys, store
from engine.cache.wrappers._common import (
    after_commit_or_now,
    dumps_json,
    l1_get,
    l1_invalidate,
    l1_is_missing,
    l1_set,
    l1_set_missing,
    parse_json,
    reload_after_codec_version_mismatch,
)
from engine.runtime import storage

FEATURE_SNAPSHOT_CODEC_VERSION = 1
FEATURE_SNAPSHOT_TTL_S = 300


def _load_result(row: dict[str, Any] | None, *, miss_safe: bool) -> tuple[dict[str, Any] | None, bool]:
    return row, bool(miss_safe)


def _row_to_snapshot(row: Any) -> dict[str, Any]:
    return {
        "symbol": str(row[0] or "").upper().strip(),
        "ts_ms": int(row[1] or 0),
        "feature_set_tag": str(row[2] or ""),
        "snapshot_version": int(row[3] or 1),
        "feature_ids": parse_json(row[4], []),
        "vector": parse_json(row[5], []),
        "features": parse_json(row[6], {}),
        "source_timestamps": parse_json(row[7], {}),
        "availability": parse_json(row[8], {}),
        "created_ts_ms": int(row[9] or 0),
    }


def _load_latest_result(symbol: str, feature_group: str) -> tuple[dict[str, Any] | None, bool]:
    symbol_key = str(symbol or "").upper().strip()
    group = str(feature_group or "").strip()
    con = None
    try:
        con = storage.connect(readonly=True)
        row = con.execute(
            """
            SELECT
              symbol,
              ts_ms,
              feature_set_tag,
              snapshot_version,
              feature_ids_json,
              vector_json,
              features_json,
              source_timestamps_json,
              availability_json,
              created_ts_ms
            FROM model_feature_snapshots
            WHERE symbol = ?
              AND feature_set_tag = ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol_key, group),
        ).fetchone()
        return _load_result(_row_to_snapshot(row) if row else None, miss_safe=True)
    except Exception:
        return _load_result(None, miss_safe=False)
    finally:
        if con is not None:
            con.close()


def _load_latest(symbol: str, feature_group: str) -> dict[str, Any] | None:
    row, _miss_safe = _load_latest_result(symbol, feature_group)
    return row


def _load_latest_many_result(symbols: Iterable[str], feature_group: str) -> tuple[dict[str, dict[str, Any]], bool]:
    symbol_keys = list(
        dict.fromkeys(
            str(symbol or "").upper().strip()
            for symbol in list(symbols or [])
            if str(symbol or "").strip()
        )
    )
    group = str(feature_group or "").strip()
    if not symbol_keys or not group:
        return {}, True

    con = None
    try:
        con = storage.connect(readonly=True)
        placeholders = ",".join("?" for _ in symbol_keys)
        rows = con.execute(
            f"""
            SELECT
              symbol,
              ts_ms,
              feature_set_tag,
              snapshot_version,
              feature_ids_json,
              vector_json,
              features_json,
              source_timestamps_json,
              availability_json,
              created_ts_ms
            FROM (
              SELECT
                symbol,
                ts_ms,
                feature_set_tag,
                snapshot_version,
                feature_ids_json,
                vector_json,
                features_json,
                source_timestamps_json,
                availability_json,
                created_ts_ms,
                ROW_NUMBER() OVER (
                  PARTITION BY symbol, feature_set_tag
                  ORDER BY ts_ms DESC
                ) AS rn
              FROM model_feature_snapshots
              WHERE feature_set_tag = ?
                AND symbol IN ({placeholders})
            ) ranked
            WHERE rn = 1
            """,
            (group, *symbol_keys),
        ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows or []:
            payload = _row_to_snapshot(row)
            symbol = str(payload.get("symbol") or "").upper().strip()
            if symbol:
                out[symbol] = payload
        return out, True
    except Exception:
        return {}, False
    finally:
        if con is not None:
            con.close()


def _load_latest_many(symbols: Iterable[str], feature_group: str) -> dict[str, dict[str, Any]]:
    rows, _miss_safe = _load_latest_many_result(symbols, feature_group)
    return rows


def _decode_snapshot_raw(
    key: str,
    raw: bytes | bytearray | memoryview | str | None,
    *,
    symbol: str,
    feature_group: str,
) -> dict[str, Any] | None:
    if raw is None:
        return None
    try:
        data = codec.decode(raw, expected_version=FEATURE_SNAPSHOT_CODEC_VERSION)
    except codec.UnsupportedCacheVersion as exc:
        def _loader() -> bytes | None:
            row = _load_latest(symbol, feature_group)
            return codec.encode(row, version=FEATURE_SNAPSHOT_CODEC_VERSION) if row is not None else None

        raw = reload_after_codec_version_mismatch(
            key,
            _loader,
            ttl_s=FEATURE_SNAPSHOT_TTL_S,
            wrapper=__name__,
            expected_version=FEATURE_SNAPSHOT_CODEC_VERSION,
            error=exc,
        )
        if raw is None:
            return None
        try:
            data = codec.decode(raw, expected_version=FEATURE_SNAPSHOT_CODEC_VERSION)
        except codec.CacheCodecError:
            store.invalidate(key)
            l1_invalidate(key)
            return _load_latest(symbol, feature_group)
    except codec.CacheCodecError:
        store.invalidate(key)
        l1_invalidate(key)
        return _load_latest(symbol, feature_group)
    return dict(data or {})


def latest(symbol: str, feature_group: str) -> dict[str, Any] | None:
    symbol_key = str(symbol or "").upper().strip()
    group = str(feature_group or "").strip()
    key = keys.feature_snapshot(symbol_key, group)
    cached_l1 = l1_get(key)
    if isinstance(cached_l1, dict):
        return dict(cached_l1)
    if l1_is_missing(cached_l1):
        return None

    miss_cacheable = {"value": False}

    def _loader() -> bytes | None:
        row, miss_safe = _load_latest_result(symbol_key, group)
        miss_cacheable["value"] = bool(row is None and miss_safe)
        return codec.encode(row, version=FEATURE_SNAPSHOT_CODEC_VERSION) if row is not None else None

    raw = store.read(key, _loader, ttl_s=FEATURE_SNAPSHOT_TTL_S)
    snapshot = _decode_snapshot_raw(key, raw, symbol=symbol_key, feature_group=group)
    if snapshot is not None:
        l1_set(key, snapshot)
    elif miss_cacheable["value"]:
        l1_set_missing(key)
    return snapshot


def latest_many(symbols: Iterable[str], feature_group: str) -> dict[str, dict[str, Any] | None]:
    group = str(feature_group or "").strip()
    symbol_keys = list(
        dict.fromkeys(
            str(symbol or "").upper().strip()
            for symbol in list(symbols or [])
            if str(symbol or "").strip()
        )
    )
    if not symbol_keys or not group:
        return {}

    key_by_symbol = {symbol: keys.feature_snapshot(symbol, group) for symbol in symbol_keys}
    symbol_by_key = {cache_key: symbol for symbol, cache_key in key_by_symbol.items()}
    out: dict[str, dict[str, Any] | None] = {}
    missing_symbols: list[str] = []
    for symbol, key in key_by_symbol.items():
        cached_l1 = l1_get(key)
        if isinstance(cached_l1, dict):
            out[symbol] = dict(cached_l1)
        elif l1_is_missing(cached_l1):
            out[symbol] = None
        else:
            missing_symbols.append(symbol)
    if not missing_symbols:
        return out

    negative_cacheable_keys: set[str] = set()

    def _loader(missing_keys: list[str]) -> Mapping[str, bytes | None]:
        missing_symbols = [symbol_by_key[key] for key in list(missing_keys or []) if key in symbol_by_key]
        loaded, miss_safe = _load_latest_many_result(missing_symbols, group)
        if miss_safe:
            loaded_symbols = {str(symbol or "").upper().strip() for symbol in loaded}
            for symbol in missing_symbols:
                if symbol not in loaded_symbols:
                    negative_cacheable_keys.add(key_by_symbol[symbol])
        return {
            key_by_symbol[symbol]: codec.encode(row, version=FEATURE_SNAPSHOT_CODEC_VERSION)
            for symbol, row in loaded.items()
            if symbol in key_by_symbol and row is not None
        }

    missing_keys = [key_by_symbol[symbol] for symbol in missing_symbols]
    raw_by_key = store.read_many(missing_keys, _loader, ttl_s=FEATURE_SNAPSHOT_TTL_S)
    for symbol in missing_symbols:
        key = key_by_symbol[symbol]
        snapshot = _decode_snapshot_raw(
            key,
            raw_by_key.get(key),
            symbol=symbol,
            feature_group=group,
        )
        out[symbol] = snapshot
        if snapshot is not None:
            l1_set(key, snapshot)
        elif key in negative_cacheable_keys:
            l1_set_missing(key)
    return out


def prime_feature_snapshot(snapshot: dict[str, Any]) -> None:
    symbol = str((snapshot or {}).get("symbol") or "").upper().strip()
    group = str((snapshot or {}).get("feature_set_tag") or "").strip()
    if symbol and group:
        key = keys.feature_snapshot(symbol, group)
        l1_invalidate(key)
        store.prime(
            key,
            codec.encode(snapshot, version=FEATURE_SNAPSHOT_CODEC_VERSION),
            ttl_s=FEATURE_SNAPSHOT_TTL_S,
        )
        l1_set(key, dict(snapshot))


def _ensure_schema(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_feature_snapshots (
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          feature_set_tag TEXT NOT NULL,
          snapshot_version INTEGER NOT NULL,
          feature_ids_json TEXT NOT NULL,
          vector_json TEXT NOT NULL,
          features_json TEXT NOT NULL,
          source_timestamps_json TEXT NOT NULL,
          availability_json TEXT NOT NULL,
          created_ts_ms INTEGER NOT NULL,
          PRIMARY KEY(symbol, ts_ms, feature_set_tag)
        )
        """
    )


def store_latest(snapshot: dict[str, Any], *, con: Any | None = None) -> dict[str, Any]:
    payload = dict(snapshot or {})
    payload["symbol"] = str(payload.get("symbol") or "").upper().strip()
    payload["feature_set_tag"] = str(payload.get("feature_set_tag") or "").strip()
    if not payload["symbol"] or not payload["feature_set_tag"]:
        raise ValueError("feature_snapshot_symbol_and_group_required")

    def _persist(db: Any) -> None:
        _ensure_schema(db)
        db.execute(
            """
            INSERT INTO model_feature_snapshots(
              symbol, ts_ms, feature_set_tag, snapshot_version,
              feature_ids_json, vector_json, features_json,
              source_timestamps_json, availability_json, created_ts_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, ts_ms, feature_set_tag) DO UPDATE SET
              snapshot_version=excluded.snapshot_version,
              feature_ids_json=excluded.feature_ids_json,
              vector_json=excluded.vector_json,
              features_json=excluded.features_json,
              source_timestamps_json=excluded.source_timestamps_json,
              availability_json=excluded.availability_json,
              created_ts_ms=excluded.created_ts_ms
            """,
            (
                payload["symbol"],
                int(payload.get("ts_ms") or 0),
                payload["feature_set_tag"],
                int(payload.get("snapshot_version") or 1),
                dumps_json(list(payload.get("feature_ids") or [])),
                dumps_json(list(payload.get("vector") or [])),
                dumps_json(dict(payload.get("features") or {})),
                dumps_json(dict(payload.get("source_timestamps") or {})),
                dumps_json(dict(payload.get("availability") or {})),
                int(payload.get("created_ts_ms") or 0),
            ),
        )

    if con is not None:
        _persist(con)
        after_commit_or_now(con, lambda: prime_feature_snapshot(payload))
    else:
        key = keys.feature_snapshot(payload["symbol"], payload["feature_set_tag"])
        l1_invalidate(key)
        store.write_through(
            key,
            codec.encode(payload, version=FEATURE_SNAPSHOT_CODEC_VERSION),
            persist=_persist,
            ttl_s=FEATURE_SNAPSHOT_TTL_S,
        )
        l1_set(key, payload)
    return payload
