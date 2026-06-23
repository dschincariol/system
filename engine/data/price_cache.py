"""
Canonical in-process price cache for live feature generation and replay.
"""

from __future__ import annotations

import bisect
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional, cast

from engine.runtime.db_guard import resolve_db_path
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.live_cache import get_live_cache
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

PRICE_CACHE_MAX_POINTS = max(32, int(os.environ.get("PRICE_CACHE_MAX_POINTS", "512")))
PRICE_CACHE_TTL_S = max(1.0, float(os.environ.get("PRICE_CACHE_TTL_S", "900.0")))
LOG = get_logger("engine.data.price_cache")
_WARNED_NONFATAL_KEYS: set[str] = set()


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
        component="engine.data.price_cache",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out or out in (float("inf"), float("-inf")):
        return float(default)
    return float(out)


@dataclass(frozen=True)
class PricePoint:
    """One normalized price observation held in the in-process cache."""

    ts_ms: int
    price: float
    volume: float = 0.0


@dataclass(frozen=True)
class PriceSnapshot:
    """Read-side price snapshot returned by cache consumers."""

    symbol: str
    points: tuple[PricePoint, ...]
    source: str = "memory"
    recovered_from_db: bool = False

    @property
    def asof_ts_ms(self) -> int:
        if not self.points:
            return 0
        return int(self.points[-1].ts_ms)


class _SymbolCacheEntry:
    __slots__ = ("expires_at_monotonic", "points", "source")

    def __init__(self, *, source: str = "memory") -> None:
        self.expires_at_monotonic = 0.0
        self.points: list[PricePoint] = []
        self.source = str(source or "memory")


_CACHE_LOCK = threading.RLock()
_CACHE_DB_PATH = ""


def _new_cache_metrics() -> dict[str, Any]:
    return {
        "initialized_ts_ms": int(time.time() * 1000),
        "last_update_ts_ms": 0,
        "last_db_recovery_ts_ms": 0,
        "last_symbol": "",
        "update_count": 0,
        "db_recovery_count": 0,
    }


_CACHE_METRICS: dict[str, Any] = _new_cache_metrics()


def _cache_backend():
    return get_live_cache()


def _current_db_path() -> str:
    try:
        return str(resolve_db_path())
    except Exception:
        return ""


def _reset_cache_if_db_changed() -> None:
    global _CACHE_DB_PATH, _CACHE_METRICS
    current_db_path = _current_db_path()
    with _CACHE_LOCK:
        if str(_CACHE_DB_PATH or "") == str(current_db_path or ""):
            return
        _CACHE_METRICS = _new_cache_metrics()
        _CACHE_DB_PATH = str(current_db_path or "")
    try:
        _cache_backend().clear_price()
    except Exception as exc:
        _warn_nonfatal(
            "PRICE_CACHE_BACKEND_RESET_FAILED",
            exc,
            once_key="price_cache_backend_reset_failed",
        )


def _snapshot_from_entry(symbol: str, entry: _SymbolCacheEntry, *, recovered_from_db: bool = False) -> PriceSnapshot:
    points = tuple(
        PricePoint(ts_ms=int(point.ts_ms), price=float(point.price), volume=float(point.volume))
        for point in list(entry.points)
    )
    return PriceSnapshot(
        symbol=str(symbol),
        points=points,
        source=str(entry.source or "memory"),
        recovered_from_db=bool(recovered_from_db),
    )


def _snapshot_to_payload(snapshot: PriceSnapshot) -> dict[str, Any]:
    return {
        "symbol": str(snapshot.symbol or ""),
        "source": str(snapshot.source or "memory"),
        "recovered_from_db": bool(snapshot.recovered_from_db),
        "points": [
            {
                "ts_ms": int(point.ts_ms),
                "price": float(point.price),
                "volume": float(point.volume),
            }
            for point in list(snapshot.points or ())
        ],
    }


def _snapshot_from_payload(payload: Mapping[str, Any] | None) -> PriceSnapshot | None:
    if not isinstance(payload, Mapping):
        return None
    symbol = _normalize_symbol(payload.get("symbol"))
    points = []
    for point in list(payload.get("points") or ()):
        try:
            ts_ms = _safe_int((point or {}).get("ts_ms"), 0)
            price = _safe_float((point or {}).get("price"), 0.0)
            volume = _safe_float((point or {}).get("volume"), 0.0)
        except Exception:
            continue
        if ts_ms <= 0 or price <= 0.0:
            continue
        points.append(
            PricePoint(
                ts_ms=int(ts_ms),
                price=float(price),
                volume=float(max(0.0, volume)),
            )
        )
    if not symbol and not points:
        return None
    return PriceSnapshot(
        symbol=str(symbol),
        points=tuple(points),
        source=str(payload.get("source") or "memory"),
        recovered_from_db=bool(payload.get("recovered_from_db")),
    )


def _get_cached_price_snapshot(symbol: str) -> PriceSnapshot | None:
    try:
        return _snapshot_from_payload(_cache_backend().get_price_snapshot(symbol))
    except Exception as exc:
        _warn_nonfatal(
            "PRICE_CACHE_BACKEND_GET_FAILED",
            exc,
            once_key=f"price_cache_backend_get_failed:{symbol}",
            symbol=str(symbol),
        )
        return None


def _get_cached_price_snapshots(symbols: Iterable[str]) -> dict[str, PriceSnapshot | None]:
    symbol_keys = [_normalize_symbol(symbol) for symbol in list(symbols or [])]
    symbol_keys = [symbol for symbol in symbol_keys if symbol]
    if not symbol_keys:
        return {}
    backend = _cache_backend()
    getter = getattr(backend, "get_price_snapshots", None)
    if callable(getter):
        try:
            batch_getter = cast(Callable[[list[str]], Mapping[str, Mapping[str, Any] | None]], getter)
            payloads = batch_getter(symbol_keys) or {}
            return {symbol: _snapshot_from_payload(payloads.get(symbol)) for symbol in symbol_keys}
        except Exception as exc:
            _warn_nonfatal(
                "PRICE_CACHE_BACKEND_MGET_FAILED",
                exc,
                once_key="price_cache_backend_mget_failed",
                symbols=symbol_keys,
            )
            return {symbol: None for symbol in symbol_keys}
    return {symbol: _get_cached_price_snapshot(symbol) for symbol in symbol_keys}


def _set_cached_price_snapshot(snapshot: PriceSnapshot) -> PriceSnapshot:
    payload = _snapshot_to_payload(snapshot)
    try:
        _cache_backend().set_price_snapshot(
            str(snapshot.symbol),
            payload,
            ttl_s=float(PRICE_CACHE_TTL_S),
            snapshot_ts_ms=int(snapshot.asof_ts_ms),
        )
    except Exception as exc:
        _warn_nonfatal(
            "PRICE_CACHE_BACKEND_SET_FAILED",
            exc,
            once_key=f"price_cache_backend_set_failed:{snapshot.symbol}",
            symbol=str(snapshot.symbol),
        )
    return snapshot


def _set_cached_price_snapshots(snapshots: Iterable[PriceSnapshot]) -> dict[str, bool]:
    snapshot_list = [snapshot for snapshot in list(snapshots or []) if _normalize_symbol(getattr(snapshot, "symbol", ""))]
    if not snapshot_list:
        return {}
    payloads = {str(snapshot.symbol): _snapshot_to_payload(snapshot) for snapshot in snapshot_list}
    snapshot_ts_ms_by_symbol = {str(snapshot.symbol): int(snapshot.asof_ts_ms) for snapshot in snapshot_list}
    backend = _cache_backend()
    setter = getattr(backend, "set_price_snapshots", None)
    if callable(setter):
        try:
            batch_setter = cast(Callable[..., Mapping[str, bool]], setter)
            return dict(
                batch_setter(
                    payloads,
                    ttl_s=float(PRICE_CACHE_TTL_S),
                    snapshot_ts_ms_by_symbol=snapshot_ts_ms_by_symbol,
                )
            )
        except Exception as exc:
            _warn_nonfatal(
                "PRICE_CACHE_BACKEND_MSET_FAILED",
                exc,
                once_key="price_cache_backend_mset_failed",
                symbols=list(payloads),
            )
            return {str(snapshot.symbol): False for snapshot in snapshot_list}
    results: dict[str, bool] = {}
    for snapshot in snapshot_list:
        _set_cached_price_snapshot(snapshot)
        results[str(snapshot.symbol)] = True
    return results


def _find_point_index(points: list[PricePoint], ts_ms: int) -> int:
    return bisect.bisect_left([int(point.ts_ms) for point in points], int(ts_ms))


def _upsert_point(entry: _SymbolCacheEntry, point: PricePoint) -> None:
    idx = _find_point_index(entry.points, int(point.ts_ms))
    if idx < len(entry.points) and int(entry.points[idx].ts_ms) == int(point.ts_ms):
        prev = entry.points[idx]
        volume = float(point.volume) if float(point.volume) > 0.0 else float(prev.volume)
        entry.points[idx] = PricePoint(
            ts_ms=int(point.ts_ms),
            price=float(point.price),
            volume=float(volume),
        )
    else:
        entry.points.insert(idx, point)
    if len(entry.points) > int(PRICE_CACHE_MAX_POINTS):
        del entry.points[: len(entry.points) - int(PRICE_CACHE_MAX_POINTS)]


def _normalize_price_row(row: Mapping[str, Any]) -> Optional[tuple[str, PricePoint, str]]:
    symbol = _normalize_symbol(row.get("symbol"))
    ts_ms = _safe_int(row.get("ts_ms", row.get("timestamp")), 0)
    price = row.get("price")
    if price in (None, ""):
        price = row.get("last")
    if price in (None, ""):
        price = row.get("px")
    price_f = _safe_float(price, 0.0)
    if not symbol or ts_ms <= 0 or price_f <= 0.0:
        return None
    volume_f = _safe_float(row.get("volume"), 0.0)
    source = str(row.get("source") or row.get("provider") or "memory").strip().lower() or "memory"
    return symbol, PricePoint(ts_ms=int(ts_ms), price=float(price_f), volume=float(max(0.0, volume_f))), source


def snapshot_from_rows(
    symbol: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    source: str = "offline",
    recovered_from_db: bool = False,
) -> PriceSnapshot:
    """Build a snapshot from normalized rows without mutating the shared cache."""
    entry = _SymbolCacheEntry(source=str(source or "offline"))
    for row in rows or []:
        normalized = _normalize_price_row({"symbol": symbol, **dict(row or {})})
        if normalized is None:
            continue
        _symbol, point, source = normalized
        entry.source = str(source or entry.source)
        _upsert_point(entry, point)
    return _snapshot_from_entry(_normalize_symbol(symbol), entry, recovered_from_db=bool(recovered_from_db))


def clear_price_cache(symbol: str | None = None) -> None:
    """Clear one symbol or the entire in-process price cache."""
    _reset_cache_if_db_changed()
    try:
        _cache_backend().clear_price(symbol)
    except Exception as exc:
        _warn_nonfatal(
            "PRICE_CACHE_BACKEND_CLEAR_FAILED",
            exc,
            once_key="price_cache_backend_clear_failed",
            symbol=(str(symbol) if symbol is not None else None),
        )


def record_price_rows(rows: Iterable[Mapping[str, Any]]) -> int:
    """Merge normalized price rows into the in-process cache."""
    _reset_cache_if_db_changed()
    normalized_rows: list[tuple[str, PricePoint, str]] = []
    for row in rows or []:
        try:
            normalized = _normalize_price_row(dict(row or {}))
        except Exception as exc:
            _warn_nonfatal(
                "PRICE_CACHE_NORMALIZE_ROW_FAILED",
                exc,
                once_key="price_cache_normalize_row_failed",
            )
            continue
        if normalized is not None:
            normalized_rows.append(normalized)

    if not normalized_rows:
        return 0

    grouped: dict[str, list[tuple[PricePoint, str]]] = {}
    for symbol, point, source in normalized_rows:
        grouped.setdefault(str(symbol), []).append((point, str(source or "memory")))

    latest_ts_ms = 0
    last_symbol = ""
    cached_by_symbol = _get_cached_price_snapshots(grouped.keys())
    snapshots_to_write: list[PriceSnapshot] = []
    for symbol, points in grouped.items():
        cached = cached_by_symbol.get(str(symbol))
        entry = _SymbolCacheEntry(source=str((cached.source if cached is not None else "") or "memory"))
        if cached is not None:
            for point in list(cached.points or ()):
                _upsert_point(entry, point)
            entry.source = str(cached.source or entry.source)
        for point, source in points:
            entry.source = str(source or entry.source)
            _upsert_point(entry, point)
            latest_ts_ms = max(int(latest_ts_ms), int(point.ts_ms))
            last_symbol = str(symbol)
        snapshots_to_write.append(_snapshot_from_entry(symbol, entry, recovered_from_db=False))

    _set_cached_price_snapshots(snapshots_to_write)

    with _CACHE_LOCK:
        _CACHE_METRICS["last_update_ts_ms"] = int(max(int(_CACHE_METRICS.get("last_update_ts_ms") or 0), int(latest_ts_ms)))
        _CACHE_METRICS["last_symbol"] = str(last_symbol)
        _CACHE_METRICS["update_count"] = int(_CACHE_METRICS.get("update_count") or 0) + int(len(normalized_rows))
    return int(len(normalized_rows))


def load_symbol_snapshot(
    symbol: str,
    *,
    asof_ts_ms: int | None = None,
    con: Any | None = None,
    limit: int | None = None,
) -> PriceSnapshot:
    """Load a symbol snapshot, recovering from DB when the cache is cold."""
    _reset_cache_if_db_changed()
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return PriceSnapshot(symbol="", points=(), source="sqlite", recovered_from_db=True)

    owns = con is None
    if con is None:
        con = connect(readonly=True)
    assert con is not None
    row_limit = max(1, int(limit or PRICE_CACHE_MAX_POINTS))
    params: list[Any] = [str(symbol_key)]
    ts_filter_sql = ""
    if asof_ts_ms is not None:
        ts_filter_sql = " AND p.ts_ms <= ?"
        params.append(int(asof_ts_ms))
    params.append(int(row_limit))

    try:
        try:
            rows = con.execute(
                f"""
                SELECT
                  p.ts_ms,
                  COALESCE(p.price, p.px) AS price,
                  COALESCE(q.volume, 0.0) AS volume,
                  COALESCE(q.source, p.source, 'sqlite') AS source
                FROM prices p
                LEFT JOIN price_quotes q
                  ON q.symbol = p.symbol
                 AND q.ts_ms = p.ts_ms
                WHERE p.symbol = ?
                  {ts_filter_sql}
                  AND COALESCE(p.price, p.px) IS NOT NULL
                ORDER BY p.ts_ms DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        except Exception as exc:
            if "no such table: price_quotes" not in str(exc).lower():
                raise
            fallback_params: list[Any] = [str(symbol_key)]
            fallback_filter_sql = ""
            if asof_ts_ms is not None:
                fallback_filter_sql = " AND ts_ms <= ?"
                fallback_params.append(int(asof_ts_ms))
            fallback_params.append(int(row_limit))
            rows = con.execute(
                f"""
                SELECT
                  ts_ms,
                  COALESCE(price, px) AS price,
                  0.0 AS volume,
                  COALESCE(source, 'sqlite') AS source
                FROM prices
                WHERE symbol = ?
                  {fallback_filter_sql}
                  AND COALESCE(price, px) IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                tuple(fallback_params),
            ).fetchall()
    except Exception as exc:
        rows = []
        _warn_nonfatal(
            "PRICE_CACHE_DB_RECOVERY_FAILED",
            exc,
            once_key=f"price_cache_db_recovery_failed:{symbol_key}",
            symbol=str(symbol_key),
            asof_ts_ms=(int(asof_ts_ms) if asof_ts_ms is not None else None),
        )
    finally:
        if owns:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "PRICE_CACHE_DB_CLOSE_FAILED",
                    exc,
                    once_key="price_cache_db_close_failed",
                )

    if not rows:
        return PriceSnapshot(symbol=str(symbol_key), points=(), source="sqlite", recovered_from_db=True)

    historical_rows = [
        {
            "symbol": str(symbol_key),
            "ts_ms": int(row[0] or 0),
            "price": float(row[1] or 0.0),
            "volume": float(row[2] or 0.0),
            "source": str(row[3] or "sqlite"),
        }
        for row in reversed(rows or [])
        if row and _safe_int(row[0], 0) > 0 and _safe_float(row[1], 0.0) > 0.0
    ]
    if not historical_rows:
        return PriceSnapshot(symbol=str(symbol_key), points=(), source="sqlite", recovered_from_db=True)

    return snapshot_from_rows(
        symbol_key,
        historical_rows,
        source="sqlite",
        recovered_from_db=True,
    )


def _recover_symbol_from_db(symbol: str) -> PriceSnapshot:
    snapshot = load_symbol_snapshot(str(symbol))
    if not snapshot.points:
        return snapshot

    with _CACHE_LOCK:
        _CACHE_METRICS["db_recovery_count"] = int(_CACHE_METRICS.get("db_recovery_count") or 0) + 1
        _CACHE_METRICS["last_db_recovery_ts_ms"] = int(time.time() * 1000)
        _CACHE_METRICS["last_symbol"] = str(snapshot.symbol or symbol)

    record_price_rows(
        [
            {
                "symbol": str(snapshot.symbol),
                "ts_ms": int(point.ts_ms),
                "price": float(point.price),
                "volume": float(point.volume),
                "source": str(snapshot.source or "sqlite"),
            }
            for point in list(snapshot.points or ())
        ]
    )
    return snapshot


def get_symbol_snapshot(symbol: str, *, allow_db_recovery: bool = True) -> PriceSnapshot:
    """Return the latest snapshot for a symbol from cache or optional DB recovery."""
    _reset_cache_if_db_changed()
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return PriceSnapshot(symbol="", points=(), source="memory", recovered_from_db=False)

    cached = _get_cached_price_snapshot(symbol_key)
    if cached is not None:
        return PriceSnapshot(
            symbol=str(cached.symbol),
            points=tuple(cached.points or ()),
            source=str(cached.source or "memory"),
            recovered_from_db=False,
        )

    if not allow_db_recovery:
        return PriceSnapshot(symbol=symbol_key, points=(), source="memory", recovered_from_db=False)
    return _recover_symbol_from_db(symbol_key)


def get_live_symbol_snapshot(symbol: str) -> PriceSnapshot:
    """Return a live-only snapshot without triggering database recovery."""
    return get_symbol_snapshot(symbol, allow_db_recovery=False)


def price_cache_initialized() -> bool:
    """Return whether the in-memory price cache has completed initial population."""
    _reset_cache_if_db_changed()
    with _CACHE_LOCK:
        return int(_CACHE_METRICS.get("initialized_ts_ms") or 0) > 0


def is_cache_stale(*, stale_after_s: float | None = None) -> bool:
    """Return whether the cache exceeds the configured freshness threshold."""
    snapshot = get_cache_snapshot(stale_after_s=stale_after_s)
    return bool(snapshot.get("stale"))


def get_cache_snapshot(*, stale_after_s: float | None = None) -> dict[str, Any]:
    """Return aggregate cache health and freshness metadata."""
    _reset_cache_if_db_changed()
    effective_stale_after_s = max(1.0, float(stale_after_s or PRICE_CACHE_TTL_S))
    effective_stale_after_ms = int(effective_stale_after_s * 1000.0)
    now_ts_ms = int(time.time() * 1000)
    backend_snapshot = dict(_cache_backend().get_snapshot() or {})
    with _CACHE_LOCK:
        last_update_ts_ms = int(_CACHE_METRICS.get("last_update_ts_ms") or 0)
        last_db_recovery_ts_ms = int(_CACHE_METRICS.get("last_db_recovery_ts_ms") or 0)
        update_count = int(_CACHE_METRICS.get("update_count") or 0)
        db_recovery_count = int(_CACHE_METRICS.get("db_recovery_count") or 0)
        last_symbol = str(_CACHE_METRICS.get("last_symbol") or "")
        initialized_ts_ms = int(_CACHE_METRICS.get("initialized_ts_ms") or 0)

    symbol_count = int(backend_snapshot.get("price_symbols") or 0)
    total_points = int(backend_snapshot.get("price_points") or 0)
    age_ms = max(0, now_ts_ms - last_update_ts_ms) if last_update_ts_ms > 0 else 10**12
    has_runtime_data = bool(symbol_count > 0 and total_points > 0 and last_update_ts_ms > 0)
    stale = bool(has_runtime_data and age_ms > int(effective_stale_after_ms))
    return {
        "ok": bool(initialized_ts_ms > 0 and not stale),
        "initialized": bool(initialized_ts_ms > 0),
        "initialized_ts_ms": int(initialized_ts_ms) if initialized_ts_ms > 0 else None,
        "authoritative_runtime": True,
        "db_reads_required": False,
        "symbol_count": int(symbol_count),
        "total_points": int(total_points),
        "update_count": int(update_count),
        "db_recovery_count": int(db_recovery_count),
        "last_symbol": str(last_symbol),
        "last_update_ts_ms": int(last_update_ts_ms) if last_update_ts_ms > 0 else None,
        "last_db_recovery_ts_ms": int(last_db_recovery_ts_ms) if last_db_recovery_ts_ms > 0 else None,
        "backend": str(backend_snapshot.get("resolved_backend") or backend_snapshot.get("backend") or "memory"),
        "backend_requested": str(backend_snapshot.get("requested_backend") or "memory"),
        "backend_degraded": bool(backend_snapshot.get("degraded")),
        "backend_fallback_reason": backend_snapshot.get("fallback_reason"),
        "age_s": (round(age_ms / 1000.0, 1) if age_ms < 10**12 else None),
        "stale": bool(stale),
        "stale_after_s": float(effective_stale_after_s),
        "detail": (
            "ok"
            if has_runtime_data and not stale
            else ("initialized_no_prices_yet" if initialized_ts_ms > 0 and not has_runtime_data else "price_cache_uninitialized")
        ),
        "ts_ms": int(now_ts_ms),
    }
