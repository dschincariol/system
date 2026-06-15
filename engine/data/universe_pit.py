"""
Point-in-time universe helpers for offline training and backtesting.

This module is additive by design:
- it does not replace the canonical live universe in `engine.data.universe`
- it derives lifecycle evidence from already-ingested repository tables
- it is only consumed by opt-in offline jobs
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.data.universe_pit")
_WARNED_NONFATAL_KEYS: set[str] = set()
_ACTIVE_STALE_MS = 45 * 24 * 60 * 60 * 1000


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.data.universe_pit",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_json_dumps(value: Dict[str, Any]) -> str:
    return json.dumps(value or {}, separators=(",", ":"), sort_keys=True, default=str)


def _safe_json_loads(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str) or not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _normalize_symbols(symbols: Iterable[Any] | None) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in symbols or []:
        symbol = str(raw or "").upper().strip()
        if not symbol:
            continue
        if symbol == "*":
            return ["*"]
        if symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def use_pit_universe_enabled() -> bool:
    """Return whether point-in-time universe reads are enabled."""
    return str(os.environ.get("USE_PIT_UNIVERSE", "0")).strip().lower() in {"1", "true", "yes", "on"}


def pit_universe_backfill_enabled() -> bool:
    """Return whether PIT-universe backfills should run automatically."""
    return str(os.environ.get("PIT_UNIVERSE_BACKFILL_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}


def ensure_universe_pit_schema(con) -> None:
    """Create the additive PIT-universe schema if it does not already exist."""
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS universe_pit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          first_seen_ts INTEGER NOT NULL,
          last_seen_ts INTEGER,
          is_active INTEGER NOT NULL,
          metadata_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_universe_symbol_ts
        ON universe_pit(symbol, first_seen_ts, last_seen_ts);
        """
    )


def _table_exists(con, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_PIT_TABLE_EXISTS_FAILED",
            e,
            once_key=f"table_exists:{table}",
            table=str(table),
        )
        return False


def _query_source_bounds(
    con,
    *,
    table: str,
    symbol_col: str = "symbol",
    ts_col: str = "ts_ms",
    symbols: Sequence[str] | None = None,
) -> List[tuple[str, int, int]]:
    if not _table_exists(con, table):
        return []

    filters = [f"{symbol_col} IS NOT NULL", f"TRIM({symbol_col}) <> ''", f"{ts_col} IS NOT NULL"]
    params: List[Any] = []
    normalized = _normalize_symbols(symbols)
    if normalized and "*" not in normalized:
        filters.append("UPPER(TRIM(" + symbol_col + ")) IN (" + ",".join("?" for _ in normalized) + ")")
        params.extend(normalized)

    sql = f"""
        SELECT
          UPPER(TRIM({symbol_col})) AS symbol,
          MIN({ts_col}) AS first_seen_ts,
          MAX({ts_col}) AS last_seen_ts
        FROM {table}
        WHERE {" AND ".join(filters)}
        GROUP BY UPPER(TRIM({symbol_col}))
    """
    try:
        return [
            (str(symbol or "").upper().strip(), _safe_int(first_seen_ts), _safe_int(last_seen_ts))
            for symbol, first_seen_ts, last_seen_ts in (con.execute(sql, tuple(params)).fetchall() or [])
            if str(symbol or "").strip()
        ]
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_PIT_SOURCE_QUERY_FAILED",
            e,
            once_key=f"source_query:{table}:{symbol_col}:{ts_col}",
            table=str(table),
            symbol_col=str(symbol_col),
            ts_col=str(ts_col),
        )
        return []


def _merge_bounds(dest: Dict[str, Dict[str, Any]], rows: Sequence[tuple[str, int, int]], *, source: str) -> None:
    for symbol, first_seen_ts, last_seen_ts in rows or []:
        sym = str(symbol or "").upper().strip()
        if not sym:
            continue
        rec = dest.setdefault(
            sym,
            {
                "symbol": sym,
                "first_candidates": [],
                "last_candidates": [],
                "sources": [],
            },
        )
        if int(first_seen_ts or 0) > 0:
            rec["first_candidates"].append(int(first_seen_ts))
        if int(last_seen_ts or 0) > 0:
            rec["last_candidates"].append(int(last_seen_ts))
        sources = rec.setdefault("sources", [])
        if source not in sources:
            sources.append(str(source))


def _load_symbol_registry_rows(con, symbols: Sequence[str] | None = None) -> List[tuple[str, int, int, str]]:
    if not _table_exists(con, "symbols"):
        return []
    filters = ["symbol IS NOT NULL", "TRIM(symbol) <> ''"]
    params: List[Any] = []
    normalized = _normalize_symbols(symbols)
    if normalized and "*" not in normalized:
        filters.append("UPPER(TRIM(symbol)) IN (" + ",".join("?" for _ in normalized) + ")")
        params.extend(normalized)
    try:
        return [
            (
                str(symbol or "").upper().strip(),
                _safe_int(created_ts_ms),
                _safe_int(updated_ts_ms),
                str(status or "").upper().strip(),
            )
            for symbol, created_ts_ms, updated_ts_ms, status in (
                con.execute(
                    f"""
                    SELECT symbol, created_ts_ms, updated_ts_ms, status
                    FROM symbols
                    WHERE {" AND ".join(filters)}
                    """,
                    tuple(params),
                ).fetchall()
                or []
            )
            if str(symbol or "").strip()
        ]
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_PIT_SYMBOL_REGISTRY_LOAD_FAILED",
            e,
            once_key="symbol_registry_rows",
        )
        return []


def _load_symbol_universe_rows(con, symbols: Sequence[str] | None = None) -> List[tuple[str, int, int]]:
    if not _table_exists(con, "symbol_universe"):
        return []
    filters = ["symbol IS NOT NULL", "TRIM(symbol) <> ''"]
    params: List[Any] = []
    normalized = _normalize_symbols(symbols)
    if normalized and "*" not in normalized:
        filters.append("UPPER(TRIM(symbol)) IN (" + ",".join("?" for _ in normalized) + ")")
        params.extend(normalized)
    try:
        return [
            (str(symbol or "").upper().strip(), _safe_int(first_seen_ms), _safe_int(last_seen_ms))
            for symbol, first_seen_ms, last_seen_ms in (
                con.execute(
                    f"""
                    SELECT symbol, first_seen_ms, last_seen_ms
                    FROM symbol_universe
                    WHERE {" AND ".join(filters)}
                    """,
                    tuple(params),
                ).fetchall()
                or []
            )
            if str(symbol or "").strip()
        ]
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_PIT_SYMBOL_UNIVERSE_LOAD_FAILED",
            e,
            once_key="symbol_universe_rows",
        )
        return []


def _collect_lifecycle_evidence(con, *, symbols: Sequence[str] | None = None) -> Dict[str, Dict[str, Any]]:
    evidence: Dict[str, Dict[str, Any]] = {}

    for source in (
        ("prices", "symbol", "ts_ms"),
        ("price_quotes", "symbol", "ts_ms"),
        ("events", "symbol", "ts_ms"),
        ("alerts", "symbol", "ts_ms"),
        ("sec_filings", "symbol", "ts_ms"),
        ("news_symbol_features", "symbol", "bucket_ts_ms"),
        ("social_features", "symbol", "bucket_ts_ms"),
        ("options_symbol_features", "symbol", "bucket_ts_ms"),
    ):
        table, symbol_col, ts_col = source
        rows = _query_source_bounds(con, table=table, symbol_col=symbol_col, ts_col=ts_col, symbols=symbols)
        _merge_bounds(evidence, rows, source=table)

    for symbol, created_ts_ms, updated_ts_ms, status in _load_symbol_registry_rows(con, symbols=symbols):
        rec = evidence.setdefault(
            symbol,
            {
                "symbol": symbol,
                "first_candidates": [],
                "last_candidates": [],
                "sources": [],
            },
        )
        if int(created_ts_ms or 0) > 0:
            rec["first_candidates"].append(int(created_ts_ms))
        if int(updated_ts_ms or 0) > 0:
            rec["last_candidates"].append(int(updated_ts_ms))
        rec["symbol_status"] = str(status or "").upper().strip()
        sources = rec.setdefault("sources", [])
        if "symbols" not in sources:
            sources.append("symbols")

    for symbol, first_seen_ms, last_seen_ms in _load_symbol_universe_rows(con, symbols=symbols):
        rec = evidence.setdefault(
            symbol,
            {
                "symbol": symbol,
                "first_candidates": [],
                "last_candidates": [],
                "sources": [],
            },
        )
        if int(first_seen_ms or 0) > 0:
            rec["first_candidates"].append(int(first_seen_ms))
        if int(last_seen_ms or 0) > 0:
            rec["last_candidates"].append(int(last_seen_ms))
        sources = rec.setdefault("sources", [])
        if "symbol_universe" not in sources:
            sources.append("symbol_universe")

    price_rows = {symbol: last_seen_ts for symbol, _first_seen_ts, last_seen_ts in _query_source_bounds(con, table="prices", symbols=symbols)}
    quote_rows = {symbol: last_seen_ts for symbol, _first_seen_ts, last_seen_ts in _query_source_bounds(con, table="price_quotes", symbols=symbols)}
    for symbol, rec in evidence.items():
        rec["last_price_ts"] = int(price_rows.get(symbol) or 0)
        rec["last_quote_ts"] = int(quote_rows.get(symbol) or 0)
        rec["last_market_ts"] = max(int(rec.get("last_price_ts") or 0), int(rec.get("last_quote_ts") or 0))

    return evidence


def _infer_lifecycle_row(symbol: str, rec: Dict[str, Any], *, now_ts_ms: int) -> Optional[Dict[str, Any]]:
    first_candidates = [int(ts) for ts in list(rec.get("first_candidates") or []) if int(ts or 0) > 0]
    last_candidates = [int(ts) for ts in list(rec.get("last_candidates") or []) if int(ts or 0) > 0]
    if not first_candidates or not last_candidates:
        return None

    first_seen_ts = int(min(first_candidates))
    observed_last_ts = int(max(last_candidates))
    if observed_last_ts < first_seen_ts:
        observed_last_ts = int(first_seen_ts)

    symbol_status = str(rec.get("symbol_status") or "").upper().strip()
    last_market_ts = int(rec.get("last_market_ts") or 0)
    status_disabled = symbol_status == "DISABLED"
    status_active = symbol_status in {"ACTIVE", "WATCH", "COOLDOWN"}

    is_active = False
    if status_disabled:
        is_active = False
    elif last_market_ts > 0 and (int(now_ts_ms) - int(last_market_ts)) <= int(_ACTIVE_STALE_MS):
        is_active = True
    elif status_active and (int(now_ts_ms) - int(observed_last_ts)) <= int(_ACTIVE_STALE_MS):
        is_active = True

    last_seen_ts = None if is_active else int(observed_last_ts)
    delisted_inferred = bool(
        (not is_active)
        and (
            status_disabled
            or (last_market_ts > 0 and (int(now_ts_ms) - int(last_market_ts)) > int(_ACTIVE_STALE_MS))
        )
    )

    metadata = {
        "symbol_status": str(symbol_status or ""),
        "sources": sorted(str(source) for source in list(rec.get("sources") or []) if str(source).strip()),
        "last_market_ts": (int(last_market_ts) if last_market_ts > 0 else None),
        "last_price_ts": (int(rec.get("last_price_ts") or 0) if int(rec.get("last_price_ts") or 0) > 0 else None),
        "last_quote_ts": (int(rec.get("last_quote_ts") or 0) if int(rec.get("last_quote_ts") or 0) > 0 else None),
        "delisted_inferred": bool(delisted_inferred),
        "derived_from_ingestion": True,
    }

    return {
        "symbol": str(symbol),
        "first_seen_ts": int(first_seen_ts),
        "last_seen_ts": (int(last_seen_ts) if last_seen_ts is not None else None),
        "is_active": (1 if is_active else 0),
        "metadata_json": _safe_json_dumps(metadata),
    }


def backfill_universe_pit(
    con,
    *,
    now_ts_ms: Optional[int] = None,
    symbols: Sequence[str] | None = None,
) -> Dict[str, Any]:
    """Rebuild PIT-universe rows from existing lifecycle evidence."""
    ensure_universe_pit_schema(con)
    effective_now_ts_ms = int(now_ts_ms or _now_ms())
    evidence = _collect_lifecycle_evidence(con, symbols=symbols)

    rows: List[Dict[str, Any]] = []
    for symbol in sorted(evidence.keys()):
        row = _infer_lifecycle_row(symbol, dict(evidence.get(symbol) or {}), now_ts_ms=int(effective_now_ts_ms))
        if row is None:
            continue
        rows.append(dict(row))

    for row in rows:
        con.execute("DELETE FROM universe_pit WHERE symbol=?", (str(row["symbol"]),))
        con.execute(
            """
            INSERT INTO universe_pit(symbol, first_seen_ts, last_seen_ts, is_active, metadata_json)
            VALUES (?,?,?,?,?)
            """,
            (
                str(row["symbol"]),
                int(row["first_seen_ts"]),
                (int(row["last_seen_ts"]) if row.get("last_seen_ts") is not None else None),
                int(row["is_active"]),
                str(row["metadata_json"] or "{}"),
            ),
        )

    return {
        "ok": True,
        "now_ts_ms": int(effective_now_ts_ms),
        "symbols_requested": int(len(_normalize_symbols(symbols))) if symbols else None,
        "row_count": int(len(rows)),
        "active_count": int(sum(1 for row in rows if int(row.get("is_active") or 0) == 1)),
        "inactive_count": int(sum(1 for row in rows if int(row.get("is_active") or 0) == 0)),
    }


def maybe_backfill_universe_pit(con, *, now_ts_ms: Optional[int] = None) -> Dict[str, Any]:
    """Run a PIT-universe backfill only when the feature flag is enabled."""
    if not pit_universe_backfill_enabled():
        return {"ok": True, "enabled": False}
    try:
        result = backfill_universe_pit(con, now_ts_ms=now_ts_ms)
        result["enabled"] = True
        return result
    except Exception as e:
        _warn_nonfatal("UNIVERSE_PIT_BACKFILL_FAILED", e, once_key="maybe_backfill_universe_pit")
        return {"ok": False, "enabled": True, "error": str(e)}


def _snapshot_rows(
    con,
    *,
    start_ts_ms: Optional[int],
    end_ts_ms: int,
    symbols: Sequence[str] | None = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not _table_exists(con, "universe_pit"):
        return []
    filters = ["first_seen_ts <= ?"]
    params: List[Any] = [int(end_ts_ms)]
    if start_ts_ms is None:
        filters.append("(last_seen_ts IS NULL OR last_seen_ts >= ?)")
        params.append(int(end_ts_ms))
    else:
        filters.append("(last_seen_ts IS NULL OR last_seen_ts >= ?)")
        params.append(int(start_ts_ms))

    normalized = _normalize_symbols(symbols)
    if normalized and "*" not in normalized:
        filters.append("symbol IN (" + ",".join("?" for _ in normalized) + ")")
        params.extend(normalized)

    sql = """
        SELECT symbol, first_seen_ts, last_seen_ts, is_active, metadata_json
        FROM universe_pit
        WHERE {where_clause}
        ORDER BY symbol ASC
    """.format(where_clause=" AND ".join(filters))
    if limit is not None and int(limit) > 0:
        sql += "\n        LIMIT ?"
        params.append(int(limit))

    try:
        rows = con.execute(sql, tuple(params)).fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_PIT_SNAPSHOT_QUERY_FAILED",
            e,
            once_key="snapshot_rows",
            start_ts_ms=(None if start_ts_ms is None else int(start_ts_ms)),
            end_ts_ms=int(end_ts_ms),
        )
        return []

    return [
        {
            "symbol": str(symbol or "").upper().strip(),
            "first_seen_ts": int(first_seen_ts or 0),
            "last_seen_ts": (int(last_seen_ts) if last_seen_ts is not None else None),
            "is_active": int(is_active or 0),
            "metadata": _safe_json_loads(metadata_json),
        }
        for symbol, first_seen_ts, last_seen_ts, is_active, metadata_json in rows
        if str(symbol or "").strip()
    ]


def get_pit_universe_snapshot(
    con,
    *,
    ts_ms: int,
    symbols: Sequence[str] | None = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return PIT-universe rows valid at one timestamp."""
    return _snapshot_rows(con, start_ts_ms=None, end_ts_ms=int(ts_ms), symbols=symbols, limit=limit)


def get_pit_universe_symbols(
    con,
    *,
    ts_ms: int,
    symbols: Sequence[str] | None = None,
    limit: Optional[int] = None,
) -> List[str]:
    """Return only the symbols active in the PIT universe at one timestamp."""
    return [str(row.get("symbol") or "") for row in get_pit_universe_snapshot(con, ts_ms=ts_ms, symbols=symbols, limit=limit)]


def get_pit_universe_window_snapshot(
    con,
    *,
    start_ts_ms: int,
    end_ts_ms: int,
    symbols: Sequence[str] | None = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Return PIT-universe rows that overlap the requested window."""
    return _snapshot_rows(
        con,
        start_ts_ms=int(start_ts_ms),
        end_ts_ms=int(end_ts_ms),
        symbols=symbols,
        limit=limit,
    )


def get_pit_universe_window_symbols(
    con,
    *,
    start_ts_ms: int,
    end_ts_ms: int,
    symbols: Sequence[str] | None = None,
    limit: Optional[int] = None,
) -> List[str]:
    """Return symbols that were active at any point in the requested window."""
    return [
        str(row.get("symbol") or "")
        for row in get_pit_universe_window_snapshot(
            con,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            symbols=symbols,
            limit=limit,
        )
    ]


def resolve_training_window_universe(
    con,
    *,
    configured_symbols: Sequence[Any] | None,
    lookback_days: int,
    as_of_ts_ms: Optional[int] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Resolve the symbol set that is valid for one historical training window."""
    normalized_configured = _normalize_symbols(configured_symbols)
    as_of = int(as_of_ts_ms or _now_ms())
    lookback_window_days = max(0, int(lookback_days or 0))

    if not use_pit_universe_enabled():
        return {
            "pit_enabled": False,
            "pit_applied": False,
            "symbols": list(normalized_configured),
            "configured_symbols": list(normalized_configured),
            "window_start_ts_ms": int(as_of - (lookback_window_days * 24 * 60 * 60 * 1000)),
            "window_end_ts_ms": int(as_of),
            "fallback_reason": "pit_disabled",
        }

    maybe_backfill_universe_pit(con, now_ts_ms=as_of)
    window_start_ts_ms = int(as_of - (lookback_window_days * 24 * 60 * 60 * 1000))
    filter_symbols = None if ("*" in normalized_configured or not normalized_configured) else normalized_configured
    pit_symbols = get_pit_universe_window_symbols(
        con,
        start_ts_ms=int(window_start_ts_ms),
        end_ts_ms=int(as_of),
        symbols=filter_symbols,
        limit=limit,
    )
    if pit_symbols:
        return {
            "pit_enabled": True,
            "pit_applied": True,
            "symbols": list(pit_symbols),
            "configured_symbols": list(normalized_configured),
            "window_start_ts_ms": int(window_start_ts_ms),
            "window_end_ts_ms": int(as_of),
            "fallback_reason": "",
        }

    return {
        "pit_enabled": True,
        "pit_applied": False,
        "symbols": list(normalized_configured),
        "configured_symbols": list(normalized_configured),
        "window_start_ts_ms": int(window_start_ts_ms),
        "window_end_ts_ms": int(as_of),
        "fallback_reason": "no_pit_rows",
    }


def filter_symbols_for_snapshot(
    con,
    *,
    symbols: Sequence[Any] | None,
    ts_ms: int,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Filter a candidate symbol list against the PIT universe at one timestamp."""
    normalized_symbols = _normalize_symbols(symbols)
    if not normalized_symbols:
        return {
            "pit_enabled": use_pit_universe_enabled(),
            "pit_applied": False,
            "symbols": [],
            "fallback_reason": "no_symbols",
        }

    if not use_pit_universe_enabled():
        return {
            "pit_enabled": False,
            "pit_applied": False,
            "symbols": list(normalized_symbols),
            "fallback_reason": "pit_disabled",
        }

    maybe_backfill_universe_pit(con, now_ts_ms=int(ts_ms))
    pit_symbols = get_pit_universe_symbols(con, ts_ms=int(ts_ms), symbols=normalized_symbols, limit=limit)
    if pit_symbols:
        return {
            "pit_enabled": True,
            "pit_applied": True,
            "symbols": list(pit_symbols),
            "fallback_reason": "",
        }
    return {
        "pit_enabled": True,
        "pit_applied": False,
        "symbols": list(normalized_symbols),
        "fallback_reason": "no_pit_rows",
    }


def label_window_within_symbol_lifecycle(
    con,
    *,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> bool:
    """Return false when a forward-label window crosses a known delist date."""
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    if int(end_ts_ms) < int(start_ts_ms):
        return False
    if not _table_exists(con, "universe_pit"):
        return True
    try:
        row = con.execute(
            """
            SELECT first_seen_ts, last_seen_ts, is_active
            FROM universe_pit
            WHERE symbol=?
              AND first_seen_ts <= ?
              AND (last_seen_ts IS NULL OR last_seen_ts >= ?)
            ORDER BY first_seen_ts DESC
            LIMIT 1
            """,
            (str(sym), int(start_ts_ms), int(start_ts_ms)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_PIT_LABEL_WINDOW_QUERY_FAILED",
            e,
            once_key=f"label_window:{sym}",
            symbol=str(sym),
            start_ts_ms=int(start_ts_ms),
            end_ts_ms=int(end_ts_ms),
        )
        return True
    if not row:
        return True
    _first_seen_ts, last_seen_ts, is_active = row
    if last_seen_ts is None:
        return True
    if int(is_active or 0) == 1:
        return True
    return int(end_ts_ms) <= int(last_seen_ts)
