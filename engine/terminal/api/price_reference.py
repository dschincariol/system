"""Shared terminal market-price reference helpers."""

from __future__ import annotations

import time
from typing import Any, Callable, Dict


def _table_columns(con: Any, table: str, *, warn_fn: Callable[..., None] | None = None) -> set[str]:
    try:
        return {
            str(row[1])
            for row in (con.execute(f"PRAGMA table_info({table})").fetchall() or [])
            if row and len(row) > 1 and row[1]
        }
    except Exception as exc:
        if callable(warn_fn):
            warn_fn("TERMINAL_PRICE_REFERENCE_COLUMNS_FAILED", exc, table=str(table))
        return set()


def latest_terminal_price(
    con: Any,
    symbol: str,
    *,
    table_exists_fn: Callable[[Any, str], bool],
    warn_fn: Callable[..., None] | None = None,
) -> Dict[str, Any]:
    """Return the latest market-data price used by terminal pre-trade checks.

    The terminal deliberately uses the canonical ``prices`` table only. Broker
    position marks are account state, not market-data freshness evidence.
    """

    normalized = str(symbol or "").strip().upper()
    now_ms = int(time.time() * 1000)
    if not normalized:
        return {"ok": False, "error": "missing_symbol", "source": "prices"}
    if not table_exists_fn(con, "prices"):
        return {"ok": False, "error": "prices_table_missing", "source": "prices"}
    cols = _table_columns(con, "prices", warn_fn=warn_fn)
    price_col = "price" if "price" in cols else ("close" if "close" in cols else "")
    ts_col = "ts_ms" if "ts_ms" in cols else ("timestamp_ms" if "timestamp_ms" in cols else "")
    if not price_col or not ts_col or "symbol" not in cols:
        return {
            "ok": False,
            "error": "prices_columns_missing",
            "source": "prices",
            "missing_columns": [
                name
                for name, present in (
                    ("symbol", "symbol" in cols),
                    ("price_or_close", bool(price_col)),
                    ("ts_ms_or_timestamp_ms", bool(ts_col)),
                )
                if not present
            ],
        }
    try:
        row = con.execute(
            f"""
            SELECT {price_col}, {ts_col}
              FROM prices
             WHERE UPPER(symbol)=?
             ORDER BY {ts_col} DESC
             LIMIT 1
            """,
            (normalized,),
        ).fetchone()
    except Exception as exc:
        if callable(warn_fn):
            warn_fn("TERMINAL_PRICE_REFERENCE_READ_FAILED", exc, symbol=normalized)
        return {"ok": False, "error": "price_read_failed", "source": "prices"}
    if not row or row[0] is None:
        return {"ok": False, "error": "missing_price", "source": "prices", "symbol": normalized}
    ts_ms = int(row[1] or 0)
    return {
        "ok": True,
        "symbol": normalized,
        "price": float(row[0]),
        "ts_ms": ts_ms,
        "age_ms": max(0, now_ms - ts_ms) if ts_ms > 0 else None,
        "source": "prices",
    }
