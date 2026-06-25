"""Price-row hygiene checks shared by live and offline ingestion paths."""

from __future__ import annotations

import logging
import math
import os
from typing import Any, Iterable, Mapping, Sequence

from engine.data.asset_map import asset_class_for_symbol
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.data.price_hygiene")

SPLIT_DOWN_RETURN = -0.45
SPLIT_UP_RETURN = 0.90
PRICE_HYGIENE_USE_CORP_ACTION_CALENDAR = os.environ.get("PRICE_HYGIENE_USE_CORP_ACTION_CALENDAR", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_DAY_MS = 24 * 60 * 60 * 1000


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


def is_split_like_price_jump(previous_price: Any, current_price: Any) -> bool:
    prev = _safe_float(previous_price)
    cur = _safe_float(current_price)
    if prev is None or cur is None or prev <= 0.0 or cur <= 0.0:
        return False
    ret = (float(cur) - float(prev)) / float(prev)
    return bool(ret < SPLIT_DOWN_RETURN or ret > SPLIT_UP_RETURN)


def _utc_day_window(ts_ms: int) -> tuple[int, int]:
    start = (int(ts_ms) // _DAY_MS) * _DAY_MS
    return int(start - 1), int(start + _DAY_MS - 1)


def is_explained_split(con: Any, *, symbol: str, ts_ms: int) -> bool:
    if not PRICE_HYGIENE_USE_CORP_ACTION_CALENDAR:
        return False
    try:
        from engine.data.corporate_actions import corporate_action_ex_dates

        start, end = _utc_day_window(int(ts_ms))
        return bool(
            corporate_action_ex_dates(
                con,
                symbol=str(symbol),
                action_type="split",
                start_ts_ms=int(start),
                end_ts_ms=int(end),
            )
        )
    except Exception:
        return False


def _has_news_or_corporate_action_flag(row: Mapping[str, Any]) -> bool:
    for key in (
        "has_news",
        "news_flag",
        "corporate_action",
        "corporate_action_flag",
        "split",
        "split_flag",
    ):
        value = row.get(key)
        if isinstance(value, str):
            if value.strip().lower() in {"1", "true", "yes", "on", "split", "dividend", "corporate_action"}:
                return True
        elif bool(value):
            return True
    return False


def log_split_like_price_row(
    *,
    symbol: str,
    ts_ms: int,
    previous_price: float,
    current_price: float,
    source: str = "",
) -> None:
    ret = (float(current_price) - float(previous_price)) / float(previous_price)
    log_failure(
        LOG,
        event="split_like_price_row_flagged",
        code="SPLIT_LIKE_PRICE_ROW_FLAGGED",
        message="Split-like overnight price move flagged for review and excluded from training prices.",
        error=RuntimeError("split_like_price_row_flagged"),
        level=logging.WARNING,
        component="engine.data.price_hygiene",
        extra={
            "symbol": str(symbol),
            "ts_ms": int(ts_ms),
            "previous_price": float(previous_price),
            "current_price": float(current_price),
            "return": float(ret),
            "source": str(source or ""),
            "down_threshold": float(SPLIT_DOWN_RETURN),
            "up_threshold": float(SPLIT_UP_RETURN),
        },
        persist=True,
    )


def latest_price_before(con: Any, symbol: str, ts_ms: int) -> tuple[int, float] | None:
    row = con.execute(
        """
        SELECT ts_ms, COALESCE(price, px)
        FROM prices
        WHERE symbol=?
          AND ts_ms < ?
          AND COALESCE(price, px) IS NOT NULL
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (str(symbol).upper().strip(), int(ts_ms)),
    ).fetchone()
    if not row:
        return None
    px = _safe_float(row[1])
    if px is None:
        return None
    return int(row[0] or 0), float(px)


def filter_split_like_price_rows(
    con: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    symbol_key: str = "symbol",
    price_key: str = "price",
    ts_key: str = "ts_ms",
    source_key: str = "source",
) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]]]:
    accepted: list[Mapping[str, Any]] = []
    flagged: list[Mapping[str, Any]] = []
    for row in rows or []:
        rec = dict(row or {})
        symbol = str(rec.get(symbol_key) or "").upper().strip()
        current = _safe_float(rec.get(price_key))
        ts_ms = int(rec.get(ts_key) or 0)
        if (
            not symbol
            or current is None
            or ts_ms <= 0
            or _has_news_or_corporate_action_flag(rec)
            or asset_class_for_symbol(symbol) == "FUTURES"
        ):
            accepted.append(rec)
            continue
        try:
            previous = latest_price_before(con, symbol, int(ts_ms))
        except Exception:
            accepted.append(rec)
            continue
        if previous is None or not is_split_like_price_jump(previous[1], current):
            accepted.append(rec)
            continue
        if is_explained_split(con, symbol=symbol, ts_ms=int(ts_ms)):
            accepted.append(rec)
            continue
        log_split_like_price_row(
            symbol=symbol,
            ts_ms=int(ts_ms),
            previous_price=float(previous[1]),
            current_price=float(current),
            source=str(rec.get(source_key) or rec.get("provider") or ""),
        )
        flagged.append(rec)
    return accepted, flagged


def filter_split_like_price_tuples(
    con: Any,
    rows: Sequence[tuple[Any, ...]],
    *,
    source: str = "",
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    mappings = [
        {"ts_ms": row[0], "symbol": row[1], "price": row[2], "source": source, "_row": tuple(row)}
        for row in rows or []
        if len(tuple(row)) >= 3
    ]
    accepted, flagged = filter_split_like_price_rows(con, mappings)
    return [tuple(row.get("_row") or ()) for row in accepted], [tuple(row.get("_row") or ()) for row in flagged]
