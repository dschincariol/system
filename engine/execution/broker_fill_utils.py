"""
FILE: broker_fill_utils.py

Execution subsystem module for `broker_fill_utils`.
"""

# dev_core/broker_fill_utils.py
"""
Utilities to aggregate broker fills into realized entry/exit prices.
"""

from datetime import datetime, timezone
import logging
import json
from typing import Optional, Dict, Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("execution.broker_fill_utils")

_WARNED_NONFATAL_KEYS: set[str] = set()
_NON_REAL_FILL_SOURCES = {
    "sim",
    "paper",
    "paper_sim",
    "shadow",
    "shadow_sim",
    "rl",
    "rl_shadow",
    "rl_sim",
    "sim_training",
    "training",
    "paper_training",
    "backtest",
    "offline_sim",
    "synthetic_training",
}


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="execution_broker_fill_utils_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.broker_fill_utils",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _vwap(rows):
    qty_sum = 0.0
    notional = 0.0
    for px, qty in rows:
        q = abs(float(qty))
        qty_sum += q
        notional += q * float(px)
    if qty_sum <= 0:
        return None
    return notional / qty_sum


def _broker_fills_columns(con) -> set[str]:
    try:
        rows = con.execute("PRAGMA table_info(broker_fills)").fetchall() or []
        return {str(row[1] or "").strip() for row in rows if len(row) > 1}
    except Exception as e:
        _warn_nonfatal(
            "BROKER_FILL_UTILS_COLUMNS_LOOKUP_FAILED",
            e,
            once_key="broker_fills_columns",
        )
        return set()


def _json_obj(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception as e:
        _warn_nonfatal(
            "BROKER_FILL_UTILS_JSON_PARSE_FAILED",
            e,
            once_key="json_obj_parse",
            value=repr(value)[:120],
        )
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _source_is_training_or_shadow(source: Any) -> bool:
    text = str(source or "").strip().lower()
    if not text:
        return False
    if text in _NON_REAL_FILL_SOURCES:
        return True
    return text.startswith("shadow") or text.startswith("rl_") or "training" in text


def _is_real_broker_fill(*, source: Any, book_key: Any, explain_json: Any) -> bool:
    if str(book_key or "").strip():
        return False
    if _source_is_training_or_shadow(source):
        return False
    explain = _json_obj(explain_json)
    if str(explain.get("book_key") or explain.get("shadow_book_key") or "").strip():
        return False
    if str(explain.get("source") or "").strip() and _source_is_training_or_shadow(explain.get("source")):
        return False
    if explain.get("shadow_model_id") not in (None, ""):
        return False
    return True


def parse_broker_timestamp_ms(value: Any, *, default_ms: Optional[int] = None) -> int:
    try:
        if value is None:
            raise ValueError("missing")
        if isinstance(value, (int, float)):
            raw = float(value)
            if raw > 1e12:
                return int(raw)
            if raw > 1e9:
                return int(raw * 1000.0)
    except Exception as e:
        _warn_nonfatal(
            "BROKER_FILL_UTILS_TIMESTAMP_NUMERIC_PARSE_FAILED",
            e,
            once_key="timestamp_numeric_parse",
            value=repr(value)[:120],
        )

    text = str(value or "").strip()
    if not text:
        return int(default_ms if default_ms is not None else int(datetime.now(tz=timezone.utc).timestamp() * 1000.0))

    last_fmt_error: Optional[BaseException] = None
    for fmt in ("%Y%m%d  %H:%M:%S", "%Y%m%d %H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000.0)
        except Exception as e:
            last_fmt_error = e

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000.0)
    except Exception as e:
        if last_fmt_error is not None:
            _warn_nonfatal(
                "BROKER_FILL_UTILS_TIMESTAMP_FORMAT_PARSE_FAILED",
                last_fmt_error,
                once_key="timestamp_format_parse",
                value=text[:120],
            )
        _warn_nonfatal(
            "BROKER_FILL_UTILS_TIMESTAMP_ISO_PARSE_FAILED",
            e,
            once_key="timestamp_iso_parse",
            value=text[:120],
        )
        return int(default_ms if default_ms is not None else int(datetime.now(tz=timezone.utc).timestamp() * 1000.0))


def get_realized_trade(
    *,
    symbol: str,
    entry_ts_ms: int,
    exit_ts_ms: int,
    book_key: Optional[str] = None,
    real_only: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Returns realized execution info using broker_fills.
    We assume:
      - entry fills between [entry_ts_ms, entry_ts_ms + small buffer]
      - exit fills between [exit_ts_ms - buffer, exit_ts_ms + buffer]
    """

    BUFFER_MS = 60_000  # 60s tolerance

    con = connect()
    try:
        # Fail-soft if labels table isn't created yet; this helper supports
        # downstream analytics and should not create bootstrap dependencies.
        try:
            con.execute("SELECT 1 FROM labels LIMIT 1").fetchone()
        except Exception as e:
            _warn_nonfatal(
                "BROKER_FILL_UTILS_LABELS_TABLE_LOOKUP_FAILED",
                e,
                once_key="labels_table_lookup",
            )
            return None

        cols = _broker_fills_columns(con)
        price_expr = "price" if "price" in cols else "px"
        side_expr = "side" if "side" in cols else "CASE WHEN qty >= 0 THEN 'BUY' ELSE 'SELL' END"
        fees_expr = "fees" if "fees" in cols else "0.0"
        source_expr = "source" if "source" in cols else "NULL"
        book_key_expr = "book_key" if "book_key" in cols else "NULL"
        explain_expr = "explain_json" if "explain_json" in cols else "NULL"
        predicates = [
            "UPPER(TRIM(symbol)) = UPPER(TRIM(?))",
            "ts_ms BETWEEN ? AND ?",
        ]
        params: list[Any] = [
            symbol,
            int(entry_ts_ms - BUFFER_MS),
            int(exit_ts_ms + BUFFER_MS),
        ]
        if real_only:
            if "book_key" in cols:
                predicates.append("COALESCE(NULLIF(TRIM(CAST(book_key AS TEXT)), ''), '') = ''")
            if "source" in cols:
                placeholders = ",".join("?" for _ in sorted(_NON_REAL_FILL_SOURCES))
                predicates.append(
                    f"LOWER(TRIM(COALESCE(CAST(source AS TEXT), 'real'))) NOT IN ({placeholders})"
                )
                params.extend(sorted(_NON_REAL_FILL_SOURCES))
        elif book_key is not None and "book_key" in cols:
            predicates.append("COALESCE(CAST(book_key AS TEXT), '') = ?")
            params.append(str(book_key))

        rows = con.execute(
            f"""
            SELECT {price_expr}, qty, {side_expr}, ts_ms, {fees_expr}, {source_expr}, {book_key_expr}, {explain_expr}
            FROM broker_fills
            WHERE {" AND ".join(predicates)}
            ORDER BY ts_ms ASC
            """,
            tuple(params),
        ).fetchall()

        if not rows:
            return None

        entry_rows = []
        exit_rows = []
        fees_total = 0.0
        side = None

        for price, qty, s, ts, fees, fill_source, fill_book_key, explain_json in rows:
            if real_only and not _is_real_broker_fill(
                source=fill_source,
                book_key=fill_book_key,
                explain_json=explain_json,
            ):
                continue
            fees_total += float(fees or 0.0)
            if side is None:
                side = s

            # heuristic split: early fills = entry, later fills = exit
            if ts <= entry_ts_ms + BUFFER_MS:
                entry_rows.append((price, qty))
            elif ts >= exit_ts_ms - BUFFER_MS:
                exit_rows.append((price, qty))

        if not entry_rows:
            return None

        px_in = _vwap(entry_rows)
        px_out = _vwap(exit_rows) if exit_rows else None

        if px_in is None:
            return None
        entry_qty = sum(abs(float(qty or 0.0)) for _price, qty in entry_rows)
        exit_qty = sum(abs(float(qty or 0.0)) for _price, qty in exit_rows)
        entry_notional = sum(abs(float(qty or 0.0)) * float(price or 0.0) for price, qty in entry_rows)
        exit_notional = sum(abs(float(qty or 0.0)) * float(price or 0.0) for price, qty in exit_rows)

        return {
            "side": 1 if str(side).upper().startswith("B") else -1,
            "px_in": float(px_in),
            "px_out": float(px_out) if px_out is not None else None,
            "qty": float(entry_qty),
            "entry_qty": float(entry_qty),
            "exit_qty": float(exit_qty),
            "entry_notional": float(entry_notional),
            "exit_notional": float(exit_notional),
            "fees_total": float(fees_total),
        }

    finally:
        con.close()
