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
from typing import Optional, Dict, Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("execution.broker_fill_utils")

_WARNED_NONFATAL_KEYS: set[str] = set()


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

        rows = con.execute(
            """
            SELECT price, qty, side, ts_ms, fees
            FROM broker_fills
            WHERE symbol=?
              AND ts_ms BETWEEN ? AND ?
            ORDER BY ts_ms ASC
            """,
            (
                symbol,
                int(entry_ts_ms - BUFFER_MS),
                int(exit_ts_ms + BUFFER_MS),
            ),
        ).fetchall()

        if not rows:
            return None

        entry_rows = []
        exit_rows = []
        fees_total = 0.0
        side = None

        for price, qty, s, ts, fees in rows:
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

        return {
            "side": 1 if str(side).upper().startswith("B") else -1,
            "px_in": float(px_in),
            "px_out": float(px_out) if px_out is not None else None,
            "fees_total": float(fees_total),
        }

    finally:
        con.close()
