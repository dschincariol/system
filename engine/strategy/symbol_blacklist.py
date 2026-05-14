"""
FILE: symbol_blacklist.py

SQLite-backed symbol blacklist helpers. This module provides a simple persistent
way for risk and governance logic to suppress problematic symbols temporarily or
permanently.
"""

import json
import time
import logging
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

SCHEMA = """
CREATE TABLE IF NOT EXISTS symbol_blacklist (
  symbol TEXT PRIMARY KEY,
  until_ts_ms INTEGER,
  reason TEXT NOT NULL,
  score REAL NOT NULL DEFAULT 0,
  meta_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_symbol_blacklist_until
  ON symbol_blacklist(until_ts_ms);
"""
LOG = get_logger("strategy.symbol_blacklist")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_symbol_blacklist_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.symbol_blacklist",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def init_blacklist() -> None:
    con = connect()
    try:
        con.executescript(SCHEMA)
        con.commit()
    finally:
        con.close()


def is_blacklisted(con, symbol: str, now_ms: Optional[int] = None) -> bool:
    init_blacklist()
    if now_ms is None:
        now_ms = _now_ms()

    row = con.execute(
        """
        SELECT until_ts_ms
        FROM symbol_blacklist
        WHERE symbol=?
        """,
        (str(symbol).upper().strip(),),
    ).fetchone()

    if not row:
        return False

    until_ts = row[0]
    if until_ts is None:
        return True  # `NULL until_ts_ms` means a permanent blacklist entry.
    try:
        return int(until_ts) > int(now_ms)
    except Exception as e:
        _warn_nonfatal(
            "SYMBOL_BLACKLIST_UNTIL_TS_PARSE_FAILED",
            e,
            once_key="until_ts_parse",
            symbol=repr(symbol)[:120],
        )
        return True


def get_blacklisted_symbols(con, now_ms: Optional[int] = None, limit: int = 5000) -> List[str]:
    init_blacklist()
    if now_ms is None:
        now_ms = _now_ms()

    rows = con.execute(
        """
        SELECT symbol, until_ts_ms
        FROM symbol_blacklist
        ORDER BY COALESCE(until_ts_ms, 9223372036854775807) DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    out = []
    for sym, until_ts in rows or []:
        try:
            sym = str(sym).upper().strip()
            if not sym:
                continue
            if until_ts is None:
                out.append(sym)
                continue
            if int(until_ts) > int(now_ms):
                out.append(sym)
        except Exception as e:
            _warn_nonfatal(
                "SYMBOL_BLACKLIST_ROW_PARSE_FAILED",
                e,
                once_key="row_parse",
                symbol=repr(sym)[:120],
            )
            continue
    return out


def upsert_blacklist(
    con,
    symbol: str,
    reason: str,
    score: float,
    ttl_s: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    init_blacklist()
    sym = str(symbol).upper().strip()
    until_ts = None
    if ttl_s is not None:
        until_ts = _now_ms() + int(ttl_s) * 1000

    con.execute(
        """
        INSERT INTO symbol_blacklist(symbol, until_ts_ms, reason, score, meta_json)
        VALUES (?,?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
          until_ts_ms=excluded.until_ts_ms,
          reason=excluded.reason,
          score=excluded.score,
          meta_json=excluded.meta_json
        """,
        (sym, int(until_ts) if until_ts is not None else None, str(reason), float(score), json.dumps(meta or {}, separators=(",", ":"), sort_keys=True)),
    )


def clear_blacklist(con, symbol: str) -> None:
    init_blacklist()
    con.execute("DELETE FROM symbol_blacklist WHERE symbol=?", (str(symbol).upper().strip(),))
