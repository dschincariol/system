"""
FILE: universe.py

Data subsystem module for `universe`.
"""

# dev_core/universe.py
"""
Dynamic trading universe registry.

Responsibilities:
- Maintain symbols table (WATCH/ACTIVE/COOLDOWN/DISABLED)
- Provide helper APIs to fetch the current universe for other jobs
- Provide lightweight event->candidate extraction (safe defaults)

This module is intentionally conservative:
- It never executes trades
- It does not require external APIs
- It is safe if events are noisy (hard filters applied elsewhere)
"""

import json
import logging
import re
import time
from typing import Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.universe")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_universe_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.universe",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

# Optional: asset-class mapping if available (your repo has asset_map.py at project root)
try:
    from engine.data.asset_map import asset_class_for_symbol  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "DATA_UNIVERSE_ASSET_CLASS_IMPORT_FAILED",
        e,
        once_key="asset_class_import",
    )
    def asset_class_for_symbol(symbol: str) -> str:  # fallback
        s = str(symbol or "").upper().strip()
        if not s:
            return "UNKNOWN"
        if s in ("BTC", "ETH", "SOL", "BNB", "XRP"):
            return "CRYPTO"
        if s in ("SPY", "QQQ", "DIA", "IWM", "VTI", "VOO"):
            return "EQUITY"
        if s in ("CL", "NG", "GC", "SI", "OIL", "GOLD", "SILVER"):
            return "COMMODITY"
        return "UNKNOWN"


# Very conservative ticker extraction:
# - captures $TSLA or TSLA
# - rejects too-short/too-long
# - rejects common English words (small denylist)
_TICKER_RX = re.compile(r"(?:\$(?P<t1>[A-Z]{2,6})\b|\b(?P<t2>[A-Z]{2,6})\b)")

_DENY = {
    "THE", "AND", "FOR", "WITH", "THIS", "THAT", "FROM", "HAVE", "WILL", "YOUR",
    "USD", "FED", "FOMC", "CEO", "CPI", "PCE", "GDP", "SEC", "ETF", "OPEC",
}

_VALID_STATUS = {"WATCH", "ACTIVE", "COOLDOWN", "DISABLED"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def extract_symbol_candidates(text: str) -> List[str]:
    """
    Extract uppercase ticker-like tokens. Safe, noisy, best-effort.
    """
    t = (text or "").strip()
    if not t:
        return []
    out = []
    for m in _TICKER_RX.finditer(t):
        sym = (m.group("t1") or m.group("t2") or "").strip().upper()
        if not sym:
            continue
        if sym in _DENY:
            continue
        # avoid single-letter, avoid weird tickers; keep simple
        if len(sym) < 2 or len(sym) > 6:
            continue
        out.append(sym)
    # de-dupe preserving order
    seen = set()
    dedup = []
    for s in out:
        if s in seen:
            continue
        seen.add(s)
        dedup.append(s)
    return dedup


def upsert_symbol(
    con,
    symbol: str,
    *,
    asset_class: Optional[str] = None,
    status: Optional[str] = None,
    score_delta: float = 0.0,
    last_seen_event_ts_ms: Optional[int] = None,
    meta: Optional[Dict] = None,
) -> None:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return

    now_ms = _now_ms()
    ac = str(asset_class_for_symbol(sym) if asset_class is None else asset_class).upper().strip()
    st = status.upper().strip() if isinstance(status, str) else None
    if st is not None and st not in _VALID_STATUS:
        st = None

    try:
        row = con.execute(
            "SELECT score, status, asset_class, meta_json FROM symbols WHERE symbol=?",
            (sym,),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_SYMBOL_LOOKUP_FAILED",
            e,
            once_key=f"symbol_lookup:{sym}",
            symbol=str(sym),
        )
        row = None

    if row is None:
        base_score = 0.0
        new_score = float(base_score + float(score_delta))
        mj = json.dumps(meta or {}, separators=(",", ":"), sort_keys=True)
        

        con.execute(
            """
            INSERT OR IGNORE INTO symbols(
              symbol, asset_class, status, score,
              last_seen_event_ts_ms, meta_json,
              created_ts_ms, updated_ts_ms
            )
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                sym,
                ac or "UNKNOWN",
                (st or "WATCH"),
                float(new_score),
                int(last_seen_event_ts_ms) if last_seen_event_ts_ms is not None else None,
                mj,
                now_ms,
                now_ms,
            ),
        )
        return

    # Existing rows are merged rather than replaced so independent discovery
    # signals can accumulate over time in score and metadata.
    # existing row
    try:
        cur_score = float(row[0] or 0.0)
    except Exception:
        cur_score = 0.0

    cur_status = str(row[1] or "WATCH").upper()
    cur_ac = str(row[2] or "UNKNOWN").upper()
    cur_meta_json = row[3] if len(row) >= 4 else None

    # merge meta
    merged = {}
    try:
        merged = json.loads(cur_meta_json) if cur_meta_json else {}
        if not isinstance(merged, dict):
            merged = {}
    except Exception:
        merged = {}

    if isinstance(meta, dict):
        merged.update(meta)

    new_score = float(cur_score + float(score_delta))
    new_status = st or cur_status
    new_ac = ac or cur_ac

    con.execute(
        """
        UPDATE symbols SET
          asset_class=?,
          status=?,
          score=?,
          last_seen_event_ts_ms=COALESCE(?, last_seen_event_ts_ms),
          meta_json=?,
          updated_ts_ms=?
        WHERE symbol=?
        """,
        (
            new_ac,
            new_status,
            float(new_score),
            int(last_seen_event_ts_ms) if last_seen_event_ts_ms is not None else None,
            json.dumps(merged, separators=(",", ":"), sort_keys=True),
            now_ms,
            sym,
        ),
    )


def _normalized_limit(limit: Optional[int]) -> Optional[int]:
    if limit is None:
        return None
    try:
        value = int(limit)
    except Exception as e:
        _warn_nonfatal(
            "DATA_UNIVERSE_LIMIT_PARSE_FAILED",
            e,
            once_key="normalized_limit",
            limit=repr(limit)[:120],
        )
        return None
    if value <= 0:
        return None
    return int(value)


def get_symbols_by_status(con, statuses: List[str], limit: Optional[int] = 2000) -> List[str]:
    sts = [str(s).upper().strip() for s in (statuses or []) if str(s).strip()]
    if not sts:
        return []
    q = ",".join("?" for _ in sts)
    normalized_limit = _normalized_limit(limit)
    sql = f"""
        SELECT symbol
        FROM symbols
        WHERE status IN ({q})
        ORDER BY score DESC, updated_ts_ms DESC
    """
    params: Tuple[object, ...]
    if normalized_limit is None:
        params = tuple(sts)
    else:
        sql += "\n        LIMIT ?"
        params = (*sts, int(normalized_limit))
    rows = con.execute(sql, params).fetchall()
    return [str(r[0]) for r in rows or []]


def get_active_symbols(con, limit: Optional[int] = 2000) -> List[str]:
    # ACTIVE first, then WATCH. This is the core exploration/exploitation balance
    # consumed by pollers and event processors across the data pipeline.
    # ACTIVE first, then WATCH (so the universe can explore)
    normalized_limit = _normalized_limit(limit)
    active = get_symbols_by_status(con, ["ACTIVE"], limit=normalized_limit)
    if normalized_limit is None:
        active_set = set(active)
        watch = get_symbols_by_status(con, ["WATCH"], limit=None)
        return active + [sym for sym in watch if sym not in active_set]
    if len(active) >= int(normalized_limit):
        return active[: int(normalized_limit)]
    watch = get_symbols_by_status(con, ["WATCH"], limit=int(normalized_limit) - len(active))
    return active + watch


def get_universe_snapshot(con, limit: int = 5000) -> List[Dict]:
    rows = con.execute(
        """
        SELECT symbol, asset_class, status, score, last_seen_event_ts_ms, updated_ts_ms, meta_json
        FROM symbols
        ORDER BY
          CASE status
            WHEN 'ACTIVE' THEN 0
            WHEN 'WATCH' THEN 1
            WHEN 'COOLDOWN' THEN 2
            ELSE 3
          END,
          score DESC,
          updated_ts_ms DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall()

    out = []
    for r in rows or []:
        try:
            meta = json.loads(r[6]) if r[6] else {}
            if not isinstance(meta, dict):
                meta = {}
        except Exception:
            meta = {}
        out.append(
            {
                "symbol": str(r[0]),
                "asset_class": str(r[1] or "UNKNOWN"),
                "status": str(r[2] or "WATCH"),
                "score": float(r[3] or 0.0),
                "last_seen_event_ts_ms": int(r[4]) if r[4] is not None else None,
                "updated_ts_ms": int(r[5]) if r[5] is not None else None,
                "meta": meta,
            }
        )
    return out
