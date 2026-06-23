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


try:
    from engine.data.fx_instrument import parse_fx_symbol  # type: ignore
except Exception as e:
    _warn_nonfatal(
        "DATA_UNIVERSE_FX_INSTRUMENT_IMPORT_FAILED",
        e,
        once_key="fx_instrument_import",
    )

    def parse_fx_symbol(symbol: object):  # type: ignore
        return None


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
_INSTRUMENT_METADATA_COLUMNS = (
    "instrument_kind",
    "base_ccy",
    "quote_ccy",
    "pip_size",
    "contract_size",
    "pnl_ccy",
    "leverage_cap",
    "session_calendar",
    "instrument_meta_source",
)


def _instrument_column_values(metadata) -> Tuple[object, ...]:
    if metadata is None:
        return (None,) * len(_INSTRUMENT_METADATA_COLUMNS)
    return (
        metadata.instrument_kind,
        metadata.base_ccy,
        metadata.quote_ccy,
        float(metadata.pip_size),
        float(metadata.contract_size),
        metadata.pnl_ccy,
        float(metadata.leverage_cap),
        metadata.session_calendar,
        metadata.source,
    )


def _missing_instrument_column_error(error: BaseException) -> bool:
    message = str(error or "").lower()
    return (
        "instrument_kind" in message
        or "base_ccy" in message
        or "quote_ccy" in message
        or "pip_size" in message
        or "contract_size" in message
        or "pnl_ccy" in message
        or "leverage_cap" in message
        or "session_calendar" in message
        or "instrument_meta_source" in message
        or "no such column" in message
        or "has no column named" in message
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _metadata_dict_from_row(symbol: str, row, fallback) -> Optional[Dict]:
    if row is None:
        return fallback.to_dict() if fallback is not None else None
    try:
        instrument_kind = row[0]
        if not instrument_kind:
            return fallback.to_dict() if fallback is not None else None
        base_ccy = row[1]
        quote_ccy = row[2]
        pip_size = row[3]
        contract_size = row[4]
        pnl_ccy = row[5]
        leverage_cap = row[6]
        session_calendar = row[7]
        source = row[8] if len(row) > 8 else None
        return {
            "asset_class": "FX",
            "base_ccy": str(base_ccy) if base_ccy is not None else None,
            "contract_size": float(contract_size),
            "instrument_kind": str(instrument_kind),
            "leverage_cap": float(leverage_cap),
            "pip_size": float(pip_size),
            "pnl_ccy": str(pnl_ccy or ""),
            "quote_ccy": str(quote_ccy) if quote_ccy is not None else None,
            "session_calendar": str(session_calendar or ""),
            "source": str(source or "parser"),
            "symbol": str(symbol),
        }
    except Exception as e:
        _warn_nonfatal(
            "UNIVERSE_INSTRUMENT_METADATA_ROW_PARSE_FAILED",
            e,
            once_key=f"instrument_metadata_row:{symbol}",
            symbol=str(symbol),
        )
        return fallback.to_dict() if fallback is not None else None


def _insert_symbol_row(
    con,
    *,
    sym: str,
    ac: str,
    st: Optional[str],
    new_score: float,
    last_seen_event_ts_ms: Optional[int],
    meta_json: str,
    now_ms: int,
    instrument_metadata,
) -> None:
    try:
        con.execute(
            """
            INSERT OR IGNORE INTO symbols(
              symbol, asset_class, status, score,
              last_seen_event_ts_ms, meta_json,
              instrument_kind, base_ccy, quote_ccy, pip_size,
              contract_size, pnl_ccy, leverage_cap, session_calendar,
              instrument_meta_source,
              created_ts_ms, updated_ts_ms
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                sym,
                ac or "UNKNOWN",
                (st or "WATCH"),
                float(new_score),
                int(last_seen_event_ts_ms) if last_seen_event_ts_ms is not None else None,
                meta_json,
                *_instrument_column_values(instrument_metadata),
                now_ms,
                now_ms,
            ),
        )
    except Exception as e:
        if not _missing_instrument_column_error(e):
            raise
        _warn_nonfatal(
            "UNIVERSE_SYMBOL_INSERT_INSTRUMENT_COLUMNS_UNAVAILABLE",
            e,
            once_key="symbol_insert_instrument_columns_unavailable",
            symbol=str(sym),
        )
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
                meta_json,
                now_ms,
                now_ms,
            ),
        )


def _update_symbol_row(
    con,
    *,
    sym: str,
    new_ac: str,
    new_status: str,
    new_score: float,
    last_seen_event_ts_ms: Optional[int],
    meta_json: str,
    now_ms: int,
    instrument_metadata,
) -> None:
    try:
        con.execute(
            """
            UPDATE symbols SET
              asset_class=?,
              status=?,
              score=?,
              last_seen_event_ts_ms=COALESCE(?, last_seen_event_ts_ms),
              meta_json=?,
              instrument_kind=?,
              base_ccy=?,
              quote_ccy=?,
              pip_size=?,
              contract_size=?,
              pnl_ccy=?,
              leverage_cap=?,
              session_calendar=?,
              instrument_meta_source=?,
              updated_ts_ms=?
            WHERE symbol=?
            """,
            (
                new_ac,
                new_status,
                float(new_score),
                int(last_seen_event_ts_ms) if last_seen_event_ts_ms is not None else None,
                meta_json,
                *_instrument_column_values(instrument_metadata),
                now_ms,
                sym,
            ),
        )
    except Exception as e:
        if not _missing_instrument_column_error(e):
            raise
        _warn_nonfatal(
            "UNIVERSE_SYMBOL_UPDATE_INSTRUMENT_COLUMNS_UNAVAILABLE",
            e,
            once_key="symbol_update_instrument_columns_unavailable",
            symbol=str(sym),
        )
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
                meta_json,
                now_ms,
                sym,
            ),
        )


def get_instrument_metadata(con, symbol) -> Optional[Dict]:
    """Return FX instrument metadata, or ``None`` for non-FX symbols.

    This accessor is the FX-02 single source of truth that FX-03/04/05/06/07
    MUST consume as ``from engine.data.universe import get_instrument_metadata``.
    For FX spot pairs, the returned ``symbol`` is the canonical stored form:
    uppercase 6-letter ``BASE+QUOTE`` with no separator, such as ``EURUSD``.
    Downstream code that receives broker-style keys such as ``EUR_USD`` must
    normalize through ``parse_fx_symbol(sym).symbol`` or this accessor before
    keying feature, cost, risk, execution, or UI tables.
    """
    parsed = parse_fx_symbol(symbol)
    if parsed is None:
        return None
    canonical = str(parsed.symbol)
    try:
        row = con.execute(
            """
            SELECT instrument_kind, base_ccy, quote_ccy, pip_size,
                   contract_size, pnl_ccy, leverage_cap, session_calendar,
                   instrument_meta_source
            FROM symbols
            WHERE symbol=?
            """,
            (canonical,),
        ).fetchone()
    except Exception as e:
        if not _missing_instrument_column_error(e):
            _warn_nonfatal(
                "UNIVERSE_INSTRUMENT_METADATA_LOOKUP_FAILED",
                e,
                once_key=f"instrument_metadata_lookup:{canonical}",
                symbol=canonical,
            )
        else:
            _warn_nonfatal(
                "UNIVERSE_INSTRUMENT_METADATA_COLUMNS_UNAVAILABLE",
                e,
                once_key="instrument_metadata_columns_unavailable",
                symbol=canonical,
            )
        return parsed.to_dict()
    return _metadata_dict_from_row(canonical, row, parsed)


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
    raw_sym = str(symbol or "").upper().strip()
    instrument_metadata = parse_fx_symbol(raw_sym)
    sym = str(instrument_metadata.symbol if instrument_metadata is not None else raw_sym).upper().strip()
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

        _insert_symbol_row(
            con,
            sym=sym,
            ac=ac,
            st=st,
            new_score=new_score,
            last_seen_event_ts_ms=last_seen_event_ts_ms,
            meta_json=mj,
            now_ms=now_ms,
            instrument_metadata=instrument_metadata,
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
    merged_meta_json = json.dumps(merged, separators=(",", ":"), sort_keys=True)

    _update_symbol_row(
        con,
        sym=sym,
        new_ac=new_ac,
        new_status=new_status,
        new_score=new_score,
        last_seen_event_ts_ms=last_seen_event_ts_ms,
        meta_json=merged_meta_json,
        now_ms=now_ms,
        instrument_metadata=instrument_metadata,
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
