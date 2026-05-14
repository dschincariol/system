"""
FILE: options_surface_intelligence.py

Builds higher-level options-surface features from raw option-chain snapshots.
This module turns skew/term-structure information into factor features that the
rest of the strategy stack can consume.
"""

import math
import logging
import statistics
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.factor_universe import put_factor_feature


_SURFACE_Z_WINDOW = 240
_SURFACE_D5_LAG = 5
_SNAPSHOT_STALE_MS = 15 * 60 * 1000
_VOL_OF_VOL_LOOKBACK_MS = 24 * 3600 * 1000
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("strategy.options_surface_intelligence")


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_options_surface_intelligence_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.options_surface_intelligence",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_f(x: Any, d: Optional[float] = None) -> Optional[float]:
    try:
        v = float(x)
    except Exception as e:
        _warn_nonfatal("OPTIONS_SURFACE_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(x)[:120])
        return d
    if not math.isfinite(v):
        return d
    return v


def _safe_pos(x: Any) -> Optional[float]:
    v = _safe_f(x, None)
    if v is None or v <= 0.0:
        return None
    return float(v)


def _safe_str(x: Any) -> str:
    return str(x or "").strip()


def _days_to_exp(expiry: str, ts_ms: int) -> Optional[float]:
    try:
        dt = datetime.strptime(str(expiry), "%Y-%m-%d")
    except Exception as e:
        _warn_nonfatal("OPTIONS_SURFACE_EXPIRY_PARSE_FAILED", e, once_key="days_to_exp", expiry=str(expiry), ts_ms=int(ts_ms))
        return None
    days = (dt.timestamp() * 1000.0 - float(ts_ms)) / 86400000.0
    if not math.isfinite(days):
        return None
    return float(days)


def _pick_latest_ts_v2(con, underlying: str) -> Optional[int]:
    row = con.execute(
        """
        SELECT MAX(ts_ms)
        FROM options_chain_v2
        WHERE underlying=?
        """,
        (str(underlying),),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _load_snapshot_rows_v2(con, underlying: str, ts_ms: int) -> List[Tuple]:
    rows = con.execute(
        """
        SELECT expiration, contract_type, strike, iv, delta
        FROM options_chain_v2
        WHERE underlying=?
          AND ts_ms >= ?
          AND ts_ms <= ?
          AND expiration IS NOT NULL
          AND iv IS NOT NULL
        ORDER BY expiration ASC, ts_ms DESC
        """,
        (str(underlying), int(ts_ms) - _SNAPSHOT_STALE_MS, int(ts_ms)),
    ).fetchall()
    return list(rows or [])


def _pick_latest_ts_v1(con, underlying: str) -> Optional[int]:
    row = con.execute(
        """
        SELECT MAX(ts_ms)
        FROM options_chain
        WHERE symbol=?
        """,
        (str(underlying),),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _load_snapshot_rows_v1(con, underlying: str, ts_ms: int) -> List[Tuple]:
    rows = con.execute(
        """
        SELECT expiry, call_put, strike, iv, NULL as delta
        FROM options_chain
        WHERE symbol=?
          AND ts_ms >= ?
          AND ts_ms <= ?
          AND expiry IS NOT NULL
          AND iv IS NOT NULL
        ORDER BY expiry ASC, ts_ms DESC
        """,
        (str(underlying), int(ts_ms) - _SNAPSHOT_STALE_MS, int(ts_ms)),
    ).fetchall()
    return list(rows or [])


def _normalize_contract_type(v: Any) -> str:
    s = _safe_str(v).lower()
    if s in ("call", "c"):
        return "call"
    if s in ("put", "p"):
        return "put"
    return ""


def _group_by_expiry(rows: Iterable[Tuple]) -> Dict[str, List[Dict[str, Any]]]:
    # Normalize raw chain rows into a structure that downstream expiry logic can
    # reuse for both v1 and v2 chain sources.
    out: Dict[str, List[Dict[str, Any]]] = {}
    for expiry, contract_type, strike, iv, delta in rows:
        exp = _safe_str(expiry)
        if not exp:
            continue
        out.setdefault(exp, []).append(
            {
                "contract_type": _normalize_contract_type(contract_type),
                "strike": _safe_f(strike, None),
                "iv": _safe_pos(iv),
                "delta": _safe_f(delta, None),
            }
        )
    return out


def _sorted_future_expiries(grouped: Dict[str, List[Dict[str, Any]]], ts_ms: int) -> List[str]:
    out: List[Tuple[float, str]] = []
    for exp in grouped.keys():
        dte = _days_to_exp(exp, ts_ms)
        if dte is None or dte <= 1.0:
            continue
        out.append((dte, exp))
    out.sort(key=lambda x: x[0])
    return [exp for _, exp in out]


def _closest_by_abs_delta(rows: List[Dict[str, Any]], want_type: str, target_abs_delta: float) -> Optional[Dict[str, Any]]:
    best = None
    best_dist = None
    for r in rows:
        if r.get("contract_type") != want_type:
            continue
        iv = _safe_pos(r.get("iv"))
        delta = _safe_f(r.get("delta"), None)
        if iv is None or delta is None:
            continue
        dist = abs(abs(float(delta)) - float(target_abs_delta))
        if best is None or best_dist is None or dist < best_dist:
            best = r
            best_dist = dist
    return best


def _closest_atm_iv(rows: List[Dict[str, Any]]) -> Optional[float]:
    best_iv = None
    best_dist = None
    strikes = [float(r["strike"]) for r in rows if _safe_f(r.get("strike"), None) is not None]
    strike_mid = statistics.median(strikes) if strikes else None
    for r in rows:
        iv = _safe_pos(r.get("iv"))
        if iv is None:
            continue
        delta = _safe_f(r.get("delta"), None)
        strike = _safe_f(r.get("strike"), None)
        if delta is not None:
            dist = abs(abs(float(delta)) - 0.50)
        elif strike_mid is not None and strike is not None:
            dist = abs(float(strike) - float(strike_mid))
        else:
            dist = 999999.0
        if best_iv is None or best_dist is None or dist < best_dist:
            best_iv = float(iv)
            best_dist = dist
    return best_iv


def _compute_surface_row_from_snapshot(grouped: Dict[str, List[Dict[str, Any]]], underlying: str, ts_ms: int, source: str) -> Optional[Dict[str, Any]]:
    expiries = _sorted_future_expiries(grouped, ts_ms)
    if not expiries:
        return None

    expiry_near = expiries[0]
    expiry_next = expiries[1] if len(expiries) > 1 else None

    near_rows = grouped.get(expiry_near) or []
    next_rows = grouped.get(expiry_next) or [] if expiry_next is not None else []

    call_25 = _closest_by_abs_delta(near_rows, "call", 0.25)
    put_25 = _closest_by_abs_delta(near_rows, "put", 0.25)

    skew_25d = None
    if call_25 and put_25:
        call_iv = _safe_pos(call_25.get("iv"))
        put_iv = _safe_pos(put_25.get("iv"))
        if call_iv is not None and put_iv is not None:
            skew_25d = float(put_iv - call_iv)

    atm_iv_near = _closest_atm_iv(near_rows)
    atm_iv_next = _closest_atm_iv(next_rows) if expiry_next else None

    term_structure_slope = None
    if expiry_next and atm_iv_near is not None and atm_iv_next is not None:
        near_dte = _days_to_exp(expiry_near, ts_ms)
        next_dte = _days_to_exp(expiry_next, ts_ms)
        if near_dte is not None and next_dte is not None:
            dt = float(next_dte - near_dte)
            if dt > 0.25:
                term_structure_slope = float((float(atm_iv_next) - float(atm_iv_near)) / dt)

    return {
        "ts_ms": int(ts_ms),
        "underlying": str(underlying),
        "expiry_near": str(expiry_near),
        "expiry_next": (str(expiry_next) if expiry_next else None),
        "skew_25d": skew_25d,
        "term_structure_slope": term_structure_slope,
        "atm_iv_near": atm_iv_near,
        "atm_iv_next": atm_iv_next,
        "vol_of_vol_1d": None,
        "source": str(source or "polygon"),
    }


def _compute_vol_of_vol_from_history(con, underlying: str, ts_ms: int) -> Optional[float]:
    rows = con.execute(
        """
        SELECT atm_iv_near
        FROM options_surface
        WHERE underlying=?
          AND ts_ms >= ?
          AND ts_ms <= ?
          AND atm_iv_near IS NOT NULL
        ORDER BY ts_ms ASC
        """,
        (str(underlying), int(ts_ms) - _VOL_OF_VOL_LOOKBACK_MS, int(ts_ms)),
    ).fetchall()
    xs = [float(r[0]) for r in (rows or []) if r and r[0] is not None]
    if len(xs) < 8:
        return None
    try:
        return float(statistics.pstdev(xs))
    except Exception as e:
        _warn_nonfatal("OPTIONS_SURFACE_VOL_OF_VOL_FAILED", e, once_key=f"vol_of_vol:{underlying}", underlying=str(underlying), ts_ms=int(ts_ms))
        return None


def _put_surface_row(con, row: Dict[str, Any]) -> None:
    con.execute(
        """
        INSERT INTO options_surface(
          ts_ms, underlying, expiry_near, expiry_next,
          skew_25d, term_structure_slope, atm_iv_near, atm_iv_next,
          vol_of_vol_1d, source
        )
        VALUES (?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(underlying, ts_ms) DO UPDATE SET
          expiry_near=excluded.expiry_near,
          expiry_next=excluded.expiry_next,
          skew_25d=excluded.skew_25d,
          term_structure_slope=excluded.term_structure_slope,
          atm_iv_near=excluded.atm_iv_near,
          atm_iv_next=excluded.atm_iv_next,
          vol_of_vol_1d=excluded.vol_of_vol_1d,
          source=excluded.source
        """,
        (
            int(row["ts_ms"]),
            str(row["underlying"]),
            (str(row["expiry_near"]) if row.get("expiry_near") is not None else None),
            (str(row["expiry_next"]) if row.get("expiry_next") is not None else None),
            (_safe_f(row.get("skew_25d"), None) if row.get("skew_25d") is not None else None),
            (_safe_f(row.get("term_structure_slope"), None) if row.get("term_structure_slope") is not None else None),
            (_safe_f(row.get("atm_iv_near"), None) if row.get("atm_iv_near") is not None else None),
            (_safe_f(row.get("atm_iv_next"), None) if row.get("atm_iv_next") is not None else None),
            (_safe_f(row.get("vol_of_vol_1d"), None) if row.get("vol_of_vol_1d") is not None else None),
            str(row.get("source") or "polygon"),
        ),
    )


def _put_agg_row(con, ts_ms: int, rows: List[Dict[str, Any]], source: str) -> Dict[str, Any]:
    skews = [float(r["skew_25d"]) for r in rows if r.get("skew_25d") is not None]
    slopes = [float(r["term_structure_slope"]) for r in rows if r.get("term_structure_slope") is not None]
    vovs = [float(r["vol_of_vol_1d"]) for r in rows if r.get("vol_of_vol_1d") is not None]

    agg = {
        "ts_ms": int(ts_ms),
        "skew_25d": (sum(skews) / len(skews) if skews else None),
        "term_structure_slope": (sum(slopes) / len(slopes) if slopes else None),
        "vol_of_vol_1d": (sum(vovs) / len(vovs) if vovs else None),
        "sample_n": int(len(rows)),
        "source": str(source or "polygon"),
    }

    con.execute(
        """
        INSERT INTO options_surface_agg(
          ts_ms, skew_25d, term_structure_slope, vol_of_vol_1d, sample_n, source
        )
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(ts_ms) DO UPDATE SET
          skew_25d=excluded.skew_25d,
          term_structure_slope=excluded.term_structure_slope,
          vol_of_vol_1d=excluded.vol_of_vol_1d,
          sample_n=excluded.sample_n,
          source=excluded.source
        """,
        (
            int(agg["ts_ms"]),
            (_safe_f(agg["skew_25d"], None) if agg.get("skew_25d") is not None else None),
            (_safe_f(agg["term_structure_slope"], None) if agg.get("term_structure_slope") is not None else None),
            (_safe_f(agg["vol_of_vol_1d"], None) if agg.get("vol_of_vol_1d") is not None else None),
            int(agg["sample_n"]),
            str(agg["source"]),
        ),
    )
    return agg


def _series_for_feature(con, feature_col: str, limit: int) -> List[float]:
    rows = con.execute(
        f"""
        SELECT {feature_col}
        FROM (
          SELECT ts_ms, {feature_col}
          FROM options_surface_agg
          WHERE {feature_col} IS NOT NULL
          ORDER BY ts_ms DESC
          LIMIT ?
        ) q
        ORDER BY ts_ms ASC
        """,
        (int(limit),),
    ).fetchall()
    return [float(r[0]) for r in (rows or []) if r and r[0] is not None]


def _zscore_latest(xs: List[float]) -> float:
    if len(xs) < 30:
        return 0.0
    win = xs[-_SURFACE_Z_WINDOW:] if len(xs) > _SURFACE_Z_WINDOW else xs
    if len(win) < 30:
        return 0.0
    mu = sum(win) / len(win)
    var = sum((x - mu) ** 2 for x in win) / len(win)
    sd = math.sqrt(var)
    if sd <= 1e-12:
        return 0.0
    return float((win[-1] - mu) / sd)


def _delta_latest(xs: List[float], lag: int) -> float:
    if len(xs) <= lag:
        return 0.0
    return float(xs[-1] - xs[-1 - lag])


def compute_options_surface_intelligence(con, underlyings: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    syms = [str(s).upper().strip() for s in (underlyings or []) if str(s).strip()]
    if not syms:
        rows = con.execute(
            """
            SELECT DISTINCT underlying
            FROM options_chain_v2
            WHERE underlying IS NOT NULL AND underlying <> ''
            ORDER BY underlying
            """
        ).fetchall()
        syms = [str(r[0]).upper().strip() for r in (rows or []) if r and r[0]]

    updated_rows: List[Dict[str, Any]] = []

    for sym in syms:
        ts_v2 = _pick_latest_ts_v2(con, sym)
        source = "polygon"
        if ts_v2 is not None:
            grouped = _group_by_expiry(_load_snapshot_rows_v2(con, sym, ts_v2))
            ts_ref = int(ts_v2)
        else:
            ts_v1 = _pick_latest_ts_v1(con, sym)
            if ts_v1 is None:
                continue
            grouped = _group_by_expiry(_load_snapshot_rows_v1(con, sym, ts_v1))
            ts_ref = int(ts_v1)
            source = "legacy"

        row = _compute_surface_row_from_snapshot(grouped, sym, ts_ref, source)
        if not row:
            continue

        _put_surface_row(con, row)
        row["vol_of_vol_1d"] = _compute_vol_of_vol_from_history(con, sym, int(row["ts_ms"]))
        _put_surface_row(con, row)
        updated_rows.append(row)

    if not updated_rows:
        return {"updated": 0, "features": 0}

    latest_ts = max(int(r["ts_ms"]) for r in updated_rows)
    agg = _put_agg_row(con, latest_ts, updated_rows, "options_surface_intelligence")

    skew_series = _series_for_feature(con, "skew_25d", _SURFACE_Z_WINDOW + 32)
    slope_series = _series_for_feature(con, "term_structure_slope", _SURFACE_Z_WINDOW + 32)
    vov_series = _series_for_feature(con, "vol_of_vol_1d", _SURFACE_Z_WINDOW + 32)

    put_factor_feature(
        con,
        feature_id="options.surface_skew_z",
        asof_ts=int(latest_ts),
        effective_ts=int(latest_ts),
        value=_zscore_latest(skew_series),
        meta={"source": "options_surface_agg", "sample_n": int(agg["sample_n"])},
    )
    put_factor_feature(
        con,
        feature_id="options.term_structure_slope_z",
        asof_ts=int(latest_ts),
        effective_ts=int(latest_ts),
        value=_zscore_latest(slope_series),
        meta={"source": "options_surface_agg", "sample_n": int(agg["sample_n"])},
    )
    put_factor_feature(
        con,
        feature_id="options.vol_of_vol_z",
        asof_ts=int(latest_ts),
        effective_ts=int(latest_ts),
        value=_zscore_latest(vov_series),
        meta={"source": "options_surface_agg", "sample_n": int(agg["sample_n"])},
    )

    put_factor_feature(
        con,
        feature_id="options.skew_25d_z",
        asof_ts=int(latest_ts),
        effective_ts=int(latest_ts),
        value=_zscore_latest(skew_series),
        meta={"source": "options_surface_agg", "sample_n": int(agg["sample_n"])},
    )
    put_factor_feature(
        con,
        feature_id="options.skew_25d_d5",
        asof_ts=int(latest_ts),
        effective_ts=int(latest_ts),
        value=_delta_latest(skew_series, _SURFACE_D5_LAG),
        meta={"source": "options_surface_agg", "sample_n": int(agg["sample_n"])},
    )

    return {
        "updated": int(len(updated_rows)),
        "features": 5,
        "ts_ms": int(latest_ts),
    }
