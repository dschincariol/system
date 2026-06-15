"""
Turns raw options-chain snapshots into symbol-level features and options events.

README:
- Source: existing Polygon/Tradier option-chain snapshots persisted in
  ``options_chain_v2`` / ``options_chain`` by ``ingest_options`` and
  ``options_poll``.
- Cadence: computed whenever the registered options ingestion jobs refresh
  chain snapshots; ``ingest_options`` is scheduled in the runtime registry.
- Availability lag: feature rows use the snapshot timestamp as availability,
  and consumers must join on ``snapshot_ts_ms <= ts_ms``.
- Caveats: ``opt_flow_imbalance`` is a snapshot proxy, not trade-level signed
  order flow. Dealer GEX is a naive volatility-regime conditioning input under
  the convention dealers are long calls and short puts; it is not a directional
  alpha signal.
"""

from __future__ import annotations

import json
import logging
import math
import os
import statistics
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine.data.time_utils import utc_ms_from_datetime
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import put_normalized_event, put_options_event_feature, run_write_txn

_SNAPSHOT_STALE_MS = int(os.environ.get("OPTIONS_FEATURE_STALE_MS", str(15 * 60 * 1000)))
_INTRADAY_BUCKET_SEC = max(60, int(os.environ.get("OPTIONS_INTRADAY_BUCKET_SEC", "900")))
_DAILY_BUCKET_SEC = 86400
_IVR_LONG_OBS = max(20, int(os.environ.get("OPTIONS_IVR_LONG_OBS", "252")))
_IVR_SHORT_OBS = max(10, int(os.environ.get("OPTIONS_IVR_SHORT_OBS", "63")))
_ZSCORE_OBS = max(20, int(os.environ.get("OPTIONS_ZSCORE_OBS", "63")))
_UNUSUAL_LOOKBACK_MS = int(os.environ.get("OPTIONS_UNUSUAL_LOOKBACK_MS", str(20 * 24 * 3600 * 1000)))
_UNUSUAL_MEDIAN_POINTS = max(5, int(os.environ.get("OPTIONS_UNUSUAL_MEDIAN_POINTS", "20")))
_UNUSUAL_RATIO_TRIGGER = float(os.environ.get("OPTIONS_UNUSUAL_RATIO_TRIGGER", "3.0"))
_UNUSUAL_VOL_OI_TRIGGER = float(os.environ.get("OPTIONS_UNUSUAL_VOL_OI_TRIGGER", "1.0"))
_EVENT_IVR_HIGH = float(os.environ.get("OPTIONS_EVENT_IVR_HIGH", "0.80"))
_EVENT_IVR_LOW = float(os.environ.get("OPTIONS_EVENT_IVR_LOW", "0.20"))
_EVENT_ZSCORE = float(os.environ.get("OPTIONS_EVENT_ZSCORE", "1.50"))
_EVENT_UNUSUAL_SCORE = float(os.environ.get("OPTIONS_EVENT_UNUSUAL_SCORE", "2.0"))
_GEX_RISK_FREE_RATE = float(os.environ.get("OPTIONS_GEX_RISK_FREE_RATE", "0.045"))
_GEX_ZSCORE_OBS = max(2, min(5, int(os.environ.get("OPTIONS_GEX_ZSCORE_OBS", "5"))))
_FLOW_ZSCORE_OBS = max(2, min(5, int(os.environ.get("OPTIONS_FLOW_ZSCORE_OBS", "5"))))
_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("data.options_features")

OPTIONS_GEX_FLOW_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("gex_raw", "DOUBLE PRECISION"),
    ("gex_norm", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("gex_norm_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("gex_sign", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("opt_flow_imbalance", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("opt_flow_imbalance_z", "DOUBLE PRECISION NOT NULL DEFAULT 0.0"),
    ("gex_zero_gamma_flip", "DOUBLE PRECISION"),
)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_options_features_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.options_features",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal("OPTIONS_FEATURES_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(value)[:120])
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_pos(value: Any) -> Optional[float]:
    out = _safe_float(value, float("nan"))
    if not math.isfinite(out) or out <= 0.0:
        return None
    return float(out)


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _bucket_start(ts_ms: int, bucket_sec: int) -> int:
    size_ms = max(1, int(bucket_sec)) * 1000
    return int(ts_ms // size_ms) * size_ms


def _days_to_expiration(expiration: str, ts_ms: int) -> Optional[float]:
    try:
        dt = datetime.strptime(str(expiration), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception as e:
        _warn_nonfatal("OPTIONS_FEATURES_EXPIRY_PARSE_FAILED", e, once_key="days_to_expiration", expiration=str(expiration), ts_ms=int(ts_ms))
        return None
    out = (float(utc_ms_from_datetime(dt, field_name="options_expiration")) - float(ts_ms)) / 86400000.0
    if not math.isfinite(out):
        return None
    return float(out)


def _norm_pdf(x: float) -> float:
    return float(math.exp(-0.5 * float(x) * float(x)) / math.sqrt(2.0 * math.pi))


def _norm_cdf(x: float) -> float:
    return float(0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0))))


def _bs_d1(spot: float, strike: float, sigma: float, years: float, rate: float = _GEX_RISK_FREE_RATE) -> Optional[float]:
    s = _safe_pos(spot)
    k = _safe_pos(strike)
    vol = _safe_pos(sigma)
    t = _safe_pos(years)
    if s is None or k is None or vol is None or t is None:
        return None
    denom = float(vol) * math.sqrt(float(t))
    if denom <= 1e-12:
        return None
    return float((math.log(float(s) / float(k)) + (float(rate) + 0.5 * float(vol) * float(vol)) * float(t)) / denom)


def black_scholes_gamma(
    *,
    spot: float,
    strike: float,
    iv: float,
    expiration: str,
    ts_ms: int,
    risk_free_rate: float = _GEX_RISK_FREE_RATE,
) -> float:
    """Return Black-Scholes unit gamma for a contract as of the snapshot time."""

    days = _days_to_expiration(str(expiration), int(ts_ms))
    if days is None or days <= 0.0:
        return 0.0
    years = max(float(days) / 365.25, 1.0 / 365.25)
    d1 = _bs_d1(float(spot), float(strike), float(iv), float(years), float(risk_free_rate))
    if d1 is None:
        return 0.0
    denom = float(spot) * float(iv) * math.sqrt(float(years))
    if denom <= 1e-12:
        return 0.0
    return float(_norm_pdf(float(d1)) / denom)


def _black_scholes_delta_abs(row: Dict[str, Any], *, spot: float, ts_ms: int) -> float:
    raw_delta = row.get("delta")
    if raw_delta is not None:
        delta = abs(_safe_float(raw_delta, 0.0))
        if math.isfinite(delta) and delta > 0.0:
            return float(max(0.0, min(1.0, delta)))
    days = _days_to_expiration(str(row.get("expiration") or ""), int(ts_ms))
    if days is None or days <= 0.0:
        return 0.0
    years = max(float(days) / 365.25, 1.0 / 365.25)
    d1 = _bs_d1(
        float(spot),
        _safe_float(row.get("strike"), 0.0),
        _safe_float(row.get("iv"), 0.0),
        float(years),
        float(_GEX_RISK_FREE_RATE),
    )
    if d1 is None:
        return 0.0
    ctype = _normalize_contract_type(row.get("contract_type"))
    if ctype == "put":
        return float(abs(_norm_cdf(float(d1)) - 1.0))
    if ctype == "call":
        return float(abs(_norm_cdf(float(d1))))
    return 0.0


def _normalize_contract_type(value: Any) -> str:
    text = _safe_str(value).lower()
    if text in {"call", "c"}:
        return "call"
    if text in {"put", "p"}:
        return "put"
    return ""


def _contract_gamma(row: Dict[str, Any], *, spot: float, ts_ms: int) -> float:
    raw_gamma = row.get("gamma")
    if raw_gamma is not None:
        gamma = _safe_float(raw_gamma, 0.0)
        if gamma > 0.0:
            return float(gamma)
    return black_scholes_gamma(
        spot=float(spot),
        strike=_safe_float(row.get("strike"), 0.0),
        iv=_safe_float(row.get("iv"), 0.0),
        expiration=str(row.get("expiration") or ""),
        ts_ms=int(ts_ms),
    )


def _short_zscore(current: float, history: List[float]) -> float:
    vals = [float(v) for v in history if math.isfinite(float(v))]
    if len(vals) < 2:
        return 0.0
    mu = sum(vals) / float(len(vals))
    var = sum((x - mu) ** 2 for x in vals) / float(max(1, len(vals) - 1))
    if var <= 1e-12:
        return 0.0
    return float(max(-10.0, min(10.0, (float(current) - mu) / math.sqrt(var))))


def compute_dealer_gex_metrics(
    rows: List[Dict[str, Any]],
    *,
    spot: float,
    adv_dollars: float,
    ts_ms: int,
) -> Dict[str, float]:
    """Compute naive dealer GEX for volatility-regime conditioning, not direction."""

    spot_num = _safe_float(spot, 0.0)
    if spot_num <= 0.0:
        return {
            "gex_raw": 0.0,
            "gex_norm": 0.0,
            "gex_sign": 0.0,
            "gex_call_raw": 0.0,
            "gex_put_raw": 0.0,
            "gex_zero_gamma_flip": float("nan"),
        }
    raw = 0.0
    call_raw = 0.0
    put_raw = 0.0
    for row in rows or []:
        oi = _safe_float(row.get("open_interest"), 0.0)
        if oi <= 0.0:
            continue
        gamma = _contract_gamma(row, spot=float(spot_num), ts_ms=int(ts_ms))
        if gamma <= 0.0:
            continue
        exposure = float(gamma) * float(oi) * 100.0 * float(spot_num)
        ctype = _normalize_contract_type(row.get("contract_type"))
        if ctype == "call":
            call_raw += float(exposure)
            raw += float(exposure)
        elif ctype == "put":
            put_raw += float(exposure)
            raw -= float(exposure)
    denom = max(1.0, _safe_float(adv_dollars, 1.0))
    gex_norm = float(raw / denom)
    gex_sign = 1.0 if raw > 0.0 else (-1.0 if raw < 0.0 else 0.0)
    return {
        "gex_raw": float(raw),
        "gex_norm": float(gex_norm),
        "gex_sign": float(gex_sign),
        "gex_call_raw": float(call_raw),
        "gex_put_raw": float(put_raw),
        "gex_zero_gamma_flip": float("nan"),
    }


def compute_flow_imbalance_proxy(
    rows: List[Dict[str, Any]],
    previous_by_contract: Dict[str, Dict[str, Any]],
    *,
    spot: float,
    ts_ms: int,
) -> Dict[str, float]:
    """Compute a snapshot proxy for signed options flow from volume and OI deltas."""

    call_flow = 0.0
    put_flow = 0.0
    oi_delta_total = 0.0
    volume_total = 0.0
    for row in rows or []:
        ctype = _normalize_contract_type(row.get("contract_type"))
        if ctype not in {"call", "put"}:
            continue
        key = str(row.get("contract_key") or "").strip()
        prev = dict(previous_by_contract.get(key) or {})
        volume = max(0.0, _safe_float(row.get("volume"), 0.0))
        current_oi = max(0.0, _safe_float(row.get("open_interest"), 0.0))
        previous_oi = max(0.0, _safe_float(prev.get("open_interest"), 0.0))
        oi_delta = max(0.0, float(current_oi) - float(previous_oi))
        delta_abs = _black_scholes_delta_abs(row, spot=float(spot), ts_ms=int(ts_ms))
        activity = float(delta_abs) * (float(volume) + float(oi_delta))
        volume_total += float(volume)
        oi_delta_total += float(oi_delta)
        if ctype == "call":
            call_flow += float(activity)
        else:
            put_flow += float(activity)
    denom = float(call_flow) + float(put_flow)
    imbalance = float((call_flow - put_flow) / denom) if denom > 0.0 else 0.0
    return {
        "opt_flow_imbalance": float(max(-1.0, min(1.0, imbalance))),
        "call_delta_activity": float(call_flow),
        "put_delta_activity": float(put_flow),
        "oi_delta_total": float(oi_delta_total),
        "volume_total": float(volume_total),
    }


def _load_latest_snapshot_rows(con, symbol: str) -> Tuple[List[Dict[str, Any]], Optional[int], str]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return [], None, ""

    row = con.execute(
        """
        SELECT MAX(ts_ms)
        FROM options_chain_v2
        WHERE underlying=?
        """,
        (sym,),
    ).fetchone()
    ts_v2 = int(row[0]) if row and row[0] is not None else None
    if ts_v2 is not None:
        rows = con.execute(
            """
            SELECT ts_ms, contract, expiration, contract_type, strike, iv, open_interest, volume, delta, gamma
            FROM options_chain_v2
            WHERE underlying=?
              AND ts_ms >= ?
              AND ts_ms <= ?
            ORDER BY contract ASC, ts_ms DESC
            """,
            (sym, int(ts_v2) - _SNAPSHOT_STALE_MS, int(ts_v2)),
        ).fetchall()
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for ts_ms, contract, expiration, contract_type, strike, iv, open_interest, volume, delta, gamma in rows or []:
            key = _safe_str(contract)
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(
                {
                    "ts_ms": int(ts_ms),
                    "contract_key": key,
                    "expiration": _safe_str(expiration),
                    "contract_type": _normalize_contract_type(contract_type),
                    "strike": _safe_float(strike, 0.0),
                    "iv": _safe_pos(iv),
                    "open_interest": _safe_pos(open_interest),
                    "volume": _safe_pos(volume),
                    "delta": (_safe_float(delta, float("nan")) if delta is not None else None),
                    "gamma": (_safe_float(gamma, float("nan")) if gamma is not None else None),
                }
            )
        return deduped, int(ts_v2), "polygon"

    row = con.execute(
        """
        SELECT MAX(ts_ms)
        FROM options_chain
        WHERE symbol=?
        """,
        (sym,),
    ).fetchone()
    ts_v1 = int(row[0]) if row and row[0] is not None else None
    if ts_v1 is None:
        return [], None, ""

    rows = con.execute(
        """
        SELECT ts_ms, expiry, call_put, strike, iv, open_interest, volume
        FROM options_chain
        WHERE symbol=?
          AND ts_ms >= ?
          AND ts_ms <= ?
        ORDER BY expiry ASC, strike ASC, call_put ASC, ts_ms DESC
        """,
        (sym, int(ts_v1) - _SNAPSHOT_STALE_MS, int(ts_v1)),
    ).fetchall()
    deduped = []
    seen = set()
    for ts_ms, expiry, call_put, strike, iv, open_interest, volume in rows or []:
        contract_type = _normalize_contract_type(call_put)
        key = f"{_safe_str(expiry)}:{_safe_float(strike, 0.0):.4f}:{contract_type}"
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(
            {
                "ts_ms": int(ts_ms),
                "contract_key": key,
                "expiration": _safe_str(expiry),
                "contract_type": contract_type,
                "strike": _safe_float(strike, 0.0),
                "iv": _safe_pos(iv),
                "open_interest": _safe_pos(open_interest),
                "volume": _safe_pos(volume),
                "delta": None,
                "gamma": None,
            }
        )
    return deduped, int(ts_v1), "legacy"


def _load_surface_row(con, symbol: str, snapshot_ts_ms: int) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT ts_ms, atm_iv_near, atm_iv_next, skew_25d, term_structure_slope
        FROM options_surface
        WHERE underlying=?
          AND ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (str(symbol), int(snapshot_ts_ms)),
    ).fetchone()
    if not row:
        return {}
    return {
        "ts_ms": int(row[0] or 0),
        "atm_iv_near": _safe_float(row[1], 0.0),
        "atm_iv_next": _safe_float(row[2], 0.0),
        "skew_25d": _safe_float(row[3], 0.0),
        "term_structure_slope": _safe_float(row[4], 0.0),
    }


def _collapse_latest_per_day(rows: Iterable[Tuple[int, Any]], limit: int) -> List[float]:
    latest_by_day: Dict[int, float] = {}
    for ts_ms, value in rows or []:
        v = _safe_pos(value)
        if v is None:
            continue
        day_key = int(int(ts_ms) // 86400000)
        latest_by_day[day_key] = float(v)
    keys = sorted(latest_by_day.keys())
    if limit > 0:
        keys = keys[-int(limit):]
    return [float(latest_by_day[k]) for k in keys]


def _load_daily_series_from_symbol_features(con, symbol: str, column: str, limit: int) -> List[float]:
    try:
        rows = con.execute(
            f"""
            SELECT bucket_ts_ms, {column}
            FROM options_symbol_features
            WHERE symbol=?
              AND bucket_sec=?
              AND {column} IS NOT NULL
            ORDER BY bucket_ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(_DAILY_BUCKET_SEC), int(limit)),
        ).fetchall()
    except Exception:
        rows = []
    vals = [float(r[1]) for r in reversed(rows or []) if r and r[1] is not None]
    return vals


def _load_surface_history_series(con, symbol: str, column: str, limit: int) -> List[float]:
    rows = con.execute(
        f"""
        SELECT ts_ms, {column}
        FROM options_surface
        WHERE underlying=?
          AND {column} IS NOT NULL
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (str(symbol), int(limit) * 8),
    ).fetchall()
    return _collapse_latest_per_day(reversed(rows or []), limit)


def _history_series(con, symbol: str, column: str, limit: int) -> List[float]:
    vals = _load_daily_series_from_symbol_features(con, symbol, column, limit)
    if len(vals) >= max(8, min(limit, 20)):
        return vals
    return _load_surface_history_series(con, symbol, column, limit)


def _ensure_options_gex_flow_columns(con) -> None:
    for table_name in ("options_symbol_features", "options_event_features"):
        for column_name, column_type in OPTIONS_GEX_FLOW_COLUMNS:
            try:
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")
            except Exception as e:
                _warn_nonfatal(
                    "OPTIONS_FEATURES_GEX_FLOW_COLUMN_ADD_FAILED",
                    e,
                    once_key=f"gex_flow_column:{table_name}:{column_name}",
                    table=str(table_name),
                    column=str(column_name),
                )
                try:
                    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
                except Exception as fallback_error:
                    _warn_nonfatal(
                        "OPTIONS_FEATURES_GEX_FLOW_COLUMN_ADD_FALLBACK_FAILED",
                        fallback_error,
                        once_key=f"gex_flow_column_fallback:{table_name}:{column_name}",
                        table=str(table_name),
                        column=str(column_name),
                    )
                    continue


def _load_spot_price(con, symbol: str, ts_ms: int) -> Optional[float]:
    for table_name, expr in (
        ("price_quotes", "last"),
        ("prices", "COALESCE(px, price)"),
    ):
        try:
            row = con.execute(
                f"""
                SELECT {expr}
                FROM {table_name}
                WHERE symbol=?
                  AND ts_ms <= ?
                  AND {expr} IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (str(symbol), int(ts_ms)),
            ).fetchone()
        except Exception:
            row = None
        if row and row[0] is not None:
            value = _safe_pos(row[0])
            if value is not None:
                return float(value)
    return None


def _adv_dollars(con, symbol: str, ts_ms: int) -> Tuple[float, bool]:
    window_start = int(ts_ms) - int(30 * 24 * 3600 * 1000)
    rows = []
    try:
        rows = con.execute(
            """
            SELECT last, volume
            FROM price_quotes
            WHERE symbol=?
              AND ts_ms <= ?
              AND ts_ms >= ?
              AND last IS NOT NULL
              AND volume IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 390
            """,
            (str(symbol), int(ts_ms), int(window_start)),
        ).fetchall()
    except Exception:
        rows = []
    values = [
        float(_safe_float(row[0], 0.0) * _safe_float(row[1], 0.0))
        for row in rows or []
        if _safe_float(row[0], 0.0) > 0.0 and _safe_float(row[1], 0.0) > 0.0
    ]
    if values:
        return float(max(1.0, sum(values) / max(1, len(values)))), False
    return 1.0, True


def _load_previous_snapshot_rows(con, symbol: str, source: str, snapshot_ts_ms: int) -> Dict[str, Dict[str, Any]]:
    cutoff = int(snapshot_ts_ms) - int(5 * 24 * 3600 * 1000)
    if str(source) == "polygon":
        try:
            prev_ts_row = con.execute(
                """
                SELECT MAX(ts_ms)
                FROM options_chain_v2
                WHERE underlying=?
                  AND ts_ms < ?
                  AND ts_ms >= ?
                """,
                (str(symbol), int(snapshot_ts_ms), int(cutoff)),
            ).fetchone()
        except Exception:
            prev_ts_row = None
        prev_ts = int(prev_ts_row[0]) if prev_ts_row and prev_ts_row[0] is not None else None
        if prev_ts is None:
            return {}
        rows = con.execute(
            """
            SELECT contract, open_interest, volume, delta
            FROM options_chain_v2
            WHERE underlying=?
              AND ts_ms=?
            """,
            (str(symbol), int(prev_ts)),
        ).fetchall()
        return {
            _safe_str(contract): {
                "open_interest": _safe_float(open_interest, 0.0),
                "volume": _safe_float(volume, 0.0),
                "delta": (_safe_float(delta, 0.0) if delta is not None else None),
            }
            for contract, open_interest, volume, delta in rows or []
            if _safe_str(contract)
        }

    try:
        prev_ts_row = con.execute(
            """
            SELECT MAX(ts_ms)
            FROM options_chain
            WHERE symbol=?
              AND ts_ms < ?
              AND ts_ms >= ?
            """,
            (str(symbol), int(snapshot_ts_ms), int(cutoff)),
        ).fetchone()
    except Exception:
        prev_ts_row = None
    prev_ts = int(prev_ts_row[0]) if prev_ts_row and prev_ts_row[0] is not None else None
    if prev_ts is None:
        return {}
    rows = con.execute(
        """
        SELECT expiry, strike, call_put, open_interest, volume
        FROM options_chain
        WHERE symbol=?
          AND ts_ms=?
        """,
        (str(symbol), int(prev_ts)),
    ).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for expiry, strike, call_put, open_interest, volume in rows or []:
        key = f"{_safe_str(expiry)}:{_safe_float(strike, 0.0):.4f}:{_normalize_contract_type(call_put)}"
        if not key:
            continue
        out[key] = {
            "open_interest": _safe_float(open_interest, 0.0),
            "volume": _safe_float(volume, 0.0),
            "delta": None,
        }
    return out


def _iv_rank(current: float, history: List[float]) -> float:
    xs = [float(v) for v in history if math.isfinite(float(v))]
    if not xs:
        return 0.0
    lo = min(xs)
    hi = max(xs)
    if hi - lo <= 1e-12:
        return 0.0
    return float(max(0.0, min(1.0, (float(current) - lo) / (hi - lo))))


def _zscore(current: float, history: List[float]) -> float:
    xs = [float(v) for v in history if math.isfinite(float(v))]
    if len(xs) < 20:
        return 0.0
    mu = sum(xs) / float(len(xs))
    var = sum((x - mu) ** 2 for x in xs) / float(len(xs))
    if var <= 1e-12:
        return 0.0
    return float((float(current) - mu) / math.sqrt(var))


def _load_contract_volume_history(con, symbol: str, source: str, snapshot_ts_ms: int) -> Dict[str, List[float]]:
    cutoff = int(snapshot_ts_ms) - _UNUSUAL_LOOKBACK_MS
    out: Dict[str, List[float]] = {}
    if str(source) == "polygon":
        rows = con.execute(
            """
            SELECT contract, volume
            FROM options_chain_v2
            WHERE underlying=?
              AND ts_ms >= ?
              AND ts_ms < ?
              AND volume IS NOT NULL
            ORDER BY ts_ms ASC
            """,
            (str(symbol), int(cutoff), int(snapshot_ts_ms) - _SNAPSHOT_STALE_MS),
        ).fetchall()
        for contract, volume in rows or []:
            vol = _safe_pos(volume)
            key = _safe_str(contract)
            if vol is None or not key:
                continue
            out.setdefault(key, []).append(float(vol))
        return out

    rows = con.execute(
        """
        SELECT expiry, strike, call_put, volume
        FROM options_chain
        WHERE symbol=?
          AND ts_ms >= ?
          AND ts_ms < ?
          AND volume IS NOT NULL
        ORDER BY ts_ms ASC
        """,
        (str(symbol), int(cutoff), int(snapshot_ts_ms) - _SNAPSHOT_STALE_MS),
    ).fetchall()
    for expiry, strike, call_put, volume in rows or []:
        vol = _safe_pos(volume)
        key = f"{_safe_str(expiry)}:{_safe_float(strike, 0.0):.4f}:{_normalize_contract_type(call_put)}"
        if vol is None:
            continue
        out.setdefault(key, []).append(float(vol))
    return out


def _compute_unusual_volume(rows: List[Dict[str, Any]], history: Dict[str, List[float]]) -> Dict[str, Any]:
    unusual_contracts = 0
    unusual_ratio = 0.0
    unusual_volume = 0.0
    total_volume = 0.0

    for row in rows or []:
        volume = _safe_pos(row.get("volume"))
        if volume is None:
            continue
        total_volume += float(volume)
        oi = _safe_pos(row.get("open_interest")) or 0.0
        hist = list(history.get(str(row.get("contract_key") or "")) or [])
        hist = hist[-int(_UNUSUAL_MEDIAN_POINTS):]
        ratio_hist = 0.0
        if hist:
            try:
                hist_med = statistics.median(hist)
            except Exception:
                hist_med = 0.0
            if hist_med > 0.0:
                ratio_hist = float(volume) / float(hist_med)
        ratio_oi = float(volume) / max(1.0, float(oi))
        score = max(float(ratio_hist), float(ratio_oi))
        if score >= _UNUSUAL_RATIO_TRIGGER or ratio_oi >= _UNUSUAL_VOL_OI_TRIGGER:
            unusual_contracts += 1
            unusual_volume += float(volume)
        unusual_ratio = max(unusual_ratio, float(score))

    unusual_share = float(unusual_volume / total_volume) if total_volume > 0.0 else 0.0
    unusual_score = max(float(unusual_ratio), 4.0 * float(unusual_share))
    return {
        "unusual_volume_score": float(unusual_score),
        "unusual_volume_contracts": int(unusual_contracts),
        "unusual_volume_ratio": float(unusual_ratio),
        "unusual_volume_share": float(unusual_share),
        "total_volume": float(total_volume),
    }


def _flow_ratios(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    call_volume = 0.0
    put_volume = 0.0
    call_oi = 0.0
    put_oi = 0.0
    expiries = set()
    for row in rows or []:
        ctype = str(row.get("contract_type") or "")
        exp = _safe_str(row.get("expiration"))
        if exp:
            expiries.add(exp)
        if ctype == "call":
            call_volume += _safe_pos(row.get("volume")) or 0.0
            call_oi += _safe_pos(row.get("open_interest")) or 0.0
        elif ctype == "put":
            put_volume += _safe_pos(row.get("volume")) or 0.0
            put_oi += _safe_pos(row.get("open_interest")) or 0.0

    return {
        "call_put_volume_ratio": float((call_volume + 1.0) / (put_volume + 1.0)),
        "call_put_oi_ratio": float((call_oi + 1.0) / (put_oi + 1.0)),
        "expiry_count": int(len(expiries)),
    }


def _signal_score(row: Dict[str, Any]) -> float:
    volume_ratio = max(1e-6, float(row.get("call_put_volume_ratio") or 1.0))
    flow_signal = math.tanh(math.log(volume_ratio))
    skew_signal = -math.tanh(8.0 * float(row.get("skew_25d") or 0.0))
    term_signal = math.tanh(12.0 * float(row.get("term_structure_slope") or 0.0))
    volume_intensity = min(1.0, math.log1p(max(0.0, float(row.get("unusual_volume_score") or 0.0))) / math.log(4.0))
    return float((0.45 * flow_signal + 0.35 * skew_signal + 0.20 * term_signal) * max(0.25, volume_intensity))


def _upsert_symbol_feature(con, row: Dict[str, Any], bucket_sec: int) -> None:
    bucket_ts_ms = _bucket_start(int(row["snapshot_ts_ms"]), int(bucket_sec))
    meta_json = json.dumps(row.get("meta_json") or {}, separators=(",", ":"), sort_keys=True)
    con.execute(
        """
        INSERT OR REPLACE INTO options_symbol_features(
          symbol, bucket_ts_ms, bucket_sec, snapshot_ts_ms, chain_source,
          contract_count, expiry_count, atm_iv_near, atm_iv_next, iv_rank,
          iv_rank_short, skew_25d, skew_zscore, term_structure_slope,
          term_structure_zscore, call_put_volume_ratio, call_put_oi_ratio,
          unusual_volume_score, unusual_volume_contracts, unusual_volume_ratio,
          signal_score, gex_raw, gex_norm, gex_norm_z, gex_sign,
          opt_flow_imbalance, opt_flow_imbalance_z, gex_zero_gamma_flip,
          meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(row.get("symbol") or "").upper().strip(),
            int(bucket_ts_ms),
            int(bucket_sec),
            int(row.get("snapshot_ts_ms") or int(time.time() * 1000)),
            str(row.get("chain_source") or ""),
            int(row.get("contract_count") or 0),
            int(row.get("expiry_count") or 0),
            float(row.get("atm_iv_near") or 0.0),
            float(row.get("atm_iv_next") or 0.0),
            float(row.get("iv_rank") or 0.0),
            float(row.get("iv_rank_short") or 0.0),
            float(row.get("skew_25d") or 0.0),
            float(row.get("skew_zscore") or 0.0),
            float(row.get("term_structure_slope") or 0.0),
            float(row.get("term_structure_zscore") or 0.0),
            float(row.get("call_put_volume_ratio") or 1.0),
            float(row.get("call_put_oi_ratio") or 1.0),
            float(row.get("unusual_volume_score") or 0.0),
            int(row.get("unusual_volume_contracts") or 0),
            float(row.get("unusual_volume_ratio") or 0.0),
            float(row.get("signal_score") or 0.0),
            float(row.get("gex_raw") or 0.0),
            float(row.get("gex_norm") or 0.0),
            float(row.get("gex_norm_z") or 0.0),
            float(row.get("gex_sign") or 0.0),
            float(row.get("opt_flow_imbalance") or 0.0),
            float(row.get("opt_flow_imbalance_z") or 0.0),
            (float(row.get("gex_zero_gamma_flip")) if row.get("gex_zero_gamma_flip") is not None and math.isfinite(float(row.get("gex_zero_gamma_flip"))) else None),
            meta_json,
        ),
    )


def _build_feature_row(con, symbol: str) -> Optional[Dict[str, Any]]:
    rows, snapshot_ts_ms, chain_source = _load_latest_snapshot_rows(con, symbol)
    if not rows or snapshot_ts_ms is None:
        return None

    surface = _load_surface_row(con, symbol, int(snapshot_ts_ms))
    flow = _flow_ratios(rows)
    unusual = _compute_unusual_volume(
        rows,
        _load_contract_volume_history(con, symbol, chain_source, int(snapshot_ts_ms)),
    )
    spot = _load_spot_price(con, symbol, int(snapshot_ts_ms))
    if spot is None:
        strikes = [_safe_pos(row.get("strike")) for row in rows or []]
        valid_strikes = [float(value) for value in strikes if value is not None]
        spot = float(statistics.median(valid_strikes)) if valid_strikes else 0.0
    adv_dollars, adv_missing = _adv_dollars(con, symbol, int(snapshot_ts_ms))
    gex = compute_dealer_gex_metrics(
        rows,
        spot=float(spot or 0.0),
        adv_dollars=float(adv_dollars),
        ts_ms=int(snapshot_ts_ms),
    )
    flow_proxy = compute_flow_imbalance_proxy(
        rows,
        _load_previous_snapshot_rows(con, symbol, chain_source, int(snapshot_ts_ms)),
        spot=float(spot or 0.0),
        ts_ms=int(snapshot_ts_ms),
    )

    atm_iv_near = float(surface.get("atm_iv_near") or 0.0)
    skew_25d = float(surface.get("skew_25d") or 0.0)
    term_structure_slope = float(surface.get("term_structure_slope") or 0.0)

    iv_history = _history_series(con, symbol, "atm_iv_near", _IVR_LONG_OBS)
    iv_history_short = iv_history[-int(_IVR_SHORT_OBS):]
    skew_history = _history_series(con, symbol, "skew_25d", _ZSCORE_OBS)
    term_history = _history_series(con, symbol, "term_structure_slope", _ZSCORE_OBS)
    gex_history = _load_daily_series_from_symbol_features(con, symbol, "gex_norm", _GEX_ZSCORE_OBS)
    flow_history = _load_daily_series_from_symbol_features(con, symbol, "opt_flow_imbalance", _FLOW_ZSCORE_OBS)

    row: Dict[str, Any] = {
        "symbol": str(symbol).upper().strip(),
        "snapshot_ts_ms": int(snapshot_ts_ms),
        "chain_source": str(chain_source),
        "contract_count": int(len(rows)),
        "expiry_count": int(flow["expiry_count"]),
        "atm_iv_near": float(atm_iv_near),
        "atm_iv_next": float(surface.get("atm_iv_next") or 0.0),
        "iv_rank": _iv_rank(atm_iv_near, iv_history),
        "iv_rank_short": _iv_rank(atm_iv_near, iv_history_short),
        "skew_25d": float(skew_25d),
        "skew_zscore": _zscore(skew_25d, skew_history),
        "term_structure_slope": float(term_structure_slope),
        "term_structure_zscore": _zscore(term_structure_slope, term_history),
        "call_put_volume_ratio": float(flow["call_put_volume_ratio"]),
        "call_put_oi_ratio": float(flow["call_put_oi_ratio"]),
        "gex_raw": float(gex["gex_raw"]),
        "gex_norm": float(gex["gex_norm"]),
        "gex_norm_z": _short_zscore(float(gex["gex_norm"]), gex_history),
        "gex_sign": float(gex["gex_sign"]),
        "opt_flow_imbalance": float(flow_proxy["opt_flow_imbalance"]),
        "opt_flow_imbalance_z": _short_zscore(float(flow_proxy["opt_flow_imbalance"]), flow_history),
        "gex_zero_gamma_flip": None,
    }
    row.update(unusual)
    row["signal_score"] = _signal_score(row)
    row["meta_json"] = {
        "source": str(chain_source),
        "iv_history_obs": int(len(iv_history)),
        "skew_history_obs": int(len(skew_history)),
        "term_history_obs": int(len(term_history)),
        "spot": float(spot or 0.0),
        "adv_dollars": float(adv_dollars),
        "adv_missing": bool(adv_missing),
        "gex_call_raw": float(gex["gex_call_raw"]),
        "gex_put_raw": float(gex["gex_put_raw"]),
        "gex_zero_gamma_flip": None,
        "gex_caveat": "naive dealer convention: long calls, short puts; volatility-regime input, not direction",
        "flow_caveat": "snapshot proxy using volume plus positive OI delta; not trade-level signed flow",
        "flow_call_delta_activity": float(flow_proxy["call_delta_activity"]),
        "flow_put_delta_activity": float(flow_proxy["put_delta_activity"]),
        "flow_oi_delta_total": float(flow_proxy["oi_delta_total"]),
        "flow_volume_total": float(flow_proxy["volume_total"]),
    }
    return row


def _event_importance_floor(kind: str, row: Dict[str, Any]) -> float:
    if kind == "unusual_options_volume":
        return 0.74
    if kind == "options_iv_rank_extreme":
        return 0.66
    if kind == "options_skew_shift":
        return 0.64
    if kind == "options_term_structure_shift":
        return 0.62
    return 0.58


def _event_specs(row: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    symbol = str(row.get("symbol") or "")
    specs: List[Tuple[str, str, str]] = []
    iv_rank = float(row.get("iv_rank") or 0.0)
    if iv_rank >= _EVENT_IVR_HIGH:
        specs.append(
            (
                "options_iv_rank_extreme",
                f"{symbol} options IV elevated",
                f"IV rank {iv_rank:.2f} with ATM IV {float(row.get('atm_iv_near') or 0.0):.3f}.",
            )
        )
    elif iv_rank <= _EVENT_IVR_LOW and float(row.get("atm_iv_near") or 0.0) > 0.0:
        specs.append(
            (
                "options_iv_rank_extreme",
                f"{symbol} options IV compressed",
                f"IV rank {iv_rank:.2f} with ATM IV {float(row.get('atm_iv_near') or 0.0):.3f}.",
            )
        )
    if abs(float(row.get("skew_zscore") or 0.0)) >= _EVENT_ZSCORE:
        specs.append(
            (
                "options_skew_shift",
                f"{symbol} options skew shift",
                f"25d skew {float(row.get('skew_25d') or 0.0):+.3f} ({float(row.get('skew_zscore') or 0.0):+.2f}z).",
            )
        )
    if abs(float(row.get("term_structure_zscore") or 0.0)) >= _EVENT_ZSCORE:
        specs.append(
            (
                "options_term_structure_shift",
                f"{symbol} term structure shift",
                f"Term slope {float(row.get('term_structure_slope') or 0.0):+.4f} ({float(row.get('term_structure_zscore') or 0.0):+.2f}z).",
            )
        )
    if float(row.get("unusual_volume_score") or 0.0) >= _EVENT_UNUSUAL_SCORE:
        specs.append(
            (
                "unusual_options_volume",
                f"{symbol} unusual options volume",
                f"Score {float(row.get('unusual_volume_score') or 0.0):.2f} across {int(row.get('unusual_volume_contracts') or 0)} contracts.",
            )
        )
    return specs


def _emit_event(kind: str, title: str, body: str, row: Dict[str, Any], bucket_sec: int, con=None) -> int:
    snapshot_ts_ms = int(row.get("snapshot_ts_ms") or int(time.time() * 1000))
    payload = {
        "ts_ms": int(snapshot_ts_ms),
        "timestamp": int(snapshot_ts_ms),
        "event_type": "options",
        "symbol": str(row.get("symbol") or "").upper().strip(),
        "source": "options_features",
        "title": str(title),
        "body": str(body),
        "source_id": f"{row.get('symbol')}:{kind}:{snapshot_ts_ms}",
        "event_key": f"options:{kind}:{row.get('symbol')}:{snapshot_ts_ms}",
        "raw_payload": {
            "symbol": row.get("symbol"),
            "event_kind": kind,
            "snapshot_ts_ms": snapshot_ts_ms,
            "bucket_sec": int(bucket_sec),
        },
        "derived_features": {
            "options_event_kind": kind,
            "bucket_sec": int(bucket_sec),
            "iv_rank": float(row.get("iv_rank") or 0.0),
            "iv_rank_short": float(row.get("iv_rank_short") or 0.0),
            "atm_iv_near": float(row.get("atm_iv_near") or 0.0),
            "skew_25d": float(row.get("skew_25d") or 0.0),
            "skew_zscore": float(row.get("skew_zscore") or 0.0),
            "term_structure_slope": float(row.get("term_structure_slope") or 0.0),
            "term_structure_zscore": float(row.get("term_structure_zscore") or 0.0),
            "unusual_volume_score": float(row.get("unusual_volume_score") or 0.0),
            "call_put_volume_ratio": float(row.get("call_put_volume_ratio") or 1.0),
            "call_put_oi_ratio": float(row.get("call_put_oi_ratio") or 1.0),
            "signal_score": float(row.get("signal_score") or 0.0),
            "gex_norm": float(row.get("gex_norm") or 0.0),
            "gex_norm_z": float(row.get("gex_norm_z") or 0.0),
            "gex_sign": float(row.get("gex_sign") or 0.0),
            "opt_flow_imbalance_z": float(row.get("opt_flow_imbalance_z") or 0.0),
            "source_reliability": 0.78,
            "importance_floor": _event_importance_floor(kind, row),
        },
    }
    event_id = put_normalized_event(payload, con=con)
    if event_id > 0:
        put_options_event_feature(
            {
                "event_id": int(event_id),
                "ts_ms": int(snapshot_ts_ms),
                "symbol": str(row.get("symbol") or "").upper().strip(),
                "event_kind": kind,
                "bucket_sec": int(bucket_sec),
                "signal_score": float(row.get("signal_score") or 0.0),
                "iv_rank": float(row.get("iv_rank") or 0.0),
                "iv_rank_short": float(row.get("iv_rank_short") or 0.0),
                "skew_25d": float(row.get("skew_25d") or 0.0),
                "skew_zscore": float(row.get("skew_zscore") or 0.0),
                "term_structure_slope": float(row.get("term_structure_slope") or 0.0),
                "term_structure_zscore": float(row.get("term_structure_zscore") or 0.0),
                "unusual_volume_score": float(row.get("unusual_volume_score") or 0.0),
                "call_put_volume_ratio": float(row.get("call_put_volume_ratio") or 1.0),
                "call_put_oi_ratio": float(row.get("call_put_oi_ratio") or 1.0),
                "gex_raw": float(row.get("gex_raw") or 0.0),
                "gex_norm": float(row.get("gex_norm") or 0.0),
                "gex_norm_z": float(row.get("gex_norm_z") or 0.0),
                "gex_sign": float(row.get("gex_sign") or 0.0),
                "opt_flow_imbalance": float(row.get("opt_flow_imbalance") or 0.0),
                "opt_flow_imbalance_z": float(row.get("opt_flow_imbalance_z") or 0.0),
                "gex_zero_gamma_flip": row.get("gex_zero_gamma_flip"),
                "meta_json": row.get("meta_json") or {},
            },
            con=con,
        )
    return int(event_id)


def materialize_options_features(con, underlyings: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    _ensure_options_gex_flow_columns(con)
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
        syms = [str(r[0]).upper().strip() for r in rows or [] if r and r[0]]
    if not syms:
        rows = con.execute(
            """
            SELECT DISTINCT symbol
            FROM options_chain
            WHERE symbol IS NOT NULL AND symbol <> ''
            ORDER BY symbol
            """
        ).fetchall()
        syms = [str(r[0]).upper().strip() for r in rows or [] if r and r[0]]

    feature_rows: List[Dict[str, Any]] = []
    for sym in syms:
        row = _build_feature_row(con, sym)
        if not row:
            continue
        _upsert_symbol_feature(con, row, _DAILY_BUCKET_SEC)
        _upsert_symbol_feature(con, row, _INTRADAY_BUCKET_SEC)
        feature_rows.append(row)

    return {
        "rows": feature_rows,
        "symbols": int(len(feature_rows)),
        "snapshots": int(len(feature_rows) * 2),
        "ts_ms": max((int(r.get("snapshot_ts_ms") or 0) for r in feature_rows), default=0),
    }


def emit_options_feature_events(rows: Iterable[Dict[str, Any]], *, bucket_sec: int = _INTRADAY_BUCKET_SEC) -> Dict[str, Any]:
    last_ts_ms = 0
    rows_list = list(rows or [])
    if not rows_list:
        return {"events": 0, "ts_ms": 0}

    def _write(con):
        emitted = 0
        for row in rows_list:
            for kind, title, body in _event_specs(row):
                event_id = _emit_event(kind, title, body, row, int(bucket_sec), con=con)
                if event_id > 0:
                    emitted += 1
        return emitted

    emitted = int(
        run_write_txn(
            _write,
            table="events",
            operation="emit_options_feature_events",
            context={"rows": int(len(rows_list)), "bucket_sec": int(bucket_sec)},
        )
        or 0
    )
    for row in rows_list:
        last_ts_ms = max(last_ts_ms, int(row.get("snapshot_ts_ms") or 0))
    return {"events": int(emitted), "ts_ms": int(last_ts_ms)}
