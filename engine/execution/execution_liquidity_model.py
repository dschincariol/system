"""
FILE: execution_liquidity_model.py

Execution subsystem module for `execution_liquidity_model`.
"""

import math
import os
import time
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect

LOG = get_logger("engine.execution.execution_liquidity_model")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=30,
        component="engine.execution.execution_liquidity_model",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception as e:
        _warn_nonfatal(
            "execution_liquidity_model_safe_float_failed",
            "EXECUTION_LIQUIDITY_MODEL_SAFE_FLOAT_FAILED",
            e,
            warn_key="execution_liquidity_model_safe_float_failed",
            value_type=type(v).__name__,
        )
        return default


def _percentile(vals: List[float], p: float) -> float:
    if not vals:
        return 0.0
    p = max(0.0, min(1.0, float(p)))
    s = sorted([float(v) for v in vals if v is not None and math.isfinite(float(v))])
    if not s:
        return 0.0
    if len(s) == 1:
        return float(s[0])
    idx = int(round((len(s) - 1) * p))
    idx = max(0, min(len(s) - 1, idx))
    return float(s[idx])


def _rolling_adv(con, symbol: str, lookback_ms: int) -> float:
    # ADV is derived from recent quote volume snapshots and is intentionally
    # approximate; it is used for slicing heuristics, not accounting.
    try:
        rows = con.execute(
            """
            SELECT volume
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms >= ?
              AND volume IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 1500
            """,
            (str(symbol).upper().strip(), int(_now_ms() - int(lookback_ms))),
        ).fetchall()
        vals = []
        for (v,) in rows or []:
            try:
                fv = float(v)
                if fv > 0.0:
                    vals.append(fv)
            except Exception as exc:
                _warn_nonfatal(
                    "execution_liquidity_model_adv_volume_parse_failed",
                    "EXECUTION_LIQUIDITY_MODEL_ADV_VOLUME_PARSE_FAILED",
                    exc,
                    warn_key="execution_liquidity_model_adv_volume_parse_failed",
                    symbol=str(symbol),
                )
        if not vals:
            return 0.0
        return float(sum(vals) / float(len(vals)))
    except Exception as e:
        _warn_nonfatal(
            "execution_liquidity_model_rolling_adv_failed",
            "EXECUTION_LIQUIDITY_MODEL_ROLLING_ADV_FAILED",
            e,
            warn_key=f"execution_liquidity_model_rolling_adv_failed:{symbol}",
            symbol=str(symbol),
        )
        return 0.0


def _recent_volume_delta(con, symbol: str, lookback_ms: int) -> float:
    try:
        rows = con.execute(
            """
            SELECT volume
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms >= ?
              AND volume IS NOT NULL
            ORDER BY ts_ms ASC
            LIMIT 500
            """,
            (str(symbol).upper().strip(), int(_now_ms() - int(lookback_ms))),
        ).fetchall()
        vals = []
        for (v,) in rows or []:
            try:
                fv = float(v)
                if fv >= 0.0:
                    vals.append(fv)
            except Exception as exc:
                _warn_nonfatal(
                    "execution_liquidity_model_recent_volume_parse_failed",
                    "EXECUTION_LIQUIDITY_MODEL_RECENT_VOLUME_PARSE_FAILED",
                    exc,
                    warn_key="execution_liquidity_model_recent_volume_parse_failed",
                    symbol=str(symbol),
                )
        if not vals:
            return 0.0
        total = 0.0
        prev = None
        for cur in vals:
            if prev is None:
                prev = cur
                continue
            delta = float(cur) - float(prev)
            if delta < 0.0:
                delta = float(cur)
            if delta > 0.0:
                total += float(delta)
            prev = cur
        if total <= 0.0 and vals:
            return float(vals[-1])
        return float(total)
    except Exception as e:
        _warn_nonfatal(
            "execution_liquidity_model_recent_volume_delta_failed",
            "EXECUTION_LIQUIDITY_MODEL_RECENT_VOLUME_DELTA_FAILED",
            e,
            warn_key=f"execution_liquidity_model_recent_volume_delta_failed:{symbol}",
            symbol=str(symbol),
        )
        return 0.0


def _intraday_vol_bps(con, symbol: str, lookback_ms: int) -> float:
    try:
        rows = con.execute(
            """
            SELECT last
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms >= ?
              AND last IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT 500
            """,
            (str(symbol).upper().strip(), int(_now_ms() - int(lookback_ms))),
        ).fetchall()
        px = []
        for (v,) in rows or []:
            try:
                fv = float(v)
                if fv > 0.0:
                    px.append(fv)
            except Exception as exc:
                _warn_nonfatal(
                    "execution_liquidity_model_intraday_price_parse_failed",
                    "EXECUTION_LIQUIDITY_MODEL_INTRADAY_PRICE_PARSE_FAILED",
                    exc,
                    warn_key="execution_liquidity_model_intraday_price_parse_failed",
                    symbol=str(symbol),
                )
        if len(px) < 3:
            return 0.0

        rets: List[float] = []
        prev = None
        for cur in reversed(px):
            if prev is not None and prev > 0.0 and cur > 0.0:
                rets.append((float(cur) / float(prev)) - 1.0)
            prev = cur

        if len(rets) < 2:
            return 0.0

        mean = sum(rets) / float(len(rets))
        var = sum((r - mean) ** 2 for r in rets) / float(max(1, len(rets) - 1))
        std = math.sqrt(max(0.0, var))
        return float(std * 10000.0)
    except Exception as e:
        _warn_nonfatal(
            "execution_liquidity_model_intraday_vol_failed",
            "EXECUTION_LIQUIDITY_MODEL_INTRADAY_VOL_FAILED",
            e,
            warn_key=f"execution_liquidity_model_intraday_vol_failed:{symbol}",
            symbol=str(symbol),
        )
        return 0.0


def get_true_nbbo_snapshot(
    symbol: str,
    ts_ms: Optional[int] = None,
    max_age_ms: Optional[int] = None,
) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return {
            "ok": False,
            "symbol": sym,
            "bid_px": None,
            "ask_px": None,
            "mid_px": None,
            "spread_px": None,
            "true_spread_bps": 0.0,
            "source": None,
            "quote_ts_ms": None,
            "age_ms": None,
        }

    # Quotes are bounded by max_age so stale NBBO data does not masquerade as
    # current liquidity context.
    max_age = int(max_age_ms or int(os.environ.get("EXEC_TRUE_SPREAD_MAX_AGE_MS", "120000")))
    ref_ts = int(ts_ms or _now_ms())

    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT ts_ms, bid, ask, last, spread, source
            FROM price_quotes
            WHERE symbol = ?
              AND ts_ms <= ?
              AND ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (sym, int(ref_ts), int(ref_ts - max_age)),
        ).fetchone()

        if not row:
            return {
                "ok": False,
                "symbol": sym,
                "bid_px": None,
                "ask_px": None,
                "mid_px": None,
                "spread_px": None,
                "true_spread_bps": 0.0,
                "source": None,
                "quote_ts_ms": None,
                "age_ms": None,
            }

        quote_ts_ms, bid, ask, last, spread, source = row
        bid_px = _safe_float(bid, 0.0) if bid is not None else None
        ask_px = _safe_float(ask, 0.0) if ask is not None else None
        last_px = _safe_float(last, 0.0) if last is not None else None

        mid_px = None
        if bid_px is not None and ask_px is not None and bid_px > 0.0 and ask_px > 0.0:
            mid_px = (float(bid_px) + float(ask_px)) / 2.0
            spread_px = max(0.0, float(ask_px) - float(bid_px))
        else:
            spread_px = max(0.0, _safe_float(spread, 0.0))
            if last_px and last_px > 0.0:
                mid_px = float(last_px)

        true_spread_bps = 0.0
        if mid_px is not None and float(mid_px) > 0.0 and spread_px is not None:
            true_spread_bps = float(spread_px) / float(mid_px) * 10000.0

        return {
            "ok": True,
            "symbol": sym,
            "bid_px": float(bid_px) if bid_px is not None else None,
            "ask_px": float(ask_px) if ask_px is not None else None,
            "mid_px": float(mid_px) if mid_px is not None else None,
            "spread_px": float(spread_px) if spread_px is not None else None,
            "true_spread_bps": float(true_spread_bps),
            "source": str(source) if source is not None else None,
            "quote_ts_ms": int(quote_ts_ms) if quote_ts_ms is not None else None,
            "age_ms": (int(ref_ts) - int(quote_ts_ms)) if quote_ts_ms is not None else None,
        }
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "execution_liquidity_model_nbbo_close_failed",
                "EXECUTION_LIQUIDITY_MODEL_NBBO_CLOSE_FAILED",
                exc,
                warn_key="execution_liquidity_model_nbbo_close_failed",
            )


def get_execution_liquidity_snapshot(
    symbol: str,
    qty: float,
    px: float,
    ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    ref_ts = int(ts_ms or _now_ms())
    nbbo = get_true_nbbo_snapshot(sym, ts_ms=ref_ts)

    con = connect(readonly=True)
    try:
        adv = _rolling_adv(
            con,
            sym,
            lookback_ms=int(os.environ.get("EXEC_ADV_LOOKBACK_MS", str(3 * 86400000))),
        )
        vol_bps = _intraday_vol_bps(
            con,
            sym,
            lookback_ms=int(os.environ.get("EXEC_VOL_LOOKBACK_MS", str(6 * 3600000))),
        )
        recent_volume_1m = _recent_volume_delta(
            con,
            sym,
            lookback_ms=int(os.environ.get("EXEC_RECENT_VOLUME_1M_LOOKBACK_MS", str(60 * 1000))),
        )
        recent_volume_5m = _recent_volume_delta(
            con,
            sym,
            lookback_ms=int(os.environ.get("EXEC_RECENT_VOLUME_5M_LOOKBACK_MS", str(5 * 60 * 1000))),
        )
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "execution_liquidity_model_snapshot_close_failed",
                "EXECUTION_LIQUIDITY_MODEL_SNAPSHOT_CLOSE_FAILED",
                exc,
                warn_key="execution_liquidity_model_snapshot_close_failed",
            )

    px_f = max(0.0, _safe_float(px, 0.0))
    qty_abs = abs(_safe_float(qty, 0.0))
    notional = float(qty_abs * px_f)

    spread_bps = float(nbbo.get("true_spread_bps") or 0.0)
    adv_participation = 0.0
    if adv > 0.0:
        adv_participation = float(qty_abs / adv)

    live_participation_rate = 0.0
    recent_volume_ref = max(float(recent_volume_1m or 0.0), float(recent_volume_5m or 0.0) / 5.0)
    if recent_volume_ref > 0.0:
        live_participation_rate = float(qty_abs / recent_volume_ref)

    spread_regime = "tight"
    if spread_bps >= float(os.environ.get("EXEC_SPREAD_WIDE_BPS", "12.0")):
        spread_regime = "wide"
    elif spread_bps >= float(os.environ.get("EXEC_SPREAD_NORMAL_BPS", "4.0")):
        spread_regime = "normal"

    liq_score = 1.0
    liq_score *= 1.0 + min(2.0, max(0.0, spread_bps) / max(1.0, float(os.environ.get("EXEC_LIQ_SPREAD_DENOM_BPS", "8.0"))))
    liq_score *= 1.0 + min(2.0, max(0.0, vol_bps) / max(1.0, float(os.environ.get("EXEC_LIQ_VOL_DENOM_BPS", "35.0"))))
    liq_score *= 1.0 + min(2.0, max(0.0, adv_participation) / max(1e-9, float(os.environ.get("EXEC_LIQ_ADV_PARTICIPATION", "0.05"))))

    if spread_regime == "tight":
        aggressiveness_bias = -0.20
    elif spread_regime == "wide":
        aggressiveness_bias = 0.35
    else:
        aggressiveness_bias = 0.0

    if adv_participation >= float(os.environ.get("EXEC_POV_ESCALATE_PARTICIPATION", "0.08")):
        aggressiveness_bias += 0.35

    if live_participation_rate >= float(os.environ.get("EXEC_LIVE_PARTICIPATION_ESCALATE", "0.20")):
        aggressiveness_bias += 0.35

    if vol_bps >= float(os.environ.get("EXEC_VOL_ESCALATE_BPS", "45.0")):
        aggressiveness_bias += 0.35

    limit_offset_bps = max(
        0.0,
        min(
            float(os.environ.get("EXEC_LIMIT_OFFSET_BPS_MAX", "25.0")),
            (spread_bps * float(os.environ.get("EXEC_LIMIT_OFFSET_SPREAD_MULT", "0.50")))
            + (vol_bps * float(os.environ.get("EXEC_LIMIT_OFFSET_VOL_MULT", "0.08"))),
        ),
    )

    slice_mult = 1.0 / max(1.0, liq_score)
    slice_mult = max(
        float(os.environ.get("EXEC_SLICE_MULT_MIN", "0.15")),
        min(float(os.environ.get("EXEC_SLICE_MULT_MAX", "1.25")), slice_mult),
    )

    interval_mult = max(
        1.0,
        min(
            float(os.environ.get("EXEC_INTERVAL_MULT_MAX", "6.0")),
            liq_score,
        ),
    )

    return {
        "ok": True,
        "symbol": sym,
        "ts_ms": int(ref_ts),
        "qty_abs": float(qty_abs),
        "notional": float(notional),
        "rolling_adv": float(adv),
        "adv_participation": float(adv_participation),
        "live_participation_rate": float(live_participation_rate),
        "recent_volume_1m": float(recent_volume_1m),
        "recent_volume_5m": float(recent_volume_5m),
        "spread_regime": str(spread_regime),
        "intraday_vol_bps": float(vol_bps),
        "true_spread_bps": float(spread_bps),
        "aggressiveness_bias": float(aggressiveness_bias),
        "slice_size_mult": float(slice_mult),
        "interval_mult": float(interval_mult),
        "limit_offset_bps": float(limit_offset_bps),
        "nbbo": nbbo,
    }


def attach_liquidity_context(
    order_meta: Dict[str, Any],
    symbol: str,
    qty: float,
    px: float,
    ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    meta = dict(order_meta or {})
    snap = get_execution_liquidity_snapshot(symbol=symbol, qty=qty, px=px, ts_ms=ts_ms)
    meta["liquidity_snapshot"] = snap
    meta["rolling_adv"] = float(snap.get("rolling_adv") or 0.0)
    meta["adv_participation"] = float(snap.get("adv_participation") or 0.0)
    meta["intraday_vol_bps"] = float(snap.get("intraday_vol_bps") or 0.0)
    meta["spread_regime"] = str(snap.get("spread_regime") or "")
    meta["true_spread_bps"] = float(snap.get("true_spread_bps") or 0.0)
    meta["live_participation_rate"] = float(snap.get("live_participation_rate") or 0.0)
    meta["recent_volume_1m"] = float(snap.get("recent_volume_1m") or 0.0)
    meta["recent_volume_5m"] = float(snap.get("recent_volume_5m") or 0.0)

    nbbo_raw = snap.get("nbbo")
    nbbo = dict(nbbo_raw) if isinstance(nbbo_raw, dict) else {}
    if nbbo.get("bid_px") is not None:
        meta["bid_px"] = _safe_float(nbbo.get("bid_px"))
    if nbbo.get("ask_px") is not None:
        meta["ask_px"] = _safe_float(nbbo.get("ask_px"))
    if nbbo.get("mid_px") is not None:
        mid_px = _safe_float(nbbo.get("mid_px"))
        meta["mid_px"] = mid_px
        meta.setdefault("arrival_mid_px", mid_px)
    if nbbo.get("true_spread_bps") is not None:
        meta["spread_bps"] = _safe_float(nbbo.get("true_spread_bps"))
    return meta
