# NEW FILE: compute_factor_features.py
# (create this file exactly)

"""
Compute Tier-1 external factor-universe features from existing `prices` proxies.

Writes FIXED-DIM feature series into `factor_features` for leakage-safe as-of joins.

Intended cadence: 1–5 minutes (or aligned to your prices poll cadence).
"""

import os
import time
import math
import json
import logging
from typing import List, Tuple, Optional

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)

from engine.runtime.factor_universe import put_factor_feature

OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()


LOG = logging.getLogger("compute_factor_features")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))


_LOOKBACK = int(os.environ.get("FACTOR_PRICE_LOOKBACK", "600"))  # points
_ZWIN = int(os.environ.get("FACTOR_ZWIN", "240"))                # points
_DELTA_N = int(os.environ.get("FACTOR_DN", "5"))                 # points


def _warn_nonfatal(event: str, error: BaseException, **extra) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="ops.compute_factor_features",
        extra=extra,
        persist=False,
    )


def _load_prices(con, symbol: str, ts_ms: int, n: int) -> List[Tuple[int, float]]:
    rows = con.execute(
        """
        SELECT ts_ms, price
        FROM prices
        WHERE symbol=? AND ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (str(symbol), int(ts_ms), int(n)),
    ).fetchall()
    out: List[Tuple[int, float]] = []
    for t, p in (rows or []):
        if t is None or p is None:
            continue
        try:
            out.append((int(t), float(p)))
        except Exception as e:
            logging.warning("compute_factor_features price_parse_failed err=%s", e)
            continue
    out.reverse()
    return out


def _zscore_last(xs: np.ndarray, win: int) -> float:
    xs = np.asarray(xs, dtype=float)
    if xs.size < max(30, int(win)):
        return 0.0
    w = xs[-int(win):]
    mu = float(np.mean(w))
    sd = float(np.std(w, ddof=1)) if w.size >= 3 else 0.0
    if sd <= 1e-12:
        return 0.0
    z = (float(w[-1]) - mu) / sd
    return float(z) if math.isfinite(z) else 0.0


def _delta_n(xs: np.ndarray, n: int) -> float:
    xs = np.asarray(xs, dtype=float)
    if xs.size < (int(n) + 1):
        return 0.0
    d = float(xs[-1] - xs[-(int(n) + 1)])
    return float(d) if math.isfinite(d) else 0.0


def _realized_vol(pts: List[Tuple[int, float]], win: int = 20) -> float:
    if len(pts) < (win + 2):
        return 0.0
    px = np.asarray([p for _, p in pts], dtype=float)
    rets = np.diff(np.log(np.maximum(px, 1e-12)))
    if rets.size < win:
        return 0.0
    r = rets[-win:]
    sd = float(np.std(r, ddof=1)) if r.size >= 3 else 0.0
    return float(sd) if math.isfinite(sd) else 0.0


def run_once(ts_ms: Optional[int] = None) -> None:
    init_db()
    con = connect()
    try:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)

        # This script derives factor-universe features from already-ingested price
        # proxies. It does not fetch external data itself; that boundary matters
        # for leakage safety and runtime ownership.
        # --- load proxy series from prices
        vix_pts = _load_prices(con, "VIX", ts_ms, _LOOKBACK)
        tnx_pts = _load_prices(con, "TNX", ts_ms, _LOOKBACK)
        fvx_pts = _load_prices(con, "FVX", ts_ms, _LOOKBACK)

        hyg_pts = _load_prices(con, "HYG", ts_ms, _LOOKBACK)
        lqd_pts = _load_prices(con, "LQD", ts_ms, _LOOKBACK)

        spy_pts = _load_prices(con, "SPY", ts_ms, _LOOKBACK)
        agg_pts = _load_prices(con, "AGG", ts_ms, _LOOKBACK)

        # --- arrays
        vix = np.asarray([p for _, p in vix_pts], dtype=float)
        tnx = np.asarray([p for _, p in tnx_pts], dtype=float)
        fvx = np.asarray([p for _, p in fvx_pts], dtype=float)

        hyg = np.asarray([p for _, p in hyg_pts], dtype=float)
        lqd = np.asarray([p for _, p in lqd_pts], dtype=float)

        spy = np.asarray([p for _, p in spy_pts], dtype=float)
        agg = np.asarray([p for _, p in agg_pts], dtype=float)

        # --- macro: yields + curve
        put_factor_feature(
            con,
            feature_id="macro.us_10y_yield_z",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_zscore_last(tnx, _ZWIN),
            meta={"proxy_symbol": "TNX"},
        )
        put_factor_feature(
            con,
            feature_id="macro.us_10y_yield_d5",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_delta_n(tnx, _DELTA_N),
            meta={"proxy_symbol": "TNX", "dn": int(_DELTA_N)},
        )
        put_factor_feature(
            con,
            feature_id="macro.us_5y_yield_z",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_zscore_last(fvx, _ZWIN),
            meta={"proxy_symbol": "FVX"},
        )
        put_factor_feature(
            con,
            feature_id="macro.us_5y_yield_d5",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_delta_n(fvx, _DELTA_N),
            meta={"proxy_symbol": "FVX", "dn": int(_DELTA_N)},
        )

        curve = tnx - fvx if (tnx.size and fvx.size and tnx.size == fvx.size) else np.asarray([], dtype=float)
        put_factor_feature(
            con,
            feature_id="macro.us_curve_10y_5y_z",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_zscore_last(curve, _ZWIN),
            meta={"proxy_symbols": ["TNX", "FVX"]},
        )

        # --- vol: VIX + realized vol on SPY
        put_factor_feature(
            con,
            feature_id="vol.vix_z",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_zscore_last(vix, _ZWIN),
            meta={"proxy_symbol": "VIX"},
        )
        put_factor_feature(
            con,
            feature_id="vol.vix_d5",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_delta_n(vix, _DELTA_N),
            meta={"proxy_symbol": "VIX", "dn": int(_DELTA_N)},
        )

        # zscore of realized vol requires history; proxy by zscore of rolling rv computed on the fly
        if len(spy_pts) >= (_ZWIN + 25):
            px = np.asarray([p for _, p in spy_pts], dtype=float)
            rets = np.diff(np.log(np.maximum(px, 1e-12)))
            rv_series = []
            for i in range(20, rets.size + 1):
                w = rets[i - 20 : i]
                sd = float(np.std(w, ddof=1)) if w.size >= 3 else 0.0
                rv_series.append(sd)
            rv_series = np.asarray(rv_series, dtype=float)
            rv_z = _zscore_last(rv_series, min(_ZWIN, rv_series.size))
        else:
            rv_z = 0.0

        put_factor_feature(
            con,
            feature_id="vol.rv20_z",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=float(rv_z),
            meta={"proxy_symbol": "SPY", "win": 20},
        )

        # Credit and flow features here are explicitly proxies, not a claim that
        # the underlying economic quantity is directly observed in the DB.
        # --- credit: HYG vs LQD spread proxy (log ratio)
        if hyg.size and lqd.size and hyg.size == lqd.size:
            spread = np.log(np.maximum(hyg, 1e-12)) - np.log(np.maximum(lqd, 1e-12))
        else:
            spread = np.asarray([], dtype=float)

        put_factor_feature(
            con,
            feature_id="credit.hyg_lqd_spread_z",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_zscore_last(spread, _ZWIN),
            meta={"proxy_symbols": ["HYG", "LQD"]},
        )
        put_factor_feature(
            con,
            feature_id="credit.hyg_lqd_spread_d5",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_delta_n(spread, _DELTA_N),
            meta={"proxy_symbols": ["HYG", "LQD"], "dn": int(_DELTA_N)},
        )

        # --- flows proxy: SPY vs AGG log ratio (risk-on appetite)
        if spy.size and agg.size and spy.size == agg.size:
            ratio = np.log(np.maximum(spy, 1e-12)) - np.log(np.maximum(agg, 1e-12))
        else:
            ratio = np.asarray([], dtype=float)

        put_factor_feature(
            con,
            feature_id="flows.spy_agg_ratio_z",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_zscore_last(ratio, _ZWIN),
            meta={"proxy_symbols": ["SPY", "AGG"]},
        )
        put_factor_feature(
            con,
            feature_id="flows.spy_agg_ratio_d5",
            asof_ts=ts_ms,
            effective_ts=ts_ms,
            value=_delta_n(ratio, _DELTA_N),
            meta={"proxy_symbols": ["SPY", "AGG"], "dn": int(_DELTA_N)},
        )

    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("compute_factor_features_db_close_failed", e)


def main():
    job_name = "compute_factor_features"
    init_db()

    lock_stale_after_s = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
    if not acquire_job_lock(job_name, OWNER, PID, ttl_s=lock_stale_after_s):
        return

    last_hb = 0.0
    try:
        while True:
            ts_ms = int(time.time() * 1000)
            run_once(ts_ms=ts_ms)

            now = time.time()
            if now - last_hb > 30:
                touch_job_lock(job_name, OWNER, PID)
                put_job_heartbeat(
                    job_name,
                    OWNER,
                    PID,
                    extra_json=json.dumps({"ts_ms": int(ts_ms)}, separators=(",", ":"), sort_keys=True),
                )
                last_hb = now

            time.sleep(float(os.environ.get("FACTOR_FEATURES_INTERVAL_S", "60")))
    finally:
        release_job_lock(job_name, OWNER, PID)


if __name__ == "__main__":
    main()
