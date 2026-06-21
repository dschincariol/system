"""Background Monte Carlo refresher for portfolio risk state.

This module samples portfolio return paths from recent price history and stores
summary metrics plus compact chart artifacts in ``risk_state`` so dashboards and
execution gates can consume stressed portfolio risk estimates without blocking
live paths.
"""

import json
import math
import os
import random
import sys
import threading
import time
from typing import Dict, Any, List, Optional, Tuple

from engine.runtime.storage import connect, _table_exists
from engine.runtime.risk_state import set_state
from engine.strategy.har_rv import resolve_vol_forecast


MC_SIMULATIONS = int(float(os.environ.get("MC_SIMULATIONS", "1500")))
MC_HORIZON = int(float(os.environ.get("MC_HORIZON", "10")))
MC_LOOKBACK = int(float(os.environ.get("MC_LOOKBACK", "240")))
MC_REFRESH_MIN_INTERVAL_S = float(os.environ.get("MC_REFRESH_MIN_INTERVAL_S", "30"))
MC_STRESS_VOL_MULT = float(os.environ.get("MC_STRESS_VOL_MULT", "1.35"))
MC_STRESS_CORR_MULT = float(os.environ.get("MC_STRESS_CORR_MULT", "1.20"))
MC_STRESS_NEGATIVE_DRIFT = float(os.environ.get("MC_STRESS_NEGATIVE_DRIFT", "0.0025"))
VOL_FORECAST_SOURCE = str(os.environ.get("VOL_FORECAST_SOURCE", "trailing") or "trailing").strip().lower()

_LOCK = threading.Lock()
_RUNNING = False
_LAST_RUN = 0


def _now_ms():
    return int(time.time() * 1000)


def _pct(xs, q):
    if not xs:
        return 0.0
    vals = sorted(float(x) for x in xs)
    i = int(max(0, min(len(vals) - 1, round((len(vals) - 1) * float(q)))))
    return float(vals[i])


def _cvar(xs, q):
    if not xs:
        return 0.0
    cutoff = _pct(xs, q)
    tail = [float(x) for x in xs if float(x) <= float(cutoff)]
    if not tail:
        return float(cutoff)
    return float(sum(tail) / len(tail))


def _drawdown_percentiles(dd):
    return {
        "p50": _pct(dd, 0.50),
        "p90": _pct(dd, 0.90),
        "p95": _pct(dd, 0.95),
        "p99": _pct(dd, 0.99),
    }


def _fan_rows(paths_by_step: List[List[float]]) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    for idx, values in enumerate(paths_by_step or []):
        if not values:
            continue
        rows.append(
            {
                "step": int(idx + 1),
                "p05": _pct(values, 0.05),
                "p50": _pct(values, 0.50),
                "p95": _pct(values, 0.95),
            }
        )
    return rows


def _distribution_buckets(values: List[float], bucket_count: int = 21) -> List[Dict[str, float]]:
    vals = [float(v) for v in values or [] if math.isfinite(float(v))]
    if not vals:
        return []

    lo = min(vals)
    hi = max(vals)
    n_buckets = int(max(1, min(50, bucket_count)))
    if abs(hi - lo) <= 1e-12:
        return [
            {
                "bucket": f"{lo * 100.0:.2f}%",
                "lower": float(lo),
                "upper": float(hi),
                "value": float(lo),
                "count": int(len(vals)),
                "probability": 1.0,
            }
        ]

    width = float((hi - lo) / n_buckets)
    counts = [0 for _ in range(n_buckets)]
    for value in vals:
        idx = int((float(value) - lo) / width)
        idx = max(0, min(n_buckets - 1, idx))
        counts[idx] += 1

    rows: List[Dict[str, float]] = []
    total = float(len(vals))
    for idx, count in enumerate(counts):
        lower = float(lo + (idx * width))
        upper = float(hi if idx == n_buckets - 1 else lo + ((idx + 1) * width))
        midpoint = float((lower + upper) / 2.0)
        rows.append(
            {
                "bucket": f"{lower * 100.0:.2f}% to {upper * 100.0:.2f}%",
                "lower": lower,
                "upper": upper,
                "value": midpoint,
                "count": int(count),
                "probability": float(count / total if total > 0.0 else 0.0),
            }
        )
    return rows


def _load_history(con, symbol: str, lookback: int) -> List[float]:
    if not _table_exists(con, "prices"):
        return []
    try:
        rows = con.execute(
            """
            SELECT px, price
            FROM prices
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(max(lookback, 10)) + 1),
        ).fetchall()
    except Exception:
        try:
            rows = con.execute(
                """
                SELECT price
                FROM prices
                WHERE symbol=?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (str(symbol), int(max(lookback, 10)) + 1),
            ).fetchall()
        except Exception:
            sys.stderr.write(f"[monte_carlo_risk_engine] load_history_failed symbol={symbol!r}\n")
            sys.stderr.flush()
            return []

    prices = []
    for row in reversed(rows or []):
        if len(row) >= 2:
            v = row[0] if row[0] is not None else row[1]
        else:
            v = row[0]
        try:
            fv = float(v or 0.0)
        except Exception:
            fv = 0.0
        if fv > 0.0:
            prices.append(fv)

    out = []
    prev = None
    for px in prices:
        if prev is not None and prev > 0.0 and px > 0.0:
            out.append(float((px / prev) - 1.0))
        prev = px
    return out[-int(max(lookback, 10)):]


def _mean(xs: List[float]) -> float:
    return float(sum(xs) / len(xs)) if xs else 0.0


def _stdev(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    mu = _mean(xs)
    var = sum((float(x) - mu) ** 2 for x in xs) / float(max(1, len(xs) - 1))
    return float(math.sqrt(max(var, 0.0)))


def _corr(xs: List[float], ys: List[float]) -> float:
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0
    xa = [float(v) for v in xs[-n:]]
    ya = [float(v) for v in ys[-n:]]
    mx = _mean(xa)
    my = _mean(ya)
    sx = _stdev(xa)
    sy = _stdev(ya)
    if sx <= 0.0 or sy <= 0.0:
        return 0.0
    cov = sum((xa[i] - mx) * (ya[i] - my) for i in range(n)) / float(max(1, n - 1))
    return float(max(-0.999, min(0.999, cov / (sx * sy))))


def _normalize_weights(weights: List[float]) -> List[float]:
    gross = float(sum(abs(float(w)) for w in weights))
    if gross <= 1e-12:
        return [0.0 for _ in weights]
    return [float(w) / gross for w in weights]


def _build_inputs(con, desired: Dict[str, Any]) -> Tuple[List[str], List[float], List[float], List[float], List[List[float]], Dict[str, Dict[str, Any]]]:
    rows = []
    for sym, payload in (desired or {}).items():
        w = float(payload.get("weight", 0.0) or 0.0)
        if abs(w) > 0.0:
            rows.append((str(sym), w))
    symbols = [sym for sym, _w in rows]
    weights = _normalize_weights([float(w) for _sym, w in rows])

    # Monte Carlo is intentionally fed by lightweight recent-return proxies from
    # the canonical prices table, not by a separate risk-only market data store.
    histories = [_load_history(con, sym, MC_LOOKBACK) for sym in symbols]
    vols = []
    drifts = []
    vol_meta: Dict[str, Dict[str, Any]] = {}
    now_ms = _now_ms()
    for idx, h in enumerate(histories):
        symbol = symbols[idx]
        trailing_vol = max(0.0001, _stdev(h))
        resolved = resolve_vol_forecast(
            con,
            symbol,
            ts_ms=int(now_ms),
            source=str(VOL_FORECAST_SOURCE or "trailing"),
            trailing_lookback=int(MC_LOOKBACK),
        )
        forecast_vol = resolved.get("vol")
        if forecast_vol is None or float(forecast_vol or 0.0) <= 0.0:
            vol_value = float(trailing_vol)
            source = "trailing"
        else:
            vol_value = float(forecast_vol)
            source = str(resolved.get("resolved_source") or resolved.get("source") or VOL_FORECAST_SOURCE)
        vols.append(max(0.0001, float(vol_value)))
        vol_meta[str(symbol)] = {
            "source": str(source),
            "forecast_ratio": resolved.get("forecast_ratio"),
            "forecast_ts_ms": resolved.get("ts_ms"),
            "fallback": bool(resolved.get("fallback", False)),
            "trailing_vol": float(trailing_vol),
        }
        drifts.append(_mean(h))

    n = len(symbols)
    corr = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            c = _corr(histories[i], histories[j])
            corr[i][j] = c
            corr[j][i] = c

    return symbols, weights, vols, drifts, corr, vol_meta


def _cholesky(cov):
    n = len(cov)
    chol = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1):
            s = float(cov[i][j])
            for k in range(j):
                s -= chol[i][k] * chol[j][k]
            if i == j:
                chol[i][j] = math.sqrt(max(s, 1e-12))
            else:
                denom = chol[j][j] if abs(chol[j][j]) > 1e-12 else 1e-12
                chol[i][j] = s / denom
    return chol


def _simulate(weights, vols, drifts, corr, *, vol_mult=1.0, corr_mult=1.0, drift_shift=0.0):
    n = len(weights)
    if n <= 0:
        return [], [], []

    corr_adj = []
    for i in range(n):
        row = []
        for j in range(n):
            if i == j:
                row.append(1.0)
            else:
                row.append(max(-0.999, min(0.999, float(corr[i][j]) * float(corr_mult))))
        corr_adj.append(row)

    cov = [[float(vols[i]) * float(vol_mult) * float(vols[j]) * float(vol_mult) * float(corr_adj[i][j]) for j in range(n)] for i in range(n)]
    chol = _cholesky(cov)

    pnl = []
    dd = []
    paths_by_step: List[List[float]] = [[] for _ in range(max(0, int(MC_HORIZON)))]
    rng = random.Random()

    # This is a pragmatic stress engine, not a full market simulator. The main
    # knobs are shocked vol/correlation/drift rather than path-dependent microstructure.
    for _ in range(MC_SIMULATIONS):
        equity = 1.0
        peak = 1.0
        worst = 0.0
        total = 0.0

        for step_idx in range(MC_HORIZON):
            z = [rng.gauss(0.0, 1.0) for _ in range(n)]
            asset_r = []
            for i in range(n):
                shock = sum(chol[i][k] * z[k] for k in range(i + 1))
                asset_r.append(float(drifts[i]) + float(drift_shift) + float(shock))
            step = sum(float(weights[i]) * float(asset_r[i]) for i in range(n))
            total += step
            if 0 <= step_idx < len(paths_by_step):
                paths_by_step[step_idx].append(float(total))
            equity *= (1.0 + step)
            if equity > peak:
                peak = equity
            worst = max(worst, 1.0 - (equity / peak if peak > 0.0 else 1.0))

        pnl.append(float(total))
        dd.append(float(worst))

    return pnl, dd, _fan_rows(paths_by_step)


def _write_status(status: str, info: Optional[Dict[str, Any]] = None, pending: Optional[bool] = None):
    # Risk state is published into the shared runtime store so portfolio risk can
    # consume the latest simulation summary without direct process coupling.
    set_state("monte_carlo_risk_status", str(status))
    set_state("monte_carlo_risk_ts_ms", str(_now_ms()))
    if pending is not None:
        set_state("monte_carlo_risk_pending", "1" if pending else "0")
    if info is not None:
        set_state("monte_carlo_risk_info", json.dumps(info, separators=(",", ":"), sort_keys=True))


def _worker(desired):
    con = connect()
    try:
        # The worker owns one full refresh and then exits; external callers are
        # responsible for throttling/scheduling rather than a long-running loop here.
        _write_status("running", pending=True)
        symbols, weights, vols, drifts, corr, vol_meta = _build_inputs(con, desired or {})

        if not symbols:
            info = {
                "enabled": True,
                "ready": False,
                "status": "empty_portfolio",
                "pending": False,
                "ts_ms": _now_ms(),
                "symbols": [],
                "simulations": int(MC_SIMULATIONS),
                "horizon": int(MC_HORIZON),
                "lookback": int(MC_LOOKBACK),
            }
            _write_status("idle", info=info, pending=False)
            return

        base_pnl, base_dd, base_fan = _simulate(weights, vols, drifts, corr)
        stress_pnl, stress_dd, _ = _simulate(
            weights,
            vols,
            drifts,
            corr,
            vol_mult=MC_STRESS_VOL_MULT,
            corr_mult=MC_STRESS_CORR_MULT,
            drift_shift=-abs(MC_STRESS_NEGATIVE_DRIFT),
        )

        info = {
            "enabled": True,
            "ready": True,
            "pending": False,
            "status": "ok",
            "ts_ms": _now_ms(),
            "symbols": symbols,
            "weights": {symbols[i]: float(weights[i]) for i in range(len(symbols))},
            "simulations": int(MC_SIMULATIONS),
            "horizon": int(MC_HORIZON),
            "lookback": int(MC_LOOKBACK),
            "var_95": _pct(base_pnl, 0.05),
            "var_99": _pct(base_pnl, 0.01),
            "cvar_95": _cvar(base_pnl, 0.05),
            "cvar_99": _cvar(base_pnl, 0.01),
            "worst_simulated_drawdown": max(base_dd) if base_dd else 0.0,
            "drawdown_percentiles": _drawdown_percentiles(base_dd),
            "drawdown_cvar_95": _cvar(base_dd, 0.95),
            "drawdown_cvar_99": _cvar(base_dd, 0.99),
            "fan": base_fan,
            "distribution": _distribution_buckets(base_pnl),
            "stress": {
                "vol_mult": float(MC_STRESS_VOL_MULT),
                "corr_mult": float(MC_STRESS_CORR_MULT),
                "negative_drift": float(MC_STRESS_NEGATIVE_DRIFT),
                "var_95": _pct(stress_pnl, 0.05),
                "var_99": _pct(stress_pnl, 0.01),
                "cvar_95": _cvar(stress_pnl, 0.05),
                "cvar_99": _cvar(stress_pnl, 0.01),
                "worst_simulated_drawdown": max(stress_dd) if stress_dd else 0.0,
                "drawdown_percentiles": _drawdown_percentiles(stress_dd),
                "drawdown_cvar_95": _cvar(stress_dd, 0.95),
                "drawdown_cvar_99": _cvar(stress_dd, 0.99),
            },
            "inputs": {
                "volatility": {symbols[i]: float(vols[i]) for i in range(len(symbols))},
                "volatility_source": {symbols[i]: dict(vol_meta.get(symbols[i]) or {}) for i in range(len(symbols))},
                "drift": {symbols[i]: float(drifts[i]) for i in range(len(symbols))},
                "correlation": {symbols[i]: {symbols[j]: float(corr[i][j]) for j in range(len(symbols))} for i in range(len(symbols))},
            },
        }
        _write_status("idle", info=info, pending=False)
    except Exception as e:
        info = {
            "enabled": True,
            "ready": False,
            "pending": False,
            "status": "error",
            "error": str(e),
            "ts_ms": _now_ms(),
        }
        _write_status("error", info=info, pending=False)
    finally:
        con.close()
        global _RUNNING, _LAST_RUN
        _LAST_RUN = _now_ms()
        _RUNNING = False


def request_monte_carlo_refresh(desired):
    """Queue an asynchronous Monte Carlo risk refresh when allowed.

    Parameters
    ----------
    desired : dict
        Desired target portfolio snapshot forwarded to the worker thread.

    Returns
    -------
    bool
        ``True`` if a new daemon worker was queued. ``False`` if a refresh is
        already running or the last completed run is newer than
        ``MC_REFRESH_MIN_INTERVAL_S`` seconds.

    Notes
    -----
    Rate limiting is process-local and guarded by ``_LOCK``. The minimum
    interval is measured in seconds and compared using wall-clock milliseconds.

    Side Effects
    ------------
    Marks Monte Carlo status as queued/pending in risk state and spawns a
    background daemon thread.
    """
    global _RUNNING, _LAST_RUN
    now_ms = _now_ms()
    if _RUNNING:
        return False
    if _LAST_RUN and (now_ms - int(_LAST_RUN)) < int(max(0.0, MC_REFRESH_MIN_INTERVAL_S) * 1000.0):
        return False

    with _LOCK:
        if _RUNNING:
            return False
        if _LAST_RUN and (_now_ms() - int(_LAST_RUN)) < int(max(0.0, MC_REFRESH_MIN_INTERVAL_S) * 1000.0):
            return False
        _RUNNING = True
        _write_status("queued", pending=True)

    t = threading.Thread(target=_worker, args=(dict(desired or {}),), daemon=True)
    t.start()
    return True
