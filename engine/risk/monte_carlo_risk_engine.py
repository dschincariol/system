"""Background Monte Carlo refresher for portfolio risk state.

This module samples portfolio return paths from recent price history and stores
summary metrics plus compact chart artifacts in ``risk_state`` so dashboards and
execution gates can consume stressed portfolio risk estimates without blocking
live paths.
"""

import json
import hashlib
import math
import os
import random
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Tuple, cast

from engine.runtime.storage import connect, _table_exists
from engine.runtime.risk_state import set_state
from engine.risk.covariance import (
    correlation_matrix_dict,
    estimate_covariance_from_return_matrix,
    load_aligned_returns,
)
from engine.strategy.har_rv import resolve_vol_forecast


MC_SIMULATIONS = int(float(os.environ.get("MC_SIMULATIONS", "1500")))
MC_HORIZON = int(float(os.environ.get("MC_HORIZON", "10")))
MC_LOOKBACK = int(float(os.environ.get("MC_LOOKBACK", "240")))
MC_REFRESH_MIN_INTERVAL_S = float(os.environ.get("MC_REFRESH_MIN_INTERVAL_S", "30"))
MC_STRESS_VOL_MULT = float(os.environ.get("MC_STRESS_VOL_MULT", "1.35"))
MC_STRESS_CORR_MULT = float(os.environ.get("MC_STRESS_CORR_MULT", "1.20"))
MC_STRESS_NEGATIVE_DRIFT = float(os.environ.get("MC_STRESS_NEGATIVE_DRIFT", "0.0025"))
MC_SIMULATION_METHOD = str(os.environ.get("MC_SIMULATION_METHOD", "gaussian") or "gaussian").strip().lower().replace("-", "_")
MC_STUDENT_T_DOF = float(os.environ.get("MC_STUDENT_T_DOF", "6.0"))
MC_STUDENT_T_ESTIMATE_DOF = str(os.environ.get("MC_STUDENT_T_ESTIMATE_DOF", "1") or "1").strip().lower() in ("1", "true", "yes", "on")
MC_STUDENT_T_MIN_DOF = float(os.environ.get("MC_STUDENT_T_MIN_DOF", "3.0"))
MC_STUDENT_T_MAX_DOF = float(os.environ.get("MC_STUDENT_T_MAX_DOF", "50.0"))
MC_HISTORICAL_MIN_OBS = int(float(os.environ.get("MC_HISTORICAL_MIN_OBS", "60")))
MC_HISTORICAL_WINDOW = int(float(os.environ.get("MC_HISTORICAL_WINDOW", str(MC_LOOKBACK))))
MC_EVT_CVAR_ENABLED = str(os.environ.get("MC_EVT_CVAR_ENABLED", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
MC_EVT_THRESHOLD_Q = float(os.environ.get("MC_EVT_THRESHOLD_Q", "0.10"))
MC_EVT_MIN_TAIL = int(float(os.environ.get("MC_EVT_MIN_TAIL", "20")))
MC_RANDOM_SEED = str(os.environ.get("MC_RANDOM_SEED", "") or "").strip()
VOL_FORECAST_SOURCE = str(os.environ.get("VOL_FORECAST_SOURCE", "trailing") or "trailing").strip().lower()

_LOCK = threading.Lock()
_RUNNING = False
_LAST_RUN = 0


def _now_ms():
    return int(time.time() * 1000)


def _normalize_simulation_method(value: Any) -> str:
    method = str(value or "gaussian").strip().lower().replace("-", "_")
    aliases = {
        "normal": "gaussian",
        "gauss": "gaussian",
        "gaussian": "gaussian",
        "student": "student_t",
        "student_t": "student_t",
        "t": "student_t",
        "t_copula": "student_t",
        "student_t_copula": "student_t",
        "historical": "historical",
        "bootstrap": "historical",
        "empirical": "historical",
        "empirical_bootstrap": "historical",
        "filtered_historical": "filtered_historical",
        "fhs": "filtered_historical",
    }
    return aliases.get(method, method)


def _optional_seed(value: Any = None) -> Optional[int]:
    raw = MC_RANDOM_SEED if value is None else str(value or "").strip()
    if raw in (None, ""):
        return None
    try:
        return int(float(str(raw).strip()))
    except Exception:
        return None


def _make_rng(seed: Optional[int] = None):
    if seed is None:
        return random.Random()
    return random.Random(int(seed))


def _finite_values(xs: List[float]) -> List[float]:
    out: List[float] = []
    for value in xs or []:
        try:
            fv = float(value)
        except Exception:
            continue
        if math.isfinite(fv):
            out.append(float(fv))
    return out


def _pct(xs, q):
    if not xs:
        return 0.0
    vals = sorted(_finite_values(list(xs)))
    if not vals:
        return 0.0
    i = int(max(0, min(len(vals) - 1, round((len(vals) - 1) * float(q)))))
    return float(vals[i])


def _cvar(xs, q):
    if not xs:
        return 0.0
    cutoff = _pct(xs, q)
    tail = [float(x) for x in _finite_values(list(xs)) if float(x) <= float(cutoff)]
    if not tail:
        return float(cutoff)
    return float(sum(tail) / len(tail))


def _upper_cvar(xs, q):
    if not xs:
        return 0.0
    cutoff = _pct(xs, q)
    tail = [float(x) for x in _finite_values(list(xs)) if float(x) >= float(cutoff)]
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


def _fan_rows(paths_by_step: List[List[float]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
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


def _distribution_buckets(values: List[float], bucket_count: int = 21) -> List[Dict[str, Any]]:
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

    rows: List[Dict[str, Any]] = []
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


def _build_inputs(con, desired: Dict[str, Any]) -> Tuple[List[str], List[float], List[float], List[float], List[List[float]], Dict[str, Dict[str, Any]], Dict[str, Any]]:
    rows = []
    for sym, payload in (desired or {}).items():
        w = float(payload.get("weight", 0.0) or 0.0)
        if abs(w) > 0.0:
            rows.append((str(sym), w))
    symbols = [sym for sym, _w in rows]
    weights = _normalize_weights([float(w) for _sym, w in rows])

    # Monte Carlo uses the shared covariance facade so simulation, portfolio
    # volatility, and correlation caps consume one canonical risk covariance
    # estimate for the same symbol set.
    return_matrix = load_aligned_returns(con, symbols, lookback=int(MC_LOOKBACK))
    historical_window = int(max(1, MC_HISTORICAL_WINDOW))
    historical_matrix = (
        return_matrix
        if historical_window == int(MC_LOOKBACK)
        else load_aligned_returns(con, symbols, lookback=historical_window)
    )
    covariance_estimate = estimate_covariance_from_return_matrix(return_matrix, lookback=int(MC_LOOKBACK))
    histories = [list((return_matrix.individual_returns or {}).get(sym) or []) for sym in symbols]
    n = len(symbols)
    matrix_symbols = [str(sym) for sym in list(historical_matrix.symbols or [])]
    if matrix_symbols == symbols:
        historical_returns = _valid_historical_rows([list(row) for row in list(historical_matrix.returns or [])], n)
    else:
        historical_histories = [list((historical_matrix.individual_returns or {}).get(sym) or []) for sym in symbols]
        historical_returns = _historical_rows_from_histories(historical_histories, n)
    input_meta: Dict[str, Any] = {
        "historical_returns": historical_returns,
        "historical_window": int(MC_HISTORICAL_WINDOW),
        "covariance": {
            "used": True,
            "diagnostics": dict(covariance_estimate.diagnostics or {}),
            "historical_load_diagnostics": dict(historical_matrix.diagnostics or {}),
        },
    }
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

    corr = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
    corr_by_symbol = correlation_matrix_dict(covariance_estimate)
    for i in range(n):
        for j in range(i + 1, n):
            c = corr_by_symbol.get(symbols[i], {}).get(symbols[j])
            if c is None:
                c = _corr(histories[i], histories[j])
            corr[i][j] = c
            corr[j][i] = c

    vol_meta["__covariance_diagnostics__"] = dict(covariance_estimate.diagnostics or {})
    return symbols, weights, vols, drifts, corr, vol_meta, input_meta


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


def _valid_historical_rows(historical_returns: Optional[List[List[float]]], n_assets: int) -> List[List[float]]:
    rows: List[List[float]] = []
    for raw in historical_returns or []:
        if not isinstance(raw, list) and not isinstance(raw, tuple):
            continue
        if len(raw) != int(n_assets):
            continue
        row: List[float] = []
        ok = True
        for value in raw:
            try:
                fv = float(value)
            except Exception:
                ok = False
                break
            if not math.isfinite(fv):
                ok = False
                break
            row.append(float(fv))
        if ok:
            rows.append(row)
    return rows


def _historical_rows_from_histories(histories: List[List[float]], n_assets: int) -> List[List[float]]:
    if int(n_assets) <= 0 or not histories:
        return []
    usable = [[float(v) for v in (hist or []) if math.isfinite(float(v))] for hist in histories[:n_assets]]
    if len(usable) != int(n_assets) or any(len(row) <= 0 for row in usable):
        return []
    n_obs = min(len(row) for row in usable)
    if n_obs <= 0:
        return []
    rows: List[List[float]] = []
    for idx in range(n_obs):
        rows.append([float(usable[col][-n_obs + idx]) for col in range(int(n_assets))])
    return rows


def _estimate_student_t_dof(historical_returns: Optional[List[List[float]]], default: float) -> Tuple[float, Dict[str, Any]]:
    values: List[float] = []
    rows = _valid_historical_rows(historical_returns, len(historical_returns[0]) if historical_returns else 0)
    if rows:
        n_assets = len(rows[0])
        cols = [[float(row[i]) for row in rows] for i in range(n_assets)]
        for col in cols:
            mu = _mean(col)
            sd = _stdev(col)
            if sd <= 1e-12:
                continue
            values.extend([(float(v) - mu) / sd for v in col])
    values = _finite_values(values)
    default_dof = max(float(MC_STUDENT_T_MIN_DOF), min(float(MC_STUDENT_T_MAX_DOF), float(default or 6.0)))
    if len(values) < 20:
        return default_dof, {"estimated": False, "reason": "insufficient_history", "sample_size": int(len(values))}
    mu = _mean(values)
    var = sum((float(v) - mu) ** 2 for v in values) / float(max(1, len(values)))
    if var <= 1e-12:
        return default_dof, {"estimated": False, "reason": "zero_variance", "sample_size": int(len(values))}
    fourth = sum((float(v) - mu) ** 4 for v in values) / float(max(1, len(values)))
    excess = float(fourth / max(var * var, 1e-18) - 3.0)
    if excess <= 1e-9:
        return default_dof, {"estimated": False, "reason": "non_positive_excess_kurtosis", "sample_size": int(len(values)), "excess_kurtosis": excess}
    dof = 4.0 + (6.0 / excess)
    dof = max(float(MC_STUDENT_T_MIN_DOF), min(float(MC_STUDENT_T_MAX_DOF), float(dof)))
    return float(dof), {"estimated": True, "sample_size": int(len(values)), "excess_kurtosis": excess}


def _historical_column_stats(rows: List[List[float]], n_assets: int) -> Tuple[List[float], List[float]]:
    means: List[float] = []
    stdevs: List[float] = []
    for idx in range(int(n_assets)):
        col = [float(row[idx]) for row in rows if len(row) > idx]
        means.append(_mean(col))
        stdevs.append(max(1e-12, _stdev(col)))
    return means, stdevs


def _sample_student_t_vector(rng, n_assets: int, dof: float) -> List[float]:
    df = max(2.01, float(dof or 6.0))
    scale = math.sqrt(max(1e-12, rng.gammavariate(df / 2.0, 2.0) / df))
    variance_normalizer = math.sqrt(max(1e-12, (df - 2.0) / df))
    return [float(rng.gauss(0.0, 1.0) / scale * variance_normalizer) for _ in range(int(n_assets))]


def _simulate(
    weights,
    vols,
    drifts,
    corr,
    *,
    vol_mult=1.0,
    corr_mult=1.0,
    drift_shift=0.0,
    method: Optional[str] = None,
    historical_returns: Optional[List[List[float]]] = None,
    student_t_dof: Optional[float] = None,
    seed: Optional[int] = None,
    return_metadata: bool = False,
):
    n = len(weights)
    if n <= 0:
        empty_meta = {
            "requested_method": _normalize_simulation_method(method or MC_SIMULATION_METHOD),
            "method": "none",
            "distribution": "none",
            "fallback_reasons": ["empty_portfolio"],
        }
        return ([], [], [], empty_meta) if return_metadata else ([], [], [])

    requested_method = _normalize_simulation_method(method or MC_SIMULATION_METHOD)
    method_used = requested_method
    fallback_reasons: List[str] = []
    historical_rows = _valid_historical_rows(historical_returns, n)
    dof_meta: Dict[str, Any] = {}
    dof = float(student_t_dof if student_t_dof is not None else MC_STUDENT_T_DOF)
    if requested_method == "student_t" and bool(MC_STUDENT_T_ESTIMATE_DOF):
        dof, dof_meta = _estimate_student_t_dof(historical_rows, dof)
    dof = max(float(MC_STUDENT_T_MIN_DOF), min(float(MC_STUDENT_T_MAX_DOF), float(dof or 6.0)))

    if requested_method in {"historical", "filtered_historical"} and len(historical_rows) < int(MC_HISTORICAL_MIN_OBS):
        method_used = "gaussian"
        fallback_reasons.append(
            f"insufficient_historical_data:{len(historical_rows)}<{int(MC_HISTORICAL_MIN_OBS)}"
        )
    elif requested_method not in {"gaussian", "student_t", "historical", "filtered_historical"}:
        method_used = "gaussian"
        fallback_reasons.append(f"unsupported_simulation_method:{requested_method}")

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
    rng = _make_rng(seed)
    historical_means, historical_stdevs = _historical_column_stats(historical_rows, n) if historical_rows else ([], [])

    # This is a pragmatic stress engine, not a full market simulator. The main
    # knobs are shocked vol/correlation/drift rather than path-dependent microstructure.
    for _ in range(MC_SIMULATIONS):
        equity = 1.0
        peak = 1.0
        worst = 0.0
        total = 0.0

        for step_idx in range(MC_HORIZON):
            asset_r = []
            if method_used in {"historical", "filtered_historical"} and historical_rows:
                row = historical_rows[rng.randrange(len(historical_rows))]
                for i in range(n):
                    centered = float(row[i]) - float(historical_means[i])
                    if method_used == "filtered_historical":
                        scale = (float(vols[i]) * float(vol_mult)) / max(float(historical_stdevs[i]), 1e-12)
                    else:
                        scale = float(vol_mult)
                    asset_r.append(float(drifts[i]) + float(drift_shift) + float(centered) * float(scale))
            else:
                if method_used == "student_t":
                    z = _sample_student_t_vector(rng, n, dof)
                else:
                    z = [rng.gauss(0.0, 1.0) for _ in range(n)]
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

    metadata = {
        "requested_method": str(requested_method),
        "method": str(method_used),
        "distribution": "student_t_copula" if method_used == "student_t" else ("empirical_bootstrap" if method_used == "historical" else ("filtered_historical" if method_used == "filtered_historical" else "gaussian")),
        "fallback_reasons": list(fallback_reasons),
        "student_t": {
            "dof": float(dof) if method_used == "student_t" else None,
            "estimated": bool(dof_meta.get("estimated", False)),
            "diagnostics": dict(dof_meta or {}),
        },
        "historical": {
            "window": int(MC_HISTORICAL_WINDOW),
            "min_obs": int(MC_HISTORICAL_MIN_OBS),
            "sample_size": int(len(historical_rows)),
        },
        "seeded": seed is not None,
    }
    return (pnl, dd, _fan_rows(paths_by_step), metadata) if return_metadata else (pnl, dd, _fan_rows(paths_by_step))


def _evt_pot_cvar(xs: List[float], q: float, *, threshold_q: float, min_tail: int) -> Tuple[Optional[float], Dict[str, Any]]:
    vals = _finite_values(list(xs or []))
    if not vals:
        return None, {"enabled": True, "applied": False, "reason": "empty_sample", "tail_sample_size": 0}

    losses = sorted([-float(v) for v in vals])
    threshold_prob = max(float(q), min(0.50, float(threshold_q or 0.10)))
    threshold_idx = int(max(0, min(len(losses) - 1, round((len(losses) - 1) * (1.0 - threshold_prob)))))
    threshold = float(losses[threshold_idx])
    excesses = [float(loss - threshold) for loss in losses if float(loss) > threshold]
    if len(excesses) < int(min_tail):
        return None, {
            "enabled": True,
            "applied": False,
            "reason": f"insufficient_tail_sample:{len(excesses)}<{int(min_tail)}",
            "threshold": float(threshold),
            "threshold_q": float(threshold_q),
            "tail_sample_size": int(len(excesses)),
        }

    mean_excess = _mean(excesses)
    if mean_excess <= 1e-12:
        return None, {
            "enabled": True,
            "applied": False,
            "reason": "zero_mean_excess",
            "threshold": float(threshold),
            "threshold_q": float(threshold_q),
            "tail_sample_size": int(len(excesses)),
        }
    var_excess = sum((float(v) - mean_excess) ** 2 for v in excesses) / float(max(1, len(excesses) - 1))
    if var_excess <= mean_excess * mean_excess:
        shape = 0.0
        scale = mean_excess
    else:
        shape = 0.5 * (1.0 - (mean_excess * mean_excess / max(var_excess, 1e-18)))
        shape = max(-0.25, min(0.95, float(shape)))
        scale = max(1e-12, mean_excess * (1.0 - shape))

    tail_prob = len(excesses) / float(len(losses))
    target_prob = max(1e-9, float(q))
    if tail_prob <= target_prob:
        loss_var = _pct(losses, 1.0 - target_prob)
    elif abs(shape) <= 1e-9:
        loss_var = threshold + scale * math.log(tail_prob / target_prob)
    else:
        loss_var = threshold + (scale / shape) * (((tail_prob / target_prob) ** shape) - 1.0)
    if shape >= 1.0:
        return None, {
            "enabled": True,
            "applied": False,
            "reason": "shape_exceeds_es_limit",
            "threshold": float(threshold),
            "threshold_q": float(threshold_q),
            "tail_sample_size": int(len(excesses)),
            "shape": float(shape),
            "scale": float(scale),
        }
    loss_es = (float(loss_var) + float(scale) - float(shape) * float(threshold)) / max(1e-12, 1.0 - float(shape))
    cvar_pnl = -float(loss_es)
    if not math.isfinite(cvar_pnl):
        return None, {
            "enabled": True,
            "applied": False,
            "reason": "nonfinite_evt_cvar",
            "threshold": float(threshold),
            "threshold_q": float(threshold_q),
            "tail_sample_size": int(len(excesses)),
            "shape": float(shape),
            "scale": float(scale),
        }
    return float(cvar_pnl), {
        "enabled": True,
        "applied": True,
        "threshold": float(threshold),
        "threshold_q": float(threshold_q),
        "tail_sample_size": int(len(excesses)),
        "shape": float(shape),
        "scale": float(scale),
        "tail_probability": float(tail_prob),
    }


def _tail_risk_metrics(xs: List[float], *, evt_enabled: Optional[bool] = None) -> Tuple[Dict[str, float], Dict[str, Any]]:
    vals = _finite_values(list(xs or []))
    metrics = {
        "var_95": _pct(vals, 0.05),
        "var_99": _pct(vals, 0.01),
        "cvar_95": _cvar(vals, 0.05),
        "cvar_99": _cvar(vals, 0.01),
    }
    evt_meta: Dict[str, Any] = {
        "enabled": bool(MC_EVT_CVAR_ENABLED if evt_enabled is None else evt_enabled),
        "threshold_q": float(MC_EVT_THRESHOLD_Q),
        "min_tail": int(MC_EVT_MIN_TAIL),
        "tails": {},
    }
    if bool(evt_meta["enabled"]):
        for label, q in (("95", 0.05), ("99", 0.01)):
            candidate, meta = _evt_pot_cvar(vals, q, threshold_q=float(MC_EVT_THRESHOLD_Q), min_tail=int(MC_EVT_MIN_TAIL))
            empirical_key = f"cvar_{label}"
            if candidate is not None and float(candidate) < float(metrics[empirical_key]):
                metrics[empirical_key] = float(candidate)
                meta["used_for_cvar"] = True
            else:
                meta["used_for_cvar"] = False
                if candidate is not None:
                    meta["reason"] = "evt_not_more_conservative"
            evt_meta["tails"][label] = meta
    return metrics, evt_meta


def _forecast_id(info: Dict[str, Any]) -> str:
    payload = {
        "ts_ms": int(info.get("ts_ms") or 0),
        "symbols": list(info.get("symbols") or []),
        "weights": dict(info.get("weights") or {}),
        "horizon": int(info.get("horizon") or 0),
        "method": ((info.get("simulation") or {}).get("method") if isinstance(info.get("simulation"), dict) else ""),
        "var_95": info.get("var_95"),
        "var_99": info.get("var_99"),
        "cvar_95": info.get("cvar_95"),
        "cvar_99": info.get("cvar_99"),
    }
    digest = hashlib.sha256(json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return f"mcvar_{digest[:20]}"


def _persist_var_forecast(con, info: Dict[str, Any]) -> Dict[str, Any]:
    try:
        from engine.runtime.storage import record_risk_var_forecast

        forecast_id = str(info.get("forecast_id") or _forecast_id(info))
        row_id = record_risk_var_forecast(
            con=con,
            forecast_id=forecast_id,
            forecast_ts_ms=int(info.get("ts_ms") or _now_ms()),
            horizon_steps=int(info.get("horizon") or MC_HORIZON),
            var_95=info.get("var_95"),
            var_99=info.get("var_99"),
            cvar_95=info.get("cvar_95"),
            cvar_99=info.get("cvar_99"),
            simulation_method=str((info.get("simulation") or {}).get("method") or ""),
            metadata_json={
                "simulation": info.get("simulation") or {},
                "evt": info.get("evt") or {},
                "symbols": info.get("symbols") or [],
                "weights": info.get("weights") or {},
                "inputs": info.get("inputs") or {},
            },
            created_ts_ms=int(_now_ms()),
        )
        return {"persisted": True, "forecast_id": forecast_id, "row_id": int(row_id or 0)}
    except Exception as exc:
        return {"persisted": False, "forecast_id": str(info.get("forecast_id") or _forecast_id(info)), "reason": f"{type(exc).__name__}:{exc}"}


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
        built = tuple(cast(Any, _build_inputs(con, desired or {})))
        if len(built) >= 7:
            current_built = cast(
                Tuple[List[str], List[float], List[float], List[float], List[List[float]], Dict[str, Dict[str, Any]], Dict[str, Any]],
                built[:7],
            )
            symbols, weights, vols, drifts, corr, vol_meta, input_meta = current_built
        else:
            legacy_built = cast(
                Tuple[List[str], List[float], List[float], List[float], List[List[float]], Dict[str, Dict[str, Any]]],
                built,
            )
            symbols, weights, vols, drifts, corr, vol_meta = legacy_built
            input_meta = {}

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

        covariance_diagnostics = dict((vol_meta or {}).get("__covariance_diagnostics__") or {})
        historical_returns = _valid_historical_rows(input_meta.get("historical_returns"), len(symbols))
        seed = _optional_seed()

        base_pnl, base_dd, base_fan, simulation_meta = cast(
            Tuple[List[float], List[float], List[Dict[str, Any]], Dict[str, Any]],
            _simulate(
                weights,
                vols,
                drifts,
                corr,
                method=MC_SIMULATION_METHOD,
                historical_returns=historical_returns,
                seed=seed,
                return_metadata=True,
            ),
        )
        stress_seed = None if seed is None else int(seed) + 1
        stress_pnl, stress_dd, _, stress_simulation_meta = cast(
            Tuple[List[float], List[float], List[Dict[str, Any]], Dict[str, Any]],
            _simulate(
                weights,
                vols,
                drifts,
                corr,
                vol_mult=MC_STRESS_VOL_MULT,
                corr_mult=MC_STRESS_CORR_MULT,
                drift_shift=-abs(MC_STRESS_NEGATIVE_DRIFT),
                method=MC_SIMULATION_METHOD,
                historical_returns=historical_returns,
                seed=stress_seed,
                return_metadata=True,
            ),
        )
        base_tail_metrics, evt_meta = _tail_risk_metrics(base_pnl)
        stress_tail_metrics, stress_evt_meta = _tail_risk_metrics(stress_pnl)

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
            "var_95": float(base_tail_metrics["var_95"]),
            "var_99": float(base_tail_metrics["var_99"]),
            "cvar_95": float(base_tail_metrics["cvar_95"]),
            "cvar_99": float(base_tail_metrics["cvar_99"]),
            "worst_simulated_drawdown": max(base_dd) if base_dd else 0.0,
            "drawdown_percentiles": _drawdown_percentiles(base_dd),
            "drawdown_cvar_95": _upper_cvar(base_dd, 0.95),
            "drawdown_cvar_99": _upper_cvar(base_dd, 0.99),
            "fan": base_fan,
            "distribution": _distribution_buckets(base_pnl),
            "simulation": dict(simulation_meta),
            "evt": dict(evt_meta),
            "stress": {
                "vol_mult": float(MC_STRESS_VOL_MULT),
                "corr_mult": float(MC_STRESS_CORR_MULT),
                "negative_drift": float(MC_STRESS_NEGATIVE_DRIFT),
                "var_95": float(stress_tail_metrics["var_95"]),
                "var_99": float(stress_tail_metrics["var_99"]),
                "cvar_95": float(stress_tail_metrics["cvar_95"]),
                "cvar_99": float(stress_tail_metrics["cvar_99"]),
                "worst_simulated_drawdown": max(stress_dd) if stress_dd else 0.0,
                "drawdown_percentiles": _drawdown_percentiles(stress_dd),
                "drawdown_cvar_95": _upper_cvar(stress_dd, 0.95),
                "drawdown_cvar_99": _upper_cvar(stress_dd, 0.99),
                "simulation": dict(stress_simulation_meta),
                "evt": dict(stress_evt_meta),
            },
            "inputs": {
                "volatility": {symbols[i]: float(vols[i]) for i in range(len(symbols))},
                "volatility_source": {symbols[i]: dict(vol_meta.get(symbols[i]) or {}) for i in range(len(symbols))},
                "drift": {symbols[i]: float(drifts[i]) for i in range(len(symbols))},
                "correlation": {symbols[i]: {symbols[j]: float(corr[i][j]) for j in range(len(symbols))} for i in range(len(symbols))},
                "covariance_diagnostics": covariance_diagnostics,
                "historical_simulation": {
                    "window": int(MC_HISTORICAL_WINDOW),
                    "min_obs": int(MC_HISTORICAL_MIN_OBS),
                    "sample_size": int(len(historical_returns)),
                },
            },
        }
        info["forecast_id"] = _forecast_id(info)
        persist_meta = _persist_var_forecast(con, info)
        info["forecast_persistence"] = persist_meta
        info["forecast_id"] = str(persist_meta.get("forecast_id") or info["forecast_id"])
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
