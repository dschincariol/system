"""Canonical covariance estimates for money-at-risk paths.

The facade loads recent point-in-time price returns once for a requested
symbol set, estimates covariance/correlation with a conservative shrinkage
default when enough aligned history exists, and falls back with explicit
serializable diagnostics when it cannot.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np


DEFAULT_METHOD = "ledoit_wolf"
DEFAULT_MIN_OBS = 60
DEFAULT_FALLBACK_POLICY = "sample"
_PSD_FLOOR = 1e-12


@dataclass(frozen=True)
class ReturnMatrix:
    symbols: List[str]
    returns: List[List[float]]
    timestamps: List[int]
    individual_returns: Dict[str, List[float]]
    diagnostics: Dict[str, Any]


@dataclass(frozen=True)
class RiskCovarianceEstimate:
    symbols: List[str]
    covariance: List[List[float]]
    correlation: List[List[float]]
    mean_returns: List[float]
    diagnostics: Dict[str, Any]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(float(str(raw).strip()))
    except Exception:
        return int(default)


def _normalize_method(value: Any) -> str:
    method = str(value or DEFAULT_METHOD).strip().lower().replace("-", "_")
    aliases = {
        "lw": "ledoit_wolf",
        "ledoitwolf": "ledoit_wolf",
        "ledoit_wolf": "ledoit_wolf",
        "oas": "oas",
        "oracle_approximating_shrinkage": "oas",
        "raw": "sample",
        "sample": "sample",
        "sample_covariance": "sample",
        "empirical": "sample",
        "auto": DEFAULT_METHOD,
    }
    return aliases.get(method, DEFAULT_METHOD)


def covariance_config_from_env() -> Dict[str, Any]:
    return {
        "method": _normalize_method(os.environ.get("RISK_COVARIANCE_METHOD", DEFAULT_METHOD)),
        "min_obs": max(2, _env_int("RISK_COVARIANCE_MIN_OBS", DEFAULT_MIN_OBS)),
        "fallback_policy": str(os.environ.get("RISK_COVARIANCE_FALLBACK", DEFAULT_FALLBACK_POLICY) or DEFAULT_FALLBACK_POLICY)
        .strip()
        .lower()
        .replace("-", "_"),
        "rmt_enabled": _env_bool("RISK_COVARIANCE_RMT_ENABLED", False),
        "rmt_min_assets": max(2, _env_int("RISK_COVARIANCE_RMT_MIN_ASSETS", 12)),
        "rmt_detone": _env_bool("RISK_COVARIANCE_RMT_DETONE", False),
    }


def _normalize_symbols(symbols: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for raw in symbols or []:
        symbol = str(raw or "").upper().strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


def _finite_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _prices_table_readable(con: Any) -> bool:
    try:
        con.execute("SELECT 1 FROM prices LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _price_history_batch(
    con: Any,
    symbols: Sequence[str],
    *,
    lookback: int,
    end_ts_ms: Optional[int] = None,
) -> Tuple[Dict[str, List[Tuple[int, float]]], str]:
    if con is None or not symbols or not _prices_table_readable(con):
        return {}, "prices_table_unavailable"

    placeholders = ",".join("?" for _ in symbols)
    limit = int(max(3, lookback) + 1)
    where = f"UPPER(TRIM(symbol)) IN ({placeholders})"
    params: List[Any] = list(symbols)
    if end_ts_ms is not None and int(end_ts_ms) > 0:
        where += " AND ts_ms <= ?"
        params.append(int(end_ts_ms))

    variants = (
        ("COALESCE(px, price)", "px_or_price"),
        ("price", "price"),
        ("px", "px"),
    )
    rows: Sequence[Any] = ()
    source = ""
    last_error = ""
    for price_expr, source_name in variants:
        try:
            rows = con.execute(
                f"""
                SELECT symbol, ts_ms, price_value
                FROM (
                  SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    ts_ms,
                    {price_expr} AS price_value,
                    ROW_NUMBER() OVER (
                      PARTITION BY UPPER(TRIM(symbol))
                      ORDER BY ts_ms DESC
                    ) AS rn
                  FROM prices
                  WHERE {where}
                ) ranked
                WHERE rn <= ?
                ORDER BY symbol ASC, ts_ms ASC
                """,
                tuple(params + [limit]),
            ).fetchall()
            source = source_name
            break
        except Exception as exc:
            last_error = type(exc).__name__
            rows = ()
            continue

    if not source:
        return {}, f"price_query_failed:{last_error or 'unknown'}"

    by_symbol: Dict[str, List[Tuple[int, float]]] = {str(s): [] for s in symbols}
    for row in rows or []:
        try:
            symbol = str(row[0] or "").upper().strip()
            ts_ms = int(row[1])
            price = _finite_float(row[2])
        except Exception:
            continue
        if symbol not in by_symbol or price is None or price <= 0.0:
            continue
        by_symbol[symbol].append((int(ts_ms), float(price)))
    return by_symbol, source


def load_aligned_returns(
    con: Any,
    symbols: Sequence[Any],
    *,
    lookback: int = 240,
    end_ts_ms: Optional[int] = None,
) -> ReturnMatrix:
    requested = _normalize_symbols(symbols)
    diagnostics: Dict[str, Any] = {
        "requested_symbols": list(requested),
        "lookback": int(lookback),
        "end_ts_ms": (int(end_ts_ms) if end_ts_ms is not None and int(end_ts_ms) > 0 else None),
    }
    if not requested:
        diagnostics.update(
            {
                "symbols": [],
                "covered_symbols": [],
                "missing_symbols": [],
                "n_obs": 0,
                "n_assets": 0,
                "fallback_reason": "empty_symbol_set",
            }
        )
        return ReturnMatrix([], [], [], {}, diagnostics)

    history, source = _price_history_batch(con, requested, lookback=int(lookback), end_ts_ms=end_ts_ms)
    diagnostics["price_source"] = str(source)
    if not history:
        diagnostics.update(
            {
                "symbols": [],
                "covered_symbols": [],
                "missing_symbols": list(requested),
                "n_obs": 0,
                "n_assets": 0,
                "fallback_reason": str(source),
            }
        )
        return ReturnMatrix([], [], [], {}, diagnostics)

    individual: Dict[str, List[float]] = {}
    return_by_ts: Dict[str, Dict[int, float]] = {}
    for symbol in requested:
        rows = sorted(history.get(symbol) or [], key=lambda item: int(item[0]))
        series: List[float] = []
        by_ts: Dict[int, float] = {}
        prev: Optional[float] = None
        for ts_ms, price in rows:
            if prev is not None and prev > 0.0 and price > 0.0:
                ret = math.log(float(price) / float(prev))
                if math.isfinite(ret):
                    series.append(float(ret))
                    by_ts[int(ts_ms)] = float(ret)
            prev = float(price)
        if series:
            individual[symbol] = list(series[-int(max(1, lookback)) :])
            return_by_ts[symbol] = dict(by_ts)

    eligible = [symbol for symbol in requested if len(individual.get(symbol) or []) >= 2]
    if not eligible:
        diagnostics.update(
            {
                "symbols": [],
                "covered_symbols": [],
                "missing_symbols": list(requested),
                "n_obs": 0,
                "n_assets": 0,
                "fallback_reason": "insufficient_price_history",
            }
        )
        return ReturnMatrix([], [], [], individual, diagnostics)

    if len(eligible) == 1:
        symbol = eligible[0]
        values = list(individual.get(symbol) or [])[-int(max(1, lookback)) :]
        timestamps = sorted(return_by_ts.get(symbol) or {})[-len(values) :]
        matrix = [[float(v)] for v in values]
        diagnostics.update(
            {
                "symbols": [symbol],
                "covered_symbols": [symbol],
                "missing_symbols": [s for s in requested if s != symbol],
                "n_obs": int(len(matrix)),
                "n_assets": 1,
                "aligned_observations": int(len(matrix)),
            }
        )
        return ReturnMatrix([symbol], matrix, [int(ts) for ts in timestamps], individual, diagnostics)

    common: Optional[set[int]] = None
    for symbol in eligible:
        ts_set = set(return_by_ts.get(symbol) or {})
        common = ts_set if common is None else common & ts_set
    common_ts = sorted(common or [])[-int(max(1, lookback)) :]

    matrix: List[List[float]] = []
    for ts_ms in common_ts:
        row: List[float] = []
        complete = True
        for symbol in eligible:
            value = return_by_ts.get(symbol, {}).get(int(ts_ms))
            if value is None:
                complete = False
                break
            row.append(float(value))
        if complete:
            matrix.append(row)

    diagnostics.update(
        {
            "symbols": list(eligible),
            "covered_symbols": list(eligible),
            "missing_symbols": [s for s in requested if s not in eligible],
            "n_obs": int(len(matrix)),
            "n_assets": int(len(eligible)),
            "aligned_observations": int(len(matrix)),
        }
    )
    if len(matrix) < 2:
        diagnostics["fallback_reason"] = "insufficient_aligned_observations"
    return ReturnMatrix(list(eligible), matrix, [int(ts) for ts in common_ts], individual, diagnostics)


def _empty_estimate(requested: Sequence[str], reason: str, *, lookback: int, method: str) -> RiskCovarianceEstimate:
    diagnostics = {
        "method": "unavailable",
        "requested_method": str(method),
        "n_obs": 0,
        "n_assets": 0,
        "lookback": int(lookback),
        "symbols": [],
        "covered_symbols": [],
        "requested_symbols": list(requested),
        "missing_symbols": list(requested),
        "fallback_reason": str(reason),
        "condition_number": None,
        "shrinkage": None,
    }
    return RiskCovarianceEstimate([], [], [], [], diagnostics)


def _sample_covariance(matrix: np.ndarray) -> np.ndarray:
    arr = np.asarray(matrix, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[0] < 2 or arr.shape[1] < 1:
        return np.zeros((0, 0), dtype=np.float64)
    if arr.shape[1] == 1:
        return np.asarray([[float(np.var(arr[:, 0], ddof=1))]], dtype=np.float64)
    cov = np.cov(arr, rowvar=False, ddof=1)
    return np.asarray(cov, dtype=np.float64)


def _pairwise_sample_covariance(symbols: Sequence[str], individual: Mapping[str, Sequence[float]]) -> Tuple[np.ndarray, int]:
    n_assets = len(symbols)
    cov = np.zeros((n_assets, n_assets), dtype=np.float64)
    n_obs_used = 0
    centered: Dict[str, List[float]] = {}
    stdevs: Dict[str, float] = {}
    for symbol in symbols:
        vals = [float(v) for v in list(individual.get(symbol) or []) if math.isfinite(float(v))]
        if len(vals) >= 2:
            mean = float(sum(vals) / len(vals))
            centered[symbol] = [float(v - mean) for v in vals]
            var = sum(v * v for v in centered[symbol]) / float(max(1, len(vals) - 1))
            stdevs[symbol] = math.sqrt(max(0.0, float(var)))
            n_obs_used = max(n_obs_used, len(vals))
    for i, left in enumerate(symbols):
        std_i = float(stdevs.get(left, 0.0))
        cov[i, i] = float(std_i * std_i)
        for j in range(i + 1, n_assets):
            right = symbols[j]
            xs = centered.get(left) or []
            ys = centered.get(right) or []
            pair_n = min(len(xs), len(ys))
            if pair_n < 2:
                value = 0.0
            else:
                xa = xs[-pair_n:]
                ya = ys[-pair_n:]
                raw_cov = sum(float(xa[k]) * float(ya[k]) for k in range(pair_n)) / float(max(1, pair_n - 1))
                std_j = float(stdevs.get(right, 0.0))
                if std_i <= 1e-12 or std_j <= 1e-12:
                    value = 0.0
                else:
                    corr = max(-1.0, min(1.0, float(raw_cov / (std_i * std_j))))
                    value = float(corr * std_i * std_j)
            cov[i, j] = float(value)
            cov[j, i] = float(value)
    return cov, int(n_obs_used)


def _condition_number(covariance: np.ndarray) -> Optional[float]:
    arr = np.asarray(covariance, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[0] == 0 or arr.shape[0] != arr.shape[1]:
        return None
    try:
        eig = np.linalg.eigvalsh((arr + arr.T) / 2.0)
    except Exception:
        return None
    if eig.size == 0:
        return None
    max_eig = float(max(float(np.max(eig)), _PSD_FLOOR))
    min_eig = float(max(float(np.min(np.maximum(eig, _PSD_FLOOR))), _PSD_FLOOR))
    cond = max_eig / max(float(min_eig), _PSD_FLOOR)
    if not math.isfinite(cond):
        return 1e300
    return float(min(cond, 1e300))


def _make_psd(covariance: np.ndarray) -> np.ndarray:
    arr = np.asarray(covariance, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[0] == 0:
        return np.zeros((0, 0), dtype=np.float64)
    arr = (arr + arr.T) / 2.0
    try:
        eig, vec = np.linalg.eigh(arr)
        eig = np.maximum(eig, _PSD_FLOOR)
        out = (vec * eig) @ vec.T
        out = (out + out.T) / 2.0
    except Exception:
        diag = np.maximum(np.diag(arr), _PSD_FLOOR)
        out = np.diag(diag)
    return np.asarray(out, dtype=np.float64)


def _corr_from_cov(covariance: np.ndarray) -> np.ndarray:
    cov = np.asarray(covariance, dtype=np.float64)
    if cov.ndim != 2 or cov.shape[0] == 0:
        return np.zeros((0, 0), dtype=np.float64)
    diag = np.maximum(np.diag(cov), 0.0)
    vol = np.sqrt(diag)
    corr = np.zeros_like(cov, dtype=np.float64)
    for i in range(cov.shape[0]):
        corr[i, i] = 1.0
        for j in range(i + 1, cov.shape[1]):
            denom = float(vol[i] * vol[j])
            value = 0.0 if denom <= 1e-18 else float(cov[i, j] / denom)
            value = max(-1.0, min(1.0, value))
            corr[i, j] = value
            corr[j, i] = value
    return corr


def _cov_from_corr(correlation: np.ndarray, vols: np.ndarray) -> np.ndarray:
    corr = np.asarray(correlation, dtype=np.float64)
    vol = np.asarray(vols, dtype=np.float64)
    return corr * np.outer(vol, vol)


def _apply_rmt(
    covariance: np.ndarray,
    *,
    n_obs: int,
    detone: bool,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    cov = np.asarray(covariance, dtype=np.float64)
    n_assets = int(cov.shape[0]) if cov.ndim == 2 else 0
    meta: Dict[str, Any] = {
        "rmt_applied": False,
        "rmt_detoned": False,
        "rmt_lambda_plus": None,
    }
    if n_assets < 2 or int(n_obs) <= n_assets:
        return cov, meta

    vols = np.sqrt(np.maximum(np.diag(cov), _PSD_FLOOR))
    corr = _corr_from_cov(cov)
    try:
        eig, vec = np.linalg.eigh((corr + corr.T) / 2.0)
    except Exception:
        return cov, meta

    q = float(n_obs) / float(n_assets)
    lambda_plus = float((1.0 + math.sqrt(1.0 / max(q, 1e-12))) ** 2)
    clipped = np.asarray(eig, dtype=np.float64).copy()
    noise_mask = clipped <= float(lambda_plus)
    if bool(np.any(noise_mask)):
        clipped[noise_mask] = float(np.mean(clipped[noise_mask]))
    if bool(detone) and clipped.size >= 2:
        clipped[-1] = float(np.mean(clipped[:-1]))
        meta["rmt_detoned"] = True

    corr_clipped = (vec * clipped) @ vec.T
    diag = np.sqrt(np.maximum(np.diag(corr_clipped), _PSD_FLOOR))
    corr_clipped = corr_clipped / np.outer(diag, diag)
    corr_clipped = (corr_clipped + corr_clipped.T) / 2.0
    np.fill_diagonal(corr_clipped, 1.0)

    meta["rmt_applied"] = True
    meta["rmt_lambda_plus"] = float(lambda_plus)
    return _cov_from_corr(corr_clipped, vols), meta


def _estimate_from_matrix(
    symbols: Sequence[str],
    matrix: Sequence[Sequence[float]],
    *,
    requested_method: str,
    min_obs: int,
    fallback_policy: str,
    fallback_reason: str = "",
    individual_returns: Optional[Mapping[str, Sequence[float]]] = None,
    rmt_enabled: bool = False,
    rmt_min_assets: int = 12,
    rmt_detone: bool = False,
    lookback: int = 240,
    requested_symbols: Optional[Sequence[str]] = None,
    load_diagnostics: Optional[Mapping[str, Any]] = None,
) -> RiskCovarianceEstimate:
    method = _normalize_method(requested_method)
    requested = list(requested_symbols or symbols)
    covered = list(symbols)
    arr = np.asarray(matrix, dtype=np.float64)
    if arr.ndim == 1 and arr.size > 0:
        arr = arr.reshape((-1, 1))
    if arr.ndim != 2:
        arr = np.zeros((0, len(covered)), dtype=np.float64)
    n_obs_aligned = int(arr.shape[0])
    n_assets = int(len(covered))
    sample_cov = _sample_covariance(arr) if n_obs_aligned >= 2 and n_assets > 0 else np.zeros((n_assets, n_assets), dtype=np.float64)
    sample_condition = _condition_number(sample_cov)

    diagnostics: Dict[str, Any] = {
        "method": "",
        "requested_method": str(method),
        "n_obs": int(n_obs_aligned),
        "n_assets": int(n_assets),
        "lookback": int(lookback),
        "min_obs": int(min_obs),
        "fallback_policy": str(fallback_policy),
        "fallback_reason": str(fallback_reason or ""),
        "shrinkage": None,
        "condition_number": None,
        "sample_condition_number": sample_condition,
        "symbols": list(covered),
        "covered_symbols": list(covered),
        "requested_symbols": list(requested),
        "missing_symbols": [str(s) for s in requested if str(s) not in set(covered)],
        "aligned_observations": int(n_obs_aligned),
        "rmt_enabled": bool(rmt_enabled),
        "rmt_applied": False,
        "rmt_detoned": False,
        "rmt_lambda_plus": None,
    }
    if load_diagnostics:
        for key in ("price_source", "end_ts_ms"):
            if key in load_diagnostics:
                diagnostics[key] = load_diagnostics[key]

    if n_assets == 0:
        diagnostics["method"] = "unavailable"
        diagnostics["fallback_reason"] = diagnostics["fallback_reason"] or "no_covered_symbols"
        return RiskCovarianceEstimate([], [], [], [], diagnostics)

    covariance: Optional[np.ndarray] = None
    method_used = method
    shrinkage: Optional[float] = None
    reason = str(fallback_reason or "")

    if n_assets == 1:
        if n_obs_aligned >= 2:
            covariance = sample_cov
        else:
            vals = list((individual_returns or {}).get(covered[0]) or [])
            if len(vals) >= 2:
                covariance = np.asarray([[float(np.var(np.asarray(vals, dtype=np.float64), ddof=1))]], dtype=np.float64)
            else:
                covariance = np.asarray([[0.0]], dtype=np.float64)
                reason = reason or "insufficient_price_history"
        method_used = "sample"
        if method != "sample":
            reason = reason or "single_asset_sample"
    elif method in {"ledoit_wolf", "oas"} and n_obs_aligned >= int(min_obs):
        try:
            if method == "ledoit_wolf":
                from sklearn.covariance import LedoitWolf

                model = LedoitWolf().fit(arr)
            else:
                from sklearn.covariance import OAS

                model = OAS().fit(arr)
            covariance = np.asarray(model.covariance_, dtype=np.float64)
            raw_shrinkage = getattr(model, "shrinkage_", None)
            shrinkage = None if raw_shrinkage is None else float(raw_shrinkage)
            method_used = method
        except Exception as exc:
            reason = f"shrinkage_failed:{type(exc).__name__}"
            covariance = None
    elif method in {"ledoit_wolf", "oas"}:
        reason = reason or "insufficient_observations_for_shrinkage"

    if covariance is None and method == "sample" and n_obs_aligned >= 2:
        covariance = sample_cov
        method_used = "sample_aligned"

    if covariance is None:
        policy = str(fallback_policy or DEFAULT_FALLBACK_POLICY).strip().lower().replace("-", "_")
        if policy == "diagonal":
            base = sample_cov if n_obs_aligned >= 2 else None
            if base is None or base.size == 0:
                base, used = _pairwise_sample_covariance(covered, individual_returns or {})
                diagnostics["n_obs"] = int(used)
            covariance = np.diag(np.maximum(np.diag(base), 0.0))
            method_used = "diagonal"
            reason = reason or "fallback_policy_diagonal"
        elif policy in {"none", "fail", "unavailable"}:
            covariance = np.zeros((n_assets, n_assets), dtype=np.float64)
            method_used = "unavailable"
            reason = reason or "fallback_policy_none"
        elif n_obs_aligned >= 2:
            covariance = sample_cov
            method_used = "sample_aligned"
            reason = reason or "sample_fallback"
        else:
            covariance, used = _pairwise_sample_covariance(covered, individual_returns or {})
            diagnostics["n_obs"] = int(used)
            method_used = "sample_pairwise"
            reason = reason or "insufficient_aligned_observations"

    if bool(rmt_enabled) and n_assets >= int(rmt_min_assets) and method_used != "unavailable":
        covariance, rmt_meta = _apply_rmt(covariance, n_obs=max(int(diagnostics.get("n_obs") or 0), n_obs_aligned), detone=bool(rmt_detone))
        diagnostics.update(rmt_meta)
        if bool(rmt_meta.get("rmt_applied")):
            method_used = f"{method_used}+rmt"
            if bool(rmt_meta.get("rmt_detoned")):
                method_used = f"{method_used}+detoned"

    covariance = _make_psd(covariance)
    correlation = _corr_from_cov(covariance)
    mean_returns = [0.0 for _ in covered]
    if n_obs_aligned > 0 and arr.shape[1] == n_assets:
        means = np.mean(arr, axis=0)
        mean_returns = [float(x) if math.isfinite(float(x)) else 0.0 for x in means]
    elif individual_returns:
        mean_returns = []
        for symbol in covered:
            vals = [float(v) for v in list(individual_returns.get(symbol) or []) if math.isfinite(float(v))]
            mean_returns.append(float(sum(vals) / len(vals)) if vals else 0.0)

    diagnostics["method"] = str(method_used)
    diagnostics["fallback_reason"] = str(reason or "")
    diagnostics["shrinkage"] = shrinkage
    diagnostics["condition_number"] = _condition_number(covariance)

    return RiskCovarianceEstimate(
        list(covered),
        covariance.astype(float).tolist(),
        correlation.astype(float).tolist(),
        [float(v) for v in mean_returns],
        _json_safe_diagnostics(diagnostics),
    )


def _json_safe_diagnostics(payload: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in sorted(payload.keys()):
        value = payload[key]
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, float) and not math.isfinite(value):
            out[str(key)] = None
        elif isinstance(value, (list, tuple)):
            clean: List[Any] = []
            for item in value:
                if isinstance(item, np.generic):
                    item = item.item()
                if isinstance(item, float) and not math.isfinite(item):
                    clean.append(None)
                else:
                    clean.append(item)
            out[str(key)] = clean
        else:
            out[str(key)] = value
    return out


def estimate_covariance_from_returns(
    symbols: Sequence[Any],
    returns: Sequence[Sequence[float]],
    *,
    method: Optional[str] = None,
    min_obs: Optional[int] = None,
    fallback_policy: Optional[str] = None,
    rmt_enabled: Optional[bool] = None,
    rmt_min_assets: Optional[int] = None,
    rmt_detone: Optional[bool] = None,
    lookback: int = 240,
) -> RiskCovarianceEstimate:
    config = covariance_config_from_env()
    normalized = _normalize_symbols(symbols)
    return _estimate_from_matrix(
        normalized,
        returns,
        requested_method=_normalize_method(method or config["method"]),
        min_obs=int(min_obs if min_obs is not None else config["min_obs"]),
        fallback_policy=str(fallback_policy if fallback_policy is not None else config["fallback_policy"]),
        rmt_enabled=bool(config["rmt_enabled"] if rmt_enabled is None else rmt_enabled),
        rmt_min_assets=int(config["rmt_min_assets"] if rmt_min_assets is None else rmt_min_assets),
        rmt_detone=bool(config["rmt_detone"] if rmt_detone is None else rmt_detone),
        lookback=int(lookback),
        requested_symbols=normalized,
    )


def estimate_covariance_from_return_matrix(
    matrix: ReturnMatrix,
    *,
    method: Optional[str] = None,
    min_obs: Optional[int] = None,
    fallback_policy: Optional[str] = None,
    rmt_enabled: Optional[bool] = None,
    rmt_min_assets: Optional[int] = None,
    rmt_detone: Optional[bool] = None,
    lookback: int = 240,
) -> RiskCovarianceEstimate:
    config = covariance_config_from_env()
    requested_symbols = list((matrix.diagnostics or {}).get("requested_symbols") or matrix.symbols or [])
    if not matrix.symbols:
        return _empty_estimate(
            _normalize_symbols(requested_symbols),
            str((matrix.diagnostics or {}).get("fallback_reason") or "no_covered_symbols"),
            lookback=int(lookback),
            method=_normalize_method(method or config["method"]),
        )
    return _estimate_from_matrix(
        matrix.symbols,
        matrix.returns,
        requested_method=_normalize_method(method or config["method"]),
        min_obs=int(min_obs if min_obs is not None else config["min_obs"]),
        fallback_policy=str(fallback_policy if fallback_policy is not None else config["fallback_policy"]),
        fallback_reason=str((matrix.diagnostics or {}).get("fallback_reason") or ""),
        individual_returns=matrix.individual_returns,
        rmt_enabled=bool(config["rmt_enabled"] if rmt_enabled is None else rmt_enabled),
        rmt_min_assets=int(config["rmt_min_assets"] if rmt_min_assets is None else rmt_min_assets),
        rmt_detone=bool(config["rmt_detone"] if rmt_detone is None else rmt_detone),
        lookback=int(lookback),
        requested_symbols=_normalize_symbols(requested_symbols),
        load_diagnostics=matrix.diagnostics,
    )


def estimate_covariance(
    con: Any,
    symbols: Sequence[Any],
    *,
    lookback: int = 240,
    method: Optional[str] = None,
    min_obs: Optional[int] = None,
    fallback_policy: Optional[str] = None,
    end_ts_ms: Optional[int] = None,
    rmt_enabled: Optional[bool] = None,
    rmt_min_assets: Optional[int] = None,
    rmt_detone: Optional[bool] = None,
) -> RiskCovarianceEstimate:
    config = covariance_config_from_env()
    requested = _normalize_symbols(symbols)
    requested_method = _normalize_method(method or config["method"])
    if not requested:
        return _empty_estimate([], "empty_symbol_set", lookback=int(lookback), method=requested_method)

    matrix = load_aligned_returns(con, requested, lookback=int(lookback), end_ts_ms=end_ts_ms)
    if not matrix.symbols:
        return _empty_estimate(
            requested,
            str(matrix.diagnostics.get("fallback_reason") or "no_covered_symbols"),
            lookback=int(lookback),
            method=requested_method,
        )

    return _estimate_from_matrix(
        matrix.symbols,
        matrix.returns,
        requested_method=requested_method,
        min_obs=int(min_obs if min_obs is not None else config["min_obs"]),
        fallback_policy=str(fallback_policy if fallback_policy is not None else config["fallback_policy"]),
        fallback_reason=str(matrix.diagnostics.get("fallback_reason") or ""),
        individual_returns=matrix.individual_returns,
        rmt_enabled=bool(config["rmt_enabled"] if rmt_enabled is None else rmt_enabled),
        rmt_min_assets=int(config["rmt_min_assets"] if rmt_min_assets is None else rmt_min_assets),
        rmt_detone=bool(config["rmt_detone"] if rmt_detone is None else rmt_detone),
        lookback=int(lookback),
        requested_symbols=requested,
        load_diagnostics=matrix.diagnostics,
    )


def correlation_matrix_dict(estimate: RiskCovarianceEstimate) -> Dict[str, Dict[str, float]]:
    symbols = list(estimate.symbols or [])
    corr = estimate.correlation or []
    out: Dict[str, Dict[str, float]] = {}
    for i, left in enumerate(symbols):
        out[str(left)] = {}
        for j, right in enumerate(symbols):
            try:
                out[str(left)][str(right)] = float(corr[i][j])
            except Exception:
                out[str(left)][str(right)] = 1.0 if i == j else 0.0
    return out


def covariance_matrix_dict(estimate: RiskCovarianceEstimate) -> Dict[str, Dict[str, float]]:
    symbols = list(estimate.symbols or [])
    cov = estimate.covariance or []
    out: Dict[str, Dict[str, float]] = {}
    for i, left in enumerate(symbols):
        out[str(left)] = {}
        for j, right in enumerate(symbols):
            try:
                out[str(left)][str(right)] = float(cov[i][j])
            except Exception:
                out[str(left)][str(right)] = 0.0
    return out


def correlation_for_pair(
    con: Any,
    left: Any,
    right: Any,
    *,
    lookback: int = 240,
    method: Optional[str] = None,
    min_obs: Optional[int] = None,
    fallback_policy: Optional[str] = None,
) -> Optional[float]:
    a = str(left or "").upper().strip()
    b = str(right or "").upper().strip()
    if not a or not b:
        return None
    if a == b:
        return 1.0
    estimate = estimate_covariance(
        con,
        [a, b],
        lookback=int(lookback),
        method=method,
        min_obs=min_obs,
        fallback_policy=fallback_policy,
    )
    index = {symbol: idx for idx, symbol in enumerate(estimate.symbols or [])}
    if a not in index or b not in index:
        return None
    try:
        return float(estimate.correlation[index[a]][index[b]])
    except Exception:
        return None


def portfolio_volatility_from_estimate(
    estimate: RiskCovarianceEstimate,
    weights_by_symbol: Mapping[str, float],
    *,
    normalize_gross: bool = False,
) -> Optional[float]:
    symbols = list(estimate.symbols or [])
    if not symbols or not estimate.covariance:
        return None
    weights = np.asarray([float(weights_by_symbol.get(str(symbol), 0.0) or 0.0) for symbol in symbols], dtype=np.float64)
    if bool(normalize_gross):
        gross = float(np.sum(np.abs(weights)))
        if gross <= 1e-12:
            return None
        weights = weights / gross
    covariance = np.asarray(estimate.covariance, dtype=np.float64)
    if covariance.shape != (len(symbols), len(symbols)):
        return None
    variance = float(weights.T @ covariance @ weights)
    if not math.isfinite(variance):
        return None
    return float(math.sqrt(max(0.0, variance)))


__all__ = [
    "RiskCovarianceEstimate",
    "ReturnMatrix",
    "correlation_for_pair",
    "correlation_matrix_dict",
    "covariance_config_from_env",
    "covariance_matrix_dict",
    "estimate_covariance",
    "estimate_covariance_from_return_matrix",
    "estimate_covariance_from_returns",
    "load_aligned_returns",
    "portfolio_volatility_from_estimate",
]
