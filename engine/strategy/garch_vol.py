"""Conditional volatility forecasts for risk sizing.

GARCH-family forecasts are risk inputs only. They may be used for volatility
targeting, sizing, and stress simulation, but are not registered as alpha
features by default.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import math
import os
import time
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.strategy.har_rv import (
    MS_PER_DAY,
    TRADING_DAYS_PER_YEAR,
    _candidate_symbols,
    _day_start_ms,
    _safe_float,
    _table_readable,
    _trailing_fallback_vol,
    realized_variance_observations,
    trailing_vol_from_rv,
)

LOG = get_logger("engine.strategy.garch_vol")
DEFAULT_MIN_HISTORY = int(os.environ.get("GARCH_VOL_MIN_HISTORY", "120"))
DEFAULT_HISTORY_DAYS = int(os.environ.get("GARCH_VOL_HISTORY_DAYS", "756"))
DEFAULT_FORECAST_MAX_AGE_DAYS = int(os.environ.get("GARCH_VOL_FORECAST_MAX_AGE_DAYS", "7"))
DEFAULT_MODEL_TYPE = str(os.environ.get("GARCH_VOL_MODEL_TYPE", "garch") or "garch")
DEFAULT_DISTRIBUTION = str(os.environ.get("GARCH_VOL_DISTRIBUTION", "normal") or "normal")
DEFAULT_USE_ARCH = str(os.environ.get("GARCH_VOL_USE_ARCH", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_EWMA_LAMBDA = float(os.environ.get("GARCH_VOL_EWMA_LAMBDA", "0.94"))
DEFAULT_HORIZON_DAYS = int(os.environ.get("GARCH_VOL_HORIZON_DAYS", "1"))
RETURN_SCALE = 100.0
_WARNED_NONFATAL_KEYS: set[str] = set()


@dataclass(frozen=True)
class ReturnObservation:
    ts_ms: int
    ret: float
    source: str


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.garch_vol",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _normalize_model_type(value: Any) -> str:
    key = str(value or DEFAULT_MODEL_TYPE).strip().lower().replace("-", "_")
    aliases = {
        "garch": "garch",
        "garch_1_1": "garch",
        "garch11": "garch",
        "egarch": "egarch",
        "egarch_1_1": "egarch",
        "gjr": "gjr_garch",
        "gjr_garch": "gjr_garch",
        "gjrgarch": "gjr_garch",
        "garch_gjr": "gjr_garch",
    }
    return aliases.get(key, "garch")


def _normalize_distribution(value: Any) -> str:
    key = str(value or DEFAULT_DISTRIBUTION).strip().lower().replace("-", "")
    aliases = {
        "normal": "normal",
        "gaussian": "normal",
        "t": "t",
        "studentt": "t",
        "students_t": "t",
        "skewt": "skewt",
        "skewstudent": "skewt",
    }
    return aliases.get(key, "normal")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _daily_bar_returns(
    con,
    symbol: str,
    *,
    end_ts_ms: int,
    max_days: int,
) -> list[ReturnObservation]:
    if not _table_readable(con, "price_bars"):
        return []
    try:
        rows = con.execute(
            """
            SELECT ts_ms, c
            FROM price_bars
            WHERE symbol=?
              AND tf_s >= 86400
              AND ts_ms <= ?
              AND c IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol).upper().strip(), int(end_ts_ms), int(max_days) + 1),
        ).fetchall()
    except Exception:
        return []
    closes: list[tuple[int, float]] = []
    for row in reversed(rows or []):
        try:
            ts_ms = int(row[0])
            close = float(row[1])
        except Exception:
            continue
        if close > 0.0 and math.isfinite(close):
            closes.append((int(ts_ms), float(close)))
    return _returns_from_closes(closes, source="daily_price_bars")[-int(max_days) :]


def _prices_daily_close_returns(
    con,
    symbol: str,
    *,
    end_ts_ms: int,
    max_days: int,
) -> list[ReturnObservation]:
    if not _table_readable(con, "prices"):
        return []
    variants = ("COALESCE(px, price)", "price", "px")
    rows: Sequence[Any] = ()
    for price_expr in variants:
        try:
            rows = con.execute(
                f"""
                SELECT ts_ms, {price_expr} AS price_value
                FROM prices
                WHERE symbol=?
                  AND ts_ms <= ?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (str(symbol).upper().strip(), int(end_ts_ms), int(max_days) * 100 + 100),
            ).fetchall()
            break
        except Exception:
            rows = ()
            continue
    if not rows:
        return []

    daily_close: dict[int, tuple[int, float]] = {}
    for row in reversed(rows or []):
        try:
            ts_ms = int(row[0])
            price = float(row[1])
        except Exception:
            continue
        if price <= 0.0 or not math.isfinite(price):
            continue
        daily_close[_day_start_ms(int(ts_ms))] = (int(ts_ms), float(price))
    closes = [daily_close[day] for day in sorted(daily_close)]
    return _returns_from_closes(closes, source="prices_daily_close")[-int(max_days) :]


def _returns_from_closes(closes: Sequence[tuple[int, float]], *, source: str) -> list[ReturnObservation]:
    out: list[ReturnObservation] = []
    prev: float | None = None
    for ts_ms, close in closes:
        if prev is not None and prev > 0.0 and close > 0.0:
            ret = math.log(float(close) / float(prev))
            if math.isfinite(ret):
                out.append(ReturnObservation(ts_ms=int(ts_ms), ret=float(ret), source=str(source)))
        prev = float(close)
    return out


def daily_return_observations(
    con,
    symbol: str,
    *,
    end_ts_ms: int | None = None,
    max_days: int = DEFAULT_HISTORY_DAYS,
) -> list[ReturnObservation]:
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return []
    anchor_ts_ms = int(end_ts_ms if end_ts_ms is not None and int(end_ts_ms) > 0 else _now_ms())
    returns = _daily_bar_returns(
        con,
        symbol_key,
        end_ts_ms=int(anchor_ts_ms),
        max_days=int(max_days),
    )
    if returns:
        return returns
    return _prices_daily_close_returns(
        con,
        symbol_key,
        end_ts_ms=int(anchor_ts_ms),
        max_days=int(max_days),
    )


def _sample_vol(returns: Sequence[float]) -> float | None:
    values = [float(v) for v in returns if _safe_float(v, None) is not None]
    if len(values) < 2:
        return None
    vol = float(np.std(np.asarray(values, dtype=float), ddof=1))
    if not math.isfinite(vol) or vol <= 0.0:
        return None
    return float(vol)


def ewma_variance_forecast(returns: Sequence[float], *, decay: float = DEFAULT_EWMA_LAMBDA) -> float | None:
    values = [float(v) for v in returns if _safe_float(v, None) is not None]
    if not values:
        return None
    decay = max(0.50, min(0.995, float(decay)))
    squares = [float(v) * float(v) for v in values]
    seed_window = squares[-min(len(squares), 20) :]
    variance = float(sum(seed_window) / float(len(seed_window)))
    for ret in values[-min(len(values), 252) :]:
        variance = float(decay) * float(variance) + (1.0 - float(decay)) * float(ret) * float(ret)
    if not math.isfinite(variance) or variance <= 0.0:
        return None
    return float(max(1.0e-12, variance))


def _load_arch_model() -> tuple[Any | None, str | None]:
    try:
        from arch import arch_model  # type: ignore

        return arch_model, None
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _arch_model_kwargs(model_type: str, distribution: str) -> dict[str, Any]:
    model = _normalize_model_type(model_type)
    kwargs: dict[str, Any] = {
        "mean": "Zero",
        "p": 1,
        "q": 1,
        "dist": _normalize_distribution(distribution),
        "rescale": False,
    }
    if model == "egarch":
        kwargs.update({"vol": "EGARCH", "o": 1})
    elif model == "gjr_garch":
        kwargs.update({"vol": "GARCH", "o": 1})
    else:
        kwargs.update({"vol": "GARCH", "o": 0})
    return kwargs


def _extract_forecast_variance(value: Any, *, horizon_days: int) -> float | None:
    variance = getattr(value, "variance", value)
    horizon_idx = max(0, int(horizon_days) - 1)
    try:
        if hasattr(variance, "iloc"):
            shape = getattr(variance, "shape", None)
            col_idx = horizon_idx
            if shape and len(shape) >= 2:
                col_idx = min(int(shape[1]) - 1, horizon_idx)
            out = variance.iloc[-1, col_idx]
            return float(out)
    except Exception:
        pass  # no-op-guard: allow - fall back to array-based variance extraction
    try:
        arr = np.asarray(variance.to_numpy() if hasattr(variance, "to_numpy") else variance, dtype=float)
        if arr.ndim == 0:
            return float(arr)
        if arr.ndim == 1:
            return float(arr[min(arr.size - 1, horizon_idx)])
        return float(arr[-1, min(arr.shape[1] - 1, horizon_idx)])
    except Exception:
        return None


def _fit_arch_forecast(
    returns: Sequence[float],
    *,
    model_type: str,
    distribution: str,
    horizon_days: int,
) -> tuple[float | None, dict[str, Any]]:
    arch_model, load_error = _load_arch_model()
    diagnostics: dict[str, Any] = {
        "model": _normalize_model_type(model_type),
        "distribution": _normalize_distribution(distribution),
        "horizon_days": int(horizon_days),
        "return_scale": float(RETURN_SCALE),
    }
    if arch_model is None:
        diagnostics.update(
            {
                "converged": False,
                "convergence_status": "dependency_unavailable",
                "fallback_reason": "arch_dependency_unavailable",
                "arch_error": str(load_error or ""),
            }
        )
        return None, diagnostics

    values = np.asarray([float(v) for v in returns if _safe_float(v, None) is not None], dtype=float)
    if values.size < 2:
        diagnostics.update(
            {
                "converged": False,
                "convergence_status": "insufficient_history",
                "fallback_reason": "insufficient_return_history",
            }
        )
        return None, diagnostics

    scaled = values * float(RETURN_SCALE)
    try:
        model = arch_model(scaled, **_arch_model_kwargs(model_type, distribution))
        fit = model.fit(disp="off", show_warning=False)
    except Exception as exc:
        diagnostics.update(
            {
                "converged": False,
                "convergence_status": "fit_failed",
                "fallback_reason": "garch_fit_failed",
                "fit_error": f"{type(exc).__name__}: {exc}",
            }
        )
        return None, diagnostics

    flag = getattr(fit, "convergence_flag", None)
    opt = getattr(fit, "optimization_result", None)
    success = getattr(opt, "success", None) if opt is not None else None
    converged = bool((flag in (None, 0)) and (success is not False))
    diagnostics.update(
        {
            "converged": bool(converged),
            "convergence_status": "converged" if converged else "convergence_failed",
            "convergence_flag": flag,
            "loglikelihood": _safe_float(getattr(fit, "loglikelihood", None), None),
            "aic": _safe_float(getattr(fit, "aic", None), None),
            "bic": _safe_float(getattr(fit, "bic", None), None),
        }
    )
    if not converged:
        diagnostics["fallback_reason"] = "garch_convergence_failed"
        return None, diagnostics

    try:
        forecast = fit.forecast(horizon=int(horizon_days), reindex=False)
    except Exception as exc:
        diagnostics.update(
            {
                "converged": False,
                "convergence_status": "forecast_failed",
                "fallback_reason": "garch_forecast_failed",
                "forecast_error": f"{type(exc).__name__}: {exc}",
            }
        )
        return None, diagnostics
    forecast_variance_scaled = _extract_forecast_variance(forecast, horizon_days=int(horizon_days))
    if forecast_variance_scaled is None or not math.isfinite(float(forecast_variance_scaled)) or float(forecast_variance_scaled) <= 0.0:
        diagnostics.update(
            {
                "converged": False,
                "convergence_status": "invalid_forecast",
                "fallback_reason": "garch_invalid_forecast",
            }
        )
        return None, diagnostics
    forecast_rv = float(forecast_variance_scaled) / float(RETURN_SCALE * RETURN_SCALE)
    if not math.isfinite(forecast_rv) or forecast_rv <= 0.0:
        diagnostics.update(
            {
                "converged": False,
                "convergence_status": "invalid_forecast_units",
                "fallback_reason": "garch_invalid_forecast",
            }
        )
        return None, diagnostics
    diagnostics["forecast_variance_scaled"] = float(forecast_variance_scaled)
    return float(forecast_rv), diagnostics


def ensure_garch_vol_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS garch_vol_forecasts (
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            asof_ts_ms BIGINT,
            model_type TEXT NOT NULL,
            distribution TEXT NOT NULL,
            horizon_days BIGINT NOT NULL DEFAULT 1,
            return_source TEXT NOT NULL,
            trailing_vol DOUBLE PRECISION,
            forecast_rv_1d DOUBLE PRECISION NOT NULL,
            forecast_vol_1d DOUBLE PRECISION NOT NULL,
            forecast_ann_vol DOUBLE PRECISION NOT NULL,
            forecast_ratio DOUBLE PRECISION NOT NULL,
            n_obs BIGINT NOT NULL DEFAULT 0,
            n_train BIGINT NOT NULL DEFAULT 0,
            min_history BIGINT NOT NULL DEFAULT 120,
            converged BIGINT NOT NULL DEFAULT 0,
            convergence_status TEXT,
            loglikelihood DOUBLE PRECISION,
            aic DOUBLE PRECISION,
            bic DOUBLE PRECISION,
            fallback BIGINT NOT NULL DEFAULT 0,
            fallback_reason TEXT,
            diagnostics_json TEXT,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(symbol, ts_ms, model_type)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_garch_vol_forecasts_symbol_model_ts_desc
          ON garch_vol_forecasts(symbol, model_type, ts_ms DESC)
        """
    )


def _fallback_rv(
    *,
    returns: Sequence[float],
    trailing_vol: float | None,
) -> float:
    rv = ewma_variance_forecast(returns)
    if rv is not None:
        return float(rv)
    if trailing_vol is not None and float(trailing_vol) > 0.0:
        return float(trailing_vol) * float(trailing_vol)
    return 1.0e-12


def _forecast_payload(
    *,
    symbol: str,
    ts_ms: int,
    returns: Sequence[ReturnObservation],
    trailing_vol: float,
    forecast_rv: float,
    model_type: str,
    distribution: str,
    horizon_days: int,
    min_history: int,
    fallback: bool,
    fallback_reason: str | None,
    diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    latest = returns[-1] if returns else None
    forecast_rv = max(1.0e-12, float(forecast_rv))
    forecast_vol = math.sqrt(float(forecast_rv))
    trailing = max(1.0e-12, float(trailing_vol))
    diag = dict(diagnostics or {})
    diag.setdefault("model", _normalize_model_type(model_type))
    diag.setdefault("distribution", _normalize_distribution(distribution))
    diag.setdefault("n_obs", int(len(returns)))
    diag.setdefault("forecast_ts_ms", int(ts_ms))
    diag.setdefault("horizon_days", int(horizon_days))
    if fallback_reason:
        diag.setdefault("fallback_reason", str(fallback_reason))
    return {
        "symbol": str(symbol).upper().strip(),
        "ts_ms": int(ts_ms),
        "asof_ts_ms": int(latest.ts_ms) if latest else None,
        "model_type": _normalize_model_type(model_type),
        "distribution": _normalize_distribution(distribution),
        "horizon_days": int(horizon_days),
        "return_source": str(latest.source if latest else "unavailable"),
        "trailing_vol": float(trailing),
        "forecast_rv_1d": float(forecast_rv),
        "forecast_vol_1d": float(forecast_vol),
        "forecast_ann_vol": float(forecast_vol * math.sqrt(TRADING_DAYS_PER_YEAR)),
        "forecast_ratio": float(forecast_vol / trailing),
        "n_obs": int(len(returns)),
        "n_train": int(len(returns)),
        "min_history": int(min_history),
        "converged": bool(diag.get("converged", False)),
        "convergence_status": str(diag.get("convergence_status") or ("fallback" if fallback else "converged")),
        "loglikelihood": _safe_float(diag.get("loglikelihood"), None),
        "aic": _safe_float(diag.get("aic"), None),
        "bic": _safe_float(diag.get("bic"), None),
        "fallback": bool(fallback),
        "fallback_reason": None if fallback_reason is None else str(fallback_reason),
        "diagnostics": diag,
        "created_ts_ms": _now_ms(),
    }


def forecast_garch_for_symbol(
    con,
    symbol: str,
    *,
    ts_ms: int | None = None,
    min_history: int = DEFAULT_MIN_HISTORY,
    history_days: int = DEFAULT_HISTORY_DAYS,
    model_type: str = DEFAULT_MODEL_TYPE,
    distribution: str = DEFAULT_DISTRIBUTION,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
    use_arch: bool | None = None,
) -> dict[str, Any]:
    symbol_key = str(symbol or "").upper().strip()
    now_ms = int(ts_ms if ts_ms is not None else _now_ms())
    returns = daily_return_observations(
        con,
        symbol_key,
        end_ts_ms=int(now_ms),
        max_days=int(history_days),
    )
    return_values = [float(obs.ret) for obs in returns]
    trailing_vol = _sample_vol(return_values)
    if trailing_vol is None:
        try:
            rv_values = [float(obs.rv) for obs in realized_variance_observations(con, symbol_key, end_ts_ms=int(now_ms), max_days=int(history_days))]
            trailing_vol = trailing_vol_from_rv(rv_values)
        except Exception:
            trailing_vol = None
    if trailing_vol is None:
        trailing_vol = _trailing_fallback_vol(con, symbol_key)
    trailing_vol_f = max(1.0e-6, float(trailing_vol or 0.0))

    use_arch_flag = _env_bool("GARCH_VOL_USE_ARCH", DEFAULT_USE_ARCH) if use_arch is None else bool(use_arch)
    model = _normalize_model_type(model_type)
    dist = _normalize_distribution(distribution)
    diagnostics: dict[str, Any] = {
        "model": model,
        "distribution": dist,
        "n_obs": int(len(return_values)),
        "forecast_ts_ms": int(now_ms),
        "horizon_days": int(horizon_days),
        "use_arch": bool(use_arch_flag),
    }
    forecast_rv: float | None = None
    fallback_reason: str | None = None

    if len(return_values) < int(min_history):
        fallback_reason = "insufficient_return_history"
        diagnostics["convergence_status"] = "insufficient_history"
    elif not use_arch_flag:
        fallback_reason = "arch_disabled"
        diagnostics["convergence_status"] = "arch_disabled"
    else:
        forecast_rv, fit_diag = _fit_arch_forecast(
            return_values,
            model_type=model,
            distribution=dist,
            horizon_days=int(horizon_days),
        )
        diagnostics.update(fit_diag)
        fallback_reason = str(fit_diag.get("fallback_reason") or "") or None

    fallback = forecast_rv is None
    if fallback:
        forecast_rv = _fallback_rv(returns=return_values, trailing_vol=trailing_vol_f)
        fallback_reason = fallback_reason or "garch_forecast_unavailable"
        diagnostics["fallback_reason"] = str(fallback_reason)
        diagnostics.setdefault("converged", False)
        diagnostics.setdefault("convergence_status", "fallback")

    return _forecast_payload(
        symbol=symbol_key,
        ts_ms=int(now_ms),
        returns=returns,
        trailing_vol=float(trailing_vol_f),
        forecast_rv=float(forecast_rv),
        model_type=model,
        distribution=dist,
        horizon_days=int(horizon_days),
        min_history=int(min_history),
        fallback=bool(fallback),
        fallback_reason=fallback_reason,
        diagnostics=diagnostics,
    )


def upsert_garch_forecast(con, payload: Mapping[str, Any]) -> None:
    ensure_garch_vol_schema(con)
    diagnostics = dict(payload.get("diagnostics") or {})
    con.execute(
        """
        INSERT INTO garch_vol_forecasts(
          symbol, ts_ms, asof_ts_ms, model_type, distribution, horizon_days,
          return_source, trailing_vol, forecast_rv_1d, forecast_vol_1d,
          forecast_ann_vol, forecast_ratio, n_obs, n_train, min_history,
          converged, convergence_status, loglikelihood, aic, bic,
          fallback, fallback_reason, diagnostics_json, created_ts_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, ts_ms, model_type) DO UPDATE SET
          asof_ts_ms=excluded.asof_ts_ms,
          distribution=excluded.distribution,
          horizon_days=excluded.horizon_days,
          return_source=excluded.return_source,
          trailing_vol=excluded.trailing_vol,
          forecast_rv_1d=excluded.forecast_rv_1d,
          forecast_vol_1d=excluded.forecast_vol_1d,
          forecast_ann_vol=excluded.forecast_ann_vol,
          forecast_ratio=excluded.forecast_ratio,
          n_obs=excluded.n_obs,
          n_train=excluded.n_train,
          min_history=excluded.min_history,
          converged=excluded.converged,
          convergence_status=excluded.convergence_status,
          loglikelihood=excluded.loglikelihood,
          aic=excluded.aic,
          bic=excluded.bic,
          fallback=excluded.fallback,
          fallback_reason=excluded.fallback_reason,
          diagnostics_json=excluded.diagnostics_json,
          created_ts_ms=excluded.created_ts_ms
        """,
        (
            str(payload.get("symbol") or "").upper().strip(),
            int(payload.get("ts_ms") or 0),
            payload.get("asof_ts_ms"),
            _normalize_model_type(payload.get("model_type")),
            _normalize_distribution(payload.get("distribution")),
            int(payload.get("horizon_days") or DEFAULT_HORIZON_DAYS),
            str(payload.get("return_source") or ""),
            float(payload.get("trailing_vol") or 0.0),
            float(payload.get("forecast_rv_1d") or 0.0),
            float(payload.get("forecast_vol_1d") or 0.0),
            float(payload.get("forecast_ann_vol") or 0.0),
            float(payload.get("forecast_ratio") or 0.0),
            int(payload.get("n_obs") or 0),
            int(payload.get("n_train") or 0),
            int(payload.get("min_history") or DEFAULT_MIN_HISTORY),
            1 if bool(payload.get("converged")) else 0,
            str(payload.get("convergence_status") or ""),
            payload.get("loglikelihood"),
            payload.get("aic"),
            payload.get("bic"),
            1 if bool(payload.get("fallback")) else 0,
            payload.get("fallback_reason"),
            json.dumps(diagnostics, separators=(",", ":"), sort_keys=True),
            int(payload.get("created_ts_ms") or _now_ms()),
        ),
    )


def latest_garch_forecast(
    con,
    symbol: str,
    *,
    ts_ms: int | None = None,
    max_age_days: int = DEFAULT_FORECAST_MAX_AGE_DAYS,
    model_type: str | None = None,
) -> dict[str, Any] | None:
    if not _table_readable(con, "garch_vol_forecasts"):
        return None
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return None
    model = _normalize_model_type(model_type or DEFAULT_MODEL_TYPE)
    params: list[Any] = [symbol_key, model]
    where = "symbol=? AND model_type=?"
    if ts_ms is not None and int(ts_ms) > 0:
        where += " AND ts_ms <= ?"
        params.append(int(ts_ms))
    try:
        row = con.execute(
            f"""
            SELECT symbol, ts_ms, asof_ts_ms, model_type, distribution,
                   horizon_days, return_source, trailing_vol,
                   forecast_rv_1d, forecast_vol_1d, forecast_ann_vol,
                   forecast_ratio, n_obs, n_train, min_history, converged,
                   convergence_status, loglikelihood, aic, bic,
                   fallback, fallback_reason, diagnostics_json, created_ts_ms
            FROM garch_vol_forecasts
            WHERE {where}
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
    except Exception:
        return None
    if not row:
        return None
    forecast_ts_ms = int(row[1] or 0)
    anchor_ts_ms = int(ts_ms if ts_ms is not None else _now_ms())
    if forecast_ts_ms > 0 and int(max_age_days) > 0:
        if anchor_ts_ms - forecast_ts_ms > int(max_age_days) * MS_PER_DAY:
            return None
    diagnostics: dict[str, Any] = {}
    try:
        diagnostics = dict(json.loads(row[22] or "{}"))
    except Exception:
        diagnostics = {}
    return {
        "symbol": str(row[0]),
        "ts_ms": int(row[1] or 0),
        "asof_ts_ms": None if row[2] is None else int(row[2]),
        "model_type": str(row[3] or ""),
        "distribution": str(row[4] or ""),
        "horizon_days": int(row[5] or 1),
        "return_source": str(row[6] or ""),
        "trailing_vol": float(row[7] or 0.0),
        "forecast_rv_1d": float(row[8] or 0.0),
        "forecast_vol_1d": float(row[9] or 0.0),
        "forecast_ann_vol": float(row[10] or 0.0),
        "forecast_ratio": float(row[11] or 0.0),
        "n_obs": int(row[12] or 0),
        "n_train": int(row[13] or 0),
        "min_history": int(row[14] or 0),
        "converged": bool(int(row[15] or 0)),
        "convergence_status": str(row[16] or ""),
        "loglikelihood": None if row[17] is None else float(row[17]),
        "aic": None if row[18] is None else float(row[18]),
        "bic": None if row[19] is None else float(row[19]),
        "fallback": bool(int(row[20] or 0)),
        "fallback_reason": None if row[21] is None else str(row[21]),
        "diagnostics": diagnostics,
        "created_ts_ms": int(row[23] or 0),
    }


def run_garch_vol_forecast_job(
    *,
    con=None,
    symbols: Iterable[str] | None = None,
    ts_ms: int | None = None,
    min_history: int = DEFAULT_MIN_HISTORY,
    history_days: int = DEFAULT_HISTORY_DAYS,
    model_type: str = DEFAULT_MODEL_TYPE,
    distribution: str = DEFAULT_DISTRIBUTION,
    use_arch: bool | None = None,
) -> dict[str, Any]:
    owns = False
    if con is None:
        from engine.runtime.storage import connect

        con = connect(readonly=False)
        owns = True
    now_ms = int(ts_ms if ts_ms is not None else _now_ms())
    try:
        ensure_garch_vol_schema(con)
        symbol_list = [str(s).upper().strip() for s in list(symbols or []) if str(s or "").strip()]
        if not symbol_list:
            symbol_list = _candidate_symbols(con, int(os.environ.get("GARCH_VOL_SYMBOL_LIMIT", "5000")))
        rows: list[dict[str, Any]] = []
        fallback = 0
        fitted = 0
        for symbol in symbol_list:
            payload = forecast_garch_for_symbol(
                con,
                symbol,
                ts_ms=int(now_ms),
                min_history=int(min_history),
                history_days=int(history_days),
                model_type=str(model_type),
                distribution=str(distribution),
                use_arch=use_arch,
            )
            upsert_garch_forecast(con, payload)
            rows.append(payload)
            if bool(payload.get("fallback")):
                fallback += 1
            else:
                fitted += 1
        if owns:
            con.commit()
        return {
            "ok": True,
            "job": "garch_vol_forecast",
            "ts_ms": int(now_ms),
            "symbols": int(len(symbol_list)),
            "forecasts": int(len(rows)),
            "fallback": int(fallback),
            "fitted": int(fitted),
            "model_type": _normalize_model_type(model_type),
            "distribution": _normalize_distribution(distribution),
            "use_arch": bool(_env_bool("GARCH_VOL_USE_ARCH", DEFAULT_USE_ARCH) if use_arch is None else use_arch),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("GARCH_VOL_FORECAST_JOB_CLOSE_FAILED", e, once_key="forecast_job_close")


def main() -> int:
    result = run_garch_vol_forecast_job()
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0 if bool(result.get("ok")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
