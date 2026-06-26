"""HAR-RV volatility forecasts for risk sizing.

This module forecasts next-day realized variance only. It is intentionally a
risk input: callers may use the forecast for volatility targeting, sizing, and
stress simulation, but not as a return-timing signal.
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

MS_PER_DAY = 86_400_000
FIVE_MIN_MS = 300_000
TRADING_DAYS_PER_YEAR = 252.0

DEFAULT_MIN_HISTORY = int(os.environ.get("HAR_RV_MIN_HISTORY", "60"))
DEFAULT_HISTORY_DAYS = int(os.environ.get("HAR_RV_HISTORY_DAYS", "756"))
DEFAULT_TRAILING_DAYS = int(os.environ.get("HAR_RV_TRAILING_DAYS", "20"))
DEFAULT_FORECAST_MAX_AGE_DAYS = int(os.environ.get("HAR_RV_FORECAST_MAX_AGE_DAYS", "7"))
DEFAULT_INTRADAY_MIN_RETURNS = int(os.environ.get("HAR_RV_MIN_INTRADAY_RETURNS", "3"))
LOG = get_logger("engine.strategy.har_rv")
_WARNED_NONFATAL_KEYS: set[str] = set()


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
        component="engine.strategy.har_rv",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


@dataclass(frozen=True)
class RVObservation:
    ts_ms: int
    rv: float
    source: str


@dataclass(frozen=True)
class HARFit:
    intercept: float
    beta_daily: float
    beta_weekly: float
    beta_monthly: float
    n_obs: int
    n_train: int

    @property
    def coefficients(self) -> tuple[float, float, float, float]:
        return (
            float(self.intercept),
            float(self.beta_daily),
            float(self.beta_weekly),
            float(self.beta_monthly),
        )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        out = float(value)
    except Exception:
        return default
    if not math.isfinite(out):
        return default
    return float(out)


def _day_start_ms(ts_ms: int) -> int:
    return int(int(ts_ms) // MS_PER_DAY) * MS_PER_DAY


def _table_readable(con, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _latest_bar_ts(con, symbol: str, end_ts_ms: int | None) -> int | None:
    if end_ts_ms is not None and int(end_ts_ms) > 0:
        return int(end_ts_ms)
    if not _table_readable(con, "price_bars"):
        return None
    try:
        row = con.execute(
            "SELECT MAX(ts_ms) FROM price_bars WHERE symbol=?",
            (str(symbol).upper().strip(),),
        ).fetchone()
    except Exception:
        return None
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def _intraday_rv_observations(
    con,
    symbol: str,
    *,
    end_ts_ms: int,
    max_days: int,
    min_returns_per_day: int,
) -> list[RVObservation]:
    start_ts_ms = int(end_ts_ms) - (int(max_days) + 3) * MS_PER_DAY
    try:
        rows = con.execute(
            """
            SELECT ts_ms, tf_s, c
            FROM price_bars
            WHERE symbol=?
              AND tf_s > 0
              AND tf_s <= 300
              AND ts_ms <= ?
              AND ts_ms >= ?
              AND c IS NOT NULL
            ORDER BY ts_ms ASC
            """,
            (str(symbol).upper().strip(), int(end_ts_ms), int(start_ts_ms)),
        ).fetchall()
    except Exception:
        rows = []
    if not rows:
        return []

    bucket_close: dict[int, float] = {}
    raw_tf_s: set[int] = set()
    for row in rows:
        try:
            ts_ms = int(row[0])
            tf_s = int(row[1] or 0)
            close = float(row[2])
        except Exception:
            continue
        if close <= 0.0 or not math.isfinite(close):
            continue
        raw_tf_s.add(int(tf_s))
        bucket_close[int(ts_ms // FIVE_MIN_MS) * FIVE_MIN_MS] = float(close)

    daily_rv: dict[int, float] = {}
    daily_n: dict[int, int] = {}
    prev_bucket: int | None = None
    prev_close: float | None = None
    prev_day: int | None = None
    for bucket_ts_ms in sorted(bucket_close):
        close = float(bucket_close[bucket_ts_ms])
        day = _day_start_ms(int(bucket_ts_ms))
        if (
            prev_bucket is not None
            and prev_close is not None
            and prev_day == day
            and close > 0.0
            and prev_close > 0.0
        ):
            ret = math.log(close / prev_close)
            if math.isfinite(ret):
                daily_rv[day] = float(daily_rv.get(day, 0.0) + ret * ret)
                daily_n[day] = int(daily_n.get(day, 0) + 1)
        prev_bucket = int(bucket_ts_ms)
        prev_close = float(close)
        prev_day = int(day)

    source = "intraday_5m" if raw_tf_s == {300} else "intraday_aggregated_5m"
    out = [
        RVObservation(ts_ms=int(day), rv=float(rv), source=source)
        for day, rv in sorted(daily_rv.items())
        if float(rv) > 0.0 and int(daily_n.get(day, 0)) >= int(min_returns_per_day)
    ]
    return out[-int(max_days):]


def _garman_klass_rv(open_px: float, high_px: float, low_px: float, close_px: float) -> float | None:
    if min(open_px, high_px, low_px, close_px) <= 0.0 or high_px < low_px:
        return None
    try:
        log_hl = math.log(float(high_px) / float(low_px))
        log_co = math.log(float(close_px) / float(open_px))
        rv = 0.5 * log_hl * log_hl - (2.0 * math.log(2.0) - 1.0) * log_co * log_co
        if not math.isfinite(rv) or rv <= 0.0:
            rv = (log_hl * log_hl) / (4.0 * math.log(2.0))
    except Exception:
        return None
    if not math.isfinite(rv) or rv <= 0.0:
        return None
    return float(rv)


def _daily_range_rv_observations(
    con,
    symbol: str,
    *,
    end_ts_ms: int,
    max_days: int,
) -> list[RVObservation]:
    try:
        rows = con.execute(
            """
            SELECT ts_ms, o, h, l, c
            FROM price_bars
            WHERE symbol=?
              AND tf_s >= 86400
              AND ts_ms <= ?
              AND o IS NOT NULL
              AND h IS NOT NULL
              AND l IS NOT NULL
              AND c IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol).upper().strip(), int(end_ts_ms), int(max_days)),
        ).fetchall()
    except Exception:
        rows = []
    out: list[RVObservation] = []
    for row in reversed(rows or []):
        try:
            ts_ms = int(row[0])
            rv = _garman_klass_rv(float(row[1]), float(row[2]), float(row[3]), float(row[4]))
        except Exception:
            rv = None
        if rv is None:
            continue
        out.append(RVObservation(ts_ms=int(ts_ms), rv=float(rv), source="garman_klass_daily_ohlc"))
    return out[-int(max_days):]


def realized_variance_observations(
    con,
    symbol: str,
    *,
    end_ts_ms: int | None = None,
    max_days: int = DEFAULT_HISTORY_DAYS,
) -> list[RVObservation]:
    """Return source-aware daily realized variance observations.

    If 5-minute or finer intraday bars are available in ``price_bars``, the
    daily measure is ``sum(log(c_t / c_{t-1}) ** 2)`` over 5-minute buckets
    inside each UTC day. If no usable intraday bars exist, daily OHLC bars are
    converted with the Garman-Klass range estimator. The function only reads
    bars with ``ts_ms <= end_ts_ms`` so callers can use it in walk-forward tests
    and point-in-time feature generation.
    """

    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key or not _table_readable(con, "price_bars"):
        return []
    anchor_ts_ms = _latest_bar_ts(con, symbol_key, end_ts_ms)
    if anchor_ts_ms is None:
        return []
    intraday = _intraday_rv_observations(
        con,
        symbol_key,
        end_ts_ms=int(anchor_ts_ms),
        max_days=int(max_days),
        min_returns_per_day=int(DEFAULT_INTRADAY_MIN_RETURNS),
    )
    if intraday:
        return intraday
    return _daily_range_rv_observations(
        con,
        symbol_key,
        end_ts_ms=int(anchor_ts_ms),
        max_days=int(max_days),
    )


def fit_har_coefficients(rv_values: Sequence[float], *, min_history: int = DEFAULT_MIN_HISTORY) -> HARFit | None:
    values = np.asarray([float(v) for v in rv_values if _safe_float(v, None) is not None and float(v) > 0.0], dtype=float)
    if values.size < int(min_history):
        return None
    x_rows: list[list[float]] = []
    y_rows: list[float] = []
    for idx in range(21, int(values.size) - 1):
        x_rows.append(
            [
                1.0,
                float(values[idx]),
                float(np.mean(values[idx - 4: idx + 1])),
                float(np.mean(values[idx - 21: idx + 1])),
            ]
        )
        y_rows.append(float(values[idx + 1]))
    if len(y_rows) < 4:
        return None
    x = np.asarray(x_rows, dtype=float)
    y = np.asarray(y_rows, dtype=float)
    try:
        beta, *_ = np.linalg.lstsq(x, y, rcond=None)
    except Exception:
        return None
    if beta.size != 4 or not np.all(np.isfinite(beta)):
        return None
    return HARFit(
        intercept=float(beta[0]),
        beta_daily=float(beta[1]),
        beta_weekly=float(beta[2]),
        beta_monthly=float(beta[3]),
        n_obs=int(values.size),
        n_train=int(len(y_rows)),
    )


def predict_next_rv(rv_values: Sequence[float], fit: HARFit) -> float | None:
    values = np.asarray([float(v) for v in rv_values if _safe_float(v, None) is not None and float(v) > 0.0], dtype=float)
    if values.size < 22:
        return None
    b0, b1, b2, b3 = fit.coefficients
    forecast = (
        float(b0)
        + float(b1) * float(values[-1])
        + float(b2) * float(np.mean(values[-5:]))
        + float(b3) * float(np.mean(values[-22:]))
    )
    if not math.isfinite(forecast) or forecast <= 0.0:
        return None
    return float(forecast)


def trailing_vol_from_rv(rv_values: Sequence[float], *, lookback: int = DEFAULT_TRAILING_DAYS) -> float | None:
    values = [float(v) for v in rv_values if _safe_float(v, None) is not None and float(v) > 0.0]
    if not values:
        return None
    window = values[-int(max(1, lookback)):]
    rv = float(sum(window) / float(len(window)))
    if not math.isfinite(rv) or rv <= 0.0:
        return None
    return float(math.sqrt(rv))


def ensure_har_rv_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS har_rv_forecasts (
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            asof_ts_ms BIGINT,
            rv DOUBLE PRECISION,
            trailing_vol DOUBLE PRECISION,
            forecast_rv_1d DOUBLE PRECISION NOT NULL,
            forecast_vol_1d DOUBLE PRECISION NOT NULL,
            forecast_ann_vol DOUBLE PRECISION NOT NULL,
            forecast_ratio DOUBLE PRECISION NOT NULL,
            intercept DOUBLE PRECISION,
            beta_daily DOUBLE PRECISION,
            beta_weekly DOUBLE PRECISION,
            beta_monthly DOUBLE PRECISION,
            n_obs BIGINT NOT NULL DEFAULT 0,
            n_train BIGINT NOT NULL DEFAULT 0,
            min_history BIGINT NOT NULL DEFAULT 60,
            source TEXT NOT NULL,
            fallback BIGINT NOT NULL DEFAULT 0,
            diagnostics_json TEXT,
            created_ts_ms BIGINT NOT NULL,
            PRIMARY KEY(symbol, ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_har_rv_forecasts_symbol_ts_desc
          ON har_rv_forecasts(symbol, ts_ms DESC)
        """
    )


def _trailing_fallback_vol(con, symbol: str, *, lookback: int = 240) -> float | None:
    try:
        from engine.strategy.risk import realized_vol_from_prices

        value = realized_vol_from_prices(con, str(symbol), lookback=int(lookback))
    except Exception:
        value = None
    if value is None:
        return None
    value_f = _safe_float(value, None)
    if value_f is None or value_f <= 0.0:
        return None
    return float(value_f)


def _forecast_payload(
    *,
    symbol: str,
    ts_ms: int,
    observations: Sequence[RVObservation],
    forecast_rv: float,
    trailing_vol: float,
    source: str,
    fallback: bool,
    fit: HARFit | None,
    min_history: int,
    diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    forecast_rv = max(1.0e-12, float(forecast_rv))
    forecast_vol = math.sqrt(forecast_rv)
    ann_vol = forecast_vol * math.sqrt(TRADING_DAYS_PER_YEAR)
    ratio = forecast_vol / max(1.0e-12, float(trailing_vol))
    latest = observations[-1] if observations else None
    return {
        "symbol": str(symbol).upper().strip(),
        "ts_ms": int(ts_ms),
        "asof_ts_ms": int(latest.ts_ms) if latest else None,
        "rv": float(latest.rv) if latest else None,
        "trailing_vol": float(trailing_vol),
        "forecast_rv_1d": float(forecast_rv),
        "forecast_vol_1d": float(forecast_vol),
        "forecast_ann_vol": float(ann_vol),
        "forecast_ratio": float(ratio),
        "intercept": None if fit is None else float(fit.intercept),
        "beta_daily": None if fit is None else float(fit.beta_daily),
        "beta_weekly": None if fit is None else float(fit.beta_weekly),
        "beta_monthly": None if fit is None else float(fit.beta_monthly),
        "n_obs": int(len(observations)),
        "n_train": 0 if fit is None else int(fit.n_train),
        "min_history": int(min_history),
        "source": str(source),
        "fallback": bool(fallback),
        "diagnostics": dict(diagnostics or {}),
        "created_ts_ms": _now_ms(),
    }


def forecast_har_for_symbol(
    con,
    symbol: str,
    *,
    ts_ms: int | None = None,
    min_history: int = DEFAULT_MIN_HISTORY,
    history_days: int = DEFAULT_HISTORY_DAYS,
    trailing_price_lookback: int = 240,
) -> dict[str, Any]:
    symbol_key = str(symbol or "").upper().strip()
    now_ms = int(ts_ms if ts_ms is not None else _now_ms())
    observations = realized_variance_observations(
        con,
        symbol_key,
        end_ts_ms=now_ms,
        max_days=int(history_days),
    )
    rv_values = [float(obs.rv) for obs in observations]
    trailing_vol = trailing_vol_from_rv(rv_values) if rv_values else None
    if trailing_vol is None:
        trailing_vol = _trailing_fallback_vol(con, symbol_key, lookback=int(trailing_price_lookback))

    fit = fit_har_coefficients(rv_values, min_history=int(min_history)) if len(rv_values) >= int(min_history) else None
    forecast_rv = predict_next_rv(rv_values, fit) if fit is not None else None
    if fit is not None and forecast_rv is not None and trailing_vol is not None:
        return _forecast_payload(
            symbol=symbol_key,
            ts_ms=int(now_ms),
            observations=observations,
            forecast_rv=float(forecast_rv),
            trailing_vol=float(trailing_vol),
            source=str(observations[-1].source if observations else "har_rv"),
            fallback=False,
            fit=fit,
            min_history=int(min_history),
            diagnostics={"model": "har_rv_ols"},
        )

    if trailing_vol is None:
        trailing_vol = 0.0
    return _forecast_payload(
        symbol=symbol_key,
        ts_ms=int(now_ms),
        observations=observations,
        forecast_rv=max(1.0e-12, float(trailing_vol) * float(trailing_vol)),
        trailing_vol=max(1.0e-6, float(trailing_vol)),
        source="trailing_fallback",
        fallback=True,
        fit=None,
        min_history=int(min_history),
        diagnostics={
            "reason": "insufficient_har_history" if len(rv_values) < int(min_history) else "har_fit_unavailable",
            "rv_observations": int(len(rv_values)),
        },
    )


def upsert_har_forecast(con, payload: dict[str, Any]) -> None:
    ensure_har_rv_schema(con)
    con.execute(
        """
        INSERT INTO har_rv_forecasts(
          symbol, ts_ms, asof_ts_ms, rv, trailing_vol,
          forecast_rv_1d, forecast_vol_1d, forecast_ann_vol, forecast_ratio,
          intercept, beta_daily, beta_weekly, beta_monthly,
          n_obs, n_train, min_history, source, fallback, diagnostics_json, created_ts_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, ts_ms) DO UPDATE SET
          asof_ts_ms=excluded.asof_ts_ms,
          rv=excluded.rv,
          trailing_vol=excluded.trailing_vol,
          forecast_rv_1d=excluded.forecast_rv_1d,
          forecast_vol_1d=excluded.forecast_vol_1d,
          forecast_ann_vol=excluded.forecast_ann_vol,
          forecast_ratio=excluded.forecast_ratio,
          intercept=excluded.intercept,
          beta_daily=excluded.beta_daily,
          beta_weekly=excluded.beta_weekly,
          beta_monthly=excluded.beta_monthly,
          n_obs=excluded.n_obs,
          n_train=excluded.n_train,
          min_history=excluded.min_history,
          source=excluded.source,
          fallback=excluded.fallback,
          diagnostics_json=excluded.diagnostics_json,
          created_ts_ms=excluded.created_ts_ms
        """,
        (
            str(payload.get("symbol") or "").upper().strip(),
            int(payload.get("ts_ms") or 0),
            payload.get("asof_ts_ms"),
            payload.get("rv"),
            float(payload.get("trailing_vol") or 0.0),
            float(payload.get("forecast_rv_1d") or 0.0),
            float(payload.get("forecast_vol_1d") or 0.0),
            float(payload.get("forecast_ann_vol") or 0.0),
            float(payload.get("forecast_ratio") or 0.0),
            payload.get("intercept"),
            payload.get("beta_daily"),
            payload.get("beta_weekly"),
            payload.get("beta_monthly"),
            int(payload.get("n_obs") or 0),
            int(payload.get("n_train") or 0),
            int(payload.get("min_history") or DEFAULT_MIN_HISTORY),
            str(payload.get("source") or ""),
            1 if bool(payload.get("fallback")) else 0,
            json.dumps(dict(payload.get("diagnostics") or {}), separators=(",", ":"), sort_keys=True),
            int(payload.get("created_ts_ms") or _now_ms()),
        ),
    )


def latest_har_forecast(
    con,
    symbol: str,
    *,
    ts_ms: int | None = None,
    max_age_days: int = DEFAULT_FORECAST_MAX_AGE_DAYS,
) -> dict[str, Any] | None:
    if not _table_readable(con, "har_rv_forecasts"):
        return None
    symbol_key = str(symbol or "").upper().strip()
    if not symbol_key:
        return None
    params: list[Any] = [symbol_key]
    where = "symbol=?"
    if ts_ms is not None and int(ts_ms) > 0:
        where += " AND ts_ms <= ?"
        params.append(int(ts_ms))
    try:
        row = con.execute(
            f"""
            SELECT symbol, ts_ms, asof_ts_ms, rv, trailing_vol,
                   forecast_rv_1d, forecast_vol_1d, forecast_ann_vol,
                   forecast_ratio, source, fallback, n_obs, n_train,
                   intercept, beta_daily, beta_weekly, beta_monthly
            FROM har_rv_forecasts
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
        max_age_ms = int(max_age_days) * MS_PER_DAY
        if anchor_ts_ms - forecast_ts_ms > max_age_ms:
            return None
    return {
        "symbol": str(row[0]),
        "ts_ms": int(row[1] or 0),
        "asof_ts_ms": None if row[2] is None else int(row[2]),
        "rv": None if row[3] is None else float(row[3]),
        "trailing_vol": float(row[4] or 0.0),
        "forecast_rv_1d": float(row[5] or 0.0),
        "forecast_vol_1d": float(row[6] or 0.0),
        "forecast_ann_vol": float(row[7] or 0.0),
        "forecast_ratio": float(row[8] or 0.0),
        "source": str(row[9] or ""),
        "fallback": bool(int(row[10] or 0)),
        "n_obs": int(row[11] or 0),
        "n_train": int(row[12] or 0),
        "intercept": None if row[13] is None else float(row[13]),
        "beta_daily": None if row[14] is None else float(row[14]),
        "beta_weekly": None if row[15] is None else float(row[15]),
        "beta_monthly": None if row[16] is None else float(row[16]),
    }


def _normalize_vol_source(source: Any) -> str:
    source_key = str(source if source is not None else os.environ.get("VOL_FORECAST_SOURCE", "trailing")).strip().lower().replace("-", "_")
    aliases = {
        "": "trailing",
        "realized": "trailing",
        "realized_vol": "trailing",
        "har": "har",
        "har_rv": "har",
        "harrv": "har",
        "garch": "garch",
        "garch_1_1": "garch",
        "garch11": "garch",
        "egarch": "egarch",
        "gjr": "gjr_garch",
        "gjr_garch": "gjr_garch",
        "blend": "blend",
    }
    return aliases.get(source_key, source_key)


def _parse_blend_weights(raw: Any | None) -> tuple[dict[str, float], str]:
    if isinstance(raw, Mapping):
        weights = {str(k).strip().lower().replace("-", "_"): float(v) for k, v in raw.items()}
        return weights, "argument"
    text = str(raw if raw is not None else os.environ.get("VOL_FORECAST_BLEND_WEIGHTS", "")).strip()
    if not text:
        return {"trailing": 0.34, "har": 0.33, "garch": 0.33}, "default"
    try:
        parsed = json.loads(text)
        if isinstance(parsed, Mapping):
            return {str(k).strip().lower().replace("-", "_"): float(v) for k, v in parsed.items()}, "env_json"
    except Exception:
        pass  # no-op-guard: allow - fall back to comma-separated blend weights
    weights: dict[str, float] = {}
    for part in text.split(","):
        item = part.strip()
        if not item:
            continue
        if "=" in item:
            key, value = item.split("=", 1)
        elif ":" in item:
            key, value = item.split(":", 1)
        else:
            continue
        try:
            weights[str(key).strip().lower().replace("-", "_")] = float(value)
        except Exception:
            continue
    return weights, "env"


def _trailing_resolved(symbol: str, *, ts_ms: int | None, trailing_lookback: int, reason: str | None = None) -> dict[str, Any]:
    return {
        "symbol": str(symbol).upper().strip(),
        "ts_ms": int(ts_ms or 0),
        "vol": None,
        "forecast_vol_1d": None,
        "forecast_rv_1d": None,
        "forecast_ratio": 1.0,
        "source": "trailing",
        "resolved_source": "trailing",
        "fallback": True,
        "diagnostics": ({"fallback_reason": str(reason)} if reason else {}),
    }


def _resolve_trailing_vol(con, symbol: str, *, ts_ms: int | None, trailing_lookback: int, reason: str | None = None) -> dict[str, Any]:
    vol = _trailing_fallback_vol(con, str(symbol), lookback=int(trailing_lookback))
    out = _trailing_resolved(symbol, ts_ms=ts_ms, trailing_lookback=int(trailing_lookback), reason=reason)
    out["vol"] = None if vol is None else float(vol)
    out["forecast_vol_1d"] = None if vol is None else float(vol)
    out["forecast_rv_1d"] = None if vol is None else float(vol) * float(vol)
    return out


def _resolve_har_vol(con, symbol: str, *, ts_ms: int | None) -> dict[str, Any] | None:
    row = latest_har_forecast(con, symbol, ts_ms=ts_ms)
    if row and float(row.get("forecast_vol_1d") or 0.0) > 0.0:
        out = dict(row)
        out["vol"] = float(row["forecast_vol_1d"])
        out["source"] = "har_rv"
        out["resolved_source"] = "har" if not bool(row.get("fallback")) else "trailing_fallback"
        return out
    return None


def _resolve_garch_vol(con, symbol: str, *, ts_ms: int | None, model_type: str = "garch") -> dict[str, Any] | None:
    try:
        from engine.strategy.garch_vol import latest_garch_forecast

        row = latest_garch_forecast(con, symbol, ts_ms=ts_ms, model_type=str(model_type))
    except Exception:
        return None
    if row and float(row.get("forecast_vol_1d") or 0.0) > 0.0:
        out = dict(row)
        out["vol"] = float(row["forecast_vol_1d"])
        out["source"] = "garch"
        out["resolved_source"] = str(row.get("model_type") or model_type)
        return out
    return None


def _resolve_blend_vol(
    con,
    symbol: str,
    *,
    ts_ms: int | None,
    trailing_lookback: int,
    blend_weights: Any | None,
) -> dict[str, Any]:
    weights, weights_source = _parse_blend_weights(blend_weights)
    components: dict[str, dict[str, Any]] = {}
    skipped: dict[str, str] = {}
    weighted_rv = 0.0
    total_weight = 0.0
    trailing_vol: float | None = None
    for raw_source, raw_weight in weights.items():
        source_key = _normalize_vol_source(raw_source)
        if source_key == "blend":
            skipped[str(raw_source)] = "recursive_blend_component"
            continue
        try:
            weight = float(raw_weight)
        except Exception:
            skipped[str(raw_source)] = "invalid_weight"
            continue
        if not math.isfinite(weight) or weight <= 0.0:
            skipped[str(raw_source)] = "non_positive_weight"
            continue
        resolved = resolve_vol_forecast(
            con,
            symbol,
            ts_ms=ts_ms,
            source=source_key,
            trailing_lookback=int(trailing_lookback),
        )
        vol = _safe_float(resolved.get("forecast_vol_1d") if resolved.get("forecast_vol_1d") is not None else resolved.get("vol"), None)
        rv = _safe_float(resolved.get("forecast_rv_1d"), None)
        if rv is None and vol is not None:
            rv = float(vol) * float(vol)
        if rv is None or rv <= 0.0:
            skipped[str(raw_source)] = "missing_forecast"
            continue
        if source_key == "trailing" and vol is not None:
            trailing_vol = float(vol)
        weighted_rv += float(weight) * float(rv)
        total_weight += float(weight)
        components[source_key] = {
            "input_weight": float(weight),
            "forecast_rv_1d": float(rv),
            "forecast_vol_1d": (None if vol is None else float(vol)),
            "resolved_source": str(resolved.get("resolved_source") or resolved.get("source") or source_key),
            "forecast_ts_ms": resolved.get("ts_ms"),
            "fallback": bool(resolved.get("fallback", False)),
        }
    if total_weight <= 0.0:
        out = _resolve_trailing_vol(
            con,
            symbol,
            ts_ms=ts_ms,
            trailing_lookback=int(trailing_lookback),
            reason="blend_components_unavailable",
        )
        out["diagnostics"] = {
            "fallback_reason": "blend_components_unavailable",
            "blend_weights": dict(weights),
            "weights_source": str(weights_source),
            "skipped": dict(skipped),
        }
        return out
    forecast_rv = max(1.0e-12, float(weighted_rv / total_weight))
    forecast_vol = math.sqrt(float(forecast_rv))
    if trailing_vol is None:
        trailing_vol = _trailing_fallback_vol(con, str(symbol), lookback=int(trailing_lookback))
    ratio_base = max(1.0e-12, float(trailing_vol or forecast_vol))
    normalized_weights = {
        str(source): float(component["input_weight"]) / float(total_weight)
        for source, component in components.items()
    }
    return {
        "symbol": str(symbol).upper().strip(),
        "ts_ms": int(ts_ms or 0),
        "vol": float(forecast_vol),
        "forecast_vol_1d": float(forecast_vol),
        "forecast_rv_1d": float(forecast_rv),
        "forecast_ann_vol": float(forecast_vol * math.sqrt(TRADING_DAYS_PER_YEAR)),
        "forecast_ratio": float(forecast_vol / ratio_base),
        "source": "blend",
        "resolved_source": "blend",
        "fallback": False,
        "diagnostics": {
            "blend_space": "variance",
            "blend_weights": dict(weights),
            "normalized_weights": normalized_weights,
            "weights_source": str(weights_source),
            "components": components,
            "skipped": skipped,
        },
    }


def resolve_vol_forecast(
    con,
    symbol: str,
    *,
    ts_ms: int | None = None,
    source: str | None = None,
    trailing_lookback: int = 240,
    blend_weights: Any | None = None,
) -> dict[str, Any]:
    source_key = _normalize_vol_source(source)
    if source_key == "har":
        row = _resolve_har_vol(con, symbol, ts_ms=ts_ms)
        if row:
            return row
        return _resolve_trailing_vol(con, symbol, ts_ms=ts_ms, trailing_lookback=int(trailing_lookback), reason="har_forecast_unavailable")
    if source_key in {"garch", "egarch", "gjr_garch"}:
        row = _resolve_garch_vol(con, symbol, ts_ms=ts_ms, model_type=source_key)
        if row:
            return row
        return _resolve_trailing_vol(con, symbol, ts_ms=ts_ms, trailing_lookback=int(trailing_lookback), reason="garch_forecast_unavailable")
    if source_key == "blend":
        return _resolve_blend_vol(
            con,
            symbol,
            ts_ms=ts_ms,
            trailing_lookback=int(trailing_lookback),
            blend_weights=blend_weights,
        )
    return _resolve_trailing_vol(con, symbol, ts_ms=ts_ms, trailing_lookback=int(trailing_lookback))


def har_feature_values(con, symbol: str, ts_ms: int) -> dict[str, float]:
    row = latest_har_forecast(con, symbol, ts_ms=int(ts_ms))
    if not row:
        return {
            "tech.har_rv_forecast_1d": 0.0,
            "tech.har_rv_forecast_ratio": 0.0,
        }
    return {
        "tech.har_rv_forecast_1d": float(row.get("forecast_vol_1d") or 0.0),
        "tech.har_rv_forecast_ratio": float(row.get("forecast_ratio") or 0.0),
    }


def _candidate_symbols(con, limit: int) -> list[str]:
    symbols: list[str] = []
    if _table_readable(con, "price_bars"):
        try:
            rows = con.execute(
                "SELECT DISTINCT symbol FROM price_bars WHERE symbol IS NOT NULL ORDER BY symbol LIMIT ?",
                (int(limit),),
            ).fetchall()
            symbols.extend(str(row[0]).upper().strip() for row in rows or [] if row and str(row[0]).strip())
        except Exception as e:
            _warn_nonfatal(
                "HAR_RV_CANDIDATE_SYMBOLS_QUERY_FAILED",
                e,
                once_key="candidate_symbols:price_bars",
                table="price_bars",
                limit=int(limit),
            )
    if not symbols and _table_readable(con, "prices"):
        try:
            rows = con.execute(
                "SELECT DISTINCT symbol FROM prices WHERE symbol IS NOT NULL ORDER BY symbol LIMIT ?",
                (int(limit),),
            ).fetchall()
            symbols.extend(str(row[0]).upper().strip() for row in rows or [] if row and str(row[0]).strip())
        except Exception as e:
            _warn_nonfatal(
                "HAR_RV_CANDIDATE_SYMBOLS_QUERY_FAILED",
                e,
                once_key="candidate_symbols:prices",
                table="prices",
                limit=int(limit),
            )
    return list(dict.fromkeys(symbols))


def run_har_rv_forecast_job(
    *,
    con=None,
    symbols: Iterable[str] | None = None,
    ts_ms: int | None = None,
    min_history: int = DEFAULT_MIN_HISTORY,
    history_days: int = DEFAULT_HISTORY_DAYS,
) -> dict[str, Any]:
    owns = False
    if con is None:
        from engine.runtime.storage import connect

        con = connect(readonly=False)
        owns = True
    now_ms = int(ts_ms if ts_ms is not None else _now_ms())
    try:
        ensure_har_rv_schema(con)
        symbol_list = [str(s).upper().strip() for s in list(symbols or []) if str(s or "").strip()]
        if not symbol_list:
            symbol_list = _candidate_symbols(con, int(os.environ.get("HAR_RV_SYMBOL_LIMIT", "5000")))
        rows = []
        fallback = 0
        for symbol in symbol_list:
            payload = forecast_har_for_symbol(
                con,
                symbol,
                ts_ms=int(now_ms),
                min_history=int(min_history),
                history_days=int(history_days),
            )
            upsert_har_forecast(con, payload)
            rows.append(payload)
            fallback += 1 if bool(payload.get("fallback")) else 0
        if owns:
            con.commit()
        return {
            "ok": True,
            "job": "har_rv_forecast",
            "ts_ms": int(now_ms),
            "symbols": int(len(symbol_list)),
            "forecasts": int(len(rows)),
            "fallback": int(fallback),
            "har": int(len(rows) - fallback),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("HAR_RV_FORECAST_JOB_CLOSE_FAILED", e, once_key="forecast_job_close")


def _qlike(actual_rv: float, forecast_rv: float) -> float:
    a = max(1.0e-12, float(actual_rv))
    f = max(1.0e-12, float(forecast_rv))
    ratio = a / f
    return float(ratio - math.log(ratio) - 1.0)


def walk_forward_validation(
    con,
    *,
    symbols: Iterable[str] | None = None,
    min_history: int = DEFAULT_MIN_HISTORY,
    holdout: int = 60,
    history_days: int = DEFAULT_HISTORY_DAYS,
) -> dict[str, Any]:
    symbol_list = [str(s).upper().strip() for s in list(symbols or []) if str(s or "").strip()]
    if not symbol_list:
        symbol_list = _candidate_symbols(con, int(os.environ.get("HAR_RV_VALIDATION_SYMBOL_LIMIT", "200")))

    rows: list[dict[str, Any]] = []
    for symbol in symbol_list:
        obs = realized_variance_observations(con, symbol, max_days=int(history_days))
        rv = [float(item.rv) for item in obs]
        if len(rv) < int(min_history) + 5:
            continue
        start_idx = max(int(min_history), len(rv) - int(holdout) - 1)
        har_log_errors: list[float] = []
        base_log_errors: list[float] = []
        har_qlike: list[float] = []
        base_qlike: list[float] = []
        for idx in range(start_idx, len(rv) - 1):
            train = rv[: idx + 1]
            actual = float(rv[idx + 1])
            fit = fit_har_coefficients(train, min_history=int(min_history))
            har_pred = predict_next_rv(train, fit) if fit is not None else None
            base_vol = trailing_vol_from_rv(train, lookback=int(DEFAULT_TRAILING_DAYS))
            if har_pred is None or base_vol is None:
                continue
            base_pred = float(base_vol) * float(base_vol)
            har_log_errors.append((math.log(max(1.0e-12, har_pred)) - math.log(max(1.0e-12, actual))) ** 2)
            base_log_errors.append((math.log(max(1.0e-12, base_pred)) - math.log(max(1.0e-12, actual))) ** 2)
            har_qlike.append(_qlike(actual, har_pred))
            base_qlike.append(_qlike(actual, base_pred))
        if not har_log_errors:
            continue
        har_mse = float(sum(har_log_errors) / len(har_log_errors))
        base_mse = float(sum(base_log_errors) / len(base_log_errors))
        har_q = float(sum(har_qlike) / len(har_qlike))
        base_q = float(sum(base_qlike) / len(base_qlike))
        rows.append(
            {
                "symbol": symbol,
                "n_obs": int(len(rv)),
                "n_eval": int(len(har_log_errors)),
                "source": str(obs[-1].source if obs else ""),
                "har_log_mse": har_mse,
                "trailing_log_mse": base_mse,
                "har_qlike": har_q,
                "trailing_qlike": base_q,
                "har_wins_log_mse": bool(har_mse <= base_mse),
                "har_wins_qlike": bool(har_q <= base_q),
            }
        )
    majority_log = sum(1 for row in rows if row["har_wins_log_mse"])
    majority_qlike = sum(1 for row in rows if row["har_wins_qlike"])
    return {
        "symbols_tested": int(len(rows)),
        "har_wins_log_mse": int(majority_log),
        "har_wins_qlike": int(majority_qlike),
        "majority_log_mse": bool(rows and majority_log > len(rows) / 2.0),
        "majority_qlike": bool(rows and majority_qlike > len(rows) / 2.0),
        "rows": rows,
    }


def main() -> int:
    result = run_har_rv_forecast_job()
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0 if bool(result.get("ok")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
