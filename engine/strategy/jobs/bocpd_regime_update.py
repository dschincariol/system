"""Daily BOCPD update for slow-moving regime-risk series."""

from __future__ import annotations

import logging
import math
import os
import time
from typing import Any

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.strategy.bocpd import latest_summary, persist_summary


JOB_NAME = "bocpd_regime_update"
LOG = get_logger("engine.strategy.jobs.bocpd_regime_update")
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
        component="engine.strategy.jobs.bocpd_regime_update",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _symbols() -> list[str]:
    raw = str(os.environ.get("BOCPD_SYMBOLS", "SPY,QQQ,IWM,BTCUSD,ETHUSD") or "")
    out = []
    seen = set()
    for part in raw.split(","):
        sym = str(part or "").upper().strip()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
    return out or ["SPY"]


def _price_points(con, symbol: str, limit: int) -> list[tuple[int, float]]:
    try:
        rows = con.execute(
            """
            SELECT ts_ms, px
            FROM prices
            WHERE symbol=? AND px IS NOT NULL AND px > 0
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol).upper().strip(), int(limit)),
        ).fetchall()
    except Exception:
        return []
    points = [(int(row[0] or 0), float(row[1] or 0.0)) for row in rows or [] if row and row[0] and row[1]]
    return sorted(points, key=lambda item: int(item[0]))


def _realized_vol_series(con, symbol: str, limit: int) -> tuple[list[int], list[float], str]:
    try:
        rows = con.execute(
            """
            SELECT ts_ms, COALESCE(rv, forecast_rv_1d)
            FROM har_rv_forecasts
            WHERE symbol=? AND COALESCE(rv, forecast_rv_1d) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol).upper().strip(), int(limit)),
        ).fetchall()
        ordered = sorted(
            [(int(row[0] or 0), max(0.0, float(row[1] or 0.0))) for row in rows or []],
            key=lambda item: int(item[0]),
        )
        if len(ordered) >= 20:
            return [ts for ts, _ in ordered], [val for _, val in ordered], "har_rv_forecasts"
    except Exception as e:
        _warn_nonfatal(
            "BOCPD_REGIME_HAR_RV_SERIES_QUERY_FAILED",
            e,
            once_key=f"har_rv_series:{symbol}",
            symbol=str(symbol).upper().strip(),
            limit=int(limit),
        )
    pts = _price_points(con, symbol, max(limit + 1, 260))
    ts: list[int] = []
    vals: list[float] = []
    for prev, cur in zip(pts, pts[1:]):
        if prev[1] <= 0.0 or cur[1] <= 0.0:
            continue
        ret = math.log(float(cur[1]) / float(prev[1]))
        ts.append(int(cur[0]))
        vals.append(float(ret * ret))
    return ts[-limit:], vals[-limit:], "prices_squared_log_returns"


def _returns_by_symbol(con, symbols: list[str], limit: int) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for sym in symbols:
        pts = _price_points(con, sym, max(limit + 1, 260))
        vals: list[float] = []
        for prev, cur in zip(pts, pts[1:]):
            if prev[1] > 0.0 and cur[1] > 0.0:
                vals.append(float(math.log(float(cur[1]) / float(prev[1]))))
        if len(vals) >= 30:
            out[str(sym)] = vals[-limit:]
    return out


def _mean_corr_series(con, symbols: list[str], limit: int, rolling: int = 20) -> tuple[list[int], list[float], str]:
    returns = _returns_by_symbol(con, symbols, max(limit + rolling, 260))
    usable = [vals for vals in returns.values() if len(vals) >= rolling]
    if len(usable) < 2:
        return [], [], "insufficient_prices"
    n = min(len(vals) for vals in usable)
    matrix = np.asarray([vals[-n:] for vals in usable], dtype=np.float64)
    vals: list[float] = []
    ts = list(range(max(0, n - limit), n))
    for idx in ts:
        start = max(0, idx - rolling + 1)
        window = matrix[:, start : idx + 1]
        if window.shape[1] < 3:
            continue
        corr = np.corrcoef(window)
        upper = corr[np.triu_indices_from(corr, k=1)]
        finite = upper[np.isfinite(upper)]
        vals.append(float(np.mean(finite)) if finite.size else 0.0)
    now = int(time.time() * 1000)
    synthetic_ts = [now - (len(vals) - idx) * 86_400_000 for idx in range(len(vals))]
    return synthetic_ts, vals[-limit:], "prices_mean_pairwise_corr"


def _funding_series(con, limit: int) -> tuple[list[int], list[float], str]:
    try:
        rows = con.execute(
            """
            SELECT funding_ts_ms, AVG(funding_rate)
            FROM crypto_funding_rates
            WHERE funding_rate IS NOT NULL
            GROUP BY funding_ts_ms
            ORDER BY funding_ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    except Exception:
        return [], [], "missing_crypto_funding_rates"
    ordered = sorted(
        [(int(row[0] or 0), float(row[1] or 0.0)) for row in rows or [] if row and row[0] is not None],
        key=lambda item: int(item[0]),
    )
    return [ts for ts, _ in ordered], [val for _, val in ordered], "crypto_funding_rates"


def run_update(*, con=None, symbols: list[str] | None = None, now_ms: int | None = None, limit: int | None = None) -> dict[str, Any]:
    owns = con is None
    if owns:
        init_db()
    con = connect() if con is None else con
    ts_value = int(now_ms if now_ms is not None else time.time() * 1000)
    series_limit = max(60, _safe_int(limit, int(os.environ.get("BOCPD_SERIES_LIMIT", "500"))))
    expected_run = max(1.0, _safe_float(os.environ.get("BOCPD_EXPECTED_RUN"), 60.0))
    try:
        updated: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        syms = list(symbols or _symbols())
        for sym in syms:
            series_ts, values, source = _realized_vol_series(con, sym, series_limit)
            if len(values) < 20:
                skipped.append({"series_key": f"realized_vol:{sym}", "reason": "insufficient_series", "source": source})
                continue
            summary = latest_summary(values, series_key=f"realized_vol:{sym}", ts_ms=(series_ts[-1] if series_ts else ts_value), expected_run=expected_run)
            persist_summary(con, summary, series_type="realized_vol", symbol=str(sym))
            updated.append({**summary, "series_type": "realized_vol", "symbol": str(sym), "source": str(source)})

        corr_ts, corr_values, corr_source = _mean_corr_series(con, syms, series_limit)
        if len(corr_values) >= 20:
            summary = latest_summary(corr_values, series_key="portfolio_mean_correlation", ts_ms=(corr_ts[-1] if corr_ts else ts_value), expected_run=expected_run)
            persist_summary(con, summary, series_type="portfolio_correlation", symbol="*")
            updated.append({**summary, "series_type": "portfolio_correlation", "symbol": "*", "source": str(corr_source)})
        else:
            skipped.append({"series_key": "portfolio_mean_correlation", "reason": "insufficient_series", "source": corr_source})

        funding_ts, funding_values, funding_source = _funding_series(con, series_limit)
        if len(funding_values) >= 20:
            summary = latest_summary(funding_values, series_key="crypto_funding_aggregate", ts_ms=(funding_ts[-1] if funding_ts else ts_value), expected_run=expected_run)
            persist_summary(con, summary, series_type="crypto_funding", symbol="*")
            updated.append({**summary, "series_type": "crypto_funding", "symbol": "*", "source": str(funding_source)})
        else:
            skipped.append({"series_key": "crypto_funding_aggregate", "reason": "insufficient_series", "source": funding_source})

        try:
            con.commit()
        except Exception as e:
            _warn_nonfatal("BOCPD_REGIME_COMMIT_FAILED", e, once_key="commit_failed")
            raise
        return {"ok": True, "updated": updated, "skipped": skipped, "updated_count": len(updated), "skipped_count": len(skipped)}
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("BOCPD_REGIME_CLOSE_FAILED", e, once_key="close_failed")


def main() -> None:
    result = run_update()
    print(result)


if __name__ == "__main__":
    main()
