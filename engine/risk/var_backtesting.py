"""VaR/CVaR forecast persistence and exception backtesting.

This module keeps risk-model validation evidence separate from live risk
consumption. Writers persist forecast and realized-exception rows when the
tables exist; read APIs return explicit empty/unavailable payloads instead of
raising when schema is not yet migrated.
"""

from __future__ import annotations

import json
import math
import os
import time
from typing import Any, Iterable, Mapping, Sequence

from engine.runtime import dbapi_compat
from engine.runtime.storage import connect, table_exists


DEFAULT_BACKTEST_STEP_MS = 86_400_000
DEFAULT_ROLLING_WINDOW = 250
DEFAULT_TEST_ALPHA = 0.05


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value in (None, ""):
            return default
        out = float(value)
    except Exception:
        return default
    return float(out) if math.isfinite(out) else default


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value or {}, separators=(",", ":"), sort_keys=True, default=str)


def _bounded_limit(value: Any, *, default: int = 100, maximum: int = 1000) -> int:
    try:
        limit = int(value if value is not None else default)
    except Exception:
        limit = int(default)
    return max(1, min(int(maximum), int(limit)))


def _clean_exceptions(exceptions: Iterable[Any]) -> list[int]:
    return [1 if bool(x) else 0 for x in list(exceptions or [])]


def _lr_p_value_df1(statistic: float | None) -> float | None:
    stat = _safe_float(statistic)
    if stat is None:
        return None
    if stat <= 0.0:
        return 1.0
    return float(math.erfc(math.sqrt(float(stat) / 2.0)))


def _bernoulli_log_likelihood(exceptions: Sequence[int], probability: float) -> float:
    p = max(1e-12, min(1.0 - 1e-12, float(probability)))
    x = float(sum(int(v) for v in exceptions))
    n = float(len(exceptions))
    return float(x * math.log(p) + (n - x) * math.log(1.0 - p))


def kupiec_pof_test(
    exceptions: Iterable[Any],
    confidence_level: float,
    *,
    alpha: float = DEFAULT_TEST_ALPHA,
) -> dict[str, Any]:
    """Run Kupiec's unconditional coverage likelihood-ratio test."""

    seq = _clean_exceptions(exceptions)
    n = len(seq)
    expected_exception_prob = max(1e-9, min(1.0 - 1e-9, 1.0 - float(confidence_level)))
    x = int(sum(seq))
    if n <= 0:
        return {
            "test": "kupiec_pof",
            "n": 0,
            "exceptions": 0,
            "expected_exception_prob": float(expected_exception_prob),
            "statistic": None,
            "p_value": None,
            "status": "insufficient",
        }

    phat = max(1e-12, min(1.0 - 1e-12, float(x) / float(n)))
    ll_null = _bernoulli_log_likelihood(seq, expected_exception_prob)
    ll_alt = _bernoulli_log_likelihood(seq, phat)
    statistic = max(0.0, -2.0 * (ll_null - ll_alt))
    p_value = _lr_p_value_df1(statistic)
    return {
        "test": "kupiec_pof",
        "n": int(n),
        "exceptions": int(x),
        "exception_rate": float(x / n),
        "expected_exception_prob": float(expected_exception_prob),
        "statistic": float(statistic),
        "p_value": p_value,
        "status": "pass" if p_value is not None and p_value >= float(alpha) else "fail",
    }


def christoffersen_independence_test(
    exceptions: Iterable[Any],
    *,
    alpha: float = DEFAULT_TEST_ALPHA,
) -> dict[str, Any]:
    """Run Christoffersen's independence likelihood-ratio test."""

    seq = _clean_exceptions(exceptions)
    if len(seq) < 2:
        return {
            "test": "christoffersen_independence",
            "n": int(len(seq)),
            "transitions": {"n00": 0, "n01": 0, "n10": 0, "n11": 0},
            "statistic": None,
            "p_value": None,
            "status": "insufficient",
        }

    n00 = n01 = n10 = n11 = 0
    for prev, cur in zip(seq[:-1], seq[1:]):
        if prev == 0 and cur == 0:
            n00 += 1
        elif prev == 0 and cur == 1:
            n01 += 1
        elif prev == 1 and cur == 0:
            n10 += 1
        else:
            n11 += 1

    total = n00 + n01 + n10 + n11
    if total <= 0:
        statistic = 0.0
    else:
        pi = (n01 + n11) / float(total)
        pi01 = n01 / float(max(1, n00 + n01))
        pi11 = n11 / float(max(1, n10 + n11))
        ll_null = (n00 + n10) * math.log(max(1e-12, 1.0 - pi)) + (n01 + n11) * math.log(max(1e-12, pi))
        ll_alt = (
            n00 * math.log(max(1e-12, 1.0 - pi01))
            + n01 * math.log(max(1e-12, pi01))
            + n10 * math.log(max(1e-12, 1.0 - pi11))
            + n11 * math.log(max(1e-12, pi11))
        )
        statistic = max(0.0, -2.0 * (ll_null - ll_alt))

    p_value = _lr_p_value_df1(statistic)
    return {
        "test": "christoffersen_independence",
        "n": int(len(seq)),
        "transitions": {"n00": int(n00), "n01": int(n01), "n10": int(n10), "n11": int(n11)},
        "statistic": float(statistic),
        "p_value": p_value,
        "status": "pass" if p_value is not None and p_value >= float(alpha) else "fail",
    }


def traffic_light_status(
    exceptions: Iterable[Any],
    confidence_level: float,
    *,
    window: int = DEFAULT_ROLLING_WINDOW,
) -> dict[str, Any]:
    """Return Basel-style green/yellow/red status for recent exceptions."""

    seq = _clean_exceptions(exceptions)
    w = max(1, int(window or DEFAULT_ROLLING_WINDOW))
    recent = seq[-w:]
    n = len(recent)
    x = int(sum(recent))
    p = max(1e-9, min(1.0 - 1e-9, 1.0 - float(confidence_level)))

    if n >= 250 and abs(float(confidence_level) - 0.99) <= 1e-9:
        if x <= 4:
            status = "green"
        elif x <= 9:
            status = "yellow"
        else:
            status = "red"
        return {
            "status": status,
            "exceptions": int(x),
            "window": int(n),
            "exception_rate": float(x / n if n else 0.0),
            "reason": f"basel_traffic_light_99_250_exceptions_{x}",
        }

    expected = float(n) * p
    sd = math.sqrt(max(0.0, float(n) * p * (1.0 - p)))
    green_max = int(math.floor(expected + 2.0 * sd))
    red_min = int(math.floor(expected + 4.0 * sd)) + 1
    if x <= green_max:
        status = "green"
    elif x >= red_min:
        status = "red"
    else:
        status = "yellow"
    return {
        "status": status,
        "exceptions": int(x),
        "window": int(n),
        "exception_rate": float(x / n if n else 0.0),
        "reason": f"generic_binomial_band_exceptions_{x}_green_max_{green_max}_red_min_{red_min}",
    }


def exception_evidence(
    exceptions: Iterable[Any],
    confidence_level: float,
    *,
    rolling_window: int = DEFAULT_ROLLING_WINDOW,
    alpha: float = DEFAULT_TEST_ALPHA,
) -> dict[str, Any]:
    seq = _clean_exceptions(exceptions)
    kupiec = kupiec_pof_test(seq, confidence_level, alpha=alpha)
    christoffersen = christoffersen_independence_test(seq, alpha=alpha)
    traffic = traffic_light_status(seq, confidence_level, window=rolling_window)
    recent = seq[-max(1, int(rolling_window or DEFAULT_ROLLING_WINDOW)) :]
    return {
        "kupiec": kupiec,
        "christoffersen": christoffersen,
        "rolling_exception_rate": float(sum(recent) / len(recent)) if recent else 0.0,
        "rolling_window": int(len(recent)),
        "traffic_light": traffic,
    }


def _equity_point(con: Any, sql: str, params: tuple[Any, ...]) -> tuple[int, float] | None:
    row = con.execute(sql, params).fetchone()
    if not row:
        return None
    try:
        ts_ms = int(row[0])
        equity = float(row[1])
    except Exception:
        return None
    if not math.isfinite(equity) or equity <= 0.0:
        return None
    return int(ts_ms), float(equity)


def realized_portfolio_return(
    con: Any,
    *,
    forecast_ts_ms: int,
    horizon_steps: int,
    step_ms: int = DEFAULT_BACKTEST_STEP_MS,
) -> dict[str, Any]:
    target_ts_ms = int(forecast_ts_ms) + int(max(1, horizon_steps)) * int(max(1, step_ms))
    if not table_exists(con, "equity_history"):
        return {"ok": False, "reason": "equity_history_missing", "target_ts_ms": int(target_ts_ms)}

    start = _equity_point(
        con,
        """
        SELECT ts_ms, equity
        FROM equity_history
        WHERE ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (int(forecast_ts_ms),),
    )
    end = _equity_point(
        con,
        """
        SELECT ts_ms, equity
        FROM equity_history
        WHERE ts_ms >= ?
        ORDER BY ts_ms ASC
        LIMIT 1
        """,
        (int(target_ts_ms),),
    )
    if start is None or end is None:
        return {
            "ok": False,
            "reason": "realized_equity_points_missing",
            "forecast_ts_ms": int(forecast_ts_ms),
            "target_ts_ms": int(target_ts_ms),
            "start_found": bool(start is not None),
            "end_found": bool(end is not None),
        }

    start_ts_ms, start_equity = start
    realized_ts_ms, end_equity = end
    realized = float((end_equity / start_equity) - 1.0)
    return {
        "ok": True,
        "forecast_ts_ms": int(forecast_ts_ms),
        "target_ts_ms": int(target_ts_ms),
        "start_ts_ms": int(start_ts_ms),
        "realized_ts_ms": int(realized_ts_ms),
        "start_equity": float(start_equity),
        "end_equity": float(end_equity),
        "realized_portfolio_return": float(realized),
        "realized_portfolio_loss": float(-realized),
    }


def ensure_var_backtest_schema(con: Any) -> None:
    """Create the validation tables for direct SQLite/unit-test callers."""

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_var_forecasts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          forecast_id TEXT NOT NULL UNIQUE,
          forecast_ts_ms INTEGER NOT NULL,
          horizon_steps INTEGER NOT NULL,
          var_95 REAL,
          var_99 REAL,
          cvar_95 REAL,
          cvar_99 REAL,
          simulation_method TEXT,
          metadata_json TEXT,
          created_ts_ms INTEGER NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_var_backtest_results (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          forecast_id TEXT NOT NULL,
          forecast_ts_ms INTEGER NOT NULL,
          realized_ts_ms INTEGER NOT NULL,
          horizon_steps INTEGER NOT NULL,
          confidence_level REAL NOT NULL,
          var_value REAL NOT NULL,
          cvar_value REAL,
          realized_portfolio_return REAL NOT NULL,
          realized_portfolio_loss REAL NOT NULL,
          exception INTEGER NOT NULL,
          kupiec_pof_stat REAL,
          kupiec_pof_p_value REAL,
          kupiec_pof_status TEXT,
          christoffersen_ind_stat REAL,
          christoffersen_ind_p_value REAL,
          christoffersen_ind_status TEXT,
          rolling_exception_rate REAL,
          rolling_window INTEGER,
          traffic_light_status TEXT,
          traffic_light_reason TEXT,
          metadata_json TEXT,
          created_ts_ms INTEGER NOT NULL,
          UNIQUE(forecast_id, confidence_level)
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_risk_var_forecasts_ts ON risk_var_forecasts(forecast_ts_ms DESC)")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_risk_var_backtest_results_ts "
        "ON risk_var_backtest_results(forecast_ts_ms DESC, confidence_level)"
    )


def _table_ready(con: Any, table_name: str) -> bool:
    try:
        return bool(table_exists(con, table_name))
    except Exception:
        return False


def _forecast_rows(con: Any, *, limit: int, now_ms: int, step_ms: int) -> list[dict[str, Any]]:
    if not _table_ready(con, "risk_var_forecasts"):
        return []
    rows = con.execute(
        """
        SELECT id, forecast_id, forecast_ts_ms, horizon_steps, var_95, var_99, cvar_95, cvar_99,
               simulation_method, metadata_json, created_ts_ms
        FROM risk_var_forecasts
        WHERE forecast_ts_ms + (horizon_steps * ?) <= ?
        ORDER BY forecast_ts_ms ASC, id ASC
        LIMIT ?
        """,
        (int(max(1, step_ms)), int(now_ms), int(limit)),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows or []:
        out.append(
            {
                "id": _safe_int(row[0]),
                "forecast_id": str(row[1] or ""),
                "forecast_ts_ms": _safe_int(row[2]),
                "horizon_steps": _safe_int(row[3], 1),
                "var_95": _safe_float(row[4]),
                "var_99": _safe_float(row[5]),
                "cvar_95": _safe_float(row[6]),
                "cvar_99": _safe_float(row[7]),
                "simulation_method": str(row[8] or ""),
                "metadata": _json_dict(row[9]),
                "created_ts_ms": _safe_int(row[10]),
            }
        )
    return out


def _prior_exceptions(
    con: Any,
    *,
    confidence_level: float,
    horizon_steps: int,
    before_forecast_ts_ms: int,
    limit: int,
) -> list[int]:
    if not _table_ready(con, "risk_var_backtest_results"):
        return []
    rows = con.execute(
        """
        SELECT exception
        FROM risk_var_backtest_results
        WHERE confidence_level=? AND horizon_steps=? AND forecast_ts_ms < ?
        ORDER BY forecast_ts_ms DESC, id DESC
        LIMIT ?
        """,
        (float(confidence_level), int(horizon_steps), int(before_forecast_ts_ms), int(max(1, limit))),
    ).fetchall()
    seq = [1 if bool((row or [0])[0]) else 0 for row in rows or []]
    return list(reversed(seq))


def _persist_result(con: Any, row: Mapping[str, Any]) -> None:
    metadata_value: Any = _json_dumps(row.get("metadata"))
    if not dbapi_compat.is_sqlite_connection(con):
        metadata_value = row.get("metadata") or {}
    con.execute(
        """
        INSERT INTO risk_var_backtest_results(
          forecast_id, forecast_ts_ms, realized_ts_ms, horizon_steps, confidence_level,
          var_value, cvar_value, realized_portfolio_return, realized_portfolio_loss,
          exception, kupiec_pof_stat, kupiec_pof_p_value, kupiec_pof_status,
          christoffersen_ind_stat, christoffersen_ind_p_value, christoffersen_ind_status,
          rolling_exception_rate, rolling_window, traffic_light_status, traffic_light_reason,
          metadata_json, created_ts_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(forecast_id, confidence_level) DO UPDATE SET
          realized_ts_ms=excluded.realized_ts_ms,
          var_value=excluded.var_value,
          cvar_value=excluded.cvar_value,
          realized_portfolio_return=excluded.realized_portfolio_return,
          realized_portfolio_loss=excluded.realized_portfolio_loss,
          exception=excluded.exception,
          kupiec_pof_stat=excluded.kupiec_pof_stat,
          kupiec_pof_p_value=excluded.kupiec_pof_p_value,
          kupiec_pof_status=excluded.kupiec_pof_status,
          christoffersen_ind_stat=excluded.christoffersen_ind_stat,
          christoffersen_ind_p_value=excluded.christoffersen_ind_p_value,
          christoffersen_ind_status=excluded.christoffersen_ind_status,
          rolling_exception_rate=excluded.rolling_exception_rate,
          rolling_window=excluded.rolling_window,
          traffic_light_status=excluded.traffic_light_status,
          traffic_light_reason=excluded.traffic_light_reason,
          metadata_json=excluded.metadata_json,
          created_ts_ms=excluded.created_ts_ms
        """,
        (
            str(row.get("forecast_id") or ""),
            int(row.get("forecast_ts_ms") or 0),
            int(row.get("realized_ts_ms") or 0),
            int(row.get("horizon_steps") or 0),
            float(row.get("confidence_level") or 0.0),
            float(row.get("var_value") or 0.0),
            _safe_float(row.get("cvar_value")),
            float(row.get("realized_portfolio_return") or 0.0),
            float(row.get("realized_portfolio_loss") or 0.0),
            1 if bool(row.get("exception")) else 0,
            _safe_float(row.get("kupiec_pof_stat")),
            _safe_float(row.get("kupiec_pof_p_value")),
            str(row.get("kupiec_pof_status") or ""),
            _safe_float(row.get("christoffersen_ind_stat")),
            _safe_float(row.get("christoffersen_ind_p_value")),
            str(row.get("christoffersen_ind_status") or ""),
            _safe_float(row.get("rolling_exception_rate")),
            _safe_int(row.get("rolling_window")),
            str(row.get("traffic_light_status") or ""),
            str(row.get("traffic_light_reason") or ""),
            metadata_value,
            int(row.get("created_ts_ms") or _now_ms()),
        ),
    )


def run_var_backtest(
    *,
    con: Any = None,
    now_ms: int | None = None,
    limit: int = 100,
    step_ms: int | None = None,
    rolling_window: int = DEFAULT_ROLLING_WINDOW,
    persist: bool = True,
) -> dict[str, Any]:
    """Backtest matured VaR forecasts against realized portfolio equity."""

    owns = con is None
    db = con or connect(readonly=False)
    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    step = int(step_ms if step_ms is not None else _safe_int(os.environ.get("VAR_BACKTEST_STEP_MS"), DEFAULT_BACKTEST_STEP_MS))
    out_rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    try:
        if not _table_ready(db, "risk_var_forecasts") or not _table_ready(db, "risk_var_backtest_results"):
            return {
                "ok": True,
                "ready": False,
                "status": "schema_missing",
                "ts_ms": int(ts_ms),
                "written": 0,
                "rows": [],
                "skipped": [],
            }
        forecasts = _forecast_rows(db, limit=_bounded_limit(limit), now_ms=ts_ms, step_ms=step)
        for forecast in forecasts:
            realized = realized_portfolio_return(
                db,
                forecast_ts_ms=int(forecast.get("forecast_ts_ms") or 0),
                horizon_steps=int(forecast.get("horizon_steps") or 1),
                step_ms=step,
            )
            if not bool(realized.get("ok")):
                skipped.append({"forecast_id": forecast.get("forecast_id"), "reason": realized.get("reason")})
                continue
            for confidence, var_key, cvar_key in ((0.95, "var_95", "cvar_95"), (0.99, "var_99", "cvar_99")):
                var_value = _safe_float(forecast.get(var_key))
                if var_value is None:
                    continue
                cvar_value = _safe_float(forecast.get(cvar_key))
                exception = float(realized["realized_portfolio_return"]) <= float(var_value)
                prior = _prior_exceptions(
                    db,
                    confidence_level=float(confidence),
                    horizon_steps=int(forecast.get("horizon_steps") or 1),
                    before_forecast_ts_ms=int(forecast.get("forecast_ts_ms") or 0),
                    limit=max(1, int(rolling_window) - 1),
                )
                evidence = exception_evidence(
                    [*prior, 1 if exception else 0],
                    float(confidence),
                    rolling_window=int(rolling_window),
                )
                kupiec = evidence["kupiec"]
                christoffersen = evidence["christoffersen"]
                traffic = evidence["traffic_light"]
                row = {
                    "forecast_id": str(forecast.get("forecast_id") or ""),
                    "forecast_ts_ms": int(forecast.get("forecast_ts_ms") or 0),
                    "realized_ts_ms": int(realized.get("realized_ts_ms") or 0),
                    "horizon_steps": int(forecast.get("horizon_steps") or 1),
                    "confidence_level": float(confidence),
                    "var_value": float(var_value),
                    "cvar_value": cvar_value,
                    "realized_portfolio_return": float(realized.get("realized_portfolio_return") or 0.0),
                    "realized_portfolio_loss": float(realized.get("realized_portfolio_loss") or 0.0),
                    "exception": bool(exception),
                    "kupiec_pof_stat": kupiec.get("statistic"),
                    "kupiec_pof_p_value": kupiec.get("p_value"),
                    "kupiec_pof_status": str(kupiec.get("status") or ""),
                    "christoffersen_ind_stat": christoffersen.get("statistic"),
                    "christoffersen_ind_p_value": christoffersen.get("p_value"),
                    "christoffersen_ind_status": str(christoffersen.get("status") or ""),
                    "rolling_exception_rate": evidence.get("rolling_exception_rate"),
                    "rolling_window": evidence.get("rolling_window"),
                    "traffic_light_status": str(traffic.get("status") or ""),
                    "traffic_light_reason": str(traffic.get("reason") or ""),
                    "metadata": {
                        "pit_alignment": realized,
                        "simulation_method": forecast.get("simulation_method"),
                        "forecast_metadata": forecast.get("metadata") or {},
                    },
                    "created_ts_ms": int(ts_ms),
                }
                if persist:
                    _persist_result(db, row)
                out_rows.append(row)
        return {
            "ok": True,
            "ready": bool(out_rows),
            "status": "ok" if out_rows else "no_matured_forecasts",
            "ts_ms": int(ts_ms),
            "written": int(len(out_rows)) if persist else 0,
            "rows": out_rows,
            "skipped": skipped,
        }
    finally:
        if owns:
            db.close()


def fetch_recent_backtest_rows(con: Any, *, limit: int = 100) -> list[dict[str, Any]]:
    if not _table_ready(con, "risk_var_backtest_results"):
        return []
    columns = [
        "id",
        "forecast_id",
        "forecast_ts_ms",
        "realized_ts_ms",
        "horizon_steps",
        "confidence_level",
        "var_value",
        "cvar_value",
        "realized_portfolio_return",
        "realized_portfolio_loss",
        "exception",
        "kupiec_pof_stat",
        "kupiec_pof_p_value",
        "kupiec_pof_status",
        "christoffersen_ind_stat",
        "christoffersen_ind_p_value",
        "christoffersen_ind_status",
        "rolling_exception_rate",
        "rolling_window",
        "traffic_light_status",
        "traffic_light_reason",
        "metadata_json",
        "created_ts_ms",
    ]
    rows = con.execute(
        f"""
        SELECT {', '.join(columns)}
        FROM risk_var_backtest_results
        ORDER BY forecast_ts_ms DESC, confidence_level DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows or []:
        item = {column: row[idx] for idx, column in enumerate(columns)}
        item["exception"] = bool(item.get("exception"))
        item["metadata"] = _json_dict(item.pop("metadata_json", None))
        out.append(item)
    return out


def build_var_backtest_payload(*, limit: int = 100, con: Any = None, now_ms: int | None = None) -> dict[str, Any]:
    owns = con is None
    db = con or connect(readonly=True)
    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    try:
        if not _table_ready(db, "risk_var_backtest_results"):
            return {
                "ok": True,
                "ready": False,
                "status": "schema_missing",
                "ts_ms": int(ts_ms),
                "rows": [],
                "summary": {"count": 0, "latest_status": "unknown"},
                "authority": {
                    "mode": "read_only_risk_model_backtesting",
                    "source_table": "risk_var_backtest_results",
                },
            }
        rows = fetch_recent_backtest_rows(db, limit=limit)
        latest_status = str(rows[0].get("traffic_light_status") or "unknown") if rows else "unknown"
        red_or_fail = [
            row
            for row in rows
            if str(row.get("traffic_light_status") or "").lower() == "red"
            or str(row.get("kupiec_pof_status") or "").lower() == "fail"
            or str(row.get("christoffersen_ind_status") or "").lower() == "fail"
        ]
        return {
            "ok": True,
            "ready": bool(rows),
            "status": "ok" if rows else "empty",
            "ts_ms": int(ts_ms),
            "rows": rows,
            "summary": {
                "count": int(len(rows)),
                "latest_status": latest_status,
                "failing_count": int(len(red_or_fail)),
                "latest_forecast_ts_ms": int(rows[0].get("forecast_ts_ms") or 0) if rows else 0,
            },
            "authority": {
                "mode": "read_only_risk_model_backtesting",
                "source_table": "risk_var_backtest_results",
                "source_route": "/api/risk/var_backtest",
            },
        }
    finally:
        if owns:
            db.close()


__all__ = [
    "build_var_backtest_payload",
    "christoffersen_independence_test",
    "ensure_var_backtest_schema",
    "exception_evidence",
    "fetch_recent_backtest_rows",
    "kupiec_pof_test",
    "realized_portfolio_return",
    "run_var_backtest",
    "traffic_light_status",
]
