"""Read-only drift and anomaly attribution snapshot for dashboard operators."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect_ro_direct


DEFAULT_TOP_N = 8
DEFAULT_STALE_AFTER_MS = int(os.environ.get("DRIFT_EXPLAINER_STALE_AFTER_MS", str(6 * 60 * 60 * 1000)))
LOG = get_logger("engine.api.drift_explainer")
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
        component="engine.api.drift_explainer",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        out = float(value)
        if not math.isfinite(out):
            return default
        return out
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except Exception:
        return int(default)


def _safe_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _safe_ident(name: str) -> str:
    ident = str(name or "").strip()
    if not ident.replace("_", "").isalnum() or not ident:
        raise ValueError(f"unsafe identifier: {name!r}")
    return ident


def _table_exists(con, table: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {_safe_ident(table)} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _query(con, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
    try:
        return list(con.execute(sql, params).fetchall() or [])
    except Exception:
        return []


def _query_one(con, sql: str, params: tuple[Any, ...] = ()) -> Any | None:
    try:
        return con.execute(sql, params).fetchone()
    except Exception:
        return None


def _is_stale(ts_ms: int, now_ms: int, stale_after_ms: int) -> bool:
    return bool(ts_ms > 0 and (int(now_ms) - int(ts_ms)) > int(stale_after_ms))


def _source_state(
    *,
    available: bool,
    rows: int = 0,
    ts_ms: int = 0,
    now_ms: int,
    stale_after_ms: int,
    reason: str = "",
    endpoint: str | None = None,
) -> dict[str, Any]:
    stale = bool(available and ts_ms > 0 and _is_stale(ts_ms, now_ms, stale_after_ms))
    return {
        "available": bool(available),
        "rows": int(rows),
        "ts_ms": int(ts_ms or 0),
        "stale": stale,
        "reason": str(reason or ("ok" if available else "unavailable")),
        **({"endpoint": endpoint} if endpoint else {}),
    }


def _severity_rank(severity: str) -> int:
    return {
        "CRIT": 4,
        "CRITICAL": 4,
        "WARN": 3,
        "STALE": 2,
        "UNKNOWN": 1,
        "OK": 0,
        "NORMAL": 0,
        "INFO": 0,
    }.get(str(severity or "").upper(), 0)


def _normalize_severity(severity: str) -> str:
    value = str(severity or "").upper().strip()
    if value == "CRITICAL":
        return "CRIT"
    if value in {"CRIT", "WARN", "OK", "STALE", "UNKNOWN", "INFO"}:
        return value
    if value == "NORMAL":
        return "OK"
    return "UNKNOWN"


def _append_unavailable(items: list[dict[str, str]], field: str, reason: str) -> None:
    items.append({"field": str(field), "reason": str(reason)})


def _add_symbol(affected: dict[str, Any], symbol: str, *, source: str, metric: str, value: Any) -> None:
    sym = str(symbol or "").upper().strip()
    if not sym or sym == "__ALL__":
        return
    rows = affected.setdefault("symbols", [])
    key = (sym, str(source), str(metric))
    existing = {
        (str(row.get("symbol") or ""), str(row.get("source") or ""), str(row.get("metric") or ""))
        for row in rows
    }
    if key not in existing:
        rows.append({"symbol": sym, "source": str(source), "metric": str(metric), "value": value})


def _add_model(affected: dict[str, Any], model_name: str, *, source: str, detail: str = "") -> None:
    name = str(model_name or "").strip()
    if not name:
        return
    rows = affected.setdefault("models", [])
    existing = {(str(row.get("model") or ""), str(row.get("source") or "")) for row in rows}
    key = (name, str(source))
    if key not in existing:
        rows.append({"model": name, "source": str(source), "detail": str(detail or "")})


def _add_regime(affected: dict[str, Any], regime: str, *, source: str, detail: str = "") -> None:
    name = str(regime or "").strip()
    if not name:
        return
    rows = affected.setdefault("regimes", [])
    existing = {(str(row.get("regime") or ""), str(row.get("source") or "")) for row in rows}
    key = (name, str(source))
    if key not in existing:
        rows.append({"regime": name, "source": str(source), "detail": str(detail or "")})


def _contributor(
    *,
    kind: str,
    label: str,
    dimension: str,
    source: str,
    severity: str,
    current_value: Any = None,
    baseline_value: Any = None,
    delta_value: Any = None,
    delta_pct: Any = None,
    metric: str = "",
    metric_value: Any = None,
    ts_ms: int = 0,
    stale: bool = False,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "kind": str(kind),
        "label": str(label),
        "dimension": str(dimension),
        "source": str(source),
        "severity": _normalize_severity(severity),
        "current_value": current_value,
        "baseline_value": baseline_value,
        "delta_value": delta_value,
        "delta_pct": delta_pct,
        "metric": str(metric or ""),
        "metric_value": metric_value,
        "ts_ms": int(ts_ms or 0),
        "stale": bool(stale),
        "details": dict(details or {}),
    }


def _read_equity_drift(con, *, now_ms: int, stale_after_ms: int, contributors: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if not _table_exists(con, "equity_drift"):
        return (
            _source_state(
                available=False,
                now_ms=now_ms,
                stale_after_ms=stale_after_ms,
                reason="equity_drift table unavailable",
                endpoint="/api/equity_drift",
            ),
            None,
        )

    row = _query_one(
        con,
        """
        SELECT ts_ms, broker_equity, backtest_equity, diff_equity,
               diff_equity_pct, level, reason, backtest_run_id, backtest_ts_ms
        FROM equity_drift
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
    )
    if not row:
        return (
            _source_state(
                available=True,
                rows=0,
                now_ms=now_ms,
                stale_after_ms=stale_after_ms,
                reason="no equity drift rows",
                endpoint="/api/equity_drift",
            ),
            None,
        )

    ts_ms = _safe_int(row[0])
    level = _normalize_severity(str(row[5] or "UNKNOWN"))
    stale = _is_stale(ts_ms, now_ms, stale_after_ms)
    equity = {
        "ts_ms": ts_ms,
        "broker_equity": _safe_float(row[1]),
        "backtest_equity": _safe_float(row[2]),
        "diff_equity": _safe_float(row[3]),
        "diff_equity_pct": _safe_float(row[4]),
        "level": level,
        "reason": str(row[6] or ""),
        "backtest_run_id": _safe_int(row[7]),
        "backtest_ts_ms": _safe_int(row[8]),
        "stale": stale,
    }
    contributors.append(
        _contributor(
            kind="equity",
            label="Broker vs backtest equity",
            dimension="portfolio",
            source="equity_drift",
            severity=level,
            current_value=equity["broker_equity"],
            baseline_value=equity["backtest_equity"],
            delta_value=equity["diff_equity"],
            delta_pct=equity["diff_equity_pct"],
            metric="equity_diff_pct",
            metric_value=equity["diff_equity_pct"],
            ts_ms=ts_ms,
            stale=stale,
            details={
                "reason": equity["reason"],
                "backtest_run_id": equity["backtest_run_id"],
                "backtest_ts_ms": equity["backtest_ts_ms"],
            },
        )
    )
    return (
        _source_state(
            available=True,
            rows=1,
            ts_ms=ts_ms,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
            reason="ok",
            endpoint="/api/equity_drift",
        ),
        equity,
    )


def _read_model_drift(
    con,
    *,
    top_n: int,
    now_ms: int,
    stale_after_ms: int,
    contributors: list[dict[str, Any]],
    affected: dict[str, Any],
) -> dict[str, Any]:
    if not _table_exists(con, "model_drift"):
        return _source_state(
            available=False,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
            reason="model_drift table unavailable",
        )

    rows = _query(
        con,
        """
        SELECT symbol, horizon_s, ts_ms, n, mae, baseline_mae, drift_ratio
        FROM model_drift
        ORDER BY drift_ratio DESC, ts_ms DESC
        LIMIT ?
        """,
        (int(top_n),),
    )
    latest_ts_ms = max((_safe_int(row[2]) for row in rows), default=0)
    for row in rows:
        symbol = str(row[0] or "").upper().strip()
        horizon_s = _safe_int(row[1])
        ts_ms = _safe_int(row[2])
        ratio = _safe_float(row[6], 0.0)
        mae = _safe_float(row[4])
        baseline_mae = _safe_float(row[5])
        contributors.append(
            _contributor(
                kind="model_drift",
                label="Model residual drift",
                dimension=f"{symbol or 'UNKNOWN'} / {horizon_s}s",
                source="model_drift",
                severity="INFO",
                current_value=mae,
                baseline_value=baseline_mae,
                delta_value=(None if mae is None or baseline_mae is None else float(mae) - float(baseline_mae)),
                metric="drift_ratio",
                metric_value=ratio,
                ts_ms=ts_ms,
                stale=_is_stale(ts_ms, now_ms, stale_after_ms),
                details={"symbol": symbol, "horizon_s": horizon_s, "n": _safe_int(row[3])},
            )
        )
        _add_symbol(affected, symbol, source="model_drift", metric="drift_ratio", value=ratio)

    return _source_state(
        available=True,
        rows=len(rows),
        ts_ms=latest_ts_ms,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
        reason=("ok" if rows else "no model drift rows"),
    )


def _read_feature_distribution_drift(
    con,
    *,
    top_n: int,
    now_ms: int,
    stale_after_ms: int,
    contributors: list[dict[str, Any]],
) -> dict[str, Any]:
    if not _table_exists(con, "feature_distribution_drift"):
        return _source_state(
            available=False,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
            reason="feature_distribution_drift table unavailable",
        )

    rows = _query(
        con,
        """
        SELECT feature_id, ts_ms, recent_n, baseline_n, recent_mean,
               baseline_mean, baseline_std, shift_z, drift_score, drift_flag
        FROM feature_distribution_drift
        ORDER BY drift_score DESC, shift_z DESC, feature_id ASC
        LIMIT ?
        """,
        (int(top_n),),
    )
    latest_ts_ms = max((_safe_int(row[1]) for row in rows), default=0)
    for row in rows:
        ts_ms = _safe_int(row[1])
        drift_flag = _safe_int(row[9])
        contributors.append(
            _contributor(
                kind="feature",
                label=str(row[0] or ""),
                dimension=str(row[0] or ""),
                source="feature_distribution_drift",
                severity=("WARN" if drift_flag else "OK"),
                current_value=_safe_float(row[4]),
                baseline_value=_safe_float(row[5]),
                delta_value=(
                    None
                    if _safe_float(row[4]) is None or _safe_float(row[5]) is None
                    else float(row[4]) - float(row[5])
                ),
                metric="shift_z",
                metric_value=_safe_float(row[7]),
                ts_ms=ts_ms,
                stale=_is_stale(ts_ms, now_ms, stale_after_ms),
                details={
                    "recent_n": _safe_int(row[2]),
                    "baseline_n": _safe_int(row[3]),
                    "baseline_std": _safe_float(row[6]),
                    "drift_score": _safe_float(row[8]),
                    "drift_flag": drift_flag,
                },
            )
        )

    return _source_state(
        available=True,
        rows=len(rows),
        ts_ms=latest_ts_ms,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
        reason=("ok" if rows else "no feature distribution drift rows"),
    )


def _read_residual_distribution_drift(
    con,
    *,
    top_n: int,
    now_ms: int,
    stale_after_ms: int,
    contributors: list[dict[str, Any]],
    affected: dict[str, Any],
) -> dict[str, Any]:
    if not _table_exists(con, "residual_distribution_drift"):
        return _source_state(
            available=False,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
            reason="residual_distribution_drift table unavailable",
        )

    rows = _query(
        con,
        """
        SELECT scope, symbol, ts_ms, recent_n, baseline_n, recent_mean,
               baseline_mean, baseline_std, shift_z, abs_mean_recent,
               abs_mean_base, abs_shift_ratio, drift_score, drift_flag
        FROM residual_distribution_drift
        ORDER BY drift_score DESC, shift_z DESC, scope ASC, symbol ASC
        LIMIT ?
        """,
        (int(top_n),),
    )
    latest_ts_ms = max((_safe_int(row[2]) for row in rows), default=0)
    for row in rows:
        scope = str(row[0] or "global")
        symbol = str(row[1] or "")
        ts_ms = _safe_int(row[2])
        drift_flag = _safe_int(row[13])
        label = "Global residuals" if symbol == "__all__" else f"{symbol} residuals"
        contributors.append(
            _contributor(
                kind="residual",
                label=label,
                dimension=f"{scope}:{symbol}",
                source="residual_distribution_drift",
                severity=("WARN" if drift_flag else "OK"),
                current_value=_safe_float(row[5]),
                baseline_value=_safe_float(row[6]),
                delta_value=(
                    None
                    if _safe_float(row[5]) is None or _safe_float(row[6]) is None
                    else float(row[5]) - float(row[6])
                ),
                metric="abs_shift_ratio",
                metric_value=_safe_float(row[11]),
                ts_ms=ts_ms,
                stale=_is_stale(ts_ms, now_ms, stale_after_ms),
                details={
                    "recent_n": _safe_int(row[3]),
                    "baseline_n": _safe_int(row[4]),
                    "baseline_std": _safe_float(row[7]),
                    "shift_z": _safe_float(row[8]),
                    "abs_mean_recent": _safe_float(row[9]),
                    "abs_mean_base": _safe_float(row[10]),
                    "drift_score": _safe_float(row[12]),
                    "drift_flag": drift_flag,
                },
            )
        )
        if scope == "symbol":
            _add_symbol(affected, symbol, source="residual_distribution_drift", metric="abs_shift_ratio", value=_safe_float(row[11]))

    return _source_state(
        available=True,
        rows=len(rows),
        ts_ms=latest_ts_ms,
        now_ms=now_ms,
        stale_after_ms=stale_after_ms,
        reason=("ok" if rows else "no residual distribution drift rows"),
    )


def _production_monitor_label(metric_name: str) -> str:
    labels = {
        "feature_drift": "Feature drift",
        "missing_feature_rate": "Missing feature rate",
        "prediction_drift": "Prediction drift",
        "target_label_drift": "Target/label drift",
        "calibration_ece": "Calibration drift",
        "conformal_coverage": "Conformal coverage drift",
        "shadow_live_disagreement": "Shadow-vs-live disagreement",
        "net_pnl_degradation": "Net PnL degradation",
    }
    return labels.get(str(metric_name or ""), str(metric_name or "production monitor").replace("_", " ").title())


def _read_production_monitoring(
    con,
    *,
    top_n: int,
    now_ms: int,
    stale_after_ms: int,
    contributors: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not _table_exists(con, "production_monitoring_metrics"):
        return (
            _source_state(
                available=False,
                now_ms=now_ms,
                stale_after_ms=stale_after_ms,
                reason="production_monitoring_metrics table unavailable",
                endpoint="/api/model/performance_divergence",
            ),
            {},
        )

    rows = _query(
        con,
        """
        SELECT metric_name, scope, dimension, ts_ms, value, baseline_value,
               threshold_value, severity, state, action_signal, labels_available,
               sample_n, details_json
        FROM production_monitoring_metrics
        ORDER BY
          CASE severity WHEN 'CRIT' THEN 4 WHEN 'WARN' THEN 3 WHEN 'STALE' THEN 2 WHEN 'UNKNOWN' THEN 1 ELSE 0 END DESC,
          ts_ms DESC,
          metric_name ASC
        LIMIT ?
        """,
        (int(top_n),),
    )
    latest_ts_ms = max((_safe_int(row[3]) for row in rows), default=0)
    metrics: list[dict[str, Any]] = []
    for row in rows:
        metric_name = str(row[0] or "")
        severity = _normalize_severity(str(row[7] or "UNKNOWN"))
        value = _safe_float(row[4])
        baseline = _safe_float(row[5])
        threshold = _safe_float(row[6])
        details = _safe_json_obj(row[12])
        metrics.append(
            {
                "metric_name": metric_name,
                "scope": str(row[1] or "global"),
                "dimension": str(row[2] or ""),
                "ts_ms": _safe_int(row[3]),
                "value": value,
                "baseline_value": baseline,
                "threshold_value": threshold,
                "severity": severity,
                "state": str(row[8] or ""),
                "action_signal": str(row[9] or ""),
                "labels_available": bool(_safe_int(row[10])),
                "sample_n": _safe_int(row[11]),
                "details": details,
            }
        )
        contributors.append(
            _contributor(
                kind="production_monitor",
                label=_production_monitor_label(metric_name),
                dimension=str(row[2] or row[1] or "global"),
                source="production_monitoring_metrics",
                severity=severity,
                current_value=value,
                baseline_value=baseline,
                delta_value=(None if value is None or baseline is None else float(value) - float(baseline)),
                metric=metric_name,
                metric_value=value,
                ts_ms=_safe_int(row[3]),
                stale=_is_stale(_safe_int(row[3]), now_ms, stale_after_ms),
                details={
                    **details,
                    "threshold_value": threshold,
                    "state": str(row[8] or ""),
                    "action_signal": str(row[9] or ""),
                    "labels_available": bool(_safe_int(row[10])),
                    "sample_n": _safe_int(row[11]),
                },
            )
        )

    return (
        _source_state(
            available=True,
            rows=len(rows),
            ts_ms=latest_ts_ms,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
            reason=("ok" if rows else "no production monitoring rows"),
            endpoint="/api/model/performance_divergence",
        ),
        {"metrics": metrics},
    )


def _read_distribution_state(con) -> dict[str, Any] | None:
    try:
        from engine.strategy.distribution_drift import get_latest_distribution_drift_snapshot

        snapshot = get_latest_distribution_drift_snapshot(con=con)
        return dict(snapshot or {}) if isinstance(snapshot, dict) else None
    except Exception:
        return None


def _read_risk_state(con, *, now_ms: int, stale_after_ms: int, affected: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if not _table_exists(con, "risk_state"):
        return (
            _source_state(
                available=False,
                now_ms=now_ms,
                stale_after_ms=stale_after_ms,
                reason="risk_state table unavailable",
                endpoint="/api/risk/monte_carlo",
            ),
            {},
        )

    rows = _query(
        con,
        """
        SELECT key, value, updated_ts_ms
        FROM risk_state
        WHERE key IN (
          'monte_carlo_risk_info',
          'monte_carlo_risk_status',
          'monte_carlo_risk_pending',
          'monte_carlo_risk_ts_ms'
        )
        """,
    )
    state = {str(row[0] or ""): {"value": row[1], "updated_ts_ms": _safe_int(row[2])} for row in rows}
    raw_info = state.get("monte_carlo_risk_info", {}).get("value")
    info = _safe_json_obj(raw_info)
    status = str(state.get("monte_carlo_risk_status", {}).get("value") or info.get("status") or "unknown")
    pending_raw = str(state.get("monte_carlo_risk_pending", {}).get("value") or info.get("pending") or "0")
    pending = pending_raw.strip().lower() in {"1", "true", "yes", "on"}
    ts_ms = _safe_int(info.get("ts_ms") or state.get("monte_carlo_risk_ts_ms", {}).get("value") or state.get("monte_carlo_risk_info", {}).get("updated_ts_ms"))

    weights = info.get("weights") if isinstance(info.get("weights"), dict) else {}
    for symbol, weight in sorted(weights.items(), key=lambda item: abs(float(_safe_float(item[1], 0.0) or 0.0)), reverse=True)[:DEFAULT_TOP_N]:
        _add_symbol(affected, str(symbol), source="monte_carlo_risk", metric="portfolio_weight", value=_safe_float(weight))

    risk = {
        "ready": bool(info.get("ready")),
        "status": status,
        "pending": pending,
        "ts_ms": ts_ms,
        "symbols": list(info.get("symbols") or []),
        "weights": dict(weights),
        "var_95": _safe_float(info.get("var_95")),
        "var_99": _safe_float(info.get("var_99")),
        "cvar_95": _safe_float(info.get("cvar_95")),
        "cvar_99": _safe_float(info.get("cvar_99")),
        "stress": info.get("stress") if isinstance(info.get("stress"), dict) else {},
    }
    return (
        _source_state(
            available=bool(rows),
            rows=len(rows),
            ts_ms=ts_ms,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
            reason=("ok" if rows else "no Monte Carlo risk state rows"),
            endpoint="/api/risk/monte_carlo",
        ),
        risk,
    )


def _read_model_diagnostics_context(
    con,
    *,
    top_n: int,
    now_ms: int,
    stale_after_ms: int,
    affected: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    context: dict[str, Any] = {"drift_retrain_status": {}, "drift_retrain_events": []}
    latest_ts_ms = 0
    rows_total = 0

    if _table_exists(con, "runtime_meta"):
        row = _query_one(con, "SELECT value FROM runtime_meta WHERE key=?", ("drift_retrain_status",))
        if row:
            context["drift_retrain_status"] = _safe_json_obj(row[0])
            status_ts = _safe_int(context["drift_retrain_status"].get("ts_ms"))
            latest_ts_ms = max(latest_ts_ms, status_ts)
            for model_name in context["drift_retrain_status"].get("triggered_models") or []:
                _add_model(affected, str(model_name), source="drift_retrain_status")

    if _table_exists(con, "drift_retrain_events"):
        rows = _query(
            con,
            """
            SELECT created_ts, model_name, family, trigger_type, trigger_metrics,
                   action_taken, cooldown_applied, candidate_version,
                   outcome_status, diagnostics
            FROM drift_retrain_events
            ORDER BY created_ts DESC, id DESC
            LIMIT ?
            """,
            (int(top_n),),
        )
        rows_total += len(rows)
        for row in rows:
            created_ts = _safe_int(row[0])
            metrics = _safe_json_obj(row[4])
            diagnostics = _safe_json_obj(row[9])
            event = {
                "created_ts": created_ts,
                "model_name": str(row[1] or ""),
                "family": str(row[2] or ""),
                "trigger_type": str(row[3] or ""),
                "trigger_metrics": metrics,
                "action_taken": str(row[5] or ""),
                "cooldown_applied": bool(row[6]),
                "candidate_version": str(row[7] or ""),
                "outcome_status": str(row[8] or ""),
                "diagnostics": diagnostics,
            }
            context["drift_retrain_events"].append(event)
            latest_ts_ms = max(latest_ts_ms, created_ts)
            _add_model(affected, event["model_name"], source="drift_retrain_events", detail=event["trigger_type"])
            regime = metrics.get("regime") or diagnostics.get("regime")
            if regime:
                _add_regime(affected, str(regime), source="drift_retrain_events", detail=event["model_name"])

    return (
        _source_state(
            available=bool(context["drift_retrain_status"] or context["drift_retrain_events"]),
            rows=rows_total,
            ts_ms=latest_ts_ms,
            now_ms=now_ms,
            stale_after_ms=stale_after_ms,
            reason=(
                "ok"
                if context["drift_retrain_status"] or context["drift_retrain_events"]
                else "no drift retrain status or events"
            ),
            endpoint="/api/model/diagnostics",
        ),
        context,
    )


def build_drift_explainer_snapshot(
    *,
    con=None,
    top_n: int = DEFAULT_TOP_N,
    now_ms: int | None = None,
    stale_after_ms: int = DEFAULT_STALE_AFTER_MS,
) -> dict[str, Any]:
    """Build a read-only explanation from already-persisted drift outputs."""
    owns = con is None
    if con is None:
        con = connect_ro_direct()

    now = int(now_ms if now_ms is not None else _now_ms())
    top_n = max(1, min(25, int(top_n or DEFAULT_TOP_N)))
    stale_after_ms = max(1, int(stale_after_ms or DEFAULT_STALE_AFTER_MS))

    contributors: list[dict[str, Any]] = []
    affected: dict[str, Any] = {"symbols": [], "models": [], "regimes": [], "time_slices": []}
    unavailable: list[dict[str, str]] = []
    sources: dict[str, Any] = {}
    current: dict[str, Any] = {}

    try:
        sources["equity_drift"], current["equity_drift"] = _read_equity_drift(
            con,
            now_ms=now,
            stale_after_ms=stale_after_ms,
            contributors=contributors,
        )
        sources["model_drift"] = _read_model_drift(
            con,
            top_n=top_n,
            now_ms=now,
            stale_after_ms=stale_after_ms,
            contributors=contributors,
            affected=affected,
        )
        sources["feature_distribution_drift"] = _read_feature_distribution_drift(
            con,
            top_n=top_n,
            now_ms=now,
            stale_after_ms=stale_after_ms,
            contributors=contributors,
        )
        sources["residual_distribution_drift"] = _read_residual_distribution_drift(
            con,
            top_n=top_n,
            now_ms=now,
            stale_after_ms=stale_after_ms,
            contributors=contributors,
            affected=affected,
        )
        sources["production_monitoring"], current["production_monitoring"] = _read_production_monitoring(
            con,
            top_n=top_n,
            now_ms=now,
            stale_after_ms=stale_after_ms,
            contributors=contributors,
        )
        current["distribution_drift"] = _read_distribution_state(con) or {}
        sources["monte_carlo_risk"], current["monte_carlo_risk"] = _read_risk_state(
            con,
            now_ms=now,
            stale_after_ms=stale_after_ms,
            affected=affected,
        )
        sources["model_diagnostics"], current["model_diagnostics"] = _read_model_diagnostics_context(
            con,
            top_n=top_n,
            now_ms=now,
            stale_after_ms=stale_after_ms,
            affected=affected,
        )

        for source_name, state in sources.items():
            if not bool((state or {}).get("available")):
                _append_unavailable(unavailable, f"sources.{source_name}", str((state or {}).get("reason") or "unavailable"))

        if not sources["feature_distribution_drift"].get("rows"):
            _append_unavailable(
                unavailable,
                "top_contributing_features",
                "No feature_distribution_drift rows are available.",
            )
        if not sources["model_drift"].get("rows"):
            _append_unavailable(
                unavailable,
                "top_contributing_model_dimensions",
                "No model_drift rows are available.",
            )
        if not affected.get("models"):
            _append_unavailable(
                unavailable,
                "affected.models",
                "No model-keyed drift attribution is available; model_drift is keyed by symbol and horizon only.",
            )
        if not affected.get("regimes"):
            _append_unavailable(
                unavailable,
                "affected.regimes",
                "No regime-keyed drift attribution is available in the current diagnostics payload.",
            )

        affected["time_slices"] = [
            {
                "label": str(item.get("dimension") or ""),
                "source": str(item.get("source") or ""),
                "ts_ms": int(item.get("ts_ms") or 0),
            }
            for item in contributors
            if int(item.get("ts_ms") or 0) > 0
        ][:top_n]

        distribution_state = _normalize_severity(str((current.get("distribution_drift") or {}).get("state") or ""))
        explicit_severities = [_normalize_severity(str(item.get("severity") or "")) for item in contributors]
        if distribution_state in {"CRIT", "WARN"}:
            explicit_severities.append(distribution_state)
        max_severity = max(explicit_severities or ["UNKNOWN"], key=_severity_rank)
        max_severity = _normalize_severity(max_severity)

        latest_ts_ms = max(
            [int(item.get("ts_ms") or 0) for item in contributors]
            + [int((state or {}).get("ts_ms") or 0) for state in sources.values()],
            default=0,
        )
        stale = bool(latest_ts_ms > 0 and _is_stale(latest_ts_ms, now, stale_after_ms))
        active = _severity_rank(max_severity) >= _severity_rank("WARN")
        if latest_ts_ms <= 0:
            state = "unavailable"
            status_severity = "UNKNOWN"
        elif stale:
            state = "stale"
            status_severity = "STALE"
        elif active:
            state = "active"
            status_severity = max_severity
        else:
            state = "normal"
            status_severity = "OK"

        contributors.sort(
            key=lambda item: (
                _severity_rank(str(item.get("severity") or "")),
                abs(float(_safe_float(item.get("metric_value"), 0.0) or 0.0)),
                abs(float(_safe_float(item.get("delta_pct"), 0.0) or 0.0)),
                int(item.get("ts_ms") or 0),
            ),
            reverse=True,
        )

        return {
            "ok": True,
            "schema_version": 1,
            "ts_ms": int(now),
            "status": {
                "state": state,
                "severity": status_severity,
                "active": bool(active),
                "stale": bool(stale),
                "latest_ts_ms": int(latest_ts_ms),
                "stale_after_ms": int(stale_after_ms),
                "reason": (
                    "No drift is flagged in available sources."
                    if state == "normal"
                    else "One or more drift sources are flagged."
                    if state == "active"
                    else "Latest drift source data is stale."
                    if state == "stale"
                    else "No drift source data is available."
                ),
            },
            "contributors": contributors[:top_n],
            "affected": affected,
            "current": current,
            "sources": sources,
            "unavailable": unavailable,
            "related_links": [
                {"label": "Equity drift", "panel_id": "equityDriftPanel", "screen": "explain"},
                {"label": "Model divergence", "panel_id": "performanceDivergenceCard", "screen": "analyze"},
                {"label": "Decision log", "panel_id": "recentDecisionsCard", "screen": "explain"},
                {"label": "Risk", "panel_id": "positionsExposureSummaryCard", "screen": "positions"},
            ],
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("DRIFT_EXPLAINER_CLOSE_FAILED", e, once_key="close_failed")
