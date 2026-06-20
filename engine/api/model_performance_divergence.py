"""
Read-only aggregation for expected/shadow/live performance divergence.

This module deliberately does not score trades or run backtests. It composes
existing dashboard/read endpoints into a compact UI payload so operators can see
model decay without manually comparing several raw routes.
"""

from __future__ import annotations

import math
import time
from typing import Any


RETURN_WATCH_ABS = 0.02
RETURN_DIVERGED_ABS = 0.05
HIT_RATE_WATCH_ABS = 0.05
HIT_RATE_DIVERGED_ABS = 0.10
SLIPPAGE_WATCH_BPS = 2.0
SLIPPAGE_DIVERGED_BPS = 5.0
FILL_RATE_WATCH_ABS = 0.05
FILL_RATE_DIVERGED_ABS = 0.10


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _safe_int(value: Any) -> int | None:
    try:
        out = int(value)
    except Exception:
        return None
    return out


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _rows_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [_as_dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("rows", "items", "data", "history"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [_as_dict(row) for row in rows if isinstance(row, dict)]
    return []


def _first_number(mapping: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in mapping:
            value = _safe_float(mapping.get(key))
            if value is not None:
                return value
    return None


def _pct_decimal(mapping: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = _first_number(mapping, key)
        if value is None:
            continue
        if "pct" in key.lower() and abs(value) > 1.0:
            return value / 100.0
        return value
    return None


def _max_ts(rows: list[dict[str, Any]], *keys: str) -> int | None:
    candidates: list[int] = []
    for row in rows:
        for key in keys or ("ts_ms",):
            ts = _safe_int(row.get(key))
            if ts and ts > 0:
                candidates.append(ts)
    return max(candidates) if candidates else None


def _source(ok: bool, *, ts_ms: int | None = None, count: int = 0, reason: str = "") -> dict[str, Any]:
    return {
        "ok": bool(ok),
        "ts_ms": int(ts_ms) if ts_ms else None,
        "count": int(count or 0),
        "reason": str(reason or ""),
    }


def _value(value: float | None, *, source: str, ts_ms: int | None, unit: str) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "value": float(value),
        "source": str(source),
        "ts_ms": int(ts_ms) if ts_ms else None,
        "unit": str(unit),
    }


def _latest_registry_row(payload: Any, requested_model_id: str = "") -> dict[str, Any]:
    data = _as_dict(payload)
    rows = _rows_from_payload(payload)
    requested = str(requested_model_id or "").strip()
    if requested:
        for row in rows:
            if str(row.get("model_name") or row.get("model_id") or "").strip() == requested:
                return row
    champion = _as_dict(data.get("champion"))
    if champion:
        return champion
    for row in rows:
        if str(row.get("stage") or "").strip().lower() == "champion":
            return row
    return rows[0] if rows else {}


def _select_shadow_row(rows: list[dict[str, Any]], *, model_id: str = "", strategy: str = "") -> dict[str, Any]:
    wanted_model = str(model_id or "").strip()
    wanted_strategy = str(strategy or "").strip()

    def _matches(row: dict[str, Any]) -> bool:
        detail = _as_dict(row.get("detail"))
        candidates = {
            str(row.get("model_name") or "").strip(),
            str(row.get("model_id") or "").strip(),
            str(detail.get("model_name") or "").strip(),
            str(detail.get("model_id") or "").strip(),
        }
        strategy_candidates = {
            str(row.get("strategy_name") or "").strip(),
            str(row.get("strategy") or "").strip(),
            str(detail.get("strategy_name") or "").strip(),
            str(detail.get("strategy") or "").strip(),
        }
        return bool(
            (wanted_model and wanted_model in candidates)
            or (wanted_strategy and wanted_strategy in strategy_candidates)
        )

    for row in rows:
        if _matches(row):
            return row
    return rows[0] if rows else {}


def _backtest_return_and_hit(backtest_payload: Any) -> tuple[float | None, float | None, int | None]:
    payload = _as_dict(backtest_payload)
    run = _as_dict(payload.get("run"))
    metrics = _as_dict(run.get("metrics"))
    points = run.get("points") if isinstance(run.get("points"), list) else []
    ts_ms = _safe_int(run.get("ts_ms")) or _safe_int(run.get("end_ts_ms"))

    expected_return = _pct_decimal(
        metrics,
        "total_return",
        "return",
        "portfolio_return",
        "return_pct",
        "total_return_pct",
    )
    if expected_return is None and len(points) >= 2:
        first = _safe_float(_as_dict(points[0]).get("equity"))
        last = _safe_float(_as_dict(points[-1]).get("equity"))
        if first is not None and first > 0 and last is not None:
            expected_return = (last / first) - 1.0

    expected_hit = _pct_decimal(
        metrics,
        "hit_rate",
        "win_rate",
        "directional_acc",
        "directional_accuracy",
    )
    return expected_return, expected_hit, ts_ms


def _realized_return(pnl_payload: Any, pnl_summary_payload: Any | None = None) -> tuple[float | None, int | None]:
    payload = _as_dict(pnl_payload)
    summary = _as_dict(pnl_summary_payload)
    data = _as_dict(payload.get("data")) or payload
    ts_ms = _safe_int(data.get("ts_ms")) or _safe_int(payload.get("ts_ms")) or _safe_int(summary.get("ts_ms"))

    direct = _pct_decimal(
        data,
        "total_return",
        "return",
        "return_pct",
        "realized_return",
        "realized_return_pct",
    )
    if direct is not None:
        return direct, ts_ms

    pnl = _first_number(data, "total_pnl", "total", "day_pnl", "daily_pnl")
    if pnl is None:
        pnl = _first_number(summary, "total_pnl", "day_pnl", "daily_pnl")

    denominator = _first_number(
        data,
        "starting_equity",
        "initial_equity",
        "equity_baseline",
        "capital",
        "capital_base",
    )
    equity = _first_number(data, "equity", "account_equity")
    if denominator is None and equity is not None and pnl is not None:
        implied_start = equity - pnl
        if implied_start > 0:
            denominator = implied_start

    if pnl is None or denominator is None or denominator <= 0:
        return None, ts_ms
    return pnl / denominator, ts_ms


def _expected_slippage(
    shadow_row: dict[str, Any],
    advisory_payload: Any,
    registry_row: dict[str, Any],
) -> tuple[float | None, int | None, str]:
    items = _rows_from_payload(advisory_payload)
    values = [_safe_float(item.get("expected_slippage_bps")) for item in items]
    values = [value for value in values if value is not None]
    if values:
        return sum(values) / float(len(values)), _max_ts(items, "ts_ms"), "execution_advisories"

    shadow = _first_number(shadow_row, "avg_slippage_impact", "expected_slippage_bps")
    if shadow is not None:
        return shadow, _safe_int(shadow_row.get("ts_ms")), "temporal_shadow_eval"

    metrics = _as_dict(registry_row.get("metrics"))
    registry = _first_number(metrics, "expected_slippage_bps", "avg_slippage_bps")
    if registry is not None:
        return registry, _safe_int(registry_row.get("created_ts_ms")) or _safe_int(registry_row.get("model_ts_ms")), "model_registry"
    return None, None, ""


def _realized_slippage(execution_metrics_payload: Any, execution_stats_payload: Any) -> tuple[float | None, int | None]:
    metrics = _as_dict(execution_metrics_payload)
    stats = _as_dict(execution_stats_payload)
    value = _first_number(metrics, "avg_slippage_bps", "avg_slippage")
    if value is None:
        value = _first_number(_as_dict(stats.get("metrics")), "avg_slippage_bps", "avg_slippage")
    return value, _safe_int(stats.get("ts_ms")) or _safe_int(_as_dict(stats.get("fills")).get("last_fill_ts_ms"))


def _expected_fill_rate(registry_row: dict[str, Any]) -> tuple[float | None, int | None]:
    metrics = _as_dict(registry_row.get("metrics"))
    value = _pct_decimal(metrics, "expected_fill_rate", "fill_rate", "fill_rate_expected")
    ts_ms = _safe_int(registry_row.get("created_ts_ms")) or _safe_int(registry_row.get("model_ts_ms"))
    return value, ts_ms


def _realized_fill_rate(execution_stats_payload: Any) -> tuple[float | None, int | None]:
    stats = _as_dict(execution_stats_payload)
    orders = _as_dict(stats.get("orders"))
    fills = _as_dict(stats.get("fills"))
    total_orders = _safe_float(orders.get("total"))
    total_fills = _safe_float(fills.get("total"))
    if total_orders is None or total_orders <= 0 or total_fills is None:
        return None, _safe_int(stats.get("ts_ms")) or _safe_int(fills.get("last_fill_ts_ms"))
    return min(1.0, max(0.0, total_fills / total_orders)), _safe_int(fills.get("last_fill_ts_ms")) or _safe_int(stats.get("ts_ms"))


def _registry_hit(registry_row: dict[str, Any]) -> tuple[float | None, int | None]:
    metrics = _as_dict(registry_row.get("metrics"))
    value = _pct_decimal(metrics, "live_hit_rate", "realized_hit_rate", "hit_rate_live", "win_rate_live")
    ts_ms = _safe_int(registry_row.get("created_ts_ms")) or _safe_int(registry_row.get("model_ts_ms"))
    return value, ts_ms


def _classify(metric: str, expected: float | None, realized: float | None) -> tuple[str, float | None, str]:
    if expected is None or realized is None:
        missing = []
        if expected is None:
            missing.append("expected")
        if realized is None:
            missing.append("realized")
        return "incomplete", None, f"Missing {' and '.join(missing)} {metric.replace('_', ' ')} source."

    delta = realized - expected
    if metric == "return":
        if expected > 0 and realized < 0:
            return "diverged", delta, "Live return is negative while expected return is positive."
        if abs(delta) >= max(RETURN_DIVERGED_ABS, abs(expected) * 0.75):
            return "diverged", delta, "Live return materially diverges from the expected range."
        if abs(delta) >= max(RETURN_WATCH_ABS, abs(expected) * 0.40):
            return "watch", delta, "Live return is drifting away from the expected range."
        return "ok", delta, "Live return is broadly aligned with expected performance."

    if metric == "hit_rate":
        if delta <= -HIT_RATE_DIVERGED_ABS:
            return "diverged", delta, "Live hit rate is materially below expected hit rate."
        if delta <= -HIT_RATE_WATCH_ABS:
            return "watch", delta, "Live hit rate is below expected hit rate."
        return "ok", delta, "Hit rate is aligned where comparable data is available."

    if metric == "slippage_bps":
        if delta >= SLIPPAGE_DIVERGED_BPS or (expected > 0 and realized >= expected * 1.75 and delta >= SLIPPAGE_WATCH_BPS):
            return "diverged", delta, "Realized slippage is materially worse than expected."
        if delta >= SLIPPAGE_WATCH_BPS:
            return "watch", delta, "Realized slippage is above expected slippage."
        return "ok", delta, "Realized slippage is within the expected range."

    if metric == "fill_rate":
        if delta <= -FILL_RATE_DIVERGED_ABS:
            return "diverged", delta, "Realized fill rate is materially below expectation."
        if delta <= -FILL_RATE_WATCH_ABS:
            return "watch", delta, "Realized fill rate is below expectation."
        return "ok", delta, "Fill rate is aligned where comparable data is available."

    return "ok", delta, "Comparable data is aligned."


def _comparison(
    key: str,
    label: str,
    unit: str,
    *,
    expected: tuple[float | None, int | None, str],
    realized: tuple[float | None, int | None, str],
    shadow: tuple[float | None, int | None, str] | None = None,
) -> dict[str, Any]:
    expected_value, expected_ts, expected_source = expected
    realized_value, realized_ts, realized_source = realized
    status, delta, explanation = _classify(key, expected_value, realized_value)
    return {
        "key": key,
        "label": label,
        "unit": unit,
        "expected": _value(expected_value, source=expected_source, ts_ms=expected_ts, unit=unit),
        "shadow": (
            _value(shadow[0], source=shadow[2], ts_ms=shadow[1], unit=unit)
            if shadow is not None
            else None
        ),
        "realized": _value(realized_value, source=realized_source, ts_ms=realized_ts, unit=unit),
        "delta": delta,
        "status": status,
        "explanation": explanation,
    }


def _monitor_status(severity: str, state: str) -> str:
    sev = str(severity or "").upper()
    st = str(state or "").lower()
    if sev == "CRIT" or st == "crit":
        return "diverged"
    if sev == "WARN" or st == "warn":
        return "watch"
    if st in {"ok", "normal"} or sev == "OK":
        return "ok"
    return "incomplete"


def _monitor_unit(metric_name: str) -> str:
    name = str(metric_name or "").lower()
    if any(token in name for token in ("rate", "coverage", "ece", "pnl")):
        return "pct"
    return ""


def _monitor_label(metric_name: str) -> str:
    labels = {
        "feature_drift": "Feature Drift",
        "missing_feature_rate": "Missing Features",
        "prediction_drift": "Prediction Drift",
        "target_label_drift": "Target/Label Drift",
        "calibration_ece": "Calibration ECE",
        "conformal_coverage": "Conformal Coverage",
        "shadow_live_disagreement": "Shadow/Live Disagreement",
        "net_pnl_degradation": "Net PnL Degradation",
    }
    return labels.get(str(metric_name or ""), str(metric_name or "Production Monitor").replace("_", " ").title())


def _monitor_explanation(row: dict[str, Any], status: str) -> str:
    action = str(row.get("action_signal") or "").replace("_", " ").strip()
    state = str(row.get("state") or "unavailable").replace("_", " ")
    if status == "diverged":
        return f"Production monitor is critical; {action or 'review'} signal is recorded."
    if status == "watch":
        return f"Production monitor is warning; {action or 'review'} signal is recorded."
    if status == "ok":
        return "Production monitor is within the configured threshold."
    return f"Production monitor is {state}; no threshold signal is emitted."


def _production_monitoring_comparisons(payload: Any) -> list[dict[str, Any]]:
    data = _as_dict(payload)
    rows = data.get("metrics") if isinstance(data.get("metrics"), list) else []
    comparisons: list[dict[str, Any]] = []
    for item in rows:
        row = _as_dict(item)
        metric_name = str(row.get("metric_name") or "").strip()
        if not metric_name:
            continue
        value = _safe_float(row.get("value"))
        baseline = _safe_float(row.get("baseline_value"))
        threshold = _safe_float(row.get("threshold_value"))
        unit = _monitor_unit(metric_name)
        status = _monitor_status(str(row.get("severity") or ""), str(row.get("state") or ""))
        expected_value = threshold
        if metric_name == "conformal_coverage" and baseline is not None:
            expected_value = baseline
        comparisons.append(
            {
                "key": metric_name,
                "label": _monitor_label(metric_name),
                "unit": unit,
                "expected": _value(expected_value, source="production_monitoring_threshold", ts_ms=_safe_int(row.get("ts_ms")), unit=unit),
                "shadow": None,
                "realized": _value(value, source="production_monitoring", ts_ms=_safe_int(row.get("ts_ms")), unit=unit),
                "delta": (None if value is None or baseline is None else float(value) - float(baseline)),
                "status": status,
                "explanation": _monitor_explanation(row, status),
                "details": dict(row.get("details") or {}),
            }
        )
    return comparisons


def build_model_performance_divergence(
    *,
    model_id: str = "",
    strategy: str = "",
    shadow_payload: Any = None,
    backtest_payload: Any = None,
    pnl_payload: Any = None,
    pnl_summary_payload: Any = None,
    execution_metrics_payload: Any = None,
    execution_stats_payload: Any = None,
    execution_advisories_payload: Any = None,
    model_registry_payload: Any = None,
    production_monitoring_payload: Any = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    now = int(now_ms if now_ms is not None else time.time() * 1000)

    registry_row = _latest_registry_row(model_registry_payload, requested_model_id=model_id)
    registry_metrics = _as_dict(registry_row.get("metrics"))
    selected_model = (
        str(model_id or "").strip()
        or str(registry_row.get("model_name") or registry_row.get("model_id") or "").strip()
        or str(_as_dict(execution_metrics_payload).get("model_id") or "").strip()
        or None
    )

    by_strategy = _as_dict(execution_metrics_payload).get("by_strategy")
    top_strategy = ""
    if isinstance(by_strategy, list) and by_strategy:
        top_strategy = str(_as_dict(by_strategy[0]).get("strategy_name") or "").strip()
    selected_strategy = str(strategy or "").strip() or top_strategy or None

    shadow_rows = _rows_from_payload(shadow_payload)
    shadow_row = _select_shadow_row(shadow_rows, model_id=selected_model or "", strategy=selected_strategy or "")
    shadow_ts = _max_ts(shadow_rows, "ts_ms")
    shadow_hit = _first_number(shadow_row, "directional_acc", "hit_rate", "win_rate")
    shadow_slippage = _first_number(shadow_row, "avg_slippage_impact", "expected_slippage_bps")

    expected_return, expected_hit, backtest_ts = _backtest_return_and_hit(backtest_payload)
    realized_return, live_pnl_ts = _realized_return(pnl_payload, pnl_summary_payload)
    registry_live_hit, registry_live_hit_ts = _registry_hit(registry_row)
    expected_slip, expected_slip_ts, expected_slip_source = _expected_slippage(
        shadow_row,
        execution_advisories_payload,
        registry_row,
    )
    realized_slip, realized_slip_ts = _realized_slippage(execution_metrics_payload, execution_stats_payload)
    expected_fill, expected_fill_ts = _expected_fill_rate(registry_row)
    realized_fill, realized_fill_ts = _realized_fill_rate(execution_stats_payload)

    if expected_hit is None:
        expected_hit = _pct_decimal(registry_metrics, "hit_rate", "win_rate", "directional_acc")
    if expected_hit is None and shadow_hit is not None:
        expected_hit = shadow_hit

    comparisons = [
        _comparison(
            "return",
            "Return",
            "pct",
            expected=(expected_return, backtest_ts, "portfolio_backtest"),
            shadow=(None, shadow_ts, "temporal_shadow_eval"),
            realized=(realized_return, live_pnl_ts, "pnl"),
        ),
        _comparison(
            "hit_rate",
            "Hit Rate",
            "pct",
            expected=(expected_hit, backtest_ts or _safe_int(registry_row.get("created_ts_ms")), "backtest_or_registry"),
            shadow=(shadow_hit, _safe_int(shadow_row.get("ts_ms")) or shadow_ts, "temporal_shadow_eval"),
            realized=(registry_live_hit, registry_live_hit_ts, "model_registry_live_metrics"),
        ),
        _comparison(
            "slippage_bps",
            "Slippage",
            "bps",
            expected=(expected_slip, expected_slip_ts, expected_slip_source or "execution_advisories"),
            shadow=(shadow_slippage, _safe_int(shadow_row.get("ts_ms")) or shadow_ts, "temporal_shadow_eval"),
            realized=(realized_slip, realized_slip_ts, "execution_metrics"),
        ),
        _comparison(
            "fill_rate",
            "Fill Rate",
            "pct",
            expected=(expected_fill, expected_fill_ts, "model_registry"),
            realized=(realized_fill, realized_fill_ts, "execution_stats"),
        ),
    ]
    monitoring_comparisons = _production_monitoring_comparisons(production_monitoring_payload)
    comparisons.extend(monitoring_comparisons)

    sources = {
        "model_registry": _source(
            bool(_rows_from_payload(model_registry_payload) or registry_row),
            ts_ms=_safe_int(registry_row.get("created_ts_ms")) or _safe_int(registry_row.get("model_ts_ms")),
            count=len(_rows_from_payload(model_registry_payload)),
            reason="" if registry_row else "model registry returned no rows",
        ),
        "shadow_eval": _source(
            bool(shadow_rows),
            ts_ms=shadow_ts,
            count=len(shadow_rows),
            reason="" if shadow_rows else "temporal shadow evaluation returned no rows",
        ),
        "portfolio_backtest": _source(
            bool(_as_dict(backtest_payload).get("ok") and _as_dict(backtest_payload).get("run")),
            ts_ms=backtest_ts,
            count=int(_as_dict(_as_dict(backtest_payload).get("meta")).get("count") or 0),
            reason=str(_as_dict(backtest_payload).get("error") or "") if not _as_dict(backtest_payload).get("ok") else "",
        ),
        "live_pnl": _source(
            bool(_as_dict(pnl_payload).get("ok") and _as_dict(_as_dict(pnl_payload).get("data")).get("source") != "missing"),
            ts_ms=live_pnl_ts,
            count=int(_as_dict(_as_dict(pnl_payload).get("meta")).get("count") or 0),
            reason=(
                "live PnL lacks a positive equity/capital baseline for return conversion"
                if realized_return is None
                else ""
            ),
        ),
        "execution_metrics": _source(
            bool(_as_dict(execution_metrics_payload).get("ok") or _as_dict(execution_stats_payload).get("ok")),
            ts_ms=realized_slip_ts or realized_fill_ts,
            count=int(_as_dict(execution_metrics_payload).get("n_fills") or _as_dict(_as_dict(execution_stats_payload).get("fills")).get("total") or 0),
            reason="" if (_as_dict(execution_metrics_payload).get("ok") or _as_dict(execution_stats_payload).get("ok")) else "execution metrics unavailable",
        ),
        "execution_advisories": _source(
            bool(_rows_from_payload(execution_advisories_payload)),
            ts_ms=_max_ts(_rows_from_payload(execution_advisories_payload), "ts_ms"),
            count=len(_rows_from_payload(execution_advisories_payload)),
            reason="" if _rows_from_payload(execution_advisories_payload) else "execution advisories returned no rows",
        ),
        "production_monitoring": _source(
            bool(_rows_from_payload(production_monitoring_payload) or _as_dict(production_monitoring_payload).get("metrics")),
            ts_ms=_safe_int(_as_dict(production_monitoring_payload).get("updated_ts_ms")),
            count=len(_as_dict(production_monitoring_payload).get("metrics") or []),
            reason="" if _as_dict(production_monitoring_payload).get("metrics") else "production monitoring metrics unavailable",
        ),
    }

    missing_sources = [
        name
        for name, source in sources.items()
        if not bool(source.get("ok"))
    ]

    statuses = [str(row.get("status") or "incomplete") for row in comparisons]
    comparable = [status for status in statuses if status != "incomplete"]
    if "diverged" in statuses:
        state = "diverged"
        reason = "At least one live metric materially diverges from expected or shadow performance."
    elif "watch" in statuses:
        state = "watch"
        reason = "Live behavior is drifting from at least one expected metric."
    elif not comparable:
        state = "incomplete"
        reason = "Not enough comparable expected and live data is available yet."
    elif missing_sources:
        state = "partial"
        reason = "Comparable metrics are available, but at least one source is missing."
    else:
        state = "ok"
        reason = "Comparable live metrics are aligned with expected and shadow behavior."

    return {
        "ok": True,
        "selection": {
            "model_id": selected_model,
            "strategy": selected_strategy,
            "registry_stage": registry_row.get("stage") if registry_row else None,
            "registry_regime": registry_row.get("regime") if registry_row else None,
        },
        "status": {
            "state": state,
            "reason": reason,
            "ts_ms": now,
        },
        "comparisons": comparisons,
        "production_monitoring": _as_dict(production_monitoring_payload),
        "sources": sources,
        "missing_sources": missing_sources,
        "updated_ts_ms": now,
    }


def get_model_performance_divergence(*, model_id: str = "", strategy: str = "") -> dict[str, Any]:
    """Build the UI aggregation from existing read paths."""

    now_ms = int(time.time() * 1000)

    from engine.api.api_read import get_execution_metrics, get_execution_stats, get_model_registry
    from engine.api.api_read_advanced import get_latest_portfolio_backtest, get_temporal_shadow_eval
    from engine.execution.execution_ai_advisor import list_execution_advisories
    from engine.runtime.position_store import get_pnl_snapshot
    from engine.strategy.production_monitoring import get_latest_production_monitoring_snapshot

    def _call_source(fn, fallback):
        try:
            return fn()
        except Exception as e:
            out = dict(fallback) if isinstance(fallback, dict) else fallback
            if isinstance(out, dict):
                out.setdefault("ok", False)
                out.setdefault("error", f"{type(e).__name__}: {e}")
            return out

    model_filter = str(model_id or "").strip()
    pnl_data = _call_source(
        lambda: get_pnl_snapshot(model_id=model_filter or None),
        {"source": "missing", "ts_ms": 0},
    ) or {}
    pnl_payload = {
        "ok": str(_as_dict(pnl_data).get("source") or "") != "missing",
        "meta": {
            "ready": bool(pnl_data) and str(pnl_data.get("source") or "") != "missing",
            "count": int(len(pnl_data)) if isinstance(pnl_data, dict) else 0,
        },
        "data": pnl_data,
        "model_id": model_filter or None,
    }
    pnl_summary_payload = {
        "ok": True,
        "day_pnl": pnl_data.get("day_pnl", pnl_data.get("total", 0.0)),
        "daily_pnl": pnl_data.get("daily_pnl", pnl_data.get("total", 0.0)),
        "total_pnl": pnl_data.get("total", 0.0),
        "realized": pnl_data.get("realized", 0.0),
        "unrealized": pnl_data.get("unrealized", 0.0),
        "ts_ms": pnl_data.get("ts_ms") or now_ms,
    }

    return build_model_performance_divergence(
        model_id=model_filter,
        strategy=strategy,
        shadow_payload=_call_source(lambda: get_temporal_shadow_eval(limit=200), []),
        backtest_payload=_call_source(
            get_latest_portfolio_backtest,
            {"ok": False, "error": "portfolio_backtest_unavailable", "run": None},
        ),
        pnl_payload=pnl_payload,
        pnl_summary_payload=pnl_summary_payload,
        execution_metrics_payload=_call_source(
            lambda: get_execution_metrics(model_id=model_filter),
            {"ok": False, "error": "execution_metrics_unavailable"},
        ),
        execution_stats_payload=_call_source(
            lambda: get_execution_stats(model_id=model_filter),
            {"ok": False, "error": "execution_stats_unavailable"},
        ),
        execution_advisories_payload=_call_source(
            lambda: list_execution_advisories(limit=20),
            {"ok": False, "error": "execution_advisories_unavailable", "items": []},
        ),
        model_registry_payload=_call_source(
            lambda: get_model_registry(limit=50),
            {"ok": False, "error": "model_registry_unavailable", "rows": []},
        ),
        production_monitoring_payload=_call_source(
            lambda: get_latest_production_monitoring_snapshot(limit=50),
            {"ok": False, "error": "production_monitoring_unavailable", "metrics": []},
        ),
        now_ms=now_ms,
    )


__all__ = [
    "build_model_performance_divergence",
    "get_model_performance_divergence",
]
