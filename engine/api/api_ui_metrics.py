"""
Canonical UI metrics adapter.

This module does not introduce new broker/account semantics.  It normalizes
existing read-only dashboard sources into one stable shape for top-level UI
cards so individual panels do not independently derive PnL and exposure.
"""

from __future__ import annotations

import time
from typing import Any


ROUTE_SPECS_UI_METRICS = [
    ("GET", "/api/ui/metrics", "api_get_ui_metrics"),
]

DEFAULT_STALE_MS = 5 * 60 * 1000


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _num(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        out = float(value)
    except Exception:
        return None
    return out if out == out and out not in (float("inf"), float("-inf")) else None


def _int_ts(value: Any) -> int | None:
    n = _num(value)
    if n is None or n <= 0:
        return None
    return int(n)


def _pick_num(*values: Any) -> float | None:
    for value in values:
        n = _num(value)
        if n is not None:
            return n
    return None


def _pick_ts(*values: Any) -> int | None:
    for value in values:
        ts_ms = _int_ts(value)
        if ts_ms is not None:
            return ts_ms
    return None


def _latest_ts_from_rows(rows: list[Any], *keys: str) -> int | None:
    latest = 0
    for row in rows:
        item = _as_dict(row)
        for key in keys:
            latest = max(latest, int(_int_ts(item.get(key)) or 0))
    return latest or None


def _source_state(
    *,
    endpoint: str,
    payload: Any,
    ts_ms: int | None,
    now_ms: int,
    stale_after_ms: int,
    missing: bool = False,
    reason: str = "",
) -> dict[str, Any]:
    item = _as_dict(payload)
    ok = bool(item.get("ok", True)) if item else False
    error = item.get("error") if item else None
    source_missing = bool(missing or not item or ok is False)
    age_ms = (max(0, int(now_ms) - int(ts_ms)) if ts_ms else None)
    stale = bool(age_ms is not None and age_ms >= int(stale_after_ms))
    return {
        "endpoint": str(endpoint),
        "ok": bool(ok and not source_missing),
        "missing": bool(source_missing),
        "stale": bool(stale),
        "ts_ms": int(ts_ms or 0),
        "age_ms": age_ms,
        "reason": str(reason or (error or ("missing" if source_missing else "ok"))),
        "error": str(error) if error else None,
    }


def _normalize_pnl(pnl_payload: Any, pnl_summary_payload: Any) -> tuple[dict[str, Any], int | None, bool, str]:
    pnl = _as_dict(pnl_payload)
    summary = _as_dict(pnl_summary_payload)
    data = _as_dict(pnl.get("data")) or pnl
    source_name = str(data.get("source") or pnl.get("source") or "").strip().lower()

    today = _pick_num(
        data.get("day_pnl"),
        data.get("daily_pnl"),
        summary.get("day_pnl"),
        summary.get("daily_pnl"),
        data.get("total"),
        data.get("total_pnl"),
        summary.get("total_pnl"),
    )
    total = _pick_num(data.get("total"), data.get("total_pnl"), summary.get("total_pnl"), today)
    realized = _pick_num(data.get("realized"), data.get("realized_pnl"), summary.get("realized"))
    unrealized = _pick_num(data.get("unrealized"), data.get("unrealized_pnl"), summary.get("unrealized"))
    ts_ms = _pick_ts(data.get("ts_ms"), pnl.get("ts_ms"), summary.get("ts_ms"))
    missing = bool(
        pnl.get("ok") is False
        or source_name == "missing"
        or (today is None and total is None and realized is None and unrealized is None)
    )
    return (
        {
            "today_pnl": today,
            "total_pnl": total,
            "realized_pnl": realized,
            "unrealized_pnl": unrealized,
            "source": str(data.get("source") or pnl.get("source") or "pnl"),
        },
        ts_ms,
        missing,
        "pnl_source_missing" if missing else "ok",
    )


def _normalize_account(broker_payload: Any) -> tuple[dict[str, Any], int | None, bool, str]:
    broker = _as_dict(broker_payload)
    account = _as_dict(broker.get("account"))
    cash = _pick_num(account.get("cash"))
    equity = _pick_num(account.get("equity"))
    ts_ms = _pick_ts(account.get("updated_ts_ms"), account.get("ts_ms"), broker.get("ts_ms"))
    missing = bool(broker.get("ok") is False or (cash is None and equity is None))
    return (
        {
            "cash": cash,
            "equity": equity,
            "source": "/api/broker.account",
        },
        ts_ms,
        missing,
        "account_source_missing" if missing else "ok",
    )


def _normalize_positions(
    portfolio_payload: Any,
    broker_payload: Any,
    terminal_positions_payload: Any,
) -> tuple[dict[str, Any], int | None, bool, str]:
    portfolio = _as_dict(portfolio_payload)
    broker = _as_dict(broker_payload)
    terminal = _as_dict(terminal_positions_payload)
    state_rows = _as_list(portfolio.get("state"))
    order_rows = _as_list(portfolio.get("orders"))
    broker_rows = _as_list(broker.get("positions"))
    terminal_rows = _as_list(terminal.get("rows"))

    live_symbols = {
        str(_as_dict(row).get("symbol") or "").strip().upper()
        for row in [*broker_rows, *terminal_rows]
        if str(_as_dict(row).get("symbol") or "").strip()
    }
    ts_ms = max(
        int(_latest_ts_from_rows(state_rows, "updated_ts_ms", "ts_ms") or 0),
        int(_latest_ts_from_rows(broker_rows, "updated_ts_ms", "ts_ms") or 0),
        int(_latest_ts_from_rows(terminal_rows, "updated_ts_ms", "ts_ms") or 0),
        int(_pick_ts(_as_dict(portfolio.get("meta")).get("orders_batch_ts_ms")) or 0),
    ) or None
    portfolio_meta = _as_dict(portfolio.get("meta"))
    portfolio_missing = bool(portfolio.get("ok") is False or portfolio_meta.get("ready") is False)
    live_missing = bool(broker.get("ok") is False and terminal.get("ok") is False)
    missing = bool(portfolio_missing and live_missing)
    return (
        {
            "target_count": int(len(state_rows)),
            "order_count": int(len(order_rows)),
            "live_count": int(len(live_symbols)),
            "broker_position_count": int(len(broker_rows)),
            "terminal_position_count": int(len(terminal_rows)),
            "source": "portfolio+broker+terminal",
        },
        ts_ms,
        missing,
        "positions_source_missing" if missing else "ok",
    )


def _normalize_exposure_and_risk(
    risk_summary_payload: Any,
    portfolio_risk_payload: Any,
) -> tuple[dict[str, Any], dict[str, Any], int | None, bool, str]:
    risk_summary = _as_dict(risk_summary_payload)
    portfolio_risk = _as_dict(portfolio_risk_payload)
    risk_history = _as_list(portfolio_risk.get("history"))
    risk_latest = _as_dict(risk_history[0] if risk_history else {})
    risk_summary_block = _as_dict(portfolio_risk.get("summary"))

    gross = _pick_num(
        risk_summary.get("gross_exposure"),
        risk_latest.get("gross"),
        risk_summary_block.get("gross"),
    )
    net = _pick_num(
        risk_summary.get("net_exposure"),
        risk_latest.get("net"),
        risk_summary_block.get("net"),
    )
    drawdown = _pick_num(risk_summary.get("max_drawdown_pct"), risk_latest.get("drawdown"))
    barrier = _as_dict(risk_summary.get("execution_barrier"))
    risk_status = str(portfolio_risk.get("status") or "unknown")
    blocked = (
        bool(portfolio_risk.get("blocked"))
        if "blocked" in portfolio_risk
        else (not bool(barrier.get("allowed")) if "allowed" in barrier else None)
    )
    ts_ms = _pick_ts(
        risk_summary.get("ts_ms"),
        portfolio_risk.get("ts_ms"),
        risk_latest.get("ts_ms"),
    )
    missing = bool(
        (risk_summary.get("ok") is False and portfolio_risk.get("ok") is False)
        or (gross is None and net is None and drawdown is None and not barrier and not risk_history)
    )
    exposure = {
        "gross": gross,
        "net": net,
        "source": "risk_summary",
    }
    risk = {
        "max_drawdown_pct": drawdown,
        "status": risk_status,
        "blocked": blocked,
        "execution_barrier": barrier,
        "ready": bool(portfolio_risk.get("ready", bool(risk_history or risk_summary_block))),
        "source": "risk_summary+portfolio_risk",
    }
    return exposure, risk, ts_ms, missing, "risk_source_missing" if missing else "ok"


def build_ui_metrics_snapshot(
    *,
    pnl: Any = None,
    pnl_summary: Any = None,
    portfolio: Any = None,
    risk_summary: Any = None,
    portfolio_risk: Any = None,
    broker: Any = None,
    terminal_positions: Any = None,
    now_ms: int | None = None,
    stale_after_ms: int = DEFAULT_STALE_MS,
) -> dict[str, Any]:
    """Build the canonical UI metrics payload from existing endpoint payloads."""

    now = int(now_ms or time.time() * 1000)
    stale_ms = int(stale_after_ms or DEFAULT_STALE_MS)

    pnl_metrics, pnl_ts, pnl_missing, pnl_reason = _normalize_pnl(pnl, pnl_summary)
    account_metrics, account_ts, account_missing, account_reason = _normalize_account(broker)
    positions_metrics, positions_ts, positions_missing, positions_reason = _normalize_positions(
        portfolio,
        broker,
        terminal_positions,
    )
    exposure_metrics, risk_metrics, risk_ts, risk_missing, risk_reason = _normalize_exposure_and_risk(
        risk_summary,
        portfolio_risk,
    )

    sources = {
        "pnl": _source_state(
            endpoint="/api/pnl",
            payload=pnl,
            ts_ms=pnl_ts,
            now_ms=now,
            stale_after_ms=stale_ms,
            missing=pnl_missing,
            reason=pnl_reason,
        ),
        "pnl_summary": _source_state(
            endpoint="/api/pnl/summary",
            payload=pnl_summary,
            ts_ms=_pick_ts(_as_dict(pnl_summary).get("ts_ms")),
            now_ms=now,
            stale_after_ms=stale_ms,
            missing=not bool(_as_dict(pnl_summary)),
        ),
        "portfolio": _source_state(
            endpoint="/api/portfolio",
            payload=portfolio,
            ts_ms=positions_ts,
            now_ms=now,
            stale_after_ms=stale_ms,
            missing=positions_missing,
            reason=positions_reason,
        ),
        "risk_summary": _source_state(
            endpoint="/api/risk/summary",
            payload=risk_summary,
            ts_ms=risk_ts,
            now_ms=now,
            stale_after_ms=stale_ms,
            missing=risk_missing,
            reason=risk_reason,
        ),
        "portfolio_risk": _source_state(
            endpoint="/api/risk/portfolio",
            payload=portfolio_risk,
            ts_ms=_pick_ts(_as_dict(portfolio_risk).get("ts_ms"), risk_ts),
            now_ms=now,
            stale_after_ms=stale_ms,
            missing=not bool(_as_dict(portfolio_risk)),
        ),
        "broker": _source_state(
            endpoint="/api/broker",
            payload=broker,
            ts_ms=account_ts,
            now_ms=now,
            stale_after_ms=stale_ms,
            missing=account_missing,
            reason=account_reason,
        ),
        "terminal_positions": _source_state(
            endpoint="/api/terminal/positions",
            payload=terminal_positions,
            ts_ms=_latest_ts_from_rows(_as_list(_as_dict(terminal_positions).get("rows")), "updated_ts_ms", "ts_ms"),
            now_ms=now,
            stale_after_ms=stale_ms,
            missing=not bool(_as_dict(terminal_positions)),
        ),
    }

    missing_sources = sorted(name for name, source in sources.items() if source.get("missing"))
    stale_sources = sorted(name for name, source in sources.items() if source.get("stale"))
    source_ts_values = [int(source.get("ts_ms") or 0) for source in sources.values()]
    source_ts_values = [value for value in source_ts_values if value > 0]

    return {
        "ok": True,
        "schema_version": 1,
        "ts_ms": now,
        "stale_after_ms": stale_ms,
        "pnl": pnl_metrics,
        "exposure": exposure_metrics,
        "positions": positions_metrics,
        "account": account_metrics,
        "risk": risk_metrics,
        "sources": sources,
        "summary": {
            "degraded": bool(missing_sources or stale_sources),
            "missing_sources": missing_sources,
            "stale_sources": stale_sources,
            "source_ts_ms": max(source_ts_values) if source_ts_values else 0,
        },
        # Top-level aliases keep simple dashboard/header consumers stable.
        "today_pnl": pnl_metrics.get("today_pnl"),
        "realized_pnl": pnl_metrics.get("realized_pnl"),
        "unrealized_pnl": pnl_metrics.get("unrealized_pnl"),
        "gross_exposure": exposure_metrics.get("gross"),
        "net_exposure": exposure_metrics.get("net"),
        "cash": account_metrics.get("cash"),
        "equity": account_metrics.get("equity"),
    }
