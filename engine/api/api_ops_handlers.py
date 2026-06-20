"""HTTP handlers for ops, diagnostics, execution analytics, and governance reads.

These handlers stay thin by delegating business logic into runtime, strategy,
and execution modules while exposing operator-oriented response shapes through
the API layer.
"""

from __future__ import annotations

import time

from engine.runtime.gates import execution_gate_snapshot
from engine.api.http_parsing import qs as _qs, deny_if_shutdown
from engine.runtime.failure_diagnostics import failure_response, log_failure
from engine.runtime.logging import get_logger
from engine.runtime.state_cache import cache_get_or_load

LOG = get_logger("engine.api.api_ops_handlers")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.api.api_ops_handlers",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _failure_out(event: str, code: str, error: BaseException, **extra: object) -> dict:
    payload = failure_response(
        LOG,
        event=event,
        code=code,
        message=str(error),
        error=error,
        component="engine.api.api_ops_handlers",
        extra=extra or None,
    )
    payload.setdefault("error", str(error))
    payload.update(extra or {})
    return payload


def _parse_int(value, default, minimum=None, maximum=None):
    try:
        out = int(value)
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out

# ----------------------------
# Simple pass-through GETs
# ----------------------------

def api_get_alerts(_parsed, ctx):
    try:
        # Ops handlers mostly compose lower-level read helpers into operator-
        # oriented endpoints without duplicating query logic.
        from engine.api.api_read import get_alerts
        out = get_alerts()
        if isinstance(out, dict):
            out.setdefault("ok", True)
            return out
        return {"ok": True, "data": out}
    except Exception as e:
        payload = _failure_out("api_ops_handlers_alerts_failed", "API_OPS_HANDLERS_ALERTS_FAILED", e)
        return payload


def api_get_notifications_status(_parsed, ctx):
    try:
        from engine.runtime.alerts_notify import (
            get_notification_channel_status,
            get_runtime_health_notification_status,
        )

        return {
            "ok": True,
            "channels": get_notification_channel_status(),
            "runtime_health_alert": get_runtime_health_notification_status(),
            "ts_ms": int(time.time() * 1000),
        }
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_notifications_status_failed",
            "API_OPS_HANDLERS_NOTIFICATIONS_STATUS_FAILED",
            e,
        )
        return payload


def api_post_notifications_test(parsed, body=None, ctx=None):
    try:
        from engine.runtime.alerts_notify import send_notification_test

        payload = body if isinstance(body, dict) else {}
        qs = _qs(parsed)
        channel = str(payload.get("channel") or qs.get("channel", "") or "").strip().lower()
        actor = str(payload.get("actor") or payload.get("who") or "operator").strip() or "operator"
        source = str(payload.get("source") or "dashboard").strip() or "dashboard"

        if not channel:
            return {"ok": False, "error": "missing_channel"}

        return send_notification_test(channel, actor=actor, source=source)
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_notifications_test_failed",
            "API_OPS_HANDLERS_NOTIFICATIONS_TEST_FAILED",
            e,
        )
        return payload


def api_get_validation(_parsed, ctx):
    try:
        from engine.api.api_dashboard_reads import api_get_validation
        return api_get_validation(_parsed, ctx)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_validation_failed", "API_OPS_HANDLERS_VALIDATION_FAILED", e)
        return payload


def api_get_model_registry(parsed, ctx):
    try:
        from engine.api.api_read import get_model_registry
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "50") or "50", 50, 1, 5000)
        return get_model_registry(limit=limit)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_model_registry_failed", "API_OPS_HANDLERS_MODEL_REGISTRY_FAILED", e)
        return payload


def api_get_model_lifecycle(parsed, ctx):
    try:
        from engine.api.api_read import get_model_lifecycle_summary
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "6") or "6", 6, 1, 50)
        return get_model_lifecycle_summary(limit=limit)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_model_lifecycle_failed", "API_OPS_HANDLERS_MODEL_LIFECYCLE_FAILED", e)
        return payload


def api_get_model_performance_divergence(parsed, ctx):
    try:
        from engine.api.model_performance_divergence import get_model_performance_divergence

        qs = _qs(parsed)
        model_id = str(qs.get("model_id", "") or "").strip()
        strategy = str(qs.get("strategy", "") or "").strip()
        return get_model_performance_divergence(model_id=model_id, strategy=strategy)
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_model_performance_divergence_failed",
            "API_OPS_HANDLERS_MODEL_PERFORMANCE_DIVERGENCE_FAILED",
            e,
        )
        return payload


def api_get_embed_model_eval(parsed, ctx):
    try:
        from engine.api.api_read import get_embed_model_eval
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "500") or "500", 500, 1, 5000)
        return get_embed_model_eval(limit=limit)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_embed_model_eval_failed", "API_OPS_HANDLERS_EMBED_MODEL_EVAL_FAILED", e)
        return payload


def api_get_embed_conf_calib(parsed, ctx):
    try:
        from engine.api.api_read import get_embed_conf_calib
        qs = _qs(parsed)
        horizon_s = _parse_int(qs.get("horizon_s", "0") or "0", 0, 0)
        model_kind = str(qs.get("model_kind", "") or "")
        limit = _parse_int(qs.get("limit", "200") or "200", 200, 1, 5000)
        return get_embed_conf_calib(
            horizon_s=horizon_s,
            model_kind=model_kind,
            limit=limit,
        )
    except Exception as e:
        payload = _failure_out("api_ops_handlers_embed_conf_calib_failed", "API_OPS_HANDLERS_EMBED_CONF_CALIB_FAILED", e)
        return payload


def api_get_temporal_eval(parsed, ctx):
    try:
        from engine.api.api_read import get_temporal_eval
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "50") or "50", 50, 1, 5000)
        return get_temporal_eval(limit=limit)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_temporal_eval_failed", "API_OPS_HANDLERS_TEMPORAL_EVAL_FAILED", e)
        return payload

def api_get_temporal_models(parsed, ctx):
    from engine.api.api_dashboard_reads import api_get_temporal_models
    return api_get_temporal_models(parsed, ctx)

def api_get_model_diagnostics(_parsed, ctx):
    try:
        from engine.api.api_read_advanced import get_model_diagnostics
        return {"ok": True, "data": get_model_diagnostics()}
    except Exception as e:
        payload = _failure_out("api_ops_handlers_model_diagnostics_failed", "API_OPS_HANDLERS_MODEL_DIAGNOSTICS_FAILED", e, data={})
        return payload

def api_get_latest_portfolio_backtest(_parsed, ctx):
    try:
        from engine.api.api_read_advanced import get_latest_portfolio_backtest
        return get_latest_portfolio_backtest()
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_latest_portfolio_backtest_failed",
            "API_OPS_HANDLERS_LATEST_PORTFOLIO_BACKTEST_FAILED",
            e,
        )
        return payload

def api_get_execution_metrics(parsed, ctx):
    try:
        from engine.api.api_read import get_execution_metrics
        qs = _qs(parsed)
        model_id = str(qs.get("model_id", "") or "").strip()
        return get_execution_metrics(model_id=model_id)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_execution_metrics_failed", "API_OPS_HANDLERS_EXECUTION_METRICS_FAILED", e)
        return payload


def api_get_feeds(_parsed, ctx):
    try:
        from engine.api.api_read import get_feed_status
        return get_feed_status()
    except Exception as e:
        payload = _failure_out("api_ops_handlers_feeds_failed", "API_OPS_HANDLERS_FEEDS_FAILED", e)
        return payload


def api_get_execution_stats(parsed, ctx):
    try:
        from engine.api.api_read import get_execution_stats
        qs = _qs(parsed)
        model_id = str(qs.get("model_id", "") or "").strip()
        return get_execution_stats(model_id=model_id)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_execution_stats_failed", "API_OPS_HANDLERS_EXECUTION_STATS_FAILED", e)
        return payload


def api_get_execution_metrics_rolling(parsed, ctx):
    try:
        from engine.api.api_read_advanced import get_execution_metrics_rolling
        qs = _qs(parsed)
        model_id = str(qs.get("model_id", "") or "").strip()
        return get_execution_metrics_rolling(model_id=model_id)
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_execution_metrics_rolling_failed",
            "API_OPS_HANDLERS_EXECUTION_METRICS_ROLLING_FAILED",
            e,
        )
        return payload


def api_get_execution_metrics_by_symbol(parsed, ctx):
    try:
        from engine.api.api_read_advanced import get_execution_metrics_by_symbol
        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "50") or "50", 50, 1, 5000)
        model_id = str(qs.get("model_id", "") or "").strip()
        return get_execution_metrics_by_symbol(limit=limit, model_id=model_id)
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_execution_metrics_by_symbol_failed",
            "API_OPS_HANDLERS_EXECUTION_METRICS_BY_SYMBOL_FAILED",
            e,
        )
        return payload

def api_get_execution_cost_by_confidence(parsed, ctx):
    try:
        from engine.api.api_read_advanced import get_execution_cost_by_confidence
        qs = _qs(parsed)
        model_id = str(qs.get("model_id", "") or "").strip()
        return get_execution_cost_by_confidence(model_id=model_id)
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_execution_cost_by_confidence_failed",
            "API_OPS_HANDLERS_EXECUTION_COST_BY_CONFIDENCE_FAILED",
            e,
        )
        return payload


def api_get_execution_diagnostics(parsed, ctx):
    try:
        from engine.execution.execution_diagnostics import build_execution_diagnostics

        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "50") or "50", 50, 1, 500)
        symbol = str(qs.get("symbol", "") or "").strip().upper()
        return build_execution_diagnostics(limit=limit, symbol=symbol or None)
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_execution_diagnostics_failed",
            "API_OPS_HANDLERS_EXECUTION_DIAGNOSTICS_FAILED",
            e,
            inventory={"routes": [], "summary": {}},
            tca={"state": "unavailable", "reason": str(e), "by_symbol": [], "rolling": [], "partial_fills": []},
            order_flow={"state": "unavailable", "partial_fills": [], "rejected_intents": [], "suppressed_intents": []},
            lob={"state": "unavailable", "warnings": [str(e)]},
            learned_slicing={"state": "unavailable", "reason": str(e)},
            drilldowns=[],
        )
        return payload


def api_get_execution_advisories(parsed, ctx):
    try:
        from engine.execution.execution_ai_advisor import list_execution_advisories

        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "20") or "20", 20, 1, 200)
        return list_execution_advisories(limit=limit)
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_execution_advisories_failed",
            "API_OPS_HANDLERS_EXECUTION_ADVISORIES_FAILED",
            e,
            items=[],
            summary={},
        )
        return payload

def api_get_social_features(parsed, ctx):
    try:
        from engine.api.api_read_advanced import get_social_features
        qs = _qs(parsed)
        symbol = str(qs.get("symbol", "") or "").strip()
        if not symbol:
            return {"ok": False, "error": "missing_symbol"}
        limit = _parse_int(qs.get("limit", "200") or "200", 200, 1, 5000)
        return get_social_features(symbol=symbol, limit=limit)
    except Exception as e:
        payload = _failure_out("api_ops_handlers_social_features_failed", "API_OPS_HANDLERS_SOCIAL_FEATURES_FAILED", e)
        return payload

def api_get_social_regimes(parsed, ctx):
    from engine.api.api_dashboard_reads import api_get_social_regimes
    return api_get_social_regimes(parsed, ctx)


def api_get_social_blocks(parsed, ctx):
    from engine.api.api_dashboard_reads import api_get_social_blocks
    return api_get_social_blocks(parsed, ctx)


def api_get_confidence_mass(_parsed, ctx):
    try:
        from engine.api.api_read import get_confidence_mass
        return get_confidence_mass()
    except Exception as e:
        payload = _failure_out("api_ops_handlers_confidence_mass_failed", "API_OPS_HANDLERS_CONFIDENCE_MASS_FAILED", e)
        return payload


def api_get_news_latest(parsed, ctx):
    from engine.runtime.storage import connect_ro

    qs = _qs(parsed)
    limit = _parse_int(qs.get("limit", "20") or "20", 20, 1, 100)

    con = connect_ro()
    try:
        rows = con.execute(
            """
            SELECT ts_ms, source, title, symbol
            FROM events
            WHERE COALESCE(event_type, 'news') = 'news'
            ORDER BY ts_ms DESC, id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []
    except Exception as e:
        payload = _failure_out("api_ops_handlers_news_latest_failed", "API_OPS_HANDLERS_NEWS_LATEST_FAILED", e, items=[])
        return payload
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "API_OPS_HANDLERS_NEWS_LATEST_CLOSE_FAILED",
                e,
                once_key="api_ops_handlers_news_latest_close",
            )

    items = []
    for ts_ms, source, title, symbol in rows:
        items.append(
            {
                "ts_ms": int(ts_ms or 0),
                "source": str(source or ""),
                "title": str(title or ""),
                "symbol": str(symbol or "").strip().upper(),
            }
        )

    return {"ok": True, "items": items}


def api_get_news_sentiment(parsed, ctx):
    from engine.runtime.storage import connect_ro

    qs = _qs(parsed)
    limit = _parse_int(qs.get("limit", "120") or "120", 120, 1, 500)

    con = connect_ro()
    try:
        rows = con.execute(
            """
            SELECT bucket_ts_ms, tone_mean
            FROM gdelt_macro_features
            ORDER BY bucket_ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall() or []
    except Exception as e:
        payload = _failure_out("api_ops_handlers_news_sentiment_failed", "API_OPS_HANDLERS_NEWS_SENTIMENT_FAILED", e, series=[])
        return payload
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "API_OPS_HANDLERS_MARKET_REGIME_CLOSE_FAILED",
                e,
                once_key="api_ops_handlers_market_regime_close",
            )

    series = [
        {
            "ts_ms": int(ts_ms or 0),
            "sentiment": float(tone_mean or 0.0),
        }
        for ts_ms, tone_mean in reversed(rows)
    ]
    return {"ok": True, "series": series}


def api_get_human_alignment_summary(parsed, ctx):
    try:
        from engine.runtime.storage import fetch_human_alignment_report

        qs = _qs(parsed)
        limit = _parse_int(qs.get("limit", "10") or "10", 10, 1, 100)
        lookback_h = _parse_int(qs.get("lookback_h", "24") or "24", 24, 1, 24 * 30)
        cache_key = f"human_alignment:{lookback_h}:{limit}"

        return cache_get_or_load(
            "api_ops",
            cache_key,
            lambda: {
                "ok": True,
                **fetch_human_alignment_report(
                    lookback_hours=lookback_h,
                    limit_rules=limit,
                ),
            },
            ttl_s=5.0,
        )
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_human_alignment_summary_failed",
            "API_OPS_HANDLERS_HUMAN_ALIGNMENT_SUMMARY_FAILED",
            e,
            summary={},
            top_rules=[],
            recommendations=[],
        )
        return payload


def api_get_weather_snapshot(parsed, ctx):
    from engine.data.weather_api import api_weather_snapshot

    qs = _qs(parsed)
    symbol = str(qs.get("symbol", "SPY") or "SPY").strip().upper()
    ts_ms = qs.get("ts_ms")
    try:
        ts_ms = int(ts_ms) if ts_ms not in (None, "") else None
    except Exception:
        ts_ms = None
    return api_weather_snapshot(symbol=symbol, ts_ms=ts_ms)


def api_get_weather_alerts(parsed, ctx):
    from engine.data.weather_api import api_weather_alerts

    qs = _qs(parsed)
    ts_ms = qs.get("ts_ms")
    try:
        ts_ms = int(ts_ms) if ts_ms not in (None, "") else None
    except Exception:
        ts_ms = None
    return api_weather_alerts(ts_ms=ts_ms)


def api_get_weather_effect(parsed, ctx):
    from engine.data.weather_api import api_weather_effect

    qs = _qs(parsed)
    ts_ms = qs.get("ts_ms")
    try:
        ts_ms = int(ts_ms) if ts_ms not in (None, "") else None
    except Exception:
        ts_ms = None
    return api_weather_effect(ts_ms=ts_ms)


# ----------------------------
# POST
# ----------------------------


def api_post_execution_advisory_action(parsed, body, ctx):
    try:
        from engine.execution.execution_ai_advisor import record_execution_advisory_action

        payload = body if isinstance(body, dict) else {}
        advisory_id = _parse_int(payload.get("advisory_id", "0") or "0", 0, 0)
        action = str(payload.get("action") or "").strip().lower()
        actor = str(payload.get("actor") or "operator").strip() or "operator"
        note = str(payload.get("note") or "").strip()
        detail = payload.get("detail")
        if detail is not None and not isinstance(detail, dict):
            detail = {"value": str(detail)}
        if advisory_id <= 0:
            return {"ok": False, "error": "missing_advisory_id"}
        return record_execution_advisory_action(
            advisory_id=advisory_id,
            action=action,
            actor=actor,
            note=note,
            detail=detail if isinstance(detail, dict) else None,
        )
    except Exception as e:
        payload = _failure_out(
            "api_ops_handlers_execution_advisory_action_failed",
            "API_OPS_HANDLERS_EXECUTION_ADVISORY_ACTION_FAILED",
            e,
        )
        return payload

def api_post_rollback(parsed, body, ctx):
    denied = deny_if_shutdown()
    if denied:
        return denied

    from engine.api.api_governance import validate_rollback_request

    validation_error = validate_rollback_request(body)
    if validation_error:
        return validation_error

    jobs = (ctx or {}).get("JOBS")
    get_execution_mode_fn = getattr(jobs, "get_execution_mode_fn", None) if jobs else None

    gate = execution_gate_snapshot(
        get_execution_mode_fn=get_execution_mode_fn
    )
    if not (gate.get("allow_execution") or gate.get("allowed")):
        return {
            "ok": False,
            "error": f"execution_gated:{gate.get('reason')}",
            "gate": gate,
        }

    from engine.api.api_governance import api_post_rollback
    return api_post_rollback(parsed, body)

__all__ = [
    "api_get_alerts",
    "api_get_notifications_status",
    "api_get_validation",
    "api_get_model_registry",
    "api_get_model_performance_divergence",
    "api_get_model_lifecycle",
    "api_get_embed_model_eval",
    "api_get_embed_conf_calib",
    "api_get_temporal_eval",
    "api_get_temporal_models",
    "api_get_model_diagnostics",
    "api_get_latest_portfolio_backtest",
    "api_get_execution_metrics",
    "api_get_feeds",
    "api_get_execution_stats",
    "api_get_execution_metrics_rolling",
    "api_get_execution_metrics_by_symbol",
    "api_get_execution_cost_by_confidence",
    "api_get_execution_diagnostics",
    "api_get_execution_advisories",
    "api_get_social_features",
    "api_get_social_regimes",
    "api_get_social_blocks",
    "api_get_news_latest",
    "api_get_news_sentiment",
    "api_get_human_alignment_summary",
    "api_get_weather_snapshot",
    "api_get_weather_alerts",
    "api_get_weather_effect",
    "api_get_confidence_mass",
    "api_post_notifications_test",
    "api_post_execution_advisory_action",
    "api_post_rollback",
]
