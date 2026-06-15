"""
FILE: alerts_notify.py

Runtime subsystem module for `alerts_notify`.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from engine.runtime.event_log import record_state_transition
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.platform import LOCALHOST_NAME
from engine.runtime.storage import connect, init_db, run_write_txn

LOG = get_logger("runtime.alerts_notify")
_RUNTIME_HEALTH_ALERT_NAMESPACE = "runtime_health_alert"
_RUNTIME_HEALTH_ALERT_KEY = "global"


def _env_int(key: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.environ.get(key)
    try:
        value = int(float(str(raw if raw is not None else default).strip()))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def _env_float(
    key: str,
    default: float,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    raw = os.environ.get(key)
    try:
        value = float(str(raw if raw is not None else default).strip())
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


def _notification_config() -> dict[str, Any]:
    default_email_from = f"alerts@{LOCALHOST_NAME}"
    return {
        "email_to": str(os.environ.get("EQ_CRIT_EMAIL_TO", "") or "").strip(),
        "email_from": str(os.environ.get("EQ_CRIT_EMAIL_FROM", default_email_from) or default_email_from).strip(),
        "smtp_host": str(os.environ.get("EQ_CRIT_SMTP_HOST", "") or "").strip(),
        "smtp_port": _env_int("EQ_CRIT_SMTP_PORT", 25, minimum=1, maximum=65535),
        "webhook_url": str(os.environ.get("EQ_CRIT_WEBHOOK_URL", "") or "").strip(),
        "webhook_timeout_s": _env_float("EQ_CRIT_WEBHOOK_TIMEOUT_S", 4.0, minimum=0.1, maximum=60.0),
    }


def _ts_iso(ts_ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts_ms) / 1000.0))


def _dedupe_strs(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _reason_code(item: Any) -> str:
    if isinstance(item, dict):
        for key in ("code", "reason", "detail", "source"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
        return ""
    return str(item or "").strip()


def _webhook_provider_name(url: str) -> str:
    lowered = str(url or "").strip().lower()
    if "hooks.slack.com" in lowered:
        return "slack"
    if "discord.com/api/webhooks" in lowered or "discordapp.com/api/webhooks" in lowered:
        return "discord"
    return "generic"


def _notification_test_table_exists(con: Any) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            ("notification_channel_tests",),
        ).fetchone()
        return bool(row)
    except Exception as e:
        log_failure(
            LOG,
            event="runtime_alerts_notify_test_table_exists_failed",
            code="RUNTIME_ALERTS_NOTIFY_TEST_TABLE_EXISTS_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.runtime.alerts_notify",
            persist=False,
        )
        return False


def _ensure_notification_test_table() -> None:
    init_db()

    def _txn(con: Any) -> None:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS notification_channel_tests (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              channel TEXT NOT NULL,
              provider TEXT,
              ok INTEGER NOT NULL,
              message TEXT,
              error TEXT,
              requested_by TEXT,
              source TEXT,
              requested_ts_ms INTEGER NOT NULL,
              detail_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_notification_channel_tests_channel_ts
              ON notification_channel_tests(channel, requested_ts_ms DESC);
            """
        )

    run_write_txn(_txn)


def _load_latest_notification_tests() -> dict[str, dict[str, Any]]:
    init_db()
    con = None
    out: dict[str, dict[str, Any]] = {}
    try:
        con = connect(readonly=True)
        if not _notification_test_table_exists(con):
            return out

        rows = con.execute(
            """
            SELECT channel, provider, ok, message, error, requested_by, source, requested_ts_ms, detail_json
            FROM notification_channel_tests
            ORDER BY requested_ts_ms DESC, id DESC
            """
        ).fetchall() or []

        for row in rows:
            channel = str(row[0] or "").strip().lower()
            if not channel or channel in out:
                continue
            detail_json = str(row[8] or "").strip()
            detail: dict[str, Any] = {}
            if detail_json:
                try:
                    parsed = json.loads(detail_json)
                    if isinstance(parsed, dict):
                        detail = parsed
                except Exception:
                    detail = {}
            out[channel] = {
                "ts_ms": int(row[7] or 0),
                "ok": bool(int(row[2] or 0) == 1),
                "message": str(row[3] or ""),
                "error": str(row[4] or ""),
                "requested_by": str(row[5] or ""),
                "source": str(row[6] or ""),
                "provider": str(row[1] or ""),
                "detail": detail,
            }
        return out
    except Exception as e:
        log_failure(
            LOG,
            event="runtime_alerts_notify_load_latest_tests_failed",
            code="RUNTIME_ALERTS_NOTIFY_LOAD_LATEST_TESTS_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.runtime.alerts_notify",
            persist=False,
        )
        return out
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                log_failure(
                    LOG,
                    event="runtime_alerts_notify_load_latest_tests_close_failed",
                    code="RUNTIME_ALERTS_NOTIFY_LOAD_LATEST_TESTS_CLOSE_FAILED",
                    message=str(e),
                    error=e,
                    level=logging.WARNING,
                    component="engine.runtime.alerts_notify",
                    persist=False,
                )


def _record_notification_test(
    *,
    channel: str,
    provider: str,
    ok: bool,
    message: str = "",
    error: str = "",
    requested_by: str = "operator",
    source: str = "dashboard",
    requested_ts_ms: int | None = None,
    detail: dict[str, Any] | None = None,
) -> None:
    _ensure_notification_test_table()
    ts_ms = int(requested_ts_ms or int(time.time() * 1000))
    detail_json = json.dumps(detail or {}, separators=(",", ":"), sort_keys=True)

    def _txn(con: Any) -> None:
        con.execute(
            """
            INSERT INTO notification_channel_tests
            (channel, provider, ok, message, error, requested_by, source, requested_ts_ms, detail_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(channel or "").strip().lower(),
                str(provider or "").strip().lower(),
                1 if ok else 0,
                str(message or "")[:1000],
                str(error or "")[:1000],
                str(requested_by or "")[:200],
                str(source or "")[:200],
                ts_ms,
                detail_json,
            ),
        )

    run_write_txn(_txn)


def _smtp_send_message(
    *,
    smtp_host: str,
    smtp_port: int,
    from_addr: str,
    to_addrs: str,
    subject: str,
    body: str,
    timeout_s: float = 5.0,
) -> None:
    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["From"] = str(from_addr or f"alerts@{LOCALHOST_NAME}")
    msg["To"] = str(to_addrs or "")
    msg["Subject"] = str(subject or "")
    msg.set_content(str(body or ""))

    with smtplib.SMTP(str(smtp_host), int(smtp_port), timeout=float(timeout_s)) as smtp:
        smtp.send_message(msg)


def _format_slack_eq_crit(payload: dict[str, Any]) -> bytes:
    bt = payload.get("bt") or {}
    diff = payload.get("diff_equity")
    pct = payload.get("diff_equity_pct")
    alert_id = payload.get("alert_id", 0)

    text = "*CRITICAL: Broker vs Backtest Equity Mismatch*"
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Delta Equity:*\n{diff:.4f}" if diff is not None else "*Delta Equity:*\n?"},
                {"type": "mrkdwn", "text": f"*Delta %:*\n{pct*100:.2f}%" if pct is not None else "*Delta %:*\n?"},
                {"type": "mrkdwn", "text": f"*BT Equity:*\n{bt.get('equity')}"},
                {"type": "mrkdwn", "text": f"*Run ID:*\n{bt.get('run_id')}"},
            ],
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "primary",
                    "text": {"type": "plain_text", "text": "Acknowledge"},
                    "value": f"ACK_ALERT:{alert_id}",
                }
            ],
        },
    ]
    return json.dumps({"text": text, "blocks": blocks}, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _format_discord_eq_crit(payload: dict[str, Any]) -> bytes:
    bt = payload.get("bt") or {}
    diff = payload.get("diff_equity")
    pct = payload.get("diff_equity_pct")
    embed = {
        "title": "CRITICAL: Equity Reconciliation Failed",
        "color": 15158332,
        "fields": [
            {"name": "Delta Equity", "value": f"{diff:.4f}" if diff is not None else "?", "inline": True},
            {"name": "Delta %", "value": f"{pct*100:.2f}%" if pct is not None else "?", "inline": True},
            {"name": "BT Equity", "value": str(bt.get("equity")), "inline": True},
            {"name": "Run ID", "value": str(bt.get("run_id")), "inline": True},
            {"name": "Reason", "value": str(payload.get("reason", "n/a")), "inline": False},
        ],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    return json.dumps({"embeds": [embed]}, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _format_slack_test(payload: dict[str, Any]) -> bytes:
    text = "[TEST] Trading dashboard notification test"
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": "*[TEST] Trading dashboard notification test*"}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Actor:*\n{payload.get('actor', 'operator')}"},
                {"type": "mrkdwn", "text": f"*Time:*\n{payload.get('ts_iso', '')}"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Safe operator-initiated test only. No trading or alert action was triggered.",
                }
            ],
        },
    ]
    return json.dumps({"text": text, "blocks": blocks}, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _format_discord_test(payload: dict[str, Any]) -> bytes:
    embed = {
        "title": "[TEST] Trading dashboard notification test",
        "color": 3447003,
        "description": "Safe operator-initiated test only. No trading or alert action was triggered.",
        "fields": [
            {"name": "Actor", "value": str(payload.get("actor", "operator")), "inline": True},
            {"name": "Time", "value": str(payload.get("ts_iso", "")), "inline": True},
        ],
        "timestamp": str(payload.get("ts_iso", "")),
    }
    return json.dumps({"embeds": [embed]}, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _generic_webhook_test_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _post_webhook_bytes(*, webhook_url: str, data: bytes, timeout_s: float) -> None:
    import urllib.request

    request = urllib.request.Request(
        str(webhook_url),
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=float(timeout_s)):
        pass


def _send_eq_crit_email(subject: str, body: str) -> None:
    cfg = _notification_config()
    if not cfg["email_to"] or not cfg["smtp_host"]:
        return
    _smtp_send_message(
        smtp_host=str(cfg["smtp_host"]),
        smtp_port=int(cfg["smtp_port"]),
        from_addr=str(cfg["email_from"]),
        to_addrs=str(cfg["email_to"]),
        subject=subject,
        body=body,
        timeout_s=5.0,
    )


def _send_eq_crit_webhook(payload: dict[str, Any]) -> None:
    cfg = _notification_config()
    webhook_url = str(cfg["webhook_url"] or "")
    if not webhook_url:
        return

    provider = _webhook_provider_name(webhook_url)
    try:
        if provider == "slack":
            data = _format_slack_eq_crit(payload)
        elif provider == "discord":
            data = _format_discord_eq_crit(payload)
        else:
            data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

        _post_webhook_bytes(
            webhook_url=webhook_url,
            data=data,
            timeout_s=float(cfg["webhook_timeout_s"]),
        )
    except Exception as e:
        log_failure(
            LOG,
            event="runtime_alerts_notify_webhook_failed",
            code="RUNTIME_ALERTS_NOTIFY_WEBHOOK_FAILED",
            message="runtime_alerts_notify_webhook_failed",
            error=e,
            level=logging.WARNING,
            component="engine.runtime.alerts_notify",
            persist=False,
            extra={"webhook_url": webhook_url},
        )


def _channel_status(channel: str, latest_test: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _notification_config()
    channel_name = str(channel or "").strip().lower()

    if channel_name == "email":
        email_to = str(cfg["email_to"] or "")
        smtp_host = str(cfg["smtp_host"] or "")
        validation_errors: list[str] = []
        configured = bool(email_to or smtp_host)
        if email_to and not smtp_host:
            validation_errors.append("smtp_host_missing")
        if smtp_host and not email_to:
            validation_errors.append("email_recipient_missing")
        enabled = bool(email_to and smtp_host and not validation_errors)
        return {
            "channel": "email",
            "transport": "smtp",
            "provider": "smtp",
            "configured": configured,
            "enabled": enabled,
            "supports_test": enabled,
            "status": "ready" if enabled else ("invalid_config" if configured else "not_configured"),
            "validation_errors": validation_errors,
            "last_test": latest_test or None,
        }

    if channel_name == "webhook":
        webhook_url = str(cfg["webhook_url"] or "")
        provider = _webhook_provider_name(webhook_url)
        validation_errors = []
        configured = bool(webhook_url)
        if webhook_url and not webhook_url.lower().startswith(("http://", "https://")):
            validation_errors.append("webhook_url_invalid")
        enabled = bool(webhook_url and not validation_errors)
        return {
            "channel": "webhook",
            "transport": "webhook",
            "provider": provider,
            "configured": configured,
            "enabled": enabled,
            "supports_test": enabled,
            "status": "ready" if enabled else ("invalid_config" if configured else "not_configured"),
            "validation_errors": validation_errors,
            "last_test": latest_test or None,
        }

    return {
        "channel": channel_name,
        "transport": "unknown",
        "provider": "unknown",
        "configured": False,
        "enabled": False,
        "supports_test": False,
        "status": "unsupported",
        "validation_errors": ["unsupported_channel"],
        "last_test": latest_test or None,
    }


def _runtime_health_components(health_snapshot: dict[str, Any] | None) -> tuple[dict[str, bool], list[str]]:
    health = health_snapshot if isinstance(health_snapshot, dict) else {}
    timeseries_storage = health.get("timeseries_storage") if isinstance(health.get("timeseries_storage"), dict) else {}
    feature_store = health.get("feature_store") if isinstance(health.get("feature_store"), dict) else {}
    nested_feature_store = (
        timeseries_storage.get("feature_store")
        if isinstance(timeseries_storage.get("feature_store"), dict)
        else {}
    )
    if not feature_store and nested_feature_store:
        feature_store = nested_feature_store
    portfolio_runtime = health.get("portfolio_runtime") if isinstance(health.get("portfolio_runtime"), dict) else {}
    execution_degraded = health.get("execution_degraded") if isinstance(health.get("execution_degraded"), dict) else {}
    execution_barrier = health.get("execution_barrier") if isinstance(health.get("execution_barrier"), dict) else {}

    timeseries_problem = bool(timeseries_storage.get("enabled")) and (
        not bool(timeseries_storage.get("ok"))
        or bool(timeseries_storage.get("degraded"))
        or bool(str(timeseries_storage.get("detail") or "").strip())
        or bool(list(timeseries_storage.get("degraded_reasons") or []))
    )
    feature_problem = bool(feature_store.get("enabled")) and (
        not bool(feature_store.get("ok"))
        or bool(feature_store.get("degraded"))
        or bool(list(feature_store.get("degraded_reasons") or []))
    )
    portfolio_problem = bool(portfolio_runtime.get("available", True)) and (
        bool(portfolio_runtime.get("degraded"))
        or portfolio_runtime.get("ok") is False
        or bool(str(portfolio_runtime.get("detail") or "").strip())
        or bool(list(portfolio_runtime.get("degraded_codes") or []))
        or bool(list(portfolio_runtime.get("degraded_reasons") or []))
    )
    execution_problem = bool(execution_degraded.get("active"))
    barrier_problem = bool(execution_barrier) and execution_barrier.get("allowed") is False

    codes: list[str] = []
    if timeseries_problem:
        detail = str(timeseries_storage.get("detail") or "").strip()
        if detail:
            codes.append(detail)
        codes.extend(str(item or "").strip() for item in list(timeseries_storage.get("degraded_reasons") or []))
    if feature_problem:
        codes.extend(str(item or "").strip() for item in list(feature_store.get("degraded_reasons") or []))
        if not list(feature_store.get("degraded_reasons") or []) and feature_store.get("ok") is False:
            codes.append("feature_store_not_ready")
    if portfolio_problem:
        detail = str(portfolio_runtime.get("detail") or "").strip()
        if detail:
            codes.append(detail)
        codes.extend(str(item or "").strip() for item in list(portfolio_runtime.get("degraded_codes") or []))
        codes.extend(_reason_code(item) for item in list(portfolio_runtime.get("degraded_reasons") or []))
    if execution_problem:
        codes.extend(str(item or "").strip() for item in list(execution_degraded.get("reason_codes") or []))
        reason = str(execution_degraded.get("reason") or "").strip()
        if reason:
            codes.append(reason)
    if barrier_problem:
        barrier_reason = str(execution_barrier.get("reason") or "").strip()
        if barrier_reason:
            codes.append(barrier_reason)
    if not codes and health.get("ok") is False:
        codes.extend(str(item or "").strip() for item in list(health.get("reasons") or []))
    if not codes and health.get("ok") is False:
        codes.append("health_not_ok")

    components = {
        "timeseries_storage": bool(timeseries_problem),
        "feature_store": bool(feature_problem),
        "portfolio_runtime": bool(portfolio_problem),
        "execution_degraded": bool(execution_problem),
        "execution_barrier": bool(barrier_problem),
    }
    return components, _dedupe_strs(codes)


def _build_runtime_health_notification_payload(
    health_snapshot: dict[str, Any] | None,
    *,
    actor: str,
    source: str,
    ts_ms: int,
) -> tuple[str, dict[str, Any]]:
    health = health_snapshot if isinstance(health_snapshot, dict) else {}
    components, reason_codes = _runtime_health_components(health)
    health_ok = bool(health.get("ok", False))
    degraded = (not health_ok) or bool(reason_codes)
    execution_degraded = health.get("execution_degraded") if isinstance(health.get("execution_degraded"), dict) else {}
    severity = str(execution_degraded.get("severity") or "").strip().upper()
    if not severity:
        severity = "CRITICAL" if degraded else "OK"
    if degraded and severity == "OK":
        severity = "WARN"
    state = "degraded" if degraded else "healthy"
    state_value = "healthy"
    if degraded:
        state_value = "degraded:" + "|".join(reason_codes[:8] or ["health_not_ok"])

    headline = "Runtime health recovered"
    summary = "Health monitor returned to a clean state."
    if degraded:
        headline = "Runtime health degraded"
        summary = ", ".join(reason_codes[:3]) if reason_codes else "runtime health degraded"

    payload = {
        "actor": str(actor or "system"),
        "source": str(source or "runtime"),
        "state": state,
        "headline": headline,
        "summary": summary,
        "severity": severity,
        "health_ok": health_ok,
        "reason_codes": reason_codes,
        "components": components,
        "execution_barrier_reason": str(
            ((health.get("execution_barrier") or {}) if isinstance(health.get("execution_barrier"), dict) else {}).get("reason")
            or ""
        ).strip(),
        "ts_ms": int(ts_ms),
        "ts_iso": _ts_iso(ts_ms),
    }
    return state_value, payload


def _format_slack_runtime_health(payload: dict[str, Any]) -> bytes:
    severity = str(payload.get("severity") or "WARN").upper()
    state = str(payload.get("state") or "unknown").strip().lower()
    reason_codes = list(payload.get("reason_codes") or [])
    components = payload.get("components") if isinstance(payload.get("components"), dict) else {}
    active_components = [name for name, active in components.items() if active]
    color_text = "recovered" if state == "healthy" else severity
    title = "*Runtime health recovered*" if state == "healthy" else f"*Runtime health degraded ({color_text})*"
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": title}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Source:*\n{payload.get('source', 'runtime')}"},
                {"type": "mrkdwn", "text": f"*Time:*\n{payload.get('ts_iso', '')}"},
                {"type": "mrkdwn", "text": f"*Severity:*\n{severity}"},
                {"type": "mrkdwn", "text": f"*State:*\n{state}"},
            ],
        },
    ]
    if reason_codes:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Reasons:*\n" + "\n".join(f"• {code}" for code in reason_codes[:5])},
            }
        )
    if active_components:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": "Active components: " + ", ".join(active_components[:5])}],
            }
        )
    return json.dumps({"text": title.replace("*", ""), "blocks": blocks}, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _format_discord_runtime_health(payload: dict[str, Any]) -> bytes:
    state = str(payload.get("state") or "unknown").strip().lower()
    severity = str(payload.get("severity") or "WARN").upper()
    reason_codes = list(payload.get("reason_codes") or [])
    components = payload.get("components") if isinstance(payload.get("components"), dict) else {}
    active_components = [name for name, active in components.items() if active]
    embed = {
        "title": "Runtime health recovered" if state == "healthy" else "Runtime health degraded",
        "color": 3066993 if state == "healthy" else (15158332 if severity == "CRITICAL" else 15844367),
        "description": str(payload.get("summary") or ""),
        "fields": [
            {"name": "Source", "value": str(payload.get("source") or "runtime"), "inline": True},
            {"name": "Severity", "value": severity, "inline": True},
            {"name": "State", "value": state, "inline": True},
        ],
        "timestamp": str(payload.get("ts_iso") or ""),
    }
    if reason_codes:
        embed["fields"].append({"name": "Reasons", "value": "\n".join(reason_codes[:5]), "inline": False})
    if active_components:
        embed["fields"].append({"name": "Active Components", "value": ", ".join(active_components[:5]), "inline": False})
    return json.dumps({"embeds": [embed]}, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _send_runtime_health_email(payload: dict[str, Any]) -> None:
    cfg = _notification_config()
    _smtp_send_message(
        smtp_host=str(cfg["smtp_host"]),
        smtp_port=int(cfg["smtp_port"]),
        from_addr=str(cfg["email_from"]),
        to_addrs=str(cfg["email_to"]),
        subject=(
            "[RUNTIME RECOVERED] Trading system health monitor"
            if str(payload.get("state") or "") == "healthy"
            else f"[RUNTIME DEGRADED] Trading system health monitor ({payload.get('severity', 'WARN')})"
        ),
        body=(
            f"{payload.get('headline', 'Runtime health update')}\n\n"
            f"Source: {payload.get('source', 'runtime')}\n"
            f"Time: {payload.get('ts_iso', '')}\n"
            f"Severity: {payload.get('severity', 'WARN')}\n"
            f"State: {payload.get('state', 'unknown')}\n"
            f"Summary: {payload.get('summary', '')}\n"
            f"Reason codes: {', '.join(list(payload.get('reason_codes') or [])[:8]) or 'none'}\n"
        ),
        timeout_s=5.0,
    )


def _send_runtime_health_webhook(payload: dict[str, Any]) -> None:
    cfg = _notification_config()
    webhook_url = str(cfg["webhook_url"] or "")
    provider = _webhook_provider_name(webhook_url)
    if provider == "slack":
        data = _format_slack_runtime_health(payload)
    elif provider == "discord":
        data = _format_discord_runtime_health(payload)
    else:
        data = json.dumps(
            {"kind": "runtime_health_alert", **payload},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    _post_webhook_bytes(
        webhook_url=webhook_url,
        data=data,
        timeout_s=float(cfg["webhook_timeout_s"]),
    )


def get_runtime_health_notification_status() -> dict[str, Any] | None:
    init_db()
    con = None
    try:
        con = connect(readonly=True)
        row = con.execute(
            """
            SELECT state_value, updated_ts_ms, payload_json
            FROM event_log_state
            WHERE namespace=? AND state_key=?
            LIMIT 1
            """,
            (_RUNTIME_HEALTH_ALERT_NAMESPACE, _RUNTIME_HEALTH_ALERT_KEY),
        ).fetchone()
        if not row:
            return None
        payload_json = str(row[2] or "").strip()
        payload: dict[str, Any] = {}
        if payload_json:
            try:
                parsed = json.loads(payload_json)
                if isinstance(parsed, dict):
                    payload = parsed
            except Exception:
                payload = {}
        state_value = str(row[0] or "").strip()
        return {
            "state_value": state_value,
            "state": str(payload.get("state") or ("healthy" if state_value == "healthy" else "degraded")),
            "headline": str(payload.get("headline") or ""),
            "summary": str(payload.get("summary") or ""),
            "severity": str(payload.get("severity") or ""),
            "reason_codes": list(payload.get("reason_codes") or []),
            "components": payload.get("components") if isinstance(payload.get("components"), dict) else {},
            "updated_ts_ms": int(row[1] or 0),
            "payload": payload,
        }
    except Exception as e:
        log_failure(
            LOG,
            event="runtime_alerts_notify_load_runtime_health_status_failed",
            code="RUNTIME_ALERTS_NOTIFY_LOAD_RUNTIME_HEALTH_STATUS_FAILED",
            message="runtime_alerts_notify_load_runtime_health_status_failed",
            error=e,
            level=logging.WARNING,
            component="engine.runtime.alerts_notify",
            persist=False,
        )
        return None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception as e:
                log_failure(
                    LOG,
                    event="runtime_alerts_notify_runtime_health_status_close_failed",
                    code="RUNTIME_ALERTS_NOTIFY_RUNTIME_HEALTH_STATUS_CLOSE_FAILED",
                    message=str(e),
                    error=e,
                    level=logging.WARNING,
                    component="engine.runtime.alerts_notify",
                    persist=False,
                )


def get_notification_channel_status() -> list[dict[str, Any]]:
    latest_tests = _load_latest_notification_tests()
    return [
        _channel_status("email", latest_tests.get("email")),
        _channel_status("webhook", latest_tests.get("webhook")),
    ]


def _build_test_email_payload(*, actor: str, ts_ms: int) -> tuple[str, str]:
    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0))
    subject = "[TEST] Trading dashboard notification check"
    body = (
        "This is a safe operator-initiated notification test from the trading dashboard.\n\n"
        "No live alert was triggered.\n"
        "No trading action was taken.\n\n"
        f"Actor: {actor}\n"
        f"Time: {ts_iso}\n"
    )
    return subject, body


def _build_test_webhook_payload(*, actor: str, ts_ms: int) -> dict[str, Any]:
    return {
        "kind": "operator_notification_test",
        "safe": True,
        "actor": str(actor or "operator"),
        "message": "Safe operator-initiated test only. No trading or alert action was triggered.",
        "ts_ms": int(ts_ms),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts_ms / 1000.0)),
    }


def send_notification_test(
    channel: str,
    *,
    actor: str = "operator",
    source: str = "dashboard",
) -> dict[str, Any]:
    channel_name = str(channel or "").strip().lower()
    requested_by = str(actor or "operator").strip() or "operator"
    request_source = str(source or "dashboard").strip() or "dashboard"
    ts_ms = int(time.time() * 1000)
    snapshot = _channel_status(channel_name)
    provider = str(snapshot.get("provider") or "")

    if channel_name not in {"email", "webhook"}:
        error = "unknown_channel"
        _record_notification_test(
            channel=channel_name or "unknown",
            provider=provider or "unknown",
            ok=False,
            error=error,
            requested_by=requested_by,
            source=request_source,
            requested_ts_ms=ts_ms,
            detail={"reason": "unsupported_channel"},
        )
        return {"ok": False, "channel": channel_name, "error": error, "status": snapshot}

    if not bool(snapshot.get("supports_test")):
        validation_errors = list(snapshot.get("validation_errors") or [])
        error = validation_errors[0] if validation_errors else "channel_not_configured"
        _record_notification_test(
            channel=channel_name,
            provider=provider,
            ok=False,
            error=error,
            requested_by=requested_by,
            source=request_source,
            requested_ts_ms=ts_ms,
            detail={"reason": "test_not_supported", "validation_errors": validation_errors},
        )
        latest = _load_latest_notification_tests().get(channel_name)
        return {"ok": False, "channel": channel_name, "error": error, "status": _channel_status(channel_name, latest)}

    cfg = _notification_config()
    try:
        if channel_name == "email":
            subject, body = _build_test_email_payload(actor=requested_by, ts_ms=ts_ms)
            _smtp_send_message(
                smtp_host=str(cfg["smtp_host"]),
                smtp_port=int(cfg["smtp_port"]),
                from_addr=str(cfg["email_from"]),
                to_addrs=str(cfg["email_to"]),
                subject=subject,
                body=body,
                timeout_s=5.0,
            )
        else:
            payload = _build_test_webhook_payload(actor=requested_by, ts_ms=ts_ms)
            if provider == "slack":
                data = _format_slack_test(payload)
            elif provider == "discord":
                data = _format_discord_test(payload)
            else:
                data = _generic_webhook_test_payload(payload)
            _post_webhook_bytes(
                webhook_url=str(cfg["webhook_url"]),
                data=data,
                timeout_s=float(cfg["webhook_timeout_s"]),
            )

        _record_notification_test(
            channel=channel_name,
            provider=provider,
            ok=True,
            message="notification_test_sent",
            requested_by=requested_by,
            source=request_source,
            requested_ts_ms=ts_ms,
            detail={"provider": provider},
        )
        latest = _load_latest_notification_tests().get(channel_name)
        return {
            "ok": True,
            "channel": channel_name,
            "message": "notification_test_sent",
            "status": _channel_status(channel_name, latest),
        }
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        log_failure(
            LOG,
            event="runtime_alerts_notify_send_test_failed",
            code="RUNTIME_ALERTS_NOTIFY_SEND_TEST_FAILED",
            message=error,
            error=e,
            level=logging.WARNING,
            component="engine.runtime.alerts_notify",
            extra={"channel": channel_name, "provider": provider},
            persist=False,
        )
        _record_notification_test(
            channel=channel_name,
            provider=provider,
            ok=False,
            error=error,
            requested_by=requested_by,
            source=request_source,
            requested_ts_ms=ts_ms,
            detail={"provider": provider},
        )
        latest = _load_latest_notification_tests().get(channel_name)
        return {
            "ok": False,
            "channel": channel_name,
            "error": error,
            "status": _channel_status(channel_name, latest),
        }


def send_runtime_health_notification(
    health_snapshot: dict[str, Any] | None,
    *,
    actor: str = "system",
    source: str = "runtime",
) -> dict[str, Any]:
    ts_ms = int(time.time() * 1000)
    state_value, payload = _build_runtime_health_notification_payload(
        health_snapshot,
        actor=str(actor or "system"),
        source=str(source or "runtime"),
        ts_ms=ts_ms,
    )
    event_id = record_state_transition(
        namespace=_RUNTIME_HEALTH_ALERT_NAMESPACE,
        state_key=_RUNTIME_HEALTH_ALERT_KEY,
        state_value=state_value,
        payload=payload,
        event_type="runtime_health_alert_transition",
        event_source=f"engine.runtime.alerts_notify:{source}",
        entity_type="runtime_health_alert",
        entity_id=_RUNTIME_HEALTH_ALERT_KEY,
        correlation_id=_RUNTIME_HEALTH_ALERT_KEY,
        ts_ms=ts_ms,
    )
    changed = event_id is not None
    deliveries: list[dict[str, Any]] = []
    ok = True

    if changed:
        for channel in get_notification_channel_status():
            if not bool(channel.get("enabled")):
                continue
            channel_name = str(channel.get("channel") or "").strip().lower()
            try:
                if channel_name == "email":
                    _send_runtime_health_email(payload)
                elif channel_name == "webhook":
                    _send_runtime_health_webhook(payload)
                else:
                    continue
                deliveries.append({"channel": channel_name, "ok": True})
            except Exception as e:
                ok = False
                error = f"{type(e).__name__}: {e}"
                deliveries.append({"channel": channel_name, "ok": False, "error": error})
                log_failure(
                    LOG,
                    event="runtime_alerts_notify_runtime_health_delivery_failed",
                    code="RUNTIME_ALERTS_NOTIFY_RUNTIME_HEALTH_DELIVERY_FAILED",
                    message="runtime_alerts_notify_runtime_health_delivery_failed",
                    error=e,
                    level=logging.WARNING,
                    component="engine.runtime.alerts_notify",
                    persist=False,
                    extra={
                        "channel": channel_name,
                        "state_value": state_value,
                        "source": str(source or "runtime"),
                    },
                )

    return {
        "ok": bool(ok),
        "changed": bool(changed),
        "event_id": int(event_id or 0) if event_id is not None else None,
        "state_value": state_value,
        "state": str(payload.get("state") or ""),
        "reason_codes": list(payload.get("reason_codes") or []),
        "deliveries": deliveries,
        "runtime_health_alert": get_runtime_health_notification_status(),
    }
