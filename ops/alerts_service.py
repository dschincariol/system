"""
FILE: alerts_service.py

Operational helper script for `alerts_service`.
"""

# alerts_service.py
import json
import time
import logging
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect as _db_connect, init_db, run_write_txn
def _normalize_explain_json(val) -> str:
    if val is None:
        return "{}"
    if isinstance(val, str):
        s = val.strip()
        return s or "{}"
    try:
        return json.dumps(val, separators=(",", ":"), sort_keys=True)
    except Exception as e:
        logging.warning("alerts_service normalize_explain_json_failed err=%s", e)
        return "{}"
from engine.runtime.dashboard_config import (
    EQ_CRIT_EMAIL_TO,
    EQ_CRIT_EMAIL_FROM,
    EQ_CRIT_SMTP_HOST,
    EQ_CRIT_SMTP_PORT,
    EQ_CRIT_WEBHOOK_URL,
    EQ_CRIT_WEBHOOK_TIMEOUT_S,
)

LOG = logging.getLogger("alerts_service")


def _warn_nonfatal(event: str, error: BaseException, **extra) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="ops.alerts_service",
        extra=extra,
        persist=False,
    )

def _ensure_alert_acks():
    init_db()

def _ensure_alert_resolutions():
    init_db()

def _ack_alert(alert_id: int, who: str, source: str):
    init_db()

    # Acks/resolutions are separate side tables so the base `alerts` record stays
    # append-only and can still reflect the original alert payload unchanged.
    def _write(con):
        con.execute("""
            INSERT OR REPLACE INTO alert_acks
            (alert_id, acked_ts_ms, acked_by, source)
            VALUES (?,?,?,?)
        """, (
            int(alert_id),
            int(time.time() * 1000),
            str(who or ""),
            str(source or ""),
        ))

    run_write_txn(_write)

def _resolve_alert(alert_id: int, who: str, reason: str, source: str):
    init_db()

    def _write(con):
        con.execute("""
            INSERT OR IGNORE INTO alert_resolutions
            (alert_id, resolved_ts_ms, resolved_by, reason, source)
            VALUES (?,?,?,?,?)
        """, (
            int(alert_id),
            int(time.time() * 1000),
            str(who or ""),
            str(reason or ""),
            str(source or ""),
        ))

    run_write_txn(_write)

def _is_alert_acked(alert_id: int) -> bool:
    con = _db_connect()
    try:
        row = con.execute(
            "SELECT 1 FROM alert_acks WHERE alert_id = ?",
            (int(alert_id),),
        ).fetchone()
        return bool(row)
    finally:
        con.close()

def _is_alert_resolved(alert_id: int) -> bool:
    con = _db_connect()
    try:
        row = con.execute(
            "SELECT 1 FROM alert_resolutions WHERE alert_id = ?",
            (int(alert_id),),
        ).fetchone()
        return bool(row)
    finally:
        con.close()

def get_alerts():
    con = _db_connect()
    try:
        # This is a dashboard/service read model: it joins raw alerts with ack and
        # resolution state so UIs do not have to reconstruct that view themselves.
        rows = con.execute("""
            SELECT
              a.id, a.ts_ms, a.severity, a.symbol, a.horizon_s,
              a.expected_z, a.confidence, a.event_title, a.rule_id, a.explain_json,

              ak.alert_id IS NOT NULL AS acked,
              ak.acked_ts_ms,
              ak.acked_by,

              ar.alert_id IS NOT NULL AS resolved,
              ar.resolved_ts_ms,
              ar.resolved_by,
              ar.reason

            FROM alerts a
            LEFT JOIN alert_acks ak ON ak.alert_id = a.id
            LEFT JOIN alert_resolutions ar ON ar.alert_id = a.id
            ORDER BY a.ts_ms DESC
            LIMIT 50
        """).fetchall()

        return [{
            "id": r[0],
            "ts_ms": r[1],
            "severity": r[2],
            "symbol": r[3],
            "horizon_s": r[4],
            "expected_z": r[5],
            "confidence": r[6],
            "event_title": r[7],
            "rule_id": r[8],
            "explain_json": _normalize_explain_json(r[9]),
            "acked": bool(r[10]),
            "acked_ts_ms": r[11],
            "acked_by": r[12],
            "resolved": bool(r[13]),
            "resolved_ts_ms": r[14],
            "resolved_by": r[15],
            "resolved_reason": r[16],
        } for r in rows]
    finally:
        con.close()

def _format_slack_eq_crit(p: dict) -> bytes:
    bt = p.get("bt") or {}
    diff = p.get("diff_equity")
    pct = p.get("diff_equity_pct")
    alert_id = p.get("alert_id", 0)

    text = "*🚨 CRITICAL: Broker vs Backtest Equity Mismatch*"

    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Δ Equity:*\n{diff:.4f}" if diff is not None else "*Δ Equity:*\n?"},
                {"type": "mrkdwn", "text": f"*Δ %:*\n{pct*100:.2f}%" if pct is not None else "*Δ %:*\n?"},
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

    return json.dumps(
        {"text": text, "blocks": blocks},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

def _format_discord_eq_crit(p: dict) -> bytes:
    bt = p.get("bt") or {}
    diff = p.get("diff_equity")
    pct = p.get("diff_equity_pct")

    embed = {
        "title": "🚨 CRITICAL: Equity Reconciliation Failed",
        "color": 15158332,
        "fields": [
            {"name": "Δ Equity", "value": f"{diff:.4f}" if diff is not None else "?", "inline": True},
            {"name": "Δ %", "value": f"{pct*100:.2f}%" if pct is not None else "?", "inline": True},
            {"name": "BT Equity", "value": str(bt.get("equity")), "inline": True},
            {"name": "Run ID", "value": str(bt.get("run_id")), "inline": True},
            {"name": "Reason", "value": str(p.get("reason", "n/a")), "inline": False},
        ],
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

    return json.dumps(
        {"embeds": [embed]},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

def _send_eq_crit_email(subject: str, body: str):
    if not EQ_CRIT_EMAIL_TO or not EQ_CRIT_SMTP_HOST:
        return

    import smtplib
    from email.message import EmailMessage

    msg = EmailMessage()
    msg["From"] = EQ_CRIT_EMAIL_FROM
    msg["To"] = EQ_CRIT_EMAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(EQ_CRIT_SMTP_HOST, EQ_CRIT_SMTP_PORT, timeout=5) as s:
        s.send_message(msg)

def _send_eq_crit_webhook(payload: dict):
    if not EQ_CRIT_WEBHOOK_URL:
        return

    # Webhook transport is best-effort and format-adaptive. Failure to notify
    # externally must not break local alert persistence or dashboard behavior.
    url = str(EQ_CRIT_WEBHOOK_URL).lower()
    is_slack = "hooks.slack.com" in url
    is_discord = "discord.com/api/webhooks" in url or "discordapp.com/api/webhooks" in url

    try:
        import urllib.request

        if is_slack:
            data = _format_slack_eq_crit(payload)
        elif is_discord:
            data = _format_discord_eq_crit(payload)
        else:
            data = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

        req = urllib.request.Request(
            EQ_CRIT_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(req, timeout=EQ_CRIT_WEBHOOK_TIMEOUT_S):
            pass
    except Exception as e:
        _warn_nonfatal("alerts_service_webhook_send_failed", e, webhook_url=str(EQ_CRIT_WEBHOOK_URL or ""))
