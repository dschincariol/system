"""
FILE: email_notifier.py

Operational helper script for `email_notifier`.
"""

# ops/email_notifier.py
import os
import smtplib
from email.message import EmailMessage

SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")

ALERT_FROM = os.environ.get("ALERT_FROM")
ALERT_TO = [x.strip() for x in os.environ.get("ALERT_TO", "").split(",") if x.strip()]

def send_email(subject: str, body: str) -> None:
    # Notification delivery is intentionally fail-open at the caller boundary:
    # missing config just skips send instead of crashing the operational script.
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and ALERT_FROM and ALERT_TO):
        print("[email] not configured, skipping send")
        return

    msg = EmailMessage()
    msg["From"] = ALERT_FROM
    msg["To"] = ", ".join(ALERT_TO)
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
