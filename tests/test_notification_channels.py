import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_NOTIFICATION_ENV_KEYS = (
    "DB_PATH",
    "EQ_CRIT_EMAIL_TO",
    "EQ_CRIT_EMAIL_FROM",
    "EQ_CRIT_SMTP_HOST",
    "EQ_CRIT_SMTP_PORT",
    "EQ_CRIT_WEBHOOK_URL",
    "EQ_CRIT_WEBHOOK_TIMEOUT_S",
)


class NotificationChannelsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._saved_env = {key: os.environ.get(key) for key in _NOTIFICATION_ENV_KEYS}

        db_path = Path(self.tmp.name) / "notification-tests.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_path.touch()
        os.environ["DB_PATH"] = str(db_path)
        for key in _NOTIFICATION_ENV_KEYS:
            if key != "DB_PATH":
                os.environ.pop(key, None)

        import engine.runtime.storage as storage
        import engine.runtime.alerts_notify as alerts_notify

        self.storage = importlib.reload(storage)
        self.alerts_notify = importlib.reload(alerts_notify)

    def tearDown(self):
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

        import engine.runtime.storage as storage
        import engine.runtime.alerts_notify as alerts_notify

        importlib.reload(storage)
        importlib.reload(alerts_notify)
        self.tmp.cleanup()

    def test_status_reports_partial_email_configuration(self):
        os.environ["EQ_CRIT_EMAIL_TO"] = "ops@example.com"

        channels = self.alerts_notify.get_notification_channel_status()
        email = next(row for row in channels if row["channel"] == "email")
        webhook = next(row for row in channels if row["channel"] == "webhook")

        self.assertTrue(email["configured"])
        self.assertFalse(email["enabled"])
        self.assertFalse(email["supports_test"])
        self.assertIn("smtp_host_missing", email["validation_errors"])

        self.assertFalse(webhook["configured"])
        self.assertFalse(webhook["enabled"])
        self.assertEqual(webhook["status"], "not_configured")

    def test_send_email_test_records_latest_result(self):
        os.environ["EQ_CRIT_EMAIL_TO"] = "ops@example.com"
        os.environ["EQ_CRIT_EMAIL_FROM"] = "alerts@example.com"
        os.environ["EQ_CRIT_SMTP_HOST"] = "smtp.example.com"
        os.environ["EQ_CRIT_SMTP_PORT"] = "2525"

        sent = {}

        class FakeSMTP:
            def __init__(self, host, port, timeout=None):
                sent["host"] = host
                sent["port"] = port
                sent["timeout"] = timeout

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def send_message(self, message):
                sent["subject"] = str(message["Subject"])
                sent["to"] = str(message["To"])

        with patch("smtplib.SMTP", FakeSMTP):
            result = self.alerts_notify.send_notification_test("email", actor="qa", source="unit")

        self.assertTrue(result["ok"])
        self.assertEqual(sent["host"], "smtp.example.com")
        self.assertEqual(sent["port"], 2525)
        self.assertEqual(sent["to"], "ops@example.com")
        self.assertTrue(sent["subject"].startswith("[TEST]"))

        channels = self.alerts_notify.get_notification_channel_status()
        email = next(row for row in channels if row["channel"] == "email")

        self.assertIsNotNone(email["last_test"])
        self.assertTrue(email["last_test"]["ok"])
        self.assertEqual(email["last_test"]["requested_by"], "qa")
        self.assertEqual(email["last_test"]["source"], "unit")

    def test_notification_routes_are_registered(self):
        from engine.api.api_ops import ROUTE_SPECS

        self.assertIn(("GET", "/api/notifications/status", "api_get_notifications_status"), ROUTE_SPECS)
        self.assertIn(("POST", "/api/notifications/test", "api_post_notifications_test"), ROUTE_SPECS)

    def test_runtime_health_notifications_are_deduped_and_persist_latest_state(self):
        os.environ["EQ_CRIT_EMAIL_TO"] = "ops@example.com"
        os.environ["EQ_CRIT_EMAIL_FROM"] = "alerts@example.com"
        os.environ["EQ_CRIT_SMTP_HOST"] = "smtp.example.com"
        os.environ["EQ_CRIT_SMTP_PORT"] = "2525"

        sent = []

        class FakeSMTP:
            def __init__(self, host, port, timeout=None):
                self.host = host
                self.port = port
                self.timeout = timeout

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def send_message(self, message):
                sent.append(
                    {
                        "subject": str(message["Subject"]),
                        "to": str(message["To"]),
                    }
                )

        degraded_health = {
            "ok": False,
            "timeseries_storage": {
                "ok": False,
                "enabled": True,
                "detail": "timeseries_storage_not_ready",
                "degraded_reasons": ["timescale_flush_failures"],
            },
            "feature_store": {
                "ok": False,
                "enabled": True,
                "degraded_reasons": ["feature_store_queue_backpressure_active"],
            },
            "portfolio_runtime": {
                "degraded": True,
                "detail": "portfolio_runtime_degraded",
                "degraded_codes": ["PORTFOLIO_RISK_GATE_FAILED"],
            },
            "execution_degraded": {
                "active": True,
                "severity": "CRITICAL",
                "reason": "event_bus_critical_backpressure",
                "reason_codes": ["event_bus_critical_backpressure"],
            },
            "execution_barrier": {"allowed": False, "reason": "event_bus_critical_backpressure"},
        }
        healthy_snapshot = {
            "ok": True,
            "timeseries_storage": {"ok": True, "enabled": True},
            "feature_store": {"ok": True, "enabled": True},
            "portfolio_runtime": {"ok": True, "available": True, "degraded": False},
            "execution_degraded": {"active": False, "reason_codes": []},
            "execution_barrier": {"allowed": True, "reason": ""},
        }

        with patch("smtplib.SMTP", FakeSMTP):
            first = self.alerts_notify.send_runtime_health_notification(
                degraded_health,
                actor="system",
                source="unit",
            )
            second = self.alerts_notify.send_runtime_health_notification(
                degraded_health,
                actor="system",
                source="unit",
            )
            recovered = self.alerts_notify.send_runtime_health_notification(
                healthy_snapshot,
                actor="system",
                source="unit",
            )

        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        self.assertTrue(recovered["changed"])
        self.assertEqual(len(sent), 2)
        self.assertTrue(sent[0]["subject"].startswith("[RUNTIME DEGRADED]"))
        self.assertTrue(sent[1]["subject"].startswith("[RUNTIME RECOVERED]"))

        latest = self.alerts_notify.get_runtime_health_notification_status()
        self.assertIsNotNone(latest)
        self.assertEqual(latest["state"], "healthy")
        self.assertEqual(latest["state_value"], "healthy")

    def test_notification_status_handler_includes_runtime_health_alert(self):
        degraded_health = {
            "ok": False,
            "portfolio_runtime": {
                "degraded": True,
                "detail": "portfolio_runtime_degraded",
                "degraded_codes": ["PORTFOLIO_RISK_GATE_FAILED"],
            },
            "execution_degraded": {"active": False, "reason_codes": []},
        }
        self.alerts_notify.send_runtime_health_notification(
            degraded_health,
            actor="system",
            source="unit",
        )

        import engine.api.api_ops_handlers as api_ops_handlers

        api_ops_handlers = importlib.reload(api_ops_handlers)
        payload = api_ops_handlers.api_get_notifications_status(None, None)

        self.assertTrue(payload["ok"])
        self.assertIn("runtime_health_alert", payload)
        self.assertEqual(payload["runtime_health_alert"]["state"], "degraded")
        self.assertIn("PORTFOLIO_RISK_GATE_FAILED", payload["runtime_health_alert"]["reason_codes"])

    def test_kill_health_monitor_emits_runtime_health_notifications(self):
        import engine.strategy.kill_health_monitor as kill_health_monitor

        kill_health_monitor = importlib.reload(kill_health_monitor)

        with patch.object(kill_health_monitor, "get_health_snapshot", return_value={"ok": False, "reasons": ["db_down"]}):
            with patch.object(kill_health_monitor, "activate", return_value={"ok": True}) as activate_mock:
                with patch.object(
                    kill_health_monitor,
                    "send_runtime_health_notification",
                    return_value={"ok": True, "changed": True},
                ) as notify_mock:
                    rc = kill_health_monitor.main()

        self.assertEqual(rc, 0)
        activate_mock.assert_called_once()
        notify_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
