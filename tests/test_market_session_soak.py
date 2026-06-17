from __future__ import annotations

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from tools import market_session_soak as soak


def _runner_args() -> argparse.Namespace:
    return argparse.Namespace(
        dashboard_url="http://127.0.0.1:8000",
        operator_url="http://127.0.0.1:4001",
        timeout_s=0.1,
        symbol="SPY",
        qty=1.0,
        duration_s=1,
        interval_s=1,
        provider_job="poll_prices",
        reconcile_broker="sim",
        max_price_age_s=120,
    )


def test_full_market_session_plan_fails_after_close():
    now = datetime(2026, 6, 16, 17, 28, tzinfo=ZoneInfo("America/New_York"))

    plan = soak.market_session_plan(now=now, duration_s=soak.DEFAULT_SESSION_SECONDS)

    assert plan["ok"] is False
    assert plan["reason"] == "full_market_session_start_missed"
    assert plan["open_et"].startswith("2026-06-16T09:30:00")
    assert plan["close_et"].startswith("2026-06-16T16:00:00")


def test_full_market_session_plan_accepts_open_grace_window():
    now = datetime(2026, 6, 16, 9, 30, 30, tzinfo=ZoneInfo("America/New_York"))

    plan = soak.market_session_plan(now=now, duration_s=soak.DEFAULT_SESSION_SECONDS)

    assert plan["ok"] is True
    assert plan["reason"] == "market_open_full_session_window"


def test_signed_report_uses_hmac_without_leaking_key():
    report = {"status": "NO-GO", "failures": [{"reason": "unit_test"}]}

    signed = soak.sign_report(report, signing_key="secret-signing-key")

    assert signed["status"] == "signed"
    assert signed["algorithm"] == "hmac-sha256"
    assert signed["signature"]
    assert "secret-signing-key" not in str(signed)


def test_unsigned_report_is_no_go_evidence():
    report = {"status": "NO-GO", "failures": [{"reason": "unit_test"}]}

    signed = soak.sign_report(report, signing_key="")

    assert signed["status"] == "unsigned"
    assert signed["error"] == "soak_report_signing_key_missing"
    assert signed["report_sha256"]


def test_finalize_report_marks_unsigned_no_go_with_exit_code(monkeypatch):
    monkeypatch.delenv("SOAK_REPORT_SIGNING_KEY", raising=False)
    report = {"status": "GO", "failures": []}

    finalized = soak.finalize_report(report)

    assert finalized["status"] == "NO-GO"
    assert finalized["exit_code"] == soak.EXIT_NO_GO
    assert finalized["ended_at"]
    assert {"reason": "soak_report_signing_key_missing"} in finalized["failures"]
    assert finalized["signature"]["status"] == "unsigned"


def test_scan_fail_patterns_flags_traceback_and_db_lock():
    text = """
    harmless line
    Traceback (most recent call last):
    sqlite3.OperationalError: database is locked
    """

    matches = soak.scan_fail_patterns(text)

    assert len(matches) == 2
    assert "Traceback" in matches[0]
    assert "database is locked" in matches[1]


def test_provider_freshness_findings_detect_stale_price_and_provider():
    health = {
        "prices": {"ok": True, "age_s": 500},
        "providers": {
            "ok": False,
            "healthy": 0,
            "total": 1,
            "by_provider": {
                "polygon_ws": {"ok": False, "status": "STALE", "age_ms": 500_000},
            },
        },
    }

    findings = soak.provider_freshness_findings(health, max_price_age_s=120)

    reasons = {str(item.get("reason") or "") for item in findings}
    assert "price_age_exceeded" in reasons
    assert "no_healthy_providers" in reasons
    assert any(item.get("provider") == "polygon_ws" for item in findings)


def test_allowed_paper_brokers_accepts_alpaca_only_on_paper_endpoint(monkeypatch):
    monkeypatch.setenv("BROKER", "alpaca")
    monkeypatch.setenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

    assert "alpaca" in soak._allowed_paper_brokers()

    monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")
    assert "alpaca" not in soak._allowed_paper_brokers()


def test_paper_broker_preflight_rejects_alpaca_live_endpoint(monkeypatch):
    monkeypatch.setenv("BROKER", "alpaca")
    monkeypatch.setenv("BROKER_NAME", "alpaca")
    monkeypatch.setenv("ALPACA_BASE_URL", "https://api.alpaca.markets")

    snapshot = soak.paper_broker_preflight_snapshot()

    assert snapshot["ok"] is False
    assert snapshot["reason"] == "alpaca_paper_endpoint_required"


def test_sample_marks_missing_required_snapshots_no_go(monkeypatch):
    runner = soak.SoakRunner(_runner_args())
    runner.log_cursors = []

    def fake_get(path: str, *, base: str | None = None) -> soak.HttpResult:
        if path == "/api/health":
            return soak.HttpResult(
                True,
                200,
                1.0,
                {
                    "prices": {"ok": True, "age_s": 1},
                    "providers": {"ok": True, "healthy": 1, "total": 1, "by_provider": {}},
                },
            )
        return soak.HttpResult(False, 503, 1.0, {"error": "down"}, "down")

    monkeypatch.setattr(runner, "get", fake_get)

    runner.sample("unit")

    assert any(item["reason"] == "snapshot_capture_failed" for item in runner.failures)


def test_report_marks_audit_tail_capture_failure_no_go(monkeypatch):
    runner = soak.SoakRunner(_runner_args())
    runner.log_cursors = []

    def raise_audit_tail(_: int) -> dict:
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(soak, "audit_tail_since", raise_audit_tail)

    report = runner.report({})

    assert report["status"] == "NO-GO"
    assert report["audit_tail"]["_capture_ok"] is False
    assert any(item["reason"] == "audit_tail_capture_failed" for item in report["failures"])
