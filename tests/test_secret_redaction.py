from __future__ import annotations

import json
from pathlib import Path


def test_log_redaction_covers_tokens_password_urls_and_key_ids() -> None:
    from engine.api.redaction import redact_api_payload, redact_string

    token_value = "token-value-for-redaction"
    password_value = "password-value-for-redaction"
    key_id_value = "key-id-value-for-redaction"
    rendered = redact_string(
        " ".join(
            [
                f"DASHBOARD_API_TOKEN={token_value}",
                f"password={password_value}",
                f"ALPACA_KEY_ID={key_id_value}",
                "postgresql://trading:url-password-for-redaction@db:5432/trading",
            ]
        )
    )

    assert token_value not in rendered
    assert password_value not in rendered
    assert key_id_value not in rendered
    assert "url-password-for-redaction" not in rendered
    assert "<redacted" in rendered

    payload = redact_api_payload({"alpaca_key_id": key_id_value, "nested": {"api_key": token_value}})
    payload_text = json.dumps(payload, sort_keys=True)
    assert key_id_value not in payload_text
    assert token_value not in payload_text


def test_audit_records_are_redacted_before_api_return(monkeypatch) -> None:
    import engine.api.api_read_advanced as advanced

    audit_secret = "audit-secret-value"

    monkeypatch.setattr(
        advanced,
        "fetch_recent_audit_records",
        lambda *args, **kwargs: [
            {
                "id": 1,
                "event": "credential_test",
                "payload_json": json.dumps({"api_key": audit_secret, "safe": "visible"}),
                "detail": f"api_key={audit_secret}",
            }
        ],
    )

    out = advanced.get_audit_records("execution_policy_audit", limit=1)
    rendered = json.dumps(out, sort_keys=True)

    assert out["ok"] is True
    assert audit_secret not in rendered
    assert "visible" in rendered


def test_config_error_redaction_excludes_inline_secret_value(monkeypatch, tmp_path: Path) -> None:
    from engine.runtime.config_schema import ConfigError, validate_production_secret_sources

    secret_value = "config-error-secret-value"
    monkeypatch.setenv("PROD_LOCK", "1")
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("TRADING_ENFORCE_SECRET_SOURCE_POLICY", "1")
    monkeypatch.setenv("TRADING_SECRET_POLICY_REPO_ROOT", str(tmp_path))
    monkeypatch.setenv("ALPACA_SECRET_KEY", secret_value)

    try:
        validate_production_secret_sources({"strict_runtime": True})
        raise AssertionError("expected ConfigError")
    except ConfigError as exc:
        message = str(exc)

    assert "inline_secret_env:ALPACA_SECRET_KEY" in message
    assert secret_value not in message


def test_failure_diagnostics_redacts_startup_diagnostic_payload(monkeypatch, tmp_path: Path) -> None:
    from engine.runtime.failure_diagnostics import build_failure_payload

    diagnostic_secret = "startup-diagnostic-secret-value"
    monkeypatch.setenv("DASHBOARD_API_TOKEN", diagnostic_secret)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "runtime.db"))

    payload = build_failure_payload(
        code="startup_secret_test",
        message=f"startup failed token={diagnostic_secret}",
        scope="startup",
        extra={"ALPACA_KEY_ID": diagnostic_secret, "safe": "visible"},
        include_health=False,
        include_quick_check=False,
    )
    rendered = json.dumps(payload, sort_keys=True)

    assert diagnostic_secret not in rendered
    assert "visible" in rendered
    assert "<redacted" in rendered
