import sys

from tools import runtime_stability_probe


def _clear_probe_tokens(monkeypatch):
    for key in (
        "DASHBOARD_API_TOKEN",
        "DASHBOARD_API_TOKEN_FILE",
        "DASHBOARD_API_TOKEN_SECRET",
        "PIPELINE_SMOKE_OPERATOR_TOKEN",
        "PIPELINE_SMOKE_OPERATOR_TOKEN_FILE",
        "PIPELINE_SMOKE_OPERATOR_TOKEN_SECRET",
        "OPERATOR_API_TOKEN",
        "OPERATOR_API_TOKEN_FILE",
        "OPERATOR_API_TOKEN_SECRET",
        "PIPELINE_SMOKE_OPERATOR_BASE",
    ):
        monkeypatch.delenv(key, raising=False)


def test_runtime_stability_probe_default_skips_operator_without_auth(monkeypatch, capsys):
    _clear_probe_tokens(monkeypatch)
    seen = {}

    def _wait(base_url, timeout_s, *, operator_url):
        seen["base_url"] = base_url
        seen["timeout_s"] = timeout_s
        seen["operator_url"] = operator_url
        return False

    monkeypatch.setattr(runtime_stability_probe, "_wait_for_runtime", _wait)
    monkeypatch.setattr(sys, "argv", ["runtime_stability_probe.py", "--warmup-s", "1"])

    assert runtime_stability_probe.main() == 1
    assert seen["operator_url"] is None
    assert "operator_probe_skipped reason=missing_auth" in capsys.readouterr().err


def test_runtime_stability_probe_explicit_operator_url_stays_strict_without_auth(monkeypatch, capsys):
    _clear_probe_tokens(monkeypatch)
    seen = {}

    def _wait(base_url, timeout_s, *, operator_url):
        seen["operator_url"] = operator_url
        return False

    operator_url = "http://127.0.0.1:8000/operator"
    monkeypatch.setattr(runtime_stability_probe, "_wait_for_runtime", _wait)
    monkeypatch.setattr(
        sys,
        "argv",
        ["runtime_stability_probe.py", "--operator-url", operator_url, "--warmup-s", "1"],
    )

    assert runtime_stability_probe.main() == 1
    assert seen["operator_url"] == operator_url
    assert "operator_probe_auth_missing" in capsys.readouterr().err


def test_runtime_stability_probe_default_skips_operator_even_with_dashboard_auth(monkeypatch, capsys):
    _clear_probe_tokens(monkeypatch)
    seen = {}

    def _wait(base_url, timeout_s, *, operator_url):
        seen["operator_url"] = operator_url
        return False

    monkeypatch.setenv("DASHBOARD_API_TOKEN", "dashboard-token")
    monkeypatch.setattr(runtime_stability_probe, "_wait_for_runtime", _wait)
    monkeypatch.setattr(sys, "argv", ["runtime_stability_probe.py", "--warmup-s", "1"])

    assert runtime_stability_probe.main() == 1
    assert seen["operator_url"] is None
    assert "operator_probe_skipped reason=not_required" in capsys.readouterr().err


def test_runtime_stability_probe_operator_auth_detection(monkeypatch):
    _clear_probe_tokens(monkeypatch)
    assert not runtime_stability_probe._operator_probe_has_auth("http://127.0.0.1:8000/operator")
    assert not runtime_stability_probe._operator_probe_has_auth("http://127.0.0.1:4001")

    monkeypatch.setenv("DASHBOARD_API_TOKEN", "dashboard-token")
    assert runtime_stability_probe._operator_probe_has_auth("http://127.0.0.1:8000/operator")
    assert not runtime_stability_probe._operator_probe_has_auth("http://127.0.0.1:4001")

    monkeypatch.setenv("OPERATOR_API_TOKEN", "operator-token")
    assert runtime_stability_probe._operator_probe_has_auth("http://127.0.0.1:4001")


def test_runtime_stability_probe_dashboard_auth_detection_uses_token_file(monkeypatch, tmp_path):
    _clear_probe_tokens(monkeypatch)
    token_path = tmp_path / "dashboard-token"
    token_path.write_text("dashboard-token-from-file\n", encoding="utf-8")
    monkeypatch.setenv("DASHBOARD_API_TOKEN_FILE", str(token_path))

    assert runtime_stability_probe._dashboard_api_token() == "dashboard-token-from-file"
    assert runtime_stability_probe._operator_probe_has_auth("http://127.0.0.1:8000/operator")
