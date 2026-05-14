from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
AGENT_PATH = ROOT / "services" / "operator_ai" / "agent.js"
SERVER_PATH = ROOT / "boot" / "operator_server.js"


def test_operator_ai_context_keeps_support_snapshot_and_barrier_fields_aligned():
    text = AGENT_PATH.read_text(encoding="utf-8")

    assert "supportSnapshot," in text
    assert "get(\"/api/operator/support_snapshot?mode=quick\")" in text
    assert "get(\"/api/operator/snapshot?mode=quick\")" in text
    assert "get(\"/api/operator/provider_telemetry\")" in text
    assert "get(\"/api/operator/runtime_watchdogs\")" in text
    assert "get(\"/api/execution/barrier\")" in text
    assert "support_snapshot: supportSnapshot" in text
    assert "barrier," in text


def test_operator_sidecar_proxies_execution_barrier_for_diagnostics_agent():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert 'app.get("/api/execution/barrier",' in text
    assert "OPERATOR_BARRIER_PROXY_TIMEOUT_MS" in text
    assert (
        'operatorProxyGet("/api/execution/barrier", "invalid_execution_barrier_response", '
        "OPERATOR_BARRIER_PROXY_TIMEOUT_MS)"
    ) in text
