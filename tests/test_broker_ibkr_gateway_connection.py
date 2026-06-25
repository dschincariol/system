from __future__ import annotations


def test_ibkr_ping_uses_bounded_tcp_preflight_timeout(monkeypatch):
    from engine.execution import broker_ibkr_gateway

    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    calls = []

    def fake_create_connection(address, timeout):
        calls.append((address, timeout))
        raise TimeoutError("timed out")

    monkeypatch.setattr(broker_ibkr_gateway.socket, "create_connection", fake_create_connection)

    result = broker_ibkr_gateway.ping_broker_connection(
        timeout_s=0.25,
        retries=3,
        host="127.0.0.1",
        port=7497,
        client_id=7,
    )

    assert result["ok"] is False
    assert result["broker"] == "ibkr"
    assert result["state"] == "connect_timeout"
    assert result["attempt"] == 1
    assert result["reasons"] == ["connect_timeout"]
    assert "ibkr_connect_timeout" in result["error"]
    assert calls == [(("127.0.0.1", 7497), 0.25)]
