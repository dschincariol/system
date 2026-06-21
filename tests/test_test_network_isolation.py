from __future__ import annotations

import socket
import threading

import pytest

from engine.runtime import test_isolation
from engine.runtime.test_network_isolation import (
    LIVE_NETWORK_ENV,
    NetworkBlockedError,
    external_network_allowed_for_current_test,
)


def test_external_dns_resolution_is_blocked_before_resolver_use() -> None:
    with pytest.raises(NetworkBlockedError, match="External network access blocked"):
        socket.getaddrinfo("example.com", 443)


def test_external_socket_connection_is_blocked_by_default() -> None:
    with pytest.raises(NetworkBlockedError, match="socket.create_connection"):
        socket.create_connection(("203.0.113.10", 443), timeout=0.01)


def test_external_udp_sendto_is_blocked_by_default() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        with pytest.raises(NetworkBlockedError, match="socket.sendto"):
            sock.sendto(b"ping", ("203.0.113.10", 53))


def test_live_network_env_alone_does_not_open_unmarked_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(LIVE_NETWORK_ENV, "1")
    with pytest.raises(NetworkBlockedError, match="External network access blocked"):
        socket.getaddrinfo("example.com", 443)


def test_runtime_test_env_scrubs_proxy_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    proxy_keys = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy")
    for key in proxy_keys:
        monkeypatch.setenv(key, "http://127.0.0.1:8888")
    monkeypatch.setattr(test_isolation, "_BASE_TEST_ENV", None)

    env = test_isolation._build_base_test_env()

    assert not any(key in env for key in proxy_keys)


def test_loopback_socket_connection_is_allowed() -> None:
    ready = threading.Event()
    accepted = threading.Event()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = int(server.getsockname()[1])

        def _accept_once() -> None:
            ready.set()
            conn, _addr = server.accept()
            with conn:
                accepted.set()

        thread = threading.Thread(target=_accept_once, daemon=True)
        thread.start()
        assert ready.wait(timeout=1.0)

        with socket.create_connection(("127.0.0.1", port), timeout=1.0):
            pass

        thread.join(timeout=1.0)
        assert accepted.is_set()


@pytest.mark.live_network
def test_live_network_marker_and_env_enable_current_item_policy_without_network() -> None:
    assert external_network_allowed_for_current_test()
