"""Socket-level network isolation for automated Python tests.

The guard is intentionally small and dependency-free so every pytest entrypoint
can install it before test modules are collected.  It blocks DNS and outbound
socket calls before the process can reach a broker, market-data API, or public
resolver, while still allowing loopback and Unix-domain sockets for hermetic
local services.
"""

from __future__ import annotations

import errno
import ipaddress
import os
import socket
import threading
from typing import Any

LIVE_NETWORK_ENV = "TRADING_TEST_ALLOW_LIVE_NETWORK"
LOCAL_HOST_ALLOWLIST_ENV = "TRADING_TEST_SOCKET_ALLOW_HOSTS"

_LOCAL_HOSTNAMES = frozenset({"", "localhost", "localhost.", "ip6-localhost", "ip6-loopback"})
_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})

_LOCK = threading.RLock()
_INSTALLED = False
_ALLOW_EXTERNAL_FOR_CURRENT_TEST = False
_ORIGINALS: dict[str, Any] = {}


class NetworkBlockedError(OSError):
    """Raised when a test attempts non-local network access."""

    def __init__(self, operation: str, host: object, port: object | None = None) -> None:
        target = _format_target(host, port)
        message = (
            f"External network access blocked during tests: attempted {operation} to {target}. "
            "Tests may use loopback hosts (127.0.0.1, ::1, localhost) and Unix-domain sockets "
            "by default. Mark intentional live-service tests with @pytest.mark.live_network and "
            f"run them with {LIVE_NETWORK_ENV}=1; use {LOCAL_HOST_ALLOWLIST_ENV} only for "
            "explicit local-only hostnames."
        )
        super().__init__(errno.EPERM, message)


def _env_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in _TRUTHY_VALUES


def live_network_opt_in_enabled() -> bool:
    """Return whether the current pytest process opted into live network tests."""

    return _env_truthy(os.environ.get(LIVE_NETWORK_ENV))


def set_external_network_allowed_for_current_test(allowed: bool) -> None:
    """Temporarily allow non-local network access for the active pytest item."""

    global _ALLOW_EXTERNAL_FOR_CURRENT_TEST
    with _LOCK:
        _ALLOW_EXTERNAL_FOR_CURRENT_TEST = bool(allowed and live_network_opt_in_enabled())


def external_network_allowed_for_current_test() -> bool:
    with _LOCK:
        return bool(_ALLOW_EXTERNAL_FOR_CURRENT_TEST and live_network_opt_in_enabled())


def _configured_local_hosts() -> set[str]:
    raw = str(os.environ.get(LOCAL_HOST_ALLOWLIST_ENV) or "")
    return {
        _normalize_host(part)
        for part in raw.replace(";", ",").split(",")
        if _normalize_host(part)
    }


def _normalize_host(host: object) -> str:
    if host is None:
        return ""
    if isinstance(host, bytes):
        try:
            host = host.decode("ascii")
        except UnicodeDecodeError:
            host = host.decode("utf-8", errors="replace")
    text = str(host).strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    return text.lower()


def _host_is_loopback_or_unspecified(host: str) -> bool:
    if host in _LOCAL_HOSTNAMES:
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return bool(ip.is_loopback or ip.is_unspecified)


def _host_allowed(host: object) -> bool:
    normalized = _normalize_host(host)
    return bool(
        _host_is_loopback_or_unspecified(normalized)
        or normalized in _configured_local_hosts()
    )


def _format_target(host: object, port: object | None = None) -> str:
    host_text = "<default>" if host is None or host == "" else str(host)
    if port is None:
        return host_text
    return f"{host_text}:{port}"


def _guard_destination(operation: str, host: object, port: object | None = None) -> None:
    if external_network_allowed_for_current_test() or _host_allowed(host):
        return
    raise NetworkBlockedError(operation, host, port)


def _guard_socket_address(sock: socket.socket, address: object, operation: str) -> None:
    family = getattr(sock, "family", None)
    if family == socket.AF_UNIX:
        return
    if address is None:
        return
    if isinstance(address, tuple) and address:
        host = address[0]
        port = address[1] if len(address) > 1 else None
        _guard_destination(operation, host, port)


def install_socket_guard() -> None:
    """Install process-global socket/DNS guards for a pytest run."""

    global _INSTALLED
    with _LOCK:
        if _INSTALLED:
            return

        _ORIGINALS.update(
            {
                "socket_connect": socket.socket.connect,
                "socket_connect_ex": socket.socket.connect_ex,
                "socket_sendto": socket.socket.sendto,
                "create_connection": socket.create_connection,
                "getaddrinfo": socket.getaddrinfo,
                "gethostbyname": socket.gethostbyname,
                "gethostbyname_ex": socket.gethostbyname_ex,
                "gethostbyaddr": socket.gethostbyaddr,
            }
        )

        def guarded_connect(sock: socket.socket, address: object) -> Any:
            _guard_socket_address(sock, address, "socket.connect")
            return _ORIGINALS["socket_connect"](sock, address)

        def guarded_connect_ex(sock: socket.socket, address: object) -> Any:
            _guard_socket_address(sock, address, "socket.connect_ex")
            return _ORIGINALS["socket_connect_ex"](sock, address)

        def guarded_sendto(sock: socket.socket, data: bytes, *args: Any) -> Any:
            if len(args) == 1:
                address = args[0]
            elif len(args) >= 2:
                address = args[1]
            else:
                address = None
            _guard_socket_address(sock, address, "socket.sendto")
            return _ORIGINALS["socket_sendto"](sock, data, *args)

        def guarded_create_connection(
            address: tuple[object, object],
            timeout: float | object = socket._GLOBAL_DEFAULT_TIMEOUT,
            source_address: tuple[object, object] | None = None,
            *,
            all_errors: bool = False,
        ) -> Any:
            host = address[0] if address else None
            port = address[1] if len(address) > 1 else None
            _guard_destination("socket.create_connection", host, port)
            return _ORIGINALS["create_connection"](
                address,
                timeout=timeout,
                source_address=source_address,
                all_errors=all_errors,
            )

        def guarded_getaddrinfo(host: object, port: object, *args: Any, **kwargs: Any) -> Any:
            _guard_destination("socket.getaddrinfo", host, port)
            return _ORIGINALS["getaddrinfo"](host, port, *args, **kwargs)

        def guarded_gethostbyname(hostname: object) -> Any:
            _guard_destination("socket.gethostbyname", hostname)
            return _ORIGINALS["gethostbyname"](hostname)

        def guarded_gethostbyname_ex(hostname: object) -> Any:
            _guard_destination("socket.gethostbyname_ex", hostname)
            return _ORIGINALS["gethostbyname_ex"](hostname)

        def guarded_gethostbyaddr(ip_address: object) -> Any:
            _guard_destination("socket.gethostbyaddr", ip_address)
            return _ORIGINALS["gethostbyaddr"](ip_address)

        socket.socket.connect = guarded_connect
        socket.socket.connect_ex = guarded_connect_ex
        socket.socket.sendto = guarded_sendto
        socket.create_connection = guarded_create_connection
        socket.getaddrinfo = guarded_getaddrinfo
        socket.gethostbyname = guarded_gethostbyname
        socket.gethostbyname_ex = guarded_gethostbyname_ex
        socket.gethostbyaddr = guarded_gethostbyaddr
        _INSTALLED = True


def uninstall_socket_guard() -> None:
    """Restore original socket functions.

    Normal pytest execution does not need this, but it keeps targeted tests and
    embedded runners from leaking the guard into later work in the same process.
    """

    global _INSTALLED
    with _LOCK:
        if not _INSTALLED:
            return
        socket.socket.connect = _ORIGINALS["socket_connect"]
        socket.socket.connect_ex = _ORIGINALS["socket_connect_ex"]
        socket.socket.sendto = _ORIGINALS["socket_sendto"]
        socket.create_connection = _ORIGINALS["create_connection"]
        socket.getaddrinfo = _ORIGINALS["getaddrinfo"]
        socket.gethostbyname = _ORIGINALS["gethostbyname"]
        socket.gethostbyname_ex = _ORIGINALS["gethostbyname_ex"]
        socket.gethostbyaddr = _ORIGINALS["gethostbyaddr"]
        _ORIGINALS.clear()
        _INSTALLED = False
        set_external_network_allowed_for_current_test(False)
