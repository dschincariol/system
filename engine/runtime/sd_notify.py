"""Small systemd sd_notify helpers.

The helpers are safe no-ops outside a systemd notify service. They never raise:
startup and watchdog supervision must not become an additional runtime failure
source when the service manager socket is absent or unavailable.
"""

from __future__ import annotations

import os
import socket


def _notify_socket_address(raw: str) -> str | bytes:
    if raw.startswith("@"):
        return b"\0" + raw[1:].encode()
    return raw


def notify(state: str) -> bool:
    """Send a raw sd_notify state datagram if ``NOTIFY_SOCKET`` is set."""
    notify_socket = str(os.environ.get("NOTIFY_SOCKET") or "").strip()
    if not notify_socket:
        return False
    payload = str(state or "").strip()
    if not payload:
        return False

    try:
        address = _notify_socket_address(notify_socket)
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.sendto(payload.encode("utf-8"), address)
        return True
    except OSError:
        return False
    except Exception:
        return False


def notify_ready() -> bool:
    """Notify systemd that startup completed successfully."""
    return notify("READY=1")


def notify_watchdog() -> bool:
    """Notify systemd that the runtime watchdog liveness check passed."""
    return notify("WATCHDOG=1")


__all__ = ["notify", "notify_ready", "notify_watchdog"]
