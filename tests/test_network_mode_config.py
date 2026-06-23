"""Config + URL/banner generation tests for the LAN network-access mode.

These cover the shared helper in ``engine/runtime/platform.py`` that expands
``TRADING_NETWORK_MODE=lan`` into the dashboard bind-host default and renders
the operator-facing startup banner (local + LAN URLs). The operator sidecar
(:4001) intentionally remains internal/loopback by default and is reached
through the dashboard bridge.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from engine.runtime import platform as platform_mod
from engine.runtime.platform import (
    apply_network_mode_bind_defaults,
    is_wildcard_host,
    network_access_banner_lines,
    resolve_lan_advertise_ip,
    resolve_network_mode,
)


class NetworkModeResolutionTests(unittest.TestCase):
    def test_default_mode_is_local(self) -> None:
        self.assertEqual(resolve_network_mode({}), "local")
        self.assertEqual(resolve_network_mode({"TRADING_NETWORK_MODE": ""}), "local")
        self.assertEqual(resolve_network_mode({"TRADING_NETWORK_MODE": "garbage"}), "local")

    def test_lan_aliases_resolve_to_lan(self) -> None:
        for value in ("lan", "LAN", "host", "server", "remote", "0.0.0.0", "wildcard"):
            with self.subTest(value=value):
                self.assertEqual(
                    resolve_network_mode({"TRADING_NETWORK_MODE": value}), "lan"
                )

    def test_is_wildcard_host(self) -> None:
        self.assertTrue(is_wildcard_host("0.0.0.0"))
        self.assertTrue(is_wildcard_host("::"))
        self.assertFalse(is_wildcard_host("127.0.0.1"))
        self.assertFalse(is_wildcard_host("192.168.0.165"))


class ApplyBindDefaultsTests(unittest.TestCase):
    def test_local_mode_is_a_noop(self) -> None:
        env: dict[str, str] = {}
        applied = apply_network_mode_bind_defaults(env)
        self.assertEqual(applied, {})
        self.assertNotIn("DASHBOARD_HOST", env)
        self.assertNotIn("OPERATOR_BIND_HOST", env)

    def test_lan_mode_sets_only_dashboard_to_wildcard(self) -> None:
        env = {"TRADING_NETWORK_MODE": "lan"}
        applied = apply_network_mode_bind_defaults(env)
        self.assertEqual(env["DASHBOARD_HOST"], "0.0.0.0")
        self.assertNotIn("OPERATOR_BIND_HOST", env)
        self.assertEqual(applied, {"DASHBOARD_HOST": "0.0.0.0"})

    def test_lan_mode_never_overwrites_explicit_hosts(self) -> None:
        env = {
            "TRADING_NETWORK_MODE": "lan",
            "DASHBOARD_HOST": "192.168.0.165",
            "OPERATOR_BIND_HOST": "127.0.0.1",
        }
        applied = apply_network_mode_bind_defaults(env)
        self.assertEqual(env["DASHBOARD_HOST"], "192.168.0.165")
        self.assertEqual(env["OPERATOR_BIND_HOST"], "127.0.0.1")
        self.assertEqual(applied, {})

    def test_idempotent(self) -> None:
        env = {"TRADING_NETWORK_MODE": "lan"}
        first = apply_network_mode_bind_defaults(env)
        second = apply_network_mode_bind_defaults(env)
        self.assertEqual(first, {"DASHBOARD_HOST": "0.0.0.0"})
        self.assertEqual(second, {})  # nothing left to apply


class LanAdvertiseIpTests(unittest.TestCase):
    def test_explicit_lan_ip_wins(self) -> None:
        self.assertEqual(
            resolve_lan_advertise_ip({"TRADING_LAN_IP": "10.0.0.7"}), "10.0.0.7"
        )

    def test_falls_back_to_documented_default_when_undetectable(self) -> None:
        # Force detection to fail so we exercise the documented fallback.
        original = platform_mod._detect_primary_lan_ip
        platform_mod._detect_primary_lan_ip = lambda: ""  # type: ignore[assignment]
        try:
            self.assertEqual(
                resolve_lan_advertise_ip({}), platform_mod.DEFAULT_LAN_ADVERTISE_IP
            )
        finally:
            platform_mod._detect_primary_lan_ip = original  # type: ignore[assignment]


class BannerGenerationTests(unittest.TestCase):
    def test_loopback_bind_has_no_lan_url(self) -> None:
        lines = network_access_banner_lines(
            service="dashboard_server",
            bind_host="127.0.0.1",
            port=8000,
            environ={"ENGINE_MODE": "safe"},
        )
        joined = "\n".join(lines)
        self.assertIn("local URL:  http://127.0.0.1:8000/", joined)
        self.assertNotIn("LAN URL", joined)

    def test_wildcard_bind_emits_lan_url_and_token_warning(self) -> None:
        lines = network_access_banner_lines(
            service="dashboard_server",
            bind_host="0.0.0.0",
            port=8000,
            environ={
                "ENGINE_MODE": "safe",
                "TRADING_NETWORK_MODE": "lan",
                "TRADING_LAN_IP": "192.168.0.165",
            },
        )
        joined = "\n".join(lines)
        # Local URL collapses 0.0.0.0 -> loopback (you cannot browse to 0.0.0.0).
        self.assertIn("local URL:  http://127.0.0.1:8000/", joined)
        self.assertIn("LAN URL:    http://192.168.0.165:8000/", joined)
        self.assertIn("network_mode=lan", joined)
        # No DASHBOARD_API_TOKEN -> security reminder.
        self.assertTrue(any("WARNING" in line and "DASHBOARD_API_TOKEN" in line for line in lines))

    def test_token_present_suppresses_warning(self) -> None:
        lines = network_access_banner_lines(
            service="dashboard_server",
            bind_host="0.0.0.0",
            port=8000,
            environ={
                "ENGINE_MODE": "safe",
                "TRADING_NETWORK_MODE": "lan",
                "TRADING_LAN_IP": "192.168.0.165",
                "DASHBOARD_API_TOKEN": "x" * 24,
            },
        )
        self.assertFalse(any("WARNING" in line for line in lines))


if __name__ == "__main__":
    unittest.main()
