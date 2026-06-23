"""Static contract: browser-side code must not hard-code loopback/direct-sidecar
hosts for links/origins that have to work from a remote LAN browser.

A Windows desktop opening ``http://192.168.0.165:8000`` must never be handed a
``127.0.0.1``/``localhost`` URL for the dashboard or operator bridge -- those
resolve to the *client* machine, not the server. The operator sidecar must not
be exposed as a direct LAN browser target; client code should use the
same-origin dashboard bridge. These are text-level assertions (the repo's
established idiom for UI contracts) so they run without a browser.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


OPERATOR_UI = REPO_ROOT / "boot" / "operator_ui.html"
DASHBOARD_JS = REPO_ROOT / "ui" / "dashboard.js"


class OperatorUiUrlContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = OPERATOR_UI.read_text(encoding="utf-8")

    def test_operator_ui_does_not_synthesize_direct_sidecar_origin(self) -> None:
        self.assertNotIn("OPERATOR_DIRECT_ORIGIN", self.text)
        self.assertNotIn(":4001", self.text)

    def test_dashboard_link_uses_browser_host_not_config_host(self) -> None:
        # Regression guard: the old code forced the link host from the
        # server-reported DASHBOARD_HOST (then rewrote 0.0.0.0/localhost to
        # 127.0.0.1), which breaks remote access.
        self.assertNotIn('let host = cfg.DASHBOARD_HOST || "127.0.0.1";', self.text)
        self.assertIn("updateDashboardLink", self.text)

    def test_no_hardcoded_dashboard_localhost_link(self) -> None:
        self.assertNotIn("http://127.0.0.1:8000/ui/data_sources.html", self.text)

    def test_telemetry_ws_derives_from_location_host(self) -> None:
        # Direct and bridged telemetry attempts must use the page host/origin.
        self.assertIn("location.host", self.text)
        self.assertIn("/ws/operator", self.text)


class DashboardJsUrlContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = DASHBOARD_JS.read_text(encoding="utf-8")

    def test_console_link_uses_same_origin_bridge(self) -> None:
        self.assertIn("_operatorBridgeUrlForClient", self.text)
        self.assertIn("/operator/", self.text)
        self.assertIn("Open Operator Bridge", self.text)

    def test_direct_sidecar_link_not_assigned_or_synthesized(self) -> None:
        self.assertNotIn("directLink.href = directUrl;", self.text)
        self.assertNotIn("Open Port 4001", self.text)
        self.assertNotIn("host:4001", self.text)


if __name__ == "__main__":
    unittest.main()
