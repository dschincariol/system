from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MOBILE_DIR = ROOT / "ui" / "mobile"


def test_mobile_surface_is_dedicated_companion_not_dashboard_reflow():
    html = (MOBILE_DIR / "index.html").read_text(encoding="utf-8")
    js = (MOBILE_DIR / "mobile.js").read_text(encoding="utf-8")

    assert "/ui/dashboard.js" not in html
    assert "../dashboard.js" not in html
    assert "/api/terminal/order" not in js
    assert "/api/terminal/flatten" not in js
    assert "/api/operator/emergency_stop" in js


def test_mobile_manifest_icon_references_exist():
    manifest = json.loads((MOBILE_DIR / "manifest.json").read_text(encoding="utf-8"))
    icons = manifest.get("icons") or []
    assert icons, "mobile manifest must declare at least one icon"

    for icon in icons:
        src = str(icon.get("src") or "")
        assert src.startswith("/ui/")
        assert (ROOT / src.lstrip("/")).exists(), f"manifest icon is missing: {src}"


def test_mobile_service_worker_is_network_only_for_live_data():
    sw = (MOBILE_DIR / "sw.js").read_text(encoding="utf-8")

    assert "cache: \"no-store\"" in sw
    assert "caches.open" not in sw
    assert ".put(" not in sw
    assert "event.respondWith(fetch(" in sw


def test_mobile_emergency_confirmation_requires_typed_phrase_and_hold():
    helpers = (MOBILE_DIR / "mobile_helpers.mjs").read_text(encoding="utf-8")

    assert 'KILL_SWITCH_CONFIRM_PHRASE = "KILL"' in helpers
    assert "holdComplete === true" in helpers
    assert "canStartKillSwitchHold({ typedPhrase, pending })" in helpers
    js = (MOBILE_DIR / "mobile.js").read_text(encoding="utf-8")
    assert 'action_id: "operator.emergency_stop"' in js
    assert 'source_surface: "mobile_pwa"' in js
    assert 'confirmation_method: "typed_phrase_hold"' in js
    assert 'request_id: requestId("mobile-emergency-stop")' in js
