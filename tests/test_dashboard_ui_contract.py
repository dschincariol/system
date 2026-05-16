from __future__ import annotations

import functools
import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.check_dashboard_ui_contract import (  # noqa: E402
    collect_dashboard_asset_graph,
    collect_dashboard_endpoint_references,
    collect_dashboard_js_modules,
    find_js_syntax_issues,
    find_unregistered_endpoint_references,
    route_path_registered,
)


OPTIONAL_OR_DEGRADED_API_ENDPOINT_ALLOWLIST: dict[str, str] = {
    # Intentionally empty: UI /api paths must be registered unless this
    # allowlist documents a consciously degraded/optional backend dependency.
}

WEBSOCKET_ENDPOINT_ALLOWLIST = {
    "/ws/operator": (
        "Operator realtime channel is served by the operator/control-plane "
        "surface; the dashboard keeps polling fallback behavior when it is absent."
    ),
}

FALLBACK_ONLY_UI_ENDPOINT_ALLOWLIST = {
    "/api/alerts/by_id": "Incident drawer detail remains dashboard-server fallback-only until alert detail routes move into ops route specs.",
    "/api/alerts/timeline": "Alert timeline cards use the dashboard-server alerts fallback until timeline ownership moves into ops route specs.",
    "/api/alerts/{param}/ack": "Alert acknowledgement is UI-critical and intentionally protected by a dashboard-server fallback mutation.",
    "/api/alerts/{param}/resolve": "Alert resolution is UI-critical and intentionally protected by a dashboard-server fallback mutation.",
    "/api/audit/records": "Audit record drilldowns use dashboard-server fallback reads until audit routes are split into canonical route specs.",
    "/api/broker": "Broker summary cards depend on the dashboard-server fallback while broker read routes are not split into route specs.",
    "/api/causal/scores": "Model analysis panel uses a dashboard-server fallback until causal score reads have a canonical route module.",
    "/api/champion/rollback": "Rollback remains a guarded dashboard-server fallback mutation with confirmation checks.",
    "/api/copilot/ask": "Read-only copilot requests are intentionally fallback-only until a dedicated copilot route module exists.",
    "/api/crash_analytics": "Crash analytics panel is fallback-only until runtime diagnostics routes own the endpoint.",
    "/api/db/health": "Database health is a dashboard UI hard-dependency kept as a dashboard-server fallback.",
    "/api/equity_drift": "Positions diagnostics use the dashboard-server fallback until equity drift reads have a canonical route module.",
    "/api/execution/overlays": "Execution overlay cards use a dashboard-server fallback until execution analytics routes own the endpoint.",
    "/api/feeds": "Mobile broker and feed status uses the dashboard-server fallback until feed status reads have a canonical route module.",
    "/api/governance/summary": "Governance summary is fallback-only while governance route specs do not publish the dashboard read path.",
    "/api/market_stress": "Market stress overview cards use the dashboard-server fallback while the stress route remains dashboard-local.",
    "/api/market_stress_history": "Market stress history charts use the dashboard-server fallback while the stress route remains dashboard-local.",
    "/api/model/metrics": "Model metrics analysis uses the dashboard-server fallback until model metric reads have canonical route specs.",
    "/api/operator/bootstrap": "Operator bootstrap state is a dashboard hard-dependency kept behind a dashboard-server fallback.",
    "/api/operator/logs": "Operator log viewer uses dashboard-server fallback routing for local process log tails.",
    "/api/operator/emergency_stop": "Mobile emergency stop remains a guarded dashboard-server fallback mutation with typed and hold confirmation.",
    "/api/operator/readiness": "Operator readiness aliases dashboard readiness through a dashboard-server fallback path.",
    "/api/operator/sidecar_status": "Operator console bridge status is dashboard-local while the Node sidecar remains the owner of the console.",
    "/api/operator/status": "Operator summary needs status even when split operator route specs are incomplete.",
    "/api/operator/stderr_tail": "Operator log viewer uses dashboard-server fallback routing for stderr tail access.",
    "/api/pnl": "PnL summary remains dashboard-server fallback-only until account/PnL reads move to canonical route specs.",
    "/api/portfolio": "Portfolio overview cards use a dashboard-server fallback until portfolio reads have canonical route specs.",
    "/api/promotion/audit": "Promotion audit panel uses a dashboard-server fallback until promotion audit reads move into route specs.",
    "/api/promotion/enable": "Promotion toggle remains a guarded dashboard-server fallback mutation with confirmation checks.",
    "/api/promotion/explain": "Promotion explanation panel uses a dashboard-server fallback until governance route specs publish it.",
    "/api/promotion/status": "Promotion status is UI-critical and remains fallback-only while governance route specs are incomplete.",
    "/api/reconcile/broker_backtest": "Broker/backtest reconciliation card uses a dashboard-server fallback read.",
    "/api/relevance/stats": "Relevance stats panel uses a dashboard-server fallback until relevance routes are canonical.",
    "/api/risk/summary": "Risk summary cards use a dashboard-server fallback until risk summary reads have canonical route specs.",
    "/api/strategy/metrics": "Strategy metrics panel uses a dashboard-server fallback until strategy metric reads have canonical route specs.",
    "/api/strategy/size_policy": "Size-policy panel uses a dashboard-server fallback read until strategy route specs own it.",
    "/api/strategy/size_policy/train": "Size-policy training remains a guarded dashboard-server fallback mutation with confirmation checks.",
    "/api/strategy/status": "Strategy status cards use a dashboard-server fallback until strategy route specs own the read.",
    "/api/system/fix": "System autofix remains a guarded dashboard-server fallback mutation with confirmation checks.",
    "/api/temporal/shadow_eval": "Temporal shadow evaluation panel uses a dashboard-server fallback until temporal routes own it.",
    "/api/ui/decision": "Decision detail modal is dashboard-local and protected by a dashboard-server fallback read.",
    "/api/ui/decisions": "Decision list panel is dashboard-local and protected by a dashboard-server fallback read.",
    "/api/ui/interaction": "UI interaction audit writes are dashboard-local and protected by a dashboard-server fallback mutation.",
}


@functools.lru_cache(maxsize=1)
def _dashboard_route_snapshot():
    code = r"""
import json
import os

os.environ.setdefault("TIMESCALE_ENABLED", "0")
os.environ.setdefault("FEATURE_STORE_ENABLED", "0")
os.environ.setdefault("FEATURE_STORE_INIT_ON_STARTUP", "0")
os.environ.setdefault("ENGINE_PRIMARY_BOOTSTRAP_DONE", "1")

import dashboard_server

def _norm(route):
    if isinstance(route, dict):
        return {
            "method": str(route.get("method") or "").upper(),
            "path": str(route.get("path") or ""),
            "handler": str(route.get("handler") or ""),
        }
    return {
        "method": str(route[0] or "").upper(),
        "path": str(route[1] or ""),
        "handler": str(route[2] or ""),
    }

print("__DASHBOARD_ROUTE_SNAPSHOT__" + json.dumps({
    "route_specs": [_norm(route) for route in dashboard_server.ROUTE_SPECS],
    "fallback_route_specs": [_norm(route) for route in dashboard_server._FALLBACK_ROUTE_SPECS],
    "raw_route_specs": [_norm(route) for route in dashboard_server._RAW_ROUTE_SPECS],
}, sort_keys=True), flush=True)
"""
    env = dict(os.environ)
    env.setdefault("TIMESCALE_ENABLED", "0")
    env.setdefault("FEATURE_STORE_ENABLED", "0")
    env.setdefault("FEATURE_STORE_INIT_ON_STARTUP", "0")
    env.setdefault("ENGINE_PRIMARY_BOOTSTRAP_DONE", "1")
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    prefix = "__DASHBOARD_ROUTE_SNAPSHOT__"
    for line in reversed((result.stdout or "").splitlines()):
        if line.startswith(prefix):
            return json.loads(line.removeprefix(prefix))
    raise AssertionError(f"dashboard route snapshot missing from subprocess output:\n{result.stdout}\n{result.stderr}")


def _canonical_route_specs(route_snapshot):
    fallback_len = len(route_snapshot["fallback_route_specs"])
    if fallback_len <= 0:
        return list(route_snapshot["raw_route_specs"])
    return list(route_snapshot["raw_route_specs"][:-fallback_len])


def _api_endpoint_paths(refs):
    return sorted({ref.path for ref in refs if ref.transport in {"http", "eventsource"}})


def test_dashboard_html_js_surface_static_smoke():
    assets, asset_issues = collect_dashboard_asset_graph(root=REPO_ROOT)
    assert asset_issues == [], "dashboard asset issues:\n- " + "\n- ".join(
        f"{issue.source_path}:{issue.line} {issue.reason} {issue.raw_ref} -> {issue.resolved_path}"
        for issue in asset_issues
    )

    js_modules = collect_dashboard_js_modules(root=REPO_ROOT)
    assert {"ui/dashboard.js", "ui/data_sources.js", "ui/terminal/terminal.js"}.issubset(set(js_modules))

    syntax_issues = find_js_syntax_issues(js_modules, root=REPO_ROOT)
    assert syntax_issues == [], "dashboard JS syntax issues:\n- " + "\n- ".join(
        f"{issue.source_path}: {issue.detail}" for issue in syntax_issues
    )
    assert assets, "dashboard asset graph should not be empty"


def test_dashboard_unsafe_job_buttons_fail_closed_with_execution_barrier():
    html = (REPO_ROOT / "ui" / "dashboard.html").read_text(encoding="utf-8")
    js = (REPO_ROOT / "ui" / "dashboard.js").read_text(encoding="utf-8")

    assert 'data-job="broker_apply_orders" data-action="start"' in html
    assert "isSafePaletteJobAction" in js
    assert "function syncJobActionSafetyState" in js
    assert "Execution barrier/read-only mode blocks this job." in js
    assert "window.__LAST_EXECUTION_BARRIER__ = j;" in js
    assert "syncJobActionSafetyState();" in js


def test_dashboard_ui_api_paths_are_registered_or_documented():
    route_snapshot = _dashboard_route_snapshot()
    refs = collect_dashboard_endpoint_references(root=REPO_ROOT)

    for path, reason in OPTIONAL_OR_DEGRADED_API_ENDPOINT_ALLOWLIST.items():
        assert path.startswith("/api/")
        assert len(reason.strip()) >= 20

    issues = find_unregistered_endpoint_references(
        refs,
        route_snapshot["route_specs"],
        optional_allowlist=OPTIONAL_OR_DEGRADED_API_ENDPOINT_ALLOWLIST,
    )

    assert issues == [], "unregistered dashboard UI endpoints:\n- " + "\n- ".join(
        f"{issue.source_path}:{issue.line} {issue.transport} {issue.path}"
        for issue in issues
    )


def test_dashboard_realtime_paths_are_checked_separately():
    route_snapshot = _dashboard_route_snapshot()
    refs = collect_dashboard_endpoint_references(root=REPO_ROOT)

    websocket_paths = sorted({ref.path for ref in refs if ref.transport == "websocket"})
    assert websocket_paths == sorted(WEBSOCKET_ENDPOINT_ALLOWLIST)
    for path, reason in WEBSOCKET_ENDPOINT_ALLOWLIST.items():
        assert path.startswith(("/ws/", "/socket/"))
        assert len(reason.strip()) >= 20

    eventsource_paths = sorted({ref.path for ref in refs if ref.transport == "eventsource"})
    assert eventsource_paths == ["/api/market/stream"]
    missing_eventsource_routes = [
        path for path in eventsource_paths if not route_path_registered(route_snapshot["route_specs"], path)
    ]
    assert missing_eventsource_routes == []


def test_dashboard_fallback_only_ui_endpoint_boundary_is_documented():
    route_snapshot = _dashboard_route_snapshot()
    refs = collect_dashboard_endpoint_references(root=REPO_ROOT)
    canonical_routes = _canonical_route_specs(route_snapshot)

    fallback_only_paths = sorted(
        path
        for path in _api_endpoint_paths(refs)
        if route_path_registered(route_snapshot["route_specs"], path)
        and not route_path_registered(canonical_routes, path)
    )

    for path, reason in FALLBACK_ONLY_UI_ENDPOINT_ALLOWLIST.items():
        assert path.startswith("/api/")
        assert len(reason.strip()) >= 20

    assert fallback_only_paths == sorted(FALLBACK_ONLY_UI_ENDPOINT_ALLOWLIST), (
        "fallback-only UI endpoint documentation drift:\n"
        f"actual={fallback_only_paths}\n"
        f"documented={sorted(FALLBACK_ONLY_UI_ENDPOINT_ALLOWLIST)}"
    )
