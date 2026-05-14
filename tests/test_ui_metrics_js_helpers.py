from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_dashboard_ui_metrics_helpers_use_canonical_values() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("node executable is not available")

    helper_path = REPO_ROOT / "ui" / "ui_metrics.js"
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const payload = {
  ok: true,
  schema_version: 1,
  ts_ms: 1700000000000,
  pnl: {
    today_pnl: 12.5,
    total_pnl: 30,
    realized_pnl: 10,
    unrealized_pnl: 2.5,
  },
  exposure: { gross: 0.42, net: -0.1 },
  account: { cash: 40000, equity: 100000 },
  positions: { target_count: 2, live_count: 1, order_count: 3 },
  risk: {
    max_drawdown_pct: 0.03,
    status: "ok",
    blocked: false,
    execution_barrier: { allowed: true },
  },
  sources: {
    pnl: { endpoint: "/api/pnl", missing: false, stale: false, ts_ms: 1699999999000 },
    risk_summary: { endpoint: "/api/risk/summary", missing: false, stale: false, ts_ms: 1699999998000 },
    broker: { endpoint: "/api/broker", missing: false, stale: false, ts_ms: 1699999998000 },
    portfolio: { endpoint: "/api/portfolio", missing: false, stale: false, ts_ms: 1699999998000 },
  },
  summary: { degraded: false, missing_sources: [], stale_sources: [] },
};

const pnl = mod.canonicalPnlValues(payload);
const exposure = mod.canonicalExposureValues(payload);
console.log(JSON.stringify({ pnl, exposure }));
"""

    result = subprocess.run(
        [node, "--input-type=module", "-e", code, str(helper_path)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    parsed = json.loads(result.stdout)

    assert parsed["pnl"]["today"] == 12.5
    assert parsed["pnl"]["total"] == 30
    assert parsed["pnl"]["realized"] == 10
    assert parsed["pnl"]["unrealized"] == 2.5
    assert parsed["pnl"]["missing"] is False
    assert parsed["exposure"]["gross"] == 0.42
    assert parsed["exposure"]["net"] == -0.1
    assert parsed["exposure"]["equity"] == 100000
    assert parsed["exposure"]["targetCount"] == 2
    assert parsed["exposure"]["liveCount"] == 1
    assert parsed["exposure"]["degraded"] is False
