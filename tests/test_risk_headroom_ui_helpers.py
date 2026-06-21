from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_node(code: str, *paths: Path) -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node executable is not available")
    result = subprocess.run(
        [node, "--input-type=module", "-e", code, *[str(path) for path in paths]],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    return json.loads(result.stdout)


def test_bullet_bar_view_models_use_caps_and_blocked_state() -> None:
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildRiskHeadroomViewModel({
  uiMetrics: {
    ok: true,
    exposure: { gross: 0.75, net: -0.50 },
    risk: {
      max_drawdown_pct: 0.055,
      vol_proxy: 0.018,
      blocked: true,
      caps: { gross: 1.0, net: 0.60, drawdown: 0.06, vol_target: 0.02 },
      vol_target: { target_vol: 0.02 },
    },
  },
  portfolioRisk: {
    ok: true,
    blocked: true,
    caps: { gross: 1.0, net: 0.60, drawdown: 0.06, vol_target: 0.02 },
    history: [{ gross: 0.7, net: -0.4, vol_proxy: 0.017, drawdown: 0.04 }],
  },
});

console.log(JSON.stringify({
  blocked: vm.blocked,
  caps: vm.caps,
  bars: vm.bars.map((bar) => ({
    id: bar.id,
    value: bar.value,
    cap: bar.cap,
    capPct: Number(bar.capPct.toFixed(2)),
    fillPct: Number(bar.fillPct.toFixed(2)),
    tone: bar.tone,
    statusWord: bar.statusWord,
    ratio: Number(bar.ratio.toFixed(4)),
    fallbackText: bar.fallbackText,
  })),
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "bullet_bars.js")

    assert parsed["blocked"] is True
    assert parsed["caps"]["gross"] == 1.0
    assert parsed["caps"]["net"] == 0.6
    assert parsed["caps"]["drawdown"] == 0.06
    assert parsed["caps"]["vol"] == 0.02
    by_id = {row["id"]: row for row in parsed["bars"]}
    assert by_id["gross-exposure"]["ratio"] == 0.75
    assert by_id["gross-exposure"]["capPct"] == 80.0
    assert by_id["gross-exposure"]["fillPct"] == 60.0
    assert by_id["net-exposure"]["ratio"] == 0.8333
    assert by_id["vol-proxy"]["ratio"] == 0.9
    assert by_id["drawdown"]["ratio"] == 0.9167
    assert {row["tone"] for row in parsed["bars"]} == {"blocked"}
    assert "against cap" in by_id["drawdown"]["fallbackText"]


def test_risk_headroom_uses_latest_timestamp_for_ascending_and_descending_history() -> None:
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const rows = [
  { ts_ms: 1000, gross: 0.20, net: 0.10, vol_proxy: 0.010, drawdown: 0.010 },
  { ts_ms: 2000, gross: 0.40, net: 0.20, vol_proxy: 0.012, drawdown: 0.020 },
  { ts_ms: 3000, gross: 0.95, net: -0.45, vol_proxy: 0.018, drawdown: 0.055 },
];
const build = (history) => mod.buildRiskHeadroomViewModel({
  portfolioRisk: {
    ok: true,
    caps: { gross: 1.0, net: 0.60, drawdown: 0.06, vol_target: 0.02 },
    history,
  },
});
const asc = build(rows);
const desc = build([...rows].reverse());
const values = (vm) => Object.fromEntries(vm.bars.map((bar) => [bar.id, {
  value: bar.value,
  ratio: Number(bar.ratio.toFixed(4)),
  tone: bar.tone,
}]));
console.log(JSON.stringify({
  latestAsc: mod.latestRiskRow({ history: rows }),
  latestDesc: mod.latestRiskRow({ history: [...rows].reverse() }),
  asc: values(asc),
  desc: values(desc),
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "bullet_bars.js")

    assert parsed["latestAsc"]["ts_ms"] == 3000
    assert parsed["latestDesc"]["ts_ms"] == 3000
    assert parsed["asc"] == parsed["desc"]
    assert parsed["asc"]["gross-exposure"]["value"] == 0.95
    assert parsed["asc"]["net-exposure"]["value"] == -0.45
    assert parsed["asc"]["vol-proxy"]["value"] == 0.018
    assert parsed["asc"]["drawdown"]["value"] == 0.055


def test_risk_headroom_bands_and_status_share_boundary_constants() -> None:
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const thresholds = mod.RISK_HEADROOM_THRESHOLDS;
const samples = [0.8499, thresholds.watchStart, 0.90, thresholds.cap, 1.0001];
const bars = samples.map((ratio) => mod.buildBulletBarViewModel({
  label: `ratio ${ratio}`,
  value: ratio,
  cap: 1.0,
}));
console.log(JSON.stringify({
  thresholds,
  defaultBands: mod.RISK_HEADROOM_BANDS,
  samples: bars.map((bar) => ({
    ratio: Number(bar.ratio.toFixed(4)),
    tone: bar.tone,
    statusWord: bar.statusWord,
  })),
}));
"""
    parsed = _run_node(
        code,
        REPO_ROOT / "ui" / "bullet_bars.js",
    )

    assert parsed["thresholds"]["watchStart"] == 0.85
    assert parsed["thresholds"]["cap"] == 1.0
    by_key = {row["key"]: row for row in parsed["defaultBands"]}
    assert by_key["ok"]["end"] == parsed["thresholds"]["watchStart"]
    assert by_key["watch"]["start"] == parsed["thresholds"]["watchStart"]
    assert by_key["watch"]["end"] == parsed["thresholds"]["cap"]
    assert by_key["over"]["start"] == parsed["thresholds"]["cap"]
    assert [row["tone"] for row in parsed["samples"]] == ["ok", "watch", "watch", "watch", "over"]
    assert [row["statusWord"] for row in parsed["samples"]] == ["OK", "Watch", "Watch", "Watch", "Over cap"]


def test_risk_thresholds_caps_and_portfolio_drawdown_throttle_share_source() -> None:
    code = r"""
import { pathToFileURL } from "node:url";

const bullet = await import(pathToFileURL(process.argv[1]).href);
const shared = await import(pathToFileURL(process.argv[2]).href);
const portfolio = await import(pathToFileURL(process.argv[3]).href);

console.log(JSON.stringify({
  sameCapsObject: bullet.DEFAULT_RISK_CAPS === shared.DEFAULT_RISK_CAPS,
  sameBandsObject: bullet.RISK_HEADROOM_BANDS === shared.RISK_HEADROOM_BANDS,
  sameThresholdsObject: bullet.RISK_HEADROOM_THRESHOLDS === shared.RISK_HEADROOM_THRESHOLDS,
  caps: shared.DEFAULT_RISK_CAPS,
  portfolioDrawdownThrottle: portfolio.PORTFOLIO_DRAWDOWN_THROTTLE,
  overCapTone: bullet.classifyRiskHeadroomRatio(shared.RISK_HEADROOM_THRESHOLDS.cap + 0.001).tone,
}));
"""
    parsed = _run_node(
        code,
        REPO_ROOT / "ui" / "bullet_bars.js",
        REPO_ROOT / "ui" / "risk_headroom_thresholds.js",
        REPO_ROOT / "ui" / "portfolio_backtest.js",
    )

    assert parsed["sameCapsObject"] is True
    assert parsed["sameBandsObject"] is True
    assert parsed["sameThresholdsObject"] is True
    assert parsed["caps"] == {"gross": 1.0, "net": 0.6, "drawdown": 0.06, "vol": 0.02}
    assert parsed["portfolioDrawdownThrottle"] == -parsed["caps"]["drawdown"]
    assert parsed["overCapTone"] == "over"


def test_bullet_bar_defaults_fail_gracefully_when_data_missing() -> None:
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildRiskHeadroomViewModel({});
console.log(JSON.stringify({
  ok: vm.ok,
  caps: vm.caps,
  bars: vm.bars.map((bar) => ({ id: bar.id, missing: bar.missing, tone: bar.tone, fallbackText: bar.fallbackText })),
}));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "bullet_bars.js")

    assert parsed["ok"] is False
    assert parsed["caps"]["gross"] == 1.0
    assert parsed["caps"]["net"] == 0.6
    assert parsed["caps"]["drawdown"] == 0.06
    assert parsed["caps"]["vol"] == 0.02
    assert all(row["missing"] for row in parsed["bars"])
    assert all(row["tone"] == "unavailable" for row in parsed["bars"])
    assert all("data unavailable" in row["fallbackText"] for row in parsed["bars"])


def test_regime_ribbon_fallback_is_accessible_and_non_throwing() -> None:
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const vm = mod.buildRegimeRibbonViewModel({ ok: false, source: "/api/regime/context", layers: {} });
console.log(JSON.stringify(vm));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "regime_ribbon.js")

    assert parsed["degraded"] is True
    assert parsed["fallbackText"] == "Regime context unavailable."
    assert [row["regimeLabel"] for row in parsed["items"]] == ["UNKNOWN", "UNKNOWN", "UNKNOWN"]
    assert all(row["tone"] == "unavailable" for row in parsed["items"])


def test_kill_switch_rows_have_real_activation_controls() -> None:
    code = r"""
import { pathToFileURL } from "node:url";

const mod = await import(pathToFileURL(process.argv[1]).href);
const rows = mod.buildKillSwitchRows({
  state: [{ scope: "global", key: "global", enabled: 1, reason: "unit_test" }],
  auto_pipeline: { enabled: false, reason: "AUTO_PIPELINE=0" },
});
console.log(JSON.stringify(rows));
"""
    parsed = _run_node(code, REPO_ROOT / "ui" / "kill_switch_ui.js")

    by_id = {row["id"]: row for row in parsed}
    assert by_id["global:global"]["action"] == "explain"
    assert "Explain kill switch global:global enabled" == by_id["global:global"]["ariaLabel"]
    assert by_id["auto_pipeline"]["action"] == "hint"
    assert by_id["auto_pipeline"]["actionLabel"] == "Recovery hint"

    source = (REPO_ROOT / "ui" / "kill_switch_ui.js").read_text(encoding="utf-8")
    assert "data-ks-action" in source
    assert "clientY" not in source
    assert "lineHeight" not in source
