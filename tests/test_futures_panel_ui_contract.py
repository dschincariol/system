from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_futures_panel_renders_from_mocked_endpoint() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed")
    script = r"""
import assert from "node:assert/strict";
import { loadFuturesPanel } from "./ui/futures_panel.js";

const elements = new Map();
function element(id) {
  if (!elements.has(id)) {
    elements.set(id, {
      id,
      innerHTML: "",
      textContent: "",
      className: "",
      dataset: {},
      setAttribute() {},
    });
  }
  return elements.get(id);
}
const ids = [
  "futuresPanelCard",
  "futuresStatePill",
  "futuresFreshnessPill",
  "futuresModePill",
  "futuresLineagePill",
  "futuresSummaryGrid",
  "futuresRollsBody",
  "futuresCurveBody",
  "futuresCotBody",
  "futuresMarginBody",
  "futuresNotes",
];
ids.forEach((id) => element(id));
const document = { getElementById: (id) => elements.get(id) || null };
const canary = "DATABENTO_CANARY_TOKEN_DO_NOT_RENDER";
const payload = {
  ok: true,
  state: "ready",
  read_only: true,
  shadow_only: true,
  generated_ts_ms: 1800000,
  latest_ts_ms: 1800000,
  summary: { roll_count: 1, curve_count: 1, cot_count: 1, margin_count: 1 },
  roll_calendar: [{ root: "ES", roll_ts_ms: 1800000, from_contract: "ESZ26", to_contract: "ESH27", gap_ratio: 1.0025 }],
  term_structure: [{ symbol: "ESZ26", root: "ES", close: 5005, open_interest: 2000, ts_ms: 1800000 }],
  cot: [{ symbol: "ES.c.0", asof_ts_ms: 1790000, noncomm_net_z: 1.25, commercial_net_pctile_3y: 0.75, open_interest_z: 0.5 }],
  margin: [{ symbol: "ES.c.0", position_qty: 2, multiplier: 50, one_contract_notional: 250000, margin_ref: 1000000 }],
  lineage: { tables: ["futures_roll_calendar", "symbols"] },
  warnings: [],
};
await loadFuturesPanel({ fetchJSON: async () => payload, document });
const rendered = Array.from(elements.values()).map((el) => `${el.innerHTML} ${el.textContent}`).join("\n");
assert.match(elements.get("futuresRollsBody").innerHTML, /ESZ26/);
assert.match(elements.get("futuresCurveBody").innerHTML, /5005/);
assert.match(elements.get("futuresCotBody").innerHTML, /ES\.c\.0/);
assert.match(elements.get("futuresMarginBody").innerHTML, /250000/);
assert.equal(rendered.includes(canary), false);
"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr or result.stdout

