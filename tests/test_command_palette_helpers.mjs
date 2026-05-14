import assert from "node:assert/strict";
import test from "node:test";

import {
  filterCommandItems,
  fuzzyScoreText,
  isSafePaletteJobAction,
  parseDecisionIdQuery,
} from "../ui/command_palette.mjs";

test("fuzzy scoring rewards direct and sequential matches", () => {
  assert.ok(fuzzyScoreText("job", "Job Console") > 0);
  assert.ok(fuzzyScoreText("jc", "Job Console") > 0);
  assert.equal(fuzzyScoreText("zz", "Job Console"), 0);
  assert.ok(fuzzyScoreText("job", "Job Console") > fuzzyScoreText("jc", "Job Console"));
});

test("filterCommandItems sorts useful commands above weaker matches", () => {
  const items = [
    { id: "screen:overview", title: "Go to Overview", keywords: ["screen"], priority: 80 },
    { id: "panel:jobs", title: "Open Job Console", keywords: ["jobs"], priority: 60 },
    { id: "symbol:spy", title: "Focus symbol SPY", keywords: ["ticker"], priority: 40 },
  ];
  const results = filterCommandItems(items, "job", { limit: 5 });
  assert.equal(results[0].id, "panel:jobs");
});

test("decision ID parser accepts pasted IDs without broad text matches", () => {
  assert.equal(parseDecisionIdQuery("12345"), "12345");
  assert.equal(parseDecisionIdQuery("decision 12345"), "12345");
  assert.equal(parseDecisionIdQuery("#12345"), "12345");
  assert.equal(parseDecisionIdQuery("decision latest"), "");
});

test("palette job actions exclude trading and destructive job names", () => {
  assert.equal(isSafePaletteJobAction("poll_prices", "start"), true);
  assert.equal(isSafePaletteJobAction("portfolio_backtest", "start"), true);
  assert.equal(isSafePaletteJobAction("broker_apply_orders", "start"), false);
  assert.equal(isSafePaletteJobAction("portfolio_rebalance", "start"), false);
  assert.equal(isSafePaletteJobAction("force_promote", "start"), false);
  assert.equal(isSafePaletteJobAction("poll_prices", "delete"), false);
});
