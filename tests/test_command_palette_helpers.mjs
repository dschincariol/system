import assert from "node:assert/strict";
import test from "node:test";

import {
  filterCommandItems,
  fuzzyScoreText,
  isPaletteJobActionAllowed,
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

test("palette job actions use backend safety metadata", () => {
  const pollPrices = {
    name: "poll_prices",
    safety: "data_refresh",
    action_policy: { start: { enabled: true }, stop: { enabled: true } },
  };
  const brokerApplyOrders = {
    name: "broker_apply_orders",
    safety: "execution_sensitive",
    action_policy: { start: { enabled: true, safety_confirmation_required: true } },
  };
  const unavailable = {
    name: "llm_factor_discovery",
    safety: "unavailable",
    action_policy: { start: { enabled: false, disabled_reason: "Missing prerequisite" } },
  };

  assert.equal(isSafePaletteJobAction(pollPrices, "start"), true);
  assert.equal(isPaletteJobActionAllowed(pollPrices, "start"), true);
  assert.equal(isSafePaletteJobAction(brokerApplyOrders, "start"), false);
  assert.equal(isPaletteJobActionAllowed(brokerApplyOrders, "start"), true);
  assert.equal(isSafePaletteJobAction(unavailable, "start"), false);
  assert.equal(isPaletteJobActionAllowed(unavailable, "start"), false);
  assert.equal(isSafePaletteJobAction("poll_prices", "start"), false);
  assert.equal(isSafePaletteJobAction(pollPrices, "delete"), false);
});
