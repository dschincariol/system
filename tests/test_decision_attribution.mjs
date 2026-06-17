import assert from "node:assert/strict";
import test from "node:test";

import { normalizeDecisionAttribution } from "../ui/decision_attribution.js";

test("decision attribution normalizes signed contributions by magnitude", () => {
  const vm = normalizeDecisionAttribution({
    available: true,
    source: "decision.explain.prediction_explanation",
    explanation_type: "shap",
    is_shap: true,
    top_features: [
      { feature_id: "news_score", attribution: 0.2, value: 1.1 },
      { feature_id: "drawdown_pressure", attribution: -0.4, value: -0.7 },
      { feature_id: "volatility", attribution: 0.1, value: 0.3 },
    ],
  });

  assert.equal(vm.available, true);
  assert.equal(vm.isShap, true);
  assert.deepEqual(
    vm.rows.map((row) => row.featureId),
    ["drawdown_pressure", "news_score", "volatility"],
  );
  assert.equal(vm.rows[0].sign, "-");
  assert.equal(vm.rows[0].directionLabel, "pushes decision lower");
  assert.equal(vm.rows[0].magnitudePct, 100);
  assert.equal(vm.rows[1].magnitudePct, 50);
  assert.equal(Number(vm.rows[0].sharePct.toFixed(1)), 57.1);
});

test("decision attribution renders unavailable state when backend rows are absent", () => {
  const vm = normalizeDecisionAttribution({
    available: false,
    unavailable_reason: "No persisted prediction explanation row exists.",
  });

  assert.equal(vm.available, false);
  assert.equal(vm.rows.length, 0);
  assert.match(vm.reason, /No persisted prediction explanation/);
  assert.equal(vm.summary, "Feature attribution unavailable.");
});
