import assert from "node:assert/strict";
import test from "node:test";

import {
  buildDecisionStepperModel,
  renderDecisionStepper,
  renderDecisionStepperHtml,
} from "../ui/decision_stepper.js";

const TS = Date.UTC(2026, 0, 2, 14, 30);

function stage(key, label, status = "available", summary = `${label} ok`, extra = {}) {
  return {
    key,
    label,
    status,
    summary,
    ts_ms: TS,
    ...extra,
  };
}

function basePassStages(overrides = {}) {
  return [
    stage("source", "Source signal"),
    stage("model", "Model decision"),
    stage("policy", "Risk and policy checks"),
    stage("portfolio", "Portfolio intent"),
    stage("route", "Route"),
    stage("outcome", "Outcome", "executed", "fill recorded"),
  ].map((row) => ({ ...row, ...(overrides[row.key] || {}) }));
}

test("decision stepper renders pass flow through fill", () => {
  const payload = { stages: basePassStages() };
  const target = { innerHTML: "" };
  const model = renderDecisionStepper(target, payload);

  assert.deepEqual(model.stages.map((item) => item.label), [
    "Signal",
    "Confidence/Calibration",
    "Risk/Suppression",
    "Sizing",
    "Order",
    "Fill",
  ]);
  assert.equal(model.summary.stageKey, "");
  assert.match(model.summary.text, /reached Fill/);
  assert.match(target.innerHTML, /role="group"/);
  assert.match(target.innerHTML, /<details/);
  assert.match(target.innerHTML, /<summary/);
  assert.match(target.innerHTML, /aria-label="Stage 6 of 6, Fill, Filled/);
});

test("decision stepper emphasizes partial stage when no blocker exists", () => {
  const model = buildDecisionStepperModel({
    stages: basePassStages({
      model: { status: "partial", summary: "calibration bucket missing" },
    }),
  });

  assert.equal(model.summary.stageKey, "confidence");
  assert.match(model.summary.text, /Confidence\/Calibration has partial detail/);
  assert.equal(model.stages.find((item) => item.key === "confidence").isEmphasized, true);
  assert.equal(model.stages.find((item) => item.key === "confidence").tone, "warn");
});

test("decision stepper stops at first suppressed risk stage", () => {
  const model = buildDecisionStepperModel({
    stages: basePassStages({
      policy: { status: "suppressed", summary: "max_position" },
      route: { status: "suppressed", summary: "No broker route was created." },
      outcome: { status: "suppressed", summary: "Trade suppressed." },
    }),
  });

  assert.equal(model.summary.stageKey, "risk");
  assert.match(model.summary.text, /Risk\/Suppression suppressed: max_position/);
  assert.equal(model.stages.find((item) => item.key === "risk").icon, "!");
  assert.equal(model.stages.find((item) => item.key === "risk").timestamp.includes("2026-01-02"), true);
});

test("decision stepper stops at first blocked stage with critical tone", () => {
  const model = buildDecisionStepperModel({
    stages: basePassStages({
      policy: { status: "blocked", summary: "kill switch active" },
    }),
  });

  const risk = model.stages.find((item) => item.key === "risk");
  assert.equal(model.summary.stageKey, "risk");
  assert.equal(risk.tone, "crit");
  assert.equal(risk.icon, "X");
  assert.match(model.summary.text, /kill switch active/);
});

test("decision stepper renders loading payload without declaring a stop", () => {
  const model = buildDecisionStepperModel({
    loading: true,
    stages: [
      { label: "Decision path", status: "loading", summary: "Loading decision details." },
    ],
  });

  assert.equal(model.summary.stageKey, "signal");
  assert.match(model.summary.text, /loading from the server/);
  assert.deepEqual([...new Set(model.stages.map((item) => item.status))], ["loading"]);
});

test("decision stepper emphasizes unavailable order stage from server details", () => {
  const model = buildDecisionStepperModel({
    stages: basePassStages({
      route: {
        status: "unavailable",
        summary: "",
        unavailable_reason: "execution_route_unavailable",
      },
      outcome: { status: "unavailable", summary: "No fill linked yet." },
    }),
  });
  const html = renderDecisionStepperHtml({
    stages: basePassStages({
      route: {
        status: "unavailable",
        summary: "",
        unavailable_reason: "execution_route_unavailable",
      },
      outcome: { status: "unavailable", summary: "No fill linked yet." },
    }),
  });

  assert.equal(model.summary.stageKey, "order");
  assert.match(model.summary.text, /Order unavailable: execution_route_unavailable/);
  assert.match(html, /data-stage-key="order" data-stage-status="unavailable"/);
  assert.match(html, /aria-current="step"/);
  assert.match(html, /execution_route_unavailable/);
});

test("decision stepper renders empty stage payload as useful unavailable flow", () => {
  const model = buildDecisionStepperModel({});
  const html = renderDecisionStepperHtml({});

  assert.equal(model.stages.length, 6);
  assert.equal(model.summary.stageKey, "signal");
  assert.equal(model.stages[0].status, "unavailable");
  assert.match(model.stages[0].reason, /No stage data was returned/);
  assert.match(html, /Signal/);
  assert.match(html, /Fill/);
  assert.match(html, /Timestamp unavailable/);
});
