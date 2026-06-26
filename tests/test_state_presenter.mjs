import assert from "node:assert/strict";
import test from "node:test";

import {
  emptyStateCopy,
  stateBlockHtml,
  summarizeOperatorError,
  technicalDetailsHtml,
} from "../ui/state_presenter.js";

test("state presenter keeps raw diagnostics out of primary copy", () => {
  const html = stateBlockHtml({
    state: "error",
    title: '{"error":"OPENAI_API_KEY failed"}',
    message: 'Traceback (most recent call last):\n  File "x.py", line 1\n/api/health?token=abc OPENAI_API_KEY failed',
    why: "Operators need a safe summary.",
    action: "Open technical details if debugging is required.",
    details: {
      error: "OPENAI_API_KEY failed",
      route: "/api/health?token=abc",
      stack: 'Traceback (most recent call last):\n  File "x.py", line 1',
    },
  });

  const primary = html.split("<details")[0];
  assert.match(primary, /backend route credential failed/);
  assert.doesNotMatch(primary, /OPENAI_API_KEY/);
  assert.doesNotMatch(primary, /\/api\/health/);
  assert.doesNotMatch(primary, /Traceback/);
  assert.doesNotMatch(primary, /\{"error"/);

  assert.match(html, /Technical details/);
  assert.match(html, /OPENAI_API_KEY/);
  assert.match(html, /Traceback/);
});

test("state presenter distinguishes configured active and filtered empty states", () => {
  assert.equal(emptyStateCopy("configured").title, "Nothing configured");
  assert.equal(emptyStateCopy("active").title, "Nothing active");
  assert.equal(emptyStateCopy("filtered").title, "Filtered to none");

  assert.match(stateBlockHtml({ state: "empty", emptyKind: "configured" }), /Nothing configured/);
  assert.match(stateBlockHtml({ state: "empty", emptyKind: "active" }), /Nothing active/);
  assert.match(stateBlockHtml({ state: "empty", emptyKind: "filtered" }), /Filtered to none/);
});

test("technical details preserve backend truth behind disclosure", () => {
  const details = technicalDetailsHtml({ ok: false, error: "provider_timeout", retry_after_s: 30 });
  assert.match(details, /<details/);
  assert.match(details, /Technical details/);
  assert.match(details, /provider_timeout/);
  assert.match(details, /retry_after_s/);
});

test("state presenter can embed trusted rendered body content before diagnostics", () => {
  const html = stateBlockHtml({
    state: "ok",
    title: "Trading monitor loaded",
    message: "Runtime activity was returned.",
    bodyHtml: '<div class="metricsGrid"><div>Positions</div></div>',
    details: { positions: 1 },
  });

  assert.match(html, /metricsGrid/);
  assert.ok(html.indexOf("metricsGrid") < html.indexOf("Technical details"));
});

test("summarizeOperatorError redacts routes and credential names", () => {
  assert.equal(
    summarizeOperatorError("/api/data_sources?token=x POLYGON_API_KEY missing", "failed"),
    "backend route credential missing",
  );
});
