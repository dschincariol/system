import assert from "node:assert/strict";
import test from "node:test";

import {
  buildJobCatalogViewModel,
  filterJobCatalogRows,
  isJobActionEnabled,
  renderJobCatalogRows,
} from "../ui/job_catalog.js";

const rows = [
  {
    id: "poll_prices",
    name: "poll_prices",
    label: "Poll prices",
    workflow: "price_feed",
    mode: "daemon",
    safety: "data_refresh",
    safety_label: "Data refresh",
    stage: "market_data",
    owner_subsystem: "data",
    purpose: "Refreshes prices.",
    running: false,
    action_policy: {
      start: { enabled: true, confirmation_required: true },
      stop: { enabled: true, confirmation_required: true },
    },
    last_output_url: "/api/jobs/log?name=poll_prices&tail=800",
  },
  {
    id: "llm_factor_discovery",
    name: "llm_factor_discovery",
    label: "LLM factor discovery",
    workflow: "model_training",
    mode: "oneshot",
    safety: "unavailable",
    safety_label: "Unavailable",
    stage: "feature_discovery",
    owner_subsystem: "strategy",
    purpose: "Discovers factors.",
    disabled_reason: "Missing prerequisite: ANTHROPIC_API_KEY",
    missing_prerequisites: [{ type: "secret", name: "ANTHROPIC_API_KEY" }],
    action_policy: {
      start: { enabled: false, disabled_reason: "Missing prerequisite: ANTHROPIC_API_KEY" },
      stop: { enabled: false, disabled_reason: "Missing prerequisite: ANTHROPIC_API_KEY" },
    },
    last_output_url: "/api/jobs/log?name=llm_factor_discovery&tail=800",
  },
];

test("job catalog filters search safety and workflow fields", () => {
  assert.equal(filterJobCatalogRows(rows, { search: "anthropic" }).length, 1);
  assert.equal(filterJobCatalogRows(rows, { safety: "data_refresh" }).length, 1);
  assert.equal(filterJobCatalogRows(rows, { workflow: "model_training" })[0].name, "llm_factor_discovery");
});

test("job catalog rendering groups rows and exposes disabled prerequisites", () => {
  const view = buildJobCatalogViewModel(rows, {});
  const html = renderJobCatalogRows(view, { selectedJob: "poll_prices" });

  assert.match(html, /jobCatalogGroupRow/);
  assert.match(html, /price_feed/);
  assert.match(html, /model_training/);
  assert.match(html, /data-job-select="poll_prices"/);
  assert.match(html, /Missing prerequisite: ANTHROPIC_API_KEY/);
  assert.match(html, /disabled aria-disabled="true"/);
});

test("job catalog action enabled state honors backend policy", () => {
  assert.equal(isJobActionEnabled(rows[0], "start"), true);
  assert.equal(isJobActionEnabled(rows[1], "start"), false);
});
