import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { buildConfirmationPayload } from "../ui/confirmation_modal.mjs";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));

test("buildConfirmationPayload emits structured audit fields", () => {
  const payload = buildConfirmationPayload(
    { reason: "operator incident response", holdMs: 3000, requestId: "request-123" },
    {
      actionId: "operator.emergency_stop",
      confirmText: "KILL",
      actor: "operator_ui",
      source: "operator_console",
      target: "global",
    },
  );

  assert.equal(payload.confirm, "KILL");
  assert.equal(payload.confirmation, "KILL");
  assert.equal(payload.confirmation_token, "KILL");
  assert.equal(payload.action_id, "operator.emergency_stop");
  assert.equal(payload.actor, "operator_ui");
  assert.equal(payload.source_surface, "operator_console");
  assert.equal(payload.reason, "operator incident response");
  assert.equal(payload.request_id, "request-123");
  assert.equal(payload.target, "global");
  assert.equal(payload.confirmation_method, "typed_phrase_hold");
  assert.equal(payload.confirmation_hold_ms, 3000);
  assert.equal(payload.consequence_ack, true);
});

test("confirmation modal source keeps keyboard cancel, focus trap, acknowledgement, and hold handling", () => {
  const source = readFileSync(join(ROOT, "ui", "confirmation_modal.mjs"), "utf8");

  assert.match(source, /event\.key === "Escape"/);
  assert.match(source, /event\.key !== "Tab"/);
  assert.match(source, /previousFocus\.focus/);
  assert.match(source, /data-field="ack"/);
  assert.match(source, /data-role="hold"/);
  assert.match(source, /pointerdown/);
  assert.match(source, /keyup/);
  assert.match(source, /submit\.disabled = !/);
});
