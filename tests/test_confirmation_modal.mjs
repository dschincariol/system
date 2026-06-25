import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import {
  buildConfirmationPayload,
  validateConfirmationInput,
} from "../ui/confirmation_modal.mjs";

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
  assert.equal(payload.action, "this action");
  assert.match(payload.consequence, /changes system state/);
  assert.match(payload.reversibility, /Not automatically reversible/);
  assert.equal(payload.confirmation_method, "typed_phrase_hold");
  assert.equal(payload.confirmation_hold_ms, 3000);
  assert.equal(payload.consequence_ack, true);
  assert.equal(Number.isFinite(payload.confirmed_at_ms), true);
});

test("validateConfirmationInput requires typed phrase, acknowledgement, reason, and hold completion", () => {
  const options = {
    confirmText: "DELETE_SOURCE",
    requireReason: true,
    minReasonLength: 8,
    holdMs: 1500,
  };

  let state = validateConfirmationInput({
    phrase: "DELETE",
    reason: "too short",
    ack: false,
    holdComplete: false,
  }, options);
  assert.equal(state.ok, false);
  assert.equal(state.checks.phraseOk, false);
  assert.equal(state.checks.ackOk, false);
  assert.equal(state.checks.holdOk, false);
  assert.match(state.missing.join(" | "), /Type DELETE_SOURCE/);
  assert.match(state.missing.join(" | "), /Acknowledge/);

  state = validateConfirmationInput({
    phrase: "DELETE_SOURCE",
    reason: "operator cleanup",
    ack: true,
    holdComplete: true,
  }, options);
  assert.equal(state.ok, true);
  assert.deepEqual(state.missing, []);
});

test("confirmation modal source keeps keyboard cancel, focus trap, acknowledgement, and hold handling", () => {
  const source = readFileSync(join(ROOT, "ui", "confirmation_modal.mjs"), "utf8");

  assert.match(source, /role="alertdialog"/);
  assert.match(source, /aria-modal="true"/);
  assert.match(source, /aria-describedby/);
  assert.match(source, /event\.key === "Escape"/);
  assert.match(source, /event\.key !== "Tab"/);
  assert.match(source, /previousFocus\.focus/);
  assert.match(source, /data-field="ack"/);
  assert.match(source, /data-role="hold"/);
  assert.match(source, /pointerdown/);
  assert.match(source, /keyup/);
  assert.match(source, /submit\.disabled = !/);
  assert.match(source, /Reversibility/);
  assert.match(source, /confirmModalActionGroup/);
});

function* walkFiles(dir) {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    const stat = statSync(path);
    if (stat.isDirectory()) {
      if (name === "vendor") continue;
      yield* walkFiles(path);
    } else if (/\.(?:js|mjs|html)$/.test(name)) {
      yield path;
    }
  }
}

test("high-impact UI code does not call native confirm or prompt", () => {
  const offenders = [];
  for (const path of walkFiles(join(ROOT, "ui"))) {
    const text = readFileSync(path, "utf8");
    const stripped = text
      .replace(/\/\/.*$/gm, "")
      .replace(/\/\*[\s\S]*?\*\//g, "");
    if (/\b(?:window\.)?(?:confirm|prompt)\s*\(/.test(stripped)) {
      offenders.push(path.replace(`${ROOT}/`, ""));
    }
  }
  assert.deepEqual(offenders, []);
});
