import assert from "node:assert/strict";
import test from "node:test";

import {
  buildDirectionalOrderBody,
  configureTerminalOrderStateForTests,
  submitTerminalOrder,
} from "../ui/terminal/terminal.js";

function validConfirmationPayload(overrides = {}) {
  return {
    confirm: "TRADE",
    confirmation: "TRADE",
    confirmation_token: "TRADE",
    confirmation_method: "typed_phrase_hold",
    confirmation_hold_ms: 1000,
    consequence_ack: true,
    actor: "terminal_operator",
    source: "terminal",
    source_surface: "terminal",
    request_id: "operator-confirmed-request",
    target: "BUY AAPL qty 1.0000",
    action_id: "terminal.order",
    ...overrides,
  };
}

function allowDirectionalSubmit(statusBanner) {
  configureTerminalOrderStateForTests({
    symbol: "AAPL",
    executionBarrier: {
      real_trading_allowed: true,
      mode: "live",
      reason: "real_trading_allowed",
    },
    accountSnapshotAvailable: true,
    priceReference: {
      symbol: "AAPL",
      ok: true,
      price: 125.25,
      source: "test",
      age_ms: 250,
    },
    statusBanner,
  });
}

test("directional order body fails closed without operator confirmation payload", () => {
  assert.throws(
    () => buildDirectionalOrderBody({ symbol: "AAPL", side: "BUY", qty: 1 }),
    /directional_order_confirmation_required/,
  );

  assert.throws(
    () => buildDirectionalOrderBody({
      symbol: "AAPL",
      side: "BUY",
      qty: 1,
      confirmationPayload: validConfirmationPayload({ consequence_ack: false }),
    }),
    /directional_order_consequence_ack_required/,
  );

  assert.throws(
    () => buildDirectionalOrderBody({
      symbol: "AAPL",
      side: "BUY",
      qty: 1,
      confirmationPayload: validConfirmationPayload({ confirmation_token: "" }),
    }),
    /directional_order_confirmation_token_required/,
  );
});

test("directional order body uses operator-supplied confirmation fields", () => {
  const confirmationPayload = validConfirmationPayload({
    confirmation_token: "OPERATOR_TYPED_TOKEN",
    confirmation_method: "typed_phrase_hold",
    confirmation_hold_ms: 1234,
    request_id: "operator-payload-id",
  });

  const body = buildDirectionalOrderBody({
    symbol: "aapl",
    side: "buy",
    qty: 2,
    confirmationPayload,
  });

  assert.equal(body.symbol, "AAPL");
  assert.equal(body.side, "BUY");
  assert.equal(body.qty, 2);
  assert.equal(body.consequence_ack, confirmationPayload.consequence_ack);
  assert.equal(body.confirmation_token, confirmationPayload.confirmation_token);
  assert.equal(body.confirmation_method, confirmationPayload.confirmation_method);
  assert.equal(body.confirmation_hold_ms, confirmationPayload.confirmation_hold_ms);
  assert.equal(body.request_id, "operator-payload-id");
});

test("submitTerminalOrder awaits up-front confirmation before first order post", async () => {
  allowDirectionalSubmit(null);
  const events = [];
  let sequence = 0;
  const confirmationPayload = validConfirmationPayload({ request_id: "modal-first" });
  const posts = [];

  const requestConfirmation = async (options) => {
    events.push({ name: "confirmation:start", sequence: ++sequence, options });
    assert.equal(options.title, "Confirm terminal order");
    assert.equal(options.action, "Terminal directional order");
    assert.equal(options.actionId, "terminal.order");
    assert.equal(options.confirmText, "TRADE");
    assert.equal(options.submitLabel, "Send Order");
    assert.equal(options.actor, "terminal_operator");
    assert.equal(options.sourceSurface, "terminal");
    assert.equal(options.holdMs, 1000);
    assert.match(options.target, /^BUY AAPL qty 3\.0000$/);
    assert.match(options.consequence, /estimated notional/);
    await Promise.resolve();
    events.push({ name: "confirmation:resolved", sequence: ++sequence });
    return { ok: true, payload: confirmationPayload };
  };

  const postJson = async (url, body) => {
    events.push({ name: "post", sequence: ++sequence, url, body });
    posts.push({ url, body });
    return { ok: true };
  };

  await submitTerminalOrder("BUY", 3, "Buy", {
    requestConfirmation,
    postJson,
    refreshSnapshot: async () => {},
  });

  assert.equal(posts.length, 1);
  assert.equal(posts[0].url, "/api/terminal/order");
  assert.equal(posts[0].body.request_id, "modal-first");
  assert.equal(posts[0].body.confirmation_token, confirmationPayload.confirmation_token);
  assert.equal(posts[0].body.consequence_ack, confirmationPayload.consequence_ack);

  const resolved = events.find((event) => event.name === "confirmation:resolved");
  const posted = events.find((event) => event.name === "post");
  assert.ok(resolved);
  assert.ok(posted);
  assert.ok(resolved.sequence < posted.sequence);
});

test("submitTerminalOrder cancellation returns before any post", async () => {
  const banner = { className: "", textContent: "" };
  allowDirectionalSubmit(banner);
  let postCount = 0;

  await submitTerminalOrder("SELL", 1, "Sell", {
    requestConfirmation: async () => ({ ok: false, cancelled: true }),
    postJson: async () => {
      postCount += 1;
      return { ok: true };
    },
    refreshSnapshot: async () => {},
  });

  assert.equal(postCount, 0);
  assert.equal(banner.textContent, "Sell confirmation cancelled.");
});

test("sub-threshold single click cannot send when confirmation is cancelled", async () => {
  allowDirectionalSubmit(null);
  const posts = [];

  await submitTerminalOrder("BUY", 5, "Buy", {
    requestConfirmation: async () => ({ ok: false, cancelled: true }),
    postJson: async (url, body) => {
      posts.push({ url, body });
      return { ok: true };
    },
    refreshSnapshot: async () => {},
  });

  assert.equal(posts.length, 0);
});
