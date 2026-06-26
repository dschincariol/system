import assert from "node:assert/strict";
import test from "node:test";

import {
  DASHBOARD_API_TOKEN_STORAGE_KEY,
  apiEventSource,
  apiFetch,
  businessDegradedReason,
  configureDashboardApiReadThrottleForTests,
  clearDashboardApiToken,
  dashboardApiEventSourceUrl,
  fetchJSON,
  isBusinessDegradedPayload,
  isSameOriginApiRequest,
  resetDashboardApiReadThrottleForTests,
  resolveDashboardApiToken,
} from "../ui/api_client.js";

class MemoryStorage {
  constructor() {
    this.values = new Map();
  }

  getItem(key) {
    return this.values.has(key) ? this.values.get(key) : null;
  }

  setItem(key, value) {
    this.values.set(key, String(value));
  }

  removeItem(key) {
    this.values.delete(key);
  }
}

function installBrowser(url, storage = new MemoryStorage()) {
  const parsed = new URL(url);
  globalThis.window = {
    location: {
      href: parsed.href,
      search: parsed.search,
    },
    localStorage: storage,
  };
  globalThis.location = globalThis.window.location;
  globalThis.localStorage = storage;
  clearDashboardApiToken();
  resetDashboardApiReadThrottleForTests();
  return storage;
}

test("dashboard fetchJSON attaches URL token to same-origin API requests only", async () => {
  const previousFetch = globalThis.fetch;
  const previousWindow = globalThis.window;
  const previousLocation = globalThis.location;
  const previousStorage = globalThis.localStorage;
  const storage = installBrowser("http://127.0.0.1:8000/ui/dashboard.html?token=url-token-123");
  const requests = [];

  try {
    assert.equal(resolveDashboardApiToken({ force: true }), "url-token-123");
    globalThis.fetch = async (url, options) => {
      requests.push({ url, options });
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    assert.equal(isSameOriginApiRequest("/api/health"), true);
    assert.equal(isSameOriginApiRequest("http://127.0.0.1:8000/api/health"), true);
    assert.equal(isSameOriginApiRequest("https://example.test/api/health"), false);

    await fetchJSON("/api/health?surface=dashboard", { allowBusinessFalse: true });
    await fetchJSON("https://example.test/api/health", { allowBusinessFalse: true });

    assert.equal(requests[0].options.headers.get("X-API-Token"), "url-token-123");
    assert.equal(requests[1].options.headers.has("X-API-Token"), false);
    assert.equal(storage.getItem(DASHBOARD_API_TOKEN_STORAGE_KEY), "url-token-123");
  } finally {
    globalThis.fetch = previousFetch;
    globalThis.window = previousWindow;
    globalThis.location = previousLocation;
    globalThis.localStorage = previousStorage;
  }
});

test("apiFetch attaches dashboard token and paces low-level same-origin API reads", async () => {
  const previousFetch = globalThis.fetch;
  const previousWindow = globalThis.window;
  const previousLocation = globalThis.location;
  const previousStorage = globalThis.localStorage;
  installBrowser("http://127.0.0.1:8000/ui/dashboard.html?token=low-level-token-789");
  assert.equal(resolveDashboardApiToken({ force: true }), "low-level-token-789");
  configureDashboardApiReadThrottleForTests({ burstLimit: 1, windowMs: 30 });
  const requests = [];

  try {
    globalThis.fetch = async (url, options) => {
      requests.push({ url, options, startedAt: Date.now() });
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    await Promise.all([
      apiFetch("/api/terminal/decision_overlays?symbol=SPY"),
      apiFetch("/api/terminal/markers?symbol=SPY"),
    ]);

    assert.equal(requests.length, 2);
    assert.equal(requests[0].options.headers.get("X-API-Token"), "low-level-token-789");
    assert.equal(requests[1].options.headers.get("X-API-Token"), "low-level-token-789");
    assert.ok(requests[1].startedAt - requests[0].startedAt >= 20);

    requests.length = 0;
    await apiFetch("https://example.test/api/health");
    assert.equal(requests[0].options.headers.has("X-API-Token"), false);
  } finally {
    resetDashboardApiReadThrottleForTests();
    globalThis.fetch = previousFetch;
    globalThis.window = previousWindow;
    globalThis.location = previousLocation;
    globalThis.localStorage = previousStorage;
  }
});

test("apiEventSource authenticates same-origin API streams with token query only", () => {
  const previousWindow = globalThis.window;
  const previousLocation = globalThis.location;
  const previousStorage = globalThis.localStorage;
  const previousEventSource = globalThis.EventSource;
  installBrowser("http://127.0.0.1:8000/ui/dashboard.html?token=sse-token-456");
  assert.equal(resolveDashboardApiToken({ force: true }), "sse-token-456");

  try {
    assert.equal(
      dashboardApiEventSourceUrl("/api/market/stream?symbol=SPY&tf=1m"),
      "/api/market/stream?symbol=SPY&tf=1m&token=sse-token-456",
    );
    assert.equal(
      dashboardApiEventSourceUrl("/api/market/stream?symbol=SPY&token=already-present"),
      "/api/market/stream?symbol=SPY&token=already-present",
    );
    assert.equal(
      dashboardApiEventSourceUrl("https://example.test/api/market/stream?symbol=SPY"),
      "https://example.test/api/market/stream?symbol=SPY",
    );

    const seen = [];
    globalThis.EventSource = class FakeEventSource {
      constructor(url, options) {
        this.url = url;
        this.options = options;
        seen.push({ url, options });
      }
    };

    const es = apiEventSource("/api/market/stream?symbol=QQQ", { withCredentials: true });
    assert.equal(es.url, "/api/market/stream?symbol=QQQ&token=sse-token-456");
    assert.deepEqual(seen[0].options, { withCredentials: true });
  } finally {
    globalThis.EventSource = previousEventSource;
    globalThis.window = previousWindow;
    globalThis.location = previousLocation;
    globalThis.localStorage = previousStorage;
  }
});

test("same-origin API GET fan-out is paced below the dashboard read budget", async () => {
  const previousFetch = globalThis.fetch;
  const previousWindow = globalThis.window;
  const previousLocation = globalThis.location;
  const previousStorage = globalThis.localStorage;
  installBrowser("http://127.0.0.1:8000/ui/dashboard.html?token=url-token-123");
  configureDashboardApiReadThrottleForTests({ burstLimit: 2, windowMs: 35 });
  const starts = [];

  try {
    globalThis.fetch = async (url) => {
      starts.push({ url, startedAt: Date.now() });
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    };

    await Promise.all([
      fetchJSON("/api/health?i=1", { allowBusinessFalse: true }),
      fetchJSON("/api/health?i=2", { allowBusinessFalse: true }),
      fetchJSON("/api/health?i=3", { allowBusinessFalse: true }),
    ]);

    assert.equal(starts.length, 3);
    assert.ok(starts[2].startedAt - starts[0].startedAt >= 25);

    starts.length = 0;
    await Promise.all([
      fetchJSON("/api/mutate", { method: "POST", body: JSON.stringify({ ok: true }) }),
      fetchJSON("https://example.test/api/health", { allowBusinessFalse: true }),
    ]);
    assert.equal(starts.length, 2);
    assert.ok(starts[1].startedAt - starts[0].startedAt < 25);
  } finally {
    resetDashboardApiReadThrottleForTests();
    globalThis.fetch = previousFetch;
    globalThis.window = previousWindow;
    globalThis.location = previousLocation;
    globalThis.localStorage = previousStorage;
  }
});

test("dashboard token resolver falls back to storage and supports explicit clear", () => {
  const previousWindow = globalThis.window;
  const previousLocation = globalThis.location;
  const previousStorage = globalThis.localStorage;
  const storage = installBrowser("http://127.0.0.1:8000/ui/dashboard.html");

  try {
    storage.setItem(DASHBOARD_API_TOKEN_STORAGE_KEY, "stored-token-456");
    assert.equal(resolveDashboardApiToken({ force: true }), "stored-token-456");

    globalThis.window.location.href = "http://127.0.0.1:8000/ui/dashboard.html?clear_token=1";
    globalThis.window.location.search = "?clear_token=1";
    assert.equal(resolveDashboardApiToken({ force: true }), "");
    assert.equal(storage.getItem(DASHBOARD_API_TOKEN_STORAGE_KEY), null);
  } finally {
    globalThis.window = previousWindow;
    globalThis.location = previousLocation;
    globalThis.localStorage = previousStorage;
  }
});

test("fetchJSON returns reasoned 2xx business-degraded payloads without allowBusinessFalse", async () => {
  const previousFetch = globalThis.fetch;
  const previousWindow = globalThis.window;
  const previousLocation = globalThis.location;
  const previousStorage = globalThis.localStorage;
  installBrowser("http://127.0.0.1:8000/ui/dashboard.html?token=url-token-123");

  try {
    globalThis.fetch = async () => new Response(JSON.stringify({
      ok: false,
      reason: "warming_up",
      reason_code: "WARMING_UP",
      data: { rows: [] },
      meta: { status: 200 },
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });

    const payload = await fetchJSON("/api/portfolio");
    assert.equal(payload.ok, false);
    assert.equal(payload.reason_code, "WARMING_UP");
    assert.equal(isBusinessDegradedPayload(payload), true);
    assert.equal(businessDegradedReason(payload), "warming_up");
  } finally {
    globalThis.fetch = previousFetch;
    globalThis.window = previousWindow;
    globalThis.location = previousLocation;
    globalThis.localStorage = previousStorage;
  }
});

test("fetchJSON still rejects auth failures, server faults, and unreasoned application errors", async () => {
  const previousFetch = globalThis.fetch;
  const previousWindow = globalThis.window;
  const previousLocation = globalThis.location;
  const previousStorage = globalThis.localStorage;
  installBrowser("http://127.0.0.1:8000/ui/dashboard.html?token=url-token-123");

  try {
    globalThis.fetch = async () => new Response(JSON.stringify({
      ok: false,
      reason_code: "operator_token_required",
      message: "Operator token required.",
    }), {
      status: 401,
      statusText: "Unauthorized",
      headers: { "Content-Type": "application/json" },
    });
    await assert.rejects(() => fetchJSON("/api/operator/support_snapshot"), /401 Unauthorized: Operator token required/);

    globalThis.fetch = async () => new Response(JSON.stringify({
      ok: false,
      error: "internal_server_error",
      reason_code: "handler_exception",
      message: "Internal server error.",
    }), {
      status: 500,
      statusText: "Internal Server Error",
      headers: { "Content-Type": "application/json" },
    });
    await assert.rejects(() => fetchJSON("/api/boom"), /500 Internal Server Error: Internal server error/);

    globalThis.fetch = async () => new Response(JSON.stringify({
      ok: false,
      error: "request_failed",
    }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
    await assert.rejects(() => fetchJSON("/api/malformed_business_response"), /request_failed/);
  } finally {
    globalThis.fetch = previousFetch;
    globalThis.window = previousWindow;
    globalThis.location = previousLocation;
    globalThis.localStorage = previousStorage;
  }
});
