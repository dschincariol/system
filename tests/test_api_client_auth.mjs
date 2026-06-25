import assert from "node:assert/strict";
import test from "node:test";

import {
  DASHBOARD_API_TOKEN_STORAGE_KEY,
  configureDashboardApiReadThrottleForTests,
  clearDashboardApiToken,
  fetchJSON,
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
