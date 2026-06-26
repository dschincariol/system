import assert from "node:assert/strict";
import test from "node:test";

import {
  OPERATIONAL_CONTEXT_STORAGE_KEY,
  SAVED_OPERATIONAL_WORKSPACE_STORAGE_KEY,
  applyOperationalContextToUrl,
  clearOperationalContext,
  hasOperationalContext,
  initOperationalContext,
  normalizeOperationalContext,
  operationalContextSummary,
  persistOperationalContextToUrl,
  readOperationalContextFromStorage,
  readSavedOperationalWorkspace,
  updateOperationalContext,
  writeSavedOperationalWorkspace,
} from "../ui/symbol_context.mjs";

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

function fakeWindow(href, storage = new MemoryStorage()) {
  const state = { href };
  return {
    localStorage: storage,
    get location() {
      return new URL(state.href);
    },
    history: {
      state: {},
      replaceState(nextState, _title, nextUrl) {
        this.state = nextState;
        state.href = new URL(nextUrl, state.href).toString();
      },
      pushState(nextState, _title, nextUrl) {
        this.state = nextState;
        state.href = new URL(nextUrl, state.href).toString();
      },
    },
    get href() {
      return state.href;
    },
  };
}

test("operational context normalizes supported identifiers conservatively", () => {
  const ctx = normalizeOperationalContext({
    symbol: " spy ",
    source_key: "rss:<bad>?key",
    job_id: " poll prices ",
    decision_id: "dec<script>",
    alert_id: "alert#1",
    advisory_id: 42,
    source: "Dashboard Nav!",
    surface: "Data Sources",
    ts_ms: 123,
  });

  assert.equal(ctx.symbol, "SPY");
  assert.equal(ctx.source_key, "rss:badkey");
  assert.equal(ctx.job_id, "pollprices");
  assert.equal(ctx.decision_id, "decscript");
  assert.equal(ctx.alert_id, "alert1");
  assert.equal(ctx.advisory_id, "42");
  assert.equal(ctx.source, "dashboardnav");
  assert.equal(ctx.surface, "datasources");
  assert.equal(ctx.ts_ms, 123);
  assert.equal(hasOperationalContext(ctx), true);
  assert.equal(
    operationalContextSummary(ctx),
    "symbol SPY | source rss:badkey | job pollprices | decision decscript | alert alert1 | advisory 42",
  );
});

test("URL helpers preserve unrelated params and clear context params", () => {
  const storage = new MemoryStorage();
  const win = fakeWindow("https://example.test/ui/data_sources.html?screen=data#inventory", storage);
  const url = applyOperationalContextToUrl(win.location.href, {
    symbol: "nvda",
    source_key: "polygon:prices",
    job_id: "poll_prices",
  });
  const parsed = new URL(url);
  assert.equal(parsed.searchParams.get("screen"), "data");
  assert.equal(parsed.searchParams.get("symbol"), "NVDA");
  assert.equal(parsed.searchParams.get("source_key"), "polygon:prices");
  assert.equal(parsed.searchParams.get("job_id"), "poll_prices");
  assert.equal(parsed.hash, "#inventory");

  assert.equal(persistOperationalContextToUrl({
    symbol: "msft",
    decision_id: "decision-1",
  }, { windowRef: win }), true);
  assert.equal(win.location.searchParams.get("symbol"), "MSFT");
  assert.equal(win.location.searchParams.get("decision_id"), "decision-1");
  assert.equal(win.location.searchParams.get("screen"), "data");

  clearOperationalContext({
    windowRef: win,
    storage,
    persistStorage: true,
    persistUrl: true,
  });
  assert.equal(win.location.searchParams.has("symbol"), false);
  assert.equal(win.location.searchParams.has("decision_id"), false);
  assert.equal(win.location.searchParams.get("screen"), "data");
  assert.equal(win.location.hash, "#inventory");
  assert.equal(storage.getItem(OPERATIONAL_CONTEXT_STORAGE_KEY), null);
});

test("storage parser ignores malformed and stale-version context values", () => {
  const storage = new MemoryStorage();
  storage.setItem(OPERATIONAL_CONTEXT_STORAGE_KEY, "{not-json");
  assert.equal(readOperationalContextFromStorage({ storage }).symbol, "");

  storage.setItem(OPERATIONAL_CONTEXT_STORAGE_KEY, JSON.stringify({ version: 999, symbol: "SPY" }));
  assert.equal(readOperationalContextFromStorage({ storage }).symbol, "");

  const saved = updateOperationalContext({
    source_key: "fmp:news",
    job_id: "ingest_now",
    source: "test",
    surface: "data_sources",
  }, {
    storage,
    persistStorage: true,
    persistUrl: false,
  });
  assert.equal(saved.source_key, "fmp:news");
  assert.equal(JSON.parse(storage.getItem(OPERATIONAL_CONTEXT_STORAGE_KEY)).version, 1);

  const win = fakeWindow("https://example.test/ui/data_sources.html?source_key=polygon:prices", storage);
  const fromUrl = initOperationalContext({
    windowRef: win,
    storage,
    source: "url_test",
    surface: "data_sources",
  });
  assert.equal(fromUrl.source_key, "polygon:prices");
  assert.equal(JSON.parse(storage.getItem(OPERATIONAL_CONTEXT_STORAGE_KEY)).source_key, "polygon:prices");
});

test("saved workspace is versioned and fails soft on malformed values", () => {
  const storage = new MemoryStorage();
  const saved = writeSavedOperationalWorkspace({
    surface: "dashboard",
    dashboard: {
      screen: "data",
      persona: "expert",
    },
    context: {
      source_key: "rss:reuters_top",
      job_id: "ingest_now",
    },
  }, { storage });

  assert.equal(saved.version, 1);
  assert.equal(saved.dashboard.screen, "data");
  assert.equal(saved.dashboard.persona, "expert");
  assert.equal(saved.context.source_key, "rss:reuters_top");
  assert.equal(readSavedOperationalWorkspace({ storage }).context.job_id, "ingest_now");

  storage.setItem(SAVED_OPERATIONAL_WORKSPACE_STORAGE_KEY, "{bad-json");
  assert.equal(readSavedOperationalWorkspace({ storage }), null);

  storage.setItem(SAVED_OPERATIONAL_WORKSPACE_STORAGE_KEY, JSON.stringify({ version: 999 }));
  assert.equal(readSavedOperationalWorkspace({ storage }), null);
});
