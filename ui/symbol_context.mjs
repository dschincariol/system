/*
  FILE: ui/symbol_context.mjs

  Shared browser context helpers for UI surfaces. The selected-symbol API stays
  intentionally independent from dashboard state; the operational-context API
  adds versioned URL/localStorage handoff for cross-surface workflows.
*/

const DEFAULT_CONTEXT = Object.freeze({
  symbol: "",
  source: "",
  ts_ms: 0,
});

let _context = { ...DEFAULT_CONTEXT };
const _subscribers = new Set();

function _nowMs() {
  return Date.now();
}

function _getWindow(windowRef) {
  if (windowRef) return windowRef;
  return typeof window !== "undefined" ? window : null;
}

export function normalizeSelectedSymbol(value) {
  const raw = String(value || "").trim().toUpperCase();
  if (!raw) return "";
  return raw.replace(/[^A-Z0-9._:-]/g, "").slice(0, 32);
}

export function getSelectedSymbolContext() {
  return { ..._context };
}

function _notify(next, prev) {
  for (const subscriber of Array.from(_subscribers)) {
    try {
      subscriber({ ...next }, { ...prev });
    } catch {}
  }
}

export function persistSelectedSymbolToUrl(symbol, options = {}) {
  const win = _getWindow(options.windowRef);
  if (!win || !win.location || !win.history || typeof win.history.replaceState !== "function") {
    return false;
  }

  try {
    const param = String(options.param || "symbol");
    const url = new URL(win.location.href);
    const normalized = normalizeSelectedSymbol(symbol);
    if (normalized) url.searchParams.set(param, normalized);
    else url.searchParams.delete(param);
    win.history.replaceState(win.history.state || {}, "", url.toString());
    return true;
  } catch {
    return false;
  }
}

export function updateSelectedSymbolContext(input = {}) {
  const prev = _context;
  const symbol = normalizeSelectedSymbol(input.symbol);
  const source = String(input.source || (symbol ? "unknown" : "clear")).trim();
  const ts = Number(input.ts_ms);
  const next = {
    symbol,
    source,
    ts_ms: Number.isFinite(ts) && ts > 0 ? ts : _nowMs(),
  };

  _context = next;

  if (input.persistUrl !== false) {
    persistSelectedSymbolToUrl(next.symbol, { windowRef: input.windowRef, param: input.param });
  }

  if (input.notify !== false) {
    _notify(next, prev);
  }

  return { ...next };
}

export function clearSelectedSymbolContext(options = {}) {
  return updateSelectedSymbolContext({
    ...options,
    symbol: "",
    source: options.source || "clear",
  });
}

export function subscribeSelectedSymbolContext(listener, options = {}) {
  if (typeof listener !== "function") return () => {};
  _subscribers.add(listener);
  if (options.emit === true) {
    try {
      listener(getSelectedSymbolContext(), getSelectedSymbolContext());
    } catch {}
  }
  return () => {
    _subscribers.delete(listener);
  };
}

export function initSelectedSymbolContextFromUrl(options = {}) {
  const win = _getWindow(options.windowRef);
  if (!win || !win.location) return getSelectedSymbolContext();

  try {
    const param = String(options.param || "symbol");
    const url = new URL(win.location.href);
    const symbol = normalizeSelectedSymbol(url.searchParams.get(param));
    if (!symbol && options.clearWhenMissing !== true) return getSelectedSymbolContext();
    return updateSelectedSymbolContext({
      symbol,
      source: options.source || "url",
      persistUrl: false,
      notify: options.notify,
      ts_ms: options.ts_ms,
    });
  } catch {
    return getSelectedSymbolContext();
  }
}

export const OPERATIONAL_CONTEXT_STORAGE_KEY = "operational.context.v1";
export const SAVED_OPERATIONAL_WORKSPACE_STORAGE_KEY = "operational.workspace.v1";

const OPERATIONAL_CONTEXT_VERSION = 1;
const WORKSPACE_VERSION = 1;
const OPERATIONAL_CONTEXT_KEYS = Object.freeze([
  "symbol",
  "decision_id",
  "alert_id",
  "source_key",
  "job_id",
  "advisory_id",
]);

const DEFAULT_OPERATIONAL_CONTEXT = Object.freeze({
  version: OPERATIONAL_CONTEXT_VERSION,
  symbol: "",
  decision_id: "",
  alert_id: "",
  source_key: "",
  job_id: "",
  advisory_id: "",
  source: "",
  surface: "",
  ts_ms: 0,
});

let _operationalContext = { ...DEFAULT_OPERATIONAL_CONTEXT };

function _getStorage(storageRef) {
  if (storageRef) return storageRef;
  const win = _getWindow();
  if (win && win.localStorage) return win.localStorage;
  if (typeof globalThis !== "undefined" && globalThis.localStorage) return globalThis.localStorage;
  return null;
}

function _cleanIdentifier(value, maxLen = 96) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return raw.replace(/[^A-Za-z0-9._:/-]/g, "").slice(0, maxLen);
}

function _cleanMeta(value, maxLen = 48) {
  return _cleanIdentifier(value, maxLen).toLowerCase();
}

function _firstDefined(input, keys) {
  for (const key of keys) {
    if (input && input[key] !== undefined && input[key] !== null) return input[key];
  }
  return "";
}

export function normalizeOperationalContext(input = {}) {
  const src = input && typeof input === "object" ? input : {};
  const ts = Number(src.ts_ms ?? src.tsMs);
  return {
    version: OPERATIONAL_CONTEXT_VERSION,
    symbol: normalizeSelectedSymbol(_firstDefined(src, ["symbol", "ticker"])),
    decision_id: _cleanIdentifier(_firstDefined(src, ["decision_id", "decisionId"]), 96),
    alert_id: _cleanIdentifier(_firstDefined(src, ["alert_id", "alertId"]), 96),
    source_key: _cleanIdentifier(_firstDefined(src, ["source_key", "sourceKey"]), 128),
    job_id: _cleanIdentifier(_firstDefined(src, ["job_id", "jobId", "job"]), 128),
    advisory_id: _cleanIdentifier(_firstDefined(src, ["advisory_id", "advisoryId"]), 96),
    source: _cleanMeta(src.source, 64),
    surface: _cleanMeta(src.surface, 48),
    ts_ms: Number.isFinite(ts) && ts > 0 ? ts : _nowMs(),
  };
}

export function hasOperationalContext(context = _operationalContext) {
  const ctx = normalizeOperationalContext(context);
  return OPERATIONAL_CONTEXT_KEYS.some((key) => !!ctx[key]);
}

export function getOperationalContext() {
  return { ..._operationalContext };
}

export function operationalContextSummary(context = _operationalContext) {
  const ctx = normalizeOperationalContext(context);
  const parts = [];
  if (ctx.symbol) parts.push(`symbol ${ctx.symbol}`);
  if (ctx.source_key) parts.push(`source ${ctx.source_key}`);
  if (ctx.job_id) parts.push(`job ${ctx.job_id}`);
  if (ctx.decision_id) parts.push(`decision ${ctx.decision_id}`);
  if (ctx.alert_id) parts.push(`alert ${ctx.alert_id}`);
  if (ctx.advisory_id) parts.push(`advisory ${ctx.advisory_id}`);
  return parts.length ? parts.join(" | ") : "context none";
}

function _contextForUrl(context) {
  const ctx = normalizeOperationalContext(context);
  const out = {};
  for (const key of OPERATIONAL_CONTEXT_KEYS) {
    out[key] = ctx[key] || "";
  }
  return out;
}

export function applyOperationalContextToUrl(urlInput, context = {}, options = {}) {
  try {
    const win = _getWindow(options.windowRef);
    const base = options.base || (win && win.location && win.location.href) || "http://localhost/";
    const url = new URL(urlInput, base);
    const ctx = _contextForUrl(context);
    for (const key of OPERATIONAL_CONTEXT_KEYS) {
      if (ctx[key]) url.searchParams.set(key, ctx[key]);
      else if (options.clearMissing === true) url.searchParams.delete(key);
    }
    if (options.relative === true) return `${url.pathname}${url.search}${url.hash}`;
    return url.toString();
  } catch {
    return String(urlInput || "");
  }
}

export function persistOperationalContextToUrl(context = _operationalContext, options = {}) {
  const win = _getWindow(options.windowRef);
  if (!win || !win.location || !win.history || typeof win.history.replaceState !== "function") {
    return false;
  }
  try {
    const url = new URL(win.location.href);
    const next = applyOperationalContextToUrl(url.toString(), context, {
      clearMissing: true,
      windowRef: win,
    });
    const mode = options.mode === "push" && typeof win.history.pushState === "function" ? "push" : "replace";
    if (mode === "push") {
      win.history.pushState(win.history.state || {}, "", next);
    } else {
      win.history.replaceState(win.history.state || {}, "", next);
    }
    return true;
  } catch {
    return false;
  }
}

export function operationalContextFromUrl(options = {}) {
  const win = _getWindow(options.windowRef);
  if (!win || !win.location) {
    return { context: { ...DEFAULT_OPERATIONAL_CONTEXT }, hasContext: false };
  }
  try {
    const url = new URL(win.location.href);
    const raw = {};
    let hasContext = false;
    for (const key of OPERATIONAL_CONTEXT_KEYS) {
      const value = url.searchParams.get(key);
      if (value !== null) {
        raw[key] = value;
        hasContext = true;
      }
    }
    return {
      context: normalizeOperationalContext({
        ...raw,
        source: options.source || "url",
        surface: options.surface || "",
      }),
      hasContext,
    };
  } catch {
    return { context: { ...DEFAULT_OPERATIONAL_CONTEXT }, hasContext: false };
  }
}

export function readOperationalContextFromStorage(options = {}) {
  const store = _getStorage(options.storage);
  if (!store) return { ...DEFAULT_OPERATIONAL_CONTEXT };
  try {
    const raw = store.getItem(OPERATIONAL_CONTEXT_STORAGE_KEY);
    if (!raw) return { ...DEFAULT_OPERATIONAL_CONTEXT };
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.version !== OPERATIONAL_CONTEXT_VERSION) return { ...DEFAULT_OPERATIONAL_CONTEXT };
    return normalizeOperationalContext(parsed);
  } catch {
    return { ...DEFAULT_OPERATIONAL_CONTEXT };
  }
}

export function writeOperationalContextToStorage(context = _operationalContext, options = {}) {
  const store = _getStorage(options.storage);
  if (!store) return false;
  try {
    const ctx = normalizeOperationalContext(context);
    if (!hasOperationalContext(ctx)) {
      if (typeof store.removeItem === "function") store.removeItem(OPERATIONAL_CONTEXT_STORAGE_KEY);
      return true;
    }
    store.setItem(OPERATIONAL_CONTEXT_STORAGE_KEY, JSON.stringify(ctx));
    return true;
  } catch {
    return false;
  }
}

export function updateOperationalContext(input = {}, options = {}) {
  const next = normalizeOperationalContext({
    ..._operationalContext,
    ...(input || {}),
  });
  _operationalContext = next;
  if (options.persistStorage !== false) {
    writeOperationalContextToStorage(next, options);
  }
  if (options.persistUrl === true) {
    persistOperationalContextToUrl(next, options);
  }
  return { ...next };
}

export function initOperationalContext(options = {}) {
  const stored = options.readStorage === false
    ? { ...DEFAULT_OPERATIONAL_CONTEXT }
    : readOperationalContextFromStorage(options);
  const fromUrl = operationalContextFromUrl(options);
  const merged = normalizeOperationalContext({
    ...stored,
    ...(fromUrl.hasContext ? fromUrl.context : {}),
    source: fromUrl.hasContext ? (options.source || "url") : (stored.source || options.source || ""),
    surface: options.surface || (fromUrl.hasContext ? fromUrl.context.surface : stored.surface),
  });
  _operationalContext = merged;
  if (fromUrl.hasContext && options.persistStorage !== false) {
    writeOperationalContextToStorage(merged, options);
  }
  return { ..._operationalContext };
}

export function clearOperationalContext(options = {}) {
  _operationalContext = normalizeOperationalContext({
    source: options.source || "clear",
    surface: options.surface || "",
    ts_ms: options.ts_ms,
  });
  if (options.persistStorage !== false) {
    writeOperationalContextToStorage(_operationalContext, options);
  }
  if (options.persistUrl === true) {
    persistOperationalContextToUrl(_operationalContext, options);
  }
  return { ..._operationalContext };
}

function _normalizeWorkspace(input = {}) {
  const src = input && typeof input === "object" ? input : {};
  const dashboard = src.dashboard && typeof src.dashboard === "object" ? src.dashboard : {};
  const savedAt = Number(src.saved_at_ms ?? src.savedAtMs);
  return {
    version: WORKSPACE_VERSION,
    saved_at_ms: Number.isFinite(savedAt) && savedAt > 0 ? savedAt : _nowMs(),
    surface: _cleanMeta(src.surface || "dashboard", 48),
    dashboard: {
      screen: _cleanMeta(dashboard.screen, 48),
      persona: _cleanMeta(dashboard.persona, 48),
    },
    context: normalizeOperationalContext(src.context || {}),
  };
}

export function readSavedOperationalWorkspace(options = {}) {
  const store = _getStorage(options.storage);
  if (!store) return null;
  try {
    const raw = store.getItem(SAVED_OPERATIONAL_WORKSPACE_STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.version !== WORKSPACE_VERSION) return null;
    return _normalizeWorkspace(parsed);
  } catch {
    return null;
  }
}

export function writeSavedOperationalWorkspace(workspace = {}, options = {}) {
  const store = _getStorage(options.storage);
  if (!store) return null;
  try {
    const normalized = _normalizeWorkspace(workspace);
    store.setItem(SAVED_OPERATIONAL_WORKSPACE_STORAGE_KEY, JSON.stringify(normalized));
    return normalized;
  } catch {
    return null;
  }
}

export function clearSavedOperationalWorkspace(options = {}) {
  const store = _getStorage(options.storage);
  if (!store || typeof store.removeItem !== "function") return false;
  try {
    store.removeItem(SAVED_OPERATIONAL_WORKSPACE_STORAGE_KEY);
    return true;
  } catch {
    return false;
  }
}
