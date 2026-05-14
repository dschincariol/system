/*
  FILE: ui/symbol_context.mjs

  Small shared selected-symbol context for browser UI surfaces. It intentionally
  stays independent from dashboard state so panels can opt in without coupling
  their refresh loops.
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
