/*
  Display-only FX formatting helpers.

  FX-02 owns the canonical Python symbol semantics and stored form. This
  browser module mirrors the accepted pair spellings for UI display only so
  prices, pip distances, and lot quantities render consistently before the
  backend-specific payloads arrive.
*/

const KNOWN_FIAT = new Set([
  "USD", "EUR", "JPY", "GBP", "CHF", "AUD", "NZD", "CAD",
  "SEK", "NOK", "DKK", "MXN", "ZAR", "SGD", "HKD", "CNH", "TRY",
]);

function normalizePair(symbol) {
  const text = String(symbol || "").trim().toUpperCase();
  const compact = text.replace(/[\/_]/g, "");
  if (!/^[A-Z]{6}$/.test(compact)) return null;
  const base = compact.slice(0, 3);
  const quote = compact.slice(3, 6);
  if (!KNOWN_FIAT.has(base) || !KNOWN_FIAT.has(quote) || base === quote) return null;
  return { base, quote, compact, display: `${base}/${quote}` };
}

function numberOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export function normalizeFxSymbol(symbol) {
  const pair = normalizePair(symbol);
  return pair ? pair.compact : "";
}

export function isFxSymbol(symbol) {
  return normalizePair(symbol) !== null;
}

export function pipDecimals(symbol) {
  const pair = normalizePair(symbol);
  if (!pair) return 2;
  return pair.quote === "JPY" ? 3 : 5;
}

export function pipSize(symbol) {
  const pair = normalizePair(symbol);
  if (!pair) return null;
  return pair.quote === "JPY" ? 0.01 : 0.0001;
}

export function formatFxPrice(symbol, price) {
  const n = numberOrNull(price);
  if (n == null) return "—";
  return n.toFixed(pipDecimals(symbol));
}

export function pipValueDisplay(symbol, priceA, priceB) {
  const size = pipSize(symbol);
  const a = numberOrNull(priceA);
  const b = numberOrNull(priceB);
  if (size == null || a == null || b == null) return "—";
  const pips = Math.abs(b - a) / size;
  const digits = pips < 10 ? 1 : 0;
  return `${pips.toFixed(digits)} pips`;
}

export function formatLotQty(symbol, units, lotSize = 100000) {
  const qty = numberOrNull(units);
  if (qty == null) return "—";
  if (!isFxSymbol(symbol)) {
    return qty.toLocaleString("en-US", { maximumFractionDigits: 6 });
  }
  const lot = numberOrNull(lotSize) || 100000;
  const lots = lot > 0 ? qty / lot : 0;
  const lotsText = lots.toLocaleString("en-US", {
    minimumFractionDigits: Math.abs(lots) < 10 ? 2 : 1,
    maximumFractionDigits: 2,
  });
  const unitsText = Math.round(qty).toLocaleString("en-US");
  return `${lotsText} lots (${unitsText})`;
}
