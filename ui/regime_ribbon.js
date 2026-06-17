"use strict";

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function escapeHTML(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function numOrNull(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function fmtTime(value) {
  const n = numOrNull(value);
  if (n == null || n <= 0) return "time unavailable";
  try {
    return new Date(n).toLocaleTimeString();
  } catch {
    return "time unavailable";
  }
}

function normalizeLabel(value) {
  const label = String(value || "UNKNOWN").trim().toUpperCase();
  return label || "UNKNOWN";
}

function toneForLabel(label) {
  const raw = normalizeLabel(label);
  if (["UNKNOWN", "UNAVAILABLE", "MISSING"].includes(raw)) return "unavailable";
  if (["RISK_OFF", "VOL_EXPANSION", "CREDIT_STRESS", "THIN", "SHIFT", "BREAK", "CRITICAL"].includes(raw)) return "warn";
  if (["RISK_ON", "CALM", "NORMAL", "STABLE", "AMPLE"].includes(raw)) return "ok";
  return "info";
}

export function buildRegimeRibbonViewModel(payload = {}) {
  const root = asObject(payload);
  const layers = asObject(root.layers);
  const tsMs = numOrNull(root.ts_ms);
  const source = String(root.source || "unavailable");
  const degraded = root.ok === false || !!root.degraded;

  const names = [
    ["macro", "Macro"],
    ["asset", "Asset"],
    ["micro", "Micro"],
  ];

  const items = names.map(([key, label]) => {
    const item = asObject(layers[key]);
    const regimeLabel = normalizeLabel(item.label);
    const confidence = numOrNull(item.confidence);
    const itemTs = numOrNull(item.ts_ms) || tsMs;
    return {
      key,
      label,
      regimeLabel,
      confidence,
      tsMs: itemTs,
      tone: degraded ? "unavailable" : toneForLabel(regimeLabel),
      text: `${label}: ${regimeLabel}${confidence == null ? "" : ` (${confidence.toFixed(2)})`}`,
    };
  });

  return {
    ok: root.ok !== false && items.some((item) => item.regimeLabel !== "UNKNOWN"),
    degraded,
    source,
    tsMs,
    items,
    fallbackText: degraded
      ? "Regime context unavailable."
      : items.map((item) => item.text).join("; "),
  };
}

export function renderRegimeRibbon(mount, model) {
  if (!mount) return;
  const vm = buildRegimeRibbonViewModel(model);
  mount.innerHTML = `
    <div class="regimeRibbon ${vm.degraded ? "regimeRibbon-degraded" : ""}" role="group" aria-label="${escapeHTML(vm.fallbackText)}">
      ${vm.items.map((item) => `
        <div class="regimeRibbonItem regimeRibbon-${escapeHTML(item.tone)}">
          <span class="regimeRibbonLayer">${escapeHTML(item.label)}</span>
          <span class="regimeRibbonLabel">${escapeHTML(item.regimeLabel)}</span>
          <span class="regimeRibbonMeta">${escapeHTML(item.confidence == null ? "conf —" : `conf ${item.confidence.toFixed(2)}`)}</span>
        </div>
      `).join("")}
      <div class="regimeRibbonFoot">
        <span>${escapeHTML(vm.source)}</span>
        <span>${escapeHTML(fmtTime(vm.tsMs))}</span>
      </div>
      <div class="sr-only">${escapeHTML(vm.fallbackText)}</div>
    </div>
  `;
}
