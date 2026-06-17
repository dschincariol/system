"use strict";

/*
  FILE: ui/decision_attribution.js

  Testable helpers for the decision modal attribution bar. The browser only
  renders backend-provided feature contributions; it never computes SHAP.
*/

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
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

function featureLabel(featureId) {
  return String(featureId || "unknown feature")
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim() || "unknown feature";
}

function contributionValue(row) {
  const item = asObject(row);
  return numOrNull(
    item.attribution ??
    item.contribution ??
    item.value_contribution ??
    item.score,
  );
}

function contributionRows(payload, limit) {
  const root = asObject(payload);
  const rows = asArray(root.top_features || root.features || root.contributions)
    .map((row) => {
      const item = asObject(row);
      const featureId = String(item.feature_id || item.feature || item.name || "").trim();
      const attribution = contributionValue(item);
      if (!featureId || attribution == null) return null;
      const value = numOrNull(item.value);
      return {
        featureId,
        label: featureLabel(featureId),
        value,
        attribution,
        absAttribution: Math.abs(attribution),
        direction: attribution > 0 ? "positive" : attribution < 0 ? "negative" : "neutral",
      };
    })
    .filter(Boolean);
  rows.sort((a, b) => {
    if (b.absAttribution !== a.absAttribution) return b.absAttribution - a.absAttribution;
    return a.featureId.localeCompare(b.featureId);
  });
  return rows.slice(0, Math.max(1, Number(limit) || 6));
}

export function normalizeDecisionAttribution(payload = {}, opts = {}) {
  const root = asObject(payload);
  const limit = Math.max(1, Math.min(20, Number(opts.limit || root.limit || 6)));
  const rows = contributionRows(root, limit);
  if (!rows.length) {
    return {
      available: false,
      rows: [],
      source: String(root.source || "unavailable"),
      reason: String(root.unavailable_reason || root.reason || "No backend feature contributions were provided for this decision."),
      explanationType: String(root.explanation_type || "unavailable"),
      isShap: false,
      supportsShap: Boolean(root.supports_shap),
      baseValue: numOrNull(root.base_value),
      totalAbs: 0,
      maxAbs: 0,
      summary: "Feature attribution unavailable.",
    };
  }

  const maxAbs = Math.max(...rows.map((row) => row.absAttribution), 1e-12);
  const totalAbs = rows.reduce((acc, row) => acc + row.absAttribution, 0);
  const normalizedRows = rows.map((row, index) => {
    const magnitudePct = Math.max(0, Math.min(100, (row.absAttribution / maxAbs) * 100));
    const sharePct = totalAbs > 0 ? Math.max(0, Math.min(100, (row.absAttribution / totalAbs) * 100)) : 0;
    const sign = row.attribution > 0 ? "+" : row.attribution < 0 ? "-" : "0";
    return {
      ...row,
      rank: index + 1,
      magnitudePct,
      sharePct,
      sign,
      directionLabel: row.direction === "positive"
        ? "pushes decision higher"
        : row.direction === "negative"
          ? "pushes decision lower"
          : "neutral contribution",
    };
  });

  const top = normalizedRows[0];
  return {
    available: true,
    rows: normalizedRows,
    source: String(root.source || "decision_payload"),
    reason: "",
    explanationType: String(root.explanation_type || "feature_contribution"),
    isShap: Boolean(root.is_shap),
    supportsShap: Boolean(root.supports_shap),
    baseValue: numOrNull(root.base_value),
    totalAbs,
    maxAbs,
    summary: `${top.label} is the largest signed contribution (${top.sign}${top.absAttribution.toFixed(4)}); ${normalizedRows.length} features shown.`,
  };
}

function formatValue(value, digits = 4) {
  const n = numOrNull(value);
  if (n == null) return "unavailable";
  return n.toFixed(digits);
}

function renderRows(rows) {
  return rows.map((row) => {
    const sideClass = row.direction === "negative" ? "is-negative" : row.direction === "positive" ? "is-positive" : "is-neutral";
    return `
      <div class="decisionAttributionRow ${sideClass}" role="listitem" aria-label="${escapeHTML(`${row.label}: ${row.sign}${formatValue(row.absAttribution)} ${row.directionLabel}`)}">
        <div class="decisionAttributionFeature">
          <span class="decisionAttributionRank">${escapeHTML(String(row.rank))}</span>
          <span>${escapeHTML(row.label)}</span>
        </div>
        <div class="decisionAttributionTrack" aria-hidden="true">
          <span class="decisionAttributionZero"></span>
          <span class="decisionAttributionFill" style="width:${(row.magnitudePct / 2).toFixed(2)}%;"></span>
        </div>
        <div class="decisionAttributionValue mono">
          <span>${escapeHTML(row.sign)}</span>
          <span>${escapeHTML(formatValue(row.absAttribution))}</span>
        </div>
      </div>
    `;
  }).join("");
}

function renderTable(rows) {
  return `
    <table class="decisionAttributionTable">
      <thead>
        <tr>
          <th>Feature</th>
          <th>Sign</th>
          <th>Contribution</th>
          <th>Relative Magnitude</th>
          <th>Feature Value</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map((row) => `
          <tr>
            <td>${escapeHTML(row.label)}</td>
            <td>${escapeHTML(row.directionLabel)}</td>
            <td class="mono">${escapeHTML(row.attribution.toFixed(6))}</td>
            <td class="mono">${escapeHTML(`${row.sharePct.toFixed(1)}%`)}</td>
            <td class="mono">${escapeHTML(formatValue(row.value))}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

export function renderDecisionAttribution(target, payload = {}, opts = {}) {
  const el = typeof target === "string" && typeof document !== "undefined"
    ? document.getElementById(target)
    : target;
  if (!el) return null;
  const vm = normalizeDecisionAttribution(payload, opts);
  if (!vm.available) {
    el.innerHTML = `
      <div class="decisionAttributionUnavailable" role="status">
        <strong>Feature attribution unavailable.</strong>
        <span>${escapeHTML(vm.reason)}</span>
      </div>
    `;
    return vm;
  }

  const shapLabel = vm.isShap ? "SHAP values" : "feature contributions";
  el.innerHTML = `
    <div class="decisionAttribution" data-source="${escapeHTML(vm.source)}">
      <div class="decisionAttributionSummary">
        <span class="pill dim">${escapeHTML(shapLabel)}</span>
        <span>${escapeHTML(vm.summary)}</span>
      </div>
      <div class="decisionAttributionAxis" aria-hidden="true">
        <span>pushes lower</span>
        <span>0 baseline</span>
        <span>pushes higher</span>
      </div>
      <div class="decisionAttributionRows" role="list" aria-label="${escapeHTML(`Signed feature attribution: ${vm.summary}`)}">
        ${renderRows(vm.rows)}
      </div>
      <details class="rawToggle decisionAttributionFallback">
        <summary>Feature Contribution Table</summary>
        ${renderTable(vm.rows)}
      </details>
    </div>
  `;
  return vm;
}

export default {
  normalizeDecisionAttribution,
  renderDecisionAttribution,
};
