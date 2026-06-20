"use strict";

/*
  FILE: ui/feature_visibility.js

  Render helpers for structured-document and graph-relational feature
  visibility. Backend payloads own authority; this module only formats them.
*/

import {
  escapeHTML,
  fmtTime,
  formatAgeMs,
  formatDecimal,
  freshnessTone,
  numOrNull,
} from "./utils.js";

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function clean(value, fallback = "") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function intValue(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function pillTone(status) {
  const text = clean(status).toLowerCase();
  if (["available", "ok", "shadow_only"].includes(text)) return "ok";
  if (["stale", "warning", "partial"].includes(text)) return "warn";
  if (["blocked", "invalid"].includes(text)) return "crit";
  return "dim";
}

function pillClass(tone) {
  const safe = clean(tone, "dim");
  if (safe === "bad") return "pill bad";
  if (safe === "crit") return "pill crit";
  if (safe === "warn") return "pill warn";
  if (safe === "ok") return "pill ok";
  return "pill dim";
}

function statusLabel(value) {
  return clean(value, "unavailable").replace(/_/g, " ");
}

function timestampLabel(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return "unavailable";
  return fmtTime(n);
}

function warningList(warnings, emptyText) {
  const rows = asArray(warnings).map((item) => clean(item)).filter(Boolean);
  if (!rows.length) return `<div class="opsNote">${escapeHTML(emptyText || "No active warnings.")}</div>`;
  return rows.slice(0, 6).map((item) => `<div class="opsNote warn">${escapeHTML(item)}</div>`).join("");
}

function statGrid(stats) {
  return `
    <div class="opsStatGrid">
      ${stats.map((row) => `
        <div class="opsStat">
          <div class="opsStatLabel">${escapeHTML(row.label)}</div>
          <div class="opsStatValue">${escapeHTML(row.value)}</div>
          <div class="opsStatMeta">${escapeHTML(row.meta || "")}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function topRows(rows, keyName, limit = 6) {
  return asArray(rows).slice(0, limit).map((row) => {
    const item = asObject(row);
    return {
      key: clean(item[keyName], "unknown"),
      count: intValue(item.count, 0),
      latestTs: item.latest_ts_ms || item.latest_availability_ts_ms,
    };
  });
}

function miniTable(rows, keyLabel, emptyText) {
  const safeRows = asArray(rows);
  if (!safeRows.length) {
    return `<div class="metric-meta">${escapeHTML(emptyText || "No rows available.")}</div>`;
  }
  return `
    <div class="table-wrap table-wrap-spaced">
      <table>
        <thead><tr><th>${escapeHTML(keyLabel)}</th><th>Count</th><th>Latest</th></tr></thead>
        <tbody>
          ${safeRows.map((row) => `
            <tr class="table-row">
              <td>${escapeHTML(row.key)}</td>
              <td class="mono">${escapeHTML(formatDecimal(row.count, 0))}</td>
              <td class="metric-meta">${escapeHTML(timestampLabel(row.latestTs))}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function confidenceMarkup(confidence) {
  const root = asObject(confidence);
  const buckets = asArray(root.buckets);
  if (!buckets.length) return `<div class="metric-meta">Confidence distribution unavailable.</div>`;
  return `
    <div class="pill-row">
      ${buckets.map((bucket) => {
        const item = asObject(bucket);
        const count = intValue(item.count, 0);
        const tone = clean(item.label) === "very_low" || clean(item.label) === "low"
          ? (count > 0 ? "warn" : "dim")
          : (count > 0 ? "ok" : "dim");
        return `<span class="${escapeHTML(pillClass(tone))}">${escapeHTML(statusLabel(item.label))} ${escapeHTML(formatDecimal(count, 0))}</span>`;
      }).join("")}
    </div>
  `;
}

function lineageMarkup(lineage) {
  const docs = asArray(asObject(lineage).source_documents).slice(0, 6);
  if (!docs.length) return `<div class="metric-meta">Source document lineage unavailable.</div>`;
  return `
    <div class="table-wrap table-wrap-spaced">
      <table>
        <thead><tr><th>Source Artifact</th><th>Symbol</th><th>Event</th><th>Confidence</th><th>PIT Availability</th></tr></thead>
        <tbody>
          ${docs.map((raw) => {
            const doc = asObject(raw);
            const artifact = clean(doc.source_artifact || doc.source_document_id, "unavailable");
            return `
              <tr class="table-row">
                <td class="mono">${escapeHTML(artifact)}</td>
                <td class="mono">${escapeHTML(clean(doc.symbol, "-"))}</td>
                <td>${escapeHTML(clean(doc.event_type || doc.document_type, "unknown"))}</td>
                <td class="mono">${escapeHTML(numOrNull(doc.extraction_confidence) == null ? "unavailable" : Number(doc.extraction_confidence).toFixed(2))}</td>
                <td class="metric-meta">${escapeHTML(timestampLabel(doc.availability_ts_ms))}</td>
              </tr>
            `;
          }).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function structuredMarkup(payload) {
  const root = asObject(payload);
  const counts = asObject(root.counts);
  const pit = asObject(root.pit_status);
  const failures = asObject(root.extraction_failures);
  const status = clean(root.status, "unavailable");
  const age = root.latest_availability_age_ms;
  const freshness = freshnessTone(age, 24 * 60 * 60 * 1000, 7 * 24 * 60 * 60 * 1000);
  const failureText = failures.available === false
    ? "failure telemetry unavailable"
    : `${formatDecimal(failures.count || 0, 0)} extraction failures`;
  return `
    <div class="pill-row">
      <span class="${escapeHTML(pillClass(pillTone(status)))}">${escapeHTML(statusLabel(status))}</span>
      <span class="pill warn">shadow-only</span>
      <span class="${escapeHTML(pillClass(pit.ok ? "ok" : "warn"))}">PIT ${escapeHTML(pit.ok ? "valid" : "warning")}</span>
      <span class="${escapeHTML(pillClass(freshness))}">freshness ${escapeHTML(formatAgeMs(age))}</span>
      <span class="${escapeHTML(pillClass(failures.available === false ? "warn" : "dim"))}">${escapeHTML(failureText)}</span>
    </div>
    ${statGrid([
      { label: "Events", value: formatDecimal(counts.events || 0, 0), meta: `source docs ${formatDecimal(counts.source_documents || 0, 0)}` },
      { label: "Symbols", value: formatDecimal(counts.symbols || 0, 0), meta: "symbol coverage" },
      { label: "Low Confidence", value: formatDecimal(counts.low_confidence || 0, 0), meta: `threshold ${Number(asObject(root.confidence).low_confidence_threshold || 0.6).toFixed(2)}` },
      { label: "Latest Extraction", value: timestampLabel(root.latest_extraction_ts_ms), meta: `availability ${timestampLabel(root.latest_availability_ts_ms)}` },
    ])}
    <div class="small section-note-title">Confidence Distribution</div>
    ${confidenceMarkup(root.confidence)}
    <div class="small section-note-title">Event Type Coverage</div>
    ${miniTable(topRows(asObject(root.coverage).event_types, "event_type"), "Event Type", "No event types available.")}
    <div class="small section-note-title">Symbol Coverage</div>
    ${miniTable(topRows(asObject(root.coverage).symbols, "symbol"), "Symbol", "No symbol coverage available.")}
    <div class="small section-note-title">Source Lineage</div>
    ${lineageMarkup(root.lineage)}
    <div class="small section-note-title">Warnings</div>
    <div class="opsNotes small">${warningList(root.warnings, "No structured document warnings.")}</div>
  `;
}

function graphMarkup(payload) {
  const root = asObject(payload);
  const counts = asObject(root.counts);
  const featureAvailability = asObject(root.feature_availability);
  const pit = asObject(root.pit_status);
  const status = clean(root.status, "unavailable");
  const freshness = asObject(root.snapshot_freshness);
  const age = root.latest_snapshot_age_ms || freshness.age_ms;
  const latestSnapshot = asObject(asArray(root.snapshots)[0]);
  const sourceArtifact = clean(latestSnapshot.source_artifact, "unavailable");
  const relationRows = asArray(asObject(root.coverage).relationship_types)
    .slice(0, 8)
    .map((row) => ({ key: clean(asObject(row).relationship_type, "unknown"), count: intValue(asObject(row).count, 0), latestTs: root.latest_snapshot_ts_ms }));
  return `
    <div class="pill-row">
      <span class="${escapeHTML(pillClass(pillTone(status)))}">${escapeHTML(statusLabel(status))}</span>
      <span class="pill warn">shadow-only</span>
      <span class="${escapeHTML(pillClass(root.enabled ? "ok" : "warn"))}">env ${escapeHTML(root.enabled ? "enabled" : "disabled")}</span>
      <span class="${escapeHTML(pillClass(pit.ok ? "ok" : "warn"))}">PIT ${escapeHTML(pit.ok ? "valid" : "warning")}</span>
      <span class="${escapeHTML(pillClass(freshnessTone(age, 60 * 60 * 1000, 24 * 60 * 60 * 1000)))}">freshness ${escapeHTML(formatAgeMs(age))}</span>
    </div>
    ${statGrid([
      { label: "Snapshots", value: formatDecimal(counts.snapshots || 0, 0), meta: `symbols ${formatDecimal(counts.symbols || 0, 0)}` },
      { label: "Feature Availability", value: `${formatDecimal(featureAvailability.observed_feature_count || 0, 0)}/${formatDecimal(featureAvailability.expected_feature_count || 0, 0)}`, meta: "observed graph feature ids" },
      { label: "PIT Snapshots", value: `${formatDecimal(pit.pit_valid_snapshot_count || 0, 0)}/${formatDecimal((pit.pit_valid_snapshot_count || 0) + (pit.pit_invalid_snapshot_count || 0), 0)}`, meta: "latest sampled snapshots" },
      { label: "Latest Artifact", value: timestampLabel(root.latest_snapshot_ts_ms), meta: sourceArtifact },
    ])}
    <div class="small section-note-title">Relationship Coverage</div>
    ${miniTable(relationRows, "Relationship", "No relationship coverage available.")}
    <div class="small section-note-title">Snapshot Lineage</div>
    <div class="metric-meta mono">${escapeHTML(sourceArtifact)}</div>
    <div class="metric-meta">graph ${escapeHTML(clean(root.graph_id, "unavailable"))} v${escapeHTML(formatDecimal(root.snapshot_version || 0, 0))}; hash ${escapeHTML(clean(latestSnapshot.relationship_hash, "unavailable"))}</div>
    <div class="small section-note-title">Warnings</div>
    <div class="opsNotes small">${warningList(root.warnings, "No graph feature warnings.")}</div>
  `;
}

export function normalizeFeatureVisibilityPayload(payload = {}) {
  const root = asObject(payload);
  return {
    ok: root.ok !== false,
    tsMs: Number(root.ts_ms) || null,
    structuredDocuments: asObject(root.structured_documents),
    graphFeatures: asObject(root.graph_features),
    meta: asObject(root.meta),
  };
}

export function buildFeatureVisibilityMarkup(payload = {}) {
  const vm = normalizeFeatureVisibilityPayload(payload);
  return {
    structuredHtml: structuredMarkup(vm.structuredDocuments),
    graphHtml: graphMarkup(vm.graphFeatures),
  };
}

export function renderFeatureVisibility(targets = {}, payload = {}) {
  const markup = buildFeatureVisibilityMarkup(payload);
  const structuredEl = typeof targets.structured === "string"
    ? document.getElementById(targets.structured)
    : targets.structured;
  const graphEl = typeof targets.graph === "string"
    ? document.getElementById(targets.graph)
    : targets.graph;
  if (structuredEl) structuredEl.innerHTML = markup.structuredHtml;
  if (graphEl) graphEl.innerHTML = markup.graphHtml;
  return markup;
}

export default {
  buildFeatureVisibilityMarkup,
  normalizeFeatureVisibilityPayload,
  renderFeatureVisibility,
};
