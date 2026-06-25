"use strict";

/*
  FILE: ui/futures_panel.js

  Read-only futures dashboard panel. This module only fetches and renders
  FUT-03/FUT-07 status; it does not expose order entry or mutation controls.
*/

import {
  escapeHTML,
  fmtTime,
  formatAgeMs,
  formatDecimal,
  freshnessTone,
  numOrNull,
  pickTimestamp,
  statusPillClasses,
} from "./utils.js";

export const FUTURES_PANEL_ENDPOINT = "/api/data/futures/rolls?limit=50";

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function ageMsFromTimestamp(tsMs, nowMs) {
  const ts = numOrNull(tsMs);
  if (ts == null || ts <= 0) return null;
  return Math.max(0, (numOrNull(nowMs) ?? Date.now()) - ts);
}

function safeText(value, fallback = "-") {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function setPill(rootDocument, id, tone, text) {
  const el = rootDocument.getElementById(id);
  if (!el) return;
  el.className = statusPillClasses(tone || "neutral");
  el.textContent = String(text || "-");
}

function statMarkup(label, value, meta = "") {
  return `
    <div class="opsStat">
      <div class="opsStatLabel metric-label">${escapeHTML(label)}</div>
      <div class="opsStatValue metric-value">${escapeHTML(value)}</div>
      <div class="opsStatMeta metric-meta">${escapeHTML(meta || "-")}</div>
    </div>
  `;
}

function emptyRow(colspan, text) {
  return `<tr class="table-row"><td colspan="${Number(colspan) || 1}" class="metric-meta">${escapeHTML(text)}</td></tr>`;
}

function tableRows(rows, render, emptyText, colspan) {
  const values = asArray(rows);
  if (!values.length) return emptyRow(colspan, emptyText);
  return values.map(render).join("");
}

export async function fetchFuturesPanel(fetchJSON) {
  return fetchJSON(FUTURES_PANEL_ENDPOINT, { allowBusinessFalse: true });
}

export function normalizeFuturesPanelPayload(payload, { nowMs = Date.now() } = {}) {
  const root = asObject(payload);
  const summary = asObject(root.summary);
  const latestTs = pickTimestamp(root.latest_ts_ms, root.generated_ts_ms);
  const ageMs = ageMsFromTimestamp(latestTs, nowMs);
  const state = safeText(root.state || summary.status, root.ok === false ? "error" : "empty").toLowerCase();
  const warnings = asArray(root.warnings).map((item) => String(item)).filter(Boolean);
  return {
    ok: root.ok !== false,
    state,
    latestTs,
    ageMs,
    generatedTs: numOrNull(root.generated_ts_ms),
    readOnly: root.read_only !== false,
    shadowOnly: root.shadow_only !== false,
    summary: {
      rollCount: numOrNull(summary.roll_count) ?? asArray(root.roll_calendar).length,
      curveCount: numOrNull(summary.curve_count) ?? asArray(root.term_structure).length,
      cotCount: numOrNull(summary.cot_count) ?? asArray(root.cot).length,
      marginCount: numOrNull(summary.margin_count) ?? asArray(root.margin).length,
    },
    rolls: asArray(root.roll_calendar),
    curve: asArray(root.term_structure),
    rollYield: asArray(root.roll_yield),
    cot: asArray(root.cot),
    margin: asArray(root.margin),
    lineage: asObject(root.lineage),
    warnings,
  };
}

export function renderFuturesPanel(rootDocument, payload) {
  if (!rootDocument || !rootDocument.getElementById("futuresPanelCard")) return null;
  const model = normalizeFuturesPanelPayload(payload);
  const freshness = freshnessTone(model.ageMs, 24 * 60 * 60 * 1000, 7 * 24 * 60 * 60 * 1000);
  const stateTone = !model.ok ? "crit" : model.state === "ready" ? "ok" : model.state === "empty" ? "dim" : "warn";

  setPill(rootDocument, "futuresStatePill", stateTone, `state ${model.state}`);
  setPill(rootDocument, "futuresFreshnessPill", freshness, `freshness ${formatAgeMs(model.ageMs)}`);
  setPill(rootDocument, "futuresModePill", model.readOnly ? "ok" : "crit", model.readOnly ? "read only" : "mutation risk");
  setPill(rootDocument, "futuresLineagePill", model.shadowOnly ? "info" : "warn", model.shadowOnly ? "shadow only" : "live surface");

  const summaryEl = rootDocument.getElementById("futuresSummaryGrid");
  if (summaryEl) {
    summaryEl.innerHTML = [
      statMarkup("Roll Events", formatDecimal(model.summary.rollCount, 0), "OI/volume calendar"),
      statMarkup("Curve Points", formatDecimal(model.summary.curveCount, 0), "latest contract bars"),
      statMarkup("COT Rows", formatDecimal(model.summary.cotCount, 0), "futures anchored"),
      statMarkup("Margin Rows", formatDecimal(model.summary.marginCount, 0), "contract metadata"),
    ].join("");
  }

  const rollsBody = rootDocument.getElementById("futuresRollsBody");
  if (rollsBody) {
    rollsBody.innerHTML = tableRows(
      model.rolls.slice(0, 8),
      (row) => `
        <tr class="table-row">
          <td class="mono">${escapeHTML(safeText(row.root))}</td>
          <td>${escapeHTML(fmtTime(row.roll_ts_ms))}</td>
          <td class="mono">${escapeHTML(safeText(row.from_contract))}</td>
          <td class="mono">${escapeHTML(safeText(row.to_contract))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.gap_ratio, 4))}</td>
        </tr>
      `,
      "No roll calendar rows.",
      5
    );
  }

  const curveBody = rootDocument.getElementById("futuresCurveBody");
  if (curveBody) {
    curveBody.innerHTML = tableRows(
      model.curve.slice(0, 10),
      (row) => `
        <tr class="table-row">
          <td class="mono">${escapeHTML(safeText(row.symbol))}</td>
          <td class="mono">${escapeHTML(safeText(row.root))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.close, 4))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.open_interest, 0))}</td>
          <td>${escapeHTML(fmtTime(row.ts_ms))}</td>
        </tr>
      `,
      "No term-structure points.",
      5
    );
  }

  const cotBody = rootDocument.getElementById("futuresCotBody");
  if (cotBody) {
    cotBody.innerHTML = tableRows(
      model.cot.slice(0, 8),
      (row) => `
        <tr class="table-row">
          <td class="mono">${escapeHTML(safeText(row.symbol))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.noncomm_net_z, 2))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.commercial_net_pctile_3y, 2))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.open_interest_z, 2))}</td>
          <td>${escapeHTML(fmtTime(row.asof_ts_ms))}</td>
        </tr>
      `,
      "No futures COT rows.",
      5
    );
  }

  const marginBody = rootDocument.getElementById("futuresMarginBody");
  if (marginBody) {
    marginBody.innerHTML = tableRows(
      model.margin.slice(0, 10),
      (row) => `
        <tr class="table-row">
          <td class="mono">${escapeHTML(safeText(row.symbol))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.position_qty, 0))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.multiplier, 2))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.one_contract_notional, 0))}</td>
          <td class="table-cell-num">${escapeHTML(formatDecimal(row.margin_ref, 0))}</td>
        </tr>
      `,
      "No futures margin metadata.",
      5
    );
  }

  const notesEl = rootDocument.getElementById("futuresNotes");
  if (notesEl) {
    const notes = [];
    notes.push(`lineage: ${asArray(model.lineage.tables).join(", ") || safeText(model.lineage.source, "none")}`);
    if (model.latestTs) notes.push(`latest input: ${fmtTime(model.latestTs)}`);
    model.warnings.slice(0, 4).forEach((warning) => notes.push(`warning: ${warning}`));
    notesEl.innerHTML = notes.length
      ? notes.map((note) => `<div class="opsNote">${escapeHTML(note)}</div>`).join("")
      : `<div class="opsNote">No futures warnings reported.</div>`;
  }

  return model;
}

export async function loadFuturesPanel({ fetchJSON, document: rootDocument = document } = {}) {
  if (!rootDocument || !rootDocument.getElementById("futuresPanelCard")) return null;
  if (typeof fetchJSON !== "function") {
    throw new TypeError("loadFuturesPanel requires fetchJSON");
  }
  const payload = await fetchFuturesPanel(fetchJSON);
  return renderFuturesPanel(rootDocument, payload);
}

