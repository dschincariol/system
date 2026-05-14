"use strict";

import { numOrNull } from "./utils.js";

const PANEL_STATES = new Set(["fresh", "stale", "empty", "error"]);

function normalizePanelState(state) {
  const normalized = String(state || "").toLowerCase();
  return PANEL_STATES.has(normalized) ? normalized : "fresh";
}

export function applyStalenessState(el, ageMs, warnMs = 60_000, critMs = 300_000) {
  if (!el) return;
  el.classList.remove("data-stale", "data-stale-warning", "data-stale-critical");
  const age = numOrNull(ageMs);
  if (age == null) return;
  if (age >= warnMs) {
    el.classList.add("data-stale", "data-stale-warning");
  }
  if (age >= critMs) {
    el.classList.add("data-stale-critical");
  }
}

export function stalenessClassNames(ageMs, warnMs = 60_000, critMs = 300_000) {
  const age = numOrNull(ageMs);
  if (age == null || age < warnMs) return "";
  return age >= critMs
    ? "data-stale data-stale-warning data-stale-critical"
    : "data-stale data-stale-warning";
}

function ensurePanelStateElements(cardId) {
  const card = document.getElementById(cardId);
  if (!card) return null;

  let row = card.querySelector(".panelStateRow");
  if (!row) {
    row = document.createElement("div");
    row.className = "panelStateRow";
    row.innerHTML = `
      <span class="panelStateBadge is-empty">empty</span>
      <span class="panelStateText">Waiting for data.</span>
    `;
    const header = card.querySelector("h2");
    if (header && header.nextSibling) {
      card.insertBefore(row, header.nextSibling);
    } else if (header) {
      card.appendChild(row);
    } else {
      card.insertBefore(row, card.firstChild);
    }
  }

  return {
    card,
    row,
    badge: row.querySelector(".panelStateBadge"),
    text: row.querySelector(".panelStateText"),
  };
}

export function setPanelState(cardId, { state = "fresh", reason = "" } = {}) {
  const nodes = ensurePanelStateElements(cardId);
  if (!nodes) return;

  const normalized = normalizePanelState(state);
  const nextReason = String(reason || "No additional detail provided.");

  if (nodes.badge) {
    nodes.badge.className = `panelStateBadge is-${normalized}`;
    nodes.badge.textContent = normalized;
  }
  if (nodes.text) {
    nodes.text.textContent = nextReason;
  }
  nodes.card.dataset.panelState = normalized;
}

export function setSurfaceState(surfaceId, { state = "fresh", reason = "" } = {}) {
  const el = document.getElementById(surfaceId);
  if (!el) return;

  const normalized = normalizePanelState(state);
  const nextReason = String(reason || "");

  if (!nextReason || normalized === "fresh") {
    el.className = "surfaceOverlayState";
    el.textContent = "";
    return;
  }

  el.className = `surfaceOverlayState is-visible is-${normalized}`;
  el.textContent = `${normalized.toUpperCase()}: ${nextReason}`;
}
