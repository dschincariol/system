"use strict";

import {
  escapeHTML,
  formatAgeMs,
  statusPillClasses
} from "./utils.js";

const STATUS_ORDER = {
  blocked: 0,
  unavailable: 1,
  warning: 2,
  passing: 3,
};

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function statusTone(status) {
  const value = String(status || "").toLowerCase();
  if (value === "passing") return "ok";
  if (value === "blocked") return "blocked";
  if (value === "warning") return "warn";
  return "unavailable";
}

function statusLabel(status) {
  const value = String(status || "").toLowerCase();
  if (value === "passing") return "PASS";
  if (value === "blocked") return "BLOCKED";
  if (value === "warning") return "WARN";
  return "UNAVAILABLE";
}

function severityRank(item) {
  const status = String(item && item.status || "unavailable").toLowerCase();
  return STATUS_ORDER[status] == null ? 1 : STATUS_ORDER[status];
}

function freshnessText(item) {
  const freshness = asObject(item && item.freshness);
  if (freshness.age_ms == null) return "age unavailable";
  return `${formatAgeMs(freshness.age_ms)} ago`;
}

function screenHrefForCategory(category) {
  const value = String(category || "").toLowerCase();
  if (value === "broker" || value === "execution") return "#screen=execution";
  if (value === "data") return "#screen=data";
  if (value === "governance") return "#screen=explain";
  if (value === "operator" || value === "runtime" || value === "live_trading") return "#screen=operate";
  if (value === "storage") return "#screen=operate";
  return "#screen=overview";
}

export function normalizeReadinessEvidence(payload) {
  const body = asObject(payload);
  const items = asArray(body.items)
    .map((item) => asObject(item))
    .filter((item) => String(item.id || "").trim())
    .sort((a, b) => {
      const rank = severityRank(a) - severityRank(b);
      if (rank !== 0) return rank;
      return String(a.category || "").localeCompare(String(b.category || ""));
    });
  const categories = {};
  items.forEach((item) => {
    const category = String(item.category || item.source_subsystem || "runtime").trim() || "runtime";
    const row = categories[category] || {
      category,
      total: 0,
      blocking: 0,
      warning: 0,
      unavailable: 0,
      passing: 0,
      items: [],
    };
    const status = String(item.status || "unavailable").toLowerCase();
    row.total += 1;
    if (item.blocking === true) row.blocking += 1;
    if (status === "warning") row.warning += 1;
    if (status === "unavailable") row.unavailable += 1;
    if (status === "passing") row.passing += 1;
    row.items.push(item);
    categories[category] = row;
  });
  return {
    ok: body.ok === true,
    status: String(body.status || "unavailable").toLowerCase(),
    mode: String(body.mode || "unknown"),
    executionMode: String(body.execution_mode || "unknown"),
    tsMs: Number(body.ts_ms || 0) || 0,
    items,
    blockers: asArray(body.blockers).map(asObject),
    warnings: asArray(body.warnings).map(asObject),
    unavailable: asArray(body.unavailable).map(asObject),
    categories: Object.values(categories).sort((a, b) => {
      if (b.blocking !== a.blocking) return b.blocking - a.blocking;
      if (b.unavailable !== a.unavailable) return b.unavailable - a.unavailable;
      return String(a.category).localeCompare(String(b.category));
    }),
    actionGuards: asObject(body.action_guards),
  };
}

function groupStatus(group) {
  if (Number(group.blocking || 0) > 0) return "blocked";
  if (Number(group.unavailable || 0) > 0) return "unavailable";
  if (Number(group.warning || 0) > 0) return "warning";
  return "passing";
}

function renderGroup(group) {
  const status = groupStatus(group);
  const preview = asArray(group.items).slice(0, 4).map((item) => {
    const tone = statusTone(item.status);
    return `
      <li class="readinessEvidenceItem readinessEvidenceItem-${escapeHTML(tone)}">
        <span class="${escapeHTML(statusPillClasses(tone))} mono">${escapeHTML(statusLabel(item.status))}</span>
        <span class="readinessEvidenceItemMain">
          <b>${escapeHTML(item.title || item.id)}</b>
          <span>${escapeHTML(item.detail || "No detail supplied.")}</span>
          <span class="metric-meta mono">${escapeHTML(freshnessText(item))} · ${escapeHTML(item.source_subsystem || "unknown")}</span>
        </span>
      </li>
    `;
  }).join("");
  return `
    <section class="readinessEvidenceGroup readinessEvidenceGroup-${escapeHTML(statusTone(status))}">
      <div class="readinessEvidenceGroupHead">
        <span class="${escapeHTML(statusPillClasses(statusTone(status)))} mono">${escapeHTML(statusLabel(status))}</span>
        <a href="${escapeHTML(screenHrefForCategory(group.category))}" class="readinessEvidenceOwner">${escapeHTML(group.category)}</a>
        <span class="metric-meta">${escapeHTML(String(group.blocking || 0))} blocking · ${escapeHTML(String(group.warning || 0))} warnings · ${escapeHTML(String(group.unavailable || 0))} unavailable</span>
      </div>
      <ul class="readinessEvidenceList">${preview || "<li class=\"metric-meta\">No evidence rows.</li>"}</ul>
    </section>
  `;
}

export function renderReadinessEvidencePanel(payload, { root = document } = {}) {
  const panel = root.getElementById("readinessEvidencePanel");
  if (!panel) return null;
  const model = normalizeReadinessEvidence(payload);
  const meta = root.getElementById("readinessEvidenceMeta");
  const groups = root.getElementById("readinessEvidenceGroups");
  const blockers = root.getElementById("readinessEvidenceBlockers");
  const notes = root.getElementById("readinessEvidenceNotes");
  const tone = statusTone(model.status);
  if (meta) {
    meta.className = statusPillClasses(tone);
    meta.textContent = `${statusLabel(model.status)} · ${model.mode}/${model.executionMode}`;
  }
  if (groups) {
    groups.innerHTML = model.categories.length
      ? model.categories.map(renderGroup).join("")
      : "<div class=\"metric-meta\">Readiness evidence unavailable.</div>";
  }
  if (blockers) {
    const rows = model.blockers.slice(0, 8);
    blockers.innerHTML = rows.length
      ? rows.map((item) => `
        <tr class="table-row">
          <td><span class="${escapeHTML(statusPillClasses(statusTone(item.status)))} mono">${escapeHTML(statusLabel(item.status))}</span></td>
          <td>${escapeHTML(item.title || item.id)}</td>
          <td>${escapeHTML(item.source_subsystem || "unknown")}</td>
          <td>${escapeHTML(freshnessText(item))}</td>
          <td>${escapeHTML(item.remediation || "Inspect owning subsystem.")}</td>
        </tr>
      `).join("")
      : "<tr class=\"table-row\"><td colspan=\"5\" class=\"metric-meta\">No blocking readiness evidence.</td></tr>";
  }
  if (notes) {
    const unavailable = model.unavailable.length;
    notes.innerHTML = `
      <div class="opsNote">${escapeHTML(model.ok ? "All critical readiness evidence is passing." : "Critical readiness evidence is blocking or unavailable.")}</div>
      <div class="opsNote mono">${escapeHTML(String(model.items.length))} items · ${escapeHTML(String(model.blockers.length))} blockers · ${escapeHTML(String(unavailable))} unavailable</div>
    `;
  }
  panel.setAttribute("data-readiness-status", model.status);
  return model;
}

export function readinessEvidenceUrlForBrokerActivation(payload) {
  const broker = encodeURIComponent(String(payload && payload.active_broker || "sim").trim().toLowerCase() || "sim");
  const mode = encodeURIComponent(String(payload && payload.paper_live_mode || "safe").trim().toLowerCase() || "safe");
  return `/api/operator/readiness_evidence?mode=${mode}&execution_mode=${mode}&broker=${broker}`;
}

export function brokerActivationReadinessDecision(evidencePayload) {
  const evidence = normalizeReadinessEvidence(evidencePayload);
  const guard = asObject(evidence.actionGuards.broker_activation);
  const blockers = asArray(guard.blockers).map(asObject);
  const warnings = asArray(guard.warnings).map(asObject);
  return {
    ok: guard.allowed !== false,
    requiresConfirmation: guard.requires_confirmation === true || warnings.length > 0,
    blockers,
    warnings,
    evidence,
    message: blockers.length
      ? blockers.slice(0, 3).map((item) => item.title || item.id).join(", ")
      : "",
  };
}

export async function guardBrokerActivationWithReadinessEvidence({
  fetchJSON,
  requestConfirmation,
  toast,
  payload,
} = {}) {
  if (typeof fetchJSON !== "function") {
    return { ok: false, reason: "readiness_fetch_unavailable" };
  }
  let evidencePayload;
  try {
    evidencePayload = await fetchJSON(readinessEvidenceUrlForBrokerActivation(payload), { allowBusinessFalse: true });
  } catch (error) {
    if (typeof toast === "function") {
      toast(`Readiness evidence unavailable: ${error && error.message ? error.message : error}`, "bad", 4800);
    }
    return { ok: false, reason: "readiness_evidence_unavailable", error };
  }
  const decision = brokerActivationReadinessDecision(evidencePayload);
  if (!decision.ok) {
    if (typeof toast === "function") {
      toast(`Broker activation blocked by readiness evidence: ${decision.message || "critical blocker"}`, "bad", 5200);
    }
    return { ok: false, reason: "readiness_blocked", decision, evidence: evidencePayload };
  }
  if (decision.requiresConfirmation && typeof requestConfirmation === "function") {
    const acknowledgement = await requestConfirmation({
      title: "Acknowledge readiness evidence",
      action: "continue broker activation",
      target: `${String(payload && payload.active_broker || "sim").toUpperCase()} ${String(payload && payload.paper_live_mode || "safe").toUpperCase()}`,
      consequence: `Readiness evidence has ${decision.warnings.length} warning or unavailable item(s). Review the readiness card before continuing.`,
      confirmText: "READINESS_ACK",
      requireReason: true,
      minReasonLength: 8,
      submitLabel: "Continue",
      actor: "operator",
      source: "dashboard_readiness_evidence",
    });
    if (!acknowledgement || !acknowledgement.confirmed) {
      return { ok: false, reason: "readiness_ack_cancelled", decision, evidence: evidencePayload };
    }
    return { ok: true, decision, evidence: evidencePayload, acknowledgement };
  }
  return { ok: true, decision, evidence: evidencePayload };
}
