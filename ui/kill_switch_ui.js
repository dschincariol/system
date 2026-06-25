"use strict";

import { requestConfirmation } from "./confirmation_modal.mjs";

/*
  ui/kill_switch_ui.js — Kill-switch UI engine
  Extracted from ui/dashboard.js (Phase 6)

  Responsibilities:
    - renderKillSwitchPills(ks, lastRun, elId)
    - showKillSwitchFixHint(key)

  This module is presentation-only. It reflects backend kill-switch state in
  the dashboard and may suggest recovery actions, but it does not own the
  authoritative execution barrier.
*/

export function renderKillSwitchPills(ks, lastRun, elId = "systemStateText") {
  if (!ks || typeof ks !== "object") return;

  const lines = [];
  const controlRows = buildKillSwitchRows(ks, lastRun);
  const stateRows = Array.isArray(ks.state) ? ks.state : null;
  const effective = killSwitchEffectiveState(ks);

  if (effective) {
    lines.push(`effective:any: ${effectiveStatus(effective)} (${effectiveReason(effective)})`);
  }

  if (stateRows) {
    stateRows.forEach((row) => {
      const key = `${row.scope || "global"}:${row.key || "global"}`;
      const on = Number(row.enabled || 0) === 1;
      const reason = row.reason ? ` (${row.reason})` : "";
      lines.push(`${key}: ${on ? "PERSISTED ARMED" : "PERSISTED DISARMED"}${reason}`);
    });
  }

  Object.keys(ks).forEach(k => {
    if (KILL_SWITCH_METADATA_KEYS.has(k)) return;
    const v = ks[k] || {};
    const on = !!v.enabled;
    const reason = v.reason ? ` (${v.reason})` : "";
    const ts = lastRun && lastRun[k]
      ? ` last_run=${new Date(lastRun[k] * 1000).toLocaleString()}`
      : " last_run=—";

    lines.push(
      `${k}: ${on ? "ENABLED" : "DISABLED"}${reason}${ts}`
    );
  });

  const el = document.getElementById(elId);
  if (!el) return;

  if (!lines.length) {
    lines.push("kill_switches: UNKNOWN");
  }

  el.textContent += "\n\n[automation]\n" + lines.join("\n");
  renderKillSwitchControls(el, controlRows);
}

function esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const KILL_SWITCH_METADATA_KEYS = new Set([
  "state",
  "loaded_ts_ms",
  "source",
  "max_age_ms",
  "cache_age_ms",
  "cache_fresh",
  "read_source",
  "cache_status",
  "effective",
  "effective_state",
  "provenance",
  "activation_failure",
]);

function killSwitchEffectiveState(ks) {
  const effective = ks && typeof ks === "object" && ks.effective && typeof ks.effective === "object"
    ? ks.effective
    : null;
  return effective;
}

function effectiveSources(effective) {
  const sources = Array.isArray(effective && effective.sources)
    ? effective.sources.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  return sources.length ? sources.join("+") : "unknown";
}

function effectiveStatus(effective) {
  if (!effective || typeof effective !== "object") return "UNKNOWN";
  return effective.armed ? `ARMED VIA ${effectiveSources(effective).toUpperCase()}` : "DISARMED";
}

function effectiveReason(effective) {
  if (!effective || typeof effective !== "object") return "effective state unavailable";
  const summary = String(effective.summary || "").trim();
  if (summary) return summary;
  const persisted = effective.persisted_armed ? "persisted armed" : "persisted disarmed";
  return effective.armed ? `armed via ${effectiveSources(effective)}; ${persisted}` : `disarmed; ${persisted}`;
}

export function buildKillSwitchRows(ks, lastRun = null) {
  const rows = [];
  const root = ks && typeof ks === "object" ? ks : {};
  const stateRows = Array.isArray(root.state) ? root.state : [];
  const effective = killSwitchEffectiveState(root);

  if (effective) {
    const armed = !!effective.armed;
    const reason = effectiveReason(effective);
    rows.push({
      id: "effective:any",
      key: "effective",
      scope: "effective",
      displayKey: "effective:any",
      enabled: armed,
      reason,
      status: effectiveStatus(effective),
      tone: armed ? "crit" : "ok",
      action: "explain",
      actionLabel: "Explain",
      ariaLabel: `Explain effective kill-switch state ${armed ? "armed" : "disarmed"}`,
    });
  }

  stateRows.forEach((row) => {
    const scope = String((row && row.scope) || "global").trim() || "global";
    const key = String((row && row.key) || "global").trim() || "global";
    const enabled = Number((row && row.enabled) || 0) === 1;
    const reason = String((row && row.reason) || "").trim();
    const displayKey = `${scope}:${key}`;
    rows.push({
      id: displayKey,
      key,
      scope,
      displayKey,
      enabled,
      reason,
      status: enabled ? "PERSISTED ARMED" : "PERSISTED DISARMED",
      tone: enabled ? "crit" : "ok",
      action: enabled ? "explain" : "hint",
      actionLabel: enabled ? "Explain" : "Recovery hint",
      ariaLabel: `${enabled ? "Explain" : "Recovery hint for"} kill switch ${displayKey} ${enabled ? "enabled" : "disabled"}`,
    });
  });

  Object.keys(root).forEach((key) => {
    if (KILL_SWITCH_METADATA_KEYS.has(key)) return;
    const value = root[key] || {};
    if (!value || typeof value !== "object") return;
    const enabled = !!value.enabled;
    const reason = String(value.reason || "").trim();
    const lastRunTs = lastRun && lastRun[key] ? Number(lastRun[key]) * 1000 : null;
    rows.push({
      id: String(key),
      key: String(key),
      scope: "automation",
      displayKey: String(key),
      enabled,
      reason,
      lastRunTs,
      status: enabled ? "ENABLED" : "DISABLED",
      tone: enabled ? "ok" : "warn",
      action: enabled ? "explain" : "hint",
      actionLabel: enabled ? "Explain" : "Recovery hint",
      ariaLabel: `${enabled ? "Explain" : "Recovery hint for"} automation ${key} ${enabled ? "enabled" : "disabled"}`,
    });
  });

  return rows;
}

function renderKillSwitchControls(anchorEl, rows) {
  if (!anchorEl || !anchorEl.parentNode) return;
  const doc = anchorEl.ownerDocument || document;
  const id = `${anchorEl.id || "systemStateText"}KillSwitchRows`;
  let mount = doc.getElementById(id);
  if (!mount) {
    mount = doc.createElement("div");
    mount.id = id;
    mount.className = "killSwitchRows";
    anchorEl.insertAdjacentElement("beforebegin", mount);
  }

  if (!Array.isArray(rows) || !rows.length) {
    mount.innerHTML = '<div class="killSwitchEmpty small">Kill-switch state unavailable.</div>';
    return;
  }

  mount.innerHTML = rows.map((row) => `
    <div class="killSwitchRow killSwitchRow-${esc(row.tone)}">
      <span class="killSwitchLight" aria-hidden="true"></span>
      <span class="killSwitchName">${esc(row.displayKey)}</span>
      <span class="killSwitchState">${esc(row.status)}</span>
      <span class="killSwitchReason">${esc(row.reason || "no reason reported")}</span>
      <button
        type="button"
        class="btn killSwitchAction"
        data-ks-action="${esc(row.action)}"
        data-ks-key="${esc(row.key)}"
        data-ks-display-key="${esc(row.displayKey)}"
        data-ks-reason="${esc(row.reason || "")}"
        aria-label="${esc(row.ariaLabel)}"
      >${esc(row.actionLabel)}</button>
    </div>
  `).join("");

  if (!mount._boundKillSwitchRows) {
    mount._boundKillSwitchRows = true;
    mount.addEventListener("click", (event) => {
      const target = event && event.target && event.target.closest
        ? event.target.closest("[data-ks-action]")
        : null;
      if (!target) return;
      const key = target.getAttribute("data-ks-key") || "";
      const displayKey = target.getAttribute("data-ks-display-key") || key;
      const reason = target.getAttribute("data-ks-reason") || "";
      const action = target.getAttribute("data-ks-action") || "explain";
      if (action === "hint") {
        showKillSwitchFixHint(key || displayKey);
      } else {
        showKillSwitchExplanation(displayKey, reason);
      }
    });
  }

  renderBrokerRiskControls(mount);
}

function renderBrokerRiskControls(mount) {
  const doc = mount.ownerDocument || document;
  const id = "brokerRiskControls";
  let controls = doc.getElementById(id);
  if (!controls) {
    controls = doc.createElement("div");
    controls.id = id;
    controls.className = "killSwitchRows brokerRiskControls";
    mount.insertAdjacentElement("afterend", controls);
  }

  controls.innerHTML = `
    <div class="killSwitchRow killSwitchRow-crit">
      <span class="killSwitchLight" aria-hidden="true"></span>
      <span class="killSwitchName">broker:risk</span>
      <span class="killSwitchState">OPERATOR</span>
      <span class="killSwitchReason">confirmed broker order controls</span>
      <button type="button" class="btn killSwitchAction" data-broker-risk-policy="cancel_only">Cancel</button>
      <button type="button" class="btn killSwitchAction" data-broker-risk-policy="cancel_and_flatten">Flatten</button>
    </div>
  `;

  if (!controls._boundBrokerRiskControls) {
    controls._boundBrokerRiskControls = true;
    controls.addEventListener("click", (event) => {
      const target = event && event.target && event.target.closest
        ? event.target.closest("[data-broker-risk-policy]")
        : null;
      if (!target) return;
      const policy = target.getAttribute("data-broker-risk-policy") || "";
      runBrokerRiskAction(policy);
    });
  }
}

async function runBrokerRiskAction(policy) {
  const normalized = String(policy || "").trim();
  if (!normalized) return;
  const confirmation = await requestConfirmation({
    actionId: "operator.broker_risk",
    title: normalized === "cancel_only" ? "Cancel Broker Orders" : "Flatten Broker Risk",
    confirmText: "BROKER_RISK",
    consequence: "Cancels live broker orders and may submit flattening orders under configured shutdown-risk limits.",
    holdMs: 3000,
    requireReason: true,
    target: `broker:${normalized}`,
  });
  if (!confirmation || !confirmation.ok) return;
  await fetch("/api/operator/broker_risk", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      ...confirmation.payload,
      policy: normalized,
      reason: confirmation.reason,
      source_surface: "dashboard_kill_switch",
      source: "dashboard_kill_switch",
      target: `broker:${normalized}`,
    }),
  });
}

export function showKillSwitchFixHint(key) {
  const why = document.getElementById("whyModal");
  const body = document.getElementById("whyBody");
  const title = document.getElementById("whyTitle");

  if (!why || !body || !title) return;

  const envMap = {
    auto_pipeline: "AUTO_PIPELINE=1",
    auto_challenger: "AUTO_CHALLENGER=1",
    auto_size_policy: "AUTO_SIZE_POLICY=1",
  };

  title.textContent = "How to re-enable " + key;
  body.textContent = envMap[key]
    ? (
      "This automation is disabled by configuration.\n\n" +
      "Set the following environment variable and restart the server:\n\n" +
      envMap[key]
    )
    : (
      "This kill-switch row is disabled or has no configured automatic recovery hint.\n\n" +
      "Review the reported reason and backend state before changing runtime configuration."
    );

  why.style.display = "block";
}

export function showKillSwitchExplanation(key, reason = "") {
  const why = document.getElementById("whyModal");
  const body = document.getElementById("whyBody");
  const title = document.getElementById("whyTitle");

  if (!why || !body || !title) return;

  title.textContent = "Kill-switch state: " + key;
  body.textContent =
    "This is a read-only kill-switch status row.\n\n" +
    "Status details:\n\n" +
    (reason ? reason : "No reason was reported by the backend snapshot.") +
    "\n\nThe dashboard does not grant mutation authority from this control.";

  why.style.display = "block";
}
