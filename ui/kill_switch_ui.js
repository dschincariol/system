"use strict";

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

  if (stateRows) {
    stateRows.forEach((row) => {
      const key = `${row.scope || "global"}:${row.key || "global"}`;
      const on = Number(row.enabled || 0) === 1;
      const reason = row.reason ? ` (${row.reason})` : "";
      lines.push(`${key}: ${on ? "ENABLED" : "DISABLED"}${reason}`);
    });
  }

  Object.keys(ks).forEach(k => {
    if (k === "state") return;
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

export function buildKillSwitchRows(ks, lastRun = null) {
  const rows = [];
  const root = ks && typeof ks === "object" ? ks : {};
  const stateRows = Array.isArray(root.state) ? root.state : [];

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
      status: enabled ? "ENABLED" : "DISABLED",
      tone: enabled ? "crit" : "ok",
      action: enabled ? "explain" : "hint",
      actionLabel: enabled ? "Explain" : "Recovery hint",
      ariaLabel: `${enabled ? "Explain" : "Recovery hint for"} kill switch ${displayKey} ${enabled ? "enabled" : "disabled"}`,
    });
  });

  Object.keys(root).forEach((key) => {
    if (key === "state") return;
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
