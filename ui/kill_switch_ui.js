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
      `${k}: ${on ? "ENABLED" : "DISABLED"}${reason}${ts}` +
      (!on ? `  [fix]` : "")
    );
  });

  const el = document.getElementById(elId);
  if (!el) return;

  if (!lines.length) {
    lines.push("kill_switches: UNKNOWN");
  }

  el.textContent += "\n\n[automation]\n" + lines.join("\n");

  el.onclick = (e) => {
    // Determine clicked line from mouse Y offset (works for <pre> / monospace blocks)
    try {
      const txt = String(el.textContent || "");
      if (!txt.includes("[fix]")) return;

      const cs = window.getComputedStyle(el);
      const lh = parseFloat(cs.lineHeight) || 16;
      const rect = el.getBoundingClientRect();
      const y = (e && typeof e.clientY === "number") ? (e.clientY - rect.top) : -1;
      if (y < 0) return;

      const linesAll = txt.split("\n");
      const idx = Math.max(0, Math.min(linesAll.length - 1, Math.floor(y / lh)));
      const line = String(linesAll[idx] || "");

      if (!line.includes("[fix]")) return;

      const key = line.split(":")[0].trim();
      if (!key) return;

      showKillSwitchFixHint(key);
    } catch {}
  };
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
  body.textContent =
    "This automation is disabled by configuration.\n\n" +
    "Set the following environment variable and restart the server:\n\n" +
    envMap[key];

  why.style.display = "block";
}
