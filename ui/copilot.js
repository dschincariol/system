/*
  FILE: ui/copilot.js

  Lightweight read-only dashboard copilot panel.
  - Session history only
  - Plain-text rendering only
  - No action execution
  - Safe failure path when backend or model is unavailable
*/

import { buildVoiceContextSnapshot } from "./voice.js";

const PERSONA_KEYS = [
  "alert_persona",
  "dashboard.persona",
  "ui.persona",
  "persona",
];

let _booted = false;
let _history = [];
let _pending = false;

function getEl(id) {
  return document.getElementById(id);
}

function textOf(id) {
  const el = getEl(id);
  return el ? String(el.textContent || "").trim() : "";
}

function safePersona() {
  try {
    for (const key of PERSONA_KEYS) {
      const raw = localStorage.getItem(key);
      const value = String(raw || "").trim().toLowerCase().replace(/[\s-]+/g, "_");
      if (value === "fund_manager" || value === "operations") return value;
    }
  } catch {}
  return "operations";
}

function activeView() {
  return String(document.body?.dataset?.dashboardScreen || "overview").trim().toLowerCase() || "overview";
}

function activeIncident() {
  const row = window.__ACTIVE_INCIDENT__;
  if (!row || typeof row !== "object") return null;
  return {
    id: row.id ?? null,
    severity: row.severity ?? "",
    symbol: row.symbol ?? "",
    event_title: row.event_title ?? row.message ?? "",
    reason: row.reason ?? "",
    expected_z: row.expected_z ?? null,
    confidence: row.confidence ?? null,
    confidence_raw: row.confidence_raw ?? null,
    horizon_s: row.horizon_s ?? null,
    ts_ms: row.ts_ms ?? null,
    status: row.status ?? "",
    acked: !!row.acked,
    resolved: !!row.resolved,
  };
}

function visibleState() {
  const voiceContext = (() => {
    try {
      return buildVoiceContextSnapshot();
    } catch {
      return {};
    }
  })();

  return {
    voice_context: voiceContext,
    screen_label: textOf("dashboardScreenLabel"),
    health_score: {
      value: textOf("healthScoreValue"),
      badge: textOf("healthScoreBadge"),
      summary: textOf("healthScoreSummary"),
      coverage: textOf("healthScoreCoverage"),
    },
  };
}

function starterPrompts() {
  const view = activeView();
  const incident = activeIncident();

  const byView = {
    overview: [
      "What does this mean?",
      "What should I review next?",
      "Why is health degraded?",
    ],
    operate: [
      "What should I review next?",
      "Why is execution degraded?",
      "What does the current operating state mean?",
    ],
    explain: [
      "What does this mean?",
      "What should I review next?",
      "Why is health degraded?",
    ],
    analyze: [
      "What should I review next?",
      "What does this performance picture mean?",
      "Why is health degraded?",
    ],
    data: [
      "Why is health degraded?",
      "What should I review next?",
      "What does the data health state mean?",
    ],
    positions: [
      "What does this position state mean?",
      "What should I review next?",
      "Why is health degraded?",
    ],
    execution: [
      "Why is execution degraded?",
      "What should I review next?",
      "What does this execution view mean?",
    ],
  };

  const prompts = [...(byView[view] || byView.overview)];
  if (incident) {
    prompts.unshift("Explain this alert.");
  }

  const out = [];
  const seen = new Set();
  for (const prompt of prompts) {
    const text = String(prompt || "").trim();
    const key = text.toLowerCase();
    if (!text || seen.has(key)) continue;
    seen.add(key);
    out.push(text);
    if (out.length >= 3) break;
  }
  return out;
}

function fallbackActions() {
  const incident = activeIncident();
  const view = activeView();
  const actions = [];

  if (incident) {
    actions.push("Review the incident drawer facts, reason, severity, and confidence together.");
  }
  if (view === "execution" || view === "operate") {
    actions.push("Review execution barrier and operator summary directly in the dashboard.");
  } else if (view === "data") {
    actions.push("Review the system-status header and ingestion details directly in the dashboard.");
  } else if (view === "positions") {
    actions.push("Review portfolio state, broker snapshot, and equity reconciliation directly.");
  } else {
    actions.push("Review top-level health, decision bar, and active alerts directly in the dashboard.");
  }
  return actions.slice(0, 3);
}

function isOpen() {
  return getEl("copilotPanel")?.classList.contains("is-open") || false;
}

function setOpen(open) {
  const panel = getEl("copilotPanel");
  const btn = getEl("btnCopilotToggle");
  if (!panel || !btn) return;

  panel.classList.toggle("is-open", !!open);
  panel.setAttribute("aria-hidden", open ? "false" : "true");
  btn.setAttribute("aria-expanded", open ? "true" : "false");

  renderStarters();

  if (open) {
    setTimeout(() => {
      try {
        getEl("copilotQuestion")?.focus();
      } catch {}
    }, 0);
  }
}

function escapeText(value) {
  return String(value == null ? "" : value);
}

function renderStarters() {
  const host = getEl("copilotStarters");
  if (!host) return;

  host.replaceChildren();
  for (const prompt of starterPrompts()) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "btn btnSmall copilotStarter";
    btn.setAttribute("data-copilot-prompt", prompt);
    btn.textContent = prompt;
    host.appendChild(btn);
  }
}

function renderThread() {
  const host = getEl("copilotThread");
  if (!host) return;

  host.replaceChildren();

  if (!_history.length) {
    const empty = document.createElement("div");
    empty.className = "copilotEmpty";
    empty.textContent = "Ask for an explanation, a health summary, or what to review next.";
    host.appendChild(empty);
    return;
  }

  for (const item of _history) {
    const row = document.createElement("div");
    row.className = `copilotMessage ${item.role === "user" ? "is-user" : "is-assistant"}`;

    const role = document.createElement("div");
    role.className = "copilotRole";
    role.textContent = item.role === "user" ? "You" : "Copilot";
    row.appendChild(role);

    const body = document.createElement("div");
    body.className = "copilotText";
    body.textContent = escapeText(item.text);
    row.appendChild(body);

    if (item.role === "assistant" && Array.isArray(item.suggestedActions) && item.suggestedActions.length) {
      const label = document.createElement("div");
      label.className = "copilotSuggestedLabel";
      label.textContent = "Review next";
      row.appendChild(label);

      const list = document.createElement("ul");
      list.className = "copilotSuggestedList";
      for (const action of item.suggestedActions) {
        const li = document.createElement("li");
        li.textContent = escapeText(action);
        list.appendChild(li);
      }
      row.appendChild(list);
    }

    host.appendChild(row);
  }

  host.scrollTop = host.scrollHeight;
}

function setPending(pending) {
  _pending = !!pending;
  const form = getEl("copilotForm");
  const input = getEl("copilotQuestion");
  const submit = getEl("btnCopilotAsk");
  const status = getEl("copilotStatus");

  if (form) form.classList.toggle("is-busy", _pending);
  if (input) input.disabled = _pending;
  if (submit) submit.disabled = _pending;
  if (status) {
    status.textContent = _pending ? "Thinking…" : "Read-only";
    status.className = `pill ${_pending ? "warn" : "dim"}`;
  }
}

function pushMessage(role, text, suggestedActions = []) {
  _history.push({
    role: role === "user" ? "user" : "assistant",
    text: String(text || "").trim(),
    suggestedActions: Array.isArray(suggestedActions)
      ? suggestedActions.map((item) => String(item || "").trim()).filter(Boolean).slice(0, 4)
      : [],
  });
  renderThread();
}

async function askCopilot(question) {
  const prompt = String(question || "").trim();
  if (!prompt || _pending) return;

  pushMessage("user", prompt);
  setPending(true);

  const payload = {
    question: prompt,
    active_view: activeView(),
    active_incident: activeIncident(),
    persona: safePersona(),
    visible_state: visibleState(),
    history: _history.slice(-6).map((item) => ({
      role: item.role,
      text: item.text,
    })),
  };

  try {
    const res = await fetch("/api/copilot/ask", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
      },
      cache: "no-store",
      credentials: "same-origin",
      body: JSON.stringify(payload),
    });

    const raw = await res.text();
    let json = null;
    try {
      json = raw ? JSON.parse(raw) : null;
    } catch {
      json = null;
    }

    if (json && typeof json === "object" && (json.answer || Array.isArray(json.suggested_actions))) {
      pushMessage(
        "assistant",
        String(json.answer || "Copilot did not return an answer.").trim(),
        Array.isArray(json.suggested_actions) ? json.suggested_actions : fallbackActions()
      );
    } else if (res.ok) {
      pushMessage(
        "assistant",
        "Copilot returned an unreadable response. Review the dashboard panels directly.",
        fallbackActions()
      );
    } else {
      pushMessage(
        "assistant",
        "Copilot is unavailable right now. Review the suggested dashboard panels directly.",
        fallbackActions()
      );
    }
  } catch {
    pushMessage(
      "assistant",
      "Copilot is unavailable right now. Review the suggested dashboard panels directly.",
      fallbackActions()
    );
  } finally {
    setPending(false);
    const input = getEl("copilotQuestion");
    if (input) input.value = "";
  }
}

function onPanelClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) return;

  const closeBtn = target.closest("[data-copilot-close]");
  if (closeBtn) {
    setOpen(false);
    return;
  }

  const promptBtn = target.closest("[data-copilot-prompt]");
  if (promptBtn) {
    const prompt = promptBtn.getAttribute("data-copilot-prompt") || "";
    void askCopilot(prompt);
  }
}

function onFormSubmit(event) {
  event.preventDefault();
  const input = getEl("copilotQuestion");
  if (!input) return;
  void askCopilot(input.value);
}

function bootCopilot() {
  if (_booted) return;
  _booted = true;

  const panel = getEl("copilotPanel");
  const openBtn = getEl("btnCopilotToggle");
  const form = getEl("copilotForm");
  if (!panel || !openBtn || !form) return;

  if (!openBtn._copilotBound) {
    openBtn._copilotBound = true;
    openBtn.addEventListener("click", () => setOpen(!isOpen()));
  }

  if (!panel._copilotBound) {
    panel._copilotBound = true;
    panel.addEventListener("click", onPanelClick);
  }

  if (!form._copilotBound) {
    form._copilotBound = true;
    form.addEventListener("submit", onFormSubmit);
  }

  renderStarters();
  renderThread();
  setPending(false);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootCopilot, { once: true });
} else {
  bootCopilot();
}
