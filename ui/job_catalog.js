"use strict";

const SAFETY_TONES = {
  read_only: "neutral",
  data_refresh: "ok",
  training_research: "warn",
  execution_sensitive: "bad",
  destructive_admin: "bad",
  unavailable: "unavailable",
};

const SAFETY_LABELS = {
  read_only: "Read-only",
  data_refresh: "Data refresh",
  training_research: "Training/research",
  execution_sensitive: "Execution-sensitive",
  destructive_admin: "Destructive/admin",
  unavailable: "Unavailable",
};

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function clean(value) {
  return String(value == null ? "" : value).replace(/\s+/g, " ").trim();
}

function escapeHtml(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeSearch(value) {
  return clean(value).toLowerCase();
}

function firstText(...values) {
  for (const value of values) {
    const text = clean(value);
    if (text) return text;
  }
  return "";
}

export function normalizeJobCatalogRows(payload) {
  const root = payload && typeof payload === "object" ? payload : {};
  return asArray(root.catalog).length ? asArray(root.catalog) : asArray(root.jobs);
}

export function jobSafetyLabel(row) {
  const safety = clean(row && row.safety);
  return firstText(row && row.safety_label, SAFETY_LABELS[safety], safety);
}

export function jobSafetyTone(row) {
  const safety = clean(row && row.safety);
  return SAFETY_TONES[safety] || "neutral";
}

export function getJobActionPolicy(row, action) {
  const policies = row && typeof row === "object" && row.action_policy && typeof row.action_policy === "object"
    ? row.action_policy
    : {};
  const policy = policies[clean(action).toLowerCase()];
  return policy && typeof policy === "object" ? policy : null;
}

export function isJobActionEnabled(row, action) {
  const policy = getJobActionPolicy(row, action);
  if (!policy) return false;
  if (policy.enabled === false) return false;
  if (clean(row && row.safety) === "unavailable") return false;
  return true;
}

export function formatJobRunState(row) {
  const latest = row && row.latest_run && typeof row.latest_run === "object" ? row.latest_run : {};
  const state = firstText(latest.state, row && row.running ? "running" : "");
  if (state) return state;
  const code = row && (row.exit_code ?? row.last_exit_code);
  if (code === 0) return "succeeded";
  if (code !== null && code !== undefined && code !== "") return `failed rc=${code}`;
  return "idle";
}

export function jobCatalogFilterOptions(rows) {
  const workflows = new Set();
  const safety = new Set();
  const modes = new Set();
  for (const row of asArray(rows)) {
    const workflow = clean(row && row.workflow);
    const safetyName = clean(row && row.safety);
    const mode = clean(row && row.mode);
    if (workflow) workflows.add(workflow);
    if (safetyName) safety.add(safetyName);
    if (mode) modes.add(mode);
  }
  return {
    workflows: Array.from(workflows).sort(),
    safety: Array.from(safety).sort(),
    modes: Array.from(modes).sort(),
  };
}

export function filterJobCatalogRows(rows, filters = {}) {
  const query = normalizeSearch(filters.search);
  const safetyFilter = clean(filters.safety).toLowerCase();
  const workflowFilter = clean(filters.workflow).toLowerCase();
  const modeFilter = clean(filters.mode).toLowerCase();

  return asArray(rows).filter((row) => {
    const text = [
      row && row.name,
      row && row.label,
      row && row.workflow,
      row && row.group,
      row && row.stage,
      row && row.owner_subsystem,
      row && row.script,
      row && row.module,
      row && row.resource_class,
      row && row.purpose,
      row && row.disabled_reason,
      jobSafetyLabel(row),
      ...asArray(row && row.required_providers),
      ...asArray(row && row.required_secrets),
      ...asArray(row && row.required_secret_any),
      ...asArray(row && row.missing_prerequisites).map((item) => (
        item && typeof item === "object"
          ? [item.type, item.name, item.label, item.reason].join(" ")
          : item
      )),
    ].join(" ").toLowerCase();
    if (query && !text.includes(query)) return false;
    if (safetyFilter && clean(row && row.safety).toLowerCase() !== safetyFilter) return false;
    if (workflowFilter && clean(row && row.workflow).toLowerCase() !== workflowFilter) return false;
    if (modeFilter && clean(row && row.mode).toLowerCase() !== modeFilter) return false;
    return true;
  });
}

export function buildJobCatalogViewModel(payloadOrRows, filters = {}) {
  const rows = Array.isArray(payloadOrRows) ? payloadOrRows : normalizeJobCatalogRows(payloadOrRows);
  const filtered = filterJobCatalogRows(rows, filters);
  const sections = [];
  const byWorkflow = new Map();
  for (const row of filtered) {
    const workflow = firstText(row && row.workflow, "general");
    if (!byWorkflow.has(workflow)) byWorkflow.set(workflow, []);
    byWorkflow.get(workflow).push(row);
  }
  for (const [workflow, items] of byWorkflow.entries()) {
    sections.push({ workflow, rows: items });
  }
  sections.sort((a, b) => a.workflow.localeCompare(b.workflow));
  return {
    rows,
    filtered,
    sections,
    filters: { ...filters },
    options: jobCatalogFilterOptions(rows),
  };
}

function renderPrerequisites(row) {
  const missing = asArray(row && row.missing_prerequisites);
  if (missing.length) {
    return `<span class="jobCatalogPrereq jobCatalogBlocked">${escapeHtml(row.disabled_reason || "Missing prerequisite")}</span>`;
  }
  const providers = asArray(row && row.required_providers);
  const secrets = [...asArray(row && row.required_secrets), ...asArray(row && row.required_secret_any)];
  const bits = [];
  if (providers.length) bits.push(`providers ${providers.join(", ")}`);
  if (secrets.length) bits.push(`secrets ${secrets.join(", ")}`);
  return escapeHtml(bits.join(" / ") || "Ready");
}

function renderActionButton(row, action) {
  const id = clean(row && (row.id || row.name));
  const mode = clean(row && row.mode);
  const policy = getJobActionPolicy(row, action) || {};
  const enabled = isJobActionEnabled(row, action);
  const disabled = enabled ? "" : " disabled aria-disabled=\"true\"";
  const reason = clean(policy.disabled_reason || row && row.disabled_reason);
  const title = reason ? ` title="${escapeHtml(reason)}"` : "";
  const label = action === "start" ? (mode === "daemon" ? "Start" : "Run") : "Stop";
  const danger = policy.safety_confirmation_required ? " dangerAction" : "";
  return `<button class="btn btnSmall jobCatalogAction${danger}" type="button" data-job="${escapeHtml(id)}" data-action="${escapeHtml(action)}"${disabled}${title}>${escapeHtml(label)}</button>`;
}

export function renderJobCatalogRows(viewModelOrRows, options = {}) {
  const viewModel = Array.isArray(viewModelOrRows)
    ? buildJobCatalogViewModel(viewModelOrRows, options.filters || {})
    : viewModelOrRows;
  const sections = asArray(viewModel && viewModel.sections);
  const selected = clean(options.selectedJob);
  if (!sections.length) {
    return `<tr><td colspan="7" class="jobCatalogEmpty">No jobs match the current filters.</td></tr>`;
  }
  const html = [];
  for (const section of sections) {
    const rows = asArray(section.rows);
    html.push(
      `<tr class="jobCatalogGroupRow"><th colspan="7">${escapeHtml(section.workflow)} <span>${rows.length}</span></th></tr>`,
    );
    for (const row of rows) {
      const id = clean(row && (row.id || row.name));
      const active = selected && selected === id ? " is-selected" : "";
      const safetyTone = jobSafetyTone(row);
      const latest = formatJobRunState(row);
      const stage = firstText(row && row.stage, row && row.resource_class, "general");
      const schedule = firstText(row && row.schedule, row && row.cadence_seconds ? `every ${row.cadence_seconds}s` : "");
      const purpose = firstText(row && row.purpose, "No operator purpose is documented.");
      html.push(`
        <tr class="jobCatalogRow${active}" data-job-row="${escapeHtml(id)}">
          <td>
            <button class="jobCatalogSelect" type="button" data-job-select="${escapeHtml(id)}">${escapeHtml(row.label || id)}</button>
            <div class="jobCatalogSub mono">${escapeHtml(id)}</div>
          </td>
          <td><span class="pill ${escapeHtml(safetyTone)}">${escapeHtml(jobSafetyLabel(row))}</span></td>
          <td>${escapeHtml(stage)}<div class="jobCatalogSub">${escapeHtml(row.owner_subsystem || "")}</div></td>
          <td>${escapeHtml(row.mode || "")}<div class="jobCatalogSub">${escapeHtml(schedule)}</div></td>
          <td>${escapeHtml(latest)}<div class="jobCatalogSub">${escapeHtml(row.latest_run && row.latest_run.stale ? "stale heartbeat" : "")}</div></td>
          <td>${renderPrerequisites(row)}<div class="jobCatalogPurpose">${escapeHtml(purpose)}</div></td>
          <td class="jobCatalogActions">
            ${renderActionButton(row, "start")}
            ${renderActionButton(row, "stop")}
            <a class="btn btnSmall" href="${escapeHtml(row.last_output_url || row.log_url || "#")}" target="_blank" rel="noreferrer">Log</a>
          </td>
        </tr>
      `);
    }
  }
  return html.join("");
}

export { escapeHtml as escapeJobCatalogHtml };
