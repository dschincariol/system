"use strict";

const ROOT_ID = "dashboardCommandPaletteRoot";
const STYLE_ID = "dashboardCommandPaletteStyle";
const DATA_TTL_MS = 15000;
const DEFAULT_LIMIT = 14;

const UNSAFE_JOB_PATTERNS = [
  /broker/i,
  /apply[_-]?orders/i,
  /\border\b/i,
  /trade/i,
  /live[_-]?trading/i,
  /kill/i,
  /emergency/i,
  /force/i,
  /promote/i,
  /rollback/i,
  /rebalance/i,
];

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function cleanLabel(value) {
  return String(value == null ? "" : value)
    .replace(/\s+/g, " ")
    .trim();
}

function stripIconText(value) {
  return cleanLabel(value).replace(/^[^\w]+/u, "").trim() || cleanLabel(value);
}

function commandId(prefix, value) {
  return `${prefix}:${String(value == null ? "" : value).trim().toLowerCase()}`;
}

function capitalize(value) {
  const s = String(value || "").trim().toLowerCase();
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : "";
}

export function normalizeCommandText(value) {
  return String(value == null ? "" : value)
    .toLowerCase()
    .replace(/[_:/#.,()[\]{}]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function fuzzyScoreText(query, candidate) {
  const q = normalizeCommandText(query);
  const text = normalizeCommandText(candidate);
  if (!q) return 1;
  if (!text) return 0;
  if (text === q) return 1000 + q.length * 4;

  const direct = text.indexOf(q);
  if (direct >= 0) {
    return 800 + q.length * 8 - direct * 2 - Math.max(0, text.length - q.length) * 0.05;
  }

  let qi = 0;
  let lastMatch = -1;
  let streak = 0;
  let score = 0;

  for (let i = 0; i < text.length && qi < q.length; i += 1) {
    if (text[i] !== q[qi]) continue;
    const boundary = i === 0 || text[i - 1] === " ";
    streak = lastMatch === i - 1 ? streak + 1 : 1;
    const gap = lastMatch >= 0 ? i - lastMatch - 1 : i;
    score += 10 + (boundary ? 10 : 0) + Math.min(streak, 5) * 3 - Math.min(gap, 18) * 0.35;
    lastMatch = i;
    qi += 1;
  }

  if (qi !== q.length) return 0;
  return Math.max(1, score - text.length * 0.04);
}

export function scoreCommandItem(item, query) {
  const q = normalizeCommandText(query);
  const priority = Number(item && item.priority);
  const basePriority = Number.isFinite(priority) ? priority : 0;
  if (!q) return 1 + basePriority;

  const keywords = Array.isArray(item && item.keywords) ? item.keywords : [];
  const searchText = [
    item && item.title,
    item && item.subtitle,
    item && item.badge,
    item && item.searchText,
    ...keywords,
  ].join(" ");
  const score = fuzzyScoreText(q, searchText);
  return score > 0 ? score + basePriority : 0;
}

export function filterCommandItems(items, query, options = {}) {
  const limit = Math.max(1, Math.min(50, Number(options.limit || DEFAULT_LIMIT)));
  const unique = new Map();
  for (const item of asArray(items)) {
    if (!item || !item.id || unique.has(item.id)) continue;
    unique.set(item.id, item);
  }

  return Array.from(unique.values())
    .map((item) => ({ item, score: scoreCommandItem(item, query) }))
    .filter((entry) => entry.score > 0)
    .sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      return cleanLabel(a.item.title).localeCompare(cleanLabel(b.item.title));
    })
    .slice(0, limit)
    .map((entry) => entry.item);
}

export function parseDecisionIdQuery(query) {
  const raw = String(query || "").trim();
  if (!raw) return "";
  const match = raw.match(/^(?:decision(?:[_\s-]?id)?|id|#)?\s*(\d{1,18})$/i);
  return match ? match[1] : "";
}

export function isSafePaletteJobAction(jobName, action = "start") {
  const normalizedAction = String(action || "").trim().toLowerCase();
  if (!["start", "stop"].includes(normalizedAction)) return false;
  const name = String(jobName || "").trim();
  if (!name) return false;
  return !UNSAFE_JOB_PATTERNS.some((pattern) => pattern.test(name));
}

function normalizeSymbolsPayload(payload) {
  if (Array.isArray(payload)) return payload;
  const root = payload && typeof payload === "object" ? payload : {};
  if (Array.isArray(root.symbols)) return root.symbols;
  if (Array.isArray(root.watchlist)) return root.watchlist;
  if (Array.isArray(root.rows)) return root.rows;
  return [];
}

function normalizeModelRows(payload) {
  const root = payload && typeof payload === "object" ? payload : {};
  const rows = Array.isArray(root.history)
    ? root.history
    : (Array.isArray(root.rows) ? root.rows : []);
  const out = [...rows];
  for (const key of ["champion", "challenger"]) {
    if (root[key] && typeof root[key] === "object" && Object.keys(root[key]).length) {
      out.push({ stage: key, ...root[key] });
    }
  }
  return out;
}

function normalizeJobRows(payload) {
  const root = payload && typeof payload === "object" ? payload : {};
  return Array.isArray(root.jobs) ? root.jobs : [];
}

async function defaultFetchJSON(path) {
  const res = await fetch(path, { cache: "no-store" });
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    throw new Error((data && data.error) || `${res.status} ${res.statusText}`);
  }
  return data;
}

function ensureStyles(doc) {
  if (!doc || doc.getElementById(STYLE_ID)) return;
  const style = doc.createElement("style");
  style.id = STYLE_ID;
  style.textContent = `
.commandPaletteOverlay{
  position:fixed;
  inset:0;
  z-index:12000;
  display:none;
  align-items:flex-start;
  justify-content:center;
  padding:72px 16px 16px;
  background:rgba(0,0,0,.52);
}
.commandPaletteOverlay.is-open{display:flex;}
.commandPaletteDialog{
  width:min(720px, 100%);
  max-height:min(680px, calc(100vh - 96px));
  display:flex;
  flex-direction:column;
  overflow:hidden;
  border:1px solid rgba(88,166,255,.28);
  border-radius:8px;
  background:#0b0f15;
  box-shadow:0 22px 72px rgba(0,0,0,.48);
}
.commandPaletteTop{
  display:flex;
  align-items:center;
  gap:10px;
  padding:12px;
  border-bottom:1px solid rgba(255,255,255,.08);
}
.commandPaletteTitle{
  font-size:13px;
  font-weight:800;
  color:#e6edf3;
  white-space:nowrap;
}
.commandPaletteInput{
  flex:1 1 auto;
  min-width:0;
  height:38px;
  border:1px solid #30363d;
  border-radius:8px;
  background:#0a0d12;
  color:#e6edf3;
  padding:0 12px;
  font-size:14px;
  outline:none;
}
.commandPaletteInput:focus{border-color:#58a6ff; box-shadow:0 0 0 2px rgba(88,166,255,.18);}
.commandPaletteStatus{
  min-height:20px;
  padding:7px 12px;
  border-bottom:1px solid rgba(255,255,255,.06);
  color:#9da7b3;
  font-size:12px;
}
.commandPaletteStatus.is-error{color:#ffb4b4;}
.commandPaletteList{
  overflow:auto;
  padding:6px;
}
.commandPaletteItem{
  width:100%;
  display:grid;
  grid-template-columns:minmax(0,1fr) auto;
  gap:10px;
  align-items:center;
  text-align:left;
  border:1px solid transparent;
  border-radius:8px;
  background:transparent;
  color:#e6edf3;
  padding:10px;
  cursor:pointer;
}
.commandPaletteItem:hover,
.commandPaletteItem.is-active{
  border-color:rgba(88,166,255,.36);
  background:rgba(88,166,255,.12);
}
.commandPaletteItemTitle{
  min-width:0;
  font-size:14px;
  font-weight:800;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
}
.commandPaletteItemSubtitle{
  min-width:0;
  margin-top:3px;
  color:#9da7b3;
  font-size:12px;
  line-height:1.35;
  white-space:nowrap;
  overflow:hidden;
  text-overflow:ellipsis;
}
.commandPaletteBadge{
  justify-self:end;
  border:1px solid rgba(255,255,255,.14);
  border-radius:999px;
  padding:3px 8px;
  color:#9da7b3;
  background:rgba(255,255,255,.04);
  font-size:11px;
  font-weight:800;
  white-space:nowrap;
}
.commandPaletteBadge.is-confirm{
  color:#d29922;
  border-color:rgba(210,153,34,.45);
  background:rgba(210,153,34,.12);
}
.commandPaletteState{
  padding:18px 14px;
  color:#9da7b3;
  font-size:13px;
  line-height:1.45;
}
.commandPaletteState.is-error{color:#ffb4b4;}
.commandPaletteTargetFlash{
  outline:2px solid #58a6ff;
  outline-offset:2px;
  box-shadow:0 0 0 4px rgba(88,166,255,.16);
}
@media (max-width: 640px){
  .commandPaletteOverlay{padding-top:36px;}
  .commandPaletteTop{align-items:stretch; flex-wrap:wrap;}
  .commandPaletteTitle{width:100%;}
  .commandPaletteInput{flex-basis:100%;}
  .commandPaletteItem{grid-template-columns:minmax(0,1fr);}
  .commandPaletteBadge{justify-self:start;}
}
`;
  doc.head.appendChild(style);
}

function createShell(doc) {
  let root = doc.getElementById(ROOT_ID);
  if (root) return root;

  root = doc.createElement("div");
  root.id = ROOT_ID;
  root.className = "commandPaletteOverlay";
  root.setAttribute("aria-hidden", "true");
  root.innerHTML = `
    <div class="commandPaletteDialog" role="dialog" aria-modal="true" aria-label="Command palette">
      <div class="commandPaletteTop">
        <div class="commandPaletteTitle">Command palette</div>
        <input
          id="dashboardCommandPaletteInput"
          class="commandPaletteInput"
          type="text"
          autocomplete="off"
          spellcheck="false"
          role="combobox"
          aria-expanded="true"
          aria-controls="dashboardCommandPaletteList"
          placeholder="Search screens, panels, symbols, models, jobs, or decision ID"
        />
        <button class="btn btnSmall" id="dashboardCommandPaletteClose" type="button">Close</button>
      </div>
      <div id="dashboardCommandPaletteStatus" class="commandPaletteStatus" role="status" aria-live="polite"></div>
      <div id="dashboardCommandPaletteList" class="commandPaletteList" role="listbox"></div>
    </div>
  `;
  doc.body.appendChild(root);
  return root;
}

function getVisibleScreenTargets(doc) {
  const targets = [];
  doc.querySelectorAll("[data-screen-target]").forEach((btn) => {
    if (btn.classList.contains("dashboard-persona-hidden")) return;
    const screen = cleanLabel(btn.getAttribute("data-screen-target")).toLowerCase();
    if (screen && !targets.includes(screen)) targets.push(screen);
  });
  return targets;
}

function buildStaticItems(doc, options) {
  const items = [];
  const labels = options.screenLabels || {};
  const allowedScreens = getVisibleScreenTargets(doc);
  const screenSet = new Set(allowedScreens.length ? allowedScreens : Object.keys(labels));

  for (const screen of screenSet) {
    const label = labels[screen] || capitalize(screen);
    items.push({
      id: commandId("screen", screen),
      title: `Go to ${label}`,
      subtitle: "Dashboard screen",
      badge: "Screen",
      keywords: [screen, label, "tab", "navigate"],
      priority: 80,
      run: () => options.navigateToScreen && options.navigateToScreen(screen),
    });
  }

  doc.querySelectorAll("#page-dashboard [id][data-screens]").forEach((el) => {
    const screens = cleanLabel(el.getAttribute("data-screens"))
      .split(",")
      .map((part) => cleanLabel(part).toLowerCase())
      .filter(Boolean)
      .filter((screen) => !screenSet.size || screenSet.has(screen));
    if (!screens.length) return;
    const heading = el.querySelector("h2,h3,.card-header,.drawerTitle,.copilotTitle");
    const label = stripIconText(heading ? heading.textContent : el.id);
    if (!label) return;
    const preferred = screens.includes(String(options.getActiveScreen && options.getActiveScreen() || "").toLowerCase())
      ? String(options.getActiveScreen()).toLowerCase()
      : screens[0];
    items.push({
      id: commandId("panel", el.id),
      title: `Open ${label}`,
      subtitle: `${labels[preferred] || capitalize(preferred)} panel`,
      badge: "Panel",
      keywords: [el.id, label, ...screens],
      priority: 60,
      run: () => options.navigateToPanel && options.navigateToPanel(preferred, el.id),
    });
  });

  return items;
}

function getExposedJobActions(doc) {
  const byJob = new Map();
  doc.querySelectorAll("button[data-job][data-action]").forEach((btn) => {
    const job = cleanLabel(btn.getAttribute("data-job"));
    const action = cleanLabel(btn.getAttribute("data-action")).toLowerCase();
    if (!isSafePaletteJobAction(job, action)) return;
    if (!byJob.has(job)) byJob.set(job, new Set());
    byJob.get(job).add(action);
  });
  return byJob;
}

function buildJobItems(doc, jobs, options) {
  const exposed = getExposedJobActions(doc);
  const items = [];
  for (const row of asArray(jobs)) {
    const name = cleanLabel(row && row.name);
    if (!name) continue;
    const running = !!(row && row.running);
    const group = cleanLabel(row && row.group);
    const mode = cleanLabel(row && row.mode);
    items.push({
      id: commandId("job-select", name),
      title: `Select job ${name}`,
      subtitle: [running ? "running" : "idle", group, mode].filter(Boolean).join(" / "),
      badge: "Job",
      keywords: [name, group, mode, row && row.script],
      priority: 35,
      run: () => options.selectJob && options.selectJob(name),
    });

    const actions = exposed.get(name);
    if (!actions) continue;
    actions.forEach((action) => {
      if (action === "start" && running) return;
      if (action === "stop" && !running) return;
      items.push({
        id: commandId(`job-${action}`, name),
        title: `${capitalize(action)} job ${name}`,
        subtitle: "Uses existing job control endpoint with confirmation",
        badge: "Confirm",
        confirm: true,
        keywords: [name, action, "job", "control"],
        priority: 30,
        run: () => options.runJobAction && options.runJobAction(name, action),
      });
    });
  }
  return items;
}

function buildSymbolItems(symbols, options) {
  const seen = new Set();
  const items = [];
  for (const raw of asArray(symbols)) {
    const symbol = cleanLabel(typeof raw === "string" ? raw : (raw && (raw.symbol || raw.ticker || raw.name))).toUpperCase();
    if (!symbol || seen.has(symbol)) continue;
    seen.add(symbol);
    items.push({
      id: commandId("symbol", symbol),
      title: `Focus symbol ${symbol}`,
      subtitle: "Applies the dashboard symbol filter",
      badge: "Symbol",
      keywords: [symbol, "ticker", "watchlist"],
      priority: 45,
      run: () => options.focusSymbol && options.focusSymbol(symbol),
    });
  }
  return items;
}

function buildModelItems(rows, options) {
  const seen = new Set();
  const items = [];
  for (const row of asArray(rows)) {
    const stage = cleanLabel(row && row.stage);
    const modelName = cleanLabel(row && (row.model_name || row.name || row.id));
    const kind = cleanLabel(row && (row.model_kind || row.kind || row.family));
    const label = cleanLabel([stage, modelName || kind].filter(Boolean).join(" "));
    if (!label) continue;
    const id = commandId("model", `${stage}:${modelName}:${kind}`);
    if (seen.has(id)) continue;
    seen.add(id);
    items.push({
      id,
      title: `Focus model ${label}`,
      subtitle: [kind, stage].filter(Boolean).join(" / ") || "Model registry row",
      badge: "Model",
      keywords: [stage, modelName, kind, "registry", "champion", "challenger"],
      priority: 40,
      run: () => options.focusModel && options.focusModel({ row, stage, modelName, kind, label }),
    });
  }
  return items;
}

function buildDecisionQueryItems(query, options) {
  const decisionId = parseDecisionIdQuery(query);
  if (!decisionId) return [];
  return [{
    id: commandId("decision", decisionId),
    title: `Open decision ${decisionId}`,
    subtitle: "Decision drill-down",
    badge: "Decision",
    keywords: [decisionId, "decision", "drilldown", "trace"],
    priority: 120,
    run: () => options.openDecision && options.openDecision(decisionId),
  }];
}

export function initCommandPalette(options = {}) {
  const doc = options.document || options.root || (typeof document !== "undefined" ? document : null);
  if (!doc || !doc.body) return null;
  if (doc.__dashboardCommandPalette) return doc.__dashboardCommandPalette;

  ensureStyles(doc);
  const root = createShell(doc);
  const input = root.querySelector("#dashboardCommandPaletteInput");
  const closeBtn = root.querySelector("#dashboardCommandPaletteClose");
  const status = root.querySelector("#dashboardCommandPaletteStatus");
  const list = root.querySelector("#dashboardCommandPaletteList");
  const fetchJSON = typeof options.fetchJSON === "function" ? options.fetchJSON : defaultFetchJSON;

  const state = {
    open: false,
    activeIndex: 0,
    results: [],
    staticItems: [],
    dynamicItems: [],
    loading: false,
    errors: [],
    loadedAt: 0,
    loadToken: 0,
  };

  function setStatus(text, isError = false) {
    status.textContent = text || "";
    status.classList.toggle("is-error", !!isError);
  }

  function renderStateRow(text, isError = false) {
    list.innerHTML = "";
    const row = doc.createElement("div");
    row.className = `commandPaletteState${isError ? " is-error" : ""}`;
    row.textContent = text;
    list.appendChild(row);
  }

  function render() {
    const query = input.value || "";
    const queryItems = buildDecisionQueryItems(query, options);
    const items = [...queryItems, ...state.staticItems, ...state.dynamicItems];
    const results = filterCommandItems(items, query, { limit: options.limit || DEFAULT_LIMIT });
    state.results = results;
    if (state.activeIndex >= results.length) state.activeIndex = Math.max(0, results.length - 1);

    if (state.loading) {
      setStatus("Loading dynamic commands...");
    } else if (state.errors.length) {
      setStatus(`Some command sources are unavailable: ${state.errors.join(", ")}`, true);
    } else {
      setStatus(results.length ? `${results.length} command${results.length === 1 ? "" : "s"}` : "");
    }

    if (!results.length) {
      if (state.loading) {
        renderStateRow("Loading commands...");
      } else if (state.errors.length && !query.trim()) {
        renderStateRow(`Command sources unavailable: ${state.errors.join(", ")}`, true);
      } else {
        renderStateRow(query.trim() ? "No commands match." : "No commands available.");
      }
      return;
    }

    list.innerHTML = "";
    results.forEach((item, index) => {
      const button = doc.createElement("button");
      button.type = "button";
      button.className = `commandPaletteItem${index === state.activeIndex ? " is-active" : ""}`;
      button.setAttribute("role", "option");
      button.setAttribute("aria-selected", index === state.activeIndex ? "true" : "false");
      button.dataset.index = String(index);
      const badgeClass = item.confirm ? "commandPaletteBadge is-confirm" : "commandPaletteBadge";
      button.innerHTML = `
        <span>
          <span class="commandPaletteItemTitle"></span>
          <span class="commandPaletteItemSubtitle"></span>
        </span>
        <span class="${badgeClass}"></span>
      `;
      button.querySelector(".commandPaletteItemTitle").textContent = item.title || "";
      button.querySelector(".commandPaletteItemSubtitle").textContent = item.subtitle || "";
      button.querySelector(".commandPaletteBadge").textContent = item.badge || "";
      button.addEventListener("mouseenter", () => {
        state.activeIndex = index;
        render();
      });
      button.addEventListener("click", () => {
        state.activeIndex = index;
        void executeActive();
      });
      list.appendChild(button);
    });
  }

  async function loadDynamic(force = false) {
    const now = Date.now();
    if (!force && state.loadedAt && now - state.loadedAt < DATA_TTL_MS) return;
    const token = state.loadToken + 1;
    state.loadToken = token;
    state.loading = true;
    state.errors = [];
    render();

    const nextItems = [];
    const errors = [];
    const [jobsResult, symbolsResult, modelsResult] = await Promise.allSettled([
      fetchJSON("/api/jobs"),
      fetchJSON("/api/terminal/watchlist"),
      fetchJSON("/api/model/registry?limit=25"),
    ]);
    if (token !== state.loadToken) return;

    if (jobsResult.status === "fulfilled") {
      nextItems.push(...buildJobItems(doc, normalizeJobRows(jobsResult.value), options));
    } else {
      errors.push("jobs");
    }
    if (symbolsResult.status === "fulfilled") {
      nextItems.push(...buildSymbolItems(normalizeSymbolsPayload(symbolsResult.value), options));
    } else {
      errors.push("symbols");
    }
    if (modelsResult.status === "fulfilled") {
      nextItems.push(...buildModelItems(normalizeModelRows(modelsResult.value), options));
    } else {
      errors.push("models");
    }

    state.dynamicItems = nextItems;
    state.errors = errors;
    state.loading = false;
    state.loadedAt = Date.now();
    render();
  }

  function open() {
    state.staticItems = buildStaticItems(doc, options);
    state.activeIndex = 0;
    state.open = true;
    root.classList.add("is-open");
    root.removeAttribute("hidden");
    root.setAttribute("aria-hidden", "false");
    input.value = "";
    render();
    setTimeout(() => {
      input.focus();
      input.select();
    }, 0);
    void loadDynamic(false);
  }

  function close() {
    state.open = false;
    root.classList.remove("is-open");
    root.setAttribute("hidden", "");
    root.setAttribute("aria-hidden", "true");
  }

  function toggle() {
    if (state.open) close();
    else open();
  }

  async function executeActive() {
    const item = state.results[state.activeIndex];
    if (!item || typeof item.run !== "function") return;
    close();
    try {
      await item.run();
    } catch (error) {
      if (typeof options.toast === "function") {
        options.toast(`Command failed: ${error && error.message ? error.message : error}`, "warn", 3600);
      }
      console.error("command palette action failed", error);
    }
  }

  input.addEventListener("input", () => {
    state.activeIndex = 0;
    render();
  });
  input.addEventListener("keydown", (event) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      state.activeIndex = Math.min(state.results.length - 1, state.activeIndex + 1);
      render();
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      state.activeIndex = Math.max(0, state.activeIndex - 1);
      render();
    } else if (event.key === "Enter") {
      event.preventDefault();
      void executeActive();
    } else if (event.key === "Escape") {
      event.preventDefault();
      close();
    }
  });

  closeBtn.addEventListener("click", close);
  root.addEventListener("click", (event) => {
    if (event.target === root) close();
  });

  doc.addEventListener("keydown", (event) => {
    if (!event || event.defaultPrevented) return;
    const key = String(event.key || "").toLowerCase();
    if ((event.metaKey || event.ctrlKey) && !event.shiftKey && key === "k") {
      event.preventDefault();
      toggle();
      return;
    }
    if (state.open && key === "escape") {
      event.preventDefault();
      close();
    }
  });

  const api = {
    open,
    close,
    toggle,
    refresh: () => loadDynamic(true),
  };
  doc.__dashboardCommandPalette = api;
  if (typeof window !== "undefined") {
    window.__dashboardCommandPalette = api;
  }
  return api;
}
