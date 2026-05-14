const PERSONA_KEYS = [
  "dashboard.persona",
  "ui.persona",
  "persona",
  "alert_persona",
];

const PERSONA_LABELS = {
  operations: "Operations",
  fund_manager: "Fund Manager",
  expert: "Expert / All",
};

const DEFAULT_PERSONA = "operations";

const PERSONA_SCREEN_ALLOWLISTS = {
  operations: ["overview", "operate", "data", "execution"],
  fund_manager: ["overview", "explain", "analyze", "positions"],
  expert: ["overview", "operate", "explain", "analyze", "data", "positions", "execution"],
};

const PERSONA_PANEL_ALLOWLISTS = {
  operations: {
    overview: [
      "telemetryStrip",
      "operatorSummaryCard",
      "decisionBar",
      "livePnlCard",
      "systemHealthCard",
      "marketStressPanel",
      "notificationStatusCard",
      "alertsCard",
      "executionAdvisoryCard",
    ],
    operate: [
      "operatorStartupCard",
      "decisionBar",
      "jobConsoleCard",
      "logViewerCard",
      "systemHealthCard",
      "trainingStatusCard",
      "notificationStatusCard",
      "promotionsSafetyCard",
      "brokerPanel",
      "executionCostCard",
      "executionAdvisoryCard",
      "systemStateCard",
    ],
    data: [
      "dataHealthSummaryCard",
      "dataProviderTelemetryCard",
      "dataRuntimeSignalsCard",
    ],
    execution: [
      "execOverlaysPanel",
      "executionCostCard",
      "executionSnapshotCard",
      "executionOrdersCard",
      "executionFillsCard",
      "executionMetricsSummaryCard",
    ],
  },
  fund_manager: {
    overview: [
      "telemetryStrip",
      "operatorSummaryCard",
      "decisionBar",
      "livePnlCard",
      "recentDecisionsCard",
      "marketStressPanel",
      "alertsCard",
    ],
    explain: [
      "decisionBar",
      "recentDecisionsCard",
      "humanAlignmentCard",
      "competitionOpsCard",
      "governanceSummaryCard",
      "promotionGateCard",
      "promotionsSafetyCard",
      "promotionAuditCard",
      "driftExplainerPanel",
      "equityDriftPanel",
      "strategyStatusCard",
      "executionAdvisoryCard",
      "portfolioCard",
      "systemStateCard",
    ],
    analyze: [
      "proChartsCard",
      "promotionGateCard",
      "portfolioBacktestCard",
      "competitionOpsCard",
      "confidenceCalibrationCard",
      "relevanceStatsCard",
      "validationScoresCard",
      "temporalEvalCard",
      "calibrationCurvesCard",
      "driftExplainerPanel",
      "equityDriftPanel",
      "strategyMetricsCard",
      "modelMetricsCard",
      "executionCostCard",
      "telemetryCard",
    ],
    positions: [
      "positionsExposureSummaryCard",
      "positionsTargetsCard",
      "positionsLiveBookCard",
      "positionsDiagnosticsCard",
    ],
  },
  expert: null,
};

let _activePersona = DEFAULT_PERSONA;

export function normalizeDashboardPersona(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
  return PERSONA_LABELS[normalized] ? normalized : DEFAULT_PERSONA;
}

export function getDashboardPersonaLabel(persona) {
  return PERSONA_LABELS[normalizeDashboardPersona(persona)] || PERSONA_LABELS[DEFAULT_PERSONA];
}

export function readDashboardPersona() {
  try {
    for (const key of PERSONA_KEYS) {
      const raw = localStorage.getItem(key);
      const normalized = String(raw || "")
        .trim()
        .toLowerCase()
        .replace(/[\s-]+/g, "_");
      if (PERSONA_LABELS[normalized]) {
        _activePersona = normalized;
        return _activePersona;
      }
    }
  } catch {}
  _activePersona = DEFAULT_PERSONA;
  return _activePersona;
}

export function writeDashboardPersona(persona) {
  _activePersona = normalizeDashboardPersona(persona);
  try {
    for (const key of PERSONA_KEYS) {
      localStorage.setItem(key, _activePersona);
    }
  } catch {}
  return _activePersona;
}

export function getActiveDashboardPersona() {
  return _activePersona || readDashboardPersona();
}

export function getAllowedDashboardScreens(persona = getActiveDashboardPersona()) {
  return PERSONA_SCREEN_ALLOWLISTS[normalizeDashboardPersona(persona)] || PERSONA_SCREEN_ALLOWLISTS[DEFAULT_PERSONA];
}

export function isDashboardScreenAllowed(persona, screen) {
  return getAllowedDashboardScreens(persona).includes(String(screen || "").trim().toLowerCase());
}

export function getDefaultDashboardScreen(persona = getActiveDashboardPersona()) {
  const screens = getAllowedDashboardScreens(persona);
  return screens[0] || "overview";
}

function getAllowedPanelIds(persona, screen) {
  const normalizedPersona = normalizeDashboardPersona(persona);
  if (normalizedPersona === "expert") return null;
  const screenMap = PERSONA_PANEL_ALLOWLISTS[normalizedPersona] || PERSONA_PANEL_ALLOWLISTS[DEFAULT_PERSONA];
  const ids = screenMap?.[String(screen || "").trim().toLowerCase()] || [];
  return new Set(ids);
}

function syncPersonaDatasets(root, persona) {
  const normalizedPersona = normalizeDashboardPersona(persona);
  if (document?.body) {
    document.body.dataset.dashboardPersona = normalizedPersona;
  }
  const page = root?.querySelector?.("#page-dashboard");
  if (page) {
    page.dataset.dashboardPersona = normalizedPersona;
  }
}

function syncPersonaControls(root, persona) {
  const select = root?.querySelector?.("#dashboardPersonaSelect");
  if (select && select.value !== persona) {
    select.value = persona;
  }
  const label = root?.querySelector?.("#dashboardPersonaLabel");
  if (label) {
    label.textContent = getDashboardPersonaLabel(persona);
  }
}

export function applyDashboardPersonaView({ root = document, screen } = {}) {
  const persona = getActiveDashboardPersona();
  const normalizedScreen = String(screen || "").trim().toLowerCase() || getDefaultDashboardScreen(persona);
  const allowedScreens = new Set(getAllowedDashboardScreens(persona));
  const allowedPanels = getAllowedPanelIds(persona, normalizedScreen);

  syncPersonaDatasets(root, persona);
  syncPersonaControls(root, persona);

  root.querySelectorAll("[data-screen-target]").forEach((btn) => {
    const target = String(btn.getAttribute("data-screen-target") || "").trim().toLowerCase();
    btn.classList.toggle("dashboard-persona-hidden", !allowedScreens.has(target));
  });

  root.querySelectorAll("#page-dashboard [id][data-screens]").forEach((el) => {
    const isPanel = el.classList.contains("card") || el.id === "decisionBar" || el.id === "telemetryStrip";
    if (!isPanel) return;
    const hidden = allowedPanels ? !allowedPanels.has(el.id) : false;
    el.classList.toggle("dashboard-persona-hidden", hidden);
  });

  return {
    persona,
    screen: normalizedScreen,
    allowedScreens: Array.from(allowedScreens),
  };
}

export function wireDashboardPersonaControls({ root = document, onChange } = {}) {
  const persona = readDashboardPersona();
  syncPersonaDatasets(root, persona);
  syncPersonaControls(root, persona);

  const select = root?.querySelector?.("#dashboardPersonaSelect");
  if (!select || select._boundDashboardPersonaSelect) return persona;

  select._boundDashboardPersonaSelect = true;
  select.addEventListener("change", () => {
    const nextPersona = writeDashboardPersona(select.value);
    syncPersonaDatasets(root, nextPersona);
    syncPersonaControls(root, nextPersona);
    if (typeof onChange === "function") {
      onChange(nextPersona);
    }
  });
  return persona;
}
