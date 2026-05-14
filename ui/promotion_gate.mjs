const STATE_LABELS = Object.freeze({
  pass: "PASS",
  fail: "FAIL",
  unavailable: "Not available",
  available: "Available",
  unknown: "Unknown",
});

const STATE_TONES = Object.freeze({
  pass: "ok",
  fail: "crit",
  unavailable: "dim",
  available: "ok",
  unknown: "dim",
});

function stateKey(state) {
  const key = String(state || "").trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(STATE_LABELS, key) ? key : "unknown";
}

export function formatGateState(state) {
  return STATE_LABELS[stateKey(state)];
}

export function promotionGateStateTone(state) {
  return STATE_TONES[stateKey(state)] || "dim";
}

export function promotionGateStateClass(state, base = "pill") {
  return `${base} ${promotionGateStateTone(state)}`.trim();
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function renderGateStateBadge(state) {
  return `<span class="${escapeHtml(promotionGateStateClass(state))}">${escapeHtml(formatGateState(state))}</span>`;
}

export function formatPromotionGateValue(value) {
  if (value === null || value === undefined || value === "") return "not available";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return "not available";
    if (Number.isInteger(value)) return String(value);
    const abs = Math.abs(value);
    return abs >= 100 ? value.toFixed(2) : abs >= 1 ? value.toFixed(3) : value.toFixed(4);
  }
  if (typeof value === "boolean") return value ? "true" : "false";
  if (Array.isArray(value)) {
    if (!value.length) return "none";
    return value.map((item) => formatPromotionGateValue(item)).join(", ");
  }
  if (typeof value === "object") {
    const compact = JSON.stringify(value);
    return compact.length > 140 ? `${compact.slice(0, 137)}...` : compact;
  }
  return String(value);
}

export function normalizePromotionGatePayload(payload) {
  const root = payload && typeof payload === "object" ? payload : {};
  const gate = root.gate && typeof root.gate === "object" ? root.gate : root;
  return {
    ok: gate.ok !== false,
    modelName: String(gate.model_name || ""),
    regime: String(gate.regime || "global"),
    status: gate.status && typeof gate.status === "object" ? gate.status : {},
    champion: gate.champion && typeof gate.champion === "object" ? gate.champion : null,
    challenger: gate.challenger && typeof gate.challenger === "object" ? gate.challenger : null,
    rollbackTarget: gate.rollback_target && typeof gate.rollback_target === "object" ? gate.rollback_target : null,
    comparisonMetrics: Array.isArray(gate.comparison_metrics) ? gate.comparison_metrics : [],
    checklist: Array.isArray(gate.checklist) ? gate.checklist : [],
    cooldown: gate.cooldown && typeof gate.cooldown === "object" ? gate.cooldown : {},
    validation: gate.validation && typeof gate.validation === "object" ? gate.validation : {},
    actions: gate.actions && typeof gate.actions === "object" ? gate.actions : {},
    raw: gate,
  };
}

export function modelLabel(model) {
  if (!model || typeof model !== "object") return "not available";
  const kind = String(model.model_kind || model.model_name || "model").trim();
  const ts = Number(model.model_ts_ms || 0);
  return ts > 0 ? `${kind} @ ${ts}` : kind;
}

export function summarizeCooldown(cooldown) {
  const c = cooldown && typeof cooldown === "object" ? cooldown : {};
  if (!c.available) return "not available";
  const remaining = Number(c.remaining_s);
  if (Number.isFinite(remaining) && remaining > 0) {
    return `${Math.ceil(remaining / 60)} min remaining`;
  }
  return String(c.state || "").toLowerCase() === "pass" ? "clear" : formatGateState(c.state);
}

export function buildRollbackConsequencePreview(gatePayload) {
  const gate = normalizePromotionGatePayload(gatePayload);
  const rollback = gate.actions.rollback && typeof gate.actions.rollback === "object"
    ? gate.actions.rollback
    : {};
  const preview = rollback.preview && typeof rollback.preview === "object" ? rollback.preview : {};
  const champion = preview.current_champion || gate.champion;
  const target = preview.rollback_target || gate.rollbackTarget;
  const modelName = String(preview.model_name || gate.modelName || "model");
  const regime = String(preview.regime || gate.regime || "global");
  const consequence = String(preview.consequence || "Current champion will be replaced by the rollback target.");

  return [
    `Rollback ${modelName}/${regime}`,
    `Current champion: ${modelLabel(champion)}`,
    `Rollback target: ${modelLabel(target)}`,
    `Consequence: ${consequence}`,
    "Audit: justification and confirmation will be sent to the existing promotion audit path.",
  ].join("\n");
}

export function validatePromotionActionInput({ justification, minLength = 12 } = {}) {
  const text = String(justification || "").trim();
  if (text.length < Number(minLength || 12)) {
    return {
      ok: false,
      error: "justification_required",
      minLength: Number(minLength || 12),
    };
  }
  return { ok: true, justification: text };
}

export function buildPromotionActionPayload({
  action = "rollback",
  justification,
  confirm,
  preview = {},
  source = "dashboard",
} = {}) {
  return {
    action: String(action || "rollback"),
    confirm: String(confirm || ""),
    justification: String(justification || "").trim(),
    source: String(source || "dashboard"),
    preview: preview && typeof preview === "object" ? preview : {},
  };
}
