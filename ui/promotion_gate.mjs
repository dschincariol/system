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

function finiteNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clampPct(value) {
  const n = finiteNumber(value, 0) || 0;
  return Math.max(0, Math.min(100, n));
}

function thresholdPayload(metric = {}) {
  const source = metric && typeof metric === "object" ? metric : {};
  const thresholdSource = source.gate_threshold && typeof source.gate_threshold === "object"
    ? source.gate_threshold
    : source.threshold && typeof source.threshold === "object"
      ? source.threshold
      : {};
  const rawValue =
    source.threshold_value ??
    source.gate_threshold_value ??
    thresholdSource.value ??
    source.threshold;
  const value = finiteNumber(rawValue);
  if (value == null) return null;
  const direction = String(source.direction || "").toLowerCase() === "lower" ? "lower" : "higher";
  const operator = String(
    source.threshold_operator ||
    thresholdSource.operator ||
    (direction === "lower" ? "<=" : ">=")
  );
  return {
    value,
    operator,
    source: String(source.threshold_source || thresholdSource.source || "gate"),
  };
}

function metricTimestamp(metric = {}, gate = {}) {
  const source = metric && typeof metric === "object" ? metric : {};
  const direct = finiteNumber(source.updated_ts_ms ?? source.ts_ms);
  if (direct != null && direct > 0) return direct;
  const championTs = finiteNumber(gate.champion && (gate.champion.updated_ts_ms || gate.champion.model_ts_ms));
  const challengerTs = finiteNumber(gate.challenger && (gate.challenger.updated_ts_ms || gate.challenger.model_ts_ms));
  const candidates = [championTs, challengerTs].filter((v) => v != null && v > 0);
  return candidates.length ? Math.min(...candidates) : null;
}

function significanceLabel(metric = {}, alpha = 0.05) {
  const source = metric && typeof metric === "object" ? metric : {};
  if (source.significant === true) return { state: "pass", label: "significant" };
  if (source.significant === false) return { state: "warn", label: "not significant" };
  const q = finiteNumber(source.q_value ?? source.q);
  if (q != null) return q <= alpha ? { state: "pass", label: `q ${q.toFixed(3)}` } : { state: "warn", label: `q ${q.toFixed(3)}` };
  const p = finiteNumber(source.p_value ?? source.p);
  if (p != null) return p <= alpha ? { state: "pass", label: `p ${p.toFixed(3)}` } : { state: "warn", label: `p ${p.toFixed(3)}` };
  return { state: "dim", label: "significance not tested" };
}

function thresholdPassed(value, threshold, direction) {
  if (value == null || !threshold) return null;
  return String(direction || "").toLowerCase() === "lower"
    ? value <= threshold.value
    : value >= threshold.value;
}

function metricDecision({ champion, challenger, direction, threshold, significance }) {
  if (champion == null && challenger == null) {
    return { state: "unavailable", label: "metric unavailable" };
  }
  if (challenger == null) {
    return { state: "unavailable", label: "challenger unavailable" };
  }

  const thresholdOk = thresholdPassed(challenger, threshold, direction);
  if (thresholdOk === false) {
    return { state: "fail", label: "challenger misses gate" };
  }

  if (champion == null) {
    return thresholdOk === true
      ? { state: "pass", label: "challenger passes gate" }
      : { state: "unknown", label: "no champion context" };
  }

  const improvement = String(direction || "").toLowerCase() === "lower"
    ? champion - challenger
    : challenger - champion;
  if (improvement > 0) {
    if (significance && significance.state === "warn") return { state: "warn", label: "challenger leads; significance weak" };
    return { state: "pass", label: "challenger leads" };
  }
  if (Math.abs(improvement) <= 1e-12) return { state: "warn", label: "tied with champion" };
  return { state: "fail", label: "champion leads" };
}

export function buildPromotionComparisonBarViewModel(payload = {}, options = {}) {
  const gate = normalizePromotionGatePayload(payload);
  const metrics = Array.isArray(gate.comparisonMetrics) ? gate.comparisonMetrics : [];
  const nowMs = finiteNumber(options.nowMs, Date.now()) || Date.now();
  const staleAfterMs = Math.max(1, finiteNumber(options.staleAfterMs, 6 * 60 * 60 * 1000) || (6 * 60 * 60 * 1000));
  const alpha = Math.max(0, finiteNumber(options.alpha, 0.05) || 0.05);

  const bars = metrics.map((metric) => {
    const champion = finiteNumber(metric && metric.champion);
    const challenger = finiteNumber(metric && metric.challenger);
    const threshold = thresholdPayload(metric);
    const direction = String(metric && metric.direction || "higher").toLowerCase() === "lower" ? "lower" : "higher";
    const domainMax = Math.max(
      1e-9,
      Math.abs(champion ?? 0),
      Math.abs(challenger ?? 0),
      Math.abs(threshold ? threshold.value : 0)
    );
    const significance = significanceLabel(metric, alpha);
    const decision = metricDecision({ champion, challenger, direction, threshold, significance });
    const ts = metricTimestamp(metric, gate);
    const stale = !!(metric && metric.stale) || (ts != null && nowMs - ts > staleAfterMs);

    return {
      key: String(metric && (metric.key || metric.label) || "metric"),
      label: String(metric && (metric.label || metric.key) || "Metric"),
      direction,
      champion,
      challenger,
      delta: finiteNumber(metric && metric.delta, champion != null && challenger != null ? challenger - champion : null),
      championPct: champion == null ? 0 : clampPct((Math.abs(champion) / domainMax) * 100),
      challengerPct: challenger == null ? 0 : clampPct((Math.abs(challenger) / domainMax) * 100),
      thresholdPct: threshold ? clampPct((Math.abs(threshold.value) / domainMax) * 100) : null,
      threshold,
      thresholdLabel: threshold
        ? `Gate ${threshold.operator} ${formatPromotionGateValue(threshold.value)}`
        : "Gate threshold unavailable",
      significance,
      decision,
      stale,
      staleLabel: stale ? "stale data" : "fresh data",
      valueLabel: `${formatPromotionGateValue(champion)} vs ${formatPromotionGateValue(challenger)}`,
    };
  });

  const blocked = gate.status.enabled === false || gate.status.allowed === false;
  const passed = gate.status.allowed === true;
  const summaryState = passed ? "pass" : blocked ? "fail" : "unknown";
  const summaryLabel = passed
    ? "Promotion allowed"
    : gate.status.enabled === false
      ? "Promotion off"
      : gate.status.allowed === false
        ? "Promotion blocked"
        : "Promotion state unknown";

  return {
    ok: gate.ok,
    modelName: gate.modelName,
    regime: gate.regime,
    summaryState,
    summaryLabel,
    bars,
    staleCount: bars.filter((bar) => bar.stale).length,
    unavailableCount: bars.filter((bar) => bar.decision.state === "unavailable").length,
    raw: gate.raw,
  };
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

const GOVERNANCE_EVIDENCE_STATE_LABELS = Object.freeze({
  pass: "Pass",
  block: "Block",
  unknown: "Unknown",
});

const GOVERNANCE_EVIDENCE_STATE_TONES = Object.freeze({
  pass: "ok",
  block: "crit",
  unknown: "dim",
});

function governanceEvidenceStateKey(value) {
  const key = String(value || "").trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(GOVERNANCE_EVIDENCE_STATE_LABELS, key) ? key : "unknown";
}

function governanceFiniteNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function governanceFormatCount(value) {
  const n = governanceFiniteNumber(value, 0) || 0;
  return String(Math.max(0, Math.trunc(n)));
}

function governanceFormatTime(value) {
  const ts = governanceFiniteNumber(value, 0) || 0;
  if (ts <= 0) return "not recorded";
  try {
    return new Date(ts).toLocaleString();
  } catch (_e) {
    return String(Math.trunc(ts));
  }
}

function governanceFormatScore(value) {
  const n = governanceFiniteNumber(value);
  if (n == null) return "not available";
  return Math.abs(n) >= 100 ? n.toFixed(2) : Math.abs(n) >= 1 ? n.toFixed(3) : n.toFixed(4);
}

function governanceEvidencePill(state) {
  const key = governanceEvidenceStateKey(state);
  return `<span class="pill ${escapeHtml(GOVERNANCE_EVIDENCE_STATE_TONES[key])}">${escapeHtml(GOVERNANCE_EVIDENCE_STATE_LABELS[key])}</span>`;
}

function governanceEvidenceRows(payload = {}) {
  return Array.isArray(payload.evidence) ? payload.evidence : [];
}

function governanceEvidenceBlockerRows(payload = {}) {
  const blockers = Array.isArray(payload.blockers) ? payload.blockers : [];
  if (blockers.length) return blockers;
  const promotion = payload.promotion_blockers && typeof payload.promotion_blockers === "object"
    ? payload.promotion_blockers
    : {};
  return Array.isArray(promotion.evidence_blockers) ? promotion.evidence_blockers : [];
}

export function summarizeGovernanceEvidence(payload = {}) {
  const rows = governanceEvidenceRows(payload);
  const blockers = governanceEvidenceBlockerRows(payload);
  const unknowns = Array.isArray(payload.unknowns) ? payload.unknowns : [];
  const state = governanceEvidenceStateKey(payload.state || (blockers.length ? "block" : rows.length ? "pass" : "unknown"));
  const latest = rows.reduce(
    (maxTs, row) => Math.max(maxTs, governanceFiniteNumber(row && row.last_update_ts_ms, 0) || 0),
    0
  );
  return {
    state,
    label: GOVERNANCE_EVIDENCE_STATE_LABELS[state],
    evidenceCount: rows.length,
    blockerCount: blockers.length,
    unknownCount: unknowns.length,
    latestTsMs: latest,
    meta: `${rows.length} sources | ${blockers.length} blockers | ${unknowns.length} unknown`,
  };
}

function renderGovernanceEvidenceTable(rows) {
  if (!rows.length) {
    return `<div class="metric-meta">No governance evidence rows returned.</div>`;
  }
  return `
    <div class="table-wrap governanceEvidenceTableWrap">
      <table class="governanceEvidenceTable">
        <thead>
          <tr>
            <th>Evidence</th>
            <th>State</th>
            <th>Freshness</th>
            <th>Samples</th>
            <th>Last update</th>
            <th>Source</th>
            <th>Remediation</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr class="table-row" data-evidence-state="${escapeHtml(governanceEvidenceStateKey(row && row.state))}">
              <td>
                <div class="metric-title">${escapeHtml(row && (row.label || row.key) || "Evidence")}</div>
                <div class="metric-meta mono">${escapeHtml(row && row.key || "")}</div>
              </td>
              <td>${governanceEvidencePill(row && row.state)}</td>
              <td>${escapeHtml(row && row.freshness || "unknown")}</td>
              <td class="mono">${escapeHtml(governanceFormatCount(row && row.sample_count))}</td>
              <td>${escapeHtml(governanceFormatTime(row && row.last_update_ts_ms))}</td>
              <td class="mono">${escapeHtml(row && row.source_artifact || "")}</td>
              <td>${escapeHtml(row && row.remediation || "")}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderGovernanceEvidenceBlockers(blockers) {
  const rows = Array.isArray(blockers) ? blockers.slice(0, 8) : [];
  if (!rows.length) {
    return `<div class="metric-meta">No active governance evidence blockers.</div>`;
  }
  return `
    <div class="governanceEvidenceBlockers">
      ${rows.map((row) => `
        <div class="governanceEvidenceBlocker">
          <div class="metric-title">${escapeHtml(row && (row.label || row.key) || "Blocker")}</div>
          <div class="metric-meta mono">${escapeHtml(row && row.source_artifact || "")}</div>
          <div class="metric-meta">${escapeHtml(row && row.remediation || "")}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function renderGovernanceShadowCapital(payload = {}) {
  const shadow = payload.shadow_capital && typeof payload.shadow_capital === "object" ? payload.shadow_capital : {};
  const rows = Array.isArray(shadow.rows) ? shadow.rows.slice(0, 5) : [];
  const evidence = shadow.evidence && typeof shadow.evidence === "object" ? shadow.evidence : {};
  if (!rows.length) {
    return `
      <div class="structuredSummaryRow">
        <div class="structuredSummaryLabel">Shadow capital</div>
        <div class="structuredSummaryValue">${governanceEvidencePill(evidence.state || "block")}</div>
        <div class="structuredSummaryMeta">${escapeHtml(evidence.remediation || "No shadow-capital rows returned.")}</div>
      </div>
    `;
  }
  return `
    <div class="structuredSummaryRow">
      <div class="structuredSummaryLabel">Shadow capital</div>
      <div class="structuredSummaryValue">${governanceEvidencePill(evidence.state || "unknown")}</div>
      <div class="structuredSummaryMeta">Top score ${escapeHtml(governanceFormatScore(rows[0] && rows[0].score))} from ${escapeHtml(rows[0] && rows[0].model_name || "model")}</div>
    </div>
    <div class="table-wrap governanceShadowTableWrap">
      <table class="governanceShadowTable">
        <thead>
          <tr>
            <th>Model</th>
            <th>Score</th>
            <th>Samples</th>
            <th>Total PnL</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((row) => `
            <tr class="table-row">
              <td class="mono">${escapeHtml(row && row.model_name || "")}</td>
              <td class="mono">${escapeHtml(governanceFormatScore(row && row.score))}</td>
              <td class="mono">${escapeHtml(governanceFormatCount(row && row.n))}</td>
              <td class="mono">${escapeHtml(governanceFormatScore(row && row.total_pnl))}</td>
              <td>${escapeHtml(governanceFormatTime(row && row.ts_ms))}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>
  `;
}

export function renderGovernanceEvidenceCenter(target, payload = {}) {
  if (!target) return summarizeGovernanceEvidence(payload);
  const summary = summarizeGovernanceEvidence(payload);
  const rows = governanceEvidenceRows(payload);
  const blockers = governanceEvidenceBlockerRows(payload);
  const authority = payload.authority && typeof payload.authority === "object" ? payload.authority : {};
  target.innerHTML = `
    <div class="structuredSummaryGrid governanceEvidenceSummary">
      <div class="structuredSummaryRow">
        <div class="structuredSummaryLabel">Evidence state</div>
        <div class="structuredSummaryValue">${governanceEvidencePill(summary.state)}</div>
        <div class="structuredSummaryMeta">${escapeHtml(summary.meta)}</div>
      </div>
      <div class="structuredSummaryRow">
        <div class="structuredSummaryLabel">Last update</div>
        <div class="structuredSummaryValue">${escapeHtml(governanceFormatTime(summary.latestTsMs))}</div>
        <div class="structuredSummaryMeta">${escapeHtml(authority.mode || "read_only_governance_evidence")}</div>
      </div>
      ${renderGovernanceShadowCapital(payload)}
    </div>
    <div class="small table-section-title">Evidence blockers</div>
    ${renderGovernanceEvidenceBlockers(blockers)}
    <div class="small table-section-title">Evidence sources</div>
    ${renderGovernanceEvidenceTable(rows)}
  `;
  return summary;
}
