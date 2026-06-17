/*
  FILE: ui/decision_stepper.js

  DOM renderer for the automated-decision flow inside the decision modal.
  The server payload is normalized through buildDecisionStageRows() first, so
  this visual stays tied to the same production drill-down data as the text
  summary and raw related-record view.
*/

import { buildDecisionStageRows } from "./decision_drilldown.mjs";
import { escapeHTML, statusToken } from "./utils.js";

export const DECISION_FLOW_STAGES = Object.freeze([
  Object.freeze({
    key: "signal",
    label: "Signal",
    sourceKeys: Object.freeze(["source", "signal", "alert"]),
    labelHints: Object.freeze(["source signal", "signal", "alert"]),
  }),
  Object.freeze({
    key: "confidence",
    label: "Confidence/Calibration",
    sourceKeys: Object.freeze(["model", "confidence", "calibration"]),
    labelHints: Object.freeze(["model decision", "confidence", "calibration"]),
  }),
  Object.freeze({
    key: "risk",
    label: "Risk/Suppression",
    sourceKeys: Object.freeze(["policy", "risk", "suppression"]),
    labelHints: Object.freeze(["risk and policy checks", "risk", "suppression", "policy"]),
  }),
  Object.freeze({
    key: "sizing",
    label: "Sizing",
    sourceKeys: Object.freeze(["portfolio", "sizing", "size"]),
    labelHints: Object.freeze(["portfolio intent", "sizing", "size"]),
  }),
  Object.freeze({
    key: "order",
    label: "Order",
    sourceKeys: Object.freeze(["route", "order", "execution_order"]),
    labelHints: Object.freeze(["route", "order"]),
  }),
  Object.freeze({
    key: "fill",
    label: "Fill",
    sourceKeys: Object.freeze(["outcome", "fill", "execution_fill"]),
    labelHints: Object.freeze(["outcome", "fill"]),
  }),
]);

const BLOCKING_STATUSES = new Set(["blocked", "suppressed", "unavailable"]);
const PARTIAL_STATUSES = new Set(["partial"]);
const LOADING_STATUSES = new Set(["loading"]);

function normalizeToken(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, "_");
}

function normalizeLabel(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
}

function stageStatus(row) {
  return normalizeToken(row && (row.status || row.value)) || "unavailable";
}

function statusDisplay(status, rowValue = "") {
  const raw = String(rowValue || status || "unavailable").trim().replace(/_/g, " ");
  if (status === "available") return "Passed";
  if (status === "executed") return "Filled";
  if (status === "partial") return "Partial";
  if (status === "suppressed") return "Suppressed";
  if (status === "blocked") return "Blocked";
  if (status === "loading") return "Loading";
  if (status === "unavailable") return "Unavailable";
  return raw || "Unavailable";
}

function iconForStatus(status) {
  if (status === "available" || status === "executed") return "OK";
  if (status === "blocked") return "X";
  if (status === "suppressed" || status === "partial") return "!";
  if (status === "loading") return "...";
  return "?";
}

function toneForStatus(status, fallbackTone = "") {
  if (status === "available" || status === "executed") return "ok";
  if (status === "blocked") return "crit";
  if (status === "suppressed" || status === "partial") return "warn";
  if (status === "unavailable") return "unavailable";
  if (status === "loading") return "neutral";
  return statusToken(fallbackTone || "neutral").key;
}

function rowMatchesDefinition(row, definition) {
  const key = normalizeToken(row && row.key);
  if (key && definition.sourceKeys.includes(key)) return true;

  const label = normalizeLabel(row && row.label);
  if (!label) return false;
  return definition.labelHints.some((hint) => label === hint || label.includes(hint));
}

function findRowForDefinition(rows, definition, usedIndexes) {
  const keyedIndex = rows.findIndex((row, index) => (
    !usedIndexes.has(index)
    && normalizeToken(row && row.key)
    && definition.sourceKeys.includes(normalizeToken(row && row.key))
  ));
  if (keyedIndex >= 0) {
    usedIndexes.add(keyedIndex);
    return rows[keyedIndex];
  }

  const labelIndex = rows.findIndex((row, index) => (
    !usedIndexes.has(index) && rowMatchesDefinition(row, definition)
  ));
  if (labelIndex >= 0) {
    usedIndexes.add(labelIndex);
    return rows[labelIndex];
  }

  return null;
}

function normalizeSourceRows(payload) {
  return buildDecisionStageRows(payload || {}).map((row) => ({
    ...row,
    key: normalizeToken(row && row.key),
    status: stageStatus(row),
    reason: String((row && (row.reason || row.meta)) || "Stage detail unavailable.").trim(),
    timestamp: String((row && row.timestamp) || "").trim(),
  }));
}

function buildStage(definition, row, { loading = false } = {}) {
  const status = row ? stageStatus(row) : (loading ? "loading" : "unavailable");
  const reason = row
    ? String(row.reason || row.meta || "Stage detail unavailable.").trim()
    : (loading
      ? "Waiting for the decision detail payload from the server."
      : `No ${definition.label} stage was returned by the decision detail payload.`);
  const tone = toneForStatus(status, row && row.tone);
  const token = statusToken(tone);

  return {
    key: definition.key,
    label: definition.label,
    sourceLabel: row && row.label ? String(row.label) : "",
    status,
    statusText: statusDisplay(status, row && row.value),
    tone: token.key,
    icon: iconForStatus(status),
    reason,
    timestamp: row && row.timestamp ? String(row.timestamp) : "",
    timestampMs: row && row.timestampMs ? Number(row.timestampMs) : null,
    isBlocking: BLOCKING_STATUSES.has(status),
    isPartial: PARTIAL_STATUSES.has(status),
    isLoading: LOADING_STATUSES.has(status),
    isSynthetic: !row,
  };
}

function assignRowsToFlow(payload, rows) {
  const firstRow = rows[0] || null;
  const isLoadingPayload = !!(payload && payload.loading)
    || (rows.length === 1 && stageStatus(firstRow) === "loading");
  const hasServerStages = Array.isArray(payload && payload.stages) && payload.stages.length > 0;

  if (isLoadingPayload) {
    return DECISION_FLOW_STAGES.map((definition) => buildStage(definition, firstRow, { loading: true }));
  }

  const usedIndexes = new Set();
  const stages = DECISION_FLOW_STAGES.map((definition) => {
    const row = hasServerStages ? findRowForDefinition(rows, definition, usedIndexes) : null;
    return buildStage(definition, row);
  });

  if (!hasServerStages && firstRow) {
    stages[0] = buildStage(DECISION_FLOW_STAGES[0], firstRow);
  }

  return stages;
}

function summaryForStages(stages) {
  const blockingStage = stages.find((stage) => stage.isBlocking) || null;
  const partialStage = stages.find((stage) => stage.isPartial) || null;
  const loadingStage = stages.find((stage) => stage.isLoading) || null;
  const emphasizedStage = blockingStage || partialStage || loadingStage || null;

  if (blockingStage) {
    return {
      stage: emphasizedStage,
      tone: blockingStage.tone,
      icon: blockingStage.icon,
      text: `${blockingStage.label} ${blockingStage.statusText.toLowerCase()}: ${blockingStage.reason}`,
    };
  }
  if (partialStage) {
    return {
      stage: emphasizedStage,
      tone: partialStage.tone,
      icon: partialStage.icon,
      text: `${partialStage.label} has partial detail: ${partialStage.reason}`,
    };
  }
  if (loadingStage) {
    return {
      stage: emphasizedStage,
      tone: "neutral",
      icon: loadingStage.icon,
      text: "Decision flow is loading from the server.",
    };
  }
  return {
    stage: null,
    tone: "ok",
    icon: "OK",
    text: "Decision path reached Fill without a blocking stage.",
  };
}

export function buildDecisionStepperModel(payload = {}) {
  const sourceRows = normalizeSourceRows(payload);
  const stages = assignRowsToFlow(payload, sourceRows);
  const summary = summaryForStages(stages);

  return {
    flowLabel: DECISION_FLOW_STAGES.map((stage) => stage.label).join(" -> "),
    stages: stages.map((stage) => ({
      ...stage,
      isEmphasized: !!(summary.stage && summary.stage.key === stage.key),
    })),
    summary: {
      tone: statusToken(summary.tone).key,
      icon: summary.icon,
      text: summary.text,
      stageKey: summary.stage ? summary.stage.key : "",
    },
  };
}

function stageAriaLabel(stage, index, total) {
  const timestamp = stage.timestamp ? ` Timestamp ${stage.timestamp}.` : " Timestamp unavailable.";
  const source = stage.sourceLabel && stage.sourceLabel !== stage.label
    ? ` Server stage ${stage.sourceLabel}.`
    : "";
  return `Stage ${index + 1} of ${total}, ${stage.label}, ${stage.statusText}. ${stage.reason}.${timestamp}${source}`;
}

function renderStage(stage, index, total) {
  const timestamp = stage.timestamp || "Timestamp unavailable";
  const classes = [
    "decisionStepperItem",
    `is-${stage.tone}`,
    `is-status-${stage.status}`,
    stage.isBlocking ? "is-blocking" : "",
    stage.isPartial ? "is-partial" : "",
    stage.isLoading ? "is-loading" : "",
    stage.isEmphasized ? "is-emphasized" : "",
  ].filter(Boolean).join(" ");
  const detailsAttrs = [
    'class="decisionStep"',
    stage.isEmphasized ? "open" : "",
    stage.isEmphasized ? 'aria-current="step"' : "",
  ].filter(Boolean).join(" ");

  return `
    <li class="${escapeHTML(classes)}" data-stage-key="${escapeHTML(stage.key)}" data-stage-status="${escapeHTML(stage.status)}">
      <details ${detailsAttrs}>
        <summary class="decisionStepSummary" aria-label="${escapeHTML(stageAriaLabel(stage, index, total))}">
          <span class="decisionStepIcon" aria-hidden="true">${escapeHTML(stage.icon)}</span>
          <span class="decisionStepText">
            <span class="decisionStepLabel">${escapeHTML(stage.label)}</span>
            <span class="decisionStepStatus">${escapeHTML(stage.statusText)}</span>
          </span>
        </summary>
        <div class="decisionStepDetail">
          <div class="decisionStepReason">${escapeHTML(stage.reason)}</div>
          <div class="decisionStepTimestamp">${escapeHTML(timestamp)}</div>
        </div>
      </details>
    </li>
  `;
}

export function renderDecisionStepperHtml(payload = {}, { summaryId = "decisionStepperStatus" } = {}) {
  const model = buildDecisionStepperModel(payload);
  const total = model.stages.length;
  return `
    <div class="decisionStepperWrap" role="group" aria-label="Automated decision flow" aria-describedby="${escapeHTML(summaryId)}">
      <div id="${escapeHTML(summaryId)}" class="decisionStepperSummary is-${escapeHTML(model.summary.tone)}" role="status" aria-live="polite">
        <span class="decisionStepperSummaryIcon" aria-hidden="true">${escapeHTML(model.summary.icon)}</span>
        <span>${escapeHTML(model.summary.text)}</span>
      </div>
      <ol class="decisionStepper" role="list" aria-label="${escapeHTML(model.flowLabel)}">
        ${model.stages.map((stage, index) => renderStage(stage, index, total)).join("")}
      </ol>
    </div>
  `;
}

export function renderDecisionStepper(target, payload = {}, options = {}) {
  const el = typeof target === "string" ? document.getElementById(target) : target;
  if (!el) return null;
  el.innerHTML = renderDecisionStepperHtml(payload, options);
  return buildDecisionStepperModel(payload);
}
