"use strict";

const MODAL_ID = "sharedConfirmationModal";
const STYLE_ID = "sharedConfirmationModalStyle";

function escapeHTML(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function ensureStyle() {
  if (document.getElementById(STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = STYLE_ID;
  style.textContent = `
    .confirmModalOverlay{position:fixed;inset:0;z-index:10000;display:grid;place-items:center;background:rgba(0,0,0,.58);padding:18px}
    .confirmModalDialog{width:min(560px,100%);max-height:min(720px,92vh);overflow:auto;background:#101418;color:#f3f5f7;border:1px solid #4b5563;border-radius:8px;box-shadow:0 20px 70px rgba(0,0,0,.45)}
    .confirmModalHead,.confirmModalBody,.confirmModalActions{padding:16px 18px}
    .confirmModalHead{border-bottom:1px solid #2d333b;display:grid;gap:6px}
    .confirmModalTitle{margin:0;font-size:18px;line-height:1.25}
    .confirmModalSeverity{justify-self:start;border:1px double #d55e00;border-radius:999px;padding:2px 8px;background:rgba(213,94,0,.16);color:#ffd8c2;font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}
    .confirmModalBody{display:grid;gap:12px}
    .confirmModalSummary{display:grid;gap:6px;border:1px solid #30363d;border-radius:6px;padding:10px 12px;background:#0b0f14}
    .confirmModalSummary div{display:grid;grid-template-columns:108px 1fr;gap:8px}
    .confirmModalSummary dt{color:#aab4c0;font-size:12px}
    .confirmModalSummary dd{margin:0;color:#f3f5f7}
    .confirmModalConsequence{border-left:5px double #d55e00;background:#1b2027;padding:10px 12px;border-radius:4px}
    .confirmModalConsequence strong{display:block;margin-bottom:4px;color:#ffd8c2}
    .confirmModalInstruction{font-size:13px;line-height:1.45;color:#d9e1ea}
    .confirmModalField{display:grid;gap:6px}
    .confirmModalField span{font-size:12px;color:#aab4c0}
    .confirmModalField input,.confirmModalField textarea{width:100%;box-sizing:border-box;border:1px solid #4b5563;border-radius:6px;background:#0b0f14;color:#f3f5f7;padding:9px 10px}
    .confirmModalCheck{display:flex;align-items:flex-start;gap:8px;font-size:13px;color:#d9e1ea}
    .confirmModalCheck input{width:auto;margin-top:2px}
    .confirmModalHold{display:grid;gap:6px}
    .confirmModalHold button{justify-self:start}
    .confirmModalStatus{min-height:18px;font-size:12px;color:#aab4c0}
    .confirmModalValidation{min-height:18px;font-size:12px;color:#ffd8c2}
    .confirmModalActions{display:flex;justify-content:space-between;gap:16px;border-top:1px solid #2d333b}
    .confirmModalActionGroup{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
    .confirmModalActions button{border:1px solid #4b5563;border-radius:6px;background:#1f2937;color:#f3f5f7;padding:8px 12px;cursor:pointer}
    .confirmModalActions button[data-role="cancel"]{border-style:solid;background:#111827}
    .confirmModalActions button[data-role="submit"]{border-color:#d55e00;border-style:double;background:#7f1d1d;font-weight:800}
    .confirmModalActions button:disabled{opacity:.45;cursor:not-allowed}
  `;
  document.head.appendChild(style);
}

function focusable(root) {
  return Array.from(root.querySelectorAll("button,input,textarea,select,a[href],[tabindex]"))
    .filter((el) => !el.disabled && el.getAttribute("tabindex") !== "-1");
}

function normalizeOptions(options = {}) {
  const actionId = String(options.actionId || options.action_id || "").trim();
  const source = String(options.sourceSurface || options.source_surface || options.source || "dashboard").trim();
  const target = options.target == null ? "" : String(options.target);
  const consequence = String(options.consequence || "This action changes system state.");
  const reversibility = String(
    options.reversibility
      || options.reversibilityText
      || "Not automatically reversible from this dialog. Use the relevant audit and rollback workflow if recovery is needed."
  );
  return {
    title: String(options.title || "Confirm action"),
    action: String(options.action || options.title || "this action"),
    actionId,
    target,
    consequence,
    reversibility,
    confirmText: String(options.confirmText || options.requiredToken || "CONFIRM"),
    requireReason: !!options.requireReason,
    minReasonLength: Math.max(0, Number(options.minReasonLength || 0)),
    submitLabel: String(options.submitLabel || "Confirm"),
    cancelLabel: String(options.cancelLabel || "Cancel"),
    actor: String(options.actor || "operator"),
    source,
    holdMs: Math.max(0, Number(options.holdMs || 0)),
    severity: String(options.severity || "destructive").trim().toLowerCase() || "destructive",
  };
}

function requestId() {
  try {
    if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
      return globalThis.crypto.randomUUID();
    }
  } catch {}
  return `confirm-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export function buildConfirmationPayload(result = {}, options = {}) {
  const cfg = normalizeOptions(options);
  const reason = String(result.reason || "");
  const holdMs = Math.max(0, Number(result.holdMs ?? cfg.holdMs ?? 0));
  const rid = String(result.requestId || result.request_id || requestId());
  const method = holdMs > 0 ? "typed_phrase_hold" : "typed_phrase";
  return {
    confirm: cfg.confirmText,
    confirmation: cfg.confirmText,
    confirmation_token: cfg.confirmText,
    confirmation_method: method,
    confirmation_hold_ms: holdMs,
    consequence_ack: true,
    actor: cfg.actor,
    source: cfg.source,
    source_surface: cfg.source,
    reason,
    request_id: rid,
    target: cfg.target,
    action_id: cfg.actionId,
    action: cfg.action,
    consequence: cfg.consequence,
    reversibility: cfg.reversibility,
    confirmed_at_ms: Date.now(),
  };
}

export function validateConfirmationInput(input = {}, options = {}) {
  const cfg = normalizeOptions(options);
  const typedPhrase = String(input.phrase || input.confirmation || "").trim();
  const reason = String(input.reason || "").trim();
  const holdComplete = cfg.holdMs <= 0 || input.holdComplete === true;
  const checks = {
    phraseOk: typedPhrase === cfg.confirmText,
    reasonOk: !cfg.requireReason || reason.length >= cfg.minReasonLength,
    ackOk: input.ack === true || input.consequenceAck === true,
    holdOk: holdComplete,
  };
  const missing = [];
  if (!checks.phraseOk) missing.push(`Type ${cfg.confirmText}`);
  if (!checks.reasonOk) missing.push(`Enter a reason with at least ${cfg.minReasonLength} characters`);
  if (!checks.ackOk) missing.push("Acknowledge the consequence");
  if (!checks.holdOk) missing.push(`Complete the ${Math.ceil(cfg.holdMs / 1000)}s hold`);
  return {
    ok: checks.phraseOk && checks.reasonOk && checks.ackOk && checks.holdOk,
    checks,
    missing,
  };
}

export async function requestConfirmation(options = {}) {
  const cfg = normalizeOptions(options);
  if (typeof document === "undefined") {
    return { ok: false, cancelled: true };
  }
  ensureStyle();
  const previous = document.getElementById(MODAL_ID);
  if (previous) previous.remove();

  const overlay = document.createElement("div");
  overlay.id = MODAL_ID;
  overlay.className = "confirmModalOverlay";
  overlay.innerHTML = `
    <div class="confirmModalDialog" role="alertdialog" aria-modal="true" aria-labelledby="${MODAL_ID}Title" aria-describedby="${MODAL_ID}Consequence ${MODAL_ID}Instruction ${MODAL_ID}Validation">
      <div class="confirmModalHead">
        <div class="confirmModalSeverity" data-field="severity"></div>
        <h2 class="confirmModalTitle" id="${MODAL_ID}Title"></h2>
      </div>
      <div class="confirmModalBody">
        <dl class="confirmModalSummary">
          <div><dt>Action</dt><dd data-field="action"></dd></div>
          <div><dt>Target</dt><dd data-field="target"></dd></div>
          <div><dt>Reversibility</dt><dd data-field="reversibility"></dd></div>
        </dl>
        <div class="confirmModalConsequence" id="${MODAL_ID}Consequence"><strong>Consequence</strong><span data-field="consequenceText"></span></div>
        <div class="confirmModalInstruction" id="${MODAL_ID}Instruction"></div>
        <label class="confirmModalField">
          <span>Type phrase</span>
          <input data-field="phrase" autocomplete="off" spellcheck="false">
        </label>
        <label class="confirmModalField" data-field="reasonWrap">
          <span>Reason</span>
          <textarea data-field="reason" rows="3"></textarea>
        </label>
        <label class="confirmModalCheck">
          <input data-field="ack" type="checkbox">
          <span>I understand the consequence.</span>
        </label>
        <div class="confirmModalHold" data-field="holdWrap">
          <button type="button" data-role="hold"></button>
          <div class="confirmModalStatus" data-field="holdStatus" aria-live="polite"></div>
        </div>
        <div class="confirmModalValidation" data-field="validation" id="${MODAL_ID}Validation" aria-live="polite"></div>
      </div>
      <div class="confirmModalActions">
        <div class="confirmModalActionGroup"><button type="button" data-role="cancel"></button></div>
        <div class="confirmModalActionGroup"><button type="button" data-role="submit" disabled></button></div>
      </div>
    </div>
  `;

  const title = overlay.querySelector(`#${MODAL_ID}Title`);
  const severity = overlay.querySelector('[data-field="severity"]');
  const action = overlay.querySelector('[data-field="action"]');
  const target = overlay.querySelector('[data-field="target"]');
  const reversibility = overlay.querySelector('[data-field="reversibility"]');
  const consequenceText = overlay.querySelector('[data-field="consequenceText"]');
  const instruction = overlay.querySelector(`#${MODAL_ID}Instruction`);
  const phrase = overlay.querySelector('[data-field="phrase"]');
  const reasonWrap = overlay.querySelector('[data-field="reasonWrap"]');
  const reason = overlay.querySelector('[data-field="reason"]');
  const ack = overlay.querySelector('[data-field="ack"]');
  const holdWrap = overlay.querySelector('[data-field="holdWrap"]');
  const holdStatus = overlay.querySelector('[data-field="holdStatus"]');
  const holdButton = overlay.querySelector('[data-role="hold"]');
  const validation = overlay.querySelector('[data-field="validation"]');
  const cancel = overlay.querySelector('[data-role="cancel"]');
  const submit = overlay.querySelector('[data-role="submit"]');

  title.textContent = cfg.title;
  if (severity) severity.textContent = `${cfg.severity} action`;
  action.textContent = cfg.action;
  target.textContent = cfg.target || "unspecified target";
  if (reversibility) reversibility.textContent = cfg.reversibility;
  if (consequenceText) consequenceText.textContent = cfg.consequence;
  if (instruction) {
    instruction.innerHTML = `To continue, type <strong>${escapeHTML(cfg.confirmText)}</strong>${cfg.requireReason ? ", enter a reason" : ""}, and acknowledge the consequence.`;
  }
  phrase.placeholder = cfg.confirmText;
  phrase.setAttribute("aria-label", `Type ${cfg.confirmText} to confirm ${cfg.action}`);
  cancel.textContent = cfg.cancelLabel;
  submit.textContent = cfg.submitLabel;
  if (!cfg.requireReason && reasonWrap) {
    reasonWrap.style.display = "none";
    if (reason) reason.disabled = true;
  }
  if (cfg.holdMs > 0) {
    holdButton.textContent = `Hold ${Math.ceil(cfg.holdMs / 1000)}s`;
    holdStatus.textContent = "Hold confirmation is required.";
  } else if (holdWrap) {
    holdWrap.style.display = "none";
    if (holdButton) holdButton.disabled = true;
  }

  const previousFocus = document.activeElement;

  return new Promise((resolve) => {
    let resolved = false;
    let holdStart = 0;
    let holdTimer = null;
    let holdComplete = cfg.holdMs <= 0;
    let measuredHoldMs = 0;
    const close = (result) => {
      if (resolved) return;
      resolved = true;
      if (holdTimer) clearInterval(holdTimer);
      overlay.removeEventListener("keydown", onKeydown);
      overlay.remove();
      try {
        if (previousFocus && typeof previousFocus.focus === "function") previousFocus.focus();
      } catch {}
      resolve(result);
    };
    const valid = () => {
      const state = validateConfirmationInput({
        phrase: phrase.value,
        reason: reason.value,
        ack: !!(ack && ack.checked),
        holdComplete,
      }, cfg);
      submit.disabled = !state.ok;
      if (validation) {
        validation.textContent = state.ok ? "Ready to submit." : state.missing.join("; ");
      }
    };
    const finishHold = () => {
      const elapsed = Math.max(0, Date.now() - holdStart);
      if (elapsed >= cfg.holdMs) {
        holdComplete = true;
        measuredHoldMs = elapsed;
        if (holdStatus) holdStatus.textContent = "Hold confirmed.";
        if (holdButton) holdButton.disabled = true;
        if (holdTimer) clearInterval(holdTimer);
        holdTimer = null;
        valid();
      }
    };
    const cancelHold = () => {
      if (holdComplete) return;
      if (holdTimer) clearInterval(holdTimer);
      holdTimer = null;
      holdStart = 0;
      measuredHoldMs = 0;
      if (holdStatus) holdStatus.textContent = "Hold confirmation is required.";
      valid();
    };
    const startHold = () => {
      if (cfg.holdMs <= 0 || holdComplete || holdTimer) return;
      holdStart = Date.now();
      if (holdStatus) holdStatus.textContent = "Holding...";
      holdTimer = setInterval(finishHold, 50);
      finishHold();
    };
    const onKeydown = (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        close({ ok: false, cancelled: true });
        return;
      }
      if (event.key !== "Tab") return;
      const nodes = focusable(overlay);
      if (!nodes.length) return;
      const first = nodes[0];
      const last = nodes[nodes.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    phrase.addEventListener("input", valid);
    reason.addEventListener("input", valid);
    ack.addEventListener("change", valid);
    if (holdButton) {
      holdButton.addEventListener("pointerdown", startHold);
      holdButton.addEventListener("pointerup", finishHold);
      holdButton.addEventListener("pointercancel", cancelHold);
      holdButton.addEventListener("pointerleave", cancelHold);
      holdButton.addEventListener("keydown", (event) => {
        if (event.key !== " " && event.key !== "Enter") return;
        event.preventDefault();
        startHold();
      });
      holdButton.addEventListener("keyup", (event) => {
        if (event.key !== " " && event.key !== "Enter") return;
        event.preventDefault();
        finishHold();
      });
      holdButton.addEventListener("blur", cancelHold);
    }
    cancel.addEventListener("click", () => close({ ok: false, cancelled: true }));
    submit.addEventListener("click", () => {
      if (submit.disabled) return;
      const requestIdValue = requestId();
      const finalHoldMs = cfg.holdMs > 0 ? Math.max(cfg.holdMs, measuredHoldMs) : 0;
      close({
        ok: true,
        confirmed: true,
        phrase: cfg.confirmText,
        reason: String(reason.value || "").trim(),
        requestId: requestIdValue,
        payload: buildConfirmationPayload({
          reason: String(reason.value || "").trim(),
          holdMs: finalHoldMs,
          requestId: requestIdValue,
        }, cfg),
      });
    });
    overlay.addEventListener("keydown", onKeydown);
    document.body.appendChild(overlay);
    phrase.focus();
    valid();
  });
}
