"use strict";

const MODAL_ID = "sharedConfirmationModal";
const STYLE_ID = "sharedConfirmationModalStyle";

function ensureStyle() {
  if (document.getElementById(STYLE_ID)) return;
  const style = document.createElement("style");
  style.id = STYLE_ID;
  style.textContent = `
    .confirmModalOverlay{position:fixed;inset:0;z-index:10000;display:grid;place-items:center;background:rgba(0,0,0,.58);padding:18px}
    .confirmModalDialog{width:min(560px,100%);max-height:min(720px,92vh);overflow:auto;background:#101418;color:#f3f5f7;border:1px solid #4b5563;border-radius:8px;box-shadow:0 20px 70px rgba(0,0,0,.45)}
    .confirmModalHead,.confirmModalBody,.confirmModalActions{padding:16px 18px}
    .confirmModalHead{border-bottom:1px solid #2d333b}
    .confirmModalTitle{margin:0;font-size:18px;line-height:1.25}
    .confirmModalBody{display:grid;gap:12px}
    .confirmModalConsequence{border-left:4px solid #d55e00;background:#1b2027;padding:10px 12px;border-radius:4px}
    .confirmModalField{display:grid;gap:6px}
    .confirmModalField span{font-size:12px;color:#aab4c0}
    .confirmModalField input,.confirmModalField textarea{width:100%;box-sizing:border-box;border:1px solid #4b5563;border-radius:6px;background:#0b0f14;color:#f3f5f7;padding:9px 10px}
    .confirmModalCheck{display:flex;align-items:flex-start;gap:8px;font-size:13px;color:#d9e1ea}
    .confirmModalCheck input{width:auto;margin-top:2px}
    .confirmModalHold{display:grid;gap:6px}
    .confirmModalHold button{justify-self:start}
    .confirmModalStatus{min-height:18px;font-size:12px;color:#aab4c0}
    .confirmModalActions{display:flex;justify-content:flex-end;gap:10px;border-top:1px solid #2d333b}
    .confirmModalActions button{border:1px solid #4b5563;border-radius:6px;background:#1f2937;color:#f3f5f7;padding:8px 12px;cursor:pointer}
    .confirmModalActions button[data-role="submit"]{border-color:#d55e00;background:#7f1d1d}
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
  return {
    title: String(options.title || "Confirm action"),
    action: String(options.action || options.title || "this action"),
    actionId,
    target,
    consequence: String(options.consequence || "This action changes system state."),
    confirmText: String(options.confirmText || options.requiredToken || "CONFIRM"),
    requireReason: !!options.requireReason,
    minReasonLength: Math.max(0, Number(options.minReasonLength || 0)),
    submitLabel: String(options.submitLabel || "Confirm"),
    cancelLabel: String(options.cancelLabel || "Cancel"),
    actor: String(options.actor || "operator"),
    source,
    holdMs: Math.max(0, Number(options.holdMs || 0)),
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
    <div class="confirmModalDialog" role="dialog" aria-modal="true" aria-labelledby="${MODAL_ID}Title" aria-describedby="${MODAL_ID}Consequence">
      <div class="confirmModalHead">
        <h2 class="confirmModalTitle" id="${MODAL_ID}Title"></h2>
      </div>
      <div class="confirmModalBody">
        <div><strong data-field="action"></strong><div data-field="target"></div></div>
        <div class="confirmModalConsequence" id="${MODAL_ID}Consequence"></div>
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
      </div>
      <div class="confirmModalActions">
        <button type="button" data-role="cancel"></button>
        <button type="button" data-role="submit" disabled></button>
      </div>
    </div>
  `;

  const title = overlay.querySelector(`#${MODAL_ID}Title`);
  const action = overlay.querySelector('[data-field="action"]');
  const target = overlay.querySelector('[data-field="target"]');
  const consequence = overlay.querySelector(`#${MODAL_ID}Consequence`);
  const phrase = overlay.querySelector('[data-field="phrase"]');
  const reasonWrap = overlay.querySelector('[data-field="reasonWrap"]');
  const reason = overlay.querySelector('[data-field="reason"]');
  const ack = overlay.querySelector('[data-field="ack"]');
  const holdWrap = overlay.querySelector('[data-field="holdWrap"]');
  const holdStatus = overlay.querySelector('[data-field="holdStatus"]');
  const holdButton = overlay.querySelector('[data-role="hold"]');
  const cancel = overlay.querySelector('[data-role="cancel"]');
  const submit = overlay.querySelector('[data-role="submit"]');

  title.textContent = cfg.title;
  action.textContent = cfg.action;
  target.textContent = cfg.target;
  consequence.textContent = cfg.consequence;
  phrase.placeholder = cfg.confirmText;
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
      const phraseOk = String(phrase.value || "").trim() === cfg.confirmText;
      const reasonOk = !cfg.requireReason || String(reason.value || "").trim().length >= cfg.minReasonLength;
      const ackOk = !!(ack && ack.checked);
      submit.disabled = !(phraseOk && reasonOk && ackOk && holdComplete);
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
