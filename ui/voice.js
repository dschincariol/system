"use strict";

/*
  voice.js — Collaborative Voice Partner (v2)
  ------------------------------------------
  Adds:
    🔊 CRIT auto-readouts (deduped + throttled)
    📱 iPhone push-to-talk (press-and-hold)
    🧠 LLM fallback (optional backend endpoint)

  Safety:
    - No system actions are executed.
    - Voice navigates + explains only.
    - LLM fallback is read-only + gated to "explain/advice" output.

  Assumes dashboard.js provides:
    - toast(msg, level, ms)
    - _jumpToCard(titleContains)
    - openWhyModal(alertRow)
    - openIncidentDrawer(row)
    - window.__ACTIVE_INCIDENT__ set when drawer opens (recommended)
    - window._lastAlerts array exists (you have it)
*/

const VOICE_CAPS = {
  synth: typeof window !== "undefined" && "speechSynthesis" in window,
  recog:
    typeof window !== "undefined" &&
    ("SpeechRecognition" in window || "webkitSpeechRecognition" in window),
};

const VOICE_ENABLED = !!(VOICE_CAPS.synth && VOICE_CAPS.recog);

// Optional backend LLM endpoint (non-breaking):
// POST /api/voice/ask  { text, context } -> { ok, answer, confidence, suggested_intent? }
const VOICE_LLM_ENDPOINT = "";

function _getWindowFn(name) {
  if (typeof window === "undefined") return null;
  return typeof window[name] === "function" ? window[name] : null;
}

// ------------------------------------
// Speech (TTS) helpers
// ------------------------------------
function _speak(text) {
  if (!VOICE_CAPS.synth || !text) return;
  try {
    const u = new SpeechSynthesisUtterance(String(text));
    u.rate = 0.95;
    u.pitch = 1.0;
    u.volume = 0.9;

    // Cancel any prior speech so CRIT readouts feel snappy
    speechSynthesis.cancel();
    speechSynthesis.speak(u);
  } catch {}
}

function _sayAndToast(msg, level = "dim", ms = 4200) {
  const toastFn = _getWindowFn("toast");
  if (toastFn) {
    try { toastFn(msg, level, ms); } catch {}
  }
  _speak(msg);
}

// ------------------------------------
// Intent classification (deterministic)
// ------------------------------------
function classifyVoiceIntent(text) {
  const t = String(text || "").toLowerCase();

  if (/open latest critical|open the critical|latest crit/.test(t))
    return "incident.open_latest_crit";

  if (/summarize system|system summary|what's going on/.test(t))
    return "system.summary";

  if (/safe to ignore|ignore this|can i ignore/.test(t))
    return "incident.safe_to_ignore";

  if (/what should i do|recommended|posture/.test(t))
    return "incident.posture";

  if (/decision confidence|how confident/.test(t))
    return "incident.decision_confidence";

  if (/what happens next|if nothing changes/.test(t))
    return "incident.future_outcome";

  if (/similar|seen before|history/.test(t))
    return "incident.similar";

  if (/why|explain/.test(t))
    return "incident.explain";

  if (/alerts/.test(t)) return "nav.alerts";
  if (/execution/.test(t)) return "nav.execution";

  return "unknown";
}

// ------------------------------------
// Context builder (small + safe)
// ------------------------------------
function _getDecisionBarSnapshot() {
  const g = (id) => document.getElementById(id)?.textContent || "";
  return {
    system: g("pillSystem"),
    crit: g("pillCrit"),
    warn: g("pillWarn"),
    data: g("pillData"),
    model: g("pillModel"),
    exec: g("pillExec"),
    updated: g("pillUpdated"),
  };
}

function _summarizeAlert(a) {
  if (!a) return null;
  return {
    id: a.id,
    ts_ms: a.ts_ms,
    severity: a.severity,
    symbol: a.symbol,
    horizon_s: a.horizon_s,
    expected_z: a.expected_z,
    confidence: a.confidence,
    event_title: a.event_title,
    reason: a.reason || "",
    resolved: !!a.resolved,
  };
}

function _buildVoiceContext() {
  const active = window.__ACTIVE_INCIDENT__ ? _summarizeAlert(window.__ACTIVE_INCIDENT__) : null;

  const last = Array.isArray(window._lastAlerts) ? window._lastAlerts : [];
  const top = last
    .filter((x) => x && !x.resolved)
    .slice(0, 12)
    .map(_summarizeAlert)
    .filter(Boolean);

  return {
    now_ms: Date.now(),
    decision_bar: _getDecisionBarSnapshot(),
    active_incident: active,
    top_open_alerts: top,
  };
}

export function buildVoiceContextSnapshot() {
  return _buildVoiceContext();
}

// ------------------------------------
// LLM fallback (optional)
// ------------------------------------
async function _askLLMFallback(userText) {
  if (!VOICE_LLM_ENDPOINT) return null;

  try {
    const res = await fetch(VOICE_LLM_ENDPOINT, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify({
        text: String(userText || ""),
        context: _buildVoiceContext(),
      }),
    });

    const txt = await res.text();
    let j = null;
    try { j = txt ? JSON.parse(txt) : null; } catch { j = null; }

    if (!res.ok || !j || !j.ok || !j.answer) return null;

    // Answer must be short & ops-safe
    return {
      answer: String(j.answer).slice(0, 700),
      confidence: j.confidence,
      suggested_intent: j.suggested_intent,
    };
  } catch {
    return null;
  }
}

// ------------------------------------
// Deterministic “co-pilot” helpers
// (These are intentionally conservative)
// ------------------------------------

// These functions are expected in your earlier patch notes; if missing, we fall back to simple outputs.
function _recommendedPosture(r) {
  if (!r) return "Monitor.";
  const sev = String(r.severity || "INFO").toUpperCase();
  const ageMin = Math.max(0, Math.floor((Date.now() - Number(r.ts_ms)) / 60000));

  if (sev === "CRIT") {
    return ageMin < 5
      ? "Stop and assess now. Open the incident and confirm data + execution health before any actions."
      : "Escalate. If still CRIT, investigate root cause and consider pausing risky ops.";
  }
  if (sev === "WARN") return "Investigate briefly. Confirm if escalating; monitor closely.";
  return "Observe. Likely informational unless it repeats.";
}

function _decisionConfidence(r) {
  if (!r) return "unknown";
  const sev = String(r.severity || "INFO").toUpperCase();
  const conf = Number(r.confidence);
  const z = Math.abs(Number(r.expected_z));

  // “Decision confidence” framing != model confidence
  // Conservative heuristic: severity + strength + data freshness (unknown here) => lean cautious
  if (sev === "CRIT") return "low — treat as high risk until verified";
  if (sev === "WARN") {
    if (Number.isFinite(z) && z >= 2.0 && Number.isFinite(conf) && conf >= 0.75) return "medium";
    return "low to medium";
  }
  if (Number.isFinite(z) && z >= 2.5 && Number.isFinite(conf) && conf >= 0.85) return "medium";
  return "low";
}

function _safeToIgnore(r) {
  if (!r) return "No — open an incident first.";
  const sev = String(r.severity || "INFO").toUpperCase();
  const ageMin = Math.max(0, Math.floor((Date.now() - Number(r.ts_ms)) / 60000));
  if (sev === "CRIT") return "No — not safe to ignore.";
  if (sev === "WARN") return ageMin > 30 ? "Maybe — monitor for repeat." : "No — check quickly first.";
  return "Yes — safe to monitor unless it repeats.";
}

function _ifNothingChanges(r) {
  if (!r) return "Open an incident first.";
  const sev = String(r.severity || "INFO").toUpperCase();
  if (sev === "CRIT") return "If nothing changes, this may cascade. Expect more alerts or degraded performance. Investigate now.";
  if (sev === "WARN") return "If nothing changes, this may either resolve or escalate. Monitor for repeat within the next hour.";
  return "If nothing changes, this likely stays informational. Watch for recurrence.";
}

// ------------------------------------
// Intent handlers
// ------------------------------------
async function handleVoice(textSpoken) {
  const text = String(textSpoken || "").trim();
  if (!text) return;

  // Deterministic first
  let intent = classifyVoiceIntent(text);

  // If unknown, try LLM fallback if available
  let llm = null;
  if (intent === "unknown") {
    llm = await _askLLMFallback(text);
    if (llm && llm.suggested_intent) {
      intent = String(llm.suggested_intent);
    }
  }

  const r = window.__ACTIVE_INCIDENT__;

  // Voice stop
  if (intent === "voice.stop") {
    stopVoiceCapture();
    _sayAndToast("Voice stopped.", "dim", 1800);
    return;
  }

  // Incident intents require active incident
  if (!r && intent.startsWith("incident.")) {
    // If LLM gave an answer anyway, speak it
    if (llm && llm.answer) {
      _sayAndToast(llm.answer, "dim", 5200);
      return;
    }
    _sayAndToast("Please open an incident first.", "warn", 2600);
    return;
  }

  switch (intent) {
    case "incident.posture":
      _sayAndToast(`Recommended posture: ${_recommendedPosture(r)}`, "warn", 5200);
      return;

    case "incident.decision_confidence":
      _sayAndToast(`Decision confidence is ${_decisionConfidence(r)}.`, "dim", 5200);
      return;

    case "incident.safe_to_ignore": {
      const msg = _safeToIgnore(r);
      _sayAndToast(`Safe to ignore? ${msg}`, msg.startsWith("Yes") ? "ok" : "warn", 5200);
      return;
    }

    case "incident.future_outcome":
      _sayAndToast(_ifNothingChanges(r), "warn", 6500);
      return;

    case "incident.similar":
      _sayAndToast("Opening the incident. Look for similar patterns in the timeline and reasons.", "dim", 5200);
      try {
        const openIncidentDrawerFn = _getWindowFn("openIncidentDrawer");
        if (openIncidentDrawerFn) openIncidentDrawerFn(r);
      } catch {}
      return;

    case "incident.explain":
      _sayAndToast("Opening explanation.", "dim", 2200);
      try {
        const openWhyModalFn = _getWindowFn("openWhyModal");
        if (openWhyModalFn) openWhyModalFn(r);
      } catch {}
      return;

    case "nav.alerts":
      _sayAndToast("Showing alerts.", "dim", 2200);
      try {
        const jumpToCardFn = _getWindowFn("_jumpToCard");
        if (jumpToCardFn) jumpToCardFn("Alerts");
      } catch {}
      return;

    case "nav.execution":
      _sayAndToast("Showing execution status.", "dim", 2200);
      try {
        const jumpToCardFn = _getWindowFn("_jumpToCard");
        if (jumpToCardFn) jumpToCardFn("Execution");
      } catch {}
      return;

    case "nav.promotions":
      _sayAndToast("Showing promotions.", "dim", 2200);
      try {
        const jumpToCardFn = _getWindowFn("_jumpToCard");
        if (jumpToCardFn) jumpToCardFn("Promotions");
      } catch {}
      return;

    case "system.state": {
      const snap = _getDecisionBarSnapshot();
      const msg =
        snap.system && /CRIT/.test(snap.system)
          ? "System is in critical state."
          : snap.warn && /WARN/.test(snap.warn)
            ? "System has warnings."
            : "System appears healthy.";
      _sayAndToast(msg, "warn", 3200);
      return;
    }

    default: {
      // If LLM responded, use it
      if (llm && llm.answer) {
        _sayAndToast(llm.answer, "dim", 6500);
      } else {
        _sayAndToast("Sorry — I didn’t understand that. Try: “what should I do” or “explain why”.", "dim", 5200);
      }
      return;
    }
  }
}

// ------------------------------------
// Recognition engine
// ------------------------------------
let _rec = null;
let _listening = false;

function _ensureRecognizer() {
  if (!VOICE_CAPS.recog) return null;
  if (_rec) return _rec;

  const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  _rec = new SR();
  _rec.lang = "en-US";
  _rec.interimResults = false;
  _rec.maxAlternatives = 1;

  _rec.onresult = async (e) => {
    const text = e.results?.[0]?.[0]?.transcript || "";
    await handleVoice(text);
  };

  _rec.onerror = () => {
    _listening = false;
    _setVoiceButtonState(false);
    _sayAndToast("Voice recognition error.", "bad", 2200);
  };

  _rec.onend = () => {
    _listening = false;
    _setVoiceButtonState(false);
  };

  return _rec;
}

function startVoiceCapture() {
  if (!VOICE_ENABLED) {
    _sayAndToast("Voice not supported in this browser.", "warn", 2600);
    return;
  }
  const r = _ensureRecognizer();
  if (!r) return;

  try {
    _listening = true;
    _setVoiceButtonState(true);
    _sayAndToast("Listening…", "dim", 1400);
    r.start();
  } catch {
    _listening = false;
    _setVoiceButtonState(false);
  }
}

function stopVoiceCapture() {
  if (!_rec) return;
  try { _rec.stop(); } catch {}
  _listening = false;
  _setVoiceButtonState(false);
}

// ------------------------------------
// iPhone push-to-talk (press & hold)
// ------------------------------------
function _wirePushToTalk(btn) {
  if (!btn) return;
  if (btn._pushToTalkBound) return;
  btn._pushToTalkBound = true;

  // Prevent iOS “double tap scroll/zoom” weirdness
  btn.style.touchAction = "manipulation";

  let pressed = false;
  let suppressClickUntil = 0;

  const down = (e) => {
    if (pressed) return;
    pressed = true;

    const pt = String((e && e.pointerType) || "").toLowerCase();
    if (pt === "touch" || (e && e.type && e.type.startsWith("touch"))) {
      suppressClickUntil = Date.now() + 750;
    }

    e.preventDefault?.();
    startVoiceCapture();
  };

  const up = (e) => {
    if (!pressed) return;
    pressed = false;

    const pt = String((e && e.pointerType) || "").toLowerCase();
    if (pt === "touch" || (e && e.type && e.type.startsWith("touch"))) {
      suppressClickUntil = Date.now() + 750;
    }

    e.preventDefault?.();
    stopVoiceCapture();
  };

  if (window.PointerEvent) {
    btn.addEventListener("pointerdown", down);
    btn.addEventListener("pointerup", up);
    btn.addEventListener("pointercancel", up);
    btn.addEventListener("pointerleave", up);
  } else {
    btn.addEventListener("touchstart", down, { passive: false });
    btn.addEventListener("touchend", up, { passive: false });
    btn.addEventListener("touchcancel", up, { passive: false });
    btn.addEventListener("mousedown", down);
    btn.addEventListener("mouseup", up);
    btn.addEventListener("mouseleave", up);
  }

  // Desktop click toggles
  btn.addEventListener("click", (e) => {
    if (Date.now() < suppressClickUntil) return;
    if (e && e.pointerType === "touch") return;

    if (_listening) stopVoiceCapture();
    else startVoiceCapture();
  });
}

function _setVoiceButtonState(on) {
  const btn = document.getElementById("btnVoice");
  if (!btn) return;
  btn.classList.toggle("voice-on", !!on);
  btn.textContent = on ? "🎙 Listening…" : "🎙 Hold to Talk";
}

// ------------------------------------
// 🔊 CRIT auto-readouts
// ------------------------------------
//
// Reads new CRIT alerts once, throttled, with dedupe.
// Works with your existing window._lastAlerts refresh loops.
//
const _CRIT_READ_KEY = "voice_crit_read_map_v1";
const _CRIT_LAST_SPOKEN_TS_KEY = "voice_crit_last_spoken_ts_v1";

function _loadCritReadMap() {
  try { return JSON.parse(localStorage.getItem(_CRIT_READ_KEY) || "{}") || {}; }
  catch { return {}; }
}
function _saveCritReadMap(m) {
  try { localStorage.setItem(_CRIT_READ_KEY, JSON.stringify(m || {})); } catch {}
}

function _shouldSpeakCritNow() {
  // Basic throttle: don’t speak more than once every 10 seconds
  const last = Number(localStorage.getItem(_CRIT_LAST_SPOKEN_TS_KEY) || 0);
  return (Date.now() - last) > 10000;
}

function _markSpokeNow() {
  try { localStorage.setItem(_CRIT_LAST_SPOKEN_TS_KEY, String(Date.now())); } catch {}
}

function _critSpeakLoop() {
  // Only run if voice synthesis exists (readouts need TTS)
  if (!VOICE_CAPS.synth) return;
  if (typeof document !== "undefined" && document.hidden) return;

  const rows = Array.isArray(window._lastAlerts) ? window._lastAlerts : [];
  if (!rows.length) return;

  const readMap = _loadCritReadMap();

  // Find newest unresolved CRIT
  const crits = rows
    .filter((a) => a && !a.resolved && String(a.severity || "").toUpperCase() === "CRIT")
    .sort((a, b) => Number(b.ts_ms) - Number(a.ts_ms));

  if (!crits.length) return;

  const newest = crits[0];
  const id = String(newest.id);

  if (readMap[id]) return; // already spoken
  if (!_shouldSpeakCritNow()) return;

  // Speak it
  const sym = newest.symbol === "EXECUTION" ? "Execution" : String(newest.symbol || "Unknown");
  const title = String(newest.event_title || "Critical incident");
  const reason = String(newest.reason || "");

  const msg =
    reason
      ? `Critical alert: ${sym}. ${title}. ${reason}`
      : `Critical alert: ${sym}. ${title}.`;

  _markSpokeNow();
  readMap[id] = Date.now();
  _saveCritReadMap(readMap);

  _sayAndToast(msg, "warn", 7000);
}

function _startCritLoop() {
  if (window.__voiceCritLoopId) return;
  window.__voiceCritLoopId = setInterval(_critSpeakLoop, 2500);
}

function _stopCritLoop() {
  if (!window.__voiceCritLoopId) return;
  clearInterval(window.__voiceCritLoopId);
  window.__voiceCritLoopId = null;
}

// ------------------------------------
// UI wiring
// ------------------------------------
function wireVoiceUI() {
  const btn = document.getElementById("btnVoice");
  if (!btn) return;
  if (btn._voiceUiBound) return;
  btn._voiceUiBound = true;

  // Label + state
  _setVoiceButtonState(false);

  // Push-to-talk
  _wirePushToTalk(btn);

  // Optional keyboard shortcut: hold V to talk (desktop)
  let keyDown = false;
  if (!document._voiceKeyBindingsBound) {
    document._voiceKeyBindingsBound = true;
    document.addEventListener("keydown", (e) => {
      if (e.key.toLowerCase() === "v" && !e.repeat && !keyDown) {
        keyDown = true;
        startVoiceCapture();
      }
    });
    document.addEventListener("keyup", (e) => {
      if (e.key.toLowerCase() === "v") {
        keyDown = false;
        stopVoiceCapture();
      }
    });
  }

  // Start CRIT auto-readouts loop (lightweight)
  if (!document._voiceVisibilityBound) {
    document._voiceVisibilityBound = true;
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) {
        _stopCritLoop();
        return;
      }
      _startCritLoop();
      _critSpeakLoop();
    });
  }

  if (typeof document !== "undefined" && document.hidden) {
    _stopCritLoop();
  } else {
    _startCritLoop();
  }
}

// Boot
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", wireVoiceUI);
} else {
  wireVoiceUI();
  // Auto voice summary on load (CRIT only, once per session)
setTimeout(() => {
  if (sessionStorage.getItem("voice_autosummary_done")) return;

  const rows = Array.isArray(window._lastAlerts) ? window._lastAlerts : [];
  const crits = rows.filter(r => r.severity === "CRIT" && !r.resolved);

  if (crits.length > 0) {
    _sayAndToast(
      `Attention. ${crits.length} critical alert${crits.length > 1 ? "s" : ""} detected. Say “open latest critical” to review.`,
      "warn",
      6000
    );
  }

  sessionStorage.setItem("voice_autosummary_done", "1");
}, 2500);

}
